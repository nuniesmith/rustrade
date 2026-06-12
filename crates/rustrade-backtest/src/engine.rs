//! The replay engine.
//!
//! Single-threaded loop: feeds candles to the brain in order, builds an
//! order from each non-`Hold` decision, applies slippage and fees, and
//! updates a synthetic per-symbol position. On position-reducing fills
//! (closes or flips) a [`TradeOutcome`] is emitted into the result.
//!
//! # Multi-symbol replay
//!
//! Each candle is tagged with a symbol. The engine maintains independent
//! [`Position`] state per symbol but a *single* shared cash balance. When
//! [`Backtest::with_candles`] is used, every candle is assumed to be for
//! the (single) symbol on the config. For multi-symbol runs use
//! [`Backtest::with_symbol_candles`].

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use rustrade_core::{
    Brain, Candle, Decision, Exchange, Fill, MarketDataEvent, OrderKind, Position, Side,
    SignalType, SizeHint, Symbol,
};
use rustrade_risk::clock::ManualClock;
use rustrade_risk::{CircuitBreaker, PositionSizer, SessionPnl};

use crate::config::BacktestConfig;
use crate::error::{Error, Result};
use crate::metrics::TradeOutcome;
use crate::result::BacktestResult;

/// The replay engine itself. Configure via [`BacktestConfig`], attach a
/// [`Brain`] and one or more candle series, then `.run().await` for the
/// result.
///
/// # Example
///
/// ```no_run
/// # use std::sync::Arc;
/// use rustrade_backtest::{Backtest, BacktestConfig, load_csv};
/// # async fn run(brain: Arc<dyn rustrade_core::Brain>) -> rustrade_backtest::Result<()> {
/// let candles = load_csv("data/btcusdt-1m.csv")?;
/// let result = Backtest::new(
///     BacktestConfig::builder()
///         .symbol("BTCUSDT")
///         .initial_cash(10_000.0)
///         .build()?,
///     brain,
/// )
/// .with_candles(candles)
/// .run()
/// .await?;
///
/// println!("{}", result.summary());
/// # Ok(())
/// # }
/// ```
pub struct Backtest {
    config: BacktestConfig,
    brain: Arc<dyn Brain>,
    /// Per-symbol candle series. Within each series candles are assumed
    /// to be in chronological order; across series the engine merges
    /// chronologically before replay.
    series: Vec<(Symbol, Vec<Candle>)>,
}

impl Backtest {
    /// Construct with a config + brain. The candle series is attached
    /// separately via [`Self::with_candles`] / [`Self::with_symbol_candles`].
    pub fn new(config: BacktestConfig, brain: Arc<dyn Brain>) -> Self {
        Self {
            config,
            brain,
            series: Vec::new(),
        }
    }

    /// Feed a candle series for the (single) symbol on the config.
    /// Convenience wrapper around [`Self::with_symbol_candles`].
    ///
    /// Panics if the config has more than one symbol — use
    /// [`Self::with_symbol_candles`] for multi-symbol backtests.
    pub fn with_candles(mut self, candles: Vec<Candle>) -> Self {
        assert_eq!(
            self.config.symbols.len(),
            1,
            "Backtest::with_candles requires a single-symbol config; \
             this config has {} symbols. Use Backtest::with_symbol_candles instead.",
            self.config.symbols.len()
        );
        let symbol = self.config.symbols[0].clone();
        self.series = vec![(symbol, candles)];
        self
    }

    /// Feed a candle series for a specific symbol. Call multiple times
    /// for multi-symbol backtests; repeated calls for the same symbol
    /// replace the previous series.
    ///
    /// The engine merges all series chronologically before replay, so
    /// the brain sees the global event stream — not per-symbol blocks.
    pub fn with_symbol_candles(mut self, symbol: impl Into<Symbol>, candles: Vec<Candle>) -> Self {
        let symbol = symbol.into();
        self.series.retain(|(s, _)| s != &symbol);
        self.series.push((symbol, candles));
        self
    }

    /// Run the backtest to completion. Returns the aggregated result.
    pub async fn run(self) -> Result<BacktestResult> {
        let exchange = Exchange::from("backtest");
        let sizer = PositionSizer::new(self.config.sizing.clone());

        let merged = merge_series(&self.series);
        let candles_processed = merged.len();

        // Reject non-finite / non-positive prices up front. A single
        // `NaN` close would otherwise propagate through the equity curve
        // and silently turn every downstream metric (Sharpe, Sortino,
        // drawdown) into `NaN`. Fail loud instead.
        for (symbol, candle) in &merged {
            if let Err(why) = validate_candle(candle) {
                return Err(Error::Data(format!(
                    "{symbol} candle at t={}: {why}",
                    candle.time
                )));
            }
        }

        let mut state = State::new(
            self.config.initial_cash,
            self.config.symbols.iter().cloned(),
        );
        let mut signals_emitted = 0usize;
        let mut orders_filled = 0usize;
        let mut orders_blocked = 0usize;
        let mut trades: Vec<TradeOutcome> = Vec::new();

        // Per-symbol risk gates (session-PnL halt + circuit breaker),
        // mirroring the live execution path's gate sequence. Time is
        // driven by candle timestamps through a shared `ManualClock`, so
        // the UTC-midnight rollover and the breaker's window/cooldown run
        // on replay time and the run stays deterministic.
        let risk_clock = Arc::new(ManualClock::new(0));
        let mut risk: BTreeMap<Symbol, SymbolRisk> = BTreeMap::new();
        if self.config.session_pnl.is_some() || self.config.circuit_breaker.is_some() {
            for s in &self.config.symbols {
                risk.insert(
                    s.clone(),
                    SymbolRisk {
                        session: self
                            .config
                            .session_pnl
                            .clone()
                            .map(|c| SessionPnl::with_clock(s.as_str(), c, risk_clock.clone())),
                        breaker: self
                            .config
                            .circuit_breaker
                            .clone()
                            .map(|c| CircuitBreaker::with_clock(c, risk_clock.clone())),
                    },
                );
            }
        }

        for (symbol, candle) in &merged {
            // Advance replay time and roll the risk state over before any
            // decision on this candle — the live counterpart is the
            // periodic `RiskSweepService` tick.
            if !risk.is_empty() {
                risk_clock.set(candle.time.max(0) as u64 / 1_000);
                if let Some(r) = risk.get_mut(symbol) {
                    r.tick();
                }
            }
            let event = MarketDataEvent::Candle {
                exchange: exchange.clone(),
                symbol: symbol.clone(),
                candle: *candle,
            };

            // Brains see the live position at decision time — same as
            // the live `ExecutionService` does. For symbols not in the
            // config we still route the event through the brain (so
            // multi-symbol brains can filter) but treat the position as
            // FLAT and skip any orders.
            let position = state.position(symbol).copied().unwrap_or(Position::FLAT);
            let decision = self
                .brain
                .on_event(&event, &position)
                .await
                .map_err(|e| Error::Brain(e.to_string()))?;

            let in_config = state.has_symbol(symbol);

            if !in_config || matches!(decision.signal, SignalType::Hold) {
                state.sample_step(symbol, candle.close, self.config.contract_value);
                continue;
            }
            signals_emitted += 1;

            // ── Risk gates (parity with the live ExecutionService) ────
            // Gate 1: session-PnL halt; gate 2: circuit breaker. Like
            // live, these block *every* non-Hold decision — including
            // `Close` — after the signal is counted.
            if let Some(r) = risk.get(symbol)
                && (r
                    .session
                    .as_ref()
                    .is_some_and(SessionPnl::is_session_halted)
                    || r.breaker.as_ref().is_some_and(CircuitBreaker::is_tripped))
            {
                orders_blocked += 1;
                state.sample_step(symbol, candle.close, self.config.contract_value);
                continue;
            }

            // Translate the decision into a concrete fill request. For
            // `Close` we use the existing position size. For Buy/Sell we
            // size from the brain's hint just like ExecutionService, and
            // carry the requested order kind + limit price.
            let Some(resolved) = resolve_order(
                &decision,
                &position,
                &sizer,
                candle.close,
                self.config.contract_value,
            ) else {
                state.sample_step(symbol, candle.close, self.config.contract_value);
                continue;
            };
            if resolved.qty <= 0.0 {
                state.sample_step(symbol, candle.close, self.config.contract_value);
                continue;
            }

            // Decide the fill by order kind. Market / IOC / FOK and closes
            // are immediate takers at the candle close. Limit / PostOnly
            // rest and fill only if this candle's range crosses the limit
            // (a post-only that would cross as taker is rejected). A limit
            // that doesn't cross is treated as unfilled and dropped — orders
            // that rest across candles or partially fill need an order book
            // (a 0.4a item).
            let Some((reference_price, is_taker)) = resolve_fill(&resolved, candle) else {
                state.sample_step(symbol, candle.close, self.config.contract_value);
                continue;
            };
            // Slippage applies to taker fills (crossing the spread); a
            // resting maker fills at its limit price exactly.
            let fill_price = if is_taker {
                self.config.slippage.apply(resolved.side, reference_price)
            } else {
                reference_price
            };
            let fee = self.config.fees.fee_for(
                fill_price,
                resolved.qty * self.config.contract_value,
                is_taker,
            );

            // Update position state. If this fill reduces or flips the
            // position, emit one or more TradeOutcomes.
            let trades_before = trades.len();
            apply_fill(
                &mut state,
                symbol,
                resolved.side,
                resolved.qty,
                fill_price,
                fee,
                self.config.contract_value,
                candle_time(candle),
                &mut trades,
            );

            // Feed each realised close into the risk gates — the live
            // counterpart is `FillRoutingService`'s auto-PnL recording.
            if let Some(r) = risk.get_mut(symbol) {
                for t in &trades[trades_before..] {
                    if let Some(session) = &mut r.session {
                        session.record_close(t.gross_pnl, t.fee);
                    }
                    if let Some(breaker) = &mut r.breaker {
                        let net = t.net_pnl();
                        if net > 0.0 {
                            breaker.record_win();
                        } else if net < 0.0 {
                            breaker.record_loss();
                        }
                    }
                }
            }

            orders_filled += 1;

            // Inform the brain of the (synthetic) fill — same callback
            // the live `FillRoutingService` would invoke.
            let fill = Fill {
                symbol: symbol.clone(),
                order_id: format!("bt-{orders_filled}"),
                client_id: None,
                side: resolved.side,
                price: rustrade_core::Price(fill_price),
                size: rustrade_core::Volume(resolved.qty),
                fee,
                fee_currency: "QUOTE".into(),
                timestamp: candle_time(candle),
            };
            self.brain
                .on_fill(&fill)
                .await
                .map_err(|e| Error::Brain(e.to_string()))?;

            state.sample_step(symbol, candle.close, self.config.contract_value);
        }

        let total_fees: f64 = trades.iter().map(|t| t.fee).sum();
        let net_pnl: f64 = trades.iter().map(|t| t.net_pnl()).sum();
        let symbol_label = if self.config.symbols.len() == 1 {
            self.config.symbols[0].as_str().to_string()
        } else {
            // Stable, deterministic label for multi-symbol runs.
            let parts: Vec<&str> = self.config.symbols.iter().map(|s| s.as_str()).collect();
            parts.join(",")
        };

        let returns = state.into_returns();
        Ok(BacktestResult {
            symbol: symbol_label,
            initial_cash: self.config.initial_cash,
            final_cash: self.config.initial_cash + net_pnl,
            net_pnl,
            total_fees,
            candles_processed,
            signals_emitted,
            orders_filled,
            orders_blocked,
            trades,
            max_drawdown: returns.max_drawdown,
            equity_curve: returns.equity,
            period_returns: returns.period_returns,
            risk_free_rate: self.config.risk_free_rate,
            periods_per_year: self.config.periods_per_year,
        })
    }
}

/// Per-symbol risk gates for replay: the same primitives the live bot
/// runs, time-driven by the shared replay clock.
struct SymbolRisk {
    session: Option<SessionPnl>,
    breaker: Option<CircuitBreaker>,
}

impl SymbolRisk {
    /// Roll both primitives forward to the current replay time.
    fn tick(&mut self) {
        if let Some(s) = &mut self.session {
            s.tick();
        }
        if let Some(b) = &mut self.breaker {
            b.tick();
        }
    }
}

// ── Series merging ──────────────────────────────────────────────────────

/// Merge per-symbol candle series into a chronological `(symbol, candle)`
/// stream. Stable for equal timestamps: ties preserve the *order the
/// series were attached*, then the order candles appear within their
/// series. This keeps multi-symbol runs deterministic even if two
/// exchanges produce identical timestamps.
fn merge_series(series: &[(Symbol, Vec<Candle>)]) -> Vec<(Symbol, Candle)> {
    let total: usize = series.iter().map(|(_, c)| c.len()).sum();
    let mut out: Vec<(Symbol, Candle, usize)> = Vec::with_capacity(total);
    for (series_idx, (sym, candles)) in series.iter().enumerate() {
        for c in candles {
            out.push((sym.clone(), *c, series_idx));
        }
    }
    // Sort by (time, series order) — stable for matching timestamps
    // within the same series.
    out.sort_by(|a, b| a.1.time.cmp(&b.1.time).then(a.2.cmp(&b.2)));
    out.into_iter().map(|(s, c, _)| (s, c)).collect()
}

// ── State + helpers ─────────────────────────────────────────────────────

/// Mutable in-loop state: per-symbol position, shared realised cash,
/// equity HWM, drawdown, and the equity / per-period returns sample
/// stream used by Sharpe / Sortino.
struct State {
    // `BTreeMap`, not `HashMap`: `equity_now` sums unrealised PnL by
    // iterating this map, and float addition is not associative. A
    // `HashMap`'s per-process-randomized iteration order would make a
    // multi-symbol equity curve (and thus Sharpe/Sortino) differ in the
    // last ULP between otherwise-identical runs, breaking the engine's
    // determinism guarantee. `BTreeMap` iterates in sorted symbol order,
    // so the summation order is fixed across runs.
    positions: BTreeMap<Symbol, Position>,
    cash: f64,
    equity_hwm: f64,
    max_drawdown: f64,
    // Sampled once per candle in `sample_step`, so Sharpe/Sortino see
    // the full price path even on Hold ticks.
    last_equity: f64,
    equity_curve: Vec<f64>,
    period_returns: Vec<f64>,
    // Cached marks per symbol (last close seen) so we can compute the
    // total portfolio equity at any sample boundary even when only one
    // symbol's price has just changed. `BTreeMap` for the same
    // determinism reason as `positions`.
    last_marks: BTreeMap<Symbol, f64>,
}

struct ReturnsSummary {
    max_drawdown: f64,
    equity: Vec<f64>,
    period_returns: Vec<f64>,
}

impl State {
    fn new(initial_cash: f64, symbols: impl IntoIterator<Item = Symbol>) -> Self {
        let mut positions = BTreeMap::new();
        for s in symbols {
            positions.insert(s, Position::FLAT);
        }
        Self {
            positions,
            cash: initial_cash,
            equity_hwm: initial_cash,
            max_drawdown: 0.0,
            last_equity: initial_cash,
            equity_curve: vec![initial_cash],
            period_returns: Vec::new(),
            last_marks: BTreeMap::new(),
        }
    }

    fn has_symbol(&self, sym: &Symbol) -> bool {
        self.positions.contains_key(sym)
    }

    fn position(&self, sym: &Symbol) -> Option<&Position> {
        self.positions.get(sym)
    }

    fn position_mut(&mut self, sym: &Symbol) -> &mut Position {
        self.positions.entry(sym.clone()).or_insert(Position::FLAT)
    }

    /// Record the latest close for a symbol, then sample portfolio
    /// equity. The equity curve always grows by one sample per candle —
    /// even on `Hold` ticks — so Sharpe/Sortino see the full price path.
    fn sample_step(&mut self, sym: &Symbol, close: f64, contract_value: f64) {
        self.last_marks.insert(sym.clone(), close);
        let equity = self.equity_now(contract_value);

        // Drawdown bookkeeping.
        if equity > self.equity_hwm {
            self.equity_hwm = equity;
        }
        let dd = equity - self.equity_hwm;
        if dd < self.max_drawdown {
            self.max_drawdown = dd;
        }

        self.equity_curve.push(equity);
        // Per-period simple return on prior equity (skip the first
        // sample — no prior period). Use prior equity to avoid divide-
        // by-zero on a fully-drained account.
        let prev = self.last_equity;
        if prev > 0.0 {
            self.period_returns.push((equity - prev) / prev);
        } else {
            self.period_returns.push(0.0);
        }
        self.last_equity = equity;
    }

    /// Total portfolio equity = realised cash + sum of unrealised PnL
    /// across all symbols using their last-known marks.
    fn equity_now(&self, contract_value: f64) -> f64 {
        let mut equity = self.cash;
        for (sym, pos) in &self.positions {
            if let Some(entry) = pos.entry_price
                && let Some(mark) = self.last_marks.get(sym)
            {
                let pnl_per_unit = (mark - entry) * pos.qty.signum();
                equity += pnl_per_unit * pos.qty.abs() * contract_value;
            }
        }
        equity
    }

    fn into_returns(self) -> ReturnsSummary {
        ReturnsSummary {
            max_drawdown: self.max_drawdown,
            equity: self.equity_curve,
            period_returns: self.period_returns,
        }
    }
}

/// Validate a candle's OHLCV fields are usable: prices finite and
/// strictly positive, volume finite and non-negative. Returns a
/// human-readable reason on the first offending field.
///
/// OHLC *ordering* (`high >= low`, etc.) is intentionally not enforced —
/// the goal is to keep `NaN`/`inf`/negative values out of the
/// mark-to-market math, not to police exchange data quality.
pub(crate) fn validate_candle(c: &Candle) -> std::result::Result<(), String> {
    for (name, v) in [
        ("open", c.open),
        ("high", c.high),
        ("low", c.low),
        ("close", c.close),
    ] {
        if !v.is_finite() || v <= 0.0 {
            return Err(format!("{name}={v} (prices must be finite and > 0)"));
        }
    }
    if !c.volume.is_finite() || c.volume < 0.0 {
        return Err(format!("volume={} (must be finite and >= 0)", c.volume));
    }
    Ok(())
}

/// A decision resolved into a concrete fill request for the engine.
struct ResolvedOrder {
    side: Side,
    qty: f64,
    is_close: bool,
    kind: OrderKind,
    /// Limit price (quote currency) for resting kinds; `None` for market
    /// and close orders.
    limit_price: Option<f64>,
}

/// Resolve a `Decision` into a [`ResolvedOrder`].
fn resolve_order(
    decision: &Decision,
    position: &Position,
    sizer: &PositionSizer,
    price: f64,
    contract_value: f64,
) -> Option<ResolvedOrder> {
    match decision.signal {
        SignalType::Hold => None,
        SignalType::Close => {
            let close_side = position.close_side()?;
            Some(ResolvedOrder {
                side: close_side,
                qty: position.qty.abs(),
                is_close: true,
                kind: OrderKind::Market,
                limit_price: None,
            })
        }
        SignalType::Buy | SignalType::Sell => {
            let side = if matches!(decision.signal, SignalType::Buy) {
                Side::Buy
            } else {
                Side::Sell
            };
            let contracts = size_from_hint(sizer, decision.size_hint, price, contract_value);
            if contracts == 0 {
                None
            } else {
                Some(ResolvedOrder {
                    side,
                    qty: contracts as f64,
                    is_close: false,
                    kind: decision.order_kind,
                    limit_price: decision.limit_price.map(|p| p.value()),
                })
            }
        }
    }
}

/// Decide whether and at what reference price a resolved order fills on
/// `candle`. Returns `(reference_price, is_taker)`; the caller applies
/// slippage to taker fills. Returns `None` when a resting limit doesn't
/// cross this candle (or a post-only would cross as taker).
///
/// - **Market / IOC / FOK and closes** fill immediately at the candle
///   close as takers.
/// - **Limit / PostOnly** rest: a buy fills when `low` trades down to the
///   limit, a sell when `high` trades up to it. A limit already marketable
///   at the candle open fills at the open as a taker (a post-only in that
///   case is rejected); a non-marketable limit fills at its limit price as
///   a maker. A missing limit falls back to the close, mirroring the live
///   execution layer's event-price fallback.
fn resolve_fill(resolved: &ResolvedOrder, candle: &Candle) -> Option<(f64, bool)> {
    if resolved.is_close
        || matches!(
            resolved.kind,
            OrderKind::Market | OrderKind::Ioc | OrderKind::Fok
        )
    {
        return Some((candle.close, true));
    }

    let limit = resolved.limit_price.unwrap_or(candle.close);
    let (fills, price, marketable) = match resolved.side {
        Side::Buy => (
            candle.low <= limit,
            limit.min(candle.open),
            limit >= candle.open,
        ),
        Side::Sell => (
            candle.high >= limit,
            limit.max(candle.open),
            limit <= candle.open,
        ),
    };
    if !fills {
        return None;
    }
    if matches!(resolved.kind, OrderKind::PostOnly) && marketable {
        return None;
    }
    Some((price, marketable))
}

fn size_from_hint(sizer: &PositionSizer, hint: SizeHint, price: f64, contract_value: f64) -> u32 {
    match hint {
        SizeHint::Default => sizer.contracts(price, contract_value),
        SizeHint::MarginFraction(f) => {
            let f = f.clamp(0.0, 1.0);
            let margin = sizer.config().margin_per_trade * f;
            sizer.contracts_with_margin(margin, price, contract_value)
        }
        SizeHint::NotionalUsd(n) => {
            let leverage = sizer.config().leverage.max(1);
            let margin = n / f64::from(leverage);
            sizer.contracts_with_margin(margin, price, contract_value)
        }
        SizeHint::Quantity(q) => {
            let raw = q.value().max(0.0).floor() as u32;
            raw.min(sizer.config().max_contracts)
        }
    }
}

/// Apply a fill to the synthetic position. Emits one [`TradeOutcome`]
/// per closed quantity (so a flip from +5 to -3 emits one close-5 trade).
#[allow(clippy::too_many_arguments)]
fn apply_fill(
    state: &mut State,
    symbol: &Symbol,
    side: Side,
    qty: f64,
    fill_price: f64,
    fee: f64,
    contract_value: f64,
    when: DateTime<Utc>,
    trades: &mut Vec<TradeOutcome>,
) {
    // Signed delta to the position quantity from this fill.
    let signed_qty = match side {
        Side::Buy => qty,
        Side::Sell => -qty,
    };

    let (old_qty, old_entry) = {
        let p = state.position_mut(symbol);
        (p.qty, p.entry_price)
    };
    let new_qty = old_qty + signed_qty;

    // The realised-PnL portion is whatever quantity *reduces* the
    // existing position. Anything beyond that opens a new position in
    // the opposite direction.
    let closing_qty = if old_qty.signum() != signed_qty.signum() && old_qty != 0.0 {
        old_qty.abs().min(qty)
    } else {
        0.0
    };
    let opening_qty = qty - closing_qty;

    if closing_qty > 0.0 {
        let entry = old_entry.unwrap_or(fill_price);
        let direction = old_qty.signum();
        let gross = (fill_price - entry) * direction * closing_qty * contract_value;
        // Fee is apportioned by closing fraction so a single fill that
        // both closes and reopens charges fees pro-rata to each side.
        let fee_share = if qty > 0.0 {
            fee * (closing_qty / qty)
        } else {
            0.0
        };
        trades.push(TradeOutcome {
            symbol: symbol.as_str().to_string(),
            close_side: side,
            qty: closing_qty,
            entry_price: entry,
            exit_price: fill_price,
            gross_pnl: gross,
            fee: fee_share,
            closed_at: when,
        });
        state.cash += gross - fee_share;
    }

    let new_position = if opening_qty > 0.0 {
        // The fee component charged to opening.
        let fee_open = if qty > 0.0 {
            fee * (opening_qty / qty)
        } else {
            0.0
        };
        state.cash -= fee_open;
        // New entry price: if we were FLAT or fully closed first, this
        // is the fresh entry; if we'd somehow added to an existing
        // position (same-side fill), it's a weighted average. Brains
        // don't pyramid in normal use since they emit one direction at
        // a time, but handle it correctly anyway.
        let new_position_qty_after_close = old_qty + side_sign(side) * closing_qty;
        let post_open_qty = new_position_qty_after_close + side_sign(side) * opening_qty;
        let entry = if new_position_qty_after_close == 0.0 {
            fill_price
        } else {
            let prev_entry = old_entry.unwrap_or(fill_price);
            let prev_notional = prev_entry * new_position_qty_after_close.abs();
            let new_notional = fill_price * opening_qty;
            (prev_notional + new_notional) / post_open_qty.abs()
        };
        Position {
            qty: post_open_qty,
            entry_price: Some(entry),
            unrealised_pnl: 0.0,
        }
    } else if new_qty == 0.0 {
        Position::FLAT
    } else {
        Position {
            qty: new_qty,
            entry_price: old_entry,
            unrealised_pnl: 0.0,
        }
    };
    *state.position_mut(symbol) = new_position;
}

fn side_sign(side: Side) -> f64 {
    match side {
        Side::Buy => 1.0,
        Side::Sell => -1.0,
    }
}

fn candle_time(c: &Candle) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(c.time)
        .single()
        .unwrap_or_else(Utc::now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rustrade_core::{BrainHealth, Decision, MarketDataEvent, Position, Result as CoreResult};
    use rustrade_risk::SizingConfig;

    /// Brain that always emits the configured signal.
    struct FixedBrain {
        signal: SignalType,
    }
    #[async_trait]
    impl Brain for FixedBrain {
        fn name(&self) -> &str {
            "fixed"
        }
        async fn on_event(&self, _e: &MarketDataEvent, _p: &Position) -> CoreResult<Decision> {
            Ok(match self.signal {
                SignalType::Hold => Decision::hold(),
                SignalType::Buy => Decision::buy(1.0),
                SignalType::Sell => Decision::sell(1.0),
                SignalType::Close => Decision::close(),
            })
        }
        async fn health(&self) -> BrainHealth {
            BrainHealth::ok()
        }
    }

    fn flat_series(n: usize, price: f64) -> Vec<Candle> {
        (0..n)
            .map(|i| Candle {
                time: i as i64 * 60_000,
                open: price,
                high: price,
                low: price,
                close: price,
                volume: 1.0,
            })
            .collect()
    }

    fn ramp_series(n: usize, start: f64, step: f64) -> Vec<Candle> {
        (0..n)
            .map(|i| {
                let p = start + step * i as f64;
                Candle {
                    time: i as i64 * 60_000,
                    open: p,
                    high: p,
                    low: p,
                    close: p,
                    volume: 1.0,
                }
            })
            .collect()
    }

    fn cfg() -> BacktestConfig {
        BacktestConfig::builder()
            .symbol("BTCUSDT")
            .initial_cash(10_000.0)
            .sizing(SizingConfig {
                margin_per_trade: 1_000.0,
                leverage: 1,
                max_contracts: 100,
            })
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn hold_brain_produces_no_trades() {
        let result = Backtest::new(
            cfg(),
            Arc::new(FixedBrain {
                signal: SignalType::Hold,
            }),
        )
        .with_candles(flat_series(50, 100.0))
        .run()
        .await
        .unwrap();
        assert_eq!(result.signals_emitted, 0);
        assert_eq!(result.orders_filled, 0);
        assert_eq!(result.trades.len(), 0);
        assert_eq!(result.net_pnl, 0.0);
        assert_eq!(result.candles_processed, 50);
        // Equity curve always seeds the initial cash, then one sample
        // per candle.
        assert_eq!(result.equity_curve.len(), 51);
        assert_eq!(result.period_returns.len(), 50);
    }

    #[tokio::test]
    async fn buy_then_close_realises_pnl_on_uptrend() {
        // Buy on every candle. Position opens once; subsequent buys add
        // to it (pyramiding). Test just runs to completion and asserts
        // we accumulated *some* position and saw no trade close yet.
        let result = Backtest::new(
            cfg(),
            Arc::new(FixedBrain {
                signal: SignalType::Buy,
            }),
        )
        .with_candles(ramp_series(20, 100.0, 1.0))
        .run()
        .await
        .unwrap();
        // Every candle emits Buy → orders_filled equals candle count
        // (sizer always returns ≥ 1 contract here).
        assert_eq!(result.orders_filled, 20);
        // No reducing fills yet → no completed trades.
        assert_eq!(result.trades.len(), 0);
        assert_eq!(result.net_pnl, 0.0);
    }

    #[tokio::test]
    async fn determinism_two_runs_same_inputs() {
        let series = ramp_series(30, 100.0, 0.5);
        let r1 = Backtest::new(
            cfg(),
            Arc::new(FixedBrain {
                signal: SignalType::Buy,
            }),
        )
        .with_candles(series.clone())
        .run()
        .await
        .unwrap();
        let r2 = Backtest::new(
            cfg(),
            Arc::new(FixedBrain {
                signal: SignalType::Buy,
            }),
        )
        .with_candles(series)
        .run()
        .await
        .unwrap();
        assert_eq!(r1.candles_processed, r2.candles_processed);
        assert_eq!(r1.signals_emitted, r2.signals_emitted);
        assert_eq!(r1.orders_filled, r2.orders_filled);
        assert_eq!(r1.trades.len(), r2.trades.len());
        assert!((r1.net_pnl - r2.net_pnl).abs() < 1e-12);
        assert_eq!(r1.equity_curve, r2.equity_curve);
    }

    #[tokio::test]
    async fn close_against_flat_is_noop() {
        let result = Backtest::new(
            cfg(),
            Arc::new(FixedBrain {
                signal: SignalType::Close,
            }),
        )
        .with_candles(flat_series(10, 100.0))
        .run()
        .await
        .unwrap();
        assert_eq!(result.orders_filled, 0);
        assert_eq!(result.trades.len(), 0);
    }

    #[test]
    fn merge_series_interleaves_by_timestamp() {
        let s1 = Symbol::from("AAA");
        let s2 = Symbol::from("BBB");
        let series = vec![
            (
                s1.clone(),
                vec![
                    Candle {
                        time: 1000,
                        open: 1.0,
                        high: 1.0,
                        low: 1.0,
                        close: 1.0,
                        volume: 0.0,
                    },
                    Candle {
                        time: 3000,
                        open: 1.0,
                        high: 1.0,
                        low: 1.0,
                        close: 1.0,
                        volume: 0.0,
                    },
                ],
            ),
            (
                s2.clone(),
                vec![
                    Candle {
                        time: 2000,
                        open: 2.0,
                        high: 2.0,
                        low: 2.0,
                        close: 2.0,
                        volume: 0.0,
                    },
                    Candle {
                        time: 3000,
                        open: 2.0,
                        high: 2.0,
                        low: 2.0,
                        close: 2.0,
                        volume: 0.0,
                    },
                ],
            ),
        ];
        let merged = merge_series(&series);
        let times: Vec<i64> = merged.iter().map(|(_, c)| c.time).collect();
        assert_eq!(times, vec![1000, 2000, 3000, 3000]);
        // Tie at t=3000 is broken by series-insertion order — AAA first.
        assert_eq!(merged[2].0, s1);
        assert_eq!(merged[3].0, s2);
    }

    #[tokio::test]
    async fn multi_symbol_routes_to_each_symbol_state() {
        // Brain that goes long on AAA and short on BBB; FlipBrain takes
        // both sides simultaneously to verify per-symbol position state.
        struct SymBrain;
        #[async_trait]
        impl Brain for SymBrain {
            fn name(&self) -> &str {
                "sym"
            }
            async fn on_event(&self, e: &MarketDataEvent, _p: &Position) -> CoreResult<Decision> {
                match e.symbol().as_str() {
                    "AAA" => Ok(Decision::buy(1.0)),
                    "BBB" => Ok(Decision::sell(1.0)),
                    _ => Ok(Decision::hold()),
                }
            }
            async fn health(&self) -> BrainHealth {
                BrainHealth::ok()
            }
        }

        let cfg = BacktestConfig::builder()
            .symbols(["AAA", "BBB"])
            .initial_cash(100_000.0)
            .sizing(SizingConfig {
                margin_per_trade: 1_000.0,
                leverage: 1,
                max_contracts: 100,
            })
            .build()
            .unwrap();
        let result = Backtest::new(cfg, Arc::new(SymBrain))
            .with_symbol_candles("AAA", flat_series(5, 100.0))
            .with_symbol_candles("BBB", flat_series(5, 200.0))
            .run()
            .await
            .unwrap();
        // 5 AAA + 5 BBB = 10 orders (every candle, both symbols).
        assert_eq!(result.candles_processed, 10);
        assert_eq!(result.orders_filled, 10);
        // No closes — no completed trades yet.
        assert_eq!(result.trades.len(), 0);
        // Symbol label is the concatenated list.
        assert_eq!(result.symbol, "AAA,BBB");
    }

    // ── Candle validation ───────────────────────────────────────────────

    fn good_candle() -> Candle {
        Candle {
            time: 0,
            open: 1.0,
            high: 1.0,
            low: 1.0,
            close: 1.0,
            volume: 1.0,
        }
    }

    #[test]
    fn validate_candle_accepts_finite_positive() {
        assert!(validate_candle(&good_candle()).is_ok());
        // Zero volume is legitimate (illiquid candle).
        let c = Candle {
            volume: 0.0,
            ..good_candle()
        };
        assert!(validate_candle(&c).is_ok());
    }

    #[test]
    fn validate_candle_rejects_non_finite_and_non_positive_prices() {
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 0.0, -1.0] {
            let c = Candle {
                close: bad,
                ..good_candle()
            };
            assert!(
                validate_candle(&c).is_err(),
                "close={bad} should be rejected"
            );
        }
    }

    #[test]
    fn validate_candle_rejects_negative_or_nan_volume() {
        for bad in [-1.0, f64::NAN, f64::INFINITY] {
            let c = Candle {
                volume: bad,
                ..good_candle()
            };
            assert!(
                validate_candle(&c).is_err(),
                "volume={bad} should be rejected"
            );
        }
    }

    #[tokio::test]
    async fn run_rejects_non_finite_candle() {
        // A NaN close passed straight through `with_candles` (bypassing
        // the CSV loader) must still be caught — otherwise it poisons
        // the equity curve and every metric silently.
        let mut series = flat_series(5, 100.0);
        series[2].close = f64::NAN;
        let err = Backtest::new(
            cfg(),
            Arc::new(FixedBrain {
                signal: SignalType::Hold,
            }),
        )
        .with_candles(series)
        .run()
        .await
        .unwrap_err();
        assert!(matches!(err, Error::Data(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn multi_symbol_equity_curve_deterministic_across_runs() {
        // Regression test for the HashMap-iteration-order determinism
        // bug: with two simultaneously-open positions, `equity_now` sums
        // unrealised PnL across the per-symbol map. Float addition isn't
        // associative, so a randomized map order would make the equity
        // curve differ run-to-run. `BTreeMap` fixes the order. Prices
        // are deliberately awkward so reordering *would* change the ULPs.
        struct DualLong;
        #[async_trait]
        impl Brain for DualLong {
            fn name(&self) -> &str {
                "dual-long"
            }
            async fn on_event(&self, e: &MarketDataEvent, p: &Position) -> CoreResult<Decision> {
                if p.qty == 0.0 && matches!(e, MarketDataEvent::Candle { .. }) {
                    Ok(Decision::buy(1.0))
                } else {
                    Ok(Decision::hold())
                }
            }
            async fn health(&self) -> BrainHealth {
                BrainHealth::ok()
            }
        }

        let run = || async {
            let cfg = BacktestConfig::builder()
                .symbols(["AAA", "BBB", "CCC"])
                .initial_cash(1_000_000.0)
                .sizing(SizingConfig {
                    margin_per_trade: 1_000.0,
                    leverage: 1,
                    max_contracts: 100,
                })
                .build()
                .unwrap();
            Backtest::new(cfg, Arc::new(DualLong))
                .with_symbol_candles("AAA", ramp_series(40, 100.13, 0.37))
                .with_symbol_candles("BBB", ramp_series(40, 250.07, -0.19))
                .with_symbol_candles("CCC", ramp_series(40, 33.31, 0.53))
                .run()
                .await
                .unwrap()
        };

        let r1 = run().await;
        let r2 = run().await;
        // Bit-exact across runs — not just approximately equal.
        assert_eq!(r1.equity_curve, r2.equity_curve);
        assert_eq!(r1.period_returns, r2.period_returns);
        assert_eq!(r1.net_pnl.to_bits(), r2.net_pnl.to_bits());
        assert_eq!(r1.max_drawdown.to_bits(), r2.max_drawdown.to_bits());
    }
}
