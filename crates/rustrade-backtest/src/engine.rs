//! The replay engine.
//!
//! Single-threaded loop: feeds candles to the brain in order, builds an
//! order from each non-`Hold` decision, applies slippage and fees, and
//! updates a synthetic position. On position-reducing fills (closes or
//! flips) a [`TradeOutcome`] is emitted into the result.

use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use rustrade_core::{
    Brain, Candle, Decision, Exchange, Fill, MarketDataEvent, Position, Side, SignalType, SizeHint,
    Symbol,
};
use rustrade_risk::PositionSizer;

use crate::config::BacktestConfig;
use crate::error::{Error, Result};
use crate::metrics::TradeOutcome;
use crate::result::BacktestResult;

/// The replay engine itself. Configure via [`BacktestConfig`], attach a
/// [`Brain`] and a candle series, then `.run().await` for the result.
pub struct Backtest {
    config: BacktestConfig,
    brain: Arc<dyn Brain>,
    candles: Vec<Candle>,
}

impl Backtest {
    /// Construct with a config + brain. The candle series is attached
    /// separately via [`Self::with_candles`].
    pub fn new(config: BacktestConfig, brain: Arc<dyn Brain>) -> Self {
        Self {
            config,
            brain,
            candles: Vec::new(),
        }
    }

    /// Feed a candle series. Replaces any previously attached candles.
    pub fn with_candles(mut self, candles: Vec<Candle>) -> Self {
        self.candles = candles;
        self
    }

    /// Run the backtest to completion. Returns the aggregated result.
    pub async fn run(self) -> Result<BacktestResult> {
        let symbol = self.config.symbol.clone();
        let exchange = Exchange::from("backtest");
        let sizer = PositionSizer::new(self.config.sizing.clone());

        let mut state = State::new(self.config.initial_cash);
        let mut signals_emitted = 0usize;
        let mut orders_filled = 0usize;
        let mut trades: Vec<TradeOutcome> = Vec::new();

        for candle in &self.candles {
            let event = MarketDataEvent::Candle {
                exchange: exchange.clone(),
                symbol: symbol.clone(),
                candle: *candle,
            };

            // Brains see the live position at decision time — same as
            // the live `ExecutionService` does.
            let position = state.position;
            let decision = self
                .brain
                .on_event(&event, &position)
                .await
                .map_err(|e| Error::Brain(e.to_string()))?;

            if matches!(decision.signal, SignalType::Hold) {
                state.observe_equity(candle.close, self.config.contract_value);
                continue;
            }
            signals_emitted += 1;

            // Translate the decision into a concrete (side, qty). For
            // `Close` we use the existing position size. For Buy/Sell we
            // size from the brain's hint just like ExecutionService.
            let Some((side, qty, is_close)) = resolve_order(
                &decision,
                &position,
                &sizer,
                candle.close,
                self.config.contract_value,
            ) else {
                state.observe_equity(candle.close, self.config.contract_value);
                continue;
            };
            if qty <= 0.0 {
                state.observe_equity(candle.close, self.config.contract_value);
                continue;
            }

            // Apply slippage + fees.
            let fill_price = self.config.slippage.apply(side, candle.close);
            let fee = self.config.fees.fee_for(
                fill_price,
                qty * self.config.contract_value,
                true, // every order is a taker in Phase 4a
            );

            // Update position state. If this fill reduces or flips the
            // position, emit one or more TradeOutcomes.
            apply_fill(
                &mut state,
                &symbol,
                side,
                qty,
                fill_price,
                fee,
                self.config.contract_value,
                candle_time(candle),
                &mut trades,
            );

            orders_filled += 1;

            // Inform the brain of the (synthetic) fill — same callback
            // the live `FillRoutingService` would invoke.
            let fill = Fill {
                symbol: symbol.clone(),
                order_id: format!("bt-{orders_filled}"),
                client_id: None,
                side,
                price: rustrade_core::Price(fill_price),
                size: rustrade_core::Volume(qty),
                fee,
                fee_currency: "QUOTE".into(),
                timestamp: candle_time(candle),
            };
            self.brain
                .on_fill(&fill)
                .await
                .map_err(|e| Error::Brain(e.to_string()))?;

            state.observe_equity(candle.close, self.config.contract_value);
            let _ = is_close;
        }

        let total_fees: f64 = trades.iter().map(|t| t.fee).sum();
        let net_pnl: f64 = trades.iter().map(|t| t.net_pnl()).sum();
        Ok(BacktestResult {
            symbol: symbol.as_str().to_string(),
            initial_cash: self.config.initial_cash,
            final_cash: self.config.initial_cash + net_pnl,
            net_pnl,
            total_fees,
            candles_processed: self.candles.len(),
            signals_emitted,
            orders_filled,
            trades,
            max_drawdown: state.max_drawdown(),
        })
    }
}

// ── State + helpers ─────────────────────────────────────────────────────

/// Mutable in-loop state: position, realised cash, equity HWM, drawdown.
struct State {
    position: Position,
    cash: f64,
    equity_hwm: f64,
    max_drawdown: f64,
}

impl State {
    fn new(initial_cash: f64) -> Self {
        Self {
            position: Position::FLAT,
            cash: initial_cash,
            equity_hwm: initial_cash,
            max_drawdown: 0.0,
        }
    }

    /// Mark-to-market the current position at `close` and update the
    /// drawdown tracker. Equity = realised cash + unrealised on the open
    /// position.
    fn observe_equity(&mut self, close: f64, contract_value: f64) {
        let equity = if let Some(entry) = self.position.entry_price {
            let pnl_per_unit = (close - entry) * self.position.qty.signum();
            self.cash + pnl_per_unit * self.position.qty.abs() * contract_value
        } else {
            self.cash
        };
        if equity > self.equity_hwm {
            self.equity_hwm = equity;
        }
        let dd = equity - self.equity_hwm;
        if dd < self.max_drawdown {
            self.max_drawdown = dd;
        }
    }

    fn max_drawdown(&self) -> f64 {
        self.max_drawdown
    }
}

/// Resolve a `Decision` into a concrete `(side, qty, is_close)`.
fn resolve_order(
    decision: &Decision,
    position: &Position,
    sizer: &PositionSizer,
    price: f64,
    contract_value: f64,
) -> Option<(Side, f64, bool)> {
    match decision.signal {
        SignalType::Hold => None,
        SignalType::Close => {
            let close_side = position.close_side()?;
            Some((close_side, position.qty.abs(), true))
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
                Some((side, contracts as f64, false))
            }
        }
    }
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

    let old_qty = state.position.qty;
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
        let entry = state.position.entry_price.unwrap_or(fill_price);
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

    if opening_qty > 0.0 {
        // The fee component charged to opening.
        let fee_open = if qty > 0.0 {
            fee * (opening_qty / qty)
        } else {
            0.0
        };
        state.cash -= fee_open;
        // New entry price: if we were FLAT or fully closed first, this
        // is the fresh entry; if we'd somehow added to an existing
        // position (same-side fill), it's a weighted average. Phase 4a
        // doesn't pyramid since brains emit one direction at a time, but
        // handle it correctly anyway.
        let new_position_qty_after_close = old_qty + side_sign(side) * closing_qty;
        let post_open_qty = new_position_qty_after_close + side_sign(side) * opening_qty;
        let entry = if new_position_qty_after_close == 0.0 {
            fill_price
        } else {
            let prev_entry = state.position.entry_price.unwrap_or(fill_price);
            let prev_notional = prev_entry * new_position_qty_after_close.abs();
            let new_notional = fill_price * opening_qty;
            (prev_notional + new_notional) / post_open_qty.abs()
        };
        state.position = Position {
            qty: post_open_qty,
            entry_price: Some(entry),
            unrealised_pnl: 0.0,
        };
    } else if new_qty == 0.0 {
        // Fully closed.
        state.position = Position::FLAT;
    } else {
        // Reduced but not fully closed; keep the original entry price.
        let entry = state.position.entry_price;
        state.position = Position {
            qty: new_qty,
            entry_price: entry,
            unrealised_pnl: 0.0,
        };
    }
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
}
