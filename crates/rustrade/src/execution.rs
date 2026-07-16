//! Framework-side execution: subscribes to the [`MarketDataBus`] and
//! routes each event through the risk gates to the [`ExchangeClient`].
//!
//! # Pre-trade gate sequence
//!
//! For every non-`Hold` decision the brain emits, the gates run in this
//! exact order. Each one that blocks emits a structured `tracing` event
//! and the order is not placed.
//!
//! 1. `SessionPnl::is_session_halted(symbol)` — daily drawdown cap.
//! 2. `CircuitBreaker::is_tripped(symbol)` — rolling-window loss breaker.
//! 3. `PositionSizer::contracts(price, contract_value)` — `0` means the
//!    sized order would be too small to send.
//!
//! The first three are *risk-layer* concerns and never raise an error
//! that aborts the service. A `Decision::Close` for a flat position is
//! also a silent skip (logged at debug level).
//!
//! ## De-risking exits bypass Gates 1 & 2
//!
//! Gates 1 and 2 protect against *taking on new risk*. A genuine
//! de-risking exit — a `Decision::Close` against a **non-flat** position —
//! must always be allowed through so an automated bot that has halted (or
//! tripped its breaker) can still flatten a losing position instead of
//! leaving it to ride on resting stops alone. Such exits therefore skip
//! the Gate 1 / Gate 2 *checks* (they do not skip Gate 3, which already
//! exempts exits by construction).
//!
//! The exemption is deliberately **tight**: it fires only for
//! [`SignalType::Close`] *and* only when the current position is not flat
//! (`Position::close_side().is_some()`), which is the exact same notion
//! [`build_order`](ExecutionService::build_order)'s `Close` arm uses to
//! turn the decision into a reduce-only order sized to `position.qty`.
//! A `Close` in that arm can only ever reduce or flatten — never open,
//! increase, or flip — so an exempted signal can never take on new risk.
//! Every `Buy`/`Sell` (open or increase) stays fully gated, and a `Close`
//! with nothing to reduce (flat) is *not* exempted: the safe default is to
//! gate unless the order is provably reducing.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use chrono::Utc;
use rustrade_core::{
    Brain, Capability, Decision, ExchangeClient, MarketDataBus, MarketDataEvent, Order, OrderKind,
    Position, Price, Side, Signal, SignalBus, SignalType, SizeHint, StopAttachment, Symbol, Volume,
};
use rustrade_risk::{PortfolioState, PositionSizer, SizingConfig};
use rustrade_supervisor::{RestartPolicy, TradingService};
use tokio::sync::broadcast::error::RecvError;
use tokio_util::sync::CancellationToken;

use crate::pending::PendingEntryLedger;
use crate::risk_state::{PortfolioRiskState, PositionCache, RiskStateMap};

/// Shared inputs every [`ExecutionService`] needs.
///
/// Cheaply cloneable. Constructed once by [`Bot`](crate::Bot) and
/// shared across all per-brain execution services.
#[derive(Clone)]
pub(crate) struct ExecutionContext {
    pub exchange: Arc<dyn ExchangeClient>,
    pub bus: MarketDataBus,
    pub signals: SignalBus,
    pub positions: PositionCache,
    pub risk: RiskStateMap,
    /// Account-wide portfolio risk: the pre-trade gate reads it (entries only);
    /// the risk sweep maintains its daily-loss latch.
    pub portfolio: PortfolioRiskState,
    /// Per-symbol sizing resolver (default + overrides). The execution
    /// service sizes each order with the config for the event's symbol.
    pub sizing: Arc<SymbolSizing>,
    /// Set when order tracking is wired (`Bot::with_order_tracking`) and the
    /// adapter advertises `Capability::OrderTracking`. Resting orders the
    /// service places are recorded here so the reaper can age them out.
    pub order_tracker: Option<crate::order_tracker::OrderTracker>,
    /// Set when bracket (SL+TP / OCO) orders are active — the adapter
    /// supports `StopOrders` + `OrderTracking` and a fill source is wired.
    /// A market entry carrying both `stop_price` and `take_profit_price`
    /// then gets two reduce-only protective orders registered as an OCO
    /// pair here; the `FillRoutingService` cancels the sibling on fill.
    pub oco: Option<crate::order_tracker::OcoRegistry>,
    /// What to do when a bracket entry fills but its stop-loss leg fails
    /// to place (see [`BracketFailurePolicy`](crate::BracketFailurePolicy)).
    pub bracket_failure_policy: crate::bot::BracketFailurePolicy,
    /// Outstanding entry reservations: orders that passed the portfolio
    /// gate but whose fills aren't visible in the position cache yet.
    /// Makes the gate check-and-reserve, so concurrent brains can't both
    /// slip through `max_concurrent_positions` / `max_gross_exposure`.
    pub pending: PendingEntryLedger,
}

/// Resolves the [`SizingConfig`] to use for a given symbol: a per-symbol
/// override if one exists, otherwise the bot-wide default.
pub(crate) struct SymbolSizing {
    default: SizingConfig,
    per_symbol: HashMap<Symbol, SizingConfig>,
}

impl SymbolSizing {
    pub(crate) fn new(default: SizingConfig, per_symbol: HashMap<Symbol, SizingConfig>) -> Self {
        Self {
            default,
            per_symbol,
        }
    }

    pub(crate) fn for_symbol(&self, symbol: &Symbol) -> &SizingConfig {
        self.per_symbol.get(symbol).unwrap_or(&self.default)
    }
}

/// Per-brain execution loop with full risk gating + order placement.
pub struct ExecutionService {
    name: String,
    brain: Arc<dyn Brain>,
    ctx: ExecutionContext,
    /// Symbols this brain owns (`Brain::owned_symbols`), cached at
    /// construction. `None` ⇒ the brain sees every symbol; `Some` ⇒ events
    /// for other symbols are skipped before `on_event`.
    owned: Option<std::collections::HashSet<Symbol>>,
    events_processed: AtomicU64,
    events_dropped: AtomicU64,
    orders_placed: AtomicU64,
    orders_blocked: AtomicU64,
}

impl ExecutionService {
    pub(crate) fn new(brain: Arc<dyn Brain>, ctx: ExecutionContext) -> Self {
        let name = format!("execution[{}]", brain.name());
        let owned = brain
            .owned_symbols()
            .map(|syms| syms.into_iter().collect::<std::collections::HashSet<_>>());
        Self {
            name,
            brain,
            ctx,
            owned,
            events_processed: AtomicU64::new(0),
            events_dropped: AtomicU64::new(0),
            orders_placed: AtomicU64::new(0),
            orders_blocked: AtomicU64::new(0),
        }
    }

    /// Total events the brain has been called with.
    pub fn events_processed(&self) -> u64 {
        self.events_processed.load(Ordering::Relaxed)
    }
    /// Total events dropped by the broadcast bus due to slow consumption.
    pub fn events_dropped(&self) -> u64 {
        self.events_dropped.load(Ordering::Relaxed)
    }
    /// Total orders successfully passed to the exchange.
    pub fn orders_placed(&self) -> u64 {
        self.orders_placed.load(Ordering::Relaxed)
    }
    /// Total decisions blocked by a risk gate or by the sizer returning 0.
    pub fn orders_blocked(&self) -> u64 {
        self.orders_blocked.load(Ordering::Relaxed)
    }

    async fn position_for(&self, symbol: &Symbol) -> Position {
        self.ctx
            .positions
            .read()
            .await
            .get(symbol)
            .copied()
            .unwrap_or(Position::FLAT)
    }

    async fn handle_event(&self, event: &MarketDataEvent) -> anyhow::Result<()> {
        let symbol = event.symbol().clone();

        // Ownership filter: a brain that declared `owned_symbols` only ever
        // sees its own symbols — others are dropped before `on_event`, so
        // two brains can't both act on one symbol (also enforced at startup).
        if let Some(owned) = &self.owned
            && !owned.contains(&symbol)
        {
            return Ok(());
        }

        let position = self.position_for(&symbol).await;

        let decision = self.brain.on_event(event, &position).await?;
        self.events_processed.fetch_add(1, Ordering::Relaxed);

        let signal = decision.signal;
        if matches!(signal, SignalType::Hold) {
            return Ok(());
        }

        // Publish the brain's intent *before* gates run. Subscribers see
        // the full signal stream; whether each was acted on is visible
        // from order placement metrics.
        let published = self.ctx.signals.publish(Signal {
            id: format!("{}-{}", self.brain.name(), self.events_processed()),
            symbol: symbol.as_str().to_string(),
            kind: signal,
            confidence: decision.confidence,
            timestamp: Utc::now(),
            source: self.brain.name().to_string(),
            metadata: decision.metadata.clone(),
        });
        let _ = published;

        // A genuine de-risking exit: a `Close` against a non-flat position.
        // `build_order`'s `Close` arm turns exactly this case into a
        // reduce-only order sized to `position.qty` — it can only reduce or
        // flatten, never open/increase/flip — so it can never take on new
        // risk and is allowed to bypass the Gate 1 / Gate 2 *checks* below.
        // The predicate is intentionally tight: any `Buy`/`Sell`, and a
        // `Close` with nothing to reduce (flat), is NOT exempt. Uses the
        // same `position` snapshot `build_order` will size against, so the
        // exemption decision and the order built can never diverge. Safe
        // default: gate unless the order is provably reducing.
        let is_reduce_only_exit =
            matches!(signal, SignalType::Close) && position.close_side().is_some();

        if !is_reduce_only_exit {
            // ── Gate 1: session PnL halt ──────────────────────────────
            if let Some(risk) = self.ctx.risk.read().await.get(&symbol)
                && risk.session_pnl.is_session_halted()
            {
                self.orders_blocked.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    service = %self.name,
                    symbol = %symbol,
                    signal = %signal,
                    "decision blocked: session PnL halted"
                );
                return Ok(());
            }

            // ── Gate 2: circuit breaker ───────────────────────────────
            if let Some(risk) = self.ctx.risk.read().await.get(&symbol)
                && risk.circuit_breaker.is_tripped()
            {
                self.orders_blocked.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    service = %self.name,
                    symbol = %symbol,
                    signal = %signal,
                    cooldown_secs = ?risk.circuit_breaker.cooldown_remaining(),
                    "decision blocked: circuit breaker tripped"
                );
                return Ok(());
            }
        } else {
            tracing::debug!(
                service = %self.name,
                symbol = %symbol,
                signal = %signal,
                "reduce-only exit — bypassing session-halt / circuit-breaker checks"
            );
        }

        // ── Build the order ───────────────────────────────────────────
        let order = match self.build_order(event, &symbol, &position, &decision).await {
            Some(o) => o,
            None => return Ok(()),
        };

        // ── Place ────────────────────────────────────────────────────
        match self.ctx.exchange.place_order(&order).await {
            Ok(id) => {
                self.orders_placed.fetch_add(1, Ordering::Relaxed);
                // Track resting orders so the reaper can age them out. The
                // tracker itself ignores market orders (they never rest).
                if let Some(tracker) = &self.ctx.order_tracker {
                    tracker.record(id.clone(), &order).await;
                }
                tracing::info!(
                    service = %self.name,
                    symbol = %symbol,
                    side = ?order.side,
                    size = %order.size,
                    reduce_only = order.reduce_only,
                    order_id = %id,
                    "order placed"
                );
                // Bracket entry: place the SL + TP protective legs now that
                // the market entry is accepted.
                if self.is_bracket(&decision, order.kind)
                    && let (Some(sl), Some(tp)) = (decision.stop_price, decision.take_profit_price)
                {
                    self.place_brackets(&symbol, order.side, order.size, sl, tp)
                        .await;
                }
            }
            Err(e) => {
                // An entry's portfolio reservation must not outlive the
                // rejected order (exits never reserve).
                if matches!(signal, SignalType::Buy | SignalType::Sell) {
                    self.ctx.pending.release(&symbol).await;
                }
                tracing::error!(
                    service = %self.name,
                    symbol = %symbol,
                    error = %e,
                    "exchange rejected order — risk state unchanged"
                );
            }
        }
        Ok(())
    }

    /// Atomic portfolio gate: assemble the aggregate account state —
    /// open-position count + gross exposure (Σ `|qty|·entry·contract_value`)
    /// from the position cache **plus outstanding pending-entry
    /// reservations**, and the account net PnL (Σ per-symbol session net)
    /// from the risk map — run [`check_entry`](rustrade_risk::PortfolioRisk::check_entry),
    /// and on success record this entry's reservation, all under the
    /// ledger lock. Concurrent brains serialise here, so two entries
    /// can't both slip through `max_concurrent_positions` or the
    /// gross-exposure cap before either fill is visible.
    ///
    /// The caller owns the reservation's lifecycle: it is released on
    /// exchange rejection (here, in `handle_event`), on the fill-driven
    /// position-cache refresh (`FillRoutingService`), or by TTL.
    async fn check_and_reserve_entry(
        &self,
        symbol: &Symbol,
        new_notional: f64,
    ) -> Result<(), rustrade_risk::PortfolioBlock> {
        let mut pending = self.ctx.pending.lock().await;

        let (open_positions, gross_exposure, symbol_already_open) = {
            let positions = self.ctx.positions.read().await;
            let mut open = 0u32;
            let mut gross = 0.0;
            for (sym, p) in positions.iter() {
                if p.is_flat() {
                    continue;
                }
                open += 1;
                let px = p.entry_price.unwrap_or(0.0);
                gross += p.qty.abs() * px * self.ctx.exchange.contract_value(sym);
            }
            let cache_open = |s: &Symbol| positions.get(s).is_some_and(|p| !p.is_flat());
            // Reservations whose symbol is already open in the cache add
            // exposure but no new concurrency slot — same rule the gate
            // applies to the entry under consideration.
            let open = open + pending.new_slots(cache_open);
            let already = cache_open(symbol) || pending.contains(symbol);
            (open, gross + pending.gross_notional(), already)
        };
        let account_net_pnl = {
            let risk = self.ctx.risk.read().await;
            risk.values().map(|sr| sr.session_pnl.net_pnl()).sum()
        };

        self.ctx
            .portfolio
            .read()
            .await
            .check_entry(PortfolioState {
                open_positions,
                gross_exposure,
                new_notional,
                symbol_already_open,
                account_net_pnl,
            })?;

        pending.reserve(symbol.clone(), new_notional);
        Ok(())
    }

    async fn build_order(
        &self,
        event: &MarketDataEvent,
        symbol: &Symbol,
        position: &Position,
        decision: &Decision,
    ) -> Option<Order> {
        match decision.signal {
            SignalType::Hold => None,
            SignalType::Close => {
                // Use the actual position size; size hint is ignored.
                let Some(close_side) = position.close_side() else {
                    tracing::debug!(
                        service = %self.name,
                        symbol = %symbol,
                        "decision=Close but position is flat — nothing to do"
                    );
                    return None;
                };
                let size = Volume(position.qty.abs());
                Some(Order::market(symbol.clone(), close_side, size).with_reduce_only(true))
            }
            SignalType::Buy | SignalType::Sell => {
                let side = if matches!(decision.signal, SignalType::Buy) {
                    Side::Buy
                } else {
                    Side::Sell
                };
                let price = price_from_event(event)?;
                // Instrument metadata: contract size for sizing, plus tick /
                // min-notional used below. `contract_value` mirrors the spec.
                let spec = self.ctx.exchange.instrument_spec(symbol);
                let contract_value = spec.contract_value;
                let contracts = size_decision(
                    self.ctx.sizing.for_symbol(symbol),
                    decision.size_hint,
                    price,
                    contract_value,
                );

                if contracts == 0 {
                    self.orders_blocked.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        service = %self.name,
                        symbol = %symbol,
                        signal = %decision.signal,
                        price = price.value(),
                        contract_value,
                        "decision blocked: sizer returned 0 contracts"
                    );
                    return None;
                }
                let size = Volume(contracts as f64);

                let new_notional = f64::from(contracts) * price.value() * contract_value;

                // ── Instrument min-notional gate ──────────────────────
                // Skip a dust order the venue would reject. No-op when the
                // adapter reports no minimum (the default).
                if !spec.meets_min_notional(new_notional) {
                    self.orders_blocked.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        service = %self.name,
                        symbol = %symbol,
                        signal = %decision.signal,
                        notional = new_notional,
                        min_notional = spec.min_notional,
                        "decision blocked: order below instrument min notional"
                    );
                    return None;
                }

                // ── Order-kind capability gate ────────────────────────
                // Block (don't silently downgrade) a kind the adapter
                // can't honour — downgrading post-only / IOC / FOK would
                // change the fill and fee semantics the brain relied on.
                let kind = decision.order_kind;
                if let Some(cap) = capability_for_kind(kind)
                    && !self.ctx.exchange.supports(cap)
                {
                    self.orders_blocked.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        service = %self.name,
                        symbol = %symbol,
                        signal = %decision.signal,
                        ?kind,
                        required = ?cap,
                        "decision blocked: adapter does not support requested order kind"
                    );
                    return None;
                }

                // ── Gate 3: account-wide portfolio risk (entries only) ─
                // Exits (the Close arm) are never gated here — only new
                // risk. Runs LAST so the reservation it records is only
                // taken when the order is actually about to be placed;
                // the reservation is released on rejection, on the
                // fill-driven cache refresh, or by TTL.
                if let Err(block) = self.check_and_reserve_entry(symbol, new_notional).await {
                    self.orders_blocked.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        service = %self.name,
                        symbol = %symbol,
                        signal = %decision.signal,
                        reason = %block,
                        "decision blocked: portfolio risk"
                    );
                    return None;
                }

                // ── Build the base order for the requested kind ───────
                let order = match kind {
                    OrderKind::Market => Order::market(symbol.clone(), side, size),
                    OrderKind::Limit | OrderKind::PostOnly | OrderKind::Ioc | OrderKind::Fok => {
                        let limit = decision.limit_price.unwrap_or_else(|| {
                            tracing::warn!(
                                service = %self.name,
                                symbol = %symbol,
                                ?kind,
                                fallback = price.value(),
                                "non-market order kind without limit_price; \
                                 falling back to event price"
                            );
                            price
                        });
                        // Snap to the venue's price tick (no-op when unknown).
                        let limit = Price(spec.round_price(limit.value()));
                        let mut o = Order::limit(symbol.clone(), side, size, limit);
                        o.kind = kind;
                        o
                    }
                };

                // ── Protective handling ───────────────────────────────
                // When a full bracket applies (both SL+TP, market entry,
                // OCO active), the entry stays clean — the two protective
                // orders are placed separately after the entry fills (see
                // `place_brackets`). Otherwise attach a single stop/TP (or
                // fall back / warn) as in 0.2b.
                if self.is_bracket(decision, order.kind) {
                    Some(order)
                } else {
                    Some(self.attach_protection(order, symbol, decision))
                }
            }
        }
    }

    /// Does this decision warrant a full SL+TP bracket placed as two
    /// separate OCO orders? Requires both prices, a market entry, and an
    /// active OCO registry (which `Bot` only sets when the adapter supports
    /// `StopOrders` + `OrderTracking` and a fill source is wired).
    fn is_bracket(&self, decision: &Decision, kind: OrderKind) -> bool {
        self.ctx.oco.is_some()
            && matches!(kind, OrderKind::Market)
            && decision.stop_price.is_some()
            && decision.take_profit_price.is_some()
    }

    /// Place the two reduce-only protective legs for a bracket entry and
    /// register them as an OCO pair. Called after the market entry is
    /// accepted.
    ///
    /// # Failure handling
    ///
    /// - **Stop-loss leg fails:** the entry has no protection at all, so
    ///   the configured [`BracketFailurePolicy`](crate::BracketFailurePolicy)
    ///   applies — by default the entry is closed with a reduce-only
    ///   market order.
    /// - **Take-profit leg fails:** the stop-loss is **kept** (the
    ///   position stays loss-protected) and the bracket degrades to
    ///   stop-only, mirroring [`Self::attach_protection`]'s fallback.
    async fn place_brackets(
        &self,
        symbol: &Symbol,
        entry_side: Side,
        size: Volume,
        sl: Price,
        tp: Price,
    ) {
        let Some(oco) = &self.ctx.oco else { return };
        let close_side = entry_side.opposite();
        let sl_order = Order::market(symbol.clone(), close_side, size)
            .with_reduce_only(true)
            .with_stop(StopAttachment::stop_market(sl));
        let tp_order = Order::market(symbol.clone(), close_side, size)
            .with_reduce_only(true)
            .with_stop(StopAttachment::take_profit(tp));

        let sl_id = match self.ctx.exchange.place_order(&sl_order).await {
            Ok(id) => id,
            Err(e) => {
                tracing::error!(
                    service = %self.name,
                    symbol = %symbol,
                    error = %e,
                    policy = ?self.ctx.bracket_failure_policy,
                    "bracket: stop-loss leg failed to place — entry has no protection"
                );
                self.handle_unprotected_entry(symbol, entry_side, size)
                    .await;
                return;
            }
        };
        let tp_id = match self.ctx.exchange.place_order(&tp_order).await {
            Ok(id) => id,
            Err(e) => {
                // The stop-loss is live, so the position is still
                // loss-protected — keep it and degrade to stop-only
                // rather than cancelling the SL and leaving the entry
                // with nothing.
                tracing::warn!(
                    service = %self.name,
                    symbol = %symbol,
                    error = %e,
                    sl_id = %sl_id,
                    "bracket: take-profit leg failed; keeping the stop-loss \
                     (degraded to stop-only protection)"
                );
                if let Some(tracker) = &self.ctx.order_tracker {
                    tracker.record(sl_id.clone(), &sl_order).await;
                }
                self.orders_placed.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };

        oco.register(symbol.clone(), sl_id.clone(), tp_id.clone())
            .await;
        if let Some(tracker) = &self.ctx.order_tracker {
            tracker.record(sl_id.clone(), &sl_order).await;
            tracker.record(tp_id.clone(), &tp_order).await;
        }
        self.orders_placed.fetch_add(2, Ordering::Relaxed);
        tracing::info!(
            service = %self.name,
            symbol = %symbol,
            close_side = ?close_side,
            stop = sl.value(),
            take_profit = tp.value(),
            sl_id = %sl_id,
            tp_id = %tp_id,
            "bracket placed (SL + TP, OCO-linked)"
        );
    }

    /// Apply the configured [`BracketFailurePolicy`](crate::BracketFailurePolicy)
    /// to a bracket entry whose stop-loss leg could not be placed.
    async fn handle_unprotected_entry(&self, symbol: &Symbol, entry_side: Side, size: Volume) {
        use crate::bot::BracketFailurePolicy;
        match self.ctx.bracket_failure_policy {
            BracketFailurePolicy::CloseEntry => {
                let close = Order::market(symbol.clone(), entry_side.opposite(), size)
                    .with_reduce_only(true);
                match self.ctx.exchange.place_order(&close).await {
                    Ok(id) => {
                        self.orders_placed.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            service = %self.name,
                            symbol = %symbol,
                            close_id = %id,
                            "bracket: closed the unprotected entry (BracketFailurePolicy::CloseEntry)"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            service = %self.name,
                            symbol = %symbol,
                            error = %e,
                            "bracket: FAILED to close the unprotected entry — \
                             an unprotected position is resting on the exchange; \
                             manual intervention required"
                        );
                    }
                }
            }
            BracketFailurePolicy::KeepUnprotected => {
                tracing::error!(
                    service = %self.name,
                    symbol = %symbol,
                    "bracket: entry left open WITHOUT protection \
                     (BracketFailurePolicy::KeepUnprotected)"
                );
            }
        }
    }

    /// Attach a **single** protective [`StopAttachment`] to the entry order,
    /// gated on [`Capability::StopOrders`]. This is the fallback path used
    /// when a full bracket doesn't apply — i.e. only one of SL/TP is set, the
    /// entry is a limit order, or bracket prerequisites aren't met (no fill
    /// source, or the adapter lacks `StopOrders`/`OrderTracking`).
    ///
    /// When *both* SL and TP are set but brackets are inactive, the single
    /// `Order.stop` field can only carry one, so the protective **stop-loss**
    /// takes priority and the take-profit is logged as dropped — full SL+TP
    /// is handled by [`Self::place_brackets`] when brackets are active. When
    /// the adapter lacks `StopOrders`, the order is placed **without**
    /// protection and a warning is emitted (never silently dropped) — a brain
    /// that requires stops can introspect `supports` itself.
    fn attach_protection(&self, order: Order, symbol: &Symbol, decision: &Decision) -> Order {
        let stop = match (decision.stop_price, decision.take_profit_price) {
            (Some(sl), Some(_tp)) => {
                tracing::warn!(
                    service = %self.name,
                    symbol = %symbol,
                    "both stop_price and take_profit_price set but brackets inactive \
                     (needs StopOrders + OrderTracking + a fill source); attaching \
                     stop-loss only"
                );
                StopAttachment::stop_market(sl)
            }
            (Some(sl), None) => StopAttachment::stop_market(sl),
            (None, Some(tp)) => StopAttachment::take_profit(tp),
            (None, None) => return order,
        };

        if self.ctx.exchange.supports(Capability::StopOrders) {
            order.with_stop(stop)
        } else {
            tracing::warn!(
                service = %self.name,
                symbol = %symbol,
                "protective stop / take-profit requested but adapter lacks \
                 Capability::StopOrders; placing order WITHOUT protection"
            );
            order
        }
    }
}

/// The adapter [`Capability`] a given [`OrderKind`] requires, if any.
/// `Market` and `Limit` are assumed universally supported.
fn capability_for_kind(kind: OrderKind) -> Option<Capability> {
    match kind {
        OrderKind::Market | OrderKind::Limit => None,
        OrderKind::PostOnly => Some(Capability::PostOnly),
        OrderKind::Ioc => Some(Capability::Ioc),
        OrderKind::Fok => Some(Capability::Fok),
    }
}

#[async_trait]
impl TradingService for ExecutionService {
    fn name(&self) -> &str {
        &self.name
    }

    fn restart_policy(&self) -> RestartPolicy {
        RestartPolicy::OnFailure
    }

    async fn run(&self, cancel: CancellationToken) -> anyhow::Result<()> {
        let mut rx = self.ctx.bus.subscribe();
        tracing::info!(service = %self.name, "execution service subscribed");

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!(
                        service = %self.name,
                        events = self.events_processed(),
                        dropped = self.events_dropped(),
                        placed = self.orders_placed(),
                        blocked = self.orders_blocked(),
                        "execution service shutting down"
                    );
                    return Ok(());
                }
                next = rx.recv() => match next {
                    Ok(event) => {
                        if let Err(e) = self.handle_event(&event).await {
                            tracing::error!(
                                service = %self.name,
                                error = %e,
                                "brain returned error from on_event — service continuing"
                            );
                        }
                    }
                    Err(RecvError::Lagged(skipped)) => {
                        self.events_dropped.fetch_add(skipped, Ordering::Relaxed);
                        tracing::warn!(
                            service = %self.name,
                            skipped,
                            "market data bus lagged — events dropped"
                        );
                    }
                    Err(RecvError::Closed) => {
                        tracing::info!(service = %self.name, "market data bus closed");
                        return Ok(());
                    }
                },
            }
        }
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

fn price_from_event(event: &MarketDataEvent) -> Option<Price> {
    match event {
        MarketDataEvent::Candle { candle, .. } => Some(Price(candle.close)),
        MarketDataEvent::Ticker { tick, .. } => Some(tick.mid_price()),
        MarketDataEvent::Trade { price, .. } => Some(Price(*price)),
    }
}

/// Translate the brain's `SizeHint` into a contract count via the sizer.
fn size_decision(sizing: &SizingConfig, hint: SizeHint, price: Price, contract_value: f64) -> u32 {
    let sizer = PositionSizer::new(sizing.clone());
    match hint {
        SizeHint::Default => sizer.contracts(price.value(), contract_value),
        SizeHint::MarginFraction(f) => {
            // Fraction of the configured default margin — clamped to [0, 1].
            let f = f.clamp(0.0, 1.0);
            let margin = sizing.margin_per_trade * f;
            sizer.contracts_with_margin(margin, price.value(), contract_value)
        }
        SizeHint::NotionalUsd(n) => {
            // notional = margin × leverage  →  margin = notional / leverage
            let leverage = sizing.leverage.max(1);
            let margin = n / f64::from(leverage);
            sizer.contracts_with_margin(margin, price.value(), contract_value)
        }
        SizeHint::Quantity(q) => {
            // The brain knows what it wants — clamp to max and floor.
            let raw = q.value().max(0.0).floor() as u32;
            raw.min(sizing.max_contracts)
        }
    }
}
