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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use chrono::Utc;
use rustrade_core::{
    Brain, Capability, Decision, ExchangeClient, MarketDataBus, MarketDataEvent, Order, OrderKind,
    Position, Price, Side, Signal, SignalBus, SignalType, SizeHint, StopAttachment, Symbol, Volume,
};
use rustrade_risk::{PositionSizer, SizingConfig};
use rustrade_supervisor::{RestartPolicy, TradingService};
use tokio::sync::broadcast::error::RecvError;
use tokio_util::sync::CancellationToken;

use crate::risk_state::{PositionCache, RiskStateMap};

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
    events_processed: AtomicU64,
    events_dropped: AtomicU64,
    orders_placed: AtomicU64,
    orders_blocked: AtomicU64,
}

impl ExecutionService {
    pub(crate) fn new(brain: Arc<dyn Brain>, ctx: ExecutionContext) -> Self {
        let name = format!("execution[{}]", brain.name());
        Self {
            name,
            brain,
            ctx,
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

        // ── Gate 1: session PnL halt ──────────────────────────────────
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

        // ── Gate 2: circuit breaker ───────────────────────────────────
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
                let contract_value = self.ctx.exchange.contract_value(symbol);
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
    /// accepted. Best-effort: if the second leg fails to place, the first is
    /// cancelled so no orphaned protective order is left resting.
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
                tracing::error!(service = %self.name, symbol = %symbol, error = %e, "bracket: stop-loss leg failed to place; entry is UNPROTECTED");
                return;
            }
        };
        let tp_id = match self.ctx.exchange.place_order(&tp_order).await {
            Ok(id) => id,
            Err(e) => {
                tracing::error!(service = %self.name, symbol = %symbol, error = %e, "bracket: take-profit leg failed; cancelling the stop-loss leg to avoid an orphan");
                let _ = self.ctx.exchange.cancel_order(symbol, &sl_id).await;
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
