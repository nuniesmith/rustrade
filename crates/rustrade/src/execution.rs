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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use rustrade_core::{
    Brain, ExchangeClient, MarketDataBus, MarketDataEvent, Order, Position, Price, Side,
    SignalType, SizeHint, Symbol, Volume,
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
    pub positions: PositionCache,
    pub risk: RiskStateMap,
    pub sizing: Arc<SizingConfig>,
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

    pub fn events_processed(&self) -> u64 {
        self.events_processed.load(Ordering::Relaxed)
    }
    pub fn events_dropped(&self) -> u64 {
        self.events_dropped.load(Ordering::Relaxed)
    }
    pub fn orders_placed(&self) -> u64 {
        self.orders_placed.load(Ordering::Relaxed)
    }
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
                tracing::info!(
                    service = %self.name,
                    symbol = %symbol,
                    side = ?order.side,
                    size = %order.size,
                    reduce_only = order.reduce_only,
                    order_id = %id,
                    "order placed"
                );
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
        decision: &rustrade_core::Decision,
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
                let contracts =
                    size_decision(&self.ctx.sizing, decision.size_hint, price, contract_value);

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
                Some(Order::market(
                    symbol.clone(),
                    side,
                    Volume(contracts as f64),
                ))
            }
        }
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
