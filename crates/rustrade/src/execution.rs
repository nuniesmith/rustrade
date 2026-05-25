//! Framework-side execution: subscribes to the [`MarketDataBus`] and
//! routes each event to a [`Brain`].
//!
//! **Phase 2a scope.** Routes events and calls `brain.on_event` for each
//! one with a hard-coded `Position::FLAT`. Decisions are logged via
//! `tracing` but not yet acted on. Phase 2b wires the risk gates
//! (`SessionPnl` halt → `CircuitBreaker` trip → `PositionSizer` cap)
//! and actually places orders through the [`ExchangeClient`].

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use rustrade_core::{Brain, ExchangeClient, MarketDataBus, Position};
use rustrade_supervisor::{RestartPolicy, TradingService};
use tokio::sync::broadcast::error::RecvError;
use tokio_util::sync::CancellationToken;

/// Subscribes to the [`MarketDataBus`] and feeds each event to a single
/// brain.
pub struct ExecutionService {
    name: String,
    brain: Arc<dyn Brain>,
    #[allow(dead_code)] // wired up in Phase 2b
    exchange: Arc<dyn ExchangeClient>,
    bus: MarketDataBus,
    events_processed: AtomicU64,
    events_dropped: AtomicU64,
}

impl ExecutionService {
    pub fn new(
        brain: Arc<dyn Brain>,
        exchange: Arc<dyn ExchangeClient>,
        bus: MarketDataBus,
    ) -> Self {
        let name = format!("execution[{}]", brain.name());
        Self {
            name,
            brain,
            exchange,
            bus,
            events_processed: AtomicU64::new(0),
            events_dropped: AtomicU64::new(0),
        }
    }

    /// Number of events the brain has been called with.
    pub fn events_processed(&self) -> u64 {
        self.events_processed.load(Ordering::Relaxed)
    }

    /// Number of events the broadcast bus dropped because this service
    /// fell behind. Should always be zero in healthy operation.
    pub fn events_dropped(&self) -> u64 {
        self.events_dropped.load(Ordering::Relaxed)
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
        let mut rx = self.bus.subscribe();
        tracing::info!(service = %self.name, "execution service subscribed to market data bus");

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!(
                        service = %self.name,
                        events = self.events_processed(),
                        dropped = self.events_dropped(),
                        "execution service shutting down"
                    );
                    return Ok(());
                }
                next = rx.recv() => match next {
                    Ok(event) => {
                        // Phase 2a: no position tracking yet — pass FLAT.
                        // Phase 2b will look up the actual position from
                        // the exchange (or a cache populated by the
                        // FillRoutingService).
                        let decision = self.brain.on_event(&event, &Position::FLAT).await?;
                        self.events_processed.fetch_add(1, Ordering::Relaxed);

                        if !matches!(decision.signal, rustrade_core::SignalType::Hold) {
                            tracing::debug!(
                                service = %self.name,
                                signal = %decision.signal,
                                confidence = decision.confidence,
                                "brain decision (Phase 2a: not yet acted on)"
                            );
                        }
                    }
                    Err(RecvError::Lagged(skipped)) => {
                        // Broadcast slow-consumer: count and continue.
                        self.events_dropped.fetch_add(skipped, Ordering::Relaxed);
                        tracing::warn!(
                            service = %self.name,
                            skipped,
                            "market data bus lagged — events dropped"
                        );
                    }
                    Err(RecvError::Closed) => {
                        tracing::info!(
                            service = %self.name,
                            "market data bus closed; exiting"
                        );
                        return Ok(());
                    }
                },
            }
        }
    }
}
