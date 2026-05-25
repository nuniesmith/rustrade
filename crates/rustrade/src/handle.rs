//! Cheap cloneable handle into a running [`Bot`](crate::Bot).
//!
//! Host services hold a `BotHandle` to:
//!
//! - Query aggregated health via [`BotHandle::health`].
//! - Trigger shutdown via [`BotHandle::shutdown`].
//! - Await shutdown via [`BotHandle::await_shutdown`].
//! - Feed realised trade outcomes into the risk gates via
//!   [`BotHandle::record_trade_outcome`].
//!
//! The handle is `Clone` so multiple parts of the host service (an HTTP
//! handler, a metrics endpoint, a shutdown coordinator) can hold one
//! without contention. All shared state is `Arc`-wrapped, so a clone is
//! an atomic-ref-count bump.

use std::sync::Arc;

use rustrade_core::{Brain, Position, Signal, SignalBus, Symbol};
use rustrade_supervisor::{ServiceLifecycleSnapshot, Supervisor};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::risk_state::{PositionCache, RiskStateMap};

/// Aggregated health snapshot returned by [`BotHandle::health`].
#[derive(Debug, Clone)]
pub struct BotHealth {
    /// `true` iff every brain reports healthy AND no service is in a
    /// non-alive (terminated) state.
    pub healthy: bool,
    /// Whether shutdown has been triggered (signal or `handle.shutdown()`).
    pub shutting_down: bool,
    /// Per-service lifecycle snapshots from the supervisor.
    pub services: Vec<ServiceLifecycleSnapshot>,
    /// One entry per brain, in the order they were passed to `Bot::new`.
    pub brains: Vec<BrainHealthSnapshot>,
}

/// Per-brain health information surfaced in [`BotHealth::brains`].
#[derive(Debug, Clone)]
pub struct BrainHealthSnapshot {
    /// Brain name (see `Brain::name`).
    pub name: String,
    /// Whether the brain reports itself healthy.
    pub healthy: bool,
    /// Total events processed since startup.
    pub events_processed: u64,
    /// Number of non-`Hold` decisions emitted.
    pub non_hold_decisions: u64,
    /// Free-form details reported by the brain.
    pub details: serde_json::Value,
}

/// Cheap cloneable handle into a running [`Bot`](crate::Bot).
///
/// See [the module docs](self) for the host-side contract.
#[derive(Clone)]
pub struct BotHandle {
    cancel: CancellationToken,
    supervisor: Arc<Supervisor>,
    brains: Arc<Vec<Arc<dyn Brain>>>,
    risk: RiskStateMap,
    positions: PositionCache,
    signals: SignalBus,
}

impl BotHandle {
    pub(crate) fn new(
        supervisor: Arc<Supervisor>,
        brains: Arc<Vec<Arc<dyn Brain>>>,
        risk: RiskStateMap,
        positions: PositionCache,
        signals: SignalBus,
    ) -> Self {
        Self {
            cancel: supervisor.cancel_token().clone(),
            supervisor,
            brains,
            risk,
            positions,
            signals,
        }
    }

    /// Trigger a graceful shutdown. Fire-and-forget; idempotent.
    pub fn shutdown(&self) {
        self.cancel.cancel();
    }

    /// Has shutdown been triggered?
    pub fn is_shutting_down(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Resolves once shutdown has been triggered by anyone (signal,
    /// `shutdown()` call on this or any other handle clone, or programmatic
    /// supervisor cancellation).
    pub async fn await_shutdown(&self) {
        self.cancel.cancelled().await;
    }

    /// Feed a realised trade outcome into the per-symbol risk state.
    ///
    /// Called by the host (or a brain) when a position closes. Updates
    /// the symbol's `SessionPnl` and records a win/loss on the
    /// `CircuitBreaker` based on the **net** PnL.
    ///
    /// Phase 2b does not automate this from the fill stream — that's
    /// `FillRoutingService` territory in Phase 2c.
    pub async fn record_trade_outcome(&self, symbol: &Symbol, gross_pnl: f64, fee: f64) {
        let mut map = self.risk.write().await;
        if let Some(risk) = map.get_mut(symbol) {
            risk.session_pnl.record_close(gross_pnl, fee);
            let net = gross_pnl - fee;
            if net > 0.0 {
                risk.circuit_breaker.record_win();
            } else if net < 0.0 {
                risk.circuit_breaker.record_loss();
            }
        } else {
            tracing::warn!(
                symbol = %symbol,
                "record_trade_outcome: symbol not in risk state map (was it configured?)"
            );
        }
    }

    /// Read the current cached position for a symbol, or `Position::FLAT`
    /// if the symbol isn't tracked.
    pub async fn position(&self, symbol: &Symbol) -> Position {
        self.positions
            .read()
            .await
            .get(symbol)
            .copied()
            .unwrap_or(Position::FLAT)
    }

    /// Overwrite the cached position for a symbol. Typically called by the
    /// host's fill-handling code; Phase 2c's `FillRoutingService` will do
    /// this automatically.
    pub async fn set_position(&self, symbol: &Symbol, position: Position) {
        self.positions
            .write()
            .await
            .insert(symbol.clone(), position);
    }

    /// Subscribe to the bot's signal stream.
    ///
    /// The [`ExecutionService`](crate::execution::ExecutionService)
    /// publishes a [`Signal`] on every non-`Hold` decision a brain emits,
    /// *before* the risk gates run. Subscribers see the strategic intent;
    /// whether each signal was acted on is observable from order logs and
    /// metrics.
    ///
    /// The underlying channel is `tokio::sync::broadcast`, so slow
    /// subscribers will see `RecvError::Lagged(n)` if they fall behind.
    ///
    /// # Subscriber lifetime
    ///
    /// Because `BotHandle` keeps a `Sender` clone alive, the channel
    /// does **not** close when the bot exits. A subscriber that loops on
    /// `recv()` will block forever after shutdown unless it also watches
    /// a cancellation signal:
    ///
    /// ```ignore
    /// let mut rx = handle.subscribe_signals();
    /// let shutdown = host.shutdown_token();
    /// loop {
    ///     tokio::select! {
    ///         _ = shutdown.cancelled() => break,
    ///         r = rx.recv() => match r {
    ///             Ok(sig) => handle_signal(sig),
    ///             Err(_)  => break,
    ///         },
    ///     }
    /// }
    /// ```
    pub fn subscribe_signals(&self) -> broadcast::Receiver<Signal> {
        self.signals.subscribe()
    }

    /// Number of currently-attached signal subscribers.
    pub fn signal_subscriber_count(&self) -> usize {
        self.signals.subscriber_count()
    }

    /// Snapshot of bot-wide health.
    pub async fn health(&self) -> BotHealth {
        let services = self.supervisor.lifecycle_snapshots().await;

        let mut brains = Vec::with_capacity(self.brains.len());
        for brain in self.brains.iter() {
            let h = brain.health().await;
            brains.push(BrainHealthSnapshot {
                name: brain.name().to_string(),
                healthy: h.healthy,
                events_processed: h.events_processed,
                non_hold_decisions: h.non_hold_decisions,
                details: h.details,
            });
        }

        let all_services_alive = services
            .iter()
            .all(|s| !matches!(s.phase, rustrade_supervisor::ServicePhase::Terminated));
        let all_brains_healthy = brains.iter().all(|b| b.healthy);

        BotHealth {
            healthy: all_services_alive && all_brains_healthy,
            shutting_down: self.is_shutting_down(),
            services,
            brains,
        }
    }
}

impl std::fmt::Debug for BotHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BotHandle")
            .field("shutting_down", &self.is_shutting_down())
            .field("brain_count", &self.brains.len())
            .finish_non_exhaustive()
    }
}
