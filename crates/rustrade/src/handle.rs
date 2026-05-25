//! Cheap cloneable handle into a running [`Bot`](crate::Bot).
//!
//! Host services hold a `BotHandle` to:
//!
//! - Query aggregated health via [`BotHandle::health`].
//! - Trigger shutdown via [`BotHandle::shutdown`].
//! - Await shutdown via [`BotHandle::await_shutdown`].
//!
//! The handle is `Clone` so multiple parts of the host service (an HTTP
//! handler, a metrics endpoint, a shutdown coordinator) can hold one
//! without contention. All shared state is `Arc`-wrapped, so a clone is
//! an atomic-ref-count bump.

use std::sync::Arc;

use rustrade_core::Brain;
use rustrade_supervisor::{ServiceLifecycleSnapshot, Supervisor};
use tokio_util::sync::CancellationToken;

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
    pub name: String,
    pub healthy: bool,
    pub events_processed: u64,
    pub non_hold_decisions: u64,
    pub details: serde_json::Value,
}

#[derive(Clone)]
pub struct BotHandle {
    cancel: CancellationToken,
    supervisor: Arc<Supervisor>,
    brains: Arc<Vec<Arc<dyn Brain>>>,
}

impl BotHandle {
    pub(crate) fn new(
        supervisor: Arc<Supervisor>,
        brains: Vec<Arc<dyn Brain>>,
        _brain_names: Vec<String>,
    ) -> Self {
        Self {
            cancel: supervisor.cancel_token().clone(),
            supervisor,
            brains: Arc::new(brains),
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
