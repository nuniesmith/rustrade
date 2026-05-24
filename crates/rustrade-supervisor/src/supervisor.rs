//! The [`Supervisor`] itself — manages a fleet of [`TradingService`]s.
//!
//! **Port target.** Copy the implementation of `JanusSupervisor` from
//! `janus-main/lib/janus-core/src/supervisor/mod.rs` here. The skeleton
//! below compiles so the rest of the workspace can be developed in
//! parallel with the port — but it does not yet restart services, track
//! lifecycle, or enforce backoff.
//!
//! # Porting checklist
//!
//! - [ ] Lift `JanusSupervisor::new`, `spawn_service`, `trigger_shutdown`,
//!       `run_until_shutdown`, `wait_for_drain`.
//! - [ ] Lift `SupervisorMetrics` atomics; gate the Prometheus `.inc()`
//!       calls behind `#[cfg(feature = "prometheus")]`.
//! - [ ] Lift the chaos tests (`test_chaos_backoff`,
//!       `test_chaos_circuit_breaker_trips`, `test_chaos_mixed_fleet`).
//! - [ ] Replace references to `crate::metrics::metrics()` with a local
//!       prometheus registry wrapped in `OnceCell` inside this crate,
//!       populated only when the `prometheus` feature is on.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::backoff::BackoffConfig;
use crate::lifecycle::ServiceLifecycle;
use crate::service::TradingService;

/// Configuration for the [`Supervisor`].
#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    /// Default backoff applied to services without their own override.
    pub default_backoff: BackoffConfig,
    /// Maximum time to wait for services to drain on shutdown.
    pub shutdown_timeout: Duration,
    /// Install a Ctrl-C / SIGTERM handler automatically.
    pub install_signal_handler: bool,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            default_backoff: BackoffConfig::default(),
            shutdown_timeout: Duration::from_secs(30),
            install_signal_handler: true,
        }
    }
}

impl SupervisorConfig {
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    pub fn with_default_backoff(mut self, backoff: BackoffConfig) -> Self {
        self.default_backoff = backoff;
        self
    }

    pub fn without_signal_handler(mut self) -> Self {
        self.install_signal_handler = false;
        self
    }
}

/// Atomic counters for supervisor-level metrics.
///
/// When the `prometheus` feature is enabled these also publish to a
/// Prometheus registry.
#[derive(Debug, Default)]
pub struct SupervisorMetrics {
    pub restarts_total: AtomicU64,
    pub active_services: AtomicU64,
    pub spawned_total: AtomicU64,
    pub terminated_total: AtomicU64,
    pub circuit_breaker_trips: AtomicU64,
}

impl SupervisorMetrics {
    fn new() -> Self {
        Self::default()
    }
}

/// Tracks a fleet of [`TradingService`]s, manages their lifecycle, and
/// orchestrates graceful shutdown on Ctrl-C / SIGTERM.
///
/// This skeleton only spawns services — it does **not** yet restart them on
/// failure. Complete the port from `janus-core/supervisor/mod.rs` to get
/// restart behaviour, backoff, and circuit breakers.
pub struct Supervisor {
    config: SupervisorConfig,
    tracker: TaskTracker,
    cancel: CancellationToken,
    lifecycles: Arc<RwLock<HashMap<String, ServiceLifecycle>>>,
    metrics: Arc<SupervisorMetrics>,
}

impl Supervisor {
    pub fn new(config: SupervisorConfig) -> Self {
        Self {
            config,
            tracker: TaskTracker::new(),
            cancel: CancellationToken::new(),
            lifecycles: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(SupervisorMetrics::new()),
        }
    }

    /// Spawn a service under supervisor control.
    ///
    /// The service is added to the lifecycle registry and its `run` method
    /// is started in a tracked task. If it exits the task completes — in
    /// the full implementation this is where the restart-policy logic fires.
    pub fn spawn_service(&self, service: Box<dyn TradingService>) {
        let name = service.name().to_string();
        let cancel_child = self.cancel.child_token();
        let lifecycles = Arc::clone(&self.lifecycles);
        let metrics = Arc::clone(&self.metrics);

        // Record lifecycle entry
        {
            let lifecycles = Arc::clone(&lifecycles);
            let name_clone = name.clone();
            tokio::spawn(async move {
                lifecycles
                    .write()
                    .await
                    .insert(name_clone.clone(), ServiceLifecycle::new(name_clone));
            });
        }

        metrics.spawned_total.fetch_add(1, Ordering::Relaxed);
        metrics.active_services.fetch_add(1, Ordering::Relaxed);

        self.tracker.spawn(async move {
            tracing::info!(service = %name, "service starting");
            let service = Arc::new(service);
            let result = service.run(cancel_child).await;
            match &result {
                Ok(()) => tracing::info!(service = %name, "service exited cleanly"),
                Err(e) => tracing::warn!(service = %name, error = %e, "service exited with error"),
            }
            metrics.terminated_total.fetch_add(1, Ordering::Relaxed);
            metrics
                .active_services
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(1))
                })
                .ok();
            // TODO: apply restart policy + backoff here.
        });
    }

    /// Trigger graceful shutdown — all services will receive a cancellation.
    pub fn trigger_shutdown(&self) {
        tracing::info!("shutdown triggered");
        self.cancel.cancel();
    }

    /// Wait for all spawned tasks to complete.
    pub async fn wait_for_drain(&self) {
        self.tracker.close();
        let timeout = self.config.shutdown_timeout;
        if tokio::time::timeout(timeout, self.tracker.wait())
            .await
            .is_err()
        {
            tracing::warn!(
                timeout_secs = timeout.as_secs(),
                "shutdown timed out; some services did not exit"
            );
        }
    }

    /// Block until Ctrl-C / SIGTERM, then trigger shutdown and wait for drain.
    pub async fn run_until_shutdown(&self) -> anyhow::Result<()> {
        if self.config.install_signal_handler {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{SignalKind, signal};
                let mut sigterm = signal(SignalKind::terminate())?;
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => tracing::info!("received Ctrl-C"),
                    _ = sigterm.recv()          => tracing::info!("received SIGTERM"),
                }
            }
            #[cfg(not(unix))]
            {
                tokio::signal::ctrl_c().await?;
                tracing::info!("received Ctrl-C");
            }
        } else {
            self.cancel.cancelled().await;
        }
        self.trigger_shutdown();
        self.wait_for_drain().await;
        Ok::<(), anyhow::Error>(())
    }

    /// Number of currently-tracked services.
    pub async fn service_count(&self) -> usize {
        self.lifecycles.read().await.len()
    }

    pub fn metrics(&self) -> Arc<SupervisorMetrics> {
        Arc::clone(&self.metrics)
    }
}
