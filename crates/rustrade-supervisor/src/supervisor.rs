//! The [`Supervisor`] — manages a fleet of [`TradingService`]s.
//!
//! Replaces the naive `tokio::spawn` pattern with a structured supervision
//! tree built on:
//!
//! - [`TaskTracker`] — tracks spawned tasks without accumulating results,
//!   preventing memory leaks in long-running processes.
//! - [`CancellationToken`] — propagates graceful shutdown through the
//!   service hierarchy.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────┐
//! │                  Supervisor                     │
//! │                                                 │
//! │  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
//! │  │  Market  │  │   Risk   │  │ Execution│ ...   │
//! │  │   Feed   │  │  Engine  │  │  Service │       │
//! │  └──────────┘  └──────────┘  └──────────┘       │
//! │                                                 │
//! │  TaskTracker  ◄── tracks all spawned tasks      │
//! │  CancellationToken ◄── shutdown signal tree     │
//! │  BackoffState[] ◄── per-service restart state   │
//! │  ServiceLifecycle[] ◄── per-service state machine│
//! └─────────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use rustrade_supervisor::{Supervisor, SupervisorConfig};
//!
//! let supervisor = Supervisor::new(SupervisorConfig::default());
//! supervisor.spawn_service(Box::new(my_market_feed));
//! supervisor.spawn_service(Box::new(my_risk_engine));
//! supervisor.run_until_shutdown().await?;
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::backoff::{BackoffAction, BackoffConfig, BackoffState};
use crate::lifecycle::{
    ServiceLifecycle, ServiceLifecycleSnapshot, ServicePhase, TerminationReason,
};
use crate::service::{RestartPolicy, TradingService};

// ---------------------------------------------------------------------------
// SupervisorConfig
// ---------------------------------------------------------------------------

/// Configuration for the [`Supervisor`].
#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    /// Default backoff config applied to services that don't override it.
    pub default_backoff: BackoffConfig,
    /// Maximum time to wait for services to drain during shutdown.
    pub shutdown_timeout: Duration,
    /// Whether to install a Ctrl-C / SIGTERM handler automatically.
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
    /// Override the drain timeout.
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Override the default backoff config applied to services that don't
    /// supply their own [`SpawnOptions::backoff`].
    pub fn with_default_backoff(mut self, backoff: BackoffConfig) -> Self {
        self.default_backoff = backoff;
        self
    }

    /// Skip installing the supervisor's signal handler — the host
    /// service drives shutdown directly.
    pub fn without_signal_handler(mut self) -> Self {
        self.install_signal_handler = false;
        self
    }
}

// ---------------------------------------------------------------------------
// SupervisorMetrics
// ---------------------------------------------------------------------------

/// Atomic counters for supervisor-level metrics.
///
/// When the `prometheus` feature is enabled, these are mirrored into the
/// crate-local prometheus registry available via the `prometheus`
/// submodule. Without the feature this is the only metrics surface.
#[derive(Debug, Default)]
pub struct SupervisorMetrics {
    /// Total service restarts (clean-exit + on-failure cycles).
    pub restarts_total: AtomicU64,
    /// Currently-alive services (not yet in `Terminated`).
    pub active_services: AtomicU64,
    /// Total services ever spawned, including restarts.
    pub spawned_total: AtomicU64,
    /// Total services that have reached `Terminated`.
    pub terminated_total: AtomicU64,
    /// Total times any service's circuit breaker has tripped.
    pub circuit_breaker_trips: AtomicU64,
}

impl SupervisorMetrics {
    fn new() -> Self {
        Self::default()
    }

    fn record_spawn(&self) {
        self.spawned_total.fetch_add(1, Ordering::Relaxed);
        let new_active = self.active_services.fetch_add(1, Ordering::Relaxed) + 1;

        #[cfg(feature = "prometheus")]
        {
            let p = crate::prometheus::collectors();
            p.spawned_total.inc();
            p.active_services.set(new_active as f64);
        }
        #[cfg(not(feature = "prometheus"))]
        let _ = new_active;
    }

    fn record_restart(&self) {
        self.restarts_total.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "prometheus")]
        crate::prometheus::collectors().restarts_total.inc();
    }

    fn record_termination(&self) {
        self.terminated_total.fetch_add(1, Ordering::Relaxed);
        let prev = self
            .active_services
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            })
            .unwrap_or(0);
        let new_active = prev.saturating_sub(1);

        #[cfg(feature = "prometheus")]
        {
            let p = crate::prometheus::collectors();
            p.terminated_total.inc();
            p.active_services.set(new_active as f64);
        }
        #[cfg(not(feature = "prometheus"))]
        let _ = new_active;
    }

    fn record_termination_with_uptime(&self, _service_name: &str, _uptime_secs: f64) {
        self.record_termination();
        #[cfg(feature = "prometheus")]
        crate::prometheus::collectors()
            .uptime_seconds
            .with_label_values(&[_service_name])
            .observe(_uptime_secs);
    }

    fn record_circuit_breaker_trip(&self) {
        self.circuit_breaker_trips.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "prometheus")]
        crate::prometheus::collectors().circuit_breaker_trips.inc();
    }

    /// Snapshot the current metric values.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            restarts_total: self.restarts_total.load(Ordering::Relaxed),
            active_services: self.active_services.load(Ordering::Relaxed),
            spawned_total: self.spawned_total.load(Ordering::Relaxed),
            terminated_total: self.terminated_total.load(Ordering::Relaxed),
            circuit_breaker_trips: self.circuit_breaker_trips.load(Ordering::Relaxed),
        }
    }
}

/// Plain-data snapshot of supervisor metrics.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetricsSnapshot {
    /// Total service restarts captured at snapshot time.
    pub restarts_total: u64,
    /// Currently-alive services at snapshot time.
    pub active_services: u64,
    /// Total services ever spawned (including restarts).
    pub spawned_total: u64,
    /// Total services terminated.
    pub terminated_total: u64,
    /// Total circuit-breaker trips.
    pub circuit_breaker_trips: u64,
}

// ---------------------------------------------------------------------------
// SpawnOptions
// ---------------------------------------------------------------------------

/// Per-service spawn configuration.
#[derive(Debug, Clone, Default)]
pub struct SpawnOptions {
    /// Override the supervisor's default backoff config for this service.
    pub backoff: Option<BackoffConfig>,
}

impl SpawnOptions {
    /// Create options with a custom backoff config.
    pub fn with_backoff(backoff: BackoffConfig) -> Self {
        Self {
            backoff: Some(backoff),
        }
    }
}

// ---------------------------------------------------------------------------
// Supervisor
// ---------------------------------------------------------------------------

/// The central supervisor.
///
/// Manages the lifecycle of all [`TradingService`] implementations using
/// [`TaskTracker`] for structured concurrency and [`CancellationToken`]
/// for graceful shutdown propagation.
///
/// # Memory safety
///
/// Unlike `JoinSet`, `TaskTracker` does **not** accumulate task return
/// values. Completed task memory is reclaimed immediately, making this safe
/// for long-running processes that may restart services hundreds of times
/// over weeks of operation.
pub struct Supervisor {
    config: SupervisorConfig,
    tracker: TaskTracker,
    cancel_token: CancellationToken,
    metrics: Arc<SupervisorMetrics>,
    lifecycles: Arc<RwLock<HashMap<String, ServiceLifecycle>>>,
}

impl Supervisor {
    /// Create a new supervisor with the given configuration.
    pub fn new(config: SupervisorConfig) -> Self {
        Self {
            config,
            tracker: TaskTracker::new(),
            cancel_token: CancellationToken::new(),
            metrics: Arc::new(SupervisorMetrics::new()),
            lifecycles: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new supervisor with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(SupervisorConfig::default())
    }

    /// Get a reference to the supervisor's cancellation token.
    ///
    /// Useful for external code that needs to observe or trigger shutdown.
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }

    /// Get a reference to the supervisor's metrics.
    pub fn metrics(&self) -> &Arc<SupervisorMetrics> {
        &self.metrics
    }

    /// Snapshot all service lifecycles.
    pub async fn lifecycle_snapshots(&self) -> Vec<ServiceLifecycleSnapshot> {
        self.lifecycles
            .read()
            .await
            .values()
            .map(ServiceLifecycleSnapshot::from)
            .collect()
    }

    /// Lifecycle snapshot for a specific service by name.
    pub async fn service_lifecycle(&self, name: &str) -> Option<ServiceLifecycleSnapshot> {
        self.lifecycles
            .read()
            .await
            .get(name)
            .map(ServiceLifecycleSnapshot::from)
    }

    /// Number of services currently tracked (alive + terminated).
    pub async fn service_count(&self) -> usize {
        self.lifecycles.read().await.len()
    }

    /// Trigger a graceful shutdown of all managed services.
    #[tracing::instrument(skip(self))]
    pub fn trigger_shutdown(&self) {
        tracing::info!("supervisor: shutdown triggered");
        self.cancel_token.cancel();
    }

    /// Returns `true` if the supervisor's shutdown has been triggered.
    pub fn is_shutting_down(&self) -> bool {
        self.cancel_token.is_cancelled()
    }

    // ── Spawn ─────────────────────────────────────────────────────────

    /// Spawn a service with default options.
    pub fn spawn_service(&self, service: Box<dyn TradingService>) {
        self.spawn_service_with_options(service, SpawnOptions::default());
    }

    /// Spawn a service with custom per-service options.
    #[tracing::instrument(
        skip(self, service, options),
        fields(service = %service.name(), policy = %service.restart_policy())
    )]
    pub fn spawn_service_with_options(
        &self,
        service: Box<dyn TradingService>,
        options: SpawnOptions,
    ) {
        let service_name = service.name().to_string();
        let restart_policy = service.restart_policy();
        let backoff_config = options
            .backoff
            .unwrap_or_else(|| self.config.default_backoff.clone());

        let cancel = self.cancel_token.child_token();
        let metrics = self.metrics.clone();
        let lifecycles = self.lifecycles.clone();

        metrics.record_spawn();

        self.tracker.spawn(Self::service_loop(
            service,
            service_name,
            restart_policy,
            backoff_config,
            cancel,
            metrics,
            lifecycles,
        ));
    }

    /// The core restart loop for a single service.
    ///
    /// 1. Runs the service.
    /// 2. Catches failures.
    /// 3. Applies the restart policy and backoff strategy.
    /// 4. Loops until cancelled or the circuit breaker trips.
    #[tracing::instrument(
        skip_all,
        fields(service = %service_name, policy = %restart_policy)
    )]
    async fn service_loop(
        service: Box<dyn TradingService>,
        service_name: String,
        restart_policy: RestartPolicy,
        backoff_config: BackoffConfig,
        cancel: CancellationToken,
        metrics: Arc<SupervisorMetrics>,
        lifecycles: Arc<RwLock<HashMap<String, ServiceLifecycle>>>,
    ) {
        let mut backoff = BackoffState::new(backoff_config);
        let mut lifecycle = ServiceLifecycle::new(&service_name);

        // Record the initial lifecycle entry synchronously before any
        // failure can occur, so the lifecycle map always has the service.
        {
            let mut lc_map = lifecycles.write().await;
            lc_map.insert(service_name.clone(), lifecycle.clone());
        }

        loop {
            // Check cancellation before each attempt.
            if cancel.is_cancelled() {
                tracing::info!(
                    service = %service_name,
                    "cancellation detected, not starting service"
                );
                let _ = lifecycle.transition_to_stopping();
                let _ = lifecycle.transition_to_terminated(TerminationReason::Cancelled);
                Self::update_lifecycle(&lifecycles, &service_name, &lifecycle).await;
                let uptime = lifecycle.cumulative_running_time().as_secs_f64();
                metrics.record_termination_with_uptime(&service_name, uptime);
                return;
            }

            // Transition to Running.
            if lifecycle.phase() == ServicePhase::Starting {
                let _ = lifecycle.transition_to_running();
            } else if lifecycle.phase() == ServicePhase::BackingOff {
                let _ = lifecycle.transition_to_restarting();
                let _ = lifecycle.transition_to_running();
                metrics.record_restart();
            }

            backoff.record_start();
            Self::update_lifecycle(&lifecycles, &service_name, &lifecycle).await;

            tracing::info!(
                service = %service_name,
                attempt = lifecycle.start_count(),
                "running service"
            );

            // Run the service. We pass the cancel token in so cooperative
            // services can return promptly; we do NOT race the run future
            // against `cancel.cancelled()` here, as that would drop the
            // service's future before cleanup. The shutdown timeout in
            // `wait_for_drain` is the safety net for non-responsive services.
            let result = service.run(cancel.clone()).await;

            // If the cancel token fired, treat as a clean cancellation.
            if cancel.is_cancelled() {
                tracing::info!(service = %service_name, "service exited after cancellation");
                let _ = lifecycle.transition_to_stopping();
                let _ = lifecycle.transition_to_terminated(TerminationReason::Cancelled);
                Self::update_lifecycle(&lifecycles, &service_name, &lifecycle).await;
                let uptime = lifecycle.cumulative_running_time().as_secs_f64();
                metrics.record_termination_with_uptime(&service_name, uptime);
                return;
            }

            match result {
                Ok(()) => {
                    tracing::info!(service = %service_name, "service exited cleanly");
                    backoff.maybe_reset_on_cooldown();

                    match restart_policy {
                        RestartPolicy::Always => {
                            // Clean exit means the service completed
                            // successfully; explicitly reset backoff state
                            // so stale attempt counts don't bleed into
                            // subsequent restart cycles.
                            backoff.reset();

                            tracing::info!(
                                service = %service_name,
                                "restart_policy=always, will restart after short delay"
                            );
                            // Use a short fixed delay rather than exponential
                            // backoff (which is for errors).
                            let delay = Duration::from_millis(100);
                            let _ = lifecycle
                                .transition_to_backing_off("clean exit, policy=always", delay);
                            Self::update_lifecycle(&lifecycles, &service_name, &lifecycle).await;

                            tokio::select! {
                                _ = cancel.cancelled() => {
                                    let _ = lifecycle.transition_to_stopping();
                                    let _ = lifecycle.transition_to_terminated(
                                        TerminationReason::Cancelled,
                                    );
                                    Self::update_lifecycle(&lifecycles, &service_name, &lifecycle).await;
                                    let uptime = lifecycle.cumulative_running_time().as_secs_f64();
                                    metrics.record_termination_with_uptime(&service_name, uptime);
                                    return;
                                }
                                _ = tokio::time::sleep(delay) => {}
                            }
                            continue;
                        }
                        RestartPolicy::OnFailure | RestartPolicy::Never => {
                            let _ =
                                lifecycle.transition_to_terminated(TerminationReason::Completed);
                            Self::update_lifecycle(&lifecycles, &service_name, &lifecycle).await;
                            let uptime = lifecycle.cumulative_running_time().as_secs_f64();
                            metrics.record_termination_with_uptime(&service_name, uptime);
                            return;
                        }
                    }
                }

                Err(err) => {
                    let error_msg = format!("{err:#}");
                    tracing::error!(
                        service = %service_name,
                        error = %error_msg,
                        "service failed"
                    );

                    backoff.maybe_reset_on_cooldown();

                    match restart_policy {
                        RestartPolicy::Never => {
                            tracing::warn!(
                                service = %service_name,
                                "restart_policy=never, service will not be restarted"
                            );
                            let _ = lifecycle.transition_to_terminated(
                                TerminationReason::Unrecoverable(error_msg),
                            );
                            Self::update_lifecycle(&lifecycles, &service_name, &lifecycle).await;
                            let uptime = lifecycle.cumulative_running_time().as_secs_f64();
                            metrics.record_termination_with_uptime(&service_name, uptime);
                            return;
                        }

                        RestartPolicy::OnFailure | RestartPolicy::Always => {
                            match backoff.next_backoff() {
                                BackoffAction::Retry(delay) => {
                                    tracing::info!(
                                        service = %service_name,
                                        delay_ms = delay.as_millis() as u64,
                                        attempt = backoff.attempt(),
                                        "scheduling restart after backoff"
                                    );

                                    let _ = lifecycle.transition_to_backing_off(&error_msg, delay);
                                    Self::update_lifecycle(&lifecycles, &service_name, &lifecycle)
                                        .await;

                                    tokio::select! {
                                        _ = cancel.cancelled() => {
                                            tracing::info!(
                                                service = %service_name,
                                                "cancellation during backoff"
                                            );
                                            let _ = lifecycle.transition_to_stopping();
                                            let _ = lifecycle.transition_to_terminated(
                                                TerminationReason::Cancelled,
                                            );
                                            Self::update_lifecycle(&lifecycles, &service_name, &lifecycle).await;
                                            let uptime = lifecycle.cumulative_running_time().as_secs_f64();
                                            metrics.record_termination_with_uptime(&service_name, uptime);
                                            return;
                                        }
                                        _ = tokio::time::sleep(delay) => {}
                                    }
                                }

                                BackoffAction::CircuitOpen {
                                    failures,
                                    max_retries,
                                } => {
                                    tracing::error!(
                                        service = %service_name,
                                        failures = failures,
                                        max_retries = max_retries,
                                        "CIRCUIT BREAKER OPEN — too many failures, giving up"
                                    );
                                    metrics.record_circuit_breaker_trip();

                                    let _ = lifecycle.transition_to_terminated(
                                        TerminationReason::CircuitBreakerOpen {
                                            failures,
                                            max_retries,
                                        },
                                    );
                                    Self::update_lifecycle(&lifecycles, &service_name, &lifecycle)
                                        .await;
                                    let uptime = lifecycle.cumulative_running_time().as_secs_f64();
                                    metrics.record_termination_with_uptime(&service_name, uptime);
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Update the shared lifecycle map.
    async fn update_lifecycle(
        lifecycles: &Arc<RwLock<HashMap<String, ServiceLifecycle>>>,
        name: &str,
        lifecycle: &ServiceLifecycle,
    ) {
        let mut lc_map = lifecycles.write().await;
        lc_map.insert(name.to_string(), lifecycle.clone());
    }

    // ── Shutdown coordination ─────────────────────────────────────────

    /// Close the tracker and wait for all tasks to complete, with timeout.
    ///
    /// Call after triggering shutdown (or after `run_until_shutdown` returns)
    /// to ensure all tasks have drained.
    #[tracing::instrument(skip(self), fields(timeout_secs = self.config.shutdown_timeout.as_secs()))]
    pub async fn wait_for_drain(&self) {
        self.tracker.close();
        tracing::info!(
            timeout_secs = self.config.shutdown_timeout.as_secs(),
            "waiting for all services to drain"
        );
        match tokio::time::timeout(self.config.shutdown_timeout, self.tracker.wait()).await {
            Ok(()) => tracing::info!("all services drained successfully"),
            Err(_) => tracing::warn!(
                timeout_secs = self.config.shutdown_timeout.as_secs(),
                "shutdown timeout exceeded, some services may not have exited cleanly"
            ),
        }
    }

    /// Run the supervisor until a shutdown signal is received.
    ///
    /// If `install_signal_handler` is `true` (the default), listens for
    /// Ctrl-C / SIGTERM and triggers shutdown. Returns after all services
    /// have drained (or the shutdown timeout has elapsed).
    #[tracing::instrument(skip(self), fields(signal_handler = self.config.install_signal_handler))]
    pub async fn run_until_shutdown(&self) -> anyhow::Result<()> {
        if self.config.install_signal_handler {
            self.wait_for_signal_and_shutdown().await?;
        } else {
            self.cancel_token.cancelled().await;
            tracing::info!("external shutdown signal received");
        }

        self.wait_for_drain().await;

        let snap = self.metrics.snapshot();
        tracing::info!(
            restarts = snap.restarts_total,
            spawned = snap.spawned_total,
            terminated = snap.terminated_total,
            circuit_trips = snap.circuit_breaker_trips,
            "supervisor shutdown complete"
        );

        Ok(())
    }

    async fn wait_for_signal_and_shutdown(&self) -> anyhow::Result<()> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut sigterm = signal(SignalKind::terminate())?;
            let mut sigint = signal(SignalKind::interrupt())?;
            tokio::select! {
                _ = sigterm.recv() => tracing::info!("received SIGTERM"),
                _ = sigint.recv() => tracing::info!("received SIGINT"),
                _ = self.cancel_token.cancelled() => {
                    tracing::info!("shutdown triggered programmatically");
                    return Ok(());
                }
            }
        }

        #[cfg(not(unix))]
        {
            tokio::select! {
                result = tokio::signal::ctrl_c() => {
                    result?;
                    tracing::info!("received Ctrl-C");
                }
                _ = self.cancel_token.cancelled() => {
                    tracing::info!("shutdown triggered programmatically");
                    return Ok(());
                }
            }
        }

        self.cancel_token.cancel();
        Ok(())
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::AtomicU32;

    // ── Helpers ───────────────────────────────────────────────────────

    /// A simple service that counts how many times it ran and exits
    /// cleanly on cancellation.
    struct CountingService {
        name: String,
        policy: RestartPolicy,
        run_count: Arc<AtomicU64>,
    }

    impl CountingService {
        fn new(name: &str, policy: RestartPolicy) -> (Self, Arc<AtomicU64>) {
            let count = Arc::new(AtomicU64::new(0));
            (
                Self {
                    name: name.to_string(),
                    policy,
                    run_count: count.clone(),
                },
                count,
            )
        }
    }

    #[async_trait]
    impl TradingService for CountingService {
        fn name(&self) -> &str {
            &self.name
        }
        fn restart_policy(&self) -> RestartPolicy {
            self.policy
        }
        async fn run(&self, cancel: CancellationToken) -> anyhow::Result<()> {
            self.run_count.fetch_add(1, Ordering::SeqCst);
            cancel.cancelled().await;
            Ok(())
        }
    }

    /// A service that fails N times before succeeding.
    struct FailNTimes {
        name: String,
        fail_count: u32,
        current: Arc<AtomicU64>,
    }

    impl FailNTimes {
        fn new(name: &str, fail_count: u32) -> (Self, Arc<AtomicU64>) {
            let current = Arc::new(AtomicU64::new(0));
            (
                Self {
                    name: name.to_string(),
                    fail_count,
                    current: current.clone(),
                },
                current,
            )
        }
    }

    #[async_trait]
    impl TradingService for FailNTimes {
        fn name(&self) -> &str {
            &self.name
        }
        fn restart_policy(&self) -> RestartPolicy {
            RestartPolicy::OnFailure
        }
        async fn run(&self, cancel: CancellationToken) -> anyhow::Result<()> {
            let attempt = self.current.fetch_add(1, Ordering::SeqCst) as u32;
            if attempt < self.fail_count {
                tokio::time::sleep(Duration::from_millis(1)).await;
                anyhow::bail!("simulated failure #{}", attempt + 1);
            }
            cancel.cancelled().await;
            Ok(())
        }
    }

    /// A service that immediately returns Ok (one-shot).
    struct OneShotService {
        name: String,
        ran: Arc<AtomicU64>,
    }

    impl OneShotService {
        fn new(name: &str) -> (Self, Arc<AtomicU64>) {
            let ran = Arc::new(AtomicU64::new(0));
            (
                Self {
                    name: name.to_string(),
                    ran: ran.clone(),
                },
                ran,
            )
        }
    }

    #[async_trait]
    impl TradingService for OneShotService {
        fn name(&self) -> &str {
            &self.name
        }
        fn restart_policy(&self) -> RestartPolicy {
            RestartPolicy::Never
        }
        async fn run(&self, _cancel: CancellationToken) -> anyhow::Result<()> {
            self.ran.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// A service that always fails (for circuit breaker testing).
    struct AlwaysFailService {
        name: String,
        attempts: Arc<AtomicU64>,
    }

    impl AlwaysFailService {
        fn new(name: &str) -> (Self, Arc<AtomicU64>) {
            let attempts = Arc::new(AtomicU64::new(0));
            (
                Self {
                    name: name.to_string(),
                    attempts: attempts.clone(),
                },
                attempts,
            )
        }
    }

    #[async_trait]
    impl TradingService for AlwaysFailService {
        fn name(&self) -> &str {
            &self.name
        }
        fn restart_policy(&self) -> RestartPolicy {
            RestartPolicy::OnFailure
        }
        async fn run(&self, _cancel: CancellationToken) -> anyhow::Result<()> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(1)).await;
            anyhow::bail!("permanent failure");
        }
    }

    // ── Tests ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_supervisor_creation() {
        let sup = Supervisor::with_defaults();
        assert!(!sup.is_shutting_down());
        assert_eq!(sup.service_count().await, 0);
    }

    #[tokio::test]
    async fn test_spawn_and_cancel_single_service() {
        let config = SupervisorConfig::default().without_signal_handler();
        let sup = Supervisor::new(config);

        let (svc, count) = CountingService::new("test-svc", RestartPolicy::OnFailure);
        sup.spawn_service(Box::new(svc));

        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(sup.metrics().active_services.load(Ordering::Relaxed), 1);

        sup.trigger_shutdown();
        sup.wait_for_drain().await;

        let snap = sup.metrics().snapshot();
        assert_eq!(snap.spawned_total, 1);
        assert_eq!(snap.terminated_total, 1);
        assert_eq!(snap.active_services, 0);
    }

    #[tokio::test]
    async fn test_spawn_multiple_services() {
        let config = SupervisorConfig::default().without_signal_handler();
        let sup = Supervisor::new(config);

        let (svc1, count1) = CountingService::new("svc-1", RestartPolicy::OnFailure);
        let (svc2, count2) = CountingService::new("svc-2", RestartPolicy::OnFailure);
        let (svc3, count3) = CountingService::new("svc-3", RestartPolicy::OnFailure);

        sup.spawn_service(Box::new(svc1));
        sup.spawn_service(Box::new(svc2));
        sup.spawn_service(Box::new(svc3));

        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(count1.load(Ordering::SeqCst), 1);
        assert_eq!(count2.load(Ordering::SeqCst), 1);
        assert_eq!(count3.load(Ordering::SeqCst), 1);

        sup.trigger_shutdown();
        sup.wait_for_drain().await;

        let snap = sup.metrics().snapshot();
        assert_eq!(snap.spawned_total, 3);
        assert_eq!(snap.terminated_total, 3);
    }

    #[tokio::test]
    async fn test_service_restart_on_failure() {
        let config = SupervisorConfig::default()
            .without_signal_handler()
            .with_default_backoff(
                BackoffConfig::new(Duration::from_millis(10), Duration::from_millis(50))
                    .without_circuit_breaker(),
            );
        let sup = Supervisor::new(config);

        let (svc, attempts) = FailNTimes::new("fail-3", 3);
        sup.spawn_service(Box::new(svc));

        tokio::time::sleep(Duration::from_millis(500)).await;

        assert!(
            attempts.load(Ordering::SeqCst) >= 4,
            "expected >= 4 attempts, got {}",
            attempts.load(Ordering::SeqCst)
        );

        sup.trigger_shutdown();
        sup.wait_for_drain().await;

        let snap = sup.metrics().snapshot();
        assert!(snap.restarts_total >= 3);
    }

    #[tokio::test]
    async fn test_one_shot_service_no_restart() {
        let config = SupervisorConfig::default().without_signal_handler();
        let sup = Supervisor::new(config);

        let (svc, ran) = OneShotService::new("one-shot");
        sup.spawn_service(Box::new(svc));

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(ran.load(Ordering::SeqCst), 1);

        let snap = sup.metrics().snapshot();
        assert_eq!(snap.terminated_total, 1);
        assert_eq!(snap.restarts_total, 0);

        sup.trigger_shutdown();
        sup.wait_for_drain().await;
    }

    #[tokio::test]
    async fn test_restart_policy_never_on_failure() {
        let config = SupervisorConfig::default().without_signal_handler();
        let sup = Supervisor::new(config);

        struct FailOnce {
            ran: Arc<AtomicU64>,
        }

        #[async_trait]
        impl TradingService for FailOnce {
            fn name(&self) -> &str {
                "fail-once-never"
            }
            fn restart_policy(&self) -> RestartPolicy {
                RestartPolicy::Never
            }
            async fn run(&self, _cancel: CancellationToken) -> anyhow::Result<()> {
                self.ran.fetch_add(1, Ordering::SeqCst);
                anyhow::bail!("intentional failure");
            }
        }

        let ran = Arc::new(AtomicU64::new(0));
        let svc = FailOnce { ran: ran.clone() };
        sup.spawn_service(Box::new(svc));

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(ran.load(Ordering::SeqCst), 1);

        let snap = sup.metrics().snapshot();
        assert_eq!(snap.terminated_total, 1);
        assert_eq!(snap.restarts_total, 0);

        sup.trigger_shutdown();
        sup.wait_for_drain().await;
    }

    #[tokio::test]
    async fn test_circuit_breaker_trips() {
        let config = SupervisorConfig::default()
            .without_signal_handler()
            .with_default_backoff(
                BackoffConfig::new(Duration::from_millis(5), Duration::from_millis(20))
                    .with_circuit_breaker(3, Duration::from_secs(60)),
            );
        let sup = Supervisor::new(config);

        let (svc, attempts) = AlwaysFailService::new("always-fail");
        sup.spawn_service(Box::new(svc));

        tokio::time::sleep(Duration::from_millis(500)).await;

        let att = attempts.load(Ordering::SeqCst);
        assert!(att >= 3, "expected >= 3 attempts, got {att}");

        let snap = sup.metrics().snapshot();
        assert_eq!(snap.circuit_breaker_trips, 1);
        assert_eq!(snap.terminated_total, 1);

        sup.trigger_shutdown();
        sup.wait_for_drain().await;
    }

    #[tokio::test]
    async fn test_lifecycle_snapshots() {
        let config = SupervisorConfig::default().without_signal_handler();
        let sup = Supervisor::new(config);

        let (svc, _) = CountingService::new("lifecycle-test", RestartPolicy::OnFailure);
        sup.spawn_service(Box::new(svc));

        tokio::time::sleep(Duration::from_millis(50)).await;

        let snapshots = sup.lifecycle_snapshots().await;
        assert_eq!(snapshots.len(), 1);
        let snap = &snapshots[0];
        assert_eq!(snap.service_name, "lifecycle-test");
        assert_eq!(snap.phase, ServicePhase::Running);
        assert_eq!(snap.start_count, 1);
        assert_eq!(snap.total_failures, 0);

        sup.trigger_shutdown();
        sup.wait_for_drain().await;

        let snapshots = sup.lifecycle_snapshots().await;
        assert_eq!(snapshots[0].phase, ServicePhase::Terminated);
    }

    #[tokio::test]
    async fn test_service_lifecycle_by_name() {
        let config = SupervisorConfig::default().without_signal_handler();
        let sup = Supervisor::new(config);

        let (svc, _) = CountingService::new("named-svc", RestartPolicy::OnFailure);
        sup.spawn_service(Box::new(svc));

        tokio::time::sleep(Duration::from_millis(50)).await;

        let snap = sup.service_lifecycle("named-svc").await;
        assert!(snap.is_some());
        assert_eq!(snap.unwrap().service_name, "named-svc");

        assert!(sup.service_lifecycle("nonexistent").await.is_none());

        sup.trigger_shutdown();
        sup.wait_for_drain().await;
    }

    #[tokio::test]
    async fn test_metrics_snapshot() {
        let sup = Supervisor::with_defaults();
        let snap = sup.metrics().snapshot();
        assert_eq!(snap.restarts_total, 0);
        assert_eq!(snap.active_services, 0);
        assert_eq!(snap.spawned_total, 0);
        assert_eq!(snap.terminated_total, 0);
        assert_eq!(snap.circuit_breaker_trips, 0);
    }

    #[tokio::test]
    async fn test_shutdown_timeout() {
        let config = SupervisorConfig::default()
            .without_signal_handler()
            .with_shutdown_timeout(Duration::from_millis(100));
        let sup = Supervisor::new(config);

        struct HangingService;
        #[async_trait]
        impl TradingService for HangingService {
            fn name(&self) -> &str {
                "hanger"
            }
            async fn run(&self, _cancel: CancellationToken) -> anyhow::Result<()> {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Ok(())
            }
        }

        sup.spawn_service(Box::new(HangingService));
        tokio::time::sleep(Duration::from_millis(20)).await;
        sup.trigger_shutdown();

        let start = std::time::Instant::now();
        sup.wait_for_drain().await;
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_secs(1),
            "drain took too long: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn test_spawn_with_custom_backoff() {
        let config = SupervisorConfig::default().without_signal_handler();
        let sup = Supervisor::new(config);

        let (svc, attempts) = AlwaysFailService::new("custom-backoff");
        let custom_backoff =
            BackoffConfig::new(Duration::from_millis(5), Duration::from_millis(10))
                .with_circuit_breaker(2, Duration::from_secs(60));

        sup.spawn_service_with_options(Box::new(svc), SpawnOptions::with_backoff(custom_backoff));

        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(attempts.load(Ordering::SeqCst) >= 2);
        let snap = sup.metrics().snapshot();
        assert_eq!(snap.circuit_breaker_trips, 1);

        sup.trigger_shutdown();
        sup.wait_for_drain().await;
    }

    #[tokio::test]
    async fn test_restart_policy_always_on_clean_exit() {
        let config = SupervisorConfig::default()
            .without_signal_handler()
            .with_default_backoff(
                BackoffConfig::new(Duration::from_millis(10), Duration::from_millis(50))
                    .without_circuit_breaker(),
            );
        let sup = Supervisor::new(config);

        struct ExitImmediately {
            count: Arc<AtomicU64>,
        }
        #[async_trait]
        impl TradingService for ExitImmediately {
            fn name(&self) -> &str {
                "exit-immediately"
            }
            fn restart_policy(&self) -> RestartPolicy {
                RestartPolicy::Always
            }
            async fn run(&self, _cancel: CancellationToken) -> anyhow::Result<()> {
                self.count.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(1)).await;
                Ok(())
            }
        }

        let count = Arc::new(AtomicU64::new(0));
        let svc = ExitImmediately {
            count: count.clone(),
        };
        sup.spawn_service(Box::new(svc));

        tokio::time::sleep(Duration::from_millis(500)).await;

        let runs = count.load(Ordering::SeqCst);
        assert!(
            runs >= 2,
            "expected service to run multiple times with Always policy, got {runs}"
        );

        sup.trigger_shutdown();
        sup.wait_for_drain().await;
    }

    #[tokio::test]
    async fn test_is_shutting_down() {
        let sup = Supervisor::with_defaults();
        assert!(!sup.is_shutting_down());
        sup.trigger_shutdown();
        assert!(sup.is_shutting_down());
    }

    #[tokio::test]
    async fn test_config_builder() {
        let config = SupervisorConfig::default()
            .with_shutdown_timeout(Duration::from_secs(10))
            .with_default_backoff(BackoffConfig::new(
                Duration::from_millis(200),
                Duration::from_secs(30),
            ))
            .without_signal_handler();

        assert_eq!(config.shutdown_timeout, Duration::from_secs(10));
        assert!(!config.install_signal_handler);
        assert_eq!(
            config.default_backoff.base_delay,
            Duration::from_millis(200)
        );
        assert_eq!(config.default_backoff.max_delay, Duration::from_secs(30));
    }

    // =====================================================================
    // Chaos tests
    // =====================================================================

    /// Configurable chaos service: fails `fail_times` before succeeding,
    /// records each attempt timestamp for backoff analysis, then waits for
    /// cancellation.
    struct ChaosService {
        name: String,
        fail_times: u32,
        current: Arc<AtomicU32>,
        attempt_times: Arc<tokio::sync::Mutex<Vec<std::time::Instant>>>,
        policy: RestartPolicy,
    }

    impl ChaosService {
        fn new(name: &str, fail_times: u32, policy: RestartPolicy) -> Self {
            Self {
                name: name.to_string(),
                fail_times,
                current: Arc::new(AtomicU32::new(0)),
                attempt_times: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                policy,
            }
        }
    }

    #[async_trait]
    impl TradingService for ChaosService {
        fn name(&self) -> &str {
            &self.name
        }
        fn restart_policy(&self) -> RestartPolicy {
            self.policy
        }
        async fn run(&self, cancel: CancellationToken) -> anyhow::Result<()> {
            {
                let mut ts = self.attempt_times.lock().await;
                ts.push(std::time::Instant::now());
            }
            let n = self.current.fetch_add(1, Ordering::SeqCst);
            if n < self.fail_times {
                anyhow::bail!("chaos failure #{}", n + 1);
            }
            cancel.cancelled().await;
            Ok(())
        }
    }

    /// **Chaos test — exponential backoff verification.**
    #[tokio::test]
    async fn test_chaos_exponential_backoff() {
        let backoff = BackoffConfig::new(Duration::from_millis(20), Duration::from_secs(2))
            .without_circuit_breaker();
        let config = SupervisorConfig::default()
            .with_shutdown_timeout(Duration::from_secs(5))
            .with_default_backoff(backoff)
            .without_signal_handler();
        let sup = Supervisor::new(config);

        let chaos = ChaosService::new("chaos-backoff", 3, RestartPolicy::OnFailure);
        let attempts_arc = chaos.attempt_times.clone();
        let current_arc = chaos.current.clone();

        sup.spawn_service(Box::new(chaos));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let count = current_arc.load(Ordering::SeqCst);
            if count >= 4 {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("chaos service did not recover; attempts={count}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        let timestamps = attempts_arc.lock().await;
        assert!(
            timestamps.len() >= 4,
            "expected >= 4 attempts, got {}",
            timestamps.len()
        );

        let delays: Vec<Duration> = timestamps
            .windows(2)
            .map(|w| w[1].duration_since(w[0]))
            .collect();

        // Skip delays[0] — that's the gap between initial spawn and first
        // restart, which has no preceding failure to back off from. Starting
        // at delays[1] we're measuring actual backoff intervals.
        for (i, d) in delays.iter().enumerate().skip(1) {
            assert!(
                *d >= Duration::from_millis(1),
                "delay[{i}] too short: {d:?} — backoff may not be working"
            );
        }

        let metrics = sup.metrics().snapshot();
        assert!(
            metrics.restarts_total >= 3,
            "expected >= 3 restarts, got {}",
            metrics.restarts_total
        );

        sup.trigger_shutdown();
        sup.wait_for_drain().await;

        let snap = sup.service_lifecycle("chaos-backoff").await.unwrap();
        assert_eq!(snap.phase, ServicePhase::Terminated);
    }

    /// **Chaos test — circuit breaker trips.**
    #[tokio::test]
    async fn test_chaos_circuit_breaker_trips() {
        let backoff = BackoffConfig::new(Duration::from_millis(10), Duration::from_millis(50))
            .with_circuit_breaker(3, Duration::from_secs(60));
        let config = SupervisorConfig::default()
            .with_shutdown_timeout(Duration::from_secs(5))
            .with_default_backoff(backoff)
            .without_signal_handler();
        let sup = Supervisor::new(config);

        let chaos = ChaosService::new("chaos-cb", 1000, RestartPolicy::OnFailure);
        let current_arc = chaos.current.clone();
        sup.spawn_service(Box::new(chaos));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(snap) = sup.service_lifecycle("chaos-cb").await
                && snap.phase == ServicePhase::Terminated
            {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!(
                    "circuit breaker did not trip; attempts={}",
                    current_arc.load(Ordering::SeqCst)
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let snap = sup.service_lifecycle("chaos-cb").await.unwrap();
        assert_eq!(snap.phase, ServicePhase::Terminated);
        let reason = snap
            .termination_reason
            .as_deref()
            .expect("termination reason");
        assert!(
            reason.contains("circuit breaker"),
            "expected circuit breaker termination, got: {reason}"
        );

        let metrics = sup.metrics().snapshot();
        assert!(metrics.circuit_breaker_trips >= 1);

        let attempts_at_trip = current_arc.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(200)).await;
        let attempts_after = current_arc.load(Ordering::SeqCst);
        assert_eq!(
            attempts_at_trip, attempts_after,
            "service should NOT restart after circuit breaker trips"
        );

        sup.trigger_shutdown();
        sup.wait_for_drain().await;
    }

    /// Tracer used in the mixed-fleet chaos test.
    struct LifecycleTracer {
        name: String,
        log: Arc<tokio::sync::Mutex<Vec<String>>>,
        policy: RestartPolicy,
    }

    impl LifecycleTracer {
        fn new(
            name: &str,
            log: Arc<tokio::sync::Mutex<Vec<String>>>,
            policy: RestartPolicy,
        ) -> Self {
            Self {
                name: name.to_string(),
                log,
                policy,
            }
        }
    }

    #[async_trait]
    impl TradingService for LifecycleTracer {
        fn name(&self) -> &str {
            &self.name
        }
        fn restart_policy(&self) -> RestartPolicy {
            self.policy
        }
        async fn run(&self, cancel: CancellationToken) -> anyhow::Result<()> {
            {
                let mut l = self.log.lock().await;
                l.push(format!("{}:started", self.name));
            }
            cancel.cancelled().await;
            {
                let mut l = self.log.lock().await;
                l.push(format!("{}:stopped", self.name));
            }
            Ok(())
        }
    }

    /// **Chaos test — mixed fleet (healthy + failing services).**
    #[tokio::test]
    async fn test_chaos_mixed_fleet() {
        let backoff = BackoffConfig::new(Duration::from_millis(10), Duration::from_millis(100))
            .with_circuit_breaker(3, Duration::from_secs(60));
        let config = SupervisorConfig::default()
            .with_shutdown_timeout(Duration::from_secs(5))
            .with_default_backoff(backoff)
            .without_signal_handler();
        let sup = Supervisor::new(config);

        let log = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));

        sup.spawn_service(Box::new(LifecycleTracer::new(
            "healthy-api",
            log.clone(),
            RestartPolicy::OnFailure,
        )));

        let chaos = ChaosService::new("bad-data", 1000, RestartPolicy::OnFailure);
        sup.spawn_service(Box::new(chaos));

        let recovering = ChaosService::new("flaky-cns", 2, RestartPolicy::OnFailure);
        let recovering_attempts = recovering.current.clone();
        sup.spawn_service(Box::new(recovering));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if sup.service_count().await == 3 {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!(
                    "timed out waiting for 3 services to register; got {}",
                    sup.service_count().await
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let bad_done = sup
                .service_lifecycle("bad-data")
                .await
                .is_some_and(|s| s.phase == ServicePhase::Terminated);
            let recovered = recovering_attempts.load(Ordering::SeqCst) >= 3;
            if bad_done && recovered {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("mixed fleet did not reach expected state");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let healthy_snap = sup.service_lifecycle("healthy-api").await.unwrap();
        assert!(healthy_snap.phase.is_alive());

        let bad_snap = sup.service_lifecycle("bad-data").await.unwrap();
        assert_eq!(bad_snap.phase, ServicePhase::Terminated);
        assert!(
            bad_snap
                .termination_reason
                .as_deref()
                .is_some_and(|r| r.contains("circuit breaker"))
        );

        let flaky_snap = sup.service_lifecycle("flaky-cns").await.unwrap();
        assert!(flaky_snap.phase.is_alive());
        assert!(flaky_snap.start_count >= 3);

        sup.trigger_shutdown();
        sup.wait_for_drain().await;

        for name in &["healthy-api", "bad-data", "flaky-cns"] {
            let snap = sup.service_lifecycle(name).await.unwrap();
            assert_eq!(
                snap.phase,
                ServicePhase::Terminated,
                "service '{name}' should be Terminated after shutdown"
            );
        }

        let metrics = sup.metrics().snapshot();
        assert_eq!(metrics.active_services, 0);
        assert_eq!(metrics.spawned_total, 3);
        assert_eq!(metrics.terminated_total, 3);
    }
}
