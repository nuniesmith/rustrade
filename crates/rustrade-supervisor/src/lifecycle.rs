//! Service lifecycle state machine.
//!
//! Each service managed by the [`Supervisor`](super::Supervisor) progresses
//! through a well-defined set of states:
//!
//! ```text
//!   ┌──────────┐
//!   │ Starting │──────────────────────────┐
//!   └────┬─────┘                          │
//!        │ run() entered                  │ init error
//!        ▼                                ▼
//!   ┌──────────┐                    ┌────────────┐
//!   │ Running  │───── error ──────▶│ BackingOff │
//!   └────┬─────┘                    └──────┬─────┘
//!        │                                 │
//!        │ cancel / Ok(())                 │ retry
//!        │                                 │
//!        │    ┌────────────────────────────┘
//!        ▼    ▼
//!   ┌──────────┐         ┌────────────┐
//!   │ Stopping │────────▶│ Terminated │
//!   └──────────┘         └────────────┘
//! ```
//!
//! - **Starting**: the service is initializing.
//! - **Running**: the service's `run()` loop is active.
//! - **BackingOff**: the service failed and is waiting for the backoff
//!   timer before the supervisor retries.
//! - **Stopping**: a cancellation signal was received; the service is
//!   finalizing.
//! - **Terminated**: terminal state — the service has exited (or the
//!   circuit breaker tripped and the supervisor gave up).
//!
//! The `BackingOff` state prevents the supervisor from tight-looping on a
//! persistent failure, which would burn CPU and flood logs.

use std::fmt;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// ServicePhase
// ---------------------------------------------------------------------------

/// Lifecycle phase of a supervised service.
///
/// Plain enum without associated data; richer context (timing, error info,
/// attempt counts) lives in [`ServiceLifecycle`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServicePhase {
    /// The service is initializing.
    Starting,
    /// The service's main `run()` loop is executing.
    Running,
    /// The service failed and is waiting for the backoff timer to expire.
    BackingOff,
    /// A shutdown signal was received; the service is performing cleanup.
    Stopping,
    /// Terminal state — the service has exited.
    Terminated,
}

impl fmt::Display for ServicePhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Starting => write!(f, "starting"),
            Self::Running => write!(f, "running"),
            Self::BackingOff => write!(f, "backing_off"),
            Self::Stopping => write!(f, "stopping"),
            Self::Terminated => write!(f, "terminated"),
        }
    }
}

impl ServicePhase {
    /// True if the service is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Terminated)
    }

    /// True if the service is "alive" (starting, running, or backing off).
    pub fn is_alive(&self) -> bool {
        matches!(self, Self::Starting | Self::Running | Self::BackingOff)
    }
}

// ---------------------------------------------------------------------------
// TerminationReason
// ---------------------------------------------------------------------------

/// Why a service reached the `Terminated` phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminationReason {
    /// The service's `run()` returned `Ok(())` — clean completion.
    Completed,
    /// The supervisor's cancellation token was triggered (graceful shutdown).
    Cancelled,
    /// The circuit breaker tripped after too many failures.
    CircuitBreakerOpen {
        /// Number of failures observed within the circuit-breaker window.
        failures: u32,
        /// The configured maximum before tripping.
        max_retries: u32,
    },
    /// The service encountered an unrecoverable error and its restart
    /// policy is [`RestartPolicy::Never`](super::RestartPolicy::Never).
    Unrecoverable(String),
}

impl fmt::Display for TerminationReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Completed => write!(f, "completed"),
            Self::Cancelled => write!(f, "cancelled"),
            Self::CircuitBreakerOpen {
                failures,
                max_retries,
            } => write!(
                f,
                "circuit breaker open ({failures}/{max_retries} failures)"
            ),
            Self::Unrecoverable(msg) => write!(f, "unrecoverable: {msg}"),
        }
    }
}

// ---------------------------------------------------------------------------
// TransitionError
// ---------------------------------------------------------------------------

/// Error returned when an invalid state transition is attempted.
#[derive(Debug, Clone, thiserror::Error)]
#[error("invalid lifecycle transition: {from} → {to}")]
pub struct TransitionError {
    /// Phase the service was in.
    pub from: ServicePhase,
    /// Phase the caller tried to move to.
    pub to: ServicePhase,
}

// ---------------------------------------------------------------------------
// ServiceLifecycle
// ---------------------------------------------------------------------------

/// Full lifecycle tracker for a single supervised service.
///
/// Wraps the [`ServicePhase`] enum with timing data, counters, and
/// transition validation logic. The supervisor holds one of these per
/// managed service.
#[derive(Debug, Clone)]
pub struct ServiceLifecycle {
    phase: ServicePhase,
    service_name: String,
    created_at: Instant,
    phase_entered_at: Instant,
    start_count: u32,
    total_failures: u32,
    last_error: Option<String>,
    termination_reason: Option<TerminationReason>,
    cumulative_running: Duration,
    running_since: Option<Instant>,
}

impl ServiceLifecycle {
    /// Create a new lifecycle tracker in the `Starting` phase.
    pub fn new(service_name: impl Into<String>) -> Self {
        let now = Instant::now();
        Self {
            phase: ServicePhase::Starting,
            service_name: service_name.into(),
            created_at: now,
            phase_entered_at: now,
            start_count: 1,
            total_failures: 0,
            last_error: None,
            termination_reason: None,
            cumulative_running: Duration::ZERO,
            running_since: None,
        }
    }

    // ── Accessors ─────────────────────────────────────────────────────

    /// Current lifecycle phase.
    pub fn phase(&self) -> ServicePhase {
        self.phase
    }

    /// Service name as configured on construction.
    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    /// How long the service has existed (since first `Starting`).
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// How long the service has been in its current phase.
    pub fn time_in_current_phase(&self) -> Duration {
        self.phase_entered_at.elapsed()
    }

    /// Total number of times the service has been started.
    pub fn start_count(&self) -> u32 {
        self.start_count
    }

    /// Total failures over the service's lifetime.
    pub fn total_failures(&self) -> u32 {
        self.total_failures
    }

    /// The last error message recorded on a failed transition, if any.
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    /// Why the service terminated (only `Some` when phase is `Terminated`).
    pub fn termination_reason(&self) -> Option<&TerminationReason> {
        self.termination_reason.as_ref()
    }

    /// Cumulative wall-clock time spent in the `Running` phase.
    ///
    /// If the service is currently running, includes time up to *now*.
    pub fn cumulative_running_time(&self) -> Duration {
        let extra = self
            .running_since
            .map(|since| since.elapsed())
            .unwrap_or(Duration::ZERO);
        self.cumulative_running + extra
    }

    // ── Transitions ───────────────────────────────────────────────────

    /// Move from `Starting` to `Running`.
    pub fn transition_to_running(&mut self) -> Result<(), TransitionError> {
        self.validate_transition(ServicePhase::Running)?;
        self.set_phase(ServicePhase::Running);
        self.running_since = Some(Instant::now());
        tracing::info!(
            service = %self.service_name,
            start_count = self.start_count,
            "service entered Running phase"
        );
        Ok(())
    }

    /// Move from `Running` (or `Starting`) to `BackingOff` after a failure.
    pub fn transition_to_backing_off(
        &mut self,
        error: &str,
        backoff_duration: Duration,
    ) -> Result<(), TransitionError> {
        self.validate_transition(ServicePhase::BackingOff)?;
        self.accumulate_running_time();
        self.total_failures += 1;
        self.last_error = Some(error.to_string());
        self.set_phase(ServicePhase::BackingOff);
        tracing::warn!(
            service = %self.service_name,
            error = %error,
            attempt = self.total_failures,
            backoff_ms = backoff_duration.as_millis() as u64,
            "service failed, entering BackingOff phase"
        );
        Ok(())
    }

    /// Transition from `BackingOff` → `Starting` (retry).
    pub fn transition_to_restarting(&mut self) -> Result<(), TransitionError> {
        self.validate_transition(ServicePhase::Starting)?;
        self.start_count += 1;
        self.set_phase(ServicePhase::Starting);
        tracing::info!(
            service = %self.service_name,
            start_count = self.start_count,
            "service restarting (entering Starting phase)"
        );
        Ok(())
    }

    /// Move to `Stopping` on cancellation — services drain after this.
    pub fn transition_to_stopping(&mut self) -> Result<(), TransitionError> {
        self.validate_transition(ServicePhase::Stopping)?;
        self.accumulate_running_time();
        self.set_phase(ServicePhase::Stopping);
        tracing::info!(
            service = %self.service_name,
            "service entering Stopping phase"
        );
        Ok(())
    }

    /// Transition to `Terminated`. Terminal — no further transitions allowed.
    pub fn transition_to_terminated(
        &mut self,
        reason: TerminationReason,
    ) -> Result<(), TransitionError> {
        self.validate_transition(ServicePhase::Terminated)?;
        self.accumulate_running_time();
        self.termination_reason = Some(reason.clone());
        self.set_phase(ServicePhase::Terminated);
        tracing::info!(
            service = %self.service_name,
            reason = %reason,
            total_starts = self.start_count,
            total_failures = self.total_failures,
            cumulative_running_secs = self.cumulative_running.as_secs_f64(),
            "service terminated"
        );
        Ok(())
    }

    // ── Internal helpers ──────────────────────────────────────────────

    fn validate_transition(&self, target: ServicePhase) -> Result<(), TransitionError> {
        let valid = match (self.phase, target) {
            (ServicePhase::Starting, ServicePhase::Running) => true,
            (ServicePhase::Starting, ServicePhase::Terminated) => true,
            (ServicePhase::Starting, ServicePhase::Stopping) => true,
            (ServicePhase::Starting, ServicePhase::BackingOff) => true,

            (ServicePhase::Running, ServicePhase::BackingOff) => true,
            (ServicePhase::Running, ServicePhase::Stopping) => true,
            (ServicePhase::Running, ServicePhase::Terminated) => true,

            (ServicePhase::BackingOff, ServicePhase::Starting) => true,
            (ServicePhase::BackingOff, ServicePhase::Stopping) => true,
            (ServicePhase::BackingOff, ServicePhase::Terminated) => true,

            (ServicePhase::Stopping, ServicePhase::Terminated) => true,

            (ServicePhase::Terminated, _) => false,

            _ => false,
        };

        if valid {
            Ok(())
        } else {
            Err(TransitionError {
                from: self.phase,
                to: target,
            })
        }
    }

    fn set_phase(&mut self, phase: ServicePhase) {
        self.phase = phase;
        self.phase_entered_at = Instant::now();
    }

    fn accumulate_running_time(&mut self) {
        if let Some(since) = self.running_since.take() {
            self.cumulative_running += since.elapsed();
        }
    }
}

impl fmt::Display for ServiceLifecycle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}[{}] starts={} failures={} running={:.1}s",
            self.service_name,
            self.phase,
            self.start_count,
            self.total_failures,
            self.cumulative_running_time().as_secs_f64(),
        )
    }
}

// ---------------------------------------------------------------------------
// Serializable snapshot for health / metrics
// ---------------------------------------------------------------------------

/// Point-in-time snapshot of a service's lifecycle, suitable for
/// serialization (e.g., for a `/health` JSON endpoint).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceLifecycleSnapshot {
    /// Service name.
    pub service_name: String,
    /// Lifecycle phase at snapshot time.
    pub phase: ServicePhase,
    /// Total start attempts to date.
    pub start_count: u32,
    /// Total failures to date.
    pub total_failures: u32,
    /// Last recorded error message, if any.
    pub last_error: Option<String>,
    /// Cumulative wall-clock time in `Running`.
    pub cumulative_running_secs: f64,
    /// How long since the service was first created.
    pub age_secs: f64,
    /// How long since entering the current phase.
    pub time_in_phase_secs: f64,
    /// Termination reason as a human string, if `phase == Terminated`.
    pub termination_reason: Option<String>,
}

impl From<&ServiceLifecycle> for ServiceLifecycleSnapshot {
    fn from(lc: &ServiceLifecycle) -> Self {
        Self {
            service_name: lc.service_name.clone(),
            phase: lc.phase,
            start_count: lc.start_count,
            total_failures: lc.total_failures,
            last_error: lc.last_error.clone(),
            cumulative_running_secs: lc.cumulative_running_time().as_secs_f64(),
            age_secs: lc.age().as_secs_f64(),
            time_in_phase_secs: lc.time_in_current_phase().as_secs_f64(),
            termination_reason: lc.termination_reason.as_ref().map(|r| r.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_lifecycle_starts_in_starting() {
        let lc = ServiceLifecycle::new("test-svc");
        assert_eq!(lc.phase(), ServicePhase::Starting);
        assert_eq!(lc.start_count(), 1);
        assert_eq!(lc.total_failures(), 0);
        assert!(lc.last_error().is_none());
        assert!(lc.termination_reason().is_none());
    }

    #[test]
    fn test_service_name() {
        let lc = ServiceLifecycle::new("data-service");
        assert_eq!(lc.service_name(), "data-service");
    }

    #[test]
    fn test_happy_path_starting_to_running_to_stopping_to_terminated() {
        let mut lc = ServiceLifecycle::new("happy");
        lc.transition_to_running().unwrap();
        assert_eq!(lc.phase(), ServicePhase::Running);
        lc.transition_to_stopping().unwrap();
        assert_eq!(lc.phase(), ServicePhase::Stopping);
        lc.transition_to_terminated(TerminationReason::Cancelled)
            .unwrap();
        assert_eq!(lc.phase(), ServicePhase::Terminated);
        assert_eq!(lc.termination_reason(), Some(&TerminationReason::Cancelled));
    }

    #[test]
    fn test_failure_and_restart_cycle() {
        let mut lc = ServiceLifecycle::new("flaky");
        lc.transition_to_running().unwrap();
        assert_eq!(lc.start_count(), 1);

        lc.transition_to_backing_off("connection refused", Duration::from_millis(200))
            .unwrap();
        assert_eq!(lc.phase(), ServicePhase::BackingOff);
        assert_eq!(lc.total_failures(), 1);
        assert_eq!(lc.last_error(), Some("connection refused"));

        lc.transition_to_restarting().unwrap();
        assert_eq!(lc.phase(), ServicePhase::Starting);
        assert_eq!(lc.start_count(), 2);

        lc.transition_to_running().unwrap();
        assert_eq!(lc.phase(), ServicePhase::Running);
    }

    #[test]
    fn test_circuit_breaker_termination() {
        let mut lc = ServiceLifecycle::new("breaker");
        lc.transition_to_running().unwrap();
        lc.transition_to_backing_off("error 1", Duration::from_millis(100))
            .unwrap();

        lc.transition_to_terminated(TerminationReason::CircuitBreakerOpen {
            failures: 10,
            max_retries: 10,
        })
        .unwrap();

        assert_eq!(lc.phase(), ServicePhase::Terminated);
        assert!(matches!(
            lc.termination_reason(),
            Some(TerminationReason::CircuitBreakerOpen { .. })
        ));
    }

    #[test]
    fn test_completed_termination_from_running() {
        let mut lc = ServiceLifecycle::new("one-shot");
        lc.transition_to_running().unwrap();
        lc.transition_to_terminated(TerminationReason::Completed)
            .unwrap();
        assert_eq!(lc.phase(), ServicePhase::Terminated);
        assert_eq!(lc.termination_reason(), Some(&TerminationReason::Completed));
    }

    #[test]
    fn test_invalid_transition_terminated_to_anything() {
        let mut lc = ServiceLifecycle::new("dead");
        lc.transition_to_running().unwrap();
        lc.transition_to_terminated(TerminationReason::Completed)
            .unwrap();

        assert!(lc.transition_to_running().is_err());
        assert!(lc.transition_to_stopping().is_err());
        assert!(
            lc.transition_to_terminated(TerminationReason::Cancelled)
                .is_err()
        );
        assert!(lc.transition_to_restarting().is_err());
    }

    #[test]
    fn test_invalid_transition_running_to_starting() {
        let mut lc = ServiceLifecycle::new("bad");
        lc.transition_to_running().unwrap();

        let err = lc.transition_to_restarting().unwrap_err();
        assert_eq!(err.from, ServicePhase::Running);
        assert_eq!(err.to, ServicePhase::Starting);
    }

    #[test]
    fn test_stopping_from_backing_off() {
        let mut lc = ServiceLifecycle::new("interrupted");
        lc.transition_to_running().unwrap();
        lc.transition_to_backing_off("timeout", Duration::from_secs(5))
            .unwrap();

        lc.transition_to_stopping().unwrap();
        assert_eq!(lc.phase(), ServicePhase::Stopping);

        lc.transition_to_terminated(TerminationReason::Cancelled)
            .unwrap();
        assert_eq!(lc.phase(), ServicePhase::Terminated);
    }

    #[test]
    fn test_starting_directly_to_terminated() {
        let mut lc = ServiceLifecycle::new("init-fail");
        lc.transition_to_terminated(TerminationReason::Unrecoverable(
            "missing config".to_string(),
        ))
        .unwrap();
        assert_eq!(lc.phase(), ServicePhase::Terminated);
    }

    #[test]
    fn test_starting_to_backing_off() {
        let mut lc = ServiceLifecycle::new("init-retry");
        lc.transition_to_backing_off("db connect timeout", Duration::from_millis(500))
            .unwrap();
        assert_eq!(lc.phase(), ServicePhase::BackingOff);
        assert_eq!(lc.total_failures(), 1);
    }

    #[test]
    fn test_phase_display() {
        assert_eq!(ServicePhase::Starting.to_string(), "starting");
        assert_eq!(ServicePhase::Running.to_string(), "running");
        assert_eq!(ServicePhase::BackingOff.to_string(), "backing_off");
        assert_eq!(ServicePhase::Stopping.to_string(), "stopping");
        assert_eq!(ServicePhase::Terminated.to_string(), "terminated");
    }

    #[test]
    fn test_phase_is_terminal() {
        assert!(!ServicePhase::Starting.is_terminal());
        assert!(!ServicePhase::Running.is_terminal());
        assert!(!ServicePhase::BackingOff.is_terminal());
        assert!(!ServicePhase::Stopping.is_terminal());
        assert!(ServicePhase::Terminated.is_terminal());
    }

    #[test]
    fn test_phase_is_alive() {
        assert!(ServicePhase::Starting.is_alive());
        assert!(ServicePhase::Running.is_alive());
        assert!(ServicePhase::BackingOff.is_alive());
        assert!(!ServicePhase::Stopping.is_alive());
        assert!(!ServicePhase::Terminated.is_alive());
    }

    #[test]
    fn test_lifecycle_display() {
        let lc = ServiceLifecycle::new("display-test");
        let display = format!("{lc}");
        assert!(display.contains("display-test"));
        assert!(display.contains("starting"));
        assert!(display.contains("starts=1"));
        assert!(display.contains("failures=0"));
    }

    #[test]
    fn test_snapshot_from_lifecycle() {
        let mut lc = ServiceLifecycle::new("snapshot-svc");
        lc.transition_to_running().unwrap();
        lc.transition_to_backing_off("oops", Duration::from_millis(100))
            .unwrap();

        let snap = ServiceLifecycleSnapshot::from(&lc);
        assert_eq!(snap.service_name, "snapshot-svc");
        assert_eq!(snap.phase, ServicePhase::BackingOff);
        assert_eq!(snap.start_count, 1);
        assert_eq!(snap.total_failures, 1);
        assert_eq!(snap.last_error.as_deref(), Some("oops"));
        assert!(snap.termination_reason.is_none());
        assert!(snap.age_secs >= 0.0);
    }

    #[test]
    fn test_termination_reason_display() {
        assert_eq!(TerminationReason::Completed.to_string(), "completed");
        assert_eq!(TerminationReason::Cancelled.to_string(), "cancelled");
        assert_eq!(
            TerminationReason::CircuitBreakerOpen {
                failures: 5,
                max_retries: 5
            }
            .to_string(),
            "circuit breaker open (5/5 failures)"
        );
        assert_eq!(
            TerminationReason::Unrecoverable("bad config".into()).to_string(),
            "unrecoverable: bad config"
        );
    }

    #[test]
    fn test_transition_error_display() {
        let err = TransitionError {
            from: ServicePhase::Terminated,
            to: ServicePhase::Running,
        };
        assert_eq!(
            err.to_string(),
            "invalid lifecycle transition: terminated → running"
        );
    }

    #[test]
    fn test_multiple_failure_cycles_accumulate() {
        let mut lc = ServiceLifecycle::new("multi-fail");

        for i in 1..=5 {
            lc.transition_to_running().unwrap();
            lc.transition_to_backing_off(
                &format!("error {i}"),
                Duration::from_millis(100 * i as u64),
            )
            .unwrap();
            if i < 5 {
                lc.transition_to_restarting().unwrap();
            }
        }

        assert_eq!(lc.total_failures(), 5);
        assert_eq!(lc.start_count(), 5);
        assert_eq!(lc.last_error(), Some("error 5"));
    }

    #[test]
    fn test_stopping_from_starting() {
        let mut lc = ServiceLifecycle::new("early-stop");
        lc.transition_to_stopping().unwrap();
        assert_eq!(lc.phase(), ServicePhase::Stopping);
        lc.transition_to_terminated(TerminationReason::Cancelled)
            .unwrap();
    }
}
