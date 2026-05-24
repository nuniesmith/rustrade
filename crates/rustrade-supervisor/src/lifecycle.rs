//! Per-service lifecycle state machine.
//!
//! **Port target.** Copy `janus-main/lib/janus-core/src/supervisor/lifecycle.rs`
//! here verbatim.

use std::time::Instant;

/// Service lifecycle phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServicePhase {
    Starting,
    Running,
    Restarting,
    Terminating,
    Terminated,
}

impl ServicePhase {
    /// True if the service could still produce more activity.
    pub fn is_alive(self) -> bool {
        !matches!(self, Self::Terminated)
    }
}

impl std::fmt::Display for ServicePhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Starting => write!(f, "starting"),
            Self::Running => write!(f, "running"),
            Self::Restarting => write!(f, "restarting"),
            Self::Terminating => write!(f, "terminating"),
            Self::Terminated => write!(f, "terminated"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminationReason {
    /// Service exited cleanly.
    NormalExit,
    /// Service was cancelled by the supervisor.
    Cancelled,
    /// Service exceeded its restart budget.
    MaxRetriesExceeded,
    /// Circuit breaker tripped.
    CircuitBreakerOpen,
    /// Service panicked or returned an unrecoverable error.
    Unrecoverable(String),
}

/// Placeholder lifecycle record.
#[derive(Debug)]
pub struct ServiceLifecycle {
    pub name: String,
    pub phase: ServicePhase,
    pub started_at: Instant,
    pub start_count: u32,
    pub termination_reason: Option<String>,
}

impl ServiceLifecycle {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            phase: ServicePhase::Starting,
            started_at: Instant::now(),
            start_count: 0,
            termination_reason: None,
        }
    }
}
