//! Exponential backoff with optional circuit breaker.
//!
//! **Port target.** Copy `janus-main/lib/janus-core/src/supervisor/backoff.rs`
//! here verbatim. It is self-contained (no janus-specific dependencies)
//! and compiles with only `std` and `tokio::time`.
//!
//! Public API (stable across the port):
//!
//! - [`BackoffConfig`] — tunable min/max delay, jitter, and circuit breaker
//!   parameters.
//! - [`BackoffState`] — per-service restart state.
//! - [`BackoffAction`] — what the supervisor should do next: `Retry(delay)`,
//!   `Stop(reason)`, or `CircuitOpen`.

// TODO: lift `janus-core/supervisor/backoff.rs` here.

use std::time::Duration;

/// Placeholder — replace with the janus-core BackoffConfig.
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    pub min_delay: Duration,
    pub max_delay: Duration,
    pub max_attempts: Option<u32>,
    pub circuit_breaker_threshold: Option<u32>,
    pub circuit_breaker_cooldown: Option<Duration>,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            min_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(60),
            max_attempts: None,
            circuit_breaker_threshold: Some(10),
            circuit_breaker_cooldown: Some(Duration::from_secs(300)),
        }
    }
}

impl BackoffConfig {
    /// Construct with custom min/max delays.
    pub fn new(min_delay: Duration, max_delay: Duration) -> Self {
        Self {
            min_delay,
            max_delay,
            ..Self::default()
        }
    }

    pub fn with_circuit_breaker(mut self, threshold: u32, cooldown: Duration) -> Self {
        self.circuit_breaker_threshold = Some(threshold);
        self.circuit_breaker_cooldown = Some(cooldown);
        self
    }
}

/// Placeholder — replace with janus-core BackoffState.
#[derive(Debug, Default)]
pub struct BackoffState {
    pub attempts: u32,
    pub consecutive_failures: u32,
}

/// What the supervisor should do next after a service exit.
#[derive(Debug, Clone)]
pub enum BackoffAction {
    Retry(Duration),
    Stop(String),
    CircuitOpen,
}
