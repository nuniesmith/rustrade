//! Exponential backoff with jitter for supervisor restart strategies.
//!
//! ```text
//! delay = min(cap, base * 2^attempt) + jitter
//! ```
//!
//! Where `jitter` is a random value in `[0, base * 2^attempt)` to prevent
//! thundering-herd scenarios when multiple services restart simultaneously
//! after a shared outage.
//!
//! # Features
//!
//! - **Exponential growth** with configurable base and cap
//! - **Full jitter** to desynchronize concurrent restarts
//! - **Cooldown reset**: if a service runs successfully for a configurable
//!   period, the attempt counter resets to zero so occasional rare failures
//!   don't accumulate toward a long backoff
//! - **Circuit breaker**: if a service fails N times within a window T,
//!   the supervisor terminates the service rather than retrying forever

use std::time::{Duration, Instant};

use rand::Rng;

/// Configuration for the exponential backoff strategy.
///
/// All fields have sensible defaults for a production trading system.
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    /// Initial delay before the first retry (default: 100ms).
    pub base_delay: Duration,

    /// Maximum delay between retries (default: 60s).
    pub max_delay: Duration,

    /// If a service runs successfully for at least this long, the attempt
    /// counter resets to zero (default: 5 minutes).
    pub cooldown_period: Duration,

    /// Maximum consecutive failures before the circuit breaker trips.
    /// Set to `0` to disable the circuit breaker (default: 10).
    pub max_retries: u32,

    /// Window within which `max_retries` failures trigger the circuit
    /// breaker (default: 10 minutes). Only relevant when `max_retries > 0`.
    pub circuit_breaker_window: Duration,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(60),
            cooldown_period: Duration::from_secs(300),
            max_retries: 10,
            circuit_breaker_window: Duration::from_secs(600),
        }
    }
}

impl BackoffConfig {
    /// Create a new config with the given base and max delays.
    pub fn new(base_delay: Duration, max_delay: Duration) -> Self {
        Self {
            base_delay,
            max_delay,
            ..Default::default()
        }
    }

    /// Builder: set the cooldown period.
    pub fn with_cooldown(mut self, cooldown: Duration) -> Self {
        self.cooldown_period = cooldown;
        self
    }

    /// Builder: set the circuit breaker parameters.
    pub fn with_circuit_breaker(mut self, max_retries: u32, window: Duration) -> Self {
        self.max_retries = max_retries;
        self.circuit_breaker_window = window;
        self
    }

    /// Builder: disable the circuit breaker entirely.
    pub fn without_circuit_breaker(mut self) -> Self {
        self.max_retries = 0;
        self
    }
}

/// Result of computing the next backoff delay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackoffAction {
    /// Wait for the given duration and then retry.
    Retry(Duration),

    /// The circuit breaker has tripped — too many failures within the
    /// configured window. The supervisor should escalate rather than retry.
    CircuitOpen {
        /// Total failures observed within the window.
        failures: u32,
        /// The configured maximum before tripping.
        max_retries: u32,
    },
}

/// Mutable state tracker for an individual service's backoff history.
///
/// One `BackoffState` instance is maintained per supervised service inside
/// the [`Supervisor`](super::Supervisor).
#[derive(Debug, Clone)]
pub struct BackoffState {
    config: BackoffConfig,

    /// Number of consecutive failures (resets on cooldown).
    attempt: u32,

    /// Timestamps of recent failures (within the circuit-breaker window).
    /// Older entries are pruned on each call to [`Self::next_backoff`].
    failure_timestamps: Vec<Instant>,

    /// When the service last started successfully. Used for cooldown logic.
    last_start: Option<Instant>,
}

impl BackoffState {
    /// Create a fresh backoff state with the given configuration.
    pub fn new(config: BackoffConfig) -> Self {
        Self {
            config,
            attempt: 0,
            failure_timestamps: Vec::new(),
            last_start: None,
        }
    }

    /// Create with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(BackoffConfig::default())
    }

    /// Record that the service has started (or restarted) successfully.
    ///
    /// Called by the supervisor when a service's `run()` method is entered.
    pub fn record_start(&mut self) {
        self.last_start = Some(Instant::now());
    }

    /// Check if the cooldown period has elapsed since the last start, and
    /// if so, reset the attempt counter.
    ///
    /// Called by the supervisor when a service exits so a service that ran
    /// healthily for a long time doesn't carry old failure history into
    /// its next restart.
    pub fn maybe_reset_on_cooldown(&mut self) {
        if let Some(start) = self.last_start
            && start.elapsed() >= self.config.cooldown_period
        {
            tracing::info!(
                cooldown_secs = self.config.cooldown_period.as_secs(),
                "service ran longer than cooldown period, resetting backoff"
            );
            self.attempt = 0;
            self.failure_timestamps.clear();
        }
    }

    /// Record a failure and compute the next action.
    pub fn next_backoff(&mut self) -> BackoffAction {
        let now = Instant::now();

        self.failure_timestamps.push(now);
        self.attempt = self.attempt.saturating_add(1);

        // Prune old failure timestamps outside the circuit-breaker window.
        if self.config.max_retries > 0 {
            let window_start = now - self.config.circuit_breaker_window;
            self.failure_timestamps.retain(|ts| *ts >= window_start);

            if self.failure_timestamps.len() as u32 >= self.config.max_retries {
                return BackoffAction::CircuitOpen {
                    failures: self.failure_timestamps.len() as u32,
                    max_retries: self.config.max_retries,
                };
            }
        }

        let exp_delay = self.compute_exponential_delay();
        let jittered = self.add_jitter(exp_delay);
        BackoffAction::Retry(jittered)
    }

    /// Reset all state (attempt counter, failure history, start time).
    pub fn reset(&mut self) {
        self.attempt = 0;
        self.failure_timestamps.clear();
        self.last_start = None;
    }

    /// Current attempt number (consecutive failures since last reset).
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Number of failures recorded within the circuit-breaker window.
    pub fn recent_failures(&self) -> usize {
        self.failure_timestamps.len()
    }

    // ── Internal helpers ──────────────────────────────────────────────

    /// Compute `min(cap, base * 2^attempt)` without overflow.
    fn compute_exponential_delay(&self) -> Duration {
        let base_ms = self.config.base_delay.as_millis() as u64;
        let max_ms = self.config.max_delay.as_millis() as u64;

        // Prevent overflow: if attempt >= 63 the shift would overflow u64,
        // so we clamp early.
        let shift = self.attempt.min(62);

        let exp_ms = base_ms.saturating_mul(1u64.checked_shl(shift).unwrap_or(u64::MAX));
        let capped_ms = exp_ms.min(max_ms);

        Duration::from_millis(capped_ms)
    }

    /// Full jitter: returns a duration uniformly distributed in `[0, base]`.
    fn add_jitter(&self, base: Duration) -> Duration {
        if base.is_zero() {
            return base;
        }
        let base_ms = base.as_millis() as u64;
        let mut rng = rand::rng();
        let jitter_ms = rng.random_range(0..=base_ms);
        Duration::from_millis(jitter_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = BackoffConfig::default();
        assert_eq!(cfg.base_delay, Duration::from_millis(100));
        assert_eq!(cfg.max_delay, Duration::from_secs(60));
        assert_eq!(cfg.cooldown_period, Duration::from_secs(300));
        assert_eq!(cfg.max_retries, 10);
        assert_eq!(cfg.circuit_breaker_window, Duration::from_secs(600));
    }

    #[test]
    fn test_builder_pattern() {
        let cfg = BackoffConfig::new(Duration::from_millis(200), Duration::from_secs(30))
            .with_cooldown(Duration::from_secs(120))
            .with_circuit_breaker(5, Duration::from_secs(60));

        assert_eq!(cfg.base_delay, Duration::from_millis(200));
        assert_eq!(cfg.max_delay, Duration::from_secs(30));
        assert_eq!(cfg.cooldown_period, Duration::from_secs(120));
        assert_eq!(cfg.max_retries, 5);
        assert_eq!(cfg.circuit_breaker_window, Duration::from_secs(60));
    }

    #[test]
    fn test_without_circuit_breaker() {
        let cfg = BackoffConfig::default().without_circuit_breaker();
        assert_eq!(cfg.max_retries, 0);
    }

    #[test]
    fn test_exponential_growth_is_capped() {
        let cfg = BackoffConfig::new(Duration::from_millis(100), Duration::from_secs(5))
            .without_circuit_breaker();
        let state = BackoffState::new(cfg);

        let mut s = state.clone();
        s.attempt = 0;
        assert_eq!(s.compute_exponential_delay(), Duration::from_millis(100));

        s.attempt = 5;
        assert_eq!(s.compute_exponential_delay(), Duration::from_millis(3200));

        s.attempt = 10;
        assert_eq!(s.compute_exponential_delay(), Duration::from_secs(5)); // capped
    }

    #[test]
    fn test_exponential_no_overflow_at_high_attempts() {
        let cfg = BackoffConfig::default().without_circuit_breaker();
        let mut state = BackoffState::new(cfg);
        state.attempt = 100;
        assert_eq!(state.compute_exponential_delay(), Duration::from_secs(60));
    }

    #[test]
    fn test_jitter_stays_within_bounds() {
        let cfg = BackoffConfig::new(Duration::from_millis(100), Duration::from_secs(60))
            .without_circuit_breaker();
        let state = BackoffState::new(cfg);
        for _ in 0..1000 {
            let jittered = state.add_jitter(Duration::from_millis(1000));
            assert!(jittered <= Duration::from_millis(1000));
        }
    }

    #[test]
    fn test_jitter_zero_base() {
        let cfg = BackoffConfig::default();
        let state = BackoffState::new(cfg);
        assert_eq!(state.add_jitter(Duration::ZERO), Duration::ZERO);
    }

    #[test]
    fn test_next_backoff_increments_attempt() {
        let cfg = BackoffConfig::default().without_circuit_breaker();
        let mut state = BackoffState::new(cfg);
        assert_eq!(state.attempt(), 0);
        assert!(matches!(state.next_backoff(), BackoffAction::Retry(_)));
        assert_eq!(state.attempt(), 1);
        assert!(matches!(state.next_backoff(), BackoffAction::Retry(_)));
        assert_eq!(state.attempt(), 2);
    }

    #[test]
    fn test_circuit_breaker_trips() {
        let cfg = BackoffConfig::default().with_circuit_breaker(3, Duration::from_secs(600));
        let mut state = BackoffState::new(cfg);

        assert!(matches!(state.next_backoff(), BackoffAction::Retry(_)));
        assert!(matches!(state.next_backoff(), BackoffAction::Retry(_)));

        assert!(matches!(
            state.next_backoff(),
            BackoffAction::CircuitOpen {
                failures: 3,
                max_retries: 3
            }
        ));
    }

    #[test]
    fn test_reset_clears_state() {
        let cfg = BackoffConfig::default().without_circuit_breaker();
        let mut state = BackoffState::new(cfg);
        state.next_backoff();
        state.next_backoff();
        assert_eq!(state.attempt(), 2);
        assert_eq!(state.recent_failures(), 2);

        state.reset();
        assert_eq!(state.attempt(), 0);
        assert_eq!(state.recent_failures(), 0);
    }

    #[test]
    fn test_cooldown_resets_attempt() {
        let cfg = BackoffConfig::default()
            .with_cooldown(Duration::from_millis(50))
            .without_circuit_breaker();
        let mut state = BackoffState::new(cfg);

        state.next_backoff();
        state.next_backoff();
        assert_eq!(state.attempt(), 2);

        // Simulate elapsed cooldown by backdating last_start.
        state.last_start = Some(Instant::now() - Duration::from_millis(100));

        state.maybe_reset_on_cooldown();
        assert_eq!(state.attempt(), 0);
        assert_eq!(state.recent_failures(), 0);
    }

    #[test]
    fn test_cooldown_does_not_reset_if_too_early() {
        let cfg = BackoffConfig::default()
            .with_cooldown(Duration::from_secs(300))
            .without_circuit_breaker();
        let mut state = BackoffState::new(cfg);

        state.next_backoff();
        state.next_backoff();
        state.record_start();

        state.maybe_reset_on_cooldown();
        assert_eq!(state.attempt(), 2);
    }

    #[test]
    fn test_backoff_delay_increases_monotonically_ignoring_jitter() {
        let cfg = BackoffConfig::new(Duration::from_millis(100), Duration::from_secs(60))
            .without_circuit_breaker();
        let mut state = BackoffState::new(cfg);

        let delays: Vec<Duration> = (0..8)
            .map(|_| {
                let _ = state.next_backoff();
                state.compute_exponential_delay()
            })
            .collect();

        for window in delays.windows(2) {
            assert!(window[1] >= window[0], "{:?} < {:?}", window[1], window[0]);
        }
    }

    #[test]
    fn test_attempt_saturates() {
        let cfg = BackoffConfig::default().without_circuit_breaker();
        let mut state = BackoffState::new(cfg);
        state.attempt = u32::MAX;

        assert!(matches!(state.next_backoff(), BackoffAction::Retry(_)));
        assert_eq!(state.attempt(), u32::MAX);
    }
}
