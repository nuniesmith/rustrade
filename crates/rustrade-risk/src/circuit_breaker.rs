//! Sliding-window circuit breaker for trading strategies.
//!
//! Trips when `loss_limit` losses occur within a rolling `window` duration.
//! Once tripped, new trade entries are blocked for `cooldown` seconds
//! before the breaker auto-resets.
//!
//! This is a direct generalization of the circuit breaker shipped with the
//! kucoin bot in Apr 2026 — the sliding-window design replaces the older
//! consecutive-loss pattern because losses spaced hours apart would reset
//! the consecutive counter before ever tripping it.

use std::collections::VecDeque;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX epoch")
        .as_secs()
}

/// Configuration for [`CircuitBreaker`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Number of losses in the rolling window that trips the breaker.
    pub loss_limit: u32,
    /// Rolling lookback window in seconds (e.g. 14400 = 4 hours).
    pub window_secs: u64,
    /// How long the breaker stays tripped once fired.
    pub cooldown_secs: u64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        // Defaults chosen from the kucoin Apr 2026 review:
        //   4 losses in any rolling 4h window → trip, then 1h cooldown.
        Self {
            loss_limit: 4,
            window_secs: 14_400,
            cooldown_secs: 3_600,
        }
    }
}

/// Sliding-window loss breaker.
///
/// # Example
///
/// ```
/// use rustrade_risk::{CircuitBreaker, CircuitBreakerConfig};
///
/// let mut cb = CircuitBreaker::new(CircuitBreakerConfig {
///     loss_limit: 3,
///     window_secs: 3600,
///     cooldown_secs: 600,
/// });
///
/// cb.record_loss();
/// cb.record_loss();
/// assert!(!cb.is_tripped());
/// cb.record_loss();
/// assert!(cb.is_tripped());
/// ```
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    /// Timestamps of recent losses (Unix seconds). Wins are not stored —
    /// see `record_win()` below.
    recent_losses: VecDeque<u64>,
    tripped_at_unix_secs: Option<u64>,
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            recent_losses: VecDeque::with_capacity(16),
            tripped_at_unix_secs: None,
        }
    }

    /// Call once per decision tick to auto-reset the breaker after cooldown
    /// and evict stale loss timestamps.
    pub fn tick(&mut self) {
        let now = now_unix_secs();
        if let Some(t) = self.tripped_at_unix_secs
            && now.saturating_sub(t) >= self.config.cooldown_secs
        {
            self.reset();
        }
        self.evict_old(now);
    }

    /// Record a losing trade. Trips the breaker if the rolling count
    /// within `window_secs` reaches `loss_limit`.
    pub fn record_loss(&mut self) {
        let now = now_unix_secs();
        self.recent_losses.push_back(now);
        self.evict_old(now);

        if self.recent_losses.len() as u32 >= self.config.loss_limit {
            self.tripped_at_unix_secs = Some(now);
            tracing::warn!(
                losses = self.recent_losses.len(),
                window_secs = self.config.window_secs,
                "circuit breaker tripped"
            );
        }
    }

    /// Record a winning trade.
    ///
    /// **Does NOT clear the tripped state** — once tripped, only elapsed
    /// cooldown can un-trip the breaker. A single win is not evidence that
    /// market conditions have recovered.
    pub fn record_win(&mut self) {
        self.evict_old(now_unix_secs());
    }

    /// Is the breaker currently tripped and within its cooldown window?
    pub fn is_tripped(&self) -> bool {
        self.tripped_at_unix_secs
            .is_some_and(|t| now_unix_secs().saturating_sub(t) < self.config.cooldown_secs)
    }

    /// Manually clear the breaker. Typically not called in production — the
    /// cooldown does this automatically.
    pub fn reset(&mut self) {
        self.recent_losses.clear();
        self.tripped_at_unix_secs = None;
    }

    /// Number of losses currently in the rolling window.
    pub fn recent_loss_count(&self) -> usize {
        self.recent_losses.len()
    }

    /// Cooldown time remaining if tripped, else `None`.
    pub fn cooldown_remaining(&self) -> Option<Duration> {
        let t = self.tripped_at_unix_secs?;
        let elapsed = now_unix_secs().saturating_sub(t);
        (elapsed < self.config.cooldown_secs)
            .then(|| Duration::from_secs(self.config.cooldown_secs - elapsed))
    }

    fn evict_old(&mut self, now: u64) {
        let cutoff = now.saturating_sub(self.config.window_secs);
        while let Some(&ts) = self.recent_losses.front() {
            if ts < cutoff {
                self.recent_losses.pop_front();
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(loss_limit: u32, window: u64, cooldown: u64) -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            loss_limit,
            window_secs: window,
            cooldown_secs: cooldown,
        }
    }

    #[test]
    fn starts_untripped() {
        let cb = CircuitBreaker::new(cfg(4, 14400, 3600));
        assert!(!cb.is_tripped());
        assert_eq!(cb.recent_loss_count(), 0);
    }

    #[test]
    fn trips_at_limit() {
        let mut cb = CircuitBreaker::new(cfg(3, 14400, 3600));
        cb.record_loss();
        cb.record_loss();
        assert!(!cb.is_tripped());
        cb.record_loss();
        assert!(cb.is_tripped());
    }

    #[test]
    fn win_does_not_untrip() {
        let mut cb = CircuitBreaker::new(cfg(2, 14400, 3600));
        cb.record_loss();
        cb.record_loss();
        assert!(cb.is_tripped());
        cb.record_win();
        assert!(cb.is_tripped());
    }

    #[test]
    fn reset_clears_state() {
        let mut cb = CircuitBreaker::new(cfg(2, 14400, 3600));
        cb.record_loss();
        cb.record_loss();
        cb.reset();
        assert!(!cb.is_tripped());
        assert_eq!(cb.recent_loss_count(), 0);
    }
}
