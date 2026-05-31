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
//!
//! Time is read through the [`Clock`] trait so tests can advance the
//! clock without sleeping.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::clock::{Clock, SystemClock};

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

/// Restart-durable snapshot of a [`CircuitBreaker`]'s mutable state.
///
/// Both fields are absolute Unix-second timestamps, so the snapshot is
/// fully portable across a restart: after restoring and calling
/// [`CircuitBreaker::tick`], elapsed wall-clock downtime is accounted for
/// — stale losses fall out of the rolling window and an expired cooldown
/// auto-resets. The configured limits are excluded; they come from the
/// live instance on restore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CircuitBreakerSnapshot {
    /// Unix-second timestamps of recent losses still in (or near) the window.
    pub recent_losses: Vec<u64>,
    /// When the breaker tripped (Unix seconds), or `None` if untripped.
    pub tripped_at_unix_secs: Option<u64>,
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
    /// see [`Self::record_win`] below.
    recent_losses: VecDeque<u64>,
    tripped_at_unix_secs: Option<u64>,
    clock: Arc<dyn Clock>,
}

impl CircuitBreaker {
    /// Create with the default system clock.
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self::with_clock(config, Arc::new(SystemClock))
    }

    /// Create with an injected clock — typically `Arc<ManualClock>` from
    /// [`crate::clock`] in tests.
    pub fn with_clock(config: CircuitBreakerConfig, clock: Arc<dyn Clock>) -> Self {
        Self {
            config,
            recent_losses: VecDeque::with_capacity(16),
            tripped_at_unix_secs: None,
            clock,
        }
    }

    /// Call once per decision tick to auto-reset the breaker after cooldown
    /// and evict stale loss timestamps.
    pub fn tick(&mut self) {
        let now = self.clock.now_unix_secs();
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
        let now = self.clock.now_unix_secs();
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
        self.evict_old(self.clock.now_unix_secs());
    }

    /// Is the breaker currently tripped and within its cooldown window?
    pub fn is_tripped(&self) -> bool {
        self.tripped_at_unix_secs.is_some_and(|t| {
            self.clock.now_unix_secs().saturating_sub(t) < self.config.cooldown_secs
        })
    }

    /// Capture the mutable breaker state for persistence.
    ///
    /// Pairs with [`Self::restore`]. The configured limits and the clock
    /// are intentionally excluded — only the recorded loss timestamps and
    /// trip time are persisted.
    pub fn snapshot(&self) -> CircuitBreakerSnapshot {
        CircuitBreakerSnapshot {
            recent_losses: self.recent_losses.iter().copied().collect(),
            tripped_at_unix_secs: self.tripped_at_unix_secs,
        }
    }

    /// Restore breaker state from a [`CircuitBreakerSnapshot`].
    ///
    /// Overwrites the loss window and trip time; keeps the configured
    /// limits and clock from the live instance. Call [`Self::tick`]
    /// afterwards to evict losses now outside the window and auto-reset the
    /// breaker if its cooldown elapsed while the process was down.
    pub fn restore(&mut self, snap: CircuitBreakerSnapshot) {
        self.recent_losses = snap.recent_losses.into_iter().collect();
        self.tripped_at_unix_secs = snap.tripped_at_unix_secs;
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
        let elapsed = self.clock.now_unix_secs().saturating_sub(t);
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
    use crate::clock::ManualClock;

    fn cfg(loss_limit: u32, window: u64, cooldown: u64) -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            loss_limit,
            window_secs: window,
            cooldown_secs: cooldown,
        }
    }

    fn breaker(
        loss_limit: u32,
        window: u64,
        cooldown: u64,
        start: u64,
    ) -> (CircuitBreaker, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::new(start));
        let cb = CircuitBreaker::with_clock(cfg(loss_limit, window, cooldown), clock.clone());
        (cb, clock)
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

    #[test]
    fn old_losses_evicted_from_rolling_window() {
        let (mut cb, clock) = breaker(
            /*limit*/ 3, /*window*/ 3600, /*cooldown*/ 600, 1_000_000,
        );

        cb.record_loss(); // t=1_000_000
        cb.record_loss(); // t=1_000_000
        assert_eq!(cb.recent_loss_count(), 2);

        // Advance past the window — those losses should be evicted on the
        // next interaction with the breaker.
        clock.advance_secs(3_700);
        cb.record_loss(); // pushes a fresh loss; previous ones are >window
        assert_eq!(
            cb.recent_loss_count(),
            1,
            "losses outside the rolling window must be evicted"
        );
        assert!(!cb.is_tripped(), "rolling count of 1 should not trip");
    }

    #[test]
    fn cooldown_auto_resets_on_tick() {
        let (mut cb, clock) = breaker(
            /*limit*/ 2, /*window*/ 3600, /*cooldown*/ 600, 1_000_000,
        );

        cb.record_loss();
        cb.record_loss();
        assert!(cb.is_tripped());
        assert_eq!(
            cb.cooldown_remaining(),
            Some(Duration::from_secs(600)),
            "cooldown should report full remaining at trip time"
        );

        // Halfway through cooldown — still tripped.
        clock.advance_secs(300);
        cb.tick();
        assert!(cb.is_tripped());
        assert_eq!(cb.cooldown_remaining(), Some(Duration::from_secs(300)));

        // Past cooldown — tick should reset.
        clock.advance_secs(301);
        cb.tick();
        assert!(!cb.is_tripped());
        assert_eq!(cb.cooldown_remaining(), None);
        assert_eq!(cb.recent_loss_count(), 0);
    }

    #[test]
    fn losses_spaced_outside_window_never_trip() {
        // The whole point of the sliding-window design over a consecutive
        // counter: 3 losses spaced > window apart should not trip a
        // limit-3 breaker.
        let (mut cb, clock) = breaker(3, /*window*/ 3600, 600, 1_000_000);
        cb.record_loss();
        clock.advance_secs(3_700);
        cb.record_loss();
        clock.advance_secs(3_700);
        cb.record_loss();
        assert!(!cb.is_tripped());
        assert_eq!(cb.recent_loss_count(), 1);
    }

    #[test]
    fn snapshot_restore_preserves_tripped_state() {
        let (mut cb, clock) = breaker(2, 14_400, 3_600, 1_000_000);
        cb.record_loss();
        cb.record_loss();
        assert!(cb.is_tripped());
        let snap = cb.snapshot();
        assert_eq!(snap.recent_losses.len(), 2);
        assert_eq!(snap.tripped_at_unix_secs, Some(1_000_000));

        // Restore into a fresh breaker sharing the clock — still tripped,
        // same remaining cooldown.
        let mut restored = CircuitBreaker::with_clock(cfg(2, 14_400, 3_600), clock.clone());
        restored.restore(snap.clone());
        assert!(restored.is_tripped());
        assert_eq!(restored.recent_loss_count(), 2);
        assert_eq!(restored.snapshot(), snap);
    }

    #[test]
    fn restore_then_tick_resets_after_cooldown_elapsed_during_downtime() {
        // Trip at t=1_000_000 with a 600s cooldown, snapshot, then "reboot"
        // 700s later. A post-restore tick must auto-reset the expired breaker.
        let (mut cb, _clock) = breaker(2, 14_400, 600, 1_000_000);
        cb.record_loss();
        cb.record_loss();
        assert!(cb.is_tripped());
        let snap = cb.snapshot();

        let later = Arc::new(ManualClock::new(1_000_700));
        let mut restored = CircuitBreaker::with_clock(cfg(2, 14_400, 600), later);
        restored.restore(snap);
        restored.tick();
        assert!(
            !restored.is_tripped(),
            "expired cooldown must reset on tick"
        );
        assert_eq!(restored.recent_loss_count(), 0);
    }

    #[test]
    fn restore_then_tick_evicts_losses_outside_window() {
        // Two losses at t=1_000_000 with a 3_600s window; reboot 4_000s
        // later. Tick must evict both stale losses.
        let (mut cb, _clock) = breaker(5, 3_600, 600, 1_000_000);
        cb.record_loss();
        cb.record_loss();
        let snap = cb.snapshot();

        let later = Arc::new(ManualClock::new(1_004_000));
        let mut restored = CircuitBreaker::with_clock(cfg(5, 3_600, 600), later);
        restored.restore(snap);
        restored.tick();
        assert_eq!(
            restored.recent_loss_count(),
            0,
            "stale losses must be evicted"
        );
    }
}
