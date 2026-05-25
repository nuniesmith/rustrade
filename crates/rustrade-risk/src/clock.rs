//! Injectable clock abstraction for time-dependent risk primitives.
//!
//! [`CircuitBreaker`](crate::CircuitBreaker) and
//! [`SessionPnl`](crate::SessionPnl) both depend on wall-clock time —
//! the breaker's rolling loss window, the PnL session's 00:00 UTC
//! rollover. Real code uses the [`SystemClock`]; tests can substitute
//! [`ManualClock`] (or any other [`Clock`] impl) so they don't have to
//! sleep for hours to exercise rollover boundaries.
//!
//! ```
//! use std::sync::Arc;
//! use rustrade_risk::clock::{Clock, ManualClock};
//!
//! let clock = Arc::new(ManualClock::new(1_000));
//! assert_eq!(clock.now_unix_secs(), 1_000);
//! clock.advance_secs(86_400);
//! assert_eq!(clock.now_unix_secs(), 87_400);
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Source of wall-clock time, in whole UNIX seconds.
///
/// The trait is intentionally minimal: every risk primitive in this crate
/// needs seconds-since-epoch and the derived day number, and nothing else.
///
/// `Debug` is a supertrait so types that store an `Arc<dyn Clock>` (like
/// [`CircuitBreaker`](crate::CircuitBreaker) and
/// [`SessionPnl`](crate::SessionPnl)) can still `#[derive(Debug)]`.
pub trait Clock: std::fmt::Debug + Send + Sync + 'static {
    /// Current time in seconds since the UNIX epoch.
    fn now_unix_secs(&self) -> u64;

    /// Whole days since the UNIX epoch. Used by
    /// [`SessionPnl`](crate::SessionPnl) to detect 00:00 UTC rollover.
    fn utc_day_number(&self) -> u64 {
        self.now_unix_secs() / 86_400
    }
}

/// The default clock — reads the OS wall clock via [`SystemTime`].
///
/// All production code uses this.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_secs(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before UNIX epoch")
            .as_secs()
    }
}

/// A manually-advanceable clock for tests.
///
/// Stored as an `AtomicU64` so it's `Send + Sync` and cheap to clone via
/// `Arc<ManualClock>`. Both [`Self::set`] and [`Self::advance_secs`] are
/// available; pick whichever reads better at the call site.
#[derive(Debug, Default)]
pub struct ManualClock {
    now: AtomicU64,
}

impl ManualClock {
    /// Create a clock starting at the given UNIX seconds.
    pub fn new(start_unix_secs: u64) -> Self {
        Self {
            now: AtomicU64::new(start_unix_secs),
        }
    }

    /// Set the clock to an absolute time.
    pub fn set(&self, unix_secs: u64) {
        self.now.store(unix_secs, Ordering::SeqCst);
    }

    /// Move the clock forward by `secs`. Returns the new time.
    pub fn advance_secs(&self, secs: u64) -> u64 {
        self.now.fetch_add(secs, Ordering::SeqCst) + secs
    }
}

impl Clock for ManualClock {
    fn now_unix_secs(&self) -> u64 {
        self.now.load(Ordering::SeqCst)
    }
}

/// `Arc<ManualClock>` implements [`Clock`] too — so callers can share a
/// single clock between a test harness and the risk primitive under test
/// without losing the manual-advance methods.
impl<C: Clock + ?Sized> Clock for Arc<C> {
    fn now_unix_secs(&self) -> u64 {
        (**self).now_unix_secs()
    }
    fn utc_day_number(&self) -> u64 {
        (**self).utc_day_number()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_advances() {
        let c = SystemClock;
        let t1 = c.now_unix_secs();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let t2 = c.now_unix_secs();
        assert!(t2 > t1, "expected clock to advance, t1={t1} t2={t2}");
    }

    #[test]
    fn manual_clock_set_and_advance() {
        let c = ManualClock::new(100);
        assert_eq!(c.now_unix_secs(), 100);

        let after = c.advance_secs(50);
        assert_eq!(after, 150);
        assert_eq!(c.now_unix_secs(), 150);

        c.set(1_000);
        assert_eq!(c.now_unix_secs(), 1_000);
    }

    #[test]
    fn manual_clock_default_day_number() {
        let c = ManualClock::new(86_400 * 3 + 42);
        assert_eq!(c.utc_day_number(), 3);
    }

    #[test]
    fn arc_clock_delegates() {
        let c = Arc::new(ManualClock::new(500));
        assert_eq!(c.now_unix_secs(), 500);
        c.advance_secs(1);
        // Calling Clock methods on the Arc directly:
        assert_eq!(Clock::now_unix_secs(&c), 501);
    }
}
