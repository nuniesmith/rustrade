//! Per-session realised PnL tracker with optional drawdown cap and
//! daily 00:00 UTC rollover.
//!
//! Generalized from the `kucoin/bot/pnl.rs` shipped with the Apr 2026 bot.
//! Strategy code records every trade close via [`SessionPnl::record_close`];
//! the framework checks [`SessionPnl::is_session_halted`] before allowing
//! new entries.
//!
//! Time is read through the [`Clock`] trait so tests can advance the
//! clock and verify rollover without sleeping for a day.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::clock::{Clock, SystemClock};

/// Configuration for [`SessionPnl`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionPnlConfig {
    /// Per-session net loss ceiling in quote currency (negative number,
    /// e.g. -50.0 USDT). When net PnL drops to or below this value, the
    /// session is halted until the next 00:00 UTC rollover.
    ///
    /// Set to `f64::NEG_INFINITY` to disable the halt entirely.
    pub loss_limit: f64,
}

impl Default for SessionPnlConfig {
    fn default() -> Self {
        Self { loss_limit: -50.0 }
    }
}

/// Restart-durable snapshot of a [`SessionPnl`]'s mutable state.
///
/// Captures the running totals and halt flag — **not** the configured
/// `loss_limit`, the symbol, or the clock. Those come from the live
/// instance on restore, so an operator can adjust the loss limit between
/// runs and the new value takes effect immediately.
///
/// Restore via [`SessionPnl::restore`]. Because `last_reset_day` is kept,
/// calling [`SessionPnl::tick`] right after a restore correctly rolls the
/// session over if the snapshot was taken on an earlier UTC day.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionPnlSnapshot {
    /// Cumulative gross realised PnL, in quote currency.
    pub realised: f64,
    /// Cumulative fees paid this session, in quote currency.
    pub fees: f64,
    /// Total trades recorded this session.
    pub trades: u32,
    /// Wins (net PnL > 0).
    pub wins: u32,
    /// Losses (net PnL < 0).
    pub losses: u32,
    /// Break-evens (net PnL == 0).
    pub breakevens: u32,
    /// Whether the session was halted by the loss cap when snapshotted.
    pub halted: bool,
    /// UTC day number of the last reset, for rollover detection on restore.
    pub last_reset_day: u64,
}

/// Running PnL totals for one trading session, with an optional drawdown
/// cap that auto-resets at the daily 00:00 UTC boundary.
///
/// # Example
///
/// ```
/// use rustrade_risk::{SessionPnl, SessionPnlConfig};
///
/// let mut pnl = SessionPnl::new("XBTUSDTM", SessionPnlConfig {
///     loss_limit: -100.0,
/// });
///
/// pnl.record_close(20.0, 1.5);  // gross +20, fee 1.5 → net +18.5
/// pnl.record_close(-50.0, 1.5); // gross -50, fee 1.5 → net -51.5
/// pnl.record_close(-80.0, 1.5); // running net = -113.0 → halts (≤ -100)
///
/// assert!(pnl.is_session_halted());
/// ```
#[derive(Debug, Clone)]
pub struct SessionPnl {
    /// Symbol this PnL tracker is for (used in log records).
    pub symbol: String,
    /// Cumulative gross realised PnL, in quote currency.
    pub realised: f64,
    /// Cumulative fees paid this session, in quote currency.
    pub fees: f64,
    /// Total trades recorded this session.
    pub trades: u32,
    /// Wins (net PnL > 0).
    pub wins: u32,
    /// Losses (net PnL < 0).
    pub losses: u32,
    /// Break-evens (net PnL == 0).
    pub breakevens: u32,
    config: SessionPnlConfig,
    halted: bool,
    /// UTC day number (days since epoch) of the last reset. Used to detect
    /// rollover; the `tick` method is the only place this is updated.
    last_reset_day: u64,
    clock: Arc<dyn Clock>,
}

impl SessionPnl {
    /// Create with the default system clock.
    pub fn new(symbol: impl Into<String>, config: SessionPnlConfig) -> Self {
        Self::with_clock(symbol, config, Arc::new(SystemClock))
    }

    /// Create with an injected clock — typically `Arc<ManualClock>` from
    /// [`crate::clock`] in tests.
    pub fn with_clock(
        symbol: impl Into<String>,
        config: SessionPnlConfig,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let last_reset_day = clock.utc_day_number();
        Self {
            symbol: symbol.into(),
            realised: 0.0,
            fees: 0.0,
            trades: 0,
            wins: 0,
            losses: 0,
            breakevens: 0,
            config,
            halted: false,
            last_reset_day,
            clock,
        }
    }

    /// Net PnL after fees.
    pub fn net_pnl(&self) -> f64 {
        self.realised - self.fees
    }

    /// Win rate over decided trades (excludes break-evens).
    pub fn win_rate(&self) -> f64 {
        let decided = self.wins + self.losses;
        if decided == 0 {
            0.0
        } else {
            f64::from(self.wins) / f64::from(decided)
        }
    }

    /// Is the session currently halted by the loss cap?
    pub fn is_session_halted(&self) -> bool {
        self.halted
    }

    /// Record a closed trade.
    ///
    /// `gross_pnl` is the realised PnL before fees (negative for losses).
    /// `fee` is the round-trip fee charged for opening + closing the
    /// position. The trade is classified as W/L/B on **net** PnL so that
    /// fee-flipped trades (small gross win, large fee = real loss) count
    /// correctly.
    pub fn record_close(&mut self, gross_pnl: f64, fee: f64) {
        self.realised += gross_pnl;
        self.fees += fee;
        self.trades += 1;

        let net = gross_pnl - fee;
        if net > 0.0 {
            self.wins += 1;
        } else if net < 0.0 {
            self.losses += 1;
        } else {
            self.breakevens += 1;
        }

        tracing::info!(
            target: "pnl",
            symbol      = %self.symbol,
            trade       = self.trades,
            gross_usdt  = format!("{:.4}", gross_pnl),
            fee_usdt    = format!("{:.4}", fee),
            net_usdt    = format!("{:.4}", net),
            outcome     = if net > 0.0 { "WIN" } else if net < 0.0 { "LOSS" } else { "BREAKEVEN" },
            running_net = format!("{:.4}", self.net_pnl()),
            "trade closed",
        );

        if !self.halted && self.net_pnl() <= self.config.loss_limit {
            self.halted = true;
            tracing::warn!(
                target: "pnl",
                symbol  = %self.symbol,
                net_pnl = format!("{:.4}", self.net_pnl()),
                limit   = format!("{:.4}", self.config.loss_limit),
                "session loss limit breached — trading halted",
            );
        }
    }

    /// Call periodically (e.g. once per candle poll) to detect 00:00 UTC
    /// rollover and reset the session totals + halt flag.
    pub fn tick(&mut self) {
        let today = self.clock.utc_day_number();
        if today > self.last_reset_day {
            self.reset_session();
            self.last_reset_day = today;
        }
    }

    /// Capture the mutable session state for persistence.
    ///
    /// Pairs with [`Self::restore`]. The configured `loss_limit`, symbol,
    /// and clock are intentionally excluded — they belong to the live
    /// instance, not the snapshot.
    pub fn snapshot(&self) -> SessionPnlSnapshot {
        SessionPnlSnapshot {
            realised: self.realised,
            fees: self.fees,
            trades: self.trades,
            wins: self.wins,
            losses: self.losses,
            breakevens: self.breakevens,
            halted: self.halted,
            last_reset_day: self.last_reset_day,
        }
    }

    /// Restore session state from a [`SessionPnlSnapshot`].
    ///
    /// Overwrites the running totals, halt flag, and last-reset day; keeps
    /// the symbol, configured loss limit, and clock from the live instance.
    /// Call [`Self::tick`] afterwards so a snapshot taken on an earlier UTC
    /// day rolls over to a fresh session instead of resuming a stale halt.
    pub fn restore(&mut self, snap: SessionPnlSnapshot) {
        self.realised = snap.realised;
        self.fees = snap.fees;
        self.trades = snap.trades;
        self.wins = snap.wins;
        self.losses = snap.losses;
        self.breakevens = snap.breakevens;
        self.halted = snap.halted;
        self.last_reset_day = snap.last_reset_day;
    }

    /// Force a session reset. Normally called automatically by `tick` at
    /// the daily UTC rollover.
    pub fn reset_session(&mut self) {
        tracing::info!(
            target: "pnl",
            symbol = %self.symbol,
            trades = self.trades,
            net_usdt = format!("{:.4}", self.net_pnl()),
            "session reset — rolling over",
        );
        self.realised = 0.0;
        self.fees = 0.0;
        self.trades = 0;
        self.wins = 0;
        self.losses = 0;
        self.breakevens = 0;
        self.halted = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ManualClock;

    fn cfg(limit: f64) -> SessionPnlConfig {
        SessionPnlConfig { loss_limit: limit }
    }

    #[test]
    fn classifies_on_net_not_gross() {
        let mut p = SessionPnl::new("TEST", cfg(-1000.0));
        p.record_close(1.0, 3.0); // net = -2 → loss
        assert_eq!(p.losses, 1);
        assert_eq!(p.wins, 0);
    }

    #[test]
    fn halts_when_limit_breached() {
        let mut p = SessionPnl::new("TEST", cfg(-10.0));
        p.record_close(-5.0, 1.0); // net = -6, above limit
        assert!(!p.is_session_halted());
        p.record_close(-5.0, 1.0); // net = -12, below limit
        assert!(p.is_session_halted());
    }

    #[test]
    fn reset_clears_halt_and_totals() {
        let mut p = SessionPnl::new("TEST", cfg(-10.0));
        p.record_close(-20.0, 0.0);
        assert!(p.is_session_halted());
        p.reset_session();
        assert!(!p.is_session_halted());
        assert_eq!(p.trades, 0);
        assert!((p.net_pnl()).abs() < 1e-9);
    }

    #[test]
    fn win_rate_excludes_breakevens() {
        let mut p = SessionPnl::new("TEST", cfg(-1000.0));
        p.record_close(10.0, 1.0); // win
        p.record_close(-5.0, 1.0); // loss
        p.record_close(1.0, 1.0); // breakeven
        assert!((p.win_rate() - 0.5).abs() < 1e-9);
    }

    #[test]
    fn utc_rollover_resets_session_via_tick() {
        // Start at midnight UTC on day 100. Halt the session within the
        // day, advance past midnight, then verify tick() rolls over.
        let day = 100u64;
        let clock = Arc::new(ManualClock::new(day * 86_400));
        let mut p = SessionPnl::with_clock("TEST", cfg(-10.0), clock.clone());

        // Take a halting loss intra-day.
        clock.advance_secs(3_600); // +1 h, still day 100
        p.record_close(-20.0, 0.0);
        assert!(p.is_session_halted());
        assert_eq!(p.trades, 1);

        // Tick before midnight — no rollover yet.
        clock.advance_secs(3_600); // +1 h more, still day 100
        p.tick();
        assert!(
            p.is_session_halted(),
            "should still be halted before midnight"
        );

        // Cross midnight into day 101 and tick — should reset cleanly.
        clock.set((day + 1) * 86_400 + 5);
        p.tick();
        assert!(!p.is_session_halted(), "rollover must clear the halt");
        assert_eq!(p.trades, 0);
        assert!((p.net_pnl()).abs() < 1e-9);
    }

    #[test]
    fn tick_within_same_day_is_a_noop() {
        let clock = Arc::new(ManualClock::new(100 * 86_400 + 10));
        let mut p = SessionPnl::with_clock("TEST", cfg(-1000.0), clock.clone());

        p.record_close(5.0, 1.0); // net +4
        let before = p.net_pnl();

        clock.advance_secs(60 * 60 * 12); // +12 h, still day 100
        p.tick();
        assert!(
            (p.net_pnl() - before).abs() < 1e-9,
            "intra-day tick must not reset session totals"
        );
        assert_eq!(p.trades, 1);
    }

    #[test]
    fn snapshot_restore_roundtrips_state() {
        let mut p = SessionPnl::new("TEST", cfg(-100.0));
        p.record_close(10.0, 1.0); // win
        p.record_close(-30.0, 2.0); // loss
        let snap = p.snapshot();

        // Restore into a fresh instance (same config) and compare totals.
        let mut q = SessionPnl::new("TEST", cfg(-100.0));
        q.restore(snap.clone());
        assert_eq!(q.snapshot(), snap);
        assert!((q.net_pnl() - p.net_pnl()).abs() < 1e-9);
        assert_eq!(q.trades, 2);
        assert_eq!(q.wins, 1);
        assert_eq!(q.losses, 1);
    }

    #[test]
    fn restore_preserves_halt_within_same_day() {
        // Snapshot a halted session, restore on the *same* UTC day, tick:
        // the halt must survive (a restart must not reopen a halted day).
        let clock = Arc::new(ManualClock::new(200 * 86_400 + 100));
        let mut p = SessionPnl::with_clock("TEST", cfg(-10.0), clock.clone());
        p.record_close(-20.0, 0.0);
        assert!(p.is_session_halted());
        let snap = p.snapshot();

        let mut q = SessionPnl::with_clock("TEST", cfg(-10.0), clock.clone());
        q.restore(snap);
        q.tick(); // same day → no rollover
        assert!(
            q.is_session_halted(),
            "halt must survive a same-day restore"
        );
    }

    #[test]
    fn restore_then_tick_rolls_over_stale_day() {
        // Snapshot a halted session on day 300; restore on day 301. A
        // post-restore tick must roll the session over (fresh, un-halted)
        // so a day-old breaker doesn't wrongly block today's trading.
        let day = 300u64;
        let clock = Arc::new(ManualClock::new(day * 86_400 + 100));
        let mut p = SessionPnl::with_clock("TEST", cfg(-10.0), clock.clone());
        p.record_close(-50.0, 0.0);
        assert!(p.is_session_halted());
        let snap = p.snapshot();

        // Reboot "the next day".
        let next = Arc::new(ManualClock::new((day + 1) * 86_400 + 5));
        let mut q = SessionPnl::with_clock("TEST", cfg(-10.0), next);
        q.restore(snap);
        q.tick();
        assert!(!q.is_session_halted(), "stale day must roll over to fresh");
        assert_eq!(q.trades, 0);
        assert!(q.net_pnl().abs() < 1e-9);
    }
}
