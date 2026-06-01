//! Account-level (portfolio) risk.
//!
//! The per-symbol [`SessionPnl`](crate::SessionPnl) and
//! [`CircuitBreaker`](crate::CircuitBreaker) bound each symbol in isolation.
//! [`PortfolioRisk`] bounds the **whole account**, which is what multi-asset
//! trading needs: a single bad day across many symbols, or too much aggregate
//! exposure, should stop new risk even when no individual symbol has tripped.
//!
//! It provides two things:
//!
//! 1. An **account-wide daily-loss halt** — when net realised PnL summed
//!    across all symbols breaches the limit, every new entry is halted until
//!    the next 00:00 UTC rollover. The halt **latches** ([`PortfolioRisk::observe`]):
//!    once breached it stays halted for the day even if a later realised win
//!    nudges the sum back above the limit.
//! 2. A **pre-trade entry gate** ([`PortfolioRisk::check_entry`]) over aggregate
//!    state: the daily-loss halt, max concurrent open positions, and a
//!    gross-exposure cap.
//!
//! The account net PnL is **derived** from the per-symbol session PnLs by the
//! framework rather than bookkept separately here — that keeps a single source
//! of truth and avoids drift. The framework computes the sum (in its periodic
//! risk sweep and in the pre-trade gate) and hands it in via [`PortfolioRisk::observe`]
//! and [`PortfolioState::account_net_pnl`].
//!
//! Every limit defaults to **off** ([`PortfolioRiskConfig::default`]), so a bot
//! that doesn't configure portfolio risk behaves exactly as before — it's
//! purely additive and opt-in.
//!
//! Time is read through the [`Clock`] trait so tests advance the clock instead
//! of sleeping.

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::clock::{Clock, SystemClock};

/// Configuration for [`PortfolioRisk`]. Every limit is independently
/// disable-able, and the [`Default`] disables them all (opt-in).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioRiskConfig {
    /// Account-wide net loss ceiling for the day, in quote currency (a
    /// negative number, e.g. `-500.0`). When net realised PnL across all
    /// symbols drops to or below this, **every** new entry is halted until
    /// the next 00:00 UTC rollover. `f64::NEG_INFINITY` disables the halt.
    pub max_daily_loss: f64,
    /// Maximum number of symbols holding a position at once. A new entry on a
    /// symbol that is currently flat is blocked when this many symbols are
    /// already open. `0` means unlimited.
    pub max_concurrent_positions: u32,
    /// Cap on aggregate **gross** exposure (the sum of `|notional|` across all
    /// open positions) in quote currency. A new entry is blocked when it would
    /// push gross exposure past this. `f64::INFINITY` disables the cap.
    pub max_gross_exposure: f64,
}

impl Default for PortfolioRiskConfig {
    fn default() -> Self {
        // All-off: a bot that doesn't opt in is unaffected.
        Self {
            max_daily_loss: f64::NEG_INFINITY,
            max_concurrent_positions: 0,
            max_gross_exposure: f64::INFINITY,
        }
    }
}

/// Why [`PortfolioRisk::check_entry`] blocked a new entry.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PortfolioBlock {
    /// The account-wide daily loss limit is breached; halted until UTC rollover.
    DailyLossHalt {
        /// Net realised PnL across the account.
        net_pnl: f64,
        /// The configured ceiling.
        limit: f64,
    },
    /// Already at the maximum number of concurrent open positions.
    MaxConcurrentPositions {
        /// How many symbols are currently open.
        open: u32,
        /// The configured ceiling.
        limit: u32,
    },
    /// Adding this position would exceed the gross-exposure cap.
    GrossExposureCap {
        /// Gross exposure already on the book.
        current: f64,
        /// Notional this entry would add.
        additional: f64,
        /// The configured ceiling.
        limit: f64,
    },
}

impl fmt::Display for PortfolioBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DailyLossHalt { net_pnl, limit } => write!(
                f,
                "account daily-loss halt (net {net_pnl:.2} ≤ limit {limit:.2})"
            ),
            Self::MaxConcurrentPositions { open, limit } => write!(
                f,
                "max concurrent positions reached ({open} open ≥ limit {limit})"
            ),
            Self::GrossExposureCap {
                current,
                additional,
                limit,
            } => write!(
                f,
                "gross-exposure cap (current {current:.2} + {additional:.2} > limit {limit:.2})"
            ),
        }
    }
}

/// Aggregate account state at the moment of a pre-trade check, assembled by
/// the framework from its per-symbol risk + position state.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PortfolioState {
    /// Number of symbols currently holding a non-flat position.
    pub open_positions: u32,
    /// Aggregate gross exposure already on the book (quote currency).
    pub gross_exposure: f64,
    /// Notional the proposed new entry would add (quote currency).
    pub new_notional: f64,
    /// Whether the entry's symbol already holds a position — if so the entry
    /// adds to an existing slot rather than consuming a new concurrency slot.
    pub symbol_already_open: bool,
    /// Account-wide **net realised** PnL (sum of every symbol's session net
    /// PnL). Checked live so a fresh breach blocks immediately, independent of
    /// the latched halt.
    pub account_net_pnl: f64,
}

/// Restart-durable snapshot of [`PortfolioRisk`]'s latch state.
///
/// The account PnL itself is derived from the per-symbol session snapshots, so
/// only the halt latch + rollover day are persisted here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortfolioRiskSnapshot {
    /// Whether the account was halted by the daily-loss limit when snapshotted.
    pub halted: bool,
    /// UTC day number of the last reset, for rollover detection on restore.
    pub last_reset_day: u64,
}

/// Account-wide risk: a latching daily-loss halt plus a pre-trade entry gate
/// over aggregate exposure and concurrency. See the [module docs](self).
///
/// # Example
///
/// ```
/// use rustrade_risk::{PortfolioRisk, PortfolioRiskConfig, PortfolioState, PortfolioBlock};
///
/// let mut pf = PortfolioRisk::new(PortfolioRiskConfig {
///     max_daily_loss: -100.0,
///     max_concurrent_positions: 2,
///     max_gross_exposure: 10_000.0,
/// });
///
/// // A fresh entry that fits every limit is allowed.
/// assert!(pf.check_entry(PortfolioState {
///     open_positions: 1,
///     gross_exposure: 3_000.0,
///     new_notional: 2_000.0,
///     symbol_already_open: false,
///     account_net_pnl: -10.0,
/// }).is_ok());
///
/// // Once the account net PnL breaches the limit the whole account halts.
/// pf.observe(-120.0);
/// assert!(pf.is_halted());
/// assert!(matches!(
///     pf.check_entry(PortfolioState {
///         open_positions: 0, gross_exposure: 0.0, new_notional: 1.0,
///         symbol_already_open: false, account_net_pnl: -120.0,
///     }),
///     Err(PortfolioBlock::DailyLossHalt { .. })
/// ));
/// ```
#[derive(Debug, Clone)]
pub struct PortfolioRisk {
    config: PortfolioRiskConfig,
    halted: bool,
    last_reset_day: u64,
    clock: Arc<dyn Clock>,
}

impl PortfolioRisk {
    /// Create with the default system clock.
    pub fn new(config: PortfolioRiskConfig) -> Self {
        Self::with_clock(config, Arc::new(SystemClock))
    }

    /// Create with an injected clock — typically `Arc<ManualClock>` in tests.
    pub fn with_clock(config: PortfolioRiskConfig, clock: Arc<dyn Clock>) -> Self {
        let last_reset_day = clock.utc_day_number();
        Self {
            config,
            halted: false,
            last_reset_day,
            clock,
        }
    }

    /// Is the account currently halted by the daily-loss limit?
    pub fn is_halted(&self) -> bool {
        self.halted
    }

    /// Borrow the configuration.
    pub fn config(&self) -> &PortfolioRiskConfig {
        &self.config
    }

    /// Observe the account-wide net realised PnL (the sum of every symbol's
    /// session net PnL). Latches the daily-loss halt once it breaches the
    /// limit; the latch is sticky until the next UTC rollover ([`Self::tick`]).
    /// The framework calls this from its periodic risk sweep.
    pub fn observe(&mut self, account_net_pnl: f64) {
        if !self.halted && account_net_pnl <= self.config.max_daily_loss {
            self.halted = true;
            tracing::warn!(
                target: "portfolio",
                net_pnl = format!("{:.4}", account_net_pnl),
                limit = format!("{:.4}", self.config.max_daily_loss),
                "account daily-loss limit breached — all new entries halted",
            );
        }
    }

    /// Pre-trade gate for a **new entry**. Returns `Err` with the binding
    /// reason if any account-level limit blocks it. Exits / reduce-only orders
    /// should not be gated by this — only entries that add risk.
    ///
    /// The daily-loss check fires on either the latched halt or a live breach
    /// in `state.account_net_pnl`, so a fresh breach blocks immediately even
    /// between sweeps.
    pub fn check_entry(&self, state: PortfolioState) -> Result<(), PortfolioBlock> {
        if self.halted || state.account_net_pnl <= self.config.max_daily_loss {
            return Err(PortfolioBlock::DailyLossHalt {
                net_pnl: state.account_net_pnl,
                limit: self.config.max_daily_loss,
            });
        }

        if self.config.max_concurrent_positions > 0
            && !state.symbol_already_open
            && state.open_positions >= self.config.max_concurrent_positions
        {
            return Err(PortfolioBlock::MaxConcurrentPositions {
                open: state.open_positions,
                limit: self.config.max_concurrent_positions,
            });
        }

        if state.gross_exposure + state.new_notional > self.config.max_gross_exposure {
            return Err(PortfolioBlock::GrossExposureCap {
                current: state.gross_exposure,
                additional: state.new_notional,
                limit: self.config.max_gross_exposure,
            });
        }

        Ok(())
    }

    /// Call periodically to detect the 00:00 UTC rollover and clear the halt
    /// latch. Mirrors [`SessionPnl::tick`](crate::SessionPnl::tick).
    pub fn tick(&mut self) {
        let today = self.clock.utc_day_number();
        if today > self.last_reset_day {
            self.reset_session();
            self.last_reset_day = today;
        }
    }

    /// Force a session reset (normally automatic at the UTC rollover).
    pub fn reset_session(&mut self) {
        if self.halted {
            tracing::info!(target: "portfolio", "account session reset — daily-loss halt cleared");
        }
        self.halted = false;
    }

    /// Capture the latch state for persistence.
    pub fn snapshot(&self) -> PortfolioRiskSnapshot {
        PortfolioRiskSnapshot {
            halted: self.halted,
            last_reset_day: self.last_reset_day,
        }
    }

    /// Restore latch state from a snapshot, keeping the live config + clock.
    /// Call [`Self::tick`] afterwards so a snapshot from an earlier UTC day
    /// rolls over instead of resuming a stale halt.
    pub fn restore(&mut self, snap: PortfolioRiskSnapshot) {
        self.halted = snap.halted;
        self.last_reset_day = snap.last_reset_day;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ManualClock;

    fn cfg(loss: f64, max_pos: u32, max_gross: f64) -> PortfolioRiskConfig {
        PortfolioRiskConfig {
            max_daily_loss: loss,
            max_concurrent_positions: max_pos,
            max_gross_exposure: max_gross,
        }
    }

    fn state(open: u32, gross: f64, new: f64, already: bool, net: f64) -> PortfolioState {
        PortfolioState {
            open_positions: open,
            gross_exposure: gross,
            new_notional: new,
            symbol_already_open: already,
            account_net_pnl: net,
        }
    }

    #[test]
    fn default_config_blocks_nothing() {
        let pf = PortfolioRisk::new(PortfolioRiskConfig::default());
        // Huge exposure, many positions, big loss — all allowed when off.
        assert!(
            pf.check_entry(state(1_000, 1e12, 1e12, false, -1e12))
                .is_ok()
        );
    }

    #[test]
    fn live_daily_loss_blocks_even_before_latch() {
        let pf = PortfolioRisk::new(cfg(-100.0, 0, f64::INFINITY));
        // Not latched yet, but the live net is already past the limit.
        assert!(matches!(
            pf.check_entry(state(0, 0.0, 1.0, false, -150.0)),
            Err(PortfolioBlock::DailyLossHalt { .. })
        ));
        // Comfortably above the limit → allowed.
        assert!(pf.check_entry(state(0, 0.0, 1.0, false, -50.0)).is_ok());
    }

    #[test]
    fn observe_latches_sticky_halt() {
        let mut pf = PortfolioRisk::new(cfg(-100.0, 0, f64::INFINITY));
        pf.observe(-60.0);
        assert!(!pf.is_halted());
        pf.observe(-120.0); // breach → latch
        assert!(pf.is_halted());
        // Even if PnL recovers above the limit, the latch holds for the day.
        assert!(matches!(
            pf.check_entry(state(0, 0.0, 1.0, false, 50.0)),
            Err(PortfolioBlock::DailyLossHalt { .. })
        ));
    }

    #[test]
    fn max_concurrent_blocks_new_symbol_only() {
        let pf = PortfolioRisk::new(cfg(f64::NEG_INFINITY, 2, f64::INFINITY));
        assert!(matches!(
            pf.check_entry(state(2, 0.0, 1.0, false, 0.0)),
            Err(PortfolioBlock::MaxConcurrentPositions { open: 2, limit: 2 })
        ));
        // Adding to an already-open symbol consumes no new slot.
        assert!(pf.check_entry(state(2, 0.0, 1.0, true, 0.0)).is_ok());
        // Below the cap a new symbol is fine.
        assert!(pf.check_entry(state(1, 0.0, 1.0, false, 0.0)).is_ok());
    }

    #[test]
    fn gross_exposure_cap_blocks_when_exceeded() {
        let pf = PortfolioRisk::new(cfg(f64::NEG_INFINITY, 0, 10_000.0));
        assert!(
            pf.check_entry(state(1, 8_000.0, 1_500.0, false, 0.0))
                .is_ok()
        );
        assert!(matches!(
            pf.check_entry(state(1, 8_000.0, 2_500.0, false, 0.0)),
            Err(PortfolioBlock::GrossExposureCap { .. })
        ));
    }

    #[test]
    fn halt_takes_precedence_over_other_gates() {
        let pf = PortfolioRisk::new(cfg(-10.0, 1, 100.0));
        // Live breach blocks even an entry that fits concurrency + exposure.
        assert!(matches!(
            pf.check_entry(state(0, 0.0, 1.0, false, -20.0)),
            Err(PortfolioBlock::DailyLossHalt { .. })
        ));
    }

    #[test]
    fn utc_rollover_clears_latch_via_tick() {
        let day = 100u64;
        let clock = Arc::new(ManualClock::new(day * 86_400));
        let mut pf = PortfolioRisk::with_clock(cfg(-10.0, 0, f64::INFINITY), clock.clone());
        pf.observe(-20.0);
        assert!(pf.is_halted());

        clock.advance_secs(3_600);
        pf.tick(); // same day → still halted
        assert!(pf.is_halted());

        clock.set((day + 1) * 86_400 + 5);
        pf.tick(); // next UTC day → cleared
        assert!(!pf.is_halted());
    }

    #[test]
    fn snapshot_restore_preserves_latch_same_day() {
        let clock = Arc::new(ManualClock::new(200 * 86_400 + 100));
        let mut pf = PortfolioRisk::with_clock(cfg(-10.0, 0, f64::INFINITY), clock.clone());
        pf.observe(-20.0);
        let snap = pf.snapshot();

        let mut q = PortfolioRisk::with_clock(cfg(-10.0, 0, f64::INFINITY), clock.clone());
        q.restore(snap.clone());
        assert_eq!(q.snapshot(), snap);
        q.tick(); // same day → latch survives
        assert!(q.is_halted());
    }

    #[test]
    fn restore_then_tick_rolls_over_stale_day() {
        let day = 300u64;
        let clock = Arc::new(ManualClock::new(day * 86_400 + 100));
        let mut pf = PortfolioRisk::with_clock(cfg(-10.0, 0, f64::INFINITY), clock);
        pf.observe(-50.0);
        let snap = pf.snapshot();

        let next = Arc::new(ManualClock::new((day + 1) * 86_400 + 5));
        let mut q = PortfolioRisk::with_clock(cfg(-10.0, 0, f64::INFINITY), next);
        q.restore(snap);
        q.tick();
        assert!(!q.is_halted(), "stale halted day must roll over fresh");
    }
}
