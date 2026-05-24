//! Per-session realised PnL tracker with optional drawdown cap and
//! daily 00:00 UTC rollover.
//!
//! Generalized from the `kucoin/bot/pnl.rs` shipped with the Apr 2026 bot.
//! Strategy code records every trade close via [`SessionPnl::record_close`];
//! the framework checks [`SessionPnl::is_session_halted`] before allowing
//! new entries.

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

fn utc_day_number() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() / 86_400)
        .unwrap_or(0)
}

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
        Self {
            loss_limit: -50.0,
        }
    }
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
    pub symbol: String,
    pub realised: f64,
    pub fees: f64,
    pub trades: u32,
    pub wins: u32,
    pub losses: u32,
    pub breakevens: u32,
    config: SessionPnlConfig,
    halted: bool,
    /// UTC day number (days since epoch) of the last reset. Used to detect
    /// rollover; the `tick` method is the only place this is updated.
    last_reset_day: u64,
}

impl SessionPnl {
    pub fn new(symbol: impl Into<String>, config: SessionPnlConfig) -> Self {
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
            last_reset_day: utc_day_number(),
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
        let today = utc_day_number();
        if today > self.last_reset_day {
            self.reset_session();
            self.last_reset_day = today;
        }
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
        p.record_close(1.0, 1.0);  // breakeven
        assert!((p.win_rate() - 0.5).abs() < 1e-9);
    }
}
