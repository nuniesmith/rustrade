//! Trade-level outcome captured during the replay loop.
//!
//! [`BacktestResult`](crate::BacktestResult) aggregates these into
//! summary statistics. Closing more than one position per trade (e.g.
//! flipping from long to short) emits multiple [`TradeOutcome`]s — one
//! per closed quantity.

use chrono::{DateTime, Utc};
use rustrade_core::Side;
use serde::{Deserialize, Serialize};

/// A single realised trade — open → close — recorded by the engine.
///
/// `gross_pnl` is in quote currency, before `fee`. `net_pnl = gross_pnl
/// - fee`. The "side" is the side of the *closing* fill (so a long
/// position is closed with `Side::Sell`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeOutcome {
    /// Symbol the trade was on.
    pub symbol: String,
    /// Side of the *closing* fill (`Sell` to close a long, etc.).
    pub close_side: Side,
    /// Closed quantity.
    pub qty: f64,
    /// Average entry price of the closed quantity.
    pub entry_price: f64,
    /// Fill price of the close.
    pub exit_price: f64,
    /// Gross PnL on the closed quantity, in quote currency, before fees.
    pub gross_pnl: f64,
    /// Fee charged to this close, in quote currency.
    pub fee: f64,
    /// When the close fill occurred.
    pub closed_at: DateTime<Utc>,
}

impl TradeOutcome {
    /// Net of fees.
    pub fn net_pnl(&self) -> f64 {
        self.gross_pnl - self.fee
    }

    /// Win (`true`) / loss / breakeven on net PnL.
    pub fn outcome(&self) -> Outcome {
        let n = self.net_pnl();
        if n > 0.0 {
            Outcome::Win
        } else if n < 0.0 {
            Outcome::Loss
        } else {
            Outcome::Breakeven
        }
    }
}

/// Trade classification on net PnL — used by metric aggregation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Net PnL > 0.
    Win,
    /// Net PnL < 0.
    Loss,
    /// Net PnL == 0.
    Breakeven,
}
