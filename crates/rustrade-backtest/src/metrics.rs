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
    pub symbol: String,
    pub close_side: Side,
    pub qty: f64,
    pub entry_price: f64,
    pub exit_price: f64,
    pub gross_pnl: f64,
    pub fee: f64,
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
    Win,
    Loss,
    Breakeven,
}
