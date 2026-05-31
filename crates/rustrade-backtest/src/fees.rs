//! Fee models applied to every simulated fill in the backtest engine.
//!
//! Fees are charged against the trade's *notional* (fill price × size).
//! For Phase 4a the maker/taker distinction is exposed but every
//! market order is treated as taker — limit orders aren't simulated yet.

use serde::{Deserialize, Serialize};

/// Pluggable fee schedule.
///
/// # Example
///
/// ```
/// use rustrade_backtest::FeeModel;
///
/// // 5 bps flat fee.
/// let f = FeeModel::Flat(0.0005);
/// assert!((f.fee_for(100.0, 10.0, true) - 0.5).abs() < 1e-9);
///
/// // Different maker / taker rates.
/// let mt = FeeModel::MakerTaker { maker: 0.0002, taker: 0.0006 };
/// assert!((mt.fee_for(100.0, 1.0, true) - 0.06).abs() < 1e-9);
/// assert!((mt.fee_for(100.0, 1.0, false) - 0.02).abs() < 1e-9);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum FeeModel {
    /// Zero fees — useful for sanity-checking PnL against a pure
    /// price-difference benchmark.
    Zero,
    /// Flat rate applied to every fill, as a fraction of notional.
    /// `0.001` = 10 bps = 0.1%.
    Flat(f64),
    /// Different rates for maker vs taker fills. The engine charges the
    /// taker rate for market / IOC / FOK orders and closes, and the maker
    /// rate for limit / post-only orders that rest before filling.
    MakerTaker {
        /// Fraction-of-notional rate when the fill is a maker.
        maker: f64,
        /// Fraction-of-notional rate when the fill is a taker.
        taker: f64,
    },
}

impl Default for FeeModel {
    fn default() -> Self {
        // Sensible crypto-futures-ish default: 5 bps round trip.
        Self::Flat(0.0005)
    }
}

impl FeeModel {
    /// Compute the fee in quote currency for a fill of `size` units at
    /// `fill_price`. The boolean `is_taker` is ignored unless the model
    /// is `MakerTaker`.
    pub fn fee_for(self, fill_price: f64, size: f64, is_taker: bool) -> f64 {
        let notional = fill_price * size;
        match self {
            Self::Zero => 0.0,
            Self::Flat(rate) => notional * rate,
            Self::MakerTaker { maker, taker } => notional * if is_taker { taker } else { maker },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_returns_zero() {
        assert_eq!(FeeModel::Zero.fee_for(100.0, 1.0, true), 0.0);
    }

    #[test]
    fn flat_proportional_to_notional() {
        let f = FeeModel::Flat(0.001);
        assert!((f.fee_for(100.0, 1.0, true) - 0.1).abs() < 1e-9);
        assert!((f.fee_for(100.0, 2.0, true) - 0.2).abs() < 1e-9);
        assert!((f.fee_for(50.0, 4.0, true) - 0.2).abs() < 1e-9);
    }

    #[test]
    fn maker_taker_distinguishes() {
        let f = FeeModel::MakerTaker {
            maker: 0.0002,
            taker: 0.0006,
        };
        let maker = f.fee_for(100.0, 1.0, false);
        let taker = f.fee_for(100.0, 1.0, true);
        assert!((maker - 0.02).abs() < 1e-9);
        assert!((taker - 0.06).abs() < 1e-9);
    }
}
