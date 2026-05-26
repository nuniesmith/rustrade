//! Slippage models applied between a brain's signal and the simulated
//! fill price.
//!
//! Slippage is modelled as a *price adjustment*: the brain decides at a
//! reference price (the candle's `close`), the engine adjusts that price
//! against the side of the trade, and the resulting price becomes the
//! fill price. Buys fill higher than the reference; sells fill lower.

use rustrade_core::Side;
use serde::{Deserialize, Serialize};

/// Slippage policy for the backtest engine.
///
/// Pluggable: future variants can include book-walk slippage (which
/// needs an order-book replay, not just candles) or per-symbol overrides.
///
/// # Example
///
/// ```
/// use rustrade_backtest::SlippageModel;
/// use rustrade_core::Side;
///
/// // 5 bps adverse slippage applied symmetrically.
/// let m = SlippageModel::FixedBps(5.0);
/// let buy = m.apply(Side::Buy, 100.0);
/// let sell = m.apply(Side::Sell, 100.0);
/// assert!((buy - 100.05).abs() < 1e-9);
/// assert!((sell - 99.95).abs() < 1e-9);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub enum SlippageModel {
    /// No slippage — fills land exactly at the reference price.
    #[default]
    Zero,
    /// Fixed slippage in basis points applied symmetrically against the
    /// trade side. `5.0` bps = 0.05% adverse slippage.
    FixedBps(f64),
}

impl SlippageModel {
    /// Adjust `reference_price` for an order of the given side.
    /// Returns the simulated fill price.
    pub fn apply(self, side: Side, reference_price: f64) -> f64 {
        match self {
            Self::Zero => reference_price,
            Self::FixedBps(bps) => {
                let factor = 1.0 + (bps / 10_000.0) * direction_sign(side);
                reference_price * factor
            }
        }
    }
}

/// `+1` for buys (slippage adds to fill price), `-1` for sells
/// (slippage subtracts).
fn direction_sign(side: Side) -> f64 {
    match side {
        Side::Buy => 1.0,
        Side::Sell => -1.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_returns_reference() {
        assert_eq!(SlippageModel::Zero.apply(Side::Buy, 100.0), 100.0);
        assert_eq!(SlippageModel::Zero.apply(Side::Sell, 100.0), 100.0);
    }

    #[test]
    fn fixed_bps_buy_adds() {
        let fill = SlippageModel::FixedBps(10.0).apply(Side::Buy, 100.0);
        // 10 bps = 0.1% → 100 * 1.001 = 100.1
        assert!((fill - 100.1).abs() < 1e-9);
    }

    #[test]
    fn fixed_bps_sell_subtracts() {
        let fill = SlippageModel::FixedBps(10.0).apply(Side::Sell, 100.0);
        assert!((fill - 99.9).abs() < 1e-9);
    }

    #[test]
    fn fixed_bps_symmetric_about_reference() {
        let buy = SlippageModel::FixedBps(25.0).apply(Side::Buy, 1_000.0);
        let sell = SlippageModel::FixedBps(25.0).apply(Side::Sell, 1_000.0);
        assert!(((buy - 1_000.0) + (sell - 1_000.0)).abs() < 1e-9);
    }
}
