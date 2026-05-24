//! Notional-based position sizing.
//!
//! Generalized from `kucoin/bot/sizing.rs`. Computes the integer number of
//! contracts (or base-asset units) that corresponds to a desired margin
//! commitment at the current price and leverage:
//!
//! ```text
//! notional   = margin_usd × leverage
//! contracts  = floor(notional / (price × contract_value))
//! contracts  = min(contracts, max_contracts)
//! ```
//!
//! The `contract_value` is exchange- and symbol-specific (0.001 BTC for
//! XBTUSDTM, 0.01 ETH for ETHUSDTM, 1.0 SOL for SOLUSDTM). The framework
//! gets it from the `ExchangeClient` adapter — see the v0 trait extension
//! discussed in `kucoin-v2/DESIGN_NOTES.md`.

use serde::{Deserialize, Serialize};

/// Configuration for [`PositionSizer`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SizingConfig {
    /// Default margin to commit per trade in quote currency (e.g. USDT).
    pub margin_per_trade: f64,
    /// Leverage multiplier. Used to convert margin into notional.
    pub leverage: u32,
    /// Hard ceiling on contracts per trade — never exceeded regardless of
    /// what the formula returns.
    pub max_contracts: u32,
}

impl Default for SizingConfig {
    fn default() -> Self {
        Self {
            margin_per_trade: 500.0,
            leverage: 5,
            max_contracts: 50,
        }
    }
}

/// Computes order sizes from margin + leverage + price + contract multiplier.
///
/// Returns `0` if any input is non-positive or the resulting size rounds
/// down to zero. Callers should treat `0` as "skip this trade — too small".
pub struct PositionSizer {
    config: SizingConfig,
}

impl PositionSizer {
    pub fn new(config: SizingConfig) -> Self {
        Self { config }
    }

    /// Compute contract count for a trade.
    ///
    /// `price` is the current mark or last-trade price in quote currency.
    /// `contract_value` is the base-asset units per 1 contract (e.g. 0.001
    /// for XBTUSDTM).
    pub fn contracts(&self, price: f64, contract_value: f64) -> u32 {
        if price <= 0.0
            || contract_value <= 0.0
            || self.config.margin_per_trade <= 0.0
            || self.config.leverage == 0
        {
            return 0;
        }

        let notional = self.config.margin_per_trade * f64::from(self.config.leverage);
        let raw = (notional / (price * contract_value)).floor() as u32;
        raw.min(self.config.max_contracts)
    }

    /// Same as [`contracts`] but takes an explicit override for the
    /// per-trade margin (used by brains that want to scale up/down based
    /// on confidence or by the framework when honouring `SizeHint::NotionalUsd`).
    pub fn contracts_with_margin(&self, margin_usd: f64, price: f64, contract_value: f64) -> u32 {
        if price <= 0.0
            || contract_value <= 0.0
            || margin_usd <= 0.0
            || self.config.leverage == 0
        {
            return 0;
        }
        let notional = margin_usd * f64::from(self.config.leverage);
        let raw = (notional / (price * contract_value)).floor() as u32;
        raw.min(self.config.max_contracts)
    }

    pub fn config(&self) -> &SizingConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sizer(margin: f64, lev: u32, max: u32) -> PositionSizer {
        PositionSizer::new(SizingConfig {
            margin_per_trade: margin,
            leverage: lev,
            max_contracts: max,
        })
    }

    #[test]
    fn zero_price_returns_zero() {
        let s = sizer(500.0, 5, 100);
        assert_eq!(s.contracts(0.0, 0.001), 0);
    }

    #[test]
    fn zero_leverage_returns_zero() {
        let s = sizer(500.0, 0, 100);
        assert_eq!(s.contracts(50_000.0, 0.001), 0);
    }

    #[test]
    fn btc_known_value() {
        // notional = 500 × 5 = 2500
        // per-contract = 50000 × 0.001 = 50
        // contracts = floor(2500 / 50) = 50
        let s = sizer(500.0, 5, 100);
        assert_eq!(s.contracts(50_000.0, 0.001), 50);
    }

    #[test]
    fn cap_is_respected() {
        // Massive margin × leverage at low price would otherwise blow past cap.
        let s = sizer(500_000.0, 100, 10);
        assert_eq!(s.contracts(1.0, 0.001), 10);
    }

    #[test]
    fn rounds_to_zero_when_price_too_high() {
        // notional = 1, per-contract = 1000 → floor(1/1000) = 0
        let s = sizer(1.0, 1, 50);
        assert_eq!(s.contracts(1_000_000.0, 0.001), 0);
    }
}
