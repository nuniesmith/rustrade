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
    /// Construct a sizer with the given [`SizingConfig`].
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

    /// Same as [`Self::contracts`] but takes an explicit override for the
    /// per-trade margin (used by brains that want to scale up/down based
    /// on confidence or by the framework when honouring `SizeHint::NotionalUsd`).
    pub fn contracts_with_margin(&self, margin_usd: f64, price: f64, contract_value: f64) -> u32 {
        if price <= 0.0 || contract_value <= 0.0 || margin_usd <= 0.0 || self.config.leverage == 0 {
            return 0;
        }
        let notional = margin_usd * f64::from(self.config.leverage);
        let raw = (notional / (price * contract_value)).floor() as u32;
        raw.min(self.config.max_contracts)
    }

    /// Borrow the sizer's underlying config.
    pub fn config(&self) -> &SizingConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

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

    // ── Property tests ─────────────────────────────────────────────────
    //
    // Bounds chosen to stay inside ranges that are realistic for crypto
    // futures (prices up to ~1M, leverage up to 200x, margins up to ~1M)
    // *and* to keep the unsaturated formula well inside `u32::MAX`.

    proptest! {
        /// Sizing must never exceed the configured cap.
        #[test]
        fn contracts_never_exceeds_cap(
            margin in 0.0_f64..1_000_000.0,
            leverage in 0_u32..200,
            max in 0_u32..10_000,
            price in 0.0_f64..1_000_000.0,
            contract_value in 0.0_f64..100.0,
        ) {
            let s = sizer(margin, leverage, max);
            prop_assert!(s.contracts(price, contract_value) <= max);
        }

        /// Any degenerate non-positive input forces a zero return.
        #[test]
        fn degenerate_inputs_return_zero(
            margin in proptest::sample::select(vec![0.0, -1.0, -1_000.0]),
            leverage in 0_u32..50,
            price in 1.0_f64..100_000.0,
            contract_value in 0.001_f64..1.0,
        ) {
            let s = sizer(margin, leverage, 1_000);
            prop_assert_eq!(s.contracts(price, contract_value), 0);
        }

        #[test]
        fn zero_or_negative_price_returns_zero(
            price in proptest::sample::select(vec![0.0, -1.0, -50_000.0]),
            margin in 1.0_f64..10_000.0,
            leverage in 1_u32..50,
            contract_value in 0.001_f64..1.0,
        ) {
            let s = sizer(margin, leverage, 1_000);
            prop_assert_eq!(s.contracts(price, contract_value), 0);
        }

        #[test]
        fn zero_or_negative_contract_value_returns_zero(
            cv in proptest::sample::select(vec![0.0, -0.001, -1.0]),
            margin in 1.0_f64..10_000.0,
            leverage in 1_u32..50,
            price in 1.0_f64..100_000.0,
        ) {
            let s = sizer(margin, leverage, 1_000);
            prop_assert_eq!(s.contracts(price, cv), 0);
        }

        /// Doubling margin can only ever produce >= the original count
        /// (subject to the cap). I.e. the sizer is monotone in margin.
        #[test]
        fn monotone_in_margin(
            margin in 1.0_f64..10_000.0,
            leverage in 1_u32..50,
            price in 10.0_f64..50_000.0,
            contract_value in 0.001_f64..1.0,
        ) {
            // High cap so the cap itself doesn't mask the property.
            let s_low  = sizer(margin,        leverage, u32::MAX);
            let s_high = sizer(margin * 2.0,  leverage, u32::MAX);
            let c_low  = s_low.contracts(price, contract_value);
            let c_high = s_high.contracts(price, contract_value);
            prop_assert!(
                c_high >= c_low,
                "expected monotone in margin: low={c_low} high={c_high}"
            );
        }

        /// Same property in leverage.
        #[test]
        fn monotone_in_leverage(
            margin in 1.0_f64..10_000.0,
            leverage in 1_u32..50,
            price in 10.0_f64..50_000.0,
            contract_value in 0.001_f64..1.0,
        ) {
            let s_low  = sizer(margin, leverage,     u32::MAX);
            let s_high = sizer(margin, leverage * 2, u32::MAX);
            let c_low  = s_low.contracts(price, contract_value);
            let c_high = s_high.contracts(price, contract_value);
            prop_assert!(
                c_high >= c_low,
                "expected monotone in leverage: low={c_low} high={c_high}"
            );
        }

        /// The formula matches: contracts = floor(margin·leverage / (price·cv))
        /// capped by max_contracts. Verify against an independently computed
        /// reference for inputs that stay inside f64 → u32 safety.
        #[test]
        fn matches_reference_formula(
            margin in 1.0_f64..100_000.0,
            leverage in 1_u32..100,
            max in 1_u32..1_000_000,
            price in 1.0_f64..50_000.0,
            contract_value in 0.001_f64..10.0,
        ) {
            let s = sizer(margin, leverage, max);
            let got = s.contracts(price, contract_value);

            let notional = margin * f64::from(leverage);
            let per_contract = price * contract_value;
            let raw = (notional / per_contract).floor();
            let expected = if raw < 0.0 || !raw.is_finite() {
                0
            } else {
                (raw as u32).min(max)
            };
            prop_assert_eq!(got, expected);
        }
    }
}
