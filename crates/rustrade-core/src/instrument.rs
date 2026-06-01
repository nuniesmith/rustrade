//! Instrument metadata — the exchange/asset-specific knobs the framework needs
//! to size and round orders correctly, and to apply asset-class-aware rules.
//!
//! [`InstrumentSpec`] is returned by
//! [`ExchangeClient::instrument_spec`](crate::ExchangeClient::instrument_spec).
//! It generalises the older single `contract_value` hook into the full set a
//! multi-asset bot needs: contract size, price tick, quantity lot, minimum
//! order notional, and a broad [`AssetClass`]. The defaults are permissive so
//! adapters that expose no metadata keep working unchanged.

use serde::{Deserialize, Serialize};

/// Broad asset class an instrument belongs to. Drives class-aware risk presets
/// (different leverage/stop/size conventions per class) and sizing semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AssetClass {
    /// Perpetual / dated crypto futures — contract-based (the default for the
    /// venues the framework targets first).
    #[default]
    CryptoPerp,
    /// Crypto spot — one unit traded equals one base-asset unit.
    CryptoSpot,
    /// Foreign exchange.
    Fx,
    /// Traditional dated futures (CME, etc.).
    Future,
    /// Cash equities.
    Equity,
    /// Anything else / unknown.
    Other,
}

/// Exchange/asset metadata for one instrument.
///
/// Used by the framework to round prices/quantities to the venue's increments,
/// enforce a minimum order notional, and apply class-aware rules. Adapters that
/// expose no metadata get a permissive default (see
/// [`ExchangeClient::instrument_spec`](crate::ExchangeClient::instrument_spec)),
/// which preserves today's behaviour.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct InstrumentSpec {
    /// Broad asset class.
    pub asset_class: AssetClass,
    /// Base-asset units per one contract (e.g. `0.001` BTC for `XBTUSDTM`);
    /// `1.0` for spot. Subsumes the older `ExchangeClient::contract_value`.
    pub contract_value: f64,
    /// Minimum price increment (tick). `0.0` ⇒ unknown / unconstrained.
    pub tick_size: f64,
    /// Minimum quantity increment (lot), in contracts. `0.0` ⇒ unknown;
    /// futures are typically whole contracts (`1.0`).
    pub lot_size: f64,
    /// Minimum order notional in quote currency. `0.0` ⇒ no minimum.
    pub min_notional: f64,
}

impl Default for InstrumentSpec {
    fn default() -> Self {
        Self::spot_default()
    }
}

impl InstrumentSpec {
    /// A permissive spot default: 1:1 contract value, no tick/lot/min-notional
    /// constraints. Appropriate when an adapter exposes no metadata.
    #[must_use]
    pub const fn spot_default() -> Self {
        Self {
            asset_class: AssetClass::CryptoSpot,
            contract_value: 1.0,
            tick_size: 0.0,
            lot_size: 0.0,
            min_notional: 0.0,
        }
    }

    /// A futures-contract spec from a known `contract_value`, with no other
    /// constraints. Backs the blanket [`ExchangeClient::instrument_spec`](crate::ExchangeClient::instrument_spec)
    /// default so an adapter that only overrides `contract_value` still gets a
    /// correct (if minimal) spec.
    #[must_use]
    pub const fn from_contract_value(contract_value: f64) -> Self {
        Self {
            asset_class: AssetClass::CryptoPerp,
            contract_value,
            tick_size: 0.0,
            lot_size: 0.0,
            min_notional: 0.0,
        }
    }

    /// Round a price to the nearest valid tick. No-op when `tick_size <= 0`.
    #[must_use]
    pub fn round_price(&self, price: f64) -> f64 {
        round_to_increment(price, self.tick_size)
    }

    /// Round a quantity **down** to the nearest valid lot — down so a sized
    /// order never exceeds the intended risk. No-op when `lot_size <= 0`.
    #[must_use]
    pub fn round_qty_down(&self, qty: f64) -> f64 {
        if self.lot_size <= 0.0 || !self.lot_size.is_finite() {
            return qty;
        }
        (qty / self.lot_size).floor() * self.lot_size
    }

    /// Does `notional` (quote currency) meet the minimum order notional?
    /// Always `true` when `min_notional` is `0.0`.
    #[must_use]
    pub fn meets_min_notional(&self, notional: f64) -> bool {
        notional >= self.min_notional
    }
}

/// Round `value` to the nearest multiple of `increment` (no-op if `increment <= 0`).
fn round_to_increment(value: f64, increment: f64) -> f64 {
    if increment <= 0.0 || !increment.is_finite() {
        return value;
    }
    (value / increment).round() * increment
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_permissive() {
        let s = InstrumentSpec::default();
        assert_eq!(s.contract_value, 1.0);
        assert_eq!(s.asset_class, AssetClass::CryptoSpot);
        // No constraints → every round is a no-op, every notional passes.
        assert_eq!(s.round_price(1234.5678), 1234.5678);
        assert_eq!(s.round_qty_down(3.7), 3.7);
        assert!(s.meets_min_notional(0.0));
    }

    #[test]
    fn from_contract_value_keeps_value_and_is_perp() {
        let s = InstrumentSpec::from_contract_value(0.001);
        assert_eq!(s.contract_value, 0.001);
        assert_eq!(s.asset_class, AssetClass::CryptoPerp);
    }

    #[test]
    fn round_price_snaps_to_tick() {
        let s = InstrumentSpec {
            tick_size: 0.5,
            ..InstrumentSpec::default()
        };
        assert_eq!(s.round_price(100.24), 100.0);
        assert_eq!(s.round_price(100.25), 100.5); // .round() rounds half up
        assert_eq!(s.round_price(100.74), 100.5);
        assert_eq!(s.round_price(100.75), 101.0);
    }

    #[test]
    fn round_qty_rounds_down_to_lot() {
        let s = InstrumentSpec {
            lot_size: 0.1,
            ..InstrumentSpec::default()
        };
        assert!((s.round_qty_down(3.79) - 3.7).abs() < 1e-9);
        assert!((s.round_qty_down(3.70) - 3.7).abs() < 1e-9);
        // Whole-contract lot.
        let c = InstrumentSpec {
            lot_size: 1.0,
            ..InstrumentSpec::default()
        };
        assert_eq!(c.round_qty_down(4.9), 4.0);
    }

    #[test]
    fn min_notional_gate() {
        let s = InstrumentSpec {
            min_notional: 10.0,
            ..InstrumentSpec::default()
        };
        assert!(!s.meets_min_notional(9.99));
        assert!(s.meets_min_notional(10.0));
        assert!(s.meets_min_notional(25.0));
    }

    #[test]
    fn asset_class_serdes_snake_case() {
        let json = serde_json::to_string(&AssetClass::CryptoPerp).unwrap();
        assert_eq!(json, "\"crypto_perp\"");
        let back: AssetClass = serde_json::from_str("\"fx\"").unwrap();
        assert_eq!(back, AssetClass::Fx);
    }
}
