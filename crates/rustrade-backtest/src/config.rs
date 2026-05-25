//! Backtest configuration + builder.

use rustrade_core::Symbol;
use rustrade_risk::SizingConfig;

use crate::error::{Error, Result};
use crate::fees::FeeModel;
use crate::slippage::SlippageModel;

/// Configuration for a [`crate::Backtest`].
#[derive(Debug, Clone)]
pub struct BacktestConfig {
    /// Symbol the brain trades. The engine only routes events with this
    /// symbol to the brain; events for other symbols are silently
    /// ignored (kept for future multi-symbol support).
    pub symbol: Symbol,
    /// Starting cash balance in quote currency.
    pub initial_cash: f64,
    /// Sizing config — how the brain's `Decision` becomes a contract
    /// count. Same struct used by the live `ExecutionService`.
    pub sizing: SizingConfig,
    /// Slippage policy applied to every fill.
    pub slippage: SlippageModel,
    /// Fee schedule applied to every fill.
    pub fees: FeeModel,
    /// Base-asset units per contract. For spot adapters this is `1.0`;
    /// futures adapters override per symbol. Backtests are single-symbol
    /// so it lives on the config rather than the (absent) exchange.
    pub contract_value: f64,
}

impl BacktestConfig {
    /// Start a [`BacktestConfigBuilder`].
    pub fn builder() -> BacktestConfigBuilder {
        BacktestConfigBuilder::default()
    }
}

/// Builder for [`BacktestConfig`]. Validates on [`Self::build`].
#[derive(Debug, Clone, Default)]
pub struct BacktestConfigBuilder {
    symbol: Option<Symbol>,
    initial_cash: Option<f64>,
    sizing: Option<SizingConfig>,
    slippage: Option<SlippageModel>,
    fees: Option<FeeModel>,
    contract_value: Option<f64>,
}

impl BacktestConfigBuilder {
    /// Symbol to backtest. Required.
    pub fn symbol(mut self, sym: impl Into<Symbol>) -> Self {
        self.symbol = Some(sym.into());
        self
    }
    /// Override the starting cash balance (default 10_000.0).
    pub fn initial_cash(mut self, cash: f64) -> Self {
        self.initial_cash = Some(cash);
        self
    }
    /// Override the position-sizing config.
    pub fn sizing(mut self, sizing: SizingConfig) -> Self {
        self.sizing = Some(sizing);
        self
    }
    /// Override the slippage model (default `Zero`).
    pub fn slippage(mut self, m: SlippageModel) -> Self {
        self.slippage = Some(m);
        self
    }
    /// Override the fee model (default `Flat(0.0005)`).
    pub fn fees(mut self, m: FeeModel) -> Self {
        self.fees = Some(m);
        self
    }
    /// Override the contract multiplier (default 1.0 — spot).
    pub fn contract_value(mut self, cv: f64) -> Self {
        self.contract_value = Some(cv);
        self
    }

    /// Validate and build. Returns `Error::Config` on any constraint
    /// violation.
    pub fn build(self) -> Result<BacktestConfig> {
        let symbol = self
            .symbol
            .ok_or_else(|| Error::Config("BacktestConfig.symbol is required".into()))?;
        let initial_cash = self.initial_cash.unwrap_or(10_000.0);
        if !initial_cash.is_finite() || initial_cash <= 0.0 {
            return Err(Error::Config(
                "BacktestConfig.initial_cash must be a finite positive number".into(),
            ));
        }
        let contract_value = self.contract_value.unwrap_or(1.0);
        if !contract_value.is_finite() || contract_value <= 0.0 {
            return Err(Error::Config(
                "BacktestConfig.contract_value must be a finite positive number".into(),
            ));
        }
        Ok(BacktestConfig {
            symbol,
            initial_cash,
            sizing: self.sizing.unwrap_or_default(),
            slippage: self.slippage.unwrap_or_default(),
            fees: self.fees.unwrap_or_default(),
            contract_value,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requires_symbol() {
        assert!(matches!(
            BacktestConfig::builder().build(),
            Err(Error::Config(_))
        ));
    }

    #[test]
    fn rejects_non_positive_cash() {
        let r = BacktestConfig::builder()
            .symbol("BTCUSDT")
            .initial_cash(-100.0)
            .build();
        assert!(matches!(r, Err(Error::Config(_))));
    }

    #[test]
    fn rejects_non_positive_contract_value() {
        let r = BacktestConfig::builder()
            .symbol("X")
            .contract_value(0.0)
            .build();
        assert!(matches!(r, Err(Error::Config(_))));
    }

    #[test]
    fn defaults_for_optional_fields() {
        let c = BacktestConfig::builder().symbol("X").build().unwrap();
        assert_eq!(c.initial_cash, 10_000.0);
        assert_eq!(c.contract_value, 1.0);
        assert_eq!(c.slippage, SlippageModel::Zero);
    }
}
