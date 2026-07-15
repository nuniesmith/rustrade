//! Backtest configuration + builder.

use rustrade_core::Symbol;
use rustrade_risk::{CircuitBreakerConfig, SessionPnlConfig, SizingConfig};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::fees::FeeModel;
use crate::funding::FundingModel;
use crate::slippage::SlippageModel;

/// How the replay engine turns orders into fills.
///
/// Selected via [`BacktestConfig::fill_model`] (builder:
/// `.fill_model(FillModel::…)`). Defaults to [`FillModel::TakerAtClose`],
/// the engine's historical behaviour — existing backtests are
/// bit-identical with the default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FillModel {
    /// Legacy single-candle fills (the default).
    ///
    /// Market / IOC / FOK entries and closes fill as takers at the
    /// decision candle's **close**. Limit / post-only entries fill only
    /// if the *decision candle itself* crosses the level (marketability
    /// judged against the candle **open**; a marketable limit fills at
    /// the open as taker, a marketable post-only is rejected) and are
    /// dropped otherwise — nothing rests across candles. Protective
    /// brackets require **both** SL and TP legs and fill at their fixed
    /// level even when a candle gaps through it.
    #[default]
    TakerAtClose,
    /// Honest resting-order semantics.
    ///
    /// Orders reach the synthetic book at the decision candle's
    /// **close** (the market price at decision time), so the decision
    /// candle can never fill a resting order — its range printed before
    /// the order existed (no lookahead).
    ///
    /// - **Market entries and closes** fill at the decision close as
    ///   takers, same as legacy.
    /// - **Limit / post-only entries** marketable at the close fill
    ///   immediately at the close as takers (a marketable post-only is
    ///   rejected). Non-marketable ones **rest** (GTC) and fill on the
    ///   first later candle that crosses the level: at the **limit
    ///   price** on a cross, or at that candle's **open** when the
    ///   candle gaps through the level — never at a better price than
    ///   the market offered. Resting fills are makers: maker fee rate,
    ///   no slippage.
    /// - **IOC / FOK entries** that are not marketable at the close are
    ///   cancelled (the engine has no book depth, so both fill in full
    ///   or not at all).
    /// - **Protective stops** trigger on a cross and fill at the stop
    ///   level *or worse* (gap through → that candle's open), as takers
    ///   with slippage. **Take-profits** are resting limits: level on a
    ///   cross, open on a gap (price improvement), maker fee, no
    ///   slippage. Standalone (stop-only or TP-only) brackets are
    ///   honoured — legacy mode requires both legs.
    /// - **Same-candle ambiguity** (multiple levels inside one candle's
    ///   range, OHLC path unknown) resolves to the *worse* outcome for
    ///   the strategy: the stop leg fires before the take-profit leg;
    ///   on the candle a resting entry fills, its attached stop may
    ///   fire on that same candle, but its take-profit only becomes
    ///   eligible from the next candle.
    /// - At most **one resting entry per symbol**: any later non-`Hold`
    ///   decision that passes the risk gates cancels it
    ///   (cancel-and-replace, where the replacement may be nothing).
    Resting,
}

/// Configuration for a [`crate::Backtest`].
///
/// # Example
///
/// ```
/// use rustrade_backtest::{BacktestConfig, FeeModel, SlippageModel};
///
/// let config = BacktestConfig::builder()
///     .symbol("BTCUSDT")
///     .initial_cash(10_000.0)
///     .slippage(SlippageModel::FixedBps(5.0))
///     .fees(FeeModel::Flat(0.001))
///     .periods_per_year(252 * 24 * 60) // per-minute Sharpe
///     .build()
///     .unwrap();
///
/// assert_eq!(config.initial_cash, 10_000.0);
/// assert_eq!(config.periods_per_year, 252 * 24 * 60);
/// ```
#[derive(Debug, Clone)]
pub struct BacktestConfig {
    /// Symbols the brain trades. For single-symbol backtests this is a
    /// one-element vector; events whose symbol is not in the list are
    /// silently ignored. The engine routes each `MarketDataEvent` to the
    /// brain with the *current* position for that symbol.
    pub symbols: Vec<Symbol>,
    /// Starting cash balance in quote currency. Shared across all
    /// symbols — there's a single equity curve.
    pub initial_cash: f64,
    /// Sizing config — how the brain's `Decision` becomes a contract
    /// count. Same struct used by the live `ExecutionService`.
    pub sizing: SizingConfig,
    /// Slippage policy applied to every fill.
    pub slippage: SlippageModel,
    /// Fee schedule applied to every fill.
    pub fees: FeeModel,
    /// How orders turn into fills — see [`FillModel`]. Defaults to
    /// [`FillModel::TakerAtClose`] (the engine's historical behaviour);
    /// opt in to honest resting limit/stop semantics with
    /// [`FillModel::Resting`].
    pub fill_model: FillModel,
    /// Perp funding-rate schedule applied to open positions at funding
    /// settlement timestamps (see [`FundingModel`] for the sign
    /// conventions and window semantics). Multi-symbol backtests share a
    /// single schedule — for per-symbol funding run each symbol in its
    /// own `Backtest`, like [`Self::contract_value`]. Defaults to
    /// [`FundingModel::None`] — existing backtests are unchanged.
    pub funding: FundingModel,
    /// Base-asset units per contract. For spot adapters this is `1.0`;
    /// futures adapters override per symbol. Multi-symbol backtests
    /// share a single multiplier — for mixed spot/futures portfolios
    /// run each symbol in its own `Backtest` instance.
    pub contract_value: f64,
    /// Per-period risk-free rate used by [`crate::BacktestResult::sharpe_ratio`]
    /// and [`crate::BacktestResult::sortino_ratio`]. Expressed in the same
    /// cadence as the candles — e.g. for daily candles with a 2 % annual
    /// rate set this to `0.02 / 252 ≈ 7.94e-5`. Defaults to `0.0`.
    pub risk_free_rate: f64,
    /// Annualisation factor for the Sharpe and Sortino ratios. For daily
    /// candles use `252` (trading days), for hourly `24 * 252`, for
    /// minute `60 * 24 * 365`, etc. Defaults to `252`.
    pub periods_per_year: u32,
    /// Per-symbol session-PnL halt applied during replay — the same gate
    /// the live `ExecutionService` checks first. When set, each symbol
    /// gets its own `SessionPnl` driven by **candle time** (so the daily
    /// halt rolls over at 00:00 UTC in replay time, not wall time), fed
    /// from every emitted `TradeOutcome`. Once the net session PnL hits
    /// `loss_limit`, further non-`Hold` decisions for that symbol are
    /// blocked (counted in `BacktestResult::orders_blocked`) until the
    /// next UTC day. `None` (the default) disables the gate — existing
    /// backtests are unaffected.
    pub session_pnl: Option<SessionPnlConfig>,
    /// Per-symbol circuit breaker applied during replay — the live
    /// execution path's second gate. Sliding-window loss counting and the
    /// cooldown both run on **candle time**. `None` (the default)
    /// disables the gate.
    pub circuit_breaker: Option<CircuitBreakerConfig>,
}

impl BacktestConfig {
    /// Convenience accessor for single-symbol configs.
    ///
    /// Returns the first (and only) symbol when [`Self::symbols`] is a
    /// one-element vector. Panics on empty or multi-symbol configs —
    /// callers that mix scopes should use [`Self::symbols`] directly.
    pub fn symbol(&self) -> &Symbol {
        assert_eq!(
            self.symbols.len(),
            1,
            "BacktestConfig::symbol() is only valid for single-symbol backtests; \
             this config has {} symbols. Use BacktestConfig::symbols instead.",
            self.symbols.len()
        );
        &self.symbols[0]
    }
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
    symbols: Vec<Symbol>,
    initial_cash: Option<f64>,
    sizing: Option<SizingConfig>,
    slippage: Option<SlippageModel>,
    fees: Option<FeeModel>,
    fill_model: Option<FillModel>,
    funding: Option<FundingModel>,
    contract_value: Option<f64>,
    risk_free_rate: Option<f64>,
    periods_per_year: Option<u32>,
    session_pnl: Option<SessionPnlConfig>,
    circuit_breaker: Option<CircuitBreakerConfig>,
}

impl BacktestConfigBuilder {
    /// Single symbol to backtest. Convenience wrapper — equivalent to
    /// calling [`Self::symbols`] with a one-element vector. Repeated
    /// calls replace any previously set symbols.
    pub fn symbol(mut self, sym: impl Into<Symbol>) -> Self {
        self.symbols = vec![sym.into()];
        self
    }
    /// Set the full symbol list. The brain will see events for all
    /// listed symbols and is responsible for filtering. At least one
    /// symbol is required.
    pub fn symbols<I, S>(mut self, syms: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Symbol>,
    {
        self.symbols = syms.into_iter().map(Into::into).collect();
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
    /// Override the fill model (default [`FillModel::TakerAtClose`] —
    /// the engine's historical single-candle fills). Pass
    /// [`FillModel::Resting`] for honest resting limit/stop semantics.
    pub fn fill_model(mut self, m: FillModel) -> Self {
        self.fill_model = Some(m);
        self
    }
    /// Enable perp funding cashflows (default [`FundingModel::None`] —
    /// off). Pass a historical [`FundingModel::Series`] when one exists,
    /// or a [`FundingModel::Constant`] rate + interval as the fallback.
    pub fn funding(mut self, m: FundingModel) -> Self {
        self.funding = Some(m);
        self
    }
    /// Override the contract multiplier (default 1.0 — spot).
    pub fn contract_value(mut self, cv: f64) -> Self {
        self.contract_value = Some(cv);
        self
    }
    /// Per-period risk-free rate for Sharpe / Sortino (default `0.0`).
    /// See [`BacktestConfig::risk_free_rate`] for the expected scaling.
    pub fn risk_free_rate(mut self, r: f64) -> Self {
        self.risk_free_rate = Some(r);
        self
    }
    /// Annualisation factor for Sharpe / Sortino (default `252`).
    /// See [`BacktestConfig::periods_per_year`] for the typical cadences.
    pub fn periods_per_year(mut self, n: u32) -> Self {
        self.periods_per_year = Some(n);
        self
    }
    /// Enable the per-symbol session-PnL halt during replay (off by
    /// default). Use the same [`SessionPnlConfig`] the live bot runs with
    /// so the backtest reproduces live gating.
    pub fn session_pnl(mut self, cfg: SessionPnlConfig) -> Self {
        self.session_pnl = Some(cfg);
        self
    }
    /// Enable the per-symbol circuit breaker during replay (off by
    /// default). Use the same [`CircuitBreakerConfig`] the live bot runs
    /// with so the backtest reproduces live gating.
    pub fn circuit_breaker(mut self, cfg: CircuitBreakerConfig) -> Self {
        self.circuit_breaker = Some(cfg);
        self
    }

    /// Validate and build. Returns `Error::Config` on any constraint
    /// violation.
    pub fn build(self) -> Result<BacktestConfig> {
        if self.symbols.is_empty() {
            return Err(Error::Config(
                "BacktestConfig requires at least one symbol".into(),
            ));
        }
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
        let risk_free_rate = self.risk_free_rate.unwrap_or(0.0);
        if !risk_free_rate.is_finite() {
            return Err(Error::Config(
                "BacktestConfig.risk_free_rate must be finite".into(),
            ));
        }
        let periods_per_year = self.periods_per_year.unwrap_or(252);
        if periods_per_year == 0 {
            return Err(Error::Config(
                "BacktestConfig.periods_per_year must be > 0".into(),
            ));
        }
        // Mirror BotConfig's validation: a NaN loss limit would make
        // every halt comparison silently false (a disabled gate that
        // looks enabled).
        if let Some(sp) = &self.session_pnl
            && sp.loss_limit.is_nan()
        {
            return Err(Error::Config(
                "BacktestConfig.session_pnl.loss_limit must not be NaN".into(),
            ));
        }
        // Validate + normalise the funding schedule (sorts a Series,
        // rejects NaN rates / non-positive intervals / duplicate
        // settlement timestamps).
        let funding = self
            .funding
            .unwrap_or_default()
            .validated()
            .map_err(|why| Error::Config(format!("BacktestConfig.funding: {why}")))?;
        Ok(BacktestConfig {
            symbols: self.symbols,
            initial_cash,
            sizing: self.sizing.unwrap_or_default(),
            slippage: self.slippage.unwrap_or_default(),
            fees: self.fees.unwrap_or_default(),
            fill_model: self.fill_model.unwrap_or_default(),
            funding,
            contract_value,
            risk_free_rate,
            periods_per_year,
            session_pnl: self.session_pnl,
            circuit_breaker: self.circuit_breaker,
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
    fn rejects_zero_periods_per_year() {
        let r = BacktestConfig::builder()
            .symbol("X")
            .periods_per_year(0)
            .build();
        assert!(matches!(r, Err(Error::Config(_))));
    }

    #[test]
    fn rejects_nan_risk_free_rate() {
        let r = BacktestConfig::builder()
            .symbol("X")
            .risk_free_rate(f64::NAN)
            .build();
        assert!(matches!(r, Err(Error::Config(_))));
    }

    #[test]
    fn defaults_for_optional_fields() {
        let c = BacktestConfig::builder().symbol("X").build().unwrap();
        assert_eq!(c.initial_cash, 10_000.0);
        assert_eq!(c.contract_value, 1.0);
        assert_eq!(c.slippage, SlippageModel::Zero);
        assert_eq!(c.fill_model, FillModel::TakerAtClose);
        assert_eq!(c.funding, FundingModel::None);
        assert_eq!(c.risk_free_rate, 0.0);
        assert_eq!(c.periods_per_year, 252);
    }

    #[test]
    fn rejects_invalid_funding_models() {
        for bad in [
            FundingModel::Constant {
                rate: f64::NAN,
                interval_ms: 1_000,
            },
            FundingModel::Constant {
                rate: 0.0001,
                interval_ms: 0,
            },
            FundingModel::Series(vec![(100, 0.1), (100, 0.2)]),
            FundingModel::Series(vec![(100, f64::INFINITY)]),
        ] {
            let r = BacktestConfig::builder().symbol("X").funding(bad).build();
            assert!(matches!(r, Err(Error::Config(_))));
        }
    }

    #[test]
    fn build_sorts_funding_series() {
        let c = BacktestConfig::builder()
            .symbol("X")
            .funding(FundingModel::Series(vec![(300, 0.3), (100, 0.1)]))
            .build()
            .unwrap();
        assert_eq!(
            c.funding,
            FundingModel::Series(vec![(100, 0.1), (300, 0.3)])
        );
    }

    #[test]
    fn multi_symbol_config_round_trips() {
        let c = BacktestConfig::builder()
            .symbols(["BTCUSDT", "ETHUSDT", "SOLUSDT"])
            .build()
            .unwrap();
        assert_eq!(c.symbols.len(), 3);
        assert_eq!(c.symbols[0].as_str(), "BTCUSDT");
        assert_eq!(c.symbols[2].as_str(), "SOLUSDT");
    }

    #[test]
    fn symbol_accessor_panics_on_multi_symbol() {
        let c = BacktestConfig::builder()
            .symbols(["A", "B"])
            .build()
            .unwrap();
        let r = std::panic::catch_unwind(|| {
            let _ = c.symbol();
        });
        assert!(r.is_err());
    }

    #[test]
    fn symbol_accessor_works_on_single_symbol() {
        let c = BacktestConfig::builder().symbol("X").build().unwrap();
        assert_eq!(c.symbol().as_str(), "X");
    }
}
