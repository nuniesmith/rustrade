#![warn(missing_docs)]

//! # rustrade-backtest
//!
//! Deterministic backtest engine for [`rustrade_core::Brain`]s. The same
//! `Brain` trait used by `rustrade` for live trading drives the backtest —
//! no special "backtest-mode" code paths in the strategy, no duplicate
//! decision logic to keep in sync.
//!
//! See the crate-level [`README.md`](https://github.com/nuniesmith/rustrade/blob/main/crates/rustrade-backtest/README.md)
//! for a quickstart.
//!
//! # Determinism
//!
//! Given the same `(Brain, Vec<Candle>, BacktestConfig)` inputs the engine
//! produces the same [`BacktestResult`] every run. There is no random
//! number source and no thread interleaving — the loop is single-threaded
//! and synchronous through every step that affects state.
//!
//! # Risk-gate parity with live
//!
//! The live `ExecutionService` blocks decisions behind a per-symbol
//! session-PnL halt and circuit breaker. Thread the same configs through
//! [`BacktestConfig::session_pnl`] / [`BacktestConfig::circuit_breaker`]
//! and the replay engine applies the same gates — driven by **candle
//! time** (the daily halt rolls over at 00:00 UTC in replay time, the
//! breaker's window and cooldown run on candle timestamps), so a brain
//! that relies on live gating backtests the way it trades. Blocked
//! decisions are counted in [`BacktestResult::orders_blocked`]. Both
//! gates default to off.
//!
//! # Fill models
//!
//! [`BacktestConfig::fill_model`] (builder: `.fill_model(FillModel::…)`)
//! selects how orders become fills. The default,
//! [`FillModel::TakerAtClose`], keeps the engine's historical
//! single-candle semantics — existing backtests are bit-for-bit
//! unchanged. [`FillModel::Resting`] opts in to honest resting-order
//! semantics: non-marketable limits rest across candles and fill when a
//! later candle crosses their level (at the limit price, or at the
//! candle's open when it gaps through), stops trigger on a cross and
//! fill at the level or worse, standalone (stop-only / TP-only)
//! protective orders are honoured, and same-candle ambiguity resolves
//! conservatively. See [`FillModel`] for the full convention list.
//!
//! # Perp funding
//!
//! [`BacktestConfig::funding`] (builder: `.funding(FundingModel::…)`)
//! books perp funding cashflows against open positions at settlement
//! timestamps — a historical [`FundingModel::Series`] or a constant
//! rate + interval fallback. Positive rate → longs pay, shorts receive;
//! cashflows land in the equity curve, accrue onto each closed trade's
//! [`TradeOutcome::funding`], and total up in
//! [`BacktestResult::funding_received`] / [`BacktestResult::funding_paid`].
//! Defaults to [`FundingModel::None`] — existing backtests are
//! bit-for-bit unchanged.
//!
//! # Status
//!
//! Phase 4b — adds CSV candle loader, Sharpe/Sortino metrics, and
//! multi-symbol replay. Parquet loaders and book-walk slippage remain
//! deferred. See the workspace `TODO.md`.

pub mod config;
pub mod engine;
pub mod error;
pub mod fees;
pub mod funding;
pub mod loaders;
pub mod metrics;
pub mod result;
pub mod slippage;

pub use config::{BacktestConfig, BacktestConfigBuilder, FillModel};
pub use engine::Backtest;
pub use error::{Error, Result};
pub use fees::FeeModel;
pub use funding::{EIGHT_HOURS_MS, FundingModel};
pub use loaders::{load_csv, load_csv_str, sort_chronological};
pub use metrics::TradeOutcome;
pub use result::{BacktestResult, EquityPoint};
pub use slippage::SlippageModel;
