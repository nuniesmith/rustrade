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
//! # Status
//!
//! Phase 4a — minimum viable engine. CSV / Parquet candle loaders, Sharpe
//! / Sortino metrics, and book-walk slippage land in Phase 4b. See the
//! workspace `TODO.md`.

pub mod config;
pub mod engine;
pub mod error;
pub mod fees;
pub mod metrics;
pub mod result;
pub mod slippage;

pub use config::{BacktestConfig, BacktestConfigBuilder};
pub use engine::Backtest;
pub use error::{Error, Result};
pub use fees::FeeModel;
pub use metrics::TradeOutcome;
pub use result::BacktestResult;
pub use slippage::SlippageModel;
