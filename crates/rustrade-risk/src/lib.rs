#![warn(missing_docs)]

//! # rustrade-risk
//!
//! Generic risk primitives for trading bots:
//!
//! - [`CircuitBreaker`] — sliding-window loss breaker (ported from the
//!   kucoin bot's `circuit_breaker.rs`).
//! - [`SessionPnl`] — daily PnL tracker with drawdown cap and automatic
//!   00:00 UTC rollover.
//! - [`PositionSizer`] — notional-based sizing from margin + leverage +
//!   price + contract multiplier (ported from kucoin's `sizing.rs`).
//! - [`PortfolioRisk`] — account-level risk: an account-wide daily-loss halt
//!   plus a pre-trade gate over aggregate exposure and concurrent positions.
//!   The per-symbol primitives above bound each symbol; this bounds the whole
//!   account (multi-asset trading needs both).
//!
//! Nothing here is strategy- or exchange-specific. If you find yourself
//! needing to special-case a particular strategy, the logic belongs in
//! the `Brain` implementation, not here.

pub mod circuit_breaker;
pub mod clock;
pub mod portfolio;
pub mod session_pnl;
pub mod sizing;

pub use circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitBreakerSnapshot};
pub use clock::{Clock, ManualClock, SystemClock};
pub use portfolio::{
    PortfolioBlock, PortfolioRisk, PortfolioRiskConfig, PortfolioRiskSnapshot, PortfolioState,
};
pub use session_pnl::{SessionPnl, SessionPnlConfig, SessionPnlSnapshot};
pub use sizing::{PositionSizer, SizingConfig};
