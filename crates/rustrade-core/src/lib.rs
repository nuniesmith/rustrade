//! # rustrade-core
//!
//! Core types and traits for the rustrade trading bot framework.
//!
//! This crate is the type-layer foundation used by every other crate in the
//! `rustrade` ecosystem. It is deliberately dependency-light and contains
//! **no I/O, no async runtime state, and no exchange-specific logic**.
//!
//! # What lives here
//!
//! - Domain types: [`Price`], [`Volume`], [`Candle`], [`Tick`], [`Order`], [`Fill`], [`Position`]
//! - Trading intents: [`Signal`], [`Decision`], [`SizeHint`]
//! - Trait contracts: [`Brain`], [`ExchangeClient`], [`MarketSource`]
//! - Broadcast buses: [`MarketDataBus`], [`SignalBus`]
//! - Error types: [`Error`], [`Result`]
//!
//! # What does NOT live here
//!
//! - Service lifecycle / supervision → [`rustrade-supervisor`]
//! - Risk calculations → [`rustrade-risk`]
//! - Backtest replay → [`rustrade-backtest`]
//! - Exchange clients → separate crates (e.g. `exchange-apiws`)
//! - Indicator implementations → separate crates (e.g. `indicators-ta`)
//!
//! # Design principles
//!
//! 1. **Zero runtime deps beyond tokio primitives.** No HTTP, no Redis, no
//!    tracing-subscriber, no prometheus. Downstream crates add those.
//! 2. **Newtype wrappers for numeric quantities** to prevent unit confusion.
//! 3. **Traits are narrow.** Implementors should not need to stub dozens
//!    of methods they don't care about.
//! 4. **Traits are object-safe where possible** so `Box<dyn Brain>` works.

pub mod brain;
pub mod bus;
pub mod error;
pub mod exchange;
pub mod market;
pub mod signal;
pub mod types;

pub use brain::{Brain, BrainHealth, Decision, SizeHint};
pub use bus::{MarketDataBus, SignalBus};
pub use error::{Error, Result};
pub use exchange::{ExchangeClient, MarketSource, OrderStatus};
pub use market::{Exchange, MarketDataEvent, Side, Symbol};
pub use signal::{Signal, SignalType};
pub use types::{Candle, Fill, Order, OrderKind, Position, Price, Tick, Volume};
