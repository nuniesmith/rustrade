//! Trait contracts for exchange integrations.
//!
//! Concrete exchange clients (KuCoin, Binance, …) implement [`ExchangeClient`]
//! so the bot framework can stay exchange-agnostic. A client crate like
//! `exchange-apiws` already provides most of this — these traits are the
//! framework-side view.

use async_trait::async_trait;

use crate::error::Result;
use crate::market::MarketDataEvent;
use crate::types::{Fill, Order, Position};

/// Status of an order as reported by the exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    /// Submitted, not yet on the book.
    Pending,
    /// Resting on the book or in the matching engine.
    Open,
    /// Partially filled; still resting for the remainder.
    PartiallyFilled,
    /// Fully filled.
    Filled,
    /// Cancelled before full fill.
    Cancelled,
    /// Rejected by the exchange.
    Rejected,
}

/// What the bot framework needs from an exchange to trade.
///
/// This trait is intentionally narrow — the full surface of a real exchange
/// client (ws token management, stop orders, funding history, account tiers)
/// belongs in the concrete adapter crate, not here. The framework only
/// needs to: place orders, close positions, read balance and position state.
///
/// # Async + object-safe
///
/// `async_trait` is used so `Arc<dyn ExchangeClient>` works — downstream
/// code can swap concrete exchanges at runtime without generics propagating
/// through the whole system.
#[async_trait]
pub trait ExchangeClient: Send + Sync + 'static {
    /// Short, lowercase exchange identifier — e.g. `"kucoin"`.
    fn name(&self) -> &str;

    /// Place an order. Returns the exchange-assigned order id.
    async fn place_order(&self, order: &Order) -> Result<String>;

    /// Cancel all open orders for a symbol. Returns the count cancelled.
    async fn cancel_all(&self, symbol: &str) -> Result<usize>;

    /// Close the given position with a market order. Returns the exchange
    /// order id of the close.
    async fn close_position(&self, symbol: &str, position: &Position) -> Result<String>;

    /// Fetch the current position for a symbol (or `Position::FLAT` if flat).
    async fn get_position(&self, symbol: &str) -> Result<Position>;

    /// Fetch the current balance in the given currency.
    async fn get_balance(&self, currency: &str) -> Result<f64>;
}

/// A source of live market data (WebSocket feed, backtest replay, simulator).
///
/// Implementors push events into the bot via the `rustrade-core::bus::MarketDataBus`
/// that the supervisor creates. The `MarketSource` itself is modelled as a
/// `TradingService` in `rustrade-supervisor` (so it gets lifecycle management
/// and auto-restart); this trait exists just to document the contract.
#[async_trait]
pub trait MarketSource: Send + Sync + 'static {
    fn name(&self) -> &str;

    /// Begin streaming events. Runs until the returned future completes
    /// (typically never, unless the feed closes or is cancelled).
    async fn run(&self) -> Result<()>;

    /// Is the feed currently receiving data?
    fn is_live(&self) -> bool;
}

/// Received fill events from the exchange's private feed.
///
/// Adapters implement this to route fills into the bot. Most exchanges push
/// both order updates and fill events; this trait abstracts the "fill" part.
#[async_trait]
pub trait FillSource: Send + Sync + 'static {
    /// Await the next fill. Returns `None` when the stream ends.
    async fn next_fill(&self) -> Option<Fill>;
}

/// Received order-book / market-data events from the exchange's public feed.
#[async_trait]
pub trait EventSource: Send + Sync + 'static {
    /// Await the next event. Returns `None` when the stream ends.
    async fn next_event(&self) -> Option<MarketDataEvent>;
}
