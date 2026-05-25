//! Trait contracts for exchange integrations.
//!
//! Concrete exchange clients (KuCoin, Binance, …) implement [`ExchangeClient`]
//! so the bot framework can stay exchange-agnostic. A client crate like
//! `exchange-apiws` already provides most of this — these traits are the
//! framework-side view.

use std::time::Duration;

use async_trait::async_trait;

use crate::error::Result;
use crate::market::{MarketDataEvent, Symbol};
use crate::types::{Candle, Fill, Order, Position};

/// Optional adapter capabilities, queried via [`ExchangeClient::supports`].
///
/// The framework consults this to degrade gracefully when an adapter
/// doesn't implement a feature an [`Order`] or strategy requests — e.g.
/// rejecting an order with `Order.stop = Some(...)` against an adapter
/// that returns `false` for [`Capability::StopOrders`] rather than
/// silently dropping the attachment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Capability {
    /// Adapter accepts `Order.stop` and translates to native stop orders.
    StopOrders,
    /// Adapter rejects post-only orders that would cross the book as taker.
    PostOnly,
    /// Adapter honours `Order.reduce_only`.
    ReduceOnly,
    /// Adapter supports `OrderKind::Ioc`.
    Ioc,
    /// Adapter supports `OrderKind::Fok`.
    Fok,
    /// Adapter can stream a public market-data feed alongside trading.
    /// Most do; spot-only HTTP adapters may not.
    PublicFeed,
    /// Adapter pushes fill / order-update events on a private feed.
    PrivateFeed,
}

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
    async fn cancel_all(&self, symbol: &Symbol) -> Result<usize>;

    /// Close the given position with a market order. Returns the exchange
    /// order id of the close.
    async fn close_position(&self, symbol: &Symbol, position: &Position) -> Result<String>;

    /// Fetch the current position for a symbol (or `Position::FLAT` if flat).
    async fn get_position(&self, symbol: &Symbol) -> Result<Position>;

    /// Fetch the current balance in the given currency.
    async fn get_balance(&self, currency: &str) -> Result<f64>;

    /// Does this adapter support the given optional capability?
    ///
    /// The default returns `false` for every variant — adapters opt in by
    /// overriding. This is intentionally conservative: a new adapter that
    /// forgets to override won't quietly accept orders it can't execute.
    fn supports(&self, _capability: Capability) -> bool {
        false
    }

    /// Base-asset units per one contract for the given symbol.
    ///
    /// For spot exchanges this is `1.0` for every symbol — one unit traded
    /// equals one unit of the base asset. For futures, it's the contract
    /// multiplier (e.g. `0.001` for KuCoin XBTUSDTM where one contract is
    /// 0.001 BTC). The risk layer's
    /// [`PositionSizer`](https://docs.rs/rustrade-risk/latest/rustrade_risk/sizing/struct.PositionSizer.html)
    /// uses this to convert margin × leverage into a contract count.
    ///
    /// The default returns `1.0` — appropriate for spot adapters. Futures
    /// adapters override.
    fn contract_value(&self, _symbol: &Symbol) -> f64 {
        1.0
    }
}

/// A source of live market data (WebSocket feed, backtest replay, simulator).
///
/// Implementors push events into the bot via the
/// [`MarketDataBus`](crate::bus::MarketDataBus) that the supervisor creates.
/// `MarketSource` is intended to be wrapped by a `TradingService` in
/// `rustrade-supervisor` so it inherits lifecycle management and
/// auto-restart; this trait just documents the contract on the data side.
///
/// # Cancellation contract
///
/// `run` does **not** take a `CancellationToken` directly. Cancellation is
/// expected to flow through the wrapping `TradingService` — when the
/// supervisor cancels that service's token, it drops the
/// `MarketSource::run` future at its next `.await`.
///
/// Implementors must therefore be **drop-safe**: any open resources
/// (WebSocket connections, HTTP sessions, file handles) must release
/// cleanly when their containing future is dropped. In practice this
/// means:
///
/// - Use `tokio::select!` against external events only inside your own
///   loop, not against an externally-owned cancel signal here.
/// - Don't hold a `MutexGuard` across an `.await` that could be dropped
///   mid-flight — dropping a guard is fine, but holding one while the
///   future is destructured can deadlock the lock.
/// - If you need explicit teardown, perform it in a `Drop` impl on the
///   implementing type rather than at the end of `run`.
#[async_trait]
pub trait MarketSource: Send + Sync + 'static {
    /// Short identifier for logging — typically the exchange name.
    fn name(&self) -> &str;

    /// Begin streaming events. Should run until the feed terminates or
    /// the caller drops the returned future (see the cancellation contract
    /// in the trait docs).
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

/// Periodic candle source — separate from [`MarketSource`] because
/// candle polling has a fundamentally different shape (pull, paced)
/// than streaming events (push, unbounded).
///
/// Spot-only adapters don't need to implement this; the framework will
/// only spawn a candle poller when one is wired via
/// `Bot::with_candle_poller`. Futures adapters with native candle
/// endpoints (KuCoin, Binance, Bybit, …) implement it directly.
#[async_trait]
pub trait CandleSource: Send + Sync + 'static {
    /// Short identifier for logging — typically the exchange name.
    fn name(&self) -> &str;

    /// Fetch up to `limit` of the most recent completed candles for
    /// `symbol` at the given interval. Implementors return them in
    /// chronological order (oldest first). If the exchange's native
    /// endpoint returns newest-first, sort before returning.
    async fn poll(&self, symbol: &Symbol, interval: Duration, limit: usize) -> Result<Vec<Candle>>;
}
