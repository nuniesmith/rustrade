//! Trait contracts for exchange integrations.
//!
//! Concrete exchange clients (KuCoin, Binance, …) implement [`ExchangeClient`]
//! so the bot framework can stay exchange-agnostic. A client crate like
//! `exchange-apiws` already provides most of this — these traits are the
//! framework-side view.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::market::{MarketDataEvent, Side, Symbol};
use crate::types::{Candle, Fill, Order, OrderKind, Position, Price, Volume};

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
    /// Adapter implements [`ExchangeClient::get_open_orders`] and
    /// [`ExchangeClient::cancel_order`] — required for resting-order
    /// tracking, TTL cancellation, and reconnect reconciliation.
    OrderTracking,
}

/// Status of an order as reported by the exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

impl OrderStatus {
    /// `true` while the order may still rest on the book and consume margin
    /// — i.e. it is not in a terminal state. The order-tracking layer keeps
    /// watching these and drops the rest.
    pub fn is_live(self) -> bool {
        matches!(
            self,
            OrderStatus::Pending | OrderStatus::Open | OrderStatus::PartiallyFilled
        )
    }
}

/// A resting (not-yet-terminal) order as reported by the exchange, returned
/// by [`ExchangeClient::get_open_orders`].
///
/// This is the exchange's view of an order the bot previously placed — used
/// by the framework's order-tracking layer to age out stale resting orders
/// and to reconcile tracked state after a reconnect.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenOrder {
    /// Exchange-assigned order id (matches the id returned by
    /// [`ExchangeClient::place_order`]).
    pub order_id: String,
    /// Client-supplied id echoed back by the exchange, if any.
    pub client_id: Option<String>,
    /// Symbol the order is for.
    pub symbol: Symbol,
    /// Side of the resting order.
    pub side: Side,
    /// Order kind (limit / post-only / …). Market orders never rest, so a
    /// well-behaved adapter won't report them here.
    pub kind: OrderKind,
    /// Limit price, if the order kind carries one.
    pub limit_price: Option<Price>,
    /// Original order quantity.
    pub size: Volume,
    /// Quantity filled so far (`0` for an untouched resting order).
    pub filled: Volume,
    /// Current status as reported by the exchange.
    pub status: OrderStatus,
    /// When the order was created at the exchange, if known. Used by the
    /// tracker's TTL logic; `None` falls back to the time the bot first
    /// observed the order.
    pub created_at: Option<DateTime<Utc>>,
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
///
/// # Example
///
/// A stub adapter useful for examples and tests. Real adapters connect
/// to a network and report actual state.
///
/// ```
/// use async_trait::async_trait;
/// use rustrade_core::{Capability, ExchangeClient, Order, Position, Result, Symbol};
///
/// struct StubExchange;
///
/// #[async_trait]
/// impl ExchangeClient for StubExchange {
///     fn name(&self) -> &str { "stub" }
///     async fn place_order(&self, _order: &Order) -> Result<String> {
///         Ok("order-1".into())
///     }
///     async fn cancel_all(&self, _symbol: &Symbol) -> Result<usize> { Ok(0) }
///     async fn close_position(&self, _symbol: &Symbol, _p: &Position) -> Result<String> {
///         Ok("close-1".into())
///     }
///     async fn get_position(&self, _symbol: &Symbol) -> Result<Position> {
///         Ok(Position::FLAT)
///     }
///     async fn get_balance(&self, _currency: &str) -> Result<f64> { Ok(0.0) }
///     fn supports(&self, c: Capability) -> bool {
///         matches!(c, Capability::ReduceOnly)
///     }
/// }
/// ```
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

    /// List the currently-resting (non-terminal) orders for a symbol.
    ///
    /// Used by the framework's order-tracking layer to age out stale limit
    /// orders and to reconcile after a reconnect. The default returns an
    /// empty list — an adapter that doesn't advertise
    /// [`Capability::OrderTracking`] is assumed to have no resting orders
    /// the framework should manage (e.g. a market-only or fire-and-forget
    /// adapter). Adapters that support resting orders **must** override this
    /// and advertise the capability.
    async fn get_open_orders(&self, _symbol: &Symbol) -> Result<Vec<OpenOrder>> {
        Ok(Vec::new())
    }

    /// Cancel a single resting order by its exchange-assigned id.
    ///
    /// Returns `Ok(true)` if the order was cancelled, `Ok(false)` if it was
    /// already gone (filled or cancelled) — a benign no-op the caller can
    /// ignore. The default errors, so the tracking layer never believes it
    /// cancelled an order against an adapter that can't actually do so;
    /// adapters advertising [`Capability::OrderTracking`] override it.
    async fn cancel_order(&self, _symbol: &Symbol, _order_id: &str) -> Result<bool> {
        Err(crate::Error::exchange(
            "cancel_order not supported by this adapter (no Capability::OrderTracking)",
        ))
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
/// # Example
///
/// A loopback source that publishes a single tick and exits. Production
/// sources hold the `MarketDataBus` sender they were constructed with
/// and publish to it from `run`.
///
/// ```
/// use async_trait::async_trait;
/// use rustrade_core::{MarketSource, Result};
///
/// struct OneShotSource {
///     name: String,
/// }
///
/// #[async_trait]
/// impl MarketSource for OneShotSource {
///     fn name(&self) -> &str { &self.name }
///     async fn run(&self) -> Result<()> {
///         // In a real impl: connect to feed, loop publishing events to
///         // the bus, return Ok(()) on clean shutdown.
///         Ok(())
///     }
///     fn is_live(&self) -> bool { false }
/// }
/// ```
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
///
/// # Example
///
/// An in-memory fill source backed by a [`tokio::sync::mpsc`] channel —
/// useful for tests and replay drivers.
///
/// ```
/// use async_trait::async_trait;
/// use rustrade_core::{Fill, FillSource};
/// use tokio::sync::mpsc;
/// use tokio::sync::Mutex;
///
/// struct ChannelFills {
///     rx: Mutex<mpsc::UnboundedReceiver<Fill>>,
/// }
///
/// #[async_trait]
/// impl FillSource for ChannelFills {
///     async fn next_fill(&self) -> Option<Fill> {
///         self.rx.lock().await.recv().await
///     }
/// }
/// ```
#[async_trait]
pub trait FillSource: Send + Sync + 'static {
    /// Await the next fill. Returns `None` when the stream ends.
    async fn next_fill(&self) -> Option<Fill>;
}

/// Received order-book / market-data events from the exchange's public feed.
///
/// # Example
///
/// A simple channel-backed event source — typical for tests that push
/// scripted ticks/candles into the bot.
///
/// ```
/// use async_trait::async_trait;
/// use rustrade_core::{EventSource, MarketDataEvent};
/// use tokio::sync::mpsc;
/// use tokio::sync::Mutex;
///
/// struct ChannelEvents {
///     rx: Mutex<mpsc::UnboundedReceiver<MarketDataEvent>>,
/// }
///
/// #[async_trait]
/// impl EventSource for ChannelEvents {
///     async fn next_event(&self) -> Option<MarketDataEvent> {
///         self.rx.lock().await.recv().await
///     }
/// }
/// ```
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
///
/// # Example
///
/// A fixed-series source useful for backtests and replays. The
/// framework's poller will dedupe by `Candle::time`, so repeated polls
/// returning the same head are safe.
///
/// ```
/// use std::time::Duration;
/// use async_trait::async_trait;
/// use rustrade_core::{Candle, CandleSource, Result, Symbol};
///
/// struct FixedCandles {
///     candles: Vec<Candle>,
/// }
///
/// #[async_trait]
/// impl CandleSource for FixedCandles {
///     fn name(&self) -> &str { "fixed" }
///     async fn poll(
///         &self,
///         _symbol: &Symbol,
///         _interval: Duration,
///         limit: usize,
///     ) -> Result<Vec<Candle>> {
///         Ok(self.candles.iter().rev().take(limit).rev().copied().collect())
///     }
/// }
/// ```
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Price, Volume};

    #[test]
    fn order_status_is_live_only_for_non_terminal() {
        assert!(OrderStatus::Pending.is_live());
        assert!(OrderStatus::Open.is_live());
        assert!(OrderStatus::PartiallyFilled.is_live());
        assert!(!OrderStatus::Filled.is_live());
        assert!(!OrderStatus::Cancelled.is_live());
        assert!(!OrderStatus::Rejected.is_live());
    }

    #[test]
    fn open_order_serde_roundtrip() {
        let o = OpenOrder {
            order_id: "ex-1".into(),
            client_id: Some("cli-1".into()),
            symbol: Symbol::from("BTCUSDT"),
            side: Side::Buy,
            kind: OrderKind::Limit,
            limit_price: Some(Price(100.0)),
            size: Volume(2.0),
            filled: Volume(0.5),
            status: OrderStatus::PartiallyFilled,
            created_at: None,
        };
        let json = serde_json::to_string(&o).unwrap();
        let back: OpenOrder = serde_json::from_str(&json).unwrap();
        assert_eq!(back, o);
    }

    #[test]
    fn order_status_serde_is_snake_case() {
        let json = serde_json::to_string(&OrderStatus::PartiallyFilled).unwrap();
        assert_eq!(json, "\"partially_filled\"");
    }

    // A default adapter (no OrderTracking) reports no open orders and
    // refuses to cancel — proving the conservative defaults.
    struct DefaultAdapter;
    #[async_trait]
    impl ExchangeClient for DefaultAdapter {
        fn name(&self) -> &str {
            "default"
        }
        async fn place_order(&self, _o: &Order) -> Result<String> {
            Ok("id".into())
        }
        async fn cancel_all(&self, _s: &Symbol) -> Result<usize> {
            Ok(0)
        }
        async fn close_position(&self, _s: &Symbol, _p: &Position) -> Result<String> {
            Ok("c".into())
        }
        async fn get_position(&self, _s: &Symbol) -> Result<Position> {
            Ok(Position::FLAT)
        }
        async fn get_balance(&self, _c: &str) -> Result<f64> {
            Ok(0.0)
        }
    }

    #[tokio::test]
    async fn default_get_open_orders_is_empty() {
        let a = DefaultAdapter;
        assert!(
            a.get_open_orders(&Symbol::from("X"))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(!a.supports(Capability::OrderTracking));
    }

    #[tokio::test]
    async fn default_cancel_order_errors() {
        let a = DefaultAdapter;
        assert!(a.cancel_order(&Symbol::from("X"), "id").await.is_err());
    }
}
