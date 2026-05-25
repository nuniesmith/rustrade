//! Domain types using the newtype pattern to prevent unit-confusion bugs.
//!
//! Every quantity that has units (price, volume, notional) gets its own
//! wrapper type. This costs a few lines of code now and saves hours of
//! debugging later.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::market::{Side, Symbol};

// ── Scalar wrappers ──────────────────────────────────────────────────────────

/// Price in the quote currency (e.g. USD / USDT).
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct Price(pub f64);

impl Price {
    pub const ZERO: Self = Self(0.0);

    #[inline]
    pub const fn new(v: f64) -> Self {
        Self(v)
    }
    #[inline]
    pub const fn value(self) -> f64 {
        self.0
    }
}

impl fmt::Display for Price {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Volume / quantity in base-asset units (e.g. BTC, ETH) or in contracts.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct Volume(pub f64);

impl Volume {
    pub const ZERO: Self = Self(0.0);

    #[inline]
    pub const fn new(v: f64) -> Self {
        Self(v)
    }
    #[inline]
    pub const fn value(self) -> f64 {
        self.0
    }
}

impl fmt::Display for Volume {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ── Market data ──────────────────────────────────────────────────────────────

/// A single trade tick or best-bid/best-ask snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tick {
    pub symbol: Symbol,
    pub timestamp: DateTime<Utc>,
    pub bid: Price,
    pub ask: Price,
    pub bid_size: Volume,
    pub ask_size: Volume,
    pub last_price: Option<Price>,
    pub last_size: Option<Volume>,
}

impl Tick {
    pub fn mid_price(&self) -> Price {
        Price((self.bid.0 + self.ask.0) / 2.0)
    }

    pub fn spread(&self) -> Price {
        Price(self.ask.0 - self.bid.0)
    }
}

/// OHLCV candle — the atomic unit of batched market data.
///
/// `time` is the open time of the candle in milliseconds since the UNIX epoch.
/// Stored as `i64` (not `f64`) to avoid precision loss at millisecond granularity.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Candle {
    pub time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

// ── Orders and fills ─────────────────────────────────────────────────────────

/// Order kind (market vs limit and their time-in-force variants).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderKind {
    Market,
    Limit,
    /// Post-only limit (rejected if it would cross the book as taker).
    PostOnly,
    /// Immediate-or-cancel — fill what you can now, cancel the rest.
    Ioc,
    /// Fill-or-kill — fill completely at the given price or cancel entirely.
    Fok,
}

/// What kind of stop attachment an [`Order`] carries.
///
/// Opaque to the framework — the adapter is responsible for translating
/// these into native exchange semantics (e.g. KuCoin futures stops vs
/// Binance OCO orders vs Bybit conditional orders). Adapters that don't
/// support a given variant should reject the order with a clear error
/// rather than silently ignoring the attachment.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StopKind {
    /// Trigger a market order at `trigger_price`.
    StopMarket,
    /// Trigger a limit order at `trigger_price`, posted at `limit_price`.
    StopLimit { limit_price: Price },
    /// Take-profit market order at `trigger_price`.
    TakeProfit,
    /// Trailing stop with the given trail distance in quote currency.
    TrailingStop { trail_distance: Price },
}

/// Stop-order attachment on an [`Order`].
///
/// See [`StopKind`] for variants. Use
/// [`ExchangeClient::supports`](crate::ExchangeClient::supports) with
/// [`Capability::StopOrders`](crate::Capability::StopOrders) to check
/// adapter capability before constructing one.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct StopAttachment {
    /// Price at which the exchange triggers the stop.
    pub trigger_price: Price,
    /// What kind of stop to fire on trigger.
    pub kind: StopKind,
}

impl StopAttachment {
    /// Convenience: stop-market at `trigger_price`.
    pub fn stop_market(trigger_price: Price) -> Self {
        Self {
            trigger_price,
            kind: StopKind::StopMarket,
        }
    }

    /// Convenience: stop-limit triggered at `trigger_price`, posted at `limit_price`.
    pub fn stop_limit(trigger_price: Price, limit_price: Price) -> Self {
        Self {
            trigger_price,
            kind: StopKind::StopLimit { limit_price },
        }
    }

    /// Convenience: take-profit market order at `trigger_price`.
    pub fn take_profit(trigger_price: Price) -> Self {
        Self {
            trigger_price,
            kind: StopKind::TakeProfit,
        }
    }

    /// Convenience: trailing stop with the given trail distance.
    pub fn trailing(trigger_price: Price, trail_distance: Price) -> Self {
        Self {
            trigger_price,
            kind: StopKind::TrailingStop { trail_distance },
        }
    }
}

/// A request to enter, exit, or reduce a position.
///
/// This is the framework-level abstraction; concrete exchange adapters translate
/// it into exchange-specific payloads. The `client_id` is optional but strongly
/// recommended — it lets the framework reconcile fills back to this order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub symbol: Symbol,
    pub side: Side,
    pub kind: OrderKind,
    pub size: Volume,
    /// Limit price for non-market orders.
    pub limit_price: Option<Price>,
    /// Set to `true` for exit orders that must never increase the position.
    pub reduce_only: bool,
    /// Optional client-supplied id. Exchanges that support it will echo it back
    /// on fills, making reconciliation trivial.
    pub client_id: Option<String>,
    /// Optional stop-order attachment. Adapters that don't advertise
    /// [`Capability::StopOrders`](crate::Capability::StopOrders) must
    /// reject orders carrying this field.
    #[serde(default)]
    pub stop: Option<StopAttachment>,
}

impl Order {
    pub fn market(symbol: impl Into<Symbol>, side: Side, size: Volume) -> Self {
        Self {
            symbol: symbol.into(),
            side,
            kind: OrderKind::Market,
            size,
            limit_price: None,
            reduce_only: false,
            client_id: None,
            stop: None,
        }
    }

    pub fn limit(symbol: impl Into<Symbol>, side: Side, size: Volume, price: Price) -> Self {
        Self {
            symbol: symbol.into(),
            side,
            kind: OrderKind::Limit,
            size,
            limit_price: Some(price),
            reduce_only: false,
            client_id: None,
            stop: None,
        }
    }

    pub fn with_reduce_only(mut self, reduce_only: bool) -> Self {
        self.reduce_only = reduce_only;
        self
    }

    pub fn with_client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = Some(id.into());
        self
    }

    /// Attach a stop. Adapter must advertise
    /// [`Capability::StopOrders`](crate::Capability::StopOrders).
    pub fn with_stop(mut self, stop: StopAttachment) -> Self {
        self.stop = Some(stop);
        self
    }
}

/// A trade fill reported by the exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    pub symbol: Symbol,
    pub order_id: String,
    pub client_id: Option<String>,
    pub side: Side,
    pub price: Price,
    pub size: Volume,
    pub fee: f64,
    pub fee_currency: String,
    pub timestamp: DateTime<Utc>,
}

// ── Position ─────────────────────────────────────────────────────────────────

/// Current exchange-reported position for a single symbol.
///
/// `qty` is signed: positive = long, negative = short, zero = flat.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Default)]
pub struct Position {
    pub qty: f64,
    pub entry_price: Option<f64>,
    pub unrealised_pnl: f64,
}

impl Position {
    pub const FLAT: Self = Self {
        qty: 0.0,
        entry_price: None,
        unrealised_pnl: 0.0,
    };

    #[inline]
    pub fn is_flat(&self) -> bool {
        self.qty == 0.0
    }

    #[inline]
    pub fn is_long(&self) -> bool {
        self.qty > 0.0
    }

    #[inline]
    pub fn is_short(&self) -> bool {
        self.qty < 0.0
    }

    /// Side needed to fully close this position (None if flat).
    pub fn close_side(&self) -> Option<Side> {
        if self.qty > 0.0 {
            Some(Side::Sell)
        } else if self.qty < 0.0 {
            Some(Side::Buy)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn tick(bid: f64, ask: f64) -> Tick {
        Tick {
            symbol: Symbol::new("BTCUSDT"),
            timestamp: Utc.timestamp_opt(0, 0).unwrap(),
            bid: Price(bid),
            ask: Price(ask),
            bid_size: Volume(1.0),
            ask_size: Volume(1.0),
            last_price: None,
            last_size: None,
        }
    }

    #[test]
    fn tick_mid_price_and_spread() {
        let t = tick(100.0, 102.0);
        assert!((t.mid_price().value() - 101.0).abs() < 1e-9);
        assert!((t.spread().value() - 2.0).abs() < 1e-9);
    }

    #[test]
    fn tick_mid_handles_zero_spread() {
        let t = tick(100.0, 100.0);
        assert_eq!(t.mid_price().value(), 100.0);
        assert_eq!(t.spread().value(), 0.0);
    }

    #[test]
    fn position_close_side_long() {
        let p = Position {
            qty: 5.0,
            ..Position::FLAT
        };
        assert!(p.is_long());
        assert!(!p.is_short());
        assert!(!p.is_flat());
        assert_eq!(p.close_side(), Some(Side::Sell));
    }

    #[test]
    fn position_close_side_short() {
        let p = Position {
            qty: -5.0,
            ..Position::FLAT
        };
        assert!(p.is_short());
        assert!(!p.is_long());
        assert_eq!(p.close_side(), Some(Side::Buy));
    }

    #[test]
    fn position_close_side_flat() {
        assert_eq!(Position::FLAT.close_side(), None);
        assert!(Position::FLAT.is_flat());
    }

    #[test]
    fn order_market_builder() {
        let o = Order::market("BTCUSDT", Side::Buy, Volume(1.0));
        assert_eq!(o.symbol, Symbol::new("BTCUSDT"));
        assert_eq!(o.side, Side::Buy);
        assert_eq!(o.kind, OrderKind::Market);
        assert!(o.limit_price.is_none());
        assert!(!o.reduce_only);
        assert!(o.client_id.is_none());
        assert!(o.stop.is_none());
    }

    #[test]
    fn order_limit_builder() {
        let o = Order::limit("BTCUSDT", Side::Sell, Volume(2.0), Price(50_000.0));
        assert_eq!(o.kind, OrderKind::Limit);
        assert_eq!(o.limit_price, Some(Price(50_000.0)));
    }

    #[test]
    fn order_with_reduce_only() {
        let o = Order::market("X", Side::Buy, Volume(1.0)).with_reduce_only(true);
        assert!(o.reduce_only);
    }

    #[test]
    fn order_with_client_id() {
        let o = Order::market("X", Side::Buy, Volume(1.0)).with_client_id("abc-123");
        assert_eq!(o.client_id.as_deref(), Some("abc-123"));
    }

    #[test]
    fn order_with_stop_market() {
        let o = Order::market("X", Side::Sell, Volume(1.0))
            .with_reduce_only(true)
            .with_stop(StopAttachment::stop_market(Price(95.0)));
        assert!(o.reduce_only);
        let stop = o.stop.unwrap();
        assert_eq!(stop.trigger_price, Price(95.0));
        assert!(matches!(stop.kind, StopKind::StopMarket));
    }

    #[test]
    fn order_with_stop_limit() {
        let s = StopAttachment::stop_limit(Price(95.0), Price(94.5));
        assert_eq!(s.trigger_price, Price(95.0));
        match s.kind {
            StopKind::StopLimit { limit_price } => assert_eq!(limit_price, Price(94.5)),
            other => panic!("unexpected stop kind: {other:?}"),
        }
    }

    #[test]
    fn stop_attachment_take_profit() {
        let s = StopAttachment::take_profit(Price(110.0));
        assert!(matches!(s.kind, StopKind::TakeProfit));
    }

    #[test]
    fn stop_attachment_trailing() {
        let s = StopAttachment::trailing(Price(100.0), Price(2.5));
        match s.kind {
            StopKind::TrailingStop { trail_distance } => {
                assert_eq!(trail_distance, Price(2.5));
            }
            other => panic!("unexpected stop kind: {other:?}"),
        }
    }

    #[test]
    fn order_serde_roundtrip_without_stop() {
        let o = Order::market("BTCUSDT", Side::Buy, Volume(1.5));
        let json = serde_json::to_string(&o).unwrap();
        let back: Order = serde_json::from_str(&json).unwrap();
        assert_eq!(back.symbol, o.symbol);
        assert_eq!(back.side, o.side);
        assert_eq!(back.size, o.size);
        assert!(back.stop.is_none());
    }

    #[test]
    fn order_serde_roundtrip_with_stop() {
        let o = Order::market("X", Side::Sell, Volume(1.0))
            .with_stop(StopAttachment::trailing(Price(100.0), Price(2.5)));
        let json = serde_json::to_string(&o).unwrap();
        let back: Order = serde_json::from_str(&json).unwrap();
        assert_eq!(back.stop, o.stop);
    }
}
