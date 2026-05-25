//! Exchange-agnostic market data primitives.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::{Candle, Tick};

/// Side of a trade or order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    /// Buying side — long entries and short exits.
    Buy,
    /// Selling side — short entries and long exits.
    Sell,
}

impl Side {
    /// The opposite side — used to construct closing orders.
    pub fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

/// Identifies an exchange-symbol pair.
///
/// `Symbol` is a thin wrapper over `String`. Symbols are exchange-specific
/// (e.g. `"XBTUSDTM"` on KuCoin, `"BTCUSDT"` on Binance) so the framework
/// stays string-typed rather than pretending to enumerate them, but the
/// newtype prevents accidental confusion with other free-form `String`
/// fields (URLs, account ids, error messages, …).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Symbol(pub String);

impl Symbol {
    /// Construct a new `Symbol`.
    #[inline]
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Borrow the underlying string slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Symbol {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for Symbol {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl AsRef<str> for Symbol {
    #[inline]
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::borrow::Borrow<str> for Symbol {
    #[inline]
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for Symbol {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for Symbol {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Opaque identifier for which exchange produced this data.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Exchange(pub String);

impl From<&str> for Exchange {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// A normalized market-data event that can come from any exchange.
///
/// Exchange adapters parse their native formats into this enum so the
/// downstream brain and risk layers are exchange-agnostic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MarketDataEvent {
    /// Best-bid/best-ask update.
    Ticker {
        /// Exchange this ticker came from.
        exchange: Exchange,
        /// Symbol the ticker is for.
        symbol: Symbol,
        /// The tick itself.
        tick: Tick,
    },
    /// A completed candle (fully closed bar).
    Candle {
        /// Exchange this candle came from.
        exchange: Exchange,
        /// Symbol the candle is for.
        symbol: Symbol,
        /// The OHLCV candle.
        candle: Candle,
    },
    /// An individual trade print.
    Trade {
        /// Exchange this trade was reported by.
        exchange: Exchange,
        /// Symbol the trade was for.
        symbol: Symbol,
        /// Aggressor side of the trade.
        side: Side,
        /// Trade price in quote currency.
        price: f64,
        /// Trade size in base-asset units or contracts.
        size: f64,
        /// Time the trade occurred.
        timestamp: DateTime<Utc>,
    },
}

impl MarketDataEvent {
    /// Borrow the event's [`Symbol`] regardless of variant.
    pub fn symbol(&self) -> &Symbol {
        match self {
            Self::Ticker { symbol, .. }
            | Self::Candle { symbol, .. }
            | Self::Trade { symbol, .. } => symbol,
        }
    }

    /// Borrow the event's source [`Exchange`] regardless of variant.
    pub fn exchange(&self) -> &Exchange {
        match self {
            Self::Ticker { exchange, .. }
            | Self::Candle { exchange, .. }
            | Self::Trade { exchange, .. } => exchange,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Candle, Price, Tick, Volume};
    use chrono::TimeZone;

    #[test]
    fn side_opposite_is_involutive() {
        assert_eq!(Side::Buy.opposite(), Side::Sell);
        assert_eq!(Side::Sell.opposite(), Side::Buy);
        assert_eq!(Side::Buy.opposite().opposite(), Side::Buy);
        assert_eq!(Side::Sell.opposite().opposite(), Side::Sell);
    }

    #[test]
    fn symbol_as_str_borrow_and_display() {
        let s = Symbol::new("BTCUSDT");
        assert_eq!(s.as_str(), "BTCUSDT");
        assert_eq!(s.as_ref(), "BTCUSDT");
        assert_eq!(format!("{s}"), "BTCUSDT");

        // PartialEq<&str> for ergonomic comparison.
        assert_eq!(s, "BTCUSDT");
        assert_ne!(s, "ETHUSDT");

        // Borrow<str> so HashMap<Symbol, _> can be looked up by &str.
        use std::collections::HashMap;
        let mut m: HashMap<Symbol, i32> = HashMap::new();
        m.insert(Symbol::new("BTCUSDT"), 1);
        assert_eq!(m.get("BTCUSDT").copied(), Some(1));
    }

    #[test]
    fn symbol_serde_transparent() {
        let s = Symbol::new("ETHUSDT");
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(json, "\"ETHUSDT\"");
        let back: Symbol = serde_json::from_str(&json).unwrap();
        assert_eq!(back, s);
    }

    fn ev_ticker() -> MarketDataEvent {
        MarketDataEvent::Ticker {
            exchange: Exchange::from("kucoin"),
            symbol: Symbol::new("XBTUSDTM"),
            tick: Tick {
                symbol: Symbol::new("XBTUSDTM"),
                timestamp: Utc.timestamp_opt(0, 0).unwrap(),
                bid: Price(1.0),
                ask: Price(1.0),
                bid_size: Volume(1.0),
                ask_size: Volume(1.0),
                last_price: None,
                last_size: None,
            },
        }
    }

    fn ev_candle() -> MarketDataEvent {
        MarketDataEvent::Candle {
            exchange: Exchange::from("binance"),
            symbol: Symbol::new("BTCUSDT"),
            candle: Candle {
                time: 0,
                open: 1.0,
                high: 1.0,
                low: 1.0,
                close: 1.0,
                volume: 1.0,
            },
        }
    }

    fn ev_trade() -> MarketDataEvent {
        MarketDataEvent::Trade {
            exchange: Exchange::from("bybit"),
            symbol: Symbol::new("ETHUSDT"),
            side: Side::Buy,
            price: 1.0,
            size: 1.0,
            timestamp: Utc.timestamp_opt(0, 0).unwrap(),
        }
    }

    #[test]
    fn market_data_event_accessors_cover_all_variants() {
        let t = ev_ticker();
        assert_eq!(t.symbol(), &Symbol::new("XBTUSDTM"));
        assert_eq!(t.exchange(), &Exchange::from("kucoin"));

        let c = ev_candle();
        assert_eq!(c.symbol(), &Symbol::new("BTCUSDT"));
        assert_eq!(c.exchange(), &Exchange::from("binance"));

        let tr = ev_trade();
        assert_eq!(tr.symbol(), &Symbol::new("ETHUSDT"));
        assert_eq!(tr.exchange(), &Exchange::from("bybit"));
    }
}
