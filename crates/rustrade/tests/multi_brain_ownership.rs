//! Multi-brain arbitration (0.2d): `Brain::owned_symbols` routing + the
//! startup overlap guard, exercised end-to-end through the public `Bot` API.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use rustrade::{
    Bot, BotConfig, Brain, Candle, Decision, Exchange, ExchangeClient, MarketDataEvent, Order,
    Position, Result, SizingConfig, Symbol,
};

/// Buys on every event it's given. Owns exactly the symbols it's told to.
struct OwningBuyer {
    name: &'static str,
    owns: Option<Vec<Symbol>>,
}
#[async_trait]
impl Brain for OwningBuyer {
    fn name(&self) -> &str {
        self.name
    }
    fn owned_symbols(&self) -> Option<Vec<Symbol>> {
        self.owns.clone()
    }
    async fn on_event(&self, _e: &MarketDataEvent, _p: &Position) -> Result<Decision> {
        Ok(Decision::buy(1.0))
    }
}

/// Captures the symbols of every placed order.
struct CapturingExchange {
    order_symbols: Arc<Mutex<Vec<String>>>,
}
impl CapturingExchange {
    fn new() -> (Arc<Self>, Arc<Mutex<Vec<String>>>) {
        let order_symbols = Arc::new(Mutex::new(Vec::new()));
        (
            Arc::new(Self {
                order_symbols: order_symbols.clone(),
            }),
            order_symbols,
        )
    }
}
#[async_trait]
impl ExchangeClient for CapturingExchange {
    fn name(&self) -> &str {
        "capturing"
    }
    async fn place_order(&self, o: &Order) -> Result<String> {
        self.order_symbols
            .lock()
            .unwrap()
            .push(o.symbol.as_str().to_string());
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

fn candle_event(symbol: &str, close: f64) -> MarketDataEvent {
    MarketDataEvent::Candle {
        exchange: Exchange::from("test"),
        symbol: Symbol::from(symbol),
        candle: Candle {
            time: 0,
            open: close,
            high: close,
            low: close,
            close,
            volume: 1.0,
        },
    }
}

async fn eventually<F: FnMut() -> bool>(secs: u64, mut cond: F) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if cond() {
            return true;
        }
        if tokio::time::Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn owned_symbols_routes_only_owned_events_to_brain() {
    // One brain owns BTCUSDT; the bot also trades ETHUSDT. The brain must
    // only ever place BTC orders — ETH events are filtered out before
    // on_event, even though the brain naively buys on everything.
    let (exchange, order_symbols) = CapturingExchange::new();
    let bot = Bot::new(
        BotConfig::builder()
            .name("owned")
            .symbols(["BTCUSDT", "ETHUSDT"])
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .sizing_config(SizingConfig {
                margin_per_trade: 100.0,
                leverage: 1,
                max_contracts: 10,
            })
            .build()
            .unwrap(),
        exchange,
        vec![Arc::new(OwningBuyer {
            name: "btc-only",
            owns: Some(vec![Symbol::from("BTCUSDT")]),
        })],
    )
    .unwrap();

    let bus = bot.market_data_bus().clone();
    let handle = bot.handle();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });

    // Publish BTC until one order lands (proves the brain is live), then a
    // burst of ETH events that must be ignored.
    let got_btc = eventually(10, || {
        if order_symbols.lock().unwrap().iter().any(|s| s == "BTCUSDT") {
            true
        } else {
            bus.publish(candle_event("BTCUSDT", 100.0));
            false
        }
    })
    .await;
    assert!(got_btc, "brain should place a BTC order");

    for _ in 0..5 {
        bus.publish(candle_event("ETHUSDT", 200.0));
    }
    // Let the ETH events drain through the execution loop.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let symbols = order_symbols.lock().unwrap().clone();
    assert!(
        symbols.iter().all(|s| s == "BTCUSDT"),
        "brain owning BTCUSDT must never place ETH orders, got {symbols:?}"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn overlapping_ownership_is_rejected_at_construction() {
    let cfg = BotConfig::builder()
        .name("dup")
        .symbol("BTCUSDT")
        .without_signal_handler()
        .build()
        .unwrap();
    let (exchange, _) = CapturingExchange::new();
    let result = Bot::new(
        cfg,
        exchange,
        vec![
            Arc::new(OwningBuyer {
                name: "a",
                owns: Some(vec![Symbol::from("BTCUSDT")]),
            }),
            Arc::new(OwningBuyer {
                name: "b",
                owns: Some(vec![Symbol::from("BTCUSDT")]),
            }),
        ],
    );
    assert!(
        result.is_err(),
        "two brains owning BTCUSDT must be rejected at Bot::new"
    );
}
