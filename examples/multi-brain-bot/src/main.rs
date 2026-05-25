//! Two `Brain`s in one `Bot`, each filtering events to its own symbol.
//!
//! The framework runs every brain against every event. Symbol filtering
//! is a brain-internal concern — illustrated here with a tiny
//! `SymbolFilterBrain` that ignores events for symbols it doesn't own.
//!
//! Run:
//! ```sh
//! cargo run -p multi-brain-bot
//! ```
//!
//! Expected: each brain reports a count equal to the number of events
//! published for its symbol.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use rustrade::{
    Bot, BotConfig, Brain, Candle, Decision, Exchange, ExchangeClient, MarketDataEvent, Order,
    Position, Result, Symbol,
};

struct SymbolFilterBrain {
    name: String,
    target: Symbol,
    seen: Arc<AtomicUsize>,
}

impl SymbolFilterBrain {
    fn new(name: &str, target: impl Into<Symbol>) -> (Arc<Self>, Arc<AtomicUsize>) {
        let seen = Arc::new(AtomicUsize::new(0));
        (
            Arc::new(Self {
                name: name.to_string(),
                target: target.into(),
                seen: seen.clone(),
            }),
            seen,
        )
    }
}

#[async_trait]
impl Brain for SymbolFilterBrain {
    fn name(&self) -> &str {
        &self.name
    }
    async fn on_event(&self, event: &MarketDataEvent, _p: &Position) -> Result<Decision> {
        if event.symbol() != &self.target {
            // Not our symbol — silently ignore.
            return Ok(Decision::hold());
        }
        self.seen.fetch_add(1, Ordering::SeqCst);
        Ok(Decision::hold())
    }
}

/// Stub exchange — never asked to do anything in this example.
struct StubExchange;
#[async_trait]
impl ExchangeClient for StubExchange {
    fn name(&self) -> &str {
        "stub"
    }
    async fn place_order(&self, _o: &Order) -> Result<String> {
        Ok("stub".into())
    }
    async fn cancel_all(&self, _s: &Symbol) -> Result<usize> {
        Ok(0)
    }
    async fn close_position(&self, _s: &Symbol, _p: &Position) -> Result<String> {
        Ok("stub-close".into())
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
            time: Utc::now().timestamp_millis(),
            open: close,
            high: close,
            low: close,
            close,
            volume: 1.0,
        },
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    rustrade::logging::init_tracing();

    let (btc_brain, btc_count) = SymbolFilterBrain::new("btc-brain", "BTCUSDT");
    let (eth_brain, eth_count) = SymbolFilterBrain::new("eth-brain", "ETHUSDT");

    let bot = Bot::new(
        BotConfig::builder()
            .name("multi-brain-bot")
            .symbols(["BTCUSDT", "ETHUSDT"])
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .build()?,
        Arc::new(StubExchange),
        vec![btc_brain, eth_brain],
    )?;

    let handle = bot.handle();
    let bus = bot.market_data_bus().clone();

    let bot_task = tokio::spawn(async move { bot.run_until_shutdown().await });

    // Let services subscribe before publishing.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish a non-trivial mix: 3 BTC, 5 ETH.
    for close in [100.0, 101.0, 102.0] {
        bus.publish(candle_event("BTCUSDT", close));
    }
    for close in [2_000.0, 2_010.0, 2_020.0, 2_030.0, 2_040.0] {
        bus.publish(candle_event("ETHUSDT", close));
    }

    // Allow events to flow through both brains.
    tokio::time::sleep(Duration::from_millis(200)).await;

    handle.shutdown();
    bot_task.await??;

    let btc = btc_count.load(Ordering::SeqCst);
    let eth = eth_count.load(Ordering::SeqCst);
    tracing::info!(
        btc_events = btc,
        eth_events = eth,
        "multi-brain-bot finished"
    );

    assert_eq!(btc, 3, "btc-brain should have seen exactly 3 BTC events");
    assert_eq!(eth, 5, "eth-brain should have seen exactly 5 ETH events");
    Ok(())
}
