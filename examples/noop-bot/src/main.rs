//! The smallest possible rustrade Bot.
//!
//! Wires a `NoopBrain` (always returns `Decision::hold`) and a mock
//! `ExchangeClient` that records — but does not actually place — orders.
//! Runs for the duration given by the first command-line argument
//! (default 10 seconds), then shuts down via the `BotHandle`.
//!
//! Run:
//! ```sh
//! cargo run -p noop-bot                # 10 s
//! cargo run -p noop-bot -- 3           # 3 s
//! ```
//!
//! Expected: no orders placed, "rustrade Bot exited" log line on exit.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use rustrade::{
    Bot, BotConfig, Brain, Decision, ExchangeClient, MarketDataEvent, Order, Position, Result,
    Symbol,
};

struct NoopBrain;

#[async_trait]
impl Brain for NoopBrain {
    fn name(&self) -> &str {
        "noop"
    }
    async fn on_event(&self, _e: &MarketDataEvent, _p: &Position) -> Result<Decision> {
        Ok(Decision::hold())
    }
}

/// Mock exchange — records every call but always returns `FLAT` / `Ok`.
struct MockExchange {
    orders_placed: AtomicUsize,
}
impl MockExchange {
    fn new() -> Self {
        Self {
            orders_placed: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl ExchangeClient for MockExchange {
    fn name(&self) -> &str {
        "mock"
    }
    async fn place_order(&self, _order: &Order) -> Result<String> {
        let n = self.orders_placed.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(format!("mock-{n}"))
    }
    async fn cancel_all(&self, _symbol: &Symbol) -> Result<usize> {
        Ok(0)
    }
    async fn close_position(&self, _symbol: &Symbol, _pos: &Position) -> Result<String> {
        Ok("mock-close".into())
    }
    async fn get_position(&self, _symbol: &Symbol) -> Result<Position> {
        Ok(Position::FLAT)
    }
    async fn get_balance(&self, _ccy: &str) -> Result<f64> {
        Ok(0.0)
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    rustrade::logging::init_tracing();

    let duration_secs = std::env::args()
        .nth(1)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(10);

    let exchange = Arc::new(MockExchange::new());
    let bot = Bot::new(
        BotConfig::builder()
            .name("noop-bot")
            .symbol("BTCUSDT")
            // The host drives shutdown — no signal handler needed.
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(5))
            .build()?,
        exchange.clone(),
        vec![Arc::new(NoopBrain)],
    )?;

    let handle = bot.handle();
    let bot_task = tokio::spawn(async move { bot.run_until_shutdown().await });

    tracing::info!(duration_secs, "noop-bot running");
    tokio::time::sleep(Duration::from_secs(duration_secs)).await;

    handle.shutdown();
    bot_task.await??;

    let placed = exchange.orders_placed.load(Ordering::SeqCst);
    tracing::info!(orders_placed = placed, "noop-bot finished");
    assert_eq!(placed, 0, "NoopBrain should never trigger an order");
    Ok(())
}
