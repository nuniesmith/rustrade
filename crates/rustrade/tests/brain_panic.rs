//! A panicking `Brain` must not take down the whole `Bot`.
//!
//! Each brain runs in its own supervised `ExecutionService` task, and the
//! execution path releases the shared position lock *before* calling
//! `Brain::on_event`. So a brain that panics inside `on_event` unwinds
//! only its own task — sibling brains keep processing and the bot still
//! shuts down cleanly.
//!
//! # Caveat: this isolation holds under `panic = "unwind"` (the default
//! for `dev`/`test`). The release profile sets `panic = "abort"`, under
//! which *any* panic aborts the whole process — so brains should treat a
//! panic as a hard bug and return `Err` for recoverable conditions
//! instead. This test runs in the test profile and verifies the
//! task-boundary containment that unwind builds provide.
//!
//! Running under `#[tokio::test(start_paused = true)]` keeps it
//! deterministic — see the `wait_for` rationale in `phase_2d.rs`.
//!
//! NOTE: this test deliberately triggers a panic in a background task.
//! With CI's `--nocapture`, the panic's backtrace prints to the log; that
//! is expected output, not a failure.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use rustrade::{
    Bot, BotConfig, Brain, BrainHealth, Candle, Decision, Exchange, ExchangeClient,
    MarketDataEvent, Order, Position, Result, Symbol,
};

// ── Fixtures ─────────────────────────────────────────────────────────────

struct StubExchange;
#[async_trait]
impl ExchangeClient for StubExchange {
    fn name(&self) -> &str {
        "stub"
    }
    async fn place_order(&self, _o: &Order) -> Result<String> {
        Ok("ok".into())
    }
    async fn cancel_all(&self, _s: &Symbol) -> Result<usize> {
        Ok(0)
    }
    async fn close_position(&self, _s: &Symbol, _p: &Position) -> Result<String> {
        Ok("close".into())
    }
    async fn get_position(&self, _s: &Symbol) -> Result<Position> {
        Ok(Position::FLAT)
    }
    async fn get_balance(&self, _c: &str) -> Result<f64> {
        Ok(0.0)
    }
}

/// Panics the first time it sees a candle event.
struct PanicBrain;
#[async_trait]
impl Brain for PanicBrain {
    fn name(&self) -> &str {
        "panic"
    }
    async fn on_event(&self, _e: &MarketDataEvent, _p: &Position) -> Result<Decision> {
        panic!("INTENTIONAL TEST PANIC: brain isolation check");
    }
    async fn health(&self) -> BrainHealth {
        BrainHealth::ok()
    }
}

/// Counts every event it sees; never panics.
struct CountingBrain {
    seen: Arc<AtomicU64>,
}
#[async_trait]
impl Brain for CountingBrain {
    fn name(&self) -> &str {
        "counting"
    }
    async fn on_event(&self, _e: &MarketDataEvent, _p: &Position) -> Result<Decision> {
        self.seen.fetch_add(1, Ordering::SeqCst);
        Ok(Decision::hold())
    }
    async fn health(&self) -> BrainHealth {
        BrainHealth::ok()
    }
}

fn candle_event(symbol: &str) -> MarketDataEvent {
    MarketDataEvent::Candle {
        exchange: Exchange::from("test"),
        symbol: Symbol::from(symbol),
        candle: Candle {
            time: 1,
            open: 100.0,
            high: 100.0,
            low: 100.0,
            close: 100.0,
            volume: 1.0,
        },
    }
}

async fn wait_for<F: FnMut() -> bool>(mut predicate: F, what: &str) {
    let poll = async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(30), poll)
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for: {what}"));
}

// ── Test ─────────────────────────────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn panicking_brain_is_isolated_and_bot_shuts_down_cleanly() {
    let seen = Arc::new(AtomicU64::new(0));
    let counting = Arc::new(CountingBrain { seen: seen.clone() });

    let bot = Bot::new(
        BotConfig::builder()
            .name("panic-isolation")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .build()
            .unwrap(),
        Arc::new(StubExchange),
        // Panic brain first so it's clearly not the "last" service.
        vec![Arc::new(PanicBrain), counting],
    )
    .unwrap();

    let bus = bot.market_data_bus().clone();
    let handle = bot.handle();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });

    // Let both ExecutionServices subscribe to the bus.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish three events. The panic brain's service unwinds on the
    // first one and its task dies; the counting brain's service is a
    // separate task and keeps processing all three.
    bus.publish(candle_event("BTCUSDT"));
    bus.publish(candle_event("BTCUSDT"));
    bus.publish(candle_event("BTCUSDT"));

    // If the panic had taken down the runtime or poisoned shared state,
    // the counting brain would never reach 3.
    wait_for(
        || seen.load(Ordering::SeqCst) >= 3,
        "counting brain to process 3 events despite sibling panic",
    )
    .await;

    // The bot is still alive and drains cleanly on shutdown.
    handle.shutdown();
    let joined = tokio::time::timeout(Duration::from_secs(30), task)
        .await
        .expect("bot did not drain after a sibling brain panicked");
    // The bot's own task must not have panicked — only the brain's
    // service task did, and that was contained.
    let run_result = joined.expect("the bot task itself panicked — isolation failed");
    assert!(run_result.is_ok(), "bot returned error: {run_result:?}");
}
