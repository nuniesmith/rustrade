//! End-to-end integration tests for the `Bot` runtime.
//!
//! These run against the public `rustrade` API only — same surface
//! downstream services depend on. If something here breaks, downstream
//! integration is broken.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use rustrade::{
    Bot, BotConfig, Brain, Candle, Decision, Exchange, ExchangeClient, MarketDataEvent, Order,
    Position, Result, Symbol,
};

// ── Fixtures ────────────────────────────────────────────────────────────

struct CountingBrain {
    events: Arc<AtomicU64>,
}

impl CountingBrain {
    fn new() -> (Arc<Self>, Arc<AtomicU64>) {
        let events = Arc::new(AtomicU64::new(0));
        let brain = Arc::new(Self {
            events: events.clone(),
        });
        (brain, events)
    }
}

#[async_trait]
impl Brain for CountingBrain {
    fn name(&self) -> &str {
        "counting"
    }
    async fn on_event(&self, _e: &MarketDataEvent, _p: &Position) -> Result<Decision> {
        self.events.fetch_add(1, Ordering::SeqCst);
        Ok(Decision::hold())
    }
}

struct StubExchange;
#[async_trait]
impl ExchangeClient for StubExchange {
    fn name(&self) -> &str {
        "stub"
    }
    async fn place_order(&self, _o: &Order) -> Result<String> {
        Ok("stub-1".into())
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

fn candle_event(symbol: &str) -> MarketDataEvent {
    MarketDataEvent::Candle {
        exchange: Exchange::from("test"),
        symbol: Symbol::from(symbol),
        candle: Candle {
            time: Utc::now().timestamp_millis(),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: 1.0,
        },
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn bot_routes_market_events_to_brain_and_drains_cleanly() {
    let (brain, event_count) = CountingBrain::new();

    let bot = Bot::new(
        BotConfig::builder()
            .name("integration")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .build()
            .unwrap(),
        Arc::new(StubExchange),
        vec![brain.clone()],
    )
    .unwrap();

    let handle = bot.handle();
    let bus = bot.market_data_bus().clone();

    // Drive the bot in a task; meanwhile publish events and trigger
    // shutdown from the test thread.
    let bot_task = tokio::spawn(async move { bot.run_until_shutdown().await });

    // Give the ExecutionService a beat to subscribe before publishing.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let published = bus.publish(candle_event("BTCUSDT"));
    assert!(
        published >= 1,
        "execution service should have subscribed to the bus"
    );
    bus.publish(candle_event("BTCUSDT"));
    bus.publish(candle_event("BTCUSDT"));

    // Wait for the brain to see all three events (with a deadline).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if event_count.load(Ordering::SeqCst) >= 3 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "brain only saw {} of 3 events",
                event_count.load(Ordering::SeqCst)
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert!(!handle.is_shutting_down());
    handle.shutdown();
    assert!(handle.is_shutting_down());

    let result = tokio::time::timeout(Duration::from_secs(3), bot_task)
        .await
        .expect("bot did not exit within timeout")
        .expect("bot task panicked");
    assert!(result.is_ok(), "bot returned error: {result:?}");
}

#[tokio::test]
async fn bot_handle_health_reports_running_service() {
    let (brain, _) = CountingBrain::new();
    let bot = Bot::new(
        BotConfig::builder()
            .name("health-test")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .build()
            .unwrap(),
        Arc::new(StubExchange),
        vec![brain],
    )
    .unwrap();

    let handle = bot.handle();
    let bot_task = tokio::spawn(async move { bot.run_until_shutdown().await });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let health = handle.health().await;
    assert!(
        health.healthy,
        "should be healthy while running: {health:?}"
    );
    assert!(!health.shutting_down);
    assert_eq!(health.brains.len(), 1);
    assert_eq!(health.brains[0].name, "counting");
    assert!(
        health
            .services
            .iter()
            .any(|s| s.service_name.starts_with("execution[")),
        "execution service should be registered: {:?}",
        health.services
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), bot_task).await;
}

#[tokio::test]
async fn external_shutdown_via_handle_clone_drains_bot() {
    let (brain, _) = CountingBrain::new();
    let bot = Bot::new(
        BotConfig::builder()
            .name("ext-shutdown")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .build()
            .unwrap(),
        Arc::new(StubExchange),
        vec![brain],
    )
    .unwrap();

    let handle_a = bot.handle();
    let handle_b = handle_a.clone();

    let bot_task = tokio::spawn(async move { bot.run_until_shutdown().await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Use one clone to await, another to trigger.
    let awaiter = tokio::spawn(async move {
        handle_a.await_shutdown().await;
    });
    handle_b.shutdown();

    let _ = tokio::time::timeout(Duration::from_secs(2), awaiter)
        .await
        .expect("await_shutdown did not resolve");

    let _ = tokio::time::timeout(Duration::from_secs(3), bot_task)
        .await
        .expect("bot did not drain after handle shutdown");
}
