//! Integration tests for the Phase 2c additions:
//!
//! - External cancellation token wiring
//! - Signal subscription via `BotHandle::subscribe_signals`
//! - `MarketFeedService` wired by `Bot::with_market_source`
//! - `FillRoutingService` wired by `Bot::with_fill_source`

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use rustrade::{
    Bot, BotConfig, Brain, Candle, Decision, Exchange, ExchangeClient, Fill, FillSource,
    MarketDataBus, MarketDataEvent, MarketSource, Order, Position, Price, Result, Side, SignalType,
    Symbol, Volume,
};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;

// ── Fixtures ────────────────────────────────────────────────────────────

struct CountingExchange {
    placed: Arc<AtomicU64>,
    position_lookups: Arc<AtomicU64>,
    positions: Mutex<std::collections::HashMap<Symbol, Position>>,
}
impl CountingExchange {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            placed: Arc::new(AtomicU64::new(0)),
            position_lookups: Arc::new(AtomicU64::new(0)),
            positions: Mutex::new(std::collections::HashMap::new()),
        })
    }
    fn set_position(&self, sym: Symbol, pos: Position) {
        self.positions.lock().unwrap().insert(sym, pos);
    }
}
#[async_trait]
impl ExchangeClient for CountingExchange {
    fn name(&self) -> &str {
        "counting"
    }
    async fn place_order(&self, _o: &Order) -> Result<String> {
        let n = self.placed.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(format!("ord-{n}"))
    }
    async fn cancel_all(&self, _s: &Symbol) -> Result<usize> {
        Ok(0)
    }
    async fn close_position(&self, _s: &Symbol, _p: &Position) -> Result<String> {
        Ok("close".into())
    }
    async fn get_position(&self, s: &Symbol) -> Result<Position> {
        self.position_lookups.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .positions
            .lock()
            .unwrap()
            .get(s)
            .copied()
            .unwrap_or(Position::FLAT))
    }
    async fn get_balance(&self, _c: &str) -> Result<f64> {
        Ok(0.0)
    }
}

struct HoldBrain;
#[async_trait]
impl Brain for HoldBrain {
    fn name(&self) -> &str {
        "hold"
    }
    async fn on_event(&self, _e: &MarketDataEvent, _p: &Position) -> Result<Decision> {
        Ok(Decision::hold())
    }
}

struct BuyBrain;
#[async_trait]
impl Brain for BuyBrain {
    fn name(&self) -> &str {
        "buy"
    }
    async fn on_event(&self, _e: &MarketDataEvent, _p: &Position) -> Result<Decision> {
        Ok(Decision::buy(1.0))
    }
}

/// Brain that tracks every fill it sees.
struct FillRecordingBrain {
    seen: Arc<AsyncMutex<Vec<Fill>>>,
}
impl FillRecordingBrain {
    fn new() -> (Arc<Self>, Arc<AsyncMutex<Vec<Fill>>>) {
        let seen = Arc::new(AsyncMutex::new(Vec::new()));
        (Arc::new(Self { seen: seen.clone() }), seen)
    }
}
#[async_trait]
impl Brain for FillRecordingBrain {
    fn name(&self) -> &str {
        "fill-recorder"
    }
    async fn on_event(&self, _e: &MarketDataEvent, _p: &Position) -> Result<Decision> {
        Ok(Decision::hold())
    }
    async fn on_fill(&self, fill: &Fill) -> Result<()> {
        self.seen.lock().await.push(fill.clone());
        Ok(())
    }
}

/// Synthetic MarketSource: publishes one candle/sec to the bus it was
/// constructed with until its `run` future is dropped.
struct TickingMarketSource {
    bus: MarketDataBus,
    interval: Duration,
}
impl TickingMarketSource {
    fn new(bus: MarketDataBus, interval: Duration) -> Self {
        Self { bus, interval }
    }
}
#[async_trait]
impl MarketSource for TickingMarketSource {
    fn name(&self) -> &str {
        "tick-source"
    }
    fn is_live(&self) -> bool {
        true
    }
    async fn run(&self) -> Result<()> {
        loop {
            tokio::time::sleep(self.interval).await;
            self.bus.publish(candle_event("BTCUSDT", 100.0));
        }
    }
}

/// FillSource backed by an mpsc channel so tests can push fills at will.
struct ChannelFillSource {
    rx: AsyncMutex<tokio::sync::mpsc::UnboundedReceiver<Fill>>,
}
impl ChannelFillSource {
    fn new() -> (Arc<Self>, tokio::sync::mpsc::UnboundedSender<Fill>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (
            Arc::new(Self {
                rx: AsyncMutex::new(rx),
            }),
            tx,
        )
    }
}
#[async_trait]
impl FillSource for ChannelFillSource {
    async fn next_fill(&self) -> Option<Fill> {
        self.rx.lock().await.recv().await
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

fn make_fill(symbol: &str, side: Side, qty: f64) -> Fill {
    Fill {
        symbol: Symbol::from(symbol),
        order_id: "ord-x".into(),
        client_id: None,
        side,
        price: Price(100.0),
        size: Volume(qty),
        fee: 0.1,
        fee_currency: "USDT".into(),
        timestamp: Utc::now(),
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn external_cancel_triggers_bot_shutdown() {
    let bot = Bot::new(
        BotConfig::builder()
            .name("ext-cancel")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .build()
            .unwrap(),
        CountingExchange::new(),
        vec![Arc::new(HoldBrain)],
    )
    .unwrap();

    let external = CancellationToken::new();
    let bot = bot.with_external_cancel(external.clone());
    let handle = bot.handle();

    let task = tokio::spawn(async move { bot.run_until_shutdown().await });

    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(!handle.is_shutting_down());

    // Cancel via the external token (not handle.shutdown).
    external.cancel();

    let _ = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("bot did not exit after external cancel");
    assert!(handle.is_shutting_down());
}

#[tokio::test(start_paused = true)]
async fn signal_subscribers_see_non_hold_decisions() {
    let bot = Bot::new(
        BotConfig::builder()
            .name("signal-sub")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .build()
            .unwrap(),
        CountingExchange::new(),
        vec![Arc::new(BuyBrain)],
    )
    .unwrap();

    let handle = bot.handle();
    let mut sub = handle.subscribe_signals();
    let bus = bot.market_data_bus().clone();

    let task = tokio::spawn(async move { bot.run_until_shutdown().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(handle.signal_subscriber_count() >= 1);

    bus.publish(candle_event("BTCUSDT", 100.0));

    let signal = tokio::time::timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("no signal arrived")
        .expect("signal channel closed");
    assert!(matches!(signal.kind, SignalType::Buy));
    assert_eq!(signal.symbol, "BTCUSDT");
    assert_eq!(signal.source, "buy");

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}

#[tokio::test(start_paused = true)]
async fn market_feed_service_runs_until_shutdown() {
    let bot = Bot::new(
        BotConfig::builder()
            .name("market-feed")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .build()
            .unwrap(),
        CountingExchange::new(),
        vec![Arc::new(HoldBrain)],
    )
    .unwrap();

    let source = Arc::new(TickingMarketSource::new(
        bot.market_data_bus().clone(),
        Duration::from_millis(50),
    ));
    let bot = bot.with_market_source(source);

    let handle = bot.handle();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });

    // Give the source time to publish at least a couple of candles.
    tokio::time::sleep(Duration::from_millis(250)).await;

    // The source service should be registered with the supervisor.
    let health = handle.health().await;
    assert!(
        health
            .services
            .iter()
            .any(|s| s.service_name.starts_with("market-feed[")),
        "market-feed service should be registered: {:?}",
        health
            .services
            .iter()
            .map(|s| &s.service_name)
            .collect::<Vec<_>>()
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}

#[tokio::test(start_paused = true)]
async fn fill_routing_service_delivers_to_brain_and_refreshes_cache() {
    let (brain, seen) = FillRecordingBrain::new();
    let exchange = CountingExchange::new();
    exchange.set_position(
        Symbol::from("BTCUSDT"),
        Position {
            qty: 7.0,
            entry_price: Some(100.0),
            unrealised_pnl: 0.0,
        },
    );
    let lookups = exchange.position_lookups.clone();

    let bot = Bot::new(
        BotConfig::builder()
            .name("fill-routing")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .build()
            .unwrap(),
        exchange,
        vec![brain],
    )
    .unwrap();

    let (fill_source, tx) = ChannelFillSource::new();
    let bot = bot.with_fill_source(fill_source);

    let handle = bot.handle();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });

    // Settle: the bot's prefetch_positions on startup already incremented
    // the lookup counter once per symbol. Capture that baseline.
    tokio::time::sleep(Duration::from_millis(80)).await;
    let baseline_lookups = lookups.load(Ordering::SeqCst);

    // Push two fills.
    tx.send(make_fill("BTCUSDT", Side::Buy, 1.0)).unwrap();
    tx.send(make_fill("BTCUSDT", Side::Buy, 2.0)).unwrap();

    // Wait for both to flow through.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if seen.lock().await.len() == 2 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("brain saw {} fills, expected 2", seen.lock().await.len());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Position lookups should have advanced by at least 2 (one refresh
    // per fill).
    let after = lookups.load(Ordering::SeqCst);
    assert!(
        after >= baseline_lookups + 2,
        "expected >= {} position lookups, got {after}",
        baseline_lookups + 2
    );

    // And the cache should reflect the seeded position.
    let cached = handle.position(&Symbol::from("BTCUSDT")).await;
    assert!((cached.qty - 7.0).abs() < 1e-9);

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}
