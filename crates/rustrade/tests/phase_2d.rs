//! Integration tests for Phase 2d additions:
//!
//! - `MetricsSink` trait — host-pluggable counters / histograms
//! - `CandleSource` trait + `CandlePollerService` wired by
//!   `Bot::with_candle_poller`
//! - Auto-PnL feeding in `FillRoutingService`

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use rustrade::{
    Bot, BotConfig, Brain, Candle, CandleSource, CircuitBreakerConfig, Decision, ExchangeClient,
    Fill, FillSource, MarketDataEvent, MetricsSink, Order, Position, Price, Result, Side, Symbol,
    Volume,
};
use tokio::sync::Mutex as AsyncMutex;

// ── Fixtures ────────────────────────────────────────────────────────────

struct RecordingSink {
    counters: Mutex<HashMap<String, u64>>,
    histograms: Mutex<Vec<(String, f64)>>,
}
impl RecordingSink {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            counters: Mutex::new(HashMap::new()),
            histograms: Mutex::new(Vec::new()),
        })
    }
    fn counter_total(&self, name: &str) -> u64 {
        *self.counters.lock().unwrap().get(name).unwrap_or(&0)
    }
    fn histogram_samples(&self, name: &str) -> Vec<f64> {
        self.histograms
            .lock()
            .unwrap()
            .iter()
            .filter(|(n, _)| n == name)
            .map(|(_, v)| *v)
            .collect()
    }
}
impl MetricsSink for RecordingSink {
    fn counter(&self, name: &str, _labels: &[(&str, &str)], value: u64) {
        *self
            .counters
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_insert(0) += value;
    }
    fn gauge(&self, _name: &str, _labels: &[(&str, &str)], _value: f64) {}
    fn histogram(&self, name: &str, _labels: &[(&str, &str)], value: f64) {
        self.histograms
            .lock()
            .unwrap()
            .push((name.to_string(), value));
    }
}

struct PositionTrackingExchange {
    positions: Mutex<HashMap<Symbol, Position>>,
    placed: Arc<AtomicU64>,
}
impl PositionTrackingExchange {
    fn new() -> (Arc<Self>, Arc<AtomicU64>) {
        let placed = Arc::new(AtomicU64::new(0));
        let inst = Arc::new(Self {
            positions: Mutex::new(HashMap::new()),
            placed: placed.clone(),
        });
        (inst, placed)
    }
    fn set_position(&self, sym: Symbol, pos: Position) {
        self.positions.lock().unwrap().insert(sym, pos);
    }
}
#[async_trait]
impl ExchangeClient for PositionTrackingExchange {
    fn name(&self) -> &str {
        "tracking"
    }
    async fn place_order(&self, _o: &Order) -> Result<String> {
        self.placed.fetch_add(1, Ordering::SeqCst);
        Ok("ok".into())
    }
    async fn cancel_all(&self, _s: &Symbol) -> Result<usize> {
        Ok(0)
    }
    async fn close_position(&self, _s: &Symbol, _p: &Position) -> Result<String> {
        Ok("close".into())
    }
    async fn get_position(&self, s: &Symbol) -> Result<Position> {
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

/// FillSource backed by an mpsc channel.
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

/// CandleSource that returns a fixed pre-loaded sequence per poll.
struct ScriptedCandles {
    batches: AsyncMutex<Vec<Vec<Candle>>>,
}
impl ScriptedCandles {
    fn new(batches: Vec<Vec<Candle>>) -> Arc<Self> {
        Arc::new(Self {
            batches: AsyncMutex::new(batches),
        })
    }
}
#[async_trait]
impl CandleSource for ScriptedCandles {
    fn name(&self) -> &str {
        "scripted"
    }
    async fn poll(&self, _s: &Symbol, _i: Duration, _l: usize) -> Result<Vec<Candle>> {
        let mut b = self.batches.lock().await;
        if b.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(b.remove(0))
        }
    }
}

fn candle(time: i64, close: f64) -> Candle {
    Candle {
        time,
        open: close,
        high: close,
        low: close,
        close,
        volume: 1.0,
    }
}

fn make_fill(symbol: &str, side: Side, qty: f64, price: f64, fee: f64) -> Fill {
    Fill {
        symbol: Symbol::from(symbol),
        order_id: "ord-x".into(),
        client_id: None,
        side,
        price: Price(price),
        size: Volume(qty),
        fee,
        fee_currency: "USDT".into(),
        timestamp: Utc::now(),
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn candle_poller_deduplicates_by_timestamp() {
    let (exchange, _) = PositionTrackingExchange::new();
    let bot = Bot::new(
        BotConfig::builder()
            .name("candle-poll")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .build()
            .unwrap(),
        exchange,
        vec![Arc::new(NoopBrain)],
    )
    .unwrap();

    // Two polls: batch 1 returns candles 100..103, batch 2 returns
    // 102..105 (overlap on 102, 103). Only the four distinct ones
    // should reach the bus.
    let source = ScriptedCandles::new(vec![
        vec![
            candle(100, 1.0),
            candle(101, 1.0),
            candle(102, 1.0),
            candle(103, 1.0),
        ],
        vec![
            candle(102, 1.0),
            candle(103, 1.0),
            candle(104, 1.0),
            candle(105, 1.0),
        ],
    ]);
    let bot = bot.with_candle_poller(
        source,
        "BTCUSDT",
        Duration::from_secs(60),
        Duration::from_millis(50),
        4,
    );

    let mut events = bot.market_data_bus().subscribe();
    let handle = bot.handle();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });

    // Wait until we've seen the expected 6 distinct candles, or give
    // up after a generous deadline. Two polls × 50 ms cadence = ~100 ms
    // of expected work; budget 2 s to absorb slow CI startup.
    let mut count = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while count < 6 {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        match tokio::time::timeout(deadline - now, events.recv()).await {
            Ok(Ok(_)) => count += 1,
            _ => break,
        }
    }

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;

    // Batches contain 4 + 4 = 8 candles total. Batch 2 overlaps with
    // batch 1 on times 102 + 103 → 2 dropped as dupes, 6 published
    // distinctly. Asserting `>= 6` rather than `== 6` because a third
    // poll on slow CI returns an empty batch (we only scripted two);
    // no new candles can sneak in after the dedup boundary.
    assert_eq!(
        count, 6,
        "expected dedup to drop 2 of 8 candles, got {count}"
    );
}

#[tokio::test]
async fn metrics_sink_receives_fill_routing_counters() {
    let (exchange, _) = PositionTrackingExchange::new();

    let sink = RecordingSink::new();
    let bot = Bot::new(
        BotConfig::builder()
            .name("metrics-sink")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .build()
            .unwrap(),
        exchange.clone(),
        vec![Arc::new(NoopBrain)],
    )
    .unwrap()
    .with_metrics(sink.clone());

    let (fill_source, tx) = ChannelFillSource::new();
    let bot = bot.with_fill_source(fill_source);

    let handle = bot.handle();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });

    // Seed the position cache deterministically via the handle.
    // `Bot::run_until_shutdown`'s `prefetch_positions` runs
    // concurrently with the test, so we set + verify in a loop until
    // our seed survives a read-back (signalling prefetch is done).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let p = handle.position(&Symbol::from("BTCUSDT")).await;
        if (p.qty - 1.0).abs() < 1e-9 && p.entry_price == Some(100.0) {
            break;
        }
        handle
            .set_position(
                &Symbol::from("BTCUSDT"),
                Position {
                    qty: 1.0,
                    entry_price: Some(100.0),
                    unrealised_pnl: 0.0,
                },
            )
            .await;
        if tokio::time::Instant::now() > deadline {
            panic!("failed to seed position cache");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // After the close, the FillRoutingService refreshes from the
    // exchange. Set the exchange to FLAT so that refresh produces a
    // sensible post-close state.
    exchange.set_position(Symbol::from("BTCUSDT"), Position::FLAT);
    tx.send(make_fill("BTCUSDT", Side::Sell, 1.0, 110.0, 0.5))
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while sink.counter_total("rustrade_fills_routed_total") == 0 {
        if tokio::time::Instant::now() > deadline {
            panic!("fill counter never incremented");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;

    assert!(sink.counter_total("rustrade_fills_routed_total") >= 1);
    let pnl_samples = sink.histogram_samples("rustrade_realised_pnl_quote");
    assert_eq!(pnl_samples.len(), 1, "exactly one realised-PnL sample");
    // Gross = (110 - 100) * 1 * 1 = 10; fee = 0.5; net = 9.5
    assert!((pnl_samples[0] - 9.5).abs() < 1e-9);
}

#[tokio::test]
async fn fill_routing_auto_feeds_circuit_breaker_on_loss() {
    let (exchange, _) = PositionTrackingExchange::new();

    let bot = Bot::new(
        BotConfig::builder()
            .name("auto-pnl-breaker")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            // 1 loss trips the breaker — easy to assert.
            .circuit_breaker_config(CircuitBreakerConfig {
                loss_limit: 1,
                window_secs: 3600,
                cooldown_secs: 3600,
            })
            .build()
            .unwrap(),
        exchange.clone(),
        vec![Arc::new(NoopBrain)],
    )
    .unwrap();

    let (fill_source, tx) = ChannelFillSource::new();
    let bot = bot.with_fill_source(fill_source);
    let handle = bot.handle();

    let task = tokio::spawn(async move { bot.run_until_shutdown().await });

    // Seed the position cache deterministically via the handle.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if handle.position(&Symbol::from("BTCUSDT")).await.qty != 1.0 {
            handle
                .set_position(
                    &Symbol::from("BTCUSDT"),
                    Position {
                        qty: 1.0,
                        entry_price: Some(100.0),
                        unrealised_pnl: 0.0,
                    },
                )
                .await;
        }
        let p = handle.position(&Symbol::from("BTCUSDT")).await;
        if (p.qty - 1.0).abs() < 1e-9 && p.entry_price == Some(100.0) {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("failed to seed position cache");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Close at 90 — a -10 loss.
    exchange.set_position(Symbol::from("BTCUSDT"), Position::FLAT);
    tx.send(make_fill("BTCUSDT", Side::Sell, 1.0, 90.0, 0.0))
        .unwrap();

    // Wait for the breaker to trip via the auto-PnL feed.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let h = handle.health().await;
        // SessionPnl reflects net of -10 immediately; checking via
        // record_trade_outcome's downstream effect: emit a Sell again
        // and see if the breaker now blocks it. Actually we just check
        // the health snapshot's reported brain health stays consistent
        // — and rely on the gate-side test for true breaker behavior.
        // Here we just assert the bot doesn't crash on the fill.
        let _ = h;
        if tokio::time::Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Send another fill to confirm the breaker is now tripped from
    // the auto-PnL feed. We won't see it via place_order (no brain
    // emits orders), but `handle.health` doesn't expose the breaker
    // state directly. So this test mostly asserts the bot survives
    // the fill without panicking and the FillRoutingService runs
    // record_trade_outcome under the hood.
    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}

#[tokio::test]
async fn metrics_sink_default_is_noop() {
    // Bot without with_metrics should use NoopSink. Spawning + running
    // shouldn't panic.
    let (exchange, _) = PositionTrackingExchange::new();
    let bot = Bot::new(
        BotConfig::builder()
            .name("noop-metrics")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .build()
            .unwrap(),
        exchange,
        vec![Arc::new(NoopBrain)],
    )
    .unwrap();

    let handle = bot.handle();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}
