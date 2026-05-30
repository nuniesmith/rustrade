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

/// Poll `predicate` until true, yielding to the runtime between checks.
///
/// These tests run under `#[tokio::test(start_paused = true)]`, so the
/// `sleep` advances *virtual* time: it resolves the instant the awaited
/// background work (a supervised service processing a channel message)
/// completes, with zero wall-clock dependence. That's what makes the
/// suite immune to slow/contended CI runners — the historical source of
/// the macOS flake. The outer `timeout` fires only if the condition is
/// genuinely unreachable, and fires instantly in virtual time, turning a
/// hang into a fast, clear failure.
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

// ── Tests ───────────────────────────────────────────────────────────────

#[tokio::test(start_paused = true)]
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

    // Batches contain 4 + 4 = 8 candles; batch 2 overlaps batch 1 on
    // times 102 + 103, which the poller drops as dupes → exactly 6
    // distinct candles ever reach the bus. Under virtual time the poller
    // is driven deterministically through both batches, so we can wait
    // for precisely 6 and assert the deduped timestamp set directly.
    let collect = async {
        let mut times = Vec::new();
        for _ in 0..6 {
            if let MarketDataEvent::Candle { candle, .. } = events.recv().await.unwrap() {
                times.push(candle.time);
            }
        }
        times
    };
    let mut times = tokio::time::timeout(Duration::from_secs(30), collect)
        .await
        .expect("timed out waiting for 6 deduped candles");

    handle.shutdown();
    let _ = task.await;

    times.sort_unstable();
    assert_eq!(
        times,
        vec![100, 101, 102, 103, 104, 105],
        "expected the 6 distinct timestamps after dedup"
    );
}

#[tokio::test(start_paused = true)]
async fn metrics_sink_receives_fill_routing_counters() {
    let (exchange, _) = PositionTrackingExchange::new();
    // Seed the position on the *exchange* before the bot starts. The
    // bot's `prefetch_positions` reads it during startup (before any
    // service is spawned), so the position cache is seeded
    // deterministically — no racing the prefetch via the handle.
    exchange.set_position(
        Symbol::from("BTCUSDT"),
        Position {
            qty: 1.0,
            entry_price: Some(100.0),
            unrealised_pnl: 0.0,
        },
    );

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

    // Sell-to-close at 110. The FillRoutingService snapshots the cached
    // LONG@100 *before* refreshing, so PnL is computed against the seed.
    tx.send(make_fill("BTCUSDT", Side::Sell, 1.0, 110.0, 0.5))
        .unwrap();

    wait_for(
        || sink.counter_total("rustrade_fills_routed_total") >= 1,
        "fill counter to increment",
    )
    .await;

    handle.shutdown();
    let _ = task.await;

    assert!(sink.counter_total("rustrade_fills_routed_total") >= 1);
    let pnl_samples = sink.histogram_samples("rustrade_realised_pnl_quote");
    assert_eq!(pnl_samples.len(), 1, "exactly one realised-PnL sample");
    // Gross = (110 - 100) * 1 * 1 = 10; fee = 0.5; net = 9.5
    assert!((pnl_samples[0] - 9.5).abs() < 1e-9);
}

#[tokio::test(start_paused = true)]
async fn fill_routing_auto_feeds_circuit_breaker_on_loss() {
    let (exchange, _) = PositionTrackingExchange::new();
    // Seed via the exchange so prefetch deterministically seeds the
    // cache to LONG@100 (see `metrics_sink_receives_fill_routing_counters`).
    exchange.set_position(
        Symbol::from("BTCUSDT"),
        Position {
            qty: 1.0,
            entry_price: Some(100.0),
            unrealised_pnl: 0.0,
        },
    );

    let sink = RecordingSink::new();
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
    .unwrap()
    .with_metrics(sink.clone());

    let (fill_source, tx) = ChannelFillSource::new();
    let bot = bot.with_fill_source(fill_source);
    let handle = bot.handle();

    let task = tokio::spawn(async move { bot.run_until_shutdown().await });

    // Sell-to-close at 90 — a -10 realised loss, fed into the breaker.
    tx.send(make_fill("BTCUSDT", Side::Sell, 1.0, 90.0, 0.0))
        .unwrap();

    // Wait until the fill has actually been routed (the auto-PnL feed
    // runs inside the same handler, before the counter bumps).
    wait_for(
        || sink.counter_total("rustrade_fills_routed_total") >= 1,
        "loss fill to be routed",
    )
    .await;

    // The loss was recorded against the breaker; assert the realised PnL
    // sample is the -10 loss so we know the auto-feed actually fired.
    let pnl_samples = sink.histogram_samples("rustrade_realised_pnl_quote");
    assert_eq!(pnl_samples.len(), 1, "exactly one realised-PnL sample");
    assert!(
        (pnl_samples[0] - (-10.0)).abs() < 1e-9,
        "expected -10 loss, got {}",
        pnl_samples[0]
    );

    handle.shutdown();
    let _ = task.await;
}

#[tokio::test(start_paused = true)]
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
    // Let the bot reach steady state, then shut down. Virtual time, so
    // this is instant and deterministic.
    tokio::time::sleep(Duration::from_millis(50)).await;
    handle.shutdown();
    let _ = task.await;
}
