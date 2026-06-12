//! Ingestion-boundary validation of fills and trade outcomes.
//!
//! A NaN that reaches `SessionPnl::record_close` poisons the accumulated
//! PnL: every subsequent `net_pnl() <= loss_limit` comparison is false and
//! the session halt silently never fires. These tests pin the two
//! boundaries that now reject non-finite values:
//!
//! - `FillRoutingService` drops a non-finite fill entirely (not routed to
//!   brains, not recorded in risk state).
//! - `BotHandle::record_trade_outcome` rejects non-finite PnL, so the
//!   loss-limit gate still works after a host feeds it garbage.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use rustrade::{
    Bot, BotConfig, Brain, Candle, Decision, Exchange, ExchangeClient, Fill, FillSource,
    MarketDataEvent, Order, Position, Price, Result, SessionPnlConfig, Side, SizingConfig, Symbol,
    Volume,
};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;

// ── Fixtures ────────────────────────────────────────────────────────────

struct CountingExchange {
    placed: Arc<AtomicU64>,
    positions: Mutex<HashMap<Symbol, Position>>,
}
impl CountingExchange {
    fn new() -> (Arc<Self>, Arc<AtomicU64>) {
        let placed = Arc::new(AtomicU64::new(0));
        (
            Arc::new(Self {
                placed: placed.clone(),
                positions: Mutex::new(HashMap::new()),
            }),
            placed,
        )
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
        Ok("c".into())
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

/// Brain that records every fill routed to it and always buys.
struct RecordingBuyBrain {
    fills: Arc<AsyncMutex<Vec<Fill>>>,
}
impl RecordingBuyBrain {
    fn new() -> (Arc<Self>, Arc<AsyncMutex<Vec<Fill>>>) {
        let fills = Arc::new(AsyncMutex::new(Vec::new()));
        (
            Arc::new(Self {
                fills: fills.clone(),
            }),
            fills,
        )
    }
}
#[async_trait]
impl Brain for RecordingBuyBrain {
    fn name(&self) -> &str {
        "recording-buy"
    }
    async fn on_event(&self, _e: &MarketDataEvent, _p: &Position) -> Result<Decision> {
        Ok(Decision::buy(1.0))
    }
    async fn on_fill(&self, fill: &Fill) -> Result<()> {
        self.fills.lock().await.push(fill.clone());
        Ok(())
    }
}

struct ChannelFills {
    rx: AsyncMutex<mpsc::UnboundedReceiver<Fill>>,
}
#[async_trait]
impl FillSource for ChannelFills {
    async fn next_fill(&self) -> Option<Fill> {
        self.rx.lock().await.recv().await
    }
}

fn fill(symbol: &str, side: Side, price: f64, size: f64, fee: f64) -> Fill {
    Fill {
        symbol: Symbol::from(symbol),
        order_id: "x".into(),
        client_id: None,
        side,
        price: Price(price),
        size: Volume(size),
        fee,
        fee_currency: "USDT".into(),
        timestamp: Utc::now(),
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

async fn eventually<F>(secs: u64, mut cond: F) -> bool
where
    F: FnMut() -> bool,
{
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

// ── Tests ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn non_finite_fill_is_dropped_but_valid_fill_still_routes() {
    let (exchange, _placed) = CountingExchange::new();
    let (brain, seen) = RecordingBuyBrain::new();
    let (fill_tx, fill_rx) = mpsc::unbounded_channel();
    let fills = Arc::new(ChannelFills {
        rx: AsyncMutex::new(fill_rx),
    });

    let bot = Bot::new(
        BotConfig::builder()
            .name("fill-validation")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .build()
            .unwrap(),
        exchange,
        vec![brain],
    )
    .unwrap()
    .with_fill_source(fills);

    let handle = bot.handle();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });

    // Three corrupt fills (NaN price, NaN fee, negative size), then a valid one.
    fill_tx
        .send(fill("BTCUSDT", Side::Sell, f64::NAN, 1.0, 0.0))
        .unwrap();
    fill_tx
        .send(fill("BTCUSDT", Side::Sell, 100.0, 1.0, f64::NAN))
        .unwrap();
    fill_tx
        .send(fill("BTCUSDT", Side::Sell, 100.0, -1.0, 0.0))
        .unwrap();
    fill_tx
        .send(fill("BTCUSDT", Side::Buy, 100.0, 1.0, 0.1))
        .unwrap();

    // The valid fill arrives at the brain; the corrupt ones never do.
    let routed = eventually(10, || {
        seen.try_lock().map(|v| !v.is_empty()).unwrap_or(false)
    })
    .await;
    assert!(routed, "the valid fill must still be routed");
    // Let any (erroneously surviving) corrupt fill drain through.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let seen = seen.lock().await;
    assert_eq!(
        seen.len(),
        1,
        "only the finite fill may reach brains, got {seen:?}"
    );
    assert_eq!(seen[0].price, Price(100.0));

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nan_trade_outcome_rejected_and_loss_halt_still_fires() {
    // loss_limit -50: a NaN outcome must be rejected (state unchanged), and
    // a later genuine -60 loss must still halt the session — i.e. the NaN
    // did not poison the accumulated PnL.
    let (exchange, placed) = CountingExchange::new();
    let (brain, _seen) = RecordingBuyBrain::new();

    let bot = Bot::new(
        BotConfig::builder()
            .name("nan-outcome")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .session_pnl_config(SessionPnlConfig { loss_limit: -50.0 })
            .sizing_config(SizingConfig {
                margin_per_trade: 100.0,
                leverage: 1,
                max_contracts: 10,
            })
            .build()
            .unwrap(),
        exchange,
        vec![brain],
    )
    .unwrap();

    let symbol = Symbol::from("BTCUSDT");
    let bus = bot.market_data_bus().clone();
    let handle = bot.handle();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });

    // Garbage in: rejected, risk state unchanged.
    handle.record_trade_outcome(&symbol, f64::NAN, 0.0).await;
    handle.record_trade_outcome(&symbol, 0.0, f64::NAN).await;

    // The gate must still be open: a buy decision goes through.
    let first = eventually(10, || {
        if placed.load(Ordering::SeqCst) >= 1 {
            true
        } else {
            bus.publish(candle_event("BTCUSDT", 100.0));
            false
        }
    })
    .await;
    assert!(first, "gate must be open after NaN outcomes were rejected");

    // A genuine loss past the cap must halt the session — this is exactly
    // what would NOT happen had the NaN poisoned the accumulated PnL.
    handle.record_trade_outcome(&symbol, -60.0, 0.0).await;
    let before = placed.load(Ordering::SeqCst);
    for _ in 0..10 {
        bus.publish(candle_event("BTCUSDT", 100.0));
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert_eq!(
        placed.load(Ordering::SeqCst),
        before,
        "session must be halted after a -60 loss with a -50 cap"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}
