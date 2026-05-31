//! SL+TP bracket / OCO end-to-end (post-0.2c).
//!
//! With a capable adapter (StopOrders + OrderTracking) and a fill source
//! wired, a market entry carrying both `stop_price` and `take_profit_price`
//! produces three orders — entry + reduce-only stop-loss + reduce-only
//! take-profit — and a fill on one protective leg cancels the other (OCO).

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use rustrade::{
    Bot, BotConfig, Brain, Candle, Capability, Decision, Exchange, ExchangeClient, Fill,
    FillSource, MarketDataEvent, Order, Position, Price, Result, Side, SizingConfig, StopKind,
    Symbol, Volume,
};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;

/// Places one market buy with a bracket (SL+TP) on its first event, holds after.
struct BracketBrain {
    fired: Mutex<bool>,
}
impl BracketBrain {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            fired: Mutex::new(false),
        })
    }
}
#[async_trait]
impl Brain for BracketBrain {
    fn name(&self) -> &str {
        "bracket"
    }
    async fn on_event(&self, _e: &MarketDataEvent, _p: &Position) -> Result<Decision> {
        let mut fired = self.fired.lock().unwrap();
        if *fired {
            return Ok(Decision::hold());
        }
        *fired = true;
        Ok(Decision::buy(1.0)
            .with_stop(Price(90.0))
            .with_take_profit(Price(110.0)))
    }
}

/// Captures every placed order, advertises StopOrders + OrderTracking, and
/// records cancels.
struct BracketExchange {
    placed: Mutex<Vec<Order>>,
    cancels: Arc<Mutex<Vec<String>>>,
    next_id: AtomicU64,
}
impl BracketExchange {
    fn new() -> (Arc<Self>, Arc<Mutex<Vec<String>>>) {
        let cancels = Arc::new(Mutex::new(Vec::new()));
        (
            Arc::new(Self {
                placed: Mutex::new(Vec::new()),
                cancels: cancels.clone(),
                next_id: AtomicU64::new(0),
            }),
            cancels,
        )
    }
    fn placed_snapshot(&self) -> Vec<Order> {
        self.placed.lock().unwrap().clone()
    }
}
#[async_trait]
impl ExchangeClient for BracketExchange {
    fn name(&self) -> &str {
        "bracket-ex"
    }
    async fn place_order(&self, o: &Order) -> Result<String> {
        let n = self.next_id.fetch_add(1, Ordering::SeqCst) + 1;
        self.placed.lock().unwrap().push(o.clone());
        Ok(format!("ord-{n}"))
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
    fn supports(&self, c: Capability) -> bool {
        matches!(c, Capability::StopOrders | Capability::OrderTracking)
    }
    async fn get_open_orders(&self, _s: &Symbol) -> Result<Vec<rustrade::OpenOrder>> {
        Ok(Vec::new())
    }
    async fn cancel_order(&self, _s: &Symbol, order_id: &str) -> Result<bool> {
        self.cancels.lock().unwrap().push(order_id.to_string());
        Ok(true)
    }
}

/// Channel-backed fill source so the test can inject a fill for a given id.
struct ChannelFills {
    rx: AsyncMutex<mpsc::UnboundedReceiver<Fill>>,
}
#[async_trait]
impl FillSource for ChannelFills {
    async fn next_fill(&self) -> Option<Fill> {
        self.rx.lock().await.recv().await
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

#[tokio::test(flavor = "multi_thread")]
async fn bracket_places_three_orders_and_oco_cancels_sibling() {
    let (exchange, cancels) = BracketExchange::new();
    let (fill_tx, fill_rx) = mpsc::unbounded_channel();
    let fills = Arc::new(ChannelFills {
        rx: AsyncMutex::new(fill_rx),
    });

    let bot = Bot::new(
        BotConfig::builder()
            .name("bracket")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .sizing_config(SizingConfig {
                margin_per_trade: 100.0,
                leverage: 1,
                max_contracts: 10,
            })
            .build()
            .unwrap(),
        exchange.clone(),
        vec![BracketBrain::new()],
    )
    .unwrap()
    .with_fill_source(fills); // fill source → brackets active (adapter is capable)

    let bus = bot.market_data_bus().clone();
    let handle = bot.handle();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });

    // Drive candles until all three bracket orders are placed.
    let three = eventually(10, || {
        if exchange.placed_snapshot().len() >= 3 {
            true
        } else {
            bus.publish(candle_event("BTCUSDT", 100.0));
            false
        }
    })
    .await;
    assert!(
        three,
        "expected entry + SL + TP, got {:?}",
        exchange.placed_snapshot()
    );

    let orders = exchange.placed_snapshot();
    assert_eq!(orders.len(), 3, "entry + 2 protective legs");
    // [0] = market entry buy, clean (no stop attached).
    assert_eq!(orders[0].side, Side::Buy);
    assert!(
        orders[0].stop.is_none(),
        "bracket entry must be a clean order"
    );
    // [1], [2] = reduce-only Sell protective legs: one stop-market, one TP.
    let protective = &orders[1..3];
    assert!(
        protective
            .iter()
            .all(|o| o.reduce_only && o.side == Side::Sell)
    );
    let kinds: Vec<_> = protective
        .iter()
        .filter_map(|o| o.stop.map(|s| s.kind))
        .collect();
    assert!(
        kinds.iter().any(|k| matches!(k, StopKind::StopMarket))
            && kinds.iter().any(|k| matches!(k, StopKind::TakeProfit)),
        "protective legs must be one stop-market + one take-profit, got {kinds:?}"
    );

    // The SL leg is ord-2 (entry=ord-1, SL=ord-2, TP=ord-3). Inject a fill
    // for it → the TP sibling (ord-3) must be cancelled by OCO.
    fill_tx
        .send(Fill {
            symbol: Symbol::from("BTCUSDT"),
            order_id: "ord-2".into(),
            client_id: None,
            side: Side::Sell,
            price: Price(90.0),
            size: Volume(1.0),
            fee: 0.0,
            fee_currency: "USDT".into(),
            timestamp: Utc::now(),
        })
        .unwrap();

    let cancelled = eventually(10, || !cancels.lock().unwrap().is_empty()).await;
    assert!(
        cancelled,
        "OCO should cancel the sibling after the SL leg filled"
    );
    assert_eq!(
        cancels.lock().unwrap().as_slice(),
        &["ord-3".to_string()],
        "the take-profit leg (ord-3) must be the cancelled sibling"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn no_fill_source_falls_back_to_single_stop() {
    // Capable adapter but NO fill source → brackets inactive (OCO can't be
    // enforced), so a both-SL+TP decision attaches a single stop-loss to the
    // entry instead of placing three orders.
    let (exchange, _cancels) = BracketExchange::new();
    let bot = Bot::new(
        BotConfig::builder()
            .name("fallback")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .sizing_config(SizingConfig {
                margin_per_trade: 100.0,
                leverage: 1,
                max_contracts: 10,
            })
            .build()
            .unwrap(),
        exchange.clone(),
        vec![BracketBrain::new()],
    )
    .unwrap(); // no with_fill_source

    let bus = bot.market_data_bus().clone();
    let handle = bot.handle();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });

    let placed = eventually(10, || {
        if !exchange.placed_snapshot().is_empty() {
            true
        } else {
            bus.publish(candle_event("BTCUSDT", 100.0));
            false
        }
    })
    .await;
    assert!(placed, "entry should be placed");
    // Give any (erroneous) extra legs a beat to show up, then assert exactly one.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let orders = exchange.placed_snapshot();
    assert_eq!(
        orders.len(),
        1,
        "fallback places only the entry, got {orders:?}"
    );
    let stop = orders[0]
        .stop
        .expect("fallback attaches a single stop-loss");
    assert!(matches!(stop.kind, StopKind::StopMarket));
    assert_eq!(stop.trigger_price, Price(90.0));

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}
