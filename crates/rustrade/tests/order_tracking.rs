//! 0.2c end-to-end: with order tracking wired and a capable adapter, a
//! resting limit order placed by the execution service is recorded in the
//! tracker and aged out by the reaper once it passes the TTL — all observed
//! through the public `Bot` / `BotHandle` API.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use rustrade::{
    Bot, BotConfig, Brain, Candle, Capability, Decision, Exchange, ExchangeClient, MarketDataEvent,
    OpenOrder, Order, OrderStatus, Position, Price, Result, SizingConfig, Symbol,
};

/// Brain that places one resting limit buy on its first event, then holds.
struct OneLimitBrain {
    fired: Mutex<bool>,
}
impl OneLimitBrain {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            fired: Mutex::new(false),
        })
    }
}
#[async_trait]
impl Brain for OneLimitBrain {
    fn name(&self) -> &str {
        "one-limit"
    }
    async fn on_event(&self, _e: &MarketDataEvent, _p: &Position) -> Result<Decision> {
        let mut fired = self.fired.lock().unwrap();
        if *fired {
            return Ok(Decision::hold());
        }
        *fired = true;
        Ok(Decision::buy(1.0).with_limit_price(Price(95.0)))
    }
}

/// Adapter that advertises OrderTracking, lists what it has been told is
/// resting, and records cancels. `created_at` is set far in the past so the
/// reaper's TTL fires immediately.
struct TrackingExchange {
    open: Mutex<Vec<OpenOrder>>,
    cancels: Arc<Mutex<Vec<String>>>,
    next_id: Mutex<u64>,
    placed: Arc<std::sync::atomic::AtomicU64>,
}
impl TrackingExchange {
    fn new() -> (Arc<Self>, Arc<Mutex<Vec<String>>>) {
        let cancels = Arc::new(Mutex::new(Vec::new()));
        (
            Arc::new(Self {
                open: Mutex::new(Vec::new()),
                cancels: cancels.clone(),
                next_id: Mutex::new(0),
                placed: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            }),
            cancels,
        )
    }
    /// Shared counter of `place_order` calls, for the test to poll on.
    fn placed_count(&self) -> Arc<std::sync::atomic::AtomicU64> {
        self.placed.clone()
    }
}
#[async_trait]
impl ExchangeClient for TrackingExchange {
    fn name(&self) -> &str {
        "tracking"
    }
    async fn place_order(&self, o: &Order) -> Result<String> {
        self.placed
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut n = self.next_id.lock().unwrap();
        *n += 1;
        let id = format!("ord-{n}");
        // Record the resting order as "open", created an hour ago so it's
        // immediately past any short TTL.
        self.open.lock().unwrap().push(OpenOrder {
            order_id: id.clone(),
            client_id: None,
            symbol: o.symbol.clone(),
            side: o.side,
            kind: o.kind,
            limit_price: o.limit_price,
            size: o.size,
            filled: rustrade::Volume(0.0),
            status: OrderStatus::Open,
            created_at: Some(chrono::Utc::now() - chrono::Duration::hours(1)),
        });
        Ok(id)
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
        matches!(c, Capability::OrderTracking)
    }
    async fn get_open_orders(&self, symbol: &Symbol) -> Result<Vec<OpenOrder>> {
        Ok(self
            .open
            .lock()
            .unwrap()
            .iter()
            .filter(|o| &o.symbol == symbol)
            .cloned()
            .collect())
    }
    async fn cancel_order(&self, _s: &Symbol, order_id: &str) -> Result<bool> {
        self.cancels.lock().unwrap().push(order_id.to_string());
        self.open.lock().unwrap().retain(|o| o.order_id != order_id);
        Ok(true)
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

/// Poll `cond` every 25ms until it returns true or `secs` elapses; returns
/// whether it became true. Robust to slow CI: a loaded runner just waits
/// longer instead of failing on a fixed sleep.
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
async fn reaper_cancels_stale_resting_order_end_to_end() {
    let (exchange, cancels) = TrackingExchange::new();
    let bot = Bot::new(
        BotConfig::builder()
            .name("track")
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
        vec![OneLimitBrain::new()],
    )
    .unwrap()
    // 1s TTL, sweep every 100ms — the placed order is "created an hour ago"
    // so the first sweep cancels it.
    .with_order_tracking(Duration::from_secs(1), Duration::from_millis(100));

    let handle = bot.handle();
    let bus = bot.market_data_bus().clone();
    let placed = exchange.placed_count();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });

    // Publish candles until the brain's limit order actually lands. A single
    // fixed-delay publish can race service startup on a slow runner: the
    // broadcast bus drops events with no subscriber yet, so the candle is
    // lost and no order is placed. Re-publishing until `place_order` fired
    // removes that timing dependence. (The brain places at most one order;
    // extra candles are held.)
    let order_placed = eventually(10, || {
        placed.load(std::sync::atomic::Ordering::SeqCst) >= 1 || {
            bus.publish(candle_event("BTCUSDT", 100.0));
            false
        }
    })
    .await;
    assert!(order_placed, "brain never placed its limit order");

    // Then wait for the reaper to cancel the stale order.
    let cancelled_ok = eventually(10, || !cancels.lock().unwrap().is_empty()).await;
    assert!(cancelled_ok, "reaper never cancelled the stale order");

    let cancelled = cancels.lock().unwrap().clone();
    assert_eq!(
        cancelled.len(),
        1,
        "reaper should cancel exactly the one stale order"
    );
    assert_eq!(cancelled[0], "ord-1");

    // Tracker should no longer hold it (cancelled → forgotten).
    let tracked = handle.tracked_orders().await;
    assert!(
        tracked.is_empty(),
        "cancelled order should be dropped from the tracker, got {tracked:?}"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn no_tracking_without_capability() {
    // Same brain, but an adapter WITHOUT OrderTracking → nothing is tracked
    // even though a limit order is placed.
    struct PlainExchange {
        placed: Arc<std::sync::atomic::AtomicU64>,
    }
    #[async_trait]
    impl ExchangeClient for PlainExchange {
        fn name(&self) -> &str {
            "plain"
        }
        async fn place_order(&self, _o: &Order) -> Result<String> {
            self.placed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
        // supports() defaults to false for everything, incl. OrderTracking.
    }

    let placed = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let bot = Bot::new(
        BotConfig::builder()
            .name("no-track")
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
        Arc::new(PlainExchange {
            placed: placed.clone(),
        }),
        vec![OneLimitBrain::new()],
    )
    .unwrap()
    .with_order_tracking(Duration::from_secs(1), Duration::from_millis(100));

    let handle = bot.handle();
    let bus = bot.market_data_bus().clone();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });

    // Drive candles until the order is actually placed (proves the brain
    // fired), so the "nothing tracked" assertion is meaningful rather than
    // racing startup.
    let order_placed = eventually(10, || {
        placed.load(std::sync::atomic::Ordering::SeqCst) >= 1 || {
            bus.publish(candle_event("BTCUSDT", 100.0));
            false
        }
    })
    .await;
    assert!(order_placed, "brain never placed its limit order");

    // Adapter can't track, so the execution service never recorded the order.
    assert!(
        handle.tracked_orders().await.is_empty(),
        "no orders should be tracked without Capability::OrderTracking"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}
