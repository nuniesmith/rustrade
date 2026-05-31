//! 0.2b: the `ExecutionService` honours `Decision` order-kind / limit /
//! protective-stop fields, gated on adapter `Capability`.
//!
//! A capturing exchange records every placed `Order` so we can assert on
//! its kind, limit price, and stop attachment — all driven through the
//! public `Bot` API.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use rustrade::{
    Bot, BotConfig, Brain, Candle, Capability, Decision, Exchange, ExchangeClient, MarketDataEvent,
    Order, OrderKind, Position, Price, Result, SizingConfig, StopKind, Symbol,
};

// ── Fixtures ────────────────────────────────────────────────────────────

/// Brain that emits a single configured decision on its first event, then
/// holds forever.
struct OnceBrain {
    decision: Mutex<Option<Decision>>,
}
impl OnceBrain {
    fn new(decision: Decision) -> Arc<Self> {
        Arc::new(Self {
            decision: Mutex::new(Some(decision)),
        })
    }
}
#[async_trait]
impl Brain for OnceBrain {
    fn name(&self) -> &str {
        "once"
    }
    async fn on_event(&self, _e: &MarketDataEvent, _p: &Position) -> Result<Decision> {
        Ok(self
            .decision
            .lock()
            .unwrap()
            .take()
            .unwrap_or_else(Decision::hold))
    }
}

/// Exchange that captures every placed order and advertises a configurable
/// capability set.
struct CapturingExchange {
    orders: Arc<Mutex<Vec<Order>>>,
    caps: Vec<Capability>,
}
impl CapturingExchange {
    fn new(caps: Vec<Capability>) -> (Arc<Self>, Arc<Mutex<Vec<Order>>>) {
        let orders = Arc::new(Mutex::new(Vec::new()));
        let inst = Arc::new(Self {
            orders: orders.clone(),
            caps,
        });
        (inst, orders)
    }
}
#[async_trait]
impl ExchangeClient for CapturingExchange {
    fn name(&self) -> &str {
        "capturing"
    }
    async fn place_order(&self, o: &Order) -> Result<String> {
        self.orders.lock().unwrap().push(o.clone());
        Ok(format!("ord-{}", self.orders.lock().unwrap().len()))
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
    fn supports(&self, c: Capability) -> bool {
        self.caps.contains(&c)
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

/// Boot a bot with the given brain + exchange, publish one candle, wait for
/// the order to land (or a timeout), shut down, and return captured orders.
async fn run_once(
    brain: Arc<dyn Brain>,
    exchange: Arc<dyn ExchangeClient>,
    orders: Arc<Mutex<Vec<Order>>>,
    expect_order: bool,
) -> Vec<Order> {
    let bot = Bot::new(
        BotConfig::builder()
            .name("intents")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            // margin 100 × lev 1 ÷ price 100 = 1 contract
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

    let handle = bot.handle();
    let bus = bot.market_data_bus().clone();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });

    tokio::time::sleep(Duration::from_millis(50)).await;
    bus.publish(candle_event("BTCUSDT", 100.0));

    // Poll briefly for the order (or confirm none lands).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let n = orders.lock().unwrap().len();
        if (expect_order && n >= 1) || tokio::time::Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Settle so a (wrongly) placed order would have been captured even when
    // we expected none.
    if !expect_order {
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
    orders.lock().unwrap().clone()
}

// ── Tests ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn limit_decision_places_limit_order() {
    let brain = OnceBrain::new(Decision::buy(1.0).with_limit_price(Price(99.5)));
    let (exchange, orders) = CapturingExchange::new(vec![]);
    let captured = run_once(brain, exchange, orders, true).await;

    assert_eq!(captured.len(), 1);
    let o = &captured[0];
    assert_eq!(o.kind, OrderKind::Limit);
    assert_eq!(o.limit_price, Some(Price(99.5)));
}

#[tokio::test(flavor = "multi_thread")]
async fn stop_price_attaches_stop_when_capable() {
    let brain = OnceBrain::new(Decision::buy(1.0).with_stop(Price(90.0)));
    let (exchange, orders) = CapturingExchange::new(vec![Capability::StopOrders]);
    let captured = run_once(brain, exchange, orders, true).await;

    assert_eq!(captured.len(), 1);
    let stop = captured[0].stop.expect("stop should be attached");
    assert_eq!(stop.trigger_price, Price(90.0));
    assert!(matches!(stop.kind, StopKind::StopMarket));
}

#[tokio::test(flavor = "multi_thread")]
async fn stop_price_dropped_without_capability_but_order_still_placed() {
    let brain = OnceBrain::new(Decision::buy(1.0).with_stop(Price(90.0)));
    let (exchange, orders) = CapturingExchange::new(vec![]); // no StopOrders
    let captured = run_once(brain, exchange, orders, true).await;

    assert_eq!(captured.len(), 1, "order is still placed, just unprotected");
    assert!(
        captured[0].stop.is_none(),
        "no stop attached when adapter lacks Capability::StopOrders"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn take_profit_only_attaches_take_profit() {
    let brain = OnceBrain::new(Decision::sell(1.0).with_take_profit(Price(110.0)));
    let (exchange, orders) = CapturingExchange::new(vec![Capability::StopOrders]);
    let captured = run_once(brain, exchange, orders, true).await;

    let stop = captured[0].stop.expect("take-profit should attach");
    assert!(matches!(stop.kind, StopKind::TakeProfit));
    assert_eq!(stop.trigger_price, Price(110.0));
}

#[tokio::test(flavor = "multi_thread")]
async fn post_only_blocked_when_adapter_lacks_capability() {
    let brain = OnceBrain::new(
        Decision::buy(1.0)
            .with_limit_price(Price(99.0))
            .with_order_kind(OrderKind::PostOnly),
    );
    let (exchange, orders) = CapturingExchange::new(vec![]); // no PostOnly
    let captured = run_once(brain, exchange, orders, false).await;

    assert!(
        captured.is_empty(),
        "post-only must be blocked when adapter can't honour it (no silent downgrade)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn post_only_placed_when_adapter_supports_it() {
    let brain = OnceBrain::new(
        Decision::buy(1.0)
            .with_limit_price(Price(99.0))
            .with_order_kind(OrderKind::PostOnly),
    );
    let (exchange, orders) = CapturingExchange::new(vec![Capability::PostOnly]);
    let captured = run_once(brain, exchange, orders, true).await;

    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].kind, OrderKind::PostOnly);
    assert_eq!(captured[0].limit_price, Some(Price(99.0)));
}

#[tokio::test(flavor = "multi_thread")]
async fn plain_market_buy_unaffected() {
    // Regression: a vanilla buy still produces a market order with no stop.
    let brain = OnceBrain::new(Decision::buy(1.0));
    let (exchange, orders) = CapturingExchange::new(vec![]);
    let captured = run_once(brain, exchange, orders, true).await;

    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].kind, OrderKind::Market);
    assert!(captured[0].stop.is_none());
    assert!(captured[0].limit_price.is_none());
}
