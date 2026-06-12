//! Pending-entry reservations: the portfolio gate under concurrency.
//!
//! Before the reservation ledger, the gate read only the position cache —
//! which doesn't reflect an order until its fill is processed — so two
//! brains deciding concurrently could BOTH pass `max_concurrent_positions`
//! or the gross-exposure cap and both place. These tests pin that the gate
//! is now check-and-reserve, and that reservations are released when the
//! exchange rejects the order.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use rustrade::{
    Bot, BotConfig, Brain, Candle, Decision, Exchange, ExchangeClient, MarketDataEvent, Order,
    PortfolioRiskConfig, Position, Result, SizingConfig, Symbol,
};

// ── Fixtures ────────────────────────────────────────────────────────────

/// Exchange that counts accepted orders and can reject the first N.
struct GateExchange {
    placed: Arc<AtomicU64>,
    rejected: Arc<AtomicU64>,
    reject_first: AtomicU64,
    positions: Mutex<HashMap<Symbol, Position>>,
}
impl GateExchange {
    fn new(reject_first: u64) -> (Arc<Self>, Arc<AtomicU64>, Arc<AtomicU64>) {
        let placed = Arc::new(AtomicU64::new(0));
        let rejected = Arc::new(AtomicU64::new(0));
        (
            Arc::new(Self {
                placed: placed.clone(),
                rejected: rejected.clone(),
                reject_first: AtomicU64::new(reject_first),
                positions: Mutex::new(HashMap::new()),
            }),
            placed,
            rejected,
        )
    }
}
#[async_trait]
impl ExchangeClient for GateExchange {
    fn name(&self) -> &str {
        "gate-ex"
    }
    async fn place_order(&self, _o: &Order) -> Result<String> {
        // Hold every concurrent caller inside place_order briefly so the
        // gate→exchange window of racing brains genuinely overlaps.
        tokio::time::sleep(Duration::from_millis(30)).await;
        if self
            .reject_first
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            self.rejected.fetch_add(1, Ordering::SeqCst);
            return Err(rustrade::Error::exchange("synthetic rejection"));
        }
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

/// Brain that owns one symbol and buys it — either on every event, or
/// only once (so a concurrency test sees exactly one entry per brain and
/// same-symbol pyramiding can't muddy the count).
struct OwnedBuyBrain {
    name: String,
    symbol: Symbol,
    once: bool,
    fired: Mutex<bool>,
}
impl OwnedBuyBrain {
    fn every(symbol: &str) -> Arc<Self> {
        Self::build(symbol, false)
    }
    fn once(symbol: &str) -> Arc<Self> {
        Self::build(symbol, true)
    }
    fn build(symbol: &str, once: bool) -> Arc<Self> {
        Arc::new(Self {
            name: format!("buy[{symbol}]"),
            symbol: Symbol::from(symbol),
            once,
            fired: Mutex::new(false),
        })
    }
}
#[async_trait]
impl Brain for OwnedBuyBrain {
    fn name(&self) -> &str {
        &self.name
    }
    fn owned_symbols(&self) -> Option<Vec<Symbol>> {
        Some(vec![self.symbol.clone()])
    }
    async fn on_event(&self, _e: &MarketDataEvent, _p: &Position) -> Result<Decision> {
        if self.once {
            let mut fired = self.fired.lock().unwrap();
            if *fired {
                return Ok(Decision::hold());
            }
            *fired = true;
        }
        Ok(Decision::buy(1.0))
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

/// `margin_per_trade` 1000 at price 100 → 10 contracts → 1000 notional.
fn sizing() -> SizingConfig {
    SizingConfig {
        margin_per_trade: 1_000.0,
        leverage: 1,
        max_contracts: 100,
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
async fn concurrent_brains_cannot_exceed_max_concurrent_positions() {
    // Two brains, two symbols, a one-position cap. Without the reservation
    // ledger both gates read "0 open" (no fill source → the cache never
    // updates) and both place. With it, exactly one wins.
    let (exchange, placed, _rejected) = GateExchange::new(0);
    let bot = Bot::new(
        BotConfig::builder()
            .name("race")
            .symbols(["AAA", "BBB"])
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .sizing_config(sizing())
            .portfolio_config(PortfolioRiskConfig {
                max_daily_loss: f64::NEG_INFINITY,
                max_concurrent_positions: 1,
                max_gross_exposure: f64::INFINITY,
            })
            .build()
            .unwrap(),
        exchange,
        vec![OwnedBuyBrain::once("AAA"), OwnedBuyBrain::once("BBB")],
    )
    .unwrap();

    let bus = bot.market_data_bus().clone();
    let handle = bot.handle();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });

    // Fire both symbols' candles back-to-back, repeatedly, so the two
    // execution services race through the gate together.
    let one = eventually(10, || {
        if placed.load(Ordering::SeqCst) >= 1 {
            true
        } else {
            bus.publish(candle_event("AAA", 100.0));
            bus.publish(candle_event("BBB", 100.0));
            false
        }
    })
    .await;
    assert!(one, "at least one entry must pass the gate");

    // Give the loser's in-flight attempt time to land if it (wrongly) got
    // through, then assert the cap held.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        placed.load(Ordering::SeqCst),
        1,
        "max_concurrent_positions=1 must hold across concurrent brains"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_brains_cannot_exceed_gross_exposure_cap() {
    // Each entry is 1000 notional; the cap fits one (1500). Two brains on
    // two symbols race — only one entry may pass.
    let (exchange, placed, _rejected) = GateExchange::new(0);
    let bot = Bot::new(
        BotConfig::builder()
            .name("race-gross")
            .symbols(["AAA", "BBB"])
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .sizing_config(sizing())
            .portfolio_config(PortfolioRiskConfig {
                max_daily_loss: f64::NEG_INFINITY,
                max_concurrent_positions: 0,
                max_gross_exposure: 1_500.0,
            })
            .build()
            .unwrap(),
        exchange,
        vec![OwnedBuyBrain::once("AAA"), OwnedBuyBrain::once("BBB")],
    )
    .unwrap();

    let bus = bot.market_data_bus().clone();
    let handle = bot.handle();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });

    let one = eventually(10, || {
        if placed.load(Ordering::SeqCst) >= 1 {
            true
        } else {
            bus.publish(candle_event("AAA", 100.0));
            bus.publish(candle_event("BBB", 100.0));
            false
        }
    })
    .await;
    assert!(one, "at least one entry must pass the gate");

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        placed.load(Ordering::SeqCst),
        1,
        "gross-exposure cap must count the winner's pending reservation"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rejected_entry_releases_its_reservation() {
    // The exchange rejects the first order. The cap (1500) fits only one
    // 1000-notional reservation at a time — so the retry on the next
    // candle only passes the gate if the rejected attempt's reservation
    // was released.
    let (exchange, placed, rejected) = GateExchange::new(1);
    let bot = Bot::new(
        BotConfig::builder()
            .name("release")
            .symbol("AAA")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .sizing_config(sizing())
            .portfolio_config(PortfolioRiskConfig {
                max_daily_loss: f64::NEG_INFINITY,
                max_concurrent_positions: 0,
                max_gross_exposure: 1_500.0,
            })
            .build()
            .unwrap(),
        exchange,
        vec![OwnedBuyBrain::every("AAA")],
    )
    .unwrap();

    let bus = bot.market_data_bus().clone();
    let handle = bot.handle();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });

    let accepted = eventually(10, || {
        if placed.load(Ordering::SeqCst) >= 1 {
            true
        } else {
            bus.publish(candle_event("AAA", 100.0));
            false
        }
    })
    .await;
    assert_eq!(rejected.load(Ordering::SeqCst), 1, "first attempt rejected");
    assert!(
        accepted,
        "the retry must pass the gate after the rejection released its reservation"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}
