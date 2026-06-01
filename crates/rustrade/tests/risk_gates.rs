//! Integration tests for the risk-gated `ExecutionService`.
//!
//! Exercises the gate sequence end-to-end through the public `Bot` API:
//! brain emits a buy/sell → execution checks `SessionPnl::is_session_halted`
//! → `CircuitBreaker::is_tripped` → `PositionSizer::contracts` → places
//! order. Each gate is verified by setting up the state that should block
//! and asserting the exchange was not called.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use rustrade::{
    AssetClass, Bot, BotConfig, Brain, Candle, CircuitBreakerConfig, Decision, Exchange,
    ExchangeClient, InstrumentSpec, MarketDataEvent, Order, PortfolioRiskConfig, Position, Result,
    RiskConfig, SessionPnlConfig, SignalType, SizingConfig, Symbol,
};

// ── Fixtures ────────────────────────────────────────────────────────────

/// Brain that always emits a fixed signal.
struct FixedSignalBrain {
    signal: SignalType,
}
#[async_trait]
impl Brain for FixedSignalBrain {
    fn name(&self) -> &str {
        "fixed"
    }
    async fn on_event(&self, _e: &MarketDataEvent, _p: &Position) -> Result<Decision> {
        Ok(match self.signal {
            SignalType::Hold => Decision::hold(),
            SignalType::Buy => Decision::buy(1.0),
            SignalType::Sell => Decision::sell(1.0),
            SignalType::Close => Decision::close(),
        })
    }
}

/// Exchange that counts orders + closes and exposes settable per-symbol
/// positions (so `Bot::prefetch_positions` loads what the test wants).
struct CountingExchange {
    placed: Arc<AtomicU64>,
    closed: Arc<AtomicU64>,
    positions: Mutex<HashMap<Symbol, Position>>,
    min_notional: Mutex<f64>,
    asset_class: Mutex<AssetClass>,
    /// Size (contracts) of the most recently placed order.
    last_size: AtomicU64,
}
impl CountingExchange {
    fn new() -> (Arc<Self>, Arc<AtomicU64>, Arc<AtomicU64>) {
        let placed = Arc::new(AtomicU64::new(0));
        let closed = Arc::new(AtomicU64::new(0));
        let inst = Arc::new(Self {
            placed: placed.clone(),
            closed: closed.clone(),
            positions: Mutex::new(HashMap::new()),
            min_notional: Mutex::new(0.0),
            asset_class: Mutex::new(AssetClass::CryptoSpot),
            last_size: AtomicU64::new(0),
        });
        (inst, placed, closed)
    }

    fn set_position(&self, sym: Symbol, pos: Position) {
        self.positions.lock().unwrap().insert(sym, pos);
    }

    fn set_min_notional(&self, v: f64) {
        *self.min_notional.lock().unwrap() = v;
    }

    fn set_asset_class(&self, class: AssetClass) {
        *self.asset_class.lock().unwrap() = class;
    }

    fn last_size(&self) -> u64 {
        self.last_size.load(Ordering::SeqCst)
    }
}
#[async_trait]
impl ExchangeClient for CountingExchange {
    fn name(&self) -> &str {
        "counting"
    }
    async fn place_order(&self, o: &Order) -> Result<String> {
        self.last_size
            .store(o.size.value() as u64, Ordering::SeqCst);
        let n = self.placed.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(format!("ord-{n}"))
    }
    async fn cancel_all(&self, _s: &Symbol) -> Result<usize> {
        Ok(0)
    }
    async fn close_position(&self, _s: &Symbol, _p: &Position) -> Result<String> {
        let n = self.closed.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(format!("close-{n}"))
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
    fn instrument_spec(&self, _s: &Symbol) -> InstrumentSpec {
        InstrumentSpec {
            asset_class: *self.asset_class.lock().unwrap(),
            min_notional: *self.min_notional.lock().unwrap(),
            ..InstrumentSpec::default()
        }
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

/// Wait until `f()` returns true or the deadline expires.
async fn wait_until<F>(mut f: F, timeout: Duration, msg: &str)
where
    F: FnMut() -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout;
    while !f() {
        if tokio::time::Instant::now() > deadline {
            panic!("timed out: {msg}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn happy_path_buy_places_order() {
    let brain = Arc::new(FixedSignalBrain {
        signal: SignalType::Buy,
    });
    let (exchange, placed, _) = CountingExchange::new();

    // Sizing: margin=100, leverage=1, price=100, cv=1 ⇒ 1 contract.
    let bot = Bot::new(
        BotConfig::builder()
            .name("happy")
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
        exchange,
        vec![brain],
    )
    .unwrap();

    let handle = bot.handle();
    let bus = bot.market_data_bus().clone();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });

    tokio::time::sleep(Duration::from_millis(50)).await;
    bus.publish(candle_event("BTCUSDT", 100.0));

    wait_until(
        || placed.load(Ordering::SeqCst) == 1,
        Duration::from_secs(2),
        "buy order never placed",
    )
    .await;

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}

#[tokio::test(start_paused = true)]
async fn session_halt_blocks_buy_order() {
    let brain = Arc::new(FixedSignalBrain {
        signal: SignalType::Buy,
    });
    let (exchange, placed, _closed) = CountingExchange::new();
    let bot = Bot::new(
        BotConfig::builder()
            .name("halt")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .session_pnl_config(SessionPnlConfig { loss_limit: -1.0 })
            .build()
            .unwrap(),
        exchange,
        vec![brain],
    )
    .unwrap();

    let handle = bot.handle();
    // Trip the halt: -10 net ≤ -1 limit.
    handle
        .record_trade_outcome(&Symbol::from("BTCUSDT"), -10.0, 0.0)
        .await;

    let bus = bot.market_data_bus().clone();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    bus.publish(candle_event("BTCUSDT", 100.0));
    bus.publish(candle_event("BTCUSDT", 100.0));
    bus.publish(candle_event("BTCUSDT", 100.0));
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        placed.load(Ordering::SeqCst),
        0,
        "session halt must block every buy order"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}

#[tokio::test(start_paused = true)]
async fn circuit_breaker_blocks_buy_order() {
    let brain = Arc::new(FixedSignalBrain {
        signal: SignalType::Buy,
    });
    let (exchange, placed, _closed) = CountingExchange::new();
    let bot = Bot::new(
        BotConfig::builder()
            .name("breaker")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .circuit_breaker_config(CircuitBreakerConfig {
                loss_limit: 1,
                window_secs: 3600,
                cooldown_secs: 3600,
            })
            .build()
            .unwrap(),
        exchange,
        vec![brain],
    )
    .unwrap();

    let handle = bot.handle();
    // Trip the breaker: 1 loss is enough.
    handle
        .record_trade_outcome(&Symbol::from("BTCUSDT"), -5.0, 0.0)
        .await;

    let bus = bot.market_data_bus().clone();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    bus.publish(candle_event("BTCUSDT", 100.0));
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(
        placed.load(Ordering::SeqCst),
        0,
        "tripped circuit breaker must block buy orders"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}

#[tokio::test(start_paused = true)]
async fn portfolio_daily_loss_halt_blocks_buy_order() {
    let brain = Arc::new(FixedSignalBrain {
        signal: SignalType::Buy,
    });
    let (exchange, placed, _closed) = CountingExchange::new();
    let bot = Bot::new(
        BotConfig::builder()
            .name("pf-halt")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            // Per-symbol caps stay loose so ONLY the account gate can block.
            .session_pnl_config(SessionPnlConfig {
                loss_limit: -10_000.0,
            })
            .portfolio_config(PortfolioRiskConfig {
                max_daily_loss: -1.0,
                ..Default::default()
            })
            .build()
            .unwrap(),
        exchange,
        vec![brain],
    )
    .unwrap();

    let handle = bot.handle();
    // -10 net on the only symbol → account net -10 ≤ -1 → portfolio halt
    // (the per-symbol session, capped at -10_000, is NOT halted).
    handle
        .record_trade_outcome(&Symbol::from("BTCUSDT"), -10.0, 0.0)
        .await;

    let bus = bot.market_data_bus().clone();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    bus.publish(candle_event("BTCUSDT", 100.0));
    bus.publish(candle_event("BTCUSDT", 100.0));
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        placed.load(Ordering::SeqCst),
        0,
        "account daily-loss halt must block every buy"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}

#[tokio::test(start_paused = true)]
async fn portfolio_max_concurrent_blocks_new_symbol() {
    let brain = Arc::new(FixedSignalBrain {
        signal: SignalType::Buy,
    });
    let (exchange, placed, _closed) = CountingExchange::new();
    // BTC already holds a position → consumes the single concurrency slot.
    exchange.set_position(
        Symbol::from("BTCUSDT"),
        Position {
            qty: 1.0,
            entry_price: Some(100.0),
            unrealised_pnl: 0.0,
        },
    );

    let bot = Bot::new(
        BotConfig::builder()
            .name("pf-concurrent")
            .symbols(["BTCUSDT", "ETHUSDT"])
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            // margin 100 × 1x at price 100, cv 1 ⇒ 1 contract (passes the sizer).
            .sizing_config(SizingConfig {
                margin_per_trade: 100.0,
                leverage: 1,
                max_contracts: 10,
            })
            .portfolio_config(PortfolioRiskConfig {
                max_concurrent_positions: 1,
                ..Default::default()
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
    // Allow the startup position prefetch to load BTC into the cache.
    tokio::time::sleep(Duration::from_millis(80)).await;
    // ETH is a NEW symbol; the one allowed slot is taken by BTC.
    bus.publish(candle_event("ETHUSDT", 100.0));
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        placed.load(Ordering::SeqCst),
        0,
        "a new-symbol entry must be blocked at the concurrency cap"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}

#[tokio::test(start_paused = true)]
async fn per_class_risk_sizes_by_asset_class() {
    let brain = Arc::new(FixedSignalBrain {
        signal: SignalType::Buy,
    });
    let (exchange, placed, _closed) = CountingExchange::new();
    // The symbol trades as FX, so the FX class override should apply.
    exchange.set_asset_class(AssetClass::Fx);

    let bot = Bot::new(
        BotConfig::builder()
            .name("class-sizing")
            .symbol("EURUSD")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            // Default sizing would give 1 contract (1× leverage); the FX class
            // override gives 10 (10× leverage). cv = 1 from the spec.
            .sizing_config(SizingConfig {
                margin_per_trade: 100.0,
                leverage: 1,
                max_contracts: 100,
            })
            .class_risk(
                AssetClass::Fx,
                RiskConfig {
                    sizing: SizingConfig {
                        margin_per_trade: 100.0,
                        leverage: 10,
                        max_contracts: 100,
                    },
                    ..Default::default()
                },
            )
            .build()
            .unwrap(),
        exchange.clone(),
        vec![brain],
    )
    .unwrap();

    let handle = bot.handle();
    let bus = bot.market_data_bus().clone();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    bus.publish(candle_event("EURUSD", 100.0));

    wait_until(
        || placed.load(Ordering::SeqCst) == 1,
        Duration::from_secs(2),
        "order never placed",
    )
    .await;

    // 100 margin × 10 leverage / (100 price × 1 cv) = 10 contracts.
    assert_eq!(
        exchange.last_size(),
        10,
        "the FX class override (10× leverage) should size the order, not the 1× default"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}

#[tokio::test(start_paused = true)]
async fn instrument_min_notional_blocks_small_order() {
    let brain = Arc::new(FixedSignalBrain {
        signal: SignalType::Buy,
    });
    let (exchange, placed, _closed) = CountingExchange::new();
    // margin 100 × 1x at price 100, cv 1 ⇒ 1 contract ⇒ notional 100.
    // A 1000 min-notional rejects it.
    exchange.set_min_notional(1000.0);

    let bot = Bot::new(
        BotConfig::builder()
            .name("min-notional")
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
        exchange,
        vec![brain],
    )
    .unwrap();

    let handle = bot.handle();
    let bus = bot.market_data_bus().clone();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    bus.publish(candle_event("BTCUSDT", 100.0));
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(
        placed.load(Ordering::SeqCst),
        0,
        "an order below the instrument min notional must be blocked"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}

#[tokio::test(start_paused = true)]
async fn sizer_zero_blocks_buy_order() {
    let brain = Arc::new(FixedSignalBrain {
        signal: SignalType::Buy,
    });
    let (exchange, placed, _closed) = CountingExchange::new();
    let bot = Bot::new(
        BotConfig::builder()
            .name("sizer-zero")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .sizing_config(SizingConfig {
                margin_per_trade: 0.01,
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
    bus.publish(candle_event("BTCUSDT", 50_000.0));
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(
        placed.load(Ordering::SeqCst),
        0,
        "sizer returning 0 must block the order"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}

#[tokio::test(start_paused = true)]
async fn close_positions_on_shutdown_invokes_close() {
    let brain = Arc::new(FixedSignalBrain {
        signal: SignalType::Hold,
    });
    let (exchange, _placed, closed) = CountingExchange::new();

    // Make the prefetch on Bot::run_until_shutdown load a non-flat
    // position into the cache, so close-on-shutdown has something to do.
    exchange.set_position(
        Symbol::from("BTCUSDT"),
        Position {
            qty: 3.0,
            entry_price: Some(100.0),
            unrealised_pnl: 0.0,
        },
    );

    let bot = Bot::new(
        BotConfig::builder()
            .name("close-on-shutdown")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .close_positions_on_shutdown(true)
            .build()
            .unwrap(),
        exchange,
        vec![brain],
    )
    .unwrap();

    let handle = bot.handle();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;

    assert_eq!(
        closed.load(Ordering::SeqCst),
        1,
        "exchange.close_position should fire once for the open position"
    );
}

#[tokio::test(start_paused = true)]
async fn close_decision_emits_reduce_only_order_against_position() {
    let brain = Arc::new(FixedSignalBrain {
        signal: SignalType::Close,
    });
    let (exchange, placed, _closed) = CountingExchange::new();
    exchange.set_position(
        Symbol::from("BTCUSDT"),
        Position {
            qty: 5.0,
            entry_price: Some(100.0),
            unrealised_pnl: 0.0,
        },
    );

    let bot = Bot::new(
        BotConfig::builder()
            .name("close-decision")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
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

    wait_until(
        || placed.load(Ordering::SeqCst) == 1,
        Duration::from_secs(2),
        "close-decision order never placed",
    )
    .await;

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}

#[tokio::test(start_paused = true)]
async fn close_decision_on_flat_position_is_silent_noop() {
    let brain = Arc::new(FixedSignalBrain {
        signal: SignalType::Close,
    });
    let (exchange, placed, _closed) = CountingExchange::new();
    let bot = Bot::new(
        BotConfig::builder()
            .name("close-when-flat")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .build()
            .unwrap(),
        exchange,
        vec![brain],
    )
    .unwrap();

    let bus = bot.market_data_bus().clone();
    let handle = bot.handle();
    let task = tokio::spawn(async move { bot.run_until_shutdown().await });

    tokio::time::sleep(Duration::from_millis(50)).await;
    bus.publish(candle_event("BTCUSDT", 100.0));
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(
        placed.load(Ordering::SeqCst),
        0,
        "Close against a flat position must not place an order"
    );

    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
}
