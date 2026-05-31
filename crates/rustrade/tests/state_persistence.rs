//! End-to-end: per-symbol risk state survives a simulated restart when a
//! `StateStore` is wired. Two `Bot` instances share one `InMemoryStore`;
//! the first trips the breaker and halts the session via the handle, the
//! second restores that state on startup and re-persists on shutdown.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rustrade::{Bot, BotConfig, CircuitBreakerConfig, SessionPnlConfig};
use rustrade_core::{
    Brain, Decision, ExchangeClient, InMemoryStore, MarketDataEvent, Order, Position, Result,
    StateStore, Symbol,
};

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

struct NoopExchange;
#[async_trait]
impl ExchangeClient for NoopExchange {
    fn name(&self) -> &str {
        "noop"
    }
    async fn place_order(&self, _o: &Order) -> Result<String> {
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
}

fn config() -> BotConfig {
    BotConfig::builder()
        .name("persist-bot")
        .symbol("BTCUSDT")
        .without_signal_handler()
        .shutdown_timeout(Duration::from_secs(2))
        // Tight limits so a single recorded loss trips both gates.
        .session_pnl_config(SessionPnlConfig { loss_limit: -10.0 })
        .circuit_breaker_config(CircuitBreakerConfig {
            loss_limit: 1,
            window_secs: 14_400,
            cooldown_secs: 3_600,
        })
        .build()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn risk_state_survives_restart_via_shared_store() {
    let store: Arc<dyn StateStore> = Arc::new(InMemoryStore::new());
    let sym = Symbol::from("BTCUSDT");
    const KEY: &str = "persist-bot/risk/BTCUSDT";

    // ── First run: trip the breaker + halt the session, then shut down. ──
    let bot1 = Bot::new(config(), Arc::new(NoopExchange), vec![Arc::new(NoopBrain)])
        .unwrap()
        .with_state_store(store.clone());
    let handle1 = bot1.handle();
    let task1 = tokio::spawn(async move { bot1.run_until_shutdown().await });

    // No startup settle needed: record_trade_outcome writes the shared risk
    // map directly, and the awaited task1.await after shutdown guarantees the
    // bot's persist_all ran (writing the snapshot) before we assert on it.
    // (A fixed yield count here would be fragile on a slow CI runner.)
    handle1.record_trade_outcome(&sym, -50.0, 1.0).await;

    handle1.shutdown();
    task1.await.unwrap().unwrap();

    // The store now holds a snapshot for this bot/symbol.
    assert!(
        store.load(KEY).await.unwrap().is_some(),
        "first run should have persisted a snapshot"
    );

    // ── Second run: restore from the same store, then shut down. ──
    let bot2 = Bot::new(config(), Arc::new(NoopExchange), vec![Arc::new(NoopBrain)])
        .unwrap()
        .with_state_store(store.clone());
    let handle2 = bot2.handle();
    let task2 = tokio::spawn(async move { bot2.run_until_shutdown().await });

    // bot2 restores synchronously at startup and re-persists on shutdown; the
    // awaited task2.await guarantees that completes before the final store
    // assertion below, so no startup settle is needed here either.
    assert!(handle2.health().await.healthy, "healthy after restore");

    handle2.shutdown();
    task2.await.unwrap().unwrap();

    // Proof of round-trip through the public API only: bot2 re-persists on
    // shutdown. Had it NOT restored the prior state, that persist would
    // have overwritten the snapshot with a fresh (un-halted, zero-PnL) one.
    // The snapshot still showing the prior loss + trip means bot2 genuinely
    // loaded what bot1 saved.
    let value = store
        .load(KEY)
        .await
        .unwrap()
        .expect("snapshot present after second run");
    let pnl = &value["session_pnl"];
    assert_eq!(
        pnl["halted"],
        serde_json::json!(true),
        "halt persisted: {value}"
    );
    assert_eq!(pnl["losses"], serde_json::json!(1));
    assert!((pnl["realised"].as_f64().unwrap() - (-50.0)).abs() < 1e-9);
    assert!(
        value["circuit_breaker"]["tripped_at_unix_secs"].is_number(),
        "breaker trip time persisted: {value}"
    );
}
