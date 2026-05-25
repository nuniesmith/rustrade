//! Reference pattern for embedding a rustrade `Bot` inside a larger host
//! service.
//!
//! Simulates a host that owns:
//!
//! - Its own tokio runtime
//! - Its own shutdown `CancellationToken`
//! - Its own "main loop" task (here: publishes a few synthetic ticks)
//!
//! The bot is wired into the host via:
//!
//! - `Bot::with_external_cancel(token)` — the host's shutdown token
//!   triggers the bot, no linker task needed
//! - `bot.market_data_bus()` — the host's main loop publishes here
//! - `bot.handle()` — the host can query health, await shutdown,
//!   subscribe to signals without holding the bot itself
//!
//! Run:
//! ```sh
//! cargo run -p embed-in-service
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use rustrade::{
    Bot, BotConfig, Brain, Candle, Decision, Exchange, ExchangeClient, MarketDataBus,
    MarketDataEvent, Order, Position, Result, SignalType, Symbol,
};
use tokio_util::sync::CancellationToken;

// ── A trivial host service ─────────────────────────────────────────────

/// Stand-in for the host's own state. In a real service this would be
/// an HTTP server, gRPC handler, or other top-level coordinator.
struct HostService {
    shutdown: CancellationToken,
}

impl HostService {
    fn new() -> Self {
        Self {
            shutdown: CancellationToken::new(),
        }
    }

    fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    fn trigger_shutdown(&self) {
        self.shutdown.cancel();
    }

    /// The host's own task — publishes ten synthetic candles, then asks
    /// itself to stop.
    async fn publish_loop(self: Arc<Self>, bus: MarketDataBus) {
        for i in 1..=10 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            bus.publish(MarketDataEvent::Candle {
                exchange: Exchange::from("host"),
                symbol: Symbol::from("BTCUSDT"),
                candle: Candle {
                    time: Utc::now().timestamp_millis(),
                    open: 100.0,
                    high: 100.0 + i as f64,
                    low: 100.0 - i as f64,
                    close: 100.0 + (i as f64) * 0.5,
                    volume: 1.0,
                },
            });
        }
        // Host is done — trigger its own shutdown. The bot's external-cancel
        // wiring picks this up and drains the supervisor.
        tracing::info!("host publish_loop done — triggering shutdown");
        self.trigger_shutdown();
    }
}

// ── Brain ──────────────────────────────────────────────────────────────

/// Brain that counts received events and emits a Buy on every 4th one
/// so the signal stream has something to show the host.
struct EveryNthBuyBrain {
    seen: AtomicUsize,
    n: usize,
}
impl EveryNthBuyBrain {
    fn new(n: usize) -> Arc<Self> {
        Arc::new(Self {
            seen: AtomicUsize::new(0),
            n,
        })
    }
}
#[async_trait]
impl Brain for EveryNthBuyBrain {
    fn name(&self) -> &str {
        "every-nth-buy"
    }
    async fn on_event(&self, _e: &MarketDataEvent, _p: &Position) -> Result<Decision> {
        let count = self.seen.fetch_add(1, Ordering::SeqCst) + 1;
        if count.is_multiple_of(self.n) {
            Ok(Decision::buy(0.5))
        } else {
            Ok(Decision::hold())
        }
    }
}

// ── Stub exchange ──────────────────────────────────────────────────────

struct StubExchange;
#[async_trait]
impl ExchangeClient for StubExchange {
    fn name(&self) -> &str {
        "stub"
    }
    async fn place_order(&self, _o: &Order) -> Result<String> {
        Ok("ord-1".into())
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
}

// ── Main ───────────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    rustrade::logging::init_tracing();

    let host = Arc::new(HostService::new());

    // Build the bot, tying its lifecycle to the host's shutdown token.
    let bot = Bot::new(
        BotConfig::builder()
            .name("embedded-bot")
            .symbol("BTCUSDT")
            // Host owns the signal handler — bot must not install one.
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            .build()?,
        Arc::new(StubExchange),
        vec![EveryNthBuyBrain::new(4)],
    )?
    .with_external_cancel(host.shutdown_token());

    let handle = bot.handle();
    let bus = bot.market_data_bus().clone();

    // Subscribe to signals — a real host would forward these to a dashboard.
    // The subscriber needs its own exit condition: dropping the bot does
    // not close the SignalBus, because BotHandle holds a Sender clone.
    // Here we tie the subscriber's lifetime to the host's shutdown token.
    let mut signals = handle.subscribe_signals();
    let signal_shutdown = host.shutdown_token();
    let signal_logger = tokio::spawn(async move {
        let mut buys = 0usize;
        loop {
            tokio::select! {
                _ = signal_shutdown.cancelled() => break,
                r = signals.recv() => match r {
                    Ok(sig) if matches!(sig.kind, SignalType::Buy) => {
                        buys += 1;
                        tracing::info!(
                            source = %sig.source,
                            symbol = %sig.symbol,
                            confidence = sig.confidence,
                            buy_count = buys,
                            "host received Buy signal from bot"
                        );
                    }
                    Ok(_) => {}
                    Err(_) => break, // channel closed
                }
            }
        }
        buys
    });

    // Spawn the bot in the host's runtime.
    let bot_task = tokio::spawn(async move { bot.run_until_shutdown().await });

    // Spawn the host's own task. It publishes events to the bot's bus,
    // then triggers shutdown via the host's token.
    let host_loop = tokio::spawn(host.clone().publish_loop(bus));

    // Optionally: the host can also await the bot's shutdown directly.
    handle.await_shutdown().await;
    tracing::info!("host observed bot shutdown");

    let _ = host_loop.await;
    bot_task.await??;
    let buys_observed = signal_logger.await.unwrap_or_default();

    tracing::info!(buys_observed, "embed-in-service finished");
    // 10 events, every 4th → 2 buys (4 and 8).
    assert_eq!(buys_observed, 2);
    Ok(())
}
