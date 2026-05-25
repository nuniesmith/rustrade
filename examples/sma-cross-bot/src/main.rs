//! A toy SMA-crossover strategy against a deterministic candle replay.
//!
//! Demonstrates that any `Brain` implementation works end-to-end:
//!
//! - `SmaCrossBrain` tracks fast(`FAST`) and slow(`SLOW`) simple moving
//!   averages of candle closes and emits a `Buy` when the fast crosses
//!   above the slow / `Sell` when it crosses below. State lives behind
//!   a `Mutex` because `Brain::on_event` takes `&self` (see the
//!   `rustrade-core::brain` docs on interior mutability).
//! - `ReplaySource` is a `MarketSource` that walks a hard-coded
//!   sinusoid; the same seed → the same crossings → the same orders.
//! - `RecordingExchange` counts orders by side so the test below can
//!   pin down "this brain, this series → exactly N buys + N sells".
//!
//! Run:
//! ```sh
//! cargo run -p sma-cross-bot
//! ```

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use rustrade::{
    Bot, BotConfig, Brain, Candle, Decision, Exchange, ExchangeClient, MarketDataBus,
    MarketDataEvent, MarketSource, Order, Position, Result, Side, Symbol,
};

const FAST: usize = 5;
const SLOW: usize = 20;
const SYMBOL: &str = "BTCUSDT";

// ── Brain ──────────────────────────────────────────────────────────────

struct SmaCrossBrain {
    state: Mutex<SmaState>,
    /// Total non-Hold decisions emitted by this brain (Buy + Sell).
    /// Read by tests as the deterministic ground truth — signal-bus
    /// subscribers see the same numbers but are subject to drain timing.
    non_hold_emitted: Arc<AtomicUsize>,
}

#[derive(Default)]
struct SmaState {
    closes: Vec<f64>,
    /// `Some(true)` = fast was above slow on the last decided bar;
    /// `Some(false)` = fast was below; `None` until both windows are
    /// warm. Used to detect *crossings* rather than just "fast > slow".
    last_fast_above: Option<bool>,
}

impl SmaCrossBrain {
    fn new() -> (Arc<Self>, Arc<AtomicUsize>) {
        let non_hold_emitted = Arc::new(AtomicUsize::new(0));
        let brain = Arc::new(Self {
            state: Mutex::new(SmaState::default()),
            non_hold_emitted: non_hold_emitted.clone(),
        });
        (brain, non_hold_emitted)
    }
}

fn sma(window: &[f64], n: usize) -> Option<f64> {
    if window.len() < n {
        return None;
    }
    let slice = &window[window.len() - n..];
    Some(slice.iter().sum::<f64>() / n as f64)
}

#[async_trait]
impl Brain for SmaCrossBrain {
    fn name(&self) -> &str {
        "sma-cross"
    }
    async fn on_event(&self, event: &MarketDataEvent, _p: &Position) -> Result<Decision> {
        let close = match event {
            MarketDataEvent::Candle { candle, .. } => candle.close,
            // This bot only cares about candles.
            _ => return Ok(Decision::hold()),
        };

        let mut st = self.state.lock().expect("sma state mutex poisoned");
        st.closes.push(close);
        // Keep the buffer bounded to SLOW; we never look further back.
        if st.closes.len() > SLOW * 2 {
            let drop = st.closes.len() - SLOW * 2;
            st.closes.drain(0..drop);
        }

        let (Some(fast), Some(slow)) = (sma(&st.closes, FAST), sma(&st.closes, SLOW)) else {
            return Ok(Decision::hold());
        };
        let fast_above = fast > slow;

        let decision = match st.last_fast_above {
            Some(prev) if prev != fast_above => {
                self.non_hold_emitted.fetch_add(1, Ordering::SeqCst);
                if fast_above {
                    Decision::buy(0.9)
                } else {
                    Decision::sell(0.9)
                }
            }
            _ => Decision::hold(),
        };
        st.last_fast_above = Some(fast_above);
        Ok(decision)
    }
}

// ── Replay source ──────────────────────────────────────────────────────

/// `MarketSource` that publishes a fixed series of candles, paced by
/// `tick_interval`, then awaits cancellation (drops cleanly when the
/// wrapping `TradingService` is cancelled).
struct ReplaySource {
    bus: MarketDataBus,
    closes: Vec<f64>,
    tick_interval: Duration,
}

impl ReplaySource {
    fn new(bus: MarketDataBus, closes: Vec<f64>, tick_interval: Duration) -> Self {
        Self {
            bus,
            closes,
            tick_interval,
        }
    }
}

#[async_trait]
impl MarketSource for ReplaySource {
    fn name(&self) -> &str {
        "replay-source"
    }
    fn is_live(&self) -> bool {
        true
    }
    async fn run(&self) -> Result<()> {
        // Initial warmup: let the supervisor finish spawning all services
        // and the ExecutionService subscribe to the bus before we publish
        // anything. The broadcast bus doesn't replay events that landed
        // before a receiver subscribed.
        tokio::time::sleep(Duration::from_millis(50)).await;
        for (i, close) in self.closes.iter().enumerate() {
            tokio::time::sleep(self.tick_interval).await;
            self.bus.publish(MarketDataEvent::Candle {
                exchange: Exchange::from("replay"),
                symbol: Symbol::from(SYMBOL),
                candle: Candle {
                    time: i as i64,
                    open: *close,
                    high: *close,
                    low: *close,
                    close: *close,
                    volume: 1.0,
                },
            });
        }
        // Once the series is exhausted, sleep forever — the wrapping
        // TradingService will drop us at cancellation.
        std::future::pending::<()>().await;
        Ok(())
    }
}

// ── Exchange ───────────────────────────────────────────────────────────

#[derive(Default)]
struct RecordingExchange {
    buys: AtomicUsize,
    sells: AtomicUsize,
}

#[async_trait]
impl ExchangeClient for RecordingExchange {
    fn name(&self) -> &str {
        "recording"
    }
    async fn place_order(&self, order: &Order) -> Result<String> {
        match order.side {
            Side::Buy => self.buys.fetch_add(1, Ordering::SeqCst),
            Side::Sell => self.sells.fetch_add(1, Ordering::SeqCst),
        };
        Ok("ok".into())
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

// ── Series ─────────────────────────────────────────────────────────────

/// Deterministic sinusoidal close series: enough cycles to produce
/// multiple SMA crossovers.
fn series(n: usize) -> Vec<f64> {
    (0..n)
        .map(|i| 100.0 + 10.0 * (i as f64 * 0.15).sin())
        .collect()
}

// ── Main ───────────────────────────────────────────────────────────────

async fn run_bot(closes: Vec<f64>, tick: Duration) -> anyhow::Result<(usize, usize, usize)> {
    let exchange = Arc::new(RecordingExchange::default());
    let (brain, brain_non_hold) = SmaCrossBrain::new();
    let bot = Bot::new(
        BotConfig::builder()
            .name("sma-cross-bot")
            .symbol(SYMBOL)
            .without_signal_handler()
            .shutdown_timeout(Duration::from_secs(2))
            // Pick sizing where the sizer always returns ≥1 contract over
            // the price range of `series()` (closes ∈ [90, 110]). At
            // price=110, contract_value=1.0, leverage=1:
            //   contracts = floor(margin / 110) ⇒ margin ≥ 110 yields 1.
            // Use margin=1000 for headroom; max_contracts caps the upside.
            .sizing_config(rustrade::SizingConfig {
                margin_per_trade: 1000.0,
                leverage: 1,
                max_contracts: 100,
            })
            .build()?,
        exchange.clone(),
        vec![brain],
    )?;

    let bus = bot.market_data_bus().clone();
    let total = closes.len();
    let source = Arc::new(ReplaySource::new(bus, closes, tick));
    let bot = bot.with_market_source(source);

    let handle = bot.handle();
    let bot_task = tokio::spawn(async move { bot.run_until_shutdown().await });

    // Replay duration + warmup + slack for the last few events to flow.
    let runtime = tick * (total as u32) + Duration::from_millis(300);
    tokio::time::sleep(runtime).await;
    handle.shutdown();
    bot_task.await??;

    Ok((
        exchange.buys.load(Ordering::SeqCst),
        exchange.sells.load(Ordering::SeqCst),
        brain_non_hold.load(Ordering::SeqCst),
    ))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    rustrade::logging::init_tracing();

    let closes = series(200);
    let n = closes.len();
    let (buys, sells, brain_signals) = run_bot(closes, Duration::from_millis(2)).await?;

    tracing::info!(
        candles = n,
        brain_signals,
        buys,
        sells,
        "sma-cross-bot replay finished"
    );

    // The series produces multiple cycles of buy/sell; expecting some of each.
    assert!(
        buys >= 1 && sells >= 1,
        "expected at least one of each side"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic-replay invariant: same input series → same order
    /// count, every run. If the SMA logic or the gate sequence regresses
    /// this number will move.
    #[tokio::test(flavor = "multi_thread")]
    async fn deterministic_buy_sell_counts_on_known_series() {
        let closes = series(200);
        let (buys_a, sells_a, brain_signals_a) = run_bot(closes.clone(), Duration::from_millis(1))
            .await
            .unwrap();
        let (buys_b, sells_b, brain_signals_b) =
            run_bot(closes, Duration::from_millis(1)).await.unwrap();
        assert_eq!(buys_a, buys_b, "buy count must be deterministic");
        assert_eq!(sells_a, sells_b, "sell count must be deterministic");
        assert_eq!(
            brain_signals_a, brain_signals_b,
            "brain non-Hold emission count must be deterministic"
        );
        assert!(buys_a >= 1 && sells_a >= 1);
        // With the configured sizing every brain signal passes the
        // gates and becomes an order. If the gates ever block one,
        // brain_signals > buys + sells and the assert below will fire.
        assert_eq!(brain_signals_a, buys_a + sells_a);
    }
}
