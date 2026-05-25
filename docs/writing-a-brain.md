# Writing a Brain

A `Brain` is the strategic layer of a rustrade bot. The framework
handles every plumbing concern — service supervision, market-data
delivery, risk gates, order placement, shutdown drain. Your `Brain`
answers one question: *given this market event and my current position,
what do I want to do?*

This guide walks through writing a `Brain` from scratch.

## 1. The trait

```rust
use async_trait::async_trait;
use rustrade::{Brain, Decision, MarketDataEvent, Position, Result};

#[async_trait]
pub trait Brain: Send + Sync + 'static {
    fn name(&self) -> &str;

    async fn on_event(&self, event: &MarketDataEvent, position: &Position)
        -> Result<Decision>;

    async fn on_fill(&self, _fill: &rustrade::Fill) -> Result<()> {
        Ok(())  // default: ignore fills
    }

    async fn on_position_change(&self, _symbol: &rustrade::Symbol, _position: &Position)
        -> Result<()> {
        Ok(())  // default: ignore position changes
    }

    async fn health(&self) -> rustrade::BrainHealth {
        rustrade::BrainHealth::ok()
    }
}
```

Notice the `&self` receivers. This lets the framework hold every brain
behind an `Arc<dyn Brain>` and share it across services without
exclusive ownership. State that needs to mutate lives behind interior
mutability.

## 2. Picking a state model

Three common patterns, in order of preference:

| Pattern                     | When                                         |
| --------------------------- | -------------------------------------------- |
| `Mutex<State>`              | Default. Cheap, ergonomic, and brains are called serially per event so contention is minimal. |
| `AtomicU64` / `AtomicBool`  | When the brain only needs counter-like state (event count, flags). Lock-free. |
| `tokio::sync::RwLock<State>` | When the brain exposes async accessors to host code AND those accessors might run concurrently with `on_event`. Rarely needed. |

For most strategies, a `std::sync::Mutex` is the right tool.

## 3. A worked example: SMA crossover

```rust
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rustrade::{Brain, Decision, MarketDataEvent, Position, Result};

const FAST: usize = 5;
const SLOW: usize = 20;

pub struct SmaCrossBrain {
    state: Mutex<SmaState>,
}

#[derive(Default)]
struct SmaState {
    closes: Vec<f64>,
    /// Tracks whether the fast SMA was above the slow SMA on the
    /// previous decided bar — `None` until both windows are warm.
    last_fast_above: Option<bool>,
}

impl SmaCrossBrain {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(SmaState::default()),
        })
    }
}

fn sma(window: &[f64], n: usize) -> Option<f64> {
    if window.len() < n {
        return None;
    }
    Some(window[window.len() - n..].iter().sum::<f64>() / n as f64)
}

#[async_trait]
impl Brain for SmaCrossBrain {
    fn name(&self) -> &str { "sma-cross" }

    async fn on_event(
        &self,
        event: &MarketDataEvent,
        _position: &Position,
    ) -> Result<Decision> {
        let close = match event {
            MarketDataEvent::Candle { candle, .. } => candle.close,
            _ => return Ok(Decision::hold()),
        };

        let mut st = self.state.lock().unwrap();
        st.closes.push(close);
        // Bound the buffer; we never look further back than SLOW.
        if st.closes.len() > SLOW * 2 {
            let drop = st.closes.len() - SLOW * 2;
            st.closes.drain(0..drop);
        }

        let (Some(fast), Some(slow)) = (sma(&st.closes, FAST), sma(&st.closes, SLOW))
        else {
            return Ok(Decision::hold());
        };
        let fast_above = fast > slow;

        let decision = match st.last_fast_above {
            Some(prev) if prev != fast_above => {
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
```

Three things to internalise from this example:

1. **Filter the event variant.** The framework delivers every
   `MarketDataEvent` — candles, ticks, trades — to every brain. Match
   on the variants you care about and fall through to `Decision::hold`.
2. **Bound your state.** Rolling-window indicators only need the last
   `SLOW` bars; trim the buffer to prevent unbounded growth in long
   runs.
3. **`Decision::hold` is always safe.** When in doubt, return it.

## 4. What the framework does next

When your brain returns a non-`Hold` decision, the framework's
[`ExecutionService`](https://docs.rs/rustrade) runs it through three
risk gates before any order reaches the exchange:

1. `SessionPnl::is_session_halted(symbol)` — daily drawdown cap
2. `CircuitBreaker::is_tripped(symbol)` — rolling-window loss breaker
3. `PositionSizer::contracts(price, contract_value)` — `0` blocks

Each blocked decision is logged with the gate that fired. The brain
does **not** need to handle this — emit the decision and the framework
takes care of the rest.

## 5. Using `Decision`'s builder

```rust
Decision::buy(0.85)
    .with_stop(Price(95.0))
    .with_take_profit(Price(110.0))
    .with_size_hint(SizeHint::MarginFraction(0.25))
    .with_metadata(serde_json::json!({
        "reason": "fast crossed above slow",
        "fast_sma": 100.5,
        "slow_sma": 99.8,
    }))
```

- **`size_hint`** is *advisory* — the risk layer can scale down or
  ignore it. `SizeHint::Default` (the default) lets the framework's
  `PositionSizer` make the call.
- **`stop_price` / `take_profit_price`** are advisory too. The
  framework doesn't currently attach them to orders automatically; the
  brain can use them in its own tracking, or a future phase may wire
  them through.
- **`metadata`** is opaque to the framework but persists into the
  emitted `Signal`, which host services can subscribe to. Use it for
  post-hoc analysis.

## 6. Reporting health

```rust
async fn health(&self) -> BrainHealth {
    let st = self.state.lock().unwrap();
    BrainHealth {
        healthy: st.closes.len() >= SLOW,  // warm yet?
        events_processed: st.events as u64,
        non_hold_decisions: st.signals as u64,
        details: serde_json::json!({
            "buffer_len": st.closes.len(),
            "warm": st.last_fast_above.is_some(),
        }),
    }
}
```

`BotHandle::health()` aggregates per-brain `BrainHealth` into the
`BotHealth` snapshot host services see. Use this to expose
warm-up state, indicator staleness, model drift, etc.

## 7. Testing

A brain is just an `impl Brain` — synthesise events and assert. The
framework is not involved in the test:

```rust
#[tokio::test]
async fn sma_brain_holds_until_both_windows_warm() {
    let brain = SmaCrossBrain::new();
    for i in 0..(SLOW - 1) {
        let ev = candle_event("BTCUSDT", 100.0 + i as f64);
        let d = brain.on_event(&ev, &Position::FLAT).await.unwrap();
        assert!(matches!(d.signal, SignalType::Hold));
    }
}
```

For an end-to-end deterministic test that runs the brain through the
full backtest engine, see
[`crates/rustrade-backtest/tests/sma_replay.rs`](../crates/rustrade-backtest/tests/sma_replay.rs).

## Next steps

- See [`examples/sma-cross-bot/`](../examples/sma-cross-bot) for the
  same brain wired into a live `Bot`.
- The same `Brain` impl runs unchanged in
  [`rustrade-backtest`](../crates/rustrade-backtest) — write once,
  validate offline before deploying live.
- For multi-symbol filtering, see
  [`examples/multi-brain-bot/`](../examples/multi-brain-bot).
