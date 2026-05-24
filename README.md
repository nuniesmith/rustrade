# rustrade

Open-source trading bot framework in Rust. Provides the scaffolding —
service lifecycle, supervision, risk primitives, buses, traits — that every
trading bot rewrites from scratch. Plug in your own exchange adapter,
indicator stack, and strategy (`Brain`) and you get a production-ready bot.

> **Status: 0.1.0 skeleton.** The `rustrade-core` and `rustrade-risk` crates
> are complete and tested. `rustrade-supervisor` compiles but needs its full
> supervisor logic ported from `janus-core/supervisor/`. `rustrade-backtest`
> and the top-level `rustrade` facade crate are not yet populated.

---

## Design in one diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         YOUR SERVICE                            │
│  (kucoin-bot, binance-bot, janus-bin, …)                        │
│                                                                 │
│  fn main() {                                                    │
│    let exchange = Arc::new(KucoinExchangeAdapter::new(..));     │
│    let brain    = Arc::new(MySarBrain::new(..));                │
│    Bot::new(config, exchange, vec![brain])                      │
│       .run_until_shutdown().await                               │
│  }                                                              │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                       rustrade (facade)                         │
│    Bot builder, logging setup, ergonomic re-exports             │
└──────┬──────────────┬──────────────┬──────────────┬─────────────┘
       │              │              │              │
┌──────▼──────┐ ┌─────▼──────┐ ┌─────▼──────┐ ┌─────▼──────┐
│   -core     │ │ -supervisor│ │   -risk    │ │  -backtest │
│             │ │            │ │            │ │            │
│ Types,      │ │ Service    │ │ Position   │ │ Temporal   │
│ Brain,      │ │ lifecycle, │ │ sizer,     │ │ fortress   │
│ Buses,      │ │ backoff,   │ │ breaker,   │ │ replay     │
│ Traits      │ │ circuit    │ │ session    │ │ engine     │
│             │ │ breaker,   │ │ PnL        │ │            │
│             │ │ prometheus │ │            │ │            │
└─────────────┘ └────────────┘ └────────────┘ └────────────┘
       ▲                                              ▲
       │                                              │
       │  (your brain consumes these external crates) │
       │                                              │
┌──────┴──────┐   ┌──────────────┐   ┌────────────────┘
│  exchange-  │   │ indicators-  │   │
│  apiws      │   │ ta           │   │
│ (published) │   │ (published)  │   │
└─────────────┘   └──────────────┘   │
                                     │
                  ┌──────────────────┘
                  │
            ┌─────▼──────┐
            │   janus    │   (private — your brain IP)
            │ neuromorph │
            │ strategies │
            └────────────┘
```

---

## Crates

### `rustrade-core` ✅ complete

Zero-runtime type layer. Defines:

- **Domain types** — `Price`, `Volume`, `Candle`, `Tick`, `Order`, `Fill`, `Position`
- **Market primitives** — `Side`, `Symbol`, `Exchange`, `MarketDataEvent`
- **The `Brain` trait** — the single abstraction every strategy implements
- **`Decision` + `SizeHint`** — intent-vs-execution separation
- **Trait contracts** — `ExchangeClient`, `MarketSource`, `FillSource`, `EventSource`
- **Broadcast buses** — `MarketDataBus`, `SignalBus`

No I/O. No tokio runtime state. No optional features. Every other rustrade
crate depends on this; this one depends on nothing internal.

### `rustrade-supervisor` 🟡 skeleton

Structured service lifecycle. Every long-running task in your bot (WS feed,
candle poller, heartbeat, brain) implements `TradingService` and is spawned
through a `Supervisor` that handles:

- Graceful shutdown via `CancellationToken` propagation
- Exponential-backoff restart with per-service circuit breakers
- Service lifecycle state machine (Starting → Running → Restarting → Terminated)
- Optional Prometheus metrics (feature-gated)

The skeleton compiles and responds to Ctrl-C/SIGTERM. The full backoff +
lifecycle + chaos-test suite needs to be lifted from `janus-core/supervisor/`
(see the porting checklist at the top of `supervisor.rs`).

### `rustrade-risk` ✅ complete, 13 passing tests

Generic trading risk primitives. Nothing strategy- or exchange-specific.

- **`CircuitBreaker`** — sliding-window loss breaker. Trips when N losses
  occur within a rolling window; stays open for the configured cooldown
  regardless of intervening wins. Ported from the kucoin Apr 2026 patch.
- **`SessionPnl`** — realised PnL tracker with drawdown cap and automatic
  00:00 UTC rollover. Classifies trades as W/L/B on **net** (after fees),
  so fee-flipped trades count correctly.
- **`PositionSizer`** — notional-based sizing from margin × leverage ÷
  (price × contract_value). Includes `max_contracts` cap and bailout-on-zero
  guard for all degenerate inputs.

### `rustrade-backtest` ⬜ not yet populated

Planned: replay engine with zero-lookahead guarantees, slippage models,
fee simulation, and the performance metrics suite (Sharpe, Sortino,
drawdown, profit factor). Lift target: `janus-main/crates/backtest/`.

### `rustrade` ⬜ not yet populated

Top-level facade crate. Planned API:

```rust
use rustrade::{Bot, BotConfig};

let config = BotConfig::builder()
    .name("my-bot")
    .symbols(["BTCUSDT", "ETHUSDT"])
    .poll_secs(20)
    .sim_mode(false)
    .close_positions_on_shutdown(true)
    .build()?;

Bot::new(config, exchange, brains)
    .run_until_shutdown()
    .await
```

This is the crate most users will depend on directly; it pulls in and
re-exports the others.

---

## Testing the skeleton

The `rustrade-core` and `rustrade-risk` crates are fully tested. On rustc
1.94+ with edition 2024:

```
cargo test --workspace
```

Expected: 13 passing unit tests + 2 passing doc tests.

During development this skeleton was also verified to compile on rustc 1.75
after trivial edition-2021 adjustments (let-chain → nested if in one place);
the committed source uses the edition-2024 idiom.

---

## What's next

See `NEXT_STEPS.md` for the proposed build order. The short version:

1. **Port the supervisor.** Lift `backoff.rs`, `lifecycle.rs`, and the main
   `JanusSupervisor` impl from `janus-core` verbatim; rename to
   `Supervisor`/`TradingService`; gate Prometheus behind the feature flag.
2. **Populate `rustrade` (facade).** Write the `Bot` builder that wires a
   `Vec<Arc<dyn Brain>>` + `Arc<dyn ExchangeClient>` into a running
   supervised system. Biggest piece: the candle-poll + private-WS tasks
   that the kucoin v1 main.rs currently hand-spawns.
3. **Port kucoin to the framework.** See `/home/claude/kucoin_v2/` for a
   code-shaped sketch of what this looks like. The `SarBrain` is written
   in full; the main.rs drops from 1239 lines to ~120.
4. **Extract `rustrade-backtest`.** Lower priority — wait until the live
   path is stable.
5. **Extract `janus` to its own repo.** After rustrade is battle-tested,
   the neuromorphic code (brain regions, LTN, cortex, …) moves to a
   private repo that depends on rustrade and implements the `Brain` trait.

---

## License

MIT.
# rustrade
