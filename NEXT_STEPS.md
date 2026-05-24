# Next Steps

This document complements the high-level README with a concrete, ordered
build plan. The order is chosen so each step produces something runnable
that validates the previous step, rather than accumulating mocked-out
skeletons that all have to work at once.

## 0. Pre-flight — what you have in this archive

```
rustrade/
├── Cargo.toml                                   (workspace, 5 crates configured)
├── README.md                                    (top-level design)
├── NEXT_STEPS.md                                (this file)
└── crates/
    ├── rustrade-core/                           ✅ complete + tested
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs
    │       ├── brain.rs                         Brain trait + Decision + SizeHint
    │       ├── bus.rs                           MarketDataBus + SignalBus
    │       ├── error.rs
    │       ├── exchange.rs                      ExchangeClient + source traits
    │       ├── market.rs                        Side, Symbol, Exchange, MarketDataEvent
    │       ├── signal.rs                        Signal + SignalType
    │       └── types.rs                         Price, Volume, Candle, Tick, Order, Fill, Position
    │
    ├── rustrade-supervisor/                     🟡 skeleton — needs port
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs
    │       ├── backoff.rs                       PLACEHOLDER — port from janus-core
    │       ├── lifecycle.rs                     PLACEHOLDER — port from janus-core
    │       ├── service.rs                       complete (TradingService trait)
    │       └── supervisor.rs                    compiles, but no restart logic yet
    │
    └── rustrade-risk/                           ✅ complete + tested (13 tests)
        ├── Cargo.toml
        └── src/
            ├── lib.rs
            ├── circuit_breaker.rs               sliding-window breaker
            ├── session_pnl.rs                   drawdown cap + daily rollover
            └── sizing.rs                        notional-based position sizer
```

Workspace status: `cargo check --workspace` passes clean. `cargo test
--workspace` reports 13 unit tests + 2 doc tests passing.

## 1. Port the supervisor (~1 day)

**Source:** `janus-main/lib/janus-core/src/supervisor/{backoff.rs, lifecycle.rs, mod.rs}`

**Target files in this crate:**
- `backoff.rs` — replace placeholder with janus-core source verbatim. No changes.
- `lifecycle.rs` — replace placeholder with janus-core source verbatim. No changes.
- `supervisor.rs` — replace with full janus-core `mod.rs` implementation, with these renames:
  - `JanusSupervisor` → `Supervisor`
  - `JanusService` → `TradingService` (already done in `service.rs`)

**Changes during port:**
1. Remove the dependency on `crate::metrics::metrics()`. Replace each call
   with a conditional block:
   ```rust
   #[cfg(feature = "prometheus")]
   {
       // existing prometheus .inc() calls
   }
   ```
   The atomic `SupervisorMetrics` struct stays as the authoritative
   in-memory state; Prometheus just mirrors it.
2. Add a new `prometheus` module inside this crate that owns a local
   `OnceCell<Registry>` when the feature is on. Don't use a global registry.

**Validation:** lift the three chaos tests from janus-core verbatim
(`test_chaos_backoff`, `test_chaos_circuit_breaker_trips`,
`test_chaos_mixed_fleet`). If they pass here, the port is done.

## 2. Write the `rustrade` facade crate (~2 days)

**New directory:** `crates/rustrade/`

**What goes in it:**

```rust
// crates/rustrade/src/lib.rs
pub use rustrade_core::*;
pub use rustrade_supervisor::*;
pub use rustrade_risk::*;

pub mod bot;       // Bot + BotConfig builder
pub mod execution; // The framework-side execution service (consumes Decisions)
pub mod logging;   // tracing subscriber setup helpers
```

**The `Bot` struct is what makes this a framework**, not a collection of crates.
Its job:

1. Accept `Arc<dyn ExchangeClient>` + `Vec<Arc<dyn Brain>>` + config
2. Build a `Supervisor`
3. Spawn, as supervised services:
   - A `CandlePollerService` per brain (consumes the poller trait from
     `exchange-apiws` or an equivalent)
   - A `PrivateFeedService` that routes fills and position changes into
     brain callbacks
   - A `PublicFeedService` that publishes to the `MarketDataBus`
   - An `ExecutionService` that subscribes to the `MarketDataBus`, invokes
     `brain.on_event()`, and translates `Decision`s into `ExchangeClient`
     calls — gated by `PositionSizer`, `CircuitBreaker`, and `SessionPnl`
4. Run `supervisor.run_until_shutdown()`
5. On shutdown: optionally close open positions, flush logs, call
   `brain.health()` for a final summary

This is the crate where the leverage / stop-orders / contract-values design
holes from `kucoin_v2/DESIGN_NOTES.md` have to be resolved. Don't try to
get them right upfront; pick the pragmatic answer (per-adapter leverage,
escape-hatch stop orders, `ExchangeClient::contract_value()` method),
build, and revisit after kucoin v2 is running.

**Minimum viable deliverable:** an example binary in `examples/noop-bot/`
that builds a `Bot` with a `NoopBrain` (always returns `Decision::hold()`)
and a mocked `ExchangeClient`, runs for 10 seconds, then shuts down cleanly.
If that runs, the framework works; everything after is strategy.

## 3. Port kucoin to the framework (~1 week)

You already have the sketch in `/home/claude/kucoin_v2/`. Key files
(preserved in this archive):

- `main.rs` (~120 lines) — shows the new entry point shape
- `brain.rs` (~280 lines) — the SAR strategy as an `impl Brain`
- `adapter.rs` (~120 lines) — bridges `exchange-apiws::KuCoinClient` to
  `rustrade_core::ExchangeClient`
- `DESIGN_NOTES.md` — rationale and known design holes

When doing the real port:

1. Start by making the existing kucoin bot depend on `rustrade-core` only
   (not yet the facade). Import the `Brain` trait; don't wire it up yet.
2. Move the SAR logic from `bot/strategy.rs` into `brain.rs`. The
   `SarBrain::on_event` method is the single biggest piece — it consumes
   what `plan_candle` used to do.
3. Delete `bot/execute.rs`, `bot/sizing.rs`, `bot/pnl.rs`,
   `bot/circuit_breaker.rs`, `bot/candle_poller.rs`. Their logic now lives
   in the framework.
4. Rewrite `main.rs` on top of `rustrade::Bot`.
5. Shadow-run the new binary alongside the old one in sim mode for 48
   hours. Compare PnL event-for-event.
6. Cut over once the numbers match.

## 4. Extract `rustrade-backtest` (~3 days, lower priority)

**Source:** `janus-main/crates/backtest/`

Largely a verbatim lift. The main work is decoupling it from
`janus-indicators`, `janus-strategies`, and `janus-models`, which all
become generic-parameters / trait-object inputs in the extracted crate.

The backtest engine consumes the same `Brain` trait as live trading — this
is the payoff of the separation. Any brain that runs live can run in a
backtest with zero code changes.

## 5. Extract `janus` to a private repo (~1 week)

This is the last step, not the first. The neuromorphic code, specific
strategies, and prop-firm compliance logic move to a private repo that
depends on `rustrade + exchange-apiws + indicators-ta`. The
`janus-main` monorepo can then be archived.

---

## Things to explicitly decide before shipping 0.1

These are design questions the current skeleton punts on. The right time
to answer them is after the kucoin port is running, not before.

### Leverage on orders
Current: hardcoded in `KucoinExchangeAdapter`. Pick one:
- (a) `leverage: Option<u32>` field on `Order`
- (b) Per-adapter via constructor (recommended for 0.1)
- (c) Per-order via an `ExchangeMetadata` extension trait

### Stop orders
Current: not in the `ExchangeClient` trait; kucoin adapter exposes escape-hatch access to the native client. Pick one:
- (a) Add `place_stop` / `cancel_all_stops` trait methods
- (b) `Order.stop: Option<StopAttachment>` — adapter decides how to honour
- (c) Leave out of the framework trait; service code calls native APIs (current)

### Contract multipliers
Current: not modelled. Pick one:
- (a) `ExchangeClient::contract_value(symbol) -> f64` trait method (recommended)
- (b) `PositionSizer` takes an explicit multiplier on every call (current)
- (c) Brain returns `SizeHint::Quantity` directly and skips the problem

### Optuna params overrides
Current: kucoin has a single `BotSettings` with 100 fields all in one JSON.
In the framework these split across strategy (brain), risk, session PnL,
and execution subsystems. Decision needed:
- (a) Each subsystem owns its own `*Overrides` struct; `BotConfig` dispatches
- (b) One global flattened `BotOverrides` with feature-gated fields per subsystem
- (c) Brain-specific strategy params stay opaque to the framework; only
  risk/exec params are framework-owned

---

## Why this ordering

The supervisor is first because **everything else needs it** — the facade
can't work without it, and there's no point writing more code against the
current unsupervised skeleton because that code will all have to be redone.

The facade is second because **it's where the rubber meets the road** on
the design holes. Nothing reveals bad trait design faster than trying to
wire two concrete implementations together through it.

The kucoin port is third because **it's the only way to know if any of
this actually works end-to-end**. Before that step the entire framework
is hypothesis; after it, you have one working service and a much clearer
picture of which bits of the design survived contact with reality.

Backtest and janus come later because they're high-value but not
blocking. Any time spent on them before the live path is solid is time
you might have to throw away.
