# TODO

Living checklist for `rustrade`. Pairs with [`NEXT_STEPS.md`](./NEXT_STEPS.md)
(porting plan) and [`README.md`](./README.md) (design overview). When in doubt,
that document explains *why*; this one tracks *what's left*.

Scope reminder (decided 2026-05):

- **Consumption model:** embedded library only. Downstream services depend on
  the `rustrade` crate and call `Bot::new(...).run().await`. No HTTP/gRPC
  control plane, no IPC, no message-bus integration in 0.1.
- **Exchanges:** rustrade ships zero exchange adapters. The framework only
  defines `ExchangeClient` / `MarketSource` traits; concrete adapters live in
  downstream crates (e.g. `exchange-apiws`).
- **Persistence:** in-memory only for 0.1. Session PnL, breaker state, etc.
  reset on restart. A `StateStore` trait is a 0.2 concern.

---

## Status snapshot

| Crate                  | State          | Tests           | Blockers                                      |
| ---------------------- | -------------- | --------------- | --------------------------------------------- |
| `rustrade-core`        | usable         | 0 unit, 0 doc   | trait surface not yet locked (see decisions)  |
| `rustrade-supervisor`  | skeleton       | 0               | restart logic, backoff, lifecycle are stubs   |
| `rustrade-risk`        | complete       | 13 unit + 2 doc | none                                          |
| `rustrade-backtest`    | not started    | -               | waits on facade                               |
| `rustrade` (facade)    | not started    | -               | waits on supervisor port                      |
| examples               | none           | -               | waits on facade                               |

---

## Definition of done for 0.1.0

The framework is shippable when **all** of these are true:

- [ ] `cargo test --workspace --all-features` passes on stable Rust.
- [ ] `cargo doc --workspace --no-deps` builds with zero warnings.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` is clean.
- [ ] `examples/noop-bot` runs a `Bot` with a `NoopBrain` + mock
      `ExchangeClient` for 10 s and shuts down cleanly on Ctrl-C.
- [ ] `examples/sma-cross-bot` runs the same way against a deterministic
      replay feed and produces a non-zero, reproducible PnL.
- [ ] All four open design decisions below are answered in code, not docs.
- [ ] CI green on Linux + macOS for stable + MSRV.
- [ ] `CHANGELOG.md` and per-crate `README.md` written.
- [ ] First-time-user tutorial ("a bot in 50 lines") exists.

---

## Phase 0 — Workspace hygiene (½ day)

Cheap items that make everything else easier. Do these first.

- [x] Add `.gitignore` covering `target/`, `.idea/`, `.vscode/`, `*.swp`,
      `.DS_Store`, `*.log`. `Cargo.lock` is committed (workspace has
      planned binaries).
- [x] Pin `rust-toolchain.toml` to `1.94.1` (matches workspace MSRV).
- [x] Fix duplicated `# rustrade` heading at bottom of `README.md`
      (line 184 was a leftover).
- [x] Add `CHANGELOG.md` (Keep-a-Changelog format, `Unreleased` section).
- [x] Add `CONTRIBUTING.md` with: build/test commands, branch naming,
      commit-message convention, "no merge commits inside feature
      branches" rule.
- [x] Add per-crate `README.md` stubs and wire `readme = "README.md"`
      into each `Cargo.toml`.
- [x] Add `.editorconfig` (4-space indent, LF, final newline).
- [x] Add `rustfmt.toml` (edition 2024, max_width 100; nightly-only
      `group_imports`/`imports_granularity` documented as comments).
- [x] Add `clippy.toml` with `msrv = "1.94.1"`.

## Phase 1 — Finish the framework (~1 week)

Blocker for everything else. The facade can't be built on a stub supervisor.

### Supervisor port — see [`NEXT_STEPS.md §1`](./NEXT_STEPS.md)

- [x] Lift `janus-core/supervisor/backoff.rs` verbatim into
      `crates/rustrade-supervisor/src/backoff.rs`. Replace placeholder.
- [x] Lift `janus-core/supervisor/lifecycle.rs` verbatim into
      `crates/rustrade-supervisor/src/lifecycle.rs`. Replace placeholder.
- [x] Lift `JanusSupervisor` from `janus-core/supervisor/mod.rs` into
      `supervisor.rs`. Renamed to `Supervisor` / `TradingService`.
- [x] Gate every prometheus call behind `#[cfg(feature = "prometheus")]`.
- [x] Add a local `prometheus::Registry` in `OnceLock` — new
      `crates/rustrade-supervisor/src/prometheus.rs`; host services
      `gather()` from `prometheus::registry()` instead of a global registry.
- [x] Wire `TradingService::restart_policy()` into the supervisor's
      restart decision (`Always`, `OnFailure`, `Never` all honoured).
- [x] Fix the lifecycle insertion race — the initial `ServiceLifecycle`
      is now inserted synchronously inside `service_loop` before any work
      starts.
- [x] Port the three chaos tests verbatim: `test_chaos_exponential_backoff`,
      `test_chaos_circuit_breaker_trips`, `test_chaos_mixed_fleet`.

### Core trait surface lockdown

The traits in `rustrade-core` are the framework's public ABI for downstream
crates. Breaking them post-0.1 hurts every dependent. Audit and lock now.

- [x] **Unit tests for `rustrade-core`.** Added 27 unit tests covering
      `Decision` builders, `Position::close_side`, `Side::opposite`,
      `MarketDataEvent::symbol/exchange`, `Tick::mid_price/spread`,
      `Symbol` ergonomics, `StopAttachment`/`StopKind` constructors,
      and serde roundtrips for `Order`, `Decision`, `Signal`, `Symbol`.
- [x] Replace `String` symbol fields with `Symbol` newtype for
      consistency: `Tick.symbol`, `Order.symbol`, `Fill.symbol`, all
      relevant `ExchangeClient` and `Brain` parameters.
- [x] **Leverage:** per-adapter via constructor (decision (b) from below).
      No change to `Order` — adapters configure leverage at construction.
- [x] **Stop orders:** added `Order.stop: Option<StopAttachment>` carrying
      a `StopKind` enum. Opaque to the framework; adapters interpret.
- [x] **Contract multipliers:** added
      `ExchangeClient::contract_value(&Symbol) -> f64` with a `1.0`
      default. Spot adapters need not override.
- [x] Add `ExchangeClient::supports(capability: Capability) -> bool`
      introspection. Default returns `false` for every variant —
      pessimistic, so a new adapter doesn't quietly accept orders it
      can't execute.
- [x] Document the cancellation contract on `MarketSource::run` — the
      future is dropped by the wrapping `TradingService`; implementors
      must be drop-safe (see updated trait docstring).

### Risk crate polish

- [x] Added 7 `proptest`-based property tests for `PositionSizer`: cap
      respected, monotone in margin, monotone in leverage, zero on every
      degenerate input flavour, and matches `floor(margin·leverage /
      (price·cv))` against a reference computation.
- [x] Added `Clock` trait + `SystemClock` (default) + `ManualClock`
      (tests) in the new `clock` module. `CircuitBreaker::with_clock`
      and `SessionPnl::with_clock` constructors accept any `Arc<dyn
      Clock>`; existing `::new` constructors keep the default
      `SystemClock` so production code doesn't move.
- [x] UTC rollover verified end-to-end via `ManualClock`-driven
      `SessionPnl::tick()` test. Sliding-window eviction and cooldown
      auto-reset on `CircuitBreaker::tick()` also covered.

## Phase 2 — Build the `rustrade` facade (~1–2 weeks)

The crate downstream services actually depend on. Lives in
`crates/rustrade/`. Split into three reviewable tracks like Phase 1.

### Phase 2a — minimum viable facade

- [x] Create `crates/rustrade/Cargo.toml` and add to workspace members.
- [x] `lib.rs` re-exports `core`, `risk`, and selected `supervisor`
      items. Downstream `use rustrade::*` covers the public surface.
- [x] `bot::BotConfig` + `BotConfigBuilder`. Phase 2a fields: name,
      symbols, shutdown_timeout, install_signal_handler,
      market_bus_capacity, close_positions_on_shutdown (reserved; not
      yet honoured). Poll cadence / sim mode wait for 2b.
- [x] `bot::Bot::new(config, exchange, brains) -> Result<Self>`.
- [x] `Bot::run_until_shutdown(self) -> anyhow::Result<()>` — spawns
      services, drives the supervisor, drains on exit.
- [x] `Bot::handle() -> BotHandle` — cheap cloneable handle with
      `shutdown()`, `await_shutdown()`, `is_shutting_down()`, `health()`.
- [x] `BotHealth` aggregate combining per-service `ServiceLifecycleSnapshot`s
      and per-`Brain` health into one snapshot.
- [x] `ExecutionService` (Phase 2a scope only): subscribes to
      `MarketDataBus`, calls `brain.on_event` for each event. Records
      `events_processed` / `events_dropped` via atomics. **Risk gating
      and order placement land in Phase 2b.**
- [x] `logging::init_tracing()` opinionated default subscriber.
- [x] Integration tests in `crates/rustrade/tests/bot_lifecycle.rs`
      against the public API only.

### Phase 2b — risk-gated execution (this batch)

- [x] Expand `ExecutionService`: wire `Decision` through the risk
      gates in this order — `SessionPnl::is_session_halted` →
      `CircuitBreaker::is_tripped` → `PositionSizer::contracts` →
      `ExchangeClient::place_order`. Each gate emits a structured
      `tracing` event on block.
- [x] Position-cache (`PositionCache`) prefetched on `Bot::run_until_shutdown`
      startup via `exchange.get_position(symbol)`. `ExecutionService`
      reads it before each `brain.on_event` call.
- [x] `RiskConfig` (session-PnL + circuit-breaker + sizing) in
      `BotConfig` with builder methods. Per-symbol overrides deferred
      to a future phase.
- [x] Wire `Bot::config().close_positions_on_shutdown` to a real
      close-on-stop hook via `ExchangeClient::close_position`.
- [x] `BotHandle::record_trade_outcome(symbol, gross, fee)` so brains
      / host code can feed PnL into the gates while the automated
      fill routing waits for Phase 2c.

### Phase 2c — optional services + observability (this batch)

- [x] `MarketFeedService` — drives a `MarketSource` under supervisor
      control. Wired via `Bot::with_market_source(...)`. Source
      implementors publish to the bus they were constructed with.
- [x] `FillRoutingService` — polls a `FillSource`, calls
      `brain.on_fill` on every brain, and refreshes the per-symbol
      position cache from `ExchangeClient::get_position`. Wired via
      `Bot::with_fill_source(...)`. Auto-feed of realised PnL into the
      risk state is deferred until entry-price-aware PnL accounting
      lands; for now hosts continue to call `record_trade_outcome`.
- [x] `BotHandle::subscribe_signals() -> broadcast::Receiver<Signal>`.
      `ExecutionService` publishes on every non-`Hold` decision before
      gates run.
- [x] Externally-owned `CancellationToken` support via
      `Bot::with_external_cancel(token)` — internal linker task,
      no host-side glue required.

### Phase 2d

- [x] `MetricsSink` trait + `NoopSink` default in `rustrade-core`;
      `Bot::with_metrics(Arc<dyn MetricsSink>)` plugs in a host-owned
      backend. Framework services emit
      `rustrade_fills_routed_total`, `rustrade_candles_published_total`,
      `rustrade_realised_pnl_quote`, etc.
- [x] `CandleSource` trait in `rustrade-core` — separate from
      `MarketSource` because polling has a different shape than
      streaming.
- [x] `CandlePollerService` wired via `Bot::with_candle_poller(source,
      symbol, interval, poll_cadence, limit)`. Per-symbol cadences via
      repeated calls. Deduplicates by `Candle::time`.
- [x] Auto-feed realised PnL into the risk state from
      `FillRoutingService` using weighted-average entry accounting
      (same model the backtest engine uses). Reducing fills emit
      `record_close` + win/loss on the breaker; flip fills emit PnL
      for the closed portion only.

## Phase 3 — Examples & end-to-end validation

Examples are the framework's UX. They double as integration tests.

- [x] `examples/noop-bot/` — `NoopBrain` (always `Decision::hold`), mock
      `ExchangeClient`. Runs for N seconds (default 10), shuts down via
      `BotHandle::shutdown`, asserts no orders placed.
- [x] `examples/sma-cross-bot/` — fast(5)/slow(20) SMA-crossover brain
      against a deterministic sinusoidal candle replay driven by a
      `MarketSource`. Ships a `#[tokio::test]` that pins down the order
      count for regression testing.
- [x] `examples/multi-brain-bot/` — two brains, each filtering events
      to its own symbol. Asserts per-brain event counts.
- [x] `examples/embed-in-service/` — host service with its own tokio
      runtime and `CancellationToken` that drives the bot via
      `Bot::with_external_cancel` + `bot.market_data_bus()` +
      `BotHandle::subscribe_signals`. Reference for downstream consumers.
- [x] Integration test harness already in place from Phase 2:
      `bot_lifecycle.rs`, `risk_gates.rs`, `phase_2c.rs` boot a bot with
      a scripted mock exchange and assert on the sequence of orders /
      signals / lifecycle events.

## Phase 4 — Backtest engine

### Phase 4a — minimum viable engine (this batch)

- [x] Create `crates/rustrade-backtest/Cargo.toml`, add to workspace.
- [x] Single-threaded synchronous replay engine: feeds candles to a
      `Brain` in order, applies slippage + fees, tracks position +
      realised PnL, emits `TradeOutcome`s for every reducing fill.
- [x] Pluggable slippage models: `Zero`, `FixedBps`.
- [x] Pluggable fee schedules: `Zero`, `Flat`, `MakerTaker`.
- [x] Performance metrics in `BacktestResult`: total return, win rate,
      profit factor, max drawdown, per-trade ledger.
- [x] Brain-identical guarantee: `tests/sma_replay.rs` runs the same
      `impl Brain` shape as `examples/sma-cross-bot/` through the
      engine. The trait is the contract; no engine-specific brain code.
- [x] Determinism: pinned down by two-run regression test
      `deterministic_replay_same_brain_same_series`.

### Phase 4b — deferred

- [ ] **Zero-lookahead invariant** baked into the bus / event layer
      rather than enforced by convention. Today the engine guarantees
      it by construction (candles fed in order; brain has no random
      access), but a stricter `BacktestBus` type would prevent future
      regressions.
- [ ] CSV / Parquet candle loaders. Today candles are passed as a
      `Vec<Candle>` — fine for tests, awkward for real research.
- [ ] Sharpe / Sortino / expectancy / avg win-loss metrics. Need a
      stable risk-free rate input and a deterministic equity sampling
      cadence.
- [ ] Book-walk slippage (needs an order-book replay, not just candles).
- [ ] Multi-symbol backtest. Today the engine is single-symbol; brain
      filtering on symbol is exercised in `examples/multi-brain-bot/`
      but not yet in the backtest.

## Phase 5 — Service-integration ergonomics

- [x] `BotHandle` API surface — landed across Phase 2a–2c:
  - [x] `health() -> BotHealth`
  - [x] `shutdown()` — fire-and-forget cancellation trigger
  - [x] `await_shutdown()` — resolves when shutdown is triggered (note:
        not when fully drained — host awaits the bot's `JoinHandle` for
        that)
  - [x] `subscribe_signals() -> broadcast::Receiver<Signal>`
  - [x] `record_trade_outcome` to feed risk gates from host fill flow
- [x] Externally-owned `CancellationToken` via
      `Bot::with_external_cancel(token)` — internal linker, no host
      glue required.
- [x] Tokio runtime contract documented in `lib.rs` and
      `Bot::run_until_shutdown` docs.
- [x] Tightened `BotConfig` validation: empty symbol list, zero
      shutdown timeout, NaN loss limit, non-finite margin, zero
      market/signal bus capacities — all return `Error::Config`. The
      framework never panics on bad config.
- [x] Channel capacities configurable: `market_bus_capacity` (default
      1024) and `signal_bus_capacity` (default 256). Drop-oldest
      semantics documented on both buses and on `subscribe_signals`.
- [x] Resource SLAs documented: memory per active symbol, channel
      buffer sizes, expected shutdown time, restart-after-crash latency
      bounds.

## Phase 6 — Documentation & release

### Phase 6a — docs + version policy (this batch)

- [x] `#![warn(missing_docs)]` on every public crate. Every public item
      carries at least a one-line rustdoc. `cargo doc` is clean across
      `--workspace --no-deps --all-features`.
- [x] Top-level `docs/quickstart.md` — "Your first rustrade bot in 50
      lines". Walks through `Brain`, `ExchangeClient`, and `Bot`
      end-to-end, matching `examples/noop-bot/` line-for-line.
- [x] Workspace-locked `0.1.x` version policy documented in
      `CONTRIBUTING.md`, plus the planned publish ordering.

### Phase 6b — extended tutorials + CI (this batch)

- [x] Additional tutorials in `docs/`:
  - [x] `writing-a-brain.md` — `Brain` trait, state, position
        handling, the canonical `Mutex<State>` pattern, a worked
        SMA crossover.
  - [x] `writing-an-exchange-adapter.md` — `ExchangeClient`,
        `MarketSource`, `FillSource`, `Capability` introspection,
        `contract_value`, the cancellation contract.
  - [x] `embedding.md` — `BotHandle`, external cancellation, signal
        subscription, runtime + resource expectations.
  - [x] `backtesting.md` — brain-identical guarantee, position state
        machine, determinism, intentional non-features.
- [x] `.github/workflows/ci.yml`: `fmt`, `clippy` (with and without
      features), `test` matrix (Ubuntu + macOS), `doc` with
      `-D warnings`, `cargo-deny`.
- [x] `.github/dependabot.yml`: weekly Cargo + GitHub Actions
      updates, grouped by `tokio*` / `tracing*`.
- [x] `deny.toml`: licence allow-list, advisory blocking,
      registry/git source pinning.

### Phase 6c — deferred

- [ ] `# Example` rustdoc block on every trait and major struct.
      (Mechanical — lower priority than narrative tutorials.)
- [ ] `cargo publish` driver + `cargo-semver-checks` in CI. Wait
      until the first crates.io release to design the workflow against
      a real target.
- [ ] Consider migrating to mdbook if the docs grow past ~6 files.
- [ ] cargo-audit weekly scheduled job (subsumed by `cargo-deny`'s
      advisory check for now; revisit if we need a separate report
      pipeline).
- [ ] Coverage with `cargo-llvm-cov` surfaced in PR comments.

## Cross-cutting

- [ ] **CI** (`.github/workflows/ci.yml`):
  - [ ] fmt: `cargo fmt --check`
  - [ ] clippy: `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  - [ ] test: `cargo test --workspace --all-features`
  - [ ] doc: `cargo doc --workspace --no-deps`
  - [ ] MSRV: same matrix on `1.94.1`
  - [ ] Matrix: ubuntu-latest, macos-latest (skip windows until someone needs it)
- [ ] `cargo-audit` weekly scheduled job.
- [ ] `cargo-deny` for licence + duplicate-dep policy.
- [ ] Coverage with `cargo-llvm-cov`; surface in PR comments.
- [ ] Dependabot for Cargo + GitHub Actions.

---

## Open design decisions

Lifted from [`NEXT_STEPS.md §"Things to explicitly decide"`](./NEXT_STEPS.md).

- [x] **Leverage on orders.** Resolved: per-adapter via constructor. No
      change to `Order`.
- [x] **Stop orders.** Resolved: `Order.stop: Option<StopAttachment>` —
      adapters interpret, framework treats as opaque. Gated by
      `Capability::StopOrders`.
- [x] **Contract multipliers.** Resolved: `ExchangeClient::contract_value(&Symbol)
      -> f64` with a `1.0` default. The sizer continues to take an explicit
      `contract_value` argument for now — wiring the adapter through is
      the facade's job (Phase 2).
- [ ] **Parameter overrides.** Deferred to Phase 2 (facade). The shape
      lands when the `Bot`/`BotConfig` types do; risk and brain configs
      already own their own structs.

---

## Explicitly out of scope for 0.1

Listed so contributors don't accidentally build them. Revisit for 0.2+:

- Persistence (`StateStore` trait, sqlite/postgres impls)
- HTTP/gRPC/IPC control plane
- Built-in exchange adapters
- Built-in indicator library
- Strategy ensemble / brain composer
- Order-book reconstruction
- Live + replay hybrid (warm-start brains from history)
- Multi-account / sub-account routing
- Web dashboard

---

## How to use this file

- Tick boxes as work lands. Reference the PR that closed each item in
  `CHANGELOG.md`, not here.
- When adding a new item: pick the right phase, keep it actionable
  ("add `Symbol` newtype" not "improve types"), and link the relevant
  source file with `file:line` when the item is a concrete code change.
- Phase order is a dependency order, not a calendar. Phase 4 can start
  early in parallel if someone wants to — but Phase 2 cannot start before
  Phase 1's supervisor port.
