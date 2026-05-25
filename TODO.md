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

- [ ] Add `proptest`-based property tests for `PositionSizer` (sizing must
      be monotone in margin/leverage, capped by `max_contracts`, zero on
      degenerate inputs).
- [ ] Inject a clock into `CircuitBreaker` and `SessionPnl` so tests can
      simulate the rolling window without `tokio::time::sleep`. Trait or
      `Fn() -> u64` — small change, big test ergonomics win.
- [ ] Verify `SessionPnl` UTC rollover with a clock injection test (the
      current `last_reset_day` logic is plausible but untested).

## Phase 2 — Build the `rustrade` facade (~1–2 weeks)

The crate downstream services actually depend on. Lives in
`crates/rustrade/`. Create the Cargo.toml and uncomment in the workspace
manifest as the first step.

- [ ] Create `crates/rustrade/Cargo.toml` and add to workspace members.
- [ ] `lib.rs` re-exports `core`, `risk`, and selected `supervisor` items.
      Downstream should `use rustrade::*` and never need to depend on the
      sub-crates directly.
- [ ] `bot::BotConfig` builder: name, symbols, poll cadence, sim/live mode,
      shutdown timeout, close-positions-on-shutdown flag.
- [ ] `bot::Bot::new(config, exchange, brains) -> Self`.
- [ ] `Bot::run_until_shutdown(self) -> anyhow::Result<()>`.
- [ ] `Bot::handle() -> BotHandle` returning a cheap, cloneable handle a
      downstream service can hold to query health / trigger shutdown
      without retaining the bot itself. This is the **key embedding API**.
- [ ] Framework-side services (each `impl TradingService`):
  - [ ] `MarketFeedService` — drives a `MarketSource`, publishes to bus.
  - [ ] `CandlePollerService` — periodic poll of an adapter, publishes.
  - [ ] `FillRoutingService` — consumes a `FillSource`, calls `brain.on_fill`,
        updates `SessionPnl`.
  - [ ] `ExecutionService` — subscribes to `MarketDataBus`, calls
        `brain.on_event`, runs `Decision` through risk gates, places orders.
  - [ ] `HealthService` — aggregates per-service phases and `brain.health()`
        into a `BotHealth` struct, exposed via `BotHandle`.
- [ ] Risk gating order in `ExecutionService`: `SessionPnl::is_session_halted`
      → `CircuitBreaker::is_tripped` → `PositionSizer::contracts` → place.
      Each gate emits a structured `tracing` event on block.
- [ ] `Bot::on_shutdown` hook: optional close-positions-on-stop, final
      `brain.health()` snapshot to logs.
- [ ] `logging::init_tracing()` helper — opinionated default subscriber for
      downstream services that don't already have one. Skippable.
- [ ] `metrics::MetricsSink` trait so downstream services can plug in their
      own metrics backend instead of being forced onto prometheus. The
      `prometheus` feature provides a built-in impl.

## Phase 3 — Examples & end-to-end validation (~1 week)

Examples are the framework's UX. They double as integration tests.

- [ ] `examples/noop-bot/` — `NoopBrain` (always `Decision::hold`), mock
      `ExchangeClient`. Runs 10 s, shuts down on Ctrl-C, asserts no orders
      placed. This is the smallest possible "framework works" demo.
- [ ] `examples/sma-cross-bot/` — toy SMA crossover brain against a
      deterministic candle replay. Validates the live execution path and
      produces reproducible PnL.
- [ ] `examples/multi-brain-bot/` — two brains, one symbol each, same
      `Bot`. Validates the multi-`Arc<dyn Brain>` plumbing.
- [ ] `examples/embed-in-service/` — host service with its own tokio
      runtime and `CancellationToken` that drives the bot via `BotHandle`.
      This is the reference for downstream consumers.
- [ ] Integration test harness in `crates/rustrade/tests/` that boots a
      bot with a scripted mock exchange and asserts on the sequence of
      orders placed.

## Phase 4 — Backtest engine (~1–2 weeks)

Lower priority than 1–3 — defer until the live path is stable, since the
backtest engine consumes the same `Brain` trait and benefits from a
locked-down core.

- [ ] Create `crates/rustrade-backtest/Cargo.toml`, add to workspace.
- [ ] **Zero-lookahead invariant.** Engine must guarantee that a `Brain`
      cannot observe data with a timestamp `>` the current event's. Bake
      this in at the bus layer, not just by convention.
- [ ] Replay engine: CSV / Parquet candle source, deterministic order
      ordering, monotonic clock.
- [ ] Pluggable slippage models: zero, fixed-bps, book-walk (last optional).
- [ ] Pluggable fee schedules: flat, tiered, maker/taker.
- [ ] Performance metrics: total return, Sharpe, Sortino, max drawdown,
      profit factor, win rate, avg win/loss, expectancy.
- [ ] Brain-identical guarantee: any `impl Brain` that runs live runs in
      a backtest unchanged. Document this and add a regression test that
      runs the same brain through both paths against a tiny scripted feed.

## Phase 5 — Service-integration ergonomics (~1 week)

This is what "framework I can use with other service-level apps" actually
demands once the basics work.

- [ ] `BotHandle` API surface:
  - [ ] `health() -> BotHealth`
  - [ ] `shutdown()` — fire-and-forget cancellation trigger
  - [ ] `await_shutdown()` — future that resolves when bot has drained
  - [ ] `subscribe_signals() -> broadcast::Receiver<Signal>` — host
        service can stream brain output for logging / dashboards
- [ ] Accept an externally-owned `CancellationToken` in `Bot::new` so the
      host service can tie bot lifetime to its own shutdown sequence.
- [ ] Document the tokio runtime contract (uses `tokio::spawn`; assumes
      multi-thread runtime; how to embed inside an existing runtime).
- [ ] `BotConfig` validation: reject empty symbol list, zero poll cadence,
      conflicting flags. Return `Error::Config`, never panic.
- [ ] Bounded channel capacities everywhere — make them config knobs.
      Document the drop-oldest semantics of `broadcast`.
- [ ] Resource SLAs: document expected memory per active symbol, expected
      shutdown time, expected restart-after-crash latency.

## Phase 6 — Documentation & release (~ongoing)

- [ ] `#![warn(missing_docs)]` on every public crate.
- [ ] Rustdoc on every public item, with at least one `# Example` block on
      each trait and major struct.
- [ ] Top-level `docs/` mdbook (or rustdoc landing page) with:
  - [ ] "Your first trading bot in 50 lines"
  - [ ] "Writing a Brain" (covers `Brain` trait, state, position handling)
  - [ ] "Writing an exchange adapter" (covers `ExchangeClient`,
        `MarketSource`, `FillSource`)
  - [ ] "Embedding rustrade in your service" (covers `BotHandle`, external
        cancellation, signal subscription)
  - [ ] "Backtesting" (once Phase 4 lands)
- [ ] Decide version policy: workspace-locked `0.1.x` for all crates
      (simple, recommended) vs per-crate independent versions (flexible
      but painful). Document in CONTRIBUTING.
- [ ] Publish workflow: `cargo publish` order is core → supervisor → risk →
      backtest → rustrade. Automate with `cargo-release` or a small script.
- [ ] Pre-publish: `cargo-semver-checks` in CI to catch accidental breaks.

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
