# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Until 0.1.0 ships, breaking changes may land in any release; pin to an exact
version if you depend on `rustrade` before then.

## [Unreleased]

### Added
- **Facade crate, observability + optional services (Phase 2c).**
  - `Bot::with_external_cancel(token)` ties the bot's shutdown to a
    host-owned `CancellationToken` — no host-side linker task needed.
  - `Bot::with_market_source(Arc<dyn MarketSource>)` wires the source
    into a supervised `MarketFeedService`.
  - `Bot::with_fill_source(Arc<dyn FillSource>)` wires a
    `FillRoutingService` that delivers each fill to every brain via
    `Brain::on_fill` and refreshes the per-symbol position cache from
    the exchange.
  - New `signal_bus` on `Bot` (defaults to the same capacity as the
    market bus). `BotHandle::subscribe_signals` returns a
    `broadcast::Receiver<Signal>`. `ExecutionService` publishes a
    `Signal` on every non-`Hold` decision *before* the risk gates
    run — subscribers see strategic intent, whether each was acted on
    is observable from order metrics.
  - `BotHandle::signal_subscriber_count()` for diagnostics.
  - `rustrade-core` now re-exports `FillSource` and `EventSource` from
    its `lib.rs` (previously available only via the `exchange` module
    path).
  - 4 new integration tests in `tests/phase_2c.rs` covering external
    cancellation, signal subscription, market-feed wiring, and
    fill-routing end-to-end.
- **Facade crate, risk-gated execution (Phase 2b).** Builds on the
  Phase 2a facade:
  - `RiskConfig` carrying `SessionPnlConfig`, `CircuitBreakerConfig`,
    and `SizingConfig` is now part of `BotConfig`. New builder methods
    `session_pnl_config`, `circuit_breaker_config`, `sizing_config`.
  - `ExecutionService` runs the full pre-trade gate sequence:
    `SessionPnl::is_session_halted` → `CircuitBreaker::is_tripped` →
    `PositionSizer::contracts`. Each blocked decision emits a
    structured `tracing` event with the gate that fired. Buy/Sell place
    market orders; `Close` emits a `reduce_only` order sized to the
    cached position.
  - Per-symbol `RiskStateMap` and `PositionCache` constructed at
    `Bot::new` and shared between services.
  - `Bot::run_until_shutdown` prefetches positions for each configured
    symbol on startup via `exchange.get_position` (best-effort —
    failures are logged and the cache stays at `FLAT`).
  - `close_positions_on_shutdown = true` now actually closes any
    non-flat cached position via `exchange.close_position` after the
    supervisor drains. Best-effort; errors are logged.
  - `BotHandle::record_trade_outcome(&Symbol, gross_pnl, fee)` feeds
    realised trade outcomes into the per-symbol `SessionPnl` and
    `CircuitBreaker`. (Automatic fill-driven feeding lands with
    `FillRoutingService` in Phase 2c.)
  - `BotHandle::position(&Symbol)` and `BotHandle::set_position(...)`
    for host code that owns its own fill flow.
  - 7 new integration tests in `tests/risk_gates.rs` covering the
    happy path, each blocked gate, close-on-shutdown, and the close
    decision against both held and flat positions.
- **Facade crate, minimum viable (Phase 2a).** New
  [`rustrade`](./crates/rustrade) crate, the entry point downstream
  services depend on:
  - `Bot` + `BotConfig` + `BotConfigBuilder` — embedded runtime that
    owns a `Supervisor`, an `ExchangeClient`, one or more `Brain`s, and
    the in-process `MarketDataBus`.
  - `BotHandle` — cheap cloneable handle exposing `shutdown()`,
    `await_shutdown()`, `is_shutting_down()`, and `health()`. Host
    services hold one to drive the bot without retaining the `Bot`
    itself.
  - `BotHealth` + `BrainHealthSnapshot` — aggregate snapshot returned
    by `handle.health()`.
  - `ExecutionService` (Phase 2a scope) — subscribes to the
    `MarketDataBus`, calls `brain.on_event(...)` for each event,
    tracks events processed + dropped via atomics. Risk gating and
    order placement land in Phase 2b.
  - `logging::init_tracing()` — opinionated default subscriber. Skippable;
    silently no-ops when the host already has one installed.
  - Re-exports from `rustrade-core`, `rustrade-supervisor`, and
    `rustrade-risk` so downstream services depend on `rustrade` alone.
  - 6 unit tests + 3 integration tests in `crates/rustrade/tests/`
    against the public API only — same surface a downstream service
    sees.
- **Risk crate polish (Phase 1, track 3).** `rustrade-risk` picks up:
  - New `clock` module: `Clock` trait, `SystemClock` (default impl),
    `ManualClock` for tests. `Arc<C: Clock>` delegates so a single
    `Arc<ManualClock>` can be shared between a test harness and the
    risk primitive it drives.
  - `CircuitBreaker::with_clock(...)` and `SessionPnl::with_clock(...)`
    constructors. The existing `::new(...)` constructors are unchanged
    and default to `SystemClock`, so production code does not need to
    move.
  - 7 proptest property tests for `PositionSizer`: cap is never
    exceeded, all degenerate input flavours return zero, monotone in
    margin, monotone in leverage, and the unsaturated result matches
    `floor(margin·leverage / (price·cv))`.
  - 5 new unit tests using `ManualClock` for `CircuitBreaker`
    (sliding-window eviction, cooldown auto-reset on `tick()`,
    spaced-out losses never trip) and `SessionPnl` (UTC rollover via
    `tick()`, intra-day tick is a no-op).
  - Risk crate now reports **29 unit tests + 3 doc tests** (was 13 + 2).
- **Core trait surface lockdown (Phase 1, track 2).** `rustrade-core`
  picks up:
  - `Symbol` newtype gains `as_str()`, `AsRef<str>`, `Borrow<str>`,
    `PartialEq<str>`, `PartialEq<&str>`, transparent serde.
  - `Capability` enum + `ExchangeClient::supports(Capability) -> bool`
    for adapter introspection (`StopOrders`, `PostOnly`, `ReduceOnly`,
    `Ioc`, `Fok`, `PublicFeed`, `PrivateFeed`).
  - `ExchangeClient::contract_value(&Symbol) -> f64` with a `1.0` default
    so spot adapters need not override.
  - `StopAttachment` + `StopKind` (`StopMarket`, `StopLimit`,
    `TakeProfit`, `TrailingStop`) on `Order.stop`.
  - 27 unit tests in `rustrade-core` (was zero) covering `Decision`
    builders, `Position::close_side`, `Side::opposite`,
    `MarketDataEvent::symbol/exchange` for every variant,
    `Tick::mid_price/spread`, `Symbol` ergonomics, and serde roundtrips.
  - Documented cancellation contract on `MarketSource::run` — the future
    is dropped by the wrapping `TradingService`; implementors must be
    drop-safe.
- **Supervisor port (Phase 1, track 1).** `rustrade-supervisor` now has
  the full restart loop, exponential backoff with full jitter, per-service
  circuit breaker, lifecycle state machine, and graceful shutdown
  drain — ported from `nuniesmith/janus`. Adds `SpawnOptions`,
  `MetricsSnapshot`, `ServiceLifecycleSnapshot`, `TransitionError`, and
  the `BackingOff` / `Stopping` lifecycle phases. 41 new unit tests
  including three chaos tests
  (`test_chaos_exponential_backoff`, `test_chaos_circuit_breaker_trips`,
  `test_chaos_mixed_fleet`).
- New crate-local `prometheus` module (feature-gated). Hosts that want
  metrics gather from `rustrade_supervisor::prometheus::registry()` —
  no global registry is touched, so host services that already own one
  do not collide with the supervisor.
- Phase 0 project hygiene: `.gitignore`, `rust-toolchain.toml`,
  `.editorconfig`, `rustfmt.toml`, `clippy.toml`, this file,
  `CONTRIBUTING.md`, and per-crate `README.md` stubs.
- `TODO.md` tracking actionable work toward 0.1.0
  ([#1](https://github.com/nuniesmith/rustrade/pull/1)).

### Changed
- **BREAKING (`rustrade-core`):** `Tick.symbol`, `Order.symbol`, and
  `Fill.symbol` are now `Symbol` instead of `String`. `Order::market` /
  `Order::limit` take `impl Into<Symbol>` (still accepts `&str` and
  `String`).
- **BREAKING (`rustrade-core`):** `ExchangeClient::cancel_all`,
  `close_position`, and `get_position` take `&Symbol` instead of `&str`.
- **BREAKING (`rustrade-core`):** `Brain::on_position_change` takes
  `&Symbol` instead of `&str`.
- **BREAKING (`rustrade-core`):** `Order` gains a new `stop:
  Option<StopAttachment>` field. The `#[serde(default)]` means JSON from
  older callers still deserializes, but the `Order { ... }` struct
  literal must now include `stop: None` (or use the
  `Order::market`/`Order::limit`/`Order::with_stop` builders, which
  default it).
- **BREAKING (`rustrade-supervisor`):** `BackoffConfig` field rename —
  `min_delay → base_delay`, `circuit_breaker_threshold → max_retries`,
  `circuit_breaker_cooldown → circuit_breaker_window`; new
  `cooldown_period` field; `max_attempts` removed.
- **BREAKING (`rustrade-supervisor`):** `BackoffAction::Stop(String)`
  removed; `BackoffAction::CircuitOpen` now carries `{ failures,
  max_retries }`.
- **BREAKING (`rustrade-supervisor`):** `ServicePhase` variants
  `Restarting` and `Terminating` removed; `BackingOff` and `Stopping`
  added.
- **BREAKING (`rustrade-supervisor`):** `TerminationReason::NormalExit →
  Completed`, `MaxRetriesExceeded` removed,
  `CircuitBreakerOpen { failures, max_retries }` is now a struct variant.
- **BREAKING (`rustrade-supervisor`):** `ServiceLifecycle` is now a
  state machine with `transition_to_*` methods instead of bare field
  access. Snapshotting goes via `ServiceLifecycleSnapshot::from(&lc)`.

### Fixed
- Lifecycle insertion race in `Supervisor::spawn_service` — the initial
  `ServiceLifecycle` is now recorded synchronously inside the spawned
  task before any service work runs, so a service that crashes on
  startup always has a lifecycle entry.
- `Supervisor` now honours `TradingService::restart_policy()`. Previously
  the trait method existed but the supervisor ignored it.
- Removed duplicate `# rustrade` heading at the bottom of the top-level
  README ([#1](https://github.com/nuniesmith/rustrade/pull/1)).
- Cleaned up pre-existing `cargo fmt`, `cargo clippy --deny warnings`, and
  `cargo doc` findings so the workflow declared in `CONTRIBUTING.md`
  passes from day one. Touched only doc-comments and whitespace; no
  behavioural change.

## [0.1.0-pre] — unreleased

Pre-release skeleton. See [`TODO.md`](./TODO.md) for the 0.1.0 ship criteria.

- `rustrade-core` — types, traits, buses. Compiles; no unit tests yet.
- `rustrade-supervisor` — skeleton; restart logic, backoff, and lifecycle
  are placeholders pending the port from `janus-core`.
- `rustrade-risk` — complete with 13 passing unit tests and 2 doc tests.
- `rustrade-backtest` — directory reserved; not yet populated.
- `rustrade` (facade) — directory reserved; not yet populated.

[Unreleased]: https://github.com/nuniesmith/rustrade/compare/main...HEAD
