# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Until 0.1.0 ships, breaking changes may land in any release; pin to an exact
version if you depend on `rustrade` before then.

## [Unreleased]

### Added
- **Ship-prep cleanup.**
  - New `cargo test (ubuntu-latest, stable)` CI job alongside the
    existing MSRV (1.94.1) matrix. Uses `dtolnay/rust-toolchain@stable`
    so it tracks whatever `stable` resolves to today — catches
    future-incompat warnings, new clippy lints, and stdlib
    deprecations before downstream users on stable hit them. Kept to
    one OS so CI cost only grows by ~30 %; the macOS axis already
    covers platform portability against MSRV.

### Changed
- **TODO.md reconciled with reality.** The Status snapshot now
  reflects what's actually shipped (every crate complete, every test
  count current). All Definition-of-Done boxes are ticked. The
  Cross-cutting CI checklist is marked done where the workflow now
  has it, with `cargo-audit` remaining noted as deferred-but-subsumed
  by `cargo-deny`. The last open design decision (Parameter
  overrides) is resolved: the as-built architecture already answers
  it — each subsystem owns its own config struct, brain params are
  framework-opaque.

### Added
- **Doc examples + coverage in CI (Phase 6c).**
  - `# Example` rustdoc block on every public trait
    (`Brain`, `ExchangeClient`, `MarketSource`, `FillSource`,
    `EventSource`, `CandleSource`, `MetricsSink`, `Clock`,
    `TradingService`) and every major framework struct
    (`Bot`, `BotConfig`, `BotHandle`, `Supervisor`, `BackoffConfig`,
    `PositionSizer`, `Decision`, `Position`, `Order`, `Backtest`,
    `BacktestConfig`, `SlippageModel`, `FeeModel`). Most are
    runnable doctests; the few that need an `ExchangeClient` /
    `Brain` from outside the example are `no_run` and use `#`-prefixed
    setup so docs.rs still shows the relevant code.
  - New `coverage` CI job using `cargo-llvm-cov`. Runs on every PR
    and every push to `main`, generates `lcov.info`, prints the
    text summary in the workflow log, posts a sticky PR comment via
    `marocchino/sticky-pull-request-comment` (one comment per PR,
    edited in place on subsequent pushes), and uploads `lcov.info`
    as a 14-day retention artefact for offline analysis.

### Added
- **Backtest engine, Phase 4b — CSV loader + Sharpe/Sortino + multi-symbol.**
  - CSV candle loader: `load_csv` (path), `load_csv_str` (in-memory),
    and `sort_chronological` for newest-first sources. Fixed
    `time,open,high,low,close,volume` column layout, `#` comments and
    blank lines skipped, malformed rows surface as `Error::Config`
    with a 1-based row index.
  - `BacktestResult.sharpe_ratio()` and `.sortino_ratio()` driven by
    per-candle equity sampling. New `BacktestResult.equity_curve` and
    `period_returns` fields; new `BacktestConfig.risk_free_rate`
    (default `0.0`) and `periods_per_year` (default `252`)
    annualisation knobs. Both ratios return `None` when undefined
    (fewer than two samples, zero variance for Sharpe, no downside
    for Sortino).
  - Multi-symbol replay. `BacktestConfig.symbols: Vec<Symbol>` (the
    old `symbol` field is gone; use `BacktestConfig::symbol()` for
    single-symbol access). `BacktestConfigBuilder::symbols(iter)` for
    multi-symbol configs; the existing `.symbol(...)` method still
    works as a single-symbol shorthand. New
    `Backtest::with_symbol_candles(symbol, candles)` attaches a series
    per symbol; the engine merges all series chronologically before
    replay and maintains independent `Position` state per symbol
    against one shared cash balance. The brain sees the global
    event stream and is responsible for any per-symbol filtering.
  - 5 new integration tests in `crates/rustrade-backtest/tests/phase_4b.rs`
    covering CSV → engine end-to-end, chronological sorting, multi-symbol
    routing, chronological merge across symbols, and finite Sharpe /
    Sortino on a synthetic replay. Unit tests expand to 31 (was 15).

### Added
- **Framework observability + candle polling + auto-PnL (Phase 2d).**
  - `MetricsSink` trait in `rustrade-core` with default `NoopSink`.
    `Bot::with_metrics(Arc<dyn MetricsSink>)` plugs in a host-owned
    backend (Prometheus, StatsD, OpenTelemetry, etc.). The framework's
    services emit `rustrade_fills_routed_total`,
    `rustrade_candles_published_total`,
    `rustrade_realised_pnl_quote`, and friends on every observable
    event.
  - `CandleSource` trait in `rustrade-core`. Separate from
    `MarketSource` because polling has a fundamentally different shape
    (pull, paced) than streaming events.
  - `CandlePollerService` + `Bot::with_candle_poller(source, symbol,
    interval, poll_cadence, limit)`. Per-symbol cadences via repeated
    calls. Deduplicates by `Candle::time` so overlapping responses
    don't republish.
  - `FillRoutingService` now auto-feeds realised PnL into the
    per-symbol risk state using a weighted-average entry model (the
    same model the backtest engine uses). Reducing fills emit
    `record_close` + a `record_win` / `record_loss` on the breaker;
    same-direction fills are no-ops; flip fills emit PnL for the
    closed portion only. Hosts that want a different accounting model
    can still use the manual `BotHandle::record_trade_outcome`, but
    cannot also wire a `FillRoutingService` (they'd double-count).
  - 4 new integration tests in `crates/rustrade/tests/phase_2d.rs`:
    candle poller dedup, metrics sink receives fill counters,
    auto-PnL feeds the circuit breaker, NoopSink default doesn't
    panic.

### Added
- **CI + extended tutorials (Phase 6b).**
  - GitHub Actions workflow `.github/workflows/ci.yml`: `fmt`, `clippy`
    (with and without `--all-features`), `test` matrix on
    `ubuntu-latest` + `macos-latest`, `doc` with `-D warnings`, and
    `cargo-deny` for licences + advisories + duplicate-dep policy.
    Every PR and push to `main` runs the full gauntlet.
  - `.github/dependabot.yml`: weekly Cargo + GitHub Actions updates,
    grouped by `tokio*` / `tracing*` to reduce PR noise.
  - `deny.toml`: licence allow-list, advisory blocking, registry/git
    source pinning. Catches new transitive deps with surprising
    licences before they land.
  - Four new tutorials in `docs/`:
    - `writing-a-brain.md` — the `Brain` trait, the canonical
      `Mutex<State>` pattern, a worked SMA crossover, what the
      framework does next.
    - `writing-an-exchange-adapter.md` — `ExchangeClient`,
      `MarketSource`, `FillSource`, `Capability` introspection,
      `contract_value`, the cancellation contract, leverage and
      symbol typing.
    - `embedding.md` — `BotHandle` API surface, external cancellation,
      signal subscription, runtime + resource expectations,
      feeding the risk gates.
    - `backtesting.md` — the brain-identical guarantee, the position
      state machine for closes and flips, determinism, what the
      engine intentionally doesn't do.
  - README.md "Getting started" section expanded with links to each
    tutorial.

### Added
- **Documentation + release polish (Phase 6a).**
  - `#![warn(missing_docs)]` on every published crate. Every public
    item now carries at least a one-line rustdoc, surfaced through
    `cargo doc`. Tightens the docs.rs landing page and CI catches
    regressions.
  - New `docs/quickstart.md` — "Your first rustrade bot in 50 lines"
    walks through wiring a `Brain`, a stub `ExchangeClient`, and the
    `Bot` builder end-to-end, mirroring `examples/noop-bot/`.
  - Versioning policy documented in `CONTRIBUTING.md`: workspace-locked
    `0.1.x` for all crates plus the planned publish ordering
    (core → supervisor → risk → backtest → rustrade).
  - Top-level `README.md` gains a "Getting started" section linking
    the quickstart, all four examples, and the API docs.

### Added
- **Service-integration ergonomics (Phase 5).**
  - `BotConfig.signal_bus_capacity` (default 256) now separate from
    `market_bus_capacity` (default 1024). Signal-bus consumers can be
    sized independently of the market-data bus.
  - Crate-level rustdoc gains "Tokio runtime requirements" and
    "Resource expectations" sections covering multi-thread runtime
    requirement, memory-per-symbol estimate, channel drop-oldest
    semantics, expected shutdown time, and restart-after-crash latency
    bounds. `Bot::run_until_shutdown` gets the same coverage in its
    method docs.
  - Stricter config validation in `BotConfigBuilder::build`:
    - Empty symbol list → `Error::Config`
    - Zero `shutdown_timeout` → `Error::Config`
    - NaN `session_pnl.loss_limit` → `Error::Config`
    - Non-finite or negative `sizing.margin_per_trade` → `Error::Config`
    - Zero `signal_bus_capacity` → `Error::Config` (already existed for
      `market_bus_capacity`)
  - Six new builder validation tests; the rustrade-crate unit-test
    count is now 13 (was 7).

### Added
- **Backtest engine, minimum viable (Phase 4a).** New
  [`rustrade-backtest`](./crates/rustrade-backtest) crate. The same
  `Brain` trait used by `rustrade` for live trading drives the
  backtest — no special "backtest-mode" code paths in the strategy.
  - `Backtest` / `BacktestConfig` / `BacktestConfigBuilder` — single-
    threaded synchronous replay loop fed by a `Vec<Candle>`.
  - `SlippageModel`: `Zero`, `FixedBps` (book-walk waits for Phase 4b).
  - `FeeModel`: `Zero`, `Flat`, `MakerTaker` (Phase 4a treats every
    order as taker).
  - `BacktestResult` aggregates `TradeOutcome`s into total return, win
    rate, profit factor, max drawdown, and per-trade ledger.
  - Determinism guarantee: same `(Brain, candles, config)` → same
    `BacktestResult` every run. Pinned down by
    `tests/sma_replay.rs::deterministic_replay_same_brain_same_series`.
  - Brain-identical guarantee: `tests/sma_replay.rs` runs an
    SMA-crossover `Brain` through the engine end-to-end — same trait
    impl that `examples/sma-cross-bot` uses for the live path.
  - 15 unit tests + 3 integration tests covering hold/buy/close paths,
    determinism, slippage-reduces-PnL invariant, and the SMA-crossover
    replay.
- **Examples + end-to-end validation (Phase 3).** Four reference
  embeddings in `examples/`, each a workspace member with its own
  `Cargo.toml` so downstream services can clone the shape verbatim:
  - `noop-bot` — minimum viable embedding. `NoopBrain` + mock exchange,
    runs for `N` seconds (default 10), asserts no orders placed.
  - `sma-cross-bot` — fast(5)/slow(20) SMA-crossover `Brain` against a
    deterministic sinusoidal candle replay driven by a `MarketSource`.
    Ships a `#[tokio::test]` that runs the same series twice and pins
    down the order count for regression testing.
  - `multi-brain-bot` — two `Brain`s in one `Bot`, each filtering events
    to its own symbol. Asserts both brains see exactly the events for
    their symbol.
  - `embed-in-service` — host-service embedding pattern. Host owns the
    runtime + shutdown `CancellationToken`; the bot is wired via
    `Bot::with_external_cancel`; the host subscribes to signals and
    publishes events to `bot.market_data_bus()`.
- `BotHandle::subscribe_signals` now documents the subscriber-lifetime
  pitfall — the channel does not close when the bot exits because
  `BotHandle` keeps a `Sender` clone alive. Includes a `tokio::select!`
  pattern subscribers should use.

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
- **BREAKING (`rustrade-backtest`):** `BacktestConfig.symbol: Symbol`
  is now `symbols: Vec<Symbol>`. For single-symbol callers the builder
  is unchanged (`.symbol("X")` still works); use
  `BacktestConfig::symbol()` to read the (single) configured symbol
  back. `BacktestResult.symbol` is now a comma-separated string in
  multi-symbol configs.
- **BREAKING (`rustrade-backtest`):** `BacktestConfig` gained
  `risk_free_rate: f64` and `periods_per_year: u32` fields with
  defaults `0.0` and `252`. Struct-literal constructors must include
  them; the builder API is unchanged.
- **BREAKING (`rustrade-backtest`):** `BacktestResult` gained
  `equity_curve: Vec<f64>`, `period_returns: Vec<f64>`,
  `risk_free_rate: f64`, and `periods_per_year: u32` fields. Hosts
  that build a `BacktestResult` directly (uncommon — the engine is
  the only producer in the wild) must populate them.
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
