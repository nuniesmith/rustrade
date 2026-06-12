# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
All crates are versioned together at the workspace level; the whole
workspace moves as one until any single crate needs to diverge.

## [Unreleased]

### Added — CI + packaging hygiene
- **`cargo-semver-checks` CI job** (advisory, `continue-on-error`) — flags
  public-API changes that need a version bump before the next publish.
- **Weekly fuzz workflow** (`.github/workflows/fuzz.yml`) — runs the
  `load_csv` libFuzzer target for 5 minutes every Monday (and on demand via
  `workflow_dispatch`), uploading crash artifacts on failure.
- **Default-features test pass** in CI — catches code that only compiles
  under `--all-features`.
- **docs.rs metadata** (`all-features = true`) for the two feature-gated
  crates (`rustrade-supervisor`, `rustrade-framework`) so the
  prometheus-gated APIs are documented.

### Fixed — portfolio gate race under concurrent brains
- **Pending-entry reservations.** The portfolio gate read only the position
  cache, which doesn't reflect an order until its fill is processed — so two
  brains deciding concurrently could *both* pass `max_concurrent_positions` /
  `max_gross_exposure` and both place. The gate is now **check-and-reserve**:
  under a shared ledger lock it assembles the aggregate state (cache **plus**
  outstanding reservations), runs `check_entry`, and records the new entry's
  reservation. Reservations are released when the exchange rejects the order,
  when the fill-driven position-cache refresh makes the position visible, or
  after a 30 s TTL (the safety net for setups without a fill source). The
  portfolio gate now runs *after* the min-notional and order-kind gates so a
  reservation is only taken for an order actually about to be placed.

### Added — backtest risk-gate parity
- **Risk gates in the replay engine** — `BacktestConfig::{session_pnl,
  circuit_breaker}` (builder: `.session_pnl(cfg)` / `.circuit_breaker(cfg)`)
  thread the same per-symbol `SessionPnlConfig` / `CircuitBreakerConfig` the
  live bot runs into the backtest. The engine applies the live gate sequence
  (session halt, then breaker, blocking **every** non-`Hold` decision —
  including `Close` — exactly like `ExecutionService`) and feeds each
  realised `TradeOutcome` back into the gates, mirroring
  `FillRoutingService`'s auto-PnL. Time is driven by candle timestamps
  through a `ManualClock`, so the daily halt rolls over at 00:00 UTC in
  *replay* time and the breaker window/cooldown run on candle time — runs
  stay fully deterministic. Both gates default to off; existing backtests
  are unchanged.
- **`BacktestResult::orders_blocked`** — count of decisions a risk gate
  blocked (also shown in `summary()`). `#[serde(default)]`, so previously
  serialized results still deserialize.

### Added — live-safety hardening
- **`BracketFailurePolicy`** — configurable handling for a bracket entry whose
  protective stop-loss leg fails to place
  (`BotConfigBuilder::bracket_failure_policy`). The default, `CloseEntry`,
  immediately closes the unprotected entry with a reduce-only market order
  (previously the position was left resting with only an error log);
  `KeepUnprotected` opts back into log-and-keep for hosts with their own
  protective layer.
- **`rustrade_invalid_fills_total` / `rustrade_unrecorded_fills_total`
  metrics** — counters for fills dropped at the ingestion boundary and fills
  whose realised PnL could not be attributed to a configured symbol.

### Changed — bracket degradation + risk-state poisoning guards
- **Take-profit leg failure now keeps the stop-loss.** Previously a TP-leg
  rejection cancelled the already-placed SL "to avoid an orphan", leaving the
  entry with no protection at all. The SL (reduce-only, so safe to rest) is now
  kept and the bracket degrades to stop-only protection, mirroring the
  `attach_protection` fallback.
- **Non-finite fills are dropped at the boundary.** `FillRoutingService` now
  rejects fills with a non-finite price/size/fee (or negative size) before they
  are routed to brains or recorded — a NaN reaching `SessionPnl::record_close`
  made the accumulated PnL NaN and silently disabled the loss-limit halt
  (every NaN comparison is false). `BotHandle::record_trade_outcome` rejects
  non-finite PnL the same way, the computed gross/fee in the auto-PnL path is
  re-checked, and a non-finite position returned by `get_position` no longer
  overwrites the cache.
- **Fills for unconfigured symbols now warn.** A realised PnL that no risk
  gate will ever see was previously logged at debug level; it is now a
  `warn` with a counter, since it usually means a symbol is missing from
  `BotConfig.symbols`.

## [0.3.0] - 2026-06-01

### Added — multi-asset risk layer (account-level + asset-class)
- **`PortfolioRisk` (`rustrade-risk`)** — account-wide risk complementing the
  per-symbol gates: a latching daily-loss halt (net realised PnL summed across
  symbols, sticky until the 00:00 UTC rollover), a max-concurrent-positions cap,
  and a gross-exposure cap. Checked as a third pre-trade gate in the execution
  service (entries only). Configured via `BotConfig::portfolio`
  (`BotConfigBuilder::portfolio_config`); defaults to all-off (opt-in), so
  existing bots are unaffected.
- **`RiskSweepService`** — a supervised periodic sweep that `tick()`s every
  symbol's `SessionPnl` / `CircuitBreaker` and the `PortfolioRisk` during a live
  run, so the daily-loss halt rolls over at 00:00 UTC (previously `tick()` only
  ran on restart — a long-running bot never rolled over).
- **`InstrumentSpec` + `AssetClass` (`rustrade-core`)** — instrument metadata on
  `ExchangeClient::instrument_spec(&Symbol)`: contract value, price tick,
  quantity lot, min notional, and asset class, generalising the lone
  `contract_value` hook (subsumed by the spec). The execution service sizes from
  the spec, enforces min-notional, and snaps limit prices to the tick — all
  no-ops under the permissive default, so existing adapters are unaffected.
- **Per-asset-class `RiskConfig` presets** — `RiskConfig::{crypto_perp,
  crypto_spot, fx, futures, equity, preset_for}` plus `BotConfig::per_class_risk`
  (`BotConfigBuilder::class_risk`). Each symbol resolves its effective config
  (risk gates **and** sizing) per-symbol → per-class (keyed off
  `instrument_spec().asset_class`) → default, so one bot trades crypto-perps /
  FX / futures side by side with class-correct leverage and limits.
- **`JsonFileStore` (`rustrade` facade)** — the first durable `StateStore`: an
  atomic (temp-file + rename), write-through JSON-file backend. With
  `Bot::with_state_store`, per-symbol risk state (session-PnL halt + breaker)
  survives a restart; the portfolio halt re-derives from the restored per-symbol
  PnLs via the sweep. (Adds the `fs` tokio feature to the facade.)

### Added — execution & order management
- **SL+TP bracket orders with OCO sibling cancellation.** A `Decision` can
  attach a stop-loss and take-profit; the execution service places them as a
  linked pair and cancels the sibling when either fills (one-cancels-other), so
  a bracketed position can't leave a dangling resting order.
- **Per-symbol `RiskConfig` overrides.** `BotConfig` can carry a distinct
  `RiskConfig` per symbol (the precursor to the per-asset-class resolution
  added above), so one bot runs symbols at different risk budgets.
- **Multi-brain-per-symbol arbitration via `owned_symbols`.** A bot can run
  several `Brain`s, each declaring the symbols it owns; events route only to the
  owning brain, so independent strategies share one bot without cross-talk.

### Changed
- **Facade integration tests now use deterministic virtual time
  (Tier 2 resilience).** Every timing-dependent test across
  `phase_2c`, `phase_2d`, `risk_gates`, and `bot_lifecycle` runs under
  `#[tokio::test(start_paused = true)]`. Wall-clock `sleep`/deadline
  polling — the source of a long-running macOS CI flake that went red
  on five consecutive PRs — is gone: ready tasks fully drain before the
  virtual clock advances, so assertions see settled state regardless of
  runner speed. The two prefetch-race tests additionally seed the
  position via the *exchange* before startup (so `prefetch_positions`
  reads the seed) instead of racing it through the handle. Stress-run
  50× locally with zero failures; the suite now completes in
  milliseconds instead of seconds.

## [0.2.1] - 2026-05-31

### Added
- **Order lifecycle tracking — `OrderTracker` + TTL reaper + reconciliation.**
  Submitted orders are tracked through their lifecycle; a TTL reaper cancels
  orders that linger unfilled past a deadline, and a reconciliation pass
  realigns local order/position state with the exchange's view on reconnect.

### Fixed
- Made the `order_tracking` integration tests slow-runner-proof (removed
  fixed-iteration settles that could flake on a slow CI runner).

## [0.2.0] - 2026-05-31

### Added — risk-state persistence & order semantics
- **`StateStore` trait + persistence wiring.** Session-PnL halt and
  circuit-breaker state are read/written through a `StateStore` so risk state
  survives a restart (0.1 was in-memory only; the durable file-backed
  `JsonFileStore` impl arrived in 0.3.0). Adds `SymbolRiskSnapshot` (serde).
- **`Decision` stops + order kinds honoured end-to-end.** The execution service
  now acts on a `Decision`'s stop price and order kind (market/limit) through to
  the exchange adapter, instead of treating every decision as a bare market order.

### Added — backtest robustness (Tier 1 / Tier 2)
- **Brain-panic isolation test.** `tests/brain_panic.rs` proves a
  `Brain` that panics inside `on_event` unwinds only its own supervised
  `ExecutionService` task — a sibling brain keeps processing and the bot
  still drains cleanly on shutdown. The `Brain::on_event` docs gain a
  `# Panics` section spelling out the contract: panics are bugs, not
  control flow, and a `panic = "abort"` release build aborts the whole
  process (no task-level isolation there) — return `Err` for anything
  recoverable.
- **CSV loader fuzzing.** `crates/rustrade-backtest/tests/loader_robustness.rs`
  is a stable, every-PR proptest asserting `load_csv_str` never panics
  and never emits a non-finite candle across thousands of arbitrary and
  structured-hostile inputs. A coverage-guided `cargo-fuzz` harness over
  the same entry point lives in the workspace-excluded `fuzz/` crate
  (nightly-only; see `fuzz/README.md`) — verified to build and ran
  386k iterations crash-free.

### Fixed
- **Multi-symbol backtests are now actually deterministic.**
  `Backtest`'s engine summed unrealised PnL by iterating a `HashMap`,
  whose per-process-randomized order made the equity curve (and thus
  Sharpe/Sortino) differ by a ULP between otherwise-identical runs once
  two or more positions were open — violating the engine's headline
  determinism guarantee. The per-symbol position and mark maps are now
  `BTreeMap`s (sorted, fixed iteration order). Regression test
  `multi_symbol_equity_curve_deterministic_across_runs` asserts a
  bit-identical equity curve / PnL across two runs with three open
  positions. Single-symbol runs were never affected.

### Added
- **NaN/inf/negative candle guards.** `load_csv` / `load_csv_str` and
  `Backtest::run` now reject non-finite or non-positive OHLC prices and
  non-finite/negative volume with the new `Error::Data` variant, instead
  of letting one bad value silently turn the whole equity curve and
  every metric into `NaN`. OHLC *ordering* is intentionally not policed.
- **Property tests for the engine** (`tests/engine_properties.rs`, via
  `proptest`). Random candle series × random decision streams assert the
  invariants that must hold for any input: no `NaN`/`inf` leaks,
  `net_pnl == Σ trade.net_pnl`, `total_fees == Σ trade.fee`,
  `final_cash == initial + net_pnl`, correct curve/return lengths,
  non-positive drawdown, and bit-identical determinism across reruns.

### Changed
- **(`rustrade-core`)** `Symbol` now derives `PartialOrd`/`Ord` so it can
  key a `BTreeMap`. Additive — it gains trait impls, breaks nothing.

## [0.1.0] - 2026-05-29

First tagged release. The framework is feature-complete against its 0.1.0
[definition of done](./TODO.md): every crate is usable, CI is green on
Linux + macOS across MSRV (1.94.1) and stable, and workspace coverage is
~91% lines. Everything below was developed across the Phase 0–6 batches
and is collected here as the 0.1.0 surface.

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

[Unreleased]: https://github.com/nuniesmith/rustrade/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/nuniesmith/rustrade/releases/tag/v0.1.0
