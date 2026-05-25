# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Until 0.1.0 ships, breaking changes may land in any release; pin to an exact
version if you depend on `rustrade` before then.

## [Unreleased]

### Added
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
