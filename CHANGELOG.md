# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Until 0.1.0 ships, breaking changes may land in any release; pin to an exact
version if you depend on `rustrade` before then.

## [Unreleased]

### Added
- Phase 0 project hygiene: `.gitignore`, `rust-toolchain.toml`,
  `.editorconfig`, `rustfmt.toml`, `clippy.toml`, this file,
  `CONTRIBUTING.md`, and per-crate `README.md` stubs.
- `TODO.md` tracking actionable work toward 0.1.0
  ([#1](https://github.com/nuniesmith/rustrade/pull/1)).

### Fixed
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
