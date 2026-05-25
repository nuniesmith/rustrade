# Contributing to rustrade

Thanks for considering a contribution. Rustrade is an early-stage trading
framework; the surface area is small and changes quickly. Please read
[`TODO.md`](./TODO.md) before starting non-trivial work — it lists the
phased plan toward 0.1.0 and what is explicitly out of scope.

## Toolchain

The repository pins the toolchain via `rust-toolchain.toml` to the version
declared as the workspace MSRV. With `rustup` installed, the right
toolchain is selected automatically when you run any `cargo` command in
this directory. Verify with:

```sh
rustc --version    # should match the channel in rust-toolchain.toml
```

## Build & test

```sh
cargo check   --workspace --all-features
cargo test    --workspace --all-features
cargo clippy  --workspace --all-targets --all-features -- -D warnings
cargo fmt     --all -- --check
cargo doc     --workspace --no-deps
```

All five commands must pass before opening a PR. CI runs the same set.

## Branch & commit conventions

- Branch off `main`. Name branches descriptively
  (`feat/supervisor-backoff`, `fix/sizer-zero-leverage`, `docs/brain-trait`).
- Keep commits focused. Each commit should leave the workspace in a
  buildable state.
- Subject line: imperative mood, ≤ 72 characters, no trailing period.
  Body wrapped at 72 columns when present.
- Reference the relevant `TODO.md` item or GitHub issue in the body, not
  the subject. Example:

  ```
  Port janus-core backoff into rustrade-supervisor

  Replaces the placeholder in backoff.rs with the verbatim
  janus-core implementation. Wires it into Supervisor::spawn_service
  so failed services are retried with exponential backoff.

  Closes TODO Phase 1 § supervisor port — backoff item.
  ```

- Use `git rebase` to keep history linear before merging. Avoid merge
  commits inside feature branches; the PR merge into `main` is the only
  merge commit we want.

## Pull requests

- One logical change per PR. If a TODO item is large, split it; the
  smaller the diff, the faster the review.
- Open as a **draft** until CI is green. Mark ready-for-review only when
  the PR is ready to land.
- Update `CHANGELOG.md` under `## [Unreleased]` for any user-visible
  change. Group entries as `Added`, `Changed`, `Deprecated`, `Removed`,
  `Fixed`, `Security`.
- Tick the corresponding box in `TODO.md` in the same PR that closes it.

## Code style

- The pinned toolchain ships `rustfmt`; run `cargo fmt --all` before
  committing. The configured options live in `rustfmt.toml`.
- Public items get a doc comment. Crate-level docs describe what lives in
  the crate and what does not (see `rustrade-core/src/lib.rs` for the
  template).
- Error handling: framework crates return `rustrade_core::Error` /
  `Result`. Adapters and binaries are free to use `anyhow`.
- Tests live alongside the code in `#[cfg(test)] mod tests` blocks.
  Integration tests go under `crates/<crate>/tests/`.

## Reporting bugs & proposing features

Open a GitHub issue. For bugs, include the minimal reproducer, the
expected behaviour, and the observed behaviour. For features, sketch the
problem before the solution and explain how it fits the embedded-library
+ exchange-agnostic scope declared in `TODO.md`.

## Licence

By contributing you agree your work is offered under the MIT licence in
[`LICENSE`](./LICENSE).
