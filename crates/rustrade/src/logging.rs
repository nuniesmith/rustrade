//! Opinionated default tracing subscriber for downstream services that
//! don't already own one.
//!
//! Skippable — if the host service has already installed a
//! `tracing_subscriber`, calling [`init_tracing`] is a no-op (the
//! `try_init` call returns an error, which we deliberately swallow).

use tracing_subscriber::EnvFilter;

/// Install a default tracing subscriber.
///
/// - Reads filter directives from `RUST_LOG`; defaults to `info` for the
///   rustrade crates and `warn` for everything else when unset.
/// - Uses the `tracing-subscriber` "fmt" formatter with target + level +
///   message; no ANSI colours when not on a TTY.
/// - Returns `true` if this call installed the subscriber, `false` if a
///   subscriber was already installed (e.g. by the host) — either way,
///   logging is wired up.
pub fn init_tracing() -> bool {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "warn,rustrade=info,rustrade_core=info,rustrade_supervisor=info,rustrade_risk=info",
        )
    });

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .try_init()
        .is_ok()
}
