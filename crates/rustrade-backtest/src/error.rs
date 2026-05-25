//! Errors specific to the backtest engine.

/// Result alias used throughout `rustrade-backtest`.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Configuration was missing a required field or had an invalid value.
    #[error("backtest configuration error: {0}")]
    Config(String),

    /// The `Brain` returned an error while processing a candle.
    #[error("brain error during backtest: {0}")]
    Brain(String),

    /// An invariant inside the engine was violated. These are bugs —
    /// please file an issue.
    #[error("internal backtest error: {0}")]
    Internal(String),
}

impl From<rustrade_core::Error> for Error {
    fn from(e: rustrade_core::Error) -> Self {
        Self::Brain(e.to_string())
    }
}
