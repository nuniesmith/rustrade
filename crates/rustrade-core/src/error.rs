//! Unified error type for the rustrade framework.
//!
//! Downstream crates should either use this type directly or wrap it in their
//! own error enum via `#[from]`. The variants are intentionally coarse — this
//! is a framework-level type, not a domain-specific one.

use thiserror::Error;

/// Result alias used throughout rustrade.
pub type Result<T> = std::result::Result<T, Error>;

/// Framework-level errors.
///
/// Exchange adapters, brain implementations, and risk checks wrap their
/// domain-specific errors in one of these variants.
#[derive(Debug, Error)]
pub enum Error {
    /// The exchange rejected the request, or the connection to it failed.
    #[error("exchange error: {0}")]
    Exchange(String),

    /// The brain returned an error while processing a market event.
    #[error("brain error: {0}")]
    Brain(String),

    /// A risk check blocked an action.
    #[error("risk check blocked: {0}")]
    Risk(String),

    /// Configuration is invalid or incomplete.
    #[error("configuration error: {0}")]
    Config(String),

    /// Serialization or deserialization failed.
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),

    /// An otherwise-uncategorised framework error.
    #[error("internal error: {0}")]
    Internal(String),
}

impl Error {
    /// Convenience constructor for `Exchange` variant.
    pub fn exchange(msg: impl Into<String>) -> Self {
        Self::Exchange(msg.into())
    }

    /// Convenience constructor for `Brain` variant.
    pub fn brain(msg: impl Into<String>) -> Self {
        Self::Brain(msg.into())
    }

    /// Convenience constructor for `Risk` variant.
    pub fn risk(msg: impl Into<String>) -> Self {
        Self::Risk(msg.into())
    }

    /// Convenience constructor for `Config` variant.
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    /// Convenience constructor for `Internal` variant.
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }
}
