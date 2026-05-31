//! Persistence contract for framework state that must survive restarts.
//!
//! In 0.1 the risk primitives ([`SessionPnl`], [`CircuitBreaker`] in
//! `rustrade-risk`) live entirely in memory, so a crash mid-session
//! resets the daily drawdown cap and the loss-streak breaker. A
//! [`StateStore`] lets the framework snapshot that state and restore it
//! on the next boot.
//!
//! The trait is deliberately payload-agnostic: it stores opaque,
//! versioned [`serde_json::Value`] blobs keyed by a string. The framework
//! (the `rustrade` facade) owns the snapshot schema and the key layout;
//! a backend only has to persist bytes durably. This keeps `rustrade-core`
//! free of any real I/O — concrete durable backends (a JSON file, sqlite,
//! Postgres, Redis, …) live in downstream crates and only need to depend
//! on this trait.
//!
//! [`SessionPnl`]: https://docs.rs/rustrade-risk/latest/rustrade_risk/session_pnl/struct.SessionPnl.html
//! [`CircuitBreaker`]: https://docs.rs/rustrade-risk/latest/rustrade_risk/circuit_breaker/struct.CircuitBreaker.html

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::error::{Error, Result};

/// A durable key/value store for framework state snapshots.
///
/// Implementors persist opaque [`serde_json::Value`] payloads keyed by a
/// string. The framework calls [`save`](StateStore::save) after every
/// realised trade and on graceful shutdown, and [`load`](StateStore::load)
/// once per symbol at startup. Keys are stable for the lifetime of a
/// `(bot, symbol)` pair.
///
/// # Object-safe + async
///
/// `async_trait` is used so `Arc<dyn StateStore>` works — the facade holds
/// the store behind a trait object and never names the concrete backend.
///
/// # Example
///
/// A trivial echo store backed by a `Mutex<HashMap>` — this is exactly
/// what [`InMemoryStore`] does. Real backends write to disk or a database.
///
/// ```
/// use std::collections::HashMap;
/// use std::sync::{Arc, Mutex};
/// use async_trait::async_trait;
/// use rustrade_core::{Result, StateStore};
///
/// #[derive(Default)]
/// struct MapStore(Arc<Mutex<HashMap<String, serde_json::Value>>>);
///
/// #[async_trait]
/// impl StateStore for MapStore {
///     async fn load(&self, key: &str) -> Result<Option<serde_json::Value>> {
///         Ok(self.0.lock().unwrap().get(key).cloned())
///     }
///     async fn save(&self, key: &str, value: serde_json::Value) -> Result<()> {
///         self.0.lock().unwrap().insert(key.to_string(), value);
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait StateStore: Send + Sync + 'static {
    /// Load the snapshot stored under `key`, or `None` if there is none.
    ///
    /// A missing key is **not** an error — first-ever boots return `None`.
    /// Reserve [`Error::Storage`] for genuine backend failures (I/O,
    /// deserialization of a corrupt blob).
    async fn load(&self, key: &str) -> Result<Option<serde_json::Value>>;

    /// Persist `value` under `key`, overwriting any previous snapshot.
    async fn save(&self, key: &str, value: serde_json::Value) -> Result<()>;

    /// Flush any buffered writes to durable storage.
    ///
    /// The default is a no-op for stores that write through on every
    /// [`save`](StateStore::save). Buffered backends override it; the
    /// framework calls it once on graceful shutdown.
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

/// Non-durable [`StateStore`] backed by an in-memory map.
///
/// This is the default the framework uses when no store is configured: it
/// keeps snapshots only for the lifetime of the process, so it does **not**
/// survive a restart. It is useful as an explicit default, in tests, and
/// as a reference implementation. For real restart durability, wire a
/// disk- or database-backed store via `Bot::with_state_store`.
///
/// Cheaply cloneable — clones share the same underlying map (an `Arc`),
/// so two handles to the same `InMemoryStore` see each other's writes.
#[derive(Debug, Clone, Default)]
pub struct InMemoryStore {
    inner: Arc<Mutex<HashMap<String, serde_json::Value>>>,
}

impl InMemoryStore {
    /// Create an empty in-memory store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of keys currently held.
    pub fn len(&self) -> usize {
        self.inner.lock().expect("InMemoryStore poisoned").len()
    }

    /// `true` when no keys are held.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl StateStore for InMemoryStore {
    async fn load(&self, key: &str) -> Result<Option<serde_json::Value>> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| Error::Storage("InMemoryStore mutex poisoned".into()))?
            .get(key)
            .cloned())
    }

    async fn save(&self, key: &str, value: serde_json::Value) -> Result<()> {
        self.inner
            .lock()
            .map_err(|_| Error::Storage("InMemoryStore mutex poisoned".into()))?
            .insert(key.to_string(), value);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn load_missing_key_returns_none() {
        let store = InMemoryStore::new();
        assert!(store.load("nope").await.unwrap().is_none());
        assert!(store.is_empty());
    }

    #[tokio::test]
    async fn save_then_load_roundtrips() {
        let store = InMemoryStore::new();
        let val = serde_json::json!({"realised": 12.5, "halted": true});
        store.save("bot/BTCUSDT", val.clone()).await.unwrap();
        assert_eq!(store.load("bot/BTCUSDT").await.unwrap(), Some(val));
        assert_eq!(store.len(), 1);
    }

    #[tokio::test]
    async fn save_overwrites_previous() {
        let store = InMemoryStore::new();
        store.save("k", serde_json::json!(1)).await.unwrap();
        store.save("k", serde_json::json!(2)).await.unwrap();
        assert_eq!(store.load("k").await.unwrap(), Some(serde_json::json!(2)));
        assert_eq!(store.len(), 1);
    }

    #[tokio::test]
    async fn clones_share_state() {
        let a = InMemoryStore::new();
        let b = a.clone();
        a.save("k", serde_json::json!("v")).await.unwrap();
        // b sees a's write — same backing Arc.
        assert_eq!(b.load("k").await.unwrap(), Some(serde_json::json!("v")));
    }

    #[tokio::test]
    async fn flush_default_is_ok() {
        let store = InMemoryStore::new();
        assert!(store.flush().await.is_ok());
    }
}
