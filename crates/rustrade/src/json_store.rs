//! [`JsonFileStore`] — a durable [`StateStore`] backed by a single JSON file.
//!
//! This is the simplest real-durability backend: the framework's risk
//! snapshots (per-symbol [`SessionPnl`](rustrade_risk::SessionPnl) /
//! [`CircuitBreaker`](rustrade_risk::CircuitBreaker) and the account
//! [`PortfolioRisk`](rustrade_risk::PortfolioRisk) latch) survive a restart, so
//! a halted daily session or a tripped breaker isn't forgotten when the bot
//! reboots. Wire it with `Bot::with_state_store`.
//!
//! Writes are **write-through and atomic**: every
//! [`save`](StateStore::save) (and [`flush`](StateStore::flush)) serialises the
//! full map to a sibling temp file and `rename`s it over the target, so a crash
//! mid-write can't corrupt the store — you either keep the previous good file
//! or get the new one. Saves are infrequent (one per realised trade) and the
//! payload is tiny (a handful of symbols), so rewriting the whole file each
//! time is cheap.
//!
//! For higher write volumes or multi-process access, a sqlite/Postgres backend
//! (another `StateStore` impl) is the next step — the framework only depends on
//! the trait.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::Mutex;

use rustrade_core::{Error, Result, StateStore};

/// A [`StateStore`] that persists snapshots to a JSON file on disk.
///
/// Open one with [`JsonFileStore::open`] (which loads any existing file), then
/// hand it to `Bot::with_state_store`:
///
/// ```no_run
/// use std::sync::Arc;
/// use rustrade::JsonFileStore;
/// # async fn demo() -> rustrade::Result<()> {
/// let store = Arc::new(JsonFileStore::open("/var/lib/fks/bot-risk.json").await?);
/// // Bot::builder()...build()?.with_state_store(store)
/// # let _ = store;
/// # Ok(())
/// # }
/// ```
pub struct JsonFileStore {
    path: PathBuf,
    // Async mutex: held across the file write in `save`/`flush`, which both
    // serialises writers (no concurrent file writes ⇒ no torn temp file) and
    // keeps the in-memory map the single source of truth.
    data: Mutex<HashMap<String, Value>>,
}

impl JsonFileStore {
    /// Open (or create) a store at `path`, loading any existing snapshot map.
    ///
    /// A missing file starts empty (first boot). The parent directory is
    /// created if needed.
    ///
    /// # Errors
    /// [`Error::Storage`] if the parent dir can't be created, the file can't be
    /// read, or an existing file is corrupt (not valid JSON object) — corruption
    /// is surfaced rather than silently dropping persisted state.
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();

        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                Error::Storage(format!(
                    "JsonFileStore: create dir {}: {e}",
                    parent.display()
                ))
            })?;
        }

        let data = match tokio::fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice::<HashMap<String, Value>>(&bytes).map_err(|e| {
                Error::Storage(format!(
                    "JsonFileStore: corrupt store {}: {e}",
                    path.display()
                ))
            })?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(e) => {
                return Err(Error::Storage(format!(
                    "JsonFileStore: read {}: {e}",
                    path.display()
                )));
            }
        };

        Ok(Self {
            path,
            data: Mutex::new(data),
        })
    }

    /// The file path this store persists to.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Serialise `map` and write it atomically to `path` (temp file + rename).
async fn persist(map: &HashMap<String, Value>, path: &Path) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(map)
        .map_err(|e| Error::Storage(format!("JsonFileStore: serialize: {e}")))?;

    let mut tmp = path.to_path_buf();
    tmp.set_extension("json.tmp");

    tokio::fs::write(&tmp, &bytes)
        .await
        .map_err(|e| Error::Storage(format!("JsonFileStore: write {}: {e}", tmp.display())))?;
    tokio::fs::rename(&tmp, path).await.map_err(|e| {
        Error::Storage(format!(
            "JsonFileStore: rename {} -> {}: {e}",
            tmp.display(),
            path.display()
        ))
    })?;
    Ok(())
}

#[async_trait]
impl StateStore for JsonFileStore {
    async fn load(&self, key: &str) -> Result<Option<Value>> {
        Ok(self.data.lock().await.get(key).cloned())
    }

    async fn save(&self, key: &str, value: Value) -> Result<()> {
        // Write-through under the lock so a crash right after a realised trade
        // keeps that trade's effect on the risk gates.
        let mut map = self.data.lock().await;
        map.insert(key.to_string(), value);
        persist(&map, &self.path).await
    }

    async fn flush(&self) -> Result<()> {
        let map = self.data.lock().await;
        persist(&map, &self.path).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A unique temp path per test invocation (no `tempfile` dep).
    fn temp_path() -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "rustrade-jsonstore-{}-{n}/risk.json",
            std::process::id()
        ))
    }

    /// Remove the temp dir a `temp_path()` lives in (best effort).
    fn cleanup(path: &Path) {
        if let Some(dir) = path.parent() {
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    #[tokio::test]
    async fn open_missing_file_starts_empty() {
        let path = temp_path();
        let store = JsonFileStore::open(&path).await.unwrap();
        assert!(store.load("nope").await.unwrap().is_none());
        cleanup(&path);
    }

    #[tokio::test]
    async fn save_then_load_roundtrips() {
        let path = temp_path();
        let store = JsonFileStore::open(&path).await.unwrap();
        let v = serde_json::json!({ "realised": 12.5, "halted": true });
        store.save("bot/BTCUSDT", v.clone()).await.unwrap();
        assert_eq!(store.load("bot/BTCUSDT").await.unwrap(), Some(v));
        cleanup(&path);
    }

    #[tokio::test]
    async fn state_survives_reopen() {
        let path = temp_path();
        {
            let store = JsonFileStore::open(&path).await.unwrap();
            store
                .save("bot/ETHUSDT", serde_json::json!({ "trades": 3 }))
                .await
                .unwrap();
            // Drop the store — the write-through already persisted it.
        }
        // Re-open the same path: the snapshot must be there.
        let reopened = JsonFileStore::open(&path).await.unwrap();
        assert_eq!(
            reopened.load("bot/ETHUSDT").await.unwrap(),
            Some(serde_json::json!({ "trades": 3 }))
        );
        cleanup(&path);
    }

    #[tokio::test]
    async fn save_overwrites_and_persists_latest() {
        let path = temp_path();
        let store = JsonFileStore::open(&path).await.unwrap();
        store.save("k", serde_json::json!(1)).await.unwrap();
        store.save("k", serde_json::json!(2)).await.unwrap();
        drop(store);
        let reopened = JsonFileStore::open(&path).await.unwrap();
        assert_eq!(
            reopened.load("k").await.unwrap(),
            Some(serde_json::json!(2))
        );
        cleanup(&path);
    }

    #[tokio::test]
    async fn corrupt_file_is_an_error_not_silent_loss() {
        let path = temp_path();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"{ this is not valid json").unwrap();
        let err = JsonFileStore::open(&path).await;
        assert!(err.is_err(), "a corrupt store must surface an error");
        cleanup(&path);
    }

    #[tokio::test]
    async fn creates_missing_parent_dirs() {
        let base = temp_path();
        let dir = base.parent().unwrap().to_path_buf();
        let nested = dir.join("a").join("b").join("c").join("risk.json");
        let store = JsonFileStore::open(&nested).await.unwrap();
        store.save("k", serde_json::json!("v")).await.unwrap();
        assert!(
            nested.exists(),
            "nested dirs + file should have been created"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
