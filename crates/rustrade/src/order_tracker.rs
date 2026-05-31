//! Order lifecycle tracking (0.2c).
//!
//! [`ExecutionService`](crate::execution::ExecutionService) returns an
//! exchange order id from `place_order` and then forgets it. For market
//! orders that's fine — they fill or reject immediately. But once a brain
//! can place resting limit / post-only orders (0.2b), an order can sit on
//! the book indefinitely: a stale limit ties up margin and may fill long
//! after the signal that produced it is meaningless.
//!
//! The [`OrderTracker`] records every resting order the framework places.
//! The [`OrderReaperService`] periodically:
//!
//! 1. **Reconciles** tracked state against the exchange's actual open
//!    orders (so fills/cancels that happened out-of-band drop out of the
//!    tracker), and
//! 2. **Ages out** any tracked order older than a configured TTL by calling
//!    [`ExchangeClient::cancel_order`].
//!
//! Both require [`Capability::OrderTracking`](rustrade_core::Capability);
//! without it the reaper is never spawned (see `Bot::with_order_tracking`).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustrade_core::{ExchangeClient, MetricsSink, Order, OrderKind, Symbol};
use rustrade_supervisor::{RestartPolicy, TradingService};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

/// One order the framework is tracking on the book.
#[derive(Debug, Clone, PartialEq)]
pub struct TrackedOrder {
    /// Exchange-assigned order id.
    pub order_id: String,
    /// Symbol the order is for.
    pub symbol: Symbol,
    /// When the framework first recorded the order (its own clock).
    pub placed_at: DateTime<Utc>,
}

/// Shared, cheaply-cloneable record of resting orders the framework placed.
///
/// Keyed by exchange order id. Only orders that can *rest* are tracked —
/// market orders fill or reject immediately and are never recorded.
#[derive(Clone, Default)]
pub struct OrderTracker {
    inner: Arc<RwLock<HashMap<String, TrackedOrder>>>,
}

impl OrderTracker {
    /// Create an empty tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a freshly-placed order — unless it's a market order (those
    /// never rest, so tracking them would only create churn). Called by the
    /// [`ExecutionService`](crate::execution::ExecutionService) right after
    /// a successful `place_order`.
    pub(crate) async fn record(&self, order_id: String, order: &Order) {
        if matches!(order.kind, OrderKind::Market) {
            return;
        }
        self.inner.write().await.insert(
            order_id.clone(),
            TrackedOrder {
                order_id,
                symbol: order.symbol.clone(),
                placed_at: Utc::now(),
            },
        );
    }

    /// Stop tracking an order (filled, cancelled, or reconciled away).
    pub(crate) async fn forget(&self, order_id: &str) {
        self.inner.write().await.remove(order_id);
    }

    /// Number of orders currently tracked.
    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }

    /// `true` when no orders are tracked.
    pub async fn is_empty(&self) -> bool {
        self.inner.read().await.is_empty()
    }

    /// Snapshot of all tracked orders (for `BotHealth` / debugging).
    pub async fn snapshot(&self) -> Vec<TrackedOrder> {
        self.inner.read().await.values().cloned().collect()
    }
}

/// Supervised service that reconciles tracked orders against the exchange
/// and cancels any that outlive the configured TTL.
///
/// Spawned by `Bot::run_until_shutdown` only when order tracking is wired
/// **and** the adapter advertises
/// [`Capability::OrderTracking`](rustrade_core::Capability).
pub struct OrderReaperService {
    exchange: Arc<dyn ExchangeClient>,
    tracker: OrderTracker,
    symbols: Vec<Symbol>,
    ttl: Duration,
    poll_cadence: Duration,
    metrics: Arc<dyn MetricsSink>,
    cancelled: AtomicU64,
    reconciled: AtomicU64,
    sweeps: AtomicU64,
}

impl OrderReaperService {
    pub(crate) fn new(
        exchange: Arc<dyn ExchangeClient>,
        tracker: OrderTracker,
        symbols: Vec<Symbol>,
        ttl: Duration,
        poll_cadence: Duration,
        metrics: Arc<dyn MetricsSink>,
    ) -> Self {
        Self {
            exchange,
            tracker,
            symbols,
            ttl,
            poll_cadence,
            metrics,
            cancelled: AtomicU64::new(0),
            reconciled: AtomicU64::new(0),
            sweeps: AtomicU64::new(0),
        }
    }

    /// Total resting orders cancelled for exceeding the TTL.
    pub fn cancelled(&self) -> u64 {
        self.cancelled.load(Ordering::Relaxed)
    }
    /// Total tracked orders dropped because the exchange no longer lists
    /// them (filled or cancelled out-of-band).
    pub fn reconciled(&self) -> u64 {
        self.reconciled.load(Ordering::Relaxed)
    }
    /// Total reconcile/reap sweeps performed.
    pub fn sweeps(&self) -> u64 {
        self.sweeps.load(Ordering::Relaxed)
    }

    /// One reconcile + reap pass across all symbols. Factored out so it can
    /// be unit-tested without the service loop.
    pub(crate) async fn sweep_once(&self) {
        self.sweeps.fetch_add(1, Ordering::Relaxed);
        let now = Utc::now();

        for symbol in &self.symbols {
            let open = match self.exchange.get_open_orders(symbol).await {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!(symbol = %symbol, error = %e, "get_open_orders failed; skipping sweep for symbol");
                    continue;
                }
            };
            // Index the exchange's live orders by id for O(1) membership.
            let live: HashMap<&str, &rustrade_core::OpenOrder> =
                open.iter().map(|o| (o.order_id.as_str(), o)).collect();

            // Snapshot tracked orders for this symbol, then decide per order
            // without holding the lock across the await for cancellation.
            let tracked: Vec<TrackedOrder> = self
                .tracker
                .snapshot()
                .await
                .into_iter()
                .filter(|t| &t.symbol == symbol)
                .collect();

            for t in tracked {
                match live.get(t.order_id.as_str()) {
                    None => {
                        // Exchange no longer lists it → filled or cancelled
                        // elsewhere. Drop from the tracker.
                        self.tracker.forget(&t.order_id).await;
                        self.reconciled.fetch_add(1, Ordering::Relaxed);
                        tracing::debug!(symbol = %symbol, order_id = %t.order_id, "reconciled away (no longer open)");
                    }
                    Some(oo) => {
                        // Still resting. Age it out if past TTL. Prefer the
                        // exchange's created_at; fall back to when we first
                        // saw it.
                        let age_from = oo.created_at.unwrap_or(t.placed_at);
                        let age = now.signed_duration_since(age_from);
                        if age.num_milliseconds().max(0) as u128 >= self.ttl.as_millis() {
                            match self.exchange.cancel_order(symbol, &t.order_id).await {
                                Ok(_) => {
                                    self.tracker.forget(&t.order_id).await;
                                    self.cancelled.fetch_add(1, Ordering::Relaxed);
                                    self.metrics.counter(
                                        "rustrade_orders_cancelled_ttl_total",
                                        &[("symbol", symbol.as_str())],
                                        1,
                                    );
                                    tracing::info!(symbol = %symbol, order_id = %t.order_id, ttl_secs = self.ttl.as_secs(), "cancelled stale resting order (TTL)");
                                }
                                Err(e) => {
                                    tracing::warn!(symbol = %symbol, order_id = %t.order_id, error = %e, "TTL cancel failed; will retry next sweep")
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl TradingService for OrderReaperService {
    fn name(&self) -> &str {
        "order-reaper"
    }

    fn restart_policy(&self) -> RestartPolicy {
        RestartPolicy::OnFailure
    }

    async fn run(&self, cancel: CancellationToken) -> anyhow::Result<()> {
        tracing::info!(
            ttl_secs = self.ttl.as_secs(),
            cadence_secs = self.poll_cadence.as_secs(),
            symbols = self.symbols.len(),
            "order-reaper starting"
        );
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!(
                        sweeps = self.sweeps(),
                        cancelled = self.cancelled(),
                        reconciled = self.reconciled(),
                        "order-reaper shutting down"
                    );
                    return Ok(());
                }
                _ = tokio::time::sleep(self.poll_cadence) => {
                    self.sweep_once().await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustrade_core::{Capability, NoopSink, Position, Price, Result, Side, Volume};

    fn limit(symbol: &str) -> Order {
        Order::limit(symbol, Side::Buy, Volume(1.0), Price(100.0))
    }

    #[tokio::test]
    async fn tracker_ignores_market_orders() {
        let t = OrderTracker::new();
        t.record(
            "m1".into(),
            &Order::market("BTCUSDT", Side::Buy, Volume(1.0)),
        )
        .await;
        assert!(t.is_empty().await, "market orders must not be tracked");

        t.record("l1".into(), &limit("BTCUSDT")).await;
        assert_eq!(t.len().await, 1);
    }

    #[tokio::test]
    async fn tracker_forget_removes() {
        let t = OrderTracker::new();
        t.record("l1".into(), &limit("BTCUSDT")).await;
        t.forget("l1").await;
        assert!(t.is_empty().await);
    }

    // Mock exchange: configurable open orders + records cancel calls.
    struct MockEx {
        open: std::sync::Mutex<Vec<rustrade_core::OpenOrder>>,
        cancels: std::sync::Mutex<Vec<String>>,
    }
    impl MockEx {
        fn new(open: Vec<rustrade_core::OpenOrder>) -> Arc<Self> {
            Arc::new(Self {
                open: std::sync::Mutex::new(open),
                cancels: std::sync::Mutex::new(Vec::new()),
            })
        }
    }
    #[async_trait]
    impl ExchangeClient for MockEx {
        fn name(&self) -> &str {
            "mock"
        }
        async fn place_order(&self, _o: &Order) -> Result<String> {
            Ok("x".into())
        }
        async fn cancel_all(&self, _s: &Symbol) -> Result<usize> {
            Ok(0)
        }
        async fn close_position(&self, _s: &Symbol, _p: &Position) -> Result<String> {
            Ok("c".into())
        }
        async fn get_position(&self, _s: &Symbol) -> Result<Position> {
            Ok(Position::FLAT)
        }
        async fn get_balance(&self, _c: &str) -> Result<f64> {
            Ok(0.0)
        }
        fn supports(&self, c: Capability) -> bool {
            matches!(c, Capability::OrderTracking)
        }
        async fn get_open_orders(&self, _s: &Symbol) -> Result<Vec<rustrade_core::OpenOrder>> {
            Ok(self.open.lock().unwrap().clone())
        }
        async fn cancel_order(&self, _s: &Symbol, order_id: &str) -> Result<bool> {
            self.cancels.lock().unwrap().push(order_id.to_string());
            Ok(true)
        }
    }

    fn open_order(id: &str, created_at: Option<DateTime<Utc>>) -> rustrade_core::OpenOrder {
        rustrade_core::OpenOrder {
            order_id: id.into(),
            client_id: None,
            symbol: Symbol::from("BTCUSDT"),
            side: Side::Buy,
            kind: OrderKind::Limit,
            limit_price: Some(Price(100.0)),
            size: Volume(1.0),
            filled: Volume(0.0),
            status: rustrade_core::OrderStatus::Open,
            created_at,
        }
    }

    fn reaper(ex: Arc<MockEx>, tracker: OrderTracker, ttl: Duration) -> OrderReaperService {
        OrderReaperService::new(
            ex,
            tracker,
            vec![Symbol::from("BTCUSDT")],
            ttl,
            Duration::from_secs(60),
            Arc::new(NoopSink),
        )
    }

    #[tokio::test]
    async fn sweep_reconciles_away_vanished_order() {
        // Tracked but the exchange reports no open orders → forgotten.
        let tracker = OrderTracker::new();
        tracker.record("gone".into(), &limit("BTCUSDT")).await;
        let ex = MockEx::new(vec![]);
        let svc = reaper(ex.clone(), tracker.clone(), Duration::from_secs(3600));

        svc.sweep_once().await;
        assert!(
            tracker.is_empty().await,
            "vanished order should be reconciled away"
        );
        assert_eq!(svc.reconciled(), 1);
        assert_eq!(svc.cancelled(), 0);
        assert!(ex.cancels.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn sweep_keeps_fresh_resting_order() {
        // Resting and well within TTL → left alone.
        let tracker = OrderTracker::new();
        tracker.record("fresh".into(), &limit("BTCUSDT")).await;
        let ex = MockEx::new(vec![open_order("fresh", Some(Utc::now()))]);
        let svc = reaper(ex.clone(), tracker.clone(), Duration::from_secs(3600));

        svc.sweep_once().await;
        assert_eq!(tracker.len().await, 1, "fresh order should remain tracked");
        assert_eq!(svc.cancelled(), 0);
        assert!(ex.cancels.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn sweep_cancels_order_past_ttl() {
        // Resting, created an hour ago, TTL 1s → cancelled + forgotten.
        let tracker = OrderTracker::new();
        tracker.record("stale".into(), &limit("BTCUSDT")).await;
        let created = Utc::now() - chrono::Duration::hours(1);
        let ex = MockEx::new(vec![open_order("stale", Some(created))]);
        let svc = reaper(ex.clone(), tracker.clone(), Duration::from_secs(1));

        svc.sweep_once().await;
        assert_eq!(svc.cancelled(), 1, "stale order should be cancelled");
        assert!(
            tracker.is_empty().await,
            "cancelled order should be forgotten"
        );
        assert_eq!(
            ex.cancels.lock().unwrap().as_slice(),
            &["stale".to_string()]
        );
    }
}
