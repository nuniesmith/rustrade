//! The [`Bot`] entry point and its [`BotConfig`] builder.
//!
//! `Bot::new` validates configuration and constructs a supervised runtime.
//! `Bot::run_until_shutdown` starts the framework services and blocks
//! until shutdown is triggered (via signal or [`BotHandle::shutdown`]).

use std::sync::Arc;
use std::time::Duration;

use rustrade_core::{
    Brain, CandleSource, Capability, Error, ExchangeClient, FillSource, MarketDataBus,
    MarketSource, MetricsSink, NoopSink, Position, Result, SignalBus, StateStore, Symbol,
};
use rustrade_risk::{CircuitBreakerConfig, SessionPnlConfig, SizingConfig};
use rustrade_supervisor::{Supervisor, SupervisorConfig};
use tokio_util::sync::CancellationToken;

use crate::execution::{ExecutionContext, ExecutionService};
use crate::handle::BotHandle;
use crate::order_tracker::{OrderReaperService, OrderTracker};
use crate::risk_state::{
    PositionCache, RiskPersister, RiskStateMap, build_position_cache, build_risk_state,
};
use crate::services::{CandlePollerService, FillRoutingService, MarketFeedService};

const DEFAULT_MARKET_BUS_CAPACITY: usize = 1024;
const DEFAULT_SIGNAL_BUS_CAPACITY: usize = 256;

/// Risk-layer defaults applied to every symbol in the bot's config.
///
/// Per-symbol overrides are a Phase 2c concern — for now every configured
/// symbol gets the same `SessionPnl`, `CircuitBreaker`, and `PositionSizer`.
#[derive(Debug, Clone, Default)]
pub struct RiskConfig {
    /// Session PnL config applied to every configured symbol.
    pub session_pnl: SessionPnlConfig,
    /// Circuit-breaker config applied to every configured symbol.
    pub circuit_breaker: CircuitBreakerConfig,
    /// Position-sizing config used by the execution service.
    pub sizing: SizingConfig,
}

/// Configuration for a [`Bot`].
///
/// Construct via [`BotConfig::builder`]. The builder validates every
/// field on [`BotConfigBuilder::build`] and returns `Error::Config` on
/// any violation — the framework never panics on bad config. `Bot::new`
/// does a final brain-count check on top.
///
/// # Example
///
/// ```
/// use std::time::Duration;
/// use rustrade::BotConfig;
///
/// let config = BotConfig::builder()
///     .name("market-maker")
///     .symbols(["BTCUSDT", "ETHUSDT"])
///     .shutdown_timeout(Duration::from_secs(5))
///     .without_signal_handler() // tests + embedded use
///     .build()
///     .unwrap();
///
/// assert_eq!(config.name, "market-maker");
/// assert_eq!(config.symbols.len(), 2);
/// ```
#[derive(Debug, Clone)]
pub struct BotConfig {
    /// Human-readable name used in logs, tracing spans, and supervisor
    /// service identification.
    pub name: String,
    /// Symbols this bot trades. Every symbol gets a pre-seeded entry in
    /// the risk-state map and the position cache. Must be non-empty —
    /// the position cache and risk-state map would otherwise be empty,
    /// which is a silent footgun.
    pub symbols: Vec<Symbol>,
    /// Maximum time to wait for services to drain on shutdown. Must be
    /// `> 0`; the supervisor's drain logic needs a non-zero deadline.
    pub shutdown_timeout: Duration,
    /// Whether the supervisor installs its own Ctrl-C / SIGTERM handler.
    /// Disable when the host service drives shutdown via [`BotHandle::shutdown`].
    pub install_signal_handler: bool,
    /// Capacity of the in-process market-data broadcast bus. Backed by
    /// `tokio::sync::broadcast`, which has **drop-oldest** semantics: a
    /// slow subscriber that falls behind by more than `capacity` events
    /// sees `RecvError::Lagged(n)` and the oldest dropped events are
    /// gone. Size this to absorb the worst-case latency between
    /// publish and slowest subscriber's `recv`.
    pub market_bus_capacity: usize,
    /// Capacity of the in-process signal broadcast bus. Same drop-oldest
    /// semantics as `market_bus_capacity`. Typically smaller — signals
    /// are emitted ~once per non-Hold decision, far less frequent than
    /// market events.
    pub signal_bus_capacity: usize,
    /// On shutdown, attempt to close any open position for each symbol
    /// before exit, using `ExchangeClient::close_position`. Best-effort:
    /// failures are logged but do not propagate.
    pub close_positions_on_shutdown: bool,
    /// Risk-layer defaults applied to every configured symbol.
    pub risk: RiskConfig,
}

impl BotConfig {
    /// Begin building a config with sensible defaults.
    pub fn builder() -> BotConfigBuilder {
        BotConfigBuilder::default()
    }
}

/// Builder for [`BotConfig`].
#[derive(Debug, Clone, Default)]
pub struct BotConfigBuilder {
    name: Option<String>,
    symbols: Vec<Symbol>,
    shutdown_timeout: Option<Duration>,
    install_signal_handler: Option<bool>,
    market_bus_capacity: Option<usize>,
    signal_bus_capacity: Option<usize>,
    close_positions_on_shutdown: Option<bool>,
    risk: RiskConfig,
}

impl BotConfigBuilder {
    /// Human-readable bot name (logs, supervisor identification). Required.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Add a single symbol. Repeated calls accumulate.
    pub fn symbol(mut self, sym: impl Into<Symbol>) -> Self {
        self.symbols.push(sym.into());
        self
    }

    /// Add many symbols at once. Repeated calls accumulate.
    pub fn symbols<I, S>(mut self, syms: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Symbol>,
    {
        self.symbols.extend(syms.into_iter().map(Into::into));
        self
    }

    /// Maximum time to wait for services to drain on shutdown.
    pub fn shutdown_timeout(mut self, dur: Duration) -> Self {
        self.shutdown_timeout = Some(dur);
        self
    }

    /// Disable the supervisor's signal handler — host drives shutdown.
    pub fn without_signal_handler(mut self) -> Self {
        self.install_signal_handler = Some(false);
        self
    }

    /// Override the in-process market-data bus capacity (default 1024).
    pub fn market_bus_capacity(mut self, cap: usize) -> Self {
        self.market_bus_capacity = Some(cap);
        self
    }

    /// Override the in-process signal-bus capacity (default 256).
    pub fn signal_bus_capacity(mut self, cap: usize) -> Self {
        self.signal_bus_capacity = Some(cap);
        self
    }

    /// Enable best-effort `exchange.close_position` for non-flat positions
    /// after the supervisor drains.
    pub fn close_positions_on_shutdown(mut self, b: bool) -> Self {
        self.close_positions_on_shutdown = Some(b);
        self
    }

    /// Override the session-PnL config used for every symbol.
    pub fn session_pnl_config(mut self, cfg: SessionPnlConfig) -> Self {
        self.risk.session_pnl = cfg;
        self
    }

    /// Override the circuit-breaker config used for every symbol.
    pub fn circuit_breaker_config(mut self, cfg: CircuitBreakerConfig) -> Self {
        self.risk.circuit_breaker = cfg;
        self
    }

    /// Override the position-sizing config used for every symbol.
    pub fn sizing_config(mut self, cfg: SizingConfig) -> Self {
        self.risk.sizing = cfg;
        self
    }

    /// Validate and build. Returns `Error::Config` on any constraint
    /// violation — the framework never panics on bad config.
    pub fn build(self) -> Result<BotConfig> {
        let name = self
            .name
            .filter(|n| !n.trim().is_empty())
            .ok_or_else(|| Error::config("BotConfig.name is required and must not be empty"))?;

        if self.symbols.is_empty() {
            return Err(Error::config(
                "BotConfig.symbols must contain at least one Symbol — \
                 the position cache and risk-state map are pre-seeded per symbol",
            ));
        }

        let market_bus_capacity = self
            .market_bus_capacity
            .unwrap_or(DEFAULT_MARKET_BUS_CAPACITY);
        if market_bus_capacity == 0 {
            return Err(Error::config(
                "BotConfig.market_bus_capacity must be > 0 (broadcast channel cannot have 0 slots)",
            ));
        }

        let signal_bus_capacity = self
            .signal_bus_capacity
            .unwrap_or(DEFAULT_SIGNAL_BUS_CAPACITY);
        if signal_bus_capacity == 0 {
            return Err(Error::config(
                "BotConfig.signal_bus_capacity must be > 0 (broadcast channel cannot have 0 slots)",
            ));
        }

        let shutdown_timeout = self.shutdown_timeout.unwrap_or(Duration::from_secs(30));
        if shutdown_timeout.is_zero() {
            return Err(Error::config(
                "BotConfig.shutdown_timeout must be > 0 — drain needs a non-zero deadline",
            ));
        }

        if self.risk.session_pnl.loss_limit.is_nan() {
            return Err(Error::config(
                "BotConfig.risk.session_pnl.loss_limit must not be NaN",
            ));
        }
        if !self.risk.sizing.margin_per_trade.is_finite() || self.risk.sizing.margin_per_trade < 0.0
        {
            return Err(Error::config(
                "BotConfig.risk.sizing.margin_per_trade must be a finite non-negative number",
            ));
        }

        Ok(BotConfig {
            name,
            symbols: self.symbols,
            shutdown_timeout,
            install_signal_handler: self.install_signal_handler.unwrap_or(true),
            market_bus_capacity,
            signal_bus_capacity,
            close_positions_on_shutdown: self.close_positions_on_shutdown.unwrap_or(false),
            risk: self.risk,
        })
    }
}

/// The embedded trading bot.
///
/// Owns a [`Supervisor`], an [`ExchangeClient`], one or more [`Brain`]s,
/// and the in-process [`MarketDataBus`]. Created via [`Bot::new`]; run
/// via [`Bot::run_until_shutdown`]; observed and steered via the
/// [`BotHandle`] returned from [`Bot::handle`].
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use rustrade::{Bot, BotConfig};
/// # struct MyBrain;
/// # #[async_trait::async_trait]
/// # impl rustrade_core::Brain for MyBrain {
/// #     fn name(&self) -> &str { "x" }
/// #     async fn on_event(&self, _: &rustrade_core::MarketDataEvent, _: &rustrade_core::Position)
/// #         -> rustrade_core::Result<rustrade_core::Decision> {
/// #         Ok(rustrade_core::Decision::hold())
/// #     }
/// #     async fn health(&self) -> rustrade_core::BrainHealth { rustrade_core::BrainHealth::ok() }
/// # }
/// # async fn run(exchange: Arc<dyn rustrade_core::ExchangeClient>) -> anyhow::Result<()> {
/// let config = BotConfig::builder()
///     .name("my-bot")
///     .symbol("BTCUSDT")
///     .build()?;
///
/// let bot = Bot::new(config, exchange, vec![Arc::new(MyBrain) as Arc<dyn rustrade_core::Brain>])?;
/// let handle = bot.handle();
///
/// // Spawn the bot; ask it to shut down from elsewhere.
/// let task = tokio::spawn(async move { bot.run_until_shutdown().await });
/// handle.shutdown();
/// task.await??;
/// # Ok(())
/// # }
/// ```
pub struct Bot {
    config: BotConfig,
    supervisor: Arc<Supervisor>,
    exchange: Arc<dyn ExchangeClient>,
    brains: Arc<Vec<Arc<dyn Brain>>>,
    market_bus: MarketDataBus,
    signal_bus: SignalBus,
    positions: PositionCache,
    risk: RiskStateMap,
    metrics: Arc<dyn MetricsSink>,
    state_store: Option<Arc<dyn StateStore>>,
    persister_slot: crate::handle::PersisterSlot,
    handle: BotHandle,
    external_cancel: Option<CancellationToken>,
    market_source: Option<Arc<dyn MarketSource>>,
    fill_source: Option<Arc<dyn FillSource>>,
    candle_pollers: Vec<CandlePollerSpec>,
    order_tracker: OrderTracker,
    order_tracking: Option<OrderTrackingSpec>,
}

/// Settings captured by `Bot::with_order_tracking`.
struct OrderTrackingSpec {
    ttl: Duration,
    poll_cadence: Duration,
}

/// One registered `(symbol, interval, cadence)` for `Bot::with_candle_poller`.
struct CandlePollerSpec {
    source: Arc<dyn CandleSource>,
    symbol: Symbol,
    interval: Duration,
    poll_cadence: Duration,
    limit: usize,
}

impl Bot {
    /// Construct a `Bot`. Validates that at least one brain is provided.
    ///
    /// The exchange client and brain set are immutable for the bot's
    /// lifetime — to change them, build a new `Bot`.
    pub fn new(
        config: BotConfig,
        exchange: Arc<dyn ExchangeClient>,
        brains: Vec<Arc<dyn Brain>>,
    ) -> Result<Self> {
        if brains.is_empty() {
            return Err(Error::config(
                "Bot::new requires at least one Brain — empty brain list",
            ));
        }

        let supervisor = Arc::new(Supervisor::new(
            SupervisorConfig::default()
                .with_shutdown_timeout(config.shutdown_timeout)
                .with_default_backoff(Default::default())
                .pipe(|c| {
                    if config.install_signal_handler {
                        c
                    } else {
                        c.without_signal_handler()
                    }
                }),
        ));

        let market_bus = MarketDataBus::with_capacity(config.market_bus_capacity);
        let signal_bus = SignalBus::with_capacity(config.signal_bus_capacity);
        let positions = build_position_cache(&config.symbols);
        let risk = build_risk_state(
            &config.symbols,
            &config.risk.session_pnl,
            &config.risk.circuit_breaker,
        );

        let brains = Arc::new(brains);
        let persister_slot: crate::handle::PersisterSlot = Arc::new(std::sync::OnceLock::new());
        let order_tracker = OrderTracker::new();
        let handle = BotHandle::new(
            supervisor.clone(),
            brains.clone(),
            risk.clone(),
            positions.clone(),
            signal_bus.clone(),
            persister_slot.clone(),
            order_tracker.clone(),
        );

        Ok(Self {
            config,
            supervisor,
            exchange,
            brains,
            market_bus,
            signal_bus,
            positions,
            risk,
            metrics: Arc::new(NoopSink),
            state_store: None,
            persister_slot,
            order_tracker,
            handle,
            external_cancel: None,
            market_source: None,
            fill_source: None,
            candle_pollers: Vec::new(),
            order_tracking: None,
        })
    }

    /// Install a [`MetricsSink`]. The framework's services emit
    /// counters and histograms to this sink on every observable event;
    /// the default is [`NoopSink`], which discards everything.
    pub fn with_metrics(mut self, sink: Arc<dyn MetricsSink>) -> Self {
        self.metrics = sink;
        self
    }

    /// Install a [`StateStore`] so per-symbol risk state (session PnL and
    /// circuit breaker) survives restarts.
    ///
    /// Without a store, risk state is in-memory only: a crash mid-session
    /// resets the daily drawdown cap and the loss-streak breaker. With one
    /// wired, the bot:
    ///
    /// - **Restores** each symbol's snapshot on startup, then applies the
    ///   stale-snapshot policy — a session from an earlier UTC day rolls
    ///   over to fresh, and a breaker whose cooldown elapsed during
    ///   downtime auto-resets.
    /// - **Persists** after every realised trade (whether fed via
    ///   [`BotHandle::record_trade_outcome`](crate::BotHandle::record_trade_outcome)
    ///   or auto-routed by the `FillRoutingService`).
    /// - **Flushes** on graceful shutdown.
    ///
    /// Use [`rustrade_core::InMemoryStore`] for a non-durable default, or a
    /// disk-/database-backed implementation from a downstream crate for
    /// real durability. Snapshots are keyed by `(bot name, symbol)`, so
    /// distinct bots can share one backend without collision.
    pub fn with_state_store(mut self, store: Arc<dyn StateStore>) -> Self {
        self.state_store = Some(store);
        self
    }

    /// Enable resting-order lifecycle tracking.
    ///
    /// The [`ExecutionService`] records every resting order it places (limit
    /// / post-only / IOC / FOK — market orders never rest, so they're
    /// skipped), and a supervised [`OrderReaperService`] periodically:
    ///
    /// - **reconciles** the tracker against `exchange.get_open_orders` (so
    ///   orders filled or cancelled out-of-band stop being tracked), and
    /// - **cancels** any resting order older than `ttl` via
    ///   `exchange.cancel_order`.
    ///
    /// `poll_cadence` is how often a sweep runs. The reaper is only spawned
    /// if the adapter advertises [`Capability::OrderTracking`]; otherwise the
    /// call is a no-op (with a warning at startup), since the framework can't
    /// list or cancel orders without it. Live tracked orders are visible via
    /// [`BotHandle::tracked_orders`](crate::BotHandle::tracked_orders).
    pub fn with_order_tracking(mut self, ttl: Duration, poll_cadence: Duration) -> Self {
        self.order_tracking = Some(OrderTrackingSpec { ttl, poll_cadence });
        self
    }

    /// Register a [`CandleSource`] to be polled every `poll_cadence` for
    /// `(symbol, interval)`. Polled candles are deduplicated by
    /// timestamp and published to the bot's [`MarketDataBus`]. Repeated
    /// calls accumulate — one supervised service per registered tuple.
    pub fn with_candle_poller(
        mut self,
        source: Arc<dyn CandleSource>,
        symbol: impl Into<Symbol>,
        interval: Duration,
        poll_cadence: Duration,
        limit: usize,
    ) -> Self {
        self.candle_pollers.push(CandlePollerSpec {
            source,
            symbol: symbol.into(),
            interval,
            poll_cadence,
            limit,
        });
        self
    }

    /// Tie this bot's shutdown to an externally-owned cancellation token.
    ///
    /// When the external token is cancelled, the bot's supervisor token
    /// is cancelled too — equivalent to calling [`BotHandle::shutdown`]
    /// but without spawning a linker task in the host.
    ///
    /// The reverse is not true: cancelling the bot does not cancel the
    /// external token.
    pub fn with_external_cancel(mut self, token: CancellationToken) -> Self {
        self.external_cancel = Some(token);
        self
    }

    /// Attach a [`MarketSource`] to be driven by a supervised
    /// [`MarketFeedService`]. Source implementors are responsible for
    /// publishing to the bot's [`MarketDataBus`] (obtain via
    /// `bot.market_data_bus().clone()` before constructing the source).
    pub fn with_market_source(mut self, source: Arc<dyn MarketSource>) -> Self {
        self.market_source = Some(source);
        self
    }

    /// Attach a [`FillSource`] to be driven by a supervised
    /// [`FillRoutingService`]. Fills are routed to every brain via
    /// `Brain::on_fill` and the position cache is refreshed from the
    /// exchange after each one.
    pub fn with_fill_source(mut self, source: Arc<dyn FillSource>) -> Self {
        self.fill_source = Some(source);
        self
    }

    /// Cheap cloneable handle for host services. Can be obtained at any
    /// point — call before [`Self::run_until_shutdown`] so the host can
    /// drive shutdown while the bot is running.
    pub fn handle(&self) -> BotHandle {
        self.handle.clone()
    }

    /// Reference to the bot's configuration.
    pub fn config(&self) -> &BotConfig {
        &self.config
    }

    /// Borrow the in-process market-data bus. Host services and adapters
    /// publish here; the bot's framework services subscribe.
    pub fn market_data_bus(&self) -> &MarketDataBus {
        &self.market_bus
    }

    /// Borrow the in-process signal bus. The execution service publishes
    /// a [`Signal`](rustrade_core::Signal) to this bus on every
    /// non-`Hold` decision the brain emits; host services subscribe via
    /// [`BotHandle::subscribe_signals`].
    pub fn signal_bus(&self) -> &SignalBus {
        &self.signal_bus
    }

    /// Spawn the framework services and run until shutdown.
    ///
    /// Returns after all spawned services have drained (or the configured
    /// shutdown timeout elapses). Consumes `self` to make the
    /// "construct → run → exit" lifecycle explicit; persistent observation
    /// of the running bot is done via the [`BotHandle`] obtained earlier.
    ///
    /// # Runtime requirements
    ///
    /// - **Multi-thread tokio runtime.** The supervisor spawns each
    ///   service onto `tokio::spawn`. A current-thread runtime works
    ///   for small loads but loses the per-service parallelism the
    ///   framework is designed for. Use
    ///   `#[tokio::main(flavor = "multi_thread")]` or
    ///   `tokio::runtime::Builder::new_multi_thread()` in the host.
    /// - **`tokio::spawn` is used internally.** Anywhere the host
    ///   embeds this method, a tokio runtime context must be active.
    /// - **No nested runtimes.** `Bot::run_until_shutdown` is async; do
    ///   not call `block_on` on it from inside another runtime.
    ///
    /// # Resource expectations
    ///
    /// - **Memory per active symbol:** O(few hundred bytes) for the
    ///   position cache entry, `SymbolRisk` (a `SessionPnl` plus a
    ///   `CircuitBreaker` whose ring buffer is bounded by
    ///   `loss_limit`), plus the per-symbol slot in any host-owned
    ///   subscriber.
    /// - **Channel buffers:** `market_bus_capacity` + `signal_bus_capacity`
    ///   slots per bus, each slot holding a clone of `MarketDataEvent` /
    ///   `Signal`. Drop-oldest semantics — back-pressure is *not*
    ///   propagated to publishers.
    /// - **Expected shutdown time:** ≤ `shutdown_timeout`. A
    ///   well-behaved service responds to its cancel token in
    ///   milliseconds; the timeout is the worst-case bound, not the
    ///   typical case.
    /// - **Restart-after-crash latency:** bounded by
    ///   [`BackoffConfig`](rustrade_supervisor::BackoffConfig). Defaults:
    ///   100 ms base, 60 s cap, 10 retries within a 10-minute window
    ///   before the circuit breaker trips.
    pub async fn run_until_shutdown(self) -> anyhow::Result<()> {
        tracing::info!(
            bot = %self.config.name,
            brains = self.brains.len(),
            symbols = self.config.symbols.len(),
            exchange = %self.exchange.name(),
            "rustrade Bot starting"
        );

        // Best-effort position prefetch — failures don't block startup.
        self.prefetch_positions().await;

        // Restore persisted risk state (if a store is wired) before any
        // service runs, then publish the persister so the per-trade paths
        // can save through it.
        let persister = self
            .state_store
            .clone()
            .map(|store| RiskPersister::new(store, self.config.name.clone()));
        if let Some(p) = &persister {
            p.restore_into(&self.risk).await;
            // OnceLock: set is infallible the first (only) time per bot.
            let _ = self.persister_slot.set(p.clone());
        }

        // Order tracking: only active when wired AND the adapter can list +
        // cancel orders. Gate once here so both the execution service (which
        // records resting orders) and the reaper agree.
        let order_tracking_active =
            self.order_tracking.is_some() && self.exchange.supports(Capability::OrderTracking);
        if self.order_tracking.is_some() && !order_tracking_active {
            tracing::warn!(
                exchange = %self.exchange.name(),
                "order tracking requested but adapter lacks Capability::OrderTracking — \
                 resting orders will NOT be tracked or aged out"
            );
        }

        let sizing = Arc::new(self.config.risk.sizing.clone());
        let ctx = ExecutionContext {
            exchange: self.exchange.clone(),
            bus: self.market_bus.clone(),
            signals: self.signal_bus.clone(),
            positions: self.positions.clone(),
            risk: self.risk.clone(),
            sizing,
            order_tracker: order_tracking_active.then(|| self.order_tracker.clone()),
        };

        for brain in self.brains.iter() {
            let svc = ExecutionService::new(brain.clone(), ctx.clone());
            self.supervisor.spawn_service(Box::new(svc));
        }

        if order_tracking_active {
            // Safe: order_tracking_active implies order_tracking is Some.
            let spec = self.order_tracking.as_ref().unwrap();
            self.supervisor
                .spawn_service(Box::new(OrderReaperService::new(
                    self.exchange.clone(),
                    self.order_tracker.clone(),
                    self.config.symbols.clone(),
                    spec.ttl,
                    spec.poll_cadence,
                    self.metrics.clone(),
                )));
        }

        if let Some(source) = self.market_source.clone() {
            self.supervisor
                .spawn_service(Box::new(MarketFeedService::new(source)));
        }

        if let Some(source) = self.fill_source.clone() {
            self.supervisor
                .spawn_service(Box::new(FillRoutingService::new(
                    source,
                    self.brains.clone(),
                    self.exchange.clone(),
                    self.positions.clone(),
                    self.risk.clone(),
                    self.metrics.clone(),
                    persister.clone(),
                )));
        }

        for spec in &self.candle_pollers {
            self.supervisor
                .spawn_service(Box::new(CandlePollerService::new(
                    spec.source.clone(),
                    spec.symbol.clone(),
                    spec.interval,
                    spec.poll_cadence,
                    spec.limit,
                    self.market_bus.clone(),
                    self.metrics.clone(),
                )));
        }

        // External cancellation linker: when the host's token fires,
        // cancel the supervisor's root token. The reverse is not wired.
        if let Some(external) = self.external_cancel.clone() {
            let supervisor = self.supervisor.clone();
            tokio::spawn(async move {
                external.cancelled().await;
                tracing::info!("external cancellation received; triggering bot shutdown");
                supervisor.trigger_shutdown();
            });
        }

        let run_result = self.supervisor.run_until_shutdown().await;

        if self.config.close_positions_on_shutdown {
            self.close_open_positions().await;
        }

        // Persist final risk state + flush before exit so the next boot
        // restores an up-to-date snapshot.
        if let Some(p) = &persister {
            p.persist_all(&self.risk).await;
        }

        for brain in self.brains.iter() {
            let health = brain.health().await;
            tracing::info!(
                brain = %brain.name(),
                healthy = health.healthy,
                events = health.events_processed,
                non_hold = health.non_hold_decisions,
                "final brain health"
            );
        }

        tracing::info!(bot = %self.config.name, "rustrade Bot exited");
        run_result
    }

    async fn prefetch_positions(&self) {
        for symbol in &self.config.symbols {
            match self.exchange.get_position(symbol).await {
                Ok(pos) => {
                    self.positions.write().await.insert(symbol.clone(), pos);
                    tracing::debug!(
                        symbol = %symbol,
                        qty = pos.qty,
                        "prefetched position from exchange"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        symbol = %symbol,
                        error = %e,
                        "failed to prefetch position; cache defaults to FLAT"
                    );
                }
            }
        }
    }

    async fn close_open_positions(&self) {
        let snapshot: Vec<(Symbol, Position)> = {
            let map = self.positions.read().await;
            map.iter()
                .filter(|(_, p)| !p.is_flat())
                .map(|(s, p)| (s.clone(), *p))
                .collect()
        };

        if snapshot.is_empty() {
            tracing::info!("close_positions_on_shutdown: no open positions");
            return;
        }

        for (symbol, position) in snapshot {
            match self.exchange.close_position(&symbol, &position).await {
                Ok(order_id) => tracing::info!(
                    symbol = %symbol,
                    qty = position.qty,
                    order_id = %order_id,
                    "close_positions_on_shutdown: closed"
                ),
                Err(e) => tracing::error!(
                    symbol = %symbol,
                    qty = position.qty,
                    error = %e,
                    "close_positions_on_shutdown: failed (best-effort)"
                ),
            }
        }
    }
}

// Tiny `pipe` helper local to this module for builder ergonomics — keeps
// the `Bot::new` body readable when conditionally applying builder methods.
trait Pipe: Sized {
    fn pipe<F: FnOnce(Self) -> Self>(self, f: F) -> Self {
        f(self)
    }
}
impl<T> Pipe for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rustrade_core::{Fill, MarketDataEvent, Order, Position};

    struct NoopBrain;
    #[async_trait]
    impl Brain for NoopBrain {
        fn name(&self) -> &str {
            "noop"
        }
        async fn on_event(
            &self,
            _e: &MarketDataEvent,
            _p: &Position,
        ) -> Result<rustrade_core::Decision> {
            Ok(rustrade_core::Decision::hold())
        }
    }

    struct NoopExchange;
    #[async_trait]
    impl ExchangeClient for NoopExchange {
        fn name(&self) -> &str {
            "noop"
        }
        async fn place_order(&self, _o: &Order) -> Result<String> {
            Ok("noop-1".into())
        }
        async fn cancel_all(&self, _s: &Symbol) -> Result<usize> {
            Ok(0)
        }
        async fn close_position(&self, _s: &Symbol, _p: &Position) -> Result<String> {
            Ok("noop-close".into())
        }
        async fn get_position(&self, _s: &Symbol) -> Result<Position> {
            Ok(Position::FLAT)
        }
        async fn get_balance(&self, _c: &str) -> Result<f64> {
            Ok(0.0)
        }
    }

    fn cfg() -> BotConfig {
        BotConfig::builder()
            .name("test")
            .symbol("BTCUSDT")
            .without_signal_handler()
            .build()
            .unwrap()
    }

    #[test]
    fn builder_requires_name() {
        let err = BotConfig::builder().build().unwrap_err();
        assert!(matches!(err, Error::Config(_)), "got {err:?}");
    }

    #[test]
    fn builder_rejects_blank_name() {
        let err = BotConfig::builder().name("   ").build().unwrap_err();
        assert!(matches!(err, Error::Config(_)), "got {err:?}");
    }

    #[test]
    fn builder_rejects_zero_market_bus_capacity() {
        let err = BotConfig::builder()
            .name("x")
            .symbol("BTCUSDT")
            .market_bus_capacity(0)
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn builder_rejects_zero_signal_bus_capacity() {
        let err = BotConfig::builder()
            .name("x")
            .symbol("BTCUSDT")
            .signal_bus_capacity(0)
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn builder_rejects_empty_symbol_list() {
        let err = BotConfig::builder().name("x").build().unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn builder_rejects_zero_shutdown_timeout() {
        let err = BotConfig::builder()
            .name("x")
            .symbol("BTCUSDT")
            .shutdown_timeout(Duration::ZERO)
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn builder_rejects_nan_loss_limit() {
        let err = BotConfig::builder()
            .name("x")
            .symbol("BTCUSDT")
            .session_pnl_config(SessionPnlConfig {
                loss_limit: f64::NAN,
            })
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn builder_rejects_non_finite_margin() {
        let err = BotConfig::builder()
            .name("x")
            .symbol("BTCUSDT")
            .sizing_config(SizingConfig {
                margin_per_trade: f64::INFINITY,
                leverage: 1,
                max_contracts: 1,
            })
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn builder_accumulates_symbols() {
        let c = BotConfig::builder()
            .name("x")
            .symbol("A")
            .symbols(["B", "C"])
            .build()
            .unwrap();
        assert_eq!(c.symbols.len(), 3);
        assert_eq!(c.symbols[0], Symbol::new("A"));
        assert_eq!(c.symbols[2], Symbol::new("C"));
    }

    #[test]
    fn builder_accepts_risk_overrides() {
        let c = BotConfig::builder()
            .name("x")
            .symbol("BTCUSDT")
            .session_pnl_config(SessionPnlConfig { loss_limit: -123.0 })
            .sizing_config(SizingConfig {
                margin_per_trade: 250.0,
                leverage: 10,
                max_contracts: 5,
            })
            .build()
            .unwrap();
        assert_eq!(c.risk.session_pnl.loss_limit, -123.0);
        assert_eq!(c.risk.sizing.leverage, 10);
    }

    #[test]
    fn builder_has_separate_default_bus_capacities() {
        let c = BotConfig::builder()
            .name("x")
            .symbol("BTCUSDT")
            .build()
            .unwrap();
        assert_eq!(c.market_bus_capacity, 1024);
        assert_eq!(c.signal_bus_capacity, 256);
    }

    #[tokio::test]
    async fn bot_requires_at_least_one_brain() {
        match Bot::new(cfg(), Arc::new(NoopExchange), vec![]) {
            Err(Error::Config(_)) => {}
            other => panic!(
                "expected Error::Config for empty brain list, got {:?}",
                other.map(|_| "Ok(Bot)").map_err(|e| format!("Err({e})"))
            ),
        }
    }

    #[tokio::test]
    async fn bot_constructs_and_exposes_handle() {
        let bot = Bot::new(cfg(), Arc::new(NoopExchange), vec![Arc::new(NoopBrain)]).unwrap();
        let handle = bot.handle();
        assert!(!handle.is_shutting_down());
        assert_eq!(bot.config().name, "test");
        let h2 = handle.clone();
        assert!(!h2.is_shutting_down());
    }

    #[allow(dead_code)]
    fn _noop_fill_compiles(_: &Fill) {}
}
