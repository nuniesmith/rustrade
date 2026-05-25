//! The [`Bot`] entry point and its [`BotConfig`] builder.
//!
//! `Bot::new` validates configuration and constructs a supervised runtime.
//! `Bot::run_until_shutdown` starts the framework services and blocks
//! until shutdown is triggered (via signal or [`BotHandle::shutdown`]).

use std::sync::Arc;
use std::time::Duration;

use rustrade_core::{Brain, Error, ExchangeClient, MarketDataBus, Result, Symbol};
use rustrade_supervisor::{Supervisor, SupervisorConfig};

use crate::execution::ExecutionService;
use crate::handle::BotHandle;

const DEFAULT_MARKET_BUS_CAPACITY: usize = 1024;

/// Configuration for a [`Bot`].
///
/// Construct via [`BotConfig::builder`]. The builder validates required
/// fields on [`BotConfigBuilder::build`] — `Bot::new` does not double-check.
#[derive(Debug, Clone)]
pub struct BotConfig {
    /// Human-readable name used in logs, tracing spans, and supervisor
    /// service identification.
    pub name: String,
    /// Symbols this bot trades. Brains may filter further; framework
    /// services (candle pollers, etc.) use this as their primary input.
    pub symbols: Vec<Symbol>,
    /// Maximum time to wait for services to drain on shutdown.
    pub shutdown_timeout: Duration,
    /// Whether the supervisor installs its own Ctrl-C / SIGTERM handler.
    /// Disable when the host service drives shutdown via [`BotHandle::shutdown`].
    pub install_signal_handler: bool,
    /// Capacity of the in-process market-data broadcast bus.
    pub market_bus_capacity: usize,
    /// On shutdown, attempt to close any open position for each symbol
    /// before exit. **Not yet implemented in Phase 2a** — the field is
    /// reserved so callers can wire it now.
    pub close_positions_on_shutdown: bool,
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
    close_positions_on_shutdown: Option<bool>,
}

impl BotConfigBuilder {
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

    pub fn shutdown_timeout(mut self, dur: Duration) -> Self {
        self.shutdown_timeout = Some(dur);
        self
    }

    /// Disable the supervisor's signal handler — host drives shutdown.
    pub fn without_signal_handler(mut self) -> Self {
        self.install_signal_handler = Some(false);
        self
    }

    pub fn market_bus_capacity(mut self, cap: usize) -> Self {
        self.market_bus_capacity = Some(cap);
        self
    }

    pub fn close_positions_on_shutdown(mut self, b: bool) -> Self {
        self.close_positions_on_shutdown = Some(b);
        self
    }

    /// Validate and build. Returns `Error::Config` on any constraint
    /// violation.
    pub fn build(self) -> Result<BotConfig> {
        let name = self
            .name
            .filter(|n| !n.trim().is_empty())
            .ok_or_else(|| Error::config("BotConfig.name is required and must not be empty"))?;

        let capacity = self
            .market_bus_capacity
            .unwrap_or(DEFAULT_MARKET_BUS_CAPACITY);
        if capacity == 0 {
            return Err(Error::config(
                "BotConfig.market_bus_capacity must be > 0 (broadcast channel cannot have 0 slots)",
            ));
        }

        Ok(BotConfig {
            name,
            symbols: self.symbols,
            shutdown_timeout: self.shutdown_timeout.unwrap_or(Duration::from_secs(30)),
            install_signal_handler: self.install_signal_handler.unwrap_or(true),
            market_bus_capacity: capacity,
            close_positions_on_shutdown: self.close_positions_on_shutdown.unwrap_or(false),
        })
    }
}

/// The embedded trading bot.
///
/// Owns a [`Supervisor`], an [`ExchangeClient`], one or more [`Brain`]s,
/// and the in-process [`MarketDataBus`]. Created via [`Bot::new`]; run
/// via [`Bot::run_until_shutdown`]; observed and steered via the
/// [`BotHandle`] returned from [`Bot::handle`].
pub struct Bot {
    config: BotConfig,
    supervisor: Arc<Supervisor>,
    exchange: Arc<dyn ExchangeClient>,
    brains: Vec<Arc<dyn Brain>>,
    market_bus: MarketDataBus,
    handle: BotHandle,
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

        let brain_names: Vec<String> = brains.iter().map(|b| b.name().to_string()).collect();
        let handle = BotHandle::new(supervisor.clone(), brains.clone(), brain_names);

        Ok(Self {
            config,
            supervisor,
            exchange,
            brains,
            market_bus,
            handle,
        })
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

    /// Spawn the framework services and run until shutdown.
    ///
    /// Returns after all spawned services have drained (or the configured
    /// shutdown timeout elapses). Consumes `self` to make the
    /// "construct → run → exit" lifecycle explicit; persistent observation
    /// of the running bot is done via the [`BotHandle`] obtained earlier.
    pub async fn run_until_shutdown(self) -> anyhow::Result<()> {
        tracing::info!(
            bot = %self.config.name,
            brains = self.brains.len(),
            symbols = self.config.symbols.len(),
            exchange = %self.exchange.name(),
            "rustrade Bot starting"
        );

        for brain in &self.brains {
            let svc = ExecutionService::new(
                brain.clone(),
                self.exchange.clone(),
                self.market_bus.clone(),
            );
            self.supervisor.spawn_service(Box::new(svc));
        }

        let result = self.supervisor.run_until_shutdown().await;

        if self.config.close_positions_on_shutdown {
            tracing::warn!(
                "close_positions_on_shutdown is enabled but not yet implemented \
                 — Phase 2b will wire the close-on-stop hook"
            );
        }

        for brain in &self.brains {
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
        result
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
    fn builder_rejects_zero_bus_capacity() {
        let err = BotConfig::builder()
            .name("x")
            .market_bus_capacity(0)
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

    #[tokio::test]
    async fn bot_requires_at_least_one_brain() {
        // `Bot` doesn't impl Debug (holds trait objects without Debug),
        // so use a match instead of unwrap_err here.
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
        // Cloning the handle is cheap and yields the same logical handle.
        let h2 = handle.clone();
        assert!(!h2.is_shutting_down());
    }

    // Suppress unused-warnings for fields the noop test impl doesn't touch.
    #[allow(dead_code)]
    fn _noop_fill_compiles(_: &Fill) {}
}
