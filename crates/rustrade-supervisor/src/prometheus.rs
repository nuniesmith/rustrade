//! Local Prometheus collectors for supervisor metrics.
//!
//! Compiled only when the `prometheus` feature is enabled. The collectors
//! live in a process-local [`Registry`] (NOT the prometheus crate's default
//! global registry, so host services that already own a registry don't
//! collide with us). Hosts that want to expose these metrics call
//! [`registry()`] and either:
//!
//! - Serve `registry().gather()` directly from their `/metrics` handler, or
//! - Use [`prometheus::Registry::register`] on each individual collector to
//!   merge them into the host's own registry.
//!
//! The atomic counters in `SupervisorMetrics` remain the authoritative
//! in-process source of truth; this module just mirrors them into
//! Prometheus-shaped collectors.

use std::sync::OnceLock;

use prometheus::{Counter, Gauge, HistogramOpts, HistogramVec, Registry};

/// The supervisor's local prometheus collectors.
pub struct Collectors {
    /// The registry these collectors live in.
    pub registry: Registry,
    /// `rustrade_supervisor_restarts_total` — counter.
    pub restarts_total: Counter,
    /// `rustrade_supervisor_active_services` — gauge.
    pub active_services: Gauge,
    /// `rustrade_supervisor_spawned_total` — counter.
    pub spawned_total: Counter,
    /// `rustrade_supervisor_terminated_total` — counter.
    pub terminated_total: Counter,
    /// `rustrade_supervisor_circuit_breaker_trips_total` — counter.
    pub circuit_breaker_trips: Counter,
    /// `rustrade_supervisor_uptime_seconds` — histogram (labelled by service).
    pub uptime_seconds: HistogramVec,
}

static COLLECTORS: OnceLock<Collectors> = OnceLock::new();

/// Return the lazily-initialized supervisor collectors.
pub fn collectors() -> &'static Collectors {
    COLLECTORS.get_or_init(Collectors::new)
}

/// Return the local prometheus registry that owns the supervisor collectors.
///
/// Host services typically `gather()` this from their `/metrics` handler:
///
/// ```ignore
/// use prometheus::Encoder;
/// let registry = rustrade_supervisor::prometheus::registry();
/// let mut buf = Vec::new();
/// prometheus::TextEncoder::new()
///     .encode(&registry.gather(), &mut buf)
///     .unwrap();
/// ```
pub fn registry() -> &'static Registry {
    &collectors().registry
}

impl Collectors {
    fn new() -> Self {
        let registry = Registry::new();

        let restarts_total = Counter::new(
            "rustrade_supervisor_restarts_total",
            "Total number of service restarts across all services",
        )
        .expect("create restarts_total counter");

        let active_services = Gauge::new(
            "rustrade_supervisor_active_services",
            "Number of services currently in a non-terminal phase",
        )
        .expect("create active_services gauge");

        let spawned_total = Counter::new(
            "rustrade_supervisor_spawned_total",
            "Total number of services ever spawned (including restarts)",
        )
        .expect("create spawned_total counter");

        let terminated_total = Counter::new(
            "rustrade_supervisor_terminated_total",
            "Total number of services that have terminated",
        )
        .expect("create terminated_total counter");

        let circuit_breaker_trips = Counter::new(
            "rustrade_supervisor_circuit_breaker_trips_total",
            "Total number of circuit breaker trips across all services",
        )
        .expect("create circuit_breaker_trips counter");

        let uptime_seconds = HistogramVec::new(
            HistogramOpts::new(
                "rustrade_supervisor_uptime_seconds",
                "Cumulative running time of a service at termination, in seconds",
            ),
            &["service"],
        )
        .expect("create uptime_seconds histogram_vec");

        registry
            .register(Box::new(restarts_total.clone()))
            .expect("register restarts_total");
        registry
            .register(Box::new(active_services.clone()))
            .expect("register active_services");
        registry
            .register(Box::new(spawned_total.clone()))
            .expect("register spawned_total");
        registry
            .register(Box::new(terminated_total.clone()))
            .expect("register terminated_total");
        registry
            .register(Box::new(circuit_breaker_trips.clone()))
            .expect("register circuit_breaker_trips");
        registry
            .register(Box::new(uptime_seconds.clone()))
            .expect("register uptime_seconds");

        Self {
            registry,
            restarts_total,
            active_services,
            spawned_total,
            terminated_total,
            circuit_breaker_trips,
            uptime_seconds,
        }
    }
}
