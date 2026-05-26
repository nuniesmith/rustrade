//! Pluggable metrics sink.
//!
//! The framework emits structured counters / gauges / histograms at
//! every observable event. Host services plug in a [`MetricsSink`]
//! implementation to ship them somewhere — Prometheus, StatsD, an
//! in-process accumulator, anything.
//!
//! The default [`NoopSink`] discards everything.
//!
//! # Why a trait, not a global registry
//!
//! Different host services own different metrics backends. The bot is
//! an embedded library; forcing a particular metrics layer would force
//! every downstream consumer onto that layer. A small trait is
//! cheap and lets the host stay in control.

/// Push-based metrics interface.
///
/// Implementors are called from the framework's hot path on every
/// observable event, so they must be fast and lock-light. Allocation
/// per call is acceptable — the framework doesn't claim a fixed
/// per-event budget — but blocking on I/O is not. Async sinks should
/// drop events into a channel and process them on a separate task.
///
/// `Send + Sync + 'static` so an `Arc<dyn MetricsSink>` lives across
/// supervised services.
///
/// # Example
///
/// A simple in-process counting sink, useful for tests and embedded
/// dashboards.
///
/// ```
/// use std::collections::HashMap;
/// use std::sync::Mutex;
/// use rustrade_core::MetricsSink;
///
/// #[derive(Default)]
/// struct CountingSink {
///     counters: Mutex<HashMap<String, u64>>,
/// }
///
/// impl MetricsSink for CountingSink {
///     fn counter(&self, name: &str, _labels: &[(&str, &str)], value: u64) {
///         *self.counters.lock().unwrap().entry(name.into()).or_insert(0) += value;
///     }
///     fn gauge(&self, _name: &str, _labels: &[(&str, &str)], _value: f64) {}
///     fn histogram(&self, _name: &str, _labels: &[(&str, &str)], _value: f64) {}
/// }
///
/// let sink = CountingSink::default();
/// sink.counter("rustrade_fills_routed_total", &[], 1);
/// sink.inc("rustrade_fills_routed_total");
/// assert_eq!(sink.counters.lock().unwrap()["rustrade_fills_routed_total"], 2);
/// ```
pub trait MetricsSink: Send + Sync + 'static {
    /// Increment a counter by `value`.
    fn counter(&self, name: &str, labels: &[(&str, &str)], value: u64);

    /// Record a gauge value.
    fn gauge(&self, name: &str, labels: &[(&str, &str)], value: f64);

    /// Observe a histogram sample.
    fn histogram(&self, name: &str, labels: &[(&str, &str)], value: f64);

    /// Convenience: increment by 1 with no labels.
    fn inc(&self, name: &str) {
        self.counter(name, &[], 1);
    }
}

/// Default sink that discards everything. Lower overhead than even a
/// `tracing::trace!` call.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSink;

impl MetricsSink for NoopSink {
    fn counter(&self, _name: &str, _labels: &[(&str, &str)], _value: u64) {}
    fn gauge(&self, _name: &str, _labels: &[(&str, &str)], _value: f64) {}
    fn histogram(&self, _name: &str, _labels: &[(&str, &str)], _value: f64) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct CountingSink {
        counters: AtomicU64,
        gauges: AtomicU64,
        histograms: AtomicU64,
    }

    impl MetricsSink for CountingSink {
        fn counter(&self, _name: &str, _labels: &[(&str, &str)], _value: u64) {
            self.counters.fetch_add(1, Ordering::SeqCst);
        }
        fn gauge(&self, _name: &str, _labels: &[(&str, &str)], _value: f64) {
            self.gauges.fetch_add(1, Ordering::SeqCst);
        }
        fn histogram(&self, _name: &str, _labels: &[(&str, &str)], _value: f64) {
            self.histograms.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn noop_sink_no_panics() {
        let s = NoopSink;
        s.counter("a", &[], 1);
        s.gauge("a", &[("x", "y")], 1.5);
        s.histogram("a", &[], 2.0);
        s.inc("a");
    }

    #[test]
    fn inc_default_delegates_to_counter() {
        let s: Arc<CountingSink> = Arc::new(CountingSink {
            counters: AtomicU64::new(0),
            gauges: AtomicU64::new(0),
            histograms: AtomicU64::new(0),
        });
        s.inc("x");
        assert_eq!(s.counters.load(Ordering::SeqCst), 1);
        assert_eq!(s.gauges.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn arc_dyn_sink_is_object_safe() {
        let s: Arc<dyn MetricsSink> = Arc::new(NoopSink);
        s.counter("a", &[], 1);
    }
}
