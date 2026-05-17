use prometheus::{Encoder, HistogramOpts, HistogramVec, IntCounterVec, Registry, TextEncoder};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, thiserror::Error)]
pub enum MetricsError {
    #[error("prometheus error: {0}")]
    Prometheus(#[from] prometheus::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("utf8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}

#[derive(Clone)]
pub struct QuantMetrics {
    registry: Registry,
    pub events_received_total: IntCounterVec,
    pub events_decoded_total: IntCounterVec,
    pub decode_errors_total: IntCounterVec,
    pub geyser_reconnects_total: IntCounterVec,
    pub event_lag_ms: HistogramVec,
}

impl QuantMetrics {
    pub fn new() -> Result<Self, MetricsError> {
        let registry = Registry::new_custom(Some("pump_launch_quant".to_owned()), None)?;

        let events_received_total = IntCounterVec::new(
            prometheus::Opts::new(
                "events_received_total",
                "received events by source and type",
            ),
            &["source", "event_type"],
        )?;
        let events_decoded_total = IntCounterVec::new(
            prometheus::Opts::new("events_decoded_total", "decoded events by decoder"),
            &["decoder"],
        )?;
        let decode_errors_total = IntCounterVec::new(
            prometheus::Opts::new("decode_errors_total", "decode errors by component"),
            &["component"],
        )?;
        let geyser_reconnects_total = IntCounterVec::new(
            prometheus::Opts::new("geyser_reconnects_total", "geyser reconnect count"),
            &["endpoint"],
        )?;
        let event_lag_ms = HistogramVec::new(
            HistogramOpts::new("event_lag_ms", "event lag in milliseconds"),
            &["source"],
        )?;

        registry.register(Box::new(events_received_total.clone()))?;
        registry.register(Box::new(events_decoded_total.clone()))?;
        registry.register(Box::new(decode_errors_total.clone()))?;
        registry.register(Box::new(geyser_reconnects_total.clone()))?;
        registry.register(Box::new(event_lag_ms.clone()))?;

        Ok(Self {
            registry,
            events_received_total,
            events_decoded_total,
            decode_errors_total,
            geyser_reconnects_total,
            event_lag_ms,
        })
    }

    pub fn gather_text(&self) -> Result<String, MetricsError> {
        let metric_families = self.registry.gather();
        let mut out = Vec::new();
        TextEncoder::new().encode(&metric_families, &mut out)?;
        Ok(String::from_utf8(out)?)
    }
}

pub fn init_tracing(service_name: &str, json: bool) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(format!(
            "{service_name}=info,metrics=info,common=info,idl=info,ingest_geyser=info"
        ))
    });

    if json {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().json())
            .init();
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer())
            .init();
    }
}

#[cfg(test)]
mod tests {
    use super::QuantMetrics;

    #[test]
    fn renders_prometheus_output() {
        let metrics = QuantMetrics::new().expect("metrics");
        metrics
            .events_received_total
            .with_label_values(&["geyser_processed", "transaction"])
            .inc();
        let rendered = metrics.gather_text().expect("rendered");
        assert!(rendered.contains("events_received_total"));
    }
}
