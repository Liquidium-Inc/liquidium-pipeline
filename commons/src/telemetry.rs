use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{LogExporter, SpanExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::LoggerProvider;
use opentelemetry_sdk::trace::TracerProvider;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;

const DEFAULT_SERVICE_NAME: &str = "liquidium-pipeline";

pub struct TelemetryConfig {
    pub service_name: String,
    pub otlp_endpoint: Option<String>,
    pub loki_endpoint: Option<String>,
    pub auth_header: Option<String>,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            service_name: DEFAULT_SERVICE_NAME.to_string(),
            otlp_endpoint: None,
            loki_endpoint: None,
            auth_header: None,
        }
    }
}

impl TelemetryConfig {
    pub fn with_service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = name.into();
        self
    }

    pub fn with_otlp_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.otlp_endpoint = Some(endpoint.into());
        self
    }

    pub fn with_loki_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.loki_endpoint = Some(endpoint.into());
        self
    }

    pub fn with_auth_header(mut self, header: impl Into<String>) -> Self {
        self.auth_header = Some(header.into());
        self
    }
}

pub struct TelemetryGuard {
    tracer_provider: Option<TracerProvider>,
    logger_provider: Option<LoggerProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.tracer_provider.take() {
            if let Err(e) = provider.shutdown() {
                eprintln!("Failed to shutdown tracer provider: {e}");
            }
        }
        if let Some(provider) = self.logger_provider.take() {
            if let Err(e) = provider.shutdown() {
                eprintln!("Failed to shutdown logger provider: {e}");
            }
        }
    }
}

pub fn init_telemetry(config: TelemetryConfig) -> Result<TelemetryGuard, Box<dyn std::error::Error>> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = fmt::layer()
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false);

    let resource = Resource::new([KeyValue::new("service.name", config.service_name.clone())]);

    let tracer_provider = if let Some(endpoint) = &config.otlp_endpoint {
        let mut headers = std::collections::HashMap::new();
        if let Some(auth) = &config.auth_header {
            headers.insert("Authorization".to_string(), auth.clone());
        }

        let span_exporter = SpanExporter::builder()
            .with_http()
            .with_endpoint(format!("{}/v1/traces", endpoint))
            .with_headers(headers)
            .build()?;

        let tracer_provider = TracerProvider::builder()
            .with_resource(resource.clone())
            .with_batch_exporter(span_exporter, opentelemetry_sdk::runtime::Tokio)
            .build();

        Some(tracer_provider)
    } else {
        None
    };

    let logger_provider = if let Some(endpoint) = &config.loki_endpoint {
        let mut headers = std::collections::HashMap::new();
        if let Some(auth) = &config.auth_header {
            headers.insert("Authorization".to_string(), auth.clone());
        }

        let log_exporter = LogExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .with_headers(headers)
            .build()?;

        let logger_provider = LoggerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(log_exporter, opentelemetry_sdk::runtime::Tokio)
            .build();

        Some(logger_provider)
    } else {
        None
    };

    match (&tracer_provider, &logger_provider) {
        (Some(tp), Some(lp)) => {
            let tracer = tp.tracer(config.service_name);
            let otel_trace_layer = OpenTelemetryLayer::new(tracer);
            let otel_log_layer = OpenTelemetryTracingBridge::new(lp);

            let subscriber = tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .with(otel_trace_layer)
                .with(otel_log_layer);

            tracing::subscriber::set_global_default(subscriber)?;
        }
        (Some(tp), None) => {
            let tracer = tp.tracer(config.service_name);
            let otel_trace_layer = OpenTelemetryLayer::new(tracer);

            let subscriber = tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .with(otel_trace_layer);

            tracing::subscriber::set_global_default(subscriber)?;
        }
        (None, Some(lp)) => {
            let otel_log_layer = OpenTelemetryTracingBridge::new(lp);

            let subscriber = tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .with(otel_log_layer);

            tracing::subscriber::set_global_default(subscriber)?;
        }
        (None, None) => {
            let subscriber = tracing_subscriber::registry().with(env_filter).with(fmt_layer);

            tracing::subscriber::set_global_default(subscriber)?;
        }
    }

    let _ = tracing_log::LogTracer::builder().init();

    Ok(TelemetryGuard {
        tracer_provider,
        logger_provider,
    })
}

pub fn init_telemetry_from_env() -> Result<TelemetryGuard, Box<dyn std::error::Error>> {
    let service_name = std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| DEFAULT_SERVICE_NAME.to_string());
    let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok();
    let loki_endpoint = std::env::var("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT").ok();
    let auth_header = std::env::var("OTEL_EXPORTER_AUTH").ok();

    let config = TelemetryConfig {
        service_name,
        otlp_endpoint,
        loki_endpoint,
        auth_header,
    };

    init_telemetry(config)
}
