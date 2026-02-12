use opentelemetry::trace::TracerProvider as _;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{LogExporter, SpanExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use std::path::{Path, PathBuf};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::writer::{BoxMakeWriter, MakeWriterExt};
use tracing_subscriber::layer::SubscriberExt;

const DEFAULT_SERVICE_NAME: &str = "liquidium-pipeline";

pub struct TelemetryConfig {
    pub service_name: String,
    pub traces_endpoint: Option<String>,
    pub logs_endpoint: Option<String>,
    pub auth_header: Option<String>,
    pub local_log_file: Option<PathBuf>,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            service_name: DEFAULT_SERVICE_NAME.to_string(),
            traces_endpoint: None,
            logs_endpoint: None,
            auth_header: None,
            local_log_file: None,
        }
    }
}

impl TelemetryConfig {
    pub fn with_service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = name.into();
        self
    }

    pub fn with_traces_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.traces_endpoint = Some(endpoint.into());
        self
    }

    pub fn with_logs_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.logs_endpoint = Some(endpoint.into());
        self
    }

    pub fn with_auth_header(mut self, header: impl Into<String>) -> Self {
        self.auth_header = Some(header.into());
        self
    }

    pub fn with_local_log_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.local_log_file = Some(path.into());
        self
    }
}

pub struct TelemetryGuard {
    tracer_provider: Option<SdkTracerProvider>,
    logger_provider: Option<SdkLoggerProvider>,
    _file_log_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.tracer_provider.take()
            && let Err(e) = provider.shutdown()
        {
            eprintln!("Failed to shutdown tracer provider: {e}");
        }

        if let Some(provider) = self.logger_provider.take()
            && let Err(e) = provider.shutdown()
        {
            eprintln!("Failed to shutdown logger provider: {e}");
        }
    }
}

pub fn init_telemetry(config: TelemetryConfig) -> Result<TelemetryGuard, Box<dyn std::error::Error>> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let (writer, file_log_guard) = if let Some(path) = &config.local_log_file {
        let file = open_log_file(path)?;
        let (file_writer, guard) = tracing_appender::non_blocking(file);
        (BoxMakeWriter::new(std::io::stdout.and(file_writer)), Some(guard))
    } else {
        (BoxMakeWriter::new(std::io::stdout), None)
    };

    let fmt_layer = fmt::layer()
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .with_writer(writer);

    let resource = Resource::builder()
        .with_service_name(config.service_name.clone())
        .build();

    let tracer_provider = if let Some(endpoint) = &config.traces_endpoint {
        let mut headers = std::collections::HashMap::new();
        if let Some(auth) = &config.auth_header {
            headers.insert("Authorization".to_string(), auth.clone());
        }

        let span_exporter = SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .with_headers(headers)
            .build()?;

        let tracer_provider = SdkTracerProvider::builder()
            .with_resource(resource.clone())
            .with_batch_exporter(span_exporter)
            .build();

        Some(tracer_provider)
    } else {
        None
    };

    let logger_provider = if let Some(endpoint) = &config.logs_endpoint {
        let mut headers = std::collections::HashMap::new();
        if let Some(auth) = &config.auth_header {
            headers.insert("Authorization".to_string(), auth.clone());
        }

        let log_exporter = LogExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .with_headers(headers)
            .build()?;

        let logger_provider = SdkLoggerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(log_exporter)
            .build();

        Some(logger_provider)
    } else {
        None
    };

    match (&tracer_provider, &logger_provider) {
        (Some(tp), Some(lp)) => {
            let tracer = tp.tracer(config.service_name);
            // Export spans to Tempo, but keep events as logs in Loki.
            let otel_trace_layer = OpenTelemetryLayer::new(tracer).with_filter(filter_fn(|meta| meta.is_span()));
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
            let otel_trace_layer = OpenTelemetryLayer::new(tracer).with_filter(filter_fn(|meta| meta.is_span()));

            let subscriber = tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .with(otel_trace_layer);

            tracing::subscriber::set_global_default(subscriber)?
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
        _file_log_guard: file_log_guard,
    })
}

pub fn init_telemetry_from_env() -> Result<TelemetryGuard, Box<dyn std::error::Error>> {
    init_telemetry_from_env_with_log_file(None)
}

pub fn init_telemetry_from_env_with_log_file(
    local_log_file: Option<&Path>,
) -> Result<TelemetryGuard, Box<dyn std::error::Error>> {
    let service_name = std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| DEFAULT_SERVICE_NAME.to_string());
    let traces_endpoint = std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").ok();
    let logs_endpoint = std::env::var("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT").ok();
    let auth_header = std::env::var("OTEL_EXPORTER_AUTH").ok();

    let config = TelemetryConfig {
        service_name,
        traces_endpoint,
        logs_endpoint,
        auth_header,
        local_log_file: local_log_file.map(|p| p.to_path_buf()),
    };

    init_telemetry(config)
}

fn open_log_file(path: &Path) -> Result<std::fs::File, Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }

    let file = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
    Ok(file)
}
