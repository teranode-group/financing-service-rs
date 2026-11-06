use std::{collections::BTreeMap, env, time::Duration};

use opentelemetry::{global, trace::TracerProvider as _, KeyValue};
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    metrics::{PeriodicReader, SdkMeterProvider},
    propagation::TraceContextPropagator,
    resource::{Resource, SdkProvidedResourceDetector, TelemetryResourceDetector},
    trace::SdkTracerProvider,
};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::config::TelemetryConfig;

/// Keeps the tracer and meter providers alive, and flushes spans and metrics
/// on shutdown.
pub struct TelemetryGuard {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
}

impl TelemetryGuard {
    /// Whether a meter provider is installed, so the gauges' sampler is
    /// worth running (CS-608).
    pub fn metrics_enabled(&self) -> bool {
        self.meter_provider.is_some()
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = &self.tracer_provider {
            if let Err(error) = provider.shutdown() {
                eprintln!("OpenTelemetry shutdown error: {error}");
            }
        }
        if let Some(provider) = &self.meter_provider {
            if let Err(error) = provider.shutdown() {
                eprintln!("OpenTelemetry metrics shutdown error: {error}");
            }
        }
    }
}

/// Initialise tracing, with optional OpenTelemetry trace export and optional
/// OTLP metrics export (CS-608). Each has its own switch; returns a guard when
/// either is on.
pub fn init(config: &TelemetryConfig) -> Result<Option<TelemetryGuard>, String> {
    let filter = log_filter(env::var("RUST_LOG").ok().as_deref())?;

    let tracer_provider = if config.is_enabled() {
        global::set_text_map_propagator(TraceContextPropagator::new());

        let exporter = SpanExporter::builder()
            .with_tonic()
            .with_endpoint(config.effective_otlp_endpoint())
            .build()
            .map_err(|e| format!("failed to create OTLP trace exporter: {e}"))?;

        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource(config))
            .build();

        global::set_tracer_provider(provider.clone());
        Some(provider)
    } else {
        None
    };
    let otel_layer = tracer_provider
        .as_ref()
        .map(|provider| OpenTelemetryLayer::new(provider.tracer("financing-service-rs")));

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .with(otel_layer)
        .try_init()
        .map_err(|e| format!("failed to initialise tracing subscriber: {e}"))?;

    bridge_log_crate()?;
    for warning in config.ignored_resource_attribute_warnings() {
        log::warn!("{warning}");
    }
    if tracer_provider.is_some() {
        log::info!(
            "OpenTelemetry enabled (service.name={}, otlp.endpoint={})",
            config.effective_service_name(),
            redact_endpoint(&config.effective_otlp_endpoint())
        );
    }

    // After the subscriber, so the startup line is logged. Installed
    // globally: `global::meter` hands out the no-op meter until it is, which
    // is what makes every instrument free with export off.
    let meter_provider = build_meter_provider(config)?;
    if let Some(provider) = &meter_provider {
        global::set_meter_provider(provider.clone());
    }
    log::info!("{}", metrics_startup_line(config));

    if tracer_provider.is_none() && meter_provider.is_none() {
        return Ok(None);
    }
    Ok(Some(TelemetryGuard {
        tracer_provider,
        meter_provider,
    }))
}

/// The resource on traces and metrics alike: the SDK's own attributes, then
/// [`TelemetryConfig::effective_resource_attributes`], then `service.name` and
/// `service.version`, each later one winning a key.
///
/// Not `Resource::builder()`: its `EnvResourceDetector` adds
/// `OTEL_RESOURCE_ATTRIBUTES` first, so the table added after it would beat
/// the variable (CS-674). The variable is merged into the table instead.
fn resource(config: &TelemetryConfig) -> Resource {
    let env_attributes = env::var(OTEL_RESOURCE_ATTRIBUTES).ok();
    Resource::builder_empty()
        .with_detectors(&[
            Box::new(SdkProvidedResourceDetector),
            Box::new(TelemetryResourceDetector),
        ])
        .with_attributes(
            config
                .effective_resource_attributes(env_attributes.as_deref())
                .into_iter()
                .map(|(key, (value, _))| KeyValue::new(key, value)),
        )
        .with_attributes(vec![
            KeyValue::new(SERVICE_NAME, config.effective_service_name()),
            KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
        ])
        .build()
}

const SERVICE_NAME: &str = "service.name";
const SERVICE_VERSION: &str = "service.version";
const OTEL_RESOURCE_ATTRIBUTES: &str = "OTEL_RESOURCE_ATTRIBUTES";
const OTEL_METRIC_EXPORT_INTERVAL: &str = "OTEL_METRIC_EXPORT_INTERVAL";

/// The SDK's metrics export interval when nothing sets one.
pub const DEFAULT_METRIC_EXPORT_INTERVAL: Duration = Duration::from_secs(60);

/// Where a resource attribute or the export interval came from, for the
/// startup line.
const FROM_TABLE_ATTRIBUTES: &str = "telemetry.resource_attributes";
const FROM_TABLE_INTERVAL: &str = "telemetry.export_interval_sec";
const FROM_DEFAULT: &str = "default";

/// The OTLP metrics pipeline, or `None` when metrics export is off -- in which
/// case nothing is installed and `global::meter` stays the no-op meter.
///
/// The `PeriodicReader` exports at
/// [`TelemetryConfig::metric_export_interval`].
pub fn build_meter_provider(config: &TelemetryConfig) -> Result<Option<SdkMeterProvider>, String> {
    if !config.metrics_enabled() {
        return Ok(None);
    }
    let exporter = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(config.effective_metrics_otlp_endpoint())
        .build()
        .map_err(|e| format!("failed to create OTLP metric exporter: {e}"))?;
    // `with_interval` overrides `OTEL_METRIC_EXPORT_INTERVAL`, which the
    // builder has read; it is passed the variable's own value when that is
    // set, and ignores a zero.
    let (interval, _) = config.metric_export_interval();
    let reader = PeriodicReader::builder(exporter)
        .with_interval(interval)
        .build();
    Ok(Some(
        SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(resource(config))
            .build(),
    ))
}

/// The one startup line saying whether metrics are exported, where to and how
/// often, and which resource attributes they carry and from where -- keys
/// only, not values (CS-674).
pub fn metrics_startup_line(config: &TelemetryConfig) -> String {
    if config.metrics_enabled() {
        let (interval, interval_source) = config.metric_export_interval();
        let env_attributes = env::var(OTEL_RESOURCE_ATTRIBUTES).ok();
        let attributes = config
            .effective_resource_attributes(env_attributes.as_deref())
            .into_iter()
            .map(|(key, (_, source))| format!("{key} from {source}"))
            .collect::<Vec<_>>();
        format!(
            "OpenTelemetry metrics export on (service.name={}, otlp.endpoint={}, \
             export.interval={interval:?} from {interval_source}, resource.attributes=[{}])",
            config.effective_service_name(),
            redact_endpoint(&config.effective_metrics_otlp_endpoint()),
            attributes.join(", ")
        )
    } else {
        "OpenTelemetry metrics export off (enable with [telemetry] enabled = true or \
         OTEL_METRICS_EXPORTER=otlp)"
            .to_string()
    }
}

/// An endpoint fit for a log line: any `user:password@` and any query string
/// or fragment removed, so a credential carried in the URL is never logged.
fn redact_endpoint(endpoint: &str) -> String {
    let endpoint = endpoint
        .split(['?', '#'])
        .next()
        .unwrap_or_default()
        .to_string();
    let Some(scheme_end) = endpoint.find("://").map(|i| i + 3) else {
        return endpoint;
    };
    let authority_end = endpoint[scheme_end..]
        .find('/')
        .map_or(endpoint.len(), |i| scheme_end + i);
    match endpoint[scheme_end..authority_end].rfind('@') {
        Some(at) => format!(
            "{}{}",
            &endpoint[..scheme_end],
            &endpoint[scheme_end + at + 1..]
        ),
        None => endpoint,
    }
}

fn bridge_log_crate() -> Result<(), String> {
    // `try_init()` above already installs the log -> tracing bridge via
    // tracing-subscriber's `tracing-log` feature, so this call is a no-op that
    // reports "logger already initialised". Ignore that and keep the bridge for
    // the case where the feature is ever turned off.
    let _ = tracing_log::LogTracer::init();
    Ok(())
}

/// What is logged when `RUST_LOG` is unset or empty.
pub const DEFAULT_LOG_FILTER: &str = "info";

/// The log filter: `RUST_LOG` (e.g. `debug`, or
/// `info,financing_service=debug`), else [`DEFAULT_LOG_FILTER`], as Event and
/// mapi-lite read it (CS-624). One that does not parse stops startup, where
/// they fall back to `info`: a level an operator asked for and did not get
/// is not found until the line they needed is missing.
pub fn log_filter(rust_log: Option<&str>) -> Result<EnvFilter, String> {
    match rust_log.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(EnvFilter::new(DEFAULT_LOG_FILTER)),
        Some(value) => EnvFilter::try_new(value)
            .map_err(|e| format!("RUST_LOG {value:?} is not a log filter: {e}")),
    }
}

impl TelemetryConfig {
    pub fn is_enabled(&self) -> bool {
        if self.enabled {
            return true;
        }
        matches!(
            env::var("OTEL_TRACES_EXPORTER").as_deref(),
            Ok("otlp") | Ok("OTLP")
        )
    }

    /// Whether metrics are exported over OTLP (CS-608): `enabled = true`, or
    /// `OTEL_METRICS_EXPORTER=otlp`. Independent of `OTEL_TRACES_EXPORTER`.
    pub fn metrics_enabled(&self) -> bool {
        if self.enabled {
            return true;
        }
        matches!(
            env::var("OTEL_METRICS_EXPORTER").as_deref(),
            Ok("otlp") | Ok("OTLP")
        )
    }

    pub fn effective_service_name(&self) -> String {
        env::var("OTEL_SERVICE_NAME")
            .ok()
            .filter(|value| !value.is_empty())
            .or_else(|| self.service_name.clone().filter(|value| !value.is_empty()))
            .unwrap_or_else(|| "financing-service-rs".to_string())
    }

    pub fn effective_otlp_endpoint(&self) -> String {
        env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
            .ok()
            .filter(|value| !value.is_empty())
            .or_else(|| {
                env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
                    .ok()
                    .filter(|value| !value.is_empty())
            })
            .or_else(|| self.otlp_endpoint.clone().filter(|value| !value.is_empty()))
            .unwrap_or_else(|| "http://localhost:4317".to_string())
    }

    /// `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`, else as
    /// [`TelemetryConfig::effective_otlp_endpoint`] without the trace-specific
    /// variable.
    pub fn effective_metrics_otlp_endpoint(&self) -> String {
        env::var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
            .ok()
            .filter(|value| !value.is_empty())
            .or_else(|| {
                env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
                    .ok()
                    .filter(|value| !value.is_empty())
            })
            .or_else(|| self.otlp_endpoint.clone().filter(|value| !value.is_empty()))
            .unwrap_or_else(|| "http://localhost:4317".to_string())
    }

    /// The metrics export interval and where it came from:
    /// `OTEL_METRIC_EXPORT_INTERVAL` (milliseconds), else
    /// `telemetry.export_interval_sec`, else the SDK's 60s (CS-674), as
    /// mapi-lite resolves it.
    ///
    /// The variable counts only as a whole number of milliseconds above 0.
    /// The SDK's builder takes a `0` as given, and `with_interval` ignores a
    /// zero, so passing on the variable's 0 would leave the reader exporting
    /// in a tight loop; skipping it lets `with_interval` replace it.
    pub fn metric_export_interval(&self) -> (Duration, &'static str) {
        if let Some(millis) = env::var(OTEL_METRIC_EXPORT_INTERVAL)
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|&millis| millis > 0)
        {
            return (Duration::from_millis(millis), OTEL_METRIC_EXPORT_INTERVAL);
        }
        match self.export_interval_sec {
            Some(secs) => (Duration::from_secs(secs), FROM_TABLE_INTERVAL),
            None => (DEFAULT_METRIC_EXPORT_INTERVAL, FROM_DEFAULT),
        }
    }

    /// The resource attributes besides `service.name` and `service.version`,
    /// by key, with each value and where it came from:
    /// `telemetry.resource_attributes`, with a key `env_attributes` (the
    /// value of `OTEL_RESOURCE_ATTRIBUTES`) also sets taken from it (CS-674).
    /// `env_attributes` is parsed as the SDK's `EnvResourceDetector` parses
    /// it: `key=value` pairs split on `,`, each side trimmed.
    pub fn effective_resource_attributes(
        &self,
        env_attributes: Option<&str>,
    ) -> BTreeMap<String, (String, &'static str)> {
        let from_table = self
            .resource_attributes
            .iter()
            .map(|(key, value)| (key.clone(), (value.clone(), FROM_TABLE_ATTRIBUTES)));
        let from_env = env_attributes
            .unwrap_or_default()
            .split_terminator(',')
            .filter_map(|entry| entry.split_once('='))
            .map(|(key, value)| {
                (
                    key.trim().to_string(),
                    (value.trim().to_string(), OTEL_RESOURCE_ATTRIBUTES),
                )
            });
        from_table
            .chain(from_env)
            .filter(|(key, _)| key != SERVICE_NAME && key != SERVICE_VERSION)
            .collect()
    }

    /// A warning for each key of `telemetry.resource_attributes` that is
    /// ignored because the service sets it itself.
    pub fn ignored_resource_attribute_warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();
        if self.resource_attributes.contains_key(SERVICE_NAME) {
            warnings.push(format!(
                "telemetry.resource_attributes \"{SERVICE_NAME}\" is ignored: set it with \
                 telemetry.service_name or OTEL_SERVICE_NAME"
            ));
        }
        if self.resource_attributes.contains_key(SERVICE_VERSION) {
            warnings.push(format!(
                "telemetry.resource_attributes \"{SERVICE_VERSION}\" is ignored: it is always \
                 the service's version"
            ));
        }
        warnings
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.export_interval_sec == Some(0) {
            return Err(
                "telemetry.export_interval_sec must be at least 1 (seconds); leave it out for \
                 the SDK's 60s"
                    .to_string(),
            );
        }
        if (self.is_enabled() || self.metrics_enabled())
            && self.otlp_endpoint.as_deref().is_some_and(str::is_empty)
        {
            return Err("telemetry.otlp_endpoint cannot be empty when set".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_support::env_lock;

    #[test]
    fn sr_cfg_006_the_log_filter_is_rust_log_else_info() {
        assert_eq!(log_filter(None).unwrap().to_string(), "info");
        assert_eq!(log_filter(Some(" ")).unwrap().to_string(), "info");
        assert_eq!(log_filter(Some("debug")).unwrap().to_string(), "debug");
        assert!(log_filter(Some("info,financing_service=trace")).is_ok());
        let err = log_filter(Some("financing_service=loud")).unwrap_err();
        assert!(err.contains("RUST_LOG"), "{err}");
    }

    #[test]
    fn telemetry_defaults_to_disabled_without_otel_exporter_env() {
        let _env = env_lock();
        unsafe { env::remove_var("OTEL_TRACES_EXPORTER") };
        let config = TelemetryConfig::default();
        assert!(!config.is_enabled());
    }

    #[test]
    fn otel_traces_exporter_env_enables_telemetry() {
        let _env = env_lock();
        unsafe { env::set_var("OTEL_TRACES_EXPORTER", "otlp") };
        let config = TelemetryConfig::default();
        assert!(config.is_enabled());
        unsafe { env::remove_var("OTEL_TRACES_EXPORTER") };
    }

    #[test]
    fn effective_service_name_prefers_otel_service_name_env() {
        let _env = env_lock();
        unsafe { env::set_var("OTEL_SERVICE_NAME", "from-env") };
        let config = TelemetryConfig {
            service_name: Some("from-config".to_string()),
            ..Default::default()
        };
        assert_eq!(config.effective_service_name(), "from-env");
        unsafe { env::remove_var("OTEL_SERVICE_NAME") };
    }

    /// The default is the deployed name, as the image and compose service
    /// are named; `telemetry.service_name` overrides it.
    #[test]
    fn effective_service_name_defaults_to_the_deployed_name() {
        let _env = env_lock();
        unsafe { env::remove_var("OTEL_SERVICE_NAME") };
        assert_eq!(
            TelemetryConfig::default().effective_service_name(),
            "financing-service-rs"
        );
        let config = TelemetryConfig {
            service_name: Some("from-config".to_string()),
            ..Default::default()
        };
        assert_eq!(config.effective_service_name(), "from-config");
    }

    #[test]
    fn effective_otlp_endpoint_uses_standard_env_vars() {
        let _env = env_lock();
        unsafe { env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317") };
        let config = TelemetryConfig::default();
        assert_eq!(
            config.effective_otlp_endpoint(),
            "http://collector:4317".to_string()
        );
        unsafe { env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT") };
    }

    const METRICS_ENV: [&str; 5] = [
        "OTEL_METRICS_EXPORTER",
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        OTEL_METRIC_EXPORT_INTERVAL,
        OTEL_RESOURCE_ATTRIBUTES,
    ];

    fn clear_metrics_env() {
        for name in METRICS_ENV {
            unsafe { env::remove_var(name) };
        }
    }

    /// Off by default, and off means nothing is built: no provider, so
    /// `global::meter` stays the no-op meter, and the startup line says so.
    #[test]
    fn sr_tele_006_metrics_export_is_off_by_default_and_installs_nothing() {
        let _env = env_lock();
        clear_metrics_env();
        let config = TelemetryConfig::default();
        assert!(!config.metrics_enabled());
        assert!(build_meter_provider(&config).unwrap().is_none());
        let line = metrics_startup_line(&config);
        assert!(line.contains("metrics export off"), "{line}");
    }

    /// Metrics have their own switch: the traces one does not turn them on.
    #[test]
    fn sr_tele_006_otel_traces_exporter_alone_does_not_enable_metrics() {
        let _env = env_lock();
        clear_metrics_env();
        unsafe { env::set_var("OTEL_TRACES_EXPORTER", "otlp") };
        let config = TelemetryConfig::default();
        assert!(config.is_enabled());
        assert!(!config.metrics_enabled());
        unsafe { env::remove_var("OTEL_TRACES_EXPORTER") };
    }

    #[test]
    fn sr_tele_006_otel_metrics_exporter_env_enables_metrics() {
        let _env = env_lock();
        clear_metrics_env();
        unsafe { env::set_var("OTEL_METRICS_EXPORTER", "otlp") };
        let config = TelemetryConfig::default();
        assert!(config.metrics_enabled());
        assert!(!config.is_enabled(), "traces keep their own switch");
        clear_metrics_env();
    }

    #[test]
    fn sr_tele_006_telemetry_enabled_turns_metrics_on() {
        let _env = env_lock();
        clear_metrics_env();
        let config = TelemetryConfig {
            enabled: true,
            ..Default::default()
        };
        assert!(config.metrics_enabled());
    }

    /// Built inside a runtime because the tonic exporter needs one, as it
    /// does in `main`.
    #[tokio::test]
    async fn sr_tele_006_metrics_export_on_builds_a_provider_and_says_where() {
        let _env = env_lock();
        clear_metrics_env();
        unsafe { env::set_var("OTEL_METRICS_EXPORTER", "otlp") };
        unsafe {
            env::set_var(
                "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
                "http://user:secret@collector:4317/?token=abc",
            )
        };
        let config = TelemetryConfig::default();
        let provider = build_meter_provider(&config).unwrap();
        assert!(provider.is_some());
        let line = metrics_startup_line(&config);
        clear_metrics_env();
        assert!(line.contains("metrics export on"), "{line}");
        assert!(
            line.contains("otlp.endpoint=http://collector:4317/"),
            "{line}"
        );
        assert!(
            !line.contains("secret") && !line.contains("token"),
            "{line}"
        );
        let _ = provider.unwrap().shutdown();
    }

    #[test]
    fn sr_tele_006_metrics_endpoint_prefers_the_metrics_specific_variable() {
        let _env = env_lock();
        clear_metrics_env();
        let config = TelemetryConfig {
            otlp_endpoint: Some("http://from-config:4317".to_string()),
            ..Default::default()
        };
        assert_eq!(
            config.effective_metrics_otlp_endpoint(),
            "http://from-config:4317"
        );
        unsafe { env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://generic:4317") };
        assert_eq!(
            config.effective_metrics_otlp_endpoint(),
            "http://generic:4317"
        );
        unsafe { env::set_var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://metrics:4317") };
        assert_eq!(
            config.effective_metrics_otlp_endpoint(),
            "http://metrics:4317"
        );
        clear_metrics_env();
        assert_eq!(
            TelemetryConfig::default().effective_metrics_otlp_endpoint(),
            "http://localhost:4317"
        );
    }

    fn with_table_attributes(pairs: &[(&str, &str)]) -> TelemetryConfig {
        TelemetryConfig {
            resource_attributes: pairs
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn sr_tele_013_export_interval_is_the_variable_else_the_table_else_60s() {
        let _env = env_lock();
        clear_metrics_env();
        assert_eq!(
            TelemetryConfig::default().metric_export_interval(),
            (Duration::from_secs(60), "default")
        );
        let config = TelemetryConfig {
            export_interval_sec: Some(10),
            ..Default::default()
        };
        assert_eq!(
            config.metric_export_interval(),
            (Duration::from_secs(10), "telemetry.export_interval_sec")
        );
        unsafe { env::set_var(OTEL_METRIC_EXPORT_INTERVAL, "5000") };
        assert_eq!(
            config.metric_export_interval(),
            (Duration::from_secs(5), "OTEL_METRIC_EXPORT_INTERVAL")
        );
        unsafe { env::set_var(OTEL_METRIC_EXPORT_INTERVAL, " 5000 ") };
        assert_eq!(
            config.metric_export_interval(),
            (Duration::from_secs(5), "OTEL_METRIC_EXPORT_INTERVAL")
        );
        // Neither one the SDK cannot read, nor 0, nor empty hides the table's.
        for value in ["5s", "0", ""] {
            unsafe { env::set_var(OTEL_METRIC_EXPORT_INTERVAL, value) };
            assert_eq!(
                config.metric_export_interval(),
                (Duration::from_secs(10), "telemetry.export_interval_sec"),
                "{value:?}"
            );
        }
        unsafe { env::set_var(OTEL_METRIC_EXPORT_INTERVAL, "0") };
        assert_eq!(
            TelemetryConfig::default().metric_export_interval(),
            (Duration::from_secs(60), "default")
        );
        clear_metrics_env();
    }

    #[test]
    fn sr_tele_013_otel_resource_attributes_override_the_table_key_by_key() {
        let config = with_table_attributes(&[
            ("deployment.environment.name", "local"),
            ("team", "payments"),
        ]);
        assert_eq!(
            config.effective_resource_attributes(None),
            BTreeMap::from([
                (
                    "deployment.environment.name".to_string(),
                    ("local".to_string(), FROM_TABLE_ATTRIBUTES)
                ),
                (
                    "team".to_string(),
                    ("payments".to_string(), FROM_TABLE_ATTRIBUTES)
                ),
            ])
        );
        assert_eq!(
            config.effective_resource_attributes(Some(
                " deployment.environment.name = dev ,region=eu,broken"
            )),
            BTreeMap::from([
                (
                    "deployment.environment.name".to_string(),
                    ("dev".to_string(), OTEL_RESOURCE_ATTRIBUTES)
                ),
                (
                    "region".to_string(),
                    ("eu".to_string(), OTEL_RESOURCE_ATTRIBUTES)
                ),
                (
                    "team".to_string(),
                    ("payments".to_string(), FROM_TABLE_ATTRIBUTES)
                ),
            ])
        );
    }

    #[test]
    fn sr_tele_013_service_name_and_version_in_the_table_are_ignored_with_a_warning() {
        let config = with_table_attributes(&[
            ("service.name", "other"),
            ("service.version", "9.9.9"),
            ("team", "payments"),
        ]);
        let attributes =
            config.effective_resource_attributes(Some("service.name=env,service.version=0"));
        assert_eq!(attributes.keys().collect::<Vec<_>>(), ["team"]);
        let warnings = config.ignored_resource_attribute_warnings();
        assert_eq!(warnings.len(), 2, "{warnings:?}");
        assert!(warnings[0].contains("\"service.name\" is ignored"));
        assert!(warnings[1].contains("\"service.version\" is ignored"));
        assert!(with_table_attributes(&[("team", "payments")])
            .ignored_resource_attribute_warnings()
            .is_empty());
    }

    /// The resource both providers carry: the table, overridden key by key
    /// by `OTEL_RESOURCE_ATTRIBUTES`, and `service.name` from its own chain.
    #[test]
    fn sr_tele_013_the_resource_carries_the_merged_attributes() {
        let _env = env_lock();
        clear_metrics_env();
        unsafe { env::remove_var("OTEL_SERVICE_NAME") };
        let config = with_table_attributes(&[
            ("deployment.environment.name", "local"),
            ("team", "payments"),
            ("service.name", "ignored"),
        ]);
        let get = |resource: &Resource, key: &'static str| {
            resource.get(&key.into()).map(|value| value.to_string())
        };

        let plain = resource(&TelemetryConfig::default());
        assert_eq!(
            get(&plain, "service.name").as_deref(),
            Some("financing-service-rs")
        );
        assert_eq!(
            get(&plain, "service.version").as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(get(&plain, "deployment.environment.name"), None);

        let from_table = resource(&config);
        assert_eq!(
            get(&from_table, "deployment.environment.name").as_deref(),
            Some("local")
        );
        assert_eq!(
            get(&from_table, "service.name").as_deref(),
            Some("financing-service-rs")
        );

        unsafe {
            env::set_var(
                OTEL_RESOURCE_ATTRIBUTES,
                "deployment.environment.name=dev,service.version=0",
            )
        };
        let overridden = resource(&config);
        clear_metrics_env();
        assert_eq!(
            get(&overridden, "deployment.environment.name").as_deref(),
            Some("dev")
        );
        assert_eq!(get(&overridden, "team").as_deref(), Some("payments"));
        assert_eq!(
            get(&overridden, "service.version").as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
    }

    /// The startup line names the interval, and each attribute key with its
    /// source, but no attribute value.
    #[test]
    fn sr_tele_013_the_startup_line_names_interval_and_attribute_sources() {
        let _env = env_lock();
        clear_metrics_env();
        unsafe { env::set_var(OTEL_RESOURCE_ATTRIBUTES, "region=eu-secret") };
        let config = TelemetryConfig {
            enabled: true,
            export_interval_sec: Some(10),
            ..with_table_attributes(&[("deployment.environment.name", "local-secret")])
        };
        let line = metrics_startup_line(&config);
        clear_metrics_env();
        assert!(
            line.contains("export.interval=10s from telemetry.export_interval_sec"),
            "{line}"
        );
        assert!(
            line.contains(
                "resource.attributes=[deployment.environment.name from \
                 telemetry.resource_attributes, region from OTEL_RESOURCE_ATTRIBUTES]"
            ),
            "{line}"
        );
        assert!(!line.contains("secret"), "{line}");
    }

    #[test]
    fn sr_cfg_007_an_export_interval_of_zero_is_refused() {
        let config = TelemetryConfig {
            export_interval_sec: Some(0),
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("telemetry.export_interval_sec"), "{err}");
    }

    #[test]
    fn redact_endpoint_drops_credentials_and_query() {
        assert_eq!(
            redact_endpoint("http://localhost:4317"),
            "http://localhost:4317"
        );
        assert_eq!(
            redact_endpoint("https://u:p@host:4317/v1?key=1#x"),
            "https://host:4317/v1"
        );
        assert_eq!(redact_endpoint("host:4317"), "host:4317");
    }
}
