use std::{collections::BTreeMap, env, future::Future, time::Duration};

use opentelemetry::{global, trace::TracerProvider as _, KeyValue};
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    metrics::{PeriodicReader, SdkMeterProvider},
    propagation::TraceContextPropagator,
    resource::{Resource, SdkProvidedResourceDetector, TelemetryResourceDetector},
    trace::SdkTracerProvider,
};
use tracing::Instrument as _;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{
    filter::filter_fn, fmt::MakeWriter, layer::SubscriberExt, registry::LookupSpan,
    util::SubscriberInitExt, EnvFilter, Layer,
};

use crate::config::{TelemetryConfig, TelemetryExporter};

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

    let tracer_provider = if config.traces_enabled() {
        // W3C trace context both ways (CS-700): tracing-actix-web extracts an
        // incoming `traceparent` through it, so a request from Event joins
        // Event's trace, and uls-client injects one into every request to
        // mapi-lite.
        global::set_text_map_propagator(TraceContextPropagator::new());

        let exporter = SpanExporter::builder()
            .with_tonic()
            .with_endpoint(config.effective_otlp_endpoint())
            .build()
            .map_err(|e| format!("failed to create OTLP trace exporter: {e}"))?;

        // The sampler is the SDK's own, read from `OTEL_TRACES_SAMPLER` and
        // `OTEL_TRACES_SAMPLER_ARG` (default `parentbased_always_on`, every
        // trace): not set here, so a deployment chooses its rate.
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
        .with(fmt_layer(std::io::stdout))
        .with(otel_layer)
        .try_init()
        .map_err(|e| format!("failed to initialise tracing subscriber: {e}"))?;

    bridge_log_crate()?;
    for warning in config.ignored_resource_attribute_warnings() {
        log::warn!("{warning}");
    }
    log::info!("{}", traces_startup_line(config));
    if let Some(warning) = traces_sampler_warning(config) {
        log::warn!("{warning}");
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

/// The target of this service's own trace spans (CS-700): the chain call
/// spans, the background refresh's and the broadcast's. They exist for the
/// trace, and the log output does not see them (see [`fmt_layer`]).
///
/// Inside the crate's own target, so a `RUST_LOG` such as
/// `warn,financing_service=info` keeps them.
pub const SPAN_TARGET: &str = "financing_service::otel";

/// The log output, writing to `writer`.
///
/// It prints each enclosing span's name and fields before every log line.
/// The spans under [`SPAN_TARGET`] are filtered out of it, so a warning
/// logged during a broadcast reads as it did before CS-700 -- no
/// `mapi-rs broadcast{otel.name=...}:` prefix -- with trace export on or
/// off, and greps and alerts keyed on the message keep working.
pub fn fmt_layer<S, W>(writer: W) -> impl Layer<S>
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    W: for<'w> MakeWriter<'w> + Send + Sync + 'static,
{
    tracing_subscriber::fmt::layer()
        .with_writer(writer)
        .with_filter(filter_fn(|metadata| {
            !(metadata.is_span() && metadata.target() == SPAN_TARGET)
        }))
}

/// How a failed call is described on its span (CS-700 review): `error.type`,
/// a fixed name for the kind of failure, and the status description.
///
/// Neither is the error's own message, which can hold an address, a txid or
/// a URL that carries one, and none of those belong in a trace.
pub trait SpanError {
    /// `error.type`: short, lower case, from a fixed set.
    fn error_type(&self) -> &'static str;
    /// The span's status description: what the failure means, in a sentence.
    fn description(&self) -> &'static str;
}

/// `error.type` for a call dropped before it finished.
pub const CANCELLED: &str = "cancelled";

/// Run `future` in `span`, a span declaring `error.type` and
/// `otel.status_description` as empty fields, and mark the span an error when
/// the call fails: [`SpanError::error_type`] as `error.type`,
/// [`SpanError::description`] as the status description.
///
/// A call dropped before it finishes -- its caller disconnected, or an outer
/// deadline fired -- is marked an error too, as [`CANCELLED`]; otherwise its
/// span would end `Unset`, which reads as success.
pub async fn in_span<T, E: SpanError>(
    span: tracing::Span,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    let mut unfinished = Unfinished(Some(span.clone()));
    let result = future.instrument(span.clone()).await;
    unfinished.0 = None;
    if let Err(error) = &result {
        mark_error(&span, error.error_type(), error.description());
    }
    result
}

/// Mark `span` an error, with `error_type` and `description` as in
/// [`in_span`]. A disabled span is left alone.
pub fn mark_error(span: &tracing::Span, error_type: &'static str, description: &'static str) {
    span.record("error.type", error_type);
    // tracing-opentelemetry sets the status to error with this description.
    span.record("otel.status_description", description);
}

/// Marks its span cancelled when dropped still holding it: [`in_span`] takes
/// it back once the call has finished.
struct Unfinished(Option<tracing::Span>);

impl Drop for Unfinished {
    fn drop(&mut self) {
        if let Some(span) = self.0.take() {
            mark_error(
                &span,
                CANCELLED,
                "dropped before it finished: the caller went away or a deadline fired",
            );
        }
    }
}

/// `peer.service` on every span for a call to mapi-rs (mapi-lite): the
/// `service.name` mapi-rs is deployed under, so Tempo's service graph draws
/// the edge to its node. Never `mapi-lite` or `mapi_lite`, which name no
/// deployed service and would add a node of their own.
pub const MAPI_RS_PEER_SERVICE: &str = "mapi-rs";

/// A client span for a call this service makes (CS-700): the chain calls and
/// the broadcast to mapi-rs. Run the call with [`in_span`].
///
/// The one place the shape is set: [`SPAN_TARGET`], so the log output leaves
/// it out; `otel.kind = "client"`; `otel.name` from `$name`, which may be a
/// `tracing::field::display(format_args!(..))`; and the empty `error.type` and
/// `otel.status_description` that [`in_span`] fills on failure. The caller
/// adds its own attributes after the name, among them `peer.service` -- always
/// a deployed `service.name`, such as [`MAPI_RS_PEER_SERVICE`] -- when the
/// callee is a service of ours.
macro_rules! client_span {
    ($name:expr $(, $($fields:tt)*)?) => {
        tracing::info_span!(
            target: $crate::telemetry::SPAN_TARGET,
            "client",
            otel.name = $name,
            otel.kind = "client",
            otel.status_description = tracing::field::Empty,
            error.type = tracing::field::Empty,
            $($($fields)*)?
        )
    };
}
pub(crate) use client_span;
const SERVICE_VERSION: &str = "service.version";
const OTEL_RESOURCE_ATTRIBUTES: &str = "OTEL_RESOURCE_ATTRIBUTES";
const OTEL_METRIC_EXPORT_INTERVAL: &str = "OTEL_METRIC_EXPORT_INTERVAL";
const OTEL_TRACES_EXPORTER: &str = "OTEL_TRACES_EXPORTER";
const OTEL_TRACES_SAMPLER: &str = "OTEL_TRACES_SAMPLER";
const OTEL_TRACES_SAMPLER_ARG: &str = "OTEL_TRACES_SAMPLER_ARG";

/// The SDK's metrics export interval when nothing sets one.
pub const DEFAULT_METRIC_EXPORT_INTERVAL: Duration = Duration::from_secs(60);

/// Where a resource attribute or the export interval came from, for the
/// startup line.
const FROM_TABLE_ATTRIBUTES: &str = "telemetry.resource_attributes";
const FROM_TABLE_INTERVAL: &str = "telemetry.export_interval_sec";
const FROM_TABLE_TRACES: &str = "telemetry.traces_exporter";
const FROM_TABLE_ENABLED: &str = "telemetry.enabled";
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

/// The one startup line saying whether spans are exported, what turned it on
/// or off, where to and with which sampler (CS-700).
pub fn traces_startup_line(config: &TelemetryConfig) -> String {
    let (on, source) = config.traces_switch();
    if on {
        format!(
            "OpenTelemetry trace export on from {source} (service.name={}, otlp.endpoint={}, \
             sampler={})",
            config.effective_service_name(),
            redact_endpoint(&config.effective_otlp_endpoint()),
            traces_sampler(
                env::var(OTEL_TRACES_SAMPLER).ok().as_deref(),
                env::var(OTEL_TRACES_SAMPLER_ARG).ok().as_deref()
            )
            .0
        )
    } else if source == FROM_DEFAULT {
        "OpenTelemetry trace export off (enable with [telemetry] traces_exporter = \"otlp\" or \
         OTEL_TRACES_EXPORTER=otlp)"
            .to_string()
    } else {
        format!("OpenTelemetry trace export off: {source} does not list otlp")
    }
}

/// The sampler the SDK builds from `OTEL_TRACES_SAMPLER` and
/// `OTEL_TRACES_SAMPLER_ARG`, as the startup line names it, and a warning
/// when what was asked for is not what the SDK will use.
///
/// Mirrors `opentelemetry_sdk` 0.32 (`trace::Config::default`): the name is
/// matched exactly, case and spaces included; one it does not know falls back
/// to `parentbased_always_on`, and a ratio sampler whose argument is missing
/// or not a number falls back to a ratio of 1.0. Both keep every trace, so a
/// line naming the asked-for sampler would say the rate is reduced while
/// 100% is exported (CS-700 review). Values are cut short and quoted, as they
/// are printed as given.
fn traces_sampler(sampler: Option<&str>, arg: Option<&str>) -> (String, Option<String>) {
    let shown = |value: &str| format!("{:?}", value.chars().take(64).collect::<String>());
    let Some(sampler) = sampler else {
        return ("parentbased_always_on (default)".to_string(), None);
    };
    match sampler {
        "always_on" | "always_off" | "parentbased_always_on" | "parentbased_always_off" => {
            (format!("{sampler} from {OTEL_TRACES_SAMPLER}"), None)
        }
        "traceidratio" | "parentbased_traceidratio" => {
            match arg.and_then(|arg| arg.parse::<f64>().ok()) {
                Some(ratio) => (
                    format!(
                        "{sampler} from {OTEL_TRACES_SAMPLER}, ratio={ratio} from \
                         {OTEL_TRACES_SAMPLER_ARG}"
                    ),
                    None,
                ),
                None => {
                    let arg = arg.map_or_else(|| "unset".to_string(), shown);
                    (
                        format!(
                            "{sampler} from {OTEL_TRACES_SAMPLER}, ratio=1.0 because \
                             {OTEL_TRACES_SAMPLER_ARG} is {arg}: every trace kept"
                        ),
                        Some(format!(
                            "{OTEL_TRACES_SAMPLER_ARG} is {arg}, not a number: \
                             {OTEL_TRACES_SAMPLER}={sampler} keeps every trace"
                        )),
                    )
                }
            }
        }
        unknown => {
            let unknown = shown(unknown);
            (
                format!(
                    "parentbased_always_on because {OTEL_TRACES_SAMPLER}={unknown} is not a \
                     sampler: every trace kept"
                ),
                Some(format!(
                    "{OTEL_TRACES_SAMPLER}={unknown} is not a sampler, so every trace is kept; \
                     use always_on, always_off, traceidratio, parentbased_always_on, \
                     parentbased_always_off or parentbased_traceidratio"
                )),
            )
        }
    }
}

/// The warning to log at startup when the sampler asked for is not the one
/// the SDK will use. See [`traces_sampler`].
fn traces_sampler_warning(config: &TelemetryConfig) -> Option<String> {
    if !config.traces_enabled() {
        return None;
    }
    traces_sampler(
        env::var(OTEL_TRACES_SAMPLER).ok().as_deref(),
        env::var(OTEL_TRACES_SAMPLER_ARG).ok().as_deref(),
    )
    .1
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
    /// Whether spans are exported over OTLP (CS-700). See
    /// [`TelemetryConfig::traces_switch`].
    pub fn traces_enabled(&self) -> bool {
        self.traces_switch().0
    }

    /// Whether spans are exported, and the setting that decided it:
    /// `OTEL_TRACES_EXPORTER` when it is set, else `traces_exporter = "otlp"`,
    /// else `enabled = true`; off by default.
    pub fn traces_switch(&self) -> (bool, &'static str) {
        self.resolve_traces_switch(env::var(OTEL_TRACES_EXPORTER).ok().as_deref())
    }

    /// [`TelemetryConfig::traces_switch`] with `OTEL_TRACES_EXPORTER` given.
    ///
    /// The variable, set, decides alone -- `none` turns off what the table
    /// turned on, as in mapi-lite (CS-698) -- and an empty one counts as
    /// unset. It is a comma-separated list, as the spec allows (`otlp,console`):
    /// spans are exported when any entry is `otlp`, matched ignoring case and
    /// spaces, so `OTLP` works as before. The others are not exporters this
    /// service has, and are ignored.
    fn resolve_traces_switch(&self, variable: Option<&str>) -> (bool, &'static str) {
        if let Some(value) = variable.map(str::trim).filter(|value| !value.is_empty()) {
            let otlp = value
                .split(',')
                .any(|entry| entry.trim().eq_ignore_ascii_case("otlp"));
            return (otlp, OTEL_TRACES_EXPORTER);
        }
        if self.traces_exporter == TelemetryExporter::Otlp {
            return (true, FROM_TABLE_TRACES);
        }
        if self.enabled {
            return (true, FROM_TABLE_ENABLED);
        }
        (false, FROM_DEFAULT)
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
    /// it: `key=value` pairs split on `,`, each side trimmed. An empty key,
    /// as in `=orphan`, is skipped, which the SDK does not do but mapi-lite
    /// does (CS-673).
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
            .filter(|(key, _)| !key.is_empty() && key != SERVICE_NAME && key != SERVICE_VERSION)
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
        if (self.traces_enabled() || self.metrics_enabled())
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
        assert!(!config.traces_enabled());
    }

    #[test]
    fn otel_traces_exporter_env_enables_telemetry() {
        let _env = env_lock();
        unsafe { env::set_var("OTEL_TRACES_EXPORTER", "otlp") };
        let config = TelemetryConfig::default();
        assert!(config.traces_enabled());
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
        assert!(config.traces_enabled());
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
        assert!(!config.traces_enabled(), "traces keep their own switch");
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
                " deployment.environment.name = dev ,region=eu,broken,=orphan, = blank"
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

    /// `traces_exporter = "otlp"` turns spans on, alone: not metrics.
    /// `OTEL_TRACES_EXPORTER`, when set, decides over both table fields, so
    /// `none` turns off what the table turned on; empty counts as unset.
    #[test]
    fn sr_tele_014_traces_exporter_and_otel_traces_exporter_decide_span_export() {
        let off = TelemetryConfig::default();
        let traces = TelemetryConfig {
            traces_exporter: TelemetryExporter::Otlp,
            ..Default::default()
        };
        let enabled = TelemetryConfig {
            enabled: true,
            ..Default::default()
        };

        assert_eq!(off.resolve_traces_switch(None), (false, FROM_DEFAULT));
        assert_eq!(
            traces.resolve_traces_switch(None),
            (true, FROM_TABLE_TRACES)
        );
        assert_eq!(
            enabled.resolve_traces_switch(None),
            (true, FROM_TABLE_ENABLED)
        );

        for config in [&off, &traces, &enabled] {
            assert_eq!(
                config.resolve_traces_switch(Some("otlp")),
                (true, OTEL_TRACES_EXPORTER)
            );
            assert_eq!(
                config.resolve_traces_switch(Some(" OTLP ")),
                (true, OTEL_TRACES_EXPORTER)
            );
            assert_eq!(
                config.resolve_traces_switch(Some("none")),
                (false, OTEL_TRACES_EXPORTER)
            );
            // CS-700 review, comment 4: a list, as the spec allows.
            for list in ["otlp,console", "console, OTLP ", " otlp , none"] {
                assert_eq!(
                    config.resolve_traces_switch(Some(list)),
                    (true, OTEL_TRACES_EXPORTER),
                    "{list}"
                );
            }
            for list in ["console", "console,none", "otlpx,console"] {
                assert_eq!(
                    config.resolve_traces_switch(Some(list)),
                    (false, OTEL_TRACES_EXPORTER),
                    "{list}"
                );
            }
        }
        assert_eq!(
            traces.resolve_traces_switch(Some(" ")),
            (true, FROM_TABLE_TRACES)
        );
    }

    #[test]
    fn sr_tele_014_traces_exporter_does_not_turn_on_metrics() {
        let _env = env_lock();
        clear_metrics_env();
        let config = TelemetryConfig {
            traces_exporter: TelemetryExporter::Otlp,
            ..Default::default()
        };
        assert!(!config.metrics_enabled());
    }

    /// The traces startup line says what decided, where to with no
    /// credential, and the sampler; off, how to turn it on.
    #[test]
    fn sr_tele_014_the_traces_startup_line_names_source_endpoint_and_sampler() {
        let _env = env_lock();
        clear_metrics_env();
        for name in [
            OTEL_TRACES_EXPORTER,
            OTEL_TRACES_SAMPLER,
            OTEL_TRACES_SAMPLER_ARG,
        ] {
            unsafe { env::remove_var(name) };
        }
        let line = traces_startup_line(&TelemetryConfig::default());
        assert!(line.contains("trace export off"), "{line}");
        assert!(line.contains("traces_exporter = \"otlp\""), "{line}");

        let config = TelemetryConfig {
            traces_exporter: TelemetryExporter::Otlp,
            otlp_endpoint: Some("http://user:secret@collector:4317".to_string()),
            ..Default::default()
        };
        let line = traces_startup_line(&config);
        assert!(line.contains("on from telemetry.traces_exporter"), "{line}");
        assert!(
            line.contains("otlp.endpoint=http://collector:4317"),
            "{line}"
        );
        assert!(!line.contains("secret"), "{line}");
        assert!(
            line.contains("sampler=parentbased_always_on (default)"),
            "{line}"
        );

        unsafe { env::set_var(OTEL_TRACES_SAMPLER, "parentbased_traceidratio") };
        unsafe { env::set_var(OTEL_TRACES_SAMPLER_ARG, "0.1") };
        let line = traces_startup_line(&config);
        assert!(
            line.contains(
                "sampler=parentbased_traceidratio from OTEL_TRACES_SAMPLER, ratio=0.1 from \
                 OTEL_TRACES_SAMPLER_ARG"
            ),
            "{line}"
        );

        unsafe { env::set_var(OTEL_TRACES_EXPORTER, "none") };
        let line = traces_startup_line(&config);
        for name in [
            OTEL_TRACES_EXPORTER,
            OTEL_TRACES_SAMPLER,
            OTEL_TRACES_SAMPLER_ARG,
        ] {
            unsafe { env::remove_var(name) };
        }
        assert!(
            line.contains("trace export off: OTEL_TRACES_EXPORTER does not list otlp"),
            "{line}"
        );
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

    /// CS-700 review, comment 5: the startup line names the sampler the SDK
    /// will use, not the one asked for, and a warning says when they differ.
    #[test]
    fn sr_tele_014_the_startup_line_names_the_sampler_in_use() {
        let (line, warning) = traces_sampler(None, Some("0.1"));
        assert_eq!(line, "parentbased_always_on (default)");
        assert_eq!(warning, None);

        let (line, warning) = traces_sampler(Some("parentbased_traceidratio"), Some("0.1"));
        assert!(line.contains("ratio=0.1"), "{line}");
        assert_eq!(warning, None);

        let (line, warning) = traces_sampler(Some("always_off"), None);
        assert_eq!(line, "always_off from OTEL_TRACES_SAMPLER");
        assert_eq!(warning, None);

        // A typo: the SDK falls back to keeping every trace.
        for typo in [
            "parentbased_traceidratios",
            "PARENTBASED_TRACEIDRATIO",
            " traceidratio",
        ] {
            let (line, warning) = traces_sampler(Some(typo), Some("0.1"));
            assert!(line.starts_with("parentbased_always_on because"), "{line}");
            assert!(line.contains("every trace kept"), "{line}");
            assert!(!line.contains("0.1"), "{line}");
            assert!(warning.expect("a warning").contains("is not a sampler"));
        }

        // A ratio that is missing or not a number: the SDK uses 1.0.
        for arg in [None, Some("ten percent"), Some(" 0.1")] {
            let (line, warning) = traces_sampler(Some("traceidratio"), arg);
            assert!(line.contains("ratio=1.0 because"), "{line}");
            assert!(line.contains("every trace kept"), "{line}");
            assert!(warning.expect("a warning").contains("keeps every trace"));
        }

        // Printed as given, so cut short.
        let (line, _) = traces_sampler(Some(&"x".repeat(500)), None);
        assert!(line.len() < 200, "{line}");
    }

    /// CS-700 review, comment 3: the trace spans do not prefix log lines.
    /// A warning logged during a broadcast reads as it did before, while an
    /// ordinary span, such as the request's, still shows.
    #[test]
    fn sr_tele_016_trace_spans_stay_out_of_the_log_output() {
        use std::io::Write;
        use std::sync::{Arc, Mutex};

        #[derive(Clone, Default)]
        struct Buffer(Arc<Mutex<Vec<u8>>>);
        impl Write for Buffer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let buffer = Buffer::default();
        let writer = buffer.clone();
        let subscriber = tracing_subscriber::registry().with(fmt_layer(move || writer.clone()));
        tracing::subscriber::with_default(subscriber, || {
            let request = tracing::info_span!("fs_log_test_request", http.method = "POST");
            let _request = request.enter();
            let broadcast = client_span!("mapi-rs broadcast", peer.service = MAPI_RS_PEER_SERVICE);
            let _broadcast = broadcast.enter();
            tracing::warn!("fs log test: mapi-lite answered 503");
        });

        let output = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
        assert!(
            output.contains("fs log test: mapi-lite answered 503"),
            "{output}"
        );
        assert!(output.contains("fs_log_test_request"), "{output}");
        assert!(!output.contains("mapi-rs broadcast"), "{output}");
        assert!(!output.contains("otel.name"), "{output}");
        assert!(!output.contains("peer.service"), "{output}");
    }
}
