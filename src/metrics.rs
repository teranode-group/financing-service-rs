//! Phase 1 metrics, pushed over OTLP (CS-608).
//!
//! Instruments are created once, from whatever meter provider is installed
//! when they are built. With export off nothing is installed (see
//! [`crate::telemetry::build_meter_provider`]), `global::meter` is the no-op
//! meter, and recording costs nothing.
//!
//! The observable gauges never touch a lock that can wait or anything that
//! does I/O in their callbacks: a background task ([`spawn_sampler`], run only
//! with export on) samples the service into [`Samples`], and the callbacks
//! read that.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use actix_web::{
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    middleware::Next,
    web, Error, HttpResponse,
};
use opentelemetry::{
    global,
    metrics::{Counter, Histogram, Meter, ObservableGauge},
    KeyValue,
};

use crate::{
    responses::{response_code, ErrorCode},
    rest_api::{readiness, AppState},
    service::Service,
};

/// The instrumentation scope every instrument is created under.
pub const METER_NAME: &str = "financing-service";

/// How often the gauges' sampler reads the service.
pub const SAMPLE_PERIOD: Duration = Duration::from_secs(15);

/// `route` for a request that matched no resource. Never the raw path.
pub const UNMATCHED_ROUTE: &str = "unmatched";

/// Request-duration buckets, in seconds. The SDK's default boundaries are
/// sized for milliseconds and would put almost every request in the first
/// bucket.
const DURATION_BOUNDARIES: [f64; 14] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0, 30.0, 60.0,
];

/// Every `code` a `POST /fund` answer can carry in `fs_fund_requests`: the
/// list in uls-rs `observability/telemetry-inventory.md`. `client_exists` and
/// `static_client` are answered by the `/client` routes only, and
/// `unclassified` should never happen, so none of them is created up front.
pub const FUND_CODES: [&str; 17] = [
    "funded",
    ErrorCode::InsufficientBalance.as_str(),
    ErrorCode::NoSuitableUtxo.as_str(),
    ErrorCode::FundsInFlight.as_str(),
    ErrorCode::BroadcastRejected.as_str(),
    ErrorCode::BroadcastFailed.as_str(),
    ErrorCode::BroadcastOutcomeUnknown.as_str(),
    ErrorCode::BroadcasterUntrusted.as_str(),
    ErrorCode::PartialBroadcast.as_str(),
    ErrorCode::ChainUnavailable.as_str(),
    ErrorCode::InvalidRequest.as_str(),
    ErrorCode::UnknownClient.as_str(),
    ErrorCode::Unauthorized.as_str(),
    ErrorCode::RateLimited.as_str(),
    ErrorCode::KeyInProgress.as_str(),
    ErrorCode::IdempotencyKeyReused.as_str(),
    ErrorCode::Internal.as_str(),
];

/// The synchronous instruments, built once at startup and cloned cheaply.
#[derive(Clone)]
pub struct Metrics {
    http_requests: Counter<u64>,
    http_request_duration: Histogram<f64>,
    fund_requests: Counter<u64>,
}

impl Metrics {
    /// Builds the instruments and creates `fs_fund_requests` at 0 for every
    /// code in [`FUND_CODES`] (CS-667). A series that first appears at 1 has
    /// no earlier sample, so `increase()` and `rate()` would miss the first
    /// `broadcast_outcome_unknown` after every start. Adding 0 again (another
    /// `Metrics` from the same meter) changes nothing.
    pub fn new(meter: &Meter) -> Self {
        let metrics = Self::build(meter);
        for code in FUND_CODES {
            metrics.fund_requests.add(0, &[KeyValue::new("code", code)]);
        }
        metrics
    }

    fn build(meter: &Meter) -> Self {
        Self {
            http_requests: meter
                .u64_counter("http_requests")
                .with_description("HTTP requests answered, by route template and status class")
                .build(),
            http_request_duration: meter
                .f64_histogram("http_request_duration")
                .with_unit("s")
                .with_description("Time to answer an HTTP request, by route template")
                .with_boundaries(DURATION_BOUNDARIES.to_vec())
                .build(),
            fund_requests: meter
                .u64_counter("fs_fund_requests")
                .with_description(
                    "POST /fund outcomes: funded, or the error code the caller was answered with",
                )
                .build(),
        }
    }

    /// From the globally installed meter provider -- the no-op one when
    /// export is off. Build after [`crate::telemetry::init`].
    pub fn global() -> Self {
        Self::new(&global::meter(METER_NAME))
    }

    pub fn record_http(&self, route: &str, status: u16, elapsed: Duration) {
        let route = KeyValue::new("route", route.to_string());
        self.http_requests.add(
            1,
            &[
                route.clone(),
                KeyValue::new("status_class", status_class(status)),
            ],
        );
        self.http_request_duration
            .record(elapsed.as_secs_f64(), &[route]);
    }

    /// Count one `POST /fund` answer: `funded` for a success, otherwise the
    /// `ErrorCode` its body carries. A replayed answer counts as what it
    /// replays.
    pub fn record_fund(&self, response: &HttpResponse) {
        let code = match response_code(response) {
            Some(code) => code.as_str(),
            None if response.status().is_success() => "funded",
            // Every error `/fund` answers is built with a code; this is a
            // response that somehow was not, kept out of the code set.
            None => "unclassified",
        };
        self.fund_requests.add(1, &[KeyValue::new("code", code)]);
    }
}

fn status_class(status: u16) -> &'static str {
    match status / 100 {
        1 => "1xx",
        2 => "2xx",
        3 => "3xx",
        4 => "4xx",
        _ => "5xx",
    }
}

/// Middleware recording `http_requests` and `http_request_duration` for every
/// request, `/health`, `/ready` and `/status` included. `route` is the
/// resource pattern the request matched (`/client/{client_id}/balance`), never
/// the path it arrived on.
pub async fn record_http(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, Error> {
    let started = Instant::now();
    let metrics = req
        .app_data::<web::Data<AppState>>()
        .map(|state| state.metrics.clone());
    let pattern = req.match_pattern();
    let result = next.call(req).await;
    if let Some(metrics) = metrics {
        let (route, status) = match &result {
            Ok(response) => (
                response.request().match_pattern().or(pattern),
                response.status().as_u16(),
            ),
            Err(error) => (pattern, error.as_response_error().status_code().as_u16()),
        };
        metrics.record_http(
            route.as_deref().unwrap_or(UNMATCHED_ROUTE),
            status,
            started.elapsed(),
        );
    }
    result
}

/// One client's figures, as the sampler last read them.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientSample {
    pub client_id: String,
    pub confirmed: i64,
    pub unconfirmed: i64,
    /// When the client's unspent set was last read from the chain -- the
    /// moment `GET /ready` measures against. Kept as a moment rather than an
    /// age so the gauge is current when it is exported, not when sampled.
    pub chain_read_at: Option<Instant>,
}

/// What the gauges report, written by the sampler and read by the callbacks.
#[derive(Debug, Default)]
pub struct Samples {
    pub clients: Vec<ClientSample>,
    /// Each readiness check by name, then `overall`. Empty until sampled.
    pub ready: Vec<(&'static str, bool)>,
}

pub type SharedSamples = Arc<Mutex<Samples>>;

/// The observable gauges. Hold this for as long as they should report.
pub struct Gauges {
    _client_balance: ObservableGauge<i64>,
    _client_chain_state_age: ObservableGauge<f64>,
    _ready: ObservableGauge<u64>,
}

/// Register `fs_client_balance`, `fs_client_chain_state_age` and `ready`,
/// reading from `samples`. The callbacks only copy out of `samples`, under a
/// lock the sampler holds for no longer than an assignment.
pub fn register_gauges(meter: &Meter, samples: SharedSamples) -> Gauges {
    let balances = Arc::clone(&samples);
    let ages = Arc::clone(&samples);
    let ready = samples;
    Gauges {
        _client_balance: meter
            .i64_observable_gauge("fs_client_balance")
            .with_unit("sat")
            .with_description("Each client's balance in satoshis, from the service's cache")
            .with_callback(move |observer| {
                let Ok(samples) = balances.lock() else { return };
                for client in &samples.clients {
                    let id = KeyValue::new("client_id", client.client_id.clone());
                    observer.observe(
                        client.confirmed,
                        &[id.clone(), KeyValue::new("kind", "confirmed")],
                    );
                    observer.observe(
                        client.unconfirmed,
                        &[id, KeyValue::new("kind", "unconfirmed")],
                    );
                }
            })
            .build(),
        _client_chain_state_age: meter
            .f64_observable_gauge("fs_client_chain_state_age")
            .with_unit("s")
            .with_description(
                "Seconds since each client's unspent set was last read from the chain",
            )
            .with_callback(move |observer| {
                let Ok(samples) = ages.lock() else { return };
                for client in &samples.clients {
                    if let Some(at) = client.chain_read_at {
                        observer.observe(
                            at.elapsed().as_secs_f64(),
                            &[KeyValue::new("client_id", client.client_id.clone())],
                        );
                    }
                }
            })
            .build(),
        _ready: meter
            .u64_observable_gauge("ready")
            .with_description("1 when a readiness check passes, 0 when it fails")
            .with_callback(move |observer| {
                let Ok(samples) = ready.lock() else { return };
                for (check, ok) in &samples.ready {
                    observer.observe(u64::from(*ok), &[KeyValue::new("check", *check)]);
                }
            })
            .build(),
    }
}

/// Read the service into `samples` once: every client's cached balance and
/// last chain read, and the `GET /ready` verdict. Reads the caches only; the
/// one upstream call is the mapi-lite probe `GET /ready` itself makes, whose
/// verdict is cached for [`crate::config::MAPI_LITE_PROBE_TIMEOUT`].
pub async fn sample(service: &Service, samples: &SharedSamples) {
    let clients = service.client_samples().await;
    let health = readiness(service, false).await;
    let mut ready = Vec::with_capacity(3);
    if let Some(chain) = &health.chain {
        ready.push(("chain", chain.ok));
    }
    if let Some(mapi_lite) = &health.mapi_lite {
        ready.push(("mapi_lite", mapi_lite.ok));
    }
    ready.push(("overall", health.is_ok()));
    if let Ok(mut samples) = samples.lock() {
        samples.clients = clients;
        samples.ready = ready;
    }
}

/// Sample every [`SAMPLE_PERIOD`], starting now. Spawned only with metrics
/// export on.
pub fn spawn_sampler(service: Arc<Service>, samples: SharedSamples) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(SAMPLE_PERIOD);
        loop {
            interval.tick().await;
            sample(&service, &samples).await;
        }
    });
}

#[cfg(test)]
pub mod testing {
    //! An in-memory pipeline to assert metrics against.

    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::{
        data::{AggregatedMetrics, MetricData, ResourceMetrics},
        InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
    };

    pub struct TestMeter {
        pub provider: SdkMeterProvider,
        exporter: InMemoryMetricExporter,
    }

    /// One exported data point, flattened: its value and attributes.
    #[derive(Debug, Clone, PartialEq)]
    pub struct Point {
        pub value: f64,
        /// For a histogram, the number of recordings; otherwise 0.
        pub count: u64,
        pub attributes: Vec<(String, String)>,
    }

    impl Point {
        pub fn attr(&self, key: &str) -> Option<&str> {
            self.attributes
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.as_str())
        }
    }

    impl TestMeter {
        pub fn new() -> Self {
            let exporter = InMemoryMetricExporter::default();
            let provider = SdkMeterProvider::builder()
                .with_reader(PeriodicReader::builder(exporter.clone()).build())
                .build();
            Self { provider, exporter }
        }

        pub fn meter(&self) -> opentelemetry::metrics::Meter {
            self.provider.meter(super::METER_NAME)
        }

        /// Export now and return every data point of `name` in the latest
        /// export, with its unit.
        pub fn points(&self, name: &str) -> (String, Vec<Point>) {
            self.exporter.reset();
            self.provider.force_flush().expect("flush");
            let exported = self.exporter.get_finished_metrics().expect("metrics");
            let latest: &ResourceMetrics = exported.last().expect("an export");
            for scope in latest.scope_metrics() {
                for metric in scope.metrics() {
                    if metric.name() == name {
                        return (metric.unit().to_string(), flatten(metric.data()));
                    }
                }
            }
            (String::new(), Vec::new())
        }
    }

    fn attrs<'a>(kvs: impl Iterator<Item = &'a opentelemetry::KeyValue>) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = kvs
            .map(|kv| (kv.key.to_string(), kv.value.to_string()))
            .collect();
        out.sort();
        out
    }

    fn flatten(data: &AggregatedMetrics) -> Vec<Point> {
        fn points<T: Copy>(data: &MetricData<T>, to_f64: impl Fn(T) -> f64) -> Vec<Point> {
            match data {
                MetricData::Sum(sum) => sum
                    .data_points()
                    .map(|p| Point {
                        value: to_f64(p.value()),
                        count: 0,
                        attributes: attrs(p.attributes()),
                    })
                    .collect(),
                MetricData::Gauge(gauge) => gauge
                    .data_points()
                    .map(|p| Point {
                        value: to_f64(p.value()),
                        count: 0,
                        attributes: attrs(p.attributes()),
                    })
                    .collect(),
                MetricData::Histogram(histogram) => histogram
                    .data_points()
                    .map(|p| Point {
                        value: to_f64(p.sum()),
                        count: p.count(),
                        attributes: attrs(p.attributes()),
                    })
                    .collect(),
                MetricData::ExponentialHistogram(_) => Vec::new(),
            }
        }
        match data {
            AggregatedMetrics::U64(data) => points(data, |v| v as f64),
            AggregatedMetrics::I64(data) => points(data, |v| v as f64),
            AggregatedMetrics::F64(data) => points(data, |v| v),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        testing::{Point, TestMeter},
        *,
    };

    use actix_web::{middleware::from_fn, test as actix_test, App};
    use serde_json::json;

    use crate::{
        rest_api::{
            add_client, balance, delete_client, get_address, get_funds, health, index, ready,
            status,
        },
        test_support::{
            test_blockchain_interface, test_config, unique_dynamic_config_path,
            StubMapiBroadcaster, LOCKING_SCRIPT_HEX, TEST_CLIENT_ID,
        },
    };

    async fn service(mapi_lite_healthy: Option<bool>) -> Arc<Service> {
        let config = test_config(&unique_dynamic_config_path());
        let blockchain = test_blockchain_interface(&config).await;
        Arc::new(match mapi_lite_healthy {
            None => Service::new_for_test(&config, blockchain).await,
            Some(healthy) => {
                Service::new_for_test_with_broadcaster(
                    &config,
                    blockchain,
                    StubMapiBroadcaster::new(healthy),
                )
                .await
            }
        })
    }

    macro_rules! app {
        ($service:expr, $meter:expr) => {
            actix_test::init_service(
                App::new()
                    .wrap(from_fn(record_http))
                    .app_data(web::Data::new(AppState {
                        service: $service,
                        metrics: Metrics::new(&$meter.meter()),
                    }))
                    .service(index)
                    .service(health)
                    .service(ready)
                    .service(status)
                    .service(balance)
                    .service(get_funds)
                    .service(add_client)
                    .service(delete_client)
                    .service(get_address),
            )
            .await
        };
    }

    fn fund(satoshi: u64, key: Option<&str>) -> serde_json::Value {
        let mut body = json!({
            "client_id": TEST_CLIENT_ID,
            "satoshi": satoshi,
            "no_of_outpoints": 1,
            "multiple_tx": false,
            "locking_script": LOCKING_SCRIPT_HEX,
        });
        if let Some(key) = key {
            body["idempotency_key"] = json!(key);
        }
        body
    }

    fn total(points: &[Point], key: &str, value: &str) -> f64 {
        points
            .iter()
            .filter(|p| p.attr(key) == Some(value))
            .map(|p| p.value)
            .sum()
    }

    /// `route` is the template the request matched, never the path it came
    /// in on; an unmatched path is `unmatched`; the probes count like any
    /// other route.
    #[actix_web::test]
    async fn sr_tele_007_http_requests_are_counted_by_route_template_and_status_class() {
        let meter = TestMeter::new();
        let app = app!(service(None).await, meter);

        for uri in [
            format!("/client/{TEST_CLIENT_ID}/balance"),
            "/client/someone-else/balance".to_string(),
            "/no/such/path/abc123".to_string(),
            "/health".to_string(),
        ] {
            actix_test::call_service(&app, actix_test::TestRequest::get().uri(&uri).to_request())
                .await;
        }

        let (unit, requests) = meter.points("http_requests");
        assert_eq!(unit, "");
        let balance_points: Vec<&Point> = requests
            .iter()
            .filter(|p| p.attr("route") == Some("/client/{client_id}/balance"))
            .collect();
        assert_eq!(
            total(&requests, "route", "/client/{client_id}/balance"),
            2.0,
            "{requests:?}"
        );
        assert!(balance_points
            .iter()
            .any(|p| p.attr("status_class") == Some("2xx")));
        assert!(balance_points
            .iter()
            .any(|p| p.attr("status_class") == Some("4xx")));
        assert_eq!(total(&requests, "route", UNMATCHED_ROUTE), 1.0);
        assert_eq!(total(&requests, "route", "/health"), 1.0);
        for point in &requests {
            let route = point.attr("route").unwrap();
            assert!(
                !route.contains(TEST_CLIENT_ID) && !route.contains("abc123"),
                "a raw path leaked into route: {route}"
            );
        }

        let (unit, durations) = meter.points("http_request_duration");
        assert_eq!(unit, "s");
        let balance_durations = durations
            .iter()
            .find(|p| p.attr("route") == Some("/client/{client_id}/balance"))
            .expect("a duration for the balance route");
        assert_eq!(balance_durations.count, 2);
        assert_eq!(balance_durations.attributes.len(), 1, "route only");
    }

    /// `funded` on success, the response's code on every error, and a replay
    /// counted as the outcome it replays.
    #[actix_web::test]
    async fn sr_tele_008_fund_requests_are_counted_by_outcome_code() {
        let meter = TestMeter::new();
        let app = app!(service(None).await, meter);
        let post = |body: serde_json::Value| {
            actix_test::TestRequest::post()
                .uri("/fund")
                .set_json(body)
                .to_request()
        };

        let first = actix_test::call_service(&app, post(fund(123, Some("k1")))).await;
        assert!(first.status().is_success());
        // the replay of the same key
        let replay = actix_test::call_service(&app, post(fund(123, Some("k1")))).await;
        assert!(replay.status().is_success());
        // the same key with a different request
        actix_test::call_service(&app, post(fund(124, Some("k1")))).await;
        actix_test::call_service(&app, post(fund(100_000_000, None))).await;
        actix_test::call_service(&app, post(fund(0, None))).await;

        let (_, points) = meter.points("fs_fund_requests");
        assert_eq!(total(&points, "code", "funded"), 2.0, "{points:?}");
        assert_eq!(total(&points, "code", "idempotency_key_reused"), 1.0);
        assert_eq!(total(&points, "code", "insufficient_balance"), 1.0);
        assert_eq!(total(&points, "code", "invalid_request"), 1.0);
        assert!(points.iter().all(|p| p.attributes.len() == 1));
        // the codes nothing answered with are still there, at 0
        assert_eq!(points.len(), FUND_CODES.len(), "{points:?}");
        assert_eq!(total(&points, "code", "broadcast_outcome_unknown"), 0.0);
    }

    /// Before any request, every `fs_fund_requests` code is exported at 0, so
    /// the first real one is an increase from 0 rather than a new series.
    #[test]
    fn sr_tele_012_fund_request_codes_start_at_zero() {
        let meter = TestMeter::new();
        let _metrics = Metrics::new(&meter.meter());
        // a second set of instruments from the same meter adds nothing
        let _again = Metrics::new(&meter.meter());

        let (_, points) = meter.points("fs_fund_requests");
        let mut codes: Vec<&str> = points.iter().map(|p| p.attr("code").unwrap()).collect();
        codes.sort();
        let mut expected = FUND_CODES.to_vec();
        expected.sort();
        assert_eq!(codes, expected);
        assert!(points.iter().all(|p| p.value == 0.0), "{points:?}");
    }

    /// The codes are distinct and leave out the ones `POST /fund` never
    /// answers with. That each is a real code is checked at compile time.
    #[test]
    fn fund_codes_are_distinct_and_fund_answers_only() {
        let mut codes = FUND_CODES.to_vec();
        codes.sort();
        codes.dedup();
        assert_eq!(codes.len(), FUND_CODES.len());
        for code in [
            ErrorCode::ClientExists.as_str(),
            ErrorCode::StaticClient.as_str(),
            "unclassified",
        ] {
            assert!(!FUND_CODES.contains(&code), "{code}");
        }
    }

    #[actix_web::test]
    async fn sr_tele_009_client_gauges_report_each_client_s_balance_and_chain_state_age() {
        let meter = TestMeter::new();
        let service = service(None).await;
        let samples = SharedSamples::default();
        let _gauges = register_gauges(&meter.meter(), Arc::clone(&samples));
        let expected = service.get_balance(TEST_CLIENT_ID).await.unwrap();

        sample(&service, &samples).await;

        let (unit, balances) = meter.points("fs_client_balance");
        assert_eq!(unit, "sat");
        let of = |kind: &str| {
            balances
                .iter()
                .find(|p| {
                    p.attr("client_id") == Some(TEST_CLIENT_ID) && p.attr("kind") == Some(kind)
                })
                .map(|p| p.value)
        };
        assert_eq!(of("confirmed"), Some(expected.confirmed as f64));
        assert_eq!(of("unconfirmed"), Some(expected.unconfirmed as f64));

        let (unit, ages) = meter.points("fs_client_chain_state_age");
        assert_eq!(unit, "s");
        let age = ages
            .iter()
            .find(|p| p.attr("client_id") == Some(TEST_CLIENT_ID))
            .expect("an age for the client")
            .value;
        assert!((0.0..60.0).contains(&age), "{age}");

        // and the age moves with the chain read it measures
        service.backdate_chain_reads(Duration::from_secs(600)).await;
        sample(&service, &samples).await;
        let (_, ages) = meter.points("fs_client_chain_state_age");
        assert!(ages[0].value >= 600.0, "{ages:?}");
    }

    #[actix_web::test]
    async fn sr_tele_010_ready_gauge_reports_each_check_and_overall() {
        let meter = TestMeter::new();
        let service = service(Some(false)).await;
        let samples = SharedSamples::default();
        let _gauges = register_gauges(&meter.meter(), Arc::clone(&samples));

        sample(&service, &samples).await;

        let (_, points) = meter.points("ready");
        assert_eq!(total(&points, "check", "chain"), 1.0, "{points:?}");
        assert_eq!(total(&points, "check", "mapi_lite"), 0.0);
        assert!(points.iter().any(|p| p.attr("check") == Some("mapi_lite")));
        assert_eq!(total(&points, "check", "overall"), 0.0);
        assert!(points.iter().any(|p| p.attr("check") == Some("overall")));

        // with mapi-lite healthy, overall flips to 1
        let healthy = TestMeter::new();
        let service = super::tests::service(Some(true)).await;
        let samples = SharedSamples::default();
        let _gauges = register_gauges(&healthy.meter(), Arc::clone(&samples));
        sample(&service, &samples).await;
        let (_, points) = healthy.points("ready");
        assert_eq!(total(&points, "check", "overall"), 1.0, "{points:?}");
    }

    /// With export off the meter is the no-op one: recording through it is
    /// accepted and goes nowhere.
    #[test]
    fn instruments_from_the_noop_meter_are_inert() {
        let metrics = Metrics::new(&opentelemetry::metrics::MeterProvider::meter(
            &opentelemetry::metrics::noop::NoopMeterProvider::new(),
            METER_NAME,
        ));
        metrics.record_http("/health", 200, Duration::from_millis(1));
        metrics.record_fund(&HttpResponse::Ok().finish());
    }

    #[test]
    fn status_classes() {
        assert_eq!(status_class(200), "2xx");
        assert_eq!(status_class(302), "3xx");
        assert_eq!(status_class(404), "4xx");
        assert_eq!(status_class(503), "5xx");
    }
}
