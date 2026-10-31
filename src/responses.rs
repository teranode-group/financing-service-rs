use actix_web::{http::header, http::header::ContentType, http::StatusCode, HttpResponse};
use serde::Serialize;

#[derive(Debug, Serialize, Clone, Copy)]
pub enum BlockchainConnectionStatus {
    Unknown,
    Failed,
    Connected,
}

/// Stable, machine-readable classification of an error response.
///
/// The `description` accompanying a code is prose for humans and may be
/// reworded at any time; the code is the contract. Clients should treat an
/// unrecognised code the way they treated responses before codes existed.
#[derive(Debug, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// The wallet's total balance cannot cover the request. Retryable once
    /// the wallet is topped up.
    InsufficientBalance,
    /// The balance is sufficient in total, but no combination of UTXOs can
    /// satisfy the request. Retryable once the wallet's UTXOs are split.
    NoSuitableUtxo,
    /// Every UTXO that could fund the request is held by funding requests
    /// still being broadcast, and the change they return will cover it
    /// (CS-475). Nothing is wrong with the wallet: retry unchanged after the
    /// interval in `Retry-After`.
    ///
    /// Separate from [`ErrorCode::InsufficientBalance`] and
    /// [`ErrorCode::NoSuitableUtxo`] because the advice is the opposite. Those
    /// need an operator to top up or consolidate; this clears by itself as
    /// soon as the requests ahead of it finish.
    FundsInFlight,
    /// No such `client_id`. Never retryable without a configuration change.
    UnknownClient,
    /// The `client_id` is already configured.
    ClientExists,
    /// `DELETE /client` named a client from the configuration file rather
    /// than one added at runtime (CS-653). The service cannot remove it: the
    /// file is not its to edit, and the client would be back at the next
    /// restart. An operator removes it from the file and restarts.
    StaticClient,
    /// The request itself is malformed. Never retryable unchanged.
    InvalidRequest,
    /// The funding transaction was built and signed but the upstream could
    /// not be reached, or refused it in a way it said was worth retrying.
    /// Nothing was spent, so the client's own retry may succeed.
    BroadcastFailed,
    /// The upstream answered and refused the funding transaction, and said a
    /// resubmission would not change that -- a conflicting spend, say.
    ///
    /// Separate from [`ErrorCode::BroadcastFailed`] because the advice is the
    /// opposite: retrying this transaction can never work. Something has to
    /// change first, usually the wallet's UTXO set, and usually by an
    /// operator. The upstream's own reason is in the service log.
    BroadcastRejected,
    /// The funding transaction was handed to the broadcaster and its fate is
    /// unknown -- the submit deadline expired, or the answer could not be
    /// read. It may be on the network.
    ///
    /// The service has already treated its inputs as spent, so its own wallet
    /// is safe either way. A caller must not retry with a fresh
    /// `idempotency_key`: that would fund a second time. Retrying with the
    /// original `idempotency_key` is refused with `key_in_progress` until the
    /// record expires, which is the honest answer -- the service does not know
    /// the outcome either, and the chain is where to look for it.
    BroadcastOutcomeUnknown,
    /// Nothing was sent: the broadcaster (mapi-lite) signs with a key that
    /// `[mapi_lite] miner_id_public_keys` does not list, so its answers cannot
    /// be believed (CS-618). Nothing was spent. Retrying unchanged will not
    /// help until an operator fixes the key list or mapi-lite's key;
    /// `GET /ready` reports the same.
    BroadcasterUntrusted,
    /// Some of the requested funding transactions broadcast and some did not.
    /// The successful ones are in the response body.
    PartialBroadcast,
    /// The blockchain interface could not be reached to refresh chain state.
    /// Retryable.
    ChainUnavailable,
    /// An unexpected internal failure.
    Internal,
    /// Authentication missing or invalid.
    Unauthorized,
    /// The caller exceeded the configured request rate. Retryable after the
    /// interval given in the description.
    RateLimited,
    /// A request carrying this `idempotency_key` is still being processed.
    /// Retry shortly; the completed response will then be replayed.
    KeyInProgress,
    /// This `idempotency_key` was already used with a materially different
    /// request. Use a fresh `idempotency_key`.
    IdempotencyKeyReused,
}

impl ErrorCode {
    /// The code as it is serialised, for a metric attribute (CS-608) without
    /// going through serde on every request.
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::InsufficientBalance => "insufficient_balance",
            ErrorCode::NoSuitableUtxo => "no_suitable_utxo",
            ErrorCode::FundsInFlight => "funds_in_flight",
            ErrorCode::UnknownClient => "unknown_client",
            ErrorCode::ClientExists => "client_exists",
            ErrorCode::StaticClient => "static_client",
            ErrorCode::InvalidRequest => "invalid_request",
            ErrorCode::BroadcastFailed => "broadcast_failed",
            ErrorCode::BroadcastRejected => "broadcast_rejected",
            ErrorCode::BroadcastOutcomeUnknown => "broadcast_outcome_unknown",
            ErrorCode::BroadcasterUntrusted => "broadcaster_untrusted",
            ErrorCode::PartialBroadcast => "partial_broadcast",
            ErrorCode::ChainUnavailable => "chain_unavailable",
            ErrorCode::Internal => "internal",
            ErrorCode::Unauthorized => "unauthorized",
            ErrorCode::RateLimited => "rate_limited",
            ErrorCode::KeyInProgress => "key_in_progress",
            ErrorCode::IdempotencyKeyReused => "idempotency_key_reused",
        }
    }

    /// HTTP status this code is reported with.
    ///
    /// Splitting the statuses lets a caller classify a failure without
    /// parsing the body, which matters for anything between the caller and
    /// this service -- a proxy, a gateway, a retry policy, an alerting rule --
    /// that sees the status and not the payload. The rule is that `5xx` is
    /// worth retrying unchanged and `4xx` needs the caller or an operator to
    /// change something.
    pub fn status(self) -> StatusCode {
        match self {
            // the caller must change the request
            ErrorCode::InvalidRequest => StatusCode::BAD_REQUEST,
            // the named client does not exist
            ErrorCode::UnknownClient => StatusCode::NOT_FOUND,
            // the request is well formed but conflicts with current state.
            // A definitive rejection belongs here rather than with the 5xx
            // upstream failures: the service and the upstream both did their
            // job, and what has to change is the state they were working on.
            ErrorCode::ClientExists
            | ErrorCode::StaticClient
            | ErrorCode::InsufficientBalance
            | ErrorCode::NoSuitableUtxo
            | ErrorCode::BroadcastRejected
            | ErrorCode::KeyInProgress
            | ErrorCode::IdempotencyKeyReused => StatusCode::CONFLICT,
            // upstream node rejected the transaction or was unreachable, so
            // it never took it
            ErrorCode::BroadcastFailed => StatusCode::BAD_GATEWAY,
            // the upstream may have taken the transaction and we cannot tell.
            // A separate status from 502 on purpose: anything between the
            // caller and this service that retries on a status alone must be
            // able to see that this one is not a clean "it did not happen".
            ErrorCode::BroadcastOutcomeUnknown => StatusCode::GATEWAY_TIMEOUT,
            // Busy rather than wrong: the same request succeeds once the
            // funding ahead of it completes, which is what 5xx tells anything
            // reading the status alone.
            ErrorCode::ChainUnavailable | ErrorCode::FundsInFlight => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            // Not this request's fault, and not the wallet's: the service
            // cannot fund through its broadcaster until it is reconfigured,
            // which /ready reports too (CS-618).
            ErrorCode::BroadcasterUntrusted => StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::Unauthorized => StatusCode::UNAUTHORIZED,
            ErrorCode::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            // Neither a success nor a clean failure: some transactions are on
            // chain and the caller has to read the body to learn which. Kept
            // as 422 deliberately -- 207 Multi-Status is the honest code but
            // is WebDAV and rarely handled, and any plain failure status would
            // invite a caller to ignore a body it must not ignore.
            ErrorCode::PartialBroadcast => StatusCode::UNPROCESSABLE_ENTITY,
        }
    }

    /// Seconds a caller should wait before retrying, sent as `Retry-After`.
    ///
    /// Only where the wait is known to be short. Funds in flight come back
    /// when the requests holding them finish broadcasting, which takes about
    /// as long as a submit does -- well under a second in the common case --
    /// so one second is enough for most retries to succeed without leaving
    /// the caller idle for long. A retry that comes too soon is answered the
    /// same way again.
    pub fn retry_after_seconds(self) -> Option<u64> {
        match self {
            ErrorCode::FundsInFlight => Some(1),
            _ => None,
        }
    }
}

/// The description a caller gets with [`ErrorCode::ChainUnavailable`].
///
/// Fixed rather than the read's own error, because that error is not the
/// caller's to see: it names the blockchain interface's address -- a node
/// inside the network, or WhatsOnChain's URL -- and the caller can do nothing
/// with it but retry, which the code already says. The cause is logged where
/// the response is built, for the operator who can act on it (SR-SEC-016).
pub const CHAIN_UNAVAILABLE: &str =
    "The blockchain could not be read to bring the wallet up to date. Retry shortly.";

#[derive(Serialize)]
pub struct ErrorResponse {
    pub code: ErrorCode,
    pub description: String,
}

/// An error carrying both its stable code and its human-readable description.
///
/// Threading this through the service layer keeps the code attached to the
/// condition that produced it, rather than re-deriving it by matching on the
/// description at the response boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodedError {
    pub code: ErrorCode,
    pub description: String,
}

impl CodedError {
    pub fn new(code: ErrorCode, description: impl Into<String>) -> Self {
        Self {
            code,
            description: description.into(),
        }
    }

    pub fn internal(description: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, description)
    }
}

impl std::fmt::Display for CodedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)
    }
}

/// One component's verdict inside a health response.
#[derive(Serialize)]
pub struct CheckResult {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl CheckResult {
    pub fn passed() -> Self {
        Self {
            ok: true,
            detail: None,
        }
    }

    pub fn failed(detail: impl Into<String>) -> Self {
        Self {
            ok: false,
            detail: Some(detail.into()),
        }
    }
}

/// Body of `GET /health` and `GET /ready`.
///
/// `/health` is liveness and always answers `{"status":"ok"}`. `/ready`
/// answers the same when mapi-lite is not configured; with mapi-lite the
/// probe's verdict is added under `mapi_lite`, and `status` becomes
/// `"unhealthy"` (with HTTP 503) when the probe fails.
#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mapi_lite: Option<CheckResult>,
    /// Chain-read health, on `GET /ready` (CS-505, SR-BCHN-017).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain: Option<CheckResult>,
}

impl HealthResponse {
    pub fn ok() -> Self {
        Self {
            status: "ok",
            mapi_lite: None,
            chain: None,
        }
    }

    /// A readiness answer from its checks: healthy only if every check that
    /// ran passed.
    pub fn from_checks(mapi_lite: Option<CheckResult>, chain: Option<CheckResult>) -> Self {
        let ok = [&mapi_lite, &chain]
            .into_iter()
            .flatten()
            .all(|check| check.ok);
        Self {
            status: if ok { "ok" } else { "unhealthy" },
            mapi_lite,
            chain,
        }
    }

    pub fn is_ok(&self) -> bool {
        self.status == "ok"
    }
}

#[derive(Serialize)]
pub struct SuccessResponse {
    pub status: &'static str,
}

impl SuccessResponse {
    pub fn new() -> Self {
        Self { status: "Success" }
    }
}

#[derive(Serialize)]
pub struct StatusResponse {
    pub version: String,
    pub blockchain_status: BlockchainConnectionStatus,
    pub blockchain_update_time: String,
    /// Where funding transactions are sent: the configured `interface_type`
    /// (`woc` in production) or `mapi-lite`.
    pub broadcaster: String,
    /// The longest a single-transaction `POST /fund` can take, in seconds,
    /// so a caller can set its own timeout above it (CS-624); `null` when
    /// chain reads have no deadline. See [`crate::config::Config::fund_worst_case`].
    pub fund_worst_case_seconds: Option<u64>,
}

#[derive(Serialize)]
pub struct AddressResponse {
    pub address: String,
}

#[derive(Serialize)]
pub struct BalanceResponse {
    pub confirmed: i64,
    pub unconfirmed: i64,
    /// The most a single `POST /fund` could ask for right now.
    ///
    /// Not `confirmed + unconfirmed`: fees come out of the same UTXOs, and
    /// every input a transaction has to spend adds to the fee, so how the
    /// balance is divided changes what it can pay out. Without this a caller
    /// has to guess a fee and the number of inputs, which is what the balance
    /// endpoint should be telling it.
    pub max_fundable: i64,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct OutpointResponse {
    pub hash: String,
    pub index: u32,
    /// Value of this output, read from the broadcast transaction.
    pub satoshi: i64,
    /// Locking script of this output as hex, read from the broadcast
    /// transaction.
    pub locking_script: String,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct TxResponse {
    pub tx: String,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct FundingResponseJson {
    pub outpoints: Vec<OutpointResponse>,
    pub txs: Vec<TxResponse>,
    /// True when this response is being replayed against a previously used
    /// `idempotency_key` rather than describing a fresh funding.
    ///
    /// Serialised only when true, so an ordinary funding response is
    /// unchanged; clients should read its absence as false. A client that
    /// expected fresh funds and receives `"replayed": true` is holding
    /// outpoints it has already been given, and very likely already spent --
    /// better to fail on that immediately than to sign over a spent output
    /// and get an opaque script error from the node.
    #[serde(skip_serializing_if = "is_false")]
    pub replayed: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Serialize)]
pub struct PartialFundingErrorResponse {
    pub code: ErrorCode,
    pub description: String,
    pub outpoints: Vec<OutpointResponse>,
    pub txs: Vec<TxResponse>,
    /// As [`FundingResponseJson::replayed`]. A replayed partial batch matters
    /// just as much: those transactions are on chain from the first call.
    #[serde(skip_serializing_if = "is_false")]
    pub replayed: bool,
}

pub fn partial_funding_error_response(
    description: impl Into<String>,
    funding: &FundingResponseJson,
) -> HttpResponse {
    let response = HttpResponse::build(ErrorCode::PartialBroadcast.status())
        .content_type(ContentType::json())
        .json(PartialFundingErrorResponse {
            code: ErrorCode::PartialBroadcast,
            description: description.into(),
            outpoints: funding.outpoints.clone(),
            txs: funding.txs.clone(),
            replayed: funding.replayed,
        });
    with_code(response, ErrorCode::PartialBroadcast)
}

/// Tag a response with the code its body carries, so `fs_fund_requests`
/// (CS-608) can count it without parsing the body back. Response extensions
/// never leave the process.
fn with_code(mut response: HttpResponse, code: ErrorCode) -> HttpResponse {
    response.extensions_mut().insert(code);
    response
}

/// The code an error response from this module was built with, if any.
pub fn response_code(response: &HttpResponse) -> Option<ErrorCode> {
    response.extensions().get::<ErrorCode>().copied()
}

pub fn error_response(code: ErrorCode, description: impl Into<String>) -> HttpResponse {
    error_response_with_status(code.status(), code, description)
}

/// Build an error response with a status other than the code's default.
///
/// Needed only for `UnknownClient`, which is a missing resource on the
/// `/client/{client_id}/...` routes but a malformed body on `POST /fund`.
pub fn error_response_with_status(
    status: StatusCode,
    code: ErrorCode,
    description: impl Into<String>,
) -> HttpResponse {
    let mut response = HttpResponse::build(status);
    response.content_type(ContentType::json());
    if let Some(seconds) = code.retry_after_seconds() {
        response.insert_header((header::RETRY_AFTER, seconds.to_string()));
    }
    let response = response.json(ErrorResponse {
        code,
        description: description.into(),
    });
    with_code(response, code)
}

/// Build an error response from an error that already carries its code.
pub fn coded_error_response(error: CodedError) -> HttpResponse {
    error_response(error.code, error.description)
}

pub fn unauthorized_response() -> HttpResponse {
    let response = HttpResponse::build(ErrorCode::Unauthorized.status())
        .content_type(ContentType::json())
        .json(ErrorResponse {
            code: ErrorCode::Unauthorized,
            description: "Unauthorized".to_string(),
        });
    with_code(response, ErrorCode::Unauthorized)
}

pub fn json_ok<T: Serialize>(value: &T) -> HttpResponse {
    HttpResponse::Ok()
        .content_type(ContentType::json())
        .json(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The serialised form of each code is the API contract. If a variant is
    /// renamed, this test fails rather than silently changing what clients see.
    #[test]
    fn error_codes_serialise_to_their_documented_strings() {
        let cases = [
            (ErrorCode::InsufficientBalance, "insufficient_balance"),
            (ErrorCode::NoSuitableUtxo, "no_suitable_utxo"),
            (ErrorCode::FundsInFlight, "funds_in_flight"),
            (ErrorCode::UnknownClient, "unknown_client"),
            (ErrorCode::ClientExists, "client_exists"),
            (ErrorCode::StaticClient, "static_client"),
            (ErrorCode::InvalidRequest, "invalid_request"),
            (ErrorCode::BroadcastFailed, "broadcast_failed"),
            (ErrorCode::PartialBroadcast, "partial_broadcast"),
            (ErrorCode::ChainUnavailable, "chain_unavailable"),
            (ErrorCode::Internal, "internal"),
            (ErrorCode::Unauthorized, "unauthorized"),
            (ErrorCode::RateLimited, "rate_limited"),
            (ErrorCode::KeyInProgress, "key_in_progress"),
            (ErrorCode::IdempotencyKeyReused, "idempotency_key_reused"),
        ];
        for (code, expected) in cases {
            assert_eq!(
                serde_json::to_value(code).unwrap(),
                serde_json::Value::String(expected.to_string()),
            );
        }
    }

    /// `as_str` is what `fs_fund_requests` labels with, so it must be the
    /// code a client is answered with -- for every variant.
    #[test]
    fn as_str_matches_the_serialised_code_for_every_variant() {
        let all = [
            ErrorCode::InsufficientBalance,
            ErrorCode::NoSuitableUtxo,
            ErrorCode::FundsInFlight,
            ErrorCode::UnknownClient,
            ErrorCode::ClientExists,
            ErrorCode::StaticClient,
            ErrorCode::InvalidRequest,
            ErrorCode::BroadcastFailed,
            ErrorCode::BroadcastRejected,
            ErrorCode::BroadcastOutcomeUnknown,
            ErrorCode::BroadcasterUntrusted,
            ErrorCode::PartialBroadcast,
            ErrorCode::ChainUnavailable,
            ErrorCode::Internal,
            ErrorCode::Unauthorized,
            ErrorCode::RateLimited,
            ErrorCode::KeyInProgress,
            ErrorCode::IdempotencyKeyReused,
        ];
        for code in all {
            assert_eq!(
                serde_json::to_value(code).unwrap(),
                serde_json::Value::String(code.as_str().to_string()),
            );
        }
    }

    #[test]
    fn error_responses_are_tagged_with_their_code() {
        assert_eq!(
            response_code(&error_response(ErrorCode::ChainUnavailable, "x")),
            Some(ErrorCode::ChainUnavailable)
        );
        assert_eq!(
            response_code(&unauthorized_response()),
            Some(ErrorCode::Unauthorized)
        );
        assert_eq!(response_code(&json_ok(&SuccessResponse::new())), None);
    }

    #[test]
    fn error_response_carries_code_and_description() {
        let response = error_response(ErrorCode::UnknownClient, "Unknown client_id id9");
        assert_eq!(response.status(), actix_http::StatusCode::NOT_FOUND);
    }

    /// The status for each code is part of the API, so pin the whole mapping.
    /// A caller may branch on the status without reading the body, and a
    /// change here would break that silently.
    #[test]
    fn each_error_code_maps_to_its_documented_status() {
        use actix_http::StatusCode as S;
        let cases = [
            (ErrorCode::InvalidRequest, S::BAD_REQUEST),
            (ErrorCode::UnknownClient, S::NOT_FOUND),
            (ErrorCode::ClientExists, S::CONFLICT),
            (ErrorCode::InsufficientBalance, S::CONFLICT),
            (ErrorCode::NoSuitableUtxo, S::CONFLICT),
            (ErrorCode::KeyInProgress, S::CONFLICT),
            (ErrorCode::IdempotencyKeyReused, S::CONFLICT),
            (ErrorCode::BroadcastFailed, S::BAD_GATEWAY),
            (ErrorCode::ChainUnavailable, S::SERVICE_UNAVAILABLE),
            (ErrorCode::FundsInFlight, S::SERVICE_UNAVAILABLE),
            (ErrorCode::Internal, S::INTERNAL_SERVER_ERROR),
            (ErrorCode::Unauthorized, S::UNAUTHORIZED),
            (ErrorCode::RateLimited, S::TOO_MANY_REQUESTS),
            (ErrorCode::PartialBroadcast, S::UNPROCESSABLE_ENTITY),
        ];
        for (code, expected) in cases {
            assert_eq!(code.status(), expected, "status for {code:?}");
        }
    }

    /// Retry semantics fall out of the split: 5xx is worth retrying
    /// unchanged, 4xx needs the caller or an operator to act.
    #[test]
    fn retryable_codes_are_5xx_and_client_errors_are_4xx() {
        for code in [
            ErrorCode::BroadcastFailed,
            ErrorCode::ChainUnavailable,
            ErrorCode::FundsInFlight,
        ] {
            assert!(code.status().is_server_error(), "{code:?} should be 5xx");
        }
        for code in [
            ErrorCode::InvalidRequest,
            ErrorCode::UnknownClient,
            ErrorCode::ClientExists,
            ErrorCode::IdempotencyKeyReused,
        ] {
            assert!(code.status().is_client_error(), "{code:?} should be 4xx");
        }
    }

    /// A caller that reads only headers can still learn how long to wait
    /// (CS-475), and no other code claims a wait it cannot promise.
    #[test]
    fn cs_475_funds_in_flight_says_when_to_retry() {
        let response = error_response(ErrorCode::FundsInFlight, "busy");
        assert_eq!(
            response.status(),
            actix_http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            response
                .headers()
                .get(header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );

        for code in [
            ErrorCode::InsufficientBalance,
            ErrorCode::NoSuitableUtxo,
            ErrorCode::ChainUnavailable,
        ] {
            let response = error_response(code, "no");
            assert!(
                response.headers().get(header::RETRY_AFTER).is_none(),
                "{code:?} promises no wait"
            );
        }
    }

    #[test]
    fn coded_error_response_preserves_the_originating_code() {
        let error = CodedError::new(ErrorCode::BroadcastFailed, "node unreachable");
        assert_eq!(error.code, ErrorCode::BroadcastFailed);
        assert_eq!(error.to_string(), "node unreachable");
        let response = coded_error_response(error);
        assert_eq!(response.status(), actix_http::StatusCode::BAD_GATEWAY);
    }

    /// The plain `/health` body is a contract with container orchestration,
    /// so the mapi-lite check must not leak into it when unconfigured.
    #[test]
    fn health_response_without_mapi_lite_is_exactly_status_ok() {
        assert_eq!(
            serde_json::to_value(HealthResponse::ok()).unwrap(),
            serde_json::json!({ "status": "ok" })
        );
    }

    #[test]
    fn health_response_with_mapi_lite_carries_the_check() {
        assert_eq!(
            serde_json::to_value(HealthResponse::from_checks(
                Some(CheckResult::passed()),
                None
            ))
            .unwrap(),
            serde_json::json!({ "status": "ok", "mapi_lite": { "ok": true } })
        );
        assert_eq!(
            serde_json::to_value(HealthResponse::from_checks(
                Some(CheckResult::failed("503")),
                None
            ))
            .unwrap(),
            serde_json::json!({
                "status": "unhealthy",
                "mapi_lite": { "ok": false, "detail": "503" }
            })
        );
    }

    /// Any failing check makes the whole answer unhealthy (CS-505).
    #[test]
    fn a_failing_chain_check_alone_makes_readiness_unhealthy() {
        let response = HealthResponse::from_checks(
            Some(CheckResult::passed()),
            Some(CheckResult::failed("old")),
        );
        assert!(!response.is_ok());
        assert_eq!(
            serde_json::to_value(response).unwrap(),
            serde_json::json!({
                "status": "unhealthy",
                "mapi_lite": { "ok": true },
                "chain": { "ok": false, "detail": "old" }
            })
        );
    }

    #[test]
    fn partial_funding_error_response_includes_successful_transactions() {
        let funding = FundingResponseJson {
            outpoints: vec![OutpointResponse {
                hash: "abc123".to_string(),
                index: 1,
                satoshi: 123,
                locking_script: "76a914ddc574807c3035ab43553a22c0b9df1f55737fae88ac".to_string(),
            }],
            txs: vec![TxResponse {
                tx: "010000".to_string(),
            }],
            replayed: false,
        };
        let response = partial_funding_error_response("partial failure", &funding);
        assert_eq!(
            response.status(),
            actix_http::StatusCode::UNPROCESSABLE_ENTITY
        );
    }
}
