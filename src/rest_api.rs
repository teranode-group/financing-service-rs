use actix_web::http::StatusCode;
use actix_web::{delete, get, post, web, HttpRequest, HttpResponse, Responder};
use log::debug;
use serde::Deserialize;
use std::sync::Arc;

use crate::{
    auth::{authorize_admin, authorize_known_client},
    client::FundRequest,
    client::MAX_SATOSHIS,
    config::ClientConfig,
    funding_limits::{Hold, LimitExceeded},
    idempotency::{
        request_fingerprint, Outcome, RecordKey, Reservation, MAX_IDEMPOTENCY_KEY_BYTES,
    },
    metrics::Metrics,
    responses::{
        coded_error_response, error_response, error_response_with_status, json_ok,
        partial_funding_error_response, AddressResponse, BalanceResponse, CheckResult, CodedError,
        ErrorCode, HealthResponse, SuccessResponse, CHAIN_UNAVAILABLE,
    },
    secrets::{secret_reference, validate_env_var_name},
    service::Service,
};

#[derive(Deserialize, Debug)]
pub struct FundingRequest {
    client_id: String,
    satoshi: u64,
    /// Required with `locking_script`. Ignored when `locking_scripts` is used,
    /// where the count comes from the list length; if given there it must
    /// agree with it.
    #[serde(default)]
    no_of_outpoints: Option<u32>,
    multiple_tx: bool,
    /// One script applied to every outpoint. Mutually exclusive with
    /// `locking_scripts`.
    #[serde(default)]
    locking_script: Option<String>,
    /// One script per outpoint, so each is independently spendable. Mutually
    /// exclusive with `locking_script`.
    #[serde(default)]
    locking_scripts: Option<Vec<String>>,
    /// Optional. When set, a retry carrying the same `idempotency_key` replays
    /// the response of the first call instead of funding again. Omitting it
    /// preserves the previous behaviour.
    ///
    /// This is unrelated to `wif_key` and `api_key` elsewhere in this service:
    /// it is an opaque string chosen by the client to name one funding
    /// request, not a credential or a signing key.
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct ClientAddRequest {
    client_id: String,
    #[serde(default)]
    wif: Option<String>,
    #[serde(default)]
    wif_env: Option<String>,
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    api_key_env: Option<String>,
}

impl ClientAddRequest {
    /// `wif_env_prefix` is `dynamic_config.wif_env_prefix`: the only variables
    /// a runtime client may take its key from (CS-521).
    fn into_client_config(self, wif_env_prefix: &str) -> Result<ClientConfig, String> {
        let wif_key = match (self.wif, self.wif_env) {
            // A literal WIF is stored as given, so an `env:` one would be read
            // as a reference to any variable at all -- the check below,
            // skipped.
            (Some(wif), None) if wif.starts_with(crate::secrets::ENV_PREFIX) => {
                return Err(
                    "wif is a key, not a reference; name a variable with wif_env".to_string(),
                )
            }
            (Some(wif), None) => wif,
            (None, Some(env_var)) => {
                validate_env_var_name(&env_var)?;
                if env_var.len() <= wif_env_prefix.len() || !env_var.starts_with(wif_env_prefix) {
                    return Err(format!(
                        "wif_env must name a variable starting with {wif_env_prefix:?}"
                    ));
                }
                secret_reference(&env_var)
            }
            (Some(_), Some(_)) => {
                return Err("Provide either wif or wif_env, not both".to_string());
            }
            (None, None) => return Err("Missing wif or wif_env".to_string()),
        };

        let api_key = match (self.api_key, self.api_key_env) {
            (None, None) => None,
            (Some(key), None) => Some(key),
            (None, Some(env_var)) => {
                validate_env_var_name(&env_var)?;
                Some(secret_reference(&env_var))
            }
            (Some(_), Some(_)) => {
                return Err("Provide either api_key or api_key_env, not both".to_string());
            }
        };

        Ok(ClientConfig {
            client_id: self.client_id,
            wif_key,
            api_key,
        })
    }
}

/// Application State Data
pub struct AppState {
    pub service: Arc<Service>,
    /// The CS-608 instruments; no-ops when metrics export is off.
    pub metrics: Metrics,
}

impl AppState {
    /// With instruments from the global meter provider, so build it after
    /// `telemetry::init`.
    pub fn new(service: Arc<Service>) -> Self {
        Self {
            service,
            metrics: Metrics::global(),
        }
    }
}

/// Get Index endpoint
#[get("/")]
pub async fn index(_data: web::Data<AppState>) -> String {
    "Financing Service REST API".to_string()
}

/// Liveness: is this process up and serving?
///
/// Always `{"status":"ok"}` whenever the process answers, independent of the
/// blockchain interface and of mapi-lite. That is deliberate, and it is what
/// makes this endpoint safe to wire to a liveness probe or the Docker
/// HEALTHCHECK: restarting the process cannot fix an upstream that is down,
/// and a restart loop would take `/status`, balances and addresses -- none of
/// which need mapi-lite -- down with it.
///
/// For "should traffic be sent here", use [`ready`].
#[get("/health")]
pub async fn health() -> impl Responder {
    json_ok(&HealthResponse::ok())
}

/// Readiness: can this service do its job right now?
///
/// It reports chain-read health always: HTTP 503 once a client's last
/// successful chain read is older than
/// `service.ready_max_chain_read_age_seconds` (CS-505). With mapi-lite
/// configured it also asks mapi-lite's own `GET /ready` and answers HTTP 503
/// when that fails, because a funding service whose only broadcast path is
/// unreachable should be taken out of rotation rather than handed requests it
/// will refuse.
///
/// Point a readiness probe here and a liveness probe at [`health`]. Sending
/// both at this endpoint reintroduces exactly the restart loop the split
/// exists to avoid.
///
/// The probe is bounded by [`crate::config::MAPI_LITE_PROBE_TIMEOUT`] and its
/// verdict cached for that long, so this unauthenticated, rate-limit-exempt endpoint
/// cannot be used to flood mapi-lite (see [`Service::mapi_lite_health`]).
///
/// The failure detail is logged, not returned: this endpoint answers anyone
/// who can reach the port, and an upstream transport error carries the
/// mapi-lite URL that a public 503 body has no business disclosing.
#[get("/ready")]
pub async fn ready(data: web::Data<AppState>) -> impl Responder {
    let response = readiness(&data.service, true).await;
    if response.is_ok() {
        json_ok(&response)
    } else {
        HttpResponse::ServiceUnavailable().json(response)
    }
}

/// The `GET /ready` verdict, shared with the `ready` gauge's sampler
/// (CS-608), which passes `log_failures = false` so sampling adds no log
/// lines of its own.
pub async fn readiness(service: &Service, log_failures: bool) -> HealthResponse {
    let mapi_lite = match service.mapi_lite_health().await {
        None => None,
        Some(Ok(())) => Some(CheckResult::passed()),
        Some(Err(error)) => {
            if log_failures {
                log::warn!("mapi-lite readiness probe failed: {error}");
            }
            Some(CheckResult::failed("mapi-lite probe failed"))
        }
    };
    // Its detail is an age and a client id, which a caller of /status can
    // already see; nothing in it names an upstream.
    let chain = match service.chain_readiness().await {
        Ok(()) => CheckResult::passed(),
        Err(detail) => {
            if log_failures {
                log::warn!("chain readiness failed: {detail}");
            }
            CheckResult::failed(detail)
        }
    };
    HealthResponse::from_checks(mapi_lite, Some(chain))
}

/// Get Service Status endpoint
#[get("/status")]
pub async fn status(data: web::Data<AppState>) -> impl Responder {
    log::info!("status");
    json_ok(&data.service.get_status().await)
}

/// Endpoint to update all the clients, called by ticker every minute
pub async fn update_clients(data: web::Data<AppState>) -> impl Responder {
    Service::refresh_balances(&data.service).await;
    HttpResponse::Ok().finish()
}

/// Post Fund endpoint
/// Example:
///     curl --header "Content-Type: application/json" \
///     --request POST \
///     --data '{"client_id":"id1","satoshi":123,"no_of_outpoints":1,"multiple_tx":false,"locking_script":"76a914...88ac"}' \
///
/// Or with one script per outpoint:
///     --data '{"client_id":"id1","satoshi":123,"multiple_tx":false,"locking_scripts":["76a914aa...88ac","76a914bb...88ac"]}' \
///    http://127.0.0.1:8080/fund

#[post("/fund")]
pub async fn get_funds(
    req: HttpRequest,
    data: web::Data<AppState>,
    info: web::Json<FundingRequest>,
) -> impl Responder {
    let response = fund(req, &data, info).await;
    // Every answer the handler gives, replays included (CS-608).
    data.metrics.record_fund(&response);
    response
}

async fn fund(
    req: HttpRequest,
    data: &web::Data<AppState>,
    info: web::Json<FundingRequest>,
) -> HttpResponse {
    log::info!("get_funds");

    let client_id = &info.client_id;
    let satoshi = info.satoshi;
    let multiple_tx = info.multiple_tx;

    if satoshi == 0 {
        return error_response(
            ErrorCode::InvalidRequest,
            format!("Invalid satoshi value '{satoshi}'"),
        );
    }

    // Who is asking is settled before anything sized by the request is
    // built (CS-519). Expanding the scripts first let an anonymous caller ask
    // for `no_of_outpoints` copies of a script -- 2^32 of them -- and the
    // allocation killed the process before the API key was ever looked at.
    // An unknown client_id is a 401 like a wrong key wherever clients have
    // keys, so it cannot be told from a real one (CS-661).
    let unknown = || {
        // 400 rather than the code's default 404: the client_id arrives in the
        // request body, so this is a malformed request, not a missing
        // resource. A 404 here would read as "/fund does not exist".
        error_response_with_status(
            StatusCode::BAD_REQUEST,
            ErrorCode::UnknownClient,
            format!("Unknown client_id {client_id}"),
        )
    };
    if let Some(response) = authorize_known_client(&req, client_id, &data.service, unknown).await {
        return response;
    }

    // Normalise both request forms into one script per outpoint, so the rest
    // of the service never has to care which was used.
    let (no_of_outpoints, locking_scripts) = match resolve_locking_scripts(&info) {
        Ok(resolved) => resolved,
        Err(error) => return coded_error_response(error),
    };
    let scripts_hex = info
        .locking_scripts
        .clone()
        .unwrap_or_else(|| vec![info.locking_script.clone().unwrap_or_default()]);

    // What this request pays out, fees aside. Checked rather than multiplied
    // (CS-530): in a release build the product wrapped, past the balance
    // check, into a transaction consensus then rejected.
    let Some(payout) = satoshi
        .checked_mul(u64::from(no_of_outpoints))
        .filter(|payout| *payout <= MAX_SATOSHIS)
    else {
        return error_response(
            ErrorCode::InvalidRequest,
            format!(
                "satoshi x no_of_outpoints is more than the {MAX_SATOSHIS} satoshis that exist"
            ),
        );
    };
    if info
        .idempotency_key
        .as_ref()
        .is_some_and(|key| key.len() > MAX_IDEMPOTENCY_KEY_BYTES)
    {
        return error_response(
            ErrorCode::InvalidRequest,
            format!("idempotency_key is longer than {MAX_IDEMPOTENCY_KEY_BYTES} bytes"),
        );
    }

    let fund_request = FundRequest {
        client_id: client_id.to_string(),
        satoshi,
        no_of_outpoints,
        multiple_tx,
        locking_scripts,
    };

    // Claim the `idempotency_key`, if the client supplied one, before anything
    // is prepared or broadcast. Reserving first is what makes a concurrent
    // duplicate visible rather than letting two calls both fund. It comes
    // before the chain read too (CS-654): a stored answer is replayed from the
    // record, and a resend after a lost answer is when the chain is most
    // likely to be unreadable -- an unknown outcome marks the cache unusable.
    let fingerprint = request_fingerprint(satoshi, no_of_outpoints, multiple_tx, &scripts_hex);
    let record = info
        .idempotency_key
        .as_deref()
        .map(|key| (RecordKey::new(client_id.clone(), key), fingerprint.clone()));

    if let Some((key, fingerprint)) = &record {
        match data
            .service
            .reserve_idempotency(key.clone(), fingerprint)
            .await
        {
            Reservation::Proceed => {}
            Reservation::Replay(outcome) => {
                log::info!("replaying funding response for idempotency_key");
                return replay(outcome);
            }
            Reservation::InProgress => {
                return error_response(
                    ErrorCode::KeyInProgress,
                    "A request with this idempotency_key is still being processed. Retry shortly.",
                );
            }
            Reservation::Reused => {
                return error_response(
                    ErrorCode::IdempotencyKeyReused,
                    "This idempotency_key was already used with a different request. Use a fresh idempotency_key.",
                );
            }
        }
    }

    // Refreshed only if the cache has gone stale, and with `[mapi_lite]` not
    // waited for unless the cache is unusable. See
    // `Service::prepare_chain_state_for_funding` for why, and
    // `service.chain_state_max_age_seconds` for the window. Nothing has been
    // built if it fails, so the key is given back.
    if let Err(description) =
        Service::prepare_chain_state_for_funding(&data.service, client_id).await
    {
        log::warn!("refresh_client_chain_state failed: {}", description);
        if let Some((key, _)) = &record {
            data.service.release_idempotency(key).await;
        }
        return error_response(ErrorCode::ChainUnavailable, CHAIN_UNAVAILABLE);
    }

    // `[funding_limits]` (CS-530). After the idempotency check, so a replayed
    // response is not counted again; the reservation is given back if the
    // request is refused here.
    let hold = match data.service.hold_funding(client_id, payout) {
        Ok(hold) => hold,
        Err(exceeded) => {
            if let Some((key, _)) = &record {
                data.service.release_idempotency(key).await;
            }
            log::warn!("funding limit for {client_id}: {}", exceeded.description());
            return match exceeded {
                LimitExceeded::PerRequest { .. } => {
                    error_response(ErrorCode::InvalidRequest, exceeded.description())
                }
                LimitExceeded::PerPeriod { retry_after, .. } => {
                    let mut response =
                        error_response(ErrorCode::RateLimited, exceeded.description());
                    response.headers_mut().insert(
                        actix_web::http::header::RETRY_AFTER,
                        actix_web::http::header::HeaderValue::from(retry_after.as_secs().max(1)),
                    );
                    response
                }
            };
        }
    };
    let record_and_hold = (record.clone(), hold);

    if fund_request.no_of_outpoints > 1 && fund_request.multiple_tx {
        return match Service::fund_with_multiple_transactions(&data.service, &fund_request).await {
            Ok(funding_response) => match funding_response.to_response() {
                Ok(response) => {
                    if let Some((key, fingerprint)) = &record {
                        data.service
                            .complete_idempotency(
                                key.clone(),
                                fingerprint,
                                Outcome::Funded(response.clone()),
                            )
                            .await;
                    }
                    json_ok(&response)
                }
                Err(description) => error_response(ErrorCode::Internal, description),
            },
            Err(error) => {
                debug!("fund_with_multiple_transactions error = {:?}", error);
                if let Some(partial) = error.partial {
                    match partial.to_response() {
                        Ok(response) => {
                            // Some transactions are on chain. Retain that so a
                            // retry is answered with what happened instead of
                            // funding the successful ones a second time.
                            if let Some((key, fingerprint)) = &record {
                                data.service
                                    .complete_idempotency(
                                        key.clone(),
                                        fingerprint,
                                        Outcome::PartiallyFunded {
                                            description: error.description.clone(),
                                            response: response.clone(),
                                        },
                                    )
                                    .await;
                            }
                            partial_funding_error_response(error.description, &response)
                        }
                        Err(description) => error_response(ErrorCode::Internal, description),
                    }
                } else {
                    release_if_nothing_was_spent(&data.service, &record_and_hold, error.code).await;
                    error_response(error.code, error.description)
                }
            }
        };
    }

    if let Some(error) = data.service.funding_balance_error(&fund_request).await {
        log::info!("insufficient funds: {}", error.description);
        release_if_nothing_was_spent(&data.service, &record_and_hold, error.code).await;
        return coded_error_response(error);
    }

    match Service::execute_funding(&data.service, &fund_request).await {
        Ok(funding_response) => match funding_response.to_response() {
            Ok(response) => {
                if let Some((key, fingerprint)) = &record {
                    data.service
                        .complete_idempotency(
                            key.clone(),
                            fingerprint,
                            Outcome::Funded(response.clone()),
                        )
                        .await;
                }
                json_ok(&response)
            }
            Err(description) => error_response(ErrorCode::Internal, description),
        },
        Err(error) => {
            debug!("execute_funding error = {:?}", error);
            release_if_nothing_was_spent(&data.service, &record_and_hold, error.code).await;
            coded_error_response(error)
        }
    }
}

/// The most outpoints one `/fund` may ask for (CS-519, SR-SEC-017).
///
/// Far above any real use -- Event asks for one, the documented examples for
/// three -- and low enough that the funding transaction, or the transactions
/// with `multiple_tx`, stay an ordinary size.
pub const MAX_OUTPOINTS: u32 = 1000;

/// The most locking-script bytes one `/fund` may ask to be paid to, summed
/// over its outpoints (CS-519, SR-SEC-017).
///
/// Bounds what the one-script form expands to: a single script repeated
/// `no_of_outpoints` times would otherwise be 1000 copies of whatever fits in
/// the request body. A mebibyte is some forty thousand P2PKH scripts, so the
/// outpoint limit is the one an ordinary request meets first.
pub const MAX_LOCKING_SCRIPT_BYTES: usize = 1024 * 1024;

/// Work out how many outpoints are wanted and the script for each.
///
/// Accepts either `locking_script` with `no_of_outpoints` (one script applied
/// to every outpoint, the original form) or `locking_scripts` (one per
/// outpoint). Exactly one of the two must be given.
///
/// Both limits are checked from the hex lengths before anything is decoded or
/// copied, so a request over them costs nothing to refuse (CS-519).
fn resolve_locking_scripts(info: &FundingRequest) -> Result<(u32, Vec<Vec<u8>>), CodedError> {
    let invalid = |message: String| CodedError::new(ErrorCode::InvalidRequest, message);
    let too_many = |count: usize| {
        invalid(format!(
            "no_of_outpoints is {count}; at most {MAX_OUTPOINTS} are allowed."
        ))
    };
    let check_script_bytes = |hex_len: usize, copies: usize| -> Result<(), CodedError> {
        // Hex is two characters a byte; a malformed odd length fails decoding.
        match (hex_len / 2).checked_mul(copies) {
            Some(total) if total <= MAX_LOCKING_SCRIPT_BYTES => Ok(()),
            _ => Err(invalid(format!(
                "the locking scripts total more than {MAX_LOCKING_SCRIPT_BYTES} bytes."
            ))),
        }
    };
    let decode = |hex_script: &str| -> Result<Vec<u8>, CodedError> {
        hex::decode(hex_script).map_err(|_| {
            invalid(format!(
                "Unable to convert locking_script to bytes '{hex_script}'"
            ))
        })
    };

    match (&info.locking_script, &info.locking_scripts) {
        (Some(_), Some(_)) => Err(invalid(
            "Provide either locking_script or locking_scripts, not both.".to_string(),
        )),
        (None, None) => Err(invalid(
            "One of locking_script or locking_scripts is required.".to_string(),
        )),
        (Some(script), None) => {
            let count = info.no_of_outpoints.unwrap_or(1);
            if count == 0 {
                return Err(invalid("Invalid no_of_outpoints value '0'".to_string()));
            }
            if count > MAX_OUTPOINTS {
                return Err(too_many(count as usize));
            }
            check_script_bytes(script.len(), count as usize)?;
            let bytes = decode(script)?;
            // The same script for every outpoint, as before. Note this makes
            // the outpoints spendable by one key, which is only what you want
            // when a single owner takes all of them.
            Ok((count, vec![bytes; count as usize]))
        }
        (None, Some(scripts)) => {
            if scripts.is_empty() {
                return Err(invalid(
                    "locking_scripts must contain at least one script.".to_string(),
                ));
            }
            if scripts.len() > MAX_OUTPOINTS as usize {
                return Err(too_many(scripts.len()));
            }
            check_script_bytes(scripts.iter().map(String::len).sum(), 1)?;
            let count = scripts.len() as u32;
            if let Some(stated) = info.no_of_outpoints {
                if stated != count {
                    return Err(invalid(format!(
                        "no_of_outpoints ({stated}) does not match the number of locking_scripts ({count})."
                    )));
                }
            }
            let mut decoded = Vec::with_capacity(scripts.len());
            for script in scripts {
                decoded.push(decode(script)?);
            }
            Ok((count, decoded))
        }
    }
}

/// Answer a retry with the outcome the first call produced.
fn replay(outcome: Outcome) -> HttpResponse {
    // Mark the response as replayed on the way out rather than storing it
    // that way, so the flag means "you are being served a retained response"
    // regardless of how many times it is asked for.
    match outcome {
        Outcome::Funded(mut response) => {
            response.replayed = true;
            json_ok(&response)
        }
        Outcome::PartiallyFunded {
            description,
            mut response,
        } => {
            response.replayed = true;
            partial_funding_error_response(description, &response)
        }
    }
}

/// Free the `idempotency_key` only when the failure cannot have spent
/// anything, so the client may retry with the same `idempotency_key`.
///
/// This is deliberately conservative. `insufficient_balance`,
/// `no_suitable_utxo` and `funds_in_flight` are decided before a transaction
/// is built, so releasing is safe -- and for `funds_in_flight` it is the point:
/// the caller is told to retry the same request, key and all, in a second. `broadcast_rejected` is safe for a different reason: the upstream
/// answered and refused the transaction, which is as definite as "nothing was
/// spent" gets -- and since that rejection will not change on a resubmission,
/// holding the key would only stop the client using it for the corrected
/// request. `broadcast_failed` is safe since CS-501: it now means the
/// transaction certainly did not reach a node -- a refused connection, a
/// mapi-lite `503`, or a refusal the node said was worth retrying -- because
/// every failure that may have delivered it is `broadcast_outcome_unknown`.
/// Its documented advice is "retry may succeed", and holding the key made
/// that retry impossible with the same key. `broadcaster_untrusted` is refused
/// before anything is sent (CS-618), so it spent nothing either; holding the
/// key and the funding-limit hold meant that, once an operator fixed mapi-lite's key, the
/// retry was refused `key_in_progress` for the key's TTL and the amount counted
/// against the client's window for its whole period (CS-660).
///
/// Everything else is left to expire. `broadcast_outcome_unknown` is unknown
/// by definition, and a commit failure means the transaction is definitely on
/// chain. The cost is that the same `idempotency_key` cannot be
/// retried until the TTL elapses; the alternative risks the duplicate this
/// whole mechanism exists to prevent.
async fn release_if_nothing_was_spent(
    service: &Arc<Service>,
    (record, hold): &(Option<(RecordKey, String)>, Option<Hold>),
    code: ErrorCode,
) {
    let certainly_pre_broadcast = matches!(
        code,
        ErrorCode::InsufficientBalance
            | ErrorCode::NoSuitableUtxo
            | ErrorCode::FundsInFlight
            | ErrorCode::BroadcastRejected
            | ErrorCode::BroadcastFailed
            | ErrorCode::BroadcasterUntrusted
    );
    if certainly_pre_broadcast {
        if let Some((key, _)) = record {
            service.release_idempotency(key).await;
        }
        // Nothing was paid out, so it does not count against the client's
        // `[funding_limits]` window (CS-530).
        if let Some(hold) = hold {
            service.release_funding(hold);
        }
    }
}

/// Add client
/// Example:
///     curl -H "Content-Type: application/json" \
///     --request POST \
///     --data '{"client_id":"client15","wif_env":"FS_RUNTIME_WIF_CLIENT15"}' \
///   http://127.0.0.1:8082/client
/// {"status": "Success"}
#[post("/client")]
pub async fn add_client(
    req: HttpRequest,
    data: web::Data<AppState>,
    info: web::Json<ClientAddRequest>,
) -> impl Responder {
    if let Some(response) = authorize_admin(&req, &data.service) {
        return response;
    }
    let client_config = match info
        .into_inner()
        .into_client_config(data.service.wif_env_prefix())
    {
        Ok(client_config) => client_config,
        Err(description) => return error_response(ErrorCode::InvalidRequest, description),
    };
    let client_id = &client_config.client_id;
    log::info!("add_client {}", client_id);

    if data.service.is_client_id_valid(client_id).await {
        return error_response(
            ErrorCode::ClientExists,
            format!("Client already exists: {client_id}"),
        );
    }

    match data.service.add_client(&client_config).await {
        Ok(()) => json_ok(&SuccessResponse::new()),
        Err(error) => coded_error_response(error),
    }
}

/// Delete a client added at runtime
/// Example:
///     curl -H "Authorization: Bearer <admin_api_key>" \
///     -X DELETE http://127.0.0.1:8080/client/client15
/// {"status": "Success"}
///
/// Takes the admin key, as `POST /client` does, not the client's own (CS-653):
/// a client's key funds that client, and must not be able to take it offline.
/// A client from the configuration file is refused with `static_client`.
#[delete("/client/{client_id}")]
pub async fn delete_client(
    req: HttpRequest,
    data: web::Data<AppState>,
    info: web::Path<String>,
) -> impl Responder {
    if let Some(response) = authorize_admin(&req, &data.service) {
        return response;
    }
    let client_id: String = info.to_string();
    log::info!("delete_client {}", client_id);

    match data.service.delete_client(&client_id).await {
        Ok(()) => json_ok(&SuccessResponse::new()),
        Err(error) => coded_error_response(error),
    }
}

/// Get Address for a particular client_id
#[get("/client/{client_id}/address")]
pub async fn get_address(
    req: HttpRequest,
    data: web::Data<AppState>,
    info: web::Path<String>,
) -> impl Responder {
    let client_id: String = info.to_string();
    log::info!("get address {}", client_id);

    // An unknown client_id is a 401 like a wrong key wherever clients have
    // keys, so it cannot be told from a real one (CS-661).
    let unknown = || {
        error_response(
            ErrorCode::UnknownClient,
            format!("Unknown client_id {client_id}"),
        )
    };
    if let Some(response) = authorize_known_client(&req, &client_id, &data.service, unknown).await {
        return response;
    }

    match data.service.get_address(&client_id).await {
        Some(address) => json_ok(&AddressResponse { address }),
        None => error_response(
            ErrorCode::UnknownClient,
            format!("Unknown client_id {client_id}"),
        ),
    }
}

/// Get Balance for a particular client_id endpoint
#[get("/client/{client_id}/balance")]
pub async fn balance(
    req: HttpRequest,
    data: web::Data<AppState>,
    info: web::Path<String>,
) -> impl Responder {
    let client_id: String = info.to_string();
    log::info!("get balance {}", client_id);

    // An unknown client_id is a 401 like a wrong key wherever clients have
    // keys, so it cannot be told from a real one (CS-661).
    let unknown = || {
        error_response(
            ErrorCode::UnknownClient,
            format!("Unknown client_id {client_id}"),
        )
    };
    if let Some(response) = authorize_known_client(&req, &client_id, &data.service, unknown).await {
        return response;
    }

    if let Err(description) =
        Service::refresh_client_chain_state_if_stale(&data.service, &client_id).await
    {
        log::warn!("refresh_client_chain_state failed: {}", description);
        return error_response(ErrorCode::ChainUnavailable, CHAIN_UNAVAILABLE);
    }

    match data.service.get_balance_and_max_fundable(&client_id).await {
        Some((balance, max_fundable)) => json_ok(&BalanceResponse {
            confirmed: balance.confirmed,
            unconfirmed: balance.unconfirmed,
            max_fundable,
        }),
        None => error_response(
            ErrorCode::UnknownClient,
            format!("Unknown client_id {client_id}"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use actix_governor::Governor;
    use actix_http::{body::BoxBody, Request};
    use actix_web::body::EitherBody;
    use actix_web::{dev::Service as ActixService, dev::ServiceResponse, Error};
    use actix_web::{http::StatusCode, test, web, App};
    use serde_json::{json, Value};
    use std::sync::Arc;

    use crate::{
        config::{ClientConfig, Config, RateLimitConfig},
        rate_limit,
        responses::CHAIN_UNAVAILABLE,
        rest_api::{
            add_client, balance, delete_client, get_address, get_funds, health, index, ready,
            status, AppState,
        },
        service::Service,
        test_support::{
            test_blockchain_interface, test_config, test_config_with_keys,
            unique_dynamic_config_path, CountingBlockchain, StubMapiBroadcaster,
            LOCKING_SCRIPT_HEX, OTHER_TEST_WIF, TEST_ADDRESS, TEST_CLIENT_ID, TEST_WIF,
        },
    };

    const TEST_API_KEY: &str = "test-secret-key";
    const TEST_ADMIN_API_KEY: &str = "test-admin-key";

    async fn build_app_with_rate_limit(
        burst_size: u32,
    ) -> impl ActixService<Request, Response = ServiceResponse<EitherBody<BoxBody>>, Error = Error>
    {
        let mut config = test_config_with_keys(&unique_dynamic_config_path(), None, None);
        config.web_interface.rate_limit = RateLimitConfig {
            enabled: true,
            requests_per_second: 1,
            burst_size: Some(burst_size),
            trusted_proxies: Vec::new(),
        };
        let governor_config = rate_limit::build_governor_config(&config.web_interface.rate_limit);
        let blockchain = test_blockchain_interface(&config).await;
        let financing_service = Arc::new(Service::new_for_test(&config, blockchain).await);
        let app_state = web::Data::new(AppState::new(financing_service));

        test::init_service(
            App::new()
                .wrap(Governor::new(&governor_config))
                .app_data(app_state)
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
    }

    fn peer_addr() -> std::net::SocketAddr {
        "127.0.0.1:8080".parse().expect("valid peer address")
    }

    async fn build_app_with_keys(
        client_api_key: Option<&str>,
        admin_api_key: Option<&str>,
    ) -> impl ActixService<Request, Response = ServiceResponse, Error = Error> {
        let config =
            test_config_with_keys(&unique_dynamic_config_path(), client_api_key, admin_api_key);
        let blockchain = test_blockchain_interface(&config).await;
        let financing_service = Arc::new(Service::new_for_test(&config, blockchain).await);
        let app_state = web::Data::new(AppState::new(financing_service));

        test::init_service(
            App::new()
                .app_data(app_state)
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
    }

    /// Build an app whose blockchain counts broadcasts, so a test can assert
    /// that a retry did not reach the chain.
    async fn build_counting_app() -> (
        impl ActixService<Request, Response = ServiceResponse, Error = Error>,
        Arc<CountingBlockchain>,
    ) {
        let config = test_config(&unique_dynamic_config_path());
        let blockchain = CountingBlockchain::new(&config).await;
        let financing_service = Arc::new(Service::new_for_test(&config, blockchain.clone()).await);
        let app_state = web::Data::new(AppState::new(financing_service));

        let app = test::init_service(
            App::new()
                .app_data(app_state)
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
        .await;
        (app, blockchain)
    }

    /// Build an app that broadcasts through a stand-in for mapi-lite, so the
    /// health and status endpoints can be exercised in both of its states.
    async fn build_app_with_mapi_lite(
        healthy: bool,
    ) -> (
        impl ActixService<Request, Response = ServiceResponse, Error = Error>,
        Arc<StubMapiBroadcaster>,
    ) {
        let config = test_config(&unique_dynamic_config_path());
        let blockchain = test_blockchain_interface(&config).await;
        let stub = StubMapiBroadcaster::new(healthy);
        let service =
            Service::new_for_test_with_broadcaster(&config, blockchain, stub.clone()).await;
        let app_state = web::Data::new(AppState::new(Arc::new(service)));

        let app = test::init_service(
            App::new()
                .app_data(app_state)
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
        .await;
        (app, stub)
    }

    async fn build_app_with_api_key(
        api_key: Option<&str>,
    ) -> impl ActixService<Request, Response = ServiceResponse, Error = Error> {
        build_app_with_keys(api_key, None).await
    }

    async fn build_app_with_admin_key(
        admin_api_key: Option<&str>,
    ) -> impl ActixService<Request, Response = ServiceResponse, Error = Error> {
        build_app_with_keys(None, admin_api_key).await
    }

    async fn build_app() -> impl ActixService<Request, Response = ServiceResponse, Error = Error> {
        build_app_with_keys(None, None).await
    }

    async fn build_app_with_service(
        config: Config,
    ) -> (
        impl ActixService<Request, Response = ServiceResponse, Error = Error>,
        Arc<Service>,
    ) {
        let blockchain = test_blockchain_interface(&config).await;
        let service = Arc::new(Service::new_for_test(&config, blockchain).await);
        let app_state = web::Data::new(AppState::new(Arc::clone(&service)));
        let app = test::init_service(
            App::new()
                .app_data(app_state)
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
        .await;
        (app, service)
    }

    /// An app whose broadcaster reports every submit as an unknown outcome,
    /// as a mapi-lite whose deadline expires does.
    async fn build_app_with_uncertain_broadcaster() -> (
        impl ActixService<Request, Response = ServiceResponse, Error = Error>,
        Arc<Service>,
    ) {
        build_app_broadcasting_through(crate::test_support::UncertainBroadcaster::new()).await
    }

    /// An app whose upstream refuses every transaction, saying whether a
    /// resubmission could succeed.
    async fn build_app_with_rejecting_broadcaster(
        retryable: bool,
    ) -> (
        impl ActixService<Request, Response = ServiceResponse, Error = Error>,
        Arc<Service>,
    ) {
        build_app_broadcasting_through(crate::test_support::RejectingBroadcaster::new(retryable))
            .await
    }

    async fn build_app_broadcasting_through(
        broadcaster: Arc<dyn crate::broadcaster::TxBroadcaster>,
    ) -> (
        impl ActixService<Request, Response = ServiceResponse, Error = Error>,
        Arc<Service>,
    ) {
        let config = test_config(&unique_dynamic_config_path());
        let blockchain = test_blockchain_interface(&config).await;
        build_app_over(&config, blockchain, broadcaster).await
    }

    async fn build_app_over(
        config: &Config,
        blockchain: Arc<dyn chain_gang::interface::BlockchainInterface + Send + Sync>,
        broadcaster: Arc<dyn crate::broadcaster::TxBroadcaster>,
    ) -> (
        impl ActixService<Request, Response = ServiceResponse, Error = Error>,
        Arc<Service>,
    ) {
        let service =
            Arc::new(Service::new_for_test_with_broadcaster(config, blockchain, broadcaster).await);
        let app_state = web::Data::new(AppState::new(Arc::clone(&service)));
        let app = test::init_service(
            App::new()
                .app_data(app_state)
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
        .await;
        (app, service)
    }

    fn fund_body(
        client_id: &str,
        satoshi: u64,
        no_of_outpoints: u32,
        locking_script: &str,
    ) -> Value {
        json!({
            "client_id": client_id,
            "satoshi": satoshi,
            "no_of_outpoints": no_of_outpoints,
            "multiple_tx": false,
            "locking_script": locking_script,
        })
    }

    #[actix_web::test]
    async fn test_index() {
        let app = build_app().await;
        let resp = test::call_service(&app, test::TestRequest::get().uri("/").to_request()).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = test::read_body(resp).await;
        assert_eq!(body, "Financing Service REST API");
    }

    #[actix_web::test]
    async fn test_health() {
        let app = build_app().await;
        let resp =
            test::call_service(&app, test::TestRequest::get().uri("/health").to_request()).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["status"], "ok");
        // Without mapi-lite the body is exactly what it always was.
        assert_eq!(body, json!({ "status": "ok" }));
    }

    #[actix_web::test]
    async fn sr_bchn_011_ready_reports_mapi_lite_ok_when_it_is_reachable() {
        let (app, _broadcaster) = build_app_with_mapi_lite(true).await;
        let resp =
            test::call_service(&app, test::TestRequest::get().uri("/ready").to_request()).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["status"], "ok");
        assert_eq!(body["mapi_lite"]["ok"], true);
    }

    #[actix_web::test]
    async fn sr_bchn_011_ready_returns_503_when_mapi_lite_is_unreachable() {
        let (app, _broadcaster) = build_app_with_mapi_lite(false).await;
        let resp =
            test::call_service(&app, test::TestRequest::get().uri("/ready").to_request()).await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["status"], "unhealthy");
        assert_eq!(body["mapi_lite"]["ok"], false);
        // The detail is generic. /ready answers unauthenticated callers, and
        // the upstream error it replaces carries the mapi-lite URL.
        let detail = body["mapi_lite"]["detail"]
            .as_str()
            .expect("a detail string");
        assert_eq!(detail, "mapi-lite probe failed");
    }

    /// The property this split exists to protect. A liveness probe that fails
    /// while mapi-lite is down gets the process killed and restarted, which
    /// cannot fix a broken upstream and costs the service its in-memory state.
    /// So /health must answer 200 even when the very same configuration makes
    /// /ready answer 503.
    #[actix_web::test]
    async fn sr_bchn_012_health_stays_up_when_mapi_lite_is_unreachable() {
        let (app, _broadcaster) = build_app_with_mapi_lite(false).await;
        let resp =
            test::call_service(&app, test::TestRequest::get().uri("/health").to_request()).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;
        // Liveness says nothing about upstreams, so the body carries no
        // mapi_lite field at all -- only that this process is running.
        assert_eq!(body, json!({ "status": "ok" }));
    }

    /// Without `[mapi_lite]` there is no mapi-lite to probe, so readiness
    /// reports chain reads alone (CS-505). It used to be the same answer as
    /// liveness, which said nothing a liveness probe did not.
    #[actix_web::test]
    async fn sr_bchn_012_ready_reports_chain_reads_without_mapi_lite() {
        let app = build_app().await;
        let resp =
            test::call_service(&app, test::TestRequest::get().uri("/ready").to_request()).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body, json!({ "status": "ok", "chain": { "ok": true } }));
    }

    /// SR-BCHN-017: chain reads older than the bound take the service out of
    /// rotation -- with `[mapi_lite]` it would otherwise go on funding from an
    /// ever older cache -- and liveness is untouched (SR-BCHN-004).
    #[actix_web::test]
    async fn sr_bchn_017_ready_returns_503_when_chain_reads_are_stale() {
        let config = test_config(&unique_dynamic_config_path());
        let (app, service) = build_app_with_service(config).await;
        service
            .backdate_chain_reads(std::time::Duration::from_secs(61))
            .await;

        let resp =
            test::call_service(&app, test::TestRequest::get().uri("/ready").to_request()).await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["status"], "unhealthy");
        assert_eq!(body["chain"]["ok"], false);
        assert!(body["chain"]["detail"]
            .as_str()
            .unwrap()
            .contains("last read"));

        let resp =
            test::call_service(&app, test::TestRequest::get().uri("/health").to_request()).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[actix_web::test]
    async fn test_status() {
        let app = build_app().await;
        let resp =
            test::call_service(&app, test::TestRequest::get().uri("/status").to_request()).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
        assert!(body["blockchain_status"].is_string());
        // The test config's interface_type, which is what broadcasts go
        // through when mapi-lite is not configured.
        assert_eq!(body["broadcaster"], "test");
        // Three chain reads and the broadcast, each one interface call at
        // the default 30s (CS-624).
        assert_eq!(body["fund_worst_case_seconds"], 120);
    }

    #[actix_web::test]
    async fn sr_bchn_009_status_names_mapi_lite_and_fund_broadcasts_through_it() {
        let (app, broadcaster) = build_app_with_mapi_lite(true).await;
        let resp =
            test::call_service(&app, test::TestRequest::get().uri("/status").to_request()).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["broadcaster"], "mapi-lite");

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(fund_body(TEST_CLIENT_ID, 123, 1, LOCKING_SCRIPT_HEX))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(broadcaster.broadcast_count(), 1);
    }

    #[actix_web::test]
    async fn test_balance_unknown_client() {
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/client/unknown/balance")
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["code"], "unknown_client");
        assert!(body["description"]
            .as_str()
            .unwrap()
            .contains("Unknown client_id"));
    }

    #[actix_web::test]
    async fn test_balance_success() {
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/client/{TEST_CLIENT_ID}/balance"))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;
        assert!(body["confirmed"].as_i64().unwrap() > 0);
    }

    #[actix_web::test]
    async fn test_address_success() {
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/client/{TEST_CLIENT_ID}/address"))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["address"], TEST_ADDRESS);
    }

    #[actix_web::test]
    async fn test_address_unknown_client() {
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/client/unknown/address")
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[actix_web::test]
    async fn test_add_client_with_wif_env() {
        unsafe { std::env::set_var("FS_RUNTIME_WIF_TEST_ADD_CLIENT", OTHER_TEST_WIF) };
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/client")
                .set_json(json!({
                    "client_id": "client_env",
                    "wif_env": "FS_RUNTIME_WIF_TEST_ADD_CLIENT",
                }))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["status"], "Success");
    }

    #[actix_web::test]
    async fn test_add_client_success() {
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/client")
                .set_json(json!({
                    "client_id": "client2",
                    "wif": OTHER_TEST_WIF,
                }))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["status"], "Success");
    }

    #[actix_web::test]
    async fn test_add_client_duplicate() {
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/client")
                .set_json(json!({
                    "client_id": TEST_CLIENT_ID,
                    "wif": OTHER_TEST_WIF,
                }))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(resp).await;
        assert!(body["description"]
            .as_str()
            .unwrap()
            .contains("already exists"));
    }

    #[actix_web::test]
    async fn test_add_client_invalid_wif() {
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/client")
                .set_json(json!({
                    "client_id": "client3",
                    "wif": "not-a-valid-wif",
                }))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body: Value = test::read_body_json(resp).await;
        assert!(body["description"]
            .as_str()
            .unwrap()
            .contains("not a valid WIF key"));
    }

    #[actix_web::test]
    async fn test_delete_client_success() {
        let (app, service) =
            build_app_with_service(test_config(&unique_dynamic_config_path())).await;
        service
            .add_client(&ClientConfig {
                client_id: "client2".to_string(),
                wif_key: OTHER_TEST_WIF.to_string(),
                api_key: None,
            })
            .await
            .expect("add client2");

        let resp = test::call_service(
            &app,
            test::TestRequest::delete()
                .uri("/client/client2")
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["status"], "Success");
        assert!(!service.is_client_id_valid("client2").await);
    }

    #[actix_web::test]
    async fn test_delete_client_unknown() {
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::delete()
                .uri("/client/unknown")
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[actix_web::test]
    async fn test_fund_unknown_client() {
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(fund_body("unknown", 123, 1, LOCKING_SCRIPT_HEX))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body: Value = test::read_body_json(resp).await;
        assert!(body["description"]
            .as_str()
            .unwrap()
            .contains("Unknown client_id"));
    }

    #[actix_web::test]
    async fn test_fund_invalid_satoshi() {
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(fund_body(TEST_CLIENT_ID, 0, 1, LOCKING_SCRIPT_HEX))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["code"], "invalid_request");
        assert!(body["description"]
            .as_str()
            .unwrap()
            .contains("Invalid satoshi"));
    }

    #[actix_web::test]
    async fn test_fund_invalid_no_of_outpoints() {
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(fund_body(TEST_CLIENT_ID, 123, 0, LOCKING_SCRIPT_HEX))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["code"], "invalid_request");
        assert!(body["description"]
            .as_str()
            .unwrap()
            .contains("Invalid no_of_outpoints"));
    }

    #[actix_web::test]
    async fn test_fund_invalid_locking_script() {
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(fund_body(TEST_CLIENT_ID, 123, 1, "not-hex"))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["code"], "invalid_request");
        assert!(body["description"]
            .as_str()
            .unwrap()
            .contains("locking_script"));
    }

    #[actix_web::test]
    async fn test_fund_insufficient_balance() {
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(fund_body(
                    TEST_CLIENT_ID,
                    100_000_000,
                    1,
                    LOCKING_SCRIPT_HEX,
                ))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["code"], "insufficient_balance");
        assert!(body["description"]
            .as_str()
            .unwrap()
            .contains("Insufficient client balance"));
    }

    #[actix_web::test]
    async fn test_fund_consolidates_multiple_utxos() {
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(fund_body(TEST_CLIENT_ID, 50_000_000, 1, LOCKING_SCRIPT_HEX))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["txs"].as_array().unwrap().len(), 1);
    }

    #[actix_web::test]
    async fn test_fund_multiple_tx_success() {
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(json!({
                    "client_id": TEST_CLIENT_ID,
                    "satoshi": 123,
                    "no_of_outpoints": 2,
                    "multiple_tx": true,
                    "locking_script": LOCKING_SCRIPT_HEX,
                }))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["txs"].as_array().unwrap().len(), 2);
        assert_eq!(body["outpoints"].as_array().unwrap().len(), 2);
        // With multiple_tx each outpoint comes from a different transaction,
        // so this also covers the value and script being matched to the right
        // one rather than to the first in the list.
        for outpoint in body["outpoints"].as_array().unwrap() {
            assert_eq!(outpoint["satoshi"], 123);
            assert_eq!(outpoint["locking_script"], LOCKING_SCRIPT_HEX);
        }
    }

    fn fund_body_with_key(
        client_id: &str,
        satoshi: u64,
        locking_script: &str,
        idempotency_key: &str,
    ) -> Value {
        json!({
            "client_id": client_id,
            "satoshi": satoshi,
            "no_of_outpoints": 1,
            "multiple_tx": false,
            "locking_script": locking_script,
            "idempotency_key": idempotency_key,
        })
    }

    async fn post_fund(
        app: &impl actix_web::dev::Service<
            actix_http::Request,
            Response = actix_web::dev::ServiceResponse,
            Error = actix_web::Error,
        >,
        body: Value,
    ) -> (StatusCode, Value) {
        let resp = test::call_service(
            app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(body)
                .to_request(),
        )
        .await;
        let http_status = resp.status();
        (http_status, test::read_body_json(resp).await)
    }

    /// The case #39 describes: the funding transaction is on chain but the
    /// response was lost, so the client retries. The retry must replay the
    /// first response and must not broadcast again.
    ///
    /// Asserted by broadcast count rather than by comparing responses:
    /// `TestInterface` does not consume UTXOs, so funding twice would produce
    /// an identical transaction and a response comparison would pass even with
    /// idempotency removed.
    #[actix_web::test]
    async fn sr_fund_retry_with_same_idempotency_key_does_not_broadcast_again() {
        let (app, chain) = build_counting_app().await;
        let body = fund_body_with_key(TEST_CLIENT_ID, 123, LOCKING_SCRIPT_HEX, "key-1");

        let (first_status, first) = post_fund(&app, body.clone()).await;
        assert_eq!(first_status, StatusCode::OK);
        assert_eq!(chain.broadcast_count(), 1);

        let (retry_status, retry) = post_fund(&app, body).await;
        assert_eq!(retry_status, StatusCode::OK);
        assert_eq!(
            chain.broadcast_count(),
            1,
            "the retry must not have broadcast a second funding transaction"
        );
        assert_eq!(first["outpoints"], retry["outpoints"]);
        assert_eq!(first["txs"], retry["txs"]);

        // The replay is marked, so a client that expected fresh funds can
        // fail on it instead of spending an outpoint it already spent.
        assert_eq!(retry["replayed"], true);
        // and the first response carries no marker at all
        assert!(
            first.get("replayed").is_none(),
            "an ordinary funding response should not carry the marker"
        );
    }

    /// The marker is absent rather than false on a normal response, so today's
    /// response shape is unchanged for clients that do not care about it.
    #[actix_web::test]
    async fn a_normal_funding_response_carries_no_replayed_marker() {
        let app = build_app().await;
        let (_, body) = post_fund(
            &app,
            fund_body_with_key(TEST_CLIENT_ID, 123, LOCKING_SCRIPT_HEX, "key-marker"),
        )
        .await;
        assert!(body.get("replayed").is_none());
    }

    /// A response replayed without any `idempotency_key` involved cannot
    /// happen, but a response served without one must never be marked either.
    #[actix_web::test]
    async fn funding_without_an_idempotency_key_carries_no_marker() {
        let app = build_app().await;
        let (_, body) =
            post_fund(&app, fund_body(TEST_CLIENT_ID, 123, 1, LOCKING_SCRIPT_HEX)).await;
        assert!(body.get("replayed").is_none());
    }

    /// Without an `idempotency_key` the previous behaviour stands: every call
    /// funds, which is the duplicate #39 reports. This is the control for the
    /// test above.
    #[actix_web::test]
    async fn fund_without_an_idempotency_key_broadcasts_every_time() {
        let (app, chain) = build_counting_app().await;
        let body = fund_body(TEST_CLIENT_ID, 123, 1, LOCKING_SCRIPT_HEX);

        post_fund(&app, body.clone()).await;
        assert_eq!(chain.broadcast_count(), 1);
        post_fund(&app, body).await;
        assert_eq!(chain.broadcast_count(), 2);
    }

    #[actix_web::test]
    async fn reusing_an_idempotency_key_with_a_different_request_is_rejected() {
        let app = build_app().await;
        let (first_status, _) = post_fund(
            &app,
            fund_body_with_key(TEST_CLIENT_ID, 123, LOCKING_SCRIPT_HEX, "key-2"),
        )
        .await;
        assert_eq!(first_status, StatusCode::OK);

        // same idempotency_key, different satoshi value
        let (http_status, body) = post_fund(
            &app,
            fund_body_with_key(TEST_CLIENT_ID, 456, LOCKING_SCRIPT_HEX, "key-2"),
        )
        .await;
        assert_eq!(http_status, StatusCode::CONFLICT);
        assert_eq!(body["code"], "idempotency_key_reused");
    }

    /// A refusal decided before any transaction is built must free the
    /// `idempotency_key`, so the client can retry it once the wallet is topped
    /// up.
    #[actix_web::test]
    async fn a_pre_broadcast_refusal_frees_the_idempotency_key() {
        let app = build_app().await;
        let (http_status, body) = post_fund(
            &app,
            fund_body_with_key(TEST_CLIENT_ID, 100_000_000, LOCKING_SCRIPT_HEX, "key-3"),
        )
        .await;
        assert_eq!(http_status, StatusCode::CONFLICT);
        assert_eq!(body["code"], "insufficient_balance");

        // the idempotency_key is free again, so a viable request with it now succeeds
        let (retry_status, _) = post_fund(
            &app,
            fund_body_with_key(TEST_CLIENT_ID, 123, LOCKING_SCRIPT_HEX, "key-3"),
        )
        .await;
        assert_eq!(retry_status, StatusCode::OK);
    }

    #[actix_web::test]
    async fn different_idempotency_keys_each_broadcast() {
        let (app, chain) = build_counting_app().await;
        post_fund(
            &app,
            fund_body_with_key(TEST_CLIENT_ID, 123, LOCKING_SCRIPT_HEX, "key-4a"),
        )
        .await;
        post_fund(
            &app,
            fund_body_with_key(TEST_CLIENT_ID, 123, LOCKING_SCRIPT_HEX, "key-4b"),
        )
        .await;
        assert_eq!(chain.broadcast_count(), 2);
    }

    const SCRIPT_A: &str = "76a914aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa88ac";
    const SCRIPT_B: &str = "76a914bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb88ac";

    fn decode_tx(tx_hex: &str) -> chain_gang::messages::Tx {
        use chain_gang::messages::Tx;
        use chain_gang::util::Serializable;
        Tx::read(&mut std::io::Cursor::new(hex::decode(tx_hex).unwrap())).unwrap()
    }

    /// The point of #40: N outpoints from one request, each locked to a
    /// different script, in a single transaction.
    #[actix_web::test]
    async fn sr_fund_locking_scripts_gives_each_outpoint_its_own_script() {
        let app = build_app().await;
        let (http_status, body) = post_fund(
            &app,
            json!({
                "client_id": TEST_CLIENT_ID,
                "satoshi": 123,
                "multiple_tx": false,
                "locking_scripts": [SCRIPT_A, SCRIPT_B],
            }),
        )
        .await;
        assert_eq!(http_status, StatusCode::OK);
        assert_eq!(body["outpoints"].as_array().unwrap().len(), 2);
        // one transaction, not two
        assert_eq!(body["txs"].as_array().unwrap().len(), 1);

        let tx = decode_tx(body["txs"][0]["tx"].as_str().unwrap());
        let scripts: Vec<String> = body["outpoints"]
            .as_array()
            .unwrap()
            .iter()
            .map(|op| {
                let out_index = op["index"].as_u64().unwrap() as usize;
                hex::encode(&tx.outputs[out_index].lock_script.0)
            })
            .collect();
        assert_eq!(scripts, vec![SCRIPT_A.to_string(), SCRIPT_B.to_string()]);
    }

    /// The original form still applies one script to every outpoint.
    #[actix_web::test]
    async fn fund_with_one_locking_script_still_applies_it_to_every_outpoint() {
        let app = build_app().await;
        let (http_status, body) = post_fund(
            &app,
            json!({
                "client_id": TEST_CLIENT_ID,
                "satoshi": 123,
                "no_of_outpoints": 2,
                "multiple_tx": false,
                "locking_script": SCRIPT_A,
            }),
        )
        .await;
        assert_eq!(http_status, StatusCode::OK);
        let tx = decode_tx(body["txs"][0]["tx"].as_str().unwrap());
        for op in body["outpoints"].as_array().unwrap() {
            let out_index = op["index"].as_u64().unwrap() as usize;
            assert_eq!(hex::encode(&tx.outputs[out_index].lock_script.0), SCRIPT_A);
        }
    }

    #[actix_web::test]
    async fn fund_rejects_both_locking_script_forms_together() {
        let app = build_app().await;
        let (http_status, body) = post_fund(
            &app,
            json!({
                "client_id": TEST_CLIENT_ID,
                "satoshi": 123,
                "no_of_outpoints": 1,
                "multiple_tx": false,
                "locking_script": SCRIPT_A,
                "locking_scripts": [SCRIPT_A],
            }),
        )
        .await;
        assert_eq!(http_status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "invalid_request");
        assert!(body["description"].as_str().unwrap().contains("not both"));
    }

    #[actix_web::test]
    async fn fund_rejects_neither_locking_script_form() {
        let app = build_app().await;
        let (http_status, body) = post_fund(
            &app,
            json!({
                "client_id": TEST_CLIENT_ID,
                "satoshi": 123,
                "no_of_outpoints": 1,
                "multiple_tx": false,
            }),
        )
        .await;
        assert_eq!(http_status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "invalid_request");
    }

    #[actix_web::test]
    async fn fund_rejects_an_empty_locking_scripts_list() {
        let app = build_app().await;
        let (http_status, body) = post_fund(
            &app,
            json!({
                "client_id": TEST_CLIENT_ID,
                "satoshi": 123,
                "multiple_tx": false,
                "locking_scripts": [],
            }),
        )
        .await;
        assert_eq!(http_status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "invalid_request");
    }

    /// A stated count that disagrees with the list is a mistake worth
    /// reporting rather than silently preferring one of them.
    #[actix_web::test]
    async fn fund_rejects_no_of_outpoints_disagreeing_with_locking_scripts() {
        let app = build_app().await;
        let (http_status, body) = post_fund(
            &app,
            json!({
                "client_id": TEST_CLIENT_ID,
                "satoshi": 123,
                "no_of_outpoints": 3,
                "multiple_tx": false,
                "locking_scripts": [SCRIPT_A, SCRIPT_B],
            }),
        )
        .await;
        assert_eq!(http_status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "invalid_request");
        assert!(body["description"]
            .as_str()
            .unwrap()
            .contains("does not match"));
    }

    /// With multiple_tx each transaction pays its own script.
    #[actix_web::test]
    async fn fund_multiple_tx_pays_each_script_from_its_own_transaction() {
        let (app, chain) = build_counting_app().await;
        let (http_status, body) = post_fund(
            &app,
            json!({
                "client_id": TEST_CLIENT_ID,
                "satoshi": 123,
                "multiple_tx": true,
                "locking_scripts": [SCRIPT_A, SCRIPT_B],
            }),
        )
        .await;
        assert_eq!(http_status, StatusCode::OK);
        assert_eq!(body["txs"].as_array().unwrap().len(), 2);
        assert_eq!(chain.broadcast_count(), 2);

        let mut seen: Vec<String> = body["txs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| {
                let tx = decode_tx(t["tx"].as_str().unwrap());
                // The funded outputs are last. There is not always a change
                // output at index 0 -- when the change would be dust it is
                // folded into the fee instead (CS-452).
                hex::encode(&tx.outputs.last().expect("an output").lock_script.0)
            })
            .collect();
        seen.sort();
        let mut expected = vec![SCRIPT_A.to_string(), SCRIPT_B.to_string()];
        expected.sort();
        assert_eq!(seen, expected);
    }

    #[actix_web::test]
    async fn test_fund_success() {
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(fund_body(TEST_CLIENT_ID, 123, 1, LOCKING_SCRIPT_HEX))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["outpoints"].as_array().unwrap().len(), 1);
        assert_eq!(body["txs"].as_array().unwrap().len(), 1);
        assert!(!body["outpoints"][0]["hash"].as_str().unwrap().is_empty());
        assert_eq!(body["outpoints"][0]["satoshi"], 123);
        assert_eq!(body["outpoints"][0]["locking_script"], LOCKING_SCRIPT_HEX);
    }

    /// The point of returning `satoshi` and `locking_script` is that a client
    /// can verify what was paid rather than assume the request was honoured.
    /// So assert they agree with the transaction actually broadcast, decoded
    /// from the response, rather than merely echoing the request.
    #[actix_web::test]
    async fn sr_fund_outpoints_report_the_value_and_script_of_the_broadcast_tx() {
        use chain_gang::messages::Tx;
        use chain_gang::util::Serializable;

        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(fund_body(TEST_CLIENT_ID, 123, 1, LOCKING_SCRIPT_HEX))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;

        let tx_hex = body["txs"][0]["tx"].as_str().unwrap();
        let tx = Tx::read(&mut std::io::Cursor::new(hex::decode(tx_hex).unwrap())).unwrap();

        let outpoint = &body["outpoints"][0];
        let output_index = outpoint["index"].as_u64().unwrap() as usize;
        let output = &tx.outputs[output_index];

        assert_eq!(outpoint["satoshi"].as_i64().unwrap(), output.satoshis);
        assert_eq!(
            outpoint["locking_script"].as_str().unwrap(),
            hex::encode(&output.lock_script.0)
        );
        // and the requested value really is what was paid
        assert_eq!(output.satoshis, 123);
    }

    #[actix_web::test]
    async fn test_health_unauthenticated_when_api_key_enabled() {
        let app = build_app_with_api_key(Some(TEST_API_KEY)).await;
        let resp =
            test::call_service(&app, test::TestRequest::get().uri("/health").to_request()).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// Probes are answered by orchestrators, which carry no API key.
    #[actix_web::test]
    async fn test_ready_unauthenticated_when_api_key_enabled() {
        let app = build_app_with_api_key(Some(TEST_API_KEY)).await;
        let resp =
            test::call_service(&app, test::TestRequest::get().uri("/ready").to_request()).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[actix_web::test]
    async fn test_fund_requires_api_key_when_enabled() {
        let app = build_app_with_api_key(Some(TEST_API_KEY)).await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(fund_body(TEST_CLIENT_ID, 123, 1, LOCKING_SCRIPT_HEX))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["description"], "Unauthorized");
    }

    #[actix_web::test]
    async fn test_fund_accepts_bearer_token() {
        let app = build_app_with_api_key(Some(TEST_API_KEY)).await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .insert_header(("Authorization", format!("Bearer {TEST_API_KEY}")))
                .set_json(fund_body(TEST_CLIENT_ID, 123, 1, LOCKING_SCRIPT_HEX))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[actix_web::test]
    async fn test_add_client_requires_admin_key_when_enabled() {
        let app = build_app_with_admin_key(Some(TEST_ADMIN_API_KEY)).await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/client")
                .set_json(json!({
                    "client_id": "client2",
                    "wif": OTHER_TEST_WIF,
                }))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn test_add_client_accepts_admin_bearer_token() {
        let app = build_app_with_admin_key(Some(TEST_ADMIN_API_KEY)).await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/client")
                .insert_header(("Authorization", format!("Bearer {TEST_ADMIN_API_KEY}")))
                .set_json(json!({
                    "client_id": "client2",
                    "wif": OTHER_TEST_WIF,
                    "api_key": "new-client-key",
                }))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[actix_web::test]
    async fn test_add_client_rejects_wrong_admin_key() {
        let app = build_app_with_admin_key(Some(TEST_ADMIN_API_KEY)).await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/client")
                .insert_header(("X-API-Key", "wrong-admin-key"))
                .set_json(json!({
                    "client_id": "client2",
                    "wif": OTHER_TEST_WIF,
                }))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn test_status_unauthenticated_when_client_api_key_enabled() {
        let app = build_app_with_api_key(Some(TEST_API_KEY)).await;
        let resp =
            test::call_service(&app, test::TestRequest::get().uri("/status").to_request()).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[actix_web::test]
    async fn test_balance_requires_client_api_key_when_enabled() {
        let app = build_app_with_api_key(Some(TEST_API_KEY)).await;
        let resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/client/{TEST_CLIENT_ID}/balance"))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn test_fund_rejects_wrong_client_api_key() {
        let app = build_app_with_api_key(Some(TEST_API_KEY)).await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .insert_header(("X-API-Key", "wrong-key"))
                .set_json(fund_body(TEST_CLIENT_ID, 123, 1, LOCKING_SCRIPT_HEX))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn test_rate_limit_returns_429_when_burst_exceeded() {
        let app = build_app_with_rate_limit(2).await;
        for _ in 0..2 {
            let resp = test::call_service(
                &app,
                test::TestRequest::get()
                    .uri("/")
                    .peer_addr(peer_addr())
                    .to_request(),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::OK);
        }

        let resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/")
                .peer_addr(peer_addr())
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        let body: Value = test::read_body_json(resp).await;
        assert!(body["description"]
            .as_str()
            .unwrap()
            .contains("Rate limit exceeded"));
    }

    #[actix_web::test]
    async fn test_health_is_exempt_from_rate_limit() {
        let app = build_app_with_rate_limit(1).await;
        for _ in 0..5 {
            let resp = test::call_service(
                &app,
                test::TestRequest::get()
                    .uri("/health")
                    .peer_addr(peer_addr())
                    .to_request(),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::OK);
        }
    }

    /// A readiness probe runs on a fixed interval from every orchestrator
    /// replica, so rate limiting it would report the service unready for
    /// reasons that have nothing to do with the service.
    #[actix_web::test]
    async fn test_ready_is_exempt_from_rate_limit() {
        let app = build_app_with_rate_limit(1).await;
        for _ in 0..5 {
            let resp = test::call_service(
                &app,
                test::TestRequest::get()
                    .uri("/ready")
                    .peer_addr(peer_addr())
                    .to_request(),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::OK);
        }
    }

    async fn fund_with_headers(
        app: &impl ActixService<Request, Response = ServiceResponse, Error = Error>,
        body: Value,
    ) -> (StatusCode, Option<String>, Value) {
        let resp = test::call_service(
            app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(body)
                .to_request(),
        )
        .await;
        let http = resp.status();
        let retry_after = resp
            .headers()
            .get(actix_web::http::header::RETRY_AFTER)
            .map(|v| v.to_str().unwrap().to_string());
        let body: Value = test::read_body_json(resp).await;
        (http, retry_after, body)
    }

    /// CS-530: a payout that overflows, or exceeds every satoshi there is, is
    /// refused rather than wrapped.
    #[actix_web::test]
    async fn sr_sec_030_a_payout_that_overflows_is_refused() {
        let (app, _service) = build_app_with_service(test_config_with_keys(
            &unique_dynamic_config_path(),
            None,
            None,
        ))
        .await;
        for (satoshi, outpoints) in [(u64::MAX / 2 + 1, 2), (crate::client::MAX_SATOSHIS + 1, 1)] {
            let (http, _, body) = fund_with_headers(
                &app,
                fund_body(TEST_CLIENT_ID, satoshi, outpoints, LOCKING_SCRIPT_HEX),
            )
            .await;
            assert_eq!(http, StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(body["code"], "invalid_request");
        }
    }

    #[actix_web::test]
    async fn sr_sec_029_an_overlong_idempotency_key_is_refused() {
        let (app, _service) = build_app_with_service(test_config_with_keys(
            &unique_dynamic_config_path(),
            None,
            None,
        ))
        .await;
        let mut body = fund_body(TEST_CLIENT_ID, 100, 1, LOCKING_SCRIPT_HEX);
        body["idempotency_key"] =
            Value::String("k".repeat(crate::idempotency::MAX_IDEMPOTENCY_KEY_BYTES + 1));
        let (http, _, body) = fund_with_headers(&app, body).await;
        assert_eq!(http, StatusCode::BAD_REQUEST, "{body}");
    }

    /// CS-530: the per-request and per-period limits, end to end.
    #[actix_web::test]
    async fn sr_sec_030_funding_limits_refuse_over_limit_requests() {
        let mut config = test_config_with_keys(&unique_dynamic_config_path(), None, None);
        config.funding_limits.max_satoshis_per_request = Some(150);
        config.funding_limits.max_satoshis_per_period = Some(200);
        let (app, _service) = build_app_with_service(config).await;

        let (http, _, body) =
            fund_with_headers(&app, fund_body(TEST_CLIENT_ID, 151, 1, LOCKING_SCRIPT_HEX)).await;
        assert_eq!(http, StatusCode::BAD_REQUEST, "{body}");
        assert!(body["description"]
            .as_str()
            .unwrap()
            .contains("per request"));

        let (http, _, body) =
            fund_with_headers(&app, fund_body(TEST_CLIENT_ID, 120, 1, LOCKING_SCRIPT_HEX)).await;
        assert_eq!(http, StatusCode::OK, "{body}");

        // 120 paid this period; another 120 would make 240 of 200.
        let (http, retry_after, body) =
            fund_with_headers(&app, fund_body(TEST_CLIENT_ID, 120, 1, LOCKING_SCRIPT_HEX)).await;
        assert_eq!(http, StatusCode::TOO_MANY_REQUESTS, "{body}");
        assert_eq!(body["code"], "rate_limited");
        let retry_after: u64 = retry_after.expect("Retry-After").parse().unwrap();
        assert!((1..=3600).contains(&retry_after), "{retry_after}");
    }

    /// A request that broadcast nothing does not use up the client's window.
    #[actix_web::test]
    async fn sr_sec_030_a_request_that_spent_nothing_gives_its_amount_back() {
        let mut config = test_config_with_keys(&unique_dynamic_config_path(), None, None);
        config.funding_limits.max_satoshis_per_period = Some(15_000_000_000);
        let (app, _service) = build_app_with_service(config).await;
        // More than the test wallet holds: refused before any broadcast.
        let (http, _, body) = fund_with_headers(
            &app,
            fund_body(TEST_CLIENT_ID, 10_000_000_000, 1, LOCKING_SCRIPT_HEX),
        )
        .await;
        assert_ne!(http, StatusCode::OK, "{body}");
        assert_ne!(http, StatusCode::TOO_MANY_REQUESTS, "{body}");
        // Had it been kept, this would be over the limit.
        let (http, _, body) = fund_with_headers(
            &app,
            fund_body(TEST_CLIENT_ID, 10_000_000_000, 1, LOCKING_SCRIPT_HEX),
        )
        .await;
        assert_ne!(http, StatusCode::TOO_MANY_REQUESTS, "{body}");
    }

    #[actix_web::test]
    async fn sr_fund_005_fund_endpoint_refreshes_stale_utxo_cache() {
        let path = unique_dynamic_config_path();
        let config = test_config_with_keys(&path, None, None);
        let (app, service) = build_app_with_service(config).await;
        service
            .set_test_chain_state(TEST_CLIENT_ID, Vec::new())
            .await
            .expect("test client");

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(fund_body(TEST_CLIENT_ID, 123, 1, LOCKING_SCRIPT_HEX))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[actix_web::test]
    async fn sr_fund_006_balance_endpoint_refreshes_stale_utxo_cache() {
        let path = unique_dynamic_config_path();
        let config = test_config_with_keys(&path, None, None);
        let (app, service) = build_app_with_service(config).await;
        service
            .set_test_chain_state(TEST_CLIENT_ID, Vec::new())
            .await
            .expect("test client");

        let resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/client/{TEST_CLIENT_ID}/balance"))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;
        assert!(body["confirmed"].as_i64().unwrap() > 0);
    }

    /// CS-526: by default a raw `wif` is refused before anything is stored,
    /// and the error does not echo it (SR-SEC-010). `wif_env` still works.
    #[actix_web::test]
    async fn sr_sec_021_a_raw_wif_is_refused_unless_allowed() {
        // Not the configured client's key: a second client may not share its
        // wallet (CS-521). Private key 2; testnet, compressed, throwaway.
        const SECOND_WIF: &str = "cMahea7zqjxrtgAbB7LSGbcQUr1uX1ojuat9jZodMN87K7XCyj5v";
        let path = unique_dynamic_config_path();
        let mut config = test_config_with_keys(&path, None, None);
        config.dynamic_config.allow_plaintext_wif = false;
        let (app, service) = build_app_with_service(config).await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/client")
                .set_json(json!({ "client_id": "raw", "wif": SECOND_WIF }))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = String::from_utf8(test::read_body(resp).await.to_vec()).unwrap();
        assert!(body.contains("wif_env"), "{body}");
        assert!(!body.contains(SECOND_WIF), "{body}");
        let saved = std::fs::read_to_string(&path).unwrap();
        assert!(!saved.contains("raw"), "nothing may be stored: {saved}");
        assert!(service.get_balance("raw").await.is_none());

        unsafe { std::env::set_var("FS_RUNTIME_WIF_SR_SEC_021", SECOND_WIF) };
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/client")
                .set_json(json!({ "client_id": "by-env", "wif_env": "FS_RUNTIME_WIF_SR_SEC_021" }))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let saved = std::fs::read_to_string(&path).unwrap();
        assert!(saved.contains("env:FS_RUNTIME_WIF_SR_SEC_021"));
        assert!(!saved.contains(SECOND_WIF));
    }

    #[actix_web::test]
    async fn sr_func_007_add_client_persists_to_dynamic_config_file() {
        unsafe { std::env::set_var("FS_RUNTIME_WIF_TEST_ADD_CLIENT", OTHER_TEST_WIF) };
        let path = unique_dynamic_config_path();
        let config = test_config_with_keys(&path, None, None);
        let (app, _service) = build_app_with_service(config).await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/client")
                .set_json(json!({
                    "client_id": "client_env",
                    "wif_env": "FS_RUNTIME_WIF_TEST_ADD_CLIENT",
                }))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let saved = std::fs::read_to_string(&path).unwrap();
        assert!(saved.contains("client_env"));
        assert!(saved.contains("env:FS_RUNTIME_WIF_TEST_ADD_CLIENT"));
    }

    #[actix_web::test]
    async fn sr_clnt_001_service_supports_multiple_configured_clients() {
        let path = unique_dynamic_config_path();
        let mut config = test_config_with_keys(&path, None, None);
        config.client = Some(vec![
            ClientConfig {
                client_id: TEST_CLIENT_ID.to_string(),
                wif_key: TEST_WIF.to_string(),
                api_key: None,
            },
            ClientConfig {
                client_id: "client2".to_string(),
                wif_key: OTHER_TEST_WIF.to_string(),
                api_key: None,
            },
        ]);
        let (app, service) = build_app_with_service(config).await;
        assert_eq!(service.client_count().await, 2);

        for client_id in [TEST_CLIENT_ID, "client2"] {
            let resp = test::call_service(
                &app,
                test::TestRequest::get()
                    .uri(&format!("/client/{client_id}/address"))
                    .to_request(),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::OK);
        }
    }

    #[actix_web::test]
    async fn sr_sec_001_address_requires_client_api_key_when_enabled() {
        let app = build_app_with_api_key(Some(TEST_API_KEY)).await;
        let resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/client/{TEST_CLIENT_ID}/address"))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    /// DELETE with `key`, if any, as status and error code.
    async fn delete_code(
        app: &impl ActixService<Request, Response = ServiceResponse, Error = Error>,
        client_id: &str,
        key: Option<&str>,
    ) -> (StatusCode, Value) {
        let mut req = test::TestRequest::delete().uri(&format!("/client/{client_id}"));
        if let Some(key) = key {
            req = req.insert_header(("Authorization", format!("Bearer {key}")));
        }
        let resp = test::call_service(app, req.to_request()).await;
        let http = resp.status();
        let body: Value = test::read_body_json(resp).await;
        (http, body["code"].clone())
    }

    /// CS-653: `DELETE /client` takes the admin key, as `POST /client` does.
    /// A client's own key funds it; it used to be able to delete it too, and
    /// with it the funding of whatever service held that key.
    #[actix_web::test]
    async fn cs_653_delete_takes_the_admin_key_not_the_client_s_own() {
        let config = test_config_with_keys(
            &unique_dynamic_config_path(),
            Some(TEST_API_KEY),
            Some(TEST_ADMIN_API_KEY),
        );
        let (app, service) = build_app_with_service(config).await;
        service
            .add_client(&ClientConfig {
                client_id: "runtime".to_string(),
                wif_key: OTHER_TEST_WIF.to_string(),
                api_key: Some("runtime-key".to_string()),
            })
            .await
            .expect("add a runtime client");

        for key in [None, Some("runtime-key"), Some(TEST_API_KEY), Some("wrong")] {
            let (http, code) = delete_code(&app, "runtime", key).await;
            assert_eq!(http, StatusCode::UNAUTHORIZED, "{key:?}");
            assert_eq!(code, "unauthorized", "{key:?}");
        }
        assert!(service.is_client_id_valid("runtime").await);

        let (http, _) = delete_code(&app, "runtime", Some(TEST_ADMIN_API_KEY)).await;
        assert_eq!(http, StatusCode::OK);
        assert!(!service.is_client_id_valid("runtime").await);

        // The admin may know which ids exist, so an unknown one is named.
        let (http, code) = delete_code(&app, "runtime", Some(TEST_ADMIN_API_KEY)).await;
        assert_eq!(http, StatusCode::NOT_FOUND);
        assert_eq!(code, "unknown_client");
    }

    /// CS-653: a client from the configuration file is refused, not dropped
    /// from memory until the next restart brings it back, and goes on being
    /// served.
    #[actix_web::test]
    async fn cs_653_a_client_from_the_configuration_file_is_not_deleted() {
        let config = test_config_with_keys(
            &unique_dynamic_config_path(),
            Some(TEST_API_KEY),
            Some(TEST_ADMIN_API_KEY),
        );
        let (app, service) = build_app_with_service(config).await;

        let (http, code) = delete_code(&app, TEST_CLIENT_ID, Some(TEST_ADMIN_API_KEY)).await;
        assert_eq!(http, StatusCode::CONFLICT);
        assert_eq!(code, "static_client");
        assert!(service.is_client_id_valid(TEST_CLIENT_ID).await);

        let resp = test::call_service(
            &app,
            client_route_request("address", TEST_CLIENT_ID, Some(TEST_API_KEY)),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[actix_web::test]
    async fn sr_sec_002_unauthorized_response_uses_standard_json_body() {
        let app = build_app_with_api_key(Some(TEST_API_KEY)).await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(fund_body(TEST_CLIENT_ID, 123, 1, LOCKING_SCRIPT_HEX))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["description"], "Unauthorized");
    }

    /// One of the three client routes, for `client_id`, with `key` if any.
    fn client_route_request(route: &str, client_id: &str, key: Option<&str>) -> Request {
        let request = match route {
            "balance" => test::TestRequest::get().uri(&format!("/client/{client_id}/balance")),
            "address" => test::TestRequest::get().uri(&format!("/client/{client_id}/address")),
            "fund" => test::TestRequest::post().uri("/fund").set_json(fund_body(
                client_id,
                123,
                1,
                LOCKING_SCRIPT_HEX,
            )),
            other => panic!("no route {other}"),
        };
        match key {
            Some(key) => request.insert_header(("X-API-Key", key)).to_request(),
            None => request.to_request(),
        }
    }

    /// CS-661: with client keys in use, an unknown `client_id` is answered
    /// exactly as a real one with a missing or wrong key, on every route that
    /// takes one, so a caller cannot tell which ids exist. `DELETE /client`
    /// takes the admin key instead (CS-653). It used to be 404
    /// (400 on `/fund`) where a real one was 401. A real client's own key does
    /// not open an unknown id either.
    #[actix_web::test]
    async fn cs_661_an_unknown_client_id_is_answered_like_a_wrong_key() {
        let (app, _service) = build_app_with_service(test_config_with_keys(
            &unique_dynamic_config_path(),
            Some(TEST_API_KEY),
            None,
        ))
        .await;
        let unauthorized = json!({"code": "unauthorized", "description": "Unauthorized"});

        for route in ["balance", "address", "fund"] {
            for key in [None, Some("wrong-key"), Some(TEST_API_KEY)] {
                let resp =
                    test::call_service(&app, client_route_request(route, "no-such-client", key))
                        .await;
                assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{route} {key:?}");
                let body: Value = test::read_body_json(resp).await;
                assert_eq!(body, unauthorized, "{route} {key:?}");
            }
            for key in [None, Some("wrong-key")] {
                let resp =
                    test::call_service(&app, client_route_request(route, TEST_CLIENT_ID, key))
                        .await;
                assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{route} {key:?}");
                let body: Value = test::read_body_json(resp).await;
                assert_eq!(body, unauthorized, "{route} {key:?}");
            }
        }
    }

    #[actix_web::test]
    async fn sr_sec_010_invalid_wif_error_does_not_echo_secret() {
        let secret_wif = "cSecretWifValueThatMustNotAppearInErrors123456789";
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/client")
                .set_json(json!({
                    "client_id": "client3",
                    "wif": secret_wif,
                }))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = String::from_utf8(test::read_body(resp).await.to_vec()).unwrap();
        assert!(!body.contains(secret_wif));
    }

    /// POST /client with `body`: the status, and the error code if any.
    async fn add_client_code(body: Value) -> (StatusCode, Value) {
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/client")
                .set_json(body)
                .to_request(),
        )
        .await;
        let http = resp.status();
        let body: Value = test::read_body_json(resp).await;
        (http, body["code"].clone())
    }

    /// One /fund, answered after `req`, as status and error code.
    async fn fund_code(
        app: &impl ActixService<Request, Response = ServiceResponse, Error = Error>,
        body: Value,
        api_key: Option<&str>,
    ) -> (StatusCode, Value) {
        let mut req = test::TestRequest::post().uri("/fund").set_json(body);
        if let Some(key) = api_key {
            req = req.insert_header(("X-API-Key", key));
        }
        let resp = test::call_service(app, req.to_request()).await;
        let http = resp.status();
        let body: Value = test::read_body_json(resp).await;
        (http, body["code"].clone())
    }

    /// The CS-521 attack, and each way around it. A new client may not take
    /// id1's key: not by naming id1's own override variable, not by an
    /// `env:` reference in `wif`, not through a permitted variable that holds
    /// the same key, and not by supplying the key itself.
    #[actix_web::test]
    // The environment lock is held across the requests on purpose: these set
    // FS_CLIENT_ID1_WIF, which config tests also set under the lock, and any
    // service built for id1 reads it. No async test takes the lock, so holding
    // it here cannot deadlock.
    #[allow(clippy::await_holding_lock)]
    async fn sr_sec_018_a_runtime_client_cannot_take_another_client_s_key() {
        let _env = crate::test_support::env_lock();
        unsafe {
            std::env::set_var("FS_CLIENT_ID1_WIF", TEST_WIF);
            std::env::set_var("FS_RUNTIME_WIF_STOLEN", TEST_WIF);
            // A key no client holds -- another service's, say -- that the
            // operator never set aside for runtime clients.
            std::env::set_var("UNRELATED_SERVICE_KEY", OTHER_TEST_WIF);
        }
        let attempts = [
            json!({"client_id": "evil", "wif_env": "FS_CLIENT_ID1_WIF", "api_key": "mine"}),
            json!({"client_id": "evil", "wif_env": "FS_RUNTIME_WIF_", "api_key": "mine"}),
            json!({"client_id": "evil", "wif": "env:FS_CLIENT_ID1_WIF", "api_key": "mine"}),
            json!({"client_id": "evil", "wif": "env:FS_RUNTIME_WIF_STOLEN", "api_key": "mine"}),
            json!({"client_id": "evil", "wif_env": "FS_RUNTIME_WIF_STOLEN", "api_key": "mine"}),
            json!({"client_id": "evil", "wif": TEST_WIF, "api_key": "mine"}),
            // Only the prefix rule stops this one, and only the env: rule the
            // next: neither key belongs to a client.
            json!({"client_id": "evil", "wif_env": "UNRELATED_SERVICE_KEY", "api_key": "mine"}),
            json!({"client_id": "evil", "wif": "env:UNRELATED_SERVICE_KEY", "api_key": "mine"}),
        ];
        let mut outcomes = Vec::new();
        for body in attempts {
            outcomes.push(add_client_code(body.clone()).await);
        }
        unsafe {
            std::env::remove_var("FS_CLIENT_ID1_WIF");
            std::env::remove_var("FS_RUNTIME_WIF_STOLEN");
            std::env::remove_var("UNRELATED_SERVICE_KEY");
        }
        for (i, (http, code)) in outcomes.into_iter().enumerate() {
            assert_eq!(http, StatusCode::BAD_REQUEST, "attempt {i}");
            assert_eq!(code, "invalid_request", "attempt {i}");
        }
    }

    /// A permitted variable holding a key of its own is the intended use.
    #[actix_web::test]
    // The environment lock is held across the requests on purpose: these set
    // FS_CLIENT_ID1_WIF, which config tests also set under the lock, and any
    // service built for id1 reads it. No async test takes the lock, so holding
    // it here cannot deadlock.
    #[allow(clippy::await_holding_lock)]
    async fn sr_sec_018_a_prefixed_variable_with_its_own_key_is_accepted() {
        let _env = crate::test_support::env_lock();
        unsafe { std::env::set_var("FS_RUNTIME_WIF_GOOD", OTHER_TEST_WIF) };
        let (http, _) =
            add_client_code(json!({"client_id": "good", "wif_env": "FS_RUNTIME_WIF_GOOD"})).await;
        unsafe { std::env::remove_var("FS_RUNTIME_WIF_GOOD") };
        assert_eq!(http, StatusCode::OK);
    }

    /// The CS-519 attack: 2^32 copies of an empty script asked for about
    /// 103 GB before the API key was looked at. Who is asking is now settled
    /// first, so an unauthenticated or unknown caller costs nothing -- and if
    /// the order regressed, this test would not finish.
    #[actix_web::test]
    async fn sr_sec_017_an_unauthenticated_fund_is_refused_before_its_scripts_are_built() {
        let app = build_app_with_api_key(Some(TEST_API_KEY)).await;
        let huge = |client: &str| {
            json!({
                "client_id": client,
                "satoshi": 1,
                "multiple_tx": false,
                "locking_script": "",
                "no_of_outpoints": u32::MAX,
            })
        };
        let (http, code) = fund_code(&app, huge(TEST_CLIENT_ID), None).await;
        assert_eq!(http, StatusCode::UNAUTHORIZED);
        assert_eq!(code, "unauthorized");

        // With client keys in use, an unknown client is the same 401, so it
        // cannot be told from a real one (CS-661).
        let (http, code) = fund_code(&app, huge("no-such-client"), None).await;
        assert_eq!(http, StatusCode::UNAUTHORIZED);
        assert_eq!(code, "unauthorized");

        // And authenticated, the count is refused without being built.
        let (http, code) = fund_code(&app, huge(TEST_CLIENT_ID), Some(TEST_API_KEY)).await;
        assert_eq!(http, StatusCode::BAD_REQUEST);
        assert_eq!(code, "invalid_request");
    }

    /// At most MAX_OUTPOINTS outpoints, in either request form.
    #[actix_web::test]
    async fn sr_sec_017_no_more_than_the_maximum_outpoints_are_accepted() {
        use super::MAX_OUTPOINTS;
        let app = build_app().await;

        let (_, code) = fund_code(
            &app,
            fund_body(TEST_CLIENT_ID, 1, MAX_OUTPOINTS + 1, LOCKING_SCRIPT_HEX),
            None,
        )
        .await;
        assert_eq!(code, "invalid_request");
        // At the limit it is not refused as invalid: whatever the wallet then
        // says, the request itself was acceptable.
        let (_, code) = fund_code(
            &app,
            fund_body(TEST_CLIENT_ID, 1, MAX_OUTPOINTS, LOCKING_SCRIPT_HEX),
            None,
        )
        .await;
        assert_ne!(code, "invalid_request");

        let list = |n: u32| {
            json!({
                "client_id": TEST_CLIENT_ID,
                "satoshi": 1,
                "multiple_tx": false,
                "locking_scripts": vec![LOCKING_SCRIPT_HEX; n as usize],
            })
        };
        let (_, code) = fund_code(&app, list(MAX_OUTPOINTS + 1), None).await;
        assert_eq!(code, "invalid_request");
        let (_, code) = fund_code(&app, list(MAX_OUTPOINTS), None).await;
        assert_ne!(code, "invalid_request");
    }

    /// The scripts a request asks to be paid to are bounded in total, so the
    /// one-script form cannot expand a large script a thousand times.
    #[actix_web::test]
    async fn sr_sec_017_locking_scripts_are_bounded_in_total() {
        use super::{MAX_LOCKING_SCRIPT_BYTES, MAX_OUTPOINTS};
        let app = build_app().await;
        // 2 KiB a script: 512 of them is the limit exactly, under the
        // outpoint limit, so it is the byte limit being tested.
        let script = "51".repeat(2048);
        let at_limit = (MAX_LOCKING_SCRIPT_BYTES / 2048) as u32;
        assert!(at_limit < MAX_OUTPOINTS);

        let (_, code) = fund_code(
            &app,
            fund_body(TEST_CLIENT_ID, 1, at_limit + 1, &script),
            None,
        )
        .await;
        assert_eq!(code, "invalid_request", "one script over the byte limit");

        let (_, code) =
            fund_code(&app, fund_body(TEST_CLIENT_ID, 1, at_limit, &script), None).await;
        assert_ne!(code, "invalid_request", "exactly the limit");
    }

    /// A chain read that fails names the address it could not reach -- a node
    /// inside the network, or WhatsOnChain's URL. That is for the log: the
    /// caller is told the chain is unavailable and nothing about where it
    /// lives (CS-512). Event used to pass this description on to its own
    /// callers word for word.
    #[actix_web::test]
    async fn sr_sec_016_chain_unavailable_does_not_disclose_the_chain_address() {
        use crate::test_support::{
            CountingBroadcaster, UnreadableBlockchain, UNREADABLE_CHAIN_URL,
        };

        let config = test_config(&unique_dynamic_config_path());
        let blockchain = UnreadableBlockchain::new(&config).await;
        let (app, _service) = build_app_over(&config, blockchain, CountingBroadcaster::new()).await;

        let requests = [
            (
                "POST /fund",
                test::TestRequest::post().uri("/fund").set_json(fund_body(
                    TEST_CLIENT_ID,
                    123,
                    1,
                    LOCKING_SCRIPT_HEX,
                )),
            ),
            (
                "POST /fund multiple_tx",
                test::TestRequest::post().uri("/fund").set_json(json!({
                    "client_id": TEST_CLIENT_ID,
                    "satoshi": 123,
                    "no_of_outpoints": 2,
                    "multiple_tx": true,
                    "locking_script": LOCKING_SCRIPT_HEX,
                })),
            ),
            (
                "GET balance",
                test::TestRequest::get().uri(&format!("/client/{TEST_CLIENT_ID}/balance")),
            ),
        ];
        for (name, request) in requests {
            let resp = test::call_service(&app, request.to_request()).await;
            assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE, "{name}");
            let body: Value = test::read_body_json(resp).await;
            assert_eq!(body["code"], "chain_unavailable", "{name}");
            assert_eq!(body["description"], CHAIN_UNAVAILABLE, "{name}");
            let text = body.to_string();
            for leak in [UNREADABLE_CHAIN_URL, "10.20.30.40", "url", "get_utxo"] {
                assert!(!text.contains(leak), "{name} disclosed {leak:?}: {text}");
            }
        }
    }

    /// CS-654: a stored answer is replayed without reading the chain. The
    /// resend after a lost answer is when the chain is likeliest to be down,
    /// and answering it `chain_unavailable` left the caller unable to learn
    /// which outpoint it had been given. A new key still needs the read, and
    /// is given back when it fails, so it can be retried once the chain is up.
    #[actix_web::test]
    async fn cs_654_a_stored_answer_is_replayed_while_the_chain_is_unreadable() {
        use crate::test_support::{CountingBroadcaster, UnreadableBlockchain};

        let mut config = test_config(&unique_dynamic_config_path());
        // Every request reads the chain, so none is served from the cache.
        config.service.chain_state_max_age_seconds = Some(0);
        let blockchain = UnreadableBlockchain::new(&config).await;
        blockchain.set_readable(true);
        let (app, _service) =
            build_app_over(&config, blockchain.clone(), CountingBroadcaster::new()).await;
        let first_body = fund_body_with_key(TEST_CLIENT_ID, 123, LOCKING_SCRIPT_HEX, "first");

        let (http, first) = post_fund(&app, first_body.clone()).await;
        assert_eq!(http, StatusCode::OK, "{first}");

        blockchain.set_readable(false);
        let (http, replayed) = post_fund(&app, first_body).await;
        assert_eq!(http, StatusCode::OK, "{replayed}");
        assert_eq!(replayed["replayed"], true);
        assert_eq!(replayed["outpoints"], first["outpoints"]);

        let second_body = fund_body_with_key(TEST_CLIENT_ID, 123, LOCKING_SCRIPT_HEX, "second");
        let (http, refused) = post_fund(&app, second_body.clone()).await;
        assert_eq!(http, StatusCode::SERVICE_UNAVAILABLE, "{refused}");
        assert_eq!(refused["code"], "chain_unavailable");

        blockchain.set_readable(true);
        let (http, funded) = post_fund(&app, second_body).await;
        assert_eq!(http, StatusCode::OK, "{funded}");
        assert!(funded.get("replayed").is_none(), "{funded}");
    }

    #[actix_web::test]
    async fn sr_sec_011_index_unauthenticated_when_client_api_key_enabled() {
        let app = build_app_with_api_key(Some(TEST_API_KEY)).await;
        let resp = test::call_service(&app, test::TestRequest::get().uri("/").to_request()).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[actix_web::test]
    async fn sr_func_011_success_and_error_responses_use_expected_status_codes() {
        let app = build_app().await;
        let ok = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/client/{TEST_CLIENT_ID}/balance"))
                .to_request(),
        )
        .await;
        assert_eq!(ok.status(), StatusCode::OK);

        let err = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/client/unknown/balance")
                .to_request(),
        )
        .await;
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    // ---- Unknown broadcast outcome (issue #66) ----

    /// An unknown outcome gets its own code and its own status. A caller --
    /// or a proxy that sees only the status -- must be able to tell it from
    /// `broadcast_failed`, where nothing was spent and a plain retry is fine.
    #[actix_web::test]
    async fn sr_fund_011_fund_reports_an_unknown_outcome_as_504_not_502() {
        let (app, _service) = build_app_with_uncertain_broadcaster().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(fund_body(TEST_CLIENT_ID, 123, 1, LOCKING_SCRIPT_HEX))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::GATEWAY_TIMEOUT);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["code"], "broadcast_outcome_unknown");
        let description = body["description"].as_str().expect("a description");
        assert!(
            description.contains("idempotency_key"),
            "the caller is told how to retry safely: {description}"
        );
    }

    /// The idempotency record is deliberately *not* released on this path.
    /// The outcome is unknown, so answering a retry by funding again is the
    /// one thing that must not happen; the caller is told the request is
    /// still in progress until the record expires.
    #[actix_web::test]
    async fn sr_fund_011_an_unknown_outcome_does_not_release_the_idempotency_key() {
        let (app, _service) = build_app_with_uncertain_broadcaster().await;
        let mut body = fund_body(TEST_CLIENT_ID, 123, 1, LOCKING_SCRIPT_HEX);
        body["idempotency_key"] = json!("unknown-outcome-1");

        let first = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(body.clone())
                .to_request(),
        )
        .await;
        assert_eq!(first.status(), StatusCode::GATEWAY_TIMEOUT);

        let retry = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(body)
                .to_request(),
        )
        .await;
        assert_eq!(retry.status(), StatusCode::CONFLICT);
        let retry_body: Value = test::read_body_json(retry).await;
        assert_eq!(retry_body["code"], "key_in_progress");
    }

    // ---- Definitive rejection gets its own code (issue #69) ----

    /// A transaction the upstream will never accept must not be reported with
    /// a code documented as "retry may succeed". The caller is told to stop,
    /// and the status says so too: 409, not 502, because nothing upstream
    /// failed -- the state the transaction was built against has to change.
    #[actix_web::test]
    async fn sr_fund_014_a_permanent_rejection_is_not_reported_as_retryable() {
        let (app, _service) = build_app_with_rejecting_broadcaster(false).await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(fund_body(TEST_CLIENT_ID, 123, 1, LOCKING_SCRIPT_HEX))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["code"], "broadcast_rejected");
        let description = body["description"].as_str().expect("a description");
        assert!(
            description.contains("will not accept it on retry"),
            "{description}"
        );
    }

    /// The other half of the upstream's answer. When it says a resubmission
    /// could work, the existing code and its documented advice are right, and
    /// the caller should get them unchanged.
    #[actix_web::test]
    async fn sr_fund_014_a_retryable_rejection_stays_broadcast_failed() {
        let (app, _service) = build_app_with_rejecting_broadcaster(true).await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(fund_body(TEST_CLIENT_ID, 123, 1, LOCKING_SCRIPT_HEX))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["code"], "broadcast_failed");
    }

    /// A refusal is the upstream having looked at the transaction and said no,
    /// so nothing was spent and the `idempotency_key` is free again. Holding
    /// it would only stop the client using it for the corrected request.
    #[actix_web::test]
    async fn sr_fund_014_a_permanent_rejection_releases_the_idempotency_key() {
        let (app, _service) = build_app_with_rejecting_broadcaster(false).await;
        let mut body = fund_body(TEST_CLIENT_ID, 123, 1, LOCKING_SCRIPT_HEX);
        body["idempotency_key"] = json!("rejected-1");

        for _ in 0..2 {
            let resp = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/fund")
                    .set_json(body.clone())
                    .to_request(),
            )
            .await;
            // the same answer both times: released, so never key_in_progress
            assert_eq!(resp.status(), StatusCode::CONFLICT);
            let json: Value = test::read_body_json(resp).await;
            assert_eq!(json["code"], "broadcast_rejected");
        }
    }

    /// `broadcast_failed` now means the transaction certainly did not reach a
    /// node (CS-501), and its advice is "retry may succeed" -- so the key is
    /// free, and the retry is processed rather than refused as
    /// `key_in_progress`.
    #[actix_web::test]
    async fn cs_501_a_retryable_refusal_releases_the_idempotency_key() {
        let (app, _service) = build_app_with_rejecting_broadcaster(true).await;
        let mut body = fund_body(TEST_CLIENT_ID, 123, 1, LOCKING_SCRIPT_HEX);
        body["idempotency_key"] = json!("failed-1");

        for _ in 0..2 {
            let resp = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/fund")
                    .set_json(body.clone())
                    .to_request(),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
            let json: Value = test::read_body_json(resp).await;
            assert_eq!(json["code"], "broadcast_failed");
        }
    }

    /// An app whose broadcaster is untrusted for the first funding and accepts
    /// after, as when an operator fixes mapi-lite's key (CS-660).
    async fn build_app_untrusted_once(
        config: &Config,
    ) -> impl ActixService<Request, Response = ServiceResponse, Error = Error> {
        use crate::broadcaster::BroadcastError;
        let blockchain = test_blockchain_interface(config).await;
        let broadcaster = crate::test_support::ScriptedBroadcaster::new(vec![
            Some(BroadcastError::Untrusted(
                "mapi-lite signs with 02ab…, which [mapi_lite] miner_id_public_keys does not list"
                    .to_string(),
            )),
            None,
        ]);
        build_app_over(config, blockchain, broadcaster).await.0
    }

    /// CS-660: `broadcaster_untrusted` sends nothing (CS-618), so the
    /// `idempotency_key` is free: once mapi-lite is trusted again the same
    /// request, key and all, is funded. It used to be refused
    /// `key_in_progress` for the key's TTL.
    #[actix_web::test]
    async fn cs_660_an_untrusted_broadcaster_releases_the_idempotency_key() {
        let app = build_app_untrusted_once(&test_config(&unique_dynamic_config_path())).await;
        let mut body = fund_body(TEST_CLIENT_ID, 150, 1, LOCKING_SCRIPT_HEX);
        body["idempotency_key"] = json!("untrusted-1");

        let (http, json) = post_fund(&app, body.clone()).await;
        assert_eq!(http, StatusCode::SERVICE_UNAVAILABLE, "{json}");
        assert_eq!(json["code"], "broadcaster_untrusted");

        let (http, json) = post_fund(&app, body.clone()).await;
        assert_eq!(http, StatusCode::OK, "{json}");
        assert_ne!(json["replayed"], json!(true), "{json}");
    }

    /// CS-660: nor does it count against `[funding_limits]`. It used to stay
    /// held for the whole period: with a limit of 200, a second 150 was over
    /// it.
    #[actix_web::test]
    async fn cs_660_an_untrusted_broadcaster_releases_the_funding_limit_hold() {
        let mut config = test_config(&unique_dynamic_config_path());
        config.funding_limits.max_satoshis_per_period = Some(200);
        let app = build_app_untrusted_once(&config).await;

        let (http, json) =
            post_fund(&app, fund_body(TEST_CLIENT_ID, 150, 1, LOCKING_SCRIPT_HEX)).await;
        assert_eq!(http, StatusCode::SERVICE_UNAVAILABLE, "{json}");
        assert_eq!(json["code"], "broadcaster_untrusted");

        let (http, json) =
            post_fund(&app, fund_body(TEST_CLIENT_ID, 150, 1, LOCKING_SCRIPT_HEX)).await;
        assert_eq!(http, StatusCode::OK, "{json}");
    }

    /// CS-660: with `multiple_tx`, an untrusted broadcaster after the first
    /// transaction went out is `partial_broadcast`, not `broadcaster_untrusted`,
    /// so the key is kept and a resend is answered with what happened. Only the
    /// all-or-nothing code releases it; were a mid-batch untrusted reported as
    /// `broadcaster_untrusted`, the resend would fund the first output twice.
    #[actix_web::test]
    async fn cs_660_an_untrusted_broadcaster_mid_batch_keeps_the_idempotency_key() {
        use crate::broadcaster::BroadcastError;
        let config = test_config(&unique_dynamic_config_path());
        let blockchain = test_blockchain_interface(&config).await;
        // A third answer accepts, so a resend that funded again would succeed
        // and show up in `sent`.
        let broadcaster = crate::test_support::ScriptedBroadcaster::new(vec![
            None,
            Some(BroadcastError::Untrusted(
                "mapi-lite signs with 02ab…, which [mapi_lite] miner_id_public_keys does not list"
                    .to_string(),
            )),
            None,
            None,
        ]);
        let (app, _service) = build_app_over(&config, blockchain, broadcaster.clone()).await;
        let body = json!({
            "client_id": TEST_CLIENT_ID,
            "satoshi": 150,
            "no_of_outpoints": 2,
            "multiple_tx": true,
            "locking_script": LOCKING_SCRIPT_HEX,
            "idempotency_key": "untrusted-mid-batch",
        });

        let (http, json) = post_fund(&app, body.clone()).await;
        assert_eq!(http, StatusCode::UNPROCESSABLE_ENTITY, "{json}");
        assert_eq!(json["code"], "partial_broadcast");
        assert_eq!(json["outpoints"].as_array().unwrap().len(), 1, "{json}");
        let sent = broadcaster.sent().len();

        let (http, replayed) = post_fund(&app, body).await;
        assert_eq!(http, StatusCode::UNPROCESSABLE_ENTITY, "{replayed}");
        assert_eq!(replayed["code"], "partial_broadcast");
        assert_eq!(replayed["replayed"], json!(true), "{replayed}");
        assert_eq!(replayed["outpoints"], json["outpoints"]);
        assert_eq!(broadcaster.sent().len(), sent, "the resend broadcast again");
    }

    /// The reservation is not released for an outcome that might have spent
    /// something -- the distinction this code exists to draw.
    #[actix_web::test]
    async fn sr_fund_014_an_unknown_outcome_still_holds_the_idempotency_key() {
        let (app, _service) = build_app_with_uncertain_broadcaster().await;
        let mut body = fund_body(TEST_CLIENT_ID, 123, 1, LOCKING_SCRIPT_HEX);
        body["idempotency_key"] = json!("unknown-1");

        let first = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(body.clone())
                .to_request(),
        )
        .await;
        assert_eq!(first.status(), StatusCode::GATEWAY_TIMEOUT);

        let retry = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(body)
                .to_request(),
        )
        .await;
        let json: Value = test::read_body_json(retry).await;
        assert_eq!(json["code"], "key_in_progress");
    }

    /// CS-475 end to end. A request arriving while the client's only UTXO is
    /// held by one still broadcasting is answered 503 with `Retry-After`, so
    /// a retry policy reading only the status and headers waits and retries
    /// rather than paging someone. Its `idempotency_key` is released: the
    /// advice is to retry the same request, and that must not be refused as
    /// `key_in_progress`.
    #[actix_web::test]
    async fn cs_475_funds_in_flight_is_a_503_that_says_when_to_retry() {
        use chain_gang::interface::BlockchainInterface;

        let config = test_config(&unique_dynamic_config_path());
        let mut chain = chain_gang::interface::TestInterface::new();
        chain.set_network(&config.get_network().unwrap());
        chain
            .set_utxo(
                TEST_ADDRESS,
                &vec![crate::test_support::test_utxo()[0].clone()],
            )
            .await;
        chain.set_height(1_517_571).await;
        let (app, _service) = build_app_over(
            &config,
            Arc::new(chain),
            crate::test_support::SlowRecordingBroadcaster::new(std::time::Duration::from_millis(
                100,
            )),
        )
        .await;

        let first = test::TestRequest::post()
            .uri("/fund")
            .set_json(fund_body(TEST_CLIENT_ID, 10, 1, LOCKING_SCRIPT_HEX))
            .to_request();
        let mut body = fund_body(TEST_CLIENT_ID, 10, 1, LOCKING_SCRIPT_HEX);
        body["idempotency_key"] = json!("in-flight-1");
        let second = async {
            // behind the first, which has claimed the UTXO and is broadcasting
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/fund")
                    .set_json(body.clone())
                    .to_request(),
            )
            .await
        };
        let (first, second) = tokio::join!(test::call_service(&app, first), second);

        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            second
                .headers()
                .get(actix_web::http::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
        let json: Value = test::read_body_json(second).await;
        assert_eq!(json["code"], "funds_in_flight");

        // and the retry it was told to make, same key and all, funds
        let retry = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(body)
                .to_request(),
        )
        .await;
        assert_eq!(retry.status(), StatusCode::OK);
    }

    /// CS-422 asked how a client is supposed to know what it can withdraw.
    /// The balance endpoint answers it, and the answer is not the balance:
    /// fees come out of the same UTXOs.
    #[actix_web::test]
    async fn cs_422_balance_reports_the_fundable_maximum() {
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/client/{TEST_CLIENT_ID}/balance"))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;

        let confirmed = body["confirmed"].as_i64().expect("confirmed");
        let max_fundable = body["max_fundable"]
            .as_i64()
            .expect("max_fundable is reported");
        assert!(
            max_fundable < confirmed,
            "fees come out of the balance, so the maximum must be under it: \
             max_fundable={max_fundable} confirmed={confirmed}"
        );
        assert!(max_fundable >= 0);
    }

    /// And the number has to be usable, not decorative: funding exactly the
    /// advertised maximum must be accepted.
    #[actix_web::test]
    async fn cs_422_the_advertised_maximum_is_accepted_by_fund() {
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/client/{TEST_CLIENT_ID}/balance"))
                .to_request(),
        )
        .await;
        let body: Value = test::read_body_json(resp).await;
        let max_fundable = body["max_fundable"].as_i64().expect("max_fundable") as u64;

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(fund_body(
                    TEST_CLIENT_ID,
                    max_fundable,
                    1,
                    LOCKING_SCRIPT_HEX,
                ))
                .to_request(),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "the advertised maximum was refused: {:?}",
            test::read_body(resp).await
        );
    }

    /// Funding the advertised maximum spends the wallet out, so the
    /// transaction has no change output and the funded outpoint is at index 0
    /// rather than 1. The reported index has to follow, or the caller is
    /// handed a reference to an output that does not exist (CS-452).
    #[actix_web::test]
    async fn cs_452_the_reported_index_follows_a_missing_change_output() {
        let app = build_app().await;
        let resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/client/{TEST_CLIENT_ID}/balance"))
                .to_request(),
        )
        .await;
        let body: Value = test::read_body_json(resp).await;
        let max_fundable = body["max_fundable"].as_i64().expect("max_fundable") as u64;

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/fund")
                .set_json(fund_body(
                    TEST_CLIENT_ID,
                    max_fundable,
                    1,
                    LOCKING_SCRIPT_HEX,
                ))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = test::read_body_json(resp).await;

        let tx = decode_tx(body["txs"][0]["tx"].as_str().expect("a transaction"));
        assert_eq!(tx.outputs.len(), 1, "no change output when spent out");

        let outpoint = &body["outpoints"][0];
        assert_eq!(
            outpoint["index"].as_u64().expect("index"),
            0,
            "the funded output is the only one"
        );
        assert_eq!(
            outpoint["satoshi"].as_i64().expect("satoshi"),
            max_fundable as i64
        );
    }
}
