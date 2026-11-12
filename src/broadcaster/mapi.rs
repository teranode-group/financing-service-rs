//! Broadcasting through a mapi-lite server, using the `uls-client` crate.
//!
//! mapi-lite is nChain's Rust MerchantAPI surface; `uls-client` is its typed
//! HTTP client and shares the wire types with the server, so this module never
//! spells out JSON. A submit is `POST /mapi/txs` with a one-element batch; the
//! fee rate comes from `GET /mapi/feeQuote`. The health probe is mapi-lite's
//! own `GET /ready` (CS-505), which fails when mapi-lite cannot do its work --
//! nodes unreachable, block ingest behind, or it cannot sign or reach its
//! database. It used to be the fee quote, which mapi-lite answers from its
//! configuration even with every node down.
//!
//! Two `uls-client` clients share one base URL and token: the submit client
//! and the probe client, which asks `GET /ready` (`MapiClient::ready`, read as
//! the shared `ReadyReport`, CS-627) and fetches the fee quote. The probe
//! client has a short timeout and no retries, because `GET /ready` has to
//! answer inside a readiness probe's few seconds however slow mapi-lite is
//! being.
//!
//! **The submit is uls-client's `MapiClient::broadcast` (CS-627).** It reads
//! mapi-lite's verdict on one transaction: one result, for the transaction it
//! hashed; an acceptance mapi-lite could not record resubmitted once; anything
//! it cannot read an unknown outcome. Its retries keep what this service
//! needs structural: [`ClientError::may_have_been_delivered`] tells an attempt
//! that may have reached a node -- cancelled in flight, answered `504`, or
//! unreadable -- from one that cannot have, a refused connection or a `503`,
//! and only the first calls for the funding inputs to be reserved (see
//! `BroadcastError::Indeterminate` and `Client::commit_uncertain_funding_spend`).
//! This module ran that loop itself until uls-client v0.4.0 could say which.
//!
//! The whole broadcast, resubmission included, is bounded by
//! `mapi_lite.total_timeout_seconds`, the one timing setting (CS-624). Each
//! attempt outlasts the submit ceiling mapi-lite publishes in `/ready` by a
//! margin, so a hung node is reported by mapi-lite's `504` rather than this
//! side's timeout, and as many retries run as fit inside the deadline (see
//! `SubmitBudget`). Until mapi-lite has published a ceiling its default is
//! assumed; one it publishes that changes the budget rebuilds the submit
//! client. A funding call is interactive: a wedged mapi-lite must not hold
//! the caller, and the actix worker serving it, for longer than that.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use chain_gang::messages::Tx;
use chain_gang::util::Serializable;
use uls_client::{Broadcast, BroadcastOptions, ClientError, MapiClient};
use uls_core::wire::FeeAmount;

use super::{BroadcastError, TxBroadcaster, MAPI_LITE};
use crate::config::{
    MapiLiteConfig, SubmitBudget, MAPI_LITE_PROBE_TIMEOUT, RETRY_BACKOFF_BASE, RETRY_BACKOFF_CAP,
};

/// Hands transactions to a mapi-lite server.
pub struct MapiBroadcaster {
    /// The section, to rebuild the submit client from when mapi-lite
    /// publishes a ceiling that changes the budget.
    config: MapiLiteConfig,
    /// The submit client and the budget it was built with (CS-624).
    submit: RwLock<(MapiClient, SubmitBudget)>,
    /// For `/ready` and the fee quote: [`MAPI_LITE_PROBE_TIMEOUT`] and no
    /// retries.
    probe: MapiClient,
    /// mapi-lite's own submit ceiling as last published in its `/ready`,
    /// plus one, so 0 means "not seen" (CS-510). Kept so the budget is
    /// rebuilt, and logged, once per value rather than on every probe.
    seen_ceiling: AtomicU64,
    /// The network mapi-lite's last `/ready` said its nodes are on, from
    /// `ReadyReport::chain` (CS-624). Kept from the last answer that named
    /// one: an answer before any node has replied names none.
    chain: Mutex<Option<String>>,
    /// Whether `[mapi_lite] miner_id_public_keys` is set. With none, any key's
    /// signature verifies, so no answer can come from an untrusted one.
    pins_miner_ids: bool,
    /// The key mapi-lite last signed with when the list does not name it, set
    /// by any signed answer that fails that check and cleared by any that
    /// passes (CS-618). An answer from it is never believed, so a broadcast
    /// through it can only end in an unknown outcome.
    untrusted_signer: Mutex<Option<String>>,
    /// Whether mapi-lite answered the last authenticated probe 401: it
    /// refuses `mapi_lite.auth_token`, or needs one and none is set (CS-632).
    token_refused: AtomicBool,
}

/// Why mapi-lite cannot be used while it refuses the token (CS-632). Names the
/// setting, never the token.
const TOKEN_REFUSED: &str = "mapi-lite refuses mapi_lite.auth_token, or needs one and none is set \
     (401): every funding through it would be refused";

/// Why mapi-lite cannot be used while it signs with `key` (CS-618), for the
/// log and for the error naming the setting an operator has to change.
fn untrusted_signer_detail(key: &str) -> String {
    format!(
        "mapi-lite signs with {key}, which [mapi_lite] miner_id_public_keys does not list, so \
         none of its answers can be believed"
    )
}

impl MapiBroadcaster {
    /// Build a broadcaster for the server named by `config`.
    /// The submit budget assumes mapi-lite's default ceiling until its
    /// `/ready` publishes the one it runs with.
    pub fn new(config: &MapiLiteConfig) -> Result<Self, String> {
        if let Some(warning) = config.deadline_warning(None) {
            log::warn!("{warning}");
        }
        if let Some(warning) = config.unpinned_warning() {
            log::warn!("{warning}");
        }
        Self::with_budget(config, config.submit_budget(None))
    }

    /// A broadcaster that submits on `budget`.
    fn with_budget(config: &MapiLiteConfig, budget: SubmitBudget) -> Result<Self, String> {
        let submit = Self::submit_client(config, budget)?;
        // ready and fee_quote do not retry, so the probe needs no retry settings.
        let probe = Self::client(config, MAPI_LITE_PROBE_TIMEOUT)?;
        Ok(Self {
            config: config.clone(),
            submit: RwLock::new((submit, budget)),
            probe,
            seen_ceiling: AtomicU64::new(0),
            chain: Mutex::new(None),
            pins_miner_ids: !config.miner_id_public_keys.is_empty(),
            untrusted_signer: Mutex::new(None),
            token_refused: AtomicBool::new(false),
        })
    }

    /// The key mapi-lite signs with when `[mapi_lite] miner_id_public_keys`
    /// does not list it, as last seen (CS-618); `None` when its last signed
    /// answer verified, or none has been seen.
    pub fn untrusted_signer(&self) -> Option<String> {
        self.untrusted_signer
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// mapi-lite's own `GET /ready`: the half of [`TxBroadcaster::health_check`]
    /// that asks whether a broadcast through it can work at all. Read through
    /// uls-client as its `ReadyReport` (CS-627), which names the failing checks
    /// and carries mapi-lite's submit ceiling.
    async fn mapi_lite_ready(&self) -> Result<(), BroadcastError> {
        let report = self
            .probe
            .ready()
            .await
            .map_err(|e| BroadcastError::Upstream(format!("mapi-lite /ready: {e}")))?;
        // Read whatever the answer, ready or not: it is also where mapi-lite
        // publishes its submit ceiling (CS-510) and its nodes' network
        // (CS-624).
        self.note_ceiling(report.submit_timeout());
        if let Some(chain) = &report.chain {
            *self.chain.lock().unwrap_or_else(|e| e.into_inner()) = Some(chain.clone());
        }
        if report.ready {
            return Ok(());
        }
        let failing = report.failing();
        Err(BroadcastError::Upstream(if failing.is_empty() {
            "mapi-lite /ready answered 503 Service Unavailable".to_string()
        } else {
            format!(
                "mapi-lite /ready answered 503 Service Unavailable: {}",
                failing.join("; ")
            )
        }))
    }

    /// What mapi-lite's `/ready` cannot say, since it is unauthenticated and
    /// unsigned, read from the signed fee quote, which is neither, at no cost
    /// to mapi-lite's node pool:
    ///
    /// * **The token** (CS-632). A 401 is a token mapi-lite refuses, or one it
    ///   needs and was not given; every funding through it would be refused.
    /// * **The signer**, with miner IDs pinned (CS-618). A mapi-lite answering
    ///   ready while signing with an unlisted key had this service report
    ///   ready while every funding through it ended in an unknown outcome.
    ///
    /// A quote that cannot be fetched otherwise leaves the last verdicts.
    async fn check_quote(&self) -> Result<(), BroadcastError> {
        let quote = self.probe.fee_quote().await;
        let refused = matches!(quote, Err(ClientError::Status(401)));
        if self.token_refused.swap(refused, Ordering::Relaxed) != refused {
            if refused {
                log::error!("{TOKEN_REFUSED} (CS-632)");
            } else {
                log::info!("mapi-lite accepts mapi_lite.auth_token again");
            }
        }
        if refused {
            return Err(BroadcastError::Upstream(TOKEN_REFUSED.to_string()));
        }
        if !self.pins_miner_ids {
            return Ok(());
        }
        self.note_signature(quote.as_ref().err());
        match self.untrusted_signer() {
            Some(key) => Err(BroadcastError::Upstream(untrusted_signer_detail(&key))),
            None => Ok(()),
        }
    }

    /// Records what a signed answer said about mapi-lite's key: `None` for an
    /// answer that verified, the client's error otherwise. Only an untrusted
    /// signer changes anything among the errors -- a transport failure says
    /// nothing about the key.
    fn note_signature(&self, error: Option<&ClientError>) {
        let mut untrusted = self
            .untrusted_signer
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        match error {
            None => {
                if let Some(key) = untrusted.take() {
                    log::info!(
                        "mapi-lite's answers verify again (it no longer signs with {key}); \
                         funding through it resumes"
                    );
                }
            }
            Some(ClientError::UntrustedSigner { public_key }) => {
                if untrusted.as_deref() != Some(public_key.as_str()) {
                    log::error!(
                        "mapi-lite signs with {public_key}, which [mapi_lite] \
                         miner_id_public_keys does not list: none of its answers can be \
                         believed, so funding through it is refused until it signs with a \
                         listed key (CS-618)"
                    );
                }
                *untrusted = Some(public_key.clone());
            }
            Some(_) => {}
        }
    }

    /// mapi-lite's submit ceiling, as its last `/ready` published it.
    #[cfg(test)]
    fn mapi_lite_submit_ceiling(&self) -> Option<Duration> {
        match self.seen_ceiling.load(Ordering::Relaxed) {
            0 => None,
            plus_one => Some(Duration::from_secs(plus_one - 1)),
        }
    }

    /// The submit budget in force.
    pub fn submit_budget(&self) -> SubmitBudget {
        self.submit.read().unwrap_or_else(|e| e.into_inner()).1
    }

    /// The client to submit with: one built on the budget in force.
    fn submitter(&self) -> MapiClient {
        self.submit
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .0
            .clone()
    }

    /// Records the ceiling a `/ready` answer published, and takes the budget
    /// it gives (CS-624): each attempt outlasts it, so a hung node is
    /// reported by mapi-lite's `504`, and the retries are what still fit in
    /// `total_timeout_seconds`. Warns when the deadline is too short to let an
    /// attempt wait for that answer (SR-CFG-010). Once per distinct value: the
    /// probe runs every few seconds, and the ceiling changes only on a
    /// restart of mapi-lite.
    fn note_ceiling(&self, ceiling: Option<Duration>) {
        let Some(ceiling) = ceiling else {
            return;
        };
        let seconds = ceiling.as_secs();
        if self.seen_ceiling.swap(seconds + 1, Ordering::Relaxed) == seconds + 1 {
            return;
        }
        let budget = self.config.submit_budget(Some(ceiling));
        match self.config.deadline_warning(Some(ceiling)) {
            Some(warning) => log::warn!("{warning}"),
            None => log::info!(
                "mapi-lite answers a slow submit at {seconds}s: each attempt waits {}s, with {} \
                 retries inside mapi_lite.total_timeout_seconds ({}s)",
                budget.attempt.as_secs(),
                budget.retries,
                budget.deadline.as_secs()
            ),
        }
        if budget == self.submit_budget() {
            return;
        }
        match Self::submit_client(&self.config, budget) {
            Ok(client) => {
                *self.submit.write().unwrap_or_else(|e| e.into_inner()) = (client, budget)
            }
            // Building a client only fails on the HTTP stack itself; the
            // budget in force keeps working.
            Err(e) => log::warn!("keeping the previous mapi-lite submit budget: {e}"),
        }
    }

    /// A broadcast error from uls-client, with the setting that bounded it
    /// named when the submit deadline is what ended it: uls-client knows the
    /// deadline only as a duration, and an operator looks for the setting.
    fn in_this_services_terms(&self, error: ClientError) -> BroadcastError {
        let deadline = error.to_string().contains("submit deadline");
        match BroadcastError::from(error) {
            BroadcastError::Indeterminate(detail) if deadline => {
                BroadcastError::Indeterminate(format!("{detail} (mapi_lite.total_timeout_seconds)"))
            }
            BroadcastError::Upstream(detail) if deadline => {
                BroadcastError::Upstream(format!("{detail} (mapi_lite.total_timeout_seconds)"))
            }
            other => other,
        }
    }

    /// The submit client for `budget`: this service's budget, not
    /// uls-client's defaults, so an attempt never outlives the deadline it
    /// sits inside and no retry starts that could not finish inside it.
    fn submit_client(config: &MapiLiteConfig, budget: SubmitBudget) -> Result<MapiClient, String> {
        Ok(Self::client(config, budget.attempt)?
            .retry(budget.retries, RETRY_BACKOFF_BASE, RETRY_BACKOFF_CAP)
            .with_submit_deadline(Some(budget.deadline)))
    }

    /// A `MapiClient` for `config` whose every request times out after
    /// `timeout` (uls-client's own bound since v0.5.0).
    fn client(config: &MapiLiteConfig, timeout: Duration) -> Result<MapiClient, String> {
        // Only mapi-lite's own keys are believed (CS-523): otherwise any key
        // that signs a response verifies, which proves nothing about who sent it.
        let mut client = MapiClient::new(config.base_url().to_string())
            .with_timeout(timeout)
            .with_trusted_miner_ids(&config.miner_id_public_keys);
        if let Some(authorization) = config.authorization() {
            client = client.with_token(authorization);
        }
        Ok(client)
    }
}

impl From<ClientError> for BroadcastError {
    /// Split a client error by what it says about the transaction, not by how
    /// bad it looks.
    ///
    /// The question is only ever "might mapi-lite have taken this and relayed
    /// it?". A connection that was refused took nothing. A request that was
    /// sent and never answered may have taken everything.
    fn from(error: ClientError) -> Self {
        let detail = error.to_string();
        // uls-client says which (CS-501): a timeout after sending, an answer
        // that could not be read, or a mapi-lite `5xx` other than `503` may all
        // have carried the transaction to a node -- mapi-lite answers `504`
        // when a node may have received it and `503` only when none can have.
        // A refused connection, a request that could not be built, a `4xx` or
        // a `503` delivered nothing. Before uls-client said so, an exhausted
        // retry run was a string and this had to guess "delivered nothing".
        // A signature by a key that is not mapi-lite's counts as an answer that
        // could not be read (CS-523): whatever really happened is unknown.
        if error.may_have_been_delivered() {
            BroadcastError::Indeterminate(detail)
        } else {
            BroadcastError::Upstream(detail)
        }
    }
}

#[async_trait]
impl TxBroadcaster for MapiBroadcaster {
    fn name(&self) -> &str {
        MAPI_LITE
    }

    async fn broadcast_tx(&self, tx: &Tx) -> Result<String, BroadcastError> {
        let mut raw = Vec::new();
        tx.write(&mut raw).map_err(|e| {
            BroadcastError::Upstream(format!("the funding transaction did not serialise: {e}"))
        })?;
        // The server defaults the proof to on when it is not asked. A funding
        // transaction's proof is of no use to this service, and asking for one
        // only makes mapi-lite keep it.
        let options = BroadcastOptions {
            merkle_proof: Some(false),
            ..BroadcastOptions::default()
        };

        // Refused before anything is sent while mapi-lite signs with an
        // unlisted key (CS-618): its answer could not be believed, so the
        // transaction would land and this service would report an unknown
        // outcome and hold the inputs for the record's TTL, on every request.
        // The verdict is checked afresh first, so a mapi-lite that has gone
        // back to a listed key is used again at once.
        if self.pins_miner_ids && self.untrusted_signer().is_some() {
            // Only the signer verdict decides here; a refused token fails the
            // broadcast on its own, as broadcast_failed.
            let _ = self.check_quote().await;
            if let Some(key) = self.untrusted_signer() {
                return Err(BroadcastError::Untrusted(untrusted_signer_detail(&key)));
            }
        }

        let verdict = self.submitter().broadcast(&raw, &options).await;
        self.note_signature(verdict.as_ref().err());
        match verdict.map_err(|e| self.in_this_services_terms(e))? {
            Broadcast::Accepted {
                txid,
                recorded: true,
            } => Ok(txid),
            // Accepted by the node, but mapi-lite could not record it even on
            // the resubmission uls-client makes (CS-501). SR-FUND-035 keeps
            // that an unknown outcome, which reserves the inputs, rather than a
            // success.
            Broadcast::Accepted {
                recorded: false, ..
            } => Err(BroadcastError::Indeterminate(format!(
                "mapi-lite: {}",
                uls_core::status::ACCEPTED_NOT_RECORDED_DESCRIPTION
            ))),
            Broadcast::Rejected {
                description,
                retryable,
                conflicted_with,
                ..
            } => {
                // uls-client reports a conflict as not retryable whatever
                // mapi-lite says: `txn-mempool-conflict` means another
                // transaction already holds an input of this one, and
                // offering it again cannot succeed (CS-427).
                // The txids stay in structure for `inputs_conflicted` (CS-692);
                // the description names them too, for the log.
                let description = if conflicted_with.is_empty() {
                    description
                } else {
                    format!(
                        "{description} (conflicted with {})",
                        conflicted_with.join(", ")
                    )
                };
                Err(BroadcastError::Rejected {
                    description,
                    retryable,
                    conflicted_with,
                })
            }
        }
    }

    /// Asks mapi-lite's `GET /ready` (CS-505).
    ///
    /// This used to fetch the fee quote, which mapi-lite serves from its own
    /// configuration without asking a node -- so it answered with every node
    /// down, and this service went on reporting ready while every broadcast
    /// would fail. mapi-lite's readiness fails when its node pool stops
    /// answering, when block ingest falls behind, and when it cannot sign, which
    /// is the question being asked here: can a broadcast through it work.
    ///
    /// A `503` carries the names of mapi-lite's failing checks into the error,
    /// for the log line; the `/ready` body this service returns stays generic.
    /// mapi-lite passes the node's reject reason through, and names the
    /// transactions a refusal conflicted with (CS-691).
    fn reads_node_reasons(&self) -> bool {
        true
    }

    async fn health_check(&self) -> Result<(), BroadcastError> {
        // Both at once, not one after the other: each is bounded by
        // MAPI_LITE_PROBE_TIMEOUT, and the whole probe has to fit inside one,
        // since readiness probes allow under 3s (CS-618).
        let (ready, quote) = tokio::join!(self.mapi_lite_ready(), self.check_quote());
        // A refused token is the configuration, and the reason anything else
        // fails: it comes first.
        if self.token_refused() {
            return quote;
        }
        ready.and(quote)
    }

    /// The standard mining fee from mapi-lite's quote, in satoshis per
    /// kilobyte (CS-451), as uls-client reads it (CS-627): `standard_mining_fee`
    /// and `satoshis_per_kb`, which rounds up so the service never charges
    /// itself less than the miner asked for.
    ///
    /// Fetched afresh each time, not through `cached_fee_quote`: this is
    /// called once per sweep, not per request, and a fresh quote is what lets
    /// a fee change reach funding within one sweep rather than once the old
    /// quote expires.
    ///
    /// `None` on any failure, which leaves the configured rate standing: a
    /// quote that cannot be fetched, is unsigned, names no standard fee, or
    /// gives a nonsensical zero rate is not a reason to stop funding. The
    /// probe client is used because, like the health check, this wants a short
    /// timeout and no retries.
    async fn fee_satoshis_per_kb(&self) -> Option<u64> {
        let quote = self.probe.fee_quote().await;
        self.note_signature(quote.as_ref().err());
        let quote = match quote {
            Ok(quote) => quote,
            Err(e) => {
                log::warn!("fee quote failed, keeping the current fee rate: {e}");
                return None;
            }
        };
        let standard = quote
            .standard_mining_fee()
            // A quote that names only one fee is taken to mean it for
            // everything, rather than discarding a usable answer on a label.
            .or_else(|| quote.fees.first().map(|fee| fee.mining_fee))?;

        match standard.satoshis_per_kb() {
            Some(rate) if rate > 0 => Some(rate),
            _ => {
                let FeeAmount { satoshis, bytes } = standard;
                log::warn!(
                    "fee quote gave {satoshis} satoshis per {bytes} bytes, which is not a usable \
                     rate; keeping the current one"
                );
                None
            }
        }
    }

    /// `GET /query/tx/{txid}`, which asks mapi-lite's nodes (CS-619). `None`
    /// on any failure: an unanswered question is not evidence of anything.
    fn token_refused(&self) -> bool {
        self.token_refused.load(Ordering::Relaxed)
    }

    fn published_network(&self) -> Option<String> {
        self.chain.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    async fn knows_tx(&self, txid: &str) -> Option<bool> {
        match self.probe.tx_status(txid).await {
            Ok(status) => Some(status.known),
            Err(e) => {
                log::debug!("mapi-lite /query/tx/{txid} failed: {e}");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! The broadcaster against a mock mapi-lite. Response-signature
    //! verification is left ON: the mock signs its envelopes with
    //! `uls-core`'s own signer, the way the real server does, rather than the
    //! test switching verification off to make mocking easier.

    use super::*;
    use crate::test_support::{sample_tx, MAPI_TEST_SIGNER_WIF};
    use crate::util::tx_as_hexstr;
    use serde_json::{json, Value};
    use std::time::Instant;
    use uls_core::envelope::EnvelopeSigner;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Wraps a payload in a signed JSON envelope, as the server does.
    fn signed(payload: &Value) -> Value {
        let signer = EnvelopeSigner::from_wif(MAPI_TEST_SIGNER_WIF).expect("valid WIF");
        let envelope = signer
            .sign(serde_json::to_string(payload).expect("payload serializes"))
            .expect("signing succeeds");
        serde_json::to_value(envelope).expect("envelope serializes")
    }

    /// A `POST /mapi/txs` response body wrapping one result.
    fn txs_payload(result: Value) -> Value {
        signed(&json!({
            "apiVersion": "1.5.0",
            "currentHighestBlockHash": "0000abc",
            "currentHighestBlockHeight": 812_345,
            "failureCount": 0,
            "minerId": "02abc",
            "timestamp": "2026-09-16T00:00:00Z",
            "txs": [result],
            "txSecondMempoolExpiry": 0,
        }))
    }

    /// A `GET /mapi/feeQuote` response body.
    fn fee_quote_payload() -> Value {
        signed(&json!({
            "apiVersion": "1.5.0",
            "timestamp": "2026-09-16T00:00:00Z",
            "expiryTime": "2026-09-16T00:10:00Z",
            "minerId": "02abc",
            "currentHighestBlockHash": "0000abc",
            "currentHighestBlockHeight": 812_345,
            "fees": [{
                "feeType": "standard",
                "miningFee": { "satoshis": 1, "bytes": 1000 },
                "relayFee": { "satoshis": 1, "bytes": 1000 },
            }],
        }))
    }

    /// mapi-lite's answer for a transaction the node accepted and it could not
    /// record (CS-501).
    fn accepted_but_unrecorded(txid: &str) -> Value {
        json!({
            "returnResult": "failure",
            "resultDescription": uls_core::status::ACCEPTED_NOT_RECORDED_DESCRIPTION,
            "txid": txid,
            "failureRetryable": true,
        })
    }

    /// One per-transaction success result.
    fn success(txid: &str, description: &str) -> Value {
        json!({
            "returnResult": "success",
            "resultDescription": description,
            "txid": txid,
            "failureRetryable": false,
        })
    }

    fn ok(body: Value) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(body)
    }

    fn config(server: &MockServer) -> MapiLiteConfig {
        MapiLiteConfig::for_base_url(server.uri())
    }

    fn broadcaster(server: &MockServer) -> MapiBroadcaster {
        MapiBroadcaster::new(&config(server)).expect("broadcaster builds")
    }

    /// A broadcaster on a budget of `attempt`s per attempt, `retries`
    /// retries and a `deadline`, in seconds, rather than the one derived from
    /// mapi-lite's ceiling: a test's mock answers in seconds, not tens.
    fn on_budget(
        config: &MapiLiteConfig,
        attempt: u64,
        retries: u32,
        deadline: u64,
    ) -> MapiBroadcaster {
        let budget = SubmitBudget {
            attempt: Duration::from_secs(attempt),
            retries,
            deadline: Duration::from_secs(deadline),
        };
        MapiBroadcaster::with_budget(config, budget).expect("broadcaster builds")
    }

    /// The detail of an upstream error, or a panic naming what came instead.
    fn upstream_detail(error: BroadcastError) -> String {
        match error {
            BroadcastError::Upstream(detail) => detail,
            other => panic!("expected an upstream error, got {other:?}"),
        }
    }

    /// The detail of an unknown outcome -- and an assertion that it is one,
    /// since the whole point of the variant is that it is handled differently
    /// from a failure.
    fn indeterminate_detail(error: BroadcastError) -> String {
        match error {
            BroadcastError::Indeterminate(detail) => detail,
            other => panic!("expected an indeterminate outcome, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn sr_bchn_009_mapi_broadcaster_posts_the_transaction_to_mapi_txs() {
        let server = MockServer::start().await;
        let tx = sample_tx();
        let expected = tx.hash().encode();
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ok(txs_payload(success(&expected, ""))))
            .expect(1)
            .mount(&server)
            .await;

        let txid = broadcaster(&server)
            .broadcast_tx(&tx)
            .await
            .expect("accepted");
        assert_eq!(txid, expected);

        // The body is a one-element batch carrying the raw transaction hex,
        // and declines the merkle proof the server would otherwise default on.
        let requests = server.received_requests().await.expect("requests recorded");
        assert_eq!(requests.len(), 1);
        let body: Value = serde_json::from_slice(&requests[0].body).expect("a JSON body");
        let batch = body.as_array().expect("a batch");
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0]["rawtx"], tx_as_hexstr(&tx).unwrap());
        assert_eq!(batch[0]["merkleProof"], false);
        assert!(batch[0]["callBackUrl"].is_null());
    }

    #[tokio::test]
    async fn mapi_broadcaster_sends_the_configured_authorization_header() {
        let server = MockServer::start().await;
        let tx = sample_tx();
        // Without the header the mock does not match and the call fails.
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .and(header("authorization", "Bearer secret"))
            .respond_with(ok(txs_payload(success(&tx.hash().encode(), ""))))
            .expect(1)
            .mount(&server)
            .await;

        let mut config = config(&server);
        config.auth_token = Some("secret".to_string());
        let broadcaster = MapiBroadcaster::new(&config).expect("broadcaster builds");

        broadcaster.broadcast_tx(&tx).await.expect("accepted");
    }

    // --- the server's miner ID (CS-523) ------------------------------------

    /// The public key of `MAPI_TEST_SIGNER_WIF` (private key 1), which signs
    /// every mock response here.
    const MOCK_MINER_ID: &str =
        "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";
    /// Private key 2's public key: valid, and not the mock's.
    const OTHER_MINER_ID: &str =
        "02c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5";

    fn pinned_to(server: &MockServer, key: &str) -> MapiBroadcaster {
        let mut config = config(server);
        config.miner_id_public_keys = vec![key.to_string()];
        on_budget(&config, 13, 0, 45)
    }

    /// A response signed by mapi-lite's own key is believed.
    #[tokio::test]
    async fn sr_sec_019_a_response_from_the_pinned_miner_id_is_believed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/mapi/feeQuote"))
            .respond_with(ok(fee_quote_payload()))
            .mount(&server)
            .await;
        assert!(pinned_to(&server, MOCK_MINER_ID)
            .fee_satoshis_per_kb()
            .await
            .is_some());
    }

    /// A valid signature by any other key is not: the fee quote is ignored,
    /// and a submit's outcome is unknown rather than "nothing sent", since
    /// whatever the real server did is hidden behind the forged answer.
    #[tokio::test]
    async fn sr_sec_019_a_response_signed_by_another_key_is_not_believed() {
        let server = MockServer::start().await;
        let tx = sample_tx();
        Mock::given(method("GET"))
            .and(path("/mapi/feeQuote"))
            .respond_with(ok(fee_quote_payload()))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ok(txs_payload(success(&tx.hash().encode(), ""))))
            .mount(&server)
            .await;

        // The submit first: once any answer has shown the key untrusted, a
        // submit is not sent at all (CS-618, below). Before anything is known,
        // one is, and its answer cannot be believed.
        let broadcaster = pinned_to(&server, OTHER_MINER_ID);
        match broadcaster.broadcast_tx(&tx).await {
            Err(BroadcastError::Indeterminate(detail)) => {
                assert!(detail.contains("not a trusted miner ID"), "{detail}")
            }
            other => panic!("expected an unknown outcome, got {other:?}"),
        }
        assert_eq!(broadcaster.fee_satoshis_per_kb().await, None);
    }

    /// CS-618: an answer signed by an unlisted key marks the key untrusted,
    /// and an answer that verifies clears it.
    #[tokio::test]
    async fn cs_618_an_untrusted_signer_is_remembered_until_an_answer_verifies() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/mapi/feeQuote"))
            .respond_with(ok(fee_quote_payload()))
            .mount(&server)
            .await;

        let untrusted = pinned_to(&server, OTHER_MINER_ID);
        assert_eq!(untrusted.untrusted_signer(), None, "nothing seen yet");
        untrusted.fee_satoshis_per_kb().await;
        assert_eq!(untrusted.untrusted_signer().as_deref(), Some(MOCK_MINER_ID));

        let trusted = pinned_to(&server, MOCK_MINER_ID);
        trusted.fee_satoshis_per_kb().await;
        assert_eq!(trusted.untrusted_signer(), None);

        // A verified answer clears an earlier verdict.
        trusted.note_signature(Some(&ClientError::UntrustedSigner {
            public_key: OTHER_MINER_ID.to_string(),
        }));
        assert!(trusted.untrusted_signer().is_some());
        trusted.fee_satoshis_per_kb().await;
        assert_eq!(trusted.untrusted_signer(), None);
    }

    /// CS-618: a submit answered by an unlisted key marks it too.
    #[tokio::test]
    async fn cs_618_a_submit_signed_by_an_untrusted_key_marks_it() {
        let server = MockServer::start().await;
        let tx = sample_tx();
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ok(txs_payload(success(&tx.hash().encode(), ""))))
            .mount(&server)
            .await;
        let broadcaster = pinned_to(&server, OTHER_MINER_ID);
        let _ = broadcaster.broadcast_tx(&tx).await;
        assert_eq!(
            broadcaster.untrusted_signer().as_deref(),
            Some(MOCK_MINER_ID)
        );
    }

    /// CS-618: with miner IDs pinned, a mapi-lite that answers ready but signs
    /// with an unlisted key is not ready; signing with a listed one, it is.
    #[tokio::test]
    async fn cs_618_a_mapi_lite_signing_with_an_unlisted_key_is_not_ready() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/ready"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ready": true})))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/mapi/feeQuote"))
            .respond_with(ok(fee_quote_payload()))
            .mount(&server)
            .await;

        match pinned_to(&server, OTHER_MINER_ID).health_check().await {
            Err(BroadcastError::Upstream(detail)) => {
                assert!(detail.contains("miner_id_public_keys"), "{detail}");
                assert!(detail.contains(MOCK_MINER_ID), "{detail}");
            }
            other => panic!("expected not ready, got {other:?}"),
        }
        pinned_to(&server, MOCK_MINER_ID)
            .health_check()
            .await
            .expect("a listed key is ready");
    }

    /// CS-618: the signer check runs alongside mapi-lite's `/ready`, so with
    /// miner IDs pinned the probe still fits inside one probe timeout:
    /// with both answers taking 1.5s of a 2s timeout it takes about 1.5s.
    #[tokio::test]
    async fn cs_618_the_signer_check_keeps_the_probe_within_one_timeout() {
        let server = MockServer::start().await;
        let slow = Duration::from_millis(1_500);
        Mock::given(method("GET"))
            .and(path("/ready"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"ready": true}))
                    .set_delay(slow),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/mapi/feeQuote"))
            .respond_with(ok(fee_quote_payload()).set_delay(slow))
            .mount(&server)
            .await;

        let broadcaster = pinned_to(&server, MOCK_MINER_ID);
        let started = Instant::now();
        broadcaster.health_check().await.expect("ready");
        let took = started.elapsed();
        assert!(
            took < Duration::from_millis(2_500),
            "the probe took {took:?}"
        );
    }

    /// CS-618: once mapi-lite is known to sign with an unlisted key, a
    /// broadcast is refused before anything is sent -- after checking again,
    /// so the refusal is never on a stale verdict.
    #[tokio::test]
    async fn cs_618_an_untrusted_mapi_lite_is_not_sent_the_transaction() {
        let server = MockServer::start().await;
        let tx = sample_tx();
        Mock::given(method("GET"))
            .and(path("/mapi/feeQuote"))
            .respond_with(ok(fee_quote_payload()))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ok(txs_payload(success(&tx.hash().encode(), ""))))
            .expect(0)
            .mount(&server)
            .await;

        let broadcaster = pinned_to(&server, OTHER_MINER_ID);
        broadcaster.fee_satoshis_per_kb().await;
        match broadcaster.broadcast_tx(&tx).await {
            Err(BroadcastError::Untrusted(detail)) => {
                assert!(detail.contains("miner_id_public_keys"), "{detail}")
            }
            other => panic!("expected a refusal before sending, got {other:?}"),
        }
    }

    /// CS-618: a verdict that has gone stale -- mapi-lite signs with a listed
    /// key again -- is cleared by the re-check, and the transaction is sent.
    #[tokio::test]
    async fn cs_618_a_stale_untrusted_verdict_does_not_block_a_broadcast() {
        let server = MockServer::start().await;
        let tx = sample_tx();
        Mock::given(method("GET"))
            .and(path("/mapi/feeQuote"))
            .respond_with(ok(fee_quote_payload()))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ok(txs_payload(success(&tx.hash().encode(), ""))))
            .expect(1)
            .mount(&server)
            .await;

        let broadcaster = pinned_to(&server, MOCK_MINER_ID);
        broadcaster.note_signature(Some(&ClientError::UntrustedSigner {
            public_key: OTHER_MINER_ID.to_string(),
        }));
        broadcaster
            .broadcast_tx(&tx)
            .await
            .expect("sent and accepted");
        assert_eq!(broadcaster.untrusted_signer(), None);
    }

    /// With no miner IDs pinned, any key verifies: nothing is ever untrusted.
    #[tokio::test]
    async fn cs_618_without_pinned_miner_ids_nothing_is_untrusted() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/mapi/feeQuote"))
            .respond_with(ok(fee_quote_payload()))
            .mount(&server)
            .await;
        let broadcaster = broadcaster(&server);
        broadcaster.fee_satoshis_per_kb().await;
        assert_eq!(broadcaster.untrusted_signer(), None);
    }

    /// CS-619: mapi-lite's `/query/tx` says whether its nodes know a
    /// transaction; a failure says nothing.
    #[tokio::test]
    async fn cs_619_knows_tx_asks_mapi_lite_s_nodes() {
        let server = MockServer::start().await;
        let (known, unknown) = ("aa".repeat(32), "bb".repeat(32));
        Mock::given(method("GET"))
            .and(path(format!("/query/tx/{known}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                json!({"txid": known, "known": true, "confirmed": true, "block_height": 120}),
            ))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/query/tx/{unknown}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"txid": unknown, "known": false, "confirmed": false})),
            )
            .mount(&server)
            .await;
        let broadcaster = broadcaster(&server);
        assert_eq!(broadcaster.knows_tx(&known).await, Some(true));
        assert_eq!(broadcaster.knows_tx(&unknown).await, Some(false));
        assert_eq!(
            broadcaster.knows_tx(&"cc".repeat(32)).await,
            None,
            "a 404 says nothing"
        );
    }

    #[tokio::test]
    async fn mapi_broadcaster_already_known_is_a_success() {
        let server = MockServer::start().await;
        let tx = sample_tx();
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ok(txs_payload(success(
                &tx.hash().encode(),
                "Already known",
            ))))
            .mount(&server)
            .await;

        let txid = broadcaster(&server)
            .broadcast_tx(&tx)
            .await
            .expect("known is fine");
        assert_eq!(txid, tx.hash().encode());
    }

    /// CS-427: mapi-lite reported a `txn-mempool-conflict` with
    /// `failureRetryable: true`, so the caller was told "retry may succeed"
    /// about a double spend that never would. A conflict is in the inputs, and
    /// once another transaction holds them, offering the identical transaction
    /// again cannot work whatever the server says.
    #[tokio::test]
    async fn cs_427_a_conflict_is_final_even_when_the_server_calls_it_retryable() {
        let server = MockServer::start().await;
        let tx = sample_tx();
        let rejection = txs_payload(json!({
            "returnResult": "failure",
            "resultDescription": "Mempool error, retry again later. (details: 258 txn-mempool-conflict)",
            "txid": tx.hash().encode(),
            // exactly what the ticket recorded
            "failureRetryable": true,
            "conflictedWith": [{
                "txid": "1263e7f90ab93a140e5a4102c00dd12227bbbb4f98a53a62469f69521f863a17",
                "size": 100,
                "hex": "00",
            }],
        }));
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ok(rejection))
            .mount(&server)
            .await;

        match broadcaster(&server)
            .broadcast_tx(&tx)
            .await
            .expect_err("rejected")
        {
            BroadcastError::Rejected {
                description,
                retryable,
                conflicted_with,
            } => {
                assert!(
                    description.contains("txn-mempool-conflict"),
                    "{description}"
                );
                assert!(description.contains("1263e7f9"), "{description}");
                assert_eq!(
                    conflicted_with,
                    ["1263e7f90ab93a140e5a4102c00dd12227bbbb4f98a53a62469f69521f863a17"]
                );
                assert!(
                    !retryable,
                    "a conflict cannot be resolved by resubmitting the same transaction"
                );
            }
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    /// The narrowing is only for conflicts. A rejection the server calls
    /// retryable and that names no conflict is still retryable -- mempool
    /// full, say -- and reporting it as final would send a caller away from
    /// something that would have worked.
    #[tokio::test]
    async fn cs_427_a_retryable_rejection_without_a_conflict_stays_retryable() {
        let server = MockServer::start().await;
        let tx = sample_tx();
        let rejection = txs_payload(json!({
            "returnResult": "failure",
            "resultDescription": "Mempool full",
            "txid": tx.hash().encode(),
            "failureRetryable": true,
        }));
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ok(rejection))
            .mount(&server)
            .await;

        match broadcaster(&server)
            .broadcast_tx(&tx)
            .await
            .expect_err("rejected")
        {
            BroadcastError::Rejected { retryable, .. } => assert!(retryable),
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    /// CS-692: mapi-lite naming a conflicting transaction is enough. The
    /// service treats the refusal as a conflict (CS-691) even when the
    /// description carries none of the node's wording for one.
    #[tokio::test]
    async fn cs_692_a_named_conflict_is_a_conflict_whatever_the_description_says() {
        let server = MockServer::start().await;
        let tx = sample_tx();
        let rejection = txs_payload(json!({
            "returnResult": "failure",
            "resultDescription": "Mempool error",
            "txid": tx.hash().encode(),
            "failureRetryable": false,
            "conflictedWith": [{ "txid": "c0ffee", "size": 100, "hex": "00" }],
        }));
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ok(rejection))
            .mount(&server)
            .await;

        let error = broadcaster(&server)
            .broadcast_tx(&tx)
            .await
            .expect_err("rejected");

        assert!(error.inputs_conflicted(), "{error}");
        assert!(!error.inputs_missing(), "{error}");
    }

    #[tokio::test]
    async fn mapi_broadcaster_rejection_carries_description_conflicts_and_retryability() {
        let server = MockServer::start().await;
        let tx = sample_tx();
        let rejection = txs_payload(json!({
            "returnResult": "failure",
            "resultDescription": "txn-mempool-conflict",
            "txid": tx.hash().encode(),
            "failureRetryable": false,
            "conflictedWith": [{ "txid": "deadbeef", "size": 100, "hex": "00" }],
        }));
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ok(rejection))
            .mount(&server)
            .await;

        let error = broadcaster(&server)
            .broadcast_tx(&tx)
            .await
            .expect_err("rejected");

        match error {
            BroadcastError::Rejected {
                description,
                retryable,
                conflicted_with,
            } => {
                assert!(
                    description.contains("txn-mempool-conflict"),
                    "{description}"
                );
                assert!(description.contains("deadbeef"), "{description}");
                assert_eq!(conflicted_with, ["deadbeef"]);
                assert!(!retryable);
            }
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn mapi_broadcaster_refuses_a_result_count_other_than_one() {
        let server = MockServer::start().await;
        let empty_batch = signed(&json!({
            "apiVersion": "1.5.0",
            "currentHighestBlockHash": "0000abc",
            "currentHighestBlockHeight": 812_345,
            "failureCount": 0,
            "minerId": "02abc",
            "timestamp": "2026-09-16T00:00:00Z",
            "txs": [],
            "txSecondMempoolExpiry": 0,
        }));
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ok(empty_batch))
            .mount(&server)
            .await;

        let error = broadcaster(&server)
            .broadcast_tx(&sample_tx())
            .await
            .expect_err("no result for our transaction");
        assert!(indeterminate_detail(error).contains("returned 0 results"));
    }

    /// A txid that is not the one we hashed means the transaction on the
    /// network is not the one this service built, so the caller must not be
    /// handed outpoints derived from the local hash.
    #[tokio::test]
    async fn mapi_broadcaster_refuses_a_txid_that_is_not_the_transactions_own() {
        let server = MockServer::start().await;
        let tx = sample_tx();
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ok(txs_payload(success("deadbeef", ""))))
            .mount(&server)
            .await;

        let error = broadcaster(&server)
            .broadcast_tx(&tx)
            .await
            .expect_err("a foreign txid is not our transaction");
        let detail = indeterminate_detail(error);
        assert!(detail.contains("deadbeef"), "{detail}");
        assert!(detail.contains(&tx.hash().encode()), "{detail}");
    }

    /// The configured URL is normalised before the client is built, so a
    /// trailing `/` -- which the configuration documents as tolerated -- does
    /// not become a doubled separator in the request path.
    #[tokio::test]
    async fn mapi_broadcaster_tolerates_a_trailing_slash_on_the_base_url() {
        let server = MockServer::start().await;
        let tx = sample_tx();
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ok(txs_payload(success(&tx.hash().encode(), ""))))
            .expect(1)
            .mount(&server)
            .await;

        let config = MapiLiteConfig::for_base_url(format!("{}/", server.uri()));
        let broadcaster = MapiBroadcaster::new(&config).expect("broadcaster builds");

        broadcaster.broadcast_tx(&tx).await.expect("accepted");
    }

    /// The retry budget multiplies the per-request timeout, so the whole
    /// submit carries a deadline of its own and a wedged mapi-lite cannot hold
    /// a `/fund` caller for attempts * timeout.
    #[tokio::test]
    async fn mapi_broadcaster_submit_is_bounded_by_the_total_deadline() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ResponseTemplate::new(500).set_delay(Duration::from_secs(30)))
            .mount(&server)
            .await;

        // Six attempts of up to 2s each, plus back-off, is fifteen seconds and
        // more; the deadline ends it at three.
        let config = config(&server);
        let broadcaster = on_budget(&config, 2, 5, 3);

        let started = std::time::Instant::now();
        let error = broadcaster
            .broadcast_tx(&sample_tx())
            .await
            .expect_err("deadline");
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the deadline did not fire"
        );
        // Unknown, not failed: the request was cancelled in flight, and a
        // cancelled request says nothing about what the server did with it.
        assert!(indeterminate_detail(error).contains("total_timeout_seconds"));
    }

    /// A mapi-lite `500` can come after the node call -- it answers `503` when
    /// no node can have received the batch -- so a run of them leaves the
    /// outcome unknown, not failed (CS-501).
    #[tokio::test]
    async fn mapi_broadcaster_retries_a_server_error_then_reports_the_outcome_unknown() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ResponseTemplate::new(500))
            // one attempt and the budget's one retry
            .expect(2)
            .mount(&server)
            .await;

        let config = config(&server);
        let broadcaster = on_budget(&config, 13, 1, 45);

        let error = broadcaster
            .broadcast_tx(&sample_tx())
            .await
            .expect_err("exhausted");
        assert!(indeterminate_detail(error).contains("outcome unknown"));
    }

    /// A `503` is mapi-lite saying no node can have received the batch, so
    /// exhausting the retries on it is a plain failure: nothing was spent, and
    /// reserving the inputs would only idle them (CS-501).
    #[tokio::test]
    async fn mapi_broadcaster_retries_a_503_then_reports_a_failure() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ResponseTemplate::new(503))
            .expect(2)
            .mount(&server)
            .await;

        let config = config(&server);
        let broadcaster = on_budget(&config, 13, 1, 45);

        let error = broadcaster
            .broadcast_tx(&sample_tx())
            .await
            .expect_err("exhausted");
        assert!(upstream_detail(error).contains("retries exhausted"));
    }

    /// mapi-lite's "accepted by the node but could not be recorded" is a
    /// retryable `failure` on the wire, but the transaction is on the network
    /// (CS-501). It is resubmitted, and the resubmission settles it.
    #[tokio::test]
    async fn an_acceptance_mapi_lite_could_not_record_is_resubmitted_not_refused() {
        let server = MockServer::start().await;
        let tx = sample_tx();
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ok(txs_payload(accepted_but_unrecorded(
                &tx.hash().encode(),
            ))))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ok(txs_payload(success(
                &tx.hash().encode(),
                "Already known",
            ))))
            .mount(&server)
            .await;

        let txid = broadcaster(&server)
            .broadcast_tx(&tx)
            .await
            .expect("the resubmission is answered as known");
        assert_eq!(txid, tx.hash().encode());
    }

    /// And if every resubmission meets the same answer, the transaction is
    /// still on the network: an unknown outcome, which reserves its inputs,
    /// never a failure, which would release them for the next request to spend
    /// again (CS-501).
    #[tokio::test]
    async fn an_acceptance_that_is_never_recorded_is_an_unknown_outcome() {
        let server = MockServer::start().await;
        let tx = sample_tx();
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ok(txs_payload(accepted_but_unrecorded(
                &tx.hash().encode(),
            ))))
            .mount(&server)
            .await;

        let error = on_budget(&config(&server), 13, 1, 45)
            .broadcast_tx(&tx)
            .await
            .expect_err("never settled");
        assert!(indeterminate_detail(error).contains("accepted by the node"));
    }

    /// A connection that was refused delivered nothing, so it is a plain
    /// failure: reporting it as an unknown outcome would have the service
    /// reserve inputs that were demonstrably never spent, and a mapi-lite that
    /// is simply down would eat the wallet.
    #[tokio::test]
    async fn sr_fund_011_an_unreachable_mapi_lite_is_a_failure_not_an_unknown_outcome() {
        // A port nothing is listening on: the connection is refused rather
        // than accepted and left hanging.
        let config = MapiLiteConfig::for_base_url("http://127.0.0.1:1");
        let broadcaster = on_budget(&config, 2, 0, 5);

        let error = broadcaster
            .broadcast_tx(&sample_tx())
            .await
            .expect_err("nothing is listening");
        assert!(
            matches!(error, BroadcastError::Upstream(_)),
            "a refused connection took nothing, got {error:?}"
        );
    }

    #[tokio::test]
    async fn mapi_broadcaster_rejects_a_tampered_envelope() {
        let server = MockServer::start().await;
        let tx = sample_tx();
        let mut envelope = txs_payload(success(&tx.hash().encode(), ""));
        // Keep the signature, swap the payload underneath it.
        envelope["payload"] = json!("{\"txs\":[]}");
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ok(envelope))
            .mount(&server)
            .await;

        let error = broadcaster(&server)
            .broadcast_tx(&tx)
            .await
            .expect_err("tampered");
        // Unknown rather than failed: an envelope arrived, so the server acted
        // on the transaction, and this client simply cannot trust what it says
        // about it.
        assert!(indeterminate_detail(error).contains("signature"));
    }

    /// CS-505: the health check asks mapi-lite's own readiness, not the fee
    /// quote -- which mapi-lite serves from configuration with every node down.
    /// The quote is still fetched, once, for what `/ready` cannot say: whether
    /// mapi-lite accepts the token (CS-632).
    #[tokio::test]
    async fn sr_bchn_011_mapi_broadcaster_health_check_passes_when_mapi_lite_is_ready() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/ready"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ready": true})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/mapi/feeQuote"))
            .respond_with(ok(fee_quote_payload()))
            .expect(1)
            .mount(&server)
            .await;

        broadcaster(&server).health_check().await.expect("healthy");
    }

    /// CS-632: a fee quote answered 401 is a refused token. The health check
    /// fails on it, ahead of what `/ready` says, naming the setting and never
    /// the token, and it is remembered for startup; a quote answered again
    /// clears it.
    #[tokio::test]
    async fn cs_632_a_refused_token_fails_the_health_check() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/ready"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ready": true})))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/mapi/feeQuote"))
            .respond_with(ResponseTemplate::new(401))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/mapi/feeQuote"))
            .respond_with(ok(fee_quote_payload()))
            .mount(&server)
            .await;
        let mut config = config(&server);
        config.auth_token = Some("hunter2".to_string());
        let broadcaster = MapiBroadcaster::new(&config).expect("builds");
        assert!(!broadcaster.token_refused());
        let detail = upstream_detail(broadcaster.health_check().await.unwrap_err());
        assert!(
            detail.contains("mapi_lite.auth_token") && detail.contains("401"),
            "{detail}"
        );
        assert!(!detail.contains("hunter2"), "{detail}");
        assert!(broadcaster.token_refused());

        broadcaster.health_check().await.expect("accepted again");
        assert!(!broadcaster.token_refused());
    }

    /// mapi-lite's ceiling, ready or not, is read off `/ready` (CS-510).
    #[tokio::test]
    async fn sr_cfg_010_health_check_reads_mapi_lite_s_submit_ceiling() {
        for (status, ready) in [(200, true), (503, false)] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/ready"))
                .respond_with(ResponseTemplate::new(status).set_body_json(json!({
                    "ready": ready,
                    "submitTimeoutSecs": 10,
                    "checks": {}
                })))
                .mount(&server)
                .await;
            let broadcaster = broadcaster(&server);
            assert_eq!(broadcaster.mapi_lite_submit_ceiling(), None);
            let _ = broadcaster.health_check().await;
            assert_eq!(
                broadcaster.mapi_lite_submit_ceiling(),
                Some(Duration::from_secs(10)),
                "HTTP {status}"
            );
        }
    }

    /// A ceiling mapi-lite publishes sets the budget (CS-624): a 20s ceiling
    /// gives 23s attempts, and inside the default 45s no retry fits.
    #[tokio::test]
    async fn sr_cfg_010_the_published_ceiling_sets_the_submit_budget() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/ready"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "ready": true,
                "submitTimeoutSecs": 20,
                "checks": {}
            })))
            .mount(&server)
            .await;
        let broadcaster = broadcaster(&server);
        let assumed = broadcaster.submit_budget();
        assert_eq!((assumed.attempt.as_secs(), assumed.retries), (13, 2));

        broadcaster.health_check().await.expect("ready");
        let budget = broadcaster.submit_budget();
        assert_eq!((budget.attempt.as_secs(), budget.retries), (23, 0));
        assert_eq!(budget.deadline, Duration::from_secs(45));
        // And the client it submits with is the one built on it.
        assert_eq!(broadcaster.submitter().timeout(), Some(budget.attempt));
    }

    /// Every derived attempt clears mapi-lite's ceiling by uls-client's own
    /// rule, unless the deadline cannot allow it, which is warned about
    /// instead (SR-CFG-010).
    #[test]
    fn sr_cfg_010_a_derived_attempt_always_clears_mapi_lite_s_ceiling() {
        let config = MapiLiteConfig::for_base_url("http://mapi-lite".to_string());
        for seconds in 0..60 {
            let ceiling = Duration::from_secs(seconds);
            let budget = config.submit_budget(Some(ceiling));
            let warning = MapiBroadcaster::with_budget(&config, budget)
                .expect("broadcaster builds")
                .submitter()
                .submit_budget_warning(ceiling);
            if budget.cuts_attempts_short(ceiling) {
                assert!(
                    config.deadline_warning(Some(ceiling)).is_some(),
                    "{seconds}s"
                );
            } else {
                assert_eq!(warning, None, "{seconds}s");
                assert_eq!(config.deadline_warning(Some(ceiling)), None, "{seconds}s");
            }
        }
    }

    /// CS-624: the network mapi-lite's `/ready` names is recorded, ready or
    /// not, and an answer naming none keeps the last one heard.
    #[tokio::test]
    async fn cs_624_the_network_mapi_lite_publishes_is_recorded() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/ready"))
            .respond_with(
                ResponseTemplate::new(503)
                    .set_body_json(json!({"ready": false, "chain": "regtest", "checks": {}})),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/ready"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ready": true})))
            .mount(&server)
            .await;
        let broadcaster = broadcaster(&server);
        assert_eq!(broadcaster.published_network(), None);
        let _ = broadcaster.health_check().await;
        assert_eq!(broadcaster.published_network().as_deref(), Some("regtest"));
        broadcaster.health_check().await.expect("ready");
        assert_eq!(broadcaster.published_network().as_deref(), Some("regtest"));
    }

    /// CS-627: mapi-lite's answer is read as uls-client's `ReadyReport`; a body
    /// that is not one is not ready.
    #[tokio::test]
    async fn cs_627_an_unreadable_ready_answer_is_not_healthy() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/ready"))
            .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
            .mount(&server)
            .await;
        let detail = upstream_detail(broadcaster(&server).health_check().await.unwrap_err());
        assert!(detail.contains("mapi-lite /ready"), "{detail}");
    }

    /// A quote that answers does not make mapi-lite healthy when its own
    /// readiness says its nodes are gone.
    #[tokio::test]
    async fn sr_bchn_011_mapi_broadcaster_health_check_fails_when_mapi_lite_is_not_ready() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/ready"))
            .respond_with(ResponseTemplate::new(503).set_body_json(json!({
                "ready": false,
                "checks": {
                    "database": { "ok": true },
                    "chainInfo": { "ok": false, "detail": "chain info is 120s old" },
                    "ingest": { "ok": false }
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/mapi/feeQuote"))
            .respond_with(ok(fee_quote_payload()))
            .mount(&server)
            .await;

        let error = broadcaster(&server)
            .health_check()
            .await
            .expect_err("unhealthy");
        let detail = upstream_detail(error);
        assert!(detail.contains("503"), "{detail}");
        assert!(
            detail.contains("chainInfo: chain info is 120s old") && detail.contains("ingest"),
            "names mapi-lite's failing checks for the log: {detail}"
        );
        assert!(
            !detail.contains("database"),
            "only the failing ones: {detail}"
        );
    }

    #[tokio::test]
    async fn mapi_broadcaster_health_check_is_bounded_by_its_own_timeout() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/ready"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(3)))
            .mount(&server)
            .await;

        let broadcaster = broadcaster(&server);

        let started = std::time::Instant::now();
        let error = broadcaster
            .health_check()
            .await
            .expect_err("a slow upstream is unhealthy");
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "probe did not time out"
        );
        // The variant is beside the point here -- a probe only answers
        // reachable or not -- but it must carry a reason for the log.
        assert!(!error.to_string().is_empty());
    }

    #[tokio::test]
    async fn mapi_broadcaster_is_named_mapi_lite() {
        let server = MockServer::start().await;
        assert_eq!(broadcaster(&server).name(), MAPI_LITE);
    }

    // ---- The retry budget actually running (issue #68) ----

    /// The retries a budget buys have to be attempts that happen. Before this, the total
    /// deadline cut the default sequence off after one attempt and part of a
    /// second, so the number in the config was not the number that ran.
    ///
    /// wiremock verifies the expectation on drop, so the count is the test.
    #[tokio::test]
    async fn sr_fund_013_every_configured_attempt_is_actually_made() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ResponseTemplate::new(500))
            .expect(3) // one attempt plus the default budget's two retries
            .mount(&server)
            .await;

        let broadcaster = broadcaster(&server);
        assert_eq!(
            broadcaster.submit_budget().retries,
            2,
            "the default budget this test is about"
        );
        broadcaster
            .broadcast_tx(&sample_tx())
            .await
            .expect_err("every attempt is a 500");
    }

    /// The deadline is still the backstop. With attempts that cannot fit, the
    /// sequence stops at the deadline rather than running to the budget.
    #[tokio::test]
    async fn sr_fund_013_the_deadline_still_cuts_a_budget_that_cannot_fit() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ResponseTemplate::new(500).set_delay(Duration::from_secs(30)))
            .mount(&server)
            .await;

        let config = config(&server);
        let broadcaster = on_budget(&config, 2, 10, 3);

        let started = std::time::Instant::now();
        let error = broadcaster
            .broadcast_tx(&sample_tx())
            .await
            .expect_err("the deadline fires");
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "eleven 2s attempts ran instead of stopping at the 3s deadline"
        );
        assert!(indeterminate_detail(error).contains("total_timeout_seconds"));
    }

    /// A retry after a timeout is how an unknown outcome becomes a known one:
    /// mapi-lite answers a transaction it already holds with success, so the
    /// second attempt settles what the first left open. Without the retry the
    /// caller would be told the outcome is unknown and the client's inputs
    /// would be reserved for nothing.
    #[tokio::test]
    async fn sr_fund_013_a_retry_resolves_an_attempt_that_timed_out() {
        let server = MockServer::start().await;
        let tx = sample_tx();
        // first attempt: accepted but far too slow to answer
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(
                ok(txs_payload(success(&tx.hash().encode(), "")))
                    .set_delay(Duration::from_secs(30)),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // second attempt: "already known", which mapi-lite reports as success
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ok(txs_payload(success(
                &tx.hash().encode(),
                "Transaction already known",
            ))))
            .mount(&server)
            .await;

        let config = config(&server);
        let broadcaster = on_budget(&config, 1, 2, 10);

        let txid = broadcaster
            .broadcast_tx(&tx)
            .await
            .expect("the retry settles it");
        assert_eq!(txid, tx.hash().encode());
    }

    /// Once an attempt may have been delivered, a later transport failure
    /// cannot take that back. The call has to stay an unknown outcome, or the
    /// service would leave inputs spendable that may already be spent.
    #[tokio::test]
    async fn sr_fund_013_a_timeout_then_a_failure_is_still_an_unknown_outcome() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(30)))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // Everything after the first attempt is a 503 -- mapi-lite saying
        // nothing was sent -- which on its own would be a plain failure; what
        // matters is that the first attempt already put the transaction in
        // doubt.
        Mock::given(method("POST"))
            .and(path("/mapi/txs"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let config = config(&server);
        let broadcaster = on_budget(&config, 1, 1, 5);

        let error = broadcaster
            .broadcast_tx(&sample_tx())
            .await
            .expect_err("nothing succeeded");
        // uls-client's words: an outcome unknown, over both attempts.
        let detail = indeterminate_detail(error);
        assert!(
            detail.contains("outcome unknown after 2 attempts"),
            "{detail}"
        );
    }

    /// The counter-case, and the one that caught me out while fixing #66: a
    /// mapi-lite that refuses connections fails fast, so the deadline can be
    /// consumed by back-off alone. That is still a plain failure -- nothing
    /// was ever delivered -- and reporting it as unknown would reserve inputs
    /// that were never spent.
    #[tokio::test]
    async fn sr_fund_013_backoff_exhausting_the_deadline_is_a_failure_not_unknown() {
        // Nothing is listening, so every attempt is refused immediately and
        // only the back-off takes any time.
        let config = MapiLiteConfig::for_base_url("http://127.0.0.1:1");
        let broadcaster = on_budget(&config, 5, 5, 5);

        let error = broadcaster
            .broadcast_tx(&sample_tx())
            .await
            .expect_err("nothing is listening");
        assert!(
            matches!(error, BroadcastError::Upstream(_)),
            "a refused connection delivered nothing, got {error:?}"
        );
    }

    /// A fee quote naming `fees`, as `(feeType, satoshis, bytes)`.
    fn quoting(fees: &[(&str, u64, u64)]) -> Value {
        let fees: Vec<Value> = fees
            .iter()
            .map(|(fee_type, satoshis, bytes)| {
                json!({
                    "feeType": fee_type,
                    "miningFee": { "satoshis": satoshis, "bytes": bytes },
                    "relayFee": { "satoshis": satoshis, "bytes": bytes },
                })
            })
            .collect();
        signed(&json!({
            "apiVersion": "1.5.0",
            "timestamp": "2026-09-16T00:00:00Z",
            "expiryTime": "2026-09-16T00:10:00Z",
            "minerId": "02abc",
            "currentHighestBlockHash": "0000abc",
            "currentHighestBlockHeight": 812_345,
            "fees": fees,
        }))
    }

    async fn rate_from(fees: &[(&str, u64, u64)]) -> (Option<u64>, MockServer) {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/mapi/feeQuote"))
            .respond_with(ok(quoting(fees)))
            .mount(&server)
            .await;
        let rate = broadcaster(&server).fee_satoshis_per_kb().await;
        (rate, server)
    }

    /// CS-627: the standard mining rate, per kilobyte, rounded up -- as
    /// uls-client reads it.
    #[tokio::test]
    async fn cs_627_the_fee_rate_is_the_standard_mining_fee_per_kb_rounded_up() {
        assert_eq!(rate_from(&[("standard", 50, 1000)]).await.0, Some(50));
        // 1 satoshi per 300 bytes is 3.33 per kilobyte: 4, never 3.
        assert_eq!(rate_from(&[("standard", 1, 300)]).await.0, Some(4));
        // The data rate does not price a funding transaction.
        assert_eq!(
            rate_from(&[("data", 9, 1000), ("standard", 5, 1000)])
                .await
                .0,
            Some(5)
        );
    }

    /// A quote naming one fee under another label is taken for everything;
    /// a zero rate is not a usable answer, and leaves the current rate.
    #[tokio::test]
    async fn cs_627_one_fee_under_another_name_is_used_and_a_zero_rate_is_not() {
        assert_eq!(rate_from(&[("other", 7, 1000)]).await.0, Some(7));
        assert_eq!(rate_from(&[("standard", 0, 1000)]).await.0, None);
        assert_eq!(rate_from(&[("standard", 5, 0)]).await.0, None);
    }

    /// CS-627: asked afresh on every sweep, not cached until the quote
    /// expires, so a fee change reaches funding within one sweep.
    #[tokio::test]
    async fn cs_627_the_fee_quote_is_asked_afresh_each_time() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/mapi/feeQuote"))
            .respond_with(ok(quoting(&[("standard", 50, 1000)])))
            .expect(2)
            .mount(&server)
            .await;
        let broadcaster = broadcaster(&server);
        broadcaster.fee_satoshis_per_kb().await;
        broadcaster.fee_satoshis_per_kb().await;
    }
}
