use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use chain_gang::{
    interface::{Balance, BlockchainInterface, Utxo},
    messages::{OutPoint, Tx},
};
use chrono::prelude::DateTime;
use chrono::Utc;
use tokio::sync::{Mutex, RwLock};

use crate::{
    address_watcher::AddressWatcher,
    blockchain_factory::{blockchain_factory, Backend},
    broadcaster::{factory::broadcaster_factory, BroadcastError, TxBroadcaster, MAPI_LITE},
    client::{Client, FundRequest, FundingSpendPlan, InflightState},
    config::{ClientConfig, Config},
    dynamic_config::DynamicConfig,
    idempotency::{IdempotencyStore, RecordKey},
    responses::{
        BlockchainConnectionStatus, CodedError, ErrorCode, FundingResponseJson, OutpointResponse,
        StatusResponse, TxResponse,
    },
    util::tx_as_hexstr,
};

#[derive(Clone, Debug, Default)]
pub struct FundingResponse {
    pub outpoints: Vec<OutPoint>,
    pub txs: Vec<Tx>,
}

#[derive(Debug)]
pub struct MultipleTxFundError {
    pub code: ErrorCode,
    pub description: String,
    pub partial: Option<FundingResponse>,
}

impl MultipleTxFundError {
    pub fn complete(code: ErrorCode, description: impl Into<String>) -> Self {
        Self {
            code,
            description: description.into(),
            partial: None,
        }
    }

    /// Some transactions broadcast and some did not; the successful ones are
    /// carried in `partial` so the caller can return them to the client.
    pub fn partial(description: impl Into<String>, completed: FundingResponse) -> Self {
        Self {
            code: ErrorCode::PartialBroadcast,
            description: description.into(),
            partial: Some(completed),
        }
    }
}

impl FundingResponse {
    pub fn to_response(&self) -> Result<FundingResponseJson, String> {
        // The value and locking script are read from the transaction that
        // actually pays each outpoint, not echoed from the request. A client
        // signing this outpoint commits to both under BIP-143, so it needs to
        // be able to verify what was paid rather than be told what was asked
        // for.
        let mut outpoints = Vec::with_capacity(self.outpoints.len());
        for op in &self.outpoints {
            let hash = op.hash.encode();
            let tx = self
                .txs
                .iter()
                .find(|tx| tx.hash() == op.hash)
                .ok_or_else(|| format!("No transaction in response for outpoint {hash}"))?;
            let output = tx
                .outputs
                .get(op.index as usize)
                .ok_or_else(|| format!("Transaction {hash} has no output at index {}", op.index))?;
            outpoints.push(OutpointResponse {
                hash,
                index: op.index,
                satoshi: output.satoshis,
                locking_script: hex::encode(&output.lock_script.0),
            });
        }
        let mut txs = Vec::with_capacity(self.txs.len());
        for tx in &self.txs {
            txs.push(TxResponse {
                tx: tx_as_hexstr(tx)?,
            });
        }
        Ok(FundingResponseJson {
            outpoints,
            txs,
            // A freshly built response is never a replay; the flag is set on
            // the way out of the idempotency store.
            replayed: false,
        })
    }
}

#[derive(Clone)]
pub struct PreparedFunding {
    pub client_id: String,
    pub txs: Vec<Tx>,
    pub no_of_outpoints: u32,
    pub spend_plan: FundingSpendPlan,
}

/// Service data
pub struct Service {
    blockchain_status: RwLock<BlockchainConnectionStatus>,
    blockchain_update_time: RwLock<Option<SystemTime>>,
    blockchain_interface: Arc<dyn BlockchainInterface + Send + Sync>,
    /// Where funding transactions are sent. Wraps `blockchain_interface`
    /// unless `[mapi_lite]` is configured. See [`crate::broadcaster`].
    broadcaster: Arc<dyn TxBroadcaster>,
    clients: RwLock<HashMap<String, Arc<RwLock<Client>>>>,
    dynamic_config: Mutex<DynamicConfig>,
    /// `dynamic_config.wif_env_prefix` (CS-521).
    wif_env_prefix: String,
    admin_api_key: Option<String>,
    /// `admin_api_key` and every client's `api_key` are required: a
    /// production network without the opt-out (CS-514).
    auth_required: bool,
    network: String,
    idempotency: Mutex<IdempotencyStore>,
    funding_limits: crate::funding_limits::FundingLimits,
    /// Set only for backends that must be told which addresses to follow.
    address_watcher: Option<Arc<dyn AddressWatcher>>,
    /// The last mapi-lite probe verdict, reused by `GET /ready` until it
    /// goes stale. See [`Service::mapi_lite_health`].
    mapi_health: Mutex<Option<CachedHealth>>,
    /// How long a probe verdict is reused for. Zero without `[mapi_lite]`,
    /// where nothing is ever probed.
    mapi_health_ttl: Duration,
    /// `Config::fund_worst_case`, for `GET /status` (CS-624).
    fund_worst_case: Option<Duration>,
    /// Consecutive failures reaching the blockchain interface, so a run of
    /// them reads as a run and recovery from one is announced.
    chain_health: Mutex<ChainHealth>,
    /// How old cached chain state may be before a request refreshes it.
    chain_state_max_age: Duration,
    /// How old a chain read may be before `GET /ready` fails; `None` is off.
    ready_max_chain_read_age: Option<Duration>,
    /// One lock per client, held for the length of a chain-state refresh, so a
    /// client is never fetched twice at once -- whether the refresh was started
    /// by a request, in the background, or by the periodic sweep (SR-FUND-032).
    /// Two fetches of the same client in flight return the same answer and
    /// spend the interface's allowance twice for it.
    chain_refresh_locks: std::sync::Mutex<HashMap<String, Arc<Mutex<()>>>>,
    /// The rate every client currently costs its transactions at, in satoshis
    /// per kilobyte (CS-451).
    ///
    /// Held here as well as on each client so that a client added at runtime
    /// starts at the rate in force rather than the one in the config file,
    /// which a fee quote may have superseded.
    fee_satoshis_per_kb: Mutex<u64>,
    /// `fees.max_satoshis_per_kb`: no quote may set a rate above it (CS-523).
    max_satoshis_per_kb: u64,
    /// Where each client's in-flight funding state is kept across a restart
    /// (CS-465). See [`Config::inflight_state_path`].
    inflight_state_path: PathBuf,
    /// Serialises writes to that file, so two commits finishing together
    /// cannot race and leave the older snapshot on disk. Holds the ticket of
    /// the newest commit the file on disk is known to include (CS-495).
    inflight_save: Mutex<u64>,
    /// Tickets handed to commits as they ask for a save, in order. A save
    /// taken after reading this includes every commit holding a ticket up to
    /// the value read, since each commit is applied before it takes one.
    inflight_commits: std::sync::atomic::AtomicU64,
    /// How many times the file has actually been written, so a test can tell
    /// a shared save from one per commit.
    #[cfg(test)]
    inflight_writes: std::sync::atomic::AtomicU64,
}

/// Version of the in-flight state file's layout.
const INFLIGHT_FILE_VERSION: u32 = 1;

/// The on-disk form of every client's in-flight funding state (CS-465).
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct InflightFile {
    version: u32,
    #[serde(default)]
    clients: BTreeMap<String, InflightState>,
}

/// Read the in-flight state a previous run left, keyed by client id.
///
/// A missing file is normal -- first start, or nothing was ever in flight. An
/// unreadable one is logged as an error and treated as empty rather than
/// refusing to start: the service then behaves as it did before this file
/// existed, which is the degraded case CS-465 describes, and the error says
/// so. Refusing to start would take every read path down over state that only
/// matters for the next few minutes.
fn load_inflight_file(path: &Path) -> BTreeMap<String, InflightState> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return BTreeMap::new(),
        Err(e) => {
            log::error!(
                "cannot read in-flight funding state from {}: {e}. Starting without it, so \
                 outpoints spent shortly before this restart may be handed out again.",
                path.display()
            );
            return BTreeMap::new();
        }
    };
    match serde_json::from_str::<InflightFile>(&text) {
        Ok(file) if file.version == INFLIGHT_FILE_VERSION => file.clients,
        Ok(file) => {
            log::error!(
                "in-flight funding state in {} is version {}, expected {}. Starting without it.",
                path.display(),
                file.version,
                INFLIGHT_FILE_VERSION
            );
            BTreeMap::new()
        }
        Err(e) => {
            log::error!(
                "in-flight funding state in {} is not readable: {e}. Starting without it, so \
                 outpoints spent shortly before this restart may be handed out again.",
                path.display()
            );
            BTreeMap::new()
        }
    }
}

/// Write `file` to `path` so that a reader never sees half of it: into a
/// sibling first, flushed, then renamed over the original.
///
/// Compact rather than pretty-printed: it is written on the request path, and
/// pretty-printing roughly doubles what has to be serialised and flushed
/// (CS-495). `jq . <file>` reads it just as well.
fn write_inflight_file(path: &Path, file: &InflightFile) -> std::io::Result<()> {
    use std::io::Write;
    let text = serde_json::to_string(file).map_err(std::io::Error::other)?;
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".tmp");
    let temporary = PathBuf::from(temporary);
    {
        let mut out = std::fs::File::create(&temporary)?;
        out.write_all(text.as_bytes())?;
        out.sync_all()?;
    }
    std::fs::rename(&temporary, path)
}

/// A run of failures talking to the blockchain interface.
///
/// The bug report's complaint was not only that refreshes failed, but that
/// nothing said when they stopped failing: the warnings were "the last log
/// lines", leaving no way to tell a service that had recovered from one still
/// in trouble. Counting the run gives each warning the length of it, and lets
/// recovery be announced once.
#[derive(Default)]
struct ChainHealth {
    consecutive_failures: u64,
    /// When the blockchain interface last answered. What `GET /ready` holds a
    /// service with no clients to (CS-505).
    last_success: Option<Instant>,
}

/// A mapi-lite probe verdict and the moment it was taken.
struct CachedHealth {
    taken_at: Instant,
    /// The probe's own error text, kept for the log line rather than for the
    /// `/health` body, which stays generic.
    verdict: Result<(), String>,
}

/// Funding wallets held by more than one client, each with the clients that
/// hold it, sorted (CS-521). Read at startup, when nothing else holds the
/// locks; a client whose lock is taken is left out rather than waited for.
fn wallets_shared_by(clients: &HashMap<String, Arc<RwLock<Client>>>) -> Vec<(String, Vec<String>)> {
    let mut by_address: HashMap<String, Vec<String>> = HashMap::new();
    for (id, client) in clients {
        if let Ok(client) = client.try_read() {
            by_address
                .entry(client.get_address())
                .or_default()
                .push(id.clone());
        }
    }
    let mut shared: Vec<_> = by_address
        .into_iter()
        .filter(|(_, ids)| ids.len() > 1)
        .map(|(address, mut ids)| {
            ids.sort();
            (address, ids)
        })
        .collect();
    shared.sort();
    shared
}

impl Service {
    fn build(
        config: &Config,
        backend: Backend,
        broadcaster: Arc<dyn TxBroadcaster>,
    ) -> Result<Service, String> {
        let Backend {
            interface: blockchain_interface,
            address_watcher,
        } = backend;
        let mut clients = HashMap::new();
        let inflight_state_path = config.inflight_state_path();
        let mut inflight = load_inflight_file(&inflight_state_path);
        let mut restore = |client_id: &str, client: &mut Client| {
            if let Some(state) = inflight.remove(client_id) {
                log::info!(
                    "restoring {} reserved outpoint(s) and {} pending change output(s) for \
                     {client_id} from before the restart",
                    state.reserved.len(),
                    state.pending_change.len()
                );
                client.restore_inflight_state(state);
            }
        };

        if let Some(clients_config) = &config.client {
            for client_config in clients_config {
                let resolved = client_config.clone().resolve_secrets()?;
                let mut client = Client::try_new(&resolved, &config.blockchain_interface.network)?;
                client.set_fee_satoshis_per_kb(config.fees.satoshis_per_kb);
                restore(&client_config.client_id, &mut client);
                clients.insert(
                    client_config.client_id.clone(),
                    Arc::new(RwLock::new(client)),
                );
            }
        }

        let dynamic_config = DynamicConfig::new(config)?;
        for client_config in &dynamic_config.contents.clients {
            crate::secrets::warn_plaintext_client_secrets(
                &client_config.client_id,
                &client_config.wif_key,
                client_config.api_key.as_deref(),
            );
            let resolved = client_config.clone().resolve_secrets()?;
            let mut client = Client::try_new(&resolved, &config.blockchain_interface.network)?;
            client.set_fee_satoshis_per_kb(config.fees.satoshis_per_kb);
            restore(&client_config.client_id, &mut client);
            clients.insert(
                client_config.client_id.clone(),
                Arc::new(RwLock::new(client)),
            );
        }

        // Clients that share a wallet cannot be told apart by the service and
        // will spend each other's coins. `POST /client` now refuses them
        // (CS-521), but a pair added before that is still in the dynamic
        // config: say so loudly, without refusing to start a deployment that
        // may be serving.
        for (address, ids) in wallets_shared_by(&clients) {
            log::error!(
                "clients {ids:?} share one funding wallet ({address}): they will spend each \
                 other's coins, and one may have been added to take over the other's. Remove \
                 the one that should not be there from the dynamic config."
            );
        }

        Ok(Service {
            blockchain_status: RwLock::new(BlockchainConnectionStatus::Unknown),
            blockchain_update_time: RwLock::new(None),
            blockchain_interface,
            broadcaster,
            clients: RwLock::new(clients),
            dynamic_config: Mutex::new(dynamic_config),
            wif_env_prefix: config.dynamic_config.wif_env_prefix.clone(),
            admin_api_key: config
                .web_interface
                .admin_api_key
                .clone()
                .filter(|key| !key.is_empty()),
            auth_required: config.is_production_network()
                && !config.web_interface.allow_unauthenticated,
            network: config.blockchain_interface.network.clone(),
            idempotency: Mutex::new(IdempotencyStore::new(
                config.idempotency.ttl(),
                config.idempotency.max_entries,
            )),
            funding_limits: crate::funding_limits::FundingLimits::new(
                config.funding_limits.clone(),
            ),
            address_watcher,
            mapi_health: Mutex::new(None),
            chain_health: Mutex::new(ChainHealth::default()),
            chain_state_max_age: config.service.chain_state_max_age(),
            ready_max_chain_read_age: config.service.ready_max_chain_read_age(),
            chain_refresh_locks: std::sync::Mutex::new(HashMap::new()),
            fee_satoshis_per_kb: Mutex::new(config.fees.satoshis_per_kb),
            max_satoshis_per_kb: config.fees.max_satoshis_per_kb,
            inflight_state_path,
            inflight_save: Mutex::new(0),
            inflight_commits: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            inflight_writes: std::sync::atomic::AtomicU64::new(0),
            fund_worst_case: config.fund_worst_case(),
            mapi_health_ttl: if config.mapi_lite.is_some() {
                crate::config::MAPI_LITE_PROBE_TIMEOUT
            } else {
                Duration::ZERO
            },
        })
    }

    /// Note that the blockchain interface answered.
    ///
    /// Announces recovery once, naming how long the run of failures was. The
    /// bug report asked for exactly this: without it, warnings simply stop and
    /// an operator cannot tell a recovered service from a stuck one.
    /// Returns the length of the run it recovered from, so the transition is
    /// testable without reading the log it also writes.
    async fn record_chain_success(&self) -> Option<u64> {
        let mut health = self.chain_health.lock().await;
        health.last_success = Some(Instant::now());
        if health.consecutive_failures == 0 {
            return None;
        }
        let recovered_from = health.consecutive_failures;
        health.consecutive_failures = 0;
        log::info!("blockchain interface recovered after {recovered_from} consecutive failure(s)");
        Some(recovered_from)
    }

    /// Length of the current run of failures. Zero when the last attempt
    /// worked.
    #[cfg(test)]
    async fn consecutive_chain_failures(&self) -> u64 {
        self.chain_health.lock().await.consecutive_failures
    }

    /// Note that the blockchain interface did not answer, and say so with the
    /// length of the run so far.
    async fn record_chain_failure(&self, error: &str) {
        let mut health = self.chain_health.lock().await;
        health.consecutive_failures += 1;
        log::warn!(
            "blockchain interface failure #{}: {error}",
            health.consecutive_failures
        );
    }

    /// Claim an idempotency key for a funding request.
    ///
    /// The lock is taken and released inside this call, so it is never held
    /// across a broadcast.
    pub async fn reserve_idempotency(
        &self,
        key: RecordKey,
        fingerprint: &str,
    ) -> crate::idempotency::Reservation {
        self.idempotency.lock().await.reserve(key, fingerprint)
    }

    /// Retain a funding outcome so a retry with the same key replays it.
    pub async fn complete_idempotency(
        &self,
        key: RecordKey,
        fingerprint: &str,
        outcome: crate::idempotency::Outcome,
    ) {
        self.idempotency
            .lock()
            .await
            .complete(key, fingerprint, outcome);
    }

    /// Drop a reservation because nothing was broadcast, letting the client
    /// retry the same key.
    pub async fn release_idempotency(&self, key: &RecordKey) {
        self.idempotency.lock().await.release(key);
    }

    /// Checks a payout against `[funding_limits]` and holds it against the
    /// client's window (CS-530). See [`crate::funding_limits`].
    pub fn hold_funding(
        &self,
        client_id: &str,
        satoshis: u64,
    ) -> Result<Option<crate::funding_limits::Hold>, crate::funding_limits::LimitExceeded> {
        self.funding_limits.check_and_hold(client_id, satoshis)
    }

    /// Gives back a payout held for a request that broadcast nothing.
    pub fn release_funding(&self, hold: &crate::funding_limits::Hold) {
        self.funding_limits.release(hold);
    }

    /// Create a new Service from the provided config
    pub async fn new(config: &Config) -> Result<Service, String> {
        let backend = blockchain_factory(config)?;

        // Warned about, not fatal (CS-505, SR-BCHN-018) -- for the reason the
        // mapi-lite probe below is not: exiting ties this service's lifecycle
        // to an upstream it cannot fix. A WhatsOnChain that answered one 429
        // used to stop startup, and the restart that followed hit the same
        // rate limit. Serving degraded keeps /status and /health up, and
        // GET /ready reports the service unready until a chain read succeeds
        // (SR-BCHN-017).
        if let Err(e) = backend.interface.status().await {
            log::warn!(
                "Unable to reach the blockchain interface at startup: {e}. Funding will fail \
                 until it answers; GET /ready reports the service unready meanwhile."
            );
        }

        // The write path. Without [mapi_lite] it wraps the interface probed
        // just above, so only mapi-lite needs a probe of its own -- and gets
        // one, to tell the operator at startup rather than on the first /fund.
        //
        // A failed probe is reported but does not stop startup, the way a
        // refused address import does not (see
        // watch_configured_client_addresses). Refusing to start ties this
        // service's lifecycle to mapi-lite's: the container health check would
        // restart the process, the probe would fail again, and the service
        // would sit in a restart loop -- taking /status, balances and every
        // other read path, none of which need mapi-lite, down with it, and
        // unable to recover on its own. Serving degraded and saying so through
        // GET /ready leaves the operator a service that heals when mapi-lite
        // comes back.
        //
        // A network mismatch is the exception (CS-624): it is the
        // configuration, not mapi-lite being away, and no restart or wait
        // mends it. When the probe hears which network mapi-lite's nodes are
        // on and it is not this service's, startup stops.
        let broadcaster = broadcaster_factory(config, Arc::clone(&backend.interface))?;
        if let Some(mapi_lite) = &config.mapi_lite {
            if let Err(e) = broadcaster.health_check().await {
                log::warn!(
                    "Unable to reach mapi-lite at {} at startup: {e}. Funding will fail until it \
                     is reachable; GET /ready reports the service unready meanwhile.",
                    mapi_lite.base_url()
                );
            }
            if let Some(why) = network_mismatch(
                broadcaster.published_network(),
                &config.blockchain_interface.network,
            ) {
                return Err(why);
            }
            // So is a token mapi-lite refuses (CS-632).
            if broadcaster.token_refused() {
                return Err(
                    "mapi-lite refuses mapi_lite.auth_token, or needs one and none is set (401): \
                     every funding through it would be refused. Set it to the token in \
                     mapi-lite's auth_token, alone"
                        .to_string(),
                );
            }
        }

        let service = Self::build(config, backend, broadcaster)?;
        // Before the first balance read, so the backend already knows the
        // addresses it is about to be asked about.
        service.watch_configured_client_addresses().await;
        service.update_balances().await;
        Ok(service)
    }

    #[cfg(test)]
    pub async fn new_for_test(
        config: &Config,
        blockchain_interface: Arc<dyn BlockchainInterface + Send + Sync>,
    ) -> Service {
        Self::new_for_test_with_watcher(config, blockchain_interface, None).await
    }

    /// As production without `[mapi_lite]`: broadcasts go through the
    /// blockchain interface.
    #[cfg(test)]
    pub async fn new_for_test_with_watcher(
        config: &Config,
        blockchain_interface: Arc<dyn BlockchainInterface + Send + Sync>,
        address_watcher: Option<Arc<dyn AddressWatcher>>,
    ) -> Service {
        use crate::broadcaster::woc::WocBroadcaster;
        let broadcaster: Arc<dyn TxBroadcaster> = Arc::new(WocBroadcaster::new(
            Arc::clone(&blockchain_interface),
            &config.blockchain_interface.interface_type,
        ));
        Self::new_for_test_full(config, blockchain_interface, address_watcher, broadcaster).await
    }

    /// Reads through `blockchain_interface`, writes through `broadcaster`,
    /// as production with `[mapi_lite]` does.
    #[cfg(test)]
    pub async fn new_for_test_with_broadcaster(
        config: &Config,
        blockchain_interface: Arc<dyn BlockchainInterface + Send + Sync>,
        broadcaster: Arc<dyn TxBroadcaster>,
    ) -> Service {
        Self::new_for_test_full(config, blockchain_interface, None, broadcaster).await
    }

    #[cfg(test)]
    async fn new_for_test_full(
        config: &Config,
        blockchain_interface: Arc<dyn BlockchainInterface + Send + Sync>,
        address_watcher: Option<Arc<dyn AddressWatcher>>,
        broadcaster: Arc<dyn TxBroadcaster>,
    ) -> Service {
        blockchain_interface
            .status()
            .await
            .expect("Unable to connect to test blockchain.");

        let service = Self::build(
            config,
            Backend {
                interface: blockchain_interface,
                address_watcher,
            },
            broadcaster,
        )
        .expect("Invalid client configuration in test setup");
        service.watch_configured_client_addresses().await;
        service.update_balances().await;
        service
    }

    /// Ask the backend to watch every configured client's funding address.
    ///
    /// A failure is reported but does not stop startup: the node may refuse
    /// the import for a reason the operator already knows about, such as a
    /// descriptor wallet, and refusing to run would be worse than a warning
    /// they can act on. The warning names the consequence, because the symptom
    /// -- a zero balance from a reachable node -- does not point at its cause.
    async fn watch_configured_client_addresses(&self) {
        let Some(watcher) = self.address_watcher.as_ref() else {
            return;
        };
        for client in self.client_handles().await {
            let address = client.read().await.get_address();
            if let Err(error) = watcher.watch_address(&address).await {
                log::warn!(
                    "Could not ask the node to watch {address}: {error}. \
                     Its balance will read zero and funding will be refused until \
                     the node tracks it."
                );
            }
        }
    }

    /// As above, for one address, when a client is added at runtime.
    async fn watch_client_address(&self, client_id: &str) {
        let Some(watcher) = self.address_watcher.as_ref() else {
            return;
        };
        let Some(client) = self.client_handle(client_id).await else {
            return;
        };
        let address = client.read().await.get_address();
        if let Err(error) = watcher.watch_address(&address).await {
            log::warn!(
                "Could not ask the node to watch {address} for new client {client_id}: {error}. \
                 Its balance will read zero and funding will be refused until the node tracks it."
            );
        }
    }

    async fn client_handle(&self, client_id: &str) -> Option<Arc<RwLock<Client>>> {
        self.clients.read().await.get(client_id).cloned()
    }

    /// The lock that serialises chain-state refreshes of `client_id`.
    ///
    /// A std mutex guards the map, never held across an await; the lock it
    /// hands out is tokio's, held across the fetch.
    fn chain_refresh_lock(&self, client_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self
            .chain_refresh_locks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Arc::clone(locks.entry(client_id.to_string()).or_default())
    }

    /// Write every client's in-flight funding state to disk (CS-465), or wait
    /// for a write that already includes the caller's commit (CS-495).
    ///
    /// Called after each commit, so what is on disk is never behind what has
    /// been broadcast by more than the moment between the two. A failure is
    /// logged, not returned: the transaction is already on the network, and
    /// the caller has nothing it could do with the error.
    ///
    /// Commits share writes. Each takes a ticket, then queues for the save
    /// lock; whoever holds it snapshots every client and writes, and that
    /// snapshot includes every commit ticketed before it was taken. A commit
    /// that reaches the lock to find its ticket already written returns at
    /// once. So the file is still written before any caller is answered, but
    /// a burst of commits costs one write between them, not one each -- each
    /// write serialises and flushes the whole state, and doing that per commit
    /// is what capped a client at the rate one write takes.
    ///
    /// The snapshot is taken under the save lock, not before it. Two commits
    /// finishing together would otherwise each snapshot, then write in either
    /// order -- and whichever snapshot is older could land last.
    pub async fn save_inflight_state(&self) {
        use std::sync::atomic::Ordering;

        // Taken after the caller applied its commit, so any snapshot that
        // reads the counter first and the clients second includes it.
        let ticket = self.inflight_commits.fetch_add(1, Ordering::SeqCst) + 1;
        let mut written_through = self.inflight_save.lock().await;
        if *written_through >= ticket {
            return;
        }
        let covers = self.inflight_commits.load(Ordering::SeqCst);

        let handles: Vec<(String, Arc<RwLock<Client>>)> = self
            .clients
            .read()
            .await
            .iter()
            .map(|(client_id, client)| (client_id.clone(), Arc::clone(client)))
            .collect();

        let mut clients = BTreeMap::new();
        for (client_id, client) in handles {
            let state = client.read().await.inflight_state();
            if !state.is_empty() {
                clients.insert(client_id, state);
            }
        }
        let file = InflightFile {
            version: INFLIGHT_FILE_VERSION,
            clients,
        };

        // Off the async worker: the flush can take milliseconds, and the
        // worker has other requests to serve meanwhile.
        let path = self.inflight_state_path.clone();
        let written = tokio::task::spawn_blocking(move || write_inflight_file(&path, &file))
            .await
            .unwrap_or_else(|e| Err(std::io::Error::other(e)));
        #[cfg(test)]
        self.inflight_writes.fetch_add(1, Ordering::SeqCst);
        match written {
            Ok(()) => *written_through = covers,
            // Not marked written, so the next commit tries again rather than
            // being told a failed write covered it.
            Err(e) => log::error!(
                "cannot save in-flight funding state to {}: {e}. A restart before the chain \
                 catches up could hand the same outpoints out again.",
                self.inflight_state_path.display()
            ),
        }
    }

    async fn client_handles(&self) -> Vec<Arc<RwLock<Client>>> {
        self.clients.read().await.values().cloned().collect()
    }

    /// `dynamic_config.wif_env_prefix`: what a runtime client's `wif_env` must
    /// start with (CS-521).
    pub fn wif_env_prefix(&self) -> &str {
        &self.wif_env_prefix
    }

    /// Why a new client may not join `clients`, if it may not (CS-521).
    ///
    /// * **The same wallet.** Two clients funding from one key is never
    ///   meaningful -- each tracks its own UTXOs, so they would spend the same
    ///   coins -- and it is exactly what taking over another client's wallet
    ///   looks like, however the key was supplied: a `wif_env` naming that
    ///   client's variable or the literal key. Comparing addresses catches
    ///   both.
    async fn conflict_with_existing(
        clients: &HashMap<String, Arc<RwLock<Client>>>,
        address: &str,
    ) -> Option<CodedError> {
        for existing in clients.values() {
            if existing.read().await.get_address() == address {
                return Some(CodedError::new(
                    ErrorCode::InvalidRequest,
                    "this key is already the funding key of another client".to_string(),
                ));
            }
        }
        None
    }

    pub async fn add_client(&self, client_config: &ClientConfig) -> Result<(), CodedError> {
        // Its /fund, balance and address would be open to anyone (CS-514).
        if self.auth_required && client_config.api_key.as_deref().is_none_or(str::is_empty) {
            return Err(CodedError::new(
                ErrorCode::InvalidRequest,
                "this service requires every client to have an api_key; provide api_key_env",
            ));
        }
        // The dynamic config file would hold the key itself (CS-526). The
        // message names neither the key nor any part of it (SR-SEC-010).
        if !self
            .dynamic_config
            .lock()
            .await
            .accepts_wif(&client_config.wif_key)
        {
            return Err(CodedError::new(
                ErrorCode::InvalidRequest,
                "a raw wif would be stored in the dynamic config file; provide wif_env, naming \
                 an environment variable that holds it, or set \
                 dynamic_config.allow_plaintext_wif for development",
            ));
        }
        crate::secrets::warn_plaintext_client_secrets(
            &client_config.client_id,
            &client_config.wif_key,
            client_config.api_key.as_deref(),
        );
        // A bad secret reference or an unusable WIF is a problem with the
        // submitted client, not a service fault.
        let resolved = client_config
            .clone()
            .resolve_secrets()
            .map_err(|e| CodedError::new(ErrorCode::InvalidRequest, e))?;
        let mut client = Client::try_new(&resolved, &self.network)
            .map_err(|e| CodedError::new(ErrorCode::InvalidRequest, e))?;
        // The rate in force, which a fee quote may have moved on from what the
        // config file says, so a client added now costs its transactions the
        // same as one that has been here since startup.
        client.set_fee_satoshis_per_kb(*self.fee_satoshis_per_kb.lock().await);
        let address = client.get_address();
        let new_client = Arc::new(RwLock::new(client));
        {
            let mut clients = self.clients.write().await;
            if clients.contains_key(&client_config.client_id) {
                return Err(CodedError::new(
                    ErrorCode::ClientExists,
                    format!("Client already exists: {}", client_config.client_id),
                ));
            }
            // Checked under the write lock, so two requests cannot both pass.
            if let Some(error) = Self::conflict_with_existing(&clients, &address).await {
                return Err(error);
            }
            clients.insert(client_config.client_id.clone(), Arc::clone(&new_client));
        }
        // Persisting failed, so undo the in-memory insert. This one really is
        // a service fault.
        if let Err(error) = self.dynamic_config.lock().await.add(client_config) {
            self.clients.write().await.remove(&client_config.client_id);
            return Err(CodedError::internal(error));
        }
        // A client added at runtime needs watching too, or its balance reads
        // zero until the next restart.
        self.watch_client_address(&client_config.client_id).await;
        Ok(())
    }

    pub async fn delete_client(&self, client_id: &str) -> Result<(), String> {
        self.dynamic_config.lock().await.remove(client_id)?;
        self.clients.write().await.remove(client_id);
        Ok(())
    }

    /// Return the Service status
    pub async fn get_status(&self) -> StatusResponse {
        let update_time = match *self.blockchain_update_time.read().await {
            Some(time) => {
                let datetime = DateTime::<Utc>::from(time);
                datetime.format("%Y-%m-%d %H:%M:%S").to_string()
            }
            None => "None".to_string(),
        };
        StatusResponse {
            version: env!("CARGO_PKG_VERSION").to_string(),
            blockchain_status: *self.blockchain_status.read().await,
            blockchain_update_time: update_time,
            broadcaster: self.broadcaster.name().to_string(),
            fund_worst_case_seconds: self.fund_worst_case.map(|bound| bound.as_secs()),
        }
    }

    async fn get_block_headers(&self) {
        let status = match self.blockchain_interface.get_block_headers().await {
            Ok(_) => {
                self.record_chain_success().await;
                BlockchainConnectionStatus::Connected
            }
            Err(e) => {
                self.record_chain_failure(&format!("get_block_headers: {e:?}"))
                    .await;
                BlockchainConnectionStatus::Failed
            }
        };
        *self.blockchain_status.write().await = status;
        *self.blockchain_update_time.write().await = Some(SystemTime::now());
    }

    /// Update client balances at startup.
    pub async fn update_balances(&self) {
        let handles = self.client_handles().await;
        if handles.is_empty() {
            self.get_block_headers().await;
        } else {
            for client in handles {
                let status = match client
                    .write()
                    .await
                    .update_balance(self.blockchain_interface.as_ref())
                    .await
                {
                    Ok(_) => {
                        let _ = self.record_chain_success().await;
                        BlockchainConnectionStatus::Connected
                    }
                    Err(e) => {
                        self.record_chain_failure(&format!("update_balance: {e:?}"))
                            .await;
                        BlockchainConnectionStatus::Failed
                    }
                };
                *self.blockchain_status.write().await = status;
                *self.blockchain_update_time.write().await = Some(SystemTime::now());
            }
        }
    }

    /// Refresh balances without holding client locks during blockchain I/O.
    /// Take the fee rate from mapi-lite's quote, if it offers one, and apply
    /// it to every client (CS-451).
    ///
    /// Called on the same sweep that refreshes balances rather than on the
    /// funding path, so that costing a transaction never waits on a request to
    /// mapi-lite -- CS-431 had just finished taking such calls off that path.
    /// The cost is that the rate can be up to one sweep out of date, which for
    /// a fee quote is a good trade.
    ///
    /// Any failure leaves the current rate alone, so a mapi-lite that is down
    /// stops the rate changing rather than stopping funding.
    pub async fn refresh_fee_rate(&self) {
        let Some(rate) = self.broadcaster.fee_satoshis_per_kb().await else {
            return;
        };
        if rate == 0 {
            return;
        }
        // A quote above the ceiling is ignored, not clamped: a rate that high
        // is either forged or a mistake, and neither should be half-believed
        // (CS-523).
        if rate > self.max_satoshis_per_kb {
            log::warn!(
                "ignoring a mapi-lite fee quote of {rate} sat/KB, above fees.max_satoshis_per_kb \
                 ({}); keeping the current rate",
                self.max_satoshis_per_kb
            );
            return;
        }
        let changed = {
            let mut current = self.fee_satoshis_per_kb.lock().await;
            let changed = *current != rate;
            *current = rate;
            changed
        };
        if changed {
            log::info!("fee rate now {rate} sat/KB, from the mapi-lite fee quote");
        }
        for client in self.client_handles().await {
            client.write().await.set_fee_satoshis_per_kb(rate);
        }
    }

    /// The rate transactions are currently costed at, in satoshis per KB.
    #[cfg(test)]
    pub async fn fee_satoshis_per_kb(&self) -> u64 {
        *self.fee_satoshis_per_kb.lock().await
    }

    pub async fn refresh_balances(service: &Arc<Service>) {
        service.refresh_fee_rate().await;
        let (blockchain, handles) = {
            let clients: Vec<(String, Arc<RwLock<Client>>)> = service
                .clients
                .read()
                .await
                .iter()
                .map(|(id, client)| (id.clone(), Arc::clone(client)))
                .collect();
            let mut snapshot = Vec::with_capacity(clients.len());
            for (client_id, client) in clients {
                let address = client.read().await.get_address();
                snapshot.push((client_id, client, address));
            }
            (Arc::clone(&service.blockchain_interface), snapshot)
        };

        if handles.is_empty() {
            service.get_block_headers().await;
            return;
        }

        // Skip whatever a request refreshed inside the window. At the default
        // -- the window equal to this period -- a client touched by traffic
        // since the last tick is already current, so the sweep pays only for
        // the quiet ones.
        //
        // Skip, too, a client whose refresh is already running -- a request's,
        // or one started in the background (SR-FUND-032). It will bring that
        // client up to date, and fetching alongside it would spend the
        // allowance twice for one answer. Each client is applied as soon as it
        // is fetched and its lock released, so a request waiting on one client
        // does not wait for the whole sweep.
        let max_age = service.chain_state_max_age;
        let mut status = BlockchainConnectionStatus::Connected;
        for (client_id, client, address) in handles {
            if client.read().await.chain_state_is_fresh(max_age) {
                continue;
            }
            let Ok(_refreshing) = service.chain_refresh_lock(&client_id).try_lock_owned() else {
                continue;
            };
            match fetch_chain_state(blockchain.as_ref(), &address).await {
                Ok(utxo) => client.write().await.apply_chain_state(utxo),
                Err(e) => {
                    service.record_chain_failure(&e).await;
                    status = BlockchainConnectionStatus::Failed;
                }
            }
        }
        if matches!(status, BlockchainConnectionStatus::Connected) {
            let _ = service.record_chain_success().await;
        }
        *service.blockchain_status.write().await = status;
        *service.blockchain_update_time.write().await = Some(SystemTime::now());
    }

    /// Refresh one client's chain state unless the cache is still fresh.
    ///
    /// The refresh before building a funding transaction exists so the service
    /// does not select an input something else has already spent. Fetching it
    /// again when the periodic refresh took it moments ago buys nothing
    /// against that, and it is the difference between one request per client
    /// per period and one per request: on a busy service the second is what
    /// runs into the rate limit and makes funding calls queue behind it.
    ///
    /// The window is `service.chain_state_max_age_seconds`, defaulting to
    /// `utxo_refresh_period`. Wider is cheaper and staler, and the trade is
    /// the operator's -- see the note on the config field.
    ///
    /// A failed broadcast marks the state stale (see
    /// [`Service::invalidate_chain_state`]), so a conflict caused by building
    /// on a stale view costs one attempt and not more.
    pub async fn refresh_client_chain_state_if_stale(
        service: &Arc<Service>,
        client_id: &str,
    ) -> Result<(), String> {
        let client = service
            .client_handle(client_id)
            .await
            .ok_or_else(|| format!("Unknown client_id {client_id}"))?;
        if client
            .read()
            .await
            .chain_state_is_fresh(service.chain_state_max_age)
        {
            return Ok(());
        }
        let lock = service.chain_refresh_lock(client_id);
        let _refreshing = lock.lock().await;
        // Checked again under the lock. Every request that found the state
        // stale queues here; the first through refreshes it, and the rest find
        // it fresh and reuse that answer instead of each fetching the same one
        // behind the rate limit (SR-FUND-032). A zero window still refreshes
        // for every request, one after another.
        if client
            .read()
            .await
            .chain_state_is_fresh(service.chain_state_max_age)
        {
            return Ok(());
        }
        Self::fetch_and_apply_chain_state(service, client_id).await
    }

    /// Make sure a funding request has chain state it may build on.
    ///
    /// With `[mapi_lite]` configured, a request whose cache is usable but
    /// older than the window does not wait for the refresh: it is funded from
    /// the cached state, and the refresh runs behind it (SR-FUND-031). The
    /// broadcast goes to mapi-lite, so the request has no other reason to touch
    /// the blockchain interface -- and on WhatsOnChain that interface is paced
    /// at a few requests a second, which is what made funding calls queue.
    ///
    /// It still waits when the cache is not usable: before the client's state
    /// has ever been loaded, and after a refused or uncertain broadcast has
    /// marked it untrustworthy. Building on that view is the risk the refresh
    /// exists to prevent.
    ///
    /// Without `[mapi_lite]` nothing changes. The broadcast itself goes through
    /// the paced interface (SR-BCHN-013), so the request waits on it either
    /// way, and a fresh view is worth the same wait.
    pub async fn prepare_chain_state_for_funding(
        service: &Arc<Service>,
        client_id: &str,
    ) -> Result<(), String> {
        if service.mapi_lite_configured() {
            let client = service
                .client_handle(client_id)
                .await
                .ok_or_else(|| format!("Unknown client_id {client_id}"))?;
            let (usable, fresh) = {
                let client = client.read().await;
                (
                    client.chain_state_usable(),
                    client.chain_state_is_fresh(service.chain_state_max_age),
                )
            };
            if usable {
                if !fresh {
                    Self::refresh_chain_state_in_background(service, client_id);
                }
                return Ok(());
            }
        }
        Self::refresh_client_chain_state_if_stale(service, client_id).await
    }

    /// Start a refresh of `client_id` that nobody waits for, unless one is
    /// already running.
    ///
    /// The lock is taken before the task is spawned, so a burst of requests
    /// that all find the state stale starts one refresh, not one each. A
    /// refresh already in flight -- another request's, an earlier background
    /// one, or the periodic sweep's -- will bring the cache up to date, so this
    /// has nothing to add and does nothing.
    fn refresh_chain_state_in_background(service: &Arc<Service>, client_id: &str) {
        let Ok(refreshing) = service.chain_refresh_lock(client_id).try_lock_owned() else {
            return;
        };
        let service = Arc::clone(service);
        let client_id = client_id.to_string();
        tokio::spawn(async move {
            let _refreshing = refreshing;
            // A refresh that finished between the caller's check and taking
            // the lock has already done this one's work.
            let fresh = match service.client_handle(&client_id).await {
                Some(client) => client
                    .read()
                    .await
                    .chain_state_is_fresh(service.chain_state_max_age),
                None => return,
            };
            if fresh {
                return;
            }
            if let Err(e) = Self::fetch_and_apply_chain_state(&service, &client_id).await {
                // Already recorded as a chain failure. The request this was
                // started for has been funded from the cache, so there is
                // nobody to return it to.
                log::warn!("background refresh_client_chain_state failed: {e}");
            }
        });
    }

    /// Mark a client's cached chain state stale, so the next request refreshes
    /// it.
    ///
    /// Cheaper than refreshing here: the caller is already being told its
    /// funding failed and cannot use a fresh answer, and if no further request
    /// arrives the fetch is never made at all.
    async fn invalidate_chain_state(service: &Arc<Service>, client_id: &str) {
        if let Some(client) = service.client_handle(client_id).await {
            client.write().await.invalidate_chain_state();
        }
    }

    /// Refresh one client's balance and UTXO set from the blockchain.
    ///
    /// Waits for any refresh of the same client already in flight, then
    /// fetches regardless: callers of this one want a new answer, not a recent
    /// one.
    pub async fn refresh_client_chain_state(
        service: &Arc<Service>,
        client_id: &str,
    ) -> Result<(), String> {
        let lock = service.chain_refresh_lock(client_id);
        let _refreshing = lock.lock().await;
        Self::fetch_and_apply_chain_state(service, client_id).await
    }

    /// The refresh itself. The caller holds the client's refresh lock.
    async fn fetch_and_apply_chain_state(
        service: &Arc<Service>,
        client_id: &str,
    ) -> Result<(), String> {
        let client = service
            .client_handle(client_id)
            .await
            .ok_or_else(|| format!("Unknown client_id {client_id}"))?;
        let address = client.read().await.get_address();
        let chain_state =
            match fetch_chain_state(service.blockchain_interface.as_ref(), &address).await {
                Ok(state) => {
                    service.record_chain_success().await;
                    state
                }
                Err(e) => {
                    service.record_chain_failure(&e).await;
                    return Err(e);
                }
            };
        client.write().await.apply_chain_state(chain_state);
        *service.blockchain_status.write().await = BlockchainConnectionStatus::Connected;
        *service.blockchain_update_time.write().await = Some(SystemTime::now());
        Ok(())
    }

    pub async fn is_client_id_valid(&self, client_id: &str) -> bool {
        self.clients.read().await.contains_key(client_id)
    }

    pub async fn client_auth_required(&self, client_id: &str) -> bool {
        match self.client_handle(client_id).await {
            Some(client) => client.read().await.api_key().is_some(),
            None => false,
        }
    }

    pub async fn verify_client_api_key(&self, client_id: &str, provided: &str) -> bool {
        match self.client_handle(client_id).await {
            Some(client) => match client.read().await.api_key() {
                Some(expected) => crate::auth::constant_time_eq(provided, expected),
                None => true,
            },
            None => false,
        }
    }

    /// Whether any client has an API key: whether this service authenticates
    /// clients at all (CS-661).
    pub async fn any_client_has_api_key(&self) -> bool {
        let clients = self.clients.read().await;
        for client in clients.values() {
            if client.read().await.api_key().is_some() {
                return true;
            }
        }
        false
    }

    pub async fn clients_without_api_key(&self) -> Vec<String> {
        let clients = self.clients.read().await;
        let mut without = Vec::new();
        for (client_id, client) in clients.iter() {
            if client.read().await.api_key().is_none() {
                without.push(client_id.clone());
            }
        }
        without
    }

    pub async fn client_count(&self) -> usize {
        self.clients.read().await.len()
    }

    /// The write path for funding transactions.
    pub fn broadcaster(&self) -> Arc<dyn TxBroadcaster> {
        Arc::clone(&self.broadcaster)
    }

    /// Whether funding transactions go through mapi-lite.
    pub fn mapi_lite_configured(&self) -> bool {
        self.broadcaster.name() == MAPI_LITE
    }

    /// Whether mapi-lite's nodes are on the chain this service reads (CS-619).
    ///
    /// Neither service checks the other's network, so an FS reading testnet
    /// and a mapi-lite on mainnet -- or two regtest chains -- answered ready
    /// while every funding failed as missing inputs, which mapi-lite calls
    /// worth retrying. The wallet already holds the evidence: the oldest
    /// confirmed UTXO across the clients is a transaction any node on the
    /// same chain knows, and mapi-lite's nodes keep `-txindex`. Their saying
    /// they do not know it is the mismatch. With no confirmed UTXO, or no
    /// answer, there is no evidence either way, and nothing changes.
    async fn same_chain_as_mapi_lite(&self) -> Result<(), String> {
        let mut oldest: Option<(String, i32)> = None;
        for handle in self.client_handles().await {
            if let Some((txid, height)) = handle.read().await.oldest_confirmed_utxo() {
                if oldest.as_ref().is_none_or(|(_, h)| height < *h) {
                    oldest = Some((txid, height));
                }
            }
        }
        let Some((txid, height)) = oldest else {
            return Ok(());
        };
        match self.broadcaster.knows_tx(&txid).await {
            Some(false) => Err(format!(
                "mapi-lite's nodes do not know transaction {txid}, which the blockchain \
                 interface reports confirmed at height {height} in a client's wallet: mapi-lite \
                 is on a different chain, and every funding through it would be refused as \
                 missing inputs. Check network here against mapi-lite's nodes"
            )),
            _ => Ok(()),
        }
    }

    /// Probe mapi-lite for `GET /ready`, reusing a recent verdict.
    ///
    /// `None` when mapi-lite is not configured: there is then nothing to
    /// probe, and `/ready` reports chain reads alone.
    ///
    /// The verdict is cached for [`crate::config::MAPI_LITE_PROBE_TIMEOUT`], because
    /// `/ready` is unauthenticated *and* exempt from the rate limiter
    /// (SR-SEC-013), so without a cache anyone who can reach the port can turn
    /// readiness traffic into an unbounded stream of requests to mapi-lite and
    /// starve the funding path from an endpoint that costs them nothing. One
    /// probe per TTL bounds that, and still answers a readiness probe on any
    /// ordinary interval with a fresh result each time.
    pub async fn mapi_lite_health(&self) -> Option<Result<(), String>> {
        if !self.mapi_lite_configured() {
            return None;
        }

        // Held across the probe on purpose: concurrent callers wait for the
        // one in flight rather than each starting their own, which is the
        // point of the cache. The probe is bounded by MAPI_LITE_PROBE_TIMEOUT.
        let mut cached = self.mapi_health.lock().await;
        if let Some(entry) = cached.as_ref() {
            if entry.taken_at.elapsed() < self.mapi_health_ttl {
                return Some(entry.verdict.clone());
            }
        }

        // At the same time, not one after the other: each is bounded by
        // MAPI_LITE_PROBE_TIMEOUT, which the whole probe has to fit inside.
        let (health, same_chain) = tokio::join!(
            self.broadcaster.health_check(),
            self.same_chain_as_mapi_lite()
        );
        // A network mapi-lite publishes that is not this one is reported
        // first, ready or not: it is the configuration, and the reason
        // anything else fails (CS-624).
        let mismatch = network_mismatch(self.broadcaster.published_network(), &self.network);
        let verdict = match (mismatch, health) {
            (Some(why), _) => Err(why),
            (None, Ok(())) => same_chain,
            (None, Err(e)) => Err(e.to_string()),
        };
        *cached = Some(CachedHealth {
            taken_at: Instant::now(),
            verdict: verdict.clone(),
        });
        Some(verdict)
    }

    /// The keys the authentication policy requires (CS-514), checked with
    /// `FS__` overrides applied and the clients loaded: on a production
    /// network without the opt-out, `admin_api_key` must be set, or
    /// `POST /client` and `DELETE /client` are open, and every client needs an
    /// `api_key`, or its `/fund`, balance and address are. See
    /// [`crate::config::Config::check_auth_policy`] for the rest.
    pub async fn check_auth_policy(&self) -> Result<(), String> {
        if !self.auth_required {
            return Ok(());
        }
        if self.admin_api_key.is_none() {
            return Err(format!(
                "web_interface.admin_api_key is not set, so POST /client and DELETE /client would \
                 be open on {}. Set it (or FS__WEB_INTERFACE__ADMIN_API_KEY), or set \
                 web_interface.allow_unauthenticated = true if a gateway in front of this service \
                 authenticates callers",
                self.network
            ));
        }
        let mut without = self.clients_without_api_key().await;
        if without.is_empty() {
            return Ok(());
        }
        without.sort();
        Err(format!(
            "clients without an api_key: {}. Their /fund, balance and address routes would be \
             open to anyone; give each an api_key (an env: reference keeps it out of the file), or set \
             web_interface.allow_unauthenticated = true if a gateway authenticates callers",
            without.join(", ")
        ))
    }

    /// Whether chain reads are recent enough to fund from (CS-505,
    /// SR-BCHN-017). `Err` names what is too old.
    ///
    /// Every client's last read from the chain must be within
    /// `ready_max_chain_read_age`; with no clients, the blockchain interface's
    /// last answer must be. A client never read at all fails too: that is a
    /// service that started while the chain was unreachable and has not caught
    /// up, and with `[mapi_lite]` funding would otherwise go on from whatever
    /// the cache last held (SR-FUND-031).
    ///
    /// Reads nothing from the chain itself: the periodic sweep already does,
    /// and an unauthenticated probe spending a rate-limited allowance is the
    /// flood SR-SEC-013 guards against.
    pub async fn chain_readiness(&self) -> Result<(), String> {
        let Some(limit) = self.ready_max_chain_read_age else {
            return Ok(());
        };
        let handles: Vec<(String, Arc<RwLock<Client>>)> = self
            .clients
            .read()
            .await
            .iter()
            .map(|(id, client)| (id.clone(), Arc::clone(client)))
            .collect();
        if handles.is_empty() {
            return match self.chain_health.lock().await.last_success {
                Some(at) if at.elapsed() <= limit => Ok(()),
                Some(at) => Err(format!(
                    "the blockchain interface last answered {}s ago (limit {}s)",
                    at.elapsed().as_secs(),
                    limit.as_secs()
                )),
                None => Err("the blockchain interface has not answered yet".to_string()),
            };
        }
        for (client_id, client) in handles {
            match client.read().await.chain_read_age() {
                Some(age) if age <= limit => {}
                Some(age) => {
                    return Err(format!(
                        "client {client_id}'s chain state was last read {}s ago (limit {}s)",
                        age.as_secs(),
                        limit.as_secs()
                    ))
                }
                None => {
                    return Err(format!(
                        "client {client_id}'s chain state has never been read"
                    ))
                }
            }
        }
        Ok(())
    }

    /// Every client's cached balance and last chain read, for the
    /// `fs_client_balance` and `fs_client_chain_state_age` gauges (CS-608).
    /// Reads the cache only; nothing reaches the chain.
    pub async fn client_samples(&self) -> Vec<crate::metrics::ClientSample> {
        let handles: Vec<(String, Arc<RwLock<Client>>)> = self
            .clients
            .read()
            .await
            .iter()
            .map(|(id, client)| (id.clone(), Arc::clone(client)))
            .collect();
        let mut samples = Vec::with_capacity(handles.len());
        for (client_id, client) in handles {
            let client = client.read().await;
            let balance = client.get_balance();
            samples.push(crate::metrics::ClientSample {
                client_id,
                confirmed: balance.confirmed,
                unconfirmed: balance.unconfirmed,
                chain_read_at: client
                    .chain_read_age()
                    .and_then(|age| Instant::now().checked_sub(age)),
            });
        }
        samples.sort_by(|a, b| a.client_id.cmp(&b.client_id));
        samples
    }

    /// Age every client's last chain read, and the interface's last answer,
    /// by `by` -- so a test can make the cache old without waiting.
    #[cfg(test)]
    pub async fn backdate_chain_reads(&self, by: Duration) {
        for client in self.client_handles().await {
            client.write().await.backdate_chain_read(by);
        }
        if let Some(at) = self.chain_health.lock().await.last_success.as_mut() {
            *at -= by;
        }
    }

    pub fn admin_auth_required(&self) -> bool {
        self.admin_api_key.is_some()
    }

    pub fn verify_admin_api_key(&self, provided: &str) -> bool {
        match self.admin_api_key.as_deref() {
            Some(expected) => crate::auth::constant_time_eq(provided, expected),
            None => true,
        }
    }

    /// Balance alone. `GET /balance` reports the fundable maximum beside it
    /// and uses [`Service::get_balance_and_max_fundable`]; this stays for the
    /// tests that only care about the balance.
    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn get_balance(&self, client_id: &str) -> Option<Balance> {
        Some(
            self.client_handle(client_id)
                .await?
                .read()
                .await
                .get_balance(),
        )
    }

    /// Balance together with the most a single `POST /fund` could ask for.
    ///
    /// Read under one lock so the two cannot disagree: a refresh between two
    /// reads would let the endpoint report a maximum that does not belong to
    /// the balance beside it.
    pub async fn get_balance_and_max_fundable(&self, client_id: &str) -> Option<(Balance, i64)> {
        let client = self.client_handle(client_id).await?;
        let client = client.read().await;
        Some((client.get_balance(), client.max_fundable_p2pkh()))
    }

    pub async fn get_address(&self, client_id: &str) -> Option<String> {
        Some(
            self.client_handle(client_id)
                .await?
                .read()
                .await
                .get_address(),
        )
    }

    #[cfg(test)]
    pub async fn set_test_chain_state(&self, client_id: &str, unspent: Utxo) -> Result<(), String> {
        let client = self
            .client_handle(client_id)
            .await
            .ok_or_else(|| format!("Unknown client_id {client_id}"))?;
        client.write().await.apply_chain_state(unspent);
        Ok(())
    }

    pub async fn funding_balance_error(&self, fund_request: &FundRequest) -> Option<CodedError> {
        let client = self.client_handle(&fund_request.client_id).await?;
        let guard = client.read().await;
        guard.funding_balance_error(fund_request)
    }

    /// Build and sign a funding transaction, and claim its inputs so that no
    /// concurrent request for the same client can spend them (CS-473).
    ///
    /// The balance check, the plan and the claim happen under one write lock,
    /// held only while they run -- selection, building and signing, no network
    /// -- and released before the broadcast. Requests for one client therefore
    /// still overlap on the part that takes time, but no two of them can pick
    /// the same input: planning used to take a shared lock, so every request
    /// in flight planned against the same cache and chose the same smallest
    /// suitable UTXO.
    ///
    /// The check is made inside the lock rather than before it, so a request
    /// cannot pass it, lose its UTXO to a concurrent claim, and then fail to
    /// plan. It gets the coded error the check gives -- the wallet's funds are
    /// all in flight -- rather than an internal one.
    pub async fn prepare_funding_outpoints(
        service: &Arc<Service>,
        fund_request: &FundRequest,
    ) -> Result<(Arc<dyn TxBroadcaster>, PreparedFunding), CodedError> {
        let client = service
            .client_handle(&fund_request.client_id)
            .await
            .ok_or_else(|| {
                CodedError::internal(format!("Unknown client_id {}", fund_request.client_id))
            })?;
        let (tx, spend_plan) = {
            let mut client = client.write().await;
            if let Some(error) = client.funding_balance_error(fund_request) {
                return Err(error);
            }
            let (tx, plan) = client
                .plan_funding_tx(fund_request)
                .map_err(CodedError::internal)?;
            client.claim_inputs(&plan);
            (tx, plan)
        };
        Ok((
            service.broadcaster(),
            PreparedFunding {
                client_id: fund_request.client_id.clone(),
                txs: vec![tx],
                no_of_outpoints: fund_request.no_of_outpoints,
                spend_plan,
            },
        ))
    }

    pub async fn commit_prepared_funding(
        service: &Arc<Service>,
        prepared: &PreparedFunding,
    ) -> Result<(), CommitError> {
        let client = service
            .client_handle(&prepared.client_id)
            .await
            .ok_or_else(|| CommitError::ClientGone(prepared.client_id.clone()))?;
        client
            .write()
            .await
            .commit_funding_spend(prepared.spend_plan.clone())
            .map_err(CommitError::Refused)?;
        service.save_inflight_state().await;
        Ok(())
    }

    /// Settle a funding transaction that has just been broadcast successfully.
    ///
    /// Only a refusal by the commit (CS-474: these outpoints were already
    /// handed to another caller) withholds the response. A client removed
    /// while its transaction was broadcasting does not (CS-501): the
    /// transaction is on the network paying the caller's script, so the
    /// caller is given its outpoints. Answering `internal` there told the
    /// caller nothing had been created when a coin had.
    async fn conclude_broadcast(
        service: &Arc<Service>,
        prepared: &PreparedFunding,
        response: FundingResponse,
    ) -> Result<FundingResponse, CodedError> {
        match Self::commit_prepared_funding(service, prepared).await {
            Ok(()) => Ok(response),
            Err(CommitError::ClientGone(client_id)) => {
                log::error!(
                    "client {client_id} was removed while its funding transaction was \
                     broadcasting; returning the outpoints it paid, which are on the network"
                );
                Ok(response)
            }
            Err(CommitError::Refused(description)) => {
                log::warn!("commit_prepared_funding failed: {}", description);
                let _ = Self::refresh_client_chain_state(service, &prepared.client_id).await;
                Err(CodedError::internal(description))
            }
        }
    }

    pub async fn execute_funding(
        service: &Arc<Service>,
        fund_request: &FundRequest,
    ) -> Result<FundingResponse, CodedError> {
        let (broadcaster, prepared) =
            Self::prepare_funding_outpoints(service, fund_request).await?;
        match Self::broadcast_prepared_funding(broadcaster, &prepared).await {
            Ok(response) => Self::conclude_broadcast(service, &prepared, response).await,
            Err(FundingBroadcastError {
                error,
                inputs_missing,
            }) => {
                if error.code == ErrorCode::BroadcastOutcomeUnknown {
                    Self::reserve_uncertain_funding(service, &prepared).await;
                } else {
                    Self::release_prepared_funding(service, &prepared, inputs_missing).await;
                }
                // Marked stale rather than refetched. The cache is already
                // right -- nothing was spent on a refusal, and an uncertain
                // outcome has just reserved its inputs -- so a request here
                // would buy nothing for this caller. What it would buy is a
                // second opinion for the *next* caller, if the refusal was a
                // conflict, and marking the state stale gets that without
                // paying for it when no next caller arrives.
                Self::invalidate_chain_state(service, &fund_request.client_id).await;
                Err(error)
            }
        }
    }

    /// Treat a prepared funding's inputs as spent although the broadcast
    /// outcome is unknown.
    ///
    /// Called instead of [`Self::commit_prepared_funding`] when the
    /// broadcaster could not say what became of the transaction. Failure to
    /// apply it is logged rather than returned: the caller is already being
    /// told its funding did not complete, and there is nothing it could do
    /// with a second error.
    /// Give back the inputs a prepared funding claimed, after a broadcast the
    /// upstream definitely did not take (CS-473). Nothing was spent, so they
    /// return to the cache at once rather than sitting reserved.
    ///
    /// Except, when the node said inputs are missing, the service's own
    /// pending change among them, which it will never list and so could
    /// never take back (CS-506): that is forgotten, with anything built on
    /// it, and the in-flight state saved without it.
    async fn release_prepared_funding(
        service: &Arc<Service>,
        prepared: &PreparedFunding,
        inputs_missing: bool,
    ) {
        if let Some(client) = service.client_handle(&prepared.client_id).await {
            let forgotten = {
                let mut client = client.write().await;
                client.release_claim(&prepared.spend_plan);
                if inputs_missing {
                    client.forget_refused_own_change(&prepared.spend_plan)
                } else {
                    0
                }
            };
            if forgotten > 0 {
                service.save_inflight_state().await;
            }
        }
    }

    async fn reserve_uncertain_funding(service: &Arc<Service>, prepared: &PreparedFunding) {
        match service.client_handle(&prepared.client_id).await {
            Some(client) => {
                client
                    .write()
                    .await
                    .commit_uncertain_funding_spend(prepared.spend_plan.clone());
                service.save_inflight_state().await;
            }
            None => log::warn!(
                "cannot reserve the inputs of an uncertain funding transaction: unknown client_id \
                 {}",
                prepared.client_id
            ),
        }
    }

    /// Fund a request with separate transactions, preparing and broadcasting one at a time.
    pub async fn fund_with_multiple_transactions(
        service: &Arc<Service>,
        fund_request: &FundRequest,
    ) -> Result<FundingResponse, MultipleTxFundError> {
        if let Some(error) = service.funding_balance_error(fund_request).await {
            return Err(MultipleTxFundError::complete(error.code, error.description));
        }

        let mut combined = FundingResponse::default();
        let total = fund_request.no_of_outpoints;

        for tx_index in 0..total {
            // Each transaction pays its own script, so build the per-tx
            // request inside the loop rather than reusing one.
            let per_tx_request = FundRequest {
                client_id: fund_request.client_id.clone(),
                satoshi: fund_request.satoshi,
                no_of_outpoints: 1,
                multiple_tx: false,
                locking_scripts: vec![fund_request
                    .locking_scripts
                    .get(tx_index as usize)
                    .or_else(|| fund_request.locking_scripts.last())
                    .cloned()
                    .unwrap_or_default()],
            };
            match Self::prepare_funding_outpoints(service, &per_tx_request).await {
                Ok((broadcaster, prepared)) => {
                    match Self::broadcast_prepared_funding(broadcaster, &prepared).await {
                        Ok(partial) => {
                            match Self::conclude_broadcast(service, &prepared, partial).await {
                                Ok(partial) => {
                                    combined.outpoints.extend(partial.outpoints);
                                    combined.txs.extend(partial.txs);
                                }
                                // Refused (CS-474). Those outpoints are another
                                // caller's, but the ones broadcast before them are
                                // this caller's and on the network: report them
                                // as a partial funding rather than dropping them
                                // in a bare `internal` (CS-501).
                                Err(cause) => {
                                    resync_after_multiple_tx_failure(service, fund_request).await;
                                    if tx_index == 0 {
                                        return Err(MultipleTxFundError::complete(
                                            cause.code,
                                            cause.description,
                                        ));
                                    }
                                    return Err(partial_broadcast_error(
                                        tx_index + 1,
                                        total,
                                        &combined,
                                        cause.description,
                                    ));
                                }
                            }
                        }
                        Err(FundingBroadcastError {
                            error: cause,
                            inputs_missing,
                        }) => {
                            if cause.code == ErrorCode::BroadcastOutcomeUnknown {
                                Self::reserve_uncertain_funding(service, &prepared).await;
                            } else {
                                Self::release_prepared_funding(service, &prepared, inputs_missing)
                                    .await;
                            }
                            if tx_index == 0 {
                                resync_after_multiple_tx_failure(service, fund_request).await;
                                return Err(MultipleTxFundError::complete(
                                    cause.code,
                                    cause.description,
                                ));
                            }
                            resync_after_multiple_tx_failure(service, fund_request).await;
                            return Err(partial_broadcast_error(
                                tx_index + 1,
                                total,
                                &combined,
                                cause,
                            ));
                        }
                    }
                }
                Err(cause) => {
                    if tx_index == 0 {
                        return Err(MultipleTxFundError::complete(cause.code, cause.description));
                    }
                    resync_after_multiple_tx_failure(service, fund_request).await;
                    return Err(partial_broadcast_error(
                        tx_index + 1,
                        total,
                        &combined,
                        cause.description,
                    ));
                }
            }
        }

        Ok(combined)
    }

    /// Broadcast prepared funding transactions without holding client locks.
    pub async fn broadcast_prepared_funding(
        broadcaster: Arc<dyn TxBroadcaster>,
        prepared: &PreparedFunding,
    ) -> Result<FundingResponse, FundingBroadcastError> {
        let tx = prepared
            .txs
            .first()
            .cloned()
            .ok_or_else(|| CodedError::internal("No funding transaction prepared."))?;
        let mut response = FundingResponse::default();
        response.txs.push(tx.clone());
        // One line before and one after every broadcast, each naming the
        // transaction, so a txid found anywhere -- a caller's report, a block
        // explorer -- leads to what happened to it (CS-482). `funding tx
        // <txid>` is common to every line about a transaction, so one grep
        // finds them all. The attempt line stays even though an outcome line
        // follows: for an outcome that is unknown it is the only record that
        // the transaction left.
        let txid = tx.hash().encode();
        log::info!(
            "broadcasting funding tx {txid} (client_id={}, outpoints={}, broadcaster={})",
            prepared.client_id,
            prepared.no_of_outpoints,
            broadcaster.name()
        );
        log::debug!(
            "funding tx {txid} hex={}",
            tx_as_hexstr(&tx).map_err(CodedError::internal)?
        );
        // The client sees a fixed message whatever the upstream said; the
        // detail, including the upstream's own reason and its view of whether
        // a resubmission could work, goes to the log through `BroadcastError`'s
        // Display.
        if let Err(e) = broadcaster.broadcast_tx(&tx).await {
            let error = coded_broadcast_error(&e);
            log::warn!(
                "broadcast failed: funding tx {txid} (class={}, broadcaster={}): {e}",
                broadcast_failure_class(error.code),
                broadcaster.name()
            );
            return Err(FundingBroadcastError {
                error,
                inputs_missing: e.inputs_missing(),
            });
        }
        log::info!(
            "broadcast accepted: funding tx {txid} (broadcaster={})",
            broadcaster.name()
        );
        let hash = tx.hash();
        // The funded outputs are the last `no_of_outpoints` of the
        // transaction. They used to be assumed to start at index 1, because a
        // change output always sat at index 0; since CS-452 a transaction
        // whose change would have been dust has no change output, and they
        // start at 0 instead. Derived rather than assumed, so it stays right
        // either way.
        let first = tx.outputs.len() as u32 - prepared.no_of_outpoints;
        response.outpoints = (first..first + prepared.no_of_outpoints)
            .map(|index| OutPoint { hash, index })
            .collect();
        Ok(response)
    }
}

async fn resync_after_multiple_tx_failure(service: &Arc<Service>, fund_request: &FundRequest) {
    if let Err(error) = Service::refresh_client_chain_state(service, &fund_request.client_id).await
    {
        log::warn!("failed to resync UTXO cache after partial multiple_tx funding: {error}");
    }
}

/// Why a successfully broadcast funding transaction could not be committed.
#[derive(Debug)]
pub enum CommitError {
    /// The client was removed while the transaction was broadcasting. The
    /// transaction is still the caller's and on the network.
    ClientGone(String),
    /// The client refused the commit (CS-474): these outpoints were already
    /// handed to another caller.
    Refused(String),
}

fn partial_broadcast_error(
    failed_at: u32,
    total: u32,
    broadcast: &FundingResponse,
    cause: impl std::fmt::Display,
) -> MultipleTxFundError {
    let succeeded = broadcast.outpoints.len() as u32;
    let mut message =
        format!("Failed to broadcast funding transaction {failed_at} of {total}: {cause}");
    if succeeded > 0 {
        let hashes: Vec<String> = broadcast
            .outpoints
            .iter()
            .map(|outpoint| outpoint.hash.encode())
            .collect();
        message.push_str(&format!(
            ". {succeeded} transaction(s) were broadcast successfully: {}",
            hashes.join(", ")
        ));
        return MultipleTxFundError::partial(message, broadcast.clone());
    }
    MultipleTxFundError::complete(ErrorCode::BroadcastFailed, message)
}

/// The client's unspent set, which the balance is then derived from.
///
/// One request, not two. A separate balance query can disagree with the
/// unspent set -- WhatsOnChain's is deprecated and under-reports unconfirmed
/// outputs -- and the unspent set is the one that decides what can be funded,
/// so it is the one that is asked for. Halving the requests per refresh also
/// halves what the rate limit has to cover.
async fn fetch_chain_state(
    blockchain: &dyn BlockchainInterface,
    address: &str,
) -> Result<Utxo, String> {
    blockchain
        .get_utxo(address)
        .await
        .map_err(|e| format!("get_utxo failed: {e}"))
}

/// A funding broadcast that did not succeed: the caller's answer, and the one
/// fact about the refusal the cache needs that the answer does not carry.
#[derive(Debug)]
pub struct FundingBroadcastError {
    pub error: CodedError,
    /// The node refused it because an input does not exist (CS-506).
    pub inputs_missing: bool,
}

impl From<CodedError> for FundingBroadcastError {
    fn from(error: CodedError) -> Self {
        Self {
            error,
            inputs_missing: false,
        }
    }
}

/// The answer a caller gets for a broadcast that did not succeed.
///
/// The code carries the distinctions the caller's next move depends on, and
/// the three are genuinely different moves: a broadcast that failed spent
/// nothing and may work on retry; one that was refused outright spent nothing
/// and will never work on retry; one whose outcome is unknown may have spent
/// everything.
fn coded_broadcast_error(error: &BroadcastError) -> CodedError {
    match error {
        BroadcastError::Indeterminate(_) => CodedError::new(
            ErrorCode::BroadcastOutcomeUnknown,
            "The funding transaction was sent and its outcome is unknown; it may be on the \
             network. Do not retry with a new idempotency_key, which would risk funding twice.",
        ),
        // Not sent, so nothing was spent, but a retry cannot help until the
        // key list or mapi-lite's key changes (CS-618).
        BroadcastError::Untrusted(_) => CodedError::new(
            ErrorCode::BroadcasterUntrusted,
            "Nothing was sent: the broadcaster signs with a key that [mapi_lite] \
             miner_id_public_keys does not list, so its answers cannot be believed. \
             Nothing was spent. An operator has to fix the key list or the broadcaster's key.",
        ),
        // The upstream looked at this transaction and will not take it however
        // many times it is offered.
        BroadcastError::Rejected {
            retryable: false, ..
        } => CodedError::new(
            ErrorCode::BroadcastRejected,
            "The upstream refused the funding transaction and will not accept it on retry. \
             See the service log for its reason.",
        ),
        // Unreachable, or refused in a way the upstream itself called worth
        // retrying. Either way nothing was spent: since CS-501 every failure
        // that may have delivered the transaction is an unknown outcome
        // instead, including an acceptance mapi-lite could not record.
        _ => CodedError::new(
            ErrorCode::BroadcastFailed,
            "Failed to broadcast funding transaction.",
        ),
    }
}

/// The class a failed broadcast is logged with (CS-482).
///
/// Read off the code the caller is answered with rather than off the
/// upstream's error, so the log and the response cannot disagree: a refusal
/// the upstream called retryable is answered `broadcast_failed`, and is logged
/// as failed, not rejected.
fn broadcast_failure_class(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::BroadcastRejected => "rejected",
        ErrorCode::BroadcastOutcomeUnknown => "indeterminate",
        ErrorCode::BroadcasterUntrusted => "untrusted",
        _ => "failed",
    }
}

/// Why mapi-lite cannot fund for this service, when its nodes are on a
/// network that is not `ours` (CS-624): every funding transaction would be
/// refused as spending outputs its nodes have never seen. `None` when they
/// agree, or mapi-lite has not said.
pub(crate) fn network_mismatch(published: Option<String>, ours: &str) -> Option<String> {
    let theirs = published?;
    (theirs != ours).then(|| {
        format!(
            "mapi-lite's nodes are on {theirs}, but blockchain_interface.network is {ours}: \
             every funding through it would be refused as missing inputs. Point one at the \
             other's network"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::address_watcher::RecordingWatcher;
    use crate::config::ClientConfig;
    use crate::test_support::{
        test_blockchain_interface, test_config, test_config_with_keys, unique_dynamic_config_path,
        LOCKING_SCRIPT_HEX, TEST_ADDRESS, TEST_CLIENT_ID, TEST_WIF,
    };

    async fn service_with_watcher(config: &Config, watcher: Arc<RecordingWatcher>) -> Arc<Service> {
        let blockchain = test_blockchain_interface(config).await;
        Arc::new(Service::new_for_test_with_watcher(config, blockchain, Some(watcher)).await)
    }

    /// CS-526: a dynamic config file that does not parse stops the service
    /// starting, rather than starting it without those clients -- and then
    /// overwriting their WIFs on the next `POST /client`.
    #[tokio::test]
    async fn sr_sec_023_an_unparseable_dynamic_config_refuses_startup() {
        use crate::broadcaster::woc::WocBroadcaster;
        let path = unique_dynamic_config_path();
        std::fs::write(&path, "[[clients]]\nclient_id = ").unwrap();
        let config = test_config(&path);
        let blockchain = test_blockchain_interface(&config).await;
        let broadcaster: Arc<dyn TxBroadcaster> = Arc::new(WocBroadcaster::new(
            Arc::clone(&blockchain),
            &config.blockchain_interface.interface_type,
        ));
        let result = Service::build(
            &config,
            Backend {
                interface: blockchain,
                address_watcher: None,
            },
            broadcaster,
        );
        let err = result.err().expect("startup must fail");
        assert!(err.contains("does not parse"), "{err}");
    }

    /// A node reports zero for an address it does not track, so the configured
    /// clients' addresses must be handed to it before the first balance read.
    #[tokio::test]
    async fn sr_bchn_008_configured_client_addresses_are_watched_at_startup() {
        let config = test_config(&unique_dynamic_config_path());
        let watcher = Arc::new(RecordingWatcher::new(false));
        let _service = service_with_watcher(&config, watcher.clone()).await;
        assert_eq!(watcher.watched(), vec![TEST_ADDRESS.to_string()]);
    }

    /// A client added through POST /client needs watching too, or its balance
    /// reads zero until the next restart.
    #[tokio::test]
    async fn sr_bchn_008_a_runtime_added_client_address_is_watched() {
        let config = test_config(&unique_dynamic_config_path());
        let watcher = Arc::new(RecordingWatcher::new(false));
        let service = service_with_watcher(&config, watcher.clone()).await;

        service
            .add_client(&ClientConfig {
                client_id: "id2".to_string(),
                wif_key: crate::test_support::OTHER_TEST_WIF.to_string(),
                api_key: None,
            })
            .await
            .expect("client should be added");

        // the startup import, then the new client's
        assert_eq!(watcher.watched().len(), 2);
        assert_eq!(
            watcher.watched()[1],
            crate::test_support::OTHER_TEST_ADDRESS
        );
    }

    /// An import failure is reported but must not stop the service: the node
    /// may refuse for a reason the operator already knows about.
    #[tokio::test]
    async fn sr_bchn_008_a_failed_import_does_not_prevent_startup() {
        let config = test_config(&unique_dynamic_config_path());
        let watcher = Arc::new(RecordingWatcher::new(true));
        let service = service_with_watcher(&config, watcher.clone()).await;
        assert_eq!(watcher.watched(), vec![TEST_ADDRESS.to_string()]);
        // the service is still usable
        assert!(service.is_client_id_valid(TEST_CLIENT_ID).await);
    }

    /// Backends that index the chain themselves must not be sent imports.
    #[tokio::test]
    async fn sr_bchn_007_no_watcher_means_no_import_attempts() {
        let config = test_config(&unique_dynamic_config_path());
        let blockchain = test_blockchain_interface(&config).await;
        // None, as woc/uaas/test supply
        let service = Service::new_for_test_with_watcher(&config, blockchain, None).await;
        assert!(service.is_client_id_valid(TEST_CLIENT_ID).await);
    }

    /// CS-501: a client removed while its funding transaction was broadcasting
    /// still gets its outpoints. The transaction is on the network paying the
    /// caller's script; answering `internal` told the caller nothing had been
    /// created when a coin had.
    #[tokio::test]
    async fn cs_501_a_client_removed_mid_broadcast_still_gets_its_outpoints() {
        let config = test_config(&unique_dynamic_config_path());
        let service = Arc::new(
            Service::new_for_test(&config, test_blockchain_interface(&config).await).await,
        );
        let request = sample_fund_request(TEST_CLIENT_ID);
        let (broadcaster, prepared) = Service::prepare_funding_outpoints(&service, &request)
            .await
            .expect("prepared");
        let response = Service::broadcast_prepared_funding(broadcaster, &prepared)
            .await
            .expect("broadcast");

        service
            .delete_client(TEST_CLIENT_ID)
            .await
            .expect("deleted while the broadcast was in flight");

        let settled = Service::conclude_broadcast(&service, &prepared, response)
            .await
            .expect("the outpoints are the caller's");
        assert_eq!(settled.outpoints.len(), 1);
    }

    /// CS-502. A chain read that is accepted and never answered no longer
    /// holds the client's refresh: it fails at the interface deadline, the
    /// refresh lock is released, and the next refresh goes through.
    #[tokio::test]
    async fn cs_502_a_hung_chain_read_releases_the_refresh_at_the_deadline() {
        use crate::interface_timeout::Bounded;
        use crate::test_support::GatedBlockchain;

        let config = test_config(&unique_dynamic_config_path());
        let gated = GatedBlockchain::new(&config).await;
        let bounded = Arc::new(Bounded::new(gated.clone(), Duration::from_millis(100)));
        let service = Arc::new(Service::new_for_test(&config, bounded).await);
        gated.close();

        let started = Instant::now();
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            Service::refresh_client_chain_state(&service, TEST_CLIENT_ID),
        )
        .await
        .expect("the refresh waited past the deadline")
        .expect_err("the read never answered");
        assert!(error.contains("did not answer"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(5));

        gated.open();
        tokio::time::timeout(
            Duration::from_secs(5),
            Service::refresh_client_chain_state(&service, TEST_CLIENT_ID),
        )
        .await
        .expect("the refresh lock was still held")
        .expect("refreshes once the interface answers");
    }

    fn sample_fund_request(client_id: &str) -> FundRequest {
        FundRequest {
            client_id: client_id.to_string(),
            satoshi: 123,
            no_of_outpoints: 1,
            multiple_tx: false,
            locking_scripts: vec![hex::decode(LOCKING_SCRIPT_HEX).unwrap()],
        }
    }

    #[tokio::test]
    async fn refresh_client_chain_state_restores_stale_balance_and_utxo_cache() {
        let config = test_config(&unique_dynamic_config_path());
        let blockchain = test_blockchain_interface(&config).await;
        let service = Arc::new(Service::new_for_test(&config, blockchain).await);

        service
            .client_handle(TEST_CLIENT_ID)
            .await
            .expect("test client")
            .write()
            .await
            .apply_chain_state(Vec::new());

        Service::refresh_client_chain_state(&service, TEST_CLIENT_ID)
            .await
            .expect("refresh should succeed");

        let balance = service
            .get_balance(TEST_CLIENT_ID)
            .await
            .expect("test client");
        assert!(balance.confirmed > 0);
        assert!(service
            .funding_balance_error(&sample_fund_request(TEST_CLIENT_ID))
            .await
            .is_none());
    }

    #[test]
    fn partial_broadcast_error_lists_successful_txids() {
        use chain_gang::{messages::OutPoint, util::Hash256};

        let mut broadcast = FundingResponse::default();
        broadcast.outpoints.push(OutPoint {
            hash: Hash256::decode(
                "f67272e5c1408ecbeb8da543437c125ee1a17110317d44d13eafe31b771b795e",
            )
            .expect("valid hash"),
            index: 1,
        });

        let error = partial_broadcast_error(2, 3, &broadcast, "network error".to_string());
        assert!(error.partial.is_some());
        assert!(error.description.contains("2 of 3"));
        assert!(error
            .description
            .contains("1 transaction(s) were broadcast successfully"));
        assert!(error.description.contains("f67272e5"));
        assert_eq!(error.partial.unwrap().outpoints.len(), 1);
    }

    #[test]
    fn partial_broadcast_error_without_successes_has_no_partial_payload() {
        let error = partial_broadcast_error(
            1,
            2,
            &FundingResponse::default(),
            "network error".to_string(),
        );
        assert!(error.partial.is_none());
        assert!(error.description.contains("network error"));
    }

    #[tokio::test]
    async fn fund_with_multiple_transactions_broadcasts_each_tx_separately() {
        let config = test_config(&unique_dynamic_config_path());
        let blockchain = test_blockchain_interface(&config).await;
        let service = Arc::new(Service::new_for_test(&config, blockchain).await);

        let fund_request = FundRequest {
            client_id: TEST_CLIENT_ID.to_string(),
            satoshi: 123,
            no_of_outpoints: 2,
            multiple_tx: true,
            locking_scripts: vec![hex::decode(LOCKING_SCRIPT_HEX).unwrap()],
        };

        let response = Service::fund_with_multiple_transactions(&service, &fund_request)
            .await
            .expect("multiple_tx funding should succeed");
        assert_eq!(response.txs.len(), 2);
        assert_eq!(response.outpoints.len(), 2);
        assert_ne!(response.outpoints[0].hash, response.outpoints[1].hash);
    }

    #[tokio::test]
    async fn concurrent_fund_requests_for_different_clients_do_not_block() {
        let path = unique_dynamic_config_path();
        let config = test_config_with_keys(&path, None, None);
        let blockchain = test_blockchain_interface(&config).await;
        let service = Arc::new(Service::new_for_test(&config, blockchain).await);
        service
            .add_client(&ClientConfig {
                client_id: "client2".to_string(),
                wif_key: crate::test_support::OTHER_TEST_WIF.to_string(),
                api_key: None,
            })
            .await
            .expect("add second client");

        let service_a = Arc::clone(&service);
        let service_b = Arc::clone(&service);
        let request_a = sample_fund_request(TEST_CLIENT_ID);
        let request_b = sample_fund_request("client2");

        let (result_a, result_b) = tokio::join!(
            fund_single_transaction(&service_a, &request_a),
            fund_single_transaction(&service_b, &request_b),
        );

        result_a.expect("client1 funding should succeed");
        result_b.expect("client2 funding should succeed");
    }

    #[tokio::test]
    async fn concurrent_fund_requests_for_same_client_do_not_block() {
        let config = test_config(&unique_dynamic_config_path());
        let blockchain = test_blockchain_interface(&config).await;
        let service = Arc::new(Service::new_for_test(&config, blockchain).await);

        let service_a = Arc::clone(&service);
        let service_b = Arc::clone(&service);
        let request_a = sample_fund_request(TEST_CLIENT_ID);
        let mut request_b = sample_fund_request(TEST_CLIENT_ID);
        request_b.satoshi = 456;

        let (result_a, result_b) = tokio::join!(
            fund_single_transaction(&service_a, &request_a),
            fund_single_transaction(&service_b, &request_b),
        );

        result_a.expect("first same-client funding should succeed");
        result_b.expect("second same-client funding should succeed");
    }

    #[tokio::test]
    async fn sr_fund_008_partial_multiple_tx_failure_resyncs_chain_state() {
        use crate::test_support::FailingBroadcastBlockchain;

        let config = test_config(&unique_dynamic_config_path());
        let blockchain = FailingBroadcastBlockchain::new(&config, 1).await;
        let service = Arc::new(Service::new_for_test(&config, blockchain).await);

        let fund_request = FundRequest {
            client_id: TEST_CLIENT_ID.to_string(),
            satoshi: 123,
            no_of_outpoints: 2,
            multiple_tx: true,
            locking_scripts: vec![hex::decode(LOCKING_SCRIPT_HEX).unwrap()],
        };

        let error = Service::fund_with_multiple_transactions(&service, &fund_request)
            .await
            .expect_err("second broadcast should fail");
        assert!(error.partial.is_some());

        let balance = service
            .get_balance(TEST_CLIENT_ID)
            .await
            .expect("test client");
        assert!(balance.confirmed > 0);
        assert!(service
            .funding_balance_error(&sample_fund_request(TEST_CLIENT_ID))
            .await
            .is_none());
    }

    /// Stands in for the `POST /fund` handler, which refreshes chain state
    /// only when the cache has gone stale before it builds anything.
    async fn fund_single_transaction(
        service: &Arc<Service>,
        fund_request: &FundRequest,
    ) -> Result<FundingResponse, CodedError> {
        // The fund handler's path, so tests exercise what a request does.
        Service::prepare_chain_state_for_funding(service, &fund_request.client_id)
            .await
            .map_err(CodedError::internal)?;
        Service::execute_funding(service, fund_request).await
    }

    /// CS-418 asked for a line saying the interface came back. The reporter's
    /// complaint was that warnings were "the last log lines", leaving no way
    /// to tell a recovered service from a stuck one -- so recovery is
    /// announced once, with the length of the run, and only when there was
    /// something to recover from.
    #[tokio::test]
    async fn cs_418_recovery_is_announced_once_after_a_run_of_failures() {
        let config = test_config(&unique_dynamic_config_path());
        let blockchain = test_blockchain_interface(&config).await;
        let service = Arc::new(Service::new_for_test(&config, blockchain).await);

        // a healthy service announces nothing
        assert_eq!(service.consecutive_chain_failures().await, 0);
        assert_eq!(service.record_chain_success().await, None);

        for _ in 0..3 {
            service.record_chain_failure("429 Too Many Requests").await;
        }
        assert_eq!(service.consecutive_chain_failures().await, 3);

        // recovery names the run, once
        assert_eq!(service.record_chain_success().await, Some(3));
        assert_eq!(service.consecutive_chain_failures().await, 0);
        assert_eq!(
            service.record_chain_success().await,
            None,
            "a second success must not announce recovery again"
        );
    }

    // ---- CS-431: fewer refreshes, not just slower ones ----

    use crate::test_support::CountingBlockchain;

    /// A service whose cached chain state stays fresh for `seconds`, reading
    /// through a counter so refreshes can be counted at the far end.
    async fn service_with_freshness(seconds: u64) -> (Arc<Service>, Arc<CountingBlockchain>) {
        let mut config = test_config(&unique_dynamic_config_path());
        config.service.utxo_refresh_period = seconds;
        let blockchain = CountingBlockchain::new(&config).await;
        let service = Arc::new(Service::new_for_test(&config, blockchain.clone()).await);
        (service, blockchain)
    }

    /// The saving the ticket is about. Repeated requests inside the window
    /// reuse what the last refresh fetched instead of each fetching the same
    /// answer, which is what made a burst queue behind the rate limiter.
    #[tokio::test]
    async fn cs_431_requests_inside_the_window_reuse_the_cached_state() {
        let (service, blockchain) = service_with_freshness(60).await;
        Service::refresh_client_chain_state(&service, TEST_CLIENT_ID)
            .await
            .expect("first refresh");
        let after_first = blockchain.utxo_read_count();

        for _ in 0..5 {
            Service::refresh_client_chain_state_if_stale(&service, TEST_CLIENT_ID)
                .await
                .expect("cached");
        }

        assert_eq!(
            blockchain.utxo_read_count(),
            after_first,
            "five requests inside the window should not have fetched again"
        );
    }

    /// The window is a window, not a switch: once it passes, the next request
    /// fetches. Zero means every request refreshes, which is what the service
    /// did before this change and what an operator gets by setting it.
    #[tokio::test]
    async fn cs_431_a_zero_window_refreshes_on_every_request() {
        let (service, blockchain) = service_with_freshness(0).await;
        let before = blockchain.utxo_read_count();
        for _ in 0..3 {
            Service::refresh_client_chain_state_if_stale(&service, TEST_CLIENT_ID)
                .await
                .expect("refresh");
        }
        assert_eq!(
            blockchain.utxo_read_count(),
            before + 3,
            "a zero window must not skip anything"
        );
    }

    /// The mitigation for a wide window. Building on a stale view risks
    /// selecting an input something else has spent, and the upstream refusing
    /// the result is the evidence. That refusal marks the cache stale, so the
    /// next attempt refreshes rather than repeating the mistake -- and it
    /// costs nothing when no next attempt comes.
    #[tokio::test]
    async fn cs_431_a_refused_broadcast_makes_the_next_request_refresh() {
        use crate::test_support::RejectingBroadcaster;

        let mut config = test_config(&unique_dynamic_config_path());
        config.service.utxo_refresh_period = 3600;
        let blockchain = CountingBlockchain::new(&config).await;
        let service = service_with(
            &config,
            blockchain.clone(),
            RejectingBroadcaster::new(false),
        )
        .await;

        Service::refresh_client_chain_state(&service, TEST_CLIENT_ID)
            .await
            .expect("first refresh");
        let before = blockchain.utxo_read_count();

        // inside a one-hour window, so nothing would refresh on age alone
        Service::refresh_client_chain_state_if_stale(&service, TEST_CLIENT_ID)
            .await
            .expect("cached");
        assert_eq!(blockchain.utxo_read_count(), before, "still fresh");

        let error = fund_single_transaction(&service, &sample_fund_request(TEST_CLIENT_ID))
            .await
            .expect_err("the upstream refuses it");
        assert_eq!(error.code, ErrorCode::BroadcastRejected);

        Service::refresh_client_chain_state_if_stale(&service, TEST_CLIENT_ID)
            .await
            .expect("refresh");
        assert_eq!(
            blockchain.utxo_read_count(),
            before + 1,
            "a refusal should have made the cache stale"
        );
    }

    /// A failed broadcast used to refresh immediately, which spent a request
    /// on an answer the caller could not use. Marking the state stale defers
    /// it to a request that wants it, and to none at all if none comes.
    #[tokio::test]
    async fn cs_431_a_failed_broadcast_does_not_refresh_on_the_spot() {
        use crate::test_support::RejectingBroadcaster;

        let mut config = test_config(&unique_dynamic_config_path());
        config.service.utxo_refresh_period = 3600;
        let blockchain = CountingBlockchain::new(&config).await;
        let service = service_with(
            &config,
            blockchain.clone(),
            RejectingBroadcaster::new(false),
        )
        .await;

        Service::refresh_client_chain_state(&service, TEST_CLIENT_ID)
            .await
            .expect("first refresh");
        let before = blockchain.utxo_read_count();

        let _ = fund_single_transaction(&service, &sample_fund_request(TEST_CLIENT_ID)).await;

        assert_eq!(
            blockchain.utxo_read_count(),
            before,
            "the error path should not have fetched"
        );
    }

    /// The periodic sweep is the other half. A client a request already
    /// refreshed inside the window does not need fetching again on the tick,
    /// so a busy service pays for its quiet clients only.
    #[tokio::test]
    async fn cs_431_the_periodic_sweep_skips_clients_a_request_refreshed() {
        let (service, blockchain) = service_with_freshness(3600).await;
        Service::refresh_client_chain_state(&service, TEST_CLIENT_ID)
            .await
            .expect("a request refreshes it");
        let before = blockchain.utxo_read_count();

        Service::refresh_balances(&service).await;

        assert_eq!(
            blockchain.utxo_read_count(),
            before,
            "the sweep refetched a client that was already current"
        );
    }

    // ---- SR-FUND-031/032, SR-BCHN-015: writes do not queue on the refresh ----

    use crate::test_support::{GatedBlockchain, StubMapiBroadcaster};

    /// Long enough for anything runnable to have run, on the current-thread
    /// runtime a `#[tokio::test]` uses. Only ever used to show something did
    /// NOT finish; nothing waits on it to succeed.
    const SETTLE: std::time::Duration = std::time::Duration::from_millis(50);

    /// A service with `[mapi_lite]` configured whose cache goes stale after
    /// `seconds`, reading through a gate the test holds.
    async fn mapi_lite_service(
        seconds: u64,
    ) -> (Arc<Service>, Arc<GatedBlockchain>, Arc<StubMapiBroadcaster>) {
        let mut config = test_config(&unique_dynamic_config_path());
        config.service.utxo_refresh_period = seconds;
        let blockchain = GatedBlockchain::new(&config).await;
        let broadcaster = StubMapiBroadcaster::new(true);
        let service = service_with(&config, blockchain.clone(), broadcaster.clone()).await;
        (service, blockchain, broadcaster)
    }

    /// Fund one transaction, failing the test rather than hanging it if the
    /// write is held -- which is what a regression here would do.
    async fn fund_without_waiting(service: &Arc<Service>) -> FundingResponse {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            fund_single_transaction(service, &sample_fund_request(TEST_CLIENT_ID)),
        )
        .await
        .expect("the write waited on the paced interface")
        .expect("funding should succeed")
    }

    fn fund_in_background(
        service: &Arc<Service>,
    ) -> tokio::task::JoinHandle<Result<FundingResponse, CodedError>> {
        let service = Arc::clone(service);
        tokio::spawn(async move {
            fund_single_transaction(&service, &sample_fund_request(TEST_CLIENT_ID)).await
        })
    }

    /// The point of the change. With mapi-lite taking the broadcast, a write
    /// whose cache has merely aged is funded from it at once; the refresh it
    /// prompts runs behind it, on an interface that is still paced.
    #[tokio::test]
    async fn sr_fund_031_a_mapi_lite_write_does_not_wait_for_a_stale_refresh() {
        // a zero window: the cache is stale for every request
        let (service, blockchain, broadcaster) = mapi_lite_service(0).await;
        let before = blockchain.utxo_read_count();
        blockchain.close();

        let response = fund_without_waiting(&service).await;
        assert_eq!(response.outpoints.len(), 1);
        assert_eq!(broadcaster.broadcast_count(), 1);

        tokio::time::sleep(SETTLE).await;
        assert_eq!(
            blockchain.utxo_read_count(),
            before + 1,
            "the stale cache should still have been refreshed, behind the write"
        );
        blockchain.open();
    }

    /// Without mapi-lite the broadcast goes through the paced interface, so
    /// the write waits on it regardless and the refresh in front of it stays
    /// where it was.
    #[tokio::test]
    async fn sr_fund_031_without_mapi_lite_a_stale_write_still_waits() {
        let mut config = test_config(&unique_dynamic_config_path());
        config.service.utxo_refresh_period = 0;
        let blockchain = GatedBlockchain::new(&config).await;
        let service = Arc::new(Service::new_for_test(&config, blockchain.clone()).await);
        blockchain.close();

        let write = fund_in_background(&service);
        tokio::time::sleep(SETTLE).await;
        assert!(
            !write.is_finished(),
            "the write should wait for the refresh"
        );

        blockchain.open();
        write.await.unwrap().expect("funding should succeed");
        assert_eq!(blockchain.broadcast_count(), 1);
    }

    /// A refused or uncertain broadcast is evidence the cache is wrong, so
    /// the next write waits for a new view even with mapi-lite: funding from
    /// the old one is the conflict the refresh is there to prevent.
    #[tokio::test]
    async fn sr_fund_031_an_invalidated_cache_makes_a_mapi_lite_write_wait() {
        let (service, blockchain, broadcaster) = mapi_lite_service(3600).await;
        Service::invalidate_chain_state(&service, TEST_CLIENT_ID).await;
        let before = blockchain.utxo_read_count();
        blockchain.close();

        let write = fund_in_background(&service);
        tokio::time::sleep(SETTLE).await;
        assert!(
            !write.is_finished(),
            "the write should wait for the refresh"
        );
        assert_eq!(broadcaster.broadcast_count(), 0);

        blockchain.open();
        write.await.unwrap().expect("funding should succeed");
        assert_eq!(blockchain.utxo_read_count(), before + 1);
        assert_eq!(broadcaster.broadcast_count(), 1);
    }

    /// Requests that all find the cache stale share one fetch. Before, each
    /// made its own, and on a paced interface every one past the first
    /// queued for an answer the first had already fetched.
    #[tokio::test]
    async fn sr_fund_032_concurrent_stale_requests_share_one_refresh() {
        let mut config = test_config(&unique_dynamic_config_path());
        config.service.utxo_refresh_period = 3600;
        let blockchain = GatedBlockchain::new(&config).await;
        let service = Arc::new(Service::new_for_test(&config, blockchain.clone()).await);
        Service::invalidate_chain_state(&service, TEST_CLIENT_ID).await;
        let before = blockchain.utxo_read_count();
        blockchain.close();

        let requests: Vec<_> = (0..5)
            .map(|_| {
                let service = Arc::clone(&service);
                tokio::spawn(async move {
                    Service::refresh_client_chain_state_if_stale(&service, TEST_CLIENT_ID).await
                })
            })
            .collect();
        tokio::time::sleep(SETTLE).await;
        assert_eq!(
            blockchain.utxo_read_count(),
            before + 1,
            "one fetch in flight"
        );

        blockchain.open();
        for request in requests {
            request.await.unwrap().expect("refreshed");
        }
        assert_eq!(
            blockchain.utxo_read_count(),
            before + 1,
            "the queued requests should have reused the first one's answer"
        );
    }

    /// The background refresh is single-flighted too: a burst of mapi-lite
    /// writes against a stale cache starts one, not one per write.
    #[tokio::test]
    async fn sr_fund_032_a_burst_of_mapi_lite_writes_starts_one_background_refresh() {
        let (service, blockchain, broadcaster) = mapi_lite_service(0).await;
        let before = blockchain.utxo_read_count();
        blockchain.close();

        for _ in 0..3 {
            fund_without_waiting(&service).await;
        }
        tokio::time::sleep(SETTLE).await;

        assert_eq!(broadcaster.broadcast_count(), 3);
        assert_eq!(blockchain.utxo_read_count(), before + 1);
        blockchain.open();
    }

    /// The periodic sweep leaves a client alone while a refresh of it is in
    /// flight, rather than queueing a second fetch behind it.
    #[tokio::test]
    async fn sr_fund_032_the_periodic_sweep_skips_a_client_being_refreshed() {
        let (service, blockchain, _broadcaster) = mapi_lite_service(0).await;
        let before = blockchain.utxo_read_count();
        blockchain.close();

        fund_without_waiting(&service).await;
        tokio::time::sleep(SETTLE).await;
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            Service::refresh_balances(&service),
        )
        .await
        .expect("the sweep waited on the refresh in flight");

        assert_eq!(blockchain.utxo_read_count(), before + 1);
        blockchain.open();
    }

    /// With mapi-lite, nothing a write waits on goes through the paced
    /// interface: not the broadcast (SR-BCHN-009) and, with a usable cache,
    /// not the refresh either. Held here at a closed gate, which is what a
    /// paced interface with a queue in front of it looks like to a caller.
    #[tokio::test]
    async fn sr_bchn_015_a_mapi_lite_write_waits_on_nothing_from_the_paced_interface() {
        let (service, blockchain, broadcaster) = mapi_lite_service(0).await;
        blockchain.close();

        for _ in 0..3 {
            fund_without_waiting(&service).await;
        }

        assert_eq!(broadcaster.broadcast_count(), 3);
        assert_eq!(blockchain.broadcast_count(), 0);
        blockchain.open();
    }

    /// A service reading through `blockchain` and writing through
    /// `broadcaster`, as production does with `[mapi_lite]` configured.
    async fn service_with(
        config: &Config,
        blockchain: Arc<dyn BlockchainInterface + Send + Sync>,
        broadcaster: Arc<dyn TxBroadcaster>,
    ) -> Arc<Service> {
        Arc::new(Service::new_for_test_with_broadcaster(config, blockchain, broadcaster).await)
    }

    /// With a broadcaster injected, the blockchain interface serves reads
    /// only: not one broadcast may reach it.
    #[tokio::test]
    async fn sr_bchn_009_execute_funding_uses_the_broadcaster_not_the_blockchain_interface() {
        use crate::test_support::{CountingBlockchain, CountingBroadcaster};

        let config = test_config(&unique_dynamic_config_path());
        let blockchain = CountingBlockchain::new(&config).await;
        let broadcaster = CountingBroadcaster::new();
        let service = service_with(&config, blockchain.clone(), broadcaster.clone()).await;

        let response = fund_single_transaction(&service, &sample_fund_request(TEST_CLIENT_ID))
            .await
            .expect("funding should succeed");

        assert_eq!(response.outpoints.len(), 1);
        assert_eq!(broadcaster.broadcast_count(), 1);
        assert_eq!(blockchain.broadcast_count(), 0);
    }

    /// The defect in #66, end to end. A funding transaction is handed to the
    /// broadcaster, the outcome is unknown, and the next funding request must
    /// not spend the same input -- which would put a conflicting transaction
    /// on the network for the first one's inputs.
    ///
    /// Both requests are identical, so if the second one selected the same
    /// UTXO it would build a byte-identical transaction and hand over the same
    /// txid. Different txids mean different inputs.
    #[tokio::test]
    async fn sr_fund_012_an_uncertain_broadcast_does_not_leave_its_input_spendable() {
        use crate::test_support::{CountingBlockchain, UncertainBroadcaster};

        let config = test_config(&unique_dynamic_config_path());
        let blockchain = CountingBlockchain::new(&config).await;
        let broadcaster = UncertainBroadcaster::new();
        let service = service_with(&config, blockchain.clone(), broadcaster.clone()).await;

        let request = sample_fund_request(TEST_CLIENT_ID);
        let first = fund_single_transaction(&service, &request)
            .await
            .expect_err("an unknown outcome is not a success");
        assert_eq!(first.code, ErrorCode::BroadcastOutcomeUnknown);

        let second = fund_single_transaction(&service, &request)
            .await
            .expect_err("the second broadcast is equally uncertain");
        assert_eq!(second.code, ErrorCode::BroadcastOutcomeUnknown);

        let handed = broadcaster.handed();
        assert_eq!(handed.len(), 2, "both attempts reached the broadcaster");
        assert_ne!(
            handed[0], handed[1],
            "the second funding transaction spends the same input as the first, which the \
             network can only treat as a conflict"
        );
    }

    /// CS-618: a funding refused because the broadcaster is untrusted sent
    /// nothing, so it is answered 503 `broadcaster_untrusted` and its inputs
    /// stay available: the next funding, once the broadcaster is trusted
    /// again, spends the same input.
    #[tokio::test]
    async fn cs_618_an_untrusted_broadcaster_is_503_and_reserves_nothing() {
        use crate::test_support::{blockchain_listing, ScriptedBroadcaster};

        let config = test_config(&unique_dynamic_config_path());
        let utxo = chain_gang::interface::UtxoEntry {
            height: 1,
            tx_pos: 0,
            tx_hash: "a".repeat(64),
            value: 10_000,
        };
        let blockchain = blockchain_listing(&config, &[utxo]).await;
        let broadcaster = ScriptedBroadcaster::new(vec![
            Some(BroadcastError::Untrusted(
                "mapi-lite signs with 02ab…, which [mapi_lite] miner_id_public_keys does not list"
                    .to_string(),
            )),
            None,
        ]);
        let service = service_with(&config, blockchain, broadcaster.clone()).await;
        let request = FundRequest {
            satoshi: 1_000,
            ..sample_fund_request(TEST_CLIENT_ID)
        };

        let refused = fund_single_transaction(&service, &request)
            .await
            .expect_err("refused");
        assert_eq!(refused.code, ErrorCode::BroadcasterUntrusted);
        assert_eq!(
            refused.code.status(),
            actix_web::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert!(
            refused.description.contains("miner_id_public_keys"),
            "{}",
            refused.description
        );

        fund_single_transaction(&service, &request)
            .await
            .expect("the input was not held");
        let sent = broadcaster.sent();
        assert_eq!(
            sent[0].inputs[0].prev_output.hash, sent[1].inputs[0].prev_output.hash,
            "the second funding spends the input the first did not"
        );
    }

    /// A broadcast that is *known* to have failed spent nothing, so its input
    /// must stay available -- reserving there would strand funds for no
    /// reason. The distinction is the whole point of the new variant.
    /// CS-506, SR-FUND-036 acceptance: the node refuses a spend of the
    /// service's own change as missing inputs. The next request selects a
    /// different input, and the refresh between them does not restore the
    /// change. Before, it did, and the same spend was refused on every request
    /// until the change expired -- 95 times in a row on the regtest bench.
    #[tokio::test]
    async fn sr_fund_036_own_change_refused_as_missing_inputs_is_not_offered_again() {
        use crate::test_support::{blockchain_listing, ScriptedBroadcaster};

        let config = test_config(&unique_dynamic_config_path());
        let utxo = |seed: char, value| chain_gang::interface::UtxoEntry {
            height: 1517000,
            tx_pos: 0,
            tx_hash: seed.to_string().repeat(64),
            value,
        };
        let blockchain = blockchain_listing(&config, &[utxo('a', 10_000), utxo('b', 50_000)]).await;
        // Retryable, as mapi-lite reports it -- which is why the bench saw a
        // 502 `broadcast_failed` on every request rather than a 409.
        let broadcaster = ScriptedBroadcaster::new(vec![
            None,
            Some(BroadcastError::Rejected {
                description: "Mempool error, retry again later. (details: 16 missing-inputs)"
                    .to_string(),
                retryable: true,
            }),
        ]);
        let service = service_with(&config, blockchain, broadcaster.clone()).await;
        let request = FundRequest {
            satoshi: 1_000,
            ..sample_fund_request(TEST_CLIENT_ID)
        };

        fund_single_transaction(&service, &request)
            .await
            .expect("the first funding is accepted");
        let refused = fund_single_transaction(&service, &request)
            .await
            .expect_err("the second is refused");
        assert_eq!(refused.code, ErrorCode::BroadcastFailed);
        fund_single_transaction(&service, &request)
            .await
            .expect("the third funds from another input");

        let sent = broadcaster.sent();
        let first = sent[0].hash().encode();
        let spends_first = |tx: &Tx| {
            tx.inputs
                .iter()
                .any(|input| input.prev_output.hash.encode() == first)
        };
        assert!(
            spends_first(&sent[1]),
            "the refused one spent the first's change"
        );
        assert!(
            !spends_first(&sent[2]),
            "the next request did not select the refused change again"
        );
    }

    /// The forgotten change is forgotten on disk too, or a restart would bring
    /// it back (CS-465 persists pending change).
    #[tokio::test]
    async fn sr_fund_036_forgotten_change_is_not_persisted() {
        use crate::test_support::{blockchain_listing, ScriptedBroadcaster};

        let config = test_config(&unique_dynamic_config_path());
        let utxo = |seed: char, value| chain_gang::interface::UtxoEntry {
            height: 1517000,
            tx_pos: 0,
            tx_hash: seed.to_string().repeat(64),
            value,
        };
        let blockchain = blockchain_listing(&config, &[utxo('c', 10_000), utxo('d', 50_000)]).await;
        let broadcaster = ScriptedBroadcaster::new(vec![
            None,
            Some(BroadcastError::Rejected {
                description: "16 missing-inputs".to_string(),
                retryable: false,
            }),
        ]);
        let service = service_with(&config, blockchain, broadcaster.clone()).await;
        let request = FundRequest {
            satoshi: 1_000,
            ..sample_fund_request(TEST_CLIENT_ID)
        };
        fund_single_transaction(&service, &request).await.unwrap();
        let first = broadcaster.sent()[0].hash().encode();
        let persisted_change = || {
            load_inflight_file(&config.inflight_state_path())
                .get(TEST_CLIENT_ID)
                .map(|state| {
                    state
                        .pending_change
                        .iter()
                        .any(|change| change.tx_hash == first)
                })
                .unwrap_or(false)
        };
        assert!(
            persisted_change(),
            "the first funding's change is saved as pending"
        );

        fund_single_transaction(&service, &request)
            .await
            .expect_err("refused as missing inputs");
        assert!(!persisted_change(), "and no longer saved once forgotten");
    }

    #[tokio::test]
    async fn sr_fund_012_a_failed_broadcast_leaves_its_input_spendable() {
        use crate::test_support::{CountingBlockchain, FailingBroadcaster};

        let config = test_config(&unique_dynamic_config_path());
        let blockchain = CountingBlockchain::new(&config).await;
        // fails from the first call
        let broadcaster = FailingBroadcaster::new(0);
        let service = service_with(&config, blockchain.clone(), broadcaster.clone()).await;

        let request = sample_fund_request(TEST_CLIENT_ID);
        let error = fund_single_transaction(&service, &request)
            .await
            .expect_err("the broadcast failed");
        assert_eq!(error.code, ErrorCode::BroadcastFailed);

        let client = service.client_handle(TEST_CLIENT_ID).await.unwrap();
        assert_eq!(
            client.read().await.reserved_outpoint_count(),
            0,
            "nothing reached the network, so nothing should be reserved"
        );
    }

    /// The reservation has to survive the refresh that runs on the same error
    /// path, or it would be undone the moment it was made.
    #[tokio::test]
    async fn sr_fund_012_the_reservation_survives_the_refresh_on_the_error_path() {
        use crate::test_support::{CountingBlockchain, UncertainBroadcaster};

        let config = test_config(&unique_dynamic_config_path());
        let blockchain = CountingBlockchain::new(&config).await;
        let broadcaster = UncertainBroadcaster::new();
        let service = service_with(&config, blockchain.clone(), broadcaster.clone()).await;

        let _ = fund_single_transaction(&service, &sample_fund_request(TEST_CLIENT_ID)).await;

        let client = service.client_handle(TEST_CLIENT_ID).await.unwrap();
        assert_eq!(client.read().await.reserved_outpoint_count(), 1);

        // and an explicit refresh, as the periodic one does, does not free it
        Service::refresh_client_chain_state(&service, TEST_CLIENT_ID)
            .await
            .expect("refresh succeeds");
        assert_eq!(client.read().await.reserved_outpoint_count(), 1);
    }

    #[tokio::test]
    async fn sr_bchn_009_fund_with_multiple_transactions_uses_the_broadcaster() {
        use crate::test_support::{CountingBlockchain, CountingBroadcaster};

        let config = test_config(&unique_dynamic_config_path());
        let blockchain = CountingBlockchain::new(&config).await;
        let broadcaster = CountingBroadcaster::new();
        let service = service_with(&config, blockchain.clone(), broadcaster.clone()).await;

        let fund_request = FundRequest {
            client_id: TEST_CLIENT_ID.to_string(),
            satoshi: 123,
            no_of_outpoints: 3,
            multiple_tx: true,
            locking_scripts: vec![hex::decode(LOCKING_SCRIPT_HEX).unwrap()],
        };
        let response = Service::fund_with_multiple_transactions(&service, &fund_request)
            .await
            .expect("multiple_tx funding should succeed");

        assert_eq!(response.txs.len(), 3);
        assert_eq!(broadcaster.broadcast_count(), 3);
        assert_eq!(blockchain.broadcast_count(), 0);
    }

    /// A broadcaster failure is reported with the same code the blockchain
    /// interface's failure always was, and leaves the UTXO cache resynced, so
    /// the client contract does not depend on which broadcaster is in use.
    #[tokio::test]
    async fn broadcaster_failure_is_broadcast_failed_and_resyncs_chain_state() {
        use crate::test_support::FailingBroadcaster;

        let config = test_config(&unique_dynamic_config_path());
        let blockchain = test_blockchain_interface(&config).await;
        let service = service_with(&config, blockchain, FailingBroadcaster::new(0)).await;

        let error = fund_single_transaction(&service, &sample_fund_request(TEST_CLIENT_ID))
            .await
            .expect_err("the broadcaster fails");
        assert_eq!(error.code, ErrorCode::BroadcastFailed);

        assert!(service
            .funding_balance_error(&sample_fund_request(TEST_CLIENT_ID))
            .await
            .is_none());
    }

    // --- chain readiness (CS-505, SR-BCHN-017/018) ---

    /// The default bound with the test config's zero refresh period: the 60s
    /// floor.
    const READY_LIMIT: Duration = Duration::from_secs(60);

    async fn ready_test_service(config: &Config) -> Service {
        let blockchain = test_blockchain_interface(config).await;
        Service::new_for_test(config, blockchain).await
    }

    #[tokio::test]
    async fn sr_bchn_017_chain_readiness_passes_after_a_fresh_read() {
        let config = test_config(&unique_dynamic_config_path());
        let service = ready_test_service(&config).await;
        assert_eq!(service.chain_readiness().await, Ok(()));
    }

    #[tokio::test]
    async fn sr_bchn_017_a_client_read_older_than_the_bound_fails_readiness() {
        let config = test_config(&unique_dynamic_config_path());
        let service = ready_test_service(&config).await;
        service
            .backdate_chain_reads(READY_LIMIT + Duration::from_secs(1))
            .await;

        let detail = service.chain_readiness().await.expect_err("too old");
        assert!(detail.contains("last read 61s ago (limit 60s)"), "{detail}");
        assert!(
            detail.contains(TEST_CLIENT_ID),
            "names the client: {detail}"
        );
    }

    /// Marking the cache stale says "refresh before trusting it", not
    /// "nothing was read": a refused broadcast must not take the service out of
    /// rotation until the next refresh.
    #[tokio::test]
    async fn sr_bchn_017_invalidating_the_cache_does_not_fail_readiness() {
        let config = test_config(&unique_dynamic_config_path());
        let service = Arc::new(ready_test_service(&config).await);
        Service::invalidate_chain_state(&service, TEST_CLIENT_ID).await;
        assert_eq!(service.chain_readiness().await, Ok(()));
    }

    #[tokio::test]
    async fn sr_bchn_017_a_zero_bound_turns_the_check_off() {
        let mut config = test_config(&unique_dynamic_config_path());
        config.service.ready_max_chain_read_age_seconds = Some(0);
        let service = ready_test_service(&config).await;
        service
            .backdate_chain_reads(Duration::from_secs(3600))
            .await;
        assert_eq!(service.chain_readiness().await, Ok(()));
    }

    /// With no clients there is no cache to age, so the interface's last
    /// answer -- from the sweep's header read -- is what is held to the bound.
    #[tokio::test]
    async fn sr_bchn_017_without_clients_the_interfaces_last_answer_is_measured() {
        let mut config = test_config(&unique_dynamic_config_path());
        config.client = None;
        let service = ready_test_service(&config).await;
        // The test interface has no header read, so stand in for the sweep's.
        let detail = service.chain_readiness().await.expect_err("nothing yet");
        assert!(detail.contains("has not answered yet"), "{detail}");
        service.record_chain_success().await;
        assert_eq!(service.chain_readiness().await, Ok(()));

        service
            .backdate_chain_reads(READY_LIMIT + Duration::from_secs(1))
            .await;
        let detail = service.chain_readiness().await.expect_err("too old");
        assert!(detail.contains("last answered"), "{detail}");
    }

    /// SR-BCHN-018: an unreachable blockchain interface at startup is warned
    /// about, not fatal, and readiness says the service is not ready yet.
    #[tokio::test]
    async fn sr_bchn_018_startup_survives_an_unreachable_blockchain_interface() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let mut config = test_config(&unique_dynamic_config_path());
        config.blockchain_interface.interface_type = "rpc".to_string();
        config.blockchain_interface.network = "regtest".to_string();
        config.blockchain_interface.url = Some(format!("http://127.0.0.1:{port}"));
        config.blockchain_interface.rpc_user = Some("user".to_string());
        config.blockchain_interface.rpc_password = Some("pass".to_string());

        let service = Service::new(&config)
            .await
            .expect("an unreachable chain no longer stops startup");
        let detail = service.chain_readiness().await.expect_err("not read yet");
        assert!(detail.contains("never been read"), "{detail}");
    }

    /// CS-624: mapi-lite's published network is checked against this
    /// service's, and a mismatch makes mapi-lite not ready, naming both.
    #[tokio::test]
    async fn cs_624_a_mapi_lite_on_another_network_is_not_ready() {
        use crate::test_support::StubMapiBroadcaster;

        let config = test_config(&unique_dynamic_config_path());
        assert_eq!(config.blockchain_interface.network, "testnet");
        let blockchain = test_blockchain_interface(&config).await;
        let elsewhere = service_with(
            &config,
            Arc::clone(&blockchain),
            StubMapiBroadcaster::on_network("mainnet"),
        )
        .await;
        let detail = elsewhere
            .mapi_lite_health()
            .await
            .expect("mapi-lite is configured")
            .expect_err("on another network");
        assert!(
            detail.contains("on mainnet") && detail.contains("network is testnet"),
            "{detail}"
        );

        let same = service_with(
            &config,
            blockchain,
            StubMapiBroadcaster::on_network("testnet"),
        )
        .await;
        assert_eq!(same.mapi_lite_health().await, Some(Ok(())));
    }

    /// CS-624: a mapi-lite whose `/ready` names another network stops
    /// startup, where one that cannot be reached does not (SR-BCHN-018).
    #[tokio::test]
    async fn cs_624_a_mapi_lite_on_another_network_stops_startup() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        for (chain, starts) in [("mainnet", false), ("testnet", true)] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/ready"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "ready": true,
                    "chain": chain,
                    "checks": {}
                })))
                .mount(&server)
                .await;
            let mut config = test_config(&unique_dynamic_config_path());
            config.mapi_lite = Some(crate::config::MapiLiteConfig::for_base_url(server.uri()));
            match Service::new(&config).await {
                Ok(_) => assert!(starts, "started against a mapi-lite on {chain}"),
                Err(e) => {
                    assert!(!starts, "{chain}: {e}");
                    assert!(e.contains("on mainnet") && e.contains("testnet"), "{e}");
                }
            }
        }
    }

    /// CS-632: a mapi-lite that refuses the token stops startup, naming the
    /// setting and never the token; one that accepts it starts.
    #[tokio::test]
    async fn cs_632_a_refused_mapi_lite_token_stops_startup() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        for (quote_status, starts) in [(401, false), (404, true)] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/ready"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "ready": true,
                    "chain": "testnet",
                    "checks": {}
                })))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/mapi/feeQuote"))
                .respond_with(ResponseTemplate::new(quote_status))
                .mount(&server)
                .await;
            let mut config = test_config(&unique_dynamic_config_path());
            let mut mapi_lite = crate::config::MapiLiteConfig::for_base_url(server.uri());
            mapi_lite.auth_token = Some("hunter2".to_string());
            config.mapi_lite = Some(mapi_lite);
            match Service::new(&config).await {
                Ok(_) => assert!(starts, "started though mapi-lite answered {quote_status}"),
                Err(e) => {
                    assert!(!starts, "{quote_status}: {e}");
                    assert!(
                        e.contains("mapi_lite.auth_token") && e.contains("401"),
                        "{e}"
                    );
                    assert!(!e.contains("hunter2"), "{e}");
                }
            }
        }
    }

    #[test]
    fn cs_624_network_mismatch_says_nothing_until_mapi_lite_has_said() {
        assert_eq!(network_mismatch(None, "testnet"), None);
        assert_eq!(network_mismatch(Some("testnet".into()), "testnet"), None);
        assert!(network_mismatch(Some("regtest".into()), "testnet").is_some());
    }

    #[tokio::test]
    async fn sr_bchn_011_mapi_lite_health_is_none_without_mapi_lite() {
        let config = test_config(&unique_dynamic_config_path());
        let blockchain = test_blockchain_interface(&config).await;
        let service = Service::new_for_test(&config, blockchain).await;

        assert!(!service.mapi_lite_configured());
        assert!(service.mapi_lite_health().await.is_none());
        assert_eq!(service.get_status().await.broadcaster, "test");
    }

    /// The quote wins over the configured rate, and reaches the clients that
    /// actually cost transactions -- not just the service's own copy.
    #[tokio::test]
    async fn cs_451_the_mapi_fee_quote_sets_the_rate() {
        use crate::test_support::StubMapiBroadcaster;

        let config = test_config(&unique_dynamic_config_path());
        let blockchain = test_blockchain_interface(&config).await;
        let service =
            service_with(&config, blockchain, StubMapiBroadcaster::quoting(Some(250))).await;

        assert_eq!(
            service.fee_satoshis_per_kb().await,
            config.fees.satoshis_per_kb,
            "starts at the configured rate"
        );

        service.refresh_fee_rate().await;

        assert_eq!(service.fee_satoshis_per_kb().await, 250);
        let client = service.client_handle(TEST_CLIENT_ID).await.expect("client");
        assert_eq!(
            client.read().await.fee_satoshis_per_kb(),
            250,
            "the rate has to reach the client, which is what costs a transaction"
        );
    }

    /// A quote that cannot be had is not a reason to stop funding, nor to fall
    /// to some other number: the rate in force stays in force.
    #[tokio::test]
    async fn cs_451_a_failed_quote_leaves_the_rate_alone() {
        use crate::test_support::StubMapiBroadcaster;

        let config = test_config(&unique_dynamic_config_path());
        let blockchain = test_blockchain_interface(&config).await;
        let broadcaster = StubMapiBroadcaster::quoting(None);
        let handed: Arc<dyn TxBroadcaster> = broadcaster.clone();
        let service = service_with(&config, blockchain, handed).await;

        service.refresh_fee_rate().await;

        assert_eq!(broadcaster.quote_count(), 1, "it did ask");
        assert_eq!(
            service.fee_satoshis_per_kb().await,
            config.fees.satoshis_per_kb,
            "and kept the configured rate when there was no answer"
        );
    }

    /// A client added after a quote has moved the rate must cost its
    /// transactions the same as one that was here all along.
    #[tokio::test]
    async fn cs_451_a_client_added_later_gets_the_rate_in_force() {
        use crate::test_support::StubMapiBroadcaster;

        let config = test_config(&unique_dynamic_config_path());
        let blockchain = test_blockchain_interface(&config).await;
        let service =
            service_with(&config, blockchain, StubMapiBroadcaster::quoting(Some(250))).await;
        service.refresh_fee_rate().await;

        service
            .add_client(&ClientConfig {
                client_id: "added-later".to_string(),
                wif_key: crate::test_support::OTHER_TEST_WIF.to_string(),
                api_key: None,
            })
            .await
            .expect("added");

        let client = service.client_handle("added-later").await.expect("client");
        assert_eq!(
            client.read().await.fee_satoshis_per_kb(),
            250,
            "not the config file's rate, which the quote has superseded"
        );
    }

    #[tokio::test]
    async fn sr_bchn_011_mapi_lite_health_reports_the_probe_result() {
        use crate::test_support::StubMapiBroadcaster;

        let config = test_config(&unique_dynamic_config_path());

        let blockchain = test_blockchain_interface(&config).await;
        let healthy = service_with(&config, blockchain, StubMapiBroadcaster::new(true)).await;
        assert!(healthy.mapi_lite_configured());
        assert_eq!(healthy.mapi_lite_health().await, Some(Ok(())));
        assert_eq!(healthy.get_status().await.broadcaster, MAPI_LITE);

        let blockchain = test_blockchain_interface(&config).await;
        let unhealthy = service_with(&config, blockchain, StubMapiBroadcaster::new(false)).await;
        let probe = unhealthy.mapi_lite_health().await;
        assert!(
            matches!(probe, Some(Err(ref detail)) if detail.contains("503")),
            "{probe:?}"
        );
    }

    /// CS-619: mapi-lite's nodes not knowing the wallet's oldest confirmed
    /// transaction is a different chain, and mapi-lite is not ready; knowing
    /// it, or giving no answer, changes nothing.
    #[tokio::test]
    async fn cs_619_mapi_lite_on_another_chain_is_not_ready() {
        use crate::test_support::StubMapiBroadcaster;

        let config = test_config(&unique_dynamic_config_path());
        for (knows, ready) in [(Some(false), false), (Some(true), true), (None, true)] {
            let blockchain = test_blockchain_interface(&config).await;
            let service =
                service_with(&config, blockchain, StubMapiBroadcaster::knowing(knows)).await;
            match service.mapi_lite_health().await {
                Some(Ok(())) => assert!(ready, "{knows:?} answered ready"),
                Some(Err(detail)) => {
                    assert!(!ready, "{knows:?}: {detail}");
                    assert!(detail.contains("different chain"), "{detail}");
                    assert!(detail.contains("network"), "{detail}");
                }
                None => panic!("mapi-lite is configured"),
            }
        }
    }

    /// CS-619: the chain check runs alongside mapi-lite's `/ready`, not after
    /// it, so the probe still fits inside one probe timeout: with both
    /// answers taking 1.5s of a 2s timeout it takes about 1.5s, not 3.
    #[tokio::test]
    async fn cs_619_the_chain_check_keeps_the_probe_within_one_timeout() {
        use crate::broadcaster::mapi::MapiBroadcaster;
        use crate::config::MapiLiteConfig;
        use wiremock::matchers::{method, path_regex};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let slow = Duration::from_millis(1_500);
        Mock::given(method("GET"))
            .and(path_regex("^/ready$"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"ready": true}))
                    .set_delay(slow),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path_regex("^/query/tx/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(
                        serde_json::json!({"txid": "x", "known": true, "confirmed": true}),
                    )
                    .set_delay(slow),
            )
            .mount(&server)
            .await;

        let config = test_config(&unique_dynamic_config_path());
        let mapi = MapiLiteConfig::for_base_url(server.uri());
        assert_eq!(
            crate::config::MAPI_LITE_PROBE_TIMEOUT,
            Duration::from_secs(2)
        );
        let broadcaster: Arc<dyn TxBroadcaster> = Arc::new(MapiBroadcaster::new(&mapi).unwrap());
        let blockchain = test_blockchain_interface(&config).await;
        let service = service_with(&config, blockchain, broadcaster).await;

        let started = Instant::now();
        assert_eq!(service.mapi_lite_health().await, Some(Ok(())));
        let took = started.elapsed();
        assert!(
            took < Duration::from_millis(2_500),
            "the probe took {took:?}"
        );
    }

    /// CS-619: a wallet holding nothing confirmed offers no evidence, so the
    /// check passes whatever mapi-lite's nodes would say.
    #[tokio::test]
    async fn cs_619_an_empty_wallet_is_no_evidence_of_a_mismatch() {
        use crate::test_support::{blockchain_listing, StubMapiBroadcaster};

        let config = test_config(&unique_dynamic_config_path());
        let unconfirmed = chain_gang::interface::UtxoEntry {
            height: -1,
            tx_pos: 0,
            tx_hash: "d".repeat(64),
            value: 5_000,
        };
        let blockchain = blockchain_listing(&config, &[unconfirmed]).await;
        let service = service_with(
            &config,
            blockchain,
            StubMapiBroadcaster::knowing(Some(false)),
        )
        .await;
        assert_eq!(service.mapi_lite_health().await, Some(Ok(())));
    }

    /// `/health` is unauthenticated and rate-limit exempt, so its probe must
    /// not reach mapi-lite once per request. Within the TTL the verdict is
    /// reused; past it a fresh probe is taken.
    #[tokio::test]
    async fn sr_bchn_011_mapi_lite_health_is_cached_for_the_probe_timeout() {
        use crate::test_support::StubMapiBroadcaster;

        let mut config = test_config(&unique_dynamic_config_path());
        config.mapi_lite = Some(crate::config::MapiLiteConfig::for_base_url(
            "http://127.0.0.1:8080",
        ));

        let blockchain = test_blockchain_interface(&config).await;
        let broadcaster = StubMapiBroadcaster::new(true);
        let service = service_with(&config, blockchain, broadcaster.clone()).await;

        for _ in 0..5 {
            assert_eq!(service.mapi_lite_health().await, Some(Ok(())));
        }
        assert_eq!(broadcaster.probe_count(), 1, "the verdict should be reused");

        tokio::time::sleep(crate::config::MAPI_LITE_PROBE_TIMEOUT + Duration::from_millis(100))
            .await;
        assert_eq!(service.mapi_lite_health().await, Some(Ok(())));
        assert_eq!(
            broadcaster.probe_count(),
            2,
            "a stale verdict should be refreshed"
        );
    }

    // ---- CS-465: what was in flight survives a restart ----

    /// The input a funding transaction spent.
    fn spent_input(response: &FundingResponse) -> (String, u32) {
        let input = &response.txs[0].inputs[0].prev_output;
        (input.hash.encode(), input.index)
    }

    /// A chain holding exactly `utxo` for the test client, that never changes.
    ///
    /// Never changing is the point. Straight after a broadcast the read
    /// interface has usually not seen the transaction yet, so it goes on
    /// reporting the input as unspent and says nothing of the change -- and
    /// that is the chain a restart comes back to.
    async fn chain_holding(
        config: &Config,
        utxo: Vec<chain_gang::interface::UtxoEntry>,
    ) -> Arc<dyn BlockchainInterface + Send + Sync> {
        let mut interface = chain_gang::interface::TestInterface::new();
        interface.set_network(&config.get_network().unwrap());
        interface.set_utxo(TEST_ADDRESS, &utxo).await;
        interface.set_height(1_517_571).await;
        Arc::new(interface)
    }

    fn confirmed_utxo(value: i64) -> chain_gang::interface::UtxoEntry {
        chain_gang::interface::UtxoEntry {
            height: 1_514_933,
            tx_pos: 0,
            tx_hash: "f67272e5c1408ecbeb8da543437c125ee1a17110317d44d13eafe31b771b795e".to_string(),
            value,
        }
    }

    /// The ticket's scenario. Fund, restart, fund again against a chain that
    /// has not caught up. Without the state file the second run spends the
    /// same input, rebuilds the same transaction byte for byte -- signing is
    /// deterministic -- and hands the same outpoint to a second caller.
    #[tokio::test]
    async fn cs_465_a_restart_does_not_hand_out_an_input_already_spent() {
        let config = test_config(&unique_dynamic_config_path());
        let request = sample_fund_request(TEST_CLIENT_ID);

        let before = Arc::new(
            Service::new_for_test(&config, test_blockchain_interface(&config).await).await,
        );
        let first = fund_single_transaction(&before, &request)
            .await
            .expect("funds");
        drop(before);

        let after = Arc::new(
            Service::new_for_test(&config, test_blockchain_interface(&config).await).await,
        );
        let second = fund_single_transaction(&after, &request)
            .await
            .expect("funds");

        assert_ne!(
            spent_input(&first),
            spent_input(&second),
            "the restarted service spent the input its previous run had already spent"
        );
        assert_ne!(first.txs[0].hash(), second.txs[0].hash());
    }

    /// And it carries on from where it stopped: the change its last
    /// transaction created is spendable after the restart, though the chain
    /// has not reported it yet. That is the chain the ticket's first run built,
    /// continued rather than restarted.
    #[tokio::test]
    async fn cs_465_a_restart_carries_on_from_its_own_change() {
        let config = test_config(&unique_dynamic_config_path());
        let mut request = sample_fund_request(TEST_CLIENT_ID);
        request.satoshi = 10;

        let before = Arc::new(
            Service::new_for_test(
                &config,
                chain_holding(&config, vec![confirmed_utxo(5_000)]).await,
            )
            .await,
        );
        let first = fund_single_transaction(&before, &request)
            .await
            .expect("funds");
        drop(before);

        let after = Arc::new(
            Service::new_for_test(
                &config,
                chain_holding(&config, vec![confirmed_utxo(5_000)]).await,
            )
            .await,
        );
        let second = fund_single_transaction(&after, &request)
            .await
            .expect("the change from before the restart is still spendable");

        assert_eq!(
            spent_input(&second),
            (first.txs[0].hash().encode(), 0),
            "the second transaction spends the first one's change, output 0"
        );
    }

    /// A broadcast of unknown outcome reserves its inputs too, and that has to
    /// survive a restart just as a successful one does -- it is the same risk.
    #[tokio::test]
    async fn cs_465_an_uncertain_broadcast_is_remembered_across_a_restart() {
        use crate::test_support::UncertainBroadcaster;

        let config = test_config(&unique_dynamic_config_path());
        let request = sample_fund_request(TEST_CLIENT_ID);

        let broadcaster = UncertainBroadcaster::new();
        let before = service_with(
            &config,
            test_blockchain_interface(&config).await,
            broadcaster.clone(),
        )
        .await;
        let _ = fund_single_transaction(&before, &request).await;
        drop(before);

        let after = service_with(
            &config,
            test_blockchain_interface(&config).await,
            broadcaster.clone(),
        )
        .await;
        let _ = fund_single_transaction(&after, &request).await;

        let handed = broadcaster.handed();
        assert_eq!(handed.len(), 2);
        assert_ne!(
            handed[0], handed[1],
            "after the restart the service rebuilt the transaction whose outcome it did not know"
        );
    }

    /// A state file that cannot be read is survived: the service starts, as it
    /// would have with no file at all, rather than refusing to.
    #[tokio::test]
    async fn cs_465_an_unreadable_state_file_does_not_stop_startup() {
        let config = test_config(&unique_dynamic_config_path());
        std::fs::write(config.inflight_state_path(), "{ not json").unwrap();

        let service = Arc::new(
            Service::new_for_test(&config, test_blockchain_interface(&config).await).await,
        );
        let client = service.client_handle(TEST_CLIENT_ID).await.expect("client");
        assert_eq!(client.read().await.reserved_outpoint_count(), 0);
        fund_single_transaction(&service, &sample_fund_request(TEST_CLIENT_ID))
            .await
            .expect("and funds as before");
    }

    // ---- CS-495: commits share in-flight saves ----

    fn inflight_writes(service: &Service) -> u64 {
        service
            .inflight_writes
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn reservations_on_disk(config: &Config) -> usize {
        load_inflight_file(&config.inflight_state_path())
            .get(TEST_CLIENT_ID)
            .map_or(0, |state| state.reserved.len())
    }

    /// The ticket's point. Commits that finish while a write is in progress
    /// queue for the next one, and that one write covers all of them: three
    /// fundings, one write, and all three of their spends on disk before any
    /// of them is answered.
    #[tokio::test]
    async fn cs_495_commits_waiting_on_one_write_share_it() {
        let config = test_config(&unique_dynamic_config_path());
        let service = Arc::new(
            Service::new_for_test(&config, test_blockchain_interface(&config).await).await,
        );
        let before = inflight_writes(&service);

        // a write in progress
        let writing = service.inflight_save.lock().await;
        let requests: Vec<_> = (0..3)
            .map(|_| {
                let service = Arc::clone(&service);
                tokio::spawn(async move {
                    fund_single_transaction(&service, &sample_fund_request(TEST_CLIENT_ID)).await
                })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            requests.iter().all(|request| !request.is_finished()),
            "a funding was answered before its spend was written"
        );

        drop(writing);
        for request in requests {
            request.await.unwrap().expect("funds");
        }
        assert_eq!(
            inflight_writes(&service) - before,
            1,
            "one write for three commits"
        );
        assert_eq!(
            reservations_on_disk(&config),
            3,
            "and it holds all three spends"
        );
    }

    /// Sharing is only with commits already waiting. A commit that arrives
    /// after a write has been taken is not in it, so it gets a write of its
    /// own rather than being answered on the strength of an older one.
    #[tokio::test]
    async fn cs_495_a_commit_after_a_write_gets_the_next_one() {
        let config = test_config(&unique_dynamic_config_path());
        let service = Arc::new(
            Service::new_for_test(&config, test_blockchain_interface(&config).await).await,
        );
        let before = inflight_writes(&service);

        fund_single_transaction(&service, &sample_fund_request(TEST_CLIENT_ID))
            .await
            .expect("funds");
        assert_eq!(reservations_on_disk(&config), 1);
        fund_single_transaction(&service, &sample_fund_request(TEST_CLIENT_ID))
            .await
            .expect("funds");
        assert_eq!(reservations_on_disk(&config), 2);
        assert_eq!(inflight_writes(&service) - before, 2);
    }

    /// A write that fails covers nothing. Commits queued behind it are not
    /// answered on the strength of it: the next one tries again. And once the
    /// file can be written it holds every spend, including those whose own
    /// write failed.
    #[tokio::test]
    async fn cs_495_a_failed_write_is_not_taken_as_covering_anything() {
        let config = test_config(&unique_dynamic_config_path());
        let service = Arc::new(
            Service::new_for_test(&config, test_blockchain_interface(&config).await).await,
        );
        // nothing can be renamed over a directory that has something in it
        let path = config.inflight_state_path();
        let _ = std::fs::remove_file(&path);
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("occupied"), "").unwrap();
        let before = inflight_writes(&service);

        // two commits queued behind a write in progress, as in the sharing case
        let writing = service.inflight_save.lock().await;
        let requests: Vec<_> = (0..2)
            .map(|_| {
                let service = Arc::clone(&service);
                tokio::spawn(async move {
                    fund_single_transaction(&service, &sample_fund_request(TEST_CLIENT_ID)).await
                })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(writing);
        for request in requests {
            request
                .await
                .unwrap()
                .expect("funds, though the state cannot be saved");
        }
        assert_eq!(
            inflight_writes(&service) - before,
            2,
            "the second commit should have tried again after the first write failed"
        );

        std::fs::remove_dir_all(&path).unwrap();
        fund_single_transaction(&service, &sample_fund_request(TEST_CLIENT_ID))
            .await
            .expect("funds");
        assert_eq!(
            reservations_on_disk(&config),
            3,
            "all three spends, once it could"
        );
    }

    // ---- CS-473: concurrent requests for one client ----

    /// The ticket's load. Requests for one client overlap -- each spends as
    /// long between planning and committing as its broadcast takes -- and no
    /// two of the transactions they broadcast may spend the same input. One
    /// of them would be refused by the network as a conflict.
    #[tokio::test]
    async fn cs_473_concurrent_requests_for_one_client_never_share_an_input() {
        use crate::test_support::SlowRecordingBroadcaster;

        let config = test_config(&unique_dynamic_config_path());
        let broadcaster = SlowRecordingBroadcaster::new(Duration::from_millis(50));
        let service = service_with(
            &config,
            test_blockchain_interface(&config).await,
            broadcaster.clone(),
        )
        .await;
        let mut request = sample_fund_request(TEST_CLIENT_ID);
        request.satoshi = 10;

        let calls: Vec<_> = (0..8)
            .map(|_| {
                let service = Arc::clone(&service);
                let request = request.clone();
                tokio::spawn(async move { Service::execute_funding(&service, &request).await })
            })
            .collect();
        for call in calls {
            let _ = call.await.expect("task");
        }

        let spent = broadcaster.spent_inputs();
        let mut seen = std::collections::HashSet::new();
        let reused: Vec<_> = spent.iter().filter(|input| !seen.insert(*input)).collect();
        assert!(
            reused.is_empty(),
            "{} of {} inputs were spent by more than one transaction: {reused:?}",
            reused.len(),
            spent.len()
        );
    }

    /// A broadcast the upstream definitely refused spent nothing, so the input
    /// it claimed goes straight back: not reserved, and spendable by the next
    /// request without waiting for a refresh.
    #[tokio::test]
    async fn cs_473_a_refused_broadcast_gives_its_input_back() {
        use crate::test_support::RejectingBroadcaster;

        let config = test_config(&unique_dynamic_config_path());
        let service = service_with(
            &config,
            test_blockchain_interface(&config).await,
            RejectingBroadcaster::new(false),
        )
        .await;
        let client = service.client_handle(TEST_CLIENT_ID).await.expect("client");
        let held_before = client.read().await.inflight_state();

        let refused = Service::execute_funding(&service, &sample_fund_request(TEST_CLIENT_ID))
            .await
            .expect_err("the upstream refuses");
        assert_eq!(refused.code, ErrorCode::BroadcastRejected);

        assert_eq!(
            client.read().await.reserved_outpoint_count(),
            0,
            "nothing left claimed"
        );
        assert_eq!(client.read().await.inflight_state(), held_before);
    }

    /// When one UTXO is all the client has, a second request arriving while
    /// the first is broadcasting finds nothing to spend. It is told so, with
    /// a coded error, instead of building a conflicting transaction -- and
    /// only one transaction ever reaches the network.
    #[tokio::test]
    async fn cs_473_contention_for_one_utxo_is_refused_rather_than_conflicted() {
        use crate::test_support::SlowRecordingBroadcaster;

        let config = test_config(&unique_dynamic_config_path());
        let broadcaster = SlowRecordingBroadcaster::new(Duration::from_millis(100));
        let service = service_with(
            &config,
            chain_holding(&config, vec![confirmed_utxo(50_000)]).await,
            broadcaster.clone(),
        )
        .await;
        let mut request = sample_fund_request(TEST_CLIENT_ID);
        request.satoshi = 10;

        let first = {
            let (service, request) = (Arc::clone(&service), request.clone());
            tokio::spawn(async move { Service::execute_funding(&service, &request).await })
        };
        // The first has claimed the only UTXO and is still broadcasting
        tokio::time::sleep(Duration::from_millis(30)).await;
        let second = Service::execute_funding(&service, &request).await;

        assert!(first.await.expect("task").is_ok(), "the first funds");
        let refused = second.expect_err("the second has nothing to spend");
        assert_ne!(
            refused.code,
            ErrorCode::Internal,
            "an honest code, not a 500: {refused:?}"
        );
        assert_ne!(
            refused.code,
            ErrorCode::BroadcastRejected,
            "refused before the network, not by it"
        );
        assert_eq!(
            broadcaster.spent_inputs().len(),
            1,
            "one transaction reached the network"
        );
    }

    // ---- CS-474: the service's contract, stated from the caller's side ----

    /// Concurrent identical requests -- the ticket's load -- and no outpoint
    /// in any successful response may appear in another. Checked on what the
    /// callers were told, not on what was broadcast: a duplicate used to be a
    /// 200 to every one of them, with nothing logged.
    #[tokio::test]
    async fn cs_474_concurrent_identical_requests_are_never_given_the_same_outpoint() {
        use crate::test_support::SlowRecordingBroadcaster;

        let config = test_config(&unique_dynamic_config_path());
        let broadcaster = SlowRecordingBroadcaster::new(Duration::from_millis(50));
        let service = service_with(
            &config,
            test_blockchain_interface(&config).await,
            broadcaster.clone(),
        )
        .await;
        let mut request = sample_fund_request(TEST_CLIENT_ID);
        request.satoshi = 10;

        let calls: Vec<_> = (0..10)
            .map(|_| {
                let (service, request) = (Arc::clone(&service), request.clone());
                tokio::spawn(async move { Service::execute_funding(&service, &request).await })
            })
            .collect();
        let mut handed = std::collections::HashMap::new();
        for call in calls {
            if let Ok(response) = call.await.expect("task") {
                for outpoint in &response.outpoints {
                    *handed
                        .entry((outpoint.hash.encode(), outpoint.index))
                        .or_insert(0) += 1;
                }
            }
        }
        let shared: Vec<_> = handed.iter().filter(|(_, n)| **n > 1).collect();
        assert!(!handed.is_empty(), "something was funded");
        assert!(
            shared.is_empty(),
            "outpoints given to more than one caller: {shared:?}"
        );
    }

    // ---- CS-475: a refusal under load says the funds are in flight ----

    /// The ticket's load against a wallet of three UTXOs. Every request that
    /// finds them all claimed is refused with `funds_in_flight` -- not with
    /// the codes that tell an operator to top up or consolidate -- and none
    /// of the refusals is anything else.
    #[tokio::test]
    async fn cs_475_contention_is_reported_as_funds_in_flight() {
        use crate::test_support::SlowRecordingBroadcaster;

        let config = test_config(&unique_dynamic_config_path());
        let broadcaster = SlowRecordingBroadcaster::new(Duration::from_millis(100));
        let chain = vec![
            confirmed_utxo(50_000),
            chain_gang::interface::UtxoEntry {
                tx_pos: 1,
                ..confirmed_utxo(50_000)
            },
            chain_gang::interface::UtxoEntry {
                tx_pos: 2,
                ..confirmed_utxo(50_000)
            },
        ];
        let service = service_with(
            &config,
            chain_holding(&config, chain).await,
            broadcaster.clone(),
        )
        .await;
        let mut request = sample_fund_request(TEST_CLIENT_ID);
        request.satoshi = 10;

        let calls: Vec<_> = (0..8)
            .map(|_| {
                let (service, request) = (Arc::clone(&service), request.clone());
                tokio::spawn(async move { Service::execute_funding(&service, &request).await })
            })
            .collect();
        let mut refusals = Vec::new();
        for call in calls {
            if let Err(error) = call.await.expect("task") {
                refusals.push(error.code);
            }
        }
        assert!(!refusals.is_empty(), "the load outran the wallet");
        assert!(
            refusals
                .iter()
                .all(|code| *code == ErrorCode::FundsInFlight),
            "{refusals:?}"
        );
    }

    // ---- CS-482: every broadcast's outcome is logged with its txid ----

    /// The lines this test's thread logged that name `txid`.
    fn lines_naming(txid: &str) -> Vec<(log::Level, String)> {
        crate::test_support::logged_on_this_thread()
            .into_iter()
            .filter(|(_, line)| line.contains(&format!("funding tx {txid}")))
            .collect()
    }

    /// The txid of every attempt this test's thread logged, in order.
    fn attempted_txids() -> Vec<String> {
        crate::test_support::logged_on_this_thread()
            .into_iter()
            .filter_map(|(_, line)| {
                line.strip_prefix("broadcasting funding tx ")
                    .map(|rest| rest[..64].to_string())
            })
            .collect()
    }

    /// A success is logged as one: the attempt, naming the client, the
    /// outpoint count and the broadcaster, then an acceptance. It used to log
    /// nothing at all, so success could only be inferred from a failure line
    /// not appearing.
    #[tokio::test]
    async fn cs_482_an_accepted_broadcast_is_logged_with_its_txid() {
        crate::test_support::capture_logs();
        let config = test_config(&unique_dynamic_config_path());
        let service = service_with(
            &config,
            test_blockchain_interface(&config).await,
            crate::test_support::CountingBroadcaster::new(),
        )
        .await;

        let funded = Service::execute_funding(&service, &sample_fund_request(TEST_CLIENT_ID))
            .await
            .expect("funds");
        let txid = funded.txs[0].hash().encode();

        let lines = lines_naming(&txid);
        let levels: Vec<_> = lines.iter().map(|(level, _)| *level).collect();
        assert_eq!(
            levels,
            vec![log::Level::Info, log::Level::Debug, log::Level::Info],
            "{lines:?}"
        );
        assert_eq!(
            lines[0].1,
            format!(
                "broadcasting funding tx {txid} (client_id={TEST_CLIENT_ID}, outpoints=1, \
                 broadcaster=counting)"
            )
        );
        assert!(
            lines[1].1.starts_with(&format!("funding tx {txid} hex=")),
            "{lines:?}"
        );
        assert_eq!(
            lines[2].1,
            format!("broadcast accepted: funding tx {txid} (broadcaster=counting)")
        );
    }

    /// A failure names the transaction, its class and the upstream's reason.
    /// The class is the one the caller's error code says, so the log and the
    /// response agree -- including a refusal the upstream called retryable,
    /// which the caller is told is `broadcast_failed`.
    #[tokio::test]
    async fn cs_482_a_failed_broadcast_is_logged_with_its_txid_and_class() {
        use crate::test_support::{RejectingBroadcaster, UncertainBroadcaster};

        crate::test_support::capture_logs();
        let cases: Vec<(Arc<dyn TxBroadcaster>, ErrorCode, &str, &str)> = vec![
            (
                RejectingBroadcaster::new(false),
                ErrorCode::BroadcastRejected,
                "class=rejected, broadcaster=rejecting",
                "rejected: txn-mempool-conflict (retryable: false)",
            ),
            (
                RejectingBroadcaster::new(true),
                ErrorCode::BroadcastFailed,
                "class=failed, broadcaster=rejecting",
                "rejected: txn-mempool-conflict (retryable: true)",
            ),
            (
                UncertainBroadcaster::new(),
                ErrorCode::BroadcastOutcomeUnknown,
                "class=indeterminate, broadcaster=uncertain",
                "outcome unknown: simulated submit deadline expiry",
            ),
        ];
        for (broadcaster, code, class, reason) in cases {
            // Signing is deterministic, so every case builds the same
            // transaction: read only what this case logged.
            let already = crate::test_support::logged_on_this_thread().len();
            let config = test_config(&unique_dynamic_config_path());
            let service = service_with(
                &config,
                test_blockchain_interface(&config).await,
                broadcaster,
            )
            .await;

            let error = Service::execute_funding(&service, &sample_fund_request(TEST_CLIENT_ID))
                .await
                .expect_err("the broadcast does not succeed");
            assert_eq!(error.code, code);

            let txid = attempted_txids().pop().expect("the attempt was logged");
            let this_case: Vec<_> = crate::test_support::logged_on_this_thread()
                .split_off(already)
                .into_iter()
                .filter(|(_, line)| line.contains(&format!("funding tx {txid}")))
                .collect();
            let failure: Vec<_> = this_case
                .iter()
                .filter(|(level, _)| *level == log::Level::Warn)
                .cloned()
                .collect();
            assert_eq!(
                failure,
                vec![(
                    log::Level::Warn,
                    format!("broadcast failed: funding tx {txid} ({class}): {reason}")
                )],
                "{code:?}"
            );
            assert!(
                !this_case
                    .iter()
                    .any(|(_, line)| line.starts_with("broadcast accepted")),
                "a failure is not also logged as accepted"
            );
        }
    }

    /// Separate transactions get a pair of lines each, so a partial broadcast
    /// reads as accepted lines followed by one failure -- each naming its own
    /// transaction.
    #[tokio::test]
    async fn cs_482_a_partial_broadcast_logs_each_transaction() {
        crate::test_support::capture_logs();
        let config = test_config(&unique_dynamic_config_path());
        let service = service_with(
            &config,
            test_blockchain_interface(&config).await,
            crate::test_support::FailingBroadcaster::new(1),
        )
        .await;
        let request = FundRequest {
            no_of_outpoints: 2,
            multiple_tx: true,
            ..sample_fund_request(TEST_CLIENT_ID)
        };

        let _ = Service::fund_with_multiple_transactions(&service, &request)
            .await
            .expect_err("the second transaction fails");

        let txids = attempted_txids();
        assert_eq!(txids.len(), 2, "{txids:?}");
        assert_ne!(txids[0], txids[1]);
        assert!(lines_naming(&txids[0])
            .iter()
            .any(|(level, line)| *level == log::Level::Info
                && line.starts_with("broadcast accepted")));
        assert!(lines_naming(&txids[1])
            .iter()
            .any(|(level, line)| *level == log::Level::Warn
                && line.starts_with(&format!("broadcast failed: funding tx {}", txids[1]))
                && line.contains("class=failed, broadcaster=failing")
                && line.ends_with("upstream error: simulated broadcast failure")));
    }

    /// CS-514: on a production network without the opt-out, a client with no
    /// `api_key` stops startup, and cannot be added at runtime.
    #[tokio::test]
    async fn sr_sec_031_every_client_needs_an_api_key_on_a_production_network() {
        let path = unique_dynamic_config_path();
        let mut config = test_config_with_keys(&path, None, Some("admin"));
        config.web_interface.allow_unauthenticated = false;
        let blockchain = test_blockchain_interface(&config).await;
        let service = Service::new_for_test(&config, blockchain).await;
        let err = service.check_auth_policy().await.unwrap_err();
        assert!(err.contains(TEST_CLIENT_ID), "{err}");

        let added = service
            .add_client(&ClientConfig {
                client_id: "keyless".to_string(),
                wif_key: "env:FS_RUNTIME_WIF_KEYLESS".to_string(),
                api_key: None,
            })
            .await
            .unwrap_err();
        assert_eq!(added.code, ErrorCode::InvalidRequest);
        assert!(
            added.description.contains("api_key"),
            "{}",
            added.description
        );

        // With a key it starts.
        let mut config = test_config_with_keys(&path, Some("client-key"), Some("admin"));
        config.web_interface.allow_unauthenticated = false;
        let blockchain = test_blockchain_interface(&config).await;
        let service = Service::new_for_test(&config, blockchain).await;
        assert!(service.check_auth_policy().await.is_ok());
    }

    /// CS-514: without an admin key, a production network does not start;
    /// with one from the environment it does; regtest never needed one.
    #[tokio::test]
    async fn sr_sec_031_a_production_network_needs_an_admin_key() {
        let path = unique_dynamic_config_path();
        let mut config = test_config_with_keys(&path, Some("client-key"), None);
        config.web_interface.allow_unauthenticated = false;
        let blockchain = test_blockchain_interface(&config).await;
        let service = Service::new_for_test(&config, blockchain).await;
        let err = service.check_auth_policy().await.unwrap_err();
        assert!(err.contains("admin_api_key"), "{err}");

        config.blockchain_interface.network = "regtest".to_string();
        let blockchain = test_blockchain_interface(&config).await;
        let service = Service::new_for_test(&config, blockchain).await;
        assert!(service.check_auth_policy().await.is_ok());
    }

    /// The opt-out lets a keyless client start, as before.
    #[tokio::test]
    async fn sr_sec_031_the_opt_out_allows_keyless_clients() {
        let config = test_config_with_keys(&unique_dynamic_config_path(), None, None);
        assert!(config.web_interface.allow_unauthenticated);
        let blockchain = test_blockchain_interface(&config).await;
        let service = Service::new_for_test(&config, blockchain).await;
        assert!(service.check_auth_policy().await.is_ok());
    }

    /// A quote above `fees.max_satoshis_per_kb` is ignored and the current
    /// rate kept; one at the ceiling is taken (CS-523).
    #[tokio::test]
    async fn sr_sec_020_a_fee_quote_above_the_ceiling_is_ignored() {
        use crate::test_support::StubMapiBroadcaster;

        for (quoted, expected) in [(1001, 100), (50_000, 100), (1000, 1000), (250, 250)] {
            let config = test_config(&unique_dynamic_config_path());
            assert_eq!(config.fees.satoshis_per_kb, 100);
            assert_eq!(config.fees.max_satoshis_per_kb, 1000);
            let blockchain = test_blockchain_interface(&config).await;
            let service = service_with(
                &config,
                blockchain,
                StubMapiBroadcaster::quoting(Some(quoted)),
            )
            .await;
            service.refresh_fee_rate().await;
            assert_eq!(
                service.fee_satoshis_per_kb().await,
                expected,
                "quote of {quoted}"
            );
        }
    }

    // --- CS-521: one wallet, one client -----------------------------------

    /// A new client funding from a wallet an existing client already uses is
    /// refused, however the key arrived -- here, as the literal key.
    #[tokio::test]
    async fn sr_sec_018_a_client_sharing_another_s_wallet_is_refused() {
        let config = test_config(&unique_dynamic_config_path());
        let blockchain = test_blockchain_interface(&config).await;
        let service = Service::new_for_test(&config, blockchain).await;

        let error = service
            .add_client(&ClientConfig {
                client_id: "thief".to_string(),
                wif_key: TEST_WIF.to_string(),
                api_key: Some("mine".to_string()),
            })
            .await
            .expect_err("the wallet is id1's");
        assert_eq!(error.code, ErrorCode::InvalidRequest);
        assert!(
            error.description.contains("already the funding key"),
            "{}",
            error.description
        );
        assert!(service.client_handle("thief").await.is_none());
    }

    /// A pair added before the check existed is still found, at startup.
    #[tokio::test]
    async fn sr_sec_018_clients_sharing_a_wallet_are_found_at_startup() {
        let mut config = test_config(&unique_dynamic_config_path());
        config.client = Some(vec![
            ClientConfig {
                client_id: TEST_CLIENT_ID.to_string(),
                wif_key: TEST_WIF.to_string(),
                api_key: None,
            },
            ClientConfig {
                client_id: "twin".to_string(),
                wif_key: TEST_WIF.to_string(),
                api_key: None,
            },
        ]);
        let blockchain = test_blockchain_interface(&config).await;
        let service = Service::new_for_test(&config, blockchain).await;

        let shared = super::wallets_shared_by(&*service.clients.read().await);
        assert_eq!(
            shared,
            vec![(
                TEST_ADDRESS.to_string(),
                vec![TEST_CLIENT_ID.to_string(), "twin".to_string()]
            )]
        );
    }

    /// CS-634: a configured client whose key is for another network than
    /// `blockchain_interface.network` stops startup, naming it.
    #[tokio::test]
    async fn cs_634_a_client_key_for_another_network_stops_startup() {
        use crate::broadcaster::woc::WocBroadcaster;
        let mut config = test_config(&unique_dynamic_config_path());
        config.client.as_mut().unwrap()[0].wif_key =
            crate::test_support::MAPI_TEST_SIGNER_WIF.to_string();
        let blockchain = test_blockchain_interface(&config).await;
        let broadcaster: Arc<dyn TxBroadcaster> = Arc::new(WocBroadcaster::new(
            Arc::clone(&blockchain),
            &config.blockchain_interface.interface_type,
        ));
        let err = Service::build(
            &config,
            Backend {
                interface: blockchain,
                address_watcher: None,
            },
            broadcaster,
        )
        .err()
        .expect("startup must fail");
        assert!(err.contains("a mainnet key"), "{err}");
        assert!(err.contains("network is testnet"), "{err}");
        assert!(err.contains(TEST_CLIENT_ID), "{err}");
    }

    /// CS-634: `POST /client` refuses a key for another network as the
    /// caller's mistake, naming no part of it, and adds nothing.
    #[tokio::test]
    async fn cs_634_post_client_refuses_a_key_for_another_network() {
        let config = test_config(&unique_dynamic_config_path());
        let blockchain = test_blockchain_interface(&config).await;
        let service = Service::new_for_test(&config, blockchain).await;
        let wif = crate::test_support::MAPI_TEST_SIGNER_WIF;
        let error = service
            .add_client(&ClientConfig {
                client_id: "mainnet-key".to_string(),
                wif_key: wif.to_string(),
                api_key: Some("k".to_string()),
            })
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidRequest);
        assert!(
            error.description.contains("a mainnet key"),
            "{}",
            error.description
        );
        assert!(
            !error.description.contains(&wif[..8]),
            "{}",
            error.description
        );
        assert!(!service.clients.read().await.contains_key("mainnet-key"));
    }
}
