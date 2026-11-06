use std::{collections::BTreeMap, env, net::SocketAddr, time::Duration};

use chain_gang::network::Network;
use log::debug;
use serde::{Deserialize, Serialize};

use crate::secrets::resolve_secret;

/// Blockchain Interface Configuration
#[derive(Debug, Deserialize, Clone)]
pub struct BlockchainInterfaceConfig {
    pub interface_type: String,
    /// `mainnet`, `testnet`, `regtest` or `stn`, spelt exactly so: the names
    /// mapi-lite publishes for its nodes' chain, which this is checked
    /// against, and Event's (CS-624). It was `network_type`.
    pub network: String,
    /// Endpoint for the interfaces that need one: the UaaS base URL, or the
    /// node's JSON-RPC host and port for `rpc` (a bare `host:port` is taken
    /// as `http://`).
    pub url: Option<String>,
    /// JSON-RPC username, for `interface_type = "rpc"`.
    #[serde(default)]
    pub rpc_user: Option<String>,
    /// JSON-RPC password, for `interface_type = "rpc"`. Takes an
    /// `env:VAR_NAME` reference, or `FS__BLOCKCHAIN_INTERFACE__RPC_PASSWORD`.
    #[serde(default)]
    pub rpc_password: Option<String>,
    /// Whether to ask the node to watch each client's funding address at
    /// startup, and when a client is added at runtime.
    ///
    /// On by default, because a node reports zero for an address its wallet
    /// does not track -- with no error -- so without the import the service
    /// looks broken. Turn it off if you manage the node's wallet yourself, or
    /// if it is a descriptor wallet, where `importaddress` is refused.
    #[serde(default = "default_rpc_import_addresses")]
    pub rpc_import_addresses: bool,
    /// Ceiling on outbound requests per second to the blockchain interface.
    ///
    /// Left unset it follows the interface: `woc` talks to a public API that
    /// documents "up to 3 requests/sec is free" and answers 429 above it, so
    /// it is limited by default; `rpc` and `uaas` are the operator's own
    /// servers and are not. Set it explicitly to override either way, or to
    /// `0` to turn the limit off.
    #[serde(default)]
    pub max_requests_per_second: Option<u32>,
    /// Deadline on each call to the interface, in seconds (CS-502). Unset
    /// means [`DEFAULT_INTERFACE_TIMEOUT_SECONDS`]; `0` turns it off.
    ///
    /// A call that outlives it fails as a chain failure, so a read that is
    /// accepted and never answered cannot hold a client's refresh forever.
    /// It bounds a whole call, which for WhatsOnChain's paged UTXO listing is
    /// several paced requests, so leave room for the largest address.
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
}

/// The per-call deadline when `timeout_seconds` is unset (CS-502).
pub const DEFAULT_INTERFACE_TIMEOUT_SECONDS: u64 = 30;

fn default_rpc_import_addresses() -> bool {
    true
}

/// WhatsOnChain's documented free-tier allowance, and so the default for
/// `interface_type = "woc"`. Exceeding it earns a 429, and sustained
/// violation earns a ban, which no retry recovers from.
pub const WOC_REQUESTS_PER_SECOND: u32 = 3;

impl Default for BlockchainInterfaceConfig {
    fn default() -> Self {
        BlockchainInterfaceConfig {
            interface_type: String::new(),
            network: String::new(),
            url: None,
            rpc_user: None,
            rpc_password: None,
            rpc_import_addresses: default_rpc_import_addresses(),
            max_requests_per_second: None,
            timeout_seconds: None,
        }
    }
}

/// The networks `blockchain_interface.network` may name, spelt as mapi-lite
/// publishes its nodes' chain (`uls_core::wire::network_of_chain`).
pub const NETWORKS: [&str; 4] = ["mainnet", "testnet", "regtest", "stn"];

/// `network` exactly as one of [`NETWORKS`], or why not, naming the spelling
/// to write for one that means the same network (CS-624). Another spelling
/// used to fail later and less clearly, as "unable to decode network".
fn check_network(network: &str) -> Result<(), String> {
    if NETWORKS.contains(&network) {
        return Ok(());
    }
    let meant = match network.trim().to_ascii_lowercase().as_str() {
        "mainnet" | "main" | "livenet" | "bsv" => Some("mainnet"),
        "testnet" | "test" | "testnet3" => Some("testnet"),
        "regtest" => Some("regtest"),
        "stn" => Some("stn"),
        _ => None,
    };
    Err(match meant {
        Some(canonical) => format!(
            "blockchain_interface.network {network:?} is not a network name: write \"{canonical}\""
        ),
        None => format!(
            "blockchain_interface.network {network:?} is not a network name: it is one of {}",
            NETWORKS.join(", ")
        ),
    })
}

impl BlockchainInterfaceConfig {
    /// Outbound requests per second to allow, or `None` for no limit.
    ///
    /// An explicit setting always wins, including an explicit `0` meaning
    /// unlimited. Otherwise only the interfaces that call somebody else's
    /// server are limited.
    /// The deadline on each interface call, or `None` when it is turned off
    /// (CS-502).
    pub fn call_timeout(&self) -> Option<std::time::Duration> {
        match self
            .timeout_seconds
            .unwrap_or(DEFAULT_INTERFACE_TIMEOUT_SECONDS)
        {
            0 => None,
            seconds => Some(std::time::Duration::from_secs(seconds)),
        }
    }

    pub fn outbound_rate_limit(&self) -> Option<u32> {
        match self.max_requests_per_second {
            Some(0) => None,
            Some(limit) => Some(limit),
            None if self.interface_type == "woc" => Some(WOC_REQUESTS_PER_SECOND),
            None => None,
        }
    }

    /// Reject a configuration the chosen interface cannot work with, at
    /// startup rather than on the first request.
    pub fn validate(&self) -> Result<(), String> {
        check_network(&self.network)?;
        match self.interface_type.as_str() {
            "uaas" => {
                if self.url.as_deref().unwrap_or("").is_empty() {
                    return Err(
                        "blockchain_interface.url is required for interface_type = \"uaas\""
                            .to_string(),
                    );
                }
            }
            "rpc" => {
                if self.url.as_deref().unwrap_or("").is_empty() {
                    return Err("blockchain_interface.url is required for interface_type = \"rpc\" (the node's JSON-RPC host and port)".to_string());
                }
                if self.rpc_user.as_deref().unwrap_or("").is_empty() {
                    return Err(
                        "blockchain_interface.rpc_user is required for interface_type = \"rpc\""
                            .to_string(),
                    );
                }
                if self.rpc_password.as_deref().unwrap_or("").is_empty() {
                    return Err(
                        "blockchain_interface.rpc_password is required for interface_type = \"rpc\""
                            .to_string(),
                    );
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// Client Configuration
#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct ClientConfig {
    pub client_id: String,
    pub wif_key: String,
    /// When set, required for this client's API endpoints.
    #[serde(default)]
    pub api_key: Option<String>,
}

/// OpenTelemetry trace export configuration.
#[derive(Debug, Default, Deserialize, Clone)]
pub struct TelemetryConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub service_name: Option<String>,
    #[serde(default)]
    pub otlp_endpoint: Option<String>,
    /// Seconds between metrics exports (CS-674). `OTEL_METRIC_EXPORT_INTERVAL`
    /// (milliseconds) overrides it; unset, the SDK's 60s.
    #[serde(default)]
    pub export_interval_sec: Option<u64>,
    /// Resource attributes on traces and metrics, such as
    /// `deployment.environment.name` (CS-674). A key `OTEL_RESOURCE_ATTRIBUTES`
    /// also sets is taken from the variable. `service.name` and
    /// `service.version` are ignored here.
    #[serde(default)]
    pub resource_attributes: BTreeMap<String, String>,
}

#[derive(Debug, Default, Deserialize, Clone)]
pub struct ServiceConfig {
    pub utxo_refresh_period: u64,
    /// How old the cached chain state may be before a request refreshes it,
    /// in seconds.
    ///
    /// Defaults to `utxo_refresh_period`, so a request reuses whatever the
    /// periodic refresh last fetched and the steady-state cost is one request
    /// per client per period however much traffic arrives. Lower it to narrow
    /// the window in which the service can build on a view of the chain that
    /// something outside the service has moved on from; `0` refreshes on every
    /// request, which is what the service did before.
    #[serde(default)]
    pub chain_state_max_age_seconds: Option<u64>,
    /// How old a client's chain read may be before `GET /ready` reports the
    /// service unready, in seconds (CS-505, SR-BCHN-017).
    ///
    /// With `[mapi_lite]` a funding request does not wait for a refresh while
    /// the cache is usable (SR-FUND-031), so if chain reads keep failing the
    /// service goes on funding from an ever older view and nothing else says
    /// so. Defaults to three refresh periods, and at least 60s: one failed
    /// sweep is a blip, three is chain reads not working. `0` turns the check
    /// off.
    #[serde(default)]
    pub ready_max_chain_read_age_seconds: Option<u64>,
    /// Where to keep what the service has broadcast but the chain may not have
    /// caught up with, so a restart does not forget it (CS-465).
    ///
    /// Defaults to a file beside `dynamic_config.filename`, which a deployment
    /// already has to keep across restarts; see [`Config::inflight_state_path`].
    #[serde(default)]
    pub inflight_state_file: Option<String>,
}

impl ServiceConfig {
    /// How old cached chain state may be before a request refreshes it.
    pub fn chain_state_max_age(&self) -> std::time::Duration {
        std::time::Duration::from_secs(
            self.chain_state_max_age_seconds
                .unwrap_or(self.utxo_refresh_period),
        )
    }

    /// The bound `GET /ready` holds chain reads to, or `None` when the check
    /// is off. See `ready_max_chain_read_age_seconds`.
    pub fn ready_max_chain_read_age(&self) -> Option<std::time::Duration> {
        let seconds = self
            .ready_max_chain_read_age_seconds
            .unwrap_or_else(|| (self.utxo_refresh_period.saturating_mul(3)).max(60));
        (seconds > 0).then(|| std::time::Duration::from_secs(seconds))
    }
}

/// Retention policy for `POST /fund` idempotency records.
///
/// Records are held in memory only, so they do not survive a restart -- a
/// retry that spans one can still produce a second funding transaction.
#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct IdempotencyConfig {
    /// How long a completed record stays replayable, in seconds.
    #[serde(default = "default_idempotency_ttl_seconds")]
    pub ttl_seconds: u64,
    /// Upper bound on retained records, so a client cannot grow the store
    /// without limit by sending a fresh `idempotency_key` every time.
    #[serde(default = "default_idempotency_max_entries")]
    pub max_entries: usize,
}

/// The fee rate the service pays on the funding transactions it builds.
///
/// Before CS-451 this was hardcoded as `((tx_bytes / 1000) * 500) + 750`,
/// which is a step function rather than a rate: it charged 750 satoshi for any
/// transaction under a kilobyte, and jumped by 500 at each kilobyte after.
/// A 226-byte funding transaction -- the ordinary shape, one input and two
/// outputs -- therefore paid 750 satoshi, an effective 3300 sat/KB. The rate
/// is now stated directly, so what the service pays is a number an operator
/// can read and change rather than an artefact of an expression.
#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct FeesConfig {
    /// Satoshis per kilobyte of transaction. The fee for a transaction is
    /// `ceil(tx_bytes * satoshis_per_kb / 1000)`, rounded up so that rounding
    /// never underpays and risks a rejection.
    #[serde(default = "default_satoshis_per_kb")]
    pub satoshis_per_kb: u64,
    /// The highest rate a mapi-lite fee quote may set, in satoshis per KB
    /// (CS-523, SR-SEC-020). A quote above it is ignored and the current rate
    /// kept, so a forged or mistaken quote cannot make every funding
    /// transaction overpay. Default 1000: ten times `satoshis_per_kb`'s
    /// default, and a thousand times mapi-lite's own default quote. Must be at
    /// least `satoshis_per_kb`.
    #[serde(default = "default_max_satoshis_per_kb")]
    pub max_satoshis_per_kb: u64,
}

impl Default for FeesConfig {
    fn default() -> Self {
        FeesConfig {
            satoshis_per_kb: default_satoshis_per_kb(),
            max_satoshis_per_kb: default_max_satoshis_per_kb(),
        }
    }
}

impl FeesConfig {
    /// A rate of zero would build transactions no miner accepts, and the
    /// service would only find out at broadcast.
    pub fn validate(&self) -> Result<(), String> {
        if self.satoshis_per_kb == 0 {
            return Err("fees.satoshis_per_kb must be greater than 0: a \
                        transaction paying no fee is not relayed."
                .to_string());
        }
        if self.max_satoshis_per_kb < self.satoshis_per_kb {
            return Err(format!(
                "fees.max_satoshis_per_kb ({}) must be at least satoshis_per_kb ({})",
                self.max_satoshis_per_kb, self.satoshis_per_kb
            ));
        }
        Ok(())
    }
}

/// Satoshis per kilobyte when nothing is configured.
///
/// Deliberately far below what the superseded expression charged. See
/// [`FeesConfig`] and docs/Configuration.md: an upgrade without a `[fees]`
/// section pays less than it used to, which is the point of CS-451, but it is
/// a behaviour change and not only a refactor.
pub const DEFAULT_SATOSHIS_PER_KB: u64 = 100;

/// See [`FeesConfig::max_satoshis_per_kb`].
pub const DEFAULT_MAX_SATOSHIS_PER_KB: u64 = 1000;

fn default_max_satoshis_per_kb() -> u64 {
    DEFAULT_MAX_SATOSHIS_PER_KB
}

fn default_satoshis_per_kb() -> u64 {
    DEFAULT_SATOSHIS_PER_KB
}

impl Default for IdempotencyConfig {
    fn default() -> Self {
        IdempotencyConfig {
            ttl_seconds: default_idempotency_ttl_seconds(),
            max_entries: default_idempotency_max_entries(),
        }
    }
}

fn default_idempotency_ttl_seconds() -> u64 {
    600
}

fn default_idempotency_max_entries() -> usize {
    10_000
}

impl IdempotencyConfig {
    pub fn ttl(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.ttl_seconds)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.ttl_seconds == 0 {
            return Err("idempotency.ttl_seconds must be greater than zero".to_string());
        }
        if self.max_entries == 0 {
            return Err("idempotency.max_entries must be greater than zero".to_string());
        }
        Ok(())
    }
}

/// Optional mapi-lite integration.
///
/// When this section is present, funding transactions are broadcast through
/// the named mapi-lite server instead of through the `[blockchain_interface]`;
/// chain reads (balances, UTXOs) are unaffected. The two settings that select
/// and authenticate the server mirror the ones teranode-event-rs uses for the
/// same integration (`mapi_base_url`, `auth_token`). One setting bounds how
/// long a funding call may wait on mapi-lite, `total_timeout_seconds`; the
/// rest of the timing is derived (CS-624).
#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct MapiLiteConfig {
    /// Base URL of the mapi-lite server, e.g. `http://127.0.0.1:8080`.
    /// Surrounding whitespace and a trailing `/` are tolerated; read it
    /// through [`MapiLiteConfig::base_url`] rather than touching the field,
    /// so what was validated is what gets requested.
    pub base_url: String,
    /// The token alone, as mapi-lite's own `auth_token` is (CS-624): every
    /// request carries `Authorization: Bearer <token>`, see
    /// [`MapiLiteConfig::authorization`]. A value starting `Bearer ` is
    /// refused at startup. Takes an `env:VAR_NAME` reference, or
    /// `FS__MAPI_LITE__AUTH_TOKEN`.
    #[serde(default)]
    pub auth_token: Option<String>,
    /// Upper bound on a whole submit -- every attempt and every back-off
    /// between them -- in seconds: the one timing setting for this hop
    /// (CS-624). Each attempt outlasts the submit ceiling mapi-lite publishes
    /// by [`SUBMIT_CEILING_MARGIN`], and as many retries run as fit inside
    /// it: see [`SubmitBudget`]. It is what a `/fund` caller, and the worker
    /// serving it, can be held by a wedged mapi-lite, and part of the worst
    /// case `GET /status` publishes.
    #[serde(default = "default_mapi_lite_total_timeout_seconds")]
    pub total_timeout_seconds: u64,
    /// mapi-lite's miner-ID public keys, as hex: the `minerId` in every
    /// response it signs (CS-523, SR-SEC-019). A response signed by any other
    /// key is refused. Several allow a rotation. Empty -- the default -- accepts
    /// any valid signature, which proves nothing about the sender, so the
    /// service warns at startup. An `FS__` override gives it comma-separated,
    /// the one form an override can (CS-636).
    #[serde(default, deserialize_with = "list_or_comma_separated")]
    pub miner_id_public_keys: Vec<String>,
}

fn default_mapi_lite_total_timeout_seconds() -> u64 {
    45
}

/// mapi-lite's own default submit ceiling (its `submit_timeout_sec`),
/// assumed until its `/ready` publishes the one it runs with (CS-624).
pub const ASSUMED_SUBMIT_CEILING: Duration = Duration::from_secs(10);

/// How far an attempt outlasts mapi-lite's submit ceiling, so that its `504`
/// for a hung node arrives before this side gives up: the answer still has to
/// cross the network. uls-client warns below one second; three keeps the 13s
/// attempt the defaults have always had against mapi-lite's default 10s.
pub const SUBMIT_CEILING_MARGIN: Duration = Duration::from_secs(3);

/// The bound on the mapi-lite probe behind `GET /ready`, and how long its
/// verdict is cached (CS-624, as Event's). Under the three seconds a readiness
/// probe allows (Docker's `--timeout=3s` default): a slower probe is killed
/// by the caller rather than answered, marking the instance unready on every
/// check even while mapi-lite is fine.
pub const MAPI_LITE_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// The wait before submit retry `n` is `n` times this, capped at
/// [`RETRY_BACKOFF_CAP`]: uls-client's own default since v0.5.0, set here so
/// it does not change under this service with a later release.
pub const RETRY_BACKOFF_BASE: Duration = Duration::from_secs(1);
pub const RETRY_BACKOFF_CAP: Duration = Duration::from_secs(5);

/// The wait before submit retry `n` (from 1).
pub fn retry_backoff(n: u32) -> Duration {
    (RETRY_BACKOFF_BASE * n).min(RETRY_BACKOFF_CAP)
}

/// How a submit spends `mapi_lite.total_timeout_seconds` (CS-624).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubmitBudget {
    /// The bound on each attempt.
    pub attempt: Duration,
    /// Attempts after the first.
    pub retries: u32,
    /// The bound on the whole submit, back-off included.
    pub deadline: Duration,
}

impl SubmitBudget {
    /// The budget against mapi-lite's submit `ceiling`: each attempt outlasts
    /// it by [`SUBMIT_CEILING_MARGIN`], cut to the deadline if it must be, and
    /// as many retries as fit inside the deadline when every attempt runs to
    /// its bound and every back-off is waited out. Each retry it buys is a
    /// real one; none starts that the deadline would cut off.
    pub fn new(ceiling: Duration, deadline: Duration) -> Self {
        let attempt = (ceiling + SUBMIT_CEILING_MARGIN).min(deadline);
        let mut elapsed = attempt;
        let mut retries = 0;
        loop {
            let next = elapsed + retry_backoff(retries + 1) + attempt;
            if next > deadline {
                break;
            }
            elapsed = next;
            retries += 1;
        }
        SubmitBudget {
            attempt,
            retries,
            deadline,
        }
    }

    /// Whether the deadline leaves an attempt less time than mapi-lite needs
    /// to answer a hung node itself, at `ceiling`: the node is then reported
    /// as this side's deadline rather than mapi-lite's `504`. The outcome is
    /// unknown either way; it is only reported by the wrong layer.
    pub fn cuts_attempts_short(&self, ceiling: Duration) -> bool {
        self.attempt < ceiling + SUBMIT_CEILING_MARGIN
    }
}

impl MapiLiteConfig {
    /// A section naming only `base_url`, with every other field at its default.
    #[cfg(test)]
    pub fn for_base_url(base_url: impl Into<String>) -> Self {
        MapiLiteConfig {
            base_url: base_url.into(),
            auth_token: None,
            total_timeout_seconds: default_mapi_lite_total_timeout_seconds(),
            miner_id_public_keys: Vec::new(),
        }
    }

    /// The `Authorization` header every request to mapi-lite carries, when
    /// a token is configured.
    pub fn authorization(&self) -> Option<String> {
        self.auth_token
            .as_deref()
            .map(|token| format!("Bearer {token}"))
    }

    /// The configured base URL, normalised: surrounding whitespace removed and
    /// any trailing `/` stripped, so `uls-client` always joins its paths onto
    /// a bare origin. Validation, the HTTP clients and the log lines all read
    /// the URL through here, so a padded or slash-terminated value in the TOML
    /// cannot validate as one thing and be requested as another.
    pub fn base_url(&self) -> &str {
        self.base_url.trim().trim_end_matches('/')
    }

    pub fn total_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.total_timeout_seconds)
    }

    /// The submit budget against mapi-lite's published submit `ceiling`, or
    /// [`ASSUMED_SUBMIT_CEILING`] until it has published one.
    pub fn submit_budget(&self, ceiling: Option<Duration>) -> SubmitBudget {
        SubmitBudget::new(
            ceiling.unwrap_or(ASSUMED_SUBMIT_CEILING),
            self.total_timeout(),
        )
    }

    /// Why the deadline cuts every attempt short against mapi-lite's
    /// `ceiling`, or `None` when an attempt can wait for its answer.
    pub fn deadline_warning(&self, ceiling: Option<Duration>) -> Option<String> {
        let ceiling = ceiling.unwrap_or(ASSUMED_SUBMIT_CEILING);
        let budget = self.submit_budget(Some(ceiling));
        budget.cuts_attempts_short(ceiling).then(|| {
            format!(
                "mapi_lite.total_timeout_seconds is {}s, but mapi-lite answers a submit it cannot \
                 finish at {}s: every attempt is cut to {}s, so a hung node is reported as this \
                 service's deadline instead of mapi-lite's answer. Raise it to at least {}s",
                self.total_timeout_seconds,
                ceiling.as_secs(),
                budget.attempt.as_secs(),
                (ceiling + SUBMIT_CEILING_MARGIN).as_secs()
            )
        })
    }

    /// Reject a section the broadcaster cannot work with, at startup rather
    /// than on the first funding request.
    pub fn validate(&self) -> Result<(), String> {
        let base_url = self.base_url();
        if base_url.is_empty() {
            return Err("mapi_lite.base_url is required when [mapi_lite] is present".to_string());
        }
        if !(base_url.starts_with("http://") || base_url.starts_with("https://")) {
            return Err(format!(
                "mapi_lite.base_url must start with http:// or https://, got '{base_url}'"
            ));
        }
        if self.total_timeout_seconds == 0 {
            return Err("mapi_lite.total_timeout_seconds must be greater than zero".to_string());
        }
        for key in &self.miner_id_public_keys {
            if !is_public_key_hex(key) {
                return Err(format!(
                    "mapi_lite.miner_id_public_keys: {key:?} is not a public key in hex \
                     (33 bytes starting 02 or 03, or 65 starting 04)"
                ));
            }
        }
        Ok(())
    }

    /// The startup warning for a section that names no miner ID (CS-523).
    /// On mainnet and testnet it is an error instead: see
    /// [`Config::check_miner_id_pin`].
    pub fn unpinned_warning(&self) -> Option<&'static str> {
        self.miner_id_public_keys.is_empty().then_some(UNPINNED)
    }
}

/// A list of strings, as a file writes one, or as one comma-separated string,
/// which is all an `FS__` override can give (CS-636): environment variables
/// are text. Each entry is trimmed and empty ones dropped, so `""` is an empty
/// list. Only the settings that ask for it take a string this way.
fn list_or_comma_separated<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ListOrText {
        List(Vec<String>),
        Text(String),
    }
    Ok(match ListOrText::deserialize(deserializer)? {
        ListOrText::List(list) => list,
        ListOrText::Text(text) => text
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(str::to_string)
            .collect(),
    })
}

/// Why an empty `miner_id_public_keys` is warned about, or refused.
const UNPINNED: &str = "mapi_lite.miner_id_public_keys is empty: responses from mapi-lite are \
     signed, but by a key nothing checks, so anyone able to answer in its place can forge an \
     'accepted' or a fee quote. Set it to mapi-lite's minerId.";

/// A secp256k1 public key in hex: compressed (33 bytes, `02`/`03`) or
/// uncompressed (65 bytes, `04`). The shape only; whether it is on the curve is
/// for the signature check to find out.
fn is_public_key_hex(key: &str) -> bool {
    let key = key.trim();
    let hex_ok = key.chars().all(|c| c.is_ascii_hexdigit());
    hex_ok
        && match key.len() {
            66 => key.starts_with("02") || key.starts_with("03"),
            130 => key.starts_with("04"),
            _ => false,
        }
}

#[derive(Debug, Default, Deserialize, Clone)]
pub struct DynamicConfigConfig {
    pub filename: String,
    /// Whether `POST /client` may store a raw `wif` (CS-526). Off by default:
    /// the dynamic config file would then hold the key itself, a copy that
    /// outlives the request on a volume often bind-mounted from the host.
    /// Off, only `wif_env` is accepted, which stores an `env:VAR` reference.
    #[serde(default)]
    pub allow_plaintext_wif: bool,
    /// The prefix every `wif_env` named on `POST /client` must start with
    /// (CS-521, SR-SEC-018). Default [`DEFAULT_WIF_ENV_PREFIX`].
    ///
    /// Without it, `wif_env` could name any variable in the process
    /// environment -- another client's key variable among them -- and
    /// whoever could add a client could fund from that wallet under a key of
    /// their own. With it, only variables the operator has set aside for
    /// runtime clients can be named. It may not overlap `FS__`, the
    /// configuration overrides, which carry secrets (CS-624).
    #[serde(default = "default_wif_env_prefix")]
    pub wif_env_prefix: String,
}

/// See [`DynamicConfigConfig::wif_env_prefix`].
pub const DEFAULT_WIF_ENV_PREFIX: &str = "FS_RUNTIME_WIF_";

fn default_wif_env_prefix() -> String {
    DEFAULT_WIF_ENV_PREFIX.to_string()
}

impl DynamicConfigConfig {
    /// The prefix must be a usable variable-name start that no configuration
    /// override (`FS__...`) can begin with.
    pub fn validate(&self) -> Result<(), String> {
        let prefix = &self.wif_env_prefix;
        crate::secrets::validate_env_var_name(prefix)
            .map_err(|e| format!("dynamic_config.wif_env_prefix: {e}"))?;
        if prefix.starts_with(ENV_OVERRIDE_PREFIX)
            || ENV_OVERRIDE_PREFIX.starts_with(prefix.as_str())
        {
            return Err(format!(
                "dynamic_config.wif_env_prefix {prefix:?} overlaps {ENV_OVERRIDE_PREFIX:?}, the \
                 prefix of the configuration overrides, which carry secrets"
            ));
        }
        Ok(())
    }
}

/// Per-IP HTTP rate limiting configuration.
#[derive(Debug, Deserialize, Clone)]
pub struct RateLimitConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_requests_per_second")]
    pub requests_per_second: u64,
    #[serde(default)]
    pub burst_size: Option<u32>,
    /// Addresses of reverse proxies in front of this service (CS-530).
    ///
    /// A request arriving from one of them is limited by the client address
    /// it reports in `X-Forwarded-For` -- the right-most entry that is not
    /// itself a trusted proxy -- rather than by the proxy's own address, which
    /// would put every caller in one bucket. From any other peer the header is
    /// ignored, since anyone can send it.
    #[serde(default)]
    pub trusted_proxies: Vec<std::net::IpAddr>,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        RateLimitConfig {
            enabled: false,
            requests_per_second: default_requests_per_second(),
            burst_size: None,
            trusted_proxies: Vec::new(),
        }
    }
}

fn default_requests_per_second() -> u64 {
    10
}

impl RateLimitConfig {
    pub fn effective_burst_size(&self) -> u32 {
        self.burst_size
            .unwrap_or(self.requests_per_second.min(u32::MAX as u64) as u32)
            .max(1)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.enabled && self.requests_per_second == 0 {
            return Err(
                "web_interface.rate_limit.requests_per_second must be greater than 0 when rate limiting is enabled".to_string(),
            );
        }
        Ok(())
    }
}

/// Web Interface Configuration. Every key has a default, so the section may
/// be left out.
#[derive(Debug, Default, Deserialize, Clone)]
pub struct WebInterfaceConfig {
    /// When set, required for `POST /client`.
    #[serde(default)]
    pub admin_api_key: Option<String>,
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    /// Serve mainnet or testnet without `admin_api_key`, or with a client that
    /// has no `api_key` (CS-514). Off, such a configuration refuses to start:
    /// the routes it leaves open spend real or shared coins. On, it starts
    /// with a warning; set it only where something in front of this service
    /// authenticates callers.
    #[serde(default)]
    pub allow_unauthenticated: bool,
}

/// Service Configuration
#[derive(Debug, Default, Deserialize, Clone)]
pub struct Config {
    /// Where the REST API listens, `host:port` (CS-624), as `listen_addr` is
    /// in Event and mapi-lite. Default [`DEFAULT_LISTEN_ADDR`], loopback: the
    /// funding routes are not exposed until an operator chooses to. The
    /// Docker image sets `FS__LISTEN_ADDR=0.0.0.0:8080`, since a container's
    /// own loopback cannot be reached from outside it.
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    pub blockchain_interface: BlockchainInterfaceConfig,
    #[serde(default)]
    pub web_interface: WebInterfaceConfig,
    #[serde(default)]
    pub telemetry: TelemetryConfig,
    pub service: ServiceConfig,
    pub client: Option<Vec<ClientConfig>>,
    pub dynamic_config: DynamicConfigConfig,
    #[serde(default)]
    pub idempotency: IdempotencyConfig,
    /// Present => funding transactions are broadcast via mapi-lite.
    #[serde(default)]
    pub mapi_lite: Option<MapiLiteConfig>,
    #[serde(default)]
    pub fees: FeesConfig,
    /// Per-client caps on what `POST /fund` pays out (CS-530). Off unless set.
    #[serde(default)]
    pub funding_limits: crate::funding_limits::FundingLimitsConfig,
}

impl Config {
    /// The file that carries in-flight funding state across a restart.
    ///
    /// Beside `dynamic_config.filename` unless `service.inflight_state_file`
    /// says otherwise, and named after it: `./data/dynamic.toml` pairs with
    /// `./data/dynamic.inflight.json`. Beside it because that file already has
    /// to survive a restart -- it holds the clients added at runtime -- so a
    /// deployment that keeps one keeps both, with no new volume to configure.
    /// Named after it so two services sharing a directory do not share state.
    pub fn inflight_state_path(&self) -> std::path::PathBuf {
        match &self.service.inflight_state_file {
            Some(path) => std::path::PathBuf::from(path),
            None => {
                std::path::Path::new(&self.dynamic_config.filename).with_extension("inflight.json")
            }
        }
    }
}

impl ClientConfig {
    /// Resolve `env:VAR` references and client-specific environment overrides.
    pub fn resolve_secrets(self) -> Result<Self, String> {
        resolve_client_config(self)
    }
}

impl Config {
    /// Whether this is a network whose coins are worth protecting: mainnet,
    /// or testnet, whose wallets are shared and slow to refill (CS-514).
    pub fn is_production_network(&self) -> bool {
        matches!(
            self.blockchain_interface.network.as_str(),
            "mainnet" | "testnet"
        )
    }

    /// The part of the authentication policy the configuration file alone
    /// decides (CS-514). The keys are checked once environment overrides are
    /// applied and the clients loaded: see `Service::check_auth_policy`.
    pub fn check_auth_policy(&self) -> Result<(), String> {
        // Never on a production network, opt-out or not: a raw key would cross
        // the network in a request body, and sit in the dynamic config file.
        if self.is_production_network() && self.dynamic_config.allow_plaintext_wif {
            return Err(format!(
                "dynamic_config.allow_plaintext_wif is for development only and cannot be set on \
                 {}: POST /client must use wif_env",
                self.blockchain_interface.network
            ));
        }
        Ok(())
    }

    /// The longest a single-transaction `POST /fund` can take, published in
    /// `GET /status` so a caller can give up later than this service does
    /// (CS-624): a chain read already running for the client, which it
    /// queues behind, then its own; the broadcast; and the chain read after a
    /// commit is refused. Chain reads are bounded by
    /// `blockchain_interface.timeout_seconds`, the broadcast by
    /// `mapi_lite.total_timeout_seconds`, or the same per-call bound without
    /// `[mapi_lite]`. `None` when chain reads have no deadline, so nothing
    /// bounds it.
    ///
    /// Requests queued behind a failing chain read each wait their turn, so
    /// during an outage a request can take longer; and `multiple_tx` repeats
    /// the broadcast and its reads per transaction.
    pub fn fund_worst_case(&self) -> Option<Duration> {
        let read = self.blockchain_interface.call_timeout()?;
        let broadcast = match &self.mapi_lite {
            Some(mapi_lite) => mapi_lite.total_timeout(),
            None => read,
        };
        Some(read * 3 + broadcast)
    }

    /// On mainnet and testnet, `[mapi_lite]` must name the keys its answers
    /// are signed with (CS-624). Unpinned, a forged "accepted" spends real or
    /// shared coins on a transaction that never left; elsewhere it is still
    /// allowed, with a warning.
    pub fn check_miner_id_pin(&self) -> Result<(), String> {
        match &self.mapi_lite {
            Some(mapi_lite) if self.is_production_network() => match mapi_lite.unpinned_warning() {
                Some(why) => Err(format!(
                    "{why} It is required on {}.",
                    self.blockchain_interface.network
                )),
                None => Ok(()),
            },
            _ => Ok(()),
        }
    }

    /// Return the configured network as Network type
    /// Refuse a chain-read backend this configuration cannot serve (CS-601).
    ///
    /// `mapi_lite` reads through mapi-lite's `[mapi_lite]` server, so it needs
    /// that section, and mapi-lite answers address reads from WhatsOnChain,
    /// which indexes mainnet and testnet only.
    pub fn check_chain_reads(&self) -> Result<(), String> {
        if self.blockchain_interface.interface_type != "mapi_lite" {
            return Ok(());
        }
        if self.mapi_lite.is_none() {
            return Err(
                "blockchain_interface.interface_type = \"mapi_lite\" reads through the \
                 [mapi_lite] server, so it needs a [mapi_lite] section"
                    .to_string(),
            );
        }
        match self.blockchain_interface.network.as_str() {
            "mainnet" | "testnet" => Ok(()),
            other => Err(format!(
                "blockchain_interface.interface_type = \"mapi_lite\" cannot read {other}: \
                 mapi-lite answers address reads from WhatsOnChain, which indexes mainnet and \
                 testnet only. Use interface_type = \"rpc\" for a private chain"
            )),
        }
    }

    /// The startup warning for a deployment with two WhatsOnChain clients
    /// (CS-635), or `None`. With `[mapi_lite]` and `interface_type = "woc"` on
    /// mainnet or testnet, this service reads WhatsOnChain itself, and so does
    /// mapi-lite when its `[woc]` is on: each paces for WhatsOnChain's free 3
    /// requests a second, so together they can exceed it. Not refused, since
    /// this service cannot tell whether mapi-lite's `[woc]` is on.
    pub fn shared_woc_budget_warning(&self) -> Option<String> {
        let shared = self.mapi_lite.is_some()
            && self.blockchain_interface.interface_type == "woc"
            && self.is_production_network();
        shared.then(|| {
            format!(
                "blockchain_interface.interface_type = \"woc\" with [mapi_lite] on {}: this \
                 service reads WhatsOnChain itself, and so does mapi-lite when its [woc] is on, \
                 each pacing for WhatsOnChain's free 3 requests a second, which the two can \
                 exceed between them (HTTP 429). Set interface_type = \"mapi_lite\" so mapi-lite \
                 makes every WhatsOnChain request; keep \"woc\" only if mapi-lite runs without \
                 [woc].",
                self.blockchain_interface.network
            )
        })
    }

    pub fn get_network(&self) -> Result<Network, &str> {
        match self.blockchain_interface.network.as_str() {
            "mainnet" => Ok(Network::BSV_Mainnet),
            "testnet" => Ok(Network::BSV_Testnet),
            "stn" => Ok(Network::BSV_STN),
            "regtest" => Ok(Network::BSV_Regtest),
            _ => Err("unable to decode network"),
        }
    }

    /// Resolve `env:VAR_NAME` references in the secret fields.
    ///
    /// The named overrides that used to take precedence here --
    /// `FS_ADMIN_API_KEY`, `FS_RPC_USER`, `FS_RPC_PASSWORD`,
    /// `FS_MAPI_LITE_AUTH_TOKEN` and each client's `FS_CLIENT_<ID>_WIF` and
    /// `_API_KEY` -- are gone (CS-624). Any setting can be overridden with
    /// `FS__<TABLE>__<KEY>`, applied when the file is read; a client's keys,
    /// which live in an array, are `env:` references in the file.
    pub fn resolve_secrets(mut self) -> Result<Self, String> {
        let resolve = |value: Option<String>| -> Result<Option<String>, String> {
            value.map(|v| resolve_secret(&v)).transpose()
        };
        self.web_interface.admin_api_key = resolve(self.web_interface.admin_api_key.take())?;
        self.blockchain_interface.rpc_user = resolve(self.blockchain_interface.rpc_user.take())?;
        self.blockchain_interface.rpc_password =
            resolve(self.blockchain_interface.rpc_password.take())?;
        if let Some(mapi_lite) = self.mapi_lite.as_mut() {
            mapi_lite.auth_token = resolve(mapi_lite.auth_token.take())?;
            // An empty token and no token mean the same thing: send none.
            if mapi_lite.auth_token.as_deref() == Some("") {
                mapi_lite.auth_token = None;
            }
            // Checked once resolved, where the value an env: reference or
            // override supplied can be seen. The message never repeats it.
            if mapi_lite
                .auth_token
                .as_deref()
                .is_some_and(|token| token.contains(char::is_whitespace))
            {
                return Err(MAPI_LITE_TOKEN_SHAPE.to_string());
            }
        }
        if let Some(clients) = self.client.as_mut() {
            let resolved = std::mem::take(clients)
                .into_iter()
                .map(resolve_client_config)
                .collect::<Result<Vec<_>, _>>()?;
            *clients = resolved;
        }
        Ok(self)
    }
}

fn resolve_client_config(mut client: ClientConfig) -> Result<ClientConfig, String> {
    client.wif_key = resolve_secret(&client.wif_key)?;
    client.api_key = client.api_key.map(|key| resolve_secret(&key)).transpose()?;
    Ok(client)
}

/// Why a `mapi_lite.auth_token` holding a scheme, or any space, is refused.
const MAPI_LITE_TOKEN_SHAPE: &str = "mapi_lite.auth_token holds the token alone, which is sent as \
     `Authorization: Bearer <token>`: drop the \"Bearer \" prefix and any spaces (CS-624)";

/// What every configuration override's name starts with (CS-624):
/// `FS__WEB_INTERFACE__ADMIN_API_KEY` sets `web_interface.admin_api_key`.
pub const ENV_OVERRIDE_PREFIX: &str = "FS__";

/// The variable naming the configuration file, as `TE_CONFIG` and
/// `MAPI_CONFIG` do for the other two services (CS-624).
pub const CONFIG_FILE_ENV: &str = "FS_CONFIG";

/// The configuration file when [`CONFIG_FILE_ENV`] is unset.
pub const DEFAULT_CONFIG_FILE: &str = "data/financing-service.toml";

/// The configuration file to read: [`CONFIG_FILE_ENV`], else
/// [`DEFAULT_CONFIG_FILE`].
pub fn config_file() -> String {
    env::var(CONFIG_FILE_ENV)
        .ok()
        .filter(|path| !path.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_CONFIG_FILE.to_string())
}

/// Where the REST API listens when `listen_addr` is not set.
pub const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:8080";

fn default_listen_addr() -> String {
    DEFAULT_LISTEN_ADDR.to_string()
}

impl Config {
    /// `listen_addr` as a socket address, or why it is not one.
    pub fn listen_socket_addr(&self) -> Result<SocketAddr, String> {
        self.listen_addr.parse().map_err(|e| {
            format!(
                "listen_addr {:?} is not host:port, e.g. \"127.0.0.1:8080\": {e}",
                self.listen_addr
            )
        })
    }
}

/// Read, validate and resolve the configuration in `path` (CS-624): the TOML
/// file, then `FS__` environment overrides, then `env:` references. A key no
/// setting reads -- in the file or an override -- is refused, naming it.
pub fn load_config(path: &str) -> Result<Config, String> {
    let config = get_config(path)?;
    config.listen_socket_addr()?;
    config.web_interface.rate_limit.validate()?;
    config.telemetry.validate()?;
    config.idempotency.validate()?;
    config.blockchain_interface.validate()?;
    config.fees.validate()?;
    config.dynamic_config.validate()?;
    config.funding_limits.validate()?;
    config.check_auth_policy()?;
    config.check_chain_reads()?;
    if let Some(mapi_lite) = &config.mapi_lite {
        mapi_lite.validate()?;
    }
    config.check_miner_id_pin()?;
    config.resolve_secrets()
}

/// The configuration in `path` alone, without `FS__` overrides: what the
/// plaintext-secret warning reads, since a secret an override supplies is in
/// the environment already, which is what the warning asks for. `main` warns
/// once logging is up; a warning issued while loading would go nowhere.
pub fn file_only(path: &str) -> Result<Config, String> {
    read_layers(path, false)
}

/// The configuration in `path` with `FS__` overrides applied, before
/// validation and secret resolution.
pub fn get_config(path: &str) -> Result<Config, String> {
    read_layers(path, true)
}

/// The file, and the `FS__` environment when `with_env`, as one [`Config`].
fn read_layers(path: &str, with_env: bool) -> Result<Config, String> {
    debug!("read_config = {}", path);
    let mut builder = config::Config::builder()
        .add_source(config::File::new(path, config::FileFormat::Toml).required(true));
    if with_env {
        builder = builder.add_source(
            // Values stay text until a setting reads them, which then
            // parses "8080" or "true" as its own type. `try_parsing` would
            // parse first, so a key of "007" would arrive as "7".
            config::Environment::with_prefix("FS")
                .prefix_separator("__")
                .separator("__"),
        );
    }
    let built = builder
        .build()
        .map_err(|e| format!("Error reading config file {path}: {e}"))?;
    let mut unused = Vec::new();
    let parsed: Result<Config, _> = serde_ignored::deserialize(built, |key| {
        // serde_ignored writes an Option's inside as a `?` segment.
        unused.push(key.to_string().replace(".?", ""))
    });
    unused.sort();
    let unread = |keys: &[String]| {
        format!(
            "configuration keys no setting reads: {}. Check the spelling: a key belongs in the \
             [table] it is written under, and an override is {ENV_OVERRIDE_PREFIX}<TABLE>__<KEY>",
            keys.join(", ")
        )
    };
    match parsed {
        // A key renamed or misspelt is usually why a required one is
        // missing: `network_type` for `network`. Name both.
        Err(e) if !unused.is_empty() => Err(format!(
            "Error in config file {path}: {e}; and {}",
            unread(&unused)
        )),
        Err(e) => Err(format!("Error in config file {path}: {e}")),
        Ok(_) if !unused.is_empty() => Err(unread(&unused)),
        Ok(config) => Ok(config),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chain_gang::network::Network;

    use crate::test_support::env_lock;

    #[test]
    fn resolve_secrets_replaces_env_references() {
        let _env = env_lock();
        unsafe { env::set_var("FS_TEST_CONFIG_WIF", "test-wif-value") };
        unsafe { env::set_var("FS_TEST_CONFIG_API_KEY", "test-api-key") };
        unsafe { env::set_var("FS_TEST_CONFIG_ADMIN", "admin-from-env") };

        let config = Config {
            web_interface: WebInterfaceConfig {
                admin_api_key: Some("env:FS_TEST_CONFIG_ADMIN".to_string()),
                rate_limit: RateLimitConfig::default(),
                allow_unauthenticated: false,
            },
            client: Some(vec![ClientConfig {
                client_id: "id1".to_string(),
                wif_key: "env:FS_TEST_CONFIG_WIF".to_string(),
                api_key: Some("env:FS_TEST_CONFIG_API_KEY".to_string()),
            }]),
            ..Default::default()
        };

        let resolved = config.resolve_secrets().unwrap();
        assert_eq!(
            resolved.web_interface.admin_api_key.as_deref(),
            Some("admin-from-env")
        );
        let client = resolved.client.as_ref().unwrap().first().unwrap();
        assert_eq!(client.wif_key, "test-wif-value");
        assert_eq!(client.api_key.as_deref(), Some("test-api-key"));
    }

    #[test]
    fn rate_limit_config_rejects_zero_requests_per_second_when_enabled() {
        let config = RateLimitConfig {
            enabled: true,
            requests_per_second: 0,
            burst_size: None,
            trusted_proxies: Vec::new(),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn sr_cfg_001_load_config_reads_toml_file() {
        let _env = env_lock();
        let dir = std::env::temp_dir().join(format!(
            "financing-service-cfg-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            r#"
listen_addr = "127.0.0.1:9091"

[blockchain_interface]
interface_type = "test"
network = "testnet"

[web_interface]
allow_unauthenticated = true  # testnet without keys (CS-514)

[service]
utxo_refresh_period = 45

[dynamic_config]
filename = "./data/dynamic.toml"
"#,
        )
        .unwrap();

        let config = load_config(path.to_str().unwrap()).unwrap();
        assert_eq!(config.listen_addr, "127.0.0.1:9091");
        assert_eq!(config.service.utxo_refresh_period, 45);
    }

    /// `FS_CONFIG` names the file, as `TE_CONFIG` and `MAPI_CONFIG` do; it
    /// no longer holds the configuration itself (CS-624).
    #[test]
    fn sr_cfg_002_fs_config_names_the_file() {
        let _env = env_lock();
        unsafe { env::remove_var(CONFIG_FILE_ENV) };
        assert_eq!(config_file(), DEFAULT_CONFIG_FILE);
        let path = write_temp_config("named", MINIMAL_TOML);
        unsafe { env::set_var(CONFIG_FILE_ENV, &path) };
        assert_eq!(config_file(), path.to_str().unwrap());
        let config = load_config(&config_file()).unwrap();
        assert_eq!(config.listen_addr, DEFAULT_LISTEN_ADDR);
        unsafe { env::remove_var(CONFIG_FILE_ENV) };
    }

    /// Runs `body` with `vars` set, removing them after, under the env lock.
    fn with_env<T>(vars: &[(&str, &str)], body: impl FnOnce() -> T) -> T {
        let _env = env_lock();
        for (name, value) in vars {
            unsafe { env::set_var(name, value) };
        }
        let result = body();
        for (name, _) in vars {
            unsafe { env::remove_var(name) };
        }
        result
    }

    /// `FS__<TABLE>__<KEY>` overrides any setting the file holds, or adds
    /// one it does not, with its value taken as the setting's type (CS-624).
    #[test]
    fn sr_cfg_002_fs_overrides_any_setting() {
        let path = write_temp_config("override", MINIMAL_TOML);
        let config = with_env(
            &[
                ("FS__LISTEN_ADDR", "0.0.0.0:9093"),
                ("FS__WEB_INTERFACE__ADMIN_API_KEY", "admin-from-env"),
                ("FS__SERVICE__UTXO_REFRESH_PERIOD", "15"),
            ],
            || load_config(path.to_str().unwrap()).unwrap(),
        );
        assert_eq!(config.listen_addr, "0.0.0.0:9093");
        assert_eq!(
            config.web_interface.admin_api_key.as_deref(),
            Some("admin-from-env")
        );
        assert_eq!(config.service.utxo_refresh_period, 15);
    }

    /// A secret set by override is taken as written: a key of digits keeps
    /// its leading zeros and one that reads as a number stays text.
    #[test]
    fn sr_sec_008_an_override_secret_is_taken_verbatim() {
        let path = write_temp_config("verbatim", MINIMAL_TOML);
        for secret in ["007", "1e5", "true", "0x10"] {
            let config = with_env(&[("FS__WEB_INTERFACE__ADMIN_API_KEY", secret)], || {
                load_config(path.to_str().unwrap()).unwrap()
            });
            assert_eq!(
                config.web_interface.admin_api_key.as_deref(),
                Some(secret),
                "{secret:?}"
            );
        }
    }

    /// A key no setting reads -- misspelt, or under the wrong table -- is
    /// refused at startup with its name, whether the file or an override
    /// holds it, rather than silently leaving the setting at its default.
    #[test]
    fn sr_cfg_002_a_key_no_setting_reads_is_refused() {
        let path = write_temp_config(
            "unknown",
            &MINIMAL_TOML.replace("[web_interface]", "[web_interface]\nprot = 8081"),
        );
        let err = with_env(&[], || load_config(path.to_str().unwrap()).unwrap_err());
        assert!(err.contains("web_interface.prot"), "{err}");

        let path = write_temp_config("unknown-env", MINIMAL_TOML);
        let err = with_env(&[("FS__FEES__SATOSHIS_PER_KBYTE", "100")], || {
            load_config(path.to_str().unwrap()).unwrap_err()
        });
        assert!(err.contains("fees.satoshis_per_kbyte"), "{err}");
        assert!(err.contains("FS__<TABLE>__<KEY>"), "{err}");
    }

    /// The overrides that preceded `FS__` are gone, so a deployment still
    /// setting one learns nothing changed only by reading the docs: they no
    /// longer take effect (CS-624).
    #[test]
    fn sr_sec_008_the_retired_named_overrides_are_not_read() {
        let path = write_temp_config(
            "retired",
            &format!(
                "{MINIMAL_TOML}\n[[client]]\nclient_id = \"id1\"\nwif_key = \"in-file-wif\"\n"
            ),
        );
        let config = with_env(
            &[
                ("FS_ADMIN_API_KEY", "retired"),
                ("FS_CLIENT_ID1_WIF", "retired"),
                ("FS_CLIENT_ID1_API_KEY", "retired"),
            ],
            || {
                get_config(path.to_str().unwrap())
                    .unwrap()
                    .resolve_secrets()
                    .unwrap()
            },
        );
        assert_eq!(config.web_interface.admin_api_key, None);
        let client = &config.client.unwrap()[0];
        assert_eq!(client.wif_key, "in-file-wif");
        assert_eq!(client.api_key, None);
    }

    /// The plaintext-secret warning reads the file alone: a secret an
    /// override supplies is already where the warning asks for it.
    #[test]
    fn sr_sec_008_the_file_layer_excludes_overrides() {
        let path = write_temp_config("file-layer", MINIMAL_TOML);
        let (file_only, merged) = with_env(&[("FS__WEB_INTERFACE__ADMIN_API_KEY", "k")], || {
            (
                read_layers(path.to_str().unwrap(), false).unwrap(),
                read_layers(path.to_str().unwrap(), true).unwrap(),
            )
        });
        assert_eq!(file_only.web_interface.admin_api_key, None);
        assert_eq!(merged.web_interface.admin_api_key.as_deref(), Some("k"));
    }

    /// The REST API listens on loopback unless told otherwise; the address
    /// is checked at startup, naming the setting (CS-624).
    #[test]
    fn sr_cfg_003_listen_addr_defaults_to_loopback_and_is_checked() {
        let path = write_temp_config("listen", MINIMAL_TOML);
        let config = with_env(&[], || load_config(path.to_str().unwrap()).unwrap());
        let addr = config.listen_socket_addr().unwrap();
        assert!(addr.ip().is_loopback(), "{addr}");
        assert_eq!(addr.port(), 8080);

        let err = with_env(&[("FS__LISTEN_ADDR", "0.0.0.0")], || {
            load_config(path.to_str().unwrap()).unwrap_err()
        });
        assert!(err.contains("listen_addr"), "{err}");
    }

    /// The settings `listen_addr` replaced are refused, not ignored, so a
    /// file written for an earlier version cannot quietly listen elsewhere.
    #[test]
    fn sr_cfg_003_the_retired_address_and_port_are_refused() {
        let path = write_temp_config(
            "retired-address",
            &MINIMAL_TOML.replace(
                "[web_interface]",
                "[web_interface]\naddress = \"127.0.0.1\"\nport = 8080",
            ),
        );
        let err = with_env(&[], || load_config(path.to_str().unwrap()).unwrap_err());
        assert!(err.contains("web_interface.address"), "{err}");
        assert!(err.contains("web_interface.port"), "{err}");
    }

    /// The level is `RUST_LOG`'s, as in Event and mapi-lite; a `[logging]`
    /// section is refused rather than silently ignored (CS-624).
    #[test]
    fn sr_cfg_006_a_logging_section_is_refused() {
        let path = write_temp_config(
            "logging",
            &format!("{MINIMAL_TOML}\n[logging]\nlevel = \"debug\"\n"),
        );
        let err = with_env(&[], || load_config(path.to_str().unwrap()).unwrap_err());
        assert!(err.contains("no setting reads: logging"), "{err}");
    }

    #[test]
    fn sr_bchn_002_get_network_supports_mainnet_testnet_stn_and_regtest() {
        for (network, expected) in [
            ("mainnet", Network::BSV_Mainnet),
            ("testnet", Network::BSV_Testnet),
            ("stn", Network::BSV_STN),
            ("regtest", Network::BSV_Regtest),
        ] {
            let config = Config {
                blockchain_interface: BlockchainInterfaceConfig {
                    network: network.to_string(),
                    ..Default::default()
                },
                ..Default::default()
            };
            assert_eq!(config.get_network().unwrap(), expected);
        }
    }

    /// Only the four names mapi-lite publishes are accepted, spelt exactly;
    /// one meaning the same network is refused naming the spelling to write,
    /// and the old key is refused rather than ignored (CS-624).
    #[test]
    fn sr_bchn_002_the_network_is_one_of_four_exact_names() {
        for network in NETWORKS {
            assert!(check_network(network).is_ok(), "{network}");
        }
        for (network, write) in [
            ("main", "\"mainnet\""),
            ("livenet", "\"mainnet\""),
            ("Mainnet", "\"mainnet\""),
            ("test", "\"testnet\""),
            ("Testnet", "\"testnet\""),
            ("REGTEST", "\"regtest\""),
        ] {
            let err = check_network(network).unwrap_err();
            assert!(err.contains(write), "{network}: {err}");
        }
        let err = check_network("private").unwrap_err();
        assert!(err.contains("mainnet, testnet, regtest, stn"), "{err}");

        let path = write_temp_config(
            "network-type",
            &MINIMAL_TOML.replace("network = \"testnet\"", "network_type = \"testnet\""),
        );
        let err = with_env(&[], || load_config(path.to_str().unwrap()).unwrap_err());
        assert!(err.contains("blockchain_interface.network_type"), "{err}");
    }

    fn rpc_interface_config(
        url: Option<&str>,
        user: Option<&str>,
        password: Option<&str>,
    ) -> BlockchainInterfaceConfig {
        BlockchainInterfaceConfig {
            interface_type: "rpc".to_string(),
            network: "regtest".to_string(),
            url: url.map(str::to_string),
            rpc_user: user.map(str::to_string),
            rpc_password: password.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn sr_bchn_005_rpc_config_requires_address_and_credentials() {
        assert!(
            rpc_interface_config(Some("127.0.0.1:18443"), Some("u"), Some("p"))
                .validate()
                .is_ok()
        );
        for (url, user, password, expected) in [
            (None, Some("u"), Some("p"), "url"),
            (Some(""), Some("u"), Some("p"), "url"),
            (Some("127.0.0.1:18443"), None, Some("p"), "rpc_user"),
            (Some("127.0.0.1:18443"), Some("u"), None, "rpc_password"),
        ] {
            let error = rpc_interface_config(url, user, password)
                .validate()
                .expect_err("expected validation to fail");
            assert!(error.contains(expected), "should name {expected}: {error}");
        }
    }

    /// CS-635: reading WhatsOnChain here while broadcasting through mapi-lite
    /// on mainnet or testnet is warned about, naming the fix; reading through
    /// mapi-lite, no `[mapi_lite]`, or a private network is not.
    #[test]
    fn sr_bchn_022_two_whatsonchain_clients_are_warned_about() {
        let config = |interface_type: &str, network: &str, mapi_lite: bool| Config {
            blockchain_interface: BlockchainInterfaceConfig {
                interface_type: interface_type.to_string(),
                network: network.to_string(),
                ..Default::default()
            },
            mapi_lite: mapi_lite.then(|| MapiLiteConfig::for_base_url("http://mapi:8080")),
            ..Default::default()
        };
        for network in ["mainnet", "testnet"] {
            let warning = config("woc", network, true)
                .shared_woc_budget_warning()
                .expect(network);
            assert!(warning.contains(&format!("on {network}")), "{warning}");
            assert!(
                warning.contains("interface_type = \"mapi_lite\""),
                "{warning}"
            );
        }
        for (interface_type, network, mapi_lite) in [
            ("mapi_lite", "testnet", true),
            ("woc", "testnet", false),
            ("woc", "stn", true),
            ("rpc", "mainnet", true),
        ] {
            assert_eq!(
                config(interface_type, network, mapi_lite).shared_woc_budget_warning(),
                None,
                "{interface_type} on {network}, [mapi_lite] {mapi_lite}"
            );
        }
    }

    /// CS-601: reading through mapi-lite needs its server, and a network its
    /// WhatsOnChain-backed address reads can answer for.
    #[test]
    fn sr_bchn_019_mapi_lite_reads_need_the_section_and_a_public_network() {
        let reads = |network: &str, mapi_lite: bool| Config {
            blockchain_interface: BlockchainInterfaceConfig {
                interface_type: "mapi_lite".to_string(),
                network: network.to_string(),
                ..Default::default()
            },
            mapi_lite: mapi_lite.then(|| MapiLiteConfig::for_base_url("http://mapi:8080")),
            ..Default::default()
        };
        assert!(reads("testnet", true).check_chain_reads().is_ok());
        assert!(reads("mainnet", true).check_chain_reads().is_ok());

        let missing = reads("testnet", false).check_chain_reads().unwrap_err();
        assert!(missing.contains("[mapi_lite]"), "{missing}");
        for private in ["regtest", "stn"] {
            let refused = reads(private, true).check_chain_reads().unwrap_err();
            assert!(refused.contains("rpc"), "{refused}");
        }

        // Other backends are not this check's business.
        let woc = Config {
            blockchain_interface: BlockchainInterfaceConfig {
                interface_type: "woc".to_string(),
                network: "regtest".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(woc.check_chain_reads().is_ok());
    }

    /// Imports are on unless turned off, because the failure they prevent is
    /// silent.
    #[test]
    fn rpc_import_addresses_defaults_to_on() {
        let config: BlockchainInterfaceConfig = toml::from_str(
            "interface_type = \"rpc\"\nnetwork = \"regtest\"\nurl = \"127.0.0.1:18443\"",
        )
        .unwrap();
        assert!(config.rpc_import_addresses);
    }

    #[test]
    fn cs_502_the_interface_deadline_defaults_to_30s_and_0_turns_it_off() {
        let unset = BlockchainInterfaceConfig::default();
        assert_eq!(
            unset.call_timeout(),
            Some(std::time::Duration::from_secs(30))
        );
        let off = BlockchainInterfaceConfig {
            timeout_seconds: Some(0),
            ..Default::default()
        };
        assert_eq!(off.call_timeout(), None);
        let set = BlockchainInterfaceConfig {
            timeout_seconds: Some(7),
            ..Default::default()
        };
        assert_eq!(set.call_timeout(), Some(std::time::Duration::from_secs(7)));
    }

    #[test]
    fn woc_and_test_interfaces_need_no_url() {
        for interface_type in ["woc", "test"] {
            let config = BlockchainInterfaceConfig {
                interface_type: interface_type.to_string(),
                network: "testnet".to_string(),
                ..Default::default()
            };
            assert!(config.validate().is_ok());
        }
    }

    #[test]
    fn sr_sec_014_rpc_password_resolves_from_an_env_reference() {
        let _guard = crate::test_support::env_lock();
        unsafe { std::env::set_var("FS_TEST_RPC_PASSWORD", "from-env") };
        let config = Config {
            blockchain_interface: rpc_interface_config(
                Some("127.0.0.1:18443"),
                Some("rpcuser"),
                Some("env:FS_TEST_RPC_PASSWORD"),
            ),
            ..Default::default()
        };
        let resolved = config.resolve_secrets().unwrap();
        assert_eq!(
            resolved.blockchain_interface.rpc_password.as_deref(),
            Some("from-env")
        );
        unsafe { std::env::remove_var("FS_TEST_RPC_PASSWORD") };
    }

    #[test]
    fn sr_sec_014_plaintext_rpc_password_is_reported() {
        let config = Config {
            blockchain_interface: rpc_interface_config(
                Some("127.0.0.1:18443"),
                Some("rpcuser"),
                Some("plaintext-secret"),
            ),
            ..Default::default()
        };
        assert!(crate::secrets::plaintext_secret_fields(&config)
            .contains(&"blockchain_interface.rpc_password".to_string()));
    }

    #[test]
    fn sr_clnt_001_config_supports_multiple_static_clients() {
        let config = Config {
            client: Some(vec![
                ClientConfig {
                    client_id: "id1".to_string(),
                    wif_key: "wif1".to_string(),
                    api_key: None,
                },
                ClientConfig {
                    client_id: "id2".to_string(),
                    wif_key: "wif2".to_string(),
                    api_key: None,
                },
            ]),
            ..Default::default()
        };
        assert_eq!(config.client.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn sr_nfr_007_load_config_returns_error_for_invalid_file() {
        let _env = env_lock();
        let err = load_config("definitely-missing-config.toml").unwrap_err();
        assert!(err.contains("Error reading config file"));
    }

    #[test]
    fn sr_bchn_003_sample_config_sets_utxo_refresh_period() {
        let content = std::fs::read_to_string("data/financing-service.toml").unwrap();
        let config: Config = toml::from_str(&content).unwrap();
        assert_eq!(config.service.utxo_refresh_period, 60);
    }

    /// The shipped sample is read by the real loader, so a key it holds that
    /// no setting reads -- one a later change retires, say -- fails here
    /// rather than at an operator's first start.
    #[test]
    fn sr_cfg_002_the_sample_config_holds_only_keys_settings_read() {
        let config = with_env(&[], || get_config(DEFAULT_CONFIG_FILE).unwrap());
        assert_eq!(config.listen_addr, DEFAULT_LISTEN_ADDR);
    }

    #[test]
    fn sr_cfg_007_load_config_validates_telemetry_config() {
        let _env = env_lock();
        let dir = std::env::temp_dir().join(format!(
            "financing-service-telemetry-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            r#"
[blockchain_interface]
interface_type = "test"
network = "testnet"

[web_interface]
allow_unauthenticated = true  # testnet without keys (CS-514)

[telemetry]
enabled = true
otlp_endpoint = ""

[service]
utxo_refresh_period = 60

[dynamic_config]
filename = "./data/dynamic.toml"
"#,
        )
        .unwrap();

        let err = load_config(path.to_str().unwrap()).unwrap_err();
        assert!(err.contains("telemetry.otlp_endpoint"));
    }

    /// The two CS-674 fields as an operator writes them: a dotted attribute
    /// key stays one key, `FS__TELEMETRY__EXPORT_INTERVAL_SEC` overrides the
    /// interval, 0 is refused, and a misspelt key is still named.
    #[test]
    fn sr_tele_013_telemetry_reads_export_interval_and_resource_attributes() {
        let toml = format!(
            "{MINIMAL_TOML}\n[telemetry]\nexport_interval_sec = 10\n\
             resource_attributes = {{ \"deployment.environment.name\" = \"local\" }}\n"
        );
        let path = write_temp_config("telemetry-fields", &toml);
        let path = path.to_str().unwrap();

        let config = with_env(&[], || load_config(path).unwrap());
        assert_eq!(config.telemetry.export_interval_sec, Some(10));
        assert_eq!(
            config.telemetry.resource_attributes,
            BTreeMap::from([(
                "deployment.environment.name".to_string(),
                "local".to_string()
            )])
        );

        let config = with_env(&[("FS__TELEMETRY__EXPORT_INTERVAL_SEC", "30")], || {
            load_config(path).unwrap()
        });
        assert_eq!(config.telemetry.export_interval_sec, Some(30));

        let err = with_env(&[("FS__TELEMETRY__EXPORT_INTERVAL_SEC", "0")], || {
            load_config(path).unwrap_err()
        });
        assert!(err.contains("telemetry.export_interval_sec"), "{err}");

        let misspelt = write_temp_config(
            "telemetry-misspelt",
            &format!("{MINIMAL_TOML}\n[telemetry]\nexport_interval_secs = 10\n"),
        );
        let err = with_env(&[], || load_config(misspelt.to_str().unwrap()).unwrap_err());
        assert!(err.contains("telemetry.export_interval_secs"), "{err}");
    }

    /// The smallest config the service accepts, for the [mapi_lite] tests to
    /// build on.
    const MINIMAL_TOML: &str = r#"
[blockchain_interface]
interface_type = "test"
network = "testnet"

[web_interface]
allow_unauthenticated = true  # testnet without keys (CS-514)

[service]
utxo_refresh_period = 60

[dynamic_config]
filename = "./data/dynamic.toml"
"#;

    /// A config with no [fees] section is valid and takes the documented
    /// defaults, so CS-451 does not make the section mandatory.
    #[test]
    fn cs_451_the_fees_section_is_optional() {
        let config: Config = toml::from_str(MINIMAL_TOML).unwrap();
        assert_eq!(config.fees.satoshis_per_kb, DEFAULT_SATOSHIS_PER_KB);
        assert_eq!(config.fees.satoshis_per_kb, 100);
        assert!(config.fees.validate().is_ok());
    }

    /// Either field can be set on its own; the other keeps its default.
    #[test]
    fn cs_451_each_fee_field_defaults_independently() {
        let only_rate: Config =
            toml::from_str(&format!("{MINIMAL_TOML}\n[fees]\nsatoshis_per_kb = 250\n")).unwrap();
        assert_eq!(only_rate.fees.satoshis_per_kb, 250);
        assert_eq!(
            only_rate.fees.max_satoshis_per_kb,
            DEFAULT_MAX_SATOSHIS_PER_KB
        );

        let only_ceiling: Config = toml::from_str(&format!(
            "{MINIMAL_TOML}\n[fees]\nmax_satoshis_per_kb = 500\n"
        ))
        .unwrap();
        assert_eq!(only_ceiling.fees.satoshis_per_kb, DEFAULT_SATOSHIS_PER_KB);
        assert_eq!(only_ceiling.fees.max_satoshis_per_kb, 500);

        // Those are the only two. The dust threshold is derived from the rate
        // rather than configured, so there is no third key (CS-452).
    }

    /// With `[mapi_lite]` its fee quote always sets the rate; the switch that
    /// could turn it off is refused rather than ignored (CS-624).
    #[test]
    fn sr_fund_024_use_mapi_fee_quote_is_refused() {
        let path = write_temp_config(
            "fee-switch",
            &format!("{MINIMAL_TOML}\n[fees]\nuse_mapi_fee_quote = false\n"),
        );
        let err = with_env(&[], || load_config(path.to_str().unwrap()).unwrap_err());
        assert!(err.contains("fees.use_mapi_fee_quote"), "{err}");
    }

    /// A zero rate builds transactions nothing relays, and the service would
    /// only find that out at broadcast. It fails at startup instead, naming
    /// the field.
    #[test]
    fn cs_451_a_zero_rate_is_refused_at_startup() {
        let _env = env_lock();
        let path = write_temp_config(
            "fees-zero",
            &format!("{MINIMAL_TOML}\n[fees]\nsatoshis_per_kb = 0\n"),
        );
        let err = load_config(path.to_str().unwrap()).unwrap_err();
        assert!(err.contains("fees.satoshis_per_kb"), "{err}");
    }

    /// The state file sits beside the file of runtime-added clients, which a
    /// deployment already keeps across restarts, and is named after it so two
    /// services sharing a directory keep separate state (CS-465).
    #[test]
    fn cs_465_the_state_file_sits_beside_the_dynamic_config() {
        let mut config: Config = toml::from_str(MINIMAL_TOML).unwrap();
        assert_eq!(
            config.inflight_state_path(),
            std::path::PathBuf::from("./data/dynamic.inflight.json")
        );

        config.service.inflight_state_file = Some("/var/lib/fs/inflight.json".to_string());
        assert_eq!(
            config.inflight_state_path(),
            std::path::PathBuf::from("/var/lib/fs/inflight.json"),
            "an explicit path wins"
        );
    }

    fn write_temp_config(label: &str, content: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "financing-service-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        std::fs::write(&path, content).unwrap();
        path
    }

    /// The minimal config plus a `[mapi_lite]` section holding `fields`,
    /// pinned to a miner ID unless `fields` names its own: the minimal config
    /// is testnet, where an unpinned section is refused (CS-624).
    fn with_mapi_lite_section(fields: &str) -> String {
        let pin = if fields.contains("miner_id_public_keys") {
            String::new()
        } else {
            format!("miner_id_public_keys = [\"{PINNED_KEY}\"]\n")
        };
        format!("{MINIMAL_TOML}\n[mapi_lite]\n{fields}{pin}")
    }

    fn config_with_mapi_lite(auth_token: Option<&str>) -> Config {
        let mut mapi_lite = MapiLiteConfig::for_base_url("http://127.0.0.1:8080");
        mapi_lite.auth_token = auth_token.map(str::to_string);
        Config {
            mapi_lite: Some(mapi_lite),
            ..Default::default()
        }
    }

    fn plaintext_fields(auth_token: Option<&str>) -> Vec<String> {
        crate::secrets::plaintext_secret_fields(&config_with_mapi_lite(auth_token))
    }

    /// Leaving the section out is the WoC deployment, and must keep working
    /// with no change to existing config files.
    #[test]
    fn sr_cfg_008_config_without_a_mapi_lite_section_has_none() {
        let config: Config = toml::from_str(MINIMAL_TOML).unwrap();
        assert!(config.mapi_lite.is_none());
    }

    #[test]
    fn sr_cfg_008_mapi_lite_section_parses_with_defaults() {
        let content = with_mapi_lite_section("base_url = \"http://127.0.0.1:8080\"\n");
        let config: Config = toml::from_str(&content).unwrap();
        let mapi_lite = config.mapi_lite.expect("section present");
        assert_eq!(mapi_lite.base_url, "http://127.0.0.1:8080");
        assert_eq!(mapi_lite.auth_token, None);
        assert_eq!(mapi_lite.total_timeout_seconds, 45);
        assert!(mapi_lite.validate().is_ok());
    }

    /// Against mapi-lite's default 10s ceiling and the default 45s deadline,
    /// the budget the three settings it replaces used to give: 13s attempts,
    /// two retries (CS-624).
    #[test]
    fn sr_cfg_009_the_default_budget_is_13s_attempts_and_two_retries() {
        let config = MapiLiteConfig::for_base_url("http://127.0.0.1:8080");
        let budget = config.submit_budget(None);
        assert_eq!(budget, config.submit_budget(Some(ASSUMED_SUBMIT_CEILING)));
        assert_eq!(budget.attempt, Duration::from_secs(13));
        assert_eq!(budget.retries, 2);
        assert_eq!(budget.deadline, Duration::from_secs(45));
        assert_eq!(config.deadline_warning(None), None);
    }

    /// The attempt follows the ceiling mapi-lite publishes, and the retries
    /// are what then fit: every one of them can run to its bound, with its
    /// back-off, inside the deadline (CS-624).
    #[test]
    fn sr_cfg_009_the_budget_follows_mapi_lite_s_ceiling() {
        let secs = Duration::from_secs;
        for (ceiling, deadline, attempt, retries) in [
            (10, 45, 13, 2),
            (20, 45, 23, 0),
            (2, 45, 5, 5),
            (10, 13, 13, 0),
            (10, 120, 13, 6),
        ] {
            let budget = SubmitBudget::new(secs(ceiling), secs(deadline));
            assert_eq!(
                (budget.attempt, budget.retries),
                (secs(attempt), retries),
                "{ceiling}s ceiling, {deadline}s deadline"
            );
        }
        for ceiling in 0..40 {
            for deadline in 1..120 {
                let budget = SubmitBudget::new(secs(ceiling), secs(deadline));
                let worst = budget.attempt * (budget.retries + 1)
                    + (1..=budget.retries).map(retry_backoff).sum::<Duration>();
                assert!(
                    worst <= budget.deadline,
                    "{ceiling}s/{deadline}s: {budget:?} takes {worst:?}"
                );
                let one_more = worst + retry_backoff(budget.retries + 1) + budget.attempt;
                assert!(
                    one_more > budget.deadline,
                    "{ceiling}s/{deadline}s: {budget:?}"
                );
            }
        }
    }

    /// A deadline shorter than one attempt against the ceiling cuts every
    /// attempt short, so a hung node is reported as this side's deadline. It
    /// still works, so it is warned about, naming the value that would not.
    #[test]
    fn sr_cfg_010_a_deadline_shorter_than_an_attempt_is_warned_about() {
        let mut config = MapiLiteConfig::for_base_url("http://127.0.0.1:8080");
        config.total_timeout_seconds = 12;
        let warning = config.deadline_warning(None).expect("12s < 10s + 3s");
        assert!(
            warning.contains("total_timeout_seconds is 12s"),
            "{warning}"
        );
        assert!(warning.contains("at least 13s"), "{warning}");
        let budget = config.submit_budget(None);
        assert_eq!(
            (budget.attempt, budget.retries),
            (Duration::from_secs(12), 0)
        );
        assert!(config.validate().is_ok());

        let warning = MapiLiteConfig::for_base_url("http://127.0.0.1:8080")
            .deadline_warning(Some(Duration::from_secs(60)))
            .expect("45s < 60s + 3s");
        assert!(warning.contains("at 60s"), "{warning}");
        assert!(warning.contains("at least 63s"), "{warning}");
    }

    /// What `GET /status` publishes: three chain reads and the broadcast, or
    /// nothing when chain reads have no deadline (CS-624).
    #[test]
    fn sr_cfg_011_the_fund_worst_case_is_three_reads_and_the_broadcast() {
        let mut config: Config = toml::from_str(MINIMAL_TOML).unwrap();
        // Without [mapi_lite] the broadcast is one more interface call.
        assert_eq!(config.fund_worst_case(), Some(Duration::from_secs(4 * 30)));
        config.mapi_lite = Some(MapiLiteConfig::for_base_url("http://mapi"));
        assert_eq!(
            config.fund_worst_case(),
            Some(Duration::from_secs(3 * 30 + 45))
        );
        config.blockchain_interface.timeout_seconds = Some(5);
        assert_eq!(
            config.fund_worst_case(),
            Some(Duration::from_secs(3 * 5 + 45))
        );
        config.blockchain_interface.timeout_seconds = Some(0);
        assert_eq!(config.fund_worst_case(), None);
    }

    /// The settings the budget replaces are refused, not ignored (CS-624).
    #[test]
    fn sr_cfg_009_the_retired_timings_are_refused() {
        for key in [
            "timeout_seconds = 5",
            "max_retries = 1",
            "health_timeout_seconds = 1",
        ] {
            let path = write_temp_config(
                "retired-timing",
                &with_mapi_lite_section(&format!("base_url = \"http://mapi\"\n{key}\n")),
            );
            let err = with_env(&[], || load_config(path.to_str().unwrap()).unwrap_err());
            let name = key.split(' ').next().unwrap();
            assert!(err.contains(&format!("mapi_lite.{name}")), "{err}");
        }
    }

    /// Whatever `validate` checked has to be what the client requests, so the
    /// normalised URL -- not the raw field -- is what both of them read.
    #[test]
    fn sr_cfg_008_mapi_lite_base_url_is_normalised_before_use() {
        for raw in [
            "http://mapi:8080",
            "http://mapi:8080/",
            "http://mapi:8080///",
            "  http://mapi:8080  ",
            "\thttp://mapi:8080/\n",
        ] {
            let config = MapiLiteConfig::for_base_url(raw);
            assert_eq!(config.base_url(), "http://mapi:8080", "for {raw:?}");
            assert!(config.validate().is_ok(), "for {raw:?}");
        }

        // A padded value that is not a URL is still rejected: validation reads
        // the same normalised string the client would be built from.
        let error = MapiLiteConfig::for_base_url(" ftp://mapi ")
            .validate()
            .expect_err("not an http URL");
        assert!(
            error.contains("must start with http:// or https://"),
            "{error}"
        );
    }

    #[test]
    fn sr_cfg_008_mapi_lite_section_accepts_every_field() {
        let content = with_mapi_lite_section(
            "base_url = \"https://mapi.example:8443/\"\nauth_token = \"secret\"\ntotal_timeout_seconds = 30\n",
        );
        let config: Config = toml::from_str(&content).unwrap();
        let mapi_lite = config.mapi_lite.expect("section present");
        assert_eq!(mapi_lite.base_url, "https://mapi.example:8443/");
        assert_eq!(mapi_lite.auth_token.as_deref(), Some("secret"));
        assert_eq!(mapi_lite.authorization().as_deref(), Some("Bearer secret"));
        assert_eq!(mapi_lite.total_timeout(), Duration::from_secs(30));
        assert!(mapi_lite.validate().is_ok());
    }

    #[test]
    fn sr_cfg_008_mapi_lite_validate_rejects_a_missing_or_non_http_base_url() {
        for (base_url, expected) in [
            ("", "mapi_lite.base_url is required"),
            ("   ", "mapi_lite.base_url is required"),
            ("127.0.0.1:8080", "must start with http:// or https://"),
            ("ftp://mapi", "must start with http:// or https://"),
        ] {
            let error = MapiLiteConfig::for_base_url(base_url)
                .validate()
                .expect_err("expected validation to fail");
            assert!(error.contains(expected), "for '{base_url}': {error}");
        }
    }

    #[test]
    fn sr_cfg_008_mapi_lite_validate_rejects_a_zero_deadline() {
        let mut config = MapiLiteConfig::for_base_url("http://127.0.0.1:8080");
        config.total_timeout_seconds = 0;
        assert!(config
            .validate()
            .unwrap_err()
            .contains("mapi_lite.total_timeout_seconds"));
    }

    #[test]
    fn sr_cfg_008_load_config_validates_the_mapi_lite_section() {
        let _env = env_lock();
        let path = write_temp_config("mapi-lite", &with_mapi_lite_section("base_url = \"\"\n"));
        let err = load_config(path.to_str().unwrap()).unwrap_err();
        assert!(err.contains("mapi_lite.base_url"), "{err}");
    }

    #[test]
    fn sr_cfg_008_load_config_reads_a_valid_mapi_lite_section() {
        let _env = env_lock();
        let path = write_temp_config(
            "mapi-lite-ok",
            &with_mapi_lite_section("base_url = \"http://127.0.0.1:8080\"\n"),
        );
        let config = load_config(path.to_str().unwrap()).unwrap();
        assert_eq!(config.mapi_lite.unwrap().base_url, "http://127.0.0.1:8080");
    }

    /// An override can supply the whole section, which then takes the
    /// defaults for everything it leaves out.
    #[test]
    fn sr_cfg_008_overrides_can_supply_the_mapi_lite_section() {
        // Regtest: an override cannot give a list, and on testnet the section
        // needs miner_id_public_keys.
        let path = write_temp_config(
            "mapi-lite-env",
            &MINIMAL_TOML.replace("\"testnet\"", "\"regtest\""),
        );
        let config = with_env(
            &[
                ("FS__MAPI_LITE__BASE_URL", "http://mapi:8080"),
                ("FS__MAPI_LITE__TOTAL_TIMEOUT_SECONDS", "30"),
            ],
            || load_config(path.to_str().unwrap()).unwrap(),
        );
        let mapi_lite = config.mapi_lite.expect("section supplied");
        assert_eq!(mapi_lite.base_url, "http://mapi:8080");
        assert_eq!(mapi_lite.total_timeout_seconds, 30);
        assert_eq!(mapi_lite.auth_token, None);
    }

    #[test]
    fn sr_sec_015_mapi_lite_auth_token_resolves_from_an_env_reference() {
        let _env = env_lock();
        unsafe { env::set_var("FS_TEST_MAPI_TOKEN", "from-env") };
        let config = config_with_mapi_lite(Some("env:FS_TEST_MAPI_TOKEN"));
        let resolved = config.resolve_secrets().unwrap();
        assert_eq!(
            resolved.mapi_lite.unwrap().auth_token.as_deref(),
            Some("from-env")
        );
        unsafe { env::remove_var("FS_TEST_MAPI_TOKEN") };
    }

    #[test]
    fn sr_sec_015_the_mapi_lite_auth_token_can_be_an_override() {
        let path = write_temp_config(
            "mapi-lite-token",
            &with_mapi_lite_section("base_url = \"http://127.0.0.1:8080\"\n"),
        );
        let config = with_env(&[("FS__MAPI_LITE__AUTH_TOKEN", "override")], || {
            load_config(path.to_str().unwrap()).unwrap()
        });
        assert_eq!(
            config.mapi_lite.unwrap().auth_token.as_deref(),
            Some("override")
        );
    }

    /// The token is the token alone; the scheme is added when it is sent. A
    /// value written for the old verbatim header would go out as
    /// `Bearer Bearer ...` and fail only at the first request, so it stops
    /// startup, without the message repeating it (CS-624).
    #[test]
    fn sr_sec_015_a_token_with_a_scheme_or_spaces_is_refused() {
        let _env = env_lock();
        unsafe { env::set_var("FS_TEST_MAPI_TOKEN", "Bearer hunter2") };
        for token in [
            "Bearer hunter2",
            "env:FS_TEST_MAPI_TOKEN",
            " hunter2",
            "hun ter2",
        ] {
            let err = config_with_mapi_lite(Some(token))
                .resolve_secrets()
                .unwrap_err();
            assert!(err.contains("token alone"), "{token:?}: {err}");
            assert!(!err.contains("hunter2"), "{token:?}: {err}");
        }
        unsafe { env::remove_var("FS_TEST_MAPI_TOKEN") };
        let config = config_with_mapi_lite(Some("hunter2"))
            .resolve_secrets()
            .unwrap();
        assert_eq!(
            config.mapi_lite.unwrap().authorization().as_deref(),
            Some("Bearer hunter2")
        );
    }

    #[test]
    fn sr_sec_015_missing_mapi_lite_auth_token_env_reference_fails_resolution() {
        let _env = env_lock();
        unsafe { env::remove_var("FS_DEFINITELY_MISSING_MAPI_TOKEN") };
        let config = config_with_mapi_lite(Some("env:FS_DEFINITELY_MISSING_MAPI_TOKEN"));
        let err = config.resolve_secrets().unwrap_err();
        assert!(err.contains("FS_DEFINITELY_MISSING_MAPI_TOKEN"), "{err}");
    }

    #[test]
    fn mapi_lite_auth_token_absent_stays_absent_after_resolution() {
        let _env = env_lock();
        let resolved = config_with_mapi_lite(None).resolve_secrets().unwrap();
        assert_eq!(resolved.mapi_lite.unwrap().auth_token, None);
    }

    #[test]
    fn sr_sec_015_plaintext_mapi_lite_auth_token_is_reported() {
        let field = "mapi_lite.auth_token".to_string();
        assert!(plaintext_fields(Some("Bearer literal")).contains(&field));
        assert!(!plaintext_fields(Some("env:FS_MAPI_LITE_AUTH_TOKEN")).contains(&field));
        assert!(!plaintext_fields(None).contains(&field));
    }

    /// CS-418: the default has to honour what WhatsOnChain publishes, without
    /// the operator having to know the number. `woc` is somebody else's
    /// server; `rpc` and `uaas` are the operator's own and are left alone.
    #[test]
    fn cs_418_only_the_public_api_is_rate_limited_by_default() {
        let woc = BlockchainInterfaceConfig {
            interface_type: "woc".to_string(),
            ..Default::default()
        };
        assert_eq!(woc.outbound_rate_limit(), Some(WOC_REQUESTS_PER_SECOND));
        assert_eq!(
            WOC_REQUESTS_PER_SECOND, 3,
            "WhatsOnChain documents 'up to 3 requests/sec is free'"
        );

        for local in ["rpc", "uaas", "test"] {
            let config = BlockchainInterfaceConfig {
                interface_type: local.to_string(),
                ..Default::default()
            };
            assert_eq!(config.outbound_rate_limit(), None, "{local}");
        }
    }

    /// An operator with a paid plan, or a mirror of their own, can raise it;
    /// zero turns it off entirely. An explicit setting always wins over the
    /// per-interface default.
    #[test]
    fn cs_418_an_explicit_limit_overrides_the_default_either_way() {
        let faster = BlockchainInterfaceConfig {
            interface_type: "woc".to_string(),
            max_requests_per_second: Some(20),
            ..Default::default()
        };
        assert_eq!(faster.outbound_rate_limit(), Some(20));

        let off = BlockchainInterfaceConfig {
            interface_type: "woc".to_string(),
            max_requests_per_second: Some(0),
            ..Default::default()
        };
        assert_eq!(off.outbound_rate_limit(), None);

        let limited_node = BlockchainInterfaceConfig {
            interface_type: "rpc".to_string(),
            max_requests_per_second: Some(5),
            ..Default::default()
        };
        assert_eq!(limited_node.outbound_rate_limit(), Some(5));
    }

    // The struct updates cover fields other changes add to these sections.
    #[allow(clippy::needless_update)]
    fn policy_config(network: &str, admin: Option<&str>, open: bool, plaintext: bool) -> Config {
        Config {
            blockchain_interface: BlockchainInterfaceConfig {
                network: network.to_string(),
                ..Default::default()
            },
            web_interface: WebInterfaceConfig {
                admin_api_key: admin.map(str::to_string),
                allow_unauthenticated: open,
                ..Default::default()
            },
            dynamic_config: DynamicConfigConfig {
                filename: "x".to_string(),
                allow_plaintext_wif: plaintext,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// CS-514: a raw key may never be posted on a production network, opt-out
    /// or not.
    #[test]
    fn sr_sec_032_plaintext_wif_cannot_be_allowed_on_a_production_network() {
        for open in [false, true] {
            let err = policy_config("mainnet", Some("k"), open, true)
                .check_auth_policy()
                .unwrap_err();
            assert!(err.contains("allow_plaintext_wif"), "{err}");
        }
        assert!(policy_config("regtest", None, false, true)
            .check_auth_policy()
            .is_ok());
    }

    /// CS-505, SR-BCHN-017: three refresh periods by default, never under a
    /// minute, explicit when set, and off at zero.
    #[test]
    fn sr_bchn_017_ready_chain_read_bound_defaults_and_overrides() {
        let service = |period, explicit| ServiceConfig {
            utxo_refresh_period: period,
            ready_max_chain_read_age_seconds: explicit,
            ..Default::default()
        };
        let secs = |s| Some(std::time::Duration::from_secs(s));
        assert_eq!(service(60, None).ready_max_chain_read_age(), secs(180));
        assert_eq!(service(10, None).ready_max_chain_read_age(), secs(60));
        assert_eq!(service(60, Some(45)).ready_max_chain_read_age(), secs(45));
        assert_eq!(service(60, Some(0)).ready_max_chain_read_age(), None);
    }

    /// Miner IDs must look like public keys, so a typo is caught at startup
    /// rather than refusing every response (CS-523).
    #[test]
    fn sr_sec_019_miner_id_public_keys_must_be_public_keys() {
        let check = |keys: &[&str]| {
            let mut config = MapiLiteConfig::for_base_url("http://mapi-lite");
            config.miner_id_public_keys = keys.iter().map(|k| k.to_string()).collect();
            config.validate()
        };
        let good = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";
        assert!(check(&[]).is_ok());
        assert!(check(&[good]).is_ok());
        assert!(check(&[&good.to_uppercase()]).is_ok());
        assert!(check(&[&format!("04{}", "ab".repeat(64))]).is_ok());
        for bad in [
            "02abc",
            &format!("05{}", &good[2..]),
            &format!("{}zz", &good[..64]),
            &format!("04{}", "ab".repeat(32)),
        ] {
            assert!(check(&[good, bad]).is_err(), "{bad:?} was accepted");
        }
    }

    /// Unpinned is allowed off the public networks, and said so.
    #[test]
    fn sr_sec_019_an_unpinned_section_is_warned_about() {
        let mut config = MapiLiteConfig::for_base_url("http://mapi-lite");
        assert!(config.unpinned_warning().is_some());
        config.miner_id_public_keys = vec![PINNED_KEY.to_string()];
        assert!(config.unpinned_warning().is_none());
    }

    const PINNED_KEY: &str = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";
    const OTHER_PINNED_KEY: &str =
        "02c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5";

    /// CS-636: `FS__MAPI_LITE__MINER_ID_PUBLIC_KEYS` gives the keys
    /// comma-separated, replacing the file's, so a deployment configured from
    /// the environment needs no file for them; the file's list still works,
    /// and a bad key from either is refused by name.
    #[test]
    fn cs_636_miner_ids_can_come_from_an_override() {
        let section = format!(
            "\n[mapi_lite]\nbase_url = \"http://mapi:8080\"\nminer_id_public_keys = [\"{PINNED_KEY}\"]\n"
        );
        let path = write_temp_config("pins-file", &format!("{MINIMAL_TOML}{section}"));
        let keys = |vars: &[(&str, &str)]| {
            with_env(vars, || load_config(path.to_str().unwrap()))
                .map(|config| config.mapi_lite.unwrap().miner_id_public_keys)
        };
        assert_eq!(keys(&[]).unwrap(), [PINNED_KEY]);

        let var = "FS__MAPI_LITE__MINER_ID_PUBLIC_KEYS";
        let both = format!(" {OTHER_PINNED_KEY} ,{PINNED_KEY},");
        assert_eq!(
            keys(&[(var, &both)]).unwrap(),
            [OTHER_PINNED_KEY, PINNED_KEY]
        );
        assert_eq!(
            keys(&[(var, OTHER_PINNED_KEY)]).unwrap(),
            [OTHER_PINNED_KEY]
        );

        // Empty is an empty list, which testnet refuses as unpinned.
        let err = keys(&[(var, "")]).unwrap_err();
        assert!(err.contains("miner_id_public_keys is empty"), "{err}");
        let err = keys(&[(var, &format!("{PINNED_KEY},nope"))]).unwrap_err();
        assert!(err.contains("\"nope\" is not a public key"), "{err}");
    }

    /// On mainnet and testnet an unpinned section stops startup (CS-624).
    #[test]
    fn sr_sec_019_an_unpinned_section_is_refused_on_a_public_network() {
        let config = |network: &str, keys: &[&str]| {
            let mut mapi_lite = MapiLiteConfig::for_base_url("http://mapi-lite");
            mapi_lite.miner_id_public_keys = keys.iter().map(|k| k.to_string()).collect();
            Config {
                blockchain_interface: BlockchainInterfaceConfig {
                    interface_type: "woc".to_string(),
                    network: network.to_string(),
                    ..Default::default()
                },
                mapi_lite: Some(mapi_lite),
                ..Default::default()
            }
        };
        for network in ["mainnet", "testnet"] {
            let err = config(network, &[]).check_miner_id_pin().unwrap_err();
            assert!(err.contains("miner_id_public_keys"), "{err}");
            assert!(err.contains(network), "{err}");
            assert!(config(network, &[PINNED_KEY]).check_miner_id_pin().is_ok());
        }
        for network in ["regtest", "stn"] {
            assert!(config(network, &[]).check_miner_id_pin().is_ok());
        }
        let without_mapi_lite = Config {
            mapi_lite: None,
            ..config("mainnet", &[])
        };
        assert!(without_mapi_lite.check_miner_id_pin().is_ok());
    }

    /// The fee ceiling may not sit below the rate it caps.
    #[test]
    fn sr_sec_020_the_fee_ceiling_is_at_least_the_configured_rate() {
        let fees = |rate, max| FeesConfig {
            satoshis_per_kb: rate,
            max_satoshis_per_kb: max,
        };
        assert!(FeesConfig::default().validate().is_ok());
        assert!(fees(100, 100).validate().is_ok());
        assert!(fees(100, 99).validate().is_err());
    }

    /// The runtime-client prefix may not be one a configuration override
    /// (`FS__...`), which may carry any secret, could start with (CS-624).
    #[test]
    fn sr_sec_018_wif_env_prefix_may_not_overlap_the_overrides() {
        let check = |prefix: &str| {
            DynamicConfigConfig {
                filename: "x".to_string(),
                wif_env_prefix: prefix.to_string(),
                ..Default::default()
            }
            .validate()
        };
        assert!(check(DEFAULT_WIF_ENV_PREFIX).is_ok());
        assert!(check("MY_RUNTIME_KEYS_").is_ok());
        assert!(check("FS_CLIENT_").is_ok());
        for bad in ["FS__", "FS__WEB_INTERFACE__", "FS_", "FS", "F", ""] {
            assert!(check(bad).is_err(), "{bad:?} was accepted");
        }
    }
}
