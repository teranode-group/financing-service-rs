use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc, Mutex, MutexGuard,
};

use async_trait::async_trait;
use chain_gang::{
    interface::{Balance, BlockchainInterface, TestInterface, Utxo, UtxoEntry},
    messages::{BlockHeader, OutPoint, Tx, TxIn, TxOut},
    network::Network,
    script::Script,
    util::{ChainGangError, Hash256},
};

use crate::broadcaster::{BroadcastError, TxBroadcaster, MAPI_LITE};
use crate::config::{BlockchainInterfaceConfig, ClientConfig, Config, DynamicConfigConfig};

/// Serialises the tests that mutate environment variables. The environment is
/// process-global state shared by every test thread, so without this one test
/// can set or remove a variable inside another test's window.
static ENV_MUTEX: Mutex<()> = Mutex::new(());

/// Takes the environment lock, ignoring poisoning so that a panic in one test
/// does not cascade into every other test that touches the environment.
pub fn env_lock() -> MutexGuard<'static, ()> {
    ENV_MUTEX
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub const TEST_WIF: &str = "cTYmKQzX3CvHJAxe2sctsQaHG8ktiEnpXgyyycVXGg5pRcLJEeLd";
pub const TEST_CLIENT_ID: &str = "id1";
pub const TEST_ADDRESS: &str = "n1jaAsKZfE6kufLy5DAtAyQ1RzGXwMeNAF";

/// A second wallet, for a client added alongside the first. Two clients may not
/// share one (CS-521). Private key 2, as throwaway as `MAPI_TEST_SIGNER_WIF`'s
/// key 1; testnet, compressed.
pub const OTHER_TEST_WIF: &str = "cMahea7zqjxrtgAbB7LSGbcQUr1uX1ojuat9jZodMN87K7XCyj5v";
pub const OTHER_TEST_ADDRESS: &str = "mg8Jz5776UdyiYcBb9Z873NTozEiADRW5H";
pub const LOCKING_SCRIPT_HEX: &str = "76a914ddc574807c3035ab43553a22c0b9df1f55737fae88ac";

/// Signs the envelopes a mock mapi-lite returns. Any valid WIF will do: the
/// client verifies a response against the `publicKey` the envelope itself
/// carries, so a self-signed envelope verifies.
pub const MAPI_TEST_SIGNER_WIF: &str = "KwDiBf89QgGbjEhKnhXJuH7LrciVrZi3qYjgd9M7rFU73sVHnoWn";

/// A minimal spend built with chain-gang, so its bytes genuinely serialize
/// and hash rather than only looking like a transaction.
pub fn sample_tx() -> Tx {
    Tx {
        version: 2,
        inputs: vec![TxIn {
            prev_output: OutPoint {
                hash: Hash256([7u8; 32]),
                index: 0,
            },
            unlock_script: Script(vec![0x51]),
            sequence: 0xffff_ffff,
        }],
        outputs: vec![TxOut {
            satoshis: 1_000,
            lock_script: Script(vec![0x51]),
        }],
        lock_time: 0,
    }
}

pub fn test_config(dynamic_filename: &str) -> Config {
    test_config_with_keys(dynamic_filename, None, None)
}

pub fn test_config_with_api_key(dynamic_filename: &str, api_key: Option<&str>) -> Config {
    test_config_with_keys(dynamic_filename, api_key, None)
}

pub fn test_config_with_keys(
    dynamic_filename: &str,
    client_api_key: Option<&str>,
    admin_api_key: Option<&str>,
) -> Config {
    Config {
        blockchain_interface: BlockchainInterfaceConfig {
            interface_type: "test".to_string(),
            network: "testnet".to_string(),
            ..Default::default()
        },
        web_interface: crate::config::WebInterfaceConfig {
            admin_api_key: admin_api_key.map(str::to_string),
            rate_limit: crate::config::RateLimitConfig::default(),
            // The test network is testnet, where keys are otherwise required
            // (CS-514); most tests run without them. The requirement has its
            // own tests (SR-SEC-031).
            allow_unauthenticated: true,
        },
        client: Some(vec![ClientConfig {
            client_id: TEST_CLIENT_ID.to_string(),
            wif_key: TEST_WIF.to_string(),
            api_key: client_api_key.map(str::to_string),
        }]),
        dynamic_config: DynamicConfigConfig {
            filename: dynamic_filename.to_string(),
            // Most tests add clients with a raw test WIF; the default, which
            // refuses one, has its own tests (SR-SEC-021).
            allow_plaintext_wif: true,
            wif_env_prefix: crate::config::DEFAULT_WIF_ENV_PREFIX.to_string(),
        },
        ..Default::default()
    }
}

pub fn test_utxo() -> Vec<UtxoEntry> {
    vec![
        UtxoEntry {
            height: 1514933,
            tx_pos: 0,
            tx_hash: "f67272e5c1408ecbeb8da543437c125ee1a17110317d44d13eafe31b771b795e".to_string(),
            value: 240,
        },
        UtxoEntry {
            height: 1514939,
            tx_pos: 1,
            tx_hash: "b3ec9a52a1fe1689a998c869c2ae38d64d08ece8aaf218286461f330f6fd2ca8".to_string(),
            value: 100,
        },
        UtxoEntry {
            height: 1514939,
            tx_pos: 1,
            tx_hash: "76f9302ab84fc5da40de02617c10f1a26dff7007c2bfffa9d0845e57d47fa82f".to_string(),
            value: 100,
        },
        UtxoEntry {
            height: 1514939,
            tx_pos: 1,
            tx_hash: "447ee285748e88d8b875ce09026815578f0474ec3f1babcd5ba917ecb9f1dd7a".to_string(),
            value: 100,
        },
        UtxoEntry {
            height: 1514939,
            tx_pos: 2,
            tx_hash: "447ee285748e88d8b875ce09026815578f0474ec3f1babcd5ba917ecb9f1dd7a".to_string(),
            value: 100,
        },
        UtxoEntry {
            height: 1516841,
            tx_pos: 0,
            tx_hash: "70d0365df8062e5af41f8e8f2e42bafde3cabbaebd1fc94e94fa5559e87777b2".to_string(),
            value: 39_080_962,
        },
        UtxoEntry {
            height: 1517272,
            tx_pos: 0,
            tx_hash: "51b349bda57674a02ea5b90b43f2204dd6df330d751d646d74d76b19348bf5be".to_string(),
            value: 39_327_675,
        },
        UtxoEntry {
            height: 1517429,
            tx_pos: 0,
            tx_hash: "e533109de9df0184299e2199fa8f74baae7d99e4b39d3dea1e957e2f26636578".to_string(),
            value: 9_564_208,
        },
        UtxoEntry {
            height: 1517429,
            tx_pos: 1,
            tx_hash: "e533109de9df0184299e2199fa8f74baae7d99e4b39d3dea1e957e2f26636578".to_string(),
            value: 123,
        },
    ]
}

pub async fn test_blockchain_interface(
    config: &Config,
) -> Arc<dyn BlockchainInterface + Send + Sync> {
    let mut blockchain_interface = TestInterface::new();
    blockchain_interface.set_network(&config.get_network().unwrap());
    blockchain_interface
        .set_utxo(TEST_ADDRESS, &test_utxo())
        .await;
    blockchain_interface
        .set_utxo(OTHER_TEST_ADDRESS, &test_utxo())
        .await;
    blockchain_interface.set_height(1517571).await;
    Arc::new(blockchain_interface)
}

pub fn unique_dynamic_config_path() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "financing-service-test-{}-{id}-dynamic.toml",
        std::process::id()
    ));
    let path = path.to_string_lossy().into_owned();
    let _ = std::fs::write(&path, "clients = []\n");
    path
}

/// Test blockchain that counts `broadcast_tx` calls.
///
/// Needed because `TestInterface` does not consume UTXOs, so two separate
/// funding calls build a byte-identical transaction. Comparing responses
/// therefore cannot distinguish "replayed the first response" from "funded
/// again"; counting broadcasts can.
pub struct CountingBlockchain {
    inner: tokio::sync::Mutex<TestInterface>,
    broadcasts: AtomicU32,
    /// Reads of the unspent set. Every chain-state refresh makes exactly one,
    /// so this counts refreshes as the far end would see them.
    utxo_reads: AtomicU32,
}

impl CountingBlockchain {
    pub async fn new(config: &Config) -> Arc<Self> {
        let mut inner = TestInterface::new();
        inner.set_network(&config.get_network().unwrap());
        inner.set_utxo(TEST_ADDRESS, &test_utxo()).await;
        inner.set_height(1517571).await;
        Arc::new(Self {
            inner: tokio::sync::Mutex::new(inner),
            broadcasts: AtomicU32::new(0),
            utxo_reads: AtomicU32::new(0),
        })
    }

    pub fn broadcast_count(&self) -> u32 {
        self.broadcasts.load(Ordering::SeqCst)
    }

    /// How many times the unspent set has been fetched: one per refresh.
    pub fn utxo_read_count(&self) -> u32 {
        self.utxo_reads.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl BlockchainInterface for CountingBlockchain {
    fn set_network(&mut self, network: &Network) {
        if let Ok(mut inner) = self.inner.try_lock() {
            inner.set_network(network);
        }
    }

    async fn status(&self) -> Result<(), ChainGangError> {
        self.inner.lock().await.status().await
    }

    async fn get_balance(&self, address: &str) -> Result<Balance, ChainGangError> {
        self.inner.lock().await.get_balance(address).await
    }

    async fn get_utxo(&self, address: &str) -> Result<Utxo, ChainGangError> {
        self.utxo_reads.fetch_add(1, Ordering::SeqCst);
        self.inner.lock().await.get_utxo(address).await
    }

    async fn broadcast_tx(&self, tx: &Tx) -> Result<String, ChainGangError> {
        self.broadcasts.fetch_add(1, Ordering::SeqCst);
        self.inner.lock().await.broadcast_tx(tx).await
    }

    async fn get_tx(&self, txid: &str) -> Result<Tx, ChainGangError> {
        self.inner.lock().await.get_tx(txid).await
    }

    async fn get_latest_block_header(&self) -> Result<BlockHeader, ChainGangError> {
        self.inner.lock().await.get_latest_block_header().await
    }

    async fn get_block_headers(&self) -> Result<String, ChainGangError> {
        self.inner.lock().await.get_block_headers().await
    }
}

/// Test blockchain whose unspent-set reads can be held at a gate.
///
/// Stands in for an interface paced at a few requests a second, without the
/// test waiting on a clock: while the gate is closed a refresh is in flight
/// and cannot finish, so a test can tell a request that waits on the refresh
/// from one that does not. Reads are counted on arrival, before the gate, so
/// a refresh that has started counts even while it is held.
pub struct GatedBlockchain {
    inner: Arc<CountingBlockchain>,
    open: tokio::sync::watch::Sender<bool>,
    utxo_reads: AtomicU32,
}

impl GatedBlockchain {
    /// Starts open, so the service's own startup load is not held.
    pub async fn new(config: &Config) -> Arc<Self> {
        Arc::new(Self {
            inner: CountingBlockchain::new(config).await,
            open: tokio::sync::watch::channel(true).0,
            utxo_reads: AtomicU32::new(0),
        })
    }

    /// Hold every unspent-set read from now until [`GatedBlockchain::open`].
    pub fn close(&self) {
        self.open.send_replace(false);
    }

    /// Release the reads being held, and stop holding new ones.
    pub fn open(&self) {
        self.open.send_replace(true);
    }

    pub fn broadcast_count(&self) -> u32 {
        self.inner.broadcast_count()
    }

    /// Unspent-set reads started, including any held at the gate.
    pub fn utxo_read_count(&self) -> u32 {
        self.utxo_reads.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl BlockchainInterface for GatedBlockchain {
    fn set_network(&mut self, _network: &Network) {}

    async fn status(&self) -> Result<(), ChainGangError> {
        self.inner.status().await
    }

    async fn get_balance(&self, address: &str) -> Result<Balance, ChainGangError> {
        self.inner.get_balance(address).await
    }

    async fn get_utxo(&self, address: &str) -> Result<Utxo, ChainGangError> {
        self.utxo_reads.fetch_add(1, Ordering::SeqCst);
        self.open
            .subscribe()
            .wait_for(|open| *open)
            .await
            .expect("the gate outlives its readers");
        self.inner.get_utxo(address).await
    }

    /// Held at the gate too, so a test can stand in for a broadcast the
    /// interface accepts and never answers (CS-502). Counted on arrival, like
    /// reads.
    async fn broadcast_tx(&self, tx: &Tx) -> Result<String, ChainGangError> {
        self.open
            .subscribe()
            .wait_for(|open| *open)
            .await
            .expect("the gate outlives its readers");
        self.inner.broadcast_tx(tx).await
    }

    async fn get_tx(&self, txid: &str) -> Result<Tx, ChainGangError> {
        self.inner.get_tx(txid).await
    }

    async fn get_latest_block_header(&self) -> Result<BlockHeader, ChainGangError> {
        self.inner.get_latest_block_header().await
    }

    async fn get_block_headers(&self) -> Result<String, ChainGangError> {
        self.inner.get_block_headers().await
    }
}

/// Where [`UnreadableBlockchain`] says it failed to connect: an internal
/// address of the kind a real chain-read error carries, which a test can look
/// for in a response to prove it did not leak.
pub const UNREADABLE_CHAIN_URL: &str = "http://10.20.30.40:18332/";

/// Test blockchain whose unspent set cannot be read, as when WhatsOnChain or
/// the node behind the blockchain interface is down. The error names the
/// address it could not reach, as reqwest's does.
pub struct UnreadableBlockchain {
    inner: Arc<CountingBlockchain>,
    readable: AtomicBool,
}

impl UnreadableBlockchain {
    pub async fn new(config: &Config) -> Arc<Self> {
        Arc::new(Self {
            inner: CountingBlockchain::new(config).await,
            readable: AtomicBool::new(false),
        })
    }

    /// Let reads through from now on, or fail them again.
    pub fn set_readable(&self, readable: bool) {
        self.readable.store(readable, Ordering::SeqCst);
    }
}

#[async_trait]
impl BlockchainInterface for UnreadableBlockchain {
    fn set_network(&mut self, _network: &Network) {}

    async fn status(&self) -> Result<(), ChainGangError> {
        self.inner.status().await
    }

    async fn get_balance(&self, address: &str) -> Result<Balance, ChainGangError> {
        self.inner.get_balance(address).await
    }

    async fn get_utxo(&self, address: &str) -> Result<Utxo, ChainGangError> {
        if self.readable.load(Ordering::SeqCst) {
            return self.inner.get_utxo(address).await;
        }
        Err(ChainGangError::BadArgument(format!(
            "error sending request for url ({UNREADABLE_CHAIN_URL})"
        )))
    }

    async fn broadcast_tx(&self, tx: &Tx) -> Result<String, ChainGangError> {
        self.inner.broadcast_tx(tx).await
    }

    async fn get_tx(&self, txid: &str) -> Result<Tx, ChainGangError> {
        self.inner.get_tx(txid).await
    }

    async fn get_latest_block_header(&self) -> Result<BlockHeader, ChainGangError> {
        self.inner.get_latest_block_header().await
    }

    async fn get_block_headers(&self) -> Result<String, ChainGangError> {
        self.inner.get_block_headers().await
    }
}

/// Test blockchain that fails `broadcast_tx` after a configured number of successes.
pub struct FailingBroadcastBlockchain {
    inner: tokio::sync::Mutex<TestInterface>,
    fail_after: u32,
    broadcasts: AtomicU32,
}

impl FailingBroadcastBlockchain {
    pub async fn new(config: &Config, fail_after: u32) -> Arc<Self> {
        let mut inner = TestInterface::new();
        inner.set_network(&config.get_network().unwrap());
        inner.set_utxo(TEST_ADDRESS, &test_utxo()).await;
        inner.set_height(1517571).await;
        Arc::new(Self {
            inner: tokio::sync::Mutex::new(inner),
            fail_after,
            broadcasts: AtomicU32::new(0),
        })
    }
}

#[async_trait]
impl BlockchainInterface for FailingBroadcastBlockchain {
    fn set_network(&mut self, network: &Network) {
        if let Ok(mut inner) = self.inner.try_lock() {
            inner.set_network(network);
        }
    }

    async fn status(&self) -> Result<(), ChainGangError> {
        self.inner.lock().await.status().await
    }

    async fn get_balance(&self, address: &str) -> Result<Balance, ChainGangError> {
        self.inner.lock().await.get_balance(address).await
    }

    async fn get_utxo(&self, address: &str) -> Result<Utxo, ChainGangError> {
        self.inner.lock().await.get_utxo(address).await
    }

    async fn broadcast_tx(&self, tx: &Tx) -> Result<String, ChainGangError> {
        let n = self.broadcasts.fetch_add(1, Ordering::SeqCst) + 1;
        if n > self.fail_after {
            return Err(ChainGangError::BadArgument(
                "simulated broadcast failure".to_string(),
            ));
        }
        self.inner.lock().await.broadcast_tx(tx).await
    }

    async fn get_tx(&self, txid: &str) -> Result<Tx, ChainGangError> {
        self.inner.lock().await.get_tx(txid).await
    }

    async fn get_latest_block_header(&self) -> Result<BlockHeader, ChainGangError> {
        self.inner.lock().await.get_latest_block_header().await
    }

    async fn get_block_headers(&self) -> Result<String, ChainGangError> {
        self.inner.lock().await.get_block_headers().await
    }
}

/// Broadcaster that counts calls and touches no network.
///
/// Paired with [`CountingBlockchain`] as the read interface, it shows which of
/// the two a funding call actually broadcast through.
pub struct CountingBroadcaster {
    broadcasts: AtomicU32,
}

impl CountingBroadcaster {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            broadcasts: AtomicU32::new(0),
        })
    }

    pub fn broadcast_count(&self) -> u32 {
        self.broadcasts.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl TxBroadcaster for CountingBroadcaster {
    fn name(&self) -> &str {
        "counting"
    }

    async fn broadcast_tx(&self, tx: &Tx) -> Result<String, BroadcastError> {
        self.broadcasts.fetch_add(1, Ordering::SeqCst);
        Ok(tx.hash().encode())
    }

    async fn health_check(&self) -> Result<(), BroadcastError> {
        Ok(())
    }
}

/// Broadcaster that fails after a configured number of successes.
pub struct FailingBroadcaster {
    fail_after: u32,
    broadcasts: AtomicU32,
}

impl FailingBroadcaster {
    pub fn new(fail_after: u32) -> Arc<Self> {
        Arc::new(Self {
            fail_after,
            broadcasts: AtomicU32::new(0),
        })
    }
}

#[async_trait]
impl TxBroadcaster for FailingBroadcaster {
    fn name(&self) -> &str {
        "failing"
    }

    async fn broadcast_tx(&self, tx: &Tx) -> Result<String, BroadcastError> {
        let n = self.broadcasts.fetch_add(1, Ordering::SeqCst) + 1;
        if n > self.fail_after {
            return Err(BroadcastError::Upstream(
                "simulated broadcast failure".to_string(),
            ));
        }
        Ok(tx.hash().encode())
    }

    async fn health_check(&self) -> Result<(), BroadcastError> {
        Ok(())
    }
}

/// Broadcaster that answers from a script, one answer per call, and records
/// every transaction it was handed. Accepts once the script runs out.
pub struct ScriptedBroadcaster {
    answers: std::sync::Mutex<std::collections::VecDeque<Option<BroadcastError>>>,
    sent: std::sync::Mutex<Vec<Tx>>,
}

impl ScriptedBroadcaster {
    /// `None` accepts; `Some(error)` refuses with it.
    pub fn new(answers: Vec<Option<BroadcastError>>) -> Arc<Self> {
        Arc::new(Self {
            answers: std::sync::Mutex::new(answers.into()),
            sent: std::sync::Mutex::new(Vec::new()),
        })
    }

    pub fn sent(&self) -> Vec<Tx> {
        self.sent.lock().unwrap().clone()
    }
}

#[async_trait]
impl TxBroadcaster for ScriptedBroadcaster {
    fn name(&self) -> &str {
        "scripted"
    }

    async fn broadcast_tx(&self, tx: &Tx) -> Result<String, BroadcastError> {
        self.sent.lock().unwrap().push(tx.clone());
        match self.answers.lock().unwrap().pop_front().flatten() {
            Some(error) => Err(error),
            None => Ok(tx.hash().encode()),
        }
    }

    async fn health_check(&self) -> Result<(), BroadcastError> {
        Ok(())
    }
}

/// The test blockchain, listing `utxos` for the test client's address.
pub async fn blockchain_listing(
    config: &Config,
    utxos: &[UtxoEntry],
) -> Arc<dyn BlockchainInterface + Send + Sync> {
    let mut blockchain_interface = TestInterface::new();
    blockchain_interface.set_network(&config.get_network().unwrap());
    blockchain_interface
        .set_utxo(TEST_ADDRESS, &utxos.to_vec())
        .await;
    blockchain_interface.set_height(1517571).await;
    Arc::new(blockchain_interface)
}

/// Broadcaster whose upstream refuses every transaction.
///
/// `retryable` is the upstream's own view of whether resubmitting could
/// succeed -- mapi-lite's `failureRetryable` -- and is the whole point of the
/// double: the two answers call for opposite advice to the caller.
pub struct RejectingBroadcaster {
    retryable: bool,
}

impl RejectingBroadcaster {
    pub fn new(retryable: bool) -> Arc<Self> {
        Arc::new(Self { retryable })
    }
}

#[async_trait]
impl TxBroadcaster for RejectingBroadcaster {
    fn name(&self) -> &str {
        "rejecting"
    }

    async fn broadcast_tx(&self, _tx: &Tx) -> Result<String, BroadcastError> {
        Err(BroadcastError::Rejected {
            description: "txn-mempool-conflict".to_string(),
            retryable: self.retryable,
        })
    }

    async fn health_check(&self) -> Result<(), BroadcastError> {
        Ok(())
    }
}

/// Broadcaster that reports an unknown outcome, as a mapi-lite whose submit
/// deadline expires does.
///
/// It records the transactions it was handed, so a test can tell what the
/// service believes was sent versus what it does with the wallet afterwards.
pub struct UncertainBroadcaster {
    handed: std::sync::Mutex<Vec<String>>,
}

impl UncertainBroadcaster {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            handed: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// Txids of every transaction handed to this broadcaster, in order.
    pub fn handed(&self) -> Vec<String> {
        self.handed.lock().unwrap().clone()
    }
}

#[async_trait]
impl TxBroadcaster for UncertainBroadcaster {
    fn name(&self) -> &str {
        "uncertain"
    }

    async fn broadcast_tx(&self, tx: &Tx) -> Result<String, BroadcastError> {
        self.handed.lock().unwrap().push(tx.hash().encode());
        Err(BroadcastError::Indeterminate(
            "simulated submit deadline expiry".to_string(),
        ))
    }

    async fn health_check(&self) -> Result<(), BroadcastError> {
        Ok(())
    }
}

/// Accepts every transaction, but only after a pause, recording the inputs
/// each one spends.
///
/// The pause is the point: it is how long a real broadcast keeps a request
/// between planning and committing, which is the window in which concurrent
/// requests for one client could pick the same input (CS-473).
pub struct SlowRecordingBroadcaster {
    delay: std::time::Duration,
    spent: std::sync::Mutex<Vec<(String, u32)>>,
}

impl SlowRecordingBroadcaster {
    pub fn new(delay: std::time::Duration) -> Arc<Self> {
        Arc::new(Self {
            delay,
            spent: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// Every input of every transaction broadcast, as `(txid, vout)`.
    pub fn spent_inputs(&self) -> Vec<(String, u32)> {
        self.spent.lock().unwrap().clone()
    }
}

#[async_trait]
impl TxBroadcaster for SlowRecordingBroadcaster {
    fn name(&self) -> &str {
        "slow"
    }

    async fn broadcast_tx(&self, tx: &Tx) -> Result<String, BroadcastError> {
        tokio::time::sleep(self.delay).await;
        let mut spent = self.spent.lock().unwrap();
        for input in &tx.inputs {
            spent.push((input.prev_output.hash.encode(), input.prev_output.index));
        }
        Ok(tx.hash().encode())
    }

    async fn health_check(&self) -> Result<(), BroadcastError> {
        Ok(())
    }
}

/// Stands in for the mapi-lite broadcaster above the HTTP layer.
///
/// It carries the mapi-lite name, so the service treats it as mapi-lite --
/// `GET /ready` probes it and `/status` reports it -- while the probe's
/// verdict is whatever the test asked for. The real client against a mock
/// server is tested in `broadcaster::mapi`.
pub struct StubMapiBroadcaster {
    healthy: bool,
    broadcasts: AtomicU32,
    probes: AtomicU32,
    /// The rate this stub's fee quote reports, or `None` for a quote that
    /// cannot be had -- the case that must leave the configured rate alone.
    quoted_fee: Option<u64>,
    quotes: AtomicU32,
    /// What `knows_tx` answers for every txid (CS-619).
    knows: Option<bool>,
    /// The network `published_network` reports (CS-624).
    network: Option<String>,
}

impl StubMapiBroadcaster {
    pub fn new(healthy: bool) -> Arc<Self> {
        Arc::new(Self {
            healthy,
            broadcasts: AtomicU32::new(0),
            probes: AtomicU32::new(0),
            quoted_fee: None,
            quotes: AtomicU32::new(0),
            knows: None,
            network: None,
        })
    }

    /// A healthy stub whose `/ready` says its nodes are on `network`
    /// (CS-624).
    pub fn on_network(network: &str) -> Arc<Self> {
        Arc::new(Self {
            healthy: true,
            broadcasts: AtomicU32::new(0),
            probes: AtomicU32::new(0),
            quoted_fee: None,
            quotes: AtomicU32::new(0),
            knows: None,
            network: Some(network.to_string()),
        })
    }

    /// A healthy stub whose nodes answer `knows` for every transaction
    /// (CS-619).
    pub fn knowing(knows: Option<bool>) -> Arc<Self> {
        Arc::new(Self {
            healthy: true,
            broadcasts: AtomicU32::new(0),
            probes: AtomicU32::new(0),
            quoted_fee: None,
            quotes: AtomicU32::new(0),
            knows,
            network: None,
        })
    }

    /// A healthy stub whose fee quote answers `quoted_fee` satoshis per
    /// kilobyte, or answers nothing when that is `None`.
    pub fn quoting(quoted_fee: Option<u64>) -> Arc<Self> {
        Arc::new(Self {
            healthy: true,
            broadcasts: AtomicU32::new(0),
            probes: AtomicU32::new(0),
            quoted_fee,
            quotes: AtomicU32::new(0),
            knows: None,
            network: None,
        })
    }

    /// How many times the fee quote was actually asked for.
    pub fn quote_count(&self) -> u32 {
        self.quotes.load(Ordering::SeqCst)
    }

    pub fn broadcast_count(&self) -> u32 {
        self.broadcasts.load(Ordering::SeqCst)
    }

    /// How many health probes actually reached the broadcaster, so a test can
    /// tell a cached verdict from a fresh one.
    pub fn probe_count(&self) -> u32 {
        self.probes.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl TxBroadcaster for StubMapiBroadcaster {
    fn name(&self) -> &str {
        MAPI_LITE
    }

    async fn broadcast_tx(&self, tx: &Tx) -> Result<String, BroadcastError> {
        self.broadcasts.fetch_add(1, Ordering::SeqCst);
        Ok(tx.hash().encode())
    }

    async fn health_check(&self) -> Result<(), BroadcastError> {
        self.probes.fetch_add(1, Ordering::SeqCst);
        if self.healthy {
            Ok(())
        } else {
            Err(BroadcastError::Upstream("http status 503".to_string()))
        }
    }

    async fn fee_satoshis_per_kb(&self) -> Option<u64> {
        self.quotes.fetch_add(1, Ordering::SeqCst);
        self.quoted_fee
    }

    fn published_network(&self) -> Option<String> {
        self.network.clone()
    }

    async fn knows_tx(&self, _txid: &str) -> Option<bool> {
        self.knows
    }
}

/// Every log line this test's thread has written, as `(level, message)`, in
/// order (CS-482).
///
/// The first call installs a process-wide logger that records each line with
/// the thread it came from. Tests run in parallel and share that logger, so
/// lines are kept per thread: a `#[tokio::test]` runs on a current-thread
/// runtime, and everything it spawns logs from the test's own thread.
pub fn logged_on_this_thread() -> Vec<(log::Level, String)> {
    let thread = std::thread::current().id();
    capture_logger()
        .lines
        .lock()
        .unwrap()
        .iter()
        .filter(|(from, _, _)| *from == thread)
        .map(|(_, level, line)| (*level, line.clone()))
        .collect()
}

/// Install the capturing logger, if this is the first call, so a test can
/// read back what it logs with [`logged_on_this_thread`].
pub fn capture_logs() {
    capture_logger();
}

struct CaptureLogger {
    lines: std::sync::Mutex<Vec<(std::thread::ThreadId, log::Level, String)>>,
}

impl log::Log for CaptureLogger {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        self.lines.lock().unwrap().push((
            std::thread::current().id(),
            record.level(),
            record.args().to_string(),
        ));
    }

    fn flush(&self) {}
}

fn capture_logger() -> &'static CaptureLogger {
    static LOGGER: std::sync::OnceLock<&'static CaptureLogger> = std::sync::OnceLock::new();
    LOGGER.get_or_init(|| {
        let logger: &'static CaptureLogger = Box::leak(Box::new(CaptureLogger {
            lines: std::sync::Mutex::new(Vec::new()),
        }));
        log::set_logger(logger).expect("no other logger is installed in tests");
        log::set_max_level(log::LevelFilter::Trace);
        logger
    })
}
