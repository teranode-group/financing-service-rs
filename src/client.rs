// use bitcoin::{secp256k1::Secp256k1, util::key::PrivateKey, Address, PublicKey};
// use k256::ecdsa::{SigningKey, VerifyingKey};

use chain_gang::{
    interface::blockchain_interface::UNCONFIRMED_HEIGHT,
    interface::{Balance, BlockchainInterface, Utxo, UtxoEntry},
    messages::{OutPoint, Tx, TxIn, TxOut},
    network::Network,
    script::Script,
    transaction::{
        //generate_signature,
        //p2pkh::{create_unlock_script},
        //sighash::{sighash, SigHashCache, SIGHASH_ALL, SIGHASH_FORKID},
        sighash::{SIGHASH_ALL, SIGHASH_FORKID},
    },
    util::Hash256,
    wallet::{create_sighash, Wallet},
};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::config::{ClientConfig, DEFAULT_SATOSHIS_PER_KB};
use crate::responses::{CodedError, ErrorCode};

/// How long an outpoint stays reserved after a funding transaction whose
/// outcome is unknown.
///
/// The reservation exists to stop the next funding request spending an input
/// that may already be spent on chain, and it has to outlive the gap between a
/// transaction reaching the network and the read interface admitting it --
/// mempool visibility, not confirmation, so seconds to a minute in practice.
/// Ten minutes is far longer than that, and the cost of overshooting is only
/// that funds sit idle: if the transaction never landed, the reservation
/// expires and the outpoint comes back on the next refresh. The cost of
/// undershooting is a conflicting transaction, so the bound leans long.
const UNCERTAIN_SPEND_RESERVATION: Duration = Duration::from_secs(600);

/// An outpoint, as the chain names it.
type OutPointKey = (String, u32);

fn outpoint_key(entry: &UtxoEntry) -> OutPointKey {
    (entry.tx_hash.clone(), entry.tx_pos)
}

/// The balance an unspent set adds up to, split the way chain-gang defines it:
/// a negative height means unconfirmed.
///
/// Derived rather than asked for. A separate balance query can disagree with
/// the UTXO set -- WhatsOnChain's `/address/{a}/balance` is deprecated and was
/// reporting `unconfirmed: 0` for an address whose unspent set plainly held
/// unconfirmed outputs -- and when they disagree, the UTXO set is the one that
/// matters: it is what the service can actually spend, and what every funding
/// decision is made from. Deriving also means one request per refresh instead
/// of two, which is the thing the rate limit is spent on.
pub fn balance_from_unspent(unspent: &Utxo) -> Balance {
    let mut balance = Balance::default();
    for entry in unspent {
        if entry.height < 0 {
            balance.unconfirmed += entry.value;
        } else {
            balance.confirmed += entry.value;
        }
    }
    balance
}

/// A change output this service created and broadcast, which the chain has not
/// reported back yet.
///
/// Held whole, because keeping it in the cache means putting it back after a
/// refresh has replaced the cache with what the chain says.
#[derive(Clone, Debug)]
struct PendingChange {
    entry: UtxoEntry,
    since: Instant,
}

/// Change this service created and then spent in a later transaction of its
/// own, before the chain had reported that later transaction.
///
/// Keyed in [`Client`] by the txid of the spending transaction, because that
/// is what the chain's answer proves: an output of that transaction listed as
/// unspent means the chain has seen it, so it knows these are spent.
#[derive(Clone, Debug)]
struct SpentChange {
    outpoints: Vec<OutPointKey>,
    since: Instant,
}

/// A reservation, in a form that survives a restart (CS-465).
///
/// `Instant` means nothing to another process, so the moment is stored as
/// wall-clock seconds and turned back into an age when read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedReservation {
    pub tx_hash: String,
    pub tx_pos: u32,
    pub since_unix: u64,
}

/// A pending change output, in a form that survives a restart (CS-465).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedPendingChange {
    pub tx_hash: String,
    pub tx_pos: u32,
    pub value: i64,
    pub height: i32,
    pub since_unix: u64,
}

/// What a client has broadcast that the chain may not have caught up with:
/// the inputs its transactions spent and the change they created.
///
/// This is the state CS-426 holds in memory so a refresh cannot hand an
/// input out twice. It has to outlive the process, because the chain is not a
/// substitute for it: straight after a broadcast, the read interface may not
/// yet show the transaction at all, and after a restart the service would
/// otherwise rebuild -- byte for byte, since signing is deterministic -- the
/// transactions it had just sent (CS-465).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InflightState {
    #[serde(default)]
    pub reserved: Vec<PersistedReservation>,
    #[serde(default)]
    pub pending_change: Vec<PersistedPendingChange>,
}

impl InflightState {
    pub fn is_empty(&self) -> bool {
        self.reserved.is_empty() && self.pending_change.is_empty()
    }
}

/// Bytes Bitcoin's variable-length integer takes to encode `n`, as used for a
/// transaction's input and output counts and each script's length.
fn varint_bytes(n: u64) -> u64 {
    match n {
        0..=0xfc => 1,
        0xfd..=0xffff => 3,
        0x1_0000..=0xffff_ffff => 5,
        _ => 9,
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

/// `since`, as wall-clock seconds.
fn to_unix(since: Instant, now: Instant, now_unix: u64) -> u64 {
    now_unix.saturating_sub(now.saturating_duration_since(since).as_secs())
}

/// A persisted moment, as an `Instant` in this process -- or `None` if it is
/// already older than the reservation window.
///
/// Expired entries are dropped here rather than restored for the next refresh
/// to drop, because an age this process's clock cannot represent falls back to
/// "now", and that fallback must never resurrect something long expired. A
/// moment in the future -- the wall clock stepped back -- reads as age zero,
/// which holds the entry longer rather than shorter: idle funds heal, a double
/// spend does not.
fn from_unix(since_unix: u64, now: Instant, now_unix: u64) -> Option<Instant> {
    let age = Duration::from_secs(now_unix.saturating_sub(since_unix));
    if age >= UNCERTAIN_SPEND_RESERVATION {
        return None;
    }
    Some(now.checked_sub(age).unwrap_or(now))
}

#[derive(Clone, Debug)]
pub struct FundingSpendPlan {
    /// The transaction this plan built, which is what its funded outpoints
    /// are named after.
    txid: String,
    /// `None` when the change was dust and went to the fee instead, so
    /// there is no change output to track (CS-452).
    change_entry: Option<UtxoEntry>,
    /// The inputs this plan spends, by outpoint.
    ///
    /// By outpoint and never by position. Positions address the cache as it
    /// was when the plan was made, and a concurrent request's commit
    /// reshuffles it -- removing its inputs, adding its change, re-sorting --
    /// so a position taken before that points at a different UTXO after it
    /// (CS-473).
    spent_outpoints: Vec<UtxoEntry>,
}

/// Every satoshi there will ever be: 21 million BSV. No payout can exceed it,
/// and `POST /fund` refuses one that would (CS-530).
pub const MAX_SATOSHIS: u64 = 21_000_000 * 100_000_000;

#[derive(Clone)]
pub struct FundRequest {
    pub client_id: String,
    pub satoshi: u64,
    pub no_of_outpoints: u32,
    pub multiple_tx: bool,
    /// One locking script per requested outpoint.
    ///
    /// The REST layer normalises both request forms into this: a single
    /// `locking_script` is expanded to `no_of_outpoints` copies, and a
    /// `locking_scripts` list is used as given. Invariant: the length always
    /// equals `no_of_outpoints`.
    pub locking_scripts: Vec<Vec<u8>>,
}

impl FundRequest {
    /// Total bytes of all output locking scripts, for fee estimation.
    fn script_lengths(&self) -> Vec<u64> {
        self.locking_scripts
            .iter()
            .map(|script| script.len() as u64)
            .collect()
    }

    /// Script for the `index`-th outpoint, falling back to the last one if the
    /// list is somehow short (the invariant should prevent this).
    fn script_at(&self, index: usize) -> &[u8] {
        self.locking_scripts
            .get(index)
            .or_else(|| self.locking_scripts.last())
            .map(|script| script.as_slice())
            .unwrap_or(&[])
    }
}

/// Represents a Client of the service
#[derive(Debug, Clone)]
pub struct Client {
    api_key: Option<String>,
    /// Funding Wallet
    wallet: Wallet,
    address: String,
    /// Current funding UTXO
    unspent: Utxo,
    /// Outpoints spent by a funding transaction whose outcome is unknown.
    ///
    /// Held out of `unspent` -- and out of every refresh that would otherwise
    /// resurrect them -- until the chain agrees they are spent or the
    /// reservation expires. See [`UNCERTAIN_SPEND_RESERVATION`].
    /// When the unspent set was last taken from the chain, or `None` if it
    /// has not been, or has been marked stale. Drives the freshness check
    /// that keeps a burst of requests from each fetching the same answer.
    chain_state_at: Option<Instant>,
    /// When the unspent set was last read from the chain. Unlike
    /// `chain_state_at` this is never cleared: marking the state stale says
    /// "refresh before trusting it", not "nothing was ever read". It is what
    /// `GET /ready` measures the cache against (CS-505, SR-BCHN-017).
    chain_read_at: Option<Instant>,
    /// Whether `unspent` can be funded from without fetching it first: it has
    /// been taken from the chain at least once, and nothing has marked it
    /// untrustworthy since. Not the same as fresh. A usable state can be older
    /// than the window, and only then may a refresh run behind a funding
    /// request instead of in front of it (SR-FUND-031).
    chain_state_usable: bool,
    /// How many times [`Client::invalidate_chain_state`] has run (CS-721). A
    /// chain read notes it when it starts; an invalidation since then means
    /// the read began before whatever made the state untrustworthy.
    invalidations: u64,
    /// Keyed by outpoint, valued by when it was reserved. Only the moment is
    /// needed: the balance is derived from what is left in `unspent`, so
    /// removing the entry from there is all it takes to withhold its value.
    reserved: HashMap<OutPointKey, Instant>,
    /// Satoshis per kilobyte used to cost the transactions this client builds.
    ///
    /// Held per client rather than read from config at each use because it can
    /// change while the service runs: when `[mapi_lite]` is configured the
    /// rate is refreshed from its fee quote (CS-451).
    fee_satoshis_per_kb: u64,
    /// Change this service created and broadcast, which the chain has not
    /// caught up with. Kept in the cache across refreshes so a client can
    /// spend its own change without waiting for the chain to confirm what the
    /// service already knows it sent.
    pending_change: HashMap<OutPointKey, PendingChange>,
    /// Transactions whose outpoints have been handed to a caller within the
    /// reservation window, by txid (CS-474).
    ///
    /// Two callers given the same transaction are given the same outpoints,
    /// and only one of them owns them. That happened silently: concurrent
    /// requests with identical parameters built byte-identical transactions,
    /// and the upstream answers a transaction it already holds with success.
    /// Claiming inputs at planning (CS-473) removed the cause; this makes any
    /// recurrence an error rather than a second 200.
    handed_out: HashMap<String, Instant>,
    /// The change each claimed but uncommitted plan will return, by txid
    /// (CS-475).
    ///
    /// What separates "every UTXO is in flight" from "the wallet is short":
    /// a request refused now, which this change would let through, only has
    /// to wait for the requests ahead of it. It is a lower bound on what comes
    /// back whichever way their broadcasts go -- a success returns this
    /// change, a refusal returns the whole input, which is more -- so a
    /// request judged fundable on it will be. A plan whose change went to the
    /// fee returns nothing if it succeeds, so it is not recorded.
    in_flight_change: HashMap<String, PendingChange>,
    /// The service's own change spent by each of its transactions the chain
    /// has not been seen to report yet, by the spending txid (SR-FUND-033).
    ///
    /// Such change is reserved like any spent input, but the usual release --
    /// "the chain no longer lists it" -- is unsound for it: the chain may
    /// never have listed it, and may list it later, once it has seen the
    /// transaction that created it but not yet the one that spent it. Released
    /// instead when the chain lists an output of the spending transaction or
    /// of anything built on it, which it cannot do without having seen it.
    spent_change: HashMap<String, SpentChange>,
}

/// Why a key whose WIF says `key` cannot be a wallet on `network`
/// (`blockchain_interface.network`), or `None` if it can (CS-634). Its address
/// would be on the other network, so the wallet would read empty and every
/// funding fail. A WIF only tells mainnet from the rest: testnet, regtest and
/// STN keys share a version byte. Names the client, never the key.
fn key_network_mismatch(client_id: &str, key: Network, network: &str) -> Option<String> {
    let key_on_mainnet = key == Network::BSV_Mainnet;
    if key_on_mainnet == (network == "mainnet") {
        return None;
    }
    let (key, wanted) = if key_on_mainnet {
        ("a mainnet", "a test-network (testnet, regtest or STN)")
    } else {
        ("a test-network (testnet, regtest or STN)", "a mainnet")
    };
    Some(format!(
        r#"wif_key is {key} key, but blockchain_interface.network is {network} (client_id = "{client_id}"): its address would be on the other network, so its wallet would read empty. Use {wanted} key."#
    ))
}

impl Client {
    /// A client funding from `config`'s key on `network`, the configured
    /// `blockchain_interface.network`, which the key must be for (CS-634).
    pub fn try_new(config: &ClientConfig, network: &str) -> Result<Self, String> {
        let wallet = Wallet::from_wif(&config.wif_key).map_err(|_| {
            format!(
                r#"wif_key is not a valid WIF key (client_id = "{}")."#,
                config.client_id
            )
        })?;
        if let Some(why) = key_network_mismatch(&config.client_id, wallet.network, network) {
            return Err(why);
        }
        let address = wallet.get_address().map_err(|_| {
            format!(
                r#"wif_key is not a valid WIF key - issues with address (client_id = "{}")."#,
                config.client_id
            )
        })?;
        Ok(Client {
            api_key: config.api_key.clone().filter(|key| !key.is_empty()),
            wallet,
            address,
            unspent: Vec::new(),
            chain_state_at: None,
            chain_read_at: None,
            chain_state_usable: false,
            invalidations: 0,
            reserved: HashMap::new(),
            pending_change: HashMap::new(),
            handed_out: HashMap::new(),
            in_flight_change: HashMap::new(),
            spent_change: HashMap::new(),
            fee_satoshis_per_kb: DEFAULT_SATOSHIS_PER_KB,
        })
    }

    pub fn api_key(&self) -> Option<&str> {
        self.api_key.as_deref()
    }

    /// Query the interface for the latest unspent set, and take the balance
    /// from it.
    pub async fn update_balance(
        &mut self,
        blockchain_interface: &dyn BlockchainInterface,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let unspent = blockchain_interface
            .get_utxo(&self.address.to_string())
            .await?;
        self.apply_chain_state(unspent);
        Ok(())
    }

    /// Replace the cached balance and UTXO set with what the chain reports,
    /// less anything still reserved by an uncertain funding transaction.
    ///
    /// Without that subtraction a refresh would undo a reservation as fast as
    /// it was made: a transaction that has reached the network but is not yet
    /// visible to the read interface still reads as unspent, and the service
    /// would offer the same input to the next funding request.
    pub fn apply_chain_state(&mut self, unspent: Utxo) {
        self.apply_chain_state_read_since(unspent, self.invalidations);
    }

    /// [`Self::apply_chain_state`], for a read that started when
    /// [`Self::invalidations`] was `since` (CS-721).
    ///
    /// A broadcast refused as a conflict while the read was in flight
    /// invalidates the state, and the read's answer predates it: it can
    /// still list the input the conflict spent. Applied and stamped fresh, it
    /// sent the next request to that input again without a fetch, for up to
    /// `chain_state_max_age`. So it is applied -- it is still the newest
    /// answer -- but left stale and untrusted, and the next request fetches
    /// again.
    pub fn apply_chain_state_read_since(&mut self, unspent: Utxo, since: u64) {
        self.apply_chain_state_now(unspent);
        if self.invalidations != since {
            self.chain_state_at = None;
            self.chain_state_usable = false;
        }
    }

    /// How many times the chain state has been invalidated: what a chain
    /// read notes when it starts (CS-721).
    pub fn invalidations(&self) -> u64 {
        self.invalidations
    }

    fn apply_chain_state_now(&mut self, unspent: Utxo) {
        let now = Instant::now();
        self.chain_state_at = Some(now);
        self.chain_read_at = Some(now);
        self.chain_state_usable = true;
        self.release_expired_reservations();
        self.expire_pending_change();
        let reported: std::collections::HashSet<OutPointKey> =
            unspent.iter().map(outpoint_key).collect();
        self.release_seen_spent_change(&reported);

        // An outpoint the chain no longer reports as unspent has been spent,
        // so the reservation has done its job and can go.
        //
        // Except for one this service created itself and the chain has not
        // reported yet -- change from a transaction still in the mempool.
        // "The chain stopped reporting it" and "the chain never reported it"
        // look identical from here, so applying the rule to those would
        // release the reservation the moment it was made. Those are held
        // until they expire instead. The same goes for change the service has
        // spent in turn, until the chain is seen to know of the spend.
        let ours: std::collections::HashSet<OutPointKey> =
            self.pending_change.keys().cloned().collect();
        let spent_ours: std::collections::HashSet<&OutPointKey> = self
            .spent_change
            .values()
            .flat_map(|spent| &spent.outpoints)
            .collect();
        self.reserved.retain(|key, _| {
            ours.contains(key) || spent_ours.contains(key) || reported.contains(key)
        });

        self.unspent = unspent;
        if !self.reserved.is_empty() {
            self.unspent
                .retain(|entry| !self.reserved.contains_key(&outpoint_key(entry)));
        }
        self.restore_pending_change();
        self.unspent.sort_by_key(|x| x.value);
    }

    /// Forget change that has been pending too long to still be believed.
    fn expire_pending_change(&mut self) {
        let now = Instant::now();
        self.pending_change
            .retain(|_, pending| now.duration_since(pending.since) < UNCERTAIN_SPEND_RESERVATION);
        // Past the window its reservation has expired too, so there is
        // nothing left for the record to hold.
        self.spent_change
            .retain(|_, spent| now.duration_since(spent.since) < UNCERTAIN_SPEND_RESERVATION);
    }

    /// Let go of spent change the chain has caught up with (SR-FUND-033).
    ///
    /// An output listed as unspent proves the chain has seen the transaction
    /// that created it, and so every transaction that one was built on: the
    /// change each of them spent is spent as far as the chain is concerned,
    /// and its usual release applies from here. Walked from every listed
    /// output, since the chain lists only the newest change in a run of the
    /// service's own spends -- each earlier one is spent by the next.
    fn release_seen_spent_change(&mut self, reported: &std::collections::HashSet<OutPointKey>) {
        if self.spent_change.is_empty() {
            return;
        }
        let mut seen: Vec<String> = reported
            .iter()
            .map(|(txid, _)| txid.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        while let Some(txid) = seen.pop() {
            if let Some(spent) = self.spent_change.remove(&txid) {
                for outpoint in spent.outpoints {
                    // Spent as far as the chain knows, so no longer pending
                    // either; that would hold its reservation a refresh more.
                    self.pending_change.remove(&outpoint);
                    // It came from a transaction the chain has therefore seen
                    // as well.
                    seen.push(outpoint.0);
                }
            }
        }
    }

    /// Note which of a transaction's inputs are the service's own change, so
    /// their reservations outlast a chain that has not listed them yet.
    ///
    /// Own change is recognised by its txid: a transaction this service handed
    /// out within the reservation window, or one still broadcasting -- a claim
    /// can take the change of a transaction the node has accepted before the
    /// service has heard so (CS-691).
    fn record_spent_change(&mut self, txid: &str, spent: &[UtxoEntry], now: Instant) {
        let outpoints: Vec<OutPointKey> = spent
            .iter()
            .filter(|entry| {
                self.handed_out.contains_key(&entry.tx_hash)
                    || self.in_flight_change.contains_key(&entry.tx_hash)
            })
            .map(outpoint_key)
            .collect();
        if !outpoints.is_empty() {
            self.spent_change.insert(
                txid.to_string(),
                SpentChange {
                    outpoints,
                    since: now,
                },
            );
        }
    }

    /// Put back the change the service has broadcast but the chain has not
    /// reported yet, so it counts towards the balance.
    ///
    /// A refresh replaces the cache with what the chain says, and the chain
    /// does not yet say anything about a transaction still in the mempool. So
    /// the inputs it spent are held out of `unspent` and the change it created
    /// is put back here -- the two halves of the same gap. An entry goes once
    /// the chain reports it, which is the chain catching up, or once the
    /// service has spent it in turn.
    ///
    /// Putting the entry back in `unspent` is all that is needed for it to
    /// count: the balance is derived from that set, not tracked alongside it.
    fn restore_pending_change(&mut self) {
        let reported: std::collections::HashSet<OutPointKey> =
            self.unspent.iter().map(outpoint_key).collect();
        self.pending_change.retain(|key, _| !reported.contains(key));
        self.pending_change
            .retain(|key, _| !self.reserved.contains_key(key));

        for pending in self.pending_change.values() {
            self.unspent.push(pending.entry.clone());
        }
    }

    fn release_expired_reservations(&mut self) {
        let now = Instant::now();
        self.reserved.retain(|key, since| {
            let held = now.duration_since(*since) < UNCERTAIN_SPEND_RESERVATION;
            if !held {
                log::info!(
                    "releasing reserved outpoint {}:{} -- no longer spent on chain after {}s, so \
                     the uncertain funding transaction never landed",
                    key.0,
                    key.1,
                    UNCERTAIN_SPEND_RESERVATION.as_secs()
                );
            }
            held
        });
    }

    /// What this client has in flight, for writing to disk (CS-465).
    ///
    /// Sorted, so the file a given state produces does not depend on hash map
    /// iteration order.
    pub fn inflight_state(&self) -> InflightState {
        let now = Instant::now();
        let now_unix = unix_now();
        let mut reserved: Vec<PersistedReservation> = self
            .reserved
            .iter()
            .map(|((tx_hash, tx_pos), since)| PersistedReservation {
                tx_hash: tx_hash.clone(),
                tx_pos: *tx_pos,
                since_unix: to_unix(*since, now, now_unix),
            })
            .collect();
        reserved.sort_by(|a, b| (&a.tx_hash, a.tx_pos).cmp(&(&b.tx_hash, b.tx_pos)));

        let mut pending_change: Vec<PersistedPendingChange> = self
            .pending_change
            .values()
            .map(|pending| PersistedPendingChange {
                tx_hash: pending.entry.tx_hash.clone(),
                tx_pos: pending.entry.tx_pos,
                value: pending.entry.value,
                height: pending.entry.height,
                since_unix: to_unix(pending.since, now, now_unix),
            })
            .collect();
        pending_change.sort_by(|a, b| (&a.tx_hash, a.tx_pos).cmp(&(&b.tx_hash, b.tx_pos)));

        InflightState {
            reserved,
            pending_change,
        }
    }

    /// Put back what a previous run of the service had in flight (CS-465).
    ///
    /// Only the bookkeeping is restored, not the cache: the next refresh
    /// applies it exactly as it would have in the process that wrote it --
    /// withholding the reserved inputs from what the chain reports, putting
    /// back pending change the chain has not reported yet, and letting go of
    /// whatever the chain has since caught up with.
    pub fn restore_inflight_state(&mut self, state: InflightState) {
        let now = Instant::now();
        let now_unix = unix_now();
        for reservation in state.reserved {
            if let Some(since) = from_unix(reservation.since_unix, now, now_unix) {
                self.reserved
                    .insert((reservation.tx_hash, reservation.tx_pos), since);
            }
        }
        for pending in state.pending_change {
            if let Some(since) = from_unix(pending.since_unix, now, now_unix) {
                let entry = UtxoEntry {
                    height: pending.height,
                    tx_pos: pending.tx_pos,
                    tx_hash: pending.tx_hash,
                    value: pending.value,
                };
                self.pending_change
                    .insert(outpoint_key(&entry), PendingChange { entry, since });
            }
        }
    }

    /// Number of outpoints currently withheld from funding.
    #[cfg(test)]
    pub fn reserved_outpoint_count(&self) -> usize {
        self.reserved.len()
    }

    /// Age every handed-out transaction by `by`, so a test can reach the end
    /// of its window without waiting for it.
    #[cfg(test)]
    fn backdate_handed_out(&mut self, by: Duration) {
        for since in self.handed_out.values_mut() {
            *since -= by;
        }
    }

    /// Age every reservation by `by`, so a test can reach the expiry without
    /// waiting for it.
    #[cfg(test)]
    fn backdate_reservations(&mut self, by: Duration) {
        for since in self.reserved.values_mut() {
            *since -= by;
        }
        for spent in self.spent_change.values_mut() {
            spent.since -= by;
        }
    }

    /// How long ago the unspent set was last read from the chain, or `None` if
    /// it never has been. Not reset by [`Client::invalidate_chain_state`].
    pub fn chain_read_age(&self) -> Option<Duration> {
        self.chain_read_at.map(|at| at.elapsed())
    }

    /// Age the last chain read by `by`, so a test can make the cache old
    /// without waiting.
    #[cfg(test)]
    pub fn backdate_chain_read(&mut self, by: Duration) {
        if let Some(at) = self.chain_read_at.as_mut() {
            *at -= by;
        }
    }

    /// Whether the cached chain state is younger than `max_age`.
    ///
    /// A client that has never been refreshed is never fresh, so the first
    /// request for it always fetches.
    pub fn chain_state_is_fresh(&self, max_age: Duration) -> bool {
        match self.chain_state_at {
            Some(at) => at.elapsed() < max_age,
            None => false,
        }
    }

    /// Whether the cached unspent set may be funded from without fetching it
    /// first.
    ///
    /// False until the first refresh, since there is nothing to fund from, and
    /// after [`Client::invalidate_chain_state`], since then the cache is known
    /// not to be trustworthy. Merely being older than the window leaves it
    /// usable: nothing says it is wrong, only that it may be behind.
    pub fn chain_state_usable(&self) -> bool {
        self.chain_state_usable
    }

    /// Treat the cached chain state as stale, whatever its age.
    ///
    /// Used when something has happened that the cache cannot be trusted to
    /// reflect -- a broadcast the upstream refused, which may mean an input
    /// this service still believes it owns was spent by someone else, or one
    /// whose outcome is unknown. Reaching for the chain immediately would cost
    /// a request the caller cannot use; marking the state stale instead makes
    /// the *next* request pay for it, and only if one comes.
    pub fn invalidate_chain_state(&mut self) {
        self.invalidations += 1;
        self.chain_state_at = None;
        // Stale *and* untrusted, so the next request waits for the refresh
        // even where staleness alone would let it run behind (SR-FUND-031).
        self.chain_state_usable = false;
    }

    /// The client's balance, derived from its unspent set.
    ///
    /// Not stored. A stored balance drifts: it was refreshed from a separate
    /// query that could disagree with the UTXO set, and it was not updated
    /// when a funding transaction spent from that set, so it could outlive
    /// what it described. Derived, it cannot -- and it is the same quantity
    /// every funding decision is made from. Anything reserved by an uncertain
    /// broadcast is already out of `unspent`, so it is out of this too.
    pub fn get_balance(&self) -> Balance {
        balance_from_unspent(&self.unspent)
    }

    /// The confirmed UTXO with the lowest height in this wallet, as the chain
    /// interface reports it: its transaction and height (CS-619). The oldest,
    /// because it is the one any node on the same chain has had longest to
    /// see. Unconfirmed outputs, the service's own change among them, are
    /// left out.
    pub fn oldest_confirmed_utxo(&self) -> Option<(String, i32)> {
        self.unspent
            .iter()
            .filter(|utxo| utxo.height > 0)
            .min_by_key(|utxo| utxo.height)
            .map(|utxo| (utxo.tx_hash.clone(), utxo.height))
    }

    pub fn get_address(&self) -> String {
        self.address.to_string()
    }

    /// Return the smallest unspent that can cover `satoshi`.
    ///
    /// Two passes, because the smallest UTXO that covers the cost is not
    /// always the one to use. Folding dust change into the fee (CS-452) means
    /// a UTXO worth a little more than the cost hands the difference to the
    /// miner, so one whose change is either nothing or worth an output is
    /// preferred. Only if no such UTXO exists is the dust-folding one taken,
    /// which keeps the request fundable rather than refusing it to save a
    /// sum smaller than the dust threshold.
    fn get_smallest_unspent(&self, satoshi: u64) -> Option<&UtxoEntry> {
        let cost = satoshi as i64;
        self.unspent
            .iter()
            .find(|utxo| self.change_is_acceptable(utxo.value - cost))
            .or_else(|| self.unspent.iter().find(|utxo| utxo.value >= cost))
    }

    fn total_unspent(&self) -> i64 {
        self.unspent.iter().map(|utxo| utxo.value).sum()
    }

    /// Estimate the fee for a transaction paying one output to each of
    /// `funded_scripts` -- given as their lengths -- plus change.
    ///
    /// Takes the scripts rather than a byte total so the caller cannot get
    /// the serialisation wrong: every output is an 8-byte value, a length
    /// prefix and the script, and counting only the script is what left each
    /// funded output 9 bytes short and the service paying under the rate it
    /// was configured with (CS-471). Scripts of differing sizes are costed
    /// individually rather than assumed to match the first.
    fn estimate_fee(&self, funded_scripts: &[u64], no_of_inputs: u32) -> u64 {
        let tx_bytes = Self::funding_tx_bytes(funded_scripts, no_of_inputs);
        // Rounded up: rounding down would underpay, and a transaction a miner
        // will not relay costs far more to discover than the satoshi saved.
        tx_bytes
            .saturating_mul(self.fee_satoshis_per_kb)
            .div_ceil(1000)
    }

    /// The rate this client costs its transactions at, in satoshis per KB:
    /// the rate [`Self::dust_threshold`] is computed at, published beside it
    /// on `GET /client/{client_id}/balance` (CS-684).
    pub fn fee_satoshis_per_kb(&self) -> u64 {
        self.fee_satoshis_per_kb
    }

    /// Serialised size of a funding transaction: `no_of_inputs` P2PKH inputs,
    /// one output per script in `funded_scripts`, and a change output.
    ///
    /// An upper bound, not an average. Every input is costed at the most a
    /// low-S signature can take, and change is always counted though dust
    /// change is left out -- so a real transaction is never larger than this,
    /// and the fee on it never falls below the rate.
    fn funding_tx_bytes(funded_scripts: &[u64], no_of_inputs: u32) -> u64 {
        const VERSION_AND_LOCKTIME_BYTES: u64 = 8;
        let inputs = no_of_inputs.max(1) as u64;
        let outputs = funded_scripts.len() as u64 + 1;
        let funded_output_bytes: u64 = funded_scripts
            .iter()
            .map(|&script| Self::output_bytes(script))
            .sum();
        VERSION_AND_LOCKTIME_BYTES
            + varint_bytes(inputs)
            + Self::INPUT_BYTES * inputs
            + varint_bytes(outputs)
            + funded_output_bytes
            + Self::CHANGE_OUTPUT_BYTES
    }

    /// Serialised size of an output paying to a script of `script_bytes`: an
    /// 8-byte value, the script's length prefix, then the script.
    fn output_bytes(script_bytes: u64) -> u64 {
        8 + varint_bytes(script_bytes) + script_bytes
    }

    /// Bytes a change output adds to the transaction that creates it: a P2PKH
    /// output, which is [`Self::output_bytes`] of a 25-byte script.
    const CHANGE_OUTPUT_BYTES: u64 = 34;

    /// Bytes an input adds to the transaction that spends it.
    const INPUT_BYTES: u64 = 148;

    /// Headroom over break-even on [`Self::dust_threshold`].
    ///
    /// An output is created at today's rate but swept at some unknown future
    /// one. At exactly break-even a rate rise strands it, which is the failure
    /// CS-452 reported, so one doubling is bought as insurance.
    ///
    /// 1 -- true break-even -- is defensible if never donating a satoshi more
    /// than necessary matters more. 3, which is Bitcoin Core's, triples what
    /// goes to the miner to insure against a rise that current BSV fee trends
    /// do not suggest is coming.
    const DUST_THRESHOLD_MULTIPLIER: u64 = 2;

    /// The value below which change is not worth paying back.
    ///
    /// The round-trip cost of a change output: the bytes it adds to the
    /// transaction that creates it, plus the bytes it will cost to spend
    /// later, priced at the rate this client is actually paying.
    ///
    /// ```text
    /// max(1, k * ceil((CHANGE_OUTPUT_BYTES + INPUT_BYTES) * sat_per_kb / 1000))
    /// ```
    ///
    /// Below it, creating the output and later sweeping it costs more in fees
    /// than it returns, so folding it into the fee is strictly better. Above
    /// it, paying it back is.
    ///
    /// Derived rather than configured, so it cannot drift out of step with the
    /// fee the service is paying -- the same rate either way, whether that came
    /// from `[fees]` or from mapi-lite's `miningFee` (CS-451).
    ///
    /// The shape is the standard one, and the constant was the only thing
    /// wrong with the familiar figure: at 3000 sat/KB with `k = 1` this gives
    /// 546, which is where Bitcoin Core's number comes from. That figure is
    /// not wrong, it is pinned to a fee rate three orders of magnitude above
    /// what BSV charges.
    ///
    /// Published on `GET /client/{client_id}/balance` as `dust_threshold`
    /// (CS-684), so a caller that must agree with it -- Event's UTXO cache,
    /// deciding what is worth keeping -- reads it rather than derives it.
    pub fn dust_threshold(&self) -> i64 {
        let round_trip_bytes = Self::CHANGE_OUTPUT_BYTES + Self::INPUT_BYTES;
        let break_even = round_trip_bytes
            .saturating_mul(self.fee_satoshis_per_kb)
            .div_ceil(1000);
        // Never zero: a change output of nothing is not an output, and the
        // comparison below is `>=`.
        break_even
            .saturating_mul(Self::DUST_THRESHOLD_MULTIPLIER)
            .max(1) as i64
    }

    /// Whether a transaction leaving `change` wastes nothing: either it pays
    /// the change back, or there is no change to pay.
    ///
    /// Negative means the UTXO cannot cover the cost at all.
    fn change_is_acceptable(&self, change: i64) -> bool {
        change == 0 || self.change_is_worth_paying(change)
    }

    /// Whether `change` is worth paying back to the client.
    ///
    /// Zero is not worth an output by definition, and anything under the
    /// threshold costs more to spend later than it carries.
    fn change_is_worth_paying(&self, change: i64) -> bool {
        change >= self.dust_threshold() && change > 0
    }

    /// Set the rate used to cost future transactions.
    ///
    /// Zero is ignored rather than applied: it would build transactions no
    /// miner relays, and a bad quote should leave the last good rate standing
    /// rather than stop funding working.
    pub fn set_fee_satoshis_per_kb(&mut self, satoshis_per_kb: u64) {
        if satoshis_per_kb == 0 {
            log::warn!(
                "ignoring a fee rate of 0 sat/KB; keeping {} sat/KB",
                self.fee_satoshis_per_kb
            );
            return;
        }
        self.fee_satoshis_per_kb = satoshis_per_kb;
    }

    fn estimate_total_cost(&self, fund_request: &FundRequest, no_of_inputs: u32) -> u64 {
        if fund_request.no_of_outpoints > 1 && fund_request.multiple_tx {
            // One transaction per outpoint, each paying its own script, so
            // cost each separately rather than multiplying one estimate.
            (0..fund_request.no_of_outpoints as usize)
                .map(|index| {
                    let script = fund_request.script_at(index).len() as u64;
                    fund_request
                        .satoshi
                        .saturating_add(self.estimate_fee(&[script], 1))
                })
                .sum()
        } else {
            // Saturating: `POST /fund` has already refused a payout over
            // MAX_SATOSHIS, so this cannot overflow from there (CS-530).
            fund_request
                .satoshi
                .saturating_mul(fund_request.no_of_outpoints as u64)
                .saturating_add(self.estimate_fee(&fund_request.script_lengths(), no_of_inputs))
        }
    }

    /// Bytes in a standard P2PKH output script, the shape `POST /fund` pays
    /// to unless a caller asks for something else. Used to answer "how much
    /// can I withdraw" without a request to cost against.
    const P2PKH_SCRIPT_BYTES: u64 = 25;

    /// The most a single `POST /fund` could ask for right now, assuming one
    /// standard P2PKH outpoint.
    ///
    /// Reported on `GET /balance` because the balance alone does not answer
    /// the question a caller actually has. Fees come out of the same UTXOs,
    /// and how the balance is divided changes what it can pay -- so this is
    /// usually well under `confirmed + unconfirmed`, and a caller that
    /// subtracts a guessed fee will guess wrong.
    pub fn max_fundable_p2pkh(&self) -> i64 {
        self.max_fundable(&[Self::P2PKH_SCRIPT_BYTES])
    }

    /// The largest amount a single funding transaction can pay out, given the
    /// UTXOs this client actually holds.
    ///
    /// Not simply "balance minus a fee". Every input added to a transaction
    /// adds bytes and so adds fee, and a small enough input costs more to
    /// spend than it brings in -- the fee steps by 500 satoshi at each
    /// kilobyte, so the input that crosses a boundary can be worth far less
    /// than it costs. Spending everything is therefore often worse than
    /// spending some of it, and the real ceiling is the best any number of
    /// inputs achieves:
    ///
    /// ```text
    /// max over n of ( sum of the n largest UTXOs  -  fee(n inputs) )
    /// ```
    ///
    /// Nothing is taken off for change. Funding this amount leaves none, and
    /// a transaction with no change output is exactly what the builder makes
    /// when the change would not be worth an output. Taking a satoshi off, as
    /// this used to, is what produced the stranded one-satoshi output CS-452
    /// reported: the wallet was drained to a balance it could not spend.
    ///
    /// Returns 0 rather than a negative number when nothing can be funded.
    fn max_fundable(&self, funded_scripts: &[u64]) -> i64 {
        let mut values: Vec<i64> = self.unspent.iter().map(|utxo| utxo.value).collect();
        values.sort_unstable_by(|a, b| b.cmp(a));

        let mut running = 0i64;
        let mut best = 0i64;
        for (index, value) in values.iter().enumerate() {
            running += value;
            let fee = self.estimate_fee(funded_scripts, index as u32 + 1) as i64;
            best = best.max(running - fee);
        }
        best.max(0)
    }

    /// The largest amount that could be funded if this client's balance sat in
    /// a single UTXO.
    ///
    /// The ceiling that no amount of tidying can raise: it costs one input's
    /// worth of fee and nothing more. Comparing against it separates "this
    /// wallet does not hold enough" from "it holds enough but not in a shape
    /// that can be spent", which are the two things a caller can act on and
    /// which need different actions -- top up, or consolidate.
    fn max_fundable_if_consolidated(&self, funded_scripts: &[u64]) -> i64 {
        let total = self.total_unspent();
        let fee = self.estimate_fee(funded_scripts, 1) as i64;
        (total - fee).max(0)
    }

    fn count_utxos_above(&self, amount: u64) -> usize {
        self.unspent
            .iter()
            .filter(|utxo| utxo.value > amount as i64)
            .count()
    }

    /// Select UTXO indices whose combined value covers the estimated cost, preferring fewer inputs.
    fn select_utxo_indices(&self, fund_request: &FundRequest) -> Option<Vec<usize>> {
        let mut sorted: Vec<usize> = (0..self.unspent.len()).collect();
        sorted.sort_by_key(|&index| std::cmp::Reverse(self.unspent[index].value));

        let mut selected = Vec::new();
        for index in sorted {
            selected.push(index);
            let input_sum: i64 = selected.iter().map(|&i| self.unspent[i].value).sum();
            let total_cost = self.estimate_total_cost(fund_request, selected.len() as u32) as i64;
            // `>=` again, not the `>` CS-422 needed. That was there because
            // the builder rejected a change output of zero; it now leaves the
            // change out entirely when it would be dust, so a set covering the
            // cost exactly is one it can build -- as a transaction with no
            // change output at all (CS-452).
            if input_sum >= total_cost {
                return Some(selected);
            }
        }
        None
    }

    fn outpoint_from_utxo(unspent: &UtxoEntry) -> Result<OutPoint, String> {
        Ok(OutPoint {
            hash: Hash256::decode(&unspent.tx_hash)
                .map_err(|e| format!("Invalid UTXO tx hash: {e}"))?,
            index: unspent.tx_pos,
        })
    }

    fn funding_outputs(
        &self,
        fund_request: &FundRequest,
        change: i64,
        change_script: &Script,
    ) -> Vec<TxOut> {
        // Change worth less than the dust threshold gets no output at all: the
        // amount stays in the transaction and the miner takes it as fee. An
        // output of a satoshi or two cannot be spent for less than it holds,
        // so paying it back only strands it -- which is what funding the
        // advertised maximum used to do (CS-452).
        //
        // The funded outputs are therefore always the *last* `no_of_outpoints`
        // of the transaction, and the caller must not assume they start at
        // index 1.
        let mut vouts = Vec::with_capacity(fund_request.no_of_outpoints as usize + 1);
        if self.change_is_worth_paying(change) {
            vouts.push(TxOut {
                satoshis: change,
                lock_script: change_script.clone(),
            });
        }

        // One output per requested outpoint, each with its own script, so N
        // outpoints from one request can be independently spendable.
        for index in 0..fund_request.no_of_outpoints as usize {
            let mut script_pubkey = Script::new();
            script_pubkey.append_slice(fund_request.script_at(index));
            vouts.push(TxOut {
                satoshis: fund_request.satoshi as i64,
                lock_script: script_pubkey,
            });
        }
        vouts
    }

    fn sign_funding_tx_inputs(
        &self,
        tx: &mut Tx,
        input_amounts: &[i64],
        change_script: &Script,
        sighash_flags: u8,
    ) -> Result<(), String> {
        for (index, amount) in input_amounts.iter().enumerate() {
            let sighash = create_sighash(tx, index, change_script, *amount, sighash_flags)
                .map_err(|e| format!("Failed to create sighash: {e}"))?;
            let signature = self
                .wallet
                .sign_sighash(sighash, sighash_flags)
                .map_err(|e| format!("Failed to sign transaction: {e}"))?;
            tx.inputs[index].unlock_script = self.wallet.create_unlock_script(&signature);
        }
        Ok(())
    }

    /// Take `spent` out of the cache, by outpoint, and add `change_entry`.
    ///
    /// Idempotent in `spent`: removing an outpoint already gone is a no-op,
    /// which is what lets a commit follow a claim that has already taken the
    /// inputs out.
    fn remove_spent(&mut self, spent: &[UtxoEntry], change_entry: Option<UtxoEntry>) {
        let spent: std::collections::HashSet<OutPointKey> =
            spent.iter().map(outpoint_key).collect();
        self.unspent
            .retain(|utxo| !spent.contains(&outpoint_key(utxo)));
        // No entry when the change was dust and went to the fee (CS-452).
        if let Some(change_entry) = change_entry {
            self.unspent.push(change_entry);
        }
        self.unspent.sort_by_key(|utxo| utxo.value);
    }

    /// Return a coded error when the client cannot fund the request.
    ///
    /// The two modes cost quite differently -- one transaction spending as
    /// many inputs as it needs, or one transaction per outpoint each spending
    /// a single input -- so they are judged separately rather than through one
    /// estimate that suits neither.
    ///
    /// A refusal that the change of requests still broadcasting would lift is
    /// reported as [`ErrorCode::FundsInFlight`] instead (CS-475): the caller
    /// only has to wait, where the other codes need an operator to act. One it
    /// would not lift is judged as the wallet will stand once they settle.
    pub fn funding_balance_error(&self, fund_request: &FundRequest) -> Option<CodedError> {
        let error = self.funding_balance_error_now(fund_request)?;

        let now = Instant::now();
        let returning: Vec<UtxoEntry> = self
            .in_flight_change
            .values()
            .filter(|claim| now.duration_since(claim.since) < UNCERTAIN_SPEND_RESERVATION)
            .map(|claim| claim.entry.clone())
            .collect();
        if returning.is_empty() {
            return Some(error);
        }

        // Judged by the same rules as the refusal, against the cache as it
        // will be once the claims ahead of this request are settled. A clone
        // rather than a second set of checks over a borrowed UTXO list, so the
        // two verdicts cannot drift apart; it is taken only on this path.
        let returning_satoshi: i64 = returning.iter().map(|entry| entry.value).sum();
        let requests = returning.len();
        let mut settled = self.clone();
        settled.unspent.extend(returning);
        // Still refused once everything in flight has settled: the wallet
        // really is short, and the settled verdict is the one to act on. The
        // unsettled one can be "no UTXOs available" for a wallet whose UTXOs
        // are merely all claimed, which is the confusion CS-475 is about.
        if let Some(settled_error) = settled.funding_balance_error_now(fund_request) {
            return Some(settled_error);
        }
        Some(CodedError::new(
            ErrorCode::FundsInFlight,
            format!(
                "This client's funds are held by {requests} funding request(s) still being \
                 broadcast. The {returning_satoshi} satoshi of change they return when they \
                 complete covers this request, so retry shortly; nothing needs topping up."
            ),
        ))
    }

    /// Whether the cache as it stands, with nothing in flight counted, can
    /// fund the request.
    fn funding_balance_error_now(&self, fund_request: &FundRequest) -> Option<CodedError> {
        if self.unspent.is_empty() {
            return Some(CodedError::new(
                ErrorCode::NoSuitableUtxo,
                "No UTXOs available for funding.",
            ));
        }

        if fund_request.no_of_outpoints > 1 && fund_request.multiple_tx {
            return self.multiple_tx_funding_error(fund_request);
        }
        self.single_tx_funding_error(fund_request)
    }

    /// Whether one transaction can pay every requested outpoint.
    ///
    /// Judged against what this UTXO set can actually pay out, not against a
    /// single-input estimate: the fee depends on how many inputs the
    /// transaction ends up spending, so an estimate assuming one input
    /// understates the cost of every wallet that needs more, and reports a
    /// requirement the caller cannot act on.
    fn single_tx_funding_error(&self, fund_request: &FundRequest) -> Option<CodedError> {
        // What the caller is asking the transaction to pay out, change aside.
        let requested = i64::try_from(
            fund_request
                .satoshi
                .saturating_mul(fund_request.no_of_outpoints as u64),
        )
        .unwrap_or(i64::MAX);
        let scripts = fund_request.script_lengths();
        let total_available = self.total_unspent();

        // Two ceilings, and the gap between them is the diagnosis. The first
        // is what this balance could pay out if it sat in a single UTXO; the
        // second is what it can pay out as it actually sits. Asking above the
        // first needs more money. Asking between them needs the same money in
        // fewer pieces. The caller can act on either, and they are different
        // actions.
        let consolidated_max = self.max_fundable_if_consolidated(&scripts);
        let actual_max = self.max_fundable(&scripts);

        if requested > consolidated_max {
            return Some(CodedError::new(
                ErrorCode::InsufficientBalance,
                format!(
                    "Insufficient client balance: {requested} satoshi requested, but of the \
                     {total_available} satoshi available at most {consolidated_max} can be paid \
                     out once fees are covered."
                ),
            ));
        }

        if requested > actual_max {
            return Some(CodedError::new(
                ErrorCode::NoSuitableUtxo,
                format!(
                    "Unable to fund {requested} satoshi from this UTXO set: at most {actual_max} \
                     can be paid out, because spending more of these {} UTXOs costs more in fees \
                     than the inputs are worth. The balance of {total_available} satoshi would \
                     support up to {consolidated_max} if it were consolidated into one UTXO.",
                    self.unspent.len()
                ),
            ));
        }

        None
    }

    /// Whether a separate transaction can be built for each requested
    /// outpoint.
    ///
    /// Each one spends a single input, so the question is not what the balance
    /// totals but how many individual UTXOs are big enough to carry an
    /// outpoint and its fee on their own.
    fn multiple_tx_funding_error(&self, fund_request: &FundRequest) -> Option<CodedError> {
        // Scripts may differ in size, so require a UTXO large enough for the
        // most expensive of the transactions rather than assuming all are the
        // size of the first.
        let per_tx_cost = (0..fund_request.no_of_outpoints as usize)
            .map(|index| {
                fund_request.satoshi
                    + self.estimate_fee(&[fund_request.script_at(index).len() as u64], 1)
            })
            .max()
            .unwrap_or(fund_request.satoshi);

        let suitable_utxos = self.count_utxos_above(per_tx_cost);
        if suitable_utxos < fund_request.no_of_outpoints as usize {
            let total_available = self.total_unspent();
            return Some(CodedError::new(
                ErrorCode::NoSuitableUtxo,
                format!(
                    "Not enough UTXOs for {} separate funding transactions: {suitable_utxos} of \
                     the {} UTXOs hold more than the {per_tx_cost} satoshi one transaction needs, \
                     and {} are required. The balance of {total_available} satoshi is not the \
                     constraint; how it is divided is.",
                    fund_request.no_of_outpoints,
                    self.unspent.len(),
                    fund_request.no_of_outpoints
                ),
            ));
        }
        None
    }

    fn create_funding_tx_single_input(
        &self,
        fund_request: &FundRequest,
        unspent: &UtxoEntry,
        total_cost: u64,
    ) -> Result<(Tx, FundingSpendPlan), String> {
        let change_script = self.wallet.get_locking_script();
        let change = unspent.value - total_cost as i64;
        if change < 0 {
            return Err("Insufficient UTXO value for funding transaction.".to_string());
        }

        let mut tx = Tx {
            version: 1,
            inputs: vec![TxIn {
                prev_output: Self::outpoint_from_utxo(unspent)?,
                unlock_script: Script::new(),
                sequence: 0xffffffff,
            }],
            outputs: self.funding_outputs(fund_request, change, &change_script),
            lock_time: 0,
        };

        let sighash_flags = SIGHASH_ALL | SIGHASH_FORKID;
        self.sign_funding_tx_inputs(&mut tx, &[unspent.value], &change_script, sighash_flags)?;

        let change_entry = self.change_is_worth_paying(change).then(|| UtxoEntry {
            // Just built and not yet broadcast, let alone mined. Recording it
            // as height 0 said "confirmed in block 0" under chain-gang's
            // convention, which put the change on the wrong side of every
            // confirmed/unconfirmed split that reads it.
            height: UNCONFIRMED_HEIGHT,
            tx_pos: 0,
            tx_hash: tx.hash().encode(),
            value: change,
        });

        let txid = tx.hash().encode();
        Ok((
            tx,
            FundingSpendPlan {
                txid,
                change_entry,
                spent_outpoints: vec![unspent.clone()],
            },
        ))
    }

    fn create_funding_tx_multi_input(
        &self,
        fund_request: &FundRequest,
        selected_indices: &[usize],
    ) -> Result<(Tx, FundingSpendPlan), String> {
        let change_script = self.wallet.get_locking_script();
        let input_amounts: Vec<i64> = selected_indices
            .iter()
            .map(|&index| self.unspent[index].value)
            .collect();
        let input_sum: i64 = input_amounts.iter().sum();
        let total_cost =
            self.estimate_total_cost(fund_request, selected_indices.len() as u32) as i64;
        let change = input_sum - total_cost;
        if change < 0 {
            return Err("Insufficient UTXO value for funding transaction.".to_string());
        }

        let inputs = selected_indices
            .iter()
            .map(|&index| {
                Ok(TxIn {
                    prev_output: Self::outpoint_from_utxo(&self.unspent[index])?,
                    unlock_script: Script::new(),
                    sequence: 0xffffffff,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;

        let mut tx = Tx {
            version: 1,
            inputs,
            outputs: self.funding_outputs(fund_request, change, &change_script),
            lock_time: 0,
        };

        let sighash_flags = SIGHASH_ALL | SIGHASH_FORKID;
        self.sign_funding_tx_inputs(&mut tx, &input_amounts, &change_script, sighash_flags)?;

        let change_entry = self.change_is_worth_paying(change).then(|| UtxoEntry {
            // As above: unconfirmed until it is mined.
            height: UNCONFIRMED_HEIGHT,
            tx_pos: 0,
            tx_hash: tx.hash().encode(),
            value: change,
        });

        let txid = tx.hash().encode();
        Ok((
            tx,
            FundingSpendPlan {
                txid,
                change_entry,
                spent_outpoints: selected_indices
                    .iter()
                    .filter_map(|index| self.unspent.get(*index).cloned())
                    .collect(),
            },
        ))
    }

    /// Plan a funding transaction without updating the local UTXO cache.
    pub fn plan_funding_tx(
        &self,
        fund_request: &FundRequest,
    ) -> Result<(Tx, FundingSpendPlan), String> {
        let total_cost_single = self.estimate_total_cost(fund_request, 1);
        if let Some(unspent) = self.get_smallest_unspent(total_cost_single) {
            let unspent = unspent.clone();
            return self.create_funding_tx_single_input(fund_request, &unspent, total_cost_single);
        }

        let selected_indices = self
            .select_utxo_indices(fund_request)
            .ok_or_else(|| "No suitable UTXO set available for funding transaction.".to_string())?;
        self.create_funding_tx_multi_input(fund_request, &selected_indices)
    }

    /// Commit a spend whose transaction was broadcast successfully.
    ///
    /// The inputs are reserved, not merely dropped from the cache. Dropping
    /// them is undone by the very next refresh: the transaction is in the
    /// mempool and the read interface still reports its inputs as unspent, so
    /// the cache takes them back and the next funding request selects the same
    /// one -- building the identical transaction and handing a second caller
    /// an outpoint that already belongs to the first. Three requests a second
    /// apart were enough to see it.
    ///
    /// The change output is pinned for the same reason in reverse. It is real
    /// -- the transaction carrying it was broadcast -- but it is not on chain
    /// yet either, so a refresh would drop it and leave the client unable to
    /// spend its own change until the chain caught up.
    ///
    /// Refuses a transaction whose outpoints have already been handed to a
    /// caller (CS-474). Nothing a correct service does builds the same
    /// transaction twice -- claiming inputs at planning keeps concurrent
    /// requests off each other's -- so this is an invariant check. But the
    /// failure it guards against is silent everywhere else: the upstream
    /// answers a transaction it already holds with success, so without it a
    /// second caller would be told it owns outpoints someone else was already
    /// given, and would find out only when its own transaction failed.
    pub fn commit_funding_spend(&mut self, plan: FundingSpendPlan) -> Result<(), String> {
        // Settled either way: committed, its change is in the cache below; if
        // refused, there is no longer a request in flight to wait for.
        self.in_flight_change.remove(&plan.txid);
        self.forget_expired_handed_out();
        if self.handed_out.contains_key(&plan.txid) {
            log::error!(
                "refusing to hand out the outpoints of {} a second time: they already belong to \
                 another caller. Two funding requests built the same transaction, which should \
                 be impossible (CS-474).",
                plan.txid
            );
            return Err(format!(
                "transaction {} was already handed to another caller",
                plan.txid
            ));
        }
        // Change a request has already claimed stays out (CS-691). The node can
        // accept this transaction before the service hears that it did, so a
        // refresh in between can list the change and a request claim it; put
        // back now, it would be offered again while that claim's broadcast is
        // still running. Its claim, commit or reservation settles it from here.
        let change_entry = plan
            .change_entry
            .filter(|change| !self.reserved.contains_key(&outpoint_key(change)));
        self.remove_spent(&plan.spent_outpoints, change_entry.clone());
        let now = Instant::now();
        self.record_spent_change(&plan.txid, &plan.spent_outpoints, now);
        self.handed_out.insert(plan.txid, now);
        for entry in plan.spent_outpoints {
            self.reserved.insert(outpoint_key(&entry), now);
        }
        // A transaction whose change went to the fee has no change output to
        // pin, so there is nothing to hold across a refresh (CS-452).
        if let Some(change_entry) = change_entry {
            self.pending_change.insert(
                outpoint_key(&change_entry),
                PendingChange {
                    entry: change_entry,
                    since: now,
                },
            );
        }
        Ok(())
    }

    /// Drop transactions handed out longer ago than the reservation window.
    ///
    /// Past it the inputs have been released too, so a transaction built from
    /// them would be a new one, not a duplicate.
    fn forget_expired_handed_out(&mut self) {
        let now = Instant::now();
        self.handed_out
            .retain(|_, since| now.duration_since(*since) < UNCERTAIN_SPEND_RESERVATION);
    }

    /// Take a plan's inputs out of what any other request can select, before
    /// its transaction is broadcast (CS-473).
    ///
    /// A broadcast takes as long as the network does, and a plan used to hold
    /// nothing while it ran: every request for the client planned against the
    /// same cache, picked the same smallest suitable UTXO, and all but one of
    /// the transactions they built were refused as conflicts. Claiming at
    /// planning time, under the same exclusive section that planned, means a
    /// concurrent request never sees an input another is spending.
    ///
    /// The inputs are reserved as well as removed, so that a refresh while the
    /// broadcast is in flight does not put them back. A claim is then
    /// committed ([`Self::commit_funding_spend`]), kept as a reservation when
    /// the outcome is unknown ([`Self::commit_uncertain_funding_spend`]), or
    /// given back ([`Self::release_claim`]).
    pub fn claim_inputs(&mut self, plan: &FundingSpendPlan) {
        self.remove_spent(&plan.spent_outpoints, None);
        let now = Instant::now();
        for entry in &plan.spent_outpoints {
            self.reserved.insert(outpoint_key(entry), now);
        }
        // Own change among the inputs is held until the chain is seen to know
        // of this transaction, as a committed spend of it is (SR-FUND-033),
        // and from the claim on, not only once the broadcast has settled: a
        // broadcast can take seconds, and a refresh in that time cannot tell
        // "not listed yet" from "spent", so it released the claim's hold and a
        // later one listed the change as unspent again (CS-691).
        self.record_spent_change(&plan.txid, &plan.spent_outpoints, now);
        // A claim is normally settled within a broadcast. One never settled --
        // its request dropped mid-flight -- is forgotten at the same age its
        // reservation is.
        self.in_flight_change
            .retain(|_, claim| now.duration_since(claim.since) < UNCERTAIN_SPEND_RESERVATION);
        if let Some(change) = &plan.change_entry {
            self.in_flight_change.insert(
                plan.txid.clone(),
                PendingChange {
                    entry: change.clone(),
                    since: now,
                },
            );
        }
    }

    /// Give back a plan's inputs after a broadcast the upstream definitely did
    /// not take: nothing was spent, so they are spendable again at once.
    ///
    /// Put back in the cache now rather than left for the next refresh, which
    /// may be a whole freshness window away. The service's own change among
    /// them is pending again too, so a refresh from an interface that has not
    /// listed it yet does not lose it (CS-691).
    ///
    /// Except when `spent_elsewhere`: the refusal was a conflict, the input
    /// spent by another transaction. The caller then marks the chain state
    /// stale, and the next refresh drops what the chain no longer lists. Own
    /// change is not made pending for that reason: pending change is put back
    /// after every refresh whether listed or not, so change the conflicting
    /// transaction spent would be offered, and refused, until the reservation
    /// window ran out.
    ///
    /// Returns how many own-change outputs were made pending again, so the
    /// caller knows the in-flight state needs saving (CS-465).
    pub fn release_claim(&mut self, plan: &FundingSpendPlan, spent_elsewhere: bool) -> usize {
        self.in_flight_change.remove(&plan.txid);
        // Nothing was spent, so there is no spend for the chain to catch up
        // with (CS-691).
        self.spent_change.remove(&plan.txid);
        let now = Instant::now();
        let mut repended = 0;
        for entry in &plan.spent_outpoints {
            let key = outpoint_key(entry);
            self.reserved.remove(&key);
            if !self.unspent.iter().any(|utxo| outpoint_key(utxo) == key) {
                self.unspent.push(entry.clone());
            }
            // Own change is pending again: a refresh while the claim held it
            // dropped it from the pending change, and the chain may not list
            // it yet, so without this the next refresh would lose it until
            // the chain caught up (CS-691).
            if !spent_elsewhere
                && self.handed_out.contains_key(&entry.tx_hash)
                && !self.pending_change.contains_key(&key)
            {
                self.pending_change.insert(
                    key,
                    PendingChange {
                        entry: entry.clone(),
                        since: now,
                    },
                );
                repended += 1;
            }
        }
        self.unspent.sort_by_key(|utxo| utxo.value);
        repended
    }

    /// Forget the service's own pending change that a refused plan spent, after
    /// the node refused it as missing inputs, and any pending change built on
    /// it (CS-506, SR-FUND-036). Returns how many outputs were forgotten.
    ///
    /// Call after [`Self::release_claim`], which puts every input back. That
    /// is right for an input the chain reported -- a refresh will drop it
    /// again if it was the missing one -- but not for the service's own
    /// change: the chain has never listed it, so no refresh can say it is
    /// gone, and [`Self::restore_pending_change`] put it back after every one.
    /// Its creating transaction was dropped by the network (a conflict, a
    /// reorg, an eviction), so it does not exist, and with a single chain of
    /// change every later request selected it and was refused the same way
    /// until it expired -- 95 refusals in a row on the regtest bench.
    ///
    /// "Built on it" follows [`SpentChange`]: a transaction of ours that spent
    /// a missing output cannot exist either, so neither can its change.
    pub fn forget_refused_own_change(&mut self, plan: &FundingSpendPlan) -> usize {
        let mut doomed: Vec<OutPointKey> = plan
            .spent_outpoints
            .iter()
            .map(outpoint_key)
            .filter(|key| self.pending_change.contains_key(key))
            .collect();
        let mut forgotten: std::collections::HashSet<OutPointKey> =
            std::collections::HashSet::new();
        while let Some(key) = doomed.pop() {
            if !forgotten.insert(key.clone()) {
                continue;
            }
            self.pending_change.remove(&key);
            self.reserved.remove(&key);
            self.unspent.retain(|utxo| outpoint_key(utxo) != key);
            log::warn!(
                "forgetting own change {}:{}: the node refused a spend of it as missing inputs, \
                 so the transaction that created it is not on the network",
                key.0,
                key.1
            );

            let spenders: Vec<String> = self
                .spent_change
                .iter()
                .filter(|(_, spent)| spent.outpoints.contains(&key))
                .map(|(txid, _)| txid.clone())
                .collect();
            for txid in spenders {
                self.spent_change.remove(&txid);
                // Its outputs: change still pending, or change it in turn
                // spent, which is held as a reservation.
                doomed.extend(
                    self.pending_change
                        .keys()
                        .chain(self.reserved.keys())
                        .filter(|(created_by, _)| *created_by == txid)
                        .cloned(),
                );
            }
        }
        forgotten.len()
    }

    /// Commit a spend whose transaction may or may not have reached the
    /// network, reserving its inputs so no later refresh offers them again.
    ///
    /// Pessimistic on purpose. The two ways of being wrong are not equal: if
    /// the transaction landed and this service kept treating the inputs as
    /// spendable, the next funding request would build a conflicting
    /// transaction and be refused by the network -- a failure the service
    /// creates for itself and cannot detect. If it never landed, the inputs
    /// sit idle until the reservation expires and the next refresh restores
    /// them. Idle funds heal; a double spend does not.
    ///
    /// The change output is deliberately *not* added to the cache: it exists
    /// only if the transaction landed, and unlike the inputs, wrongly counting
    /// it would have the service try to spend an output that may not exist.
    pub fn commit_uncertain_funding_spend(&mut self, plan: FundingSpendPlan) {
        // No longer a request that will settle shortly: its change may never
        // exist, and its inputs are held for the whole reservation window, so
        // a request refused behind it is not told to retry in a second.
        self.in_flight_change.remove(&plan.txid);
        self.remove_spent(&plan.spent_outpoints, None);

        let now = Instant::now();
        // It may be on the network, so another request building the same
        // transaction would be a duplicate of it just the same.
        self.forget_expired_handed_out();
        self.record_spent_change(&plan.txid, &plan.spent_outpoints, now);
        self.handed_out.insert(plan.txid.clone(), now);
        for entry in plan.spent_outpoints {
            log::warn!(
                "reserving outpoint {}:{} for up to {}s: its funding transaction was handed to \
                 the broadcaster and the outcome is unknown",
                entry.tx_hash,
                entry.tx_pos,
                UNCERTAIN_SPEND_RESERVATION.as_secs()
            );
            self.reserved.insert(outpoint_key(&entry), now);
        }
    }

    /// Create one funding transaction and update the local UTXO cache.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn create_funding_tx(&mut self, fund_request: &FundRequest) -> Result<Tx, String> {
        let (tx, plan) = self.plan_funding_tx(fund_request)?;
        self.commit_funding_spend(plan)?;
        Ok(tx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::ClientConfig,
        test_support::{
            test_blockchain_interface, test_config, LOCKING_SCRIPT_HEX, TEST_CLIENT_ID,
        },
        util::tx_as_hexstr,
    };

    #[tokio::test]
    async fn test_create_tx() {
        let config = test_config("/tmp/financing-service-client-test-dynamic.toml");

        let blockchain_interface = test_blockchain_interface(&config).await;

        let client_config = config.client.unwrap();
        let mut client = Client::try_new(&client_config[0], "regtest").unwrap();

        let result = client.update_balance(&*blockchain_interface).await;
        assert!(result.is_ok());

        let locking_script = hex::decode(LOCKING_SCRIPT_HEX).unwrap();

        let fund_request = FundRequest {
            client_id: TEST_CLIENT_ID.to_string(),
            satoshi: 123,
            no_of_outpoints: 1,
            multiple_tx: false,
            locking_scripts: vec![locking_script],
        };
        let tx = client.create_funding_tx(&fund_request).unwrap();

        // Stated before the serialised form below, so that a change of fee
        // rate fails here with a number rather than as a wall of hex.
        //
        // At 100 sat/KB a one-input, two-output transaction is 226 bytes and
        // costs 23 satoshi, so the smallest UTXO that can pay 123 and still
        // leave change is the 240. Its change of 94 clears the 38 that rate
        // implies as dust, so it is paid back rather than given away.
        assert_eq!(tx.inputs.len(), 1, "one input suffices at this rate");
        assert_eq!(tx.outputs.len(), 2);
        assert_eq!(tx.outputs[0].satoshis, 94, "change: 240 - 123 - 23");
        assert_eq!(tx.outputs[1].satoshis, 123, "the requested amount");

        assert_eq!(
            tx_as_hexstr(&tx).unwrap(),
            "01000000015e791b771be3af3ed1447d311071a1e15e127c4343a58debcb8e40c1e57272f6000000006a473044022026bc38b2a528e18e009ad4cc30673c9b0ac9a101c4d034342caca6b34d1db84e022053c9aa99d88d47050df5828498c1101d1ece941276c71427850209091bc9bed9412103a8ae071ddd8690b94755c7112ca304bcac45c15904cc013f0ad6c2ea0b1019b2ffffffff025e000000000000001976a914ddc574807c3035ab43553a22c0b9df1f55737fae88ac7b000000000000001976a914ddc574807c3035ab43553a22c0b9df1f55737fae88ac00000000"
        );
    }

    #[tokio::test]
    async fn test_invalid_wif_key() {
        let client_config = ClientConfig {
            client_id: TEST_CLIENT_ID.to_string(),
            wif_key: "EGa6cZHpfLZmUzXbkvq72s15rbiUonkrQAhDU4FG".to_string(),
            api_key: None,
        };
        assert!(Client::try_new(&client_config, "regtest").is_err());
    }

    #[tokio::test]
    async fn test_create_funding_tx_no_utxo() {
        let client_config = ClientConfig {
            client_id: TEST_CLIENT_ID.to_string(),
            wif_key: crate::test_support::TEST_WIF.to_string(),
            api_key: None,
        };
        let mut client = Client::try_new(&client_config, "regtest").unwrap();
        let fund_request = FundRequest {
            client_id: TEST_CLIENT_ID.to_string(),
            satoshi: 123,
            no_of_outpoints: 1,
            multiple_tx: false,
            locking_scripts: vec![hex::decode(LOCKING_SCRIPT_HEX).unwrap()],
        };

        let result = client.create_funding_tx(&fund_request);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("No suitable UTXO"));
    }

    fn test_client_with_utxos(values: &[i64]) -> Client {
        use chain_gang::interface::UtxoEntry;

        let client_config = ClientConfig {
            client_id: TEST_CLIENT_ID.to_string(),
            wif_key: crate::test_support::TEST_WIF.to_string(),
            api_key: None,
        };
        let mut client = Client::try_new(&client_config, "regtest").unwrap();
        client.unspent = values
            .iter()
            .enumerate()
            .map(|(index, value)| UtxoEntry {
                height: 1,
                tx_pos: 0,
                tx_hash: format!("{:064x}", index + 1),
                value: *value,
            })
            .collect();
        client.unspent.sort_by_key(|utxo| utxo.value);
        client
    }

    fn sample_fund_request(satoshi: u64) -> FundRequest {
        FundRequest {
            client_id: TEST_CLIENT_ID.to_string(),
            satoshi,
            no_of_outpoints: 1,
            multiple_tx: false,
            locking_scripts: vec![hex::decode(LOCKING_SCRIPT_HEX).unwrap()],
        }
    }

    /// CS-619: the oldest confirmed UTXO, unconfirmed ones left out.
    #[test]
    fn cs_619_oldest_confirmed_utxo_skips_unconfirmed_outputs() {
        let mut client = Client::try_new(
            &crate::test_support::test_config(&crate::test_support::unique_dynamic_config_path())
                .client
                .unwrap()[0],
            "regtest",
        )
        .unwrap();
        let utxo = |height, seed: char| UtxoEntry {
            height,
            tx_pos: 0,
            tx_hash: seed.to_string().repeat(64),
            value: 1_000,
        };
        assert_eq!(client.oldest_confirmed_utxo(), None);
        client.apply_chain_state(vec![
            utxo(-1, 'u'),
            utxo(130, 'b'),
            utxo(120, 'a'),
            utxo(0, 'z'),
        ]);
        assert_eq!(client.oldest_confirmed_utxo(), Some(("a".repeat(64), 120)));
        client.apply_chain_state(vec![utxo(-1, 'u')]);
        assert_eq!(client.oldest_confirmed_utxo(), None);
    }

    #[test]
    fn test_funding_balance_error_insufficient_total() {
        let client = test_client_with_utxos(&[100]);
        let error = client
            .funding_balance_error(&sample_fund_request(123))
            .unwrap();
        assert_eq!(error.code, ErrorCode::InsufficientBalance);
        assert!(error.description.contains("Insufficient client balance"));
        assert!(error.description.contains("100 satoshi available"));
    }

    #[test]
    fn test_funding_balance_error_consolidates_multiple_utxos() {
        let client = test_client_with_utxos(&[300, 300, 300]);
        // More than any one UTXO holds, so it can only be funded by combining
        // them -- which is what this is here to check.
        assert!(client
            .funding_balance_error(&sample_fund_request(700))
            .is_none());
    }

    #[test]
    fn test_create_funding_tx_consolidates_multiple_utxos() {
        let mut client = test_client_with_utxos(&[300, 300, 300]);
        // 700 exceeds any single UTXO, so all three are needed. (123 would now
        // be met by one of them: at 100 sat/KB the fee is 23, not 750.)
        let tx = client
            .create_funding_tx(&sample_fund_request(700))
            .expect("expected multi-input funding transaction");
        assert_eq!(tx.inputs.len(), 3);
        assert_eq!(tx.outputs.len(), 2);
    }

    #[test]
    fn test_funding_balance_error_sufficient() {
        let client = test_client_with_utxos(&[100, 10_000]);
        assert!(client
            .funding_balance_error(&sample_fund_request(123))
            .is_none());
    }

    #[test]
    fn test_funding_balance_error_multiple_tx_not_enough_utxos() {
        let client = test_client_with_utxos(&[10_000, 10_000]);
        let fund_request = FundRequest {
            client_id: TEST_CLIENT_ID.to_string(),
            satoshi: 123,
            no_of_outpoints: 3,
            multiple_tx: true,
            locking_scripts: vec![hex::decode(LOCKING_SCRIPT_HEX).unwrap()],
        };
        let error = client.funding_balance_error(&fund_request).unwrap();
        assert_eq!(error.code, ErrorCode::NoSuitableUtxo);
        assert!(error.description.contains("Not enough UTXOs"));
        // 2 UTXOs are big enough, 3 transactions are wanted
        assert!(
            error.description.contains("2 of the 2 UTXOs"),
            "{}",
            error.description
        );
        assert!(
            error.description.contains("3 are required"),
            "{}",
            error.description
        );
        // and the caller is told the balance is not what is short
        assert!(
            error.description.contains("how it is divided"),
            "{}",
            error.description
        );
    }

    #[test]
    fn sr_fund_001_funding_tx_uses_supplied_locking_script() {
        let client = test_client_with_utxos(&[50_000]);
        let fund_request = sample_fund_request(1_000);
        let (tx, _) = client.plan_funding_tx(&fund_request).unwrap();
        let tx_hex = tx_as_hexstr(&tx).unwrap();
        assert!(tx_hex.contains(LOCKING_SCRIPT_HEX));
    }

    #[test]
    fn sr_fund_010_planning_is_pure_and_a_claim_keeps_the_next_plan_off_its_inputs() {
        let mut client = test_client_with_utxos(&[50_000, 40_000]);
        let fund_request = sample_fund_request(1_000);
        // Planning alone changes nothing: two plans against the same cache
        // choose the same input. That is why a plan on its own is not enough
        // to fund concurrently -- this used to be asserted as the goal.
        let (_, first) = client.plan_funding_tx(&fund_request).unwrap();
        let (_, again) = client.plan_funding_tx(&fund_request).unwrap();
        assert_eq!(first.spent_outpoints, again.spent_outpoints);

        // The claim is what keeps a concurrent plan off them (CS-473).
        client.claim_inputs(&first);
        let (_, next) = client.plan_funding_tx(&fund_request).unwrap();
        assert_ne!(
            first.spent_outpoints, next.spent_outpoints,
            "a plan made after a claim picked the claimed input"
        );
        client.commit_funding_spend(first).expect("commits");
        assert!(client.plan_funding_tx(&fund_request).is_ok());
    }

    // ---- Reserved outpoints after an uncertain broadcast (issue #66) ----

    fn utxo(tx_hash: &str, tx_pos: u32, value: i64, height: i32) -> UtxoEntry {
        UtxoEntry {
            height,
            tx_pos,
            tx_hash: tx_hash.to_string(),
            value,
        }
    }

    /// A client holding exactly `unspent`, with a matching balance.
    fn client_holding(unspent: Utxo) -> Client {
        let client_config = ClientConfig {
            client_id: TEST_CLIENT_ID.to_string(),
            wif_key: crate::test_support::TEST_WIF.to_string(),
            api_key: None,
        };
        let mut client = Client::try_new(&client_config, "regtest").unwrap();
        client.apply_chain_state(unspent);
        client
    }

    /// A wallet holding `unspent`, costing its transactions at `rate`.
    fn client_holding_at_rate(unspent: Utxo, rate: u64) -> Client {
        let mut client = client_holding(unspent);
        client.set_fee_satoshis_per_kb(rate);
        client
    }

    /// The whole point of CS-451: the rate is a number an operator sets, and
    /// changing it changes what the service pays. The superseded expression
    /// could not express any of these.
    #[test]
    fn cs_451_the_fee_follows_the_configured_rate() {
        let script_bytes = Client::P2PKH_SCRIPT_BYTES;
        // One input, two outputs: 8 + 1 + 148 + 1 + 34 + 34 = 226 bytes.
        for (rate, expected) in [(100u64, 23u64), (500, 113), (1000, 226), (50, 12)] {
            let client = client_holding_at_rate(Vec::new(), rate);
            assert_eq!(
                client.estimate_fee(&[script_bytes], 1),
                expected,
                "at {rate} sat/KB"
            );
        }
    }

    /// Rounding is upwards. Rounding down would shave a satoshi off the fee,
    /// and a transaction a miner will not relay costs far more to discover
    /// than the satoshi saved.
    #[test]
    fn cs_451_a_partial_satoshi_rounds_up_rather_than_down() {
        // 226 bytes at 100 sat/KB is 22.6 satoshi
        let client = client_holding_at_rate(Vec::new(), 100);
        assert_eq!(client.estimate_fee(&[Client::P2PKH_SCRIPT_BYTES], 1), 23);

        // and a rate low enough to make the exact fee a fraction of a satoshi
        // still pays one, not none
        let client = client_holding_at_rate(Vec::new(), 1);
        assert_eq!(
            client.estimate_fee(&[Client::P2PKH_SCRIPT_BYTES], 1),
            1,
            "0.226 satoshi rounds to 1, never to 0"
        );
    }

    /// CS-422's insight survives the move to a flat rate, at a different
    /// threshold: an input is worth adding only if it brings in more than the
    /// 148 bytes it costs. The step fee made this obvious at a kilobyte
    /// boundary; a flat rate makes it a question of the input's own value.
    #[test]
    fn cs_451_an_input_worth_less_than_its_own_fee_is_left_out() {
        // At 100 sat/KB an extra input costs ceil(148 * 100 / 1000) = 15, so
        // the 10 is not worth spending and the ceiling ignores it.
        let client =
            client_holding_at_rate(vec![cs_422_utxo(1, 0, 1000), cs_422_utxo(2, 0, 10)], 100);
        assert_eq!(client.total_unspent(), 1010);
        assert_eq!(
            client.max_fundable(&[Client::P2PKH_SCRIPT_BYTES]),
            977,
            "1000 - 23, leaving the 10 alone rather than paying 15 for it"
        );
    }

    /// A quote of zero would build transactions nothing relays. Keeping the
    /// last good rate is better than believing it.
    #[test]
    fn cs_451_a_zero_rate_is_refused_and_the_last_good_rate_stands() {
        let mut client = client_holding_at_rate(Vec::new(), 250);
        client.set_fee_satoshis_per_kb(0);
        assert_eq!(client.fee_satoshis_per_kb(), 250);
    }

    /// Without configuration the service uses the documented default, so the
    /// constant and the built-in agree.
    #[test]
    fn cs_451_a_new_client_starts_at_the_default_rate() {
        assert_eq!(
            client_holding(Vec::new()).fee_satoshis_per_kb(),
            DEFAULT_SATOSHIS_PER_KB
        );
        assert_eq!(DEFAULT_SATOSHIS_PER_KB, 100);
    }

    /// The ticket's transaction: funding the advertised maximum produced two
    /// outputs, the second worth one satoshi, and left the client holding a
    /// balance of 1 that it could never spend. There is now no second output.
    #[test]
    fn cs_452_funding_the_maximum_leaves_no_dust_output() {
        let mut client = client_holding_at_rate(vec![cs_422_utxo(1, 0, 5_000)], 100);
        let script_bytes = Client::P2PKH_SCRIPT_BYTES;
        let max = client.max_fundable(&[script_bytes]) as u64;
        assert_eq!(max, 4_977, "5000 - 23, with nothing held back for change");

        let tx = client
            .create_funding_tx(&cs_422_request(max))
            .expect("the advertised maximum must build");

        assert_eq!(tx.outputs.len(), 1, "no change output at all");
        assert_eq!(tx.outputs[0].satoshis, 4_977);
        let after = client.get_balance();
        assert_eq!(
            after.confirmed + after.unconfirmed,
            0,
            "spent out, not left holding an unspendable satoshi: {after:?}"
        );
    }

    /// Change below the threshold is not paid back as a tiny output; it stays
    /// in the transaction, which means the miner takes it as fee.
    #[test]
    fn cs_452_dust_change_goes_to_the_fee_rather_than_an_output() {
        // 5_030 covers 5_000 plus the 23 fee and leaves 7, under the 38 the
        // rate implies. It is the only UTXO, so there is nothing better.
        let mut client = client_holding_at_rate(vec![cs_422_utxo(1, 0, 5_030)], 100);
        let tx = client
            .create_funding_tx(&cs_422_request(5_000))
            .expect("still fundable");

        assert_eq!(tx.outputs.len(), 1, "the 8 is not worth an output");
        assert_eq!(tx.outputs[0].satoshis, 5_000);

        let paid_out: i64 = tx.outputs.iter().map(|out| out.satoshis).sum();
        assert_eq!(
            5_030 - paid_out,
            30,
            "the whole remainder, fee plus the dust, goes to the miner"
        );
    }

    /// Given a choice, the service does not pick the UTXO that would hand its
    /// change to the miner.
    #[test]
    fn cs_452_a_utxo_that_would_leave_dust_is_not_preferred() {
        // 5_030 leaves 8 of dust; 9_000 leaves 3_977, which is worth an
        // output. The smaller one would otherwise win, being smallest-first.
        let mut client = client_holding_at_rate(
            vec![cs_422_utxo(1, 0, 5_030), cs_422_utxo(2, 0, 9_000)],
            100,
        );
        let tx = client
            .create_funding_tx(&cs_422_request(5_000))
            .expect("fundable");

        assert_eq!(tx.outputs.len(), 2, "change is paid back, not given away");
        assert_eq!(tx.outputs[0].satoshis, 3_977, "9000 - 5000 - 23");
        assert_eq!(client.get_balance().unconfirmed, 3_977);
    }

    /// A UTXO covering the cost exactly is fundable again. CS-422 had to
    /// exclude it because the builder rejected a zero change output; it now
    /// builds one with no change output instead.
    #[test]
    fn cs_452_a_utxo_covering_the_cost_exactly_is_fundable() {
        let exact = 5_000
            + client_holding(Vec::new()).estimate_fee(&[Client::P2PKH_SCRIPT_BYTES], 1) as i64;
        let mut client = client_holding_at_rate(vec![cs_422_utxo(1, 0, exact)], 100);

        let request = cs_422_request(5_000);
        assert!(
            client.funding_balance_error(&request).is_none(),
            "covering the cost exactly is enough"
        );
        let tx = client.create_funding_tx(&request).expect("and it builds");
        assert_eq!(tx.outputs.len(), 1);
    }

    /// The threshold is the round-trip cost of a change output: the 34 bytes
    /// it adds now plus the 148 it costs to spend later, at the rate in force,
    /// doubled for headroom.
    #[test]
    fn cs_452_the_threshold_is_the_round_trip_cost_of_an_output() {
        // 182 bytes at 100 sat/KB is 18.2, rounded up to 19, doubled
        assert_eq!(client_holding_at_rate(Vec::new(), 100).dust_threshold(), 38);
        // and at 500: 91 exactly, doubled
        assert_eq!(
            client_holding_at_rate(Vec::new(), 500).dust_threshold(),
            182
        );
    }

    /// The familiar 546 is this same formula at a fee rate three orders of
    /// magnitude above BSV's. The shape was never wrong; the constant was.
    #[test]
    fn cs_452_the_formula_reproduces_bitcoin_cores_number() {
        let at_core_rate = client_holding_at_rate(Vec::new(), 3_000).dust_threshold();
        assert_eq!(
            at_core_rate / Client::DUST_THRESHOLD_MULTIPLIER as i64,
            546,
            "182 * 3 is Bitcoin Core's dust limit, at Core's relay fee"
        );
    }

    /// Derived, not stored: a client that moves to a new rate -- from a
    /// mapi-lite quote, say -- moves its threshold with it, so the two cannot
    /// drift apart.
    #[test]
    fn cs_452_the_threshold_follows_the_fee_rate() {
        let mut client = client_holding(Vec::new());
        let before = client.dust_threshold();
        client.set_fee_satoshis_per_kb(1_000);
        assert!(
            client.dust_threshold() > before,
            "a dearer fee makes a small output less worth keeping"
        );
        assert_eq!(client.dust_threshold(), 364, "182 at 1000 sat/KB, doubled");
    }

    /// Never zero, however cheap the fee: a change output of nothing is not
    /// an output, and the comparison is `>=`.
    #[test]
    fn cs_452_the_threshold_is_never_zero() {
        assert_eq!(client_holding_at_rate(Vec::new(), 1).dust_threshold(), 2);
    }

    // ---- CS-465: in-flight state across a restart ----

    /// What a client writes out is what a fresh one reads back: the input its
    /// transaction spent, and the change that transaction created.
    #[test]
    fn cs_465_in_flight_state_survives_a_round_trip() {
        let mut client = client_holding_at_rate(vec![cs_422_utxo(1, 0, 5_000)], 100);
        client
            .create_funding_tx(&cs_422_request(10))
            .expect("funds");
        let written = client.inflight_state();
        assert_eq!(written.reserved.len(), 1, "the input it spent");
        assert_eq!(written.pending_change.len(), 1, "the change it created");

        let mut restarted = client_holding_at_rate(Vec::new(), 100);
        restarted.restore_inflight_state(written.clone());
        let read_back = restarted.inflight_state();

        // The moment is stored in whole seconds and recomputed as an age on
        // each side, so it may move by one; everything else must be exact.
        let same = |a: &PersistedReservation, b: &PersistedReservation| {
            a.tx_hash == b.tx_hash
                && a.tx_pos == b.tx_pos
                && a.since_unix.abs_diff(b.since_unix) <= 1
        };
        assert!(same(&written.reserved[0], &read_back.reserved[0]));
        let (w, r) = (&written.pending_change[0], &read_back.pending_change[0]);
        assert_eq!(
            (&w.tx_hash, w.tx_pos, w.value, w.height),
            (&r.tx_hash, r.tx_pos, r.value, r.height)
        );
        assert!(w.since_unix.abs_diff(r.since_unix) <= 1);
    }

    /// Something that expired while the service was down stays expired.
    /// Restoring it would at best be dropped again by the next refresh -- and
    /// at worst, where the clock cannot express an age that long, come back
    /// looking fresh.
    #[test]
    fn cs_465_an_entry_older_than_the_window_is_not_restored() {
        let long_ago = unix_now() - UNCERTAIN_SPEND_RESERVATION.as_secs() - 1;
        let mut client = client_holding(Vec::new());
        client.restore_inflight_state(InflightState {
            reserved: vec![PersistedReservation {
                tx_hash: "aa".to_string(),
                tx_pos: 0,
                since_unix: long_ago,
            }],
            pending_change: vec![PersistedPendingChange {
                tx_hash: "bb".to_string(),
                tx_pos: 0,
                value: 1_000,
                height: UNCONFIRMED_HEIGHT,
                since_unix: long_ago,
            }],
        });
        assert!(client.inflight_state().is_empty(), "expired while down");
    }

    /// A moment in the future -- the wall clock stepped back between runs --
    /// is held rather than dropped. Holding too long idles funds; dropping too
    /// early risks the double spend this exists to prevent.
    #[test]
    fn cs_465_a_moment_in_the_future_is_held_rather_than_dropped() {
        let mut client = client_holding(Vec::new());
        client.restore_inflight_state(InflightState {
            reserved: vec![PersistedReservation {
                tx_hash: "aa".to_string(),
                tx_pos: 0,
                since_unix: unix_now() + 3_600,
            }],
            pending_change: Vec::new(),
        });
        assert_eq!(client.reserved_outpoint_count(), 1);
    }

    // ---- CS-721: a read that began before an invalidation ----

    #[test]
    fn cs_721_a_read_noted_before_an_invalidation_is_applied_but_stale() {
        let mut client = test_client_with_utxos(&[10_000]);
        let chain = client.unspent.clone();
        let window = Duration::from_secs(60);

        let since = client.invalidations();
        client.invalidate_chain_state();
        client.apply_chain_state_read_since(chain.clone(), since);
        assert_eq!(client.unspent, chain, "applied: it is the newest answer");
        assert!(!client.chain_state_is_fresh(window));
        assert!(!client.chain_state_usable());

        let since = client.invalidations();
        client.apply_chain_state_read_since(chain, since);
        assert!(client.chain_state_is_fresh(window));
        assert!(client.chain_state_usable());
    }

    // ---- CS-473: concurrent requests must not share an input ----

    /// Two requests planned against the same cache, as concurrent requests
    /// are, picking different inputs. Committing them in turn must remove the
    /// input each actually spent. Removing by the position an input held when
    /// it was planned goes wrong as soon as the first commit reshuffles the
    /// cache: the second removes whatever now sits there, and the input it
    /// really spent stays offered to the next request.
    #[test]
    fn cs_473_committing_one_plan_after_another_removes_the_inputs_each_spent() {
        let mut client = client_holding_at_rate(
            vec![
                cs_422_utxo(1, 0, 1_000),
                cs_422_utxo(2, 0, 2_000),
                cs_422_utxo(3, 0, 3_000),
            ],
            100,
        );
        // the 2000 is the smallest that covers 1500; the 1000 covers 500
        let (_, a) = client
            .plan_funding_tx(&cs_422_request(1_500))
            .expect("plans");
        let (_, b) = client.plan_funding_tx(&cs_422_request(500)).expect("plans");
        client.commit_funding_spend(a).expect("commits");
        client.commit_funding_spend(b).expect("commits");

        let held: Vec<i64> = client.unspent.iter().map(|u| u.value).collect();
        assert!(
            !held.contains(&2_000),
            "the first plan's input is still offered: {held:?}"
        );
        assert!(
            !held.contains(&1_000),
            "the second plan's input is still offered: {held:?}"
        );
        assert!(
            held.contains(&3_000),
            "an input nobody spent was removed: {held:?}"
        );
    }

    /// A refresh while a claimed input's broadcast is still in flight: the
    /// chain has not seen the transaction, so it still reports the input as
    /// unspent. The claim has to survive that, or the refresh hands the input
    /// to the next request just as if it had never been claimed.
    #[test]
    fn cs_473_a_refresh_during_the_broadcast_does_not_hand_a_claimed_input_back() {
        let chain = vec![cs_422_utxo(1, 0, 1_000), cs_422_utxo(2, 0, 2_000)];
        let mut client = client_holding_at_rate(chain.clone(), 100);
        let (_, claimed) = client.plan_funding_tx(&cs_422_request(500)).expect("plans");
        client.claim_inputs(&claimed);

        client.apply_chain_state(chain);

        let (_, next) = client.plan_funding_tx(&cs_422_request(500)).expect("plans");
        assert_ne!(
            claimed.spent_outpoints, next.spent_outpoints,
            "the refresh put the claimed input back and the next plan took it"
        );
    }

    // ---- CS-474: an outpoint is handed to one caller ----

    /// The same transaction committed twice is refused the second time: its
    /// outpoints already belong to whoever it was first handed to. Nothing
    /// correct builds it twice, but the upstream answers a resubmission with
    /// success, so this is the only place a duplicate could be caught.
    #[test]
    fn cs_474_the_same_transaction_is_never_handed_out_twice() {
        let mut client = client_holding_at_rate(vec![cs_422_utxo(1, 0, 5_000)], 100);
        let (_, plan) = client.plan_funding_tx(&cs_422_request(10)).expect("plans");
        client
            .commit_funding_spend(plan.clone())
            .expect("handed out once");
        let refused = client.commit_funding_spend(plan).expect_err("not twice");
        assert!(
            refused.contains("already handed to another caller"),
            "{refused}"
        );
    }

    /// A transaction whose outcome was unknown may be on the network, so it
    /// counts as handed out too.
    #[test]
    fn cs_474_an_uncertain_transaction_counts_as_handed_out() {
        let mut client = client_holding_at_rate(vec![cs_422_utxo(1, 0, 5_000)], 100);
        let (_, plan) = client.plan_funding_tx(&cs_422_request(10)).expect("plans");
        client.commit_uncertain_funding_spend(plan.clone());
        assert!(client.commit_funding_spend(plan).is_err());
    }

    /// Past the reservation window the inputs have been released as well, so
    /// the record goes with them rather than growing without bound.
    #[test]
    fn cs_474_the_record_of_handed_out_transactions_expires() {
        let mut client = client_holding_at_rate(vec![cs_422_utxo(1, 0, 5_000)], 100);
        let (_, plan) = client.plan_funding_tx(&cs_422_request(10)).expect("plans");
        client
            .commit_funding_spend(plan.clone())
            .expect("handed out");
        client.backdate_handed_out(UNCERTAIN_SPEND_RESERVATION + Duration::from_secs(1));
        client.forget_expired_handed_out();
        assert!(client.handed_out.is_empty());
    }

    // ---- CS-471: the fee is priced on the transaction actually built ----

    /// Serialised size of `tx`, in bytes: what a miner prices.
    fn serialised_size(tx: &Tx) -> u64 {
        crate::util::tx_as_hexstr(tx).unwrap().len() as u64 / 2
    }

    /// Satoshis `tx` pays in fee, given the value of the inputs it spends.
    fn fee_paid(tx: &Tx, inputs_value: i64) -> i64 {
        inputs_value - tx.outputs.iter().map(|out| out.satoshis).sum::<i64>()
    }

    /// The property that matters: a transaction pays at least the configured
    /// rate on its real serialised size. Checked against the bytes, not
    /// against the estimate, because the bug was in the estimate.
    fn assert_pays_the_rate(tx: &Tx, inputs_value: i64, rate: u64) {
        let size = serialised_size(tx);
        let floor = (size * rate).div_ceil(1000) as i64;
        let paid = fee_paid(tx, inputs_value);
        assert!(
            paid >= floor,
            "a {size}-byte transaction pays {paid} satoshi; at {rate} sat/KB it needs {floor} \
             ({:.1} sat/KB paid)",
            paid as f64 * 1000.0 / size as f64
        );
    }

    /// The ticket's transaction: one P2PKH input, a 100-satoshi output and
    /// change. 226 bytes, which at 100 sat/KB needs 23 satoshi; the estimate
    /// counted each funded output as its bare script, came to 217, and paid 22.
    #[test]
    fn cs_471_a_standard_funding_transaction_pays_the_rate_on_its_real_size() {
        let mut client = client_holding_at_rate(vec![cs_422_utxo(1, 0, 9_792_234)], 100);
        let tx = client
            .create_funding_tx(&cs_422_request(100))
            .expect("funds");
        // The ticket's shape. Its size is 225 or 226: a signature is a byte
        // longer or shorter depending on what it signs, and the estimate
        // assumes the longer, so it is an upper bound on either.
        assert_eq!((tx.inputs.len(), tx.outputs.len()), (1, 2));
        assert!((225..=226).contains(&serialised_size(&tx)));
        assert_eq!(
            Client::funding_tx_bytes(&[Client::P2PKH_SCRIPT_BYTES], 1),
            226
        );
        assert_pays_the_rate(&tx, 9_792_234, 100);
    }

    /// Every funded output carries its own value and length prefix, so the
    /// shortfall grew with the number of outpoints.
    #[test]
    fn cs_471_several_outpoints_in_one_transaction_pay_the_rate() {
        let mut client = client_holding_at_rate(vec![cs_422_utxo(1, 0, 1_000_000)], 100);
        let mut request = cs_422_request(100);
        request.no_of_outpoints = 5;
        request.locking_scripts = vec![hex::decode(LOCKING_SCRIPT_HEX).unwrap(); 5];
        let tx = client.create_funding_tx(&request).expect("funds");
        assert_eq!(tx.outputs.len(), 6, "five funded outputs and change");
        assert_pays_the_rate(&tx, 1_000_000, 100);
    }

    /// And a transaction that has to combine inputs, at a rate high enough
    /// that a few bytes' error is several satoshi.
    #[test]
    fn cs_471_a_consolidating_transaction_pays_the_rate() {
        let utxos = vec![
            cs_422_utxo(1, 0, 3_000),
            cs_422_utxo(2, 0, 3_000),
            cs_422_utxo(3, 0, 3_000),
        ];
        let mut client = client_holding_at_rate(utxos, 1_000);
        let tx = client
            .create_funding_tx(&cs_422_request(7_000))
            .expect("funds");
        assert_eq!(tx.inputs.len(), 3);
        assert_pays_the_rate(&tx, 9_000, 1_000);
    }

    /// Every shape, not one. Outpoint counts, rates and wallet sizes are swept
    /// so the number of inputs, the number of outputs and the rounding all
    /// vary, and every transaction built must pay the rate on its real size.
    #[test]
    fn cs_471_every_shape_pays_the_rate_on_its_real_size() {
        for rate in [1u64, 50, 100, 500, 1_000, 5_000] {
            for outpoints in 1..=4u32 {
                for utxo_count in 1..=5u8 {
                    // Equal UTXOs small enough that larger requests need
                    // several of them, large enough that the request fits.
                    let each = 40_000i64;
                    let utxos: Vec<UtxoEntry> = (0..utxo_count)
                        .map(|seed| cs_422_utxo(seed + 1, 0, each))
                        .collect();
                    let total = each * utxo_count as i64;
                    let mut client = client_holding_at_rate(utxos, rate);

                    let mut request = cs_422_request(((total / 2) / outpoints as i64) as u64);
                    request.no_of_outpoints = outpoints;
                    request.locking_scripts =
                        vec![hex::decode(LOCKING_SCRIPT_HEX).unwrap(); outpoints as usize];

                    let Ok(tx) = client.create_funding_tx(&request) else {
                        continue;
                    };
                    let spent = each * tx.inputs.len() as i64;
                    assert_pays_the_rate(&tx, spent, rate);
                    assert!(
                        serialised_size(&tx)
                            <= Client::funding_tx_bytes(
                                &request.script_lengths(),
                                tx.inputs.len() as u32
                            ),
                        "the estimate is an upper bound on the size"
                    );
                }
            }
        }
    }

    /// A script of 253 bytes or more needs a three-byte length prefix, not
    /// one. Rare for a funding output, but a caller chooses the script.
    #[test]
    fn cs_471_a_long_locking_script_is_sized_with_its_longer_prefix() {
        let mut client = client_holding_at_rate(vec![cs_422_utxo(1, 0, 100_000)], 1_000);
        let mut request = cs_422_request(1_000);
        // OP_RETURN and 299 bytes of data: a 300-byte script
        let mut script = vec![0x6a];
        script.extend(std::iter::repeat_n(0u8, 299));
        request.locking_scripts = vec![script];
        let tx = client.create_funding_tx(&request).expect("funds");
        assert_eq!(Client::output_bytes(300), 8 + 3 + 300);
        assert_pays_the_rate(&tx, 100_000, 1_000);
    }

    /// The size arithmetic, prefix by prefix. Tested directly rather than
    /// through a built transaction: a signature is a byte shorter about half
    /// the time and the estimate assumes the longer, so across hundreds of
    /// inputs the estimate is over by more than a hundred bytes -- enough to
    /// hide a two-byte prefix it forgot entirely.
    #[test]
    fn cs_471_the_size_counts_every_length_prefix() {
        let p2pkh = Client::P2PKH_SCRIPT_BYTES;
        // one input, one P2PKH output and change: the ticket's 226
        assert_eq!(Client::funding_tx_bytes(&[p2pkh], 1), 226);
        // a funded output is its script plus a value and a prefix
        assert_eq!(Client::output_bytes(p2pkh), 34);
        // 253 inputs: the count takes three bytes, not one
        assert_eq!(
            Client::funding_tx_bytes(&[p2pkh], 253) - Client::funding_tx_bytes(&[p2pkh], 252),
            Client::INPUT_BYTES + 2
        );
        // 253 outputs, change included: likewise
        assert_eq!(
            Client::funding_tx_bytes(&[p2pkh; 252], 1) - Client::funding_tx_bytes(&[p2pkh; 251], 1),
            Client::output_bytes(p2pkh) + 2
        );
        // a 253-byte script: its length takes three bytes, not one
        assert_eq!(Client::output_bytes(253) - Client::output_bytes(252), 1 + 2);
    }

    /// And a transaction that genuinely has hundreds of inputs still pays the
    /// rate -- the sanity check, not the discriminating one (see above).
    #[test]
    fn cs_471_a_transaction_with_hundreds_of_inputs_pays_the_rate() {
        let utxos: Vec<UtxoEntry> = (0..260u32)
            .map(|pos| utxo(&format!("{:064x}", 7), pos, 100, 1_758_719))
            .collect();
        let mut client = client_holding_at_rate(utxos, 100);
        // 85 satoshi of each 100-satoshi input is left after its own fee, so
        // this needs about 254 of them
        let tx = client
            .create_funding_tx(&cs_422_request(21_600))
            .expect("funds");
        assert!(tx.inputs.len() > 252, "{} inputs", tx.inputs.len());
        assert_pays_the_rate(&tx, 100 * tx.inputs.len() as i64, 100);
    }

    /// Reserve the first cached outpoint, as an uncertain broadcast does.
    fn reserve_first(client: &mut Client) -> UtxoEntry {
        let entry = client.unspent[0].clone();
        client.commit_uncertain_funding_spend(FundingSpendPlan {
            txid: "test".to_string(),
            change_entry: Some(utxo("change", 0, 1, 0)),
            spent_outpoints: vec![entry.clone()],
        });
        entry
    }

    /// The defect in #66. A funding transaction reaches the network, the
    /// deadline fires before the answer does, and the refresh that follows
    /// still sees the input as unspent because the transaction is not
    /// confirmed yet. Without the reservation the cache takes that at face
    /// value and offers the same input to the next funding request, which
    /// builds a transaction the network can only refuse.
    #[test]
    fn sr_fund_012_a_refresh_does_not_resurrect_a_reserved_outpoint() {
        let chain = vec![utxo("aa", 0, 5_000, 100), utxo("bb", 1, 7_000, 100)];
        let mut client = client_holding(chain.clone());
        let reserved = reserve_first(&mut client);

        // the read interface has not seen the spend yet, so it reports both
        client.apply_chain_state(chain);

        assert_eq!(client.reserved_outpoint_count(), 1);
        assert!(
            !client
                .unspent
                .iter()
                .any(|u| u.tx_hash == reserved.tx_hash && u.tx_pos == reserved.tx_pos),
            "the reserved outpoint came back and could be spent again"
        );
        assert_eq!(client.unspent.len(), 1);
    }

    /// The reservation is not a leak: once the chain stops reporting the
    /// outpoint as unspent, the uncertain transaction has landed and there is
    /// nothing left to protect against.
    #[test]
    fn sr_fund_012_a_reservation_is_released_once_the_chain_agrees_it_is_spent() {
        let mut client = client_holding(vec![utxo("aa", 0, 5_000, 100), utxo("bb", 1, 7_000, 100)]);
        reserve_first(&mut client);
        assert_eq!(client.reserved_outpoint_count(), 1);

        // the spend is visible now: "aa:0" is gone and the change has arrived
        client.apply_chain_state(vec![utxo("bb", 1, 7_000, 100), utxo("cc", 0, 4_800, -1)]);

        assert_eq!(client.reserved_outpoint_count(), 0);
        assert_eq!(client.unspent.len(), 2);
    }

    /// The other way the uncertainty resolves: the transaction never landed.
    /// The outpoint is still spendable, so the reservation must let go of it
    /// rather than stranding the funds for the life of the process.
    #[test]
    fn sr_fund_012_a_reservation_expires_so_funds_return_if_nothing_landed() {
        let chain = vec![utxo("aa", 0, 5_000, 100), utxo("bb", 1, 7_000, 100)];
        let mut client = client_holding(chain.clone());
        reserve_first(&mut client);

        client.backdate_reservations(UNCERTAIN_SPEND_RESERVATION + Duration::from_secs(1));
        client.apply_chain_state(chain);

        assert_eq!(client.reserved_outpoint_count(), 0);
        assert_eq!(client.unspent.len(), 2, "the funds came back");
        assert_eq!(client.get_balance().confirmed, 12_000);
    }

    /// Balance and UTXO set have to tell the same story, or `/balance` invites
    /// a funding request that `/fund` then refuses for want of a UTXO.
    #[test]
    fn sr_fund_012_a_reserved_outpoint_is_off_the_balance_too() {
        let chain = vec![utxo("aa", 0, 5_000, 100), utxo("bb", 1, 7_000, 100)];
        let mut client = client_holding(chain.clone());
        reserve_first(&mut client);
        assert_eq!(client.get_balance().confirmed, 7_000);

        // and it stays off across a refresh that still reports it
        client.apply_chain_state(chain);
        assert_eq!(client.get_balance().confirmed, 7_000);
    }

    /// The change output of an uncertain transaction exists only if that
    /// transaction landed. Counting it would have the service try to spend an
    /// output that may never have been created -- the same defect as #66, in
    /// the other direction.
    #[test]
    fn sr_fund_012_an_uncertain_commit_does_not_add_the_change_output() {
        let mut client = client_holding(vec![utxo("aa", 0, 5_000, 100)]);
        client.commit_uncertain_funding_spend(FundingSpendPlan {
            txid: "test".to_string(),
            change_entry: Some(utxo("change", 0, 4_800, 0)),
            spent_outpoints: vec![utxo("aa", 0, 5_000, 100)],
        });
        assert!(client.unspent.is_empty(), "{:?}", client.unspent);
    }

    /// A certain broadcast commits the spend outright, change and all -- and
    /// reserves its inputs too.
    ///
    /// This test used to assert the opposite, that a successful broadcast
    /// reserved nothing. That was wrong, and CS-426 is what it cost: the
    /// inputs were dropped from the cache but not held, so the next refresh
    /// took them straight back from a chain that had not seen the transaction
    /// yet, and the request after that spent them again.
    #[test]
    fn sr_fund_012_a_successful_broadcast_reserves_its_inputs() {
        let mut client = client_holding(vec![utxo("aa", 0, 5_000, 100)]);
        client
            .commit_funding_spend(FundingSpendPlan {
                txid: "test".to_string(),
                change_entry: Some(utxo("change", 0, 4_800, 0)),
                spent_outpoints: vec![utxo("aa", 0, 5_000, 100)],
            })
            .expect("commits");
        assert_eq!(
            client.reserved_outpoint_count(),
            1,
            "the spent input must be held, not merely dropped"
        );
        assert_eq!(client.unspent.len(), 1);
        assert_eq!(client.unspent[0].value, 4_800, "change is spendable");
    }

    // ---- CS-475: "all in flight" is not "the wallet is short" ----

    /// Claim a plan for `satoshi` against the client, as a request that has
    /// planned and is now broadcasting holds it.
    fn claim_one(client: &mut Client, satoshi: u64) -> FundingSpendPlan {
        let (_, plan) = client
            .plan_funding_tx(&cs_422_request(satoshi))
            .expect("plans");
        client.claim_inputs(&plan);
        plan
    }

    /// The ticket's case. The only UTXO is claimed by a request still
    /// broadcasting, and its change would cover the next one: that request is
    /// told to wait, not to top up -- and once the first settles, it funds.
    #[test]
    fn cs_475_a_request_behind_a_claim_is_told_its_funds_are_in_flight() {
        let mut client = client_holding_at_rate(vec![cs_422_utxo(1, 0, 5_000)], 100);
        let first = claim_one(&mut client, 10);

        let refused = client
            .funding_balance_error(&cs_422_request(10))
            .expect("nothing is spendable now");
        assert_eq!(refused.code, ErrorCode::FundsInFlight, "{refused:?}");
        assert!(
            refused.description.contains("1 funding request"),
            "{}",
            refused.description
        );

        client.commit_funding_spend(first).expect("commits");
        assert!(
            client.funding_balance_error(&cs_422_request(10)).is_none(),
            "the change has come back"
        );
    }

    /// A wallet that could not cover the request even with every claim
    /// settled is short, and says so. Only a refusal the returning change
    /// would lift is reported as in flight -- and this one is not reported as
    /// "no UTXOs available" either, which is all the cache can say while its
    /// only UTXO is claimed.
    #[test]
    fn cs_475_a_wallet_short_even_after_its_claims_settle_is_still_short() {
        let mut client = client_holding_at_rate(vec![cs_422_utxo(1, 0, 5_000)], 100);
        claim_one(&mut client, 10);
        let refused = client
            .funding_balance_error(&cs_422_request(10_000))
            .expect("refused");
        assert_eq!(refused.code, ErrorCode::InsufficientBalance, "{refused:?}");
    }

    /// A committed claim is settled: its change is in the cache now, and
    /// counting it as still to come would count it twice. Here the change is
    /// then spent in turn, and a request the wallet cannot cover must be told
    /// so rather than told to wait for change it already has.
    #[test]
    fn cs_475_a_committed_claim_is_no_longer_in_flight() {
        let mut client = client_holding_at_rate(vec![cs_422_utxo(1, 0, 5_000)], 100);
        let first = claim_one(&mut client, 10);
        client.commit_funding_spend(first).expect("commits");
        let second = claim_one(&mut client, 4_900);
        client.commit_funding_spend(second).expect("commits");

        let refused = client
            .funding_balance_error(&cs_422_request(4_000))
            .expect("a few dozen satoshi of change is all that is left");
        assert_eq!(refused.code, ErrorCode::InsufficientBalance, "{refused:?}");
    }

    /// A claim given back after a refused broadcast has nothing left in
    /// flight: its input is spendable at once, so the next request funds.
    #[test]
    fn cs_475_a_released_claim_is_no_longer_in_flight() {
        let mut client = client_holding_at_rate(vec![cs_422_utxo(1, 0, 5_000)], 100);
        let first = claim_one(&mut client, 10);
        client.release_claim(&first, false);
        assert!(client.in_flight_change.is_empty());
        assert!(client.funding_balance_error(&cs_422_request(10)).is_none());
    }

    /// An uncertain outcome holds its inputs for the whole reservation window,
    /// and its change may never exist. A request refused behind it will not
    /// succeed in a second, so it is not told it will.
    #[test]
    fn cs_475_an_uncertain_outcome_is_not_reported_as_in_flight() {
        let mut client = client_holding_at_rate(vec![cs_422_utxo(1, 0, 5_000)], 100);
        let first = claim_one(&mut client, 10);
        client.commit_uncertain_funding_spend(first);
        let refused = client
            .funding_balance_error(&cs_422_request(10))
            .expect("refused");
        assert_ne!(refused.code, ErrorCode::FundsInFlight, "{refused:?}");
    }

    /// A claim whose change went to the fee returns nothing if its broadcast
    /// succeeds, so it cannot promise the next request anything.
    #[test]
    fn cs_475_a_claim_with_no_change_promises_nothing() {
        // 10 and the fee leave less of 60 than the 38 dust threshold
        let mut client = client_holding_at_rate(vec![cs_422_utxo(1, 0, 60)], 100);
        let first = claim_one(&mut client, 10);
        assert!(first.change_entry.is_none(), "the change went to the fee");
        let refused = client
            .funding_balance_error(&cs_422_request(10))
            .expect("refused");
        assert_eq!(refused.code, ErrorCode::NoSuitableUtxo, "{refused:?}");
    }

    /// A claim never settled -- its request dropped mid-broadcast -- stops
    /// counting when its reservation would have expired, rather than telling
    /// callers to retry for ever.
    #[test]
    fn cs_475_a_claim_never_settled_stops_counting() {
        let mut client = client_holding_at_rate(vec![cs_422_utxo(1, 0, 5_000)], 100);
        claim_one(&mut client, 10);
        for claim in client.in_flight_change.values_mut() {
            claim.since -= UNCERTAIN_SPEND_RESERVATION + Duration::from_secs(1);
        }
        let refused = client
            .funding_balance_error(&cs_422_request(10))
            .expect("refused");
        assert_ne!(refused.code, ErrorCode::FundsInFlight, "{refused:?}");
    }

    /// One transaction per outpoint is judged the same way: each needs a UTXO
    /// of its own, and the change coming back provides them.
    #[test]
    fn cs_475_multiple_transactions_wait_for_funds_in_flight_too() {
        let mut client = client_holding_at_rate(
            vec![cs_422_utxo(1, 0, 5_000), cs_422_utxo(2, 0, 5_000)],
            100,
        );
        claim_one(&mut client, 10);
        claim_one(&mut client, 10);
        let request = FundRequest {
            no_of_outpoints: 2,
            multiple_tx: true,
            ..cs_422_request(10)
        };
        let refused = client.funding_balance_error(&request).expect("refused");
        assert_eq!(refused.code, ErrorCode::FundsInFlight, "{refused:?}");
    }

    // ---- CS-426: the same outpoint handed out more than once ----

    /// A real 32-byte txid, because these tests build transactions rather than
    /// only inspecting the cache.
    fn cs_426_utxo(seed: u8, value: i64) -> UtxoEntry {
        utxo(&format!("{:064x}", seed), 0, value, 1_758_719)
    }

    fn cs_426_request(satoshi: u64) -> FundRequest {
        FundRequest {
            client_id: TEST_CLIENT_ID.to_string(),
            satoshi,
            no_of_outpoints: 1,
            multiple_tx: false,
            locking_scripts: vec![hex::decode(LOCKING_SCRIPT_HEX).unwrap()],
        }
    }

    /// The bug as reported. Three identical `POST /fund` calls a second apart
    /// came back with the same outpoint and the same raw transaction, so three
    /// callers were each told they owned what was in fact one output.
    ///
    /// Each request refreshes, and the chain has not caught up: the first
    /// transaction is in the mempool and its input still reads as unspent. The
    /// cache took it back, and the next request spent it again.
    #[test]
    fn cs_426_repeated_requests_do_not_hand_out_the_same_outpoint() {
        let chain = vec![
            cs_426_utxo(1, 5_000),
            cs_426_utxo(2, 6_000),
            cs_426_utxo(3, 7_000),
        ];
        let mut client = client_holding(chain.clone());

        let mut hashes = Vec::new();
        for _ in 0..3 {
            let tx = client
                .create_funding_tx(&cs_426_request(1_000))
                .expect("funds");
            hashes.push(tx.hash().encode());
            // the refresh each request makes, against a chain that still
            // reports every input as unspent
            client.apply_chain_state(chain.clone());
        }

        hashes.sort();
        hashes.dedup();
        assert_eq!(
            hashes.len(),
            3,
            "three requests must produce three different transactions"
        );
    }

    /// A run of spends of the service's own change, as a busy client makes:
    /// each funding request takes the newest change, the smallest output.
    /// Returns the change outputs in order, the last one still unspent.
    fn spend_own_change(client: &mut Client, times: usize) -> Vec<UtxoEntry> {
        let mut change = Vec::new();
        for _ in 0..times {
            let tx = client
                .create_funding_tx(&cs_426_request(1_000))
                .expect("funds");
            let created = client
                .unspent
                .iter()
                .find(|utxo| utxo.tx_hash == tx.hash().encode())
                .expect("its change is in the cache")
                .clone();
            change.push(created);
        }
        change
    }

    /// Change the service spends before the chain has listed it must stay
    /// held until the chain has seen the spend, not merely the change. An
    /// index a refresh or two behind used to lose the hold, then -- having
    /// caught up with the parent but not yet the child -- list the change as
    /// unspent, and it was offered to the next request: a double spend,
    /// refused, and refused again on each retry until the index saw the child.
    #[test]
    fn sr_fund_033_spent_own_change_is_not_offered_while_the_index_lags() {
        let input = cs_426_utxo(1, 5_000);
        let mut client = client_holding(vec![input.clone()]);
        let change = spend_own_change(&mut client, 2);
        let (parent_change, child_change) = (&change[0], &change[1]);
        let offered = |client: &Client| {
            client
                .unspent
                .iter()
                .any(|utxo| outpoint_key(utxo) == outpoint_key(parent_change))
        };

        // two refreshes from an index that has seen neither transaction
        client.apply_chain_state(vec![input.clone()]);
        client.apply_chain_state(vec![input.clone()]);
        assert!(!offered(&client));

        // the index has the parent, not yet the child, for a refresh or two
        let parent_seen = vec![UtxoEntry {
            height: -1,
            ..parent_change.clone()
        }];
        client.apply_chain_state(parent_seen.clone());
        assert!(!offered(&client), "spent change offered again");
        client.apply_chain_state(parent_seen);
        assert!(!offered(&client), "spent change offered again");

        // and once it lists the child's change, there is nothing left to hold
        client.apply_chain_state(vec![child_change.clone()]);
        assert_eq!(client.reserved_outpoint_count(), 0);
        assert_eq!(client.unspent, vec![child_change.clone()]);
    }

    /// The cost side. Holding spent change until it expires would hold one
    /// outpoint per request for ten minutes -- six thousand a minute at 100
    /// requests a second, all rewritten to disk on every commit. The chain
    /// lists only the newest change of a run, and that one listing is enough
    /// to release everything before it.
    #[test]
    fn sr_fund_033_listing_the_newest_change_releases_the_whole_run() {
        let mut client = client_holding(vec![cs_426_utxo(1, 1_000_000)]);
        let change = spend_own_change(&mut client, 50);
        let newest = change.last().unwrap().clone();
        assert!(
            client.reserved_outpoint_count() >= 50,
            "every spend is held"
        );

        // what a node reports once it has the whole run: only its newest change
        client.apply_chain_state(vec![newest.clone()]);

        assert_eq!(client.reserved_outpoint_count(), 0);
        assert!(client.spent_change.is_empty());
        assert_eq!(client.unspent, vec![newest]);
    }

    /// A chain that is part way through the run releases the part it has seen
    /// and keeps holding the rest.
    #[test]
    fn sr_fund_033_an_index_part_way_through_a_run_releases_only_what_it_has_seen() {
        let mut client = client_holding(vec![cs_426_utxo(1, 1_000_000)]);
        let change = spend_own_change(&mut client, 10);

        // the index has the first five transactions: the fifth's change is the
        // newest output it knows of, and is unspent as far as it can tell
        client.apply_chain_state(vec![change[4].clone()]);

        for (i, created) in change.iter().enumerate() {
            let offered = client
                .unspent
                .iter()
                .any(|utxo| outpoint_key(utxo) == outpoint_key(created));
            // the last change is unspent and pending; every other one is spent
            assert_eq!(offered, i == 9, "change {i}");
        }
        // change[0..4] released; change[4..9] still held, spent by
        // transactions the index has not seen
        assert_eq!(client.reserved_outpoint_count(), 5);
    }

    // ---- CS-691: own change claimed by a broadcast still in flight ----

    /// The CS-691 bench's case, 60 of 60 conflicts traced. A node can accept
    /// a funding transaction before the service hears it did, so a refresh in
    /// between lists its change as unspent and the next request claims it.
    /// The transaction's commit then arrived and put that change back in the
    /// cache, claimed or not, and the request after that spent it a second
    /// time: refused as `txn-mempool-conflict`, against the claim still
    /// broadcasting.
    #[test]
    fn cs_691_a_late_commit_does_not_hand_back_change_already_claimed() {
        let input = cs_426_utxo(1, 5_000);
        let mut client = client_holding(vec![input]);
        let parent = claim_one(&mut client, 1_000);
        let change = parent.change_entry.clone().expect("the parent has change");

        // the node has the parent before the service hears that it does
        client.apply_chain_state(vec![UtxoEntry {
            height: -1,
            ..change.clone()
        }]);
        let child = claim_one(&mut client, 1_000);
        assert_eq!(child.spent_outpoints, vec![change.clone()]);

        client.commit_funding_spend(parent).expect("commits");

        assert!(
            !client
                .unspent
                .iter()
                .any(|utxo| outpoint_key(utxo) == outpoint_key(&change)),
            "the parent's commit put its claimed change back"
        );
        assert!(
            client.plan_funding_tx(&cs_426_request(1_000)).is_err(),
            "the next request planned a second spend of the claimed change"
        );
        client.commit_funding_spend(child).expect("commits");
        assert!(client.plan_funding_tx(&cs_426_request(1_000)).is_ok());
    }

    /// Own change [`Client::claim_inputs`] has handed to a broadcast that is
    /// still running, read by an index that lags: it must stay held, as a
    /// committed spend of it is (SR-FUND-033). The first refresh after the
    /// claim dropped it from the pending change, because it was reserved; one
    /// from an index that had not yet seen the parent then released the
    /// reservation as "no longer listed, so spent"; and one that had caught up
    /// with the parent but not the child listed it as unspent, with nothing
    /// holding it. The next request spent it again, and the node refused that
    /// as a conflict with the broadcast still in flight -- 177 times in 30s on
    /// the CS-691 bench.
    #[test]
    fn cs_691_claimed_own_change_is_not_offered_while_the_index_lags() {
        let input = cs_426_utxo(1, 5_000);
        let mut client = client_holding(vec![input.clone()]);
        let change = spend_own_change(&mut client, 1).remove(0);
        let in_flight = claim_one(&mut client, 1_000);
        assert_eq!(
            in_flight.spent_outpoints,
            vec![change.clone()],
            "the claim spends the service's own change"
        );
        let offered = |client: &Client| {
            client
                .unspent
                .iter()
                .any(|utxo| outpoint_key(utxo) == outpoint_key(&change))
        };

        // two refreshes from an index that has seen neither transaction
        client.apply_chain_state(vec![input.clone()]);
        client.apply_chain_state(vec![input.clone()]);
        assert!(!offered(&client));

        // the index has the parent, not yet the in-flight child
        let parent_seen = vec![UtxoEntry {
            height: -1,
            ..change.clone()
        }];
        client.apply_chain_state(parent_seen.clone());
        assert!(!offered(&client), "claimed change offered again");
        client.apply_chain_state(parent_seen);
        assert!(!offered(&client), "claimed change offered again");
        assert!(
            client.plan_funding_tx(&cs_426_request(1_000)).is_err(),
            "the next request planned a spend of the claimed change"
        );

        // committed, it goes on being held as any spent change is
        client.commit_funding_spend(in_flight).expect("commits");
        client.apply_chain_state(vec![UtxoEntry {
            height: -1,
            ..change.clone()
        }]);
        assert!(!offered(&client));
    }

    /// The bench case followed by the lagging index the next test assumes
    /// (from the review of #160). The child claimed the parent's change
    /// while the parent was still broadcasting, so the parent was not handed
    /// out yet; the claim must still hold the change as own change, through a
    /// refresh from an index that has not seen the parent and one that has
    /// the parent but not the child.
    #[test]
    fn cs_691_change_claimed_before_its_parent_commits_survives_a_lagging_index() {
        let input = cs_426_utxo(1, 5_000);
        let mut client = client_holding(vec![input.clone()]);
        let parent = claim_one(&mut client, 1_000);
        let change = parent.change_entry.clone().expect("the parent has change");
        client.apply_chain_state(vec![UtxoEntry {
            height: -1,
            ..change.clone()
        }]);
        let child = claim_one(&mut client, 1_000);
        assert_eq!(child.spent_outpoints, vec![change.clone()]);
        client.commit_funding_spend(parent).expect("commits");

        // an index that has not seen the parent yet
        client.apply_chain_state(vec![input.clone()]);
        client.apply_chain_state(vec![input.clone()]);
        // one that has the parent, not the child still broadcasting
        client.apply_chain_state(vec![UtxoEntry {
            height: -1,
            ..change.clone()
        }]);
        assert!(
            client.plan_funding_tx(&cs_426_request(1_000)).is_err(),
            "the child's claimed change was offered again"
        );
    }

    /// A claim on own change refused as a conflict: another transaction spent
    /// the change. The claim gives it back, but not as pending change, so
    /// the next refresh, from an index that has seen the conflicting spend,
    /// drops it rather than offering it again until the reservation window
    /// runs out (from the review of #160).
    #[test]
    fn cs_691_own_change_refused_as_a_conflict_is_dropped_by_the_next_refresh() {
        let input = cs_426_utxo(1, 5_000);
        let mut client = client_holding(vec![input.clone()]);
        let change = spend_own_change(&mut client, 1).remove(0);
        let conflicted = claim_one(&mut client, 1_000);
        // a refresh while the broadcast runs, from an index behind the parent
        client.apply_chain_state(vec![input.clone()]);

        assert_eq!(client.release_claim(&conflicted, true), 0);
        client.invalidate_chain_state();
        // the index has the parent and the conflicting spend of its change
        client.apply_chain_state(vec![]);

        assert!(
            !client
                .unspent
                .iter()
                .any(|utxo| outpoint_key(utxo) == outpoint_key(&change)),
            "change spent by a conflicting transaction is offered again"
        );
    }

    /// The same, with the flag the service computes for a conflict refused
    /// through the `rpc` blockchain interface (no `[mapi_lite]`), which
    /// reports it as `Upstream`, in chain-gang's own words (#160 review).
    #[test]
    fn cs_691_a_conflict_reported_as_upstream_is_dropped_by_the_next_refresh() {
        let input = cs_426_utxo(1, 5_000);
        let mut client = client_holding(vec![input.clone()]);
        let change = spend_own_change(&mut client, 1).remove(0);
        let conflicted = claim_one(&mut client, 1_000);
        client.apply_chain_state(vec![input.clone()]);

        let spent_elsewhere = crate::broadcaster::BroadcastError::Upstream(
            "Invalid response: sendrawtransaction failed with code -26: 258: txn-mempool-conflict"
                .to_string(),
        )
        .inputs_conflicted();
        assert!(spent_elsewhere);
        assert_eq!(client.release_claim(&conflicted, spent_elsewhere), 0);
        client.invalidate_chain_state();
        client.apply_chain_state(vec![]);

        assert!(
            !client
                .unspent
                .iter()
                .any(|utxo| outpoint_key(utxo) == outpoint_key(&change)),
            "change spent by a conflicting transaction is offered again"
        );
    }

    /// A claim the upstream definitely refused spent nothing: its own change
    /// is spendable again at once, and stays so across a refresh from an
    /// index that has not listed it yet -- the claim's hold is gone, and the
    /// change is pending again.
    #[test]
    fn cs_691_a_released_claim_gives_its_own_change_back() {
        let input = cs_426_utxo(1, 5_000);
        let mut client = client_holding(vec![input.clone()]);
        let change = spend_own_change(&mut client, 1).remove(0);
        let refused = claim_one(&mut client, 1_000);
        // a refresh while the broadcast runs drops it from the pending change
        client.apply_chain_state(vec![input.clone()]);

        assert_eq!(
            client.release_claim(&refused, false),
            1,
            "reports the own change it made pending, so the caller saves it (CS-465)"
        );
        assert!(!client.spent_change.contains_key(&refused.txid));
        client.apply_chain_state(vec![input.clone()]);

        assert!(
            client
                .unspent
                .iter()
                .any(|utxo| outpoint_key(utxo) == outpoint_key(&change)),
            "released own change not offered"
        );
        let (_, next) = client
            .plan_funding_tx(&cs_426_request(1_000))
            .expect("funds from the released change");
        assert_eq!(next.spent_outpoints, vec![change]);
    }

    /// The mechanism behind it. A refresh replaces the cache with what the
    /// chain says, and the chain says nothing about a transaction it has not
    /// seen, so an input the service has just spent must be held out of the
    /// cache rather than merely removed from it.
    #[test]
    fn cs_426_a_refresh_does_not_return_an_input_already_spent() {
        let chain = vec![cs_426_utxo(1, 5_000), cs_426_utxo(2, 6_000)];
        let mut client = client_holding(chain.clone());

        let tx = client
            .create_funding_tx(&cs_426_request(1_000))
            .expect("funds");
        let spent: Vec<String> = tx
            .inputs
            .iter()
            .map(|input| input.prev_output.hash.encode())
            .collect();

        client.apply_chain_state(chain);

        for hash in &spent {
            assert!(
                !client.unspent.iter().any(|utxo| &utxo.tx_hash == hash),
                "the refresh handed back an input the service had spent: {hash}"
            );
        }
    }

    /// The other half. The change is real -- its transaction was broadcast --
    /// but the chain has not reported it either, so a refresh would drop it
    /// and leave the client unable to spend its own change.
    #[test]
    fn cs_426_change_survives_a_refresh_that_has_not_seen_it() {
        let chain = vec![cs_426_utxo(1, 5_000)];
        let mut client = client_holding(chain.clone());

        client
            .create_funding_tx(&cs_426_request(1_000))
            .expect("funds");
        let before: i64 = client.unspent.iter().map(|u| u.value).sum();
        assert!(before > 0, "there is change to keep");

        client.apply_chain_state(chain);

        assert_eq!(
            client.unspent.iter().map(|u| u.value).sum::<i64>(),
            before,
            "the refresh dropped change the service had already broadcast"
        );
    }

    /// Once the chain catches up it is authoritative again: the spent input is
    /// gone from what it reports, the change is in it, and the service stops
    /// holding either.
    #[test]
    fn cs_426_the_service_defers_to_the_chain_once_it_catches_up() {
        let mut client = client_holding(vec![cs_426_utxo(1, 5_000)]);
        client
            .create_funding_tx(&cs_426_request(1_000))
            .expect("funds");
        let change = client.unspent[0].clone();

        // the chain now reports the change and no longer reports the input
        client.apply_chain_state(vec![UtxoEntry {
            height: 900,
            ..change.clone()
        }]);

        assert_eq!(client.reserved_outpoint_count(), 0, "reservation released");
        assert_eq!(client.unspent.len(), 1);
        assert_eq!(
            client.get_balance().confirmed,
            change.value,
            "the chain's confirmed copy is what is reported now"
        );
    }

    /// CS-427: the same defect as CS-426 with a different symptom. Varying the
    /// satoshi value makes each request build a *different* transaction, so
    /// they are no longer identical -- but they still select the same input,
    /// and the second one to reach the network is refused as
    /// `txn-mempool-conflict`.
    ///
    /// Asserting on inputs rather than transaction hashes is what separates
    /// this from CS-426: distinct hashes are not enough if they spend the same
    /// coin.
    #[test]
    fn cs_427_varying_the_amount_still_must_not_reuse_an_input() {
        let chain = vec![
            cs_426_utxo(1, 5_000),
            cs_426_utxo(2, 6_000),
            cs_426_utxo(3, 7_000),
        ];
        let mut client = client_holding(chain.clone());

        let mut spent: Vec<(String, u32)> = Vec::new();
        for satoshi in [1_000u64, 1_100, 1_200] {
            let tx = client
                .create_funding_tx(&cs_426_request(satoshi))
                .expect("funds");
            for input in &tx.inputs {
                spent.push((input.prev_output.hash.encode(), input.prev_output.index));
            }
            // the refresh each request makes, against a chain that has not
            // seen any of these transactions yet
            client.apply_chain_state(chain.clone());
        }

        let unique: std::collections::HashSet<_> = spent.iter().collect();
        assert_eq!(
            unique.len(),
            spent.len(),
            "an input was spent twice, which the network refuses as a conflict: {spent:?}"
        );
    }

    // ---- CS-422: funding limits reported honestly ----

    /// A real 32-byte txid. These tests build transactions, not just inspect
    /// the cache, so the hash has to decode.
    fn cs_422_utxo(seed: u8, pos: u32, value: i64) -> UtxoEntry {
        utxo(&format!("{:064x}", seed), pos, value, 1_758_719)
    }

    /// The wallet from the bug report: 480 + 250 + five 100s = 1230 satoshi.
    fn cs_422_wallet() -> Client {
        client_holding(vec![
            cs_422_utxo(1, 0, 100),
            cs_422_utxo(2, 1, 100),
            cs_422_utxo(3, 1, 100),
            cs_422_utxo(4, 1, 100),
            cs_422_utxo(5, 1, 100),
            cs_422_utxo(6, 0, 480),
            cs_422_utxo(7, 0, 250),
        ])
    }

    fn cs_422_request(satoshi: u64) -> FundRequest {
        FundRequest {
            client_id: TEST_CLIENT_ID.to_string(),
            satoshi,
            no_of_outpoints: 1,
            multiple_tx: false,
            locking_scripts: vec![hex::decode(LOCKING_SCRIPT_HEX).unwrap()],
        }
    }

    /// What the wallet can really pay out, and why it is under the balance.
    ///
    /// At 100 sat/KB every one of the seven UTXOs is worth spending -- an
    /// input adds 148 bytes, so it costs 15 satoshi to bring in 100 -- and the
    /// ceiling is all seven minus the fee for seven, with nothing held back
    /// for change: funding exactly that builds a transaction with no change
    /// output at all (CS-452). Consolidated, the same
    /// coins would cost one input's fee instead of seven, which is why the two
    /// figures differ and why the error codes below split. Under the
    /// superseded step fee the seventh input crossed a kilobyte and cost 500
    /// to bring in 100, so it was left out; a flat rate has no such cliff, but
    /// an input can still cost more than it is worth (see
    /// `cs_451_an_input_worth_less_than_its_own_fee_is_left_out`).
    #[test]
    fn cs_422_max_fundable_accounts_for_the_fee_each_input_adds() {
        let client = cs_422_wallet();
        let script_bytes = hex::decode(LOCKING_SCRIPT_HEX).unwrap().len() as u64;

        assert_eq!(client.total_unspent(), 1230);
        assert_eq!(client.max_fundable(&[script_bytes]), 1118);
        assert_eq!(client.max_fundable_if_consolidated(&[script_bytes]), 1207);
    }

    /// The report's first complaint: asking for 480 was refused as
    /// "1230 available, 1230 required", which reads as a contradiction. The
    /// refusal was right -- 480 is genuinely out of reach -- but the figure
    /// was the cost of a one-input transaction, and this wallet cannot fund
    /// anything with one input.
    #[test]
    fn cs_422_insufficient_balance_reports_what_can_actually_be_paid_out() {
        let error = cs_422_wallet()
            .funding_balance_error(&cs_422_request(1208))
            .expect("1208 is beyond this wallet");
        assert_eq!(error.code, ErrorCode::InsufficientBalance);
        assert!(
            error.description.contains("1208 satoshi requested"),
            "{}",
            error.description
        );
        assert!(
            error.description.contains("1230 satoshi available"),
            "{}",
            error.description
        );
        assert!(
            error.description.contains("at most 1207"),
            "{}",
            error.description
        );
        // the number that caused the confusion is gone
        assert!(
            !error.description.contains("1230 required"),
            "{}",
            error.description
        );
    }

    /// The report's second complaint: asking for *less* produced a *larger*
    /// requirement, more than the balance, because the figure quoted was the
    /// cost of spending every UTXO rather than what the wallet can achieve.
    /// Here that discredited figure would be 1150 + 112 = 1262, against a
    /// balance of 1230.
    #[test]
    fn cs_422_no_suitable_utxo_does_not_quote_a_cost_nobody_would_pay() {
        let error = cs_422_wallet()
            .funding_balance_error(&cs_422_request(1150))
            .expect("1150 is beyond this UTXO set as it stands");
        assert_eq!(error.code, ErrorCode::NoSuitableUtxo);
        assert!(
            error.description.contains("at most 1118"),
            "{}",
            error.description
        );
        assert!(
            error.description.contains("consolidated"),
            "{}",
            error.description
        );
        assert!(
            !error.description.contains("1262"),
            "the all-inputs cost is not a requirement: {}",
            error.description
        );
    }

    /// The two codes now mean different things, and the boundary between them
    /// is the point where consolidating would stop helping. Up to 1118 the
    /// wallet funds as it is; between 1119 and 1207 it could fund only if it
    /// were consolidated; above 1207 no arrangement is enough.
    #[test]
    fn cs_422_the_two_codes_split_at_the_point_consolidating_stops_helping() {
        let client = cs_422_wallet();

        assert!(client
            .funding_balance_error(&cs_422_request(1118))
            .is_none());

        let shape = client
            .funding_balance_error(&cs_422_request(1119))
            .expect("1119 needs consolidating");
        assert_eq!(shape.code, ErrorCode::NoSuitableUtxo);

        let shape = client
            .funding_balance_error(&cs_422_request(1207))
            .expect("1207 needs consolidating");
        assert_eq!(shape.code, ErrorCode::NoSuitableUtxo);

        let balance = client
            .funding_balance_error(&cs_422_request(1208))
            .expect("1208 needs more money");
        assert_eq!(balance.code, ErrorCode::InsufficientBalance);
    }

    /// The report asked how a client is meant to know what it can withdraw.
    /// The answer has to be a number it can actually spend: funding exactly
    /// the reported maximum must work, and the transaction must balance.
    #[test]
    fn cs_422_the_reported_maximum_can_actually_be_funded() {
        let mut client = cs_422_wallet();
        let script_bytes = hex::decode(LOCKING_SCRIPT_HEX).unwrap().len() as u64;
        let max = client.max_fundable(&[script_bytes]) as u64;

        let request = cs_422_request(max);
        assert!(
            client.funding_balance_error(&request).is_none(),
            "the maximum this wallet reports must be fundable"
        );
        let tx = client
            .create_funding_tx(&request)
            .expect("and the transaction must build");
        // Funding the advertised maximum spends the wallet out exactly, so
        // there is no change output at all. This is CS-452: the maximum used
        // to be one satoshi lower, which left a one-satoshi output that cost
        // more to spend than it held, and a balance the client could not use.
        assert_eq!(
            tx.outputs.len(),
            1,
            "no change output, so nothing is stranded"
        );
        assert_eq!(tx.outputs[0].satoshis, max as i64);

        let after = client.get_balance();
        assert_eq!(
            after.confirmed + after.unconfirmed,
            0,
            "the wallet is spent out, not left holding dust: {after:?}"
        );
    }

    /// The guard that keeps the two halves honest. Whatever the pre-check
    /// admits, the builder must be able to build -- otherwise a request that
    /// looked fundable dies as an internal error instead of a coded one.
    /// Sweeping the whole range is cheap and catches a boundary that a
    /// hand-picked case would not.
    #[test]
    fn cs_422_anything_the_check_admits_can_be_built() {
        for satoshi in 1..=600u64 {
            let mut client = cs_422_wallet();
            let request = cs_422_request(satoshi);
            if client.funding_balance_error(&request).is_some() {
                continue;
            }
            client
                .create_funding_tx(&request)
                .unwrap_or_else(|e| panic!("{satoshi} passed the check but would not build: {e}"));
        }
    }

    /// A wallet whose single UTXO covers the cost exactly. Selection used to
    /// accept it on `>=` while the builder demanded a positive change output,
    /// so the request passed every check and then failed as an internal
    /// error.
    #[test]
    fn cs_422_a_utxo_covering_the_cost_exactly_is_not_offered_as_fundable() {
        let script_bytes = hex::decode(LOCKING_SCRIPT_HEX).unwrap().len() as u64;
        // an empty wallet, only to cost the transaction at the same rate the
        // wallet under test uses
        let exact = 123 + client_holding(Vec::new()).estimate_fee(&[script_bytes], 1) as i64;
        let mut client = client_holding(vec![cs_422_utxo(9, 0, exact)]);

        let request = cs_422_request(123);
        match client.funding_balance_error(&request) {
            // refused is correct: there is no room for change
            Some(error) => assert_eq!(error.code, ErrorCode::InsufficientBalance),
            // or, if admitted, it must genuinely build
            None => {
                client
                    .create_funding_tx(&request)
                    .expect("admitted, so it must build");
            }
        }
    }

    // ---- CS-425: unconfirmed funds, and a balance that matches them ----

    /// The question the ticket asks. Unconfirmed outputs *are* spendable, and
    /// always have been: nothing filters the unspent set by height. The
    /// reporter concluded otherwise from a `/balance` that under-reported
    /// them, which is the other half of this fix.
    ///
    /// Written as a test so the policy is pinned rather than described: the
    /// confirmed UTXO alone cannot cover this request.
    #[test]
    fn cs_425_unconfirmed_outputs_are_spendable() {
        let client = client_holding(vec![
            utxo(&format!("{:064x}", 1), 0, 400, 500),
            utxo(&format!("{:064x}", 2), 0, 900, UNCONFIRMED_HEIGHT),
        ]);
        let script_bytes = hex::decode(LOCKING_SCRIPT_HEX).unwrap().len() as u64;

        assert!(
            client.max_fundable(&[script_bytes]) > 400,
            "the unconfirmed 900 is counted towards what can be funded"
        );
        let request = cs_422_request(500);
        assert!(
            client.funding_balance_error(&request).is_none(),
            "500 needs the unconfirmed output; the confirmed 400 cannot cover it"
        );
    }

    /// The balance has to agree with the unspent set it came from, including
    /// its unconfirmed part. WhatsOnChain's deprecated balance endpoint
    /// reported `unconfirmed: 0` for an address whose unspent set plainly held
    /// unconfirmed outputs, which is what made the service look as though it
    /// ignored them.
    #[test]
    fn cs_425_balance_is_derived_from_the_unspent_set() {
        let client = client_holding(vec![
            utxo(&format!("{:064x}", 1), 0, 400, 500),
            utxo(&format!("{:064x}", 2), 0, 900, UNCONFIRMED_HEIGHT),
        ]);
        let balance = client.get_balance();
        assert_eq!(balance.confirmed, 400);
        assert_eq!(
            balance.unconfirmed, 900,
            "an unconfirmed output must not vanish from the balance"
        );
    }

    /// A stored balance drifts away from the UTXOs it describes as soon as a
    /// funding transaction spends from them. Derived, it cannot.
    #[test]
    fn cs_425_the_balance_follows_a_spend_without_a_refresh() {
        let mut client = cs_422_wallet();
        assert_eq!(client.get_balance().confirmed, 1230);

        let request = cs_422_request(300);
        client.create_funding_tx(&request).expect("funds");

        let after = client.get_balance();
        assert!(
            after.confirmed + after.unconfirmed < 1230,
            "the balance still reports the funds the transaction just spent: {after:?}"
        );
        assert_eq!(
            after.confirmed + after.unconfirmed,
            client.total_unspent(),
            "balance and unspent set must agree"
        );
    }

    /// The change output of a transaction that has not been broadcast, let
    /// alone mined, is unconfirmed. Recording it as height 0 meant "confirmed
    /// in block 0" under chain-gang's convention, putting it on the wrong side
    /// of every confirmed/unconfirmed split.
    #[test]
    fn cs_425_change_from_a_new_transaction_is_unconfirmed() {
        let mut client = cs_422_wallet();
        let before = client.get_balance();
        assert_eq!(before.unconfirmed, 0);

        client
            .create_funding_tx(&cs_422_request(300))
            .expect("funds");

        // At 100 sat/KB a single input covers it: the 480 is the smallest
        // UTXO above 300 + 23 of fee, so it alone is spent and 157 comes back
        // as change. The other six are untouched and still confirmed.
        let after = client.get_balance();
        assert_eq!(
            after.unconfirmed, 157,
            "the change output should be counted as unconfirmed: {after:?}"
        );
        assert_eq!(
            after.confirmed, 750,
            "the UTXOs that were not spent stay confirmed: {after:?}"
        );
    }

    // ---- CS-506: own change the node says is missing is forgotten ----

    fn spends(plan: &FundingSpendPlan, txid: &str) -> bool {
        plan.spent_outpoints
            .iter()
            .any(|entry| entry.tx_hash == txid)
    }

    /// The ticket's case, at the cache. The first funding leaves change; the
    /// second spends it and is refused as missing inputs. Released as any
    /// refusal is, the change came straight back -- and a refresh restored it
    /// every time -- so every request selected it again.
    #[test]
    fn cs_506_refused_own_change_is_not_selected_again_or_restored() {
        let small = cs_422_utxo(1, 0, 10_000);
        let large = cs_422_utxo(2, 0, 50_000);
        let mut client = client_holding_at_rate(vec![small.clone(), large.clone()], 100);

        let first = claim_one(&mut client, 1_000);
        let first_txid = first.txid.clone();
        client.commit_funding_spend(first).expect("commits");

        let second = claim_one(&mut client, 1_000);
        assert!(
            spends(&second, &first_txid),
            "the change is the smallest suitable input, so it is selected"
        );
        client.release_claim(&second, false);
        assert_eq!(client.forget_refused_own_change(&second), 1);

        // A refresh: the chain still lists both originals (it has not seen the
        // first funding transaction drop out), and has never listed the change.
        client.apply_chain_state(vec![small, large]);
        assert!(
            !client.unspent.iter().any(|u| u.tx_hash == first_txid),
            "the refresh did not put the change back"
        );

        let third = claim_one(&mut client, 1_000);
        assert!(
            !spends(&third, &first_txid),
            "the next request selects a different input"
        );
    }

    /// Without the missing-inputs verdict a refusal gives the change back as
    /// before: it may be a conflict on another input, and the change is fine.
    #[test]
    fn cs_506_an_ordinary_refusal_keeps_own_change() {
        let mut client = client_holding_at_rate(
            vec![cs_422_utxo(1, 0, 10_000), cs_422_utxo(2, 0, 50_000)],
            100,
        );
        let first = claim_one(&mut client, 1_000);
        let first_txid = first.txid.clone();
        client.commit_funding_spend(first).expect("commits");
        let second = claim_one(&mut client, 1_000);
        client.release_claim(&second, false);

        assert!(client.unspent.iter().any(|u| u.tx_hash == first_txid));
    }

    /// Change built on missing change cannot exist either: the transaction
    /// that created it spent an output that is not there.
    #[test]
    fn cs_506_change_built_on_missing_change_is_forgotten_with_it() {
        let mut client = client_holding_at_rate(vec![cs_422_utxo(1, 0, 50_000)], 100);
        let first = claim_one(&mut client, 1_000);
        let first_txid = first.txid.clone();
        client.commit_funding_spend(first).expect("commits");
        let second = claim_one(&mut client, 1_000);
        let second_txid = second.txid.clone();
        assert!(spends(&second, &first_txid));
        client
            .commit_funding_spend(second.clone())
            .expect("commits");

        // The node says the first change is missing.
        assert_eq!(client.forget_refused_own_change(&second), 2);
        assert!(client.pending_change.is_empty());
        assert!(client.spent_change.is_empty());
        assert_eq!(
            client.reserved_outpoint_count(),
            1,
            "only the wallet UTXO the first funding spent is still held; the missing change \
             it reserved for the second is not"
        );
        assert!(
            !client.unspent.iter().any(|u| u.tx_hash == second_txid),
            "the second funding's change is gone too"
        );
    }

    /// Only own change is forgotten. An input the chain lists is the chain's to
    /// judge: the refresh after the refusal drops it if it really is gone.
    #[test]
    fn cs_506_a_chain_listed_input_is_left_to_the_refresh() {
        let mut client = client_holding_at_rate(vec![cs_422_utxo(1, 0, 10_000)], 100);
        let plan = claim_one(&mut client, 1_000);
        client.release_claim(&plan, false);
        assert_eq!(client.forget_refused_own_change(&plan), 0);
        assert_eq!(client.unspent.len(), 1);
    }

    /// CS-634: a key must be for `blockchain_interface.network`. A WIF only
    /// tells mainnet from the rest, so a test-network key serves testnet,
    /// regtest and STN. The refusal names the client and both networks, and
    /// no part of the key.
    #[test]
    fn cs_634_a_key_must_be_for_the_configured_network() {
        use crate::test_support::{MAPI_TEST_SIGNER_WIF as MAINNET_WIF, TEST_WIF};
        let client = |wif: &str| ClientConfig {
            client_id: TEST_CLIENT_ID.to_string(),
            wif_key: wif.to_string(),
            api_key: None,
        };
        for (wif, network) in [
            (MAINNET_WIF, "testnet"),
            (MAINNET_WIF, "regtest"),
            (MAINNET_WIF, "stn"),
            (TEST_WIF, "mainnet"),
        ] {
            let Err(error) = Client::try_new(&client(wif), network) else {
                panic!("{network} took the key");
            };
            assert!(error.contains(TEST_CLIENT_ID), "{error}");
            assert!(
                error.contains(&format!("blockchain_interface.network is {network}")),
                "{error}"
            );
            assert!(!error.contains(&wif[..8]), "{error}");
        }
        assert!(Client::try_new(&client(MAINNET_WIF), "mainnet").is_ok());
        for network in ["testnet", "regtest", "stn"] {
            assert!(
                Client::try_new(&client(TEST_WIF), network).is_ok(),
                "{network}"
            );
        }
    }
}
