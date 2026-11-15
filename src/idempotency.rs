//! Idempotency records for `POST /fund`.
//!
//! `POST /fund` broadcasts a funding transaction and then returns the outpoint
//! to the client in the response body. If that response is lost -- a client
//! timeout, a dropped connection -- the transaction is already on chain and the
//! client has no record of the outpoint. A plain retry then produces a *second*
//! funding transaction, spending more of the wallet, and the first output is
//! stranded: locked to a P2PKH key the client may no longer be able to name.
//! (That is a signing key -- not to be confused with the `idempotency_key`
//! below, which is an opaque string naming a funding request.)
//!
//! A client can avoid that by sending an `idempotency_key` with the request.
//! The first call reserves that `idempotency_key`, and once the transaction is
//! broadcast the response is retained against it. A retry carrying the same
//! `idempotency_key` replays that stored response instead of funding again.
//!
//! Records live in memory only, so they do not survive a restart. A retry that
//! spans one can still double-fund; that limit is deliberate (it keeps this out
//! of the write path of every funding call) and is documented for callers.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::responses::FundingResponseJson;

/// Identifies a record: a client and the `idempotency_key` it supplied.
///
/// Scoped per client, so two clients choosing the same `idempotency_key`
/// string cannot collide.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RecordKey {
    pub client_id: String,
    pub key: String,
}

impl RecordKey {
    pub fn new(client_id: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            key: key.into(),
        }
    }
}

/// The outcome of a funding attempt that reached the chain, retained so a
/// retry can be answered with what actually happened rather than being funded
/// again.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// Every requested transaction broadcast.
    Funded(FundingResponseJson),
    /// Some transactions broadcast and some did not. The ones that did are in
    /// `response`, and the original failure description is kept so the retry
    /// is answered the same way as the first call.
    PartiallyFunded {
        description: String,
        response: FundingResponseJson,
    },
}

/// What the caller should do with a request that carries an `idempotency_key`.
#[derive(Debug, Clone, PartialEq)]
pub enum Reservation {
    /// This `idempotency_key` is new. Proceed with funding; report the outcome via
    /// [`IdempotencyStore::complete`] or [`IdempotencyStore::release`].
    Proceed,
    /// This `idempotency_key` already reached the chain. Replay the retained outcome
    /// without preparing or broadcasting anything.
    Replay(Outcome),
    /// A request with this `idempotency_key` is still being processed. The caller should not
    /// fund, because doing so risks the duplicate this exists to prevent.
    InProgress,
    /// This `idempotency_key` was used before with a materially different request, so
    /// replaying the stored response would answer a question that was not
    /// asked.
    Reused,
}

#[derive(Debug, Clone)]
enum Entry {
    InFlight {
        fingerprint: String,
        at: Instant,
    },
    Completed {
        fingerprint: String,
        outcome: Outcome,
        at: Instant,
    },
}

impl Entry {
    fn at(&self) -> Instant {
        match self {
            Entry::InFlight { at, .. } => *at,
            Entry::Completed { at, .. } => *at,
        }
    }

    fn fingerprint(&self) -> &str {
        match self {
            Entry::InFlight { fingerprint, .. } => fingerprint,
            Entry::Completed { fingerprint, .. } => fingerprint,
        }
    }
}

/// In-memory idempotency records, bounded by both age and count.
#[derive(Debug)]
pub struct IdempotencyStore {
    entries: HashMap<RecordKey, Entry>,
    ttl: Duration,
    max_entries: usize,
}

impl IdempotencyStore {
    pub fn new(ttl: Duration, max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            ttl,
            max_entries,
        }
    }

    /// Claim an `idempotency_key` for a request whose identity is `fingerprint`.
    ///
    /// Reserving takes effect immediately, so a concurrent duplicate sees
    /// `InProgress` rather than both callers broadcasting.
    pub fn reserve(&mut self, key: RecordKey, fingerprint: &str) -> Reservation {
        self.evict_expired();

        if let Some(entry) = self.entries.get(&key) {
            if entry.fingerprint() != fingerprint {
                return Reservation::Reused;
            }
            return match entry {
                Entry::InFlight { .. } => Reservation::InProgress,
                Entry::Completed { outcome, .. } => Reservation::Replay(outcome.clone()),
            };
        }

        self.make_room_for_one();
        self.entries.insert(
            key,
            Entry::InFlight {
                fingerprint: fingerprint.to_string(),
                at: Instant::now(),
            },
        );
        Reservation::Proceed
    }

    /// Retain `outcome` against this `idempotency_key`, so a later retry replays it.
    ///
    /// Call this once a funding transaction has been broadcast, whether or not
    /// the local UTXO cache was updated afterwards, and whether the batch
    /// completed or only partly succeeded: the funds have moved, so a retry
    /// must not fund again.
    pub fn complete(&mut self, key: RecordKey, fingerprint: &str, outcome: Outcome) {
        self.entries.insert(
            key,
            Entry::Completed {
                fingerprint: fingerprint.to_string(),
                outcome,
                at: Instant::now(),
            },
        );
    }

    /// Drop the reservation for this `idempotency_key` because nothing was
    /// broadcast.
    ///
    /// This lets the client retry with the same `idempotency_key`, which is the
    /// right outcome for a request that failed before spending anything.
    pub fn release(&mut self, key: &RecordKey) {
        self.entries.remove(key);
    }

    /// Number of retained records. Used by the tests that assert the store
    /// stays bounded.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    fn evict_expired(&mut self) {
        let ttl = self.ttl;
        let now = Instant::now();
        self.entries
            .retain(|_, entry| now.duration_since(entry.at()) < ttl);
    }

    /// Make space for one new record when the store is full, by dropping the
    /// oldest record of whichever client holds the most (CS-530).
    ///
    /// Dropping the globally oldest let one client evict the others: about
    /// `max_entries` cheap fundings inside the TTL pushed out every other
    /// client's records, and their retries then funded twice. Taking from the
    /// biggest holder means a client flooding the store evicts its own.
    ///
    /// Only completed records are dropped, never one in flight (CS-723). A
    /// request whose broadcast outcome was unknown leaves its record in
    /// flight on purpose, so a retry is told it is in progress until the TTL
    /// rather than funded again; evicted under load -- above about 16.7
    /// fundings a second at the defaults -- that retry funded a second time.
    /// With nothing completed left to drop the store goes over its bound, by
    /// at most the requests in flight, which the funding limits bound.
    fn make_room_for_one(&mut self) {
        while self.entries.len() >= self.max_entries {
            let mut held: HashMap<&str, usize> = HashMap::new();
            for (key, entry) in &self.entries {
                if matches!(entry, Entry::Completed { .. }) {
                    *held.entry(key.client_id.as_str()).or_default() += 1;
                }
            }
            // Ties go to the client id that sorts first, so eviction does not
            // depend on hash order.
            let Some(biggest) = held
                .into_iter()
                .max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.0.cmp(a.0)))
                .map(|(client, _)| client.to_string())
            else {
                break;
            };
            let oldest = self
                .entries
                .iter()
                .filter(|(key, entry)| {
                    key.client_id == biggest && matches!(entry, Entry::Completed { .. })
                })
                .min_by_key(|(_, entry)| entry.at())
                .map(|(key, _)| key.clone());
            match oldest {
                Some(key) => {
                    self.entries.remove(&key);
                }
                None => break,
            }
        }
    }
}

/// Identity of a funding request, so an `idempotency_key` reused with different
/// parameters is detected rather than silently answered with the earlier
/// response.
///
/// A hash of the request rather than the request itself (CS-530), so a record
/// costs the same whatever the scripts were: kept whole, one request paying
/// many large scripts made one record that size, for the whole TTL.
pub fn request_fingerprint(
    satoshi: u64,
    no_of_outpoints: u32,
    multiple_tx: bool,
    locking_scripts: &[String],
) -> String {
    // Every script is included, so two requests that differ only in which
    // scripts they pay cannot share a key.
    let identity = format!(
        "{satoshi}:{no_of_outpoints}:{multiple_tx}:{}",
        locking_scripts.join(",")
    );
    chain_gang::util::sha256d(identity.as_bytes()).encode()
}

/// The longest `idempotency_key` accepted, in bytes (CS-530). Keys are
/// retained for the TTL, so their size is memory a client chooses.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 256;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::responses::{OutpointResponse, TxResponse};

    fn response(hash: &str) -> FundingResponseJson {
        FundingResponseJson {
            outpoints: vec![OutpointResponse {
                hash: hash.to_string(),
                index: 1,
                satoshi: 123,
                locking_script: "76a914ddc574807c3035ab43553a22c0b9df1f55737fae88ac".to_string(),
            }],
            txs: vec![TxResponse {
                tx: "0100000001".to_string(),
            }],
            replayed: false,
        }
    }

    fn store() -> IdempotencyStore {
        IdempotencyStore::new(Duration::from_secs(600), 10)
    }

    fn key() -> RecordKey {
        RecordKey::new("id1", "abc")
    }

    #[test]
    fn a_new_key_proceeds() {
        let mut store = store();
        assert_eq!(store.reserve(key(), "fp"), Reservation::Proceed);
    }

    #[test]
    fn a_completed_key_replays_the_stored_response() {
        let mut store = store();
        store.reserve(key(), "fp");
        store.complete(key(), "fp", Outcome::Funded(response("aabb")));
        match store.reserve(key(), "fp") {
            Reservation::Replay(Outcome::Funded(replayed)) => {
                assert_eq!(replayed.outpoints[0].hash, "aabb")
            }
            other => panic!("expected Replay, got {other:?}"),
        }
    }

    /// The case the issue is about: the funding transaction is on chain, the
    /// response was lost, and the client retries. It must not fund again.
    #[test]
    fn sr_fund_retry_after_a_lost_response_does_not_fund_again() {
        let mut store = store();
        assert_eq!(store.reserve(key(), "fp"), Reservation::Proceed);
        store.complete(key(), "fp", Outcome::Funded(response("aabb")));
        // the retry
        assert!(matches!(store.reserve(key(), "fp"), Reservation::Replay(_)));
    }

    #[test]
    fn a_concurrent_duplicate_is_in_progress() {
        let mut store = store();
        store.reserve(key(), "fp");
        assert_eq!(store.reserve(key(), "fp"), Reservation::InProgress);
    }

    #[test]
    fn the_same_key_with_a_different_request_is_reused() {
        let mut store = store();
        store.reserve(key(), "fp");
        store.complete(key(), "fp", Outcome::Funded(response("aabb")));
        assert_eq!(store.reserve(key(), "different"), Reservation::Reused);
    }

    #[test]
    fn releasing_lets_the_same_key_be_retried() {
        let mut store = store();
        store.reserve(key(), "fp");
        store.release(&key());
        assert_eq!(store.reserve(key(), "fp"), Reservation::Proceed);
    }

    #[test]
    fn keys_are_scoped_per_client() {
        let mut store = store();
        store.reserve(RecordKey::new("id1", "shared"), "fp");
        store.complete(
            RecordKey::new("id1", "shared"),
            "fp",
            Outcome::Funded(response("aabb")),
        );
        // a different client using the same idempotency_key string is unaffected
        assert_eq!(
            store.reserve(RecordKey::new("id2", "shared"), "fp"),
            Reservation::Proceed
        );
    }

    #[test]
    fn expired_records_are_forgotten() {
        let mut store = IdempotencyStore::new(Duration::from_nanos(1), 10);
        store.reserve(key(), "fp");
        store.complete(key(), "fp", Outcome::Funded(response("aabb")));
        std::thread::sleep(Duration::from_millis(5));
        // past its TTL, so the idempotency_key is new again rather than replayable
        assert_eq!(store.reserve(key(), "fp"), Reservation::Proceed);
    }

    /// A funding that completed: what eviction drops (CS-723).
    fn funded(store: &mut IdempotencyStore, key: RecordKey) {
        assert_eq!(store.reserve(key.clone(), "fp"), Reservation::Proceed);
        store.complete(key, "fp", Outcome::Funded(response("aabb")));
    }

    fn replays(store: &mut IdempotencyStore, key: RecordKey) -> bool {
        matches!(store.reserve(key, "fp"), Reservation::Replay(_))
    }

    #[test]
    fn the_store_is_bounded_by_max_entries() {
        let mut store = IdempotencyStore::new(Duration::from_secs(600), 3);
        for i in 0..10 {
            funded(&mut store, RecordKey::new("id1", format!("key{i}")));
        }
        assert_eq!(store.len(), 3);
    }

    /// CS-530: a client filling the store evicts its own records, not other
    /// clients'.
    #[test]
    fn sr_sec_029_a_flooding_client_evicts_its_own_records() {
        let mut store = IdempotencyStore::new(Duration::from_secs(600), 10);
        let victim = RecordKey::new("victim", "their-retry");
        funded(&mut store, victim.clone());
        std::thread::sleep(Duration::from_millis(2));
        for i in 0..100 {
            funded(&mut store, RecordKey::new("flooder", format!("k{i}")));
        }
        assert_eq!(store.len(), 10);
        assert!(
            replays(&mut store, victim),
            "the victim's record must survive the flood"
        );
    }

    /// Among clients, the one holding the most loses its oldest.
    #[test]
    fn sr_sec_029_eviction_takes_the_biggest_holders_oldest() {
        let mut store = IdempotencyStore::new(Duration::from_secs(600), 4);
        funded(&mut store, RecordKey::new("a", "1"));
        std::thread::sleep(Duration::from_millis(2));
        funded(&mut store, RecordKey::new("b", "1"));
        std::thread::sleep(Duration::from_millis(2));
        funded(&mut store, RecordKey::new("b", "2"));
        std::thread::sleep(Duration::from_millis(2));
        funded(&mut store, RecordKey::new("b", "3"));
        // Full: b holds three, so b's oldest goes, not a's older one.
        funded(&mut store, RecordKey::new("c", "1"));
        assert!(replays(&mut store, RecordKey::new("a", "1")));
        assert_eq!(
            store.reserve(RecordKey::new("b", "1"), "fp"),
            Reservation::Proceed
        );
    }

    /// A record's size does not grow with the scripts it pays.
    #[test]
    fn sr_sec_029_the_fingerprint_is_a_fixed_size_hash() {
        let big = vec!["ab".repeat(100_000)];
        let fingerprint = request_fingerprint(1, 1, false, &big);
        assert_eq!(fingerprint.len(), 64);
    }

    #[test]
    fn fingerprint_distinguishes_materially_different_requests() {
        let one = |s: &str| vec![s.to_string()];
        let base = request_fingerprint(123, 1, false, &one("76a914aa88ac"));
        assert_eq!(
            base,
            request_fingerprint(123, 1, false, &one("76a914aa88ac"))
        );
        assert_ne!(
            base,
            request_fingerprint(124, 1, false, &one("76a914aa88ac"))
        );
        assert_ne!(
            base,
            request_fingerprint(123, 2, false, &one("76a914aa88ac"))
        );
        assert_ne!(
            base,
            request_fingerprint(123, 1, true, &one("76a914aa88ac"))
        );
        assert_ne!(
            base,
            request_fingerprint(123, 1, false, &one("76a914bb88ac"))
        );
    }

    /// Two requests for the same count but different per-outpoint scripts are
    /// different requests, so a key used for one must not answer the other.
    #[test]
    fn fingerprint_covers_every_script_in_the_list() {
        let a = request_fingerprint(
            123,
            2,
            false,
            &["76a914aa88ac".to_string(), "76a914bb88ac".to_string()],
        );
        let b = request_fingerprint(
            123,
            2,
            false,
            &["76a914aa88ac".to_string(), "76a914cc88ac".to_string()],
        );
        assert_ne!(a, b);
    }

    /// CS-723: a full store drops completed records, never one in flight: a
    /// request of unknown outcome stays in flight so its retry is told so,
    /// and dropping it let the retry fund again.
    #[test]
    fn cs_723_eviction_never_drops_a_record_in_flight() {
        let mut store = IdempotencyStore::new(Duration::from_secs(600), 3);
        let key = |client: &str, k: &str| RecordKey::new(client, k);

        // In flight: an outcome-unknown broadcast, left to expire.
        assert_eq!(
            store.reserve(key("a", "unknown"), "f"),
            Reservation::Proceed
        );
        // Completed records from the same client fill the store.
        for k in ["done-1", "done-2"] {
            assert_eq!(store.reserve(key("a", k), "f"), Reservation::Proceed);
            store.complete(key("a", k), "f", Outcome::Funded(response(k)));
        }
        // More arrive, and each needs room.
        for k in ["next-1", "next-2", "next-3"] {
            assert_eq!(store.reserve(key("a", k), "f"), Reservation::Proceed);
        }
        assert_eq!(
            store.reserve(key("a", "unknown"), "f"),
            Reservation::InProgress,
            "the in-flight record was evicted, so its retry would fund again"
        );
        // Over the bound by the records in flight, and no more.
        assert_eq!(store.len(), 4);
    }
}
