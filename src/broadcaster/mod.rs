//! Sending signed funding transactions to the network.
//!
//! The service reads chain state (balances, UTXOs) through chain-gang's
//! `BlockchainInterface` and, until now, broadcast through the same object.
//! The two are separate concerns with separate failure modes -- a public
//! explorer that answers reads fine may still be the wrong place to hand a
//! transaction -- so the write path has its own seam here.
//!
//! Two implementations exist. [`woc::WocBroadcaster`] wraps whatever
//! `BlockchainInterface` the `[blockchain_interface]` section configured (WoC
//! in production), which is exactly the behaviour the service had before this
//! module existed. [`mapi::MapiBroadcaster`] hands transactions to a mapi-lite
//! server instead, and is selected by the presence of a `[mapi_lite]` section
//! -- see [`factory::broadcaster_factory`]. Reads stay on the blockchain
//! interface either way.

pub mod factory;
pub mod mapi;
pub mod woc;

use async_trait::async_trait;
use chain_gang::messages::Tx;

/// [`TxBroadcaster::name`] of the mapi-lite implementation. The service uses
/// it to decide whether `GET /ready` has an upstream to probe.
pub const MAPI_LITE: &str = "mapi-lite";

/// Why a broadcast did not succeed.
///
/// The split matters to a caller deciding what to do next, and to this service
/// deciding what to believe about its own wallet. A rejection is the upstream's
/// verdict on the transaction; an upstream error says nothing about the
/// transaction at all; an indeterminate outcome says the transaction may be on
/// the network and we cannot find out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BroadcastError {
    /// The upstream answered and refused the transaction.
    Rejected {
        description: String,
        /// The upstream's own view of whether resubmitting could succeed.
        retryable: bool,
    },
    /// The upstream could not be reached or did not answer usably: transport
    /// failure, HTTP error status, undecodable body, bad response signature.
    Upstream(String),
    /// The transaction was handed to the upstream and its fate is unknown.
    ///
    /// Distinct from [`BroadcastError::Upstream`] because it is not a failure,
    /// it is an absence of information, and the two call for opposite
    /// handling. A transaction that never left cannot have spent anything; a
    /// transaction that may have left may have spent its inputs, and the
    /// service must not offer those inputs to the next funding request.
    Indeterminate(String),
    /// Not sent: the upstream signs with a key `[mapi_lite]
    /// miner_id_public_keys` does not list, so its answer could not be
    /// believed and a broadcast could only end in an unknown outcome
    /// (CS-618). Nothing reached the network; an operator has to fix the key
    /// list or mapi-lite's key.
    Untrusted(String),
}

impl BroadcastError {
    /// The text a node's reject reason can be read from, lowercased: a
    /// refusal's description, or an upstream error's detail.
    ///
    /// [`woc::WocBroadcaster`] reports a node's refusal as
    /// [`BroadcastError::Upstream`], because chain-gang's interfaces do not
    /// tell a refusal from a transport failure, so the reason is only in the
    /// text. That is safe to read: a transport failure never carries a node's
    /// reject reason. An indeterminate or untrusted outcome is never read,
    /// since it says nothing definite about the inputs.
    fn node_reason(&self) -> Option<String> {
        match self {
            BroadcastError::Rejected { description, .. } => Some(description.to_ascii_lowercase()),
            BroadcastError::Upstream(detail) => Some(detail.to_ascii_lowercase()),
            BroadcastError::Indeterminate(_) | BroadcastError::Untrusted(_) => None,
        }
    }

    /// Whether the node refused the transaction because an input is already
    /// spent by another transaction (CS-691), as SV Node's
    /// `258 txn-mempool-conflict` or a double-spend reason says, through
    /// mapi-lite or through the blockchain interface ([`Self::node_reason`]).
    pub fn inputs_conflicted(&self) -> bool {
        let Some(description) = self.node_reason() else {
            return false;
        };
        description.contains("txn-mempool-conflict")
            || description.contains("double-spend")
            || description.contains("conflicted with")
    }

    /// Whether the upstream refused the transaction because an input does not
    /// exist (CS-506).
    ///
    /// Matched on the node's reject reason as mapi-lite passes it through --
    /// `16 missing-inputs` from SV Node, `bad-txns-inputs-missingorspent` from
    /// its consensus check -- since neither the node nor mapi-lite gives it a
    /// code of its own. Read through the blockchain interface too
    /// ([`Self::node_reason`]). An outcome that may never have reached the
    /// node says nothing about the inputs.
    pub fn inputs_missing(&self) -> bool {
        let Some(description) = self.node_reason() else {
            return false;
        };
        description.contains("missing-inputs")
            || description.contains("missing inputs")
            || description.contains("missingorspent")
    }
}

impl std::fmt::Display for BroadcastError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BroadcastError::Rejected {
                description,
                retryable,
            } => write!(f, "rejected: {description} (retryable: {retryable})"),
            BroadcastError::Upstream(detail) => write!(f, "upstream error: {detail}"),
            BroadcastError::Indeterminate(detail) => {
                write!(f, "outcome unknown: {detail}")
            }
            BroadcastError::Untrusted(detail) => write!(f, "not sent: {detail}"),
        }
    }
}

impl std::error::Error for BroadcastError {}

/// Where funding transactions are sent.
///
/// One implementation is chosen at startup from configuration and shared by
/// every funding request. Implementations must be safe to call concurrently:
/// the service does not serialise broadcasts.
#[async_trait]
pub trait TxBroadcaster: Send + Sync {
    /// Short name for logs and `GET /status`: the `interface_type` for the
    /// blockchain-interface broadcaster, [`MAPI_LITE`] for mapi-lite.
    fn name(&self) -> &str;

    /// Send `tx` to the network. Returns the txid (hex) as the upstream
    /// reported it, which for a well-behaved upstream equals `tx.hash()`.
    async fn broadcast_tx(&self, tx: &Tx) -> Result<String, BroadcastError>;

    /// Reachability probe, used at startup and by `GET /ready`. Should be
    /// cheap and bounded in time: a readiness probe allows a few seconds in
    /// total.
    async fn health_check(&self) -> Result<(), BroadcastError>;

    /// The rate this upstream says it charges, in satoshis per kilobyte, for
    /// the service to cost the transactions it builds (CS-451).
    ///
    /// `None` is the answer for an upstream with nothing to say about fees --
    /// broadcasting through the blockchain interface asks no miner anything --
    /// and leaves the configured rate standing. The default is `None` so that
    /// a broadcaster only implements this if it genuinely has a quote.
    async fn fee_satoshis_per_kb(&self) -> Option<u64> {
        None
    }

    /// Whether the nodes behind this upstream know transaction `txid`
    /// (CS-619): `Some(false)` when they answer that they do not. `None` for
    /// an upstream with no such question to ask -- broadcasting through the
    /// blockchain interface reads the same chain as the wallet, so it cannot
    /// disagree with it -- or when the question could not be answered.
    async fn knows_tx(&self, _txid: &str) -> Option<bool> {
        None
    }

    /// Whether this upstream refused the token on its last authenticated
    /// probe (CS-632). Read at startup, after a health check.
    fn token_refused(&self) -> bool {
        false
    }

    /// The network this upstream last said its nodes are on, as `mainnet`,
    /// `testnet`, `regtest` or `stn` (CS-624): mapi-lite publishes it in
    /// `/ready`, which [`TxBroadcaster::health_check`] reads. `None` before
    /// it has said, or for an upstream that reads the wallet's own chain.
    fn published_network(&self) -> Option<String> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The response body carries a fixed description, so this Display is the
    /// only place an operator learns *why* the upstream refused a transaction
    /// and whether it thought a retry could help. `Service` logs the error
    /// through it on every broadcast failure.
    /// CS-506: the node's missing-inputs refusal, in the forms it reaches
    /// this service, and nothing else.
    #[test]
    fn inputs_missing_is_read_off_a_refusal_only() {
        let rejected = |description: &str| BroadcastError::Rejected {
            description: description.to_string(),
            retryable: false,
        };
        // Verbatim from mapi-lite v0.3.0 over an SV Node regtest, 2026-10-09.
        assert!(
            rejected("Mempool error, retry again later. (details: 16 missing-inputs)")
                .inputs_missing()
        );
        assert!(rejected("16 missing-inputs").inputs_missing());
        assert!(rejected("Missing inputs").inputs_missing());
        assert!(rejected("bad-txns-inputs-missingorspent").inputs_missing());
        assert!(!rejected("258 txn-mempool-conflict").inputs_missing());
        assert!(rejected("258 txn-mempool-conflict").inputs_conflicted());
        assert!(
            rejected("Mempool error (details: 258 txn-mempool-conflict) (conflicted with ab)")
                .inputs_conflicted()
        );
        assert!(!rejected("16 missing-inputs").inputs_conflicted());
        // Through the blockchain interface a node's refusal is `Upstream`, as
        // `WocBroadcaster` reports chain-gang's error (#160 review).
        let upstream = |detail: &str| BroadcastError::Upstream(detail.to_string());
        assert!(
            upstream("A provided argument is not valid `258: txn-mempool-conflict`")
                .inputs_conflicted()
        );
        assert!(upstream("16: missing-inputs").inputs_missing());
        assert!(!upstream("http status 503").inputs_conflicted());
        assert!(!upstream("http status 503").inputs_missing());
        // An outcome that may never have reached the node says nothing.
        assert!(
            !BroadcastError::Indeterminate("txn-mempool-conflict".to_string()).inputs_conflicted()
        );
        assert!(!BroadcastError::Indeterminate("missing-inputs".to_string()).inputs_missing());
        assert!(!BroadcastError::Untrusted("txn-mempool-conflict".to_string()).inputs_conflicted());
    }

    #[test]
    fn broadcast_error_display_names_the_kind_of_failure() {
        let rejected = BroadcastError::Rejected {
            description: "txn-mempool-conflict".to_string(),
            retryable: false,
        };
        assert_eq!(
            rejected.to_string(),
            "rejected: txn-mempool-conflict (retryable: false)"
        );
        let retryable = BroadcastError::Rejected {
            description: "mempool full".to_string(),
            retryable: true,
        };
        assert_eq!(
            retryable.to_string(),
            "rejected: mempool full (retryable: true)"
        );
        let upstream = BroadcastError::Upstream("http status 503".to_string());
        assert_eq!(upstream.to_string(), "upstream error: http status 503");
        let unknown = BroadcastError::Indeterminate("no answer in 45s".to_string());
        assert_eq!(unknown.to_string(), "outcome unknown: no answer in 45s");
    }
}
