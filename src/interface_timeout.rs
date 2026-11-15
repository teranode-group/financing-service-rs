//! A deadline on every blockchain interface call (CS-502).
//!
//! chain-gang's WhatsOnChain client calls `reqwest::get`, which has no timeout,
//! so a read that is accepted and never answered waits forever. Since chain
//! state refreshes are single-flighted per client (SR-FUND-032), that one read
//! also holds the client's refresh lock forever: every request that needs a
//! fresh read queues behind it, the periodic sweep skips the client, and
//! nothing is logged. Bounding each call here covers every interface and every
//! caller -- refreshes, the startup status check, the sweep -- without
//! depending on how a given interface builds its HTTP client.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chain_gang::interface::{Balance, BlockchainInterface, Utxo};
use chain_gang::messages::{BlockHeader, Tx};
use chain_gang::network::Network;
use chain_gang::util::ChainGangError;

/// Wraps a blockchain interface and fails any call that outlives `timeout`.
pub struct Bounded {
    inner: Arc<dyn BlockchainInterface + Send + Sync>,
    timeout: Duration,
    scope: Scope,
}

/// Which calls a [`Bounded`] puts its deadline on (CS-718).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// Every call.
    All,
    /// Every call but `broadcast_tx`, which is bounded further in, past the
    /// wait for a pacing slot.
    Reads,
    /// `broadcast_tx` alone, inside the pacing, so its deadline starts once
    /// its slot is granted.
    Broadcasts,
}

/// The interface call a [`Bounded`] is bounding: what its [`Scope`] tells
/// apart, and what a timeout names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Call {
    Status,
    GetBalance,
    GetUtxo,
    BroadcastTx,
    GetTx,
    GetLatestBlockHeader,
    GetBlockHeaders,
}

impl Call {
    fn name(self) -> &'static str {
        match self {
            Call::Status => "status",
            Call::GetBalance => "get_balance",
            Call::GetUtxo => "get_utxo",
            Call::BroadcastTx => "broadcast_tx",
            Call::GetTx => "get_tx",
            Call::GetLatestBlockHeader => "get_latest_block_header",
            Call::GetBlockHeaders => "get_block_headers",
        }
    }
}

impl Bounded {
    #[cfg(test)]
    pub fn new(inner: Arc<dyn BlockchainInterface + Send + Sync>, timeout: Duration) -> Self {
        Self::covering(inner, timeout, Scope::All)
    }

    pub fn covering(
        inner: Arc<dyn BlockchainInterface + Send + Sync>,
        timeout: Duration,
        scope: Scope,
    ) -> Self {
        Self {
            inner,
            timeout,
            scope,
        }
    }

    async fn bound<T>(
        &self,
        call: Call,
        future: impl Future<Output = Result<T, ChainGangError>>,
    ) -> Result<T, ChainGangError> {
        let broadcast = call == Call::BroadcastTx;
        let covered = match self.scope {
            Scope::All => true,
            Scope::Reads => !broadcast,
            Scope::Broadcasts => broadcast,
        };
        if !covered {
            return future.await;
        }
        match tokio::time::timeout(self.timeout, future).await {
            Ok(result) => result,
            Err(_) => Err(timed_out(call.name(), self.timeout)),
        }
    }
}

/// The error a call that ran out of time is reported with.
///
/// chain-gang has no timeout variant, so it is an I/O error of kind
/// `TimedOut`, which [`is_timeout`] recognises.
fn timed_out(call: &str, after: Duration) -> ChainGangError {
    ChainGangError::IoError(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!(
            "{call} did not answer within {}s (blockchain_interface.timeout_seconds)",
            after.as_secs()
        ),
    ))
}

/// Whether `error` says a call may have been carried out although it failed:
/// it ran out of time (here, or in the interface's own HTTP client) or lost its
/// answer after the request was sent.
///
/// For a broadcast that is the difference between "nothing was spent" and "the
/// transaction may be on the network" (CS-501, CS-502). An HTTP error status
/// is the server answering, so it is not one of these.
pub fn may_have_been_delivered(error: &ChainGangError) -> bool {
    match error {
        ChainGangError::IoError(e) => e.kind() == std::io::ErrorKind::TimedOut,
        ChainGangError::ReqwestError(e) => e.is_timeout() || (e.is_request() && !e.is_connect()),
        _ => false,
    }
}

/// Whether `error` is a call running out of time.
#[cfg(test)]
pub fn is_timeout(error: &ChainGangError) -> bool {
    matches!(error, ChainGangError::IoError(e) if e.kind() == std::io::ErrorKind::TimedOut)
}

#[async_trait]
impl BlockchainInterface for Bounded {
    fn set_network(&mut self, _network: &Network) {
        // The wrapped interface is configured before it is wrapped, as for
        // `RateLimited`.
    }

    async fn status(&self) -> Result<(), ChainGangError> {
        self.bound(Call::Status, self.inner.status()).await
    }

    async fn get_balance(&self, address: &str) -> Result<Balance, ChainGangError> {
        self.bound(Call::GetBalance, self.inner.get_balance(address))
            .await
    }

    async fn get_utxo(&self, address: &str) -> Result<Utxo, ChainGangError> {
        self.bound(Call::GetUtxo, self.inner.get_utxo(address))
            .await
    }

    async fn broadcast_tx(&self, tx: &Tx) -> Result<String, ChainGangError> {
        self.bound(Call::BroadcastTx, self.inner.broadcast_tx(tx))
            .await
    }

    async fn get_tx(&self, txid: &str) -> Result<Tx, ChainGangError> {
        self.bound(Call::GetTx, self.inner.get_tx(txid)).await
    }

    async fn get_latest_block_header(&self) -> Result<BlockHeader, ChainGangError> {
        self.bound(
            Call::GetLatestBlockHeader,
            self.inner.get_latest_block_header(),
        )
        .await
    }

    async fn get_block_headers(&self) -> Result<String, ChainGangError> {
        self.bound(Call::GetBlockHeaders, self.inner.get_block_headers())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{test_config, unique_dynamic_config_path, GatedBlockchain};

    /// CS-502. A read that is accepted and never answered fails at the
    /// deadline, as a timeout, rather than waiting forever.
    #[tokio::test]
    async fn a_read_that_never_answers_times_out() {
        let config = test_config(&unique_dynamic_config_path());
        let gated = GatedBlockchain::new(&config).await;
        gated.close();
        let bounded = Bounded::new(gated.clone(), Duration::from_millis(50));

        // Bounded from outside as well, so a regression fails here instead
        // of hanging the suite -- the very failure this guards against.
        let error = tokio::time::timeout(Duration::from_secs(5), bounded.get_utxo("anything"))
            .await
            .expect("the read was not bounded")
            .unwrap_err();
        assert!(is_timeout(&error), "{error}");
        assert!(may_have_been_delivered(&error));
        gated.open();
    }

    /// A call that answers in time is passed through untouched.
    #[tokio::test]
    async fn a_call_inside_the_deadline_is_unchanged() {
        let config = test_config(&unique_dynamic_config_path());
        let gated = GatedBlockchain::new(&config).await;
        let bounded = Bounded::new(gated.clone(), Duration::from_secs(5));
        bounded
            .get_utxo(crate::test_support::TEST_ADDRESS)
            .await
            .unwrap();
        assert_eq!(gated.utxo_read_count(), 1);
    }
}
