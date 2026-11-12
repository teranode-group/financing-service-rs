//! Broadcasting through the configured chain-gang `BlockchainInterface`.
//!
//! This is the service's original write path, kept behind the
//! [`TxBroadcaster`] seam so it is interchangeable with mapi-lite. In
//! production the wrapped interface is WhatsOnChain (`interface_type = "woc"`),
//! hence the name; with `uaas`, `rpc` or `test` configured it wraps that
//! interface instead, and reports that name.

use std::sync::Arc;

use async_trait::async_trait;
use chain_gang::{interface::BlockchainInterface, messages::Tx};

use super::{BroadcastError, TxBroadcaster};

/// Hands transactions to the blockchain interface the service also reads from.
pub struct WocBroadcaster {
    inner: Arc<dyn BlockchainInterface + Send + Sync>,
    /// The configured `interface_type`, reported by [`TxBroadcaster::name`].
    name: String,
}

impl WocBroadcaster {
    /// Wrap `inner`, which `interface_type` selected.
    pub fn new(inner: Arc<dyn BlockchainInterface + Send + Sync>, interface_type: &str) -> Self {
        Self {
            inner,
            name: interface_type.to_string(),
        }
    }
}

#[async_trait]
impl TxBroadcaster for WocBroadcaster {
    fn name(&self) -> &str {
        &self.name
    }

    async fn broadcast_tx(&self, tx: &Tx) -> Result<String, BroadcastError> {
        // chain-gang's interfaces do not distinguish a rejection from a
        // transport failure in their error type, so most things are upstream.
        // Not a call that ran out of time or lost its answer after sending,
        // though (CS-502): the transaction may be on the network, and
        // reporting it as failed would release inputs it spent (CS-501).
        self.inner.broadcast_tx(tx).await.map_err(|e| {
            if crate::interface_timeout::may_have_been_delivered(&e) {
                BroadcastError::Indeterminate(e.to_string())
            } else {
                BroadcastError::Upstream(e.to_string())
            }
        })?;
        Ok(tx.hash().encode())
    }

    async fn health_check(&self) -> Result<(), BroadcastError> {
        self.inner
            .status()
            .await
            .map_err(|e| BroadcastError::Upstream(e.to_string()))
    }

    /// Only `rpc` keeps the node's reject reason in its error; chain-gang
    /// 0.13.1's `woc` and `uaas` keep only the HTTP status (CS-691).
    fn reads_node_reasons(&self) -> bool {
        self.name == "rpc"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        sample_tx, test_config, unique_dynamic_config_path, CountingBlockchain,
        FailingBroadcastBlockchain,
    };

    /// CS-502. A broadcast the interface never answers fails at the deadline,
    /// and as an unknown outcome: the transaction may be on the network, so
    /// its inputs must be reserved, not released (CS-501).
    #[tokio::test]
    async fn a_broadcast_that_runs_out_of_time_is_an_unknown_outcome() {
        use crate::interface_timeout::Bounded;
        use crate::test_support::GatedBlockchain;
        use std::time::Duration;

        let config = test_config(&unique_dynamic_config_path());
        let gated = GatedBlockchain::new(&config).await;
        gated.close();
        let bounded = Arc::new(Bounded::new(gated.clone(), Duration::from_millis(50)));
        let broadcaster = WocBroadcaster::new(bounded, "woc");

        let error = tokio::time::timeout(
            Duration::from_secs(5),
            broadcaster.broadcast_tx(&sample_tx()),
        )
        .await
        .expect("the broadcast was not bounded")
        .unwrap_err();
        assert!(
            matches!(error, BroadcastError::Indeterminate(_)),
            "{error:?}"
        );
        gated.open();
    }

    #[tokio::test]
    async fn woc_broadcaster_delegates_to_the_blockchain_interface() {
        let config = test_config(&unique_dynamic_config_path());
        let blockchain = CountingBlockchain::new(&config).await;
        let broadcaster = WocBroadcaster::new(blockchain.clone(), "test");
        let tx = sample_tx();

        let txid = broadcaster.broadcast_tx(&tx).await.expect("broadcast");

        assert_eq!(txid, tx.hash().encode());
        assert_eq!(blockchain.broadcast_count(), 1);
        assert_eq!(broadcaster.name(), "test");
    }

    #[tokio::test]
    async fn woc_broadcaster_reports_an_interface_failure_as_upstream() {
        let config = test_config(&unique_dynamic_config_path());
        let blockchain = FailingBroadcastBlockchain::new(&config, 0).await;
        let broadcaster = WocBroadcaster::new(blockchain, "test");

        let error = broadcaster
            .broadcast_tx(&sample_tx())
            .await
            .expect_err("the interface fails every broadcast");

        match error {
            BroadcastError::Upstream(detail) => assert!(detail.contains("simulated")),
            other => panic!("expected an upstream error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn woc_broadcaster_health_check_uses_the_interface_status() {
        let config = test_config(&unique_dynamic_config_path());
        let blockchain = CountingBlockchain::new(&config).await;
        let broadcaster = WocBroadcaster::new(blockchain, "test");
        assert!(broadcaster.health_check().await.is_ok());
    }

    /// A node behind the blockchain interface that refuses every broadcast
    /// as a mempool conflict, with the error the interface really returns:
    /// `rpc` keeps the node's message, and `woc` (and `uaas`) only the HTTP
    /// status, the body dropped (chain-gang 0.13.1, `woc_interface.rs`).
    struct ConflictingBlockchain {
        inner: tokio::sync::Mutex<chain_gang::interface::TestInterface>,
        error: fn() -> chain_gang::util::ChainGangError,
    }

    #[async_trait]
    impl BlockchainInterface for ConflictingBlockchain {
        fn set_network(&mut self, network: &chain_gang::network::Network) {
            if let Ok(mut inner) = self.inner.try_lock() {
                inner.set_network(network);
            }
        }
        async fn status(&self) -> Result<(), chain_gang::util::ChainGangError> {
            self.inner.lock().await.status().await
        }
        async fn get_balance(
            &self,
            address: &str,
        ) -> Result<chain_gang::interface::Balance, chain_gang::util::ChainGangError> {
            self.inner.lock().await.get_balance(address).await
        }
        async fn get_utxo(
            &self,
            address: &str,
        ) -> Result<chain_gang::interface::Utxo, chain_gang::util::ChainGangError> {
            self.inner.lock().await.get_utxo(address).await
        }
        async fn broadcast_tx(&self, _tx: &Tx) -> Result<String, chain_gang::util::ChainGangError> {
            Err((self.error)())
        }
        async fn get_tx(&self, txid: &str) -> Result<Tx, chain_gang::util::ChainGangError> {
            self.inner.lock().await.get_tx(txid).await
        }
        async fn get_latest_block_header(
            &self,
        ) -> Result<chain_gang::messages::BlockHeader, chain_gang::util::ChainGangError> {
            self.inner.lock().await.get_latest_block_header().await
        }
        async fn get_block_headers(&self) -> Result<String, chain_gang::util::ChainGangError> {
            self.inner.lock().await.get_block_headers().await
        }
    }

    /// CS-691, from the #160 review: without `[mapi_lite]`, a node's
    /// conflict refusal reaches the service through this broadcaster. Through
    /// `rpc` it is read as a conflict, so the own change it spent is not
    /// offered again.
    #[tokio::test]
    async fn cs_691_a_conflict_through_rpc_is_a_conflict() {
        let blockchain = Arc::new(ConflictingBlockchain {
            inner: tokio::sync::Mutex::new(chain_gang::interface::TestInterface::new()),
            // What chain-gang's RPC interface returns for SV Node's refusal.
            error: || {
                chain_gang::util::ChainGangError::ResponseError(
                    "sendrawtransaction failed with code -26: 258: txn-mempool-conflict"
                        .to_string(),
                )
            },
        });
        let broadcaster = WocBroadcaster::new(blockchain, "rpc");
        assert!(broadcaster.reads_node_reasons());
        let error = broadcaster
            .broadcast_tx(&sample_tx())
            .await
            .expect_err("refused");
        assert!(error.inputs_conflicted(), "classified as: {error}");
        assert!(!error.inputs_missing(), "classified as: {error}");
    }

    /// Through `woc` (and `uaas`) the same refusal arrives as the HTTP status
    /// alone: nothing says it was a conflict, and the broadcaster says it
    /// cannot read node reasons, so the service treats it as one that may
    /// have been (#160 review).
    #[tokio::test]
    async fn cs_691_a_conflict_through_woc_cannot_be_read() {
        let blockchain = Arc::new(ConflictingBlockchain {
            inner: tokio::sync::Mutex::new(chain_gang::interface::TestInterface::new()),
            // chain-gang 0.13.1's `woc_interface.rs` on a non-200 answer.
            error: || {
                chain_gang::util::ChainGangError::ResponseError(
                    "response.status() = 400 Bad Request".to_string(),
                )
            },
        });
        for name in ["woc", "uaas"] {
            let broadcaster = WocBroadcaster::new(blockchain.clone(), name);
            assert!(!broadcaster.reads_node_reasons(), "{name}");
            let error = broadcaster
                .broadcast_tx(&sample_tx())
                .await
                .expect_err("refused");
            assert!(!error.inputs_conflicted(), "classified as: {error}");
        }
    }
}
