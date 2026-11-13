//! Broadcasting through the configured chain-gang `BlockchainInterface`.
//!
//! This is the service's original write path, kept behind the
//! [`TxBroadcaster`] seam so it is interchangeable with mapi-lite. In
//! production the wrapped interface is WhatsOnChain (`interface_type = "woc"`),
//! hence the name; with `uaas`, `rpc` or `test` configured it wraps that
//! interface instead, and reports that name.

use std::sync::Arc;

use async_trait::async_trait;
use chain_gang::{interface::BlockchainInterface, messages::Tx, util::ChainGangError};

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
        // Not one that may have reached the network, though (CS-502, CS-718):
        // reporting it as failed would release inputs it spent (CS-501).
        self.inner.broadcast_tx(tx).await.map_err(|e| {
            if broadcast_may_have_been_delivered(&e) {
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

    /// `rpc` keeps the node's reject reason in its error, and from chain-gang
    /// 0.13.2 `woc` and `uaas` keep the refusal's body, where the node's
    /// reason is (CS-701). The `test` interface keeps none.
    fn reads_node_reasons(&self) -> bool {
        matches!(self.name.as_str(), "rpc" | "woc" | "uaas")
    }
}

/// Whether a failed broadcast may still have reached the network (CS-718).
///
/// Beyond a call that ran out of time or lost its answer after sending
/// ([`crate::interface_timeout::may_have_been_delivered`]):
///
/// * **An answer cut off once it had begun.** Its status was already sent,
///   which for `woc` and `uaas` means a 200: they read the body only then.
///   For `rpc` it can be any status, and reading the node's verdict is what
///   failed.
/// * **A 5xx other than 503.** chain-gang turns every status but 200 into
///   `ResponseError("response.status() = 502 Bad Gateway")`, followed from
///   0.13.2 by the body (CS-701). A gateway's 502 or 504 can follow WhatsOnChain relaying
///   the transaction, and a 500 can be WhatsOnChain failing after it. A 503
///   is a server turning the request away, and a 4xx is it refusing it.
/// * **An answer that is not one.** `rpc` reads the node's body for every
///   status; one it cannot parse is a node, or something in front of it, that
///   may have taken the transaction first, unless its status says the request
///   was turned away.
///
/// `rpc`'s own refusals are JSON-RPC errors (`sendrawtransaction failed with
/// code ..`), the node's verdict, and stay definite.
pub fn broadcast_may_have_been_delivered(error: &ChainGangError) -> bool {
    if crate::interface_timeout::may_have_been_delivered(error) {
        return true;
    }
    match error {
        ChainGangError::ReqwestError(e) => e.is_body() || e.is_decode(),
        ChainGangError::ResponseError(message) => {
            status_in(message, "response.status() = ").is_some_and(maybe_relayed)
        }
        ChainGangError::JSONParseError(message) => {
            !status_in(message, "returned status ").is_some_and(turned_away)
        }
        _ => false,
    }
}

/// The HTTP status chain-gang wrote after `prefix` in an error message.
fn status_in(message: &str, prefix: &str) -> Option<u16> {
    let rest = &message[message.find(prefix)? + prefix.len()..];
    rest.get(..3)?.parse().ok()
}

/// A status after which the transaction may have been relayed anyway.
fn maybe_relayed(status: u16) -> bool {
    (500..600).contains(&status) && status != 503
}

/// A status saying the request was not taken: refused, or turned away.
fn turned_away(status: u16) -> bool {
    (400..500).contains(&status) || status == 503
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
    /// `rpc` keeps the node's message, and `woc` (and `uaas`) the HTTP status
    /// followed by the body (chain-gang 0.13.2, `woc_interface.rs`).
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

    /// Through `woc` (and `uaas`) the same refusal arrives with the node's
    /// reason after the HTTP status (chain-gang 0.13.2), so it is read as a
    /// conflict, as through `rpc` (CS-701). Before 0.13.2 the status came
    /// alone, and the service had to treat any refusal as one that may have
    /// been (#160 review).
    #[tokio::test]
    async fn cs_701_a_conflict_through_woc_is_a_conflict() {
        let blockchain = Arc::new(ConflictingBlockchain {
            inner: tokio::sync::Mutex::new(chain_gang::interface::TestInterface::new()),
            // chain-gang 0.13.2's `woc_interface.rs` on a non-200 answer.
            error: || {
                chain_gang::util::ChainGangError::ResponseError(
                    "response.status() = 400 Bad Request: 258: txn-mempool-conflict".to_string(),
                )
            },
        });
        for name in ["woc", "uaas"] {
            let broadcaster = WocBroadcaster::new(blockchain.clone(), name);
            assert!(broadcaster.reads_node_reasons(), "{name}");
            let error = broadcaster
                .broadcast_tx(&sample_tx())
                .await
                .expect_err("refused");
            assert!(error.inputs_conflicted(), "{name} classified as: {error}");
            assert!(!error.inputs_missing(), "{name} classified as: {error}");
        }
        // Only the `test` interface has no reason to read.
        let test = WocBroadcaster::new(blockchain, "test");
        assert!(!test.reads_node_reasons());
    }

    /// CS-701: a refusal from a real UaaS endpoint, through chain-gang, reaches
    /// the service with the node's reason in its text: a conflict and a
    /// missing input are each read as such. A 502 carrying the same words is
    /// still an unknown outcome (CS-718), which is never read.
    #[tokio::test]
    async fn cs_701_a_refusal_reaches_the_service_with_the_nodes_reason() {
        let (_s, broadcaster) = uaas_answering(
            wiremock::ResponseTemplate::new(400).set_body_string("258: txn-mempool-conflict"),
        )
        .await;
        let conflict = broadcaster.broadcast_tx(&sample_tx()).await.unwrap_err();
        assert!(
            matches!(conflict, BroadcastError::Upstream(_)),
            "{conflict:?}"
        );
        assert!(
            conflict.to_string().contains("txn-mempool-conflict"),
            "{conflict}"
        );
        assert!(
            conflict.inputs_conflicted() && !conflict.inputs_missing(),
            "{conflict}"
        );

        let (_s, broadcaster) = uaas_answering(
            wiremock::ResponseTemplate::new(400).set_body_string("16: missing-inputs"),
        )
        .await;
        let missing = broadcaster.broadcast_tx(&sample_tx()).await.unwrap_err();
        assert!(
            missing.inputs_missing() && !missing.inputs_conflicted(),
            "{missing}"
        );

        let (_s, broadcaster) = uaas_answering(
            wiremock::ResponseTemplate::new(502).set_body_string("258: txn-mempool-conflict"),
        )
        .await;
        let gateway = broadcaster.broadcast_tx(&sample_tx()).await.unwrap_err();
        assert!(
            matches!(gateway, BroadcastError::Indeterminate(_)),
            "{gateway:?}"
        );
        assert!(!gateway.inputs_conflicted(), "{gateway}");
    }

    // --- CS-718: what a failed HTTP broadcast says about delivery ---------

    /// A `uaas` broadcaster against a server answering every broadcast with
    /// `response`. `uaas` is chain-gang's WhatsOnChain client in every way
    /// that matters here (`woc_interface.rs` and `uaas_interface.rs` share
    /// the code), with a URL a test can point at.
    async fn uaas_answering(
        response: wiremock::ResponseTemplate,
    ) -> (wiremock::MockServer, WocBroadcaster) {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .respond_with(response)
            .mount(&server)
            .await;
        let uaas = chain_gang::interface::UaaSInterface::new(&server.uri()).unwrap();
        (server, WocBroadcaster::new(Arc::new(uaas), "uaas"))
    }

    /// CS-718: a gateway's 502 or 504, or a 500, may follow the transaction
    /// being relayed, so it is an unknown outcome and its inputs stay
    /// reserved; a 503 or a 4xx is a server not taking it, so nothing was
    /// spent.
    #[tokio::test]
    async fn cs_718_a_5xx_but_503_is_an_unknown_outcome_and_a_503_or_4xx_a_failure() {
        for status in [500, 502, 504] {
            let (_server, broadcaster) =
                uaas_answering(wiremock::ResponseTemplate::new(status)).await;
            let error = broadcaster.broadcast_tx(&sample_tx()).await.unwrap_err();
            assert!(
                matches!(error, BroadcastError::Indeterminate(_)),
                "{status}: {error:?}"
            );
        }
        for status in [503, 400, 404, 422] {
            let (_server, broadcaster) =
                uaas_answering(wiremock::ResponseTemplate::new(status)).await;
            let error = broadcaster.broadcast_tx(&sample_tx()).await.unwrap_err();
            assert!(
                matches!(error, BroadcastError::Upstream(_)),
                "{status}: {error:?}"
            );
        }
    }

    /// CS-718: a 200 whose body is cut off: the server took the transaction
    /// and its answer was lost, so the outcome is unknown.
    #[tokio::test]
    async fn cs_718_a_body_cut_off_after_a_200_is_an_unknown_outcome() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0u8; 64 * 1024];
            let _ = socket.read(&mut request).await;
            // 64 bytes promised, 10 sent, then the connection closes.
            socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 64\r\n\r\n\"abcdef12")
                .await
                .unwrap();
            socket.shutdown().await.unwrap();
        });

        let uaas = chain_gang::interface::UaaSInterface::new(&format!("http://{address}")).unwrap();
        let broadcaster = WocBroadcaster::new(Arc::new(uaas), "uaas");
        let error = broadcaster.broadcast_tx(&sample_tx()).await.unwrap_err();
        assert!(
            matches!(error, BroadcastError::Indeterminate(_)),
            "{error:?}"
        );
        server.await.unwrap();
    }

    /// CS-718: through `rpc`, the node's own refusal, a JSON-RPC error on a
    /// 500, stays a failure; a proxy's 502 page in front of it is an unknown
    /// outcome, and its 503 page a failure.
    #[tokio::test]
    async fn cs_718_rpc_keeps_the_nodes_refusal_definite() {
        let rpc = |response: wiremock::ResponseTemplate| async move {
            let server = wiremock::MockServer::start().await;
            wiremock::Mock::given(wiremock::matchers::method("POST"))
                .respond_with(response)
                .mount(&server)
                .await;
            let rpc = chain_gang::interface::RpcInterface::new(
                &server.uri(),
                "user",
                "pass",
                chain_gang::network::Network::BSV_Regtest,
            );
            let error = WocBroadcaster::new(Arc::new(rpc), "rpc")
                .broadcast_tx(&sample_tx())
                .await
                .unwrap_err();
            (server, error)
        };

        let (_s, refused) = rpc(wiremock::ResponseTemplate::new(500).set_body_json(
            serde_json::json!({
                "result": null,
                "error": { "code": -26, "message": "258: txn-mempool-conflict" },
                "id": "chain-gang",
            }),
        ))
        .await;
        assert!(
            !matches!(refused, BroadcastError::Indeterminate(_)),
            "{refused:?}"
        );

        let (_s, gateway) =
            rpc(wiremock::ResponseTemplate::new(502).set_body_string("<html>Bad Gateway</html>"))
                .await;
        assert!(
            matches!(gateway, BroadcastError::Indeterminate(_)),
            "{gateway:?}"
        );

        let (_s, busy) =
            rpc(wiremock::ResponseTemplate::new(503).set_body_string("<html>Unavailable</html>"))
                .await;
        assert!(
            !matches!(busy, BroadcastError::Indeterminate(_)),
            "{busy:?}"
        );
    }

    #[test]
    fn cs_718_the_status_is_read_from_chain_gangs_messages() {
        assert_eq!(
            status_in(
                "response.status() = 502 Bad Gateway",
                "response.status() = "
            ),
            Some(502)
        );
        assert_eq!(
            status_in(
                "sendrawtransaction returned status 200 OK and a body that is not a JSON-RPC \
                 response: eof",
                "returned status "
            ),
            Some(200)
        );
        assert_eq!(status_in("no status here", "response.status() = "), None);
        assert_eq!(
            status_in("response.status() = 5", "response.status() = "),
            None
        );
    }
}
