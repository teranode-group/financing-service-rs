//! A child span for every blockchain interface call (CS-700).
//!
//! A `POST /fund` trace used to be a single request span, so a slow funding
//! did not show whether the time went on chain reads (WhatsOnChain, UaaS, a
//! node, mapi-lite's chain endpoints) or the broadcast. [`Traced`] wraps the
//! interface outermost and runs each call inside a client span named after
//! it, so the time a call spends waiting for its pacing slot or running into
//! its deadline is inside the span too. Under a request the span is a child of
//! the request's, and so is the background refresh a request prompts. A call
//! made under no span -- the periodic sweep, the startup reads -- is not
//! traced: each would be a one-span trace of its own, kept around the clock.
//!
//! The span is named `chain <call>` (`chain get_utxo`, `chain broadcast_tx`,
//! ...) and carries the interface and the call, never the address or txid it
//! was asked about. A call that goes to mapi-rs (the `mapi_lite` interface)
//! also carries `peer.service = "mapi-rs"`, the callee's deployed
//! `service.name`, so Tempo's service graph draws the edge to the mapi-rs
//! node. A failed call marks the span an error, with `error.type` and a
//! description from the kind of failure, never its message; so does a call
//! dropped before it finished, as `cancelled`.
//!
//! The span is entered on every poll of the call, so a client that sends HTTP
//! through uls-client -- the `mapi_lite` interface -- injects its
//! `traceparent`, and mapi-lite's spans join this trace.

use std::future::Future;
use std::sync::Arc;

use crate::telemetry::{in_span, SpanError, MAPI_RS_PEER_SERVICE, SPAN_TARGET};
use async_trait::async_trait;
use chain_gang::interface::{Balance, BlockchainInterface, Utxo};
use chain_gang::messages::{BlockHeader, Tx};
use chain_gang::network::Network;
use chain_gang::util::ChainGangError;

/// Wraps a blockchain interface and traces each call.
pub struct Traced {
    inner: Arc<dyn BlockchainInterface + Send + Sync>,
    /// `blockchain_interface.interface_type`, on every span.
    interface: &'static str,
}

impl Traced {
    pub fn new(inner: Arc<dyn BlockchainInterface + Send + Sync>, interface_type: &str) -> Self {
        Self {
            inner,
            interface: interface_name(interface_type),
        }
    }

    /// `name` is the span name, `chain <call>`; it is passed whole, not
    /// formatted, so it stays a `&'static str`.
    async fn traced<T>(
        &self,
        name: &'static str,
        call: &'static str,
        future: impl Future<Output = Result<T, ChainGangError>>,
    ) -> Result<T, ChainGangError> {
        // Only under a span: a request's, or the background refresh one
        // prompted. The periodic sweep and the startup reads run under none,
        // and would otherwise start a one-span trace per call, around the
        // clock, crowding the request traces out (CS-700 review).
        let span = if tracing::Span::current().is_none() {
            tracing::Span::none()
        } else {
            tracing::info_span!(
                target: SPAN_TARGET,
                "chain",
                otel.name = name,
                otel.kind = "client",
                otel.status_description = tracing::field::Empty,
                error.type = tracing::field::Empty,
                peer.service = tracing::field::Empty,
                chain.interface = self.interface,
                chain.call = call,
            )
        };
        if self.interface == MAPI_RS {
            span.record("peer.service", MAPI_RS_PEER_SERVICE);
        }
        in_span(span, future).await
    }
}

/// A chain call's error, as its span describes it.
impl SpanError for ChainGangError {
    fn error_type(&self) -> &'static str {
        match self {
            ChainGangError::Timeout => "timeout",
            ChainGangError::ReqwestError(_) | ChainGangError::IoError(_) => "transport",
            ChainGangError::ResponseError(_) => "response",
            ChainGangError::SerdeJSONParseError(_)
            | ChainGangError::JSONParseError(_)
            | ChainGangError::Utf8Error(_)
            | ChainGangError::HexError(_)
            | ChainGangError::BadData(_) => "bad_data",
            ChainGangError::BadArgument(_) | ChainGangError::URLParseError(_) => "bad_argument",
            _ => "other",
        }
    }

    fn description(&self) -> &'static str {
        match self.error_type() {
            "timeout" => "the chain call timed out",
            "transport" => "the chain backend could not be reached",
            "response" => "the chain backend answered with an error",
            "bad_data" => "the chain backend's answer could not be read",
            "bad_argument" => "the chain call was refused before it was sent",
            _ => "the chain call failed",
        }
    }
}

/// `chain.interface` for the `mapi_lite` interface: the service it reads
/// through, by its deployed name, as everywhere else in a trace.
const MAPI_RS: &str = "mapi-rs";

/// The interface types, as span attribute values from a fixed set, so a
/// misconfigured one cannot carry anything arbitrary into a trace.
fn interface_name(interface_type: &str) -> &'static str {
    match interface_type {
        "woc" => "woc",
        "uaas" => "uaas",
        "rpc" => "rpc",
        "mapi_lite" => MAPI_RS,
        "test" => "test",
        _ => "other",
    }
}

#[async_trait]
impl BlockchainInterface for Traced {
    fn set_network(&mut self, _network: &Network) {
        // The wrapped interface is configured before it is wrapped, as for
        // `Bounded` and `RateLimited`.
    }

    async fn status(&self) -> Result<(), ChainGangError> {
        self.traced("chain status", "status", self.inner.status())
            .await
    }

    async fn get_balance(&self, address: &str) -> Result<Balance, ChainGangError> {
        self.traced(
            "chain get_balance",
            "get_balance",
            self.inner.get_balance(address),
        )
        .await
    }

    async fn get_utxo(&self, address: &str) -> Result<Utxo, ChainGangError> {
        self.traced("chain get_utxo", "get_utxo", self.inner.get_utxo(address))
            .await
    }

    async fn broadcast_tx(&self, tx: &Tx) -> Result<String, ChainGangError> {
        self.traced(
            "chain broadcast_tx",
            "broadcast_tx",
            self.inner.broadcast_tx(tx),
        )
        .await
    }

    async fn get_tx(&self, txid: &str) -> Result<Tx, ChainGangError> {
        self.traced("chain get_tx", "get_tx", self.inner.get_tx(txid))
            .await
    }

    async fn get_latest_block_header(&self) -> Result<BlockHeader, ChainGangError> {
        self.traced(
            "chain get_latest_block_header",
            "get_latest_block_header",
            self.inner.get_latest_block_header(),
        )
        .await
    }

    async fn get_block_headers(&self) -> Result<String, ChainGangError> {
        self.traced(
            "chain get_block_headers",
            "get_block_headers",
            self.inner.get_block_headers(),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::trace::{SpanKind, Status};

    use super::*;
    use crate::test_support::{
        test_blockchain_interface, test_config, traced_spans, unique_dynamic_config_path,
        UnreadableBlockchain, TEST_ADDRESS,
    };
    use tracing::Instrument as _;

    /// Each call is a client span named after it, carrying the interface, a
    /// child of the span it was made under; a failed one is marked an error.
    #[tokio::test]
    async fn sr_tele_015_each_chain_call_is_a_child_span() {
        let config = test_config(&unique_dynamic_config_path());
        let ok = Traced::new(test_blockchain_interface(&config).await, "woc");
        let failing = Traced::new(UnreadableBlockchain::new(&config).await, "uaas");

        let spans = traced_spans(async {
            async {
                ok.get_utxo(TEST_ADDRESS).await.expect("read");
                failing
                    .get_utxo(TEST_ADDRESS)
                    .await
                    .expect_err("unreadable");
            }
            .instrument(tracing::info_span!("request"))
            .await;
        })
        .await;

        let parent = spans.iter().find(|s| s.name == "request").expect("parent");
        let on = |interface: &str| {
            spans
                .iter()
                .find(|s| {
                    s.name == "chain get_utxo"
                        && s.attributes.iter().any(|kv| {
                            kv.key.as_str() == "chain.interface" && kv.value.as_str() == interface
                        })
                })
                .unwrap_or_else(|| panic!("no chain get_utxo span on {interface}: {spans:?}"))
        };

        let read = on("woc");
        assert_eq!(read.parent_span_id, parent.span_context.span_id());
        assert_eq!(read.span_context.trace_id(), parent.span_context.trace_id());
        assert_eq!(read.span_kind, SpanKind::Client);
        assert!(
            !read
                .attributes
                .iter()
                .any(|kv| kv.value.as_str().contains(TEST_ADDRESS)),
            "no address on the span"
        );
        assert_eq!(read.status, Status::Unset);

        let failed = on("uaas");
        assert_eq!(failed.parent_span_id, parent.span_context.span_id());
        // CS-700 review, comment 6: the kind of failure, not its message,
        // which names the address.
        assert_eq!(
            failed.status,
            Status::error("the chain call was refused before it was sent")
        );
        assert_eq!(
            attribute(failed, "error.type").as_deref(),
            Some("bad_argument")
        );
        assert!(
            !failed
                .attributes
                .iter()
                .any(|kv| kv.value.as_str().contains(TEST_ADDRESS)),
            "no address on the span"
        );
    }

    /// A call through the `mapi_lite` interface goes to mapi-rs: the span
    /// names it by its deployed `service.name` in `chain.interface` and
    /// `peer.service`, so the service graph draws the edge to the mapi-rs
    /// node. Other interfaces carry no `peer.service`, and no span carries
    /// `mapi_lite` or `mapi-lite`.
    #[tokio::test]
    async fn sr_tele_015_a_call_to_mapi_rs_names_it_as_the_peer_service() {
        let config = test_config(&unique_dynamic_config_path());
        let via_mapi = Traced::new(test_blockchain_interface(&config).await, "mapi_lite");
        let via_woc = Traced::new(test_blockchain_interface(&config).await, "woc");

        let spans = traced_spans(async {
            async {
                via_mapi.get_utxo(TEST_ADDRESS).await.expect("read");
                via_woc.status().await.expect("status");
            }
            .instrument(tracing::info_span!("request"))
            .await;
        })
        .await;

        let attribute = |name: &str, key: &str| {
            let span = spans
                .iter()
                .find(|s| s.name == name)
                .unwrap_or_else(|| panic!("no {name} span: {spans:?}"));
            span.attributes
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .map(|kv| kv.value.as_str().into_owned())
        };
        assert_eq!(
            attribute("chain get_utxo", "chain.interface").as_deref(),
            Some("mapi-rs")
        );
        assert_eq!(
            attribute("chain get_utxo", "peer.service").as_deref(),
            Some("mapi-rs")
        );
        assert_eq!(
            attribute("chain get_utxo", "chain.call").as_deref(),
            Some("get_utxo")
        );
        assert_eq!(attribute("chain status", "peer.service"), None);
        for span in &spans {
            assert!(!span.name.contains("mapi_lite") && !span.name.contains("mapi-lite"));
            for kv in &span.attributes {
                let value = kv.value.as_str();
                assert!(
                    !value.contains("mapi_lite") && !value.contains("mapi-lite"),
                    "{kv:?}"
                );
            }
        }
    }

    /// CS-700 review, comment 2: a call made under no span -- the periodic
    /// sweep, the startup reads -- is not traced, so it does not start a
    /// one-span trace of its own every refresh period.
    #[tokio::test]
    async fn sr_tele_015_a_call_under_no_span_is_not_traced() {
        let config = test_config(&unique_dynamic_config_path());
        let interface = Traced::new(test_blockchain_interface(&config).await, "woc");

        let spans = traced_spans(async {
            interface.get_utxo(TEST_ADDRESS).await.expect("read");
        })
        .await;

        assert!(spans.is_empty(), "{spans:?}");
    }

    fn attribute(span: &opentelemetry_sdk::trace::SpanData, key: &str) -> Option<String> {
        span.attributes
            .iter()
            .find(|kv| kv.key.as_str() == key)
            .map(|kv| kv.value.as_str().into_owned())
    }

    /// CS-700 review, comment 6: a call dropped before it finished -- here
    /// by an outer deadline -- is marked an error, not left `Unset`.
    #[tokio::test]
    async fn sr_tele_015_a_cancelled_call_is_marked_an_error() {
        use crate::test_support::GatedBlockchain;

        let config = test_config(&unique_dynamic_config_path());
        let gated = GatedBlockchain::new(&config).await;
        let interface = Traced::new(gated.clone(), "woc");
        gated.close();

        let spans = traced_spans(async {
            async {
                let held = interface.get_utxo(TEST_ADDRESS);
                tokio::time::timeout(std::time::Duration::from_millis(20), held)
                    .await
                    .expect_err("held at the gate");
            }
            .instrument(tracing::info_span!("request"))
            .await;
        })
        .await;
        gated.open();

        let read = spans
            .iter()
            .find(|s| s.name == "chain get_utxo")
            .unwrap_or_else(|| panic!("no chain get_utxo span: {spans:?}"));
        assert!(
            matches!(read.status, Status::Error { .. }),
            "{:?}",
            read.status
        );
        assert_eq!(attribute(read, "error.type").as_deref(), Some("cancelled"));
    }
}
