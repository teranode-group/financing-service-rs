//! Telling a backend which addresses to care about.
//!
//! Most backends need no such thing: WhatsOnChain and UaaS index the whole
//! chain, so any address can be queried straight away. A node is different.
//! Balance and UTXO queries reach it through `listunspent`, which reports only
//! what the node's own wallet tracks, so an address the node has never heard
//! of reads as **zero, with no error** — the node is answering truthfully
//! about a wallet that does not know the address.
//!
//! That failure is the unpleasant kind: a reachable node, a successful
//! response, and a balance of nothing. So where a backend needs to be told,
//! the service tells it — at startup for configured clients, and again when a
//! client is added at runtime.

use async_trait::async_trait;
use chain_gang::interface::RpcInterface;

/// A backend that must be told which addresses to watch before it can report
/// anything about them.
///
/// Kept as a trait rather than reaching for `RpcInterface` directly so the
/// service does not depend on which backend is configured, and so the
/// behaviour can be tested without a node.
#[async_trait]
pub trait AddressWatcher: Send + Sync {
    /// Ask the backend to start tracking `address`.
    ///
    /// Implementations should be safe to call for an address already being
    /// watched, since the service calls this on every startup.
    async fn watch_address(&self, address: &str) -> Result<(), String>;
}

#[async_trait]
impl AddressWatcher for RpcInterface {
    async fn watch_address(&self, address: &str) -> Result<(), String> {
        // `false` skips the rescan. A rescan blocks the node's RPC connection
        // while it walks the chain, and the addresses handed to it here are
        // freshly derived client keys with no history to find.
        self.import_address(address, false)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Puts a deadline on another watcher's calls (CS-740).
///
/// `rpc`'s `importaddress` goes through a client with no timeout of its own,
/// as every chain-gang interface call does, which [`crate::interface_timeout`]
/// bounds. The watch was not bounded: a node that accepted the call and never
/// answered held up whatever waited on it -- at startup the first sweep, and
/// with it the periodic refresh and the fee-rate refresh, which then never
/// ran, and at runtime a `POST /client`. Out of time, the watch fails like
/// any refused import: warned about, and the service goes on (SR-BCHN-008).
pub struct BoundedWatcher {
    inner: std::sync::Arc<dyn AddressWatcher>,
    timeout: std::time::Duration,
}

impl BoundedWatcher {
    pub fn new(inner: std::sync::Arc<dyn AddressWatcher>, timeout: std::time::Duration) -> Self {
        Self { inner, timeout }
    }
}

#[async_trait]
impl AddressWatcher for BoundedWatcher {
    async fn watch_address(&self, address: &str) -> Result<(), String> {
        match tokio::time::timeout(self.timeout, self.inner.watch_address(address)).await {
            Ok(result) => result,
            Err(_) => Err(format!(
                "the node did not answer within {}s (blockchain_interface.timeout_seconds)",
                self.timeout.as_secs()
            )),
        }
    }
}

/// Records what it was asked to watch, and can be told to fail.
///
/// Lets the service's import behaviour be tested without a node.
#[cfg(test)]
pub struct RecordingWatcher {
    pub watched: std::sync::Mutex<Vec<String>>,
    pub fail: bool,
}

#[cfg(test)]
impl RecordingWatcher {
    pub fn new(fail: bool) -> Self {
        Self {
            watched: std::sync::Mutex::new(Vec::new()),
            fail,
        }
    }

    pub fn watched(&self) -> Vec<String> {
        self.watched.lock().unwrap().clone()
    }
}

#[cfg(test)]
#[async_trait]
impl AddressWatcher for RecordingWatcher {
    async fn watch_address(&self, address: &str) -> Result<(), String> {
        self.watched.lock().unwrap().push(address.to_string());
        if self.fail {
            return Err("simulated import failure".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Accepts the call and never answers, as a hung node does.
    struct SilentWatcher;

    #[async_trait]
    impl AddressWatcher for SilentWatcher {
        async fn watch_address(&self, _address: &str) -> Result<(), String> {
            std::future::pending().await
        }
    }

    /// CS-740: a watch the node never answers fails at the deadline, as a
    /// refused import does, instead of waiting forever.
    #[tokio::test]
    async fn cs_740_a_watch_the_node_never_answers_fails_at_the_deadline() {
        let bounded = BoundedWatcher::new(
            std::sync::Arc::new(SilentWatcher),
            std::time::Duration::from_millis(50),
        );
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            bounded.watch_address("mwxrVFs"),
        )
        .await
        .expect("the watch was not bounded")
        .unwrap_err();
        assert!(error.contains("did not answer"), "{error}");
    }

    /// One that answers in time is passed through, failure and all.
    #[tokio::test]
    async fn cs_740_a_watch_inside_the_deadline_is_unchanged() {
        let recording = std::sync::Arc::new(RecordingWatcher::new(false));
        let bounded = BoundedWatcher::new(recording.clone(), std::time::Duration::from_secs(5));
        bounded.watch_address("n1jaAsK").await.unwrap();
        assert_eq!(recording.watched(), vec!["n1jaAsK".to_string()]);

        let failing = BoundedWatcher::new(
            std::sync::Arc::new(RecordingWatcher::new(true)),
            std::time::Duration::from_secs(5),
        );
        assert_eq!(
            failing.watch_address("n1jaAsK").await.unwrap_err(),
            "simulated import failure"
        );
    }

    #[tokio::test]
    async fn a_watcher_records_what_it_was_asked_to_watch() {
        let watcher = RecordingWatcher::new(false);
        watcher.watch_address("n1jaAsK").await.unwrap();
        watcher.watch_address("mwxrVFs").await.unwrap();
        assert_eq!(
            watcher.watched(),
            vec!["n1jaAsK".to_string(), "mwxrVFs".to_string()]
        );
    }

    #[tokio::test]
    async fn a_failing_watcher_reports_the_error() {
        let watcher = RecordingWatcher::new(true);
        let error = watcher.watch_address("n1jaAsK").await.unwrap_err();
        assert!(error.contains("simulated import failure"));
        // still recorded, so a caller can see what was attempted
        assert_eq!(watcher.watched().len(), 1);
    }
}
