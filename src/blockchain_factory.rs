use std::sync::Arc;
use std::time::Duration;

use chain_gang::interface::{
    BlockchainInterface, RpcInterface, TestInterface, UaaSInterface, WocInterface,
};

use crate::address_watcher::AddressWatcher;
use crate::config::Config;
use crate::interface_timeout::{Bounded, Scope};
use crate::mapi_lite_interface::MapiLiteInterface;
use crate::outbound_rate_limit::RateLimited;

/// A configured backend, plus the means to tell it which addresses to watch
/// when it needs telling.
pub struct Backend {
    pub interface: Arc<dyn BlockchainInterface + Send + Sync>,
    /// `Some` only for backends that index nothing themselves. See
    /// [`crate::address_watcher`].
    pub address_watcher: Option<Arc<dyn AddressWatcher>>,
}

/// Whether the interface applies the configured limit to its own HTTP
/// requests, leaving nothing for [`RateLimited`] to do.
///
/// Only `woc` can. chain-gang 0.11.4 added
/// `WocInterface::set_max_requests_per_second`, which paces the requests
/// themselves; [`RateLimited`] can only pace the calls, and since 0.11.3 one
/// call has been many requests (CS-457). Where both are possible the inner one
/// is the one that tells the truth, and wrapping as well would pace twice for
/// no gain.
///
/// The other interfaces talk to a server the operator runs, so the published
/// limit that prompted this does not apply to them; they keep the decorator,
/// which is the only thing available. `mapi_lite` among them: mapi-lite paces
/// its own WhatsOnChain requests, so nothing is limited here unless set.
fn paces_its_own_requests(interface_type: &str) -> bool {
    interface_type == "woc"
}

/// Takes a config and returns the appropriate configured object that implements BlockchainInterface
pub fn blockchain_factory(config: &Config) -> Result<Backend, String> {
    let timeout = config.blockchain_interface.call_timeout();
    let (backend, paced_here) = paced_backend(config, timeout)?;
    // Outermost, so the deadline covers a read's wait for a pacing slot as
    // well as the read itself: what it bounds is how long a caller waits.
    // Not a broadcast's, where this service paces (CS-718): one that ran out
    // of time queued behind reads would be reported as possibly sent, and
    // hold its inputs for nothing. That one is bounded inside the pacing.
    match timeout {
        Some(timeout) => {
            log::info!(
                "bounding each blockchain interface call to {}s",
                timeout.as_secs()
            );
            let scope = if paced_here { Scope::Reads } else { Scope::All };
            Ok(Backend {
                interface: Arc::new(Bounded::covering(backend.interface, timeout, scope)),
                address_watcher: backend.address_watcher,
            })
        }
        None => {
            log::warn!(
                "blockchain interface calls have no deadline (timeout_seconds = 0): a read \
                 that is never answered will hold that client's chain refresh forever"
            );
            Ok(backend)
        }
    }
}

/// The interface, paced, and whether this service does the pacing: then a
/// broadcast's deadline, `timeout`, is put on inside it, so it starts once
/// the broadcast's slot is granted (CS-718). `woc` paces inside chain-gang's
/// own call, where nothing here can start a deadline after the wait.
fn paced_backend(config: &Config, timeout: Option<Duration>) -> Result<(Backend, bool), String> {
    let limit = config.blockchain_interface.outbound_rate_limit();
    let interface_type = config.blockchain_interface.interface_type.as_str();
    let backend = build_backend(config, limit)?;

    let Some(rps) = limit else {
        return Ok((backend, false));
    };
    if paces_its_own_requests(interface_type) {
        log::info!("limiting {interface_type} to {rps} HTTP request(s) per second");
        return Ok((backend, false));
    }
    let inner = match timeout {
        Some(timeout) => Arc::new(Bounded::covering(
            backend.interface,
            timeout,
            Scope::Broadcasts,
        )) as Arc<dyn BlockchainInterface + Send + Sync>,
        None => backend.interface,
    };

    // Wrapped last, so every outbound call goes through it whatever the
    // interface -- and so the inner interface is fully configured first.
    // The address watcher keeps talking to the node directly: importing an
    // address happens once per client at startup and is not what runs into a
    // rate limit.
    log::info!("limiting {interface_type} to {rps} interface call(s) per second");
    Ok((
        Backend {
            interface: Arc::new(RateLimited::new(inner, rps)),
            address_watcher: backend.address_watcher,
        },
        true,
    ))
}

fn build_backend(config: &Config, limit: Option<u32>) -> Result<Backend, String> {
    let network = config
        .get_network()
        .map_err(|_| "Unable to decode network from config".to_string())?;

    match config.blockchain_interface.interface_type.as_str() {
        "woc" => {
            let mut interface = WocInterface::new();
            interface.set_network(&network);
            // Applied here rather than by a decorator, because only the
            // interface sees the requests a single call turns into: a balance
            // is two, and a UTXO read one per 1000 entries (CS-457).
            if let Some(rps) = limit {
                interface.set_max_requests_per_second(rps);
            }
            Ok(Backend {
                interface: Arc::new(interface),
                address_watcher: None,
            })
        }
        "test" => {
            let mut interface = TestInterface::new();
            interface.set_network(&network);
            Ok(Backend {
                interface: Arc::new(interface),
                address_watcher: None,
            })
        }
        "uaas" => {
            let uaas_url = config
                .blockchain_interface
                .url
                .as_ref()
                .ok_or_else(|| "Config blockchain interface url not found.".to_string())?;
            let mut interface = UaaSInterface::new(uaas_url)
                .map_err(|e| format!("Unable to create UaaS interface: {e}"))?;
            interface.set_network(&network);
            Ok(Backend {
                interface: Arc::new(interface),
                address_watcher: None,
            })
        }
        "mapi_lite" => {
            // Chain reads through mapi-lite's address queries (CS-601), so
            // mapi-lite holds the deployment's only WhatsOnChain client.
            // `Config::check_chain_reads` has made sure [mapi_lite] is there.
            let mapi_lite = config.mapi_lite.as_ref().ok_or_else(|| {
                "interface_type = \"mapi_lite\" needs a [mapi_lite] section".to_string()
            })?;
            let mut interface =
                MapiLiteInterface::new(mapi_lite, config.blockchain_interface.call_timeout())?;
            interface.set_network(&network);
            Ok(Backend {
                interface: Arc::new(interface),
                address_watcher: None,
            })
        }
        "rpc" => {
            // A node we talk to directly. The only interface that reaches
            // regtest, since no public explorer indexes a private chain.
            let address = required(&config.blockchain_interface.url, "url")?;
            let user = required(&config.blockchain_interface.rpc_user, "rpc_user")?;
            let password = required(&config.blockchain_interface.rpc_password, "rpc_password")?;
            let interface = Arc::new(RpcInterface::new(address, user, password, network));
            // A node reports zero for an address its wallet does not track, so
            // it has to be told which ones to follow -- unless the operator
            // manages the wallet themselves.
            let address_watcher: Option<Arc<dyn AddressWatcher>> =
                if config.blockchain_interface.rpc_import_addresses {
                    Some(interface.clone())
                } else {
                    None
                };
            Ok(Backend {
                interface,
                address_watcher,
            })
        }
        other => Err(format!("Unknown interface type '{other}'")),
    }
}

/// Read a required `blockchain_interface` field, naming it if absent so the
/// operator is told which one to add rather than left guessing.
fn required<'a>(value: &'a Option<String>, field: &str) -> Result<&'a str, String> {
    value
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            format!("Config blockchain_interface.{field} is required for interface_type = \"rpc\".")
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BlockchainInterfaceConfig;
    use chain_gang::network::Network;

    // --- CS-457: which limiter applies ---

    /// `woc` paces its own requests from chain-gang 0.11.4, so wrapping it as
    /// well would pace twice and bound nothing extra. Everything else talks to
    /// a server the operator runs and keeps the call-level decorator, which is
    /// all that is available for it.
    #[test]
    fn cs_457_only_woc_paces_its_own_requests() {
        assert!(paces_its_own_requests("woc"));
        for other in ["uaas", "rpc", "test"] {
            assert!(
                !paces_its_own_requests(other),
                "{other} has no request-level limit of its own"
            );
        }
    }

    /// The limit still reaches `woc` when nothing is configured, because that
    /// is the interface with a published limit to stay inside.
    #[test]
    fn cs_457_woc_is_limited_by_default_and_the_backend_still_builds() {
        let config = base_config("woc", None);
        assert_eq!(
            config.blockchain_interface.outbound_rate_limit(),
            Some(crate::config::WOC_REQUESTS_PER_SECOND)
        );
        // The rate is handed to WocInterface, which keeps it private, so this
        // can only check that the configured path builds. What the limit
        // actually does is chain-gang's own tests.
        assert!(blockchain_factory(&config).is_ok());
    }

    /// An explicit zero means unlimited, and must not be passed down as a
    /// limit of zero -- which chain-gang would read as one request per second.
    #[test]
    fn cs_457_an_explicit_zero_leaves_the_interface_unlimited() {
        let mut config = base_config("woc", None);
        config.blockchain_interface.max_requests_per_second = Some(0);
        assert_eq!(config.blockchain_interface.outbound_rate_limit(), None);
        assert!(blockchain_factory(&config).is_ok());
    }

    fn base_config(interface_type: &str, url: Option<&str>) -> Config {
        Config {
            blockchain_interface: BlockchainInterfaceConfig {
                interface_type: interface_type.to_string(),
                network: "testnet".to_string(),
                url: url.map(str::to_string),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// CS-718: where this service paces, a broadcast's deadline starts once
    /// its slot is granted. Queued behind reads past `timeout_seconds`, it
    /// is still sent, rather than timing out unsent and holding its inputs
    /// as a possibly-sent broadcast. A read keeps its deadline over the wait.
    #[tokio::test]
    async fn cs_718_a_broadcast_is_bounded_from_its_slot_and_a_read_from_its_call() {
        let mut config = base_config("test", None);
        config.blockchain_interface.max_requests_per_second = Some(1);
        config.blockchain_interface.timeout_seconds = Some(1);
        let interface = blockchain_factory(&config).expect("builds").interface;

        // Three reads book the next three slots, a second apart; the
        // broadcast's is the fourth, about 3s out.
        let booked: Vec<_> = (0..3)
            .map(|_| {
                let interface = interface.clone();
                tokio::spawn(async move { interface.status().await })
            })
            .collect();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let queued = std::time::Instant::now();
        interface
            .broadcast_tx(&crate::test_support::sample_tx())
            .await
            .expect("sent once its slot came, past the 1s deadline");
        assert!(
            queued.elapsed() >= std::time::Duration::from_secs(2),
            "it did wait for its slot: {:?}",
            queued.elapsed()
        );

        // A read queued 2s out ran out of time, as before. (The one 1s out
        // races its own deadline, so it is not asserted either way.)
        let mut reads = Vec::new();
        for read in booked {
            reads.push(read.await.unwrap());
        }
        let timed_out = reads
            .iter()
            .filter(|read| matches!(read, Err(e) if crate::interface_timeout::is_timeout(e)))
            .count();
        assert!(timed_out >= 1, "{reads:?}");
        assert!(reads.iter().any(Result::is_ok), "{reads:?}");
    }

    /// CS-601: `mapi_lite` builds from the [mapi_lite] section, watches no
    /// addresses, and is not paced here: mapi-lite paces its own WhatsOnChain.
    #[test]
    fn sr_bchn_019_mapi_lite_reads_build_from_the_mapi_lite_section() {
        let mut config = base_config("mapi_lite", None);
        config.mapi_lite = Some(crate::config::MapiLiteConfig::for_base_url(
            "http://mapi:8080",
        ));
        let backend = blockchain_factory(&config).expect("builds");
        assert!(backend.address_watcher.is_none());
        assert_eq!(config.blockchain_interface.outbound_rate_limit(), None);

        let without = base_config("mapi_lite", None);
        assert!(blockchain_factory(&without).is_err());
    }

    fn rpc_config(
        url: Option<&str>,
        user: Option<&str>,
        password: Option<&str>,
        import: bool,
    ) -> Config {
        Config {
            blockchain_interface: BlockchainInterfaceConfig {
                interface_type: "rpc".to_string(),
                network: "regtest".to_string(),
                url: url.map(str::to_string),
                rpc_user: user.map(str::to_string),
                rpc_password: password.map(str::to_string),
                rpc_import_addresses: import,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn sr_bchn_001_blockchain_factory_supports_woc_test_uaas_and_rpc() {
        assert!(blockchain_factory(&base_config("woc", None)).is_ok());
        assert!(blockchain_factory(&base_config("test", None)).is_ok());
        assert!(blockchain_factory(&base_config("uaas", Some("http://localhost:5010"))).is_ok());
        assert!(blockchain_factory(&rpc_config(
            Some("127.0.0.1:18443"),
            Some("u"),
            Some("p"),
            true
        ))
        .is_ok());
    }

    /// Only the node needs telling which addresses to follow, so only it
    /// supplies a watcher. A stray watcher on another backend would mean
    /// pointless RPC calls.
    #[test]
    fn sr_bchn_007_only_the_rpc_backend_supplies_an_address_watcher() {
        for interface_type in ["woc", "test"] {
            let backend = blockchain_factory(&base_config(interface_type, None)).unwrap();
            assert!(
                backend.address_watcher.is_none(),
                "{interface_type} should not supply a watcher"
            );
        }
        let backend =
            blockchain_factory(&base_config("uaas", Some("http://localhost:5010"))).unwrap();
        assert!(backend.address_watcher.is_none());

        let backend = blockchain_factory(&rpc_config(
            Some("127.0.0.1:18443"),
            Some("u"),
            Some("p"),
            true,
        ))
        .unwrap();
        assert!(backend.address_watcher.is_some());
    }

    /// An operator who manages the node's wallet can turn the import off.
    #[test]
    fn sr_bchn_007_rpc_import_addresses_false_supplies_no_watcher() {
        let backend = blockchain_factory(&rpc_config(
            Some("127.0.0.1:18443"),
            Some("u"),
            Some("p"),
            false,
        ))
        .unwrap();
        assert!(backend.address_watcher.is_none());
    }

    #[test]
    fn sr_bchn_005_rpc_interface_requires_address_and_credentials() {
        for (url, user, password, expected) in [
            (None, Some("u"), Some("p"), "url"),
            (Some("127.0.0.1:18443"), None, Some("p"), "rpc_user"),
            (Some("127.0.0.1:18443"), Some("u"), None, "rpc_password"),
        ] {
            let error = blockchain_factory(&rpc_config(url, user, password, true))
                .err()
                .expect("expected a missing-field error");
            assert!(error.contains(expected), "should name {expected}: {error}");
        }
    }

    #[test]
    fn sr_bchn_006_rpc_backend_can_be_built_for_regtest() {
        let config = rpc_config(Some("127.0.0.1:18443"), Some("u"), Some("p"), true);
        assert_eq!(config.get_network().unwrap(), Network::BSV_Regtest);
        assert!(blockchain_factory(&config).is_ok());
    }

    #[test]
    fn sr_bchn_001_blockchain_factory_rejects_unknown_interface_type() {
        let result = blockchain_factory(&base_config("unknown", None));
        assert!(result.is_err());
        assert!(result.err().unwrap().contains("Unknown interface type"));
    }
}
