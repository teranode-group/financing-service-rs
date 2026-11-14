//! Keeping the service inside the blockchain interface's rate limit.
//!
//! WhatsOnChain publishes "up to 3 requests/sec is free" and answers 429 above
//! it. Nothing in the service spaced its calls, and it makes an interface call
//! per client per refresh -- the UTXOs, from which the balance is derived;
//! there used to be a second, for the balance -- from a periodic timer, from
//! `GET /balance`, and from `POST /fund` both before building and again on the
//! error path. Several clients, or one client and a little traffic, and the
//! bursts overlap. The reported symptom was a run of 429s within the same
//! second, and the real risk is not the failed refresh: it is being banned for
//! sustained violation, which no amount of retrying recovers from.
//!
//! So the limit is honoured here rather than at each call site. A decorator
//! around [`BlockchainInterface`] sees every outbound *call* whatever asked for
//! it, and no future caller has to remember.
//!
//! # What this paces, and who uses it now (CS-457)
//!
//! **One slot per interface call, not per HTTP request.** Those were the same
//! thing when this was written against chain-gang 0.11.2. They stopped being
//! the same in 0.11.3 (CS-456): `get_balance` is two requests, and `get_utxo`
//! one per 1000 UTXOs -- 21 for the mainnet address CS-456 measured. This
//! decorator reserves one slot and chain-gang then issues all of them inside
//! it, so for `woc` it stopped bounding the request rate at all.
//!
//! That could not be fixed from here: `WocInterface` built its own client and
//! called `reqwest::get` directly, with no hook to supply. chain-gang 0.11.4
//! added `set_max_requests_per_second`, which paces the requests themselves,
//! and [`crate::blockchain_factory`] now hands the configured rate to the
//! interface for `woc` and does **not** wrap it here -- pacing in both places
//! would pace twice and bound nothing extra.
//!
//! So this decorator is now for the interfaces that cannot pace themselves: a
//! node reached over RPC, or a UaaS. Those are the operator's own servers and
//! are unthrottled by default, so it applies only when
//! `max_requests_per_second` is set explicitly. For them the caveat above
//! still stands -- one slot per call, and a call may be more than one
//! request -- but the published limit that prompted CS-418 is not theirs to
//! break.
//!
//! # How far ahead a slot may be (CS-736)
//!
//! A slot is booked when the call starts, and a caller that gives up while
//! it waits -- a read cut off by its deadline -- keeps its booking. So if
//! calls are booked faster than the limit, the next free slot runs ahead of
//! real time, and with no bound on that, a broadcast behind them waited
//! however long it came to: the deadline on a broadcast starts only once its
//! slot is granted (CS-718), and nothing at the request level timed out the
//! wait.
//!
//! With [`RateLimited::with_max_wait`], a call whose slot is further away
//! than that fails at once, books nothing, and is never made: an error
//! [`never_sent`] recognises, which a broadcast reports as not sent rather
//! than as a possibly-sent unknown outcome. Since nothing is booked past the
//! limit, the next free slot is never more than the limit ahead either.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chain_gang::interface::{Balance, BlockchainInterface, Utxo};
use chain_gang::messages::{BlockHeader, Tx};
use chain_gang::network::Network;
use chain_gang::util::ChainGangError;
use tokio::sync::Mutex;

/// A call was not made because its slot was further away than the limit
/// allows (CS-736). Carried inside a [`ChainGangError::IoError`], since
/// chain-gang has no variant of its own for it; [`never_sent`] finds it.
#[derive(Debug)]
pub struct SlotTooFar {
    /// How long the call would have waited for its slot.
    pub wait: Duration,
    /// The most it was allowed to wait.
    pub limit: Duration,
}

impl std::fmt::Display for SlotTooFar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the next request slot is {:.1}s away, more than the {}s \
             (blockchain_interface.timeout_seconds) a call may wait for one",
            self.wait.as_secs_f64(),
            self.limit.as_secs()
        )
    }
}

impl std::error::Error for SlotTooFar {}

/// Whether `error` is a call that was never made, because its slot was too
/// far away ([`SlotTooFar`]). For a broadcast that means nothing reached the
/// network, so its inputs are spendable again (CS-736).
pub fn never_sent(error: &ChainGangError) -> bool {
    matches!(
        error,
        ChainGangError::IoError(e) if e.get_ref().is_some_and(|inner| inner.is::<SlotTooFar>())
    )
}

/// Wraps a blockchain interface and spaces its outbound requests.
pub struct RateLimited {
    inner: Arc<dyn BlockchainInterface + Send + Sync>,
    /// Minimum gap between the starts of two requests.
    min_interval: Duration,
    /// The earliest a request may start. Each caller reserves a slot and moves
    /// this on, so concurrent callers queue rather than collide.
    next_slot: Mutex<Instant>,
    /// The longest a call may wait for its slot; `None` waits however long
    /// (CS-736).
    max_wait: Option<Duration>,
}

impl RateLimited {
    /// Wrap `inner` at `requests_per_second`.
    pub fn new(
        inner: Arc<dyn BlockchainInterface + Send + Sync>,
        requests_per_second: u32,
    ) -> Self {
        let rps = requests_per_second.max(1);
        Self {
            inner,
            min_interval: Duration::from_secs(1) / rps,
            next_slot: Mutex::new(Instant::now()),
            max_wait: None,
        }
    }

    /// Refuse a call whose slot is more than `max_wait` away, rather than
    /// wait for it (CS-736). `None` waits however long.
    pub fn with_max_wait(mut self, max_wait: Option<Duration>) -> Self {
        self.max_wait = max_wait;
        self
    }

    /// Wait until this request's turn.
    ///
    /// The slot is reserved under the lock, and the lock released before the
    /// sleep. That is not a throughput trick -- holding it across the sleep
    /// finishes every caller at the same moment, since each wakes exactly at
    /// the slot it reserved -- it is to avoid holding a lock across an await
    /// at all, so nothing that later takes this lock for a cheap read can be
    /// parked behind somebody else's wait.
    ///
    /// A slot further away than `max_wait` is not booked, and the call is
    /// refused with [`SlotTooFar`] (CS-736).
    async fn acquire(&self) -> Result<(), ChainGangError> {
        let wait = {
            let mut next_slot = self.next_slot.lock().await;
            let now = Instant::now();
            let start = (*next_slot).max(now);
            let wait = start.saturating_duration_since(now);
            if let Some(limit) = self.max_wait {
                if wait > limit {
                    return Err(ChainGangError::IoError(std::io::Error::other(SlotTooFar {
                        wait,
                        limit,
                    })));
                }
            }
            *next_slot = start + self.min_interval;
            wait
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        Ok(())
    }
}

#[async_trait]
impl BlockchainInterface for RateLimited {
    fn set_network(&mut self, _network: &Network) {
        // The wrapped interface is configured before it is wrapped. Taking
        // `&mut self` here cannot reach through an `Arc` anyway, and silently
        // doing nothing is better than pretending: the factory sets the
        // network on the inner interface first.
    }

    async fn status(&self) -> Result<(), ChainGangError> {
        self.acquire().await?;
        self.inner.status().await
    }

    async fn get_balance(&self, address: &str) -> Result<Balance, ChainGangError> {
        self.acquire().await?;
        self.inner.get_balance(address).await
    }

    async fn get_utxo(&self, address: &str) -> Result<Utxo, ChainGangError> {
        self.acquire().await?;
        self.inner.get_utxo(address).await
    }

    async fn broadcast_tx(&self, tx: &Tx) -> Result<String, ChainGangError> {
        self.acquire().await?;
        self.inner.broadcast_tx(tx).await
    }

    async fn get_tx(&self, txid: &str) -> Result<Tx, ChainGangError> {
        self.acquire().await?;
        self.inner.get_tx(txid).await
    }

    async fn get_latest_block_header(&self) -> Result<BlockHeader, ChainGangError> {
        self.acquire().await?;
        self.inner.get_latest_block_header().await
    }

    async fn get_block_headers(&self) -> Result<String, ChainGangError> {
        self.acquire().await?;
        self.inner.get_block_headers().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{test_blockchain_interface, test_config, TEST_ADDRESS};
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Counts calls and answers instantly, so what a test measures is the
    /// limiter's spacing and nothing else.
    struct CountingInterface {
        inner: Arc<dyn BlockchainInterface + Send + Sync>,
        calls: AtomicU32,
    }

    #[async_trait]
    impl BlockchainInterface for CountingInterface {
        fn set_network(&mut self, _network: &Network) {}
        async fn status(&self) -> Result<(), ChainGangError> {
            Ok(())
        }
        async fn get_balance(&self, address: &str) -> Result<Balance, ChainGangError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.get_balance(address).await
        }
        async fn get_utxo(&self, address: &str) -> Result<Utxo, ChainGangError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.get_utxo(address).await
        }
        async fn broadcast_tx(&self, _tx: &Tx) -> Result<String, ChainGangError> {
            unimplemented!()
        }
        async fn get_tx(&self, _txid: &str) -> Result<Tx, ChainGangError> {
            unimplemented!()
        }
        async fn get_latest_block_header(&self) -> Result<BlockHeader, ChainGangError> {
            unimplemented!()
        }
        async fn get_block_headers(&self) -> Result<String, ChainGangError> {
            unimplemented!()
        }
    }

    async fn counting() -> Arc<CountingInterface> {
        let config = test_config("/tmp/financing-service-rate-limit-test.toml");
        Arc::new(CountingInterface {
            inner: test_blockchain_interface(&config).await,
            calls: AtomicU32::new(0),
        })
    }

    /// The point of the whole module: a burst of calls is spread out instead
    /// of leaving all at once. Six requests at 3/s cannot finish before the
    /// gaps between them have been waited out.
    #[tokio::test]
    async fn cs_418_a_burst_is_spaced_out_rather_than_sent_at_once() {
        let counter = counting().await;
        let limited = RateLimited::new(counter.clone(), 3);

        let started = Instant::now();
        for _ in 0..6 {
            limited.get_balance(TEST_ADDRESS).await.expect("balance");
        }
        let elapsed = started.elapsed();

        assert_eq!(counter.calls.load(Ordering::SeqCst), 6);
        // Five gaps of a third of a second between six calls.
        assert!(
            elapsed >= Duration::from_millis(1600),
            "six calls at 3/s took {elapsed:?}, which is faster than the limit allows"
        );
    }

    /// Concurrent callers have to share the same allowance -- a limiter that
    /// only spaced one caller's calls would be no limiter at all, since the
    /// service refreshes several clients at once.
    #[tokio::test]
    async fn cs_418_concurrent_callers_share_the_allowance() {
        let counter = counting().await;
        let limited = Arc::new(RateLimited::new(counter.clone(), 3));

        let started = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..6 {
            let limited = Arc::clone(&limited);
            handles.push(tokio::spawn(async move {
                limited.get_utxo(TEST_ADDRESS).await.expect("utxo")
            }));
        }
        for handle in handles {
            handle.await.expect("joined");
        }
        let elapsed = started.elapsed();

        assert_eq!(counter.calls.load(Ordering::SeqCst), 6);
        assert!(
            elapsed >= Duration::from_millis(1600),
            "six concurrent calls at 3/s took {elapsed:?}: they did not share the allowance"
        );
    }

    /// CS-736: a call whose slot is further away than the limit is refused
    /// at once, never made, and books nothing: the next call waits only for
    /// the slot it would have had anyway.
    #[tokio::test]
    async fn cs_736_a_slot_too_far_away_is_refused_unsent_and_books_nothing() {
        let counter = counting().await;
        let limited =
            RateLimited::new(counter.clone(), 1).with_max_wait(Some(Duration::from_millis(400)));

        limited
            .get_utxo(TEST_ADDRESS)
            .await
            .expect("the first slot is now");
        // The next slot is a second away, past the 400ms limit.
        let started = Instant::now();
        let refused = limited.get_utxo(TEST_ADDRESS).await.unwrap_err();
        assert!(never_sent(&refused), "{refused}");
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "it waited {:?} instead of failing at once",
            started.elapsed()
        );
        assert_eq!(counter.calls.load(Ordering::SeqCst), 1, "nor was it made");
        assert!(refused.to_string().contains("timeout_seconds"), "{refused}");

        // 700ms on, the next slot is 300ms away, within reach: the refused
        // call did not move it on.
        tokio::time::sleep(Duration::from_millis(700)).await;
        limited.get_utxo(TEST_ADDRESS).await.expect("within reach");
        assert_eq!(counter.calls.load(Ordering::SeqCst), 2);
    }

    /// Only that refusal is "never sent": a call that timed out may have been
    /// carried out, and is not.
    #[test]
    fn cs_736_only_a_refused_slot_is_never_sent() {
        let timed_out =
            ChainGangError::IoError(std::io::Error::new(std::io::ErrorKind::TimedOut, "late"));
        assert!(!never_sent(&timed_out));
        assert!(!never_sent(&ChainGangError::IoError(
            std::io::Error::other("something else")
        )));
        assert!(!never_sent(&ChainGangError::ResponseError(
            "response.status() = 400 Bad Request".to_string()
        )));
    }
}
