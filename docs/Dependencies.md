# Dependencies

Most of this crate's dependencies come from crates.io and need nothing said about them. Two do not, and they bring one invariant with them. This page covers those.

## `uls-client` and `uls-core` — the mapi-lite crates

| Crate | What it is | Source | Pin |
|---|---|---|---|
| `uls-client` | mapi-lite's typed HTTP client: `MapiClient::submit_transactions`, `fee_quote`, … | `ssh://git@github.com/nchain-innovation/mapi-lite.git` (**private**) | commit `e1a03cad02e7531764d0ec76f2390a56c2caccf5` |
| `uls-core` | The wire types and signed-envelope code both the server and client share | same repo, **same commit** | same |

They exist for the optional mapi-lite transaction broadcaster (`src/broadcaster/mapi.rs`, configured by `[mapi_lite]`). They are compiled in whether or not that section is configured; a build without them is not offered, because a second build variant is more maintenance than the SSH requirement costs.

### Why a commit, not a tag or a version

mapi-lite is not published to crates.io. The pin names the commit tagged `v0.7.0` (CS-640, CS-641), on chain-gang 0.13 as before. It is a minor because `uls-core`'s `MempoolCheckConfig` and `Metrics` each gain a field, which this service does not build. What it adds is server-side: mapi-lite counts accepted transactions left unmined and reports each one stuck past `stuck_after_blocks`, and `GET /mapi/tx` describes one as "Accepted but not mined …" while still answering `success`. `uls-client`'s API is unchanged. The move before, to `v0.6.1` (CS-637), was taken for `uls-client`'s fix with no API change: after a submit attempt that may have reached a node, a `4xx` on a retry is `ClientError::OutcomeUnknown`, not `Status`, so `may_have_been_delivered` says it may have been, and a rotated token or a gateway's `429` no longer reads as nothing sent. The move before, to `v0.6.0` (CS-624), was taken for `ReadyReport::chain`, the network mapi-lite's nodes are on, which this service checks its own `blockchain_interface.network` against; `uls-client`'s API is otherwise unchanged. The move before, to `v0.5.0` (CS-627), was taken for `uls-client`'s CS-600 work, which replaced this service's own copies: `MapiClient::broadcast` (one transaction's verdict, with the retries, the submit deadline and the resubmission of an acceptance mapi-lite could not record), the fee helpers on `FeeQuotePayload` and `FeeAmount`, `ready` with `ReadyReport`, and `submit_budget_warning`. `uls-core`'s `AppSettings` and `NodeConfig` gained fields, which this service does not build. The move before, to `v0.4.0`, was taken for `ClientError::may_have_been_delivered` (CS-501) and `MapiClient::with_trusted_miner_ids` (CS-523). It names the commit rather than the tag because a tag can be moved and a commit cannot; `Cargo.lock` would record the commit either way, but a `rev` pin also keeps `Cargo.toml` honest about it, and `sr_bchn_009_uls_client_and_uls_core_are_pinned_to_one_mapi_lite_revision` relies on that form.

`teranode-event-rs`, the other consumer, takes mapi-lite by tag, on the same chain-gang; it takes `v0.7.0` too (CS-625, CS-637, CS-641). The parent `uls-rs` repo's `scripts/preflight.sh` warns when a pin falls behind the mapi-lite checkout it is about to run.

### Why the `ssh://` URL form

Cargo accepts `git = "ssh://git@github.com/org/repo.git"` and not the `git@github.com:org/repo.git` shorthand. `.cargo/config.toml` sets `net.git-fetch-with-cli = true`, so cargo delegates the fetch to the system `git`, which authenticates through your ssh-agent and `~/.ssh/config` exactly as `git clone` would. Cargo's built-in fetcher has weaker SSH support, and this avoids it.

### What you need to build

* **Locally:** an SSH key with read access to `nchain-innovation/mapi-lite` loaded in your agent. `ssh -T git@github.com` should greet you.
* **CI (`.github/workflows/rust.yml`):** a read-only deploy key on the mapi-lite repository, stored in this repository as the `MAPI_LITE_DEPLOY_KEY` secret. The workflow fails fast with a named error if the secret is missing, then loads it with `webfactory/ssh-agent` before the first cargo command.
* **Docker (`build.sh`, `multi-build.sh`):** BuildKit with `--ssh default`, which lends your agent socket to the one `RUN --mount=type=ssh` step that fetches dependencies. The key never enters the image; the builder stage installs `git` and `openssh-client` and pre-trusts `github.com` in `known_hosts`.

## The single-`chain-gang` invariant

`chain-gang` is referenced twice: directly by this crate, and transitively by `uls-core`. Cargo unifies two references into one package only when they name the **same source** and **compatible versions**. If either differs, the graph carries two `chain-gang` packages, and `Hash256` from one is not `Hash256` from the other. `uls-core` hands chain-gang types across the boundary (`ParsedTx.tx`, `compute_merkle_root(&[Hash256])`), so the failure is real: a compile error naming what looks like the same type twice, some distance from the cause.

The two ways it can split, both of which have happened to `teranode-event-rs`:

1. **By source.** A git pin and a crates.io release never unify, even at the same version number. All three repos therefore take chain-gang from **crates.io**.
2. **By version.** `chain-gang` is pre-1.0, so `0.10` and `0.11` are incompatible ranges. If this crate asks for `0.12` while the pinned mapi-lite commit still asks for `0.11`, Cargo resolves both. No `[patch]` can fix this: a patch has to satisfy the dependent's own requirement.

The same applies to `reqwest`, because `uls-client`'s error type wraps `reqwest::Error` and this crate hands `uls-client` a `reqwest::Client`. This crate's `reqwest` requirement therefore matches mapi-lite's major.minor and names no TLS backend of its own; the backend is whatever `uls-client` enables.

`src/dependency_graph.rs` asserts on `Cargo.lock` that `chain-gang` and `reqwest` each resolve to exactly one package and that `uls-client` and `uls-core` share one source. A bump that breaks the invariant fails there, with an explanation, rather than downstream.

### How to bump

**chain-gang, within the range mapi-lite already asks for** (it asks for `0.13.0`, so anything `0.13.x`): raise the version in `Cargo.toml`, `cargo update -p chain-gang`, `cargo test`, commit `Cargo.lock`. Nothing about mapi-lite changes, because a patch release satisfies the requirement the pinned commit already states and Cargo unifies the two references on one package. `0.11.2` to `0.11.3` was done this way.

**chain-gang, out of that range** (`0.14` and up): move mapi-lite first. Once a mapi-lite commit on the new chain-gang exists, in one change here: repin `uls-client` and `uls-core` to that commit, raise `chain-gang` in `Cargo.toml`, run `cargo update -p chain-gang -p uls-client -p uls-core`, run `cargo test`, commit `Cargo.lock`. `0.11.6` to `0.13.0` was done this way.

**mapi-lite alone** (no chain-gang change): edit both `rev`s to the same new commit, `cargo update -p uls-client -p uls-core`, `cargo test`, commit `Cargo.lock`. Check that `teranode-event-rs` is on the same commit, or the two services will speak subtly different wire contracts to the one mapi-lite they share.

## Everything else

`chain-gang` (crates.io, exact release in `Cargo.lock`) provides the BSV primitives, wallet and the `BlockchainInterface` implementations for WhatsOnChain, UaaS and node RPC. `Cargo.lock` is committed; run `cargo update` only when upgrading on purpose.
