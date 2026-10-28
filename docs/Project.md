# Project status

High-level status and known limitations for the Financing Service Rust implementation (v5.1.0).

For API details see [SupportedEndpoints.md](SupportedEndpoints.md). For configuration see [Configuration.md](Configuration.md). For build and test instructions see [Development.md](Development.md).

## History

| Milestone | Date |
|-----------|------|
| Initial design document | September 2022 |
| Python implementation | October 2022 |
| Rust implementation | October 2022 – October 2023 |
| Overlay network updates (client names removed from status) | October 2024 |
| REST API with JSON body for `/fund` | 2024 |
| Per-client API key authentication | 2026 |
| Secret management (`env:VAR`, overrides, dynamic client env refs) | 2026 |
| Per-IP HTTP rate limiting | 2026 |
| Live balance refresh and pre-fund UTXO resync | 2026 |
| Multi-tx partial failure structured responses | 2026 |
| Same-client concurrent funding (plan-then-commit) | 2026 |
| Optional mapi-lite transaction broadcaster | September 2026 |

The Rust service uses the [`chain-gang`](https://github.com/nchain-innovation/chain-gang) library for blockchain access (WhatsOnChain, UaaS, a node's JSON-RPC endpoint, or the test interface) and wallet operations. Optionally it broadcasts through [mapi-lite](https://github.com/nchain-innovation/mapi-lite) via that project's `uls-client` crate.

## Current architecture

```
Client App  ──REST──▶  Actix Web API  ──▶  Service  ──reads───▶  BlockchainInterface (woc/uaas/rpc/test)
Admin       ──REST──▶       │                │
                            │                ├──writes──▶  TxBroadcaster
                            │                │              ├── WocBroadcaster  (wraps the BlockchainInterface; default)
                            │                │              └── MapiBroadcaster (uls-client ──▶ mapi-lite; when [mapi_lite] is set)
                            │                ├── Per-client wallets (Arc<RwLock<Client>>)
                            │                ├── dynamic.toml (runtime clients)
                            │                └── dynamic.inflight.json (in-flight funding state, survives restarts)
                            ├── rate_limit (per-IP, /health and /ready exempt)
                            ├── /health (liveness, no auth; no upstream checks)
                            └── /ready  (readiness, no auth; chain-read age, and mapi-lite's /ready when configured)
```

| Module | Role |
|--------|------|
| `main.rs` | Config load, HTTP server, periodic UTXO refresh |
| `rest_api.rs` | REST handlers, auth gates |
| `service.rs` | Orchestration, funding flow, client management |
| `client.rs` | UTXO selection, transaction construction, plan/commit |
| `auth.rs` | Per-client and admin API key verification |
| `secrets.rs` | `env:VAR` resolution and plaintext warnings |
| `responses.rs` | Typed JSON request/response DTOs |
| `config.rs` | TOML and environment config loading |
| `blockchain_factory.rs` | Pluggable blockchain backends (chain reads, and broadcast by default) |
| `broadcaster/mod.rs` | `TxBroadcaster` seam for sending funding transactions |
| `broadcaster/woc.rs` | Broadcast through the configured `BlockchainInterface` (WoC in production) |
| `broadcaster/mapi.rs` | Broadcast through mapi-lite with `uls-client`; health probe against mapi-lite's `GET /ready`; fee quote |
| `broadcaster/factory.rs` | Select the broadcaster from `[mapi_lite]`; the startup log line |
| `address_watcher.rs` | Tell a node-backed interface which addresses to track |
| `dynamic_config.rs` | Persist runtime-added clients |
| `idempotency.rs` | `POST /fund` idempotency records: reserve a key, replay the retained response |
| `rate_limit.rs` | Per-IP HTTP rate limiting middleware |
| `outbound_rate_limit.rs` | Keep outbound calls inside the blockchain interface's rate limit |
| `telemetry.rs` | Tracing subscriber and OpenTelemetry OTLP export |

## Implemented

* REST API for funding, balance, address, client management, status, and health
* JSON request/response bodies with typed serde DTOs
* Dynamic client add/remove via `POST /client` and `DELETE /client/{id}`
* Optional per-client `api_key` authentication
* Optional `admin_api_key` for `POST /client`
* Secret references via `env:VAR`, environment overrides, and `wif_env` / `api_key_env` on `POST /client`
* Optional OpenTelemetry trace and metrics export via OTLP (configurable, disabled by default; metrics listed in [Configuration.md](Configuration.md#metrics))
* Configurable per-IP HTTP rate limiting with `/health` and `/ready` exempt
* Balance checks against total wallet balance; funding combines multiple UTXOs when needed; balance endpoint refreshes from chain when the cached state is older than `service.chain_state_max_age_seconds`; `multiple_tx` partial failures return structured successful transaction data; concurrent fund requests for the same client each claim their inputs under an exclusive per-client lock and commit the claim only after a successful broadcast
* Optional mapi-lite transaction broadcaster (`[mapi_lite]`): broadcasts go to mapi-lite, reads stay on the blockchain interface, `/ready` probes mapi-lite's own `/ready` and returns 503 when it is down or not ready while `/health` stays up, `/status` names the broadcaster, startup logs the selection
* Docker image with `/health` liveness check
* CI: build, test, `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo audit`
* `chain-gang` from crates.io at an exact release, with a committed `Cargo.lock`, for reproducible builds
* Automated test suite (unit, integration, REST API, system requirements), run by `cargo test`

## Known limitations

* **Multi-tx partial failure** — in `multiple_tx` mode, earlier successful broadcasts remain on-chain if a later step fails. The service resyncs UTXO state and returns HTTP 422 with code `partial_broadcast` and `description`, plus `outpoints` and `txs` for any transactions that were broadcast successfully.
* **No HTTPS** — expected to be handled at the deployment layer (reverse proxy, firewall).
* **Rate limiting behind a reverse proxy** — limits apply to the proxy's IP unless the proxy forwards the original client address and a custom key extractor is added.
* **Same-client concurrent funding** — concurrent requests for one client never spend the same input: each claims its inputs while planning. A request that finds every UTXO already claimed by requests still broadcasting is refused with `funds_in_flight` (HTTP 503, `Retry-After: 1`) until one completes, so a client funded from a single UTXO funds one request at a time; `insufficient_balance` and `no_suitable_utxo` are kept for a wallet that is short however the claims settle. Different clients are not blocked by each other.

## Open items

None at present.

## Related documentation

* [SupportedEndpoints.md](SupportedEndpoints.md) — REST API reference
* [Configuration.md](Configuration.md) — service and client configuration
* [LockingScripts.md](LockingScripts.md) — generating locking scripts for `/fund`
* [Development.md](Development.md) — build, test, and CI
* [Dependencies.md](Dependencies.md) — the private `uls-client` dependency and the single-`chain-gang` invariant
* [SystemRequirements.md](SystemRequirements.md) — system requirements and verification methods
* [README.md](../README.md) — overview and getting started
