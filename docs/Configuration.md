# Financing Service - Configuration

Configuration for this service is a TOML file, read when the service starts: the file `FS_CONFIG` names, or `data/financing-service.toml` when it is unset. `FS_CONFIG` names a file, as `TE_CONFIG` and `MAPI_CONFIG` do for Event and mapi-lite; it no longer holds JSON (CS-624).

Any setting can then be overridden from the environment with `FS__<TABLE>__<KEY>`, two underscores between each part: `FS__SERVICE__UTXO_REFRESH_PERIOD=30` sets `[service] utxo_refresh_period`, and `FS__LISTEN_ADDR=0.0.0.0:9090` sets the top-level `listen_addr`. See [Environment overrides](#environment-overrides).

**A key no setting reads stops startup**, naming it: a misspelling, a key written under the wrong `[table]`, an override with a typo, or a setting this version has removed. Before CS-624 such a key was silently ignored and the setting kept its default.

The file is composed of the following sections:

## [blockchain_interface]

Configures the blockchain interface. Supported `interface_type` values: `woc`, `mapi_lite`, `uaas`, `rpc`, `test`.

```toml
[blockchain_interface]
interface_type = "mapi_lite"             # reads through [mapi_lite]; see below
network = "testnet"
# url = "http://localhost:5010"  # required for uaas, and for rpc (host:port)
# rpc_user = "rpcuser"                    # required for rpc
# rpc_password = "env:FS_RPC_PASSWORD"    # required for rpc
# rpc_import_addresses = true             # rpc only; default true
# max_requests_per_second = 3             # default 3 for woc, unset otherwise
# timeout_seconds = 30                    # deadline on each interface call; 0 turns it off
```

`network` is one of `mainnet`, `testnet`, `regtest` or `stn`, spelt exactly so: they are the names mapi-lite publishes for its nodes' chain, and Event's. Any other spelling stops startup; one that means the same network (`main`, `livenet`, `test`, `Mainnet`, …) is refused naming the one to write. It was `network_type` until CS-624, and that key is now refused.

### Which interface reaches which network

The interface you choose constrains which networks you can reach, and this is usually the deciding factor for a local deployment.

| `interface_type` | Reaches | mainnet | testnet | stn | regtest |
|---|---|---|---|---|---|
| `woc` | WhatsOnChain, a public API | ✅ | ✅ | ✅ | ❌ |
| `mapi_lite` | the [`[mapi_lite]`](#mapi_lite) server's address reads, which it answers from WhatsOnChain | ✅ | ✅ | ❌ | ❌ |
| `uaas` | a UTXO as a Service instance (set `url`) | ✅ | ✅ | ✅ | ❌ |
| `rpc` | a node's JSON-RPC endpoint, directly | ✅ | ✅ | ✅ | ✅ |
| `test` | nothing — an in-process stub | — | — | — | — |

**`rpc` is the only interface that reaches regtest**, because no public explorer indexes a private chain. It also works against a node you control on any other network, which removes the dependency on a third-party API being up.

The interface serves both chain *reads* (balances, UTXOs) and, by default, the *broadcast* of funding transactions. The broadcast alone can be moved to a mapi-lite server with the optional [`[mapi_lite]`](#mapi_lite) section; with `interface_type = "mapi_lite"` the reads go there too.

### The `mapi_lite` interface

```toml
[blockchain_interface]
interface_type = "mapi_lite"
network = "testnet"

[mapi_lite]
base_url = "http://127.0.0.1:8080"
```

Reads each client's UTXO set and balance through the `[mapi_lite]` server's `/query/address/*` endpoints, with the same `base_url`, `auth_token` and `miner_id_public_keys` as broadcasts (CS-601, SR-BCHN-019). mapi-lite then makes every WhatsOnChain request for the deployment, paced by its one limit (`[woc] max_requests_per_second` there) against WhatsOnChain's one quota. With `woc` here as well, this service and mapi-lite each pace their own requests against that same quota and can exceed it between them.

**This is the interface for mainnet and testnet** (CS-635), and the sample config uses it. With `[mapi_lite]` and `interface_type = "woc"` on mainnet or testnet, startup warns, naming the shared quota and this interface as the fix (SR-BCHN-022). It is a warning, not a refusal: this service cannot tell whether mapi-lite's `[woc]` is on, and with it off, `woc` here is the only WhatsOnChain client. Neither service sends a WhatsOnChain API key, so both are on the free quota.

* Needs a `[mapi_lite]` section, and mapi-lite needs `[woc]` configured: without it mapi-lite answers address reads with `503`, which this service reports as `chain_unavailable`, never as an empty wallet.
* mainnet and testnet only, since mapi-lite answers these reads from WhatsOnChain. Startup refuses `regtest` and `stn`; use `rpc` for a private chain.
* `max_requests_per_second` is unset by default, as mapi-lite paces its own requests. `timeout_seconds` bounds each read as for any interface, and a read is one request to mapi-lite however many WhatsOnChain pages it takes there, so leave room for the largest address.
* Broadcasts go through `[mapi_lite]` as before. `/status`'s connectivity check and the startup probe use mapi-lite's signed fee quote, which costs mapi-lite no WhatsOnChain request.

### The `rpc` interface

```toml
[blockchain_interface]
interface_type = "rpc"
network = "regtest"
url = "127.0.0.1:18332"   # bitcoin-sv's regtest RPC port; Bitcoin Core's is 18443
rpc_user = "rpcuser"
rpc_password = "env:FS_RPC_PASSWORD"
```

* `url` — the node's JSON-RPC host and port. A bare `host:port` is treated as `http://`; give a full URL for `https`.
* `rpc_user` / `rpc_password` — the node's RPC credentials, sent as HTTP basic auth on every call. **Point this only at a node you control**, since the credentials go to whatever host is configured.

`rpc_password` and `rpc_user` take an `env:VAR_NAME` reference, or can be set with `FS__BLOCKCHAIN_INTERFACE__RPC_PASSWORD` and `FS__BLOCKCHAIN_INTERFACE__RPC_USER`. A plaintext `rpc_password` is reported at startup like any other plaintext secret.

#### Address imports

A node answers balance and UTXO queries from its own wallet, so an address it does not track reads as **zero, with no error**. The service therefore asks the node to watch each client's funding address — at startup for configured clients, and again whenever one is added through `POST /client`, so a new client works without a restart.

The import uses `importaddress` with the rescan disabled, because a freshly derived client key has no history to find and a rescan blocks the node's RPC connection while it runs.

```toml
rpc_import_addresses = false   # default: true
```

Turn it off if you manage the node's wallet yourself, or if it is a **descriptor wallet**, where `importaddress` is refused. With imports off, make sure each client's address is already tracked; `GET /client/{client_id}/address` gives you the address.

An import that fails is logged as a warning and does not stop the service — the node may be refusing for a reason you already know about. The warning names the address and says what follows from it: that balance reads zero and funding is refused until the node tracks the address. Worth watching for on first run, because a reachable node reporting an empty wallet otherwise looks like a bug at the caller.

#### Outbound rate limiting

The service reads chain state often: once per client per refresh — the UTXOs, from which the balance is derived — from the periodic timer, from `GET /balance`, and from `POST /fund` both before building a transaction and again on the error path. Several clients, or one client and a little traffic, and those bursts overlap.

**That is one *call*, which is not one request.** Since chain-gang 0.11.3 (CS-456) a UTXO read is one HTTP request per 1000 UTXOs — 21 for the busy mainnet address that change was measured against. See the limit's scope below.

WhatsOnChain documents **"up to 3 requests/sec is free"** and answers `429 Too Many Requests` above it. A failed refresh is the mild consequence; sustained violation risks a ban, which no amount of retrying recovers from. So outbound calls are spaced:

```toml
max_requests_per_second = 3   # woc default; unset for rpc and uaas
```

Left unset it follows the interface. `woc` talks to a public API and is limited to 3/s. `rpc` and `uaas` are the operator's own servers, so they are not limited at all.

Set it explicitly to override either way — raise it if you have a paid WhatsOnChain plan, lower it for a node you want to go easy on, or set it to `0` to turn the limit off entirely.

The limit applies to the whole service, not per client: concurrent refreshes share one allowance, which is what a per-IP limit at the far end actually measures. At startup the chosen limit is logged at INFO.

**What it paces (CS-457).** For `woc` it is **HTTP requests**. The rate is handed to chain-gang's WhatsOnChain interface, which applies it to every request it makes — including the pages a single `get_utxo` turns into, and the retries. So the configured number is a genuine ceiling on the rate reaching the API.

For `rpc` and `uaas` it is **interface calls**. Those interfaces have no request-level limit of their own, so the service falls back to a decorator that takes one slot per call, and a call may be more than one request. They are unthrottled by default, because the server is the operator's own.

This was not always so. Until chain-gang 0.11.4 the WhatsOnChain client built its own HTTP client with no way to supply one, so its requests were unreachable from this service and the limit could only pace calls — which stopped bounding anything once 0.11.3 made one call many requests. `set_max_requests_per_second` was added upstream for this, and the service requires 0.11.4 or later.

Setting `max_requests_per_second = 0` means unlimited, and is passed down as no limit rather than as a limit of zero.

#### When the interface is unreachable

A failed refresh is logged as a warning with the length of the run so far:

```
WARN blockchain interface failure #7: get_utxo failed: ... 429 Too Many Requests
```

and when it comes back, that is logged too:

```
INFO blockchain interface recovered after 7 consecutive failure(s)
```

The recovery line matters as much as the warnings. Without it, warnings simply stop, and a service that has recovered looks exactly like one that has given up. `GET /status` reports `blockchain_status` and `blockchain_update_time` alongside, for the same question asked at a point in time rather than in the log.

Note that `GET /health` stays `ok` throughout: it is a liveness check and deliberately depends on no upstream. `GET /ready` is the endpoint that reflects whether the service can do its job: it fails once the last successful chain read is older than `service.ready_max_chain_read_age_seconds` (CS-505).

**An unreachable blockchain interface at startup is a warning, not a fatal error** (CS-505). It used to stop the service — on a single WhatsOnChain 429 — and the restart that followed met the same rate limit. Now the service starts degraded, logs `Unable to reach the blockchain interface at startup`, and `GET /ready` answers 503 until the first chain read succeeds; `/health` and `/status` serve throughout.

**`test` is a fixture, not a backend.** It is an in-process stub used by the unit tests, with a UTXO set injected directly by the test harness. It has no network of its own, so the `network` you set alongside it only affects address encoding. It will start and serve requests as a configured backend, but its UTXO set is empty, so balances read zero and funding is refused — useful for exercising the API surface, not for funding anything.

### Every interface call has a deadline

`timeout_seconds` (default `30`) bounds each call to the blockchain interface — a chain refresh, the startup status check, a broadcast when `[mapi_lite]` is not configured — including any wait for a rate-limit slot. A call that outlives it fails like any other chain failure, and is logged and counted the same way (CS-502).

It exists because WhatsOnChain's client has no timeout of its own, and a read that is accepted and never answered would otherwise wait forever. Chain refreshes are one at a time per client, so that one read would hold the client's refresh: every request needing a fresh read would queue behind it and the periodic refresh would skip the client, with nothing logged. A broadcast that runs out of time is reported as `broadcast_outcome_unknown`, since it may have reached the network.

It bounds a whole call, and WhatsOnChain's UTXO listing is one call made of several paced page requests, so leave room for the largest address the service funds from. `0` turns the deadline off and logs a warning at startup.

### A note on the port

`listen_addr` defaults to `127.0.0.1:8080`. Port `8080` is also mapi-lite's default, among other nChain services. If you are running this alongside one of those, change the port here or there; nothing will respond on a port that another process already holds.

### Required keys

Most keys have defaults, and most sections can be left out. These cannot, and the service refuses to start without them:

* `blockchain_interface.interface_type` and `blockchain_interface.network`
* `service.utxo_refresh_period`
* `dynamic_config.filename`

Within an optional section, `mapi_lite.base_url` is required when `[mapi_lite]` is present, and `client_id` and `wif_key` in each `[[client]]`.

## listen_addr

Where the REST API listens, as `host:port`. A top-level key, before any `[table]`, named as it is in Event and mapi-lite (CS-624).

```toml
listen_addr = "127.0.0.1:8080"
```

Default `127.0.0.1:8080`. It replaces `[web_interface] address` and `port`, which are now refused, and the `APP_ENV=docker` switch, which is no longer read.

mapi-lite also defaults to port 8080. Running both on one host, as a local regtest setup does, move this service to another port: Event's regtest config expects it on 8081.

The Docker image sets `FS__LISTEN_ADDR=0.0.0.0:8080`: a container's own loopback cannot be reached from the host. Which host interfaces then see the port is set where it is published — `run.sh` publishes it on the host's loopback. A container run with `--network host` that should bind one address takes `-e FS__LISTEN_ADDR=10.1.2.3:8080`; the image's health check expects port 8080.

Listen on loopback, or place the service behind a reverse proxy on a private network, when exposing funding endpoints.

## [web_interface]

Configures who may call the REST API.

```toml
[web_interface]
# admin_api_key = "your-admin-secret"
```

### Admin authentication

Set `admin_api_key` to require a shared secret on `POST /client`. Clients must send either:

* `Authorization: Bearer <admin_api_key>`
* `X-API-Key: <admin_api_key>`

When `admin_api_key` is omitted, `POST /client` is unauthenticated. The service logs a warning at startup when the admin key is not configured.

### Authentication is required on mainnet and testnet

With `network` `mainnet` or `testnet`, the service **refuses to start** (CS-514) when:

* `admin_api_key` is unset — `POST /client` and `DELETE /client` would be open; or
* any client, static or dynamic, has no `api_key` — its `/fund`, balance and address routes would be open; or
* `dynamic_config.allow_plaintext_wif` is set — a raw key would cross the network in a `POST /client` body. This one has no opt-out.

At runtime, `POST /client` likewise refuses a client without an `api_key` (or `api_key_env`).

If a gateway in front of the service authenticates callers instead, set `web_interface.allow_unauthenticated = true`. The service then starts as before, with a warning. `regtest` and `stn` are unaffected.

### TLS is required in front of the service in production

The service speaks plain HTTP. API keys, and the WIF a client is funded from, are secrets, so in production terminate TLS in front of it — a reverse proxy or load balancer on the same host or a private network — and do not expose its port directly. `POST /client` takes only `wif_env` (an environment-variable name, not the key) on mainnet and testnet, so no private key needs to cross the network at all; API keys still do, which is what the TLS is for.

### Rate limiting

Optional per-IP rate limiting is configured under `[web_interface.rate_limit]`:

```toml
[web_interface.rate_limit]
enabled = true
requests_per_second = 10
burst_size = 20
# trusted_proxies = ["10.0.0.1"]   # your reverse proxies, if any
```

* `enabled` — turn rate limiting on or off (default: `false`)
* `requests_per_second` — sustained request rate allowed per client IP (default: `10`)
* `burst_size` — maximum burst before limiting (defaults to `requests_per_second`)
* `trusted_proxies` — addresses of the reverse proxies in front of the service (default: none)

When enabled, excess requests receive HTTP 429 with a JSON error body. `/health` and `/ready` are exempt so container orchestration probes are not throttled.

Behind a reverse proxy, list the proxy in `trusted_proxies`; otherwise every caller arrives from the proxy's address and shares one limit. A request from a trusted proxy is limited by the client address it reports in `X-Forwarded-For`: the right-most entry that is not itself a trusted proxy, since entries further left were supplied by the client and could say anything. From any other address the header is ignored, so a caller cannot choose its own key by sending one.

## Logging

The log level is set by `RUST_LOG`, as it is for Event and mapi-lite (CS-624): `RUST_LOG=debug`, or per module, `RUST_LOG=info,financing_service=debug`. Unset or empty, it is `info`. The levels are `error`, `warn`, `info`, `debug` and `trace`.

A `RUST_LOG` that does not parse stops startup, naming it. The `[logging]` section is no longer read and is refused: delete it, and set `RUST_LOG` instead of `level`.

Every funding broadcast is logged before it is sent and again with its outcome, each line naming the transaction as `funding tx <txid>`:

```text
INFO  broadcasting funding tx <txid> (client_id=id1, outpoints=1, broadcaster=mapi-lite)
INFO  broadcast accepted: funding tx <txid> (broadcaster=mapi-lite)
WARN  broadcast failed: funding tx <txid> (class=rejected, broadcaster=mapi-lite): <upstream's reason>
```

`class` is `failed`, `rejected` or `indeterminate`, matching the `broadcast_failed`, `broadcast_rejected` or `broadcast_outcome_unknown` the caller was answered with. At `debug` the transaction's hex is logged too, as `funding tx <txid> hex=...`.

`RUST_LOG` applies to a release build, which is what the Docker image runs, as it does to any other: no level is compiled out (CS-630). At `debug` and `trace` the dependencies log too, among them chain-gang's blockchain interface calls and the HTTP client's connections; `RUST_LOG=info,financing_service=debug` keeps them at `info`.

## [telemetry]

Optional OpenTelemetry export via OTLP (gRPC): traces, and metrics (see [Metrics](#metrics)). Both are disabled by default; logs are not exported over OTLP.

```toml
[telemetry]
enabled = true
service_name = "financing-service"
otlp_endpoint = "http://localhost:4317"
```

* `enabled` — export traces and metrics to an OTLP collector (default: `false`)
* `service_name` — `service.name` resource attribute (default: `financing-service`)
* `otlp_endpoint` — OTLP gRPC endpoint (default: `http://localhost:4317`)

When enabled, the service:

* Creates a span per HTTP request via `tracing-actix-web` (OpenTelemetry semantic conventions)
* Exports traces in batches to the configured collector
* Bridges existing `log` crate output through `tracing` to include `trace_id` in logs
* Exports the [metrics](#metrics) periodically to the same collector

Standard OpenTelemetry environment variables are also supported:

| Variable | Purpose |
|----------|---------|
| `OTEL_TRACES_EXPORTER=otlp` | Enable trace export when `telemetry.enabled` is `false` |
| `OTEL_METRICS_EXPORTER=otlp` | Enable metrics export when `telemetry.enabled` is `false` |
| `OTEL_SERVICE_NAME` | Overrides `telemetry.service_name` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Overrides `telemetry.otlp_endpoint` |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | Trace-specific OTLP endpoint |
| `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | Metrics-specific OTLP endpoint |
| `OTEL_METRIC_EXPORT_INTERVAL` | Metrics export interval in milliseconds (SDK default: 60000) |

Traces and metrics have separate switches: `OTEL_TRACES_EXPORTER=otlp` turns on traces only and `OTEL_METRICS_EXPORTER=otlp` metrics only; `enabled = true` turns on both.

Example with the OpenTelemetry Collector:

```bash
docker run -p 4317:4317 otel/opentelemetry-collector:latest
```

Then enable telemetry in config or set `OTEL_TRACES_EXPORTER=otlp` and `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317`.

## Metrics

The service pushes metrics over OTLP (CS-608). There is no `/metrics` endpoint to scrape: point the export at an OpenTelemetry Collector and let it forward to Prometheus (or any other backend).

**Enabling.** Off by default. Set `OTEL_METRICS_EXPORTER=otlp`, or `[telemetry] enabled = true` (which also turns on traces). The endpoint is `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`, else `OTEL_EXPORTER_OTLP_ENDPOINT`, else `telemetry.otlp_endpoint`, else `http://localhost:4317`; the resource carries `service.name` (`OTEL_SERVICE_NAME`, default `financing-service`) and `service.version`. With export off no meter provider is installed and recording costs nothing. Startup logs one line, `OpenTelemetry metrics export on (...)` or `... off`, naming the endpoint without any credential or query string. Metrics are flushed on shutdown.

Names follow OpenTelemetry conventions: counters have no `_total` and units are set on the instrument, not in the name. An OTLP-to-Prometheus conversion adds both, so `http_requests` becomes `http_requests_total` and `http_request_duration` (unit `s`) becomes `http_request_duration_seconds`. Attribute values come from fixed sets -- never raw paths, addresses, txids or keys -- so cardinality is bounded by the routes and clients configured.

| Metric | Type | Unit | Attributes | Meaning | Prometheus name |
|--------|------|------|------------|---------|-----------------|
| `http_requests` | Counter | — | `route`, `status_class` | Requests answered. `route` is the matched route template (`/client/{client_id}/balance`, never the raw path), `unmatched` for a path no route matches; `status_class` is `2xx`/`3xx`/`4xx`/`5xx`. `/health`, `/ready` and `/status` count like any other route, and so do requests the rate limiter refuses. | `http_requests_total` |
| `http_request_duration` | Histogram | `s` | `route` | Time to answer a request. Buckets from 5 ms to 60 s. | `http_request_duration_seconds` (`_bucket`, `_sum`, `_count`) |
| `ready` | Gauge (0/1) | — | `check` | The `GET /ready` verdict: one series per check (`chain`, and `mapi_lite` when `[mapi_lite]` is configured) plus `check="overall"`. Sampled every 15 s. | `ready` |
| `fs_fund_requests` | Counter | — | `code` | `POST /fund` answers: `funded`, or the [error code](SupportedEndpoints.md) the caller got (`insufficient_balance`, `no_suitable_utxo`, `funds_in_flight`, `broadcast_rejected`, `broadcast_failed`, `broadcast_outcome_unknown`, `partial_broadcast`, `chain_unavailable`, `invalid_request`, `unknown_client`, `unauthorized`, `rate_limited`, `key_in_progress`, `idempotency_key_reused`, `internal`). A replayed `idempotency_key` answer counts as the outcome it replays. A body that is not valid JSON is refused before the handler and appears only in `http_requests`. | `fs_fund_requests_total` |
| `fs_client_balance` | Gauge | `sat` | `client_id`, `kind` | Each client's balance from the service's cache, `kind` = `confirmed` or `unconfirmed`. Sampled every 15 s. | `fs_client_balance_sat` |
| `fs_client_chain_state_age` | Gauge | `s` | `client_id` | Seconds since the client's unspent set was last read from the chain -- what `service.ready_max_chain_read_age_seconds` is measured against. No series until the first read. | `fs_client_chain_state_age_seconds` |

The gauges are read from a snapshot a background task takes every 15 s while export is on; the one upstream call it makes is the mapi-lite readiness probe `GET /ready` already makes, cached for 2s.

**Suggested alerts** (CS-608 phase 1), in PromQL over the converted names:

1. Not ready: `ready{check="overall"} == 0` for 2m.
2. Outcome unknown, so funds may be spent without the caller knowing: `increase(fs_fund_requests_total{code="broadcast_outcome_unknown"}[5m]) > 0`. Across the stack, also Event's `increase(event_funding_outputs_stranded_total[5m]) > 0` and its `*outcome_unknown` outcomes.
3. A client running dry: `fs_client_balance_sat{kind="confirmed"} < <threshold>` (per client; pick the threshold from its spend rate).
4. mapi-lite (exported by mapi-lite): `mapi_nodes_live < <min_live_nodes>` or `mapi_block_ingest_lag > 3`.
5. Errors or latency, for 5m: `sum(rate(http_requests_total{status_class="5xx"}[5m])) / sum(rate(http_requests_total[5m])) > 0.01`, or `histogram_quantile(0.95, sum by (le, route) (rate(http_request_duration_seconds_bucket[5m]))) > <target>`.

## [service]

```toml
[service]
utxo_refresh_period = 60
# chain_state_max_age_seconds = 60   # defaults to utxo_refresh_period
# ready_max_chain_read_age_seconds = 180   # defaults to 3 x utxo_refresh_period, at least 60; 0 = off
# inflight_state_file = "./data/dynamic.inflight.json"   # defaults beside dynamic_config.filename
```

* `utxo_refresh_period` — seconds between the periodic refresh of every client's chain state.
* `chain_state_max_age_seconds` — how old that cached state may be before a request refreshes it again. Defaults to `utxo_refresh_period`.
* `ready_max_chain_read_age_seconds` — how old a client's last successful chain read may be before `GET /ready` reports the service unready (CS-505). Defaults to three refresh periods and never under 60s: one failed sweep is a blip, three is chain reads not working. `0` turns the check off. See [Readiness check](SupportedEndpoints.md#readiness-check).
* `inflight_state_file` — where the service keeps what it has broadcast but the chain may not have caught up with. Defaults to a file beside `dynamic_config.filename`, named after it: `./data/dynamic.toml` pairs with `./data/dynamic.inflight.json`. See [In-flight state survives a restart](#in-flight-state-survives-a-restart).

### In-flight state survives a restart

When the service funds a transaction it holds back the inputs that transaction spent, and keeps the change it created, until the chain reports both. Without that, a refresh in the seconds before the read interface has seen the transaction would offer the same input to the next request (CS-426).

That state is now **written to disk after every funding commit** and read back at startup (CS-465). Before, it lived only in memory, so a restart forgot it: the chain it came back to had not yet seen the transactions, still reported their inputs as unspent, and the service rebuilt them — byte for byte, because signing is deterministic — handing each outpoint to a second caller.

After a restart the service now picks up where it stopped: inputs it spent stay held back, and it spends the change its last transaction created even though the chain has not reported it yet. Entries older than the ten-minute reservation window are dropped rather than restored, since they would have expired had the service stayed up.

**The file has to survive a restart**, the same as `dynamic.toml` beside it. A deployment that already keeps `dynamic.toml` on a volume keeps this too, with nothing to add. If the file is missing the service starts with no in-flight state, which is normal on first run; if it is unreadable, that is logged as an error and the service starts without it rather than refusing to — in that case outpoints spent shortly before the restart may be handed out again, which is the behaviour this replaced.

The ten-minute window is backed by the chain itself, from chain-gang 0.11.5. WhatsOnChain goes on listing an output after a mempool transaction spends it, flagged `isSpentInMempoolTx`; earlier chain-gang releases ignored the flag and reported such outputs as unspent, so a transaction still unconfirmed when its reservation ran out would have had its input offered again. Now the read interface drops them, the way a node's own `listunspent` does, and the reservation is a guard against the read interface lagging rather than the only thing standing between an input and a second spend.

**Commits share writes** (CS-495). Each write serialises and flushes the whole state, so writing once per commit capped a busy client at the rate one write takes — and that rate falls as the state grows, which it does through each refresh window under load. Commits that finish while a write is in progress now wait for the next one, which covers them all, so a burst costs one write rather than one per request. Every request is still answered only after a write that includes it. The file is written compact rather than pretty-printed; `jq . dynamic.inflight.json` reads it.

One window remains: the moment between a broadcast succeeding and the state reaching disk. A process killed in that moment can still lose it. A clean restart, or a crash at any other time, does not.

### How often the service reads the chain

Every chain-state refresh is one request to the blockchain interface, and `POST /fund` and `GET /balance` each want current state. Refreshing on every request means the cost scales with traffic, which is what runs into a public API's rate limit — see [Outbound rate limiting](#outbound-rate-limiting).

So a request reuses the cached state while it is younger than `chain_state_max_age_seconds`, and the periodic refresh skips clients a request has already refreshed inside that window. At the default the two are the same length, so the steady-state cost is **one request per client per period however much traffic arrives**.

**The trade is staleness.** Widening the window widens the period in which the service can build a funding transaction against a view of the chain that has moved on. It is narrower than it looks: the service applies its own spends to the cache as it makes them, so this only matters if something *outside* the service spends from the same client key — another service sharing the WIF, or an operator moving funds by hand. If that is your deployment, lower it. `0` refreshes on every request, which is what the service did before this existed.

There is a backstop either way. A broadcast the upstream refuses — which is how a conflicting input shows up — marks the cached state stale, so the attempt after it works from a fresh read. Building on a stale view therefore costs one refused transaction, not a run of them.

Requests that find the same client's state stale at the same moment **share one refresh**: the first fetches, and the rest wait for that answer rather than each fetching it again behind the rate limit. The periodic refresh leaves a client alone while a request is refreshing it.

**With `[mapi_lite]`, a funding request does not wait for a refresh it only needs because the cache has aged.** The broadcast goes to mapi-lite, so the refresh in front of it was the only thing holding a `POST /fund` behind the paced interface. The request is funded from the cached state and the refresh runs behind it. The interface stays paced, so WhatsOnChain's limit is still respected. A request still waits when the cache is not usable: before a client's state has been loaded for the first time, and after a refused or uncertain broadcast has marked it stale (the backstop above). Without `[mapi_lite]` the broadcast itself goes through the paced interface, so nothing changes there. `GET /balance` always waits for a stale refresh, since a current balance is what it is asked for.

## [idempotency]

Optional. Controls how long `POST /fund` idempotency records are retained; see [Idempotency](SupportedEndpoints.md#idempotency) for what they do. Both fields have defaults, so the section can be omitted entirely.

```toml
[idempotency]
ttl_seconds = 600
max_entries = 10000
```

* `ttl_seconds` — how long a completed record stays replayable. Should comfortably exceed your clients' retry window; too short and a legitimate retry funds a second time. Default `600`.
* `max_entries` — upper bound on retained records, so a client sending a fresh `idempotency_key` on every request cannot grow the store without limit. When full, the oldest record of **whichever client holds the most** is dropped, so a client filling the store evicts its own records rather than other clients' (whose retries would then fund twice). Default `10000`.

Each record holds a hash of the request rather than the request, and an `idempotency_key` may be at most 256 bytes, so a record's size does not depend on what the client sends.

Records are held **in memory only** and are lost when the service restarts, so a retry that spans a restart can still produce a second funding transaction.

## [funding_limits]

Optional, and off unless set. Per-client caps on what `POST /fund` pays out — `satoshi` × `no_of_outpoints`, fees aside — so a leaked client API key is not worth the whole wallet in one call.

```toml
[funding_limits]
max_satoshis_per_request = 100000     # one request
max_satoshis_per_period = 1000000     # one client, within period_seconds
period_seconds = 3600                 # default
```

* `max_satoshis_per_request` — a larger request is refused with `400` `invalid_request`.
* `max_satoshis_per_period` — a request that would take the client past this within any `period_seconds` window is refused with `429` `rate_limited` and a `Retry-After` giving the soonest it could fit. A request's amount counts from the moment it is accepted, so concurrent requests cannot both slip under the limit, and is given back if nothing was broadcast. The window is held in memory and restarts with the service.
* A limit of `0`, or a `period_seconds` of `0` with a period limit, is refused at startup.

Whatever these say, no request may pay out more than the 21 million BSV that exist; one asking for more, or whose `satoshi` × `no_of_outpoints` overflows, is a `400`.

The right values depend on what your clients fund. Event, for example, asks for one transaction's fee and outputs at a time — a few hundred satoshis.

## [fees]

Optional. Sets the fee the service pays on the funding transactions it builds. Both fields have defaults, so the section can be omitted.

```toml
[fees]
satoshis_per_kb = 100
# max_satoshis_per_kb = 1000
```

* `satoshis_per_kb` — the rate, in satoshis per kilobyte. The fee for a transaction is `ceil(tx_bytes * satoshis_per_kb / 1000)`, rounded **up** so that rounding never underpays. `tx_bytes` is the transaction's serialised size, counted as an upper bound: every output is its 8-byte value, its script's length prefix and the script, and every input is costed at the longest a signature can be. So a transaction never pays less than the rate on the bytes it actually has — until CS-471 each funded output was counted as its script alone, nine bytes short, and an ordinary transaction paid about 97 sat/KB at a configured 100. Must be greater than zero; the service refuses to start otherwise, because a transaction paying no fee is not relayed. Default `100`.
* `max_satoshis_per_kb` — the highest rate a fee quote may set (CS-523, SR-SEC-020). A quote above it is ignored, with a warning, and the current rate kept. It is not clamped: a rate that high is forged or a mistake. Default `1000`, which is ten times the default `satoshis_per_kb` and a thousand times mapi-lite's own default quote. Must be at least `satoshis_per_kb`.

### Dust change goes to the fee

A change output of a satoshi or two cannot be spent for less than it holds, so paying it back only strands it. Funding the reported `max_fundable` used to do exactly that: it left one satoshi behind, and the client was then holding a balance of 1 with `max_fundable: 0`.

The service now leaves the change out entirely when it falls under the dust threshold, so **funding the maximum spends the wallet out and produces a transaction with no change output at all**.

**The threshold is derived from the fee rate, not configured.** It is the round-trip cost of a change output — the 34 bytes it adds to the transaction that creates it, plus the 148 it will cost to spend later — priced at the rate above and doubled:

```text
max(1, 2 × ceil(182 × satoshis_per_kb / 1000))
```

At the default 100 sat/KB that is **38 satoshi**. Below it, creating the output and later sweeping it costs more in fees than it returns, so folding it is strictly better; above it, paying it back is. Deriving it means it cannot drift out of step with what the service is actually paying, including when the rate comes from mapi-lite's `miningFee`.

The doubling is headroom: the output is created at today's rate but swept at some unknown future one, and at exactly break-even a rate rise strands it — which is the failure this fixes.

This is the standard formula, and only the constant was ever wrong. At Bitcoin Core's 3000 sat/KB relay fee it gives 546 undoubled — which is exactly where that familiar number comes from. Core's figure is not wrong; it is pinned to a fee rate three orders of magnitude above BSV's.

Two things follow from that, both worth knowing before you set the threshold:

* **The remainder goes to the miner.** At most one satoshi under the threshold — 37 at the default rate — and only on a transaction that would otherwise have created an output nobody would spend.
* **Selection avoids it where it can.** Given a choice, the service picks a UTXO whose change is either nothing or worth an output, so an ordinary spend does not donate. It falls back to folding only when no such UTXO exists — a request stays fundable rather than being refused to save a sum smaller than the threshold.

### The funded outputs are not always at index 1

A funding transaction used to be change-at-0 followed by the funded outputs, so the first funded output was always index 1. A transaction whose change was folded into the fee has no change output, and its funded outputs start at **0**.

The `index` in each `outpoints` entry of the `POST /fund` response is correct either way. **A caller that assumes 1 rather than reading `index` will reference an output that does not exist.**

### Upgrading from 4.2.0 or earlier pays a lower fee

Before this section existed the fee was hardcoded as `((tx_bytes / 1000) * 500) + 750`. That is a step function rather than a rate: 750 satoshi for any transaction under a kilobyte, jumping by 500 at each kilobyte after.

An ordinary funding transaction — one input, two outputs — is 226 bytes, so it used to pay **750** satoshi, an effective 3300 sat/KB. At the new default of 100 sat/KB it pays **23**.

That is the point of the change, but it is a change in behaviour and not only a refactor: **an existing deployment that upgrades without adding a `[fees]` section will pay less than it used to.** Set `satoshis_per_kb` explicitly if you need the old figures, and note that a rate your miners will not accept is only discovered at broadcast.

The lower fee also changes which UTXOs get spent. A cheaper transaction can be funded by a smaller input, so the service now reaches for small UTXOs where it previously had to break up a large one.

### Taking the rate from mapi-lite

With `[mapi_lite]` configured, the service reads the **standard** mining fee from mapi-lite's `feeQuote` and converts it to satoshis per kilobyte, rounding up. One server then sets the rate for every service broadcasting through it, rather than each keeping its own number.

`satoshis_per_kb` remains the fallback. It is used before the first quote arrives, and whenever a refresh fails — a quote that cannot be fetched, names no fee, or gives a zero rate leaves the rate in force untouched. A mapi-lite that is down therefore stops the rate *changing* rather than stopping funding.

There is no longer a switch to turn the quote off: `use_mapi_fee_quote` is refused (CS-624). To pin the rate, have mapi-lite quote it; `max_satoshis_per_kb` bounds what any quote can set.

The quote is refreshed on the same sweep that refreshes balances (`service.utxo_refresh_period`), not on the funding path, so costing a transaction never waits on a request to mapi-lite. The rate can therefore be up to one sweep out of date.

## [mapi_lite]

Optional. When this section is present, funding transactions are **broadcast through a [mapi-lite](https://github.com/nchain-innovation/mapi-lite) server** instead of through the `[blockchain_interface]`. Chain reads — balances and UTXO refreshes — are unaffected and keep using `[blockchain_interface]`. Leave the section out and the service behaves exactly as before: transactions are broadcast via WhatsOnChain (or whichever interface is configured).

```toml
[mapi_lite]
base_url = "http://127.0.0.1:8080"
auth_token = "env:MAPI_AUTH_TOKEN"
# total_timeout_seconds = 45
# miner_id_public_keys = ["02…"]   # mapi-lite's minerId; see below
```

* `base_url` — **required.** Base URL of the mapi-lite server, `http://` or `https://`. Surrounding whitespace and a trailing `/` are stripped before the URL is validated or used, so the value that is checked at startup is the one that is requested.
* `auth_token` — optional. The token alone, as mapi-lite's own `auth_token` is: every request carries `Authorization: Bearer <token>`. **Before CS-624 this held the whole header**; a value starting `Bearer `, or holding any space, is now refused at startup, so drop the prefix. Takes an `env:VAR_NAME` reference, or can be set with `FS__MAPI_LITE__AUTH_TOKEN`. A plaintext value is reported at startup like any other plaintext secret. Never logged. mapi-lite's `GET /ready` is unauthenticated, so the startup probe and `GET /ready` also fetch its signed fee quote, which is not: a 401 there, a token mapi-lite refuses or one it needs and was not given, stops startup and makes `/ready` report mapi-lite not ready, naming the setting (CS-632).
* `miner_id_public_keys` — mapi-lite's miner-ID public keys, as hex (33-byte compressed or 65-byte uncompressed): the `minerId` in every response it signs (CS-523, SR-SEC-019). **Set this.** A signed response is otherwise checked only against the key *it carries*, which shows it was not altered, not who sent it. Anyone able to answer in mapi-lite's place — over plain HTTP, anyone on the path — could then forge an "accepted" or a fee quote. A response signed by any other key is refused: a fee quote is ignored, and a submit is reported as `broadcast_outcome_unknown`, because what the real mapi-lite did is hidden. Once any signed answer has shown mapi-lite signing with an unlisted key, `GET /ready` reports mapi-lite not ready and `POST /fund` sends nothing, answering 503 `broadcaster_untrusted`, until a signed answer verifies again (CS-618, SR-SEC-033): otherwise each funding would land and be reported as an unknown outcome, holding its inputs for 600s. List more than one key to cover a rotation; from the environment, `FS__MAPI_LITE__MINER_ID_PUBLIC_KEYS` takes them comma-separated (CS-636). **Required on mainnet and testnet**: left empty there, the service refuses to start (CS-624). On regtest and STN it may be empty, which accepts any valid signature and is warned about at startup. Invalid hex or length is refused at startup.
* `total_timeout_seconds` — ceiling on a whole submit: every attempt and every back-off between them. Default `45`. The one timing setting for mapi-lite (CS-624): the rest is derived from it and from what mapi-lite publishes, below. It is the worst case a `POST /fund` caller — and the actix worker serving it — can be held by an unresponsive mapi-lite.

  On expiry the caller gets `broadcast_outcome_unknown` (HTTP 504) if an attempt was still in flight, because cancelling a request says nothing about what the server did with it and the transaction may be on the network; the service reserves that transaction's inputs in response — see [When the outcome is unknown](SupportedEndpoints.md#when-the-outcome-is-unknown). If instead the deadline was consumed by back-off between attempts that were all refused outright, nothing was ever delivered and the caller gets `broadcast_failed` (HTTP 502) with no reservation.

**`timeout_seconds`, `max_retries` and `health_timeout_seconds` are gone (CS-624)**, and refused if present. They had to be kept in step with each other and with mapi-lite by hand. Now:

* **Each attempt** outlasts mapi-lite's submit ceiling by 3s. mapi-lite answers a submission it cannot finish within its `submit_timeout_sec` (default 10s) with HTTP 504, outcome unknown, and publishes that ceiling in `GET /ready` as `submitTimeoutSecs`. An attempt that gave up first would report a hung node as *this* service's timeout: the same unknown outcome, from the wrong layer, with mapi-lite still working on a request nobody is waiting for (CS-510). The service reads the ceiling whenever it probes mapi-lite, at startup included, and assumes the 10s default until then. Against the default it is 13s, as before.
* **The retries** are as many as fit inside `total_timeout_seconds` when every attempt runs to its bound, with linear back-off of 1s, 2s, … capped at 5s. At the defaults that is two, so three attempts in 42s, as before. Resubmitting is safe: mapi-lite answers an already-known transaction with success — which is also how a retry can *settle* an attempt that timed out, turning `broadcast_outcome_unknown` back into a plain success.
* **The probe** behind `GET /ready` is bounded at 2s, as Event's is, under the 3s a readiness probe allows (Docker's health check defaults to `--timeout=3s`). Its verdict is cached for as long, so `/ready` — unauthenticated and exempt from rate limiting — cannot be used to flood mapi-lite.

A `total_timeout_seconds` shorter than one attempt against the ceiling cuts every attempt short, so a hung node is reported as this service's deadline. That still works, so the service **warns**, naming the value that would not (SR-CFG-010), and carries on. The budget, outermost first:

| Layer | Gives up after (defaults) |
|---|---|
| Event → `POST /fund` | 90s; this service publishes its own worst case in `GET /status` as `fund_worst_case_seconds` for Event to check (135s at these defaults) |
| this service → mapi-lite, whole submit | `total_timeout_seconds` = 45s |
| this service → mapi-lite, per attempt | mapi-lite's ceiling + 3s = 13s |
| mapi-lite → node, whole submit | `submit_timeout_sec` = 10s, answered 504 |

When the section is present the service:

* Logs at startup: `mapi-lite integration configured (base_url=...): funding transactions will be broadcast via mapi-lite`. Without the section the line reads `mapi-lite not configured: funding transactions will be broadcast via the 'woc' blockchain interface` (naming whichever `interface_type` is configured).
* Probes mapi-lite at startup (its `GET /ready`) and **warns** if it is unreachable or not ready, without refusing to start. Funding will fail until mapi-lite is reachable, but the read paths — `/status`, balances, UTXO refreshes — do not depend on it and keep serving, and the service recovers on its own when mapi-lite returns. Refusing to start would instead put the container in a restart loop driven by its own health check, taking the read paths down with it. The one exception is a mapi-lite whose nodes are on another network than `network`: that is the configuration, which no wait mends, so the service refuses to start (CS-624).
* Asks mapi-lite's own `GET /ready` for this service's `GET /ready`, and returns HTTP 503 when mapi-lite is unreachable or not ready, reusing a verdict for up to 2s. mapi-lite's readiness fails when it cannot do its work: its nodes are unreachable, its block ingest has fallen behind, or it cannot sign or reach its database; the fee quote this used to probe is served from mapi-lite's configuration and answered with every node down (CS-505). `GET /health` is unaffected. See [Readiness check](SupportedEndpoints.md#readiness-check).
* Reports `"broadcaster": "mapi-lite"` in `GET /status`.
* Funds a request from cached chain state that has merely aged, rather than waiting for it to be refreshed through `[blockchain_interface]`, and refreshes it behind the request — see [How often the service reads the chain](#how-often-the-service-reads-the-chain).
* Submits each funding transaction as a one-element batch to `POST /mapi/txs`, with the merkle proof declined (no callbacks are wanted). A rejection by mapi-lite or the node surfaces to the caller as `broadcast_rejected` (HTTP 409) when the upstream calls it final or names a conflicting transaction, since retrying cannot help, and otherwise as `broadcast_failed` (HTTP 502), as does a mapi-lite `503` (no node can have received it); either way the reason is in the service log. Anything that may have reached the network surfaces as `broadcast_outcome_unknown` (HTTP 504) instead, and the inputs are reserved: an answer that never arrives or cannot be read, a mapi-lite `500`/`502`/`504` (from mapi-lite v0.4.0 a `504` means a node may have received it), and mapi-lite's "accepted by the node but could not be recorded", which is resubmitted first since the resubmission records it (CS-501).

The mapi-lite server must be pointed at the same network as `network`, since the funding transactions are signed for that network. **It is checked (CS-624):** mapi-lite v0.6.0 and later publish their nodes' network in `GET /ready` (`chain`), and when it is not `network` the service refuses to start, naming both. A mapi-lite that cannot be reached at startup is still only warned about. If it later answers with another network, or a mapi-lite that answered nothing about its network now does, `GET /ready` reports mapi-lite not ready. Its body says only "mapi-lite probe failed", as for any mapi-lite failure, since it answers anyone; the message naming both networks goes to the log. Because mapi-lite talks to a node directly, this is also a way to broadcast on regtest while still reading chain state through another interface.

Building the service with this integration requires read access to the private `mapi-lite` repository — see [Dependencies.md](Dependencies.md).

## [dynamic_config]

Path to the file used to persist clients added at runtime via `POST /client`. Dynamically added clients may include an `api_key` field in the same format as static `[[client]]` entries.

```toml
[dynamic_config]
filename = "./data/dynamic.toml"
# allow_plaintext_wif = false   # true lets POST /client store a raw wif; development only
# wif_env_prefix = "FS_RUNTIME_WIF_"
```

* **Only `env:VAR` references by default.** `POST /client` refuses a raw `wif` with HTTP 400 and stores nothing; send `wif_env`, naming an environment variable that holds the key, and the file records `env:VAR` instead. `allow_plaintext_wif = true` lifts this for development; on mainnet and testnet it is refused at startup (CS-514). A file written before this, holding raw WIFs, still loads, with the usual plaintext warning.
* **Written atomically, owner-only.** Every change is written to a `0600` sibling file, synced, and renamed over the original, so a crash cannot truncate it and other users cannot read it. A file found readable by other users at startup is made `0600`, with a warning. A client is added or removed in memory only once the file has been written.
* **A file that does not parse stops startup.** It is left exactly as it is, to be fixed by hand, rather than replaced with an empty list on the next `POST /client`. A missing or empty file means no dynamic clients.
* `wif_env_prefix` — the only environment variables a client added through `POST /client` may take its key from with `wif_env`. Default `FS_RUNTIME_WIF_`. It is rejected at startup if it overlaps `FS__`, which every configuration override starts with: those can carry any secret (CS-624).

**Runtime clients cannot take another client's wallet (CS-521, SR-SEC-018).** Before this, `wif_env` could name any variable in the service's environment, another client's key variable included. Whoever could call `POST /client` (anyone, with no `admin_api_key`) could then fund from that client's wallet under an API key of their own. Now:

* `wif_env` must start with `wif_env_prefix`. **This is a breaking change:** a deployment that names other variables has to rename them, or set the prefix to match.
* A `wif` beginning `env:` is refused; name a variable with `wif_env` instead.
* A client whose key is the funding key of an existing client is refused, however the key was supplied. Two clients funding from one wallet would spend the same coins.

Clients that already share a wallet when the service starts are reported as an error in the log, naming them. The service still starts. Remove the one that should not be there from the dynamic config.

## [[client]]

Static client configuration. Clients can also be added at runtime via the REST API.

```toml
[[client]]
client_id = "id1"
wif_key = "cW1ciwAgTLs2EGa6cZHpf...kvq72s15rbiUonkrQAhDU4FG"
api_key = "your-client-secret"
```

* `client_id` — identifier used in API requests
* `wif_key` — WIF private key for the client's funding wallet. It must be a key for `blockchain_interface.network` (CS-634): a mainnet key on mainnet, a test-network key on testnet, regtest or STN, which share a WIF version. A key for the other network stops startup, naming the client and both networks, and `POST /client` refuses one with HTTP 400 `invalid_request`. Its address would be on the other network, so its wallet would read empty and every funding would fail.
* `api_key` — optional shared secret for this client's API endpoints

When `api_key` is set for a client, requests for that client must include either `Authorization: Bearer <api_key>` (the scheme in any case, `bearer` too: CS-663) or `X-API-Key: <api_key>`. A request carrying both is judged on the Bearer key alone, so a proxy that adds its own `Authorization: Bearer` header, in any case, replaces the caller's `X-API-Key`. Clients without an `api_key` remain unauthenticated; restrict them via network isolation. The service logs a warning at startup listing clients without an `api_key`.

## Secret management

Avoid storing literal secrets in TOML when possible. The service supports:

### `env:VAR_NAME` references

Use the `env:` prefix to load a value from the process environment at startup:

```toml
[web_interface]
admin_api_key = "env:FS_ADMIN_API_KEY"

[[client]]
client_id = "id1"
wif_key = "env:MY_CLIENT_WIF"
api_key = "env:MY_CLIENT_API_KEY"
```

If a referenced variable is not set, startup fails with a clear error.

### Environment overrides

`FS__<TABLE>__<KEY>` sets any setting, over whatever the file holds, as `TE__` and `MAPI__` do for Event and mapi-lite (CS-624). The value is taken as written; the setting parses it, so a port or a number of seconds can be overridden too.

| Variable | Sets |
|----------|------|
| `FS__WEB_INTERFACE__ADMIN_API_KEY` | `web_interface.admin_api_key` |
| `FS__BLOCKCHAIN_INTERFACE__RPC_USER` / `FS__BLOCKCHAIN_INTERFACE__RPC_PASSWORD` | `blockchain_interface.rpc_user` / `rpc_password` |
| `FS__MAPI_LITE__AUTH_TOKEN` | `mapi_lite.auth_token` (the token alone, without `Bearer `) |
| `FS__LISTEN_ADDR` | `listen_addr` |
| `FS__MAPI_LITE__MINER_ID_PUBLIC_KEYS` | `mapi_lite.miner_id_public_keys`, comma-separated: `02ab…,03cd…` |

An override is text, so it cannot give a list. `mapi_lite.miner_id_public_keys` is the exception (CS-636): required on mainnet and testnet, it takes a comma-separated value, replacing the file's list, so a deployment configured from the environment needs no file for it. Spaces around each key are ignored, and an empty value is an empty list.

A `[[client]]` entry sits in an array, which an override cannot address: give its `wif_key` and `api_key` as `env:` references.

**Breaking change (CS-624).** The named overrides `FS_ADMIN_API_KEY`, `FS_RPC_USER`, `FS_RPC_PASSWORD`, `FS_MAPI_LITE_AUTH_TOKEN`, `FS_CLIENT_<ID>_WIF` and `FS_CLIENT_<ID>_API_KEY` are no longer read. Rename the first four to their `FS__` form; for a client, write `wif_key = "env:FS_CLIENT_ID1_WIF"` (any variable name works) in the file.

### Plaintext warnings

When WIF keys, client `api_key`, `admin_api_key`, `rpc_password`, or `mapi_lite.auth_token` are stored as literal values in the config file, the service logs a warning at startup. A value set by an `FS__` override is not warned about: it is already in the environment. Literal values still work for local development.

Dynamic clients added via `POST /client` use `wif_env` and can use `api_key_env` instead of `wif` and `api_key`; the service stores `env:VAR` references in the dynamic config file rather than the secret values. A raw `wif` is refused unless `dynamic_config.allow_plaintext_wif` is set — see [\[dynamic_config\]](#dynamic_config).