# Supported endpoints

The service listens on `listen_addr` (default `127.0.0.1:8080`; the examples below use port `8080`). See [Configuration](Configuration.md#listen_addr).

When a client has an `api_key` configured, that client's endpoints require authentication via either:

* `Authorization: Bearer <api_key>`
* `X-API-Key: <api_key>`

The key must match the `client_id` being accessed. Using another client's key returns `401`.

This applies to `POST /fund`, `GET /client/{client_id}/balance`, `GET /client/{client_id}/address`, and `DELETE /client/{client_id}` when that client has an `api_key`. `/`, `/health`, `/ready`, and `/status` remain unauthenticated.

`POST /client` requires `web_interface.admin_api_key` when configured. Send the admin key via `Authorization: Bearer <admin_api_key>` or `X-API-Key: <admin_api_key>`. The request body may include an optional `api_key` field that is stored for the new client. See [Configuration](Configuration.md).

Missing or invalid credentials return HTTP `401`:

```json
{"code": "unauthorized", "description": "Unauthorized"}
```

When `[web_interface.rate_limit]` is enabled, excess requests from the same IP receive HTTP `429`:

```json
{"code": "rate_limited", "description": "Rate limit exceeded, retry in 1s"}
```

All JSON error responses share this shape:

```json
{"code": "invalid_request", "description": "Error message"}
```

The `code` is a stable, machine-readable classification, and the HTTP status is derived from it — see [Error responses](#error-responses) for the full mapping.

Successful JSON responses use HTTP status `200`.

## Index

`GET /`

Returns a plain-text service identifier.

```bash
curl http://127.0.0.1:8080/
```

```
Financing Service REST API
```

## Health check

`GET /health`

Liveness: is this process running? It checks nothing else — not the blockchain (read) interface, not mapi-lite — and answers 200 whenever the service is up.

```bash
curl http://127.0.0.1:8080/health
```

```json
{"status": "ok"}
```

Point Docker's `HEALTHCHECK` and a Kubernetes `livenessProbe` here. Both can restart the container, and a restart cannot fix an upstream that is down — it would only take `/status`, balances and addresses, none of which need an upstream, down with it.

## Readiness check

`GET /ready`

Readiness: should traffic be sent here right now? Use this for a Kubernetes `readinessProbe` or a load balancer's health check, which remove an instance from rotation rather than restarting it.

```bash
curl http://127.0.0.1:8080/ready
```

Two checks, and the answer is healthy only if both pass (CS-505):

* **`chain`** — chain reads are working. Every client's UTXO set must have been read from the chain within `service.ready_max_chain_read_age_seconds` (default three refresh periods, at least 60s); with no clients, the blockchain interface must have answered within that time. A client never read at all fails too — a service that started while the chain was unreachable. The check reads nothing itself: it reports what the periodic refresh has managed. With [`[mapi_lite]`](Configuration.md#mapi_lite) this matters most, because funding then goes on from the cached state without waiting for a refresh, so failing chain reads would otherwise go unnoticed while the cache grew ever older.
* **`mapi_lite`** — with `[mapi_lite]` configured, mapi-lite's own `GET /ready`, which fails when mapi-lite cannot do its work: its nodes are unreachable, its block ingest has fallen behind, or it cannot sign or reach its database. Until CS-505 this probed `GET /mapi/feeQuote`, which mapi-lite serves from its configuration and which kept answering with every node down. It also fails when mapi-lite's `GET /ready` names another network for its nodes than `blockchain_interface.network` (CS-624), naming both in the log. And it fails when mapi-lite's nodes are on another chain than this service reads (CS-619): alongside that probe, mapi-lite's `/query/tx` is asked about the oldest confirmed UTXO in the clients' wallets, and its nodes not knowing it means a different network — every `/fund` would otherwise fail as missing inputs. A wallet with nothing confirmed gives no evidence, and the check passes.

| Situation | HTTP | Body |
|---|---|---|
| chain reads current, mapi-lite not configured | 200 | `{"status": "ok", "chain": {"ok": true}}` |
| chain reads current, mapi-lite ready | 200 | `{"status": "ok", "mapi_lite": {"ok": true}, "chain": {"ok": true}}` |
| mapi-lite not ready or unreachable | 503 | `{"status": "unhealthy", "mapi_lite": {"ok": false, "detail": "mapi-lite probe failed"}, "chain": {"ok": true}}` |
| chain reads too old | 503 | `{"status": "unhealthy", "chain": {"ok": false, "detail": "client event's chain state was last read 412s ago (limit 180s)"}}` |

`/health` is unaffected by that 503 and stays 200, which is the point of the split. Sending a liveness probe to `/ready` puts the restart loop back.

The probe is bounded at 2s, under the few seconds a readiness probe allows before it gives up.

The `mapi_lite` `detail` is deliberately generic. This endpoint is unauthenticated and exempt from rate limiting, and the underlying transport error names the mapi-lite host and port; the full error — including the names of mapi-lite's failing checks — is written to the service log instead. The `chain` detail names only a client id and an age, which `GET /status` already shows.

The verdict is **cached for 2s**, so repeated calls do not each reach mapi-lite. Without that cache an unauthenticated, unmetered endpoint could be used to flood the broadcast path. A readiness probe on a 5s interval therefore reaches mapi-lite at most every 2s.

Through a 503 here the service keeps running and keeps serving reads (`/status`, balances, UTXOs), which do not depend on mapi-lite, and returns to ready on its own when mapi-lite comes back.

## Service status

`GET /status`

Returns the current service status.

```bash
curl http://127.0.0.1:8080/status
```

```json
{
    "version": "5.0.1",
    "blockchain_status": "Connected",
    "blockchain_update_time": "2024-11-05 14:42:29",
    "broadcaster": "woc",
    "fund_worst_case_seconds": 120
}
```

`blockchain_status` can be one of:

* `Unknown` — the service has started but not yet connected to the blockchain
* `Failed` — the service failed to connect to the blockchain
* `Connected` — the service is connected to the blockchain

When no update has occurred yet, `blockchain_update_time` is `"None"`.

`broadcaster` names where funding transactions are sent: the configured `interface_type` (`woc`, `uaas`, `rpc`, `test`) or `mapi-lite` when the [`[mapi_lite]`](Configuration.md#mapi_lite) section is present.

`fund_worst_case_seconds` is the longest a single-transaction `POST /fund` can take, so a caller can set its own timeout above it (CS-624): a chain read already running for the client, which the request waits behind, then its own; the broadcast; and the chain read after a commit is refused. Chain reads are bounded by `blockchain_interface.timeout_seconds` (default 30s) and the broadcast by `mapi_lite.total_timeout_seconds` (default 45s), or by one more interface call without `[mapi_lite]`: 135s with mapi-lite at the defaults, 120s without. It is `null` when chain reads have no deadline (`timeout_seconds = 0`). It does not cover requests queued behind a chain read that keeps failing, each of which waits its turn, nor a `multiple_tx` request, which repeats the broadcast and its reads per transaction.

## Fund transactions

`POST /fund`

Creates one or more funding transactions. Request body (JSON):

| Field | Type | Description |
|-------|------|-------------|
| `client_id` | string | Client whose wallet funds the transaction |
| `satoshi` | number | Value in satoshis for each funding output |
| `no_of_outpoints` | number | Number of funding outpoints to provide, at most **1000**. The locking scripts across all of them may total at most **1 MiB** (SR-SEC-017) |
| `multiple_tx` | boolean | If true and `no_of_outpoints` > 1, create separate transactions |
| `locking_script` | string | Hex-encoded locking script for the funding outputs |

Requires the `client_id` client's `api_key` when configured.

```bash
curl -H "Authorization: Bearer your-client-api-key" \
     -H "Content-Type: application/json" \
     --request POST \
     --data '{"client_id":"client1","satoshi":123,"no_of_outpoints":1,"multiple_tx":false,"locking_script":"76a914ddc574807c3035ab43553a22c0b9df1f55737fae88ac"}' \
     http://127.0.0.1:8080/fund
```

```json
{
    "outpoints": [{
        "hash": "11e1128551854896dba1af5ebd75f7fb712ae88684cae59e86f89b158de86697",
        "index": 1,
        "satoshi": 123,
        "locking_script": "76a914ddc574807c3035ab43553a22c0b9df1f55737fae88ac"
    }],
    "txs": [{"tx": "0100000001..."}]
}
```

`satoshi` and `locking_script` describe the output this outpoint refers to. **They are read from the transaction that was broadcast, not echoed from the request**, so a client can verify what was actually paid rather than assume the request was honoured.

That matters when spending the outpoint: a BSV (BIP-143) signature commits to both the previous output's value and its locking script, so if either differed from what the client assumed, the signature would not verify and the node would reject the spending transaction with a script error that says nothing about the funding value being wrong.

**Read `index` rather than assuming it.** A funding transaction normally pays change back at output 0, which puts the first funded output at 1 — as above. When the change would be worth less than the dust threshold it is not paid back at all (see [`[fees]`](Configuration.md#dust-change-goes-to-the-fee)), and the funded outputs start at **0** instead. Funding the reported `max_fundable` always takes that form, because it spends the client out exactly. A caller that hardcodes 1 will reference an output that does not exist.
### Locking scripts per outpoint

`POST /fund` accepts the locking script in either of two forms. Exactly one is required.

**One script for every outpoint** — the original form:

```json
{"client_id": "client1", "satoshi": 123, "no_of_outpoints": 3,
 "multiple_tx": false, "locking_script": "76a914...88ac"}
```

All three outputs pay the same script, so all three outpoints are spendable by the same key. That is only what you want when a single owner takes all of them.

**One script per outpoint** — for independently spendable outputs:

```json
{"client_id": "client1", "satoshi": 123, "multiple_tx": false,
 "locking_scripts": ["76a914aa...88ac", "76a914bb...88ac", "76a914cc...88ac"]}
```

The number of outpoints comes from the list length. `no_of_outpoints` may be omitted; if given it must agree with the length, and a mismatch is rejected rather than one silently winning.

This matters when each funded output is spent by a different transaction. Outputs sharing one key are each spendable by the other's input, so one becomes a double-spend of the other. With `locking_scripts` a single funding transaction can serve N independent spends, instead of needing N separate `POST /fund` calls and N funding transactions on chain.

With `multiple_tx: true` the scripts are paid one per transaction rather than one per output of a single transaction.

Rejected as `invalid_request`: supplying both fields, supplying neither, an empty `locking_scripts` list, a `no_of_outpoints` that disagrees with the list length, more than 1000 outpoints, and locking scripts totalling more than 1 MiB (a single `locking_script` counts once per outpoint). The client and its API key are checked first, so an unknown or unauthenticated caller is refused before any of this is looked at (CS-519).

### Idempotency

`POST /fund` broadcasts a funding transaction and then returns the outpoint in the response body. If that response is lost — a client timeout, a dropped connection — the transaction is already on chain and the client has no record of the outpoint. A plain retry funds a **second** time, and the first output is stranded: locked to a key the client may no longer be able to name.

Send an optional `idempotency_key` to prevent that:

```json
{"client_id": "client1", "satoshi": 123, "no_of_outpoints": 1, "multiple_tx": false,
 "locking_script": "76a914...88ac", "idempotency_key": "any-unique-string"}
```

The first call reserves the key and, once the transaction is broadcast, retains the response against it. A retry carrying the same key replays that response without funding again. Omitting the field preserves the previous behaviour exactly.

| Situation | Result |
|---|---|
| New key | Funds normally, and the response is retained |
| Same key, same request, already funded | Replays the first response with `"replayed": true`; nothing is broadcast |
| Same key, still being processed | `409` `key_in_progress` — retry shortly |
| Same key, materially different request | `409` `idempotency_key_reused` — use a fresh `idempotency_key` |
| Refused before anything was built (`insufficient_balance`, `no_suitable_utxo`, `funds_in_flight`), or the broadcast certainly did not land (`broadcast_rejected`, `broadcast_failed`) | The key is freed, so it can be retried |

**A replayed response is marked.** It carries `"replayed": true`; an ordinary funding response omits the field entirely, so read its absence as false.

```json
{"outpoints": [...], "txs": [...], "replayed": true}
```

This matters because a replay is otherwise indistinguishable from a fresh funding: same status, same shape, same outpoints. If you reuse a key by mistake — meaning "fund me again" rather than "tell me how that went" — you are handed outpoints you were already given, and probably already spent. Signing over a spent output fails at broadcast with a script error that says nothing about why. **Treat an unexpected `"replayed": true` as an error in your own key generation, and fail on it immediately.**

A partly-successful `multiple_tx` batch is retained too, and a retry is answered the same way as the first call — the transactions that reached the chain are reported rather than being funded a second time.

Keys are scoped per `client_id`, so two clients may choose the same string without colliding.

**Two limits worth knowing:**

* **Records are held in memory and are lost when the service restarts.** A retry that spans a restart can still produce a second funding transaction. Retention is bounded by `[idempotency] ttl_seconds` (default 600) and `max_entries` (default 10000); see [Configuration](Configuration.md).
* **After a failure that may have spent something, the key is not freed** — only the outcomes listed above free it. Any other failure leaves the key reserved until its TTL expires, and a retry with it returns `key_in_progress`. This is deliberate: a broadcast may have reached the node before the connection dropped, so releasing the key could cause the duplicate this mechanism exists to prevent. Use a fresh `idempotency_key` if you need to retry sooner.

### Error responses

Every error response carries a machine-readable `code` alongside the human-readable `description`:

```json
{
    "code": "insufficient_balance",
    "description": "Insufficient client balance: 900 satoshi available, 873 required."
}
```

**The `code` is the contract; the `description` is prose for humans and may be reworded at any time.** Clients should branch on `code` and log `description`. Treat an unrecognised code the way you would have treated the response before codes existed — the set may grow.

The status is derived from the code, so a caller that cannot read the body — a proxy, a gateway, a retry policy, an alerting rule — can still classify the failure: **5xx is worth retrying unchanged, 4xx needs the caller or an operator to change something.** `partial_broadcast` is the sole exception to that reading: it is a 422 precisely because the body must be read.

| `code` | HTTP | Meaning | Caller action |
|---|---|---|---|
| `insufficient_balance` | 409 | The balance cannot cover the request once fees are paid, however the UTXOs were arranged | Top the wallet up; the description gives the ceiling |
| `no_suitable_utxo` | 409 | The balance would cover it, but not split the way it is — spending the extra UTXOs costs more in fees than they add | Consolidate the wallet's UTXOs; the description gives both limits |
| `funds_in_flight` | 503 | Every UTXO that could fund the request is claimed by funding requests still being broadcast, and the change they return will cover it | **Retry unchanged** after `Retry-After` (1 second) — see below |
| `unknown_client` | 404 / 400 | No such `client_id`. 404 where the id is a path segment, 400 where it is a body field. Only while no client has an API key: once any does, an unknown `client_id` is `401 unauthorized`, the same answer as a missing or wrong key, so the id of a client with a key cannot be discovered without that key; the service log names the unknown id at `info` (CS-661) | Fix configuration; never retryable as-is |
| `client_exists` | 409 | `client_id` is already configured | `POST /client` only |
| `invalid_request` | 400 | The request is malformed | Fix the request; never retryable unchanged |
| `broadcast_failed` | 502 | The transaction certainly did not reach a node: the upstream could not be reached, answered that no node can have received it, or refused it in a way it said was worth retrying. Anything that may have landed is `broadcast_outcome_unknown` instead (CS-501) | Nothing was spent; retry may succeed, with the same `idempotency_key` |
| `broadcast_rejected` | 409 | The upstream looked at the transaction and refused it finally — either because it said so, or because it named a conflicting transaction | Nothing was spent, and **retrying will not help** — see below |
| `broadcast_outcome_unknown` | 504 | The transaction was handed over and its fate is unknown — it may be on the network | **Do not retry with a new `idempotency_key`**; see below |
| `broadcaster_untrusted` | 503 | Nothing was sent: the broadcaster (mapi-lite) signs with a key `[mapi_lite] miner_id_public_keys` does not list, so its answers cannot be believed (CS-618). `GET /ready` reports the same | Nothing was spent. Retrying cannot help until an operator fixes the key list or mapi-lite's key |
| `partial_broadcast` | 422 | Some of the requested transactions broadcast, some did not | **Read the body** — the successful ones are in it |
| `chain_unavailable` | 503 | The blockchain interface could not be reached. The description is fixed; the read's own error, which names the interface's address, is in the service log | Retryable |
| `internal` | 500 | Unexpected internal failure | Report it |
| `unauthorized` | 401 | Authentication missing or invalid | Fix credentials |
| `rate_limited` | 429 | Request rate exceeded, or (from `POST /fund`) the client's `funding_limits.max_satoshis_per_period` would be exceeded | Retry after the interval in `description` or `Retry-After` |
| `key_in_progress` | 409 | A request with this `idempotency_key` is still being processed | Retry shortly |
| `idempotency_key_reused` | 409 | This `idempotency_key` was used with a different request | Use a fresh `idempotency_key` |

### When the funds are in flight

Under concurrent load for one client, each request claims the UTXOs it spends while it plans, so no two in-flight requests can spend the same input. A request that arrives while every UTXO it could use is claimed by requests still broadcasting has nothing to spend *now* — but nothing is wrong with the wallet. It is refused with `funds_in_flight`, before anything reaches the network, with HTTP 503 and `Retry-After: 1`.

The service answers it only when the request **will** fund once the claims ahead of it settle. It counts, for each claim, the change it returns: a broadcast that succeeds returns that change, and one that is refused returns its whole input, which is more. So a retry succeeds unless new requests take the funds first. A client funded from a single UTXO, chaining through its own change, funds one request at a time and sees this code for the rest.

A request the wallet could not cover even then gets `insufficient_balance` or `no_suitable_utxo`, judged as the wallet will stand once the claims settle — those still mean an operator has to act. A funding transaction whose outcome is unknown does not count as in flight: it holds its inputs for the whole reservation window, so a retry a second later would not succeed.

The `idempotency_key` is released, so the retry can reuse it.

### When the transaction is refused

`broadcast_rejected` means the upstream answered, looked at the funding transaction, and will not take it however many times it is offered. That is either `failureRetryable: false` in mapi-lite's terms, or a rejection that **names a conflicting transaction**.

A conflict is treated as final whatever the upstream says about retrying. mapi-lite reports `txn-mempool-conflict` with `failureRetryable: true`, but the conflict is in the transaction's *inputs*: another transaction already holds them, and offering the identical transaction again cannot succeed. Callers were previously told "retry may succeed" about a double spend that never would. A rejection naming no conflict keeps the upstream's own verdict.

Nothing was spent, so the `idempotency_key` is released and can be used again for the corrected request. But the same transaction will never be accepted, so something has to change first — usually the wallet's UTXO set, usually by an operator. The upstream's own reason, and its `retryable` verdict, are in the service log; they are not in the response, which carries a fixed description like every other error here.

This is 409 rather than 502 because nothing upstream failed. The service built a transaction, the upstream read it and said no, and what needs to change is the state they were both working on — the same shape as `insufficient_balance` and `no_suitable_utxo`.

A refusal the upstream *does* think is worth retrying stays `broadcast_failed`, with its documented "retry may succeed" advice intact.

### When the outcome is unknown

`broadcast_outcome_unknown` is not a failure. It means the funding transaction was handed to the broadcaster and the service could not learn what happened to it — the mapi-lite submit deadline expired with the request in flight, or the answer came back unreadable. The transaction may well be on the network.

Two things follow, and both are deliberate:

* **The service treats the inputs as spent.** They are removed from the client's UTXO cache and reserved, so no later refresh offers them to the next funding request while the transaction may be live. Otherwise the service would build a second transaction spending the same inputs, which the network can only treat as a conflict. If the transaction turns out never to have landed, the reservation expires and the funds return on the next refresh — briefly idle funds are the cheaper mistake.
* **The idempotency record is kept.** Retrying with the original `idempotency_key` is answered `key_in_progress` until the record expires, rather than funding again. That is the honest answer: the service does not know the outcome either.

A caller that needs to know what happened should look at the chain for the transaction, not ask the service again. Retrying with a **new** `idempotency_key` will fund a second time, which is exactly what the first request may already have done.

Examples:

* Unauthorized — `{"code": "unauthorized", "description": "Unauthorized"}` (missing or invalid `api_key` for the client)
* Unknown client — `{"code": "unknown_client", "description": "Unknown client_id client1"}`, only while no client has an API key; otherwise the Unauthorized answer above (CS-661)
* Insufficient total balance — `{"code": "insufficient_balance", "description": "Insufficient client balance: 480 satoshi requested, but of the 1230 satoshi available at most 479 can be paid out once fees are covered."}`
* No suitable UTXO set — `{"code": "no_suitable_utxo", "description": "Unable to fund 470 satoshi from this UTXO set: at most 379 can be paid out, because spending more of these 7 UTXOs costs more in fees than the inputs are worth. The balance of 1230 satoshi would support up to 479 if it were consolidated into one UTXO."}`

  The two are a pair. `insufficient_balance` means no arrangement of this balance is enough; `no_suitable_utxo` means this arrangement is not, and says what a consolidated one would allow. Both quote a number the caller can act on, and `GET /balance` reports the same limit as `max_fundable` before a request is made.
* Invalid input — `{"code": "invalid_request", "description": "Invalid satoshi value '0'"}`
* Partial `multiple_tx` failure — HTTP 422 with successful transactions included when some broadcasts succeed before a later failure:

```json
{
    "code": "partial_broadcast",
    "description": "Failed to broadcast funding transaction 2 of 3: Failed to broadcast funding transaction. 1 transaction(s) were broadcast successfully: 11e11285...",
    "outpoints": [{"hash": "11e1128551854896dba1af5ebd75f7fb712ae88684cae59e86f89b158de86697", "index": 1, "satoshi": 123, "locking_script": "76a914ddc574807c3035ab43553a22c0b9df1f55737fae88ac"}],
    "txs": [{"tx": "0100000001..."}]
}
```

Funding combines multiple UTXOs when no single input is large enough, as long as total wallet balance covers outputs and fees.

## Add client

`POST /client`

Add a client at runtime. The client is persisted to the dynamic config file. Requires `web_interface.admin_api_key` when configured.

Request body (JSON):

| Field | Type | Description |
|-------|------|-------------|
| `client_id` | string | Identifier for the new client |
| `wif` | string | WIF private key for the client's funding wallet (use `wif` or `wif_env`). Not an `env:` reference. Refused with HTTP 400 unless `dynamic_config.allow_plaintext_wif` is set, since the file would then hold the key itself |
| `wif_env` | string | Environment variable name containing the WIF; stored as `env:VAR` in dynamic config. Must start with `dynamic_config.wif_env_prefix` (default `FS_RUNTIME_WIF_`) |
| `api_key` | string | Optional shared secret for this client's API endpoints |
| `api_key_env` | string | Environment variable name containing the client API key; stored as `env:VAR` in dynamic config |

Provide either `wif` or `wif_env`, and either `api_key` or `api_key_env` (not both pairs).

Refused as `invalid_request` (CS-521): a `wif_env` outside the prefix, a `wif` that is an `env:` reference, and a key that is already another client's funding key.

```bash
curl -H "Authorization: Bearer your-admin-api-key" \
     -H "Content-Type: application/json" \
     --request POST \
     --data '{"client_id":"client15","wif_env":"FS_RUNTIME_WIF_CLIENT15","api_key_env":"FS_RUNTIME_API_KEY_CLIENT15"}' \
     http://127.0.0.1:8080/client
```

```json
{"status": "Success"}
```

Common error responses:

* Unauthorized — `{"code": "unauthorized", "description": "Unauthorized"}` (missing or invalid admin key)

## Delete client

`DELETE /client/{client_id}`

Remove a dynamically added client. Requires the target client's `api_key` when configured.

```bash
curl -H "Authorization: Bearer your-client-api-key" \
     -X DELETE http://127.0.0.1:8080/client/client1
```

```json
{"status": "Success"}
```

## Get address

`GET /client/{client_id}/address`

Return the funding address for a client. Requires the target client's `api_key` when configured.

```bash
curl -H "Authorization: Bearer your-client-api-key" \
     http://127.0.0.1:8080/client/client1/address
```

```json
{
    "address": "mfxjfLTXLUcCxMDojqRejpfKnF9WhRG5BK"
}
```

## Client balance

`GET /client/{client_id}/balance`

Return the satoshi balance for a client. Requires the target client's `api_key` when configured.

The service refreshes balance and UTXO state from the blockchain before responding when its cached state is older than `service.chain_state_max_age_seconds` (by default the refresh period); otherwise it answers from the cache. See [How often the service reads the chain](Configuration.md#how-often-the-service-reads-the-chain).

```bash
curl -H "Authorization: Bearer your-client-api-key" \
     http://127.0.0.1:8080/client/client1/balance
```

```json
{
    "confirmed": 99904,
    "unconfirmed": 95162,
    "max_fundable": 98904
}
```

**Unconfirmed outputs are spendable.** `POST /fund` draws on the whole unspent set, so `max_fundable` counts `unconfirmed` as well as `confirmed`, and a funding request can be satisfied entirely from outputs that are still in the mempool. That is deliberate — a funding service that waited for confirmation would be unusable on a chain where its own change output is the next input — but it does mean a funded outpoint can depend on a parent transaction that has not been mined. Callers that need confirmed inputs specifically should check `confirmed` before requesting.

If such a parent never makes it — a conflict, an eviction or a reorg drops it — the node refuses the next spend of its change as missing inputs. That request fails (`broadcast_failed` or `broadcast_rejected`, as the upstream judged it retryable or not), and the service forgets the change (and any change built on it) instead of offering it again, so the request after it funds from another input (CS-506).

Both figures are **derived from the unspent set**, not asked for separately. A separate balance query can disagree with the UTXOs it is meant to describe — WhatsOnChain's `/address/{address}/balance` is deprecated and was reporting `unconfirmed: 0` for addresses whose unspent set plainly held unconfirmed outputs — and when they disagree, the unspent set is the one that decides what can be funded. Deriving means `confirmed`, `unconfirmed` and `max_fundable` always describe the same set of coins, and that the balance reflects a funding transaction the moment it spends from them rather than at the next refresh.

`max_fundable` is **the most a single `POST /fund` can ask for right now**, assuming one standard P2PKH outpoint. Use it rather than deriving a figure from the balance.

It is not `confirmed + unconfirmed` minus a fee you can guess. Fees come out of the same UTXOs, and every input the transaction has to spend adds bytes and so adds fee — so a wallet holding its balance in many small pieces can fund far less than one holding the same total in a single piece. Where the difference is large, the UTXOs are fragmented: a request between `max_fundable` and what the balance suggests is refused with `no_suitable_utxo`, whose description says what consolidating would raise the limit to.
