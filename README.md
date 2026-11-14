# Financing Service - Rust

The Financing Service (FS) creates Bitcoin SV transaction outpoint(s) of the correct satoshi value to fund client transactions, on request by a client application.

The FS is a component that can be used in different applications. Initially these will be Research applications. The component should be flexible, robust, clearly documented and maintainable, so that it is capable of supporting nChain release products.

The initial concept of this service was captured in the document:
https://docs.google.com/document/d/159T_RDgf8CnSq3Kd4PaYgfw9OUrX-kwwdZw4qEe4iP0/edit?usp=sharing


The FS is designed to be as simple as possible, in that light it:

* One FS can serve multiple clients.
* FS provides REST API for clients to interact with.
* FS supports the dynamic addition and removal of clients from the system (via REST API).

* FS can provide any satoshi amount (subject to sufficient funds).
* FS can provide any number of outpoints, in any number of transactions.

* FS only accesses the funding key required to sign the transaction that provides the funds the client wishes to spend.
* FS does not access the client's key, instead the client provides the locking script (script_pubkey).

* FS accesses the blockchain through WhatsOnChain, mapi-lite, UTXO as a Service (UaaS), or a node's JSON-RPC endpoint. WhatsOnChain and UaaS reach mainnet, testnet and STN, and mapi-lite mainnet and testnet; the `rpc` interface additionally reaches regtest, and works against any node you control. See [which interface reaches which network](docs/Configuration.md#which-interface-reaches-which-network).
* FS can optionally broadcast funding transactions through [mapi-lite](https://github.com/teranode-group/mapi-rs) instead of the blockchain interface. Configure a `[mapi_lite]` section and transactions go to mapi-lite while chain reads stay where they were, unless `interface_type = "mapi_lite"` sends them there too, leaving mapi-lite the deployment's only WhatsOnChain client; leave it out and nothing changes. When configured, mapi-lite's own readiness is part of `GET /ready`, the readiness probe, alongside chain-read health; `GET /health` stays a pure liveness check. See [Configuration](docs/Configuration.md#mapi_lite). Building with this integration needs SSH access to the private mapi-lite repository — see [Dependencies](docs/Dependencies.md).
* FS maintains an in-memory UTXO cache per client, refreshed periodically from the blockchain, and again by a funding or balance request that finds it older than `service.chain_state_max_age_seconds`. Clients added at runtime are persisted to a dynamic config file.
* FS is configurable; it reads its configuration on startup.

* FS does not cache funding transactions for client applications. That is a task better performed by the requesting application, which has much better oversight to determine how many funding transactions to cache, and when to request them.

* FS is written in Rust.
* FS build dependencies are open-source Rust crates, except `uls-client` and `uls-core`, which come from the private mapi-lite repository (see [Dependencies](docs/Dependencies.md)).

* FS does not support Hierarchical Deterministic (HD) Keys (BIP-32).
* What FS must agree on with Event and mapi-lite (network, miner ID, mapi-lite token, its clients' keys, fee rate, the timeout chain), who owns each, and how a mismatch shows is in [What must agree](docs/what-must-agree.md).
* FS supports optional per-client API key authentication and an optional admin key for `POST /client`. Clients without an `api_key` rely on network isolation (firewalls, private networks, reverse proxies). See [Configuration](docs/Configuration.md) and [Supported endpoints](docs/SupportedEndpoints.md).
* FS supports secret references (`env:VAR`), environment variable overrides, and `wif_env` / `api_key_env` when adding clients at runtime. See [Configuration](docs/Configuration.md#secret-management).
* FS supports optional per-IP HTTP rate limiting (disabled by default). See [Configuration](docs/Configuration.md#rate-limiting).
* FS supports optional OpenTelemetry trace export via OTLP (disabled by default). Each HTTP request creates a span; traces include resource metadata and request fields but not secrets or request bodies. See [Configuration](docs/Configuration.md#telemetry).
* FS can also push metrics over OTLP (disabled by default; `OTEL_METRICS_EXPORTER=otlp`): requests by route and status, request duration, readiness, `/fund` outcomes, and each client's balance and chain-state age. See [Metrics](docs/Configuration.md#metrics).

## Use cases

![Diagram 1](docs/diagrams/use-case.png)

Diagram 1 - Financing Service Use Cases

The Financing Service Client use cases are:
* `Request Transaction Fund` - the FS receives a request for a satoshi value, it creates a funding transaction and provides the outpoint to the requestor so that they can fund their transaction.
* `Request Transaction Funds` - the FS receives a request for multiple outpoints for  a satoshi value, it creates a funding transaction and returns the outpoints.
* `Get Balance` - the FS returns the current level of funding associated with a particular client.
* `Get Address` - the FS returns the address of particular client, this can be used for providing additional funds.

The Financing Service Admin use cases are:
* `Get Status` - the FS will return the current status of the component.
* `Health Check` - the FS exposes a liveness endpoint (`GET /health`) for container deployments, which checks no upstream, and a readiness endpoint (`GET /ready`), which reports chain-read health and, when mapi-lite is configured, mapi-lite's own readiness.
* `Add Client` - Dynamically add the client while the service is running.
* `Delete Client` - Dynamically delete the client while the service is running.
* `Top-up Balance` - The Admin will provide a funding transaction to increase the satoshi that the FS can use for funding. This is done outside the Financing Service.


## Overview

![Diagram 2](docs/diagrams/overview.png)

Diagram 2 - Financing Service Overview

As shown in diagram 2 the FS provides an interface that the other application components interface with and uses the blockchain to create the funding transaction outpoints.

The service reads its configuration on startup, maintains per-client wallet state in memory, and persists runtime-added clients to disk.

Before returning a balance or building a funding transaction, the service refreshes UTXO state from the blockchain if its cached state is older than `service.chain_state_max_age_seconds` (with `[mapi_lite]`, a funding request uses state that has merely aged and the refresh runs behind it). Concurrent fund requests for the same client each claim the inputs they select under an exclusive per-client lock, so no two spend the same input, and broadcast outside it; a claim is committed only after its transaction is broadcast successfully.

The service uses the `chain-gang` library to interact with the BSV blockchain via a configurable interface: WhatsOnChain (WoC) by default in sample config, or optionally UTXO as a Service (UaaS) or a node's JSON-RPC endpoint (`rpc`).


## Getting Started

The project can either be run as an executable or as a docker container (smallish 100MB).


## Docker
Encapsulating the service in Docker removes the need to install the project dependencies on the host machine.
Only Docker is required to build and run the service.
### 1) Build The Docker Image
To build the docker image associated with the service run the following comand in the project directory.
```bash
./build.sh
```
This builds the Docker image `financing-service-rust`. The image includes a health check against `GET /health`, the liveness endpoint.

The image is built with `--locked` from the committed `Cargo.lock`, from base images pinned by digest, and runs as an unprivileged user (`fs`, uid 10001). `.dockerignore` keeps `.git`, `target/`, the runtime files in `data/` — which can hold client WIFs — and, at whatever depth, any `certs` directory and `*.pem`, `*.key` or `.env` file out of the build context.
### 2) To Run the Image
To start the Docker container:
```bash
./run.sh
```
This will provide a REST API at http://localhost:8080, on the host's loopback only. Set `FS_PUBLISH_ADDRESS` (for example `FS_PUBLISH_ADDRESS=0.0.0.0 ./run.sh`) to publish it more widely — and put the funding endpoints behind something that authenticates when you do.

`run.sh` gives `docker stop` 150s (`FS_STOP_TIMEOUT`) before it kills the container, so a funding in progress can finish (CS-739). It mounts `./data` as the data directory and runs the container as you, so it can write the dynamic config file there. It mounts nothing else from the host. Elsewhere, give the image a volume at `/app/bin/data` that uid 10001 can write, or run it with `--user` as `run.sh` does.


## To Build the Service
The service is developed in Rust.

The best way to install Rust is to use `rustup`, see https://www.rust-lang.org/tools/install

To build:
```bash
cargo build
```

## To Run the Service
To run locally:

```bash
cargo run
```

Run tests, formatting, and lint checks:

```bash
cargo test
cargo fmt --all -- --check
cargo clippy -- -D warnings
```

These checks also run in GitHub Actions on push and pull request.

## Building and Publishing Docker Images

This project creates the Docker image: `financing-service` which is published to Docker Hub.  This makes it accessible for use by other projects and applications within the ecosystem.

To build and publish the image to Docker Hub, run the following command:
```
./multi-build.sh
```

The tag is `v` plus the `version` in `Cargo.toml`. The script refuses to publish from a working tree with uncommitted changes, and refuses to push a tag that is already published: an image someone pulled by tag stays the image that tag names. Bump the version to publish again. `BUILDER=cloud-nchain-rndprototyping ./multi-build.sh` uses the cloud builder.

**Requirements**

- **Docker Buildx:** The script requires Docker's Buildx extension to be set as the active builder. Ensure Buildx is properly installed and selected as the current Docker engine. For help, see [Docker Buildx](https://docs.docker.com/build/builders/)  

- **Publishing Permissions:** Only members of the `rndprototyping` team within the `nChain` Docker Hub organisation are authorised to publish images with the appropriate tags. Ensure you are logged in with the necessary permissions before running the script, else this will fail.


## Supported endpoints
For details of the REST API endpoints provided by this service see [here](docs/SupportedEndpoints.md)

## Configuration
For service and client configuration, including per-client `api_key` authentication and optional OpenTelemetry, see [here](docs/Configuration.md)

## Project status
For architecture, implementation status, and known limitations, see [Project.md](docs/Project.md)

## System requirements
For formal requirements and how each is verified (automated tests, CI, manual checks), see [SystemRequirements.md](docs/SystemRequirements.md)

## Locking scripts
For details on generating locking scripts for the `fund` call see [here](docs/LockingScripts.md)

