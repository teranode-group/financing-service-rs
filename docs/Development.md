# Development

This document contains information useful in installing build tools, building, maintaining and extending this project.

Many of the elements of this service are common with the Financing Service written in Python which can be found under

https://bitbucket.stressedsharks.com/projects/SDL/repos/financing-service

An overview of the project can be found:
https://docs.google.com/document/d/159T_RDgf8CnSq3Kd4PaYgfw9OUrX-kwwdZw4qEe4iP0/edit?usp=sharing


## Rust installation

This project is built using Rust. The best way to install Rust is to use `rustup`.

To determine the current version of Rust run `rustup show`:

```bash
rustup show
```

Once installed, update the Rust toolchain using:

```bash
rustup update
```

## Build and test

```bash
cargo build
cargo test
```

### SSH access to mapi-lite

The optional mapi-lite broadcaster depends on the `uls-client` and `uls-core` crates, which live in the **private** `nchain-innovation/mapi-lite` repository and are pinned in `Cargo.toml` as `git+ssh` dependencies. Building therefore needs an SSH key with read access to that repository loaded in your agent:

```bash
eval "$(ssh-agent)" && ssh-add ~/.ssh/<your-key>
ssh -T git@github.com     # should greet you by name
cargo build
```

`.cargo/config.toml` sets `net.git-fetch-with-cli = true`, so cargo hands the fetch to your `git`, which uses the agent and `~/.ssh/config` as usual. Nothing else in the dependency tree needs credentials. Details, including how to repin and the single-`chain-gang` rule, are in [Dependencies.md](Dependencies.md).

### Docker

`./build.sh` runs `docker build` with BuildKit and `--ssh default`, which lends your ssh-agent socket to the one build step that fetches dependencies; the key never enters the image. Have an agent running with the key loaded before building. `multi-build.sh` passes the same flag to `docker buildx build`.

CI builds the image for every pull request and publishes it to `ghcr.io/teranode-group/financing-service-rs` (CS-606), named after the repository: `:sha-<commit>` and `:main` from every push to `main`, and `:X.Y.Z` and `:latest`, for amd64 and arm64, from a `vX.Y.Z` tag. The package's visibility is its own setting, not the repository's (which is public): while the package is private, `docker login ghcr.io` with a token that can read packages before pulling. The job's `packages: write` is the workflow's one write permission (SR-SEC-028). `multi-build.sh` still publishes to Docker Hub (`nchain/innovation-financing-service`), by hand; that repository is public and was last pushed in January 2025 (`v2.1`). Images up to 5.0.1 were published before the repository moved from `nchain-innovation/financing-service`, and stay at `ghcr.io/nchain-innovation/financing-service`.

Moving a base image: both `FROM` lines are pinned by tag **and** digest. Take the index digest from `docker buildx imagetools inspect <image:tag>` and update both parts together; `system_requirements::tests::sr_sec_025_base_images_are_pinned_by_digest` fails on a `FROM` without one.

## Formatting and linting

Check formatting:

```bash
cargo fmt --all -- --check
```

Apply formatting:

```bash
cargo fmt --all
```

Run Clippy with warnings denied:

```bash
cargo clippy -- -D warnings
```

Audit dependencies for known vulnerabilities:

```bash
cargo install cargo-audit --locked
cargo audit
```

These checks run automatically in GitHub Actions (`.github/workflows/rust.yml`) on every push to any branch and on every pull request, whatever it targets.

## Dependencies

`Cargo.lock` is committed for reproducible builds. Run `cargo update` only when intentionally upgrading dependencies, then re-run tests and commit the updated lockfile.

`chain-gang` must resolve to exactly one package in the lockfile — it is referenced both directly and through `uls-core` — and `dependency_graph::tests` fails the build if it does not. See [Dependencies.md](Dependencies.md) for why, and for the order to bump things in.

## Releases and versioning

The version is the `version` in `Cargo.toml`; `GET /status` and the telemetry `service.version` report it. Release notes are in [Releases](Releases), oldest first, with changes not yet released in an `Unreleased` entry at the end. A release is tagged `vX.Y.Z`. An entry that begins `Breaking:` marks a change that is not backwards compatible.

## Source code documentation

Generate source code documentation:

```bash
cargo doc --open
```

This outputs documentation to `./target/doc/financing_service/index.html`.

## Directories

```
├── data
├── docs
│   └── diagrams
├── src
└── target
```

* `data` — configuration used by the service
* `docs` — project documentation
* `docs/diagrams` — PlantUML diagrams and source in support of the documentation
* `src` — service source code in Rust
* `target` — Rust compiler output
