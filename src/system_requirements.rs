//! Automated tests for requirements that are verified at repository or deployment level.

#[cfg(test)]
mod tests {
    use std::path::Path;

    #[test]
    fn sr_nfr_001_ci_runs_cargo_fmt_check() {
        let workflow = std::fs::read_to_string(".github/workflows/rust.yml").unwrap();
        assert!(workflow.contains("cargo fmt --all -- --check"));
    }

    #[test]
    fn sr_nfr_002_ci_runs_cargo_clippy_with_warnings_denied() {
        let workflow = std::fs::read_to_string(".github/workflows/rust.yml").unwrap();
        assert!(workflow.contains("cargo clippy"));
        assert!(workflow.contains("-D warnings"));
    }

    #[test]
    fn sr_nfr_003_ci_runs_cargo_audit() {
        let workflow = std::fs::read_to_string(".github/workflows/rust.yml").unwrap();
        assert!(workflow.contains("cargo audit"));
    }

    #[test]
    fn sr_nfr_004_ci_runs_cargo_test() {
        let workflow = std::fs::read_to_string(".github/workflows/rust.yml").unwrap();
        assert!(workflow.contains("cargo test"));
    }

    #[test]
    fn sr_nfr_005_cargo_lock_is_committed() {
        assert!(Path::new("Cargo.lock").is_file());
    }

    #[test]
    fn sr_nfr_007_main_returns_error_instead_of_panicking_on_config_failure() {
        let source = include_str!("main.rs");
        assert!(source.contains("load_config(&config_path).map_err("));
    }

    /// A warning logged before the subscriber is installed goes nowhere, so
    /// the plaintext-secret warning waits for it.
    #[test]
    fn sr_cfg_006_configuration_warnings_follow_logging_setup() {
        let source = include_str!("main.rs");
        let init = source.find("telemetry::init(").expect("telemetry::init");
        let warn = source
            .find("warn_plaintext_secrets(&file)")
            .expect("the warning");
        assert!(init < warn);
    }

    /// The line an operator reads to learn where funding transactions go.
    #[test]
    fn sr_bchn_010_main_logs_which_broadcaster_is_in_use() {
        let source = include_str!("main.rs");
        assert!(source.contains("describe_broadcaster(&config)"));
    }

    /// uls-client and uls-core share types; two revisions would give two
    /// incompatible sets. Both pins in the manifest must name one commit.
    #[test]
    fn sr_bchn_009_uls_client_and_uls_core_are_pinned_to_one_mapi_lite_revision() {
        let manifest = std::fs::read_to_string("Cargo.toml").unwrap();
        let revs: Vec<&str> = manifest
            .lines()
            .filter(|line| line.starts_with("uls-client") || line.starts_with("uls-core"))
            .map(|line| {
                let start = line.find("rev = \"").expect("a rev pin") + "rev = \"".len();
                let end = line[start..].find('"').expect("closing quote") + start;
                &line[start..end]
            })
            .collect();
        assert_eq!(
            revs.len(),
            2,
            "expected uls-client and uls-core pins: {revs:?}"
        );
        assert_eq!(
            revs[0], revs[1],
            "uls-client and uls-core pin different revs"
        );
        assert!(
            manifest.contains("git = \"ssh://git@github.com/nchain-innovation/mapi-lite.git\""),
            "cargo needs the ssh:// URL form for the private mapi-lite repo"
        );
    }

    /// The private mapi-lite repo is fetched by the system git so the
    /// developer's (or CI's) ssh-agent authenticates it.
    #[test]
    fn sr_nfr_009_cargo_fetches_git_dependencies_with_the_system_git() {
        let cargo_config = std::fs::read_to_string(".cargo/config.toml").unwrap();
        assert!(cargo_config.contains("git-fetch-with-cli = true"));
    }

    #[test]
    fn sr_nfr_009_ci_loads_the_mapi_lite_deploy_key_before_building() {
        let workflow = std::fs::read_to_string(".github/workflows/rust.yml").unwrap();
        assert!(workflow.contains("MAPI_LITE_DEPLOY_KEY"));
        assert!(workflow.contains("webfactory/ssh-agent"));
    }

    #[test]
    fn sr_cfg_003_the_image_listens_on_every_interface() {
        let dockerfile = std::fs::read_to_string("Dockerfile").unwrap();
        assert!(dockerfile.contains("ENV FS__LISTEN_ADDR=0.0.0.0:8080"));
        assert!(!dockerfile.contains("APP_ENV"));
    }

    #[test]
    fn sr_nfr_009_docker_build_mounts_ssh_for_the_dependency_fetch() {
        let dockerfile = std::fs::read_to_string("Dockerfile").unwrap();
        assert!(dockerfile.contains("--mount=type=ssh"));
        let build = std::fs::read_to_string("build.sh").unwrap();
        assert!(build.contains("--ssh default"));
    }

    #[test]
    fn sr_bchn_009_configuration_and_readme_document_mapi_lite() {
        let configuration = std::fs::read_to_string("docs/Configuration.md").unwrap();
        assert!(configuration.contains("## [mapi_lite]"));
        assert!(configuration.contains("FS__MAPI_LITE__AUTH_TOKEN"));
        let readme = std::fs::read_to_string("README.md").unwrap();
        assert!(readme.contains("Configuration.md#mapi_lite"));
        let endpoints = std::fs::read_to_string("docs/SupportedEndpoints.md").unwrap();
        assert!(endpoints.contains("\"unhealthy\""));
    }

    #[test]
    fn sr_cfg_005_dockerfile_healthcheck_calls_health_endpoint() {
        let dockerfile = std::fs::read_to_string("Dockerfile").unwrap();
        assert!(dockerfile.contains("HEALTHCHECK"));
        assert!(dockerfile.contains("/health"));
    }

    #[test]
    fn sr_lim_001_hd_wallet_support_is_not_implemented() {
        let source = concat!(
            include_str!("client.rs"),
            include_str!("service.rs"),
            include_str!("config.rs"),
        )
        .to_ascii_lowercase();
        assert!(!source.contains("bip32"));
        assert!(!source.contains("hd_wallet"));
        assert!(!source.contains("hierarchical deterministic"));
    }

    #[test]
    fn sr_lim_002_https_is_not_provided_by_the_service_binary() {
        let manifest = std::fs::read_to_string("Cargo.toml").unwrap();
        assert!(!manifest.contains("rustls"));
        assert!(!manifest.contains("native-tls"));
        let main_source = include_str!("main.rs");
        assert!(!main_source.contains("rustls"));
    }

    #[test]
    fn sr_lim_005_same_client_concurrency_is_covered_by_service_tests() {
        let source = include_str!("service.rs");
        assert!(source.contains("concurrent_fund_requests_for_same_client_do_not_block"));
        assert!(source.contains("cs_473_contention_for_one_utxo_is_refused_rather_than_conflicted"));
    }

    #[test]
    fn sr_tele_002_main_wraps_tracing_logger() {
        let source = include_str!("main.rs");
        assert!(source.contains("tracing_actix_web::TracingLogger"));
        assert!(source.contains("telemetry::init"));
    }

    #[test]
    fn sr_tele_004_telemetry_sets_service_resource_attributes() {
        let source = include_str!("telemetry.rs");
        assert!(source.contains("service.name"));
        assert!(source.contains("service.version"));
        assert!(source.contains("CARGO_PKG_VERSION"));
    }

    #[test]
    fn sr_tele_005_readme_and_configuration_document_opentelemetry() {
        let readme = std::fs::read_to_string("README.md").unwrap();
        assert!(readme.contains("OpenTelemetry"));
        assert!(readme.contains("Configuration.md#telemetry"));

        let configuration = std::fs::read_to_string("docs/Configuration.md").unwrap();
        assert!(configuration.contains("## [telemetry]"));
        assert!(configuration.contains("OTEL_EXPORTER_OTLP_ENDPOINT"));
    }

    #[test]
    fn sr_tele_011_configuration_documents_every_metric() {
        let configuration = std::fs::read_to_string("docs/Configuration.md").unwrap();
        assert!(configuration.contains("## Metrics"));
        assert!(configuration.contains("OTEL_METRICS_EXPORTER"));
        assert!(configuration.contains("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"));
        for metric in [
            "`http_requests`",
            "`http_request_duration`",
            "`ready`",
            "`fs_fund_requests`",
            "`fs_client_balance`",
            "`fs_client_chain_state_age`",
            "http_requests_total",
            "http_request_duration_seconds",
            "fs_client_balance_sat",
            "ready{check=\"overall\"} == 0",
        ] {
            assert!(configuration.contains(metric), "{metric} is not documented");
        }
        let source = include_str!("metrics.rs");
        for name in [
            "\"http_requests\"",
            "\"http_request_duration\"",
            "\"ready\"",
            "\"fs_fund_requests\"",
            "\"fs_client_balance\"",
            "\"fs_client_chain_state_age\"",
        ] {
            assert!(source.contains(name), "{name} is not an instrument");
        }
    }

    /// The ticket's first point was that the documentation never said whether
    /// unconfirmed outputs could be used as funds, so a reader had to guess --
    /// and guessed wrong. The behaviour is pinned by a test in `client`; this
    /// pins that it is also written down, which is the half a caller can see.
    #[test]
    fn sr_fund_019_endpoints_document_unconfirmed_eligibility() {
        let endpoints = std::fs::read_to_string("docs/SupportedEndpoints.md").unwrap();
        assert!(
            endpoints.contains("Unconfirmed outputs are spendable"),
            "the balance endpoint must state whether unconfirmed funds can be spent"
        );
        assert!(
            endpoints.contains("derived from the unspent set"),
            "and that the figures come from the unspent set rather than a separate query"
        );
    }

    #[test]
    fn sr_lim_006_opentelemetry_exports_traces_and_metrics_not_logs() {
        let manifest = std::fs::read_to_string("Cargo.toml").unwrap();
        assert!(manifest.contains(
            "opentelemetry-otlp = { version = \"0.32\", features = [\"grpc-tonic\", \"trace\", \"metrics\"] }"
        ));
        assert!(!manifest.contains("\"logs\""));
        let telemetry = include_str!("telemetry.rs");
        assert!(telemetry.contains("SpanExporter"));
        assert!(telemetry.contains("MetricExporter"));
        assert!(!telemetry.contains("LogExporter"));
        let main_source = include_str!("main.rs");
        assert!(!main_source.contains("\"/metrics\""));
    }

    // --- container and image (CS-527) ---

    /// Lines of a shell script or Dockerfile that are not comments.
    fn instructions(path: &str) -> Vec<String> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(str::to_string)
            .collect()
    }

    /// The runtime stage is the part after the last `FROM`.
    fn runtime_stage() -> Vec<String> {
        let lines = instructions("Dockerfile");
        let last_from = lines.iter().rposition(|l| l.starts_with("FROM ")).unwrap();
        lines[last_from..].to_vec()
    }

    #[test]
    fn sr_sec_024_the_image_runs_as_an_unprivileged_user() {
        let stage = runtime_stage();
        let user = stage
            .iter()
            .rev()
            .find(|l| l.starts_with("USER "))
            .expect("the runtime stage must set USER");
        let who = user.trim_start_matches("USER ").trim();
        assert!(
            who != "root" && who != "0" && !who.starts_with("0:"),
            "{user}"
        );
    }

    #[test]
    fn sr_sec_024_run_sh_mounts_no_docker_socket_and_publishes_on_loopback() {
        let run = instructions("run.sh").join("\n");
        assert!(
            !run.contains("docker.sock"),
            "run.sh must not mount the Docker socket"
        );
        assert!(
            run.contains("FS_PUBLISH_ADDRESS:-127.0.0.1"),
            "run.sh must publish on loopback unless told otherwise"
        );
        assert!(run.contains("-p \"${PUBLISH_ADDRESS}:8080:8080\""), "{run}");
    }

    #[test]
    fn sr_sec_025_the_image_builds_the_committed_lockfile() {
        let build = instructions("Dockerfile")
            .into_iter()
            .filter(|l| l.contains("cargo install") || l.contains("cargo build"))
            .collect::<Vec<_>>();
        assert!(!build.is_empty(), "no cargo build step found");
        for line in build {
            assert!(line.contains("--locked"), "{line}");
        }
    }

    #[test]
    fn sr_sec_025_base_images_are_pinned_by_digest() {
        let froms: Vec<_> = instructions("Dockerfile")
            .into_iter()
            .filter(|l| l.starts_with("FROM "))
            .collect();
        assert!(froms.len() >= 2, "{froms:?}");
        for from in froms {
            let image = from.split_whitespace().nth(1).unwrap();
            let digest = image.split("@sha256:").nth(1).unwrap_or("");
            assert!(
                digest.len() == 64 && digest.chars().all(|c| c.is_ascii_hexdigit()),
                "not pinned by digest: {from}"
            );
        }
    }

    /// Nothing that can hold a key reaches the build context, and the build
    /// does not copy the data directory in at all.
    #[test]
    fn sr_sec_025_the_build_context_excludes_secrets_and_history() {
        let ignore = instructions(".dockerignore");
        for required in [
            ".git",
            "target",
            "data/dynamic*",
            "data/*.inflight.json",
            "certs",
        ] {
            assert!(
                ignore.iter().any(|l| l == required),
                ".dockerignore must exclude {required}"
            );
        }
        assert!(
            !instructions("Dockerfile")
                .iter()
                .any(|l| l.starts_with("COPY ./data") || l.starts_with("COPY data")),
            "the image build must not copy data/"
        );
    }

    /// A published tag is the version in Cargo.toml and is never overwritten.
    #[test]
    fn sr_sec_026_publishing_takes_its_tag_from_cargo_toml_and_never_overwrites_one() {
        let script = instructions("multi-build.sh").join("\n");
        assert!(
            !script.contains("VERSION=v"),
            "the version must not be hard-coded"
        );
        assert!(
            script.contains("Cargo.toml"),
            "the version comes from Cargo.toml"
        );
        assert!(
            script.contains("imagetools inspect \"$PUBLISH_TAG\""),
            "an existing tag must be detected before pushing"
        );
        let check = script.find("imagetools inspect").unwrap();
        let push = script.find("--push").unwrap();
        assert!(
            check < push,
            "the existing-tag check must come before the push"
        );
        assert!(
            script.contains("git status --porcelain"),
            "a dirty tree must not be published"
        );
    }

    // --- CI workflow (CS-528) ---

    fn workflow() -> String {
        std::fs::read_to_string(".github/workflows/rust.yml").unwrap()
    }

    /// The `uses:` lines, comments stripped.
    fn uses_lines(workflow: &str) -> Vec<String> {
        workflow
            .lines()
            .map(|l| l.split(" #").next().unwrap().trim())
            .filter(|l| l.trim_start_matches("- ").starts_with("uses:"))
            .map(|l| l.trim_start_matches("- ").to_string())
            .collect()
    }

    /// A moved tag could hand the deploy key to other code.
    #[test]
    fn sr_sec_027_every_action_is_pinned_by_commit_sha() {
        let uses = uses_lines(&workflow());
        assert!(uses.len() >= 3, "{uses:?}");
        for line in uses {
            let target = line.trim_start_matches("uses:").trim();
            if target.starts_with("./") {
                continue;
            }
            let sha = target.split('@').nth(1).unwrap_or("");
            assert!(
                sha.len() == 40 && sha.chars().all(|c| c.is_ascii_hexdigit()),
                "not pinned by commit SHA: {line}"
            );
        }
    }

    #[test]
    fn sr_sec_028_the_workflow_token_is_read_only_and_not_persisted() {
        let workflow = workflow();
        assert!(
            workflow.lines().any(|l| l == "permissions: {}"),
            "the workflow must default the token to no permissions"
        );
        assert!(workflow.contains("contents: read"));
        // The one write permission is the image job's `packages: write`, to
        // push to ghcr.io (CS-606); no other job, and nothing else, writes.
        let writes: Vec<&str> = workflow
            .lines()
            .map(|l| l.split(" #").next().unwrap().trim())
            .filter(|l| l.ends_with(": write") || *l == "permissions: write-all")
            .collect();
        assert_eq!(writes, ["packages: write"], "only the image job writes");
        let image = workflow.find("\n  image:\n").expect("the image job");
        let packages = workflow.find("packages: write").unwrap();
        assert!(packages > image, "packages: write belongs to the image job");
        assert!(
            !workflow.contains("persist-credentials: true"),
            "no checkout keeps the token"
        );
        assert!(workflow.contains("persist-credentials: false"));
    }

    /// The image is built and pushed for a version bump only: a `vX.Y.Z`
    /// tag, never a pull request or a push to a branch (CS-606).
    #[test]
    fn sr_sec_028_the_image_job_runs_on_release_tags_only() {
        let workflow = workflow();
        let image = &workflow[workflow.find("\n  image:\n").expect("the image job")..];
        let condition = image
            .lines()
            .map(str::trim)
            .find(|l| l.starts_with("if:"))
            .expect("the image job's condition");
        assert_eq!(condition, "if: startsWith(github.ref, 'refs/tags/v')");
    }

    /// The audit installer is third-party code: it runs before the deploy
    /// key is in the agent, not after.
    #[test]
    fn sr_nfr_003_cargo_audit_runs_before_the_deploy_key_is_loaded() {
        let workflow = workflow();
        let audit = workflow.find("run: cargo audit").expect("an audit step");
        let installer = workflow
            .find("taiki-e/install-action")
            .expect("its installer");
        let key = workflow.find("webfactory/ssh-agent").expect("the key step");
        assert!(
            installer < key && audit < key,
            "audit must run before the key is loaded"
        );
    }
}
