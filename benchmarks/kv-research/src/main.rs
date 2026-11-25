#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::undocumented_unsafe_blocks,
    clippy::multiple_unsafe_ops_per_block,
    clippy::mutex_atomic,
    clippy::await_holding_lock,
    clippy::await_holding_refcell_ref,
    clippy::arc_with_non_send_sync,
    unsafe_op_in_unsafe_fn
)]

use kv_research::backend::BackendResult;
use kv_research::{backends, config::RunConfig, runner};
use prometheus::Registry;
use std::path::{Path, PathBuf};

fn main() {
    if let Err(error) = entry() {
        eprintln!("kv-research: {error}");
        std::process::exit(1);
    }
}

// Keep feature-gated backend admission and dispatch together so they cannot diverge.
#[allow(clippy::too_many_lines)]
fn entry() -> BackendResult<()> {
    if backends::tidehunter::maybe_run_storage_node()? {
        return Ok(());
    }
    #[cfg(all(feature = "test-utils", target_os = "linux"))]
    if backends::tidehunter::maybe_run_leased_fixture_owner()? {
        return Ok(());
    }
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if arguments
        .first()
        .is_some_and(|arg| arg == "--experimental-live-nvmeof-qualification-child")
    {
        if arguments.len() != 3 {
            return Err("qualification child requires INPUT.json and a fresh READY.json".into());
        }
        return backends::tidehunter::qualify_live_nvmeof_child(
            Path::new(&arguments[1]),
            Path::new(&arguments[2]),
        );
    }
    if arguments
        .first()
        .is_some_and(|arg| arg == "--experimental-live-nvmeof-qualification")
    {
        if arguments.len() != 4 || arguments[2] != "--output" {
            return Err("usage: kv-research --experimental-live-nvmeof-qualification INPUT.json --output FRESH_DIR".into());
        }
        return backends::tidehunter::qualify_live_nvmeof(
            Path::new(&arguments[1]),
            Path::new(&arguments[3]),
        );
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "--prepare-external-case")
    {
        if arguments.len() != 4 || arguments[2] != "--output" {
            return Err(
                "usage: kv-research --prepare-external-case INPUT.json --output FRESH_DIR".into(),
            );
        }
        return backends::tidehunter::prepare_external_case_file(
            Path::new(&arguments[1]),
            Path::new(&arguments[3]),
        );
    }
    #[cfg(all(feature = "test-utils", target_os = "linux"))]
    if arguments
        .first()
        .is_some_and(|argument| argument == "--latency-replay-fixture")
    {
        if arguments.len() != 4 || arguments[2] != "--output" {
            return Err(
                "usage: kv-research --latency-replay-fixture DRIVER.json --output FRESH_DIR".into(),
            );
        }
        // Owner and engine threads live in this process: a background panic must
        // fail the case, as in the standard replay entry.
        install_replay_panic_hook(Path::new(&arguments[3]));
        return tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(backends::tidehunter::replay::latency_fixture::run(
                Path::new(&arguments[1]),
                Path::new(&arguments[3]),
            ));
    }
    let mut args = arguments.into_iter();
    let mut config_path = None;
    let mut output = None;
    let mut validate_only = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                if config_path.is_some() {
                    return Err("duplicate --config".into());
                }
                config_path = Some(PathBuf::from(args.next().ok_or("missing --config path")?));
            }
            "--output" => {
                if output.is_some() {
                    return Err("duplicate --output".into());
                }
                output = Some(PathBuf::from(args.next().ok_or("missing --output path")?));
            }
            "--validate-config" => validate_only = true,
            "--help" | "-h" => {
                println!(
                    "Usage: kv-research --config FILE --output FRESH_DIR [--validate-config]\n\
                    External owner prep: kv-research --prepare-external-case INPUT.json --output FRESH_DIR\n\
                    Backends: native, tidehunter (structured TCP/OFI transport), valkey_tcp (feature valkey-tcp), valkey_rdma (feature valkey-rdma), garnet, kvrocks, rocksdb, tikv, cassandra, foundationdb.\n\
                    OFI requires the fabric feature and never falls back to TCP."
                );
                return Ok(());
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }
    let config_path = config_path.ok_or("--config is required")?;
    let raw: serde_json::Value = serde_json::from_slice(&std::fs::read(config_path)?)?;
    if raw["workload"]["kind"] == "sui_mainnet_replay" {
        let config: kv_research::mainnet_replay::ReplayConfig = serde_json::from_value(raw)?;
        if validate_only {
            let (_, _, admission) = config.admit_configuration()?;
            println!(
                "{}",
                serde_json::to_string(&serde_json::json!({
                    "admitted_config": config, "scope": kv_research::mainnet_trace::SCOPE,
                    "workload_admission": admission,
                    "host_resources": admission["host_resources"],
                }))?
            );
            return Ok(());
        }
        let output = output.ok_or("--output is required")?;
        install_replay_panic_hook(&output);
        return tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(kv_research::mainnet_replay::run(config, &output));
    }
    let mut config: RunConfig = serde_json::from_value(raw)?;
    if config.backend.kind == "native" {
        backends::native::normalize_options(&mut config.backend.options)?;
    }
    config.validate()?;
    backends::validate_options(&config.backend.kind, &config.backend.options)?;
    if validate_only {
        println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "admitted_config": config,
                "scope": "configuration_only; no backend connected or guarantee qualification claimed"
            }))?
        );
        return Ok(());
    }
    let output = output.ok_or("--output is required")?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let registry = Registry::new();
        match config.backend.kind.as_str() {
            "native" => {
                let backend = backends::native::open(&config.backend.options, &registry).await?;
                runner::run(backend, config, &output, registry).await
            }
            #[cfg(feature = "valkey-tcp")]
            "valkey_tcp" => {
                #[cfg(feature = "valkey-rdma")]
                if config
                    .backend
                    .options
                    .get("client")
                    .and_then(|v| v.as_str())
                    == Some("libvalkey")
                {
                    let backend = backends::valkey_rdma::ValkeyRdmaBackend::open_tcp(
                        &config.backend.options,
                        &registry,
                    )
                    .await?;
                    return runner::run(backend, config, &output, registry).await;
                }
                let backend = backends::valkey_tcp::ValkeyTcpBackend::open(
                    &config.backend.options,
                    &registry,
                )
                .await?;
                runner::run(backend, config, &output, registry).await
            }
            #[cfg(feature = "valkey-rdma")]
            "valkey_rdma" => {
                let backend = backends::valkey_rdma::ValkeyRdmaBackend::open(
                    &config.backend.options,
                    &registry,
                )
                .await?;
                runner::run(backend, config, &output, registry).await
            }
            #[cfg(feature = "garnet")]
            "garnet" => {
                let backend = backends::industry::IndustryBackend::open(
                    &config.backend.options,
                    backends::industry::Store::Garnet,
                )
                .await?;
                runner::run(backend, config, &output, registry).await
            }
            #[cfg(feature = "kvrocks")]
            "kvrocks" => {
                let backend = backends::industry::IndustryBackend::open(
                    &config.backend.options,
                    backends::industry::Store::Kvrocks,
                )
                .await?;
                runner::run(backend, config, &output, registry).await
            }
            "tidehunter" => {
                let backend =
                    backends::tidehunter::open(&config.backend.options, &registry).await?;
                runner::run(backend, config, &output, registry).await
            }
            #[cfg(feature = "rocksdb")]
            kind @ ("rocksdb" | "blobdb") => {
                let backend =
                    backends::rocks::open(&config.backend.options, &registry, kind == "blobdb")
                        .await?;
                runner::run(backend, config, &output, registry).await
            }
            #[cfg(feature = "tikv")]
            "tikv" => {
                let backend = backends::tikv::open(&config.backend.options, &registry).await?;
                runner::run(backend, config, &output, registry).await
            }
            #[cfg(feature = "cassandra")]
            "cassandra" => {
                let backend = backends::cassandra::open(&config.backend.options, &registry).await?;
                runner::run(backend, config, &output, registry).await
            }
            #[cfg(feature = "foundationdb")]
            "foundationdb" => {
                let backend =
                    backends::foundationdb::open(&config.backend.options, &registry).await?;
                runner::run(backend, config, &output, registry).await
            }
            _ => Err("backend admission and dispatch disagree".into()),
        }
    })
}

/// Replay owns this process. A background engine panic is fatal, not a healthy
/// foreground result. Preserve the ordinary panic diagnostic and fail closed.
fn install_replay_panic_hook(output: &Path) {
    use std::io::Write;
    let previous = std::panic::take_hook();
    let error_output = output.join("panic.txt");
    std::panic::set_hook(Box::new(move |info| {
        previous(info);
        if let Ok(mut file) = std::fs::File::create(&error_output) {
            let _ = writeln!(file, "{info}");
            let _ = file.sync_all();
        }
        std::process::abort();
    }));
}

#[cfg(test)]
#[path = "main_tests.rs"]
mod tests;
