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
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if arguments
        .first()
        .is_some_and(|arg| arg == "--experimental-live-nvmeof-qualification-child")
    {
        if arguments.len() != 3 {
            return Err("qualification child requires INPUT.json and a fresh READY.json".into());
        }
        #[cfg(all(feature = "nvmeof", target_os = "linux"))]
        return backends::tidehunter::qualify_live_nvmeof_child(
            Path::new(&arguments[1]),
            Path::new(&arguments[2]),
        );
        #[cfg(not(all(feature = "nvmeof", target_os = "linux")))]
        return Err("live NVMe-oF qualification requires Linux and the nvmeof feature".into());
    }
    if arguments
        .first()
        .is_some_and(|arg| arg == "--experimental-live-nvmeof-qualification")
    {
        if arguments.len() != 4 || arguments[2] != "--output" {
            return Err("usage: kv-research --experimental-live-nvmeof-qualification INPUT.json --output FRESH_DIR".into());
        }
        #[cfg(all(feature = "nvmeof", target_os = "linux"))]
        return backends::tidehunter::qualify_live_nvmeof(
            Path::new(&arguments[1]),
            Path::new(&arguments[3]),
        );
        #[cfg(not(all(feature = "nvmeof", target_os = "linux")))]
        return Err("live NVMe-oF qualification requires Linux and the nvmeof feature".into());
    }
    if arguments
        .first()
        .is_some_and(|arg| arg == "prepare-nvmeof-dataset" || arg == "export-nvmeof-dataset")
    {
        #[cfg(all(feature = "nvmeof", target_os = "linux"))]
        {
            return match (arguments[0].as_str(), arguments.as_slice()) {
                ("prepare-nvmeof-dataset", [_, input, output]) =>
                    backends::tidehunter::prepare_nvmeof_dataset(Path::new(input), Path::new(output)),
                ("export-nvmeof-dataset", [_, launch, prepared, output]) =>
                    backends::tidehunter::export_nvmeof_dataset(Path::new(launch), Path::new(prepared), Path::new(output)),
                _ => Err("usage: prepare-nvmeof-dataset RUN_CONFIG.json OUTPUT.json | export-nvmeof-dataset NODE_LAUNCH.yaml PREPARED.json OUTPUT_DIR".into()),
            };
        }
        #[cfg(not(all(feature = "nvmeof", target_os = "linux")))]
        return Err("NVMe-oF campaign tools require Linux and the nvmeof feature".into());
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
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;

    #[test]
    fn replay_panic_hook_aborts_on_a_background_panic() {
        const CHILD: &str = "KV_RESEARCH_REPLAY_PANIC_TEST";
        if let Some(output) = std::env::var_os(CHILD) {
            install_replay_panic_hook(Path::new(&output));
            let thread = std::thread::spawn(|| panic!("replay background health regression"));
            let _ = thread.join();
            panic!("panic hook did not abort");
        }
        let output =
            std::env::temp_dir().join(format!("mainnet-panic-hook-{}", std::process::id()));
        std::fs::create_dir_all(&output).unwrap();
        // Avoid producing a core dump for this deliberately aborting child.
        let child = std::process::Command::new("bash")
            .args(["-c", "ulimit -c 0; exec \"$@\"", "--"])
            .arg(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::replay_panic_hook_aborts_on_a_background_panic",
                "--nocapture",
            ])
            .env(CHILD, &output)
            .output()
            .unwrap();
        assert_eq!(child.status.signal(), Some(libc::SIGABRT));
        assert!(
            String::from_utf8_lossy(&child.stderr).contains("replay background health regression")
        );
        let error = std::fs::read_to_string(output.join("panic.txt")).unwrap();
        assert!(error.contains("replay background health regression"));
        assert!(error.contains("src/main.rs:"));
        std::fs::remove_dir_all(output).unwrap();
    }
}
