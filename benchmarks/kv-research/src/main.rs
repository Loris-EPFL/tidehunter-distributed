use kv_research::backend::BackendResult;
use kv_research::{backends, config::RunConfig, runner};
use prometheus::Registry;
use std::path::PathBuf;

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
        .is_some_and(|argument| argument == "--prepare-external-case")
    {
        if arguments.len() != 4 || arguments[2] != "--output" {
            return Err(
                "usage: kv-research --prepare-external-case INPUT.json --output FRESH_DIR".into(),
            );
        }
        return backends::tidehunter::prepare_external_case_file(
            PathBuf::from(&arguments[1]).as_path(),
            PathBuf::from(&arguments[3]).as_path(),
        );
    }
    let mut args = std::env::args().skip(1);
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
                    Backends: native, tidehunter (structured TCP/OFI transport), valkey_tcp (plain TCP, feature valkey-tcp), rocksdb, tikv, cassandra, foundationdb.\n\
                    OFI requires the fabric feature and never falls back to TCP."
                );
                return Ok(());
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }
    let config_path = config_path.ok_or("--config is required")?;
    let config: RunConfig = serde_json::from_slice(&std::fs::read(config_path)?)?;
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
        .worker_threads(2)
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
                let backend = backends::valkey_tcp::ValkeyTcpBackend::open(
                    &config.backend.options,
                    &registry,
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
