//! Native and actual database-client adapters; no middleware or protocol fallback.

#[cfg(feature = "cassandra")]
pub mod cassandra;
#[cfg(feature = "foundationdb")]
pub mod foundationdb;
#[cfg(any(feature = "garnet", feature = "kvrocks"))]
pub mod industry;
pub mod native;
#[cfg(feature = "rocksdb")]
pub mod rocks;
pub mod tidehunter;
#[cfg(feature = "tikv")]
pub mod tikv;
#[cfg(feature = "valkey-rdma")]
pub mod valkey_rdma;
#[cfg(feature = "valkey-tcp")]
pub mod valkey_tcp;

/// Admit configuration before any process, connection or database is created.
pub fn validate_options(
    kind: &str,
    options: &serde_json::Value,
) -> crate::backend::BackendResult<()> {
    match kind {
        "native" => native::validate_options(options),
        "valkey_tcp" => {
            #[cfg(feature = "valkey-tcp")]
            {
                if options.get("client").and_then(|v| v.as_str()) == Some("libvalkey") {
                    #[cfg(feature = "valkey-rdma")]
                    { return valkey_rdma::validate_tcp_options(options); }
                    #[cfg(not(feature = "valkey-rdma"))]
                    { return Err("client libvalkey requires Cargo feature valkey-rdma".into()); }
                }
                valkey_tcp::validate_options(options)
            }
            #[cfg(not(feature = "valkey-tcp"))]
            {
                Err("backend valkey_tcp is unavailable; enable Cargo feature valkey-tcp".into())
            }
        }
        "valkey_rdma" => {
            #[cfg(feature = "valkey-rdma")]
            { valkey_rdma::validate_options(options) }
            #[cfg(not(feature = "valkey-rdma"))]
            { Err("backend valkey_rdma is unavailable; enable Cargo feature valkey-rdma".into()) }
        }
        #[cfg(feature = "garnet")]
        "garnet" => industry::validate_options(options),
        #[cfg(feature = "kvrocks")]
        "kvrocks" => industry::validate_options(options),
        kind if matches!(kind, "garnet" | "kvrocks") => Err(format!("backend {kind} is unavailable; enable Cargo feature {kind}").into()),
        "tidehunter" => tidehunter::validate_options(options),
        #[cfg(feature = "rocksdb")]
        "rocksdb" | "blobdb" => rocks::validate_options(options, kind == "blobdb"),
        #[cfg(feature = "tikv")]
        "tikv" => tikv::validate_options(options),
        #[cfg(feature = "cassandra")]
        "cassandra" => cassandra::validate_options(options),
        #[cfg(feature = "foundationdb")]
        "foundationdb" => foundationdb::validate_options(options),
        kind if matches!(kind, "rocksdb" | "blobdb" | "tikv" | "cassandra" | "foundationdb" | "valkey_tcp") => {
            Err(format!("backend {kind} is unavailable; enable its Cargo feature (rocksdb also enables blobdb)").into())
        }
        _ => Err(format!("unknown backend: {kind}").into()),
    }
}
