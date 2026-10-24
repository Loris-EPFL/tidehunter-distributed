//! Native and actual database-client adapters; no middleware or protocol fallback.

#[cfg(feature = "cassandra")]
pub mod cassandra;
#[cfg(feature = "foundationdb")]
pub mod foundationdb;
pub mod native;
#[cfg(feature = "rocksdb")]
pub mod rocks;
pub mod tidehunter;
#[cfg(feature = "tikv")]
pub mod tikv;

/// Admit configuration before any process, connection or database is created.
pub fn validate_options(
    kind: &str,
    options: &serde_json::Value,
) -> crate::backend::BackendResult<()> {
    match kind {
        "native" => native::validate_options(options),
        "tidehunter_tcp" => tidehunter::validate_options(options),
        #[cfg(feature = "rocksdb")]
        "rocksdb" | "blobdb" => rocks::validate_options(options, kind == "blobdb"),
        #[cfg(feature = "tikv")]
        "tikv" => tikv::validate_options(options),
        #[cfg(feature = "cassandra")]
        "cassandra" => cassandra::validate_options(options),
        #[cfg(feature = "foundationdb")]
        "foundationdb" => foundationdb::validate_options(options),
        kind if matches!(kind, "rocksdb" | "blobdb" | "tikv" | "cassandra" | "foundationdb") => {
            Err(format!("backend {kind} is unavailable; enable its Cargo feature (rocksdb also enables blobdb)").into())
        }
        _ => Err(format!("unknown backend: {kind}").into()),
    }
}
