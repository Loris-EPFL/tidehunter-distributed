//! `FoundationDB` adapter using one serializable FDB transaction per API operation.

#[cfg(target_os = "linux")]
use std::os::unix::fs::MetadataExt;
use std::{
    fs::File,
    io::{self, Read},
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use blake2::{Blake2b512, Digest};

use foundationdb::{
    Database,
    options::{DatabaseOption, TransactionOption},
};
use prometheus::Registry;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tidehunter::minibytes::Bytes;

use crate::backend::{BackendInfo, BackendResult, BatchAtomicity, KvBackend};

static API_BOOTED: AtomicBool = AtomicBool::new(false);
const FDB_MAX_KEY_BYTES: usize = 10_000;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Options {
    cluster_file: PathBuf,
    required_replication_factor: usize,
    #[serde(default = "default_timeout_ms")]
    transaction_timeout_ms: u64,
}

fn default_timeout_ms() -> u64 {
    5_000
}

fn invalid(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    io::Error::new(io::ErrorKind::InvalidInput, message.into()).into()
}

fn normalize_fdb_error(
    error: foundationdb::FdbBindingError,
) -> Box<dyn std::error::Error + Send + Sync> {
    let is_timeout = error
        .get_fdb_error()
        .is_some_and(|error| matches!(error.code(), 1004 | 1031));
    if is_timeout {
        io::Error::new(io::ErrorKind::TimedOut, error).into()
    } else {
        error.into()
    }
}

fn timeout_elapsed(error: tokio::time::error::Elapsed) -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, error)
}

// One reusable 64 KiB setup buffer fits the non-recursive 2 MiB worker stack; avoid a heap copy.
#[allow(clippy::large_stack_arrays)]
fn loaded_client_library() -> BackendResult<(String, String)> {
    #[cfg(target_os = "linux")]
    {
        let maps = std::fs::read_to_string("/proc/self/maps")?;
        let mut paths = std::collections::BTreeMap::new();
        for line in maps.lines() {
            let mut fields = line.splitn(6, ' ');
            let _range = fields.next();
            let Some(permissions) = fields.next() else {
                continue;
            };
            let _offset = fields.next();
            let _device = fields.next();
            let Some(inode) = fields.next().and_then(|inode| inode.parse::<u64>().ok()) else {
                continue;
            };
            let Some(path) = fields.next().map(str::trim) else {
                continue;
            };
            let file_name = std::path::Path::new(path)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default();
            if !permissions.contains('x')
                || !path.starts_with('/')
                || !(file_name == "libfdb_c.so" || file_name.starts_with("libfdb_c.so."))
            {
                continue;
            }
            if path.ends_with(" (deleted)") {
                return Err(invalid("loaded libfdb_c.so mapping is marked deleted"));
            }
            let canonical_path = std::fs::canonicalize(path)?;
            if paths
                .insert(canonical_path, inode)
                .is_some_and(|previous| previous != inode)
            {
                return Err(invalid(
                    "executable libfdb_c.so mappings disagree on the backing inode",
                ));
            }
        }
        if paths.len() != 1 {
            return Err(invalid(format!(
                "expected exactly one executable libfdb_c.so mapping, observed {}",
                paths.len()
            )));
        }
        let (path, mapped_inode) = paths
            .into_iter()
            .next()
            .ok_or_else(|| invalid("loaded libfdb_c.so path was not observed"))?;
        let mut file = File::open(&path)?;
        if file.metadata()?.ino() != mapped_inode {
            return Err(invalid(
                "loaded libfdb_c.so path no longer identifies the mapped inode",
            ));
        }
        let mut hasher = Blake2b512::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
        }
        let hash = format!("{:x}", hasher.finalize());
        Ok((path.to_string_lossy().into_owned(), hash))
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(invalid(
            "runtime FoundationDB client-library identity is only supported on Linux",
        ))
    }
}

fn cluster_configuration(status: &JsonValue) -> Option<&JsonValue> {
    status
        .get("cluster")
        .and_then(|value| value.get("configuration"))
        .or_else(|| status.get("configuration"))
}

fn observed_replication_factor(configuration: &JsonValue) -> Option<usize> {
    if let Some(replicas) = configuration
        .get("storage_replicas")
        .and_then(JsonValue::as_u64)
    {
        return usize::try_from(replicas).ok();
    }
    let mode = configuration.get("redundancy_mode").and_then(|value| {
        value
            .get("$enum")
            .and_then(JsonValue::as_str)
            .or_else(|| value.get("$value").and_then(JsonValue::as_str))
            .or_else(|| value.as_str())
    })?;
    match mode.to_ascii_lowercase().as_str() {
        "single" => Some(1),
        "double" => Some(2),
        "triple" => Some(3),
        _ => None,
    }
}

fn require_full_replication(status: &JsonValue) -> BackendResult<()> {
    let full_replication = status
        .get("full_replication")
        .or_else(|| {
            status
                .get("cluster")
                .and_then(|value| value.get("full_replication"))
        })
        .and_then(JsonValue::as_bool);
    if full_replication != Some(true) {
        return Err(invalid(format!(
            "FoundationDB full_replication is not verified true (observed {full_replication:?})"
        )));
    }
    Ok(())
}
fn redact_sensitive_fields(value: &mut JsonValue) {
    match value {
        JsonValue::Object(object) => {
            let sensitive: Vec<String> = object
                .keys()
                .filter(|key| {
                    let key = key.to_ascii_lowercase();
                    key.contains("password")
                        || key.contains("credential")
                        || key.contains("private_key")
                        || key.contains("secret")
                        || key.contains("authorization")
                        || key.contains("tls_key")
                })
                .cloned()
                .collect();
            for key in sensitive {
                object.remove(&key);
            }
            for child in object.values_mut() {
                redact_sensitive_fields(child);
            }
        }
        JsonValue::Array(array) => {
            for child in array {
                redact_sensitive_fields(child);
            }
        }
        _ => {}
    }
}

fn parse_options(value: &JsonValue) -> BackendResult<Options> {
    let options: Options = serde_json::from_value(value.clone())?;
    if options.cluster_file.as_os_str().is_empty() || options.cluster_file.to_str().is_none() {
        return Err(invalid("cluster_file must be a nonempty UTF-8 path"));
    }
    if options.required_replication_factor == 0 {
        return Err(invalid("required_replication_factor must be nonzero"));
    }
    if options.transaction_timeout_ms == 0 || options.transaction_timeout_ms > i32::MAX as u64 {
        return Err(invalid("transaction_timeout_ms must be in 1..=i32::MAX"));
    }
    Ok(options)
}

pub fn validate_options(value: &JsonValue) -> BackendResult<()> {
    parse_options(value).map(|_| ())
}

pub struct FoundationDbBackend {
    database: Database,
    timeout: Duration,
    info: BackendInfo,
    _network: foundationdb::api::NetworkAutoStop,
}

impl FoundationDbBackend {
    // Keep native-client identity, live cluster admission and key-domain metadata in one constructor.
    #[allow(clippy::too_many_lines)]
    pub async fn open(options: &JsonValue, _registry: &Registry) -> BackendResult<Self> {
        let options = parse_options(options)?;
        if API_BOOTED
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(invalid(
                "FoundationDB API is process-global and this adapter can only be opened once per process",
            ));
        }
        // Safety: this adapter owns the returned handle for its full lifetime and
        // drops it before program exit; the FoundationDB network API is booted once.
        let network = unsafe { foundationdb::boot() };
        let (client_library_path, client_library_blake2b512) = loaded_client_library()?;
        let client_api_max_version = foundationdb::api::get_max_api_version();
        let cluster_file = options
            .cluster_file
            .to_str()
            .ok_or_else(|| invalid("cluster_file path is not valid UTF-8"))?;
        let database = Database::from_path(cluster_file)?;
        database.set_option(DatabaseOption::TransactionTimeout(i32::try_from(
            options.transaction_timeout_ms,
        )?))?;
        let status_bytes = database
            .run(|transaction, _maybe_committed| async move {
                transaction.set_option(TransactionOption::ReadSystemKeys)?;
                Ok(transaction.get(b"\xff\xff/status/json", false).await?)
            })
            .await
            .map_err(normalize_fdb_error)?
            .ok_or_else(|| invalid("FoundationDB status key returned no value"))?;
        let status_text = std::str::from_utf8(status_bytes.as_ref())?;
        let mut status: JsonValue = serde_json::from_str(status_text)?;
        redact_sensitive_fields(&mut status);
        require_full_replication(&status)?;
        let configuration = cluster_configuration(&status).ok_or_else(|| {
            invalid("FoundationDB status JSON did not expose cluster configuration")
        })?;
        let observed_rf = observed_replication_factor(configuration)
            .ok_or_else(|| invalid("FoundationDB status lacks a supported standard redundancy_mode or custom storage_replicas"))?;
        if observed_rf != options.required_replication_factor {
            return Err(invalid(format!(
                "FoundationDB reports replication factor {observed_rf}, required_replication_factor is {}",
                options.required_replication_factor
            )));
        }
        let server_versions: std::collections::BTreeSet<String> = status
            .get("cluster")
            .and_then(|value| value.get("processes"))
            .and_then(JsonValue::as_object)
            .into_iter()
            .flat_map(|processes| processes.values())
            .filter_map(|process| process.get("version").and_then(JsonValue::as_str))
            .map(str::to_owned)
            .collect();
        let server_version = if server_versions.is_empty() {
            "server version not reported in cluster status".to_owned()
        } else {
            server_versions.into_iter().collect::<Vec<_>>().join(",")
        };
        let info = BackendInfo {
            name: "foundationdb".to_owned(),
            server_version,
            client_version: format!("foundationdb Rust bindings 0.11.0 (loaded C API max version {client_api_max_version})"),
            acknowledgement: "successful FDB transaction commit using the observed cluster storage-replication configuration; physical device fsync policy not independently verified".to_owned(),
            consistency: "FoundationDB serializable transaction semantics".to_owned(),
            replication_factor: observed_rf,
            measurement_boundary: "FoundationDB transaction API through commit/fully materialized read value".to_owned(),
            get_many_strategy: "one serializable transaction, sequential native transaction gets, exact caller order".to_owned(),
            batch_atomicity: BatchAtomicity::SerializableTransaction,
            topology: serde_json::json!({"cluster_file": options.cluster_file, "cluster_status": status}),
            configuration: serde_json::json!({
                "required_replication_factor": options.required_replication_factor,
                "observed_replication_factor": observed_rf,
                "storage_replicas": configuration.get("storage_replicas"),
                "redundancy_mode": configuration.get("redundancy_mode"),
                "storage_replication_policy": configuration.get("storage_replication_policy"),
                "log_replication_policy": configuration.get("log_replication_policy"),
                "full_replication": true,
                "disk_ack": "cluster transaction commit acknowledged; physical-device persistence unverified",
                "transport_security": "effective TLS/authentication not reported by adapter; unverified",
                "compaction": "storage-engine compaction configuration not exposed by observed status",
                "value_separation": "not configured by this KV adapter; internal storage representation unverified",
                "key_encoding": {
                    "encoding": "bounded-leading-byte-v1",
                    "first_byte_mapping": "min(first logical byte, 0xfe), or 0 for the empty key; then append all logical key bytes unchanged",
                    "overhead_bytes": 1,
                    "logical_key_max_bytes": FDB_MAX_KEY_BYTES - 1,
                    "physical_key_max_bytes": FDB_MAX_KEY_BYTES,
                    "physical_first_byte_max": 254,
                    "lexicographic_order_preserved": true
                },
                "loaded_native_library": {
                    "path": client_library_path,
                    "blake2b512": client_library_blake2b512,
                    "api_max_version": client_api_max_version
                }
            }),
        };
        Ok(Self {
            database,
            timeout: Duration::from_millis(options.transaction_timeout_ms),
            info,
            _network: network,
        })
    }

    fn key(key: &[u8]) -> BackendResult<Vec<u8>> {
        if key.len() > FDB_MAX_KEY_BYTES - 1 {
            return Err(invalid(format!(
                "FoundationDB logical key exceeds {} byte limit after ordered user-keyspace encoding",
                FDB_MAX_KEY_BYTES - 1
            )));
        }
        let mut physical = Vec::with_capacity(key.len() + 1);
        physical.push(key.first().copied().unwrap_or(0).min(0xfe));
        physical.extend_from_slice(key);
        Ok(physical)
    }
}

impl KvBackend for FoundationDbBackend {
    fn info(&self) -> BackendInfo {
        self.info.clone()
    }

    async fn get(&self, key: &[u8]) -> BackendResult<Option<Bytes>> {
        let key = Self::key(key)?;
        let key = key.as_slice();
        let value = tokio::time::timeout(
            self.timeout,
            self.database
                .run(|transaction, _| async move { Ok(transaction.get(key, false).await?) }),
        )
        .await
        .map_err(timeout_elapsed)?
        .map_err(normalize_fdb_error)?;
        Ok(value.map(|value| Bytes::copy_from_slice(value.as_ref())))
    }

    async fn put(&self, key: Bytes, value: Bytes) -> BackendResult<()> {
        let key = Self::key(key.as_ref())?;
        let key = key.as_slice();
        let value = value.as_ref();
        tokio::time::timeout(
            self.timeout,
            self.database.run(|transaction, _| async move {
                transaction.set(key, value);
                Ok(())
            }),
        )
        .await
        .map_err(timeout_elapsed)?
        .map_err(normalize_fdb_error)?;
        Ok(())
    }

    async fn delete(&self, key: Bytes) -> BackendResult<()> {
        let key = Self::key(key.as_ref())?;
        let key = key.as_slice();
        tokio::time::timeout(
            self.timeout,
            self.database.run(|transaction, _| async move {
                transaction.clear(key);
                Ok(())
            }),
        )
        .await
        .map_err(timeout_elapsed)?
        .map_err(normalize_fdb_error)?;
        Ok(())
    }

    async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
        let keys = keys
            .iter()
            .map(|key| Self::key(key.as_ref()))
            .collect::<BackendResult<Vec<_>>>()?;
        let keys = keys.as_slice();
        let values = tokio::time::timeout(
            self.timeout,
            self.database.run(|transaction, _| async move {
                let mut values = Vec::with_capacity(keys.len());
                for key in keys {
                    values.push(transaction.get(key.as_slice(), false).await?);
                }
                Ok(values)
            }),
        )
        .await
        .map_err(timeout_elapsed)?
        .map_err(normalize_fdb_error)?;
        Ok(values
            .into_iter()
            .map(|value| value.map(|value| Bytes::copy_from_slice(value.as_ref())))
            .collect())
    }

    async fn put_batch(&self, entries: &[(Bytes, Bytes)]) -> BackendResult<()> {
        let entries = entries
            .iter()
            .map(|(key, value)| Ok((Self::key(key.as_ref())?, value.as_ref())))
            .collect::<BackendResult<Vec<_>>>()?;
        let entries = entries.as_slice();
        tokio::time::timeout(
            self.timeout,
            self.database.run(|transaction, _| async move {
                for (key, value) in entries {
                    transaction.set(key.as_slice(), value);
                }
                Ok(())
            }),
        )
        .await
        .map_err(timeout_elapsed)?
        .map_err(normalize_fdb_error)?;
        Ok(())
    }
}
pub async fn open(options: &JsonValue, registry: &Registry) -> BackendResult<FoundationDbBackend> {
    FoundationDbBackend::open(options, registry).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_timeout_codes_are_classified_as_timeouts() {
        for code in [1004, 1031] {
            let error = normalize_fdb_error(foundationdb::FdbBindingError::from(
                foundationdb::FdbError::from_code(code),
            ));
            let io_error = error.downcast_ref::<io::Error>().unwrap();
            assert_eq!(io_error.kind(), io::ErrorKind::TimedOut);
        }
    }
    #[test]
    fn logical_key_embedding_preserves_order_and_spreads_leading_buckets() {
        let logical_keys: [&[u8]; 9] = [
            &[],
            &[0x00],
            &[0x00, 0x00],
            &[0x01, 0xff],
            &[0x80, 0x00],
            &[0xfe],
            &[0xfe, 0xff],
            &[0xff],
            &[0xff, 0x00],
        ];
        let physical_keys: Vec<_> = logical_keys
            .iter()
            .map(|key| FoundationDbBackend::key(key).unwrap())
            .collect();

        for pair in logical_keys.windows(2).zip(physical_keys.windows(2)) {
            let (logical, physical) = pair;
            assert_eq!(logical[0].cmp(logical[1]), physical[0].cmp(&physical[1]));
        }
        for physical in &physical_keys {
            assert!(physical[0] <= 0xfe);
        }
        assert_ne!(physical_keys[3][0], physical_keys[4][0]);
    }

    #[test]
    fn logical_key_embedding_enforces_physical_key_limit() {
        let largest_logical = vec![0xff; FDB_MAX_KEY_BYTES - 1];
        let physical = FoundationDbBackend::key(&largest_logical).unwrap();
        assert_eq!(physical.len(), FDB_MAX_KEY_BYTES);

        let oversized_logical = vec![0xff; FDB_MAX_KEY_BYTES];
        assert!(FoundationDbBackend::key(&oversized_logical).is_err());
    }
}
