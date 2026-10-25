//! CQL adapter implemented with the Scylla Rust driver, usable against Cassandra.

use std::sync::Arc;
use std::{
    fs::File,
    io::{self, BufReader},
    path::PathBuf,
    time::Duration,
};

use prometheus::Registry;
use scylla::{
    client::{session::Session, session_builder::SessionBuilder},
    statement::{
        Consistency,
        batch::{Batch, BatchType},
        prepared::PreparedStatement,
    },
};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tidehunter::minibytes::Bytes;

use scylla::errors::PrepareError;
use scylla::errors::{DbError, ExecutionError, RequestAttemptError};

fn is_cql_request_timeout(error: &RequestAttemptError) -> bool {
    matches!(
        error,
        RequestAttemptError::DbError(
            DbError::ReadTimeout { .. } | DbError::WriteTimeout { .. },
            _
        )
    )
}

fn is_cql_timeout(error: &ExecutionError) -> bool {
    match error {
        ExecutionError::RequestTimeout(_) => true,
        ExecutionError::LastAttemptError(error) => is_cql_request_timeout(error),
        _ => false,
    }
}

fn normalize_cql_error(error: ExecutionError) -> Box<dyn std::error::Error + Send + Sync> {
    if is_cql_timeout(&error) {
        io::Error::new(io::ErrorKind::TimedOut, error).into()
    } else {
        error.into()
    }
}

fn normalize_prepare_error(error: PrepareError) -> Box<dyn std::error::Error + Send + Sync> {
    let is_timeout = matches!(&error, PrepareError::AllAttemptsFailed { first_attempt } if is_cql_request_timeout(first_attempt));
    if is_timeout {
        io::Error::new(io::ErrorKind::TimedOut, error).into()
    } else {
        error.into()
    }
}

use crate::backend::{BackendInfo, BackendResult, BatchAtomicity, KvBackend};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Options {
    nodes: Vec<String>,
    keyspace: String,
    required_replication_factor: usize,
    #[serde(default = "default_consistency")]
    consistency: String,
    #[serde(default = "default_timeout_ms")]
    request_timeout_ms: u64,
    #[serde(default = "default_fanout")]
    get_many_concurrency: usize,
    #[serde(default)]
    username_env: Option<String>,
    #[serde(default)]
    password_env: Option<String>,
    tls_ca_file: Option<PathBuf>,
}
fn default_consistency() -> String {
    "quorum".to_owned()
}
fn default_timeout_ms() -> u64 {
    5_000
}
fn default_fanout() -> usize {
    16
}

fn invalid(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    io::Error::new(io::ErrorKind::InvalidInput, message.into()).into()
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn consistency(value: &str) -> BackendResult<Consistency> {
    if value.eq_ignore_ascii_case("quorum") {
        Ok(Consistency::Quorum)
    } else {
        Err(invalid(
            "consistency must be quorum with the validated SimpleStrategy keyspace",
        ))
    }
}

fn row_string(
    rows: &scylla::response::query_result::QueryRowsResult,
    column: usize,
) -> BackendResult<String> {
    let mut rows = rows.rows::<scylla::value::Row>()?;
    let row = rows
        .next()
        .transpose()?
        .ok_or_else(|| invalid("CQL metadata query returned no rows"))?;
    let value: Option<&str> = row.columns[column]
        .as_ref()
        .and_then(|value| value.as_text().map(String::as_str));
    value
        .map(str::to_owned)
        .ok_or_else(|| invalid("CQL metadata field was absent or not text"))
}

fn column_index(
    rows: &scylla::response::query_result::QueryRowsResult,
    name: &str,
) -> BackendResult<usize> {
    rows.column_specs()
        .iter()
        .position(|column| column.name() == name)
        .ok_or_else(|| invalid(format!("CQL system.local metadata lacks column {name}")))
}

fn parse_options(value: &JsonValue) -> BackendResult<Options> {
    let options: Options = serde_json::from_value(value.clone())?;
    if options.nodes.is_empty() {
        return Err(invalid("nodes must contain at least one CQL host"));
    }
    if options.nodes.iter().any(|node| node.contains('@')) {
        return Err(invalid(
            "CQL contact nodes must not contain URL userinfo or credentials",
        ));
    }
    if !valid_identifier(&options.keyspace) {
        return Err(invalid("keyspace must be an unquoted CQL identifier"));
    }
    if options.required_replication_factor == 0 {
        return Err(invalid("required_replication_factor must be nonzero"));
    }
    if options.request_timeout_ms == 0 {
        return Err(invalid("request_timeout_ms must be nonzero"));
    }
    if options.get_many_concurrency == 0 {
        return Err(invalid("get_many_concurrency must be nonzero"));
    }
    if options.username_env.is_some() != options.password_env.is_some() {
        return Err(invalid(
            "username_env and password_env must be supplied together",
        ));
    }
    if (options.username_env.is_some() || options.password_env.is_some())
        && options.tls_ca_file.is_none()
    {
        return Err(invalid(
            "CQL credentials require tls_ca_file; credentials are never sent over plaintext",
        ));
    }
    consistency(&options.consistency)?;
    Ok(options)
}

pub fn validate_options(value: &JsonValue) -> BackendResult<()> {
    parse_options(value).map(|_| ())
}

pub struct CassandraBackend {
    session: Arc<Session>,
    get: PreparedStatement,
    put: PreparedStatement,
    delete: PreparedStatement,
    consistency: Consistency,
    request_timeout: Duration,
    get_many_concurrency: usize,
    info: BackendInfo,
}

impl CassandraBackend {
    // Keep session setup and its observed durability/RF/compaction admission in one error scope.
    #[allow(clippy::too_many_lines)]
    pub async fn open(options: &JsonValue, _registry: &Registry) -> BackendResult<Self> {
        let options = parse_options(options)?;
        let timeout = Duration::from_millis(options.request_timeout_ms);
        let mut builder = SessionBuilder::new().connection_timeout(timeout);
        for node in &options.nodes {
            builder = builder.known_node(node);
        }
        if let Some(ca_path) = &options.tls_ca_file {
            let file = File::open(ca_path)?;
            let mut reader = BufReader::new(file);
            let certificates = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
            if certificates.is_empty() {
                return Err(invalid("tls_ca_file contains no PEM certificates"));
            }
            let mut roots = rustls::RootCertStore::empty();
            for certificate in certificates {
                roots.add(certificate)?;
            }
            let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth();
            builder = builder.tls_context(Some(Arc::new(tls)));
        }
        if let (Some(user_var), Some(pass_var)) = (&options.username_env, &options.password_env) {
            let username = std::env::var(user_var)
                .map_err(|_| invalid("CQL username environment-variable reference is unset"))?;
            let password = std::env::var(pass_var)
                .map_err(|_| invalid("CQL password environment-variable reference is unset"))?;
            builder = builder.user(username, password);
        }
        let session = Arc::new(builder.build().await?);
        let selected_consistency = consistency(&options.consistency)?;
        let local = session
            .query_unpaged("SELECT * FROM system.local", &[])
            .await
            .map_err(normalize_cql_error)?
            .into_rows_result()?;
        let version_index = column_index(&local, "release_version")?;
        let dc_index = column_index(&local, "data_center")?;
        let cluster_index = column_index(&local, "cluster_name")?;
        let server_version = row_string(&local, version_index)?;
        let data_center = row_string(&local, dc_index)?;
        let cluster_name = row_string(&local, cluster_index)?;
        let scylla_version = local
            .column_specs()
            .iter()
            .position(|column| column.name() == "scylla_version")
            .map(|index| row_string(&local, index))
            .transpose()?;
        let keyspace_meta = session.query_unpaged(
            "SELECT replication, durable_writes FROM system_schema.keyspaces WHERE keyspace_name = ?",
            (options.keyspace.as_str(),),
        ).await.map_err(normalize_cql_error)?.into_rows_result()?;
        let mut metadata_rows = keyspace_meta.rows::<scylla::value::Row>()?;
        let metadata_row = metadata_rows
            .next()
            .transpose()?
            .ok_or_else(|| invalid(format!("CQL keyspace {} does not exist", options.keyspace)))?;
        let replication: std::collections::HashMap<String, String> = metadata_row.columns[0]
            .as_ref()
            .and_then(|value| value.as_map())
            .ok_or_else(|| invalid("keyspace replication metadata unavailable"))?
            .iter()
            .filter_map(|(key, value)| Some((key.as_text()?.clone(), value.as_text()?.clone())))
            .collect();
        let durable_writes: bool = metadata_row.columns[1]
            .as_ref()
            .and_then(scylla::value::CqlValue::as_boolean)
            .ok_or_else(|| invalid("keyspace durable_writes metadata unavailable"))?;
        if !durable_writes {
            return Err(invalid("Cassandra keyspace durable_writes is false"));
        }
        let strategy = replication
            .get("class")
            .map(String::as_str)
            .unwrap_or_default();
        if !strategy.ends_with("SimpleStrategy") {
            return Err(invalid(format!(
                "keyspace replication strategy {strategy:?} is not SimpleStrategy; adapter cannot prove a single RF"
            )));
        }
        let observed_rf = replication
            .get("replication_factor")
            .and_then(|value| value.parse::<usize>().ok())
            .ok_or_else(|| {
                invalid("keyspace SimpleStrategy replication_factor is missing or invalid")
            })?;
        if observed_rf != options.required_replication_factor {
            return Err(invalid(format!(
                "keyspace RF is {observed_rf}, required_replication_factor is {}",
                options.required_replication_factor
            )));
        }
        let table = format!("{}.kv_research", options.keyspace);
        session
            .query_unpaged(
                format!("CREATE TABLE IF NOT EXISTS {table} (k blob PRIMARY KEY, v blob)"),
                &[],
            )
            .await
            .map_err(normalize_cql_error)?;
        let compaction_rows = session.query_unpaged(
            "SELECT compaction FROM system_schema.tables WHERE keyspace_name = ? AND table_name = 'kv_research'",
            (options.keyspace.as_str(),),
        ).await.map_err(normalize_cql_error)?.into_rows_result()?;
        let mut compaction_rows = compaction_rows.rows::<scylla::value::Row>()?;
        let compaction_row = compaction_rows
            .next()
            .transpose()?
            .ok_or_else(|| invalid("CQL table compaction metadata missing"))?;
        let compaction: std::collections::HashMap<String, String> = compaction_row.columns[0]
            .as_ref()
            .and_then(|value| value.as_map())
            .ok_or_else(|| invalid("CQL table compaction settings unavailable"))?
            .iter()
            .filter_map(|(key, value)| Some((key.as_text()?.clone(), value.as_text()?.clone())))
            .collect();
        let mut get = session
            .prepare(format!("SELECT v FROM {table} WHERE k = ?"))
            .await
            .map_err(normalize_prepare_error)?;
        let mut put = session
            .prepare(format!("INSERT INTO {table} (k, v) VALUES (?, ?)"))
            .await
            .map_err(normalize_prepare_error)?;
        let mut delete = session
            .prepare(format!("DELETE FROM {table} WHERE k = ?"))
            .await
            .map_err(normalize_prepare_error)?;
        for statement in [&mut get, &mut put, &mut delete] {
            statement.set_consistency(selected_consistency);
            statement.set_request_timeout(Some(timeout));
            statement.set_is_idempotent(true);
        }
        let product = if scylla_version.is_some() {
            "ScyllaDB"
        } else {
            "Cassandra-compatible CQL (vendor not identified)"
        };
        let reported_version = scylla_version.as_deref().unwrap_or(&server_version);
        let tls_enabled = options.tls_ca_file.is_some();
        let info = BackendInfo {
            name: if scylla_version.is_some() {
                "scylla_cql".to_owned()
            } else {
                "cassandra_cql_vendor_unverified".to_owned()
            },
            server_version: reported_version.to_owned(),
            client_version: "scylla Rust driver 1.9.0".to_owned(),
            acknowledgement: format!(
                "CQL write acknowledged at {selected_consistency:?}; durable_writes=true observed; commitlog fsync policy and physical device persistence not verified"
            ),
            consistency: format!(
                "{selected_consistency:?} configured on each prepared read/write/delete and logged batch"
            ),
            replication_factor: observed_rf,
            measurement_boundary: "CQL driver API through fully materialized response value"
                .to_owned(),
            get_many_strategy: format!(
                "bounded per-key prepared-query fanout (limit {}); no native CQL batch GET",
                options.get_many_concurrency
            ),
            batch_atomicity: BatchAtomicity::CrashRecovery,
            topology: serde_json::json!({"server_product": product, "contact_node_count": options.nodes.len(), "observed_data_center": data_center, "keyspace_replication": replication, "cluster_name": cluster_name, "token_and_rack_placement": "not observed by adapter"}),
            configuration: serde_json::json!({"keyspace": options.keyspace, "required_replication_factor": options.required_replication_factor, "observed_replication_factor": observed_rf, "durable_writes": durable_writes, "consistency": format!("{selected_consistency:?}"), "transport_security": if tls_enabled { "TLS with configured CA trust; TLS negotiated at connection" } else { "plaintext; TLS not configured" }, "authentication": if options.username_env.is_some() { "enabled (credential values not recorded)" } else { "disabled" }, "compaction": compaction, "value_separation": "standard CQL blob column; storage-engine value separation not queried", "commitlog_sync_and_fsync": "not observed"}),
        };
        Ok(Self {
            session,
            get,
            put,
            delete,
            consistency: selected_consistency,
            request_timeout: timeout,
            get_many_concurrency: options.get_many_concurrency,
            info,
        })
    }

    async fn get_value(&self, key: Vec<u8>) -> BackendResult<Option<Bytes>> {
        let rows = self
            .session
            .execute_unpaged(&self.get, (key,))
            .await
            .map_err(normalize_cql_error)?
            .into_rows_result()?;
        let mut rows = rows.rows::<scylla::value::Row>()?;
        match rows.next().transpose()? {
            None => Ok(None),
            Some(row) => {
                let value: Option<&[u8]> = row.columns[0]
                    .as_ref()
                    .and_then(|value| value.as_blob().map(Vec::as_slice));
                Ok(value.map(Bytes::copy_from_slice))
            }
        }
    }
}

impl KvBackend for CassandraBackend {
    fn info(&self) -> BackendInfo {
        self.info.clone()
    }

    async fn get(&self, key: &[u8]) -> BackendResult<Option<Bytes>> {
        self.get_value(key.to_vec()).await
    }

    async fn put(&self, key: Bytes, value: Bytes) -> BackendResult<()> {
        self.session
            .execute_unpaged(&self.put, (key.as_ref(), value.as_ref()))
            .await
            .map_err(normalize_cql_error)?;
        Ok(())
    }

    async fn delete(&self, key: Bytes) -> BackendResult<()> {
        self.session
            .execute_unpaged(&self.delete, (key.as_ref(),))
            .await
            .map_err(normalize_cql_error)?;
        Ok(())
    }

    async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
        use tokio::task::JoinSet;
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut output: Vec<Option<Bytes>> =
            std::iter::repeat_with(|| None).take(keys.len()).collect();
        let mut pending = JoinSet::new();
        let mut next = 0;
        while next < keys.len() || !pending.is_empty() {
            while next < keys.len() && pending.len() < self.get_many_concurrency {
                let session = self.session.clone();
                let statement = self.get.clone();
                let key = keys[next].as_ref().to_vec();
                let index = next;
                pending.spawn(async move {
                    let rows = session
                        .execute_unpaged(&statement, (key,))
                        .await
                        .map_err(normalize_cql_error)?
                        .into_rows_result()?;
                    let mut rows = rows.rows::<scylla::value::Row>()?;
                    let value = match rows.next().transpose()? {
                        None => None,
                        Some(row) => {
                            let value: Option<&[u8]> = row.columns[0]
                                .as_ref()
                                .and_then(|value| value.as_blob().map(Vec::as_slice));
                            value.map(Bytes::copy_from_slice)
                        }
                    };
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>((index, value))
                });
                next += 1;
            }
            let joined = pending
                .join_next()
                .await
                .ok_or_else(|| io::Error::other("CQL GET fanout ended unexpectedly"))?;
            let (index, value) = joined.map_err(|error| {
                io::Error::other(format!("CQL GET fanout task failed: {error}"))
            })??;
            output[index] = value;
        }
        Ok(output)
    }

    async fn put_batch(&self, entries: &[(Bytes, Bytes)]) -> BackendResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut batch = Batch::new(BatchType::Logged);
        batch.set_consistency(self.consistency);
        batch.set_request_timeout(Some(self.request_timeout));
        batch.set_is_idempotent(true);
        for _ in entries {
            batch.append_statement(self.put.clone());
        }
        let values: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(key, value)| (key.as_ref(), value.as_ref()))
            .collect();
        self.session
            .batch(&batch, values)
            .await
            .map_err(normalize_cql_error)?;
        Ok(())
    }
}
pub async fn open(options: &JsonValue, registry: &Registry) -> BackendResult<CassandraBackend> {
    CassandraBackend::open(options, registry).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use scylla::errors::WriteType;

    #[test]
    fn client_request_deadline_is_classified_as_timeout() {
        let original = ExecutionError::RequestTimeout(Duration::from_millis(10));
        let error = normalize_cql_error(original);
        let io_error = error.downcast_ref::<io::Error>().unwrap();
        assert_eq!(io_error.kind(), io::ErrorKind::TimedOut);
    }

    #[test]
    fn server_read_and_write_timeouts_are_normalized() {
        for db_error in [
            DbError::ReadTimeout {
                consistency: Consistency::Quorum,
                received: 1,
                required: 2,
                data_present: false,
            },
            DbError::WriteTimeout {
                consistency: Consistency::Quorum,
                received: 1,
                required: 2,
                write_type: WriteType::Simple,
            },
        ] {
            let original = ExecutionError::LastAttemptError(RequestAttemptError::DbError(
                db_error,
                "server timeout".to_owned(),
            ));
            let error = normalize_cql_error(original);
            assert_eq!(
                error.downcast_ref::<io::Error>().unwrap().kind(),
                io::ErrorKind::TimedOut
            );
        }
    }
}
