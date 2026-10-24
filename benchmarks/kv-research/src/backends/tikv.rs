//! `TiKV` `RawKV` adapter. `RawKV` batch commands are not treated as transactions.

use std::{collections::HashMap, io, time::Duration};

use prometheus::Registry;
use reqwest::Client as HttpClient;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tidehunter::minibytes::Bytes;
use tikv_client::{Config, RawClient};

use crate::backend::{BackendInfo, BackendResult, BatchAtomicity, KvBackend, unsupported};
use reqwest::Url;
use tonic::Code;

fn invalid(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    io::Error::new(io::ErrorKind::InvalidInput, message.into()).into()
}

fn validate_endpoint_url(endpoint: &str, allow_bare_host_port: bool) -> BackendResult<String> {
    let normalized = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_owned()
    } else if allow_bare_host_port {
        format!("http://{endpoint}")
    } else {
        return Err(invalid("pd_api_endpoints must be absolute http(s) URLs"));
    };
    let url = Url::parse(&normalized).map_err(|error| invalid(error.to_string()))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || url.port().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid(
            "PD endpoints must be host:port or root http(s) URLs without credentials",
        ));
    }
    Ok(url.to_string().trim_end_matches('/').to_owned())
}

fn normalize_tikv_error(error: tikv_client::Error) -> Box<dyn std::error::Error + Send + Sync> {
    if matches!(&error, tikv_client::Error::GrpcAPI(status) if status.code() == Code::DeadlineExceeded)
    {
        io::Error::new(io::ErrorKind::TimedOut, error).into()
    } else {
        error.into()
    }
}

fn normalize_http_error(error: reqwest::Error) -> Box<dyn std::error::Error + Send + Sync> {
    if error.is_timeout() {
        io::Error::new(io::ErrorKind::TimedOut, error).into()
    } else {
        error.into()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Options {
    pd_endpoints: Vec<String>,
    #[serde(default)]
    pd_api_endpoints: Vec<String>,
    required_replication_factor: usize,
    #[serde(default = "default_timeout_ms")]
    request_timeout_ms: u64,
}

fn default_timeout_ms() -> u64 {
    5_000
}

pub struct TikvBackend {
    client: RawClient,
    info: BackendInfo,
}

async fn pd_json(http: &HttpClient, endpoints: &[String], path: &str) -> BackendResult<JsonValue> {
    let mut errors = Vec::new();
    for endpoint in endpoints {
        let url = format!("{}/pd/api/v1/{path}", endpoint.trim_end_matches('/'));
        match http.get(url).send().await {
            Ok(response) => match response.error_for_status() {
                Ok(response) => match response.json().await {
                    Ok(value) => return Ok(value),
                    Err(error) if error.is_timeout() => return Err(normalize_http_error(error)),
                    Err(error) => errors.push(error.to_string()),
                },
                Err(error) if error.is_timeout() => return Err(normalize_http_error(error)),
                Err(error) => errors.push(error.to_string()),
            },
            Err(error) if error.is_timeout() => return Err(normalize_http_error(error)),
            Err(error) => errors.push(error.to_string()),
        }
    }
    Err(io::Error::other(format!(
        "could not read TiKV PD {path}: {}",
        errors.join("; ")
    ))
    .into())
}

fn parse_options(value: &JsonValue) -> BackendResult<Options> {
    let options: Options = serde_json::from_value(value.clone())?;
    if options.pd_endpoints.is_empty() {
        return Err(invalid(
            "pd_endpoints must contain at least one PD endpoint",
        ));
    }
    for endpoint in &options.pd_endpoints {
        if endpoint.starts_with("https://") {
            return Err(invalid(
                "TiKV RawClient TLS is not configured; pd_endpoints must use host:port or plaintext http://",
            ));
        }
        validate_endpoint_url(endpoint, true)?;
    }
    let mut options = options;
    options.pd_api_endpoints = if options.pd_api_endpoints.is_empty() {
        options
            .pd_endpoints
            .iter()
            .map(|endpoint| validate_endpoint_url(endpoint, true))
            .collect::<BackendResult<Vec<_>>>()?
    } else {
        options
            .pd_api_endpoints
            .iter()
            .map(|endpoint| validate_endpoint_url(endpoint, false))
            .collect::<BackendResult<Vec<_>>>()?
    };
    if options.required_replication_factor == 0 {
        return Err(invalid("required_replication_factor must be nonzero"));
    }
    if options.request_timeout_ms == 0 {
        return Err(invalid("request_timeout_ms must be nonzero"));
    }
    Ok(options)
}

pub fn validate_options(value: &JsonValue) -> BackendResult<()> {
    parse_options(value).map(|_| ())
}

impl TikvBackend {
    pub async fn open(options: &JsonValue, _registry: &Registry) -> BackendResult<Self> {
        let options = parse_options(options)?;
        let api_endpoints = options.pd_api_endpoints.clone();
        let http = HttpClient::builder()
            .timeout(Duration::from_millis(options.request_timeout_ms))
            .build()?;
        let replication: JsonValue = pd_json(&http, &api_endpoints, "config/replicate").await?;
        let observed_rf = replication
            .get("max-replicas")
            .or_else(|| replication.get("max_replicas"))
            .and_then(JsonValue::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| invalid("PD replication config did not expose max-replicas"))?;
        if observed_rf != options.required_replication_factor {
            return Err(invalid(format!(
                "PD reports max-replicas={observed_rf}, required_replication_factor={} (placement-rule peer count remains a separate observed property)",
                options.required_replication_factor
            )));
        }
        let pd_version = pd_json(&http, &api_endpoints, "version").await?;
        let stores = pd_json(&http, &api_endpoints, "stores").await?;
        let store_rows = stores
            .get("stores")
            .and_then(JsonValue::as_array)
            .ok_or_else(|| invalid("PD stores API did not expose store metadata"))?;
        let store_versions: std::collections::BTreeSet<String> = store_rows
            .iter()
            .filter_map(|entry| {
                entry
                    .get("store")
                    .and_then(|store| store.get("version"))
                    .and_then(JsonValue::as_str)
            })
            .map(str::to_owned)
            .collect();
        if store_rows.is_empty() || store_versions.is_empty() {
            return Err(invalid("PD stores API did not expose TiKV node versions"));
        }
        let server_version = format!(
            "TiKV {}; PD {}",
            store_versions.iter().cloned().collect::<Vec<_>>().join(","),
            pd_version
                .as_str()
                .or_else(|| pd_version.get("version").and_then(JsonValue::as_str))
                .unwrap_or("version unreported")
        );

        let client = RawClient::new_with_config(
            options.pd_endpoints,
            Config::default().with_timeout(Duration::from_millis(options.request_timeout_ms)),
        )
        .await
        .map_err(normalize_tikv_error)?;
        Ok(Self {
            client,
            info: BackendInfo {
                name: "tikv_raw".to_owned(),
                server_version,
                client_version: "tikv-client 0.4.0".to_owned(),
                acknowledgement: "RawKV request success; TiKV/PD acknowledgement policy is server-defined; device fsync is not independently verified".to_owned(),
                consistency: "TiKV RawKV single-key semantics; no transaction isolation; batch commands are non-atomic".to_owned(),
                replication_factor: observed_rf,
                measurement_boundary: "RawClient API call through fully materialized response value".to_owned(),
                get_many_strategy: "TiKV native RawKV batch_get; missing keys are restored as None and results reordered to caller order".to_owned(),
                batch_atomicity: BatchAtomicity::Unsupported,
                topology: serde_json::json!({"pd_endpoint_count": api_endpoints.len(), "observed_pd_replication_config": replication, "store_metadata": stores, "region_peer_count": "not observed; PD max-replicas is a configuration ceiling"}),
                configuration: serde_json::json!({"required_replication_factor": options.required_replication_factor, "observed_max_replicas": observed_rf, "rawkv_consistency": "server-defined", "transport_security": "plaintext; client TLS not configured", "acknowledgement_policy": "server-defined; disk persistence unverified", "compaction": "TiKV store config not queried", "value_separation_titan": "not queried; unverified"}),
            },
        })
    }
}

impl KvBackend for TikvBackend {
    fn info(&self) -> BackendInfo {
        self.info.clone()
    }

    async fn get(&self, key: &[u8]) -> BackendResult<Option<Bytes>> {
        Ok(self
            .client
            .get(key.to_vec())
            .await
            .map_err(normalize_tikv_error)?
            .map(Bytes::from))
    }

    async fn put(&self, key: Bytes, value: Bytes) -> BackendResult<()> {
        self.client
            .put(key.as_ref().to_vec(), value.as_ref().to_vec())
            .await
            .map_err(normalize_tikv_error)?;
        Ok(())
    }

    async fn delete(&self, key: Bytes) -> BackendResult<()> {
        self.client
            .delete(key.as_ref().to_vec())
            .await
            .map_err(normalize_tikv_error)?;
        Ok(())
    }

    async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let found = self
            .client
            .batch_get(
                keys.iter()
                    .map(|key| key.as_ref().to_vec())
                    .collect::<Vec<_>>(),
            )
            .await
            .map_err(normalize_tikv_error)?;
        let mut by_key = HashMap::with_capacity(found.len());
        for pair in found {
            let key: Vec<u8> = pair.key().clone().into();
            by_key.insert(key, Bytes::from(pair.into_value()));
        }
        Ok(keys
            .iter()
            .map(|key| by_key.get(key.as_ref()).cloned())
            .collect())
    }

    async fn put_batch(&self, _entries: &[(Bytes, Bytes)]) -> BackendResult<()> {
        Err(unsupported(
            "atomic put_batch for TiKV RawKV; use the transactional API, which this RawKV adapter intentionally does not conflate with RawKV",
        ))
    }
}
pub async fn open(options: &JsonValue, registry: &Registry) -> BackendResult<TikvBackend> {
    TikvBackend::open(options, registry).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Status;

    #[test]
    fn validates_client_and_http_endpoint_shapes() {
        assert_eq!(
            validate_endpoint_url("127.0.0.1:2379", true).unwrap(),
            "http://127.0.0.1:2379"
        );
        assert!(validate_endpoint_url("127.0.0.1:2379", false).is_err());
        assert!(validate_endpoint_url("https://pd.example:2379", false).is_ok());
        assert!(validate_endpoint_url("http://user:secret@pd.example:2379", false).is_err());
    }

    #[test]
    fn preflight_rejects_bare_http_api_endpoint() {
        let options = serde_json::json!({
            "pd_endpoints": ["127.0.0.1:2379"],
            "pd_api_endpoints": ["127.0.0.1:2379"],
            "required_replication_factor": 1
        });
        assert!(validate_options(&options).is_err());

        let options = serde_json::json!({
            "pd_endpoints": ["127.0.0.1:2379"],
            "pd_api_endpoints": ["http://127.0.0.1:2379"],
            "required_replication_factor": 1
        });
        assert!(validate_options(&options).is_ok());
    }

    #[test]
    fn tonic_deadline_is_classified_as_timeout() {
        let error = normalize_tikv_error(tikv_client::Error::GrpcAPI(Status::deadline_exceeded(
            "deadline",
        )));
        let io_error = error.downcast_ref::<io::Error>().unwrap();
        assert_eq!(io_error.kind(), io::ErrorKind::TimedOut);
    }
}
