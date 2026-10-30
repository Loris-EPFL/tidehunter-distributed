use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::backend::BackendResult;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BackendSpec {
    pub kind: String,
    #[serde(default)]
    pub options: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetConfig {
    pub keys: u64,
    pub value_size: usize,
    /// Fraction of each value filled with deterministic pseudorandom bytes; the
    /// remaining suffix is zero-filled. This describes byte entropy, not compression.
    #[serde(default = "default_entropy")]
    pub entropy: f64,
}

fn default_entropy() -> f64 {
    1.0
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyDistribution {
    Uniform,
    Zipf { exponent: f64 },
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OperationMix {
    pub read: u32,
    pub put: u32,
    pub delete: u32,
    pub missing_read: u32,
    pub get_many: u32,
    pub put_batch: u32,
}

impl OperationMix {
    #[must_use]
    pub fn total_weight(&self) -> u64 {
        u64::from(self.read)
            + u64::from(self.put)
            + u64::from(self.delete)
            + u64::from(self.missing_read)
            + u64::from(self.get_many)
            + u64::from(self.put_batch)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadConfig {
    pub distribution: KeyDistribution,
    pub mix: OperationMix,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}
fn default_batch_size() -> usize {
    32
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    ClosedLoop,
    OpenLoop,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    pub mode: ExecutionMode,
    pub concurrency: usize,
    /// Maximum admitted in-flight requests. Arrivals beyond this bound are recorded rejected.
    pub queue_capacity: usize,
    /// Per-request timeout; global phase deadline always takes precedence.
    pub request_timeout_ms: u64,
    /// Independent arrival rate for open-loop only; operations per second.
    #[serde(default)]
    pub offered_ops_per_second: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseConfig {
    #[serde(default = "default_true")]
    pub preload: bool,
    #[serde(default)]
    pub warmup_operations: u64,
    /// Visit the full unchanged working set during a labelled read-only warm-up.
    /// Measurement continues to use the configured random key distribution.
    #[serde(default)]
    pub warmup_key_sweep: bool,
    /// Visit distinct keys during measurement, without wrapping or changing normal workloads.
    #[serde(default)]
    pub measurement_key_sweep: bool,
    #[serde(default)]
    pub aging_operations: u64,
    /// Zero is allowed only when `qualification_only` is true.
    pub measurement_operations: u64,
}
fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LimitsConfig {
    #[serde(default = "default_max_keys")]
    pub max_keys: u64,
    #[serde(default = "default_max_bytes")]
    pub max_bytes: u64,
    #[serde(default = "default_max_operations")]
    pub max_operations: u64,
    #[serde(default = "default_deadline")]
    pub deadline_seconds: u64,
    #[serde(default = "default_shutdown_grace")]
    pub shutdown_grace_seconds: u64,
}
fn default_max_keys() -> u64 {
    10_000_000
}
fn default_max_bytes() -> u64 {
    64 * 1024 * 1024 * 1024
}
fn default_max_operations() -> u64 {
    100_000_000
}
fn default_deadline() -> u64 {
    3600
}
fn default_shutdown_grace() -> u64 {
    30
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementGateConfig {
    pub ready_file: PathBuf,
    pub release_file: PathBuf,
    pub timeout_seconds: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticsConfig {
    /// Maximum write operations to sample in the measured phase. Zero disables profiling.
    #[serde(default)]
    pub write_profile_limit: usize,
    /// Maximum remote read requests to sample in the measured phase. Zero disables profiling.
    #[serde(default)]
    pub read_profile_limit: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunConfig {
    pub schema_version: u32,
    pub backend: BackendSpec,
    pub seed: u64,
    /// Non-secret human label; does not select the physical key namespace.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Explicit owned-cloud dataset identity; requires a complete preload before reuse.
    #[serde(default)]
    pub key_namespace_hex: Option<String>,
    pub dataset: DatasetConfig,
    pub workload: WorkloadConfig,
    pub execution: ExecutionConfig,
    pub phases: PhaseConfig,
    #[serde(default)]
    pub qualification_only: bool,
    #[serde(default)]
    pub source_revision: Option<String>,
    #[serde(default)]
    pub measurement_gate: Option<MeasurementGateConfig>,
    #[serde(default)]
    pub measurement_completion_gate: Option<MeasurementGateConfig>,
    #[serde(default)]
    pub diagnostics: DiagnosticsConfig,
    pub limits: LimitsConfig,
}

impl RunConfig {
    pub fn from_path(path: &Path) -> BackendResult<Self> {
        let bytes = std::fs::read(path)?;
        let config: Self = serde_json::from_slice(&bytes)?;
        config.validate()?;
        Ok(config)
    }

    // Keep all admission bounds together so their cross-field constraints remain visible.
    #[allow(clippy::too_many_lines)]
    pub fn validate(&self) -> BackendResult<()> {
        let invalid = |message: &str| -> BackendResult<()> {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, message.to_owned()).into())
        };
        if let Some(namespace) = &self.key_namespace_hex
            && (namespace.len() != 32
                || !namespace
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                || !self.phases.preload)
        {
            return invalid(
                "key_namespace_hex requires 32 lowercase hex digits and complete preload",
            );
        }
        for (gate, name) in [
            (&self.measurement_gate, "measurement_gate"),
            (
                &self.measurement_completion_gate,
                "measurement_completion_gate",
            ),
        ] {
            if let Some(gate) = gate
                && (!gate.ready_file.is_absolute()
                    || !gate.release_file.is_absolute()
                    || gate.ready_file == gate.release_file
                    || !(1..=120).contains(&gate.timeout_seconds))
            {
                return invalid(&format!(
                    "{name} requires distinct absolute marker paths and timeout_seconds between 1 and 120"
                ));
            }
        }
        if let (Some(begin), Some(end)) =
            (&self.measurement_gate, &self.measurement_completion_gate)
            && (begin.ready_file == end.ready_file
                || begin.ready_file == end.release_file
                || begin.release_file == end.ready_file
                || begin.release_file == end.release_file)
        {
            return invalid("measurement and completion gates require four distinct marker paths");
        }
        if self.diagnostics.write_profile_limit > 65_536 {
            return invalid("diagnostics.write_profile_limit must not exceed 65536");
        }
        if self.diagnostics.read_profile_limit > 65_536 {
            return invalid("diagnostics.read_profile_limit must not exceed 65536");
        }
        if self.diagnostics.read_profile_limit > 0 && self.backend.kind != "tidehunter" {
            return invalid("diagnostics.read_profile_limit is only supported by tidehunter");
        }
        if self.schema_version != 1 {
            return invalid("schema_version must be 1");
        }
        if self.backend.kind.trim().is_empty() {
            return invalid("backend.kind must not be empty");
        }
        if self.backend.kind == "valkey_tcp" && self.workload.mix.put_batch > 0 {
            return invalid("valkey_tcp does not support atomic put_batch");
        }
        if self.dataset.keys == 0 || self.dataset.keys > self.limits.max_keys {
            return invalid("dataset.keys must be positive and within limits.max_keys");
        }
        if !matches!(self.dataset.value_size, 64 | 256 | 1024 | 8192 | 65_536) {
            return invalid("dataset.value_size must be 64, 256, 1024, 8192, or 65536 bytes");
        }
        if self.backend.kind == "tidehunter" {
            // Qualification always checks a two-entry batch; workload batches
            // use the configured size only when put_batch is actually offered.
            let batch_operations = if self.workload.mix.put_batch > 0 {
                self.workload.batch_size.max(2)
            } else {
                2
            };
            // CompleteBatch's fixed header and per-member key/record framing
            // are included in this conservative owner admission bound.
            let encoded_batch_bytes = self
                .dataset
                .value_size
                .checked_add(crate::workload::KEY_BYTES)
                .and_then(|bytes| bytes.checked_add(32))
                .and_then(|bytes| bytes.checked_mul(batch_operations))
                .and_then(|bytes| bytes.checked_add(96));
            let owner_batch_operations = self
                .backend
                .options
                .get("external_remote_config")
                .and_then(|config| config.get("storage"))
                .and_then(|storage| storage.get("batch_limits"))
                .and_then(|limits| limits.get("max_operations"))
                .and_then(serde_json::Value::as_u64)
                .and_then(|count| usize::try_from(count).ok())
                .unwrap_or(32);
            if batch_operations > owner_batch_operations {
                return invalid(
                    "Tidehunter batch qualification/workload exceeds the configured owner operation limit",
                );
            }
            let owner_batch_limit = self
                .backend
                .options
                .get("external_remote_config")
                .and_then(|config| config.get("storage"))
                .and_then(|storage| storage.get("batch_limits"))
                .and_then(|limits| limits.get("max_encoded_bytes"))
                .and_then(serde_json::Value::as_u64)
                .and_then(|bytes| usize::try_from(bytes).ok())
                .unwrap_or(320 * 1024);
            if encoded_batch_bytes.is_none_or(|bytes| bytes > owner_batch_limit) {
                return invalid(
                    "Tidehunter batch qualification/workload exceeds the configured owner batch byte limit",
                );
            }
        }
        if !self.dataset.entropy.is_finite() || !(0.0..=1.0).contains(&self.dataset.entropy) {
            return invalid("dataset.entropy must be finite and between 0 and 1");
        }
        let bytes = self
            .dataset
            .keys
            .checked_mul(self.dataset.value_size as u64);
        if bytes.is_none_or(|bytes| bytes > self.limits.max_bytes) {
            return invalid("dataset exceeds limits.max_bytes");
        }
        if self.workload.mix.total_weight() == 0 {
            return invalid("workload.mix must include an operation");
        }
        if self.workload.batch_size < 2 || self.workload.batch_size > 1024 {
            return invalid("workload.batch_size must be between 2 and 1024");
        }
        if let KeyDistribution::Zipf { exponent } = &self.workload.distribution
            && (!exponent.is_finite() || !(0.0..=2.0).contains(exponent) || *exponent == 0.0)
        {
            return invalid("Zipf exponent must be finite and in (0, 2]");
        }
        if self.phases.warmup_key_sweep {
            let only_reads = self.workload.mix.put == 0
                && self.workload.mix.delete == 0
                && self.workload.mix.missing_read == 0
                && self.workload.mix.put_batch == 0;
            let keys_per_operation = if self.workload.mix.read > 0 {
                1
            } else {
                self.workload.batch_size as u64
            };
            if self.qualification_only
                || !self.phases.preload
                || !only_reads
                || self.execution.mode != ExecutionMode::ClosedLoop
                || self
                    .phases
                    .warmup_operations
                    .checked_mul(keys_per_operation)
                    .is_none_or(|keys| keys < self.dataset.keys)
            {
                return invalid(
                    "warmup_key_sweep requires a preloaded closed-loop read-only run and enough warm-up operations to visit every key",
                );
            }
        }
        if self.phases.measurement_key_sweep {
            let only_reads = self.workload.mix.put == 0
                && self.workload.mix.delete == 0
                && self.workload.mix.missing_read == 0
                && self.workload.mix.put_batch == 0;
            // A mixed scalar/batch workload can select a full batch on every call.
            let keys_per_operation = if self.workload.mix.get_many > 0 {
                self.workload.batch_size as u64
            } else {
                1
            };
            if self.qualification_only
                || !self.phases.preload
                || !only_reads
                || self.execution.mode != ExecutionMode::ClosedLoop
                || self
                    .phases
                    .measurement_operations
                    .checked_mul(keys_per_operation)
                    .is_none_or(|keys| keys > self.dataset.keys)
            {
                return invalid(
                    "measurement_key_sweep requires a preloaded closed-loop read-only run and no more measurement keys than the dataset",
                );
            }
        }
        if self.execution.request_timeout_ms == 0 || self.execution.request_timeout_ms > 300_000 {
            return invalid("execution.request_timeout_ms must be between 1 and 300000");
        }
        if self.execution.concurrency == 0
            || self.execution.concurrency > 4096
            || self.execution.queue_capacity == 0
            || self.execution.queue_capacity > 65536
            || self.execution.queue_capacity < self.execution.concurrency
        {
            return invalid(
                "execution concurrency and queue capacity must be bounded, nonzero, and queue_capacity >= concurrency",
            );
        }
        match self.execution.mode {
            ExecutionMode::OpenLoop => {
                let Some(rate) = self.execution.offered_ops_per_second else {
                    return invalid("open_loop requires offered_ops_per_second");
                };
                if !rate.is_finite() || !(0.000_001..=1_000_000_000.0).contains(&rate) {
                    return invalid(
                        "offered_ops_per_second must be finite and between 0.000001 and 1000000000",
                    );
                }
            }
            ExecutionMode::ClosedLoop if self.execution.offered_ops_per_second.is_some() => {
                return invalid("closed_loop must not specify offered_ops_per_second");
            }
            ExecutionMode::ClosedLoop => {}
        }
        if !self.qualification_only && self.phases.measurement_operations == 0 {
            return invalid(
                "measurement_operations must be positive unless qualification_only is true",
            );
        }
        let operations = self
            .phases
            .warmup_operations
            .checked_add(self.phases.aging_operations)
            .and_then(|count| count.checked_add(self.phases.measurement_operations))
            .and_then(|count| {
                count.checked_add(if self.phases.preload {
                    self.dataset.keys
                } else {
                    0
                })
            });
        if operations.is_none_or(|count| count > self.limits.max_operations) {
            return invalid(
                "preload and configured workload operations exceed limits.max_operations",
            );
        }
        if self.limits.max_keys == 0
            || self.limits.max_bytes == 0
            || self.limits.max_operations == 0
            || self.limits.deadline_seconds == 0
            || self.limits.deadline_seconds > 86_400
            || self.limits.shutdown_grace_seconds == 0
            || self.limits.shutdown_grace_seconds > 600
        {
            return invalid("limits must be positive and within supported bounds");
        }
        if let Some(namespace) = &self.namespace
            && (namespace.is_empty()
                || namespace.len() > 64
                || !namespace
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b)))
        {
            return invalid("namespace must be 1-64 ASCII letters, digits, '-' or '_'");
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> RunConfig {
        RunConfig {
            schema_version: 1,
            backend: BackendSpec {
                kind: "native".to_owned(),
                options: serde_json::json!({}),
            },
            seed: 1,
            namespace: Some("test".to_owned()),
            key_namespace_hex: None,
            dataset: DatasetConfig {
                keys: 100,
                value_size: 256,
                entropy: 1.0,
            },
            workload: WorkloadConfig {
                distribution: KeyDistribution::Uniform,
                mix: OperationMix {
                    read: 1,
                    ..OperationMix::default()
                },
                batch_size: 32,
            },
            execution: ExecutionConfig {
                mode: ExecutionMode::ClosedLoop,
                concurrency: 2,
                queue_capacity: 4,
                request_timeout_ms: 1000,
                offered_ops_per_second: None,
            },
            phases: PhaseConfig {
                preload: true,
                warmup_operations: 1,
                warmup_key_sweep: false,
                measurement_key_sweep: false,
                aging_operations: 0,
                measurement_operations: 2,
            },
            qualification_only: false,
            source_revision: None,
            measurement_gate: None,
            measurement_completion_gate: None,
            diagnostics: DiagnosticsConfig::default(),
            limits: LimitsConfig {
                max_keys: 100,
                max_bytes: 25_600,
                max_operations: 103,
                deadline_seconds: 60,
                shutdown_grace_seconds: 5,
            },
        }
    }

    #[test]
    fn accepts_bounded_valid_configuration_and_counts_preload_operations() {
        assert!(config().validate().is_ok());
        let mut too_few = config();
        too_few.limits.max_operations = 102;
        assert!(too_few.validate().is_err());
    }

    #[test]
    fn warmup_sweep_rejects_partial_or_mutating_working_sets() {
        let mut config = config();
        config.phases.warmup_key_sweep = true;
        assert!(config.validate().is_err());
        config.phases.warmup_operations = config.dataset.keys;
        config.limits.max_operations = 1000;
        assert!(config.validate().is_ok());
        config.workload.mix.put = 1;
        assert!(config.validate().is_err());
        config.workload.mix.put = 0;
        config.phases.preload = false;
        assert!(config.validate().is_err());
        config.phases.preload = true;
        config.workload.mix.read = 0;
        config.workload.mix.get_many = 1;
        config.phases.warmup_operations = config.dataset.keys.div_ceil(32);
        assert!(config.validate().is_ok());
        config.phases.warmup_operations -= 1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn measurement_sweep_rejects_wrap_mutation_and_nonclosed_loop() {
        let mut config = config();
        config.phases.measurement_key_sweep = true;
        assert!(config.validate().is_ok());
        config.phases.measurement_operations = config.dataset.keys + 1;
        config.limits.max_operations = 1000;
        assert!(config.validate().is_err());
        config.phases.measurement_operations = 2;
        config.workload.mix.get_many = 1;
        config.workload.batch_size = 64;
        assert!(config.validate().is_err());
        config.workload.batch_size = 32;
        assert!(config.validate().is_ok());
        config.workload.mix.put = 1;
        assert!(config.validate().is_err());
        config.workload.mix.put = 0;
        config.phases.preload = false;
        assert!(config.validate().is_err());
        config.phases.preload = true;
        config.execution.mode = ExecutionMode::OpenLoop;
        assert!(config.validate().is_err());
    }

    #[test]
    fn reused_key_namespace_requires_full_preload_and_exact_hex_identity() {
        let mut reused = config();
        reused.key_namespace_hex = Some("00112233445566778899aabbccddeeff".to_owned());
        assert!(reused.validate().is_ok());
        reused.phases.preload = false;
        assert!(reused.validate().is_err());
        reused.phases.preload = true;
        for malformed in [
            "0",
            "00112233445566778899aabbccddeeff00",
            "00112233445566778899AABBCCDDEEFF",
        ] {
            reused.key_namespace_hex = Some(malformed.to_owned());
            assert!(reused.validate().is_err());
        }
    }

    #[test]
    fn rejects_unbounded_or_invalid_execution_shapes() {
        let mut invalid = config();
        invalid.execution.queue_capacity = 1;
        assert!(invalid.validate().is_err());
        invalid = config();
        invalid.execution.request_timeout_ms = 0;
        assert!(invalid.validate().is_err());
        invalid = config();
        invalid.dataset.entropy = f64::NAN;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn open_loop_requires_finite_bounded_arrival_rate() {
        let mut config = config();
        config.execution.mode = ExecutionMode::OpenLoop;
        config.execution.offered_ops_per_second = Some(100.0);
        assert!(config.validate().is_ok());
        config.execution.offered_ops_per_second = Some(f64::INFINITY);
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_unknown_json_configuration_fields() {
        let decoded =
            serde_json::from_str::<BackendSpec>(r#"{"kind":"native","options":{},"secret":"x"}"#);
        assert!(decoded.is_err());
    }
    #[test]
    fn admits_only_value_sizes_and_tidehunter_batch_limits_that_fit() {
        let mut config = config();
        for size in [64, 65_536] {
            config.dataset.value_size = size;
            config.limits.max_bytes = config.dataset.keys * size as u64;
            assert!(config.validate().is_ok(), "value size {size}");
        }

        config.backend.kind = "tidehunter".to_owned();
        config.backend.options = serde_json::json!({});
        config.dataset.value_size = 8192;
        config.limits.max_bytes = config.dataset.keys * 8192;
        config.workload.mix.put_batch = 1;
        config.workload.batch_size = 32;
        assert!(config.validate().is_ok());

        config.dataset.value_size = 65_536;
        config.limits.max_bytes = config.dataset.keys * 65_536;
        config.workload.mix.put_batch = 0;
        assert!(config.validate().is_ok());
        config.workload.mix.put_batch = 1;
        config.workload.batch_size = 32;
        assert!(config.validate().is_err());
        config.workload.batch_size = 4;
        assert!(config.validate().is_ok());
        config.backend.options = serde_json::json!({
            "external_remote_config": {
                "storage": {
                    "batch_limits": {
                        "max_operations": 3,
                        "max_encoded_bytes": 320 * 1024
                    }
                }
            }
        });
        assert!(config.validate().is_err());
        config.workload.batch_size = 1;
        config.backend.options["external_remote_config"]["storage"]["batch_limits"]["max_operations"] =
            serde_json::json!(1);
        assert!(config.validate().is_err());
    }

    #[test]
    fn read_profile_is_bounded_and_requires_distributed_tidehunter() {
        let mut config = config();
        assert_eq!(config.diagnostics.read_profile_limit, 0);
        config.diagnostics.read_profile_limit = 1;
        assert!(config.validate().is_err());
        config.backend.kind = "tidehunter".to_owned();
        assert!(config.validate().is_ok());
        config.diagnostics.read_profile_limit = 65_536;
        assert!(config.validate().is_ok());
        config.diagnostics.read_profile_limit = 65_537;
        assert!(config.validate().is_err());
        assert_eq!(
            serde_json::from_str::<DiagnosticsConfig>(r#"{"write_profile_limit":0}"#)
                .unwrap()
                .read_profile_limit,
            0
        );
    }

    #[test]
    fn validates_optional_measurement_gate_paths_and_timeout() {
        let mut config = config();
        config.measurement_gate = Some(MeasurementGateConfig {
            ready_file: PathBuf::from("/tmp/kv-ready"),
            release_file: PathBuf::from("/tmp/kv-release"),
            timeout_seconds: 60,
        });
        assert!(config.validate().is_ok());
        config.measurement_completion_gate = config.measurement_gate.clone();
        assert!(config.validate().is_err());
        config
            .measurement_completion_gate
            .as_mut()
            .unwrap()
            .ready_file = PathBuf::from("/tmp/kv-completion-ready");
        config
            .measurement_completion_gate
            .as_mut()
            .unwrap()
            .release_file = PathBuf::from("/tmp/kv-completion-release");
        assert!(config.validate().is_ok());
        config.measurement_gate.as_mut().unwrap().timeout_seconds = 121;
        assert!(config.validate().is_err());
        config.measurement_gate.as_mut().unwrap().timeout_seconds = 60;
        config
            .measurement_completion_gate
            .as_mut()
            .unwrap()
            .release_file = PathBuf::from("relative");
        assert!(config.validate().is_err());
        config
            .measurement_completion_gate
            .as_mut()
            .unwrap()
            .release_file = PathBuf::from("/tmp/kv-completion-release");
        config.diagnostics.write_profile_limit = 65_537;
        assert!(config.validate().is_err());
    }
}
