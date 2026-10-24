use serde::{Deserialize, Serialize};
use std::path::Path;

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
pub struct RunConfig {
    pub schema_version: u32,
    pub backend: BackendSpec,
    pub seed: u64,
    /// Non-secret human label; physical keys always use an independently generated run id.
    #[serde(default)]
    pub namespace: Option<String>,
    pub dataset: DatasetConfig,
    pub workload: WorkloadConfig,
    pub execution: ExecutionConfig,
    pub phases: PhaseConfig,
    #[serde(default)]
    pub qualification_only: bool,
    #[serde(default)]
    pub source_revision: Option<String>,
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
        if self.schema_version != 1 {
            return invalid("schema_version must be 1");
        }
        if self.backend.kind.trim().is_empty() {
            return invalid("backend.kind must not be empty");
        }
        if self.dataset.keys == 0 || self.dataset.keys > self.limits.max_keys {
            return invalid("dataset.keys must be positive and within limits.max_keys");
        }
        if !matches!(self.dataset.value_size, 256 | 1024 | 8192) {
            return invalid("dataset.value_size must be 256, 1024, or 8192 bytes");
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
                aging_operations: 0,
                measurement_operations: 2,
            },
            qualification_only: false,
            source_revision: None,
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
}
