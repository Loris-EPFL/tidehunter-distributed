use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::backend::BackendResult;

/// Engine acknowledgement tier, re-used so the harness cannot drift from the engine. Comparisons
/// never mix tiers: `Synchronous` matches native `value_wal_barrier`,
/// `value_wal_group_barrier` and Valkey `appendfsync always`;
/// `Native` matches only the native `page_cache` cohort.
pub use tidehunter::distributed::durability::DurabilityTier;

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

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PutKeyPolicy {
    #[default]
    Overwrite,
    New,
}

/// Required wire discriminant; old untagged workloads are deliberately rejected.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SyntheticKind {
    Synthetic,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadConfig {
    pub kind: SyntheticKind,
    pub distribution: KeyDistribution,
    pub mix: OperationMix,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default)]
    pub put_key_policy: PutKeyPolicy,
}
fn default_batch_size() -> usize {
    32
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    ClosedLoop,
    OpenLoop,
    Interference,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    pub mode: ExecutionMode,
    pub concurrency: usize,
    /// Independent scheduling threads. Seeds are stable for a fixed (seed, shard count);
    /// changing the count changes the random stream, not the configured key distribution.
    /// The fixed caller/queue budgets split by quotient/remainder. Each client owns
    /// global arrival indexes shard + n*K; open-loop due times use the global rate.
    /// Keys remain globally distributed, with the existing per-key atomic oracle.
    /// Phase gates enclose all clients; rates use min scheduled through max completed,
    /// never a sum or mean of client windows. Interference requires one client.
    /// Absent selects the smaller of caller concurrency and available CPUs,
    /// except interference mode, which retains one scheduling client.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_shards: Option<usize>,
    /// Maximum admitted in-flight requests. Arrivals beyond this bound are recorded rejected.
    pub queue_capacity: usize,
    /// Per-request timeout; global phase deadline always takes precedence.
    pub request_timeout_ms: u64,
    /// Independent arrival rate for open-loop only; operations per second.
    #[serde(default)]
    pub offered_ops_per_second: Option<f64>,
}

impl ExecutionConfig {
    pub fn effective_client_shards(&self) -> usize {
        self.client_shards.unwrap_or_else(|| {
            if self.mode == ExecutionMode::Interference {
                1
            } else {
                self.concurrency.min(tidehunter::parallelism::available())
            }
        })
    }
}

/// Explicit dual-stream settings. No campaign value is a harness default.
///
/// Use `execution.mode = interference`. `execution.concurrency` is the read worker count;
/// `execution.queue_capacity` must cover read workers plus put workers. The read-only workload
/// selects prepared keys; puts use new ids above that range. No warmup, aging or sweep runs.
/// `phases.measurement_operations` is a hard offered-call ceiling, not a duration or target.
/// Reaching it early fails the case. `limits.max_bytes` must cover the baseline plus that ceiling.
/// Admission stops after `duration_seconds`; both pools drain before the result end clock.
/// A full raw-sample writer queue fails the case without delaying arrival admission.
/// Unscheduled reads at the end gate also fail the case; no arrivals are added after that gate.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InterferenceConfig {
    pub read_offered_per_second: u64,
    pub read_queue_capacity: usize,
    pub put_concurrency: usize,
    pub duration_seconds: u64,
    pub key_policy: InterferenceKeyPolicy,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InterferenceKeyPolicy {
    DisjointReadPut,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseConfig {
    #[serde(default = "default_true")]
    pub preload: bool,
    /// Verify every externally prepared key and its deterministic payload instead
    /// of overwriting a sealed, read-only dataset during preload.
    #[serde(default)]
    pub verify_existing_dataset: bool,
    /// One advisory mapped-WAL eviction after warmup/aging, before the measurement gate.
    #[serde(default)]
    pub evict_mapped_wal_before_measurement: bool,
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
    /// Request one relocation pass after the measurement boundary and before
    /// the completion gate. The worker publishes no non-test completion
    /// signal, so the gate is the bound and the driver releases it only after
    /// it observes the owner-side retirement. `true` uses the default fuel
    /// stream; an object sets its rate and bound. See [`RelocationPhase`].
    #[serde(default)]
    pub relocation: RelocationPhase,
}

/// Post-measurement relocation request and its fuel stream.
///
/// The pass freezes its scan cut at `min(Version B durable, index applied, held
/// latch)`. A measurement that drained leaves that cut at the published tip, so
/// a keyspace without a relocation filter reaches `EndOfWal` and the pass is
/// `NoWork`: no copy, no callback start, no retirement. The fuel stream rewrites
/// dataset keys while the pass runs, which keeps the cut behind the tip and
/// allocates past the file the pass derives as its whole-file target.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum RelocationPhase {
    /// `false` requests nothing; `true` requests one pass with the default fuel.
    Enabled(bool),
    /// Explicit fuel bounds; implies a request.
    Fuel(RelocationFuelConfig),
}

impl Default for RelocationPhase {
    fn default() -> Self {
        Self::Enabled(false)
    }
}

impl RelocationPhase {
    /// Whether one pass is requested.
    #[must_use]
    pub fn requested(self) -> bool {
        match self {
            Self::Enabled(enabled) => enabled,
            Self::Fuel(_) => true,
        }
    }

    /// The fuel stream that must accompany the request, if one is requested.
    #[must_use]
    pub fn fuel(self) -> Option<RelocationFuelConfig> {
        match self {
            Self::Enabled(false) => None,
            Self::Enabled(true) => Some(RelocationFuelConfig::default()),
            Self::Fuel(config) => Some(config),
        }
    }
}

/// Bounds of the post-measurement rewrite stream.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RelocationFuelConfig {
    /// Rewrites per second. The stream never exceeds this rate.
    #[serde(default = "default_fuel_puts_per_second")]
    pub puts_per_second: u32,
    /// Wall-clock bound of the stream; the completion gate is the real bound.
    #[serde(default = "default_fuel_max_seconds")]
    pub max_seconds: u32,
}

impl Default for RelocationFuelConfig {
    fn default() -> Self {
        Self {
            puts_per_second: default_fuel_puts_per_second(),
            max_seconds: default_fuel_max_seconds(),
        }
    }
}

/// A few hundred rewrites per second keeps the cut behind the tip without
/// approaching the measured write rate (thousands per second in these cells).
fn default_fuel_puts_per_second() -> u32 {
    300
}

/// Covers the longest completion gate (300 s).
fn default_fuel_max_seconds() -> u32 {
    300
}

/// The admitted fuel rate: nonzero, and far below a measurement rate.
pub const RELOCATION_FUEL_PUTS_PER_SECOND: std::ops::RangeInclusive<u32> = 1..=4_096;
/// The admitted fuel bound: nonzero, and no longer than a long cell.
pub const RELOCATION_FUEL_MAX_SECONDS: std::ops::RangeInclusive<u32> = 1..=600;

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

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticsConfig {
    /// Maximum write operations to sample in the measured phase. Zero disables profiling.
    #[serde(default)]
    pub write_profile_limit: usize,
    /// Maximum measurement-phase mutations with request-linked timelines; zero disables.
    #[serde(default)]
    pub write_timeline_limit: usize,
    /// Retain every Nth mutation across the measurement; one retains a prefix.
    #[serde(default = "default_write_timeline_sample_every")]
    pub write_timeline_sample_every: usize,
    /// Maximum remote read requests to sample in the measured phase. Zero disables profiling.
    #[serde(default)]
    pub read_profile_limit: usize,
    /// Maximum retained Valkey owner-pipeline timing records; zero disables.
    #[serde(default)]
    pub valkey_profile_limit: usize,
}

fn default_write_timeline_sample_every() -> usize {
    1
}

impl Default for DiagnosticsConfig {
    fn default() -> Self {
        Self {
            write_profile_limit: 0,
            write_timeline_limit: 0,
            write_timeline_sample_every: 1,
            read_profile_limit: 0,
            valkey_profile_limit: 0,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunConfig {
    pub schema_version: u32,
    /// Artifact identity for focused cases. Old configs do not need this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub case_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interference: Option<InterferenceConfig>,
    pub backend: BackendSpec,
    pub seed: u64,
    /// Non-secret human label; does not select the physical key namespace.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Explicit owned dataset identity; requires preload or full existing-data
    /// verification.
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

    /// Requested Tidehunter tier (`backend.options.durability_tier`, default `native`).
    /// `None` for other backends, whose durability contract is fixed by their own options.
    pub fn durability_tier(&self) -> BackendResult<Option<DurabilityTier>> {
        if self.backend.kind != "tidehunter" {
            return Ok(None);
        }
        match self.backend.options.get("durability_tier") {
            None => Ok(Some(DurabilityTier::default())),
            Some(value) => Ok(Some(DurabilityTier::deserialize(value)?)),
        }
    }

    // Keep all admission bounds together so their cross-field constraints remain visible.
    #[allow(clippy::too_many_lines)]
    pub fn validate(&self) -> BackendResult<()> {
        let invalid = |message: &str| -> BackendResult<()> {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, message.to_owned()).into())
        };
        if self.case_id.as_ref().is_some_and(|id| {
            id.is_empty()
                || id.len() > 160
                || !id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
        }) {
            return invalid("case_id must be 1-160 ASCII letters, digits, '-' or '_'");
        }
        if self.durability_tier().is_err() {
            return invalid(
                "backend.options.durability_tier must be \"native\" or \"synchronous\"",
            );
        }
        if let Some(namespace) = &self.key_namespace_hex
            && (namespace.len() != 32
                || !namespace
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                || !(self.phases.preload || self.phases.verify_existing_dataset))
        {
            return invalid(
                "key_namespace_hex requires 32 lowercase hex digits and preload or full existing-data verification",
            );
        }
        if self.phases.verify_existing_dataset
            && (self.phases.preload
                || self.qualification_only
                || self.key_namespace_hex.is_none()
                || self.phases.aging_operations != 0
                || self.workload.mix.put != 0
                || self.workload.mix.put_batch != 0
                || self.workload.mix.delete != 0
                || self.workload.mix.missing_read != 0)
        {
            return invalid(
                "verify_existing_dataset requires an explicit namespace, no preload or aging, and a read-only measured run",
            );
        }
        if self.phases.evict_mapped_wal_before_measurement
            && (self.qualification_only
                || !matches!(self.backend.kind.as_str(), "native" | "tidehunter")
                || !cfg!(all(target_os = "linux", feature = "test-utils")))
        {
            return invalid(
                "mapped-WAL eviction requires Linux, test-utils, a native or tidehunter backend, and a measurement phase",
            );
        }
        if self.phases.relocation.requested() && self.measurement_completion_gate.is_none() {
            return invalid(
                "a relocation phase requires a measurement_completion_gate to bound the pass",
            );
        }
        if let Some(fuel) = self.phases.relocation.fuel() {
            if !RELOCATION_FUEL_PUTS_PER_SECOND.contains(&fuel.puts_per_second) {
                return invalid("phases.relocation.puts_per_second must be in 1..=4096");
            }
            if !RELOCATION_FUEL_MAX_SECONDS.contains(&fuel.max_seconds) {
                return invalid("phases.relocation.max_seconds must be in 1..=600");
            }
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
                    || !(1..=300).contains(&gate.timeout_seconds))
            {
                return invalid(&format!(
                    "{name} requires distinct absolute marker paths and timeout_seconds between 1 and 300"
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
        if self.diagnostics.write_timeline_limit > 65_536 {
            return invalid("diagnostics.write_timeline_limit must not exceed 65536");
        }
        if !(1..=1_000_000).contains(&self.diagnostics.write_timeline_sample_every) {
            return invalid(
                "diagnostics.write_timeline_sample_every must be between 1 and 1000000",
            );
        }
        if self.diagnostics.write_timeline_limit > 0 && self.backend.kind != "tidehunter" {
            return invalid("diagnostics.write_timeline_limit is only supported by tidehunter");
        }
        if self.diagnostics.write_timeline_limit > 0 && self.diagnostics.write_profile_limit > 0 {
            return invalid(
                "write timeline cannot be combined with the admission-gated write profile",
            );
        }
        if self.diagnostics.read_profile_limit > 65_536 {
            return invalid("diagnostics.read_profile_limit must not exceed 65536");
        }
        if self.diagnostics.read_profile_limit > 0 && self.backend.kind != "tidehunter" {
            return invalid("diagnostics.read_profile_limit is only supported by tidehunter");
        }
        if self.diagnostics.valkey_profile_limit > 65_536 {
            return invalid("diagnostics.valkey_profile_limit must not exceed 65536");
        }
        if self.diagnostics.valkey_profile_limit > 0
            && !(self.backend.kind == "valkey_rdma"
                || (self.backend.kind == "valkey_tcp"
                    && self
                        .backend
                        .options
                        .get("client")
                        .and_then(|value| value.as_str())
                        == Some("libvalkey")))
        {
            return invalid(
                "Valkey client profiling requires valkey_rdma or valkey_tcp with client=libvalkey",
            );
        }
        if self.schema_version != 1 {
            return invalid("schema_version must be 1");
        }
        if self.backend.kind.trim().is_empty() {
            return invalid("backend.kind must not be empty");
        }
        if matches!(
            self.backend.kind.as_str(),
            "valkey_tcp" | "valkey_rdma" | "garnet" | "kvrocks"
        ) && self.workload.mix.put_batch > 0
        {
            return invalid("selected backend does not support atomic put_batch");
        }
        if self.dataset.keys == 0 || self.dataset.keys > self.limits.max_keys {
            return invalid("dataset.keys must be positive and within limits.max_keys");
        }
        if !matches!(
            self.dataset.value_size,
            64 | 256 | 512 | 1024 | 4096 | 8192 | 16_384 | 65_536
        ) {
            return invalid(
                "dataset.value_size must be 64, 256, 512, 1024, 4096, 8192, 16384, or 65536 bytes",
            );
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
            let options = &self.backend.options;
            let limits = match options.get("external_remote_config") {
                Some(remote) if !remote.is_null() => remote["storage"].get("batch_limits"),
                _ => options.get("batch_limits"),
            };
            let owner_batch_operations = limits
                .and_then(|limits| limits.get("max_operations"))
                .and_then(serde_json::Value::as_u64)
                .and_then(|count| usize::try_from(count).ok())
                .unwrap_or(32);
            if batch_operations > owner_batch_operations {
                return invalid(
                    "Tidehunter batch qualification/workload exceeds the configured owner operation limit",
                );
            }
            let owner_batch_limit = limits
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
        if self.workload.put_key_policy == PutKeyPolicy::New {
            if self.execution.mode == ExecutionMode::Interference {
                return invalid(
                    "workload.put_key_policy=new is not supported in interference mode",
                );
            }
            if self.phases.measurement_key_sweep {
                return invalid("workload.put_key_policy=new cannot use measurement_key_sweep");
            }
            let mix = &self.workload.mix;
            if (mix.put == 0 && mix.put_batch == 0)
                || mix.read != 0
                || mix.get_many != 0
                || mix.missing_read != 0
                || mix.delete != 0
            {
                return invalid(
                    "workload.put_key_policy=new requires only put and/or put_batch operations",
                );
            }
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
                || !(self.phases.preload || self.phases.verify_existing_dataset)
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
                || !(self.phases.preload || self.phases.verify_existing_dataset)
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
        if self
            .execution
            .client_shards
            .is_some_and(|shards| shards == 0 || shards > self.execution.concurrency)
        {
            return invalid("execution.client_shards must be between 1 and concurrency");
        }
        if self.execution.effective_client_shards() > 1
            && self.execution.mode == ExecutionMode::Interference
        {
            return invalid("interference mode requires execution.client_shards=1");
        }
        if self.execution.mode == ExecutionMode::Interference {
            let Some(interference) = &self.interference else {
                return invalid("interference mode requires explicit interference settings");
            };
            let workers = self
                .execution
                .concurrency
                .checked_add(interference.put_concurrency);
            let reads = interference
                .read_offered_per_second
                .checked_mul(interference.duration_seconds);
            let storage = self
                .dataset
                .keys
                .checked_add(self.phases.measurement_operations)
                .and_then(|keys| keys.checked_mul(self.dataset.value_size as u64));
            if self.case_id.is_none()
                || self.qualification_only
                || !(self.phases.preload || self.phases.verify_existing_dataset)
                || self.phases.warmup_operations != 0
                || self.phases.aging_operations != 0
                || self.phases.warmup_key_sweep
                || self.phases.measurement_key_sweep
                || self.workload.mix.read == 0
                || self.workload.mix.put != 0
                || self.workload.mix.delete != 0
                || self.workload.mix.missing_read != 0
                || self.workload.mix.get_many != 0
                || self.workload.mix.put_batch != 0
                || self.execution.offered_ops_per_second.is_some()
                || !(1..=1_000_000_000).contains(&interference.read_offered_per_second)
                || !(1..=65_536).contains(&interference.read_queue_capacity)
                || interference.read_queue_capacity < self.execution.concurrency
                || !(1..=4096).contains(&interference.put_concurrency)
                || workers.is_none_or(|n| n > 4096 || n > self.execution.queue_capacity)
                || interference.duration_seconds == 0
                || interference.duration_seconds >= self.limits.deadline_seconds
                || reads.is_none_or(|n| n >= self.phases.measurement_operations)
                || self.phases.measurement_operations > 100_000_000
                || storage.is_none_or(|n| n > self.limits.max_bytes)
            {
                return invalid(
                    "interference requires a case_id, prepared read-only baseline, no warmup/aging/sweeps, explicit bounded streams, total worker capacity, duration within deadline, and operation/storage headroom",
                );
            }
        } else if self.interference.is_some() {
            return invalid("interference settings require interference mode");
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
            ExecutionMode::Interference => {}
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
                count.checked_add(
                    if self.phases.preload || self.phases.verify_existing_dataset {
                        self.dataset.keys
                    } else {
                        0
                    },
                )
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
mod tests;
