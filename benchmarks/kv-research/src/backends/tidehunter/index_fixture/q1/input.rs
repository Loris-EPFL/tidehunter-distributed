//! Sealed Q1 index stage input (`kv-research --q1-index-stage STAGE.json`).
//!
//! One file binds every phase of one case: the Python-sealed index input
//! (`index`, field for field as `q1_index.validate_stage` admits it), the
//! dataset, the route (fleet: an external coordinator configuration and two
//! separately supervised L2 OwnerHosts; local proof: the ordinary-file
//! fixture), the case root and the workload bounds. Its SHA-256 is bound in
//! every phase receipt.

use super::super::{DatasetIdentity, valid_geometry};
use crate::backend::BackendResult;
use crate::config::RunConfig;
use crate::leased_fixture::{BenchmarkGeometry, FixtureOwner, FixtureSpec};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tidehunter::distributed::durability::DurabilityTier;
use tidehunter::distributed::remote_db::{RemoteDatabaseConfig, ReplayLimits};
use tidehunter::distributed::tcp::Transport;

pub(super) const SCHEMA_VERSION: u32 = 1;
pub(super) const KIND: &str = "q1_index_stage";
/// Fleet dataset size (`q1_index.FLEET_KEYS`).
pub(super) const FLEET_KEYS: u64 = 100_000_000;
/// Fleet value size (1 KiB values).
const FLEET_VALUE_SIZE: usize = 1024;
const MIB: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum StageMode {
    /// One coordinator, two external L2 OwnerHosts over TCP/mTLS control and
    /// kernel NVMe-oF data. The only mode whose receipts can qualify Q1.
    Fleet,
    /// Ordinary-file fixture owners supervised by this process; never
    /// qualifies a fleet cell.
    LocalFixtureProof,
}

/// Exact replay envelope of the sealed setting (E4: 4,096 batches / 256 MiB,
/// E16: 16,384 / 1 GiB; soft triggers are one quarter).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum ExactSetting {
    E4,
    E16,
}

impl ExactSetting {
    pub(super) const fn replay_limits(self) -> ReplayLimits {
        match self {
            Self::E4 => ReplayLimits {
                max_batches: 4096,
                max_encoded_bytes: 256 * MIB,
            },
            Self::E16 => ReplayLimits {
                max_batches: 16_384,
                max_encoded_bytes: 1024 * MIB,
            },
        }
    }

    /// Soft byte trigger: `q1_index.EXACT_SOFT_BYTES`.
    pub(super) const fn soft_bytes(self) -> u64 {
        (self.replay_limits().max_encoded_bytes / 4) as u64
    }
}

/// The Python-sealed Q1 index input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IndexInput {
    pub version: u32,
    /// Owner root fingerprints (64 lowercase hex), partition order.
    pub owner_roots: Vec<String>,
    pub tier: DurabilityTier,
    pub exact: ExactSetting,
    pub keys: u64,
    pub minimum_puts: u64,
    pub max_puts: u64,
    /// Runner admission bound (`execution.queue_capacity`).
    pub queue: usize,
    pub read_cache: bool,
    /// Coordinator process RSS ceiling.
    pub rss_bytes: u64,
    /// Coordinator cgroup memory ceiling.
    pub cgroup_bytes: u64,
    /// Per-owner WAL allocation ceilings, partition order.
    pub owner_allocation_bytes: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StageDataset {
    pub key_namespace_hex: String,
    pub seed: u64,
    pub value_size: usize,
    pub entropy: f64,
}

/// One owner host. `root` is its host root on its own guest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StageOwner {
    pub partition: u32,
    pub address: String,
    pub server_name: String,
    pub root: PathBuf,
}

/// Local proof fixture geometry (the A1 rule).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FixtureGeometry {
    pub max_admitted_batches: usize,
    pub max_index_bytes: usize,
    pub fragment_bytes: u64,
    pub wal_file_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PilotWorkload {
    /// Closed-loop pilot operations.
    pub operations: u64,
    /// Seconds of each open-loop confirmation (operations = rate x seconds);
    /// at least 9 so the suffix-debt verdict has at least 8 complete-second
    /// samples; a 5 s confirmation leaves 4 samples and its thirds verdict
    /// flips on checkpoint phase.
    pub confirm_seconds: u64,
    /// Seconds skipped before complete-second buckets count.
    pub skip_seconds: u64,
    /// Complete seconds the closed-loop rate needs to qualify.
    pub min_complete_seconds: u64,
    /// Run the open-loop confirmations and apply the stability rule. CI
    /// correctness routes set false: the pilot then selects the primary
    /// rate without confirming it, and no rate or suffix-debt claim is
    /// made.
    #[serde(default = "default_qualify")]
    pub qualify: bool,
}

/// Confirmations and the stability rule run unless a stage opts out.
const fn default_qualify() -> bool {
    true
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StageWorkload {
    /// Closed-loop callers (`execution.concurrency`); at most `index.queue`.
    pub clients: usize,
    pub request_timeout_ms: u64,
    /// Per runner invocation (`limits.deadline_seconds`).
    pub deadline_seconds: u64,
    pub shutdown_grace_seconds: u64,
    pub pilot: PilotWorkload,
    /// Timed overwrite operations, in `index.minimum_puts..=index.max_puts`.
    pub overwrite_operations: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StageInput {
    pub schema_version: u32,
    pub kind: String,
    pub case_id: String,
    pub mode: StageMode,
    pub index: IndexInput,
    pub dataset: StageDataset,
    /// Coordinator data, the phase receipt registry and owner evidence.
    pub case_root: PathBuf,
    /// Partitions 0 and 1, in order.
    pub owners: Vec<StageOwner>,
    /// Fleet: the coordinator configuration (no `storage.database`).
    #[serde(default)]
    pub remote: Option<RemoteDatabaseConfig>,
    /// Local proof: the fixture geometry.
    #[serde(default)]
    pub fixture: Option<FixtureGeometry>,
    pub workload: StageWorkload,
}

/// One runner invocation of a phase.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "rate")]
pub(super) enum RunKind {
    /// Exactly `keys` new puts at IDs `0..keys`, closed loop.
    Build,
    /// Closed-loop pilot overwrites (pilot value domain).
    PilotClosed,
    /// Open-loop pilot confirmation at a rate (pilot value domain).
    PilotConfirm(u64),
    /// Timed open-loop overwrites at the selected rate (timed value domain).
    Overwrite(u64),
}

impl RunKind {
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::Build => "build",
            Self::PilotClosed => "pilot-closed",
            Self::PilotConfirm(_) => "pilot-confirm",
            Self::Overwrite(_) => "overwrite",
        }
    }
}

fn is_hex(text: &str, digits: usize) -> bool {
    text.len() == digits
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Local owner root fingerprint: SHA-256 of the absolute root path bytes.
pub(super) fn local_root_fingerprint(root: &Path) -> String {
    super::sha256_hex(root.as_os_str().as_encoded_bytes())
}

impl StageInput {
    pub(super) fn replay_limits(&self) -> ReplayLimits {
        self.index.exact.replay_limits()
    }

    pub(super) fn dataset_identity(&self) -> DatasetIdentity {
        DatasetIdentity {
            key_namespace_hex: self.dataset.key_namespace_hex.clone(),
            seed: self.dataset.seed,
            keys: self.index.keys,
            value_size: self.dataset.value_size,
            entropy: self.dataset.entropy,
        }
    }

    pub(super) fn coordinator_path(&self) -> PathBuf {
        self.case_root.join("coordinator")
    }

    pub(super) fn fixture_root(&self) -> PathBuf {
        self.case_root.join("fixture")
    }

    fn owner_addresses(&self) -> BackendResult<Vec<SocketAddr>> {
        self.owners
            .iter()
            .map(|owner| {
                owner
                    .address
                    .parse()
                    .map_err(|error| format!("owner {} address: {error}", owner.partition).into())
            })
            .collect()
    }

    /// Every admission rule; collects all failures.
    pub(super) fn validate(&self) -> BackendResult<()> {
        let mut failures: Vec<String> = Vec::new();
        let mut require = |ok: bool, reason: &str| {
            if !ok {
                failures.push(reason.to_owned());
            }
        };
        let index = &self.index;
        require(
            self.schema_version == SCHEMA_VERSION && self.kind == KIND,
            "schema_version must be 1 and kind q1_index_stage",
        );
        require(
            !self.case_id.is_empty()
                && self.case_id.len() <= 128
                && self
                    .case_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte)),
            "case_id must be 1-128 ASCII letters, digits, '-', '_' or '.'",
        );
        require(index.version == 1, "index.version must be 1");
        require(
            index.owner_roots.len() == 2
                && index.owner_roots[0] != index.owner_roots[1]
                && index.owner_roots.iter().all(|root| is_hex(root, 64)),
            "index.owner_roots must be two distinct 64-hex fingerprints",
        );
        require(
            index.owner_allocation_bytes.len() == 2
                && index.owner_allocation_bytes.iter().all(|bytes| *bytes > 0),
            "index.owner_allocation_bytes must be two positive ceilings",
        );
        require(!index.read_cache, "index.read_cache must be false");
        require(
            index.keys > 0
                && index.minimum_puts > 0
                && index.minimum_puts <= index.max_puts
                && index.rss_bytes > 0
                && index.cgroup_bytes > 0,
            "index sizing and memory ceilings must be positive and minimum_puts <= max_puts",
        );
        require(
            (index.minimum_puts..=index.max_puts).contains(&self.workload.overwrite_operations),
            "workload.overwrite_operations must lie in index.minimum_puts..=index.max_puts",
        );
        // Without confirmations there is no debt verdict, so only the
        // original 3 s bound applies.
        let confirm_floor = if self.workload.pilot.qualify { 9 } else { 3 };
        require(
            self.workload.clients > 0
                && self.workload.clients <= index.queue
                && index.queue <= 65_536
                && self.workload.request_timeout_ms > 0
                && (1..=86_400).contains(&self.workload.deadline_seconds)
                && (1..=600).contains(&self.workload.shutdown_grace_seconds)
                && self.workload.pilot.operations > 0
                && self.workload.pilot.confirm_seconds >= confirm_floor
                && self.workload.pilot.min_complete_seconds > 0,
            "workload bounds: 0 < clients <= index.queue <= 65536, positive timeouts and pilot operations, confirmations of at least 9 s so suffix debt has at least 8 complete-second samples (3 s when pilot.qualify is false, which skips them), deadline <= 86400 s, grace <= 600 s",
        );
        require(
            is_hex(&self.dataset.key_namespace_hex, 32),
            "dataset.key_namespace_hex must be 32 lowercase hex digits",
        );
        require(
            self.case_root.is_absolute(),
            "case_root must be an absolute path",
        );
        require(
            self.owners.len() == 2
                && self
                    .owners
                    .iter()
                    .zip(0_u32..)
                    .all(|(owner, partition)| owner.partition == partition),
            "owners must be partitions 0 and 1 in order",
        );
        require(
            self.owners.iter().all(|owner| owner.root.is_absolute())
                && self.owner_addresses().is_ok(),
            "owner roots must be absolute and owner addresses socket addresses",
        );
        match self.mode {
            StageMode::Fleet => self.validate_fleet(&mut require),
            StageMode::LocalFixtureProof => self.validate_local(&mut require),
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(format!("invalid Q1 stage input: {}", failures.join("; ")).into())
        }
    }

    fn validate_fleet(&self, require: &mut impl FnMut(bool, &str)) {
        require(
            self.fixture.is_none(),
            "fleet mode takes no fixture geometry",
        );
        require(
            self.index.keys == FLEET_KEYS && self.dataset.value_size == FLEET_VALUE_SIZE,
            "fleet mode requires 100M keys with 1 KiB values",
        );
        let Some(remote) = &self.remote else {
            require(false, "fleet mode requires remote");
            return;
        };
        require(remote.validate().is_ok(), "remote configuration is invalid");
        require(
            remote.storage.partitions == [0, 1],
            "remote.storage.partitions must be [0, 1]",
        );
        require(
            remote.endpoints.len() == 2
                && remote
                    .endpoints
                    .iter()
                    .zip(&self.owners)
                    .all(|(endpoint, owner)| {
                        endpoint.partition == owner.partition
                            && endpoint.address == owner.address
                            && endpoint.server_name == owner.server_name
                    }),
            "remote.endpoints must equal the stage owners",
        );
        require(
            remote.tls.transport == Transport::Tcp,
            "fleet control must be TCP/mTLS (transport tcp)",
        );
        require(
            remote.read_cache_bytes == 0,
            "remote.read_cache_bytes must be 0 (read cache disabled)",
        );
        require(
            remote.replay_limits == self.replay_limits(),
            "remote.replay_limits must equal the sealed Exact setting",
        );
        require(
            remote.live_nvme.leased_durability == self.index.tier,
            "remote.live_nvme.leased_durability must equal index.tier",
        );
    }

    fn validate_local(&self, require: &mut impl FnMut(bool, &str)) {
        require(self.remote.is_none(), "local proof mode takes no remote");
        require(
            self.fixture.is_some_and(|geometry| {
                valid_geometry(
                    geometry.max_admitted_batches,
                    geometry.max_index_bytes,
                    geometry.fragment_bytes,
                    geometry.wal_file_bytes,
                )
            }),
            "local proof mode requires valid fixture geometry",
        );
        require(
            self.owner_addresses()
                .is_ok_and(|addresses| addresses.iter().all(|address| address.ip().is_loopback())),
            "local proof owners must listen on loopback",
        );
        let root = self.fixture_root();
        require(
            self.owners
                .iter()
                .zip(&self.index.owner_roots)
                .all(|(owner, fingerprint)| {
                    owner.root == root.join(format!("owner-p{}", owner.partition))
                        && *fingerprint == local_root_fingerprint(&owner.root)
                }),
            "local proof owner roots must be <case_root>/fixture/owner-p<N> with SHA-256 path fingerprints",
        );
    }

    /// The local proof fixture of this stage with `host_id`.
    pub(super) fn local_spec(&self, host_id: [u8; 16]) -> BackendResult<FixtureSpec> {
        let geometry = self
            .fixture
            .ok_or("local proof mode requires fixture geometry")?;
        let owners = self
            .owner_addresses()?
            .into_iter()
            .zip(&self.owners)
            .map(|(address, owner)| FixtureOwner {
                partition: owner.partition,
                address,
            })
            .collect();
        let mut spec = BenchmarkGeometry {
            fragment_bytes: geometry.fragment_bytes,
            wal_file_bytes: geometry.wal_file_bytes,
            max_admitted_batches: geometry.max_admitted_batches,
            max_index_bytes: geometry.max_index_bytes,
            replay_limits: Some(self.replay_limits()),
        }
        .spec(self.fixture_root(), self.index.tier, owners);
        spec.host_id = host_id;
        Ok(spec)
    }

    /// The exact runner configuration of one invocation. `backend.options`
    /// carries only the tier and the index-fixture stage marker the runner
    /// validates; the adapter is opened by the Q1 route, never from options.
    pub(super) fn run_config(&self, run: RunKind) -> BackendResult<RunConfig> {
        let (stage, policy, operations, rate) = match run {
            RunKind::Build => ("build", "new", self.index.keys, None),
            RunKind::PilotClosed => (
                "overwrite",
                "overwrite",
                self.workload.pilot.operations,
                None,
            ),
            RunKind::PilotConfirm(rate) => (
                "overwrite",
                "overwrite",
                rate.checked_mul(self.workload.pilot.confirm_seconds)
                    .ok_or("confirmation operations overflow")?,
                Some(rate),
            ),
            RunKind::Overwrite(rate) => (
                "overwrite",
                "overwrite",
                self.workload.overwrite_operations,
                Some(rate),
            ),
        };
        let mut execution = json!({"mode": "closed_loop", "concurrency": self.workload.clients,
            "client_shards": 1, "queue_capacity": self.index.queue,
            "request_timeout_ms": self.workload.request_timeout_ms});
        if let Some(rate) = rate {
            if rate == 0 {
                return Err("open-loop rate must be positive".into());
            }
            execution["mode"] = json!("open_loop");
            execution["offered_ops_per_second"] = json!(rate);
        }
        let max_bytes = self
            .index
            .keys
            .checked_mul(u64::try_from(self.dataset.value_size)?)
            .ok_or("dataset bytes overflow")?;
        let config: RunConfig = serde_json::from_value(json!({
            "schema_version": 1,
            "case_id": format!("{}-{}", self.case_id, run.label()),
            "backend": {"kind": "tidehunter", "options": {
                "durability_tier": self.index.tier,
                "index_fixture": {"stage": stage}}},
            "seed": self.dataset.seed,
            "key_namespace_hex": self.dataset.key_namespace_hex,
            "dataset": {"keys": self.index.keys, "value_size": self.dataset.value_size,
                "entropy": self.dataset.entropy},
            "workload": {"kind": "synthetic", "distribution": "uniform",
                "put_key_policy": policy,
                "mix": {"read": 0, "put": 100, "delete": 0, "missing_read": 0,
                    "get_many": 0, "put_batch": 0}, "batch_size": 32},
            "execution": execution,
            "phases": {"preload": false, "verify_existing_dataset": false,
                "warmup_operations": 0, "aging_operations": 0,
                "measurement_operations": operations},
            "qualification_only": false,
            "diagnostics": {"index_checkpoint": true, "write_profile_limit": 0,
                "read_profile_limit": 0, "write_timeline_limit": 0},
            "limits": {"max_keys": self.index.keys, "max_bytes": max_bytes,
                "max_operations": operations,
                "deadline_seconds": self.workload.deadline_seconds,
                "shutdown_grace_seconds": self.workload.shutdown_grace_seconds},
        }))?;
        config.validate()?;
        Ok(config)
    }
}
