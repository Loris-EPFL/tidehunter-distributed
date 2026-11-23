//! The two replay arms behind one adapter: native `sui::Db::open_native`, and
//! leased on the shared leased fixture route (`crate::leased_fixture`) with
//! one in-process owner (rootless ordinary-file live I/O). Both arms receive
//! the recorded key shape with replay relocation filters attached.

use super::trace::{FilterDecision, OpenSpec};
use super::{RowHasher, sha256};
use crate::backend::BackendResult;
use crate::backends::replay::ReplayBackend;
use crate::leased_fixture::{
    CoordinatorSpec, FixtureSpec, LeasedOwner, LiveSpec, fresh_host_id, fresh_owners,
};
use crate::mainnet_trace::Row;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use tidehunter::config::Config;
use tidehunter::distributed::codec::BatchLimits;
use tidehunter::distributed::durability::DurabilityTier;
use tidehunter::distributed::group::GroupLimits;
use tidehunter::distributed::sui;
use tidehunter::key_shape::{
    Compactor, KeyIndexing, KeyShape, KeyShapeBuilder, KeySpace, KeySpaceConfig, KeyType,
};
use tidehunter::metrics::Metrics;
use tidehunter::minibytes::Bytes;
use tidehunter::{Decision, RelocationStrategy};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Kind {
    Native,
    Leased,
}

impl Kind {
    pub(super) const fn name(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Leased => "leased",
        }
    }
}

type FilterKey = (String, Vec<u8>, [u8; 32]);
/// Unrecorded candidates kept for the report; the total is always counted.
const CANDIDATE_SAMPLE: usize = 16;

/// Recorded filter decisions of the fenced pass under replay. One instance per
/// arm; the engine's relocation thread is the only caller during a pass.
#[derive(Default)]
pub(super) struct Decisions {
    recorded: BTreeMap<FilterKey, u8>,
    /// Recorded callback requests in native order, duplicates included.
    order: Vec<FilterKey>,
    /// Observed requests, bounded at `order.len() + CANDIDATE_SAMPLE`.
    seen: Vec<FilterKey>,
    calls: u64,
    unexpected: u64,
    sample: Vec<FilterKey>,
}

/// Filter-callback evidence of one pass on one arm.
#[derive(Debug, Default, Serialize)]
pub struct FilterEvidence {
    pub calls: u64,
    pub recorded_requests: usize,
    /// The observed callback requests equal the recorded ordered sequence,
    /// including duplicates. Native must match; the leased pass before R1
    /// visits history and then live rows, so it differs by design.
    pub ordered_match: bool,
    pub unrecorded_candidates: u64,
    pub unrecorded_sample: Vec<String>,
}

impl Decisions {
    /// Lookups do not consume entries, so the leased pass, which can ask about
    /// one candidate twice, still receives the recorded decision. Sui filters
    /// are pure functions of (keyspace, key, value) within a pass, so a
    /// candidate recorded with two different decisions is a trace error.
    fn arm(&mut self, records: &[FilterDecision]) -> BackendResult<()> {
        *self = Self::default();
        for record in records {
            if record.decision > 2 {
                return Err(format!("invalid relocation decision {}", record.decision).into());
            }
            let key = (
                record.keyspace.clone(),
                record.key.clone(),
                record.value_sha256,
            );
            if let Some(previous) = self.recorded.insert(key.clone(), record.decision)
                && previous != record.decision
            {
                return Err(format!(
                    "keyspace {}: one filter candidate recorded as both {previous} and {}",
                    record.keyspace, record.decision
                )
                .into());
            }
            self.order.push(key);
        }
        Ok(())
    }

    fn take(&mut self) -> FilterEvidence {
        let state = std::mem::take(self);
        FilterEvidence {
            calls: state.calls,
            recorded_requests: state.order.len(),
            ordered_match: state.seen == state.order,
            unrecorded_candidates: state.unexpected,
            unrecorded_sample: state
                .sample
                .iter()
                .map(|(keyspace, key, hash)| {
                    format!("{keyspace}/{}/value-sha256={}", hex(key), hex(hash))
                })
                .collect(),
        }
    }

    fn decide(&mut self, keyspace: &str, key: &[u8], value: &[u8]) -> Decision {
        self.calls += 1;
        let candidate = (keyspace.to_owned(), key.to_vec(), sha256(value));
        let decision = self.recorded.get(&candidate).copied();
        if self.seen.len() < self.order.len() + CANDIDATE_SAMPLE {
            self.seen.push(candidate.clone());
        }
        match decision {
            Some(0) => Decision::Keep,
            Some(1) => Decision::Remove,
            Some(2) => Decision::StopRelocation,
            _ => {
                // Finish the real pass and report the candidate; never invent a removal.
                self.unexpected += 1;
                if self.sample.len() < CANDIDATE_SAMPLE {
                    self.sample.push(candidate);
                }
                Decision::Keep
            }
        }
    }
}

pub(super) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

// The engine shape YAML (`KeyShape` serde) skips closures, so the canonical
// shape is rebuilt with the builder and live callbacks attached.
#[derive(Deserialize)]
struct ShapeYaml {
    key_spaces: Vec<ShapeEntry>,
}

#[derive(Deserialize)]
struct ShapeEntry {
    inner: ShapeFields,
}

#[derive(Deserialize)]
struct ShapeFields {
    id: u8,
    name: String,
    key_indexing: KeyIndexing,
    mutexes: usize,
    key_type: KeyType,
    config: KeySpaceConfig,
    #[serde(default)]
    destroyed: bool,
}

/// Rebuild the recorded canonical shape. Keyspaces named in `filters` get a
/// replay filter bound to `decisions`; `compactor` attaches to one keyspace.
pub(super) fn rebuild_shape(
    yaml: &str,
    filters: &[String],
    decisions: &Arc<Mutex<Decisions>>,
    mut compactor: Option<(&str, Compactor)>,
) -> BackendResult<KeyShape> {
    let shape: ShapeYaml = serde_yaml::from_str(yaml)?;
    let mut builder = KeyShapeBuilder::new();
    let filters: BTreeSet<&str> = filters.iter().map(String::as_str).collect();
    for (position, entry) in shape.key_spaces.into_iter().enumerate() {
        let ShapeFields {
            id,
            name,
            key_indexing,
            mutexes,
            key_type,
            mut config,
            destroyed,
        } = entry.inner;
        if usize::from(id) != position {
            return Err(format!("noncanonical keyspace id {id} at position {position}").into());
        }
        if destroyed {
            return Err(format!("recorded shape has destroyed keyspace {name}").into());
        }
        // Like the native recorder's replay, attach filters only to present
        // keyspaces. Recorded callbacks are still checked against the pass.
        if filters.contains(name.as_str()) {
            let decisions = Arc::clone(decisions);
            let keyspace = name.clone();
            config = config.with_relocation_filter(move |key: &[u8], value: &[u8]| {
                decisions
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .decide(&keyspace, key, value)
            });
        }
        if compactor
            .as_ref()
            .is_some_and(|(target, _)| *target == name)
            && let Some((_, function)) = compactor.take()
        {
            config = config.with_compactor(function);
        }
        builder.add_key_space_config_indexing(&name, key_indexing, mutexes, key_type, config);
    }
    if let Some((target, _)) = compactor {
        return Err(format!("compactor keyspace {target} is absent from the shape").into());
    }
    Ok(builder.build())
}

pub(super) fn native_config(yaml: &str) -> BackendResult<Config> {
    serde_yaml::from_str(yaml).map_err(|error| format!("recorded native config: {error}").into())
}

fn engine<T>(result: sui::Result<T>) -> BackendResult<T> {
    result.map_err(|error| format!("{error:?}").into())
}

const MIB: usize = 1024 * 1024;
const PAGE: usize = 4096;
/// Members per recorded batch the leased arm admits. With the pending budget
/// it keeps at least one bounded recovery batch per owner admissible
/// (`RemoteDatabaseConfig::recovery_page_limit`).
const MAX_BATCH_OPERATIONS: u32 = 16_384;
const MAX_PENDING_BYTES: usize = 64 * MIB;

/// One in-process owner host on the shared leased fixture route with the
/// recorded native WAL geometry: owner fragment and file sizes equal the
/// native `frag_size` and `wal_file_size`.
pub(super) fn leased_owner(
    dir: &Path,
    native: &Config,
    tier: DurabilityTier,
) -> BackendResult<LeasedOwner> {
    let fragment = usize::try_from(native.frag_size)?;
    if !fragment.is_multiple_of(PAGE) || !native.wal_file_size.is_multiple_of(native.frag_size) {
        return Err(format!(
            "leased owner cannot use native geometry frag_size={} wal_file_size={}: fragments must be 4 KiB multiples and divide the file",
            native.frag_size, native.wal_file_size
        )
        .into());
    }
    // One live write frame (group + 8, page aligned) must fit one fragment;
    // a batch member (batch + 8, 8-byte aligned) must fit the group.
    let frame = fragment.min(16 * MIB - PAGE);
    let spec = FixtureSpec {
        root: dir.to_path_buf(),
        host_id: fresh_host_id(),
        owners: fresh_owners(1)?,
        tier,
        host_nqn: "nqn.2026-11.test:op-replay".into(),
        fragment_bytes: native.frag_size,
        wal_file_bytes: native.wal_file_size,
        batch_limits: BatchLimits {
            max_operations: MAX_BATCH_OPERATIONS,
            max_encoded_bytes: frame - 40,
        },
        group_limits: GroupLimits {
            max_batches: 32,
            max_encoded_bytes: frame - 8,
        },
        max_batches_per_partition: 8192,
        max_reference_metadata_bytes: 64 * MIB,
        coordinator: CoordinatorSpec {
            timeout_secs: 60,
            max_admitted_batches: 8192,
            max_pending_batches: 64,
            max_pending_bytes: MAX_PENDING_BYTES,
            read_memory_allowance_bytes: 256 * MIB,
        },
        live: LiveSpec {
            write_window: None,
            per_frame_fua: false,
        },
        // Owner-only fixture: the coordinator is opened elsewhere.
        index: None,
    };
    LeasedOwner::start(&spec)
}

/// One arm's open database and its filter decisions.
pub(super) struct Arm {
    pub kind: Kind,
    pub path: PathBuf,
    pub db: Option<Arc<sui::Db>>,
    pub decisions: Arc<Mutex<Decisions>>,
    pub strategy: RelocationStrategy,
    /// Native metrics with collection on: the relocation terminal-position gauge
    /// is the native pass evidence (OPS-FORMAT.md, native fence).
    metrics: Option<Arc<Metrics>>,
}

impl Arm {
    pub(super) fn new(kind: Kind, path: PathBuf) -> Self {
        Self {
            kind,
            path,
            db: None,
            decisions: Arc::default(),
            strategy: RelocationStrategy::default(),
            metrics: None,
        }
    }

    pub(super) fn db(&self) -> BackendResult<&Arc<sui::Db>> {
        self.db
            .as_ref()
            .ok_or_else(|| format!("{}: database is not open", self.kind.name()).into())
    }

    /// Open (or create) with the recorded schema and native configuration.
    /// The leased arm starts its owner at first creation and keeps it across
    /// coordinator close and reopen, as an owner process outlives a validator restart.
    pub(super) fn open(
        &mut self,
        spec: &OpenSpec,
        create: bool,
        owner: &mut Option<LeasedOwner>,
        compactor: Option<(&str, Compactor)>,
    ) -> BackendResult<()> {
        if self.db.is_some() {
            return Err(format!("{}: open while already open", self.kind.name()).into());
        }
        if create == self.path.exists() {
            return Err(format!(
                "{}: {} of {} {}",
                self.kind.name(),
                if create { "create" } else { "reopen" },
                if create { "existing" } else { "absent" },
                self.path.display()
            )
            .into());
        }
        let shape = rebuild_shape(&spec.shape, &spec.filters, &self.decisions, compactor)?;
        let config = native_config(&spec.config)?;
        self.strategy = config.relocation_strategy;
        let db = match self.kind {
            Kind::Native => {
                if create {
                    std::fs::create_dir(&self.path)?;
                }
                let metrics = Metrics::new_in_enabled(&prometheus::Registry::new(), true);
                self.metrics = Some(Arc::clone(&metrics));
                let db = engine(sui::Db::open_native(
                    &self.path,
                    shape,
                    Arc::new(config),
                    metrics,
                ))?;
                db.set_native_durable(spec.durable);
                db
            }
            Kind::Leased => {
                let tier = if spec.durable {
                    DurabilityTier::Synchronous
                } else {
                    DurabilityTier::Native
                };
                if owner.is_none() {
                    let dir = self
                        .path
                        .parent()
                        .ok_or("leased coordinator path has no parent")?;
                    *owner = Some(leased_owner(dir, &config, tier)?);
                }
                let owner = owner.as_ref().ok_or("leased owner missing")?;
                if owner.tier != tier {
                    return Err(format!(
                        "durability changed across reopen: owner tier {:?}, requested {tier:?}",
                        owner.tier
                    )
                    .into());
                }
                owner.open(&self.path, shape)?
            }
        };
        self.db = Some(db);
        Ok(())
    }

    pub(super) fn close(&mut self) -> BackendResult<()> {
        let db = self
            .db
            .take()
            .ok_or_else(|| format!("{}: close of a closed database", self.kind.name()))?;
        engine(db.wait_for_background_threads_to_finish())
    }

    /// Run one complete relocation pass with the recorded decisions armed.
    /// Native: the recorder's driver, the blocking native `WalBased` worker; the
    /// pass ran only if it published a new nonnegative terminal position.
    /// Leased: wait for one named pass before taking its callback evidence.
    /// This is a logical comparison, not a comparison of physical offsets.
    pub(super) fn relocate(&self, records: &[FilterDecision]) -> BackendResult<FilterEvidence> {
        let db = self.db()?;
        self.decisions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .arm(records)?;
        let result = match self.kind {
            Kind::Native => self.native_pass(db),
            Kind::Leased => db
                .start_relocation_for_test(
                    std::time::Instant::now() + std::time::Duration::from_secs(60),
                )
                .map(drop)
                .map_err(|error| format!("{error:?}").into()),
        };
        let evidence = self
            .decisions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        result.map(|()| evidence)
    }

    fn native_pass(&self, db: &sui::Db) -> BackendResult<()> {
        if self.strategy != RelocationStrategy::WalBased {
            return Err(format!(
                "TraceRelocationUnsupportedStrategy: the native fence accepts only WalBased, recorded {:?}",
                self.strategy
            )
            .into());
        }
        let native = db.native().ok_or("native arm has no native database")?;
        let terminal = &self
            .metrics
            .as_ref()
            .ok_or("TraceRelocationEvidenceUnavailable: native metrics missing")?
            .relocation_terminal_position;
        terminal.set(-1);
        native.start_blocking_relocation_with_strategy(self.strategy);
        if terminal.get() < 0 {
            return Err("TraceRelocationPassNotRun: no terminal position published".into());
        }
        Ok(())
    }

    /// Typed-store `flush` at the Sui pin (`typed-store/src/rocks/mod.rs:206-222`):
    /// a no-op on native, `sui::Db::flush` (Exact checkpoint) on the remote backend.
    pub(super) fn typed_store_flush(&self) -> BackendResult<()> {
        let db = self.db()?;
        match self.kind {
            Kind::Native => Ok(()),
            Kind::Leased => engine(db.flush()),
        }
    }

    pub(super) fn drop_db(&self) -> BackendResult<()> {
        if self.db.is_some() {
            return Err(format!("{}: drop of an open database", self.kind.name()).into());
        }
        sui::Db::drop_db(&self.path).map_err(Into::into)
    }
}

/// Each arm is a kv-research replay backend (`backends/replay.rs`), the same
/// contract as the mainnet replay: recorded point reads and put-only batches
/// go through it. A replay table is a canonical keyspace ID, and an arm holds
/// exactly one database, so its node-DB ID is always 0. Deletes, existence
/// checks, iterators, range drops and lifecycle calls have no `ReplayBackend`
/// form and stay on the arm.
impl ReplayBackend for Arm {
    fn get(&self, table: u16, key: &[u8]) -> BackendResult<Option<Bytes>> {
        let db = self.db()?;
        engine(db.get(keyspace(db, u32::from(table))?, key))
    }

    fn commit(&self, db: u16, entries: &[Row]) -> BackendResult<()> {
        if db != 0 {
            return Err(format!("{}: unknown replay database {db}", self.kind.name()).into());
        }
        let database = self.db()?;
        let mut batch = database.write_batch();
        for row in entries {
            let ks = keyspace(database, u32::from(row.table))?;
            batch.write(ks, row.key.clone(), row.value.clone());
        }
        engine(batch.commit())
    }

    fn check_background_health(&self) -> BackendResult<()> {
        let db = self.db()?;
        match db.native() {
            Some(native) => native
                .check_background_health()
                .map_err(|error| format!("native background health: {error:?}").into()),
            // A poisoned authority, publication or read worker fails even on cache hits.
            None => engine(db.validate_cached_read()),
        }
    }
}

/// Engine handle of a canonical recorded keyspace ID.
pub(super) fn keyspace(db: &sui::Db, id: u32) -> BackendResult<KeySpace> {
    db.shape()
        .iter_ks()
        .find(|desc| u32::from(desc.id().as_u8()) == id && !desc.destroyed())
        .map(|desc| desc.id())
        .ok_or_else(|| format!("keyspace {id} is not in the open shape").into())
}

pub(super) type KeySet = BTreeSet<(u32, Vec<u8>)>;

/// Every visible key of every live keyspace, from forward live iterators.
/// A key returned twice is an engine error, never silently merged.
pub(super) fn key_set(db: &Arc<sui::Db>) -> BackendResult<KeySet> {
    let mut keys = KeySet::new();
    for desc in db.shape().iter_ks().filter(|desc| !desc.destroyed()) {
        let id = u32::from(desc.id().as_u8());
        for row in db.iterator(desc.id()) {
            let key = engine(row)?.0.to_vec();
            if let Some((_, key)) = keys.replace((id, key)) {
                return Err(
                    format!("keyspace {} key {} returned twice", desc.name(), hex(&key)).into(),
                );
            }
        }
    }
    Ok(keys)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct KeyspaceDigest {
    pub keyspace: u32,
    pub name: String,
    pub rows: u64,
    /// SHA-256 over `(raw key, OPS-FORMAT row hash)` pairs, each encoded as an
    /// OPS-FORMAT row, in ascending raw key order: independent of the engine's
    /// iteration order.
    pub sha256: String,
}

/// Row count and canonical digest of a keyspace's rows. The engine iterates
/// in index (reduced) key order, which differs from raw key order when the key
/// reduction is not a key prefix, so rows are sorted by raw key first. Holds
/// one raw key and one 32-byte hash per row; a duplicate raw key is an error.
pub(super) fn canonical_digest<K: AsRef<[u8]>, V: AsRef<[u8]>>(
    rows: impl IntoIterator<Item = BackendResult<(K, V)>>,
) -> BackendResult<(u64, [u8; 32])> {
    let mut sorted = BTreeMap::new();
    for row in rows {
        let (key, value) = row?;
        let (key, value) = (key.as_ref(), value.as_ref());
        if sorted
            .insert(key.to_vec(), super::row_hash(key, value))
            .is_some()
        {
            return Err(format!("row with key {} returned twice", hex(key)).into());
        }
    }
    let mut hasher = RowHasher::new();
    for (key, hash) in &sorted {
        hasher.row(key, hash);
    }
    Ok((u64::try_from(sorted.len())?, hasher.finish()))
}

/// Per-keyspace canonical digest of the visible state.
pub(super) fn digests(db: &Arc<sui::Db>) -> BackendResult<Vec<KeyspaceDigest>> {
    let mut out = Vec::new();
    for desc in db.shape().iter_ks().filter(|desc| !desc.destroyed()) {
        let (rows, digest) = canonical_digest(db.iterator(desc.id()).map(engine))
            .map_err(|error| format!("keyspace {}: {error}", desc.name()))?;
        out.push(KeyspaceDigest {
            keyspace: u32::from(desc.id().as_u8()),
            name: desc.name().to_owned(),
            rows,
            sha256: hex(&digest),
        });
    }
    Ok(out)
}
