//! Serial cache evaluation from TSV events or seeded synthetic traces.
//!
//! Events run in recorded order. A warm-up prefix changes state but not measured
//! counters. Results contain lookup ratios, admission counters and final byte
//! charges. CSV `retained_bytes` excludes the separate `metadata_bytes` charge.
//! Production retained bytes include live Entry charges; its metadata is fixed
//! arrays only. Controls report live metadata separately from payload bytes.
//! The ignored sweep writes CSV to `TH_TRACE_EVAL_OUT`.
//! This tool has no concurrency and supplies no real Sui trace. Set collisions
//! depend on the placement seed. It does not measure latency or process RSS.

use super::*;
use crate::distributed::types::{
    BatchId, BatchVersion, MemberChecksum, PayloadDigest, PhysicalValueAddress, RecordDescriptor,
};
use crate::key_shape::KeySpace;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::io::{self, BufRead, Write};

// Each sweep run has 4,096 warm-up and 16,384 measured point reads. Scans add
// 1,024 events. Small windows bound debug-build sweep work, not MRC convergence.
const SWEEP_WARMUP: usize = 4_096;
const SWEEP_EVALUATION: usize = 16_384;
const SWEEP_SCAN: usize = 1_024;
const TRACE_HEADER: &str = "op\tkeyspace\tkey_id\tkey_len\tvalue_len\tversion";
const MIB: usize = 1024 * 1024;
// Keep the controls and generated universes identical to the slot-floor baseline.
const CONTROL_METADATA_BYTES: usize = 240;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Op {
    Get,
    PublishPut,
    PublishDelete,
    ScanGet,
}

impl Op {
    fn name(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::PublishPut => "publish_put",
            Self::PublishDelete => "publish_delete",
            Self::ScanGet => "scan_get",
        }
    }

    fn parse(text: &str) -> io::Result<Self> {
        match text {
            "get" => Ok(Self::Get),
            "publish_put" => Ok(Self::PublishPut),
            "publish_delete" => Ok(Self::PublishDelete),
            "scan_get" => Ok(Self::ScanGet),
            _ => Err(invalid_trace("unknown operation")),
        }
    }
}

/// A Get names a live Put reference, not an absent authoritative-index lookup.
/// A delete warms a distinct tombstone; it does not erase historical references.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Event {
    op: Op,
    keyspace: u8,
    key_id: u64,
    key_len: usize,
    value_len: usize,
    version: u64,
}

impl Event {
    fn identity(self) -> Identity {
        Identity(
            self.keyspace,
            self.key_id,
            self.version,
            self.op == Op::PublishDelete,
        )
    }

    fn payload_bytes(self) -> usize {
        self.key_len
            + self.value_len
            + BYTE_OWNER_BYTES * (usize::from(self.key_len != 0) + usize::from(self.value_len != 0))
    }

    fn key(self) -> Bytes {
        let id = self.key_id.to_le_bytes();
        Bytes::from(
            (0..self.key_len)
                .map(|index| id[index % id.len()])
                .collect::<Vec<_>>(),
        )
    }

    fn reference(self) -> ValueRecordReference {
        let mut operation = [0; 16];
        operation[..8].copy_from_slice(&self.key_id.to_le_bytes());
        operation[8..].copy_from_slice(&self.version.to_le_bytes());
        let mut database = [1; 16];
        database[0] = self.keyspace;
        let key_len = u32::try_from(self.key_len).expect("trace key length exceeds u32");
        let value_len = u32::try_from(self.value_len).expect("trace value length exceeds u32");
        let value_offset = key_len
            .checked_add(32)
            .expect("trace member length overflow");
        let len = value_offset
            .checked_add(value_len)
            .expect("trace member length overflow");
        ValueRecordReference {
            batch_id: BatchId {
                database,
                operation,
            },
            batch_version: BatchVersion {
                epoch: 1,
                sequence: self.version,
            },
            batch_digest: PayloadDigest([2; 32]),
            batch_address: PhysicalValueAddress {
                partition: 0,
                segment_generation: 1,
                offset: 0,
                len,
            },
            descriptor: RecordDescriptor {
                operation_index: 0,
                offset: 0,
                len,
                keyspace: self.keyspace,
                kind: if self.op == Op::PublishDelete {
                    RecordKind::Delete
                } else {
                    RecordKind::Put
                },
                key_len,
                value_offset,
                value_len,
                checksum: MemberChecksum(3),
            },
        }
    }

    fn mutation(self) -> Mutation {
        let keyspace = KeySpace(self.keyspace);
        let key = self.key();
        if self.op == Op::PublishDelete {
            Mutation::Delete { keyspace, key }
        } else {
            let byte = self.key_id.to_le_bytes()[0] ^ self.version.to_le_bytes()[0];
            Mutation::Put {
                keyspace,
                key,
                value: Bytes::from(vec![byte; self.value_len]),
            }
        }
    }
}

fn invalid_trace(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// TSV has one header and six columns. No escaping or comments are used.
/// Lengths and ids are decimal. One immutable identity must keep the same shape.
fn read_trace(reader: impl BufRead) -> io::Result<Vec<Event>> {
    let mut lines = reader.lines();
    if lines.next().transpose()?.as_deref() != Some(TRACE_HEADER) {
        return Err(invalid_trace(
            "expected TSV header: op keyspace key_id key_len value_len version",
        ));
    }
    let mut events = Vec::new();
    let mut shapes = HashMap::new();
    for (index, line) in lines.enumerate() {
        let line = line?;
        let columns: Vec<_> = line.split('\t').collect();
        if columns.len() != 6 {
            return Err(invalid_trace(&format!(
                "line {}: expected six TSV columns",
                index + 2
            )));
        }
        let number = |column: usize| -> io::Result<u64> {
            columns[column]
                .parse()
                .map_err(|_| invalid_trace(&format!("line {}: invalid number", index + 2)))
        };
        let event = Event {
            op: Op::parse(columns[0])?,
            keyspace: u8::try_from(number(1)?).map_err(|_| invalid_trace("keyspace exceeds u8"))?,
            key_id: number(2)?,
            key_len: usize::try_from(number(3)?)
                .map_err(|_| invalid_trace("key length exceeds usize"))?,
            value_len: usize::try_from(number(4)?)
                .map_err(|_| invalid_trace("value length exceeds usize"))?,
            version: number(5)?,
        };
        if event.op == Op::PublishDelete && event.value_len != 0 {
            return Err(invalid_trace("delete value length must be zero"));
        }
        if event
            .key_len
            .checked_add(event.value_len)
            .and_then(|len| len.checked_add(32))
            .is_none_or(|len| u32::try_from(len).is_err())
        {
            return Err(invalid_trace("member length exceeds u32"));
        }
        let shape = (event.key_len, event.value_len);
        if shapes
            .insert(event.identity(), shape)
            .is_some_and(|previous| previous != shape)
        {
            return Err(invalid_trace(
                "immutable reference has inconsistent lengths",
            ));
        }
        events.push(event);
    }
    Ok(events)
}

fn write_trace(mut writer: impl Write, events: &[Event]) -> io::Result<()> {
    writeln!(writer, "{TRACE_HEADER}")?;
    for event in events {
        writeln!(
            writer,
            "{}\t{}\t{}\t{}\t{}\t{}",
            event.op.name(),
            event.keyspace,
            event.key_id,
            event.key_len,
            event.value_len,
            event.version
        )?;
    }
    Ok(())
}

#[derive(Debug, PartialEq)]
struct Evaluation {
    lookups: u64,
    hits: u64,
    misses: u64,
    requested_value_bytes: u64,
    hit_value_bytes: u64,
    fills: u64,
    evictions: u64,
    admission_rejections: u64,
    oversize_rejections: u64,
    eviction_limit_rejections: u64,
    entries: usize,
    retained_entry_bytes: usize,
    metadata_bytes: usize,
    slots: usize,
    bytes_per_entry: f64,
    budget_invariants_checked: bool,
    per_keyspace: [PerKeyspaceReadCacheStats; KEYSPACE_COUNTER_COUNT],
}

impl Evaluation {
    fn hit_ratio(&self) -> f64 {
        ratio(self.hits, self.lookups)
    }

    fn byte_hit_ratio(&self) -> f64 {
        ratio(self.hit_value_bytes, self.requested_value_bytes)
    }

    fn rejections(&self) -> u64 {
        self.admission_rejections + self.oversize_rejections + self.eviction_limit_rejections
    }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn slot_metadata() -> usize {
    FIXED_SLOT_BYTES
}

/// Replay is serial. Scan misses do not fill and scan hits do not promote, as in
/// `read_pipeline::finish_scan_page`. Publication warms Put and Delete records.
fn replay(events: &[Event], warmup: usize, budget: usize, seed: u64) -> Evaluation {
    assert!(warmup <= events.len());
    let cache = ReadCache::new_seeded(budget, seed);
    let mut before = cache.stats();
    let mut before_keyspace = cache.per_keyspace_stats();
    let mut requested_value_bytes = 0;
    let mut hit_value_bytes = 0;
    for (index, &event) in events.iter().enumerate() {
        if index == warmup {
            before = cache.stats();
            before_keyspace = cache.per_keyspace_stats();
        }
        let reference = event.reference();
        let hit = match event.op {
            Op::Get => {
                let hit = cache
                    .get_point(&reference, KeySpace(event.keyspace), &event.key())
                    .is_some();
                if !hit {
                    let prepared = cache.prepare_verified(reference, &event.mutation());
                    cache.insert_prepared(prepared);
                }
                Some(hit)
            }
            Op::ScanGet => Some(cache.get_scan(&reference).is_some()),
            Op::PublishPut | Op::PublishDelete => {
                let prepared = cache.prepare_post_publish(reference, &event.mutation());
                cache.insert_prepared(prepared);
                None
            }
        };
        if index >= warmup
            && let Some(hit) = hit
        {
            requested_value_bytes += u64::try_from(event.value_len).unwrap();
            if hit {
                hit_value_bytes += u64::try_from(event.value_len).unwrap();
            }
        }
    }
    if warmup == events.len() {
        before = cache.stats();
        before_keyspace = cache.per_keyspace_stats();
    }
    let after = cache.stats();
    let mut per_keyspace = cache.per_keyspace_stats();
    for (after, before) in per_keyspace.iter_mut().zip(before_keyspace) {
        after.hits -= before.hits;
        after.misses -= before.misses;
        after.inserts -= before.inserts;
        after.evictions -= before.evictions;
    }
    let slots = after.metadata_bytes / slot_metadata();
    assert!(after.retained_bytes <= budget);
    assert!(after.entries <= slots);
    assert_eq!(after.rejected_mismatch, 0);
    let hits = after.hits - before.hits;
    let misses = after.misses - before.misses;
    assert_eq!(
        hits,
        per_keyspace.iter().map(|stats| stats.hits).sum::<u64>()
    );
    assert_eq!(
        misses,
        per_keyspace.iter().map(|stats| stats.misses).sum::<u64>()
    );
    assert_eq!(
        after.inserts - before.inserts,
        per_keyspace.iter().map(|stats| stats.inserts).sum::<u64>()
    );
    assert_eq!(
        after.evictions - before.evictions,
        per_keyspace
            .iter()
            .map(|stats| stats.evictions)
            .sum::<u64>()
    );
    for shard in cache.shards.iter() {
        let stats = shard
            .counters
            .snapshot(cache.shard_budget_bytes, cache.shard_metadata_bytes);
        assert!(stats.retained_bytes <= cache.shard_budget_bytes);
        assert!(stats.entries <= cache.shard_metadata_bytes / slot_metadata());
    }
    Evaluation {
        lookups: hits + misses,
        hits,
        misses,
        requested_value_bytes,
        hit_value_bytes,
        fills: after.inserts - before.inserts,
        evictions: after.evictions - before.evictions,
        admission_rejections: after.bypass_admission - before.bypass_admission,
        oversize_rejections: after.rejected_oversize - before.rejected_oversize,
        eviction_limit_rejections: after.bypass_eviction_limit - before.bypass_eviction_limit,
        entries: after.entries,
        retained_entry_bytes: after.retained_bytes - after.metadata_bytes,
        metadata_bytes: after.metadata_bytes,
        slots,
        bytes_per_entry: ratio(
            u64::try_from(after.retained_bytes).unwrap(),
            u64::try_from(after.entries).unwrap(),
        ),
        budget_invariants_checked: true,
        per_keyspace,
    }
}

#[derive(Clone, Copy)]
struct Rng(u64);

fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        mix(self.0)
    }

    fn unit(&mut self) -> f64 {
        // Use 53 random bits, so the result is in [0, 1).
        (self.next() >> 11) as f64 / 9_007_199_254_740_992.0
    }
}

#[derive(Clone, Copy, Debug)]
enum SizeModel {
    Small64,
    Small256,
    Kib,
    SmallMix,
    Skewed,
    SuiOutput,
}

const SIZE_MODELS: [SizeModel; 6] = [
    SizeModel::Small64,
    SizeModel::Small256,
    SizeModel::Kib,
    SizeModel::SmallMix,
    SizeModel::Skewed,
    SizeModel::SuiOutput,
];

impl SizeModel {
    fn name(self) -> &'static str {
        match self {
            Self::Small64 => "64b",
            Self::Small256 => "256b",
            Self::Kib => "1kib",
            Self::SmallMix => "small_mix",
            Self::Skewed => "skewed",
            Self::SuiOutput => "sui_output_quantiles",
        }
    }

    fn mean_value_bytes(self) -> usize {
        match self {
            Self::Small64 => 64,
            Self::Small256 => 256,
            Self::Kib => 1024,
            Self::SmallMix => 160,
            Self::Skewed => (4 * 640 + 36 * 1024) / 5,
            Self::SuiOutput => 869,
        }
    }

    fn size(self, quantile: f64) -> usize {
        match self {
            Self::Small64 => 64,
            Self::Small256 => 256,
            Self::Kib => 1024,
            Self::SmallMix => {
                if quantile < 0.5 {
                    64
                } else {
                    256
                }
            }
            Self::Skewed => {
                // Uniform integer sizes within each count-weighted interval.
                if quantile < 0.8 {
                    256 + ((quantile / 0.8 * 769.0).floor() as usize).min(768)
                } else {
                    8192 + (((quantile - 0.8) / 0.2 * 57_345.0).floor() as usize).min(57_344)
                }
            }
            Self::SuiOutput => {
                // Piecewise-linear quantile fit to workload-model.json:
                // p50=332, p90=1129, p99=6506, p999=7656, max=132431.
                // The 64-byte lower endpoint is an assumption, not a sample.
                let knots = [
                    (0.0, 64.0),
                    (0.5, 332.0),
                    (0.9, 1129.0),
                    (0.99, 6506.0),
                    (0.999, 7656.0),
                    (1.0, 132_431.0),
                ];
                let pair = knots.windows(2).find(|pair| quantile <= pair[1].0).unwrap();
                (pair[0].1
                    + (pair[1].1 - pair[0].1) * (quantile - pair[0].0) / (pair[1].0 - pair[0].0))
                    .round() as usize
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Correlation {
    Independent,
    HotSmall,
    HotLarge,
}

const CORRELATIONS: [Correlation; 3] = [
    Correlation::Independent,
    Correlation::HotSmall,
    Correlation::HotLarge,
];

impl Correlation {
    fn name(self) -> &'static str {
        match self {
            Self::Independent => "independent",
            Self::HotSmall => "hot_small",
            Self::HotLarge => "hot_large",
        }
    }
}

#[derive(Clone, Copy)]
struct Generator {
    universe: usize,
    warmup: usize,
    evaluation: usize,
    alpha: f64,
    model: SizeModel,
    correlation: Correlation,
    scan_len: usize,
    warming_fraction: f64,
    seed: u64,
}

struct Trace {
    events: Vec<Event>,
    warmup: usize,
}

fn generate(config: Generator) -> Trace {
    assert!(config.universe > 0);
    assert!((0.0..=1.0).contains(&config.warming_fraction));
    let mut rng = Rng(config.seed);
    let mut cdf = Vec::with_capacity(config.universe);
    let mut sum = 0.0;
    for rank in 1..=config.universe {
        sum += 1.0 / (rank as f64).powf(config.alpha);
        cdf.push(sum);
    }
    let mut quantiles: Vec<_> = (0..config.universe)
        .map(|rank| (rank as f64 + 0.5) / config.universe as f64)
        .collect();
    match config.correlation {
        Correlation::Independent => {
            // Shuffle size ranks without changing the count distribution.
            for index in (1..quantiles.len()).rev() {
                let target =
                    usize::try_from(rng.next() % u64::try_from(index + 1).unwrap()).unwrap();
                quantiles.swap(index, target);
            }
        }
        Correlation::HotSmall => {}
        Correlation::HotLarge => quantiles.reverse(),
    }
    let sizes: Vec<_> = quantiles
        .into_iter()
        .map(|q| config.model.size(q))
        .collect();
    let mut seen = vec![false; config.universe];
    let mut events = Vec::with_capacity(config.warmup + config.evaluation + config.scan_len);
    let mut warmup = 0;
    for index in 0..config.warmup + config.evaluation {
        if index == config.warmup {
            warmup = events.len();
            // Scan a disjoint one-pass range at the start of measurement.
            for key in 0..config.scan_len {
                events.push(Event {
                    op: Op::ScanGet,
                    keyspace: 0,
                    key_id: u64::try_from(config.universe + key).unwrap(),
                    key_len: 32,
                    value_len: sizes[key % sizes.len()],
                    version: 1,
                });
            }
        }
        let target = rng.unit() * sum;
        let key = cdf
            .partition_point(|value| *value <= target)
            .min(config.universe - 1);
        let event = Event {
            op: Op::Get,
            keyspace: 0,
            key_id: u64::try_from(key).unwrap(),
            key_len: 32,
            value_len: sizes[key],
            version: 1,
        };
        if !seen[key] {
            if rng.unit() < config.warming_fraction {
                events.push(Event {
                    op: Op::PublishPut,
                    ..event
                });
            }
            seen[key] = true;
        }
        events.push(event);
    }
    if config.evaluation == 0 {
        warmup = events.len();
    }
    Trace { events, warmup }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Identity(u8, u64, u64, bool);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Policy {
    Lru,
    Clock,
    WindowTinyLfu,
}

impl Policy {
    fn name(self) -> &'static str {
        match self {
            Self::Lru => "lru",
            Self::Clock => "clock",
            Self::WindowTinyLfu => "wtinylfu",
        }
    }
}

struct Sketch {
    counts: Box<[[u16; 2048]; 4]>,
    seed: u64,
    samples: usize,
    age_after: usize,
}

impl Sketch {
    fn new(seed: u64, age_after: usize) -> Self {
        Self {
            counts: Box::new([[0; 2048]; 4]),
            seed,
            samples: 0,
            age_after,
        }
    }

    fn columns(&self, key: Identity) -> [usize; 4] {
        let hash =
            mix(key.1 ^ mix(key.2) ^ (u64::from(key.0) << 32) ^ u64::from(key.3) ^ self.seed);
        std::array::from_fn(|row| {
            usize::try_from(mix(hash ^ u64::try_from(row).unwrap()) % 2048).unwrap()
        })
    }

    fn record(&mut self, key: Identity) {
        for (row, column) in self.columns(key).into_iter().enumerate() {
            self.counts[row][column] = self.counts[row][column].saturating_add(1);
        }
        self.samples += 1;
        if self.samples == self.age_after {
            for row in self.counts.iter_mut() {
                for count in row {
                    *count /= 2;
                }
            }
            self.samples = 0;
        }
    }

    fn estimate(&self, key: Identity) -> u16 {
        self.columns(key)
            .into_iter()
            .enumerate()
            .map(|(row, column)| self.counts[row][column])
            .min()
            .unwrap()
    }
}

struct Resident {
    payload: usize,
    stamp: u64,
    referenced: bool,
    window: bool,
}

/// Byte-bounded comparison controls, not production-policy replicas. Each live
/// entry pays `metadata_charge` plus key/value bytes and nonempty byte owners.
/// Thus metadata_charge=240 and a 32-byte key add 336 bytes to a nonempty value.
/// Hash maps, queues and the sketch are simulator machinery, not RSS estimates.
/// W-TinyLFU uses a 1% byte window, LRU main region, four seeded count-min rows,
/// and halves counts after max(1,024, estimated_entries * 10) point requests.
struct Simulator {
    policy: Policy,
    budget: usize,
    metadata_charge: usize,
    residents: HashMap<Identity, Resident>,
    main: BTreeSet<(u64, Identity)>,
    window: BTreeSet<(u64, Identity)>,
    clock: VecDeque<Identity>,
    sketch: Sketch,
    tick: u64,
    payload: usize,
    window_bytes: usize,
    main_bytes: usize,
    evictions: u64,
    rejections: u64,
    oversize: u64,
    fills: u64,
}

impl Simulator {
    fn new(policy: Policy, budget: usize, metadata_charge: usize, seed: u64) -> Self {
        Self {
            policy,
            budget,
            metadata_charge,
            residents: HashMap::new(),
            main: BTreeSet::new(),
            window: BTreeSet::new(),
            clock: VecDeque::new(),
            sketch: Sketch::new(
                seed,
                (budget / (metadata_charge + 256))
                    .saturating_mul(10)
                    .max(1024),
            ),
            tick: 0,
            payload: 0,
            window_bytes: 0,
            main_bytes: 0,
            evictions: 0,
            rejections: 0,
            oversize: 0,
            fills: 0,
        }
    }

    fn charge(&self, key: Identity) -> usize {
        self.residents[&key].payload + self.metadata_charge
    }

    fn contains(&self, key: Identity) -> bool {
        self.residents.contains_key(&key)
    }

    fn get(&mut self, key: Identity, promote: bool) -> bool {
        if promote && self.policy == Policy::WindowTinyLfu {
            self.sketch.record(key);
        }
        let Some(entry) = self.residents.get_mut(&key) else {
            return false;
        };
        if promote {
            entry.referenced = true;
            if self.policy != Policy::Clock {
                let order = if entry.window {
                    &mut self.window
                } else {
                    &mut self.main
                };
                order.remove(&(entry.stamp, key));
                self.tick += 1;
                entry.stamp = self.tick;
                order.insert((entry.stamp, key));
            }
        }
        true
    }

    fn remove(&mut self, key: Identity) -> Resident {
        let entry = self.residents.remove(&key).unwrap();
        let charge = entry.payload + self.metadata_charge;
        self.payload -= entry.payload;
        if entry.window {
            self.window.remove(&(entry.stamp, key));
            self.window_bytes -= charge;
        } else {
            self.main.remove(&(entry.stamp, key));
            self.main_bytes -= charge;
        }
        entry
    }

    fn add(&mut self, key: Identity, payload: usize, window: bool) {
        self.tick += 1;
        self.residents.insert(
            key,
            Resident {
                payload,
                stamp: self.tick,
                referenced: true,
                window,
            },
        );
        self.payload += payload;
        if window {
            self.window.insert((self.tick, key));
            self.window_bytes += payload + self.metadata_charge;
        } else {
            if self.policy == Policy::Clock {
                self.clock.push_back(key);
            } else {
                self.main.insert((self.tick, key));
            }
            self.main_bytes += payload + self.metadata_charge;
        }
    }

    fn evict_oldest(&mut self) {
        let key = if self.policy == Policy::Clock {
            // At most two passes: one to clear reference bits and one to evict.
            let limit = self.clock.len() * 2;
            let mut victim = None;
            for _ in 0..limit {
                let key = self.clock.pop_front().unwrap();
                let entry = self.residents.get_mut(&key).unwrap();
                if entry.referenced {
                    entry.referenced = false;
                    self.clock.push_back(key);
                } else {
                    victim = Some(key);
                    break;
                }
            }
            victim.expect("CLOCK must find a victim in two passes")
        } else {
            self.main.first().unwrap().1
        };
        self.remove(key);
        self.evictions += 1;
    }

    fn insert(&mut self, event: Event) -> bool {
        let key = event.identity();
        if self.contains(key) {
            return true;
        }
        let payload = event.payload_bytes();
        let charge = payload + self.metadata_charge;
        if charge > self.budget {
            self.oversize += 1;
            return false;
        }
        if self.policy != Policy::WindowTinyLfu {
            while self.main_bytes + charge > self.budget {
                self.evict_oldest();
            }
            self.add(key, payload, false);
        } else {
            self.add(key, payload, true);
            let window_budget = self.budget / 100;
            let main_budget = self.budget - window_budget;
            while self.window_bytes > window_budget {
                let candidate = self.window.first().unwrap().1;
                let charge = self.charge(candidate);
                let mut available = main_budget.saturating_sub(self.main_bytes);
                let mut admitted = charge <= main_budget;
                for &(_, victim) in &self.main {
                    if available >= charge {
                        break;
                    }
                    if self.sketch.estimate(candidate) <= self.sketch.estimate(victim) {
                        admitted = false;
                        break;
                    }
                    available += self.charge(victim);
                }
                let entry = self.remove(candidate);
                if admitted {
                    while self.main_bytes + charge > main_budget {
                        self.evict_oldest();
                    }
                    self.add(candidate, entry.payload, false);
                } else {
                    // A rejected new candidate was not a resident eviction.
                    if candidate != key {
                        self.evictions += 1;
                    }
                    self.rejections += 1;
                }
            }
        }
        let admitted = self.contains(key);
        if admitted {
            self.fills += 1;
        }
        assert!(self.payload + self.residents.len() * self.metadata_charge <= self.budget);
        admitted
    }
}

fn replay_control(
    events: &[Event],
    warmup: usize,
    budget: usize,
    policy: Policy,
    seed: u64,
) -> Evaluation {
    assert!(warmup <= events.len());
    let mut cache = Simulator::new(policy, budget, CONTROL_METADATA_BYTES, seed);
    let mut result = Evaluation {
        lookups: 0,
        hits: 0,
        misses: 0,
        requested_value_bytes: 0,
        hit_value_bytes: 0,
        fills: 0,
        evictions: 0,
        admission_rejections: 0,
        oversize_rejections: 0,
        eviction_limit_rejections: 0,
        entries: 0,
        retained_entry_bytes: 0,
        metadata_bytes: 0,
        slots: 0,
        bytes_per_entry: 0.0,
        budget_invariants_checked: true,
        per_keyspace: [PerKeyspaceReadCacheStats::default(); KEYSPACE_COUNTER_COUNT],
    };
    let mut before = (0, 0, 0, 0);
    for (index, &event) in events.iter().enumerate() {
        if index == warmup {
            before = (
                cache.fills,
                cache.evictions,
                cache.rejections,
                cache.oversize,
            );
        }
        let hit = match event.op {
            Op::Get | Op::ScanGet => {
                let hit = cache.get(event.identity(), event.op == Op::Get);
                if !hit && event.op == Op::Get {
                    cache.insert(event);
                }
                Some(hit)
            }
            Op::PublishPut | Op::PublishDelete => {
                cache.insert(event);
                None
            }
        };
        if index >= warmup
            && let Some(hit) = hit
        {
            result.lookups += 1;
            result.requested_value_bytes += u64::try_from(event.value_len).unwrap();
            let counters = &mut result.per_keyspace[usize::from(event.keyspace)];
            if hit {
                result.hits += 1;
                result.hit_value_bytes += u64::try_from(event.value_len).unwrap();
                counters.hits += 1;
            } else {
                result.misses += 1;
                counters.misses += 1;
            }
        }
    }
    if warmup == events.len() {
        before = (
            cache.fills,
            cache.evictions,
            cache.rejections,
            cache.oversize,
        );
    }
    result.fills = cache.fills - before.0;
    result.evictions = cache.evictions - before.1;
    result.admission_rejections = cache.rejections - before.2;
    result.oversize_rejections = cache.oversize - before.3;
    result.entries = cache.residents.len();
    result.retained_entry_bytes = cache.payload;
    result.metadata_bytes = result.entries * cache.metadata_charge;
    result.slots = result.entries;
    result.bytes_per_entry = ratio(
        u64::try_from(result.retained_entry_bytes + result.metadata_bytes).unwrap(),
        u64::try_from(result.entries).unwrap(),
    );
    assert!(result.retained_entry_bytes + result.metadata_bytes <= budget);
    result
}

fn csv_row(
    writer: &mut impl Write,
    id: &str,
    config: Generator,
    budget: usize,
    policy: &str,
    result: &Evaluation,
) -> io::Result<()> {
    let seed = config.seed;
    writeln!(
        writer,
        "{id},{},{},{},{budget},{seed},{policy},{},{:.8},{:.8},{},{},{},{},{}",
        config.model.name(),
        config.alpha,
        config.correlation.name(),
        result.lookups,
        result.hit_ratio(),
        result.byte_hit_ratio(),
        result.entries,
        result.evictions,
        result.rejections(),
        result.metadata_bytes,
        result.retained_entry_bytes
    )
}

/// Read an integer from the environment, or return `default` when the variable is unset.
fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |text| {
        text.trim()
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be a positive integer"))
    })
}

#[test]
#[ignore = "explicit CSV experiment; set TH_TRACE_EVAL_OUT"]
fn synthetic_mrc_sweep() {
    let path = std::env::var_os("TH_TRACE_EVAL_OUT")
        .expect("synthetic_mrc_sweep requires TH_TRACE_EVAL_OUT to name the output CSV");
    let mut output = io::BufWriter::new(
        std::fs::File::create(path).expect("cannot create trace evaluation CSV"),
    );
    // Window lengths and budgets can be raised for a real MRC; the defaults only bound debug-build work.
    let warmup = env_usize("TH_TRACE_EVAL_WARMUP", SWEEP_WARMUP);
    let evaluation = env_usize("TH_TRACE_EVAL_EVALUATION", SWEEP_EVALUATION);
    let budgets: Vec<usize> = std::env::var("TH_TRACE_EVAL_BUDGET_MIB").map_or_else(
        |_| vec![8 * MIB, 32 * MIB, 128 * MIB, 512 * MIB],
        |text| {
            text.split(',')
                .map(|part| part.trim().parse::<usize>().expect("budget MiB") * MIB)
                .collect()
        },
    );
    writeln!(output, "trace_id,model,alpha,correlation,budget,seed,policy,lookups,hit_ratio,byte_hit_ratio,entries,evictions,rejections,metadata_bytes,retained_bytes").unwrap();
    for model in SIZE_MODELS {
        for alpha in [0.6, 0.8, 1.0, 1.2] {
            for correlation in CORRELATIONS {
                for &budget in &budgets {
                    for seed in [11, 29, 47] {
                        // The universe is about twice an ideal live-entry cache.
                        // Budget-dependent traces are capacity stress points, not
                        // one fixed-trace MRC. Use replay() on one real trace for that.
                        let universe = 2
                            * (budget
                                / (model.mean_value_bytes()
                                    + 32
                                    + 2 * BYTE_OWNER_BYTES
                                    + CONTROL_METADATA_BYTES));
                        let config = Generator {
                            universe,
                            warmup,
                            evaluation,
                            alpha,
                            model,
                            correlation,
                            scan_len: SWEEP_SCAN,
                            warming_fraction: 0.1,
                            seed,
                        };
                        let trace = generate(config);
                        let id = format!(
                            "{}-{alpha}-{}-b{budget}-s{seed}",
                            model.name(),
                            correlation.name()
                        );
                        let controls =
                            [Policy::Lru, Policy::Clock, Policy::WindowTinyLfu].map(|policy| {
                                (
                                    policy,
                                    replay_control(
                                        &trace.events,
                                        trace.warmup,
                                        budget,
                                        policy,
                                        seed,
                                    ),
                                )
                            });
                        let result = replay(&trace.events, trace.warmup, budget, seed);
                        csv_row(&mut output, &id, config, budget, "production", &result).unwrap();
                        for (policy, result) in &controls {
                            csv_row(&mut output, &id, config, budget, policy.name(), result)
                                .unwrap();
                        }
                    }
                }
            }
        }
    }
    output.flush().unwrap();
}

/// Replay a captured file without recompilation. The file has no warm-up prefix.
#[test]
#[ignore = "set TH_TRACE_EVAL_IN and TH_TRACE_EVAL_OUT"]
fn file_trace_evaluation() {
    let input = std::env::var_os("TH_TRACE_EVAL_IN").expect("set TH_TRACE_EVAL_IN to a TSV trace");
    let output =
        std::env::var_os("TH_TRACE_EVAL_OUT").expect("set TH_TRACE_EVAL_OUT to an output CSV");
    let events = read_trace(io::BufReader::new(std::fs::File::open(input).unwrap())).unwrap();
    let mut writer = io::BufWriter::new(std::fs::File::create(output).unwrap());
    writeln!(writer, "budget,seed,policy,lookups,hit_ratio,byte_hit_ratio,entries,evictions,rejections,metadata_bytes,retained_bytes").unwrap();
    let budgets =
        std::env::var("TH_TRACE_EVAL_BUDGET_MIB").unwrap_or_else(|_| "8,32,128,512".to_owned());
    for budget in budgets
        .split(',')
        .map(|part| part.trim().parse::<usize>().expect("budget MiB") * MIB)
    {
        for seed in [11, 29, 47] {
            let result = replay(&events, 0, budget, seed);
            writeln!(
                writer,
                "{budget},{seed},production,{},{:.8},{:.8},{},{},{},{},{}",
                result.lookups,
                result.hit_ratio(),
                result.byte_hit_ratio(),
                result.entries,
                result.evictions,
                result.rejections(),
                result.metadata_bytes,
                result.retained_entry_bytes
            )
            .unwrap();
            for policy in [Policy::Lru, Policy::Clock, Policy::WindowTinyLfu] {
                let result = replay_control(&events, 0, budget, policy, seed);
                writeln!(
                    writer,
                    "{budget},{seed},{},{},{:.8},{:.8},{},{},{},{},{}",
                    policy.name(),
                    result.lookups,
                    result.hit_ratio(),
                    result.byte_hit_ratio(),
                    result.entries,
                    result.evictions,
                    result.rejections(),
                    result.metadata_bytes,
                    result.retained_entry_bytes
                )
                .unwrap();
            }
        }
    }
    writer.flush().unwrap();
}

fn event(key: u64) -> Event {
    Event {
        op: Op::Get,
        keyspace: 0,
        key_id: key,
        key_len: 32,
        value_len: 64,
        version: 1,
    }
}

#[test]
fn exact_layout_capacity_and_metadata_match_construction() {
    for budget in [128 * MIB, 1024 * MIB, 8192 * MIB] {
        let shards = ReadCache::shard_count_for(budget);
        let shard_budget = budget / shards;
        let slots = (shard_budget / (FIXED_SLOT_BYTES + MIN_ENTRY_CHARGE) / WAYS) * WAYS;
        let metadata = slots * FIXED_SLOT_BYTES;
        assert_eq!(ReadCache::metadata_bytes(budget), shards * metadata);
        assert!(metadata * 100 <= shard_budget * 8);
        let capacity =
            slots.min((shard_budget - metadata) / (ENTRY_BYTES + event(1).payload_bytes()));
        let old_slots = (shard_budget / (CONTROL_METADATA_BYTES + 1024) / WAYS) * WAYS;
        let old_capacity = old_slots
            .min((shard_budget - old_slots * CONTROL_METADATA_BYTES) / event(1).payload_bytes());
        assert!(ratio(capacity as u64, old_capacity as u64) >= 2.9);
    }
    let cache = ReadCache::new_seeded(128 * MIB, 7);
    assert_eq!(
        cache.stats().metadata_bytes,
        ReadCache::metadata_bytes(128 * MIB)
    );
    assert_eq!(
        cache
            .shards
            .iter()
            .map(|shard| shard.table.lock().slots.len())
            .sum::<usize>(),
        cache.stats().metadata_bytes / FIXED_SLOT_BYTES
    );
}

#[test]
fn largest_entry_charge_includes_the_entry_allocation() {
    let budget = 8 * MIB;
    let cache = ReadCache::new_seeded(budget, 7);
    let largest = Event {
        value_len: budget - cache.stats().metadata_bytes - ENTRY_BYTES - 32 - 2 * BYTE_OWNER_BYTES,
        ..event(1)
    };
    assert!(cache.insert_verified(largest.reference(), &largest.mutation()));
    assert_eq!(cache.stats().retained_bytes, budget);
    let oversize = Event {
        key_id: 2,
        value_len: largest.value_len + 1,
        ..largest
    };
    assert!(!cache.insert_verified(oversize.reference(), &oversize.mutation()));
    assert_eq!(cache.stats().rejected_oversize, 1);
}

#[test]
fn mixed_sizes_charge_only_live_entries_and_the_fixed_arrays() {
    let cache = ReadCache::new_seeded(MIB, 7);
    let events = [
        event(1),
        Event {
            value_len: 256,
            ..event(2)
        },
        Event {
            value_len: 1024,
            ..event(3)
        },
        Event {
            op: Op::PublishDelete,
            value_len: 0,
            ..event(4)
        },
    ];
    for event in events {
        assert!(cache.insert_verified(event.reference(), &event.mutation()));
    }
    let charge: usize = events
        .iter()
        .map(|event| event.payload_bytes() + ENTRY_BYTES)
        .sum();
    assert_eq!(cache.stats().entries, events.len());
    assert_eq!(
        cache.stats().retained_bytes,
        cache.stats().metadata_bytes + charge
    );
    cache.clear();
    assert_eq!(cache.stats().retained_bytes, cache.stats().metadata_bytes);
    assert_eq!(cache.stats().entries, 0);
    assert!(
        cache
            .shards
            .iter()
            .all(|shard| shard.table.lock().tags.iter().all(|tag| *tag == 0))
    );
}

#[test]
fn driver_counts_match_cache_stats() {
    let a = event(1);
    let overwrite = Event { version: 2, ..a };
    let trace = [
        a,
        a,
        Event {
            op: Op::PublishPut,
            ..overwrite
        },
        overwrite,
        Event {
            op: Op::PublishDelete,
            version: 3,
            value_len: 0,
            ..a
        },
        Event {
            op: Op::ScanGet,
            ..overwrite
        },
        Event {
            op: Op::ScanGet,
            ..event(2)
        },
        event(2),
        a,
    ];
    let result = replay(&trace, 0, MIB, 7);
    assert_eq!((result.lookups, result.hits, result.misses), (7, 4, 3));
    assert_eq!((result.fills, result.evictions, result.entries), (4, 0, 4));
    assert_eq!(
        (
            result.admission_rejections,
            result.oversize_rejections,
            result.eviction_limit_rejections
        ),
        (0, 0, 0)
    );
    assert_eq!(result.requested_value_bytes, 7 * 64);
    assert_eq!(result.hit_value_bytes, 4 * 64);
    assert_eq!(result.hit_ratio(), 4.0 / 7.0);
    assert_eq!(result.byte_hit_ratio(), 4.0 / 7.0);
    assert_eq!(
        result.retained_entry_bytes,
        3 * (32 + 64 + 2 * BYTE_OWNER_BYTES) + 32 + BYTE_OWNER_BYTES + 4 * ENTRY_BYTES
    );
    assert_eq!(
        result.per_keyspace[0],
        PerKeyspaceReadCacheStats {
            hits: 4,
            misses: 3,
            inserts: 4,
            evictions: 0
        }
    );
    assert!(result.budget_invariants_checked);
    assert!(result.entries <= result.slots);
    assert_eq!(
        result.bytes_per_entry,
        (result.retained_entry_bytes + result.metadata_bytes) as f64 / 4.0
    );
    assert_eq!(replay(&trace, trace.len(), MIB, 7).lookups, 0);
    let warmed = replay(&[a, a], 1, MIB, 7);
    assert_eq!((warmed.lookups, warmed.hits, warmed.fills), (1, 1, 0));
}

#[test]
fn seeded_placement_is_reproducible() {
    let mut trace = Vec::new();
    for key in (0..300).cycle().take(3000) {
        trace.push(event(key));
    }
    assert_eq!(
        replay(&trace, 300, 32 * 1024, 11),
        replay(&trace, 300, 32 * 1024, 11)
    );
    let first = ReadCache::new_seeded(MIB, 11);
    let second = ReadCache::new_seeded(MIB, 29);
    assert_ne!(
        first.shards[0]
            .table
            .lock()
            .fingerprint(&event(1).reference()),
        second.shards[0]
            .table
            .lock()
            .fingerprint(&event(1).reference())
    );
}

#[test]
fn trace_tsv_round_trip_and_errors() {
    let trace = [
        event(1),
        Event {
            op: Op::PublishPut,
            keyspace: 3,
            version: 4,
            ..event(2)
        },
        Event {
            op: Op::PublishDelete,
            value_len: 0,
            version: 5,
            ..event(2)
        },
        Event {
            op: Op::ScanGet,
            ..event(3)
        },
    ];
    let mut bytes = Vec::new();
    write_trace(&mut bytes, &trace).unwrap();
    assert_eq!(read_trace(io::Cursor::new(bytes)).unwrap(), trace);
    for text in [
        "bad header\n".to_owned(),
        format!("{TRACE_HEADER}\nget\t0\t1\t32\t64\n"),
        format!("{TRACE_HEADER}\nget\t256\t1\t32\t64\t1\n"),
        format!("{TRACE_HEADER}\npublish_delete\t0\t1\t32\t64\t1\n"),
        format!("{TRACE_HEADER}\nget\t0\t1\t32\t64\t1\nget\t0\t1\t32\t65\t1\n"),
    ] {
        assert_eq!(
            read_trace(io::Cursor::new(text)).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}

#[test]
fn lru_eviction_order() {
    let charge = slot_metadata() + event(1).payload_bytes();
    let mut cache = Simulator::new(Policy::Lru, 2 * charge, slot_metadata(), 1);
    cache.insert(event(1));
    cache.insert(event(2));
    assert!(cache.get(event(1).identity(), true));
    cache.insert(event(3));
    assert!(cache.contains(event(1).identity()));
    assert!(!cache.contains(event(2).identity()));
    assert!(cache.contains(event(3).identity()));
    assert_eq!(cache.evictions, 1);
}

#[test]
fn clock_second_chance() {
    let charge = slot_metadata() + event(1).payload_bytes();
    let mut cache = Simulator::new(Policy::Clock, 3 * charge, slot_metadata(), 1);
    cache.insert(event(1));
    cache.insert(event(2));
    cache.insert(event(3));
    cache.insert(event(4));
    // The first eviction cleared key 2 and key 3. A demand gives key 2 a chance.
    assert!(cache.get(event(2).identity(), true));
    cache.insert(event(5));
    assert!(cache.contains(event(2).identity()));
    assert!(!cache.contains(event(3).identity()));
    assert!(cache.contains(event(4).identity()));
    assert!(cache.contains(event(5).identity()));
    assert_eq!(cache.evictions, 2);
}

#[test]
fn tinylfu_admits_repeat_over_one_hit() {
    let charge = slot_metadata() + event(1).payload_bytes();
    let mut cache = Simulator::new(Policy::WindowTinyLfu, 2 * charge, slot_metadata(), 7);
    assert!(!cache.get(event(1).identity(), true));
    assert!(cache.insert(event(1)));
    assert!(!cache.get(event(2).identity(), true));
    assert!(!cache.insert(event(2)));
    assert!(!cache.get(event(2).identity(), true));
    assert!(cache.insert(event(2)));
    assert!(!cache.contains(event(1).identity()));
    assert!(cache.contains(event(2).identity()));
    assert_eq!(cache.rejections, 1);
}

#[test]
fn controls_zero_budget_oversize_scan_and_versions() {
    for policy in [Policy::Lru, Policy::Clock, Policy::WindowTinyLfu] {
        let mut zero = Simulator::new(policy, 0, slot_metadata(), 1);
        assert!(!zero.insert(event(1)));
        assert_eq!(
            (zero.oversize, zero.payload, zero.residents.len()),
            (1, 0, 0)
        );
        let charge = slot_metadata() + event(1).payload_bytes();
        let mut cache = Simulator::new(policy, charge - 1, slot_metadata(), 1);
        assert!(!cache.insert(event(1)));
        assert_eq!(cache.oversize, 1);
        let old = event(1);
        let new = Event {
            version: 2,
            op: Op::PublishPut,
            ..old
        };
        let trace = [
            old,
            new,
            Event {
                op: Op::PublishDelete,
                version: 3,
                value_len: 0,
                ..old
            },
            old,
            Event {
                op: Op::ScanGet,
                ..event(2)
            },
            event(2),
        ];
        let result = replay_control(&trace, 0, MIB, policy, 1);
        assert_eq!(
            (result.lookups, result.hits, result.misses, result.fills),
            (4, 1, 3, 4)
        );
    }
}

#[test]
fn sketch_has_seeded_counts_and_halving_aging() {
    let key = event(1).identity();
    let mut sketch = Sketch::new(7, 4);
    for _ in 0..3 {
        sketch.record(key);
    }
    assert_eq!(sketch.estimate(key), 3);
    sketch.record(key);
    assert_eq!(sketch.estimate(key), 2);
    assert_eq!(Sketch::new(7, 4).columns(key), sketch.columns(key));
    assert_ne!(Sketch::new(8, 4).columns(key), sketch.columns(key));
}

#[test]
fn generator_is_deterministic_and_respects_models() {
    let config = Generator {
        universe: 1000,
        warmup: 100,
        evaluation: 10_000,
        alpha: 0.8,
        model: SizeModel::SmallMix,
        correlation: Correlation::Independent,
        scan_len: 19,
        warming_fraction: 0.5,
        seed: 42,
    };
    let first = generate(config);
    let second = generate(config);
    assert_eq!(first.events, second.events);
    assert_eq!(first.warmup, second.warmup);
    assert_eq!(
        first
            .events
            .iter()
            .filter(|event| event.op == Op::ScanGet)
            .count(),
        19
    );
    assert_eq!(
        first.events[..first.warmup]
            .iter()
            .filter(|event| event.op == Op::Get)
            .count(),
        100
    );
    assert_eq!(
        first.events[first.warmup..]
            .iter()
            .filter(|event| event.op == Op::Get)
            .count(),
        10_000
    );
    assert!(first.events.iter().any(|event| event.op == Op::PublishPut));
    for model in SIZE_MODELS {
        for correlation in CORRELATIONS {
            let trace = generate(Generator {
                model,
                correlation,
                ..config
            });
            assert!(trace.events.iter().all(|event| match model {
                SizeModel::Small64 => event.value_len == 64,
                SizeModel::Small256 => event.value_len == 256,
                SizeModel::Kib => event.value_len == 1024,
                SizeModel::SmallMix => matches!(event.value_len, 64 | 256),
                SizeModel::Skewed =>
                    (256..=1024).contains(&event.value_len)
                        || (8192..=65536).contains(&event.value_len),
                SizeModel::SuiOutput => (64..=132_431).contains(&event.value_len),
            }));
        }
    }
    let top: Vec<_> = [0.6, 0.8, 1.0, 1.2]
        .into_iter()
        .map(|alpha| {
            generate(Generator {
                alpha,
                warming_fraction: 0.0,
                ..config
            })
            .events
            .iter()
            .filter(|event| event.op == Op::Get && event.key_id == 0)
            .count()
        })
        .collect();
    assert!(top.windows(2).all(|pair| pair[0] < pair[1]));
    for (correlation, first_size, last_size) in [
        (Correlation::HotSmall, 64, 256),
        (Correlation::HotLarge, 256, 64),
    ] {
        let trace = generate(Generator {
            correlation,
            alpha: 0.6,
            warmup: 0,
            evaluation: 100_000,
            warming_fraction: 0.0,
            scan_len: 0,
            ..config
        });
        assert_eq!(
            trace
                .events
                .iter()
                .find(|event| event.key_id == 0)
                .unwrap()
                .value_len,
            first_size
        );
        assert_eq!(
            trace
                .events
                .iter()
                .find(|event| event.key_id == 999)
                .unwrap()
                .value_len,
            last_size
        );
    }
}

#[test]
fn scans_do_not_promote_control_entries() {
    for policy in [Policy::Lru, Policy::Clock, Policy::WindowTinyLfu] {
        let mut cache = Simulator::new(policy, MIB, slot_metadata(), 7);
        cache.insert(event(1));
        let key = event(1).identity();
        let before = (
            cache.residents[&key].stamp,
            cache.residents[&key].referenced,
            cache.sketch.estimate(key),
        );
        assert!(cache.get(key, false));
        assert_eq!(
            before,
            (
                cache.residents[&key].stamp,
                cache.residents[&key].referenced,
                cache.sketch.estimate(key),
            )
        );
    }
}

#[test]
fn csv_row_preserves_evaluation_fields() {
    let config = Generator {
        universe: 2,
        warmup: 0,
        evaluation: 2,
        alpha: 1.0,
        model: SizeModel::Small64,
        correlation: Correlation::Independent,
        scan_len: 0,
        warming_fraction: 0.0,
        seed: 7,
    };
    let result = replay(&[event(1), event(1)], 0, MIB, config.seed);
    let mut output = Vec::new();
    csv_row(&mut output, "trace-1", config, MIB, "production", &result).unwrap();
    let text = String::from_utf8(output).unwrap();
    let cells: Vec<_> = text.trim_end().split(',').collect();
    assert_eq!(cells.len(), 15);
    assert_eq!(cells[0], "trace-1");
    assert_eq!(cells[4].parse::<usize>().unwrap(), MIB);
    assert_eq!(cells[5].parse::<u64>().unwrap(), config.seed);
    assert_eq!(cells[7].parse::<u64>().unwrap(), result.lookups);
    assert_eq!(cells[8].parse::<f64>().unwrap(), result.hit_ratio());
    assert_eq!(cells[9].parse::<f64>().unwrap(), result.byte_hit_ratio());
    assert_eq!(cells[10].parse::<usize>().unwrap(), result.entries);
    assert_eq!(cells[11].parse::<u64>().unwrap(), result.evictions);
    assert_eq!(cells[12].parse::<u64>().unwrap(), result.rejections());
    assert_eq!(cells[13].parse::<usize>().unwrap(), result.metadata_bytes);
    assert_eq!(
        cells[14].parse::<usize>().unwrap(),
        result.retained_entry_bytes
    );
}

/// Fixed-seed, prebuilt inputs keep input generation outside the timed interval.
/// Get measures resident scalar hits. Insert measures demand observation,
/// verified preparation, detachment, admission and insertion, including rejects.
#[test]
#[ignore = "set TH_TRACE_MICRO_OUT to the result CSV"]
fn cache_micro_throughput() {
    use std::hint::black_box;
    use std::time::Instant;
    let path = std::env::var_os("TH_TRACE_MICRO_OUT").expect("set TH_TRACE_MICRO_OUT");
    let mut output = io::BufWriter::new(std::fs::File::create(path).unwrap());
    writeln!(
        output,
        "operation,repetition,events,elapsed_ns,events_per_second,accepted"
    )
    .unwrap();
    let hit_inputs: Vec<_> = (0..64)
        .map(|key| {
            let event = event(key);
            (event.reference(), event.mutation())
        })
        .collect();
    let insert_inputs: Vec<_> = (0..100_000)
        .map(|key| {
            let event = event(key);
            (event.reference(), event.mutation())
        })
        .collect();
    for repetition in 0..7 {
        let cache = ReadCache::new_seeded(8 * MIB, 11);
        for (reference, mutation) in &hit_inputs {
            assert!(cache.insert_verified(*reference, mutation));
        }
        let started = Instant::now();
        let mut hits = 0;
        for index in 0..2_000_000 {
            let (reference, mutation) = &hit_inputs[index % hit_inputs.len()];
            hits += usize::from(
                black_box(cache.get_point(reference, mutation.keyspace(), mutation.key()))
                    .is_some(),
            );
        }
        let elapsed = started.elapsed();
        assert_eq!(hits, 2_000_000);
        writeln!(
            output,
            "get,{repetition},{hits},{},{},{}",
            elapsed.as_nanos(),
            hits as f64 / elapsed.as_secs_f64(),
            hits
        )
        .unwrap();
        let cache = ReadCache::new_seeded(8 * MIB, 11);
        let started = Instant::now();
        let mut accepted = 0;
        for (reference, mutation) in &insert_inputs {
            black_box(cache.get_point(reference, mutation.keyspace(), mutation.key()));
            accepted +=
                usize::from(cache.insert_prepared(cache.prepare_verified(*reference, mutation)));
        }
        let elapsed = started.elapsed();
        writeln!(
            output,
            "insert,{repetition},{},{},{},{}",
            insert_inputs.len(),
            elapsed.as_nanos(),
            insert_inputs.len() as f64 / elapsed.as_secs_f64(),
            accepted
        )
        .unwrap();
    }
    output.flush().unwrap();
}

/// Optimistic physical bounds: balanced shards, no reserve or collision loss.
#[test]
#[ignore = "set TH_TRACE_CAPACITY_OUT to the capacity CSV"]
fn exact_capacity_table() {
    let path = std::env::var_os("TH_TRACE_CAPACITY_OUT").expect("set TH_TRACE_CAPACITY_OUT");
    let mut writer = io::BufWriter::new(std::fs::File::create(path).unwrap());
    writeln!(writer, "budget,value_bytes,entries,slots,metadata_bytes,fixed_fraction,entry_size,entry_allocator_bytes").unwrap();
    for budget in [128 * MIB, 1024 * MIB, 8192 * MIB] {
        let shards = ReadCache::shard_count_for(budget);
        let shard_budget = budget / shards;
        let slots = CacheTable::slot_count(shard_budget);
        let metadata = CacheTable::metadata_bytes(shard_budget);
        for value_len in [64, 256, 1024] {
            let charge = ENTRY_BYTES
                + Event {
                    value_len,
                    ..event(1)
                }
                .payload_bytes();
            let entries = shards * slots.min((shard_budget - metadata) / charge);
            writeln!(
                writer,
                "{budget},{value_len},{entries},{},{},{:.9},{},{}",
                shards * slots,
                shards * metadata,
                metadata as f64 / shard_budget as f64,
                size_of::<Entry>(),
                ENTRY_ALLOCATOR_BYTES
            )
            .unwrap();
        }
    }
    writer.flush().unwrap();
}
