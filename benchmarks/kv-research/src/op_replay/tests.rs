//! Synthetic streams in the recorder's format, replayed into both arms. The
//! expected results come from an in-memory model of native semantics: lower
//! iterator bounds are inclusive, upper bounds exclusive.

use super::trace::encode::{StreamWriter, write_trace};
use super::trace::{
    FilterDecision, Member, Op, OpenSpec, Outcome, RelocationEvidence, StreamObservation,
};
use super::{
    LeasedForce, RowHasher, StreamStatus, compactor_parity, compactor_parity_with, replay_trace,
    row_hash, sha256,
};
use crate::backend::BackendResult;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tidehunter::config::Config;
use tidehunter::key_shape::{KeyIndexing, KeyShapeBuilder, KeySpaceConfig, KeyType};

const A: u32 = 0;
const B: u32 = 1;
/// Raw `(key, value)` rows in the order an iterator must return them.
type Rows = Vec<(Vec<u8>, Vec<u8>)>;
/// Rows as an engine iterator yields them.
type RowResults = Vec<BackendResult<(Vec<u8>, Vec<u8>)>>;

fn scratch(name: &str) -> BackendResult<PathBuf> {
    let path = std::env::temp_dir().join(format!(
        "op-replay-{name}-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    std::fs::create_dir_all(&path)?;
    Ok(path)
}

fn key(i: u32) -> Vec<u8> {
    let mut key = i.wrapping_mul(0x9e37_79b1).to_be_bytes().to_vec();
    key.extend_from_slice(&i.to_be_bytes());
    key
}

fn spec() -> BackendResult<OpenSpec> {
    let mut builder = KeyShapeBuilder::new();
    for name in ["a", "b"] {
        builder.add_key_space_config_indexing(
            name,
            KeyIndexing::fixed(8),
            4,
            KeyType::uniform(1),
            KeySpaceConfig::new(),
        );
    }
    let config = Config {
        frag_size: 1 << 20,
        wal_file_size: 4 << 20,
        ..Config::default()
    };
    Ok(OpenSpec {
        shape: serde_yaml::to_string(&builder.build())?,
        config: serde_yaml::to_string(&config)?,
        durable: false,
        filters: vec!["a".into()],
    })
}

/// Records operations with the results the model predicts for native.
struct Synth {
    w: StreamWriter,
    model: BTreeMap<u32, BTreeMap<Vec<u8>, Vec<u8>>>,
    /// Every put into the filtered keyspace `a`, in WAL order.
    filtered_puts: Rows,
}

impl Synth {
    fn new() -> Self {
        Self {
            w: StreamWriter::new(),
            model: BTreeMap::new(),
            filtered_puts: Vec::new(),
        }
    }

    fn put(&mut self, ks: u32, key: Vec<u8>, value: Vec<u8>) {
        if ks == A {
            self.filtered_puts.push((key.clone(), value.clone()));
        }
        self.model
            .entry(ks)
            .or_default()
            .insert(key.clone(), value.clone());
        self.w.push(
            ks,
            &Op::Put {
                key,
                value,
                result: Outcome::Ok(()),
            },
        );
    }

    fn delete(&mut self, ks: u32, key: Vec<u8>) {
        self.model.entry(ks).or_default().remove(&key);
        self.w.push(
            ks,
            &Op::Delete {
                key,
                result: Outcome::Ok(()),
            },
        );
    }

    /// Batches write only to `b`: native relocation asks the filter about WAL
    /// records, so `a` stays on single puts with one record each.
    fn batch_b(&mut self, members: Vec<Member>) {
        for member in &members {
            match member {
                Member::Put(ks, key, value) => {
                    self.model
                        .entry(*ks)
                        .or_default()
                        .insert(key.clone(), value.clone());
                }
                Member::Delete(ks, key) => {
                    self.model.entry(*ks).or_default().remove(key);
                }
            }
        }
        self.w.push(
            B,
            &Op::Batch {
                members,
                result: Outcome::Ok(()),
            },
        );
    }

    fn value(&self, ks: u32, key: &[u8]) -> Option<Vec<u8>> {
        self.model.get(&ks).and_then(|rows| rows.get(key)).cloned()
    }

    fn reads(&mut self, ks: u32, keys: &[Vec<u8>]) {
        for key in keys {
            let value = self.value(ks, key);
            let exists = value.is_some();
            self.w.push(
                ks,
                &Op::Get {
                    key: key.clone(),
                    result: Outcome::Ok(value),
                },
            );
            self.w.push(
                ks,
                &Op::Exists {
                    key: key.clone(),
                    result: Outcome::Ok(exists),
                },
            );
        }
        let values = keys
            .iter()
            .map(|key| Outcome::Ok(self.value(ks, key)))
            .collect();
        let exists = keys
            .iter()
            .map(|key| self.value(ks, key).is_some())
            .collect();
        self.w.push(
            ks,
            &Op::MultiGet {
                keys: keys.to_vec(),
                result: values,
            },
        );
        self.w.push(
            ks,
            &Op::MultiExists {
                keys: keys.to_vec(),
                result: Outcome::Ok(exists),
            },
        );
    }

    fn rows(&self, ks: u32, lower: Option<&[u8]>, upper: Option<&[u8]>, reverse: bool) -> Rows {
        let mut rows: Vec<_> = self
            .model
            .get(&ks)
            .into_iter()
            .flatten()
            .filter(|(key, _)| lower.is_none_or(|lower| key.as_slice() >= lower))
            .filter(|(key, _)| upper.is_none_or(|upper| key.as_slice() < upper))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        if reverse {
            rows.reverse();
        }
        rows
    }

    /// Begin an iterator; returns its ID and the rows it must return.
    fn begin(
        &mut self,
        ks: u32,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        reverse: bool,
        snapshot: bool,
    ) -> (u64, Rows) {
        let rows = self.rows(ks, lower.as_deref(), upper.as_deref(), reverse);
        let id = self.w.frames;
        self.w.push(
            ks,
            &Op::IteratorBegin {
                id,
                lower,
                upper,
                reverse,
                snapshot,
            },
        );
        (id, rows)
    }

    /// Step `take` rows (and the observed end if `take` exceeds them), then end.
    fn steps(&mut self, ks: u32, id: u64, rows: &[(Vec<u8>, Vec<u8>)], take: usize) -> RowHasher {
        let mut hasher = RowHasher::new();
        for (key, value) in rows.iter().take(take) {
            hasher.row(key, value);
            self.w.push(
                ks,
                &Op::IteratorStep {
                    id,
                    result: Outcome::Ok(Some(row_hash(key, value))),
                },
            );
        }
        if take > rows.len() {
            self.w.push(
                ks,
                &Op::IteratorStep {
                    id,
                    result: Outcome::Ok(None),
                },
            );
        }
        hasher
    }

    fn scan(
        &mut self,
        ks: u32,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        reverse: bool,
        take: usize,
    ) {
        let (id, rows) = self.begin(ks, lower, upper, reverse, false);
        let hasher = self.steps(ks, id, &rows, take);
        let count = rows.len().min(take) as u64;
        self.w.push(
            ks,
            &Op::IteratorEnd {
                id,
                count,
                sha256: hasher.finish(),
            },
        );
    }

    /// A relocation fence whose recorded decisions cover every filtered put
    /// in WAL order. Native removes nothing: no WAL file lies below the cut.
    /// Version 2 adds placeholder evidence: it is reported, not compared.
    fn fence(&mut self, remove: Option<&[u8]>) {
        let v2 = self.w.version == 2;
        let decisions = self
            .filtered_puts
            .iter()
            .map(|(key, value)| FilterDecision {
                keyspace: "a".into(),
                key: key.clone(),
                value_sha256: sha256(value),
                wal_position: v2.then_some(0),
                decision: u8::from(remove == Some(key.as_slice())),
            })
            .collect();
        let evidence = v2.then(|| RelocationEvidence {
            gate: 0,
            streams: vec![StreamObservation {
                partition: 0,
                fencing_generation: 0,
                processed_upper: 0,
                terminal_position: 0,
                rounded_target: 0,
            }],
            copied_survivors: 0,
            witness: 0,
        });
        self.w.database(&Op::RelocationStart);
        self.w.database(&Op::RelocationComplete {
            decisions,
            removed: Vec::new(),
            evidence,
        });
    }
}

/// Every operation kind except drop, with a close/reopen in the middle.
fn synthetic_stream(remove_at_fence: Option<u32>) -> BackendResult<StreamWriter> {
    synthetic_stream_version(remove_at_fence, 1)
}

fn synthetic_stream_version(
    remove_at_fence: Option<u32>,
    version: u8,
) -> BackendResult<StreamWriter> {
    let mut s = Synth::new();
    s.w = StreamWriter::with_version(version);
    s.w.database(&Op::Create(spec()?));
    for i in 0..40u8 {
        s.put(A, key(u32::from(i)), vec![i; 64 + usize::from(i)]);
    }
    s.put(A, key(3), b"overwritten".to_vec());
    s.delete(A, key(4));
    s.batch_b(
        (0..30)
            .map(|i| Member::Put(B, key(i), vec![0xb0; 32]))
            .collect(),
    );
    s.batch_b(vec![
        Member::Delete(B, key(1)),
        Member::Put(B, key(100), b"late".to_vec()),
    ]);
    let probe: Vec<_> = [0, 3, 4, 39, 1000].into_iter().map(key).collect();
    s.reads(A, &probe);
    s.reads(B, &[key(1), key(2), key(100)]);
    s.scan(A, None, None, false, usize::MAX);
    s.scan(A, Some(key(10)), Some(key(20)), true, usize::MAX);
    s.scan(B, Some(key(5)), None, false, 3);

    // A snapshot iterator does not observe a put after its creation.
    let (id, rows) = s.begin(A, None, None, false, true);
    let mut hasher = s.steps(A, id, &rows, 1);
    s.put(A, key(41), b"after-snapshot".to_vec());
    for (key, value) in rows.iter().skip(1) {
        hasher.row(key, value);
        s.w.push(
            A,
            &Op::IteratorStep {
                id,
                result: Outcome::Ok(Some(row_hash(key, value))),
            },
        );
    }
    s.w.push(
        A,
        &Op::IteratorStep {
            id,
            result: Outcome::Ok(None),
        },
    );
    s.w.push(
        A,
        &Op::IteratorEnd {
            id,
            count: u64::try_from(rows.len())?,
            sha256: hasher.finish(),
        },
    );

    s.w.database(&Op::Flush {
        result: Outcome::Ok(()),
    });
    s.w.database(&Op::Checkpoint {
        result: Outcome::Ok(()),
    });
    s.w.database(&Op::RebuildControl {
        result: Outcome::Ok(()),
    });
    let removed = remove_at_fence.map(key);
    s.fence(removed.as_deref());
    s.reads(A, &probe);
    s.w.database(&Op::Close);

    s.w.database(&Op::Open(spec()?));
    s.reads(A, &probe);
    s.w.push(
        B,
        &Op::DropCells {
            from: vec![0; 8],
            to: vec![0xff; 8],
            result: Outcome::Ok(()),
        },
    );
    s.model.remove(&B);
    s.reads(B, &[key(2), key(100)]);
    s.scan(B, None, None, false, usize::MAX);
    s.put(A, key(42), b"after-reopen".to_vec());
    s.scan(A, Some(key(40)), None, false, usize::MAX);
    s.w.database(&Op::Close);
    Ok(s.w)
}

fn replay(name: &str, stream: &StreamWriter) -> BackendResult<(PathBuf, super::TraceReport)> {
    let root = scratch(name)?;
    let trace = root.join("trace");
    write_trace(&trace, &[("authorities_db/epoch_0", stream)])?;
    let report = replay_trace(&trace, &root.join("work"))?;
    Ok((root, report))
}

fn only(report: &super::TraceReport) -> BackendResult<&super::StreamReport> {
    match report.streams.as_slice() {
        [stream] => Ok(stream),
        streams => Err(format!("expected one stream, got {}", streams.len()).into()),
    }
}

fn first_fence(stream: &super::StreamReport) -> BackendResult<&super::stream::Fence> {
    stream
        .fences
        .first()
        .ok_or_else(|| "no relocation fence".into())
}

fn pass(arm: Option<&super::stream::ArmPass>) -> BackendResult<&super::stream::ArmPass> {
    arm.ok_or_else(|| "relocation pass missing".into())
}

#[test]
fn synthetic_stream_matches_native_and_leased_with_reopen() -> BackendResult<()> {
    let (root, report) = replay("synthetic", &synthetic_stream(None)?)?;
    let stream = only(&report)?;
    let pretty = serde_json::to_string_pretty(&report)?;
    assert_eq!(stream.status, StreamStatus::Passed, "{pretty}");
    assert!(report.passed);
    assert_eq!(stream.replayed_frames, stream.frames);
    assert_eq!(stream.final_digests, stream.reopen_digests);
    assert!(
        stream
            .final_digests
            .as_ref()
            .is_some_and(|digests| digests.len() == 2)
    );
    assert!(!stream.gaps.contains_key("relocation_fence"));
    assert!(!stream.gaps.contains_key("drop_cells"));
    assert!(stream.gaps.contains_key("typed_store_checkpoint"));
    let fence = first_fence(stream)?;
    let native = pass(fence.native.as_ref())?;
    assert!(native.filters.ordered_match);
    assert_eq!(native.filters.unrecorded_candidates, 0);
    assert_eq!(
        pass(fence.leased.as_ref())?.filters.unrecorded_candidates,
        0
    );
    std::fs::remove_dir_all(root)?;
    Ok(())
}

/// A version 2 trace replays with the same oracle; its native evidence is
/// reported on the fence and listed as an uncompared gap until R1.
#[test]
fn version_two_stream_replays_and_reports_uncompared_evidence() -> BackendResult<()> {
    let (root, report) = replay("synthetic-v2", &synthetic_stream_version(None, 2)?)?;
    let stream = only(&report)?;
    assert_eq!(stream.status, StreamStatus::Passed, "{stream:?}");
    assert!(stream.gaps.contains_key("exact_r1_evidence"));
    let fence = first_fence(stream)?;
    assert!(fence.recorded_evidence.is_some());
    assert!(pass(fence.native.as_ref())?.filters.ordered_match);
    std::fs::remove_dir_all(root)?;
    Ok(())
}

/// With R1 logical the leased pass removes, like native, only below a
/// whole-file cut: the recorded native removed set (empty here) and the leased
/// pass agree. Before R1 the leased pass deleted every live `Remove` row.
#[test]
fn leased_relocation_matches_native_removed_set_after_r1() -> BackendResult<()> {
    let (root, report) = replay("remove", &synthetic_stream(Some(7))?)?;
    let stream = only(&report)?;
    assert_eq!(stream.status, StreamStatus::Passed, "{stream:?}");
    let fence = first_fence(stream)?;
    assert_eq!(pass(fence.native.as_ref())?.removed, 0);
    let leased = pass(fence.leased.as_ref())?;
    assert!(leased.extra_removals.is_empty(), "{leased:?}");
    assert!(stream.divergences.is_empty(), "{:?}", stream.divergences);
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn drop_completes_on_both_arms() -> BackendResult<()> {
    let mut s = Synth::new();
    s.w.database(&Op::Create(spec()?));
    s.put(A, key(1), b"x".to_vec());
    s.w.database(&Op::Close);
    s.w.database(&Op::Drop {
        result: Outcome::Ok(()),
    });
    let (root, report) = replay("drop", &s.w)?;
    let stream = only(&report)?;
    assert_eq!(stream.status, StreamStatus::Passed, "{stream:?}");
    assert!(stream.divergences.is_empty());
    assert_eq!(stream.replayed_frames, stream.frames);
    std::fs::remove_dir_all(root)?;
    Ok(())
}

/// A live iterator stepped after an interleaved write is its own divergence
/// class: the recorded rows here omit the write, and whichever arm observes it
/// is reported under `live_iterator_after_write`, never as a plain step.
#[test]
fn live_iterator_after_write_is_a_named_class() -> BackendResult<()> {
    let mut s = Synth::new();
    s.w.database(&Op::Create(spec()?));
    for i in 0..4 {
        s.put(A, key(i), b"before".to_vec());
    }
    let (id, rows) = s.begin(A, None, None, false, false);
    let mut hasher = s.steps(A, id, &rows, 1);
    s.put(B, key(9), b"other keyspace".to_vec());
    s.put(A, key(10), b"interleaved".to_vec());
    for (key, value) in rows.iter().skip(1) {
        hasher.row(key, value);
        let result = Outcome::Ok(Some(row_hash(key, value)));
        s.w.push(A, &Op::IteratorStep { id, result });
    }
    s.w.push(
        A,
        &Op::IteratorStep {
            id,
            result: Outcome::Ok(None),
        },
    );
    let count = u64::try_from(rows.len())?;
    s.w.push(
        A,
        &Op::IteratorEnd {
            id,
            count,
            sha256: hasher.finish(),
        },
    );
    s.w.database(&Op::Close);
    let (root, report) = replay("live-iterator", &s.w)?;
    let stream = only(&report)?;
    assert!(
        stream.gaps.contains_key("live_iterator_after_write"),
        "{stream:?}"
    );
    assert!(
        stream
            .divergences
            .iter()
            .all(|divergence| divergence.op == "live_iterator_after_write")
    );
    std::fs::remove_dir_all(root)?;
    Ok(())
}

/// Final and reopen digests sort rows by raw key: the engine iterates in
/// reduced-key order, so the same rows in another order give the same digest,
/// and a raw key returned twice is an error.
#[test]
fn canonical_digest_ignores_iteration_order_and_rejects_duplicates() -> BackendResult<()> {
    let rows = |order: &[u8]| -> RowResults {
        order
            .iter()
            .map(|i| Ok((vec![*i; 4], vec![*i; 2])))
            .collect()
    };
    let forward = super::arm::canonical_digest(rows(&[1, 2, 3]))?;
    assert_eq!(forward, super::arm::canonical_digest(rows(&[3, 1, 2]))?);
    assert_eq!(forward.0, 3);
    assert_ne!(forward, super::arm::canonical_digest(rows(&[1, 2]))?);
    assert!(super::arm::canonical_digest(rows(&[1, 2, 1])).is_err());
    Ok(())
}

/// A keyspace whose key reduction is a non-leading byte range: index order
/// differs from raw key order. Reads, close and reopen must match on both arms.
#[test]
fn non_prefix_key_reduction_matches_on_both_arms() -> BackendResult<()> {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space_config_indexing(
        "r",
        KeyIndexing::key_reduction(16, 8..16),
        4,
        KeyType::uniform(1),
        KeySpaceConfig::new(),
    );
    let config = Config {
        frag_size: 1 << 20,
        wal_file_size: 4 << 20,
        ..Config::default()
    };
    let spec = OpenSpec {
        shape: serde_yaml::to_string(&builder.build())?,
        config: serde_yaml::to_string(&config)?,
        durable: false,
        filters: Vec::new(),
    };
    // Raw keys descend in bytes 0..8 while the reduced bytes 8..16 are spread.
    let raw = |i: u64| {
        let mut key = (1000 - i).to_be_bytes().to_vec();
        key.extend_from_slice(&i.wrapping_mul(0x9e37_79b9_7f4a_7c15).to_be_bytes());
        key
    };
    let mut s = Synth::new();
    s.w.database(&Op::Create(spec.clone()));
    for i in 0..32 {
        s.put(A, raw(i), i.to_be_bytes().to_vec());
    }
    s.delete(A, raw(5));
    let probe: Vec<_> = [0, 5, 31, 900].into_iter().map(raw).collect();
    s.reads(A, &probe);
    s.w.database(&Op::Close);
    s.w.database(&Op::Open(spec));
    s.reads(A, &probe);
    s.w.database(&Op::Close);
    let (root, report) = replay("reduction", &s.w)?;
    let stream = only(&report)?;
    assert_eq!(stream.status, StreamStatus::Passed, "{stream:?}");
    assert_eq!(stream.final_digests, stream.reopen_digests);
    let rows = stream
        .final_digests
        .as_ref()
        .and_then(|digests| digests.first());
    assert_eq!(rows.map(|digest| digest.rows), Some(31));
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn wrong_recorded_read_is_reported_on_both_arms() -> BackendResult<()> {
    let mut s = Synth::new();
    s.w.database(&Op::Create(spec()?));
    s.put(A, key(1), b"x".to_vec());
    s.w.push(
        A,
        &Op::Get {
            key: key(1),
            result: Outcome::Ok(Some(b"y".to_vec())),
        },
    );
    s.w.database(&Op::Close);
    let (root, report) = replay("mismatch", &s.w)?;
    let stream = only(&report)?;
    assert_eq!(stream.status, StreamStatus::Diverged);
    let arms: Vec<_> = stream
        .divergences
        .iter()
        .map(|divergence| divergence.arm)
        .collect();
    assert_eq!(arms, ["native", "leased"]);
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn objects_compactor_keeps_latest_versions_on_both_arms() -> BackendResult<()> {
    let root = scratch("compactor")?;
    let report = compactor_parity(&root.join("work"))?;
    let json = serde_json::to_string(&report)?;
    assert!(report.passed, "{json}");
    // Raw model before the force; exact raw winners after it and after reopen.
    assert_eq!(report.native.rows_before, report.versions_written);
    assert_eq!(report.leased.rows_before, report.versions_written);
    assert_eq!(report.model_rows, usize::from(64u8));
    for arm in [&report.native, &report.leased] {
        assert_eq!(arm.rows_after_compaction, report.model_rows);
        assert_eq!(arm.rows_after_reopen, report.model_rows);
        assert!(arm.invocations > 0);
        assert_eq!(arm.invocations_after_reopen, 0);
    }
    let witness = report
        .leased_witness
        .as_ref()
        .ok_or("leased adoption witness missing")?;
    assert_eq!(
        witness.evaluated_cells,
        u64::try_from(report.leased.invocations)?
    );
    assert_eq!(
        usize::try_from(witness.keys_marked)?,
        report.versions_written - report.model_rows
    );
    eprintln!("compactor-parity {json}");
    std::fs::remove_dir_all(root)?;
    Ok(())
}

/// Negative control: the earlier leased behavior (an Exact checkpoint that
/// never evaluates the policy) keeps the old rows and fails the raw oracle.
#[test]
fn exact_only_leased_forcing_fails_the_raw_compactor_oracle() -> BackendResult<()> {
    let root = scratch("compactor-negative")?;
    let report = compactor_parity_with(&root.join("work"), LeasedForce::ExactOnly)?;
    assert!(!report.passed);
    assert_eq!(report.leased.rows_after_compaction, report.versions_written);
    assert_eq!(report.leased.invocations, 0);
    assert!(
        report
            .problems
            .iter()
            .any(|problem| problem.starts_with("leased after compaction: raw rows differ")),
        "{:?}",
        report.problems
    );
    std::fs::remove_dir_all(root)?;
    Ok(())
}

/// Replay a recorded golden trace: `OP_REPLAY_TRACE_DIR` (with `index.json`)
/// into the fresh `OP_REPLAY_WORK_DIR`; the report is `op-replay.json` there.
/// Each stream's fresh working database is removed after the stream closes.
#[test]
#[ignore = "needs a recorded trace; set OP_REPLAY_TRACE_DIR and OP_REPLAY_WORK_DIR"]
fn replay_recorded_trace() -> BackendResult<()> {
    let var = |name: &str| -> BackendResult<PathBuf> {
        std::env::var_os(name)
            .map(PathBuf::from)
            .ok_or_else(|| format!("{name} is required").into())
    };
    let trace = var("OP_REPLAY_TRACE_DIR")?;
    let work = var("OP_REPLAY_WORK_DIR")?;
    let report = super::replay_trace_inner(Path::new(&trace), Path::new(&work), true)?;
    for stream in &report.streams {
        eprintln!(
            "{} {:?} frames={}/{} divergences={} gaps={:?}",
            stream.database,
            stream.status,
            stream.replayed_frames,
            stream.frames,
            stream.divergence_count,
            stream.gaps.keys().collect::<Vec<_>>()
        );
    }
    assert!(
        report.passed,
        "see {}",
        work.join("op-replay.json").display()
    );
    Ok(())
}
