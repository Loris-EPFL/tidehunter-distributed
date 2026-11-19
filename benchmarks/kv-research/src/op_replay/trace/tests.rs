use super::encode::{StreamWriter, frame, payload};
use super::{
    DATABASE, FilterDecision, Index, Member, Op, OpenSpec, Outcome, Reader, RelocationEvidence,
    StreamObservation, magic,
};
use crate::backend::BackendResult;
use std::path::PathBuf;

fn scratch(name: &str) -> BackendResult<PathBuf> {
    let path = std::env::temp_dir().join(format!(
        "op-replay-trace-{name}-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    std::fs::create_dir_all(&path)?;
    Ok(path)
}

/// The error text of an expected failure.
fn failure<T: std::fmt::Debug>(result: BackendResult<T>) -> BackendResult<String> {
    match result {
        Ok(value) => Err(format!("expected an error, got {value:?}").into()),
        Err(error) => Ok(error.to_string()),
    }
}

fn every_kind() -> Vec<Op> {
    let spec = OpenSpec {
        shape: "key_spaces: []".into(),
        config: "{}".into(),
        durable: true,
        filters: vec!["a".into(), "b".into()],
    };
    vec![
        Op::Create(spec.clone()),
        Op::Open(spec),
        Op::Drop {
            result: Outcome::Err("busy".into()),
        },
        Op::Put {
            key: vec![1],
            value: vec![2; 200],
            result: Outcome::Ok(()),
        },
        Op::Delete {
            key: vec![3],
            result: Outcome::Ok(()),
        },
        Op::Batch {
            members: vec![Member::Put(4, vec![5], vec![6]), Member::Delete(7, vec![8])],
            result: Outcome::Ok(()),
        },
        Op::Get {
            key: vec![9],
            result: Outcome::Ok(Some(vec![10])),
        },
        Op::Get {
            key: vec![9],
            result: Outcome::Ok(None),
        },
        Op::Exists {
            key: vec![11],
            result: Outcome::Ok(true),
        },
        Op::MultiGet {
            keys: vec![vec![12], vec![13]],
            result: vec![Outcome::Ok(None), Outcome::Err("io".into())],
        },
        Op::MultiExists {
            keys: vec![vec![14]],
            result: Outcome::Ok(vec![false]),
        },
        Op::DropCells {
            from: vec![0; 8],
            to: vec![0xff; 8],
            result: Outcome::Ok(()),
        },
        Op::Flush {
            result: Outcome::Ok(()),
        },
        Op::Checkpoint {
            result: Outcome::Ok(()),
        },
        Op::IteratorBegin {
            id: 14,
            lower: Some(vec![1]),
            upper: None,
            reverse: true,
            snapshot: false,
        },
        Op::IteratorStep {
            id: 14,
            result: Outcome::Ok(Some([3; 32])),
        },
        Op::IteratorStep {
            id: 14,
            result: Outcome::Ok(None),
        },
        Op::IteratorEnd {
            id: 14,
            count: 1,
            sha256: [4; 32],
        },
        Op::RelocationStart,
        Op::RelocationComplete {
            decisions: vec![FilterDecision {
                keyspace: "a".into(),
                key: vec![1],
                value_sha256: [5; 32],
                wal_position: None,
                decision: 2,
            }],
            removed: vec![(0, vec![1]), (1, vec![2])],
            evidence: None,
        },
        Op::RebuildControl {
            result: Outcome::Ok(()),
        },
        Op::Close,
    ]
}

#[test]
fn every_kind_round_trips_with_sequence_and_keyspace() -> BackendResult<()> {
    let dir = scratch("roundtrip")?;
    let ops = every_kind();
    let mut stream = StreamWriter::new();
    for (i, op) in ops.iter().enumerate() {
        let keyspace = if op.database_wide() {
            DATABASE
        } else {
            u32::try_from(i)?
        };
        stream.push(keyspace, op);
    }
    let path = dir.join("ops-0000.bin");
    std::fs::write(&path, stream.finish()?)?;
    let mut reader = Reader::open(&path, 1)?;
    for (i, op) in ops.iter().enumerate() {
        let frame = reader.next_frame()?.ok_or("stream ended early")?;
        assert_eq!(frame.seq, u64::try_from(i)?);
        assert_eq!(&frame.op, op);
        if !op.database_wide() {
            assert_eq!(frame.keyspace, u32::try_from(i)?);
        }
    }
    assert!(reader.next_frame()?.is_none());
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

fn read_all(bytes: &[u8]) -> BackendResult<usize> {
    read_version(bytes, 1)
}

/// Read every frame of `bytes` as a stream of `version`; the scratch file is
/// removed before the result is returned.
fn read_version(bytes: &[u8], version: u32) -> BackendResult<usize> {
    let dir = scratch("corrupt")?;
    let path = dir.join("ops.bin");
    std::fs::write(&path, bytes)?;
    let result = (|| {
        let mut reader = Reader::open(&path, version)?;
        let mut frames = 0;
        while reader.next_frame()?.is_some() {
            frames += 1;
        }
        BackendResult::Ok(frames)
    })();
    std::fs::remove_dir_all(dir)?;
    result
}

#[test]
fn corrupt_streams_are_errors() -> BackendResult<()> {
    let mut good = StreamWriter::new();
    good.database(&Op::Close);
    good.database(&Op::Close);
    let good = good.finish()?.to_vec();
    assert_eq!(read_all(&good)?, 2);

    let mut bad_crc = good.clone();
    let last = bad_crc.len() - 1;
    bad_crc[last] ^= 1;
    assert!(failure(read_all(&bad_crc))?.contains("CRC"));

    let truncated = &good[..good.len() - 2];
    assert!(failure(read_all(truncated))?.contains("truncated"));

    let mut wrong_version = good.clone();
    wrong_version[6] = 2;
    assert!(failure(read_all(&wrong_version))?.contains("version 1"));

    let mut gap = magic(1).to_vec();
    let (kind, body) = payload(&Op::Close);
    gap.extend(frame(0, DATABASE, kind, &body)?);
    gap.extend(frame(2, DATABASE, kind, &body)?);
    assert!(failure(read_all(&gap))?.contains("sequence 2"));

    let mut unknown = magic(1).to_vec();
    unknown.extend(frame(0, DATABASE, 21, &[])?);
    assert!(failure(read_all(&unknown))?.contains("has no kind 21"));

    let mut trailing = magic(1).to_vec();
    trailing.extend(frame(0, DATABASE, 19, &[0])?);
    assert!(failure(read_all(&trailing))?.contains("trailing"));

    let mut short = magic(1).to_vec();
    short.extend_from_slice(&16u32.to_le_bytes());
    short.extend_from_slice(&[0; 20]);
    assert!(failure(read_all(&short))?.contains("body length"));
    Ok(())
}

fn v2_completion() -> Op {
    Op::RelocationComplete {
        decisions: vec![
            FilterDecision {
                keyspace: "a".into(),
                key: vec![1],
                value_sha256: [5; 32],
                wal_position: Some(4096),
                decision: 0,
            },
            // Duplicates are kept in recorded order.
            FilterDecision {
                keyspace: "a".into(),
                key: vec![1],
                value_sha256: [5; 32],
                wal_position: Some(8192),
                decision: 0,
            },
        ],
        removed: vec![(0, vec![2])],
        evidence: Some(RelocationEvidence {
            gate: 1,
            streams: vec![StreamObservation {
                partition: 0,
                fencing_generation: 0,
                processed_upper: 1 << 22,
                terminal_position: 3 << 20,
                rounded_target: 0,
            }],
            copied_survivors: 7,
            witness: 0,
        }),
    }
}

#[test]
fn version_two_completion_round_trips_and_versions_do_not_mix() -> BackendResult<()> {
    let dir = scratch("v2")?;
    let mut v2 = StreamWriter::with_version(2);
    v2.database(&Op::RelocationStart);
    v2.database(&v2_completion());
    let path = dir.join("ops-0000.bin");
    std::fs::write(&path, v2.finish()?)?;
    let mut reader = Reader::open(&path, 2)?;
    assert_eq!(
        reader.next_frame()?.ok_or("stream ended early")?.op,
        Op::RelocationStart
    );
    assert_eq!(
        reader.next_frame()?.ok_or("stream ended early")?.op,
        v2_completion()
    );
    assert!(
        Reader::open(&path, 1).is_err(),
        "header version must match the index"
    );
    std::fs::remove_dir_all(dir)?;

    // A v1 stream rejects tag 20 and a v2 stream rejects tag 17.
    let (kind, body) = payload(&v2_completion());
    assert_eq!(kind, 20);
    let mut v1_with_20 = magic(1).to_vec();
    v1_with_20.extend(frame(0, DATABASE, kind, &body)?);
    assert!(failure(read_version(&v1_with_20, 1))?.contains("no kind 20"));
    let v1_completion = every_kind()
        .into_iter()
        .find(|op| matches!(op, Op::RelocationComplete { .. }))
        .ok_or("every_kind has a v1 completion")?;
    let (kind, body) = payload(&v1_completion);
    assert_eq!(kind, 17);
    let mut v2_with_17 = magic(2).to_vec();
    v2_with_17.extend(frame(0, DATABASE, kind, &body)?);
    assert!(failure(read_version(&v2_with_17, 2))?.contains("no kind 17"));
    Ok(())
}

#[test]
fn index_requires_completion_and_plain_stream_names() -> BackendResult<()> {
    let dir = scratch("index")?;
    assert!(failure(Index::load(&dir))?.contains("did not finish"));
    let index = |file: &str| {
        serde_json::json!({"format_version": 1, "objects_compactor_enabled": false,
            "databases": {"authorities_db/perpetual": {"file": file, "frames": 1,
            "objects_compactor_enabled": false}}})
    };
    std::fs::write(dir.join("index.json"), index("ops-0000.bin").to_string())?;
    assert_eq!(Index::load(&dir)?.databases.len(), 1);
    std::fs::write(dir.join("index.json"), index("../ops-0000.bin").to_string())?;
    assert!(Index::load(&dir).is_err());
    let mut version = index("ops-0000.bin");
    version["format_version"] = 2.into();
    std::fs::write(dir.join("index.json"), version.to_string())?;
    assert_eq!(Index::load(&dir)?.format_version, 2);
    version["format_version"] = 3.into();
    std::fs::write(dir.join("index.json"), version.to_string())?;
    assert!(Index::load(&dir).is_err());
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
