//! Frozen Exact checkpoint metadata (format 3) and the pre-L1 authority
//! manifest (format 4), produced at base revision e3b920d. The checkpoint
//! image keeps its bytes; L1 rejects the pre-cutover manifest unchanged.

use super::*;
use crate::distributed::types::PayloadDigest;

const MANIFEST_4: &[u8] = include_bytes!("fixtures/manifest-format4.bin");
const CHECKPOINT_3: &[u8] = include_bytes!("fixtures/checkpoint-format3.bin");

const DATABASE: [u8; 16] = [0x11; 16];
const SHAPE_YAML: &str = "frozen shape document";

fn version(epoch: u64, sequence: u64, operation_index: u32) -> MutationVersion {
    MutationVersion {
        batch: BatchVersion { epoch, sequence },
        operation_index,
    }
}

fn receipt(sequence: u64) -> RecentReceipt {
    RecentReceipt {
        id: BatchId {
            database: DATABASE,
            operation: u128::from(sequence).to_be_bytes(),
        },
        version: BatchVersion { epoch: 2, sequence },
        digest: PayloadDigest([sequence as u8; 32]),
    }
}

fn frozen_checkpoint_3() -> CheckpointImageV3 {
    CheckpointImageV3 {
        format: 3,
        database: DATABASE,
        shape_yaml: SHAPE_YAML.into(),
        authority_epoch: 2,
        generation: 5,
        cut: Some(version(2, 40, 3)),
        published_next: 41,
        index_bytes: 777,
        index_digest: [0xbb; 32],
        positions: vec![
            StreamPosition {
                partition: 0,
                epoch: 2,
                replay_from: 8192,
                last_offset: Some(8300),
                durable_end: 9000,
                complete: false,
            },
            StreamPosition {
                partition: 1,
                epoch: 1,
                replay_from: 0,
                last_offset: None,
                durable_end: 4096,
                complete: true,
            },
        ],
        recent: vec![receipt(39), receipt(40)],
    }
}

/// Write `bytes` to a fresh file and return its directory and path.
fn frozen_file(name: &str, bytes: &[u8]) -> (tempdir::TempDir, PathBuf) {
    let temp = tempdir::TempDir::new("remote-legacy-fixture").unwrap();
    let path = temp.path().join(name);
    fs::write(&path, bytes).unwrap();
    (temp, path)
}

#[test]
fn pre_generation_manifest_format_4_is_rejected_unchanged() {
    let (_temp, path) = frozen_file(MANIFEST, MANIFEST_4);
    let error = fencing_generations::read_authority_manifest(&path).unwrap_err();
    assert_eq!(
        error.to_string(),
        format!(
            "unsupported distributed authority manifest format 4 (expected {})",
            fencing_generations::AUTHORITY_FORMAT
        )
    );
    assert_eq!(fs::read(&path).unwrap(), MANIFEST_4);
}

#[test]
fn exact_checkpoint_format_3_bytes_are_frozen() {
    assert_eq!(
        encode_checkpoint(&frozen_checkpoint_3()).unwrap(),
        CHECKPOINT_3
    );
    let (_temp, path) = frozen_file("checkpoint-2-5.metadata", CHECKPOINT_3);
    let read: CheckpointImageV3 = read_checkpoint(&path).unwrap();
    assert_eq!(encode_checkpoint(&read).unwrap(), CHECKPOINT_3);
    // `load_checkpoint` dispatches on the leading format field.
    assert_eq!(
        bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .deserialize::<u32>(&CHECKPOINT_3[..4])
            .unwrap(),
        3
    );
}
