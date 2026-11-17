//! Frozen exact-mode checkpoint metadata (formats 2 and 3) and authority
//! manifest (format 4) fixtures. The bytes were produced at base revision
//! e3b920d, before the option C contract. Exact mode must keep writing and
//! reading them byte for byte; the per-cell manifest fields stay absent.

use super::*;
use crate::distributed::codec::BatchLimits;
use crate::distributed::group::GroupLimits;
use crate::distributed::types::PayloadDigest;

const MANIFEST_4: &[u8] = include_bytes!("fixtures/manifest-format4.bin");
const CHECKPOINT_2: &[u8] = include_bytes!("fixtures/checkpoint-format2.bin");
const CHECKPOINT_3: &[u8] = include_bytes!("fixtures/checkpoint-format3.bin");

const DATABASE: [u8; 16] = [0x11; 16];
const SHAPE_YAML: &str = "frozen shape document";

fn version(epoch: u64, sequence: u64, operation_index: u32) -> MutationVersion {
    MutationVersion {
        batch: BatchVersion { epoch, sequence },
        operation_index,
    }
}

fn commit(generation: u64, index_files: &[&str]) -> CheckpointCommit {
    CheckpointCommit {
        epoch: 2,
        generation,
        metadata_bytes: 300 + generation,
        metadata_digest: [0xa0 + generation as u8; 32],
        index_files: index_files.iter().map(|name| (*name).to_owned()).collect(),
    }
}

fn frozen_manifest() -> AuthorityManifest {
    AuthorityManifest {
        format: 4,
        storage: LocalInventoryConfig {
            database: DATABASE,
            partitions: vec![0, 1],
            batch_limits: BatchLimits {
                max_operations: 32,
                max_encoded_bytes: 2048,
            },
            group_limits: GroupLimits {
                max_batches: 4,
                max_encoded_bytes: 4088,
            },
            fragment_bytes: 4096,
            wal_file_bytes: 8192,
            max_batches_per_partition: 16,
            max_reference_metadata_bytes: 1 << 20,
            max_epochs: 8,
        },
        history_persistence: HistoryPersistence::WalReplay,
        endpoints: (0..2)
            .map(|partition| RemoteEndpoint {
                partition,
                address: format!("127.0.0.1:{}", 7000 + partition),
                server_name: format!("owner-{partition}"),
            })
            .collect(),
        shape_yaml: SHAPE_YAML.into(),
        replay_limits: ReplayLimits {
            max_batches: 4096,
            max_encoded_bytes: 256 * 1024 * 1024,
        },
        live_nvme_identity: Some((
            "nqn.2014-08.org.nvmexpress:uuid:22222222-2222-2222-2222-222222222222".into(),
            [2; 16],
        )),
        live_nvme_owner_identities: BTreeMap::new(),
        epochs: (1..=3)
            .map(|epoch| AuthorityEpoch {
                epoch,
                active: epoch != 3,
            })
            .collect(),
        checkpoint: Some(commit(5, &["index-2-5.snapshot.cells"])),
        checkpoint_staging: Some(CheckpointFiles {
            epoch: 3,
            generation: 1,
            index_files: Vec::new(),
        }),
        checkpoint_obsolete: Some(commit(
            4,
            &["index-2-3.snapshot.cells", "index-2-4.snapshot.cells"],
        )),
        checkpoint_mode: CheckpointMode::Exact,
        cell_checkpoint: None,
        cell_checkpoint_obsolete: None,
    }
}

fn proof(partition: u32, epoch: u64) -> CheckpointProof {
    CheckpointProof {
        database: DATABASE,
        partition,
        epoch,
        prefix: 12 + partition as usize,
        retained: 4,
        digest: [0xc0 + partition as u8; 32],
    }
}

fn frozen_checkpoint_2() -> CheckpointImage {
    let mut pointer = 4096u64.to_le_bytes().to_vec();
    pointer.extend_from_slice(&[0xee; 32]);
    CheckpointImage {
        format: 2,
        database: DATABASE,
        shape_yaml: SHAPE_YAML.into(),
        authority_epoch: 2,
        generation: 5,
        cut: Some(version(2, 40, 3)),
        published_next: 41,
        index_bytes: 777,
        index_digest: [0xbb; 32],
        streams: vec![
            CheckpointStream {
                proof: proof(0, 1),
                state: [0xd0; 32],
            },
            CheckpointStream {
                proof: proof(1, 2),
                state: [0xd1; 32],
            },
        ],
        history_root: Some(
            bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .deserialize(&pointer)
                .unwrap(),
        ),
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
fn exact_manifest_format_4_bytes_are_frozen() {
    let manifest = frozen_manifest();
    assert_eq!(encode_metadata(&manifest).unwrap(), MANIFEST_4);
    let (_temp, path) = frozen_file(MANIFEST, MANIFEST_4);
    let read: AuthorityManifest = read_metadata(&path).unwrap();
    assert_eq!(encode_metadata(&read).unwrap(), MANIFEST_4);
    assert_eq!(
        manifest_checkpoint_mode(&read).unwrap(),
        CheckpointMode::Exact
    );
    let text = String::from_utf8(MANIFEST_4[8..].to_vec()).unwrap();
    assert!(!text.contains("checkpoint_mode") && !text.contains("cell_checkpoint"));
}

#[test]
fn exact_checkpoint_formats_2_and_3_bytes_are_frozen() {
    assert_eq!(
        encode_checkpoint(&frozen_checkpoint_2()).unwrap(),
        CHECKPOINT_2
    );
    assert_eq!(
        encode_checkpoint(&frozen_checkpoint_3()).unwrap(),
        CHECKPOINT_3
    );
    let (_temp, path) = frozen_file("checkpoint-2-5.metadata", CHECKPOINT_2);
    let read: CheckpointImage = read_checkpoint(&path).unwrap();
    assert_eq!(encode_checkpoint(&read).unwrap(), CHECKPOINT_2);
    let (_temp, path) = frozen_file("checkpoint-2-5.metadata", CHECKPOINT_3);
    let read: CheckpointImageV3 = read_checkpoint(&path).unwrap();
    assert_eq!(encode_checkpoint(&read).unwrap(), CHECKPOINT_3);
    // `load_checkpoint` dispatches on the leading format field.
    for (bytes, format) in [(CHECKPOINT_2, 2u32), (CHECKPOINT_3, 3)] {
        assert_eq!(
            bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .deserialize::<u32>(&bytes[..4])
                .unwrap(),
            format
        );
    }
}
