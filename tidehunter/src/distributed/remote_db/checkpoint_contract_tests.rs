//! The authority-side option C contract: the inactive `PerCell` mode, the
//! manifest tag rules, the format-4 wrapper rules and exactly-once credit
//! retirement.

use super::super::pipeline_tests::Fixture;
use super::super::tests::{fail_stop_without_checkpoint, reopen_result};
use super::*;
use crate::distributed::types::PayloadDigest;

fn through(epoch: u64, sequence: u64) -> PrefixCut {
    PrefixCut::Through(BatchVersion { epoch, sequence })
}

#[test]
fn checkpoint_mode_defaults_to_exact_and_per_cell_is_explicit_opt_in() {
    let fixture = Fixture::new();
    let mut serialized = serde_json::to_value(&fixture.config).unwrap();
    assert!(serialized.get("checkpoint_mode").is_none());
    let config: RemoteDatabaseConfig = serde_json::from_value(serialized.clone()).unwrap();
    assert_eq!(config.checkpoint_mode, CheckpointMode::Exact);
    config.validate().unwrap();
    serialized["checkpoint_mode"] = "per_cell".into();
    let per_cell: RemoteDatabaseConfig = serde_json::from_value(serialized).unwrap();
    assert_eq!(per_cell.checkpoint_mode, CheckpointMode::PerCell);
    assert_eq!(
        serde_json::to_value(&per_cell).unwrap()["checkpoint_mode"],
        "per_cell"
    );
    // PerCell is accepted only when named explicitly; absent stays Exact.
    per_cell.validate().unwrap();
    assert_eq!(
        serde_yaml::from_str::<CheckpointMode>("exact").unwrap(),
        CheckpointMode::Exact
    );
}

fn cell_commit(generation: u64) -> CellCheckpointCommit {
    CellCheckpointCommit {
        epoch: 3,
        generation,
        metadata_bytes: 100,
        metadata_digest: [1; 32],
        root_bytes: 200,
        root_digest: [2; 32],
    }
}

fn per_cell_manifest(exact: &AuthorityManifest) -> AuthorityManifest {
    AuthorityManifest {
        format: PER_CELL_MANIFEST_FORMAT,
        checkpoint: None,
        checkpoint_mode: CheckpointMode::PerCell,
        cell_checkpoint: Some(cell_commit(2)),
        cell_checkpoint_obsolete: Some(cell_commit(1)),
        ..exact.clone()
    }
}

#[test]
fn manifest_checkpoint_tags_never_mix_and_per_cell_never_opens_as_exact() {
    let fixture = Fixture::new();
    fail_stop_without_checkpoint(fixture.open());
    let root = fixture.temp.path().join("authority");
    let exact: AuthorityManifest = read_metadata(&root.join(MANIFEST)).unwrap();
    assert_eq!(exact.format, 4);
    assert_eq!(
        manifest_checkpoint_mode(&exact).unwrap(),
        CheckpointMode::Exact
    );
    let per_cell = per_cell_manifest(&exact);
    assert_eq!(
        manifest_checkpoint_mode(&per_cell).unwrap(),
        CheckpointMode::PerCell
    );
    // First adoption after migration: the obsolete commit is the last exact one.
    let migrated = AuthorityManifest {
        checkpoint_obsolete: Some(CheckpointCommit {
            epoch: 2,
            generation: 9,
            metadata_bytes: 1,
            metadata_digest: [3; 32],
            index_files: vec!["index-2-9.snapshot.cells".into()],
        }),
        cell_checkpoint_obsolete: None,
        ..per_cell.clone()
    };
    assert_eq!(
        manifest_checkpoint_mode(&migrated).unwrap(),
        CheckpointMode::PerCell
    );
    let mixed = [
        AuthorityManifest {
            checkpoint_mode: CheckpointMode::PerCell,
            ..exact.clone()
        },
        AuthorityManifest {
            cell_checkpoint: Some(cell_commit(2)),
            ..exact.clone()
        },
        AuthorityManifest {
            cell_checkpoint_obsolete: Some(cell_commit(1)),
            ..exact.clone()
        },
        AuthorityManifest {
            checkpoint_mode: CheckpointMode::Exact,
            ..per_cell.clone()
        },
        AuthorityManifest {
            checkpoint: migrated.checkpoint_obsolete.clone(),
            ..per_cell.clone()
        },
    ];
    for manifest in &mixed {
        assert!(
            manifest_checkpoint_mode(manifest)
                .unwrap_err()
                .to_string()
                .contains("inconsistent checkpoint mode tags")
        );
    }

    // Open rejects both before changing the manifest; no fallback to Exact.
    persist_manifest(&root, &per_cell).unwrap();
    let error = reopen_result(&fixture).err().unwrap();
    assert!(error.to_string().contains("checkpoint mode differs"));
    let after: AuthorityManifest = read_metadata(&root.join(MANIFEST)).unwrap();
    assert_eq!(after.format, PER_CELL_MANIFEST_FORMAT);
    assert_eq!(after.cell_checkpoint, Some(cell_commit(2)));
    persist_manifest(&root, &mixed[1]).unwrap();
    let error = reopen_result(&fixture).err().unwrap();
    assert!(
        error
            .to_string()
            .contains("inconsistent checkpoint mode tags")
    );
    // The exact manifest still opens.
    persist_manifest(&root, &exact).unwrap();
    drop(reopen_result(&fixture).unwrap());
}

#[test]
fn cell_commit_names_the_format_5_root_and_the_exact_wrapper() {
    let commit = cell_commit(7);
    assert_eq!(commit.metadata_name(), "checkpoint-3-7.metadata");
    assert_eq!(commit.root_name(), "index-root-3-7.cellroot");
    // The per-cell staging reservation names the root and its staging file;
    // the wrapper and its staging file come from the exact convention.
    let staged = CheckpointFiles {
        epoch: 3,
        generation: 7,
        index_files: vec![
            commit.root_name(),
            crate::distributed::native_index::cell_root_staging_name(&commit.root_name()),
        ],
    }
    .names();
    assert!(staged.contains(&commit.metadata_name()) && staged.contains(&commit.root_name()));
    assert!(staged.contains(&".index-root-3-7.cellroot.pending".to_owned()));
    assert!(staged.contains(&".checkpoint-3-7.metadata.pending".to_owned()));
}

fn header(wrapper: &CheckpointImageV4) -> CellRootHeader {
    CellRootHeader {
        format: 5,
        flags: 0,
        database: wrapper.database,
        schema: [4; 32],
        authority_epoch: wrapper.authority_epoch,
        snapshot_generation: wrapper.generation,
        lifecycle_generation: 1,
        directory_target: wrapper.directory_target,
        replay_floor: wrapper.replay_floor,
        max_frame_cut: wrapper.max_frame_cut,
        history_floor: wrapper.history_floor,
    }
}

fn receipt(database: [u8; 16], sequence: u64) -> RecentReceipt {
    RecentReceipt {
        id: BatchId {
            database,
            operation: u128::from(sequence).to_be_bytes(),
        },
        version: BatchVersion { epoch: 3, sequence },
        digest: PayloadDigest([5; 32]),
    }
}

/// A valid wrapper for `config`: generation 7 of epoch 3, F = (3,10).
fn wrapper(config: &RemoteDatabaseConfig) -> CheckpointImageV4 {
    let database = config.storage.database;
    let fragment = config.storage.fragment_bytes;
    let receipts = config.history_persistence != HistoryPersistence::WalReplay;
    let stream = |partition: u32, epoch: u64| CellCheckpointStream {
        partition,
        epoch,
        replay_from: 2 * fragment,
        last_offset: Some(2 * fragment + 64),
        durable_end: 3 * fragment,
        complete: epoch < 3,
        receipt_prefix: receipts.then_some(CheckpointStream {
            proof: CheckpointProof {
                database,
                partition,
                epoch,
                prefix: 9,
                retained: 2,
                digest: [6; 32],
            },
            state: [7; 32],
        }),
    };
    let partition = config.storage.partitions[0];
    let mut pointer = 64u64.to_le_bytes().to_vec();
    pointer.extend_from_slice(&[8; 32]);
    CheckpointImageV4 {
        format: PER_CELL_CHECKPOINT_FORMAT,
        recovery_kind: RecoveryKind::PerCellCoverageV1,
        history_persistence: config.history_persistence,
        database,
        shape_yaml: "shape".into(),
        authority_epoch: 3,
        generation: 7,
        root_name: cell_commit(7).root_name(),
        root_bytes: 200,
        root_digest: [2; 32],
        directory_target: through(3, 20),
        replay_floor: through(3, 10),
        max_frame_cut: through(3, 25),
        history_floor: Some(MutationVersion {
            batch: BatchVersion {
                epoch: 3,
                sequence: 4,
            },
            operation_index: 0,
        }),
        published_next: 11,
        streams: vec![stream(partition, 2), stream(partition, 3)],
        recent: if receipts {
            Vec::new()
        } else {
            vec![receipt(database, 9), receipt(database, 10)]
        },
        history_root: receipts.then(|| {
            bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .deserialize(&pointer)
                .unwrap()
        }),
        replay_limits: ReplayLimits::default(),
        retirement: RetirementToken {
            generation: 7,
            from: through(3, 2),
            through: through(3, 10),
            batches: 8,
            encoded_bytes: 800,
            identities: 8,
        },
    }
}

fn check(config: &RemoteDatabaseConfig, wrapper: &CheckpointImageV4) -> Result<(), String> {
    wrapper
        .check(&cell_commit(7), &header(wrapper), config, "shape")
        .map_err(|error| error.to_string())
}

#[test]
fn wrapper_rules_accept_both_history_modes_and_reject_each_violation() {
    let mut fixture = Fixture::new();
    for history in [HistoryPersistence::WalReplay, HistoryPersistence::OnDisk] {
        fixture.config.history_persistence = history;
        let config = &fixture.config;
        check(config, &wrapper(config)).unwrap();
        type Mutate = fn(&mut CheckpointImageV4);
        let cases: [(&str, Mutate); 15] = [
            ("root binding differs", |w| w.format = 3),
            ("root binding differs", |w| w.generation = 8),
            ("root binding differs", |w| w.root_digest = [0; 32]),
            ("root binding differs", |w| {
                w.root_name = "index-3-7.root".into()
            }),
            ("prefix state is not at F", |w| w.published_next = 10),
            ("invalid checkpoint wrapper stream", |w| {
                w.streams.swap(0, 1)
            }),
            ("invalid checkpoint wrapper stream", |w| {
                w.streams[0].epoch = 4
            }),
            ("invalid checkpoint wrapper stream", |w| {
                w.streams[0].replay_from += 1
            }),
            ("invalid checkpoint wrapper stream", |w| {
                w.streams[1].last_offset = Some(w.streams[1].durable_end);
            }),
            ("invalid checkpoint wrapper stream", |w| {
                w.streams[1].receipt_prefix = match w.streams[1].receipt_prefix.take() {
                    Some(_) => None,
                    None => Some(CheckpointStream {
                        proof: CheckpointProof {
                            database: w.database,
                            partition: w.streams[1].partition,
                            epoch: 3,
                            prefix: 0,
                            retained: 0,
                            digest: [0; 32],
                        },
                        state: [0; 32],
                    }),
                };
            }),
            ("invalid checkpoint wrapper retry state", |w| {
                w.recent = vec![receipt(w.database, 11)];
            }),
            ("invalid checkpoint wrapper retry state", |w| {
                w.recent = vec![receipt(w.database, 10), receipt(w.database, 10)];
            }),
            ("retirement is not owned by this generation", |w| {
                w.retirement.through = through(3, 9);
            }),
            ("retirement is not owned by this generation", |w| {
                w.retirement.generation = 6;
            }),
            ("invalid retirement token", |w| {
                w.retirement.encoded_bytes = 0
            }),
        ];
        for (expected, mutate) in cases {
            let mut changed = wrapper(config);
            mutate(&mut changed);
            let error = check(config, &changed).unwrap_err();
            assert!(error.contains(expected), "{history:?}: {error}");
        }
        // A changed cut must match the bound root header too.
        let changed = wrapper(config);
        let mut root = header(&changed);
        root.replay_floor = through(3, 9);
        assert!(
            changed
                .check(&cell_commit(7), &root, config, "shape")
                .unwrap_err()
                .to_string()
                .contains("cuts differ from root")
        );
    }
    // A retry window or history root of the other mode is rejected.
    fixture.config.history_persistence = HistoryPersistence::OnDisk;
    let mut on_disk = wrapper(&fixture.config);
    on_disk.recent = vec![receipt(on_disk.database, 10)];
    assert!(
        check(&fixture.config, &on_disk)
            .unwrap_err()
            .contains("retry state")
    );
    fixture.config.history_persistence = HistoryPersistence::WalReplay;
    let mut replay = wrapper(&fixture.config);
    replay.history_root = wrapper(&{
        let mut config = fixture.config.clone();
        config.history_persistence = HistoryPersistence::OnDisk;
        config
    })
    .history_root;
    assert!(
        check(&fixture.config, &replay)
            .unwrap_err()
            .contains("retry state")
    );
}

#[test]
fn wrapper_format_leads_the_bounded_encoding() {
    let fixture = Fixture::new();
    let wrapper = wrapper(&fixture.config);
    let bytes = encode_checkpoint(&wrapper).unwrap();
    let options = bincode::DefaultOptions::new().with_fixint_encoding();
    assert_eq!(
        options.deserialize::<u32>(&bytes[..4]).unwrap(),
        PER_CELL_CHECKPOINT_FORMAT
    );
    let temp = tempdir::TempDir::new("checkpoint-wrapper-4").unwrap();
    let path = temp.path().join(cell_commit(7).metadata_name());
    fs::write(&path, &bytes).unwrap();
    let read: CheckpointImageV4 = read_checkpoint(&path).unwrap();
    assert_eq!(encode_checkpoint(&read).unwrap(), bytes);
    check(&fixture.config, &read).unwrap();
}

#[test]
fn retirement_returns_credits_exactly_once_and_in_order() {
    let token = |generation, from, through, batches, encoded_bytes, identities| RetirementToken {
        generation,
        from,
        through,
        batches,
        encoded_bytes,
        identities,
    };
    let mut ledger = RetirementLedger {
        generation: 4,
        floor: through(3, 2),
    };
    let first = token(5, through(3, 2), through(3, 10), 7, 700, 6);
    let credits = ledger.apply(&first).unwrap().unwrap();
    assert_eq!(
        (
            credits.identities,
            credits.replay_batches,
            credits.replay_bytes
        ),
        (6, 7, 700)
    );
    assert_eq!(
        ledger,
        RetirementLedger {
            generation: 5,
            floor: through(3, 10)
        }
    );
    // A second apply of the same generation returns nothing.
    assert!(ledger.apply(&first).unwrap().is_none());
    // An unchanged F returns zero credit, once.
    let unchanged = token(6, through(3, 10), through(3, 10), 0, 0, 0);
    let credits = ledger.apply(&unchanged).unwrap().unwrap();
    assert_eq!(
        (
            credits.identities,
            credits.replay_batches,
            credits.replay_bytes
        ),
        (0, 0, 0)
    );
    // An aborted-only prefix: F advances with no WAL batch.
    let aborted = token(7, through(3, 10), through(3, 12), 0, 0, 0);
    assert!(ledger.apply(&aborted).unwrap().is_some());
    for (invalid, message) in [
        (
            token(6, through(3, 12), through(3, 13), 1, 1, 1),
            "does not follow",
        ),
        (
            token(9, through(3, 11), through(3, 13), 1, 1, 1),
            "does not follow",
        ),
        (
            token(7, through(3, 10), through(3, 13), 1, 1, 1),
            "does not follow",
        ),
        (
            token(8, through(3, 12), through(3, 12), 1, 9, 0),
            "invalid retirement token",
        ),
        (
            token(8, through(3, 13), through(3, 12), 0, 0, 0),
            "invalid retirement token",
        ),
        (
            token(8, through(3, 12), through(3, 13), 1, 0, 1),
            "invalid retirement token",
        ),
    ] {
        let before = ledger;
        let error = ledger.apply(&invalid).err().unwrap().to_string();
        assert!(error.contains(message), "{error}");
        assert_eq!(ledger, before);
    }
}
