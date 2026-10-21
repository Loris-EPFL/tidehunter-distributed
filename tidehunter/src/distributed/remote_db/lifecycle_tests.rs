//! Deterministic lifecycle cuts over real five-node authenticated WAL storage.
//! These are recovery/retention regressions, not performance measurements.

use super::pipeline_tests::Fixture;
use super::*;

fn put(value: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: 1u32.to_be_bytes().to_vec().into(),
        value: vec![value; 32].into(),
    }
}

#[test]
fn acknowledged_suffix_survives_every_background_checkpoint_adoption_stage() {
    for stage in 0..=4 {
        let fixture = Fixture::new();
        let mut db = fixture.open();
        let first = fixture.operation(0, 1);
        let captured = fixture.operation(0, 2);
        let suffix = fixture.operation(0, 3);
        let first_version = db.write_batch(first, vec![put(1)]).unwrap();
        db.flush_index().unwrap();
        let captured_version = db.write_batch(captured, vec![put(2)]).unwrap();
        let (entered, waiting) = mpsc::sync_channel(1);
        let (resume, gate) = mpsc::sync_channel(1);
        db.checkpoint_pause = Some((entered, gate));
        db.checkpoint_interruption = Some(stage);
        db.start_checkpoint().unwrap();
        waiting.recv_timeout(Duration::from_secs(5)).unwrap();
        // This is the same physical stream as the frozen prefix. Its later
        // append must not invalidate the earlier exact-prefix checkpoint proof.
        let suffix_version = db.write_batch(suffix, vec![put(3)]).unwrap();
        assert_eq!(
            db.authority_write_stats()
                .background_checkpoint_suffix_batches,
            1
        );
        assert_eq!(
            db.get(KeySpace(0), &1u32.to_be_bytes())
                .unwrap()
                .unwrap()
                .as_ref(),
            &[3; 32]
        );
        resume.send(()).unwrap();
        assert!(db.progress_checkpoint(true).is_err());
        if stage == 4 {
            use std::os::unix::fs::FileExt;
            let ordinal = db.receipt_history.counts[&(0, db.epoch)];
            let stream = db
                .path
                .join(format!("receipt-history-v2/stream-0-{}.pages", db.epoch));
            let file = fs::OpenOptions::new().write(true).open(stream).unwrap();
            // Full-length torn metadata outside the adopted prefix is not
            // authority. Recovery must rebuild it from durable complete WALs.
            file.write_all_at(&[0; 256], ordinal as u64 * 256).unwrap();
            file.sync_all().unwrap();
        }
        drop(db);

        let mut reopened = fixture.open();
        assert_eq!(
            reopened
                .get(KeySpace(0), &1u32.to_be_bytes())
                .unwrap()
                .unwrap()
                .as_ref(),
            &[3; 32]
        );
        assert_eq!(
            reopened.write_batch(first, vec![put(1)]).unwrap(),
            first_version
        );
        assert_eq!(
            reopened.write_batch(captured, vec![put(2)]).unwrap(),
            captured_version
        );
        assert_eq!(
            reopened.write_batch(suffix, vec![put(3)]).unwrap(),
            suffix_version
        );
        assert!(reopened.write_batch(first, vec![put(9)]).is_err());
        reopened.validate_cached_read().unwrap();
    }
}

#[test]
fn failed_staging_intent_fences_serving_without_authorizing_outputs() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![put(1)])
        .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.checkpoint_pause = Some((entered, gate));
    db.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    let staged = db.manifest.checkpoint_staging.as_ref().unwrap().names();

    // Persistence is blocked before the intent reaches disk. Owner WAL progress
    // remains available, but no output can be written without the intent.
    db.write_batch(fixture.operation(1, 2), vec![put(2)])
        .unwrap();
    let pending = db.path.join(format!(".{MANIFEST}.pending"));
    fs::create_dir(&pending).unwrap();
    resume.send(()).unwrap();
    assert!(db.progress_checkpoint(true).is_err());
    assert!(!db.healthy);
    assert!(db.ensure_healthy().is_err());

    let persisted: AuthorityManifest = read_metadata(&db.path.join(MANIFEST)).unwrap();
    assert!(persisted.checkpoint_staging.is_none());
    assert!(persisted.checkpoint_obsolete.is_none());
    assert!(persisted.checkpoint.is_none());
    for name in staged {
        assert!(!db.path.join(name).exists());
    }

    fs::remove_dir(pending).unwrap();
    drop(db);
    let mut reopened = fixture.open();
    assert_eq!(
        reopened
            .get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[2; 32]
    );
}

#[test]
fn completed_owner_metadata_is_bounded_across_lifetime_capacity_and_reopens() {
    let fixture = Fixture::new_with_capacity(8);
    let original = fixture.operation(0, 100);
    let mut original_version = None;
    let mut resident_bound = None;
    let mut next = 100u64;
    for _ in 0..3 {
        let mut db = fixture.open();
        for _ in 0..4 {
            for _ in 0..16 {
                let operation = fixture.operation((next % 5) as u32, next);
                let version = db.write_batch(operation, vec![put(next as u8)]).unwrap();
                if next == 100 {
                    original_version = Some(version);
                }
                next += 1;
                assert!(db.submissions.is_empty());
                assert_eq!(db.publication.retained_batches(), 0);
                assert!(db.known.len() <= fixture.config.max_admitted_batches);
            }
            db.flush_index().unwrap();
            assert!(
                db.known.is_empty(),
                "completed outcomes must page out after durable adoption"
            );
            let charge = db.metadata_history_bytes();
            let bound = *resident_bound.get_or_insert(charge);
            assert!(
                charge <= bound,
                "resident completed bookkeeping grew with lifetime history"
            );
            assert_eq!(
                db.write_batch(original, vec![put(100)]).unwrap(),
                original_version.unwrap()
            );
        }
        db.shutdown().unwrap();
    }
}

#[test]
fn absent_or_corrupt_pageable_retry_authority_fails_closed() {
    for missing in [false, true] {
        let fixture = Fixture::new();
        let mut db = fixture.open();
        let operation = fixture.operation(0, 1);
        db.write_batch(operation, vec![put(1)]).unwrap();
        db.flush_index().unwrap();
        let nodes = db.path.join("receipt-history-v2/nodes.pages");
        if missing {
            fs::remove_file(&nodes).unwrap();
        } else {
            use std::os::unix::fs::FileExt;
            let file = fs::OpenOptions::new().write(true).open(&nodes).unwrap();
            file.write_all_at(&[0; 256], 0).unwrap();
            file.sync_all().unwrap();
        }
        // Reopen, rather than a live external file mutation, is the supported
        // authority-corruption boundary. Never treat a missing root as no IDs.
        db.healthy = false;
        drop(db);
        assert!(
            RemoteDatabase::open(
                &fixture.temp.path().join("authority"),
                &fixture.shape,
                fixture.config.clone(),
                Metrics::new_in_enabled(&prometheus::Registry::new(), false),
            )
            .is_err()
        );
    }
}

#[test]
fn index_pressure_waits_for_existing_capture_before_rejecting_live_residency() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![put(1)])
        .unwrap();

    let resident = db.index_stats().charged_metadata_bytes;
    let additional_bytes = db
        .config
        .max_index_bytes
        .checked_sub(resident)
        .unwrap()
        .checked_add(1)
        .unwrap();
    assert!(db.index.read().check_capacity(additional_bytes).is_err());

    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.checkpoint_pause = Some((entered, gate));
    db.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    let generation = db.snapshot_generation;
    let release = std::thread::spawn(move || resume.send(()).unwrap());

    db.ensure_admission_pressure(AdmissionPressure::Index { additional_bytes })
        .unwrap();
    release.join().unwrap();

    assert_eq!(db.snapshot_generation, generation);
    assert!(db.background_checkpoint.is_none());
    db.index.read().check_capacity(additional_bytes).unwrap();
}

#[test]
fn drained_partition_pressure_does_not_wait_for_an_unrelated_capture() {
    let mut fixture = Fixture::new_with_capacity(1);
    fixture.config.max_admitted_batches = 4;
    fixture.config.max_pending_batches = 4;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![put(1)])
        .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.checkpoint_pause = Some((entered, gate));
    db.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();

    let connection = Arc::clone(&db.clients[1].client);
    let held = connection.lock();
    db.submit(fixture.operation(1, 2), vec![put(2)]).unwrap();
    let pressure = AdmissionPressure::Partition {
        partition: 1,
        records: 1,
    };
    assert!(!db.admission_pressure_fits(pressure));
    let (finished, completion) = mpsc::sync_channel(1);
    let owner = thread::spawn(move || {
        let result = db.ensure_admission_pressure(pressure);
        finished.send(result.is_ok()).unwrap();
        (db, result)
    });
    drop(held);
    // The channel bounds a dependency deadlock, not a device-latency target.
    let observed = completion.recv_timeout(Duration::from_secs(5));
    resume.send(()).unwrap();
    let (mut db, result) = owner.join().unwrap();
    assert!(observed.unwrap());
    result.unwrap();
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[2; 32]
    );
    db.flush_index().unwrap();
}

#[test]
fn adopted_capture_releases_identity_pressure_without_waiting_for_another_cut() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![put(1)])
        .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.checkpoint_pause = Some((entered, gate));
    db.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut suffix = None;
    for sequence in 1..fixture.config.max_admitted_batches {
        let operation = fixture.operation((sequence % 5) as u32, sequence as u64 + 1);
        let version = db.write_batch(operation, vec![put(2)]).unwrap();
        suffix = Some((operation, version));
    }
    let pressure = AdmissionPressure::Tail {
        encoded_bytes: fixture.config.storage.batch_limits.max_encoded_bytes,
        identity: true,
    };
    assert!(!db.admission_pressure_fits(pressure));
    let (next_entered, _next_waiting) = mpsc::sync_channel(1);
    let (next_resume, next_gate) = mpsc::sync_channel(1);
    db.checkpoint_pause = Some((next_entered, next_gate));
    let (finished, completion) = mpsc::sync_channel(1);
    let owner = thread::spawn(move || {
        let result = db.ensure_admission_pressure(pressure);
        finished.send(result.is_ok()).unwrap();
        (db, result)
    });
    resume.send(()).unwrap();
    // An unnecessary full catch-up would now depend on next_resume.
    let observed = completion.recv_timeout(Duration::from_secs(5));
    next_resume.send(()).unwrap();
    let (mut db, result) = owner.join().unwrap();
    assert!(observed.unwrap());
    result.unwrap();
    assert_eq!(
        db.checkpoint_tail_batches,
        fixture.config.max_admitted_batches - 1
    );
    db.healthy = false;
    drop(db);
    let mut reopened = fixture.open();
    assert_eq!(
        reopened
            .get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[2; 32]
    );
    let (operation, version) = suffix.unwrap();
    assert_eq!(
        reopened.write_batch(operation, vec![put(2)]).unwrap(),
        version
    );
}

#[test]
fn remaining_index_pressure_requires_one_new_capture_after_the_existing_cut() {
    let mut fixture = Fixture::new();
    let mut db = fixture.open();
    let mutations = |value| {
        (1u32..=3)
            .map(|key| Mutation::Put {
                keyspace: KeySpace(0),
                key: key.to_be_bytes().to_vec().into(),
                value: vec![value; 32].into(),
            })
            .collect::<Vec<_>>()
    };
    db.write_batch(fixture.operation(0, 1), mutations(0))
        .unwrap();
    db.flush_index().unwrap();
    let resident_directory = db.index_stats().charged_metadata_bytes;
    let incoming = CompleteBatch::new(
        &fixture.shape,
        BatchId {
            database: fixture.config.storage.database,
            operation: fixture.operation(2, 4),
        },
        BatchVersion {
            epoch: db.epoch,
            sequence: db.submission_next(),
        },
        mutations(3),
        fixture.config.storage.batch_limits,
    )
    .unwrap();
    let additional_bytes = db.index.read().estimated_batch_bytes(&incoming).unwrap();
    db.shutdown().unwrap();
    drop(db);
    fixture.config.max_index_bytes = resident_directory.checked_add(additional_bytes).unwrap();
    let mut db = fixture.open();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.checkpoint_pause = Some((entered, gate));
    db.write_batch(fixture.operation(0, 2), vec![put(1)])
        .unwrap();
    db.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    db.write_batch(fixture.operation(1, 3), vec![put(2)])
        .unwrap();
    let generation = db.snapshot_generation;
    let pressure = AdmissionPressure::Index { additional_bytes };
    assert!(!db.admission_pressure_fits(pressure));
    resume.send(()).unwrap();
    db.ensure_admission_pressure(pressure).unwrap();
    assert_eq!(db.snapshot_generation, generation + 1);
    assert!(db.background_checkpoint.is_none());
    db.index.read().check_capacity(additional_bytes).unwrap();
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[2; 32]
    );
    db.flush_index().unwrap();
}

#[test]
fn unchanged_flush_waits_for_captured_cut_and_then_covers_its_acknowledged_suffix() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![put(1)])
        .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.checkpoint_pause = Some((entered, gate));
    db.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    db.write_batch(fixture.operation(1, 2), vec![put(2)])
        .unwrap();
    resume.send(()).unwrap();
    db.flush_index().unwrap();
    assert_eq!(db.checkpoint_tail_batches, 0);
    assert_eq!(db.checkpoint_tail_bytes, 0);
    assert!(db.background_checkpoint.is_none());
    assert!(db.known.is_empty());
    db.healthy = false;
    drop(db);
    let reopened = fixture.open();
    assert_eq!(reopened.recovery_stats().tail_batches, 0);
}

#[test]
fn skewed_submit_window_spills_one_partition_without_lifetime_rejection() {
    let mut fixture = Fixture::new_with_capacity(4);
    fixture.config.max_admitted_batches = 64;
    fixture.config.max_pending_batches = 64;
    let mut db = fixture.open();
    // One prevalidated window is larger than this node's four-batch metadata
    // window. Every identity deliberately maps to the same physical partition.
    let writes: Vec<_> = (0..19u64)
        .map(|sequence| {
            (
                fixture.operation(0, 1000 + sequence),
                vec![put(sequence as u8)],
            )
        })
        .collect();
    let versions = db.submit_many(&writes).unwrap();
    db.drain().unwrap();
    assert_eq!(versions.len(), writes.len());
    for (sequence, version) in versions.iter().enumerate() {
        assert_eq!(version.sequence, sequence as u64);
    }
    assert_eq!(db.stats().admitted_batches, writes.len() as u64);
    assert!(db.submissions.is_empty());
    assert!(db.partition_admissions.iter().all(|count| *count == 0));
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[18; 32]
    );
    db.flush_index().unwrap();
    db.shutdown().unwrap();
    drop(db);
    let mut reopened = fixture.open();
    for ((operation, mutations), version) in writes.into_iter().zip(versions) {
        assert_eq!(reopened.write_batch(operation, mutations).unwrap(), version);
    }
    reopened.validate_cached_read().unwrap();
}

#[test]
fn publication_failure_fences_and_recovers_dispatched_successor_group() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let connection = std::sync::Arc::clone(&db.clients[0].client);
    let held = connection.lock();
    let mut identities = Vec::new();
    for key in 0..4u32 {
        let operation = fixture.operation(0, u64::from(key));
        let version = db.submit(operation, vec![put(key as u8)]).unwrap();
        identities.push((operation, version));
    }
    assert_eq!(db.staged[0].len(), 2);
    drop(held);

    // A missing prepared route makes the first ordered publication fail after
    // progress_pipeline has dispatched the already-admitted successor group.
    db.submissions[0].routing = None;
    assert!(db.drain().is_err());
    assert!(!db.healthy);
    assert!(db.ensure_healthy().is_err());
    assert_eq!(db.publication.published_prefix().next_sequence(), 0);
    assert!(db.pending_bytes > 0);

    let successor = db.inflight[0].take().unwrap();
    let receipts = successor
        .reply
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(receipts.len(), 2);
    assert_eq!(receipts[0].appended().version().sequence, 2);
    assert_eq!(receipts[1].appended().version().sequence, 3);
    drop(db);

    let mut reopened = fixture.open();
    assert_eq!(reopened.recovery_stats().tail_batches, 4);
    for (operation, version) in identities {
        assert_eq!(
            reopened
                .submit(operation, vec![put(version.sequence as u8)])
                .unwrap(),
            version
        );
    }
    assert_eq!(
        reopened.get(KeySpace(0), &1u32.to_be_bytes()).unwrap(),
        Some(vec![3; 32].into())
    );
    assert_eq!(reopened.stats.admitted_batches, 0);
    reopened.shutdown().unwrap();
}
