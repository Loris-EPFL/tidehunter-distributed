//! Option C counters: name tables, aggregation, monotone snapshots under
//! concurrent recording, thread accounting and gauges. No sleeps: threads
//! meet at barriers and channels.

use super::*;
use std::collections::HashMap;
use std::sync::Barrier;
use std::sync::mpsc::sync_channel;

fn version(epoch: u64, sequence: u64) -> BatchVersion {
    BatchVersion { epoch, sequence }
}

fn through(epoch: u64, sequence: u64) -> PrefixCut {
    PrefixCut::Through(version(epoch, sequence))
}

fn names(snapshot: &CellMetricsSnapshot) -> HashMap<&'static str, u64> {
    let mut names = HashMap::new();
    snapshot.visit(|name, value| {
        assert!(names.insert(name, value).is_none(), "duplicate {name}");
    });
    names
}

fn store_stats(frames: u64, frame_bytes: u64, scratch: u64) -> IndexStoreWriterStats {
    IndexStoreWriterStats {
        encode: CellEncodeStats {
            buffered_cells: frames,
            encode_passes: frames,
            row_passes: frames,
            references_encoded: 2 * frames,
            buffered_bytes: frame_bytes,
            scratch_capacity: scratch,
            ..CellEncodeStats::default()
        },
        frames,
        frame_bytes,
        rows: frames,
        versions: 2 * frames,
        segments_created: 1,
        groups_sealed: 1,
        sealed_bytes: frame_bytes,
        file_syncs: 1,
        directory_syncs: 1,
        sync_ns: 10,
        mappings: 1,
    }
}

/// Every indexed name row is the total name table with the row's prefix, so
/// a macro row can never shift fields.
fn assert_rows<const N: usize>(total: &[&str; N], indexed: &[[&str; N]; INDEXED], letter: char) {
    for (row, names) in indexed.iter().enumerate() {
        for (name, total) in names.iter().zip(total) {
            let prefix = format!("cell_{letter}{row}_");
            assert_eq!(name.strip_prefix(&prefix), total.strip_prefix("cell_"));
        }
    }
}

#[test]
fn names_are_unique_prefixed_and_aligned() {
    assert_rows(
        &WorkerSnapshot::TOTAL_NAMES,
        &WorkerSnapshot::INDEXED_NAMES,
        'w',
    );
    assert_rows(
        &StoreSnapshot::TOTAL_NAMES,
        &StoreSnapshot::INDEXED_NAMES,
        'w',
    );
    assert_rows(
        &RangeSnapshot::TOTAL_NAMES,
        &RangeSnapshot::INDEXED_NAMES,
        'r',
    );
    let metrics = CellMetrics::new(MAX_CELL_FLUSHERS);
    let snapshot = metrics.snapshot(CellGauges {
        published: Some(through(1, 9)),
        adopted: Some(AdoptedGauges {
            generation: 3,
            target: through(1, 8),
            floor: through(1, 6),
            max_frame_cut: through(1, 9),
            history_floor: through(1, 2),
        }),
        resident_index_bytes: Some(1),
        admitted_index_bytes: Some(2),
        transient_index_bytes: Some(3),
        segments: Some(IndexStoreSpace::default()),
    });
    let names = names(&snapshot);
    assert!(names.keys().all(|name| name.starts_with("cell_")));
    let indexed = WorkerSnapshot::FIELDS + StoreSnapshot::FIELDS + RangeSnapshot::FIELDS + 3;
    for worker in 0..MAX_CELL_FLUSHERS {
        let prefixes = [format!("cell_w{worker}_"), format!("cell_r{worker}_")];
        let count = names
            .keys()
            .filter(|name| {
                prefixes
                    .iter()
                    .any(|prefix| name.starts_with(prefix.as_str()))
            })
            .count();
        assert_eq!(count, indexed);
    }
    for name in [
        "cell_captured",
        "cell_frames",
        "cell_selected_size",
        "cell_scheduler_passes",
        "cell_root_payload_cell_encodes",
        "cell_exact_checkpoints",
        "cell_admission_blocked_replay_tail_ns",
        "cell_recovery_runs",
        "cell_maintenance_cpu_ns",
        "cell_gauge_lag_l_f_batches",
        "cell_gauge_segment_gc_pending_bytes",
    ] {
        assert!(names.contains_key(name), "{name}");
    }
    // A pool of two exports two rows per indexed block, no more.
    let two = CellMetrics::new(2).snapshot(CellGauges::default());
    let names = self::names(&two);
    assert!(names.contains_key("cell_w1_captured") && !names.contains_key("cell_w2_captured"));
    assert!(names.contains_key("cell_r1_queue_full") && !names.contains_key("cell_r2_queue_full"));
    assert!(!names.contains_key("cell_gauge_published_present"));
    assert!(!names.contains_key("cell_gauge_segments"));
}

#[test]
fn totals_sum_counters_and_take_maxima() {
    let metrics = CellMetrics::new(3);
    metrics.worker(0).dequeued(Duration::from_nanos(5));
    metrics.worker(2).dequeued(Duration::from_nanos(9));
    metrics.worker(2).dequeued(Duration::from_nanos(1));
    // Five captures; three installed and one stale result: two in flight.
    for _ in 0..5 {
        metrics.worker(1).captured(4, 2, 1);
    }
    metrics
        .worker(1)
        .installed(3, 1, 0, Duration::from_nanos(7), Duration::from_nanos(40));
    metrics
        .worker(0)
        .installed(0, 0, 2, Duration::ZERO, Duration::from_nanos(60));
    metrics.range(0).selected(SelectionReason::Pressure);
    metrics.range(1).selected(SelectionReason::Aged);
    metrics.range(1).selected(SelectionReason::Size);
    metrics.range(1).selected(SelectionReason::Lagging);
    metrics.range(2).queue_full();
    metrics.range(2).scanned(10, 4);
    metrics
        .store(0)
        .publish(&StoreSnapshot::default(), &store_stats(2, 100, 64));
    metrics
        .store(1)
        .publish(&StoreSnapshot::default(), &store_stats(1, 30, 256));
    let snapshot = metrics.snapshot(CellGauges::default());
    let events = snapshot.worker_totals();
    assert_eq!((events.dequeued, events.queue_wait_ns), (3, 15));
    assert_eq!(events.queue_wait_max_ns, 9);
    assert_eq!((events.installed, events.stale, events.restored), (3, 1, 2));
    assert_eq!((events.install_holds, events.install_hold_ns), (2, 100));
    assert_eq!(events.install_hold_max_ns, 60);
    let files = snapshot.store_totals();
    assert_eq!((files.frames, files.frame_bytes), (3, 130));
    assert_eq!(files.scratch_capacity_peak, 256);
    let ranges = snapshot.range_totals();
    assert_eq!(ranges.dispatched(), 4);
    assert_eq!(
        (ranges.queue_full, ranges.scanned_cells, ranges.dirty_cells),
        (1, 10, 4)
    );
    let names = names(&snapshot);
    assert_eq!(names["cell_dispatched"], 4);
    assert_eq!(names["cell_w2_queue_wait_max_ns"], 9);
    assert_eq!(names["cell_crc_bytes"], 130 - 3 * 8);
    assert_eq!(names["cell_blake2b_bytes"], 130);
    // Queue depth: sent minus taken, per range; in flight: captured minus
    // installed and restored.
    assert_eq!(names["cell_gauge_r0_queue_depth"], 0);
    assert_eq!(names["cell_gauge_r1_queue_depth"], 3);
    assert_eq!(names["cell_gauge_queue_depth"], 3);
    assert_eq!(names["cell_gauge_w1_in_flight"], 2);
    assert_eq!(names["cell_gauge_active_workers"], 1);
    // The pool's cheap totals agree with the snapshot.
    assert_eq!(metrics.worker_totals(), events);
    assert_eq!(metrics.range_totals(), ranges);
}

#[test]
fn store_counters_continue_across_writer_restarts() {
    let metrics = CellMetrics::new(1);
    let store = metrics.store(0);
    let base = store.base();
    store.publish(&base, &store_stats(1, 10, 32));
    store.publish(&base, &store_stats(4, 50, 32));
    assert_eq!(store.base().frames, 4);
    // A second writer starts from zero; its counters add to the first's.
    let base = store.base();
    store.publish(&base, &store_stats(2, 20, 16));
    let files = store.base();
    assert_eq!(
        (files.frames, files.frame_bytes, files.sealed_bytes),
        (6, 70, 70)
    );
    assert_eq!((files.file_syncs, files.segments_created), (2, 2));
    assert_eq!(files.scratch_capacity_peak, 32);
}

/// Eight workers record into their own blocks while a reader snapshots: no
/// update is lost and every snapshot is monotone.
#[test]
fn concurrent_recording_is_exact_and_snapshots_are_monotone() {
    const ROUNDS: u64 = 20_000;
    let metrics = CellMetrics::new(MAX_CELL_FLUSHERS);
    let start = Barrier::new(MAX_CELL_FLUSHERS + 1);
    let (done_sender, done) = sync_channel::<()>(MAX_CELL_FLUSHERS);
    std::thread::scope(|scope| {
        for worker in 0..MAX_CELL_FLUSHERS {
            let (metrics, start, done_sender) = (&metrics, &start, done_sender.clone());
            scope.spawn(move || {
                let _cpu = metrics.enter_worker(worker);
                start.wait();
                let base = metrics.store(worker).base();
                for round in 1..=ROUNDS {
                    metrics.worker(worker).dequeued(Duration::from_nanos(1));
                    metrics.worker(worker).captured(2, 1, 0);
                    metrics
                        .worker(worker)
                        .installed(1, 0, 0, Duration::ZERO, Duration::ZERO);
                    metrics.range(worker).selected(SelectionReason::Size);
                    metrics
                        .store(worker)
                        .publish(&base, &store_stats(round, round * 10, 8));
                }
                done_sender.send(()).unwrap();
            });
        }
        drop(done_sender);
        start.wait();
        let mut last = (0u64, 0u64, 0u64);
        let mut finished = 0;
        // Bounded: each iteration either observes a finished worker or takes
        // a snapshot, and the workers finish after ROUNDS updates each.
        while finished < MAX_CELL_FLUSHERS {
            let snapshot = metrics.snapshot(CellGauges::default());
            let now = (
                snapshot.worker_totals().captured,
                snapshot.store_totals().frames,
                snapshot.range_totals().dispatched(),
            );
            assert!(now.0 >= last.0 && now.1 >= last.1 && now.2 >= last.2);
            last = now;
            while done.try_recv().is_ok() {
                finished += 1;
            }
        }
    });
    let snapshot = metrics.snapshot(CellGauges::default());
    let total = ROUNDS * MAX_CELL_FLUSHERS as u64;
    let events = snapshot.worker_totals();
    assert_eq!(
        (events.dequeued, events.captured, events.installed),
        (total, total, total)
    );
    assert_eq!(events.captured_versions, 2 * total);
    assert_eq!(snapshot.store_totals().frames, total);
    assert_eq!(snapshot.store_totals().frame_bytes, 10 * total);
    assert_eq!(snapshot.range_totals().dispatched(), total);
    for worker in 0..MAX_CELL_FLUSHERS {
        assert_eq!(snapshot.worker[worker].captured, ROUNDS);
    }
}

#[test]
fn thread_usage_is_monotone_and_kept_after_exit() {
    // Other threads are read from schedstat; the thread's own final value
    // comes from its exact CPU clock.
    let available = thread_cpu_available();
    let metrics = CellMetrics::new(1);
    let (ready_sender, ready) = sync_channel::<u64>(1);
    let (release_sender, release) = sync_channel::<()>(1);
    let live = std::thread::scope(|scope| {
        let metrics = &metrics;
        scope.spawn(move || {
            let _cpu = metrics.enter_worker(0);
            // Fixed CPU work, then report this thread's own CPU time.
            let mut value = 0u64;
            for round in 0..20_000_000u64 {
                value = std::hint::black_box(value.wrapping_mul(31).wrapping_add(round));
            }
            std::hint::black_box(value);
            ready_sender.send(thread_cpu_ns().unwrap()).unwrap();
            release.recv().unwrap();
        });
        let own = ready.recv().unwrap();
        assert!(own > 0);
        let live = metrics.snapshot(CellGauges::default()).worker_threads[0].cpu_ns;
        release_sender.send(()).unwrap();
        (own, live)
    });
    let (own, live) = live;
    if !available {
        assert_eq!(live, 0);
    }
    // The guard dropped before the thread exited: its exact final usage
    // stays, and it is never below an earlier (possibly lagging) read.
    let snapshot = metrics.snapshot(CellGauges::default());
    let names = names(&snapshot);
    assert_eq!(
        names["cell_gauge_thread_cpu_available"],
        u64::from(available)
    );
    let last = snapshot.worker_threads[0].cpu_ns;
    assert!(
        last >= own && last >= live,
        "own {own} live {live} final {last}"
    );
    assert_eq!(names["cell_w0_thread_cpu_ns"], last);
    assert_eq!(names["cell_maintenance_cpu_ns"], last);
    let again = metrics.snapshot(CellGauges::default());
    assert_eq!(again.worker_threads[0].cpu_ns, last);
}

#[test]
fn proc_parsers_handle_odd_command_names() {
    assert_eq!(parse_schedstat("123456 789 10\n"), Some(123_456));
    assert_eq!(parse_schedstat(""), None);
    // Fields: pid (comm) state ppid pgrp session tty tpgid flags minflt
    // cminflt majflt ...; the command name holds spaces and parentheses.
    let stat = "42 (value (index) 0) S 1 42 42 0 -1 4194560 77 0 5 0 3 4 0 0 20 0 1 0";
    assert_eq!(parse_stat_faults(stat), Some((77, 5)));
    assert_eq!(parse_stat_faults("42 (x) S 1"), None);
    let usage = current_thread_usage().unwrap();
    assert!(usage.cpu_ns > 0);
}

#[test]
fn gauges_report_lags_only_within_one_epoch() {
    let metrics = CellMetrics::new(1);
    let since = Instant::now();
    metrics.scheduler_pass(
        PassGauges {
            target: Some(through(2, 40)),
            covered: Some(through(2, 30)),
            oldest_uncovered: Some(version(2, 31)),
            oldest_dirty_since: Some(since),
            dirty_cells: 5,
        },
        true,
        false,
    );
    let adopted = AdoptedGauges {
        generation: 7,
        target: through(2, 35),
        floor: through(2, 30),
        max_frame_cut: through(2, 38),
        history_floor: through(2, 12),
    };
    let names = names(&metrics.snapshot(CellGauges {
        published: Some(through(2, 45)),
        adopted: Some(adopted),
        ..CellGauges::default()
    }));
    assert_eq!(names["cell_gauge_lag_l_f_batches"], 15);
    assert_eq!(names["cell_gauge_lag_f_h_batches"], 18);
    assert_eq!(names["cell_gauge_lag_l_oldest_batches"], 14);
    assert_eq!(names["cell_gauge_lag_cross_epoch"], 0);
    assert_eq!(names["cell_gauge_root_generation"], 7);
    assert_eq!(names["cell_gauge_replay_floor_sequence"], 30);
    assert_eq!(names["cell_gauge_pressure_target_sequence"], 40);
    assert_eq!(names["cell_gauge_dirty_cells"], 5);
    assert!(names.contains_key("cell_gauge_oldest_uncovered_age_ms"));
    assert_eq!(names["cell_scheduler_targets_started"], 1);
    // F from an older epoch than L, and an empty history floor.
    let names = self::names(&metrics.snapshot(CellGauges {
        published: Some(through(3, 2)),
        adopted: Some(AdoptedGauges {
            history_floor: PrefixCut::BeforeAll,
            ..adopted
        }),
        ..CellGauges::default()
    }));
    assert!(!names.contains_key("cell_gauge_lag_l_f_batches"));
    assert!(!names.contains_key("cell_gauge_lag_f_h_batches"));
    assert_eq!(names["cell_gauge_lag_cross_epoch"], 1);
    assert_eq!(names["cell_gauge_history_floor_present"], 0);
}

#[test]
fn root_exact_admission_and_recovery_blocks() {
    let metrics = CellMetrics::new(1);
    metrics.root_captured(10, 2, 100);
    metrics.root_committed(1_000, 200, 50, 2, 30, 400);
    metrics.root_applied(20, 7, 700, 7, true);
    metrics.root_applied(20, 0, 0, 0, false);
    metrics.root_skipped_unchanged();
    let encode = CellEncodeStats {
        buffered_cells: 3,
        streamed_cells: 1,
        conservative_fallbacks: 1,
        empty_cells: 2,
        encode_passes: 5,
        row_passes: 8,
        references_encoded: 9,
        buffered_bytes: 300,
        streamed_bytes: 100,
        moved_bytes: 4,
        scratch_capacity: 512,
        ..CellEncodeStats::default()
    };
    metrics.exact_checkpoint(&encode, Duration::from_nanos(900));
    metrics.exact_thread_cpu(1_234);
    metrics.admission_blocked(BlockedDimension::ReplayTail, Duration::from_nanos(70));
    metrics.admission_blocked(BlockedDimension::Identities, Duration::from_nanos(5));
    metrics.admission_blocked(BlockedDimension::IndexBytes, Duration::from_nanos(6));
    metrics.admission_blocked(
        BlockedDimension::CheckpointMetadata,
        Duration::from_nanos(7),
    );
    metrics.admission_blocked(BlockedDimension::Other, Duration::from_nanos(8));
    metrics.recovery(
        &CellReplayStats {
            batches: 12,
            encoded_bytes: 4_096,
            fully_covered_batches: 3,
            skipped_batches: 4,
            applied_members: 20,
            covered_members: 6,
            offline_flush_passes: 1,
            offline_flushed_cells: 2,
            peak_resident_bytes: 65_536,
        },
        Duration::from_nanos(8),
    );
    let snapshot = metrics.snapshot(CellGauges::default());
    let names = names(&snapshot);
    assert_eq!(names["cell_root_applies"], 2);
    assert_eq!(names["cell_root_f_advances"], 1);
    assert_eq!(names["cell_root_returned_batches"], 7);
    assert_eq!(names["cell_root_skipped_unchanged"], 1);
    assert_eq!(names["cell_root_payload_cell_encodes"], 0);
    assert_eq!(names["cell_exact_checkpoints"], 1);
    assert_eq!(names["cell_exact_encoder_invocations"], 5);
    assert_eq!(names["cell_exact_encode_bytes"], 300 + 2 * 100);
    assert_eq!(names["cell_exact_blake2b_bytes"], 400);
    assert_eq!(names["cell_exact_crc_bytes"], 400 - 4 * 8);
    assert_eq!(names["cell_exact_scratch_capacity_peak"], 512);
    assert_eq!(names["cell_admission_blocked_replay_tail"], 1);
    assert_eq!(names["cell_admission_blocked_replay_tail_ns"], 70);
    assert_eq!(names["cell_admission_blocked_identities_ns"], 5);
    assert_eq!(names["cell_admission_blocked_index_bytes_ns"], 6);
    assert_eq!(names["cell_admission_blocked_checkpoint_metadata_ns"], 7);
    assert_eq!(names["cell_admission_blocked_other"], 1);
    assert_eq!(names["cell_admission_blocked_other_ns"], 8);
    assert_eq!(names["cell_recovery_batches"], 12);
    assert_eq!(names["cell_recovery_peak_resident_bytes"], 65_536);
    // Exact frames, root, wrapper and manifest bytes are index writes; the
    // Exact thread's CPU is maintenance CPU.
    assert_eq!(
        names["cell_logical_index_write_bytes"],
        400 + 1_000 + 200 + 50
    );
    assert_eq!(names["cell_maintenance_cpu_ns"], 1_234);
    // A real checkpoint thread adds its own, nonzero CPU time.
    std::thread::scope(|scope| {
        scope.spawn(|| metrics.exact_checkpoint_thread_exiting());
    });
    let cpu = metrics.snapshot(CellGauges::default()).exact.thread_cpu_ns;
    assert!(cpu > 1_234);
    metrics.root_payload_cell_encodes(1);
    let names = self::names(&metrics.snapshot(CellGauges::default()));
    assert_eq!(names["cell_root_payload_cell_encodes"], 1);
}

#[test]
fn snapshot_actor_thread_cpu_is_exact_at_exit() {
    let metrics = CellMetrics::new(1);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let _cpu = metrics.enter_snapshot();
            let mut value = 0u64;
            for round in 0..1_000_000u64 {
                value = std::hint::black_box(value ^ round);
            }
            std::hint::black_box(value);
        });
    });
    let snapshot = metrics.snapshot(CellGauges::default());
    assert!(snapshot.snapshot_thread.cpu_ns > 0);
    assert_eq!(
        snapshot.maintenance_cpu_ns(),
        snapshot.snapshot_thread.cpu_ns
    );
}

#[test]
fn flush_state_size_is_exported() {
    let names = names(&CellMetrics::new(1).snapshot(CellGauges::default()));
    assert_eq!(
        names["cell_gauge_flush_state_bytes"],
        CELL_FLUSH_STATE_BYTES as u64
    );
    // Reported in the C6b report (spec §11.4 asks for this size).
    eprintln!("CELL_FLUSH_STATE_BYTES={CELL_FLUSH_STATE_BYTES}");
}
