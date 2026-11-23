#![cfg(target_os = "linux")]

use super::*;
use std::fs::OpenOptions;

fn fixture() -> (tempdir::TempDir, LiveIo) {
    fixture_limits(8192, 4, 4 * (4096 + 4095))
}

#[test]
fn polled_ring_reads_or_reports_unsupported() {
    fn unsupported(error: LiveIoError) {
        let LiveIoError::Io(error) = error else {
            panic!("polled read failed without an OS unsupported error: {error}");
        };
        assert!(
            matches!(
                error.raw_os_error(),
                Some(libc::EINVAL | libc::EOPNOTSUPP | libc::ENOSYS)
            ),
            "unexpected polled I/O error: {error}"
        );
        eprintln!("local filesystem or kernel rejects polled I/O: {error}");
    }

    let (_dir, mut interrupt) = fixture();
    let expected = write_pattern(&mut interrupt, 0, 17);
    let mut polled = match LiveIo::new(interrupt.namespace.clone(), 4, 65536, true) {
        Ok(io) => io,
        Err(error) => {
            unsupported(error);
            return;
        }
    };
    let timeout = Duration::from_millis(10);
    let before_wait = (polled.stats.submit_ns, polled.stats.submissions);
    let started = Instant::now();
    assert!(!polled.wait(timeout).unwrap());
    assert!(started.elapsed() >= timeout);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "polled wait exceeded its bound"
    );
    assert_eq!(
        (polled.stats.submit_ns, polled.stats.submissions),
        before_wait
    );
    assert_eq!(
        polled
            .submit_reads(1, &[(20, 64)], Instant::now() + Duration::from_secs(10))
            .unwrap(),
        LiveSubmitOutcome::Submitted
    );
    assert!(polled.wait(Duration::from_secs(10)).unwrap());
    let mut completions = polled.progress().unwrap();
    assert_eq!(
        completions.len(),
        1,
        "the polled CQE retires exactly one completion"
    );
    let completed = completions.remove(0);
    match completed.result {
        Ok(LiveIoResult::Reads(records)) => {
            assert_eq!(records.len(), 1);
            assert_eq!(&records[0][..], &expected[20..84]);
        }
        Err(error) => unsupported(error),
        Ok(LiveIoResult::Written | LiveIoResult::Flushed) => {
            panic!("polled read returned a write completion")
        }
    }
    assert_eq!(polled.outstanding(), 0);
    assert!(polled.shutdown().unwrap().is_empty());
}

#[test]
fn polled_ring_writes_or_reports_unsupported() {
    let (_dir, interrupt) = fixture();
    let mut polled = match LiveIo::new(interrupt.namespace.clone(), 4, 65536, true) {
        Ok(io) => io,
        Err(LiveIoError::Io(error)) => {
            assert_iopoll_unsupported(error.raw_os_error());
            return;
        }
        Err(error) => panic!("polled write ring setup failed: {error}"),
    };
    let expected = vec![0xa5; 4096];
    for (id, sync) in [(1, LiveWriteSync::None), (2, LiveWriteSync::Data)] {
        assert_eq!(
            polled
                .submit_write(
                    id,
                    0,
                    &expected,
                    sync,
                    Instant::now() + Duration::from_secs(10)
                )
                .unwrap(),
            LiveSubmitOutcome::Submitted
        );
        assert!(polled.wait(Duration::from_secs(10)).unwrap());
        let mut completions = polled.progress().unwrap();
        assert_eq!(
            completions.len(),
            1,
            "the polled write retires exactly one completion"
        );
        let completed = completions.remove(0);
        assert_eq!(completed.id, id);
        match completed.result {
            Ok(LiveIoResult::Written) => {}
            Err(LiveIoError::UnknownWrite) => {
                assert_iopoll_unsupported(polled.last_write_errno());
                assert_eq!(polled.outstanding(), 0);
                assert!(polled.shutdown().unwrap().is_empty());
                return;
            }
            result => panic!("unexpected polled write completion: {result:?}"),
        }
    }
    let mut actual = vec![0; 4096];
    std::os::unix::fs::FileExt::read_exact_at(
        &std::fs::File::open(_dir.path().join("wal")).unwrap(),
        &mut actual,
        0,
    )
    .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(polled.stats().completed_write_ops, 2);
    assert!(polled.shutdown().unwrap().is_empty());
}

fn assert_iopoll_unsupported(errno: Option<i32>) {
    assert!(
        matches!(errno, Some(libc::EINVAL | libc::EOPNOTSUPP | libc::ENOSYS)),
        "unexpected polled I/O errno: {errno:?}"
    );
    eprintln!("local filesystem or kernel rejects polled I/O: errno={errno:?}");
}

#[test]
fn owned_write_buffer_recycles_on_drop_success_and_failed_cqe() {
    let (_dir, mut io) = fixture_limits(8192, 2, 8191);
    let initial = io.credit();
    let pointer = {
        let mut buffer = io.write_buffer(4096).unwrap();
        assert_eq!(buffer.as_ptr() as usize % 4096, 0);
        buffer.fill(0xa5);
        buffer.as_ptr()
    };
    assert_eq!(io.credit(), initial);
    assert_eq!(io.allocated_bytes, 8191);
    assert_eq!(io.pool.len(), 1);
    let deadline = Instant::now() + Duration::from_secs(10);
    for (id, result) in [(1, None), (2, Some(-libc::EIO)), (3, Some(2048))] {
        if let Some(result) = result {
            io.override_next_completion_result(result);
        }
        let mut buffer = io.write_buffer(4096).unwrap();
        assert_eq!(buffer.as_ptr(), pointer);
        for (index, byte) in buffer.iter_mut().enumerate() {
            *byte = (index % 251) as u8;
        }
        assert_eq!(
            buffer
                .submit_write_buffer(id, 0, LiveWriteSync::Data, deadline)
                .unwrap(),
            LiveSubmitOutcome::Submitted
        );
        assert_eq!(io.credit(), (1, 0));
        assert!(matches!(io.write_buffer(4096), Err(LiveIoError::Full)));
        let result = completion(&mut io).result;
        if id == 1 {
            assert!(matches!(result, Ok(LiveIoResult::Written)));
        } else {
            assert!(matches!(result, Err(LiveIoError::UnknownWrite)));
        }
        assert_eq!(io.credit(), initial);
        assert_eq!(io.pool.len(), 1);
        assert_eq!(io.allocated_bytes, 8191);
    }
    io.submit_reads(4, &[(0, 4096)], deadline).unwrap();
    let LiveIoResult::Reads(records) = completion(&mut io).result.unwrap() else {
        panic!("expected read result");
    };
    assert!(
        records[0]
            .iter()
            .enumerate()
            .all(|(index, byte)| *byte == (index % 251) as u8)
    );
    assert_eq!(io.credit(), initial);
}

#[test]
fn owned_write_buffer_rejects_unaligned_geometry_and_restores_credit() {
    let (_dir, mut io) = fixture();
    let initial = io.credit();
    assert!(matches!(
        io.write_buffer(4095),
        Err(LiveIoError::Invalid(_))
    ));
    assert!(matches!(
        io.write_buffer(4097),
        Err(LiveIoError::Invalid(_))
    ));
    let deadline = Instant::now() + Duration::from_secs(10);
    assert!(matches!(
        io.write_buffer(4096)
            .unwrap()
            .submit_write_buffer(1, 1, LiveWriteSync::None, deadline),
        Err(LiveIoError::Invalid(_))
    ));
    assert_eq!(io.credit(), initial);
    assert_eq!(io.pool.len(), 1);
    assert!(io.operations.is_empty());
    assert!(io.requests.is_empty());
}
fn fixture_limits(bytes: u64, slots: usize, budget: usize) -> (tempdir::TempDir, LiveIo) {
    let dir = tempdir::TempDir::new("live-namespace").unwrap();
    let path = dir.path().join("wal");
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.set_len(bytes).unwrap();
    let namespace =
        LiveNamespace::open_local_file(&path, bytes, 4096, LiveNamespaceAccess::ReadWrite).unwrap();
    let io = LiveIo::new(namespace, slots, budget, false).unwrap();
    (dir, io)
}
fn completion(io: &mut LiveIo) -> LiveCompletion {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(c) = io.progress().unwrap().pop() {
            return c;
        }
        assert!(
            Instant::now() < deadline,
            "local file operation did not retire"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn attached_files_read_the_selected_namespace_and_reject_wrong_block_size() {
    let (dir, mut io) = fixture();
    let mut slots = Vec::new();
    for (index, value) in [11u8, 22].into_iter().enumerate() {
        let path = dir.path().join(format!("attached-{index}"));
        std::fs::write(&path, [value; 8192]).unwrap();
        let namespace = Arc::new(
            LiveNamespace::open_local_file(&path, 8192, 4096, LiveNamespaceAccess::ReadShared)
                .unwrap(),
        );
        slots.push(io.attach(namespace).unwrap());
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    for (index, value) in [11u8, 22].into_iter().enumerate() {
        io.submit_reads_at(index as u64, slots[index], &[(17, 13)], deadline, None)
            .unwrap();
        let LiveIoResult::Reads(records) = completion(&mut io).result.unwrap() else {
            panic!("expected reads")
        };
        assert_eq!(&records[0][..], &[value; 13]);
    }
    let path = dir.path().join("attached-0");
    let different = Arc::new(
        LiveNamespace::open_local_file(&path, 8192, 512, LiveNamespaceAccess::ReadShared).unwrap(),
    );
    assert!(matches!(
        io.attach(different),
        Err(LiveIoError::Invalid(
            "attached namespace block size differs"
        ))
    ));
    let namespace =
        LiveNamespace::open_local_file(&path, 8192, 4096, LiveNamespaceAccess::ReadShared).unwrap();
    let mut readonly = LiveIo::new(namespace, 2, 16384, false).unwrap();
    assert!(matches!(
        readonly.submit_write(3, 0, &[0; 4096], LiveWriteSync::None, deadline),
        Err(LiveIoError::Invalid("namespace is read only"))
    ));
    assert!(matches!(
        readonly.write_buffer(4096),
        Err(LiveIoError::Invalid("namespace is read only"))
    ));
    assert_eq!(io.attached.len(), 2);
    io.shutdown().unwrap();
    assert!(matches!(
        io.attach(Arc::new(
            LiveNamespace::open_local_file(&path, 8192, 4096, LiveNamespaceAccess::ReadShared)
                .unwrap()
        )),
        Err(LiveIoError::Invalid("queue closed"))
    ));
}
#[test]
fn scalar_plans_and_member_metadata_stay_inline() {
    let (_dir, mut io) = fixture();
    let plan = io.plan_reads(io.namespace.bytes, &[(20, 8)]).unwrap();
    assert!(!plan.spilled());
    assert_eq!(plan.len(), 1);
    assert!(!plan[0].members.spilled());
    assert_eq!(io.read_request_charge(&[(20, 8)]).unwrap(), (1, 8191));
    let deadline = Instant::now() + Duration::from_secs(10);
    io.submit_write(1, 0, &[7; 4096], LiveWriteSync::Data, deadline)
        .unwrap();
    assert_eq!(io.requests[&1].records.capacity(), 0);
    assert!(!io.operations.values().next().unwrap().members.spilled());
    assert!(matches!(
        completion(&mut io).result,
        Ok(LiveIoResult::Written)
    ));
    io.submit_reads(2, &[(20, 8)], deadline).unwrap();
    assert!(!io.operations.values().next().unwrap().members.spilled());
    let LiveIoResult::Reads(records) = completion(&mut io).result.unwrap() else {
        panic!("expected reads")
    };
    assert_eq!(&records[0][..], &[7; 8]);
}
#[test]
fn local_file_exact_write_and_ordered_ranged_reads() {
    let (_dir, mut io) = fixture();
    let data: Vec<u8> = (0..4096).map(|n| (n % 251) as u8).collect();
    let deadline = Instant::now() + Duration::from_secs(10);
    assert_eq!(
        io.submit_write(1, 0, &data, LiveWriteSync::Data, deadline)
            .unwrap(),
        LiveSubmitOutcome::Submitted
    );
    assert!(matches!(
        completion(&mut io).result,
        Ok(LiveIoResult::Written)
    ));
    assert_eq!(
        io.submit_reads(2, &[(300, 30), (0, 10), (4090, 6)], deadline)
            .unwrap(),
        LiveSubmitOutcome::Submitted
    );
    let LiveIoResult::Reads(records) = completion(&mut io).result.unwrap() else {
        panic!("expected reads")
    };
    let records: Vec<Vec<u8>> = records.iter().map(|record| record[..].to_vec()).collect();
    assert_eq!(
        records,
        vec![
            data[300..330].to_vec(),
            data[..10].to_vec(),
            data[4090..].to_vec()
        ]
    );
    assert_eq!(io.stats().copy_bytes_avoided, 46);
    assert_eq!(io.stats().completions, 2);
    assert_eq!(io.stats().data_sync_writes, 1);
    assert_eq!(io.stats().merged_members, 2);
    assert_eq!(io.stats().read_sqes_saved, 2);
    assert_eq!(io.stats().barrier_count, 0);
    assert!(io.shutdown().unwrap().is_empty());
}
fn write_pattern(io: &mut LiveIo, offset: u64, seed: u8) -> Vec<u8> {
    let data: Vec<u8> = (0..4096).map(|n| (n % 250) as u8 ^ seed).collect();
    let deadline = Instant::now() + Duration::from_secs(10);
    assert_eq!(
        io.submit_write(
            900 + u64::from(seed),
            offset,
            &data,
            LiveWriteSync::None,
            deadline
        )
        .unwrap(),
        LiveSubmitOutcome::Submitted
    );
    assert!(matches!(completion(io).result, Ok(LiveIoResult::Written)));
    data
}
fn read_member(io: &mut LiveIo, id: u64, offset: u64, len: usize) -> Bytes {
    let deadline = Instant::now() + Duration::from_secs(10);
    assert_eq!(
        io.submit_reads(id, &[(offset, len)], deadline).unwrap(),
        LiveSubmitOutcome::Submitted
    );
    let completed = completion(io);
    assert_eq!(completed.id, id);
    let LiveIoResult::Reads(mut records) = completed.result.unwrap() else {
        panic!("expected reads")
    };
    records.pop().unwrap()
}
#[test]
fn member_slice_pins_its_block_until_every_slice_drops_then_recycles() {
    let (_dir, mut io) = fixture();
    let first = write_pattern(&mut io, 0, 1);
    let second = write_pattern(&mut io, 4096, 2);
    let slice = read_member(&mut io, 1, 10, 20);
    let part = slice.slice(0..5);
    assert_eq!(&slice[..], &first[10..30]);
    // The block is lent: outside the byte budget and not in the pool.
    io.reclaim();
    assert_eq!((io.pool.len(), io.allocated_bytes), (0, 0));
    // A later read must not reuse or overwrite the lent block.
    let other = read_member(&mut io, 2, 4096 + 10, 20);
    assert_eq!(&other[..], &second[10..30]);
    assert_eq!(&slice[..], &first[10..30]);
    drop(other);
    drop(slice);
    io.reclaim();
    // Another slice still references the first block.
    assert_eq!(io.pool.len(), 1);
    assert_eq!(&part[..], &first[10..15]);
    drop(part);
    io.reclaim();
    assert_eq!(io.pool.len(), 2);
    assert_eq!(io.allocated_bytes, 2 * (4096 + 4095));
    assert_eq!(io.stats().copy_bytes_avoided, 40);
    // A recycled block serves the next read without a new allocation.
    let again = read_member(&mut io, 3, 20, 4);
    assert_eq!(&again[..], &first[20..24]);
    assert_eq!(io.allocated_bytes, 4096 + 4095);
    assert!(io.shutdown().unwrap().is_empty());
}
#[test]
fn blocks_returned_together_never_queue_more_bytes_than_the_budget() {
    let block = 4096 + 4095;
    let dir = tempdir::TempDir::new("live-return-budget").unwrap();
    let path = dir.path().join("wal");
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.set_len(8192).unwrap();
    let namespace =
        LiveNamespace::open_local_file(&path, 8192, 4096, LiveNamespaceAccess::ReadWrite).unwrap();
    // Eight slots (so the channel could hold eight blocks) but a budget of two.
    let mut io = LiveIo::new(namespace, 8, 2 * block, false).unwrap();
    let data = write_pattern(&mut io, 0, 9);
    // Lent blocks leave the budget, so all eight reads succeed and stay retained.
    let retained: Vec<Bytes> = (0..8)
        .map(|n| read_member(&mut io, 20 + n, n * 4, 4))
        .collect();
    for (n, slice) in retained.iter().enumerate() {
        assert_eq!(&slice[..], &data[n * 4..n * 4 + 4]);
    }
    // Dropping them together offers eight blocks; only the budget is queued.
    drop(retained);
    let queued = io.lane.queued_bytes.load(Ordering::Relaxed);
    assert!(queued <= 2 * block, "queued {queued} > budget");
    assert_eq!(queued, 2 * block);
    io.reclaim();
    assert_eq!(io.lane.queued_bytes.load(Ordering::Relaxed), 0);
    assert_eq!(io.pool.len(), 2);
    assert!(io.allocated_bytes <= 2 * block);
    assert!(io.shutdown().unwrap().is_empty());
}
#[test]
fn retained_slices_neither_starve_the_queue_nor_dangle_after_it_drops() {
    let (_dir, mut io) = fixture();
    let data = write_pattern(&mut io, 0, 3);
    // The budget holds four blocks; lent blocks leave it.
    let retained: Vec<Bytes> = (0..8)
        .map(|n| read_member(&mut io, 10 + n, n * 4, 4))
        .collect();
    assert_eq!(io.allocated_bytes, 0);
    assert!(io.shutdown().unwrap().is_empty());
    drop(io);
    for (n, slice) in retained.iter().enumerate() {
        assert_eq!(&slice[..], &data[n * 4..n * 4 + 4]);
    }
    drop(retained);
}
#[test]
fn requests_run_in_flight_together_and_complete_independently() {
    let (_dir, mut io) = fixture();
    let first = write_pattern(&mut io, 0, 4);
    let second = write_pattern(&mut io, 4096, 5);
    let deadline = Instant::now() + Duration::from_secs(10);
    for (id, offset) in [(1, 0), (2, 4096), (3, 100)] {
        assert_eq!(
            io.submit_reads(id, &[(offset, 8)], deadline).unwrap(),
            LiveSubmitOutcome::Submitted
        );
    }
    // Submission starts the I/O; none of it waits for a progress call.
    assert_eq!(io.stats().submissions, 3 + 2);
    assert_eq!(io.outstanding(), 3);
    let mut done = Vec::new();
    let patience = Instant::now() + Duration::from_secs(10);
    while done.len() < 3 {
        assert!(Instant::now() < patience, "reads did not retire");
        done.extend(io.progress().unwrap());
    }
    done.sort_by_key(|c| c.id);
    let expected = [&first[..8], &second[..8], &first[100..108]];
    for (completion, want) in done.into_iter().zip(expected) {
        let LiveIoResult::Reads(records) = completion.result.unwrap() else {
            panic!("expected reads")
        };
        assert_eq!(&records[0][..], want);
    }
}
#[test]
fn truncated_local_file_short_read_retires_without_retry() {
    let (dir, mut io) = fixture();
    OpenOptions::new()
        .write(true)
        .open(dir.path().join("wal"))
        .unwrap()
        .set_len(4096)
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    assert_eq!(
        io.submit_reads(1, &[(0, 8192)], deadline).unwrap(),
        LiveSubmitOutcome::Submitted
    );
    let result = completion(&mut io);
    assert_eq!(result.id, 1);
    assert!(
        matches!(&result.result, Err(LiveIoError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof)
    );
    let stats = io.stats();
    assert_eq!(stats.read_operations, 1);
    assert_eq!(stats.submissions, 1);
    assert_eq!(stats.completions, 1);
    assert_eq!(stats.errors, 1);
    assert!(io.shutdown().unwrap().is_empty());
}
#[test]
fn failed_partial_preparation_restores_buffer_capacity() {
    let (_dir, mut io) = fixture_limits(16384, 4, 4 * (4096 + 4095));
    io.fail_buffer_after = Some(1);
    let deadline = Instant::now() + Duration::from_secs(10);
    assert!(io.submit_reads(1, &[(0, 1), (8192, 1)], deadline).is_err());
    assert_eq!(io.pool.len(), 1);
    assert_eq!(
        io.submit_reads(2, &[(0, 1)], deadline).unwrap(),
        LiveSubmitOutcome::Submitted
    );
    assert!(matches!(
        completion(&mut io).result,
        Ok(LiveIoResult::Reads(_))
    ));
    assert_eq!(io.stats().submissions, 1);
    assert_eq!(io.stats().completions, 1);
    assert!(io.shutdown().unwrap().is_empty());
}
#[test]
fn rejects_bad_ranges_and_cancelled_write_is_unknown_after_retirement() {
    let (_dir, mut io) = fixture();
    let deadline = Instant::now() + Duration::from_secs(10);
    assert!(
        io.submit_write(1, 1, &[0; 4096], LiveWriteSync::None, deadline)
            .is_err()
    );
    assert!(
        io.submit_write(1, 0, &[0; 17], LiveWriteSync::None, deadline)
            .is_err()
    );
    assert!(io.submit_reads(1, &[(8190, 4)], deadline).is_err());
    assert!(io.submit_reads(1, &[(0, 0)], deadline).is_err());
    assert_eq!(
        io.submit_write(2, 0, &[7; 4096], LiveWriteSync::None, deadline)
            .unwrap(),
        LiveSubmitOutcome::Submitted
    );
    io.cancel(2);
    assert!(matches!(
        completion(&mut io).result,
        Err(LiveIoError::UnknownWrite)
    ));
    assert_eq!(io.stats().completions, 1);
    assert_eq!(io.stats().cancellations, 1);
}
#[test]
fn wait_times_out_idle_and_submits_without_harvesting() {
    let (_dir, mut io) = fixture();
    let timeout = Duration::from_millis(10);
    let started = Instant::now();
    assert!(!io.wait(timeout).unwrap());
    assert!(started.elapsed() >= timeout);
    let deadline = Instant::now() + Duration::from_secs(10);
    // Internal submit keeps SQEs queued, unlike the eager public read API.
    let reads = io
        .plan_reads(io.namespace.bytes, &[(20, 4), (40, 4)])
        .unwrap();
    assert_eq!(
        io.submit(
            io.namespace.file.as_raw_fd(),
            1,
            reads,
            None,
            deadline,
            None
        )
        .unwrap(),
        LiveSubmitOutcome::Submitted
    );
    assert_eq!(io.stats().submissions, 0);
    assert!(io.wait(Duration::from_secs(10)).unwrap());
    assert_eq!(io.stats().submissions, 1);
    assert_eq!(io.stats().completions, 0);
    assert_eq!(io.outstanding(), 1);
    assert!(io.queued.is_empty());
    assert!(io.operations.values().all(|op| op.submitted.is_some()));
    assert!(matches!(
        completion(&mut io).result,
        Ok(LiveIoResult::Reads(_))
    ));
    assert_eq!(io.stats().completions, 1);
    // Also exercise the eager public API without a preceding progress.
    io.submit_reads(2, &[(0, 1)], deadline).unwrap();
    assert!(io.wait(Duration::from_secs(10)).unwrap());
    assert_eq!(completion(&mut io).id, 2);
}

#[test]
fn merged_reads_preserve_order_duplicates_touching_and_gaps() {
    let (_dir, mut io) = fixture_limits(16384, 4, 4 * (4096 + 4095));
    let first = write_pattern(&mut io, 0, 1);
    let second = write_pattern(&mut io, 4096, 2);
    let fourth = write_pattern(&mut io, 12288, 3);
    // Out of order, duplicate, overlapping, touching and separated by a gap.
    let ranges = [(4098, 6), (20, 8), (4090, 12), (20, 8), (12290, 4)];
    let (sqes, bytes) = io.read_request_charge(&ranges).unwrap();
    assert_eq!((sqes, bytes), (2, 8192 + 4095 + 4096 + 4095));
    let upper: usize = ranges
        .iter()
        .map(|&(o, n)| io.read_charge(o, n).unwrap())
        .sum();
    assert!(bytes < upper);
    let before = io.stats();
    io.submit_reads(1, &ranges, Instant::now() + Duration::from_secs(10))
        .unwrap();
    assert_eq!(io.outstanding(), 2);
    assert_eq!(io.used_bytes, bytes);
    assert_eq!(io.credit(), (2, io.max_bytes - bytes));
    let LiveIoResult::Reads(records) = completion(&mut io).result.unwrap() else {
        panic!("reads")
    };
    let crossing: Vec<u8> = first[4090..].iter().chain(&second[..6]).copied().collect();
    let expected: [&[u8]; 5] = [
        &second[2..8],
        &first[20..28],
        &crossing,
        &first[20..28],
        &fourth[2..6],
    ];
    for (record, expected) in records.iter().zip(expected) {
        assert_eq!(&record[..], expected);
    }
    let after = io.stats();
    assert_eq!(after.read_operations - before.read_operations, 2);
    assert_eq!(
        after.completed_read_bytes - before.completed_read_bytes,
        12288
    );
    assert_eq!(after.merged_members - before.merged_members, 3);
    assert_eq!(after.read_sqes_saved - before.read_sqes_saved, 3);
    assert_eq!(after.copy_bytes_avoided - before.copy_bytes_avoided, 38);
    assert_eq!(io.credit(), io.capacity());
    assert_eq!(io.allocated_bytes, 0);
    // The duplicate slices share their block, not separate return entries.
    let retained = records[1].clone();
    drop(records);
    io.reclaim();
    assert_eq!(io.pool.len(), 1);
    assert_eq!(io.allocated_bytes, 4096 + 4095);
    drop(retained);
    io.reclaim();
    assert_eq!(io.pool.len(), 2);
    assert_eq!(io.allocated_bytes, bytes);
}

#[test]
fn thirty_three_same_block_members_split_at_thirty_two() {
    let (_dir, mut io) = fixture_limits(8192, 2, 2 * (4096 + 4095));
    let data = write_pattern(&mut io, 0, 4);
    let ranges: Vec<_> = (0..33).map(|index| (index * 7, 3)).collect();
    assert_eq!(
        io.read_request_charge(&ranges).unwrap(),
        (2, 2 * (4096 + 4095))
    );
    io.submit_reads(1, &ranges, Instant::now() + Duration::from_secs(10))
        .unwrap();
    assert_eq!(io.outstanding(), 2);
    let LiveIoResult::Reads(records) = completion(&mut io).result.unwrap() else {
        panic!("reads")
    };
    assert_eq!(records.len(), 33);
    for (index, record) in records.iter().enumerate() {
        assert_eq!(&record[..], &data[index * 7..index * 7 + 3]);
    }
    assert_eq!(io.stats().read_operations, 2);
    assert_eq!(io.stats().read_sqes_saved, 31);
    assert_eq!(io.used_bytes, 0);
}

#[test]
fn merged_range_never_exceeds_byte_budget() {
    let (_dir, mut io) = fixture_limits(8192, 2, 4096 + 4095);
    let ranges = [(0, 1), (4096, 1)];
    let plan = io.plan_reads(io.namespace.bytes, &ranges).unwrap();
    assert_eq!(plan.len(), 2);
    assert!(plan.iter().all(|read| read.len + 4095 <= io.max_bytes));
    let deadline = Instant::now() + Duration::from_secs(10);
    assert_eq!(
        io.submit_reads(1, &ranges, deadline).unwrap(),
        LiveSubmitOutcome::Full
    );
    assert_eq!(
        io.submit_reads(2, &[(0, 8192)], deadline).unwrap(),
        LiveSubmitOutcome::Full
    );
    assert_eq!(io.outstanding(), 0);
    assert_eq!(io.used_bytes, 0);
    assert_eq!(io.allocated_bytes, 0);
    assert_eq!(
        io.submit_reads(3, &[(0, 1), (20, 1)], deadline).unwrap(),
        LiveSubmitOutcome::Submitted
    );
    assert_eq!(io.outstanding(), 1);
    assert_eq!(io.used_bytes, 8191);
    assert!(matches!(
        completion(&mut io).result,
        Ok(LiveIoResult::Reads(_))
    ));
    io.reclaim();
    assert_eq!(io.allocated_bytes, 8191);
}

#[test]
fn failed_merged_read_fails_request_and_restores_credit() {
    let (dir, mut io) = fixture();
    OpenOptions::new()
        .write(true)
        .open(dir.path().join("wal"))
        .unwrap()
        .set_len(4096)
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    io.submit_reads(1, &[(0, 8), (4096, 8), (4096, 8)], deadline)
        .unwrap();
    assert_eq!(io.outstanding(), 1);
    assert_eq!(io.used_bytes, 8192 + 4095);
    let completed = completion(&mut io);
    assert!(
        matches!(&completed.result, Err(LiveIoError::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof)
    );
    assert_eq!(io.credit(), io.capacity());
    assert!(io.requests.is_empty());
    assert_eq!(io.pool.len(), 1);
    assert_eq!(io.allocated_bytes, 8192 + 4095);
    assert_eq!(io.stats().copy_bytes_avoided, 0);
    assert_eq!(io.stats().errors, 1);
    assert_eq!(io.stats().completions, 1);
    assert_eq!(io.lane.queued_bytes.load(Ordering::Relaxed), 0);
    assert_eq!(&read_member(&mut io, 2, 0, 8)[..], &[0; 8]);
    assert_eq!(io.credit(), io.capacity());
}

#[test]
fn failed_merged_sqe_discards_successful_sibling_and_returns_both_blocks() {
    let (dir, mut io) = fixture_limits(16384, 4, 4 * 8191);
    OpenOptions::new()
        .write(true)
        .open(dir.path().join("wal"))
        .unwrap()
        .set_len(12288)
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    io.submit_reads(1, &[(0, 8), (8192, 8), (12288, 8)], deadline)
        .unwrap();
    assert_eq!(io.outstanding(), 2);
    assert!(matches!(
        completion(&mut io).result,
        Err(LiveIoError::Io(_))
    ));
    io.reclaim();
    assert_eq!(io.credit(), io.capacity());
    assert!(io.requests.is_empty());
    assert_eq!(io.pool.len(), 2);
    assert_eq!(io.allocated_bytes, 4096 + 4095 + 8192 + 4095);
    assert_eq!(io.lane.queued_bytes.load(Ordering::Relaxed), 0);
    assert_eq!(io.stats().completions, 2);
    assert_eq!(io.stats().errors, 1);
}

#[test]
fn merged_requests_use_exact_credit_and_cancel_only_after_retirement() {
    let (_dir, mut io) = fixture_limits(8192, 2, 2 * 8191);
    let deadline = Instant::now() + Duration::from_secs(10);
    io.submit_reads(1, &[(0, 8), (20, 8)], deadline).unwrap();
    assert_eq!(io.credit(), (1, 8191));
    io.submit_reads(2, &[(4096, 8), (4116, 8)], deadline)
        .unwrap();
    assert_eq!(io.credit(), (0, 0));
    assert_eq!(
        io.submit_reads(3, &[(0, 1)], deadline).unwrap(),
        LiveSubmitOutcome::Full
    );
    io.cancel(1);
    io.cancel(2);
    assert_eq!(io.credit(), (0, 0));
    let mut done = Vec::new();
    for _ in 0..100 {
        io.wait(Duration::from_millis(100)).unwrap();
        done.extend(io.progress().unwrap());
        if done.len() == 2 {
            break;
        }
    }
    assert_eq!(done.len(), 2);
    assert!(
        done.iter()
            .all(|c| matches!(c.result, Err(LiveIoError::Cancelled)))
    );
    assert_eq!(io.credit(), io.capacity());
    assert_eq!(io.pool.len(), 2);
    assert_eq!(io.allocated_bytes, 2 * 8191);
    assert_eq!(io.stats().copy_bytes_avoided, 0);
}

#[test]
fn namespace_flush_uses_one_slot_without_a_buffer_and_retires_unknown_outcomes() {
    let (_dir, mut io) = fixture();
    let deadline = Instant::now() + Duration::from_secs(10);
    let before = io.credit();
    let allocated = io.allocated_bytes;
    let id = io.submit_flush(100, deadline).unwrap();
    assert_eq!(id, 100);
    assert_eq!(io.credit().0, before.0 - 1);
    assert_eq!(io.credit().1, before.1);
    assert_eq!(io.allocated_bytes, allocated);
    assert!(matches!(
        completion(&mut io).result,
        Ok(LiveIoResult::Flushed)
    ));
    assert_eq!(io.credit(), before);
    assert_eq!(io.stats().barrier_count, 1);
    assert_eq!(io.stats().completed_write_ops, 0);
    assert_eq!(io.stats().completed_read_ops, 0);
    for (id, expire, cancel) in [(101, false, false), (102, true, false), (103, false, true)] {
        if !expire && !cancel {
            io.override_completion_result(id, -libc::EIO);
        }
        io.submit_flush(id, deadline).unwrap();
        if expire {
            io.requests.get_mut(&id).unwrap().deadline = Instant::now();
        }
        if cancel {
            io.cancel(id);
        }
        assert!(matches!(
            completion(&mut io).result,
            Err(LiveIoError::UnknownWrite)
        ));
        assert_eq!(io.credit(), before);
        assert_eq!(io.allocated_bytes, allocated);
    }
}

#[test]
fn failed_and_expired_data_sync_writes_remain_unknown() {
    let (_dir, mut io) = fixture();
    let deadline = Instant::now() + Duration::from_secs(10);
    io.submit_write(1, 0, &[9; 4096], LiveWriteSync::Data, deadline)
        .unwrap();
    // Force expiry after admission but before harvesting a terminal CQE.
    io.requests.get_mut(&1).unwrap().deadline = Instant::now();
    assert!(matches!(
        completion(&mut io).result,
        Err(LiveIoError::UnknownWrite)
    ));
    assert_eq!(io.stats().timeouts, 1);
    assert_eq!(io.stats().data_sync_writes, 1);
    assert_eq!(io.credit(), io.capacity());
    let (dir, mut failed) = fixture();
    // A read-only file descriptor forces a real failing write CQE.
    Arc::get_mut(&mut failed.namespace).unwrap().file =
        std::fs::File::open(dir.path().join("wal")).unwrap();
    failed
        .submit_write(2, 0, &[9; 4096], LiveWriteSync::Data, deadline)
        .unwrap();
    assert!(matches!(
        completion(&mut failed).result,
        Err(LiveIoError::UnknownWrite)
    ));
    assert_eq!(failed.stats().errors, 1);
    assert_eq!(failed.credit(), failed.capacity());
    assert_eq!(failed.allocated_bytes, 8191);
}

#[test]
fn terminal_write_result_override_is_one_shot_and_keeps_unknown_outcomes() {
    let (_dir, mut io) = fixture();
    let deadline = Instant::now() + Duration::from_secs(10);
    for (id, result) in [(1, 2048), (2, -libc::EIO)] {
        io.override_next_completion_result(result);
        io.submit_write(id, 0, &[7; 4096], LiveWriteSync::Data, deadline)
            .unwrap();
        assert_eq!(io.outstanding(), 1);
        assert!(matches!(
            completion(&mut io).result,
            Err(LiveIoError::UnknownWrite)
        ));
        assert!(io.next_completion_result.is_none());
        assert_eq!(io.credit(), io.capacity());
        assert_eq!(io.allocated_bytes, 8191);
    }
    io.submit_write(3, 0, &[8; 4096], LiveWriteSync::Data, deadline)
        .unwrap();
    assert!(matches!(
        completion(&mut io).result,
        Ok(LiveIoResult::Written)
    ));
    assert_eq!(io.stats().errors, 2);
    assert_eq!(io.stats().failed_write_ops, 1);
    assert_eq!(io.stats().short_write_ops, 1);
    assert_eq!(io.stats().short_write_bytes, 2048);
    assert_eq!(io.stats().completed_write_ops, 1);
    assert_eq!(io.stats().completions, 3);
}

/// Unit test of `MemberBuffer` alone: a member slice is a refcounted view of a
/// lent read block, and the block cannot return to its queue, and so cannot be
/// reused for another read, while any slice lives. The lane-level statement is
/// `a_retained_read_is_never_overwritten_and_its_block_returns_after_the_drop`.
#[test]
fn a_lent_block_returns_only_after_its_last_member_slice_is_dropped() {
    let (sender, returns) = mpsc::sync_channel(4);
    let lane = Arc::new(ReturnLane {
        sender,
        queued_bytes: AtomicUsize::new(0),
        max_bytes: 1 << 20,
    });
    let mut buffer = Buffer::new(4096, 512);
    buffer.slice_mut()[..8].copy_from_slice(b"member-A");
    buffer.slice_mut()[8..16].copy_from_slice(b"member-B");
    let whole = Bytes::from_owner(MemberBuffer {
        buffer: Some(buffer),
        lane,
    });
    let first = whole.slice(0..8);
    let second = whole.slice(8..16);
    drop(whole);
    assert!(returns.try_recv().is_err(), "returned with slices alive");
    drop(second);
    assert!(returns.try_recv().is_err(), "returned with one slice alive");
    assert_eq!(&first[..], b"member-A", "a live slice changed");
    drop(first);
    assert!(returns.try_recv().is_ok(), "the block was never returned");
}

/// The lane half of the frame-ownership contract documented on
/// `ReadRequest::finish_member`, on a real `LiveIo` with a budget of exactly
/// one block: read A and keep it, read B through the same lane while A is
/// held, then drop A and read C.
///
/// * Lending released A's charge (`allocated_bytes` is 0 and `credit` is
///   unchanged), so B is admitted and gets a fresh block rather than A's:
///   different pointers, A's bytes
///   unchanged. A broken lend (block pooled at completion) would hand B A's
///   block and overwrite it, and fail here.
/// * The block is in the pool only after A's last slice drops: while A is held
///   the pool is empty after `reclaim`, afterwards it holds exactly A's block,
///   and C reads into that same block (pointer equality).
/// * The budget bounds only the pool, not caller-held memory: B's block is
///   dropped last and, the one-block budget being full, is freed instead of
///   pooled.
#[test]
fn a_retained_read_is_never_overwritten_and_its_block_returns_after_the_drop() {
    let block = 4096 + 4095;
    let (_dir, mut io) = fixture_limits(8192, 2, block);
    let first = write_pattern(&mut io, 0, 1);
    let second = write_pattern(&mut io, 4096, 2);
    let initial = io.credit();

    let a = read_member(&mut io, 1, 0, 64);
    let a_ptr = a.as_ptr();
    assert_eq!(&a[..], &first[..64]);
    // Lent: the block left the byte budget and is not in the pool.
    io.reclaim();
    assert_eq!((io.pool.len(), io.allocated_bytes), (0, 0));
    assert_eq!(io.credit(), initial, "a retained A holds in-flight credit");

    let b = read_member(&mut io, 2, 4096, 64);
    assert_eq!(&b[..], &second[..64]);
    assert_ne!(a_ptr, b.as_ptr(), "B was read into A's block");
    assert_eq!(&a[..], &first[..64], "reading B changed the retained A");
    assert_ne!(&a[..], &b[..]);
    io.reclaim();
    assert_eq!(io.pool.len(), 0, "a block was pooled while it was retained");

    drop(a);
    io.reclaim();
    assert_eq!(io.pool.len(), 1, "A's block did not return after the drop");
    assert_eq!(io.pool[0].slice().as_ptr(), a_ptr);

    drop(b);
    io.reclaim();
    assert_eq!(io.pool.len(), 1, "a block beyond the budget was pooled");

    let c = read_member(&mut io, 3, 0, 64);
    assert_eq!(c.as_ptr(), a_ptr, "C did not reuse the returned block");
    assert_eq!(&c[..], &first[..64]);
    drop(c);
    assert!(io.shutdown().unwrap().is_empty());
}
