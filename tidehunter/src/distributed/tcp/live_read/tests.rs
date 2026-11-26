use super::*;
use crate::distributed::codec::CompleteBatch;
use crate::distributed::nvmeof::live_namespace::LiveNamespace;
use crate::distributed::test_bounds::PROGRESS_CAP;
use crate::distributed::types::PhysicalValueAddress;
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;

#[path = "subsystem_tests.rs"]
mod subsystem_tests;

#[path = "reconnect_tests.rs"]
mod reconnect_tests;

#[test]
fn namespace_device_is_owned_before_the_cached_grant_is_retired() {
    let mut lane = lane(4);
    let device = lane.cache.namespace_device().unwrap().unwrap();
    lane.cache.grants.clear();
    assert_eq!(
        device,
        std::fs::canonicalize(lane._dir.path().join("wal-0")).unwrap()
    );
    assert!(lane.cache.namespace_device().is_none());
}

#[test]
fn failed_grant_drains_error_cqe_before_releasing_flights() {
    let mut lane = lane(4);
    let namespace = LiveNamespace::open_local_file(
        &lane._dir.path().join("wal-0"),
        8192,
        4096,
        LiveNamespaceAccess::ReadOnly,
    )
    .unwrap();
    let mut inline = LiveIo::new(namespace, 4, 65536, false).unwrap();
    inline
        .submit_reads(99, &[(0, 1)], Instant::now() + Duration::from_secs(10))
        .unwrap();
    lane.cache.grants[0].inline_io = Some(inline);
    assert!(!lane.cache.inline_ready(&lane.a));
    let token = lane.submit(vec![lane.a]);
    assert_eq!(lane.cache.grants[0].io.outstanding(), 1);
    lane.cache.grants[0]
        .io
        .override_next_completion_result(-libc::EIO);
    lane.cache
        .fail_grant(&lane.client, 0, &TcpError::new("injected ring failure"));
    assert_eq!(lane.cache.grants[0].io.outstanding(), 0);
    assert_eq!(lane.cache.grants[0].outstanding(), 0);
    assert_eq!(lane.cache.grants[0].inline_recorded.read_requests, 1);
    assert!(lane.cache.grants[0].broken);
    assert_eq!(lane.cache.active_requests, 0);
    let state = lane.cache.requests.remove(&token).unwrap();
    assert_eq!(state.flights, 0);
    let done = LiveReadCache::finish(&lane.client, token, state);
    assert!(done.result.is_err());
}

pub(crate) fn configure_local_client(client: &mut Client) {
    client.live_config = Some(super::super::live_client::LiveClientConfig {
        lease_ahead: true,
        polled_writes: false,
        host_nqn: "nqn.2026-01.test:inline".into(),
        host_id: [1; 16],
        owner_identities: Default::default(),
        leased_write_window: Some(4),
        leased_sync: Default::default(),
        leased_flush_min_frame_bytes: 0,
        max_inflight_requests: 4,
        max_inflight_bytes: 65536,
        connect_timeout_ms: 1000,
        io_timeout_ms: 10_000,
        leased_durability: crate::distributed::durability::DurabilityTier::Native,
        inline_reads: true,
        queue_geometry: Default::default(),
    });
}

/// Pin the owner's real WAL inode and open it as a local namespace. Only
/// target/controller setup is replaced; validation and pin release are real.
pub(crate) fn install_owner_local(
    cache: &mut LiveReadCache,
    node: &super::super::StorageNode,
    position: VersionPosition,
) -> std::path::PathBuf {
    use crate::distributed::nvmeof::live_storage::LiveTargetBinding;
    // The position names the member frame itself: no descriptor to derive it from.
    let member = position.address;
    let generation = member.segment_generation;
    let mut inventory = node.inventory.lock();
    let lease = inventory
        .active(generation)
        .unwrap()
        .resolve_live_member(member)
        .unwrap();
    let segment = lease.segment.clone();
    let path = segment.path.clone();
    let location = LiveRecordLocation {
        member,
        offset: lease.offset,
        len: u64::from(lease.len),
        binding: LiveTargetBinding {
            nqn: "nqn.2026-01.test:wal".into(),
            uuid: [1; 16],
            nsid: 1,
            bytes: segment.bytes,
            block_size: segment.block_size,
            address: "127.0.0.1".into(),
            service: 4420,
            transport: crate::distributed::nvmeof::NvmeofTransport::Tcp,
            readonly: true,
            permanent: false,
        },
        segment,
    };
    let token = [42; 16];
    assert!(
        inventory
            .live_pins
            .insert(token, (generation, vec![lease]))
            .is_none()
    );
    drop(inventory);
    install_existing_local(cache, location, token);
    path
}

pub(crate) fn install_existing_local(
    cache: &mut LiveReadCache,
    location: LiveRecordLocation,
    token: [u8; 16],
) {
    let segment = &location.segment;
    let namespace = LiveNamespace::open_local_file(
        &segment.path,
        segment.bytes,
        segment.block_size,
        LiveNamespaceAccess::ReadOnly,
    )
    .unwrap();
    let io = LiveIo::new(namespace, 4, 65536, false).unwrap();
    let recorded = io.stats();
    cache.grants.push(Grant {
        id: cache.next_grant,
        scope: GrantScope::File(location),
        token: Some(token),
        io,
        inline_io: None,
        inline_recorded: LiveIoStats::default(),
        recorded,
        last_used: 0,
        broken: false,
        lost: false,
        #[cfg(test)]
        controller_lost: None,
        lease: None,
    });
    cache.next_grant += 1;
}

#[test]
fn cached_scalar_matches_lane_and_preserves_grant_credit() {
    let mut lane = lane(4);
    let before = local_snapshot(&lane.cache);
    assert!(lane.cache.inline_ready(&lane.a));
    assert!(lane.cache.inline_credit(&lane.a));
    let inline = lane
        .cache
        .read_inline_cached(
            &lane.client,
            &lane.a,
            Instant::now() + Duration::from_secs(10),
            None,
        )
        .unwrap();
    assert_eq!(local_snapshot(&lane.cache), before);
    let token = lane.submit(vec![lane.a]);
    assert!(!lane.cache.inline_ready(&lane.a));
    let mut done = lane.collect(1);
    assert_eq!(done[0].token, token);
    assert_eq!(done.pop().unwrap().result.unwrap(), inline);
    assert_eq!(local_snapshot(&lane.cache), before);
}

#[test]
fn cached_scalar_uses_separate_inline_queue_and_folds_stats() {
    let mut lane = lane(4);
    let namespace = LiveNamespace::open_local_file(
        &lane._dir.path().join("wal-0"),
        8192,
        4096,
        LiveNamespaceAccess::ReadOnly,
    )
    .unwrap();
    // An interrupt fixture exercises the same ring selection on filesystems
    // that reject IOPOLL. The LiveIo test probes IOPOLL itself.
    lane.cache.grants[0].inline_io = Some(LiveIo::new(namespace, 1, 8191, false).unwrap());
    assert!(lane.cache.inline_ready(&lane.a));
    assert!(lane.cache.inline_credit(&lane.a));
    let inline = lane
        .cache
        .read_inline_cached(
            &lane.client,
            &lane.a,
            Instant::now() + Duration::from_secs(10),
            None,
        )
        .unwrap();
    let grant = &lane.cache.grants[0];
    assert_eq!(grant.io.stats().read_requests, 0);
    assert_eq!(grant.inline_recorded.read_requests, 1);
    assert_eq!(grant.outstanding(), 0);
    lane.submit(vec![lane.a]);
    let mut done = lane.collect(1);
    assert_eq!(done.pop().unwrap().result.unwrap(), inline);
    let grant = &lane.cache.grants[0];
    assert_eq!(grant.recorded.read_requests, 1);
    assert_eq!(grant.inline_recorded.read_requests, 1);
}

#[test]
fn cached_scalar_corruption_returns_credit_and_the_hybrid_decode_refuses() {
    let mut lane = lane(4);
    let before = local_snapshot(&lane.cache);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(lane._dir.path().join("wal-0"))
        .unwrap();
    let offset = lane.a.address.offset % 8192 + u64::from(lane.a.address.len) - 1;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&[0xff]).unwrap();
    // The primitive returns the bytes and releases the credit; the payload
    // CRC is refused upstream by `hybrid::read::decode_entry_at`.
    let frame = lane
        .cache
        .read_inline_cached(
            &lane.client,
            &lane.a,
            Instant::now() + Duration::from_secs(10),
            None,
        )
        .expect("the primitive returns the corrupted member bytes");
    assert_ne!(crc32fast::hash(&frame[0][8..]), lane.a.payload_crc);
    assert_eq!(local_snapshot(&lane.cache), before);
}

#[test]
fn cached_scalar_expired_deadline_does_not_submit_or_drop_pin() {
    let mut lane = lane(4);
    let before = local_snapshot(&lane.cache);
    assert!(
        lane.cache
            .read_inline_cached(&lane.client, &lane.a, Instant::now(), None)
            .is_err()
    );
    assert_eq!(local_snapshot(&lane.cache), before);
    assert_eq!(lane.metrics.snapshot().counts.completed_read_ops, 0);
}

/// A read whose CQE reports success is a success even when the queue
/// observes it past the flight deadline. The grant keeps its credit and
/// pin until that terminal CQE, and the late observation is not counted
/// as a timeout.
#[test]
fn cached_scalar_late_success_holds_credit_until_terminal_cqe() {
    let mut lane = lane(4);
    let before = local_snapshot(&lane.cache);
    let (entered, waiting) = std::sync::mpsc::sync_channel(1);
    let (release, resume) = std::sync::mpsc::sync_channel(1);
    lane.cache.inline_submitted_pause = Some((entered, resume));
    let worker = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_millis(20);
        let result = lane
            .cache
            .read_inline_cached(&lane.client, &lane.a, deadline, None);
        (lane, result)
    });
    // The pause handshake follows worker progress.
    waiting.recv_timeout(PROGRESS_CAP).unwrap();
    // The read's 20 ms deadline was taken before the worker entered the
    // pause, so 30 ms after entry is strictly past it. A floor: machine
    // load only lengthens the overdue window, never shortens it.
    std::thread::sleep(Duration::from_millis(30));
    release.send(()).unwrap();
    let (lane, result) = worker.join().unwrap();
    let frames = result.expect("a terminal success stays a success");
    assert_eq!(frames, vec![member_frame(&lane.first, &lane.a)]);
    assert_eq!(local_snapshot(&lane.cache), before);
    assert_eq!(
        lane.cache.grants[0].io.stats().timeouts,
        0,
        "a late success is not a timeout"
    );
}

pub(crate) fn local_snapshot(cache: &LiveReadCache) -> (usize, usize, usize) {
    (
        cache.grants.len(),
        cache.grants.iter().map(|g| g.io.credit().1).sum(),
        cache.grants.iter().map(|g| g.io.outstanding()).sum(),
    )
}

#[test]
fn local_file_reads_decode_order_duplicates_and_corruption() {
    let dir = tempdir::TempDir::new("live-read-members").unwrap();
    let config = super::super::tests::config(dir.path());
    let first = super::super::tests::batch(&config, 1, 0);
    let second = super::super::tests::batch(&config, 1, 1);
    let address = |offset, batch: &CompleteBatch| PhysicalValueAddress {
        partition: 2,
        segment_generation: 1,
        offset,
        len: (batch.encoded().len() + 8) as u32,
    };
    let a = position(address(0, &first), &first);
    let b = position(address(2048, &second), &second);
    let path = dir.path().join("wal");
    let mut file = std::fs::File::create(&path).unwrap();
    file.set_len(8192).unwrap();
    file.seek(SeekFrom::Start(8)).unwrap();
    file.write_all(first.encoded()).unwrap();
    file.seek(SeekFrom::Start(2056)).unwrap();
    file.write_all(second.encoded()).unwrap();
    drop(file);
    let namespace =
        LiveNamespace::open_local_file(&path, 8192, 4096, LiveNamespaceAccess::ReadOnly).unwrap();
    let mut io = LiveIo::new(namespace, 4, 65536, false).unwrap();
    let positions = [b, a, b];
    let ranges: Vec<_> = positions
        .iter()
        .map(|p| (p.address.offset % 8192, p.address.len as usize))
        .collect();
    let deadline = Instant::now() + Duration::from_secs(10);
    assert_eq!(
        io.submit_reads(1, &ranges, deadline).unwrap(),
        LiveSubmitOutcome::Submitted
    );
    let completion = loop {
        if let Some(completion) = io.progress().unwrap().pop() {
            break completion;
        }
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    };
    let LiveIoResult::Reads(records) = completion.result.unwrap() else {
        panic!("expected member reads")
    };
    // The transport returns exactly the member frame of each requested
    // position, in request order. Expectation comes from the batch encoding,
    // not from the file the lane reads, so an address bug cannot move both.
    let expected = vec![
        member_frame(&second, &b),
        member_frame(&first, &a),
        member_frame(&second, &b),
    ];
    assert_eq!(records, expected);
    // Each fetched frame carries a length header that matches its payload;
    // bytes 4..8 are the identity checksum, not a plain CRC of the entry.
    let batches = [&second, &first, &second];
    for (frame, batch) in records.iter().zip(batches) {
        let declared = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
        assert_eq!(declared, frame.len() - 8);
        match crate::db::WalEntry::from_bytes(Bytes::copy_from_slice(&frame[8..])) {
            crate::db::WalEntry::Record(_, key, value, _) => {
                let crate::distributed::codec::Mutation::Put {
                    key: expected_key,
                    value: expected_value,
                    ..
                } = &batch.mutations()[0]
                else {
                    panic!("fixture batch holds a put");
                };
                assert_eq!(&key, expected_key);
                assert_eq!(&value, expected_value);
            }
            _ => panic!("member entry is a record"),
        }
    }
    let mut corrupt = records[0].to_vec();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 1;
    // The reader's integrity check is the payload CRC over `frame[8..]`.
    assert_ne!(
        crc32fast::hash(&corrupt[8..]),
        crc32fast::hash(&records[0][8..])
    );
    assert!(io.shutdown().unwrap().is_empty());
}

#[test]
fn least_recently_used_idle_grant_is_the_victim() {
    // (index, evictable, last_used): a busy or broken grant is never chosen,
    // and a recently used one outlives an older one.
    let grants = [(0, true, 5), (1, true, 2), (2, false, 1), (3, true, 9)];
    assert_eq!(lru_victim(grants.into_iter()), Some(1));
    assert_eq!(lru_victim([(0, false, 1), (1, false, 2)].into_iter()), None);
    assert_eq!(lru_victim(std::iter::empty()), None);
}

fn thread_cpu() -> Duration {
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `now` is a valid timespec and the clock id is a constant.
    let read = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut now) };
    assert_eq!(read, 0);
    Duration::new(now.tv_sec as u64, now.tv_nsec as u32)
}

#[test]
fn idle_wait_blocks_without_spinning_and_a_waker_ends_it() {
    let mut cache = LiveReadCache::default();
    let cpu = thread_cpu();
    let started = Instant::now();
    assert_eq!(
        cache.wait(Duration::from_millis(200)).unwrap(),
        LiveReadWait::TimedOut
    );
    assert!(started.elapsed() >= Duration::from_millis(190));
    // A spin would use about 200 ms of CPU; a sleeping thread uses almost none.
    assert!(thread_cpu() - cpu < Duration::from_millis(40));

    let waker = cache.waker().unwrap();
    let sender = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        waker.wake().unwrap();
    });
    // `Woken` (not `TimedOut` after the wait's own 10 s deadline) proves
    // the wake ended the wait; no clock threshold is asserted because heavy
    // load may delay the waker thread arbitrarily.
    assert_eq!(
        cache.wait(Duration::from_secs(10)).unwrap(),
        LiveReadWait::Woken
    );
    sender.join().unwrap();
    // The wake was consumed: the next wait blocks again.
    assert_eq!(
        cache.wait(Duration::from_millis(30)).unwrap(),
        LiveReadWait::TimedOut
    );
}

struct Lane {
    _dir: tempdir::TempDir,
    client: Client,
    cache: LiveReadCache,
    metrics: Arc<crate::distributed::nvmeof::live_metrics::LiveNvmeMetrics>,
    first: CompleteBatch,
    second: CompleteBatch,
    a: VersionPosition,
    b: VersionPosition,
    // Keeps the node behind the client alive.
    _running: super::super::tests::multiplexed::Running,
}

/// The member position of a fixture batch's first record at `batch_address`.
///
/// `payload_crc` is `crc32` of the member's native entry — the slice a
/// publisher CRCs and the slice the reader recomputes — not the identity
/// `MemberChecksum` stored at bytes 4..8 of the member frame, which binds
/// identity, version, bounds and the entry and is not a plain CRC.
fn position(batch_address: PhysicalValueAddress, batch: &CompleteBatch) -> VersionPosition {
    let descriptor = batch.record_descriptors()[0];
    let entry = &batch.encoded()
        [descriptor.offset as usize + 8..(descriptor.offset + descriptor.len) as usize];
    VersionPosition::member(
        batch.version(),
        batch_address,
        0,
        descriptor,
        crc32fast::hash(entry),
    )
}

/// The member frame `position` names, derived from the batch encoding
/// rather than from the file the lane will read.
///
/// Deriving it here is what makes the oracle independent: a wrong `+8` in
/// `VersionPosition::member` would otherwise move the read and the
/// expectation together and still pass. The descriptor is the same one the
/// position was built from, so the length must agree as well.
fn member_frame(batch: &CompleteBatch, position: &VersionPosition) -> Bytes {
    let descriptor = batch.record_descriptors()[position.operation_index as usize];
    assert_eq!(
        descriptor.len, position.address.len,
        "the position names this member's frame"
    );
    Bytes::copy_from_slice(
        &batch.encoded()[descriptor.offset as usize..(descriptor.offset + descriptor.len) as usize],
    )
}

/// A lane over two local-file grants: `a` lives in file 0, `b` in file 1.
fn lane(slots: usize) -> Lane {
    use crate::distributed::nvmeof::NvmeofTransport;
    use crate::distributed::nvmeof::live_metrics::LiveNvmeMetrics;
    use crate::distributed::nvmeof::live_storage::LiveTargetBinding;
    use crate::distributed::storage::WalMemberReadPolicy;
    let running = super::super::tests::multiplexed::Running::new(
        super::super::Transport::Tcp,
        WalMemberReadPolicy::Async,
    );
    let dir = tempdir::TempDir::new("live-lane").unwrap();
    let first = super::super::tests::batch(&running.config, 1, 0);
    let second = super::super::tests::batch(&running.config, 1, 1);
    let address = |offset, batch: &CompleteBatch| PhysicalValueAddress {
        partition: 2,
        segment_generation: 1,
        offset,
        len: (batch.encoded().len() + 8) as u32,
    };
    let a = position(address(0, &first), &first);
    let b = position(address(8192, &second), &second);
    let mut client = running.client();
    let metrics = LiveNvmeMetrics::new();
    client.live_counters = Some(metrics.register().unwrap());
    client.live_config = Some(super::super::live_client::LiveClientConfig {
        lease_ahead: true,
        polled_writes: false,
        host_nqn: "nqn.2026-01.test:host".into(),
        host_id: [1; 16],
        owner_identities: Default::default(),
        leased_write_window: Some(4),
        leased_sync: Default::default(),
        leased_flush_min_frame_bytes: 0,
        max_inflight_requests: slots,
        max_inflight_bytes: 16 * (4096 + 4095),
        connect_timeout_ms: 1000,
        io_timeout_ms: 10_000,
        leased_durability: crate::distributed::durability::DurabilityTier::Native,
        inline_reads: true,
        queue_geometry: Default::default(),
    });
    let mut cache = LiveReadCache::with_max_requests(4);
    for (file, (batch, position)) in [(&first, a), (&second, b)].into_iter().enumerate() {
        let path = dir.path().join(format!("wal-{file}"));
        let mut data = std::fs::File::create(&path).unwrap();
        data.set_len(8192).unwrap();
        data.seek(SeekFrom::Start(8)).unwrap();
        data.write_all(batch.encoded()).unwrap();
        drop(data);
        let namespace =
            LiveNamespace::open_local_file(&path, 8192, 4096, LiveNamespaceAccess::ReadOnly)
                .unwrap();
        let binding = LiveTargetBinding {
            nqn: "nqn.2026-01.test:wal".into(),
            uuid: [1; 16],
            nsid: 1,
            bytes: 8192,
            block_size: 4096,
            address: "127.0.0.1".into(),
            service: 4420,
            transport: NvmeofTransport::Tcp,
            readonly: true,
            permanent: false,
        };
        let segment = LiveWalSegment {
            database: [51; 16],
            partition: 2,
            epoch: 1,
            generation: 1,
            file_index: file as u64,
            path,
            bytes: 8192,
            device: 0,
            inode: 0,
            block_size: 4096,
        };
        let io = LiveIo::new(namespace, slots, 16 * (4096 + 4095), false).unwrap();
        let recorded = io.stats();
        cache.grants.push(Grant {
            id: file as u64 + 1,
            scope: GrantScope::File(LiveRecordLocation {
                member: position.address,
                segment,
                binding,
                offset: 0,
                len: 0,
            }),
            token: Some([file as u8; 16]),
            io,
            inline_io: None,
            inline_recorded: LiveIoStats::default(),
            recorded,
            last_used: 0,
            broken: false,
            lost: false,
            #[cfg(test)]
            controller_lost: None,
            lease: None,
        });
    }
    Lane {
        _dir: dir,
        client,
        cache,
        metrics,
        first,
        second,
        a,
        b,
        _running: running,
    }
}

#[test]
fn subsystem_timeout_cancel_and_decode_errors_do_not_quarantine() {
    let mut lane = subsystem_lane();
    let expired = Instant::now() - Duration::from_secs(1);
    assert!(
        lane.cache
            .read_inline_cached(&lane.client, &lane.a, expired, None)
            .is_err()
    );
    assert!(lane.cache.failed_subsystems.is_empty());
    let a = lane.a;
    let token = lane.submit(vec![a]);
    lane.cache.cancel_flights(token);
    assert!(lane.collect(1)[0].result.is_err());
    assert!(lane.cache.failed_subsystems.is_empty());
    assert!(!lane.cache.grants[0].broken);
    // The real ring returns these corrupted fixture bytes. The lane transfers
    // them; the reader's CRC over the frame is what rejects them, so the
    // corruption must not quarantine the subsystem.
    use std::os::unix::fs::FileExt;
    std::fs::OpenOptions::new()
        .write(true)
        .open(lane._dir.path().join("wal-0"))
        .unwrap()
        .write_all_at(&vec![0; a.address.len as usize], a.address.offset % 8192)
        .unwrap();
    let corrupt = lane
        .cache
        .read_inline_cached(
            &lane.client,
            &a,
            Instant::now() + Duration::from_secs(1),
            None,
        )
        .expect("the transfer completes");
    assert_ne!(crc32fast::hash(&corrupt[0][8..]), a.payload_crc);
    assert!(lane.cache.failed_subsystems.is_empty());
    assert!(!lane.cache.grants[0].broken);
}

#[test]
fn subsystem_io_errors_quarantine_inline_and_lane_reads() {
    for inline in [false, true] {
        let mut lane = subsystem_lane();
        lane.cache.grants[0]
            .io
            .override_next_completion_result(-libc::EIO);
        let a = lane.a;
        if inline {
            assert!(
                lane.cache
                    .read_inline_cached(
                        &lane.client,
                        &a,
                        Instant::now() + Duration::from_secs(1),
                        None
                    )
                    .is_err()
            );
        } else {
            let b = lane.b;
            lane.submit(vec![a, b]);
            assert!(lane.collect(1)[0].result.is_err());
        }
        assert!(lane.cache.failed_subsystems.contains(&(1, 0)));
        assert_eq!(lane.metrics.snapshot().counts.metadata_requests, 0);
    }
}

fn subsystem_lane() -> Lane {
    let mut lane = lane(4);
    lane.cache.grants.clear();
    let mut handles = Vec::new();
    for file in 0..2 {
        handles.push(Arc::new(
            LiveNamespace::open_local_file(
                &lane._dir.path().join(format!("wal-{file}")),
                8192,
                4096,
                LiveNamespaceAccess::ReadShared,
            )
            .unwrap(),
        ));
    }
    let mut io = LiveIo::new(handles[0].clone(), 4, 65536, false).unwrap();
    let mut slots = vec![None; 1024];
    for (file, handle) in handles.into_iter().enumerate() {
        slots[file] = Some(io.attach(handle).unwrap());
    }
    lane.cache.grants.push(Grant {
        id: 1,
        scope: GrantScope::Subsystem {
            generation: 1,
            partition: 2,
            subsystem: 0,
            files_per_subsystem: 1024,
            file_bytes: 8192,
            slots,
        },
        token: None,
        io,
        inline_io: None,
        inline_recorded: LiveIoStats::default(),
        recorded: LiveIoStats::default(),
        last_used: 0,
        broken: false,
        lost: false,
        #[cfg(test)]
        controller_lost: None,
        lease: None,
    });
    lane.client.expected.wal_file_bytes = 8192;
    lane
}

/// per_owner: one controller serves both WAL members. A grant opened for
/// member 2 must name member 2, not the member the controller connected with.
#[test]
fn shared_grant_route_names_the_opened_member() {
    use crate::distributed::nvmeof::NvmeofTransport;
    use crate::distributed::nvmeof::live_connect::{LiveConnectTarget, LiveConnectionRegistry};
    use crate::distributed::nvmeof::live_pool::LiveControllerPool;
    let mut lane = subsystem_lane();
    lane.cache.grants.clear();
    lane.client.live_metrics = Some(lane.metrics.clone());
    let pool = Arc::new(
        LiveControllerPool::new(
            LiveConnectionRegistry::open(
                &lane._dir.path().join("registry"),
                crate::distributed::nvmeof::live_connect::RegistryMode::Retire,
            )
            .unwrap(),
            8,
            4,
        )
        .unwrap(),
    );
    lane.client.set_live_pool(pool.clone());
    let config = lane.client.live_config.clone().unwrap();
    let member = |nsid: u32| LiveConnectTarget {
        nqn: "nqn.2026-01.test:wal".into(),
        uuid: [nsid as u8; 16],
        nsid,
        bytes: 8192,
        block_size: 4096,
        address: "127.0.0.1".into(),
        service: 4420,
        transport: NvmeofTransport::Tcp,
        geometry: config.queue_geometry,
    };
    for nsid in [1, 2] {
        pool.cache_local_shared_namespace(
            &member(nsid),
            &config.host_nqn,
            config.host_id,
            &lane._dir.path().join(format!("wal-{}", nsid - 1)),
        )
        .unwrap();
    }
    let Ok(grant) = LiveReadCache::connect_scope(
        &lane.client,
        1,
        member(2),
        GrantScope::Subsystem {
            generation: 1,
            partition: 2,
            subsystem: 0,
            files_per_subsystem: 1024,
            file_bytes: 8192,
            slots: vec![None; 1024],
        },
        None,
        7,
        LiveNamespaceAccess::ReadShared,
        Instant::now() + Duration::from_secs(1),
        true,
    ) else {
        panic!("shared grant setup failed");
    };
    let routes = lane.metrics.snapshot().routes;
    assert_eq!(routes.len(), 1);
    let identity = &routes[0].namespace_identity;
    assert!(routes[0].writer.is_none());
    assert_eq!((identity.nsid, identity.uuid), (2, [2; 16]));
    assert_eq!(identity.namespace_path, lane._dir.path().join("wal-1"));
    drop(grant);
}

#[test]
fn subsystem_grant_reads_two_files_as_two_flights_and_checks_generation() {
    let mut lane = subsystem_lane();
    assert!(lane.cache.inline_ready(&lane.a));
    assert!(lane.cache.inline_ready(&lane.b));
    let mut unattached = lane.a;
    unattached.address.offset += 2 * 8192;
    assert!(!lane.cache.inline_ready(&unattached));
    // A member whose address names another generation is not covered by a
    // grant of this generation, however its logical batch compares.
    let mut other_generation = lane.a;
    other_generation.address.segment_generation = 2;
    assert_eq!(
        member_range(&lane.cache.grants[0], &other_generation)
            .unwrap_err()
            .to_string(),
        "invalid cached live member geometry"
    );
    // The positive half of decision (a): a position whose *batch* epoch is
    // older than its address generation, served by that generation's grant.
    // The second grant covers the same file range in generation 2, so the
    // only difference from the refused case is which grant is asked.
    let second_generation = GrantScope::Subsystem {
        generation: 2,
        partition: 2,
        subsystem: 0,
        files_per_subsystem: 1024,
        file_bytes: 8192,
        slots: vec![None; 1024],
    };
    let mut second = Grant {
        id: lane.cache.grants[0].id + 100,
        scope: second_generation,
        token: None,
        io: LiveIo::new(
            LiveNamespace::open_local_file(
                &lane._dir.path().join("wal-0"),
                8192,
                4096,
                LiveNamespaceAccess::ReadOnly,
            )
            .unwrap(),
            4,
            16 * (4096 + 4095),
            false,
        )
        .unwrap(),
        inline_io: None,
        inline_recorded: LiveIoStats::default(),
        recorded: LiveIoStats::default(),
        last_used: 0,
        broken: false,
        lost: false,
        #[cfg(test)]
        controller_lost: None,
        lease: None,
    };
    second.recorded = second.io.stats();
    lane.cache.grants.push(second);
    let served = &lane.cache.grants[1];
    // `batch.epoch = 1` is older than the address generation 2 and must not
    // matter: the grant is selected by the address.
    assert!(member_range(served, &other_generation).is_ok());
    assert!(served.scope.covers(&other_generation));
    let (a, b) = (lane.a, lane.b);
    let token = lane.submit(vec![a, b, a]);
    assert_eq!(lane.cache.flights.len(), 2);
    let done = lane.collect(1);
    assert_eq!(done[0].token, token);
    assert_eq!(
        done[0].result.as_ref().unwrap(),
        &vec![
            member_frame(&lane.first, &a),
            member_frame(&lane.second, &b),
            member_frame(&lane.first, &a)
        ]
    );
    assert_eq!(lane.metrics.snapshot().counts.completed_read_ops, 2);
    assert_eq!(lane.metrics.snapshot().counts.metadata_requests, 0);
    lane.cache.close(&mut lane.client, 1).unwrap();
    assert!(lane.cache.unreleased.is_empty());
}

#[test]
fn failed_subsystem_is_not_reopened_and_new_epoch_drops_old_failures() {
    let mut lane = subsystem_lane();
    lane.cache.fail_grant(&lane.client, 0, &"test I/O failure");
    lane.cache.retire_broken(&mut lane.client, 1);
    assert!(lane.cache.grants.is_empty());
    assert!(lane.cache.failed_subsystems.contains(&(1, 0)));
    let before = lane.metrics.snapshot().counts;
    let a = lane.a;
    let token = lane.submit(vec![a]);
    let done = lane.collect(1);
    assert_eq!(done[0].token, token);
    assert_eq!(
        done[0].result.as_ref().unwrap_err().to_string(),
        "live subsystem failed; reopen the database"
    );
    let after = lane.metrics.snapshot().counts;
    assert_eq!(after.metadata_requests, before.metadata_requests);
    assert_eq!(after.namespace_connects, before.namespace_connects);
    let mut state = InFlightRequest {
        positions: vec![a],
        deadline: Instant::now() + Duration::from_secs(1),
        retries: 0,
        controller_losses: 0,
        io_deadline: None,
        trace: None,
        slots: vec![Slot::Pending],
        output: vec![None],
        pending: 1,
        first_pending: 0,
        flights: 0,
        error: None,
        submitted_at: None,
        last_cqe: None,
        retry_at: None,
        connect_since: None,
    };
    state.positions[0].address.segment_generation = 2;
    let _ = lane
        .cache
        .open_permanent(&mut lane.client, 2, &mut state, 0, Duration::from_secs(1));
    assert!(
        !lane
            .cache
            .failed_subsystems
            .iter()
            .any(|(generation, _)| *generation == 1)
    );
}

#[test]
fn healthy_subsystem_retirement_does_not_block_later_use() {
    let mut lane = subsystem_lane();
    lane.cache.retire(&mut lane.client, 1, 0).unwrap();
    assert!(lane.cache.failed_subsystems.is_empty());
    assert!(lane.cache.unreleased.is_empty());
    use crate::distributed::nvmeof::{
        NvmeofTransport, live_connect::LiveConnectionRegistry, live_pool::LiveControllerPool,
    };
    let registry = LiveConnectionRegistry::open(
        &lane._dir.path().join("registry"),
        crate::distributed::nvmeof::live_connect::RegistryMode::Retire,
    )
    .unwrap();
    let pool = Arc::new(LiveControllerPool::new(registry, 8, 4).unwrap());
    let config = lane.client.live_config.as_ref().unwrap();
    let name = wal_export_name(1024, lane.client.expected.inventory.database, 2, 1, 0);
    let target = LiveConnectTarget {
        nqn: name.nqn,
        uuid: name.uuid,
        nsid: name.nsid,
        bytes: 8192,
        block_size: 4096,
        address: "127.0.0.1".into(),
        service: 4420,
        transport: NvmeofTransport::Tcp,
        geometry: config.queue_geometry,
    };
    pool.cache_local_shared_namespace(
        &target,
        &config.host_nqn,
        config.host_id,
        &lane._dir.path().join("wal-0"),
    )
    .unwrap();
    let mut second = target.clone();
    second.nsid = 2;
    second.uuid = wal_export_name(1024, lane.client.expected.inventory.database, 2, 1, 1).uuid;
    pool.cache_local_shared_namespace(
        &second,
        &config.host_nqn,
        config.host_id,
        &lane._dir.path().join("wal-1"),
    )
    .unwrap();
    lane.client.live_pool = Some(pool.clone());
    lane.client.live_exports = Some((
        1,
        super::super::LiveExportInfo {
            address: target.address.clone(),
            service: target.service,
            transport: target.transport,
            wal_file_bytes: 8192,
            block_size: 4096,
            exported_files: 2,
        },
        2,
    ));
    for _ in 0..2 {
        let a = lane.a;
        let frame = member_frame(&lane.first, &a);
        lane.submit(vec![a]);
        let done = lane.collect(1);
        assert_eq!(done[0].result.as_ref().unwrap(), &vec![frame]);
        assert!(!lane.cache.inline_ready(&lane.b));
        let b = lane.b;
        let frame = member_frame(&lane.second, &b);
        lane.submit(vec![b]);
        let done = lane.collect(1);
        assert_eq!(done[0].result.as_ref().unwrap(), &vec![frame]);
        assert!(lane.cache.inline_ready(&lane.b));
        lane.cache.retire(&mut lane.client, 1, 0).unwrap();
        assert!(lane.cache.failed_subsystems.is_empty());
    }
    let counts = lane.metrics.snapshot().counts;
    assert_eq!(counts.grant_misses, 4);
    assert_eq!(counts.namespace_connects, 0);
    assert_eq!(counts.metadata_requests, 0);
    lane.client.live_pool = None;
    pool.shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
}

impl Lane {
    fn submit(&mut self, positions: Vec<VersionPosition>) -> LiveReadToken {
        let request = LiveReadRequest {
            positions,
            deadline: Instant::now() + Duration::from_secs(10),
            trace: None,
        };
        match self.cache.submit(&mut self.client, 1, request) {
            Ok(token) => token,
            Err(_) => panic!("lane rejected the request"),
        }
    }
    fn collect(&mut self, count: usize) -> Vec<LiveReadDone> {
        let mut done = Vec::new();
        for _ in 0..1000 {
            self.cache.poll(&mut self.client, 1, &mut done).unwrap();
            if done.len() >= count {
                return done;
            }
            self.cache.wait(Duration::from_secs(1)).unwrap();
        }
        panic!("requests did not finish")
    }
}

#[test]
fn lane_runs_requests_for_different_files_in_flight_together() {
    let mut lane = lane(4);
    let (a, b) = (lane.a, lane.b);
    let first = lane.submit(vec![a]);
    let second = lane.submit(vec![b, a, b]);
    // Both are on the rings before any CQE is harvested.
    assert_eq!(lane.cache.in_flight(), 2);
    assert_eq!(lane.cache.active_requests, 2);
    assert_eq!(lane.cache.grants[0].io.outstanding(), 2);
    assert_eq!(lane.cache.grants[1].io.outstanding(), 1);
    let counts = lane.metrics.snapshot().counts;
    assert_eq!(counts.lane_inflight_request_max, 2);
    assert_eq!(counts.lane_inflight_op_max, 3);

    let done = lane.collect(2);
    let result = |token| {
        let found = done.iter().find(|d| d.token == token).unwrap();
        found.result.as_ref().ok().unwrap().clone()
    };
    let (one, two) = (
        member_frame(&lane.first, &a),
        member_frame(&lane.second, &b),
    );
    assert_eq!(result(first), vec![one.clone()]);
    assert_eq!(result(second), vec![two.clone(), one, two]);
    assert_eq!(lane.cache.in_flight(), 0);
    assert_eq!(lane.cache.active_requests, 0);
    let counts = lane.metrics.snapshot().counts;
    assert_eq!(counts.completed_read_ops, 3);
    assert_eq!((counts.grant_hits, counts.grant_misses), (3, 0));
    assert_eq!(counts.delivery_samples, 2);
    assert!(counts.copy_bytes_avoided > 0);
    assert_eq!(counts.errors, 0);
    // A hit refreshes recency: reading file 0 again makes it newest.
    lane.submit(vec![b]);
    lane.submit(vec![a]);
    lane.collect(2);
    assert!(lane.cache.grants[0].last_used > lane.cache.grants[1].last_used);
}

#[test]
fn lane_admits_a_request_larger_than_the_grant_credit_in_waves() {
    let mut lane = lane(1);
    let a = lane.a;
    let token = lane.submit(vec![a; 65]);
    let state = &lane.cache.requests[&token];
    // One SQE fits 32 duplicate members; 33 remain for later waves.
    assert_eq!((state.flights, state.pending), (1, 33));
    // A second request on the full grant parks without failing.
    let other = lane.submit(vec![a]);
    assert_eq!(lane.cache.requests[&other].pending, 1);
    let done = lane.collect(2);
    let one = member_frame(&lane.first, &a);
    for finished in &done {
        let values = finished.result.as_ref().ok().unwrap();
        assert!(values.iter().all(|value| *value == one));
        assert_eq!(values.len(), if finished.token == token { 65 } else { 1 });
    }
    assert_eq!(lane.cache.grants[0].io.outstanding(), 0);
    assert!(lane.cache.grants[0].io.stats().read_requests >= 3);
}

#[test]
fn lane_merged_members_fit_one_block_budget() {
    let mut lane = lane(1);
    let grant = &mut lane.cache.grants[0];
    let namespace = LiveNamespace::open_local_file(
        &match &grant.scope {
            GrantScope::File(location) => location.segment.path.clone(),
            _ => unreachable!(),
        },
        8192,
        4096,
        LiveNamespaceAccess::ReadOnly,
    )
    .unwrap();
    grant.io = LiveIo::new(namespace, 1, 4096 + 4095, false).unwrap();
    grant.recorded = grant.io.stats();
    let token = lane.submit(vec![lane.a; 8]);
    let request = &lane.cache.requests[&token];
    assert_eq!((request.pending, request.flights), (0, 1));
    assert_eq!(lane.cache.grants[0].io.outstanding(), 1);
    let mut done = lane.collect(1);
    let one = member_frame(&lane.first, &lane.a);
    assert_eq!(done.pop().unwrap().result.unwrap(), vec![one; 8]);
    assert_eq!(lane.cache.grants[0].io.stats().completed_read_ops, 1);
    assert_eq!(lane.cache.grants[0].io.credit(), (1, 4096 + 4095));
}

#[test]
fn lane_rejects_invalid_cached_member_geometry_before_io() {
    let mut lane = lane(4);
    let mut outside = lane.a;
    // A member that would run past the end of its WAL file is not read: the
    // lane rejects the geometry before submitting any I/O.
    outside.address.offset = 8192 - u64::from(outside.address.len) + 1;
    let invalid = lane.submit(vec![outside]);
    assert!(
        lane.cache
            .grants
            .iter()
            .all(|grant| grant.io.outstanding() == 0)
    );
    let valid = lane.submit(vec![lane.b]);
    let done = lane.collect(2);
    assert!(
        done.iter()
            .find(|result| result.token == invalid)
            .unwrap()
            .result
            .is_err()
    );
    assert!(
        done.iter()
            .find(|result| result.token == valid)
            .unwrap()
            .result
            .is_ok()
    );
    assert_eq!(lane.metrics.snapshot().counts.completed_read_ops, 1);
}

#[test]
fn lane_keeps_serving_after_a_corrupt_member_and_the_reader_rejects_it() {
    let mut lane = lane(4);
    let (a, b) = (lane.a, lane.b);
    // The lane transfers bytes; integrity is the reader's CRC over the
    // fetched frame. Corrupt one byte of `a`'s entry in the owner file.
    let offset = a.address.offset % 8192 + u64::from(a.address.len) - 1;
    std::fs::OpenOptions::new()
        .write(true)
        .open(lane._dir.path().join("wal-0"))
        .unwrap()
        .write_all_at(&[0xff], offset)
        .unwrap();
    let bad = lane.submit(vec![a]);
    let good = lane.submit(vec![b]);
    let done = lane.collect(2);
    let corrupt = done.iter().find(|d| d.token == bad).unwrap();
    let frame = corrupt.result.as_ref().expect("the transfer completes");
    assert_ne!(crc32fast::hash(&frame[0][8..]), a.payload_crc);
    assert!(
        done.iter()
            .find(|d| d.token == good)
            .unwrap()
            .result
            .is_ok()
    );
    // The grant survived the corrupted member and serves the next read.
    let again = lane.submit(vec![b]);
    let done = lane.collect(1);
    assert_eq!(done[0].token, again);
    assert!(done[0].result.is_ok());
    assert_eq!(lane.cache.grants.len(), 2);
}

#[test]
fn lane_rejects_when_full() {
    let mut lane = lane(4);
    let a = lane.a;
    for _ in 0..4 {
        lane.submit(vec![a]);
    }
    let request = LiveReadRequest {
        positions: vec![a],
        deadline: Instant::now() + Duration::from_secs(10),
        trace: None,
    };
    match lane.cache.submit(&mut lane.client, 1, request) {
        Err(LiveReadSubmitError::Full(request)) => assert_eq!(request.positions.len(), 1),
        _ => panic!("a full lane must hand the request back"),
    }
    lane.collect(4);
    assert_eq!(lane.cache.grants[0].io.outstanding(), 0);
}

/// Native parity: a member parked for grant credit owns no flight, so it
/// has no I/O deadline. Its flight timer starts at submission; the
/// caller's absolute deadline is the only bound on how long it may park.
#[test]
fn parked_member_has_no_flight_deadline_until_submission() {
    // One queue slot: the first request's flight holds the grant credit.
    let mut lane = lane(1);
    let a = lane.a;
    let first = lane.submit(vec![a]);
    let wait = Duration::from_millis(50);
    let request = LiveReadRequest {
        positions: vec![a],
        deadline: Instant::now() + Duration::from_secs(10),
        trace: None,
    };
    let Ok(parked) = lane.cache.submit(&mut lane.client, 1, request) else {
        panic!("lane rejected the request")
    };
    assert_eq!(lane.cache.requests[&parked].pending, 1);
    assert!(lane.cache.io_deadline(first).is_some());
    assert_eq!(lane.cache.io_deadline(parked), None);
    std::thread::sleep(3 * wait);
    let done = lane.collect(2);
    let one = member_frame(&lane.first, &a);
    for finished in &done {
        assert_eq!(finished.result.as_ref().unwrap(), &vec![one.clone()]);
    }
    assert_eq!(lane.cache.grants[0].io.outstanding(), 0);
}

#[test]
fn drain_cancels_in_flight_requests_and_waits_for_every_cqe() {
    let mut lane = lane(4);
    let (a, b) = (lane.a, lane.b);
    lane.submit(vec![a, a]);
    lane.submit(vec![b]);
    let mut done = Vec::new();
    lane.cache.drain(&mut lane.client, 1, &mut done).unwrap();
    assert_eq!(done.len(), 2);
    assert!(done.iter().all(|d| d.result.is_err()));
    // No operation, and so no DMA buffer, is left when drain returns.
    assert!(lane.cache.grants.iter().all(|g| g.io.outstanding() == 0));
    assert_eq!(lane.cache.in_flight(), 0);
    // Closing with requests in flight is refused.
    lane.submit(vec![a]);
    assert!(lane.cache.close(&mut lane.client, 1).is_err());
    lane.collect(1);
}

#[test]
fn owner_release_control_retires_real_file_pin_and_rejects_wrong_epoch() {
    use super::super::{Request, Response, StorageNode, TcpLimits};
    let dir = tempdir::TempDir::new("live-pin-release").unwrap();
    let config = super::super::tests::config(&dir.path().join("node"));
    let node = StorageNode::open(config.clone(), super::super::tests::metrics()).unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    let batch = super::super::tests::batch(&config, 1, 0);
    let log = node.inventory.lock().active(1).unwrap();
    let appended = log
        .append_complete_batch(
            crate::distributed::types::RequestAuthority { epoch: 1 },
            &batch,
        )
        .unwrap();
    let reference = batch.record_reference(appended.address(), 0).unwrap();
    // The removed owner-RPC append returned a durable receipt.
    log.persist_through(appended).unwrap();
    let lease = log.resolve_live_record(&reference).unwrap();
    let token = [42; 16];
    node.inventory
        .lock()
        .live_pins
        .insert(token, (1, vec![lease]));
    assert!(
        node.handle(
            Request::ReleaseLiveEpochPin { epoch: 2, token },
            TcpLimits::default()
        )
        .is_err()
    );
    assert!(node.inventory.lock().live_pins.contains_key(&token));
    assert!(matches!(
        node.handle(
            Request::ReleaseLiveEpochPin { epoch: 1, token },
            TcpLimits::default()
        ),
        Ok(Response::NvmeofEpochReleased)
    ));
    assert!(!node.inventory.lock().live_pins.contains_key(&token));
}

#[test]
fn failed_release_retains_token_for_later_cleanup() {
    use crate::distributed::storage::WalMemberReadPolicy;
    let running = super::super::tests::multiplexed::Running::new(
        super::super::Transport::Tcp,
        WalMemberReadPolicy::Async,
    );
    let mut client = running.client();
    client.fence_to(1).unwrap();
    let batch = super::super::tests::batch(&running.config, 1, 0);
    let appended = running
        .node
        .inventory
        .lock()
        .active(1)
        .unwrap()
        .append_complete_batch(
            crate::distributed::types::RequestAuthority { epoch: 1 },
            &batch,
        )
        .unwrap();
    let reference = batch.record_reference(appended.address(), 0).unwrap();
    let log = running.node.inventory.lock().active(1).unwrap();
    // The removed owner-RPC append returned a durable receipt.
    log.persist_through(appended).unwrap();
    // The native WAL mapper extends the file to its final size on its own
    // thread, and a live lease requires the final size. Wait for that
    // condition instead of assuming the mapper already ran.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let lease = loop {
        match log.resolve_live_record(&reference) {
            Ok(lease) => break lease,
            Err(crate::distributed::storage::StorageError::Invalid(message))
                if message == "invalid live namespace geometry"
                    && std::time::Instant::now() < deadline =>
            {
                std::thread::yield_now();
            }
            Err(error) => panic!("live record did not resolve: {error:?}"),
        }
    };
    let token = [43; 16];
    running
        .node
        .inventory
        .lock()
        .live_pins
        .insert(token, (1, vec![lease]));
    let mut cache = LiveReadCache::default();
    cache.unreleased.push(token);
    assert!(cache.close(&mut client, 2).is_err());
    assert_eq!(cache.unreleased, vec![token]);
    assert!(running.node.inventory.lock().live_pins.contains_key(&token));
    cache.close(&mut client, 1).unwrap();
    assert!(cache.unreleased.is_empty());
    assert!(!running.node.inventory.lock().live_pins.contains_key(&token));
}

#[test]
fn io_counter_deltas_do_not_recount_prior_completions() {
    use crate::distributed::nvmeof::live_io::LiveIoStats;
    use crate::distributed::nvmeof::live_metrics::LiveNvmeMetrics;
    let metrics = LiveNvmeMetrics::new();
    let producer = metrics.register().unwrap();
    let before = LiveIoStats {
        completed_read_ops: 2,
        completed_read_bytes: 4096,
        submissions: 2,
        completions: 2,
        queue_ns: 10,
        submit_ns: 20,
        submit_to_cq_ns: 30,
        ..Default::default()
    };
    let after = LiveIoStats {
        completed_read_ops: 3,
        completed_read_bytes: 8192,
        submissions: 3,
        completions: 3,
        queue_ns: 17,
        submit_ns: 28,
        submit_to_cq_ns: 39,
        ..Default::default()
    };
    record_read_io_stats(&producer, &before, &after, false);
    let counts = metrics.snapshot().counts;
    assert_eq!(
        (counts.completed_read_ops, counts.completed_read_bytes),
        (1, 4096)
    );
    assert_eq!((counts.queue_ns, counts.queue_samples), (7, 1));
    assert_eq!((counts.submit_ns, counts.submit_samples), (8, 1));
    assert_eq!((counts.cq_ns, counts.cq_samples), (9, 1));
    assert_eq!(counts.recovery_read_ops, 0);
}

/// Two `File` grants of one subsystem (files 0 and 1 of 1024) opened through
/// one pooled controller lease, as a lane holds them after reading both files.
fn pooled_file_grants() -> (
    Lane,
    Arc<crate::distributed::nvmeof::live_pool::LiveControllerPool>,
) {
    use crate::distributed::nvmeof::NvmeofTransport;
    use crate::distributed::nvmeof::live_connect::LiveConnectionRegistry;
    use crate::distributed::nvmeof::live_pool::LiveControllerPool;
    use crate::distributed::nvmeof::live_storage::LiveTargetBinding;
    let mut lane = lane(4);
    lane.cache.grants.clear();
    let pool = Arc::new(
        LiveControllerPool::new(
            LiveConnectionRegistry::open(
                &lane._dir.path().join("registry"),
                crate::distributed::nvmeof::live_connect::RegistryMode::Retire,
            )
            .unwrap(),
            8,
            4,
        )
        .unwrap(),
    );
    lane.client.set_live_pool(pool.clone());
    // The file acquisition creates the lease, so the pool entry carries the
    // retirement routing (`identity.file`) the owner's eviction matches on.
    lane.client.live_write_local_fixture = true;
    let config = lane.client.live_config.clone().unwrap();
    let partition = lane.client.expected.partition;
    let deadline = Instant::now() + Duration::from_secs(1);
    for (file, nsid) in [(0u64, 1u32), (1, 2)] {
        let binding = LiveTargetBinding {
            nqn: "nqn.2026-01.test:wal".into(),
            uuid: [u8::try_from(nsid).unwrap(); 16],
            nsid,
            bytes: 8192,
            block_size: 4096,
            address: "127.0.0.1".into(),
            service: 4420,
            transport: NvmeofTransport::Tcp,
            readonly: true,
            permanent: false,
        };
        let location = LiveRecordLocation {
            member: if file == 0 {
                lane.a.address
            } else {
                lane.b.address
            },
            offset: 0,
            len: 0,
            segment: LiveWalSegment {
                database: [51; 16],
                partition,
                epoch: 1,
                generation: 1,
                file_index: file,
                path: lane._dir.path().join(format!("wal-{file}")),
                bytes: 8192,
                device: 0,
                inode: 0,
                block_size: 4096,
            },
            binding: binding.clone(),
        };
        let target = LiveConnectTarget {
            nqn: binding.nqn,
            uuid: binding.uuid,
            nsid: binding.nsid,
            bytes: binding.bytes,
            block_size: binding.block_size,
            address: binding.address,
            service: binding.service,
            transport: binding.transport,
            geometry: config.queue_geometry,
        };
        // No owner pin token: the pin exchange is orthogonal to whether the
        // cached grants release the shared subsystem lease.
        let Ok(grant) = LiveReadCache::connect_scope(
            &lane.client,
            1,
            target,
            GrantScope::File(location),
            None,
            file + 1,
            LiveNamespaceAccess::ReadOnly,
            deadline,
            false,
        ) else {
            panic!("file grant {file} setup failed");
        };
        lane.cache.grants.push(grant);
    }
    (lane, pool)
}

/// An idle cached grant of sibling file B keeps the shared subsystem lease
/// live, so retiring file A must release B's grant too. Otherwise the pooled
/// controller of one subsystem can never be evicted while B stays cached.
#[test]
fn idle_sibling_grant_of_the_victim_subsystem_does_not_defer_retirement() {
    use crate::distributed::tcp::checkpoint::RetirementResult;
    let (mut lane, pool) = pooled_file_grants();
    let partition = lane.client.expected.partition;
    let deadline = Instant::now() + Duration::from_secs(5);
    assert_eq!(
        pool.evict_files(partition, 1, 8192, &[0], deadline)
            .unwrap(),
        RetirementResult::Busy,
        "both idle grants still pin the shared subsystem lease"
    );
    lane.cache
        .evict_retirement_files(&mut lane.client, 1, 1, &[0])
        .unwrap();
    let ids: Vec<u64> = lane.cache.grants.iter().map(|grant| grant.id).collect();
    assert!(
        ids.is_empty(),
        "an idle cached grant of sibling file 1 must not defer retiring file 0: {ids:?}"
    );
    assert_eq!(
        pool.evict_files(partition, 1, 8192, &[0], deadline)
            .unwrap(),
        RetirementResult::Completed,
        "retirement completes once the idle subsystem grants are released"
    );
}

/// An in-flight read of the victim file still pins its namespace, so the
/// pooled controller stays busy until that read drains.
#[test]
fn in_flight_victim_read_still_defers_retirement() {
    use crate::distributed::tcp::checkpoint::RetirementResult;
    let (mut lane, pool) = pooled_file_grants();
    let partition = lane.client.expected.partition;
    let deadline = Instant::now() + Duration::from_secs(5);
    lane.cache.grants[0]
        .io
        .submit_reads(99, &[(0, 1)], Instant::now() + Duration::from_secs(10))
        .unwrap();
    assert_eq!(
        lane.cache.grants[0].outstanding(),
        1,
        "the victim read is in flight"
    );
    lane.cache
        .evict_retirement_files(&mut lane.client, 1, 1, &[0])
        .unwrap();
    let ids: Vec<u64> = lane.cache.grants.iter().map(|grant| grant.id).collect();
    assert_eq!(
        ids,
        vec![1],
        "only the in-flight victim grant keeps the subsystem: {ids:?}"
    );
    assert_eq!(
        pool.evict_files(partition, 1, 8192, &[0], deadline)
            .unwrap(),
        RetirementResult::Busy,
        "an in-flight read of the victim still defers the retirement"
    );
    // The read drains: retirement then completes on the next pass.
    lane.cache
        .fail_grant(&lane.client, 0, &TcpError::new("test drain"));
    lane.cache.retire_broken(&mut lane.client, 1);
    assert_eq!(
        pool.evict_files(partition, 1, 8192, &[0], deadline)
            .unwrap(),
        RetirementResult::Completed,
        "retirement completes once the victim read is drained"
    );
}
