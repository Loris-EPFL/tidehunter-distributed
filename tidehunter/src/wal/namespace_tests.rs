use super::layout::WalKind;
use super::position::WalFileId;
use super::tests::detailed_count;
use super::*;

fn layout() -> WalLayout {
    WalLayout {
        frag_size: 4096,
        max_maps: 4,
        direct_io: false,
        wal_file_size: 65536,
        kind: WalKind::Replay,
    }
}

fn write(writer: &WalWriter) -> u64 {
    drop(
        writer
            .write(&PreparedWalWrite::new(&vec![42; 128]))
            .unwrap(),
    );
    writer.wal_tracker_barrier();
    writer.position()
}

#[test]
fn directory_coalescing_skips_only_unchanged_namespaces() {
    for coalesce in [false, true] {
        let dir = tempdir::TempDir::new("wal-namespace-coalesce").unwrap();
        let registry = prometheus::Registry::new();
        let wal = Wal::open_with_directory_sync(
            dir.path(),
            layout(),
            Metrics::new_in_options(&registry, true, true),
            coalesce,
        )
        .unwrap();
        let writer = wal.writer_after(None).unwrap();
        for _ in 0..8 {
            write(&writer);
            writer.sync().unwrap();
        }
        assert_eq!(detailed_count(&registry, "wal_file_sync", "success"), 8);
        assert_eq!(
            detailed_count(&registry, "wal_directory_sync", "success"),
            if coalesce { 1 } else { 8 }
        );
        assert_eq!(
            detailed_count(&registry, "wal_directory_sync", "skipped"),
            if coalesce { 7 } else { 0 }
        );
        // An already durable prefix still skips the entire barrier.
        writer.sync().unwrap();
        assert_eq!(detailed_count(&registry, "wal_barrier", "skipped"), 1);
    }
}

#[test]
fn directory_coalescing_tracks_lookahead_before_new_file_writes() {
    let dir = tempdir::TempDir::new("wal-namespace-lookahead").unwrap();
    let registry = prometheus::Registry::new();
    let mut layout = layout();
    layout.wal_file_size = layout.frag_size * 4;
    let wal = Wal::open_with_directory_sync(
        dir.path(),
        layout,
        Metrics::new_in_options(&registry, true, true),
        true,
    )
    .unwrap();
    let writer = wal.writer_after(None).unwrap();
    write(&writer);
    writer.sync().unwrap();
    let before = wal.synced.lock().namespace_generation;
    // Enter fragment 2. The mapper precreates file 1 for fragment 4, even
    // though no writer has yet allocated a byte in that file.
    for _ in 0..3 {
        drop(
            writer
                .write(&PreparedWalWrite::new(&vec![17; 4000]))
                .unwrap(),
        );
    }
    writer.wal_tracker_barrier();
    writer.gc(0).unwrap();
    assert!(writer.position() < wal.wal_file_size());
    assert!(wal.file_ids().contains(&WalFileId(1)));
    assert!(*wal.files.load().namespace_generation.lock() > before);
    writer.sync().unwrap();
    assert_eq!(
        detailed_count(&registry, "wal_directory_sync", "success"),
        2
    );
    assert_eq!(
        wal.synced.lock().namespace_generation,
        *wal.files.load().namespace_generation.lock()
    );
}

#[test]
fn directory_coalescing_failure_does_not_certify_namespace_or_prefix() {
    let dir = tempdir::TempDir::new("wal-namespace-failure").unwrap();
    let wal = Wal::open_with_directory_sync(dir.path(), layout(), Metrics::new(), true).unwrap();
    let writer = wal.writer_after(None).unwrap();
    let end = write(&writer);
    assert!(
        wal.sync_through_with_directory(end, |_| Err(io::Error::other("injected sync failure")))
            .is_err()
    );
    assert_eq!(wal.synced.lock().position, 0);
    assert_eq!(wal.synced.lock().namespace_generation, 0);
    writer.sync().unwrap();
    assert_eq!(wal.synced.lock().position, end);
    assert_eq!(wal.synced.lock().namespace_generation, 1);
}

#[test]
fn directory_coalescing_does_not_certify_creation_after_capture() {
    let dir = tempdir::TempDir::new("wal-namespace-race").unwrap();
    let wal = Wal::open_with_directory_sync(dir.path(), layout(), Metrics::new(), true).unwrap();
    let writer = wal.writer_after(None).unwrap();
    let end = write(&writer);
    writer.gc(0).unwrap();
    let captured = *wal.files.load().namespace_generation.lock();
    wal.sync_through_with_directory(end, |path| {
        File::open(path)?.sync_all()?;
        // Deterministic adversarial interleaving: publish a filename after
        // the directory sync, before the barrier certifies its generation.
        let files = wal.files.load();
        let id = WalFileId(files.current_file_id().0 + 1);
        let file = Wal::open_file(&wal.layout.wal_file_name(path, id), &wal.layout)?;
        wal.files
            .store(Arc::new(files.with_file(id, Arc::new(file))));
        Ok(())
    })
    .unwrap();
    assert_eq!(wal.synced.lock().namespace_generation, captured);
    assert!(*wal.files.load().namespace_generation.lock() > captured);
    let next = write(&writer);
    let mut synchronized = false;
    wal.sync_through_with_directory(next, |path| {
        synchronized = true;
        File::open(path)?.sync_all()
    })
    .unwrap();
    assert!(synchronized);
    assert_eq!(
        wal.synced.lock().namespace_generation,
        *wal.files.load().namespace_generation.lock()
    );
}

#[test]
fn directory_coalescing_tracks_completed_background_unlinks() {
    let dir = tempdir::TempDir::new("wal-namespace-unlink").unwrap();
    let registry = prometheus::Registry::new();
    let mut layout = layout();
    layout.wal_file_size = layout.frag_size * 2;
    let wal = Wal::open_with_directory_sync(
        dir.path(),
        layout,
        Metrics::new_in_options(&registry, true, true),
        true,
    )
    .unwrap();
    let writer = wal.writer_after(None).unwrap();
    for _ in 0..12 {
        drop(
            writer
                .write(&PreparedWalWrite::new(&vec![17; 4000]))
                .unwrap(),
        );
    }
    writer.wal_tracker_barrier();
    writer.gc(0).unwrap();
    writer.sync().unwrap();
    let before = wal.synced.lock().namespace_generation;
    writer.delete_files(vec![WalFileId(0)]).unwrap();
    // Registry removal precedes physical deletion. Wait for its actual
    // completion, with a fixed upper bound, before testing the next barrier.
    let namespace = wal.files.load().namespace_generation.clone();
    for _ in 0..1000 {
        if *namespace.lock() > before {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert!(*namespace.lock() > before);
    assert!(!wal.layout.wal_file_name(dir.path(), WalFileId(0)).exists());
    write(&writer);
    writer.gc(0).unwrap();
    writer.sync().unwrap();
    assert_eq!(
        detailed_count(&registry, "wal_directory_sync", "success"),
        2
    );
    assert_eq!(wal.synced.lock().namespace_generation, *namespace.lock());
}

#[test]
fn directory_coalescing_reopen_starts_uncertified() {
    let dir = tempdir::TempDir::new("wal-namespace-reopen").unwrap();
    let position = {
        let wal =
            Wal::open_with_directory_sync(dir.path(), layout(), Metrics::new(), true).unwrap();
        let writer = wal.writer_after(None).unwrap();
        let position = writer
            .write(&PreparedWalWrite::new(&vec![42; 128]))
            .unwrap()
            .into_wal_position();
        writer.sync().unwrap();
        position
    };
    let registry = prometheus::Registry::new();
    let wal = Wal::open_with_directory_sync(
        dir.path(),
        layout(),
        Metrics::new_in_options(&registry, true, true),
        true,
    )
    .unwrap();
    assert_eq!(wal.synced.lock().namespace_generation, 0);
    assert_eq!(wal.read(position).unwrap().1.unwrap().as_ref(), &[42; 128]);
    let writer = wal.writer_after(Some(position)).unwrap();
    write(&writer);
    writer.sync().unwrap();
    assert_eq!(
        detailed_count(&registry, "wal_directory_sync", "success"),
        1
    );
}

#[test]
fn directory_coalescing_concurrent_rollovers_recover_all_acknowledged_writes() {
    let dir = tempdir::TempDir::new("wal-namespace-concurrent").unwrap();
    let mut layout = layout();
    layout.wal_file_size = layout.frag_size * 2;
    let positions = {
        let wal = Wal::open_with_directory_sync(dir.path(), layout.clone(), Metrics::new(), true)
            .unwrap();
        let writer = wal.writer_after(None).unwrap();
        let positions = thread::scope(|scope| {
            let writer = &writer;
            let handles: Vec<_> = (0_u8..4)
                .map(|worker| {
                    scope.spawn(move || {
                        let mut positions = Vec::new();
                        for operation in 0_u8..24 {
                            let value = vec![worker * 24 + operation; 4000];
                            let position = writer
                                .write(&PreparedWalWrite::new(&value))
                                .unwrap()
                                .into_wal_position();
                            let durable = writer.sync().unwrap();
                            assert!(durable >= position.offset() + position.frame_len() as u64);
                            positions.push((position, value));
                        }
                        positions
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        writer.wal_tracker_barrier();
        writer.gc(0).unwrap();
        write(&writer);
        writer.sync().unwrap();
        assert_eq!(
            wal.synced.lock().namespace_generation,
            *wal.files.load().namespace_generation.lock()
        );
        positions
    };
    let wal = Wal::open_with_directory_sync(dir.path(), layout, Metrics::new(), true).unwrap();
    for (position, value) in positions {
        assert_eq!(wal.read(position).unwrap().1.unwrap().as_ref(), value);
    }
}

#[test]
fn directory_coalescing_configuration_defaults_preserve_old_behavior() {
    use crate::config::Config;

    assert!(!Config::default().coalesce_wal_directory_sync);
    let mut config = Config::small();
    assert!(!config.coalesce_wal_directory_sync);
    let serialized = serde_yaml::to_string(&config).unwrap();
    assert!(!serialized.contains("coalesce_wal_directory_sync"));
    let legacy: Config = serde_yaml::from_str(&serialized).unwrap();
    assert!(!legacy.coalesce_wal_directory_sync);
    config.coalesce_wal_directory_sync = true;
    let encoded = serde_yaml::to_string(&config).unwrap();
    let decoded: Config = serde_yaml::from_str(&encoded).unwrap();
    assert!(decoded.coalesce_wal_directory_sync);
}
