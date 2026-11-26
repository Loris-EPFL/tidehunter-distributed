use super::*;
use crate::distributed::test_bounds::PROGRESS_CAP;

/// Deadline of a call that must end by timing out. It is the stimulus, not a
/// measurement: the call polls until the deadline and the outcome does not depend on
/// its value. The only way to fail such a test is a stall of the test thread longer
/// than this before the first pass of the call, so the value is large against a
/// scheduling stall and small against the run time. `PROGRESS_CAP` stays the only
/// hang guard.
const EXPECTED_TIMEOUT: Duration = Duration::from_millis(250);

#[test]
fn cpu_lists_parse_single_ranges_and_mixed_sets() {
    assert_eq!(parse_cpu_list("7\n").unwrap(), vec![7]);
    assert_eq!(parse_cpu_list("0-3").unwrap(), vec![0, 1, 2, 3]);
    assert_eq!(
        parse_cpu_list("0-3,8,10-11\n").unwrap(),
        vec![0, 1, 2, 3, 8, 10, 11]
    );
    for invalid in ["", "\n", "3-1", "a", "0,", "1-2-3"] {
        assert!(parse_cpu_list(invalid).is_err(), "{invalid:?}");
    }
    assert!(parse_cpu_list(&format!("0-{}", usize::MAX)).is_err());
    assert!(parse_cpu_list(&sys::affinity_cpu_limit().to_string()).is_err());
}

#[test]
fn queue_cpus_reads_sysfs_in_numeric_queue_order_and_names_errors() {
    let temp = tempdir::TempDir::new("queue-cpus").unwrap();
    let root = temp.path();
    let mq = root.join("nvme0n1/mq");
    for (queue, cpus) in [("10", "8"), ("2", "0-3,8"), ("0", "4-5")] {
        let path = mq.join(queue);
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("cpu_list"), cpus).unwrap();
    }
    assert_eq!(
        queue_cpus_at(root, Path::new("/dev/nvme0n1")).unwrap(),
        vec![vec![4, 5], vec![0, 1, 2, 3, 8], vec![8]]
    );
    let path = mq.join("2/cpu_list");
    std::fs::write(&path, "").unwrap();
    let error = queue_cpus_at(root, Path::new("/dev/nvme0n1")).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains(path.to_str().unwrap()));
    std::fs::remove_file(&path).unwrap();
    let error = queue_cpus_at(root, Path::new("/dev/nvme0n1")).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    assert!(error.to_string().contains(path.to_str().unwrap()));
}

#[test]
fn queue_cpus_follows_native_multipath_head_in_sorted_path_order() {
    let temp = tempdir::TempDir::new("queue-cpus-multipath").unwrap();
    let root = temp.path();
    let paths = root.join("nvme0n1/multipath");
    std::fs::create_dir_all(&paths).unwrap();
    for (name, cpus) in [("nvme0c2n1", "8-9"), ("nvme0c1n1", "2-3")] {
        let disk = root.join(name);
        std::fs::create_dir_all(disk.join("mq/0")).unwrap();
        std::fs::write(disk.join("mq/0/cpu_list"), cpus).unwrap();
        std::os::unix::fs::symlink(&disk, paths.join(name)).unwrap();
    }
    assert_eq!(
        queue_cpus_at(root, Path::new("/dev/nvme0n1")).unwrap(),
        vec![vec![2, 3]]
    );
}

#[test]
fn live_queue_geometry_defaults_to_four_128_entry_queues() {
    assert_eq!(
        LiveQueueGeometry::default(),
        LiveQueueGeometry {
            io_queues: 4,
            queue_size: 128,
            poll_queues: 0
        }
    );
}

#[test]
fn live_queue_geometry_validates_all_bounds() {
    assert!(LiveQueueGeometry::default().validate().is_ok());
    for io_queues in [1, 16] {
        for queue_size in [16, 128] {
            for poll_queues in [0, 4] {
                assert!(
                    LiveQueueGeometry {
                        io_queues,
                        queue_size,
                        poll_queues
                    }
                    .validate()
                    .is_ok()
                );
            }
        }
    }
    for geometry in [
        LiveQueueGeometry {
            io_queues: 0,
            ..Default::default()
        },
        LiveQueueGeometry {
            io_queues: 17,
            ..Default::default()
        },
        LiveQueueGeometry {
            queue_size: 15,
            ..Default::default()
        },
        LiveQueueGeometry {
            queue_size: 129,
            ..Default::default()
        },
        LiveQueueGeometry {
            poll_queues: 5,
            ..Default::default()
        },
    ] {
        assert!(geometry.validate().is_err());
    }
    assert!(
        serde_json::from_value::<LiveQueueGeometry>(serde_json::json!({
            "io_queues": 1, "queue_size": 32, "poll_queues": 0, "unknown": 0
        }))
        .is_err()
    );
}

#[test]
fn old_connect_target_defaults_geometry_and_pins_connect_options() {
    let mut target: LiveConnectTarget = serde_json::from_value(serde_json::json!({
        "nqn": "nqn.2026-01.test:wal", "uuid": vec![1; 16], "nsid": 1,
        "bytes": 4096, "block_size": 4096, "address": "127.0.0.1",
        "service": 4420, "transport": "tcp"
    }))
    .unwrap();
    assert_eq!(target.geometry, LiveQueueGeometry::default());
    let expected = "nqn=nqn.2026-01.test:wal,transport=tcp,traddr=127.0.0.1,trsvcid=4420,hostnqn=nqn.2026-01.test:host,hostid=02020202-0202-0202-0202-020202020202,ctrl_loss_tmo=0,keep_alive_tmo=5,nr_io_queues=4,queue_size=128";
    assert_eq!(
        connect_options(
            &target,
            "nqn.2026-01.test:host",
            [2; 16],
            KEEP_ALIVE_TMO_SECS
        ),
        expected
    );
    target.geometry = LiveQueueGeometry {
        io_queues: 1,
        queue_size: 32,
        poll_queues: 1,
    };
    assert_eq!(
        connect_options(
            &target,
            "nqn.2026-01.test:host",
            [2; 16],
            KEEP_ALIVE_TMO_SECS
        ),
        expected.replace(
            "nr_io_queues=4,queue_size=128",
            "nr_io_queues=1,queue_size=32,nr_poll_queues=1"
        )
    );
    let other = connect_options(
        &target,
        "nqn.2026-01.test:other-owner",
        [3; 16],
        KEEP_ALIVE_TMO_SECS,
    );
    assert!(other.contains(
        "hostnqn=nqn.2026-01.test:other-owner,hostid=03030303-0303-0303-0303-030303030303"
    ));
    assert!(!other.contains("hostnqn=nqn.2026-01.test:host,"));
}

#[test]
fn vanished_accepts_only_missing_or_torn_down_devices() {
    assert!(vanished(&io::Error::from(io::ErrorKind::NotFound)));
    assert!(vanished(&io::Error::from_raw_os_error(libc::ENOENT)));
    assert!(vanished(&io::Error::from_raw_os_error(libc::ENODEV)));
    assert!(!vanished(&io::Error::from(io::ErrorKind::PermissionDenied)));
    assert!(!vanished(&io::Error::from_raw_os_error(libc::EIO)));
    assert!(!vanished(&io::Error::from_raw_os_error(libc::ENXIO)));
}

#[test]
fn namespace_attribute_disappearance_skips_but_other_errors_fail() {
    let temp = tempdir::TempDir::new("namespace-attribute").unwrap();
    let path = temp.path().join("nsid");
    assert_eq!(scan_attribute(&path).unwrap(), None);
    std::fs::write(&path, "1\n").unwrap();
    assert_eq!(scan_attribute(&path).unwrap().as_deref(), Some("1"));
    assert!(scan_attribute(temp.path()).is_err());
}

#[test]
fn queue_geometry_admission_requires_exact_readable_kernel_attributes() {
    let temp = tempdir::TempDir::new("controller-queues").unwrap();
    let path = temp.path();
    std::fs::write(path.join("queue_count"), "5\n").unwrap();
    std::fs::write(path.join("sqsize"), "127\n").unwrap();
    assert_eq!(
        verify_queue_geometry(path, LiveQueueGeometry::default()).unwrap(),
        (5, 127)
    );
    for (geometry, count, size) in [
        (
            LiveQueueGeometry {
                io_queues: 1,
                queue_size: 32,
                poll_queues: 0,
            },
            "2",
            "31",
        ),
        (
            LiveQueueGeometry {
                poll_queues: 1,
                ..Default::default()
            },
            "6",
            "127",
        ),
    ] {
        std::fs::write(path.join("queue_count"), count).unwrap();
        std::fs::write(path.join("sqsize"), size).unwrap();
        verify_queue_geometry(path, geometry).unwrap();
    }

    std::fs::write(path.join("queue_count"), "17\n").unwrap();
    assert_eq!(
        verify_queue_geometry(path, LiveQueueGeometry::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    std::fs::write(path.join("queue_count"), "5\n").unwrap();
    std::fs::write(path.join("sqsize"), "31\n").unwrap();
    assert_eq!(
        verify_queue_geometry(path, LiveQueueGeometry::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    std::fs::write(path.join("sqsize"), "not-a-number\n").unwrap();
    assert_eq!(
        verify_queue_geometry(path, LiveQueueGeometry::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    std::fs::remove_file(path.join("sqsize")).unwrap();
    assert_eq!(
        verify_queue_geometry(path, LiveQueueGeometry::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
}

#[test]
fn registry_excludes_duplicate_intents_and_holds_lock() {
    let temp = tempdir::TempDir::new("live-connect").unwrap();
    let path = temp.path().join("registry");
    let registry = LiveConnectionRegistry::open(&path, RegistryMode::Retire).unwrap();
    assert!(LiveConnectionRegistry::open(&path, RegistryMode::Retire).is_err());
    let intent = Intent {
        target: LiveConnectTarget {
            nqn: "nqn.2026-01.test:wal".into(),
            uuid: [1; 16],
            nsid: 1,
            bytes: 4096,
            block_size: 4096,
            address: "127.0.0.1".into(),
            service: 4420,
            transport: NvmeofTransport::Tcp,
            geometry: Default::default(),
        },
        host_nqn: "nqn.2026-01.test:host".into(),
        host_id: [2; 16],
    };
    std::fs::write(path.join("interrupted.tmp"), b"{\"target\":").unwrap();
    assert!(
        !registry
            .has_intent(&intent.target.nqn, &intent.host_nqn)
            .unwrap()
    );
    let journal = registry.reserve(&intent).unwrap();
    assert!(
        registry
            .has_intent(&intent.target.nqn, &intent.host_nqn)
            .unwrap()
    );
    assert!(registry.reserve(&intent).is_err());
    let restored: Intent = serde_json::from_slice(&std::fs::read(&journal).unwrap()).unwrap();
    assert_eq!(restored.host_nqn, intent.host_nqn);
    assert_eq!(restored.target.uuid, intent.target.uuid);
    registry.remove_intent(&journal).unwrap();
    assert!(
        !registry
            .has_intent(&intent.target.nqn, &intent.host_nqn)
            .unwrap()
    );
    drop(registry);
    let registry = LiveConnectionRegistry::open(&path, RegistryMode::Retire).unwrap();
    assert!(registry.reserve(&intent).is_ok());
}

#[test]
fn udev_settle_treats_absence_as_idle_and_honors_the_deadline() {
    let temp = tempdir::TempDir::new("udev-queue").unwrap();
    let queue = temp.path().join("queue");
    wait_for_udev_idle_at(&queue, Instant::now() + PROGRESS_CAP)
        .expect("a host without a udev queue is already idle");
    std::fs::write(&queue, b"").unwrap();
    let error = wait_for_udev_idle_at(&queue, Instant::now() + Duration::from_millis(20))
        .expect_err("a busy udev queue must reach its deadline");
    assert_eq!(
        error.kind(),
        io::ErrorKind::TimedOut,
        "a busy queue reports the deadline, not success"
    );
}

#[test]
fn retirement_does_not_touch_missing_or_replaced_original() {
    let temp = tempdir::TempDir::new("controller-retire").unwrap();
    let path = temp.path().join("controller");
    let original = File::create(&path).unwrap();
    let metadata = original.metadata().unwrap();
    std::fs::remove_file(&path).unwrap();
    let mut state = DeletionState::NotStarted;
    retire_controller(
        &mut state,
        &path,
        metadata.dev(),
        metadata.ino(),
        Instant::now() + PROGRESS_CAP,
        || panic!("removed controller must not be verified"),
        || panic!("removed controller must not be deleted"),
    )
    .unwrap();
    assert_eq!(state, DeletionState::Gone);
    let replacement = File::create(&path).unwrap();
    assert_ne!(replacement.metadata().unwrap().ino(), metadata.ino());
    state = DeletionState::NotStarted;
    retire_controller(
        &mut state,
        &path,
        metadata.dev(),
        metadata.ino(),
        Instant::now() + PROGRESS_CAP,
        || panic!("replacement must not be verified"),
        || panic!("replacement must not be deleted"),
    )
    .unwrap();
    assert_eq!(state, DeletionState::Gone);
    assert!(path.exists());
}

#[test]
fn retirement_missing_attribute_remains_uncertain_until_original_disappears() {
    for errno in [libc::ENOENT, libc::ENODEV] {
        let temp = tempdir::TempDir::new("controller-uncertain").unwrap();
        let path = temp.path().join("controller");
        let original = File::create(&path).unwrap();
        let metadata = original.metadata().unwrap();
        let mut state = DeletionState::NotStarted;
        let result = retire_controller(
            &mut state,
            &path,
            metadata.dev(),
            metadata.ino(),
            Instant::now() + EXPECTED_TIMEOUT,
            || Err(io::Error::from_raw_os_error(errno)),
            || panic!("missing verification attribute must not issue delete"),
        );
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_eq!(state, DeletionState::Uncertain);
        std::fs::remove_file(&path).unwrap();
        retire_controller(
            &mut state,
            &path,
            metadata.dev(),
            metadata.ino(),
            Instant::now() + PROGRESS_CAP,
            || panic!("uncertain retry must not verify again"),
            || panic!("uncertain retry must not issue delete"),
        )
        .unwrap();
        assert_eq!(state, DeletionState::Gone);
    }
}

#[test]
fn retirement_missing_delete_attribute_observes_without_reissue() {
    for errno in [libc::ENOENT, libc::ENODEV] {
        let temp = tempdir::TempDir::new("controller-delete-missing").unwrap();
        let path = temp.path().join("controller");
        let original = File::create(&path).unwrap();
        let metadata = original.metadata().unwrap();
        let mut state = DeletionState::NotStarted;
        let result = retire_controller(
            &mut state,
            &path,
            metadata.dev(),
            metadata.ino(),
            Instant::now() + EXPECTED_TIMEOUT,
            || Ok(()),
            || Err(io::Error::from_raw_os_error(errno)),
        );
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_eq!(state, DeletionState::Uncertain);
        std::fs::remove_file(&path).unwrap();
        retire_controller(
            &mut state,
            &path,
            metadata.dev(),
            metadata.ino(),
            Instant::now() + PROGRESS_CAP,
            || panic!("uncertain retry must not verify again"),
            || panic!("uncertain retry must not issue delete"),
        )
        .unwrap();
        assert_eq!(state, DeletionState::Gone);
    }
}

#[test]
fn retirement_store_io_error_retains_uncertainty_without_retry() {
    let temp = tempdir::TempDir::new("controller-store-error").unwrap();
    let path = temp.path().join("controller");
    let original = File::create(&path).unwrap();
    let metadata = original.metadata().unwrap();
    let mut state = DeletionState::NotStarted;
    let error = retire_controller(
        &mut state,
        &path,
        metadata.dev(),
        metadata.ino(),
        Instant::now() + PROGRESS_CAP,
        || Ok(()),
        || Err(io::Error::from_raw_os_error(libc::EIO)),
    )
    .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::EIO));
    assert_eq!(state, DeletionState::Uncertain);
    let error = retire_controller(
        &mut state,
        &path,
        metadata.dev(),
        metadata.ino(),
        Instant::now() + EXPECTED_TIMEOUT,
        || panic!("uncertain retry must not verify again"),
        || panic!("uncertain retry must not issue delete"),
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(state, DeletionState::Uncertain);
}

#[test]
fn missing_device_retries_but_other_failures_do_not() {
    let temp = tempdir::TempDir::new("device-readiness").unwrap();
    let path = temp.path().join("device");
    let created_path = path.clone();
    // The creator makes the file appear only after the first open failed, so the retry
    // path is certain to run; the wait follows that progress.
    let (missing_tx, missing_rx) = std::sync::mpsc::channel();
    let creator = std::thread::spawn(move || {
        missing_rx.recv().unwrap();
        File::create(created_path).unwrap();
    });
    let file = wait_for_device(
        "open test device",
        &path,
        "nvme5",
        DeviceNode::Namespace,
        Instant::now() + PROGRESS_CAP,
        || Ok(()),
        || {
            let opened = File::open(&path);
            if opened.is_err() {
                // A send after the creator ended is not an error.
                missing_tx.send(()).ok();
            }
            opened
        },
    )
    .unwrap();
    drop(file);
    creator.join().unwrap();
    let mut calls = 0;
    wait_for_device(
        "open test device",
        &path,
        "nvme5",
        DeviceNode::Namespace,
        Instant::now() + PROGRESS_CAP,
        || Ok(()),
        || {
            calls += 1;
            if calls == 1 {
                Err(io::Error::from_raw_os_error(libc::ENODEV))
            } else {
                Ok(())
            }
        },
    )
    .unwrap();
    assert_eq!(calls, 2);
    let mut calls = 0;
    let error = wait_for_device::<()>(
        "open test device",
        &path,
        "nvme5",
        DeviceNode::Namespace,
        Instant::now() + PROGRESS_CAP,
        || Ok(()),
        || {
            calls += 1;
            Err(io::Error::from_raw_os_error(libc::EIO))
        },
    )
    .unwrap_err();
    assert_eq!(calls, 1);
    assert_eq!(error.kind(), io::Error::from_raw_os_error(libc::EIO).kind());
    assert!(error.to_string().contains("open test device"));
    assert!(error.to_string().contains(path.to_str().unwrap()));
}

#[test]
fn namespace_enxio_retries_reverify_the_controller_before_each_open() {
    let temp = tempdir::TempDir::new("namespace-enxio").unwrap();
    let path = temp.path().join("nvme5n1");
    File::create(&path).unwrap();
    let mut verifies = 0;
    let mut opens = 0;
    let file = wait_for_device(
        "open namespace device",
        &path,
        "nvme5",
        DeviceNode::Namespace,
        Instant::now() + PROGRESS_CAP,
        || {
            verifies += 1;
            Ok(())
        },
        || {
            opens += 1;
            if opens <= 2 {
                Err(io::Error::from_raw_os_error(libc::ENXIO))
            } else {
                File::open(&path)
            }
        },
    )
    .unwrap();
    assert_eq!(
        file.metadata().unwrap().ino(),
        path.metadata().unwrap().ino()
    );
    assert_eq!(opens, 3);
    assert_eq!(verifies, 3);
}

#[test]
fn namespace_enxio_deadline_names_the_device_and_owned_controller() {
    let start = Instant::now();
    let deadline = start + EXPECTED_TIMEOUT;
    let mut opens = 0;
    let mut verifies = 0;
    let error = wait_for_device::<()>(
        "open namespace device",
        Path::new("/dev/nvme5n1"),
        "nvme5",
        DeviceNode::Namespace,
        deadline,
        || {
            verifies += 1;
            Ok(())
        },
        || {
            opens += 1;
            Err(io::Error::from_raw_os_error(libc::ENXIO))
        },
    )
    .unwrap_err();
    assert!(Instant::now() >= deadline);
    assert!(start.elapsed() < PROGRESS_CAP);
    assert!(opens > 0);
    assert_eq!(verifies, opens);
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    let message = error.to_string();
    assert!(message.contains("/dev/nvme5n1"));
    assert!(message.contains("controller nvme5"));
    assert!(message.contains("ENXIO persisted"));
}

#[test]
fn namespace_retry_rejects_reused_controller_identity_before_foreign_open() {
    let temp = tempdir::TempDir::new("namespace-reused-controller").unwrap();
    let controller = temp.path().join("nvme5");
    std::fs::create_dir(&controller).unwrap();
    for (name, value) in [
        ("subsysnqn", "nqn.2026-01.test:owned"),
        ("hostnqn", "nqn.2026-01.test:host"),
        ("hostid", "02020202-0202-0202-0202-020202020202"),
    ] {
        std::fs::write(controller.join(name), value).unwrap();
    }
    let directory = File::open(&controller).unwrap();
    let original = directory.metadata().unwrap();
    let mut opens = 0;
    let mut verifies = 0;
    let error = wait_for_device::<()>(
        "open namespace device",
        Path::new("/dev/nvme5n1"),
        "nvme5",
        DeviceNode::Namespace,
        Instant::now() + PROGRESS_CAP,
        || {
            verifies += 1;
            let current = controller.metadata()?;
            if current.dev() != original.dev()
                || current.ino() != original.ino()
                || controller_attribute(&controller, &directory, "subsysnqn")?.as_deref()
                    != Some("nqn.2026-01.test:owned")
                || controller_attribute(&controller, &directory, "hostnqn")?.as_deref()
                    != Some("nqn.2026-01.test:host")
                || controller_attribute(&controller, &directory, "hostid")?.as_deref()
                    != Some("02020202-0202-0202-0202-020202020202")
            {
                return Err(invalid("controller identity changed"));
            }
            Ok(())
        },
        || {
            opens += 1;
            assert_eq!(opens, 1, "must never open a foreign controller's device");
            std::fs::rename(&controller, temp.path().join("old-controller"))?;
            std::fs::create_dir(&controller)?;
            Err(io::Error::from_raw_os_error(libc::ENXIO))
        },
    )
    .unwrap_err();
    assert_eq!(opens, 1);
    assert_eq!(verifies, 2);
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("controller identity changed"));
}

#[test]
fn namespace_open_io_and_permission_errors_and_admin_enxio_do_not_retry() {
    for (node, errno) in [
        (DeviceNode::Namespace, libc::EIO),
        (DeviceNode::Namespace, libc::EACCES),
        (DeviceNode::Controller, libc::ENXIO),
    ] {
        let mut opens = 0;
        let error = wait_for_device::<()>(
            "open test device",
            Path::new("/dev/nvme5n1"),
            "nvme5",
            node,
            Instant::now() + PROGRESS_CAP,
            || Ok(()),
            || {
                opens += 1;
                Err(io::Error::from_raw_os_error(errno))
            },
        )
        .unwrap_err();
        assert_eq!(opens, 1);
        assert_eq!(error.kind(), io::Error::from_raw_os_error(errno).kind());
    }
}

#[test]
fn namespace_identity_enxio_is_not_a_retryable_device_open_error() {
    let mut verifies = 0;
    let error = wait_for_device::<()>(
        "open namespace device",
        Path::new("/dev/nvme5n1"),
        "nvme5",
        DeviceNode::Namespace,
        Instant::now() + PROGRESS_CAP,
        || {
            verifies += 1;
            Err(io::Error::from_raw_os_error(libc::ENXIO))
        },
        || panic!("identity check failed: device must not open"),
    )
    .unwrap_err();
    assert_eq!(verifies, 1);
    assert_eq!(
        error.kind(),
        io::Error::from_raw_os_error(libc::ENXIO).kind()
    );
}

#[test]
fn failed_connect_cleanup_preserves_the_primary_readiness_diagnostic() {
    let primary = "open namespace device /dev/nvme5n1 (controller nvme5): ENXIO persisted";
    let error = connect_error_after_cleanup(
        io::Error::new(io::ErrorKind::TimedOut, primary),
        Err(io::Error::from_raw_os_error(libc::EACCES)),
    );
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    let message = error.to_string();
    assert!(message.starts_with(primary));
    assert!(message.contains("; controller cleanup failed: "));
    assert!(message.contains(&io::Error::from_raw_os_error(libc::EACCES).to_string()));
    // Successful cleanup preserves even the original OS error representation.
    let error = connect_error_after_cleanup(io::Error::from_raw_os_error(libc::EIO), Ok(()));
    assert_eq!(error.raw_os_error(), Some(libc::EIO));
}

#[test]
fn disappeared_controller_is_distinct_from_missing_live_attribute() {
    let temp = tempdir::TempDir::new("controller-attribute").unwrap();
    let path = temp.path().join("controller");
    std::fs::create_dir(&path).unwrap();
    let directory = File::open(&path).unwrap();
    assert!(controller_attribute(&path, &directory, "hostid").is_err());
    std::fs::remove_dir(&path).unwrap();
    assert!(
        controller_attribute(&path, &directory, "hostid")
            .unwrap()
            .is_none()
    );
}

#[test]
fn deletion_confirmation_waits_and_never_touches_replacement() {
    let temp = tempdir::TempDir::new("controller-confirmation").unwrap();
    let path = temp.path().join("controller");
    let original = File::create(&path).unwrap();
    let metadata = original.metadata().unwrap();
    assert!(!original_removed(&path, metadata.dev(), metadata.ino()).unwrap());
    let error = wait_for_original_removal(&path, metadata.dev(), metadata.ino(), Instant::now())
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    std::fs::remove_file(&path).unwrap();
    assert!(original_removed(&path, metadata.dev(), metadata.ino()).unwrap());
    let replacement = File::create(&path).unwrap();
    assert_ne!(replacement.metadata().unwrap().ino(), metadata.ino());
    assert!(original_removed(&path, metadata.dev(), metadata.ino()).unwrap());
    wait_for_original_removal(
        &path,
        metadata.dev(),
        metadata.ino(),
        Instant::now() + PROGRESS_CAP,
    )
    .unwrap();
    assert!(path.exists());
}

#[test]
fn deletion_confirmation_ends_on_a_concurrent_removal() {
    let temp = tempdir::TempDir::new("controller-delayed").unwrap();
    let path = temp.path().join("controller");
    let original = File::create(&path).unwrap();
    let metadata = original.metadata().unwrap();
    let removed_path = path.clone();
    let remover = std::thread::spawn(move || {
        std::fs::remove_file(removed_path).unwrap();
    });
    // The remover runs on its own thread; the wait ends on its removal whether the first
    // poll sees the file or not. The deadline is a hang guard.
    wait_for_original_removal(
        &path,
        metadata.dev(),
        metadata.ino(),
        Instant::now() + PROGRESS_CAP,
    )
    .unwrap();
    remover.join().unwrap();
}

#[test]
fn namespace_visibility_rejects_hidden_and_invalid_paths() {
    assert!(namespace_name("nvme3n1"));
    assert!(namespace_name("nvme3c2n1"));
    assert!(!namespace_name("nvme3n1p1"));
    assert!(!namespace_name("nvme3"));
    let temp = tempdir::TempDir::new("namespace-hidden").unwrap();
    let hidden = temp.path().join("hidden");
    std::fs::write(&hidden, "1\n").unwrap();
    assert!(!namespace_visible(temp.path()).unwrap());
    std::fs::write(&hidden, "0\n").unwrap();
    assert!(namespace_visible(temp.path()).unwrap());
    std::fs::write(&hidden, "unexpected\n").unwrap();
    assert!(namespace_visible(temp.path()).is_err());
}

#[test]
fn fabrics_options_reject_delimiters() {
    assert!(valid_nqn("nqn.2026-01.test:host"));
    assert!(!valid_nqn("nqn.2026-01.test:host,duplicate_connect"));
    assert!(!valid_nqn("nqn.2026-01.test:host=other"));
}

fn would_block(file: &File) -> bool {
    matches!(file.try_lock(), Err(std::fs::TryLockError::WouldBlock))
        && matches!(
            file.try_lock_shared(),
            Err(std::fs::TryLockError::WouldBlock)
        )
}

/// True when no open file description holds the disk lock. `WouldBlock` is the
/// only evidence of a holder: any other `flock` failure (a signal, or no lock
/// record under memory pressure) is not proof of one, so a caller that must not
/// read a single instant waits with [`wait_until`].
fn unlocked(path: &Path) -> bool {
    File::open(path).unwrap().try_lock().is_ok()
}

/// Waits for `condition`, which another thread reaches by releasing a lock it
/// took. The wait follows that release and is bounded by the shared hang guard.
fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + PROGRESS_CAP;
    while !condition() {
        assert!(Instant::now() < deadline, "{what}");
        std::thread::yield_now();
    }
}

#[test]
fn the_unlocked_probe_sees_a_disk_held_by_another_description() {
    let temp = tempdir::TempDir::new("unlocked-probe").expect("temp dir");
    let disk = temp.path().join("nvme9n1");
    File::create(&disk).expect("create the disk");
    let holder = File::open(&disk).expect("open the holder");
    holder.lock().expect("hold the disk lock");
    assert!(!unlocked(&disk), "a held disk reports as unlocked");
    holder.unlock().expect("release the disk lock");
    assert!(unlocked(&disk), "a released disk reports as locked");
}

/// Fake sysfs and /dev for controller `nvme9`: a plain disk `nvme9n1`, a
/// multipath head `nvme9n2` linking to it, a hidden path disk, a disk of
/// `nvme8`, a node `nvme9n3` now naming another device, and a visible disk
/// `nvme9n4` whose node vanished. Returns `(block, dev, ctrl)`.
fn fake_controller_disks(root: &Path) -> (PathBuf, PathBuf, PathBuf) {
    let (block, dev) = (root.join("block"), root.join("dev"));
    for dir in ["ctrl/nvme9", "ctrl/nvme8", "subsys", "block", "dev"] {
        std::fs::create_dir_all(root.join(dir)).unwrap();
    }
    let ctrl = std::fs::canonicalize(root.join("ctrl/nvme9")).unwrap();
    std::os::unix::fs::symlink(&ctrl, root.join("subsys/nvme9")).unwrap();
    for (name, parent, hidden, number, node) in [
        ("nvme9n1", "ctrl/nvme9", "0", "0:0", true),
        ("nvme9n2", "subsys", "0", "0:0", true),
        ("nvme9c9n1", "ctrl/nvme9", "1", "0:0", true),
        ("nvme8n1", "ctrl/nvme8", "0", "0:0", true),
        ("nvme9n3", "ctrl/nvme9", "0", "7:3", true),
        ("nvme9n4", "ctrl/nvme9", "0", "0:0", false),
    ] {
        let disk = block.join(name);
        std::fs::create_dir(&disk).unwrap();
        std::os::unix::fs::symlink(root.join(parent), disk.join("device")).unwrap();
        std::fs::write(disk.join("hidden"), hidden).unwrap();
        // Regular files stand in for nodes: their device number is 0:0.
        std::fs::write(disk.join("dev"), number).unwrap();
        if node {
            File::create(dev.join(name)).unwrap();
        }
    }
    (block, dev, ctrl)
}

#[test]
fn reconcile_teardown_locks_every_visible_disk_without_resolve_history() {
    let temp = tempdir::TempDir::new("teardown-disks").unwrap();
    let (block, dev, ctrl) = fake_controller_disks(temp.path());
    let mut names: Vec<String> = owned_visible_disks(&block, &ctrl, "nvme9")
        .unwrap()
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    names.sort();
    assert_eq!(names, ["nvme9n1", "nvme9n2", "nvme9n3", "nvme9n4"]);
    let deadline = Instant::now() + PROGRESS_CAP;
    let locks = lock_controller_disks(&block, &dev, &ctrl, "nvme9", deadline).unwrap();
    assert_eq!(locks.len(), 2);
    // udevd's close-after-write reread takes LOCK_EX|LOCK_NB and its
    // workers LOCK_SH|LOCK_NB: both must skip the controller's disks.
    assert!(would_block(&File::open(dev.join("nvme9n1")).unwrap()));
    assert!(would_block(&File::open(dev.join("nvme9n2")).unwrap()));
    for other in ["nvme9c9n1", "nvme8n1", "nvme9n3"] {
        assert!(unlocked(&dev.join(other)), "{other} must not be locked");
    }
    drop(locks);
    assert!(unlocked(&dev.join("nvme9n1")));
}

#[test]
fn disk_lock_is_taken_only_at_teardown_and_released_after_deletion() {
    let temp = tempdir::TempDir::new("teardown-order").unwrap();
    let (block, dev, ctrl) = fake_controller_disks(temp.path());
    let disk = dev.join("nvme9n1");
    let deadline = Instant::now() + PROGRESS_CAP;
    // Normal use: namespace I/O opens and closes the disk read-write, and
    // udev may process it; nothing holds the lock.
    drop(
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(&disk)
            .unwrap(),
    );
    let udev = File::open(&disk).unwrap();
    udev.try_lock().unwrap();
    udev.unlock().unwrap();
    let mut locks = Vec::new();
    let error = retire_locked(
        &mut locks,
        true,
        || lock_controller_disks(&block, &dev, &ctrl, "nvme9", deadline),
        || {
            assert!(would_block(&udev), "delete issued without the lock");
            Err(io::Error::other("deletion uncertain"))
        },
    )
    .unwrap_err();
    assert_eq!(error.to_string(), "deletion uncertain");
    assert!(would_block(&udev), "unconfirmed deletion released the lock");
    // A later attempt after the store keeps the same locks until confirmation.
    retire_locked(
        &mut locks,
        false,
        || panic!("teardown relocked after the delete was issued"),
        || {
            assert!(would_block(&udev));
            Ok(())
        },
    )
    .unwrap();
    assert!(locks.is_empty());
    udev.try_lock().unwrap();
}

#[test]
fn teardown_lock_timeout_issues_no_delete() {
    let temp = tempdir::TempDir::new("teardown-timeout").unwrap();
    let (block, dev, ctrl) = fake_controller_disks(temp.path());
    let probe = File::open(dev.join("nvme9n1")).unwrap();
    probe.lock_shared().unwrap();
    let mut locks = Vec::new();
    let mut deleted = false;
    // The deadline is already expired: the probe cannot be displaced, so the
    // attempt times out at its first check and the test never waits on a
    // clock. What the test observes is the deletion decision and the lock
    // state, not how long the attempt waited.
    let deadline = Instant::now() - Duration::from_millis(1);
    let error = retire_locked(
        &mut locks,
        true,
        || lock_controller_disks(&block, &dev, &ctrl, "nvme9", deadline),
        || {
            deleted = true;
            Ok(())
        },
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(!deleted, "delete issued without the lock");
    assert!(locks.is_empty());
    // Locks already taken in the failed attempt are released. The attempt also
    // left a lock helper waiting on the probe's disk; dropping the probe lets
    // it take and release that lock, so the wait follows that release for
    // every disk of the controller instead of reading one instant.
    drop(probe);
    wait_until("the failed attempt released every disk it locked", || {
        unlocked(&dev.join("nvme9n1")) && unlocked(&dev.join("nvme9n2"))
    });
}

#[test]
fn disk_lock_waits_for_udev_probe_and_never_outlives_its_deadline() {
    let temp = tempdir::TempDir::new("disk-lock-wait").unwrap();
    let disk = temp.path().join("nvme9n1");
    File::create(&disk).unwrap();
    let probe = File::open(&disk).unwrap();
    probe.lock_shared().unwrap();
    let error = lock_disk(
        File::open(&disk).unwrap(),
        &disk,
        "nvme9",
        Instant::now() + EXPECTED_TIMEOUT,
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(
        error
            .to_string()
            .contains(&format!("{} (controller nvme9)", disk.display()))
    );
    let waiter = {
        let disk = disk.clone();
        std::thread::spawn(move || {
            lock_disk(
                File::open(&disk).unwrap(),
                &disk,
                "nvme9",
                Instant::now() + PROGRESS_CAP,
            )
        })
    };
    // Ending the probe lets the waiter in; the timed-out helper also wakes,
    // and must drop its late lock instead of keeping it.
    drop(probe);
    let held = waiter.join().unwrap().unwrap();
    assert!(would_block(&File::open(&disk).unwrap()));
    drop(held);
    // The late helper may still take the lock briefly; it must let it go.
    lock_disk(
        File::open(&disk).unwrap(),
        &disk,
        "nvme9",
        Instant::now() + PROGRESS_CAP,
    )
    .unwrap();
}

#[test]
fn controller_evidence_is_the_geometry_the_check_observed_and_is_recorded_once() {
    let temp = tempdir::TempDir::new("controller-evidence").unwrap();
    let registry =
        LiveConnectionRegistry::open(&temp.path().join("registry"), RegistryMode::Retire).unwrap();
    let target = LiveConnectTarget {
        nqn: "nqn.2026-01.test:wal".into(),
        uuid: [1; 16],
        nsid: 1,
        bytes: 4096,
        block_size: 4096,
        address: "127.0.0.1".into(),
        service: 4420,
        transport: NvmeofTransport::Tcp,
        geometry: Default::default(),
    };
    let controller = LiveController::deleted_for_test(&registry, temp.path(), &target).unwrap();
    let resolver = controller.resolver();
    assert!(resolver.evidence().is_none());
    // A geometry that differs from the configured one records nothing.
    std::fs::write(temp.path().join("queue_count"), "7\n").unwrap();
    std::fs::write(temp.path().join("sqsize"), "127\n").unwrap();
    assert!(controller.record_queue_geometry(temp.path()).is_err());
    assert!(resolver.evidence().is_none());
    std::fs::write(temp.path().join("queue_count"), "5\n").unwrap();
    controller.record_queue_geometry(temp.path()).unwrap();
    let directory = std::fs::metadata(temp.path()).unwrap();
    let evidence = resolver.evidence().unwrap();
    assert_eq!(
        (
            evidence.queue_count,
            evidence.sqsize,
            evidence.keep_alive_tmo,
            evidence.sysfs_device,
            evidence.sysfs_inode
        ),
        (
            5,
            127,
            KEEP_ALIVE_TMO_SECS,
            directory.dev(),
            directory.ino()
        )
    );
    // A second record would replace what the client verified: it must fail.
    assert!(controller.record_queue_geometry(temp.path()).is_err());
    assert_eq!(resolver.evidence(), Some(evidence));
    let json = serde_json::to_value(evidence).unwrap();
    let mut keys: Vec<_> = json.as_object().unwrap().keys().cloned().collect();
    keys.sort();
    assert_eq!(
        keys,
        [
            "keep_alive_tmo",
            "queue_count",
            "sqsize",
            "sysfs_device",
            "sysfs_inode"
        ]
    );
}

#[test]
fn namespace_identity_without_a_controller_record_deserializes_as_none() {
    let uuid = vec![1u8; 16];
    let json = serde_json::json!({
        "namespace_path": "/dev/nvme0n1", "controller_instance": 0,
        "nqn": "nqn.2026-01.test:wal", "uuid": uuid, "nsid": 1,
        "bytes": 4096, "block_size": 4096
    });
    let identity: crate::distributed::nvmeof::live_metrics::LiveNamespaceIdentity =
        serde_json::from_value(json).unwrap();
    assert_eq!(identity.controller, None);
}

/// Identify Namespace Identification Descriptor List: a 4-byte descriptor
/// header (`nidt`, `nidl`, two reserved bytes) followed by `nidl` value bytes.
fn descriptors(entries: &[(u8, &[u8])]) -> Vec<u8> {
    let mut buffer = Vec::new();
    for (kind, value) in entries {
        buffer.push(*kind);
        buffer.push(u8::try_from(value.len()).unwrap());
        buffer.extend_from_slice(&[0, 0]);
        buffer.extend_from_slice(value);
    }
    buffer
}

#[test]
fn namespace_uuid_reads_the_uuid_descriptor_after_its_siblings() {
    let uuid = [
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
        0x00,
    ];
    let eui = [1u8, 2, 3, 4, 5, 6, 7, 8];
    let nguid = [9u8; 16];
    let list = descriptors(&[(0x01, &eui), (0x02, &nguid), (0x03, &uuid)]);
    assert_eq!(
        namespace_uuid(&list),
        Some(uuid),
        "the UUID descriptor is read past the EUI64 and NGUID descriptors"
    );
}

#[test]
fn namespace_uuid_rejects_a_namespace_identify_buffer() {
    // Identify Namespace (CNS 0) starts with NSZE and NCAP, and its write
    // protect bit at byte 99 does not name the namespace.
    let mut identify = [0u8; 4096];
    identify[..16].copy_from_slice(&[0x77; 16]);
    identify[99] = 1;
    assert_eq!(
        namespace_uuid(&identify),
        None,
        "a CNS 0 identify buffer must not be read as a descriptor list"
    );
}

#[test]
fn namespace_uuid_rejects_a_list_without_a_uuid_descriptor() {
    let eui = [1u8; 8];
    assert_eq!(
        namespace_uuid(&descriptors(&[(0x01, &eui)])),
        None,
        "an EUI64-only list names no namespace UUID"
    );
    assert_eq!(
        namespace_uuid(&[0u8; 4096]),
        None,
        "zero padding ends the descriptor list"
    );
}

const ADOPT_HOST: &str = "nqn.2026-01.test:host";
const ADOPT_HOST_ID: [u8; 16] = [2; 16];

fn adoption_target(nqn: &str, nsid: u32) -> LiveConnectTarget {
    LiveConnectTarget {
        nqn: nqn.into(),
        uuid: [1; 16],
        nsid,
        bytes: 4096,
        block_size: 4096,
        address: "127.0.0.1".into(),
        service: 4420,
        transport: NvmeofTransport::Tcp,
        geometry: LiveQueueGeometry {
            io_queues: 1,
            queue_size: 32,
            poll_queues: 0,
        },
    }
}

fn adoption_registry(temp: &Path) -> (LiveConnectionRegistry, PathBuf) {
    let controllers = temp.join("controllers");
    std::fs::create_dir_all(&controllers).unwrap();
    let registry = LiveConnectionRegistry::open_at(
        &temp.join("registry"),
        &controllers,
        &temp.join("no-udev-queue"),
        RegistryMode::Adopt,
        KEEP_ALIVE_TMO_SECS,
    )
    .unwrap();
    (registry, controllers)
}

#[allow(clippy::too_many_arguments)]
fn fixture_controller(
    root: &Path,
    instance: u32,
    target: &LiveConnectTarget,
    host_nqn: &str,
    host_id: [u8; 16],
    state: &str,
    geometry: LiveQueueGeometry,
) -> PathBuf {
    let path = root.join(format!("nvme{instance}"));
    std::fs::create_dir(&path).unwrap();
    let host_id = uuid_string(host_id);
    for (name, value) in [
        ("subsysnqn", target.nqn.as_str()),
        ("hostnqn", host_nqn),
        ("hostid", host_id.as_str()),
        ("transport", "tcp"),
        (
            "address",
            "traddr=127.0.0.1,trsvcid=4420,src_addr=127.0.0.1",
        ),
        ("state", state),
    ] {
        std::fs::write(path.join(name), value).unwrap();
    }
    std::fs::write(
        path.join("queue_count"),
        (geometry.io_queues + geometry.poll_queues + 1).to_string(),
    )
    .unwrap();
    std::fs::write(path.join("sqsize"), (geometry.queue_size - 1).to_string()).unwrap();
    path
}

fn adoption_intent(target: &LiveConnectTarget) -> Intent {
    Intent {
        target: target.clone(),
        host_nqn: ADOPT_HOST.into(),
        host_id: ADOPT_HOST_ID,
    }
}

/// Remove a fixture controller once its delete store appears, as the kernel
/// would remove the sysfs object. The loop follows observed progress; the
/// deadline is only a hang guard.
fn remove_controller_when_deleted(path: PathBuf) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let deadline = Instant::now() + PROGRESS_CAP;
        while Instant::now() < deadline {
            if path.join("delete_controller").exists() {
                let _ = std::fs::remove_dir_all(&path);
                return;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    })
}

#[test]
fn adopt_reuses_the_live_controller_and_keeps_its_intent() {
    let temp = tempdir::TempDir::new("adopt-live").unwrap();
    let (registry, root) = adoption_registry(temp.path());
    let target = adoption_target("nqn.2026-01.test:wal", 1);
    let controller_dir = fixture_controller(
        &root,
        7,
        &target,
        ADOPT_HOST,
        ADOPT_HOST_ID,
        "live",
        target.geometry,
    );
    let intent_path = registry.reserve(&adoption_intent(&target)).unwrap();
    registry.reconcile().unwrap();
    assert!(
        controller_dir.exists(),
        "a live matching controller is kept across reconcile"
    );
    assert!(
        intent_path.exists(),
        "a kept controller keeps its durable intent"
    );
    let deadline = Instant::now() + PROGRESS_CAP;
    let mut controller = registry
        .try_adopt_controller(&target, ADOPT_HOST, ADOPT_HOST_ID, deadline)
        .unwrap()
        .expect("a live matching controller is adoptable");
    assert_eq!(
        controller.instance(),
        7,
        "adoption must reuse the existing controller, never create one"
    );
    let evidence = controller
        .resolver()
        .evidence()
        .expect("adoption records the geometry it verified");
    assert_eq!(
        evidence.queue_count,
        target.geometry.io_queues + 1,
        "the adopted controller's observed queue count is recorded"
    );
    assert!(
        intent_path.exists(),
        "the adopted controller keeps its durable intent until disconnect"
    );
    let remover = remove_controller_when_deleted(controller_dir.clone());
    controller.disconnect(deadline).unwrap();
    remover.join().unwrap();
    assert!(
        !controller_dir.exists(),
        "disconnect retires the controller"
    );
    assert!(
        !intent_path.exists(),
        "disconnect removes the adopted intent"
    );
}

#[test]
fn geometry_mismatch_at_reconcile_retires_the_controller_and_removes_the_intent() {
    let temp = tempdir::TempDir::new("adopt-geometry-reconcile").unwrap();
    let (registry, root) = adoption_registry(temp.path());
    let target = adoption_target("nqn.2026-01.test:wal", 1);
    let controller_dir = fixture_controller(
        &root,
        3,
        &target,
        ADOPT_HOST,
        ADOPT_HOST_ID,
        "live",
        LiveQueueGeometry {
            io_queues: 2,
            queue_size: 64,
            poll_queues: 0,
        },
    );
    let intent_path = registry.reserve(&adoption_intent(&target)).unwrap();
    let remover = remove_controller_when_deleted(controller_dir.clone());
    registry.reconcile().unwrap();
    remover.join().unwrap();
    assert!(
        !controller_dir.exists(),
        "a geometry mismatch retires the recorded controller"
    );
    assert!(
        !intent_path.exists(),
        "a declined adoption removes the intent so a fresh connect is not blocked"
    );
}

#[test]
fn caller_geometry_change_declines_adoption_and_unblocks_the_next_connect() {
    let temp = tempdir::TempDir::new("adopt-geometry-connect").unwrap();
    let (registry, root) = adoption_registry(temp.path());
    let target = adoption_target("nqn.2026-01.test:wal", 1);
    let controller_dir = fixture_controller(
        &root,
        4,
        &target,
        ADOPT_HOST,
        ADOPT_HOST_ID,
        "live",
        target.geometry,
    );
    let intent_path = registry.reserve(&adoption_intent(&target)).unwrap();
    registry.reconcile().unwrap();
    assert!(
        controller_dir.exists(),
        "the live controller survives the initial reconcile"
    );
    // The operator changed `queue_geometry` between the two opens. The
    // controller still matches the intent, so the intent-vs-target comparison
    // must decline before any resolver is built.
    let mut changed = target.clone();
    changed.geometry = LiveQueueGeometry {
        io_queues: 2,
        queue_size: 64,
        poll_queues: 0,
    };
    let remover = remove_controller_when_deleted(controller_dir.clone());
    let deadline = Instant::now() + PROGRESS_CAP;
    assert!(
        registry
            .try_adopt_controller(&changed, ADOPT_HOST, ADOPT_HOST_ID, deadline)
            .unwrap()
            .is_none(),
        "a caller geometry change is declined, not adopted"
    );
    remover.join().unwrap();
    assert!(
        !controller_dir.exists(),
        "the declined controller is retired with the intent's old geometry"
    );
    assert!(
        !intent_path.exists(),
        "a declined adoption removed the intent"
    );
    let fresh = adoption_intent(&changed);
    let path = registry
        .reserve(&fresh)
        .expect("a declined adoption must not leave a blocking intent");
    registry.remove_intent(&path).unwrap();
}

#[test]
fn two_matching_controllers_adopt_the_lowest_and_retire_the_other() {
    let temp = tempdir::TempDir::new("adopt-two").unwrap();
    let (registry, root) = adoption_registry(temp.path());
    let target = adoption_target("nqn.2026-01.test:wal", 1);
    let low = fixture_controller(
        &root,
        2,
        &target,
        ADOPT_HOST,
        ADOPT_HOST_ID,
        "live",
        target.geometry,
    );
    let high = fixture_controller(
        &root,
        5,
        &target,
        ADOPT_HOST,
        ADOPT_HOST_ID,
        "live",
        target.geometry,
    );
    let intent_path = registry.reserve(&adoption_intent(&target)).unwrap();
    let remover = remove_controller_when_deleted(high.clone());
    registry.reconcile().unwrap();
    remover.join().unwrap();
    assert!(low.exists(), "the lowest instance is kept");
    assert!(!high.exists(), "the extra live match is retired");
    assert!(
        intent_path.exists(),
        "the kept controller still owns its intent"
    );
    let deadline = Instant::now() + PROGRESS_CAP;
    let controller = registry
        .try_adopt_controller(&target, ADOPT_HOST, ADOPT_HOST_ID, deadline)
        .unwrap()
        .expect("the kept controller is adoptable");
    assert_eq!(controller.instance(), 2, "adoption is deterministic");
    let remover = remove_controller_when_deleted(low.clone());
    let mut controller = controller;
    controller.disconnect(deadline).unwrap();
    remover.join().unwrap();
    assert!(
        !low.exists(),
        "the adopted controller is retired at disconnect"
    );
}

#[test]
fn non_live_controller_is_retired_and_its_intent_removed() {
    let temp = tempdir::TempDir::new("adopt-non-live").unwrap();
    let (registry, root) = adoption_registry(temp.path());
    let target = adoption_target("nqn.2026-01.test:wal", 1);
    let controller_dir = fixture_controller(
        &root,
        6,
        &target,
        ADOPT_HOST,
        ADOPT_HOST_ID,
        "resetting",
        target.geometry,
    );
    let intent_path = registry.reserve(&adoption_intent(&target)).unwrap();
    let remover = remove_controller_when_deleted(controller_dir.clone());
    registry.reconcile().unwrap();
    remover.join().unwrap();
    assert!(!controller_dir.exists(), "a non-live controller is retired");
    assert!(
        !intent_path.exists(),
        "a non-live controller's intent is removed"
    );
}

#[test]
fn a_controller_of_another_host_is_never_adopted_or_retired() {
    let temp = tempdir::TempDir::new("adopt-foreign-host").unwrap();
    let (registry, root) = adoption_registry(temp.path());
    let target = adoption_target("nqn.2026-01.test:wal", 1);
    let foreign = fixture_controller(
        &root,
        8,
        &target,
        "nqn.2026-01.test:other-host",
        [3; 16],
        "live",
        target.geometry,
    );
    let intent_path = registry.reserve(&adoption_intent(&target)).unwrap();
    registry.reconcile().unwrap();
    assert!(
        foreign.exists(),
        "a controller of another host identity is not ours to retire"
    );
    assert!(
        !intent_path.exists(),
        "an intent with no matching controller is stale and removed"
    );
}

#[test]
fn a_crash_after_reconcile_leaves_the_adoptable_controller_and_intent() {
    let temp = tempdir::TempDir::new("adopt-crash").unwrap();
    let target = adoption_target("nqn.2026-01.test:wal", 1);
    let (registry, root) = adoption_registry(temp.path());
    let controller_dir = fixture_controller(
        &root,
        9,
        &target,
        ADOPT_HOST,
        ADOPT_HOST_ID,
        "live",
        target.geometry,
    );
    let intent_path = registry.reserve(&adoption_intent(&target)).unwrap();
    registry.reconcile().unwrap();
    // Simulate a crash between reconcile and the pool's connect: no adoption
    // record is kept in memory, only the controller and its durable intent.
    drop(registry);
    let registry = LiveConnectionRegistry::open_at(
        &temp.path().join("registry"),
        &root,
        &temp.path().join("no-udev-queue"),
        RegistryMode::Adopt,
        KEEP_ALIVE_TMO_SECS,
    )
    .unwrap();
    assert!(
        controller_dir.exists(),
        "the pre-crash controller survives the first reconcile"
    );
    assert!(intent_path.exists(), "the pre-crash intent remains durable");
    let deadline = Instant::now() + PROGRESS_CAP;
    let mut controller = registry
        .try_adopt_controller(&target, ADOPT_HOST, ADOPT_HOST_ID, deadline)
        .unwrap()
        .expect("the recovered state is still adoptable");
    assert_eq!(controller.instance(), 9);
    let remover = remove_controller_when_deleted(controller_dir);
    controller.disconnect(deadline).unwrap();
    remover.join().unwrap();
    assert!(
        !intent_path.exists(),
        "the recovered adoption removes its intent at disconnect"
    );
}

#[test]
fn already_connected_outcome_retries_only_when_no_controller_remains() {
    let temp = tempdir::TempDir::new("adopt-ealready").unwrap();
    let (registry, root) = adoption_registry(temp.path());
    let target = adoption_target("nqn.2026-01.test:wal", 1);
    let deadline = Instant::now() + PROGRESS_CAP;
    // Nothing for this identity: a retry is safe.
    assert!(
        registry
            .already_connected_outcome(&target, ADOPT_HOST, ADOPT_HOST_ID, deadline)
            .unwrap()
            .is_none()
    );
    // The bounded reconcile never runs past the caller's acquisition deadline.
    assert_eq!(
        registry
            .already_connected_outcome(&target, ADOPT_HOST, ADOPT_HOST_ID, Instant::now())
            .unwrap_err()
            .kind(),
        io::ErrorKind::TimedOut,
        "an expired caller deadline bounds the reconcile"
    );
    let live = fixture_controller(
        &root,
        1,
        &target,
        ADOPT_HOST,
        ADOPT_HOST_ID,
        "live",
        target.geometry,
    );
    let error = registry
        .already_connected_outcome(&target, ADOPT_HOST, ADOPT_HOST_ID, deadline)
        .unwrap()
        .expect("an unowned live controller is a typed fail-stop");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(
        error.to_string().contains("unowned NVMe controller"),
        "the fail-stop names the unowned identity: {error}"
    );
    // A controller the kernel is already deleting does not block a retry.
    std::fs::write(live.join("state"), "deleting").unwrap();
    assert!(
        registry
            .already_connected_outcome(&target, ADOPT_HOST, ADOPT_HOST_ID, deadline)
            .unwrap()
            .is_none()
    );
}

#[test]
fn connect_options_never_request_a_duplicate_controller() {
    let target = adoption_target("nqn.2026-01.test:wal", 1);
    let options = connect_options(&target, ADOPT_HOST, ADOPT_HOST_ID, KEEP_ALIVE_TMO_SECS);
    assert!(
        !options.contains("duplicate_connect"),
        "a second controller for one identity must be rejected by the kernel: {options}"
    );
    assert!(
        options.contains("ctrl_loss_tmo=0"),
        "reconnect policy stays ours: {options}"
    );
    assert!(
        already_connected(&io::Error::from_raw_os_error(libc::EALREADY)),
        "the kernel's -EALREADY is recognised as a tracked identity"
    );
    assert!(
        !already_connected(&io::Error::from_raw_os_error(libc::EIO)),
        "an unrelated I/O error is not retried as an identity conflict"
    );
}

/// Fails the real `write_all` call with one OS errno, so the test exercises the
/// production error mapping rather than a hand-built `io::Error`.
struct ErrnoWriter(i32);

impl Write for ErrnoWriter {
    fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
        Err(io::Error::from_raw_os_error(self.0))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn fabrics_connect_write_preserves_ealready_through_the_context_wrap() {
    let error = write_connect_options(&mut ErrnoWriter(libc::EALREADY), "nqn=test")
        .expect_err("the failing writer must fail the connect write");
    assert!(
        already_connected(&error),
        "the real write_all path must preserve -EALREADY for the bounded retry: {error}"
    );
    let error = write_connect_options(&mut ErrnoWriter(libc::EIO), "nqn=test")
        .expect_err("a non-EALREADY write failure must fail the connect");
    assert_eq!(
        error.raw_os_error(),
        None,
        "other write failures keep the control-device context: {error}"
    );
    assert!(
        error.to_string().contains("connect fabrics control"),
        "other write failures name the operation: {error}"
    );
}

#[test]
fn changed_endpoint_declines_adoption_and_still_retires_the_controller() {
    let temp = tempdir::TempDir::new("adopt-endpoint").unwrap();
    let (registry, root) = adoption_registry(temp.path());
    let target = adoption_target("nqn.2026-01.test:wal", 1);
    let controller_dir = fixture_controller(
        &root,
        4,
        &target,
        ADOPT_HOST,
        ADOPT_HOST_ID,
        "live",
        target.geometry,
    );
    let intent_path = registry.reserve(&adoption_intent(&target)).unwrap();
    registry.reconcile().unwrap();
    assert!(controller_dir.exists());
    let mut moved = target.clone();
    moved.service = 4421;
    let remover = remove_controller_when_deleted(controller_dir.clone());
    let deadline = Instant::now() + PROGRESS_CAP;
    assert!(
        registry
            .try_adopt_controller(&moved, ADOPT_HOST, ADOPT_HOST_ID, deadline)
            .unwrap()
            .is_none(),
        "a changed endpoint must not adopt the old controller"
    );
    remover.join().unwrap();
    assert!(
        !controller_dir.exists(),
        "the declined controller is retired with the intent's own endpoint"
    );
    assert!(!intent_path.exists());
}

/// The CI cell's teardown wrote `delete_controller` through a pinned directory
/// descriptor after the kernel had already stopped the controller, and the
/// store returned `EACCES`. That is a teardown already accepted, not a failure:
/// the state machine confirms the removal instead of reporting the store.
#[test]
fn retirement_accepts_a_refused_delete_once_the_controller_is_not_live() {
    let temp = tempdir::TempDir::new("controller-retire-eacces").unwrap();
    let path = temp.path().join("controller");
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("state"), "deleting\n").unwrap();
    let metadata = std::fs::metadata(&path).unwrap();
    let mut state = DeletionState::NotStarted;
    retire_controller(
        &mut state,
        &path,
        metadata.dev(),
        metadata.ino(),
        Instant::now() + PROGRESS_CAP,
        || Ok(()),
        || {
            // The kernel refuses the store and finishes the teardown.
            std::fs::remove_dir_all(&path).unwrap();
            Err(io::Error::from_raw_os_error(libc::EACCES))
        },
    )
    .unwrap();
    assert_eq!(state, DeletionState::Gone);
    assert!(!path.exists());
}

/// A refused store on a controller that is still live is a real permission
/// failure and must keep its error.
#[test]
fn retirement_keeps_a_refused_delete_of_a_live_controller() {
    let temp = tempdir::TempDir::new("controller-retire-live-eacces").unwrap();
    let path = temp.path().join("controller");
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("state"), "live\n").unwrap();
    let metadata = std::fs::metadata(&path).unwrap();
    let mut state = DeletionState::NotStarted;
    let error = retire_controller(
        &mut state,
        &path,
        metadata.dev(),
        metadata.ino(),
        Instant::now() + PROGRESS_CAP,
        || Ok(()),
        || Err(io::Error::from_raw_os_error(libc::EACCES)),
    )
    .expect_err("a live controller refuses the store for another reason");
    assert_eq!(error.raw_os_error(), Some(libc::EACCES));
    assert_eq!(state, DeletionState::Uncertain);
}

/// A deleting controller whose removal does not confirm within the deadline
/// keeps uncertainty instead of reporting the refused store.
#[test]
fn retirement_observes_a_deleting_controller_until_its_deadline() {
    let temp = tempdir::TempDir::new("controller-retire-deleting").unwrap();
    let path = temp.path().join("controller");
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("state"), "deleting\n").unwrap();
    let metadata = std::fs::metadata(&path).unwrap();
    let mut state = DeletionState::NotStarted;
    let error = retire_controller(
        &mut state,
        &path,
        metadata.dev(),
        metadata.ino(),
        Instant::now() + EXPECTED_TIMEOUT,
        || Ok(()),
        || Err(io::Error::from_raw_os_error(libc::EACCES)),
    )
    .expect_err("the removal never confirms");
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(state, DeletionState::Uncertain);
}

/// The resolver's own identity check reports a vanished or replaced controller
/// as the typed lost cause, so a reconnect classifies it like an EIO.
#[test]
fn resolver_verify_types_a_vanished_or_replaced_controller_as_lost() {
    let temp = tempdir::TempDir::new("resolver-verify-lost").unwrap();
    let controllers = temp.path().join("controllers");
    let controller = controllers.join("nvme7");
    std::fs::create_dir_all(&controller).unwrap();
    let target = LiveConnectTarget {
        nqn: "nqn.2026-01.test:sub".into(),
        uuid: [1; 16],
        nsid: 1,
        bytes: 4096,
        block_size: 4096,
        address: "127.0.0.1".into(),
        service: 4420,
        transport: NvmeofTransport::Tcp,
        geometry: Default::default(),
    };
    let host_nqn = "nqn.2026-01.test:host";
    let host_id = [2; 16];
    let directory = File::open(&controller).unwrap();
    let metadata = directory.metadata().unwrap();
    let write = |hostnqn: &str| {
        std::fs::write(controller.join("subsysnqn"), "nqn.2026-01.test:sub\n").unwrap();
        std::fs::write(controller.join("transport"), "tcp\n").unwrap();
        std::fs::write(controller.join("hostnqn"), format!("{hostnqn}\n")).unwrap();
        std::fs::write(
            controller.join("hostid"),
            "02020202-0202-0202-0202-020202020202\n",
        )
        .unwrap();
        std::fs::write(
            controller.join("address"),
            "traddr=127.0.0.1,trsvcid=4420\n",
        )
        .unwrap();
    };
    write(host_nqn);
    let resolver = LiveNamespaceResolver {
        target: target.clone(),
        host_nqn: host_nqn.into(),
        host_id,
        name: "nvme7".into(),
        directory,
        inode: metadata.ino(),
        device: metadata.dev(),
        queue: OnceLock::new(),
        controller_root: controllers.clone(),
        udev_queue: temp.path().join("no-udev-queue"),
        keep_alive_tmo: KEEP_ALIVE_TMO_SECS,
    };
    resolver.verify().expect("the matching controller verifies");
    // A live controller with another host identity is a real mismatch.
    write("nqn.2026-01.test:other");
    let error = resolver
        .verify()
        .expect_err("another host identity is fatal");
    assert!(!crate::distributed::nvmeof::live_namespace::controller_lost(&error));
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    // The same name resolving to a replacement object is the lost cause.
    std::fs::remove_dir_all(&controller).unwrap();
    std::fs::create_dir_all(&controller).unwrap();
    let error = resolver
        .verify()
        .expect_err("a replacement object cannot be the pinned controller");
    assert!(
        crate::distributed::nvmeof::live_namespace::controller_lost(&error),
        "a replaced controller is lost: {error}"
    );
    // A vanished name is lost as well.
    std::fs::remove_dir_all(&controller).unwrap();
    let error = resolver
        .verify()
        .expect_err("a vanished controller is lost");
    assert!(crate::distributed::nvmeof::live_namespace::controller_lost(
        &error
    ));
}

/// The keep-alive never outlasts the resume budget it was chosen for.
#[test]
fn keep_alive_fits_the_configured_resume_budget() {
    assert_eq!(super::keep_alive_tmo(50), 1);
    assert_eq!(super::keep_alive_tmo(1_000), 2);
    assert_eq!(super::keep_alive_tmo(2_000), 4);
    assert_eq!(super::keep_alive_tmo(2_500), 5);
    assert_eq!(super::keep_alive_tmo(30_000), 5);
    let target: LiveConnectTarget = serde_json::from_value(serde_json::json!({
        "nqn": "nqn.2026-01.test:wal", "uuid": vec![1; 16], "nsid": 1,
        "bytes": 4096, "block_size": 4096, "address": "127.0.0.1",
        "service": 4420, "transport": "tcp"
    }))
    .unwrap();
    let options = connect_options(
        &target,
        "nqn.2026-01.test:host",
        [2; 16],
        super::keep_alive_tmo(1_000),
    );
    assert!(
        options.contains("keep_alive_tmo=2"),
        "the derived keep-alive reaches the connect options: {options}"
    );
}
