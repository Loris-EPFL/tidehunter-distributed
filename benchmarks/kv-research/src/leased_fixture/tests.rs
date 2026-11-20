use super::*;
use tidehunter::distributed::tcp::LeasedSync;
use tidehunter::key_shape::KeyType;

fn spec(root: PathBuf, tier: DurabilityTier, owners: Vec<FixtureOwner>) -> FixtureSpec {
    BenchmarkGeometry {
        fragment_bytes: 4 * MIB as u64,
        wal_file_bytes: 64 * MIB as u64,
        max_admitted_batches: 262_144,
        max_index_bytes: 512 * MIB,
    }
    .spec(root, tier, owners)
}

fn fixed_owners(count: u32) -> Vec<FixtureOwner> {
    (0..count)
        .map(|partition| FixtureOwner {
            partition,
            address: SocketAddr::from(([127, 0, 0, 1], 4000 + u16::try_from(partition).unwrap())),
        })
        .collect()
}

fn scratch(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "{name}-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    fs::create_dir_all(&root).unwrap();
    root
}

#[test]
fn benchmark_spec_translates_to_the_leased_per_owner_route() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let spec = spec(PathBuf::from("/fixture"), tier, fixed_owners(2));
        let config = spec.remote_config().unwrap();
        let live = &config.live_nvme;
        assert_eq!(live.leased_durability, tier);
        assert_eq!(live.leased_write_window, Some(1));
        assert_eq!(live.leased_sync, LeasedSync::PerFrameFua);
        assert!(live.lease_ahead && live.inline_reads && !live.polled_writes);
        assert_eq!(live.host_id, spec.host_id);
        assert_eq!(config.read_cache_bytes, 0);
        assert_eq!(config.max_admitted_batches, 262_144);
        assert_eq!(config.storage.max_batches_per_partition, 262_144);
        assert_eq!(config.storage.partitions, [0, 1]);
        assert_eq!(config.endpoints[0].address, "127.0.0.1:4000");
        assert_eq!(config.endpoints[1].address, "127.0.0.1:4001");
        // Provisioned by the coordinator open, never configured.
        assert_eq!(config.storage.database, [0; 16]);
    }
}

#[test]
fn owner_hosts_take_disjoint_slot_ranges_and_the_fixture_principal() {
    let spec = spec(
        PathBuf::from("/fixture"),
        DurabilityTier::Native,
        fixed_owners(2),
    );
    let first = spec.host_config(0).unwrap();
    let second = spec.host_config(1).unwrap();
    assert_eq!(first.root, PathBuf::from("/fixture/owner-p0"));
    assert_eq!(second.root, PathBuf::from("/fixture/owner-p1"));
    assert_eq!(first.max_databases, 1);
    assert_eq!(first.trusted_coordinator_id, Some(TRUSTED_COORDINATOR));
    assert_ne!(
        first.live_target_pool.public_service_base,
        second.live_target_pool.public_service_base
    );
    assert!(spec.host_config(MAX_OWNERS).is_err());
}

#[test]
fn spec_round_trips_and_rejects_unknown_fields_bad_owners_and_oversized_frames() {
    let spec = spec(
        PathBuf::from("/fixture"),
        DurabilityTier::Native,
        fixed_owners(1),
    );
    let mut document = serde_json::to_value(&spec).unwrap();
    assert_eq!(
        serde_json::from_value::<FixtureSpec>(document.clone()).unwrap(),
        spec
    );
    document["write_mode"] = serde_json::json!("leased");
    assert!(serde_json::from_value::<FixtureSpec>(document).is_err());
    let mut small = spec.clone();
    small.fragment_bytes = 1024 * 1024;
    assert!(small.remote_config().is_err());
    let mut gap = spec.clone();
    gap.owners[0].partition = 1;
    assert!(gap.remote_config().is_err());
    let mut remote = spec;
    remote.owners[0].address = "10.0.0.1:4000".parse().unwrap();
    assert!(remote.remote_config().is_err());
}

#[test]
fn saved_spec_keeps_identity_and_rejects_duplicate_endpoints() {
    let root = scratch("leased-fixture-spec");
    let mut saved = spec(
        root.join("fixture"),
        DurabilityTier::Native,
        fixed_owners(2),
    );
    saved.create().unwrap();
    assert_eq!(FixtureSpec::read(&saved.root).unwrap(), saved);
    assert!(saved.create().is_err());
    saved.owners[1].address = saved.owners[0].address;
    assert!(saved.remote_config().is_err());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn in_process_owners_serve_reopen_with_owner_state_kept() {
    let root = scratch("in-process");
    let spec = spec(
        root.join("fixture"),
        DurabilityTier::Native,
        fresh_owners(2).unwrap(),
    );
    let shape = KeyShape::new_single(32, 16, KeyType::uniform(64));
    let owner = LeasedOwner::start(&spec).unwrap();
    let coordinator = root.join("coordinator");
    let ks = shape.iter_ks().next().unwrap().id();
    let db = owner.open(&coordinator, shape.clone()).unwrap();
    let database = db.remote_database_id().unwrap();
    assert_ne!(database, [0; 16]);
    for id in 0..64_u8 {
        db.insert(ks, vec![id; 32], vec![id; 1024]).unwrap();
    }
    db.wait_for_background_threads_to_finish().unwrap();
    let db = owner.open(&coordinator, shape.clone()).unwrap();
    assert_eq!(db.remote_database_id(), Some(database));
    assert_eq!(
        db.get(ks, &[9; 32]).unwrap().as_deref(),
        Some(&[9; 1024][..])
    );
    db.wait_for_background_threads_to_finish().unwrap();
    owner.stop().unwrap();
    // A second owner process lifetime recovers the same owner roots on the
    // same endpoints.
    let owner = LeasedOwner::start(&spec).unwrap();
    let db = owner.open(&coordinator, shape).unwrap();
    assert_eq!(
        db.get(ks, &[63; 32]).unwrap().as_deref(),
        Some(&[63; 1024][..])
    );
    db.wait_for_background_threads_to_finish().unwrap();
    owner.stop().unwrap();
    fs::remove_dir_all(root).unwrap();
}
