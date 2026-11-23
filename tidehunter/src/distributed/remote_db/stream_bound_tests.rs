//! F2 at engine level: the retained-generation table is bounded by the control
//! region (`MAX_REGION_STREAMS`), and the open enforces the bound before it
//! reserves a generation.
//!
//! The retained list is crafted into the authority manifest, as a long history
//! of aborted reservations would leave it: an aborted generation that no owner
//! ever fenced holds no data and is trimmed by the maintenance of the open that
//! follows its reservation. With two owners the bound is
//! `2 x (retained + 1) <= 4096`, so 2,047 retained generations still open and
//! 2,048 are refused.
//!
//! Negative controls (mutation runs of production code, recorded in the
//! handoff): remove the `check_stream_bound` call before `reserve` in the open
//! and `a_reservation_at_the_bound_is_refused...` fails (the open reserves and
//! then fails inside seeding with the short invariant message and a changed
//! manifest); change the comparison to `>=` and the bound-minus-one test fails.

use super::pipeline_tests::Fixture;
use super::tests::checkpoint_put;
use super::*;
use crate::distributed::hybrid::region::MAX_REGION_STREAMS;
use crate::key_shape::KeyType;

const OWNERS: usize = 2;
/// Retained generations that still fit: `OWNERS x (retained + 1) <= bound`.
const MAX_RETAINED: usize = MAX_REGION_STREAMS / OWNERS - 1;

fn fixture() -> Fixture {
    Fixture::new_with_root(
        tempdir::TempDir::new("remote-stream-bound").unwrap(),
        OWNERS,
        128,
        KeyShape::new_single(4, 1, KeyType::uniform(1)),
        2,
        |_| {},
        |_| {},
    )
}

fn manifest_path(fixture: &Fixture) -> PathBuf {
    fixture.authority().join(MANIFEST)
}

fn read_manifest(fixture: &Fixture) -> AuthorityManifest {
    read_metadata(&manifest_path(fixture)).expect("manifest")
}

/// Opens and cleanly closes once with data, then pads the retained list with
/// aborted generations until it holds exactly `retained` entries (generation 1
/// is the served one).
fn crafted(retained: usize) -> Fixture {
    let fixture = fixture();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 7)])
        .expect("write");
    db.shutdown().expect("shutdown");
    drop(db);
    let mut manifest = read_manifest(&fixture);
    assert_eq!(manifest.generations.len(), 1);
    let first = manifest.next_generation;
    for generation in first..first + (retained - 1) as u64 {
        manifest.generations.push(AuthorityGeneration {
            generation,
            state: GenerationState::Aborted,
        });
    }
    manifest.next_generation = first + (retained - 1) as u64;
    fencing_generations::validate(&manifest).expect("a valid crafted history");
    persist_manifest(&fixture.authority(), &manifest, CoordinatorIoCategory::Open)
        .expect("persist the crafted manifest");
    assert_eq!(read_manifest(&fixture).generations.len(), retained);
    fixture
}

fn high_waters(fixture: &Fixture) -> Vec<u64> {
    fixture
        .nodes
        .iter()
        .map(|node| node.status().unwrap().highest_fenced_generation)
        .collect()
}

/// One below the bound: the reservation is accepted, the open seeds every
/// retained stream, serves, and its maintenance trims the empty aborted
/// generations, so the next open starts far from the bound again. The data of
/// the first generation is intact.
#[test]
fn a_database_one_generation_below_the_bound_opens_reserves_and_trims() {
    let fixture = crafted(MAX_RETAINED);
    let mut db = fixture.open();
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .expect("read")
            .as_deref(),
        Some([7u8; 32].as_slice())
    );
    let manifest = read_manifest(&fixture);
    assert!(
        manifest.generations.len() <= 3,
        "the open trimmed the aborted generations: {} retained",
        manifest.generations.len()
    );
    assert!(
        manifest
            .generations
            .iter()
            .any(|entry| entry.generation == 1)
    );
    assert!(
        manifest
            .generations
            .iter()
            .any(|entry| entry.generation == db.epoch())
    );
    assert!(
        !db.core.stream_pressure().is_soft(),
        "after the trim the table is far below the soft threshold"
    );
}

/// The soft-pressure signal on a live database: the gauge value
/// (`StreamPressure::is_soft`, exported as `retained_stream_pressure`) is 0 one
/// stream below 75 % of the bound and 1 at it, computed from the table the
/// retirement and snapshot passes use. The streams are injected into the
/// publisher's position table, as retained generations would be.
///
/// Negative control (mutation run): change `>=` to `>` in `is_soft`, or make
/// `stream_pressure` count `retained_epochs` instead of `positions`, and this
/// fails.
#[test]
fn the_pressure_gauge_fires_at_75_percent_of_the_bound_and_not_below() {
    let fixture = fixture();
    let db = fixture.open();
    // The publisher's position table is filled by the first publication.
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 7)])
        .expect("write");
    let soft = MAX_REGION_STREAMS * super::stream_positions::SOFT_PRESSURE_PERCENT / 100;
    let mut generation = 1_000_000u64;
    let mut grow_to = |target: usize| {
        let mut publisher = db.core.publisher.lock();
        let positions = &mut publisher.checkpoint_state().positions;
        let template = positions
            .values()
            .next()
            .cloned()
            .expect("the serving stream has a position");
        while positions.len() < target {
            positions.insert((0, generation), template.clone());
            generation += 1;
        }
    };
    grow_to(soft - 1);
    let below = db.core.stream_pressure();
    assert_eq!(below.streams, soft - 1);
    assert!(!below.is_soft());
    grow_to(soft);
    let at = db.core.stream_pressure();
    assert_eq!(at.streams, soft);
    assert!(at.is_soft());
}

/// At the bound the open is refused before anything is persisted: the manifest
/// bytes are unchanged, no owner was fenced, no owner generation directory
/// appeared, the error is the operator-facing one, and the refusal repeats
/// identically. The database on disk stays valid: a manual shrink of the
/// retained list (the stand-in for the unbuilt offline tool) opens it with its
/// data intact.
#[test]
fn a_reservation_at_the_bound_is_refused_with_nothing_persisted_or_fenced() {
    let fixture = crafted(MAX_RETAINED + 1);
    let bytes = std::fs::read(manifest_path(&fixture)).unwrap();
    let waters = high_waters(&fixture);
    for _ in 0..2 {
        let error = match fixture.try_open_with(fixture.config.clone()) {
            Ok(_) => panic!("an open at the bound must be refused"),
            Err(error) => error,
        };
        let text = error.to_string();
        for part in [
            "refusing this open",
            "No generation was reserved",
            "no in-band recovery",
            "retained_stream_pressure",
        ] {
            assert!(text.contains(part), "missing {part:?}: {text}");
        }
        assert_eq!(
            std::fs::read(manifest_path(&fixture)).unwrap(),
            bytes,
            "the manifest is byte-for-byte unchanged"
        );
        assert_eq!(high_waters(&fixture), waters, "no owner was fenced");
    }
    fencing_generations::validate(&read_manifest(&fixture)).expect("valid on disk");

    let mut manifest = read_manifest(&fixture);
    manifest
        .generations
        .retain(|entry| entry.state != GenerationState::Aborted);
    persist_manifest(&fixture.authority(), &manifest, CoordinatorIoCategory::Open)
        .expect("manual shrink");
    let mut db = fixture.open();
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .expect("read")
            .as_deref(),
        Some([7u8; 32].as_slice())
    );
}
