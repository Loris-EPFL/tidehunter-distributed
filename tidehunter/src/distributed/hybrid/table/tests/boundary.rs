//! Equality cases of the cut comparisons in the hybrid table, one table per
//! site (RustReview4 audit `AUDIT-conventions-blob-and-cuts.md`, Part B). A cut
//! is exclusive (`below(cut)` = `version < cut`), so each comparison has an
//! "equal" case that is the normal state and a "one above" case that must be
//! refused; the three values (limit - 1, limit, limit + 1) are asserted here so
//! an off-by-one in either direction fails a test that names the site.
//!
//! Sites covered here:
//! * B4 `HybridLoader::ensure_durable_target`: `cut <= durable`;
//! * B15 `HybridTable::snapshot_for_relocation`: defer iff `q > promotion_cut`.
//!
//! Sites with an existing equality pin (see the handoff table): B9 in
//! `hybrid/snapshot_tests.rs` (`a_pass_with_no_advance...`, the forced
//! same-cut test and the below-stored error test), B12/B13 in
//! `write_core/allocation/tests.rs`, B16 in `relocation_unit_tests.rs`, B17 in
//! `copy_durable_pass_tests.rs`. B19/B20/B21 are in `dead_wal_files/tests.rs`
//! and B24 in `durable_cut/tests.rs`.
//!
//! Negative controls (mutation runs of production code): B4 `<=` -> `<`, B15
//! `q > promotion_cut` -> `q >= promotion_cut`.

use super::{Fixture, version};
use crate::distributed::codec::RecordKind;

/// B4: a flush at exactly the durable cut covers `< durable`, which the owners
/// have synced; one above is refused with the named error and stores nothing.
#[test]
fn the_release_check_accepts_the_durable_cut_and_refuses_one_above() {
    let fixture = Fixture::new("hybrid-boundary-durable");
    fixture.cuts.set_durable(version(4));
    let stored = fixture.table.stored_cut();
    for (cut, accepted) in [(3, true), (4, true), (5, false)] {
        let result = fixture.table.loader().ensure_durable_target(version(cut));
        assert_eq!(
            result.is_ok(),
            accepted,
            "cut {cut} against durable 4: {result:?}"
        );
        if let Err(error) = result {
            assert!(
                error.to_string().contains("is above the durable cut"),
                "{error}"
            );
        }
    }
    assert_eq!(fixture.table.stored_cut(), stored, "a check stores nothing");
}

/// B15: with the flush clamp held at 4, a scan cut of 3 and of 4 is covered (a
/// forced store), a scan cut of 5 is deferred with `stored == false`, and the
/// deferral reports the clamp as its promotion cut.
#[test]
fn a_relocation_scan_cut_is_covered_up_to_the_clamp_and_deferred_one_above() {
    let fixture = Fixture::new("hybrid-boundary-clamp");
    fixture.publish(RecordKind::Put, 1, 2, 100);
    fixture.table.advance_applied(version(4));
    fixture.cuts.set_durable(version(4));
    // Flush the dirty cell so the stored region names the head itself.
    fixture
        .table
        .snapshot_for_relocation(version(4))
        .expect("baseline");
    let (_, latch) = fixture.table.pin_checkpoint();
    fixture.table.advance_applied(version(9));
    fixture.cuts.set_durable(version(9));
    assert_eq!(fixture.table.last_processed(), version(4), "the clamp");

    // One above the clamp first: a deferral has no side effects.
    let above = fixture
        .table
        .snapshot_for_relocation(version(5))
        .expect("a deferral is not an error");
    assert!(!above.stored && !above.forced, "{above:?}");
    assert_eq!(above.promotion_cut, version(4));

    for q in [3, 4] {
        let covered = fixture
            .table
            .snapshot_for_relocation(version(q))
            .expect("a scan cut at or below the clamp is covered");
        assert!(covered.stored && covered.forced, "q {q}: {covered:?}");
        assert!(covered.promotion_cut >= version(4), "q {q}: {covered:?}");
    }
    drop(latch);
}
