//! The retirement cap's decision for one shadowed as-of position, without a
//! database: which read cuts can still name a position the pass could not
//! re-point.
//!
//! Every shadowed expectation carries the version of the cell entry that
//! stopped its compare-and-set. A read at cut `c` resolves the newest version
//! below `c`, so it resolves the shadowed position exactly when `c` is at or
//! below that shadowing version. With no latch held, only a shadowing version
//! the published cut does not cover can be named by a latch created later
//! (`relocation.rs`: the pass reads the publication floor before the latch
//! mirror, the SeqCst pair in `CutLatches::publish_min` / `min_held`).
use super::{published_covers, shadow_needs_cap};

fn version(epoch: u64, sequence: u64) -> crate::distributed::types::BatchVersion {
    crate::distributed::types::BatchVersion { epoch, sequence }
}

/// A latch above the shadowing version resolves that version or a newer entry;
/// the old position it shadowed is not reachable through it.
#[test]
fn a_latch_above_the_shadowing_version_needs_no_cap() {
    assert!(
        !shadow_needs_cap(Some(version(7, 90)), version(7, 90), version(7, 40)),
        "a latch at 90 resolves the entry at 40 or newer, not the position below it"
    );
}

/// A latch at or below the shadowing version still resolves the old position:
/// its cut does not exclude the shadowing entry's predecessor. One sequence
/// above it does, because the cut then resolves the shadowing entry itself.
#[test]
fn a_latch_at_or_below_the_shadowing_version_is_capped() {
    for cut in [1u64, 39, 40] {
        assert!(
            shadow_needs_cap(Some(version(7, cut)), version(7, 90), version(7, 40)),
            "a latch at {cut} resolves the shadowed position below 40"
        );
    }
    for cut in [41u64, 65, 89] {
        assert!(
            !shadow_needs_cap(Some(version(7, cut)), version(7, 90), version(7, 40)),
            "a latch at {cut} resolves the entry at 40 or newer, not the position below it"
        );
    }
}

/// Without a latch, a shadowing version the published cut covers cannot be
/// named by a latch created later: every such latch takes a cut above it.
#[test]
fn a_covered_shadowing_version_needs_no_cap_without_a_latch() {
    assert!(
        !shadow_needs_cap(None, version(7, 91), version(7, 90)),
        "published next 91 covers version 90, so a later latch is above it"
    );
}

/// Without a latch, an unpublished shadowing version (in the index, not yet
/// covered by the publication floor) can still be named by a latch created
/// before it publishes.
#[test]
fn an_uncovered_shadowing_version_is_capped_without_a_latch() {
    assert!(
        shadow_needs_cap(None, version(7, 90), version(7, 90)),
        "a shadowing version at the published cut is not covered yet"
    );
    assert!(
        shadow_needs_cap(None, version(7, 40), version(7, 90)),
        "a shadowing version above the published cut is not covered yet"
    );
}

/// Epochs are fencing generations: an older epoch is entirely below a newer
/// one, so a latch of the serving epoch never resolves an older generation's
/// shadowed position, and an older shadowing version is covered.
#[test]
fn epoch_comparisons_follow_the_fencing_order() {
    assert!(
        !shadow_needs_cap(Some(version(7, 1)), version(7, 2), version(6, 500)),
        "a serving-epoch latch is above every position of an older generation"
    );
    assert!(
        published_covers(version(7, 0), version(6, 500)),
        "a newer published epoch covers an older generation's version"
    );
    assert!(
        !published_covers(version(6, 500), version(7, 1)),
        "an older published epoch does not cover a newer generation's version"
    );
    assert!(
        shadow_needs_cap(Some(version(7, 2)), version(7, 3), version(7, 2)),
        "a latch in the shadowing version's own epoch at that version is capped"
    );
}
