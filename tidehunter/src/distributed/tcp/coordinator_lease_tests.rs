//! Owner coordinator-lease cell: grant, holder exclusivity, expiry margin and
//! the lapsed gate. Every premise is injected through `renew_at`, so no test
//! depends on a wall clock.

use super::*;

const HOLDER_A: [u8; 16] = [1; 16];
const HOLDER_B: [u8; 16] = [2; 16];

fn cell_at_epoch(epoch: u64) -> CoordinatorLeaseCell {
    let cell = CoordinatorLeaseCell::new();
    cell.activate(epoch);
    cell
}

#[test]
fn stale_authority_epoch_is_refused() {
    let cell = cell_at_epoch(3);
    let error = cell
        .renew_at(Instant::now(), 2, HOLDER_A, 5_000)
        .expect_err("a renewal of another epoch must be refused");
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::PermissionDenied,
        "a stale authoritative epoch is a permission error: {error}"
    );
    assert_eq!(
        error.to_string(),
        "stale coordinator lease authority",
        "the refusal names the stale authority"
    );
}

#[test]
fn second_holder_is_refused_while_the_lease_is_valid() {
    let cell = cell_at_epoch(1);
    let now = Instant::now();
    assert_eq!(
        cell.renew_at(now, 1, HOLDER_A, 5_000)
            .expect("the first grant succeeds"),
        5_000,
        "the first grant carries the requested duration"
    );
    let error = cell
        .renew_at(now + Duration::from_secs(1), 1, HOLDER_B, 5_000)
        .expect_err("another incarnation must not share a valid lease");
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::PermissionDenied,
        "a second holder is a permission error: {error}"
    );
    assert_eq!(
        error.to_string(),
        "coordinator lease held by another incarnation",
        "the refusal names the current holder rule"
    );
}

#[test]
fn second_holder_is_granted_after_the_owner_margin() {
    let cell = cell_at_epoch(1);
    let now = Instant::now();
    cell.renew_at(now, 1, HOLDER_A, 5_000)
        .expect("the first grant succeeds");
    let expires = now + Duration::from_secs(5);
    let before = cell.renew_at(expires, 1, HOLDER_B, 5_000);
    assert!(
        before.is_err(),
        "the margin still covers the old holder at its expiry"
    );
    assert_eq!(
        cell.renew_at(expires + OWNER_MARGIN, 1, HOLDER_B, 5_000)
            .expect("the old holder is past its grace period"),
        5_000,
        "the new holder receives the requested duration"
    );
}

#[test]
fn lapsed_cell_refuses_writes_and_renewals() {
    let cell = cell_at_epoch(1);
    cell.state.lock().lapsed = true;
    let error = cell
        .check_writable(1)
        .expect_err("a lapsed epoch must refuse writes");
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::PermissionDenied,
        "a lapsed lease is a permission error: {error}"
    );
    assert_eq!(
        error.to_string(),
        "coordinator lease lapsed",
        "the refusal names the lapse"
    );
    assert!(
        cell.check_writable(9).is_ok(),
        "another epoch is refused by its own authority check, not by this lapse"
    );
    let error = cell
        .renew_at(Instant::now(), 1, HOLDER_A, 5_000)
        .expect_err("a lapsed epoch must refuse renewals");
    assert_eq!(
        error.to_string(),
        "coordinator lease lapsed",
        "a renewal cannot revive a lapsed epoch"
    );
}

#[test]
fn granted_duration_is_clamped() {
    let cell = cell_at_epoch(1);
    let now = Instant::now();
    assert_eq!(
        cell.renew_at(now, 1, HOLDER_A, 10)
            .expect("a short grant is clamped"),
        MIN_LEASE_MS,
        "a duration below the minimum becomes the minimum"
    );
    assert_eq!(
        cell.renew_at(now, 1, HOLDER_A, 60_000)
            .expect("a long grant is clamped"),
        MAX_LEASE_MS,
        "a duration above the maximum becomes the maximum"
    );
}

#[test]
fn a_stopped_watchdog_returns_at_once() {
    let cell = cell_at_epoch(1);
    let stop = AtomicBool::new(true);
    assert_eq!(
        cell.wait_lapse(&stop),
        None,
        "a stopped watchdog must not report a lapse"
    );
}

#[test]
fn a_stale_seal_leaves_the_newer_epoch_lease_alone() {
    let cell = CoordinatorLeaseCell::new();
    let now = Instant::now();
    cell.activate(1);
    cell.renew_at(now, 1, HOLDER_A, 5_000)
        .expect("epoch 1 grants");
    // A new authority fences; the old Seal arrives afterwards.
    cell.activate(2);
    cell.renew_at(now, 2, HOLDER_A, 5_000)
        .expect("epoch 2 grants");
    cell.deactivate(1);
    assert!(!cell.is_lapsed(), "the newer epoch is not lapsed");
    assert_eq!(
        cell.renew_at(now, 2, HOLDER_A, 1_000)
            .expect("epoch 2 still renews after the stale Seal"),
        1_000,
        "the newer epoch keeps its lease"
    );
    cell.deactivate(2);
    let error = cell
        .renew_at(now, 2, HOLDER_A, 1_000)
        .expect_err("a sealed epoch grants nothing");
    assert_eq!(
        error.to_string(),
        "stale coordinator lease authority",
        "the sealed epoch is inactive"
    );
}

/// A closing skip frame goes through the same submission choke point as a data
/// frame: an expired lease refuses it before any DMA and writes no marker.
#[test]
fn an_expired_lease_refuses_the_closing_skip_frame() {
    use crate::distributed::durability::DurabilityTier;
    use crate::distributed::tcp::leased_tests::Fixture;

    let fixture = Fixture::new(DurabilityTier::Synchronous, 16384);
    let mut client = fixture.writer_without_ahead();
    let leased = client
        .lease_live_extent(1)
        .expect("the fixture owner grants a lease");
    client
        .install_leased_extent(leased)
        .expect("the extent opens");
    client.set_lease_validity(Arc::new(
        crate::distributed::remote_db::coordinator_lease::LeaseValidity::expired_for_test(),
    ));
    let before = fixture.metrics.snapshot().counts.leased_skip_markers;
    let refused = client
        .write_leased_skip()
        .expect_err("an expired lease refuses the closing skip frame");
    assert!(
        refused.to_string().contains("coordinator lease lapsed"),
        "{refused}"
    );
    assert_eq!(
        fixture.metrics.snapshot().counts.leased_skip_markers,
        before,
        "no skip marker is written after expiry"
    );
}
