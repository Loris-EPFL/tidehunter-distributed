use super::*;

#[test]
fn bounded_selection_errors_and_rearm_reset_maxima() {
    let diagnostics = CoordinatorResolutionDiagnostics::default();
    assert!(diagnostics.select(false, 1).is_none());
    diagnostics.arm(1);
    let sample = diagnostics.select(false, 1).unwrap();
    sample.member(MemberOutcome::Error);
    diagnostics.index_lookup.add(23);
    drop(sample);
    assert!(diagnostics.select(true, 2).is_none());
    let snapshot = diagnostics.snapshot();
    assert_eq!(
        (snapshot.selected, snapshot.errors, snapshot.member_errors),
        (1, 1, 1)
    );
    assert_eq!(
        (
            snapshot.index_lookup.count,
            snapshot.index_lookup.sum_ns,
            snapshot.index_lookup.max_ns
        ),
        (1, 23, 23)
    );
    diagnostics.arm(0);
    assert_eq!(diagnostics.snapshot().index_lookup.max_ns, 23);
    diagnostics.arm(1);
    let reset = diagnostics.snapshot();
    assert_eq!(
        (
            reset.selected,
            reset.errors,
            reset.index_lookup.count,
            reset.index_lookup.max_ns
        ),
        (0, 0, 0, 0)
    );
}
