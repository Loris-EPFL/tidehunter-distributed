use super::qualified_live_target_shutdown;
use tidehunter::distributed::nvmeof::LiveTargetStats;

#[test]
fn shutdown_requires_clean_counters() {
    let mut stats = LiveTargetStats::default();
    assert!(qualified_live_target_shutdown(true, Some(&stats)));
    assert!(!qualified_live_target_shutdown(false, Some(&stats)));
    assert!(!qualified_live_target_shutdown(true, None));
    stats.owned_namespace_count = 1;
    assert!(!qualified_live_target_shutdown(true, Some(&stats)));
    stats.owned_namespace_count = 0;
    stats.errors = 1;
    assert!(!qualified_live_target_shutdown(true, Some(&stats)));
    stats.errors = 0;
    stats.fixture_only = true;
    assert!(!qualified_live_target_shutdown(true, Some(&stats)));
}
