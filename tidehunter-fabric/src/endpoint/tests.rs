use super::*;

#[test]
fn expired_deadline_does_not_submit() {
    assert_eq!(
        remaining(Instant::now()).unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
}

#[test]
fn active_progress_never_sleeps_and_idle_backoff_requires_no_sends() {
    let active = Progress::new(false);
    let idle = Progress::new(true);
    let later = Instant::now() + Duration::from_secs(1);
    assert!(!active.should_sleep(false, later));
    assert!(!active.should_sleep(true, later));
    assert!(!idle.should_sleep(false, idle.last_completion));
    assert!(!idle.should_sleep(true, later));
    assert!(idle.should_sleep(false, later));
}

#[test]
fn domain_selector_cannot_contain_nul() {
    let config = FabricConfig {
        backend: FabricBackend::Verbs,
        domain: Some("verbs\0device".to_owned()),
        source_address: None,
        slots: None,
    };
    assert_eq!(
        config.validate().unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}
