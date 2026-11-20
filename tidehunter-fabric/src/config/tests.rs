use super::{DEFAULT_SLOTS, FabricBackend, FabricConfig, MAX_SLOTS};
use std::{
    io,
    net::{IpAddr, Ipv4Addr},
};

#[test]
fn invalid_domain_and_source_selectors_are_rejected() {
    let mut config = FabricConfig {
        backend: FabricBackend::Verbs,
        domain: Some(String::new()),
        source_address: None,
        slots: None,
    };
    assert_eq!(
        config.validate().unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    config.domain = None;
    config.source_address = Some(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    assert_eq!(
        config.validate().unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    config.source_address = Some(IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)));
    assert_eq!(
        config.validate().unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    config.backend = FabricBackend::Verbs;
    config.source_address = Some(IpAddr::V6(std::net::Ipv6Addr::LOCALHOST));
    assert_eq!(
        config.validate().unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn slot_count_is_bounded() {
    let config = |backend, slots| FabricConfig {
        backend,
        domain: None,
        source_address: None,
        slots,
    };
    assert_eq!(
        config(FabricBackend::Verbs, None).effective_slots(),
        DEFAULT_SLOTS
    );
    for slots in [1, DEFAULT_SLOTS, 16, MAX_SLOTS] {
        config(FabricBackend::Verbs, Some(slots))
            .validate()
            .unwrap();
        config(FabricBackend::SoftwareTcp, Some(slots))
            .validate()
            .unwrap();
    }
    for slots in [0, MAX_SLOTS + 1, usize::MAX] {
        assert_eq!(
            config(FabricBackend::Verbs, Some(slots))
                .validate()
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
