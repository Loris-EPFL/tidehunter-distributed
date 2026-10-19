//! Shared names for nested setup worker sums, never additive reopen wall time.
use tidehunter_fabric::OpenProfile;

pub fn counters(profile: OpenProfile) -> [(&'static str, u64); 18] {
    [
        (
            "fabric_open_allocation_worker_sum_ns",
            profile.allocation_ns,
        ),
        ("fabric_open_discovery_worker_sum_ns", profile.discovery_ns),
        ("fabric_open_fabric_worker_sum_ns", profile.fabric_ns),
        ("fabric_open_domain_worker_sum_ns", profile.domain_ns),
        ("fabric_open_cq_worker_sum_ns", profile.cq_ns),
        ("fabric_open_av_worker_sum_ns", profile.av_ns),
        ("fabric_open_endpoint_worker_sum_ns", profile.endpoint_ns),
        (
            "fabric_open_bind_enable_worker_sum_ns",
            profile.bind_enable_ns,
        ),
        ("fabric_open_register_worker_sum_ns", profile.register_ns),
        ("fabric_open_address_worker_sum_ns", profile.address_ns),
        (
            "fabric_open_peer_setup_worker_sum_ns",
            profile.peer_setup_ns,
        ),
        (
            "fabric_open_first_progress_worker_sum_ns",
            profile.first_progress_ns,
        ),
        ("fabric_open_requested_tx_max", profile.requested_tx as u64),
        ("fabric_open_requested_rx_max", profile.requested_rx as u64),
        ("fabric_open_selected_tx_max", profile.selected_tx as u64),
        ("fabric_open_selected_rx_max", profile.selected_rx as u64),
        ("fabric_open_mtu_max", profile.mtu as u64),
        (
            "fabric_open_provider_version_max",
            u64::from(profile.provider_version),
        ),
    ]
}
