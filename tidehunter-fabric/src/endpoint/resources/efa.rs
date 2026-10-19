//! Queue geometry reviewed against AWS libfabric 2.6.0amzn1.0 / upstream
//! 3cc1095272b07ebeb5cc8f44851af6aa3eb75e41. See DESIGN.md for source links.

use super::{CHUNK, OpenProfile, SLOTS, ffi, invalid};
use std::io;

// Host FI_MSG uses eager/medium through CHUNK with the supported thresholds.
// The medium base header is 24 bytes, plus at most the 40-byte padded raw
// address header. 128 is deliberately conservative, not the application's TLS
// overhead (which is already inside CHUNK). Eight-byte balancing in
// efa_rdm_ope_prepare_to_post_send can add a final packet after ceil division.
const MIN_MTU: usize = 4096;
const HEADER_BOUND: usize = 128;
const PACKETS_PER_CHUNK: usize = CHUNK.div_ceil(MIN_MTU - HEADER_BOUND) + 1;
// One peer, four sends and four receives: reserve four packets per live
// operation for handshake/control/progress, beyond the entire data burst.
// Eager/medium has no CTS/RDMA-read work; RNR retries reuse packet entries.
const PROGRESS_RESERVE: usize = 4 * (2 * SLOTS);
pub(super) const QUEUE: usize = (SLOTS * PACKETS_PER_CHUNK + PROGRESS_RESERVE).next_power_of_two();
// OFI_VERSION_DEF_PROV encodes (library_major * 100 + library_minor, revision * 10).
// This is fi_fabric_attr::prov_version, not the libfabric API version.
const PROVIDER_VERSION: u32 = 206 << 16;

pub(super) fn validate(profile: OpenProfile, protocol: u32, version: u32) -> io::Result<()> {
    if profile.provider_version != PROVIDER_VERSION
        || protocol != ffi::FI_PROTO_EFA
        || version != 4
        || profile.mtu < MIN_MTU
        || profile.selected_tx != QUEUE
        || profile.selected_rx != QUEUE
    {
        return Err(invalid(format!(
            "unsupported bounded EFA geometry: require provider 206.0 (libfabric 2.6.0), protocol EFA/4, MTU >= {MIN_MTU}, TX/RX {QUEUE}; got provider {}.{}, protocol {protocol}/{version}, MTU {}, TX/RX {}/{}",
            profile.provider_version >> 16,
            profile.provider_version & 0xffff,
            profile.mtu,
            profile.selected_tx,
            profile.selected_rx,
        )));
    }
    Ok(())
}

// These knobs can otherwise invalidate the packet/progress proof without
// changing fi_getinfo's returned queue sizes. Never mutate process environment.
// Equal defaults and harmless larger limits remain supported.
const PARAMETERS: [&str; 7] = [
    "FI_EFA_TX_SIZE",
    "FI_EFA_RX_SIZE",
    "FI_EFA_TX_QUEUE_SIZE",
    "FI_EFA_INTERNAL_RX_REFILL_THRESHOLD",
    "FI_EFA_INTER_MAX_MEDIUM_MESSAGE_SIZE",
    "FI_EFA_INTER_MIN_READ_MESSAGE_SIZE",
    "FI_EFA_EFA_CQ_READ_SIZE",
];

fn valid_parameter(name: &str, value: usize) -> bool {
    match name {
        "FI_EFA_TX_SIZE" | "FI_EFA_RX_SIZE" => value == 0 || value >= QUEUE,
        "FI_EFA_TX_QUEUE_SIZE" => (value == 0 || value >= QUEUE) && i32::try_from(value).is_ok(),
        // efa_rdm_ep_bulk_post_internal_rx_pkts clamps this to the actual
        // RX pool size, ensuring even an oversized threshold refills at empty.
        "FI_EFA_INTERNAL_RX_REFILL_THRESHOLD" => true,
        "FI_EFA_INTER_MAX_MEDIUM_MESSAGE_SIZE" => value >= CHUNK,
        "FI_EFA_INTER_MIN_READ_MESSAGE_SIZE" => value > CHUNK,
        "FI_EFA_EFA_CQ_READ_SIZE" => value > 0,
        _ => false,
    }
}

pub(super) fn check_environment() -> io::Result<()> {
    for name in PARAMETERS {
        if let Some(value) = std::env::var_os(name) {
            let supported = value
                .to_str()
                .and_then(|value| value.parse::<usize>().ok())
                .is_some_and(|value| valid_parameter(name, value));
            if !supported {
                return Err(invalid(format!(
                    "{name}={} is incompatible with bounded EFA queues; unset it or use a supported value (see tidehunter-fabric/DESIGN.md)",
                    value.to_string_lossy().escape_debug()
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn supported() -> OpenProfile {
        OpenProfile {
            selected_tx: QUEUE,
            selected_rx: QUEUE,
            mtu: MIN_MTU,
            // Captured OFI_VERSION_DEF_PROV for libfabric 2.6.0, not its API version.
            provider_version: 206 << 16,
            ..OpenProfile::default()
        }
    }

    #[test]
    fn packet_budget_covers_aligned_maximum_bursts_and_progress() {
        // Exercise the pinned provider's balancing algorithm for every legal
        // message length, including the final-packet alignment boundary. This
        // tests geometry, not source text or availability of hardware.
        let payload = MIN_MTU - HEADER_BOUND;
        for len in 1..=CHUNK {
            let count = len.div_ceil(payload);
            let actual = if count == 1 {
                1
            } else {
                let segment = len.div_ceil(count) & !7;
                let mut count = len / segment;
                let remainder = len - count * segment;
                if segment + remainder > payload {
                    count += 1;
                }
                count
            };
            assert!(actual <= PACKETS_PER_CHUNK, "length {len}");
            assert!(SLOTS * actual + PROGRESS_RESERVE <= QUEUE);
        }
        assert_eq!(PACKETS_PER_CHUNK, 18);
        assert_eq!(QUEUE, 128);
        assert!(CHUNK.div_ceil(payload) > SLOTS);
    }

    #[test]
    fn returned_geometry_rejects_small_widened_missing_or_unreviewed_limits() {
        let good = supported();
        validate(good, ffi::FI_PROTO_EFA, 4).unwrap();
        validate(OpenProfile { mtu: 8192, ..good }, ffi::FI_PROTO_EFA, 4).unwrap();
        for capacity in [0, SLOTS, QUEUE - 1, QUEUE + 1, 8192] {
            assert!(
                validate(
                    OpenProfile {
                        selected_tx: capacity,
                        ..good
                    },
                    ffi::FI_PROTO_EFA,
                    4
                )
                .is_err()
            );
            assert!(
                validate(
                    OpenProfile {
                        selected_rx: capacity,
                        ..good
                    },
                    ffi::FI_PROTO_EFA,
                    4
                )
                .is_err()
            );
        }
        for mtu in [0, HEADER_BOUND, MIN_MTU - 1] {
            assert!(validate(OpenProfile { mtu, ..good }, ffi::FI_PROTO_EFA, 4).is_err());
        }
        assert!(validate(good, ffi::FI_PROTO_EFA, 3).is_err());
        assert!(validate(good, 0, 4).is_err());
        for provider_version in [(2 << 16) | 6, PROVIDER_VERSION + 1] {
            assert!(
                validate(
                    OpenProfile {
                        provider_version,
                        ..good
                    },
                    ffi::FI_PROTO_EFA,
                    4
                )
                .is_err()
            );
        }
    }

    #[test]
    fn queue_overrides_accept_defaults_and_reject_progress_deadlocks() {
        for name in ["FI_EFA_TX_SIZE", "FI_EFA_RX_SIZE", "FI_EFA_TX_QUEUE_SIZE"] {
            for value in [0, QUEUE, 1024] {
                assert!(valid_parameter(name, value));
            }
            for value in [1, SLOTS, QUEUE - 1] {
                assert!(!valid_parameter(name, value));
            }
        }
        assert!(valid_parameter("FI_EFA_INTERNAL_RX_REFILL_THRESHOLD", 8));
        assert!(valid_parameter(
            "FI_EFA_INTERNAL_RX_REFILL_THRESHOLD",
            QUEUE
        ));
        assert!(valid_parameter(
            "FI_EFA_INTERNAL_RX_REFILL_THRESHOLD",
            usize::MAX
        ));
        assert!(valid_parameter(
            "FI_EFA_INTER_MAX_MEDIUM_MESSAGE_SIZE",
            CHUNK
        ));
        assert!(!valid_parameter(
            "FI_EFA_INTER_MAX_MEDIUM_MESSAGE_SIZE",
            CHUNK - 1
        ));
        assert!(valid_parameter(
            "FI_EFA_INTER_MIN_READ_MESSAGE_SIZE",
            CHUNK + 1
        ));
        assert!(!valid_parameter(
            "FI_EFA_INTER_MIN_READ_MESSAGE_SIZE",
            CHUNK
        ));
        assert!(valid_parameter("FI_EFA_EFA_CQ_READ_SIZE", 50));
        assert!(!valid_parameter("FI_EFA_EFA_CQ_READ_SIZE", 0));
    }
}
