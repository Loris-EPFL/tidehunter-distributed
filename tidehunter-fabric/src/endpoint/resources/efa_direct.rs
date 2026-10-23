//! Strict negotiation checks for the pinned libfabric 2.6 EFA-direct fabric.
use super::super::CHUNK;
use super::super::efa_direct::HEADER_LEN;
use super::{SLOTS, ffi, invalid};
use std::io;

const PROVIDER_VERSION: u32 = 206 << 16;
const MIN_MTU: usize = HEADER_LEN + 1;
// These are the flat attributes of one negotiated provider geometry. Validate
// them together before any provider handles or DMA operations can be created.
#[allow(clippy::too_many_arguments)]
pub(super) fn validate(
    provider_version: u32,
    protocol: u32,
    protocol_version: u32,
    max_message: usize,
    rma: bool,
    tx_size: usize,
    rx_size: usize,
    mode: u64,
    data_progress: u32,
    mr_mode: i32,
    mtu: usize,
) -> io::Result<bool> {
    let allowed_mode =
        ffi::FI_CONTEXT2 | ffi::FI_CONTEXT | if rma { ffi::FI_RX_CQ_DATA } else { 0 };
    if provider_version != PROVIDER_VERSION
        || protocol != ffi::FI_PROTO_EFA
        || protocol_version != 1
        || !(MIN_MTU..=CHUNK).contains(&mtu)
        || max_message < mtu
        || (!rma && max_message != mtu)
        || tx_size < SLOTS
        || rx_size < SLOTS
        || mode & ffi::FI_CONTEXT2 == 0
        || mode & !allowed_mode != 0
        || data_progress != ffi::fi_progress_FI_PROGRESS_AUTO
        || mr_mode & i32::try_from(ffi::FI_MR_LOCAL).unwrap() == 0
    {
        return Err(invalid(
            "unsupported efa-direct provider version, MTU, or required mode",
        ));
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_geometry_rejects_incompatible_protocol_modes_and_progress() {
        let context = ffi::FI_CONTEXT2;
        let automatic = ffi::fi_progress_FI_PROGRESS_AUTO;
        let check = |protocol_version, mode, progress| {
            validate(
                PROVIDER_VERSION,
                ffi::FI_PROTO_EFA,
                protocol_version,
                4096,
                false,
                SLOTS,
                SLOTS,
                mode,
                progress,
                i32::try_from(ffi::FI_MR_LOCAL).unwrap(),
                4096,
            )
        };
        assert!(check(1, context, automatic).is_ok());
        assert!(
            validate(
                PROVIDER_VERSION,
                ffi::FI_PROTO_EFA,
                1,
                65_536,
                true,
                SLOTS,
                SLOTS,
                context | ffi::FI_RX_CQ_DATA,
                automatic,
                i32::try_from(ffi::FI_MR_LOCAL).unwrap(),
                4096,
            )
            .is_ok()
        );
        assert!(check(4, context, automatic).is_err());
        assert!(check(1, 0, automatic).is_err());
        assert!(check(1, context | ffi::FI_MSG_PREFIX, automatic).is_err());
        assert!(check(1, context, ffi::fi_progress_FI_PROGRESS_MANUAL).is_err());
    }
}
