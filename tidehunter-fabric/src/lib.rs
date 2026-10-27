//! Bounded Rust ownership for OFI libfabric message endpoints.
//! Default builds have no native dependency. See DESIGN.md for provenance.
mod config;
pub use config::{FabricBackend, FabricConfig};
#[cfg(all(feature = "native", not(target_os = "linux")))]
compile_error!("the fabric backend requires Linux");
#[cfg(feature = "native")]
mod endpoint;
#[cfg(feature = "native")]
pub use endpoint::{
    Endpoint, OpenProfile, PeerDrainToken, ProgressStats, ProviderInfo, ReadBuffer, ReadOperation,
    RegisteredReadRegion, RemoteReadRegion, RmaAddressMode, RmaCapabilities, RmaConfig,
    RmaReadMode,
};
