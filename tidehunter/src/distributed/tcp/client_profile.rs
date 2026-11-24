//! Client-side transport facts: the inline-export counters an owner reports,
//! and the credential snapshot a connection is opened with.

use std::sync::Arc;

use rustls::ClientConfig;
use serde::Serialize;

/// Live exports created inline by an owner request, while its caller waited,
/// by request kind. A lease pays for one only when no preparation published its
/// file first. Each count is one export creation with its target-registry syncs
/// (see `LIFECYCLE_TIMEOUT`). Exports of background preparation and of
/// `OpenLiveExports` are not counted.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct LiveInlineExportStats {
    /// `LeaseLiveExtent` (kind 33): a lease without a current extent, the first
    /// after open or after a fence.
    pub lease: u64,
    /// `AdoptLiveExtent { lease_next: true }` (kind 34): rollover preparation.
    pub rollover: u64,
    /// `PreleaseLiveExtent` (kind 36): background lease-ahead.
    pub prelease: u64,
    /// `ResolveLiveRecords` (kind 29): the first resolution of a file without
    /// a binding.
    pub resolve: u64,
    /// `ReplayLiveWal`: a history page over a file without a binding.
    pub replay: u64,
}

/// Primary setup spans exclude shared TLS parsing; fabric_open is nested.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ClientConnectProfile {
    pub connect_ns: u64,
    pub tls_ns: u64,
    /// None for TCP; Some measures fabric bootstrap, including native mode setup.
    pub fabric_ns: Option<u64>,
    /// Fresh status RPC; reuse also includes certificate/control revalidation
    /// and retirement of already-owned transport fences.
    pub status_ns: u64,
    /// Fine-grained local endpoint creation spans; None for TCP/reused lanes.
    #[cfg(feature = "experimental-fabric")]
    pub fabric_open: Option<tidehunter_fabric::OpenProfile>,
}

/// Transport config and optional TLS identity built from one credential snapshot.
#[derive(Clone)]
pub(crate) struct ClientTlsConfig {
    pub config: Option<Arc<ClientConfig>>,
}
