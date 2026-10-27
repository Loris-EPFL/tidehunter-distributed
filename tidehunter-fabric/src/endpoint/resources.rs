//! Native resources and raw ownership of memory accessed by the provider.

#[cfg(test)]
mod nonblocking_tests;

mod efa;
mod efa_direct;

use super::{
    CHUNK, OpenProfile, PeerDrainToken, ProgressStats, ProviderInfo, RMA_CANCELLED, ReadBuffer,
    ReadBufferStorage, ReadOperation, RegisteredReadRegion, RemoteReadRegion, RmaAddressMode,
    RmaCapabilities, RmaConfig, RmaReadMode, SLOTS,
};
use crate::{FabricBackend, FabricConfig};
use ofi_libfabric_sys::bindgen as ffi;
use std::{
    any::Any,
    cell::UnsafeCell,
    ffi::{CStr, CString, c_void},
    io::{self, IoSlice},
    mem::{self, MaybeUninit},
    ptr,
    ptr::NonNull,
    sync::atomic::AtomicU64,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

pub(super) const ADDRESS: usize = 512;

struct Buffers {
    tx: UnsafeCell<[[u8; CHUNK]; SLOTS]>,
    rx: UnsafeCell<[[u8; CHUNK]; SLOTS]>,
    tx_context: UnsafeCell<[ffi::fi_context2; SLOTS]>,
    rx_context: UnsafeCell<[ffi::fi_context2; SLOTS]>,
}

/// This allocation has no Rust references while the provider can access it.
/// Metadata borrows cannot retag the independently allocated buffers/contexts.
struct Memory(NonNull<Buffers>);

impl Default for Memory {
    fn default() -> Self {
        let mut buffers = Box::<Buffers>::new_uninit();
        // SAFETY: Buffers contains only byte arrays and fi_context2 raw-pointer
        // arrays inside UnsafeCell. All-zero is valid for every field. Initialize
        // on the heap to avoid a 512 KiB temporary on a connection thread stack.
        let buffers = unsafe {
            buffers.as_mut_ptr().write_bytes(0, 1);
            buffers.assume_init()
        };
        Self(NonNull::new(Box::into_raw(buffers)).expect("Box allocation is non-null"))
    }
}

impl Memory {
    fn tx(&self, slot: usize) -> *mut u8 {
        debug_assert!(slot < SLOTS);
        // SAFETY: The allocation is live until successful resource teardown.
        // Only an UnsafeCell shared reference is formed, never a buffer slice.
        unsafe { (*self.0.as_ptr()).tx.get().cast::<u8>().add(slot * CHUNK) }
    }

    fn rx(&self, slot: usize) -> *mut u8 {
        debug_assert!(slot < SLOTS);
        // SAFETY: As above; provider writes are permitted inside UnsafeCell.
        unsafe { (*self.0.as_ptr()).rx.get().cast::<u8>().add(slot * CHUNK) }
    }

    fn tx_context(&self, slot: usize) -> *mut c_void {
        debug_assert!(slot < SLOTS);
        // SAFETY: The context has the same lifetime and stable allocation.
        unsafe {
            (*self.0.as_ptr())
                .tx_context
                .get()
                .cast::<ffi::fi_context2>()
                .add(slot)
                .cast()
        }
    }

    fn rx_context(&self, slot: usize) -> *mut c_void {
        debug_assert!(slot < SLOTS);
        // SAFETY: The context has the same lifetime and stable allocation.
        unsafe {
            (*self.0.as_ptr())
                .rx_context
                .get()
                .cast::<ffi::fi_context2>()
                .add(slot)
                .cast()
        }
    }
}

impl Drop for Memory {
    fn drop(&mut self) {
        // SAFETY: Endpoint frees Resources only after close() succeeds for all
        // handles. Failed close leaks Resources, including this unique owner.
        unsafe { drop(Box::from_raw(self.0.as_ptr())) };
    }
}

#[derive(Default)]
enum CqWaitMode {
    #[default]
    Polling,
    SoftwareBounded,
}

#[derive(Default)]
pub(super) struct Resources {
    fabric: *mut ffi::fid_fabric,
    domain: *mut ffi::fid_domain,
    ep: *mut ffi::fid_ep,
    cq: *mut ffi::fid_cq,
    av: *mut ffi::fid_av,
    tx_mr: *mut ffi::fid_mr,
    rx_mr: *mut ffi::fid_mr,
    memory: Memory,
    peer: Option<ffi::fi_addr_t>,
    addr_format: u32,
    address: Vec<u8>,
    tx_pending: [bool; SLOTS],
    tx_ordinal: [u64; SLOTS],
    accepted_send: u64,
    completed_send: u64,
    progress_stats: ProgressStats,
    provider_info: Option<ProviderInfo>,
    pub(super) open_profile: OpenProfile,
    initial_progress_recorded: bool,
    time_progress: bool,
    last_poll: Option<Instant>,
    last_any_poll: Option<Instant>,
    cq_wait_mode: CqWaitMode,
    rx_pending: [bool; SLOTS],
    rx_len: [usize; SLOTS],
    rx_offset: usize,
    rx_read: usize,
    rx_post: usize,
    direct: bool,
    rma: Option<RmaState>,
}

const GLOBAL_RMA_SOURCE_BYTES: usize = 256 * 1024 * 1024;
const GLOBAL_RMA_DESTINATION_BYTES: usize = 256 * 1024 * 1024;
const GLOBAL_RMA_REGION_SLOTS: usize = 65_536;
const GLOBAL_RMA_DESTINATION_SLOTS: usize = 8_192;
static RMA_SOURCE_BYTES: AtomicUsize = AtomicUsize::new(0);
static RMA_DESTINATION_BYTES: AtomicUsize = AtomicUsize::new(0);
static RMA_REGION_SLOTS: AtomicUsize = AtomicUsize::new(0);
static RMA_DESTINATION_SLOTS: AtomicUsize = AtomicUsize::new(0);
const GLOBAL_RMA_DESTINATION_METADATA_SLOTS: usize = 8_192;
static RMA_DESTINATION_METADATA_SLOTS: AtomicUsize = AtomicUsize::new(0);
static NEXT_RMA_OWNER_ID: AtomicU64 = AtomicU64::new(1);

struct PoolMetadataCharge {
    source_slots: usize,
    destination_slots: usize,
}

impl Drop for PoolMetadataCharge {
    fn drop(&mut self) {
        if self.source_slots != 0 {
            RmaState::release(&RMA_REGION_SLOTS, self.source_slots);
        }
        if self.destination_slots != 0 {
            RmaState::release(&RMA_DESTINATION_METADATA_SLOTS, self.destination_slots);
        }
    }
}

fn next_rma_owner_id() -> io::Result<u64> {
    NEXT_RMA_OWNER_ID
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |id| id.checked_add(1))
        .map_err(|_| invalid("RMA endpoint identities exhausted"))
}
const FIRST_DYNAMIC_MR_KEY: u64 = 3;
static NEXT_MR_KEY: AtomicU64 = AtomicU64::new(FIRST_DYNAMIC_MR_KEY);

fn next_mr_key() -> io::Result<u64> {
    NEXT_MR_KEY
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |key| {
            key.checked_add(1)
        })
        .map_err(|_| invalid("libfabric MR key space exhausted"))
}

pub(super) fn release_destination_charge(bytes: usize, slots: usize) {
    if bytes != 0 {
        RmaState::release(&RMA_DESTINATION_BYTES, bytes);
    }
    if slots != 0 {
        RmaState::release(&RMA_DESTINATION_SLOTS, slots);
    }
}

struct SourceRegion {
    id: u64,
    generation: u64,
    session_id: u64,
    grant_generation: u64,
    bytes: Option<Box<[u8]>>,
    // Keep original WAL authority/file/log ownership tied to the exact MR;
    // quarantining this endpoint therefore quarantines the source authority too.
    retained_owner: Option<Arc<dyn Any + Send + Sync>>,
    mr: *mut ffi::fid_mr,
}

struct ReadSlot {
    storage: Arc<ReadBufferStorage>,
    context: UnsafeCell<ffi::fi_context2>,
    mr: *mut ffi::fid_mr,
    generation: u64,
    pending: bool,
    complete: bool,
    completion_error: u32,
    cancel_state: Arc<AtomicU64>,
}

struct RmaState {
    owner_id: u64,
    config: RmaConfig,
    capabilities: RmaCapabilities,
    sources: Vec<SourceRegion>,
    // Fixed before any fi_context2 is posted; neither elements nor contexts move.
    destinations: Box<[ReadSlot]>,
    next_destination: usize,
    next_region: u64,
    next_generation: u64,
    _metadata: PoolMetadataCharge,
}

impl RmaState {
    fn reserve(counter: &AtomicUsize, limit: usize, amount: usize) -> bool {
        counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(amount).filter(|next| *next <= limit)
            })
            .is_ok()
    }

    fn release(counter: &AtomicUsize, amount: usize) {
        let previous = counter.fetch_sub(amount, Ordering::AcqRel);
        debug_assert!(previous >= amount);
    }
    fn validate_config(config: RmaConfig) -> io::Result<()> {
        let source_enabled = config.source_bytes > 0 && config.source_region_slots > 0;
        let source_invalid = (config.source_bytes == 0) != (config.source_region_slots == 0);
        let destination_enabled = config.destination_bytes > 0 && config.destination_slots > 0;
        let destination_invalid =
            (config.destination_bytes == 0) != (config.destination_slots == 0);
        if source_invalid
            || destination_invalid
            || (!source_enabled && !destination_enabled)
            || config.source_bytes > GLOBAL_RMA_SOURCE_BYTES
            || config.source_region_slots > GLOBAL_RMA_REGION_SLOTS
            || config.destination_bytes > GLOBAL_RMA_DESTINATION_BYTES
            || config.destination_slots > GLOBAL_RMA_DESTINATION_SLOTS
            || (destination_enabled
                && (config.destination_bytes / config.destination_slots == 0
                    || (config.max_read_bytes > 0
                        && config.destination_bytes / config.destination_slots
                            < config.max_read_bytes)))
        {
            return Err(invalid("invalid RMA resource limits"));
        }
        Ok(())
    }

    fn cleanup_destinations(destinations: &mut [ReadSlot]) -> bool {
        let mut uncertain = false;
        for slot in destinations {
            if !slot.mr.is_null() {
                // SAFETY: Construction has not posted reads. Each non-null MR
                // is uniquely owned here; uncertain close retains its storage.
                let status = unsafe { ffi::fi_close(&raw mut (*slot.mr).fid) };
                slot.mr = ptr::null_mut();
                uncertain |= status != 0;
            }
        }
        uncertain
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Keep ordered native registration and rollback ownership in one construction path."
    )]
    fn new(
        domain: *mut ffi::fid_domain,
        config: RmaConfig,
        capabilities: RmaCapabilities,
    ) -> io::Result<Self> {
        Self::validate_config(config)?;
        let destination_enabled = config.destination_slots > 0;
        if destination_enabled
            && (config.max_read_bytes == 0 || config.max_read_bytes > capabilities.max_read_bytes)
        {
            return Err(invalid("invalid negotiated RMA read limit"));
        }
        let owner_id = next_rma_owner_id()?;
        if !Self::reserve(
            &RMA_REGION_SLOTS,
            GLOBAL_RMA_REGION_SLOTS,
            config.source_region_slots,
        ) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "global RMA source metadata slots exhausted",
            ));
        }
        if !Self::reserve(
            &RMA_DESTINATION_METADATA_SLOTS,
            GLOBAL_RMA_DESTINATION_METADATA_SLOTS,
            config.destination_slots,
        ) {
            Self::release(&RMA_REGION_SLOTS, config.source_region_slots);
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "global RMA destination metadata slots exhausted",
            ));
        }
        let metadata = PoolMetadataCharge {
            source_slots: config.source_region_slots,
            destination_slots: config.destination_slots,
        };
        let mut destinations = Vec::with_capacity(config.destination_slots);
        if destination_enabled {
            let slot_bytes = config.destination_bytes / config.destination_slots;
            for _ in 0..config.destination_slots {
                let key = match next_mr_key() {
                    Ok(key) => key,
                    Err(err) => {
                        if Self::cleanup_destinations(&mut destinations) {
                            std::mem::forget(destinations);
                            std::mem::forget(metadata);
                        }
                        return Err(err);
                    }
                };
                if !Self::reserve(
                    &RMA_DESTINATION_BYTES,
                    GLOBAL_RMA_DESTINATION_BYTES,
                    slot_bytes,
                ) {
                    if Self::cleanup_destinations(&mut destinations) {
                        std::mem::forget(destinations);
                        std::mem::forget(metadata);
                    }
                    return Err(io::Error::other("global RMA destination budget exhausted"));
                }
                if !Self::reserve(&RMA_DESTINATION_SLOTS, GLOBAL_RMA_DESTINATION_SLOTS, 1) {
                    Self::release(&RMA_DESTINATION_BYTES, slot_bytes);
                    if Self::cleanup_destinations(&mut destinations) {
                        std::mem::forget(destinations);
                        std::mem::forget(metadata);
                    }
                    return Err(io::Error::other("global RMA destination slots exhausted"));
                }
                let storage = Arc::new(ReadBufferStorage {
                    bytes: (0..slot_bytes)
                        .map(|_| UnsafeCell::new(0))
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                    len: AtomicUsize::new(0),
                    charged_bytes: slot_bytes,
                    charged_slots: 1,
                });
                let mut mr = ptr::null_mut();
                // SAFETY: The Arc's boxed allocation is stable until its provider MR
                // closes after endpoint teardown and all consumers release their Arc.
                let code = unsafe {
                    ffi::fi_mr_reg(
                        domain,
                        storage.bytes.as_ptr().cast(),
                        slot_bytes,
                        u64::from(ffi::FI_READ),
                        0,
                        key,
                        0,
                        &raw mut mr,
                        ptr::null_mut(),
                    )
                };
                if code != 0 || mr.is_null() {
                    let current_uncertain =
                        !mr.is_null() && unsafe { ffi::fi_close(&raw mut (*mr).fid) } != 0;
                    if current_uncertain {
                        std::mem::forget(storage);
                    }
                    if Self::cleanup_destinations(&mut destinations) {
                        std::mem::forget(destinations);
                        std::mem::forget(metadata);
                    }
                    return Err(if code != 0 {
                        error("register RMA destination", code.unsigned_abs())
                    } else {
                        invalid("RMA destination registration returned no MR")
                    });
                }
                destinations.push(ReadSlot {
                    storage,
                    context: UnsafeCell::new(ffi::fi_context2::default()),
                    mr,
                    generation: 0,
                    pending: false,
                    complete: false,
                    completion_error: 0,
                    cancel_state: Arc::new(AtomicU64::new(0)),
                });
            }
        }
        let sources = Vec::with_capacity(config.source_region_slots);
        Ok(Self {
            owner_id,
            config,
            capabilities,
            sources,
            destinations: destinations.into_boxed_slice(),
            next_destination: 0,
            next_region: 1,
            next_generation: 1,
            _metadata: metadata,
        })
    }

    fn source_index(&self, region: &RegisteredReadRegion) -> io::Result<usize> {
        if region.owner_id != self.owner_id {
            return Err(invalid("read region belongs to another endpoint"));
        }
        self.sources
            .iter()
            .position(|source| {
                source.id == region.region_id
                    && source.generation == region.generation
                    && source.session_id == region.session_id
                    && source.grant_generation == region.grant_generation
            })
            .ok_or_else(|| invalid("read region reservation is not active"))
    }

    fn complete(&mut self, context: *mut c_void, completion_error: u32) -> io::Result<bool> {
        // Contexts live in a fixed slice. Check integer geometry before indexing;
        // never dereference an arbitrary provider-supplied context pointer.
        let Some(base) = self
            .destinations
            .as_ptr()
            .addr()
            .checked_add(std::mem::offset_of!(ReadSlot, context))
        else {
            return Ok(false);
        };
        let Some(relative) = context.addr().checked_sub(base) else {
            return Ok(false);
        };
        let stride = std::mem::size_of::<ReadSlot>();
        if relative % stride != 0 {
            return Ok(false);
        }
        let Some(slot) = self.destinations.get_mut(relative / stride) else {
            return Ok(false);
        };
        if !slot.pending {
            return Err(invalid("unexpected RMA completion context"));
        }
        slot.pending = false;
        slot.completion_error = completion_error;
        slot.complete =
            slot.cancel_state.load(Ordering::Acquire) != (slot.generation | RMA_CANCELLED);
        Ok(true)
    }

    fn validate_read_operation(&self, operation: &ReadOperation) -> io::Result<()> {
        let Some(slot) = self.destinations.get(operation.slot) else {
            return Err(invalid("unknown RMA operation slot"));
        };
        if slot.generation != operation.generation
            || !Arc::ptr_eq(&slot.cancel_state, &operation.cancelled)
        {
            return Err(invalid("RMA operation belongs to another slot or endpoint"));
        }
        Ok(())
    }

    fn has_pending_reads(&self) -> bool {
        self.destinations.iter().any(|slot| slot.pending)
    }
    fn read_slot_available(slot: &mut ReadSlot) -> bool {
        if slot.pending || Arc::get_mut(&mut slot.storage).is_none() {
            return false;
        }
        let cancelled =
            slot.cancel_state.load(Ordering::Acquire) == (slot.generation | RMA_CANCELLED);
        !slot.complete || cancelled
    }

    fn available_read_slots(&mut self) -> usize {
        self.destinations
            .iter_mut()
            .map(Self::read_slot_available)
            .filter(|available| *available)
            .count()
    }

    fn close(&mut self) -> Result<(), i32> {
        if self.sources.iter().any(|source| source.bytes.is_some()) || self.has_pending_reads() {
            return Err(i32::try_from(ffi::FI_EBUSY).unwrap());
        }
        for slot in &mut self.destinations {
            if !slot.mr.is_null() {
                // SAFETY: All accepted reads have terminal completions. The MR
                // closes before its stable storage can be released.
                let status = unsafe { ffi::fi_close(&raw mut (*slot.mr).fid) };
                if status != 0 {
                    slot.mr = ptr::null_mut();
                    return Err(status);
                }
                slot.mr = ptr::null_mut();
            }
        }
        Ok(())
    }
}

struct Info(*mut ffi::fi_info);

impl Drop for Info {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: This owner received the allocation from libfabric. Hint
            // strings use strdup, matching fi_freeinfo's C allocator.
            unsafe { ffi::fi_freeinfo(self.0) };
        }
    }
}

impl Info {
    fn hints(provider: &CStr, domain: &CStr, direct_fabric: bool, rma: bool) -> io::Result<Self> {
        // SAFETY: No arguments. Info owns the result and releases it on all paths.
        let hints = Info(unsafe { ffi::fi_allocinfo() });
        if hints.0.is_null() {
            return Err(error("allocate fabric hints", ffi::FI_ENOMEM));
        }
        let rma_caps = if rma {
            u64::from(ffi::FI_RMA | ffi::FI_READ | ffi::FI_REMOTE_READ)
        } else {
            0
        };
        // SAFETY: fi_allocinfo initializes all nested attribute allocations.
        // strdup gives ownership of matching C-allocator strings to fi_freeinfo.
        unsafe {
            (*(*hints.0).ep_attr).type_ = ffi::fi_ep_type_FI_EP_RDM;
            (*hints.0).caps = u64::from(ffi::FI_MSG)
                | rma_caps
                | if direct_fabric {
                    0
                } else {
                    ffi::FI_DIRECTED_RECV
                };
            (*hints.0).mode = if direct_fabric {
                ffi::FI_CONTEXT2 | if rma { ffi::FI_RX_CQ_DATA } else { 0 }
            } else {
                ffi::FI_CONTEXT
            };
            (*(*hints.0).domain_attr).threading = ffi::fi_threading_FI_THREAD_DOMAIN;
            (*(*hints.0).domain_attr).__bindgen_anon_1.data_progress = if direct_fabric {
                ffi::fi_progress_FI_PROGRESS_AUTO
            } else {
                ffi::fi_progress_FI_PROGRESS_MANUAL
            };
            let requested_mr_modes = if (rma && provider.to_bytes() == b"efa")
                || provider.to_bytes() == b"verbs;ofi_rxm"
            {
                ffi::FI_MR_LOCAL | ffi::FI_MR_PROV_KEY | ffi::FI_MR_VIRT_ADDR | ffi::FI_MR_ALLOCATED
            } else if direct_fabric {
                ffi::FI_MR_LOCAL | ffi::FI_MR_ALLOCATED
            } else {
                ffi::FI_MR_LOCAL
            };
            (*(*hints.0).domain_attr).mr_mode = i32::try_from(requested_mr_modes).unwrap();
            if !direct_fabric {
                (*(*hints.0).tx_attr).msg_order = u64::from(ffi::FI_ORDER_SAS);
                (*(*hints.0).rx_attr).msg_order = u64::from(ffi::FI_ORDER_SAS);
            }
            if provider.to_bytes() == b"efa" && !direct_fabric {
                (*(*hints.0).tx_attr).size = efa::QUEUE;
                (*(*hints.0).rx_attr).size = efa::QUEUE;
            } else if direct_fabric {
                (*(*hints.0).tx_attr).size = SLOTS;
                (*(*hints.0).rx_attr).size = SLOTS;
            }
            (*(*hints.0).fabric_attr).prov_name = libc::strdup(provider.as_ptr());
            if (*(*hints.0).fabric_attr).prov_name.is_null() {
                return Err(error("allocate provider name", ffi::FI_ENOMEM));
            }
            if direct_fabric {
                (*(*hints.0).fabric_attr).name = libc::strdup(c"efa-direct".as_ptr());
                if (*(*hints.0).fabric_attr).name.is_null() {
                    return Err(error("allocate direct fabric name", ffi::FI_ENOMEM));
                }
            }
            if !domain.to_bytes().is_empty() {
                (*(*hints.0).domain_attr).name = libc::strdup(domain.as_ptr());
                if (*(*hints.0).domain_attr).name.is_null() {
                    return Err(error("allocate domain name", ffi::FI_ENOMEM));
                }
            }
        }
        Ok(hints)
    }
}

fn source_address_matches(info: &ffi::fi_info, requested: std::net::IpAddr) -> bool {
    selected_source_address(info) == Some(requested)
}

fn optional_c_string(pointer: *const libc::c_char) -> Option<String> {
    if pointer.is_null() {
        None
    } else {
        // SAFETY: libfabric owns valid NUL-terminated attribute strings for
        // the lifetime of the fi_info result.
        Some(
            unsafe { CStr::from_ptr(pointer) }
                .to_string_lossy()
                .into_owned(),
        )
    }
}
fn selected_source_address(info: &ffi::fi_info) -> Option<std::net::IpAddr> {
    // SAFETY: `info` is live until this function returns, and each sockaddr
    // read is guarded by its advertised format, address length and pointer.
    unsafe {
        match info.addr_format {
            ffi::FI_SOCKADDR_IN
                if info.src_addrlen >= mem::size_of::<libc::sockaddr_in>()
                    && !info.src_addr.is_null() =>
            {
                let address = ptr::read_unaligned(info.src_addr.cast::<libc::sockaddr_in>());
                Some(std::net::IpAddr::V4(std::net::Ipv4Addr::from(
                    address.sin_addr.s_addr.to_ne_bytes(),
                )))
            }
            ffi::FI_SOCKADDR_IN6
                if info.src_addrlen >= mem::size_of::<libc::sockaddr_in6>()
                    && !info.src_addr.is_null() =>
            {
                let address = ptr::read_unaligned(info.src_addr.cast::<libc::sockaddr_in6>());
                Some(std::net::IpAddr::V6(std::net::Ipv6Addr::from(
                    address.sin6_addr.s6_addr,
                )))
            }
            ffi::FI_SOCKADDR
                if info.src_addrlen >= mem::size_of::<libc::sockaddr>()
                    && !info.src_addr.is_null() =>
            {
                let family = ptr::read_unaligned(info.src_addr.cast::<libc::sockaddr>()).sa_family;
                if i32::from(family) == libc::AF_INET {
                    if info.src_addrlen < mem::size_of::<libc::sockaddr_in>() {
                        return None;
                    }
                    let address = ptr::read_unaligned(info.src_addr.cast::<libc::sockaddr_in>());
                    Some(std::net::IpAddr::V4(std::net::Ipv4Addr::from(
                        address.sin_addr.s_addr.to_ne_bytes(),
                    )))
                } else if i32::from(family) == libc::AF_INET6 {
                    if info.src_addrlen < mem::size_of::<libc::sockaddr_in6>() {
                        return None;
                    }
                    let address = ptr::read_unaligned(info.src_addr.cast::<libc::sockaddr_in6>());
                    Some(std::net::IpAddr::V6(std::net::Ipv6Addr::from(
                        address.sin6_addr.s6_addr,
                    )))
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

fn is_registered_rdma_pci_device(bus_address: &str) -> bool {
    let Ok(devices) = std::fs::read_dir("/sys/class/infiniband") else {
        return false;
    };
    devices.filter_map(Result::ok).any(|device| {
        std::fs::canonicalize(device.path().join("device"))
            .ok()
            .and_then(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .map(str::to_owned)
            })
            .is_some_and(|name| name == bus_address)
    })
}
impl Resources {
    #[allow(
        clippy::too_many_lines,
        reason = "Keep validated provider info and partial resource ownership in one ordered setup path"
    )]
    pub(super) fn open_mode(
        &mut self,
        config: &FabricConfig,
        rma: Option<RmaConfig>,
    ) -> io::Result<()> {
        config.validate()?;
        let discovery = Instant::now();
        let provider_name = config.backend.provider();
        let provider = CString::new(provider_name).map_err(invalid)?;
        let domain = CString::new(config.domain.as_deref().unwrap_or_default()).map_err(invalid)?;
        let direct_fabric = config.backend.is_direct();
        let is_efa = matches!(
            config.backend,
            FabricBackend::Efa | FabricBackend::EfaDirect
        );
        if config.backend == FabricBackend::Efa && rma.is_some() {
            return Err(invalid(
                "regular EFA RMA is unsupported; select the explicit efa-direct backend",
            ));
        }
        self.direct = direct_fabric;
        let software_wait_candidate =
            config.backend == FabricBackend::SoftwareTcp && !direct_fabric && rma.is_none();
        if let Some(config) = rma {
            RmaState::validate_config(config)?;
        }
        if config.backend == FabricBackend::Efa {
            efa::check_environment()?;
            self.open_profile.requested_tx = efa::QUEUE;
            self.open_profile.requested_rx = efa::QUEUE;
        } else if direct_fabric {
            self.open_profile.requested_tx = SLOTS;
            self.open_profile.requested_rx = SLOTS;
        }
        let mut active_rma = rma;
        let mut hints = Info::hints(&provider, &domain, direct_fabric, active_rma.is_some())?;
        let source_v4 = config.source_address.and_then(|address| match address {
            std::net::IpAddr::V4(address) => Some(libc::sockaddr_in {
                sin_family: libc::sa_family_t::try_from(libc::AF_INET).unwrap(),
                sin_port: 0,
                sin_addr: libc::in_addr {
                    s_addr: u32::from_ne_bytes(address.octets()),
                },
                sin_zero: [0; 8],
            }),
            std::net::IpAddr::V6(_) => None,
        });
        let source_v6 = config.source_address.and_then(|address| match address {
            std::net::IpAddr::V4(_) => None,
            std::net::IpAddr::V6(address) => Some(libc::sockaddr_in6 {
                sin6_family: libc::sa_family_t::try_from(libc::AF_INET6).unwrap(),
                sin6_port: 0,
                sin6_flowinfo: 0,
                sin6_addr: libc::in6_addr {
                    s6_addr: address.octets(),
                },
                sin6_scope_id: 0,
            }),
        });
        let apply_source_hint = |hints: &Info| -> io::Result<()> {
            let (address, address_size, address_format) = if let Some(address) = &source_v4 {
                (
                    ptr::from_ref(address).cast::<u8>(),
                    mem::size_of::<libc::sockaddr_in>(),
                    ffi::FI_SOCKADDR_IN,
                )
            } else if let Some(address) = &source_v6 {
                (
                    ptr::from_ref(address).cast::<u8>(),
                    mem::size_of::<libc::sockaddr_in6>(),
                    ffi::FI_SOCKADDR_IN6,
                )
            } else {
                return Ok(());
            };
            // SAFETY: malloc returns storage for address_size bytes, checked
            // before use; ownership passes to fi_freeinfo with the live hints.
            let owned_address = unsafe { libc::malloc(address_size) };
            if owned_address.is_null() {
                return Err(error("allocate source address hint", ffi::FI_ENOMEM));
            }
            // SAFETY: The source struct is live and exactly address_size bytes.
            unsafe {
                ptr::copy_nonoverlapping(address, owned_address.cast(), address_size);
                if !(*hints.0).src_addr.is_null() {
                    libc::free((*hints.0).src_addr);
                }
                (*hints.0).addr_format = address_format;
                (*hints.0).src_addr = owned_address;
                (*hints.0).src_addrlen = address_size;
            }
            Ok(())
        };
        apply_source_hint(&hints)?;
        let api = (1 << 16)
            | if provider.as_bytes() == b"efa" {
                18
            } else {
                11
            };
        let mut info = Info(ptr::null_mut());
        // SAFETY: Hints and output storage are live through this synchronous call.
        let mut discovered =
            unsafe { ffi::fi_getinfo(api, ptr::null(), ptr::null(), 0, hints.0, &raw mut info.0) };
        if discovered != 0 && active_rma.is_some_and(|config| !config.require_device_read) {
            drop(info);
            info = Info(ptr::null_mut());
            active_rma = None;
            hints = Info::hints(&provider, &domain, direct_fabric, false)?;
            apply_source_hint(&hints)?;
            discovered = unsafe {
                ffi::fi_getinfo(api, ptr::null(), ptr::null(), 0, hints.0, &raw mut info.0)
            };
        }
        if direct_fabric && discovered != 0 {
            let code = discovered.unsigned_abs();
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "efa-direct hardware unavailable: {}",
                    error("discover direct fabric", code)
                ),
            ));
        }
        check("discover explicit fabric provider", discovered)?;
        self.open_profile.discovery_ns = elapsed_ns(discovery);
        if info.0.is_null() {
            return Err(invalid(if direct_fabric {
                "efa-direct hardware unavailable: no direct fabric info"
            } else {
                "fabric returned no provider info"
            }));
        }
        // SAFETY: Successful getinfo returns populated attribute structures.
        // Their ownership remains in Info until all handle creation completes.
        let mut selected_rma_capabilities = None;
        unsafe {
            let selected = &*info.0;
            if selected.fabric_attr.is_null()
                || selected.domain_attr.is_null()
                || selected.ep_attr.is_null()
                || selected.tx_attr.is_null()
                || selected.rx_attr.is_null()
            {
                return Err(invalid("incomplete fabric provider capabilities"));
            }
            if config
                .source_address
                .is_some_and(|source| !source_address_matches(selected, source))
            {
                return Err(invalid(
                    "provider did not return the explicitly requested source address",
                ));
            }
            if let Some(expected_domain) = config.domain.as_deref() {
                let selected_domain = (*selected.domain_attr).name;
                if selected_domain.is_null()
                    || CStr::from_ptr(selected_domain).to_bytes() != expected_domain.as_bytes()
                {
                    return Err(invalid(
                        "provider did not return the explicitly requested domain",
                    ));
                }
            }
            let provider_name = (*selected.fabric_attr).prov_name;
            let fabric_name = (*selected.fabric_attr).name;
            let mr_mode = (*selected.domain_attr).mr_mode;
            let supported_mr_modes = if direct_fabric
                || is_efa
                || provider.as_bytes() == b"verbs;ofi_rxm"
            {
                ffi::FI_MR_LOCAL | ffi::FI_MR_PROV_KEY | ffi::FI_MR_VIRT_ADDR | ffi::FI_MR_ALLOCATED
            } else {
                ffi::FI_MR_LOCAL | ffi::FI_MR_PROV_KEY | ffi::FI_MR_VIRT_ADDR
            };
            let valid_identity = if direct_fabric {
                !provider_name.is_null()
                    && CStr::from_ptr(provider_name).to_bytes() == b"efa"
                    && !fabric_name.is_null()
                    && CStr::from_ptr(fabric_name).to_bytes() == b"efa-direct"
            } else {
                !provider_name.is_null() && CStr::from_ptr(provider_name) == provider.as_c_str()
            };
            let allowed_direct_modes = ffi::FI_CONTEXT2
                | ffi::FI_CONTEXT
                | if active_rma.is_some() {
                    ffi::FI_RX_CQ_DATA
                } else {
                    0
                };
            let valid_direct_geometry =
                if direct_fabric && !selected.nic.is_null() && !(*selected.nic).link_attr.is_null()
                {
                    efa_direct::validate(
                        (*selected.fabric_attr).prov_version,
                        (*selected.ep_attr).protocol,
                        (*selected.ep_attr).protocol_version,
                        (*selected.ep_attr).max_msg_size,
                        active_rma.is_some(),
                        (*selected.tx_attr).size,
                        (*selected.rx_attr).size,
                        selected.mode,
                        (*selected.domain_attr).__bindgen_anon_1.data_progress,
                        mr_mode,
                        (*(*selected.nic).link_attr).mtu,
                    )?
                } else {
                    !direct_fabric
                };
            let rma_caps = u64::from(ffi::FI_RMA | ffi::FI_READ | ffi::FI_REMOTE_READ);
            let valid_rma = active_rma.is_none()
                || (selected.caps & rma_caps == rma_caps
                    && (*selected.tx_attr).rma_iov_limit > 0
                    && ((*selected.ep_attr).max_order_raw_size > 0
                        || (*selected.ep_attr).max_msg_size > 0));
            let valid_normal = direct_fabric
                || ((*selected.ep_attr).max_msg_size >= CHUNK
                    && selected.caps & (u64::from(ffi::FI_MSG) | ffi::FI_DIRECTED_RECV)
                        == (u64::from(ffi::FI_MSG) | ffi::FI_DIRECTED_RECV)
                    && (*selected.tx_attr).msg_order & u64::from(ffi::FI_ORDER_SAS) != 0
                    && (*selected.rx_attr).msg_order & u64::from(ffi::FI_ORDER_SAS) != 0
                    && selected.mode & !ffi::FI_CONTEXT == 0);
            if !valid_identity
                || !valid_direct_geometry
                || !valid_normal
                || !valid_rma
                || (*selected.ep_attr).type_ != ffi::fi_ep_type_FI_EP_RDM
                || selected.caps & u64::from(ffi::FI_MSG) == 0
                || (*selected.tx_attr).iov_limit == 0
                || (*selected.rx_attr).iov_limit == 0
                || !matches!(
                    (*selected.domain_attr).threading,
                    ffi::fi_threading_FI_THREAD_SAFE | ffi::fi_threading_FI_THREAD_DOMAIN
                )
                || (*selected.tx_attr).size < SLOTS
                || (*selected.rx_attr).size < SLOTS
                || mr_mode & !i32::try_from(supported_mr_modes).unwrap() != 0
                || (direct_fabric && mr_mode & i32::try_from(ffi::FI_MR_LOCAL).unwrap() == 0)
                || (direct_fabric
                    && ((*selected.tx_attr).mode | (*selected.rx_attr).mode)
                        & !allowed_direct_modes
                        != 0)
            {
                return Err(invalid(if direct_fabric {
                    "unsupported efa-direct fabric capabilities"
                } else {
                    "unsupported fabric provider capabilities"
                }));
            }
            self.open_profile.selected_tx = (*selected.tx_attr).size;
            self.open_profile.selected_rx = (*selected.rx_attr).size;
            self.open_profile.provider_version = (*selected.fabric_attr).prov_version;
            if !selected.nic.is_null() && !(*selected.nic).link_attr.is_null() {
                self.open_profile.mtu = (*(*selected.nic).link_attr).mtu;
            }
            if is_efa && !direct_fabric {
                efa::validate(
                    self.open_profile,
                    (*selected.ep_attr).protocol,
                    (*selected.ep_attr).protocol_version,
                )?;
            }
            if let Some(rma_config) = active_rma {
                let provider_limit = if (*selected.ep_attr).max_order_raw_size > 0 {
                    (*selected.ep_attr).max_order_raw_size
                } else {
                    (*selected.ep_attr).max_msg_size
                };
                let requested_read_limit = if rma_config.max_read_bytes == 0 {
                    provider_limit
                } else {
                    rma_config.max_read_bytes
                };
                let destination_limit = if rma_config.destination_slots == 0 {
                    usize::MAX
                } else {
                    rma_config.destination_bytes / rma_config.destination_slots
                };
                let max_read_bytes = requested_read_limit
                    .min(provider_limit)
                    .min(destination_limit);
                if max_read_bytes == 0 && rma_config.destination_slots > 0 {
                    return Err(invalid("provider selected no usable RMA read size"));
                }
                selected_rma_capabilities = Some(RmaCapabilities {
                    address_mode: if mr_mode & i32::try_from(ffi::FI_MR_VIRT_ADDR).unwrap() != 0 {
                        RmaAddressMode::Virtual
                    } else {
                        RmaAddressMode::Offset
                    },
                    read_mode: match config.backend {
                        FabricBackend::SoftwareTcp => RmaReadMode::Emulated,
                        FabricBackend::EfaDirect | FabricBackend::Verbs | FabricBackend::Efa => {
                            RmaReadMode::Unknown
                        }
                    },
                    max_read_bytes,
                    max_iov: (*selected.tx_attr).rma_iov_limit,
                });
            }
            self.addr_format = selected.addr_format;
            if !matches!(
                self.addr_format,
                ffi::FI_ADDR_EFA | ffi::FI_SOCKADDR | ffi::FI_SOCKADDR_IN | ffi::FI_SOCKADDR_IN6
            ) {
                return Err(invalid("unsupported fabric address format"));
            }
            let stage = Instant::now();
            check(
                "open fabric",
                ffi::fi_fabric(selected.fabric_attr, &raw mut self.fabric, ptr::null_mut()),
            )?;
            self.open_profile.fabric_ns = elapsed_ns(stage);
            let stage = Instant::now();
            check(
                "open fabric domain",
                ffi::fi_domain(self.fabric, info.0, &raw mut self.domain, ptr::null_mut()),
            )?;
            self.open_profile.domain_ns = elapsed_ns(stage);
            self.open_endpoint(
                info.0,
                active_rma.map_or(0, |config| config.destination_slots),
                software_wait_candidate,
            )?;
            let stage = Instant::now();
            if let (Some(rma_config), Some(mut capabilities)) =
                (active_rma, selected_rma_capabilities)
            {
                if rma_config.destination_slots > 0 {
                    let mut provider_max = 0_usize;
                    let mut option_len = mem::size_of::<usize>();
                    // SAFETY: The selected endpoint and query buffers are valid.
                    let option_status = ffi::fi_getopt(
                        &raw mut (*self.ep).fid,
                        i32::try_from(ffi::FI_OPT_ENDPOINT).unwrap(),
                        i32::try_from(ffi::FI_OPT_MAX_RMA_SIZE).unwrap(),
                        (&raw mut provider_max).cast(),
                        &raw mut option_len,
                    );
                    if option_status == 0
                        && option_len == mem::size_of::<usize>()
                        && provider_max > 0
                    {
                        capabilities.max_read_bytes = capabilities.max_read_bytes.min(provider_max);
                    }
                }
                if is_efa {
                    let mut emulated = true;
                    let mut option_len = mem::size_of::<bool>();
                    // SAFETY: EFA defines this endpoint option as a C bool and
                    // synchronously writes its native/emulated-read selection.
                    let option_status = ffi::fi_getopt(
                        &raw mut (*self.ep).fid,
                        i32::try_from(ffi::FI_OPT_ENDPOINT).unwrap(),
                        ffi::FI_OPT_EFA_EMULATED_READ,
                        (&raw mut emulated).cast(),
                        &raw mut option_len,
                    );
                    if option_status == 0 && option_len == mem::size_of::<bool>() {
                        capabilities.read_mode = if emulated {
                            RmaReadMode::Emulated
                        } else {
                            RmaReadMode::Native
                        };
                    }
                } else if config.backend == FabricBackend::Verbs {
                    let nic = selected.nic;
                    if !nic.is_null() && !(*nic).device_attr.is_null() && !(*nic).bus_attr.is_null()
                    {
                        let device = &*(*nic).device_attr;
                        let bus = &*(*nic).bus_attr;
                        let driver = if device.driver.is_null() {
                            String::new()
                        } else {
                            CStr::from_ptr(device.driver)
                                .to_string_lossy()
                                .to_ascii_lowercase()
                        };
                        if bus.bus_type == ffi::fi_bus_type_FI_BUS_PCI
                            && !device.name.is_null()
                            && !CStr::from_ptr(device.name).to_bytes().is_empty()
                            && !device.device_id.is_null()
                            && !CStr::from_ptr(device.device_id).to_bytes().is_empty()
                            && !driver.is_empty()
                            && driver != "rxe"
                            && driver != "rdma_rxe"
                            && driver != "siw"
                        {
                            let pci = bus.attr.pci;
                            let bus_address = format!(
                                "{:04x}:{:02x}:{:02x}.{}",
                                pci.domain_id, pci.bus_id, pci.device_id, pci.function_id
                            );
                            // `verbs;ofi_rxm` reports FI_READ only when its core
                            // provider supports RMA; require this exact PCI
                            // function to be registered as an RDMA device too.
                            if is_registered_rdma_pci_device(&bus_address) {
                                capabilities.read_mode = RmaReadMode::Native;
                            }
                        }
                    }
                }
                if rma_config.require_device_read && capabilities.read_mode != RmaReadMode::Native {
                    return Err(invalid(
                        "selected provider/device cannot attest native hardware RMA reads",
                    ));
                }
                let mut rma_config = rma_config;
                rma_config.max_read_bytes = capabilities.max_read_bytes;
                self.rma = Some(RmaState::new(self.domain, rma_config, capabilities)?);
            }
            if mr_mode & i32::try_from(ffi::FI_MR_LOCAL).unwrap() != 0 {
                check(
                    "register transmit buffer",
                    ffi::fi_mr_reg(
                        self.domain,
                        self.memory.tx(0).cast(),
                        CHUNK * SLOTS,
                        u64::from(ffi::FI_SEND),
                        0,
                        1,
                        0,
                        &raw mut self.tx_mr,
                        ptr::null_mut(),
                    ),
                )?;
                check(
                    "register receive buffer",
                    ffi::fi_mr_reg(
                        self.domain,
                        self.memory.rx(0).cast(),
                        CHUNK * SLOTS,
                        u64::from(ffi::FI_RECV),
                        0,
                        2,
                        0,
                        &raw mut self.rx_mr,
                        ptr::null_mut(),
                    ),
                )?;
            }
            self.open_profile.register_ns = elapsed_ns(stage);
            let stage = Instant::now();
            let mut address = AlignedAddress::default();
            let mut len = ADDRESS;
            check(
                "read fabric address",
                ffi::fi_getname(
                    &raw mut (*self.ep).fid,
                    address.bytes.as_mut_ptr().cast(),
                    &raw mut len,
                ),
            )?;
            if len == 0 || len > ADDRESS {
                return Err(invalid("invalid fabric address length"));
            }
            // Validate our own address before trusting its length for peers.
            AlignedAddress::new(self.addr_format, len, &address.bytes[..len])?;
            self.address = address.bytes[..len].to_vec();
            self.open_profile.address_ns = elapsed_ns(stage);
            let (device, driver, bus_address) =
                if !selected.nic.is_null() && !(*selected.nic).device_attr.is_null() {
                    let attributes = &*(*selected.nic).device_attr;
                    let bus_address = if !(*selected.nic).bus_attr.is_null()
                        && (*(*selected.nic).bus_attr).bus_type == ffi::fi_bus_type_FI_BUS_PCI
                    {
                        let pci = (*(*selected.nic).bus_attr).attr.pci;
                        Some(format!(
                            "{:04x}:{:02x}:{:02x}.{}",
                            pci.domain_id, pci.bus_id, pci.device_id, pci.function_id
                        ))
                    } else {
                        None
                    };
                    (
                        optional_c_string(attributes.name),
                        optional_c_string(attributes.driver),
                        bus_address,
                    )
                } else {
                    (None, None, None)
                };
            let rma_capabilities = self.rma.as_ref().map(|rma| rma.capabilities);
            let capability_evidence = match (config.backend, rma_capabilities) {
                (_, None) => "libfabric fi_getinfo provider/domain selection".to_owned(),
                (FabricBackend::SoftwareTcp, Some(_)) => {
                    "libfabric FI_READ capability; software provider, no hardware evidence".to_owned()
                }
                (FabricBackend::Efa | FabricBackend::EfaDirect, Some(capabilities))
                    if capabilities.read_mode == RmaReadMode::Native =>
                {
                    "EFA FI_OPT_EFA_EMULATED_READ returned false".to_owned()
                }
                (FabricBackend::Efa | FabricBackend::EfaDirect, Some(capabilities))
                    if capabilities.read_mode == RmaReadMode::Emulated =>
                {
                    "EFA FI_OPT_EFA_EMULATED_READ returned true".to_owned()
                }
                (FabricBackend::Verbs, Some(capabilities))
                    if capabilities.read_mode == RmaReadMode::Native =>
                {
                    "verbs;ofi_rxm FI_READ with selected PCI BDF matched to /sys/class/infiniband; Falcon identity not inferred"
                        .to_owned()
                }
                (_, Some(_)) => "FI_READ capability selected; native hardware read unverified".to_owned(),
            };
            self.provider_info = Some(ProviderInfo {
                backend: config.backend,
                provider: optional_c_string(provider_name).unwrap_or_default(),
                fabric: optional_c_string(fabric_name),
                domain: optional_c_string((*selected.domain_attr).name),
                source_address: selected_source_address(selected),
                device,
                driver,
                bus_address,
                rma_capabilities,
                capability_evidence,
            });
        }
        Ok(())
    }

    fn open_endpoint(
        &mut self,
        info: *mut ffi::fi_info,
        rma_slots: usize,
        allow_wait: bool,
    ) -> io::Result<()> {
        let cq_size = (2 * SLOTS)
            .checked_add(rma_slots)
            .ok_or_else(|| invalid("RMA completion queue size overflow"))?;
        let mut cq = ffi::fi_cq_attr {
            size: cq_size,
            format: ffi::fi_cq_format_FI_CQ_FORMAT_MSG,
            wait_obj: if allow_wait {
                ffi::fi_wait_obj_FI_WAIT_UNSPEC
            } else {
                ffi::fi_wait_obj_FI_WAIT_NONE
            },
            ..ffi::fi_cq_attr::default()
        };
        let mut av = ffi::fi_av_attr {
            type_: ffi::fi_av_type_FI_AV_TABLE,
            count: 1,
            ..ffi::fi_av_attr::default()
        };
        // SAFETY: Called only with our live domain and getinfo allocation.
        // Every acquired handle is saved before the next fallible operation.
        unsafe {
            let stage = Instant::now();
            let mut code =
                ffi::fi_cq_open(self.domain, &raw mut cq, &raw mut self.cq, ptr::null_mut());
            if allow_wait
                && (code == -i32::try_from(ffi::FI_ENOSYS).unwrap()
                    || code == -i32::try_from(ffi::FI_EOPNOTSUPP).unwrap())
            {
                cq.wait_obj = ffi::fi_wait_obj_FI_WAIT_NONE;
                self.cq = ptr::null_mut();
                code = ffi::fi_cq_open(self.domain, &raw mut cq, &raw mut self.cq, ptr::null_mut());
            } else if code == 0 && allow_wait {
                self.cq_wait_mode = CqWaitMode::SoftwareBounded;
            }
            check("open completion queue", code)?;
            self.open_profile.cq_ns = elapsed_ns(stage);
            let stage = Instant::now();
            check(
                "open address vector",
                ffi::fi_av_open(self.domain, &raw mut av, &raw mut self.av, ptr::null_mut()),
            )?;
            self.open_profile.av_ns = elapsed_ns(stage);
            let stage = Instant::now();
            check(
                "open endpoint",
                ffi::fi_endpoint(self.domain, info, &raw mut self.ep, ptr::null_mut()),
            )?;
            self.open_profile.endpoint_ns = elapsed_ns(stage);
            let stage = Instant::now();
            check(
                "bind completion queue",
                ffi::fi_ep_bind(
                    self.ep,
                    &raw mut (*self.cq).fid,
                    u64::from(ffi::FI_TRANSMIT | ffi::FI_RECV),
                ),
            )?;
            check(
                "bind address vector",
                ffi::fi_ep_bind(self.ep, &raw mut (*self.av).fid, 0),
            )?;
            check("enable endpoint", ffi::fi_enable(self.ep))?;
            self.open_profile.bind_enable_ns = elapsed_ns(stage);
        }
        Ok(())
    }

    pub(super) fn address(&self) -> &[u8] {
        &self.address
    }
    pub(super) fn provider_info(&self) -> Option<&ProviderInfo> {
        self.provider_info.as_ref()
    }

    pub(super) fn has_peer(&self) -> bool {
        self.peer.is_some()
    }

    pub(super) fn set_peer(&mut self, bytes: &[u8]) -> io::Result<()> {
        let stage = Instant::now();
        if self.peer.is_some() {
            return Err(invalid("fabric peer already installed"));
        }
        let address = AlignedAddress::new(self.addr_format, self.address.len(), bytes)?;
        let mut peer = 0;
        // SAFETY: The synchronous insertion reads one validated, aligned native
        // address. It copies that address into our exclusively owned AV.
        let count = unsafe {
            ffi::fi_av_insert(
                self.av,
                address.bytes.as_ptr().cast(),
                1,
                &raw mut peer,
                0,
                ptr::null_mut(),
            )
        };
        if count != 1 {
            return Err(error(
                "insert fabric peer",
                if count < 0 {
                    count.unsigned_abs()
                } else {
                    ffi::FI_EIO
                },
            ));
        }
        self.peer = Some(peer);
        self.fill_receives()?;
        if !self.rx_pending[0] {
            return Err(error("post initial fabric receive", ffi::FI_EAGAIN));
        }
        self.open_profile.peer_setup_ns = elapsed_ns(stage);
        Ok(())
    }
    pub(super) fn rma_capabilities(&self) -> Option<RmaCapabilities> {
        self.rma.as_ref().map(|rma| rma.capabilities)
    }
    pub(super) fn validate_read_operation(&self, operation: &ReadOperation) -> io::Result<()> {
        self.rma
            .as_ref()
            .ok_or_else(|| invalid("endpoint was not opened for RMA"))?
            .validate_read_operation(operation)
    }
    pub(super) fn available_read_slots(&mut self) -> io::Result<usize> {
        self.rma
            .as_mut()
            .map(RmaState::available_read_slots)
            .ok_or_else(|| invalid("endpoint was not opened for RMA"))
    }

    pub(super) fn has_live_read_regions(&self) -> bool {
        self.rma
            .as_ref()
            .is_some_and(|rma| rma.sources.iter().any(|region| region.bytes.is_some()))
    }

    pub(super) fn reserve_read_region(
        &mut self,
        session_id: u64,
        grant_generation: u64,
    ) -> io::Result<RegisteredReadRegion> {
        if self.peer.is_none() || session_id == 0 || grant_generation == 0 {
            return Err(invalid(
                "read region requires a bound peer and grant identity",
            ));
        }
        let rma = self
            .rma
            .as_mut()
            .ok_or_else(|| invalid("endpoint was not opened for RMA"))?;
        if rma.sources.len() >= rma.config.source_region_slots {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "RMA source region slots exhausted",
            ));
        }
        let id = rma.next_region;
        let generation = rma.next_generation;
        let next_id = id
            .checked_add(1)
            .ok_or_else(|| invalid("RMA region identifiers exhausted"))?;
        let next_generation = generation
            .checked_add(1)
            .ok_or_else(|| invalid("RMA region generations exhausted"))?;
        rma.next_region = next_id;
        rma.next_generation = next_generation;
        rma.sources.push(SourceRegion {
            id,
            generation,
            session_id,
            grant_generation,
            bytes: None,
            retained_owner: None,
            mr: ptr::null_mut(),
        });
        Ok(RegisteredReadRegion {
            owner_id: rma.owner_id,
            region_id: id,
            generation,
            session_id,
            grant_generation,
        })
    }

    pub(super) fn register_read_region(
        &mut self,
        region: &RegisteredReadRegion,
        bytes: Vec<u8>,
    ) -> io::Result<RemoteReadRegion> {
        self.register_read_region_with_owner(region, bytes, None)
    }

    pub(super) fn register_read_region_with_owner(
        &mut self,
        region: &RegisteredReadRegion,
        bytes: Vec<u8>,
        owner: Option<Arc<dyn Any + Send + Sync>>,
    ) -> io::Result<RemoteReadRegion> {
        if self.peer.is_none() {
            return Err(invalid("read region requires a bound peer"));
        }
        let rma = self
            .rma
            .as_mut()
            .ok_or_else(|| invalid("endpoint was not opened for RMA"))?;
        let source_index = rma.source_index(region)?;
        let source = &rma.sources[source_index];
        if source.bytes.is_some() || !source.mr.is_null() {
            return Err(invalid("read region reservation is already registered"));
        }
        let len = bytes.len();
        let resident = rma.sources.iter().try_fold(0_usize, |sum, source| {
            sum.checked_add(source.bytes.as_ref().map_or(0, |bytes| bytes.len()))
        });
        if len == 0
            || len > rma.config.source_bytes
            || resident.is_none_or(|used| {
                used.checked_add(len)
                    .is_none_or(|total| total > rma.config.source_bytes)
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "RMA source region byte budget exhausted",
            ));
        }
        let key = next_mr_key()?;
        if !RmaState::reserve(&RMA_SOURCE_BYTES, GLOBAL_RMA_SOURCE_BYTES, len) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "global RMA source byte budget exhausted",
            ));
        }
        let bytes = bytes.into_boxed_slice();
        let remote_address = match rma.capabilities.address_mode {
            RmaAddressMode::Virtual => {
                let Ok(address) = u64::try_from(bytes.as_ptr() as usize) else {
                    RmaState::release(&RMA_SOURCE_BYTES, len);
                    return Err(invalid("RMA source address does not fit provider format"));
                };
                address
            }
            RmaAddressMode::Offset => 0,
        };
        let Ok(remote_len) = u64::try_from(len) else {
            RmaState::release(&RMA_SOURCE_BYTES, len);
            return Err(invalid("RMA source length overflow"));
        };
        let mut mr = ptr::null_mut();
        // SAFETY: The boxed immutable source never moves while its MR is live.
        // Remote peers receive read-only permission; no remote writes are enabled.
        let status = unsafe {
            ffi::fi_mr_reg(
                self.domain,
                bytes.as_ptr().cast(),
                len,
                u64::from(ffi::FI_REMOTE_READ),
                0,
                key,
                0,
                &raw mut mr,
                ptr::null_mut(),
            )
        };
        if status != 0 || mr.is_null() {
            let registration_error = if status != 0 {
                error("register read-only source region", status.unsigned_abs())
            } else {
                invalid("RMA source registration returned no MR")
            };
            let mut retained = false;
            if !mr.is_null() {
                // SAFETY: Registration returned this owned MR, but its descriptor
                // has not escaped. Failed close quarantines the source allocation.
                let close_status = unsafe { ffi::fi_close(&raw mut (*mr).fid) };
                if close_status != 0 {
                    let source = &mut rma.sources[source_index];
                    source.bytes = Some(bytes);
                    source.retained_owner = owner;
                    source.mr = ptr::null_mut();
                    retained = true;
                }
            }
            if !retained {
                RmaState::release(&RMA_SOURCE_BYTES, len);
            }
            return Err(registration_error);
        }
        rma.sources[source_index].bytes = Some(bytes);
        rma.sources[source_index].retained_owner = owner;
        rma.sources[source_index].mr = mr;
        Ok(RemoteReadRegion {
            region_id: region.region_id,
            generation: region.generation,
            remote_address,
            // SAFETY: Successful registration returned this live read-only MR.
            key: unsafe { ffi::fi_mr_key(mr) },
            len: remote_len,
        })
    }

    pub(super) fn retire_read_region(
        &mut self,
        region: RegisteredReadRegion,
        token: PeerDrainToken,
    ) -> io::Result<()> {
        if region.session_id != token.session_id
            || region.grant_generation != token.grant_generation
        {
            return Err(invalid("read region drain token identity mismatch"));
        }
        self.retire_source_region(region)
    }
    /// Reclaims a region only when its descriptor was never exposed to a peer.
    ///
    /// # Safety
    /// The caller must prove that neither the region descriptor nor any
    /// equivalent address/key/length was ever published to the remote peer.
    /// Otherwise a remote `FI_READ` may still access these bytes.
    pub(super) unsafe fn retire_unpublished_read_region(
        &mut self,
        region: RegisteredReadRegion,
    ) -> io::Result<()> {
        self.retire_source_region(region)
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "Retirement consumes the one-shot region capability, including on failure."
    )]
    fn retire_source_region(&mut self, region: RegisteredReadRegion) -> io::Result<()> {
        let rma = self
            .rma
            .as_mut()
            .ok_or_else(|| invalid("endpoint was not opened for RMA"))?;
        let index = rma.source_index(&region)?;
        let source = &mut rma.sources[index];
        if source.bytes.is_none() {
            if !source.mr.is_null() {
                return Err(invalid(
                    "unregistered source reservation unexpectedly has an MR",
                ));
            }
            rma.sources.swap_remove(index);
            return Ok(());
        }
        if source.mr.is_null() {
            return Err(io::Error::other(
                "source registration is quarantined after uncertain MR close",
            ));
        }
        // SAFETY: The caller proved either exact-peer terminal drain or that no
        // source descriptor escaped. The MR remains uniquely owned here.
        let status = unsafe { ffi::fi_close(&raw mut (*source.mr).fid) };
        if status != 0 {
            // efa_mr_close may free its MR wrapper even when deregistration
            // fails. Never retry this FID; keep the allocation/domain quarantined.
            source.mr = ptr::null_mut();
            return Err(error(
                "retire read-only source region",
                status.unsigned_abs(),
            ));
        }
        let source = rma.sources.swap_remove(index);
        RmaState::release(
            &RMA_SOURCE_BYTES,
            source.bytes.as_ref().map_or(0, |bytes| bytes.len()),
        );
        Ok(())
    }

    pub(super) fn try_post_read(
        &mut self,
        remote: &RemoteReadRegion,
        offset: usize,
        len: usize,
    ) -> io::Result<ReadOperation> {
        let peer = self.peer.ok_or_else(|| invalid("missing fabric peer"))?;
        let rma = self
            .rma
            .as_mut()
            .ok_or_else(|| invalid("endpoint was not opened for RMA"))?;
        let end = offset
            .checked_add(len)
            .ok_or_else(|| invalid("RMA read range overflow"))?;
        let remote_len = usize::try_from(remote.len)
            .map_err(|_| invalid("remote RMA region length does not fit local size"))?;
        if remote.region_id == 0
            || remote.generation == 0
            || len == 0
            || len > rma.config.max_read_bytes
            || end > remote_len
        {
            return Err(invalid("RMA read range outside negotiated limits"));
        }
        let offset_u64 = u64::try_from(offset).map_err(|_| invalid("RMA offset overflow"))?;
        let transfer_len = u64::try_from(len).map_err(|_| invalid("RMA read length overflow"))?;
        // The peer publishes its own address convention: zero for an MR-relative
        // window, or its virtual base. Local MR mode does not describe the peer.
        let remote_addr = remote
            .remote_address
            .checked_add(offset_u64)
            .ok_or_else(|| invalid("remote RMA address overflow"))?;
        remote_addr
            .checked_add(transfer_len)
            .ok_or_else(|| invalid("remote RMA address range overflow"))?;
        let slots = rma.destinations.len();
        let first_slot = rma.next_destination;
        let slot_index = (0..slots)
            .map(|distance| (first_slot + distance) % slots)
            .find(|&index| RmaState::read_slot_available(&mut rma.destinations[index]));
        let slot_index = slot_index.ok_or_else(|| {
            io::Error::new(io::ErrorKind::WouldBlock, "RMA destination pool exhausted")
        })?;
        let slot = &mut rma.destinations[slot_index];
        if slot.complete {
            slot.complete = false;
            slot.completion_error = 0;
        }
        let generation = slot
            .generation
            .checked_add(1)
            .filter(|generation| *generation < RMA_CANCELLED)
            .ok_or_else(|| invalid("RMA operation generations exhausted"))?;
        let desc = if slot.mr.is_null() {
            ptr::null_mut()
        } else {
            // SAFETY: This live destination MR is retained through CQ completion
            // and final consumer release; no deregistration can race this call.
            unsafe { ffi::fi_mr_desc(slot.mr) }
        };
        // SAFETY: Range and unique destination ownership were checked above.
        // The registered allocation and stable context survive terminal CQ.
        let code = unsafe {
            ffi::fi_read(
                self.ep,
                slot.storage.bytes.as_ptr().cast_mut().cast(),
                len,
                desc,
                peer,
                remote_addr,
                remote.key,
                slot.context.get().cast(),
            )
        };
        if !posted("post RMA read", code)? {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        slot.storage.len.store(len, Ordering::Release);
        slot.generation = generation;
        slot.cancel_state.store(generation, Ordering::Release);
        slot.pending = true;
        slot.complete = false;
        slot.completion_error = 0;
        let cancelled = Arc::clone(&slot.cancel_state);
        rma.next_destination = (slot_index + 1) % slots;
        Ok(ReadOperation {
            slot: slot_index,
            generation,
            cancelled,
            finished: false,
        })
    }

    pub(super) fn take_read_completion(
        &mut self,
        operation: &mut ReadOperation,
    ) -> io::Result<Option<io::Result<ReadBuffer>>> {
        let Some(rma) = self.rma.as_mut() else {
            return Err(invalid("endpoint was not opened for RMA"));
        };
        rma.validate_read_operation(operation)?;
        let slot = &mut rma.destinations[operation.slot];
        if slot.pending || !slot.complete {
            return Ok(None);
        }
        slot.complete = false;
        operation.finished = true;
        if slot.completion_error != 0 {
            let code = std::mem::replace(&mut slot.completion_error, 0);
            return Ok(Some(Err(error("RMA read completion", code))));
        }
        Ok(Some(Ok(ReadBuffer {
            storage: Arc::clone(&slot.storage),
        })))
    }

    pub(super) fn free_send_slot(&self) -> Option<usize> {
        if self.direct {
            self.tx_pending[..SLOTS - 1]
                .iter()
                .position(|pending| !pending)
        } else {
            self.tx_pending.iter().position(|pending| !pending)
        }
    }
    pub(super) fn sends_pending(&self) -> bool {
        self.tx_pending.iter().any(|pending| *pending)
    }

    pub(super) fn send_fences(&self) -> (u64, u64) {
        (self.accepted_send, self.completed_send)
    }

    pub(super) fn supports_idle_wait(&self) -> bool {
        matches!(self.cq_wait_mode, CqWaitMode::SoftwareBounded)
    }

    pub(super) fn progress_stats(&self) -> ProgressStats {
        self.progress_stats
    }
    pub(super) fn record_direct_data_post(&mut self, piggyback_advanced: bool) {
        self.progress_stats.efa_direct_data_posts =
            self.progress_stats.efa_direct_data_posts.saturating_add(1);
        if piggyback_advanced {
            self.progress_stats.efa_direct_credit_piggyback_advances = self
                .progress_stats
                .efa_direct_credit_piggyback_advances
                .saturating_add(1);
        }
    }

    pub(super) fn record_direct_credit_post(&mut self) {
        self.progress_stats.efa_direct_credit_posts = self
            .progress_stats
            .efa_direct_credit_posts
            .saturating_add(1);
    }

    pub(super) fn configure_progress_stats(&mut self, enabled: bool, reset: bool) {
        self.time_progress = enabled;
        self.last_poll = None;
        self.last_any_poll = None;
        if reset {
            self.progress_stats = ProgressStats::default();
        }
    }

    #[cfg(test)]
    pub(super) fn enable_test_packet_adapter(&mut self) {
        self.direct = true;
    }

    pub(super) fn free_control_slot(&self) -> Option<usize> {
        self.direct
            .then_some(SLOTS - 1)
            .filter(|&slot| !self.tx_pending[slot])
    }

    /// Direct framing includes its payload copy and checksum in the existing
    /// opt-in staging clock. No clock is read when profiling is disabled.
    pub(super) fn prepare_direct_send(
        &mut self,
        slot: usize,
        encode: impl FnOnce(&mut [u8]) -> io::Result<usize>,
    ) -> io::Result<usize> {
        let began = self.time_progress.then(Instant::now);
        let len = encode(self.send_buffer(slot))?;
        self.record_send_copy(began, len);
        Ok(len)
    }

    /// Mutable access to a free registered TX slot for protocol framing.
    /// The returned bytes must remain unchanged after a successful post until
    /// the corresponding CQ completion retires this physical slot.
    fn send_buffer(&mut self, slot: usize) -> &mut [u8] {
        assert!(slot < SLOTS && !self.tx_pending[slot]);
        // SAFETY: The slot is not provider-owned or otherwise borrowed; the
        // mutable Resources borrow prevents another safe accessor from aliasing it.
        unsafe { std::slice::from_raw_parts_mut(self.memory.tx(slot), CHUNK) }
    }
    pub(super) fn copy_send(&mut self, slot: usize, bytes: &[IoSlice<'_>]) -> usize {
        assert!(slot < SLOTS && !self.tx_pending[slot]);
        let began = self.time_progress.then(Instant::now);
        let mut offset = 0;
        for source in bytes {
            let len = source.len().min(CHUNK - offset);
            // SAFETY: This slot is ours, has CHUNK bytes, and has no pending
            // operation. Other slots may be in use; the copy never overlaps them.
            unsafe {
                ptr::copy_nonoverlapping(source.as_ptr(), self.memory.tx(slot).add(offset), len);
            }
            offset += len;
            if offset == CHUNK {
                break;
            }
        }
        self.record_send_copy(began, offset);
        offset
    }

    fn record_send_copy(&mut self, began: Option<Instant>, len: usize) {
        if let Some(began) = began {
            self.progress_stats.send_copy_ns = self
                .progress_stats
                .send_copy_ns
                .saturating_add(elapsed_ns(began));
            self.progress_stats.send_copy_calls =
                self.progress_stats.send_copy_calls.saturating_add(1);
            self.progress_stats.send_copy_bytes = self
                .progress_stats
                .send_copy_bytes
                .saturating_add(len as u64);
        }
    }

    pub(super) fn receive_ready(&self) -> bool {
        let slot = self.rx_read;
        !self.rx_pending[slot] && self.rx_len[slot] > self.rx_offset
    }

    pub(super) fn copy_receive(&mut self, bytes: &mut [u8]) -> Option<usize> {
        let slot = self.rx_read;
        if self.rx_pending[slot] || self.rx_len[slot] == 0 {
            return None;
        }
        let len = bytes.len().min(self.rx_len[slot] - self.rx_offset);
        let began = self.time_progress.then(Instant::now);
        // SAFETY: A validated completion set this slot's length <= CHUNK and
        // cleared pending. Repost waits until every byte in this slot is copied.
        unsafe {
            ptr::copy_nonoverlapping(
                self.memory.rx(slot).add(self.rx_offset),
                bytes.as_mut_ptr(),
                len,
            );
        }
        if let Some(began) = began {
            self.progress_stats.receive_copy_ns = self
                .progress_stats
                .receive_copy_ns
                .saturating_add(elapsed_ns(began));
            self.progress_stats.receive_copy_calls =
                self.progress_stats.receive_copy_calls.saturating_add(1);
            self.progress_stats.receive_copy_bytes = self
                .progress_stats
                .receive_copy_bytes
                .saturating_add(len as u64);
        }
        self.rx_offset += len;
        if self.rx_offset == self.rx_len[slot] {
            self.rx_len[slot] = 0;
            self.rx_offset = 0;
            self.rx_read = (slot + 1) % SLOTS;
        }
        Some(len)
    }

    pub(super) fn post_send(&mut self, slot: usize, len: usize) -> io::Result<bool> {
        assert!(slot < SLOTS && !self.tx_pending[slot] && len <= CHUNK);
        let peer = self.peer.ok_or_else(|| invalid("missing fabric peer"))?;
        // Reserve the ordinal before posting: an overflow must accept no bytes.
        let ordinal = self
            .accepted_send
            .checked_add(1)
            .ok_or_else(|| invalid("fabric send fence exhausted"))?;
        let began = self.time_progress.then(Instant::now);
        // SAFETY: Callers first copy at most CHUNK bytes into this free slot.
        // Buffer, registration and context outlive completion or successful
        // endpoint close; no other operation uses this slot. A failed post,
        // including EAGAIN, accepts no operation and retains no buffer ownership
        // (fi_msg(3), RETURN VALUE); only a successful post marks it immutable.
        let code = unsafe {
            let desc = if self.tx_mr.is_null() {
                ptr::null_mut()
            } else {
                ffi::fi_mr_desc(self.tx_mr)
            };
            ffi::fi_send(
                self.ep,
                self.memory.tx(slot).cast(),
                len,
                desc,
                peer,
                self.memory.tx_context(slot),
            )
        };
        if let Some(began) = began {
            let stats = &mut self.progress_stats;
            stats.send_post_ns = stats.send_post_ns.saturating_add(elapsed_ns(began));
            stats.send_post_calls = stats.send_post_calls.saturating_add(1);
            if code == 0 {
                stats.send_post_bytes = stats.send_post_bytes.saturating_add(len as u64);
            } else if code == -isize::try_from(ffi::FI_EAGAIN).unwrap() {
                stats.send_post_would_block = stats.send_post_would_block.saturating_add(1);
            } else {
                stats.send_post_errors = stats.send_post_errors.saturating_add(1);
            }
        }
        self.tx_pending[slot] = posted("post fabric send", code)?;
        if self.tx_pending[slot] {
            if self.time_progress && self.accepted_send == self.completed_send {
                self.last_poll = Some(Instant::now());
            }
            self.tx_ordinal[slot] = ordinal;
            self.accepted_send = ordinal;
        }
        Ok(self.tx_pending[slot])
    }

    pub(super) fn fill_receives(&mut self) -> io::Result<()> {
        for _ in 0..SLOTS {
            match self.try_post_receive() {
                Ok(true) => {}
                Ok(false) => break,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    pub(super) fn try_post_receive(&mut self) -> io::Result<bool> {
        // Post in ring order. FI_ORDER_SAS orders message matching; receive
        // completions may still arrive out of order, so copy_receive follows
        // posting order rather than CQ order. A single call never fills a ring.
        let slot = self.rx_post;
        if self.rx_pending[slot] || self.rx_len[slot] != 0 {
            return Ok(false);
        }
        if !self.post_receive(slot)? {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        self.rx_post = (slot + 1) % SLOTS;
        Ok(true)
    }

    /// Processes one completed direct packet in its provider-owned RX slot.
    /// The slot is reposted only after the visitor finishes, with EAGAIN
    /// tolerated; no packet copy or receive-slot alias escapes this method.
    pub(super) fn process_direct_packet(
        &mut self,
        process: impl FnOnce(&[u8]) -> io::Result<()>,
    ) -> io::Result<bool> {
        let Some(slot) = self.rx_len.iter().position(|&len| len != 0) else {
            return Ok(false);
        };
        let len = self.rx_len[slot];
        let began = self.time_progress.then(Instant::now);
        // SAFETY: CQ validation established the completed slot length; this
        // immutable borrow ends before the receive slot is reposted below.
        process(unsafe { std::slice::from_raw_parts(self.memory.rx(slot), len) })?;
        // Direct receives include checksum validation and reassembly copying,
        // not just the ordinary transport's completed-buffer copy.
        if let Some(began) = began {
            self.progress_stats.receive_copy_ns = self
                .progress_stats
                .receive_copy_ns
                .saturating_add(elapsed_ns(began));
            self.progress_stats.receive_copy_calls =
                self.progress_stats.receive_copy_calls.saturating_add(1);
            self.progress_stats.receive_copy_bytes = self
                .progress_stats
                .receive_copy_bytes
                .saturating_add(len as u64);
        }
        self.rx_len[slot] = 0;
        self.rx_post = slot;
        if let Err(error) = self.try_post_receive()
            && error.kind() != io::ErrorKind::WouldBlock
        {
            return Err(error);
        }
        Ok(true)
    }

    fn post_receive(&mut self, slot: usize) -> io::Result<bool> {
        let peer = self.peer.ok_or_else(|| invalid("missing fabric peer"))?;
        let began = self.time_progress.then(Instant::now);
        // SAFETY: try_post_receive ensures a free slot with no unread bytes. The
        // provider sees only its stable allocation, registration and context.
        let code = unsafe {
            let desc = if self.rx_mr.is_null() {
                ptr::null_mut()
            } else {
                ffi::fi_mr_desc(self.rx_mr)
            };
            ffi::fi_recv(
                self.ep,
                self.memory.rx(slot).cast(),
                CHUNK,
                desc,
                peer,
                self.memory.rx_context(slot),
            )
        };
        if let Some(began) = began {
            let stats = &mut self.progress_stats;
            stats.receive_post_ns = stats.receive_post_ns.saturating_add(elapsed_ns(began));
            stats.receive_post_calls = stats.receive_post_calls.saturating_add(1);
            if code == 0 {
                stats.receive_post_bytes = stats.receive_post_bytes.saturating_add(CHUNK as u64);
            } else if code == -isize::try_from(ffi::FI_EAGAIN).unwrap() {
                stats.receive_post_would_block = stats.receive_post_would_block.saturating_add(1);
            } else {
                stats.receive_post_errors = stats.receive_post_errors.saturating_add(1);
            }
        }
        self.rx_pending[slot] = posted("post fabric receive", code)?;
        Ok(self.rx_pending[slot])
    }

    pub(super) fn poll(&mut self) -> io::Result<bool> {
        self.poll_with_timeout::<false>(0)
    }

    pub(super) fn poll_for_sweep(&mut self, deadline: Option<Instant>) -> io::Result<(bool, bool)> {
        let Some(deadline) = deadline else {
            return self.poll().map(|progress| (progress, false));
        };
        let can_wait = self.supports_idle_wait()
            && !self.direct
            && self.rma.is_none()
            && self.peer.is_some()
            && self.tx_pending.iter().all(|pending| !pending)
            && self.rx_pending.iter().all(|pending| *pending)
            && self.rx_len.iter().all(|&len| len == 0)
            && self.rx_offset == 0
            && self.rx_read == self.rx_post;
        if !can_wait
            || deadline.saturating_duration_since(Instant::now()) < Duration::from_millis(1)
        {
            return self.poll().map(|progress| (progress, false));
        }
        self.poll_with_timeout::<true>(1)
            .map(|progress| (progress, true))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Keep each CQ pass and its message/RMA context retirement in one ordered block."
    )]
    fn poll_with_timeout<const WAIT: bool>(&mut self, timeout_ms: i32) -> io::Result<bool> {
        if self.time_progress {
            let now = Instant::now();
            if let Some(previous) = self.last_any_poll.replace(now) {
                let gap =
                    u64::try_from(now.duration_since(previous).as_nanos()).unwrap_or(u64::MAX);
                let stats = &mut self.progress_stats;
                stats.cq_poll_gap_ns = stats.cq_poll_gap_ns.saturating_add(gap);
                stats.cq_poll_gap_samples = stats.cq_poll_gap_samples.saturating_add(1);
                stats.cq_poll_gap_max_ns = stats.cq_poll_gap_max_ns.max(gap);
            }
            if self.sends_pending()
                && let Some(previous) = self.last_poll
            {
                let gap =
                    u64::try_from(now.duration_since(previous).as_nanos()).unwrap_or(u64::MAX);
                self.progress_stats.cq_max_poll_gap_ns =
                    self.progress_stats.cq_max_poll_gap_ns.max(gap);
            }
            self.last_poll = Some(now);
        }
        self.progress_stats.cq_polls = self.progress_stats.cq_polls.saturating_add(1);
        // Empty polls need no initialization. MSG len is receive-only: read
        // fields individually instead of assuming a complete Rust struct exists.
        let mut entries = [MaybeUninit::<ffi::fi_cq_msg_entry>::uninit(); 2 * SLOTS];
        let initial_progress = (!self.initial_progress_recorded).then(Instant::now);
        let began = self.time_progress.then(Instant::now);
        // SAFETY: Our CQ uses MSG format and may initialize at most entries.len()
        // entries; no Rust references to this storage cross the synchronous call.
        let count = unsafe {
            if WAIT {
                ffi::fi_cq_sread(
                    self.cq,
                    entries.as_mut_ptr().cast(),
                    entries.len(),
                    ptr::null(),
                    timeout_ms,
                )
            } else {
                ffi::fi_cq_read(self.cq, entries.as_mut_ptr().cast(), entries.len())
            }
        };
        if let Some(began) = began {
            self.progress_stats.cq_call_ns = self
                .progress_stats
                .cq_call_ns
                .saturating_add(elapsed_ns(began));
            self.progress_stats.cq_timed_calls =
                self.progress_stats.cq_timed_calls.saturating_add(1);
        }
        if let Some(start) = initial_progress {
            self.open_profile.first_progress_ns = elapsed_ns(start);
            self.initial_progress_recorded = true;
        }
        if count == -isize::try_from(ffi::FI_EAGAIN).unwrap()
            || count == 0
            || WAIT
                && (count == -isize::try_from(ffi::FI_ETIMEDOUT).unwrap()
                    || count == -isize::try_from(libc::EINTR).unwrap())
        {
            self.progress_stats.cq_empty_polls =
                self.progress_stats.cq_empty_polls.saturating_add(1);
            return Ok(false);
        }
        if count == -isize::try_from(ffi::FI_EAVAIL).unwrap() {
            let mut failure = ffi::fi_cq_err_entry::default();
            let began = self.time_progress.then(Instant::now);
            // SAFETY: EAVAIL requires reading the error queue. The zeroed entry
            // requests no application error-data buffer from the provider.
            let count = unsafe { ffi::fi_cq_readerr(self.cq, &raw mut failure, 0) };
            if let Some(began) = began {
                self.progress_stats.cq_error_call_ns = self
                    .progress_stats
                    .cq_error_call_ns
                    .saturating_add(elapsed_ns(began));
                self.progress_stats.cq_error_calls =
                    self.progress_stats.cq_error_calls.saturating_add(1);
            }
            let code = if count > 0 {
                failure.err.unsigned_abs()
            } else {
                ffi::FI_EIO
            };
            if count > 0
                && let Some(rma) = &mut self.rma
                && rma.complete(failure.op_context, code)?
            {
                self.progress_stats.cq_completions =
                    self.progress_stats.cq_completions.saturating_add(1);
                return Ok(true);
            }
            return Err(error("fabric completion", code));
        }
        let count = usize::try_from(count).map_err(|_| {
            error(
                "fabric completion queue",
                u32::try_from(count.unsigned_abs()).unwrap_or(ffi::FI_EIO),
            )
        })?;
        if count > entries.len() {
            return Err(invalid("invalid fabric completion count"));
        }
        for entry in &entries[..count] {
            let began = self.time_progress.then(Instant::now);
            let entry = entry.as_ptr();
            // SAFETY: Positive count validates this array slot; fi_cq specifies
            // op_context for every returned completion, including sends.
            let context = unsafe { ptr::addr_of!((*entry).op_context).read() };
            let result = self.complete(context, || {
                // SAFETY: complete calls this only for an owned receive CQE,
                // for which fi_cq guarantees len. Never read send-only padding
                // or fields which the provider need not initialize.
                unsafe { ptr::addr_of!((*entry).len).read() }
            });
            if let Some(began) = began {
                self.progress_stats.cq_dispatch_ns = self
                    .progress_stats
                    .cq_dispatch_ns
                    .saturating_add(elapsed_ns(began));
                self.progress_stats.cq_dispatch_calls =
                    self.progress_stats.cq_dispatch_calls.saturating_add(1);
                self.progress_stats.cq_dispatch_errors = self
                    .progress_stats
                    .cq_dispatch_errors
                    .saturating_add(u64::from(result.is_err()));
            }
            result?;
            self.progress_stats.cq_completions =
                self.progress_stats.cq_completions.saturating_add(1);
        }
        Ok(true)
    }

    fn complete(
        &mut self,
        context: *mut c_void,
        receive_len: impl FnOnce() -> usize,
    ) -> io::Result<()> {
        if let Some(rma) = &mut self.rma
            && rma.complete(context, 0)?
        {
            return Ok(());
        }
        for slot in 0..SLOTS {
            if context == self.memory.tx_context(slot) && self.tx_pending[slot] {
                self.tx_pending[slot] = false;
                // Every accepted ordinal not in a pending slot has completed.
                // Taking the oldest live ordinal handles arbitrary CQ order and
                // slot reuse with fixed O(SLOTS) state, even if one send stalls.
                self.completed_send = self
                    .tx_pending
                    .iter()
                    .zip(self.tx_ordinal)
                    .filter(|(pending, _)| **pending)
                    .map(|(_, ordinal)| ordinal - 1)
                    .min()
                    .unwrap_or(self.accepted_send);
                return Ok(());
            }
            if context == self.memory.rx_context(slot) && self.rx_pending[slot] {
                let len = receive_len();
                if len == 0 || len > CHUNK {
                    return Err(invalid("invalid fabric receive length"));
                }
                self.rx_pending[slot] = false;
                self.rx_len[slot] = len;
                return Ok(());
            }
        }
        Err(invalid("unexpected fabric completion"))
    }

    pub(super) fn close(&mut self) -> Result<(), i32> {
        if self.rma.as_ref().is_some_and(RmaState::has_pending_reads) {
            return Err(i32::try_from(ffi::FI_EBUSY).unwrap());
        }
        macro_rules! close {
            ($field:ident) => {
                if !self.$field.is_null() {
                    // SAFETY: Each handle is owned here; its generated C struct
                    // begins with fid. Stop on failure, retaining every live
                    // handle and allocation because the provider may use them.
                    let code = unsafe { ffi::fi_close(&raw mut (*self.$field).fid) };
                    if code != 0 {
                        // Do not format or allocate before the owner quarantines
                        // this allocation; even panic paths must retain memory.
                        return Err(code);
                    }
                    self.$field = ptr::null_mut();
                }
            };
        }
        // Successful endpoint close discards pending operations before memory
        // registrations close and before Memory can reconstruct/free its Box.
        close!(ep);
        if let Some(rma) = &mut self.rma {
            rma.close()?;
        }
        close!(tx_mr);
        close!(rx_mr);
        close!(av);
        close!(cq);
        close!(domain);
        close!(fabric);
        Ok(())
    }
}

pub(super) fn elapsed_ns(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

#[repr(C)]
struct AlignedAddress {
    alignment: [libc::max_align_t; 0],
    bytes: [u8; ADDRESS],
}

impl Default for AlignedAddress {
    fn default() -> Self {
        Self {
            alignment: [],
            bytes: [0; ADDRESS],
        }
    }
}

impl AlignedAddress {
    fn new(format: u32, expected_len: usize, bytes: &[u8]) -> io::Result<Self> {
        if bytes.is_empty() || bytes.len() > ADDRESS || bytes.len() != expected_len {
            return Err(invalid("invalid fabric peer address length"));
        }
        if format != ffi::FI_ADDR_EFA {
            let family = bytes
                .get(..size_of::<libc::sa_family_t>())
                .ok_or_else(|| invalid("short socket address"))?;
            let family = i32::from(libc::sa_family_t::from_ne_bytes(
                family.try_into().map_err(invalid)?,
            ));
            if !matches!(
                format,
                ffi::FI_SOCKADDR | ffi::FI_SOCKADDR_IN | ffi::FI_SOCKADDR_IN6
            ) || !(family == libc::AF_INET && bytes.len() == size_of::<libc::sockaddr_in>()
                || family == libc::AF_INET6 && bytes.len() == size_of::<libc::sockaddr_in6>())
                || format == ffi::FI_SOCKADDR_IN && family != libc::AF_INET
                || format == ffi::FI_SOCKADDR_IN6 && family != libc::AF_INET6
            {
                return Err(invalid("invalid fabric socket address family"));
            }
        }
        let mut address = Self::default();
        address.bytes[..bytes.len()].copy_from_slice(bytes);
        Ok(address)
    }
}

pub(super) fn invalid(message: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

pub(super) fn error(stage: &str, code: u32) -> io::Error {
    // SAFETY: fi_strerror returns a static terminated string for any error code.
    let detail =
        unsafe { CStr::from_ptr(ffi::fi_strerror(i32::try_from(code).unwrap_or(i32::MAX))) };
    let kind = if code == ffi::FI_ETIMEDOUT {
        io::ErrorKind::TimedOut
    } else {
        io::ErrorKind::Other
    };
    io::Error::new(
        kind,
        format!("{stage}: {} ({code})", detail.to_string_lossy()),
    )
}

fn check(stage: &str, code: i32) -> io::Result<()> {
    if code == 0 {
        Ok(())
    } else {
        Err(error(stage, code.unsigned_abs()))
    }
}

fn posted(stage: &str, code: isize) -> io::Result<bool> {
    if code == 0 {
        Ok(true)
    } else if code == -isize::try_from(ffi::FI_EAGAIN).unwrap() {
        Ok(false)
    } else {
        Err(error(
            stage,
            u32::try_from(code.unsigned_abs()).unwrap_or(ffi::FI_EIO),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_efa_requests_bounded_packet_queues_not_application_slot_counts() {
        for (provider, size) in [(c"efa", efa::QUEUE), (c"tcp;ofi_rxm", 0)] {
            let hints = Info::hints(provider, c"", false, false).unwrap();
            // SAFETY: Info owns the live, fully initialized hint allocation.
            let (tx, rx) = unsafe { (&*(*hints.0).tx_attr, &*(*hints.0).rx_attr) };
            assert_eq!(tx.size, size);
            assert_eq!(rx.size, size);
            assert_eq!(tx.msg_order, u64::from(ffi::FI_ORDER_SAS));
            assert_eq!(rx.msg_order, u64::from(ffi::FI_ORDER_SAS));
        }
        let mut state = Resources {
            tx_pending: [true; SLOTS],
            ..Resources::default()
        };
        assert_eq!(state.free_send_slot(), None);

        state.tx_pending[SLOTS - 1] = false;
        assert_eq!(state.free_send_slot(), Some(SLOTS - 1));
    }
    #[test]
    #[ignore = "requires libfabric tcp;ofi_rxm software provider"]
    fn strict_device_read_rejects_software_provider() {
        let config = RmaConfig {
            source_bytes: 4096,
            source_region_slots: 1,
            destination_bytes: 4096,
            destination_slots: 1,
            max_read_bytes: 1024,
            require_device_read: true,
        };
        let mut resources = Resources::default();
        assert!(
            resources
                .open_mode(
                    &FabricConfig {
                        backend: FabricBackend::SoftwareTcp,
                        domain: Some("lo".to_owned()),
                        source_address: Some(std::net::Ipv4Addr::LOCALHOST.into()),
                    },
                    Some(config),
                )
                .is_err()
        );
    }

    #[test]
    fn receive_delivery_follows_posting_order_not_completion_order() {
        let mut state = Resources {
            rx_pending: [true; SLOTS],
            ..Resources::default()
        };
        for slot in 0..SLOTS {
            // SAFETY: These unit-test buffers have no provider. Each slot has
            // CHUNK writable bytes and the one-byte fake completion is in bounds.
            unsafe { state.memory.rx(slot).write(u8::try_from(slot + 1).unwrap()) };
        }
        for slot in (1..SLOTS).rev() {
            state.complete(state.memory.rx_context(slot), || 1).unwrap();
        }
        assert!(state.copy_receive(&mut [0]).is_none());
        state.complete(state.memory.rx_context(0), || 1).unwrap();
        let mut received = Vec::new();
        for _ in 0..SLOTS {
            let mut byte = [0];
            assert_eq!(state.copy_receive(&mut byte), Some(1));
            received.push(byte[0]);
        }
        assert_eq!(received, [1, 2, 3, 4]);
        assert_eq!(state.rx_read, 0);
        assert!(state.rx_len.iter().all(|len| *len == 0));
        assert!(state.complete(state.memory.rx_context(0), || 1).is_err());
    }

    #[test]
    fn transmit_slots_remain_unavailable_until_their_own_completion() {
        let mut state = Resources {
            tx_pending: [true; SLOTS],
            tx_ordinal: [1, 2, 3, 4],
            accepted_send: 4,
            ..Resources::default()
        };
        assert_eq!(state.free_send_slot(), None);
        for slot in (0..SLOTS).rev() {
            state
                .complete(state.memory.tx_context(slot), || {
                    panic!("send has no receive length")
                })
                .unwrap();
            assert_eq!(state.free_send_slot(), Some(slot));
        }
        assert!(!state.sends_pending());
    }

    #[test]
    fn vectored_copy_is_bounded_and_receive_never_reuses_unread_bytes() {
        let mut state = Resources::default();
        let body = vec![9; CHUNK + 1];
        assert_eq!(
            state.copy_send(2, &[IoSlice::new(&[7; 3]), IoSlice::new(&body)]),
            CHUNK
        );
        // SAFETY: No provider exists in this test, and both selected slots own
        // CHUNK bytes. Copy as if the provider delivered one complete message.
        unsafe { ptr::copy_nonoverlapping(state.memory.tx(2), state.memory.rx(0), CHUNK) };
        state.rx_pending[0] = true;
        state
            .complete(state.memory.rx_context(0), || CHUNK)
            .unwrap();
        let mut prefix = [0; 3];
        assert_eq!(state.copy_receive(&mut prefix), Some(3));
        assert_eq!(prefix, [7; 3]);
        assert_eq!(state.rx_read, 0);
        assert_eq!(state.rx_len[0], CHUNK);
        let mut rest = vec![0; CHUNK];
        assert_eq!(state.copy_receive(&mut rest), Some(CHUNK - 3));
        assert!(rest[..CHUNK - 3].iter().all(|byte| *byte == 9));
        assert_eq!(state.rx_read, 1);
        assert_eq!(state.rx_len[0], 0);
    }

    #[test]
    fn socket_addresses_are_aligned_and_family_length_checked() {
        let mut storage = AlignedAddress::default();
        let bytes = &mut storage.bytes[..=size_of::<libc::sockaddr_in>()];
        bytes[1..3].copy_from_slice(
            &libc::sa_family_t::try_from(libc::AF_INET)
                .unwrap()
                .to_ne_bytes(),
        );
        let unaligned = &bytes[1..];
        assert_ne!(
            unaligned
                .as_ptr()
                .align_offset(align_of::<libc::sockaddr_in>()),
            0
        );
        let aligned = AlignedAddress::new(ffi::FI_SOCKADDR_IN, unaligned.len(), unaligned).unwrap();
        assert_eq!(
            aligned
                .bytes
                .as_ptr()
                .align_offset(align_of::<libc::max_align_t>()),
            0
        );
        assert_eq!(&aligned.bytes[..unaligned.len()], unaligned);
        assert!(AlignedAddress::new(ffi::FI_SOCKADDR_IN6, unaligned.len(), unaligned).is_err());
        assert!(AlignedAddress::new(ffi::FI_SOCKADDR_IN, 1, &unaligned[..1]).is_err());
        assert!(
            AlignedAddress::new(
                ffi::FI_SOCKADDR,
                unaligned.len() - 1,
                &unaligned[..unaligned.len() - 1]
            )
            .is_err()
        );
        bytes[1..3].copy_from_slice(
            &libc::sa_family_t::try_from(libc::AF_UNIX)
                .unwrap()
                .to_ne_bytes(),
        );
        assert!(AlignedAddress::new(ffi::FI_SOCKADDR, bytes.len() - 1, &bytes[1..]).is_err());
        assert!(AlignedAddress::new(ffi::FI_ADDR_STR, bytes.len() - 1, &bytes[1..]).is_err());
    }
    #[test]
    fn efa_direct_hints_select_named_fabric_without_ordering_assumptions() {
        let hints = Info::hints(c"efa", c"", true, false).unwrap();
        // SAFETY: The live Info owns initialized attributes and their strings.
        unsafe {
            let info = &*hints.0;
            assert_eq!(
                CStr::from_ptr((*info.fabric_attr).prov_name).to_bytes(),
                b"efa"
            );
            assert_eq!(
                CStr::from_ptr((*info.fabric_attr).name).to_bytes(),
                b"efa-direct"
            );
            assert_eq!(info.caps, u64::from(ffi::FI_MSG));
            assert_eq!(info.mode, ffi::FI_CONTEXT2);
            assert_eq!((*info.tx_attr).msg_order, 0);
            assert_eq!((*info.rx_attr).msg_order, 0);
            assert_eq!((*info.tx_attr).size, SLOTS);
            assert_eq!((*info.rx_attr).size, SLOTS);
        }
    }
    #[test]
    fn direct_local_cqe_releases_physical_slot_without_remote_credit() {
        let mut state = Resources {
            tx_pending: [true; SLOTS],
            tx_ordinal: [1, 2, 3, 4],
            accepted_send: 4,
            direct: true,
            ..Resources::default()
        };
        let mut sender = super::super::efa_direct::SendWindow::default();
        for ordinal in 1..=super::super::efa_direct::DATA_WINDOW {
            sender.posted(ordinal).unwrap();
        }
        // SAFETY: The fixture has no provider. This exact context is a pending
        // TX slot and complete() validates it before changing ownership.
        state.complete(state.memory.tx_context(0), || 0).unwrap();
        assert_eq!(state.send_fences(), (4, 1));
        assert_eq!(state.free_send_slot(), Some(0));
        assert_eq!(sender.peer_credit(), 0);
        assert_eq!(
            sender.next_ordinal().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }
}
