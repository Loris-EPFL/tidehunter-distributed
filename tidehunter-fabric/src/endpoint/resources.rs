//! Native resources and raw ownership of memory accessed by the provider.

mod ffi_abi;
#[cfg(test)]
mod nonblocking_tests;

use super::{
    CHUNK, OpenProfile, ProgressStats, ProviderGeometry, ProviderInfo, ProviderParameter, SLOTS,
};
use crate::{FabricBackend, FabricConfig, MAX_SLOTS};
use ffi_abi::abi;
use ofi_libfabric_sys::bindgen as ffi;
use std::{
    cell::UnsafeCell,
    ffi::{CStr, CString, c_void},
    io::{self, IoSlice},
    mem::{self, MaybeUninit},
    ptr,
    ptr::NonNull,
    time::{Duration, Instant},
};

pub(super) const ADDRESS: usize = 512;

/// Provider-visible TX/RX chunk and completion-context storage for one endpoint.
/// Each array is one zeroed heap allocation addressed only through raw pointers:
/// no Rust reference or `Box` to this memory exists while the provider can access
/// it, so moving `Resources` cannot retag registered buffers.
struct Memory {
    slots: usize,
    tx: NonNull<UnsafeCell<[u8; CHUNK]>>,
    rx: NonNull<UnsafeCell<[u8; CHUNK]>>,
    tx_context: NonNull<UnsafeCell<ffi::fi_context2>>,
    rx_context: NonNull<UnsafeCell<ffi::fi_context2>>,
}

/// Allocates `len` zeroed elements directly on the heap (a TX array can be 4 MiB,
/// so no stack temporary) and returns a raw owner released by `release_array`
/// with the same `len`.
///
/// # Safety
/// All-zero bytes must be a valid `T`.
unsafe fn zeroed_array<T>(len: usize) -> NonNull<T> {
    let mut array = Box::<[T]>::new_uninit_slice(len);
    // SAFETY: the allocation holds exactly `len` elements and zero is valid by
    // this function's contract.
    let array = unsafe {
        array.as_mut_ptr().write_bytes(0, len);
        array.assume_init()
    };
    // SAFETY: Box::into_raw never returns null.
    unsafe { NonNull::new_unchecked(Box::into_raw(array).cast()) }
}

/// Frees an allocation produced by `zeroed_array::<T>(len)`.
///
/// # Safety
/// `ptr` and `len` must be exactly a prior `zeroed_array` result, released once,
/// with no live provider access to the memory.
unsafe fn release_array<T>(ptr: NonNull<T>, len: usize) {
    let slice = ptr::slice_from_raw_parts_mut(ptr.as_ptr(), len);
    // SAFETY: by contract `slice` is exactly the leaked boxed slice, released once.
    drop(unsafe { Box::from_raw(slice) });
}

impl Memory {
    fn new(slots: usize) -> Self {
        debug_assert!((1..=MAX_SLOTS).contains(&slots));
        // SAFETY: byte arrays and fi_context2 (raw-pointer fields) are valid all-zero.
        unsafe {
            Self {
                slots,
                tx: zeroed_array(slots),
                rx: zeroed_array(slots),
                tx_context: zeroed_array(slots),
                rx_context: zeroed_array(slots),
            }
        }
    }

    fn tx(&self, slot: usize) -> *mut u8 {
        debug_assert!(slot < self.slots);
        // SAFETY: slot is in bounds of the live allocation, which lives until
        // successful resource teardown. Only a raw cell pointer is formed.
        unsafe { UnsafeCell::raw_get(self.tx.as_ptr().add(slot)).cast::<u8>() }
    }

    fn rx(&self, slot: usize) -> *mut u8 {
        debug_assert!(slot < self.slots);
        // SAFETY: Callers use slot < self.slots in this live allocation. Forming
        // a raw pointer into UnsafeCell creates no reference to provider memory.
        unsafe { UnsafeCell::raw_get(self.rx.as_ptr().add(slot)).cast::<u8>() }
    }

    fn tx_context(&self, slot: usize) -> *mut c_void {
        debug_assert!(slot < self.slots);
        // SAFETY: Callers use slot < self.slots. The context allocation stays
        // live and fixed until teardown; raw_get creates no Rust reference.
        unsafe { UnsafeCell::raw_get(self.tx_context.as_ptr().add(slot)).cast() }
    }

    fn rx_context(&self, slot: usize) -> *mut c_void {
        debug_assert!(slot < self.slots);
        // SAFETY: Callers use slot < self.slots. The context allocation stays
        // live and fixed until teardown; raw_get creates no Rust reference.
        unsafe { UnsafeCell::raw_get(self.rx_context.as_ptr().add(slot)).cast() }
    }

    /// Slot whose TX completion context is exactly `context`, if any.
    fn tx_context_slot(&self, context: *mut c_void) -> Option<usize> {
        context_slot(self.tx_context, self.slots, context)
    }

    /// Slot whose RX completion context is exactly `context`, if any.
    fn rx_context_slot(&self, context: *mut c_void) -> Option<usize> {
        context_slot(self.rx_context, self.slots, context)
    }
}

/// Maps a CQ context pointer back to an element index of the context array at
/// `base`, rejecting any address that is not exactly an element start. Address
/// arithmetic only: the pointer is never dereferenced.
fn context_slot(
    base: NonNull<UnsafeCell<ffi::fi_context2>>,
    slots: usize,
    context: *mut c_void,
) -> Option<usize> {
    let size = mem::size_of::<UnsafeCell<ffi::fi_context2>>();
    let offset = (context as usize).checked_sub(base.as_ptr() as usize)?;
    (offset % size == 0 && offset / size < slots).then_some(offset / size)
}

impl Default for Memory {
    fn default() -> Self {
        Self::new(SLOTS)
    }
}

impl Drop for Memory {
    fn drop(&mut self) {
        // SAFETY: Each pointer and self.slots are the original zeroed_array pair,
        // released once. Endpoint drops registered memory only after all handles
        // close; failed close retains Resources and these allocations.
        unsafe {
            release_array(self.tx, self.slots);
            release_array(self.rx, self.slots);
            release_array(self.tx_context, self.slots);
            release_array(self.rx_context, self.slots);
        }
    }
}

/// Per-slot bookkeeping sized to the endpoint's slot count (default `SLOTS`).
struct PerSlot<T>(Vec<T>);

impl<T: Clone + Default> PerSlot<T> {
    fn new(slots: usize) -> Self {
        Self(vec![T::default(); slots])
    }
}

impl<T: Clone + Default> Default for PerSlot<T> {
    fn default() -> Self {
        Self::new(SLOTS)
    }
}

impl<T> std::ops::Deref for PerSlot<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        &self.0
    }
}

impl<T> std::ops::DerefMut for PerSlot<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        &mut self.0
    }
}

/// How an idle owner may park on the completion queue.
#[derive(Clone, Copy, Default)]
enum CqWaitMode {
    #[default]
    Polling,
    /// Software `RxM`: bounded `fi_cq_sread`.
    SoftwareBounded,
    /// Provider wait file descriptor: `fi_trywait` then `poll(2)` on the fd.
    FdTryWait(libc::c_int),
}

/// Which CQ wait object an open requests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WaitRequest {
    None,
    /// `FI_WAIT_UNSPEC` for the software provider's bounded `fi_cq_sread`.
    Software,
    /// `FI_WAIT_FD` for `fi_trywait` plus `poll(2)`.
    Fd,
}

impl WaitRequest {
    /// Software `RxM` always parks in bounded `fi_cq_sread`. `verbs` requests a
    /// wait fd only when the owner parks while idle (`park_idle`): the wait
    /// object adds signalling work to every completion, which a polling client
    /// never needs.
    fn select(backend: FabricBackend, park_idle: bool) -> Self {
        match backend {
            FabricBackend::SoftwareTcp => Self::Software,
            FabricBackend::Verbs if park_idle => Self::Fd,
            FabricBackend::Verbs => Self::None,
        }
    }
}

/// A filled, unposted TX slot kept after an EAGAIN. It owns no ordinal, no
/// credit and no provider resource, so it remains an ordinary free slot.
#[derive(Clone, Copy)]
struct StagedSend {
    slot: usize,
    len: usize,
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
    tx_pending: PerSlot<bool>,
    tx_ordinal: PerSlot<u64>,
    staged_send: Option<StagedSend>,
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
    /// Human-readable outcome of the CQ wait request, for provider evidence.
    cq_wait_note: String,
    rx_pending: PerSlot<bool>,
    rx_len: PerSlot<usize>,
    rx_offset: usize,
    rx_read: usize,
    rx_post: usize,
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
    fn hints(provider: &CStr, domain: &CStr) -> io::Result<Self> {
        // SAFETY: No arguments. Info owns the result and releases it on all paths.
        let hints = Info(unsafe { ffi::fi_allocinfo() });
        if hints.0.is_null() {
            return Err(error("allocate fabric hints", ffi::FI_ENOMEM));
        }
        // SAFETY: fi_allocinfo initializes all nested attribute allocations.
        // strdup gives ownership of matching C-allocator strings to fi_freeinfo.
        unsafe {
            (*(*hints.0).ep_attr).type_ = ffi::fi_ep_type_FI_EP_RDM;
            (*hints.0).caps = u64::from(ffi::FI_MSG) | ffi::FI_DIRECTED_RECV;
            (*hints.0).mode = ffi::FI_CONTEXT;
            (*(*hints.0).domain_attr).threading = ffi::fi_threading_FI_THREAD_DOMAIN;
            (*(*hints.0).domain_attr).__bindgen_anon_1.data_progress =
                ffi::fi_progress_FI_PROGRESS_MANUAL;
            let requested_mr_modes = if provider.to_bytes() == b"verbs;ofi_rxm" {
                ffi::FI_MR_LOCAL | ffi::FI_MR_PROV_KEY | ffi::FI_MR_VIRT_ADDR | ffi::FI_MR_ALLOCATED
            } else {
                ffi::FI_MR_LOCAL
            };
            (*(*hints.0).domain_attr).mr_mode = abi(requested_mr_modes);
            (*(*hints.0).tx_attr).msg_order = u64::from(ffi::FI_ORDER_SAS);
            (*(*hints.0).rx_attr).msg_order = u64::from(ffi::FI_ORDER_SAS);
            (*(*hints.0).fabric_attr).prov_name = libc::strdup(provider.as_ptr());
            if (*(*hints.0).fabric_attr).prov_name.is_null() {
                return Err(error("allocate provider name", ffi::FI_ENOMEM));
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
        // SAFETY: Callers pass a libfabric-owned NUL-terminated attribute or
        // parameter string and keep its fi_info or parameter array live here.
        Some(
            unsafe { CStr::from_ptr(pointer) }
                .to_string_lossy()
                .into_owned(),
        )
    }
}
fn selected_source_address(info: &ffi::fi_info) -> Option<std::net::IpAddr> {
    // SAFETY: Callers keep the fi_getinfo result and its source allocation live.
    // Format, length and null checks bound each read; read_unaligned does not
    // require the provider's sockaddr pointer to have Rust alignment.
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

fn format_version(version: u32) -> String {
    format!("{}.{}", version >> 16, version & 0xffff)
}

fn mr_mode_flags(mode: i32) -> Vec<String> {
    const FLAGS: [(u32, &str); 10] = [
        (ffi::FI_MR_LOCAL, "local"),
        (ffi::FI_MR_RAW, "raw"),
        (ffi::FI_MR_VIRT_ADDR, "virt-addr"),
        (ffi::FI_MR_ALLOCATED, "allocated"),
        (ffi::FI_MR_PROV_KEY, "prov-key"),
        (ffi::FI_MR_MMU_NOTIFY, "mmu-notify"),
        (ffi::FI_MR_RMA_EVENT, "rma-event"),
        (ffi::FI_MR_ENDPOINT, "endpoint"),
        (ffi::FI_MR_HMEM, "hmem"),
        (ffi::FI_MR_COLLECTIVE, "collective"),
    ];
    let mode = u32::try_from(mode).unwrap_or(0);
    FLAGS
        .iter()
        .filter(|(bit, _)| mode & bit != 0)
        .map(|(_, name)| (*name).to_owned())
        .collect()
}

fn progress_name(progress: ffi::fi_progress) -> &'static str {
    match progress {
        ffi::fi_progress_FI_PROGRESS_AUTO => "auto",
        ffi::fi_progress_FI_PROGRESS_MANUAL => "manual",
        ffi::fi_progress_FI_PROGRESS_CONTROL_UNIFIED => "control-unified",
        ffi::fi_progress_FI_PROGRESS_UNSPEC => "unspec",
        _ => "unknown",
    }
}

fn threading_name(threading: ffi::fi_threading) -> &'static str {
    match threading {
        ffi::fi_threading_FI_THREAD_SAFE => "safe",
        ffi::fi_threading_FI_THREAD_FID => "fid",
        ffi::fi_threading_FI_THREAD_DOMAIN => "domain",
        ffi::fi_threading_FI_THREAD_COMPLETION => "completion",
        ffi::fi_threading_FI_THREAD_ENDPOINT => "endpoint",
        ffi::fi_threading_FI_THREAD_UNSPEC => "unspec",
        _ => "unknown",
    }
}

/// Upper bound on libfabric parameter entries inspected; the library defines
/// a few hundred, so this only bounds a corrupted count.
const MAX_PARAMETERS: usize = 4096;
/// The default a libfabric help string states, as in "(default 16384)" or
/// "(default: 10000)." Only the first value is taken ("128, 4096 with srx").
fn help_default(help: &str) -> Option<String> {
    let after = &help[help.find("(default")? + "(default".len()..];
    let value = after
        .trim_start_matches([':', ' '])
        .split([')', ','])
        .next()?
        .trim();
    (!value.is_empty()).then(|| value.to_owned())
}

/// `RxM` parameters (`FI_OFI_RXM_*`): configured override and provider help text.
/// Returns an empty list when libfabric reports none.
fn rxm_parameters() -> Vec<ProviderParameter> {
    let mut params = ptr::null_mut();
    let mut count = 0;
    // SAFETY: fi_getparams stores an array of `count` entries that libfabric owns
    // until fi_freeparams; both out-pointers are live locals.
    if unsafe { ffi::fi_getparams(&raw mut params, &raw mut count) } != 0 || params.is_null() {
        return Vec::new();
    }
    let len = usize::try_from(count).unwrap_or(0).min(MAX_PARAMETERS);
    // SAFETY: `params` points to at least `len` initialized entries until the
    // fi_freeparams call below; the strings are copied out before then.
    let entries = unsafe { std::slice::from_raw_parts(params, len) };
    let mut found = Vec::new();
    for entry in entries {
        let Some(name) = optional_c_string(entry.name) else {
            continue;
        };
        // libfabric names parameters "<provider>_<name>" in lower case; the
        // environment variable is that name, upper-cased, with an FI_ prefix.
        let name = name.to_ascii_uppercase();
        if !name.contains("OFI_RXM_") {
            continue;
        }
        let name = if name.starts_with("FI_") {
            name
        } else {
            format!("FI_{name}")
        };
        // libfabric reads provider parameters from the environment only.
        let value = optional_c_string(entry.value).or_else(|| std::env::var(&name).ok());
        let help = optional_c_string(entry.help_string).unwrap_or_default();
        found.push(ProviderParameter {
            configured_or_default: value.clone().or_else(|| help_default(&help)),
            name,
            value,
            help,
        });
    }
    // SAFETY: `params` came from fi_getparams and is released exactly once.
    unsafe { ffi::fi_freeparams(params) };
    found.sort_by(|left, right| left.name.cmp(&right.name));
    found
}

/// Copies the selected provider/domain/endpoint attributes into plain data.
///
/// # Safety
/// `selected` must be a successful `fi_getinfo` result whose fabric, domain,
/// endpoint, TX and RX attribute pointers are non-null. The caller must keep
/// the result and its nested allocations live for this call.
unsafe fn capture_geometry(
    selected: &ffi::fi_info,
    requested_api: u32,
    slots: usize,
    cq_wait: &str,
) -> ProviderGeometry {
    // SAFETY: by this function's contract all attribute pointers are valid.
    let (fabric, domain, ep, tx, rx) = unsafe {
        (
            &*selected.fabric_attr,
            &*selected.domain_attr,
            &*selected.ep_attr,
            &*selected.tx_attr,
            &*selected.rx_attr,
        )
    };
    let rxm = optional_c_string(fabric.prov_name).is_some_and(|name| name.contains("ofi_rxm"));
    ProviderGeometry {
        // SAFETY: fi_version has no arguments and no preconditions.
        libfabric_api_version: format_version(unsafe { ffi::fi_version() }),
        requested_api_version: format_version(requested_api),
        provider_version: format_version(fabric.prov_version),
        protocol: ep.protocol,
        protocol_version: ep.protocol_version,
        tx_inject_size: tx.inject_size,
        tx_size: tx.size,
        rx_size: rx.size,
        tx_iov_limit: tx.iov_limit,
        rx_iov_limit: rx.iov_limit,
        max_msg_size: ep.max_msg_size,
        mr_mode: domain.mr_mode,
        mr_mode_flags: mr_mode_flags(domain.mr_mode),
        // SAFETY: libfabric initializes this union as fi_progress; both union
        // names have that type, which accepts every underlying integer value.
        data_progress: progress_name(unsafe { domain.__bindgen_anon_1.data_progress }).to_owned(),
        control_progress: progress_name(domain.control_progress).to_owned(),
        threading: threading_name(domain.threading).to_owned(),
        slots,
        cq_wait: cq_wait.to_owned(),
        rxm_parameters: if rxm { rxm_parameters() } else { Vec::new() },
    }
}

impl Resources {
    fn slots(&self) -> usize {
        self.memory.slots
    }

    /// Resizes the TX/RX storage before any registration or provider handle
    /// can reference it. Only called from `open_mode`.
    fn set_slots(&mut self, slots: usize) {
        if slots != self.slots() {
            self.memory = Memory::new(slots);
            self.tx_pending = PerSlot::new(slots);
            self.tx_ordinal = PerSlot::new(slots);
            self.rx_pending = PerSlot::new(slots);
            self.rx_len = PerSlot::new(slots);
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Keep validated provider info and partial resource ownership in one ordered setup path"
    )]
    pub(super) fn open_mode(&mut self, config: &FabricConfig, park_idle: bool) -> io::Result<()> {
        config.validate()?;
        let discovery = Instant::now();
        let provider_name = config.backend.provider();
        let provider = CString::new(provider_name).map_err(invalid)?;
        let domain = CString::new(config.domain.as_deref().unwrap_or_default()).map_err(invalid)?;
        let slots = config.effective_slots();
        self.set_slots(slots);
        // Software RxM parks in bounded fi_cq_sread. Verbs parks on a provider
        // wait fd only when the owner asks (`park_idle`): the wait object adds
        // signalling work to every completion, which a polling client never needs.
        let wait_request = WaitRequest::select(config.backend, park_idle);
        let hints = Info::hints(&provider, &domain)?;
        let source_v4 = config.source_address.and_then(|address| match address {
            std::net::IpAddr::V4(address) => Some(libc::sockaddr_in {
                sin_family: abi(libc::AF_INET),
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
                sin6_family: abi(libc::AF_INET6),
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
            // SAFETY: address points to the selected live sockaddr struct;
            // owned_address is a distinct, non-null allocation of address_size.
            // hints owns its initialized fi_info and any prior C-allocated address.
            // Replace that address once, transferring the new allocation to Info.
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
        let api = (1 << 16) | 11;
        let mut info = Info(ptr::null_mut());
        // SAFETY: Hints and output storage are live through this synchronous call.
        let discovered =
            unsafe { ffi::fi_getinfo(api, ptr::null(), ptr::null(), 0, hints.0, &raw mut info.0) };
        check("discover explicit fabric provider", discovered)?;
        self.open_profile.discovery_ns = elapsed_ns(discovery);
        if info.0.is_null() {
            return Err(invalid("fabric returned no provider info"));
        }
        // SAFETY: info owns the non-null fi_getinfo result throughout this block.
        // Required attribute pointers are checked before use; optional NIC fields
        // and strings are checked before dereference. Union reads use the reported
        // discriminant, or the equivalent fi_progress fields. Native handles are
        // used only after successful creation and saved for ordered teardown.
        // Registered buffers and contexts stay allocated until close succeeds;
        // output pointers address live locals or exclusively borrowed fields.
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
            let supported_mr_modes = if provider.as_bytes() == b"verbs;ofi_rxm" {
                ffi::FI_MR_LOCAL | ffi::FI_MR_PROV_KEY | ffi::FI_MR_VIRT_ADDR | ffi::FI_MR_ALLOCATED
            } else {
                ffi::FI_MR_LOCAL | ffi::FI_MR_PROV_KEY | ffi::FI_MR_VIRT_ADDR
            };
            let valid_identity =
                !provider_name.is_null() && CStr::from_ptr(provider_name) == provider.as_c_str();
            let valid_normal = (*selected.ep_attr).max_msg_size >= CHUNK
                && selected.caps & (u64::from(ffi::FI_MSG) | ffi::FI_DIRECTED_RECV)
                    == (u64::from(ffi::FI_MSG) | ffi::FI_DIRECTED_RECV)
                && (*selected.tx_attr).msg_order & u64::from(ffi::FI_ORDER_SAS) != 0
                && (*selected.rx_attr).msg_order & u64::from(ffi::FI_ORDER_SAS) != 0
                && selected.mode & !ffi::FI_CONTEXT == 0;
            if !valid_identity
                || !valid_normal
                || (*selected.ep_attr).type_ != ffi::fi_ep_type_FI_EP_RDM
                || selected.caps & u64::from(ffi::FI_MSG) == 0
                || (*selected.tx_attr).iov_limit == 0
                || (*selected.rx_attr).iov_limit == 0
                || !matches!(
                    (*selected.domain_attr).threading,
                    ffi::fi_threading_FI_THREAD_SAFE | ffi::fi_threading_FI_THREAD_DOMAIN
                )
                || (*selected.tx_attr).size < slots
                || (*selected.rx_attr).size < slots
                || mr_mode & !abi::<_, i32>(supported_mr_modes) != 0
            {
                return Err(invalid("unsupported fabric provider capabilities"));
            }
            self.open_profile.selected_tx = (*selected.tx_attr).size;
            self.open_profile.selected_rx = (*selected.rx_attr).size;
            self.open_profile.provider_version = (*selected.fabric_attr).prov_version;
            if !selected.nic.is_null() && !(*selected.nic).link_attr.is_null() {
                self.open_profile.mtu = (*(*selected.nic).link_attr).mtu;
            }
            self.addr_format = selected.addr_format;
            if !matches!(
                self.addr_format,
                ffi::FI_SOCKADDR | ffi::FI_SOCKADDR_IN | ffi::FI_SOCKADDR_IN6
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
            self.open_endpoint(info.0, wait_request)?;
            let stage = Instant::now();
            if mr_mode & abi::<_, i32>(ffi::FI_MR_LOCAL) != 0 {
                check(
                    "register transmit buffer",
                    ffi::fi_mr_reg(
                        self.domain,
                        self.memory.tx(0).cast(),
                        CHUNK * slots,
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
                        CHUNK * slots,
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
            self.provider_info = Some(ProviderInfo {
                backend: config.backend,
                provider: optional_c_string(provider_name).unwrap_or_default(),
                fabric: optional_c_string(fabric_name),
                domain: optional_c_string((*selected.domain_attr).name),
                source_address: selected_source_address(selected),
                device,
                driver,
                bus_address,
                capability_evidence: "libfabric fi_getinfo provider/domain selection".to_owned(),
                geometry: capture_geometry(selected, api, slots, &self.cq_wait_note),
            });
        }
        Ok(())
    }

    fn open_endpoint(
        &mut self,
        info: *mut ffi::fi_info,
        wait_request: WaitRequest,
    ) -> io::Result<()> {
        let mut cq = ffi::fi_cq_attr {
            size: 2 * self.slots(),
            format: ffi::fi_cq_format_FI_CQ_FORMAT_MSG,
            wait_obj: match wait_request {
                WaitRequest::None => ffi::fi_wait_obj_FI_WAIT_NONE,
                WaitRequest::Software => ffi::fi_wait_obj_FI_WAIT_UNSPEC,
                WaitRequest::Fd => ffi::fi_wait_obj_FI_WAIT_FD,
            },
            ..ffi::fi_cq_attr::default()
        };
        let mut av = ffi::fi_av_attr {
            type_: ffi::fi_av_type_FI_AV_TABLE,
            count: 1,
            ..ffi::fi_av_attr::default()
        };
        // SAFETY: The caller supplies a live domain and fi_getinfo allocation.
        // CQ/AV attributes and output pointers stay live for each synchronous call.
        // Each handle is used only after successful creation and saved before the
        // next fallible step; teardown closes the endpoint before its bindings.
        unsafe {
            let stage = Instant::now();
            let mut code =
                ffi::fi_cq_open(self.domain, &raw mut cq, &raw mut self.cq, ptr::null_mut());
            self.cq_wait_note = match wait_request {
                WaitRequest::None => "none: not requested".to_owned(),
                WaitRequest::Software => "unspec: bounded fi_cq_sread".to_owned(),
                WaitRequest::Fd => "fd: FI_WAIT_FD with fi_trywait and poll".to_owned(),
            };
            let refused = [ffi::FI_ENOSYS, ffi::FI_EOPNOTSUPP, ffi::FI_EINVAL]
                .into_iter()
                .any(|refusal| code == -abi::<_, i32>(refusal));
            if wait_request != WaitRequest::None && refused {
                // Keep the polling behavior: a provider without a wait object is
                // reported in evidence, never papered over with a different one.
                self.cq_wait_note = format!(
                    "none: provider rejected the CQ wait object ({}); busy polling retained",
                    error("open completion queue wait", code.unsigned_abs())
                );
                cq.wait_obj = ffi::fi_wait_obj_FI_WAIT_NONE;
                self.cq = ptr::null_mut();
                code = ffi::fi_cq_open(self.domain, &raw mut cq, &raw mut self.cq, ptr::null_mut());
            } else if code == 0 {
                match wait_request {
                    WaitRequest::None => {}
                    WaitRequest::Software => self.cq_wait_mode = CqWaitMode::SoftwareBounded,
                    WaitRequest::Fd => {
                        let mut fd: libc::c_int = -1;
                        let status = ffi::fi_control(
                            &raw mut (*self.cq).fid,
                            abi(ffi::FI_GETWAIT),
                            (&raw mut fd).cast(),
                        );
                        if status == 0 && fd >= 0 {
                            self.cq_wait_mode = CqWaitMode::FdTryWait(fd);
                        } else {
                            self.cq_wait_note = format!(
                                "none: FI_GETWAIT returned no fd (status {status}); busy polling retained"
                            );
                        }
                    }
                }
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

    pub(super) fn free_send_slot(&self) -> Option<usize> {
        self.tx_pending.iter().position(|pending| !pending)
    }
    pub(super) fn sends_pending(&self) -> bool {
        self.tx_pending.iter().any(|pending| *pending)
    }

    pub(super) fn send_fences(&self) -> (u64, u64) {
        (self.accepted_send, self.completed_send)
    }

    pub(super) fn supports_idle_wait(&self) -> bool {
        !matches!(self.cq_wait_mode, CqWaitMode::Polling)
    }

    pub(super) fn has_wait_fd(&self) -> bool {
        matches!(self.cq_wait_mode, CqWaitMode::FdTryWait(_))
    }

    /// Conservative quiet state for a CQ wait: no TX in use, every receive
    /// posted, no unread received bytes.
    fn wait_quiescent(&self) -> bool {
        self.peer.is_some()
            && self.tx_pending.iter().all(|pending| !pending)
            && self.rx_pending.iter().all(|pending| *pending)
            && self.rx_len.iter().all(|&len| len == 0)
            && self.rx_offset == 0
            && self.rx_read == self.rx_post
    }

    /// Arms the provider CQ wait fd for one caller-owned `poll(2)`. `Ok(None)`:
    /// not quiescent, or `fi_trywait` reported queued work (`-FI_EAGAIN`); the
    /// caller progresses normally. Any other `fi_trywait` code is fatal.
    pub(super) fn arm_wait_fd(&mut self) -> io::Result<Option<libc::c_int>> {
        let CqWaitMode::FdTryWait(fd) = self.cq_wait_mode else {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "fabric completion queue has no wait fd",
            ));
        };
        if !self.wait_quiescent() {
            return Ok(None);
        }
        // SAFETY: `fabric` and `cq` are the live handles this Resources owns and
        // this &mut self is the only user (FI_THREAD_DOMAIN); `fids` points to one
        // initialized element for the call's duration.
        let code = unsafe {
            let mut fid = &raw mut (*self.cq).fid;
            ffi::fi_trywait(self.fabric, &raw mut fid, 1)
        };
        match code {
            0 => Ok(Some(fd)),
            code if code < 0 && code.unsigned_abs() == ffi::FI_EAGAIN => Ok(None),
            code => Err(error("arm completion queue wait", code.unsigned_abs())),
        }
    }

    pub(super) fn progress_stats(&self) -> ProgressStats {
        self.progress_stats
    }
    pub(super) fn configure_progress_stats(&mut self, enabled: bool, reset: bool) {
        self.time_progress = enabled;
        self.last_poll = None;
        self.last_any_poll = None;
        if reset {
            self.progress_stats = ProgressStats::default();
        }
    }

    pub(super) fn copy_send(&mut self, slot: usize, bytes: &[IoSlice<'_>]) -> usize {
        assert!(slot < self.slots() && !self.tx_pending[slot]);
        self.forget_staged(slot);
        let began = self.time_progress.then(Instant::now);
        let mut offset = 0;
        for source in bytes {
            let len = source.len().min(CHUNK - offset);
            // SAFETY: source supplies len readable bytes and cannot alias the
            // private TX allocation. This free slot has CHUNK writable bytes;
            // offset + len <= CHUNK, with no pending provider operation.
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

    /// A write into `slot` ends any kept copy there.
    fn forget_staged(&mut self, slot: usize) {
        if self.staged_send.is_some_and(|staged| staged.slot == slot) {
            self.staged_send = None;
        }
    }

    /// Whether the kept EAGAIN copy is byte-identical to what `copy_send` would
    /// now produce for `bytes`: same length (so the post accepts exactly the
    /// bytes a fresh copy would) and same content. Compares, never copies.
    fn staged_matches(&self, staged: StagedSend, bytes: &[IoSlice<'_>]) -> bool {
        let total = bytes
            .iter()
            .fold(0_usize, |sum, slice| sum.saturating_add(slice.len()));
        if total.min(CHUNK) != staged.len {
            return false;
        }
        // SAFETY: The staged slot is not provider-owned (tx_pending is false by
        // the caller's check) and holds `staged.len <= CHUNK` initialized bytes.
        let kept = unsafe { std::slice::from_raw_parts(self.memory.tx(staged.slot), staged.len) };
        let mut offset = 0;
        for source in bytes {
            let len = source.len().min(staged.len - offset);
            if kept[offset..offset + len] != source[..len] {
                return false;
            }
            offset += len;
            if offset == staged.len {
                break;
            }
        }
        true
    }

    /// Copies `bytes` into a free slot once and posts it. On EAGAIN the filled
    /// slot is kept unposted, owning no ordinal, so a later call with the same
    /// bytes posts it again without another copy; different bytes are copied
    /// fresh into any free slot. Returns `WouldBlock` for EAGAIN or no free slot.
    pub(super) fn send_copied(&mut self, bytes: &[IoSlice<'_>]) -> io::Result<usize> {
        let reusable = self
            .staged_send
            .take()
            .filter(|staged| !self.tx_pending[staged.slot] && self.staged_matches(*staged, bytes));
        let staged = if let Some(staged) = reusable {
            self.progress_stats.send_post_retries =
                self.progress_stats.send_post_retries.saturating_add(1);
            staged
        } else {
            let slot = self.free_send_slot().ok_or(io::ErrorKind::WouldBlock)?;
            StagedSend {
                slot,
                len: self.copy_send(slot, bytes),
            }
        };
        if !self.post_send(staged.slot, staged.len)? {
            self.staged_send = Some(staged);
            return Err(io::ErrorKind::WouldBlock.into());
        }
        Ok(staged.len)
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
        // SAFETY: A terminal completion set this slot's length <= CHUNK and
        // cleared pending. The bounded range is initialized and cannot overlap
        // the caller's writable output. Repost waits until all bytes are copied.
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
            self.rx_read = (slot + 1) % self.slots();
        }
        Some(len)
    }

    pub(super) fn post_send(&mut self, slot: usize, len: usize) -> io::Result<bool> {
        assert!(slot < self.slots() && !self.tx_pending[slot] && len <= CHUNK);
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
            } else if code == -abi::<_, isize>(ffi::FI_EAGAIN) {
                stats.send_post_would_block = stats.send_post_would_block.saturating_add(1);
            } else {
                stats.send_post_errors = stats.send_post_errors.saturating_add(1);
            }
        }
        if code == 0 {
            self.record_send_bucket(len);
        } else if code == -abi::<_, isize>(ffi::FI_EAGAIN) {
            self.progress_stats.send_post_eagain =
                self.progress_stats.send_post_eagain.saturating_add(1);
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

    /// Always-on posted-length histogram; never reads a clock.
    fn record_send_bucket(&mut self, len: usize) {
        let stats = &mut self.progress_stats;
        let bucket = match len {
            0..=4096 => &mut stats.send_size_le_4k,
            4097..=16_384 => &mut stats.send_size_le_16k,
            16_385..=65_536 => &mut stats.send_size_le_64k,
            _ => &mut stats.send_size_gt_64k,
        };
        *bucket = bucket.saturating_add(1);
    }

    pub(super) fn fill_receives(&mut self) -> io::Result<()> {
        for _ in 0..self.slots() {
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
        self.rx_post = (slot + 1) % self.slots();
        Ok(true)
    }

    fn post_receive(&mut self, slot: usize) -> io::Result<bool> {
        let peer = self.peer.ok_or_else(|| invalid("missing fabric peer"))?;
        let began = self.time_progress.then(Instant::now);
        // SAFETY: try_post_receive selects an in-bounds free slot with no unread
        // bytes or Rust borrows. The endpoint, peer and optional MR are live;
        // allocation and context stay fixed until completion or successful close.
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
            } else if code == -abi::<_, isize>(ffi::FI_EAGAIN) {
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
        let can_wait = self.supports_idle_wait() && self.wait_quiescent();
        if !can_wait
            || deadline.saturating_duration_since(Instant::now()) < Duration::from_millis(1)
        {
            return self.poll().map(|progress| (progress, false));
        }
        match self.cq_wait_mode {
            CqWaitMode::FdTryWait(fd) => self.park_on_wait_fd(fd),
            CqWaitMode::SoftwareBounded | CqWaitMode::Polling => self
                .poll_with_timeout::<true>(1)
                .map(|progress| (progress, true)),
        }
    }

    /// Blocks for at most one millisecond on the provider's CQ wait fd, only
    /// after `fi_trywait` proves no completion is already queued. A nonzero
    /// `fi_trywait` (EAGAIN: provider has work to progress) polls normally
    /// instead. Returns `(progress, parked)`.
    fn park_on_wait_fd(&mut self, fd: libc::c_int) -> io::Result<(bool, bool)> {
        // SAFETY: `fabric` and `cq` are the live handles this Resources owns and
        // this &mut self is the only user (FI_THREAD_DOMAIN); `fids` points to one
        // initialized element for the call's duration.
        let code = unsafe {
            let mut fid = &raw mut (*self.cq).fid;
            ffi::fi_trywait(self.fabric, &raw mut fid, 1)
        };
        if code != 0 {
            return self.poll().map(|progress| (progress, false));
        }
        let mut descriptor = libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: one initialized stack pollfd is writable for this call; poll
        // retains neither the pointer nor the descriptor, which the CQ keeps open.
        let ready = unsafe { libc::poll(&raw mut descriptor, 1, 1) };
        if ready < 0 && io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            return Err(io::Error::last_os_error());
        }
        if descriptor.revents & libc::POLLNVAL != 0 {
            return Err(invalid("fabric CQ wait descriptor is invalid"));
        }
        self.progress_stats.cq_idle_waits = self.progress_stats.cq_idle_waits.saturating_add(1);
        self.poll().map(|progress| (progress, true))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Keep each CQ pass and its message context retirement in one ordered block."
    )]
    fn poll_with_timeout<const WAIT: bool>(&mut self, timeout_ms: i32) -> io::Result<bool> {
        if self.time_progress {
            self.progress_stats.tx_all_slots_pending_polls = self
                .progress_stats
                .tx_all_slots_pending_polls
                .saturating_add(u64::from(self.tx_pending.iter().all(|pending| *pending)));
            self.progress_stats.rx_no_posted_slots_polls = self
                .progress_stats
                .rx_no_posted_slots_polls
                .saturating_add(u64::from(self.rx_pending.iter().all(|pending| !*pending)));
            self.progress_stats.rx_unread_slots_polls = self
                .progress_stats
                .rx_unread_slots_polls
                .saturating_add(u64::from(self.rx_len.iter().any(|&len| len != 0)));
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
        let mut entries = [MaybeUninit::<ffi::fi_cq_msg_entry>::uninit(); 2 * MAX_SLOTS];
        // At most one CQE per TX slot and per RX slot can be outstanding.
        let capacity = 2 * self.slots();
        let initial_progress = (!self.initial_progress_recorded).then(Instant::now);
        let began = self.time_progress.then(Instant::now);
        // SAFETY: Our CQ uses MSG format and may initialize at most `capacity <=
        // entries.len()` entries; no Rust references to this storage cross the
        // synchronous call.
        let count = unsafe {
            if WAIT {
                ffi::fi_cq_sread(
                    self.cq,
                    entries.as_mut_ptr().cast(),
                    capacity,
                    ptr::null(),
                    timeout_ms,
                )
            } else {
                ffi::fi_cq_read(self.cq, entries.as_mut_ptr().cast(), capacity)
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
        if count == -abi::<_, isize>(ffi::FI_EAGAIN)
            || count == 0
            || WAIT
                && (count == -abi::<_, isize>(ffi::FI_ETIMEDOUT)
                    || count == -abi::<_, isize>(libc::EINTR))
        {
            self.progress_stats.cq_empty_polls =
                self.progress_stats.cq_empty_polls.saturating_add(1);
            return Ok(false);
        }
        if count == -abi::<_, isize>(ffi::FI_EAVAIL) {
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
            return Err(error("fabric completion", code));
        }
        let count = usize::try_from(count).map_err(|_| {
            error(
                "fabric completion queue",
                u32::try_from(count.unsigned_abs()).unwrap_or(ffi::FI_EIO),
            )
        })?;
        if count > capacity {
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
        if let Some(slot) = self.memory.tx_context_slot(context)
            && self.tx_pending[slot]
        {
            self.tx_pending[slot] = false;
            // Every accepted ordinal not in a pending slot has completed.
            // Taking the oldest live ordinal handles arbitrary CQ order and
            // slot reuse with fixed O(slots) state, even if one send stalls.
            self.completed_send = self
                .tx_pending
                .iter()
                .zip(self.tx_ordinal.iter())
                .filter(|(pending, _)| **pending)
                .map(|(_, ordinal)| ordinal - 1)
                .min()
                .unwrap_or(self.accepted_send);
            return Ok(());
        }
        if let Some(slot) = self.memory.rx_context_slot(context)
            && self.rx_pending[slot]
        {
            let len = receive_len();
            if len == 0 || len > CHUNK {
                return Err(invalid("invalid fabric receive length"));
            }
            self.rx_pending[slot] = false;
            self.rx_len[slot] = len;
            return Ok(());
        }
        Err(invalid("unexpected fabric completion"))
    }

    pub(super) fn close(&mut self) -> Result<(), i32> {
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
    } else if code == -abi::<_, isize>(ffi::FI_EAGAIN) {
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
    fn message_hints_request_sas_ordering_without_application_queue_sizes() {
        for provider in [c"verbs;ofi_rxm", c"tcp;ofi_rxm"] {
            let hints = Info::hints(provider, c"").unwrap();
            // SAFETY: Info owns the live, fully initialized hint allocation.
            let (tx, rx) = unsafe { (&*(*hints.0).tx_attr, &*(*hints.0).rx_attr) };
            assert_eq!(tx.size, 0);
            assert_eq!(rx.size, 0);
            assert_eq!(tx.msg_order, u64::from(ffi::FI_ORDER_SAS));
            assert_eq!(rx.msg_order, u64::from(ffi::FI_ORDER_SAS));
        }
        let mut state = Resources {
            tx_pending: PerSlot(vec![true; SLOTS]),
            ..Resources::default()
        };
        assert_eq!(state.free_send_slot(), None);

        state.tx_pending[SLOTS - 1] = false;
        assert_eq!(state.free_send_slot(), Some(SLOTS - 1));
    }

    #[test]
    fn receive_delivery_follows_posting_order_not_completion_order() {
        let mut state = Resources {
            rx_pending: PerSlot(vec![true; SLOTS]),
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
            tx_pending: PerSlot(vec![true; SLOTS]),
            tx_ordinal: PerSlot(vec![1, 2, 3, 4]),
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
}
