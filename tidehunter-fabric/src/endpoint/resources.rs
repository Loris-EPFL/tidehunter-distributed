//! Native resources and raw ownership of memory accessed by the provider.

#[cfg(test)]
mod nonblocking_tests;

mod efa;
mod efa_direct;

use super::{CHUNK, OpenProfile, ProgressStats, SLOTS};
use ofi_libfabric_sys::bindgen as ffi;
use std::{
    cell::UnsafeCell,
    ffi::{CStr, CString, c_void},
    io::{self, IoSlice},
    mem::MaybeUninit,
    ptr,
    ptr::NonNull,
    time::Instant,
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
    pub(super) open_profile: OpenProfile,
    initial_progress_recorded: bool,
    time_progress: bool,
    last_poll: Option<Instant>,
    last_any_poll: Option<Instant>,
    rx_pending: [bool; SLOTS],
    rx_len: [usize; SLOTS],
    rx_offset: usize,
    rx_read: usize,
    rx_post: usize,
    direct: bool,
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
    fn hints(provider: &CStr, domain: &CStr, direct: bool) -> io::Result<Self> {
        // SAFETY: No arguments. Info owns the result and releases it on all paths.
        let hints = Info(unsafe { ffi::fi_allocinfo() });
        if hints.0.is_null() {
            return Err(error("allocate fabric hints", ffi::FI_ENOMEM));
        }
        // SAFETY: fi_allocinfo initializes all nested attribute allocations.
        // strdup gives ownership of matching C-allocator strings to fi_freeinfo.
        unsafe {
            (*(*hints.0).ep_attr).type_ = ffi::fi_ep_type_FI_EP_RDM;
            (*hints.0).caps =
                u64::from(ffi::FI_MSG) | if direct { 0 } else { ffi::FI_DIRECTED_RECV };
            (*hints.0).mode = if direct {
                ffi::FI_CONTEXT2
            } else {
                ffi::FI_CONTEXT
            };
            (*(*hints.0).domain_attr).threading = ffi::fi_threading_FI_THREAD_DOMAIN;
            (*(*hints.0).domain_attr).__bindgen_anon_1.data_progress = if direct {
                ffi::fi_progress_FI_PROGRESS_AUTO
            } else {
                ffi::fi_progress_FI_PROGRESS_MANUAL
            };
            // EFA-direct requires allocated local registrations. Ordinary EFA's
            // MR request remains unchanged.
            (*(*hints.0).domain_attr).mr_mode = i32::try_from(if direct {
                ffi::FI_MR_LOCAL | ffi::FI_MR_ALLOCATED
            } else {
                ffi::FI_MR_LOCAL
            })
            .unwrap();
            if !direct {
                (*(*hints.0).tx_attr).msg_order = u64::from(ffi::FI_ORDER_SAS);
                (*(*hints.0).rx_attr).msg_order = u64::from(ffi::FI_ORDER_SAS);
            }
            if provider.to_bytes() == b"efa" && !direct {
                (*(*hints.0).tx_attr).size = efa::QUEUE;
                (*(*hints.0).rx_attr).size = efa::QUEUE;
            } else if direct {
                (*(*hints.0).tx_attr).size = SLOTS;
                (*(*hints.0).rx_attr).size = SLOTS;
            }
            (*(*hints.0).fabric_attr).prov_name = libc::strdup(provider.as_ptr());
            if (*(*hints.0).fabric_attr).prov_name.is_null() {
                return Err(error("allocate provider name", ffi::FI_ENOMEM));
            }
            if direct {
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

impl Resources {
    pub(super) fn open(&mut self, provider: &str, domain: Option<&str>) -> io::Result<()> {
        self.open_mode(provider, domain, false)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Keep validated provider info and partial resource ownership in one ordered setup path"
    )]
    pub(super) fn open_mode(
        &mut self,
        provider: &str,
        domain: Option<&str>,
        direct: bool,
    ) -> io::Result<()> {
        self.direct = direct;
        let discovery = Instant::now();
        let is_efa = provider == "efa";
        if is_efa && !direct {
            efa::check_environment()?;
            self.open_profile.requested_tx = efa::QUEUE;
            self.open_profile.requested_rx = efa::QUEUE;
        } else if direct {
            self.open_profile.requested_tx = SLOTS;
            self.open_profile.requested_rx = SLOTS;
        }
        let provider = CString::new(provider).map_err(invalid)?;
        let domain = CString::new(domain.unwrap_or_default()).map_err(invalid)?;
        let hints = Info::hints(&provider, &domain, direct)?;
        // EFA >= 1.18 enables device RDMA by default on supporting hardware.
        let api = (1 << 16)
            | if provider.as_bytes() == b"efa" {
                18
            } else {
                11
            };
        let mut info = Info(ptr::null_mut());
        // SAFETY: Hints and output storage are live through this synchronous call.
        let discovered =
            unsafe { ffi::fi_getinfo(api, ptr::null(), ptr::null(), 0, hints.0, &raw mut info.0) };
        if direct && discovered != 0 {
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
            return Err(invalid(if direct {
                "efa-direct hardware unavailable: no direct fabric info"
            } else {
                "fabric returned no provider info"
            }));
        }
        // SAFETY: Successful getinfo returns populated attribute structures.
        // Their ownership remains in Info until all handle creation completes.
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
            let provider_name = (*selected.fabric_attr).prov_name;
            let fabric_name = (*selected.fabric_attr).name;
            let mr_mode = (*selected.domain_attr).mr_mode;
            let supported_mr_modes = if direct {
                ffi::FI_MR_LOCAL | ffi::FI_MR_PROV_KEY | ffi::FI_MR_VIRT_ADDR | ffi::FI_MR_ALLOCATED
            } else {
                ffi::FI_MR_LOCAL | ffi::FI_MR_PROV_KEY
            };
            let valid_identity = if direct {
                !provider_name.is_null()
                    && CStr::from_ptr(provider_name).to_bytes() == b"efa"
                    && !fabric_name.is_null()
                    && CStr::from_ptr(fabric_name).to_bytes() == b"efa-direct"
            } else {
                !provider_name.is_null() && CStr::from_ptr(provider_name) == provider.as_c_str()
            };
            let valid_direct_geometry =
                if direct && !selected.nic.is_null() && !(*selected.nic).link_attr.is_null() {
                    efa_direct::validate(
                        (*selected.fabric_attr).prov_version,
                        (*selected.ep_attr).protocol,
                        (*selected.ep_attr).protocol_version,
                        (*selected.ep_attr).max_msg_size,
                        (*selected.tx_attr).size,
                        (*selected.rx_attr).size,
                        selected.mode,
                        (*selected.domain_attr).__bindgen_anon_1.data_progress,
                        mr_mode,
                        (*(*selected.nic).link_attr).mtu,
                    )?
                } else {
                    !direct
                };
            let valid_normal = direct
                || ((*selected.ep_attr).max_msg_size >= CHUNK
                    && selected.caps & (u64::from(ffi::FI_MSG) | ffi::FI_DIRECTED_RECV)
                        == (u64::from(ffi::FI_MSG) | ffi::FI_DIRECTED_RECV)
                    && (*selected.tx_attr).msg_order & u64::from(ffi::FI_ORDER_SAS) != 0
                    && (*selected.rx_attr).msg_order & u64::from(ffi::FI_ORDER_SAS) != 0
                    && selected.mode & !ffi::FI_CONTEXT == 0);
            if !valid_identity
                || !valid_direct_geometry
                || !valid_normal
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
                || (direct && mr_mode & i32::try_from(ffi::FI_MR_LOCAL).unwrap() == 0)
                || (direct
                    && ((*selected.tx_attr).mode | (*selected.rx_attr).mode)
                        & !(ffi::FI_CONTEXT2 | ffi::FI_CONTEXT)
                        != 0)
            {
                return Err(invalid(if direct {
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
            if is_efa && !direct {
                efa::validate(
                    self.open_profile,
                    (*selected.ep_attr).protocol,
                    (*selected.ep_attr).protocol_version,
                )?;
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
            self.open_endpoint(info.0)?;
            let stage = Instant::now();
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
        }
        Ok(())
    }

    fn open_endpoint(&mut self, info: *mut ffi::fi_info) -> io::Result<()> {
        let mut cq = ffi::fi_cq_attr {
            size: 2 * SLOTS,
            format: ffi::fi_cq_format_FI_CQ_FORMAT_MSG,
            wait_obj: ffi::fi_wait_obj_FI_WAIT_NONE,
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
            check(
                "open completion queue",
                ffi::fi_cq_open(self.domain, &raw mut cq, &raw mut self.cq, ptr::null_mut()),
            )?;
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
        let count = unsafe { ffi::fi_cq_read(self.cq, entries.as_mut_ptr().cast(), entries.len()) };
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
        if count == -isize::try_from(ffi::FI_EAGAIN).unwrap() || count == 0 {
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
            return Err(error(
                "fabric completion",
                if count > 0 {
                    failure.err.unsigned_abs()
                } else {
                    ffi::FI_EIO
                },
            ));
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
            let hints = Info::hints(provider, c"", false).unwrap();
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
        let hints = Info::hints(c"efa", c"", true).unwrap();
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
