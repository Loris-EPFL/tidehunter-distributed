//! Protocol-v10 exclusive-owner progress. Adapted mechanisms (not copied code):
//! eRPC §§3.1–3.2, 4.2.2, 4.3; erpc-io/eRPC at
//! de83dab3eab4a0fb19bfc4881c11d4a6b89ff17d, src/session.h and
//! src/rpc_impl/{rpc_ev_loop,rpc_resp}.cc. IDs name live slots, not durability.
//! ReFlex §§3.1/4.1: bounded two-stage progress, never disk waits on this owner.
//! HERD §4.1.1: advance ready pipeline work without waiting for another arrival.
//! Here newly admitted resident reads and their bounded output turn stay in the
//! same sweep; cold io_uring/worker execution and manual CQ ownership do not move.
//! Provider contract: libfabric 3cc1095272b07ebeb5cc8f44851af6aa3eb75e41,
//! man/fi_efa.7.md Progress: normal `efa` RDM requires manual CQ progress;
//! it does not require a TCP syscall per CQ poll.
//! Adaptive TCP parks only a drained owner on socket readability, never on a
//! periodic sleep. Adaptive fabric keeps manual progress even with no RPCs.
//! eRPC §3.2 / ReFlex §§3.1, 4.1 motivate keeping resident completion and reply
//! submission inline while cold I/O remains asynchronous, not weakening sync
//! durability. Inventory try-locks for admission/response epoch validation and
//! node-wide read cache/admission locks remain; no storage lock is removed.
//! https://www.usenix.org/system/files/nsdi19-kalia.pdf
//! ReFlex source: papers/parsed/ReFlex-2017.md, §§3.1 and 4.1.
use super::*;
use std::collections::{HashMap, VecDeque};
use std::net::Shutdown;
use std::os::fd::AsRawFd;
use std::sync::mpsc;

#[cfg(test)]
#[path = "reactor_output_tests.rs"]
mod output_tests;
#[cfg(test)]
#[path = "reactor_session_tests.rs"]
mod session_tests;

pub(super) const TLS_BUFFER_BYTES: usize = 256 * 1024;
const SESSION_REQUESTS: usize = 32;
const SESSION_BYTES: usize = 256 * 1024 * 1024;
const PROGRESS_BYTES: usize = 64 * 1024;

#[cfg(feature = "experimental-fabric")]
static NEXT_DIRECT_DIAGNOSTIC_WINDOW: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(1);

#[cfg(feature = "experimental-fabric")]
struct DirectDiagnosticWindow {
    id: u64,
    data_posts: u64,
    credit_posts: u64,
    piggyback_advances: u64,
}

/// A live fabric control socket is checked at least once per millisecond of
/// active progress, plus one bounded sweep/scheduler delay. An idle established
/// session has no silent-partition detection deadline; pending work and partial
/// frames remain bounded by their operation and I/O deadlines.
const CONTROL_PEER_CHECK_INTERVAL: Duration = Duration::from_millis(1);

/// Socket readiness ends the wait; OS scheduling can still add delay. This is
/// the maximum requested wait before checking the atomic shutdown flag again,
/// not an arrival latency floor or a scheduler-latency guarantee.
const IDLE_SHUTDOWN_CHECK_INTERVAL: Duration = Duration::from_millis(10);

fn idle_wait_timeout(deadline: Instant, now: Instant) -> libc::c_int {
    // poll(2) takes milliseconds. Round DOWN rather than extending a deadline;
    // the final sub-millisecond turn remains nonblocking.
    deadline
        .saturating_duration_since(now)
        .min(IDLE_SHUTDOWN_CHECK_INTERVAL)
        .as_millis() as libc::c_int
}

/// One allocation-free readiness wait. EINTR returns to the owner so shutdown
/// and deadlines are rechecked; EOF/errors are handled by ordinary TLS progress.
fn wait_for_readable(socket: &TcpStream, deadline: Instant) -> io::Result<bool> {
    let mut descriptor = libc::pollfd {
        fd: socket.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let timeout = idle_wait_timeout(deadline, Instant::now());
    // SAFETY: one initialized stack pollfd is writable for this call, and the
    // borrowed socket keeps its fd live. poll retains neither pointer nor fd.
    // std has no equivalent readiness wait; libc is already a dependency.
    let ready = unsafe { libc::poll(&mut descriptor, 1, timeout) };
    if ready < 0 {
        let error = io::Error::last_os_error();
        return if error.kind() == io::ErrorKind::Interrupted {
            Ok(false)
        } else {
            Err(error)
        };
    }
    if descriptor.revents & libc::POLLNVAL != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "idle session socket is invalid",
        ));
    }
    Ok(ready != 0)
}

struct ControlPeerHealth {
    next_check: Instant,
}
impl ControlPeerHealth {
    fn check(&mut self, socket: &TcpStream, now: Instant, force: bool) -> io::Result<bool> {
        if !force && now < self.next_check {
            return Ok(false);
        }
        match socket.peek(&mut [0]) {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "fabric control peer closed",
                ));
            }
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected fabric control data",
                ));
            }
        }
        self.next_check = now + CONTROL_PEER_CHECK_INTERVAL;
        Ok(true)
    }
}

/// Scheduling policy affects only this bounded network-owner sweep, not locks
/// in the storage engine, TLS, provider or OS.
#[derive(Default)]
pub(super) struct ActivePoll {
    spins: u8,
}
impl ActivePoll {
    /// Returns (scheduler yield, spin hint) for opt-in diagnostics.
    pub(super) fn pause(
        &mut self,
        policy: PollingPolicy,
        fabric: bool,
        progressed: bool,
    ) -> (bool, bool) {
        if progressed {
            self.spins = 0;
            (false, false)
        } else if policy == PollingPolicy::Busy {
            std::hint::spin_loop();
            (false, true)
        } else if fabric && self.spins < 64 {
            self.spins += 1;
            std::hint::spin_loop();
            (false, true)
        } else {
            self.spins = 0;
            thread::yield_now();
            (true, false)
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(super) struct Envelope<T> {
    pub(super) protocol: u32,
    pub(super) id: u64,
    pub(super) body: T,
}

struct Output {
    id: u64,
    bytes: Vec<u8>,
    /// Native record replies retain owned frame segments until TX-slot copy.
    segments: Option<Vec<minibytes::Bytes>>,
    offset: usize,
    len: usize,
    retained_bytes: usize,
    #[cfg(feature = "experimental-fabric")]
    segment_index: usize,
    #[cfg(feature = "experimental-fabric")]
    segment_offset: usize,
    fence: Option<u64>,
    /// Opt-in phase clock: offset zero = queued, no fence = submitting,
    /// fence present = awaiting local retirement. Reset never changes ownership.
    stage_started: Option<Instant>,
}
impl Output {
    #[cfg(feature = "experimental-fabric")]
    fn advance(&mut self, mut amount: usize) {
        self.offset += amount;
        let Some(segments) = &self.segments else {
            return;
        };
        while amount != 0 {
            let remaining = segments[self.segment_index].len() - self.segment_offset;
            if amount < remaining {
                self.segment_offset += amount;
                return;
            }
            amount -= remaining;
            self.segment_index += 1;
            self.segment_offset = 0;
        }
    }
    #[cfg(feature = "experimental-fabric")]
    fn native_slices<'a>(
        &'a self,
        budget: usize,
        slices: &mut [io::IoSlice<'a>],
    ) -> (usize, usize) {
        let mut count = 0;
        let mut total = 0;
        if let Some(segments) = &self.segments {
            let mut index = self.segment_index;
            let mut offset = self.segment_offset;
            while count < slices.len() && total < budget && index < segments.len() {
                let bytes = &segments[index][offset..];
                let len = bytes.len().min(budget - total);
                if len != 0 {
                    slices[count] = io::IoSlice::new(&bytes[..len]);
                    count += 1;
                    total += len;
                }
                index += 1;
                offset = 0;
            }
        } else if self.offset < self.bytes.len() && !slices.is_empty() {
            let len = (self.bytes.len() - self.offset).min(budget);
            slices[0] = io::IoSlice::new(&self.bytes[self.offset..self.offset + len]);
            count = 1;
            total = len;
        }
        (count, total)
    }
    #[cfg(test)]
    fn flatten(&self) -> Vec<u8> {
        if let Some(segments) = &self.segments {
            let mut bytes = Vec::with_capacity(self.len);
            for segment in segments {
                bytes.extend_from_slice(segment.as_ref());
            }
            bytes
        } else {
            self.bytes.clone()
        }
    }
}

#[cfg(feature = "experimental-fabric")]
const MAX_NATIVE_IOV: usize = 514;
const OUTPUT_FRAME_BUDGET: usize = 4;
const OUTPUT_TLS_WRITE_BUDGET: usize = 8;

#[derive(Clone, Copy, Debug)]
struct OutputBudget {
    plaintext: usize,
    tls_writes: usize,
    frames: usize,
    frame_gate_observed: bool,
}

impl OutputBudget {
    fn reset(&mut self) {
        self.plaintext = PROGRESS_BYTES;
        self.tls_writes = OUTPUT_TLS_WRITE_BUDGET;
        self.frames = OUTPUT_FRAME_BUDGET;
        self.frame_gate_observed = false;
    }
}

/// Framing and optional TLS state have one mutable owner. Endpoint's fixed
/// slots own DMA-visible copies; request/result vectors never are DMA targets.
pub(super) struct Session {
    conn: Option<rustls::Connection>,
    pub(super) sock: DeadlineIo,
    output: VecDeque<Output>,
    /// Prefix already accepted by transport, still charged until fence retirement.
    submitted: usize,
    output_budget: OutputBudget,
    closed: bool,
    stats: TransportProgressStats,
    time_work: bool,
    output_bytes: usize,
    spare_output: Vec<u8>,
    header: [u8; 4],
    header_used: usize,
    input: Vec<u8>,
    input_used: usize,
    input_len: usize,
    input_started: Option<Instant>,
    plaintext_pending: usize,
    peer_closed: bool,
    control_peer: ControlPeerHealth,
    #[cfg(feature = "experimental-fabric")]
    direct_diagnostic: Option<DirectDiagnosticWindow>,
}
impl Session {
    pub(super) fn new(mut conn: rustls::Connection, sock: DeadlineIo) -> Result<Self, TcpError> {
        sock.socket.set_nonblocking(true)?;
        let tls_state = conn.process_new_packets().map_err(TcpError::component)?;
        Ok(Self {
            conn: Some(conn),
            sock,
            output: VecDeque::new(),
            submitted: 0,
            output_budget: OutputBudget {
                plaintext: PROGRESS_BYTES,
                tls_writes: OUTPUT_TLS_WRITE_BUDGET,
                frames: OUTPUT_FRAME_BUDGET,
                frame_gate_observed: false,
            },
            closed: false,
            stats: TransportProgressStats::default(),
            time_work: false,
            output_bytes: 0,
            header: [0; 4],
            header_used: 0,
            input: Vec::new(),
            input_used: 0,
            input_len: 0,
            input_started: None,
            plaintext_pending: tls_state.plaintext_bytes_to_read(),
            peer_closed: tls_state.peer_has_closed(),
            spare_output: Vec::new(),
            control_peer: ControlPeerHealth {
                next_check: Instant::now(),
            },
            #[cfg(feature = "experimental-fabric")]
            direct_diagnostic: None,
        })
    }
    #[cfg(feature = "experimental-fabric")]
    pub(super) fn new_native(sock: DeadlineIo) -> Result<Self, TcpError> {
        if sock.fabric.is_none() {
            return Err(TcpError::new("native session requires a fabric endpoint"));
        }
        sock.socket.set_nonblocking(true)?;
        Ok(Self {
            conn: None,
            sock,
            output: VecDeque::new(),
            submitted: 0,
            output_budget: OutputBudget {
                plaintext: PROGRESS_BYTES,
                tls_writes: OUTPUT_TLS_WRITE_BUDGET,
                frames: OUTPUT_FRAME_BUDGET,
                frame_gate_observed: false,
            },
            closed: false,
            stats: TransportProgressStats::default(),
            time_work: false,
            output_bytes: 0,
            header: [0; 4],
            header_used: 0,
            input: Vec::new(),
            input_used: 0,
            input_len: 0,
            input_started: None,
            plaintext_pending: 0,
            peer_closed: false,
            spare_output: Vec::new(),
            control_peer: ControlPeerHealth {
                next_check: Instant::now(),
            },
            direct_diagnostic: None,
        })
    }
    fn queue<T: Serialize>(&mut self, id: u64, body: &T, limit: usize) -> Result<(), TcpError> {
        let began = self.time_work.then(Instant::now);
        let result = self.queue_inner(id, body, limit);
        let bytes = result
            .as_ref()
            .map(|()| self.output.back().expect("queued frame").len);
        self.stats.frame_encode.record(began, &bytes);
        result
    }

    fn queue_response(
        &mut self,
        id: u64,
        response: Response,
        limit: usize,
    ) -> Result<(), TcpError> {
        #[cfg(feature = "experimental-fabric")]
        if self.conn.is_none() {
            let began = self.time_work.then(Instant::now);
            let result = match response {
                Response::Records(records) => {
                    self.queue_native_records(id, records, None, None, limit)
                }
                Response::RecordsProfiled { records, profile } => {
                    self.queue_native_records(id, records, None, Some(profile), limit)
                }
                Response::ResidentRecords { grants, records } => {
                    self.queue_native_records(id, records, Some(grants), None, limit)
                }
                Response::ResidentRecordsProfiled {
                    grants,
                    records,
                    profile,
                } => self.queue_native_records(id, records, Some(grants), Some(profile), limit),
                response => return self.queue(id, &response, limit),
            };
            let measured = result
                .as_ref()
                .map(|_| self.output.back().map_or(0, |output| output.len));
            self.stats.frame_encode.record(began, &measured);
            return result;
        }
        self.queue(id, &response, limit)
    }

    #[cfg(feature = "experimental-fabric")]
    fn queue_native_records(
        &mut self,
        id: u64,
        records: Vec<WireBytes>,
        grants: Option<Vec<Option<ResidentGrant>>>,
        profile: Option<ServerReadProfile>,
        limit: usize,
    ) -> Result<(), TcpError> {
        if self.closed {
            return Err(TcpError::new("session is closed"));
        }
        // Serialize an empty response to obtain its exact envelope and enum
        // prefix. Remove only the fixed-width empty sequence count, then insert
        // codec-encoded lengths alongside the original owned record buffers.
        let (mut prefix, tail) = if let Some(profile) = profile {
            let body = match grants.as_ref() {
                Some(grants) => Response::ResidentRecordsProfiled {
                    grants: grants.clone(),
                    records: Vec::new(),
                    profile,
                },
                None => Response::RecordsProfiled {
                    records: Vec::new(),
                    profile,
                },
            };
            let empty = Envelope {
                protocol: PROTOCOL,
                id,
                body,
            };
            let mut encoded = wire_codec(limit)
                .serialize(&empty)
                .map_err(TcpError::component)?;
            let tail = wire_codec(limit)
                .serialize(&profile)
                .map_err(TcpError::component)?;
            let prefix_len = encoded
                .len()
                .checked_sub(tail.len() + 8)
                .ok_or_else(|| TcpError::new("invalid profiled response encoding"))?;
            let tail = encoded.split_off(prefix_len + 8);
            encoded.truncate(prefix_len);
            (encoded, tail)
        } else {
            let body = match grants.as_ref() {
                Some(grants) => Response::ResidentRecords {
                    grants: grants.clone(),
                    records: Vec::new(),
                },
                None => Response::Records(Vec::new()),
            };
            let empty = Envelope {
                protocol: PROTOCOL,
                id,
                body,
            };
            let mut encoded = wire_codec(limit)
                .serialize(&empty)
                .map_err(TcpError::component)?;
            let prefix_len = encoded
                .len()
                .checked_sub(8)
                .ok_or_else(|| TcpError::new("invalid records response encoding"))?;
            encoded.truncate(prefix_len);
            (encoded, Vec::new())
        };
        let payload_bytes = records.iter().try_fold(0usize, |sum, record| {
            sum.checked_add(record.as_slice().len())
                .ok_or_else(|| TcpError::new("record response length overflow"))
        })?;
        let member_headers = records
            .len()
            .checked_mul(8)
            .ok_or_else(|| TcpError::new("record response length overflow"))?;
        let body_len = prefix
            .len()
            .checked_add(8)
            .and_then(|size| size.checked_add(member_headers))
            .and_then(|size| size.checked_add(payload_bytes))
            .and_then(|size| size.checked_add(tail.len()))
            .ok_or_else(|| TcpError::new("record response length overflow"))?;
        if body_len == 0 || body_len > limit || body_len > u32::MAX as usize {
            return Err(TcpError::new("frame length exceeds limit"));
        }
        let total_len = body_len + 4;
        let segment_count = records
            .len()
            .checked_mul(2)
            .and_then(|count| count.checked_add(1 + usize::from(!tail.is_empty())))
            .ok_or_else(|| TcpError::new("record response metadata overflow"))?;
        let owned_member_count = records
            .iter()
            .filter(|record| matches!(record, WireBytes::Owned(_)))
            .count();
        let owned_capacity_slack = records
            .iter()
            .try_fold(0usize, |sum, record| match record {
                WireBytes::Owned(bytes) => {
                    sum.checked_add(bytes.capacity().saturating_sub(bytes.len()))
                }
                WireBytes::Shared(_) => Some(sum),
            })
            .ok_or_else(|| TcpError::new("record response metadata overflow"))?;
        let owner_count = owned_member_count
            .checked_add(1)
            .ok_or_else(|| TcpError::new("record response metadata overflow"))?;
        let metadata_bytes = segment_count
            .checked_mul(std::mem::size_of::<minibytes::Bytes>())
            .and_then(|bytes| bytes.checked_add(owner_count.checked_mul(64)?))
            .and_then(|bytes| bytes.checked_add(owned_capacity_slack))
            .ok_or_else(|| TcpError::new("record response metadata overflow"))?;
        let retained_bytes = total_len
            .checked_add(metadata_bytes)
            .ok_or_else(|| TcpError::new("record response length overflow"))?;
        if self.output.len() == SESSION_REQUESTS
            || retained_bytes > SESSION_BYTES.saturating_sub(self.output_bytes)
        {
            return Err(TcpError::new("session output credit exceeded"));
        }

        let metadata_len = total_len
            .checked_sub(payload_bytes)
            .ok_or_else(|| TcpError::new("record response length overflow"))?;
        let mut metadata = Vec::with_capacity(metadata_len);
        metadata.extend_from_slice(&(body_len as u32).to_be_bytes());
        metadata.append(&mut prefix);
        wire_codec(limit)
            .serialize_into(&mut metadata, &(records.len() as u64))
            .map_err(TcpError::component)?;
        let records_start = metadata.len();
        for record in &records {
            wire_codec(limit)
                .serialize_into(&mut metadata, &(record.as_slice().len() as u64))
                .map_err(TcpError::component)?;
        }
        let profile_start = metadata.len();
        metadata.extend_from_slice(&tail);
        let metadata_slack = metadata.capacity().saturating_sub(metadata.len());
        let metadata_owner = minibytes::Bytes::from(metadata);
        let mut segments = Vec::with_capacity(segment_count);
        let segment_slack = segments
            .capacity()
            .saturating_sub(segment_count)
            .saturating_mul(std::mem::size_of::<minibytes::Bytes>());
        // All lengths are fixed-width u64s under wire_codec's fixint encoding.
        let mut record_length_offset = records_start;
        segments.push(metadata_owner.slice(0..records_start));
        for record in records {
            let length_end = record_length_offset + 8;
            segments.push(metadata_owner.slice(record_length_offset..length_end));
            record_length_offset = length_end;
            segments.push(record.into_bytes());
        }
        if profile_start < metadata_owner.len() {
            segments.push(metadata_owner.slice(profile_start..metadata_owner.len()));
        }
        let retained_bytes = retained_bytes
            .checked_add(metadata_slack)
            .and_then(|bytes| bytes.checked_add(segment_slack))
            .ok_or_else(|| TcpError::new("record response metadata overflow"))?;
        if retained_bytes > SESSION_BYTES.saturating_sub(self.output_bytes) {
            return Err(TcpError::new("session output credit exceeded"));
        }
        self.output_bytes += retained_bytes;
        self.output.push_back(Output {
            id,
            bytes: Vec::new(),
            segments: Some(segments),
            len: total_len,
            retained_bytes,
            offset: 0,
            segment_index: 0,
            segment_offset: 0,
            fence: None,
            stage_started: self.time_work.then(Instant::now),
        });
        self.stats.peak_output_frames = self.stats.peak_output_frames.max(self.output.len() as u64);
        self.stats.peak_output_bytes = self.stats.peak_output_bytes.max(self.output_bytes as u64);
        Ok(())
    }
    fn queue_size<T: Serialize>(&self, id: u64, body: &T, limit: usize) -> Result<usize, TcpError> {
        if self.closed {
            return Err(TcpError::new("session is closed"));
        }
        let message = Envelope {
            protocol: PROTOCOL,
            id,
            body,
        };
        let size = wire_codec(limit)
            .serialized_size(&message)
            .map_err(TcpError::component)? as usize;
        if size == 0 || size > limit {
            return Err(TcpError::new("frame length exceeds limit"));
        }
        Ok(size)
    }
    fn can_queue(&self, size: usize) -> bool {
        self.output.len() < SESSION_REQUESTS
            && self.spare_output.capacity().max(size.saturating_add(4))
                <= SESSION_BYTES.saturating_sub(self.output_bytes)
    }
    fn queue_inner<T: Serialize>(
        &mut self,
        id: u64,
        body: &T,
        limit: usize,
    ) -> Result<(), TcpError> {
        if self.closed {
            return Err(TcpError::new("session is closed"));
        }
        let message = Envelope {
            protocol: PROTOCOL,
            id,
            body,
        };
        let size = wire_codec(limit)
            .serialized_size(&message)
            .map_err(TcpError::component)? as usize;
        if size == 0 || size > limit {
            return Err(TcpError::new("frame length exceeds limit"));
        }
        let retained = self.spare_output.capacity().max(size + 4);
        if self.output.len() == SESSION_REQUESTS
            || retained > SESSION_BYTES.saturating_sub(self.output_bytes)
        {
            return Err(TcpError::new("session output credit exceeded"));
        }
        let mut bytes = std::mem::take(&mut self.spare_output);
        encode_sized_frame_into(&message, limit, size, &mut bytes)?;
        let len = bytes.len();
        let retained = bytes.capacity();
        self.output_bytes += retained;
        self.output.push_back(Output {
            id,
            bytes,
            segments: None,
            len,
            retained_bytes: retained,
            offset: 0,
            #[cfg(feature = "experimental-fabric")]
            segment_index: 0,
            #[cfg(feature = "experimental-fabric")]
            segment_offset: 0,
            fence: None,
            stage_started: self.time_work.then(Instant::now),
        });
        self.stats.peak_output_frames = self.stats.peak_output_frames.max(self.output.len() as u64);
        self.stats.peak_output_bytes = self.stats.peak_output_bytes.max(self.output_bytes as u64);
        Ok(())
    }
    fn abort(&mut self) {
        #[cfg(feature = "experimental-fabric")]
        self.finish_direct_diagnostic("session-abort");
        self.stats = self.transport_stats();
        self.closed = true;
        let _ = self.sock.socket.shutdown(Shutdown::Both);
        // Endpoint::drop closes before releasing registrations/slots. If close
        // cannot prove DMA quiescence it retains them, never frees live memory.
        #[cfg(feature = "experimental-fabric")]
        self.sock.fabric.take();
        self.output.clear();
        self.output_bytes = 0;
        self.submitted = 0;
        self.stats.retained_reply_credit_bytes = 0;
        self.input.clear();
    }
    #[cfg(feature = "experimental-fabric")]
    fn write_native(&mut self, budget: usize) -> Result<usize, TcpError> {
        if budget == 0 {
            return Ok(0);
        }
        let Some(output) = self.output.get(self.submitted) else {
            return Ok(0);
        };
        if output.offset == output.len {
            return Ok(0);
        }
        let start = output.offset;
        let cap = budget.min(PROGRESS_BYTES);
        let mut slices = [io::IoSlice::new(&[]); MAX_NATIVE_IOV];
        let (count, _source_bytes) = output.native_slices(cap, &mut slices);
        let began = self.time_work.then(Instant::now);
        let result = self
            .sock
            .fabric
            .as_mut()
            .ok_or_else(|| TcpError::new("native session lost its fabric endpoint"))?
            .try_send_vectored_no_poll(&slices[..count]);
        self.stats.plaintext_write.record(began, &result);
        match result {
            Ok(n) => {
                let output = &mut self.output[self.submitted];
                if n != 0
                    && start == 0
                    && let Some(queued) = output.stage_started
                {
                    let started = began.expect("timed output requires enabled diagnostics");
                    self.stats.output_queue_ns = self.stats.output_queue_ns.saturating_add(
                        started
                            .duration_since(queued)
                            .as_nanos()
                            .min(u64::MAX as u128) as u64,
                    );
                    self.stats.output_queue_samples =
                        self.stats.output_queue_samples.saturating_add(1);
                    output.stage_started = Some(started);
                }
                output.advance(n);
                Ok(n)
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(0),
            Err(error) => Err(error.into()),
        }
    }
    fn write_plaintext(&mut self, budget: usize) -> Result<usize, TcpError> {
        if self.conn.is_none() {
            #[cfg(feature = "experimental-fabric")]
            return self.write_native(budget);
            #[cfg(not(feature = "experimental-fabric"))]
            return Err(TcpError::new("native session requires experimental-fabric"));
        }
        if budget == 0 {
            return Ok(0);
        }
        let Some(output) = self.output.get_mut(self.submitted) else {
            return Ok(0);
        };
        if output.offset == output.bytes.len() {
            return Ok(0);
        }
        let end = output.bytes.len().min(
            output
                .offset
                .saturating_add(budget)
                .min(output.offset + PROGRESS_BYTES),
        );
        let began = self.time_work.then(Instant::now);
        let conn = self
            .conn
            .as_mut()
            .ok_or_else(|| TcpError::new("TLS session lost its rustls connection"))?;
        let result = conn.writer().write(&output.bytes[output.offset..end]);
        self.stats.plaintext_write.record(began, &result);
        match result {
            Ok(n) => {
                if n != 0
                    && output.offset == 0
                    && let Some(queued) = output.stage_started
                {
                    let started = began.expect("timed output requires enabled diagnostics");
                    self.stats.output_queue_ns = self.stats.output_queue_ns.saturating_add(
                        started
                            .duration_since(queued)
                            .as_nanos()
                            .min(u64::MAX as u128) as u64,
                    );
                    self.stats.output_queue_samples =
                        self.stats.output_queue_samples.saturating_add(1);
                    output.stage_started = Some(started);
                }
                output.offset += n;
                Ok(n)
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(0),
            Err(e) => Err(e.into()),
        }
    }
    fn wants_tls_write(&self) -> bool {
        self.conn.as_ref().is_some_and(|conn| conn.wants_write())
    }
    fn is_fabric(&self) -> bool {
        #[cfg(feature = "experimental-fabric")]
        return self.sock.fabric.is_some();
        #[cfg(not(feature = "experimental-fabric"))]
        false
    }
    /// Pending work includes accepted requests, deferred admission, workers and
    /// io_uring reads. None can rely on socket readability for its next event.
    /// Once Status initializes the authenticated session, idle TCP waits remain
    /// bounded for shutdown/peer-death checks instead of expiring the session.
    /// TLS/output queues must also be drained before a read-only wait is safe.
    fn idle_wait_deadline(
        &self,
        limits: TcpLimits,
        progressed: bool,
        work_pending: bool,
        initialized: bool,
        idle_since: Instant,
    ) -> Option<Instant> {
        if limits.polling != PollingPolicy::Adaptive
            || self.is_fabric()
            || progressed
            || work_pending
            || !self.output.is_empty()
            || self.wants_tls_write()
            || self.plaintext_pending != 0
            || self.peer_closed
        {
            return None;
        }
        let idle_deadline = if initialized {
            Instant::now() + IDLE_SHUTDOWN_CHECK_INTERVAL
        } else {
            idle_since + limits.io_timeout
        };
        Some(self.input_started.map_or(idle_deadline, |began| {
            idle_deadline.min(began + limits.io_timeout)
        }))
    }
    fn check_control_peer(&mut self, force: bool) -> Result<(), TcpError> {
        if self.is_fabric()
            && self
                .control_peer
                .check(&self.sock.socket, Instant::now(), force)?
        {
            self.stats.control_peer_checks = self.stats.control_peer_checks.saturating_add(1);
        }
        Ok(())
    }
    /// One provider CQ sweep shared by TLS transmit, receive and fence
    /// retirement. Neither no-poll data adapter performs extra CQ reads.
    fn progress_fabric_data(&mut self) -> Result<bool, TcpError> {
        #[cfg(feature = "experimental-fabric")]
        if let Some(endpoint) = &mut self.sock.fabric {
            return match endpoint.try_progress_for_sweep() {
                Ok(changed) => Ok(changed),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(false),
                Err(error) => Err(error.into()),
            };
        }
        Ok(false)
    }
    fn fabric_receive_ready(&mut self) -> Result<bool, TcpError> {
        #[cfg(feature = "experimental-fabric")]
        if let Some(endpoint) = &mut self.sock.fabric {
            return Ok(endpoint.receive_ready()?);
        }
        Ok(true)
    }
    /// One bounded output turn, without another CQ poll or empty transport read.
    /// A newly ready reply can use this turn immediately in its dispatch sweep.
    fn progress_output(&mut self) -> Result<bool, TcpError> {
        if self.closed {
            return Err(TcpError::new("session is closed"));
        }
        let mut changed = false;
        loop {
            if self.output_budget.frames == 0 {
                break;
            }
            let plaintext = self.write_plaintext(self.output_budget.plaintext)?;
            self.output_budget.plaintext -= plaintext;
            changed |= plaintext != 0;

            let mut transport_progress = false;
            let mut transport_blocked = false;
            if let Some(conn) = self.conn.as_mut()
                && conn.wants_write()
                && self.output_budget.tls_writes != 0
            {
                self.output_budget.tls_writes -= 1;
                let began = self.time_work.then(Instant::now);
                let result = conn.write_tls(&mut NonblockingIo(&mut self.sock));
                self.stats.tls_write.record(began, &result);
                match result {
                    Ok(n) => {
                        transport_progress = n != 0;
                        transport_blocked = n == 0;
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        transport_blocked = true;
                    }
                    Err(e) => return Err(e.into()),
                }
                changed |= transport_progress;
            }
            // A fence covers accepted ciphertext, never plaintext still in rustls.
            // This turn shares the sweep's single CQ acquisition and budget.
            #[cfg(feature = "experimental-fabric")]
            let (accepted, completed) = if let Some(endpoint) = &mut self.sock.fabric {
                (
                    endpoint.accepted_send_fence()?,
                    endpoint.completed_send_fence()?,
                )
            } else {
                (0, 0)
            };
            #[cfg(not(feature = "experimental-fabric"))]
            let (accepted, completed) = (0, 0);
            let submitted_before = self.stats.frames_submitted;
            changed |= self.finish_output(accepted, completed);
            let submitted_now =
                usize::try_from(self.stats.frames_submitted.saturating_sub(submitted_before))
                    .unwrap_or(usize::MAX);
            self.output_budget.frames = self.output_budget.frames.saturating_sub(submitted_now);
            if transport_blocked {
                break;
            }

            if plaintext == 0 && !transport_progress && submitted_now == 0 {
                break;
            }
            if self.output_budget.plaintext == 0
                || self.output_budget.tls_writes == 0 && self.wants_tls_write()
                || self.output_budget.frames == 0
            {
                break;
            }
        }
        Ok(changed)
    }
    fn progress(&mut self) -> Result<bool, TcpError> {
        if self.closed {
            return Err(TcpError::new("session is closed"));
        }
        self.output_budget.reset();
        self.check_control_peer(false)?;
        let mut changed = self.progress_fabric_data()?;
        changed |= self.progress_output()?;
        if self.conn.as_ref().is_some_and(|conn| conn.wants_read())
            && self.plaintext_pending == 0
            && !self.peer_closed
            && (!self.is_fabric() || self.fabric_receive_ready()?)
        {
            let began = self.time_work.then(Instant::now);
            let conn = self
                .conn
                .as_mut()
                .ok_or_else(|| TcpError::new("TLS session lost its rustls connection"))?;
            let result = conn.read_tls(&mut NonblockingIo(&mut self.sock));
            self.stats.tls_read.record(began, &result);
            match result {
                Ok(0) => return Err(TcpError::new("truncated TLS session")),
                Ok(n) => {
                    let began = self.time_work.then(Instant::now);
                    let result = conn.process_new_packets();
                    self.stats
                        .tls_process
                        .record(began, &result.as_ref().map(|_| n));
                    let state = result.map_err(TcpError::component)?;
                    self.plaintext_pending = state.plaintext_bytes_to_read();
                    self.peer_closed = state.peer_has_closed();
                    self.input_started.get_or_insert_with(Instant::now);
                    changed = true;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(changed)
    }
    fn finish_fabric_sweep(&mut self) -> Result<bool, TcpError> {
        #[cfg(feature = "experimental-fabric")]
        if let Some(endpoint) = &mut self.sock.fabric {
            return Ok(endpoint.finish_sweep_no_poll()?);
        }
        Ok(false)
    }
    /// Keep submission separate from retirement. Native and TLS output frames
    /// share the same bounded provider ownership and contiguous completion rules.
    fn finish_output(&mut self, accepted: u64, completed: u64) -> bool {
        if !self.output_budget.frame_gate_observed
            && self.output.len() > self.submitted + 1
            && self.wants_tls_write()
        {
            // Only the active frame's pending TLS ciphertext blocks the next
            // plaintext frame here; CQ retirement is not a submission gate.
            self.stats.frame_gate_stalls = self.stats.frame_gate_stalls.saturating_add(1);
            self.output_budget.frame_gate_observed = true;
        }
        let mut changed = false;
        let mut transition_time = None;
        if !self.wants_tls_write()
            && let Some(output) = self.output.get_mut(self.submitted)
            && output.offset == output.len
        {
            output.fence = Some(accepted);
            if let Some(started) = output.stage_started {
                let now = *transition_time.get_or_insert_with(Instant::now);
                self.stats.output_submit_ns = self.stats.output_submit_ns.saturating_add(
                    now.duration_since(started).as_nanos().min(u64::MAX as u128) as u64,
                );
                self.stats.output_submit_samples =
                    self.stats.output_submit_samples.saturating_add(1);
                output.stage_started = Some(now);
            }
            self.submitted += 1;
            self.stats.frames_submitted = self.stats.frames_submitted.saturating_add(1);
            changed = true;
        }
        while self
            .output
            .front()
            .is_some_and(|output| output.fence.is_some_and(|fence| fence <= completed))
        {
            let output = self.output.pop_front().unwrap();
            if let Some(submitted) = output.stage_started {
                let now = *transition_time.get_or_insert_with(Instant::now);
                self.stats.output_retire_ns = self.stats.output_retire_ns.saturating_add(
                    now.duration_since(submitted)
                        .as_nanos()
                        .min(u64::MAX as u128) as u64,
                );
                self.stats.output_retire_samples =
                    self.stats.output_retire_samples.saturating_add(1);
            }
            self.submitted -= 1;
            self.output_bytes -= output.retained_bytes;
            if output.segments.is_none()
                && output.bytes.capacity() <= 1024 * 1024
                && output.bytes.capacity() > self.spare_output.capacity()
            {
                self.spare_output = output.bytes;
                self.spare_output.clear();
            }
            self.stats.frames_retired = self.stats.frames_retired.saturating_add(1);
            changed = true;
        }
        if self.submitted != 0 {
            self.stats.retirement_stalls = self.stats.retirement_stalls.saturating_add(1);
        }
        changed
    }
    fn transport_stats(&self) -> TransportProgressStats {
        let mut stats = self.stats;
        stats.output_frames = self.output.len() as u64;
        stats.output_bytes = self.output_bytes as u64;
        stats.awaiting_retirement_frames = self.submitted as u64;
        #[cfg(feature = "experimental-fabric")]
        if let Some(endpoint) = &self.sock.fabric {
            stats.set_provider(endpoint.progress_stats());
        }
        stats
    }
    pub(super) fn configure_transport_stats(&mut self, enabled: bool, reset: bool) {
        #[cfg(feature = "experimental-fabric")]
        if reset || !enabled {
            self.finish_direct_diagnostic(if reset {
                "profile-reset"
            } else {
                "profile-disable"
            });
        }
        self.time_work = enabled;
        if reset || !enabled {
            // Never attribute a frame crossing a reset/disable to the new window.
            // Ownership, offsets and completion fences are left untouched.
            for output in &mut self.output {
                output.stage_started = None;
            }
        }
        #[cfg(feature = "experimental-fabric")]
        if let Some(endpoint) = &mut self.sock.fabric {
            endpoint.configure_progress_stats(enabled, reset);
        }
        if reset {
            self.stats = TransportProgressStats {
                peak_output_frames: self.output.len() as u64,
                peak_output_bytes: self.output_bytes as u64,
                retained_reply_credit_bytes: self.stats.retained_reply_credit_bytes,
                peak_retained_reply_credit_bytes: self.stats.retained_reply_credit_bytes,
                ..TransportProgressStats::default()
            };
        }
        #[cfg(feature = "experimental-fabric")]
        if enabled
            && self.direct_diagnostic.is_none()
            && let Some(endpoint) = &self.sock.fabric
            && let Ok(id) = NEXT_DIRECT_DIAGNOSTIC_WINDOW.fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |next| next.checked_add(1),
            )
        {
            let stats = endpoint.progress_stats();
            self.direct_diagnostic = Some(DirectDiagnosticWindow {
                id,
                data_posts: stats.efa_direct_data_posts,
                credit_posts: stats.efa_direct_credit_posts,
                piggyback_advances: stats.efa_direct_credit_piggyback_advances,
            });
        }
    }
    /// Local observer output, never a protocol-v8 field or durability receipt.
    /// Profile boundaries occur outside clean API timing; no CQ is acquired here.
    #[cfg(feature = "experimental-fabric")]
    fn finish_direct_diagnostic(&mut self, reason: &'static str) {
        let Some(window) = self.direct_diagnostic.take() else {
            return;
        };
        let Some(endpoint) = &self.sock.fabric else {
            return;
        };
        let stats = endpoint.progress_stats();
        let data_posts = stats
            .efa_direct_data_posts
            .saturating_sub(window.data_posts);
        let credit_posts = stats
            .efa_direct_credit_posts
            .saturating_sub(window.credit_posts);
        let piggyback_advances = stats
            .efa_direct_credit_piggyback_advances
            .saturating_sub(window.piggyback_advances);
        if data_posts == 0 && credit_posts == 0 && piggyback_advances == 0 {
            return;
        }
        let peer = self
            .sock
            .socket
            .local_addr()
            .and_then(|local| {
                self.sock
                    .socket
                    .peer_addr()
                    .map(|remote| format!("{local}->{remote}"))
            })
            .ok();
        let record = serde_json::json!({
            "schema": 1,
            "reason": reason,
            "window": window.id,
            "data_posts": data_posts,
            "credit_posts": credit_posts,
            "piggyback_advances": piggyback_advances,
            "peer": peer,
        });
        // Observer IO must not change authority or panic during endpoint teardown.
        // The campaign collector rejects missing required diagnostic records.
        let _ = writeln!(
            std::io::stderr().lock(),
            "TIDEHUNTER_FABRIC_LOCAL_DIAGNOSTICS {record}"
        );
    }
    fn read_transport(
        conn: &mut Option<rustls::Connection>,
        sock: &mut DeadlineIo,
        bytes: &mut [u8],
    ) -> io::Result<usize> {
        if let Some(conn) = conn {
            return conn.reader().read(bytes);
        }
        #[cfg(feature = "experimental-fabric")]
        {
            sock.fabric
                .as_mut()
                .ok_or_else(|| io::Error::other("native session lost its fabric endpoint"))?
                .try_receive_no_poll(bytes)
        }
        #[cfg(not(feature = "experimental-fabric"))]
        {
            let _ = sock;
            Err(io::Error::other(
                "native session requires experimental-fabric",
            ))
        }
    }
    fn recycle_input(&mut self, bytes: Vec<u8>) {
        if bytes.capacity() <= 1024 * 1024 {
            self.input = bytes;
        }
    }
    fn consume_plaintext(&mut self, bytes: usize) -> Result<(), TcpError> {
        self.plaintext_pending = self
            .plaintext_pending
            .checked_sub(bytes)
            .ok_or_else(|| TcpError::new("rustls plaintext readiness underflow"))?;
        Ok(())
    }
    fn receive(&mut self, limit: usize) -> Result<Option<(Vec<u8>, usize)>, TcpError> {
        if self.conn.is_some() && self.plaintext_pending == 0 {
            return if self.peer_closed {
                Err(TcpError::new("truncated transport frame"))
            } else {
                Ok(None)
            };
        }
        if self.header_used < 4 {
            let began = self.time_work.then(Instant::now);
            let result = Self::read_transport(
                &mut self.conn,
                &mut self.sock,
                &mut self.header[self.header_used..],
            );
            self.stats.plaintext_read.record(began, &result);
            match result {
                Ok(0) => return Err(TcpError::new("truncated transport frame")),
                Ok(n) => {
                    if self.conn.is_some() {
                        self.consume_plaintext(n)?;
                    }
                    self.input_started.get_or_insert_with(Instant::now);
                    self.header_used += n;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(None),
                Err(e) => return Err(e.into()),
            }
            if self.header_used != 4 {
                return Ok(None);
            }
            let len = u32::from_be_bytes(self.header) as usize;
            if len < 12 || len > limit {
                return Err(TcpError::new("frame length exceeds limit"));
            }
            if self.input.len() < len {
                let began = self.time_work.then(Instant::now);
                self.input.resize(len, 0);
                self.stats
                    .plaintext_buffer_resize
                    .record(began, &Ok::<_, TcpError>(len));
            }
            self.input_len = len;
        }
        if self.conn.is_some() && self.plaintext_pending == 0 {
            return if self.peer_closed {
                Err(TcpError::new("truncated transport frame"))
            } else {
                Ok(None)
            };
        }
        let began = self.time_work.then(Instant::now);
        let result = Self::read_transport(
            &mut self.conn,
            &mut self.sock,
            &mut self.input[self.input_used..self.input_len],
        );
        self.stats.plaintext_read.record(began, &result);
        match result {
            Ok(0) => return Err(TcpError::new("truncated transport frame")),
            Ok(n) => {
                if self.conn.is_some() {
                    self.consume_plaintext(n)?;
                }
                self.input_used += n;
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        if self.input_used != self.input_len {
            return Ok(None);
        }
        self.header_used = 0;
        self.input_used = 0;
        self.input_started = None;
        let input_len = std::mem::replace(&mut self.input_len, 0);
        // Keep the full initialized length when recycling this Vec. Subsequent
        // smaller frames overwrite only their logical range without truncate /
        // resize zero-filling; consumers must decode only the reported length.
        let frame = std::mem::take(&mut self.input);
        Ok(Some((frame, input_len)))
    }
    fn output_pending(&self, id: u64) -> bool {
        self.output.iter().any(|output| output.id == id)
    }
    fn partial_expired(&self, now: Instant, timeout: Duration) -> bool {
        self.input_started
            .is_some_and(|began| now.duration_since(began) >= timeout)
    }
    fn decode<T: serde::de::DeserializeOwned>(
        &mut self,
        frame: &[u8],
        limit: usize,
    ) -> Result<T, TcpError> {
        let began = self.time_work.then(Instant::now);
        let result = wire_codec(limit)
            .deserialize(frame)
            .map_err(TcpError::component);
        self.stats
            .frame_decode
            .record(began, &result.as_ref().map(|_| frame.len()));
        result
    }
}

#[cfg(feature = "experimental-fabric")]
impl Drop for Session {
    fn drop(&mut self) {
        self.finish_direct_diagnostic("session-drop");
    }
}

/// No complete_io/StreamOwned operation is used after bootstrap: rustls must
/// return partial progress instead of waiting for the rest of a TLS record.
struct NonblockingIo<'a>(&'a mut DeadlineIo);
impl Read for NonblockingIo<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let length = bytes.len().min(PROGRESS_BYTES);
        let io = &mut self.0;
        let began = io.profile.map(|_| Instant::now());
        #[cfg(feature = "experimental-fabric")]
        let result = if let Some(endpoint) = &mut io.fabric {
            // Fabric-only data progress: never syscall-poll the TCP control
            // socket here. Session owns its separately bounded health checks.
            endpoint.try_receive_no_poll(&mut bytes[..length])
        } else {
            io.socket.read(&mut bytes[..length])
        };
        #[cfg(not(feature = "experimental-fabric"))]
        let result = io.socket.read(&mut bytes[..length]);
        if let Some(profile) = io.profile.as_mut() {
            profile.read_ns += began.map_or(0, elapsed_ns);
            profile.read_calls += 1;
            profile.read_bytes += result.as_ref().copied().unwrap_or(0) as u64;
        }
        result
    }
}
impl Write for NonblockingIo<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.write_vectored(&[io::IoSlice::new(bytes)])
    }
    fn write_vectored(&mut self, bytes: &[io::IoSlice<'_>]) -> io::Result<usize> {
        let io = &mut self.0;
        let began = io.profile.map(|_| Instant::now());
        #[cfg(feature = "experimental-fabric")]
        let result = if let Some(endpoint) = &mut io.fabric {
            endpoint.try_send_vectored_no_poll(bytes)
        } else {
            io.socket.write_vectored(bytes)
        };
        #[cfg(not(feature = "experimental-fabric"))]
        let result = io.socket.write_vectored(bytes);
        if let Some(profile) = io.profile.as_mut() {
            profile.write_ns += began.map_or(0, elapsed_ns);
            profile.write_calls += 1;
            profile.write_bytes += result.as_ref().copied().unwrap_or(0) as u64;
        }
        result
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) struct ReadCompletion {
    pub id: u64,
    pub result: Result<(Vec<Mutation>, TcpReadProfile), TcpError>,
}
pub(crate) struct AppendCompletion {
    pub id: u64,
    pub result: Result<Vec<super::DurableBatch>, TcpError>,
}
struct PendingAppend {
    batches: Vec<CompleteBatch>,
    deadline: Instant,
    charge: usize,
    exchange_began: Option<Instant>,
    profile: Option<TcpReadProfile>,
}
struct QueuedAppendCompletion {
    completion: AppendCompletion,
    charge: usize,
}

struct PendingRead {
    epoch: u64,
    references: Vec<ValueRecordReference>,
    values: Vec<Mutation>,
    start: usize,
    end: usize,
    wire_id: u64,
    deadline: Instant,
    began: Option<Instant>,
    profiled: bool,
    class: ReadClass,
    grant_limit: usize,
    charge: usize,
    profile: TcpReadProfile,
    #[cfg(feature = "experimental-fabric")]
    rma: Option<PendingRmaRead>,
}

#[cfg(feature = "experimental-fabric")]
struct PendingRmaRead {
    grants: Vec<ResidentGrant>,
    operations: Vec<Option<tidehunter_fabric::ReadOperation>>,
    buffers: Vec<Option<minibytes::Bytes>>,
    next_post: usize,
    accepted: usize,
    completed: usize,
    first_error: Option<TcpError>,
}
impl PendingRead {
    fn has_rma(&self) -> bool {
        #[cfg(feature = "experimental-fabric")]
        {
            self.rma.is_some()
        }
        #[cfg(not(feature = "experimental-fabric"))]
        {
            false
        }
    }
}
#[derive(Default)]
pub(super) struct ClientState {
    next_id: u64,
    reads: HashMap<u64, PendingRead>,
    wire_reads: HashMap<u64, u64>,
    ready: VecDeque<(ReadCompletion, usize)>,
    retained_bytes: usize,
    appends: HashMap<u64, PendingAppend>,
    wire_appends: HashMap<u64, u64>,
    append_ready: VecDeque<QueuedAppendCompletion>,
    #[cfg(feature = "experimental-fabric")]
    rma_owner: Option<u64>,
    retained_append_bytes: usize,
    control: Option<(u64, Instant)>,
    control_result: Option<Response>,
    control_decode_ns: u64,
    control_profiled: bool,
    resident_grants: HashMap<ValueRecordReference, Vec<ResidentGrant>>,
    resident_handle_count: usize,
    resident_session: Option<(u64, [u8; 16])>,
    resident_acquisition_inflight: bool,
    resident_poison: Option<([u8; 16], u64, Vec<u64>)>,
    resident_stats: ResidentReadStats,
    progressed: bool,
}
impl ClientState {
    fn allocate_id(&mut self) -> Result<u64, TcpError> {
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or_else(|| TcpError::new("request ID space exhausted"))?;
        Ok(self.next_id)
    }
    pub(super) fn resident_read_stats(&self) -> ResidentReadStats {
        let mut stats = self.resident_stats;
        stats.handles_live = self.resident_handle_count as u64;
        stats.grants_live = stats.handles_live;
        stats
    }
}
fn client_grant_limit(
    class: ReadClass,
    acquisition_inflight: bool,
    rma_available: bool,
    max_handles: usize,
    live_handles: usize,
) -> usize {
    if class == ReadClass::Point && !acquisition_inflight && rma_available {
        max_handles.saturating_sub(live_handles)
    } else {
        0
    }
}

fn member_charge(reference: &ValueRecordReference) -> Result<usize, TcpError> {
    let len = reference.descriptor.len as usize;
    let result = len.checked_add(256).and_then(|n| n.checked_mul(3));
    let plan = len.checked_add(4096);
    result
        .zip(plan)
        .map(|(result, plan)| result.max(plan))
        .ok_or_else(|| TcpError::new("read credit overflow"))
}

fn resident_grant_wire_size(reference: ValueRecordReference) -> Result<usize, TcpError> {
    let grant = Some(ResidentGrant {
        reference,
        request_epoch: 0,
        reader_generation: 0,
        owner_boot: [0; 16],
        session_id: 0,
        grant_id: 0,
        grant_generation: 0,
        region: RemoteRegion {
            region_id: 0,
            generation: 0,
            remote_address: 0,
            key: 0,
            len: 0,
        },
        source_region_id: 0,
        source_region_generation: 0,
    });
    let size = wire_codec(usize::MAX)
        .serialized_size(&grant)
        .map_err(TcpError::component)?;
    usize::try_from(size).map_err(TcpError::component)
}
fn read_charge(references: &[ValueRecordReference]) -> Result<usize, TcpError> {
    // Executor asserts its per-request/member metadata is <=4096. The other
    // term covers explicit positional/mapped results and serialized copies.
    references.iter().try_fold(4096usize, |sum, reference| {
        sum.checked_add(member_charge(reference)?)
            .ok_or_else(|| TcpError::new("read credit overflow"))
    })
}
impl Client {
    /// Cooperatively stop using every cached grant and wait for the owner's exact
    /// drain acknowledgment before a fence, seal or retirement can be acknowledged.
    pub fn revoke_resident_grants(&mut self) -> Result<(), TcpError> {
        if self.reactor.resident_grants.is_empty() {
            return Ok(());
        }
        if !self.reactor.reads.is_empty()
            || !self.reactor.wire_reads.is_empty()
            || !self.reactor.ready.is_empty()
        {
            return Err(TcpError::new(
                "resident grant revocation requires drained client reads",
            ));
        }
        let mut generations = self
            .reactor
            .resident_grants
            .values()
            .flat_map(|grants| grants.iter())
            .map(|grant| grant.grant_generation)
            .collect::<Vec<_>>();
        generations.sort_unstable();
        let response = match self.request(Request::RevokeGrants {
            generations: generations.clone(),
        }) {
            Ok(response) => response,
            Err(error) => {
                self.fail_connection();
                return Err(error);
            }
        };
        match response {
            Response::GrantsRevoked(acknowledged) if acknowledged == generations => {
                self.reactor.resident_grants.clear();
                self.reactor.resident_handle_count = 0;
                Ok(())
            }
            _ => {
                self.fail_connection();
                Err(TcpError::new("invalid resident grant drain acknowledgment"))
            }
        }
    }
    pub(crate) fn resident_reads_enabled(&self) -> bool {
        self.resident_reads.destination_slots != 0
    }
    pub(crate) fn message_read_requests(&self) -> u64 {
        self.reactor.resident_stats.message_read_requests
    }

    /// Progress one nonblocking control/CQ sweep for idle resident-read lanes.
    /// Revocation is acknowledged only after all owned work and result acceptance
    /// have drained on this lane.
    pub(crate) fn progress_resident_control(&mut self) -> Result<bool, TcpError> {
        if !self.resident_reads_enabled() {
            return Ok(false);
        }
        if let Err(error) = self.progress_correlated() {
            self.fail_connection();
            return Err(error);
        }
        let progressed = self.reactor.progressed;
        if self.reactor.resident_poison.is_none() {
            return Ok(progressed);
        }
        if !self.reactor.reads.is_empty()
            || !self.reactor.wire_reads.is_empty()
            || !self.reactor.ready.is_empty()
            || {
                #[cfg(feature = "experimental-fabric")]
                {
                    self.reactor.rma_owner.is_some()
                }
                #[cfg(not(feature = "experimental-fabric"))]
                {
                    false
                }
            }
        {
            return Ok(true);
        }
        self.revoke_resident_grants()?;
        self.fail_connection();
        Err(TcpError::new("resident reader authority was poisoned"))
    }
    pub(super) fn revalidate_peer_at(
        &self,
        now: rustls::pki_types::UnixTime,
    ) -> Result<(), TcpError> {
        use rustls::client::danger::ServerCertVerifier;
        let (Some(verifier), Some(server_name), Some(connection)) = (
            self.reuse_verifier.as_ref(),
            self.server_name.as_ref(),
            self.stream.conn.as_ref(),
        ) else {
            if self.reuse_verifier.is_none()
                && self.server_name.is_none()
                && self.stream.conn.is_none()
            {
                return Ok(());
            }
            return Err(TcpError::new("incomplete TLS peer-revalidation state"));
        };
        let certificates = connection
            .peer_certificates()
            .ok_or_else(|| TcpError::new("missing authenticated peer certificate"))?;
        let (leaf, chain) = certificates
            .split_first()
            .ok_or_else(|| TcpError::new("empty authenticated peer certificate chain"))?;
        verifier
            .verify_server_cert(leaf, chain, server_name, &[], now)
            .map_err(TcpError::component)?;
        Ok(())
    }
    /// Revalidate an exclusively held idle lane without resetting wire IDs,
    /// rustls sequence state, or provider completion fences.
    pub(crate) fn refresh_for_reuse(&mut self) -> Result<(), TcpError> {
        self.ensure_connection()?;
        if self.stream.closed
            || !self.reactor.reads.is_empty()
            || !self.reactor.wire_reads.is_empty()
            || !self.reactor.ready.is_empty()
            || self.reactor.retained_bytes != 0
            || !self.reactor.appends.is_empty()
            || !self.reactor.wire_appends.is_empty()
            || !self.reactor.append_ready.is_empty()
            || self.reactor.retained_append_bytes != 0
            || self.reactor.control.is_some()
            || self.reactor.control_result.is_some()
            || self.stream.header_used != 0
            || self.stream.input_used != 0
            || self.stream.input_started.is_some()
            || self.stream.plaintext_pending != 0
            || self.stream.peer_closed
        {
            return Err(TcpError::new("connection is not quiescent for reuse"));
        }
        let began = Instant::now();
        let refreshed = (|| {
            self.revalidate_peer_at(rustls::pki_types::UnixTime::now())?;
            self.stream.check_control_peer(true)?;
            // A consumed read reply can precede the request's local CQE.
            // Drain only its existing transport ownership, never new app work.
            let deadline = began + self.timeout;
            let mut polling = ActivePoll::default();
            while !self.stream.output.is_empty() || self.stream.wants_tls_write() {
                if Instant::now() >= deadline {
                    return Err(TcpError::new("reuse transport drain deadline expired"));
                }
                self.progress_correlated()?;
                polling.pause(
                    self.polling,
                    self.stream.is_fabric(),
                    self.reactor.progressed,
                );
            }
            if self.stream.input_started.is_some()
                || self.stream.header_used != 0
                || self.stream.input_used != 0
            {
                return Err(TcpError::new(
                    "unexpected input during reuse transport drain",
                ));
            }
            self.status()
        })();
        match refreshed {
            Ok(status) => {
                self.connected_status = Some(status);
                self.connect_profile = ClientConnectProfile {
                    status_ns: elapsed_ns(began),
                    fabric_ns: self.stream.is_fabric().then_some(0),
                    ..ClientConnectProfile::default()
                };
                Ok(())
            }
            Err(error) => {
                self.fail_connection();
                Err(error)
            }
        }
    }
    /// Local connection transport counters; no RPC or progress is performed.
    /// Server output/reply ownership is in `ConnectionProfile::transport`.
    #[must_use]
    pub fn transport_progress_stats(&self) -> TransportProgressStats {
        self.stream.transport_stats()
    }
    /// Local-only bounded read sampling; preserve explicitly enabled connection
    /// diagnostics when the sample ends. No RPC, reset, or ownership change.
    pub(crate) fn profile_read_transport(&mut self, enabled: bool) {
        self.stream
            .configure_transport_stats(enabled || self.diagnostics.is_some(), false);
    }
    fn ensure_connection(&self) -> Result<(), TcpError> {
        if self.broken {
            Err(TcpError::new(
                "connection requires replacement; request outcome may be unknown",
            ))
        } else {
            Ok(())
        }
    }
    fn fail_connection(&mut self) {
        self.broken = true;
        self.stream.abort();
        self.reactor.reads.clear();
        self.reactor.wire_reads.clear();
        self.reactor.ready.clear();
        self.reactor.appends.clear();
        self.reactor.wire_appends.clear();
        self.reactor.append_ready.clear();
        self.reactor.retained_append_bytes = 0;
        self.reactor.retained_bytes = 0;
        self.reactor.control = None;
        self.reactor.control_result = None;
        self.reactor.control_profiled = false;
    }
    pub(crate) fn has_pending_io(&self) -> bool {
        !self.stream.output.is_empty()
            || !self.reactor.reads.is_empty()
            || !self.reactor.wire_reads.is_empty()
            || !self.reactor.ready.is_empty()
            || !self.reactor.appends.is_empty()
            || !self.reactor.wire_appends.is_empty()
            || !self.reactor.append_ready.is_empty()
            || self.reactor.control.is_some()
    }
    fn data_request_count(&self) -> usize {
        self.reactor.reads.len()
            + self.reactor.ready.len()
            + self.reactor.appends.len()
            + self.reactor.append_ready.len()
    }
    fn retained_data_bytes(&self) -> usize {
        self.reactor
            .retained_bytes
            .saturating_add(self.reactor.retained_append_bytes)
            .saturating_add(self.stream.output_bytes)
            .saturating_add(self.stream.spare_output.capacity())
    }
    pub(crate) fn read_admission(
        &self,
        references: &[ValueRecordReference],
        profiled: bool,
    ) -> Result<bool, TcpError> {
        self.ensure_connection()?;
        if references.is_empty() || references.len() > 256 {
            return Err(TcpError::new("invalid read count"));
        }
        let overhead = 64
            + if profiled {
                SERVER_READ_PROFILE_BYTES
            } else {
                0
            };
        let mut total = overhead;
        for reference in references {
            if reference.batch_id.database != self.expected.inventory.database
                || reference.batch_address.partition != self.expected.partition
            {
                return Err(TcpError::new("read belongs to another database/partition"));
            }
            super::super::codec::validate_reference_bounds(reference)
                .map_err(TcpError::component)?;
            if (reference.descriptor.len as usize + 8).max(256)
                > self.frame_limit.saturating_sub(overhead)
            {
                return Err(TcpError::new(
                    "one record exceeds negotiated response frame",
                ));
            }
            if read_charge(std::slice::from_ref(reference))? > self.max_read_bytes {
                return Err(TcpError::new(
                    "one record exceeds negotiated retained-read budget",
                ));
            }
            total = total
                .checked_add((reference.descriptor.len as usize + 8).max(256))
                .ok_or_else(|| TcpError::new("read result size overflow"))?;
            if total > HARD_FRAME_LIMIT {
                return Err(TcpError::new(
                    "aggregate read result exceeds 64 MiB; split the call",
                ));
            }
        }
        let charge = read_charge(references)?;
        if charge > SESSION_BYTES {
            return Err(TcpError::new(
                "read exceeds session byte credits; split the call",
            ));
        }
        Ok(self.data_request_count() < SESSION_REQUESTS
            && self.stream.output.len() < SESSION_REQUESTS
            && self.reactor.control.is_none()
            && charge <= SESSION_BYTES.saturating_sub(self.retained_data_bytes()))
    }
    pub(crate) fn submit_read(
        &mut self,
        epoch: u64,
        references: Vec<ValueRecordReference>,
        deadline: Instant,
        class: ReadClass,
        profiled: bool,
    ) -> Result<Option<u64>, TcpError> {
        if !self.read_admission(&references, profiled)? {
            return Ok(None);
        }
        if Instant::now() >= deadline {
            return Err(TcpError::new(
                "remote read deadline expired before submission",
            ));
        }
        let id = self.reactor.allocate_id()?;
        let charge = read_charge(&references)?;
        let mut pending = PendingRead {
            epoch,
            values: Vec::with_capacity(references.len()),
            references,
            start: 0,
            end: 0,
            wire_id: id,
            deadline,
            began: (profiled || self.diagnostics.is_some()).then(Instant::now),
            profiled,
            class,
            grant_limit: 0,
            charge,
            profile: TcpReadProfile::default(),
            #[cfg(feature = "experimental-fabric")]
            rma: None,
        };
        #[cfg(feature = "experimental-fabric")]
        let wire_queued = if self.reactor.rma_owner.is_none()
            && let Some(rma) = self.prepare_rma_read(epoch, &pending.references)?
        {
            pending.end = pending.references.len();
            pending.wire_id = 0;
            pending.rma = Some(rma);
            self.reactor.rma_owner = Some(id);
            false
        } else {
            true
        };
        #[cfg(not(feature = "experimental-fabric"))]
        let wire_queued = true;
        if wire_queued {
            self.queue_read_chunk(&mut pending)?;
            self.reactor.wire_reads.insert(id, id);
        }
        self.reactor.reads.insert(id, pending);
        self.reactor.retained_bytes += charge;
        Ok(Some(id))
    }
    fn queue_read_chunk(&mut self, pending: &mut PendingRead) -> Result<(), TcpError> {
        let began = pending.began.map(|_| Instant::now());
        #[cfg(feature = "experimental-fabric")]
        let rma_available = self
            .stream
            .sock
            .fabric
            .as_ref()
            .is_some_and(|endpoint| endpoint.rma_capabilities().is_some());
        #[cfg(not(feature = "experimental-fabric"))]
        let rma_available = false;
        #[cfg(feature = "experimental-fabric")]
        if !rma_available && self.resident_reads.enabled() {
            self.reactor.resident_stats.fallback_unsupported += 1;
        }
        let requested_grant_limit = client_grant_limit(
            pending.class,
            self.reactor.resident_acquisition_inflight,
            rma_available,
            self.resident_reads.max_handles,
            self.reactor.resident_handle_count,
        );
        let mut bytes = 64
            + if pending.profiled {
                SERVER_READ_PROFILE_BYTES
            } else {
                0
            };
        let mut retained = 4096usize;
        pending.end = pending.start;
        while pending.end < pending.references.len() && pending.end - pending.start < self.max_reads
        {
            let reference = pending.references[pending.end];
            let record_len =
                usize::try_from(reference.descriptor.len).map_err(TcpError::component)?;
            let next = bytes
                .checked_add(
                    record_len
                        .checked_add(8)
                        .ok_or_else(|| TcpError::new("read chunk size overflow"))?
                        .max(256),
                )
                .ok_or_else(|| TcpError::new("read chunk size overflow"))?;
            let next_retained = retained
                .checked_add(member_charge(&reference)?)
                .ok_or_else(|| TcpError::new("read credit overflow"))?;
            if next > self.frame_limit || next_retained > self.max_read_bytes {
                break;
            }
            retained = next_retained;
            bytes = next;
            pending.end += 1;
        }
        if pending.end == pending.start {
            return Err(TcpError::new("read cannot fit negotiated frame"));
        }
        let references = pending.references[pending.start..pending.end].to_vec();
        let mut grant_limit = requested_grant_limit;
        if grant_limit != 0 {
            if self.resident_reads.destination_slots == 0 {
                grant_limit = 0;
            } else {
                let mut response_bytes = bytes;
                let mut retained_with_grants = retained;
                for reference in &references {
                    let grant_charge = resident_grant_wire_size(*reference)?;
                    response_bytes = response_bytes
                        .checked_add(grant_charge)
                        .ok_or_else(|| TcpError::new("resident response size overflow"))?;
                    retained_with_grants = retained_with_grants
                        .checked_add(grant_charge)
                        .ok_or_else(|| TcpError::new("resident read credit overflow"))?;
                }
                if response_bytes > self.frame_limit || retained_with_grants > self.max_read_bytes {
                    grant_limit = 0;
                }
            }
        }
        pending.grant_limit = grant_limit;
        let grant_max_bytes = if grant_limit == 0 {
            0
        } else {
            self.resident_reads.max_read_bytes
        };
        let request = if pending.profiled {
            Request::ReadProfiled {
                epoch: pending.epoch,
                references,
                class: pending.class,
                grant_limit,
                grant_max_bytes,
            }
        } else {
            Request::Read {
                epoch: pending.epoch,
                references,
                class: pending.class,
                grant_limit,
                grant_max_bytes,
            }
        };
        self.stream
            .queue(pending.wire_id, &request, self.frame_limit)?;
        if pending.grant_limit != 0 {
            self.reactor.resident_acquisition_inflight = true;
        }
        self.reactor.resident_stats.message_read_requests += 1;
        pending.profile.request_encode_ns += began.map_or(0, elapsed_ns);
        Ok(())
    }
    pub(super) fn submit_append(
        &mut self,
        epoch: u64,
        batches: &[CompleteBatch],
        charge: usize,
    ) -> Result<Option<u64>, TcpError> {
        self.ensure_connection()?;
        if charge > SESSION_BYTES {
            return Err(TcpError::new("append exceeds session byte limit"));
        }
        if self.data_request_count() >= SESSION_REQUESTS
            || self.stream.output.len() >= SESSION_REQUESTS
            || charge > SESSION_BYTES.saturating_sub(self.retained_data_bytes())
        {
            return Ok(None);
        }
        let id = self.reactor.allocate_id()?;
        let profile_started = self.diagnostics.is_some().then(Instant::now);
        let request = Request::Append {
            epoch,
            batches: batches
                .iter()
                .map(|batch| batch.encoded().clone().into())
                .collect(),
        };
        let current = self.retained_data_bytes();
        let spare_capacity = self.stream.spare_output.capacity();
        let retained_without_spare = current.saturating_sub(spare_capacity);
        let output_reservation = spare_capacity.max(
            self.stream
                .queue_size(id, &request, self.frame_limit)?
                .saturating_add(4),
        );
        if charge > SESSION_BYTES.saturating_sub(retained_without_spare)
            || output_reservation
                > SESSION_BYTES.saturating_sub(retained_without_spare.saturating_add(charge))
        {
            return Ok(None);
        }
        self.stream.queue(id, &request, self.frame_limit)?;
        let request_encode_ns = profile_started.map_or(0, elapsed_ns);
        self.reactor.wire_appends.insert(id, id);
        self.reactor.appends.insert(
            id,
            PendingAppend {
                batches: batches.to_vec(),
                deadline: Instant::now() + self.timeout,
                charge,
                exchange_began: profile_started.map(|_| Instant::now()),
                profile: profile_started.map(|_| TcpReadProfile {
                    request_encode_ns,
                    ..TcpReadProfile::default()
                }),
            },
        );
        self.reactor.retained_append_bytes += charge;
        Ok(Some(id))
    }
    pub(crate) fn progress_appends(&mut self) -> Result<Vec<AppendCompletion>, TcpError> {
        if let Err(error) = self.progress_correlated() {
            self.fail_connection();
            return Err(error);
        }
        let mut completions = Vec::with_capacity(self.reactor.append_ready.len());
        while let Some(queued) = self.reactor.append_ready.pop_front() {
            self.reactor.retained_append_bytes -= queued.charge;
            completions.push(queued.completion);
        }
        Ok(completions)
    }
    pub(super) fn take_append_completion(
        &mut self,
        id: u64,
    ) -> Result<Option<AppendCompletion>, TcpError> {
        if let Err(error) = self.progress_correlated() {
            self.fail_connection();
            return Err(error);
        }
        let Some(index) = self
            .reactor
            .append_ready
            .iter()
            .position(|queued| queued.completion.id == id)
        else {
            return Ok(None);
        };
        let queued = self.reactor.append_ready.remove(index).unwrap();
        self.reactor.retained_append_bytes -= queued.charge;
        Ok(Some(queued.completion))
    }
    pub(super) fn pause_append_poll(&mut self, polling: &mut ActivePoll) {
        polling.pause(
            self.polling,
            self.stream.is_fabric(),
            self.reactor.progressed,
        );
    }
    fn record_append_profile(&mut self, pending: &mut PendingAppend, decoded_ns: u64) {
        let Some(mut profile) = pending.profile.take() else {
            return;
        };
        profile.response_decode_ns += decoded_ns;
        profile.requests += 1;
        profile.exchange_ns = pending.exchange_began.map_or(0, elapsed_ns);
        if let Some(diagnostics) = self.diagnostics.as_mut() {
            diagnostics.add(profile);
        }
    }
    pub(crate) fn read_progress_made(&self) -> bool {
        self.reactor.progressed
    }
    pub(crate) fn progress_reads(&mut self) -> Result<Vec<ReadCompletion>, TcpError> {
        if let Err(error) = self.progress_correlated() {
            self.fail_connection();
            return Err(error);
        }
        let mut completions = Vec::with_capacity(self.reactor.ready.len());
        for (completion, charge) in self.reactor.ready.drain(..) {
            self.reactor.retained_bytes -= charge;
            completions.push(completion);
        }
        Ok(completions)
    }
    fn progress_correlated(&mut self) -> Result<(), TcpError> {
        self.ensure_connection()?;
        let now = Instant::now();
        if self
            .reactor
            .appends
            .values()
            .any(|append| now >= append.deadline)
        {
            return Err(TcpError::new("remote append deadline expired"));
        }
        if self.reactor.reads.values().any(|read| now >= read.deadline)
            || self
                .reactor
                .control
                .is_some_and(|(_, deadline)| now >= deadline)
        {
            return Err(TcpError::new("remote operation deadline expired"));
        }
        self.reactor.progressed = self.stream.progress()?;
        // A peer can reply before our request's local CQE. Keep subsequent
        // chunks in their bounded read owner until output retirement frees a
        // frame credit; never overfill output or turn that stall into an error.
        while self.stream.output.len() < SESSION_REQUESTS {
            let Some(id) = self
                .reactor
                .reads
                .iter()
                .find_map(|(&id, read)| (read.wire_id == 0 && !read.has_rma()).then_some(id))
            else {
                break;
            };
            let mut pending = self.reactor.reads.remove(&id).unwrap();
            pending.wire_id = self.reactor.allocate_id()?;
            self.queue_read_chunk(&mut pending)?;
            self.reactor.wire_reads.insert(pending.wire_id, id);
            self.reactor.reads.insert(id, pending);
            self.reactor.progressed = true;
        }
        for _ in 0..SESSION_REQUESTS {
            let Some((frame, frame_len)) = self.stream.receive(self.frame_limit)? else {
                break;
            };
            self.reactor.progressed = true;
            let measured = self.diagnostics.is_some()
                || self.reactor.control_profiled
                || self.reactor.reads.values().any(|read| read.profiled)
                || self
                    .reactor
                    .appends
                    .values()
                    .any(|append| append.profile.is_some());
            let began = measured.then(Instant::now);
            let envelope: Envelope<Response> =
                self.stream.decode(&frame[..frame_len], self.frame_limit)?;
            let decoded = began.map_or(0, elapsed_ns);
            self.stream.recycle_input(frame);
            if envelope.protocol != PROTOCOL {
                return Err(TcpError::new("invalid response protocol"));
            }
            if envelope.id == 0 {
                let Response::ResidentPoison {
                    owner_boot,
                    session_id,
                    mut generations,
                } = envelope.body
                else {
                    return Err(TcpError::new("invalid unsolicited response"));
                };
                if self.reactor.resident_poison.is_some()
                    || self.reactor.resident_session != Some((session_id, owner_boot))
                {
                    return Err(TcpError::new("resident poison session identity mismatch"));
                }
                let mut expected: Vec<_> = self
                    .reactor
                    .resident_grants
                    .values()
                    .flat_map(|bucket| bucket.iter())
                    .map(|grant| grant.grant_generation)
                    .collect();
                expected.sort_unstable();
                generations.sort_unstable();
                if expected != generations {
                    return Err(TcpError::new("resident poison grant set mismatch"));
                }
                self.reactor.resident_poison = Some((owner_boot, session_id, generations));
                continue;
            }
            if self
                .reactor
                .control
                .is_some_and(|(id, _)| id == envelope.id)
            {
                if self.reactor.control_result.is_some() {
                    return Err(TcpError::new("duplicate control response"));
                }
                self.reactor.control_result = Some(envelope.body);
                self.reactor.control_decode_ns += decoded;
                continue;
            }
            if let Some(append_id) = self.reactor.wire_appends.remove(&envelope.id) {
                let mut pending = self
                    .reactor
                    .appends
                    .remove(&append_id)
                    .ok_or_else(|| TcpError::new("missing append owner"))?;
                let result = match envelope.body {
                    Response::Appended(receipts) if receipts.len() == pending.batches.len() => {
                        let verified = pending
                            .batches
                            .iter()
                            .zip(receipts)
                            .map(|(batch, receipt)| self.check_receipt(batch, receipt))
                            .collect::<Result<Vec<_>, _>>();
                        match verified {
                            Ok(receipts) => Ok(receipts),
                            Err(error) => {
                                self.record_append_profile(&mut pending, decoded);
                                self.fail_connection();
                                return Err(error);
                            }
                        }
                    }
                    Response::Error(message) => Err(TcpError::new(message)),
                    _ => {
                        self.record_append_profile(&mut pending, decoded);
                        return Err(TcpError::new("unexpected correlated append response"));
                    }
                };
                self.record_append_profile(&mut pending, decoded);
                self.reactor.append_ready.push_back(QueuedAppendCompletion {
                    completion: AppendCompletion {
                        id: append_id,
                        result,
                    },
                    charge: pending.charge,
                });
                continue;
            }
            let aggregate = self
                .reactor
                .wire_reads
                .remove(&envelope.id)
                .ok_or_else(|| TcpError::new("unknown or duplicate response ID"))?;
            let mut pending = self
                .reactor
                .reads
                .remove(&aggregate)
                .ok_or_else(|| TcpError::new("missing response owner"))?;
            if pending.grant_limit != 0 {
                self.reactor.resident_acquisition_inflight = false;
            }
            pending.profile.response_decode_ns += decoded;
            pending.profile.requests += 1;
            let (records, grants) = match envelope.body {
                Response::Records(records) if !pending.profiled => (records, None),
                Response::RecordsProfiled { records, profile } if pending.profiled => {
                    pending.profile.server_handler_available = true;
                    pending.profile.server_handler_ns += profile.handler_ns;
                    pending.profile.server_inventory_wait_ns += profile.inventory_wait_ns;
                    pending.profile.server_authority_lookup_ns += profile.authority_lookup_ns;
                    pending.profile.server_record_read_ns += profile.record_read_ns;
                    pending.profile.server_response_recheck_wait_ns +=
                        profile.response_recheck_wait_ns;
                    pending.profile.server_response_recheck_ns += profile.response_recheck_ns;
                    pending.profile.server_response_recheck_samples +=
                        profile.response_recheck_samples;
                    pending.profile.server_response_recheck_deferrals +=
                        profile.response_recheck_deferrals;
                    (records, None)
                }
                Response::ResidentRecords { records, grants } if !pending.profiled => {
                    (records, Some(grants))
                }
                Response::ResidentRecordsProfiled {
                    records,
                    grants,
                    profile,
                } if pending.profiled => {
                    pending.profile.server_handler_available = true;
                    pending.profile.server_handler_ns += profile.handler_ns;
                    pending.profile.server_inventory_wait_ns += profile.inventory_wait_ns;
                    pending.profile.server_authority_lookup_ns += profile.authority_lookup_ns;
                    pending.profile.server_record_read_ns += profile.record_read_ns;
                    pending.profile.server_response_recheck_wait_ns +=
                        profile.response_recheck_wait_ns;
                    pending.profile.server_response_recheck_ns += profile.response_recheck_ns;
                    pending.profile.server_response_recheck_samples +=
                        profile.response_recheck_samples;
                    pending.profile.server_response_recheck_deferrals +=
                        profile.response_recheck_deferrals;
                    (records, Some(grants))
                }
                Response::Error(message) => {
                    self.reactor.ready.push_back((
                        ReadCompletion {
                            id: aggregate,
                            result: Err(TcpError::new(message)),
                        },
                        pending.charge,
                    ));
                    continue;
                }
                _ => return Err(TcpError::new("unexpected correlated read response")),
            };
            let references = &pending.references[pending.start..pending.end];
            if records.len() != references.len() {
                return Err(TcpError::new("remote read count mismatch"));
            }
            let new_grants = if self.reactor.resident_poison.is_some() {
                0
            } else {
                grants
                    .as_ref()
                    .map(|grants| {
                        self.validate_resident_grants(
                            pending.epoch,
                            references,
                            grants,
                            pending.grant_limit,
                        )
                    })
                    .transpose()?
                    .unwrap_or(0)
            };
            let began = pending.began.map(|_| Instant::now());
            if let [reference] = references {
                // The common scalar reply already has its final result allocation.
                // Keep every member/digest/shape check without tuple/result vectors
                // or another shared-buffer clone.
                pending.values.push(
                    CompleteBatch::decode_record(
                        &self.shape,
                        reference,
                        records
                            .into_iter()
                            .next()
                            .expect("checked one record")
                            .into_bytes(),
                    )
                    .map_err(TcpError::component)?,
                );
            } else {
                let records: Vec<_> = references
                    .iter()
                    .copied()
                    .zip(records.into_iter().map(WireBytes::into_bytes))
                    .collect();
                pending.values.extend(
                    CompleteBatch::decode_records(&self.shape, &records)
                        .map_err(TcpError::component)?,
                );
            }
            if self.reactor.resident_poison.is_none()
                && let Some(grants) = grants.as_ref()
            {
                self.install_resident_grants(references, grants);
                self.reactor.resident_stats.grants_issued += new_grants as u64;
            }
            pending.profile.record_verify_ns += began.map_or(0, elapsed_ns);
            if Instant::now() >= pending.deadline {
                return Err(TcpError::new("remote read verification exceeded deadline"));
            }
            pending.start = pending.end;
            if pending.start != pending.references.len() {
                pending.wire_id = 0;
                self.reactor.reads.insert(aggregate, pending);
            } else {
                pending.profile.exchange_ns = pending.began.map_or(0, elapsed_ns);
                if let Some(profile) = self.diagnostics.as_mut() {
                    profile.add(pending.profile);
                }
                self.reactor.ready.push_back((
                    ReadCompletion {
                        id: aggregate,
                        result: Ok((pending.values, pending.profile)),
                    },
                    pending.charge,
                ));
            }
        }
        #[cfg(feature = "experimental-fabric")]
        if let Some(id) = self.reactor.rma_owner {
            self.progress_rma_read(id)?;
        }
        self.reactor.progressed |= self.stream.progress_output()?;
        self.reactor.progressed |= self.stream.finish_fabric_sweep()?;
        Ok(())
    }
    #[cfg(feature = "experimental-fabric")]
    fn prepare_rma_read(
        &mut self,
        epoch: u64,
        references: &[ValueRecordReference],
    ) -> Result<Option<PendingRmaRead>, TcpError> {
        if references.is_empty() || self.reactor.resident_poison.is_some() {
            return Ok(None);
        }
        let Some(endpoint) = self.stream.sock.fabric.as_mut() else {
            return Ok(None);
        };
        let Some(capabilities) = endpoint.rma_capabilities() else {
            return Ok(None);
        };
        if self.resident_reads.require_device_read
            && capabilities.read_mode != tidehunter_fabric::RmaReadMode::Native
        {
            self.reactor.resident_stats.fallback_unsupported += 1;
            return Ok(None);
        }
        let mut grants = Vec::with_capacity(references.len());
        let mut total_bytes = 0usize;
        for reference in references {
            let Some(grant) = self
                .reactor
                .resident_grants
                .get(reference)
                .and_then(|bucket| bucket.iter().find(|grant| grant.request_epoch == epoch))
                .copied()
            else {
                self.reactor.resident_stats.fallback_missing += 1;
                return Ok(None);
            };
            let Ok(len) = usize::try_from(grant.region.len) else {
                self.reactor.resident_stats.fallback_oversize += 1;
                return Ok(None);
            };
            if len == 0
                || len > self.resident_reads.max_read_bytes
                || len > capabilities.max_read_bytes
            {
                self.reactor.resident_stats.fallback_oversize += 1;
                return Ok(None);
            }
            total_bytes = total_bytes
                .checked_add(len)
                .ok_or_else(|| TcpError::new("resident group size overflow"))?;
            grants.push(grant);
        }
        if total_bytes > self.resident_reads.destination_bytes
            || endpoint.available_read_slots()? < references.len()
        {
            self.reactor.resident_stats.fallback_capacity += 1;
            return Ok(None);
        }
        self.reactor.resident_stats.eligible_reads += 1;
        Ok(Some(PendingRmaRead {
            operations: (0..grants.len()).map(|_| None).collect(),
            buffers: (0..grants.len()).map(|_| None).collect(),
            grants,
            next_post: 0,
            accepted: 0,
            completed: 0,
            first_error: None,
        }))
    }

    #[cfg(feature = "experimental-fabric")]
    fn progress_rma_read(&mut self, id: u64) -> Result<(), TcpError> {
        let Some(mut pending) = self.reactor.reads.remove(&id) else {
            self.reactor.rma_owner = None;
            return Ok(());
        };
        let Some(mut rma) = pending.rma.take() else {
            self.reactor.reads.insert(id, pending);
            self.reactor.rma_owner = None;
            return Ok(());
        };
        if self.reactor.resident_poison.is_some() && rma.first_error.is_none() {
            rma.first_error = Some(TcpError::new("resident reader authority was poisoned"));
        }
        if rma.first_error.is_none() {
            let Some(endpoint) = self.stream.sock.fabric.as_mut() else {
                rma.first_error = Some(TcpError::new("RMA endpoint disappeared"));
                pending.rma = Some(rma);
                self.reactor.reads.insert(id, pending);
                return Err(TcpError::new("RMA endpoint disappeared"));
            };
            while rma.next_post < rma.grants.len() {
                let grant = rma.grants[rma.next_post];
                let remote = tidehunter_fabric::RemoteReadRegion {
                    region_id: grant.region.region_id,
                    generation: grant.region.generation,
                    remote_address: grant.region.remote_address,
                    key: grant.region.key,
                    len: grant.region.len,
                };
                let len = usize::try_from(grant.region.len).map_err(TcpError::component)?;
                match endpoint.try_post_read(&remote, 0, len) {
                    Ok(operation) => {
                        rma.operations[rma.next_post] = Some(operation);
                        rma.next_post += 1;
                        rma.accepted += 1;
                        self.reactor.resident_stats.rma_posts += 1;
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) => {
                        self.reactor.resident_stats.rma_errors += 1;
                        return Err(TcpError::component(error));
                    }
                }
            }
        }
        if let Some(endpoint) = self.stream.sock.fabric.as_mut() {
            for index in 0..rma.operations.len() {
                let Some(operation) = rma.operations[index].as_mut() else {
                    continue;
                };
                match endpoint.take_read_completion(operation)? {
                    None => {}
                    Some(Ok(buffer)) => {
                        rma.buffers[index] = Some(buffer.into_minibytes_bytes());
                        rma.operations[index] = None;
                        rma.completed += 1;
                        self.reactor.resident_stats.rma_completions += 1;
                    }
                    Some(Err(error)) => {
                        self.reactor.resident_stats.rma_errors += 1;
                        return Err(TcpError::component(error));
                    }
                }
            }
        }
        let terminal = (rma.next_post == rma.grants.len() || rma.first_error.is_some())
            && rma.completed == rma.accepted;
        if !terminal {
            pending.rma = Some(rma);
            self.reactor.reads.insert(id, pending);
            return Ok(());
        }
        self.reactor.rma_owner = None;
        if let Some(error) = rma.first_error {
            self.reactor.ready.push_back((
                ReadCompletion {
                    id,
                    result: Err(error),
                },
                pending.charge,
            ));
            return Ok(());
        }
        if self.reactor.resident_poison.is_some() {
            self.reactor.ready.push_back((
                ReadCompletion {
                    id,
                    result: Err(TcpError::new("resident reader authority was poisoned")),
                },
                pending.charge,
            ));
            return Ok(());
        }
        let verify_started = pending.began.map(|_| Instant::now());
        for index in 0..rma.grants.len() {
            let bytes = rma.buffers[index]
                .take()
                .ok_or_else(|| TcpError::new("RMA completion omitted a destination"))?;
            let member = super::resident::checked_member(rma.grants[index], &bytes)?;
            let member_start = bytes.len() - member.len();
            pending.values.push(
                CompleteBatch::decode_record(
                    &self.shape,
                    &rma.grants[index].reference,
                    bytes.slice(member_start..),
                )
                .map_err(TcpError::component)?,
            );
        }
        if Instant::now() >= pending.deadline {
            return Err(TcpError::new(
                "resident read verification exceeded deadline",
            ));
        }
        pending.profile.record_verify_ns += verify_started.map_or(0, elapsed_ns);
        pending.profile.exchange_ns = pending.began.map_or(0, elapsed_ns);
        pending.profile.server_handler_not_applicable = true;
        if let Some(profile) = self.diagnostics.as_mut() {
            profile.add(pending.profile);
        }
        self.reactor.resident_stats.rma_hits += 1;
        self.reactor.ready.push_back((
            ReadCompletion {
                id,
                result: Ok((pending.values, pending.profile)),
            },
            pending.charge,
        ));
        Ok(())
    }

    fn validate_resident_grants(
        &self,
        epoch: u64,
        references: &[ValueRecordReference],
        grants: &[Option<ResidentGrant>],
        requested_new: usize,
    ) -> Result<usize, TcpError> {
        if grants.len() != references.len() {
            return Err(TcpError::new("resident grant/reference count mismatch"));
        }
        #[cfg(feature = "experimental-fabric")]
        let has_rma = self
            .stream
            .sock
            .fabric
            .as_ref()
            .and_then(tidehunter_fabric::Endpoint::rma_capabilities)
            .is_some();
        #[cfg(not(feature = "experimental-fabric"))]
        let has_rma = false;
        if !has_rma || self.resident_reads.destination_slots == 0 {
            return Err(TcpError::new(
                "peer returned grants without an RMA destination",
            ));
        }
        let mut fresh_handles = Vec::<ResidentGrant>::new();
        let mut identity = self.reactor.resident_session;
        for (reference, grant) in references.iter().zip(grants) {
            let Some(grant) = grant else {
                continue;
            };
            let current = (grant.session_id, grant.owner_boot);
            if identity.is_some_and(|known| known != current) {
                return Err(TcpError::new("resident grant belongs to another session"));
            }
            identity = Some(current);
            if grant.reference != *reference
                || grant.request_epoch != epoch
                || grant.session_id == 0
                || grant.source_region_id != grant.region.region_id
                || usize::try_from(grant.region.len).map_or(true, |len| {
                    len == 0 || len > self.resident_reads.max_read_bytes
                })
            {
                return Err(TcpError::new("resident grant identity or bounds mismatch"));
            }
            match self
                .reactor
                .resident_grants
                .get(reference)
                .and_then(|bucket| {
                    bucket.iter().find(|existing| {
                        existing.request_epoch == epoch
                            && existing.reader_generation == grant.reader_generation
                            && existing.session_id == grant.session_id
                            && existing.owner_boot == grant.owner_boot
                    })
                }) {
                Some(existing) if *existing == *grant => {}
                Some(_) => {
                    return Err(TcpError::new(
                        "resident reference was rebound to another grant",
                    ));
                }
                None => {
                    if !fresh_handles.contains(grant) {
                        fresh_handles.push(*grant);
                    }
                }
            }
        }
        let new_handles = fresh_handles.len();
        if new_handles > requested_new
            || self
                .reactor
                .resident_handle_count
                .checked_add(new_handles)
                .is_none_or(|count| count > self.resident_reads.max_handles)
        {
            return Err(TcpError::new("resident grant handle budget exceeded"));
        }
        Ok(new_handles)
    }

    fn install_resident_grants(
        &mut self,
        references: &[ValueRecordReference],
        grants: &[Option<ResidentGrant>],
    ) {
        for (reference, grant) in references.iter().zip(grants) {
            if let Some(grant) = grant {
                let bucket = self.reactor.resident_grants.entry(*reference).or_default();
                if !bucket.iter().any(|existing| {
                    existing.request_epoch == grant.request_epoch
                        && existing.reader_generation == grant.reader_generation
                        && existing.session_id == grant.session_id
                        && existing.owner_boot == grant.owner_boot
                }) {
                    bucket.push(*grant);
                    self.reactor.resident_handle_count += 1;
                    self.reactor.resident_session = Some((grant.session_id, grant.owner_boot));
                    self.reactor.resident_stats.grant_acquisitions += 1;
                }
            }
        }
        self.reactor.resident_stats.handles_live = self.reactor.resident_handle_count as u64;
        self.reactor.resident_stats.grants_live = self.reactor.resident_handle_count as u64;
    }
    pub(super) fn read_records_correlated(
        &mut self,
        epoch: u64,
        references: &[ValueRecordReference],
        class: ReadClass,
        profile: Option<&mut TcpReadProfile>,
    ) -> Result<Vec<Mutation>, TcpError> {
        let deadline = Instant::now() + self.timeout;
        let id = self
            .submit_read(
                epoch,
                references.to_vec(),
                deadline,
                class,
                profile.is_some(),
            )?
            .ok_or_else(|| TcpError::new("session read admission is full"))?;
        let mut polling = ActivePoll::default();
        loop {
            if let Err(error) = self.progress_correlated() {
                self.fail_connection();
                return Err(error);
            }
            if let Some(index) = self
                .reactor
                .ready
                .iter()
                .position(|(completion, _)| completion.id == id)
            {
                let (completion, charge) = self.reactor.ready.remove(index).unwrap();
                self.reactor.retained_bytes -= charge;
                let (records, measured) = completion.result?;
                if let Some(profile) = profile {
                    profile.add(measured);
                }
                return Ok(records);
            }
            polling.pause(
                self.polling,
                self.stream.is_fabric(),
                self.reactor.progressed,
            );
        }
    }
    pub(super) fn exchange_correlated(
        &mut self,
        request: &Request,
        mut profile: Option<&mut TcpReadProfile>,
    ) -> Result<Response, TcpError> {
        self.ensure_connection()?;
        if self.reactor.control.is_some() {
            return Err(TcpError::new("control exchange already active"));
        }
        let id = self.reactor.allocate_id()?;
        let deadline = Instant::now() + self.timeout;
        let size_began = profile.as_ref().map(|_| Instant::now());
        let queue_size = self.stream.queue_size(id, request, self.frame_limit)?;
        let size_ns = size_began.map_or(0, elapsed_ns);
        let mut polling = ActivePoll::default();
        loop {
            if self.stream.can_queue(queue_size) {
                break;
            }
            if Instant::now() >= deadline {
                return Err(TcpError::new("control output admission deadline expired"));
            }
            if let Err(error) = self.progress_correlated() {
                self.fail_connection();
                return Err(error);
            }
            polling.pause(
                self.polling,
                self.stream.is_fabric(),
                self.reactor.progressed,
            );
        }
        let queue_began = profile.as_ref().map(|_| Instant::now());
        self.stream.queue(id, request, self.frame_limit)?;
        if let Some(profile) = profile.as_deref_mut() {
            profile.request_encode_ns += size_ns + queue_began.map_or(0, elapsed_ns);
        }
        self.reactor.control = Some((id, deadline));
        self.reactor.control_decode_ns = 0;
        self.reactor.control_profiled = profile.is_some();
        let began = profile.as_ref().map(|_| Instant::now());
        loop {
            if let Err(error) = self.progress_correlated() {
                self.fail_connection();
                return Err(error);
            }
            if self.reactor.control_result.is_some() && !self.stream.output_pending(id) {
                let response = self.reactor.control_result.take().unwrap();
                self.reactor.control = None;
                self.reactor.control_profiled = false;
                if let Some(profile) = profile.as_deref_mut() {
                    profile.exchange_ns += began.map_or(0, elapsed_ns);
                    profile.response_decode_ns += self.reactor.control_decode_ns;
                    profile.requests += 1;
                }
                return match response {
                    Response::Error(message) => Err(TcpError::new(message)),
                    response => Ok(response),
                };
            }
            polling.pause(
                self.polling,
                self.stream.is_fabric(),
                self.reactor.progressed,
            );
        }
    }
}

struct ServerPending {
    deadline: Instant,
    charge: usize,
    epoch: Option<u64>,
    read_generation: Option<u64>,
    read_references: Vec<ValueRecordReference>,
    authority_owners: Vec<super::super::storage::ResidentReadOwner>,
    grant_limit: usize,
    grant_max_bytes: usize,
    profile: Option<Instant>,
    record_started: Option<Instant>,
    read_profile: ServerReadProfile,
    response_queued: bool,
    ready_response: Option<Response>,
    response_ready_at: Option<Instant>,
    measured: bool,
    output_started: Option<Instant>,
    _lease: Option<read_executor::ReadLease>,
    async_started: Option<Instant>,
    async_handler_ns: u64,
    control_queued: Option<Instant>,
    is_append: bool,
}

struct ServerResidentGrant {
    grant: ResidentGrant,
    #[cfg(feature = "experimental-fabric")]
    region: tidehunter_fabric::RegisteredReadRegion,
}

fn next_resident_id(counter: &AtomicU64) -> Result<u64, TcpError> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |next| {
            next.checked_add(1)
        })
        .map_err(|_| TcpError::new("resident identity space exhausted"))
}
pub(super) struct WorkCompletion {
    pub(super) id: u64,
    pub(super) response: Result<Response, TcpError>,
    pub(super) profile: ConnectionProfile,
    pub(super) is_control: bool,
}
struct DeferredRead {
    id: u64,
    epoch: u64,
    references: Vec<ValueRecordReference>,
    class: ReadClass,
    members: Option<
        Vec<(
            Arc<super::super::storage::PartitionLog>,
            ValueRecordReference,
        )>,
    >,
    plans: Option<Vec<read_executor::PlannedRead>>,
}

struct PlanCompletion {
    read: DeferredRead,
    result: Result<Vec<read_executor::PlannedRead>, TcpError>,
}

type ReadMember = (
    Arc<super::super::storage::PartitionLog>,
    ValueRecordReference,
);

// Keep the bounded cold handoff inline: boxing would allocate on every cold read.
#[allow(clippy::large_enum_variant)]
enum ReadPlanning {
    Ready(Vec<read_executor::PlannedRead>),
    Cold {
        ready: Vec<read_executor::PlannedRead>,
        first: ReadMember,
        remaining: std::vec::IntoIter<ReadMember>,
    },
}

fn resident_read_plans(members: Vec<ReadMember>) -> Result<ReadPlanning, TcpError> {
    let mut remaining = members.into_iter();
    let mut ready = Vec::with_capacity(remaining.len());
    while let Some((log, reference)) = remaining.next() {
        match log
            .try_plan_record_read(&reference)
            .map_err(TcpError::component)?
        {
            Some(plan) => ready.push((reference, plan)),
            None => {
                return Ok(ReadPlanning::Cold {
                    ready,
                    first: (log, reference),
                    remaining,
                });
            }
        }
    }
    Ok(ReadPlanning::Ready(ready))
}

fn load_read_plans(
    mut ready: Vec<read_executor::PlannedRead>,
    first: ReadMember,
    remaining: std::vec::IntoIter<ReadMember>,
) -> Result<Vec<read_executor::PlannedRead>, TcpError> {
    // Only the bounded cold worker calls this blocking authority loader. Each
    // returned plan already pins exact descriptor authority and native File.
    for (log, reference) in std::iter::once(first).chain(remaining) {
        ready.push((
            reference,
            log.plan_record_read(&reference)
                .map_err(TcpError::component)?,
        ));
    }
    Ok(ready)
}

fn revalidate_client_peer(
    server: Option<&rustls::Connection>,
    verifier: Option<&dyn rustls::server::danger::ClientCertVerifier>,
    now: rustls::pki_types::UnixTime,
) -> Result<(), TcpError> {
    let (Some(server), Some(verifier)) = (server, verifier) else {
        if server.is_none() && verifier.is_none() {
            return Ok(());
        }
        return Err(TcpError::new("incomplete TLS peer-revalidation state"));
    };
    let certificates = server
        .peer_certificates()
        .ok_or_else(|| TcpError::new("missing authenticated client certificate"))?;
    let (leaf, chain) = certificates
        .split_first()
        .ok_or_else(|| TcpError::new("empty authenticated client certificate chain"))?;
    verifier
        .verify_client_cert(leaf, chain, now)
        .map_err(TcpError::component)?;
    Ok(())
}

pub(super) fn serve_session(
    node: &Arc<StorageNode>,
    mut stream: Session,
    limits: TcpLimits,
    shutdown: &AtomicBool,
    client_verifier: Option<&dyn rustls::server::danger::ClientCertVerifier>,
) -> Result<(), TcpError> {
    let mut executor = if node.wal_member_read == super::super::storage::WalMemberReadPolicy::Async
    {
        Some(
            read_executor::ReadExecutor::new(node.read_config, node.read_admission.clone())
                .map_err(TcpError::component)?,
        )
    } else {
        None
    };
    let resident_source = node
        .resident_reads
        .source_per_connection(limits.max_connections);
    #[cfg(feature = "experimental-fabric")]
    let resident_source_available = resident_source.source_enabled()
        && stream
            .sock
            .fabric
            .as_ref()
            .is_some_and(|endpoint| endpoint.rma_capabilities().is_some());
    #[cfg(not(feature = "experimental-fabric"))]
    let resident_source_available = false;
    let resident_session_id = if resident_source.source_enabled() {
        next_resident_id(&super::NEXT_RESIDENT_SESSION)?
    } else {
        0
    };
    #[cfg(feature = "experimental-fabric")]
    let mut resident_grants = Vec::<ServerResidentGrant>::new();
    #[cfg(not(feature = "experimental-fabric"))]
    let resident_grants = Vec::<ServerResidentGrant>::new();
    let (completed, completions) = mpsc::sync_channel::<WorkCompletion>(SESSION_REQUESTS);
    let (plans_completed, plan_completions) =
        mpsc::sync_channel::<PlanCompletion>(SESSION_REQUESTS);
    let mut planners = 0usize;
    let mut pending = HashMap::<u64, ServerPending>::new();
    let mut deferred = VecDeque::<DeferredRead>::new();
    #[cfg(feature = "experimental-fabric")]
    let mut deferred_revokes = VecDeque::<(u64, Vec<u64>)>::new();
    let mut controls = VecDeque::<(u64, Request)>::new();
    // One decoded input frame may await byte credits; never drain more input
    // behind it. Its original deadline and the negotiated frame bound apply.
    let mut held_request = None::<(Envelope<Request>, usize, Instant)>;
    let mut active_appends = 0usize;
    let mut barrier_active = false;
    let mut workers = 0usize;
    let mut retained_bytes = 0usize;
    let mut initialized = false;
    let mut last_id = 0;
    let mut diagnostics = None::<ConnectionProfile>;
    let mut idle_since = Instant::now();
    let mut polling = ActivePoll::default();
    let mut previous_sweep = None::<Instant>;
    #[cfg(feature = "test-utils")]
    let mut ram_probe = None::<minibytes::Bytes>;
    let result = (|| {
        while !shutdown.load(Ordering::Acquire) {
            if let Some(executor) = executor.as_mut() {
                executor.begin_sweep();
            }
            let now = Instant::now();
            let sweep_gap = if let Some(profile) = diagnostics.as_mut() {
                profile.polling.progress_sweeps += 1;
                previous_sweep.replace(now).map_or(0, |previous| {
                    now.duration_since(previous)
                        .as_nanos()
                        .min(u64::MAX as u128) as u64
                })
            } else {
                previous_sweep = None;
                0
            };
            // io_timeout bounds initial Status, partial frames and accepted work;
            // a healthy established connection may remain idle indefinitely.
            if pending.values().any(|read| now >= read.deadline)
                || held_request
                    .as_ref()
                    .is_some_and(|(_, _, deadline)| now >= *deadline)
                || stream.partial_expired(now, limits.io_timeout)
                || (!initialized
                    && pending.is_empty()
                    && now.duration_since(idle_since) >= limits.io_timeout)
            {
                return Err(TcpError::new("session request deadline expired"));
            }
            let mut progressed = stream.progress()?;
            retire_responses(
                &mut stream,
                &mut pending,
                &mut retained_bytes,
                diagnostics.as_mut(),
            );
            #[cfg(feature = "experimental-fabric")]
            if pending.is_empty()
                && let Some(generations) =
                    node.resident_grants.take_poison_notice(resident_session_id)
            {
                stream.queue(
                    0,
                    &Response::ResidentPoison {
                        owner_boot: node.owner_boot,
                        session_id: resident_session_id,
                        generations,
                    },
                    limits.max_frame_bytes,
                )?;
                progressed = true;
            }
            #[cfg(feature = "experimental-fabric")]
            if pending.len() == 1
                && deferred.is_empty()
                && controls.is_empty()
                && active_appends == 0
                && workers == 0
                && let Some((id, generations)) = deferred_revokes.pop_front()
            {
                revoke_server_grants(
                    node,
                    &mut stream,
                    resident_session_id,
                    &mut resident_grants,
                    &generations,
                )?;
                queue_response(&mut pending, id, Response::GrantsRevoked(generations))?;
                progressed = true;
            }
            if let Some(executor) = executor.as_mut() {
                #[cfg(feature = "experimental-fabric")]
                {
                    progressed |= harvest_reads(
                        node,
                        &mut stream,
                        resident_session_id,
                        resident_source,
                        &mut resident_grants,
                        executor,
                        &mut pending,
                    )?;
                }
                #[cfg(not(feature = "experimental-fabric"))]
                {
                    progressed |= harvest_reads(node, executor, &mut pending)?;
                }
            }
            while let Ok(completion) = completions.try_recv() {
                progressed = true;
                workers -= 1;
                if completion.is_control {
                    if pending
                        .get(&completion.id)
                        .is_some_and(|request| request.is_append)
                    {
                        active_appends = active_appends.checked_sub(1).ok_or_else(|| {
                            TcpError::new("append completion without active session admission")
                        })?;
                    } else {
                        barrier_active = false;
                    }
                }
                if let Some(profile) = diagnostics.as_mut() {
                    profile.add(completion.profile);
                }
                let response = completion
                    .response
                    .unwrap_or_else(|error| Response::Error(error.to_string()));
                queue_response(&mut pending, completion.id, response)?;
            }
            while let Ok(completion) = plan_completions.try_recv() {
                planners -= 1;
                progressed = true;
                let mut read = completion.read;
                let request = pending
                    .get(&read.id)
                    .ok_or_else(|| TcpError::new("unknown authority planning completion ID"))?;
                if Instant::now() >= request.deadline {
                    return Err(TcpError::new("session request deadline expired"));
                }
                // The worker pinned native file ownership BEFORE this recheck.
                // Fencing invalidates authority, never a live CQ/file owner.
                if !node.reader_authority_valid(read.epoch, request.read_generation) {
                    queue_response(
                        &mut pending,
                        read.id,
                        Response::Error(
                            "read authority was fenced or poisoned during planning".into(),
                        ),
                    )?;
                    continue;
                }
                match completion.result {
                    Ok(plans) => {
                        read.plans = Some(plans);
                        deferred.push_back(read);
                    }
                    Err(error) => {
                        queue_response(&mut pending, read.id, Response::Error(error.to_string()))?
                    }
                }
            }
            for _ in 0..SESSION_REQUESTS {
                // Peer receipt can precede local TX completion. Backpressure,
                // rather than reject, the next valid window while credits live.
                if pending.len() == SESSION_REQUESTS {
                    break;
                }
                let (envelope, frame_bytes, deadline) = if let Some(request) = held_request.take() {
                    request
                } else {
                    let starting_frame = stream.header_used == 0;
                    let received = stream.receive(limits.max_frame_bytes)?;
                    if starting_frame
                        && (received.is_some() || stream.header_used != 0)
                        && let Some(profile) = diagnostics.as_mut()
                    {
                        profile.polling.request_first_poll_samples += 1;
                        profile.polling.request_first_poll_gap_ns += sweep_gap;
                        profile.polling.request_first_poll_gap_max_ns =
                            profile.polling.request_first_poll_gap_max_ns.max(sweep_gap);
                    }
                    let Some((frame, frame_len)) = received else {
                        break;
                    };
                    progressed = true;
                    idle_since = Instant::now();
                    let began = diagnostics.as_ref().map(|_| Instant::now());
                    let envelope: Envelope<Request> =
                        stream.decode(&frame[..frame_len], limits.max_frame_bytes)?;
                    let frame_bytes = frame_len;
                    stream.recycle_input(frame);
                    if let Some(profile) = diagnostics.as_mut() {
                        profile.request_decode_ns += began.map_or(0, elapsed_ns);
                    }
                    (envelope, frame_bytes, Instant::now() + limits.io_timeout)
                };
                if envelope.protocol != PROTOCOL || envelope.id <= last_id {
                    return Err(TcpError::new(
                        "invalid, duplicate or nonmonotonic request ID",
                    ));
                }
                if !initialized && !matches!(&envelope.body, Request::Status { protocol: PROTOCOL })
                {
                    return Err(TcpError::new("status/protocol handshake required"));
                }
                if initialized && matches!(&envelope.body, Request::Status { .. }) {
                    // Reusing a live TLS channel must not let an expired client
                    // credential create a new authority. Initial status already
                    // follows a fresh mTLS handshake; ordinary I/O stays unchanged.
                    revalidate_client_peer(
                        stream.conn.as_ref(),
                        client_verifier,
                        rustls::pki_types::UnixTime::now(),
                    )?;
                }
                let (charge, epoch, profiled) = match &envelope.body {
                    Request::Read {
                        epoch,
                        references,
                        grant_limit,
                        ..
                    }
                    | Request::ReadProfiled {
                        epoch,
                        references,
                        grant_limit,
                        ..
                    } => {
                        if references.is_empty()
                            || references.len() > node.effective_max_reads(limits)
                        {
                            return Err(TcpError::new("read request count exceeds bound"));
                        }
                        let profiled = matches!(&envelope.body, Request::ReadProfiled { .. });
                        let mut size = 32usize
                            + if profiled {
                                SERVER_READ_PROFILE_BYTES
                            } else {
                                0
                            };
                        for reference in references {
                            let record_len = usize::try_from(reference.descriptor.len)
                                .map_err(TcpError::component)?;
                            size = size
                                .checked_add(
                                    record_len
                                        .checked_add(8)
                                        .ok_or_else(|| TcpError::new("read response overflow"))?,
                                )
                                .ok_or_else(|| TcpError::new("read response overflow"))?;
                            if *grant_limit != 0 && node.resident_reads.source_enabled() {
                                size = size
                                    .checked_add(resident_grant_wire_size(*reference)?)
                                    .ok_or_else(|| TcpError::new("read response overflow"))?;
                            }
                        }
                        if size > limits.max_frame_bytes {
                            return Err(TcpError::new("read response exceeds frame bound"));
                        }
                        let charge = read_charge(references)?;
                        if charge > node.read_config.max_bytes {
                            return Err(TcpError::new(
                                "read exceeds negotiated retained-read budget",
                            ));
                        }
                        (charge, Some(*epoch), profiled)
                    }
                    _ => (
                        frame_bytes.max(limits.max_frame_bytes).saturating_add(512),
                        None,
                        false,
                    ),
                };
                #[cfg(feature = "test-utils")]
                let probe_bytes = ram_probe.as_ref().map_or(0, |bytes| bytes.len());
                #[cfg(not(feature = "test-utils"))]
                let probe_bytes = 0;
                if charge > SESSION_BYTES {
                    return Err(TcpError::new("request exceeds session byte credits"));
                }
                if charge
                    > SESSION_BYTES
                        .saturating_sub(probe_bytes)
                        .saturating_sub(retained_bytes)
                {
                    held_request = Some((envelope, frame_bytes, deadline));
                    break;
                }
                last_id = envelope.id;
                initialized = true;
                retained_bytes += charge;
                let measured = diagnostics.is_some() && !envelope.body.is_measurement_control();
                if let Some(profile) = diagnostics.as_mut() {
                    profile.requests += u64::from(measured);
                }
                let read_references = match &envelope.body {
                    Request::Read { references, .. } | Request::ReadProfiled { references, .. } => {
                        references.clone()
                    }
                    _ => Vec::new(),
                };
                let grant_limit = match &envelope.body {
                    Request::Read { grant_limit, .. }
                    | Request::ReadProfiled { grant_limit, .. } => *grant_limit,
                    _ => 0,
                };
                let grant_max_bytes = match &envelope.body {
                    Request::Read {
                        grant_max_bytes, ..
                    }
                    | Request::ReadProfiled {
                        grant_max_bytes, ..
                    } => *grant_max_bytes,
                    _ => 0,
                };
                pending.insert(
                    envelope.id,
                    ServerPending {
                        deadline,
                        charge,
                        epoch,
                        read_generation: None,
                        read_references,
                        authority_owners: Vec::new(),
                        grant_limit,
                        grant_max_bytes,
                        profile: profiled.then(Instant::now),
                        record_started: None,
                        read_profile: ServerReadProfile::default(),
                        response_queued: false,
                        ready_response: None,
                        response_ready_at: None,
                        measured,
                        is_append: matches!(&envelope.body, Request::Append { .. }),
                        control_queued: measured.then(Instant::now),
                        output_started: None,
                        _lease: None,
                        async_started: (measured && epoch.is_some() && executor.is_some())
                            .then(Instant::now),
                        async_handler_ns: 0,
                    },
                );
                match envelope.body {
                    #[cfg(feature = "test-utils")]
                    Request::PrepareRamProbe { value_bytes } => {
                        let valid = validate_ram_probe_size(value_bytes, limits.max_frame_bytes)
                            .and_then(|()| {
                                if value_bytes
                                    > SESSION_BYTES
                                        .saturating_sub(probe_bytes)
                                        .saturating_sub(retained_bytes)
                                {
                                    Err(TcpError::new("RAM probe exceeds session byte credits"))
                                } else {
                                    Ok(())
                                }
                            });
                        let response = match valid {
                            Ok(()) => {
                                // Replacement releases the old retained buffer. Queued replies
                                // keep their immutable clone until encoding/completion ownership
                                // retires; ordinary session output credits still apply.
                                ram_probe = Some(
                                    (0..value_bytes)
                                        .map(|i| (i % 251) as u8)
                                        .collect::<Vec<_>>()
                                        .into(),
                                );
                                Response::RamProbePrepared
                            }
                            Err(error) => Response::Error(error.to_string()),
                        };
                        queue_response(&mut pending, envelope.id, response)?;
                    }
                    #[cfg(feature = "test-utils")]
                    Request::ReadRamProbe => {
                        let response = match &ram_probe {
                            Some(bytes) => Response::RamProbe(bytes.clone().into()),
                            None => Response::Error("RAM probe is not prepared".into()),
                        };
                        queue_response(&mut pending, envelope.id, response)?;
                    }
                    Request::Read {
                        epoch,
                        references,
                        class,
                        ..
                    }
                    | Request::ReadProfiled {
                        epoch,
                        references,
                        class,
                        ..
                    } if executor.is_some() => {
                        deferred.push_back(DeferredRead {
                            id: envelope.id,
                            epoch,
                            references,
                            class,
                            members: None,
                            plans: None,
                        });
                    }
                    #[cfg(feature = "experimental-fabric")]
                    Request::RevokeGrants { generations } => {
                        let can_defer_for_tx_retirement = pending.len() != 1
                            && deferred.is_empty()
                            && controls.is_empty()
                            && active_appends == 0
                            && workers == 0;
                        if can_defer_for_tx_retirement {
                            deferred_revokes.push_back((envelope.id, generations));
                            continue;
                        }
                        let response = if !deferred_revokes.is_empty()
                            || !deferred.is_empty()
                            || !controls.is_empty()
                            || active_appends != 0
                            || workers != 0
                        {
                            Response::Error(
                                "resident grant revoke requires drained session work".into(),
                            )
                        } else {
                            match revoke_server_grants(
                                node,
                                &mut stream,
                                resident_session_id,
                                &mut resident_grants,
                                &generations,
                            ) {
                                Ok(()) => Response::GrantsRevoked(generations),
                                Err(error) => Response::Error(error.to_string()),
                            }
                        };
                        queue_response(&mut pending, envelope.id, response)?;
                    }
                    #[cfg(not(feature = "experimental-fabric"))]
                    Request::RevokeGrants { .. } => queue_response(
                        &mut pending,
                        envelope.id,
                        Response::Error("resident grants are unavailable in this build".into()),
                    )?,
                    request => controls.push_back((envelope.id, request)),
                }
            }
            // Dispatch newly decoded requests in this sweep, not after another
            // socket/CQ poll. Each queued request gets one bounded attempt; a
            // full ring or busy inventory cannot block unrelated requests.
            let deferred_count = deferred.len();
            let mut reads_submitted = false;
            for _ in 0..deferred_count {
                let mut read = deferred.pop_front().unwrap();
                if read.members.is_none() && read.plans.is_none() {
                    let began = pending[&read.id].profile.map(|_| Instant::now());
                    let snapshot = node.reader_snapshot(read.epoch, &read.references);
                    if let Some(request) = pending.get_mut(&read.id) {
                        request.read_profile.authority_lookup_ns += began.map_or(0, elapsed_ns);
                    }
                    match snapshot {
                        Ok(snapshot) => {
                            let request = pending.get_mut(&read.id).ok_or_else(|| {
                                TcpError::new("missing pending read for authority snapshot")
                            })?;
                            request.read_generation = Some(snapshot.generation);
                            read.members = Some(snapshot.members);
                        }
                        Err(error) => {
                            queue_response(
                                &mut pending,
                                read.id,
                                Response::Error(error.to_string()),
                            )?;
                            continue;
                        }
                    }
                }
                // Shared reply credits bound both queued cold authority loads
                // and ready plans, not just requests already submitted to disk.
                if pending[&read.id]._lease.is_none() {
                    match node.read_admission.reserve_reply(pending[&read.id].charge) {
                        Ok(Some(lease)) => pending.get_mut(&read.id).unwrap()._lease = Some(lease),
                        Ok(None) => {
                            deferred.push_back(read);
                            continue;
                        }
                        Err(error) => {
                            queue_response(
                                &mut pending,
                                read.id,
                                Response::Error(error.to_string()),
                            )?;
                            continue;
                        }
                    }
                }
                let request = pending.get_mut(&read.id).unwrap();
                if request.profile.is_some() {
                    request.record_started.get_or_insert_with(Instant::now);
                }
                if read.plans.is_none() {
                    match resident_read_plans(read.members.take().unwrap()) {
                        Ok(ReadPlanning::Ready(plans)) => read.plans = Some(plans),
                        Ok(ReadPlanning::Cold {
                            ready,
                            first,
                            remaining,
                        }) => {
                            planners += 1;
                            let completed = plans_completed.clone();
                            node.read_pool.spawn(move || {
                                let result =
                                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        load_read_plans(ready, first, remaining)
                                    }))
                                    .unwrap_or_else(|_| {
                                        Err(TcpError::new("cold read authority worker panicked"))
                                    });
                                let _ = completed.try_send(PlanCompletion { read, result });
                            });
                            progressed = true;
                            continue;
                        }
                        Err(error) => {
                            queue_response(
                                &mut pending,
                                read.id,
                                Response::Error(error.to_string()),
                            )?;
                            continue;
                        }
                    }
                }
                let request = pending.get_mut(&read.id).unwrap();
                // File/authority pins exist before this check, including on
                // the entirely resident path and after a Full retry.
                if !node.reader_authority_valid(read.epoch, request.read_generation) {
                    queue_response(
                        &mut pending,
                        read.id,
                        Response::Error(
                            "read authority was fenced or poisoned before submission".into(),
                        ),
                    )?;
                    continue;
                }
                if Instant::now() >= request.deadline {
                    return Err(TcpError::new("session request deadline expired"));
                }
                let can_capture_authority_owners =
                    request.grant_limit != 0
                        && resident_source_available
                        && resident_grants.len() < resident_source.source_regions
                        && request.grant_max_bytes > 512
                        && request.epoch.zip(request.read_generation).is_some_and(
                            |(epoch, generation)| {
                                request.read_references.iter().any(|reference| {
                                    usize::try_from(reference.descriptor.len).ok().is_some_and(
                                        |len| len <= request.grant_max_bytes.saturating_sub(512),
                                    ) && !resident_grants.iter().any(|entry| {
                                        entry.grant.reference == *reference
                                            && entry.grant.request_epoch == epoch
                                            && entry.grant.reader_generation == generation
                                            && entry.grant.owner_boot == node.owner_boot
                                            && entry.grant.session_id == resident_session_id
                                    })
                                })
                            },
                        );
                if can_capture_authority_owners {
                    let plans = read
                        .plans
                        .as_ref()
                        .ok_or_else(|| TcpError::new("resident grant request has no read plans"))?;
                    request.authority_owners = plans
                        .iter()
                        .map(|(_, plan)| plan.resident_owner())
                        .collect();
                }
                match executor.as_mut().unwrap().submit_planned(
                    read.id,
                    read.plans.take().unwrap(),
                    read.class,
                    &mut request._lease,
                ) {
                    Ok(read_executor::SubmitOutcome::Ready { records, lease }) => {
                        pending.get_mut(&read.id).unwrap()._lease = Some(lease);
                        #[cfg(feature = "experimental-fabric")]
                        finish_read_with_rma(
                            node,
                            &mut stream,
                            resident_session_id,
                            resident_source,
                            &mut resident_grants,
                            &mut pending,
                            read.id,
                            Ok(records),
                        )?;
                        #[cfg(not(feature = "experimental-fabric"))]
                        finish_read(node, &mut pending, read.id, Ok(records))?;
                        progressed = true;
                    }
                    Ok(read_executor::SubmitOutcome::Pending) => {
                        reads_submitted = true;
                        progressed = true;
                    }
                    Ok(read_executor::SubmitOutcome::Full(plans)) => {
                        read.plans = Some(plans);
                        deferred.push_back(read);
                    }
                    Err(error) => {
                        queue_response(&mut pending, read.id, Response::Error(error.to_string()))?;
                    }
                }
            }
            if reads_submitted {
                // Seastar reactor.cc:3333-3355 (19ecc0d39feb36c8a10692ba262e9924e9f7df33):
                // reap again after submission, since resident I/O can already
                // have completed. One bounded, nonblocking CQ harvest only.
                #[cfg(feature = "experimental-fabric")]
                {
                    progressed |= harvest_reads(
                        node,
                        &mut stream,
                        resident_session_id,
                        resident_source,
                        &mut resident_grants,
                        executor.as_mut().unwrap(),
                        &mut pending,
                    )?;
                }
                #[cfg(not(feature = "experimental-fabric"))]
                {
                    progressed |= harvest_reads(node, executor.as_mut().unwrap(), &mut pending)?;
                }
            }
            let mut blocked_control = false;
            for _ in 0..controls.len() {
                let (id, request) = controls.pop_front().unwrap();
                let is_append = matches!(&request, Request::Append { .. });
                let is_control = !matches!(
                    &request,
                    Request::Read { .. } | Request::ReadProfiled { .. }
                );
                if is_control
                    && (blocked_control || barrier_active || (!is_append && active_appends != 0))
                {
                    controls.push_back((id, request));
                    blocked_control = true;
                    continue;
                }
                if !is_control && pending[&id]._lease.is_none() {
                    match node.read_admission.reserve_reply(pending[&id].charge) {
                        Ok(Some(lease)) => pending.get_mut(&id).unwrap()._lease = Some(lease),
                        Ok(None) => {
                            controls.push_back((id, request));
                            continue;
                        }
                        Err(error) => {
                            queue_response(&mut pending, id, Response::Error(error.to_string()))?;
                            continue;
                        }
                    }
                }
                if let Request::ConnectionProfile { enabled, reset } = request {
                    let mut previous = diagnostics.unwrap_or_default();
                    previous.socket = stream.sock.profile.take().unwrap_or_default();
                    previous.transport = stream.transport_stats();
                    stream.configure_transport_stats(enabled, reset);
                    if reset || !enabled {
                        previous_sweep = None;
                    }
                    diagnostics = enabled.then(|| {
                        if reset {
                            ConnectionProfile::default()
                        } else {
                            previous
                        }
                    });
                    stream.sock.profile = enabled.then(|| {
                        if reset {
                            SocketProfile::default()
                        } else {
                            previous.socket
                        }
                    });
                    queue_response(
                        &mut pending,
                        id,
                        Response::ConnectionProfile(Box::new(previous)),
                    )?;
                } else if is_control {
                    let work = control::ControlWork {
                        id,
                        request,
                        limits,
                        measured: pending[&id].measured,
                        queued: pending[&id].control_queued,
                        charge: pending[&id].charge,
                        completed: completed.clone(),
                    };
                    match node.control.try_submit(work) {
                        Ok(()) => {
                            if is_append {
                                active_appends += 1;
                            } else {
                                barrier_active = true;
                            }
                            workers += 1;
                        }
                        Err(work) => {
                            // Do not let a later fence/seal/checkpoint pass a
                            // backpressured append on this authenticated lane.
                            controls.push_back((id, work.request));
                            blocked_control = true;
                            continue;
                        }
                    }
                } else {
                    workers += 1;
                    let completed = completed.clone();
                    let worker_node = node.clone();
                    let measured = pending[&id].measured;
                    // Explicit blocking read diagnostics retain their pool;
                    // append/control work never takes this shared-pool hop.
                    node.read_pool.spawn(move || {
                        let mut profile = ConnectionProfile::default();
                        let began = measured.then(Instant::now);
                        let response = worker_node.handle_caught(
                            request,
                            limits,
                            measured.then_some(&mut profile),
                        );
                        profile.handler_ns = began.map_or(0, elapsed_ns);
                        let _ = completed.try_send(WorkCompletion {
                            id,
                            response,
                            profile,
                            is_control,
                        });
                    });
                }
                progressed = true;
            }
            if flush_responses(
                node,
                &mut stream,
                &mut pending,
                limits,
                diagnostics.as_mut(),
            )? {
                // Do not defer a bounded resident reply behind the next sweep's
                // socket/CQ read. This is output-only; cold I/O stays off-owner.
                stream.progress_output()?;
                progressed = true;
            }
            progressed |= stream.finish_fabric_sweep()?;
            if progressed {
                idle_since = Instant::now();
            }
            if let Some(deadline) = stream.idle_wait_deadline(
                limits,
                progressed,
                !pending.is_empty() || held_request.is_some(),
                initialized,
                idle_since,
            ) {
                let began = diagnostics.as_ref().map(|_| Instant::now());
                wait_for_readable(&stream.sock.socket, deadline)?;
                if let Some(profile) = diagnostics.as_mut() {
                    profile.polling.idle_wait_calls += 1;
                    profile.polling.idle_wait_ns += began.map_or(0, elapsed_ns);
                }
            } else {
                let (yielded, spun) = polling.pause(limits.polling, stream.is_fabric(), progressed);
                if let Some(profile) = diagnostics.as_mut() {
                    profile.polling.scheduler_yields += u64::from(yielded);
                    profile.polling.busy_spin_hints +=
                        u64::from(spun && limits.polling == PollingPolicy::Busy);
                }
            }
        }
        Ok(())
    })();
    // No timeout or disconnect proves remote FI_READ quiescence. Keep every
    // registered source owner and its admission credits quarantined until restart.
    #[cfg(feature = "experimental-fabric")]
    node.resident_grants.quarantine_session(resident_session_id);
    node.finish_poison_if_quiescent();
    stream.abort();
    // A disconnected caller cancels delivery, not accepted control work. Join
    // the bounded worker completions before releasing this session's credits.
    while workers != 0 {
        completions.recv().map_err(TcpError::component)?;
        workers -= 1;
    }
    while planners != 0 {
        // Dropping an unsubmitted pinned plan is safe; no SQE owns its buffer.
        // Keep the pending reply lease until every cold worker has retired.
        plan_completions.recv().map_err(TcpError::component)?;
        planners -= 1;
    }
    if let Some(executor) = executor.as_mut() {
        // Cancellation suppresses delivery, never releases an SQE's pinned
        // buffer/file/segment before its CQE or ring teardown quiescence.
        for id in pending.keys() {
            executor.cancel(*id);
        }
        let shutdown_result = executor.shutdown().map_err(TcpError::component);
        result.and(shutdown_result)
    } else {
        result
    }
}

#[cfg(feature = "experimental-fabric")]
fn resident_grants_for_read(
    node: &StorageNode,
    stream: &mut Session,
    session_id: u64,
    source: ResidentReadConfig,
    resident_grants: &mut Vec<ServerResidentGrant>,
    request: &ServerPending,
    records: &[minibytes::Bytes],
) -> Result<Option<Vec<Option<ResidentGrant>>>, TcpError> {
    let mut attached = None;
    if !source.source_enabled()
        || stream
            .sock
            .fabric
            .as_ref()
            .is_none_or(|endpoint| endpoint.rma_capabilities().is_none())
        || request.grant_limit == 0
        || request.grant_max_bytes == 0
        || records.len() != request.read_references.len()
    {
        return Ok(None);
    }
    let (Some(epoch), Some(reader_generation)) = (request.epoch, request.read_generation) else {
        return Ok(None);
    };
    if !node.reader_authority_valid(epoch, Some(reader_generation)) {
        return Err(TcpError::new(
            "resident read authority is no longer current",
        ));
    }
    if request.authority_owners.len() != records.len() {
        return Ok(None);
    }
    let owner_boot = node.owner_boot;
    let grant_max_bytes = source.max_read_bytes.min(request.grant_max_bytes);
    let mut new_grants = 0usize;
    for index in 0..records.len() {
        let reference = request.read_references[index];
        let member = records[index].as_ref();
        if u32::try_from(member.len()).ok() != Some(reference.descriptor.len) {
            return Err(TcpError::new(
                "resident member length does not match its reference",
            ));
        }
        if let Some(existing) = resident_grants.iter().find(|entry| {
            entry.grant.reference == reference
                && entry.grant.request_epoch == epoch
                && entry.grant.reader_generation == reader_generation
                && entry.grant.owner_boot == owner_boot
                && entry.grant.session_id == session_id
        }) {
            attached.get_or_insert_with(|| vec![None; records.len()])[index] = Some(existing.grant);
            continue;
        }
        if new_grants >= request.grant_limit
            || resident_grants.len() >= source.source_regions
            || member.len() > grant_max_bytes.saturating_sub(512)
        {
            continue;
        }
        let reserved_bytes = member
            .len()
            .checked_add(512)
            .ok_or_else(|| TcpError::new("resident envelope length overflow"))?;
        let grant_generation = next_resident_id(&super::NEXT_RESIDENT_GRANT)?;
        let Some(source_owner) = node.resident_grants.try_reserve(
            session_id,
            grant_generation,
            reserved_bytes,
            request.authority_owners[index].clone(),
        ) else {
            continue;
        };
        let region = match stream
            .sock
            .fabric
            .as_mut()
            .ok_or_else(|| TcpError::new("resident source endpoint is unavailable"))?
            .reserve_read_region(session_id, grant_generation)
        {
            Ok(region) => region,
            Err(error) => {
                node.resident_grants
                    .cancel_reservation(session_id, grant_generation);
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock
                        | io::ErrorKind::Unsupported
                        | io::ErrorKind::OutOfMemory
                ) {
                    continue;
                }
                return Err(TcpError::component(error));
            }
        };
        let grant = ResidentGrant {
            reference,
            request_epoch: epoch,
            reader_generation,
            owner_boot,
            session_id,
            grant_id: grant_generation,
            grant_generation,
            region: RemoteRegion {
                region_id: 0,
                generation: 0,
                remote_address: 0,
                key: 0,
                len: 0,
            },
            source_region_id: region.region_id(),
            source_region_generation: region.generation(),
        };
        let envelope = match encode_envelope(grant, member) {
            Ok(envelope) => envelope,
            Err(error) => {
                let retire = unsafe {
                    stream
                        .sock
                        .fabric
                        .as_mut()
                        .ok_or_else(|| TcpError::new("resident source endpoint disappeared"))?
                        .retire_unpublished_read_region(region)
                };
                if retire.is_ok() {
                    node.resident_grants
                        .cancel_reservation(session_id, grant_generation);
                }
                return Err(error);
            }
        };
        if envelope.len() > grant_max_bytes
            || !node.resident_grants.shrink_reservation(
                session_id,
                grant_generation,
                envelope.len(),
            )
        {
            let retire = unsafe {
                stream
                    .sock
                    .fabric
                    .as_mut()
                    .ok_or_else(|| TcpError::new("resident source endpoint disappeared"))?
                    .retire_unpublished_read_region(region)
            };
            match retire {
                Ok(()) => node
                    .resident_grants
                    .cancel_reservation(session_id, grant_generation),
                Err(error) => return Err(TcpError::component(error)),
            }
            continue;
        }
        let source_bytes = envelope.len();
        // Arc unsizing to the fabric's type-erased owner reuses this allocation.
        let remote = match stream
            .sock
            .fabric
            .as_mut()
            .ok_or_else(|| TcpError::new("resident source endpoint disappeared"))?
            .register_read_region_with_owner(&region, envelope, source_owner)
        {
            Ok(remote) => remote,
            Err(error) => {
                let retire = unsafe {
                    stream
                        .sock
                        .fabric
                        .as_mut()
                        .ok_or_else(|| TcpError::new("resident source endpoint disappeared"))?
                        .retire_unpublished_read_region(region)
                };
                match retire {
                    Ok(()) => node
                        .resident_grants
                        .cancel_reservation(session_id, grant_generation),
                    Err(retire_error) => return Err(TcpError::component(retire_error)),
                }
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Unsupported
                ) {
                    continue;
                }
                return Err(TcpError::component(error));
            }
        };
        let grant = ResidentGrant {
            region: RemoteRegion {
                region_id: remote.region_id,
                generation: remote.generation,
                remote_address: remote.remote_address,
                key: remote.key,
                len: remote.len,
            },
            ..grant
        };
        if !node.reader_authority_valid(epoch, Some(reader_generation)) {
            let retire = unsafe {
                stream
                    .sock
                    .fabric
                    .as_mut()
                    .ok_or_else(|| TcpError::new("resident source endpoint disappeared"))?
                    .retire_unpublished_read_region(region)
            };
            if retire.is_ok() {
                node.resident_grants
                    .cancel_reservation(session_id, grant_generation);
            }
            return Err(TcpError::new(
                "resident read authority changed during grant setup",
            ));
        }
        if !node
            .resident_grants
            .publish(session_id, grant_generation, source_bytes)
        {
            unsafe {
                stream
                    .sock
                    .fabric
                    .as_mut()
                    .ok_or_else(|| TcpError::new("resident source endpoint disappeared"))?
                    .retire_unpublished_read_region(region)
            }
            .map_err(TcpError::component)?;
            node.resident_grants
                .cancel_reservation(session_id, grant_generation);
            continue;
        }
        resident_grants.push(ServerResidentGrant { grant, region });
        attached.get_or_insert_with(|| vec![None; records.len()])[index] = Some(grant);
        new_grants += 1;
    }
    Ok(attached)
}

#[cfg(feature = "experimental-fabric")]
fn revoke_server_grants(
    node: &StorageNode,
    stream: &mut Session,
    session_id: u64,
    resident_grants: &mut Vec<ServerResidentGrant>,
    generations: &[u64],
) -> Result<(), TcpError> {
    let mut expected: Vec<_> = resident_grants
        .iter()
        .map(|entry| entry.grant.grant_generation)
        .collect();
    expected.sort_unstable();
    if generations != expected {
        return Err(TcpError::new(
            "resident grant revocation did not name every session grant",
        ));
    }
    if generations.is_empty() {
        return Ok(());
    }
    let endpoint = stream
        .sock
        .fabric
        .as_mut()
        .ok_or_else(|| TcpError::new("resident source endpoint is unavailable"))?;
    for entry in resident_grants.drain(..) {
        // SAFETY: This is the exact authenticated session's release request.
        // The client closes admission and drains every FI_READ before sending
        // this request; the response is sent only after all source MRs retire.
        let token = unsafe {
            tidehunter_fabric::PeerDrainToken::after_ack(session_id, entry.grant.grant_generation)
        };
        endpoint
            .retire_read_region(entry.region, token)
            .map_err(TcpError::component)?;
    }
    if !node.resident_grants.release(session_id, generations) {
        return Err(TcpError::new(
            "resident grant registry lost an active session owner",
        ));
    }
    node.finish_poison_if_quiescent();
    Ok(())
}

#[cfg(feature = "experimental-fabric")]
fn harvest_reads(
    node: &StorageNode,
    stream: &mut Session,
    session_id: u64,
    source: ResidentReadConfig,
    resident_grants: &mut Vec<ServerResidentGrant>,
    executor: &mut read_executor::ReadExecutor,
    pending: &mut HashMap<u64, ServerPending>,
) -> Result<bool, TcpError> {
    let completions = executor.poll_completed().map_err(TcpError::component)?;
    let progressed = !completions.is_empty();
    for completion in completions {
        pending
            .get_mut(&completion.request_id)
            .ok_or_else(|| TcpError::new("unknown executor completion ID"))?
            ._lease = completion.lease;
        finish_read_with_rma(
            node,
            stream,
            session_id,
            source,
            resident_grants,
            pending,
            completion.request_id,
            completion.result.map_err(TcpError::component),
        )?;
    }
    Ok(progressed)
}

#[cfg(not(feature = "experimental-fabric"))]
fn harvest_reads(
    node: &StorageNode,
    executor: &mut read_executor::ReadExecutor,
    pending: &mut HashMap<u64, ServerPending>,
) -> Result<bool, TcpError> {
    let completions = executor.poll_completed().map_err(TcpError::component)?;
    let progressed = !completions.is_empty();
    for completion in completions {
        pending
            .get_mut(&completion.request_id)
            .ok_or_else(|| TcpError::new("unknown executor completion ID"))?
            ._lease = completion.lease;
        finish_read(
            node,
            pending,
            completion.request_id,
            completion.result.map_err(TcpError::component),
        )?;
    }
    Ok(progressed)
}

fn finish_read(
    node: &StorageNode,
    pending: &mut HashMap<u64, ServerPending>,
    id: u64,
    records: Result<Vec<minibytes::Bytes>, TcpError>,
) -> Result<(), TcpError> {
    let request = pending
        .get_mut(&id)
        .ok_or_else(|| TcpError::new("unknown read completion ID"))?;
    let response = match records {
        Ok(records) => {
            let records = records.into_iter().map(WireBytes::from).collect();
            if let Some(began) = request.profile {
                request.read_profile.handler_ns = elapsed_ns(began);
                request.read_profile.record_read_ns = request.record_started.map_or(0, elapsed_ns);
                Response::RecordsProfiled {
                    records,
                    profile: request.read_profile,
                }
            } else {
                Response::Records(records)
            }
        }
        Err(error) => Response::Error(error.to_string()),
    };
    node.finish_poison_if_quiescent();
    queue_response(pending, id, response)
}

#[cfg(feature = "experimental-fabric")]
#[allow(clippy::too_many_arguments)] // Exact session grant ownership accompanies the completed reply and its lease.
fn finish_read_with_rma(
    node: &StorageNode,
    stream: &mut Session,
    session_id: u64,
    source: ResidentReadConfig,
    resident_grants: &mut Vec<ServerResidentGrant>,
    pending: &mut HashMap<u64, ServerPending>,
    id: u64,
    records: Result<Vec<minibytes::Bytes>, TcpError>,
) -> Result<(), TcpError> {
    let grants = match &records {
        Ok(records) => {
            let request = pending
                .get(&id)
                .ok_or_else(|| TcpError::new("unknown read completion ID"))?;
            resident_grants_for_read(
                node,
                stream,
                session_id,
                source,
                resident_grants,
                request,
                records,
            )?
        }
        Err(_) => None,
    };
    let Some(grants) = grants else {
        return finish_read(node, pending, id, records);
    };
    let records = match records {
        Ok(records) => records,
        Err(error) => return finish_read(node, pending, id, Err(error)),
    };
    let request = pending
        .get_mut(&id)
        .ok_or_else(|| TcpError::new("unknown read completion ID"))?;
    let records = records.into_iter().map(WireBytes::from).collect();
    let response = if let Some(began) = request.profile {
        request.read_profile.handler_ns = elapsed_ns(began);
        request.read_profile.record_read_ns = request.record_started.map_or(0, elapsed_ns);
        Response::ResidentRecordsProfiled {
            grants,
            records,
            profile: request.read_profile,
        }
    } else {
        Response::ResidentRecords { grants, records }
    };
    node.finish_poison_if_quiescent();
    queue_response(pending, id, response)
}
fn queue_response(
    pending: &mut HashMap<u64, ServerPending>,
    id: u64,
    response: Response,
) -> Result<(), TcpError> {
    let request = pending
        .get_mut(&id)
        .ok_or_else(|| TcpError::new("response has no request owner"))?;
    if request.response_queued || request.ready_response.is_some() {
        return Err(TcpError::new("duplicate server completion"));
    }
    request.async_handler_ns = request.async_started.map_or(0, elapsed_ns);
    request.response_ready_at = (request.epoch.is_some()
        && (request.measured || request.profile.is_some()))
    .then(Instant::now);
    request.ready_response = Some(response);
    Ok(())
}

fn flush_responses(
    node: &StorageNode,
    stream: &mut Session,
    pending: &mut HashMap<u64, ServerPending>,
    limits: TcpLimits,
    mut diagnostics: Option<&mut ConnectionProfile>,
) -> Result<bool, TcpError> {
    let mut progressed = false;
    for (&id, request) in pending.iter_mut() {
        if request.ready_response.is_none() {
            continue;
        }
        if Instant::now() >= request.deadline {
            return Err(TcpError::new("session request deadline expired"));
        }
        // Recheck the captured authority without waiting for append or persistence.
        let recheck_started = request.response_ready_at.map(|_| Instant::now());
        let mut response = request.ready_response.take().unwrap();
        let recheck_error = request.epoch.and_then(|epoch| {
            (!node.reader_authority_valid(epoch, request.read_generation))
                .then(|| TcpError::new("read authority was fenced or poisoned before response"))
        });
        if let (Some(ready), Some(began)) = (request.response_ready_at, recheck_started) {
            let wait_ns = began.duration_since(ready).as_nanos().min(u64::MAX as u128) as u64;
            let recheck_ns = elapsed_ns(began);
            if request.measured
                && let Some(profile) = diagnostics.as_deref_mut()
            {
                profile.read_response_recheck_wait_ns += wait_ns;
                profile.read_response_recheck_ns += recheck_ns;
                profile.read_response_recheck_samples += 1;
                profile.read_response_recheck_deferrals +=
                    request.read_profile.response_recheck_deferrals;
                profile.read_response_recheck_errors += u64::from(recheck_error.is_some());
            }
            if let Response::RecordsProfiled { profile, .. } = &mut response {
                profile.response_recheck_wait_ns = wait_ns;
                profile.response_recheck_ns = recheck_ns;
                profile.response_recheck_samples = 1;
                profile.response_recheck_deferrals =
                    request.read_profile.response_recheck_deferrals;
            }
        }
        if let Some(error) = recheck_error {
            response = Response::Error(error.to_string());
        }
        #[cfg(any(test, feature = "test-utils"))]
        if matches!(response, Response::Appended(_))
            && node.drop_append_response.swap(false, Ordering::AcqRel)
        {
            return Err(TcpError::new("injected lost durable append response"));
        }
        let began = request.measured.then(Instant::now);
        stream.queue_response(id, response, limits.max_frame_bytes)?;
        if let Some(profile) = diagnostics.as_deref_mut() {
            profile.response_encode_ns += began.map_or(0, elapsed_ns);
            profile.handler_ns += request.async_handler_ns;
        }
        request.output_started = request.measured.then(Instant::now);
        request.response_queued = true;
        stream.stats.retained_reply_credit_bytes += request.charge as u64;
        stream.stats.peak_retained_reply_credit_bytes = stream
            .stats
            .peak_retained_reply_credit_bytes
            .max(stream.stats.retained_reply_credit_bytes);
        progressed = true;
    }
    Ok(progressed)
}

fn retire_responses(
    stream: &mut Session,
    pending: &mut HashMap<u64, ServerPending>,
    retained_bytes: &mut usize,
    mut diagnostics: Option<&mut ConnectionProfile>,
) {
    // Completion of plaintext/TLS submission alone never refunds reply leases.
    pending.retain(|id, request| {
        let keep = !request.response_queued || stream.output_pending(*id);
        if !keep {
            *retained_bytes -= request.charge;
            stream.stats.retained_reply_credit_bytes -= request.charge as u64;
            if let Some(profile) = diagnostics.as_deref_mut() {
                profile.response_send_ns += request.output_started.map_or(0, elapsed_ns);
            }
        }
        keep
    });
}
