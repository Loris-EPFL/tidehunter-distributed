//! Test-only provider dispatch exercises the actual public nonblocking methods.
//! No network timing assumptions: every CQ read/post is counted and scripted.

use super::super::{Endpoint, LIVE};
use super::*;
use std::{
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

#[derive(Default)]
struct Provider {
    polls: usize,
    cq_capacity: usize,
    sends: usize,
    receives: usize,
    closes: usize,
    error_reads: usize,
    send_result: isize,
    receive_result: isize,
    cq_result: isize,
    completion: Option<ffi::fi_cq_msg_entry>,
    sent: *const u8,
    sent_len: usize,
    sent_context: *mut c_void,
    received: *mut u8,
    received_context: *mut c_void,
    auto_complete: bool,
    closed_byte: Option<u8>,
    close_result: i32,
    trywaits: usize,
    trywait_result: i32,
}

/// Scripted `fi_trywait`: records the call and returns the configured status.
/// Same fixture ownership as `read_cq`; the fabric fid carries the `Provider`.
unsafe extern "C" fn trywait(
    fabric: *mut ffi::fid_fabric,
    fids: *mut *mut ffi::fid,
    count: i32,
) -> i32 {
    // SAFETY: The fixture installs this callback only on a fabric whose fid
    // context points to the live Provider; calls are serialized.
    let state = unsafe { &mut *(*fabric).fid.context.cast::<Provider>() };
    assert_eq!(count, 1, "only the completion queue may be waited on");
    assert!(!fids.is_null());
    state.trywaits += 1;
    state.trywait_result
}

// The fixture keeps callback handles, vtables and UnsafeCell<Provider> at stable
// heap addresses until Endpoint closes. Calls are serialized, with no Provider
// reference live across an Endpoint call.
unsafe extern "C" fn read_cq(cq: *mut ffi::fid_cq, buf: *mut c_void, count: usize) -> isize {
    // SAFETY: cq is a live fixture handle whose context points to its Provider.
    // Serialized dispatch gives this callback exclusive access to that state.
    let state = unsafe { &mut *(*cq).fid.context.cast::<Provider>() };
    state.polls += 1;
    state.cq_capacity = count;
    if let Some(entry) = state.completion.take() {
        // SAFETY: poll supplies a MSG-format array with at least one entry.
        unsafe { buf.cast::<ffi::fi_cq_msg_entry>().write(entry) };
        1
    } else {
        state.cq_result
    }
}

unsafe extern "C" fn read_error(
    cq: *mut ffi::fid_cq,
    buf: *mut ffi::fi_cq_err_entry,
    _flags: u64,
) -> isize {
    // SAFETY: cq is a live fixture handle whose context points to its Provider;
    // serialized dispatch gives this callback exclusive access.
    let state = unsafe { &mut *(*cq).fid.context.cast::<Provider>() };
    state.error_reads += 1;
    // SAFETY: poll supplies a writable, aligned error-entry local for this call.
    unsafe {
        buf.write(ffi::fi_cq_err_entry {
            err: i32::try_from(ffi::FI_EIO).unwrap(),
            ..ffi::fi_cq_err_entry::default()
        });
    }
    1
}

unsafe extern "C" fn send(
    ep: *mut ffi::fid_ep,
    buf: *const c_void,
    len: usize,
    _desc: *mut c_void,
    _peer: ffi::fi_addr_t,
    context: *mut c_void,
) -> isize {
    // SAFETY: ep is a live fixture handle whose context points to its Provider;
    // serialized dispatch gives this callback exclusive access.
    let state = unsafe { &mut *(*ep).fid.context.cast::<Provider>() };
    state.sends += 1;
    if state.send_result == 0 {
        state.sent = buf.cast();
        state.sent_len = len;
        state.sent_context = context;
        if state.auto_complete && state.completion.is_none() {
            state.completion = Some(ffi::fi_cq_msg_entry {
                op_context: context,
                ..ffi::fi_cq_msg_entry::default()
            });
        }
    }
    state.send_result
}

unsafe extern "C" fn receive(
    ep: *mut ffi::fid_ep,
    buf: *mut c_void,
    _len: usize,
    _desc: *mut c_void,
    _peer: ffi::fi_addr_t,
    context: *mut c_void,
) -> isize {
    // SAFETY: ep is a live fixture handle whose context points to its Provider;
    // serialized dispatch gives this callback exclusive access.
    let state = unsafe { &mut *(*ep).fid.context.cast::<Provider>() };
    state.received = buf.cast();
    state.received_context = context;
    state.receives += 1;
    state.receive_result
}

unsafe extern "C" fn close(fid: *mut ffi::fid) -> i32 {
    // SAFETY: Each fixture handle has a live Provider context. Serialized close
    // callbacks have exclusive state access before fixture storage is dropped.
    let state = unsafe { &mut *(*fid).context.cast::<Provider>() };
    if state.closes == 0 && !state.sent.is_null() {
        // SAFETY: A successful nonempty send stored sent. Endpoint retains that
        // initialized TX slot through endpoint close; the fixture never writes it.
        state.closed_byte = Some(unsafe { state.sent.read() });
    }
    state.closes += 1;
    state.close_result
}

struct Fixture {
    // Field order deliberately closes the endpoint before dispatch/state storage.
    endpoint: Option<Endpoint>,
    state: Box<UnsafeCell<Provider>>,
    _ep: Box<ffi::fid_ep>,
    _cq: Box<ffi::fid_cq>,
    _msg_ops: Box<ffi::fi_ops_msg>,
    _cq_ops: Box<ffi::fi_ops_cq>,
    _ops: Box<ffi::fi_ops>,
}

impl Fixture {
    fn new() -> Self {
        let state = Box::new(UnsafeCell::new(Provider {
            cq_result: -isize::try_from(ffi::FI_EAGAIN).unwrap(),
            ..Provider::default()
        }));
        let mut ops = Box::new(ffi::fi_ops {
            close: Some(close),
            ..ffi::fi_ops::default()
        });
        let mut msg_ops = Box::new(ffi::fi_ops_msg {
            send: Some(send),
            recv: Some(receive),
            ..ffi::fi_ops_msg::default()
        });
        let mut cq_ops = Box::new(ffi::fi_ops_cq {
            read: Some(read_cq),
            readerr: Some(read_error),
            ..ffi::fi_ops_cq::default()
        });
        let fid = ffi::fid {
            context: state.get().cast(),
            ops: &raw mut *ops,
            ..ffi::fid::default()
        };
        let mut ep = Box::new(ffi::fid_ep {
            fid,
            msg: &raw mut *msg_ops,
            ..ffi::fid_ep::default()
        });
        let mut cq = Box::new(ffi::fid_cq {
            fid,
            ops: &raw mut *cq_ops,
        });
        let resources = Resources {
            ep: &raw mut *ep,
            cq: &raw mut *cq,
            peer: Some(0),
            ..Resources::default()
        };
        // Balance Endpoint::drop's admission release without opening real handles.
        LIVE.fetch_add(1, Ordering::AcqRel);
        Self {
            endpoint: Some(Endpoint {
                resources: Some(Box::new(resources)),
                failed: false,
            }),
            state,
            _ep: ep,
            _cq: cq,
            _msg_ops: msg_ops,
            _cq_ops: cq_ops,
            _ops: ops,
        }
    }

    fn endpoint(&mut self) -> &mut Endpoint {
        self.endpoint.as_mut().unwrap()
    }

    fn state(&mut self) -> &mut Provider {
        // SAFETY: Exclusive fixture access prevents any concurrent callback or
        // Endpoint call while this reference exists. The UnsafeCell stays pinned.
        unsafe { &mut *self.state.get() }
    }
}

#[test]
fn send_eagain_reuses_unposted_copy_without_poisoning_or_retrying() {
    let mut fixture = Fixture::new();
    fixture.endpoint().configure_progress_stats(true, true);
    fixture.state().send_result = -isize::try_from(ffi::FI_EAGAIN).unwrap();
    assert_eq!(
        fixture
            .endpoint()
            .try_send(b"discarded")
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(fixture.state().polls, 1);
    assert_eq!(fixture.state().sends, 1);
    assert_eq!(fixture.state().receives, 0);
    assert!(!fixture.endpoint().sends_pending());
    assert_eq!(fixture.endpoint().inner().free_send_slot(), Some(0));

    fixture.state().send_result = 0;
    let mut input = *b"owned";
    assert_eq!(fixture.endpoint().try_send(&input).unwrap(), input.len());
    input.fill(0);
    assert_eq!(fixture.state().polls, 2);
    assert_eq!(fixture.state().sends, 2);
    assert_eq!(fixture.state().cq_capacity, 2 * SLOTS);
    assert!(fixture.endpoint().sends_pending());
    assert!(fixture.endpoint().inner().tx_pending[0]);
    // SAFETY: The posted slot has five initialized bytes. This fixture provider
    // only reads TX; checking them does not mutate or release provider ownership.
    let bytes = unsafe { std::slice::from_raw_parts(fixture.state().sent, 5) };
    assert_eq!(bytes, b"owned");
    let stats = fixture.endpoint().progress_stats();
    assert_eq!(stats.send_copy_calls, 2);
    assert_eq!(stats.send_copy_bytes, 14);
    assert_eq!(stats.send_post_calls, 2);
    assert_eq!(stats.send_post_bytes, 5);
    assert_eq!(stats.send_post_would_block, 1);
    assert_eq!(stats.send_post_errors, 0);
    assert_eq!(stats.cq_timed_calls, 2);
    drop(fixture.endpoint.take());
    assert_eq!(fixture.state().closed_byte, Some(b'o'));
    assert_eq!(fixture.state().closes, 2);
}

#[test]
fn full_send_ring_requires_its_exact_completion_before_reuse() {
    let mut fixture = Fixture::new();
    for byte in 0..SLOTS {
        assert_eq!(
            fixture
                .endpoint()
                .try_send(&[u8::try_from(byte).unwrap()])
                .unwrap(),
            1
        );
    }
    let context = fixture.state().sent_context;
    assert_eq!(
        fixture.endpoint().try_send(b"full").unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(fixture.state().polls, SLOTS + 1);
    assert_eq!(fixture.state().sends, SLOTS);
    assert_eq!(fixture.endpoint().inner().free_send_slot(), None);

    fixture.state().completion = Some(ffi::fi_cq_msg_entry {
        op_context: context,
        ..ffi::fi_cq_msg_entry::default()
    });
    assert_eq!(fixture.endpoint().try_send(b"reused").unwrap(), 6);
    assert_eq!(fixture.state().sends, SLOTS + 1);
    assert_eq!(fixture.state().sent_context, context);
    assert_eq!(fixture.endpoint().inner().free_send_slot(), None);
}

#[test]
fn receive_repost_eagain_keeps_consumed_bytes_and_retries_once_per_call() {
    let mut fixture = Fixture::new();
    let state = fixture.endpoint().inner();
    state.rx_pending[0] = true;
    // SAFETY: No external provider writes in this fixture; initialize the two
    // bytes before delivering their completion to the real receive state machine.
    unsafe { ptr::copy_nonoverlapping(b"ok".as_ptr(), state.memory.rx(0), 2) };
    let context = state.memory.rx_context(0);
    fixture.state().completion = Some(ffi::fi_cq_msg_entry {
        op_context: context,
        len: 2,
        ..ffi::fi_cq_msg_entry::default()
    });
    fixture.state().receive_result = -isize::try_from(ffi::FI_EAGAIN).unwrap();
    let mut bytes = [0; 2];
    assert_eq!(fixture.endpoint().try_receive(&mut bytes).unwrap(), 2);
    assert_eq!(&bytes, b"ok");
    assert_eq!(fixture.state().polls, 1);
    assert_eq!(fixture.state().receives, 1);
    assert_eq!(fixture.endpoint().inner().rx_post, 0);
    assert_eq!(
        fixture.endpoint().try_progress().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(fixture.state().polls, 2);
    assert_eq!(fixture.state().receives, 2);
    fixture.state().receive_result = 0;
    for posted in 1..=SLOTS {
        assert!(fixture.endpoint().try_progress().unwrap());
        assert_eq!(fixture.state().receives, 2 + posted);
    }
    assert!(!fixture.endpoint().try_progress().unwrap());
    assert_eq!(fixture.state().receives, 2 + SLOTS);
    assert_eq!(
        fixture
            .endpoint()
            .try_receive(&mut bytes)
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(!fixture.endpoint().failed);
}

#[test]
fn empty_io_does_not_poll_or_post() {
    let mut fixture = Fixture::new();
    assert_eq!(fixture.endpoint().try_send(&[]).unwrap(), 0);
    assert_eq!(fixture.endpoint().try_send_vectored(&[]).unwrap(), 0);
    assert_eq!(fixture.endpoint().try_receive(&mut []).unwrap(), 0);
    assert_eq!(fixture.state().polls, 0);
    assert_eq!(fixture.state().sends, 0);
    assert_eq!(fixture.state().receives, 0);
}

#[test]
fn cq_or_post_failure_poisons_without_releasing_pending_sends() {
    for cq_failure in [false, true] {
        let mut fixture = Fixture::new();
        fixture.endpoint().try_send(b"pinned").unwrap();
        if cq_failure {
            fixture.state().cq_result = -isize::try_from(ffi::FI_EIO).unwrap();
        } else {
            fixture.state().send_result = -isize::try_from(ffi::FI_EIO).unwrap();
        }
        assert!(fixture.endpoint().try_send(b"failure").is_err());
        assert!(fixture.endpoint().failed);
        assert!(fixture.endpoint().sends_pending());
        assert_eq!(fixture.endpoint().inner().free_send_slot(), Some(1));
        assert_eq!(
            fixture.endpoint().try_progress().unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
        assert_eq!(
            fixture.endpoint().try_receive(&mut [0]).unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
        drop(fixture.endpoint.take());
        assert_eq!(fixture.state().closed_byte, Some(b'p'));
    }
}

#[test]
fn failed_close_retains_handles_and_owned_bytes_until_successful_close() {
    let mut fixture = Fixture::new();
    fixture.endpoint().try_send(b"retained").unwrap();
    fixture.state().close_result = -i32::try_from(ffi::FI_EBUSY).unwrap();
    assert!(fixture.endpoint().inner().close().is_err());
    assert_eq!(fixture.state().closes, 1);
    assert!(!fixture.endpoint().inner().ep.is_null());
    assert!(!fixture.endpoint().inner().cq.is_null());
    assert!(fixture.endpoint().sends_pending());
    // SAFETY: Failed close must not free or alter the outstanding owned TX slot.
    assert_eq!(unsafe { fixture.state().sent.read() }, b'r');
    fixture.state().close_result = 0;
    drop(fixture.endpoint.take());
    assert_eq!(fixture.state().closes, 3);
}

#[test]
fn send_fence_waits_for_oldest_cqe_across_out_of_order_slot_reuse() {
    let mut fixture = Fixture::new();
    assert_eq!(fixture.endpoint().accepted_send_fence().unwrap(), 0);
    assert_eq!(fixture.endpoint().completed_send_fence().unwrap(), 0);
    fixture.endpoint().try_send(b"oldest").unwrap();
    let oldest = fixture.state().sent_context;
    let oldest_bytes = fixture.state().sent;
    fixture.endpoint().try_send(b"later").unwrap();
    let later = fixture.state().sent_context;
    assert_ne!(oldest, later);
    assert_eq!(fixture.endpoint().accepted_send_fence().unwrap(), 2);
    assert_eq!(fixture.endpoint().completed_send_fence().unwrap(), 0);

    // Arbitrarily many later completions need only the fixed slot ordinals,
    // not an ever-growing completion-gap set. The oldest buffer stays immutable.
    for ordinal in 3..=1024 {
        fixture.state().completion = Some(ffi::fi_cq_msg_entry {
            op_context: later,
            ..ffi::fi_cq_msg_entry::default()
        });
        fixture.endpoint().try_send(b"reused").unwrap();
        assert_eq!(fixture.state().sent_context, later);
        assert_eq!(fixture.endpoint().accepted_send_fence().unwrap(), ordinal);
        assert_eq!(fixture.endpoint().completed_send_fence().unwrap(), 0);
        // SAFETY: This pending TX slot is initialized and read-only to provider.
        assert_eq!(unsafe { oldest_bytes.read() }, b'o');
    }
    fixture.state().completion = Some(ffi::fi_cq_msg_entry {
        op_context: oldest,
        ..ffi::fi_cq_msg_entry::default()
    });
    fixture.endpoint().try_progress().unwrap();
    assert_eq!(fixture.endpoint().completed_send_fence().unwrap(), 1023);
    assert!(fixture.endpoint().sends_pending());
    fixture.state().completion = Some(ffi::fi_cq_msg_entry {
        op_context: later,
        ..ffi::fi_cq_msg_entry::default()
    });
    fixture.endpoint().try_progress().unwrap();
    assert_eq!(fixture.endpoint().completed_send_fence().unwrap(), 1024);
    assert!(!fixture.endpoint().sends_pending());
}

#[test]
fn eagain_failure_and_ordinal_exhaustion_never_accept_a_fence() {
    let mut fixture = Fixture::new();
    fixture.state().send_result = -isize::try_from(ffi::FI_EAGAIN).unwrap();
    assert_eq!(
        fixture.endpoint().try_send(b"no").unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(fixture.endpoint().accepted_send_fence().unwrap(), 0);
    fixture.state().send_result = 0;
    fixture.endpoint().try_send(b"live").unwrap();
    fixture.endpoint().inner().accepted_send = u64::MAX;
    let sends = fixture.state().sends;
    assert!(fixture.endpoint().try_send(b"overflow").is_err());
    assert_eq!(fixture.state().sends, sends);
    assert!(fixture.endpoint().sends_pending());
    assert!(fixture.endpoint().accepted_send_fence().is_err());
    assert!(fixture.endpoint().completed_send_fence().is_err());
    drop(fixture.endpoint.take());
    assert_eq!(fixture.state().closed_byte, Some(b'l'));
}

#[test]
fn diagnostic_poll_samples_distinguish_tx_and_rx_slot_pressure() {
    let mut fixture = Fixture::new();
    fixture.endpoint().configure_progress_stats(true, true);
    let resources = fixture.endpoint().inner();
    resources.tx_pending.fill(true);
    resources.rx_pending.fill(false);
    resources.rx_len[0] = 1;
    assert!(!resources.poll().unwrap());
    let stats = resources.progress_stats();
    assert_eq!(stats.tx_all_slots_pending_polls, 1);
    assert_eq!(stats.rx_no_posted_slots_polls, 1);
    assert_eq!(stats.rx_unread_slots_polls, 1);
    resources.rx_pending[1] = true;
    resources.rx_len[0] = 0;
    resources.tx_pending[0] = false;
    assert!(!resources.poll().unwrap());
    let stats = resources.progress_stats();
    assert_eq!(stats.tx_all_slots_pending_polls, 1);
    assert_eq!(stats.rx_no_posted_slots_polls, 1);
    assert_eq!(stats.rx_unread_slots_polls, 1);
    resources.configure_progress_stats(false, true);
    assert!(!resources.poll().unwrap());
    let stats = resources.progress_stats();
    assert_eq!(stats.tx_all_slots_pending_polls, 0);
    assert_eq!(stats.rx_no_posted_slots_polls, 0);
    assert_eq!(stats.rx_unread_slots_polls, 0);
}

#[test]
fn cq_statistics_count_real_polls_and_reset_without_changing_fences() {
    let mut fixture = Fixture::new();
    fixture.endpoint().configure_progress_stats(true, true);
    fixture.endpoint().try_send(b"live").unwrap();
    fixture.endpoint().inner().last_poll = Some(
        Instant::now()
            .checked_sub(std::time::Duration::from_millis(1))
            .unwrap(),
    );
    fixture.endpoint().try_progress().unwrap();
    let stats = fixture.endpoint().progress_stats();
    assert_eq!(stats.cq_polls, 2);
    assert_eq!(stats.cq_empty_polls, 2);
    assert_eq!(stats.cq_completions, 0);
    assert!(stats.cq_max_poll_gap_ns >= 1_000_000);
    assert_eq!(stats.send_copy_calls, 1);
    assert_eq!(stats.send_copy_bytes, 4);
    assert_eq!(stats.send_post_calls, 1);
    assert_eq!(stats.send_post_bytes, 4);
    assert_eq!(stats.cq_timed_calls, 2);
    assert_eq!(stats.cq_poll_gap_samples, 1);
    fixture.endpoint().configure_progress_stats(false, true);
    assert_eq!(fixture.endpoint().accepted_send_fence().unwrap(), 1);
    assert_eq!(fixture.endpoint().completed_send_fence().unwrap(), 0);
    let context = fixture.state().sent_context;
    fixture.state().completion = Some(ffi::fi_cq_msg_entry {
        op_context: context,
        ..ffi::fi_cq_msg_entry::default()
    });
    fixture.endpoint().try_progress().unwrap();
    let stats = fixture.endpoint().progress_stats();
    assert_eq!(stats.cq_polls, 1);
    assert_eq!(stats.cq_empty_polls, 0);
    assert_eq!(stats.cq_completions, 1);
    assert_eq!(stats.cq_max_poll_gap_ns, 0);
    assert_eq!(stats.cq_timed_calls, 0);
    assert_eq!(stats.cq_call_ns, 0);
    assert_eq!(stats.cq_dispatch_calls, 0);
    assert_eq!(stats.send_copy_calls, 0);
    assert_eq!(stats.send_post_calls, 0);
    assert_eq!(stats.cq_poll_gap_samples, 0);
    assert_eq!(fixture.endpoint().completed_send_fence().unwrap(), 1);
}

#[test]
fn provider_work_profiles_include_errors_without_releasing_live_sends() {
    let mut fixture = Fixture::new();
    fixture.endpoint().configure_progress_stats(true, true);
    fixture.endpoint().try_send(b"live").unwrap();
    fixture.state().cq_result = -isize::try_from(ffi::FI_EAVAIL).unwrap();
    assert!(fixture.endpoint().try_progress().is_err());
    let stats = fixture.endpoint().progress_stats();
    assert_eq!(stats.cq_timed_calls, 2);
    assert_eq!(stats.cq_error_calls, 1);
    assert_eq!(stats.cq_dispatch_calls, 0);
    assert!(fixture.endpoint().sends_pending());
    drop(fixture.endpoint.take());
    assert_eq!(fixture.state().closed_byte, Some(b'l'));

    let mut fixture = Fixture::new();
    fixture.endpoint().configure_progress_stats(true, true);
    fixture.state().send_result = -isize::try_from(ffi::FI_EIO).unwrap();
    assert!(fixture.endpoint().try_send(b"failed").is_err());
    let stats = fixture.endpoint().progress_stats();
    assert_eq!(stats.send_post_calls, 1);
    assert_eq!(stats.send_post_errors, 1);
    assert_eq!(stats.send_post_bytes, 0);
    assert!(!fixture.endpoint().sends_pending());
}

#[test]
fn receive_post_profiles_distinguish_capacity_backpressure_and_failure() {
    let mut fixture = Fixture::new();
    fixture.endpoint().configure_progress_stats(true, true);
    fixture.state().receive_result = -isize::try_from(ffi::FI_EAGAIN).unwrap();
    assert_eq!(
        fixture.endpoint().try_progress().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    fixture.state().receive_result = 0;
    fixture.endpoint().try_progress().unwrap();
    fixture.state().receive_result = -isize::try_from(ffi::FI_EIO).unwrap();
    assert!(fixture.endpoint().try_progress().is_err());
    let stats = fixture.endpoint().progress_stats();
    assert_eq!(stats.receive_post_calls, 3);
    assert_eq!(stats.receive_post_would_block, 1);
    assert_eq!(stats.receive_post_errors, 1);
    assert_eq!(stats.receive_post_bytes, CHUNK as u64);
    assert_eq!(stats.receive_copy_bytes, 0);
    assert!(fixture.endpoint().inner().rx_pending[0]);
}

#[test]
fn invalid_completion_counts_dispatch_failure_without_retiring_send() {
    let mut fixture = Fixture::new();
    fixture.endpoint().configure_progress_stats(true, true);
    fixture.endpoint().try_send(b"live").unwrap();
    fixture.state().completion = Some(ffi::fi_cq_msg_entry::default());
    assert!(fixture.endpoint().try_progress().is_err());
    let stats = fixture.endpoint().progress_stats();
    assert_eq!(stats.cq_dispatch_calls, 1);
    assert_eq!(stats.cq_dispatch_errors, 1);
    assert_eq!(stats.cq_completions, 0);
    assert!(fixture.endpoint().sends_pending());
    drop(fixture.endpoint.take());
    assert_eq!(fixture.state().closed_byte, Some(b'l'));
}

#[test]
fn receive_only_gaps_and_partial_copies_survive_diagnostic_reset() {
    let mut fixture = Fixture::new();
    fixture.endpoint().configure_progress_stats(true, true);
    fixture.endpoint().try_progress().unwrap();
    fixture.endpoint().inner().last_any_poll = Some(
        Instant::now()
            .checked_sub(std::time::Duration::from_millis(1))
            .unwrap(),
    );
    fixture.endpoint().try_progress().unwrap();
    let stats = fixture.endpoint().progress_stats();
    assert!(stats.cq_poll_gap_max_ns >= 1_000_000);
    assert_eq!(stats.cq_poll_gap_samples, 1);
    assert_eq!(stats.cq_max_poll_gap_ns, 0);
    let resources = fixture.endpoint().inner();
    resources.rx_pending[0] = true;
    // SAFETY: Scripted provider owns no DMA; the two-byte receive fits slot zero.
    unsafe {
        resources
            .memory
            .rx(0)
            .copy_from_nonoverlapping(b"rx".as_ptr(), 2);
    };
    let context = resources.memory.rx_context(0);
    fixture.state().completion = Some(ffi::fi_cq_msg_entry {
        op_context: context,
        len: 2,
        ..ffi::fi_cq_msg_entry::default()
    });
    fixture.endpoint().try_progress().unwrap();
    let mut byte = [0];
    assert_eq!(fixture.endpoint().inner().copy_receive(&mut byte), Some(1));
    assert_eq!(byte, [b'r']);
    let stats = fixture.endpoint().progress_stats();
    assert_eq!(stats.cq_dispatch_calls, 1);
    assert_eq!(stats.cq_dispatch_errors, 0);
    assert_eq!(stats.receive_copy_calls, 1);
    assert_eq!(stats.receive_copy_bytes, 1);
    fixture.endpoint().configure_progress_stats(true, true);
    assert_eq!(fixture.endpoint().inner().copy_receive(&mut byte), Some(1));
    assert_eq!(byte, [b'x']);
    let stats = fixture.endpoint().progress_stats();
    assert_eq!(stats.receive_copy_calls, 1);
    assert_eq!(stats.receive_copy_bytes, 1);
    assert_eq!(stats.cq_dispatch_calls, 0);
}

#[test]
fn one_explicit_sweep_covers_no_poll_io_and_exact_slot_retirement() {
    let mut fixture = Fixture::new();
    fixture.endpoint().try_progress().unwrap();
    for byte in 0..SLOTS {
        let input = vec![u8::try_from(byte).unwrap(); CHUNK + 1];
        assert_eq!(
            fixture
                .endpoint()
                .try_send_vectored_no_poll(&[IoSlice::new(&input)])
                .unwrap(),
            CHUNK
        );
    }
    assert_eq!(fixture.state().polls, 1);
    assert_eq!(
        fixture.endpoint().accepted_send_fence().unwrap(),
        SLOTS as u64
    );
    let context = fixture.state().sent_context;
    fixture.state().completion = Some(ffi::fi_cq_msg_entry {
        op_context: context,
        ..ffi::fi_cq_msg_entry::default()
    });
    // An available CQE is not ownership release until explicitly consumed.
    assert_eq!(
        fixture
            .endpoint()
            .try_send_vectored_no_poll(&[IoSlice::new(b"wait")])
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(fixture.state().sends, SLOTS);
    assert_eq!(fixture.state().polls, 1);
    fixture.endpoint().try_progress().unwrap();
    fixture
        .endpoint()
        .try_send_vectored_no_poll(&[IoSlice::new(b"next")])
        .unwrap();
    assert_eq!(fixture.state().polls, 2);
    assert_eq!(fixture.state().sent_context, context);
    assert_eq!(fixture.endpoint().completed_send_fence().unwrap(), 0);
}

#[test]
fn no_poll_eagain_releases_no_fence_and_never_retains_caller_slices() {
    let mut fixture = Fixture::new();
    fixture.state().send_result = -isize::try_from(ffi::FI_EAGAIN).unwrap();
    assert_eq!(
        fixture
            .endpoint()
            .try_send_vectored_no_poll(&[IoSlice::new(b"discard")])
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(fixture.endpoint().accepted_send_fence().unwrap(), 0);
    assert!(!fixture.endpoint().sends_pending());
    fixture.state().send_result = 0;
    let mut input = *b"owned";
    fixture
        .endpoint()
        .try_send_vectored_no_poll(&[IoSlice::new(&input)])
        .unwrap();
    input.fill(0);
    assert_eq!(fixture.state().polls, 0);
    assert_eq!(fixture.state().sends, 2);
    // SAFETY: The successful post owns five bytes until completion/close.
    assert_eq!(
        unsafe { std::slice::from_raw_parts(fixture.state().sent, 5) },
        b"owned"
    );
    drop(fixture.endpoint.take());
    assert_eq!(fixture.state().closed_byte, Some(b'o'));
}

#[test]
fn no_poll_partial_receive_keeps_unread_bytes_until_repost() {
    let mut fixture = Fixture::new();
    let state = fixture.endpoint().inner();
    // Start with a full ring, as after initial peer setup.
    state.rx_pending.fill(true);
    // SAFETY: Scripted provider has no concurrent DMA and writes before CQE.
    unsafe { ptr::copy_nonoverlapping(b"abcd".as_ptr(), state.memory.rx(0), 4) };
    let context = state.memory.rx_context(0);
    fixture.state().completion = Some(ffi::fi_cq_msg_entry {
        op_context: context,
        len: 4,
        ..ffi::fi_cq_msg_entry::default()
    });
    assert!(!fixture.endpoint().receive_ready().unwrap());
    let mut bytes = [0; 2];
    assert_eq!(
        fixture
            .endpoint()
            .try_receive_no_poll(&mut bytes)
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(fixture.state().polls, 0);
    fixture.endpoint().try_progress().unwrap();
    assert!(fixture.endpoint().receive_ready().unwrap());
    assert_eq!(
        fixture.endpoint().try_receive_no_poll(&mut bytes).unwrap(),
        2
    );
    assert!(fixture.endpoint().receive_ready().unwrap());
    assert_eq!(&bytes, b"ab");
    fixture.state().receive_result = -isize::try_from(ffi::FI_EAGAIN).unwrap();
    assert_eq!(
        fixture.endpoint().try_receive_no_poll(&mut bytes).unwrap(),
        2
    );
    assert_eq!(&bytes, b"cd");
    assert_eq!(fixture.state().receives, 1);
    assert!(!fixture.endpoint().receive_ready().unwrap());
    assert_eq!(fixture.endpoint().inner().rx_post, 0);
    fixture.state().receive_result = 0;
    fixture.endpoint().try_progress().unwrap();
    assert_eq!(fixture.endpoint().inner().rx_post, 1);
    assert_eq!(fixture.state().polls, 2);
    assert!(!fixture.endpoint().failed);
}

#[test]
fn cq_error_or_oversized_count_never_consumes_uninitialized_entries() {
    for code in [
        -isize::try_from(ffi::FI_EAVAIL).unwrap(),
        isize::try_from(2 * SLOTS + 1).unwrap(),
    ] {
        let mut fixture = Fixture::new();
        fixture
            .endpoint()
            .try_send_vectored_no_poll(&[IoSlice::new(b"pinned")])
            .unwrap();
        fixture.state().cq_result = code;
        assert!(fixture.endpoint().try_progress().is_err());
        assert!(fixture.endpoint().failed);
        assert!(fixture.endpoint().sends_pending());
        assert_eq!(fixture.state().error_reads, usize::from(code < 0));
        assert_eq!(
            fixture
                .endpoint()
                .try_send_vectored_no_poll(&[IoSlice::new(b"rejected")])
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotConnected
        );
        assert_eq!(fixture.state().sends, 1);
        drop(fixture.endpoint.take());
        assert_eq!(fixture.state().closed_byte, Some(b'p'));
    }
}

#[test]
fn open_profile_snapshot_never_polls_and_first_progress_is_not_retimed() {
    let mut fixture = Fixture::new();
    assert_eq!(fixture.endpoint().open_profile(), OpenProfile::default());
    assert_eq!(fixture.state().polls, 0);
    assert!(!fixture.endpoint().inner().initial_progress_recorded);
    fixture.endpoint().try_progress().unwrap();
    assert!(fixture.endpoint().inner().initial_progress_recorded);
    // A deterministic sentinel distinguishes a preserved first-stage sample
    // from an accidental new measurement; no clock-resolution assumptions.
    fixture.endpoint().inner().open_profile.first_progress_ns = 37;
    fixture.endpoint().configure_progress_stats(false, true);
    fixture.endpoint().try_progress().unwrap();
    assert_eq!(fixture.endpoint().open_profile().first_progress_ns, 37);
    assert_eq!(fixture.endpoint().progress_stats().cq_polls, 1);
    assert_eq!(fixture.state().polls, 2);
}

#[test]
fn eagain_keeps_the_filled_slot_and_retries_without_copying_again() {
    let mut fixture = Fixture::new();
    fixture.endpoint().configure_progress_stats(true, true);
    fixture.state().send_result = -isize::try_from(ffi::FI_EAGAIN).unwrap();
    for _ in 0..3 {
        assert_eq!(
            fixture
                .endpoint()
                .try_send_vectored_no_poll(&[IoSlice::new(b"pay"), IoSlice::new(b"load")])
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
    }
    let stats = fixture.endpoint().progress_stats();
    assert_eq!(stats.send_copy_calls, 1, "only the first attempt may copy");
    assert_eq!(stats.send_copy_bytes, 7);
    assert_eq!(stats.send_post_eagain, 3);
    assert_eq!(stats.send_post_retries, 2);
    assert_eq!(fixture.endpoint().accepted_send_fence().unwrap(), 0);
    assert!(!fixture.endpoint().sends_pending());

    fixture.state().send_result = 0;
    assert_eq!(
        fixture
            .endpoint()
            .try_send_vectored_no_poll(&[IoSlice::new(b"pay"), IoSlice::new(b"load")])
            .unwrap(),
        7
    );
    let stats = fixture.endpoint().progress_stats();
    assert_eq!(
        stats.send_copy_calls, 1,
        "the successful retry must not copy"
    );
    assert_eq!(stats.send_post_retries, 3);
    assert_eq!(stats.send_size_le_4k, 1);
    assert_eq!(stats.send_post_bytes, 7);
    assert_eq!(fixture.endpoint().accepted_send_fence().unwrap(), 1);
    // SAFETY: The successful post owns these seven initialized TX bytes.
    assert_eq!(
        unsafe { std::slice::from_raw_parts(fixture.state().sent, 7) },
        b"payload"
    );
}

#[test]
fn changed_bytes_after_eagain_are_copied_fresh_never_posted_stale() {
    let mut fixture = Fixture::new();
    fixture.endpoint().configure_progress_stats(true, true);
    fixture.state().send_result = -isize::try_from(ffi::FI_EAGAIN).unwrap();
    fixture.endpoint().try_send(b"first").unwrap_err();
    fixture.state().send_result = 0;
    // Same length, different content, then different length: both need a copy.
    assert_eq!(fixture.endpoint().try_send(b"other").unwrap(), 5);
    // SAFETY: The successful post owns these five initialized TX bytes.
    assert_eq!(
        unsafe { std::slice::from_raw_parts(fixture.state().sent, 5) },
        b"other"
    );
    fixture.state().send_result = -isize::try_from(ffi::FI_EAGAIN).unwrap();
    fixture.endpoint().try_send(b"abc").unwrap_err();
    fixture.state().send_result = 0;
    assert_eq!(fixture.endpoint().try_send(b"abcdef").unwrap(), 6);
    // SAFETY: As above for the six posted bytes.
    assert_eq!(
        unsafe { std::slice::from_raw_parts(fixture.state().sent, 6) },
        b"abcdef"
    );
    let stats = fixture.endpoint().progress_stats();
    assert_eq!(stats.send_copy_calls, 4);
    assert_eq!(stats.send_post_retries, 0);
}

#[test]
fn staged_slot_is_not_a_provider_owned_slot() {
    // A kept EAGAIN copy owns no ordinal; filling every other slot and then
    // completing the ring must leave fences exact and reuse the staged slot.
    let mut fixture = Fixture::new();
    fixture.state().send_result = -isize::try_from(ffi::FI_EAGAIN).unwrap();
    fixture
        .endpoint()
        .try_send_vectored_no_poll(&[IoSlice::new(b"kept")])
        .unwrap_err();
    assert_eq!(fixture.endpoint().inner().free_send_slot(), Some(0));
    fixture.state().send_result = 0;
    for byte in 0..SLOTS {
        fixture
            .endpoint()
            .try_send_vectored_no_poll(&[IoSlice::new(&[u8::try_from(byte).unwrap()])])
            .unwrap();
    }
    assert_eq!(
        fixture.endpoint().accepted_send_fence().unwrap(),
        SLOTS as u64
    );
    assert_eq!(fixture.endpoint().inner().free_send_slot(), None);
}

#[test]
fn send_buckets_follow_posted_message_length() {
    let mut fixture = Fixture::new();
    for (len, bucket) in [
        (1, 0),
        (4096, 0),
        (4097, 1),
        (16_384, 1),
        (16_385, 2),
        (CHUNK, 2),
    ] {
        let state = fixture.endpoint().inner();
        let before = state.progress_stats();
        state.record_send_bucket(len);
        let after = state.progress_stats();
        let delta = [
            after.send_size_le_4k - before.send_size_le_4k,
            after.send_size_le_16k - before.send_size_le_16k,
            after.send_size_le_64k - before.send_size_le_64k,
            after.send_size_gt_64k - before.send_size_gt_64k,
        ];
        let mut expected = [0; 4];
        expected[bucket] = 1;
        assert_eq!(delta, expected, "length {len}");
    }
    fixture.endpoint().inner().record_send_bucket(CHUNK + 1);
    assert_eq!(fixture.endpoint().progress_stats().send_size_gt_64k, 1);
}

#[test]
fn configured_slot_count_sizes_every_per_slot_structure() {
    for slots in [1, 2, 8, crate::MAX_SLOTS] {
        let mut resources = Resources::default();
        resources.set_slots(slots);
        assert_eq!(resources.slots(), slots);
        assert_eq!(resources.tx_pending.len(), slots);
        assert_eq!(resources.tx_ordinal.len(), slots);
        assert_eq!(resources.rx_pending.len(), slots);
        assert_eq!(resources.rx_len.len(), slots);
        // Every slot's buffer and context is distinct, in bounds and maps back
        // to exactly its own index; a neighbor's pointer never aliases it.
        for slot in 0..slots {
            assert_eq!(
                resources
                    .memory
                    .tx_context_slot(resources.memory.tx_context(slot)),
                Some(slot)
            );
            assert_eq!(
                resources
                    .memory
                    .rx_context_slot(resources.memory.rx_context(slot)),
                Some(slot)
            );
            assert_eq!(
                resources
                    .memory
                    .tx_context_slot(resources.memory.rx_context(slot)),
                None
            );
        }
        // SAFETY: Both writes target in-bounds bytes of the last slot's buffers,
        // which no provider references in this unit test.
        unsafe {
            resources.memory.tx(slots - 1).add(CHUNK - 1).write(7);
            resources.memory.rx(slots - 1).add(CHUNK - 1).write(9);
        }
        let past = resources.memory.tx_context(slots - 1);
        // One byte past a context is not a context.
        assert_eq!(
            resources.memory.tx_context_slot(past.wrapping_byte_add(1)),
            None
        );
    }
}

#[test]
fn full_ring_of_configured_size_requires_exact_completions() {
    let mut resources = Resources::default();
    resources.set_slots(8);
    resources.peer = Some(0);
    for slot in 0..8 {
        resources.tx_pending[slot] = true;
        resources.tx_ordinal[slot] = slot as u64 + 1;
    }
    resources.accepted_send = 8;
    assert_eq!(resources.free_send_slot(), None);
    // Out-of-order completion of slot 5 frees only slot 5.
    resources
        .complete(resources.memory.tx_context(5), || unreachable!())
        .unwrap();
    assert_eq!(resources.free_send_slot(), Some(5));
    assert_eq!(resources.send_fences(), (8, 0));
    for slot in [0, 1, 2, 3, 4, 6, 7] {
        resources
            .complete(resources.memory.tx_context(slot), || unreachable!())
            .unwrap();
    }
    assert_eq!(resources.send_fences(), (8, 8));
}

/// A pipe stands in for the provider's CQ wait fd: its read end is never
/// readable, so `poll(2)` runs for the full bounded timeout.
struct WaitPipe([libc::c_int; 2]);

impl WaitPipe {
    fn new() -> Self {
        let mut fds = [0; 2];
        // SAFETY: `fds` is a writable two-element array, as pipe(2) requires.
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        Self(fds)
    }
}

impl Drop for WaitPipe {
    fn drop(&mut self) {
        // SAFETY: Both descriptors were opened by `new` and are closed once.
        unsafe {
            libc::close(self.0[0]);
            libc::close(self.0[1]);
        }
    }
}

/// Installs a scripted `fi_trywait` fabric on a fixture's resources and returns
/// the heap boxes that must outlive the endpoint.
fn install_wait_fabric(
    fixture: &mut Fixture,
    wait_fd: libc::c_int,
) -> (
    Box<ffi::fi_ops>,
    Box<ffi::fi_ops_fabric>,
    Box<ffi::fid_fabric>,
) {
    let context = fixture.state.get().cast::<c_void>();
    let mut ops = Box::new(ffi::fi_ops {
        close: Some(close),
        ..ffi::fi_ops::default()
    });
    let mut fabric_ops = Box::new(ffi::fi_ops_fabric {
        trywait: Some(trywait),
        ..ffi::fi_ops_fabric::default()
    });
    let mut fabric = Box::new(ffi::fid_fabric {
        fid: ffi::fid {
            context,
            ops: &raw mut *ops,
            ..ffi::fid::default()
        },
        ops: &raw mut *fabric_ops,
        ..ffi::fid_fabric::default()
    });
    let state = fixture.endpoint().inner();
    state.fabric = &raw mut *fabric;
    state.cq_wait_mode = CqWaitMode::FdTryWait(wait_fd);
    state.rx_pending.fill(true);
    (ops, fabric_ops, fabric)
}

#[test]
fn idle_sweep_parks_on_the_wait_fd_only_after_trywait_allows_it() {
    let pipe = WaitPipe::new();
    let mut fixture = Fixture::new();
    let keep = install_wait_fabric(&mut fixture, pipe.0[0]);
    let deadline = Instant::now() + Duration::from_secs(10);

    // fi_trywait == 0: the CQ is quiescent, so this sweep blocks on the fd.
    let started = Instant::now();
    assert_eq!(
        fixture
            .endpoint()
            .try_progress_for_sweep(Some(deadline))
            .unwrap(),
        (false, true)
    );
    assert!(started.elapsed() >= Duration::from_micros(500));
    assert_eq!(fixture.state().trywaits, 1);
    assert_eq!(
        fixture.state().polls,
        1,
        "one nonblocking CQ read after waking"
    );
    assert_eq!(fixture.endpoint().progress_stats().cq_idle_waits, 1);

    // -FI_EAGAIN: completions are pending inside the provider; never block.
    fixture.state().trywait_result = -i32::try_from(ffi::FI_EAGAIN).unwrap();
    let started = Instant::now();
    assert_eq!(
        fixture
            .endpoint()
            .try_progress_for_sweep(Some(deadline))
            .unwrap(),
        (false, false)
    );
    assert!(started.elapsed() < Duration::from_millis(500));
    assert_eq!(fixture.state().trywaits, 2);
    assert_eq!(fixture.endpoint().progress_stats().cq_idle_waits, 1);
    drop(fixture.endpoint.take());
    drop(keep);
}

#[test]
fn outstanding_requests_keep_busy_polling_instead_of_parking() {
    let pipe = WaitPipe::new();
    let mut fixture = Fixture::new();
    let keep = install_wait_fabric(&mut fixture, pipe.0[0]);
    let deadline = Instant::now() + Duration::from_secs(10);
    // An accepted send awaiting its local completion forbids parking.
    fixture
        .endpoint()
        .try_send_vectored_no_poll(&[IoSlice::new(b"x")])
        .unwrap();
    assert!(fixture.endpoint().sends_pending());
    fixture
        .endpoint()
        .try_progress_for_sweep(Some(deadline))
        .unwrap();
    assert_eq!(fixture.state().trywaits, 0);
    // An unread received slot forbids parking as well.
    let completion = fixture.state().sent_context;
    fixture.state().completion = Some(ffi::fi_cq_msg_entry {
        op_context: completion,
        ..ffi::fi_cq_msg_entry::default()
    });
    fixture.endpoint().try_progress().unwrap();
    assert!(!fixture.endpoint().sends_pending());
    fixture.endpoint().inner().rx_len[0] = 1;
    fixture
        .endpoint()
        .try_progress_for_sweep(Some(deadline))
        .unwrap();
    assert_eq!(fixture.state().trywaits, 0);
    assert_eq!(fixture.endpoint().progress_stats().cq_idle_waits, 0);
    fixture.endpoint().inner().rx_len[0] = 0;
    drop(fixture.endpoint.take());
    drop(keep);
}

#[test]
fn help_defaults_parse_the_forms_libfabric_prints() {
    assert_eq!(
        help_default("size (default 16384)."),
        Some("16384".to_owned())
    );
    assert_eq!(help_default("(default: 10000)."), Some("10000".to_owned()));
    assert_eq!(
        help_default("(default: 128, 4096 with srx"),
        Some("128".to_owned())
    );
    assert_eq!(
        help_default("(default: false/no)."),
        Some("false/no".to_owned())
    );
    assert_eq!(help_default("no stated value"), None);
    assert_eq!(help_default("(default)"), None);
}

#[test]
fn only_owners_that_park_request_a_verbs_wait_fd() {
    use FabricBackend::{SoftwareTcp, Verbs};
    // (backend, park_idle) -> requested wait object
    for ((backend, park_idle), expected) in [
        ((Verbs, true), WaitRequest::Fd),
        // A polling client never pays for a wait object.
        ((Verbs, false), WaitRequest::None),
        ((SoftwareTcp, false), WaitRequest::Software),
        ((SoftwareTcp, true), WaitRequest::Software),
    ] {
        assert_eq!(
            WaitRequest::select(backend, park_idle),
            expected,
            "{backend:?} park={park_idle}"
        );
    }
}
