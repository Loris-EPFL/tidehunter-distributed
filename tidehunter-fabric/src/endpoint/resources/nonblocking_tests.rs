//! Test-only provider dispatch exercises the actual public nonblocking methods.
//! No network timing assumptions: every CQ read/post is counted and scripted.

use super::super::{Endpoint, LIVE};
use super::*;
use std::{
    sync::atomic::Ordering,
    thread,
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
}

// SAFETY: Only the fixture invokes these callbacks through generated libfabric
// dispatch. All handles and vtables have stable heap addresses until Endpoint
// closes. Their context points to the fixture's live UnsafeCell<Provider>. Calls
// are serialized, with no Provider reference live across an Endpoint call.
unsafe extern "C" fn read_cq(cq: *mut ffi::fid_cq, buf: *mut c_void, count: usize) -> isize {
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
    // SAFETY: Same fixture ownership as read_cq; poll supplies one error entry.
    let state = unsafe { &mut *(*cq).fid.context.cast::<Provider>() };
    state.error_reads += 1;
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
    // SAFETY: The fixture context is live and exclusively accessed in this call.
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
    // SAFETY: The fixture context is live and exclusively accessed in this call.
    let state = unsafe { &mut *(*ep).fid.context.cast::<Provider>() };
    state.received = buf.cast();
    state.received_context = context;
    state.receives += 1;
    state.receive_result
}

unsafe extern "C" fn close(fid: *mut ffi::fid) -> i32 {
    // SAFETY: Both fixture handles use this context. Endpoint must retain sent
    // bytes until endpoint close, which precedes CQ close and buffer release.
    let state = unsafe { &mut *(*fid).context.cast::<Provider>() };
    if state.closes == 0 && !state.sent.is_null() {
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
        Self::new_with_direct(false)
    }

    fn new_direct() -> Self {
        Self::new_with_direct(true)
    }

    fn new_with_direct(direct: bool) -> Self {
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
            direct,
            ..Resources::default()
        };
        // Balance Endpoint::drop's admission release without opening real handles.
        LIVE.fetch_add(1, Ordering::AcqRel);
        Self {
            endpoint: Some(Endpoint {
                resources: Some(Box::new(resources)),
                direct: direct.then(|| Box::new(super::super::DirectState::new(4096).unwrap())),
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
fn direct_eagain_accepts_no_bytes_and_local_cqe_does_not_grant_peer_credit() {
    let mut fixture = Fixture::new_direct();
    fixture.state().send_result = -isize::try_from(ffi::FI_EAGAIN).unwrap();
    assert_eq!(
        fixture
            .endpoint()
            .try_send(b"discarded")
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    let direct = fixture.endpoint().direct.as_ref().unwrap();
    assert_eq!(direct.send.next_ordinal().unwrap(), 1);
    assert_eq!(fixture.endpoint().inner().send_fences(), (0, 0));
    assert_eq!(fixture.endpoint().inner().free_send_slot(), Some(0));
    assert_eq!(
        fixture.endpoint().inner().free_control_slot(),
        Some(SLOTS - 1)
    );

    fixture.state().send_result = 0;
    assert_eq!(fixture.endpoint().try_send(b"payload").unwrap(), 7);
    let (sent, len, context) = {
        let state = fixture.state();
        (state.sent, state.sent_len, state.sent_context)
    };
    // SAFETY: The successful post owns this immutable slot until its CQ event.
    let packet = unsafe { std::slice::from_raw_parts(sent, len) };
    let super::super::efa_direct::Packet::Data {
        ordinal, payload, ..
    } = super::super::efa_direct::decode(packet, 4096).unwrap()
    else {
        panic!("expected EFA-direct data packet");
    };
    assert_eq!(ordinal, 1);
    assert_eq!(payload, b"payload");
    assert_eq!(fixture.endpoint().inner().send_fences(), (1, 0));
    fixture.state().completion = Some(ffi::fi_cq_msg_entry {
        op_context: context,
        ..ffi::fi_cq_msg_entry::default()
    });
    assert!(fixture.endpoint().try_progress().unwrap());
    assert_eq!(fixture.endpoint().inner().send_fences(), (1, 1));
    let direct = fixture.endpoint().direct.as_ref().unwrap();
    assert_eq!(direct.send.peer_credit(), 0);
    assert_eq!(direct.send.next_ordinal().unwrap(), 2);
    assert_eq!(fixture.endpoint().inner().free_send_slot(), Some(0));
}

#[test]
fn direct_ack_eagain_is_retried_by_empty_flush_until_its_cq_completes() {
    let mut fixture = Fixture::new_direct();
    assert!(fixture.endpoint().try_progress().unwrap());
    let (receive_buffer, receive_context) = {
        let state = fixture.state();
        (state.received, state.received_context)
    };
    assert!(!receive_buffer.is_null());
    let mut incoming = [0; 4096];
    let len = super::super::efa_direct::encode_data(&mut incoming, 1, 0, b"reply", 4096)
        .unwrap()
        .len();
    // SAFETY: The fixture's successful receive post owns a full CHUNK slot.
    unsafe { std::ptr::copy_nonoverlapping(incoming.as_ptr(), receive_buffer, len) };
    fixture.state().completion = Some(ffi::fi_cq_msg_entry {
        op_context: receive_context,
        len,
        ..ffi::fi_cq_msg_entry::default()
    });
    fixture.state().send_result = -isize::try_from(ffi::FI_EAGAIN).unwrap();
    let mut output = [0; 5];
    assert_eq!(fixture.endpoint().try_receive(&mut output).unwrap(), 5);
    assert_eq!(&output, b"reply");
    assert_eq!(fixture.endpoint().inner().send_fences(), (0, 0));
    assert!(
        fixture
            .endpoint()
            .direct
            .as_ref()
            .unwrap()
            .control
            .needs_send(1)
    );

    fixture.state().send_result = 0;
    fixture.state().auto_complete = true;
    fixture
        .endpoint()
        .flush(Instant::now() + Duration::from_secs(1))
        .unwrap();
    assert_eq!(fixture.endpoint().inner().send_fences(), (1, 1));
    assert_eq!(fixture.state().sends, 2);
    let (sent, len) = {
        let state = fixture.state();
        (state.sent, state.sent_len)
    };
    // SAFETY: The completed control slot remains allocated until endpoint close.
    let packet = unsafe { std::slice::from_raw_parts(sent, len) };
    assert!(matches!(
        super::super::efa_direct::decode(packet, 4096).unwrap(),
        super::super::efa_direct::Packet::Credit {
            ordinal: 1,
            consumed: 1
        }
    ));
    let direct = fixture.endpoint().direct.as_ref().unwrap();
    assert_eq!(direct.receive.consumed(), 1);
    assert!(!direct.control.needs_send(1));
}
#[test]
fn sweep_data_send_piggybacks_consumed_credit_without_control_packet() {
    let mut fixture = Fixture::new_direct();
    fixture
        .endpoint()
        .direct
        .as_mut()
        .unwrap()
        .receive
        .accept_data(1, b"reply")
        .unwrap();
    assert!(fixture.endpoint().try_progress_for_sweep().unwrap());
    assert_eq!(fixture.state().polls, 1);
    assert_eq!(fixture.state().sends, 0);
    let mut received = [0; 5];
    assert_eq!(
        fixture
            .endpoint()
            .try_receive_no_poll(&mut received)
            .unwrap(),
        5
    );
    assert_eq!(&received, b"reply");
    assert_eq!(fixture.state().polls, 1);
    fixture.state().send_result = -isize::try_from(ffi::FI_EAGAIN).unwrap();
    assert_eq!(
        fixture
            .endpoint()
            .try_send_vectored_no_poll(&[std::io::IoSlice::new(b"discarded")])
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(
        fixture
            .endpoint()
            .direct
            .as_ref()
            .unwrap()
            .control
            .needs_send(1)
    );
    assert_eq!(
        fixture
            .endpoint()
            .direct
            .as_ref()
            .unwrap()
            .send
            .next_ordinal()
            .unwrap(),
        1
    );
    fixture.state().send_result = 0;
    assert_eq!(
        fixture
            .endpoint()
            .try_send_vectored_no_poll(&[std::io::IoSlice::new(b"request")])
            .unwrap(),
        7
    );
    let (sent, len) = {
        let state = fixture.state();
        (state.sent, state.sent_len)
    };
    // SAFETY: Successful provider post owns this fixed send slot.
    let packet = unsafe { std::slice::from_raw_parts(sent, len) };
    let super::super::efa_direct::Packet::Data {
        ordinal,
        credit,
        payload,
    } = super::super::efa_direct::decode(packet, 4096).unwrap()
    else {
        panic!("expected DATA packet");
    };
    assert_eq!(ordinal, 1);
    assert_eq!(credit, 1);
    assert_eq!(payload, b"request");
    assert!(
        !fixture
            .endpoint()
            .direct
            .as_ref()
            .unwrap()
            .control
            .needs_send(1)
    );
    assert!(!fixture.endpoint().finish_sweep_no_poll().unwrap());
    assert_eq!(fixture.state().sends, 2);
}

#[test]
fn sweep_without_data_output_retries_standalone_credit_after_eagain() {
    let mut fixture = Fixture::new_direct();
    fixture
        .endpoint()
        .direct
        .as_mut()
        .unwrap()
        .receive
        .accept_data(1, b"reply")
        .unwrap();
    let mut received = [0; 5];
    assert_eq!(
        fixture
            .endpoint()
            .try_receive_no_poll(&mut received)
            .unwrap(),
        5
    );
    fixture.state().send_result = -isize::try_from(ffi::FI_EAGAIN).unwrap();
    assert!(!fixture.endpoint().finish_sweep_no_poll().unwrap());
    assert!(
        fixture
            .endpoint()
            .direct
            .as_ref()
            .unwrap()
            .control
            .needs_send(1)
    );
    fixture.state().send_result = 0;
    assert!(fixture.endpoint().finish_sweep_no_poll().unwrap());
    assert_eq!(fixture.state().polls, 0);
    let (sent, len) = {
        let state = fixture.state();
        (state.sent, state.sent_len)
    };
    // SAFETY: Successful provider post owns the reserved control slot.
    let packet = unsafe { std::slice::from_raw_parts(sent, len) };
    assert!(matches!(
        super::super::efa_direct::decode(packet, 4096).unwrap(),
        super::super::efa_direct::Packet::Credit {
            ordinal: 1,
            consumed: 1
        }
    ));
}

#[test]
fn blocking_receive_posts_credit_for_sweep_resident_direct_data() {
    let mut fixture = Fixture::new_direct();
    assert!(fixture.endpoint().try_progress_for_sweep().unwrap());
    let (receive_buffer, receive_context) = {
        let state = fixture.state();
        (state.received, state.received_context)
    };
    let mut incoming = [0; 4096];
    let len = super::super::efa_direct::encode_data(&mut incoming, 1, 0, b"reply", 4096)
        .unwrap()
        .len();
    // SAFETY: The fixture's receive post owns a full CHUNK slot.
    unsafe { std::ptr::copy_nonoverlapping(incoming.as_ptr(), receive_buffer, len) };
    fixture.state().completion = Some(ffi::fi_cq_msg_entry {
        op_context: receive_context,
        len,
        ..ffi::fi_cq_msg_entry::default()
    });
    assert!(fixture.endpoint().try_progress_for_sweep().unwrap());
    assert_eq!(fixture.state().sends, 0);

    let mut output = [0; 5];
    assert_eq!(
        fixture
            .endpoint()
            .receive(&mut output, Instant::now() + Duration::from_secs(1))
            .unwrap(),
        output.len()
    );
    assert_eq!(&output, b"reply");
    assert_eq!(fixture.state().sends, 1);
    let (sent, sent_len) = {
        let state = fixture.state();
        (state.sent, state.sent_len)
    };
    // SAFETY: The successful CREDIT post owns the reserved control slot.
    let packet = unsafe { std::slice::from_raw_parts(sent, sent_len) };
    assert!(matches!(
        super::super::efa_direct::decode(packet, 4096).unwrap(),
        super::super::efa_direct::Packet::Credit {
            ordinal: 1,
            consumed: 1
        }
    ));
}
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm and local network permission"]
#[allow(
    clippy::too_many_lines,
    reason = "Keep the two-sided software-provider adapter exercise together"
)]
fn software_provider_runs_direct_packet_adapter_on_large_partial_streams() -> io::Result<()> {
    fn exchange(mut endpoint: Endpoint, outbound: &[u8], inbound: &[u8]) -> io::Result<Endpoint> {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut sent = 0;
        let mut received = Vec::with_capacity(inbound.len());
        let mut buffer = [0; 8179];
        loop {
            if Instant::now() >= deadline {
                return Err(io::ErrorKind::TimedOut.into());
            }
            if sent < outbound.len() {
                match endpoint.try_send(&outbound[sent..]) {
                    Ok(len) => sent += len,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                    Err(error) => return Err(error),
                }
            }
            if received.len() < inbound.len() {
                let amount =
                    (1 + received.len() % buffer.len()).min(inbound.len() - received.len());
                match endpoint.try_receive(&mut buffer[..amount]) {
                    Ok(len) => received.extend_from_slice(&buffer[..len]),
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                    Err(error) => return Err(error),
                }
            }
            endpoint.try_progress()?;
            if sent == outbound.len() && received.len() == inbound.len() {
                endpoint.flush(deadline)?;
                assert_eq!(received, inbound);
                // Local CQ completion is not peer consumption. Keep this live
                // endpoint until both stream consumers finish and flush.
                return Ok(endpoint);
            }
        }
    }

    let mut left = Endpoint::open_software_packet_test(Some("lo"))?;
    let mut right = Endpoint::open_software_packet_test(Some("lo"))?;
    let left_address = left.address()?;
    let right_address = right.address()?;
    left.set_peer(&right_address)?;
    right.set_peer(&left_address)?;

    let left_out: Vec<u8> = (0..1_000_003)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect();
    let right_out: Vec<u8> = (0..1_000_019)
        .map(|index| u8::try_from(index % 239).unwrap())
        .collect();
    thread::scope(|scope| {
        let left_task = scope.spawn(|| exchange(left, &left_out, &right_out));
        let right_result = exchange(right, &right_out, &left_out);
        let left_result = left_task.join().unwrap();
        let _right = right_result?;
        let _left = left_result?;
        Ok(())
    })
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
    state.rx_pending = [true; SLOTS];
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
