//! These explicit tests exercise libfabric's software provider, not RDMA speed.
#![cfg(feature = "native")]

use std::{
    io::{self, IoSlice},
    thread,
    time::{Duration, Instant},
};
use tidehunter_fabric::{Endpoint, PeerDrainToken, RemoteReadRegion, RmaConfig};

fn pair() -> io::Result<(Endpoint, Endpoint)> {
    let mut a = Endpoint::open("tcp;ofi_rxm", Some("lo"))?;
    let mut b = Endpoint::open("tcp;ofi_rxm", Some("lo"))?;
    let a_addr = a.address()?;
    let b_addr = b.address()?;
    a.set_peer(&b_addr)?;
    b.set_peer(&a_addr)?;
    Ok((a, b))
}
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm and local network permission"]
#[allow(
    clippy::too_many_lines,
    reason = "Exercise one registered grant through CQ mixing, retained ownership, cancellation and reuse."
)]
fn software_provider_rma_reads_with_mixed_cq_and_retained_buffer_ownership() -> io::Result<()> {
    let config = RmaConfig {
        source_bytes: 8192,
        source_region_slots: 1,
        destination_bytes: 8192,
        destination_slots: 1,
        max_read_bytes: 8192,
        require_device_read: false,
    };
    let mut a = Endpoint::open_rma("tcp;ofi_rxm", Some("lo"), config)?;
    let mut b = Endpoint::open_rma("tcp;ofi_rxm", Some("lo"), config)?;
    let mut wrong_endpoint = Endpoint::open_rma("tcp;ofi_rxm", Some("lo"), config)?;
    let a_address = a.address()?;
    let b_address = b.address()?;
    a.set_peer(&b_address)?;
    b.set_peer(&a_address)?;
    wrong_endpoint.set_peer(&b_address)?;

    let payload: Vec<u8> = (0..4096)
        .map(|index| u8::try_from(index % 239).unwrap())
        .collect();
    let grant = b.reserve_read_region(44, 3)?;
    let mut expected = Vec::with_capacity(payload.len() + 16);
    expected.extend_from_slice(&grant.region_id().to_le_bytes());
    expected.extend_from_slice(&grant.generation().to_le_bytes());
    expected.extend_from_slice(&payload);
    let remote = b.register_read_region(&grant, expected.clone())?;
    assert!(matches!(
        b.reserve_read_region(44, 3),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock
    ));
    assert!(matches!(
        a.register_read_region(&grant, expected.clone()),
        Err(error) if error.kind() == io::ErrorKind::InvalidInput
    ));
    assert_eq!(a.available_read_slots()?, 1);
    assert_eq!(wrong_endpoint.available_read_slots()?, 1);
    assert_eq!(usize::try_from(remote.len).ok(), Some(expected.len()));
    assert!(matches!(
        a.try_post_read(&remote, expected.len() - 1, 2),
        Err(error) if error.kind() == io::ErrorKind::InvalidInput
    ));

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut accepted = None;
    for _ in 0..1_000_000 {
        assert!(Instant::now() < deadline, "RMA connection setup stalled");
        match a.try_post_read(&remote, 0, expected.len()) {
            Ok(operation) => {
                accepted = Some(operation);
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                assert_eq!(a.available_read_slots()?, 1);
                progress_nonblocking(&mut a)?;
                progress_nonblocking(&mut b)?;
                thread::yield_now();
            }
            Err(error) => return Err(error),
        }
    }
    let mut operation = accepted
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "RMA post did not become ready"))?;
    assert_eq!(a.available_read_slots()?, 0);
    assert!(matches!(
        wrong_endpoint.try_read_completion(&mut operation),
        Err(error) if error.kind() == io::ErrorKind::InvalidInput
    ));
    assert!(matches!(
        a.try_post_read(&remote, 0, expected.len()),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock
    ));
    assert_eq!(a.try_send(b"mixed")?, 5);
    let mut received = [0; 5];
    let mut received_len = 0;
    let mut read_result = None;
    while received_len != received.len() || read_result.is_none() || a.sends_pending() {
        assert!(Instant::now() < deadline, "software-provider RMA stalled");
        progress_nonblocking(&mut a)?;
        progress_nonblocking(&mut b)?;
        if received_len < received.len() {
            match b.try_receive(&mut received[received_len..]) {
                Ok(len) => received_len += len,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error),
            }
        }
        if read_result.is_none()
            && let Some(result) = a.try_read_completion(&mut operation)?
        {
            read_result = Some(result?.into_minibytes_bytes());
        }
    }
    assert_eq!(&received, b"mixed");
    assert_eq!(read_result.as_deref(), Some(expected.as_slice()));
    assert_eq!(a.available_read_slots()?, 0);

    assert!(matches!(
        a.try_post_read(&remote, 0, expected.len()),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock
    ));
    drop(read_result);
    assert_eq!(a.available_read_slots()?, 1);

    let window = RemoteReadRegion {
        remote_address: remote.remote_address.checked_add(128).unwrap(),
        len: remote.len - 128,
        ..remote
    };
    let cancelled = a.try_post_read(&window, 0, 256)?;
    drop(cancelled);
    let mut recovered = loop {
        assert!(Instant::now() < deadline, "cancelled RMA did not retire");
        progress_nonblocking(&mut a)?;
        progress_nonblocking(&mut b)?;
        match a.try_post_read(&window, 0, 256) {
            Ok(operation) => break operation,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
    };
    let buffer = loop {
        assert!(Instant::now() < deadline, "recovered RMA did not complete");
        progress_nonblocking(&mut b)?;
        if let Some(result) = a.try_read_completion(&mut recovered)? {
            break result?;
        }
    };
    assert_eq!(buffer.as_ref(), &expected[128..384]);
    drop(buffer);

    // SAFETY: The test has completed all accepted reads and will not post more.
    b.retire_read_region(grant, unsafe { PeerDrainToken::after_ack(44, 3) })?;
    Ok(())
}

fn post_group_read(
    local: &mut Endpoint,
    peer: &mut Endpoint,
    remote: &RemoteReadRegion,
    offset: usize,
    deadline: Instant,
) -> io::Result<tidehunter_fabric::ReadOperation> {
    for _ in 0..1_000_000 {
        if Instant::now() >= deadline {
            break;
        }
        match local.try_post_read(remote, offset, 256) {
            Ok(operation) => return Ok(operation),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                progress_nonblocking(local)?;
                progress_nonblocking(peer)?;
            }
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "group RMA post stalled",
    ))
}

fn collect_group_reads(
    local: &mut Endpoint,
    peer: &mut Endpoint,
    operations: &mut [tidehunter_fabric::ReadOperation],
    deadline: Instant,
) -> io::Result<Vec<Option<minibytes::Bytes>>> {
    let mut records = vec![None; operations.len()];
    for _ in 0..1_000_000 {
        if Instant::now() >= deadline {
            break;
        }
        progress_nonblocking(peer)?;
        progress_nonblocking(local)?;
        // Harvest in reverse caller order, independently of provider CQ order.
        for index in (0..operations.len()).rev() {
            if records[index].is_none()
                && let Some(result) = local.take_read_completion(&mut operations[index])?
            {
                records[index] = Some(result?.into_minibytes_bytes());
            }
        }
        if records.iter().all(Option::is_some) {
            return Ok(records);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "group RMA completion stalled",
    ))
}

#[test]
#[ignore = "requires libfabric tcp;ofi_rxm and local network permission"]
fn software_provider_rma_group_preserves_order_with_retained_consumers() -> io::Result<()> {
    let config = RmaConfig {
        source_bytes: 8192,
        source_region_slots: 1,
        destination_bytes: 8192,
        destination_slots: 32,
        max_read_bytes: 256,
        require_device_read: false,
    };
    let mut local = Endpoint::open_rma("tcp;ofi_rxm", Some("lo"), config)?;
    let mut peer = Endpoint::open_rma("tcp;ofi_rxm", Some("lo"), config)?;
    local.set_peer(&peer.address()?)?;
    peer.set_peer(&local.address()?)?;
    let region = peer.reserve_read_region(91, 5)?;
    let payload: Vec<_> = (0..8192)
        .map(|index| u8::try_from(index / 256).unwrap())
        .collect();
    let remote = peer.register_read_region(&region, payload)?;
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut operations = Vec::with_capacity(32);
    for index in 0..32 {
        operations.push(post_group_read(
            &mut local,
            &mut peer,
            &remote,
            index * 256,
            deadline,
        )?);
    }
    let mut first = collect_group_reads(&mut local, &mut peer, &mut operations, deadline)?;
    assert_eq!(local.available_read_slots()?, 0);
    assert!(matches!(local.try_post_read(&remote, 0, 256),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock));
    for (index, record) in first.iter().enumerate() {
        assert_eq!(
            record.as_ref().unwrap().as_ref(),
            &[u8::try_from(index).unwrap(); 256]
        );
    }
    for index in (0..32).step_by(2) {
        drop(first[index].take());
    }
    assert_eq!(local.available_read_slots()?, 16);
    operations.clear();
    for index in 0..16 {
        operations.push(post_group_read(
            &mut local,
            &mut peer,
            &remote,
            (31 - index) * 256,
            deadline,
        )?);
    }
    let second = collect_group_reads(&mut local, &mut peer, &mut operations, deadline)?;
    for (index, record) in second.iter().enumerate() {
        assert_eq!(
            record.as_ref().unwrap().as_ref(),
            &[u8::try_from(31 - index).unwrap(); 256]
        );
    }
    for index in (1..32).step_by(2) {
        assert_eq!(
            first[index].as_ref().unwrap().as_ref(),
            &[u8::try_from(index).unwrap(); 256]
        );
    }
    drop(first);
    drop(second);
    assert_eq!(local.available_read_slots()?, 32);
    // SAFETY: Both groups reached terminal CQ and all returned buffers were
    // released. This exact peer will post no more reads for session 91/grant 5.
    peer.retire_read_region(region, unsafe { PeerDrainToken::after_ack(91, 5) })?;
    Ok(())
}

#[test]
#[ignore = "requires libfabric tcp;ofi_rxm and local network permission"]
fn software_provider_preserves_stream_bytes_across_reused_buffers() -> io::Result<()> {
    let (mut a, mut b) = pair()?;
    assert_eq!(
        a.send(&[1], Instant::now()).unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    assert_eq!(
        a.receive(&mut [0; 1], Instant::now()).unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    assert_eq!(
        a.progress(Instant::now()).unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    // An already-expired deadline must preserve the usable endpoint and the
    // initial receive while the following real transfer moves it across threads.
    let expected: Vec<u8> = (0..300_017)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect();
    let input = expected.clone();
    let deadline = Instant::now() + Duration::from_secs(10);
    let sender = thread::spawn(move || -> io::Result<()> {
        let mut offset = 0;
        while offset < input.len() {
            offset += a.send(&input[offset..], deadline)?;
        }
        let mut reply = [0; 17];
        let mut offset = 0;
        while offset < reply.len() {
            offset += a.receive(&mut reply[offset..], deadline)?;
        }
        assert_eq!(reply, [91; 17]);
        Ok(())
    });
    let mut received = Vec::new();
    let mut chunk = [0; 8179];
    for _ in 0..1000 {
        if received.len() == expected.len() {
            break;
        }
        let len = b.receive(&mut chunk, deadline)?;
        received.extend_from_slice(&chunk[..len]);
    }
    assert_eq!(received, expected);
    assert_eq!(b.send(&[91; 17], deadline)?, 17);
    sender.join().unwrap()?;
    Ok(())
}

#[test]
#[ignore = "requires libfabric tcp;ofi_rxm and local network permission"]
fn software_provider_timeout_poison_and_teardown_preserve_buffer_lifetimes() -> io::Result<()> {
    for _ in 0..8 {
        let (mut a, _b) = pair()?;
        let error = a
            .receive(&mut [0; 1], Instant::now() + Duration::from_millis(20))
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(
            a.send(&[1], Instant::now() + Duration::from_secs(1))
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotConnected
        );
        assert_eq!(
            a.progress(Instant::now() + Duration::from_secs(1))
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotConnected
        );
        // Dropping a failed endpoint must close pending receives before buffers.
    }
    Ok(())
}

#[test]
fn missing_provider_is_an_error_without_implicit_tcp_fallback() {
    assert!(Endpoint::open("tidehunter_missing_provider", None).is_err());
}

#[test]
#[ignore = "requires a real EFA device and libfabric efa-direct support"]
fn efa_direct_opens_only_the_negotiated_hardware_fabric() -> io::Result<()> {
    let endpoint = Endpoint::open_efa_direct(None)?;
    let profile = endpoint.open_profile();
    assert_eq!(profile.provider_version, 206 << 16);
    assert!(profile.mtu > 28);
    assert!(profile.selected_tx >= 4);
    assert!(profile.selected_rx >= 4);
    Ok(())
}

#[test]
#[ignore = "requires libfabric tcp;ofi_rxm and local network permission"]
fn software_provider_pipelines_vectored_chunks_and_wraps_both_rings() -> io::Result<()> {
    let (mut a, mut b) = pair()?;
    let deadline = Instant::now() + Duration::from_secs(20);
    let payload: Vec<u8> = (0..2_097_169)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect();
    let expected = payload.clone();
    let sender = thread::spawn(move || -> io::Result<()> {
        for _ in 0..3 {
            let mut offset = 0;
            while offset < payload.len() {
                let split = (offset + 17).min(payload.len());
                offset += a.queue_send_vectored(
                    &[
                        IoSlice::new(&payload[offset..split]),
                        IoSlice::new(&[]),
                        IoSlice::new(&payload[split..]),
                    ],
                    deadline,
                )?;
            }
            // Request/reply receives progress both directions after submission.
            a.progress(deadline)?;
            let mut reply = vec![0; payload.len()];
            let mut offset = 0;
            while offset < reply.len() {
                offset += a.receive(&mut reply[offset..], deadline)?;
            }
            assert_eq!(reply, payload);
            // Do not park between RPCs with provider-owned TX still pending.
            a.flush(deadline)?;
        }
        Ok(())
    });
    for round in 0..3 {
        let mut received = vec![0; expected.len()];
        let mut offset = 0;
        while offset < received.len() {
            // Awkward partial reads cross message and ring boundaries.
            let end = (offset + 8179).min(received.len());
            offset += b.receive_idle(&mut received[offset..end], deadline)?;
        }
        assert_eq!(received, expected);
        let mut offset = 0;
        while offset < received.len() {
            offset += b.queue_send(&received[offset..], deadline)?;
        }
        b.progress(deadline)?;
        if round == 2 {
            // There is no subsequent receive to progress the final reply.
            b.flush(deadline)?;
        }
    }
    sender.join().unwrap()?;
    Ok(())
}

fn progress_nonblocking(endpoint: &mut Endpoint) -> io::Result<bool> {
    match endpoint.try_progress() {
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(false),
        result => result,
    }
}

fn warm_nonblocking_pair(a: &mut Endpoint, b: &mut Endpoint, deadline: Instant) -> io::Result<()> {
    let mut sent = false;
    let mut received = false;
    while !received || a.sends_pending() {
        assert!(Instant::now() < deadline, "nonblocking warmup stalled");
        if !sent {
            match a.try_send(&[7]) {
                Ok(len) => {
                    assert_eq!(len, 1);
                    sent = true;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error),
            }
        }
        let mut byte = [0];
        match b.try_receive(&mut byte) {
            Ok(len) => {
                assert_eq!(len, 1);
                assert_eq!(byte, [7]);
                assert!(!received);
                received = true;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
        progress_nonblocking(a)?;
        progress_nonblocking(b)?;
        thread::yield_now();
    }
    Ok(())
}

#[test]
#[ignore = "requires libfabric tcp;ofi_rxm and local network permission"]
fn software_provider_nonblocking_multiple_sends_own_copies_and_wrap_rings() -> io::Result<()> {
    let (mut a, mut b) = pair()?;
    let deadline = Instant::now() + Duration::from_secs(20);
    let expected: Vec<u8> = (0..1_048_593)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect();
    let mut input = expected.clone();
    let mut sent = 0;
    let mut received = Vec::with_capacity(expected.len());
    let mut calls = 0;
    let mut chunk = [0; 8179];
    while received.len() != expected.len() || a.sends_pending() {
        assert!(Instant::now() < deadline, "nonblocking transfer stalled");
        // One owner submits several messages before servicing the peer. No
        // per-send flush or receive, thread-per-endpoint, or blocking API is used.
        for _ in 0..4 {
            if sent == input.len() {
                break;
            }
            let split = (sent + 17).min(input.len());
            match a.try_send_vectored(&[
                IoSlice::new(&input[sent..split]),
                IoSlice::new(&[]),
                IoSlice::new(&input[split..]),
            ]) {
                Ok(len) => {
                    assert!(len > 0 && len <= 65_536);
                    assert!(a.sends_pending());
                    input[sent..sent + len].fill(255);
                    sent += len;
                    calls += 1;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => return Err(error),
            }
        }
        match b.try_receive(&mut chunk) {
            Ok(len) => {
                assert!(len > 0);
                received.extend_from_slice(&chunk[..len]);
                assert!(received.len() <= expected.len());
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
        progress_nonblocking(&mut a)?;
        progress_nonblocking(&mut b)?;
        thread::yield_now();
    }
    assert_eq!(sent, expected.len());
    assert!(calls > 4);
    assert_eq!(received, expected);
    assert!(!a.sends_pending());
    assert!(!b.sends_pending());
    Ok(())
}

#[test]
#[ignore = "requires libfabric tcp;ofi_rxm and local network permission"]
fn software_provider_nonblocking_backpressure_resumes_without_replaying_unposted_bytes()
-> io::Result<()> {
    let (mut a, mut b) = pair()?;
    let deadline = Instant::now() + Duration::from_secs(20);
    warm_nonblocking_pair(&mut a, &mut b, deadline)?;
    let payload = vec![73; 65_536];
    let mut accepted = loop {
        assert!(Instant::now() < deadline, "initial large send stalled");
        match a.try_send(&payload) {
            Ok(len) => {
                assert_eq!(len, payload.len());
                break len;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                progress_nonblocking(&mut a)?;
                progress_nonblocking(&mut b)?;
                thread::yield_now();
            }
            Err(error) => return Err(error),
        }
    };
    let mut backpressured = false;
    // Deliberately stop peer progress. Application slots and provider capacity
    // must bound accepted work instead of making one send wait for the peer.
    for _ in 0..1024 {
        assert!(Instant::now() < deadline, "send did not return promptly");
        match a.try_send(&payload) {
            Ok(len) => {
                assert_eq!(len, payload.len());
                accepted += len;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                backpressured = true;
                break;
            }
            Err(error) => return Err(error),
        }
    }
    assert!(backpressured);
    assert!(accepted > 0);
    // The next accepted send uses different data. An EAGAIN copy must neither
    // retain the old caller buffer nor later post that failed copy implicitly.
    let mut marker = [29; 17];
    let mut marker_sent = false;
    let mut received = 0;
    let mut chunk = [0; 8179];
    while received < accepted + marker.len() || a.sends_pending() {
        assert!(Instant::now() < deadline, "backpressure did not clear");
        if !marker_sent {
            match a.try_send(&marker) {
                Ok(len) => {
                    assert_eq!(len, marker.len());
                    marker_sent = true;
                    marker.fill(255);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error),
            }
        }
        match b.try_receive(&mut chunk) {
            Ok(len) => {
                assert!(len > 0);
                assert!(received + len <= accepted + marker.len());
                for (offset, byte) in chunk[..len].iter().enumerate() {
                    assert_eq!(*byte, if received + offset < accepted { 73 } else { 29 });
                }
                received += len;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
        progress_nonblocking(&mut a)?;
        progress_nonblocking(&mut b)?;
        thread::yield_now();
    }
    assert!(marker_sent);
    assert_eq!(received, accepted + marker.len());
    assert_eq!(
        b.try_receive(&mut chunk).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    Ok(())
}

#[test]
#[ignore = "requires libfabric tcp;ofi_rxm and local network permission"]
fn software_provider_nonblocking_close_keeps_pending_dma_slots_owned() -> io::Result<()> {
    for _ in 0..8 {
        let (mut a, mut b) = pair()?;
        let deadline = Instant::now() + Duration::from_secs(10);
        warm_nonblocking_pair(&mut a, &mut b, deadline)?;
        let mut input = vec![19; 65_536];
        loop {
            assert!(Instant::now() < deadline, "close-test send stalled");
            match a.try_send(&input) {
                Ok(len) => {
                    assert_eq!(len, input.len());
                    break;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    progress_nonblocking(&mut a)?;
                    progress_nonblocking(&mut b)?;
                    thread::yield_now();
                }
                Err(error) => return Err(error),
            }
        }
        assert!(a.sends_pending());
        input.fill(255);
        drop(input);
        // Drop without a CQ drain, with both posted TX and RX. Successful close
        // must precede buffer release; failed close must quarantine the owner.
        drop(a);
        drop(b);
    }
    Ok(())
}

#[test]
#[ignore = "requires libfabric tcp;ofi_rxm and local network permission"]
fn software_provider_explicit_sweeps_transfer_without_repolling() -> io::Result<()> {
    let (mut a, mut b) = pair()?;
    for endpoint in [&a, &b] {
        let profile = endpoint.open_profile();
        assert_eq!(profile.requested_tx, 0);
        assert_eq!(profile.requested_rx, 0);
        assert!(profile.selected_tx >= 4 && profile.selected_rx >= 4);
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    warm_nonblocking_pair(&mut a, &mut b, deadline)?;
    let input: Vec<u8> = (0..=4 * 65_536)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect();
    let mut output = vec![0; input.len()];
    let (mut sent, mut received) = (0, 0);
    while received < input.len() || a.sends_pending() {
        assert!(Instant::now() < deadline, "explicit progress stalled");
        let before_a = a.progress_stats().cq_polls;
        let before_b = b.progress_stats().cq_polls;
        progress_nonblocking(&mut a)?;
        progress_nonblocking(&mut b)?;
        if sent < input.len() {
            match a.try_send_vectored_no_poll(&[IoSlice::new(&input[sent..])]) {
                Ok(len) => sent += len,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error),
            }
        }
        if received < input.len() {
            let end = (received + 8179).min(output.len());
            match b.try_receive_no_poll(&mut output[received..end]) {
                Ok(len) => received += len,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error),
            }
        }
        assert_eq!(a.progress_stats().cq_polls, before_a + 1);
        assert_eq!(b.progress_stats().cq_polls, before_b + 1);
    }
    assert_eq!(sent, input.len());
    assert_eq!(output, input);
    assert_eq!(a.accepted_send_fence()?, a.completed_send_fence()?);
    Ok(())
}
