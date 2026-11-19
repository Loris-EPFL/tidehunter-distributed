//! These explicit tests exercise libfabric's software provider, not RDMA speed.
#![cfg(feature = "native")]

use std::{
    io::{self, IoSlice},
    thread,
    time::{Duration, Instant},
};
use tidehunter_fabric::{Endpoint, FabricBackend, FabricConfig, MAX_SLOTS};

fn software_config(domain: Option<&str>) -> FabricConfig {
    FabricConfig {
        backend: FabricBackend::SoftwareTcp,
        domain: domain.map(str::to_owned),
        source_address: Some(std::net::Ipv4Addr::LOCALHOST.into()),
        slots: None,
    }
}

fn pair() -> io::Result<(Endpoint, Endpoint)> {
    let mut a = Endpoint::open(&software_config(Some("lo")))?;
    let mut b = Endpoint::open(&software_config(Some("lo")))?;
    assert_eq!(a.provider_info().provider, "tcp;ofi_rxm");
    assert_eq!(
        a.provider_info().source_address,
        Some(std::net::Ipv4Addr::LOCALHOST.into())
    );
    let a_addr = a.address()?;
    let b_addr = b.address()?;
    a.set_peer(&b_addr)?;
    b.set_peer(&a_addr)?;
    Ok((a, b))
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
fn invalid_provider_selectors_are_rejected_before_native_discovery() {
    let mut config = software_config(None);
    config.domain = Some(String::new());
    assert_eq!(
        Endpoint::open(&config)
            .err()
            .expect("empty selector must fail validation")
            .kind(),
        io::ErrorKind::InvalidInput
    );
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

#[test]
#[ignore = "requires libfabric tcp;ofi_rxm and local network permission"]
fn software_provider_idle_wait_wakes_once_and_transfers_both_ways() -> io::Result<()> {
    let (mut a, mut b) = pair()?;
    let deadline = Instant::now() + Duration::from_secs(10);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let sender_barrier = std::sync::Arc::clone(&barrier);
    let sender = thread::spawn(move || -> io::Result<Endpoint> {
        sender_barrier.wait();
        a.send(b"idle-wake", deadline)?;
        Ok(a)
    });

    let wait_supported = b.supports_idle_wait();
    barrier.wait();
    let mut waited = false;
    while !b.receive_ready()? {
        assert!(
            Instant::now() < deadline,
            "idle CQ progress deadline expired"
        );
        let (_, used_wait) = b.try_progress_for_sweep(Some(deadline))?;
        waited |= used_wait;
    }
    if wait_supported {
        assert!(waited);
    }
    let mut incoming = [0; 32];
    assert_eq!(b.try_receive(&mut incoming)?, b"idle-wake".len());
    assert_eq!(&incoming[..b"idle-wake".len()], b"idle-wake");
    assert_eq!(
        b.try_receive(&mut incoming).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );

    let mut a = sender.join().unwrap()?;
    b.send(b"reply", deadline)?;
    let mut reply_waited = false;
    while !a.receive_ready()? {
        assert!(
            Instant::now() < deadline,
            "reply CQ progress deadline expired"
        );
        let (_, used_wait) = a.try_progress_for_sweep(Some(deadline))?;
        reply_waited |= used_wait;
    }
    if a.supports_idle_wait() {
        assert!(reply_waited);
    }
    assert_eq!(a.try_receive(&mut incoming)?, b"reply".len());
    assert_eq!(&incoming[..b"reply".len()], b"reply");
    a.flush(deadline)?;
    b.flush(deadline)?;
    drop(a);
    drop(b);
    Ok(())
}

#[test]
#[ignore = "requires libfabric tcp;ofi_rxm and local network permission"]
fn software_provider_pending_send_disables_idle_cq_wait() -> io::Result<()> {
    let (mut a, mut b) = pair()?;
    let deadline = Instant::now() + Duration::from_secs(5);
    thread::scope(|scope| -> io::Result<()> {
        let sender = scope.spawn(|| -> io::Result<()> {
            assert_eq!(a.queue_send(b"pending", deadline)?, b"pending".len());
            assert!(a.sends_pending());
            let (_, waited) = a.try_progress_for_sweep(Some(deadline))?;
            assert!(!waited);
            a.flush(deadline)?;
            // Local CQ retirement is not peer delivery. Keep manual progress
            // running until the peer confirms it consumed the actual payload.
            let mut acknowledged = [0];
            assert_eq!(a.receive(&mut acknowledged, deadline)?, 1);
            assert_eq!(acknowledged, [1]);
            Ok(())
        });
        let mut received = [0; 16];
        assert_eq!(b.receive(&mut received, deadline)?, b"pending".len());
        assert_eq!(&received[..b"pending".len()], b"pending");
        b.send(&[1], deadline)?;
        sender.join().unwrap()?;
        Ok(())
    })?;
    Ok(())
}

#[test]
#[ignore = "requires libfabric tcp;ofi_rxm and local network permission"]
fn software_provider_buffered_receive_disables_idle_cq_wait() -> io::Result<()> {
    let (mut a, mut b) = pair()?;
    let deadline = Instant::now() + Duration::from_secs(5);
    thread::scope(|scope| -> io::Result<()> {
        let sender = scope.spawn(|| -> io::Result<()> {
            assert_eq!(a.send(b"buffered", deadline)?, b"buffered".len());
            let mut acknowledged = [0];
            assert_eq!(a.receive(&mut acknowledged, deadline)?, 1);
            assert_eq!(acknowledged, [1]);
            Ok(())
        });
        let mut first = [0];
        assert_eq!(b.receive(&mut first, deadline)?, 1);
        assert_eq!(first, [b'b']);
        let (_, waited) = b.try_progress_for_sweep(Some(deadline))?;
        assert!(!waited);
        let mut rest = [0; 7];
        assert_eq!(b.receive(&mut rest, deadline)?, rest.len());
        assert_eq!(&rest, b"uffered");
        b.send(&[1], deadline)?;
        sender.join().unwrap()?;
        Ok(())
    })?;
    a.flush(deadline)?;
    b.flush(deadline)?;
    Ok(())
}

#[test]
#[ignore = "requires libfabric tcp;ofi_rxm and local network permission"]
fn software_provider_records_effective_geometry_and_rxm_parameters() -> io::Result<()> {
    let endpoint = Endpoint::open(&software_config(Some("lo")))?;
    let geometry = &endpoint.provider_info().geometry;
    assert!(geometry.libfabric_api_version.contains('.'));
    assert!(geometry.provider_version.contains('.'));
    assert_eq!(geometry.slots, 4);
    assert_eq!(geometry.data_progress, "manual");
    assert!(geometry.tx_size >= 4 && geometry.rx_size >= 4);
    assert!(geometry.max_msg_size >= 65_536);
    assert_eq!(geometry.mr_mode == 0, geometry.mr_mode_flags.is_empty());
    assert!(
        geometry.cq_wait.starts_with("unspec"),
        "software provider reports its bounded CQ wait: {}",
        geometry.cq_wait
    );
    // The RxM parameter list is what explains eager/SAR/rendezvous thresholds.
    let configured = |name: &str| {
        geometry
            .rxm_parameters
            .iter()
            .find(|param| param.name == name)
            .unwrap_or_else(|| panic!("missing {name}: {:?}", geometry.rxm_parameters))
            .configured_or_default
            .as_deref()
    };
    if std::env::var_os("FI_OFI_RXM_BUFFER_SIZE").is_none() {
        assert_eq!(configured("FI_OFI_RXM_BUFFER_SIZE"), Some("16384"));
    }
    if std::env::var_os("FI_OFI_RXM_SAR_LIMIT").is_none() {
        assert_eq!(configured("FI_OFI_RXM_SAR_LIMIT"), Some("131072"));
    }
    Ok(())
}

#[test]
#[ignore = "requires libfabric tcp;ofi_rxm and local network permission"]
fn software_provider_transfers_with_a_larger_slot_count() -> io::Result<()> {
    let mut config = software_config(Some("lo"));
    config.slots = Some(MAX_SLOTS);
    let mut a = Endpoint::open(&config)?;
    let mut b = Endpoint::open(&config)?;
    assert_eq!(a.provider_info().geometry.slots, MAX_SLOTS);
    let (a_addr, b_addr) = (a.address()?, b.address()?);
    a.set_peer(&b_addr)?;
    b.set_peer(&a_addr)?;
    let deadline = Instant::now() + Duration::from_secs(20);
    // More chunks than the default four-slot ring so the larger ring wraps.
    let payload: Vec<u8> = (0..70 * 65_536 + 13)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect();
    let expected = payload.clone();
    let sender = thread::spawn(move || -> io::Result<Endpoint> {
        let mut offset = 0;
        while offset < payload.len() {
            offset += a.queue_send(&payload[offset..], deadline)?;
        }
        a.flush(deadline)?;
        Ok(a)
    });
    let mut received = vec![0; expected.len()];
    let mut offset = 0;
    while offset < received.len() {
        offset += b.receive(&mut received[offset..], deadline)?;
    }
    assert_eq!(received, expected);
    sender.join().unwrap()?;
    Ok(())
}
