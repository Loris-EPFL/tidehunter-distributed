use super::*;

fn frame(ordinal: u64, payload: &[u8]) -> Vec<u8> {
    let mut packet = vec![0; HEADER_LEN + payload.len()];
    encode_data(&mut packet, ordinal, 0, payload, 4096).expect("test packet fits");
    packet
}

#[test]
fn codec_rejects_corruption_lengths_and_unknown_types() {
    let mut packet = frame(1, b"bytes");
    assert!(matches!(
        decode(&packet, 4096),
        Ok(Packet::Data {
            ordinal: 1,
            payload: b"bytes",
            ..
        })
    ));
    packet[HEADER_LEN] ^= 0x80;
    assert_eq!(
        decode(&packet, 4096).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    let mut packet = frame(1, b"x");
    packet[6] = 9;
    assert_eq!(
        decode(&packet, 4096).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    let mut packet = frame(1, b"x");
    packet[5] = 0xff;
    assert_eq!(
        decode(&packet, 4096).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn receive_reorders_by_ordinal_and_preserves_partial_stream_reads() {
    let mut receiver = StreamReceiver::new(4096).unwrap();
    receiver.accept(&frame(2, b"def")).unwrap();
    receiver.accept(&frame(3, b"ghi")).unwrap();
    assert_eq!(
        receiver.read(&mut [0; 8]).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    receiver.accept(&frame(1, b"abc")).unwrap();
    let mut output = [0; 2];
    assert_eq!(receiver.read(&mut output).unwrap(), (2, None));
    assert_eq!(&output, b"ab");
    let mut output = [0; 4];
    assert_eq!(receiver.read(&mut output).unwrap(), (1, Some(1)));
    assert_eq!(&output[..1], b"c");
    assert_eq!(receiver.read(&mut output).unwrap(), (3, Some(2)));
    assert_eq!(&output[..3], b"def");
    assert_eq!(receiver.read(&mut output).unwrap(), (3, Some(3)));
    assert_eq!(&output[..3], b"ghi");
    assert_eq!(receiver.consumed(), 3);
}

#[test]
fn duplicates_and_packets_outside_reserved_head_window_fail_closed() {
    let mut order = ReceiveWindow::default();
    assert!(order.accept(2).is_ok());
    assert!(order.accept(2).is_err());
    assert!(order.accept(DATA_WINDOW + 1).is_err());
    assert!(order.accept(0).is_err());
}

#[test]
fn peer_credit_is_remote_consumption_based_and_cumulative() {
    let mut sender = SendWindow::default();
    for ordinal in 1..=DATA_WINDOW {
        assert_eq!(sender.next_ordinal().unwrap(), ordinal);
        sender.posted(ordinal).unwrap();
    }
    assert_eq!(
        sender.next_ordinal().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(sender.peer_credit(), 0);
    assert_eq!(
        sender.next_ordinal().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    sender.credit(1).unwrap();
    assert_eq!(sender.next_ordinal().unwrap(), DATA_WINDOW + 1);
    sender.credit(0).unwrap();
    assert!(sender.credit(DATA_WINDOW + 2).is_err());
}

#[test]
fn ordinal_exhaustion_and_malformed_credit_are_rejected() {
    let sender = SendWindow {
        next: u64::MAX,
        ..SendWindow::default()
    };
    assert!(sender.next_ordinal().is_err());
    let mut wire = [0; HEADER_LEN];
    assert!(encode_credit(&mut wire, 0, 0).is_err());
}

#[test]
fn receive_rejects_control_packets_and_malformed_peer_counts() {
    let mut receiver = StreamReceiver::new(4096).unwrap();
    let mut control = [0; HEADER_LEN];
    encode_credit(&mut control, 1, 0).unwrap();
    assert!(receiver.accept(&control).is_err());
    assert!(StreamReceiver::new(HEADER_LEN - 1).is_err());
    assert!(StreamReceiver::new(65_537).is_err());
}

#[test]
fn packet_payload_is_clamped_to_negotiated_mtu() {
    let mut output = [0; 4096];
    assert!(encode_data(&mut output, 1, 0, &[0; 4069], 4096).is_err());
    assert!(encode_data(&mut output, 1, 0, &[0; 4068], 4096).is_ok());
}

#[test]
fn reserved_control_lane_coalesces_without_data_credit_or_ack_loops() {
    let mut controls = ControlWindow::default();
    assert_eq!(controls.prepare(0).unwrap(), None);
    assert_eq!(controls.prepare(1).unwrap(), Some(1));
    assert!(controls.needs_send(1));
    // A failed provider post leaves no in-flight state, so flush/progress can
    // retry the same cumulative credit.
    assert_eq!(controls.prepare(1).unwrap(), Some(1));
    controls.posted(1, 1).unwrap();
    assert!(!controls.needs_send(1));
    assert_eq!(controls.prepare(2).unwrap(), None);
    controls.complete(1).unwrap();
    assert!(controls.needs_send(2));
    assert_eq!(controls.prepare(2).unwrap(), Some(2));
    controls.posted(2, 2).unwrap();
    assert!(controls.complete(1).is_err());
}
#[test]
fn data_credit_suppresses_standalone_credit_without_regressing_on_old_completion() {
    let mut controls = ControlWindow::default();
    assert_eq!(controls.prepare(1).unwrap(), Some(1));
    controls.posted(1, 1).unwrap();
    controls.advertised_by_data(3);
    controls.complete(1).unwrap();
    assert!(!controls.needs_send(3));
    assert_eq!(controls.prepare(3).unwrap(), None);
    assert_eq!(controls.prepare(4).unwrap(), Some(2));
    controls.posted(2, 4).unwrap();
    controls.advertised_by_data(3);
    assert!(!controls.needs_send(4));
}

#[test]
fn direct_readiness_waits_for_head_and_tracks_partial_packet_consumption() {
    let mut receiver = StreamReceiver::new(4096).unwrap();
    assert!(!receiver.receive_ready());
    receiver.accept_data(2, b"later").unwrap();
    assert!(!receiver.receive_ready());
    receiver.accept_data(1, b"first").unwrap();
    assert!(receiver.receive_ready());
    let mut first = [0; 2];
    assert_eq!(receiver.read(&mut first).unwrap(), (2, None));
    assert!(receiver.receive_ready());
    let mut rest = [0; 3];
    assert_eq!(receiver.read(&mut rest).unwrap(), (3, Some(1)));
    assert!(receiver.receive_ready());
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "Keep the deterministic two-sided packet simulation in one executable test"
)]
fn software_packet_transport_reassembles_large_bidirectional_streams() {
    fn enqueue(
        send: &mut SendWindow,
        offset: &mut usize,
        input: &[u8],
        output: &mut Vec<Vec<u8>>,
        consumed: u64,
        advertised: &mut ControlWindow,
        mtu: usize,
    ) {
        for _ in 0..DATA_WINDOW {
            if *offset == input.len() {
                break;
            }
            let ordinal = match send.next_ordinal() {
                Ok(ordinal) => ordinal,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("unexpected send-window failure: {error}"),
            };
            let len = (input.len() - *offset).min(mtu - HEADER_LEN);
            let mut packet = vec![0; HEADER_LEN + len];
            encode_data(
                &mut packet,
                ordinal,
                consumed,
                &input[*offset..*offset + len],
                mtu,
            )
            .unwrap();
            send.posted(ordinal).unwrap();
            advertised.advertised_by_data(consumed);
            *offset += len;
            output.push(packet);
        }
    }
    let mtu = 4096;
    let expected_a: Vec<u8> = (0..1_000_003)
        .map(|i| u8::try_from(i % 239).unwrap())
        .collect();
    let expected_b: Vec<u8> = (0..1_000_019)
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect();
    let mut send_a = SendWindow::default();
    let mut send_b = SendWindow::default();
    let mut recv_a = StreamReceiver::new(mtu).unwrap();
    let mut recv_b = StreamReceiver::new(mtu).unwrap();
    let mut control_a = ControlWindow::default();
    let mut control_b = ControlWindow::default();
    let mut queued_to_a = Vec::new();
    let mut queued_to_b = Vec::new();
    let mut received_a = Vec::with_capacity(expected_b.len());
    let mut received_b = Vec::with_capacity(expected_a.len());
    let mut offset_a = 0;
    let mut offset_b = 0;
    let mut chunk_a = [0; 1024];
    let mut chunk_b = [0; 1024];

    // A credit/liveness regression must fail instead of spinning without limit.
    for _ in 0..4096 {
        if received_a.len() == expected_b.len() && received_b.len() == expected_a.len() {
            break;
        }
        enqueue(
            &mut send_a,
            &mut offset_a,
            &expected_a,
            &mut queued_to_b,
            recv_a.consumed(),
            &mut control_a,
            mtu,
        );
        enqueue(
            &mut send_b,
            &mut offset_b,
            &expected_b,
            &mut queued_to_a,
            recv_b.consumed(),
            &mut control_b,
            mtu,
        );

        // A software datagram queue deliberately reverses arrival order while
        // retaining every packet; it is not an EFA hardware/performance test.
        for packet in queued_to_a.drain(..).rev() {
            let Packet::Data {
                ordinal,
                credit,
                payload,
            } = decode(&packet, mtu).unwrap()
            else {
                panic!("software data queue contains a control packet");
            };
            send_a.credit(credit).unwrap();
            recv_a.accept_data(ordinal, payload).unwrap();
        }
        for packet in queued_to_b.drain(..).rev() {
            let Packet::Data {
                ordinal,
                credit,
                payload,
            } = decode(&packet, mtu).unwrap()
            else {
                panic!("software data queue contains a control packet");
            };
            send_b.credit(credit).unwrap();
            recv_b.accept_data(ordinal, payload).unwrap();
        }
        for _ in 0..usize::try_from(DATA_WINDOW).unwrap() * (mtu.div_ceil(chunk_a.len()) + 1) {
            match recv_a.read(&mut chunk_a) {
                Ok((len, credit)) => {
                    received_a.extend_from_slice(&chunk_a[..len]);
                    if let Some(consumed) = credit {
                        let ordinal = control_a.prepare(consumed).unwrap().unwrap();
                        let mut wire = [0; HEADER_LEN];
                        encode_credit(&mut wire, ordinal, consumed).unwrap();
                        control_a.posted(ordinal, consumed).unwrap();
                        let Packet::Credit { consumed, .. } = decode(&wire, mtu).unwrap() else {
                            panic!("credit lane encoded a data packet");
                        };
                        send_b.credit(consumed).unwrap();
                        control_a.complete(ordinal).unwrap();
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("unexpected receive failure: {error}"),
            }
        }
        for _ in 0..usize::try_from(DATA_WINDOW).unwrap() * (mtu.div_ceil(chunk_b.len()) + 1) {
            match recv_b.read(&mut chunk_b) {
                Ok((len, credit)) => {
                    received_b.extend_from_slice(&chunk_b[..len]);
                    if let Some(consumed) = credit {
                        let ordinal = control_b.prepare(consumed).unwrap().unwrap();
                        let mut wire = [0; HEADER_LEN];
                        encode_credit(&mut wire, ordinal, consumed).unwrap();
                        control_b.posted(ordinal, consumed).unwrap();
                        let Packet::Credit { consumed, .. } = decode(&wire, mtu).unwrap() else {
                            panic!("credit lane encoded a data packet");
                        };
                        send_a.credit(consumed).unwrap();
                        control_b.complete(ordinal).unwrap();
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("unexpected receive failure: {error}"),
            }
        }
    }
    assert_eq!(received_a, expected_b);
    assert_eq!(received_b, expected_a);
}
