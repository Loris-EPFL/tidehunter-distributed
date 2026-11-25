use super::*;

#[test]
fn message_hints_request_sas_ordering_without_application_queue_sizes() {
    for provider in [c"verbs;ofi_rxm", c"tcp;ofi_rxm"] {
        let hints = Info::hints(provider, c"").unwrap();
        // SAFETY: Info owns the live, fully initialized hint allocation.
        let info = unsafe { &*hints.0 };
        // SAFETY: fi_allocinfo initialized tx_attr; hints keeps it live here.
        let tx = unsafe { &*info.tx_attr };
        // SAFETY: fi_allocinfo initialized rx_attr; hints keeps it live here.
        let rx = unsafe { &*info.rx_attr };
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
