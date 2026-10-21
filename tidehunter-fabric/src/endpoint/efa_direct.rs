//! Bounded, unordered-packet framing for the EFA-direct stream transport.
//!
//! This module is protocol machinery only. It never selects a provider or
//! pretends that a software transport is EFA-direct hardware.
use std::io::{self, IoSlice};

pub(super) const HEADER_LEN: usize = 28;
const MAGIC: [u8; 4] = *b"THED";
const VERSION: u8 = 1;
const DATA: u8 = 1;
const CREDIT: u8 = 2;
/// One reserved sequence-head slot beyond the bounded 32-packet data window.
pub(super) const WINDOW: usize = 33;
pub(super) const DATA_WINDOW: u64 = (WINDOW - 1) as u64;
const MAX_PACKET: usize = 65_536;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Packet<'a> {
    Data {
        ordinal: u64,
        credit: u64,
        payload: &'a [u8],
    },
    Credit {
        ordinal: u64,
        consumed: u64,
    },
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn finish_header(output: &mut [u8], packet_len: usize, kind: u8) {
    output[..4].copy_from_slice(&MAGIC);
    output[4] = VERSION;
    output[5] = kind;
    output[6..8].copy_from_slice(
        &u16::try_from(packet_len - HEADER_LEN)
            .expect("validated MTU bounds the payload length to u16")
            .to_be_bytes(),
    );
    output[24..28].fill(0);
    let sum = crc32fast::hash(&output[..packet_len]);
    output[24..28].copy_from_slice(&sum.to_be_bytes());
}

#[cfg(test)]
pub(super) fn encode_data<'a>(
    output: &'a mut [u8],
    ordinal: u64,
    peer_consumed: u64,
    payload: &[u8],
    mtu: usize,
) -> io::Result<&'a [u8]> {
    if payload.is_empty()
        || !(HEADER_LEN..=MAX_PACKET).contains(&mtu)
        || payload.len() > mtu - HEADER_LEN
        || payload.len() > output.len().saturating_sub(HEADER_LEN)
    {
        return Err(invalid("invalid EFA-direct data packet bounds"));
    }
    let packet_len = encode_data_vectored(
        output,
        ordinal,
        peer_consumed,
        &[IoSlice::new(payload)],
        mtu,
    )?;
    Ok(&output[..packet_len])
}

pub(super) fn encode_data_vectored(
    output: &mut [u8],
    ordinal: u64,
    peer_consumed: u64,
    slices: &[IoSlice<'_>],
    mtu: usize,
) -> io::Result<usize> {
    if ordinal == 0
        || ordinal == u64::MAX
        || !(HEADER_LEN..=MAX_PACKET).contains(&mtu)
        || output.len() <= HEADER_LEN
    {
        return Err(invalid("invalid EFA-direct data packet bounds"));
    }
    let maximum_payload = (mtu - HEADER_LEN).min(output.len() - HEADER_LEN);
    output[..HEADER_LEN].fill(0);
    output[8..16].copy_from_slice(&ordinal.to_be_bytes());
    output[16..24].copy_from_slice(&peer_consumed.to_be_bytes());
    let mut payload_len = 0;
    for slice in slices {
        let count = slice.len().min(maximum_payload - payload_len);
        output[HEADER_LEN + payload_len..HEADER_LEN + payload_len + count]
            .copy_from_slice(&slice[..count]);
        payload_len += count;
        if payload_len == maximum_payload {
            break;
        }
    }
    if payload_len == 0 {
        return Err(invalid("empty EFA-direct data packet"));
    }
    let packet_len = HEADER_LEN + payload_len;
    finish_header(output, packet_len, DATA);
    Ok(packet_len)
}

pub(super) fn encode_credit(output: &mut [u8], ordinal: u64, consumed: u64) -> io::Result<usize> {
    if ordinal == 0 || ordinal == u64::MAX || output.len() < HEADER_LEN {
        return Err(invalid("invalid EFA-direct credit packet bounds"));
    }
    output[..HEADER_LEN].fill(0);
    output[8..16].copy_from_slice(&ordinal.to_be_bytes());
    output[16..24].copy_from_slice(&consumed.to_be_bytes());
    finish_header(output, HEADER_LEN, CREDIT);
    Ok(HEADER_LEN)
}

pub(super) fn decode(bytes: &[u8], mtu: usize) -> io::Result<Packet<'_>> {
    if bytes.len() < HEADER_LEN
        || bytes.len() > mtu
        || mtu > MAX_PACKET
        || bytes[..4] != MAGIC
        || bytes[4] != VERSION
    {
        return Err(invalid("invalid EFA-direct packet header"));
    }
    let payload_len = usize::from(u16::from_be_bytes([bytes[6], bytes[7]]));
    if HEADER_LEN.checked_add(payload_len) != Some(bytes.len()) {
        return Err(invalid("invalid EFA-direct packet length"));
    }
    let expected = u32::from_be_bytes(
        bytes[24..28]
            .try_into()
            .map_err(|_| invalid("invalid EFA-direct checksum"))?,
    );
    let mut checksum = crc32fast::Hasher::new();
    checksum.update(&bytes[..24]);
    checksum.update(&[0; 4]);
    checksum.update(&bytes[28..]);
    if checksum.finalize() != expected {
        return Err(invalid("corrupt EFA-direct packet"));
    }
    let ordinal = u64::from_be_bytes(
        bytes[8..16]
            .try_into()
            .map_err(|_| invalid("invalid EFA-direct ordinal"))?,
    );
    let credit = u64::from_be_bytes(
        bytes[16..24]
            .try_into()
            .map_err(|_| invalid("invalid EFA-direct credit"))?,
    );
    if ordinal == 0 || ordinal == u64::MAX {
        return Err(invalid("invalid or exhausted EFA-direct ordinal"));
    }
    match bytes[5] {
        DATA if payload_len != 0 => Ok(Packet::Data {
            ordinal,
            credit,
            payload: &bytes[HEADER_LEN..],
        }),
        CREDIT if payload_len == 0 => Ok(Packet::Credit {
            ordinal,
            consumed: credit,
        }),
        _ => Err(invalid("invalid EFA-direct packet type")),
    }
}

/// Cumulative credit and bounded ordinal admission. `post_data` is deliberately
/// a two-phase operation: call it only after a successful provider post, so an
/// EAGAIN retry neither consumes stream bytes nor advances the ordinal.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct SendWindow {
    next: u64,
    peer_consumed: u64,
}

impl SendWindow {
    pub(super) fn next_ordinal(&self) -> io::Result<u64> {
        let next = self
            .next
            .checked_add(1)
            .ok_or_else(|| invalid("EFA-direct ordinal exhausted"))?;
        if next == u64::MAX {
            return Err(invalid("EFA-direct ordinal exhausted"));
        }
        if next - self.peer_consumed > DATA_WINDOW {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        Ok(next)
    }

    pub(super) fn posted(&mut self, ordinal: u64) -> io::Result<()> {
        if ordinal != self.next_ordinal()? {
            return Err(invalid("invalid EFA-direct send ordinal"));
        }
        self.next = ordinal;
        Ok(())
    }

    pub(super) fn credit(&mut self, consumed: u64) -> io::Result<()> {
        if consumed <= self.peer_consumed {
            return Ok(());
        }
        if consumed > self.next {
            return Err(invalid("invalid EFA-direct cumulative credit"));
        }
        self.peer_consumed = consumed;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn peer_credit(&self) -> u64 {
        self.peer_consumed
    }
}

/// Bounded receive reorder bookkeeping. The caller owns the fixed packet slots;
/// `accept` reports the target slot and never treats arrival order as stream order.
#[derive(Clone, Copy, Debug)]
pub(super) struct ReceiveWindow {
    next: u64,
    slots: [Option<u64>; WINDOW],
    consumed: u64,
}

impl Default for ReceiveWindow {
    fn default() -> Self {
        Self {
            next: 1,
            slots: [None; WINDOW],
            consumed: 0,
        }
    }
}

impl ReceiveWindow {
    pub(super) fn accept(&mut self, ordinal: u64) -> io::Result<usize> {
        if ordinal < self.next || ordinal - self.next >= DATA_WINDOW {
            return Err(invalid("duplicate or out-of-window EFA-direct packet"));
        }
        let slot = usize::try_from(ordinal % WINDOW as u64)
            .map_err(|_| invalid("invalid EFA-direct slot"))?;
        if self.slots[slot].is_some() {
            return Err(invalid("duplicate EFA-direct packet"));
        }
        self.slots[slot] = Some(ordinal);
        Ok(slot)
    }

    pub(super) fn head(&self) -> Option<usize> {
        let slot = usize::try_from(self.next % WINDOW as u64).ok()?;
        (self.slots[slot] == Some(self.next)).then_some(slot)
    }

    /// Marks the exact in-order packet consumed and returns cumulative credit.
    pub(super) fn consume_head(&mut self) -> io::Result<u64> {
        let slot = self.head().ok_or(io::ErrorKind::WouldBlock)?;
        self.slots[slot] = None;
        self.consumed = self.next;
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| invalid("EFA-direct ordinal exhausted"))?;
        Ok(self.consumed)
    }

    pub(super) fn consumed(&self) -> u64 {
        self.consumed
    }
}
/// Fixed-capacity stream reassembly. Packet bytes use one preallocated backing
/// allocation, not one allocation per fragment or a multi-megabyte stack array.
pub(super) struct StreamReceiver {
    order: ReceiveWindow,
    payload_limit: usize,
    slots: Vec<ReceiveSlot>,
    storage: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Default)]
struct ReceiveSlot {
    ordinal: Option<u64>,
    len: usize,
    offset: usize,
}

impl StreamReceiver {
    pub(super) fn new(mtu: usize) -> io::Result<Self> {
        if !(HEADER_LEN + 1..=MAX_PACKET).contains(&mtu) {
            return Err(invalid("unsupported EFA-direct MTU"));
        }
        let payload_limit = mtu - HEADER_LEN;
        let storage_len = payload_limit
            .checked_mul(WINDOW)
            .ok_or_else(|| invalid("EFA-direct receive capacity overflow"))?;
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(WINDOW)
            .map_err(|_| io::ErrorKind::OutOfMemory)?;
        slots.resize(WINDOW, ReceiveSlot::default());
        let mut storage = Vec::new();
        storage
            .try_reserve_exact(storage_len)
            .map_err(|_| io::ErrorKind::OutOfMemory)?;
        storage.resize(storage_len, 0);
        Ok(Self {
            order: ReceiveWindow::default(),
            payload_limit,
            slots,
            storage,
        })
    }

    #[cfg(test)]
    pub(super) fn accept(&mut self, packet: &[u8]) -> io::Result<()> {
        let Packet::Data {
            ordinal, payload, ..
        } = decode(packet, self.payload_limit + HEADER_LEN)?
        else {
            return Err(invalid("credit packet cannot enter stream reassembly"));
        };
        self.accept_data(ordinal, payload)
    }

    pub(super) fn accept_data(&mut self, ordinal: u64, payload: &[u8]) -> io::Result<()> {
        if payload.is_empty() || payload.len() > self.payload_limit {
            return Err(invalid("EFA-direct payload exceeds negotiated MTU"));
        }
        let index = self.order.accept(ordinal)?;
        let slot = &mut self.slots[index];
        if slot.ordinal.is_some() {
            return Err(invalid("occupied EFA-direct reassembly slot"));
        }
        let start = index * self.payload_limit;
        self.storage[start..start + payload.len()].copy_from_slice(payload);
        slot.len = payload.len();
        slot.offset = 0;
        slot.ordinal = Some(ordinal);
        Ok(())
    }

    /// Reads bytes in ordinal order. Peer credit advances only when a complete
    /// packet has been consumed, never on arrival or a partial read.
    pub(super) fn read(&mut self, output: &mut [u8]) -> io::Result<(usize, Option<u64>)> {
        if output.is_empty() {
            return Ok((0, None));
        }
        let Some(index) = self.order.head() else {
            return Err(io::ErrorKind::WouldBlock.into());
        };
        let slot = &mut self.slots[index];
        let len = output.len().min(slot.len - slot.offset);
        let start = index * self.payload_limit + slot.offset;
        output[..len].copy_from_slice(&self.storage[start..start + len]);
        slot.offset += len;
        if slot.offset != slot.len {
            return Ok((len, None));
        }
        slot.ordinal = None;
        slot.len = 0;
        slot.offset = 0;
        let credit = self.order.consume_head()?;
        Ok((len, Some(credit)))
    }

    pub(super) fn receive_ready(&self) -> bool {
        self.order.head().is_some_and(|index| {
            let slot = &self.slots[index];
            slot.ordinal.is_some() && slot.offset < slot.len
        })
    }
    pub(super) fn consumed(&self) -> u64 {
        self.order.consumed()
    }
}
/// One independent control lane. Updates coalesce while a control send owns
/// the reserved slot; controls never spend data credit and never create ACKs.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct ControlWindow {
    next_ordinal: u64,
    advertised_credit: u64,
    in_flight: bool,
}

impl ControlWindow {
    pub(super) fn prepare(&self, consumed: u64) -> io::Result<Option<u64>> {
        if self.in_flight || consumed <= self.advertised_credit {
            return Ok(None);
        }
        let next = self
            .next_ordinal
            .checked_add(1)
            .filter(|&ordinal| ordinal != u64::MAX)
            .ok_or_else(|| invalid("EFA-direct control ordinal exhausted"))?;
        Ok(Some(next))
    }

    /// Invoke only after the exact control packet is accepted by the provider.
    pub(super) fn posted(&mut self, ordinal: u64, consumed: u64) -> io::Result<()> {
        if ordinal == u64::MAX
            || self.in_flight
            || ordinal
                != self
                    .next_ordinal
                    .checked_add(1)
                    .ok_or_else(|| invalid("EFA-direct control ordinal exhausted"))?
            || consumed < self.advertised_credit
        {
            return Err(invalid("invalid EFA-direct control post"));
        }
        self.next_ordinal = ordinal;
        self.advertised_credit = consumed;
        self.in_flight = true;
        Ok(())
    }

    /// A successfully posted DATA packet advertises cumulative receive credit.
    pub(super) fn advertised_by_data(&mut self, consumed: u64) -> bool {
        if consumed <= self.advertised_credit {
            return false;
        }
        self.advertised_credit = consumed;
        true
    }

    pub(super) fn complete(&mut self, ordinal: u64) -> io::Result<()> {
        if !self.in_flight || ordinal != self.next_ordinal {
            return Err(invalid("unexpected EFA-direct control completion"));
        }
        self.in_flight = false;
        Ok(())
    }
    pub(super) fn needs_send(&self, consumed: u64) -> bool {
        !self.in_flight && consumed > self.advertised_credit
    }
}
#[cfg(test)]
#[path = "efa_direct_tests.rs"]
mod tests;
