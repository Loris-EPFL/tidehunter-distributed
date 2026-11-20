use super::*;

pub(super) struct Buffer {
    pub(super) data: Vec<u8>,
    pub(super) start: usize,
    pub(super) len: usize,
}
impl Buffer {
    pub(super) fn new(len: usize, alignment: usize) -> Self {
        let data = vec![0; len + alignment - 1];
        let start = (alignment - data.as_ptr() as usize % alignment) % alignment;
        Self { data, start, len }
    }
    pub(super) fn ptr(&mut self) -> *mut u8 {
        self.data.as_mut_ptr().wrapping_add(self.start)
    }
    pub(super) fn slice(&self) -> &[u8] {
        &self.data[self.start..self.start + self.len]
    }
    pub(super) fn slice_mut(&mut self) -> &mut [u8] {
        &mut self.data[self.start..self.start + self.len]
    }
}

pub(super) enum WriteData<'a> {
    Borrowed(&'a [u8]),
    Owned(&'a mut Option<Buffer>),
}

/// Exclusive loan from a queue's aligned pool. The mutable queue borrow prevents
/// other admissions until submission or drop, so its exact allocation charge
/// cannot be spent twice. Drop returns an unsubmitted buffer directly to the pool.
pub struct LiveWriteBuffer<'a> {
    pub(super) queue: &'a mut LiveIo,
    pub(super) buffer: Option<Buffer>,
}
impl std::ops::Deref for LiveWriteBuffer<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match &self.buffer {
            Some(buffer) => buffer.slice(),
            None => &[],
        }
    }
}
impl std::ops::DerefMut for LiveWriteBuffer<'_> {
    fn deref_mut(&mut self) -> &mut [u8] {
        match &mut self.buffer {
            Some(buffer) => buffer.slice_mut(),
            None => &mut [],
        }
    }
}
impl LiveWriteBuffer<'_> {
    /// Moves the aligned allocation into the queue until its terminal CQE.
    pub fn submit_write_buffer(
        mut self,
        id: u64,
        offset: u64,
        sync: LiveWriteSync,
        deadline: Instant,
    ) -> Result<LiveSubmitOutcome, LiveIoError> {
        let (start, size, member) =
            self.queue
                .range(self.queue.namespace.bytes, offset, self.len())?;
        if member != 0 || size != self.len() {
            return Err(LiveIoError::Invalid("write must cover aligned whole frame"));
        }
        self.queue.submit(
            self.queue.namespace.file.as_raw_fd(),
            id,
            smallvec::smallvec![PlannedRead {
                offset: start,
                len: size,
                members: smallvec::smallvec![MemberSlice {
                    index: 0,
                    start: 0,
                    len: size,
                }],
            }],
            Some((WriteData::Owned(&mut self.buffer), sync)),
            deadline,
            None,
        )
    }
}
impl Drop for LiveWriteBuffer<'_> {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.queue.pool.push(buffer);
        }
    }
}

/// Where dropped member blocks go back to their queue. Owner of the receiving
/// end: the `LiveIo`. Capacity: one block per slot, and at most `max_bytes` of
/// block memory queued at once. Full policy: a block that exceeds either bound,
/// or arrives after the queue dropped, is freed by its dropper, so a drop never
/// blocks and the queue never holds more than its byte budget in returns.
pub(super) struct ReturnLane {
    pub(super) sender: mpsc::SyncSender<Buffer>,
    /// Bytes of blocks sent and not yet received. Relaxed: an independent
    /// counter; the blocks themselves are handed over by the channel, which
    /// orders a block's `fetch_add` before the receiver's `fetch_sub`.
    pub(super) queued_bytes: AtomicUsize,
    pub(super) max_bytes: usize,
}
impl ReturnLane {
    pub(super) fn give_back(&self, buffer: Buffer) {
        let len = buffer.data.len();
        let mut queued = self.queued_bytes.load(Ordering::Relaxed);
        for _ in 0..16 {
            let Some(next) = queued.checked_add(len).filter(|&n| n <= self.max_bytes) else {
                return;
            };
            match self.queued_bytes.compare_exchange_weak(
                queued,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    if self.sender.try_send(buffer).is_err() {
                        self.queued_bytes.fetch_sub(len, Ordering::Relaxed);
                    }
                    return;
                }
                Err(current) => queued = current,
            }
        }
        // Contended past the retry bound: freeing the block is always safe.
    }
}

/// Shared owner of one aligned read block. Every member is a slice of this
/// owner. The last slice returns the block, only after its terminal CQE.
pub(super) struct MemberBuffer {
    pub(super) buffer: Option<Buffer>,
    pub(super) lane: Arc<ReturnLane>,
}
impl AsRef<[u8]> for MemberBuffer {
    fn as_ref(&self) -> &[u8] {
        match &self.buffer {
            Some(buffer) => buffer.slice(),
            None => &[],
        }
    }
}
impl minibytes::BytesOwner for MemberBuffer {}
impl Drop for MemberBuffer {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.lane.give_back(buffer);
        }
    }
}
#[derive(Clone, Copy)]
pub(super) struct MemberSlice {
    pub(super) index: usize,
    pub(super) start: usize,
    pub(super) len: usize,
}
