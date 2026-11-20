// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Opt-in Sui API diagnostics. `SUI_TH_KV_CAPTURE_DIR` names a fresh output
//! directory. It is read once when the first adapter opens. No public storage
//! configuration, persisted data, or durability rule changes.
//!
//! Disabled boundaries do one relaxed flag load and a predictable branch. They
//! do not read clocks, hash bytes, allocate, access thread-local state, or send.
//! Enabled producers use `try_send` on one bounded channel. The writer owns all
//! file I/O. Full/disconnected messages are counted; any loss invalidates a run.
//! Capacity is 65,536 fixed-size records (see `size_of::<Record>()` in status),
//! independent of thread count, key size, value size, and replay duration.
//! The writer polls `stop` every 20 ms, closes API admission, waits for active
//! calls, drains, flushes, and writes `final.status`. The controller must wait
//! for that receipt before terminating the node. Background events after the
//! explicit cutoff are outside the capture window. No signal handler is added.
//! Native content fingerprints are not physical WAL versions; the distributed
//! index records no reference identity. Never infer causal checkpoint
//! ownership from timestamps: checkpoint execution overlaps.

use super::{DbIterator, KeyShape, KeySpace, Result};
use crate::minibytes::Bytes;
use std::cell::Cell;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(test)]
mod tests;

const CAPACITY: usize = 65_536;
static ENABLED: AtomicBool = AtomicBool::new(false);
static RECORDER: LazyLock<io::Result<Option<Arc<Recorder>>>> = LazyLock::new(initialize);
thread_local! {
    static CURRENT: Cell<(u64, u64)> = const { Cell::new((0, 0)) };
}

pub(crate) const GET: u64 = 1;
pub(crate) const EXISTS: u64 = 2;
pub(crate) const MULTI_GET: u64 = 3;
pub(crate) const MULTI_EXISTS: u64 = 4;
pub(crate) const PUT: u64 = 5;
pub(crate) const DELETE: u64 = 6;
pub(crate) const BATCH: u64 = 7;
pub(crate) const SCAN: u64 = 8;
pub(crate) const RANGE_DELETE: u64 = 9;
// Event kinds: call begin/end, API member, batch member/end, scan begin/bound/
// row/end/reverse, reference association, cache lookup/fill, route dispatch.
pub(crate) const HEADER: &str = "event\tseq\tns\tthread\tdb\tcall\top\tks\tkey\tkey_len\tvalue_len\tcontent\tgroup\tposition\tcount\toutcome\treference\tepoch\tversion\tmember\taux\tdata";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Record {
    pub event: u64,
    pub seq: u64,
    pub ns: u64,
    pub thread: u64,
    pub db: u64,
    pub call: u64,
    pub op: u64,
    pub ks: u64,
    pub key: u64,
    pub key_len: u64,
    pub value_len: u64,
    pub content: u64,
    pub group: u64,
    pub position: u64,
    pub count: u64,
    pub outcome: u64,
    pub reference: u64,
    pub epoch: u64,
    pub version: u64,
    pub member: u64,
    pub aux: u64,
    pub data: [u8; 32],
}

impl Record {
    fn write(self, writer: &mut impl Write) -> io::Result<()> {
        for number in [
            self.event,
            self.seq,
            self.ns,
            self.thread,
            self.db,
            self.call,
            self.op,
            self.ks,
            self.key,
            self.key_len,
            self.value_len,
            self.content,
            self.group,
            self.position,
            self.count,
            self.outcome,
            self.reference,
            self.epoch,
            self.version,
            self.member,
            self.aux,
        ] {
            write!(writer, "{number}\t")?;
        }
        hex(writer, &self.data)?;
        writeln!(writer)
    }
}

fn hex(writer: &mut impl Write, data: &[u8]) -> io::Result<()> {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    for chunk in data.chunks(32) {
        let mut encoded = [0; 64];
        for (byte, pair) in chunk.iter().zip(encoded.chunks_exact_mut(2)) {
            pair[0] = DIGITS[usize::from(byte >> 4)];
            pair[1] = DIGITS[usize::from(byte & 15)];
        }
        writer.write_all(&encoded[..chunk.len() * 2])?;
    }
    Ok(())
}

/// FNV-1a 64, fixed seed, byte-order independent. Identity also includes DB,
/// keyspace and key length. Hash collisions remain a stated diagnostic limit.
pub(crate) fn hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

struct Recorder {
    sender: mpsc::SyncSender<Record>,
    start: Instant,
    next: AtomicU64,
    accepted: AtomicU64,
    dropped_full: AtomicU64,
    dropped_closed: AtomicU64,
    active: AtomicU64,
    // High bit closes admission; low bits count active record producers. The
    // writer closes this RMW gate before reading final loss counters.
    send_gate: AtomicU64,
    admitting: AtomicBool,
    // Retain the handle until adapter teardown. The explicit stop receipt also
    // proves that the writer has finished before the controller sends SIGTERM.
    writer: std::sync::Mutex<Option<JoinHandle<()>>>,
    directory: PathBuf,
}

impl Recorder {
    fn id(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed) + 1
    }
    fn send(&self, mut event: Record) {
        const CLOSED: u64 = 1 << 63;
        if self.send_gate.fetch_add(1, Ordering::AcqRel) & CLOSED != 0 {
            self.send_gate.fetch_sub(1, Ordering::Release);
            return;
        }
        event.seq = self.id();
        event.ns = self.start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        event.thread = thread_id();
        match self.sender.try_send(event) {
            Ok(()) => {
                self.accepted.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::TrySendError::Full(_)) => {
                self.dropped_full.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                self.dropped_closed.fetch_add(1, Ordering::Relaxed);
            }
        }
        // Publish this producer's queue result and counters to the drain owner.
        self.send_gate.fetch_sub(1, Ordering::Release);
    }
}

fn thread_id() -> u64 {
    // ThreadId's Hash implementation supplies its process-local identity.
    use std::hash::{Hash, Hasher};
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    thread::current().id().hash(&mut hash);
    hash.finish()
}

fn recorder() -> Option<&'static Arc<Recorder>> {
    // This flag gates work, not publication. LazyLock publishes recorder state.
    if !ENABLED.load(Ordering::Relaxed) {
        return None;
    }
    RECORDER.as_ref().ok().and_then(Option::as_ref)
}

pub(crate) fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

fn initialize() -> io::Result<Option<Arc<Recorder>>> {
    let Some(directory) = std::env::var_os("SUI_TH_KV_CAPTURE_DIR") else {
        return Ok(None);
    };
    let directory = PathBuf::from(directory);
    fs::create_dir(&directory)?;
    let mut metadata = File::create(directory.join("metadata.tsv"))?;
    let start = Instant::now();
    let anchor = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_nanos();
    let anchor_uncertainty_ns = start.elapsed().as_nanos();
    writeln!(
        metadata,
        "schema\t1\nanchor_unix_ns\t{anchor}\nanchor_uncertainty_ns\t{anchor_uncertainty_ns}\npid\t{}",
        std::process::id()
    )?;
    let mut output =
        BufWriter::with_capacity(1024 * 1024, File::create(directory.join("events.tsv"))?);
    writeln!(output, "{HEADER}")?;
    output.flush()?;
    // Single writer, fixed count, nonblocking loss policy. Stop closes API
    // admission, waits for active calls, drains this channel, then flushes.
    let (sender, receiver) = mpsc::sync_channel(CAPACITY);
    let state = Arc::new(Recorder {
        sender,
        start,
        next: AtomicU64::new(0),
        accepted: AtomicU64::new(0),
        dropped_full: AtomicU64::new(0),
        dropped_closed: AtomicU64::new(0),
        active: AtomicU64::new(0),
        send_gate: AtomicU64::new(0),
        admitting: AtomicBool::new(true),
        writer: std::sync::Mutex::new(None),
        directory,
    });
    let owner = Arc::clone(&state);
    let handle = thread::Builder::new()
        .name("sui-kv-capture".into())
        .spawn(move || {
            let result = writer(&owner, receiver, &mut output);
            if let Err(error) = result {
                ENABLED.store(false, Ordering::Relaxed);
                let _ = fs::write(owner.directory.join("writer.error"), error.to_string());
            }
        })?;
    *state
        .writer
        .lock()
        .map_err(|_| io::Error::other("capture writer handle poisoned"))? = Some(handle);
    ENABLED.store(true, Ordering::Relaxed);
    Ok(Some(state))
}

fn writer(
    owner: &Recorder,
    receiver: mpsc::Receiver<Record>,
    output: &mut impl Write,
) -> io::Result<()> {
    let mut written = 0u64;
    let mut stopping = false;
    let stop = owner.directory.join("stop");
    loop {
        let idle = match receiver.recv_timeout(Duration::from_millis(20)) {
            Ok(event) => {
                event.write(output)?;
                written += 1;
                false
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                output.flush()?;
                true
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        if !stopping && (idle || written.is_multiple_of(1024)) && stop.exists() {
            stopping = true;
            owner.admitting.store(false, Ordering::Release);
        }
        if stopping && owner.active.load(Ordering::Acquire) == 0 {
            const CLOSED: u64 = 1 << 63;
            owner.send_gate.fetch_or(CLOSED, Ordering::AcqRel);
            ENABLED.store(false, Ordering::Relaxed);
            if owner.send_gate.load(Ordering::Acquire) != CLOSED {
                continue;
            }
            while let Ok(event) = receiver.try_recv() {
                event.write(output)?;
                written += 1;
            }
            break;
        }
    }
    output.flush()?;
    let accepted = owner.accepted.load(Ordering::Relaxed);
    let full = owner.dropped_full.load(Ordering::Relaxed);
    let closed = owner.dropped_closed.load(Ordering::Relaxed);
    fs::write(
        owner.directory.join("final.status.pending"),
        format!(
            "schema\t1\naccepted\t{accepted}\nwritten\t{written}\ndropped_full\t{full}\ndropped_closed\t{closed}\nactive\t{}\ncapacity\t{CAPACITY}\nrecord_bytes\t{}\ncutoff_ns\t{}\n",
            owner.active.load(Ordering::Acquire),
            std::mem::size_of::<Record>(),
            owner.start.elapsed().as_nanos()
        ),
    )?;
    fs::rename(
        owner.directory.join("final.status.pending"),
        owner.directory.join("final.status"),
    )
}

/// Register metadata on the cold open path. A supplied invalid directory is an
/// open error; diagnostics must not silently turn off after configuration.
pub(crate) fn open(path: &Path, shape: &KeyShape, remote: Option<&[u8; 16]>) -> io::Result<u64> {
    let owner = match &*RECORDER {
        Ok(Some(owner)) => owner,
        Ok(None) => return Ok(0),
        Err(error) => return Err(io::Error::new(error.kind(), error.to_string())),
    };
    let db = hash(path.as_os_str().as_encoded_bytes());
    let mut file = File::create(owner.directory.join(format!("db-{db}.tsv")))?;
    writeln!(
        file,
        "db\t{db}\nbackend\t{}",
        if remote.is_some() { "remote" } else { "native" }
    )?;
    write!(file, "path_hex\t")?;
    hex(&mut file, path.as_os_str().as_encoded_bytes())?;
    writeln!(file)?;
    if let Some(identity) = remote {
        write!(file, "remote_identity\t")?;
        hex(&mut file, identity)?;
        writeln!(file)?;
    }
    for ks in shape.iter_ks() {
        write!(file, "keyspace\t{}\t", ks.id().0)?;
        hex(&mut file, ks.name().as_bytes())?;
        writeln!(file)?;
    }
    Ok(db)
}

pub(crate) struct Call {
    owner: &'static Recorder,
    pub id: u64,
    pub db: u64,
    op: u64,
    ks: KeySpace,
    count: u64,
    previous: (u64, u64),
    finished: bool,
}

impl Call {
    pub(crate) fn begin(db: u64, ks: KeySpace, op: u64, count: usize) -> Option<Self> {
        let owner = recorder()?;
        if !owner.admitting.load(Ordering::Acquire) || CURRENT.get().1 != 0 {
            return None;
        }
        owner.active.fetch_add(1, Ordering::AcqRel);
        if !owner.admitting.load(Ordering::Acquire) {
            owner.active.fetch_sub(1, Ordering::Release);
            return None;
        }
        let id = owner.id();
        let previous = CURRENT.replace((db, id));
        let call = Self {
            owner,
            id,
            db,
            op,
            ks,
            count: count as u64,
            previous,
            finished: false,
        };
        owner.send(call.record(1));
        Some(call)
    }
    fn record(&self, event: u64) -> Record {
        Record {
            event,
            db: self.db,
            call: self.id,
            op: self.op,
            ks: u64::from(self.ks.0),
            count: self.count,
            ..Record::default()
        }
    }
    pub(crate) fn member(
        &self,
        ks: KeySpace,
        key: &[u8],
        value: Option<&[u8]>,
        outcome: u64,
        position: usize,
    ) {
        self.owner.send(Record {
            ks: u64::from(ks.0),
            key: hash(key),
            key_len: key.len() as u64,
            value_len: value.map_or(0, |v| v.len() as u64),
            content: value.map_or(0, hash),
            outcome,
            position: position as u64,
            ..self.record(3)
        });
    }
    pub(crate) fn bound(&self, key: &[u8], upper: bool) {
        bounds(self.owner, self.db, self.id, self.ks, key, upper);
    }
    pub(crate) fn finish(mut self, success: bool) {
        self.owner.send(Record {
            outcome: u64::from(success),
            ..self.record(2)
        });
        self.finished = true;
    }
}
impl Drop for Call {
    fn drop(&mut self) {
        if !self.finished {
            self.owner.send(Record {
                outcome: 2,
                ..self.record(2)
            });
        }
        CURRENT.set(self.previous);
        self.owner.active.fetch_sub(1, Ordering::Release);
    }
}

pub(crate) fn read(
    call: &Call,
    ks: KeySpace,
    key: &[u8],
    result: &Result<Option<Bytes>>,
    position: usize,
) {
    match result {
        Ok(Some(value)) => call.member(ks, key, Some(value), 1, position),
        Ok(None) => call.member(ks, key, None, 0, position),
        Err(_) => call.member(ks, key, None, 2, position),
    }
}
pub(crate) fn exists(
    call: &Call,
    ks: KeySpace,
    key: &[u8],
    result: &Result<bool>,
    position: usize,
) {
    call.member(
        ks,
        key,
        None,
        match result {
            Ok(true) => 1,
            Ok(false) => 0,
            Err(_) => 2,
        },
        position,
    );
}

pub(crate) struct Batch {
    db: u64,
    pub id: u64,
    count: u64,
}
impl Batch {
    pub(crate) fn begin(db: u64) -> Option<Self> {
        let owner = recorder()?;
        Some(Self {
            db,
            id: owner.id(),
            count: 0,
        })
    }
    pub(crate) fn member(&mut self, ks: KeySpace, key: &[u8], value: Option<&[u8]>) {
        if let Some(owner) = recorder() {
            owner.send(Record {
                event: 4,
                db: self.db,
                group: self.id,
                position: self.count,
                ks: u64::from(ks.0),
                key: hash(key),
                key_len: key.len() as u64,
                value_len: value.map_or(0, |v| v.len() as u64),
                content: value.map_or(0, hash),
                op: if value.is_some() { PUT } else { DELETE },
                outcome: 1,
                ..Record::default()
            });
        }
        self.count += 1;
    }
    pub(crate) fn finish(&self, success: bool) {
        if let Some(owner) = recorder() {
            owner.send(Record {
                event: 5,
                db: self.db,
                call: CURRENT.get().1,
                group: self.id,
                count: self.count,
                outcome: u64::from(success),
                ..Record::default()
            });
        }
    }
}

fn bounds(owner: &Recorder, db: u64, call: u64, ks: KeySpace, bound: &[u8], upper: bool) {
    for (position, chunk) in bound.chunks(32).enumerate() {
        let mut data = [0; 32];
        data[..chunk.len()].copy_from_slice(chunk);
        owner.send(Record {
            event: 7,
            db,
            call,
            ks: u64::from(ks.0),
            key: hash(bound),
            key_len: bound.len() as u64,
            position: position as u64,
            count: chunk.len() as u64,
            outcome: u64::from(upper),
            data,
            ..Record::default()
        });
    }
    if bound.is_empty() {
        owner.send(Record {
            event: 7,
            db,
            call,
            ks: u64::from(ks.0),
            outcome: u64::from(upper),
            ..Record::default()
        });
    }
}

/// This wrapper exists only for enabled capture. It keeps the public iterator
/// methods and snapshot ownership unchanged; it never prefetches extra rows.
pub struct CapturedIterator {
    pub(super) inner: DbIterator,
    db: u64,
    ks: KeySpace,
    id: u64,
    returned: u64,
    ended: bool,
}
impl CapturedIterator {
    pub(super) fn wrap(inner: DbIterator, db: u64, ks: KeySpace) -> DbIterator {
        let Some(owner) = recorder() else {
            return inner;
        };
        let id = owner.id();
        owner.send(Record {
            event: 6,
            db,
            call: id,
            ks: u64::from(ks.0),
            ..Record::default()
        });
        DbIterator::Captured(Box::new(Self {
            inner,
            db,
            ks,
            id,
            returned: 0,
            ended: false,
        }))
    }
    pub(super) fn bound(&self, bound: &[u8], upper: bool) {
        if let Some(owner) = recorder() {
            bounds(owner, self.db, self.id, self.ks, bound, upper);
        }
    }
    pub(super) fn reverse(&self) {
        if let Some(owner) = recorder() {
            owner.send(Record {
                event: 10,
                db: self.db,
                call: self.id,
                ..Record::default()
            });
        }
    }
    fn end(&mut self, outcome: u64) {
        if !self.ended {
            if let Some(owner) = recorder() {
                owner.send(Record {
                    event: 9,
                    db: self.db,
                    call: self.id,
                    count: self.returned,
                    outcome,
                    ..Record::default()
                });
            }
            self.ended = true;
        }
    }
}
impl Iterator for CapturedIterator {
    type Item = Result<(Bytes, Bytes)>;
    fn next(&mut self) -> Option<Self::Item> {
        let call = Call::begin(self.db, self.ks, SCAN, 1);
        let result = self.inner.next();
        if let Some(call) = call {
            if let Some(Ok((key, value))) = &result {
                call.owner.send(Record {
                    event: 8,
                    db: self.db,
                    call: call.id,
                    group: self.id,
                    op: SCAN,
                    ks: u64::from(self.ks.0),
                    key: hash(key),
                    key_len: key.len() as u64,
                    value_len: value.len() as u64,
                    content: hash(value),
                    position: self.returned,
                    outcome: 1,
                    ..Record::default()
                });
            }
            call.finish(!matches!(result, Some(Err(_))));
        }
        match &result {
            Some(Ok(_)) => self.returned += 1,
            Some(Err(_)) => self.end(2),
            None => self.end(1),
        }
        result
    }
}
impl Drop for CapturedIterator {
    fn drop(&mut self) {
        self.end(0);
    }
}

#[cfg(test)]
pub(crate) fn finish_for_test() {
    let owner = RECORDER.as_ref().unwrap().as_ref().unwrap();
    fs::write(owner.directory.join("stop"), "").unwrap();
    owner.writer.lock().unwrap().take().unwrap().join().unwrap();
}
