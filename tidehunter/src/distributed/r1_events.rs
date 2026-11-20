//! Ordered R1 evidence streams (`schema_version` 1, contract in
//! `checkpoint-work/handoff-2026-11-17/agents/Q1Harness2.md`, "ordered R1
//! evidence").
//!
//! One bounded JSONL stream per process incarnation at
//! `<case_output>/capacity/r1-events/<role>/<incarnation>.jsonl`:
//! - line 1: header (run identity, producer role, incarnation, process
//!   identity, `mono_clock`);
//! - then events, `seq` contiguous from 0;
//! - on [`R1EventLog::finish`] only, one terminal line
//!   `{"terminal":{"complete":true,"events":N,"lost":L}}`.
//!
//! The file is renamed from `.jsonl.partial` after the terminal line, its
//! sync and the parent directory sync. A crashed or dropped stream stays
//! `.partial` with no terminal line: incomplete evidence. The engine writes
//! no manifest and no digest; the harness verifies the streams and builds
//! `manifest.json`.
//!
//! An event is published after the transition it names (callers emit it
//! after the operation returns). Within one stream, `after` names causal
//! predecessors by `{role,incarnation,seq}`. Across processes there is no
//! wire field: the harness joins on existing keys in each event (exact file
//! identity, fencing generation, seal root generation, batch sequences).
//!
//! Ownership and backpressure: one writer thread owns the file. Emitters
//! assign `seq` and `try_send` into a bounded channel (capacity
//! [`R1EventConfig::queue`]) under one small mutex, so seq order equals
//! channel order. A full queue, a payload over the cap, a stream over its
//! event cap or a failed writer counts one loss and never blocks or fails
//! the caller. Any loss makes the stream unqualified; the terminal line
//! reports it. Shutdown: `finish` sends the terminal command and joins; the
//! last handle's drop closes the channel and joins the writer after it has
//! flushed every queued event (the stream stays `.partial`).
//!
//! `monotonic_ns` is nanoseconds since this stream was opened (one process,
//! one monotonic clock); never compare it across streams.

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;
use std::time::Instant;

pub const SCHEMA_VERSION: u32 = 1;

/// Stream settings, fixed by the sealed job.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct R1EventConfig {
    /// Case output directory; streams go under `capacity/r1-events/`.
    pub case_output: PathBuf,
    pub run_id: String,
    pub case_id: String,
    pub tier: String,
    pub policy: String,
    pub retention_sha256: String,
    /// Events per stream; each further event is a counted loss.
    pub max_events: u64,
    /// Compact JSON bytes of one payload; a larger payload is a counted loss.
    pub max_payload_bytes: usize,
    /// Capacity of the emitter-to-writer channel (at least 1).
    pub queue: usize,
}

/// Stream-local causal identity of one event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventId {
    pub role: String,
    pub incarnation: String,
    pub seq: u64,
}

/// Precise file identity; `None` fields serialize as explicit nulls.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct FileRef {
    pub partition: Option<u32>,
    pub fencing_generation: Option<u64>,
    pub file_start: Option<u64>,
    pub file_end: Option<u64>,
    pub device: Option<u64>,
    pub inode: Option<u64>,
}

impl FileRef {
    /// A file whose inode is not known at this transition.
    pub fn located(partition: u32, fencing_generation: u64, start: u64, end: u64) -> Self {
        Self {
            partition: Some(partition),
            fencing_generation: Some(fencing_generation),
            file_start: Some(start),
            file_end: Some(end),
            device: None,
            inode: None,
        }
    }

    pub fn with_inode(mut self, device: u64, inode: u64) -> Self {
        self.device = Some(device);
        self.inode = Some(inode);
        self
    }
}

#[derive(Serialize)]
struct Header<'a> {
    schema_version: u32,
    run_id: &'a str,
    case_id: &'a str,
    tier: &'a str,
    policy: &'a str,
    retention_sha256: &'a str,
    database_id: String,
    producer_role: &'a str,
    incarnation: &'a str,
    boot_id: Option<String>,
    process_id: u32,
    process_start_ticks: Option<u64>,
    mono_clock: &'static str,
}

#[derive(Serialize)]
struct Record {
    seq: u64,
    monotonic_ns: u64,
    kind: &'static str,
    after: Vec<EventId>,
    partition: Option<u32>,
    fencing_generation: Option<u64>,
    file_start: Option<u64>,
    file_end: Option<u64>,
    device: Option<u64>,
    inode: Option<u64>,
    attempt_id: Option<u64>,
    payload: Value,
}

#[derive(Serialize)]
struct Terminal {
    complete: bool,
    events: u64,
    lost: u64,
}

#[derive(Serialize)]
struct TerminalLine {
    terminal: Terminal,
}

enum Command {
    Event(Record),
    Finish {
        events: u64,
        lost: u64,
        reply: mpsc::SyncSender<io::Result<()>>,
    },
}

struct EmitState {
    next_seq: u64,
    lost: u64,
    sender: Option<mpsc::SyncSender<Command>>,
}

struct Inner {
    role: String,
    incarnation: String,
    opened: Instant,
    max_events: u64,
    max_payload_bytes: usize,
    // Release by the writer on an I/O error; Acquire at emit.
    failed: Arc<AtomicBool>,
    state: Mutex<EmitState>,
    writer: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Close the channel first: the writer drains what is queued, flushes
        // and exits, so the join is bounded by that I/O.
        self.state.get_mut().sender.take();
        if let Some(writer) = self.writer.get_mut().take() {
            let _ = writer.join();
        }
    }
}

/// Handle to one process incarnation's stream. Cloning shares the stream.
#[derive(Clone)]
pub struct R1EventLog {
    inner: Arc<Inner>,
}

impl R1EventLog {
    /// Open the stream and start its writer. `role` is `coordinator` or
    /// `owner_O<partition>`; `incarnation` names this process incarnation.
    pub fn open(
        config: &R1EventConfig,
        role: &str,
        incarnation: &str,
        database_id: [u8; 16],
    ) -> io::Result<Self> {
        if config.queue == 0 || role.is_empty() || incarnation.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "r1 event stream needs a role, an incarnation and a queue",
            ));
        }
        let directory = config.case_output.join("capacity/r1-events").join(role);
        fs::create_dir_all(&directory)?;
        let final_path = directory.join(format!("{incarnation}.jsonl"));
        let partial = directory.join(format!("{incarnation}.jsonl.partial"));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&partial)?;
        let mut file = BufWriter::new(file);
        let header = Header {
            schema_version: SCHEMA_VERSION,
            run_id: &config.run_id,
            case_id: &config.case_id,
            tier: &config.tier,
            policy: &config.policy,
            retention_sha256: &config.retention_sha256,
            database_id: hex(&database_id),
            producer_role: role,
            incarnation,
            boot_id: fs::read_to_string("/proc/sys/kernel/random/boot_id")
                .ok()
                .map(|id| id.trim().to_owned()),
            process_id: std::process::id(),
            process_start_ticks: process_start_ticks(),
            mono_clock: "monotonic_ns",
        };
        serde_json::to_writer(&mut file, &header).map_err(io::Error::other)?;
        file.write_all(b"\n")?;
        file.flush()?;
        let (sender, receiver) = mpsc::sync_channel(config.queue);
        let failed = Arc::new(AtomicBool::new(false));
        let writer_failed = Arc::clone(&failed);
        let writer = std::thread::Builder::new()
            .name("r1-event-writer".into())
            .spawn(move || write_stream(file, receiver, &partial, &final_path, &writer_failed))?;
        Ok(Self {
            inner: Arc::new(Inner {
                role: role.to_owned(),
                incarnation: incarnation.to_owned(),
                opened: Instant::now(),
                max_events: config.max_events,
                max_payload_bytes: config.max_payload_bytes,
                failed,
                state: Mutex::new(EmitState {
                    next_seq: 0,
                    lost: 0,
                    sender: Some(sender),
                }),
                writer: Mutex::new(Some(writer)),
            }),
        })
    }

    pub fn role(&self) -> &str {
        &self.inner.role
    }

    pub fn incarnation(&self) -> &str {
        &self.inner.incarnation
    }

    /// Publish one transition that has already happened. Returns its ID, or
    /// `None` when the event was counted as lost. Never blocks on the writer.
    pub fn emit(
        &self,
        kind: &'static str,
        file: FileRef,
        attempt_id: Option<u64>,
        after: &[EventId],
        payload: Value,
    ) -> Option<EventId> {
        let payload_bytes = payload.to_string().len();
        let mut state = self.inner.state.lock();
        let sender = state.sender.as_ref()?;
        // Acquire: pairs with the writer's Release after a failed write.
        if payload_bytes > self.inner.max_payload_bytes
            || state.next_seq >= self.inner.max_events
            || self.inner.failed.load(Ordering::Acquire)
        {
            state.lost += 1;
            return None;
        }
        let seq = state.next_seq;
        let record = Record {
            seq,
            monotonic_ns: u64::try_from(self.inner.opened.elapsed().as_nanos()).unwrap_or(u64::MAX),
            kind,
            after: after.to_vec(),
            partition: file.partition,
            fencing_generation: file.fencing_generation,
            file_start: file.file_start,
            file_end: file.file_end,
            device: file.device,
            inode: file.inode,
            attempt_id,
            payload,
        };
        match sender.try_send(Command::Event(record)) {
            Ok(()) => {
                state.next_seq += 1;
                Some(EventId {
                    role: self.inner.role.clone(),
                    incarnation: self.inner.incarnation.clone(),
                    seq,
                })
            }
            Err(mpsc::TrySendError::Full(_) | mpsc::TrySendError::Disconnected(_)) => {
                state.lost += 1;
                None
            }
        }
    }

    /// Events counted as lost so far.
    pub fn lost(&self) -> u64 {
        self.inner.state.lock().lost
    }

    /// Write the terminal line, sync, rename to `.jsonl` and sync the
    /// directory. Later emits are ignored (not counted). Errors if the stream
    /// already failed or finished.
    pub fn finish(&self) -> io::Result<()> {
        let (reply, result) = mpsc::sync_channel(1);
        {
            let mut state = self.inner.state.lock();
            let sender = state
                .sender
                .take()
                .ok_or_else(|| io::Error::other("r1 event stream already finished"))?;
            let command = Command::Finish {
                events: state.next_seq,
                lost: state.lost,
                reply,
            };
            // Blocking send: the stream is closing and the writer drains it.
            sender
                .send(command)
                .map_err(|_| io::Error::other("r1 event writer stopped"))?;
        }
        let outcome = result
            .recv()
            .map_err(|_| io::Error::other("r1 event writer stopped"))?;
        if let Some(writer) = self.inner.writer.lock().take() {
            writer
                .join()
                .map_err(|_| io::Error::other("r1 event writer panicked"))?;
        }
        outcome
    }
}

fn write_stream(
    mut file: BufWriter<File>,
    receiver: mpsc::Receiver<Command>,
    partial: &Path,
    final_path: &Path,
    failed: &AtomicBool,
) {
    let mut failure: Option<io::Error> = None;
    // Ends when every sender is gone (handle dropped without finish): the
    // stream stays `.partial`, with everything written so far flushed.
    while let Ok(command) = receiver.recv() {
        let mut next = Some(command);
        while let Some(command) = next.take() {
            match command {
                Command::Event(record) => {
                    if failure.is_none()
                        && let Err(error) = write_line(&mut file, &record)
                    {
                        // Release: later emits count losses instead.
                        failed.store(true, Ordering::Release);
                        failure = Some(error);
                    }
                }
                Command::Finish {
                    events,
                    lost,
                    reply,
                } => {
                    let result = match failure.take() {
                        Some(error) => Err(error),
                        None => terminate(file, events, lost, partial, final_path),
                    };
                    let _ = reply.send(result);
                    return;
                }
            }
            next = receiver.try_recv().ok();
        }
        // Batch boundary: hand the bytes to the kernel. No fsync here.
        if failure.is_none()
            && let Err(error) = file.flush()
        {
            failed.store(true, Ordering::Release);
            failure = Some(error);
        }
    }
    let _ = file.flush();
}

fn write_line<T: Serialize>(file: &mut BufWriter<File>, value: &T) -> io::Result<()> {
    serde_json::to_writer(&mut *file, value).map_err(io::Error::other)?;
    file.write_all(b"\n")
}

fn terminate(
    mut file: BufWriter<File>,
    events: u64,
    lost: u64,
    partial: &Path,
    final_path: &Path,
) -> io::Result<()> {
    write_line(
        &mut file,
        &TerminalLine {
            terminal: Terminal {
                complete: true,
                events,
                lost,
            },
        },
    )?;
    let file = file.into_inner().map_err(io::IntoInnerError::into_error)?;
    file.sync_all()?;
    fs::rename(partial, final_path)?;
    let parent = final_path
        .parent()
        .ok_or_else(|| io::Error::other("r1 event stream has no parent directory"))?;
    File::open(parent)?.sync_all()
}

/// Lowercase hex of `bytes` (identities in headers and payloads).
pub(crate) fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    out
}

/// Field 22 of `/proc/self/stat` (start time in clock ticks after boot).
fn process_start_ticks() -> Option<u64> {
    let stat = fs::read_to_string("/proc/self/stat").ok()?;
    // The command name can hold spaces and parentheses: split after the last ')'.
    let rest = &stat[stat.rfind(')')? + 1..];
    // `rest` starts at field 3 (state); field 22 is the 20th from there.
    rest.split_whitespace().nth(19)?.parse().ok()
}

/// A fresh random incarnation name (32 lowercase hex characters).
pub fn new_incarnation() -> String {
    hex(&rand::random::<[u8; 16]>())
}

#[cfg(test)]
pub(crate) mod strict_json;
#[cfg(test)]
mod tests;
