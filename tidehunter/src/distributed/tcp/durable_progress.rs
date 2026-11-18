//! Owner durable progress for the coordinator's durable cut (`Request::DurableProgress`).
//!
//! A control worker validates a poll and answers it at once when its condition already
//! holds. Otherwise it parks the poll in the progress hub: one thread per node that owns
//! the parked polls. The hub answers a poll when the log's synced point satisfies it (each
//! log syncer notifies the hub after every sync attempt), at its deadline with the
//! current report, or with an error before a fence or seal and at shutdown. A parked poll
//! holds no control admission, no stream slot and no strong log handle.

use super::super::storage::{LogDurableProgress, PartitionLog};
use super::*;
use std::sync::{Weak, mpsc};

#[cfg(test)]
#[path = "durable_progress_tests.rs"]
mod tests;

/// Longest wait an owner grants one poll; a longer request is capped.
pub const DURABLE_PROGRESS_MAX_WAIT: Duration = Duration::from_secs(5);
/// Parked polls per node. The coordinator keeps one poll per owner connection; a poll
/// beyond this bound is answered at once with the current report.
const MAX_WAITERS: usize = 16;
/// Hub command capacity. Producers: control workers (one `Wait` per poll, at most the
/// control queue's admitted commands), log syncers (`Advanced`, dropped when full),
/// fence/seal controls and shutdown (one blocking send each; the hub waits for nothing
/// but this channel, so a blocking send always completes).
const HUB_COMMANDS: usize = 64;

pub(super) fn capped_wait(max_wait_ms: u32) -> Duration {
    Duration::from_millis(u64::from(max_wait_ms)).min(DURABLE_PROGRESS_MAX_WAIT)
}

const BOOT_ID_PATH: &str = "/proc/sys/kernel/random/boot_id";

/// The kernel boot id. A changed id means the owner host rebooted, so its page cache
/// (appended but unsynced WAL bytes) did not survive.
pub(super) fn read_boot_id() -> Result<[u8; 16], TcpError> {
    let text = fs::read_to_string(BOOT_ID_PATH)
        .map_err(|error| TcpError::new(format!("read {BOOT_ID_PATH}: {error}")))?;
    parse_boot_id(&text)
        .ok_or_else(|| TcpError::new(format!("{BOOT_ID_PATH} is not a UUID: {text:?}")))
}

/// Parse the kernel's `8-4-4-4-12` hexadecimal UUID text.
fn parse_boot_id(text: &str) -> Option<[u8; 16]> {
    let text = text.trim();
    let groups: Vec<_> = text.split('-').map(str::len).collect();
    if groups != [8, 4, 4, 4, 12] {
        return None;
    }
    let mut digits = text.bytes().filter(|byte| *byte != b'-');
    let mut id = [0u8; 16];
    for byte in &mut id {
        let high = char::from(digits.next()?).to_digit(16)?;
        let low = char::from(digits.next()?).to_digit(16)?;
        *byte = u8::try_from(high << 4 | low).ok()?;
    }
    Some(id)
}

/// One owner's durable progress in stream `epoch`, the input of the coordinator's
/// durable cut.
///
/// Contract: every batch the owner holds in stream `epoch` with a version at or below
/// `synced` is fsynced (WAL data, namespace and live frontier root). A batch the
/// coordinator wrote or sent that this report does not cover is unadopted (version above
/// `appended`) or unsynced (version above `synced`). `synced` is the appended tail
/// captured by the sync that covered it, never the appended tail at reply time. Versions
/// are global batch versions; the owner does not know owner sequences. `idle` is a
/// diagnostic: the coordinator decides idleness from its own receipts.
///
/// `generation` counts advances of the synced point on the owner's log handle and
/// restarts with the handle. A recovered (sealed) handle reports no versions.
/// `boot_id` is the owner's kernel boot id (see `NodeStatus::boot_id`): the coordinator
/// records it with the report to judge owner continuity at recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DurableProgressReport {
    pub database: [u8; 16],
    pub partition: u32,
    pub authority_epoch: u64,
    pub epoch: u64,
    pub generation: u64,
    pub synced: Option<BatchVersion>,
    pub appended: Option<BatchVersion>,
    pub synced_end: u64,
    pub appended_end: u64,
    pub idle: bool,
    pub boot_id: [u8; 16],
}

/// The request identity a report echoes.
#[derive(Clone, Copy)]
pub(super) struct PollIdentity {
    pub(super) database: [u8; 16],
    pub(super) partition: u32,
    pub(super) authority_epoch: u64,
    pub(super) epoch: u64,
    pub(super) boot_id: [u8; 16],
}

impl PollIdentity {
    pub(super) fn report(self, progress: LogDurableProgress) -> DurableProgressReport {
        DurableProgressReport {
            database: self.database,
            partition: self.partition,
            authority_epoch: self.authority_epoch,
            epoch: self.epoch,
            generation: progress.generation,
            synced: progress.synced,
            appended: progress.appended,
            synced_end: progress.synced_end,
            appended_end: progress.appended_end,
            idle: progress.idle(),
            boot_id: self.boot_id,
        }
    }
}

/// What completes a parked poll before its deadline.
#[derive(Clone, Copy, Debug)]
pub(super) enum Wake {
    /// The synced point advanced past the generation the caller saw.
    Generation(u64),
    /// The nudge sync reached this WAL end.
    Synced(u64),
}

impl Wake {
    pub(super) fn ready(self, progress: &LogDurableProgress) -> bool {
        match self {
            Self::Generation(after) => progress.generation > after,
            Self::Synced(target) => progress.synced_end >= target,
        }
    }
}

/// A parked poll, owned by the hub until it is answered.
pub(super) struct Waiter {
    pub(super) id: u64,
    pub(super) completed: mpsc::SyncSender<reactor::WorkCompletion>,
    pub(super) profile: ConnectionProfile,
    pub(super) measured: bool,
    pub(super) began: Option<Instant>,
    pub(super) identity: PollIdentity,
    pub(super) log: Weak<PartitionLog>,
    pub(super) wake: Wake,
    pub(super) deadline: Instant,
}

impl Waiter {
    /// The reply if the poll is complete at `now`.
    fn outcome(&self, now: Instant) -> Option<Result<Response, TcpError>> {
        let Some(log) = self.log.upgrade() else {
            return Some(Err(TcpError::new("durable progress stream was retired")));
        };
        let progress = log.durable_progress();
        if !self.wake.ready(&progress) {
            if let Wake::Synced(_) = self.wake
                && let Some(reason) = log.frontier().and_then(|frontier| frontier.failure())
            {
                return Some(Err(TcpError::new(format!(
                    "durable progress sync failed: {reason}"
                ))));
            }
            if now < self.deadline {
                return None;
            }
        }
        Some(Ok(Response::DurableProgress(
            self.identity.report(progress),
        )))
    }

    fn answer(self, response: Result<Response, TcpError>) {
        control::deliver(
            self.id,
            self.completed,
            response,
            self.profile,
            self.measured,
            self.began,
        );
    }

    /// Answer with the current report, for a poll the hub cannot park.
    fn answer_now(self) {
        let response = match self.log.upgrade() {
            Some(log) => Ok(Response::DurableProgress(
                self.identity.report(log.durable_progress()),
            )),
            None => Err(TcpError::new("durable progress stream was retired")),
        };
        self.answer(response);
    }
}

enum Command {
    Wait(Box<Waiter>),
    /// A log finished a sync attempt; re-check every parked poll.
    Advanced,
    /// A fence or seal is about to run: answer every parked poll with an error, then
    /// acknowledge.
    Retire(mpsc::SyncSender<()>),
    Stop,
}

/// Sender side of the progress hub, held by the control owner and every log syncer.
#[derive(Clone)]
pub(super) struct HubSender(mpsc::SyncSender<Command>);

impl HubSender {
    /// Call after each sync attempt, success or failure. A full queue drops the message:
    /// the hub re-checks every poll after each queued command, and a queued command is
    /// handled after this attempt's publication or frontier failure. A stopped hub has
    /// no polls.
    pub(super) fn advanced(&self) {
        let _ = self.0.try_send(Command::Advanced);
    }

    /// Answer every parked poll with an error and wait until the hub did. Called by an
    /// exclusive control, so no poll can park meanwhile. The waits are safe: the hub
    /// never waits for a control worker. A stopped hub has no polls.
    pub(super) fn retire_parked(&self) {
        let (done, finished) = mpsc::sync_channel(1);
        if self.0.send(Command::Retire(done)).is_ok() {
            let _ = finished.recv();
        }
    }

    /// Park `waiter`. A full or stopped hub answers it at once with the current report.
    pub(super) fn park(&self, waiter: Box<Waiter>) {
        if let Err(
            mpsc::TrySendError::Full(Command::Wait(waiter))
            | mpsc::TrySendError::Disconnected(Command::Wait(waiter)),
        ) = self.0.try_send(Command::Wait(waiter))
        {
            waiter.answer_now();
        }
    }
}

/// The progress hub thread. Dropping it answers every parked poll and joins the thread.
pub(super) struct Hub {
    sender: HubSender,
    thread: Option<thread::JoinHandle<()>>,
}

impl Hub {
    pub(super) fn start() -> io::Result<Self> {
        let (sender, receiver) = mpsc::sync_channel(HUB_COMMANDS);
        let thread = thread::Builder::new()
            .name("value-wal-progress".into())
            .spawn(move || run(&receiver))?;
        Ok(Self {
            sender: HubSender(sender),
            thread: Some(thread),
        })
    }

    pub(super) fn sender(&self) -> HubSender {
        self.sender.clone()
    }
}

impl Drop for Hub {
    fn drop(&mut self) {
        // Commands queued before `Stop` are handled first; `Stop` answers what is left.
        let _ = self.sender.0.send(Command::Stop);
        if let Some(thread) = self.thread.take() {
            // A panicked hub owns no durable state; its parked polls time out at the client.
            let _ = thread.join();
        }
    }
}

fn run(receiver: &mpsc::Receiver<Command>) {
    let mut waiters: Vec<Waiter> = Vec::with_capacity(MAX_WAITERS);
    // Intended receive loop: it ends on `Stop` or when every sender is gone.
    loop {
        let next = match waiters.iter().map(|waiter| waiter.deadline).min() {
            None => receiver.recv().ok(),
            Some(deadline) => {
                match receiver.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                    Ok(command) => Some(command),
                    // A deadline passed: re-check as after a sync.
                    Err(mpsc::RecvTimeoutError::Timeout) => Some(Command::Advanced),
                    Err(mpsc::RecvTimeoutError::Disconnected) => None,
                }
            }
        };
        match next {
            Some(Command::Wait(waiter)) if waiters.len() < MAX_WAITERS => waiters.push(*waiter),
            Some(Command::Wait(waiter)) => waiter.answer_now(),
            Some(Command::Advanced) => {}
            Some(Command::Retire(done)) => {
                fail_all(
                    &mut waiters,
                    "owner fence or seal ended a parked durable progress poll",
                );
                let _ = done.send(());
            }
            Some(Command::Stop) | None => {
                fail_all(&mut waiters, "owner stopped durable progress polls");
                return;
            }
        }
        // At most MAX_WAITERS iterations. A new waiter is checked here too, so a sync
        // published between the worker's check and the park is not missed.
        let now = Instant::now();
        let mut index = 0;
        while index < waiters.len() {
            match waiters[index].outcome(now) {
                Some(response) => waiters.swap_remove(index).answer(response),
                None => index += 1,
            }
        }
    }
}

fn fail_all(waiters: &mut Vec<Waiter>, reason: &str) {
    for waiter in waiters.drain(..) {
        waiter.answer(Err(TcpError::new(reason)));
    }
}

impl Client {
    /// Long-poll the owner's durable progress of stream `epoch` (see
    /// [`DurableProgressReport`]). Without `sync`, the reply comes when the owner's
    /// generation exceeds `after`, or after `max_wait` (capped at
    /// [`DURABLE_PROGRESS_MAX_WAIT`]) with the current report. With `sync` (a nudge), the
    /// owner adopts leased frames and syncs everything appended now; the reply comes when
    /// that sync completes, or after `max_wait` with the current report. A failed sync, a
    /// stale or fenced `authority_epoch`, an abandoned epoch, or a fence, seal or shutdown
    /// during the wait is an error. Use a dedicated connection: the wait occupies it.
    pub fn durable_progress(
        &mut self,
        authority_epoch: u64,
        epoch: u64,
        after: u64,
        max_wait: Duration,
        sync: bool,
    ) -> Result<DurableProgressReport, TcpError> {
        let max_wait_ms =
            u32::try_from(max_wait.min(DURABLE_PROGRESS_MAX_WAIT).as_millis()).unwrap_or(u32::MAX);
        match self.request(Request::DurableProgress {
            authority_epoch,
            epoch,
            after,
            max_wait_ms,
            sync,
        })? {
            Response::DurableProgress(report)
                if report.database == self.expected.inventory.database
                    && report.partition == self.expected.partition
                    && report.authority_epoch == authority_epoch
                    && report.epoch == epoch =>
            {
                Ok(report)
            }
            response => self.wrong_variant(
                "DurableProgress",
                &response,
                "DurableProgress with matching database, partition, authority_epoch and epoch",
            ),
        }
    }
}

#[cfg(test)]
impl StorageNode {
    /// The report a poll of stream `epoch` under `authority_epoch` would return now.
    pub(crate) fn durable_progress_for_test(
        &self,
        authority_epoch: u64,
        epoch: u64,
    ) -> Result<DurableProgressReport, TcpError> {
        let inventory = self.inventory.lock();
        let identity = PollIdentity {
            database: self.control_context.partition.inventory.database,
            partition: self.control_context.partition.partition,
            authority_epoch,
            epoch,
            boot_id: inventory.boot_id(),
        };
        let log = inventory.durable_progress_log(authority_epoch, epoch)?;
        drop(inventory);
        Ok(identity
            .report(log.map_or_else(LogDurableProgress::default, |log| log.durable_progress())))
    }
}
