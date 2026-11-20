use super::*;

pub(super) const MAX_EVENTS: usize = 32;

/// Fixed-size, whole-window wall spans. Stages can nest; these are not CPU
/// timings. Bucket 63 includes everything at or beyond 63 seconds.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WriteBlockStage {
    AdmissionCredit,
    Allocate,
    IdentityLookup,
    AdmissionIndexLock,
    PublicationIndexLock,
    Encode,
    OwnerQueue,
    PublishLock,
    Publish,
    Checkpoint,
    Snapshot,
}
pub(super) const BLOCK_STAGES: [WriteBlockStage; 11] = [
    WriteBlockStage::AdmissionCredit,
    WriteBlockStage::Allocate,
    WriteBlockStage::IdentityLookup,
    WriteBlockStage::AdmissionIndexLock,
    WriteBlockStage::PublicationIndexLock,
    WriteBlockStage::Encode,
    WriteBlockStage::OwnerQueue,
    WriteBlockStage::PublishLock,
    WriteBlockStage::Publish,
    WriteBlockStage::Checkpoint,
    WriteBlockStage::Snapshot,
];
pub(super) const BLOCK_BUCKETS: usize = 64;

#[derive(Default)]
pub(super) struct BlockCounter {
    pub(super) calls: AtomicU64,
    pub(super) ns: AtomicU64,
    pub(super) max_ns: AtomicU64,
    pub(super) max_depth: AtomicU64,
}

#[derive(Debug, Serialize)]
pub struct WriteBlockBucket {
    pub(super) stage: WriteBlockStage,
    pub(super) second: usize,
    pub(super) calls: u64,
    pub(super) ns: u64,
    pub(super) max_ns: u64,
    pub(super) max_depth: u64,
}

pub(super) struct BlockCounters {
    pub(super) origin_ns: AtomicU64,
    pub(super) buckets: [BlockCounter; BLOCK_STAGES.len() * BLOCK_BUCKETS],
}
impl Default for BlockCounters {
    fn default() -> Self {
        Self {
            origin_ns: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| BlockCounter::default()),
        }
    }
}
impl BlockCounters {
    pub(super) fn reset(&self, origin_ns: u64) {
        // Relaxed: arming is quiescent; enabled's release publishes this reset.
        self.origin_ns.store(origin_ns, Ordering::Relaxed);
        for bucket in &self.buckets {
            bucket.calls.store(0, Ordering::Relaxed);
            bucket.ns.store(0, Ordering::Relaxed);
            bucket.max_ns.store(0, Ordering::Relaxed);
            bucket.max_depth.store(0, Ordering::Relaxed);
        }
    }
    pub(super) fn snapshot(&self) -> Vec<WriteBlockBucket> {
        self.buckets
            .iter()
            .enumerate()
            .filter_map(|(index, bucket)| {
                let calls = bucket.calls.load(Ordering::Relaxed);
                (calls != 0).then(|| WriteBlockBucket {
                    stage: BLOCK_STAGES[index / BLOCK_BUCKETS],
                    second: index % BLOCK_BUCKETS,
                    calls,
                    ns: bucket.ns.load(Ordering::Relaxed),
                    max_ns: bucket.max_ns.load(Ordering::Relaxed),
                    max_depth: bucket.max_depth.load(Ordering::Relaxed),
                })
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WriteOwnerQueue {
    WorkerQueued,
    LeasedFlights,
}
pub(super) const OWNER_QUEUES: [WriteOwnerQueue; 2] = [
    WriteOwnerQueue::WorkerQueued,
    WriteOwnerQueue::LeasedFlights,
];

#[derive(Default)]
pub(super) struct QueueCounter {
    pub(super) samples: AtomicU64,
    pub(super) depth_sum: AtomicU64,
    pub(super) max_depth: AtomicU64,
}

#[repr(align(64))]
pub(super) struct OwnerCounters {
    pub(super) owner: u32,
    pub(super) queues: [QueueCounter; OWNER_QUEUES.len() * BLOCK_BUCKETS],
    pub(super) frames: AtomicU64,
    pub(super) fua_frames: AtomicU64,
    pub(super) batches: AtomicU64,
    pub(super) puts: AtomicU64,
}

#[derive(Debug, Serialize)]
pub struct WriteOwnerQueueBucket {
    pub(super) queue: WriteOwnerQueue,
    pub(super) second: usize,
    pub(super) samples: u64,
    pub(super) depth_sum: u64,
    pub(super) max_depth: u64,
}

#[derive(Debug, Serialize)]
pub struct WriteOwnerDiagnostics {
    pub(super) owner: u32,
    pub(super) submitted_frames: u64,
    pub(super) submitted_fua_frames: u64,
    pub(super) submitted_batches: u64,
    pub(super) submitted_puts: u64,
    pub(super) queues: Vec<WriteOwnerQueueBucket>,
}

impl OwnerCounters {
    pub(super) fn new(owner: u32) -> Self {
        Self {
            owner,
            queues: std::array::from_fn(|_| QueueCounter::default()),
            frames: AtomicU64::new(0),
            fua_frames: AtomicU64::new(0),
            batches: AtomicU64::new(0),
            puts: AtomicU64::new(0),
        }
    }
    pub(super) fn reset(&self) {
        // Relaxed: quiescent reset is published by enabled's Release store.
        for queue in &self.queues {
            queue.samples.store(0, Ordering::Relaxed);
            queue.depth_sum.store(0, Ordering::Relaxed);
            queue.max_depth.store(0, Ordering::Relaxed);
        }
        self.frames.store(0, Ordering::Relaxed);
        self.fua_frames.store(0, Ordering::Relaxed);
        self.batches.store(0, Ordering::Relaxed);
        self.puts.store(0, Ordering::Relaxed);
    }
    pub(super) fn snapshot(&self) -> WriteOwnerDiagnostics {
        WriteOwnerDiagnostics {
            owner: self.owner,
            submitted_frames: self.frames.load(Ordering::Relaxed),
            submitted_fua_frames: self.fua_frames.load(Ordering::Relaxed),
            submitted_batches: self.batches.load(Ordering::Relaxed),
            submitted_puts: self.puts.load(Ordering::Relaxed),
            queues: self
                .queues
                .iter()
                .enumerate()
                .filter_map(|(index, queue)| {
                    let samples = queue.samples.load(Ordering::Relaxed);
                    (samples != 0).then(|| WriteOwnerQueueBucket {
                        queue: OWNER_QUEUES[index / BLOCK_BUCKETS],
                        second: index % BLOCK_BUCKETS,
                        samples,
                        depth_sum: queue.depth_sum.load(Ordering::Relaxed),
                        max_depth: queue.max_depth.load(Ordering::Relaxed),
                    })
                })
                .collect(),
        }
    }
}

/// Exact local batch counts for the three coordinator write waits.
///
/// Observations count entries. Depth is the number of batches inside the wait
/// window, sampled at entry including the new batch. W1 counts owner-queue
/// READY entries until the owner worker dequeues them into a group. W2 counts
/// caller-allocated batches until the caller marks them READY in their owner
/// queue (validation-free encode and handoff). W3 counts successful owner-RPC
/// receipt batches harvested by the worker until its publication callback.
/// Wake attempts count handoffs to the consuming thread: one per W1 READY entry
/// and one per W3 harvested completion. A handoff is coalesced when it sent no
/// new hint: the W1 worker was not parked (it rechecks its queue before parking)
/// or its bounded `QueueReady` channel was full; W3 publication runs on the
/// harvesting worker, so every W3 handoff is coalesced. W2 has no handoff and
/// its wake counts stay zero.
///
/// No durations or clocks are collected. Counters saturate at `u64::MAX` and
/// set `saturated` when any counter reaches that cap. Storage has a fixed size.
/// `complete` is always true: no wake count is deferred past export.
#[derive(Debug, Serialize)]
pub struct WriteWaitMetrics {
    pub enabled: bool,
    pub saturated: bool,
    pub complete: bool,
    pub windows: [WriteWaitWindowMetrics; 3],
}

#[derive(Debug, Serialize)]
pub struct WriteWaitWindowMetrics {
    pub wait: &'static str,
    pub observations: u64,
    pub depth_sum: u64,
    pub depth_max: u64,
    pub wake_attempts: u64,
    pub wake_coalesced: u64,
}

#[derive(Clone, Copy)]
pub(crate) enum WriteWait {
    OwnerEnqueueToDequeue,
    AdmittedToAppendEnqueue,
    HarvestToDurable,
}

#[derive(Default)]
#[repr(align(64))]
pub(super) struct WaitCounters {
    pub(super) depth: AtomicU64,
    pub(super) observations: AtomicU64,
    pub(super) depth_sum: AtomicU64,
    pub(super) depth_max: AtomicU64,
    pub(super) wake_attempts: AtomicU64,
    pub(super) wake_coalesced: AtomicU64,
}

#[derive(Default)]
pub(super) struct WriteWaitCounters {
    pub(super) windows: [WaitCounters; 3],
    pub(super) saturated: AtomicBool,
}

impl WriteWaitCounters {
    // Relaxed operations order statistics only; they do not publish write data.
    pub(super) fn add(&self, counter: &AtomicU64, value: u64) -> u64 {
        let previous = counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                Some(old.saturating_add(value))
            })
            .unwrap_or_else(|old| old);
        if previous.saturating_add(value) == u64::MAX {
            self.saturated.store(true, Ordering::Relaxed);
        }
        previous.saturating_add(value)
    }

    pub(super) fn reset(&self) {
        for window in &self.windows {
            for counter in [
                &window.depth,
                &window.observations,
                &window.depth_sum,
                &window.depth_max,
                &window.wake_attempts,
                &window.wake_coalesced,
            ] {
                counter.store(0, Ordering::Relaxed);
            }
        }
        self.saturated.store(false, Ordering::Relaxed);
    }

    pub(super) fn snapshot(&self) -> WriteWaitMetrics {
        let complete = true; // No wake count is deferred past export.
        let names = [
            "owner_enqueue_to_dequeue",
            "admitted_to_append_enqueue",
            "harvest_to_durable",
        ];
        WriteWaitMetrics {
            enabled: true,
            saturated: self.saturated.load(Ordering::Relaxed),
            complete,
            windows: std::array::from_fn(|index| {
                let window = &self.windows[index];
                WriteWaitWindowMetrics {
                    wait: names[index],
                    observations: window.observations.load(Ordering::Relaxed),
                    depth_sum: window.depth_sum.load(Ordering::Relaxed),
                    depth_max: window.depth_max.load(Ordering::Relaxed),
                    wake_attempts: window.wake_attempts.load(Ordering::Relaxed),
                    wake_coalesced: window.wake_coalesced.load(Ordering::Relaxed),
                }
            }),
        }
    }
}
