//! Option-D scenarios driven through the real caller, owner-worker and
//! publication paths of a five-owner mTLS fixture. Ordering comes from
//! one-shot test hooks and condition predicates, never from elapsed time; the
//! only deadline-driven case is the caller timeout, whose resolution is held
//! by a hook until the caller has returned.

mod crash_cut;
mod readable;

use super::core::WriteCore;
use super::sequencer::Resolution;
use super::write::Retried;
use crate::distributed::codec::Mutation;
use crate::distributed::remote_db::RemoteDatabase;
use crate::distributed::remote_db::concurrent::AdmissionCharges;
use crate::distributed::remote_db::pipeline_tests::Fixture;
use crate::distributed::storage::LocalDbError;
use crate::distributed::types::{BatchId, BatchVersion};
use crate::key_shape::{KeyShape, KeySpace, KeyType};
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::{Duration, Instant};

/// Liveness guard for hook handshakes and results; never a latency bound.
const WAIT: Duration = Duration::from_secs(30);

type Hook = (mpsc::SyncSender<()>, mpsc::Receiver<()>);

/// A one-shot pause: install `hook`, receive on `entered`, send on `resume`.
/// Dropping `resume` (for example on a failed assertion) unblocks the hook.
fn gate() -> (Hook, mpsc::Receiver<()>, mpsc::SyncSender<()>) {
    let (entered, entering) = mpsc::sync_channel(1);
    let (resume, resumed) = mpsc::sync_channel(1);
    ((entered, resumed), entering, resume)
}

/// Runs `run` on a thread and returns its result channel. The thread drops its
/// `Arc` before it sends the result: a test that has received the result and
/// drops its own handle then holds the last reference, so the native database
/// lock is released before the test reopens the fixture (a clone dropped after
/// the send would race that reopen with `EAGAIN` on the lock).
fn spawn<T: Send + 'static>(
    core: &Arc<WriteCore>,
    run: impl FnOnce(&Arc<WriteCore>) -> T + Send + 'static,
) -> mpsc::Receiver<T> {
    let core = Arc::clone(core);
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let result = run(&core);
        drop(core);
        let _ = sender.send(result);
    });
    receiver
}

fn spawn_write(
    core: &Arc<WriteCore>,
    operation: [u8; 16],
    mutations: Vec<Mutation>,
) -> mpsc::Receiver<Result<BatchVersion, LocalDbError>> {
    spawn(core, move |core| core.write(operation, mutations, None))
}

/// Bounded wait for a state another thread reaches without a hook.
fn wait_for(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !condition() {
        assert!(Instant::now() < deadline, "{what}");
        thread::yield_now();
    }
}

fn put(key: u32, value: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: key.to_be_bytes().to_vec().into(),
        value: vec![value; 32].into(),
    }
}

fn value(db: &mut RemoteDatabase, key: u32) -> Option<Vec<u8>> {
    db.get(KeySpace(0), &key.to_be_bytes())
        .unwrap()
        .map(|value| value.to_vec())
}

fn version(core: &WriteCore, sequence: u64) -> BatchVersion {
    BatchVersion {
        epoch: core.epoch,
        sequence,
    }
}

fn shape() -> KeyShape {
    KeyShape::new_single(4, 1, KeyType::uniform(1))
}

/// A rejected non-blocking submit must leave every dimension unchanged.
/// Publication returns index/partition charges after waking its caller, so
/// first wait until only unpublished generations still hold a partition batch.
/// The caller must also quiesce publications that can still return charges:
/// a generation that resolves during the check would change the dimensions
/// under the comparison.
fn assert_capacity_rejected(core: &Arc<WriteCore>, operation: [u8; 16], key: u32) {
    wait_for("publication releases settled", || {
        core.admission.charges().partition_batches == core.unresolved_slots()
    });
    let next = core.next_sequence();
    let charges = core.admission.charges();
    let error = core.submit(operation, vec![put(key, 0)]).unwrap_err();
    assert!(
        error.message().contains("admission capacity reached"),
        "{error}"
    );
    assert_eq!(core.next_sequence(), next);
    assert_eq!(core.admission.charges(), charges);
    core.ensure_healthy().unwrap();
}

#[test]
fn aborted_floor_slot_is_crossed_and_its_dependent_publishes() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let core = Arc::clone(&db.core);
    let aborted = fixture.operation(0, 1);
    let (hook, encoding, resume) = gate();
    {
        let mut hooks = core.test_hooks.lock();
        hooks.fail_prepared_operation = Some(aborted);
        hooks.before_preparation = Some(hook);
    }
    // Sequence 0 is allocated (one in-flight owner, keys 1 and 2 registered),
    // then held before encoding; its injected encoding failure aborts it.
    let first = spawn_write(&core, aborted, vec![put(1, 1), put(2, 1)]);
    encoding.recv_timeout(WAIT).unwrap();
    assert_eq!(core.sequencer.resolution(0), Some(Resolution::Allocated));
    assert_eq!(core.in_flight.len(), 1);
    assert_eq!(core.pending_dependencies(), 2);
    // Sequence 1 shares key 1: durable, but parked behind its predecessor.
    let second = spawn_write(&core, fixture.operation(1, 2), vec![put(1, 2)]);
    wait_for("dependent durable behind its predecessor", || {
        core.publication_settled(1)
    });
    assert_eq!(core.sequencer.resolution(1), Some(Resolution::Durable));
    assert_eq!(
        core.publisher.lock().dependents.get(&0).map(Vec::as_slice),
        Some(&[1u64][..])
    );
    // Sequence 2 is independent and publishes beyond the unresolved floor. Its
    // writer is acknowledged only once the published prefix covers it (Ok means
    // readable), so the call parks in the prefix wait until the aborted head
    // resolves. The signal comes from the wait itself, after the batch published.
    let (waiting_hook, waiting) = mpsc::sync_channel(1);
    core.test_hooks.lock().drain_wait_entered = Some(waiting_hook);
    let independent = spawn_write(&core, fixture.operation(2, 3), vec![put(9, 9)]);
    waiting.recv_timeout(WAIT).unwrap();
    assert!(core.is_published(version(&core, 2)));
    assert_eq!(core.published_next(), 0);
    assert!(
        matches!(independent.try_recv(), Err(mpsc::TryRecvError::Empty)),
        "the write was acknowledged below an unresolved floor"
    );
    // Deviation (M) from native (PARITY.md, "Read visibility past an unresolved
    // prefix"): reads resolve at the publication cut, which stops at the
    // unresolved floor, so the batch published beyond it is invisible and its
    // writer is not yet acknowledged.
    assert_eq!(value(&mut db, 9), None);
    assert_eq!(value(&mut db, 1), None);
    // The allocated head has neither resolved nor released: no slot returns.
    assert_eq!(core.sequencer.reuse_next(), 0);
    assert_eq!(core.admission.charges().slots, 3);

    resume.send(()).unwrap();
    let error = first.recv_timeout(WAIT).unwrap().unwrap_err();
    assert!(
        error.message().contains("injected caller encoding failure"),
        "{error}"
    );
    // An abort before READY has no remote effect: a definite failure.
    assert_eq!(error.uncertain_version, None);
    assert_eq!(
        second.recv_timeout(WAIT).unwrap().unwrap(),
        version(&core, 1)
    );
    assert_eq!(
        independent.recv_timeout(WAIT).unwrap().unwrap(),
        version(&core, 2)
    );
    // Read-your-writes: the acknowledged write is readable as soon as it returns.
    assert_eq!(value(&mut db, 9), Some(vec![9; 32]));
    core.wait_settled().unwrap();
    assert_eq!(core.sequencer.resolution(0), Some(Resolution::Aborted));
    assert_eq!(core.sequencer.resolution(1), Some(Resolution::Published));
    assert_eq!(core.published_next(), 3);
    assert_eq!(core.sequencer.reuse_next(), 3);
    assert!(core.publisher.lock().dependents.is_empty());
    // The aborted generation left no in-flight owner and no receipt.
    assert!(core.in_flight.lookup(aborted).is_none());
    assert_eq!(core.in_flight.len(), 0);
    assert!(core.lookup_receipt(aborted).unwrap().is_none());
    assert_eq!(core.pending_dependencies(), 0);
    assert_eq!(value(&mut db, 1), Some(vec![2; 32]));
    assert_eq!(value(&mut db, 2), None);
    db.flush_index().unwrap();
    assert_eq!(core.admission.charges(), AdmissionCharges::default());
    // The aborted id left no effect: its retry is a fresh allocation.
    assert_eq!(
        core.write(aborted, vec![put(1, 1), put(2, 1)], None)
            .unwrap(),
        version(&core, 3)
    );
    drop(core);
    db.shutdown().unwrap();
    drop(db);
    // Sealed recovery tolerates the aborted sequence gap.
    let mut reopened = fixture.open();
    assert_eq!(value(&mut reopened, 1), Some(vec![1; 32]));
    assert_eq!(value(&mut reopened, 2), Some(vec![1; 32]));
    assert_eq!(value(&mut reopened, 9), Some(vec![9; 32]));
    reopened.shutdown().unwrap();
}

#[test]
fn ring_wrap_under_full_slot_credits_waits_for_release_and_resolution() {
    const CAPACITY: u64 = 4;
    let fixture = Fixture::new_with_config(128, shape(), 2, |config| {
        config.max_pending_batches = CAPACITY as usize;
    });
    let db = fixture.open();
    let core = Arc::clone(&db.core);
    // Sequence 0: published, caller paused before its release.
    let (hook, releasing, resume_release) = gate();
    core.test_hooks.lock().before_release = Some(hook);
    let unreleased = spawn_write(&core, fixture.operation(0, 0), vec![put(0, 0)]);
    releasing.recv_timeout(WAIT).unwrap();
    assert_eq!(core.sequencer.resolution(0), Some(Resolution::Published));
    // Sequence 1: caller released at return, owner 1 publication paused.
    let (hook, publishing, resume_publish) = gate();
    core.test_hooks.lock().before_publish = Some(hook);
    assert_eq!(
        core.submit(fixture.operation(1, 1), vec![put(1, 1)])
            .unwrap(),
        version(&core, 1)
    );
    publishing.recv_timeout(WAIT).unwrap();
    assert_eq!(core.sequencer.resolution(1), Some(Resolution::Ready));
    // Sequences 2 and 3 publish and release; every slot grant is now held.
    for seed in 2..CAPACITY {
        assert_eq!(
            core.submit(fixture.operation(0, seed), vec![put(seed as u32, 1)])
                .unwrap(),
            version(&core, seed)
        );
        // `submit`: an acknowledged write would wait for the held floor below.
        wait_for("batch published beyond the held floor", || {
            core.publication_settled(seed)
        });
    }
    let stale_receipt = core.durable_receipt(2).unwrap();
    assert_eq!(core.sequencer.reuse_next(), 0);
    let charges = core.admission.charges();
    assert_eq!((charges.slots, charges.requests), (CAPACITY as usize, 1));
    // Request and byte credits are free; the slot dimension alone rejects.
    let overflow = fixture.operation(0, 100);
    assert_capacity_rejected(&core, overflow, 100);
    assert_eq!(core.sequencer.generation(0), Some(0));
    assert_eq!(core.sequencer.generation(1), Some(1));

    let blocked = spawn_write(&core, overflow, vec![put(100, 100)]);
    wait_for("full-credit writer parked in admission", || {
        core.admission.queued_waiters() == 1
    });
    resume_release.send(()).unwrap();
    assert_eq!(
        unreleased.recv_timeout(WAIT).unwrap().unwrap(),
        version(&core, 0)
    );
    // Only sequence 0's slot returned; the parked writer reuses it. Its
    // acknowledgement waits for the held prefix (sequence 1), so the reuse is
    // observed from the sequencer; the result follows the publication below.
    wait_for("parked writer reused sequence 0's slot", || {
        core.sequencer.generation(0) == Some(CAPACITY)
    });
    assert_eq!(core.sequencer.reuse_next(), 1);
    assert_eq!(core.sequencer.generation(0), Some(CAPACITY));
    assert_eq!(core.sequencer.generation(1), Some(1));
    assert!(core.is_published(version(&core, 0)));
    assert_eq!(core.admission.charges().slots, CAPACITY as usize);
    // The reused generation publishes on its own worker and briefly still
    // holds its request, byte and checkpoint charges. Wait for that release:
    // the rejection check compares every admission dimension, so a publication
    // still in flight would change them under it.
    wait_for(
        "the reused generation publishes and returns its charges",
        || {
            let charges = core.admission.charges();
            charges.requests == 0 && charges.bytes == 0 && charges.partition_batches == 1
        },
    );
    // The released-but-unresolved generation still pins its slot.
    assert_capacity_rejected(&core, fixture.operation(0, 101), 101);

    resume_publish.send(()).unwrap();
    core.wait_settled().unwrap();
    // The parked writer's acknowledgement waited for sequence 1's publication.
    assert_eq!(
        blocked.recv_timeout(WAIT).unwrap().unwrap(),
        version(&core, CAPACITY)
    );
    assert_eq!(core.published_next(), CAPACITY + 1);
    assert_eq!(core.sequencer.reuse_next(), CAPACITY + 1);

    // Several wraps with every slot contended, alternating owners and a
    // same-key chain in each round.
    let mut seen: HashSet<u64> = (0..=CAPACITY).collect();
    for round in 0..4u64 {
        let barrier = Arc::new(Barrier::new(CAPACITY as usize));
        let results: Vec<_> = (0..CAPACITY)
            .map(|index| {
                let seed = 200 + round * 16 + index;
                let operation = fixture.operation((index % 2) as u32, seed);
                let key = if index < 2 { 300 } else { seed as u32 };
                let barrier = Arc::clone(&barrier);
                spawn(&core, move |core| {
                    barrier.wait();
                    core.write(operation, vec![put(key, index as u8)], None)
                })
            })
            .collect();
        for result in results {
            let version = result.recv_timeout(WAIT).unwrap().unwrap();
            assert_eq!(version.epoch, core.epoch);
            assert!(
                seen.insert(version.sequence),
                "duplicate sequence {}",
                version.sequence
            );
        }
        core.wait_settled().unwrap();
    }
    let next = core.next_sequence();
    assert_eq!(next, CAPACITY + 1 + 4 * CAPACITY);
    assert_eq!(seen, (0..next).collect());
    // Each owner formed its generations in strictly increasing global order,
    // and every allocated sequence was formed exactly once.
    let mut formed = Vec::new();
    for worker in &core.clients {
        let sequences: Vec<u64> = worker
            .accounting
            .formed_groups
            .lock()
            .iter()
            .flatten()
            .copied()
            .collect();
        assert!(
            sequences.windows(2).all(|pair| pair[0] < pair[1]),
            "owner order {sequences:?}"
        );
        formed.extend(sequences);
    }
    formed.sort_unstable();
    assert_eq!(formed, (0..next).collect::<Vec<_>>());
    // Every slot credit returned exactly once; an over-return would have
    // failed the core with a release-exceeds-charge error.
    let charges = core.admission.charges();
    assert_eq!((charges.requests, charges.bytes, charges.slots), (0, 0, 0));
    core.ensure_healthy().unwrap();

    // Generation checks: a retired sequence has no outcome or receipt.
    let stale = version(&core, 0);
    let error = core.sequencer.wait(stale, Instant::now()).unwrap_err();
    assert_eq!(error.uncertain_version, Some(stale));
    assert!(error.message().contains("generation changed"), "{error}");
    assert!(core.durable_receipt(2).is_none());
    assert!(core.pending_encoded_len(2).is_none());
    // A stale receipt for a retired generation fail-stops; it never becomes
    // the outcome of the generation now occupying that slot.
    let current = (0..next)
        .rev()
        .find(|sequence| sequence % CAPACITY == 2)
        .unwrap();
    core.on_durable(
        fixture.config.endpoints[0].partition,
        vec![(2, stale_receipt)],
    );
    assert!(core.ensure_healthy().is_err());
    assert!(
        core.first_failure
            .get()
            .unwrap()
            .contains("receipt outside active admission window")
    );
    assert_eq!(
        core.sequencer.resolution(current),
        Some(Resolution::Published)
    );
    let expected = version(&core, current);
    assert!(matches!(
        &core.sequencer.slot(current).data.lock().outcome,
        Some(Ok(outcome)) if *outcome == expected
    ));
    drop(core);
    assert!(db.shutdown().is_err());
}

#[test]
fn caller_timeout_then_late_resolution_reuses_only_after_both() {
    const CAPACITY: u64 = 4;
    // The caller deadline is the configured timeout. Resolution is held by a
    // hook until after the caller returned, so the timeout always fires first.
    let fixture = Fixture::new_with_root(
        tempdir::TempDir::new("write-core-caller-timeout").unwrap(),
        2,
        128,
        shape(),
        2,
        |config| {
            config.max_pending_batches = CAPACITY as usize;
            config.timeout = Duration::from_secs(2);
        },
        |_| {},
    );
    let mut db = fixture.open();
    let core = Arc::clone(&db.core);
    let operation = fixture.operation(0, 1);
    let (hook, publishing, resume) = gate();
    core.test_hooks.lock().before_publish = Some(hook);
    let caller = spawn_write(&core, operation, vec![put(1, 1)]);
    publishing.recv_timeout(WAIT).unwrap();
    let original = version(&core, 0);
    let error = caller.recv_timeout(WAIT).unwrap().unwrap_err();
    assert_eq!(error.uncertain_version, Some(original));
    assert!(
        error.message().contains("not resolved before deadline"),
        "{error}"
    );
    // A caller timeout is not fail-stop. It released its caller credit, but
    // the unresolved generation keeps its slot and its in-flight ownership.
    core.ensure_healthy().unwrap();
    assert_eq!(core.sequencer.resolution(0), Some(Resolution::Ready));
    let charges = core.admission.charges();
    assert_eq!((charges.requests, charges.bytes, charges.slots), (0, 0, 1));
    assert_eq!(core.in_flight.len(), 1);
    for seed in 1..CAPACITY {
        assert_eq!(
            core.submit(
                fixture.operation(1, 10 + seed),
                vec![put(10 + seed as u32, 1)]
            )
            .unwrap(),
            version(&core, seed)
        );
    }
    wait_for("later batches published beyond the unresolved head", || {
        (1..CAPACITY).all(|seed| core.publication_settled(seed))
    });
    assert_eq!(core.published_next(), 0);
    assert_eq!(core.sequencer.reuse_next(), 0);
    assert_capacity_rejected(&core, fixture.operation(1, 20), 20);
    assert_eq!(core.sequencer.generation(0), Some(0));

    resume.send(()).unwrap();
    core.wait_settled().unwrap();
    assert!(core.is_published(original));
    assert_eq!(core.published_next(), CAPACITY);
    assert_eq!(core.sequencer.reuse_next(), CAPACITY);
    assert_eq!(value(&mut db, 1), Some(vec![1; 32]));
    // The same id and payload obtain the original version; a changed payload
    // conflicts. Neither allocates.
    assert_eq!(
        core.write(operation, vec![put(1, 1)], None).unwrap(),
        original
    );
    let error = core.write(operation, vec![put(1, 9)], None).unwrap_err();
    assert!(
        error.message().contains("retry changed original batch"),
        "{error}"
    );
    assert_eq!(core.next_sequence(), CAPACITY);
    // Release and resolution both hold: the slot is reused by a new generation.
    assert_eq!(
        core.write(fixture.operation(1, 21), vec![put(21, 1)], None)
            .unwrap(),
        version(&core, CAPACITY)
    );
    assert_eq!(core.sequencer.generation(0), Some(CAPACITY));
    assert!(core.is_published(original));
    assert_eq!(value(&mut db, 1), Some(vec![1; 32]));
    core.ensure_healthy().unwrap();
    drop(core);
    db.flush_index().unwrap();
    db.shutdown().unwrap();
}

#[test]
fn concurrent_first_writes_of_one_id_allocate_once() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let core = Arc::clone(&db.core);
    let operation = fixture.operation(0, 1);
    let batch = vec![put(1, 1), put(2, 1)];
    // The late writer missed both owners and holds a first-write grant.
    let (hook, granted, resume_granted) = gate();
    core.test_hooks.lock().after_grant = Some(hook);
    let late = spawn_write(&core, operation, batch.clone());
    granted.recv_timeout(WAIT).unwrap();
    let charges = core.admission.charges();
    assert_eq!(charges.slots, 1);
    assert_eq!(core.in_flight.len(), 0);
    // The winner allocates sequence 0 and is held before publication.
    let (hook, publishing, resume_publish) = gate();
    core.test_hooks.lock().before_publish = Some(hook);
    let winner = spawn_write(&core, operation, batch.clone());
    publishing.recv_timeout(WAIT).unwrap();
    let original = version(&core, 0);
    assert_eq!(core.next_sequence(), 1);
    let error = core.write(operation, vec![put(1, 9)], None).unwrap_err();
    assert!(
        error.message().contains("retry changed original batch"),
        "{error}"
    );

    resume_granted.send(()).unwrap();
    // The late writer replans as a retry: it returns its slot, keeps only its
    // request credit, and observes the winner's in-flight generation.
    wait_for("late first writer replanned as a retry", || {
        let charges = core.admission.charges();
        charges.requests == 2 && charges.slots == 1 && core.in_flight.len() == 1
    });
    assert_eq!(core.next_sequence(), 1);
    resume_publish.send(()).unwrap();
    assert_eq!(winner.recv_timeout(WAIT).unwrap().unwrap(), original);
    assert_eq!(late.recv_timeout(WAIT).unwrap().unwrap(), original);
    core.wait_settled().unwrap();
    assert_eq!(core.next_sequence(), 1);
    // One published generation: its in-flight entry is retired and the retry
    // window answers the id from now on.
    assert_eq!(core.in_flight.len(), 0);
    assert_eq!(
        core.lookup_receipt(operation).unwrap().unwrap().version,
        original
    );
    assert_eq!(value(&mut db, 1), Some(vec![1; 32]));
    assert_eq!(value(&mut db, 2), Some(vec![1; 32]));
    drop(core);
    db.flush_index().unwrap();
    db.shutdown().unwrap();
}

/// A retry of an admitted but unpublished id answers with its recorded
/// version through `submit` and `submit_many`, allocates nothing and charges
/// no slot, and a changed payload is still refused (the tip contract,
/// `plan.identity.is_some()` at c97de76b). A retry that allocated a second
/// generation would apply a stale value over a later write of the same key.
#[test]
fn retry_of_an_unpublished_id_returns_its_version_without_allocating() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let core = Arc::clone(&db.core);
    let operation = fixture.operation(0, 1);
    let batch = vec![put(1, 1)];
    // The publisher is held before it publishes, so the id stays in flight.
    let (hook, publishing, resume_publish) = gate();
    core.test_hooks.lock().before_publish = Some(hook);
    let original = core.submit(operation, batch.clone()).unwrap();
    publishing.recv_timeout(WAIT).unwrap();
    assert_eq!(core.in_flight.len(), 1, "the id is admitted, not published");
    let slots = core.admission.charges().slots;
    let admitted = db.stats().admitted_batches;
    assert_eq!(core.submit(operation, batch.clone()).unwrap(), original);
    assert_eq!(
        core.submit_many(&[(operation, batch.clone())]).unwrap(),
        vec![original]
    );
    assert_eq!(core.next_sequence(), 1, "a retry allocates no sequence");
    assert_eq!(core.in_flight.len(), 1);
    assert_eq!(
        core.admission.charges().slots,
        slots,
        "a retry charges no slot"
    );
    assert_eq!(db.stats().admitted_batches, admitted);
    let error = core.submit(operation, vec![put(1, 9)]).unwrap_err();
    assert!(
        error.message().contains("retry changed original batch"),
        "{error}"
    );
    assert_eq!(core.next_sequence(), 1, "a refused retry allocates nothing");
    resume_publish.send(()).unwrap();
    core.wait_settled().unwrap();
    assert_eq!(core.next_sequence(), 1);
    assert_eq!(core.in_flight.len(), 0);
    assert_eq!(
        core.lookup_receipt(operation).unwrap().unwrap().version,
        original
    );
    assert_eq!(value(&mut db, 1), Some(vec![1; 32]));
    drop(core);
    db.flush_index().unwrap();
    db.shutdown().unwrap();
}

/// PR #63 P1, re-expressed for retry-window plus in-flight ownership: caller A
/// misses both owners and is held after its first-write grant. Caller B then
/// allocates the same operation and publishes it, and a snapshot stores the
/// window. A's stale miss must not allocate a second version: the
/// authoritative probe inside A's allocation section sees B's receipt,
/// replans as a retry, and returns B's outcome — or the conflict error when its
/// batch differs. Without that probe A allocates sequence 1 and overwrites a
/// newer value with an older payload.
#[test]
fn stale_miss_revalidates_against_the_retry_window() {
    for same_batch in [true, false] {
        let fixture = Fixture::new();
        let mut db = fixture.open();
        let core = Arc::clone(&db.core);
        let operation = fixture.operation(0, 1);
        let original = vec![put(1, 1), put(2, 1)];
        let late_batch = if same_batch {
            original.clone()
        } else {
            vec![put(1, 9)]
        };
        let (hook, granted, resume) = gate();
        core.test_hooks.lock().after_grant = Some(hook);
        let late = spawn_write(&core, operation, late_batch);
        granted.recv_timeout(WAIT).unwrap();
        let winner = core.write(operation, original, None).unwrap();
        assert_eq!(winner, version(&core, 0));
        core.flush_index().unwrap();
        // The winner is published: no in-flight owner remains and the retry
        // window is the only owner of its id, before and after the snapshot.
        assert_eq!(core.in_flight.len(), 0);
        assert_eq!(
            core.lookup_receipt(operation).unwrap().unwrap().version,
            winner
        );
        assert_eq!(core.next_sequence(), 1);

        resume.send(()).unwrap();
        let result = late.recv_timeout(WAIT).unwrap();
        if same_batch {
            assert_eq!(result.unwrap(), winner);
        } else {
            let error = result.unwrap_err();
            assert!(
                error.message().contains("retry changed original batch"),
                "{error}"
            );
            assert_eq!(error.uncertain_version, None);
        }
        // A stale miss never allocates: the winner's version stays unique.
        assert_eq!(core.next_sequence(), 1);
        core.ensure_healthy().unwrap();
        core.wait_settled().unwrap();
        assert_eq!(core.admission.charges(), AdmissionCharges::default());
        assert_eq!(value(&mut db, 1), Some(vec![1; 32]));
        assert_eq!(value(&mut db, 2), Some(vec![1; 32]));
        drop(core);
        db.shutdown().unwrap();
    }
}

/// A retry for a version of another epoch is already authoritative: the epoch
/// that produced it is closed, so its outcome is not re-derived here.
#[test]
fn a_retry_for_another_epoch_is_answered_as_published() {
    let fixture = Fixture::new();
    let db = fixture.open();
    let core = Arc::clone(&db.core);
    let other = BatchVersion {
        epoch: core.epoch + 1,
        sequence: 3,
    };
    assert_eq!(
        core.wait_retry([7; 16], other, Instant::now() + WAIT)
            .unwrap(),
        Retried::Published(other)
    );
    drop(core);
    db.shutdown().unwrap();
}

/// The two answers a reused slot can give, asserted where the code lives:
/// a published id is answered from the retry window without a second write, and
/// an id the window no longer holds **re-applies** — the only answer in this
/// design that makes the engine write another batch for one operation id, so a
/// future window-size change cannot silently turn it into `Published`.
#[test]
fn a_reused_slot_answers_from_the_window_or_reapplies() {
    let fixture = Fixture::new_with_config(128, shape(), 2, |config| {
        config.max_pending_batches = 2;
    });
    let mut db = fixture.open();
    let core = Arc::clone(&db.core);
    let published = fixture.operation(0, 1);
    assert_eq!(
        core.write(published, vec![put(1, 1)], None).unwrap(),
        version(&core, 0)
    );
    assert_eq!(
        core.write(fixture.operation(1, 2), vec![put(2, 1)], None)
            .unwrap(),
        version(&core, 1)
    );
    // Both slots released and resolved, so sequence 2 reuses slot 0.
    core.wait_settled().unwrap();
    assert_eq!(
        core.write(fixture.operation(0, 3), vec![put(3, 1)], None)
            .unwrap(),
        version(&core, 2)
    );
    assert_eq!(core.sequencer.generation(0), Some(2));
    let deadline = Instant::now() + WAIT;
    assert_eq!(
        core.wait_retry(published, version(&core, 0), deadline)
            .unwrap(),
        Retried::Published(version(&core, 0))
    );
    assert_eq!(
        core.wait_retry([9; 16], version(&core, 0), deadline)
            .unwrap(),
        Retried::Reapply
    );
    assert_eq!(value(&mut db, 1), Some(vec![1; 32]));
    drop(core);
    db.shutdown().unwrap();
}

/// A retry already waiting when its original aborts must become a fresh write,
/// never inherit the abort. The deleted identity test covered this through the
/// class table; class-free ownership keeps the property through the abort's
/// in-flight retirement plus `validate_retry`'s replan, and this is its cover.
#[test]
fn a_retry_waiting_on_an_aborted_original_reapplies_as_a_fresh_write() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let core = Arc::clone(&db.core);
    let operation = fixture.operation(1, 1);
    let (hook, encoding, resume) = gate();
    {
        let mut hooks = core.test_hooks.lock();
        hooks.fail_prepared_operation = Some(operation);
        hooks.before_preparation = Some(hook);
    }
    let first = spawn_write(&core, operation, vec![put(4, 1)]);
    encoding.recv_timeout(WAIT).unwrap();
    assert_eq!(core.sequencer.resolution(0), Some(Resolution::Allocated));
    // The retry observes the in-flight owner and waits on that generation.
    let retry = spawn_write(&core, operation, vec![put(4, 1)]);
    wait_for("retry admitted against the in-flight generation", || {
        core.admission.charges().requests == 2
    });
    resume.send(()).unwrap();
    let error = first.recv_timeout(WAIT).unwrap().unwrap_err();
    assert!(
        error.message().contains("injected caller encoding failure"),
        "{error}"
    );
    // The abort had no effect, so the retry is a fresh write with a new version.
    let reapplied = retry.recv_timeout(WAIT).unwrap().unwrap();
    assert_eq!(reapplied, version(&core, 1));
    assert_eq!(value(&mut db, 4), Some(vec![1; 32]));
    // From now on the id is answered by the retry window, not by a new version.
    assert_eq!(
        core.write(operation, vec![put(4, 1)], None).unwrap(),
        reapplied
    );
    assert_eq!(core.in_flight.len(), 0);
    drop(core);
    db.flush_index().unwrap();
    db.shutdown().unwrap();
}

#[test]
fn sequence_zero_flush_checkpoint_and_publication_wait() {
    let fixture = Fixture::new();
    let db = fixture.open();
    let core = Arc::clone(&db.core);
    assert_eq!(
        (
            core.next_sequence(),
            core.published_next(),
            core.sequencer.reuse_next()
        ),
        (0, 0, 0)
    );
    // Exclusive bounds: every control operation sees the empty prefix.
    core.wait_published_before(0).unwrap();
    core.drain().unwrap();
    core.flush_durable().unwrap();
    core.flush_index().unwrap();
    core.flush_index().unwrap();
    assert_eq!(
        (
            core.next_sequence(),
            core.published_next(),
            core.sequencer.reuse_next()
        ),
        (0, 0, 0)
    );
    assert_eq!(core.admission.charges(), AdmissionCharges::default());

    let operation = fixture.operation(0, 1);
    let (hook, publishing, resume) = gate();
    core.test_hooks.lock().before_publish = Some(hook);
    let first = spawn_write(&core, operation, vec![put(1, 1)]);
    publishing.recv_timeout(WAIT).unwrap();
    assert_eq!((core.next_sequence(), core.published_next()), (1, 0));
    // Sequence 0 is unpublished; a wait before it is still the empty prefix.
    core.wait_published_before(0).unwrap();
    let (entered, draining) = mpsc::sync_channel(1);
    core.test_hooks.lock().drain_wait_entered = Some(entered);
    let drain = spawn(&core, |core| core.drain());
    draining.recv_timeout(WAIT).unwrap();
    let flush = spawn(&core, |core| core.flush_durable());
    let index = spawn(&core, |core| core.flush_index());
    resume.send(()).unwrap();
    let original = first.recv_timeout(WAIT).unwrap().unwrap();
    assert_eq!(original, version(&core, 0));
    drain.recv_timeout(WAIT).unwrap().unwrap();
    flush.recv_timeout(WAIT).unwrap().unwrap();
    index.recv_timeout(WAIT).unwrap().unwrap();
    assert_eq!(core.published_next(), 1);
    core.wait_settled().unwrap();
    {
        // The live window holds the published batch (the in-epoch dedupe of its
        // id); it is not persisted, so a reopen starts with an empty window.
        let publisher = core.publisher.lock();
        assert_eq!(publisher.recent.len(), 1);
        assert_eq!(
            publisher
                .recent
                .iter()
                .next()
                .map(|receipt| receipt.version),
            Some(original)
        );
    }
    assert_eq!(core.admission.charges(), AdmissionCharges::default());
    core.flush_index().unwrap();
    drop(core);
    db.shutdown().unwrap();
    drop(db);
    let mut reopened = fixture.open();
    assert_eq!(value(&mut reopened, 1), Some(vec![1; 32]));
    // OWNER decision (BOARD 2026-11-20, decision 2): the retry window is empty
    // after a restart, so a retry after the reopen is a new write. The
    // published receipt is not restored: the retry takes a version in the new
    // epoch, starting again at sequence 0. It carries another value, so the
    // read proves it was applied; a repeat inside this epoch adds nothing.
    let retried = reopened.write_batch(operation, vec![put(1, 9)]).unwrap();
    assert_eq!(
        retried,
        BatchVersion {
            epoch: original.epoch + 1,
            sequence: 0,
        }
    );
    assert_eq!(
        reopened.write_batch(operation, vec![put(1, 9)]).unwrap(),
        retried,
        "an in-epoch retry returns the first retry's version"
    );
    assert_eq!(reopened.stats().admitted_batches, 1);
    assert_eq!(value(&mut reopened, 1), Some(vec![9; 32]));
    reopened.shutdown().unwrap();
}

#[test]
fn caller_granted_before_shutdown_gets_closed_error_and_returns_its_grant() {
    let fixture = Fixture::new();
    let db = fixture.open();
    let core = Arc::clone(&db.core);
    core.write(fixture.operation(0, 1), vec![put(1, 1)], None)
        .unwrap();
    core.wait_settled().unwrap();
    let before = core.admission.charges();
    let (hook, granted, resume_grant) = gate();
    core.test_hooks.lock().after_grant = Some(hook);
    let operation = fixture.operation(1, 2);
    let paused = spawn_write(&core, operation, vec![put(2, 2)]);
    granted.recv_timeout(WAIT).unwrap();
    let held = core.admission.charges();
    assert_eq!((held.requests, held.slots), (1, 1));
    // Byte credit is held for the paused batch, and no generation exists yet.
    assert!(held.bytes > before.bytes);
    assert_eq!(core.in_flight.len(), 0);
    // Shutdown closes allocations, drains and pauses in its final checkpoint.
    let (hook, checkpointing, resume_checkpoint) = gate();
    core.test_hooks.lock().checkpoint_pause = Some(hook);
    let shutdown = spawn(&core, |core| core.shutdown());
    checkpointing.recv_timeout(WAIT).unwrap();
    assert!(core.closing.load(Ordering::Acquire));
    assert_eq!(core.next_sequence(), 1);

    resume_grant.send(()).unwrap();
    let error = paused.recv_timeout(WAIT).unwrap().unwrap_err();
    assert!(
        error.message().contains("write authority closed"),
        "{error}"
    );
    assert_eq!(error.uncertain_version, None);
    // The whole grant returned and the shutdown snapshot is unchanged.
    assert_eq!(core.admission.charges(), before);
    assert_eq!(core.next_sequence(), 1);
    resume_checkpoint.send(()).unwrap();
    shutdown.recv_timeout(WAIT).unwrap().unwrap();
    assert_eq!((core.next_sequence(), core.sequencer.reuse_next()), (1, 1));
    drop(core);
    drop(db);
    let mut reopened = fixture.open();
    assert_eq!(value(&mut reopened, 1), Some(vec![1; 32]));
    assert_eq!(value(&mut reopened, 2), None);
    reopened.write_batch(operation, vec![put(2, 2)]).unwrap();
    assert_eq!(value(&mut reopened, 2), Some(vec![2; 32]));
    reopened.shutdown().unwrap();
}

#[test]
fn uncertain_append_fail_stops_with_unknown_everywhere() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let core = Arc::clone(&db.core);
    core.write(fixture.operation(1, 1), vec![put(1, 1)], None)
        .unwrap();
    core.wait_settled().unwrap();
    // Sequence 1 on owner 2 is durable at its owner but held before publication.
    let held_operation = fixture.operation(2, 2);
    let held_version = version(&core, 1);
    let (hook, publishing, resume) = gate();
    core.test_hooks.lock().before_publish = Some(hook);
    let held = spawn_write(&core, held_operation, vec![put(2, 2)]);
    publishing.recv_timeout(WAIT).unwrap();
    // Sequence 2 on owner 0 loses its append reply: an uncertain append.
    // The frame reaches the owner WAL file but its completion reports EIO.
    core.clients[0]
        .client
        .lock()
        .leased_test_fail_write(-libc::EIO);
    let uncertain_operation = fixture.operation(0, 3);
    let uncertain = version(&core, 2);
    let error = core
        .write(uncertain_operation, vec![put(3, 3)], None)
        .unwrap_err();
    assert_eq!(error.uncertain_version, Some(uncertain));
    // Fail-stop resolves every unresolved generation as unknown, never abort.
    let held_error = held.recv_timeout(WAIT).unwrap().unwrap_err();
    assert_eq!(held_error.uncertain_version, Some(held_version));
    assert_eq!(core.sequencer.resolution(1), Some(Resolution::Unknown));
    assert_eq!(core.sequencer.resolution(2), Some(Resolution::Unknown));
    resume.send(()).unwrap();
    assert!(core.ensure_healthy().is_err());
    assert!(core.reads.failed());
    assert!(
        core.write(fixture.operation(1, 4), vec![put(4, 4)], None)
            .is_err()
    );
    assert!(
        core.write(uncertain_operation, vec![put(3, 3)], None)
            .is_err()
    );
    assert!(core.write(held_operation, vec![put(2, 2)], None).is_err());
    assert!(db.get(KeySpace(0), &1u32.to_be_bytes()).is_err());
    assert!(core.drain().is_err());
    assert!(core.flush_durable().is_err());
    assert!(core.flush_index().is_err());
    assert!(core.wait_published_before(core.next_sequence()).is_err());
    assert_eq!(core.next_sequence(), 3);
    assert_eq!(core.published_next(), 1);
    // UNKNOWN generations never pass the reuse floor or return their slot.
    assert_eq!(core.sequencer.reuse_next(), 1);
    assert_eq!(core.unresolved_slots(), 2);
    assert_eq!(core.admission.charges().slots, 2);
    assert!(core.shutdown().is_err());
    // Workers are joined: the held worker's late receipt changed nothing.
    assert_eq!(core.sequencer.resolution(1), Some(Resolution::Unknown));
    assert_eq!(core.sequencer.resolution(2), Some(Resolution::Unknown));
    assert_eq!(core.published_next(), 1);
    drop(core);
    drop(db);
    let mut reopened = fixture.open();
    // Replay recovers the durable frames of both unknown outcomes.
    for key in 1..=3u32 {
        assert_eq!(value(&mut reopened, key), Some(vec![key as u8; 32]));
    }
    // OWNER decision (BOARD 2026-11-20, decision 2): the retry window is empty
    // after a restart, so a retry after the reopen is a new write. Neither
    // old-epoch version comes back; both retries take fresh sequences in the
    // next epoch, in call order, and each is admitted once.
    let next_epoch = uncertain.epoch + 1;
    // The retries carry other values, so the reads prove they were applied.
    assert_eq!(
        reopened
            .write_batch(uncertain_operation, vec![put(3, 13)])
            .unwrap(),
        BatchVersion {
            epoch: next_epoch,
            sequence: 0,
        }
    );
    assert_eq!(
        reopened
            .write_batch(uncertain_operation, vec![put(3, 13)])
            .unwrap(),
        BatchVersion {
            epoch: next_epoch,
            sequence: 0,
        },
        "an in-epoch retry returns the first retry's version"
    );
    assert_eq!(
        reopened
            .write_batch(held_operation, vec![put(2, 12)])
            .unwrap(),
        BatchVersion {
            epoch: next_epoch,
            sequence: 1,
        }
    );
    assert_eq!(reopened.stats().admitted_batches, 2);
    assert_eq!(value(&mut reopened, 1), Some(vec![1; 32]));
    assert_eq!(value(&mut reopened, 2), Some(vec![12; 32]));
    assert_eq!(value(&mut reopened, 3), Some(vec![13; 32]));
    reopened.shutdown().unwrap();
}

/// Owner 0's worker fail-stops on group A's publication while its successor
/// group B is appended at the owner. The leased queue holds the CQEs of both
/// frames (A = flight 1, B = flight 2: owner 0 has written nothing before)
/// until both writes are reaped, so B is in the owner WAL before A can
/// publish. B's completion is then either delivered first (its receipt still
/// waits behind A: leased receipts release in frame order) or withheld, so
/// the worker stops with B's flight outstanding and tears its completion down
/// after the fail-stop. Recovery replays B exactly once.
#[test]
fn publication_failure_with_appended_successor_group_recovers_it_once() {
    for successor_first in [false, true] {
        // Two frames must share one leased extent to be in flight together:
        // the default 4 KiB fragment holds a single frame.
        let fixture = Fixture::new_with_config(128, shape(), 2, |config| {
            config.storage.fragment_bytes = 64 * 1024;
            config.storage.wal_file_bytes = 128 * 1024;
        });
        let db = fixture.open();
        let core = Arc::clone(&db.core);
        let id = |operation| BatchId {
            database: fixture.config.storage.database,
            operation,
        };
        // Acknowledged before the failure; it must survive it.
        let acknowledged = fixture.operation(1, 50);
        assert_eq!(
            core.write(acknowledged, vec![put(2, 50)], None).unwrap(),
            version(&core, 0)
        );
        // Sequences 1-4 all write key 1 on owner 0, so each depends on the
        // previous one. Holding the worker until all four are READY makes
        // the two-batch group limit form A = [1, 2] and B = [3, 4].
        let connection = Arc::clone(&core.clients[0].client);
        let mut held = connection.lock();
        held.leased_test_hold_completions(&[1, 2]);
        let reaped = held.leased_test_reaped_writes();
        let mut callers = Vec::new();
        for value in 1..=4u8 {
            let operation = fixture.operation(0, u64::from(value));
            let caller = spawn_write(&core, operation, vec![put(1, value)]);
            // The owner entry turns READY after the slot does; the worker
            // forms groups from owner entries.
            wait_for("caller batch READY in the owner queue", || {
                core.clients[0].queue.snapshot().2.len() == usize::from(value)
            });
            callers.push((operation, value, caller));
        }
        let (head, next, ready) = core.clients[0].queue.snapshot();
        assert_eq!(next - head, 4);
        assert_eq!(
            ready
                .iter()
                .map(|&(_, sequence, _)| sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        core.test_hooks.lock().fail_publication = Some(1);
        drop(held);
        // Both frames are in the owner WAL file: their writes completed (CQEs
        // reaped, held or not). The owner indexes a leased frame only at
        // adoption, so its lookup cannot show an unadopted B; recovery below
        // proves B is replayed exactly once.
        wait_for("both group frames reaped", || {
            connection.lock().leased_test_reaped_writes() >= reaped + 2
        });
        if successor_first {
            // Deliver B's completion while A stays held. Leased receipts
            // release in frame order, so B is not accepted before A.
            connection.lock().leased_test_hold_completions(&[1]);
            assert!(core.durable_receipt(4).is_none());
            connection.lock().leased_test_release_held();
        } else {
            // Deliver A; B stays held through the fail-stop.
            connection.lock().leased_test_hold_completions(&[2]);
        }

        // Fail-stop: every caller gets an unknown outcome, never an abort.
        for (_, value, caller) in &callers {
            let error = caller.recv_timeout(WAIT).unwrap().unwrap_err();
            assert_eq!(
                error.uncertain_version,
                Some(version(&core, u64::from(*value))),
                "{error}"
            );
        }
        assert_eq!(
            *core.clients[0].accounting.formed_groups.lock(),
            vec![vec![1, 2], vec![3, 4]]
        );
        for sequence in 1..=4 {
            assert_eq!(
                core.sequencer.resolution(sequence),
                Some(Resolution::Unknown)
            );
        }
        // A's receipts were accepted before its publication failed. Leased
        // receipts release in frame order, so B's receipt never precedes A's
        // publication, whichever completion arrived first.
        let receipts: Vec<_> = (1..=4)
            .map(|sequence| core.durable_receipt(sequence))
            .collect();
        assert!(receipts[..2].iter().all(Option::is_some));
        assert!(receipts[2..].iter().all(Option::is_none));
        // B is in the owner WAL whether or not its receipt was released. A
        // leased owner learns frames at adoption, so adopt the extent first,
        // as the owner's flush and nudge paths do.
        fixture.nodes[0]
            .log_for_test(core.epoch)
            .self_adopt_live_extent()
            .unwrap();
        for (operation, _, _) in &callers {
            assert!(
                fixture.nodes[0]
                    .contains_batch(core.epoch, id(*operation))
                    .unwrap()
            );
        }
        assert!(core.ensure_healthy().is_err());
        assert!(core.drain().is_err());
        assert_eq!(core.published_next(), 1);
        assert!(core.shutdown().is_err());
        // Workers are joined: B's torn-down completion changed nothing.
        for sequence in 1..=4 {
            assert_eq!(
                core.sequencer.resolution(sequence),
                Some(Resolution::Unknown)
            );
        }
        assert_eq!(core.published_next(), 1);
        let epoch = core.epoch;
        drop(core);
        drop(db);
        // A client clone keeps the live connection registry (and its lock) alive.
        drop(connection);

        let mut reopened = fixture.open();
        // The acknowledged batch and both owner-0 groups are replayed, each
        // exactly once: a frame replayed twice or skipped changes this count.
        assert_eq!(reopened.recovery_stats().tail_batches, 5);
        assert_eq!(value(&mut reopened, 1), Some(vec![4; 32]));
        assert_eq!(value(&mut reopened, 2), Some(vec![50; 32]));
        // OWNER decision (BOARD 2026-11-20, decision 2): the retry window is
        // empty after a restart, so a retry after the reopen is a new write.
        // Replay restores no receipt, and every retry takes a fresh sequence in
        // the next epoch, in call order.
        let next = |sequence| BatchVersion {
            epoch: epoch + 1,
            sequence,
        };
        for (operation, _, _) in &callers {
            assert!(reopened.core.lookup_receipt(*operation).unwrap().is_none());
        }
        // The retry carries another value, so the read proves it was applied.
        assert_eq!(
            reopened
                .write_batch(acknowledged, vec![put(2, 51)])
                .unwrap(),
            next(0)
        );
        assert_eq!(
            reopened
                .write_batch(acknowledged, vec![put(2, 51)])
                .unwrap(),
            next(0),
            "an in-epoch retry returns the first retry's version"
        );
        for (operation, written, _) in callers {
            assert_eq!(
                reopened
                    .write_batch(operation, vec![put(1, written)])
                    .unwrap(),
                next(u64::from(written))
            );
            // The retry is applied again: it overwrites the later value.
            assert_eq!(
                value(&mut reopened, 1),
                Some(vec![written; 32]),
                "retry {written} must be applied as a new write"
            );
        }
        assert_eq!(reopened.stats().admitted_batches, 5);
        assert_eq!(value(&mut reopened, 2), Some(vec![51; 32]));
        reopened.shutdown().unwrap();
    }
}

#[test]
fn identical_retry_during_flight_and_after_publication_returns_original() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let core = Arc::clone(&db.core);
    let operation = fixture.operation(0, 1);
    let batch = vec![put(1, 1), put(2, 1)];
    let (hook, publishing, resume) = gate();
    core.test_hooks.lock().before_publish = Some(hook);
    let first = spawn_write(&core, operation, batch.clone());
    publishing.recv_timeout(WAIT).unwrap();
    let original = version(&core, 0);
    let retry = spawn_write(&core, operation, batch.clone());
    wait_for("retry admitted during flight", || {
        core.admission.charges().requests == 2
    });
    // The retry charged no slot and allocated nothing.
    let charges = core.admission.charges();
    assert_eq!(charges.slots, 1);
    assert_eq!(core.next_sequence(), 1);
    resume.send(()).unwrap();
    assert_eq!(first.recv_timeout(WAIT).unwrap().unwrap(), original);
    assert_eq!(retry.recv_timeout(WAIT).unwrap().unwrap(), original);
    core.wait_settled().unwrap();

    assert_eq!(
        core.write(operation, batch.clone(), None).unwrap(),
        original
    );
    assert_eq!(core.next_sequence(), 1);
    // A published retry neither allocates nor overwrites a newer value.
    assert_eq!(
        core.write(fixture.operation(1, 2), vec![put(1, 2)], None)
            .unwrap(),
        version(&core, 1)
    );
    assert_eq!(
        core.write(operation, batch.clone(), None).unwrap(),
        original
    );
    assert_eq!(value(&mut db, 1), Some(vec![2; 32]));
    assert_eq!(value(&mut db, 2), Some(vec![1; 32]));
    let error = core.write(operation, vec![put(1, 9)], None).unwrap_err();
    assert!(
        error.message().contains("retry changed original batch"),
        "{error}"
    );
    // After publication the retry is answered from the retained receipt
    // window, still without allocation.
    core.wait_settled().unwrap();
    db.flush_index().unwrap();
    assert_eq!(core.in_flight.len(), 0);
    assert_eq!(
        core.lookup_receipt(operation).unwrap().unwrap().version,
        original
    );
    assert_eq!(core.write(operation, batch, None).unwrap(), original);
    assert_eq!(core.next_sequence(), 2);
    assert_eq!(core.admission.charges(), AdmissionCharges::default());
    assert_eq!(value(&mut db, 1), Some(vec![2; 32]));
    drop(core);
    db.shutdown().unwrap();
}

#[test]
fn flush_waits_only_for_writes_allocated_before_it() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let core = Arc::clone(&db.core);
    // Sequence 0 on owner 0 is held before publication.
    let (hook, first_publishing, resume_first) = gate();
    core.test_hooks.lock().before_publish = Some(hook);
    let first = spawn_write(&core, fixture.operation(0, 1), vec![put(1, 1)]);
    first_publishing.recv_timeout(WAIT).unwrap();
    // Sequence 1 on owner 1 publishes beyond the held floor.
    assert_eq!(
        // `submit`: an acknowledged write would wait for the held floor below.
        core.submit(fixture.operation(1, 2), vec![put(2, 2)])
            .unwrap(),
        version(&core, 1)
    );
    wait_for("sequence 1 published beyond the held floor", || {
        core.publication_settled(1)
    });
    assert_eq!(core.published_next(), 0);
    // The drain snapshots next = 2 and waits.
    let (entered, draining) = mpsc::sync_channel(1);
    core.test_hooks.lock().drain_wait_entered = Some(entered);
    let drain = spawn(&core, |core| core.drain());
    draining.recv_timeout(WAIT).unwrap();
    // Sequence 2 on owner 2, allocated after that snapshot, is also held.
    let (hook, later_publishing, resume_later) = gate();
    core.test_hooks.lock().before_publish = Some(hook);
    let later = spawn_write(&core, fixture.operation(2, 3), vec![put(3, 3)]);
    later_publishing.recv_timeout(WAIT).unwrap();
    // This durable flush snapshots next = 3, so it must wait for sequence 2.
    let flush = spawn(&core, |core| core.flush_durable());

    resume_first.send(()).unwrap();
    assert_eq!(
        first.recv_timeout(WAIT).unwrap().unwrap(),
        version(&core, 0)
    );
    drain.recv_timeout(WAIT).unwrap().unwrap();
    assert_eq!(core.published_next(), 2);
    assert_eq!(core.sequencer.resolution(2), Some(Resolution::Ready));
    assert!(matches!(flush.try_recv(), Err(mpsc::TryRecvError::Empty)));
    resume_later.send(()).unwrap();
    assert_eq!(
        later.recv_timeout(WAIT).unwrap().unwrap(),
        version(&core, 2)
    );
    flush.recv_timeout(WAIT).unwrap().unwrap();
    assert_eq!(core.published_next(), 3);
    for key in 1..=3u32 {
        assert_eq!(value(&mut db, key), Some(vec![key as u8; 32]));
    }
    drop(core);
    db.flush_index().unwrap();
    db.shutdown().unwrap();
}

/// PR #62 P1, exact schedule. Window 3; keys A = 1, B = 2. Sequence 0 (owner
/// 0) writes A/B, 1 (owner 1) writes A, 2 (owner 2) writes B/A, so 2 depends
/// on 0 and 1 and parks behind 0 first. With owner 0 held, 1 and then 2 become
/// durable and park. Owner 0's receipt then wakes both in one publication
/// call: 2 must not move to 1 and back, so the three-slot work bound holds.
#[test]
fn fan_in_dependent_publishes_once_within_three_slot_window() {
    let fixture = Fixture::new_with_config(128, shape(), 2, |config| {
        config.max_pending_batches = 3;
    });
    let mut db = fixture.open();
    let core = Arc::clone(&db.core);
    let held_client = Arc::clone(&core.clients[0].client);
    let held = held_client.lock();
    let first = core
        .submit(fixture.operation(0, 1), vec![put(1, 1), put(2, 1)])
        .unwrap();
    assert_eq!(first, version(&core, 0));
    let second = core
        .submit(fixture.operation(1, 2), vec![put(1, 2)])
        .unwrap();
    assert_eq!(second, version(&core, 1));
    wait_for("sequence 1 durable behind 0", || {
        core.publication_settled(1)
    });
    let third = core
        .submit(fixture.operation(2, 3), vec![put(2, 3), put(1, 3)])
        .unwrap();
    assert_eq!(third, version(&core, 2));
    wait_for("sequence 2 durable behind 0", || {
        core.publication_settled(2)
    });
    assert_eq!(core.sequencer.resolution(0), Some(Resolution::Ready));
    assert_eq!(core.sequencer.resolution(1), Some(Resolution::Durable));
    assert_eq!(core.sequencer.resolution(2), Some(Resolution::Durable));
    assert_eq!(
        core.publisher.lock().dependents.get(&0).map(Vec::as_slice),
        Some(&[1u64, 2][..])
    );

    drop(held);
    db.drain().unwrap();
    core.ensure_healthy().unwrap();
    for sequence in 0..3 {
        assert_eq!(
            core.sequencer.resolution(sequence),
            Some(Resolution::Published)
        );
    }
    assert_eq!(core.published_next(), 3);
    assert!(core.publisher.lock().dependents.is_empty());
    assert_eq!(value(&mut db, 1), Some(vec![3; 32]));
    assert_eq!(value(&mut db, 2), Some(vec![3; 32]));
    core.wait_settled().unwrap();
    drop(core);
    db.flush_index().unwrap();
    db.shutdown().unwrap();
}

/// Seeded fan-in/fan-out over small windows: each round holds a random owner
/// subset, admits a full window of random multi-key batches on random owners,
/// then releases the held owners in random order. Every batch publishes, the
/// database stays healthy, and each key holds its highest-sequence write.
#[test]
fn random_fan_in_fan_out_windows_publish_every_batch() {
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};

    const WINDOW: usize = 4;
    const OWNERS: usize = 5;
    const KEYS: u32 = 3;
    let fixture = Fixture::new_with_config(128, shape(), 2, |config| {
        config.max_pending_batches = WINDOW;
    });
    let mut db = fixture.open();
    let core = Arc::clone(&db.core);
    let mut rng = StdRng::seed_from_u64(0x62_fa_9f);
    let mut latest = [None::<u8>; KEYS as usize];
    let mut seed = 0u64;
    for round in 0..16u8 {
        let window = rng.gen_range(2..=WINDOW);
        let clients: Vec<_> = (0..OWNERS)
            .map(|owner| Arc::clone(&core.clients[owner].client))
            .collect();
        let mut held: Vec<_> = (0..OWNERS)
            .filter(|_| rng.gen_bool(0.5))
            .map(|owner| (owner, clients[owner].lock()))
            .collect();
        let mut batches = Vec::with_capacity(window);
        for slot in 0..window {
            let owner = rng.gen_range(0..OWNERS);
            let mut keys: Vec<u32> = (0..KEYS).collect();
            keys.shuffle(&mut rng);
            keys.truncate(rng.gen_range(1..=KEYS as usize));
            let marker = round * WINDOW as u8 + slot as u8 + 1;
            seed += 1;
            let version = core
                .submit(
                    fixture.operation(owner as u32, seed),
                    keys.iter().map(|&key| put(key, marker)).collect(),
                )
                .unwrap();
            batches.push((owner, version.sequence));
            for key in keys {
                latest[key as usize] = Some(marker);
            }
        }
        for &(owner, sequence) in &batches {
            if held.iter().all(|(held, _)| *held != owner) {
                wait_for("unheld owner batch settled", || {
                    core.publication_settled(sequence)
                });
            }
        }
        held.shuffle(&mut rng);
        while let Some((owner, guard)) = held.pop() {
            drop(guard);
            for &(_, sequence) in batches.iter().filter(|batch| batch.0 == owner) {
                wait_for("released owner batch settled", || {
                    core.publication_settled(sequence)
                });
            }
        }
        db.drain().unwrap();
        core.ensure_healthy().unwrap();
        for &(_, sequence) in &batches {
            assert_eq!(
                core.sequencer.resolution(sequence),
                Some(Resolution::Published),
                "round {round}"
            );
        }
        assert!(core.publisher.lock().dependents.is_empty(), "round {round}");
        core.wait_settled().unwrap();
        for key in 0..KEYS {
            assert_eq!(
                value(&mut db, key),
                latest[key as usize].map(|marker| vec![marker; 32]),
                "round {round} key {key}"
            );
        }
    }
    drop(core);
    db.flush_index().unwrap();
    db.shutdown().unwrap();
}

/// A lane read that completes short on a controller the probe proves deleted
/// is resumed, not a read-authority verdict: the write core keeps accepting
/// writes after the healed read. This is the Sui checkpoint executor's cell:
/// read, then acknowledge more writes on the same authority.
#[test]
fn a_short_read_of_a_lost_controller_does_not_stop_the_write_core() {
    use crate::distributed::remote_db::pipeline_tests::newest_cut;
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let core = Arc::clone(&db.core);
    core.write(fixture.operation(0, 1), vec![put(1, 1)], None)
        .unwrap();
    core.wait_settled().unwrap();
    let position = core
        .table
        .get(KeySpace(0), &1u32.to_be_bytes(), newest_cut())
        .unwrap()
        .expect("the written key is indexed");
    // The lane route owns the quarantine and the retry budget.
    core.reads.set_inline_reads_for_test(false);
    // The ordinary-file fixture has no kernel target: register the owner's
    // export as a local namespace, then make the grant the lane opens report
    // the deletion the kernel would report and complete its read short.
    let path =
        crate::distributed::tcp::live_read::tests::owner_local_path(&fixture.nodes[0], position);
    let parked = core.reads.bench_park_live_lane(0);
    core.reads
        .bench_register_permanent_local(0, position, &path);
    core.reads.bench_arm_short_read_after_open(0, true);
    drop(parked);
    db.clear_read_cache();
    assert_eq!(
        value(&mut db, 1),
        Some(vec![1; 32]),
        "the resumed short read returns the value"
    );
    assert!(
        !core.reads.failed(),
        "a short read of a verifiably lost controller must not poison the read authority"
    );
    core.write(fixture.operation(0, 2), vec![put(2, 2)], None)
        .expect("the write after the healed read must be accepted");
    core.wait_settled().unwrap();
    db.clear_read_cache();
    assert_eq!(
        value(&mut db, 2),
        Some(vec![2; 32]),
        "the write after the healed read is applied"
    );
    db.shutdown().unwrap();
}
