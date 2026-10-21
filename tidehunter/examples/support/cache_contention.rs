//! Shared bounded Sui-adapter hit/read-write control for the existing benchmark drivers.
//! API timers exclude byte verification; raw caller samples retain their own identity.
use serde::Serialize;
use std::sync::{Arc, Barrier};
use std::time::Instant;
use tidehunter::distributed::sui::Db;
use tidehunter::key_shape::KeySpace;
use tidehunter::minibytes::Bytes;

#[derive(Serialize)]
pub(super) struct Observation {
    pub callers: usize,
    pub reads_per_caller: usize,
    pub concurrent_durable_writes: usize,
    pub phase_wall_ns: u128,
    pub get_api_intervals_ns_by_caller: Vec<Vec<[u64; 2]>>,
    pub write_api_intervals_ns: Vec<[u64; 2]>,
    pub overlapping_reads_by_caller: Vec<usize>,
    pub remote_cache_counter_available: bool,
    pub remote_hits: Option<u64>,
    pub remote_misses: Option<u64>,
    pub remote_read_rpcs: Option<u64>,
    pub scope: &'static str,
}

/// Read only a pre-warmed immutable set while a separate caller durably overwrites
/// a disjoint key. Rendezvous bounds the offered population; it is not included in
/// individual get timers and does not assert a particular scheduler interleaving.
pub(super) fn measure(
    db: &Arc<Db>,
    ks: KeySpace,
    hot: &[(Bytes, Bytes)],
    writer: &(Bytes, Bytes),
    callers: usize,
) -> Result<Observation, String> {
    const ROUNDS: usize = 32;
    const READS_PER_ROUND: usize = 32;
    if !(1..=8).contains(&callers)
        || hot.is_empty()
        || hot.len() > 8
        || hot.iter().any(|(key, _)| key == &writer.0)
    {
        return Err("invalid bounded cache-contention workload".to_owned());
    }
    for _ in 0..2 {
        for (key, expected) in hot {
            if db
                .get(ks, key)
                .map_err(|e| format!("warm cache get: {e:?}"))?
                .as_ref()
                != Some(expected)
            {
                return Err("cache-contention warmup value mismatch".to_owned());
            }
        }
    }
    let before = db.remote_counters();
    let barrier = Barrier::new(callers + 1);
    let origin = Instant::now();
    let (samples, writes, wall) = std::thread::scope(|scope| {
        let mut tasks = Vec::with_capacity(callers);
        for caller in 0..callers {
            let barrier = &barrier;
            tasks.push(scope.spawn(move || {
                let mut samples = Vec::with_capacity(ROUNDS * READS_PER_ROUND);
                let mut failure = None;
                for round in 0..ROUNDS {
                    barrier.wait();
                    // Always finish both rendezvous even after a read failure, so
                    // another caller cannot be stranded in this finite workload.
                    if failure.is_none() {
                        for iteration in 0..READS_PER_ROUND {
                            let (key, expected) = &hot[(caller + round + iteration) % hot.len()];
                            let start = Instant::now();
                            let found = db.get(ks, key);
                            let end = Instant::now();
                            match found {
                                Ok(Some(found)) if found == expected => samples.push([
                                    start.duration_since(origin).as_nanos() as u64,
                                    end.duration_since(origin).as_nanos() as u64,
                                ]),
                                other => {
                                    failure =
                                        Some(format!("cache-contention value mismatch: {other:?}"));
                                    break;
                                }
                            }
                        }
                    }
                    barrier.wait();
                }
                failure.map_or(Ok(samples), Err)
            }));
        }
        let mut writes = Vec::with_capacity(ROUNDS);
        let mut failure = None;
        for _ in 0..ROUNDS {
            barrier.wait();
            if failure.is_none() {
                let mut batch = db.write_batch();
                batch.write(ks, writer.0.clone(), writer.1.clone());
                let start = Instant::now();
                let result = batch.commit();
                let end = Instant::now();
                match result {
                    Ok(()) => writes.push([
                        start.duration_since(origin).as_nanos() as u64,
                        end.duration_since(origin).as_nanos() as u64,
                    ]),
                    Err(error) => {
                        failure = Some(format!("cache-contention durable write: {error:?}"))
                    }
                }
            }
            barrier.wait();
        }
        let mut samples = Vec::with_capacity(callers);
        for task in tasks {
            samples.push(
                task.join()
                    .map_err(|_| "cache-contention caller panicked".to_owned())??,
            );
        }
        if let Some(failure) = failure {
            return Err(failure);
        }
        Ok((samples, writes, origin.elapsed().as_nanos()))
    })?;
    let overlapping_reads_by_caller: Vec<_> = samples
        .iter()
        .map(|caller| {
            caller
                .iter()
                .enumerate()
                .filter(|(index, read)| {
                    let write = writes[*index / READS_PER_ROUND];
                    read[0] < write[1] && write[0] < read[1]
                })
                .count()
        })
        .collect();
    if overlapping_reads_by_caller.contains(&0) {
        return Err(
            "cache-contention has a caller with no observed read/write API overlap".to_owned(),
        );
    }
    let after = db.remote_counters();
    let delta = match (before, after) {
        (Some((before, _, before_rpcs)), Some((after, _, after_rpcs))) => {
            let hits = after
                .hits
                .checked_sub(before.hits)
                .ok_or("cache hit counter reset")?;
            let misses = after
                .misses
                .checked_sub(before.misses)
                .ok_or("cache miss counter reset")?;
            let rpcs = after_rpcs
                .checked_sub(before_rpcs)
                .ok_or("read RPC counter reset")?;
            if hits != (callers * ROUNDS * READS_PER_ROUND) as u64 || misses != 0 || rpcs != 0 {
                return Err(format!(
                    "hit-only population not established: hits={hits} misses={misses} RPCs={rpcs}"
                ));
            }
            Some((hits, misses, rpcs))
        }
        (None, None) => None,
        _ => return Err("cache counter availability changed".to_owned()),
    };
    if db
        .get(ks, &writer.0)
        .map_err(|e| format!("writer audit: {e:?}"))?
        .as_ref()
        != Some(&writer.1)
    {
        return Err("concurrent writer value audit failed".to_owned());
    }
    Ok(Observation {
        callers,
        reads_per_caller: ROUNDS * READS_PER_ROUND,
        concurrent_durable_writes: ROUNDS,
        phase_wall_ns: wall,
        get_api_intervals_ns_by_caller: samples,
        write_api_intervals_ns: writes,
        overlapping_reads_by_caller,
        remote_cache_counter_available: delta.is_some(),
        remote_hits: delta.map(|d| d.0),
        remote_misses: delta.map(|d| d.1),
        remote_read_rpcs: delta.map(|d| d.2),
        scope: "Sui_adapter_get_API_excludes_value_check;common_process_monotonic_ns;observed_API_overlap_with_disjoint_durable_writes_not_device_overlap;native_cache_hits_unavailable",
    })
}
