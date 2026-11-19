//! Per-owner allocated growth after shadow dead-only retirement (design §2).
//!
//! `new_allocated_bytes` counts each genuinely new file allocation and each
//! later positive `st_blocks` growth of a tracked inode once (high-water above
//! its baseline). A file's allocation is credited as shadow reclaimed once, at
//! its first fully qualified eligible sample, even if the file stays on disk.

use super::{OwnerGrowth, WorstOwner};
use std::collections::{BTreeMap, BTreeSet};

/// One observed file, as the ledger needs it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LedgerFile {
    pub partition: u32,
    pub generation: u64,
    pub start: u64,
    pub device: u64,
    pub inode: u64,
    pub allocated: u64,
    pub eligible: bool,
    pub retained_puts: u64,
}

#[derive(Clone, Copy, Debug)]
struct Track {
    device: u64,
    inode: u64,
    baseline: u64,
    high_water: u64,
    credited: Option<u64>,
}

#[derive(Default)]
struct OwnerLedger {
    files: BTreeMap<(u64, u64), Track>,
    baseline: u64,
    end: Option<u64>,
}

/// Run-wide growth ledger, owned by the observer thread.
#[derive(Default)]
pub(crate) struct GrowthLedger {
    owners: BTreeMap<u32, OwnerLedger>,
    baseline_taken: bool,
    errors: Vec<String>,
}

impl GrowthLedger {
    /// Record one sample. `owners` are the partitions the sample inventoried.
    /// Returns the newly shadow reclaimed bytes of each input file, in order.
    /// Only a complete sample can establish the baseline or credit a file.
    pub(crate) fn observe(
        &mut self,
        owners: &[u32],
        files: &[LedgerFile],
        complete: bool,
        end: bool,
    ) -> Vec<u64> {
        let mut reclaimed = vec![0; files.len()];
        if !self.baseline_taken {
            self.baseline_taken = true;
            if !complete {
                self.errors
                    .push("initial owner inventory incomplete; no baseline".into());
            }
            for &partition in owners {
                self.owners.entry(partition).or_default();
            }
            for file in files {
                let owner = self.owners.entry(file.partition).or_default();
                owner.baseline += file.allocated;
                owner.files.insert(
                    (file.generation, file.start),
                    Track {
                        device: file.device,
                        inode: file.inode,
                        baseline: file.allocated,
                        high_water: file.allocated,
                        credited: None,
                    },
                );
            }
        }
        let mut present: BTreeMap<u32, BTreeSet<(u64, u64)>> = BTreeMap::new();
        for (index, file) in files.iter().enumerate() {
            present
                .entry(file.partition)
                .or_default()
                .insert((file.generation, file.start));
            let owner = self.owners.entry(file.partition).or_default();
            let track = owner
                .files
                .entry((file.generation, file.start))
                .or_insert(Track {
                    device: file.device,
                    inode: file.inode,
                    baseline: 0,
                    high_water: file.allocated,
                    credited: None,
                });
            if (track.device, track.inode) != (file.device, file.inode) {
                self.errors.push(format!(
                    "partition {} generation {} file {} changed inode",
                    file.partition, file.generation, file.start
                ));
                continue;
            }
            track.high_water = track.high_water.max(file.allocated);
            match track.credited {
                Some(_) if file.retained_puts != 0 => self.errors.push(format!(
                    "partition {} generation {} file {} referenced after its shadow retirement",
                    file.partition, file.generation, file.start
                )),
                None if complete && file.eligible => {
                    track.credited = Some(file.allocated);
                    reclaimed[index] = file.allocated;
                }
                _ => {}
            }
        }
        if complete {
            for &partition in owners {
                let Some(owner) = self.owners.get(&partition) else {
                    continue;
                };
                let seen = present.remove(&partition).unwrap_or_default();
                let generations: BTreeSet<u64> =
                    seen.iter().map(|(generation, _)| *generation).collect();
                // A file of an inventoried stream vanished in a no-unlink run.
                if let Some((generation, start)) = owner
                    .files
                    .keys()
                    .find(|key| generations.contains(&key.0) && !seen.contains(key))
                {
                    self.errors.push(format!(
                        "partition {partition} generation {generation} file {start} missing in a no-unlink run"
                    ));
                }
            }
        }
        if end {
            for &partition in owners {
                let total = files
                    .iter()
                    .filter(|file| file.partition == partition)
                    .map(|file| file.allocated)
                    .sum();
                self.owners.entry(partition).or_default().end = Some(total);
            }
        }
        reclaimed
    }

    pub(crate) fn errors(&self) -> &[String] {
        &self.errors
    }

    /// Per-owner totals and slopes. A slope is `null` unless the run and the
    /// ledger are complete and the window is nonempty.
    pub(crate) fn finish(
        &self,
        elapsed_ns: u64,
        run_complete: bool,
    ) -> (Vec<OwnerGrowth>, WorstOwner) {
        let ledger_reason = self.errors.first().cloned();
        let owners: Vec<OwnerGrowth> = self
            .owners
            .iter()
            .map(|(&partition, owner)| {
                let new: u64 = owner
                    .files
                    .values()
                    .map(|track| track.high_water - track.baseline.min(track.high_water))
                    .sum();
                let reclaimed: u64 = owner
                    .files
                    .values()
                    .filter_map(|track| track.credited)
                    .sum();
                let reason = if let Some(reason) = &ledger_reason {
                    Some(reason.clone())
                } else if !run_complete {
                    Some("run incomplete".into())
                } else if owner.end.is_none() {
                    Some("no end inventory".into())
                } else if elapsed_ns == 0 {
                    Some("empty measurement window".into())
                } else {
                    None
                };
                let slope = reason.is_none().then(|| {
                    let net = i128::from(new) - i128::from(reclaimed);
                    let slope = net * 86_400_000_000_000 / i128::from(elapsed_ns);
                    i64::try_from(slope).ok()
                });
                let (slope, reason) = match slope {
                    Some(Some(slope)) => (Some(slope), None),
                    Some(None) => (None, Some("slope overflow".into())),
                    None => (None, reason),
                };
                OwnerGrowth {
                    partition,
                    baseline_allocated_bytes: owner.baseline,
                    end_allocated_bytes: owner.end.unwrap_or(0),
                    new_allocated_bytes: new,
                    newly_shadow_reclaimed_bytes: reclaimed,
                    net_allocated_growth_bytes_per_day: slope,
                    reason,
                }
            })
            .collect();
        let worst = if owners.is_empty() {
            WorstOwner {
                partition: None,
                net_allocated_growth_bytes_per_day: None,
                reason: Some("no owner observed".into()),
            }
        } else if let Some(owner) = owners
            .iter()
            .find(|owner| owner.net_allocated_growth_bytes_per_day.is_none())
        {
            WorstOwner {
                partition: None,
                net_allocated_growth_bytes_per_day: None,
                reason: Some(format!(
                    "partition {}: {}",
                    owner.partition,
                    owner.reason.as_deref().unwrap_or("slope unavailable")
                )),
            }
        } else {
            // `owners` ascends by partition; a strict comparison keeps the
            // lowest partition among equal slopes.
            let mut worst = &owners[0];
            for owner in &owners[1..] {
                if owner.net_allocated_growth_bytes_per_day
                    > worst.net_allocated_growth_bytes_per_day
                {
                    worst = owner;
                }
            }
            WorstOwner {
                partition: Some(worst.partition),
                net_allocated_growth_bytes_per_day: worst.net_allocated_growth_bytes_per_day,
                reason: None,
            }
        };
        (owners, worst)
    }
}
