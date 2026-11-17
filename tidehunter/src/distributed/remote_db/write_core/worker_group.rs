use super::super::{
    AppendByteBudget, Client, LocalDbError, PartitionConfig, ReferenceDirectory, WorkerItem,
};
use super::completion::{AppendCompletion, GroupCompletion, WorkerAccounting};
use super::owner_queue::OwnerQueue;
use crate::distributed::group::{CompleteGroup, GROUP_HEADER_LEN};
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub(in crate::distributed::remote_db) struct WorkerPolicy {
    pub storage: PartitionConfig,
    pub max_pending_batches: usize,
    pub append_occupancy: usize,
    pub append_bytes: AppendByteBudget,
}

impl WorkerPolicy {
    pub(in crate::distributed::remote_db) fn new(
        storage: PartitionConfig,
        max_pending_batches: usize,
        append_occupancy: usize,
    ) -> Result<Self, LocalDbError> {
        let largest_charge = storage
            .group_limits
            .max_encoded_bytes
            .checked_add((storage.group_limits.max_batches as usize + 1) * 64)
            .ok_or_else(|| LocalDbError::new("owner append byte budget overflow"))?;
        let append_bytes = AppendByteBudget::new(
            crate::distributed::tcp::APPEND_SESSION_BYTES,
            64 * 1024,
            largest_charge,
        )
        .map_err(LocalDbError::new)?;
        Ok(Self {
            storage,
            max_pending_batches,
            append_occupancy,
            append_bytes,
        })
    }
}

pub(in crate::distributed::remote_db) enum GroupDrain {
    Ready(WorkerItem),
    Empty,
    CreditBlocked,
}

/// Drain everything ready, without a timer, until the next complete batch would
/// exceed owner occupancy, group/reference geometry or transport byte credit.
/// ALLOCATED gaps stop the owner stream, never reorder its physical versions.
pub(in crate::distributed::remote_db) fn take_ready_group(
    client: &Client,
    queue: &OwnerQueue,
    before_owner_seq: u64,
    policy: &WorkerPolicy,
    accounting: &Arc<WorkerAccounting>,
    completion: &Arc<dyn AppendCompletion>,
) -> Result<GroupDrain, LocalDbError> {
    // Worker-only flow accounting; these atomics publish no payload or cut.
    if accounting.groups.load(Ordering::Relaxed) >= policy.append_occupancy {
        return Ok(GroupDrain::CreditBlocked);
    }
    let outstanding_bytes = accounting.bytes.load(Ordering::Relaxed);
    let limits = policy.storage.group_limits;
    let mut count = 0;
    let mut payload = GROUP_HEADER_LEN;
    let mut records = 0usize;
    let mut charge = 64usize;
    let mut batches = Vec::new();
    let mut sequences = Vec::new();
    let mut invalid = false;
    let mut credit_blocked = false;
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    let live = client
        .live_nvme_configured()
        .then(|| client.live_group_budget());
    #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
    let _ = client;

    for _ in 0..limits.max_batches {
        let next = queue.take_ready_before(before_owner_seq, |batch| {
            let Some(member) = CompleteGroup::encoded_member_len(batch.encoded().len()).ok() else {
                invalid = count == 0;
                return false;
            };
            let Some(next_payload) = payload.checked_add(member) else {
                invalid = count == 0;
                return false;
            };
            let Some(next_records) = records.checked_add(batch.mutations().len()) else {
                invalid = count == 0;
                return false;
            };
            let Some(next_charge) =
                AppendByteBudget::add_batch_charge(charge, batch.encoded().len())
            else {
                invalid = count == 0;
                return false;
            };
            if next_payload > limits.max_encoded_bytes
                || ReferenceDirectory::check_fresh_total_capacity(
                    policy.storage.max_batches.min(1024),
                    policy.storage.max_reference_metadata_bytes,
                    count + 1,
                    next_records,
                )
                .is_err()
            {
                invalid = count == 0;
                return false;
            }
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            if live.is_some_and(|budget| {
                budget
                    .extend(count, payload, std::slice::from_ref(batch))
                    .is_none()
            }) {
                invalid = count == 0;
                return false;
            }
            if !policy.append_bytes.admits(outstanding_bytes, next_charge) {
                credit_blocked = true;
                return false;
            }
            count += 1;
            payload = next_payload;
            records = next_records;
            charge = next_charge;
            true
        });
        let Some((sequence, batch)) = next else {
            break;
        };
        sequences.push(sequence);
        batches.push(batch);
    }
    let Some(first) = batches.first() else {
        if invalid {
            return Err(LocalDbError::new(
                "complete batch cannot fit owner group limits",
            ));
        }
        return Ok(if credit_blocked {
            GroupDrain::CreditBlocked
        } else {
            GroupDrain::Empty
        });
    };
    let epoch = first.version().epoch;
    if batches.iter().any(|batch| batch.version().epoch != epoch) {
        return Err(LocalDbError::new("owner queue crossed writer epochs"));
    }
    let result = GroupCompletion::new(
        policy.storage.partition,
        sequences,
        Arc::clone(completion),
        charge,
        Arc::clone(accounting),
    );
    Ok(GroupDrain::Ready(WorkerItem::Append(
        epoch, batches, result,
    )))
}
