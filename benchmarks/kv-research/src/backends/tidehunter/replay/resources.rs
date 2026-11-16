//! Coordinator-only disk bound for a fresh, finite replay; owner WAL files are not here.
use super::*;
use tidehunter::distributed::native_index::NativeIndex;
use tidehunter::distributed::storage::HistoryPersistence;
use tidehunter::distributed::types::ValueRecordReference;

fn add(a: u64, b: u64) -> BackendResult<u64> {
    a.checked_add(b)
        .ok_or_else(|| "coordinator replay reservation overflow".into())
}
fn mul(a: u64, b: u64) -> BackendResult<u64> {
    a.checked_mul(b)
        .ok_or_else(|| "coordinator replay reservation overflow".into())
}

pub(super) fn reservation(
    value: &serde_json::Value,
    trace: &Trace,
    repetitions: u64,
) -> BackendResult<serde_json::Value> {
    let mut databases = Vec::new();
    let mut total = 0;
    let mut local_owner_wal = 0;
    for (db, _, opts) in options(value)? {
        let shape = shape(&trace.tables, db);
        let index = NativeIndex::with_shards(&shape, [0; 16], 512 * 1024 * 1024, 1)?;
        let rows: Vec<_> = trace
            .preload
            .iter()
            .chain(trace.batches.iter().flat_map(|batch| &batch.entries))
            .filter(|row| trace.tables[usize::from(row.table)].db_id == db)
            .collect();
        let mut directory = 0;
        let mut cells = 0;
        let mut encoded_rows = 0;
        let mut overlay = 0;
        for table in trace.tables.iter().filter(|table| table.db_id == db) {
            let Some(row) = rows.iter().find(|row| row.table == table.id) else {
                continue;
            };
            let keyspace = shape
                .iter_ks()
                .find(|ks| ks.name() == table.node_name)
                .ok_or("replay table absent from coordinator schema")?
                .id();
            let batch = CompleteBatch::new(
                &shape,
                BatchId {
                    database: [0; 16],
                    operation: [0; 16],
                },
                BatchVersion {
                    epoch: 1,
                    sequence: 0,
                },
                vec![Mutation::Put {
                    keyspace,
                    key: row.key.clone(),
                    value: row.value.clone(),
                }],
                batch_limits(&opts)?,
            )?;
            let reduced = if table.key_shape == "digest_reduction_0_16" {
                16
            } else {
                table.key_bytes as u64
            };
            // native_index.rs VERSION_CHARGE; ask the actual index for its cell-directory charge.
            let version_charge = std::mem::size_of::<ValueRecordReference>() as u64 + 32 + reduced;
            let cell_charge = (index.estimated_batch_bytes(&batch)? as u64)
                .checked_sub(version_charge)
                .ok_or("invalid native index directory charge")?;
            let unique = rows
                .iter()
                .filter(|row| row.table == table.id)
                .map(|row| &row.key)
                .collect::<BTreeSet<_>>()
                .len() as u64;
            let count = mul(
                rows.iter().filter(|row| row.table == table.id).count() as u64,
                repetitions,
            )?;
            let key_count = if table.control {
                unique
            } else {
                mul(unique, repetitions)?
            };
            let cell_count = if matches!(
                table.key_shape.as_str(),
                "sequence_prefix_48" | "epoch_digest_prefix_140" | "epoch_marker_prefix_76"
            ) {
                key_count
            } else {
                key_count.min(table_mutexes(table) as u64)
            };
            directory = add(directory, mul(cell_count, cell_charge)?)?;
            cells = add(cells, cell_count)?;
            // snapshot.rs: u32 offset + u32 key length + key + u32 version count + 134-byte reference.
            encoded_rows = add(encoded_rows, mul(count, 146 + reduced)?)?;
            overlay = add(overlay, mul(count, version_charge)?)?;
        }
        let mut batches = 0;
        let mut encoded = 0;
        let mut maximum_batch_bytes = 0;
        let mut maximum_batch_charge = 0;
        for entries in trace
            .batches
            .iter()
            .filter(|batch| batch.db == db)
            .map(|batch| batch.entries.as_slice())
            .chain(trace.preload.chunks(256).filter(|_| db == 0))
        {
            let mutations = entries
                .iter()
                .map(|row| {
                    let table = &trace.tables[usize::from(row.table)];
                    let keyspace = shape
                        .iter_ks()
                        .find(|ks| ks.name() == table.node_name)
                        .ok_or("replay table absent from coordinator schema")?
                        .id();
                    Ok(Mutation::Put {
                        keyspace,
                        key: row.key.clone(),
                        value: row.value.clone(),
                    })
                })
                .collect::<BackendResult<Vec<_>>>()?;
            let batch = CompleteBatch::new(
                &shape,
                BatchId {
                    database: [0; 16],
                    operation: [0; 16],
                },
                BatchVersion {
                    epoch: 1,
                    sequence: 0,
                },
                mutations,
                batch_limits(&opts)?,
            )?;
            let bytes = batch.encoded().len() as u64;
            // Fresh-cell charges overbound this batch's transient charge, including unpublished routing.
            let charge = index.estimated_batch_bytes(&batch)? as u64;
            batches = add(batches, 1)?;
            encoded = add(encoded, bytes)?;
            maximum_batch_bytes = maximum_batch_bytes.max(bytes);
            maximum_batch_charge = maximum_batch_charge.max(charge);
        }
        batches = mul(batches, repetitions)?;
        encoded = mul(encoded, repetitions)?;
        let remote = opts.external_remote_config.as_ref();
        let replay_limits = opts
            .replay_limits
            .or(remote.map(|config| config.replay_limits))
            .unwrap_or_default();
        let max_index = remote.map_or(128 * 1024 * 1024, |config| config.max_index_bytes) as u64;
        let max_identities = remote.map_or(4096, |config| config.max_admitted_batches) as u64;
        // Sparse publication can return callers while an older hole still blocks
        // capture. Bound the entire suffix by max_pending_batches, not caller count.
        let pending = remote.map_or(128, |config| config.max_pending_batches) as u64;
        // checkpoint_due triggers at replay limits /4, identities /2, or resident index /2.
        // Installing an image unloads captured overlays; only the directory remains resident.
        // Capture can exclude the whole bounded pending suffix. Subtract its maximum
        // byte/charge sizes to bound the progress consumed by each checkpoint trigger.
        // Sum the four counts: different triggers can fire on different checkpoints.
        let batch_window = (replay_limits.max_batches as u64 / 4)
            .max(1)
            .saturating_sub(pending)
            .max(1);
        let byte_window = (replay_limits.max_encoded_bytes as u64 / 4)
            .max(1)
            .saturating_sub(mul(pending, maximum_batch_bytes)?)
            .max(1);
        let identity_window = (max_identities / 2).max(1).saturating_sub(pending).max(1);
        let index_window = (max_index / 2).checked_sub(directory)
            .and_then(|remaining| remaining.checked_sub(pending.checked_mul(maximum_batch_charge)?))
            .filter(|remaining| *remaining > 0)
            .ok_or("distributed replay directory and in-flight index charges exhaust the checkpoint threshold")?;
        let checkpoints = add(
            add(
                batches.div_ceil(batch_window),
                encoded.div_ceil(byte_window),
            )?,
            add(
                batches.div_ceil(identity_window),
                overlay.div_ceil(index_window),
            )?,
        )?
        .min(batches);
        // Strict fresh-run disk bound:
        // (C+2)*I + 2*(128MiB checkpoint metadata + 1MiB CRC authority manifest) + H.
        // I = 114-byte root header + encoded rows + at most one cell per distinct prefix
        // (uniform cells are capped by table_mutexes), each charged 12 bytes for its CRC
        // frame/count and 155 bytes for its root directory entry (21-byte cell identity,
        // 62-byte maximum generation filename, offsets, digest and floor).
        // current/staged/obsolete roots cannot accumulate: cleanup precedes the next capture.
        // Reused cells can keep older containers reachable, so retain ALL C full-image
        // containers, plus two generation working images; never assume only two containers.
        // H=130*256 bytes per appended batch for OnDisk: one receipt slot, one leaf,
        // one new branch and <=127 copied Patricia branches. WalReplay has no such files;
        // its bounded recent receipt window is already in the checkpoint metadata cap.
        let image = add(114, add(encoded_rows, mul(cells, 167)?)?)?;
        let history = if remote.map_or(HistoryPersistence::default(), |config| {
            config.history_persistence
        }) == HistoryPersistence::OnDisk
        {
            mul(batches, 130 * 256)?
        } else {
            0
        };
        let metadata = 2 * (128 * 1024 * 1024 + 1024 * 1024);
        let metadata_reserve = add(add(mul(add(checkpoints, 2)?, image)?, metadata)?, history)?;
        // The loopback harness nests owners under the same root. Only external
        // topologies may exclude their six-whole-file WAL allocation allowance.
        let local_wal = if remote.is_none() {
            mul(
                mul(opts.owner_wal_geometry.wal_file_bytes(), opts.owners as u64)?,
                6,
            )?
        } else {
            0
        };
        local_owner_wal = add(local_owner_wal, local_wal)?;
        let reserve = add(metadata_reserve, local_wal)?;
        total = add(total, reserve)?;
        databases.push(serde_json::json!({"db_id":db,"reservation_bytes":reserve,
            "maximum_checkpoints":checkpoints,"full_index_image_bytes":image,
            "checkpoint_and_manifest_bytes":metadata,"receipt_history_bytes":history,
            "coordinator_metadata_reservation_bytes":metadata_reserve,
            "local_owner_wal_reservation_bytes":local_wal,
            "batches":batches,"encoded_batch_bytes":encoded,"persistent_directory_bytes":directory,
            "resident_overlay_bytes":overlay,"maximum_batch_charge_bytes":maximum_batch_charge,
            "checkpoint_progress_windows":{"batches":batch_window,"encoded_bytes":byte_window,
                "identities":identity_window,"index_bytes":index_window}}));
    }
    Ok(serde_json::json!({"reservation_bytes":total,
        "local_owner_wal_reservation_bytes":local_owner_wal,"databases":databases,
        "model":"coordinator metadata: (maximum checkpoints + 2) * full encoded index image + two capped checkpoint metadata/authority manifests + retained receipt history; six whole owner WAL files additionally charged only for the colocated loopback harness, external owner WAL allocation enforced on each owner"}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloud_perpetual_reservation_counts_coordinator_metadata_not_external_wal() {
        let root = std::env::temp_dir().join(format!(
            "replay-resource-bound-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir(&root).unwrap();
        let input = serde_json::json!({
            "guest_root":root.join("guest"),"addresses":["127.0.0.1:19001","127.0.0.1:19002"],
            "database":vec![7;16],"transport":{"kind":"tcp-trusted"},
            "owner_wal_geometry":"native_default","replay_node_db_id":0,
            "batch_limits":{"max_operations":519,"max_encoded_bytes":500000},
            "max_batches_per_partition":32768,"max_admitted_batches":1000000,"max_index_bytes":512*1024*1024,
            "coordinator_cache_bytes":0,"owner_wal_member_read":"async",
            "owner_read_memory_allowance_bytes":128*1024*1024,"polling":"adaptive",
            "owner_read_resident_first":false,"history_persistence":"wal_replay"
        });
        let input_path = root.join("input.json");
        std::fs::write(&input_path, serde_json::to_vec(&input).unwrap()).unwrap();
        let prepared = root.join("prepared");
        crate::backends::tidehunter::prepare_external_case_file(&input_path, &prepared).unwrap();
        let remote: serde_json::Value =
            serde_json::from_slice(&std::fs::read(prepared.join("remote.json")).unwrap()).unwrap();
        let owner_read: serde_json::Value = serde_json::from_slice(
            &std::fs::read(prepared.join("external_owner_read_config.json")).unwrap(),
        )
        .unwrap();
        let mut value = serde_json::json!({"databases":[{"db_id":0,"options":{
            "transport":{"kind":"tcp-trusted"},"path":root.join("db"),"owners":2,
            "external_remote_config":remote,
            "external_owner_read_config":owner_read,"owner_read_resident_first":false,
            "batch_limits":{"max_operations":519,"max_encoded_bytes":500000}
        }}]});
        let mut trace = Trace::load(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../experiments/sui-mainnet/fixture/manifest.json"),
            "4264e79e0cbe4a1e36d94468df2f049f988eac57877c9bf2e7e175489dc922fa",
        )
        .unwrap();
        trace.select_node_databases(&[0]).unwrap();
        let bound = reservation(&value, &trace, 4096).unwrap();
        let database = &bound["databases"][0];
        assert_eq!(bound["local_owner_wal_reservation_bytes"], 0);
        assert_eq!(database["receipt_history_bytes"], 0);
        assert_eq!(database["batches"], 20480);
        assert_eq!(database["full_index_image_bytes"], 797_710_617_u64);
        assert_eq!(database["maximum_checkpoints"], 219);
        assert_eq!(bound["reservation_bytes"], 176_564_578_965_u64);
        // z4m-highmem-16: one 14,000 GiB local SSD; keep the owner's 50% guard.
        assert!(bound["reservation_bytes"].as_u64().unwrap() < 14000 * 1024_u64.pow(3) / 2);
        eprintln!("cloud-replay-coordinator-bound={bound}");

        value["databases"][0]["options"]["external_remote_config"]["history_persistence"] =
            "on_disk".into();
        let history = reservation(&value, &trace, 4096).unwrap();
        assert_eq!(
            history["databases"][0]["receipt_history_bytes"],
            20480_u64 * 130 * 256
        );

        let local = serde_json::json!({"databases":[{"db_id":0,"options":{
            "transport":{"kind":"tcp-trusted"},"path":root.join("local-db"),"database":vec![7;16],
            "owners":2,"owner_wal_geometry":"research",
            "batch_limits":{"max_operations":519,"max_encoded_bytes":500000}
        }}]});
        let local = reservation(&local, &trace, 2).unwrap();
        assert_eq!(
            local["local_owner_wal_reservation_bytes"],
            12 * 1024_u64.pow(3)
        );
        let database = &local["databases"][0];
        assert_eq!(
            database["reservation_bytes"].as_u64().unwrap(),
            database["coordinator_metadata_reservation_bytes"]
                .as_u64()
                .unwrap()
                + 12 * 1024_u64.pow(3)
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
