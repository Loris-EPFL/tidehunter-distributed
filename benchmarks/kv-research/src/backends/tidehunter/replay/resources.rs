//! Coordinator-only disk bound for a fresh, finite replay; owner WAL files are not here.
use super::*;
use tidehunter::distributed::HYBRID_POSITION_ENCODED_LEN;

// [INFERENCE] The hybrid coordinator keeps the native index stack, so the
// disk bound follows native mechanisms, not a measured image. Each term is an
// upper bound under the stated assumption; none is calibrated on a run.
//
// * Live index: every distinct key owns one flat row (reduced key, the 48-byte
//   encoded `VersionPosition`, one kind byte) in the latest blob of its cell,
//   plus a generous fixed blob header per cell.
// * Superseded index: native rewrites a whole cell blob at every flush. A
//   flush needs `max_dirty_keys` new rows (Sui sets 1,024) or one forced
//   flush per cell at a snapshot, and the run is credited with no snapshot,
//   so all flushes of the run stay on disk. Cells of one table are assumed
//   to hold equal shares of its rows, so a flush rewrites the average blob.
// * Preallocated index WAL fragment: the coordinator opens its index WAL and
//   creates the writer, which maps the writer's fragment and clears its suffix
//   (`WalIterator::into_writer` -> `clear_fragment_from_position`). On the
//   open path that clear covers the whole fragment, so the file's allocation
//   is one `frag_size` however little of it holds frames; native's own writer
//   does the same. Fragments the mapper only extends stay sparse (an unfilled
//   extension is never zero-filled), so one fragment per database is counted.
// * Control region: native keeps two retained generations and writes a third;
//   each holds one fixed-size entry per cell. `REGION_BYTES_PER_CELL` bounds
//   only a cell whose L1 is one blob. With L1 auto-sharding (Sui `TH_AUTOSHARD`,
//   threshold = fragment size / 2) the entry also holds one (position, max
//   key) record per shard. The flusher bisects a blob that is larger than the
//   threshold, so a shard is at least about half the threshold, except for a
//   single-entry shard. The shard count per cell is therefore bounded by
//   `2 * ceil(average_blob / SHARD_BLOB_FLOOR_BYTES)`. `SHARD_BLOB_FLOOR_BYTES`
//   is an assumed floor under half the threshold (any page-aligned fragment of
//   128 KiB or more), not a measured value: the shard bound is an [INFERENCE]
//   and is not calibrated on a run.
//
// Owner value WAL space is enforced on each external owner, not here.
const FLAT_KIND_BYTES: u64 = 1;
const BLOB_HEADER_BYTES: u64 = 1024;
const DIRTY_KEYS_PER_FLUSH: u64 = 1024;
const REGION_BYTES_PER_CELL: u64 = 256;
const SHARD_BLOB_FLOOR_BYTES: u64 = 64 * 1024;
const SHARD_ENTRY_FIXED_BYTES: u64 = 32;
const REGION_GENERATIONS: u64 = 2 + 1;
/// Index WAL fragments the coordinator's open allocates: the writer's own.
const INDEX_WAL_FRAGMENTS: u64 = 1;

fn add(a: u64, b: u64) -> BackendResult<u64> {
    a.checked_add(b)
        .ok_or_else(|| "coordinator replay reservation overflow".into())
}
fn mul(a: u64, b: u64) -> BackendResult<u64> {
    a.checked_mul(b)
        .ok_or_else(|| "coordinator replay reservation overflow".into())
}

/// What one keyspace table contributes to the coordinator index.
struct TableLoad {
    /// Puts of the run, repetitions included.
    puts: u64,
    /// Distinct keys alive at the end.
    keys: u64,
    /// Index cells the keys spread over.
    cells: u64,
    /// Bytes of one reduced key.
    reduced_key_bytes: u64,
}

#[derive(Default)]
struct TableBound {
    live: u64,
    superseded: u64,
    region: u64,
}

impl TableBound {
    fn plus(&self, other: &Self) -> BackendResult<Self> {
        Ok(Self {
            live: add(self.live, other.live)?,
            superseded: add(self.superseded, other.superseded)?,
            region: add(self.region, other.region)?,
        })
    }

    fn total(&self) -> BackendResult<u64> {
        add(add(self.live, self.superseded)?, self.region)
    }
}

fn table_bound(table: &TableLoad) -> BackendResult<TableBound> {
    let row = add(table.reduced_key_bytes, HYBRID_POSITION_ENCODED_LEN as u64)?;
    let row = add(row, FLAT_KIND_BYTES)?;
    let live = add(mul(table.keys, row)?, mul(table.cells, BLOB_HEADER_BYTES)?)?;
    let (superseded, shards_per_cell) = if table.cells == 0 {
        (0, 0)
    } else {
        let average_blob = live.div_ceil(table.cells);
        let flushes = add(table.puts.div_ceil(DIRTY_KEYS_PER_FLUSH), table.cells)?;
        (
            mul(flushes, average_blob)?,
            mul(2, average_blob.div_ceil(SHARD_BLOB_FLOOR_BYTES))?,
        )
    };
    let shard_entry = add(SHARD_ENTRY_FIXED_BYTES, table.reduced_key_bytes)?;
    let region_entry = add(REGION_BYTES_PER_CELL, mul(shards_per_cell, shard_entry)?)?;
    let region = mul(mul(table.cells, region_entry)?, REGION_GENERATIONS)?;
    Ok(TableBound {
        live,
        superseded,
        region,
    })
}

pub(super) fn reservation(
    value: &serde_json::Value,
    trace: &Trace,
    repetitions: u64,
) -> BackendResult<serde_json::Value> {
    let mut databases = Vec::new();
    let mut total = 0;
    for (db, _, opts) in options(value)? {
        let remote = external_config(&opts)?;
        let batch_limits = remote.storage.batch_limits;
        let shape = shape(&trace.tables, db);
        let rows: Vec<_> = trace
            .preload
            .iter()
            .chain(trace.batches.iter().flat_map(|batch| &batch.entries))
            .filter(|row| trace.tables[usize::from(row.table)].db_id == db)
            .collect();
        let mut index = TableBound::default();
        let mut cells = 0;
        for table in trace.tables.iter().filter(|table| table.db_id == db) {
            if !rows.iter().any(|row| row.table == table.id) {
                continue;
            }
            let reduced_key_bytes = if table.key_shape == "digest_reduction_0_16" {
                16
            } else {
                table.key_bytes as u64
            };
            let unique = rows
                .iter()
                .filter(|row| row.table == table.id)
                .map(|row| &row.key)
                .collect::<BTreeSet<_>>()
                .len() as u64;
            let puts = mul(
                rows.iter().filter(|row| row.table == table.id).count() as u64,
                repetitions,
            )?;
            let keys = if table.control {
                unique
            } else {
                mul(unique, repetitions)?
            };
            let table_cells = if matches!(
                table.key_shape.as_str(),
                "sequence_prefix_48" | "epoch_digest_prefix_140" | "epoch_marker_prefix_76"
            ) {
                keys
            } else {
                keys.min(table_mutexes(table) as u64)
            };
            let bound = table_bound(&TableLoad {
                puts,
                keys,
                cells: table_cells,
                reduced_key_bytes,
            })?;
            index = index.plus(&bound)?;
            cells = add(cells, table_cells)?;
        }
        let mut batches = 0;
        let mut encoded = 0;
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
                batch_limits,
            )?;
            batches = add(batches, 1)?;
            encoded = add(encoded, batch.encoded().len() as u64)?;
        }
        batches = mul(batches, repetitions)?;
        encoded = mul(encoded, repetitions)?;
        // Coordinator index WAL geometry of this database: an external
        // document owns its index rows; a fixture override fills the rest from
        // the production default, exactly as `Options::index` is applied.
        let index_frag = match &opts.external_remote_config {
            Some(remote) => remote.index.frag_size,
            None => opts.index.map_or_else(
                || tidehunter::distributed::remote_db::HybridIndexConfig::default().frag_size,
                |geometry| geometry.frag_size,
            ),
        };
        let wal_fragment = mul(INDEX_WAL_FRAGMENTS, index_frag)?;
        let reserve = add(index.total()?, wal_fragment)?;
        total = add(total, reserve)?;
        databases.push(serde_json::json!({"db_id":db,"reservation_bytes":reserve,
            "index_live_bytes":index.live,"index_superseded_bytes":index.superseded,
            "control_region_bytes":index.region,"index_wal_fragment_bytes":wal_fragment,"cells":cells,
            "batches":batches,"encoded_batch_bytes":encoded}));
    }
    Ok(
        serde_json::json!({"reservation_bytes":total,"databases":databases,
        "model":"[INFERENCE] coordinator index: live flat rows + every whole-cell flush of the run (no snapshot credit) + three control-region generations, each entry sized for an L1 split into at most 2 * ceil(average blob / 64 KiB) shards (assumed floor, uncalibrated) + the writer's index WAL fragment, fully allocated by the open's suffix clear (a frag_size each, not sparse); owner WAL allocation is enforced on each external owner"}),
    )
}

// The reservation input comes from a prepared leased configuration.
#[cfg(test)]
#[path = "resources_tests.rs"]
mod tests;
