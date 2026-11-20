//! Real node DB boundaries; no synthetic root keyspace or cross-DB atomicity.
use super::{Durability, NativeWalGeometry, native_config, parse_options};
use crate::backend::{BackendResult, native_error};
use crate::backends::replay::{ReplayBackend, UnsupportedReplayBackend};
use crate::mainnet_trace::{Row, Table};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tidehunter::db::Db;
use tidehunter::key_shape::{KeyIndexing, KeyShapeBuilder, KeySpace, KeySpaceConfig, KeyType};
use tidehunter::metrics::Metrics;
use tidehunter::minibytes::Bytes;

/// Pin the production adapter defaults, not its debug/test overrides:
/// typed-store/src/tidehunter_util.rs:870-877,1035-1037.
pub const ADAPTER_MUTEXES: usize = 1024;

pub fn table_mutexes(table: &Table) -> usize {
    if table.control {
        return 1;
    }
    match table.db_id {
        // authority_store_tables.rs:224-225,254-415: objects and transaction tables.
        0 if matches!(table.id, 4 | 7 | 8 | 13 | 14) => ADAPTER_MUTEXES * 8,
        0 => ADAPTER_MUTEXES * 2,
        // authority_per_epoch_store.rs:589,693-699.
        2 => ADAPTER_MUTEXES * 4,
        _ => ADAPTER_MUTEXES,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum IndexCell {
    Uniform(u32),
    Prefix([u8; 18]),
}

#[allow(
    clippy::expect_used,
    reason = "the shifted product stays below the cell count bound checked here"
)]
fn index_cell(table: &Table, key: &[u8]) -> IndexCell {
    let prefix_bits: usize = match table.key_shape.as_str() {
        "sequence_prefix_48" => 48,
        "epoch_digest_prefix_140" => 140,
        "epoch_marker_prefix_76" => 76,
        _ => 0,
    };
    if prefix_bits == 0 {
        // key_shape.rs:449-455 and math.rs:4-31; adapter uniform(1).
        let mut prefix = [0; 4];
        let len = key.len().min(4);
        prefix[..len].copy_from_slice(&key[..len]);
        let bucket = (u64::from(u32::from_be_bytes(prefix)) * table_mutexes(table) as u64) >> 32;
        return IndexCell::Uniform(u32::try_from(bucket).expect("at most 8192 cells"));
    }
    let bytes = prefix_bits.div_ceil(8);
    let mut prefix = [0; 18];
    prefix[..bytes].copy_from_slice(&key[..bytes]);
    if !prefix_bits.is_multiple_of(8) {
        prefix[bytes - 1] &= 0xff << (8 - prefix_bits % 8);
    }
    IndexCell::Prefix(prefix)
}

pub struct NativeReplayBackend {
    dbs: Vec<Arc<Db>>,
    tables: Vec<(u16, KeySpace)>,
    durability: Durability,
}
impl NativeReplayBackend {
    pub fn validate(options: &serde_json::Value) -> BackendResult<()> {
        let parsed = parse_options(options)?;
        if parsed.native_shards != 1
            || matches!(parsed.durability, Durability::ValueWalGroupBarrier)
        {
            return Err(Box::new(UnsupportedReplayBackend(
                "requires native_shards=1; grouped barriers are not a replay contract".into(),
            )));
        }
        Ok(())
    }
    pub fn open(options: &serde_json::Value, tables: &[Table]) -> BackendResult<Self> {
        Self::validate(options)?;
        let options = parse_options(options)?;
        let config = Arc::new(native_config(options.native_wal_geometry));
        let mut dbs = Vec::with_capacity(3);
        for (db_id, name) in ["perpetual", "checkpoints", "epoch_1313"]
            .into_iter()
            .enumerate()
        {
            let mut builder = KeyShapeBuilder::new();
            for table in tables.iter().filter(|t| usize::from(t.db_id) == db_id) {
                // Pinned Sui adapter: authority_store_tables.rs:248-421,
                // checkpoints/mod.rs:254-350; epoch store uses digest reduction 0..16.
                // Pruning/relocation filters are intentionally absent for this bounded fixture.
                let indexing = if table.key_shape == "digest_reduction_0_16" {
                    KeyIndexing::key_reduction(32, 0..16)
                } else {
                    KeyIndexing::fixed(table.key_bytes)
                };
                let key_type = match table.key_shape.as_str() {
                    "sequence_prefix_48" => KeyType::from_prefix_bits(48),
                    "epoch_digest_prefix_140" => KeyType::from_prefix_bits(140),
                    "epoch_marker_prefix_76" => KeyType::from_prefix_bits(76),
                    _ => KeyType::uniform(1),
                };
                let mutexes = table_mutexes(table);
                builder.add_key_space_config_indexing(
                    &table.node_name,
                    indexing,
                    mutexes,
                    key_type,
                    KeySpaceConfig::default(),
                );
            }
            let path = options.path.join(name);
            std::fs::create_dir_all(&path)?;
            let registry = prometheus::Registry::new();
            let metrics = Metrics::new_in_enabled(&registry, true);
            dbs.push(
                Db::open_for_benchmark(&path, builder.build(), Arc::clone(&config), metrics)
                    .map_err(native_error)?,
            );
        }
        let mut keyspaces = Vec::with_capacity(tables.len());
        for table in tables {
            keyspaces.push((
                table.db_id,
                dbs[usize::from(table.db_id)].ks(&table.node_name),
            ));
        }
        Ok(Self {
            dbs,
            tables: keyspaces,
            durability: options.durability,
        })
    }
    /// Exact whole-cell upper bound over all planned unique reduced keys. Each LSM
    /// index contains a subset of these keys; controls and duplicate real put passes
    /// do not inflate cardinality. No backend or WAL is opened by this check.
    pub fn validate_geometry<'a>(
        options: &serde_json::Value,
        tables: &[Table],
        rows: impl Iterator<Item = &'a Row>,
    ) -> BackendResult<serde_json::Value> {
        let options = parse_options(options)?;
        let config = native_config(options.native_wal_geometry);
        let layout = tidehunter::WalLayout {
            frag_size: config.frag_size,
            wal_file_size: config.wal_file_size,
            max_maps: config.max_maps,
            direct_io: config.direct_io,
            kind: tidehunter::WalKind::Index,
        };
        let mut keys = BTreeSet::new();
        let mut cells = BTreeMap::<(u16, IndexCell), u64>::new();
        let mut largest_value_frame = 0;
        for row in rows {
            let table = &tables[usize::from(row.table)];
            let value_frame = (row.key.len() as u64)
                .checked_add(row.value.len() as u64)
                .and_then(|n| n.checked_add(12))
                .ok_or("native value frame overflow")?;
            largest_value_frame = largest_value_frame.max(layout.align(value_frame));
            let reduced = if table.key_shape == "digest_reduction_0_16" {
                row.key.slice(..16)
            } else {
                row.key.clone()
            };
            let cell = index_cell(table, &reduced);
            if keys.insert((row.table, reduced)) {
                let count = cells.entry((row.table, cell)).or_default();
                *count = count
                    .checked_add(1)
                    .ok_or("native cell cardinality overflow")?;
            }
        }
        let mut largest = (0, 0, IndexCell::Uniform(0), 0);
        for ((id, cell), count) in cells {
            let table = &tables[usize::from(id)];
            let index_key_bytes = if table.key_shape == "digest_reduction_0_16" {
                16
            } else {
                table.key_bytes
            };
            // LookupHeaderIndex's 128*8-byte header, fixed key + WalPosition(12),
            // WalEntry::Index's two bytes, and CrcFrame's eight bytes.
            let frame = count
                .checked_mul((index_key_bytes + tidehunter::WalPosition::LENGTH) as u64)
                .and_then(|n| {
                    n.checked_add(tidehunter::index::index_format::HEADER_SIZE as u64 + 10)
                })
                .ok_or("native index frame overflow")?;
            let frame = layout.align(frame);
            if frame > largest.0 {
                largest = (frame, id, cell, count);
            }
        }
        if largest.0 > config.frag_size || largest_value_frame > config.frag_size {
            return Err(format!(
                "native replay geometry {}: table {} cell {:?}, {} unique index keys require {} B; largest value frame {} B; fragment {} B",
                options.native_wal_geometry.name(), tables[usize::from(largest.1)].name,
                largest.2, largest.3, largest.0, largest_value_frame, config.frag_size,
            ).into());
        }
        Ok(serde_json::json!({
            "maximum_index_frame_bytes": largest.0,
            "maximum_index_frame_table": tables[usize::from(largest.1)].name,
            "maximum_index_frame_cell": format!("{:?}", largest.2),
            "maximum_cell_index_keys": largest.3,
            "maximum_value_frame_bytes": largest_value_frame,
            "fragment_bytes": config.frag_size,
            "admission": "exact expanded-key routing and unique reduced-key cardinality; whole-cell serialized upper bound"
        }))
    }

    pub fn geometry(options: &serde_json::Value) -> BackendResult<serde_json::Value> {
        let parsed = parse_options(options)?;
        let config = native_config(parsed.native_wal_geometry);
        let table_shapes: Vec<_> = crate::mainnet_trace::tables().iter().map(|table| serde_json::json!({
            "table": table.name, "db": table.db_id, "key_bytes": table.key_bytes,
            "key_shape": table.key_shape, "mutexes": table_mutexes(table),
            "uniform_cells_per_mutex": if matches!(table.key_shape.as_str(), "sequence_prefix_48" | "epoch_digest_prefix_140" | "epoch_marker_prefix_76") { None } else { Some(1) },
        })).collect();
        Ok(
            serde_json::json!({"wal_file_size":config.wal_file_size,"fragment_size":config.frag_size,
            "geometry": match parsed.native_wal_geometry { NativeWalGeometry::Small=>"small", NativeWalGeometry::LargeFiles=>"large_files",NativeWalGeometry::OwnerMatched=>"owner_matched",NativeWalGeometry::NativeDefault=>"native_default"},
            "table_shapes":table_shapes,"adapter_production_base_mutexes":ADAPTER_MUTEXES,
            "native_value_cache_entries_per_keyspace":0,
            "durability":match parsed.durability {Durability::PageCache=>"page_cache",Durability::ValueWalBarrier=>"value_wal_barrier",Durability::ValueWalGroupBarrier=>"value_wal_group_barrier"}}),
        )
    }
}
impl ReplayBackend for NativeReplayBackend {
    fn check_background_health(&self) -> BackendResult<()> {
        for (id, db) in self.dbs.iter().enumerate() {
            if let Err(error) = db.check_background_health() {
                return Err(format!("native replay node DB {id}: {error:?}").into());
            }
        }
        Ok(())
    }
    fn get(&self, table: u16, key: &[u8]) -> BackendResult<Option<Bytes>> {
        let &(db, ks) = self
            .tables
            .get(usize::from(table))
            .ok_or("unknown replay table")?;
        self.dbs[usize::from(db)].get(ks, key).map_err(native_error)
    }
    fn commit(&self, db: u16, entries: &[Row]) -> BackendResult<()> {
        let database = self.dbs.get(usize::from(db)).ok_or("unknown replay DB")?;
        // Validate the whole call before constructing/mutating the engine batch.
        for row in entries {
            let &(row_db, _) = self
                .tables
                .get(usize::from(row.table))
                .ok_or("unknown replay table")?;
            if row_db != db {
                return Err("cross-DB replay batch".into());
            }
        }
        let mut batch = database.write_batch();
        for row in entries {
            batch.write(
                self.tables[usize::from(row.table)].1,
                row.key.clone(),
                row.value.clone(),
            );
        }
        batch.commit().map_err(native_error)?;
        if matches!(self.durability, Durability::ValueWalBarrier) {
            database
                .persist_value_wal_for_benchmark()
                .map_err(native_error)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
