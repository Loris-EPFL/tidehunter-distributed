//! Offline coordinator audit; remote shard processes remain live for authenticated value reads.
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, error::Error, fs, path::Path, sync::Arc};
use tidehunter::{
    db::Db as NativeDb,
    distributed::{remote_db::RemoteDatabaseConfig, sui::Db},
    key_shape::KeyShape,
    metrics::Metrics,
};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if !(args.len() == 3 && args[0] == "native" || args.len() == 5 && args[0] == "remote") {
        return Err(
            "usage: sui-sharded-state-audit native DB SHAPE | remote DB SHAPE CONFIG NAMESPACE"
                .into(),
        );
    }
    let shape: KeyShape = serde_json::from_slice(&fs::read(&args[2])?)?;
    let db = if args[0] == "native" {
        Db::open_native(
            Path::new(&args[1]),
            shape.clone(),
            Arc::new(
                NativeDb::load_config(Path::new(&args[1])).map_err(|error| format!("{error:?}"))?,
            ),
            Metrics::new_in_enabled(&prometheus::Registry::new(), false),
        )
        .map_err(|error| format!("{error:?}"))?
    } else {
        let config: serde_json::Value = serde_json::from_slice(&fs::read(&args[3])?)?;
        let selected: RemoteDatabaseConfig =
            serde_json::from_value(config["databases"][&args[4]].clone())?;
        Db::open_remote(Path::new(&args[1]), shape.clone(), selected)
            .map_err(|error| format!("{error:?}"))?
    };
    let mut tables = BTreeMap::new();
    for ks in shape.iter_ks() {
        if ks.destroyed() {
            continue;
        }
        let mut records = 0_u64;
        let mut key_bytes = 0_u64;
        let mut value_bytes = 0_u64;
        // Native/remote iteration order may differ for reduced/hash indexes.
        // Fixed 32-byte row hashes are sorted, never full values; duplicates retained.
        let mut hashes = Vec::new();
        for row in db.iterator(ks.id()) {
            let (key, value) = row.map_err(|error| format!("{error:?}"))?;
            records += 1;
            if records > 10_000_000 {
                return Err("audit exceeds ten-million-row bound".into());
            }
            key_bytes = key_bytes
                .checked_add(key.len() as u64)
                .ok_or("key byte overflow")?;
            value_bytes = value_bytes
                .checked_add(value.len() as u64)
                .ok_or("value byte overflow")?;
            let mut hash = Sha256::new();
            hash.update((key.len() as u64).to_be_bytes());
            hash.update(&key);
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(&value);
            hashes.push(<[u8; 32]>::from(hash.finalize()));
        }
        hashes.sort_unstable();
        let mut hash = Sha256::new();
        for row in hashes {
            hash.update(row);
        }
        tables.insert(ks.name().to_owned(), serde_json::json!({"records":records,
            "key_bytes":key_bytes,"value_bytes":value_bytes,"sorted_row_sha256":format!("{:x}",hash.finalize())}));
    }
    if tables.is_empty() {
        return Err("empty schema audit".into());
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({"schema":1,"tables":tables,
        "check":"all rows, counts, byte totals, SHA256 of sorted length-delimited row SHA256; multiplicity retained",
        "scope":"checkpoint/epoch/perpetual selected Tidehunter tables; no skipped errors, RPC sample or trace substitution"}))?
    );
    Ok(())
}
