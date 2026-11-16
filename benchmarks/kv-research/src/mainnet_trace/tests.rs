use super::*;
const TRACE: &[u8] = include_bytes!("../../../../experiments/sui-mainnet/fixture/trace.bin");
const PRELOAD: &[u8] = include_bytes!("../../../../experiments/sui-mainnet/fixture/preload.bin");
fn fixture() -> Trace {
    Trace::parse(
        Bytes::copy_from_slice(TRACE),
        Bytes::copy_from_slice(PRELOAD),
    )
    .unwrap()
}
fn offsets(bytes: &[u8]) -> Vec<usize> {
    let mut result = Vec::new();
    let mut p = 64;
    while p < bytes.len() {
        result.push(p);
        p += u32_at(bytes, p).unwrap() as usize;
    }
    result
}
fn reject(trace: Vec<u8>, preload: Vec<u8>) {
    assert!(Trace::parse(Bytes::from(trace), Bytes::from(preload)).is_err());
}
#[test]
fn fixture_views_and_profile_are_exact() {
    let trace = fixture();
    assert_eq!(trace.transactions, 45);
    assert_eq!(trace.preload.len(), 376);
    assert_eq!(
        trace
            .reads
            .iter()
            .flatten()
            .filter(|r| r.producer.is_none())
            .count(),
        507
    );
    assert_eq!(
        trace
            .reads
            .iter()
            .flatten()
            .filter(|r| r.producer.is_some())
            .count(),
        116
    );
    assert_eq!(
        trace.batches.iter().map(|b| b.entries.len()).sum::<usize>(),
        707
    );
    assert_eq!(trace.batches[7].entries.len(), 519);
    let bytes = Bytes::copy_from_slice(TRACE);
    let base = bytes.as_ptr() as usize;
    let records = decode(bytes, false, &tables()).unwrap();
    for record in records {
        if let Record::Put(row) | Record::Read(row, _) = record
            && !row.value.is_empty()
        {
            assert!((base..base + TRACE.len()).contains(&(row.value.as_ptr() as usize)));
        }
    }
}
#[test]
fn rejects_truncation_overflow_format_and_flags() {
    for n in [0, 1, 8, 63, 64, TRACE.len() - 1] {
        reject(TRACE[..n].to_vec(), PRELOAD.to_vec());
    }
    for (offset, value) in [
        (0, 0),
        (8, 2),
        (12, 0),
        (16, 2),
        (20, 18),
        (40, 1),
        (68, 99),
        (69, 1),
        (70, 1),
    ] {
        let mut t = TRACE.to_vec();
        t[offset] = value;
        reject(t, PRELOAD.to_vec());
    }
    for (offset, value) in [(24, u64::MAX), (32, u64::MAX)] {
        let mut t = TRACE.to_vec();
        t[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        reject(t, PRELOAD.to_vec());
    }
    let mut t = TRACE.to_vec();
    t[64..68].copy_from_slice(&u32::MAX.to_le_bytes());
    reject(t, PRELOAD.to_vec());
    let put = offsets(TRACE)
        .into_iter()
        .find(|&p| TRACE[p + 4] == 3)
        .unwrap();
    for delta in [12, 16] {
        let mut t = TRACE.to_vec();
        t[put + delta..put + delta + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        reject(t, PRELOAD.to_vec());
    }
}
#[test]
fn rejects_cross_db_member_counts_ordinals_and_read_bytes() {
    let offsets = offsets(TRACE);
    let batch = *offsets.iter().find(|&&p| TRACE[p + 4] == 2).unwrap();
    let put = *offsets.iter().find(|&&p| TRACE[p + 4] == 3).unwrap();
    let read = *offsets.iter().find(|&&p| TRACE[p + 4] == 4).unwrap();
    for (offset, bytes) in [
        (batch + 8, 0u16.to_le_bytes()),
        (put + 8, 13u16.to_le_bytes()),
        (put + 10, 45u16.to_le_bytes()),
        (read + 12, 4u16.to_le_bytes()),
    ] {
        let mut t = TRACE.to_vec();
        t[offset..offset + 2].copy_from_slice(&bytes);
        reject(t, PRELOAD.to_vec());
    }
    for (offset, value) in [(batch + 12, u32::MAX), (read + 8, 45)] {
        let mut t = TRACE.to_vec();
        t[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
        reject(t, PRELOAD.to_vec());
    }
    for offset in [read + 24, read + 64] {
        let mut t = TRACE.to_vec();
        t[offset] ^= 1;
        reject(t, PRELOAD.to_vec());
    }
    let mut t = TRACE.to_vec();
    t[read + 15] = 1;
    reject(t, PRELOAD.to_vec());
    let mut p = PRELOAD.to_vec();
    p[84] ^= 1;
    reject(TRACE.to_vec(), p);
}
#[test]
fn rejects_forward_cycles_missing_producer_preload_and_reads() {
    let offsets = offsets(TRACE);
    let read = *offsets
        .iter()
        .find(|&&p| TRACE[p + 4] == 4 && TRACE[p + 14] == 1)
        .unwrap();
    let mut t = TRACE.to_vec();
    t[read + 8..read + 12].copy_from_slice(&0u32.to_le_bytes());
    reject(t, PRELOAD.to_vec());
    let mut t = TRACE.to_vec();
    t[read + 24] ^= 1;
    reject(t, PRELOAD.to_vec());
    let mut t = TRACE.to_vec();
    t[read + 14] = 0;
    reject(t, PRELOAD.to_vec());
    let store = *offsets
        .iter()
        .find(|&&p| TRACE[p + 4] == 4 && TRACE[p + 14] == 0)
        .unwrap();
    let mut t = TRACE.to_vec();
    t[store + 14] = 1;
    reject(t, PRELOAD.to_vec());
    let mut t = TRACE.to_vec();
    let keep: Vec<_> = offsets
        .iter()
        .copied()
        .filter(|&p| !(TRACE[p + 4] == 4 && u32_at(TRACE, p + 8).unwrap() == 0))
        .collect();
    t.truncate(64);
    for p in &keep {
        let end = *p + u32_at(TRACE, *p).unwrap() as usize;
        t.extend_from_slice(&TRACE[*p..end]);
    }
    t[24..32].copy_from_slice(&(keep.len() as u64).to_le_bytes());
    let len = (t.len() - 64) as u64;
    t[32..40].copy_from_slice(&len.to_le_bytes());
    reject(t, PRELOAD.to_vec());
}
#[test]
fn manifest_limits_reject_missing_reads_and_oversized_batches() {
    let trace = fixture();
    let mut m: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../experiments/sui-mainnet/fixture/manifest.json"
    ))
    .unwrap();
    trace.check_manifest(&m).unwrap();
    m["batches"][7]["members"] = serde_json::json!(518);
    assert!(trace.check_manifest(&m).is_err());
    m["batches"][7]["members"] = serde_json::json!(519);
    m["store_reads"] = serde_json::json!(506);
    assert!(trace.check_manifest(&m).is_err());
}
#[test]
fn expansion_has_golden_bytes_disjoint_repeats_and_collision_rejection() {
    let mut trace = fixture();
    let keys = expanded_keys(&trace, 2).unwrap();
    let golden = expand_key(
        &trace.tables[13],
        7,
        &Bytes::from((0u8..40).collect::<Vec<_>>()),
    )
    .unwrap();
    assert_eq!(
        golden
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>(),
        "3836beabf565e12cfc988a7e03f6f73c7aac5495a91feca3bb98e3e2989f8cae4577ade75181f815"
    );
    for ((id, key), expanded) in &keys[0] {
        assert_eq!(key.len(), expanded.len());
        if trace.tables[usize::from(*id)].control {
            assert_eq!(expanded, key);
            assert_eq!(expanded, &keys[1][&(*id, key.clone())]);
        } else {
            assert_ne!(expanded, &keys[1][&(*id, key.clone())]);
        }
    }
    // Force the zero-byte singleton through data-key admission: it cannot acquire a
    // distinct identity. The same collision guard handles real hash collisions.
    trace.tables[11].control = false;
    assert!(
        expanded_keys(&trace, 2)
            .unwrap_err()
            .to_string()
            .contains("collision")
    );
}
#[test]
fn manifest_hash_network_format_paths_and_file_mutation_are_rejected() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../experiments/sui-mainnet/fixture");
    let sha = "4264e79e0cbe4a1e36d94468df2f049f988eac57877c9bf2e7e175489dc922fa";
    Trace::load(&root.join("manifest.json"), sha).unwrap();
    assert!(Trace::load(&root.join("manifest.json"), &"0".repeat(64)).is_err());
    let temp = std::env::temp_dir().join(format!("mainnet-parser-{}", std::process::id()));
    std::fs::create_dir_all(&temp).unwrap();
    for entry in std::fs::read_dir(&root).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), temp.join(entry.file_name())).unwrap();
    }
    let original = std::fs::read(root.join("manifest.json")).unwrap();
    for field in ["network", "format_version"] {
        let mut m: serde_json::Value = serde_json::from_slice(&original).unwrap();
        m[field] = serde_json::json!("wrong");
        let mut raw = serde_json::to_vec(&m).unwrap();
        raw.push(b'\n');
        std::fs::write(temp.join("manifest.json"), &raw).unwrap();
        assert!(Trace::load(&temp.join("manifest.json"), &sha256(&raw).unwrap()).is_err());
    }
    std::fs::write(temp.join("manifest.json"), &original).unwrap();
    std::fs::write(temp.join("preload.bin"), b"bad").unwrap();
    assert!(Trace::load(&temp.join("manifest.json"), sha).is_err());
    std::fs::remove_file(temp.join("preload.bin")).unwrap();
    std::os::unix::fs::symlink(root.join("preload.bin"), temp.join("preload.bin")).unwrap();
    assert!(Trace::load(&temp.join("manifest.json"), sha).is_err());
    std::fs::remove_dir_all(temp).unwrap();
}
