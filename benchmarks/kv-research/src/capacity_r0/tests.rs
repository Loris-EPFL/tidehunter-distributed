//! `r0.json` schema: every required run, sample, file and owner field.
use super::*;
use tidehunter::distributed::capacity::{
    Blocker, ExactKey, FileUseState, OwnerGrowth, OwnerRpcTiming, R0File, RolledFile, SampleEvent,
    WorstOwner,
};

fn file(start: u64, eligible: bool) -> R0File {
    R0File {
        partition: 0,
        fencing_generation: 1,
        file_start: start,
        file_end: start + 8,
        device: Some(1),
        inode: Some(2),
        logical_bytes: Some(8),
        allocated_bytes: Some(4096),
        state: FileUseState::Used,
        retained_put_members: 0,
        retained_member_bytes: 0,
        retained_value_bytes: 0,
        retained_batches: Some(0),
        retained_batch_bytes: Some(0),
        affected_cells: 0,
        owner_root_generation: Some(3),
        wal_scan_from: Some(16),
        wal_synced_end: Some(16),
        replay_from: Some(16),
        last_offset: None,
        durable_end: Some(16),
        complete_stream: Some(false),
        validated_through: Some(start + 8),
        last_batch_sequence: None,
        batch_sequence_bound: Some(4),
        read_pins: Some(0),
        export_pins: Some(0),
        blockers: if eligible {
            Vec::new()
        } else {
            vec![Blocker::SelectableRootRef]
        },
        shadow_eligible: eligible,
        newly_shadow_reclaimed_bytes: if eligible { 4096 } else { 0 },
    }
}

fn sample(id: u64, event: SampleEvent) -> R0Sample {
    R0Sample {
        sample_id: id,
        event,
        observed_start_ns: id * 10,
        observed_end_ns: id * 10 + 5,
        complete: true,
        reason: None,
        rolled_files: vec![RolledFile {
            partition: 0,
            fencing_generation: 1,
            file_index: 2,
        }],
        exact_key: Some(ExactKey {
            fencing_generation: 1,
            checkpoint_generation: 7,
            metadata_digest: "ab".repeat(32),
        }),
        cut: None,
        floor: None,
        floor_before_all: true,
        durable_cut: None,
        owner_rpcs: vec![OwnerRpcTiming {
            partition: 0,
            fencing_generation: 1,
            observed_start_ns: 1,
            observed_end_ns: 2,
            pages: 1,
            rows: 2,
            stable: true,
        }],
        files: vec![file(0, true), file(8, false)],
    }
}

fn keys(value: &serde_json::Value) -> Vec<String> {
    let mut keys: Vec<_> = value.as_object().unwrap().keys().cloned().collect();
    keys.sort();
    keys
}

fn sorted(names: &[&str]) -> Vec<String> {
    let mut names: Vec<_> = names.iter().map(|name| (*name).to_owned()).collect();
    names.sort();
    names
}

/// A JSON object parsed in document order that rejects a duplicate key.
/// `serde_json::Value` keeps the last duplicate silently.
struct UniqueObject(Vec<(String, serde_json::Value)>);

impl UniqueObject {
    fn field(&self, name: &str) -> Option<&serde_json::Value> {
        self.0
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value)
    }
}

impl<'de> serde::Deserialize<'de> for UniqueObject {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = UniqueObject;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<UniqueObject, A::Error> {
                let mut seen = std::collections::BTreeSet::new();
                let mut fields = Vec::new();
                while let Some((key, value)) = map.next_entry::<String, serde_json::Value>()? {
                    if !seen.insert(key.clone()) {
                        return Err(serde::de::Error::custom(format!("duplicate key `{key}`")));
                    }
                    fields.push((key, value));
                }
                Ok(UniqueObject(fields))
            }
        }
        deserializer.deserialize_map(Visitor)
    }
}

#[test]
fn r0_json_has_the_design_schema_and_is_complete_only_after_rename() {
    let root = std::env::temp_dir().join(format!(
        "kv-research-r0-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("r0.json");
    let header = R0Header {
        run_id: "case".into(),
        engine_revision: serde_json::json!({"sha256":"00"}),
        workload: R0Workload::SyntheticChurn,
        input_digest: Some("ff".into()),
        durability_tier: serde_json::json!("native"),
        route: serde_json::json!("live_nvmeof"),
        fixture: serde_json::json!("leased"),
        prewrite: None,
    };
    let writer = R0Writer::create(&path, &header).unwrap();
    let mut sink = writer.clone();
    sink.sample(&sample(0, SampleEvent::Begin)).unwrap();
    sink.sample(&sample(1, SampleEvent::End)).unwrap();
    assert!(
        !path.exists(),
        "r0.json must appear only after the final rename"
    );
    let geometry = R0Geometry {
        database: "01".repeat(16),
        owners: vec![0],
        wal_file_bytes: 8,
        fragment_bytes: 4,
        relocation_max_reclaim_pct: 100,
    };
    let summary = R0Summary {
        elapsed_ns: 20,
        sample_count: 2,
        prune_events: 0,
        file_roll_events: 1,
        missed_events: 0,
        value_wal_unlinks: Some(0),
        suppressed_retirement_passes: 0,
        complete: true,
        reasons: Vec::new(),
        per_owner: vec![OwnerGrowth {
            partition: 0,
            baseline_allocated_bytes: 4096,
            end_allocated_bytes: 8192,
            new_allocated_bytes: 4096,
            newly_shadow_reclaimed_bytes: 4096,
            net_allocated_growth_bytes_per_day: Some(0),
            reason: None,
        }],
        worst_owner: WorstOwner {
            partition: Some(0),
            net_allocated_growth_bytes_per_day: Some(0),
            reason: None,
        },
    };
    writer.finish(&geometry, &summary).unwrap();
    assert!(writer.finish(&geometry, &summary).is_err());
    assert!(!path.with_extension("json.partial").exists());
    let bytes = std::fs::read(&path).unwrap();
    // Strict parse: no top-level key may repeat (the sample array and the
    // sample count once shared `samples`).
    assert!(serde_json::from_str::<UniqueObject>(r#"{"a":1,"a":2}"#).is_err());
    let strict: UniqueObject = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        strict
            .field("samples")
            .and_then(serde_json::Value::as_array)
            .map(Vec::len),
        Some(2)
    );
    assert_eq!(strict.field("sample_count"), Some(&serde_json::json!(2)));
    let report: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        keys(&report),
        sorted(&[
            "format_version",
            "run_id",
            "engine_revision",
            "workload",
            "input_digest",
            "database",
            "durability_tier",
            "route",
            "fixture",
            "owners",
            "wal_file_bytes",
            "fragment_bytes",
            "prewrite",
            "relocation_max_reclaim_pct",
            "elapsed_ns",
            "samples",
            "sample_count",
            "prune_events",
            "file_roll_events",
            "missed_events",
            "value_wal_unlinks",
            "suppressed_retirement_passes",
            "complete",
            "reasons",
            "per_owner",
            "worst_owner",
        ])
    );
    assert_eq!(report["format_version"], 1);
    assert_eq!(report["workload"], "synthetic_churn");
    let samples = report["samples"].as_array().unwrap();
    assert_eq!(samples.len(), 2);
    assert_eq!(samples[0]["event"], "begin");
    assert_eq!(
        keys(&samples[0]),
        sorted(&[
            "sample_id",
            "event",
            "observed_start_ns",
            "observed_end_ns",
            "complete",
            "reason",
            "rolled_files",
            "exact_key",
            "cut",
            "floor",
            "floor_before_all",
            "durable_cut",
            "owner_rpcs",
            "files",
        ])
    );
    let file = &samples[0]["files"][1];
    assert_eq!(file["blockers"], serde_json::json!(["selectable_root_ref"]));
    assert_eq!(file["last_batch_sequence"], serde_json::Value::Null);
    assert_eq!(
        keys(file),
        sorted(&[
            "partition",
            "fencing_generation",
            "file_start",
            "file_end",
            "device",
            "inode",
            "logical_bytes",
            "allocated_bytes",
            "state",
            "retained_put_members",
            "retained_member_bytes",
            "retained_value_bytes",
            "retained_batches",
            "retained_batch_bytes",
            "affected_cells",
            "owner_root_generation",
            "wal_scan_from",
            "wal_synced_end",
            "replay_from",
            "last_offset",
            "durable_end",
            "complete_stream",
            "validated_through",
            "last_batch_sequence",
            "batch_sequence_bound",
            "read_pins",
            "export_pins",
            "blockers",
            "shadow_eligible",
            "newly_shadow_reclaimed_bytes",
        ])
    );
    assert_eq!(
        keys(&report["per_owner"][0]),
        sorted(&[
            "partition",
            "baseline_allocated_bytes",
            "end_allocated_bytes",
            "new_allocated_bytes",
            "newly_shadow_reclaimed_bytes",
            "net_allocated_growth_bytes_per_day",
            "reason",
        ])
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn request_defaults_to_the_sui_reclaim_budget() {
    let request: R0Request =
        serde_json::from_value(serde_json::json!({"workload":"sui_mainnet_retention"})).unwrap();
    assert_eq!(request.relocation_max_reclaim_pct, 100);
    assert_eq!(request.workload, R0Workload::SuiMainnetRetention);
    assert!(serde_json::from_value::<R0Request>(serde_json::json!({"workload":"x"})).is_err());
    assert_eq!(
        report_path(Path::new("out"), 2, 3),
        Path::new("out").join("r0-db2.json")
    );
    assert_eq!(
        report_path(Path::new("out"), 0, 1),
        Path::new("out/r0.json")
    );
}
