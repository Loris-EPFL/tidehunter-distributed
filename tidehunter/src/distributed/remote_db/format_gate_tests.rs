//! The authority manifest format gate (I9). Format 10 is the native-index
//! hybrid. Manifests of earlier formats are refused before any fence, the file
//! is never rewritten, and the Exact-era formats 6 to 9 are named as a removed mode.
//! The engine-level cases (reopen of a refused and of a current manifest) are
//! the last test of this file.

use super::pipeline_tests::Fixture;
use super::tests::{checkpoint_put, reopen_result};
use super::*;
use std::fs;
use std::path::PathBuf;

/// The pre-L1 authority manifest (format 4), frozen at base revision e3b920d.
const MANIFEST_4: &[u8] = include_bytes!("fixtures/manifest-format4.bin");

/// The authority manifest of a real Exact-era database: format 9 with a
/// committed `checkpoint`, written by the Exact code at revision `ff2d3211`
/// after one batch and `flush_index`. SHA-256
/// `23af5f3f4599b3858039d5c8effff11a34f2266d378730cf380df8271a6140f5`. The
/// bytes are not edited. They came from an ignored emitter test in a scratch
/// copy of that revision (not shipped; see the commit message for the command).
const MANIFEST_9: &[u8] = include_bytes!("fixtures/manifest-format9-exact.bin");

fn install(bytes: &[u8]) -> (tempdir::TempDir, PathBuf) {
    let temp = tempdir::TempDir::new("remote-format-gate").unwrap();
    let path = temp.path().join(MANIFEST);
    fs::write(&path, bytes).unwrap();
    (temp, path)
}

#[test]
fn pre_generation_manifest_format_4_is_rejected_unchanged() {
    let (_temp, path) = install(MANIFEST_4);
    let error = fencing_generations::read_authority_manifest(&path).unwrap_err();
    assert!(!error.is_exact_era_manifest());
    assert_eq!(
        error.to_string(),
        format!(
            "unsupported distributed authority manifest format 4 (expected {})",
            fencing_generations::AUTHORITY_FORMAT
        )
    );
    assert_eq!(fs::read(&path).unwrap(), MANIFEST_4);
}

/// The manifest of `MANIFEST_9` with only its `format` field changed. The
/// archive branch wrote formats 6, 7 and 8 with the same fields that this
/// reader probes; no real manifest of those formats exists in the repository.
fn with_format(format: u32) -> Vec<u8> {
    let (_temp, path) = install(MANIFEST_9);
    let mut exact: serde_yaml::Value = read_metadata(&path).unwrap();
    exact
        .as_mapping_mut()
        .unwrap()
        .insert("format".into(), format.into());
    encode_metadata(&exact).unwrap()
}

/// A manifest of the Exact era carries a committed `checkpoint` that this
/// engine cannot read. Opening it would ignore that checkpoint, so the format
/// gate must refuse every such manifest by name. With the format constant left
/// at 9, or with the Exact-era arm removed, the probe accepts or mislabels it
/// and this test fails.
#[test]
fn exact_era_format_9_manifest_with_a_checkpoint_is_refused_by_name_and_unchanged() {
    let (_temp, path) = install(MANIFEST_9);
    let exact: serde_yaml::Value = read_metadata(&path).unwrap();
    assert_eq!(exact["format"].as_u64(), Some(9));
    assert!(
        !exact["checkpoint"].is_null(),
        "the fixture must carry a committed checkpoint"
    );
    let error = fencing_generations::read_authority_manifest(&path).unwrap_err();
    assert!(error.is_exact_era_manifest());
    let message = error.to_string();
    assert!(
        message.starts_with("unsupported legacy distributed mode exact_index"),
        "{message}"
    );
    assert!(message.contains("format 9"), "{message}");
    assert!(message.ends_with("use archive/multi-design"), "{message}");
    assert_eq!(fs::read(&path).unwrap(), MANIFEST_9);
}

/// Formats 6, 7 and 8 are Exact-era formats too (format 9 is the real
/// fixture above); they get the same refusal with the archive pointer.
#[test]
fn exact_era_formats_6_7_and_8_are_refused_by_name_and_unchanged() {
    for format in 6..=8u32 {
        let bytes = with_format(format);
        let (_temp, path) = install(&bytes);
        let error = fencing_generations::read_authority_manifest(&path).unwrap_err();
        assert!(error.is_exact_era_manifest());
        let message = error.to_string();
        assert!(
            message.starts_with("unsupported legacy distributed mode exact_index"),
            "{message}"
        );
        assert!(message.contains(&format!("format {format})")), "{message}");
        assert!(message.ends_with("use archive/multi-design"), "{message}");
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
}

/// Format 5 is the removed per-cell mode, named by its own sentence. Formats
/// 4 (the fixture above) and 11 get the generic refusal without the pointer.
#[test]
fn format_5_is_per_cell_and_formats_below_5_and_above_10_stay_generic() {
    let bytes = with_format(5);
    let (_temp, path) = install(&bytes);
    let error = fencing_generations::read_authority_manifest(&path).unwrap_err();
    assert!(!error.is_exact_era_manifest(), "format 5 is per_cell");
    let message = error.to_string();
    assert!(
        message.starts_with("unsupported legacy distributed mode per_cell"),
        "{message}"
    );
    assert!(message.ends_with("use archive/multi-design"), "{message}");
    for format in [3u32, 11, 12] {
        let bytes = with_format(format);
        let (_temp, path) = install(&bytes);
        let error = fencing_generations::read_authority_manifest(&path).unwrap_err();
        assert!(!error.is_exact_era_manifest());
        let message = error.to_string();
        assert_eq!(
            message,
            format!(
                "unsupported distributed authority manifest format {format} (expected {})",
                fencing_generations::AUTHORITY_FORMAT
            )
        );
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
}

/// The hybrid writes format 10. A format-9 manifest of the Exact era names a
/// committed `checkpoint` that this engine cannot read; the open refuses it by
/// name before any fence and leaves the file as it was. A newer format is
/// refused with the generic message, as an older binary refuses format 10. The
/// hybrid's own manifest reopens.
#[test]
fn exact_era_manifest_with_a_committed_checkpoint_is_refused_and_format_10_reopens() {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 7)])
        .unwrap();
    db.shutdown().unwrap();
    drop(db);
    let root = fixture.temp.path().join("authority");
    let current: serde_yaml::Value = read_metadata(&root.join(MANIFEST)).unwrap();
    assert_eq!(current["format"].as_u64(), Some(10));
    fs::write(root.join(MANIFEST), MANIFEST_9).unwrap();
    let before = fs::read(root.join(MANIFEST)).unwrap();
    let error = reopen_result(&fixture).err().unwrap();
    assert!(error.is_exact_era_manifest());
    let message = error.to_string();
    assert!(
        message.starts_with("unsupported legacy distributed mode exact_index"),
        "{message}"
    );
    assert_eq!(fs::read(root.join(MANIFEST)).unwrap(), before);
    let mut newer = current.clone();
    newer
        .as_mapping_mut()
        .unwrap()
        .insert("format".into(), 11u32.into());
    install_metadata(&root, MANIFEST, &encode_metadata(&newer).unwrap()).unwrap();
    let error = reopen_result(&fixture).err().unwrap();
    assert!(!error.is_exact_era_manifest());
    let message = error.to_string();
    assert!(
        message.starts_with("unsupported distributed authority manifest format 11"),
        "{message}"
    );
    for (index, node) in fixture.nodes.iter().enumerate() {
        // Fencing generation 1 only: no refused open reached an owner.
        assert_eq!(
            node.status().unwrap().highest_fenced_generation,
            1,
            "owner {index} was fenced by a refused open"
        );
    }
    install_metadata(&root, MANIFEST, &encode_metadata(&current).unwrap()).unwrap();
    let mut db = fixture.open();
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[7; 32]
    );
    db.shutdown().unwrap();
}
