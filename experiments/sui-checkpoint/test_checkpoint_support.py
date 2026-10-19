"""Behavioral contracts for support extracted from middleware's 1969983 worktree."""

import copy
import hashlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import yaml

import checkpoint_support as support


def index_metrics(checkpoint, names=None):
    return "\n".join(
        f'rpc_store_indexer_watermark_checkpoint_in_db{{pipeline="{name}"}} {checkpoint}'
        for name in sorted(support.RPC_INDEX_PIPELINES if names is None else names)
    )


def local_audit():
    tables = {"default": {"records": 2, "key_bytes": 8, "value_bytes": 16, "digest_xor": [1] * 32}}
    return {"wal_recovery": "AbsoluteConsistency", "tables": tables,
            "semantic_tables": copy.deepcopy(tables), "schema": ["default"]}


class ConfigurationContract(unittest.TestCase):
    def test_checkpoint_bound_round_trips_as_external_yaml_tag(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for value in (0, (1 << 64) - 1):
                with self.subTest(value=value):
                    (root / "node.yaml").write_text(f"run-with-range:\n  Checkpoint: {value}\n")
                    config = support.configuration(root, "node")
                    self.assertIsInstance(config["run-with-range"], support.CheckpointBound)
                    encoded = yaml.dump(config, Dumper=support.ExperimentDumper)
                    self.assertEqual(yaml.safe_load(encoded), {"run-with-range": {"Checkpoint": value}})
            for value in ("true", "-1", str(1 << 64), "1.5", '"12"'):
                with self.subTest(value=value):
                    (root / "node.yaml").write_text(f"run-with-range:\n  Checkpoint: {value}\n")
                    with self.assertRaisesRegex(ValueError, "u64"):
                        support.configuration(root, "node")

    def test_other_configuration_bounds_are_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "node.yaml").write_text("run-with-range:\n  Epoch: 2\nother: true\n")
            self.assertEqual(support.configuration(root, "node"), {"run-with-range": {"Epoch": 2}, "other": True})

    def test_frozen_source_environment_uses_original_defaults_and_explicit_fields(self):
        self.assertEqual(support.source_environment({}), {"TH_FRAG_SIZE": "33554432", "TH_SYNC_WRITES": "false"})
        self.assertEqual(support.source_environment({"fragment_mib": 8, "durable": True}),
                         {"TH_FRAG_SIZE": "8388608", "TH_SYNC_WRITES": "true"})


class RpcContract(unittest.TestCase):
    def test_rpc_server_error_is_not_reported_as_a_success(self):
        with patch.object(support.urllib.request, "urlopen", return_value=io.BytesIO(b'{"error":{"code":-1}}')):
            with self.assertRaisesRegex(RuntimeError, "method"):
                support.rpc("localhost:123", "method")

    def test_source_range_includes_genesis_and_frozen_tip_in_digest_order(self):
        calls = []

        def checkpoint(address, method, params):
            sequence = int(params[0])
            calls.append((address, method, sequence))
            return {"sequenceNumber": str(sequence), "digest": f"digest-{sequence}"}

        with patch.object(support, "rpc", side_effect=checkpoint):
            result = support.verify_source_range("source:1", 3)
        self.assertEqual(sorted(calls), [("source:1", "sui_getCheckpoint", seq) for seq in range(4)])
        self.assertEqual(result["checkpoint_count"], 4)
        self.assertTrue(result["summary_range_verified"])
        self.assertEqual(result["range_sha256"], hashlib.sha256(json.dumps([f"digest-{seq}" for seq in range(4)]).encode()).hexdigest())

    def test_last_retained_source_tip_verifies_all_five_thousand_summaries(self):
        def checkpoint(address, method, params):
            return {"sequenceNumber": params[0], "digest": params[0]}

        with patch.object(support, "rpc", new=checkpoint):
            result = support.verify_source_range("source:1", 4999)
        self.assertEqual(result["checkpoint_count"], 5000)
        self.assertEqual(result["range_sha256"],
                         hashlib.sha256(json.dumps([str(i) for i in range(5000)]).encode()).hexdigest())

    def test_source_tip_rejects_expired_genesis_and_detects_holes(self):
        with patch.object(support, "rpc") as rpc:
            for tip in (-1, 0, 5000, 5001):
                with self.subTest(tip=tip), self.assertRaisesRegex(ValueError, "1..4999"):
                    support.verify_source_range("source:1", tip)
            rpc.assert_not_called()
        with patch.object(support, "rpc", return_value={"sequenceNumber": "9", "digest": "wrong"}):
            with self.assertRaisesRegex(ValueError, "sequence mismatch"):
                support.verify_source_range("source:1", 1)
        with patch.object(support, "rpc", side_effect=RuntimeError("missing checkpoint")):
            with self.assertRaisesRegex(RuntimeError, "missing checkpoint"):
                support.verify_source_range("source:1", 1)

    def test_readiness_retries_only_until_success_or_budget(self):
        child = SimpleNamespace(poll=lambda: None)
        with patch.object(support.time, "monotonic", return_value=0), patch.object(support.time, "sleep") as sleep:
            with patch.object(support, "rpc", side_effect=[OSError("not ready"), RuntimeError("not ready"), "4"]) as rpc:
                self.assertEqual(support.wait_rpc(child, "source:1"), "4")
                self.assertEqual(rpc.call_count, 3)
                self.assertEqual(sleep.call_count, 2)
            with patch.object(support, "rpc", side_effect=OSError("not ready")) as rpc:
                with self.assertRaisesRegex(TimeoutError, "five minutes"):
                    support.wait_rpc(child, "source:1")
                self.assertEqual(rpc.call_count, 1200)
        with patch.object(support, "rpc") as rpc:
            with self.assertRaisesRegex(RuntimeError, "exited"):
                support.wait_rpc(SimpleNamespace(poll=lambda: 7, returncode=7), "source:1")
            rpc.assert_not_called()

    def test_readiness_deadline_also_bounds_retries(self):
        with patch.object(support.time, "monotonic", side_effect=[0, 300]), patch.object(support, "rpc") as rpc:
            with self.assertRaises(TimeoutError):
                support.wait_rpc(SimpleNamespace(poll=lambda: None), "source:1")
            rpc.assert_not_called()


class ProgressContract(unittest.TestCase):
    def test_completed_watermark_and_sample_window(self):
        self.assertEqual(support.completed_checkpoint("ignored 9\nlast_executed_checkpoint 12\n"), 12)
        for metrics in ("", "last_executed_checkpoint -1", "last_executed_checkpoint 1\nlast_executed_checkpoint 2"):
            with self.subTest(metrics=metrics), self.assertRaises(ValueError):
                support.completed_checkpoint(metrics)
        progress = [{"seconds": 1, "completed_checkpoint": 2}, {"seconds": 2, "completed_checkpoint": 2},
                    {"seconds": 7, "completed_checkpoint": 8}]
        result = support.completion_window(progress)
        self.assertEqual(result["seconds"], 6)
        self.assertEqual(result["sampled_checkpoints_per_second"], 1)
        self.assertEqual(result["status"], "sampled completed window")
        self.assertEqual(support.completion_window(progress[:2])["status"], "insufficient completed progress samples")
        for final in ({"seconds": 8, "completed_checkpoint": 1}, {"seconds": 1, "completed_checkpoint": 3}):
            with self.subTest(final=final), self.assertRaises(ValueError):
                support.completion_window([progress[0], final])

    def test_only_ten_distinct_bounded_committed_index_frontiers_are_accepted(self):
        self.assertEqual(len(support.RPC_INDEX_PIPELINES), 10)
        self.assertEqual(support.rpc_index_frontiers(index_metrics(4), 4), dict.fromkeys(support.RPC_INDEX_PIPELINES, 4))
        for metrics in (index_metrics(5), index_metrics(-1), index_metrics(1.5), index_metrics(float("nan")),
                        index_metrics(4) + '\nrpc_store_indexer_watermark_checkpoint_in_db{pipeline="balance"} 4',
                        'rpc_store_indexer_watermark_checkpoint_in_db{pipeline="unknown"} 4',
                        'rpc_store_indexer_watermark_checkpoint_in_db{other="balance"} 4'):
            with self.subTest(metrics=metrics), self.assertRaises(ValueError):
                support.rpc_index_frontiers(metrics, 4)

    def test_index_wait_requires_every_pipeline_at_tip(self):
        partial = index_metrics(4, support.RPC_INDEX_PIPELINES - {"balance"})
        complete = index_metrics(4)
        child = SimpleNamespace(poll=lambda: None)
        with patch.object(support.time, "sleep"), patch.object(support.time, "monotonic", return_value=0):
            with patch.object(support.urllib.request, "urlopen", return_value=io.BytesIO(complete.encode())):
                metrics, evidence = support.wait_for_rpc_indexes(child, "source:1", 4, partial)
            self.assertEqual(metrics, complete)
            self.assertEqual(len(evidence["observations"]), 2)
            self.assertEqual(evidence["status"], "all-ten-committed-at-frozen-tip")
            with patch.object(support.urllib.request, "urlopen", side_effect=lambda *a, **k: io.BytesIO(index_metrics(3).encode())) as opened:
                with self.assertRaisesRegex(RuntimeError, "fixed sample budget"):
                    support.wait_for_rpc_indexes(child, "source:1", 4, index_metrics(3))
                self.assertEqual(opened.call_count, 480)
        with self.assertRaisesRegex(RuntimeError, "did not commit"):
            support.wait_for_rpc_indexes(SimpleNamespace(poll=lambda: 1), "source:1", 4, partial)
        with patch.object(support.time, "monotonic", side_effect=[0, 120, 120]):
            with self.assertRaisesRegex(RuntimeError, "did not commit"):
                support.wait_for_rpc_indexes(child, "source:1", 4, partial)

    def test_replay_phase_bounds_are_not_completion_latency(self):
        text = "\n".join(("2026-10-18T00:00:00+00:00 INFO startup",
                          "2026-10-18T00:00:02+00:00 INFO execute_checkpoint{seq=0}: executing checkpoint",
                          "2026-10-18T00:00:03+00:00 INFO execute_checkpoint{seq=1}: executing checkpoint"))
        result = support.replay_phases(text, 5)
        self.assertEqual(result["after_first_execution_to_exit_upper_seconds"], 3)
        self.assertEqual(result["start_spacing_ms"]["p50"], 1000)
        for elapsed in (2, float("inf"), float("nan")):
            with self.subTest(elapsed=elapsed), self.assertRaises(ValueError):
                support.replay_phases(text, elapsed)
        with self.assertRaisesRegex(ValueError, "timestamps absent"):
            support.replay_phases("no execution logs", 5)


class LocalStoreAuditContract(unittest.TestCase):
    def test_raw_store_comparison_exposes_mismatch(self):
        left, right = local_audit(), local_audit()
        self.assertTrue(support.compare_local_store("committee", left, right)["equal"])
        right["tables"]["default"]["digest_xor"][0] = 2
        right["semantic_tables"] = copy.deepcopy(right["tables"])
        result = support.compare_local_store("committee", left, right)
        self.assertFalse(result["equal"])
        self.assertFalse(result["raw_equal"])

    def test_audit_rejects_weak_recovery_partial_tables_rows_and_normalization(self):
        for side in (0, 1):
            for failure in ("recovery", "table", "row", "normalization"):
                with self.subTest(side=side, failure=failure):
                    pair = [local_audit(), local_audit()]
                    audit = pair[side]
                    if failure == "recovery":
                        audit["wal_recovery"] = "TolerateCorruptedTailRecords"
                    elif failure == "table":
                        audit["semantic_tables"] = {}
                    elif failure == "row":
                        audit["semantic_tables"]["default"]["records"] = 1
                    else:
                        audit["semantic_tables"]["default"]["digest_xor"][0] = 2
                    with self.assertRaises(RuntimeError):
                        support.compare_local_store("committee", *pair)

    def test_only_validated_indexes_can_normalize_sequence_numbers(self):
        left, right = local_audit(), local_audit()
        right["tables"]["default"]["digest_xor"][0] = 2
        for audit in (left, right):
            audit["sequence_digest_bijection"] = True
        result = support.compare_local_store("indexes", left, right)
        self.assertTrue(result["equal"])
        self.assertFalse(result["raw_equal"])
        for side in (left, right):
            side["sequence_digest_bijection"] = False
            with self.assertRaisesRegex(RuntimeError, "bijection"):
                support.compare_local_store("indexes", left, right)
            side["sequence_digest_bijection"] = True
        with self.assertRaisesRegex(RuntimeError, "normalization"):
            support.compare_local_store("nested/indexes", left, right)

    def test_inventory_covers_unknown_stores_but_prunes_exact_audited_engine_roots(self):
        with tempfile.TemporaryDirectory() as tmp:
            live = Path(tmp)
            for name in ("committee", "indexes", "unknown/nested", "store/perpetual/immutable"):
                store = live / name
                store.mkdir(parents=True)
                (store / "CURRENT").touch()
            # Enumerating this subtree would fail the depth guard if not pruned.
            (live / "store/perpetual" / "/".join(["cell"] * 10)).mkdir(parents=True)
            result = support.local_rocksdb_paths(live, ["store/perpetual"])
            self.assertEqual(set(result), {"committee", "indexes", "unknown/nested"})
            self.assertEqual(result["committee"], live / "committee")
            with self.assertRaisesRegex(RuntimeError, "RocksDB"):
                support.local_rocksdb_paths(live, ["committee"])
            for roots in (["../outside"], [str(live)], ["missing"], [f"store-{i}" for i in range(33)]):
                with self.subTest(roots=roots), self.assertRaises(RuntimeError):
                    support.local_rocksdb_paths(live, roots)

    def test_inventory_rejects_symlinks_and_resource_bound_overflow(self):
        with tempfile.TemporaryDirectory() as tmp:
            live = Path(tmp)
            (live / "link").symlink_to(live, target_is_directory=True)
            with self.assertRaisesRegex(RuntimeError, "symlinks"):
                support.local_rocksdb_paths(live)
            with self.assertRaisesRegex(RuntimeError, "unsafe"):
                support.local_rocksdb_paths(live, ["link"])
        with tempfile.TemporaryDirectory() as tmp:
            live = Path(tmp)
            for i in range(33):
                (live / str(i)).mkdir()
                (live / str(i) / "CURRENT").touch()
            with self.assertRaisesRegex(RuntimeError, "32 databases"):
                support.local_rocksdb_paths(live)
        with tempfile.TemporaryDirectory() as tmp:
            live = Path(tmp)
            (live / "/".join(["deep"] * 9)).mkdir(parents=True)
            with self.assertRaisesRegex(RuntimeError, "inventory exceeds bounds"):
                support.local_rocksdb_paths(live)


if __name__ == "__main__":
    unittest.main()
