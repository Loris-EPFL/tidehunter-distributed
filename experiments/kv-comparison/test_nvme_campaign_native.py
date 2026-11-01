import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import nvme_campaign_native as native


def config():
    return {"schema_version": 1, "backend": {"kind": "native", "options": {"path": "/unused", "key_count": 100, "durability": "page_cache"}},
            "seed": 7, "key_namespace_hex": "01" * 16,
            "dataset": {"keys": 3, "value_size": 64, "entropy": 1.0},
            "workload": {"distribution": "uniform", "mix": {"read": 100, "get_many": 0, "put": 0, "put_batch": 0, "delete": 0, "missing_read": 0}, "batch_size": 32},
            "execution": {"mode": "closed_loop", "concurrency": 1, "queue_capacity": 8, "request_timeout_ms": 1000},
            "phases": {"preload": False, "warmup_operations": 0, "aging_operations": 0, "measurement_operations": 4},
            "qualification_only": False, "measurement_gate": {"ready_file": "/case/ready", "release_file": "/case/release", "timeout_seconds": 10},
            "measurement_completion_gate": {"ready_file": "/case/done", "release_file": "/case/observed", "timeout_seconds": 10},
            "limits": {"max_keys": 100, "max_bytes": 10000, "max_operations": 100, "deadline_seconds": 30, "shutdown_grace_seconds": 2}}


class NativeDatasetTests(unittest.TestCase):
    def test_read_binding_preserves_gates_and_rejects_changed_data_or_writes(self):
        original = config()
        receipt = {"status": "completed", "identity": native._identity(original),
                   "database_path": "/shared/database", "config": copy.deepcopy(original)}
        read = native.read_config(receipt, original)
        self.assertEqual(read["backend"]["options"]["path"], "/shared/database")
        self.assertFalse(read["phases"]["preload"])
        self.assertTrue(read["phases"]["verify_existing_dataset"])
        self.assertEqual(read["measurement_gate"], original["measurement_gate"])
        self.assertEqual(read["measurement_completion_gate"], original["measurement_completion_gate"])
        self.assertNotIn("verify_existing_dataset", original["phases"])
        for mutation in (lambda c: c["dataset"].update(keys=4),
                         lambda c: c.update(seed=9),
                         lambda c: c.update(key_namespace_hex="02" * 16),
                         lambda c: c["workload"]["mix"].update(put=1),
                         lambda c: c["phases"].update(aging_operations=1)):
            changed = copy.deepcopy(original)
            mutation(changed)
            with self.assertRaises(ValueError):
                native.read_config(receipt, changed)

    def test_prepare_retains_receipt_and_is_one_shot(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            binary = base / "binary"
            binary.write_bytes(b"binary")
            root = base / "prepared"
            environment = {"PATH": "/bin", "EXAMPLE": "admitted"}
            def run(argv, log, deadline, supplied_environment):
                self.assertGreater(deadline, 0)
                self.assertIs(supplied_environment, environment)
                supplied = json.loads((root / "config.json").read_text())
                if "--validate-config" in argv:
                    log.write_text(json.dumps({"admitted_config": supplied}))
                else:
                    result = root / "result"
                    result.mkdir()
                    (result / "manifest.json").write_text(json.dumps({"schema_version": 1, "raw_samples_complete": True, "backend_info": {}}))
                    (result / "summary.json").write_text(json.dumps({"status": "completed", "backend": {}}))
                    (result / "samples.csv").write_text("raw evidence")
            with mock.patch.object(native, "_run", side_effect=run), \
                 mock.patch.object(native.analyze, "verify_normalization"), \
                 mock.patch.object(native.analyze, "read_samples", return_value=iter([{"outcome": "success"}])), \
                 mock.patch.object(native.campaign, "verify_run_identity"), \
                 mock.patch.object(native.campaign, "verify_timing_result", return_value={"preload": 3, "measurement": 1}):
                receipt = native.prepare_native(binary, config(), root, 30, environment=environment)
            self.assertEqual(receipt["status"], "completed")
            self.assertEqual(json.loads((root / "receipt.json").read_text()), receipt)
            self.assertIn("samples.csv", receipt["artifacts_sha256"])
            self.assertIsNone(receipt["config"]["measurement_gate"])
            self.assertEqual(receipt["config"]["workload"]["mix"]["get_many"], 0)
            with self.assertRaises(ValueError):
                native.prepare_native(binary, config(), root, 30)

    def test_run_passes_environment_to_subprocess(self):
        with tempfile.TemporaryDirectory() as tmp:
            environment = {"PATH": "/bin", "EXAMPLE": "admitted"}
            with mock.patch.object(native.subprocess, "Popen") as popen:
                popen.return_value.wait.return_value = 0
                native._run(["binary"], Path(tmp) / "run.log",
                            native.time.monotonic() + 30, environment)
            self.assertIs(popen.call_args.kwargs["env"], environment)

    def test_failure_retains_receipt_and_never_admits_reads(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            binary = base / "binary"
            binary.write_bytes(b"binary")
            root = base / "failed"
            with mock.patch.object(native, "_run", side_effect=TimeoutError("deadline")):
                with self.assertRaises(TimeoutError):
                    native.prepare_native(binary, config(), root, 1)
            receipt = json.loads((root / "receipt.json").read_text())
            self.assertEqual(receipt["status"], "failed")
            self.assertIn("deadline", receipt["error"])
            with self.assertRaises(ValueError):
                native.read_config(receipt, config())


if __name__ == "__main__":
    unittest.main()
