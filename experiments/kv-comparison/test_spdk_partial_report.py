import hashlib
import copy
import json
from pathlib import Path
import tempfile
import unittest

import cloud_campaign


class RejectedSpdkReport(unittest.TestCase):
    def test_failed_counter_admission_retains_error_without_publishing_timings(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bindings = {}
            for boundary in ('start', 'end'):
                name = 'spdk-target-' + boundary + '.json'
                # Retain a rejected raw owner response, not fabricated zero I/O.
                raw = json.dumps({'complete': False, 'error': 'native bdev I/O failure'}).encode()
                (root / name).write_bytes(raw)
                bindings[boundary] = {'path': name, 'sha256': hashlib.sha256(raw).hexdigest()}
            error = 'SPDK owner reported an I/O error'
            receipt = {'job': {'case_id': 'rejected-spdk', 'backend': 'tidehunter_nvme_rdma',
                              'nvmeof_target': 'spdk', 'sealed_dataset': {'source_id': 'source'}},
                       'status': 'failed', 'error': error, 'raw_artifacts_validated': False,
                       'spdk_target_measurement': bindings}
            (root / 'receipt.json').write_text(json.dumps(receipt))
            result = cloud_campaign.summarize_case(root)
            self.assertEqual(result['status'], 'failed')
            self.assertEqual(result['error'], error)
            self.assertEqual(result['spdk_target_measurement'], bindings)
            self.assertIn('metrics_unavailable', result)
            self.assertNotIn('spdk_target_io', result)
            self.assertNotIn('metrics', result)

class SpdkRoundingReport(unittest.TestCase):
    def test_live_legacy_sum_is_bound_to_exact_owner_counters(self):
        counts = [12643, 12599, 12484, 12577, 12623]
        ticks = [1468989914, 1483735056, 1403274658, 1469150342, 1491701142]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bindings = {}
            for boundary in ("start", "end"):
                rows = []
                for index, (count, latency) in enumerate(zip(counts, ticks)):
                    daemon = {"pid": index + 1, "process_start_ticks": 10,
                              "executable": "/bin/nvmf_tgt", "executable_device": 1,
                              "executable_inode": 2, "binary_sha256": "a" * 64,
                              "rpc_socket": "/run/rpc", "rpc_device": 3, "rpc_inode": 4}
                    end = boundary == "end"
                    rows.append({"case_id": "case", "index": index, "target": {
                        "daemon": daemon, "bdevs": [{"name": "wal"}], "subsystems": [],
                        "iostat": {"ticks": 200 if end else 100, "tick_rate": 2000000000,
                                   "bdevs": [{"name": "wal",
                                              "bytes_read": count * 4096 if end else 0,
                                              "num_read_ops": count if end else 0,
                                              "read_latency_ticks": latency if end else 0,
                                              "bytes_written": 0, "num_write_ops": 0,
                                              "write_latency_ticks": 0}]}}})
                name = "spdk-target-" + boundary + ".json"
                raw = json.dumps(rows).encode()
                (root / name).write_bytes(raw)
                bindings[boundary] = {"path": name, "sha256": hashlib.sha256(raw).hexdigest()}
            job = {"nvmeof_target": "spdk", "nvmeof_route": "live"}
            observed = cloud_campaign.spdk_measurement.admit_live(root, bindings)
            legacy = 0.0
            for owner in observed["owners"]:
                legacy += owner["mean_read_completion_us"] * owner["read_ops"]
            legacy /= observed["read_ops"]
            self.assertEqual(legacy, 58.13853663032769)
            receipt = {"spdk_target_measurement": bindings, "spdk_target_io": copy.deepcopy(observed)}
            receipt["spdk_target_io"]["mean_read_completion_us"] = legacy
            self.assertEqual(cloud_campaign.admit_spdk_target_measurement(
                root, job, receipt, completed=True), observed)
            receipt["spdk_target_io"]["read_ops"] += 1
            with self.assertRaisesRegex(ValueError, "differs from its bound raw evidence"):
                cloud_campaign.admit_spdk_target_measurement(root, job, receipt, completed=True)
            receipt["spdk_target_io"]["read_ops"] -= 1
            receipt["spdk_target_io"]["mean_read_completion_us"] += 0.001
            with self.assertRaisesRegex(ValueError, "differs from its bound raw evidence"):
                cloud_campaign.admit_spdk_target_measurement(root, job, receipt, completed=True)



if __name__ == '__main__':
    unittest.main()
