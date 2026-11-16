import csv
import json
from pathlib import Path
import tempfile
import unittest

import mainnet_replay_fdb_rdma as replay
import mainnet_replay_resp as resp


class FdbLibvalkeyReplayTests(unittest.TestCase):
    def test_paired_tcp_options_select_existing_client(self):
        state = {"backend": "valkey_tcp", "backend_options": {"cluster": True, "nodes": ["127.0.0.1:6379"]}}
        self.assertEqual(replay.options(state)["client"], "libvalkey")
        self.assertNotIn("client", state["backend_options"])
        fdb = replay.options({"backend": "foundationdb", "directory": "/owned"})
        self.assertEqual(fdb["cluster_file"], "/owned/fdb.cluster")
        self.assertEqual(fdb["required_replication_factor"], 1)

    def test_admission_checks_full_population_and_physical_chunk_counters(self):
        after = {"physical_sets": 1418, "chunk_count": 4, "metadata_bytes": 26,
                 "physical_clear_operations": 2828, "transactions_acknowledged": 18}
        profile = {"functional_qualification": True,
                   "final_verification": {"full_values": True, "fresh_empty_database_admitted": True,
                                          "empty_marker_table_verified": True,
                                          "table_rows": replay.TABLE_ROWS,
                                          "enumerated_physical_rows": 1986, "verified_rows": 1982},
                   "physical": {"before_measurement": dict.fromkeys(after, 0), "after_measurement": after},
                   "replay_contract": {"value_layout": "fdb_chunked_values_v1"},
                   "measurement_elapsed_seconds": 1}
        summary = {"status": "completed", "phase_counts": {"counts": resp.EXPECTED.copy(),
                   "unknown_outcomes": 0, "unresolved_workers": 0, "profile": profile}}
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            with (output / "samples.csv").open("w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(["operation", "outcome"])
                writer.writerows([("get", "success")] * 1014 + [("commit", "success")] * 18)
            (output / "summary.json").write_text(json.dumps(summary))
            self.assertEqual(replay.check_result(output, "foundationdb")["samples"], 1032)
            for field in after:
                changed = json.loads(json.dumps(summary))
                changed["phase_counts"]["profile"]["physical"]["after_measurement"][field] -= 1
                (output / "summary.json").write_text(json.dumps(changed))
                with self.subTest(field=field), self.assertRaisesRegex(RuntimeError, "physical counter"):
                    replay.check_result(output, "foundationdb")
            changed = json.loads(json.dumps(summary))
            changed["phase_counts"]["profile"]["final_verification"]["enumerated_physical_rows"] = 1982
            (output / "summary.json").write_text(json.dumps(changed))
            with self.assertRaisesRegex(RuntimeError, "physical row population"):
                replay.check_result(output, "foundationdb")


if __name__ == "__main__":
    unittest.main()
