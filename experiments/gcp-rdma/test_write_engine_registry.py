"""Seal binding for file and prewritten write-engine arms."""
import copy
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch

import manual_campaign as manual
import gcp_qualify_bootstrap as bootstrap
import workflow_input


class WriteEngineRegistryTests(unittest.TestCase):
    def test_reviewed_digest_and_arm_projection(self):
        selection = "nvmeof_p9_write_engine"
        jobs = workflow_input.cloud_campaign.make_focused_matrix(selection)
        digest = hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        self.assertEqual(digest, "b6eeea3618832c10917952aa150f6ab2036b172b91e9526582b489fc5c1727bc")
        self.assertEqual(digest, manual.FOCUSED_MATRIX_SHA256[selection])
        spec = workflow_input.specification("kv", "tidehunter_nvme_rdma", "focused", selection)
        self.assertEqual(spec["job_count"], 32)
        self.assertEqual(spec["arms"], ["file", "prewritten"])
        self.assertEqual(manual._reviewed_jobs(spec), jobs)
        for arms in (["file"], ["prewritten"]):
            selected = {**copy.deepcopy(spec), "arms": arms}
            self.assertEqual(len(manual._reviewed_jobs(selected)), 16)
            self.assertTrue(all(job["arm"] == arms[0] for job in manual._reviewed_jobs(selected)))
        with self.assertRaises(ValueError):
            manual._reviewed_jobs({**spec, "arms": ["raw"]})

    def test_six_host_pair_has_m3_window(self):
        with patch.object(bootstrap, "_call") as call, patch.object(bootstrap.time, "time", return_value=100):
            bootstrap._fleet_pair(Path("/package"), {"selection": "nvmeof_p9_write_engine"}, 10000)
        self.assertEqual(call.call_args.args[2], bootstrap.M3_PAIR_SECONDS + bootstrap.PAIR_EXIT_SECONDS)

if __name__ == "__main__":
    unittest.main()
