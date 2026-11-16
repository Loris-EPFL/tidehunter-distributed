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
        self.assertEqual(digest, "f64324a08713b8167fd6216948be8c7b5d44b48643d28537294dc6280cdcf3fa")
        self.assertEqual(digest, manual.FOCUSED_MATRIX_SHA256[selection])
        spec = workflow_input.specification("kv", "tidehunter_nvme_rdma", "focused", selection)
        self.assertEqual(spec["job_count"], 18)
        self.assertEqual(spec["arms"], ["file"])
        self.assertEqual(manual._reviewed_jobs(spec), [job for job in jobs if job["arm"] == "file"])
        for arms in (["file"], ["prewritten"]):
            selected = {**copy.deepcopy(spec), "arms": arms}
            self.assertEqual(len(manual._reviewed_jobs(selected)), 9)
            self.assertTrue(all(job["arm"] == arms[0] for job in manual._reviewed_jobs(selected)))
        with self.assertRaises(ValueError):
            manual._reviewed_jobs({**spec, "arms": ["raw"]})
        with self.assertRaises(ValueError):
            manual._reviewed_jobs({**spec, "arms": ["file", "prewritten"]})

    def test_each_single_arm_fits_two_hours_and_three_gigabyte_export(self):
        spec = workflow_input.specification("kv", "tidehunter_nvme_rdma", "focused",
                                            "nvmeof_p9_write_engine")
        self.assertEqual(manual.reservation_seconds(spec), 7200)
        fixed = (bootstrap.M3_PAIR_SECONDS + manual.PAIR_CLEANUP_SECONDS
                 + manual.EXPORT_RESERVE_SECONDS + manual.PREPARATION_SECONDS
                 + manual.CAMPAIGN_MARGIN_SECONDS)
        campaign = workflow_input.cloud_campaign
        for arm in ("file", "prewritten"):
            jobs = manual._reviewed_jobs({**spec, "arms": [arm]})
            deadlines = sum(job["measurement_sizing"]["case_deadline_seconds"] for job in jobs)
            admission = sum(job["case_kind"] != "mainnet_replay" for job in jobs) * (
                campaign.M3_OWNER_ADMISSION_TIMEOUT_SECONDS + campaign.M3_OWNER_ADMISSION_MARGIN_SECONDS)
            self.assertEqual((deadlines, admission, fixed), (3125, 700, 3300))
            self.assertEqual(deadlines + admission + fixed, 7125)
            self.assertLessEqual(deadlines + admission + fixed, manual.reservation_seconds(spec))
            archive_bytes = sum(job["measurement_sizing"]["expected_archive_bytes"] for job in jobs)
            self.assertEqual(archive_bytes, 2339917824)
            self.assertLessEqual(archive_bytes, 3_000_000_000)

    def test_six_host_pair_has_m3_window(self):
        with patch.object(bootstrap, "_call") as call, patch.object(bootstrap.time, "time", return_value=100):
            bootstrap._fleet_pair(Path("/package"), {"selection": "nvmeof_p9_write_engine"}, 10000)
        self.assertEqual(call.call_args.args[2], bootstrap.M3_PAIR_SECONDS + bootstrap.PAIR_EXIT_SECONDS)

if __name__ == "__main__":
    unittest.main()
