"""Seal binding for file and prewritten write-engine arms."""
import ast
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
    def test_p13_selected_digests_and_three_hour_envelope(self):
        selection = "nvmeof_p13_write_engine"
        spec = workflow_input.specification("kv", "tidehunter_nvme_rdma", "focused", selection)
        self.assertEqual((spec["job_count"], spec["owner_hosts"], spec["arms"]), (22, 2, ["file"]))
        self.assertEqual(spec["hard_deadline_seconds"], 10800)
        self.assertEqual(manual.reservation_quote(spec, spec["machine_type"]), (10800, 2959, 3626))
        fixed = (bootstrap.M3_PAIR_SECONDS + manual.PAIR_CLEANUP_SECONDS
                 + manual.EXPORT_RESERVE_SECONDS + manual.PREPARATION_SECONDS
                 + manual.CAMPAIGN_MARGIN_SECONDS)
        campaign = workflow_input.cloud_campaign
        for arm, digest in (
            ("file", "f67708737e501985f11ccbfcfcd7e7c191e18dbda8ecc4fe8611916862b11301"),
            ("prewritten", "abbd468292ed945e553a5a28aed7da51ca016ba6a6eaa1e919cf8e30f3128e4e"),
        ):
            with self.subTest(arm=arm):
                selected = {**spec, "arms": [arm]}
                jobs = manual._reviewed_jobs(selected)
                self.assertEqual(manual.expected_case_count({**selected, "mode": "focused"}), 11)
                self.assertEqual(manual._digest(manual._json(jobs)), digest)
                self.assertEqual(manual.SELECTED_MATRIX_SHA256[selection][arm], digest)
                deadlines = sum(job["measurement_sizing"]["case_deadline_seconds"] for job in jobs)
                admission = sum(job["case_kind"] != "mainnet_replay" for job in jobs) * (
                    campaign.M3_OWNER_ADMISSION_TIMEOUT_SECONDS + campaign.M3_OWNER_ADMISSION_MARGIN_SECONDS)
                self.assertEqual((deadlines, admission, fixed), (3871, 900, 3300))
                self.assertEqual(deadlines + admission + fixed, 8071)
                self.assertEqual(manual.reservation_seconds(spec) - deadlines - admission - fixed, 2729)
                archive = sum(job["measurement_sizing"]["expected_archive_bytes"] for job in jobs)
                self.assertEqual(archive, 2_895_663_104)
                self.assertLessEqual(archive, 3_000_000_000)
        for arms in (["raw"], ["file", "prewritten"]):
            with self.subTest(arms=arms), self.assertRaises(ValueError):
                manual._reviewed_jobs({**spec, "arms": arms})
        for window in ({"reservation_hours": 2, "hard_deadline_seconds": 7200},
                       {"reservation_hours": 3, "hard_deadline_seconds": manual.FLEET_SECONDS}):
            with self.subTest(window=window), self.assertRaises(ValueError):
                manual._reviewed_jobs({**spec, **window})

    def test_reviewed_digest_and_arm_projection(self):
        selection = "nvmeof_p9_write_engine"
        jobs = workflow_input.cloud_campaign.make_focused_matrix(selection)
        digest = hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        self.assertEqual(digest, "7e9089717674ec2980a652984005bf6c210a52a21d94e3ba393e12ed47b275b9")
        self.assertEqual(digest, manual.FOCUSED_MATRIX_SHA256[selection])
        spec = workflow_input.specification("kv", "tidehunter_nvme_rdma", "focused", selection)
        self.assertEqual(spec["job_count"], 18)
        self.assertEqual(spec["arms"], ["file"])
        self.assertEqual(spec["owner_hosts"], 2)
        self.assertEqual(manual._reviewed_jobs(spec), [job for job in jobs if job["arm"] == "file"])
        for arms in (["file"], ["prewritten"]):
            selected = {**copy.deepcopy(spec), "arms": arms}
            self.assertEqual(len(manual._reviewed_jobs(selected)), 9)
            self.assertTrue(all(job["arm"] == arms[0] for job in manual._reviewed_jobs(selected)))
            # The terminal completion check expects the selected arm's cases, not job_count (18).
            self.assertEqual(manual.expected_case_count({**selected, "mode": "focused"}), 9)
        with self.assertRaises(ValueError):
            manual._reviewed_jobs({**spec, "arms": ["raw"]})
        with self.assertRaises(ValueError):
            manual._reviewed_jobs({**spec, "arms": ["file", "prewritten"]})

    def test_each_single_arm_fits_two_hours_and_three_gigabyte_export(self):
        spec = workflow_input.specification("kv", "tidehunter_nvme_rdma", "focused",
                                            "nvmeof_p9_write_engine")
        self.assertEqual(manual.reservation_seconds(spec), 7200)
        self.assertEqual(manual.reservation_quote(spec, spec["machine_type"]), (7200, 1973, 2418))
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

    def test_three_host_pair_has_m3_window(self):
        for selection in workflow_input.cloud_campaign.WRITE_ENGINE_SELECTIONS:
            with self.subTest(selection=selection), patch.object(bootstrap, "_call") as call, \
                    patch.object(bootstrap.time, "time", return_value=100):
                bootstrap._fleet_pair(Path("/package"), {"selection": selection}, 10000)
                self.assertEqual(call.call_args.args[2], bootstrap.M3_PAIR_SECONDS + bootstrap.PAIR_EXIT_SECONDS)

    def test_focused_bootstrap_requires_one_coordinator_and_two_owners(self):
        members = [{"name": f"run-1-{i}", "id": str(i + 1), "rdma_ip": f"10.0.0.{i + 1}"}
                   for i in range(6)]
        args = ("test-bucket", "run-1", "a" * 64, "2030-01-01T00:00:00Z", "b" * 64, "c" * 64)
        for selection in workflow_input.cloud_campaign.WRITE_ENGINE_SELECTIONS:
            with self.subTest(selection=selection):
                options = {"rdma_device": "irdma0", "machine_type": "z4m-highmem-16",
                           "qualification_profile": "focused", "selection": selection,
                           "live_transports": ["rdma"]}
                script = bootstrap.fleet_startup_script(*args, members=members[:3], **options)
                program = script.split("<<'TIDEHUNTER_BOOTSTRAP'\n", 1)[1].rsplit("\nTIDEHUNTER_BOOTSTRAP", 1)[0]
                compile(program, "<three-host-write-engine-bootstrap>", "exec")
                config = json.loads(ast.literal_eval(program.splitlines()[0].split(" = ", 1)[1]))
                self.assertEqual(len(config["members"]), 3)
                for count in (2, 6):
                    with self.subTest(count=count), self.assertRaisesRegex(ValueError, "two owner hosts"):
                        bootstrap.fleet_startup_script(*args, members=members[:count], **options)

if __name__ == "__main__":
    unittest.main()
