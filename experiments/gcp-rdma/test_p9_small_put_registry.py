"""Exact admission, order and two-hour budget for both diagnostic arms."""
import copy
import json
import math
from pathlib import Path
import unittest
from unittest.mock import patch

import manual_campaign as manual
import gcp_qualify_bootstrap as bootstrap
import qualify_observer_matrix
import risk_order
import workflow_input


class SmallPutFleetTests(unittest.TestCase):
    def specification(self):
        return workflow_input.specification("kv", "tidehunter_nvme_rdma", "focused",
                                            selection="nvmeof_p9_small_put")

    def test_reviewed_digest_topology_and_mutation_refusal(self):
        spec = self.specification()
        jobs = manual._reviewed_jobs(spec)
        self.assertEqual((spec["reservation_hours"], spec["owner_hosts"], spec["machine_type"],
                          spec["job_count"], spec["max_cases"]),
                         (2, 5, "z4m-highmem-16", 7, 7))
        self.assertEqual(manual._digest(manual._json(jobs)), spec["matrix_sha256"])
        self.assertEqual(spec["matrix_sha256"],
                         "e4c667a6080f8cc19efc3b35ba4ed7120d864a7d20c0eca12b998e753095f176")
        for mutation in ({"owner_hosts": 1}, {"owner_hosts": 2}, {"machine_type": "z4m-highmem-32"},
                         {"matrix_sha256": "0" * 64}, {"job_count": 6}):
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                manual._reviewed_jobs({**copy.deepcopy(spec), **mutation})
        proof = qualify_observer_matrix.qualify_jobs(jobs)
        self.assertEqual((proof["planned_cases"], proof["matrix_sha256"]), (7, spec["matrix_sha256"]))
        order = risk_order.generate_order(jobs)
        self.assertEqual(order["case_ids"], [job["case_id"] for job in jobs])

    def test_worst_case_budget_includes_admission_and_full_cleanup_margin(self):
        spec = self.specification()
        jobs = manual._reviewed_jobs(spec)
        deadlines = []
        for job in jobs:
            sizing = job["measurement_sizing"]
            expected = (4096 + job["warmup_operations"] + job["measurement_operations"]) / sizing["rate_calls_per_second"] + 28
            self.assertEqual(sizing["expected_case_seconds"], expected)
            deadline = max(180, math.ceil(6 * expected))
            self.assertEqual(sizing["case_deadline_seconds"], deadline)
            deadlines.append(deadline)
        self.assertEqual(deadlines, [229, 229, 386, 386, 290, 290, 290])
        self.assertEqual(spec["case_deadline_seconds"], max(deadlines))
        campaign = workflow_input.cloud_campaign
        admission = campaign.M3_OWNER_ADMISSION_TIMEOUT_SECONDS + campaign.M3_OWNER_ADMISSION_MARGIN_SECONDS
        worst = (sum(deadlines) + len(jobs) * admission + bootstrap.M3_PAIR_SECONDS
                 + manual.PAIR_CLEANUP_SECONDS + manual.EXPORT_RESERVE_SECONDS
                 + manual.PREPARATION_SECONDS + manual.CAMPAIGN_MARGIN_SECONDS)
        self.assertEqual(worst, 6100)
        self.assertLessEqual(worst, 6120)
        self.assertLessEqual(worst, 0.85 * manual.reservation_seconds(spec))
        self.assertLess(sum(job["measurement_sizing"]["expected_archive_bytes"] for job in jobs), 3_000_000_000)

    def test_pair_uses_the_same_bounded_m3_qualification(self):
        with patch.object(bootstrap.time, "time", return_value=100), patch.object(bootstrap, "_call") as call:
            bootstrap._fleet_pair(Path("/package"), {"selection": "nvmeof_p9_small_put"}, 10000)
        cfg = json.loads(call.call_args.args[0][-3])
        self.assertEqual(cfg["phase_deadline"], 100 + bootstrap.M3_PAIR_SECONDS)
        self.assertEqual(call.call_args.args[2], bootstrap.M3_PAIR_SECONDS + bootstrap.PAIR_EXIT_SECONDS)


if __name__ == "__main__":
    unittest.main()
