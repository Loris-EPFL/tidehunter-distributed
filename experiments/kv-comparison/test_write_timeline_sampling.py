"""Explicit timeline stride admission without changing older matrix identities."""
import copy
import json
from pathlib import Path
import unittest

import cloud_campaign as campaign


class WriteTimelineSamplingTests(unittest.TestCase):
    def specification(self):
        spec = json.loads(Path(__file__).with_name("cloud-campaign.json").read_text())
        spec.update(backends=["tidehunter_tcp"], max_cases=2000,
                    diagnostics={"write_timeline_limit": 2})
        return spec

    def test_stride_is_explicit_and_forwarded_to_diagnostic_jobs(self):
        spec = self.specification()
        original = campaign.make_matrix(spec)
        spec["diagnostics"]["write_timeline_sample_every"] = 3
        jobs = campaign.make_matrix(spec)
        timelines = [job for job in jobs if job.get("cohort") == "write_timeline_diagnostics"]
        self.assertTrue(timelines)
        self.assertTrue(all(job["diagnostics"]["write_timeline_sample_every"] == 3 for job in timelines))
        self.assertEqual([job for job in jobs if job not in timelines],
                         [job for job in original if job.get("cohort") != "write_timeline_diagnostics"])
        self.assertTrue(all("write_timeline_sample_every" not in job.get("diagnostics", {})
                            for job in original))

    def test_stride_has_separate_positive_integer_bound(self):
        for value in (0, -1, True, 1.5, "3", 1_000_001):
            spec = copy.deepcopy(self.specification())
            spec["diagnostics"]["write_timeline_sample_every"] = value
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "sample_every"):
                campaign.make_matrix(spec)
        spec = self.specification()
        spec["diagnostics"]["write_timeline_sample_every"] = 1_000_000
        self.assertTrue(campaign.make_matrix(spec))


if __name__ == "__main__":
    unittest.main()
