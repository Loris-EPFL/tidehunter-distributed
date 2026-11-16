"""M3 immutable topology subsets and physical qualification ordering."""
import copy
import math
import unittest
from unittest.mock import patch
from pathlib import Path

import manual_campaign as manual
import gcp_qualify_bootstrap as bootstrap
import gcp_runtime as runtime


class MultiOwnerLauncherTests(unittest.TestCase):
    def test_reviewed_subsets_bind_every_case(self):
        for hosts, count in ((1, 8), (2, 8), (5, 8)):
            spec = manual.m3_spec(hosts)
            jobs = manual._reviewed_jobs(spec)
            self.assertEqual(len(jobs), count)
            self.assertTrue(all(manual.workflow_input.cloud_campaign.job_owner_hosts(j) == hosts for j in jobs))
            self.assertEqual(manual._digest(manual._json(jobs)), spec['matrix_sha256'])
            bad = copy.deepcopy(spec)
            bad['matrix_sha256'] = '0' * 64
            with self.assertRaises(ValueError):
                manual._reviewed_jobs(bad)

    def test_each_m3_selection_fits_two_hour_reservation_with_margin(self):
        for hosts in (1, 2, 5):
            spec = manual.m3_spec(hosts)
            spec["reservation_hours"] = 2
            with self.subTest(selection=spec["selection"]):
                jobs = manual._reviewed_jobs(spec)
                deadlines = []
                for job in jobs:
                    sizing = job["measurement_sizing"]
                    expected = (4096 + job["warmup_operations"] + job["measurement_operations"]
                                ) / sizing["rate_calls_per_second"] + 28
                    self.assertEqual(sizing["expected_case_seconds"], expected)
                    deadline = max(180, math.ceil(6 * expected))
                    self.assertEqual(sizing["case_deadline_seconds"], deadline)
                    deadlines.append(deadline)
                self.assertEqual(spec["case_deadline_seconds"], max(deadlines))
                campaign = manual.workflow_input.cloud_campaign
                admission = (campaign.M3_OWNER_ADMISSION_TIMEOUT_SECONDS
                             + campaign.M3_OWNER_ADMISSION_MARGIN_SECONDS)
                worst_seconds = (sum(deadlines) + len(jobs) * admission + bootstrap.M3_PAIR_SECONDS
                                 + manual.PAIR_CLEANUP_SECONDS
                                 + manual.EXPORT_RESERVE_SECONDS + manual.PREPARATION_SECONDS
                                 + 720)  # launcher cleanup (600s) plus terminal receipt slack (120s)
                self.assertLessEqual(worst_seconds, 0.85 * manual.reservation_seconds(spec))
                duration, compute_cents, _ = manual.reservation_quote(spec, spec["machine_type"])
                self.assertEqual(duration, 7200)
                self.assertEqual(compute_cents, math.ceil(
                    manual.MACHINE_RATE_NANODOLLARS[spec["machine_type"]] * (hosts + 1) * 2 / 10**7))

    def test_short_check_owners_do_not_repeat_full_pair(self):
        members = [{'name': 'vm-' + str(i), 'id': str(i + 1), 'rdma_ip': '10.0.0.' + str(i + 1)} for i in range(6)]
        cfg = {'members': members, 'run_id': 'bench-' + '0' * 16, 'selection': 'nvmeof_m3_t2_5'}
        with patch.object(bootstrap, '_fleet_phase', side_effect=lambda deadline, seconds, fn: fn(deadline)), \
             patch.object(bootstrap, '_fleet_nvmet_fix', return_value=None), \
             patch.object(bootstrap, '_bake', return_value={}), \
             patch.object(bootstrap, '_put'), patch.object(bootstrap, '_fleet_pair') as pair, \
             patch.object(bootstrap, '_fleet_cleanup'):
            bootstrap._fleet_guest(cfg, Path('/package'), 10000, object(), 'vm-4', {}, lambda key: '5')
        pair.assert_not_called()

    def test_full_pair_is_time_bounded(self):
        import json
        with patch.object(bootstrap.time, "time", return_value=100), patch.object(bootstrap, "_call") as call:
            bootstrap._fleet_pair(Path("/package"), {"selection": "nvmeof_m3_t2_5"}, 10000)
        cfg = json.loads(call.call_args.args[0][-3])
        self.assertEqual(cfg["phase_deadline"], 100 + bootstrap.M3_PAIR_SECONDS)
        self.assertEqual(call.call_args.args[2], bootstrap.M3_PAIR_SECONDS + bootstrap.PAIR_EXIT_SECONDS)

    def test_runtime_recognizes_tools_live_qualification(self):
        self.assertTrue(runtime.live_mode({'mode': 'focused', 'selection': 'nvmeof_f9_tools'}))


if __name__ == '__main__':
    unittest.main()
