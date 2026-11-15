import copy
import hashlib
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'kv-comparison'))
import cloud_campaign
import fleet_tools
import manual_campaign
import qualify_observer_matrix
import risk_order
import workflow_input


class FleetToolsRegistryTests(unittest.TestCase):
    def test_exact_registered_spec_and_digest(self):
        spec = manual_campaign.REVIEWED_SPECS['nvmeof_f9_tools']
        jobs = manual_campaign._reviewed_jobs(spec)
        self.assertEqual(len(jobs), 34)
        self.assertEqual(spec['owner_hosts'], 1)
        self.assertEqual(spec['machine_type'], 'z4m-highmem-16')
        self.assertEqual(spec['repeats'], 1)
        self.assertFalse(spec['spdk_comparison'])
        digest = hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        self.assertEqual(spec['matrix_sha256'], digest)
        bad = copy.deepcopy(spec); bad['tool_parameters'] = {'repeats': 3}
        with self.assertRaises(ValueError):
            manual_campaign._reviewed_jobs(bad)
        bad = copy.deepcopy(spec); bad['matrix_sha256'] = 'a' * 64
        with self.assertRaises(ValueError):
            manual_campaign._reviewed_jobs(bad)

    def test_worst_case_budget_fits_two_hour_reservation(self):
        spec = workflow_input.specification('kv', 'native,tidehunter_nvme_rdma', 'focused',
                                             selection='nvmeof_f9_tools')
        worst = (manual_campaign.PREPARATION_SECONDS + manual_campaign.PAIR_SECONDS
                 + manual_campaign.PAIR_CLEANUP_SECONDS + manual_campaign.WORKER_READY_SECONDS
                 + sum(fleet_tools.TOOL_BUDGET_SECONDS.values())
                 + manual_campaign.EXPORT_RESERVE_SECONDS + manual_campaign.CAMPAIGN_MARGIN_SECONDS)
        self.assertEqual(worst, 6108)
        self.assertLessEqual(worst, 0.85 * manual_campaign.reservation_seconds(spec))

    def test_workflow_observer_and_reviewed_order(self):
        spec = workflow_input.specification('kv', 'native,tidehunter_nvme_rdma', 'focused',
                                             selection='nvmeof_f9_tools')
        self.assertEqual(spec['reservation_hours'], 2)
        self.assertEqual(manual_campaign.reviewed_window_spec(spec),
                         manual_campaign.REVIEWED_SPECS['nvmeof_f9_tools'])
        jobs = cloud_campaign.make_focused_matrix(spec['selection'])
        proof = qualify_observer_matrix.qualify_jobs(jobs)
        self.assertEqual(proof['planned_cases'], 34)
        self.assertEqual(proof['matrix_sha256'], spec['matrix_sha256'])
        order = risk_order.generate_order(jobs)
        self.assertEqual(order['case_ids'], [j['case_id'] for j in jobs])


if __name__ == '__main__':
    unittest.main()
