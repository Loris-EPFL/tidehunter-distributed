import copy
import unittest
import manual_campaign
import risk_order


class ValkeyReadSpecTests(unittest.TestCase):
    def test_reviewed_spec_and_order(self):
        spec = manual_campaign.REVIEWED_SPECS['valkey_rdma_reads']
        self.assertEqual(spec['suite'], 'kv')
        self.assertEqual(spec['backends'], ['valkey_rdma', 'tidehunter_nvme_rdma'])
        self.assertEqual(spec['owner_hosts'], 1)
        jobs = manual_campaign._reviewed_jobs(spec)
        self.assertEqual(len(jobs), 22)
        order = risk_order.generate_order(jobs)
        self.assertEqual(order['matrix_sha256'], spec['matrix_sha256'])
        self.assertTrue(risk_order.validate_order(jobs, order))
        changed = copy.deepcopy(order)
        changed['case_ids'].reverse()
        with self.assertRaises(ValueError):
            risk_order.validate_order(jobs, changed)
        changed = copy.deepcopy(spec)
        changed['backends'].append('valkey_tcp')
        with self.assertRaises(ValueError):
            manual_campaign._reviewed_jobs(changed)
