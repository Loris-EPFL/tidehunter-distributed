import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import risk_order


class RiskOrderTests(unittest.TestCase):
    def test_reads_geometry_pins_and_order(self):
        import manual_campaign
        jobs = risk_order.cloud_campaign.make_focused_matrix("nvmeof_reads_geometry")
        digest = hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        self.assertEqual(digest, risk_order.READS_GEOMETRY_MATRIX_SHA256)
        spec = manual_campaign.REVIEWED_SPECS["nvmeof_reads_geometry"]
        self.assertEqual(spec["suite"], "kv")
        self.assertEqual(spec["job_count"], 74)
        self.assertEqual(spec["matrix_sha256"], digest)
        document = {"matrix_sha256": digest, "planned_cases": 74,
                    "case_ids": [j["case_id"] for j in jobs]}
        self.assertTrue(risk_order.validate_order(jobs, document))
        with self.assertRaises(ValueError):
            risk_order.validate_order(jobs, {**document, "case_ids": document["case_ids"][::-1]})

    def setUp(self):
        self.jobs = [
            {'case_id': 'a', 'backend': 'tidehunter_tcp', 'sealed_dataset': {'source_id': 's'}},
            {'case_id': 'b', 'backend': 'tidehunter_nvme_rdma', 'sealed_dataset': {'source_id': 's'}},
            {'case_id': 'c', 'backend': 'native'},
        ]
        self.order = {'matrix_sha256': hashlib.sha256(json.dumps(
            self.jobs, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
            'planned_cases': 3, 'case_ids': ['c', 'a', 'b']}

    def test_admits_complete_contiguous_order(self):
        self.assertTrue(risk_order.validate_order(self.jobs, self.order))

    def test_rejects_changed_matrix_or_nonpermutation(self):
        for change in ({'matrix_sha256': '0' * 64}, {'case_ids': ['a', 'a', 'c']}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                risk_order.validate_order(self.jobs, {**self.order, **change})

    def test_rejects_split_or_reversed_dataset(self):
        for cases in (['a', 'c', 'b'], ['b', 'a', 'c']):
            with self.subTest(cases=cases), self.assertRaises(ValueError):
                risk_order.validate_order(self.jobs, {**self.order, 'case_ids': cases})

    def test_rejects_native_read_after_regression_cleanup(self):
        jobs = [{'case_id': 'r', 'backend': 'native', 'sealed_dataset': {'source_id': 'n'}},
                {'case_id': 'w', 'backend': 'native', 'cohort': 'native_write_regression'}]
        digest = hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        with self.assertRaisesRegex(ValueError, 'cleanup'):
            risk_order.validate_order(jobs, {'matrix_sha256': digest, 'planned_cases': 2, 'case_ids': ['w', 'r']})

    def test_generated_reviewed_matrix_order(self):
        import manual_campaign
        spec = {key: value for key, value in manual_campaign.REVIEWED_SPEC.items() if key != 'suite'}
        jobs = risk_order.cloud_campaign.make_matrix(spec)
        order = risk_order.generate_order(jobs)
        self.assertTrue(risk_order.validate_order(jobs, order))
        by_id = {job['case_id']: job for job in jobs}
        first = by_id[order['case_ids'][0]]
        self.assertEqual((first.get('nvmeof_route'), first['cohort']), ('live', 'cold_os_cache'))
        self.assertEqual(len(order['case_ids']), len(jobs))
    def test_focused_order_preserves_matrix_and_canaries(self):
        for selection in ('stages', 'unified', 'append_floors', 'fair_native', 'parity'):
            with self.subTest(selection=selection):
                jobs = risk_order.cloud_campaign.make_focused_matrix(selection)
                order = risk_order.generate_order(jobs)
                self.assertEqual(order['case_ids'], [job['case_id'] for job in jobs])
                self.assertTrue(risk_order.validate_order(jobs, order))

    def test_fabric_ab_preserves_reviewed_order(self):
        jobs = risk_order.cloud_campaign.make_focused_matrix('fabric_ab_compact')
        self.assertEqual(len(jobs), 80)
        order = risk_order.generate_order(jobs)
        self.assertEqual(order['case_ids'], [job['case_id'] for job in jobs])
        self.assertTrue(risk_order.validate_order(jobs, order))
        order['case_ids'][0], order['case_ids'][1] = order['case_ids'][1], order['case_ids'][0]
        with self.assertRaisesRegex(ValueError, 'reviewed matrix'):
            risk_order.validate_order(jobs, order)

    def test_leased_ab_preserves_reviewed_order(self):
        jobs = risk_order.cloud_campaign.make_focused_matrix('leased_ab_compact')
        order = risk_order.generate_order(jobs)
        self.assertEqual(order['case_ids'], [job['case_id'] for job in jobs])
        order['case_ids'][0], order['case_ids'][1] = order['case_ids'][1], order['case_ids'][0]
        with self.assertRaisesRegex(ValueError, 'reviewed matrix'):
            risk_order.validate_order(jobs, order)
    def test_nvmeof_path_preserves_canaries_and_group_order(self):
        for selection in risk_order.cloud_campaign.NVMEOF_PATH_SELECTIONS:
            expected_count = {'nvmeof_path_compact': 157, 'nvmeof_path_core': 66, 'nvmeof_path_fix': 37, 'nvmeof_first_put': 12, 'nvmeof_contracts': 34, 'nvmeof_contracts_crossing': 35, 'nvmeof_contracts_crossing2': 36, 'nvmeof_parallel_writes': 44, 'nvmeof_reads_geometry': 74, 'nvmeof_reads_geometry_rest': 20}[selection]
            with self.subTest(selection=selection):
                jobs = risk_order.cloud_campaign.make_focused_matrix(selection)
                self.assertEqual(len(jobs), expected_count)
                original = [job['case_id'] for job in jobs]
                order = risk_order.generate_order(jobs)
                if selection == 'nvmeof_parallel_writes':
                    self.assertEqual(order['matrix_sha256'], '64def2342711b6f17b65796ff907fcc6b390c9d7cee9e93ed85d8623c8e32aa4')
                self.assertEqual(order['case_ids'][:12], original[:12])
                self.assertEqual(order['case_ids'], original)
                for left, right in ((0, 1), (min(12, len(jobs)//2), min(13, len(jobs)//2+1)), (len(jobs) - 2, len(jobs) - 1)):
                    changed = dict(order, case_ids=list(original))
                    changed['case_ids'][left], changed['case_ids'][right] = (
                        changed['case_ids'][right], changed['case_ids'][left])
                    with self.subTest(left=left, right=right), self.assertRaisesRegex(ValueError, 'reviewed matrix'):
                        risk_order.validate_order(jobs, changed)


    def test_fair_native_canary_cannot_move(self):
        for selection in ('fair_native', 'parity'):
            jobs = risk_order.cloud_campaign.make_focused_matrix(selection)
            order = risk_order.generate_order(jobs)
            self.assertEqual(order['case_ids'][0], 'G14N-r0-676ab5a538a3ed589fff')
            order['case_ids'][0], order['case_ids'][1] = order['case_ids'][1], order['case_ids'][0]
            with self.subTest(selection=selection), self.assertRaisesRegex(ValueError, 'canary'):
                risk_order.validate_order(jobs, order)

    def test_floors_order_preserves_matrix_and_canaries(self):
        jobs = risk_order.cloud_campaign.make_focused_matrix('floors')
        order = risk_order.generate_order(jobs)
        self.assertEqual(order['case_ids'], [job['case_id'] for job in jobs])
        self.assertTrue(risk_order.validate_order(jobs, order))

    def test_device_order_preserves_matrix_and_canaries(self):
        jobs = risk_order.cloud_campaign.make_focused_matrix('device')
        order = risk_order.generate_order(jobs)
        self.assertEqual(order['case_ids'], [job['case_id'] for job in jobs])
        self.assertTrue(risk_order.validate_order(jobs, order))


    def test_focused_cli_generates_and_validates_selection_bound_orders(self):
        import manual_campaign
        with tempfile.TemporaryDirectory() as directory:
            for selection in ('recommended', 'full'):
                with self.subTest(selection=selection):
                    path = Path(directory) / (selection + '.json')
                    output = io.StringIO()
                    with patch.object(sys, 'argv', ['risk_order', str(path), '--generate', '--selection', selection]), \
                            patch('sys.stdout', output):
                        risk_order.main()
                    document = json.loads(path.read_text())
                    reviewed = manual_campaign.REVIEWED_SPECS[selection]
                    self.assertEqual(document['matrix_sha256'], reviewed['matrix_sha256'])
                    self.assertEqual(document['planned_cases'], reviewed['job_count'])
                    jobs = risk_order.cloud_campaign.make_focused_matrix(selection)
                    direct_ids = {job['case_id'] for job in jobs if job.get('target_io') == 'direct'}
                    self.assertEqual(len(direct_ids), 9)
                    self.assertTrue(direct_ids <= set(document['case_ids']))
                    self.assertEqual(json.loads(output.getvalue())['cases'], reviewed['job_count'])
                    with patch.object(sys, 'argv', ['risk_order', str(path), '--selection', selection]), \
                            patch('sys.stdout', io.StringIO()):
                        risk_order.main()
                    other = 'full' if selection == 'recommended' else 'recommended'
                    with patch.object(sys, 'argv', ['risk_order', str(path), '--selection', other]), \
                            self.assertRaisesRegex(ValueError, 'does not bind'):
                        risk_order.main()


if __name__ == '__main__':
    unittest.main()
