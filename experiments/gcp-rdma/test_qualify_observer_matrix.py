"""Offline observer admission for the current generated campaign matrix."""
import copy
import io
import json
import sys
import unittest
from unittest.mock import patch

import qualify_observer_matrix as qualification


class ObserverMatrixQualification(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import manual_campaign
        spec = {key: value for key, value in manual_campaign.REVIEWED_SPEC.items() if key != 'suite'}
        cls.jobs = qualification.cloud_campaign.make_matrix(spec)

    def test_current_matrix_including_durability_and_sealed_targets(self):
        tide = [job for job in self.jobs if job['backend'] in qualification.cloud_campaign.TRANSPORTS]
        sealed = [job for job in tide if job.get('nvmeof_route') == 'sealed_read']
        self.assertTrue(tide)
        self.assertEqual({job['durability_tier'] for job in tide}, {'synchronous', 'native'})
        self.assertEqual({job.get('nvmeof_target', 'kernel') for job in sealed}, {'kernel', 'spdk'})
        # Queue geometry is configured by the sealed initiator, not a matrix job field.
        sys.path.insert(0, str(qualification.ROOT / 'experiments/nvmeof'))
        try:
            import manage
            self.assertEqual((manage.DEFAULT_NR_IO_QUEUES, manage.DEFAULT_QUEUE_SIZE), (4, 128))
        finally:
            sys.path.pop(0)
        receipt = qualification.qualify_jobs(self.jobs)
        self.assertEqual(receipt['planned_cases'], len(self.jobs))
        self.assertEqual(receipt['matrix_sha256'], qualification.matrix_digest(self.jobs))
    def test_focused_matrices_qualify_with_exact_hashes(self):
        import manual_campaign
        for selection in ('recommended', 'full', 'device', 'floors', 'stages', 'unified', 'append_floors', 'fair_native', 'parity'):
            with self.subTest(selection=selection):
                spec = manual_campaign.REVIEWED_SPECS[selection]
                if selection == 'nvmeof_parallel_writes':
                    self.assertEqual(spec['matrix_sha256'], 'e97badf8a942f01eb3ba51068f084599cd7c945b9ff88e9d26a7794672956c70')
                jobs = qualification.cloud_campaign.make_focused_matrix(selection)
                direct = [job for job in jobs if job.get('target_io') == 'direct']
                if selection not in ('stages', 'unified'):
                    self.assertEqual(len(direct), 0 if selection in ('floors', 'append_floors', 'fair_native', 'parity') else 15 if selection == 'device' else 9)
                self.assertTrue(all(job['nvmeof_route'] == 'sealed_read'
                                    and job['nvmeof_target'] == 'kernel' for job in direct))
                receipt = qualification.qualify_jobs(jobs)
                self.assertEqual(receipt['planned_cases'], spec['job_count'])
                self.assertEqual(receipt['matrix_sha256'], spec['matrix_sha256'])
                output = io.StringIO()
                with patch('sys.stdin', io.StringIO(json.dumps(spec))), patch('sys.stdout', output):
                    qualification.main()
                self.assertEqual(json.loads(output.getvalue()), receipt)


    def test_fabric_ab_observer_qualifies_reviewed_matrix(self):
        import manual_campaign
        spec = manual_campaign.REVIEWED_SPECS['fabric_ab_compact']
        jobs = qualification.cloud_campaign.make_focused_matrix('fabric_ab_compact')
        receipt = qualification.qualify_jobs(jobs)
        self.assertEqual((receipt['planned_cases'], receipt['matrix_sha256']),
                         (spec['job_count'], spec['matrix_sha256']))

    def test_leased_compact_observer_qualifies_exact_matrix(self):
        import manual_campaign
        spec = manual_campaign.REVIEWED_SPECS['leased_ab_compact']
        jobs = qualification.cloud_campaign.make_focused_matrix('leased_ab_compact')
        receipt = qualification.qualify_jobs(jobs)
        self.assertEqual((receipt['planned_cases'], receipt['matrix_sha256']),
                         (spec['job_count'], spec['matrix_sha256']))

    def test_nvmeof_path_observer_qualifies_exact_matrix(self):
        for selection in qualification.cloud_campaign.NVMEOF_PATH_SELECTIONS:
            expected_count = {'nvmeof_path_compact': 157, 'nvmeof_path_core': 66, 'nvmeof_path_fix': 37, 'nvmeof_first_put': 12, 'nvmeof_contracts': 34, 'nvmeof_contracts_crossing': 35, 'nvmeof_contracts_crossing2': 36, 'nvmeof_parallel_writes': 44}[selection]
            with self.subTest(selection=selection):
                import manual_campaign
                spec = manual_campaign.REVIEWED_SPECS[selection]
                if selection == 'nvmeof_parallel_writes':
                    self.assertEqual(spec['matrix_sha256'], 'e97badf8a942f01eb3ba51068f084599cd7c945b9ff88e9d26a7794672956c70')
                jobs = qualification.cloud_campaign.make_focused_matrix(selection)
                self.assertEqual(len(jobs), expected_count)
                self.assertEqual({job['backend'] for job in jobs},
                                 {'native', 'tidehunter_nvme_rdma'} if selection in ('nvmeof_first_put', 'nvmeof_parallel_writes') else
                                 {'native', 'tidehunter_verbs_native', 'tidehunter_nvme_rdma'})
                receipt = qualification.qualify_jobs(jobs)
                self.assertEqual((receipt['planned_cases'], receipt['matrix_sha256']),
                                 (spec['job_count'], spec['matrix_sha256']))
                output = io.StringIO()
                with patch('sys.stdin', io.StringIO(json.dumps(spec))), patch('sys.stdout', output):
                    qualification.main()
                self.assertEqual(json.loads(output.getvalue()), receipt)

    def test_failure_names_the_offending_case(self):
        job = copy.deepcopy(self.jobs[0])
        job['value_size'] = 0
        with self.assertRaisesRegex(ValueError, job['case_id']):
            qualification.qualify_jobs([job])

    def test_waiting_selection_rejects_fabricated_measurements(self):
        payload = (b'kv_observer_data_available{case_id="case"} 0\n'
                   b'kv_case_offered_calls{case_id="case"} 0\n')
        with patch.object(qualification.campaign_exporter, 'benchmark_payload', return_value=payload):
            with self.assertRaisesRegex(ValueError, 'fabricated measurement'):
                qualification.qualify_jobs([self.jobs[0]])


if __name__ == '__main__':
    unittest.main()
