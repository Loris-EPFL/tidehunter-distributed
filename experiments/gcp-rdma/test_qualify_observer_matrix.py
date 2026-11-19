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

    def test_current_matrix_has_only_leased_nvme_and_both_durability_tiers(self):
        tide = [job for job in self.jobs if job['backend'] in qualification.cloud_campaign.NVME_BACKENDS]
        self.assertEqual({job['durability_tier'] for job in tide}, {'synchronous', 'native'})
        self.assertEqual({job.get('nvmeof_route') for job in tide}, {'live'})
        self.assertEqual({job.get('nvmeof_target', 'kernel') for job in tide}, {'kernel'})
        receipt = qualification.qualify_jobs(self.jobs)
        self.assertEqual(receipt['planned_cases'], len(self.jobs))
        self.assertEqual(receipt['matrix_sha256'], qualification.matrix_digest(self.jobs))

    def test_nvmeof_path_observer_qualifies_exact_matrix(self):
        for selection in qualification.cloud_campaign.NVMEOF_PATH_SELECTIONS:
            import manual_campaign
            if selection == qualification.cloud_campaign.Q0_SELECTION:
                from test_q0_campaign import seals
                spec = manual_campaign.q0_spec(seals())
            else:
                spec = manual_campaign.REVIEWED_SPECS[selection]
            expected_count = qualification.cloud_campaign.FOCUSED_COUNTS[selection]
            with self.subTest(selection=selection):
                if selection == 'nvmeof_parallel_writes':
                    self.assertEqual(spec['matrix_sha256'], '64def2342711b6f17b65796ff907fcc6b390c9d7cee9e93ed85d8623c8e32aa4')
                jobs = qualification.cloud_campaign.make_focused_matrix(selection)
                if selection in qualification.cloud_campaign.M3_SELECTIONS:
                    jobs = [job for job in jobs if qualification.cloud_campaign.job_owner_hosts(job) == spec['owner_hosts']]
                self.assertEqual(len(jobs), expected_count)
                self.assertEqual({job['backend'] for job in jobs},
                                 {'tidehunter_nvme_rdma'} if selection in ('nvmeof_f6', 'nvmeof_f6b')
                                 else set(spec['backends']))
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
