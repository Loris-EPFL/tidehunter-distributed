import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

import workflow_input
import gcp_campaign


class WorkflowInputTest(unittest.TestCase):
    def test_deployment_image_requires_one_complete_immutable_authority(self):
        import json
        config = {'project_id': 'mysten-research', 'bucket': 'artifacts',
                  'image': '', 'image_qualification': None}
        image = 'projects/mysten-research/global/images/qualified-image'
        ref = {'bucket': 'artifacts', 'object': 'images/receipt.json',
               'generation': '123', 'sha256': 'a' * 64}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'config.json'
            source.write_text(json.dumps(config))
            workflow_input.deployment_configuration(source, image, json.dumps(ref), root / 'valid.json')
            self.assertEqual(json.loads((root / 'valid.json').read_text())['image_qualification'], ref)
            self.assertEqual(json.loads(source.read_text()), config)
            invalid = [(image, ''), ('', json.dumps(ref)),
                       (image, json.dumps(ref | {'generation': 'latest'})),
                       (image, json.dumps(ref | {'bucket': 'foreign'})),
                       (image, json.dumps(ref | {'sha256': 'bad'})),
                       (image + '/unexpected', json.dumps(ref))]
            for index, (selected, receipt) in enumerate(invalid):
                with self.subTest(index=index), self.assertRaises(ValueError):
                    workflow_input.deployment_configuration(source, selected, receipt, root / f'bad-{index}.json')
                self.assertFalse((root / f'bad-{index}.json').exists())
            source.write_text(json.dumps(config | {'image': image, 'image_qualification': ref}))
            with self.assertRaisesRegex(ValueError, 'both repository and environment'):
                workflow_input.deployment_configuration(source, image, json.dumps(ref), root / 'duplicate.json')

    def test_matrix_preserves_explicit_selections(self):
        spec = workflow_input.specification('kv', 'native,tidehunter_tcp,tidehunter_nvme_tcp,tidehunter_nvme_rdma', 'nvmeof-focused')
        self.assertEqual(spec['backends'], ['native', 'tidehunter_tcp', 'tidehunter_nvme_tcp', 'tidehunter_nvme_rdma'])
        self.assertEqual(spec['suite'], 'kv')
        self.assertEqual(spec['value_sizes'], [64, 256, 1024, 8192, 65536])

    def test_portable_focused_rdma_does_not_require_nvme_tcp(self):
        backends = 'native,tidehunter_tcp,tidehunter_verbs_native,tidehunter_nvme_rdma'
        spec = workflow_input.specification('kv', backends, 'nvmeof-focused')
        self.assertEqual(spec['backends'], backends.split(','))
        jobs = workflow_input.cloud_campaign.make_matrix({k: v for k, v in spec.items() if k != 'suite'})
        self.assertEqual({job['backend'] for job in jobs}, set(backends.split(',')))
        gcp_campaign.validate_spec(spec)
        with self.assertRaises(ValueError):
            gcp_campaign.validate_spec({**spec, 'mode': 'standard'})
        with self.assertRaises(ValueError):
            gcp_campaign.validate_spec({**spec, 'backends': spec['backends'] + ['tidehunter_efa']})
        with self.assertRaises(ValueError):
            workflow_input.specification('kv', backends, 'standard')
        with self.assertRaises(ValueError):
            workflow_input.specification('kv', backends + ',tidehunter_efa', 'nvmeof-focused')

    def test_combined_mode_is_accepted_by_workflow_and_campaign(self):
        backends = 'native,tidehunter_tcp,tidehunter_verbs_native,tidehunter_nvme_rdma,valkey_tcp,valkey_rdma'
        spec = workflow_input.specification('kv', backends, 'standard-with-nvmeof')
        gcp_campaign.validate_spec(spec)
        jobs = workflow_input.cloud_campaign.make_matrix({k: v for k, v in spec.items() if k != 'suite'})
        self.assertEqual({job['backend'] for job in jobs}, set(backends.split(',')))
        with self.assertRaises(ValueError):
            gcp_campaign.validate_spec({**spec, 'mode': 'unknown'})

    def test_live_mode_is_accepted_but_not_allocated_without_pair_proof(self):
        backends = 'native,tidehunter_tcp,tidehunter_nvme_rdma'
        spec = workflow_input.specification('kv', backends, 'standard-with-live-nvmeof')
        gcp_campaign.validate_spec(spec)
        jobs = workflow_input.cloud_campaign.make_matrix({k: v for k, v in spec.items() if k != 'suite'})
        self.assertEqual({job['backend'] for job in jobs}, set(backends.split(',')))
        nvme_routes = {job.get('nvmeof_route') for job in jobs if job['backend'] == 'tidehunter_nvme_rdma'}
        self.assertEqual(nvme_routes, {'live', 'sealed_read'})
        with self.assertRaisesRegex(ValueError, 'physical route/counter evidence'):
            gcp_campaign.launch(None, None, None, None, None, None, spec, None)

    def test_live_mode_requires_matched_native_and_tcp_controls(self):
        mode = 'standard-with-live-nvmeof'
        spec = workflow_input.specification('kv', 'native,tidehunter_tcp,tidehunter_nvme_rdma', mode)
        for missing in ('native', 'tidehunter_tcp', 'tidehunter_nvme_rdma'):
            selected = [backend for backend in spec['backends'] if backend != missing]
            with self.subTest(missing=missing):
                with self.assertRaisesRegex(ValueError, 'requires native, RPC TCP and at least one NVMe-oF backend'):
                    workflow_input.specification('kv', ','.join(selected), mode)
                with self.assertRaisesRegex(ValueError, 'requires native, RPC TCP and at least one NVMe-oF backend'):
                    gcp_campaign.validate_spec({**spec, 'backends': selected})

    def test_standard_matrix_accepts_external_comparison_backends(self):
        spec = workflow_input.specification('kv', 'native,tidehunter_tcp,tikv,cassandra,foundationdb,valkey_tcp', 'standard')
        self.assertIn('foundationdb', spec['backends'])

    def test_industry_stores_match_valkey_standard_workloads(self):
        baseline = 'native,tidehunter_tcp,tidehunter_verbs_native,tikv,cassandra,foundationdb,valkey_tcp,valkey_rdma'
        original = workflow_input.specification('kv', baseline, 'standard')
        expanded = workflow_input.specification('kv', baseline + ',garnet,kvrocks', 'standard')
        def matrix(spec):
            return workflow_input.cloud_campaign.make_matrix({k: v for k, v in spec.items() if k != 'suite'})
        original_jobs = matrix(original)
        jobs = matrix(expanded)
        self.assertEqual(len(jobs), len(original_jobs) + 50)
        self.assertLessEqual(len(jobs), expanded['max_cases'])
        def population(backend):
            return [{k: v for k, v in job.items() if k not in ('backend', 'case_id')}
                    for job in jobs if job['backend'] == backend]
        for backend in ('garnet', 'kvrocks'):
            self.assertEqual(len(population(backend)), 25)
            self.assertEqual(population(backend), population('valkey_tcp'))
        for backend in ('garnet', 'kvrocks'):
            selected = workflow_input.specification('kv', backend, 'standard')
            self.assertEqual(selected['backends'], [backend])


    def test_valkey_rdma_has_matched_tcp_workloads(self):
        spec = workflow_input.specification(
            'kv', 'native,tidehunter_tcp,tidehunter_verbs_native,valkey_tcp,valkey_rdma', 'standard')
        jobs = workflow_input.cloud_campaign.make_matrix({k: v for k, v in spec.items() if k != 'suite'})
        populations = {}
        for backend in ('valkey_tcp', 'valkey_rdma'):
            populations[backend] = [
                {k: v for k, v in job.items() if k not in ('backend', 'case_id')}
                for job in jobs if job['backend'] == backend]
        self.assertEqual(populations['valkey_tcp'], populations['valkey_rdma'])
        self.assertEqual({job['value_size'] for job in populations['valkey_rdma']},
                         {64, 256, 1024, 8192, 65536})
        with self.assertRaisesRegex(ValueError, 'matched Valkey TCP'):
            workflow_input.specification('kv', 'valkey_rdma', 'standard')

    def test_unconfigured_duration_has_policy_error_not_type_error(self):
        import json
        config = {'version': 1, 'project_id': 'mysten-research', 'project_number': '1',
                  'bucket': 'b', 'service_account': 'bench-guest@mysten-research.iam.gserviceaccount.com', 'image': 'i',
                  'machine_type': 'z4m-highmem-16', 'max_distance': 3,
                  'candidates': [{'zone': 'us-central1-a', 'ordinary_subnet': 'a',
                                  'rdma_subnet': 'b', 'network_profile': 'p'}]}
        budget = {'weekly_limit_cents': 10000, 'run_upper_bound_cents': 9000,
                  'max_duration_seconds': None, 'quote_description': 'quote'}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'config.json').write_text(json.dumps(config))
            (root / 'budget.json').write_text(json.dumps(budget))
            with self.assertRaisesRegex(ValueError, 'max_duration_seconds'):
                workflow_input.validate_policy(root / 'config.json', root / 'budget.json')
            budget['max_duration_seconds'] = 6 * 3600
            (root / 'budget.json').write_text(json.dumps(budget))
            with self.assertRaises(ValueError):
                workflow_input.validate_policy(root / 'config.json', root / 'budget.json')
            budget['max_duration_seconds'] = 4 * 3600
            (root / 'budget.json').write_text(json.dumps(budget))
            deadline = workflow_input.validate_policy(root / 'config.json', root / 'budget.json')
            from datetime import datetime, timezone
            seconds = (datetime.fromisoformat(deadline) - datetime.now(timezone.utc)).total_seconds()
            self.assertGreater(seconds, 15 * 60)
            self.assertLessEqual(seconds, 4 * 3600)

    def test_invalid_backends_fail_closed(self):
        for backends, mode in [('native,tidehunter_efa', 'standard'),
                               ('native,native', 'standard'),
                               ('tidehunter_nvme_tcp', 'standard'), ('native,tidehunter_tcp', 'nvmeof-focused'),
                               ('native,tidehunter_tcp,tidehunter_nvme_tcp,garnet', 'nvmeof-focused'),
                               ('native,tidehunter_tcp,tidehunter_nvme_tcp,kvrocks', 'nvmeof-focused'),
                               ('native,garnet-unknown', 'standard'), ('', 'standard')]:
            with self.subTest(backends=backends), self.assertRaises(ValueError):
                workflow_input.specification('kv', backends, mode)



if __name__ == '__main__':
    unittest.main()
