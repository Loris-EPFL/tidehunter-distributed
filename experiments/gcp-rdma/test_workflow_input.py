import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

import workflow_input


class WorkflowInputTest(unittest.TestCase):
    def test_matrix_preserves_explicit_selections(self):
        spec = workflow_input.specification('kv', 'native,tidehunter_tcp,tidehunter_nvme_tcp,tidehunter_nvme_rdma', 'nvmeof-focused')
        self.assertEqual(spec['backends'], ['native', 'tidehunter_tcp', 'tidehunter_nvme_tcp', 'tidehunter_nvme_rdma'])
        self.assertEqual(spec['suite'], 'kv')
        self.assertEqual(spec['value_sizes'], [64, 256, 1024, 8192, 65536])

    def test_standard_matrix_accepts_external_comparison_backends(self):
        spec = workflow_input.specification('kv', 'native,tidehunter_tcp,tikv,cassandra,foundationdb,valkey_tcp', 'standard')
        self.assertIn('foundationdb', spec['backends'])

    def test_unconfigured_duration_has_policy_error_not_type_error(self):
        import json
        config = {'version': 1, 'project_id': 'mysten-research', 'project_number': '1',
                  'bucket': 'b', 'service_account': 's', 'image': 'i',
                  'machine_type': 'z4m-highmem-16', 'max_distance': 3,
                  'candidates': [{'zone': 'us-central1-a', 'ordinary_subnet': 'a',
                                  'rdma_subnet': 'b', 'network_profile': 'p'}]}
        budget = {'weekly_limit_cents': 100, 'run_upper_bound_cents': 50,
                  'max_duration_seconds': None, 'quote_description': 'quote'}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'config.json').write_text(json.dumps(config))
            (root / 'budget.json').write_text(json.dumps(budget))
            with self.assertRaisesRegex(ValueError, 'max_duration_seconds'):
                workflow_input.validate_policy(root / 'config.json', root / 'budget.json')

    def test_invalid_backends_fail_closed(self):
        for backends, mode in [('native,tidehunter_efa', 'standard'),
                               ('native,native', 'standard'),
                               ('tidehunter_nvme_tcp', 'standard'), ('native,tidehunter_tcp', 'nvmeof-focused'), ('', 'standard')]:
            with self.subTest(backends=backends), self.assertRaises(ValueError):
                workflow_input.specification('kv', backends, mode)

    def test_sui_requires_reviewed_generation_bound_fixture(self):
        with self.assertRaises(ValueError):
            workflow_input.specification('sui', 'native,tidehunter_tcp', 'standard')
        fixture = {'key': 'fixtures/checkpoint.tar.gz', 'generation': '123', 'sha256': workflow_input.FIXTURE_SHA}
        self.assertEqual(workflow_input.specification('both', 'native,tidehunter_tcp', 'standard', fixture)['sui_fixture'], fixture)
        with self.assertRaises(ValueError):
            workflow_input.specification('sui', 'native', 'standard', fixture | {'generation': 'latest'})
        for transport in ('efa', 'fabric-tcp', 'fabric-tcp-native'):
            with self.subTest(transport=transport), self.assertRaises(ValueError):
                workflow_input.specification('sui', 'native', 'standard', fixture, transport)
        spec = workflow_input.specification('both', 'native,tidehunter_tcp,tidehunter_nvme_tcp,tidehunter_nvme_rdma', 'nvmeof-focused', fixture, 'tcp,tidehunter_verbs')
        self.assertEqual(spec['sui_transports'], ['tcp', 'tidehunter_verbs'])
        self.assertIn('tidehunter_nvme_rdma', spec['backends'])

    def test_fixture_bytes_are_verified_after_generation_qualified_download(self):
        import json
        import gcp_api
        class Store:
            def download(self, key, path, **kwargs):
                self.call = (key, kwargs)
                Path(path).write_bytes(b'wrong fixture')
        store = Store()
        class Api:
            def __init__(self, project):
                self.project = project
            def objects(self, bucket):
                return store
        with tempfile.TemporaryDirectory() as directory, patch.object(gcp_api, 'GoogleApi', Api):
            root = Path(directory)
            (root / 'config.json').write_text(json.dumps({'project_id': 'p', 'bucket': 'b'}))
            (root / 'spec.json').write_text(json.dumps({'sui_fixture': {'key': 'fixture', 'generation': '7', 'sha256': workflow_input.FIXTURE_SHA}}))
            with self.assertRaises(ValueError):
                workflow_input.download_fixture(root / 'config.json', root / 'spec.json', root / 'fixture')
            self.assertFalse((root / 'fixture').exists())
            self.assertEqual(store.call[1]['generation'], '7')


if __name__ == '__main__':
    unittest.main()
