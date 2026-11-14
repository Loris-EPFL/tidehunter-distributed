import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import cloud_controller as controller
from test_cloud_controller import config_fixture, clients_fixture


class FabricControllerTests(unittest.TestCase):
    def fleet(self):
        config = config_fixture()
        config['fleet']['nodes'] = config['fleet']['nodes'][:1]
        config['spec'].update(mode='focused', selection='fabric_ab_compact')
        return controller.Controller(config, clients_fixture(config))

    def test_all_reviewed_jobs_reach_dispatch_and_mutations_fail(self):
        runtime = self.fleet()
        jobs = controller.cloud_campaign.make_focused_matrix('fabric_ab_compact')
        self.assertEqual(len(jobs), 80)
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            with patch.object(controller, 'guarded', side_effect=RuntimeError('dispatch reached')):
                for job in jobs:
                    with self.subTest(case=job['case_id']), self.assertRaisesRegex(RuntimeError, 'dispatch reached'):
                        runtime.run_case(job)
                nvme = next(j for j in jobs if j['backend'] == 'tidehunter_nvme_tcp')
                owner = next(j for j in jobs if j['backend'] == 'tidehunter_tcp_trusted')
                mutations = [dict(nvme, nvmeof_route='live'), dict(nvme, backend='native'),
                             dict(nvme, owner_count=5), dict(nvme, durability_tier='native'),
                             dict(nvme, nvmeof_target='spdk'), dict(owner, backend='tidehunter_nvme_rdma')]
                for job in mutations:
                    with self.subTest(job=job), self.assertRaises(ValueError):
                        runtime.run_case(job)
                runtime.config['spec']['selection'] = 'remote_rw_compact'
                with self.assertRaises(ValueError):
                    runtime.run_case(nvme)

    def test_preparation_polling_transport_and_empty_credentials(self):
        import yaml
        runtime = self.fleet()
        runtime.remaining = Mock(return_value=300)
        runtime.request = Mock(return_value={'receipt': 'proof'})
        jobs = controller.cloud_campaign.make_focused_matrix('fabric_ab_compact')
        cases = [next(j for j in jobs if j['backend'] == backend and j.get('owner_polling', 'adaptive') == polling)
                 for backend, polling in [('tidehunter_tcp_trusted', 'busy'), ('tidehunter_nvme_tcp', 'adaptive')]]
        for job in cases:
            with self.subTest(backend=job['backend']), tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp)
                def prepare(argv, **kwargs):
                    prep = json.loads((directory / 'prepare-input.json').read_text())
                    self.assertEqual(prep['polling'], job.get('owner_polling', 'adaptive'))
                    expected = 'tidehunter_verbs_native' if job['backend'] == 'tidehunter_nvme_tcp' else job['backend']
                    self.assertEqual(prep['transport'], controller.cloud_campaign.TRANSPORTS[expected])
                    prepared = directory / 'prepared'
                    (prepared / 'node-0').mkdir(parents=True)
                    (prepared / 'remote.json').write_text(json.dumps({'tls': {'ca': '', 'cert': '', 'key': ''}, 'storage': {'fragment_bytes': 4194304, 'wal_file_bytes': 1073741824}}))
                    (prepared / 'external_owner_read_config.json').write_text('[]')
                    (prepared / 'node-0' / 'launch.yaml').write_text(yaml.safe_dump({'tls': {}, 'node': {'partition': {'fragment_bytes': 4194304, 'wal_file_bytes': 1073741824}}}))
                with patch.object(controller, 'controller_matrix', return_value=[]), \
                     patch.object(controller, 'command', side_effect=prepare), \
                     patch.object(controller, 'validate_launch') as validate, \
                     patch.object(controller, 'upload_object'), \
                     patch.object(controller, 'confirm_owner_durability', return_value={'partition': 0}):
                    runtime.prepare_tide(job, directory)
                self.assertEqual(validate.call_args.args[-1], job.get('owner_polling', 'adaptive'))
                self.assertFalse((directory / 'prepared' / 'tls').exists())
        with tempfile.TemporaryDirectory() as tmp, self.assertRaisesRegex(ValueError, 'owner_polling'):
            runtime.prepare_tide(dict(cases[0], owner_polling='invalid'), Path(tmp))

    def test_launch_polling_mismatch_rejected(self):
        runtime = self.fleet()
        with self.assertRaisesRegex(controller.SafetyError, 'polling'):
            controller.validate_launch(runtime.config, {'limits': {'polling': 'adaptive'}}, 'case', 0, 'busy')

    def test_direct_tcp_is_narrow_and_spdk_tcp_rejected(self):
        jobs = controller.cloud_campaign.make_focused_matrix('fabric_ab_compact')
        job = next(j for j in jobs if j['backend'] == 'tidehunter_nvme_tcp')
        controller.require_supported_nvme_route(job)
        for mutation in ({'nvmeof_target': 'spdk'}, {'nvmeof_route': 'live'}, {'selection': 'remote_rw_compact'}):
            with self.subTest(mutation=mutation), self.assertRaises(controller.SafetyError):
                controller.require_supported_nvme_route(job | mutation)

    def test_reuse_policy_mismatch_stops_old_owner(self):
        runtime = self.fleet()
        runtime.shared_tide = {'identity': ('source', 'tidehunter_tcp_trusted', 'synchronous', 'adaptive')}
        runtime.active_nvme = None
        runtime.stop_tide = Mock()
        job = {'sealed_dataset': {'source_id': 'source'}, 'backend': 'tidehunter_tcp_trusted',
               'durability_tier': 'synchronous', 'owner_polling': 'busy'}
        with tempfile.TemporaryDirectory() as tmp:
            runtime.root = Path(tmp)
            runtime.prepare_tide = Mock(side_effect=RuntimeError('new owner'))
            with self.assertRaisesRegex(RuntimeError, 'new owner'):
                runtime.prepared_tide_options(job, Path(tmp))
        runtime.stop_tide.assert_called_once()


    def test_owner_and_client_transport_match_for_all_fabric_jobs(self):
        import yaml
        runtime = self.fleet()
        runtime.remaining = Mock(return_value=300)
        runtime.request = Mock(return_value={'receipt': 'proof'})
        jobs = controller.cloud_campaign.make_focused_matrix('fabric_ab_compact')
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for job in jobs:
                with self.subTest(case=job['case_id']):
                    directory = root / job['case_id']
                    directory.mkdir()
                    def prepare(argv, **kwargs):
                        prepared = directory / 'prepared'
                        (prepared / 'node-0').mkdir(parents=True)
                        (prepared / 'remote.json').write_text(json.dumps({'tls': {'ca': '', 'cert': '', 'key': ''}, 'storage': {'fragment_bytes': 4194304, 'wal_file_bytes': 1073741824}}))
                        (prepared / 'external_owner_read_config.json').write_text('[]')
                        (prepared / 'node-0' / 'launch.yaml').write_text(yaml.safe_dump({'tls': {}, 'node': {'partition': {'fragment_bytes': 4194304, 'wal_file_bytes': 1073741824}}}))
                    with patch.object(controller, 'controller_matrix', return_value=[]), \
                         patch.object(controller, 'command', side_effect=prepare), \
                         patch.object(controller, 'validate_launch'), \
                         patch.object(controller, 'upload_object'), \
                         patch.object(controller, 'confirm_owner_durability', return_value={'partition': 0}):
                        options = runtime.prepare_tide(job, directory)
                    prep = json.loads((directory / 'prepare-input.json').read_text())
                    config = controller.cloud_campaign.make_run_config(job, directory, options, 'a' * 40)
                    key = controller.protocol_key
                    self.assertEqual(key(prep['transport']), key(config['backend']['options']['transport']))
                    wrong = (controller.cloud_campaign.TRANSPORTS['tidehunter_tcp']
                             if key(prep['transport']) != key(controller.cloud_campaign.TRANSPORTS['tidehunter_tcp'])
                             else controller.cloud_campaign.TRANSPORTS['tidehunter_verbs_native'])
                    with self.assertRaisesRegex(ValueError, 'transport'):
                        controller.cloud_campaign.make_run_config(job, directory, options | {'transport': wrong}, 'a' * 40)

    def test_tcp_qualification_rejects_rdma_and_missing_device_reads(self):
        counters = {'eligible_hits': 10, 'ineligible_fallbacks': 0, 'io_submissions': 10,
                    'io_completions': 10, 'io_errors': 0, 'io_timeouts': 0}
        evidence = {'nvmeof': {'available': True, 'readers': {'0': {
            'identity': {'transport': 'tcp'}, 'counters': {'available': True, 'counters': counters}}}},
            'deltas': {'coordinator': {'available': True, 'counters': {'read_rpc_count': 0, 'hits': 0}}}}
        target = [{'target': {'exports': [{'target_receipt': {
            'selected_transport': 'tcp', 'target_cache': 'direct_file_no_OS_pagecache',
            'target_cache_verified': True, 'target_implementation': 'kernel_direct'}}]}}]
        qualify = controller.cloud_campaign.qualify_nvme_measurement
        self.assertTrue(qualify(evidence, 1, 'tcp', target, 'kernel_direct')['qualified'])
        wrong = copy.deepcopy(evidence)
        wrong['nvmeof']['readers']['0']['identity']['transport'] = 'rdma'
        self.assertFalse(qualify(wrong, 1, 'tcp', target, 'kernel_direct')['qualified'])
        wrong_target = copy.deepcopy(target)
        wrong_target[0]['target']['exports'][0]['target_receipt']['selected_transport'] = 'rdma'
        self.assertFalse(qualify(evidence, 1, 'tcp', wrong_target, 'kernel_direct')['qualified'])
        media = controller.qualify_media_evidence('case', {'expected': 'cold'}, None, None, 10, True)
        self.assertEqual(media['status'], 'unqualified')
        self.assertIn('missing_devices', media['reason_codes'])


if __name__ == '__main__':
    unittest.main()
