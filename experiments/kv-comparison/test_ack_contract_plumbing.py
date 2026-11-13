"""External acknowledgement lifecycle plumbing without service startup."""
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import ack_contract
import campaign
import cloud_controller
import cloud_layout
import local
import local_dry_run
import local_dry_run_valkey
import local_valkey_rdma


class AckContractPlumbingTests(unittest.TestCase):
    def test_valkey_fsync_policies_are_distinct(self):
        self.assertEqual([ack_contract.valkey_fsync(value) for value in
                          ('synced', 'os_buffered', 'periodic_sync')], ['always', 'no', 'everysec'])
        with self.assertRaisesRegex(ValueError, 'not supported'):
            ack_contract.valkey_fsync('page_cache')
        text = ack_contract.acknowledgement('valkey_tcp', 'periodic_sync')
        self.assertIn('neither OS-page-cache-before-reply nor synced-before-acknowledgement', text)
        with self.assertRaisesRegex(ValueError, 'not supported'):
            ack_contract.selected('kvrocks', 'periodic_sync')

    def test_campaign_executable_options_include_only_nondefault_contract(self):
        base = {'dataset': {}, 'phases': {}, 'execution': {}, 'limits': {}}
        for backend, contract in (('valkey_tcp', 'os_buffered'), ('valkey_rdma', 'periodic_sync'),
                                  ('kvrocks', 'page_cache')):
            for selected in ('synced', contract):
                with self.subTest(backend=backend, contract=selected):
                    state = {'nodes': 5, 'backend_options': {'nodes': ['host:6379']}}
                    job = {'backend': backend, 'profile': {'keys': 1, 'aging_operations': 0}, 'rate': 1}
                    if selected != 'synced':
                        state['ack_contract'] = selected
                        job['ack_contract'] = selected
                    config = campaign.build_config(base, job, state, Path('/tmp'), 1, False)
                    options = config['backend']['options']
                    self.assertEqual(options.get('ack_contract', 'synced'), selected)
                    self.assertEqual('ack_contract' in options, selected != 'synced')

    def test_cloud_options_include_only_nondefault_contract(self):
        fleet = {'nodes': [{'address': f'10.0.0.{i+1}'} for i in range(5)],
                 'coordinator': {'address': '10.0.0.10'}}
        for backend, contract in (('valkey_tcp', 'os_buffered'), ('kvrocks', 'page_cache')):
            with patch.object(cloud_layout, 'validate_fleet'), \
                    patch.object(cloud_layout, 'paired_valkey', return_value=False):
                strict = cloud_layout.backend_options(fleet, backend, '/tmp')
                relaxed = cloud_layout.backend_options(fleet, backend, '/tmp', ack_contract=contract)
            self.assertNotIn('ack_contract', strict)
            self.assertEqual(relaxed, dict(strict, ack_contract=contract))


    def test_unsupported_relaxed_lifecycle_rejected_before_start(self):
        for backend in ('garnet', 'cassandra', 'tikv', 'foundationdb'):
            runtime = cloud_controller.Controller.__new__(cloud_controller.Controller)
            with self.subTest(backend=backend), self.assertRaisesRegex(ValueError, 'not supported'):
                runtime.start_backend(backend, ack_contract='page_cache')
            self.assertFalse(hasattr(runtime, 'active_backend'))

    def test_controller_layout_carries_contract_only_when_relaxed(self):
        runtime = cloud_controller.Controller.__new__(cloud_controller.Controller)
        runtime.request = Mock(return_value={})
        runtime.layout('kvrocks', 'probe', 0, timeout=17)
        self.assertNotIn('ack_contract', runtime.request.call_args.args[1])
        runtime.active_ack_contract = 'page_cache'
        runtime.layout('kvrocks', 'export', 0)
        self.assertEqual(runtime.request.call_args.args[1]['ack_contract'], 'page_cache')

    def test_local_layout_cli_carries_contract(self):
        mode = local_dry_run.LocalMode.__new__(local_dry_run.LocalMode)
        mode.layout_backend = 'kvrocks'
        mode.layout_fleet = {}
        mode.root_command = Mock(return_value='{}')
        for contract in ('synced', 'page_cache'):
            mode.request(0, {'action': 'layout', 'backend': 'kvrocks', 'operation': 'probe',
                            'ack_contract': contract}, case='case', timeout=17, export=False)
            argv = mode.root_command.call_args.args[0]
            self.assertEqual('--ack-contract' in argv, contract == 'page_cache')
            if contract == 'page_cache':
                self.assertEqual(argv[argv.index('--ack-contract') + 1], contract)

    def test_local_backend_options_use_matching_readback_contract(self):
        mode = local_dry_run.LocalMode.__new__(local_dry_run.LocalMode)
        mode.valkey = None
        mode.layout_backend = 'kvrocks'
        mode.layout_fleet = {}
        with patch.object(cloud_layout, 'node_config', return_value={'directory': '/tmp/service'}) as node, \
                patch.object(cloud_layout, 'backend_options', return_value={'nodes': []}) as options:
            mode.backend_options('kvrocks', ack_contract='page_cache')
        self.assertEqual(node.call_args.kwargs, {'ack_contract': 'page_cache'})
        self.assertEqual(options.call_args.kwargs, {'ack_contract': 'page_cache'})
        mode.valkey = SimpleNamespace(backend='valkey_tcp', ack_contract='os_buffered', backend_options={})
        with self.assertRaisesRegex(RuntimeError, 'contract differs'):
            mode.backend_options('valkey_tcp')

    def test_local_valkey_readback_admits_selected_contract(self):
        cluster = local_dry_run_valkey.Cluster.__new__(local_dry_run_valkey.Cluster)
        cluster.backend = 'valkey_rdma'
        cluster.ack_contract = 'os_buffered'
        cluster.controls = ['127.0.0.21:6379']
        cluster.directory = Path('/tmp/owned')
        cluster.backend_options = {'nodes': ['10.0.0.1:6380']}
        cluster.monitor_processes = Mock(return_value=[])
        with patch.object(local, 'valkey_evidence', return_value={'nodes': []}), \
                patch.object(local, 'admit_valkey', return_value={}) as admit:
            cluster.probe()
        self.assertEqual(admit.call_args.kwargs['ack_contract'], 'os_buffered')

    def test_campaign_rejects_reused_contract_mismatch(self):
        with self.assertRaisesRegex(ValueError, 'contract differs'):
            campaign.backend_options('kvrocks', {'nodes': 5, 'ack_contract': 'page_cache'}, Path('/tmp'))
        options = campaign.backend_options('valkey_tcp', {'nodes': 5, 'ack_contract': 'os_buffered'},
                                           Path('/tmp'), ack_contract='os_buffered')
        self.assertEqual(len(options['nodes']), 5)
        self.assertEqual(options['ack_contract'], 'os_buffered')

    def test_paired_valkey_client_options_preserve_strict_default(self):
        endpoints = ['10.0.0.1:6380']
        for backend in ('valkey_tcp', 'valkey_rdma'):
            strict = local_valkey_rdma.options_for_backend(backend, 1, endpoints)
            relaxed = local_valkey_rdma.options_for_backend(backend, 1, endpoints, ack_contract='os_buffered')
            self.assertNotIn('ack_contract', strict)
            self.assertEqual(relaxed, dict(strict, ack_contract='os_buffered'))


if __name__ == '__main__':
    unittest.main()
