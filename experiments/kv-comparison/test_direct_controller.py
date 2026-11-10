"""Sealed direct-control admission and reader configuration fixtures."""
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

import cloud_campaign
import cloud_controller as controller
import test_nvme_campaign_integration as fixtures


class DirectControllerTests(unittest.TestCase):
    @staticmethod
    def job():
        return {'case_id': 'direct-read', 'case_kind': 'media_read', 'group': 'G1',
                'backend': 'tidehunter_nvme_rdma', 'nvmeof_route': 'sealed_read',
                'nvmeof_target': 'kernel', 'target_io': 'direct',
                'media': {'expected': 'cold'}, 'mix': cloud_campaign.mix(read=100),
                'sealed_dataset': {'source_id': 'source-case'}}

    def test_only_cold_g1_sealed_kernel_rdma_reads_admit_direct(self):
        job = self.job()
        controller.require_supported_nvme_route(job)
        changes = ({'nvmeof_target': 'spdk'}, {'nvmeof_route': 'live'},
                   {'nvmeof_route': None}, {'nvmeof_route': 'other'},
                   {'backend': 'native'}, {'backend': 'tidehunter_tcp'},
                   {'backend': 'tidehunter_nvme_tcp'}, {'group': 'G4'},
                   {'case_kind': 'interference'}, {'media': {'expected': 'memory'}},
                   {'media': {'expected': 'observe'}}, {'mix': cloud_campaign.mix(put=100)},
                   {'mix': cloud_campaign.mix(read=50, put=50)})
        for change in changes:
            with self.subTest(change=change), self.assertRaisesRegex(controller.SafetyError, 'direct target I/O'):
                controller.require_supported_nvme_route(job | change)
        missing_dataset = dict(job)
        del missing_dataset['sealed_dataset']
        with self.assertRaises(controller.SafetyError):
            controller.require_supported_nvme_route(missing_dataset)

    def connection(self, target_io, *, bad_policy=None, bad_binding=False):
        fixture = fixtures.ControllerBoundaryTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        c, root = fixture.c, fixture.root
        policy_target = 'kernel_direct' if target_io == 'direct' else 'kernel'
        expected = 0 if target_io == 'direct' else 1
        roots, targets = fixture.mirror_fixtures(root, [1, 2], {'catalog': [{}], 'nqn': 'test-nqn'})
        for owner in targets:
            target = owner['target']
            target['transport'] = 'rdma'
            entry = target['exports'][0]
            entry['target'] = policy_target
            entry['target_receipt'].update(selected_transport='rdma', buffered_io=expected,
                target_implementation=policy_target,
                target_cache=('direct_file_no_OS_pagecache' if target_io == 'direct'
                              else 'buffered_file_page_cache'))
            if bad_policy:
                entry['target_receipt'].update(bad_policy)
        c.shared_tide = {'job': {'case_id': 'source-case'}, 'ready': [{}] * 5,
            'mirror_roots': roots, 'options': {'external_remote_config': {
                'read_memory_allowance_bytes': 256 * 1024**2,
                'read_cache_bytes': 0, 'max_pending_read_bytes': 0}}}
        c.owner_batch = Mock(return_value=targets)
        result = {'phase': 'connected', 'transport': 'rdma',
                  'bindings': [{'export_uuid': [1, 2], 'namespace_path': '/dev/test-only',
                                'transport': 'rdma', 'target': 'kernel' if bad_binding else policy_target}],
                  'exports': [{'export_uuid': [1, 2], 'host_receipt': {'controller': 'nvme0'}}]}

        def mapped_path(value):
            path = Path(value)
            return root / 'control' if str(path) == '/var/lib/tidehunter-kv-control' else path

        def command(argv, *, timeout):
            return json.dumps({'cntlid': 7} if argv[:2] == ['nvme', 'id-ctrl'] else result)

        with patch.object(controller, 'Path', side_effect=mapped_path), \
             patch.object(controller, 'command', side_effect=command):
            readers = c.connect_nvme(self.job() | {'target_io': target_io}, root)
        return c, root, readers

    def test_reader_config_and_existing_target_evidence_bind_readback(self):
        for target_io, target, expected in (('direct', 'kernel_direct', 0), ('buffered', 'kernel', 1)):
            with self.subTest(target_io=target_io):
                c, root, readers = self.connection(target_io)
                self.assertEqual(c.owner_batch.call_args.kwargs['target_implementation'], target)
                self.assertEqual({r['binding']['target'] for r in readers}, {target})
                evidence = json.loads((root / 'nvme-targets.json').read_text())
                self.assertEqual(len(evidence), 5)
                for owner in evidence:
                    for entry in owner['target']['exports']:
                        self.assertEqual(entry['target_receipt']['buffered_io'], expected)
                        self.assertTrue(entry['target_receipt']['target_cache_verified'])

    def test_direct_mismatch_unknown_readback_and_binding_fail_closed(self):
        for policy in ({'buffered_io': 1}, {'buffered_io': None}, {'buffered_io': False},
                       {'target_cache_verified': False}, {'target_cache': 'buffered_file_page_cache'}):
            with self.subTest(policy=policy), self.assertRaises(controller.SafetyError):
                self.connection('direct', bad_policy=policy)
        with self.assertRaisesRegex(controller.SafetyError, 'binding target policy'):
            self.connection('direct', bad_binding=True)
        with self.assertRaises(controller.SafetyError):
            self.connection('buffered', bad_policy={'buffered_io': 0})


if __name__ == '__main__':
    unittest.main()
