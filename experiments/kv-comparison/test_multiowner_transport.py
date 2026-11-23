"""Prove that an owner launch validates against the job that planned it.

The local four-owner proof reuses the reviewed colocated topology of an RDMA
campaign case, but it plans a TCP control transport. The reviewed case plans
OFI control. The launch must match its own plan, not the reviewed case.
"""
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

import yaml

import cloud_campaign
import cloud_controller as cloud


def local_config(root):
    """Build the local functional case that local_multiowner_check plans."""
    topology = next(job for job in cloud_campaign.make_focused_matrix('nvmeof_m3_t1')
                    if job.get('owner_count') == 16 and job.get('owner_topology') == 'colocated')
    case = topology['case_id']
    job = dict(topology)
    job.update(owner_count=4, owner_topology='colocated')
    for key in ('case_kind', 'measurement_sizing', 'device_counters', 'thread_cpu_sampling'):
        job.pop(key, None)
    job.update(selection='local_functional', backend='tidehunter_nvme_tcp', keys=4,
               warmup_operations=0, measurement_operations=4, concurrency=4,
               owner_wal_geometry='research', owner_wal_file_bytes=8388608,
               owner_read_memory_allowance_bytes=128 * 1024**2)
    config = {'prefix': 'th-0123456789abcdef', 'binary': '/opt/source/runtime/bin/kv-research',
              'source_revision': 'local-functional-unsealed', 'repository_root': '/opt/source',
              'fdb_library_dir': '/opt/source/runtime/lib',
              'deadline_utc': '2099-01-01T02:00:00Z',
              'fleet': {'run_id': 'th-0123456789abcdef', 'data_root': str(root),
                        'coordinator': {'instance_id': 'local-client', 'address': '127.0.0.1'},
                        'nodes': [{'index': 0, 'instance_id': 'local-owner-host',
                                   'address': '127.0.0.1'}],
                        'client_cpus': 4, 'client_memory_mib': 2048,
                        'server_cpus': 4, 'server_memory_mib': 8192},
              'spec': {'mode': 'standard-with-live-nvmeof', 'selection': 'nvmeof_m3_t1',
                       'backends': ['tidehunter_nvme_tcp', 'tidehunter_nvme_rdma'], 'hard_deadline_seconds': 600,
                       'case_deadline_seconds': 180, 'export_reserve_seconds': 120},
              'jobs': [job]}
    return config, case, job


def owner_launch(config, case, index, transport):
    """One host-schema launch with the exact paths and pool of this case."""
    node = Path(config['fleet']['data_root']) / 'cells' / case / f'node-{index}'
    tls = {'transport': transport, 'server_name': f'tidehunter-node-{index}'}
    if cloud.is_trusted(transport):
        tls.update(ca='', cert='', key='')
    else:
        tls.update(ca=str(node / 'tls' / 'ca.der'),
                   cert=str(node / 'tls' / f'tidehunter-node-{index}.der'),
                   key=str(node / 'tls' / f'tidehunter-node-{index}.pk8'))
    pool_transport = 'rdma' if cloud.protocol_key(transport)[0] == 'ofi' else 'tcp'
    return {'partition': index,
            'host': {'root': str(node / 'storage'), 'max_databases': 1,
                     'live_target_pool': cloud.live_target_pool_for_owner(
                         config, case, index, pool_transport),
                     'wal_prewrite': False},
            'tls': tls, 'limits': {'polling': 'adaptive'},
            'listen': f'127.0.0.1:{9400 + index}',
            'ready_file': str(node / 'ready'), 'stop_file': str(node / 'stop')}


class LocalPlanTests(unittest.TestCase):
    def test_local_functional_launch_passes_against_its_own_plan(self):
        with tempfile.TemporaryDirectory() as temporary:
            config, case, job = local_config(Path(temporary))
            launch = owner_launch(config, case, 0, {'kind': 'tcp'})
            reviewed = cloud.case_owner_job(config, case)
            self.assertEqual(cloud.protocol_key(
                cloud.owner_control_settings(job)['owner_transport']), ('tcp',))
            self.assertEqual(cloud.protocol_key(
                cloud.owner_control_settings(reviewed)['owner_transport']),
                ('ofi', 'verbs', 'trusted'))
            cloud.validate_launch(config, launch, case, 0, planning_job=job)
            with self.assertRaisesRegex(cloud.SafetyError,
                    'typed transport differs from the planned leased owner control protocol'):
                cloud.validate_launch(config, launch, case, 0)

    def test_planning_job_must_name_the_case(self):
        with tempfile.TemporaryDirectory() as temporary:
            config, case, job = local_config(Path(temporary))
            with self.assertRaisesRegex(cloud.SafetyError, 'planning job names a different case'):
                cloud.validate_launch(config, owner_launch(config, case, 0, {'kind': 'tcp'}),
                                      case, 0, planning_job=dict(job, case_id='other-case'))

    def test_reviewed_case_still_rejects_a_launch_that_differs_from_its_plan(self):
        with tempfile.TemporaryDirectory() as temporary:
            config, case, _ = local_config(Path(temporary))
            reviewed = cloud.case_owner_job(config, case)
            verbs = {'kind': 'ofi', 'fabric': {'backend': 'verbs'}, 'security': 'trusted'}
            cloud.validate_launch(config, owner_launch(config, case, 0, verbs), case, 0,
                                  planning_job=reviewed)
            with self.assertRaisesRegex(cloud.SafetyError,
                    'typed transport differs from the planned leased owner control protocol'):
                cloud.validate_launch(config, owner_launch(config, case, 0, {'kind': 'tcp'}),
                                      case, 0, planning_job=reviewed)


class PrepareTidePlanTests(unittest.TestCase):
    def test_prepare_tide_validates_against_the_job_that_planned_the_launch(self):
        with tempfile.TemporaryDirectory() as temporary:
            config, case, job = local_config(Path(temporary))
            directory = Path(temporary) / 'case'
            directory.mkdir()
            runtime = cloud.Controller.__new__(cloud.Controller)
            runtime.config, runtime.clients = config, {}
            runtime.sequence_lock = threading.Lock()
            runtime.active_tide = None
            runtime.remaining = lambda: 300
            runtime.request = Mock(return_value={'receipt': 'proof'})

            def prepare(_argv, **_kwargs):
                prepared = directory / 'prepared'
                prepared.mkdir()
                (prepared / 'remote.json').write_text(json.dumps({
                    'tls': {}, 'live_nvme': {},
                    'storage': {'fragment_bytes': 4194304, 'wal_file_bytes': 8388608}}))
                for index in range(job['owner_count']):
                    node = prepared / f'node-{index}'
                    node.mkdir()
                    (node / 'tls').mkdir()
                    for name in ('ca.der', f'tidehunter-node-{index}.der',
                                 f'tidehunter-node-{index}.pk8'):
                        (node / 'tls' / name).write_bytes(b'')
                    (node / 'launch.yaml').write_text(yaml.safe_dump(
                        owner_launch(config, case, index, {'kind': 'tcp'})))

            with patch.object(cloud, 'command', side_effect=prepare), \
                 patch.object(cloud, 'validate_launch') as validate, \
                 patch.object(cloud, 'admit_prepared_live_client', return_value='host-nqn'), \
                 patch.object(cloud, 'upload_object'), \
                 patch.object(cloud, 'confirm_owner_durability',
                              side_effect=lambda proof, tier, index, nonce: {'partition': index}):
                runtime.prepare_tide(job, directory)
            self.assertEqual(len(validate.call_args_list), job['owner_count'])
            for call in validate.call_args_list:
                self.assertIs(call.kwargs['planning_job'], job)


class LocalModePlanTests(unittest.TestCase):
    def test_local_mode_selects_the_job_that_names_the_case(self):
        from local_dry_run import LocalMode
        with tempfile.TemporaryDirectory() as temporary:
            config, case, job = local_config(Path(temporary))
            mode = LocalMode.__new__(LocalMode)
            mode.config = config
            self.assertIs(mode.planning_job(case), job)
            self.assertIsNone(mode.planning_job('other-case'))


if __name__ == '__main__':
    unittest.main()
