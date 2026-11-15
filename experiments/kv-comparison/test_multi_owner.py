"""Logical-owner identities remain distinct when physical hosts are shared."""
import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import cloud_campaign as campaign
import cloud_controller as controller


class MultiOwnerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.jobs = [job for selection in campaign.M3_SELECTIONS for job in campaign.make_focused_matrix(selection)]

    def test_reviewed_matrix(self):
        self.assertEqual(len(self.jobs), 30)
        self.assertEqual(len({job['case_id'] for job in self.jobs}), 30)
        self.assertEqual({(j['owner_topology'], j['owner_count']) for j in self.jobs},
                         {('colocated', 4), ('colocated', 16), ('separate', 2), ('separate', 5)})
        for job in self.jobs:
            campaign.validate_focused_job(job)
            self.assertTrue(job['thread_cpu_sampling'])
            self.assertIn(job['concurrency'], (16, 64, 256))
            self.assertEqual(campaign.active_owner_count(job), job['owner_count'])
            self.assertEqual(campaign.job_owner_hosts(job),
                             1 if job['owner_topology'] == 'colocated' else job['owner_count'])

    def test_unreviewed_owner_count_fails_closed(self):
        job = copy.deepcopy(self.jobs[0])
        job['owner_count'] = 2
        with self.assertRaises(ValueError):
            campaign.active_owner_count(job)

    def test_physical_fleet_selects_only_its_topology(self):
        for hosts, expected, selection in ((1, 11, "nvmeof_m3_t1"), (2, 8, "nvmeof_m3_t2_2"), (5, 11, "nvmeof_m3_t2_5")):
            jobs = controller.controller_matrix({'mode': 'focused',
                'selection': selection, 'owner_hosts': hosts})
            self.assertEqual(len(jobs), expected)
            self.assertTrue(all(campaign.job_owner_hosts(job) == hosts for job in jobs))

    def test_repeated_hosts_keep_logical_indices(self):
        config = {'fleet': {'nodes': [{'index': 0, 'address': '10.0.0.1'}]}}
        job = next(j for j in self.jobs if j['owner_count'] == 16)
        self.assertEqual(len(controller.logical_owner_hosts(config, job)), 16)
        self.assertEqual(controller.owner_host_index(config, job, 15), 0)
        with patch.object(controller, 'unit_name', side_effect=lambda c, case, role: role), patch.object(
                controller, 'owner_slice_limits', return_value=('kvowners123.slice', '')), patch.object(
                controller, 'unit_status', side_effect=lambda name: {'ControlGroup': '/kvowners123.slice/' + name}):
            observed = controller.owned_monitor_processes(config, 0, job)
        self.assertEqual([p['role'] for p in observed['processes']],
                         ['storage-' + str(i) for i in range(16)])
        self.assertEqual([p['cgroup'] for p in observed['processes']],
                         ['/kvowners123.slice/storage-' + str(i) + '.service' for i in range(16)])
        with patch.object(controller, 'unit_name', return_value='owner'), patch.object(
                controller, 'owner_slice_limits', return_value=('kvowners123.slice', '')), patch.object(
                controller, 'unit_status', return_value={'ControlGroup': ''}):
            missing = controller.owned_monitor_processes(config, 0, job)
        self.assertEqual(missing['processes'], [])
        self.assertEqual(len(missing['missing']), 16)
        with patch.object(controller, 'unit_name', return_value='owner'), patch.object(
                controller, 'unit_status', side_effect=controller.InfrastructureError('cannot observe unit')):
            missing = controller.owned_monitor_processes(config, 0, job)
        self.assertEqual(missing['processes'], [])
        self.assertEqual(len(missing['missing']), 16)
        self.assertIn('cannot observe unit', missing['missing'][0]['reason'])

    def test_short_admission_connects_writes_and_reads_each_slot(self):
        job = next(j for j in self.jobs if j['owner_count'] == 4 and j['owner_topology'] == 'colocated')
        identities = {str(i): {'host_nqn': 'owner-' + str(i), 'host_id': [i + 1] * 16} for i in range(4)}
        options = {'external_remote_config': {'storage': {'partitions': list(range(4))},
            'endpoints': [{'partition': i, 'address': '10.0.0.1:' + str(9400 + i)} for i in range(4)],
            'live_nvme': {'owner_identities': identities}}, 'external_owner_read_config': [
                {'partition': i} for i in range(4)], 'external_owner_durability_receipts': ['receipt-' + str(i) for i in range(4)]}
        instance = SimpleNamespace(config={'binary': '/binary', 'source_revision': 'a' * 40,
            'fleet': {'nodes': [{'index': 0}]}}, remaining=lambda: 3600)
        configs = []
        def run(argv, **kwargs):
            config = json.loads(Path(argv[2]).read_text())
            configs.append(config)
            owner = config['backend']['options']['external_remote_config']['storage']['partitions'][0]
            output = Path(argv[-1])
            output.mkdir()
            (output / 'summary.json').write_text(json.dumps({'status': 'completed'}))
            (output / 'samples.csv').write_text('operation,outcome\nread,ok\n')
            (output / 'measurement-read-evidence.json').write_text(json.dumps({'end': {'live_nvmeof': {
                'completed_read_ops': 1, 'completed_write_ops': 1, 'errors': 0, 'payload_rpc_requests': 0,
                'routes': [{'host_nqn': identities[str(owner)]['host_nqn']}]}}}))
        with tempfile.TemporaryDirectory() as tmp, patch.object(controller, 'command', side_effect=run), patch.object(controller, 'environment', return_value={}):
            controller.Controller.admit_logical_owners(instance, job, Path(tmp), options)
            proofs = json.loads((Path(tmp) / 'owner-admission.json').read_text())
        self.assertEqual(len(proofs['owners']), 4)
        configs.sort(key=lambda config: config["backend"]["options"]["external_remote_config"]["storage"]["partitions"][0])
        for index, config in enumerate(configs):
            self.assertFalse(config['qualification_only'])
            self.assertTrue(config['phases']['preload'])
            self.assertEqual(config['phases']['measurement_operations'], 1)
            self.assertNotIn('measurement_gate', config)
            selected = config['backend']['options']
            self.assertEqual(selected['owners'], 1)
            self.assertEqual(selected['coordinator_read_cache_bytes'], 0)
            self.assertEqual(selected['external_remote_config']['storage']['partitions'], [index])
        self.assertEqual(options['external_remote_config']['storage']['partitions'], list(range(4)))

    def test_real_unit_names_and_shared_host_caps(self):
        config = {'prefix': 'bench-' + '0' * 16, 'repository_root': '/source',
                  'spec': {'selection': 'nvmeof_m3_t1'},
                  'fleet': {'nodes': [{'index': 0}], 'server_cpus': 8, 'server_memory_mib': 16384}}
        job = next(j for j in self.jobs if j['owner_count'] == 16)
        self.assertTrue(controller.unit_name(config, job['case_id'], 'storage-15').endswith('-storage-15'))
        with self.assertRaises(controller.SafetyError):
            controller.unit_name(config, job['case_id'], 'storage-16')
        budgets = [controller.owner_process_budget(config, job, i) for i in range(16)]
        self.assertEqual({cpu for cpu, _ in budgets}, {8})
        self.assertEqual(sum(memory for _, memory in budgets), 16384)
        name, limits = controller.owner_slice_limits(config, job['case_id'])
        self.assertRegex(name, r'^kvowners[0-9a-f]{24}\.slice$')
        self.assertNotEqual(name, controller.owner_slice_limits(config, job['case_id'] + 'x')[0])
        self.assertIn('CPUQuota=800%', limits)
        self.assertIn('MemoryMax=16384M', limits)
        with patch.object(controller, 'environment', return_value={}):
            argv = controller.systemd_argv(config, job['case_id'], 'storage-15', 8, 1024, 600, ['/binary'])
        self.assertIn('--property=Slice=' + name, argv)
        self.assertIn('--property=MemoryMax=1024M', argv)
        self.assertFalse(any(arg.startswith('--property=CPUQuota=') for arg in argv))


if __name__ == '__main__':
    unittest.main()
