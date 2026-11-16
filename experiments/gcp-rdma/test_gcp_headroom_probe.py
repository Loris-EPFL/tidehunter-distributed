import errno
import os
import shutil
import tempfile
import subprocess
import json
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import gcp_headroom_probe as probe

RUN = 'bench-0123456789abcdef'


class HeadroomTests(unittest.TestCase):
    def test_unique_owner_names(self):
        host, first = probe.identities(RUN, 1)
        other_host, last = probe.identities(RUN, 5)
        self.assertEqual(host, other_host)
        self.assertEqual(len(set(first + last)), 90)
        with self.assertRaises(ValueError):
            probe.identities('not-a-run', 1)

    def test_existing_port_is_never_adopted(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / 'nvmet'
            for name in ('ports', 'hosts', 'subsystems'):
                (config / name).mkdir(parents=True)
            (config / 'ports' / str(probe.PORT_ID)).mkdir()
            with self.assertRaisesRegex(ValueError, 'already exists'):
                probe.create_targets(root / 'fixture', config, RUN, 1, '10.0.0.1')
            self.assertTrue((config / 'ports' / str(probe.PORT_ID)).is_dir())
            self.assertFalse((root / 'fixture').exists())

    def test_cleanup_refuses_missing_journal(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / 'nvmet'
            for name in ('ports', 'hosts', 'subsystems'):
                (config / name).mkdir(parents=True)
            (config / 'ports' / str(probe.PORT_ID)).mkdir()
            with self.assertRaisesRegex(ValueError, 'without ownership journal'):
                probe.cleanup_targets(root / 'fixture', config, RUN, 1)

    def test_cleanup_journal_removes_only_owned_objects(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config, fixture = root / 'nvmet', root / 'fixture'
            for name in ('ports', 'hosts', 'subsystems'):
                (config / name).mkdir(parents=True)
            fixture.mkdir()
            hostnqn, names = probe.identities(RUN, 1)
            host = config / 'hosts' / hostnqn
            port = config / 'ports' / str(probe.PORT_ID)
            sub = config / 'subsystems' / names[0]
            host.mkdir()
            (port / 'subsystems').mkdir(parents=True)
            (sub / 'allowed_hosts').mkdir(parents=True)
            namespace = sub / 'namespaces' / '1'
            namespace.mkdir(parents=True)
            (namespace / 'enable').write_text('1')
            hostlink = sub / 'allowed_hosts' / hostnqn
            portlink = port / 'subsystems' / names[0]
            hostlink.symlink_to(host)
            portlink.symlink_to(sub)
            objects = [host, port, sub, hostlink, namespace, portlink]
            state = {'run_id': RUN, 'owner': 1, 'count': 45, 'hostnqn': hostnqn,
                     'objects': [{'path': str(p), 'identity': probe._identity(p)} for p in objects]}
            (fixture / 'state.json').write_text(json.dumps(state))
            (fixture / '00.wal').write_bytes(b'probe')
            original_rmdir = Path.rmdir
            def configfs_rmdir(path):
                if path == namespace:
                    (path / 'enable').unlink()
                elif path == sub:
                    (path / 'allowed_hosts').rmdir()
                    (path / 'namespaces').rmdir()
                elif path == port:
                    (path / 'subsystems').rmdir()
                original_rmdir(path)
            with patch.object(Path, 'rmdir', configfs_rmdir):
                self.assertTrue(probe.cleanup_targets(fixture, config, RUN, 1)['clean'])
            self.assertFalse(fixture.exists())
            self.assertFalse(port.exists())

    def test_resource_sample_requires_all_four_counts(self):
        evidence = {'resources': {'available': True, 'devices': [
            {'device': 'irdma0', 'counts': {'mr': 1, 'qp': 2, 'cq': 3, 'pd': 4}}]}}
        with patch.object(probe.rdma_diagnostics, 'capture_resources', return_value=evidence):
            self.assertEqual(probe.resource_sample('irdma0', 15)['controllers'], 15)
            del evidence['resources']['devices'][0]['counts']['mr']
            with self.assertRaisesRegex(ValueError, 'counts unavailable'):
                probe.resource_sample('irdma0', 15)

    def test_threshold_and_complete_controller_retirement(self):
        for ceiling, admitted in ((134, False), (135, True), (225, True)):
            with self.subTest(ceiling=ceiling):
                controllers = {}
                def connect(argv, **kwargs):
                    if len(controllers) == ceiling:
                        raise subprocess.CalledProcessError(1, argv, stderr=b'ENOMEM')
                    name = argv[argv.index('-n') + 1]
                    address = argv[argv.index('-a') + 1]
                    index = len(controllers)
                    controllers['nvme' + str(index)] = {
                        'nqn': name, 'transport': 'rdma',
                        'address': 'traddr=' + address + ',trsvcid=' + str(probe.SERVICE),
                        'inode': index + 1}
                def retire(expected, hostnqn):
                    count = len(controllers)
                    controllers.clear()
                    return count
                with patch.object(probe.manage, 'controller_snapshot', side_effect=lambda **_: dict(controllers)), \
                     patch.object(probe.manage, 'verify_queue_geometry'), \
                     patch.object(probe, 'retire_controllers', side_effect=retire), \
                     patch.object(probe, 'resource_sample', side_effect=lambda device, count: {'controllers': count}), \
                     patch.object(probe.subprocess, 'run', side_effect=connect):
                    result = probe.probe(RUN, ['10.0.0.' + str(i) for i in range(1, 6)], 'irdma0')
                self.assertEqual(result['connected'], ceiling)
                self.assertEqual(result['admitted'], admitted)
                self.assertTrue(result['cleanup_complete'])
                self.assertFalse(controllers)
                if ceiling < 225:
                    self.assertIn('ENOMEM', result['stderr'])

    def test_every_reviewed_owner_count_can_reach_the_threshold(self):
        for owners in probe.OWNER_HOST_COUNTS:
            with self.subTest(owners=owners):
                per_owner = probe.targets_per_owner(owners)
                self.assertGreaterEqual(per_owner * owners, probe.MIN_CONTROLLERS)
                self.assertLessEqual(per_owner * owners, probe.MAX_CONTROLLERS)
                controllers = {}
                def connect(argv, **kwargs):
                    index = len(controllers)
                    controllers['nvme' + str(index)] = {
                        'nqn': argv[argv.index('-n') + 1], 'transport': 'rdma',
                        'address': 'traddr=' + argv[argv.index('-a') + 1] + ',trsvcid=' + str(probe.SERVICE),
                        'inode': index + 1}
                def retire(expected, hostnqn):
                    count = len(controllers)
                    controllers.clear()
                    return count
                with patch.object(probe.manage, 'controller_snapshot', side_effect=lambda **_: dict(controllers)), \
                     patch.object(probe.manage, 'verify_queue_geometry'), \
                     patch.object(probe, 'retire_controllers', side_effect=retire), \
                     patch.object(probe, 'resource_sample', side_effect=lambda device, count: {'controllers': count}), \
                     patch.object(probe.subprocess, 'run', side_effect=connect):
                    result = probe.probe(RUN, ['10.0.0.' + str(i) for i in range(1, owners + 1)], 'irdma0')
                self.assertEqual(result['connected'], per_owner * owners)
                self.assertTrue(result['admitted'])
        with self.assertRaisesRegex(ValueError, 'unreviewed owner count'):
            probe.targets_per_owner(3)


class ProbeRetirementTests(unittest.TestCase):
    def fixture(self, root):
        hostnqn, names = probe.identities(RUN, 1, 3)
        expected = [(name, '10.0.0.1') for name in names]
        controllers = []
        for index, name in enumerate(names):
            ctrl = root / f'nvme{index}'
            ctrl.mkdir()
            for attribute, value in (('subsysnqn', name), ('hostnqn', hostnqn), ('transport', 'rdma'),
                                     ('address', f'traddr=10.0.0.1,trsvcid={probe.SERVICE}')):
                (ctrl / attribute).write_text(value)
            (ctrl / 'delete_controller').write_bytes(b'0')
            controllers.append(ctrl)
        return hostnqn, expected, controllers

    def test_all_owned_controllers_retire_by_sysfs_without_cli(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            hostnqn, expected, controllers = self.fixture(root)
            original_write = os.write
            deleted = []

            def write(fd, data):
                path = Path(os.readlink(f'/proc/self/fd/{fd}'))
                result = original_write(fd, data)
                self.assertEqual(path.name, 'delete_controller')
                self.assertEqual(data, b'1')
                deleted.append(path.parent.name)
                shutil.rmtree(path.parent)
                return result

            with patch.object(probe.manage, 'SYS_NVME', root), \
                 patch.object(probe.manage.os, 'write', side_effect=write), \
                 patch.object(probe.subprocess, 'run') as cli:
                self.assertEqual(probe.retire_controllers(expected, hostnqn), len(controllers))
                cli.assert_not_called()
            self.assertEqual(set(deleted), {p.name for p in controllers})

    def test_changed_identity_is_not_deleted_and_error_is_retained(self):
        for field in ('inode', 'subsysnqn', 'hostnqn', 'transport', 'address'):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                hostnqn, expected, controllers = self.fixture(root)
                original_snapshot = probe.manage.controller_snapshot
                snapshots = []

                def snapshot(**kwargs):
                    result = original_snapshot(**kwargs)
                    if not snapshots:
                        if field == 'inode':
                            result['nvme0']['inode'] += 1
                        else:
                            (controllers[0] / field).write_text('changed-identity')
                    snapshots.append(result)
                    return result

                with patch.object(probe.manage, 'SYS_NVME', root), \
                     patch.object(probe.manage, 'controller_snapshot', side_effect=snapshot):
                    with self.assertRaisesRegex(ValueError, 'unconfirmed.*identity changed|unconfirmed.*inode changed'):
                        probe.retire_controllers(expected, hostnqn)
                self.assertEqual((controllers[0] / 'delete_controller').read_bytes(), b'0')

    def test_vanished_controller_never_excuses_another_live_controller(self):
        for code in (errno.ENOENT, errno.ENODEV):
            for other_stays in (False, True):
                with self.subTest(code=code, other_stays=other_stays), tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    hostnqn, expected, controllers = self.fixture(root)
                    original_open, original_write = os.open, os.write
                    writes, vanished = [], []

                    def opening(path, *args, **kwargs):
                        if path == controllers[0] and not vanished:
                            vanished.append(True)
                            shutil.rmtree(path)
                            raise OSError(code, 'controller vanished')
                        return original_open(path, *args, **kwargs)

                    def write(fd, data):
                        ctrl = Path(os.readlink(f'/proc/self/fd/{fd}')).parent
                        writes.append(ctrl)
                        result = original_write(fd, data)
                        if not other_stays:
                            shutil.rmtree(ctrl)
                        return result

                    with patch.object(probe.manage, 'SYS_NVME', root), \
                         patch.object(probe.manage.os, 'open', side_effect=opening), \
                         patch.object(probe.manage.os, 'write', side_effect=write):
                        if other_stays:
                            with self.assertRaisesRegex(ValueError, 'retirement unconfirmed'):
                                probe.retire_controllers(expected, hostnqn)
                        else:
                            self.assertEqual(probe.retire_controllers(expected, hostnqn), 3)
                    self.assertEqual(set(writes), set(controllers[1:]))


class HeadroomWiringTests(unittest.IsolatedAsyncioTestCase):
    async def test_below_threshold_cleans_targets_before_preventing_campaign_submission(self):
        import manual_campaign as manual

        submissions = []
        class Mailbox:
            def __init__(self, *args):
                pass

            def submit(self, index, argv, *, operation_id, **kwargs):
                submissions.append(operation_id)
                return {'index': index, 'action': argv[2]}

            def wait(self, reference, **kwargs):
                action = reference['action']
                if action == 'probe':
                    body = {'admitted': False, 'connected': 134, 'cleanup_complete': True,
                            'teardown_started_unix': time.time()}
                    return {'exit_code': 1, 'stdout': json.dumps(body)}
                return {'exit_code': 0, 'stdout': json.dumps({'clean': True})}

        async def ready(*args, **kwargs):
            return {'_reference': {'object': 'ready', 'generation': '1', 'sha256': 'a' * 64}}

        fleet = [{'name': f'host-{i}', 'id': str(i + 1), 'rdma_ip': f'10.0.0.{i + 1}'} for i in range(6)]
        state = {'run_id': RUN, 'source_sha256': 'a' * 64, 'deadline': manual._utc(time.time() + 7200),
                 'policy': {'name': 'policy', 'id': '1'}}
        with tempfile.TemporaryDirectory() as tmp, \
             patch.object(manual.gcp_runtime, 'build_runtime_config', return_value={'package_root': '/package'}), \
             patch.object(manual.gcp_runtime, 'RemoteMailbox', Mailbox), \
             patch.object(manual, '_wait_status', side_effect=ready), \
             patch.object(manual, '_admit_nvmet_proof'), \
             patch.object(manual, '_put_json'), patch.object(manual.campaign, 'save'):
            with self.assertRaisesRegex(RuntimeError, '135 controllers'):
                await manual._suite(object(), {'bucket': 'bucket'}, state, object(), fleet,
                                    {'manifest_sha256': 'b' * 64}, {}, tmp, 'irdma0')
        self.assertFalse(state.get('campaign_submitted', False))
        self.assertNotIn('campaign', submissions)
        self.assertEqual({f'headroom-cleanup-{i}' for i in range(1, 6)} & set(submissions),
                         {f'headroom-cleanup-{i}' for i in range(1, 6)})


if __name__ == '__main__':
    unittest.main()
