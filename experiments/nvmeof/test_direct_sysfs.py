"""Bounded sysfs inventory regressions; no NVMe hardware is required."""
from contextlib import contextmanager, nullcontext
import errno
import itertools
import os
from pathlib import Path
import shutil
import tempfile
import subprocess
import unittest
from unittest.mock import patch

import manage


class SysfsInventoryTests(unittest.TestCase):
    @contextmanager
    def clock(self):
        with patch.object(manage.time, 'monotonic', side_effect=itertools.count(0, .01)), \
             patch.object(manage.time, 'sleep'):
            yield

    def controller(self, root, name):
        ctrl = root / name
        ctrl.mkdir()
        for key, value in (('subsysnqn', 'nqn.2026-01.test:' + name),
                           ('transport', 'rdma'), ('address', 'traddr=10.0.0.1,trsvcid=4420')):
            (ctrl / key).write_text(value)
        return ctrl

    def namespace(self, root):
        ctrl = self.controller(root, 'nvme1')
        subs = root / 'subsystems'
        subs.mkdir()
        subsystem = subs / 'nvme-subsys0'
        subsystem.mkdir()
        (subsystem / ctrl.name).symlink_to(ctrl)
        manifest = {'nqn': 'nqn.2026-01.test:nvme1', 'nsid': 1,
                    'uuid_string': '00010203-0405-0607-0809-0a0b0c0d0e0f'}
        (subsystem / 'subsysnqn').write_text(manifest['nqn'])
        blocks = root / 'block'
        blocks.mkdir()
        ns = blocks / 'nvme1n1'
        ns.mkdir()
        (ns / 'device').symlink_to(subsystem)
        for key, value in (('nsid', '1'), ('uuid', manifest['uuid_string']),
                           ('wwid', 'uuid.' + manifest['uuid_string'])):
            (ns / key).write_text(value)
        return ctrl, subs, subsystem, blocks, ns, manifest

    def test_transient_controller_attribute_enodev_returns_stable_identity(self):
        for attr in ('subsysnqn', 'transport', 'address'):
            with self.subTest(attr=attr), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                ctrl = self.controller(root, 'nvme1')
                original = Path.read_text
                reads = []

                def read(path, *args, **kwargs):
                    if path == ctrl / attr:
                        reads.append(path)
                        if len(reads) == 1:
                            raise OSError(errno.ENODEV, 'controller is deleting')
                    return original(path, *args, **kwargs)

                with patch.object(manage, 'SYS_NVME', root), patch.object(Path, 'read_text', read), self.clock():
                    result = manage.controller_snapshot(timeout=1)
                self.assertEqual(result['nvme1']['inode'], ctrl.stat().st_ino)
                self.assertEqual(len(reads), 3)

    def test_enodev_during_controller_enumeration_or_stat_restarts_complete_scan(self):
        for operation in ('scandir', 'stat'):
            for failure_at in (1, 2, 4):
                with self.subTest(operation=operation, failure_at=failure_at), tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    ctrl = self.controller(root, 'nvme1')
                    original = manage.os.scandir if operation == 'scandir' else Path.stat
                    calls = []

                    def raced(path, *args, **kwargs):
                        if path == (root if operation == 'scandir' else ctrl):
                            calls.append(path)
                            if len(calls) == failure_at:
                                raise OSError(errno.ENODEV, 'inventory changed')
                        return original(path, *args, **kwargs)

                    target = manage.os if operation == 'scandir' else Path
                    with patch.object(manage, 'SYS_NVME', root), \
                         patch.object(target, operation, raced), self.clock():
                        self.assertEqual(set(manage.controller_snapshot(timeout=1)), {'nvme1'})
                    # A failed scan cannot serve as either of the final two scans.
                    self.assertGreaterEqual(len(calls), failure_at + 4)

    def test_persistent_controller_enodev_and_enoent_never_prove_absence(self):
        for code in (errno.ENODEV, errno.ENOENT):
            for operation in ('scandir', 'read_text', 'stat'):
                with self.subTest(code=code, operation=operation), tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    self.controller(root, 'nvme1')
                    target = manage.os if operation == 'scandir' else Path
                    with patch.object(manage, 'SYS_NVME', root), \
                         patch.object(target, operation, side_effect=OSError(code, 'incomplete inventory')), \
                         self.clock(), self.assertRaisesRegex(manage.InventoryTimeout,
                                                              'controller inventory did not stabilize.*identity remains unknown') as failure:
                        manage.controller_snapshot(timeout=.05)
                    self.assertIsInstance(failure.exception, ValueError)

    def test_deleting_one_of_two_controllers_resets_prior_matching_inventory(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            live = self.controller(root, 'nvme1')
            deleting = self.controller(root, 'nvme2')
            original = Path.read_text
            live_reads, deleting_reads = [], []

            def read(path, *args, **kwargs):
                if path == live / 'address':
                    live_reads.append(path)
                if path == deleting / 'subsysnqn':
                    deleting_reads.append(path)
                    if len(deleting_reads) == 2:
                        shutil.rmtree(deleting)
                        raise OSError(errno.ENODEV, 'controller is deleting')
                return original(path, *args, **kwargs)

            with patch.object(manage, 'SYS_NVME', root), patch.object(Path, 'read_text', read), self.clock():
                self.assertEqual(set(manage.controller_snapshot(timeout=1)), {'nvme1'})
            self.assertEqual(len(live_reads), 4)
            self.assertEqual(len(deleting_reads), 2)

    def test_namespace_enodev_at_every_scan_stage_requires_two_complete_scans(self):
        for operation, location in (('stat', 'ctrl'), ('resolve', 'ctrl'),
                                    ('scandir', 'subs'), ('scandir', 'subsystem'),
                                    ('read_text', 'nqn'), ('scandir', 'blocks'),
                                    ('resolve', 'device'), ('read_text', 'nsid'),
                                    ('read_text', 'uuid'), ('read_text', 'wwid')):
            with self.subTest(operation=operation, location=location), tempfile.TemporaryDirectory() as tmp:
                ctrl, subs, subsystem, blocks, ns, manifest = self.namespace(Path(tmp))
                locations = {'ctrl': ctrl, 'subs': subs, 'subsystem': subsystem,
                             'nqn': subsystem / 'subsysnqn', 'blocks': blocks,
                             'device': ns / 'device', 'nsid': ns / 'nsid',
                             'uuid': ns / 'uuid', 'wwid': ns / 'wwid'}
                original = manage.os.scandir if operation == 'scandir' else getattr(Path, operation)
                calls, complete = [], []

                def raced(path, *args, **kwargs):
                    if path == locations[location]:
                        calls.append(path)
                        if len(calls) == 1:
                            raise OSError(errno.ENODEV, 'namespace is changing')
                    return original(path, *args, **kwargs)

                once = manage._namespace_candidates_once

                def scan(*args):
                    result = once(*args)
                    complete.append(result)
                    return result

                target = manage.os if operation == 'scandir' else Path
                with patch.object(manage, 'SYS_SUBSYSTEM', subs), patch.object(manage, 'SYS_BLOCK', blocks), \
                     patch.object(target, operation, raced), \
                     patch.object(manage, '_namespace_candidates_once', side_effect=scan), self.clock():
                    self.assertEqual(manage.namespace_candidates(ctrl, manifest, timeout=1), [ns])
                self.assertEqual(complete, [[ns], [ns]])

    def test_namespace_error_resets_stability_and_persistent_errors_fail_closed(self):
        path = Path('/fixture/nvme1n1')
        for code in (errno.ENODEV, errno.ENOENT):
            with self.subTest(code=code):
                with patch.object(manage, '_namespace_candidates_once',
                                  side_effect=[[path], OSError(code, 'changing'), [path], [path]]) as scan, self.clock():
                    self.assertEqual(manage.namespace_candidates(None, {}, timeout=1), [path])
                    self.assertEqual(scan.call_count, 4)
                with patch.object(manage, '_namespace_candidates_once', side_effect=OSError(code, 'changing')), \
                     self.clock(), self.assertRaisesRegex(manage.InventoryTimeout,
                                                          'did not stabilize.*identity remains unknown') as failure:
                    manage.namespace_candidates(None, {}, timeout=.05)
                self.assertIsInstance(failure.exception, ValueError)

    def test_other_inventory_errors_are_not_retried(self):
        for code in (errno.EACCES, errno.EIO):
            with self.subTest(code=code), self.clock():
                with patch.object(manage.os, 'scandir', side_effect=OSError(code, 'not transient')) as scan:
                    with self.assertRaises(OSError) as failure:
                        manage.controller_snapshot(timeout=1)
                    self.assertEqual(failure.exception.errno, code)
                    self.assertEqual(scan.call_count, 1)
                with patch.object(manage, '_namespace_candidates_once',
                                  side_effect=OSError(code, 'not transient')) as scan:
                    with self.assertRaises(OSError) as failure:
                        manage.namespace_candidates(None, {}, timeout=1)
                    self.assertEqual(failure.exception.errno, code)
                    self.assertEqual(scan.call_count, 1)


class ControllerDeleteTests(unittest.TestCase):
    def fixture(self, root):
        ctrl = root / 'nvme1'
        ctrl.mkdir()
        attrs = {'subsysnqn': 'nqn.2026-01.test:owned', 'hostnqn': 'nqn.2026-01.test:host',
                 'transport': 'rdma', 'address': 'traddr=10.0.0.1,trsvcid=4420'}
        for key, value in attrs.items():
            (ctrl / key).write_text(value)
        (ctrl / 'delete_controller').write_bytes(b'0')
        identity = {'nqn': attrs['subsysnqn'], 'transport': attrs['transport'],
                    'address': attrs['address'], 'inode': ctrl.stat().st_ino}
        state = {'controller': ctrl.name, 'nqn': identity['nqn'], 'transport': identity['transport'],
                 'controller_inode': identity['inode'], 'controller_address': identity['address'],
                 'hostnqn': attrs['hostnqn']}
        return ctrl, identity, state

    def test_delete_verifies_every_identity_field_before_writing(self):
        for changed in ('inode', 'subsysnqn', 'hostnqn', 'transport', 'address'):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as tmp:
                ctrl, identity, state = self.fixture(Path(tmp))
                if changed == 'inode':
                    identity['inode'] += 1
                else:
                    (ctrl / changed).write_text('not-the-recorded-identity')
                with patch.object(manage, 'SYS_NVME', Path(tmp)), self.assertRaisesRegex(ValueError, 'changed'):
                    manage.delete_controller(ctrl.name, identity, {'hostnqn': state['hostnqn']})
                self.assertEqual((ctrl / 'delete_controller').read_bytes(), b'0')

    def test_delete_uses_only_the_opened_directory_when_name_is_replaced(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ctrl, identity, state = self.fixture(root)
            original_open = os.open
            opened = []

            def replace(path, flags, *args, **kwargs):
                fd = original_open(path, flags, *args, **kwargs)
                if path == ctrl:
                    opened.append(fd)
                    ctrl.rename(root / 'original')
                    self.fixture(root)
                return fd

            with patch.object(manage, 'SYS_NVME', root), patch.object(manage.os, 'open', replace):
                manage.delete_controller(ctrl.name, identity, {'hostnqn': state['hostnqn']})
            self.assertEqual(len(opened), 1)
            self.assertEqual((root / 'original/delete_controller').read_bytes(), b'1')
            self.assertEqual((ctrl / 'delete_controller').read_bytes(), b'0')

    def test_vanished_object_is_benign_but_other_errors_propagate(self):
        for code in (errno.ENOENT, errno.ENODEV, errno.EACCES, errno.EIO):
            for stage in ('directory', 'fstat', 'subsysnqn', 'hostnqn', 'transport',
                          'address', 'delete_controller', 'write'):
                with self.subTest(code=code, stage=stage), tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    ctrl, identity, state = self.fixture(root)
                    original_open = os.open

                    def opening(path, *args, **kwargs):
                        if path == (ctrl if stage == 'directory' else stage):
                            raise OSError(code, 'injected controller error')
                        return original_open(path, *args, **kwargs)

                    with patch.object(manage, 'SYS_NVME', root), patch.object(manage.os, 'open', opening):
                        target = ('fstat' if stage == 'fstat' else 'write' if stage == 'write' else None)
                        if target:
                            mutation = patch.object(manage.os, target, side_effect=OSError(code, 'injected error'))
                        else:
                            mutation = nullcontext()
                        with mutation:
                            if code in (errno.ENOENT, errno.ENODEV):
                                manage.delete_controller(ctrl.name, identity, {'hostnqn': state['hostnqn']})
                            else:
                                with self.assertRaises(OSError) as failure:
                                    manage.delete_controller(ctrl.name, identity, {'hostnqn': state['hostnqn']})
                                self.assertEqual(failure.exception.errno, code)
                    self.assertEqual((ctrl / 'delete_controller').read_bytes(), b'0')

    def test_cli_signal_failure_retries_sysfs_once_and_requires_final_absence(self):
        for outcome in ('retired', 'already_gone', 'write_failure', 'still_present', 'replaced'):
            with self.subTest(outcome=outcome), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                ctrl, _, state = self.fixture(root)
                original_write = os.write
                attempts = []

                def cli(argv, **kwargs):
                    if outcome == 'already_gone':
                        shutil.rmtree(ctrl)
                    elif outcome == 'replaced':
                        ctrl.rename(root / 'old')
                        self.fixture(root)
                    raise subprocess.CalledProcessError(-11, argv, stderr='segmentation fault')

                def write(fd, data):
                    attempts.append(data)
                    if outcome == 'write_failure':
                        raise OSError(errno.EIO, 'delete write failed')
                    result = original_write(fd, data)
                    if outcome == 'retired':
                        shutil.rmtree(ctrl)
                    return result

                with patch.object(manage, 'SYS_NVME', root), \
                     patch.object(manage.subprocess, 'run', side_effect=cli) as command, \
                     patch.object(manage.os, 'write', side_effect=write):
                    if outcome in ('retired', 'already_gone'):
                        manage.disconnect(state)
                    elif outcome == 'write_failure':
                        with self.assertRaises(OSError):
                            manage.disconnect(state)
                    else:
                        with self.assertRaisesRegex(ValueError, r'command failed \(-11\)'):
                            manage.disconnect(state)
                    self.assertEqual(command.call_count, 1)
                self.assertEqual(len(attempts), 0 if outcome in ('already_gone', 'replaced') else 1)

    def test_retry_cannot_retire_a_different_controller_with_the_same_nqn(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ctrl, _, state = self.fixture(root)
            original_write = os.write

            def write(fd, data):
                result = original_write(fd, data)
                ctrl.rename(root / 'nvme2')
                return result

            with patch.object(manage, 'SYS_NVME', root), patch.object(manage.os, 'write', side_effect=write), \
                 patch.object(manage, 'run', side_effect=ValueError('CLI failed')):
                with self.assertRaisesRegex(ValueError, 'CLI failed'):
                    manage.disconnect(state)


if __name__ == '__main__':
    unittest.main()
