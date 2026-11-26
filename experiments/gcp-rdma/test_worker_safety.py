import json
import concurrent.futures
import sqlite3
import tempfile
import hashlib
from pathlib import Path
import threading
import unittest
from unittest import mock

import gcp_bootstrap
import gcp_worker as worker
import gcp_runtime

import workflow_input
FORMATTED_STORAGE = {'device': '/dev/nvme7n1', 'mkfs_duration_seconds': 2.5,
                     'lazy_initialization_disabled': True, 'inode_ratio_bytes': 4194304, 'inode_count': 1024}


class FakePath:
    files = {}
    links = {'/dev/disk/by-id/google-local-nvme-ssd-4': '/dev/nvme7n1',
             '/dev/disk/by-id/google-boot': '/dev/nvme0n1'}

    def __init__(self, value):
        self.value = str(value)

    def __truediv__(self, value):
        return FakePath(self.value + '/' + str(value))

    def __str__(self):
        return self.value
    @property
    def name(self):
        return self.value.rsplit('/', 1)[-1]

    def glob(self, pattern):
        return [FakePath(key) for key in self.links
                if key.startswith(self.value + '/google-local-nvme-ssd-')]


    def __eq__(self, other):
        return isinstance(other, FakePath) and self.value == other.value

    def is_symlink(self):
        return self.value in self.links

    def resolve(self, strict=False):
        return FakePath(self.links.get(self.value, self.value))

    def mkdir(self, **kwargs):
        pass
    def exists(self):
        return self.value in self.files

    def read_text(self):
        return self.files[self.value]

    def write_text(self, text):
        self.files[self.value] = text



class StorageTests(unittest.TestCase):
    def setUp(self):
        self.host = {'id': '42', 'name': 'vm', 'zone': 'zone'}
        FakePath.files = {}
        self.config = {'run_id': 'run', 'deadline_utc': '2030-01-01T00:00:00Z',
                       'machine_type': 'z4m-highmem-16'}
        self.instance = {'id': '42', 'name': 'vm', 'disks': [
            {'type': 'PERSISTENT', 'boot': True, 'deviceName': 'boot'},
            {'type': 'SCRATCH', 'interface': 'NVME'}]}
        self.api = mock.Mock()
        self.api.request.return_value = self.instance
        self.meta = {'instance/disks/': '0/\n1/',
                     'instance/disks/0/type': 'PERSISTENT',
                     'instance/disks/1/type': 'LOCAL-SSD'}
        self.disk = {'path': '/dev/nvme7n1', 'type': 'disk', 'model': 'nvme_card0',
                     'serial': 'serial', 'size': 4096, 'mountpoints': [None]}
        self.commands = []

    def run_storage(self, *, signatures=None, probe_code=2, mounted_code=1, mounted_source='',
                    inode_output='Inode count: 1024\n'):
        def check_output(argv, **kwargs):
            if argv[0] == 'lsblk':
                return json.dumps({'blockdevices': [self.disk]})
            if argv[0] == 'wipefs':
                return json.dumps({'signatures': signatures or []})
            if argv[0] == 'tune2fs':
                self.assertEqual(argv, ['tune2fs', '-l', '/dev/nvme7n1'])
                self.assertEqual(kwargs['env']['LC_ALL'], 'C')
                return inode_output
            raise AssertionError(argv)

        def run(argv, **kwargs):
            self.commands.append(argv)
            if argv[0] == 'mkfs.ext4':
                self.mkfs_timeout = kwargs['timeout']
            if argv[0] == 'findmnt':
                return mock.Mock(returncode=mounted_code, stdout=mounted_source)
            if argv[0] == 'blkid':
                return mock.Mock(returncode=probe_code, stdout='', stderr='')
            return mock.Mock(returncode=0)

        with mock.patch.object(worker, 'Path', FakePath), \
             mock.patch.object(worker, 'metadata', side_effect=self.meta.__getitem__), \
             mock.patch.object(worker.subprocess, 'check_output', side_effect=check_output), \
             mock.patch.object(worker.subprocess, 'run', side_effect=run), \
             mock.patch.object(worker.time, 'time', return_value=1), \
             mock.patch.object(worker.time, 'monotonic', side_effect=[10, 12.5]):
            self.storage = worker.prepare_storage(self.config, self.host, self.api)

    def test_exact_api_and_metadata_disk_is_formatted_only_after_signature_probes(self):
        self.run_storage()
        self.assertEqual([c[0] for c in self.commands], ['findmnt', 'blkid', 'mkfs.ext4', 'mount'])
        self.assertEqual(self.commands[2][-1], '/dev/nvme7n1')
        self.assertEqual(self.commands[2], ['mkfs.ext4', '-F', '-T', 'largefile4', '-E',
                                          'lazy_itable_init=0,lazy_journal_init=0', '/dev/nvme7n1'])
        self.assertEqual(self.storage['mkfs_duration_seconds'], 2.5)
        self.assertTrue(self.storage['lazy_initialization_disabled'])
        self.assertEqual(self.storage['inode_count'], 1024)
        self.assertEqual(self.storage['inode_ratio_bytes'], 4194304)
        self.assertEqual(self.storage['mkfs_profile'], 'largefile4')
        self.assertEqual(self.mkfs_timeout, 600)
        saved = self.storage
        self.run_storage(mounted_code=0, mounted_source='/dev/nvme7n1')
        self.assertEqual(self.storage, saved)

    def test_invalid_formatted_inode_count_prevents_mount(self):
        for text in ('', 'Inode count: 0\n', 'Inode count: 1\nInode count: 2\n'):
            with self.subTest(text=text), self.assertRaisesRegex(
                    gcp_runtime.RuntimeSafetyError, 'inode count'):
                self.run_storage(inode_output=text)
        self.assertNotIn('mount', [command[0] for command in self.commands])

    def test_metadata_mismatch_rejected(self):
        self.meta['instance/disks/1/type'] = 'PERSISTENT'
        with self.assertRaises(gcp_runtime.RuntimeSafetyError):
            self.run_storage()
        self.assertFalse(self.commands)

    def test_h4d_requires_all_distinct_serials_and_selects_documented_zero(self):
        self.config['machine_type'] = 'h4d-highmem-192-lssd'
        self.instance['disks'] = [self.instance['disks'][0]] + [
            {'type': 'SCRATCH', 'interface': 'NVME'} for _ in range(10)]
        self.meta = {'instance/disks/': '\n'.join(f'{i}/' for i in range(11)),
                     'instance/disks/0/type': 'PERSISTENT',
                     **{f'instance/disks/{i}/type': 'LOCAL-SSD' for i in range(1, 11)}}
        links = {'/dev/disk/by-id/google-boot': '/dev/nvme0n1',
                 **{f'/dev/disk/by-id/google-local-nvme-ssd-{i}': f'/dev/nvme{i + 10}n1'
                    for i in range(10)}}
        def inspect(argv, **kwargs):
            path = argv[-1]
            return json.dumps({'blockdevices': [{**self.disk, 'path': path, 'serial': path}]})
        with mock.patch.dict(FakePath.links, links, clear=True), \
             mock.patch.object(worker, 'Path', FakePath), \
             mock.patch.object(worker, 'metadata', side_effect=self.meta.__getitem__), \
             mock.patch.object(worker.subprocess, 'check_output', side_effect=inspect):
            admitted = worker.admit_scratch(self.config, self.host, self.api)
            self.assertEqual(admitted['device'], '/dev/nvme10n1')
            with mock.patch.object(worker.subprocess, 'check_output',
                                   side_effect=lambda argv, **kwargs: json.dumps(
                                       {'blockdevices': [{**self.disk, 'path': argv[-1], 'serial': 'duplicate'}]})):
                with self.assertRaisesRegex(gcp_runtime.RuntimeSafetyError, 'serials'):
                    worker.admit_scratch(self.config, self.host, self.api)
            del FakePath.links['/dev/disk/by-id/google-local-nvme-ssd-0']
            FakePath.links['/dev/disk/by-id/google-local-nvme-ssd-10'] = '/dev/nvme20n1'
            with self.assertRaisesRegex(gcp_runtime.RuntimeSafetyError, 'zero identity'):
                worker.admit_scratch(self.config, self.host, self.api)

    def test_z4m_32_admits_both_local_ssd_cards_and_selects_the_first(self):
        # F7 failure: z4m-highmem-32 reports its second local SSD as nvme_card1.
        self.config['machine_type'] = 'z4m-highmem-32'
        self.instance['disks'] = [self.instance['disks'][0]] + [
            {'type': 'SCRATCH', 'interface': 'NVME'} for _ in range(2)]
        self.meta = {'instance/disks/': '0/\n1/\n2/', 'instance/disks/0/type': 'PERSISTENT',
                     'instance/disks/1/type': 'LOCAL-SSD', 'instance/disks/2/type': 'LOCAL-SSD'}
        links = {'/dev/disk/by-id/google-boot': '/dev/nvme2n1',
                 '/dev/disk/by-id/google-local-nvme-ssd-0': '/dev/nvme0n1',
                 '/dev/disk/by-id/google-local-nvme-ssd-1': '/dev/nvme1n1'}
        def inspect(argv, **kwargs):
            card = 'nvme_card' + argv[-1][len('/dev/nvme')]
            return json.dumps({'blockdevices': [{**self.disk, 'path': argv[-1], 'model': card, 'serial': card}]})
        with mock.patch.dict(FakePath.links, links, clear=True), \
             mock.patch.object(worker, 'Path', FakePath), \
             mock.patch.object(worker, 'metadata', side_effect=self.meta.__getitem__), \
             mock.patch.object(worker.subprocess, 'check_output', side_effect=inspect):
            self.assertEqual(worker.admit_scratch(self.config, self.host, self.api)['device'], '/dev/nvme0n1')
        self.assertFalse(worker.physical_storage.admitted_model('gcp', 'nvme_card-pd'))

    def test_boot_alias_rejected(self):
        with mock.patch.dict(FakePath.links, {'/dev/disk/by-id/google-boot': '/dev/nvme7n1'}):
            with self.assertRaises(gcp_runtime.RuntimeSafetyError):
                self.run_storage()
        self.assertFalse(self.commands)

    def test_partition_mount_signature_and_probe_failure_rejected(self):
        for mutation, kwargs in [({'children': [{'type': 'part'}]}, {}),
                                 ({'mountpoints': ['/other']}, {}),
                                 ({}, {'signatures': [{'type': 'ext4'}]}), ({}, {'probe_code': 0})]:
            with self.subTest(mutation=mutation, kwargs=kwargs):
                previous = self.disk.copy()
                self.disk.update(mutation)
                self.commands.clear()
                with self.assertRaises(gcp_runtime.RuntimeSafetyError):
                    self.run_storage(**kwargs)
                self.assertNotIn('mkfs.ext4', [c[0] for c in self.commands])
                self.disk = previous

    def test_existing_exact_mount_is_reused(self):
        self.run_storage(mounted_code=0, mounted_source='/dev/nvme7n1')
        self.assertEqual([c[0] for c in self.commands], ['findmnt'])


class MemoryState(worker.WorkerState):
    def __init__(self, root, prefix):
        self.entries, self.prefix = {}, prefix
    def save(self, key):
        pass
    def close(self):
        pass


class WorkerQueueTests(unittest.TestCase):
    def exercise(self, *, active_limit, request_limit, done, items, expect_error=False):
        clock = [0]
        submissions = []
        class Pool:
            def __init__(self, max_workers):
                self.max_workers = max_workers
            def shutdown(self, **kwargs):
                pass
            def submit(self, fn, *args):
                submissions.append(args[3])
                return mock.Mock(done=mock.Mock(return_value=done), result=mock.Mock())
        store = mock.Mock()
        store.list.return_value = [{'name': f'runs/run/mailbox/42/requests/{i}.json',
                                    'generation': '1'} for i in range(items)]
        api = mock.Mock()
        api.objects.return_value = store
        config = {'run_id': 'run', 'source_sha256': 'a' * 64, 'package_root': '/package',
                  'project_id': 'project', 'bucket': 'bucket', 'deadline_utc': 'deadline',
                  'spec': workflow_input.specification('kv', 'native', 'standard')}
        host = {'id': '42', 'name': 'vm'}
        def sleep(seconds):
            clock[0] += seconds
        with mock.patch.object(worker, 'Path', FakePath), \
             mock.patch.object(worker, 'WorkerState', MemoryState), \
             mock.patch.object(worker, 'identify', return_value=host), \
             mock.patch.object(worker, 'prepare_storage', return_value=FORMATTED_STORAGE), \
             mock.patch.object(worker.gcp_runtime, 'validate_runtime'), \
             mock.patch.object(worker.gcp_runtime, 'verify_fleet'), \
             mock.patch.object(worker.gcp_runtime, '_deadline', return_value=3), \
             mock.patch.object(worker.cloud_package, 'verify_directory', return_value={'source_sha256': 'a' * 64, 'manifest_sha256': 'b' * 64}), \
             mock.patch.object(worker.concurrent.futures, 'ThreadPoolExecutor', Pool), \
             mock.patch.object(worker.time, 'time', side_effect=lambda: clock[0]), \
             mock.patch.object(worker.time, 'sleep', side_effect=sleep), \
             mock.patch.object(worker, 'MAX_ACTIVE', active_limit), \
             mock.patch.object(worker, 'MAX_REQUESTS', request_limit):
            if expect_error:
                with self.assertRaises(gcp_runtime.RuntimeSafetyError):
                    worker.serve(config, api=api)
            else:
                worker.serve(config, api=api)
        return submissions

    def test_outstanding_submissions_are_bounded(self):
        submitted = self.exercise(active_limit=2, request_limit=10, done=False, items=20)
        self.assertEqual(len(submitted), 2)
        self.assertEqual(len(set(submitted)), 2)

    def test_unique_requests_are_bounded_without_replaying_completed(self):
        submitted = self.exercise(active_limit=2, request_limit=3, done=True, items=10, expect_error=True)
        self.assertEqual(len(submitted), 3)
        self.assertEqual(len(set(submitted)), 3)

    def test_delivery_failures_retry_same_generation_with_backoff_and_invalid_is_terminal(self):
        for failure, expected, permanent in ((RuntimeError('transient'), 2, False),
                                              (worker.InvalidRequest('bad'), 1, False),
                                              (RuntimeError('permanent'), 3, True)):
            with self.subTest(failure=str(failure)):
                clock, calls = [0], []
                class Pool:
                    def __init__(self, max_workers):
                        pass
                    def shutdown(self, **kwargs):
                        pass
                    def submit(self, fn, *args):
                        future = concurrent.futures.Future()
                        calls.append((clock[0], args[4]))
                        if permanent or len(calls) == 1:
                            future.set_exception(failure)
                        else:
                            future.set_result({})
                        return future
                store = mock.Mock()
                store.list.return_value = [{'name': 'runs/run/mailbox/42/requests/op.json', 'generation': '7'}]
                api = mock.Mock()
                api.objects.return_value = store
                config = {'run_id': 'run', 'source_sha256': 'a' * 64, 'package_root': '/package',
                          'project_id': 'project', 'bucket': 'bucket', 'deadline_utc': 'deadline',
                          'spec': workflow_input.specification('kv', 'native', 'standard')}
                with mock.patch.object(worker, 'Path', FakePath), \
                     mock.patch.object(worker, 'WorkerState', MemoryState), \
                     mock.patch.object(worker, 'identify', return_value={'id': '42', 'name': 'vm'}), \
                     mock.patch.object(worker, 'prepare_storage', return_value=FORMATTED_STORAGE), \
                     mock.patch.object(worker.gcp_runtime, 'validate_runtime'), \
                     mock.patch.object(worker.gcp_runtime, 'verify_fleet'), \
                     mock.patch.object(worker.gcp_runtime, '_deadline', return_value=8), \
                     mock.patch.object(worker.cloud_package, 'verify_directory', return_value={'source_sha256': 'a' * 64, 'manifest_sha256': 'b' * 64}), \
                     mock.patch.object(worker.concurrent.futures, 'ThreadPoolExecutor', Pool), \
                     mock.patch.object(worker.time, 'time', side_effect=lambda: clock[0]), \
                     mock.patch.object(worker.time, 'sleep', side_effect=lambda seconds: clock.__setitem__(0, clock[0] + seconds)):
                    worker.serve(config, api=api)
                self.assertEqual(len(calls), expected)
                self.assertTrue(all(generation == '7' for _, generation in calls))
                if expected > 1:
                    self.assertGreaterEqual(calls[1][0] - calls[0][0], 1)
                if expected == 3:
                    self.assertGreaterEqual(calls[2][0] - calls[1][0], 2)

    def test_response_retry_replays_durable_result_without_running_twice(self):
        deadline = '2099-01-01T00:00:00Z'
        config = {'run_id': 'run', 'source_sha256': 'a' * 64, 'deadline_utc': deadline}
        host = {'id': '42', 'index': 1}
        key = 'runs/run/mailbox/42/requests/op.json'
        request = {'operation_id': 'op', 'schema_version': 1, 'run_id': 'run',
                   'source_sha256': 'a' * 64, 'instance_id': '42', 'index': 1,
                   'deadline_utc': deadline, 'timeout_seconds': 10}
        store = mock.Mock()
        store.get.return_value = json.dumps(request).encode()
        store.put.side_effect = [RuntimeError('lost response upload'), {}]
        process = mock.Mock()
        process.wait.return_value = 0
        with tempfile.TemporaryDirectory() as root, \
             mock.patch.object(worker, 'admit_command', return_value=(['fake'], None, {})), \
             mock.patch.object(worker.subprocess, 'Popen', return_value=process) as popen:
            with self.assertRaisesRegex(RuntimeError, 'lost response upload'):
                worker.run_request(config, host, store, key, '7', Path(root))
            result = worker.run_request(config, host, store, key, '7', Path(root))
        self.assertEqual(result['exit_code'], 0)
        self.assertEqual(popen.call_count, 1)
        self.assertEqual(store.put.call_count, 2)

    def test_restart_preserves_attempts_and_unique_limit(self):
        original_state = worker.WorkerState
        clock, end, calls = [0], [1], []
        key = 'runs/run/mailbox/42/requests/op.json'
        class Pool:
            def __init__(self, max_workers):
                pass
            def shutdown(self, **kwargs):
                pass
            def submit(self, fn, *args):
                calls.append(args[3])
                future = concurrent.futures.Future()
                future.set_exception(RuntimeError('unavailable'))
                return future
        api = mock.Mock()
        store = api.objects.return_value
        store.list.return_value = [{'name': key, 'generation': '7'}]
        config = {'run_id': 'run', 'source_sha256': 'a' * 64, 'package_root': '/package',
                  'project_id': 'project', 'bucket': 'bucket', 'deadline_utc': 'deadline',
                  'spec': workflow_input.specification('kv', 'native', 'standard')}
        with tempfile.TemporaryDirectory() as root, \
             mock.patch.object(worker, 'Path', FakePath), \
             mock.patch.object(worker, 'WorkerState', side_effect=lambda ignored, prefix: original_state(root, prefix)), \
             mock.patch.object(worker, 'identify', return_value={'id': '42', 'name': 'vm'}), \
             mock.patch.object(worker, 'prepare_storage', return_value=FORMATTED_STORAGE), \
             mock.patch.object(worker.gcp_runtime, 'validate_runtime'), \
             mock.patch.object(worker.gcp_runtime, 'verify_fleet'), \
             mock.patch.object(worker.gcp_runtime, '_deadline', side_effect=lambda value: end[0]), \
             mock.patch.object(worker.cloud_package, 'verify_directory', return_value={'source_sha256': 'a' * 64, 'manifest_sha256': 'b' * 64}), \
             mock.patch.object(worker.concurrent.futures, 'ThreadPoolExecutor', Pool), \
             mock.patch.object(worker, 'MAX_REQUESTS', 1), \
             mock.patch.object(worker.time, 'time', side_effect=lambda: clock[0]), \
             mock.patch.object(worker.time, 'sleep', side_effect=lambda seconds: clock.__setitem__(0, clock[0] + seconds)):
            worker.serve(config, api=api)
            end[0] = 9
            worker.serve(config, api=api)
            self.assertEqual(calls, [key, key, key])
            state = original_state(root, 'runs/run/mailbox/42/requests/')
            self.assertEqual(state.entries[key]['attempts'], 3)
            self.assertEqual(state.entries[key]['status'], 'rejected')
            with self.assertRaises(BlockingIOError):
                original_state(root, 'runs/run/mailbox/42/requests/')
            state.close()
            store.list.return_value = [{'name': key, 'generation': '7'},
                                       {'name': 'runs/run/mailbox/42/requests/other.json', 'generation': '8'}]
            end[0] = 10
            with self.assertRaisesRegex(gcp_runtime.RuntimeSafetyError, 'request limit'):
                worker.serve(config, api=api)
            self.assertEqual(calls, [key, key, key])

    def test_sqlite_state_updates_one_key_and_rejects_invalid_or_excess_rows(self):
        prefix = 'runs/run/mailbox/42/requests/'
        with tempfile.TemporaryDirectory() as root:
            state = worker.WorkerState(root, prefix)
            for name in ('a', 'b'):
                key = prefix + name + '.json'
                state.entries[key] = {'generation': '7', 'attempts': 1,
                                      'status': 'pending', 'retry_after': 0}
                state.save(key)
            state.entries[prefix + 'a.json']['status'] = 'completed'
            state.entries[prefix + 'b.json']['status'] = 'rejected'
            state.save(prefix + 'a.json')
            state.close()
            state = worker.WorkerState(root, prefix)
            self.assertEqual(state.entries[prefix + 'a.json']['status'], 'completed')
            self.assertEqual(state.entries[prefix + 'b.json']['status'], 'pending')
            state.close()
            with mock.patch.object(worker, 'MAX_REQUESTS', 1):
                with self.assertRaisesRegex(ValueError, 'invalid mailbox state'):
                    worker.WorkerState(root, prefix)
            with sqlite3.connect(str(Path(root) / 'mailbox-state.sqlite3')) as db:
                db.execute('UPDATE requests SET attempts=99 WHERE key=?', (prefix + 'a.json',))
            with self.assertRaisesRegex(ValueError, 'invalid mailbox state entry'):
                worker.WorkerState(root, prefix)

    def test_corrupt_sqlite_state_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            Path(root, 'mailbox-state.sqlite3').write_bytes(b'not a sqlite database')
            with self.assertRaises(sqlite3.DatabaseError):
                worker.WorkerState(root, 'runs/run/mailbox/42/requests/')

    def test_persistence_failure_prevents_submission(self):
        state = MemoryState(None, 'runs/run/mailbox/42/requests/')
        state.save = mock.Mock(side_effect=OSError('disk full'))
        api = mock.Mock()
        api.objects.return_value.list.return_value = [
            {'name': 'runs/run/mailbox/42/requests/op.json', 'generation': '1'}]
        pool = mock.Mock()
        config = {'run_id': 'run', 'source_sha256': 'a' * 64, 'package_root': '/package',
                  'project_id': 'project', 'bucket': 'bucket', 'deadline_utc': 'deadline',
                  'spec': workflow_input.specification('kv', 'native', 'standard')}
        with mock.patch.object(worker, 'Path', FakePath), \
             mock.patch.object(worker, 'WorkerState', return_value=state), \
             mock.patch.object(worker, 'identify', return_value={'id': '42', 'name': 'vm'}), \
             mock.patch.object(worker, 'prepare_storage', return_value=FORMATTED_STORAGE), \
             mock.patch.object(worker.gcp_runtime, 'validate_runtime'), \
             mock.patch.object(worker.gcp_runtime, 'verify_fleet'), \
             mock.patch.object(worker.gcp_runtime, '_deadline', return_value=3), \
             mock.patch.object(worker.cloud_package, 'verify_directory', return_value={'source_sha256': 'a' * 64, 'manifest_sha256': 'b' * 64}), \
             mock.patch.object(worker.concurrent.futures, 'ThreadPoolExecutor', return_value=pool), \
             mock.patch.object(worker.time, 'time', return_value=0):
            with self.assertRaisesRegex(OSError, 'disk full'):
                worker.serve(config, api=api)
        pool.submit.assert_not_called()

    def test_expiry_does_not_wait_for_unfinished_real_future(self):
        started, release, finished = threading.Event(), threading.Event(), threading.Event()
        clock = [0]
        api = mock.Mock()
        api.objects.return_value.list.return_value = [
            {'name': 'runs/run/mailbox/42/requests/op.json', 'generation': '1'}]
        config = {'run_id': 'run', 'source_sha256': 'a' * 64, 'package_root': '/package',
                  'project_id': 'project', 'bucket': 'bucket', 'deadline_utc': 'deadline',
                  'spec': workflow_input.specification('kv', 'native', 'standard')}
        def blocked(*args):
            started.set()
            release.wait(5)
            finished.set()
        def expire(seconds):
            if not started.wait(5):
                raise AssertionError('future did not start')
            clock[0] = 3
        try:
            with mock.patch.object(worker, 'Path', FakePath), \
                 mock.patch.object(worker, 'WorkerState', MemoryState), \
                 mock.patch.object(worker, 'identify', return_value={'id': '42', 'name': 'vm'}), \
                 mock.patch.object(worker, 'prepare_storage', return_value=FORMATTED_STORAGE), \
                 mock.patch.object(worker, 'run_request', side_effect=blocked), \
                 mock.patch.object(worker.gcp_runtime, 'validate_runtime'), \
                 mock.patch.object(worker.gcp_runtime, 'verify_fleet'), \
                 mock.patch.object(worker.gcp_runtime, '_deadline', return_value=3), \
                 mock.patch.object(worker.cloud_package, 'verify_directory', return_value={'source_sha256': 'a' * 64, 'manifest_sha256': 'b' * 64}), \
                 mock.patch.object(worker.time, 'time', side_effect=lambda: clock[0]), \
                 mock.patch.object(worker.time, 'sleep', side_effect=expire):
                worker.serve(config, api=api)
                self.assertTrue(started.is_set())
                self.assertFalse(finished.is_set())
        finally:
            release.set()
            self.assertTrue(finished.wait(5))


class BootstrapTests(unittest.TestCase):
    def test_worker_has_unlimited_memlock(self):
        script = gcp_bootstrap.startup_script('valid-bucket', 'run', 'a' * 64, '2030-01-01T00:00:00Z')
        worker_unit = script.split("'--unit=tidehunter-worker-'")[1]
        self.assertIn('--property=LimitMEMLOCK=infinity', worker_unit)
        self.assertIn('--property=RuntimeMaxSec=', worker_unit)
        self.assertIn('--property=TimeoutStopSec=1', worker_unit)
        self.assertIn('remaining = int(deadline - time.time())', script)
        self.assertIn("if remaining <= 0: raise TimeoutError('worker startup expired')", script)
        self.assertIn("'--on-calendar='", script)
        self.assertIn("'/usr/bin/systemctl', 'poweroff'", script)
        self.assertIn("'--unit=tidehunter-idle-'", script)
        self.assertIn("'serve-idle'", script)

    def test_a_restarted_guest_reuses_the_matching_package(self):
        script = gcp_bootstrap.startup_script('valid-bucket', 'run', 'a' * 64,
                                              '2030-01-01T00:00:00Z')
        self.assertIn("if target.exists():", script)
        self.assertIn("if not target.is_dir(): raise ValueError('package target is not a directory')",
                      script)
        self.assertNotIn('package target already exists', script)
        self.assertIn("if json.loads(config_path.read_text()) != runtime:", script)
        self.assertIn("if hashlib.sha256((target / 'source.zip').read_bytes()).hexdigest() "
                      "!= c['source_sha256']:", script)


class CampaignCommandTests(unittest.TestCase):
    """The two admissions the campaign drivers need beyond a sealed argv."""

    def config(self, **extra):
        return {'run_id': 'run', 'source_sha256': 'a' * 64,
                'package_root': '/mnt/tidehunter/run/package', 'deadline_utc': 'x',
                'guest_roots': ['/mnt/tidehunter/sui-run-run'], **extra}

    def request(self, argv, **extra):
        return {'argv': argv, 'env': {}, 'cwd': None, **extra}

    def test_one_shell_script_is_admitted(self):
        argv, cwd, env = worker.admit_command(
            self.config(), self.request(['/bin/sh', '-c', 'rm -rf /mnt/tidehunter/run/x && mkdir -p /mnt/tidehunter/run/x']))
        self.assertEqual(argv[0], '/bin/sh')
        self.assertEqual((cwd, env), (None, {}))

    def test_every_other_shell_shape_is_refused(self):
        for argv in (['/bin/bash', '-c', 'true'], ['/bin/sh', '-lc', 'true'],
                     ['/bin/sh', '-c', 'true', 'extra'], ['/bin/sh'], ['/bin/sh', '-c', ''],
                     ['/bin/sh', '-c', 'x' * (worker.MAX_SCRIPT + 1)],
                     ['/bin/sh', '-c', 'true\x00'], ['/bin/sh', '--', 'true']):
            with self.subTest(argv=argv[0] if len(argv) < 3 else argv[:2]):
                with self.assertRaises(gcp_runtime.RuntimeSafetyError):
                    worker.admit_command(self.config(), self.request(argv))

    def test_sealed_argv_admission_is_unchanged(self):
        with mock.patch.object(worker, '_inside', return_value=True):
            argv, _, _ = worker.admit_command(
                self.config(), self.request([gcp_runtime.GUEST_PYTHON, '/pkg/source/a.py']))
        self.assertEqual(argv[1], '/pkg/source/a.py')
        with self.assertRaises(gcp_runtime.RuntimeSafetyError):
            worker.admit_command(self.config(), self.request(['/usr/bin/tar', '-xzf', '-']))

    def test_a_declared_guest_root_widens_only_that_root(self):
        with mock.patch.object(worker, '_inside', side_effect=lambda path, root: str(root) == str(path.parent)):
            worker.admit_command(self.config(), self.request(['/bin/sh', '-c', 'true'],
                                                            cwd='/mnt/tidehunter/sui-run-run/owners'))
        with self.assertRaises(gcp_runtime.RuntimeSafetyError):
            worker.admit_command(self.config(), self.request(['/bin/sh', '-c', 'true'],
                                                             cwd='/mnt/tidehunter/other/owners'))

    def test_the_two_transfer_blobs_are_bound_to_the_operation(self):
        host = {'id': '42', 'index': 0}
        keys = 'runs/run/transfer/42/op/'
        source, sink = worker.admit_transfer(self.config(), host, 'op', {
            'stdin': {'object': keys + 'stdin', 'sha256': 'b' * 64, 'size': 4},
            'stdout_object': keys + 'stdout'})
        self.assertEqual((source['object'], sink), (keys + 'stdin', keys + 'stdout'))
        for change in ({'object': 'runs/run/transfer/42/other/stdin'},
                       {'object': 'runs/run/transfer/43/op/stdin'},
                       {'sha256': 'B' * 64}, {'size': -1}, {'size': '4'},
                       {'object': keys + 'stdin', 'sha256': 'b' * 64, 'size': 4, 'x': 1}):
            with self.subTest(change=change):
                blob = {'object': keys + 'stdin', 'sha256': 'b' * 64, 'size': 4, **change}
                with self.assertRaises(gcp_runtime.RuntimeSafetyError):
                    worker.admit_transfer(self.config(), host, 'op', {'stdin': blob})
        with self.assertRaises(gcp_runtime.RuntimeSafetyError):
            worker.admit_transfer(self.config(), host, 'op', {'stdout_object': keys + 'other'})

    def test_a_refusal_is_published_as_a_terminal_receipt(self):
        store = mock.Mock()
        store.get.return_value = json.dumps({'operation_id': 'op'}).encode()
        host = {'id': '42', 'index': 0}
        receipt = worker.publish_rejection(store, self.config(), host,
                                           'runs/run/mailbox/42/requests/op.json', '7',
                                           worker.InvalidRequest('invalid campaign shell command'))
        self.assertIs(receipt['rejected'], True)
        self.assertEqual(receipt['exit_code'], 126)
        self.assertEqual(receipt['operation_id'], 'op')
        self.assertEqual(receipt['instance_id'], '42')
        self.assertIn('invalid campaign shell command', receipt['stderr'])
        key, raw = store.put.call_args.args[:2]
        self.assertEqual(key, 'runs/run/mailbox/42/responses/op.json')
        self.assertEqual(json.loads(raw), receipt)

    def test_an_unreadable_refusal_publishes_nothing(self):
        store = mock.Mock()
        store.get.side_effect = RuntimeError('bucket unavailable')
        self.assertIsNone(worker.publish_rejection(
            store, self.config(), {'id': '42'}, 'runs/run/mailbox/42/requests/op.json', '7',
            worker.InvalidRequest('nope')))
        store.put.assert_not_called()


class TransferRunTests(unittest.TestCase):
    """One real subprocess: stdin from the bucket, stdout back to it."""

    def test_stdin_and_spilled_stdout_round_trip(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            store = mock.Mock()
            store.put.return_value = {'generation': '9'}
            payload = b'payload' * 1000
            store.download.side_effect = lambda key, path, **kwargs: Path(path).write_bytes(payload)
            spill = {}

            def upload(key, path, **kwargs):
                spill['key'], spill['bytes'] = key, Path(path).read_bytes()
                return {'generation': '9', 'size': len(spill['bytes']),
                        'metadata': {'sha256': __import__('hashlib').sha256(spill['bytes']).hexdigest()}}
            store.upload.side_effect = upload
            config = {'run_id': 'run', 'source_sha256': 'a' * 64,
                      'deadline_utc': '2099-01-01T00:00:00Z', 'package_root': '/package'}
            host = {'id': '42', 'index': 1}
            key = 'runs/run/mailbox/42/requests/op.json'
            request = {'operation_id': 'op', 'schema_version': 1, 'run_id': 'run',
                       'source_sha256': 'a' * 64, 'instance_id': '42', 'index': 1,
                       'deadline_utc': config['deadline_utc'], 'timeout_seconds': 60,
                       'argv': ['/bin/sh', '-c', 'wc -c >&2; head -c 1200000 /dev/zero | tr "\\0" a'],
                       'env': {}, 'cwd': None,
                       'stdin': {'object': 'runs/run/transfer/42/op/stdin',
                                 'sha256': __import__('hashlib').sha256(payload).hexdigest(),
                                 'size': len(payload)},
                       'stdout_object': 'runs/run/transfer/42/op/stdout'}
            store.get.return_value = json.dumps(request).encode()
            with mock.patch.object(worker, 'scratch_root', side_effect=lambda *_: root), \
                    mock.patch.object(worker.gcp_runtime, 'GUEST_PATH', '/usr/bin:/bin'):
                result = worker.run_request(config, host, store, key, '7', root)
            self.assertEqual(result['exit_code'], 0)
            self.assertEqual(result['stdout'], '')
            self.assertEqual(result['stdout_object']['size'], 1200000)
            self.assertEqual(spill['key'], 'runs/run/transfer/42/op/stdout')
            self.assertEqual(spill['bytes'], b'a' * 1200000)
            self.assertEqual(result['stderr'].strip(), str(len(payload)))
            stored = json.loads(store.put.call_args.args[1])
            self.assertEqual(stored['stdout_object'], result['stdout_object'])


if __name__ == '__main__':
    unittest.main()
