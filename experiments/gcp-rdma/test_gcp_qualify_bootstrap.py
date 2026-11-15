import ast
import hashlib
import json
import io
import os
import pathlib
import socket
import subprocess
import sys
import threading
import tarfile
import tempfile
import time
import unittest
import zipfile
from pathlib import Path
from unittest import mock
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'kv-comparison'))

import gcp_api
import gcp_qualify_bootstrap as bootstrap


class Store:
    def __init__(self, values=None):
        self.values = values or {}
        self.writes = {}

    def get(self, key, **kwargs):
        if key not in self.values:
            raise gcp_api.ApiError(404, 'missing')
        return self.values[key]

    def put(self, key, raw, **kwargs):
        if key in self.writes:
            raise AssertionError('unexpected overwrite')
        self.writes[key] = json.loads(raw)


class QualificationBootstrapTests(unittest.TestCase):
    def setUp(self):
        self.cfg = {'run_id': 'run-1', 'source_sha256': 'a' * 64,
                    'manifest_sha256': 'b' * 64, 'deadline_utc': '2030-01-01T00:00:00Z',
                    'bucket': 'test-bucket', 'mode': 'bake', 'preparation_sha256': 'c' * 64}

    def test_bad_peer_address_and_device_are_rejected_before_script(self):
        args = ('test-bucket', 'run-1', 'a' * 64, '2030-01-01T00:00:00Z', 'b' * 64, 'peer-1')
        for ip, device in [('127.0.0.1', 'irdma0'), ('10.0.0.2', '../dev')]:
            with self.subTest(ip=ip, device=device), self.assertRaises(ValueError):
                bootstrap.pair_startup_script(*args, ip, device)

    def test_live_pair_requires_peer_id_machine_and_supported_transports(self):
        args = ('test-bucket', 'run-1', 'a' * 64, '2030-01-01T00:00:00Z', 'b' * 64,
                'peer-1', '10.1.0.2', 'irdma0')
        with self.assertRaisesRegex(ValueError, 'exact peer ID'):
            bootstrap.pair_startup_script(*args, live_transports=['rdma'])
        with self.assertRaisesRegex(ValueError, 'invalid live transport'):
            bootstrap.pair_startup_script(*args, peer_instance_id='12', machine_type='z4m-highmem-16',
                                          live_transports=['local_file'])


    def test_fleet_startup_admits_two_or_six_distinct_members(self):
        members = [{'name': f'run-1-{i}', 'id': str(i + 1), 'rdma_ip': f'10.0.0.{i + 1}'}
                   for i in range(6)]
        args = ('test-bucket', 'run-1', 'a' * 64, '2030-01-01T00:00:00Z', 'b' * 64, 'c' * 64)
        for count in (2, 6):
            self.assertIn('TIDEHUNTER_BOOTSTRAP', bootstrap.fleet_startup_script(
                *args, members=members[:count], rdma_device='irdma0', machine_type='z4m-highmem-16',
                qualification_profile='focused'))
        for count in (1, 3, 4, 5):
            with self.assertRaises(ValueError):
                bootstrap.fleet_startup_script(*args, members=members[:count],
                    rdma_device='irdma0', machine_type='z4m-highmem-16')
        with self.assertRaises(ValueError):
            bootstrap.fleet_startup_script(*args, members=[members[0], members[0]],
                rdma_device='irdma0', machine_type='z4m-highmem-16')
    def test_absolute_deadline_is_enforced(self):
        with self.assertRaises(TimeoutError):
            bootstrap._remaining(time.time() - 1, 60)

    def test_focused_live_pair_is_restricted_to_compact_leased_rdma(self):
        members = [{'name': f'run-1-{i}', 'id': str(i + 1), 'rdma_ip': f'10.0.0.{i + 1}'}
                   for i in range(6)]
        args = ('test-bucket', 'run-1', 'a' * 64, '2030-01-01T00:00:00Z', 'b' * 64, 'c' * 64)
        options = {'members': members[:2], 'rdma_device': 'irdma0', 'machine_type': 'z4m-highmem-16',
                   'qualification_profile': 'focused', 'selection': 'leased_ab_compact', 'live_transports': ['rdma']}
        self.assertIn('TIDEHUNTER_BOOTSTRAP', bootstrap.fleet_startup_script(*args, **options))
        for change in ({'selection': 'parity_compact'}, {'members': members},
                       {'live_transports': ['tcp']}, {'live_spdk': True}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                bootstrap.fleet_startup_script(*args, **{**options, **change})

    def test_fleet_startup_accepts_six_hours_but_rejects_more(self):
        import datetime
        import signal
        import urllib.request
        deadline = '2030-01-01T00:00:00Z'
        deadline_seconds = datetime.datetime.fromisoformat(deadline.replace('Z', '+00:00')).timestamp()
        script = bootstrap._script('test-bucket', 'run-1', 'a' * 64, deadline, 'b' * 64, 'fleet')
        program = script.split("<<'TIDEHUNTER_BOOTSTRAP'\n", 1)[1].rsplit('\nTIDEHUNTER_BOOTSTRAP', 1)[0]
        opener = mock.Mock()
        opener.open.side_effect = RuntimeError('metadata reached')
        for remaining, error, message in ((21600, RuntimeError, 'metadata reached'),
                                          (21601, ValueError, 'qualification lifetime exceeds bound')):
            with self.subTest(remaining=remaining), \
                 mock.patch.object(time, 'time', return_value=deadline_seconds - remaining), \
                 mock.patch.object(signal, 'signal'), mock.patch.object(signal, 'setitimer'), \
                 mock.patch.object(urllib.request, 'build_opener', return_value=opener), \
                 mock.patch.object(sys, 'stderr', new_callable=io.StringIO), \
                 self.assertRaisesRegex(error, message):
                exec(compile(program, '<fleet-startup>', 'exec'), {})

    def test_peer_barrier_rejects_mismatched_source(self):
        store = Store({'ready': json.dumps({'source_sha256': 'wrong'}).encode()})
        with self.assertRaisesRegex(ValueError, 'identity mismatch'):
            bootstrap._wait(store, 'ready', time.time() + 10, {'source_sha256': 'a' * 64})

    def test_peer_barrier_propagates_access_failure(self):
        store = mock.Mock()
        store.get.side_effect = gcp_api.ApiError(403, 'denied')
        with self.assertRaises(gcp_api.ApiError):
            bootstrap._wait(store, 'ready', time.time() + 10, {})

    def test_bake_rejects_preparation_manifest_hash(self):
        store = Store({'runs/run-1/preparation.json': b'{}'})
        with mock.patch.object(bootstrap, '_call', return_value='cp311'):
            with self.assertRaisesRegex(ValueError, 'manifest checksum'):
                bootstrap._bake(Path('/tmp/package'), self.cfg, time.time() + 30, store)

    def test_bake_rejects_wheel_escaping_run_prefix(self):
        name = 'PyYAML-6.0.2-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl'
        raw = json.dumps({'wheels': [{'path': name, 'sha256': 'd' * 64,
                                     'object_key': 'runs/other/' + name, 'generation': '1'}]}).encode()
        self.cfg['preparation_sha256'] = hashlib.sha256(raw).hexdigest()
        store = Store({'runs/run-1/preparation.json': raw})
        with mock.patch.object(bootstrap, '_call', return_value='cp311'):
            with self.assertRaisesRegex(ValueError, 'outside admitted'):
                bootstrap._bake(Path('/tmp/package'), self.cfg, time.time() + 30, store)

    def test_bake_rejects_modified_wheel(self):
        name = 'PyYAML-6.0.2-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl'
        key = 'runs/run-1/preparation/' + name
        raw = json.dumps({'wheels': [{'path': name, 'sha256': 'd' * 64,
                                     'object_key': key, 'generation': '123'}]}).encode()
        self.cfg['preparation_sha256'] = hashlib.sha256(raw).hexdigest()
        store = Store({'runs/run-1/preparation.json': raw, key: b'wrong wheel'})
        with mock.patch.object(bootstrap, '_call', return_value='cp311'):
            with self.assertRaisesRegex(ValueError, 'wheel checksum'):
                bootstrap._bake(Path('/tmp/package'), self.cfg, time.time() + 30, store)

    def test_guest_reports_manifest_failure_without_running_bake(self):
        import gcp_image_prepare
        store = Store()
        api = mock.Mock()
        api.objects.return_value = store
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory)
            (package / 'manifest.json').write_text('{}')
            with mock.patch.object(gcp_image_prepare, 'metadata', side_effect=['host-1', 'mysten-research']), \
                 mock.patch.object(gcp_api, 'GoogleApi', return_value=api), \
                 mock.patch.object(bootstrap, '_bake') as bake:
                with self.assertRaisesRegex(ValueError, 'manifest checksum'):
                    bootstrap.guest(self.cfg, package)
                bake.assert_not_called()
        status = store.writes['runs/run-1/qualification/bake/status.json']
        self.assertFalse(status['complete'])
        self.assertEqual(status['source_sha256'], 'a' * 64)

    def test_success_publishes_diagnostic_before_completion(self):
        import gcp_image_prepare
        store = Store()
        api = mock.Mock()
        api.objects.return_value = store
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory)
            raw = b'{}'
            (package / 'manifest.json').write_bytes(raw)
            self.cfg['manifest_sha256'] = hashlib.sha256(raw).hexdigest()
            with mock.patch.object(gcp_image_prepare, 'metadata', side_effect=['host-1', 'mysten-research']), \
                 mock.patch.object(gcp_api, 'GoogleApi', return_value=api), \
                 mock.patch.object(bootstrap, '_bake', return_value={'kind': 'actual-bake-proof'}):
                bootstrap.guest(self.cfg, package)
        self.assertEqual(list(store.writes), ['runs/run-1/qualification/bake/diagnostic.json',
                                               'runs/run-1/qualification/bake/status.json'])
        self.assertTrue(store.writes['runs/run-1/qualification/bake/status.json']['complete'])

    def test_startup_exec_rejects_wrong_runtime_source(self):
        import signal
        import urllib.request
        deadline = '2030-01-01T00:00:00Z'
        script = bootstrap.bake_startup_script('test-bucket', 'run-1', 'a' * 64, deadline, 'b' * 64, 'c' * 64)
        code = script.split("TIDEHUNTER_BOOTSTRAP'\n", 1)[1].rsplit('\nTIDEHUNTER_BOOTSTRAP', 1)[0]
        class Response:
            headers = {'Metadata-Flavor': 'Google'}
            def __init__(self, value): self.value = value
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def read(self, *args): return self.value
        def open_request(request, timeout):
            url = request.full_url
            for key, value in [('tidehunter-run-id', 'run-1'), ('tidehunter-source-sha256', 'a' * 64),
                               ('tidehunter-deadline-utc', deadline), ('instance/name', 'host-1')]:
                if key in url:
                    return Response(value.encode())
            if url.endswith('/token'):
                return Response(b'{\"access_token\":\"test\"}')
            if 'runtime.json' in url:
                return Response(b'{\"run_id\":\"foreign\"}')
            if 'package.json' in url:
                return Response(b'{}')
            if 'upload/storage' in url:
                return Response(b'{}')
            raise AssertionError(url)
        opener = mock.Mock()
        opener.open.side_effect = open_request
        with mock.patch.object(urllib.request, 'build_opener', return_value=opener), \
             mock.patch.object(signal, 'signal'), mock.patch.object(signal, 'setitimer'), \
             mock.patch('time.time', return_value=1893455900):
            with self.assertRaisesRegex(ValueError, 'runtime/package binding mismatch'):
                exec(compile(code, '<startup>', 'exec'), {})

    def test_startup_deadline_interrupts_archive_download_and_reports_failure(self):
        import signal
        import urllib.request
        deadline = '2030-01-01T00:00:00Z'
        script = bootstrap.bake_startup_script('test-bucket', 'run-1', 'a' * 64, deadline, 'b' * 64, 'c' * 64)
        code = script.split("TIDEHUNTER_BOOTSTRAP'\n", 1)[1].rsplit('\nTIDEHUNTER_BOOTSTRAP', 1)[0]
        package = {'object_key': 'runs/run-1/package.tar', 'generation': '1',
                   'sha256': 'd' * 64, 'source_sha256': 'a' * 64}
        runtime = {'run_id': 'run-1', 'source_sha256': 'a' * 64, 'deadline_utc': deadline,
                   'bucket': 'test-bucket', 'package': package}
        handlers = {}
        reports = []
        class Response:
            headers = {'Metadata-Flavor': 'Google'}
            def __init__(self, value, interrupt=False):
                self.value, self.interrupt = value, interrupt
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def read(self, *args):
                if self.interrupt:
                    handlers['alarm'](signal.SIGALRM, None)
                return self.value
        def open_request(request, timeout):
            url = request.full_url
            for key, value in [('tidehunter-run-id', 'run-1'), ('tidehunter-source-sha256', 'a' * 64),
                               ('tidehunter-deadline-utc', deadline), ('instance/name', 'host-1')]:
                if key in url:
                    return Response(value.encode())
            if url.endswith('/token'):
                return Response(b'{\"access_token\":\"test\"}')
            if 'runtime.json' in url:
                return Response(json.dumps(runtime).encode())
            if 'package.json' in url:
                return Response(json.dumps(package).encode())
            if 'package.tar' in url:
                return Response(b'', interrupt=True)
            if 'upload/storage' in url:
                parts = request.data.split(b'\r\n\r\n')
                metadata = json.loads(parts[1].split(b'\r\n--')[0])
                payload = parts[2].split(b'\r\n--')[0]
                self.assertEqual(metadata['metadata']['sha256'], hashlib.sha256(payload).hexdigest())
                reports.append(json.loads(payload))
                return Response(json.dumps({'generation': '42', 'size': len(payload),
                                            'metadata': metadata['metadata']}).encode())
            raise AssertionError(url)
        opener = mock.Mock()
        opener.open.side_effect = open_request
        def install_handler(signum, handler):
            handlers['alarm'] = handler
        with mock.patch.object(urllib.request, 'build_opener', return_value=opener), \
             mock.patch.object(signal, 'signal', side_effect=install_handler), \
             mock.patch.object(signal, 'setitimer') as timer, \
             mock.patch('time.time', return_value=1893455900):
            with self.assertRaisesRegex(TimeoutError, 'absolute deadline'):
                exec(compile(code, '<startup>', 'exec'), {})
        self.assertEqual(timer.call_args_list[0].args, (signal.ITIMER_REAL, 100))
        self.assertEqual(timer.call_args_list[-1].args, (signal.ITIMER_REAL, 0))
        self.assertEqual(len(reports), 1)
        self.assertFalse(reports[0]['complete'])

    def test_late_rdma_cm_listeners_and_loopback_ping(self):
        ping_server = socket.socket()
        ping_server.bind(('127.0.0.1', 0))
        ping_server.listen(1)
        ping_server.settimeout(4)
        ping_port = ping_server.getsockname()[1]
        process = mock.Mock(pid=os.getpid())
        process.poll.return_value = None
        records = [{'ifname': 'irdma0', 'pid': os.getpid(), 'state': 'LISTEN',
                    'ps': 'TCP', 'src-addr': '0.0.0.0:' + str(port)} for port in (18515, 18516)]
        self.assertFalse(bootstrap._owned_cm_listener(records, os.getppid(), 'irdma0', 18515))
        self.assertFalse(bootstrap._owned_cm_listener(records, os.getpid(), 'other', 18515))
        threads = []
        try:
            with tempfile.TemporaryDirectory() as directory:
                state = Path(directory) / 'cm.json'
                state.write_text('[]')
                rdma = Path(directory) / 'rdma'
                rdma.write_text('#!' + sys.executable + '\nprint(open(' + repr(str(state)) + ').read())\n')
                rdma.chmod(0o700)
                cli = Path(directory) / 'probe'
                cli.write_text('#!' + sys.executable + '\nimport socket\n'
                               's=socket.create_connection((\"127.0.0.1\", ' + str(ping_port) + '), 2)\n'
                               's.sendall(b\"PING\")\nprint(s.recv(16).decode())\n')
                cli.chmod(0o700)
                def delayed_listen():
                    time.sleep(0.3)
                    replacement = Path(directory) / 'next.json'
                    replacement.write_text(json.dumps(records))
                    replacement.replace(state)
                def pong():
                    conn, _ = ping_server.accept()
                    with conn:
                        if conn.recv(16) == b'PING':
                            conn.sendall(b'PONG')
                threads = [threading.Thread(target=delayed_listen, daemon=True),
                           threading.Thread(target=pong, daemon=True)]
                for thread in threads:
                    thread.start()
                with mock.patch.dict(os.environ, {'PATH': directory + os.pathsep + os.environ.get('PATH', '')}):
                    bootstrap._wait_servers([process, process, process], cli, 'irdma0', time.time() + 10)
        finally:
            ping_server.close()
            for thread in threads:
                thread.join(timeout=2)

    def test_cleanup_stops_real_process_group(self):
        process = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'],
                                   start_new_session=True)
        try:
            bootstrap._stop_processes([process])
            self.assertIsNotNone(process.poll())
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()

    def test_failed_command_output_is_retained(self):
        with self.assertRaisesRegex(RuntimeError, 'specific guest failure'):
            bootstrap._call([sys.executable, '-c',
                             'import sys; print(\"specific guest failure\"); sys.exit(7)'],
                            time.time() + 10)

    def test_pair_cleanup_preserves_primary_and_final_status_budget(self):
        import gcp_image_qualify
        import cloud_package
        clock, attempts, opened = [0.0], [], []
        primary = RuntimeError('qualification failed before cleanup')
        deadline = 170.0

        class SlowStore:
            def put(self, key, data, **kwargs):
                attempts.append(key)
                clock[0] += 60
                raise TimeoutError('bounded credential/upload failure')

        with tempfile.TemporaryDirectory() as directory:
            def open_log(name, mode):
                handle = (Path(directory) / Path(name).name).open(mode)
                opened.append(handle)
                return handle

            api = mock.Mock()
            api.request.return_value = {'networkInterfaces': [
                {'nicType': 'IRDMA', 'networkIP': '10.20.0.1'}]}
            cfg = {**self.cfg, 'rdma_device': 'irdma0'}
            with mock.patch.object(cloud_package, 'verify_directory',
                                   return_value={'manifest_sha256': cfg['manifest_sha256']}), \
                    mock.patch.object(gcp_api, 'GoogleApi', return_value=api), \
                    mock.patch.object(gcp_image_qualify, 'metadata',
                                      side_effect=['mysten-research', 'zones/us-central1-a']), \
                    mock.patch.object(bootstrap, 'open', open_log, create=True), \
                    mock.patch.object(bootstrap.subprocess, 'Popen', return_value=object()), \
                    mock.patch.object(bootstrap, '_wait_servers', side_effect=primary), \
                    mock.patch.object(bootstrap, '_stop_processes'), \
                    mock.patch.object(bootstrap.time, 'time', side_effect=lambda: clock[0]), \
                    mock.patch.object(sys, 'path', list(sys.path)), \
                    mock.patch.object(sys, 'stderr', io.StringIO()):
                with self.assertRaises(RuntimeError) as caught:
                    bootstrap._pair(Path(directory), cfg, deadline, SlowStore(), 'own', {})
            self.assertIs(caught.exception, primary)
            self.assertEqual(len(attempts), 1)
            self.assertGreaterEqual(deadline - clock[0], 50)
            self.assertTrue(all(log.closed for log in opened))

    def test_pair_profile_controls_valkey_without_changing_rdma_probes(self):
        import gcp_image_qualify
        import cloud_package
        api = mock.Mock()
        api.request.return_value = {'networkInterfaces': [{'nicType': 'IRDMA', 'networkIP': '10.20.0.1'}]}
        cfg = {**self.cfg, 'rdma_device': 'irdma0', 'peer_instance': 'peer-1', 'peer_ip': '10.20.0.2'}
        with tempfile.TemporaryDirectory() as directory:
            def open_log(name, mode):
                return (Path(directory) / Path(name).name).open(mode)
            for profile, build_profile, selection in (
                    ('focused', 'focused', 'tcp_reads'),
                    ('focused', 'focused', 'nvmeof_f7_highmem32'),
                    ('full', None, None),
                    ('focused', 'focused_valkey', 'valkey_rdma_reads')):
                with self.subTest(profile=profile, build_profile=build_profile), \
                        mock.patch.object(cloud_package, 'verify_directory',
                                          return_value={'manifest_sha256': cfg['manifest_sha256'],
                                                        'build_profile': build_profile}), \
                        mock.patch.object(gcp_api, 'GoogleApi', return_value=api), \
                        mock.patch.object(gcp_image_qualify, 'metadata',
                                          side_effect=['mysten-research', 'zones/us-central1-a']), \
                        mock.patch.object(gcp_image_qualify, 'guest', return_value={}) as guest, \
                        mock.patch.object(bootstrap, 'open', open_log, create=True), \
                        mock.patch.object(bootstrap.subprocess, 'Popen', return_value=object()) as spawn, \
                        mock.patch.object(bootstrap, '_wait_servers') as ready, \
                        mock.patch.object(bootstrap, '_stop_processes'), \
                        mock.patch.object(bootstrap, '_put'), mock.patch.object(bootstrap, '_wait'), \
                        mock.patch.object(sys, 'path', list(sys.path)):
                    bootstrap._pair(Path(directory), {**cfg, 'qualification_profile': profile,
                                                      'selection': selection},
                                    time.time() + 1000, mock.Mock(), 'own', {})
                commands = [call.args[0][0] for call in spawn.call_args_list]
                self.assertEqual(commands[:2], ['ib_read_lat', 'ib_send_lat'])
                valkey = profile == 'full' or build_profile == 'focused_valkey'
                self.assertEqual(len(commands), 3 if valkey else 2)
                self.assertEqual(ready.call_args.args[1],
                                 Path(directory) / 'runtime/bin/valkey-cli' if valkey else None)
                if valkey:
                    self.assertEqual(commands[2], str(Path(directory) / 'runtime/bin/valkey-server'))
                self.assertEqual(guest.call_args.kwargs, {'qualification_profile': 'focused'} if profile == 'focused' else {})
            with self.assertRaisesRegex(ValueError, 'forbids'):
                bootstrap._pair(Path(directory), {**cfg, 'qualification_profile': 'focused',
                                                 'selection': 'nvmeof_f7_highmem32',
                                                 'valkey_qualification': True},
                                time.time() + 1000, mock.Mock(), 'own', {})
            for build_profile, selection in (
                    ('focused_valkey', 'tcp_reads'),
                    ('focused_valkey', None),
                    ('focused_valkey', 'nvmeof_f7_highmem32'),
                    ('focused', 'valkey_rdma_reads')):
                with self.subTest(build_profile=build_profile, selection=selection), \
                        mock.patch.object(cloud_package, 'verify_directory',
                                          return_value={'manifest_sha256': cfg['manifest_sha256'],
                                                        'build_profile': build_profile}), \
                        mock.patch.object(bootstrap.subprocess, 'Popen') as spawn, \
                        mock.patch.object(sys, 'path', list(sys.path)):
                    with self.assertRaisesRegex(ValueError, 'selection/package profile mismatch'):
                        bootstrap._pair(Path(directory),
                                        {**cfg, 'qualification_profile': 'focused',
                                         'selection': selection},
                                        time.time() + 1000, mock.Mock(), 'own', {})
                    spawn.assert_not_called()


    def test_shared_bootstrap_fetches_and_extracts_admitted_archive(self):
        import gcp_bootstrap
        import urllib.request
        source_buffer = io.BytesIO()
        with zipfile.ZipFile(source_buffer, 'w') as archive:
            archive.writestr('hello.txt', b'source-content')
        source_bytes = source_buffer.getvalue()
        source_sha = hashlib.sha256(source_bytes).hexdigest()
        archive_buffer = io.BytesIO()
        with tarfile.open(fileobj=archive_buffer, mode='w:gz') as archive:
            for name, data in [('source.zip', source_bytes), ('source/hello.txt', b'source-content')]:
                info = tarfile.TarInfo(name)
                info.size = len(data)
                archive.addfile(info, io.BytesIO(data))
        archive_bytes = archive_buffer.getvalue()
        package = {'object_key': 'runs/run-1/package.tar', 'generation': '12',
                   'sha256': hashlib.sha256(archive_bytes).hexdigest(), 'source_sha256': source_sha}
        deadline = '2030-01-01T00:00:00Z'
        runtime = {'run_id': 'run-1', 'source_sha256': source_sha, 'deadline_utc': deadline,
                   'bucket': 'test-bucket', 'package': package}
        class Response:
            headers = {'Metadata-Flavor': 'Google'}
            def __init__(self, data): self.data = data
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def read(self, *args): return self.data
        def request(req, timeout):
            url = req.full_url
            for suffix, value in [('tidehunter-run-id', 'run-1'),
                                  ('tidehunter-source-sha256', source_sha),
                                  ('tidehunter-deadline-utc', deadline)]:
                if suffix in url:
                    return Response(value.encode())
            if url.endswith('/token'):
                return Response(b'{\"access_token\":\"test\"}')
            if 'runtime.json' in url:
                return Response(json.dumps(runtime).encode())
            if 'package.json' in url:
                return Response(json.dumps(package).encode())
            if 'package.tar' in url:
                return Response(archive_bytes)
            raise AssertionError(url)
        opener = mock.Mock()
        opener.open.side_effect = request
        program = gcp_bootstrap._preamble('test-bucket', 'run-1', source_sha, deadline) + gcp_bootstrap._fetch_program()
        real_path = pathlib.Path
        with tempfile.TemporaryDirectory() as directory:
            def path(value):
                return real_path(directory) if value == '/mnt/tidehunter' else real_path(value)
            with mock.patch.object(urllib.request, 'build_opener', return_value=opener), \
                 mock.patch.object(pathlib, 'Path', side_effect=path):
                context = {}
                exec(compile(program, '<shared-startup>', 'exec'), context)
            self.assertEqual((real_path(directory) / 'run-1/package/source/hello.txt').read_bytes(),
                             b'source-content')

class FleetBootstrapTests(unittest.TestCase):
    def setUp(self):
        self.members = [{'name': 'host-%d' % i, 'id': str(100 + i),
                         'rdma_ip': '10.1.0.%d' % (i + 1)} for i in range(6)]
        self.cfg = {'run_id': 'run-1', 'source_sha256': 'a' * 64,
                    'manifest_sha256': 'b' * 64, 'deadline_utc': '2030-01-01T00:00:00Z',
                    'preparation_sha256': 'c' * 64, 'members': self.members,
                    'rdma_device': 'irdma0', 'machine_type': 'z4m-highmem-16',
                    'live_transports': ['rdma'], 'live_spdk': True}
        self.identity = {key: self.cfg[key] for key in
                         ('run_id', 'source_sha256', 'manifest_sha256', 'deadline_utc')}

    def test_six_receipts_only_two_pairs_and_cleanup_precedes_status(self):
        pair_calls = []
        events = []
        for i, member in enumerate(self.members):
            store = Store()
            original_put = bootstrap._put
            def put(target, key, value):
                events.append((i, 'put', key))
                original_put(target, key, value)
            def pair(package, cfg, deadline):
                pair_calls.append((cfg['own_instance'], cfg['peer_instance'], cfg['peer_instance_id'],
                                   {key: cfg[key] for key in self.identity}))
            with mock.patch.object(bootstrap, '_fleet_phase', side_effect=lambda deadline, limit, op: op(deadline)), \
                 mock.patch.object(bootstrap, '_fleet_nvmet_fix', return_value=None), \
                 mock.patch.object(bootstrap, '_bake', return_value={'wheel_sha256': 'd' * 64}), \
                 mock.patch.object(bootstrap, '_fleet_pair', side_effect=pair), \
                 mock.patch.object(bootstrap, '_fleet_cleanup', side_effect=lambda run: events.append((i, 'cleanup', run))), \
                 mock.patch.object(bootstrap, '_put', side_effect=put):
                bootstrap._fleet_guest(self.cfg, Path('/package'), time.time() + 100, store,
                                       member['name'], self.identity, lambda key: member['id'])
            prefix = 'runs/run-1/qualification/' + member['name'] + '/'
            prep, status = store.writes[prefix + 'preparation.json'], store.writes[prefix + 'status.json']
            self.assertEqual(prep['instance_id'], member['id'])
            self.assertEqual(prep['instance_name'], member['name'])
            self.assertEqual(prep['source_sha256'], self.cfg['source_sha256'])
            self.assertTrue(status['cleanup_complete'])
            self.assertEqual(status['pair'], i < 2)
            self.assertLess(events.index((i, 'cleanup', 'run-1')),
                            events.index((i, 'put', prefix + 'status.json')))
        self.assertEqual([(p[0], p[1], p[2]) for p in pair_calls],
                         [('host-0', 'host-1', '101'), ('host-1', 'host-0', '100')])
        self.assertTrue(all(p[3] == self.identity for p in pair_calls))

    def test_wrong_instance_id_cannot_prepare(self):
        with mock.patch.object(bootstrap, '_bake') as bake, self.assertRaisesRegex(ValueError, 'identity mismatch'):
            bootstrap._fleet_guest(self.cfg, Path('/package'), time.time() + 100, Store(),
                                   'host-0', self.identity, lambda key: '999')
        bake.assert_not_called()

    def test_pair_child_rejects_changed_numeric_identity_without_status(self):
        import gcp_image_prepare
        store = Store()
        api = mock.Mock()
        api.objects.return_value = store
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory)
            (package / 'manifest.json').write_bytes(b'{}')
            cfg = {**self.cfg, 'bucket': 'test-bucket', 'mode': 'fleet-pair',
                   'manifest_sha256': hashlib.sha256(b'{}').hexdigest(),
                   'own_instance': 'host-0', 'own_instance_id': '100',
                   'phase_deadline': time.time() + 120}
            with mock.patch.object(gcp_image_prepare, 'metadata',
                                   side_effect=['host-0', 'mysten-research', '999']), \
                 mock.patch.object(gcp_api, 'GoogleApi', return_value=api), \
                 mock.patch.object(bootstrap, '_pair') as pair:
                with self.assertRaisesRegex(ValueError, 'identity mismatch'):
                    bootstrap.guest(cfg, package)
                pair.assert_not_called()
        self.assertEqual(store.writes, {})

    def test_pair_failure_keeps_preparation_but_cannot_publish_success(self):
        store = Store()
        with mock.patch.object(bootstrap, '_fleet_phase', side_effect=lambda deadline, limit, op: op(deadline)), \
             mock.patch.object(bootstrap, '_fleet_nvmet_fix', return_value=None), \
             mock.patch.object(bootstrap, '_bake', return_value={'wheel_sha256': 'd' * 64}), \
             mock.patch.object(bootstrap, '_fleet_pair', side_effect=RuntimeError('physical proof failed')), \
             mock.patch.object(bootstrap, '_fleet_cleanup') as cleanup:
            with self.assertRaisesRegex(RuntimeError, 'physical proof failed'):
                bootstrap._fleet_guest(self.cfg, Path('/package'), time.time() + 100, store,
                                       'host-0', self.identity, lambda key: '100')
        self.assertIn('runs/run-1/qualification/host-0/preparation.json', store.writes)
        self.assertNotIn('runs/run-1/qualification/host-0/status.json', store.writes)
        cleanup.assert_not_called()

    def test_cleanup_failure_prevents_admission(self):
        store = Store()
        with mock.patch.object(bootstrap, '_fleet_phase', side_effect=lambda deadline, limit, op: op(deadline)), \
             mock.patch.object(bootstrap, '_fleet_nvmet_fix', return_value=None), \
             mock.patch.object(bootstrap, '_bake', return_value={}), \
             mock.patch.object(bootstrap, '_fleet_cleanup', side_effect=RuntimeError('data busy')):
            with self.assertRaisesRegex(RuntimeError, 'data busy'):
                bootstrap._fleet_guest(self.cfg, Path('/package'), time.time() + 100, store,
                                       'host-2', self.identity, lambda key: '102')
        self.assertNotIn('runs/run-1/qualification/host-2/status.json', store.writes)

    def test_fix_install_failure_blocks_bake_and_pair(self):
        with mock.patch.object(bootstrap, '_fleet_phase', side_effect=lambda deadline, limit, op: op(deadline)), \
             mock.patch.object(bootstrap, '_fleet_nvmet_fix', side_effect=ValueError('module admission failed')), \
             mock.patch.object(bootstrap, '_bake') as bake, \
             mock.patch.object(bootstrap, '_fleet_pair') as pair:
            store = Store()
            with self.assertRaisesRegex(ValueError, 'module admission failed'):
                bootstrap._fleet_guest(self.cfg, Path('/package'), time.time() + 100, store,
                                       'host-0', self.identity, lambda key: '100')
            bake.assert_not_called()
            pair.assert_not_called()
            self.assertEqual(store.writes, {})

    def test_six_preparations_preserve_loaded_proof_before_pair(self):
        evidence = {'candidate': {'sha256': 'a' * 64}, 'loaded': {'sysfs_srcversion': 'ABC'}}
        for member in self.members:
            store = Store()
            events = []
            with mock.patch.object(bootstrap, '_fleet_phase', side_effect=lambda deadline, limit, op: op(deadline)), \
                 mock.patch.object(bootstrap, '_fleet_nvmet_fix', side_effect=lambda *a: events.append('load') or evidence), \
                 mock.patch.object(bootstrap, '_bake', side_effect=lambda *a: events.append('bake') or {}), \
                 mock.patch.object(bootstrap, '_fleet_pair', side_effect=lambda *a: events.append('pair')), \
                 mock.patch.object(bootstrap, '_fleet_cleanup'):
                bootstrap._fleet_guest(self.cfg, Path('/package'), time.time() + 100, store,
                                       member['name'], self.identity, lambda key: member['id'])
            self.assertEqual(events[:2], ['load', 'bake'])
            self.assertEqual(store.writes['runs/run-1/qualification/' + member['name'] +
                                          '/preparation.json']['proof']['nvmet_fix'], evidence)

    def test_owned_guest_and_loaded_identity_gate_install(self):
        candidate = {'module_sha256': 'a' * 64, 'candidate_srcversion': 'ABC',
                     'candidate_build_id': '123'}
        loaded = {**candidate, 'guest': {'instance_name': 'host-0', 'instance_id': '100', 'run_id': 'run-1'},
                  'loaded_nvmet': {'srcversion': 'ABC', 'build_id': '123'}}
        instance = {'name': 'host-0', 'id': '100', 'status': 'RUNNING',
                    'labels': {'bench-run': 'run-1', 'bench-source': 'a' * 63},
                    'metadata': {'items': [{'key': 'bench-run', 'value': 'run-1'},
                                           {'key': 'bench-source', 'value': 'a' * 64},
                                           {'key': 'bench-expiry', 'value': self.cfg['deadline_utc']}]},
                    'scheduling': {'instanceTerminationAction': 'DELETE', 'terminationTime': self.cfg['deadline_utc']},
                    'machineType': 'z4m-highmem-16'}
        api = mock.Mock()
        api.request.return_value = instance
        helper = mock.Mock()
        helper.install.return_value = loaded
        cloud = mock.Mock()
        cloud.verify_directory.return_value = {'manifest_sha256': 'b' * 64, 'source_sha256': 'a' * 64,
                                               'nvmet_fix': candidate}
        cloud.nvmet_fix_module.return_value = helper
        metadata = lambda key: {'instance/zone': 'zones/z', 'project/project-id': 'mysten-research'}[key]
        with tempfile.TemporaryDirectory() as directory, \
             mock.patch.dict(sys.modules, {'cloud_package': cloud}), \
             mock.patch.object(gcp_api, 'GoogleApi', return_value=api):
            root = Path(directory)
            (root / 'manifest.json').write_text(json.dumps({'nvmet_fix': candidate}))
            result = bootstrap._fleet_nvmet_fix(root, self.cfg, 'host-0', '100', metadata)
            self.assertEqual(result['loaded'], loaded)
            helper.install.reset_mock()
            instance['id'] = '999'
            with self.assertRaisesRegex(ValueError, 'ownership'):
                bootstrap._fleet_nvmet_fix(root, self.cfg, 'host-0', '100', metadata)
            helper.install.assert_not_called()
            instance['id'] = '100'
            helper.install.return_value = {**loaded, 'loaded_nvmet': {'srcversion': 'STOCK', 'build_id': '123'}}
            with self.assertRaisesRegex(ValueError, 'loaded nvmet proof'):
                bootstrap._fleet_nvmet_fix(root, self.cfg, 'host-0', '100', metadata)

    def test_guest_publishes_failure_after_pair_failure(self):
        import gcp_image_prepare
        store = Store()
        api = mock.Mock()
        api.objects.return_value = store
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory)
            (package / 'manifest.json').write_bytes(b'{}')
            cfg = {**self.cfg, 'bucket': 'test-bucket', 'mode': 'fleet',
                   'manifest_sha256': hashlib.sha256(b'{}').hexdigest()}
            with mock.patch.object(gcp_image_prepare, 'metadata',
                                   side_effect=['host-0', 'mysten-research', '100']), \
                 mock.patch.object(gcp_api, 'GoogleApi', return_value=api), \
                 mock.patch.object(bootstrap, '_fleet_phase', side_effect=lambda deadline, limit, op: op(deadline)), \
                 mock.patch.object(bootstrap, '_bake', return_value={}), \
                 mock.patch.object(bootstrap, '_fleet_pair', side_effect=RuntimeError('physical proof failed')):
                with self.assertRaisesRegex(RuntimeError, 'physical proof failed'):
                    bootstrap.guest(cfg, package)
        status = store.writes['runs/run-1/qualification/host-0/status.json']
        self.assertFalse(status['complete'])
        self.assertIn('physical proof failed', status['error'])
        self.assertIn('runs/run-1/qualification/host-0/preparation.json', store.writes)

    def test_owned_data_cleanup_preserves_other_campaign_data(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = root / 'run-1'
            owned = [f'{target}-{tier}' for target in ('live-tcp', 'live-rdma', 'live-rdma-spdk')
                     for tier in ('native', 'synchronous')]
            for name in (*owned, 'production'):
                (run / name).mkdir(parents=True)
                (run / name / 'data').write_text('data')
            real_path = Path
            with mock.patch.object(bootstrap, 'Path',
                                   side_effect=lambda value: root if value == '/mnt/tidehunter-kv-comparison'
                                   else real_path(value)):
                bootstrap._fleet_cleanup('run-1')
            self.assertTrue((run / 'production/data').exists())
            for name in owned:
                self.assertFalse((run / name).exists())

    def test_generated_fleet_startup_waits_for_bound_runtime_and_reuses_package(self):
        import datetime
        import signal
        import urllib.error
        import urllib.request

        source_buffer = io.BytesIO()
        with zipfile.ZipFile(source_buffer, 'w') as source:
            source.writestr('hello.txt', b'source-content')
        source_bytes = source_buffer.getvalue()
        source_sha = hashlib.sha256(source_bytes).hexdigest()
        archive_buffer = io.BytesIO()
        with tarfile.open(fileobj=archive_buffer, mode='w:gz') as archive:
            for name, data in [('source.zip', source_bytes), ('source/hello.txt', b'source-content')]:
                entry = tarfile.TarInfo(name)
                entry.size = len(data)
                archive.addfile(entry, io.BytesIO(data))
        archive_bytes = archive_buffer.getvalue()
        package = {'object_key': 'runs/run-1/package.tar', 'generation': '12',
                   'sha256': hashlib.sha256(archive_bytes).hexdigest(), 'source_sha256': source_sha}
        deadline = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)).isoformat()
        script = bootstrap.fleet_startup_script('test-bucket', 'run-1', source_sha, deadline,
                                                'b' * 64, 'c' * 64, members=self.members,
                                                rdma_device='irdma0', machine_type='z4m-highmem-16',
                                                live_transports=['rdma'], live_spdk=True)
        program = script.split("<<'TIDEHUNTER_BOOTSTRAP'\n", 1)[1].rsplit('\nTIDEHUNTER_BOOTSTRAP', 1)[0]

        class Response:
            headers = {'Metadata-Flavor': 'Google'}
            def __init__(self, data): self.data = data
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def read(self, *args): return self.data

        for changed in (False, True):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as directory:
                events = []
                runtime_package = dict(package)
                if changed:
                    runtime_package['generation'] = '99'
                runtime = {'run_id': 'run-1', 'source_sha256': source_sha,
                           'deadline_utc': deadline, 'bucket': 'test-bucket',
                           'package': runtime_package}
                def request(req, timeout):
                    url = req.full_url
                    for suffix, value in [('tidehunter-run-id', 'run-1'),
                                          ('tidehunter-source-sha256', source_sha),
                                          ('tidehunter-deadline-utc', deadline),
                                          ('instance/name', 'host-0')]:
                        if suffix in url:
                            return Response(value.encode())
                    if url.endswith('/token'):
                        return Response(b'{"access_token":"test"}')
                    if 'runtime.json' in url:
                        events.append('runtime')
                        if events.count('runtime') == 1:
                            raise urllib.error.HTTPError(url, 404, 'missing', {}, None)
                        return Response(json.dumps(runtime).encode())
                    if 'package.json' in url:
                        return Response(json.dumps(package).encode())
                    if 'package.tar' in url:
                        events.append('archive')
                        return Response(archive_bytes)
                    raise RuntimeError('unexpected request: ' + url)
                def run(argv, **kwargs):
                    if any('tidehunter-qualify-' in str(arg) for arg in argv):
                        events.append('qualify')
                    if any(str(arg).endswith('/gcp_worker.py') for arg in argv):
                        events.append('worker')
                        self.assertEqual(str(real_path(directory) / 'run-1/package/source/experiments/gcp-rdma/gcp_worker.py'),
                                         next(str(arg) for arg in argv if str(arg).endswith('/gcp_worker.py')))
                    return subprocess.CompletedProcess(argv, 0)
                opener = mock.Mock()
                opener.open.side_effect = request
                real_path = pathlib.Path
                def path(value):
                    return real_path(directory) if value == '/mnt/tidehunter' else real_path(value)
                with mock.patch.object(urllib.request, 'build_opener', return_value=opener), \
                     mock.patch.object(pathlib, 'Path', side_effect=path), \
                     mock.patch.object(subprocess, 'run', side_effect=run), \
                     mock.patch.object(time, 'sleep'), \
                     mock.patch.object(signal, 'signal'), mock.patch.object(signal, 'setitimer'):
                    if changed:
                        with self.assertRaisesRegex(ValueError, 'runtime/package binding mismatch'):
                            exec(compile(program, '<fleet-startup>', 'exec'), {})
                    else:
                        exec(compile(program, '<fleet-startup>', 'exec'), {})
                self.assertEqual(events.count('archive'), 1)
                self.assertEqual(events.count('runtime'), 2)
                self.assertLess(events.index('qualify'), events.index('runtime'))
                self.assertEqual(events.count('worker'), 0 if changed else 1)
                self.assertEqual((real_path(directory) / 'run-1/package/source/hello.txt').read_bytes(),
                                 b'source-content')

    def test_startup_is_valid_and_rejects_duplicate_identity(self):
        args = ('test-bucket', 'run-1', 'a' * 64, '2030-01-01T00:00:00Z', 'b' * 64, 'c' * 64)
        script = bootstrap.fleet_startup_script(*args, members=self.members, rdma_device='irdma0',
                                               machine_type='z4m-highmem-16', live_transports=['rdma'],
                                               live_spdk=True)
        program = script.split("<<'TIDEHUNTER_BOOTSTRAP'\n", 1)[1].rsplit('\nTIDEHUNTER_BOOTSTRAP', 1)[0]
        compile(program, '<fleet-startup>', 'exec')
        duplicate = [dict(member) for member in self.members]
        duplicate[2]['id'] = duplicate[0]['id']
        with self.assertRaisesRegex(ValueError, 'distinct'):
            bootstrap.fleet_startup_script(*args, members=duplicate, rdma_device='irdma0',
                                            machine_type='z4m-highmem-16')

    def test_focused_startup_binds_profile_and_rejects_live_probes(self):
        args = ('test-bucket', 'run-1', 'a' * 64, '2030-01-01T00:00:00Z', 'b' * 64, 'c' * 64)
        script = bootstrap.fleet_startup_script(*args, members=self.members, rdma_device='irdma0',
                                                machine_type='z4m-highmem-16', qualification_profile='focused')
        program = script.split("<<'TIDEHUNTER_BOOTSTRAP'\n", 1)[1].rsplit('\nTIDEHUNTER_BOOTSTRAP', 1)[0]
        config = json.loads(ast.literal_eval(program.splitlines()[0].split(' = ', 1)[1]))
        self.assertEqual(config['qualification_profile'], 'focused')
        self.assertEqual(config['live_transports'], [])
        self.assertFalse(config['live_spdk'])
        with self.assertRaisesRegex(ValueError, 'forbids'):
            bootstrap.fleet_startup_script(*args, members=self.members, rdma_device='irdma0',
                                           machine_type='z4m-highmem-16', qualification_profile='focused',
                                           live_transports=['rdma'])





class ReviewedSuiQualificationTests(unittest.TestCase):
    def test_reviewed_sui_uses_focused_live_pair_qualification(self):
        import copy
        import manual_sui
        spec = copy.deepcopy(manual_sui.REVIEWED_SUI_SPECS['sui_nvmeof_compact'])
        members = [dict(name=f'run-1-{i}', id=str(i+1), rdma_ip=f'10.0.0.{i+1}') for i in range(2)]
        args = ('test-bucket', 'run-1', 'a'*64, '2030-01-01T00:00:00Z', 'b'*64, 'c'*64)
        with mock.patch.object(bootstrap, '_script', return_value='script') as script:
            bootstrap.fleet_startup_script(*args, members=members, rdma_device='irdma0',
                machine_type='z4m-highmem-16', live_transports=['rdma'],
                qualification_profile='focused', suite_spec=spec)
        self.assertEqual(script.call_args.kwargs['qualification_profile'], 'focused')
        self.assertEqual(script.call_args.kwargs['suite_spec'], spec)
        spec['sui_cells'][0]['contract'] = 'harness'
        with self.assertRaisesRegex(ValueError, 'unreviewed Sui'):
            bootstrap.fleet_startup_script(*args, members=members, rdma_device='irdma0',
                machine_type='z4m-highmem-16', live_transports=['rdma'],
                qualification_profile='focused', suite_spec=spec)


if __name__ == '__main__':
    unittest.main()
