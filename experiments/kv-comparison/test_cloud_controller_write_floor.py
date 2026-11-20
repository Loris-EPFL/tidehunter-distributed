import json
import hashlib
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
import copy
import os
from pathlib import Path
import tempfile
import subprocess
import sys
import unittest
from unittest.mock import patch

import cloud_controller as controller
import floors
import analyze


class WriteFloorTests(unittest.TestCase):
    def spec(self, mode='buffered'):
        return {'mode': mode, 'block_bytes': 1024, 'region_bytes': 536870912,
                'runtime_seconds': 6, 'queue_depth': 1, 'location': 'local_file', 'preheat': True}

    def test_argv_modes(self):
        for mode in ('buffered', 'fdatasync', 'direct'):
            argv = floors.write_argv('/bin/helper', '/tmp/fio-scratch/test', '/tmp/case/log', self.spec(mode), {'device': 12, 'inode': 34})
            self.assertEqual(argv[1:5], ['/tmp/fio-scratch', 'test', '/tmp/case/log', mode])
            self.assertEqual(argv[5:], ['1024', '536870912', '6', str(floors.WRITE_LOG_ROWS), '12', '34'])

    def test_independent_percentiles_and_bounds(self):
        job = {'case_id': 'x', 'ssd_write': self.spec('fdatasync')}
        log = b'10,20,30,0\n50,5,55,1024\n30,10,40,2048\n'
        raw = json.dumps({'total_operations': 5, 'logged_operations': 3, 'elapsed_ns': 100}).encode()
        result = floors.write_result(job, raw, log, '1:2', False)
        self.assertEqual(result['total'], {'n': 3, 'min': 30, 'mean': 125/3,
                         'p50': 40, 'p90': 55, 'p99': 55, 'p999': 55, 'max': 55})
        self.assertEqual(result['dropped_operations'], 2)
        with self.assertRaises(ValueError):
            floors.parse_write_log(b'x' * (floors.MAX_LOG_BYTES + 1), job['ssd_write'])
        with self.assertRaises(ValueError):
            floors.parse_write_log(b'1,2,9,0\n', job['ssd_write'])

    def test_guard_rejects_symlink_device_and_wrong_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); scratch = root / 'fio-scratch'; scratch.mkdir(); case = root / 'case'; case.mkdir()
            target = scratch / 'x'; target.write_bytes(b'x')
            st = target.stat(); identity = {'path': str(target), 'device': st.st_dev, 'inode': st.st_ino}
            argv = floors.write_argv('/bin/helper', target, case / 'log', self.spec(), identity)
            for bad in (argv[:2] + ['../dev/null'] + argv[3:], argv[:3] + ['/dev/null'] + argv[4:]):
                with self.assertRaises(controller.SafetyError):
                    controller.run_write_fio(bad, identity, scratch, case, 5)
            target.unlink(); target.symlink_to('/dev/null')
            with self.assertRaises(controller.SafetyError):
                controller.run_write_fio(argv, identity, scratch, case, 5)

    def test_dispatch_and_mismatched_guest_job(self):
        runtime = controller.Controller.__new__(controller.Controller)
        runtime.config = {'fleet': {'nodes': [{} for _ in range(5)]}}
        with patch.object(runtime, 'run_floor_case', return_value='ok') as run:
            job = {'case_kind': 'ssd_write_floor'}
            self.assertEqual(runtime.run_case(job), 'ok')
            run.assert_called_once_with(job)

    def test_fd_guard_rejects_other_file_and_device(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); scratch = root / 'scratch'; scratch.write_bytes(b'x')
            case = root / 'case'; case.mkdir()
            for extra in (root / 'outside', Path('/dev/zero')):
                script = ('import os,time,sys; a=os.open(sys.argv[1],os.O_WRONLY); '
                          'b=os.open(sys.argv[2],os.O_WRONLY|os.O_CREAT,0o600); '
                          'print(\"ready\",flush=True); time.sleep(5)')
                proc = subprocess.Popen([sys.executable, '-B', '-c', script, str(scratch), str(extra)],
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    self.assertEqual(proc.stdout.readline(), b'ready\n')
                    st = scratch.stat()
                    pipes = {os.fstat(proc.stdout.fileno()).st_ino, os.fstat(proc.stderr.fileno()).st_ino}
                    with self.assertRaises(controller.SafetyError):
                        controller.write_fio_flags(proc.pid, st.st_dev, st.st_ino, case, pipes)
                finally:
                    proc.kill(); proc.communicate()

    def test_fd_guard_retries_unstable_and_rejects_persistent_inspection_errors(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scratch = root / 'scratch'
            scratch.write_bytes(b'x')
            case = root / 'case'
            case.mkdir()
            script = ('import os,time,sys; os.open(sys.argv[1],os.O_WRONLY); '
                      'print("ready",flush=True); time.sleep(30)')
            proc = subprocess.Popen([sys.executable, '-B', '-c', script, str(scratch)],
                                    stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                self.assertEqual(proc.stdout.readline(), b'ready\n')
                st = scratch.stat()
                pipes = {os.fstat(proc.stdout.fileno()).st_ino, os.fstat(proc.stderr.fileno()).st_ino}
                info_path = Path('/proc') / str(proc.pid) / 'fdinfo' / '3'
                original = Path.read_text
                attempts = []
                def transient(path, *args, **kwargs):
                    if path == info_path:
                        attempts.append(path)
                        if len(attempts) == 1:
                            raise FileNotFoundError('reopened descriptor')
                    return original(path, *args, **kwargs)
                with patch.object(Path, 'read_text', transient), patch.object(controller.time, 'sleep'):
                    self.assertEqual(controller.write_fio_flags(proc.pid, st.st_dev, st.st_ino, case, pipes),
                                     {'direct': False})
                self.assertEqual(len(attempts), 3)
                attempts.clear()
                def changed_inode(path, *args, **kwargs):
                    text = original(path, *args, **kwargs)
                    if path == info_path:
                        attempts.append(path)
                        if len(attempts) == 1:
                            return '\n'.join('ino:\t0' if line.startswith('ino:') else line
                                             for line in text.splitlines())
                    return text
                with patch.object(Path, 'read_text', changed_inode), patch.object(controller.time, 'sleep'):
                    self.assertEqual(controller.write_fio_flags(proc.pid, st.st_dev, st.st_ino, case, pipes),
                                     {'direct': False})
                self.assertEqual(len(attempts), 4)
                attempts.clear()
                def without_inode(path, *args, **kwargs):
                    text = original(path, *args, **kwargs)
                    if path.parent.name == 'fdinfo':
                        text = '\n'.join(line for line in text.splitlines() if not line.startswith('ino:'))
                    if path == info_path:
                        attempts.append(path)
                    return text
                with patch.object(Path, 'read_text', without_inode):
                    self.assertEqual(controller.write_fio_flags(proc.pid, st.st_dev, st.st_ino, case, pipes),
                                     {'direct': False})
                self.assertEqual(len(attempts), 2)
                attempts.clear()
                def changed_flags_without_inode(path, *args, **kwargs):
                    text = without_inode(path, *args, **kwargs)
                    if path == info_path and len(attempts) == 1:
                        text = '\n'.join('flags:\t0' if line.startswith('flags:') else line
                                         for line in text.splitlines())
                    return text
                with patch.object(Path, 'read_text', changed_flags_without_inode), patch.object(controller.time, 'sleep'):
                    self.assertEqual(controller.write_fio_flags(proc.pid, st.st_dev, st.st_ino, case, pipes),
                                     {'direct': False})
                self.assertEqual(len(attempts), 4)
                attempts.clear()
                def missing_flags_without_inode(path, *args, **kwargs):
                    text = without_inode(path, *args, **kwargs)
                    return '\n'.join(line for line in text.splitlines() if not line.startswith('flags:')) if path == info_path else text
                with patch.object(Path, 'read_text', missing_flags_without_inode), patch.object(controller.time, 'sleep'):
                    with self.assertRaisesRegex(controller.SafetyError, 'cannot inspect writable fd 3'):
                        controller.write_fio_flags(proc.pid, st.st_dev, st.st_ino, case, pipes)
                self.assertEqual(len(attempts), 32)
                attempts.clear()
                def persistent(path, *args, **kwargs):
                    if path == info_path:
                        attempts.append(path)
                        raise PermissionError('uninspectable descriptor')
                    return original(path, *args, **kwargs)
                with patch.object(Path, 'read_text', persistent), patch.object(controller.time, 'sleep') as sleep:
                    with self.assertRaisesRegex(controller.SafetyError, 'cannot inspect writable fd 3'):
                        controller.write_fio_flags(proc.pid, st.st_dev, st.st_ino, case, pipes)
                self.assertEqual(len(attempts), 32)
                self.assertEqual(sleep.call_count, 31)
            finally:
                proc.kill()
                proc.communicate()

    def test_fd_guard_skips_a_vanished_descriptor(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scratch = root / 'scratch'
            scratch.write_bytes(b'x')
            case = root / 'case'
            case.mkdir()
            script = ('import os,time,sys; os.open(sys.argv[1],os.O_WRONLY); '
                      'print("ready",flush=True); time.sleep(30)')
            proc = subprocess.Popen([sys.executable, '-B', '-c', script, str(scratch)],
                                    stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                self.assertEqual(proc.stdout.readline(), b'ready\n')
                st = scratch.stat()
                pipes = {os.fstat(proc.stdout.fileno()).st_ino, os.fstat(proc.stderr.fileno()).st_ino}
                directory = Path('/proc') / str(proc.pid) / 'fd'
                original = Path.iterdir
                def entries(path):
                    rows = list(original(path))
                    return iter(rows + [directory / '999999']) if path == directory else iter(rows)
                with patch.object(Path, 'iterdir', entries):
                    self.assertEqual(controller.write_fio_flags(proc.pid, st.st_dev, st.st_ino, case, pipes),
                                     {'direct': False})
            finally:
                proc.kill()
                proc.communicate()

    def test_readonly_fd_guard_rejects_mixed_identity_and_flags(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / 'target'
            target.write_bytes(b'x')
            log = root / 'log'
            log.touch()
            script = ('import os,time,sys; os.open(sys.argv[1],int(sys.argv[3])); '
                      'os.open(sys.argv[2],os.O_WRONLY); print("ready",flush=True); time.sleep(30)')
            for mode in (os.O_RDONLY, os.O_WRONLY):
                with self.subTest(mode=mode):
                    proc = subprocess.Popen([sys.executable, '-B', '-c', script, str(target), str(log), str(mode)],
                                            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    try:
                        self.assertEqual(proc.stdout.readline(), b'ready\n')
                        st = target.stat()
                        expected = {'read_only': mode == os.O_RDONLY, 'direct': False}
                        self.assertEqual(controller.fio_open_flags(proc.pid, st.st_dev, st.st_ino), expected)
                        original = Path.read_text
                        def no_inode(path, *args, **kwargs):
                            text = original(path, *args, **kwargs)
                            return '\n'.join(line for line in text.splitlines() if not line.startswith('ino:'))
                        with patch.object(Path, 'read_text', no_inode):
                            self.assertEqual(controller.fio_open_flags(proc.pid, st.st_dev, st.st_ino), expected)
                        if mode == os.O_RDONLY:
                            info = Path('/proc') / str(proc.pid) / 'fdinfo'
                            original = Path.read_text
                            swapped = [False]
                            def read_text(path, *args, **kwargs):
                                if path == info / '3' and not swapped[0]:
                                    swapped[0] = True
                                    return original(info / '4', *args, **kwargs)
                                return original(path, *args, **kwargs)
                            with patch.object(Path, 'read_text', read_text), patch.object(controller.time, 'sleep'):
                                self.assertEqual(controller.fio_open_flags(proc.pid, st.st_dev, st.st_ino), expected)
                            self.assertTrue(swapped[0])
                    finally:
                        proc.kill()
                        proc.communicate()

    def test_guard_requires_observation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); scratch = root / 'fio-scratch'; scratch.mkdir()
            case = root / 'case'; case.mkdir()
            target = scratch / 'x'; target.write_bytes(b'x')
            st = target.stat()
            identity = {'path': str(target), 'device': st.st_dev, 'inode': st.st_ino}
            helper = case / 'helper'
            helper.write_text('#!/bin/sh\nsleep 0.1\n')
            helper.chmod(0o700)
            argv = floors.write_argv(helper, target, case / 'lat', self.spec(), identity)
            with self.assertRaises(controller.FioFailure):
                controller.run_write_fio(argv, identity, scratch, case, 5)

    def test_real_write_helper_all_modes(self):
        source = Path(controller.__file__).with_name('floors_ssd_write.c')
        original_stdin = os.dup(0)
        self.addCleanup(os.close, original_stdin)
        self.addCleanup(os.dup2, original_stdin, 0)
        with open(os.devnull, 'r+b') as null:
            os.dup2(null.fileno(), 0)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); scratch = root / 'fio-scratch'; scratch.mkdir()
            case = root / 'case'; case.mkdir()
            binary = case / 'helper'
            subprocess.run(['gcc', '-O2', '-Wall', '-Werror', '-o', str(binary), str(source)], check=True)
            target = scratch / 'x'
            with target.open('wb') as stream:
                stream.truncate(536870912)
            st = target.stat()
            identity = {'path': str(target), 'device': st.st_dev, 'inode': st.st_ino}
            for mode in ('buffered', 'fdatasync', 'direct'):
                with self.subTest(mode=mode):
                    spec = self.spec(mode)
                    log = case / (mode + '.log')
                    argv = floors.write_argv(binary, target, log, spec, identity)
                    # The helper streams progress to its log while it runs, so
                    # the deadline is only a safety cap; use the validated
                    # maximum so machine load cannot masquerade as a hang.
                    raw, flags = controller.run_write_fio(argv, identity, scratch, case, 60)
                    result = floors.write_result({'case_id': 'x', 'ssd_write': spec}, raw, log.read_bytes(), '1:2', flags['direct'])
                    rows = [tuple(map(int, line.split(','))) for line in log.read_text().splitlines()]
                    self.assertEqual(len(rows), result['logged_operations'])
                    for col, name in enumerate(('write', 'sync', 'total')):
                        values = sorted(row[col] for row in rows)
                        summary = result[name]
                        self.assertEqual(summary['n'], len(values))
                        self.assertEqual(summary['min'], values[0])
                        self.assertEqual(summary['max'], values[-1])
                        self.assertEqual(summary['mean'], sum(values) / len(values))
                        for key, numerator in (('p50', 500), ('p90', 900), ('p99', 990), ('p999', 999)):
                            self.assertEqual(summary[key], values[(len(values) * numerator + 999) // 1000 - 1])
            for mode in floors.APPEND_MODES:
                with self.subTest(mode=mode):
                    target = scratch / mode
                    with target.open('wb') as stream:
                        stream.truncate(536870912)
                    st = target.stat()
                    self.assertEqual(st.st_blocks, 0)
                    identity = {'path': str(target), 'device': st.st_dev, 'inode': st.st_ino}
                    spec = {'mode': mode, 'block_bytes': 1024, 'region_bytes': 536870912,
                            'queue_depth': 1, 'location': 'local_file', 'host': 'client',
                            'operations': 4096, 'warmup_operations': 512}
                    log = case / (mode + '.log')
                    argv = floors.write_argv(binary, target, log, spec, identity)
                    # Same safety-cap rationale as above.
                    raw, flags = controller.run_write_fio(argv, identity, scratch, case, 60)
                    rows = floors.parse_write_log(log.read_bytes(), spec)
                    result = floors.write_result({'case_id': 'x', 'ssd_write': spec}, raw,
                                                 log.read_bytes(), '1:2', flags['direct'])
                    self.assertEqual(len(rows), 4096)
                    self.assertEqual([row[3] for row in rows],
                                     [i * 1024 for i in range(512, 4608)])
                    self.assertEqual(result['logged_operations'], 4096)

    def test_helper_rejects_swapped_scratch_before_writing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); scratch = root / 'fio-scratch'; scratch.mkdir()
            case = root / 'case'; case.mkdir(); binary = case / 'helper'
            subprocess.run(['gcc', '-O2', '-Wall', '-Werror', '-o', str(binary),
                            str(Path(controller.__file__).with_name('floors_ssd_write.c'))], check=True)
            target = scratch / 'x'
            with target.open('wb') as stream:
                stream.truncate(536870912)
            st = target.stat()
            identity = {'device': st.st_dev, 'inode': st.st_ino}
            argv = floors.write_argv(binary, target, case / 'lat', self.spec(), identity)
            target.rename(scratch / 'original')
            with target.open('wb') as stream:
                stream.truncate(536870912)
            result = subprocess.run(argv, capture_output=True)
            self.assertEqual(result.returncode, 3)
            self.assertIn(b'scratch inode mismatch', result.stderr)
            self.assertEqual(target.stat().st_blocks, 0)
            self.assertFalse((case / 'lat').exists())

    def test_timeout_kills_process_group_and_preheat_cleans(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); scratch = root / 'fio-scratch'; scratch.mkdir()
            case = root / 'case'; case.mkdir(); target = scratch / 'x'; target.write_bytes(b'x')
            st = target.stat()
            identity = {'path': str(target), 'device': st.st_dev, 'inode': st.st_ino}
            helper = case / 'helper'
            helper.write_text('#!/bin/sh\nexec 3>"$1/$2"\nsleep 20\n')
            helper.chmod(0o700)
            argv = floors.write_argv(helper, target, case / 'lat', self.spec(), identity)
            with self.assertRaisesRegex(controller.FioFailure, 'timed out'):
                controller.run_write_fio(argv, identity, scratch, case, .2)
            with self.assertRaises(controller.DeadlineError):
                controller.provision_fio_scratch(scratch, 'preheat', size=1, deadline=0,
                                                 pattern='random', retain_cache=True)
            self.assertFalse((scratch / 'preheat.fio-scratch').exists())

    def test_sparse_append_scratch_and_identity_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            identity = controller.provision_fio_scratch(root, 'append', size=512 * 1024**2,
                                                        pattern='sparse', retain_cache=True)
            target = Path(identity['path'])
            self.assertEqual(target.stat().st_size, 512 * 1024**2)
            self.assertEqual(target.stat().st_blocks, 0)
            with self.assertRaises(controller.SafetyError):
                controller.remove_fio_scratch(root, 'append', {**identity, 'inode': identity['inode'] + 1})
            self.assertTrue(target.exists())
            controller.remove_fio_scratch(root, 'append', identity)
            self.assertFalse(target.exists())

    def test_sparse_provision_rejects_allocated_file_and_cleans(self):
        with tempfile.TemporaryDirectory() as tmp:
            def allocated(fd):
                return type('Stat', (), {'st_blocks': 1})()
            with patch.object(controller.os, 'fstat', side_effect=allocated):
                with self.assertRaises(controller.SafetyError):
                    controller.provision_fio_scratch(tmp, 'allocated', size=4096,
                                                     pattern='sparse', retain_cache=True)
            self.assertFalse((Path(tmp) / 'allocated.fio-scratch').exists())

    def test_append_guest_host_and_quiescence_guard(self):
        with tempfile.TemporaryDirectory() as tmp, ExitStack() as stack:
            root = Path(tmp)
            config = {'fleet': {'data_root': str(root), 'nodes': [{} for _ in range(5)]}, 'spec': {'mode': 'focused'},
                      'deadline_utc': (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()}
            job = {'case_kind': 'ssd_write_floor', 'case_id': 'x',
                   'ssd_write': {**self.spec('append_pwrite'), 'host': 'client'}}
            request = {'index': 0, 'action': 'ssd_write_floor', 'case_id': 'x',
                       'job': job, 'quiescent': True, 'timeout': 60}
            stack.enter_context(patch.object(controller, 'verify_local_identity', return_value={}))
            stack.enter_context(patch.object(controller, 'guarded', side_effect=lambda p, *a, **kw: Path(p)))
            with self.assertRaises(controller.SafetyError):
                controller.guest_operation(config, request, None)
            request['index'] = -1
            request['quiescent'] = False
            with self.assertRaises(controller.SafetyError):
                controller.guest_operation(config, request, None)

    def test_write_floor_dispatches_selected_host_and_checks_receipt(self):
        for mode in ('append_pwrite', 'fdatasync'):
            for host, index in (('client', -1), ('owner0', 0)):
                with self.subTest(mode=mode, host=host), tempfile.TemporaryDirectory() as tmp, ExitStack() as stack:
                    runtime = controller.Controller.__new__(controller.Controller)
                    runtime.root = Path(tmp)
                    runtime.config = {'binary_blake2b512': 'hash'}
                    runtime.active_nvme = runtime.active_tide = runtime.active_backend = None
                    runtime.monitoring_attempted = False
                    runtime.monitoring_events = []
                    job = {'case_id': 'case', 'case_kind': 'ssd_write_floor',
                           'ssd_write': {**self.spec(mode), 'host': host}}
                    stack.enter_context(patch.object(controller.cloud_campaign, 'validate_focused_job'))
                    stack.enter_context(patch.object(controller, 'require_supported_nvme_route'))
                    stack.enter_context(patch.object(runtime, 'guard'))
                    stack.enter_context(patch.object(runtime, 'stop_tide'))
                    stack.enter_context(patch.object(runtime, 'select_monitoring'))
                    stack.enter_context(patch.object(runtime, 'finish_monitoring'))
                    stack.enter_context(patch.object(runtime, 'remaining', return_value=60))
                    request = stack.enter_context(patch.object(runtime, 'request', return_value={'host_index': 1}))
                    with self.assertRaises(controller.SafetyError):
                        runtime.run_floor_case(job)
                    self.assertEqual(request.call_args.args[0], index)
                    self.assertTrue(request.call_args.args[1]['quiescent'])

    def test_guest_cleanup_on_success_failure_and_mismatch(self):
        for failure in (False, True):
            with tempfile.TemporaryDirectory() as tmp, ExitStack() as stack:
                root = Path(tmp)
                job = {'case_kind': 'ssd_write_floor', 'case_id': 'x', 'ssd_write': self.spec()}
                config = {'fleet': {'data_root': str(root), 'nodes': [{} for _ in range(5)]}, 'spec': {'mode': 'focused'},
                          'deadline_utc': (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()}
                request = {'index': 0, 'action': 'ssd_write_floor', 'case_id': 'x',
                           'job': job, 'quiescent': True, 'timeout': 60}
                stack.enter_context(patch.object(controller, 'verify_local_identity', return_value={}))
                stack.enter_context(patch.object(controller, 'guarded', side_effect=lambda p, *a, **kw: Path(p)))
                stack.enter_context(patch.object(controller.cloud_campaign, 'validate_focused_job'))
                stack.enter_context(patch.object(controller, 'media_device_snapshot', return_value={'major': 1, 'minor': 2}))
                def provision(path, case_id, **kw):
                    path.mkdir(); file = path / (case_id + '.fio-scratch'); file.write_bytes(b'x')
                    st = file.stat()
                    return {'path': str(file), 'size': 1, 'device': st.st_dev, 'inode': st.st_ino}
                stack.enter_context(patch.object(controller, 'provision_fio_scratch', side_effect=provision))
                stack.enter_context(patch.object(controller.shutil, 'which', return_value='/usr/bin/gcc'))
                def compile_helper(argv, **kw):
                    Path(argv[argv.index('-o') + 1]).write_bytes(b'helper')
                stack.enter_context(patch.object(controller.subprocess, 'run', side_effect=compile_helper))
                def measure(argv, *args):
                    if failure:
                        raise controller.FioFailure('failed')
                    Path(argv[3]).write_bytes(b'1,0,1,0\n')
                    return b'{\"total_operations\":1,\"logged_operations\":1,\"elapsed_ns\":6000000000}', {'direct': False}
                stack.enter_context(patch.object(controller, 'run_write_fio', side_effect=measure))
                result = controller.guest_operation(config, request, None)
                self.assertEqual('fio_failed' in result, failure)
                self.assertFalse((root / 'fio-scratch' / 'x.fio-scratch').exists())
                self.assertFalse((root / 'floors' / 'x').exists())
                request['job'] = {**job, 'case_kind': 'ssd_floor'}
                with self.assertRaises(controller.SafetyError):
                    controller.guest_operation(config, request, None)

    def test_admission_recomputes_and_rejects_tampering(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            job = {'case_id': 'x', 'case_kind': 'ssd_write_floor', 'ssd_write': self.spec('fdatasync')}
            def bind(name, content):
                raw = content if isinstance(content, bytes) else json.dumps(content).encode()
                (root / name).write_bytes(raw)
                return {'path': name, 'sha256': hashlib.sha256(raw).hexdigest()}
            raw = b'{\"total_operations\":2,\"logged_operations\":2,\"elapsed_ns\":6000000000}'
            log = b'10,20,30,0\n40,10,50,1024\n'
            evidence = floors.write_result(job, raw, log, '1:2', False)
            evidence['raw'] = bind('raw.json', raw)
            log_binding = bind('lat.log', log)
            evidence['lat_log'] = {**log_binding, 'rows': 2, 'columns': ['write_ns', 'sync_ns', 'total_ns', 'offset']}
            identity = {'device': 12, 'inode': 34}
            guest = Path('/mnt/example')
            command = {'argv': floors.write_argv(guest / 'floors/x/ssd_write', guest / 'fio-scratch/x.fio-scratch',
                                                 guest / 'floors/x/lat.log', job['ssd_write'], identity),
                       'scratch_identity': identity,
                       'build': {'source_sha256': hashlib.sha256(Path(controller.__file__).with_name('floors_ssd_write.c').read_bytes()).hexdigest(),
                                 'binary_sha256': 'a' * 64}}
            receipt = {'ssd_write_floor_result': bind('result.json', evidence),
                       'ssd_write_floor_lat_log': log_binding,
                       'phase_timing': bind('phase.json', {'schema_version': 1, 'case_id': 'x', 'spans': []}),
                       'ssd_write_command': bind('command.json', command)}
            self.assertEqual(analyze.admit_focused_artifacts(root, job, receipt, completed=True)['ssd_write_floor_result'], evidence)
            missing = dict(receipt)
            del missing['ssd_write_command']
            with self.assertRaises(ValueError):
                analyze.admit_focused_artifacts(root, job, missing, completed=True)
            for index, value in ((4, 'buffered'), (5, '8192'), (1, '/tmp/other'), (2, 'other.fio-scratch'),
                                 (3, '/tmp/lat.log'), (9, '13')):
                altered_command = copy.deepcopy(command)
                altered_command['argv'][index] = value
                bad = {**receipt, 'ssd_write_command': bind('bad-command.json', altered_command)}
                with self.subTest(index=index), self.assertRaises(ValueError):
                    analyze.admit_focused_artifacts(root, job, bad, completed=True)
            bad = {**receipt, 'ssd_write_command': {**receipt['ssd_write_command'], 'sha256': '0' * 64}}
            with self.assertRaises(ValueError):
                analyze.admit_focused_artifacts(root, job, bad, completed=True)
            altered = copy.deepcopy(evidence)
            altered['total']['p50'] = 99
            receipt['ssd_write_floor_result'] = bind('bad.json', altered)
            with self.assertRaises(ValueError):
                analyze.admit_focused_artifacts(root, job, receipt, completed=True)
            receipt['ssd_write_floor_result'] = bind('result.json', evidence)
            (root / 'lat.log').write_bytes(b'corrupt')
            with self.assertRaises(ValueError):
                analyze.admit_focused_artifacts(root, job, receipt, completed=True)

    def test_append_admission_binds_host_and_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = {'mode': 'append_mmap', 'block_bytes': 1024, 'region_bytes': 536870912,
                    'queue_depth': 1, 'location': 'local_file', 'host': 'client',
                    'operations': 4096, 'warmup_operations': 512}
            job = {'case_id': 'x', 'case_kind': 'ssd_write_floor', 'ssd_write': spec}
            def bind(name, content):
                raw = content if isinstance(content, bytes) else json.dumps(content).encode()
                (root / name).write_bytes(raw)
                return {'path': name, 'sha256': hashlib.sha256(raw).hexdigest()}
            raw = b'{"total_operations":4096,"logged_operations":4096,"elapsed_ns":1000000}'
            log = b''.join(f'10,20,30,{i * 1024}\n'.encode() for i in range(512, 4608))
            evidence = floors.write_result(job, raw, log, '1:2', False)
            evidence['raw'] = bind('raw.json', raw)
            binding = bind('lat.log', log)
            evidence['lat_log'] = {**binding, 'rows': 4096,
                                   'columns': ['write_ns', 'sync_ns', 'total_ns', 'offset']}
            identity = {'device': 12, 'inode': 34}
            guest = Path('/mnt/example')
            command = {'argv': floors.write_argv(guest / 'floors/x/ssd_write',
                       guest / 'fio-scratch/x.fio-scratch', guest / 'floors/x/lat.log', spec, identity),
                       'scratch_identity': identity,
                       'build': {'source_sha256': hashlib.sha256(Path(controller.__file__).with_name('floors_ssd_write.c').read_bytes()).hexdigest(),
                                 'binary_sha256': 'a' * 64}}
            receipt = {'host_index': -1, 'ssd_write_floor_result': bind('result.json', evidence),
                       'ssd_write_floor_lat_log': binding, 'ssd_write_command': bind('command.json', command),
                       'phase_timing': bind('phase.json', {'schema_version': 1, 'case_id': 'x', 'spans': []})}
            self.assertEqual(analyze.admit_focused_artifacts(root, job, receipt, completed=True)
                             ['ssd_write_floor_result'], evidence)
            with self.assertRaises(ValueError):
                analyze.admit_focused_artifacts(root, job, {**receipt, 'host_index': 0}, completed=True)
            evidence['host_index'] = 0
            with self.assertRaises(ValueError):
                analyze.admit_focused_artifacts(root, job,
                    {**receipt, 'ssd_write_floor_result': bind('bad.json', evidence)}, completed=True)


if __name__ == '__main__':
    unittest.main()
