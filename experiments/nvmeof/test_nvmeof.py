import json
import os
import signal
import stat
import shutil
import struct
from types import SimpleNamespace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import collect
import manage
import trace as kernel_trace


class SafetyTests(unittest.TestCase):
    def test_buffered_target_requires_successful_readback(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            attribute = namespace / 'buffered_io'
            attribute.write_text('0')
            manage.enable_buffered_io(namespace)
            self.assertEqual(attribute.read_text(), '1')
            with patch.object(manage, 'put'):
                attribute.write_text('0')
                with self.assertRaisesRegex(ValueError, 'readback'):
                    manage.enable_buffered_io(namespace)

    def test_rdma_preflight_reports_missing_active_provider_without_fallback(self):
        with patch.object(manage.os, 'geteuid', return_value=0), \
             patch.object(manage.shutil, 'which', return_value='/usr/bin/nvme'), \
             patch.object(Path, 'is_dir', return_value=True), \
             patch.object(Path, 'glob', return_value=[]):
            evidence = manage.readiness('rdma', True)
            self.assertFalse(evidence['ready'])
            self.assertEqual(evidence['transport'], 'rdma')
            self.assertIn('active_rdma_port', evidence['missing'])
            self.assertEqual(evidence['rdma_cm_path'], 'not_verified')
            with self.assertRaisesRegex(ValueError, 'active_rdma_port'):
                manage.preflight('rdma', True)
    def test_active_rdma_port_does_not_prove_connection_readiness(self):
        with tempfile.TemporaryDirectory() as tmp:
            port = Path(tmp) / 'device0' / 'ports' / '1'
            port.mkdir(parents=True)
            (port / 'state').write_text('4: ACTIVE')
            (port / 'link_layer').write_text('Ethernet')
            with patch.object(manage.os, 'geteuid', return_value=0), \
                 patch.object(manage.shutil, 'which', return_value='/usr/bin/nvme'), \
                 patch.object(Path, 'is_dir', return_value=True), \
                 patch.object(Path, 'glob', return_value=[port / 'state']):
                evidence = manage.readiness('rdma', True)
                self.assertTrue(evidence['local_prerequisites_ready'])
                self.assertFalse(evidence['ready'])
                self.assertEqual(evidence['provider_rc_capability'], 'not_verified')
                self.assertEqual(evidence['rdma_cm_path'], 'not_verified')
                self.assertEqual(manage.preflight('rdma', True), evidence)


    def test_manifest_rejects_traversal_symlink_and_bad_geometry(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            path = base / 'manifest.json'
            manifest = {'version': 1, 'nqn': 'nqn.2026-01.example:unit', 'nsid': 1,
                        'image_len': 4096, 'block_size': 4096, 'export_uuid': list(range(16))}
            path.write_text(json.dumps(manifest))
            self.assertEqual(manage.manifest(path)['uuid_string'], '00010203-0405-0607-0809-0a0b0c0d0e0f')
            with self.assertRaises(ValueError):
                manage.manifest(base / '..' / base.name / 'manifest.json')
            link = base / 'link'; link.symlink_to(base, target_is_directory=True)
            with self.assertRaises(ValueError):
                manage.manifest(link / 'manifest.json')
            manifest['image_len'] = 4095; path.write_text(json.dumps(manifest))
            with self.assertRaises(ValueError):
                manage.manifest(path)

    def test_cleanup_only_owned_and_rejects_changed_link(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp); config = base / 'config'; state = base / 'state'; state.mkdir()
            sub = config / 'subsystems' / 'nqn.2026-01.example:a'; sub.mkdir(parents=True)
            host = config / 'hosts' / 'nqn.2026-01.example:h'; host.mkdir(parents=True)
            port = config / 'ports' / '8'; (port / 'subsystems').mkdir(parents=True)
            other = config / 'subsystems' / 'nqn.2026-01.example:other'; other.mkdir()
            link = port / 'subsystems' / sub.name; link.symlink_to(other)
            s = {'nqn': sub.name, 'hostnqn': host.name, 'port_id': 8, 'nsid': 1,
                 'port': True, 'subsystem': True, 'host': True}
            for flag, path in [('subsystem', sub), ('host', host), ('port', port)]:
                s[flag + '_identity'] = manage.object_identity(path)
            with patch.object(manage, 'CONFIG', config), self.assertRaises(ValueError):
                manage.target_cleanup(state, s)
            self.assertTrue(other.exists()); self.assertTrue(link.is_symlink())
            s.update(port=False, subsystem=False, host=False)
            with patch.object(manage, 'CONFIG', config):
                manage.target_cleanup(state, s)
            self.assertTrue(sub.exists()); self.assertTrue(host.exists()); self.assertTrue(port.exists())
            self.assertEqual(json.loads((state / 'state.json').read_text())['phase'], 'cleaned')

    def test_cleanup_retry_rejects_replaced_namespace_before_any_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp); config = base / 'config'; state = base / 'state'; state.mkdir()
            sub = config / 'subsystems' / 'nqn.2026-01.example:a'
            ns = sub / 'namespaces' / '1'; ns.mkdir(parents=True)
            host = config / 'hosts' / 'nqn.2026-01.example:h'; host.mkdir(parents=True)
            port = config / 'ports' / '8'; (port / 'subsystems').mkdir(parents=True)
            link = port / 'subsystems' / sub.name; link.symlink_to(sub)
            s = {'nqn': sub.name, 'hostnqn': host.name, 'port_id': 8, 'nsid': 1,
                 'port': True, 'subsystem': True, 'host': True, 'namespace': True}
            for flag, path in [('subsystem', sub), ('host', host), ('port', port), ('namespace', ns)]:
                s[flag + '_identity'] = manage.object_identity(path)
            # Simulate interrupted removal and another actor creating a same-name namespace.
            ns.rename(base / 'old-namespace')
            ns.mkdir()
            (ns / 'enable').write_text('1')
            with patch.object(manage, 'CONFIG', config), self.assertRaisesRegex(ValueError, 'replaced'):
                manage.target_cleanup(state, s)
            self.assertTrue(link.is_symlink())
            self.assertEqual((ns / 'enable').read_text(), '1')
            self.assertTrue(port.exists())
            self.assertFalse((state / 'state.json').exists())

    def test_disconnect_rejects_recycled_controller(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); c = root / 'nvme7'; c.mkdir()
            (c / 'subsysnqn').write_text('nqn.2026-01.example:a\n')
            (c / 'transport').write_text('tcp\n'); (c / 'address').write_text('traddr=127.0.0.1,trsvcid=4420\n')
            s = {'controller': 'nvme7', 'nqn': 'nqn.2026-01.example:a', 'transport': 'tcp',
                 'controller_inode': c.stat().st_ino + 1, 'controller_address': 'traddr=127.0.0.1,trsvcid=4420'}
            with patch.object(manage, 'SYS_NVME', root), patch.object(manage, 'run') as run:
                with self.assertRaises(ValueError): manage.disconnect(s)
                run.assert_not_called()
                s['controller_inode'] = c.stat().st_ino
                manage.disconnect(s)
                self.assertEqual(run.call_args.args[0], ['nvme', 'disconnect', '-d', '/dev/nvme7'])

    def test_controller_snapshot_rescans_after_concurrent_removal(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); ctrl = root / 'nvme3'; ctrl.mkdir()
            for name, value in (('subsysnqn', 'nqn.2026-01.example:a'),
                                ('transport', 'tcp'), ('address', 'traddr=127.0.0.1,trsvcid=4420')):
                (ctrl / name).write_text(value)
            original = Path.read_text
            removed = []
            def racing_read(path, *args, **kwargs):
                if path == ctrl / 'subsysnqn' and not removed:
                    removed.append(True)
                    shutil.rmtree(ctrl)
                    raise FileNotFoundError('controller hot-unplugged')
                return original(path, *args, **kwargs)
            with patch.object(manage, 'SYS_NVME', root), patch.object(Path, 'read_text', racing_read):
                self.assertEqual(manage.controller_snapshot(timeout=1), {})
            self.assertEqual(removed, [True])

    def test_controller_snapshot_rejects_visible_incomplete_and_permission_errors(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); ctrl = root / 'nvme3'; ctrl.mkdir()
            with patch.object(manage, 'SYS_NVME', root):
                with self.assertRaisesRegex(ValueError, 'did not stabilize'):
                    manage.controller_snapshot(timeout=.03)
                with patch.object(Path, 'read_text', side_effect=PermissionError('denied')):
                    with self.assertRaises(PermissionError):
                        manage.controller_snapshot(timeout=.1)
                self.assertTrue(ctrl.exists())

    def test_disconnect_failure_is_benign_only_after_strict_absence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); ctrl = root / 'nvme7'; ctrl.mkdir()
            (ctrl / 'subsysnqn').write_text('nqn.2026-01.example:a')
            (ctrl / 'transport').write_text('tcp')
            (ctrl / 'address').write_text('traddr=127.0.0.1,trsvcid=4420')
            state = {'controller': 'nvme7', 'nqn': 'nqn.2026-01.example:a', 'transport': 'tcp',
                     'controller_inode': ctrl.stat().st_ino, 'controller_address': 'traddr=127.0.0.1,trsvcid=4420'}
            with patch.object(manage, 'SYS_NVME', root):
                with patch.object(manage, 'run', side_effect=ValueError('disconnect failed')) as run:
                    with self.assertRaisesRegex(ValueError, 'disconnect failed'):
                        manage.disconnect(state)
                    self.assertEqual(run.call_args.args[0], ['nvme', 'disconnect', '-d', '/dev/nvme7'])
                def removed(*args, **kwargs):
                    shutil.rmtree(ctrl)
                    raise ValueError('controller already removed')
                with patch.object(manage, 'run', side_effect=removed) as run:
                    manage.disconnect(state)
                    self.assertEqual(run.call_args.args[0], ['nvme', 'disconnect', '-d', '/dev/nvme7'])

    def test_absent_recorded_name_with_live_matching_nqn_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); ctrl = root / 'nvme8'; ctrl.mkdir()
            (ctrl / 'subsysnqn').write_text('nqn.2026-01.example:a')
            (ctrl / 'transport').write_text('tcp')
            (ctrl / 'address').write_text('traddr=127.0.0.1,trsvcid=4420')
            state = {'controller': 'nvme7', 'nqn': 'nqn.2026-01.example:a', 'transport': 'tcp',
                     'controller_inode': 7, 'controller_address': 'traddr=127.0.0.1,trsvcid=4420'}
            with patch.object(manage, 'SYS_NVME', root), patch.object(manage, 'run') as run:
                with self.assertRaisesRegex(ValueError, 'matching NQN remains'):
                    manage.disconnect(state)
                run.assert_not_called()

    def test_connection_selection_ignores_unrelated_name_reuse_and_admits_exact_reuse(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            def create(name, nqn, service):
                path = root / name; path.mkdir()
                (path / 'subsysnqn').write_text(nqn)
                (path / 'transport').write_text('tcp')
                (path / 'address').write_text(f'traddr=10.0.0.1,trsvcid={service}')
                return path
            first = create('nvme3', 'nqn.2026-01.example:other', 4420)
            second = create('nvme4', 'nqn.2026-01.example:old', 4421)
            wanted = 'nqn.2026-01.example:wanted'
            with patch.object(manage, 'SYS_NVME', root):
                before = manage.controller_snapshot()
                first.rename(root / 'old-first')
                second.rename(root / 'old-second')
                create('nvme3', 'nqn.2026-01.example:unrelated', 4422)
                current = manage.controller_snapshot()
                self.assertEqual(manage.new_controller_matches(before, current, wanted, 'tcp', '10.0.0.1', 4420), [])
                created = create('nvme4', wanted, 4420)
                current = manage.controller_snapshot()
                self.assertNotEqual(before['nvme4']['inode'], current['nvme4']['inode'])
                self.assertEqual(manage.new_controller_matches(before, current, wanted, 'tcp', '10.0.0.1', 4420), ['nvme4'])
                (created / 'address').write_text('traddr=10.0.0.1,trsvcid=4423')
                with self.assertRaisesRegex(ValueError, 'ambiguous'):
                    manage.new_controller_matches(before, manage.controller_snapshot(), wanted, 'tcp', '10.0.0.1', 4420)
                (created / 'address').write_text('traddr=10.0.0.1,trsvcid=4420')
                create('nvme5', wanted, 4420)
                with self.assertRaisesRegex(ValueError, 'ambiguous'):
                    manage.new_controller_matches(before, manage.controller_snapshot(), wanted, 'tcp', '10.0.0.1', 4420)

    def test_discovery_requires_same_record_and_endpoint(self):
        item = {'subnqn': 'nqn.2026-01.example:a', 'trtype': 'tcp', 'traddr': '127.0.0.1', 'trsvcid': '4420'}
        self.assertTrue(manage.discovery_has_endpoint({'records': [item]}, item['subnqn'], 'tcp', '127.0.0.1', 4420))
        self.assertFalse(manage.discovery_has_endpoint({'records': [item]}, item['subnqn'], 'rdma', '127.0.0.1', 4420))
        self.assertFalse(manage.discovery_has_endpoint({'records': [item]}, item['subnqn'], 'tcp', '127.0.0.2', 4420))

    def test_untrusted_state_ancestor_and_exclusive_temp(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            insecure = root / 'insecure'; insecure.mkdir(mode=0o777); insecure.chmod(0o777)
            with self.assertRaises(ValueError):
                manage.trusted_directory(insecure)
            state = root / 'state'; state.mkdir()
            victim = root / 'victim'; victim.write_text('keep')
            name = '.state-' + 'a' * 32
            (state / name).symlink_to(victim)
            with patch.object(manage.uuid, 'uuid4', return_value=SimpleNamespace(hex='a' * 32)):
                with self.assertRaises(FileExistsError):
                    manage.save(state, {'kind': 'target'})
            self.assertEqual(victim.read_text(), 'keep')

    def test_namespace_head_and_multiple_controller_rejection(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); ctrls = root / 'controllers'; ctrls.mkdir()
            ctrl = ctrls / 'nvme4'; ctrl.mkdir()
            subs = root / 'subsystems'; subs.mkdir()
            subsystem = subs / 'nvme-subsys3'; subsystem.mkdir()
            (subsystem / 'nvme4').symlink_to(ctrl)
            (subsystem / 'subsysnqn').write_text('nqn.2026-01.example:a')
            blocks = root / 'block'; blocks.mkdir()
            ns = blocks / 'nvme3n1'; ns.mkdir()
            (ns / 'device').symlink_to(subsystem)
            (ns / 'nsid').write_text('1')
            (ns / 'uuid').write_text('00010203-0405-0607-0809-0a0b0c0d0e0f')
            (ns / 'wwid').write_text('uuid.00010203-0405-0607-0809-0a0b0c0d0e0f')
            m = {'nqn': 'nqn.2026-01.example:a', 'nsid': 1,
                 'uuid_string': '00010203-0405-0607-0809-0a0b0c0d0e0f'}
            with patch.object(manage, 'SYS_SUBSYSTEM', subs), patch.object(manage, 'SYS_BLOCK', blocks):
                self.assertEqual(manage.namespace_candidates(ctrl, m), [ns])
                second = ctrls / 'nvme5'; second.mkdir()
                (subsystem / 'nvme5').symlink_to(second)
                with self.assertRaisesRegex(ValueError, 'multiple'):
                    manage.namespace_candidates(ctrl, m)

    def test_namespace_inventory_rescans_disappearing_device_and_rejects_unknown(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); ctrl = root / 'nvme4'; ctrl.mkdir()
            subs = root / 'subsystems'; subs.mkdir()
            subsystem = subs / 'nvme-subsys3'; subsystem.mkdir()
            (subsystem / ctrl.name).symlink_to(ctrl)
            (subsystem / 'subsysnqn').write_text('nqn.2026-01.example:a')
            blocks = root / 'block'; blocks.mkdir()
            ns = blocks / 'nvme3n1'; ns.mkdir()
            (ns / 'device').symlink_to(subsystem)
            (ns / 'nsid').write_text('1')
            (ns / 'uuid').write_text('00010203-0405-0607-0809-0a0b0c0d0e0f')
            (ns / 'wwid').write_text('uuid.00010203-0405-0607-0809-0a0b0c0d0e0f')
            manifest = {'nqn': 'nqn.2026-01.example:a', 'nsid': 1,
                        'uuid_string': '00010203-0405-0607-0809-0a0b0c0d0e0f'}
            original = Path.read_text
            def removed(path, *args, **kwargs):
                if path == ns / 'uuid':
                    shutil.rmtree(ns)
                    raise FileNotFoundError('namespace disappeared')
                return original(path, *args, **kwargs)
            with patch.object(manage, 'SYS_SUBSYSTEM', subs), patch.object(manage, 'SYS_BLOCK', blocks):
                with patch.object(Path, 'read_text', removed):
                    self.assertEqual(manage.namespace_candidates(ctrl, manifest, timeout=1), [])
                ns.mkdir(); (ns / 'device').symlink_to(subsystem); (ns / 'nsid').write_text('1')
                with self.assertRaisesRegex(ValueError, 'did not stabilize'):
                    manage.namespace_candidates(ctrl, manifest, timeout=.03)
                with patch.object(Path, 'read_text', side_effect=PermissionError('denied')):
                    with self.assertRaises(PermissionError):
                        manage.namespace_candidates(ctrl, manifest, timeout=.1)

    def test_host_readonly_is_set_and_read_back(self):
        reads = iter([0, 1])
        def ioctl(fd, request, value, *args):
            if request == 0x125e:
                value[:] = struct.pack('i', next(reads))
            else:
                self.assertEqual(request, 0x125d)
                self.assertEqual(struct.unpack('i', value)[0], 1)
        with patch.object(manage.os, 'open', return_value=88), \
             patch.object(manage.os, 'fstat', return_value=SimpleNamespace(st_mode=stat.S_IFBLK)), \
             patch.object(manage.os, 'close'), patch.object(manage.fcntl, 'ioctl', side_effect=ioctl) as call:
            manage.ensure_host_readonly('/dev/nvme4n1')
            self.assertEqual([c.args[1] for c in call.call_args_list], [0x125e, 0x125d, 0x125e])

    def test_trace_recovery_refuses_replaced_instance(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); instances = root / 'instances'; instances.mkdir()
            instance = instances / ('tidehunter-' + 'a' * 32); instance.mkdir()
            (instance / 'tracing_on').write_text('1')
            state = {'instance': instance.name, 'instance_identity': [0, 0]}
            with patch.object(kernel_trace, 'TRACEFS', root), self.assertRaisesRegex(ValueError, 'replaced'):
                kernel_trace.cleanup_instance(root, state)
            self.assertEqual((instance / 'tracing_on').read_text(), '1')
            with self.assertRaises(InterruptedError):
                kernel_trace.interrupted(signal.SIGTERM, None)
            state.update(instance_identity=manage.object_identity(instance), phase='configured', pid=12345)
            real_rmdir = Path.rmdir
            def remove_fixture(path):
                self.assertEqual((path / 'tracing_on').read_text(), '0')
                (path / 'tracing_on').unlink()
                real_rmdir(path)
            with patch.object(kernel_trace, 'TRACEFS', root), \
                 patch.object(manage, 'load_state', return_value=(root, state)), \
                 patch.object(kernel_trace.os, 'kill', side_effect=ProcessLookupError), \
                 patch.object(Path, 'rmdir', remove_fixture):
                self.assertEqual(kernel_trace.recover('/unused')['phase'], 'cleaned')
            self.assertFalse(instance.exists())
            self.assertEqual(json.loads((root / 'state.json').read_text())['phase'], 'cleaned')

    def test_trace_early_stop_writes_raw_summary_and_cleans_instance(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            instances = root / 'instances'
            instances.mkdir()
            output = root / 'output'
            for event in kernel_trace.EVENTS:
                fmt = root / 'events' / event / 'format'
                fmt.parent.mkdir(parents=True, exist_ok=True)
                fmt.write_text('format')
            real_mkdir = Path.mkdir
            real_read_text = Path.read_text
            real_rmdir = Path.rmdir
            def mkdir(path, *args, **kwargs):
                real_mkdir(path, *args, **kwargs)
                if path.parent == instances:
                    for name in ('tracing_on', 'buffer_size_kb', 'trace_clock', 'trace'):
                        (path / name).write_text('[mono]' if name == 'trace_clock' else '')
                    for event in kernel_trace.EVENTS:
                        enabled = path / 'events' / event / 'enable'
                        enabled.parent.mkdir(parents=True, exist_ok=True)
                        enabled.write_text('0')
                    stats = path / 'per_cpu' / 'cpu0' / 'stats'
                    stats.parent.mkdir(parents=True)
                    stats.write_text('overrun: 0\ndropped events: 0\n')
            def read_text(path, *args, **kwargs):
                if str(path) == '/sys/class/block/nvme0n1/dev':
                    return '259:0'
                return real_read_text(path, *args, **kwargs)
            def new_state(path, state):
                Path(path).mkdir()
                directory = manage.StateDirectory(os.open(path, os.O_RDONLY | os.O_DIRECTORY))
                manage.save(directory, state)
                return directory
            def early_stop(directory, seconds):
                instance = next(instances.iterdir())
                (instance / 'trace').write_text(
                    'task [001] .... 10.000001: nvmet_req_init: nvmet7: qid=1, cmdid=3\n'
                    'task [001] .... 10.000006: nvmet_req_complete: nvmet7: qid=1, cmdid=3\n')
                kernel_trace.request_stop(directory)
                self.assertTrue(kernel_trace.stop_requested(directory))
            def rmdir(path):
                if path.parent == instances:
                    self.assertEqual((path / 'tracing_on').read_text(), '0')
                    shutil.rmtree(path)
                else:
                    real_rmdir(path)
            args = SimpleNamespace(seconds=60, buffer_kb=256, ctrl_id=7,
                                   block_device='nvme0n1', output=str(output))
            with patch.object(kernel_trace, 'TRACEFS', root), \
                 patch.object(kernel_trace.os, 'geteuid', return_value=0), \
                 patch.object(manage, 'new_state', side_effect=new_state), \
                 patch.object(Path, 'mkdir', mkdir), patch.object(Path, 'read_text', read_text), \
                 patch.object(Path, 'rmdir', rmdir), \
                 patch.object(kernel_trace, 'wait_capture', side_effect=early_stop):
                result = kernel_trace.capture(args)
            self.assertEqual(result['stages']['nvmet']['count'], 1)
            self.assertIn('nvmet_req_complete', (output / 'trace.txt').read_text())
            self.assertEqual(json.loads((output / 'summary.json').read_text()), result)
            state = json.loads((output / 'state.json').read_text())
            self.assertEqual(state['phase'], 'cleaned')
            self.assertEqual(state['result'], result)
            self.assertFalse(list(instances.iterdir()))

    def test_trace_start_waits_for_running_and_stop_returns_cleaned_result(self):
        process = SimpleNamespace(pid=123, poll=lambda: None)
        directory = SimpleNamespace()
        args = SimpleNamespace(seconds=30, buffer_kb=64, ctrl_id=7,
                               block_device='nvme0n1', output='/owned/trace')
        states = iter([(directory, {'pid': 123, 'phase': 'configured'}),
                       (directory, {'pid': 123, 'phase': 'running', 'start_monotonic_ns': 55})])
        with patch.object(kernel_trace.subprocess, 'Popen', return_value=process) as spawn, \
             patch.object(manage, 'load_state', side_effect=lambda *a: next(states)), \
             patch.object(kernel_trace.time, 'sleep'):
            self.assertEqual(kernel_trace.start(args)['start_monotonic_ns'], 55)
        self.assertIn('--seconds', spawn.call_args.args[0])
        result = {'available': True, 'stages': {}}
        states = iter([(directory, {'pid': 123, 'phase': 'running'}),
                       (directory, {'pid': 123, 'phase': 'running'}),
                       (directory, {'pid': 123, 'phase': 'cleaned', 'result': result})])
        with patch.object(manage, 'load_state', side_effect=lambda *a: next(states)), \
             patch.object(kernel_trace, 'request_stop') as request, \
             patch.object(kernel_trace.os, 'kill'), patch.object(kernel_trace.time, 'sleep'):
            self.assertEqual(kernel_trace.stop('/owned/trace'), result)
            request.assert_called_once_with(directory)

    def test_trace_start_returns_capture_completed_between_polls(self):
        process = SimpleNamespace(pid=123, poll=lambda: None)
        directory = SimpleNamespace()
        args = SimpleNamespace(seconds=0.001, buffer_kb=64, ctrl_id=7,
                               block_device='nvme0n1', output='/owned/trace')
        result = {'available': True, 'stages': {}}
        states = iter([(directory, {'pid': 123, 'phase': 'configured'}),
                       (directory, {'pid': 123, 'phase': 'cleaned', 'result': result})])
        with patch.object(kernel_trace.subprocess, 'Popen', return_value=process), \
             patch.object(manage, 'load_state', side_effect=lambda *a: next(states)), \
             patch.object(kernel_trace.time, 'sleep'):
            self.assertEqual(kernel_trace.start(args), result)

    def test_trace_stop_reports_dead_collector_and_cleanup_failure(self):
        directory = SimpleNamespace()
        state = {'pid': 123, 'phase': 'running'}
        with patch.object(manage, 'load_state', return_value=(directory, state)), \
             patch.object(kernel_trace, 'request_stop'), \
             patch.object(kernel_trace.os, 'kill', side_effect=ProcessLookupError):
            self.assertIn('--recover', kernel_trace.stop('/owned/trace')['reason'])
        state = {'pid': 123, 'phase': 'cleaned', 'error': 'capture failed'}
        with patch.object(manage, 'load_state', return_value=(directory, state)), \
             patch.object(kernel_trace, 'request_stop') as request:
            self.assertEqual(kernel_trace.stop('/owned/trace')['reason'], 'capture failed')
            request.assert_not_called()

    def test_counters_are_separate_and_counter_resets_fail_closed(self):
        a = {'start_ns': 1, 'end_ns': 2, 'values': {'read_sectors': 4, 'in_flight': 2}}
        b = {'start_ns': 3, 'end_ns': 4, 'values': {'read_sectors': 9, 'in_flight': 1}}
        self.assertEqual(collect.delta(a, b)['delta'], {'read_sectors': 5})
        b['values']['read_sectors'] = 3
        with self.assertRaises(ValueError): collect.delta(a, b)
        with self.assertRaises(ValueError): collect.block_snapshot('../nvme0n1')

    def test_target_trace_pairs_and_ambiguous_requests(self):
        lines = [
            'task [001] .... 10.000001: nvmet_req_init: nvmet7: qid=1, cmdid=3, nsid=1, cmd=(read)',
            'task [001] .... 10.000002: block_rq_issue: 259,0 R 0 () 128 + 8 [task]',
            'task [001] .... 10.000004: block_rq_complete: 259,0 R () 128 + 8 [0]',
            'task [001] .... 10.000006: nvmet_req_complete: nvmet7: qid=1, cmdid=3, res=0, status=0',
            'task [001] .... 10.000007: nvmet_req_init: nvmet7: qid=1, cmdid=4, nsid=1',
            'task [001] .... 10.000008: nvmet_req_init: nvmet7: qid=1, cmdid=4, nsid=1',
            'task [001] .... 10.000009: nvmet_req_complete: nvmet7: qid=1, cmdid=4, res=0, status=0',
        ]
        result = kernel_trace.summarize(lines, 7, (259, 0))
        self.assertEqual(result['nvmet']['count'], 1)
        self.assertEqual(result['nvmet']['sum_ns'], 5000)
        self.assertEqual(result['block']['sum_ns'], 2000)
        self.assertGreater(result['nvmet']['ambiguous'], 0)
        self.assertEqual(kernel_trace.summarize(lines, 8, (259, 1))['nvmet']['count'], 0)

    def test_invalid_commands_rejected(self):
        for value in ('../escape', 'nqn.2026-01.foo/bar', 'nqn.2026-01.foo\nbar'):
            with self.assertRaises(ValueError): manage.nqn(value)
        with patch('sys.argv', ['manage.py', 'host-disconnect', '--state', '/x', '--global']):
            with self.assertRaises(SystemExit): manage.main()


if __name__ == '__main__':
    unittest.main()
