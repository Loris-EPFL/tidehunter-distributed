import json
import os
import signal
import stat
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
