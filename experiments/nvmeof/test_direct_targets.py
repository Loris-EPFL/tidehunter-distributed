"""Sealed target policy fixtures. These tests do not exercise NVMe hardware."""
import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import campaign_targets
import manage
import test_nvmeof


class DirectTargetTests(unittest.TestCase):
    def test_buffered_policy_defaults_to_one_and_direct_requires_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            ns = Path(tmp)
            attr = ns / 'buffered_io'
            for target, expected in (('kernel', 1), ('kernel_direct', 0)):
                attr.write_text(str(1 - expected))
                observed = (manage.enable_buffered_io(ns) if target == 'kernel'
                            else manage.enable_buffered_io(ns, target))
                self.assertEqual(observed, expected)
                self.assertEqual(attr.read_text(), str(expected))
                attr.write_text(str(1 - expected))
                with patch.object(manage, 'put'), self.assertRaisesRegex(ValueError, 'readback'):
                    manage.enable_buffered_io(ns, target)
            with self.assertRaisesRegex(ValueError, 'kernel target kind'):
                manage.enable_buffered_io(ns, 'spdk')

    def setup_fixture(self, target, mismatch=False, campaign=False):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / 'config'
            for name in ('subsystems', 'hosts', 'ports'):
                (config / name).mkdir(parents=True)
            export = root / 'export'
            export.mkdir()
            image = export / 'image.wal'
            image.write_bytes(bytes(4096))
            manifest = {'nqn': 'nqn.2026-01.test:sealed', 'nsid': 1,
                        'uuid_string': '07070707-0707-0707-0707-070707070707'}
            writes, states = [], []
            original_mkdir = Path.mkdir
            original_put = manage.put

            def mkdir(path, *args, **kwargs):
                original_mkdir(path, *args, **kwargs)
                if path.parent == config / 'subsystems':
                    (path / 'allowed_hosts').mkdir()
                    (path / 'namespaces').mkdir()
                elif path.parent == config / 'ports':
                    (path / 'subsystems').mkdir()
                elif path.parent.name == 'namespaces':
                    (path / 'enable').write_text('0')
                    (path / 'buffered_io').write_text('1')

            def put(path, value):
                if path.name == 'buffered_io':
                    self.assertEqual((path.parent / 'enable').read_text(), '0')
                    if mismatch:
                        return
                if path.name == 'enable' and str(value) == '1':
                    self.assertEqual((path.parent / 'buffered_io').read_text(),
                                     '0' if target == 'kernel_direct' else '1')
                writes.append((path.name, str(value)))
                original_put(path, value)

            args = SimpleNamespace(manifest=str(export / 'manifest.json'), state=str(root / 'state'),
                transport='rdma', address='10.0.0.1', service=4420, port_id=1000,
                hostnqn='nqn.2026-01.test:host')
            if target is not None:
                args.target_kind = target
            with patch.object(manage, 'CONFIG', config), patch.object(manage, 'preflight', return_value={}), \
                 patch.object(manage, 'manifest', return_value=manifest), \
                 patch.object(manage, 'image_for', return_value=image), \
                 patch.object(manage, 'new_state', return_value=SimpleNamespace(close=lambda: None)), \
                 patch.object(manage, 'save', side_effect=lambda d, s: states.append(copy.deepcopy(s))), \
                 patch.object(manage, 'target_cleanup') as cleanup, \
                 patch.object(Path, 'mkdir', mkdir), patch.object(manage, 'put', side_effect=put):
                if campaign:
                    args.ready, args.export_root = '/ready', str(export)
                    args.base_port_id, args.base_service = 1000, 4420
                    entry = {'manifest': args.manifest, 'uuid': manifest['uuid_string']}
                    with patch.object(campaign_targets, 'load_exports', return_value=[entry]), \
                         patch.object(campaign_targets, 'new_campaign_state',
                                      return_value=SimpleNamespace(close=lambda: None)):
                        result = campaign_targets.setup(args)
                elif mismatch:
                    with self.assertRaisesRegex(ValueError, 'readback'):
                        manage.target_setup(args)
                    cleanup.assert_called_once()
                    self.assertNotIn(('enable', '1'), writes)
                    self.assertFalse(any(s.get('target_cache_verified') for s in states))
                    return
                else:
                    result = manage.target_setup(args)
            self.assertLess(next(i for i, w in enumerate(writes) if w[0] == 'buffered_io'),
                            writes.index(('enable', '1')))
            return result

    def test_setup_default_and_direct_record_readback_before_enable(self):
        for target, expected in ((None, 1), ('kernel_direct', 0)):
            with self.subTest(target=target):
                result = self.setup_fixture(target)
                self.assertEqual(result['buffered_io'], expected)
                self.assertTrue(result['target_cache_verified'])
                self.assertEqual(result['target_implementation'], target or 'kernel')
                self.assertEqual(result['phase'], 'published_unprotected')

    def test_direct_setup_mismatch_never_enables(self):
        self.setup_fixture('kernel_direct', mismatch=True)

    def test_campaign_receipt_preserves_explicit_kind_and_readback(self):
        for target, expected in (('kernel', 1), ('kernel_direct', 0)):
            result = self.setup_fixture(target, campaign=True)
            # The complete target receipt is the existing on-disk evidence payload.
            saved = json.loads(json.dumps(result))
            self.assertEqual(saved['target_implementation'], target)
            entry = saved['exports'][0]
            self.assertEqual(entry['target'], target)
            self.assertEqual(entry['target_receipt']['buffered_io'], expected)

    def test_direct_host_requires_kernel_write_protection(self):
        error, _, commands, disconnect = test_nvmeof.SafetyTests().host_connect_with(
            5, 127, export_target='kernel_direct')
        self.assertIn('write protection', error)
        self.assertTrue(any(c[1] == 'set-feature' for c in commands))
        disconnect.assert_called_once()

    def test_direct_binding_is_explicit_and_unknown_kinds_fail(self):
        self.assertEqual(manage.export_target('kernel_direct'), 'kernel_direct')
        for value in ('direct', 'unknown', None, {'kernel_direct': {}}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                manage.export_target(value)


if __name__ == '__main__':
    unittest.main()
