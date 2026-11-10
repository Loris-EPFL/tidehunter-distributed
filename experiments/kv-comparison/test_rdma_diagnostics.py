import tempfile
import unittest
from pathlib import Path
import sys
from unittest.mock import patch

import rdma_diagnostics


class RdmaDiagnosticsTests(unittest.TestCase):
    def test_resource_capture_parses_and_preserves_provenance(self):
        def command(argv):
            raw = ('hca_id: rdma0\n fw_ver: 1.2\n max_mr: 1024\n max_cqe: 0xffff\n'
                   if argv[0] == 'ibv_devinfo' else
                   '[{"ifindex":1,"ifname":"rdma0","mr":0,"qp":4,"qp-max":100}]')
            return {'argv': argv, 'available': True, 'truncated': False, 'error': None, 'raw': raw}
        with patch.object(rdma_diagnostics, '_bounded_command', side_effect=command):
            result = rdma_diagnostics.capture_resources('rdma0')
        self.assertEqual(result['capabilities']['fields']['max_cqe'], '0xffff')
        self.assertEqual(result['resources']['devices'][0]['counts'], {'mr': 0, 'qp': 4})
        self.assertEqual(result['capabilities']['argv'], ['ibv_devinfo', '-v', '-d', 'rdma0'])
        self.assertEqual(result['resources']['devices'][0]['reported_limits'], {'qp-max': 100})
        self.assertIn('"mr":0', result['resources']['raw'])
        with self.assertRaises(ValueError):
            rdma_diagnostics.capture_resources('../bad')

    def test_multiple_adapters_keep_capabilities_separate(self):
        caps = {'argv': ['ibv_devinfo', '-v'], 'available': True, 'truncated': False,
                'error': None, 'raw': ('hca_id: rdma0\n fw_ver: 1\n max_mr: 100\n'
                                       'hca_id: rdma1\n fw_ver: 2\n max_mr: 200\n')}
        resources = {'argv': ['rdma'], 'available': False, 'truncated': False,
                     'error': 'not installed', 'raw': ''}
        with patch.object(rdma_diagnostics, '_bounded_command', side_effect=[caps, resources]):
            result = rdma_diagnostics.capture_resources()
        observed = result['capabilities']
        self.assertTrue(observed['available'])
        self.assertEqual(observed['fields'], {})
        self.assertEqual(observed['devices'], [
            {'device': 'rdma0', 'fields': {'fw_ver': '1', 'max_mr': '100'}},
            {'device': 'rdma1', 'fields': {'fw_ver': '2', 'max_mr': '200'}}])
        self.assertFalse(result['resources']['available'])

    def test_requested_adapter_must_match_reported_identity(self):
        caps = {'argv': [], 'available': True, 'truncated': False, 'error': None,
                'raw': 'hca_id: rdma1\nmax_mr: 10\n'}
        resources = {'argv': [], 'available': False, 'truncated': False, 'error': 'missing', 'raw': ''}
        with patch.object(rdma_diagnostics, '_bounded_command', side_effect=[caps, resources]):
            result = rdma_diagnostics.capture_resources('rdma0')
        self.assertFalse(result['capabilities']['available'])
        self.assertIn('identity mismatch', result['capabilities']['error'])

    def test_malformed_changed_and_missing_resources_are_not_zero(self):
        cap = {'argv': [], 'available': True, 'truncated': False, 'error': None,
               'raw': 'hca_id: rdma0\nmax_mr: 42\nmax_mr: 43\n'}
        bad = {'argv': [], 'available': True, 'truncated': False, 'error': None,
               'raw': '[{"ifindex":1,"ifname":"rdma0","mr":"unknown"}]'}
        with patch.object(rdma_diagnostics, '_bounded_command', side_effect=[cap, bad]):
            result = rdma_diagnostics.capture_resources('rdma0')
        self.assertFalse(result['capabilities']['available'])
        self.assertFalse(result['resources']['available'])
        self.assertEqual(result['resources']['devices'], [])
        missing = rdma_diagnostics._bounded_command(['/definitely-not-an-rdma-command'])
        self.assertFalse(missing['available'])
        self.assertTrue(missing['error'])

    def test_command_output_and_time_are_bounded(self):
        with patch.object(rdma_diagnostics, 'MAX_COMMAND_BYTES', 64):
            observed = rdma_diagnostics._bounded_command(
                [sys.executable, '-c', 'import sys; sys.stdout.write("x" * 10000)'])
        self.assertFalse(observed['available'])
        self.assertTrue(observed['truncated'])
        self.assertEqual(len(observed['raw']), 64)
        with patch.object(rdma_diagnostics, 'COMMAND_TIMEOUT', 0.1):
            observed = rdma_diagnostics._bounded_command(
                [sys.executable, '-c', 'import time; time.sleep(3)'])
        self.assertFalse(observed['available'])
        self.assertIn('timed out', observed['error'])

    def test_real_fixture_values_and_attribute_failures(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'sys'
            device = root / 'class/infiniband/rdma0'
            counters = device / 'ports/1/counters'
            counters.mkdir(parents=True)
            hw = device / 'ports/1/hw_counters'
            hw.mkdir()
            (device / 'node_guid').write_text('1234:5678:abcd:ef01\n')
            (counters / 'port_rcv_errors').write_text('0\n')
            (hw / 'vendor_example').write_text('42\n')
            (hw / 'invalid').write_text('not numeric\n')
            (hw / 'oversized').write_bytes(b'1' * 129)
            outside = Path(tmp) / 'outside'
            outside.write_text('99\n')
            (hw / 'escape').symlink_to(outside)
            observed = rdma_diagnostics.capture(root)
            self.assertTrue(observed['available'])
            self.assertFalse(observed['truncated'])
            dev = observed['devices'][0]
            self.assertEqual(dev['node_guid']['value'], '1234:5678:abcd:ef01')
            groups = dev['ports'][0]['groups']
            self.assertEqual(groups['counters']['fields']['port_rcv_errors']['value'], 0)
            self.assertEqual(groups['hw_counters']['fields']['vendor_example']['value'], 42)
            for name in ('invalid', 'oversized', 'escape'):
                self.assertFalse(groups['hw_counters']['fields'][name]['available'])
            self.assertLessEqual(observed['started']['monotonic_ns'], observed['finished']['monotonic_ns'])

    def test_limit_is_global_and_missing_is_not_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            counters = root / 'class/infiniband/rdma0/ports/1/counters'
            counters.mkdir(parents=True)
            for index in range(140):
                (counters / f'counter{index:03}').write_text(str(index) + '\n')
            observed = rdma_diagnostics.capture(root)
            groups = observed['devices'][0]['ports'][0]['groups']
            self.assertTrue(observed['truncated'])
            self.assertEqual(len(groups['counters']['fields']), 128)
            self.assertFalse(groups['hw_counters']['available'])
            self.assertNotIn('value', groups['hw_counters'])

    def test_no_device_is_explicitly_unavailable(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / 'class/infiniband').mkdir(parents=True)
            observed = rdma_diagnostics.capture(Path(tmp))
            self.assertFalse(observed['available'])
            self.assertEqual(observed['devices'], [])
            self.assertTrue(observed['errors'])

    def test_boundary_comparison_preserves_zero_reset_and_unknown_units(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dev = root / 'class/infiniband/rdma0'
            counters = dev / 'ports/1/counters'
            hw = dev / 'ports/1/hw_counters'
            counters.mkdir(parents=True)
            hw.mkdir()
            (dev / 'node_guid').write_text('1234:5678:abcd:ef01\n')
            (counters / 'port_xmit_wait').write_text('5\n')
            (counters / 'port_rcv_errors').write_text('0\n')
            (hw / 'vendor_retry').write_text('3\n')
            before = rdma_diagnostics.capture(root)
            (counters / 'port_xmit_wait').write_text('2\n')
            (hw / 'vendor_retry').write_text('7\n')
            after = rdma_diagnostics.capture(root)
            port = rdma_diagnostics.compare(before, after)['ports'][0]
            self.assertFalse(port['fields']['counters']['port_xmit_wait']['available'])
            self.assertEqual(port['fields']['counters']['port_rcv_errors']['delta'], 0)
            self.assertFalse(port['fields']['counters']['port_xmit_data']['available'])
            vendor = port['fields']['hw_counters']['vendor_retry']
            self.assertFalse(vendor['available'])
            self.assertEqual(vendor['begin_raw']['value'], 3)
            self.assertEqual(vendor['end_raw']['value'], 7)
            self.assertIsNone(vendor['unit'])
            (dev / 'node_guid').write_text('ffff:ffff:ffff:ffff\n')
            self.assertFalse(rdma_diagnostics.compare(before, rdma_diagnostics.capture(root))['ports'][0]['available'])


if __name__ == '__main__':
    unittest.main()
