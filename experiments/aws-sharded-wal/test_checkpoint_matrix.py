"""Generic EFA negotiation and hardware activity evidence tests."""
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import checkpoint_matrix as matrix


class HardwareEvidenceTests(unittest.TestCase):
    def test_exact_fabric_is_required_for_new_hardware_negotiation(self):
        for transport, fabric in (('efa', 'efa'), ('efa-direct', 'efa-direct'), ('efa-native', 'efa-direct')):
            text = f'provider: efa\n    fabric: {fabric}\n    type: FI_EP_RDM\n'
            matrix.validate_provider(text, transport)
            verbose = f'fi_info:\n    fi_ep_attr:\n        type: FI_EP_RDM\n    fi_fabric_attr:\n        name: {fabric}\n        prov_name: efa\n'
            matrix.validate_provider(verbose, transport)
            for invalid in (text.replace('fabric: ' + fabric, 'fabric: tcp'),
                            text.replace('    fabric: ' + fabric + '\n', ''),
                            text.replace('provider: efa', 'provider: efa-direct'),
                            text + 'provider: tcp\n    fabric: tcp\n    type: FI_EP_RDM\n',
                            text + 'provider: efa\n    type: FI_EP_RDM\n',
                            text.replace('FI_EP_RDM', 'FI_EP_MSG')):
                with self.subTest(transport=transport, invalid=invalid), self.assertRaises(ValueError):
                    matrix.validate_provider(invalid, transport)
        with self.assertRaises(ValueError):
            matrix.validate_provider('provider: efa\n fabric: efa\n type: FI_EP_RDM\n', 'efa-direct')
        with self.assertRaises(ValueError):
            matrix.validate_provider('provider: efa\n fabric: efa-direct\n type: FI_EP_RDM\n', 'efa')
        with self.assertRaises(ValueError):
            matrix.validate_provider('provider: efa\n fabric: efa-direct\n type: FI_EP_RDM\n')
        with self.assertRaises(ValueError):
            matrix.validate_provider('provider: efa\n fabric: efa\n type: FI_EP_RDM\n', 'efa-native')
        with self.assertRaises(ValueError):
            matrix.validate_provider('provider: efa\n fabric: efa-native\n type: FI_EP_RDM\n', 'efa-native')

    def test_provider_must_be_hardware_efa_rdm(self):
        matrix.validate_provider('provider: efa\n    type: FI_EP_RDM\n')
        verbose = 'fi_info:\n    fi_ep_attr:\n        type: FI_EP_RDM\n    fi_fabric_attr:\n        prov_name: efa\n'
        matrix.validate_provider(verbose)
        for text in ('provider: efa;ofi_rxd\n type: FI_EP_RDM\n',
                     'provider: tcp\n type: FI_EP_RDM\n', 'provider: efa\n type: FI_EP_MSG\n',
                     verbose.replace('prov_name: efa', 'prov_name: efa;ofi_rxd'),
                     verbose.replace('prov_name: efa', 'prov_name: tcp'),
                     verbose.replace('FI_EP_RDM', 'FI_EP_MSG'),
                     verbose + 'provider: tcp\n type: FI_EP_RDM\n'):
            with self.subTest(text=text), self.assertRaises(ValueError):
                matrix.validate_provider(text)

    def test_sysfs_snapshot_fails_closed_and_binds_both_directions(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            with self.assertRaises(ValueError):
                matrix.efa_snapshot({}, base)
            driver = base / 'drivers/efa'
            driver.mkdir(parents=True)
            device = base / 'rdmap0s1'
            (device / 'device').mkdir(parents=True)
            (device / 'device/driver').symlink_to(driver)
            counters = device / 'ports/1/hw_counters'
            counters.mkdir(parents=True)
            (counters / 'send_bytes').write_text('100')
            with self.assertRaises(OSError):
                matrix.efa_snapshot({}, base)
            (counters / 'recv_bytes').write_text('200')
            before = matrix.efa_snapshot({'cell_id': 'r01-efa'}, base)
            (counters / 'send_bytes').write_text('120')
            (counters / 'recv_bytes').write_text('240')
            after = matrix.efa_snapshot({'cell_id': 'r01-efa'}, base)
            proof = matrix.efa_proof(before, after, {'cell_id': 'r01-efa'})
            self.assertEqual((proof['send_bytes'], proof['recv_bytes']), (20, 40))
            (counters / 'recv_bytes').write_text('unreadable')
            with self.assertRaises(ValueError):
                matrix.efa_snapshot({}, base)

    def test_preflight_binds_requested_fabric_and_retains_provider_output(self):
        for transport, fabric in (('efa', 'efa'), ('efa-direct', 'efa-direct'), ('efa-native', 'efa-direct')):
            provider = f'provider: efa\n fabric: {fabric}\n type: FI_EP_RDM\n'.encode()
            with self.subTest(transport=transport), tempfile.TemporaryDirectory() as temporary, \
                    patch('resource.setrlimit') as memlock, patch('shutil.which', return_value='/bin/fi_info'), \
                    patch('subprocess.run', return_value=Mock(returncode=0, stdout=provider, stderr=b'')) as run, \
                    patch.dict(os.environ):
                output = Path(temporary)
                matrix.fabric_preflight(output, transport)
                memlock.assert_called_once()
                self.assertEqual(run.call_args.args[0], ['/bin/fi_info', '-p', 'efa', '-f', fabric, '-t', 'FI_EP_RDM'])
                self.assertEqual((output / 'efa-provider.txt').read_bytes(), provider)
                self.assertEqual(os.environ['FI_EFA_USE_DEVICE_RDMA'], '1')
                self.assertEqual(os.environ['FI_EFA_ENABLE_SHM_TRANSFER'], '0')

    def test_preflight_rejects_failure_and_provider_fallback(self):
        for status, provider, error in ((1, b'no provider', RuntimeError),
                (0, b'provider: tcp\n fabric: tcp\n type: FI_EP_RDM\n', ValueError)):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as temporary, \
                    patch('resource.setrlimit'), patch('shutil.which', return_value='/bin/fi_info'), \
                    patch('subprocess.run', return_value=Mock(returncode=status, stdout=provider, stderr=b'')), \
                    patch.dict(os.environ), self.assertRaises(error):
                matrix.fabric_preflight(Path(temporary))
        with patch('subprocess.run') as run, self.assertRaises(ValueError):
            matrix.fabric_preflight(Path('/unused'), 'tcp')
        run.assert_not_called()


if __name__ == '__main__':
    unittest.main()
