"""W1 controller-loss injector tests with a fake sysfs tree and JSON-RPC."""
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import fault_inject

NQN = 'nqn.2026-01.test:owner'
ADDRESS = '10.0.0.2'
SERVICE = 4420


def inventory(controller='nvme1', inode=1):
    return {controller: dict(nqn=NQN, transport='rdma',
                             address=f'traddr={ADDRESS},trsvcid={SERVICE}', inode=inode)}


class FakeClock:
    def __init__(self):
        self.now = 100.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class ControllerSelection(unittest.TestCase):
    def test_exactly_one_matching_controller_is_required(self):
        self.assertEqual(fault_inject.select_controller(
            inventory(), nqn=NQN, transport='rdma', address=ADDRESS, service=SERVICE)[0], 'nvme1')
        for candidate in ({}, {'nvme1': inventory()['nvme1'], 'nvme2': inventory('nvme2', 2)['nvme2']}):
            with self.subTest(candidate=candidate), self.assertRaisesRegex(ValueError, 'expected one'):
                fault_inject.select_controller(candidate, nqn=NQN, transport='rdma',
                                               address=ADDRESS, service=SERVICE)
        wrong = inventory()
        wrong['nvme1']['address'] = 'traddr=10.0.0.9,trsvcid=4420'
        with self.assertRaisesRegex(ValueError, 'expected one'):
            fault_inject.select_controller(wrong, nqn=NQN, transport='rdma',
                                           address=ADDRESS, service=SERVICE)

    def test_reconnect_accepts_a_reused_inode_after_observed_absence(self):
        clock = FakeClock()
        views = iter([{}, inventory()])
        def snapshot():
            return next(views)
        seconds, observations = fault_inject.wait_for_reconnect(
            nqn=NQN, transport='rdma', address=ADDRESS, service=SERVICE,
            removed_inode=1, timeout=5, snapshot=snapshot,
            clock=clock.monotonic, sleep=clock.sleep)
        self.assertGreaterEqual(seconds, 0)
        self.assertEqual(observations[0]['matching'], 0)
        self.assertTrue(observations[-1]['present'])

    def test_canonical_JSON_ignores_mapping_order(self):
        self.assertEqual(fault_inject.canonical({'a': 1, 'b': 2}),
                         fault_inject.canonical({'b': 2, 'a': 1}))
        self.assertNotEqual(fault_inject.canonical({'a': 1}), fault_inject.canonical({'a': 2}))


class InjectionRun(unittest.TestCase):
    def fixture(self, root):
        controller = root / 'nvme1'
        controller.mkdir()
        (controller / 'hostnqn').write_text('nqn.2026-01.test:host')
        return controller

    def args(self, **changes):
        base = dict(nqn=NQN, transport='rdma', address=ADDRESS, service=SERVICE,
                    sui_rpc='127.0.0.1:9000', metrics_url='http://127.0.0.1:9000/metrics',
                    owner_pid=[os.getpid()], timeout=10.0)
        base.update(changes)
        return SimpleNamespace(**base)

    def test_controller_loss_records_reconnect_progress_and_readback(self):
        clock = FakeClock()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            state = dict(deleted=False, calls=0)
            def snapshot():
                return inventory('nvme2', 2) if state['deleted'] else inventory()
            def delete(controller, identity, attrs):
                self.assertEqual((controller, identity['inode'], attrs['hostnqn']),
                                 ('nvme1', 1, 'nqn.2026-01.test:host'))
                state['deleted'] = True
            def latest(address):
                state['calls'] += 1
                return 10 if state['calls'] == 1 else 11
            def read(address, sequence):
                return {'sequenceNumber': str(sequence), 'digest': 'stable', 'epoch': '0'}
            def metrics(url):
                value = 10 if state['calls'] <= 1 else 14
                return ('tidehunter_nvmeof_live_completed_read_ops_total %d\n'
                        'tidehunter_nvmeof_live_errors_total 0\n'
                        'tidehunter_nvmeof_live_namespace_connects_total 1\n'
                        'tidehunter_nvmeof_live_read_io_samples_total 3\n') % value
            with patch.object(fault_inject.manage, 'SYS_NVME', root):
                record = fault_inject.run(self.args(), snapshot=snapshot, delete=delete,
                    latest=latest, metrics=metrics,
                    clock=clock.monotonic, sleep=clock.sleep, read_checkpoint=read)
            self.assertTrue(record['ok'], record)
            self.assertEqual(record['target']['controller'], 'nvme1')
            self.assertEqual(record['checkpoint_before'], 10)
            self.assertEqual(record['checkpoint_after'], 11)
            self.assertTrue(record['readback_equal'])
            self.assertEqual(record['fail_stop_count'], 0)
            self.assertIsNone(record['duplicate_count'])
            self.assertEqual(record['live_delta']['completed_read_ops'], 4)
            self.assertGreaterEqual(record['reconnect_seconds'], 0)
            self.assertEqual(record['processes_after'][0]['pid'], os.getpid())

    def test_missing_reconnect_is_retained_as_failure(self):
        clock = FakeClock()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            def delete(*args):
                return None
            with patch.object(fault_inject.manage, 'SYS_NVME', root):
                record = fault_inject.run(self.args(), snapshot=inventory,
                    delete=delete, latest=lambda address: 10,
                    metrics=lambda url: '', clock=clock.monotonic, sleep=clock.sleep,
                    read_checkpoint=lambda address, sequence: {})
            self.assertFalse(record['ok'])
            self.assertIn('did not reconnect', record['reason'])
            self.assertIn('processes_after', record)

    def test_refused_delete_keeps_the_record_without_injection(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            def delete(*args):
                raise ValueError('recorded controller identity changed')
            with patch.object(fault_inject.manage, 'SYS_NVME', root):
                record = fault_inject.run(self.args(), snapshot=inventory, delete=delete,
                    latest=lambda address: 10, metrics=lambda url: '',
                    clock=FakeClock().monotonic, sleep=lambda seconds: None,
                    read_checkpoint=lambda address, sequence: {})
            self.assertFalse(record['ok'])
            self.assertIn('delete was refused', record['reason'])
            self.assertEqual(record['target']['controller'], 'nvme1')

    def test_dead_owner_before_injection_is_refused(self):
        record = fault_inject.run(self.args(owner_pid=[999999999]), snapshot=inventory,
            delete=lambda *a: None, latest=lambda address: 10, metrics=lambda url: '',
            clock=FakeClock().monotonic, sleep=lambda seconds: None,
            read_checkpoint=lambda address, sequence: {})
        self.assertFalse(record['ok'])
        self.assertIn('not alive', record['reason'])
        self.assertNotIn('controller', record['target'])


if __name__ == '__main__':
    unittest.main()
