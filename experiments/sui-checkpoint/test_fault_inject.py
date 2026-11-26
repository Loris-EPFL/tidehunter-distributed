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

    def test_service_window_selects_the_lowest_NQN_and_keeps_candidates(self):
        first = 'nqn.2026-01.test:db-a-p00000001-sub-01'
        second = 'nqn.2026-01.test:db-b-p00000001-sub-02'
        inventory_rows = {
            'nvme1': dict(nqn=second, transport='rdma',
                          address=f'traddr={ADDRESS},trsvcid={SERVICE}', inode=1),
            'nvme2': dict(nqn=first, transport='rdma',
                          address=f'traddr={ADDRESS},trsvcid={SERVICE + 3}', inode=2),
            'nvme3': dict(nqn='nqn.2026-01.test:other', transport='rdma',
                          address=f'traddr=10.0.0.9,trsvcid={SERVICE}', inode=3),
            'nvme4': dict(nqn='nqn.2026-01.test:outside', transport='rdma',
                          address=f'traddr={ADDRESS},trsvcid={SERVICE + 16}', inode=4),
            'nvme5': dict(nqn='nqn.2026-01.test:tcp', transport='tcp',
                          address=f'traddr={ADDRESS},trsvcid={SERVICE}', inode=5),
        }
        name, identity, candidates = fault_inject.select_scoped(
            inventory_rows, transport='rdma', address=ADDRESS,
            service=SERVICE, service_max=SERVICE + 15)
        self.assertEqual(name, 'nvme2')
        self.assertEqual(identity['nqn'], first)
        self.assertEqual(candidates, [first, second])
        with self.assertRaises(ValueError):
            fault_inject.select_scoped(inventory_rows, transport='rdma',
                                       address='10.0.0.7', service=SERVICE,
                                       service_max=SERVICE + 15)
        with self.assertRaises(ValueError):
            fault_inject.select_scoped(inventory_rows, transport='rdma',
                                       address=ADDRESS, service=SERVICE + 5,
                                       service_max=SERVICE + 1)

    def test_a_window_target_requires_no_NQN_but_needs_a_bounded_window(self):
        base = dict(transport='rdma', address=ADDRESS, service=SERVICE,
                    sui_rpc='127.0.0.1:9000', metrics_url=None,
                    owner_pid=[], timeout=10.0)
        fault_inject.validate_spec(SimpleNamespace(nqn=None, service_max=SERVICE + 15, **base))
        fault_inject.validate_spec(SimpleNamespace(nqn=NQN, service_max=None, **base))
        for service_max in (None, SERVICE - 1):
            with self.subTest(service_max=service_max), self.assertRaises(ValueError):
                fault_inject.validate_spec(
                    SimpleNamespace(nqn=None, service_max=service_max, **base))


class InjectionRun(unittest.TestCase):
    def fixture(self, root):
        controller = root / 'nvme1'
        controller.mkdir()
        (controller / 'hostnqn').write_text('nqn.2026-01.test:host')
        return controller

    def args(self, **changes):
        base = dict(nqn=NQN, transport='rdma', address=ADDRESS, service=SERVICE,
                    service_max=None, sui_rpc='127.0.0.1:9000',
                    metrics_url='http://127.0.0.1:9000/metrics',
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

    def test_service_window_injection_records_candidates_and_selected_nqn(self):
        clock = FakeClock()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            state = dict(deleted=False, calls=0)
            def snapshot():
                return inventory('nvme2', 2) if state['deleted'] else inventory()
            def delete(controller, identity, attrs):
                self.assertEqual((controller, identity['inode']),
                                 ('nvme1', 1))
                state['deleted'] = True
            def latest(address):
                state['calls'] += 1
                return 10 if state['calls'] == 1 else 11
            with patch.object(fault_inject.manage, 'SYS_NVME', root):
                record = fault_inject.run(
                    self.args(nqn=None, service_max=SERVICE + 15), snapshot=snapshot,
                    delete=delete, latest=latest,
                    metrics=lambda url: 'tidehunter_nvmeof_live_namespace_connects_total 2\n',
                    clock=clock.monotonic, sleep=clock.sleep,
                    read_checkpoint=lambda address, sequence: {})
            self.assertTrue(record['ok'], record)
            self.assertEqual(record['target']['nqn'], NQN)
            self.assertEqual(record['target']['service_max'], SERVICE + 15)
            self.assertEqual(record['target']['candidates'], [NQN])

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
