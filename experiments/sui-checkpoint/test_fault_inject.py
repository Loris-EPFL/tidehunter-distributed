"""W1 controller-loss injector tests with a fake sysfs tree and JSON-RPC."""
import json
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


def intent(nqn=NQN, *, transport='rdma', address=ADDRESS, service=SERVICE):
    return {'target': {'nqn': nqn, 'transport': transport, 'address': address,
                       'service': service, 'nsid': 1, 'bytes': 4096, 'block_size': 4096,
                       'uuid': [1] * 16},
            'host_nqn': 'nqn.2026-01.test:host', 'host_id': [2] * 16}


def metrics(reads, *, connects=1, attempts=0, frames=0):
    """One aggregate live family with the write-resume counters the cell reads."""
    return ('tidehunter_nvmeof_live_completed_read_ops_total %d\n'
            'tidehunter_nvmeof_live_errors_total 0\n'
            'tidehunter_nvmeof_live_namespace_connects_total %d\n'
            'tidehunter_nvmeof_live_resume_attempts_total %d\n'
            'tidehunter_nvmeof_live_resume_rewrites_total %d\n'
            'tidehunter_nvmeof_live_resume_frames_total %d\n'
            'tidehunter_nvmeof_live_resume_refused_total 0\n'
            'tidehunter_nvmeof_live_read_io_samples_total 3\n') % (
                reads, connects, attempts, 1 if attempts else 0, frames)


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


class IntentSelection(unittest.TestCase):
    def write(self, root, name, value, mtime_ns):
        path = root / f'{name}.json'
        path.write_text(json.dumps(value))
        os.utime(path, ns=(mtime_ns, mtime_ns))
        return path

    def test_the_newest_intent_of_the_window_is_the_active_writer(self):
        old = 'nqn.2026-01.test:db-a-p00000001-sub-01'
        new = 'nqn.2026-01.test:db-a-p00000001-sub-02'
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.write(root, 'old', intent(old), 1_000)
            self.write(root, 'new', intent(new, service=SERVICE + 3), 2_000)
            # A newer intent of another owner window or transport is not a
            # candidate for this owner.
            self.write(root, 'outside', intent('nqn.2026-01.test:outside', service=SERVICE + 16), 3_000)
            self.write(root, 'tcp', intent('nqn.2026-01.test:tcp', transport='tcp'), 4_000)
            self.write(root, 'other-address', intent('nqn.2026-01.test:x', address='10.0.0.9'), 5_000)
            selected, candidates, directories = fault_inject.newest_write_intent(
                root, transport='rdma', address=ADDRESS, service=SERVICE,
                service_max=SERVICE + 15)
            self.assertEqual(selected, new)
            self.assertEqual(candidates, [old, new])
            self.assertEqual(directories, [str(root)],
                             'an exact registry is used as given')
            empty = root / 'empty'
            empty.mkdir()
            with self.assertRaisesRegex(ValueError, 'no live-nvme-connections registry'):
                fault_inject.newest_write_intent(
                    empty, transport='rdma', address=ADDRESS, service=SERVICE,
                    service_max=SERVICE + 15)

    def test_the_owner_window_is_searched_across_every_namespace_registry(self):
        """The staged S1-4 tree carries one registry per opened namespace.

        The failed cells prepared `remote/full_node_db/94970e38de08` and the
        engine created `live/checkpoints`, `live/store/epoch_0` and
        `live/store/perpetual` beneath it, each with its own
        `live-nvme-connections`; the write path's controller is the newest
        intent across all of them.
        """
        old = 'nqn.2026-01.test:db-a-p00000001-sub-01'
        middle = 'nqn.2026-01.test:db-a-p00000001-sub-02'
        new = 'nqn.2026-01.test:db-a-p00000001-sub-03'
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'remote/full_node_db/94970e38de08'
            namespaces = ('live/checkpoints', 'live/store/epoch_0', 'live/store/perpetual')
            for namespace in namespaces:
                (root / namespace / 'live-nvme-connections').mkdir(parents=True)
            self.write(root / 'live/checkpoints/live-nvme-connections',
                       'checkpoints', intent(old), 1_000)
            self.write(root / 'live/store/perpetual/live-nvme-connections',
                       'perpetual', intent(middle, service=SERVICE + 1), 1_500)
            self.write(root / 'live/store/epoch_0/live-nvme-connections',
                       'epoch', intent(new, service=SERVICE + 3), 2_000)
            selected, candidates, directories = fault_inject.newest_write_intent(
                root, transport='rdma', address=ADDRESS, service=SERVICE,
                service_max=SERVICE + 15)
            self.assertEqual(selected, new)
            self.assertEqual(candidates, [old, middle, new])
            self.assertEqual(directories, [
                str(root / namespace / 'live-nvme-connections')
                for namespace in namespaces])

    def test_an_absent_root_is_named_in_the_refusal(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'remote/full_node_db/94970e38de08'
            with self.assertRaisesRegex(ValueError, 'connection intent root is absent'):
                fault_inject.newest_write_intent(
                    root, transport='rdma', address=ADDRESS, service=SERVICE,
                    service_max=SERVICE + 15)

    def test_a_corrupt_intent_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'broken.json').write_text('{not json')
            with self.assertRaisesRegex(ValueError, 'unreadable connection intent'):
                fault_inject.newest_write_intent(
                    root, transport='rdma', address=ADDRESS, service=SERVICE,
                    service_max=SERVICE + 15)


class WritePathPreflight(unittest.TestCase):
    def sysfs(self, root, *, subsysnqn=NQN, multipath=False):
        nvme = root / 'sys-class-nvme'
        block = root / 'sys-class-block'
        controller = nvme / 'nvme1'
        controller.mkdir(parents=True)
        (controller / 'subsysnqn').write_text(subsysnqn + '\n')
        (controller / 'state').write_text('live\n')
        namespace = block / 'nvme1n1'
        namespace.mkdir(parents=True)
        (namespace / 'stat').write_text('0 0 0 0 0 0 8 0\n')
        (namespace / 'device').symlink_to(controller)
        if multipath:
            subsystem = nvme / 'nvme-subsys0'
            subsystem.mkdir()
            (subsystem / 'subsysnqn').write_text(subsysnqn + '\n')
            (namespace / 'device').unlink()
            (namespace / 'device').symlink_to(subsystem)
        return nvme, block

    def test_a_namespace_of_the_controller_is_selected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            nvme, block = self.sysfs(root)
            unrelated = block / 'nvme9n1'
            unrelated.mkdir()
            (unrelated / 'stat').write_text('0 0 0 0 0 0 8 0\n')
            other = nvme / 'nvme9'
            other.mkdir()
            (unrelated / 'device').symlink_to(other)
            with patch.object(fault_inject.manage, 'SYS_NVME', nvme), \
                    patch.object(fault_inject.manage, 'SYS_BLOCK', block):
                self.assertEqual(fault_inject.controller_namespaces('nvme1'), ['nvme1n1'])
                self.assertEqual(fault_inject.sectors_written('nvme1n1'), 8)

    def test_a_multipath_member_is_selected_through_its_subsystem(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            nvme, block = self.sysfs(root, multipath=True)
            with patch.object(fault_inject.manage, 'SYS_NVME', nvme), \
                    patch.object(fault_inject.manage, 'SYS_BLOCK', block):
                self.assertEqual(fault_inject.controller_namespaces('nvme1'), ['nvme1n1'])
            (nvme / 'nvme-subsys0' / 'subsysnqn').write_text('nqn.2026-01.test:other\n')
            with patch.object(fault_inject.manage, 'SYS_NVME', nvme), \
                    patch.object(fault_inject.manage, 'SYS_BLOCK', block):
                self.assertEqual(fault_inject.controller_namespaces('nvme1'), [])

    def test_only_a_descriptor_on_the_namespace_counts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proc, dev = root / 'proc', root / 'dev'
            dev.mkdir()
            (dev / 'nvme1n1').write_text('')
            (dev / 'nvme9n1').write_text('')
            for pid, target in ((10, dev / 'nvme1n1'), (11, dev / 'nvme9n1')):
                fd = proc / str(pid) / 'fd'
                fd.mkdir(parents=True)
                (fd / '3').symlink_to(target)
                (fd / '4').symlink_to(dev)
            holders = fault_inject.namespace_holders(['nvme1n1'], proc_root=proc, dev_root=dev)
            self.assertEqual(holders, [dict(pid=10, descriptors=1)])

    def test_sector_advance_waits_on_the_counter_and_is_bounded(self):
        idle = lambda device, **kwargs: 10
        clock = FakeClock()
        with patch.object(fault_inject, 'sectors_written', idle):
            advanced, rows = fault_inject.wait_sectors_advance(
                ['nvme1n1'], timeout=1, clock=clock.monotonic, sleep=clock.sleep)
        self.assertFalse(advanced)
        self.assertEqual(rows[-1]['sectors']['nvme1n1']['now'], 10)
        self.assertGreater(rows[-1]['seconds'], 0)
        values = iter([10, 10, 12])
        moving = lambda device, **kwargs: next(values)
        clock = FakeClock()
        with patch.object(fault_inject, 'sectors_written', moving):
            advanced, rows = fault_inject.wait_sectors_advance(
                ['nvme1n1'], timeout=5, clock=clock.monotonic, sleep=clock.sleep)
        self.assertTrue(advanced)
        self.assertEqual(rows[-1]['sectors']['nvme1n1'], dict(before=10, now=12))


class InjectionRun(unittest.TestCase):
    def fixture(self, root):
        controller = root / 'nvme1'
        controller.mkdir()
        (controller / 'hostnqn').write_text('nqn.2026-01.test:host')
        (controller / 'state').write_text('live\n')
        return controller

    def preflight(self, *, holders=None, advanced=None):
        return dict(
            namespaces=lambda controller: ['nvme1n1'],
            holders=lambda devices: ([dict(pid=42, descriptors=1)]
                                     if holders is None else holders),
            sectors=lambda devices, *, timeout: (
                True if advanced is None else advanced,
                [dict(seconds=0.0, sectors={'nvme1n1': dict(before=8, now=16)})]))

    def args(self, **changes):
        base = dict(nqn=NQN, intents=None, transport='rdma', address=ADDRESS, service=SERVICE,
                    service_max=None, sui_rpc='127.0.0.1:9000',
                    metrics_url='http://127.0.0.1:9000/metrics',
                    owner_pid=[os.getpid()], timeout=10.0, preflight_timeout=5.0)
        base.update(changes)
        return SimpleNamespace(**base)

    def test_controller_loss_records_preflight_reconnect_and_resume(self):
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
            def scraped(url):
                if state['calls'] <= 1:
                    return metrics(10, connects=1)
                return metrics(14, connects=2, attempts=1, frames=1)
            with patch.object(fault_inject.manage, 'SYS_NVME', root):
                record = fault_inject.run(self.args(), snapshot=snapshot, delete=delete,
                    latest=latest, metrics=scraped,
                    clock=clock.monotonic, sleep=clock.sleep, read_checkpoint=read,
                    **self.preflight())
            self.assertTrue(record['ok'], record)
            self.assertEqual(record['target']['controller'], 'nvme1')
            self.assertEqual(record['target']['nqn'], NQN)
            self.assertEqual(record['preflight']['state'], 'live')
            self.assertEqual(record['preflight']['devices'], ['nvme1n1'])
            self.assertEqual(record['preflight']['holders'], [dict(pid=42, descriptors=1)])
            self.assertTrue(record['preflight']['sectors_advanced'])
            self.assertEqual(record['new_controller'], dict(controller='nvme2', inode=2))
            self.assertEqual(record['resume'], dict(resume_attempts=1, resume_rewrites=1,
                                                   resume_frames=1, resume_refused=0))
            self.assertEqual(record['checkpoint_before'], 10)
            self.assertEqual(record['checkpoint_after'], 11)
            self.assertTrue(record['readback_equal'])
            self.assertEqual(record['fail_stop_count'], 0)
            self.assertIsNone(record['duplicate_count'])
            self.assertEqual(record['live_delta']['completed_read_ops'], 4)
            self.assertGreaterEqual(record['reconnect_seconds'], 0)
            self.assertEqual(record['processes_after'][0]['pid'], os.getpid())

    def test_a_resume_that_follows_the_reconnect_is_polled_for(self):
        """The episode is on the write path, so it can land after the first sample."""
        clock = FakeClock()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            state = dict(deleted=False, calls=0, polls=0)
            def snapshot():
                return inventory('nvme2', 2) if state['deleted'] else inventory()
            def latest(address):
                state['calls'] += 1
                return 10 if state['calls'] == 1 else 11
            def scraped(url):
                if not state['deleted']:
                    return metrics(10, connects=1)
                state['polls'] += 1
                return (metrics(14, connects=2) if state['polls'] < 3
                        else metrics(14, connects=2, attempts=1, frames=1))
            def read(address, sequence):
                return {'sequenceNumber': str(sequence)}
            with patch.object(fault_inject.manage, 'SYS_NVME', root):
                record = fault_inject.run(self.args(), snapshot=snapshot,
                    delete=lambda *a: state.update(deleted=True), latest=latest,
                    metrics=scraped, clock=clock.monotonic, sleep=clock.sleep,
                    read_checkpoint=read, **self.preflight())
        self.assertTrue(record['ok'], record)
        self.assertEqual(record['resume_poll']['samples'], 3)
        self.assertFalse(record['resume_poll']['exhausted'])
        self.assertGreater(record['resume_poll']['elapsed_seconds'], 0)
        self.assertEqual([row['resume_attempts']
                          for row in record['resume_poll']['observations']], [0, 0, 1])
        self.assertEqual(record['resume'], dict(resume_attempts=1, resume_rewrites=1,
                                                resume_frames=1, resume_refused=0))

    def test_a_loss_without_a_resume_episode_is_refused(self):
        clock = FakeClock()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            state = dict(deleted=False, calls=0)
            def snapshot():
                return inventory('nvme2', 2) if state['deleted'] else inventory()
            def latest(address):
                state['calls'] += 1
                return 10 if state['calls'] == 1 else 11
            def metrics_only_reconnect(url):
                return (metrics(10, connects=1) if not state['deleted']
                        else metrics(14, connects=2))
            with patch.object(fault_inject.manage, 'SYS_NVME', root):
                record = fault_inject.run(self.args(), snapshot=snapshot,
                    delete=lambda *a: state.update(deleted=True), latest=latest,
                    metrics=metrics_only_reconnect, clock=clock.monotonic,
                    sleep=clock.sleep,
                    read_checkpoint=lambda address, sequence: {}, **self.preflight())
            self.assertFalse(record['ok'])
            self.assertIn('no write-resume episode', record['reason'])
            self.assertIn('no frame was rewritten', record['reason'])
            self.assertTrue(record['resume_poll']['exhausted'])
            self.assertGreater(record['resume_poll']['samples'], 1)
            self.assertGreaterEqual(record['resume_poll']['elapsed_seconds'],
                                    self.args().timeout - fault_inject.RESUME_POLL_CADENCE)

    def test_a_stale_epoch_is_refused_before_any_delete(self):
        clock = FakeClock()
        deleted = []
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            with patch.object(fault_inject.manage, 'SYS_NVME', root):
                record = fault_inject.run(self.args(), snapshot=inventory,
                    delete=lambda *a: deleted.append(a), latest=lambda address: 10,
                    metrics=lambda url: metrics(10), clock=clock.monotonic,
                    sleep=clock.sleep, read_checkpoint=lambda address, sequence: {},
                    **self.preflight(holders=[], advanced=False))
            self.assertFalse(record['ok'])
            self.assertIn('no process holds a descriptor', record['reason'])
            self.assertFalse(deleted, 'a stale epoch must not be deleted')

    def test_an_idle_namespace_is_refused_before_any_delete(self):
        clock = FakeClock()
        deleted = []
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            with patch.object(fault_inject.manage, 'SYS_NVME', root):
                record = fault_inject.run(self.args(), snapshot=inventory,
                    delete=lambda *a: deleted.append(a), latest=lambda address: 10,
                    metrics=lambda url: metrics(10), clock=clock.monotonic,
                    sleep=clock.sleep, read_checkpoint=lambda address, sequence: {},
                    **self.preflight(advanced=False))
            self.assertFalse(record['ok'])
            self.assertIn('write sectors did not advance', record['reason'])
            self.assertFalse(deleted, 'an idle namespace must not be deleted')

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
                    metrics=lambda url: metrics(10), clock=clock.monotonic,
                    sleep=clock.sleep, read_checkpoint=lambda address, sequence: {},
                    **self.preflight())
            self.assertFalse(record['ok'])
            self.assertIn('did not reconnect', record['reason'])
            self.assertIn('processes_after', record)

    def test_intent_selection_records_candidates_and_selected_nqn(self):
        clock = FakeClock()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            intents = root / 'live-nvme-connections'
            intents.mkdir()
            (intents / 'a.json').write_text(json.dumps(intent('nqn.2026-01.test:old',
                                                              service=SERVICE + 1)))
            os.utime(intents / 'a.json', ns=(1_000_000_000, 1_000_000_000))
            (intents / 'b.json').write_text(json.dumps(intent()))
            os.utime(intents / 'b.json', ns=(2_000_000_000, 2_000_000_000))
            state = dict(deleted=False, calls=0)
            def snapshot():
                return inventory('nvme2', 2) if state['deleted'] else inventory()
            def latest(address):
                state['calls'] += 1
                return 10 if state['calls'] == 1 else 11
            def scraped(url):
                if state['calls'] <= 1:
                    return metrics(10, connects=1)
                return metrics(14, connects=2, attempts=1, frames=1)
            with patch.object(fault_inject.manage, 'SYS_NVME', root):
                record = fault_inject.run(
                    self.args(nqn=None, intents=intents, service_max=SERVICE + 15),
                    snapshot=snapshot, delete=lambda *a: state.update(deleted=True),
                    latest=latest, metrics=scraped, clock=clock.monotonic,
                    sleep=clock.sleep, read_checkpoint=lambda address, sequence: {},
                    **self.preflight())
            self.assertTrue(record['ok'], record)
            self.assertEqual(record['target']['nqn'], NQN)
            self.assertEqual(record['target']['intents'], str(intents))
            self.assertEqual(record['target']['intent_directories'], [str(intents)])
            self.assertEqual(record['target']['candidates'],
                             ['nqn.2026-01.test:old', NQN])

    def test_refused_delete_keeps_the_record_without_injection(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            def delete(*args):
                raise ValueError('recorded controller identity changed')
            with patch.object(fault_inject.manage, 'SYS_NVME', root):
                record = fault_inject.run(self.args(), snapshot=inventory, delete=delete,
                    latest=lambda address: 10, metrics=lambda url: metrics(10),
                    clock=FakeClock().monotonic, sleep=lambda seconds: None,
                    read_checkpoint=lambda address, sequence: {}, **self.preflight())
            self.assertFalse(record['ok'])
            self.assertIn('delete was refused', record['reason'])
            self.assertEqual(record['target']['controller'], 'nvme1')

    def test_dead_owner_before_injection_is_refused(self):
        record = fault_inject.run(self.args(owner_pid=[999999999]), snapshot=inventory,
            delete=lambda *a: None, latest=lambda address: 10, metrics=lambda url: metrics(10),
            clock=FakeClock().monotonic, sleep=lambda seconds: None,
            read_checkpoint=lambda address, sequence: {}, **self.preflight())
        self.assertFalse(record['ok'])
        self.assertIn('not alive', record['reason'])
        self.assertNotIn('controller', record['target'])

    def test_a_record_persisted_before_the_rpc_names_the_target_and_no_delete(self):
        """A fallback record must say what the injector had done so far.

        Fleet A's `S1-4-mid-frame-native` left only
        `{schema, kind, ok:false, reason, created_unix_ns}` because the record
        was written once, at exit. The milestones now persist the selected
        controller and whether the delete was attempted, so an RPC error before
        the delete still answers "nothing was injected".
        """
        persisted = []
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            def latest(address):
                raise ConnectionResetError(104, 'Connection reset by peer')
            with patch.object(fault_inject.manage, 'SYS_NVME', root):
                with self.assertRaises(OSError):
                    fault_inject.run(self.args(), snapshot=inventory,
                        delete=lambda *a: None, latest=latest,
                        metrics=lambda url: metrics(10), clock=FakeClock().monotonic,
                        sleep=lambda seconds: None,
                        read_checkpoint=lambda address, sequence: {},
                        write=persisted.append, **self.preflight())
        self.assertGreaterEqual(len(persisted), 1,
                                'the target milestone precedes the RPC')
        record = persisted[-1]
        self.assertEqual(record['target']['controller'], 'nvme1')
        self.assertEqual(record['target']['nqn'], NQN)
        self.assertEqual(record['preflight']['holders'], [dict(pid=42, descriptors=1)])
        self.assertIs(record['deleted'], False)
        self.assertNotIn('delete_attempted_unix_ns', record)

    def test_a_record_persisted_after_the_delete_says_the_delete_happened(self):
        persisted = []
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            state = dict(calls=0, deleted=False)
            def snapshot():
                return inventory('nvme2', 2) if state['deleted'] else inventory()
            def latest(address):
                state['calls'] += 1
                if state['calls'] == 1:
                    return 10
                raise ConnectionResetError(104, 'Connection reset by peer')
            with patch.object(fault_inject.manage, 'SYS_NVME', root):
                with self.assertRaises(OSError):
                    fault_inject.run(self.args(), snapshot=snapshot,
                        delete=lambda *a: state.update(deleted=True), latest=latest,
                        metrics=lambda url: metrics(10), clock=FakeClock().monotonic,
                        sleep=lambda seconds: None,
                        read_checkpoint=lambda address, sequence: {},
                        write=persisted.append, **self.preflight())
        deleted = [record for record in persisted if record['deleted']]
        self.assertGreaterEqual(len(deleted), 1,
                                'the delete milestone survives the later RPC error')
        record = deleted[0]
        self.assertEqual(record['target']['nqn'], NQN)
        self.assertIn('deleted_unix_ns', record)
        self.assertEqual(record['checkpoint_before'], 10)
        self.assertNotIn('reconnected', record)
        self.assertTrue(persisted[-1].get('reconnected'),
                        'the reconnect milestone is the last one before the error')

    def test_the_declined_delete_persists_deleted_false(self):
        persisted = []
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            with patch.object(fault_inject.manage, 'SYS_NVME', root):
                record = fault_inject.run(self.args(), snapshot=inventory,
                    delete=lambda *a: (_ for _ in ()).throw(ValueError('refused')),
                    latest=lambda address: 10, metrics=lambda url: metrics(10),
                    clock=FakeClock().monotonic, sleep=lambda seconds: None,
                    read_checkpoint=lambda address, sequence: {},
                    write=persisted.append, **self.preflight())
        self.assertFalse(record['ok'])
        self.assertIs(record['deleted'], False)
        self.assertTrue(persisted, 'the milestones precede the refused delete')
        self.assertEqual({row['deleted'] for row in persisted}, {False})
        self.assertTrue(any('delete_attempted_unix_ns' in row for row in persisted),
                        'the attempted delete is recorded before the call')


class Specification(unittest.TestCase):
    def base(self, **changes):
        values = dict(nqn=NQN, intents=None, transport='rdma', address=ADDRESS,
                      service=SERVICE, service_max=None, sui_rpc='127.0.0.1:9000',
                      metrics_url=None, owner_pid=[], timeout=10.0, preflight_timeout=5.0)
        values.update(changes)
        return SimpleNamespace(**values)

    def test_the_window_alone_cannot_name_a_controller(self):
        fault_inject.validate_spec(self.base(intents='/run/intents',
                                             nqn=None, service_max=SERVICE + 15))
        fault_inject.validate_spec(self.base())
        with self.assertRaisesRegex(ValueError, 'database root'):
            fault_inject.validate_spec(self.base(nqn=None, service_max=SERVICE + 15))
        with self.assertRaisesRegex(ValueError, 'bounded service window'):
            fault_inject.validate_spec(self.base(nqn=None, intents='/run/intents',
                                                 service_max=None))
        for service_max in (None, SERVICE - 1):
            with self.subTest(service_max=service_max), self.assertRaises(ValueError):
                fault_inject.validate_spec(self.base(nqn=None, intents='/run/intents',
                                                     service_max=service_max))
        with self.assertRaisesRegex(ValueError, 'preflight timeout'):
            fault_inject.validate_spec(self.base(preflight_timeout=0))


if __name__ == '__main__':
    unittest.main()
