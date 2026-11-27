"""One owner close and one exact-owned configfs cleanup, with fakes.

The cleanup test runs the real POSIX command against a temporary configfs-shaped
tree, so the prefix selection and the removal order are exercised without root
or a kernel target. The stop test uses an in-memory `/proc` and asserts the
marker is touched before any signal and a forced kill is a failed stop.
"""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import owner_lifecycle


PREFIX = owner_lifecycle.NQN_PREFIX


class CleanupTree(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / 'nvmet'

    def owned(self, name):
        return self.root / 'subsystems' / (PREFIX + name)

    def port_link(self, name, destination, port=1000):
        links = self.root / 'ports' / str(port) / 'subsystems'
        links.mkdir(parents=True, exist_ok=True)
        (links / name).symlink_to(destination)

    def tree(self):
        sub = self.owned('db-a-p00000000-sub-01')
        (sub / 'namespaces' / '1').mkdir(parents=True)
        (sub / 'allowed_hosts').mkdir()
        host = self.root / 'hosts' / (PREFIX + 'db-a-p00000000-host')
        host.mkdir(parents=True)
        (sub / 'allowed_hosts' / host.name).symlink_to(host)
        self.port_link(sub.name, sub)
        foreign = self.root / 'subsystems' / 'nqn.foreign:db'
        foreign.mkdir(parents=True)
        self.port_link('nqn.foreign:db', foreign, port=1001)
        return sub, host, foreign

    def run_cleanup(self):
        return subprocess.run(['bash', '-c', owner_lifecycle.cleanup_script(self.root)],
                              capture_output=True, text=True, timeout=30)

    def test_removes_only_tidehunter_prefixed_objects(self):
        sub, host, foreign = self.tree()
        completed = self.run_cleanup()
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertIn('COMPLETE', completed.stdout)
        self.assertFalse(sub.exists())
        self.assertFalse(host.exists())
        self.assertTrue(foreign.is_dir())
        self.assertTrue((self.root / 'ports/1001/subsystems/nqn.foreign:db').is_symlink())
        self.assertFalse((self.root / 'ports/1000/subsystems' / sub.name).exists())

    def test_a_failed_removal_leaves_the_gate_closed(self):
        sub, host, _ = self.tree()
        # A file inside a namespace cannot be rmdir'd, so the subsystem remains.
        (sub / 'namespaces' / '1' / 'blocker').write_text('x')
        completed = self.run_cleanup()
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn('REMAINS\tsubsystem', completed.stdout)
        self.assertIn('INCOMPLETE', completed.stdout)
        self.assertTrue(sub.exists())

    def test_cleanup_host_records_what_it_removed_and_refuses_on_failure(self):
        sub, host, _ = self.tree()

        class Remote:
            def shell(self, host_name, command, *, stdin=None, check=True):
                completed = subprocess.run(['bash', '-c', command],
                                           capture_output=True, timeout=30)
                return completed.returncode, completed.stdout, completed.stderr

        record = owner_lifecycle.cleanup_host(Remote(), 'H1', root=self.root)
        self.assertTrue(record['complete'])
        self.assertEqual({row['kind'] for row in record['removed']},
                         {'link', 'subsystem', 'host', 'allowed-host', 'namespace'})
        self.assertTrue(any(row['path'] == str(sub) for row in record['before']))

        self.root = Path(self.tmp.name) / 'failed'
        failed_sub, _, _ = self.tree()
        (failed_sub / 'namespaces' / '1' / 'blocker').write_text('x')
        record = owner_lifecycle.cleanup_host(Remote(), 'H1', root=self.root)
        self.assertFalse(record['complete'])
        self.assertTrue(record['remains'])


class PreviousOwners(unittest.TestCase):
    """The shared check reports live owners and exits 0 on stopped ones."""

    def test_a_stopped_last_owner_is_not_a_failed_check(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            live = subprocess.Popen(['sleep', '60'])
            try:
                dead = subprocess.Popen(['true'])
                dead.wait()
                (root / 'owner-0-0.pid').write_text(f'{live.pid}\n')
                # The glob sorts this file last: its stopped owner was the
                # script's exit status before the test moved into an `if`.
                (root / 'owner-1-0.pid').write_text(f'{dead.pid}\n')
                result = subprocess.run(
                    ['sh', '-c', owner_lifecycle.PREVIOUS_OWNERS_SCRIPT, '--', str(root)],
                    capture_output=True, text=True, check=False)
            finally:
                live.kill()
                live.wait()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.split(), [str(live.pid), str(root / 'owner-0-0.pid')])


class StopOrder(unittest.TestCase):
    class Remote:
        def __init__(self, *, leave_on_marker=False, leave_on_term=False):
            self.alive = {42}
            self.calls = []
            self.leave_on_marker = leave_on_marker
            self.leave_on_term = leave_on_term

        def shell(self, host, command, *, stdin=None, check=True):
            self.calls.append(command)
            if command.startswith('touch /guest/stop') and not self.leave_on_marker:
                self.alive.clear()
            if command.startswith('kill -TERM') and not self.leave_on_term:
                self.alive.clear()
            if command.startswith('kill -KILL'):
                self.alive.clear()
            return 0, b'', b''

        def exists(self, host, path):
            return str(path) == '/proc/42' and 42 in self.alive

    def test_the_marker_is_first_and_records_the_close(self):
        remote = self.Remote()
        record = owner_lifecycle.stop_owner(
            remote, host='H1', pid=42, process_group=42,
            stop_path='/guest/stop', partition=0)
        self.assertEqual(record['signal'], 'stop-file')
        self.assertFalse(record['forced'])
        self.assertFalse(record['still_alive'])
        self.assertEqual(remote.calls[0], 'touch /guest/stop')
        self.assertFalse(any(command.startswith('kill -')
                             for command in remote.calls))

    def test_a_survivor_is_signalled_in_order_and_a_forced_kill_is_recorded(self):
        remote = self.Remote(leave_on_marker=True, leave_on_term=True)
        record = owner_lifecycle.stop_owner(
            remote, host='H1', pid=42, process_group=42,
            stop_path='/guest/stop', partition=0, grace=0.01, kill_grace=0.01)
        self.assertEqual(record['signal'], 'stop-file+SIGTERM')
        self.assertTrue(record['forced'])
        self.assertFalse(record['still_alive'])
        self.assertIn('touch /guest/stop', remote.calls)
        self.assertIn('kill -TERM -42', remote.calls)
        self.assertIn('kill -KILL -42', remote.calls)
        self.assertLess(remote.calls.index('touch /guest/stop'),
                        remote.calls.index('kill -TERM -42'))


class Stage3RefusesOnCleanupFailure(unittest.TestCase):
    def test_start_does_not_launch_when_the_gate_fails(self):
        import stage3_campaign

        class Remote:
            def __init__(self):
                self.calls = []

            def shell(self, host, command, *, stdin=None, check=True):
                self.calls.append(command)
                return 0, b'', b''

            def exists(self, host, path):
                return False

        remote = Remote()
        preparations = {'native': dict(index=0, manifest={'hosts': [
            dict(host='H1', partition=0, commands=[['owner']],
                 ready_files=['/guest/ready'], stop_files=['/guest/stop'])]})}
        owners = stage3_campaign.Owners(
            remote, preparations, root=Path('/campaign'), guest_root=Path('/guest'))
        failed = dict(host='H1', root='/sys/kernel/config/nvmet', prefix=PREFIX,
                      before=[], removed=[], remains=[], unavailable=False,
                      complete=False, exit_code=1, error='failed')
        with mock.patch.object(stage3_campaign.Owners, 'install',
                               return_value=preparations['native']), \
                mock.patch.object(stage3_campaign.owner_lifecycle, 'cleanup_hosts',
                                  return_value=[failed]):
            with self.assertRaisesRegex(ValueError, 'cleanup failed'):
                owners.start('native')
        self.assertFalse(any('setsid' in command for command in remote.calls))
        self.assertEqual(owners.cleanups, [failed])


if __name__ == '__main__':
    unittest.main()
