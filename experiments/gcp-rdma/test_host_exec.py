"""The one host-execution seam: one primitive, derived helpers, argv boundary."""
import shlex
import unittest

import host_exec


class FakeStore(host_exec.HostExecution):
    """A MailboxExec-shaped double: commands and bytes, no argv."""

    def __init__(self):
        self.files = {}
        self.commands = []

    def execute(self, host, command, *, stdin=None):
        self.commands.append((host, command, stdin))
        if command.startswith("sh -lc "):
            command = shlex.split(command)[2]
        if command.startswith('cat '):
            path = command.split(' ', 1)[1].strip("'")
            if path not in self.files:
                return 1, b'', b'missing'
            return 0, self.files[path], b''
        if command.startswith('test -e '):
            path = command.split(' ', 2)[2].strip("'")
            return (0 if path in self.files else 1), b'', b''
        if 'cat > ' in command:
            path = command.split('cat > ', 1)[1].strip("'")
            self.files[path] = stdin or b''
            return 0, b'', b''
        return 0, b'ok', b''


class Seam(unittest.TestCase):
    def test_one_primitive_serves_every_driver_helper(self):
        store = FakeStore()
        store.run('H1', 'true')
        self.assertEqual(store.commands[-1][:2], ('H1', 'true'))
        store.shell('H1', 'echo hello')
        self.assertEqual(store.commands[-1][1], "sh -lc 'echo hello'")
        store.write_bytes('H1', '/mnt/x/storage-node', b'payload')
        self.assertEqual(store.read_bytes('H1', '/mnt/x/storage-node'), b'payload')
        self.assertTrue(store.exists('H1', '/mnt/x/storage-node'))
        self.assertFalse(store.exists('H1', '/mnt/x/absent'))

    def test_a_failed_command_is_typed_and_carries_stderr(self):
        class Broken(host_exec.HostExecution):
            def execute(self, host, command, *, stdin=None):
                return 3, b'', b'no route to host'

        with self.assertRaisesRegex(host_exec.HostCommandError, 'no route to host'):
            Broken().run('H1', 'true')
        code, out, err = Broken().run('H1', 'true', check=False)
        self.assertEqual((code, out, err), (3, b'', b'no route to host'))

    def test_gcloud_is_only_the_default_implementation(self):
        calls = []

        def runner(argv, stdin=None):
            calls.append((argv, stdin))
            return 0, b'ok', b''

        ssh = host_exec.GcloudSsh('us-central1-a', runner=runner, iap=True)
        self.assertEqual(ssh.execute('H1', 'true'), (0, b'ok', b''))
        self.assertEqual(calls[0][0], [
            'gcloud', 'compute', 'ssh', 'H1', '--zone', 'us-central1-a',
            '--quiet', '--command=true', '--tunnel-through-iap'])


if __name__ == '__main__':
    unittest.main()
