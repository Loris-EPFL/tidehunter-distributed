from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest import mock

import spdk_targets


class TargetLifetimeTests(unittest.TestCase):
    def test_absent_unit_does_not_certify_unacknowledged_start_or_remove_runtime(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime = Path(temporary) / "owned-runtime"
            runtime.mkdir()
            info = runtime.stat()
            state = {"unit_start_attempted": True, "unit_started": False,
                     "rpc_socket": str(runtime / "rpc.sock"),
                     "runtime_identity": [info.st_dev, info.st_ino]}
            with mock.patch.object(spdk_targets, "unit_status", return_value={"LoadState": "not-found"}):
                with self.assertRaises(RuntimeError):
                    spdk_targets.stop_owned(state)
            self.assertTrue(runtime.is_dir(), "ambiguous launch state must remain available for reconciliation")

    def test_unknown_runtime_object_is_preserved_with_owned_lock(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime = Path(temporary) / "runtime"
            runtime.mkdir()
            lock = runtime / "rpc.sock.lock"
            lock.write_text("owned")
            lock.chmod(0o600)
            unknown = runtime / "unexpected"
            unknown.write_text("must not remove")
            info, lock_info = runtime.stat(), lock.stat()
            state = {"rpc_socket": str(runtime / "rpc.sock"),
                     "runtime_identity": [info.st_dev, info.st_ino],
                     "rpc_lock_device": lock_info.st_dev, "rpc_lock_inode": lock_info.st_ino}
            with mock.patch.object(spdk_targets, "unit_status", return_value={"LoadState": "not-found"}):
                with self.assertRaisesRegex(ValueError, "unknown SPDK runtime objects"):
                    spdk_targets.stop_owned(state)
            self.assertTrue(lock.exists())
            self.assertTrue(unknown.exists())

    def test_replaced_lock_inode_is_never_removed(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime = Path(temporary) / "runtime"
            runtime.mkdir()
            lock = runtime / "rpc.sock.lock"
            lock.write_text("original")
            lock.chmod(0o600)
            with lock.open("rb"):
                original = lock.stat()
                lock.unlink()
                lock.write_text("replacement")
                lock.chmod(0o600)
                info = runtime.stat()
                state = {"rpc_socket": str(runtime / "rpc.sock"),
                         "runtime_identity": [info.st_dev, info.st_ino],
                         "rpc_lock_device": original.st_dev, "rpc_lock_inode": original.st_ino}
                lstat = Path.lstat
                def root_lstat(path):
                    actual = lstat(path)
                    return SimpleNamespace(st_mode=actual.st_mode, st_uid=0, st_nlink=actual.st_nlink,
                                           st_dev=actual.st_dev, st_ino=actual.st_ino)
                with mock.patch.object(spdk_targets, "unit_status", return_value={"LoadState": "not-found"}), \
                     mock.patch.object(Path, "lstat", autospec=True, side_effect=root_lstat):
                    with self.assertRaisesRegex(ValueError, "unrelated RPC lock"):
                        spdk_targets.stop_owned(state)
                self.assertEqual(lock.read_text(), "replacement")
