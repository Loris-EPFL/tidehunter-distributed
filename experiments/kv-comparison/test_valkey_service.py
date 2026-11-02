import unittest
import subprocess
from unittest.mock import patch
import json
from pathlib import Path
import tempfile

import valkey_service


class ValkeyServiceTests(unittest.TestCase):
    def test_failed_owned_unit_is_admitted_before_reset(self):
        service = {"unit": "kvcloud-valkey-owned-0"}
        failed = {"LoadState": "loaded", "ActiveState": "failed", "MainPID": "0"}
        with patch.object(valkey_service, "status", side_effect=[failed, {"LoadState": "not-found"}]), \
                patch.object(valkey_service, "admit_unit") as admit, \
                patch.object(valkey_service, "command") as command:
            self.assertTrue(valkey_service.stop({"native_valkey": service})[0]["already_stopped"])
            admit.assert_called_once_with(service)
            command.assert_called_once_with(["systemctl", "reset-failed", service["unit"]], timeout=15)

    def test_failed_foreign_unit_is_never_reset(self):
        service = {"unit": "kvcloud-valkey-owned-0"}
        with patch.object(valkey_service, "status", return_value={"LoadState": "loaded", "ActiveState": "failed", "MainPID": "0"}), \
                patch.object(valkey_service, "admit_unit", side_effect=RuntimeError("foreign")), \
                patch.object(valkey_service, "command") as command:
            with self.assertRaisesRegex(RuntimeError, "foreign"):
                valkey_service.stop({"native_valkey": service})
            command.assert_not_called()

    def test_missing_unit_is_absent_even_when_systemctl_exits_nonzero(self):
        absent = subprocess.CompletedProcess([], 1, "LoadState=not-found\n", "")
        with patch.object(valkey_service.subprocess, "run", return_value=absent):
            self.assertEqual(valkey_service.status("kvcloud-valkey-owned-0"),
                             {"LoadState": "not-found"})

    def test_status_rejects_other_systemd_failures(self):
        failed = subprocess.CompletedProcess([], 1, "", "access denied")
        with patch.object(valkey_service.subprocess, "run", return_value=failed):
            with self.assertRaisesRegex(RuntimeError, "access denied"):
                valkey_service.status("kvcloud-valkey-owned-0")

    def test_foreign_or_unproven_service_is_never_stopped(self):
        config = {"native_valkey": {"unit": "kvcloud-valkey-owned-0"}}
        calls = []
        with patch.object(valkey_service, "status", return_value={"LoadState": "loaded"}), \
                patch.object(valkey_service, "evidence", side_effect=RuntimeError("identity mismatch")), \
                patch.object(valkey_service, "command", side_effect=lambda args, **kw: calls.append(args)):
            with self.assertRaisesRegex(RuntimeError, "identity mismatch"):
                valkey_service.stop(config)
        self.assertEqual(calls, [])

    def test_absent_service_is_not_stopped(self):
        config = {"native_valkey": {"unit": "kvcloud-valkey-owned-0"}}
        with patch.object(valkey_service, "status", return_value={"LoadState": "not-found"}), \
                patch.object(valkey_service, "command") as command:
            self.assertEqual(valkey_service.stop(config), [])
            command.assert_not_called()

    def test_finite_soft_memlock_is_rejected_even_with_unlimited_hard_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = root / "runtime"
            (runtime / "bin").mkdir(parents=True)
            artifacts = {}
            for name in ("valkey-server", "valkey-cli"):
                binary = runtime / "bin" / name
                binary.write_bytes(name.encode())
                artifacts["src/" + name] = {"sha256": valkey_service.digest(binary)}
            source = "fe8d406c0b5a64ec897f6455e4de684629883764"
            (runtime / "valkey-rdma.json").write_text(json.dumps({"source_commit": source, "artifacts": artifacts}))
            service = {"unit": "kvcloud-valkey-owned-0", "runtime": str(runtime),
                       "source_commit": source, "server_sha256": artifacts["src/valkey-server"]["sha256"],
                       "args": [str(runtime / "bin/valkey-server")], "cpus": 2, "memory_mib": 64}
            proc = root / "proc" / "123"
            proc.mkdir(parents=True)
            (proc / "cmdline").write_bytes(b"valkey-server\0")
            (proc / "exe").symlink_to(runtime / "bin/valkey-server")
            (proc / "limits").write_text("Max locked memory         65536                unlimited            bytes\n")
            group = "/system.slice/" + service["unit"] + ".service"
            cgroup = root / "cgroup" / group.lstrip("/")
            cgroup.mkdir(parents=True)
            for name, value in (("memory.max", str(64 * 1024**2)), ("memory.swap.max", "0"), ("cpu.max", "200000 100000")):
                (cgroup / name).write_text(value)
            state = {"ActiveState": "active", "MainPID": "123", "ControlGroup": group}
            def paths(value):
                return {"/proc": root / "proc", "/sys/fs/cgroup": root / "cgroup"}.get(str(value), Path(value))
            with patch.object(valkey_service, "Path", side_effect=paths), \
                    patch.object(valkey_service, "status", return_value=state), \
                    patch.object(valkey_service, "command", return_value="argv[]=" + service["args"][0] + " ;"), \
                    self.assertRaisesRegex(RuntimeError, "memlock"):
                valkey_service.evidence({"native_valkey": service})
