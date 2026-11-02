import unittest
import subprocess
from unittest.mock import patch

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
