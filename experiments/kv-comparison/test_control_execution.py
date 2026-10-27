"""Real-filesystem regression tests for the guest operation execution gate."""
import fcntl
import hashlib
import multiprocessing
import os
from pathlib import Path
import tempfile
import time
import unittest

import control_execution as gate


def _process_execute(root, key, request, deadline, entered, started, release, calls, output):
    entered.set()

    def execute():
        with calls.get_lock():
            calls.value += 1
        started.set()
        if not release.wait(5):
            raise TimeoutError("test callback release was not signalled")
        return {"status": "done", "items": [1, 2, 3]}

    try:
        output.put(("ok", gate.execute_at_most_once(
            Path(root), key, request, execute, deadline_epoch=deadline)))
    except BaseException as exc:
        output.put(("error", type(exc).__name__, str(exc)))


def _crash_during_operation(root, key, request, deadline, started):
    def execute():
        started.set()
        os._exit(73)

    gate.execute_at_most_once(Path(root), key, request, execute, deadline_epoch=deadline)


class ExecutionGateTests(unittest.TestCase):
    def test_completed_duplicate_returns_cached_result_even_after_deadline(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            calls = []
            deadline = time.time() + 2
            expected = {"ok": True, "value": ["persisted", 7]}
            first = gate.execute_at_most_once(
                root, "operation-1", {"action": "write", "key": "k"},
                lambda: calls.append(1) or expected,
                deadline_epoch=deadline,
            )
            self.assertEqual(first, expected)
            self.assertEqual(calls, [1])

            cached = gate.execute_at_most_once(
                root, "operation-1", {"key": "k", "action": "write"},
                lambda: self.fail("cached retry invoked its callback"),
                deadline_epoch=deadline,
            )
            self.assertEqual(cached, expected)
            time.sleep(max(0.0, deadline - time.time()) + 0.02)
            expired_cached = gate.execute_at_most_once(
                root, "operation-1", {"action": "write", "key": "k"},
                lambda: self.fail("expired cached retry invoked its callback"),
                deadline_epoch=deadline,
            )
            self.assertEqual(expired_cached, expected)
            self.assertEqual(calls, [1])

    def test_results_preserve_existing_32_mib_receipt_limit(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            deadline = time.time() + 10
            expected = {"payload": "v" * (gate.MAX_JSON_BYTES - 64)}
            first = gate.execute_at_most_once(
                root, "large-result", {"action": "report"},
                lambda: expected, deadline_epoch=deadline,
            )
            self.assertEqual(first, expected)
            cached = gate.execute_at_most_once(
                root, "large-result", {"action": "report"},
                lambda: self.fail("large cached result invoked callback"),
                deadline_epoch=deadline,
            )
            self.assertEqual(cached, expected)


    def test_concurrent_process_duplicates_execute_callback_once(self):
        context = multiprocessing.get_context("fork")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            entered_a = context.Event()
            entered_b = context.Event()
            started = context.Event()
            release = context.Event()
            calls = context.Value("i", 0)
            output = context.Queue()
            deadline = time.time() + 10
            common = (str(root), "concurrent-operation", {"action": "put", "value": 9}, deadline)
            first = context.Process(
                target=_process_execute,
                args=(*common, entered_a, started, release, calls, output),
            )
            second = context.Process(
                target=_process_execute,
                args=(*common, entered_b, started, release, calls, output),
            )
            processes = [first, second]
            try:
                first.start()
                self.assertTrue(started.wait(3), "first process did not enter callback")
                second.start()
                self.assertTrue(entered_b.wait(3), "duplicate process did not enter gate")
                self.assertTrue(second.is_alive(), "duplicate process returned before first completion")
            finally:
                release.set()
                for process in processes:
                    if process.pid is not None:
                        process.join(6)
                        if process.is_alive():
                            process.terminate()
                            process.join(2)
            for process in processes:
                if process.pid is not None:
                    self.assertFalse(process.is_alive(), "gate process exceeded bounded join")
                    self.assertEqual(process.exitcode, 0)

            outputs = [output.get(timeout=2) for _ in range(2)]
            self.assertTrue(all(item[0] == "ok" for item in outputs), outputs)
            self.assertEqual([item[1] for item in outputs], [
                {"status": "done", "items": [1, 2, 3]},
                {"status": "done", "items": [1, 2, 3]},
            ])
            self.assertEqual(calls.value, 1)

    def test_same_key_rejects_changed_request_or_deadline(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            deadline = time.time() + 10
            gate.execute_at_most_once(
                root, "stable-key", {"action": "set", "value": 1},
                lambda: {"result": "cached"}, deadline_epoch=deadline,
            )
            for request, changed_deadline in (
                ({"action": "set", "value": 2}, deadline),
                ({"action": "set", "value": 1}, deadline + 1),
            ):
                with self.subTest(request=request, deadline=changed_deadline):
                    with self.assertRaises(gate.ExecutionGateError):
                        gate.execute_at_most_once(
                            root, "stable-key", request,
                            lambda: self.fail("conflicting operation invoked callback"),
                            deadline_epoch=changed_deadline,
                        )

    def test_expired_unseen_operation_never_starts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            calls = []
            with self.assertRaises(gate.ExecutionGateError):
                gate.execute_at_most_once(
                    root, "expired-operation", {"action": "delete"},
                    lambda: calls.append("called"), deadline_epoch=time.time() - 1,
                )
            self.assertEqual(calls, [])
            self.assertEqual(list((root / ".guest-operation-gates").glob("*.json")), [])

    def test_callback_error_leaves_non_reexecutable_started_marker(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            deadline = time.time() + 10
            calls = []

            def fail_once():
                calls.append("first")
                raise OSError("consumer-visible callback failure")

            with self.assertRaisesRegex(OSError, "consumer-visible"):
                gate.execute_at_most_once(
                    root, "failed-callback", {"action": "mutate"}, fail_once,
                    deadline_epoch=deadline,
                )
            with self.assertRaisesRegex(gate.ExecutionGateError, "started marker"):
                gate.execute_at_most_once(
                    root, "failed-callback", {"action": "mutate"},
                    lambda: calls.append("retry"), deadline_epoch=deadline,
                )
            self.assertEqual(calls, ["first"])

    def test_process_crash_leaves_fail_closed_started_marker(self):
        context = multiprocessing.get_context("fork")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            started = context.Event()
            deadline = time.time() + 10
            process = context.Process(
                target=_crash_during_operation,
                args=(str(root), "crashed-operation", {"action": "apply"}, deadline, started),
            )
            process.start()
            try:
                self.assertTrue(started.wait(3), "crashing process did not enter callback")
                process.join(4)
            finally:
                if process.is_alive():
                    process.terminate()
                    process.join(2)
            self.assertFalse(process.is_alive(), "crash fixture exceeded bounded join")
            self.assertEqual(process.exitcode, 73)

            calls = []
            with self.assertRaisesRegex(gate.ExecutionGateError, "started marker"):
                gate.execute_at_most_once(
                    root, "crashed-operation", {"action": "apply"},
                    lambda: calls.append("retry"), deadline_epoch=deadline,
                )
            self.assertEqual(calls, [])

    def test_corrupt_state_fails_closed_without_callback(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            deadline = time.time() + 10
            gate.execute_at_most_once(
                root, "corrupt-operation", {"action": "read"},
                lambda: {"value": 3}, deadline_epoch=deadline,
            )
            state_path, = (root / ".guest-operation-gates").glob("*.json")
            state_path.write_bytes(b'{"state":"completed","result":')
            calls = []
            with self.assertRaisesRegex(gate.ExecutionGateError, "malformed JSON"):
                gate.execute_at_most_once(
                    root, "corrupt-operation", {"action": "read"},
                    lambda: calls.append("retry"), deadline_epoch=deadline,
                )
            self.assertEqual(calls, [])

    def test_lock_wait_is_finite_and_bounded_by_deadline(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            gate_directory = root / ".guest-operation-gates"
            gate_directory.mkdir()
            key = "locked-operation"
            digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
            lock_path = gate_directory / f"{digest}.lock"
            lock = lock_path.open("a+b")
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            calls = []
            deadline = time.time() + 0.2
            started = time.monotonic()
            try:
                with self.assertRaisesRegex(gate.ExecutionGateError, "lock deadline"):
                    gate.execute_at_most_once(
                        root, key, {"action": "blocked"},
                        lambda: calls.append("called"), deadline_epoch=deadline,
                    )
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
                lock.close()
            self.assertLess(time.monotonic() - started, 2.0)
            self.assertEqual(calls, [])

    def test_json_key_and_deadline_inputs_are_bounded_and_strict(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            invalid = (
                ("", {}, time.time() + 10),
                ("x" * (gate.MAX_KEY_BYTES + 1), {}, time.time() + 10),
                ("bad-json", {"number": float("nan")}, time.time() + 10),
                ("bad-deadline", {}, float("inf")),
                ("negative-deadline", {}, -1.0),
                ("unbounded-deadline", {}, gate.MAX_DEADLINE_EPOCH + 1),
            )
            for key, request, deadline in invalid:
                with self.subTest(key=key[:12], deadline=deadline):
                    with self.assertRaises(gate.ExecutionGateError):
                        gate.execute_at_most_once(
                            root, key, request,
                            lambda: self.fail("invalid operation invoked callback"),
                            deadline_epoch=deadline,
                        )

            with self.assertRaises(gate.ExecutionGateError):
                gate.execute_at_most_once(
                    root, "oversized", {"value": "x" * (gate.MAX_JSON_BYTES + 1)},
                    lambda: self.fail("oversized operation invoked callback"),
                    deadline_epoch=time.time() + 10,
                )


if __name__ == "__main__":
    unittest.main()
