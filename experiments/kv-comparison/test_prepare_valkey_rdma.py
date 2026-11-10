import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import prepare_valkey_rdma as prepare


class PrepareValkeyTests(unittest.TestCase):
    def test_wrong_tag_commit_fails_before_build(self):
        calls = []
        def run(argv, cwd=None, deadline=None):
            calls.append(argv)
            return SimpleNamespace(stdout="wrong\n")
        with tempfile.TemporaryDirectory() as root, patch.object(prepare, "run", side_effect=run):
            with self.assertRaisesRegex(ValueError, "pinned commit"):
                prepare.prepare(Path(root) / "valkey")
        self.assertFalse(any(call[0] == "make" for call in calls))

    def test_hook_rejects_missing_or_duplicate_anchor(self):
        source = "\n".join(old for old, _ in prepare.HOOKS)
        patched = prepare.hooked_source(source)
        self.assertIn("kv_diag_cq_poll_begin();", patched)
        with self.assertRaisesRegex(ValueError, "does not match exactly"):
            prepare.hooked_source(source + "\n" + prepare.HOOKS[1][0])
        with self.assertRaisesRegex(ValueError, "does not match exactly"):
            prepare.hooked_source(source.replace(prepare.HOOKS[2][0], "changed"))

