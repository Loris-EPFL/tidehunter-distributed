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

