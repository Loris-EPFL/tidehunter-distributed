"""Offline unit tests for the per-tip lint stack runner."""

import argparse
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / ".github/scripts"))
import lint_stack


class StackFixture(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.git("init", "--quiet")
        self.git("config", "user.email", "test@example.invalid")
        self.git("config", "user.name", "test")
        self.branch = self.git("symbolic-ref", "--short", "HEAD").strip()
        self.commits = []
        for index in range(3):
            (self.root / "file").write_text(str(index))
            self.git("add", "file")
            self.git("commit", "--quiet", "-m", f"tip {index}")
            self.commits.append(self.git("rev-parse", "HEAD").strip())

    def tearDown(self):
        self.temporary.cleanup()

    def git(self, *args):
        return subprocess.run(["git", *args], cwd=self.root, text=True,
                              capture_output=True, check=True).stdout


class CommitSelectionTests(StackFixture):
    def test_stack_commits_are_oldest_first_and_exclude_the_base(self):
        self.assertEqual(lint_stack.commits(self.root, self.commits[0], self.commits[-1]),
                         self.commits[1:])

    def test_a_base_that_is_not_an_ancestor_is_refused(self):
        self.git("checkout", "--quiet", "-b", "other", self.commits[0])
        (self.root / "file").write_text("other")
        self.git("add", "file")
        self.git("commit", "--quiet", "-m", "unrelated tip")
        unrelated = self.git("rev-parse", "HEAD").strip()
        self.git("checkout", "--quiet", self.branch)
        with self.assertRaisesRegex(RuntimeError, "not an ancestor"):
            lint_stack.commits(self.root, unrelated, self.commits[-1])


class RunTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def run_stack(self, profiles, keep=False):
        temporary = self.root / "work"
        tree = temporary / "tree"
        with patch.object(lint_stack, "commits", return_value=["one", "two"]), \
                patch.object(lint_stack.tempfile, "mkdtemp", return_value=str(temporary)), \
                patch.object(lint_stack, "worktree") as worktree, \
                patch.object(lint_stack, "checkout") as checkout, \
                patch.object(lint_stack, "run_profile", side_effect=profiles), \
                patch.object(lint_stack, "remove_worktree") as remove, \
                patch.object(lint_stack.shutil, "rmtree") as rmtree, \
                patch.object(lint_stack, "subject", side_effect=lambda root, commit: commit):
            status = lint_stack.run(argparse.Namespace(base="base", tip="tip", keep=keep))
        self.assertEqual([call.args[1] for call in checkout.call_args_list], ["one", "two"])
        self.assertEqual(worktree.call_args.args[1], tree)
        self.assertEqual(rmtree.call_count, 0 if keep else 1)
        if keep:
            remove.assert_not_called()
        else:
            remove.assert_called_once_with(lint_stack.ROOT, tree)
        return status

    def test_every_tip_runs_the_lint_profile(self):
        self.assertEqual(self.run_stack([None, None]), 0)

    def test_a_failing_tip_stops_the_run(self):
        self.assertEqual(self.run_stack([None, subprocess.CalledProcessError(1, "checks.py")]), 1)

    def test_keep_leaves_the_worktree_for_diagnosis(self):
        self.assertEqual(self.run_stack([None, None], keep=True), 0)

    def test_an_empty_stack_is_refused(self):
        with patch.object(lint_stack, "commits", return_value=[]):
            with self.assertRaisesRegex(RuntimeError, "no commits"):
                lint_stack.run(argparse.Namespace(base="base", tip="tip", keep=False))

    def test_main_reports_a_failed_setup(self):
        with patch.object(lint_stack, "run", side_effect=RuntimeError("git failed")):
            self.assertEqual(lint_stack.main(["--base", "base"]), 1)


if __name__ == "__main__":
    unittest.main()
