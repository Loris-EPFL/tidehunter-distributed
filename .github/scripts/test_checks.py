"""Offline unit tests for the checks.py ratchets."""

import io
from contextlib import redirect_stdout
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / ".github/scripts"))
import checks

INLINE_BODY = """\
#[cfg(test)]
mod tests {
    #[test]
    fn behaviour_under_condition() {}
}
"""

MODULE_ONLY = """\
#[cfg(test)]
mod tests;
"""

CLEAN = """\
pub fn production_path() {}
"""


class TestLayoutRatchetTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        for relative_root in checks.TEST_LAYOUT_ROOTS:
            (self.root / relative_root).mkdir(parents=True)
        self.baseline = self.root / "test-layout-baseline.txt"
        self.baseline.write_text("# empty baseline\n")

    def tearDown(self):
        self.temporary.cleanup()

    def write(self, relative, text):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return relative

    def run_check(self):
        with redirect_stdout(io.StringIO()) as output:
            checks.check_test_layout(root=self.root, baseline_path=self.baseline)
        return output.getvalue()

    def test_clean_file_passes(self):
        self.write("tidehunter/src/distributed/clean.rs", CLEAN)
        output = self.run_check()
        self.assertIn("total: 0 files", output)

    def test_inline_body_without_baseline_entry_fails(self):
        self.write("tidehunter/src/distributed/inline.rs", INLINE_BODY)
        with self.assertRaisesRegex(RuntimeError, "not in baseline"):
            self.run_check()

    def test_module_declaration_only_is_allowed(self):
        self.write("tidehunter/src/distributed/module.rs", MODULE_ONLY)
        output = self.run_check()
        self.assertIn("total: 0 files", output)

    def test_path_attribute_module_declaration_is_allowed(self):
        self.write("tidehunter/src/distributed/path.rs",
                   '#[cfg(test)]\n#[path = "path_tests.rs"]\nmod tests;\n')
        output = self.run_check()
        self.assertIn("total: 0 files", output)

    def test_listed_equal_counts_pass(self):
        relative = self.write("tidehunter/src/distributed/listed.rs", INLINE_BODY)
        self.baseline.write_text(f"{relative} 1 1\n")
        output = self.run_check()
        self.assertIn(f"{relative}: 1 modules, 1 tests", output)

    def test_listed_growth_fails(self):
        relative = self.write("tidehunter/src/distributed/listed.rs",
                              INLINE_BODY + "\n#[test]\nfn second() {}\n")
        self.baseline.write_text(f"{relative} 1 1\n")
        with self.assertRaisesRegex(RuntimeError, "grew past baseline"):
            self.run_check()

    def test_stale_entry_fails(self):
        relative = self.write("tidehunter/src/distributed/clean.rs", CLEAN)
        self.baseline.write_text(f"{relative} 1 1\n")
        with self.assertRaisesRegex(RuntimeError, "stale baseline entry"):
            self.run_check()

    def test_top_level_test_attribute_fails(self):
        self.write("tidehunter/src/distributed/ungated.rs",
                   "#[test]\nfn top_level() {}\n")
        with self.assertRaisesRegex(RuntimeError, "1 tests"):
            self.run_check()

    def test_comments_and_strings_are_not_counted(self):
        self.write("tidehunter/src/distributed/commented.rs",
                   '// #[test]\nconst TEXT: &str = "#[test]";\n')
        output = self.run_check()
        self.assertIn("total: 0 files", output)

    def test_skipped_paths_are_ignored(self):
        self.write("tidehunter/src/distributed/tests.rs", INLINE_BODY)
        self.write("tidehunter/src/distributed/foo_tests.rs", INLINE_BODY)
        self.write("tidehunter/src/distributed/tests/nested.rs", INLINE_BODY)
        output = self.run_check()
        self.assertIn("total: 0 files", output)


if __name__ == "__main__":
    unittest.main()
