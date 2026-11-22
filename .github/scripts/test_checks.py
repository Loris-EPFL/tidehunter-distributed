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

DISTRIBUTED = "tidehunter/src/distributed"


class RatchetFixture(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        for relative_root in checks.RATCHET_ROOTS:
            (self.root / relative_root).mkdir(parents=True)
        self.baseline = self.root / "baseline.txt"
        self.baseline.write_text("# empty baseline\n")

    def tearDown(self):
        self.temporary.cleanup()

    def write(self, relative, text):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return relative


class TestLayoutRatchetTests(RatchetFixture):
    def run_check(self):
        with redirect_stdout(io.StringIO()) as output:
            checks.check_test_layout(root=self.root, baseline_path=self.baseline)
        return output.getvalue()

    def counts(self, relative):
        return checks.layout_counts(self.root)[relative]

    def test_clean_file_passes(self):
        relative = self.write(f"{DISTRIBUTED}/clean.rs", CLEAN)
        self.run_check()
        self.assertEqual(self.counts(relative), (0, 0))

    def test_inline_body_without_baseline_entry_fails(self):
        self.write(f"{DISTRIBUTED}/inline.rs", INLINE_BODY)
        with self.assertRaisesRegex(RuntimeError, "not in the baseline"):
            self.run_check()

    def test_module_declaration_only_is_allowed(self):
        relative = self.write(f"{DISTRIBUTED}/module.rs", MODULE_ONLY)
        self.run_check()
        self.assertEqual(self.counts(relative), (0, 0))

    def test_path_attribute_module_declaration_is_allowed(self):
        relative = self.write(f"{DISTRIBUTED}/path.rs",
                              '#[cfg(test)]\n#[path = "path_tests.rs"]\nmod tests;\n')
        self.run_check()
        self.assertEqual(self.counts(relative), (0, 0))

    def test_listed_equal_counts_pass(self):
        relative = self.write(f"{DISTRIBUTED}/listed.rs", INLINE_BODY)
        self.baseline.write_text(f"{relative} 1 1\n")
        self.assertIn("1 files, 1 modules, 1 tests", self.run_check())

    def test_listed_growth_fails(self):
        relative = self.write(f"{DISTRIBUTED}/listed.rs",
                              INLINE_BODY + "\n#[test]\nfn second() {}\n")
        self.baseline.write_text(f"{relative} 1 1\n")
        with self.assertRaisesRegex(RuntimeError, "grew past baseline"):
            self.run_check()

    def test_shrunk_file_requires_the_exact_baseline_line(self):
        relative = self.write(f"{DISTRIBUTED}/listed.rs", INLINE_BODY)
        self.baseline.write_text(f"{relative} 1 3\n")
        with self.assertRaisesRegex(RuntimeError, f"fell below baseline 1/3.*must read: {relative} 1 1"):
            self.run_check()

    def test_stale_entry_fails(self):
        relative = self.write(f"{DISTRIBUTED}/clean.rs", CLEAN)
        self.baseline.write_text(f"{relative} 1 1\n")
        with self.assertRaisesRegex(RuntimeError, f"stale baseline entry.*delete the line: {relative} 1 1"):
            self.run_check()

    def test_vanished_file_entry_fails(self):
        self.baseline.write_text(f"{DISTRIBUTED}/gone.rs 1 1\n")
        with self.assertRaisesRegex(RuntimeError, "stale baseline entry"):
            self.run_check()

    def test_top_level_test_attribute_fails(self):
        self.write(f"{DISTRIBUTED}/ungated.rs", "#[test]\nfn top_level() {}\n")
        with self.assertRaisesRegex(RuntimeError, "1 tests"):
            self.run_check()

    def test_cfg_test_test_function_outside_a_module_fails(self):
        self.write(f"{DISTRIBUTED}/storage.rs", "#[cfg(test)]\n#[test]\nfn outside() {}\n")
        with self.assertRaisesRegex(RuntimeError, "0 inline test modules and 1 tests"):
            self.run_check()

    def test_tokio_attribute_with_arguments_is_counted(self):
        relative = self.write(
            f"{DISTRIBUTED}/async_tests_inline.rs",
            '#[tokio::test(flavor = "multi_thread", worker_threads = 2)]\nasync fn a() {}\n'
            "#[tokio::test]\nasync fn b() {}\n#[custom::path::test]\nfn c() {}\n")
        self.assertEqual(self.counts(relative), (0, 3))

    def test_attribute_on_a_separate_line_is_counted(self):
        relative = self.write(f"{DISTRIBUTED}/split.rs", "#[\n    tokio::test\n]\nasync fn a() {}\n")
        self.assertEqual(self.counts(relative), (0, 1))

    def test_lookalike_attributes_are_not_counted(self):
        relative = self.write(
            f"{DISTRIBUTED}/lookalike.rs",
            "#[test_case(1)]\nfn a() {}\n#[rstest]\nfn b() {}\n#[cfg_attr(test, derive(Debug))]\nstruct S;\n")
        self.assertEqual(self.counts(relative), (0, 0))

    def test_tokio_growth_in_listed_file_fails(self):
        relative = self.write(f"{DISTRIBUTED}/listed.rs",
                              INLINE_BODY + "\n#[tokio::test]\nasync fn second() {}\n")
        self.baseline.write_text(f"{relative} 1 1\n")
        with self.assertRaisesRegex(RuntimeError, "grew past baseline"):
            self.run_check()

    def test_comments_and_strings_are_not_counted(self):
        relative = self.write(f"{DISTRIBUTED}/commented.rs",
                              '// #[test]\nconst TEXT: &str = "#[test]";\n')
        self.run_check()
        self.assertEqual(self.counts(relative), (0, 0))

    def test_attribute_between_cfg_test_and_an_inline_module_is_counted(self):
        relative = self.write(f"{DISTRIBUTED}/attributed.rs",
                              "#[cfg(test)]\n#[allow(dead_code)]\nmod tests {\n}\n")
        self.assertEqual(self.counts(relative), (1, 0))

    def test_attribute_with_nested_brackets_is_skipped_whole(self):
        relative = self.write(f"{DISTRIBUTED}/nested.rs",
                              "#[cfg(test)]\n#[marker([1, 2])]\nmod tests {\n}\n")
        self.assertEqual(self.counts(relative), (1, 0))

    def test_restricted_visibility_inline_module_is_counted(self):
        relative = self.write(f"{DISTRIBUTED}/visible.rs",
                              "#[cfg(test)]\npub(crate) mod tests {\n}\n")
        self.assertEqual(self.counts(relative), (1, 0))

    def test_cfg_test_with_inner_spaces_is_counted(self):
        relative = self.write(f"{DISTRIBUTED}/spaced.rs", "#[ cfg ( test ) ]\nmod tests {\n}\n")
        self.assertEqual(self.counts(relative), (1, 0))

    def test_raw_string_with_a_quote_hides_its_attribute(self):
        relative = self.write(f"{DISTRIBUTED}/raw.rs",
                              'const R: &str = r#"a " b #[test]"#;\n#[test]\nfn f() {}\n')
        self.assertEqual(self.counts(relative), (0, 1))

    def test_raw_string_has_no_escapes(self):
        relative = self.write(f"{DISTRIBUTED}/raw_path.rs",
                              'const P: &str = r"C:\\";\n#[test]\nfn f() {}\n')
        self.assertEqual(self.counts(relative), (0, 1))

    def test_block_comment_hides_its_attribute(self):
        relative = self.write(f"{DISTRIBUTED}/block.rs",
                              "/* #[test]\nfn hidden() {} */\n#[test]\nfn f() {}\n")
        self.assertEqual(self.counts(relative), (0, 1))

    def test_character_literals_and_lifetimes_do_not_open_a_string(self):
        relative = self.write(
            f"{DISTRIBUTED}/chars.rs",
            "fn a<'a>(x: &'a str) -> char { '\"' }\nfn b() -> char { '\\'' }\n"
            "#[test]\nfn f() {}\n")
        self.assertEqual(self.counts(relative), (0, 1))

    def test_escaped_quote_does_not_end_a_string(self):
        relative = self.write(
            f"{DISTRIBUTED}/escaped.rs",
            'const S: &str = "a \\" #[test] \\" b";\n#[test]\nfn f() {}\n')
        self.assertEqual(self.counts(relative), (0, 1))

    def test_test_only_paths_are_skipped(self):
        for name in ("tests.rs", "foo_tests.rs", "tests/nested.rs", "foo_tests/nested.rs",
                     "deep/foo_tests/more/nested.rs"):
            self.write(f"{DISTRIBUTED}/{name}", INLINE_BODY)
        self.run_check()
        self.assertEqual(checks.layout_counts(self.root), {})

    def test_lookalike_paths_are_not_skipped(self):
        for name in ("attests.rs", "contests.rs", "foo_tests_helper.rs", "mytests/nested.rs"):
            self.write(f"{DISTRIBUTED}/{name}", "#[test]\nfn f() {}\n")
        self.assertEqual(len(checks.layout_counts(self.root)), 4)

    def test_examples_support_is_scanned(self):
        self.write("tidehunter/examples/support/cluster.rs", INLINE_BODY)
        with self.assertRaisesRegex(RuntimeError, "examples/support/cluster.rs"):
            self.run_check()

    def test_missing_root_fails(self):
        (self.root / "tidehunter/examples/support").rmdir()
        with self.assertRaisesRegex(RuntimeError, "ratchet root is missing"):
            self.run_check()


class FileLengthRatchetTests(RatchetFixture):
    def lines(self, count):
        return "fn f() {}\n" * count

    def run_check(self):
        checks.check_file_length(root=self.root, baseline_path=self.baseline)

    def test_unlisted_file_within_cap_passes(self):
        self.write(f"{DISTRIBUTED}/small.rs", self.lines(checks.FILE_LENGTH_LIMIT))
        self.run_check()

    def test_unlisted_file_over_cap_fails(self):
        self.write(f"{DISTRIBUTED}/big.rs", self.lines(checks.FILE_LENGTH_LIMIT + 1))
        with self.assertRaisesRegex(RuntimeError, "over the cap and not in the baseline"):
            self.run_check()

    def test_listed_equal_passes(self):
        relative = self.write(f"{DISTRIBUTED}/big.rs", self.lines(1600))
        self.baseline.write_text(f"{relative} 1600\n")
        self.run_check()

    def test_listed_growth_fails(self):
        relative = self.write(f"{DISTRIBUTED}/big.rs", self.lines(1601))
        self.baseline.write_text(f"{relative} 1600\n")
        with self.assertRaisesRegex(RuntimeError, "grew past baseline 1600"):
            self.run_check()

    def test_shrink_requires_the_exact_baseline_line(self):
        relative = self.write(f"{DISTRIBUTED}/big.rs", self.lines(1550))
        self.baseline.write_text(f"{relative} 1600\n")
        with self.assertRaisesRegex(RuntimeError, f"change the baseline line to: {relative} 1550"):
            self.run_check()

    def test_entry_within_cap_is_stale(self):
        relative = self.write(f"{DISTRIBUTED}/big.rs", self.lines(1500))
        self.baseline.write_text(f"{relative} 1600\n")
        with self.assertRaisesRegex(RuntimeError, "stale baseline entry"):
            self.run_check()

    def test_vanished_file_entry_is_stale(self):
        self.baseline.write_text(f"{DISTRIBUTED}/gone.rs 1600\n")
        with self.assertRaisesRegex(RuntimeError, "stale baseline entry"):
            self.run_check()

    def test_missing_root_fails(self):
        (self.root / "tidehunter-fabric/src").rmdir()
        with self.assertRaisesRegex(RuntimeError, "ratchet root is missing"):
            self.run_check()


class BaselineParsingTests(RatchetFixture):
    def test_duplicate_entry_is_rejected(self):
        self.baseline.write_text("a.rs 1 1\na.rs 1 2\n")
        with self.assertRaisesRegex(RuntimeError, "duplicate baseline entry for a.rs"):
            checks.read_baseline(self.baseline, 2)

    def test_short_line_is_rejected_with_a_message(self):
        self.baseline.write_text("a.rs 5\n")
        with self.assertRaisesRegex(RuntimeError, "malformed baseline line 'a.rs 5'"):
            checks.read_baseline(self.baseline, 2)

    def test_non_numeric_count_is_rejected_with_a_message(self):
        self.baseline.write_text("a.rs 5 many\n")
        with self.assertRaisesRegex(RuntimeError, "malformed baseline line 'a.rs 5 many'"):
            checks.read_baseline(self.baseline, 2)

    def test_path_without_a_count_is_rejected_with_a_message(self):
        self.baseline.write_text("a.rs\n")
        with self.assertRaisesRegex(RuntimeError, "malformed baseline line 'a.rs'"):
            checks.read_baseline(self.baseline, 1)


if __name__ == "__main__":
    unittest.main()
