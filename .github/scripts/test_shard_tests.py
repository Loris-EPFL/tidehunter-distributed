"""Offline unit tests for the cargo test shard selector."""

from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / ".github/scripts"))
import shard_tests

LISTING = """\
alpha::tests::one: test
alpha::tests::two: test
beta::tests::three: test
gamma::tests::four: test
delta::tests::five: test
"""

WITH_DOCTEST = LISTING + "src/lib.rs - documented (line 12): test\n"


class Selection(unittest.TestCase):
    def test_every_name_runs_in_exactly_one_shard(self):
        names = shard_tests.parse_listing(LISTING)
        for shards in (1, 2, 3, 5, 7):
            selected = [shard_tests.select(names, index, shards) for index in range(shards)]
            flat = [name for shard in selected for name in shard]
            self.assertEqual(sorted(flat), sorted(names))
            self.assertEqual(len(flat), len(set(flat)))

    def test_doctests_stay_out_of_the_shard_pool(self):
        unit, docs = shard_tests.split(shard_tests.parse_listing(WITH_DOCTEST))
        self.assertEqual(docs, 1)
        self.assertNotIn("src/lib.rs - documented (line 12)", unit)
        self.assertEqual(len(unit), 5)

    def test_no_doctest_means_no_doctest_run(self):
        self.assertEqual(shard_tests.split(shard_tests.parse_listing(LISTING))[1], 0)

    def test_selection_is_stable_across_shard_counts(self):
        names = shard_tests.parse_listing(LISTING)
        self.assertEqual(shard_tests.select(names, 0, 3), shard_tests.select(names, 0, 3))
        self.assertEqual(shard_tests.select(names, 0, 3), ["alpha::tests::one", "delta::tests::five"])

    def test_duplicate_names_are_counted_once_per_run(self):
        names = shard_tests.parse_listing(LISTING + "alpha::tests::one: test\n")
        self.assertEqual(names["alpha::tests::one"], 2)

    def test_benchmarks_and_build_output_are_not_tests(self):
        names = shard_tests.parse_listing("   Compiling tidehunter v0.1.0\nalpha::bench: bench\n")
        self.assertEqual(names, {})


class ResultCounting(unittest.TestCase):
    def test_summary_line_counts_passed_failed_and_ignored(self):
        line = ("test result: ok. 242 passed; 0 failed; 3 ignored; 0 measured; "
                "5 filtered out; finished in 739.90s")
        match = shard_tests.RESULT.match(line)
        self.assertIsNotNone(match)
        self.assertEqual(sum(int(count) for count in match.groups()), 245)

    def test_failed_summary_line_still_counts(self):
        line = ("test result: FAILED. 1 passed; 2 failed; 0 ignored; 0 measured; "
                "0 filtered out; finished in 0.31s")
        match = shard_tests.RESULT.match(line)
        self.assertIsNotNone(match)
        self.assertEqual(sum(int(count) for count in match.groups()), 3)

    def test_test_output_is_not_a_summary(self):
        self.assertIsNone(shard_tests.RESULT.match("test result: ok. 1 passed; finished"))
        self.assertIsNone(shard_tests.RESULT.match("note: test result: ok. 1 passed;"))


class OutcomeChecking(unittest.TestCase):
    def outcomes(self, text):
        lines = text.splitlines()
        return shard_tests.collections.Counter(
            match.group(1) for match in map(shard_tests.OUTCOME.match, lines) if match)

    def test_outcome_lines_name_the_test(self):
        counts = self.outcomes("test a::one ... ok\ntest a::two ... ignored, slow\n"
                               "test a::three ... FAILED\ntest a::four has been running\n")
        self.assertEqual(counts, {"a::one": 1, "a::two": 1, "a::three": 1})

    def test_should_panic_header_counts_once_without_a_status(self):
        # libtest prints the header, then the panic message, then a bare status.
        counts = self.outcomes("test cell::tests::too_large - should panic ... \n"
                               "thread 'cell::tests::too_large' panicked at src/cell.rs:9:5:\n"
                               "ok\n"
                               "test cell::tests::other - should panic ... ok\n")
        self.assertEqual(counts, {"cell::tests::too_large": 1, "cell::tests::other": 1})

    def test_child_process_outcomes_do_not_fail_the_shard(self):
        # A test that re-executes the binary prints the child's outcome and summary.
        stream = ("test lock::tests::cross ... ok\n"
                  "test lock::tests::holder ... ok\n"
                  "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; "
                  "9 filtered out; finished in 0.01s\n")
        selected = shard_tests.collections.Counter({"lock::tests::cross": 1})
        self.assertEqual(shard_tests.check_names("shard", selected, 0, 2, self.outcomes(stream)), 0)

    def test_a_selected_name_without_an_outcome_fails(self):
        selected = shard_tests.collections.Counter({"a::one": 1, "a::two": 1})
        with self.assertRaisesRegex(RuntimeError, "no outcome for 1 selected tests: a::two"):
            shard_tests.check_names("shard", selected, 0, 1, self.outcomes("test a::one ... ok\n"))

    def test_a_name_listed_twice_needs_two_outcomes(self):
        selected = shard_tests.collections.Counter({"a::one": 2})
        with self.assertRaises(RuntimeError):
            shard_tests.check_names("shard", selected, 0, 1, self.outcomes("test a::one ... ok\n"))

    def test_a_failing_test_returns_its_exit_code(self):
        selected = shard_tests.collections.Counter({"a::one": 1})
        self.assertEqual(
            shard_tests.check_names("shard", selected, 101, 1, self.outcomes("test a::one ... FAILED\n")), 101)


if __name__ == "__main__":
    unittest.main()
