"""The hybrid oracle gate must accept exactly one measured test and refuse a silent skip."""
import copy
import json
import unittest

import hybrid_native_oracle_ci as gate

DIGEST = "e6f773bfc17f436ea6a9054ecad9845982c54b65c383a109b30187f06a3c64ebf704a6b3ac89bc193fd86b4822eb70679c93f425a10917976504feb483c264a9"
READS = "680de4c512b172d1b5f0ac098df359fb87a5e341d801a2f95f5e2691cede767c96d4f461cd0dc1d682dc94f7becc35b36200a56d6e79670bb447c73f929ddaa4"
TABLE_ROWS = [2, 2, 2, 2, 90, 90, 56, 90, 90, 90, 2, 1, 0, 1306, 90, 66, 3]


def engine(counts):
    final = {"content_blake2b512": DIGEST, "full_values": True, "verified_rows": 1982,
             "table_rows": TABLE_ROWS}
    return {"read_digest": READS, "restart_read_digest": READS, "final": final,
            "restart_final": copy.deepcopy(final), "counts": counts}


def receipt():
    native = {"store_gets": 1014, "overlay_demands": 232, "put_members": 1414,
              "write_calls": 18, "checkpoints": 2, "transactions": 90}
    hybrid = {"store_gets": 1014, "put_members": 1414, "write_calls": 18,
              "checkpoints": 2, "transactions": 90}
    return {"repetitions": 2, "keys": 2107, "absent_keys": 15,
            "native": engine(native), "hybrid": engine(hybrid)}


def render(value):
    return f"running 1 test\n{gate.RECEIPT_PREFIX}{json.dumps(value)}\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 240 filtered out; finished in 28.35s\n"


class OracleGateTests(unittest.TestCase):
    def test_one_passing_test_is_required(self):
        self.assertEqual(gate.require_single_pass(render(receipt())), 1)
        for output in ("test result: ok. 0 passed; 0 failed; 0 ignored;",
                       "test result: ok. 1 passed; 0 failed; 0 ignored;\n"
                       "test result: ok. 1 passed; 0 failed; 0 ignored;",
                       "test result: FAILED. 0 passed; 1 failed; 0 ignored;"):
            with self.subTest(output=output):
                with self.assertRaisesRegex(RuntimeError, "exactly one passing test"):
                    gate.require_single_pass(output)

    def test_exactly_one_receipt_is_required(self):
        with self.assertRaisesRegex(RuntimeError, "exactly one receipt"):
            gate.receipt("running 1 test\n")
        line = gate.RECEIPT_PREFIX + json.dumps(receipt())
        with self.assertRaisesRegex(RuntimeError, "exactly one receipt"):
            gate.receipt(f"{line}\n{line}\n")

    def test_valid_receipt_passes(self):
        value = gate.check_receipt(receipt())
        self.assertEqual(value["repetitions"], 2)

    def test_receipt_shares_the_libtest_progress_line(self):
        # With --nocapture libtest writes "test <name> ... " without a newline
        # before the test's println output, so the receipt starts mid-line.
        line = "test backends::replay::tests::oracle ... " + gate.RECEIPT_PREFIX + json.dumps(receipt())
        self.assertEqual(gate.check_receipt(gate.receipt(line))["keys"], receipt()["keys"])

    def test_any_difference_is_refused(self):
        cases = {
            "read_digest": ("hybrid", "read_digest", "00"),
            "restart_read_digest": ("hybrid", "restart_read_digest", "00"),
            "restart_final": ("hybrid", "restart_final", "00"),
        }
        for name, (side, field, value) in cases.items():
            with self.subTest(field=name):
                changed = receipt()
                if field == "restart_final":
                    changed[side][field]["content_blake2b512"] = value
                else:
                    changed[side][field] = value
                with self.assertRaises(RuntimeError):
                    gate.check_receipt(changed)
        changed = receipt()
        changed["hybrid"]["final"]["table_rows"] = [0] * len(TABLE_ROWS)
        with self.assertRaisesRegex(RuntimeError, "table population"):
            gate.check_receipt(changed)

    def test_unverified_or_empty_population_is_refused(self):
        for mutate, message in (
                (lambda value: value["hybrid"]["final"].update(full_values=False), "full values"),
                (lambda value: value["hybrid"]["final"].update(verified_rows=0), "empty"),
                (lambda value: value.update(absent_keys=0), "absent_keys"),
                (lambda value: value.update(repetitions=0), "repetition")):
            with self.subTest(message=message):
                changed = receipt()
                mutate(changed)
                with self.assertRaisesRegex(RuntimeError, message):
                    gate.check_receipt(changed)

    def test_engine_counters_go_through_the_engine_scoped_audit(self):
        changed = receipt()
        changed["hybrid"]["counts"]["checkpoints"] = 4
        with self.assertRaisesRegex(ValueError, "checkpoints for hybrid"):
            gate.check_receipt(changed)
        changed = receipt()
        changed["native"]["counts"]["overlay_demands"] = 231
        with self.assertRaisesRegex(ValueError, "overlay_demands for native"):
            gate.check_receipt(changed)


if __name__ == "__main__":
    unittest.main()
