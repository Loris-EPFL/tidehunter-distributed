#!/usr/bin/env python3
"""Gate the native-vs-hybrid differential replay oracle; one test, exactly once, no cluster."""
import json
import os
import re

from checks import cargo
import mainnet_replay_ci

SELECTION = "backends::tidehunter::replay::tests::native_oracle_tests"
RECEIPT_PREFIX = "native-hybrid-oracle-receipt "
RESULT = re.compile(r"test result: ok\. (\d+) passed; 0 failed; 0 ignored;")


def require_single_pass(output):
    """The oracle is one test; a filtered suite that ran zero or several tests proves nothing."""
    populations = RESULT.findall(output)
    if len(populations) != 1 or int(populations[0]) != 1:
        raise RuntimeError(
            f"Oracle selection {SELECTION} did not execute exactly one passing test: {populations}")
    return int(populations[0])


def receipt(output):
    # libtest writes the test name and "... " without a newline before a
    # --nocapture test's stdout, so the receipt can share that line; take it
    # from the prefix wherever it starts.
    lines = [
        line[line.index(RECEIPT_PREFIX):]
        for line in output.splitlines()
        if RECEIPT_PREFIX in line
    ]
    if len(lines) != 1:
        raise RuntimeError(f"Oracle must print exactly one receipt; found {len(lines)}")
    return json.loads(lines[0][len(RECEIPT_PREFIX):])


def check_receipt(value):
    """Compare the two engines from the oracle's own measured receipt."""
    repetitions = value.get("repetitions")
    if not isinstance(repetitions, int) or repetitions <= 0:
        raise RuntimeError("receipt has no positive repetition count")
    for field in ("keys", "absent_keys"):
        if not isinstance(value.get(field), int) or value[field] <= 0:
            raise RuntimeError(f"receipt did not read {field}")
    engines = {}
    for name in ("native", "hybrid"):
        engine = value.get(name)
        if not isinstance(engine, dict):
            raise RuntimeError(f"receipt has no {name} engine section")
        engines[name] = engine
        if engine.get("read_digest") != engine.get("restart_read_digest"):
            raise RuntimeError(f"{name} reads changed across a restart")
        final = engine.get("final") or {}
        restart = engine.get("restart_final") or {}
        if final.get("content_blake2b512") != restart.get("content_blake2b512"):
            raise RuntimeError(f"{name} final state changed across a restart")
        if final.get("full_values") is not True:
            raise RuntimeError(f"{name} final verification did not compare full values")
        if not final.get("verified_rows"):
            raise RuntimeError(f"{name} final verification is empty")
        mainnet_replay_ci.audit_counts(engine.get("counts") or {}, repetitions, name)
    native, hybrid = engines["native"], engines["hybrid"]
    for field in ("read_digest", "restart_read_digest"):
        if native[field] != hybrid[field]:
            raise RuntimeError(f"hybrid and native differ on {field}")
    for field in ("final", "restart_final"):
        if native[field]["content_blake2b512"] != hybrid[field]["content_blake2b512"]:
            raise RuntimeError(f"hybrid and native differ on {field} content")
        if native[field]["table_rows"] != hybrid[field]["table_rows"]:
            raise RuntimeError(f"hybrid and native differ on {field} table population")
    return value


def main():
    environment = os.environ.copy()
    environment["RUST_TEST_THREADS"] = "1"
    output = cargo(environment, "test", "-p", "kv-research", "--features", "test-utils",
                   "--lib", SELECTION, "--", "--nocapture", capture=True)
    require_single_pass(output)
    value = check_receipt(receipt(output))
    print(json.dumps({
        "status": "completed", "selection": SELECTION,
        "repetitions": value["repetitions"], "keys": value["keys"],
        "absent_keys": value["absent_keys"],
        "native_read_digest": value["native"]["read_digest"],
        "hybrid_read_digest": value["hybrid"]["read_digest"],
        "final_content_blake2b512": value["native"]["final"]["content_blake2b512"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
