#!/usr/bin/env python3
"""Run one shard of a cargo test target by exact test name.

Each shard lists the live test names, keeps every ``--shards``-th name and runs
only those names with ``--exact``. The assignment follows the live list, so a
new test needs no workflow change and no test can be dropped: the run fails
when a selected name reports no outcome. A shard with no assigned name runs
nothing and never falls back to the whole suite.
"""

import argparse
import collections
import os
import re
import subprocess
import sys

# libtest reports one summary line per test binary and one for doctests.
RESULT = re.compile(
    r"^test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; "
    r"\d+ measured; \d+ filtered out; finished in ")
# One report header per test that ran: `test NAME ... STATUS`, or with a test
# mode such as `test NAME - should panic ... ` where the status follows on a
# later line after the panic message. The exit code decides pass or fail. A test
# that re-executes the test binary (for example a cross-process lock test) adds
# the child's headers and summary, so shards compare names, not summary totals.
OUTCOME = re.compile(r"^test (\S+)(?: - [a-z_ ]+)? \.\.\.(?: |$)")


def parse_listing(text):
    """Return the Counter of test names in `--list --format terse` output."""
    return collections.Counter(
        line.removesuffix(": test") for line in text.splitlines()
        if line.endswith(": test"))


def list_tests(prefix, target, env, cwd=None):
    """Return the Counter of test names of one cargo test invocation."""
    command = [*prefix, *target, "--", "--list", "--format", "terse"]
    print("+ " + " ".join(command), flush=True)
    result = subprocess.run(command, cwd=cwd, env=env, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print(result.stdout, end="", flush=True)
    if result.returncode != 0:
        raise RuntimeError(f"test listing failed with exit code {result.returncode}")
    return parse_listing(result.stdout)


def select(names, index, shards):
    """Return the names of shard `index`: position modulo `shards` in sort order."""
    return [name for position, name in enumerate(sorted(names)) if position % shards == index]


def split(listing):
    """Return the libtest name Counter and the doctest run count.

    A libtest name holds no space; a rustdoc doctest name carries its source
    path and a line number. Rustdoc does not accept an exact name filter, so
    doctests stay out of the shards and shard 0 runs them unsharded.
    """
    unit = collections.Counter({name: count for name, count in listing.items() if " " not in name})
    return unit, sum(count for name, count in listing.items() if " " in name)


def summarized(names):
    """Keep a long exact-name argument list out of one log line."""
    if len(names) <= 4:
        return " ".join(names)
    return " ".join([*names[:4], f"... {len(names)} names"])


def stream(command, env, cwd):
    """Run `command`, echo its output; return the exit code, summary count and outcomes."""
    process = subprocess.Popen(command, cwd=cwd, env=env, text=True,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    executed = 0
    outcomes = collections.Counter()
    for line in process.stdout:
        print(line, end="", flush=True)
        match = RESULT.match(line)
        if match:
            executed += sum(int(count) for count in match.groups())
        outcome = OUTCOME.match(line.rstrip("\n"))
        if outcome:
            outcomes[outcome.group(1)] += 1
    return process.wait(), executed, outcomes


def run(prefix, target, names, env, cwd=None):
    """Run only `names`; return the exit code, summary count and outcomes."""
    command = [*prefix, *target, "--", "--exact", *names]
    print("+ " + " ".join([*prefix, *target, "--", "--exact", summarized(names)]), flush=True)
    return stream(command, env, cwd)


def run_docs(prefix, target, env, cwd=None):
    """Run every doctest of the target; return the exit code, summary count and outcomes."""
    command = [*prefix, *target, "--doc"]
    print("+ " + " ".join(command), flush=True)
    return stream(command, env, cwd)


def check(name, expected, code, executed, _outcomes):
    """Fail when a doctest run ran fewer tests than it listed."""
    if executed != expected:
        raise RuntimeError(f"{name} ran {executed} tests, expected {expected}")
    if code != 0:
        print(f"{name} failed with exit code {code}", file=sys.stderr)
    return code


def check_names(name, selected, code, _executed, outcomes):
    """Fail when a selected name reported fewer outcomes than it was listed.

    Extra outcomes are allowed: they come from child test processes.
    """
    missing = sorted(test for test, count in selected.items() if outcomes[test] < count)
    if missing:
        raise RuntimeError(f"{name} has no outcome for {len(missing)} selected tests: "
                           f"{summarized(missing)}")
    if code != 0:
        print(f"{name} failed with exit code {code}", file=sys.stderr)
    return code


def run_shard(prefix, target, index, shards, env, cwd=None):
    """Run one shard of `cargo test target` and check that every name ran."""
    tests = list_tests(prefix, target, env, cwd)
    if not tests:
        raise RuntimeError(f"no test was listed for {target}")
    unit, docs = split(tests)
    names = select(unit, index, shards)
    selected = collections.Counter({test: unit[test] for test in names})
    print(f"shard {index}/{shards} selects {len(names)} names of {len(unit)} "
          f"({sum(selected.values())} runs; shard 0 also runs {docs} doctests)", flush=True)
    if names:
        code = check_names(f"shard {index}/{shards}", selected, *run(prefix, target, names, env, cwd))
    else:
        # Never run an unfiltered suite here: the other shards cover these tests.
        print("shard has no assigned test", flush=True)
        code = 0
    if docs and index == 0:
        docs_code = check("doctests", docs, *run_docs(prefix, target, env, cwd))
        code = code or docs_code
    return code


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     epilog="Pass the cargo test arguments after --.")
    parser.add_argument("--index", type=int, required=True)
    parser.add_argument("--shards", type=int, required=True)
    arguments = sys.argv[1:]
    separator = arguments.index("--") if "--" in arguments else None
    if separator is None:
        parser.print_usage(sys.stderr)
        print("error: the cargo test arguments must follow --", file=sys.stderr)
        return 2
    args = parser.parse_args(arguments[:separator])
    target = arguments[separator + 1:]
    if not 0 <= args.index < args.shards:
        print("error: --index must be in [0, --shards)", file=sys.stderr)
        return 2
    if not target:
        print("error: no cargo test arguments after --", file=sys.stderr)
        return 2
    env = os.environ.copy()
    env.setdefault("CARGO_TERM_COLOR", "always" if sys.stdout.isatty() else "never")
    try:
        return run_shard(["cargo", "test"], target, args.index, args.shards, env)
    except (OSError, RuntimeError) as error:
        print(f"shard_tests: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
