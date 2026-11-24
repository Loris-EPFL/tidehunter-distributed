#!/usr/bin/env python3
"""Run the CI lint profile at every commit of a PR stack.

Usage: python3 -B .github/scripts/lint_stack.py --base <commit> --tip <commit>

A stacked PR merges one commit at a time, so a lint failure at any tip breaks
the series. This runner checks out each first-parent commit between the base
(exclusive) and the tip (inclusive) in one temporary worktree. Commits that a
merge brings in through its other parents never become a branch tip, so they
are not checked; an upstream merge is checked at the merge commit. It runs the same
`checks.py lint` profile that the Lint job runs. The worktree path does not
change, so Cargo reuses its artifacts between tips. The run stops at the
first failing tip and prints that commit.

The repository stays untouched. The runner creates and removes its own
worktree. Set CARGO_TARGET_DIR to reuse a private build directory; otherwise
the target directory is removed with the worktree.
"""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]


def git(root, *args):
    return subprocess.run(["git", *args], cwd=root, text=True, capture_output=True)


def commits(root, base, tip):
    """Return the stack commits, oldest first. Fail when base is not an ancestor."""
    ancestor = git(root, "merge-base", "--is-ancestor", base, tip)
    if ancestor.returncode != 0:
        raise RuntimeError(f"base {base} is not an ancestor of tip {tip}: "
                           + (ancestor.stderr.strip() or f"git exit {ancestor.returncode}"))
    listed = git(root, "rev-list", "--reverse", "--first-parent", f"{base}..{tip}")
    if listed.returncode != 0:
        raise RuntimeError(listed.stderr.strip() or f"git rev-list failed: {listed.returncode}")
    return listed.stdout.split()


def subject(root, commit):
    shown = git(root, "show", "-s", "--format=%s", commit)
    return shown.stdout.strip() if shown.returncode == 0 else commit


def worktree(root, directory, commit):
    added = git(root, "worktree", "add", "--detach", str(directory), commit)
    if added.returncode != 0:
        raise RuntimeError(added.stderr.strip() or f"git worktree add failed: {added.returncode}")


def remove_worktree(root, directory):
    removed = git(root, "worktree", "remove", "--force", str(directory))
    if removed.returncode != 0:
        print(f"lint-stack: could not remove worktree {directory}: {removed.stderr.strip()}",
              file=sys.stderr, flush=True)


def checkout(tree, commit):
    subprocess.run(["git", "checkout", "--detach", "--quiet", commit],
                   cwd=tree).check_returncode()


def run_profile(tree, env):
    subprocess.run([sys.executable, "-B", str(tree / ".github/scripts/checks.py"), "lint"],
                   cwd=tree, env=env).check_returncode()


def run(args):
    tips = commits(ROOT, args.base, args.tip)
    if not tips:
        raise RuntimeError(f"no commits between {args.base} and {args.tip}")
    temporary = Path(tempfile.mkdtemp(prefix="tidehunter-lint-stack-"))
    tree = temporary / "tree"
    env = os.environ.copy()
    env.setdefault("CARGO_TARGET_DIR", str(temporary / "target"))
    created = False
    try:
        worktree(ROOT, tree, tips[0])
        created = True
        for commit in tips:
            checkout(tree, commit)
            print(f"\nlint-stack: {commit} {subject(ROOT, commit)}", flush=True)
            try:
                run_profile(tree, env)
            except subprocess.CalledProcessError:
                print(f"lint-stack: FAILED at {commit} {subject(ROOT, commit)}",
                      file=sys.stderr, flush=True)
                return 1
        print(f"lint-stack: {len(tips)} tips passed the lint profile", flush=True)
        return 0
    finally:
        if args.keep:
            print(f"lint-stack: kept worktree {tree}", flush=True)
        else:
            if created:
                remove_worktree(ROOT, tree)
            shutil.rmtree(temporary, ignore_errors=True)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base", required=True,
                        help="commit before the first tip (exclusive)")
    parser.add_argument("--tip", default="HEAD",
                        help="last commit of the stack (inclusive; default HEAD)")
    parser.add_argument("--keep", action="store_true",
                        help="keep the worktree and target directory for diagnosis")
    args = parser.parse_args(argv)
    try:
        return run(args)
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"lint-stack: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
