#!/usr/bin/env python3
"""Failure-safe cleanup of only owner receipts under one explicit result root."""
import argparse
from pathlib import Path
import sys

import local


def cleanup(root):
    errors = []
    if not root.exists():
        return
    for receipt in sorted(root.rglob("fdb-extract.json")):
        try:
            local.cleanup_extraction(receipt)
        except Exception as exc:
            errors.append({"state": str(receipt), "error": str(exc)})
    for receipt in sorted(root.rglob("owner.json")):
        # Only canonical server receipts, not exported copies inside deployment/failure.
        if receipt.parent.name != "server":
            continue
        try:
            state = local.load(receipt.parent)
            try:
                local.export(state, receipt.parent / "final-export")
            finally:
                local.stop(state)
        except Exception as exc:
            errors.append({"state": str(receipt), "error": str(exc)})
    local.dump(root / "cleanup-receipt.json", {"errors": errors, "complete": not errors})
    if errors:
        raise RuntimeError(f"exact-owned cleanup failed for {len(errors)} deployments; see cleanup-receipt.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    try:
        cleanup(args.root.resolve())
    except (RuntimeError, ValueError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
