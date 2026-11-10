#!/usr/bin/env python3
"""Join a local kv-research result's bounded write timeline to its raw samples."""
import argparse
import json
from pathlib import Path

import analyze


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result", type=Path, help="kv-research result directory")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    rows = list(analyze.read_samples(args.result / "samples.csv"))
    evidence = json.loads((args.result / "write-timeline.json").read_text())
    summary = analyze.summarize_write_timeline(rows, evidence)
    args.output.write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
