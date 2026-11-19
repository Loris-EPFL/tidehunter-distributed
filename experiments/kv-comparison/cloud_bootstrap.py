#!/usr/bin/env python3
"""Cache monitoring containers pinned by the sealed guest source."""
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
import time


def monitor_image_plan(source, index):
    """Use only the sealed lock, and cache collector images only on its host."""
    if type(index) is not int or index not in range(6):
        raise ValueError("exact admitted bootstrap launch index required")
    lock = Path(source) / "experiments/kv-comparison/observability/images.lock.json"
    pins = json.loads(lock.read_text())["images"]
    names = ["library/python", "prom/node-exporter"]
    if index == 5:
        names += ["prom/prometheus", "grafana/grafana"]
    result = {name: pins[name]["image"] for name in names}
    if any(not re.fullmatch(re.escape(name) + r":[A-Za-z0-9_.-]+@sha256:[a-f0-9]{64}", image)
           for name, image in result.items()):
        raise ValueError("monitoring images must use their sealed digest identities")
    return result


def cache_monitor_images(source, index, deadline, *, pull=True):
    started = time.monotonic()
    stop = min(started + 180, started + (deadline - datetime.now(timezone.utc)).total_seconds() - 30)
    evidence = {"complete": True, "images": {}, "gaps": []}
    try:
        pins = monitor_image_plan(source, index)
    except Exception as exc:
        return {"complete": False, "images": {}, "gaps": [type(exc).__name__ + ": invalid sealed monitoring lock"],
                "elapsed_seconds": time.monotonic() - started}
    for name, image in pins.items():
        try:
            remaining = stop - time.monotonic()
            if remaining < 1:
                raise TimeoutError("monitor image cache deadline")
            if pull:
                subprocess.run(["docker", "pull", image], check=True, capture_output=True, text=True,
                               timeout=min(60, remaining))
            remaining = stop - time.monotonic()
            if remaining < 1:
                raise TimeoutError("monitor image inspection deadline")
            observed = json.loads(subprocess.check_output(["docker", "image", "inspect", image],
                text=True, timeout=min(10, remaining)))
            if len(observed) != 1 or not observed[0].get("Id"):
                raise ValueError("pinned image did not resolve to one local identity")
            evidence["images"][name] = {"image": image, "id": observed[0]["Id"],
                                       "repo_digests": observed[0].get("RepoDigests")}
        except Exception as exc:
            evidence["complete"] = False
            evidence["gaps"].append(name + ": " + type(exc).__name__)
    evidence["elapsed_seconds"] = time.monotonic() - started
    return evidence
