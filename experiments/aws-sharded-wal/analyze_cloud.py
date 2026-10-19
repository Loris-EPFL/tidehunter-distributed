#!/usr/bin/env python3
"""Analyze extracted guest.py results locally; no AWS SDK, network, or execution.

Usage: analyze_cloud.py RESULTS [--output DIRECTORY] [--allow-partial]
Default matrix: 96 warm + 8 storage + 2 capacity API cases, 270 node archives.
The overlap profile expects 96 warm + 12 storage + 3 chunk controls, 285 archives.
The read-path profile expects 24 warm + six cold + two diagnostic cases, 80 archives.
The efa-cache profile adds 48 cache-enabled API cases to the 68-case EFA matrix.
Missing/invalid evidence is reported, never converted to zero measurements.
"""
import argparse
import csv
from datetime import datetime
import hashlib
import json
import math
import re
from pathlib import Path, PurePosixPath
import statistics
import tarfile
from collections import Counter, defaultdict

import efa_progress
import checkpoint_async
import cluster_ram
import scalar_transport
import transport_floor
import stage_timing


BACKENDS = {"tcp": "experimental_tcp_authority", "native-sync": "native_value_wal_barrier"}
META = {"backend", "durability", "value_bytes", "batch_size", "window", "repetition", "phase"}
READ_RECHECK_TIMERS = ("server_response_recheck_wait_ns", "server_response_recheck_ns")
READ_RECHECK_SAMPLES = "server_response_recheck_samples"


def expected_cases(profile="original", checkpoint_scope="full"):
    if profile == 'stage-timing':
        return stage_timing.expected_cases(checkpoint_scope)
    if profile == 'transport-floor':
        return transport_floor.expected_cases(checkpoint_scope)
    if profile == 'scalar-transport':
        return scalar_transport.expected_cases(checkpoint_scope)
    if profile == 'cluster-ram':
        return cluster_ram.expected_cases(checkpoint_scope)
    if profile == 'checkpoint-async':
        return checkpoint_async.expected_cases(checkpoint_scope)
    if profile == 'efa-progress':
        return efa_progress.expected_cases()
    result = {}
    if profile in ['efa', 'efa-cache']:
        shapes = ((1024, 1, 1), (1024, 32, 32), (8192, 1, 1), (8192, 32, 32))
        groups = [('storage', 'baseline', shapes, 1), ('storage', 'current', shapes, 3),
                  ('storage-efa', 'current', shapes, 3),
                  ('cold', 'current', ((8192, 32, 32),), 2),
                  ('cold-efa', 'current', ((8192, 32, 32),), 2),
                  ('diagnostic', 'current', ((8192, 32, 32),), 1),
                  ('diagnostic-efa', 'current', ((8192, 32, 32),), 1)]
        if profile == 'efa-cache':
            groups += [('cache', 'current', shapes, 3), ('cache-efa', 'current', shapes, 3)]
        for family, build, shapes, repeats in groups:
            for v, b, w in shapes:
                for rep in range(repeats):
                    case = f'{family}-{build}-v{v}-b{b}-w{w}-r{rep}'
                    for mode in ('tcp', 'native-sync'):
                        result[f'{case}-{mode}'] = (family, build, v, b, w, rep, mode, case)
        return result
    if profile == 'read-path':
        for family, builds, shapes, repeats in [
            ('storage', ('baseline', 'current'), ((1024, 1, 1), (8192, 32, 32)), 3),
            ('cold', ('current',), ((8192, 32, 32),), 3),
            ('diagnostic', ('current',), ((8192, 32, 32),), 1),
        ]:
            for build in builds:
                for v, b, w in shapes:
                    for rep in range(repeats):
                        case = f'{family}-{build}-v{v}-b{b}-w{w}-r{rep}'
                        for mode in ('tcp', 'native-sync'):
                            result[f'{case}-{mode}'] = (family, build, v, b, w, rep, mode, case)
        return result
    for family, shapes, builds, modes in [
        ("warm", [(v, b, w) for v in (1024, 8192) for b in (1, 32) for w in (1, 8, 32)],
         ("baseline", "current"), ("tcp", "native-sync")),
        ("storage", [(8192, 32, 32)], ("baseline", "current"), ("tcp", "native-sync")),
        ("capacity4m", [(8192, 32, 32)], ("current",), ("tcp",)),
    ]:
        for v, b, w in shapes:
            for build in builds:
                for rep in range(2):
                    case = f"{family}-{build}-v{v}-b{b}-w{w}-r{rep}"
                    for mode in modes:
                        result[f"{case}-{mode}"] = (family, build, v, b, w, rep, mode, case)
    if profile == "overlap":
        result = {name: info for name, info in result.items() if info[0] != "capacity4m"}
        for build in ("baseline", "current"):
            case = f"storage-{build}-v8192-b32-w32-r2"
            for mode in ("tcp", "native-sync"):
                result[f"{case}-{mode}"] = ("storage", build, 8192, 32, 32, 2, mode, case)
        for rep in range(3):
            case = f"chunk32-current-v8192-b32-w32-r{rep}"
            result[f"{case}-tcp"] = ("chunk32", "current", 8192, 32, 32, rep, "tcp", case)
    return result


def number(value):
    value = float(value)
    if not math.isfinite(value) or value < 0:
        raise ValueError("negative/nonfinite measurement")
    return value


def read_json(path):
    value = json.loads(Path(path).read_text())
    if not isinstance(value, dict):
        raise ValueError("expected a JSON object")
    return value


def is_sha256(value):
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def validate_build_pair(root, receipts):
    """Cross-check recorded build/capability/smoke evidence, without executing binaries.

    Config hashes attest the guest's six-file smoke; those private configs are not
    archived here, so this does not independently rehash their contents.
    """
    errors, roots = {}, {}
    labels = ("baseline", "current")
    for label in labels:
        receipt = receipts.get("cloud-build-" + label, {})
        if receipt.get("label") != label or any(not is_sha256(receipt.get(k)) for k in
                ("binary_sha256", "source_tree_sha256", "source_manifest_sha256")):
            errors["cloud-build-" + label] = "missing/invalid labeled source and cloud-binary hash receipt"
        try:
            target = PurePosixPath(receipt["target_directory"])
            if (not target.is_absolute() or ".." in target.parts or
                    target.name != label or target.parent.name != "build-target"):
                raise ValueError("target must be an absolute isolated build-target/<label> path")
            roots[label] = target.parent.parent
            command = receipt["command"]
            if not isinstance(command, list) or any(not isinstance(arg, str) for arg in command):
                raise ValueError("invalid build command")
            manifest = str(roots[label] / "build-source" / label / "Cargo.toml")
            if command.count("--manifest-path") != 1 or command[command.index("--manifest-path") + 1] != manifest:
                raise ValueError("build manifest does not match isolated source tree")
            recorded = read_json(root / f"build-{label}.command.json")
            if recorded.get("exit_code") != 0 or recorded.get("command") != command:
                raise ValueError("successful build command differs from receipt")
        except (OSError, ValueError, KeyError, TypeError, IndexError) as error:
            errors[f"cloud-build-{label}-isolation"] = str(error)
    if len(roots) == 2 and roots["baseline"] != roots["current"]:
        errors["cloud-build-targets"] = "builds must share one campaign root with distinct labeled targets"
    builds = [receipts.get("cloud-build-" + label, {}) for label in labels]
    for field in ("binary_sha256", "source_tree_sha256"):
        if builds[0].get(field) == builds[1].get(field):
            errors["cloud-build-distinct-" + field] = f"comparison requires distinct {field}; recorded values are identical"
    pair = receipts.get("cloud-build-pair-verified", {})
    if (pair.get("binary_sha256") != {label: receipt.get("binary_sha256") for label, receipt in zip(labels, builds)}
            or any(pair.get(flag) is not True for flag in
                   ("distinct_sources", "distinct_binaries", "capability_checks"))):
        errors["cloud-build-pair-verified"] = "pair receipt must confirm both exact binary hashes, distinct sources/binaries and capabilities"
    config_names = {"remote.yaml"} | {f"node-{node}/launch.yaml" for node in range(5)}
    if (root / 'campaign-config.json').is_file() and read_json(root / 'campaign-config.json').get('campaign') in ('transport-floor', 'stage-timing'):
        config_names.add('shape.yaml')
    hashes = pair.get("prepared_configuration_sha256")
    if not isinstance(hashes, dict) or set(hashes) != config_names or any(not is_sha256(v) for v in hashes.values()):
        errors["cloud-build-prepared-configurations"] = "pair receipt must contain valid hashes for remote.yaml and all five node launch configurations"
    for label in labels:
        try:
            recorded = read_json(root / f"capability-{label}.command.json")
            binary = str(roots[label] / "cloud-bin" / label / "sharded_wal_bench")
            if recorded.get("exit_code") != 0 or recorded.get("command") != [binary, "--help"]:
                raise ValueError("missing successful --help for the recorded build")
            help_text = (root / f"capability-{label}.stdout.log").read_text()
            if "--remote-config" not in help_text or (label == "current" and "--prepare-external-case" not in help_text):
                raise ValueError("required cloud benchmark capabilities absent from help output")
        except (OSError, ValueError, KeyError, TypeError) as error:
            errors[f"cloud-build-capability-{label}"] = str(error)
    try:
        command = pair["prepare_command"]
        binary = str(roots["current"] / "cloud-bin/current/sharded_wal_bench")
        current_preflight = (root / "cloud-build-preparation-check.command.json").is_file()
        case_root = str(roots["current"] / ("build-preflight/external-case" if current_preflight else "build-smoke/external-case"))
        if (not isinstance(command, list) or not command or command[0] != binary or
                len(command) % 2 != 1 or any(not isinstance(arg, str) for arg in command)):
            raise ValueError("invalid preparation command for current binary")
        options = dict(zip(command[1::2], command[2::2]))
        if len(options) != len(command) // 2:
            raise ValueError("duplicate preparation options")
        expected = {"--prepare-external-case": case_root, "--guest-case-root": case_root,
                    "--values": "1024", "--batch-sizes": "1", "--windows": "1", "--batches": "128",
                    "--mixed": "32", "--reads": "512", "--repetitions": "1"}
        endpoints = options.get("--remote-endpoints", "").split(",")
        if (any(options.get(k) != v for k, v in expected.items()) or
                set(options) != set(expected) | {"--remote-endpoints"} or
                len(endpoints) != 5 or len(set(endpoints)) != 5 or
                any(not endpoint.endswith(":7700") or len(endpoint) <= 5 for endpoint in endpoints)):
            raise ValueError("preparation did not use the expected six-config/five-endpoint smoke")
        recorded = read_json(root / ("cloud-build-preparation-check.command.json" if current_preflight else "cloud-build-preparation-smoke.command.json"))
        if recorded.get("exit_code") != 0 or recorded.get("command") != command:
            raise ValueError("successful preparation command does not match pair receipt")
    except (OSError, ValueError, KeyError, TypeError) as error:
        errors["cloud-build-preparation-smoke"] = str(error)
    return errors


def read_case(root, name, info, campaign_profile="original"):
    path = root / name
    if not (path / "COMPLETE").is_file():
        raise ValueError("missing COMPLETE")
    family, build, v, b, w, rep, mode, _ = info
    if campaign_profile in ('transport-floor', 'stage-timing'):
        profile = stage_timing if campaign_profile == 'stage-timing' else transport_floor
        cfg = read_json(root / 'campaign-config.json')
        scope = cfg.get('checkpoint_scope', 'full')
        if profile.expected_cases(scope).get(name) != info:
            raise ValueError('unplanned source-bound case identity')
        spec = next(c for c in profile.plan(scope) if c['case_id'] == info[-1])
        return profile.read_cloud_case(root, name, spec, mode)
    if campaign_profile == 'scalar-transport':
        cfg = read_json(root / 'campaign-config.json')
        scope = cfg.get('checkpoint_scope', 'full')
        if scalar_transport.expected_cases(scope).get(name) != info:
            raise ValueError('unplanned scalar transport case identity')
        spec = next(c for c in scalar_transport.plan(scope) if c['case_id'] == info[-1])
        return scalar_transport.read_cloud_case(root, name, spec, mode)
    if campaign_profile == 'cluster-ram':
        spec = next(c for c in cluster_ram.plan() if c['case_id'] == info[-1])
        if mode == 'recovery':
            return read_recovery_case(root, name, spec, cluster_ram)
        return cluster_ram.read_cloud_case(root, name, spec, mode)
    if campaign_profile == 'checkpoint-async' and mode == 'recovery':
        return read_recovery_case(root, name, next(c for c in checkpoint_async.recovery_cases() if c['case_id'] == info[-1]))
    progress_spec = None
    if campaign_profile in ('efa-progress', 'checkpoint-async'):
        profile = checkpoint_async if campaign_profile == 'checkpoint-async' else efa_progress
        progress_spec = next(c for c in profile.plan() if c['case_id'] == info[-1])
        family = ('diagnostic' if progress_spec['diagnostics'] else
                  'cold' if progress_spec['cold'] else 'cache' if progress_spec['cache'] else 'storage')
        if progress_spec['transport'] == 'efa':
            family += '-efa'
    uses_efa = family.endswith("-efa")
    family = family.removesuffix("-efa")
    backend = "experimental_efa_authority" if uses_efa and mode == "tcp" else BACKENDS[mode]
    metrics, summaries = {}, {}
    # This exact archived build used Kind::Tcp for two cold diagnostic labels.
    # Summary/sample labels, explicit transport commands and NIC evidence remain
    # mandatory. Never accept that typo from a different source or other CSVs.
    legacy_label = (uses_efa and mode == 'tcp' and family in ('cold', 'diagnostic')
                    and read_json(root / 'cloud-build-current.json').get('source_tree_sha256')
                    == 'f11eb057c71fb7ec149ce2827757a7041b68e47da6f6a6318ad2d7c3bacd6398')

    def dimensions(row, legacy_diagnostic=False):
        label = row['backend']
        if legacy_diagnostic and legacy_label and label == BACKENDS['tcp']:
            label = backend
        if (label, int(row["value_bytes"]), int(row["batch_size"]),
                int(row["window"]), int(row["repetition"])) != (backend, v, b, w, 0):
            raise ValueError("CSV dimensions differ from case name")

    with (path / "summary.csv").open() as stream:
        for row in csv.DictReader(stream):
            dimensions(row)
            phase = row["phase"]
            if phase in summaries:
                raise ValueError("duplicate summary phase")
            summaries[phase] = row
            for key, value in row.items():
                if key not in META:
                    metrics[("summary", phase, key)] = number(value)
    required = {"open", "load_write_window", "point_get", "point_get_many", "point_get_missing",
                "exists", "exists_missing", "mixed_get", "mixed_write_window", "checkpoint_capture",
                "checkpoint_intervening_write", "checkpoint_get", "delete_window", "full_scan",
                "maintenance_flush_index", "post_flush_get", "shutdown", "reopen", "reopen_get_audit"}
    if not required.issubset(summaries):
        raise ValueError("missing required benchmark phases")
    load, mixed = (320, 64) if family in ("storage", "chunk32", "cold", "diagnostic", "cache") else (128, 32)
    reads = 4096 if campaign_profile in ['read-path', 'efa', 'efa-cache'] else 512
    if progress_spec:
        load, mixed, reads = progress_spec['batches'], progress_spec['mixed'], progress_spec['reads']
    for phase, count in (("load_write_window", load * b), ("mixed_write_window", mixed * b),
                         ("mixed_get", mixed * b), ("point_get", reads), ("point_get_many", reads),
                         ("reopen_get_audit", load * b)):
        if int(summaries[phase]["logical_operations"]) != count:
            raise ValueError(f"workload operation count differs: {phase}")
    # Read raw samples incrementally. A COMPLETE marker alone cannot establish
    # that an archive contains the complete CSVs belonging to this case.
    samples = defaultdict(lambda: [0, 0, 0])
    raw_latencies = defaultdict(list)
    with (path / "samples.csv").open() as stream:
        for row in csv.DictReader(stream):
            dimensions(row)
            counts = samples[row["phase"]]
            counts[0] += 1
            counts[1] += int(row["logical_operations"])
            counts[2] += int(row["latency_ns"])
            if campaign_profile == 'checkpoint-async':
                if int(row['sample']) != len(raw_latencies[row['phase']]) or int(row['latency_ns']) < 0:
                    raise ValueError('invalid raw sample ordering/latency')
                raw_latencies[row['phase']].append(int(row['latency_ns']))
    if set(samples) != set(summaries):
        raise ValueError("sample and summary phases differ")
    for phase, actual in samples.items():
        expected = [int(summaries[phase][k]) for k in ("samples", "logical_operations", "measured_api_ns")]
        if actual != expected:
            raise ValueError(f"sample/summary counts or API-time sum differ: {phase}")
        if campaign_profile == 'checkpoint-async':
            values = sorted(raw_latencies[phase])
            for pct in (50, 95, 99):
                if int(summaries[phase][f'p{pct}_ns']) != values[max(0, (len(values) * pct + 99) // 100 - 1)]:
                    raise ValueError('reported percentile differs from raw samples')
            rate = actual[1] * 1e9 / max(1, int(summaries[phase]['phase_wall_ns']))
            if not math.isclose(float(summaries[phase]['instrumented_phase_ops_per_sec']), rate, rel_tol=1e-8, abs_tol=0.001):
                raise ValueError('reported throughput differs from raw operation count and wall time')
    with (path / "stages.csv").open() as stream:
        for row in csv.DictReader(stream):
            dimensions(row)
            key = ("stage", row["phase"], row["counter"])
            if key in metrics:
                raise ValueError("duplicate stage counter")
            metrics[key] = number(row["delta"])
    profile = path / "remote_read_profiles.csv"
    needs_profile = build == "current" and mode == "tcp"
    if needs_profile and not profile.is_file():
        raise ValueError("missing current TCP diagnostic profile")
    if profile.exists():
        grouped = defaultdict(list)
        with profile.open() as stream:
            for row in csv.DictReader(stream):
                dimensions(row, legacy_diagnostic=row['state'] == 'cache-evicted')
                for key, value in row.items():
                    if key.endswith("_ns") or key == READ_RECHECK_SAMPLES:
                        grouped[(row["state"], key)].append(number(value))
        if needs_profile and not grouped:
            raise ValueError("empty current TCP diagnostic profile")
        for (state, key), values in grouped.items():
            for suffix, value in (("p50", statistics.median(values)), ("mean", statistics.mean(values)),
                                  ("sum", sum(values)), ("n", len(values))):
                metrics[("profile", state, f"{key}_{suffix}")] = value
    for filename, kind in [('native_read_profiles.csv', 'native_profile'),
                           ('exchange_profiles.csv', 'exchange_profile')]:
        file = path / filename
        if file.exists():
            grouped = defaultdict(list)
            with file.open() as stream:
                for row in csv.DictReader(stream):
                    dimensions(row)
                    for key, value in row.items():
                        if key.endswith('_ns'):
                            grouped[(row['state'], key)].append(number(value))
                        elif key == 'value_cache_hit':
                            if value not in ['true', 'false']:
                                raise ValueError('invalid native cache-hit evidence')
                            grouped[(row['state'], key)].append(int(value == 'true'))
            for (state, key), values in grouped.items():
                for suffix, value in [('p50', statistics.median(values)), ('mean', statistics.mean(values)),
                                      ('sum', sum(values)), ('n', len(values))]:
                    metrics[(kind, state, f'{key}_{suffix}')] = value
    process = read_json(root / f"{name}-process.command.json")
    if process.get("exit_code") != 0:
        raise ValueError("missing/nonzero controller benchmark exit")
    command = process["command"]
    options = dict(zip(command[1::2], command[2::2]))
    cache_bytes = str(8 * 1024 * 1024) if family == 'cache' else '0'
    if campaign_profile in ("overlap", "read-path", "efa", "efa-cache") and mode == "tcp":
        if campaign_profile in ['efa', 'efa-cache'] and options.get('--transport', 'tcp') != ('efa' if uses_efa else 'tcp'):
            raise ValueError('benchmark transport differs from expected case')
        if options.get("--tcp-read-cache-bytes") != cache_bytes:
            raise ValueError("frontend cache budget differs from selected case")
        if build == "current" and options.get("--tcp-preparation-chunk-batches") != ("32" if family == "chunk32" else "16"):
            raise ValueError("preparation chunk differs from the selected case")
        if cache_bytes == '0' and any(value != 0 for (kind, _, metric), value in metrics.items() if kind == "stage" and metric == "cache_hits"):
            raise ValueError("disabled cache recorded hits")
    if campaign_profile == 'efa-cache':
        if build == 'current':
            if options.get('--read-window') != '32':
                raise ValueError('concurrent-read window differs from selected case')
            if ('point_get_window' not in summaries
                    or int(summaries['point_get_window']['logical_operations']) != reads):
                raise ValueError('missing concurrent-read measurements')
        hot = build == 'current' and family in ['storage', 'cache']
        if options.get('--hot-read-pass', 'false') != str(hot).lower():
            raise ValueError('repeated-read flag differs from selected cache case')
        for phase in ['point_get_hot', 'point_get_many_hot']:
            if hot != (phase in summaries) or (hot and int(summaries[phase]['logical_operations']) != reads):
                raise ValueError('missing or unexpected repeated-read measurements')
        if mode == 'native-sync':
            if options.get('--native-read-cache-bytes', '0') != cache_bytes:
                raise ValueError('native cache budget differs from selected case')
            if cache_bytes == '0' and any(value != 0 for (kind, _, metric), value in metrics.items()
                                         if kind == 'native_profile' and metric == 'value_cache_hit_sum'):
                raise ValueError('disabled native cache recorded hits')
    if campaign_profile in ['read-path', 'efa', 'efa-cache', 'efa-progress', 'checkpoint-async']:
        cold = progress_spec['cold'] if progress_spec else family in ('cold', 'diagnostic')
        if (options.get('--cold-read-pass', 'false') != str(cold).lower()
                or options.get('--transport-diagnostics', 'false') != str(family == 'diagnostic').lower()
                or options.get('--reads') != str(reads)):
            raise ValueError('read-path command flags differ from selected case')
        if cold:
            expected_parts = set(range(5)) if mode == 'tcp' else {0}
            evidence = defaultdict(dict)
            with (path / 'read_io.csv').open() as stream:
                for row in csv.DictReader(stream):
                    dimensions(row, legacy_diagnostic=True)
                    phase, part = row['phase'], int(row['partition'])
                    if part in evidence[phase]:
                        raise ValueError('duplicate physical-read evidence partition')
                    evidence[phase][part] = number(row['physical_read_bytes'])
                    if progress_spec:
                        file_bytes = number(row['file_bytes'])
                        resident = number(row['resident_bytes_before'])
                        if file_bytes <= 0 or resident > file_bytes / 10 or number(row['resident_bytes_after']) > file_bytes:
                            raise ValueError('cold pass lacks actually evicted file residency')
                    for key in ('physical_read_bytes', 'major_faults', 'minor_faults',
                                'resident_bytes_before', 'resident_bytes_after', 'file_bytes'):
                        metrics[('physical_io', phase, f'node_{part}_{key}')] = number(row[key])
            read_phases = ['cache_evicted_get', 'cache_evicted_get_many']
            if '--read-window' in options or 'cache_evicted_get_window' in summaries:
                read_phases.append('cache_evicted_get_window')
            if set(evidence) != {*read_phases, 'cache_evicted_diagnostics'}:
                raise ValueError('missing cold-read evidence phase')
            for phase, parts in evidence.items():
                if set(parts) != expected_parts or sum(parts.values()) <= 0 or (progress_spec and min(parts.values()) <= 0):
                    raise ValueError('cold phase missing nodes or actual physical reads')
            for phase in read_phases:
                if int(summaries[phase]['logical_operations']) != reads:
                    raise ValueError('cold read count differs from campaign')
            for artifact_name in ['native_read_profiles.csv'] if mode == 'native-sync' else ['exchange_profiles.csv', 'node_profiles.yaml']:
                if not (path / artifact_name).is_file() or not (path / artifact_name).stat().st_size:
                    raise ValueError('missing read diagnostic artifact: ' + artifact_name)
    if progress_spec:
        metrics.update(validate_progress_case(root, name, progress_spec, mode, process, summaries, metrics))
    for key in ("elapsed_seconds", "peak_sampled_main_process_rss_kib"):
        if key in process:
            metrics[("process", "whole_process", key)] = number(process[key])
    return metrics


def validate_progress_case(root, name, spec, mode, process, summaries, metrics):
    checkpoint = 'shard_cache' in spec
    options = efa_progress.command_options(process['command'])
    case = spec['case_id']
    if read_json(root / (case + '-plan.json')) != spec:
        raise ValueError('archived case identity differs from deterministic plan')
    cfg = read_json(root / 'campaign-config.json')
    build = read_json(root / ('cloud-build-' + spec['build'] + '.json'))
    binary = str(PurePosixPath(build['target_directory']).parent.parent /
                 'cloud-bin' / spec['build'] / 'sharded_wal_bench')
    if process['command'][0] != binary:
        raise ValueError('benchmark binary differs from source-bound cloud build')
    expected = {'--values': spec['value'], '--batch-sizes': spec['batch'], '--windows': spec['window'],
                '--batches': spec['batches'], '--mixed': spec['mixed'], '--reads': spec['reads'],
                '--repetitions': 1, '--backend': mode,
                '--output': str(PurePosixPath(cfg['run_root']) / 'results' / name)}
    if spec['cold']:
        expected['--cold-read-pass'] = 'true'
    if spec['diagnostics']:
        expected['--transport-diagnostics'] = 'true'
    if mode == 'tcp':
        expected.update({'--transport': spec['transport'], '--tcp-read-cache-bytes': spec['cache'],
                         '--tcp-preparation-chunk-batches': 16,
                         '--remote-config': str(PurePosixPath(cfg['run_root']) / 'cases' / case / 'remote.yaml')})
    if spec['build'] == 'current' or checkpoint:
        expected.update({'--read-window': spec['read_window'], '--read-resource-evidence': 'true'})
        if mode == 'native-sync':
            expected.update({'--native-read-cache-bytes': spec['cache'],
                             '--native-read-workers': spec['native_workers']})
        metadata = dict(line.split('=', 1) for line in (root / name / 'campaign.txt').read_text().splitlines()
                        if '=' in line)
        workers = spec['native_workers'] if mode == 'native-sync' else 1
        for key, value in [('native_read_workers', workers),
                           ('native_read_effective_concurrency', min(workers, spec['read_window'])),
                           ('read_window', spec['read_window']), ('read_resource_evidence', 'true')]:
            if metadata.get(key) != str(value):
                raise ValueError('actual benchmark read metadata differs from case')
        if mode == 'native-sync' and metadata.get('native_read_cache_bytes') != str(spec['cache']):
            raise ValueError('actual native cache metadata differs from matched control')
        if not spec['cold']:
            expected['--hot-read-pass'] = 'true'
        phases = ['point_get_window']
        if not spec['cold']:
            phases += ['point_get_hot', 'point_get_many_hot']
        for phase in phases:
            if phase not in summaries or int(summaries[phase]['logical_operations']) != spec['reads']:
                raise ValueError('missing current read phase: ' + phase)
    if options != {key: str(value) for key, value in expected.items()}:
        raise ValueError('benchmark command differs from exact cache/concurrency case')
    if spec['cache'] == 0 and any(value != 0 for (kind, _, metric), value in metrics.items()
            if (kind == 'stage' and metric == 'cache_hits') or
               (kind == 'native_profile' and metric == 'value_cache_hit_sum')):
        raise ValueError('disabled cache recorded hits')
    preparation = read_json(root / (case + '-prepare.command.json'))
    prep = efa_progress.command_options(preparation['command'])
    if preparation.get('exit_code') != 0 or preparation['command'][0] != binary:
        raise ValueError('preparation must use the selected source-bound binary')
    prep_expected = {flag: str(spec[key]) for flag, key in [
        ('--values', 'value'), ('--batch-sizes', 'batch'), ('--windows', 'window'),
        ('--batches', 'batches'), ('--mixed', 'mixed'), ('--reads', 'reads'),
        ('--transport', 'transport'), ('--tcp-read-cache-bytes', 'cache')]}
    case_root = str(PurePosixPath(cfg['run_root']) / 'cases' / case)
    prep_expected.update({'--repetitions': '1', '--prepare-external-case': case_root,
                          '--guest-case-root': case_root, '--remote-endpoints': ','.join(
                              n['private_ip'] + ':7700' for n in sorted(cfg['nodes'], key=lambda n: n['partition']))})
    if spec['build'] == 'current' or checkpoint:
        prep_expected.update({flag: str(spec[key]) for flag, key in [
            ('--tcp-read-lanes', 'lanes'), ('--tcp-server-read-workers', 'workers'),
            ('--tcp-wal-member-read', 'member')]})
    if checkpoint and spec['build'] == 'current':
        prep_expected['--tcp-shard-cache-bytes'] = str(spec['shard_cache'])
    if prep != prep_expected:
        raise ValueError('prepared workload/endpoint/server policy differs from exact case')
    if spec['build'] == 'current' or checkpoint:
        remote = (root / (case + '-remote.yaml')).read_text()
        for key, value in [('read_lanes', spec['lanes']), ('read_cache_bytes', spec['cache'])]:
            if re.findall(r'^\s*' + key + r':\s*(\S+)\s*$', remote, re.M) != [str(value)]:
                raise ValueError('actual remote policy differs from prepared case')
    before, after = [read_json(root / (name + '-' + stage + '.json')) for stage in ('before', 'after')]
    if any(e.get('instance_id') != cfg['coordinator_instance_id'] for e in (before, after)):
        raise ValueError('controller evidence belongs to another instance')
    controller = efa_progress.hardware_interval(before, after, mode == 'tcp' and spec['transport'] == 'efa')
    peak = number(process['peak_sampled_main_process_rss_kib']) * 1024
    if peak <= 0 or peak * efa_progress.RSS_DENOMINATOR > controller['ram_bytes'] * efa_progress.RSS_NUMERATOR:
        raise ValueError('controller sampled RSS exceeds actual guest RAM bound')
    controller_io = io_delta(before, after)
    node_rows = {p: node_evidence(root, case, p) for p in range(5)}
    if any(node_rows[p]['instance_id'] != next(n['instance_id'] for n in cfg['nodes'] if n['partition'] == p)
           for p in range(5)):
        raise ValueError('node hardware inventory differs from campaign')
    if spec['cold']:
        with (root / name / 'read_io.csv').open() as stream:
            for row in csv.DictReader(stream):
                pid = node_rows[int(row['partition'])]['pid'] if mode == 'tcp' else process['pid']
                if int(row['pid']) != pid:
                    raise ValueError('cold physical IO belongs to another process')
    result = {('hardware_io', 'whole_api_controller', key): controller_io[key]
              for key in ('read_bytes', 'write_bytes', 'io_time_ms')}
    if spec['build'] == 'current' or checkpoint:
        result.update(efa_progress.resource_metrics(root / name / 'read_resources.csv', mode, spec,
                                                   process, controller, node_rows, summaries))
    if checkpoint:
        thread_counters = checkpoint_async.validate_threads(read_json(root / (name + '-process.threads.json')), process['pid'])
        result.update({('threads', 'sampled_controller_lifetimes', key): value for key, value in thread_counters.items()})
        if spec['build'] == 'current' and mode == 'tcp':
            result.update(checkpoint_async.read_path_metrics(root / name / 'read_path_evidence.csv', spec,
                'experimental_efa_authority' if spec['transport'] == 'efa' else BACKENDS['tcp']))
            for phase in ('open_recovery', 'reopen_recovery'):
                for field in ('checkpoint_loaded', 'checkpoint_load_ns', 'proof_validation_ns', 'tail_replay_ns',
                              'checkpoint_commit_ns', 'connection_setup_ns', 'fencing_ns', 'tail_batches', 'tail_bytes'):
                    if ('stage', phase, field) not in metrics:
                        raise ValueError('missing current checkpoint recovery evidence')
            if metrics['stage', 'open_recovery', 'checkpoint_loaded'] != 0 or metrics['stage', 'reopen_recovery', 'checkpoint_loaded'] != 1:
                raise ValueError('fresh/reopen checkpoint adoption differs')
    for phase in ('cache_evicted_get', 'cache_evicted_get_many', 'cache_evicted_get_window'):
        for key in ('physical_read_bytes', 'major_faults', 'minor_faults'):
            values = [value for (kind, p, field), value in metrics.items()
                      if kind == 'physical_io' and p == phase and field.endswith('_' + key)]
            if values:
                result['physical_io_total', phase, key] = sum(values)
    return result


def validate_progress_inventory(root):
    cfg = read_json(root / 'campaign-config.json')
    ids = [cfg['coordinator_instance_id']] + [n['instance_id'] for n in cfg['nodes']]
    if (cfg.get('campaign') not in ('efa-progress', 'checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing') or cfg.get('efa') is not True or
            len(ids) != 6 or len(set(ids)) != 6 or sorted(n['partition'] for n in cfg['nodes']) != list(range(5))):
        raise ValueError('qualification requires exact six-guest/five-partition inventory')
    instances = json.loads((root / 'owned-instances.json').read_text())
    if len(instances) != 6 or {i['InstanceId'] for i in instances} != set(ids):
        raise ValueError('actual owned instance inventory differs')
    tags = {'Project': 'TideHunter', 'ManagedBy': 'sharded-wal-benchmark', 'RunId': cfg['run_id']}
    zone = cfg['availability_zone'] if cfg.get('campaign') in ('transport-floor', 'stage-timing') else 'us-east-1a'
    if cfg.get('campaign') in ('transport-floor', 'stage-timing') and not re.fullmatch(r'us-east-1[a-f]', zone):
        raise ValueError('transport campaign requires its reviewed US East zone')
    reviewed_types = ('g6.16xlarge', 'c6id.32xlarge') if cfg.get('campaign') in ('transport-floor', 'stage-timing') else ('g6.16xlarge',)
    if cfg.get('campaign') == 'stage-timing':
        stage_timing.validate_placement(cfg, instances, json.loads((root / 'placement-groups.json').read_text()))
    for instance in instances:
        actual = {t['Key']: t['Value'] for t in instance['Tags']}
        if (instance['InstanceType'] != cfg['instance_type'] or
                any(actual.get(k) != v for k, v in tags.items())):
            raise ValueError('qualification hardware ownership/type differs')
        if cfg.get('campaign') in ('checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing'):
            if (cfg.get('instance_type') not in reviewed_types or
                    instance.get('Placement', {}).get('AvailabilityZone') != zone or
                    not any(n.get('InterfaceType') in ('efa', 'efa-only') for n in instance.get('NetworkInterfaces', []))):
                raise ValueError('checkpoint fleet is not the reviewed real-EFA hardware/zone')
            node = next((n for n in cfg['nodes'] if n['instance_id'] == instance['InstanceId']), None)
            if node and instance.get('PrivateIpAddress') != node['private_ip']:
                raise ValueError('configured shard endpoint differs from actual instance')


def validate_qualification(root):
    root = Path(root)
    errors, validated = {}, []
    profile = efa_progress
    campaign_name = 'efa-progress'
    try:
        campaign_name = read_json(root / 'campaign-config.json').get('campaign')
        if campaign_name == 'checkpoint-async':
            profile = checkpoint_async
        elif campaign_name == 'cluster-ram':
            profile = cluster_ram
        elif campaign_name in ('scalar-transport', 'transport-floor', 'stage-timing'):
            profile = stage_timing if campaign_name == 'stage-timing' else transport_floor if campaign_name == 'transport-floor' else scalar_transport
            receipts = {f'cloud-build-{label}': profile.validate_build_identity(root, label)
                        for label in ('baseline', 'current')}
            receipts['cloud-build-pair-verified'] = read_json(root / 'cloud-build-pair-verified.json')
            build_errors = validate_build_pair(root, receipts)
            if build_errors:
                raise ValueError(f'scalar transport build qualification failed: {build_errors}')
        validate_progress_inventory(root)
    except (OSError, ValueError, KeyError, TypeError) as error:
        errors['inventory'] = str(error)
    expected = profile.expected_cases()
    for spec in profile.qualification_cases():
        for mode in spec.get('modes', ('native-sync', 'tcp')):
            name = spec['case_id'] + '-' + mode
            try:
                read_case(root, name, expected[name], campaign_name)
                validated.append(name)
            except (OSError, ValueError, KeyError, TypeError, tarfile.TarError) as error:
                errors[name] = str(error)
    return dict(schema=1, passed=not errors, case_ids=validated, errors=errors,
                expected_api_cases=sum(len(c.get('modes', ('native-sync', 'tcp'))) for c in profile.qualification_cases()),
                expected_node_archives=5 * len(profile.qualification_cases()),
                rss_limit_fraction=efa_progress.RSS_NUMERATOR / efa_progress.RSS_DENOMINATOR)


def read_recovery_case(root, name, spec, profile=checkpoint_async):
    cfg = read_json(root / 'campaign-config.json')
    build = read_json(root / ('cloud-build-' + spec['build'] + '.json'))
    case = spec['case_id']
    if read_json(root / (case + '-plan.json')) != spec:
        raise ValueError('recovery case differs from deterministic plan')
    base = PurePosixPath(cfg['run_root'])
    binary = str(base / 'cloud-bin' / spec['build'] / 'sharded_wal_recovery_bench')
    preparation = read_json(root / (case + '-prepare.command.json'))
    expected_prep = {'--prepare-external-case': str(base / 'cases' / case),
                     '--guest-case-root': str(base / 'cases' / case),
                     '--remote-endpoints': ','.join(n['private_ip'] + ':7700' for n in sorted(cfg['nodes'], key=lambda n: n['partition'])),
                     '--tcp-read-cache-bytes': '0', '--transport': spec['transport'],
                     '--tcp-read-lanes': str(spec['lanes']), '--tcp-server-read-workers': str(spec['workers']),
                     '--tcp-wal-member-read': spec['member'], '--values': '8192', '--batch-sizes': '32',
                     '--windows': '32', '--batches': str(spec['batches']), '--mixed': str(spec['mixed']),
                     '--reads': '8192', '--repetitions': '1'}
    if spec['build'] == 'current':
        expected_prep['--tcp-shard-cache-bytes'] = '0'
    if profile in (cluster_ram, scalar_transport, transport_floor, stage_timing):
        expected_prep = efa_progress.command_options(['binary', '--prepare-external-case', str(base / 'cases' / case),
            '--guest-case-root', str(base / 'cases' / case), '--remote-endpoints',
            ','.join(n['private_ip'] + ':7700' for n in sorted(cfg['nodes'], key=lambda n: n['partition']))]
            + profile.flags(spec, 'tcp', prepare=True))
    if (preparation.get('exit_code') != 0 or preparation['command'][0] != str(base / 'cloud-bin' / spec['build'] / 'sharded_wal_bench')
            or efa_progress.command_options(preparation['command']) != expected_prep):
        raise ValueError('recovery preparation differs from source-bound server policy')
    before, after = [read_json(root / (name + '-' + stage + '.json')) for stage in ('before', 'after')]
    if any(e.get('instance_id') != cfg['coordinator_instance_id'] for e in (before, after)):
        raise ValueError('recovery controller identity differs')
    hardware = efa_progress.hardware_interval(before, after, spec['transport'] == 'efa')
    result = {('hardware_io', 'whole_recovery_controller', k): v for k, v in io_delta(before, after).items()
              if isinstance(v, (float, int))}
    for action in ('seed', 'reopen'):
        row = read_json(root / name / action / 'recovery.json')
        profile.validate_recovery_json(row, spec, action)
        if not (root / name / action / 'COMPLETE').is_file():
            raise ValueError('missing recovery action completion')
        process = read_json(root / (name + '-' + action + '-process.command.json'))
        expected = {'--action': action, '--remote-config': str(base / 'cases' / case / 'remote.yaml'),
                    '--output': str(base / 'results' / name / action), '--value-bytes': '8192',
                    '--checkpoint-batches': '64', '--tail-batches': str(spec['tail_batches'])}
        if (process.get('exit_code') != 0 or process['command'][0] != binary
                or efa_progress.command_options(process['command']) != expected):
            raise ValueError('recovery driver command differs from planned overlay execution')
        threads = checkpoint_async.validate_threads(read_json(root / (name + '-' + action + '-process.threads.json')), process['pid'])
        result.update({('threads', action, key): value for key, value in threads.items()})
        peak = number(process['peak_sampled_main_process_rss_kib']) * 1024
        if not 0 < peak <= hardware['ram_bytes'] * 4 / 5:
            raise ValueError('recovery controller RSS exceeds actual RAM bound')
        for key in ('open_ns', 'checkpoint_create_ns', 'tail_append_ns', 'verification_ns'):
            result['recovery', action, key] = row[key]
        for key, value in row.get('recovery_stats', {}).items():
            if value is not None:
                result['recovery', action, 'stats_' + key] = int(value)
    service = read_json(root / name / 'service-restore.json')
    # Transport is verified by source-bound preparation and actual hardware/setup
    # evidence above. The service receipt records restart timing, not transport.
    if service['restart'] != spec['restart'] or service['os_cache_evicted'] is not False:
        raise ValueError('recovery restart/cache semantics differ')
    restore = checkpoint_async.positive(service['controller_service_restore_ns'])
    ready = checkpoint_async.positive(service['nodes_ready_ns'])
    if ready >= restore:
        raise ValueError('service restoration precedes shard readiness')
    result['recovery', 'controller_inclusive', 'service_restore_ns'] = restore
    result['recovery', 'controller_inclusive', 'nodes_ready_ns'] = ready
    for partition in range(5):
        node = node_evidence(root, case, partition)
        expected_id = next(n['instance_id'] for n in cfg['nodes'] if n['partition'] == partition)
        if node['instance_id'] != expected_id:
            raise ValueError('recovery node identity differs')
        if spec['restart'] == 'full':
            result['recovery', 'node_local', f'partition_{partition}_restart_ns'] = node['node_restart_ns']
    return result


def validate_checkpoint_campaign(root, receipts, recorded, qualification, profile=checkpoint_async):
    cfg = read_json(root / 'campaign-config.json')
    campaign_name = {cluster_ram: 'cluster-ram', scalar_transport: 'scalar-transport',
                     transport_floor: 'transport-floor', stage_timing: 'stage-timing', checkpoint_async: 'checkpoint-async'}[profile]
    reviewed_types = ('g6.16xlarge', 'c6id.32xlarge') if profile in (transport_floor, stage_timing) else ('g6.16xlarge',)
    if (cfg.get('campaign') != campaign_name or cfg.get('repetitions') != (3 if profile in (scalar_transport, transport_floor, stage_timing) else 5)
            or cfg.get('export_reserve_seconds') != (300 if profile is stage_timing else 600) or cfg.get('instance_type') not in reviewed_types
            or cfg.get('region') != 'us-east-1'):
        raise ValueError('checkpoint campaign hardware/repetition/lifetime policy differs')
    if not qualification['passed'] or read_json(root / 'qualification-receipt.json') != qualification:
        raise ValueError('hardware qualification receipt/evidence mismatch')
    selected = profile.plan(cfg.get('checkpoint_scope', 'full'))
    ordered = []
    for spec in selected:
        modes = spec['modes'] if spec['rep'] % 2 == 0 else list(reversed(spec['modes']))
        ordered.extend(spec['case_id'] + '-' + mode for mode in modes)
    if recorded != ordered:
        raise ValueError('campaign order differs: qualification/interleaving/exact planned cases required')
    planned = {spec['case_id'] for spec in selected}
    if {p.name.removesuffix('-plan.json') for p in root.glob('*-plan.json')} != planned:
        raise ValueError('missing/unplanned case plans')
    if {p.name for p in (root / 'nodes').iterdir() if p.is_dir()} != planned:
        raise ValueError('missing/unplanned node case archives')
    baseline = receipts['cloud-build-baseline']
    if (baseline['source_tree_sha256'] != profile.BASELINE_SOURCE_SHA256 or
            baseline['local_binary_sha256'] != profile.BASELINE_BINARY_SHA256):
        raise ValueError('baseline differs from the selected campaign source/binary pair')
    for label in ('baseline', 'current'):
        if profile is stage_timing:
            profile.validate_build_identity(root, label)
            continue
        source = root / (f'sharded_wal_recovery_bench-{label}.rs'
                         if profile in (cluster_ram, scalar_transport, transport_floor, stage_timing) else 'sharded_wal_recovery_bench.rs')
        overlay = hashlib.sha256(source.read_bytes()).hexdigest()
        recovery_stats = label == 'current' or profile in (cluster_ram, scalar_transport, transport_floor, stage_timing)
        receipt = receipts['cloud-build-' + label]
        if (receipt['features'] != 'experimental-fabric,test-utils' or receipt['recovery_overlay_sha256'] != overlay
                or receipt['recovery_cfg'] != ('checkpoint_recovery_stats' if recovery_stats else None)
                or not is_sha256(receipt.get('lock_sha256')) or receipt.get('engine_source_unchanged') is not True
                or not is_sha256(receipt['source_archive_sha256']) or not is_sha256(receipt['recovery_binary_sha256'])):
            raise ValueError('recovery source/overlay/cfg/binary receipt differs')
        command = receipt['recovery_command']
        expected = [receipt['command'][0], '+1.90.0', 'rustc', '--release', '--locked',
                    '--manifest-path', str(PurePosixPath(cfg['run_root']) / 'build-source' / label / 'Cargo.toml'),
                    '-p', 'tidehunter', '--features', receipt['features'], '--example',
                    'sharded_wal_recovery_bench', '--', '--check-cfg', 'cfg(checkpoint_recovery_stats)']
        if recovery_stats:
            expected += ['--cfg', 'checkpoint_recovery_stats']
        recorded_build = read_json(root / ('build-recovery-' + label + '.command.json'))
        if command != expected or recorded_build.get('exit_code') != 0 or recorded_build.get('command') != command:
            raise ValueError('recovery compilation must apply cfg only to identical example source')
        if not receipt.get('compiler') or not is_sha256(receipt.get('libfabric_sha256')):
            raise ValueError('missing compiler/provider identity')
        if profile in (scalar_transport, transport_floor, stage_timing):
            profile.validate_build_identity(root, label)


def compare_checkpoint(runs, profile=checkpoint_async):
    specs = {c['case_id']: c for c in profile.plan()}
    grouped = defaultdict(dict)
    for info, metrics in runs.values():
        family, build, value, batch, window, rep, mode, _ = info
        for (kind, phase, metric), measured in metrics.items():
            grouped[family, build, value, batch, window, mode, kind, phase, metric][rep] = measured
    rows = []
    for key, repetitions in sorted(grouped.items()):
        family, build, value, batch, window, mode, kind, phase, metric = key
        spec = next(c for c in specs.values() if c['family'] == family and c['build'] == build and c['value'] == value)
        oldfamily, oldbuild = family, 'baseline'
        pressure = family.startswith('pressure-') and profile is checkpoint_async
        if pressure:
            oldfamily, oldbuild = f"core-f0-s0-{spec['transport']}", 'current'
        previous = grouped.get((oldfamily, oldbuild, value, batch, window, mode, kind, phase, metric), {}) if build == 'current' else {}
        common = sorted(repetitions.keys() & previous.keys())
        new = statistics.median(repetitions.values())
        old = statistics.median(previous.values()) if previous else None
        native_family = f"core-f0-s0-{spec['transport']}" if pressure else family
        native = grouped.get((native_family, build, value, batch, window, 'native-sync', kind, phase, metric), {})
        native_old = grouped.get((oldfamily, oldbuild, value, batch, window, 'native-sync', kind, phase, metric), {})
        rows.append(dict(comparison='cache_capacity_not_equal_memory' if profile is cluster_ram and spec['workload'] in ('capacity', 'large-config', 'pressure') else 'finite_cache_pressure' if pressure else 'qualification' if spec['qualification'] else 'diagnostic_only' if profile is scalar_transport and spec['diagnostics'] else 'whole_implementation_and_config',
            family=family, backend=mode, build=build, value_bytes=value, batch_size=batch, window=window,
            kind=kind, phase=phase, metric=metric, old_label=oldbuild, new_label=build,
            old_median=old, new_median=new, new_over_old=ratio(new, old),
            old_repetitions=len(previous), new_repetitions=len(repetitions),
            old_over_native_barrier=ratio(old, statistics.median(native_old.values())) if native_old and mode == 'tcp' and kind == 'summary' else None,
            new_over_native_barrier=ratio(new, statistics.median(native.values())) if native and mode == 'tcp' and kind == 'summary' else None,
            repetition_values=repetitions, paired_deltas={r: repetitions[r] - previous[r] for r in common},
            paired_ratios={r: ratio(repetitions[r], previous[r]) for r in common},
            transport=spec['transport'], frontend_cache_bytes=spec['cache'], per_shard_cache_bytes=spec['shard_cache'],
            native_control_ids=[c['native_control_id'] for c in specs.values()
                                if c['family'] == family and c['build'] == build and c['value'] == value],
            native_control_semantics='paired correlated transport controls; never pooled across transports'))
    return rows


def compare_progress(runs):
    specs = {c['case_id']: c for c in efa_progress.plan()}
    grouped, identities = defaultdict(list), defaultdict(set)
    for info, metrics in runs.values():
        family, build, v, b, w, _, mode, case = info
        for (kind, phase, metric), value in metrics.items():
            grouped[family, build, mode, v, b, w, kind, phase, metric].append(value)
        identities[family, build, v, b, w].add(specs[case]['native_control_id'])
    medians = {key: statistics.median(values) for key, values in grouped.items()}
    rows = []
    for key in sorted(grouped):
        family, build, mode, v, b, w, kind, phase, metric = key
        spec = next(c for c in specs.values() if c['family'] == family and c['build'] == build)
        old_family, old_build, comparison = family, 'baseline', 'source_and_read_policy'
        if build == 'baseline':
            old_family = None
        elif family.startswith('core-'):
            control = next(c for c in specs.values() if c['family'].startswith('core-')
                           and c['transport'] == spec['transport'] and c['cache'] == 0
                           and c['build'] == ('current' if spec['cache'] else 'baseline'))
            old_family, old_build = control['family'], control['build']
            if spec['cache']:
                comparison = 'matched_cache_budget'
        elif family.startswith('cold-'):
            control = next(c for c in specs.values() if c['family'].startswith('cold-')
                           and (c['member'], c['lanes'], c['workers'], c['read_window']) == ('mapped', 1, 1, 1))
            old_family, old_build, comparison = control['family'], 'current', 'focused_read_policy'
        else:
            old_family, comparison = None, 'qualification' if spec['qualification'] else 'diagnostic_only'
        oldkey = (old_family, old_build, mode, v, b, w, kind, phase, metric)
        old, new = medians.get(oldkey), medians[key]
        native = (family, build, 'native-sync', v, b, w, kind, phase, metric)
        old_native = (old_family, old_build, 'native-sync', v, b, w, kind, phase, metric)
        # Resource process inventories differ by backend. Ratios are for API measurements,
        # not a synthetic comparison of differently scoped process counters.
        native_ratio = kind == 'summary' and mode == 'tcp'
        rows.append(dict(comparison=comparison, family=family, backend=mode, value_bytes=v,
            batch_size=b, window=w, kind=kind, phase=phase, metric=metric,
            old_label=old_family, new_label=family, old_median=old, new_median=new,
            new_over_old=ratio(new, old), old_repetitions=len(grouped.get(oldkey, [])),
            new_repetitions=len(grouped[key]),
            old_over_native_barrier=ratio(old, medians.get(old_native)) if native_ratio else None,
            new_over_native_barrier=ratio(new, medians.get(native)) if native_ratio else None,
            build=build, transport=spec['transport'], cache_bytes=spec['cache'],
            read_lanes=spec['lanes'], server_read_workers=spec['workers'],
            read_window=spec['read_window'], native_read_workers=spec['native_workers'],
            effective_native_concurrency=min(spec['read_window'], spec['native_workers']),
            wal_member_read=spec['member'],
            native_control_ids=sorted(identities[family, build, v, b, w]),
            measurement_semantics='concurrent_window' if phase.endswith('_window') and 'get' in phase else
                                  'scalar_latency' if phase in ('point_get', 'cache_evicted_get') else 'phase'))
    return rows


def write_csv(path, rows, fields):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def ratio(a, b):
    return a / b if a is not None and b else None


def compare(runs):
    grouped = defaultdict(list)
    for info, metrics in runs.values():
        family, build, v, b, w, _, mode, _ = info
        for (kind, phase, metric), value in metrics.items():
            grouped[(family, mode, v, b, w, kind, phase, metric, build)].append(value)
    medians = {key: statistics.median(values) for key, values in grouped.items()}
    controls = {
        "capacity4m": ("warm", "capacity_only", "current_1m", "current_4m"),
        "chunk32": ("storage", "preparation_chunk", "current_chunk16", "current_chunk32"),
        "cache": ("storage", "read_cache", "cache_disabled", "cache_8m"),
        "cache-efa": ("storage-efa", "read_cache", "cache_disabled", "cache_8m"),
    }
    rows = []
    for key in sorted({k[:-1] for k in grouped}):
        family, mode, v, b, w, kind, phase, metric = key
        control_family, comparison, old_label, new_label = controls.get(
            family, (family, "code", "baseline", "current"))
        control_build = "current" if family in controls else "baseline"
        oldkey = (control_family,) + key[1:] + (control_build,)
        newkey = key + ("current",)
        old, new = medians.get(oldkey), medians.get(newkey)
        old_native = (control_family, "native-sync", v, b, w, kind, phase, metric, control_build)
        new_native = ("storage" if family == "chunk32" else family,
                      "native-sync", v, b, w, kind, phase, metric, "current")
        rows.append(dict(comparison=comparison, family=family,
            backend=mode, value_bytes=v, batch_size=b, window=w, kind=kind, phase=phase,
            metric=metric, old_label=old_label, new_label=new_label, old_median=old, new_median=new,
            new_over_old=ratio(new, old), old_repetitions=len(grouped.get(oldkey, [])),
            new_repetitions=len(grouped.get(newkey, [])),
            old_over_native_barrier=None if family == "capacity4m" else ratio(old, medians.get(old_native)),
            new_over_native_barrier=None if family == "capacity4m" else ratio(new, medians.get(new_native))))
    return rows


def objects(value, children):
    for item in value:
        yield item
        yield from objects(item.get(children, []), children)


def verified_disk(evidence):
    mounts = list(objects(json.loads(evidence["findmnt"])["filesystems"], "children"))
    if len(mounts) != 1:
        raise ValueError("ambiguous benchmark mount")
    source = mounts[0]["source"]
    devices = list(objects(json.loads(evidence["lsblk"])["blockdevices"], "children"))
    matches = [d for d in devices if d.get("path") == source and d.get("type") == "disk"
               and "amazon ec2 nvme instance storage" in (d.get("model") or "").lower()]
    if len(matches) != 1:
        raise ValueError("mount source not verified as an instance-store whole disk")
    return source


def io_delta(before, after):
    source = verified_disk(before)
    if verified_disk(after) != source:
        raise ValueError("benchmark device changed")

    def disk(evidence):
        rows = [line.split() for line in evidence["diskstats"].splitlines()]
        found = [r for r in rows if len(r) >= 14 and r[2] == Path(source).name]
        if len(found) != 1:
            raise ValueError("instance-store diskstats missing/ambiguous")
        return {"read_sectors": int(found[0][5]), "write_sectors": int(found[0][9]),
                "io_time_ms": int(found[0][12])}

    previous, current = disk(before), disk(after)
    delta = {key: current[key] - previous[key] for key in previous}
    if min(delta.values()) < 0:
        raise ValueError("device counters decreased/reset")
    delta.update(device=source, read_bytes=delta["read_sectors"] * 512,
                 write_bytes=delta["write_sectors"] * 512)
    delta["evidence_interval_seconds"] = (
        datetime.fromisoformat(after["utc"]) - datetime.fromisoformat(before["utc"])).total_seconds()
    if delta["evidence_interval_seconds"] <= 0:
        raise ValueError("invalid evidence timestamps")
    for name in ("pswpin", "pswpout"):
        counters = [dict(line.split() for line in e["vmstat"].splitlines()) for e in (before, after)]
        delta[name + "_pages"] = int(counters[1][name]) - int(counters[0][name])
        if delta[name + "_pages"] < 0:
            raise ValueError("swap counters decreased/reset")
    return delta


def node_evidence(root, case, node):
    archive = root / "nodes" / case / f"node-{node}.tar.gz"
    receipt = read_json(archive.with_name(archive.name + ".receipt.json"))
    if archive.stat().st_size > 16 * 1024 * 1024:
        raise ValueError("oversized compressed node archive")
    with archive.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    if (archive.stat().st_size != receipt["bytes"] or digest != receipt["sha256"]
            or not str(receipt["key"]).endswith(f"/results/nodes/{case}/node-{node}.tar.gz")):
        raise ValueError("node archive differs from its scoped receipt")
    with tarfile.open(archive) as stream:
        members = stream.getmembers()
        if (len(members) > 64 or sum(m.size for m in members) > 16 * 1024 * 1024
                or any(not m.isfile() or Path(m.name).name != m.name for m in members)
                or len({m.name for m in members}) != len(members)):
            raise ValueError("unsafe/oversized/duplicate node archive members")

        def text(name):
            member = stream.getmember(name)
            if member.size > 2 * 1024 * 1024:
                raise ValueError("oversized evidence member")
            return stream.extractfile(member).read().decode()

        before, after = json.loads(text("before.json")), json.loads(text("after.json"))
        delta = io_delta(before, after)
        if '-efa-' in case:
            if not re.search(r'^\s*prov_name: efa\s*$', text('efa-provider.txt'), re.M):
                raise ValueError('EFA case lacks the actual provider identity')
            previous, current = before.get('efa_counters', {}), after.get('efa_counters', {})
            if not previous or set(previous) != set(current):
                raise ValueError('EFA case lacks stable device counters')
            differences = {k: int(current[k]) - int(previous[k]) for k in previous}
            if min(differences.values()) < 0:
                raise ValueError('EFA counters decreased')
            traffic = sum(v for k, v in differences.items() if Path(k).name in ['send_bytes', 'recv_bytes', 'tx_bytes', 'rx_bytes'])
            if traffic <= 0:
                raise ValueError('EFA case has no measured NIC traffic')
            delta['efa_traffic_bytes'] = traffic
            delta['efa_counter_deltas'] = differences
        status = dict(line.split(":", 1) for line in text("process-status.txt").splitlines() if ":" in line)
        spec = next((c for c in efa_progress.plan() + checkpoint_async.plan() + cluster_ram.plan() + scalar_transport.plan() + transport_floor.plan() + stage_timing.plan() if c['case_id'] == case), None)
        cfg = read_json(root / 'campaign-config.json') if case.startswith(('transport-', 'floor-', 'stage-')) else None
        bound_profile = stage_timing if case.startswith('stage-') else transport_floor if case.startswith('floor-') else scalar_transport
        bound_campaign = 'stage-timing' if case.startswith('stage-') else 'transport-floor' if case.startswith('floor-') else 'scalar-transport'
        if cfg is not None and (cfg.get('campaign') != bound_campaign
                or spec not in bound_profile.plan(cfg.get('checkpoint_scope', 'full'))):
            raise ValueError('unplanned source-bound node case')
        if spec is not None:
            delta.update(efa_progress.hardware_interval(before, after, spec['transport'] == 'efa'))
            if (before.get('partition') != node or after.get('partition') != node or
                    not before.get('instance_id') or before['instance_id'] != after.get('instance_id')):
                raise ValueError('node instance/partition evidence differs')
            delta['instance_id'] = before['instance_id']
            delta['pid'] = int(status['Pid'])
            if spec['build'] == 'current' or 'shard_cache' in spec:
                launch = text('launch.yaml')
                if case.startswith(('floor-', 'stage-')):
                    bound_profile.validate_node_policy(root, spec, launch, node)
                for key, value in [('read_workers', spec['workers']), ('wal_member_read', spec['member'])]:
                    if re.findall(r'^\s*' + key + r':\s*(\S+)\s*$', launch, re.M) != [str(value)]:
                        raise ValueError('launched node policy differs from case')
                if 'shard_cache' in spec and spec['build'] == 'current':
                    if re.findall(r'^\s*read_cache_bytes:\s*(\S+)\s*$', launch, re.M) != [str(spec['shard_cache'])]:
                        raise ValueError('launched shard cache budget differs')
                if spec.get('workload') and spec['build'] == 'current':
                    if re.findall(r'^\s*read_memory_allowance_bytes:\s*(\S+)\s*$', launch, re.M) != [str(spec['shard_allowance'])]:
                        raise ValueError('launched shard read-memory allowance differs')
            if 'shard_cache' in spec:
                counters = checkpoint_async.validate_threads(json.loads(text(f"threads-{delta['pid']}.json")), delta['pid'])
                delta.update({'sampled_thread_' + key: value for key, value in counters.items()})
                start = json.loads(text('start.json'))
                cfg = read_json(root / 'campaign-config.json')
                if start['deadline_utc'] != cfg['deadline_utc'] or not start['ready']:
                    raise ValueError('missing original node readiness/deadline')
                if spec.get('restart') == 'full':
                    restart = json.loads(text('restart.json'))
                    checkpoint_async.validate_restart(restart, spec, cfg, node, delta['pid'])
                    if restart['old_pid'] != start['pid'] or restart['ready'] != start['ready']:
                        raise ValueError('restart does not replace originally ready server')
                    delta['node_restart_ns'] = restart['node_restart_ns']
                    if cfg.get('campaign') in ('scalar-transport', 'transport-floor', 'stage-timing'):
                        build = bound_profile.validate_build_identity(root, spec['build'])
                        if restart.get('build') != spec['build'] or restart.get('binary_sha256') != build['binary_sha256']:
                            raise ValueError('restarted node executable differs from source-bound build')
                elif start['pid'] != delta['pid'] or any(m.name == 'restart.json' for m in members):
                    raise ValueError('surviving shard process unexpectedly replaced')
                if cfg.get('campaign') in ('scalar-transport', 'transport-floor', 'stage-timing'):
                    build = bound_profile.validate_build_identity(root, spec['build'])
                    if start.get('build') != spec['build'] or start.get('binary_sha256') != build['binary_sha256']:
                        raise ValueError('started node executable differs from source-bound build')
        for key in ("VmRSS", "VmHWM", "VmSwap"):
            delta[key + "_kib"] = int(status[key].split()[0]) if key in status else None
        if spec is not None:
            if (delta['pid'] <= 0 or delta['VmSwap_kib'] != 0 or
                    delta['VmRSS_kib'] is None or delta['VmHWM_kib'] is None or
                    not 0 < delta['VmRSS_kib'] <= delta['VmHWM_kib'] or
                    delta['VmHWM_kib'] * 1024 * efa_progress.RSS_DENOMINATOR >
                    delta['ram_bytes'] * efa_progress.RSS_NUMERATOR):
                raise ValueError('node swap/RSS resource bound failed')
        if any(member.name == 'process-stat.txt' for member in members):
            fields = text('process-stat.txt').rsplit(')', 1)[-1].split()
            if len(fields) < 20:
                raise ValueError('truncated process stat')
            delta['process_user_ticks'] = int(fields[11])
            delta['process_system_ticks'] = int(fields[12])
            delta['process_ticks_per_case_second'] = (
                delta['process_user_ticks'] + delta['process_system_ticks']) / delta['evidence_interval_seconds']
    return dict(case=case, node=node, **delta)


def summarize(rows):
    grouped = defaultdict(list)
    for row in rows:
        if row["kind"] == "summary" and row["new_over_old"] is not None:
            grouped[(row["family"], row["backend"], row["phase"], row["metric"])].append(row)
    result = []
    for (family, backend, phase, metric), values in sorted(grouped.items()):
        if metric not in ("p50_ns", "p95_ns", "p99_ns", "phase_wall_ns", "instrumented_phase_ops_per_sec"):
            continue
        ratios = [r["new_over_old"] for r in values]
        item = dict(family=family, backend=backend, phase=phase, metric=metric,
                    configurations=len(values), median_new_over_old=statistics.median(ratios),
                    range_new_over_old=[min(ratios), max(ratios)])
        for name in ("old_over_native_barrier", "new_over_native_barrier"):
            available = [r[name] for r in values if r[name] is not None]
            item[name] = statistics.median(available) if available else None
        result.append(item)
    return result


def summarize_profiles(runs):
    grouped = defaultdict(lambda: defaultdict(list))
    for info, metrics in runs.values():
        family, build, value, batch, window, _, mode, _ = info
        for (kind, state, metric), measured in metrics.items():
            if kind == "profile":
                grouped[(family, build, mode, value, batch, window, state)][metric].append(measured)
    result = []
    for (family, build, backend, value, batch, window, state), fields in sorted(grouped.items()):
        total = sum(fields["whole_call_ns_sum"])
        count = sum(fields["whole_call_ns_n"])
        metrics = {}
        for field in fields:
            if field.endswith("_ns_sum"):
                name = field.removesuffix("_sum")
                summed = sum(fields[field])
                samples = count
                denominator = "whole_call_ns_n"
                if name in READ_RECHECK_TIMERS:
                    denominator = READ_RECHECK_SAMPLES + "_sum"
                    if denominator not in fields:
                        raise ValueError("missing read-recheck sample denominator")
                    samples = sum(fields[denominator])
                metrics[name] = dict(mean_ns=ratio(summed, samples),
                                     fraction_of_summed_whole=ratio(summed, total) if samples else None,
                                     median_case_p50_ns=statistics.median(fields[name + "_p50"]) if samples else None,
                                     samples=samples, denominator_field=denominator)
        result.append(dict(family=family, build=build, backend=backend, value_bytes=value,
                           batch_size=batch, window=window, state=state,
                           diagnostic_samples=count, metrics=metrics))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--allow-partial", action="store_true")
    parser.add_argument("--profile", choices=["original", "overlap", "read-path", "efa", "efa-cache", "efa-progress", "checkpoint-async", "cluster-ram", "scalar-transport", "transport-floor", "stage-timing"], default="original")
    parser.add_argument("--spread-results", type=Path, help="Required stage-timing historical transport-floor result root")
    args = parser.parse_args()
    if args.profile == 'stage-timing' and args.spread_results is None:
        parser.error('stage-timing requires --spread-results for the matched previous-current RAM controls')
    if args.profile != 'stage-timing' and args.spread_results is not None:
        parser.error('--spread-results is only supported by stage-timing')
    root = args.results.resolve()
    scope = read_json(root / 'campaign-config.json').get('checkpoint_scope', 'full') if args.profile in ('checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing') else 'full'
    expected = expected_cases(args.profile, scope)
    output = args.output or root / "analysis"
    output.mkdir(parents=True, exist_ok=True)
    runs, invalid, nodes, node_errors = {}, {}, [], {}
    unexpected = sorted(p.name for p in root.iterdir() if p.is_dir()
                        and p.name.startswith(("warm-", "storage-", "capacity4m-", "chunk32-", "cold-", "diagnostic-", "cache-", "qual-", "core-", "diag-", "pressure-", "recovery-")) and p.name not in expected)
    if args.profile in ('checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing'):
        unexpected = sorted(p.name for p in root.iterdir() if p.is_dir() and p.name not in expected
                            and p.name != 'nodes' and p.resolve() != output.resolve())
    for name, info in expected.items():
        try:
            runs[name] = (info, read_case(root, name, info, args.profile))
        except (OSError, ValueError, KeyError, TypeError, tarfile.TarError) as error:
            invalid[name] = str(error)
    for case in sorted({info[-1] for info in expected.values()}):
        for node in range(5):
            try:
                nodes.append(node_evidence(root, case, node))
            except (OSError, ValueError, KeyError, TypeError, tarfile.TarError) as error:
                node_errors[f"{case}/node-{node}"] = str(error)
    rows = (stage_timing.compare(runs) if args.profile == 'stage-timing' else transport_floor.compare(runs) if args.profile == 'transport-floor' else
            compare_checkpoint(runs, scalar_transport) if args.profile == 'scalar-transport' else
            compare_checkpoint(runs, cluster_ram) if args.profile == 'cluster-ram' else
            compare_checkpoint(runs) if args.profile == 'checkpoint-async' else
            compare_progress(runs) if args.profile == 'efa-progress' else compare(runs))
    fields = list(rows[0]) if rows else ["comparison", "family", "backend", "metric"]
    write_csv(output / "comparison.csv", rows, fields)
    node_fields = list(dict.fromkeys(key for node in nodes for key in node)) if nodes else ["case", "node", "device"]
    write_csv(output / "node_io.csv", nodes, node_fields)
    phase_summary = stage_timing.summarize(rows) if args.profile == 'stage-timing' else summarize(rows)
    load_stages = [r for r in rows if r["backend"] == "tcp" and r["kind"] == "stage"
                   and r["phase"] == "load_write_window" and r["metric"] in
                   {"validation_ns", "storage_wait_ns", "publication_ns", "appended_groups",
                    "explicit_group_persistence_calls"}]
    profiles = summarize_profiles(runs)
    regressions = [r for r in rows if r["backend"] == "tcp" and r["kind"] == "summary"
                   and r["metric"] == "phase_wall_ns" and (r["new_over_old"] or 0) > 1.1]
    evidence_errors, receipts = {}, {}
    for name in ("campaign-receipt", "cloud-build-baseline", "cloud-build-current", "cloud-build-pair-verified"):
        try:
            receipts[name] = read_json(root / f"{name}.json")
        except (OSError, ValueError) as error:
            evidence_errors[name] = str(error)
    campaign = receipts.get("campaign-receipt", {})
    completed_cases = campaign.get("completed_cases", [])
    if not isinstance(completed_cases, list) or any(not isinstance(c, dict) for c in completed_cases):
        completed_cases = []
    recorded = [f"{c.get('case_id')}-{c.get('backend')}" for c in completed_cases]
    if (not campaign.get("complete") or campaign.get("failures") or
            len(recorded) != len(expected) or set(recorded) != set(expected)):
        evidence_errors["campaign-receipt"] = f"receipt does not confirm all {len(expected)} distinct expected API cases without failures"
    build_errors = validate_build_pair(root, receipts)
    if args.profile in ['read-path', 'efa', 'efa-cache']:
        receipt = receipts.get('cloud-build-current', {})
        command = receipt.get('command', [])
        if (receipt.get('features') != ('experimental-fabric,test-utils' if args.profile in ['efa', 'efa-cache'] else 'experimental-distributed,test-utils')
                or command.count('--features') != 1
                or ('--features', receipt.get('features')) not in zip(command, command[1:])):
            build_errors['read-path-features'] = 'cold campaign does not verify the required frozen build features'
        if args.profile == 'efa-cache':
            help_path = root / 'capability-current.stdout.log'
            if not help_path.is_file() or any(flag not in help_path.read_text() for flag in
                                              ['--native-read-cache-bytes', '--hot-read-pass']):
                build_errors['cache-capabilities'] = 'current binary does not verify cache measurement controls'
    qualification = None
    if args.profile == 'efa-progress':
        qualification = validate_qualification(root)
        try:
            recorded_qualification = read_json(root / 'qualification-receipt.json')
            if not qualification['passed'] or recorded_qualification != qualification:
                raise ValueError('qualification receipt/evidence mismatch or failed qualification')
            names = set(qualification['case_ids'])
            if set(recorded[:12]) != names or any(name in names for name in recorded[12:]):
                raise ValueError('broader cases were admitted before full qualification')
        except (OSError, ValueError, KeyError, TypeError) as error:
            evidence_errors['qualification'] = str(error)
        for label in ('baseline', 'current'):
            receipt = receipts.get('cloud-build-' + label, {})
            if receipt.get('features') != 'experimental-fabric,test-utils':
                build_errors[label + '-efa-features'] = 'progress requires two frozen EFA builds'
        if receipts.get('cloud-build-baseline', {}).get('source_tree_sha256') != efa_progress.BASELINE_SOURCE_SHA256:
            build_errors['preserved-baseline'] = 'progress requires the preserved completed October 7 EFA source'
    if args.profile in ('checkpoint-async', 'cluster-ram', 'scalar-transport', 'transport-floor', 'stage-timing'):
        qualification = validate_qualification(root)
        try:
            profile = {'cluster-ram': cluster_ram, 'scalar-transport': scalar_transport,
                       'transport-floor': transport_floor, 'stage-timing': stage_timing, 'checkpoint-async': checkpoint_async}[args.profile]
            validate_checkpoint_campaign(root, receipts, recorded, qualification, profile)
        except (OSError, ValueError, KeyError, TypeError) as error:
            evidence_errors['checkpoint-campaign'] = str(error)
    evidence_errors.update(build_errors)
    controller_io = None
    try:
        controller_io = io_delta(read_json(root / "before.json"), read_json(root / "after.json"))
    except (OSError, ValueError, KeyError, TypeError) as error:
        evidence_errors["controller-disk-evidence"] = str(error)
    detailed_stages, spread_comparison = [], []
    if args.profile == 'stage-timing':
        try:
            detailed_stages = stage_timing.summarize_stages(runs)
            spread_comparison = stage_timing.compare_saved_spread(runs, args.spread_results)
        except (OSError, ValueError, KeyError, TypeError, tarfile.TarError) as error:
            evidence_errors['stage-timing-comparison'] = str(error)
    complete = (not invalid and not node_errors and not evidence_errors and not unexpected
                and (root / "COMPLETE").is_file())
    report = dict(complete=complete, profile=args.profile, expected_api_cases=len(expected), valid_api_cases=len(runs),
        expected_node_archives=5 * len({info[-1] for info in expected.values()}), verified_node_archives=len(nodes),
        cases_by_family=dict(Counter(info[0] for info, _ in runs.values())),
        invalid_or_missing_cases=invalid, invalid_or_missing_node_evidence=node_errors,
        invalid_or_missing_campaign_evidence=evidence_errors, unexpected_case_directories=unexpected,
        cloud_build_pair_verified=not build_errors,
        campaign_complete_marker=(root / "COMPLETE").is_file(),
        methodology=[
            "Frozen source f11eb057c71fb7ec149ce2827757a7041b68e47da6f6a6318ad2d7c3bacd6398 has a TCP label typo only in EFA cold read_io and cache-evicted remote-profile rows. Analysis normalizes those labels only for that source; raw files are unchanged. Explicit EFA command, correct summary/sample labels and actual device traffic are still required.",
            "Medians across recorded repetitions of each exact shape; headline ratios are unweighted medians across shapes, not pooled operation percentiles. Repetition counts remain explicit in comparison.csv; the read-path diagnostic supplement has one repetition.",
            "Native synchronous Value-WAL file/directory barrier is the matched control. Native async is absent. Capacity4m compares only current warm1m and cannot claim native fairness: mapping allowances60MiB versus15MiB.",
            "All summary fields and stage counters are retained. Profile p50/mean/sum/n are calculated within each run then compared; remote profiles are diagnostic. Their availability differs between source generations; no absence becomes zero.",
            "Mixed get/write share one full mixed-phase wall timer. Point-get-many measures up to32keys per API; write latency is one complete window, not one mutation. Single-sample phases do not establish p99 tails.",
            "Transport exchange includes TLS/socket or fabric/node service/scheduling. Actor/transport and dispatch/wait clocks are nested. Summed parallel worker/counter time is not wall time.",
            "Controller process RSS is the sampled main process only, never a six-host peak. Node VmHWM is a per-node lifetime high-water mark; their sum is not a simultaneous cluster peak. Where archived, node process-stat gives CPU ticks through the pre-stop sample, covering the whole case including idle time; it is not phase-specific CPU cost.",
            "Node disk evidence covers the entire node case including startup/replay/shutdown and idle native phase. Only exact mounted instance-store disks are accepted; diskstats sectors are512bytes. No IOPS, device latency or benchmark-phase attribution is inferred.",
            "Controller disk/swap evidence spans the whole campaign including source builds, transfers and every native/TCP run; it cannot isolate native storage activity. Source/binary receipts are retained provenance, not a fresh source rebuild by this analyzer.",
            "Build completeness requires isolated targets, distinct source/binary hashes, matching pair receipts, successful capability commands/help output and successful six-configuration preparation. Config contents are not archived here: their hashes attest the guest's preparation, not an independent local content verification.",
            "Complete means all expected benchmark cases and evidence passed these checks. EC2 termination and billing-resource cleanup require separate AWS control-plane verification.",
            "Warm page-cache reads do not establish cold-NVMe scaling. Whole-case disk counters cannot attribute reads to an API. Read-path, EFA and EFA-cache profiles require phase-specific read_io.csv evidence for cache-evicted phases. These bounded datasets still do not establish sustained larger-than-RAM scaling.",
            "EFA-cache compares current cache/storage families with the same native cache policy, retaining uncached controls. Frontend cache has an 8 MiB accounted allocation budget; native cache has an 8 MiB nominal value budget across 1024 per-cell LRUs, excluding key/metadata overhead. This is not equal-total-RAM evidence. Repeated hot phases are not assumed all-hit; retain hit/miss counters and native diagnostic hit observations."
        ],
        headline=[r for r in phase_summary if r["backend"] == "tcp" and r["phase"] in
                  {"load_write_window", "point_get", "point_get_many", "full_scan", "reopen"}
                  and r["metric"] in {"p50_ns", "phase_wall_ns", "instrumented_phase_ops_per_sec"}],
        remaining_bottlenecks=dict(load_stage_evidence=load_stages, scalar_profile_evidence=profiles,
            interpretation=["Compare validation/storage-wait/publication using the same shape; storage wait also includes remote decode/append/persistence and is not pure network time.",
                "A1MiBgroup fits only three32x8KiB batches. Compare capacity-group counts and wall time separately; aggregate five-node persistence calls do not represent globally serialized barriers.",
                "Large scalar verification cost remains CPU work even with independent disks. Transport exchange needs node-side timing to distinguish network from storage and scheduling."]),
        phase_summary=phase_summary, observed_wall_regressions_over10percent=regressions,
        controller_process_metrics=[r for r in rows if r["kind"] == "process"],
        controller_whole_campaign_io=controller_io,
        node_io_summary=dict(valid_whole_case_snapshots=len(nodes),
            node_case_snapshots_with_device_reads=sum(n["read_bytes"] > 0 for n in nodes),
            read_bytes_range=[min(n["read_bytes"] for n in nodes), max(n["read_bytes"] for n in nodes)] if nodes else None,
            write_bytes_range=[min(n["write_bytes"] for n in nodes), max(n["write_bytes"] for n in nodes)] if nodes else None,
            swap_in_pages=sum(n["pswpin_pages"] for n in nodes) if nodes else None,
            swap_out_pages=sum(n["pswpout_pages"] for n in nodes) if nodes else None))
    if args.profile == 'stage-timing':
        report.update(qualification=qualification, case_plan=stage_timing.plan(),
                      stage_timing=detailed_stages, historical_spread_ram_controls=spread_comparison)
        report['methodology'] += [
            'Stage-timing requires six C6id hosts in the exact owned cluster placement group/AZ. Source-bound baseline v5 and current v6 never mix peers.',
            'Stage counters are local monotonic nested/parallel sums with explicit call denominators, never additive wall slices, wire-only latency, or one-way delay.',
            'Baseline continuous RAM controls match the saved spread workload (1000 samples, 64 warmups, three repetitions, 1/8KiB, five shards). Cross-time spread/cluster comparison is observational, not causal placement isolation.']
    if args.profile == 'efa-progress':
        report.update(qualification=qualification, expected_main_api_cases=224,
                      expected_qualification_api_cases=12, expected_main_node_archives=560,
                      expected_qualification_node_archives=30,
                      current_resource_evidence=[r for r in rows if r['kind'] == 'resource'],
                      phase_physical_io=[r for r in rows if r['kind'] == 'physical_io_total'],
                      case_plan=efa_progress.plan(),
                      diagnostic_cases=[r for r in rows if r['comparison'] == 'diagnostic_only'])
        report['methodology'] += [
            'EFA-progress qualifies six EFA/native pairs before broader admission and revalidates every gate from archived evidence. Missing hardware counters, process inventory, physical I/O, cache policy, swap or actual-RAM bounds fail closed.',
            'Exact equivalent native controls are deliberately repeated, not deduplicated: native_control_ids identify equality across transport/server policies. Native concurrent ratios match application cache and read window; scalar phases remain serial. Previous source has no window measurements, so absent phases have no previous/current ratio.',
            'Source comparisons include explicitly labeled read-policy changes, not a claim of isolated code changes. Cache comparisons hold current read policies fixed. Focused cold controls compare mapped L1/S1/Q1 against each exact policy, not pooled worker budgets.',
            'Resource CPU deltas span named snapshots: loaded-to-point_get_window includes preceding serial reads. CPU/window-wall is only an upper bound, never phase CPU. Aggregate RSS is the sum of asynchronous per-process snapshots; summed process peaks are not a simultaneous cluster peak.',
            'Qualification and transport diagnostics are separate from headline timed comparisons. Cold evidence requires every process to have positive physical reads and no more than ten percent file residency before each eviction pass. Per-process peak RSS is bounded at eighty percent of that actual guest RAM.'
        ]
    if args.profile == 'checkpoint-async':
        report.update(qualification=qualification, checkpoint_scope=scope, case_plan=checkpoint_async.plan(scope),
                      recovery=[r for r in rows if r['kind'] == 'recovery'],
                      read_path_evidence=[r for r in rows if r['kind'] == 'read_path'])
        report['methodology'] += [
            'October 8 CURRENT engine is the immutable baseline. The identical hashed recovery-only overlay is compiled for both; checkpoint_recovery_stats is applied only to the current example, never its dependencies.',
            'Core is a whole implementation/configuration comparison: baseline mapped/4 lanes/1 worker versus current Async/1 lane/2 control workers; both native controls use 32 workers and window32. Five repetitions, alternating build order.',
            'Native controls remain paired to each transport. Equivalent copies share native_control_id and are not pooled as ten independent repetitions. Pressure runs have no native small-cache comparison; ratios reference cache-zero native.',
            'Core includes warm and actual cache-evicted scalar/grouped/window phases. Pressure uses warm/hot phases and requires enabled-tier misses and evictions, finite retained budgets, and WAL reads; no cache-fitting hot phase is described as uncached.',
            'Full-shard restart proves five replaced PIDs with fresh readiness and preserved data/absolute TTL. Surviving-shard reopen proves unchanged PIDs. OS cache is not claimed cold. Controller service restore includes SSM orchestration and verification and is not engine open latency.',
            'Thread CPU/context-switch evidence is sampled outside latency timers over observed thread lifetimes, not attributed to API phases. Each repetition retains raw samples and separate percentile/throughput metrics; paired deltas align exact repetition identities.'
        ]
    if args.profile == 'scalar-transport':
        report.update(qualification=qualification, checkpoint_scope=scope, case_plan=scalar_transport.plan(scope),
                      recovery=[r for r in rows if r['kind'] == 'recovery'],
                      cold_reopen=[r for r in rows if r['phase'] in ('reopen', 'reopen_recovery')],
                      reused_transport_reopen=[r for r in rows if r['phase'] in
                                               ('reopen_reused_transport', 'reopen_reused_transport_recovery')],
                      diagnostic_cases=[r for r in rows if r['comparison'] == 'diagnostic_only'],
                      read_path_evidence=[r for r in rows if r['kind'] == 'read_path'])
        report['headline'] = [r for r in report['headline'] if r['family'].startswith(('transport-core-', 'transport-scalar-'))]
        report['methodology'] += [
            'Scalar-transport preserves the October 11 RAM executable/source as baseline and requires the exact rebuilt scalar candidate source. Each revision runs its own matching nodes; protocol versions are never mixed.',
            'Three paired core repetitions compare native/TCP/EFA at 1/8 KiB with scalar and batch32/window32 shapes. Diagnostics and qualification are separate; correlated transport native controls are not pooled. No cache-capacity rerun.',
            'Current EFA qualification requires measured requested/selected TX/RX128, provider ABI206.0 from libfabric2.6.0, MTU and endpoint setup stages, plus real NIC progress and six owned hosts with verified instance-store disks. The pinned runtime enforces protocol/configuration gates; geometry is not inferred from instance type.',
            'Cold reopen and same-process transport-reused reopen have separate phases and complete audits. Reused transport is not crash recovery and has no new endpoint setup profile. Process recovery covers TCP and EFA, coordinator-only and full-process restart, tails0/128, both builds, one repetition; OS page cache is not claimed evicted.',
            'Unavailable baseline optional statistics/percentiles remain absent, never zero. Endpoint/setup worker sums overlap and cannot be added to cold/reused wall latency.'
        ]
    if args.profile == 'transport-floor':
        report.update(qualification=qualification, checkpoint_scope=scope, case_plan=transport_floor.plan(scope),
                      recovery=[r for r in rows if r['kind'] == 'recovery'],
                      transport_floor=[r for r in rows if r['kind'].startswith('floor')],
                      polling_and_resident_evidence=[r for r in rows if r['kind'] in ('read_path', 'node_profile')],
                      diagnostic_cases=[r for r in rows if r['comparison'] == 'diagnostic_only'])
        report['headline'] = [r for r in report['headline'] if r['family'].startswith(('floor-core-', 'floor-scalar-'))]
        report['methodology'] += [
            'Protocol-v5 current servers are never mixed with baseline servers. The preserved scalar-transport current source/executable is the new baseline; old campaign pins are unchanged.',
            'Three interleaved repetitions preserve scalar and batch32/window32, 1/8KiB, TCP/EFA and all four polling/resident policies. Only Adaptive/resident=false measures native controls; variants reference exact measured same-transport/workload/value/repetition native cases, not synthesized or duplicated artifacts.',
            'Eight persistent RAM floor processes each measure both values and three repetitions. Raw floor phases name value/repetition explicitly. Continuous1000 is unprofiled; idle2ms/256 is profiled and includes offered idle and scheduler delay only in wall throughput, not API latency. All samples validate the complete deterministic payload and five-node evidence requires zero WAL reads.',
            'Busy consumes established-session CPU; polling gaps are not packet-arrival timestamps. Worker/setup/profile clocks overlap and must not be summed into wall latency. Raw node polling, resident fallback, resource, physical IO, lifetime and floor counters remain in comparison.csv/analysis.json.',
            'Recovery retains coordinator-only/full-node TCP/EFA tails0/128 for both revisions. service-restore.json contains restart timing, not transport. Cold/reused reopen audits remain separate from process recovery.'
        ]
    report.update(receipts)
    (output / "analysis.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"complete": complete, "valid_api_cases": len(runs),
                      "verified_node_archives": len(nodes), "output": str(output)}))
    return 0 if complete or args.allow_partial else 1


if __name__ == "__main__":
    raise SystemExit(main())
