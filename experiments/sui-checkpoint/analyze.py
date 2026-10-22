#!/usr/bin/env python3
"""Offline, source-bound checkpoint analysis.

Single replay: ``python3 analyze.py PAIRED_REPLAY``.
Closed campaign: ``python3 analyze.py RUN --campaign``. RUN retains
controller-config.json, cleanup-verified.json, retrieved/{receipt.json,
controller.tar.gz,cells/.../storage/.../final/evidence.tar.gz}, and
paired-results/<cell>/{receipt.json,evidence.tar.gz}. No AWS APIs, workload
processes or additional serving instrumentation are used. Campaign output is
analysis-details.json, cell-summary.csv, all-wrapper-latencies.csv and
adapter-means.csv. Missing diagnostics are null with a reason, never zero.
"""
import argparse
from contextlib import contextmanager
import csv
import hashlib
import json
import math
from itertools import combinations
from pathlib import Path
import re
import statistics
import sys
sys.dont_write_bytecode = True
import tarfile
import importlib.util
from functools import cache
import build

CHECKPOINT_DIAGNOSTIC_PHASES = (
    'pressure_wait', 'drain', 'existing_capture_wait', 'new_capture_wait',
    'staging_manifest', 'receipt_history', 'receipt_history_sync', 'node_prefix_wait',
    'index_prepare', 'index_persist', 'metadata_install', 'owner_adoption', 'obsolete_cleanup')
CHECKPOINT_HISTORY_COUNTERS = (
    'history_stream_opens', 'history_previous_slot_reads', 'history_receipt_write_ops',
    'history_receipt_write_bytes', 'history_buffer_flushes', 'history_node_write_ops',
    'history_node_write_bytes', 'history_sync_operations')


def metrics(path):
    return parse_metrics(path.read_text())


def parse_metrics(text):
    result = {}
    for line in text.splitlines():
        if not line or line.startswith('#'):
            continue
        metric, value = line.rsplit(' ', 1)
        if metric.startswith(('db_op_', 'rocksdb_')) or any(word in metric for word in ('checkpoint', 'execution_cache', 'sharded', 'tidehunter', 'shared_version', 'marker', 'object')):
            try:
                if metric in result:
                    raise ValueError(f'duplicate Prometheus metric: {metric}')
                result[metric] = float(value)
                if not math.isfinite(result[metric]):
                    raise ValueError('nonfinite metric')
            except ValueError:
                raise ValueError(f'invalid Prometheus metric: {line}') from None
    return result



def checkpoint_sequence_cache_stats(values):
    result = {}
    expected = {'hits', 'misses', 'backend_gets', 'busy_bypasses'}
    for metric, value in values.items():
        if not metric.startswith('checkpoint_sequence_lookup_cache{'):
            continue
        labels = {name: json.loads('"' + encoded + '"') for name, encoded in
                  re.findall(r'(\w+)="((?:\\.|[^"\\])*)"', metric)}
        stat = labels.get('stat')
        if stat not in expected or stat in result or value < 0 or value != int(value):
            raise ValueError('invalid or duplicate checkpoint sequence-cache counter')
        result[stat] = int(value)
    if result and set(result) != expected:
        raise ValueError('incomplete checkpoint sequence-cache counter population')
    return result


def adapter_stats(values):
    result = {}
    for metric, value in values.items():
        if not metric.startswith('tidehunter_adapter{'):
            continue
        labels = {name:json.loads('"' + encoded + '"') for name,encoded in
                  re.findall(r'(\w+)="((?:\\.|[^"\\])*)"', metric)}
        if 'namespace' not in labels or 'stat' not in labels:
            raise ValueError('adapter metric lacks namespace/stat identity')
        stats = result.setdefault(labels['namespace'], {})
        if labels['stat'] in stats:
            raise ValueError('duplicate adapter namespace/stat')
        stats[labels['stat']] = value
    for stats in result.values():
        if stats.get('shard_sample_available') != 1:
            for name in list(stats):
                if name.startswith('shard_') and name != 'shard_sample_available':
                    stats[name] = None
        for name in ('scalar_read_ns', 'read_index_ns'):
            if name in stats and stats.get(name + '_available') != 1:
                stats[name] = None
        for name in list(stats):
            if name.startswith('read_profile_') and name.endswith('_ns'):
                unavailable = stats.get('read_profile_enabled') != 1
                if name.startswith('read_profile_server_'):
                    unavailable |= stats.get('read_profile_server_available') != 1
                if name.startswith('read_profile_transport_'):
                    unavailable |= stats.get('read_profile_transport_progress_available') != 1
                if unavailable:
                    stats[name] = None
            if (name.startswith('read_profile_transport_cq_') and name != 'read_profile_transport_cq_available'
                    and stats.get('read_profile_transport_cq_available') != 1):
                stats[name] = None
            if name.startswith('write_profile_') and name.endswith('_ns'):
                unavailable = stats.get('write_profile_available') != 1
                unavailable |= name == 'write_profile_connection_receive_frame_including_idle_ns'
                for prefix in ('write_profile_client_transport_', 'write_profile_transport_'):
                    if name.startswith(prefix + 'cq_'):
                        unavailable |= not stats.get(prefix + 'cq_timed_calls')
                if unavailable:
                    stats[name] = None
            if (name.startswith('write_checkpoint_diagnostic_') and name.endswith('_ns')
                    and stats.get('write_checkpoint_diagnostics_available') != 1):
                stats[name] = None
            if (name in {'write_checkpoint_diagnostic_' + counter for counter in CHECKPOINT_HISTORY_COUNTERS}
                    and stats.get('write_checkpoint_diagnostics_available') != 1):
                stats[name] = None
            if name.startswith('native_profile_') and name.endswith('_ns') and stats.get('native_profile_available') != 1:
                stats[name] = None
    return result

@cache
def checkpoint_matrix_helpers():
    path = Path(__file__).resolve().parents[1] / 'aws-sharded-wal/checkpoint_matrix.py'
    spec = importlib.util.spec_from_file_location('checkpoint_analysis_matrix', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def fabric_local_replay(archive, result):
    helpers = checkpoint_matrix_helpers()
    names = {member.name for member in archive.getmembers()}
    diagnostic = bool(result.get('read_profile_limit', 0) or result.get('write_profile_limit', 0))
    selected = ['experiment/native.log', 'experiment/remote.log']
    selected += sorted(name for name in names if re.fullmatch(r'experiment/storage-[0-9]+\.log', name))
    logs = []
    for name in selected:
        if name not in names:
            logs.append(dict(source=name, available=False, records=[], record_count=0, totals=None,
                             unavailable_reason='log not retained in this replay evidence'))
            continue
        evidence = helpers.fabric_local_archive_log(archive, name,
            forbidden=not diagnostic or name == 'experiment/native.log')
        logs.append(evidence)
    return dict(logs=logs,
                completeness='only nonzero finalized endpoint windows emitted before exit; no SIGTERM/Drop completeness claim',
                scope='local nonwire data/credit post deltas; not packets per RPC, RTT, payload packetization or latency')


@contextmanager
def evidence_archive(root):
    receipt = json.loads((root/'receipt.json').read_text())
    if receipt.get('complete') is not True:
        raise ValueError('failed/incomplete replay is not a benchmark result')
    if build.digest(root/'evidence.tar.gz') != receipt['evidence_sha256']:
        raise ValueError('evidence archive binding differs')
    with tarfile.open(root/'evidence.tar.gz', 'r:gz') as archive:
        names = set()
        for member in archive.getmembers():
            if member.name in names or not member.isfile() or '..' in Path(member.name).parts or Path(member.name).is_absolute():
                raise ValueError('ambiguous or unsafe evidence archive member')
            names.add(member.name)
        yield archive


def archived_bytes(archive, name):
    try:
        member = archive.getmember(name)
    except KeyError:
        raise ValueError(f'required evidence is missing: {name}') from None
    with archive.extractfile(member) as stream:
        return stream.read()


def archived_json(archive, name):
    return json.loads(archived_bytes(archive, name))


def verified_replay(root):
    receipt = json.loads((root/'receipt.json').read_text())
    with evidence_archive(root) as archive:
        result = archived_json(archive, 'experiment/results.json')
        audit = archived_json(archive, 'experiment/audit.json')
        foreground_by_mode = {mode:parse_metrics(archived_bytes(archive, f'experiment/{mode}-foreground.prom').decode())
                              for mode in ('native', 'remote')}
        build_bytes = archived_bytes(archive, 'provenance/build-manifest.json')
        manifest = json.loads(build_bytes)
        if (hashlib.sha256(build_bytes).hexdigest() != receipt['build_manifest_sha256']
                or result['build_manifest_sha256'] != receipt['build_manifest_sha256']
                or result['source_archive_sha256'] != receipt['source_archive_sha256']
                or manifest['source_archive_sha256'] != receipt['source_archive_sha256']
                or hashlib.sha256(archived_bytes(archive, 'provenance/source-manifest.json')).hexdigest()
                   != manifest['source_manifest_sha256']):
            raise ValueError('archived source/build/result provenance differs')
    if result.get('complete') is not True or result.get('failure'):
        raise ValueError('failed/incomplete replay is not a benchmark result')
    if audit!=result['audit'] or audit.get('all_equal') is not True or not audit['databases'] or not audit['rocksdb']:
        raise ValueError('complete exact final-state audit is required')
    if 'external_manifest' in result:
        from run import validate_resources
        binding = dict(config_sha256=result['external_manifest']['config_sha256'],
                       build_manifest_sha256=result['build_manifest_sha256'],
                       checkpoint_digest=result['checkpoint_digest'])
        validate_resources(result['external_resources'], result['external_manifest'], binding, result['external_stages'])
    for replay in (result['native'], result['remote']):
        if replay['completed_checkpoint_digest'] != result['checkpoint_digest']:
            raise ValueError('completed replay digest differs')
    if any(comparison.get('equal') is not True for comparison in audit['databases'].values()):
        raise ValueError('selected Tidehunter database differs')
    observed = adapter_stats(foreground_by_mode['remote'])
    if set(observed) != set(audit['databases']) or any(stats.get('shard_sample_available') != 1 for stats in observed.values()):
        raise ValueError('remote physical shard observations are unavailable')
    return receipt, result, audit, foreground_by_mode


def summarize(root):
    verified = verified_replay(root)
    summary = summarize_verified(verified)
    with evidence_archive(root) as archive:
        summary['fabric_local_diagnostics'] = fabric_local_replay(archive, verified[1])
    return summary


def summarize_verified(verified):
    _, result, audit, foreground_by_mode = verified
    summary=dict(schema=1,checkpoint_limit=result['checkpoint_limit'],checkpoint_digest=result['checkpoint_digest'],
        source_range=result['source_range'],source_archive_sha256=result['source_archive_sha256'],
        build_manifest_sha256=result['build_manifest_sha256'],topology=result['topology'],
        durability=result['durability'],cache=result['cache'],
        read_profile_limit=result.get('read_profile_limit', 0),
        write_profile_limit=result.get('write_profile_limit', 0),modes={},table_counts={})
    summary['native_write_profile_limit'] = result.get('native_write_profile_limit', 0)
    for mode in ('native','remote'):
        replay=result[mode]
        foreground=foreground_by_mode[mode]
        summary['modes'][mode]=dict(foreground_seconds=replay['elapsed_seconds_including_startup'],
            phase_timing=replay['phase_timing'],sampled_completion_window=replay['completed_window'],
            sampled_rss_peak_bytes=replay['peak_sampled_rss_bytes'],
            process_io_final=replay['process_io_final'],
            host_swap_delta_pages={key:replay['host_after']['swap_pages'][key]-replay['host_before']['swap_pages'][key]
                for key in ('pswpin','pswpout')},foreground_metrics=foreground)
        summary['modes'][mode]['adapter_by_namespace'] = adapter_stats(foreground)
        summary['modes'][mode]['checkpoint_sequence_cache_counts'] = checkpoint_sequence_cache_stats(foreground)
        summary['modes'][mode]['execution_cache_api_calls'] = {k:v for k,v in foreground.items() if k.startswith('execution_cache_db_read_calls')}
        summary['modes'][mode]['execution_cache_keys'] = {k:v for k,v in foreground.items() if k.startswith('execution_cache_requests')}
    summary['remote_over_native']=summary['modes']['remote']['foreground_seconds']/summary['modes']['native']['foreground_seconds']
    for namespace, comparison in audit['databases'].items():
        summary['table_counts'][namespace]={name:entry['records'] for name,entry in comparison['native']['tables'].items()}
    summary['limits']=[
        'Foreground watermark sampling is 250ms resolution, not individual checkpoint latency.',
        'Execution-start log spacing is scheduling/start timing, never completion/service p99.',
        'execution_cache_db_read_calls counts AuthorityStore fallback API calls; execution_cache_requests counts keys, neither means transport RPCs.',
        'object_by_version fallback includes get and exists; cache hits may suppress physical reads.',
        'OS read_bytes/write_bytes are charged physical I/O, not logical WAL member bytes; mapped/page-cache access may charge differently.',
        'Host swap and diskstats include unrelated processes; per-process RSS maxima are sampled.',
        'Native cache shape and remote coordinator/shard budgets are recorded separately, not equal-total-memory claims.',
        'No exact-version readiness prefetch: readiness returns no payload and DB reads do not populate the execution cache.',
        'Transport recheck/output spans overlap exchange intervals; no one-way network or pure NVMe attribution.',
        'Bounded read diagnostics sample admitted inline/actor RPCs, including startup/scans; cache hits and absent keys are outside that population.',
        'Write diagnostics sample bounded first append RPCs per owning connection; collection/control calls are diagnostic overhead, not clean-path work.',
        'Native diagnostics sample the first configured foreground durability barriers per DB, not the distributed per-connection population.',
        'Native file spans synchronize every registered file; distributed file spans synchronize the exact containing file. Namespace frequency is reported separately.',
        'Owner storage_wait is completion-notification waiting, not fsync; durable-publication waiting and checkpoint spans are reported separately.',
        'Scalar/bulk/commit totals and index/actor/exchange/server spans overlap; cumulative sums are not additive wall-clock fractions.',
        'Transport diagnostic sweeps can include interleaved unsampled work; use each output-stage sample count, never divide all transport counters by RPC count.',
        'TCP completion-queue counters and disabled/absent profiled timing are unavailable, not zero-latency measurements.',
        'Full final audit covers all rows and multiplicity, including markers/shared-version/object tables; no Byzantine proof.'
    ]
    return summary


def nonnegative(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise ValueError(f'invalid nonnegative measurement: {name}')
    return value


def unavailable(reason):
    return dict(available=False, reason=reason, mean_us=None)


def mean_span(stats, numerator, denominator, flags=()):
    """A mean always names its observed population; absent/empty is not zero."""
    result = dict(numerator=numerator, denominator=denominator,
                  total_ns=stats.get(numerator), count=stats.get(denominator))
    if any(stats.get(flag) != 1 for flag in flags):
        return result | unavailable('diagnostic availability flag absent or disabled')
    total, count = result['total_ns'], result['count']
    if total is None or count is None:
        return result | unavailable('numerator or population counter missing')
    nonnegative(total, numerator)
    nonnegative(count, denominator)
    if count != int(count):
        raise ValueError('nonintegral timing population: ' + denominator)
    if count == 0:
        if total != 0:
            raise ValueError('nonzero timing with empty population: ' + numerator)
        return result | unavailable('empty observed population')
    return result | dict(available=True, reason=None, mean_us=total / count / 1000)


def multiplexed_profile_mean(stats, field, denominator, flags=(), prefix=''):
    """TcpReadProfile protocol-v2 means, preserving raw counters and totals.

    TCP, EFA TLS and EFA native share these multiplexed protocol fields.
    Compatibility clocks are not observations, even with a positive population.
    Other zero-valued clocks retain ordinary measured/empty-population semantics.
    """
    row = mean_span(stats, prefix + field, prefix + denominator, flags)
    if field in ('request_send_ns', 'response_header_ns', 'response_body_ns'):
        row.update(unavailable(
            'multiplexed protocol v2 (TCP/EFA TLS/EFA native): per-request '
            'request-write/response-header/body clocks are not attributable; '
            'socket I/O is connection-scoped, not observed zero latency'))
    elif field == 'server_inventory_wait_ns':
        row.update(unavailable(
            'multiplexed protocol v2 TcpReadProfile: server inventory wait is a '
            'compatibility field after reader authority moved to a separate lock; '
            'not an observed wait (TCP/EFA TLS/EFA native)'))
    return row


def adapter_means(stats):
    """Use observed inline/actor populations, never guess historical splits.

    Actor queue/service sums omit failed profiles, but errors are not split by
    path. Their means are unavailable on any sampled error. Inline elapsed
    includes failed calls. Wire/server sums describe successful profiles only,
    not all read_rpcs or all returned keys.
    """
    result = {}
    stages = {'write_gate_wait', 'scalar_read', 'bulk_read', 'write_commit',
              'read_submit', 'read_admission_wait', 'read_detached_wait', 'read_finish'}
    stages.update(name[:-3] for name in stats if name.endswith('_ns') and name[:-3] + '_calls' in stats)
    for stage in sorted(stages):
        flags = (('write_profile_available',) if stage.startswith('write_profile_') else
                 ('native_profile_available',) if stage.startswith('native_profile_') else
                 () if stage == 'write_gate_wait' else ('remote_timing_available',))
        if stage in ('scalar_read', 'read_index'):
            flags += (stage + '_ns_available',)
        result[stage] = mean_span(stats, stage + '_ns', stage + '_calls', flags)
    profile_denominators = dict(
        inline='inline_completed', actor_queue='actor_completed',
        actor_service='actor_completed', actor_return='returned',
        request_encode='wire_requests', exchange='wire_requests',
        request_send='wire_requests', response_header='wire_requests',
        response_body='wire_requests',
        response_decode='wire_requests', record_verify='wire_requests',
        server_handler='wire_requests', server_inventory_wait='wire_requests',
        server_authority_lookup='wire_requests', server_record_read='wire_requests',
        server_response_recheck_wait='server_response_recheck_samples',
        server_response_recheck='server_response_recheck_samples',
        transport_output_queue='transport_output_queue_samples',
        transport_output_submit='transport_output_submit_samples',
        transport_output_retire='transport_output_retire_samples',
        transport_cq_call='transport_cq_timed_calls')
    for stage, denominator in profile_denominators.items():
        flags = ['read_profile_enabled']
        if stage.startswith('server_'):
            flags.append('read_profile_server_available')
        if stage.startswith('transport_'):
            flags.append('read_profile_transport_progress_available')
        if stage.startswith('transport_cq_'):
            flags.append('read_profile_transport_cq_available')
        row = multiplexed_profile_mean(
            stats, stage + '_ns', denominator, flags, prefix='read_profile_')
        if stage in ('actor_queue', 'actor_service') and stats.get('read_profile_errors') != 0:
            row.update(unavailable('sampled errors absent or nonzero; successful actor population cannot be isolated'))
        result['read_profile_' + stage] = row
    for stage, denominator in (
            ('write_checkpoint', 'write_checkpoint_count'),
            ('write_foreground_checkpoint', 'write_foreground_checkpoint_count'),
            ('write_durable_receipt_publication_blocked', 'write_durable_receipt_publication_blocked_batches')):
        result[stage] = mean_span(stats, stage + '_ns', denominator, ('remote_timing_available',))
    for phase in CHECKPOINT_DIAGNOSTIC_PHASES:
        stage = 'write_checkpoint_diagnostic_' + phase
        result[stage] = mean_span(stats, stage + '_ns', stage + '_count',
                                  ('write_checkpoint_diagnostics_available',))
    write_denominators = {
        **{f'connection_append_{name}': 'sampled_requests' for name in
           ('inventory_wait', 'inventory_residence', 'decode', 'group_build', 'wal', 'persist')},
        **{f'connection_{name}': 'connection_requests' for name in
           ('request_decode', 'handler', 'response_encode', 'response_send')},
        **{f'client_{name}': 'client_requests' for name in
           ('request_encode', 'exchange', 'request_send', 'response_header', 'response_body', 'response_decode')},
        'client_server_inventory_wait': 'client_requests',
        **{f'storage_append_{name}': 'storage_append_calls' for name in
           ('total', 'state_lock_wait', 'validation', 'history', 'wal', 'publication')},
        **{f'storage_persist_{name}': 'storage_persist_calls' for name in
           ('total', 'serialization_lock_wait', 'state_lock_wait', 'validation', 'publication')},
        'storage_persist_wal_file': 'storage_file_persist_calls',
        'storage_persist_namespace': 'storage_namespace_persist_calls',
        'storage_persist_inventory_file': 'storage_inventory_file_persist_calls',
        'storage_persist_inventory_namespace': 'storage_inventory_namespace_persist_calls',
        'connection_control_queue': 'connection_control_queue_samples',
        'connection_control_execution': 'connection_control_execution_samples',
        'polling_request_first_poll_gap': 'polling_request_first_poll_samples',
    }
    for prefix in ('client_transport', 'transport'):
        write_denominators.update({
            **{f'{prefix}_output_{stage}': f'{prefix}_output_{stage}_samples'
               for stage in ('queue', 'submit', 'retire')},
            f'{prefix}_cq_call': f'{prefix}_cq_timed_calls',
            f'{prefix}_cq_error_call': f'{prefix}_cq_error_calls',
            f'{prefix}_cq_poll_gap': f'{prefix}_cq_poll_gap_samples',
        })
    for stage, denominator in write_denominators.items():
        if stage.startswith('client_'):
            row = multiplexed_profile_mean(
                stats, stage.removeprefix('client_') + '_ns',
                denominator.removeprefix('client_'), ('write_profile_available',),
                prefix='write_profile_client_')
        else:
            row = mean_span(stats, 'write_profile_' + stage + '_ns',
                            'write_profile_' + denominator, ('write_profile_available',))
        result['write_profile_' + stage] = row
    return result


def wrapper_latencies(values):
    """All typed-store latency histograms, including empty populations.

    Quantiles are cumulative-histogram bucket bounds in microseconds, NOT raw
    sample quantiles. The unbounded +Inf upper endpoint is represented by null.
    No interpolation assumes a distribution within a bucket.
    P99 additionally requires at least 100 observations in this histogram; cells
    and other histogram populations are never pooled to satisfy that threshold.
    """
    groups = {}
    for metric, value in values.items():
        match = re.fullmatch(r'(rocksdb_\w+_latency_seconds)_(sum|count|bucket)(?:\{(.*)\})?', metric)
        if not match:
            continue
        name, kind, encoded = match.groups()
        labels = {k: json.loads('"' + v + '"') for k, v in
                  re.findall(r'(\w+)="((?:\\.|[^"\\])*)"', encoded or '')}
        bound = labels.pop('le', None)
        key = (name, tuple(sorted(labels.items())))
        group = groups.setdefault(key, {})
        nonnegative(value, metric)
        if kind == 'bucket':
            if bound is None:
                raise ValueError('histogram bucket lacks le')
            bound = float(bound)
            if math.isnan(bound) or bound < 0:
                raise ValueError('invalid histogram bound')
            buckets = group.setdefault('buckets', {})
            if bound in buckets:
                raise ValueError('duplicate histogram bucket')
            buckets[bound] = value
        else:
            if bound is not None or kind in group:
                raise ValueError('duplicate or malformed histogram sum/count')
            group[kind] = value
    rows = []
    for (name, labels), group in sorted(groups.items()):
        count, total = group.get('count'), group.get('sum')
        row = dict(metric=name, labels=dict(labels), count=count, total_seconds=total,
                   mean_us=None, quantile_method='cumulative histogram bucket bounds, not raw quantiles',
                   quantiles={})
        if count is None or total is None:
            row.update(available=False, reason='histogram count or sum missing')
        elif count != int(count):
            raise ValueError('nonintegral histogram population')
        elif count == 0:
            if total != 0 or any(group.get('buckets', {}).values()):
                raise ValueError('nonzero histogram with empty population')
            row.update(available=False, reason='empty observed histogram population')
        else:
            row.update(available=True, reason=None, mean_us=total / count * 1e6)
        buckets = sorted(group.get('buckets', {}).items())
        if buckets:
            if any(n != int(n) for _, n in buckets) or any(a[1] > b[1] for a, b in zip(buckets, buckets[1:])):
                raise ValueError('nonintegral or decreasing cumulative histogram')
            if count is not None and (any(n > count for _, n in buckets) or
                    (math.isinf(buckets[-1][0]) and buckets[-1][1] != count)):
                raise ValueError('histogram bucket/count mismatch')
        complete = bool(buckets and math.isinf(buckets[-1][0]) and count is not None)
        for percentile in (50, 95, 99):
            quantile = dict(available=False, lower_us=None, upper_us=None,
                            upper_unbounded=False, reason='empty or incomplete histogram population')
            if percentile == 99 and count is not None and 0 < count < 100:
                quantile['reason'] = 'fewer than 100 observations; p99 unavailable'
            elif complete and count > 0:
                lower = 0.0
                for upper, cumulative in buckets:
                    if cumulative >= count * percentile / 100:
                        quantile = dict(available=True, lower_us=lower * 1e6,
                                        upper_us=upper * 1e6 if math.isfinite(upper) else None,
                                        upper_unbounded=math.isinf(upper), reason=None)
                        break
                    lower = upper
            row['quantiles'][f'p{percentile}'] = quantile
        rows.append(row)
    return rows


def validate_closure(closure, cfg):
    """Validate retained independent absence observations, not a teardown request."""
    run_id = cfg['run_id']
    hosts = {cfg['coordinator_instance_id'], *(node['instance_id'] for node in cfg['nodes'])}
    identities = {run_id + '-controller', *(run_id + f'-storage-{n["partition"]}' for n in cfg['nodes'])}
    if (closure.get('run_id') != run_id or not closure.get('verified_utc')
            or len(hosts) != 6 or closure.get('instances') != dict.fromkeys(hosts, 'terminated')
            or closure.get('interfaces') != [] or closure.get('volumes') != []
            or closure.get('bucket') != '404'
            or closure.get('placement_group') != 'InvalidPlacementGroup.Unknown'
            or closure.get('security_group') != 'InvalidGroup.NotFound'
            or any(closure.get(kind) != dict.fromkeys(identities, 'NoSuchEntity') for kind in ('roles', 'profiles'))):
        raise ValueError('complete independent owned-resource closure is required before analysis')


def validate_archive_members(archive):
    names = set()
    for member in archive.getmembers():
        if (member.name in names or not (member.isfile() or member.isdir())
                or '..' in Path(member.name).parts or Path(member.name).is_absolute()):
            raise ValueError('ambiguous or unsafe campaign archive member')
        names.add(member.name)


def verified_campaign(root):
    """Revalidate local receipts against immutable controller/replay/node archives."""
    matrix = checkpoint_matrix_helpers()
    closure = json.loads((root / 'cleanup-verified.json').read_text())
    local_cfg = json.loads((root / 'controller-config.json').read_text())
    # Input config gains coordinator identity only inside the archive. Check
    # basic closure before reading it, then check exact identities below.
    if (closure.get('run_id') != local_cfg['run_id'] or not closure.get('instances')
            or any(state != 'terminated' for state in closure['instances'].values())):
        raise ValueError('owned instances must be closed before analysis')
    receipt = json.loads((root / 'retrieved/receipt.json').read_text())
    archive_path = root / 'retrieved/controller.tar.gz'
    if (receipt.get('complete') is not True or receipt.get('run_id') != local_cfg['run_id']
            or build.digest(archive_path) != receipt.get('sha256')):
        raise ValueError('controller archive receipt/provenance differs')
    verified = []
    with tarfile.open(archive_path, 'r:gz') as archive:
        validate_archive_members(archive)
        cfg = archived_json(archive, 'results/campaign-config.json')
        if any(cfg.get(k) != v for k, v in local_cfg.items()):
            raise ValueError('archived campaign configuration differs')
        validate_closure(closure, cfg)
        if receipt.get('build_manifest_sha256') != cfg['build_manifest_sha256'] or receipt.get('checkpoint_digest') != cfg['checkpoint_digest']:
            raise ValueError('controller source/checkpoint binding differs')
        manifest_bytes = archived_bytes(archive, 'results/build-manifest.json')
        manifest = json.loads(manifest_bytes)
        if (hashlib.sha256(manifest_bytes).hexdigest() != cfg['build_manifest_sha256']
                or manifest['source_archive_sha256'] != cfg['source_archive_sha256']
                or manifest['source_manifest_sha256'] != cfg['source_manifest_sha256']
                or manifest['binaries'] != cfg['binaries']
                or hashlib.sha256(archived_bytes(archive, 'results/source-manifest.json')).hexdigest()
                   != cfg['source_manifest_sha256']):
            raise ValueError('controller source/build/binary provenance differs')
        campaign = archived_json(archive, 'results/campaign-receipt.json')
        if campaign.get('run_id') != cfg['run_id'] or campaign.get('complete') is not True or campaign.get('failures'):
            raise ValueError('campaign is incomplete')
        matrix.validate_matrix(cfg, campaign['cells'])
        placement = archived_json(archive, 'results/placement-proof.json')
        matrix.validate_placement(cfg, placement['instances'], placement['groups'])
        for cell in campaign['cells']:
            cell_id = cell['cell_id']
            prefix = f'results/cells/{cell_id}/'
            if archived_json(archive, prefix + 'cell-receipt.json') != cell:
                raise ValueError('controller cell receipt differs')
            replay = verified_replay(root / 'paired-results' / cell_id)
            replay_receipt, result, _, _ = replay
            if (replay_receipt != archived_json(archive, prefix + 'replay-receipt.json')
                    or replay_receipt['evidence_sha256'] != cell['replay_evidence_sha256']
                    or result['source_archive_sha256'] != cfg['source_archive_sha256']
                    or result['build_manifest_sha256'] != cfg['build_manifest_sha256']
                    or result['checkpoint_digest'] != cfg['checkpoint_digest']
                    or result.get('read_profile_limit') != cell['read_profile_limit']
                    or result.get('write_profile_limit', 0) != cell.get('write_profile_limit', 0)
                    or result.get('native_write_profile_limit', 0) != cell.get('native_write_profile_limit', 0)
                    or result.get('checkpoint_limit') != cfg['checkpoint_limit']
                    or result.get('cache') != dict(coordinator_bytes=cell['coordinator_cache_mib'] * 1024**2,
                                                   shard_bytes=cell['shard_cache_mib'] * 1024**2)
                    or result.get('external_manifest', {}).get('transport') != cell['transport']
                    or result.get('external_manifest', {}).get('config_sha256') != cell['config_sha256']):
                raise ValueError('paired replay cell/source/population binding differs')
            resources = archived_bytes(archive, prefix + 'resources.json')
            if (hashlib.sha256(resources).hexdigest() != cell['resources_sha256']
                    or json.loads(resources) != result.get('external_resources')):
                raise ValueError('cell resource archive binding differs')
            for node in cfg['nodes']:
                node_path = root / 'retrieved/cells' / cell_id / 'storage' / str(node['partition']) / 'final/evidence.tar.gz'
                if build.digest(node_path) != cell['node_archives'][node['instance_id']]['sha256']:
                    raise ValueError('retained node archive binding differs')
            verified.append((cell, replay))
    return cfg, closure, receipt, verified


def storage_io(result):
    """Physical process IO over enclosing healthy samples; never logical WAL IO."""
    if 'external_resources' not in result or 'external_stages' not in result:
        return dict(available=False, reason='external storage resource population absent')
    start, finish = [stage['unix_ns'] for stage in result['external_stages'][:2]]
    hosts = {}
    for name, host in result['external_resources']['hosts'].items():
        healthy = [row for row in host['samples'] if row.get('healthy') is True]
        before = [row for row in healthy if row['unix_ns'] <= start]
        after = [row for row in healthy if row['unix_ns'] >= finish]
        if not before or not after:
            raise ValueError('storage physical IO does not bracket foreground')
        first, last = before[-1], after[0]
        deltas = {}
        initial = {p['pid']: p for p in first['processes']}
        for process in last['processes']:
            previous = initial[process['pid']]
            if process['starttime_ticks'] != previous['starttime_ticks']:
                raise ValueError('physical IO process identity changed')
            for field in ('read_bytes', 'write_bytes'):
                delta = process['io'][field] - previous['io'][field]
                nonnegative(delta, 'physical IO delta')
                deltas[field] = deltas.get(field, 0) + delta
        hosts[name] = dict(available=True, process_io_delta=deltas,
                           sample_start_unix_ns=first['unix_ns'], sample_end_unix_ns=last['unix_ns'],
                           sample_bracket_seconds=(last['unix_ns'] - first['unix_ns']) / 1e9,
                           host_swap_delta_pages={k: host['swap_after'][k] - host['swap_before'][k]
                                                  for k in ('pswpin', 'pswpout')})
    return dict(available=True, hosts=hosts,
                scope='owned storage processes, enclosing foreground samples; host swap covers full host interval')


def population_comparison(cells):
    """Observed sample distributions, not a causal or zero-overhead claim."""
    if len({json.dumps(cell.get('cache'), sort_keys=True) for cell in cells}) > 1:
        raise ValueError('cannot pool different cache budgets')
    for field in ('source_range', 'source_archive_sha256', 'source_manifest_sha256',
                  'build_manifest_sha256', 'checkpoint_digest', 'durability', 'table_counts'):
        if len({json.dumps(cell.get(field), sort_keys=True) for cell in cells}) > 1:
            raise ValueError('cannot pool different source/workload populations: ' + field)
    result = {}
    for transport in sorted({cell['transport'] for cell in cells}):
        selected = [cell for cell in cells if cell['transport'] == transport]
        if len({(cell['read_profile_limit'], cell.get('write_profile_limit', 0), cell.get('native_write_profile_limit', 0))
                for cell in selected if any(cell.get(key, 0) for key in
                    ('read_profile_limit', 'write_profile_limit', 'native_write_profile_limit'))}) > 1:
            raise ValueError('cannot pool different bounded profiling limits')
        groups = {}
        for population in ('clean', 'profiled'):
            rows = [cell for cell in selected if cell['population'] == population]
            groups[population] = dict(
                n=len(rows), cells=[row['cell_id'] for row in rows],
                ordinals=[row['ordinal'] for row in rows],
                read_profile_limits=[row['read_profile_limit'] for row in rows],
                write_profile_limits=[row.get('write_profile_limit', 0) for row in rows],
                native_write_profile_limits=[row.get('native_write_profile_limit', 0) for row in rows])
            for field in ('native_seconds', 'remote_seconds', 'remote_over_native'):
                samples = [row[field] for row in rows]
                groups[population][field] = dict(samples=samples,
                    median=statistics.median(samples) if samples else None,
                    minimum=min(samples) if samples else None, maximum=max(samples) if samples else None)
        effects = {}
        for field in ('native_seconds', 'remote_seconds', 'remote_over_native'):
            clean, profiled = [groups[p][field]['median'] for p in ('clean', 'profiled')]
            effects[field] = (dict(available=True, profiled_over_clean=profiled / clean,
                                  observed_change_percent=(profiled / clean - 1) * 100)
                              if clean and profiled is not None else
                              dict(available=False, reason='clean or profiled population missing',
                                   profiled_over_clean=None, observed_change_percent=None))
        result[transport] = dict(populations=groups, observed_profile_effect=effects,
            caveats=[
                'Ratio of population medians, not paired causal profiling overhead or statistical significance.',
                'Native controls are retained per cell and never pooled across transport labels.',
                'Small samples, nonrandom order, startup, cache state, background work and time drift confound the change.',
                'Clean requires all native and remote diagnostic limits disabled; historical native internal spans remain unavailable.',
                'Read profiles are bounded first-admitted inline/actor groups, including startup/scans; not random steady-state samples.',
                'Write profiles are bounded first append requests per owning connection, not every write or random steady-state samples.',
                'No zero-overhead claim; normalization by each native control does not remove all confounding.'])
    return result


def transport_comparison(cells):
    pairs = []
    transports = {cell['transport'] for cell in cells}
    if transports - {'tcp', 'efa', 'efa-direct', 'efa-native'}:
        raise ValueError('unknown transport comparison label')
    order = ['tcp', 'efa'] + [name for name in ('efa-direct', 'efa-native') if name in transports]
    comparisons = [(reference, candidate) for reference, candidate in combinations(order, 2)
                   if reference in transports or candidate in transports]
    for repetition in sorted({cell['repetition'] for cell in cells}):
        selected = [cell for cell in cells if cell['repetition'] == repetition]
        by_transport = {cell['transport']: cell for cell in selected}
        if len(by_transport) != len(selected):
            raise ValueError('duplicate transport/repetition cell')
        for reference_transport, candidate_transport in comparisons:
            pair = dict(repetition=repetition, reference_transport=reference_transport,
                        candidate_transport=candidate_transport,
                        comparison=f'{candidate_transport}-over-{reference_transport}')
            if reference_transport not in by_transport or candidate_transport not in by_transport:
                pairs.append(pair | dict(available=False, reason='matched transport pair missing'))
                continue
            reference, candidate = by_transport[reference_transport], by_transport[candidate_transport]
            if any(reference.get(field, 0) != candidate.get(field, 0) for field in
                   ('read_profile_limit', 'write_profile_limit', 'native_write_profile_limit')):
                raise ValueError('transport pair profiling populations differ')
            if reference.get('cache') != candidate.get('cache'):
                raise ValueError('transport pair cache budgets differ')
            for field in ('population', 'source_range', 'source_archive_sha256', 'source_manifest_sha256',
                          'build_manifest_sha256', 'checkpoint_digest', 'durability', 'table_counts'):
                if reference.get(field) != candidate.get(field):
                    raise ValueError('transport pair source/workload populations differ: ' + field)
            pairs.append(pair | dict(available=True, population=reference['population'],
                cache=reference.get('cache'), source_range=reference.get('source_range'),
                reference_cell=reference['cell_id'], candidate_cell=candidate['cell_id'],
                reference_native_seconds=reference['native_seconds'],
                candidate_native_seconds=candidate['native_seconds'],
                candidate_over_reference_native=candidate['native_seconds'] / reference['native_seconds'],
                transport_order=[cell['transport'] for cell in sorted((reference, candidate), key=lambda row: row['ordinal'])],
                candidate_over_reference_elapsed=candidate['remote_seconds'] / reference['remote_seconds'],
                candidate_over_reference_native_normalized=candidate['remote_over_native'] / reference['remote_over_native']))
    return dict(pairs=pairs, caveat='Sequential matched repetitions, not simultaneous or randomized causal transport isolation.')


def compare_campaign(root):
    """Return a source-bound report only after verified resource closure."""
    root = Path(root)
    cfg, closure, receipt, verified = verified_campaign(root)
    cells = []
    for identity, replay in verified:
        summary = summarize_verified(replay)
        with evidence_archive(root / 'paired-results' / identity['cell_id']) as archive:
            fabric = fabric_local_replay(archive, replay[1])
        fabric['storage'] = []
        helpers = checkpoint_matrix_helpers()
        diagnostic = bool(identity['read_profile_limit'] or identity.get('write_profile_limit', 0))
        for node in cfg['nodes']:
            path = root / 'retrieved/cells' / identity['cell_id'] / 'storage' / str(node['partition']) / 'final/evidence.tar.gz'
            with tarfile.open(path, 'r:gz') as archive:
                names = sorted(member.name for member in archive.getmembers()
                    if re.fullmatch(r'cloud-node-logs/namespace-[0-9]+\.stderr\.log', member.name))
                if not names:
                    raise ValueError('Sui node archive omitted namespace stderr logs')
                logs = [helpers.fabric_local_archive_log(archive, name, forbidden=not diagnostic)
                        for name in names]
            fabric['storage'].append(dict(partition=node['partition'], archive=str(path), logs=logs))
        _, raw, _, _ = replay
        row = {k: identity[k] for k in ('cell_id', 'transport', 'ordinal', 'repetition', 'read_profile_limit')}
        row['write_profile_limit'] = identity.get('write_profile_limit', 0)
        row['native_write_profile_limit'] = identity.get('native_write_profile_limit', 0)
        row.update({key: cfg[key] for key in ('source_archive_sha256', 'source_manifest_sha256',
                                             'build_manifest_sha256', 'checkpoint_digest')})
        row.update(population='profiled' if any(row[key] for key in
                       ('read_profile_limit', 'write_profile_limit', 'native_write_profile_limit')) else 'clean',
                   native_seconds=summary['modes']['native']['foreground_seconds'],
                   remote_seconds=summary['modes']['remote']['foreground_seconds'],
                   remote_over_native=summary['remote_over_native'],
                   replay_evidence_sha256=identity['replay_evidence_sha256'],
                   resources_sha256=identity['resources_sha256'], efa_activity=identity['efa'],
                   node_archives=identity['node_archives'], source_range=summary['source_range'],
                   durability=summary['durability'], cache=summary['cache'], table_counts=summary['table_counts'],
                   storage_physical_io=storage_io(raw), modes={})
        row['fabric_local_diagnostics'] = fabric
        for field in ('native_seconds', 'remote_seconds'):
            if nonnegative(row[field], field) == 0:
                raise ValueError('empty elapsed population')
        for mode, measured in summary['modes'].items():
            stats = measured['adapter_by_namespace']
            measured['wrapper_latencies'] = wrapper_latencies(measured['foreground_metrics'])
            measured['wrapper_latency_availability'] = (
                'observed histograms; empty/missing bucket populations reported individually'
                if measured['wrapper_latencies'] else 'unavailable: no wrapper latency histograms')
            measured['adapter_means'] = {ns: adapter_means(stats.get(ns, {}))
                                        for ns in sorted(set(summary['table_counts']) | set(stats))}
            measured['native_internal_spans'] = dict(
                available=any(values.get('native_profile_available') == 1
                              and (values.get('native_profile_barrier_calls') or 0) > 0
                              for values in stats.values()),
                persistence_by_namespace={ns: {name: span for name, span in means.items()
                    if name.startswith('native_profile_')} for ns, means in measured['adapter_means'].items()},
                scope='sampled conservative native durability barriers, actual file-sync attempts and namespace persistence',
                unavailable='native index/allocator/physical-read internal spans; disabled or historical persistence samples')
            measured['cache_and_logical_io'] = {ns: {k: v for k, v in values.items()
                if k.startswith(('cache_', 'shard_')) or k in ('read_calls', 'read_keys', 'read_rpcs', 'scan_records')}
                for ns, values in stats.items()}
            measured['cache_counter_availability'] = {ns: dict(
                coordinator='observed' if 'cache_hits' in values else 'unavailable: no coordinator cache counters',
                shard='observed' if values.get('shard_sample_available') == 1 else 'unavailable: no valid shard sample')
                for ns, values in stats.items()}
            row['modes'][mode] = measured
        cells.append(row)
    cache_groups = {}
    for cell in cells:
        cache = cell['cache']
        name = f"coordinator-{cache['coordinator_bytes']}-shard-{cache['shard_bytes']}"
        cache_groups.setdefault(name, []).append(cell)
    return dict(schema=3, run_id=cfg['run_id'], source_archive_sha256=cfg['source_archive_sha256'],
                source_manifest_sha256=cfg['source_manifest_sha256'],
                build_manifest_sha256=cfg['build_manifest_sha256'],
                checkpoint_digest=cfg['checkpoint_digest'], deadline_utc=cfg['deadline_utc'],
                controller_archive_sha256=receipt['sha256'],
                closure_sha256=build.digest(root / 'cleanup-verified.json'), closure=closure,
                analyzer_sha256=build.digest(Path(__file__)),
                cells=cells, comparison_by_cache={name: dict(cache=rows[0]['cache'],
                    transports=population_comparison(rows)) for name, rows in cache_groups.items()},
                transport_comparison=transport_comparison(cells), limits=summary['limits'] + [
                    'Wrapper rocksdb_* metric names also cover the Tidehunter typed-store implementation; retained RocksDB tables share this instrumentation.',
                    'Histogram bucket bounds are not exact p50/p95/p99 or raw latency samples. Empty populations are unavailable.',
                    'bulk_read counts bounded windows, read_calls API calls, read_keys requested keys, read_rpcs logical groups; none is wire-request count.',
                    'Wire-request means cover successful sampled profiles; actor-return means use returned, never inline completions.',
                    'Shard WAL reads/bytes are logical buffered submissions; zero OS read_bytes can indicate page-cache hits, not zero storage work.',
                    'No nested span totals are added into elapsed time; no pure network RTT, one-way latency or pure NVMe service attribution.',
                    'Native index/cache/physical-read service decomposition and per-key bulk latency are unavailable; persistence requires explicit native diagnostics.',
                    'Closure is retained independent absence evidence, not a fresh AWS query or a cryptographic attestation.'])


def write_campaign_report(root, report):
    """Write JSON and flat CSVs with the same source/evidence identities."""
    root = Path(root)
    build.save(root / 'analysis-details.json', report)
    binding = {k: report[k] for k in ('run_id', 'source_archive_sha256', 'source_manifest_sha256',
                                     'build_manifest_sha256', 'checkpoint_digest',
                                     'controller_archive_sha256', 'closure_sha256', 'analyzer_sha256')}
    cells, wrappers, adapters = [], [], []
    for cell in report['cells']:
        identity = binding | {k: cell[k] for k in ('cell_id', 'transport', 'repetition', 'ordinal',
                              'population', 'read_profile_limit', 'write_profile_limit', 'native_write_profile_limit', 'replay_evidence_sha256')}
        identity.update(coordinator_cache_bytes=cell['cache']['coordinator_bytes'],
                        shard_cache_bytes=cell['cache']['shard_bytes'])
        cells.append(identity | {k: cell[k] for k in ('native_seconds', 'remote_seconds', 'remote_over_native')})
        for mode, measured in cell['modes'].items():
            for histogram in measured['wrapper_latencies']:
                row = identity | dict(mode=mode) | {k: histogram[k] for k in
                    ('metric', 'count', 'total_seconds', 'mean_us', 'available', 'reason', 'quantile_method')}
                row['labels'] = json.dumps(histogram['labels'], sort_keys=True)
                for percentile, quantile in histogram['quantiles'].items():
                    row.update({percentile + '_' + key: value for key, value in quantile.items()})
                wrappers.append(row)
            for namespace, means in measured['adapter_means'].items():
                for stage, mean in means.items():
                    adapters.append(identity | dict(mode=mode, namespace=namespace, stage=stage) | mean)
    for name, rows in (('cell-summary.csv', cells), ('all-wrapper-latencies.csv', wrappers), ('adapter-means.csv', adapters)):
        fields = list(dict.fromkeys(key for row in rows for key in row))
        with (root / name).open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run',type=Path)
    parser.add_argument('--campaign', action='store_true',
                        help='require full closure and compare all retained native/TCP/EFA cells offline')
    parser.add_argument('--verify-only', action='store_true',
                        help='validate complete evidence without deriving performance results')
    args=parser.parse_args()
    root = args.run.resolve(strict=True)
    if args.campaign:
        if args.verify_only:
            cfg, _, _, cells = verified_campaign(root)
            print(json.dumps(dict(verified=True, run_id=cfg['run_id'], cells=len(cells),
                                  performance_analysis='deferred'), indent=2))
        else:
            report = compare_campaign(root)
            write_campaign_report(root, report)
            print(json.dumps(dict(complete=True, run_id=report['run_id'],
                                  cells=len(report['cells']), comparison_by_cache=report['comparison_by_cache']), indent=2))
    elif args.verify_only:
        _, result, _, _ = verified_replay(root)
        print(json.dumps(dict(verified=True, checkpoint_digest=result['checkpoint_digest'],
                              performance_analysis='deferred'), indent=2))
    else:
        report=summarize(root)
        build.save(root/'summary.json',report)
        print(json.dumps(dict(complete=True,foreground_seconds={m:d['foreground_seconds'] for m,d in report['modes'].items()},
                             remote_over_native=report['remote_over_native'],checkpoint_digest=report['checkpoint_digest']),indent=2))


if __name__=='__main__':
    main()
