#!/usr/bin/env python3
"""Offline, source-bound checkpoint analysis.

Single replay: ``python3 analyze.py PAIRED_REPLAY`` (optionally
``--verify-only``). The closed-campaign comparator was removed because its
``experiments/aws-sharded-wal/checkpoint_matrix.py`` source no longer exists;
the campaign controller aggregates per-cell rows instead.

Current evidence is the aggregate ``tidehunter_nvmeof_live_*`` family and the
coordinator registry written through ``Db::open_remote_with_metrics``. The
removed per-owner ``tidehunter_operational_read_io``/``tidehunter_shard_read``
families and adapter ``shard_*`` stats are rejected, never decoded as zero.
Missing diagnostics are null with a reason, never zero.
"""
import argparse
from contextlib import contextmanager
import hashlib
import json
import math
from pathlib import Path
import re
import sys
sys.dont_write_bytecode = True
import tarfile
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
        if metric.startswith(('db_op_', 'rocksdb_', 'db_op{', 'lookup_mcs_', 'lookup_mcs{',
                             'large_table_contention_', 'large_table_contention{',
                             'index_blob_cache_', 'flush_count{', 'flushed_bytes{',
                             'flushed_keys{', 'promote_total{', 'l0_bytes_written{',
                             'l1_bytes_written{')) or any(word in metric for word in (
                             'checkpoint', 'execution_cache', 'sharded', 'tidehunter',
                             'shared_version', 'marker', 'object')):
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


LIVE_NVMEOF_PREFIX = 'tidehunter_nvmeof_live_'
# Gauges in the aggregate live family; every other aggregate is monotone.
LIVE_NVMEOF_GAUGES = frozenset(('target_observed_partitions', 'target_fixture_partitions',
    'target_owned_namespaces', 'lane_inflight_requests_max', 'lane_inflight_operations_max'))


def _labels(metric):
    return {name: json.loads('"' + encoded + '"') for name, encoded in
            re.findall(r'(\w+)="((?:\\.|[^"\\])*)"', metric)}


def live_nvmeof_stats(values):
    """Decode the pull-only aggregate NVMe-oF live family.

    `tidehunter/src/distributed/nvmeof/live_prometheus.rs` exports one unlabeled
    family per host: namespace identities stay evidence, never metric labels.
    There is no per-partition or per-sample-availability counter, so this
    decoder never invents one.
    """
    counters = {}
    for metric, value in values.items():
        if not metric.startswith(LIVE_NVMEOF_PREFIX):
            continue
        name = metric[len(LIVE_NVMEOF_PREFIX):]
        if not re.fullmatch(r'[a-z0-9_]+', name) or name in counters:
            raise ValueError('invalid or duplicate aggregate live NVMe-oF metric')
        if not math.isfinite(value) or value < 0:
            raise ValueError('invalid aggregate live NVMe-oF value')
        counters[name] = value
    return dict(available=bool(counters), counters=counters,
        read_io_samples=counters.get('read_io_samples_total'),
        read_seconds=counters.get('read_io_seconds_total'),
        delivery_seconds=counters.get('delivery_seconds_total'),
        delivery_samples=counters.get('delivery_samples_total'),
        completed_read_ops=counters.get('completed_read_ops_total'),
        completed_read_bytes=counters.get('completed_read_bytes_total'),
        completed_write_ops=counters.get('completed_write_ops_total'),
        completed_write_bytes=counters.get('completed_write_bytes_total'),
        namespace_connects=counters.get('namespace_connects_total'),
        errors=counters.get('errors_total'),
        observed_partitions=counters.get('target_observed_partitions'),
        scope='aggregate per-host live counters, no namespace or partition labels; '
              'read_io_seconds includes queue, fabric, target and storage; not a per-value service time')


def live_nvmeof_deltas(before, after):
    initial, final = live_nvmeof_stats(before), live_nvmeof_stats(after)
    if not initial['available'] or not final['available']:
        return dict(available=False, reason='aggregate live NVMe-oF family missing from one snapshot',
                    by_counter={})
    if set(initial['counters']) != set(final['counters']):
        return dict(available=False, reason='aggregate live counter population differs', by_counter={})
    deltas = {}
    for name, value in final['counters'].items():
        previous = initial['counters'][name]
        if name in LIVE_NVMEOF_GAUGES:
            deltas[name] = dict(before=previous, after=value)
        elif value < previous:
            return dict(available=False, reason='aggregate live counter reset', by_counter={})
        else:
            deltas[name] = value - previous
    return dict(available=True, reason=None, by_counter=deltas,
                read_io_samples=deltas.get('read_io_samples_total'),
                completed_read_ops=deltas.get('completed_read_ops_total'),
                completed_read_bytes=deltas.get('completed_read_bytes_total'),
                completed_write_ops=deltas.get('completed_write_ops_total'),
                completed_write_bytes=deltas.get('completed_write_bytes_total'),
                errors=deltas.get('errors_total'),
                namespace_connects=deltas.get('namespace_connects_total'),
                scope='same-host before/after aggregate deltas; unavailable samples remain absent')


def _microsecond_histograms(values, metric_pattern):
    """Decode mcs histograms (microsecond buckets) without pooling labels."""
    groups = {}
    for metric, value in values.items():
        match = re.fullmatch(r'(' + metric_pattern + r')_(sum|count|bucket)(?:\{(.*)\})?', metric)
        if not match:
            continue
        name, kind, encoded = match.groups()
        labels = _labels(encoded or '')
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
        row = dict(metric=name, labels=dict(labels), count=count, total_us=total,
                   mean_us=None, available=False, reason=None, quantiles={})
        if count is None or total is None:
            row['reason'] = 'histogram count or sum missing'
        elif count != int(count):
            raise ValueError('nonintegral histogram population')
        elif count == 0:
            if total != 0 or any(group.get('buckets', {}).values()):
                raise ValueError('nonzero histogram with empty population')
            row['reason'] = 'empty observed histogram population'
        else:
            row.update(available=True, mean_us=total / count)
        buckets = sorted(group.get('buckets', {}).items())
        if buckets:
            if any(n != int(n) for _, n in buckets) or any(a[1] > b[1] for a, b in zip(buckets, buckets[1:])):
                raise ValueError('nonintegral or decreasing cumulative histogram')
            if count is not None and (any(n > count for _, n in buckets) or
                    (math.isinf(buckets[-1][0]) and buckets[-1][1] != count)):
                raise ValueError('histogram bucket/count mismatch')
        complete = bool(buckets and math.isinf(buckets[-1][0]) and count is not None)
        for percentile in (50, 90, 99):
            quantile = dict(available=False, lower_us=None, upper_us=None,
                            upper_unbounded=False, reason='empty or incomplete histogram population')
            if percentile == 99 and count is not None and 0 < count < 100:
                quantile['reason'] = 'fewer than 100 observations; p99 unavailable'
            elif complete and count > 0:
                lower = 0.0
                for upper, cumulative in buckets:
                    if cumulative >= count * percentile / 100:
                        quantile = dict(available=True, lower_us=lower,
                                        upper_us=upper if math.isfinite(upper) else None,
                                        upper_unbounded=math.isinf(upper), reason=None)
                        break
                    lower = upper
            row['quantiles'][f'p{percentile}'] = quantile
        rows.append(row)
    return rows


def _keyed(rows, field):
    result = {}
    for row in rows:
        key = row['labels'].get(field)
        if key is None or key in result:
            raise ValueError(f'ambiguous or duplicate coordinator histogram {field} identity')
        result[key] = row
    return result


def coordinator_stats(values):
    """Decode coordinator metrics registered through `Db::open_remote_with_metrics`.

    The engine registers `db_op`, `lookup_mcs`, `large_table_contention`, the
    `index_blob_cache_*` family and per-keyspace write counters in Sui's
    registry. Names and labels are the engine ones; a `db` label is used only
    when Sui supplies it. No per-partition counter exists.
    """
    operations = {}
    for row in _microsecond_histograms(values, 'db_op'):
        op, ks = row['labels'].get('op'), row['labels'].get('ks')
        if op is None or ks is None:
            raise ValueError('db_op histogram lacks op/ks identity')
        if op in operations.setdefault(ks, {}):
            raise ValueError('duplicate db_op histogram')
        operations[ks][op] = row
    lookups = {}
    for row in _microsecond_histograms(values, 'lookup_mcs'):
        kind, ks = row['labels'].get('type'), row['labels'].get('ks')
        if kind is None or ks is None:
            raise ValueError('lookup histogram lacks type/ks identity')
        if kind in lookups.setdefault(ks, {}):
            raise ValueError('duplicate lookup histogram')
        lookups[ks][kind] = row
    contention = _keyed(_microsecond_histograms(values, 'large_table_contention'), 'ks')
    cache_contention = _microsecond_histograms(values, 'index_blob_cache_contention')
    if len(cache_contention) > 1 or (cache_contention and cache_contention[0]['labels']):
        raise ValueError('ambiguous index blob cache contention population')
    cache = {name[len('index_blob_cache_'):]: value for name, value in values.items()
             if re.fullmatch(r'index_blob_cache_[a-z0-9_]+', name)}
    writes = {}
    for name in ('flush_count', 'flushed_bytes', 'flushed_keys', 'promote_total',
                 'l0_bytes_written', 'l1_bytes_written'):
        for metric, value in values.items():
            if not metric.startswith(name + '{'):
                continue
            labels = _labels(metric)
            ks = labels.get('ks')
            if ks is None or set(labels) - {'ks', 'db'}:
                raise ValueError(f'{name} lacks ks identity')
            if name in writes.setdefault(ks, {}):
                raise ValueError(f'duplicate {name} counter')
            writes[ks][name] = value
    databases = set()
    for metric in values:
        if metric.startswith(('db_op_', 'lookup_mcs_')):
            db = _labels(metric).get('db')
            if db:
                databases.add(db)
    keyspaces = sorted(set(operations) | set(lookups) | set(contention) | set(writes))
    available = bool(operations or lookups or contention or cache)
    return dict(available=available, operations=operations, lookups=lookups,
        large_table_contention=contention, index_blob_cache=cache,
        index_blob_cache_contention=cache_contention[0] if cache_contention else None,
        writes=writes, keyspaces=keyspaces, databases=sorted(databases),
        scope='coordinator-local cumulative histograms and counters; lock wait samples only record contended waits; '
              'bucket bounds are approximations, not raw quantiles')


def owner_fetch_fraction(live, coordinator):
    """Owner read requests per coordinator API get; not a cache-hit counter.

    The engine has no value-cache hit counter at this revision. This derived
    fraction is the plan's substitute: aggregate owner read operations divided
    by coordinator get operations. It counts requests, so a multi-get and a
    point get both appear as their typed-store get calls, and it is unavailable
    when either population is absent or empty.
    """
    reads = live.get('completed_read_ops')
    gets = sum(row['get']['count'] for row in coordinator['operations'].values()
               if 'get' in row and row['get']['count'] is not None)
    if reads is None or gets is None:
        return dict(available=False, reason='owner read or coordinator get population missing',
                    owner_read_ops=reads, api_gets=gets, fraction=None)
    if gets == 0:
        return dict(available=False, reason='no coordinator get calls observed',
                    owner_read_ops=reads, api_gets=gets, fraction=None)
    return dict(available=True, reason=None, owner_read_ops=reads, api_gets=gets,
                fraction=reads / gets,
                scope='aggregate owner read ops divided by coordinator get calls; '
                      'requests, not value-cache hits or bytes')


def coordinator_covers(stats, namespace):
    """Prove a namespace reached the coordinator registry, without per-owner claims."""
    if not stats['available']:
        return False
    if stats['databases']:
        return any(namespace == db or namespace.endswith('/' + db) for db in stats['databases'])
    return bool(stats['keyspaces'])


class MetricCutUnavailable(ValueError):
    """A transiently incomplete scrape; the final-cut wait retries this only."""


def metric_cut(values, namespaces, *, remote):
    """Decode one scrape into the aggregate live and coordinator populations."""
    live = live_nvmeof_stats(values)
    coordinator = coordinator_stats(values)
    if remote and not live['available']:
        raise MetricCutUnavailable('remote scrape lacks the aggregate tidehunter_nvmeof_live_* family')
    if remote and not coordinator['available']:
        raise MetricCutUnavailable('remote scrape lacks coordinator read/lock metrics')
    if coordinator['available']:
        missing = [ns for ns in namespaces if not coordinator_covers(coordinator, ns)]
        if missing:
            raise MetricCutUnavailable('coordinator metrics lack audited namespaces: '
                                       + ', '.join(sorted(missing)))
    return dict(live=live, coordinator=coordinator)


def metric_cut_progressed(baseline, current, *, remote):
    """Require one post-certified aggregate publication advance in remote mode."""
    if not remote:
        return True
    initial, final = baseline['live'], current['live']
    if not initial['available'] or not final['available']:
        return False
    if set(initial['counters']) != set(final['counters']):
        raise ValueError('aggregate live population changed during the final cut')
    advanced = False
    for name, value in final['counters'].items():
        previous = initial['counters'][name]
        if name in LIVE_NVMEOF_GAUGES:
            continue
        if value < previous:
            raise ValueError('aggregate live counter regressed during the final cut')
        advanced |= value > previous
    return advanced



def validate_final_metric_cut(values, namespaces, cut, *, remote):
    current = metric_cut(values, namespaces, remote=remote)
    if (cut.get('status') != 'post-certified-scrape' or
            cut.get('required_namespaces') != sorted(namespaces) or
            cut.get('final') != current):
        raise ValueError('final metric cut population/status differs')
    baseline = cut.get('baseline')
    if not isinstance(baseline, dict) or set(baseline) != {'live', 'coordinator'}:
        raise ValueError('final metric cut baseline is missing')
    if remote and not metric_cut_progressed(baseline, current, remote=True):
        raise ValueError('final metric cut observed no aggregate live progress')
    return current


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
    for namespace, stats in result.items():
        legacy = {name for name in stats if name.startswith(
            ('shard_', 'resident_', 'authority_', 'initiator_'))}
        if legacy:
            raise ValueError('legacy per-shard adapter counters are not current: '
                             + ', '.join(sorted(legacy)))
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
        if result.get('metrics_schema') != 3:
            raise ValueError('this analyzer requires metrics schema 3 (aggregate live and coordinator metrics); '
                             'older schema evidence is not re-decoded')
        final_by_mode = {mode: parse_metrics(archived_bytes(
            archive, f'experiment/{mode}-final.prom').decode()) for mode in ('native', 'remote')}
        for mode, values in final_by_mode.items():
            cut = result[mode].get('final_metrics_freshness', {})
            namespaces = cut.get('required_namespaces', [])
            from run import live_metric_namespaces
            expected = live_metric_namespaces(audit['databases'], result[mode].get('completed_checkpoint_epoch'))
            if set(namespaces) != expected:
                raise ValueError('final metric cut lacks audited live namespaces')
            validate_final_metric_cut(values, namespaces, cut, remote=mode == 'remote')
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
    cut = result['remote'].get('final_metrics_freshness', {})
    namespaces = cut.get('required_namespaces')
    if not namespaces:
        raise ValueError('remote replay lacks a final metric cut')
    metric_cut(final_by_mode['remote'], namespaces, remote=True)
    return receipt, result, audit, foreground_by_mode, final_by_mode


def summarize(root):
    return summarize_verified(verified_replay(root))


def summarize_verified(verified):
    _, result, audit, foreground_by_mode, final_by_mode = verified
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
        if mode in final_by_mode:
            final = final_by_mode[mode]
            summary['modes'][mode].update(final_metrics_freshness=replay['final_metrics_freshness'],
                final_metrics=final, final_adapter_by_namespace=adapter_stats(final),
                final_live_nvmeof=live_nvmeof_stats(final),
                final_coordinator=coordinator_stats(final),
                final_metrics_scope='post-certified scrape cut; excluded from foreground metrics/timing; not quiescent physical IO')
        summary['modes'][mode]['checkpoint_completion'] = checkpoint_completion_observation(foreground)
        summary['modes'][mode]['adapter_by_namespace'] = adapter_stats(foreground)
        summary['modes'][mode]['live_nvmeof'] = live_nvmeof_stats(foreground)
        summary['modes'][mode]['coordinator'] = coordinator_stats(foreground)
        summary['modes'][mode]['owner_fetch_fraction'] = owner_fetch_fraction(
            summary['modes'][mode]['live_nvmeof'], summary['modes'][mode]['coordinator'])
        summary['modes'][mode]['disk'] = result.get('disk')
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
    """All typed-store latency histograms, including empty populations."""
    return _histogram_rows(values, r'rocksdb_\w+_latency_seconds')


def checkpoint_completion_observation(values, *, required=False):
    """Actual admitted-checkpoint completion observations; never start spacing."""
    rows = _histogram_rows(values, 'checkpoint_completion_latency', include_buckets=True)
    if len(rows) != 1:
        observation = dict(metric='checkpoint_completion_latency', labels={}, count=None,
            total_seconds=None, mean_us=None, quantiles={}, buckets=[], available=False,
            reason='missing or ambiguous checkpoint_completion_latency histogram',
            quantile_method='cumulative histogram bucket bounds, not raw quantiles')
    else:
        observation = rows[0]
        if (not observation['available'] or observation['total_seconds'] <= 0
                or not observation['complete_buckets']):
            observation.update(available=False,
                reason='checkpoint completion histogram requires positive count/sum and complete valid buckets')
    if observation['available']:
        observation['count'] = int(observation['count'])
    observation.update(
        scope='synced checkpoint admission before pipeline wait through queueing/execution/storage commit/publication after highest-executed watermark update; not certification arrival, execution-start spacing or raw device latency',
        observation_boundary='foreground tip-watermark scrape; final checkpoint may update watermark immediately before its histogram observation',
        count_contract='actual positive observed count; not assumed equal to fixture checkpoint count')
    if required and not observation['available']:
        raise ValueError('checkpoint completion histogram unavailable: ' + observation['reason'])
    return observation


def _histogram_rows(values, metric_pattern, *, include_buckets=False):
    """Decode a named histogram family without pooling labels or populations.

    Quantiles are cumulative-histogram bucket bounds in microseconds, NOT raw
    sample quantiles. The unbounded +Inf upper endpoint is represented by null.
    No interpolation assumes a distribution within a bucket.
    P99 additionally requires at least 100 observations in this histogram; cells
    and other histogram populations are never pooled to satisfy that threshold.
    """
    groups = {}
    for metric, value in values.items():
        match = re.fullmatch(r'(' + metric_pattern + r')_(sum|count|bucket)(?:\{(.*)\})?', metric)
        if not match:
            continue
        name, kind, encoded = match.groups()
        pairs = re.findall(r'(\w+)="((?:\\.|[^"\\])*)"', encoded or '')
        labels = {key: json.loads('"' + value + '"') for key, value in pairs}
        if len(labels) != len(pairs):
            raise ValueError('duplicate histogram label')
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
        if include_buckets:
            row['complete_buckets'] = complete
            row['buckets'] = [dict(upper_seconds=bound if math.isfinite(bound) else None,
                                   upper_unbounded=math.isinf(bound), cumulative_count=int(n))
                              for bound, n in buckets]
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


def validate_archive_members(archive):
    names = set()
    for member in archive.getmembers():
        if (member.name in names or not (member.isfile() or member.isdir())
                or '..' in Path(member.name).parts or Path(member.name).is_absolute()):
            raise ValueError('ambiguous or unsafe archive member')
        names.add(member.name)







def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run',type=Path)
    parser.add_argument('--verify-only', action='store_true',
                        help='validate complete evidence without deriving performance results')
    args=parser.parse_args()
    root = args.run.resolve(strict=True)
    if args.verify_only:
        _, result, _, _, _ = verified_replay(root)
        print(json.dumps(dict(verified=True, checkpoint_digest=result['checkpoint_digest'],
                              performance_analysis='deferred'), indent=2))
    else:
        report=summarize(root)
        build.save(root/'summary.json',report)
        print(json.dumps(dict(complete=True,foreground_seconds={m:d['foreground_seconds'] for m,d in report['modes'].items()},
                             remote_over_native=report['remote_over_native'],checkpoint_digest=report['checkpoint_digest']),indent=2))


if __name__=='__main__':
    main()
