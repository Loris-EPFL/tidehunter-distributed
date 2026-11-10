#!/usr/bin/env python3
"""Summarize captured Sui API calls and convert exact remote cache references.

Ordering is diagnostic monotonic event time, then sequence id. It is not a
linearizability history. Read-after-write means an observed completed write
preceded the read completion; overlapping operations can have another order.
Only found payload reads enter cache evaluation. Missing keys, index-only
exists, failed calls and native physical-version guesses never enter it.
"""
import argparse
from collections import Counter, defaultdict
import csv
import json
import math
from pathlib import Path
import statistics
import sys

sys.dont_write_bytecode = True
from capture_run import validate
from analyze import adapter_stats, metrics
from trace_metrics import checkpoint_windows, replay_performance

OPS = {1: 'get', 2: 'exists', 3: 'multi_get', 4: 'multi_exists', 5: 'put',
       6: 'delete', 7: 'batch_commit', 8: 'scan_next', 9: 'range_delete'}
EVAL_HEADER = ('op', 'keyspace', 'key_id', 'key_len', 'value_len', 'version')
CACHE = {0: 'miss', 1: 'hit', 2: 'bypass_disabled'}
PREPARATION = {1: 'prepared', 2: 'bypass_disabled', 3: 'rejected_mismatch',
               4: 'rejected_oversize', 5: 'bypass_admission'}


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def distribution(values):
    return {'n': len(values), 'min': min(values) if values else None,
            'p50': percentile(values, .5), 'p95': percentile(values, .95),
            'p99': percentile(values, .99), 'max': max(values) if values else None}


def read_events(path):
    with path.open(newline='') as stream:
        for row in csv.DictReader(stream, delimiter='\t'):
            if None in row or any(value is None for value in row.values()):
                raise ValueError('incomplete event record')
            yield {name: value if name == 'data' else int(value) for name, value in row.items()}


def namespace(path):
    parts = Path(path).parts
    if 'full_node_db' in parts:
        return '/'.join(parts[parts.index('full_node_db') + 2:])
    return path


def read_metadata(directory):
    databases = {}
    for path in sorted(directory.glob('db-*.tsv')):
        data = {'keyspaces': {}}
        for line in path.read_text().splitlines():
            cells = line.split('\t')
            if cells[0] == 'keyspace':
                data['keyspaces'][int(cells[1])] = bytes.fromhex(cells[2]).decode()
            else:
                data[cells[0]] = cells[1]
        data['path'] = bytes.fromhex(data['path_hex']).decode()
        data['namespace'] = namespace(data['path'])
        databases[int(data['db'])] = data
    return databases


def reference_catalog(rows, databases):
    identities = {data['remote_identity']: db for db, data in databases.items()
                  if 'remote_identity' in data}
    references = {}
    for row in rows:
        if row['event'] not in (11, 12, 13, 14, 15):
            continue
        db = row['db'] or identities.get(row['data'][:32], 0)
        if not db:
            raise ValueError(f'unmapped remote DB identity {row["data"][:32]}')
        ref = references.setdefault(row['reference'], {'db': db, 'ks': row['ks'],
            'key_len': row['key_len'], 'value_len': row['value_len'], 'key': None,
            'epoch': row['epoch'], 'version': row['version'], 'member': row['member']})
        for field in ('db', 'ks', 'key_len', 'value_len', 'epoch', 'version', 'member'):
            value = db if field == 'db' else row[field]
            if ref[field] != value:
                raise ValueError('reference fingerprint collision or corrupt identity')
        if row['position'] or row['key']:
            if ref['key'] not in (None, row['key']):
                raise ValueError('reference maps to different key ids')
            ref['key'] = row['key']
    return references


def evaluation_events(rows, databases):
    """Yield exact remote lookups and publications, partitioned by logical DB.

    A TSV version is the complete immutable-reference fingerprint, not just the
    batch sequence. This separates two same-key members within one batch and
    references from different epochs. Key ids still come from application bytes.
    """
    references = reference_catalog(rows, databases)
    failed_calls = {row['call'] for row in rows if row['event'] == 2 and row['outcome'] != 1}
    for row in sorted(rows, key=lambda row: (row['ns'], row['seq'])):
        lookup = row['event'] == 12
        publish = row['event'] == 13 and row['aux'] == 1
        if not (lookup or publish):
            continue
        if lookup and row['call'] in failed_calls:
            continue
        ref = references[row['reference']]
        if ref['key'] is None:
            raise ValueError('cache event has no application-key association')
        op = ('scan_get' if row['aux'] == 1 else 'get') if lookup else (
            'publish_delete' if row['op'] == 6 else 'publish_put')
        yield ref['db'], (op, ref['ks'], ref['key'], ref['key_len'],
                         0 if op == 'publish_delete' else ref['value_len'], row['reference'])


def convert(rows, databases, destination):
    destination.mkdir(parents=True, exist_ok=True)
    streams, counts = {}, Counter()
    try:
        for db, event in evaluation_events(rows, databases):
            if db not in streams:
                path = destination / f'db-{db}.tsv'
                stream = path.open('x', newline='')
                writer = csv.writer(stream, delimiter='\t', lineterminator='\n')
                writer.writerow(EVAL_HEADER)
                streams[db] = (stream, writer, str(path))
            streams[db][1].writerow(event)
            counts[db] += 1
        return {str(db): {'path': path, 'events': counts[db]} for db, (_, _, path) in streams.items()}
    finally:
        for stream, _, _ in streams.values():
            stream.close()


class Reuse:
    """Distinct-key stack distance with a Fenwick tree, plus operation/time gaps."""
    def __init__(self, capacity):
        self.tree = [0] * (capacity + 2)
        self.last = {}
        self.index = 0
        self.operations, self.times, self.distinct = [], [], []

    def add(self, index, delta):
        while index < len(self.tree):
            self.tree[index] += delta
            index += index & -index

    def prefix(self, index):
        total = 0
        while index:
            total += self.tree[index]
            index -= index & -index
        return total

    def observe(self, key, ns):
        self.index += 1
        if key in self.last:
            previous, previous_ns = self.last[key]
            self.operations.append(self.index - previous - 1)
            self.times.append(ns - previous_ns)
            self.distinct.append(self.prefix(self.index) - self.prefix(previous))
            self.add(previous, -1)
        self.add(self.index, 1)
        self.last[key] = self.index, ns

    def summary(self):
        return {'unique_keys': len(self.last), 'reuses': len(self.operations),
                'intervening_operations': distribution(self.operations),
                'distinct_key_stack_distance': distribution(self.distinct),
                'elapsed_ns': distribution(self.times)}


def call_outcome(start, end, members):
    if end['outcome'] != 1:
        return 'error'
    if start['op'] in (1, 2):
        return 'found' if 1 in members else 'not_found'
    if start['op'] in (3, 4):
        return ('empty' if not members else 'mixed' if len(members) > 1 else
                'all_found' if 1 in members else 'all_not_found')
    if start['op'] == 8:
        return 'row' if members else 'exhausted'
    return 'ok'


def summarize(rows, databases):
    starts, ends, batch_ends = {}, {}, {}
    members_by_call = defaultdict(set)
    for row in rows:
        if row['event'] == 1:
            starts[row['call']] = row
        elif row['event'] == 2:
            ends[row['call']] = row
        elif row['event'] == 5:
            batch_ends[row['group']] = row
        if row['event'] in (3, 8):
            members_by_call[row['call']].add(row['outcome'])
    incomplete = sorted(set(starts) - set(ends))
    if incomplete:
        raise ValueError(f'incomplete API calls: {incomplete[:10]}')
    references = reference_catalog(rows, databases)
    output = {}
    for db, metadata in databases.items():
        selected = [row for row in rows if row['db'] == db]
        observations, mutations = [], []
        for row in selected:
            if row['event'] in (3, 8):
                end = ends.get(row['call'])
                if row['op'] in (5, 6) and (not end or end['outcome'] != 1):
                    continue
                observation = dict(row)
                if row['op'] in (5, 6):
                    observation['ns'] = end['ns']
                    mutations.append(observation)
                observations.append(observation)
            elif row['event'] == 4:
                end = batch_ends.get(row['group'])
                if end and end['outcome'] == 1:
                    observation = dict(row, ns=end['ns'])
                    # Scalar remote writes also assemble a one-member batch.
                    parent = starts.get(end['call'], {})
                    if parent.get('op') in (5, 6):
                        continue
                    observations.append(observation)
                    mutations.append(observation)
        observations.sort(key=lambda row: (row['ns'], row['seq']))
        reuse = Reuse(len(observations))
        latest_write, raw = {}, Counter()
        keyspaces = {}
        for ks, name in metadata['keyspaces'].items():
            keyspaces[ks] = {'name': name, 'operations': Counter(), 'api_calls': Counter(), 'outcomes': Counter(),
                             'read_value_bytes': [], 'write_value_bytes': [], 'key_bytes': [], 'latency_ns': defaultdict(list)}
        for row in observations:
            ks = keyspaces[row['ks']]
            op = OPS[row['op']]
            ks['operations'][op] += 1
            ks['outcomes'][{0: 'not_found', 1: 'success', 2: 'error'}[row['outcome']]] += 1
            ks['key_bytes'].append(row['key_len'])
            key = row['ks'], row['key'], row['key_len']
            reuse.observe(key, row['ns'])
            if row['op'] in (5, 6):
                latest_write[key] = row['op']
                if row['op'] == 5:
                    ks['write_value_bytes'].append(row['value_len'])
            elif row['outcome'] == 1:
                raw['found_reads'] += 1
                raw['preceded_by_put'] += latest_write.get(key) == 5
                if row['op'] not in (2, 4):
                    ks['read_value_bytes'].append(row['value_len'])
        api_calls, threads, arrivals, second_counts = Counter(), set(), [], Counter()
        for call, start in starts.items():
            if start['db'] != db:
                continue
            api_calls[OPS[start['op']]] += 1
            threads.add(start['thread'])
            arrivals.append(start['ns'])
            second_counts[start['ns'] // 1_000_000_000] += 1
            if start['op'] != 7:
                keyspaces[start['ks']]['api_calls'][OPS[start['op']]] += 1
                outcome = call_outcome(start, ends[call], members_by_call.get(call, ()))
                keyspaces[start['ks']]['latency_ns'][OPS[start['op']] + ':' + outcome].append(ends[call]['ns'] - start['ns'])
        arrivals.sort()
        seconds = [second_counts[second] for second in range(min(second_counts, default=0), max(second_counts, default=0) + 1)]
        interarrival = [right - left for left, right in zip(arrivals, arrivals[1:])]
        mean_gap = statistics.mean(interarrival) if interarrival else 0
        mean_count = statistics.mean(seconds) if seconds else 0
        cache, preparations, insertions, routes = Counter(), Counter(), Counter(), Counter()
        for row in rows:
            if row['event'] not in (11, 12, 13, 14, 15) or references[row['reference']]['db'] != db:
                continue
            if row['event'] == 12:
                cache[('scan_' if row['aux'] else 'point_') + CACHE[row['outcome']]] += 1
            elif row['event'] == 13:
                preparations[('publish_' if row['aux'] else 'demand_') + PREPARATION[row['outcome']]] += 1
            elif row['event'] == 14:
                insertions['accepted_or_resident' if row['outcome'] else 'rejected_admission_or_eviction_limit'] += 1
            elif row['event'] == 15:
                routes[{1: 'owner_rpc_completed_members', 2: 'nvmeof_completed_members'}[row['outcome']]] += 1
        batch_rows = [row for row in selected if row['event'] == 5]
        composition = defaultdict(Counter)
        for row in selected:
            if row['event'] == 4:
                composition[row['group']][row['ks']] += 1
        consulted = {row['call'] for row in selected if row['event'] == 12}
        bypass = Counter()
        if metadata['backend'] == 'remote':
            for call, start in starts.items():
                if start['db'] == db and start['op'] in (1, 2, 3, 4) and call not in consulted:
                    outcome = call_outcome(start, ends[call], members_by_call.get(call, ()))
                    reason = (outcome if outcome in ('empty', 'error', 'not_found', 'all_not_found') else
                              'index_only_exists' if start['op'] in (2, 4) else 'unclassified_no_cache_probe')
                    bypass[reason] += 1
        scans = [row for row in selected if row['event'] == 9]
        per_second = Counter()
        touched = {row['ks'] for row in selected if row['event'] in (3, 4, 6, 7, 8) or
                   (row['event'] == 1 and row['op'] != 7)}
        for row in mutations:
            per_second[row['ns'] // 1_000_000_000] += row['key_len'] + row['value_len']
        output[str(db)] = {'namespace': metadata['namespace'], 'backend': metadata['backend'],
            'api_calls': dict(api_calls), 'api_read_calls': sum(api_calls[OPS[op]] for op in (1, 2, 3, 4)),
            'api_read_keys': sum(row['count'] for row in starts.values() if row['db'] == db and row['op'] in (1, 2, 3, 4)),
            'scan_returned_rows': sum(row['event'] == 8 for row in selected),
            'keyspaces': {str(ks): {'name': data['name'], 'operations': dict(data['operations']), 'api_calls': dict(data['api_calls']),
                'outcomes': dict(data['outcomes']), 'read_value_bytes': distribution(data['read_value_bytes']),
                'write_value_bytes': distribution(data['write_value_bytes']), 'key_bytes': distribution(data['key_bytes']),
                'api_latency_ns_by_operation_outcome': {group: distribution(values) for group, values in data['latency_ns'].items()}} for ks, data in keyspaces.items()},
            'untouched_keyspaces': [data['name'] for ks, data in keyspaces.items() if ks not in touched],
            'reuse': reuse.summary(), 'read_after_write': dict(raw, share=raw['preceded_by_put'] / raw['found_reads'] if raw['found_reads'] else None),
            'batches': {'sizes': distribution([row['count'] for row in batch_rows]),
                        'keyspaces_per_batch': distribution([len(composition[row['group']]) for row in batch_rows]),
                        'latency_ns_by_outcome': {outcome: distribution([ends[call]['ns'] - start['ns'] for call, start in starts.items() if start['db'] == db and start['op'] == 7 and call_outcome(start, ends[call], ()) == outcome]) for outcome in ('ok', 'error')},
                        'failed': sum(row['outcome'] != 1 for row in batch_rows)},
            'scans': {'returned_lengths': distribution([row['count'] for row in scans]),
                      'end_outcomes': dict(Counter({0: 'abandoned', 1: 'exhausted', 2: 'error'}[row['outcome']] for row in scans)),
                      'open_at_cutoff': sum(row['event'] == 6 for row in selected) - len(scans)},
            'cache_lookup': dict(cache), 'cache_preparation': dict(preparations),
            'cache_not_consulted_calls': dict(bypass),
            'cache_insertion': dict(insertions), 'route': dict(routes), 'threads': len(threads),
            'interarrival_ns': distribution(interarrival),
            'interarrival_cv': statistics.pstdev(interarrival) / mean_gap if mean_gap else None,
            'calls_per_second': {str(second): count for second, count in sorted(second_counts.items())},
            'burst_fano': statistics.pvariance(seconds) / mean_count if mean_count else None,
            'logical_write_bytes_per_second': {str(second): count for second, count in sorted(per_second.items())}}
    return output, mutations_by_checkpoint(rows, starts, ends, batch_ends)


def mutations_by_checkpoint(rows, starts, ends, batch_ends):
    # The store boundary does not carry checkpoint execution context. Preserve
    # write completion times for temporal (not causal) matching to node logs.
    result = []
    for row in rows:
        if row['event'] == 4:
            end = batch_ends.get(row['group'])
            if end and end['outcome'] == 1 and starts.get(end['call'], {}).get('op') not in (5, 6):
                result.append({'db': row['db'], 'ns': end['ns'], 'bytes': row['key_len'] + row['value_len']})
        elif row['event'] == 3 and row['op'] in (5, 6):
            end = ends.get(row['call'])
            if end and end['outcome'] == 1:
                result.append({'db': row['db'], 'ns': end['ns'], 'bytes': row['key_len'] + row['value_len']})
    return result


def flatten(value, prefix=''):
    if isinstance(value, dict):
        for key, child in value.items():
            yield from flatten(child, f'{prefix}.{key}' if prefix else str(key))
    elif not isinstance(value, list):
        yield prefix, value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capture', required=True, type=Path)
    parser.add_argument('--replay', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--baseline', type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    all_summary = {'schema': 1, 'limits': [
        'Private chain: 4096 user transactions, checkpoints 0..1775; not mainnet.',
        'Only TideHunter-backed tables are captured; upstream caches suppress calls.',
        'Reuse and read-after-write use observed completion order, not a causal history.',
        'Native payload fingerprints do not identify physical WAL versions.',
        'Hash collisions remain possible; exact remote shape conflicts are rejected.',
        'Route counts are completed members, not unique network RPCs or SSD reads.',
        'Checkpoint temporal windows are not causal ownership; execution overlaps.'], 'modes': {}}
    performance = {'capture_on': replay_performance(args.replay)}
    if args.baseline:
        performance['capture_off'] = replay_performance(args.baseline)
        performance['wall_time_change_percent'] = {mode: 100 * (
            performance['capture_on'][mode]['wall_seconds_including_startup'] /
            performance['capture_off'][mode]['wall_seconds_including_startup'] - 1)
            for mode in ('native', 'remote')}
    all_summary['performance'] = performance
    for mode in ('native', 'remote'):
        directory = args.capture / mode
        validity = validate(directory)
        databases = read_metadata(directory)
        rows = list(read_events(directory / 'events.tsv'))
        if len(rows) != validity['written']:
            raise ValueError('record count differs from writer receipt')
        summaries, writes = summarize(rows, databases)
        anchor = int(dict(line.split('\t', 1) for line in (directory / 'metadata.tsv').read_text().splitlines())['anchor_unix_ns'])
        windows = checkpoint_windows((args.replay / 'experiment' / f'{mode}.log').read_text(), writes, anchor, validity['cutoff_ns'])
        with (args.output / f'{mode}-checkpoint-temporal-writes.csv').open('w', newline='') as stream:
            fields = ['checkpoint_start_label', 'start_ns', 'end_ns', 'seconds', 'write_bytes', 'write_operations', 'bytes_per_second']
            writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
            writer.writeheader(); writer.writerows(windows)
        batch_members = defaultdict(lambda: [0, 0])
        for row in rows:
            if row['event'] == 4:
                count = batch_members[(row['db'], row['group'], row['ks'], row['op'])]
                count[0] += 1
                count[1] += row['key_len'] + row['value_len']
        with (args.output / f'{mode}-batch-composition.csv').open('w', newline='') as stream:
            writer = csv.writer(stream); writer.writerow(['db', 'group', 'keyspace', 'operation', 'members', 'logical_bytes'])
            for (db, group, ks, op), (count, size) in batch_members.items():
                writer.writerow([db, group, ks, OPS[op], count, size])
        counters = adapter_stats(metrics(args.replay / 'experiment' / f'{mode}-final.prom'))
        reconciliation = {}
        for db, summary in summaries.items():
            observed = counters.get(summary['namespace'], {})
            reconciliation[db] = {name: {'captured': summary[source], 'node': observed.get(name),
                'difference': summary[source] - observed[name] if name in observed else None}
                for name, source in [('read_calls', 'api_read_calls'), ('read_keys', 'api_read_keys'), ('scan_records', 'scan_returned_rows')]}
            if mode == 'native':
                reconciliation[db]['scan_records'].update(
                    difference=None, comparable=False,
                    note='The existing adapter counter counts RemoteIterator rows only; NativeDbIterator does not increment it.')
            # Cache probes can include backpressure rechecks and scan prefetch.
            if mode == 'remote':
                cache = summary['cache_lookup']
                for stat, suffix in [('cache_hits', 'hit'), ('cache_misses', 'miss')]:
                    captured = sum(value for key, value in cache.items() if key.endswith(suffix))
                    if suffix == 'miss':
                        captured += sum(value for key, value in cache.items() if key.endswith('bypass_disabled'))
                    reconciliation[db][stat] = {'captured': captured, 'node': observed.get(stat),
                        'difference': captured - observed[stat] if stat in observed else None}
        tsv = convert(rows, databases, args.output / f'{mode}-trace-eval') if mode == 'remote' else {}
        with (args.output / f'{mode}-write-timeline.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=['db', 'ns', 'bytes'])
            writer.writeheader(); writer.writerows(sorted(writes, key=lambda row: row['ns']))
        all_summary['modes'][mode] = {'validity': validity, 'databases': summaries,
            'reconciliation': reconciliation, 'trace_eval': tsv,
            'checkpoint_windows': {'count': len(windows), 'attribution': 'temporal only; most recent execution-start log, not causal ownership'}}
    (args.output / 'metrics-summary.json').write_text(json.dumps(all_summary, indent=2) + '\n')
    with (args.output / 'metrics-summary.csv').open('w', newline='') as stream:
        writer = csv.writer(stream); writer.writerow(['metric', 'value']); writer.writerows(flatten(all_summary))
    print(args.output / 'metrics-summary.json')


if __name__ == '__main__':
    main()
