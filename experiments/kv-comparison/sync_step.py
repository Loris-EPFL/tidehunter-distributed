"""Compare observed persistence calls without subtracting sampled populations."""
import json
import math
import statistics


def rows(export, cases, floors, route):
    result = []
    for case in cases:
        job = case['job']
        if (case.get('status') != 'completed' or job.get('group') != 'G12D'
                or job.get('mix', {}).get('put') != 100):
            continue
        native = job.get('backend') == 'native'
        if (job.get('native_durability') != 'value_wal_barrier' if native
                else job.get('durability_tier') != 'synchronous'):
            continue
        base = {'case_id': case['case_id'], 'route': route(job), 'size_bytes': job['value_size'],
                'concurrency': job['concurrency'], 'durability': 'value_wal_barrier: file fsync + directory fsync' if native else 'synchronous owner WAL fdatasync'}
        directory = export / 'cases' / case['case_id'] / 'result'
        if native:
            path = directory / 'write-diagnostics.json'
            if not path.is_file():
                continue
            profile = json.loads(path.read_text()).get('profile') or {}
            persist = profile.get('value_wal_persistence') or {}
            n = profile.get('selected_operations')
            if not n or persist.get('status') != 'sampled':
                continue
            file_ns, namespace_ns = persist.get('file_ns'), persist.get('namespace_ns')
            for label, value in (('file fsync', file_ns), ('directory fsync', namespace_ns),
                                 ('file + directory fsync', file_ns + namespace_ns if file_ns is not None and namespace_ns is not None else None)):
                result.append({**base, 'step': label, 'n': n, 'calls': persist.get('file_calls') if label == 'file fsync' else persist.get('namespace_calls') if label == 'directory fsync' else None,
                               'mean_us': value / n / 1000 if value is not None else None, 'p50_us': None, 'p99_us': None})
        else:
            path = directory / 'write-timeline.json'
            if not path.is_file():
                continue
            records = (json.loads(path.read_text()).get('timeline') or {}).get('owner_records') or []
            for label, field in (('file fdatasync', 'wal_file_persist_ns'), ('append persistence (inclusive)', 'append_persist_ns')):
                values = sorted(r[field] for r in records if isinstance(r.get(field), (int, float)) and not isinstance(r[field], bool))
                if values:
                    result.append({**base, 'step': label, 'n': len(values), 'calls': None,
                                   'mean_us': statistics.mean(values) / 1000,
                                   'p50_us': values[math.ceil(len(values) * .5) - 1] / 1000,
                                   'p99_us': values[math.ceil(len(values) * .99) - 1] / 1000})
    for floor in floors:
        if floor.get('mode') != 'fdatasync' or floor.get('verdict') != 'qualified':
            continue
        result.append({'case_id': floor['case_id'], 'route': 'owner0 overwrite floor',
                       'size_bytes': floor['block_bytes'], 'concurrency': 1,
                       'durability': 'fdatasync; overwrites a pre-written file; not a WAL append reference',
                       'step': 'file fdatasync', 'n': floor.get('n'), 'calls': None,
                       **{stat + '_us': floor.get('sync_' + stat + '_us') for stat in ('mean', 'p50', 'p99')}})
    return result
