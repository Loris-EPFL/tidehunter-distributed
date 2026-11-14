#!/usr/bin/env python3
"""Join slow coordinator write groups to owner evidence. Overlap is not causality.

Clock offset is owner minus coordinator. All joins retain the full offset
interval; uncertain boundary overlaps are reported, not promoted to attribution.
"""
import argparse
from collections import Counter, deque
from decimal import Decimal
import json
from pathlib import Path
import re

MS = 1_000_000
CLASSES = ('M1_dirty_throttle', 'M2_journal_commit', 'M3_queue_rebind', 'M4_target_fast', 'unattributed')


def attributes(event):
    extra = event.get('a', {})
    return {**event, **extra} if isinstance(extra, dict) else event


def stamp(event):
    return event.get('monotonic_ns', event.get('ts_ns', event.get('t_ns', event.get('t'))))


def stage(event):
    return event.get('kind', event.get('stage', event.get('event', event.get('name', ''))))


# Codes are defined by tcp/reactor.rs::request_kind_code. These are the
# requests exported by its trace_control_submit path.
CONTROL_KIND_CODES = {'checkpoint': 1, 'position': 18, 'lease': 33,
                      'adoption': 34, 'closing_adoption': 34, 'prelease': 36}


def alignment(timeline, owner):
    origin = timeline.get('origin_monotonic_ns')
    missing = {'status': 'no_owner_clock', 'samples': 0, 'inliers': 0, 'outliers': 0}
    if origin is None or not owner:
        return missing
    pending, controls = {}, {}
    for event in timeline.get('control_events', []):
        if event.get('owner', 0) != 0:
            continue
        identity = event['envelope_id']
        kind = CONTROL_KIND_CODES.get(event.get('target', {}).get('kind'))
        key = (identity, kind)
        when = origin + event['elapsed_ns']
        if event['stage'] == 'submit':
            pending.setdefault(key, deque()).append(when)
        elif event['stage'] == 'complete' and pending.get(key):
            started = pending[key].popleft()
            if started <= when:
                controls.setdefault(identity, []).append((kind, started, when))
    bounds = []
    for event in owner:
        a = attributes(event)
        if stage(event) != 'control_received' or stamp(event) is None:
            continue
        identity = a.get('id', a.get('envelope_id', a.get('a')))
        owner_kind = event.get('b')
        if type(owner_kind) is not int or owner_kind <= 0:
            owner_kind = None
        for kind, started, completed in controls.get(identity, []):
            if kind is not None and owner_kind is not None and kind != owner_kind:
                continue
            bounds.append((stamp(event) - completed, stamp(event) - started))
    if not bounds:
        return missing
    # Closed integer-nanosecond intervals vote on each sweep segment. Keep the
    # widest maximum-support segment; use the lowest offset to break a tie.
    changes = Counter()
    for low, high in bounds:
        changes[low] += 1
        changes[high + 1] -= 1
    points = sorted(changes)
    active, best, low, high = 0, 0, 0, 0
    for index, point in enumerate(points[:-1]):
        active += changes[point]
        end = points[index + 1] - 1
        if active > best or (active == best and end - point > high - low):
            best, low, high = active, point, end
    count = len(bounds)
    return {'status': 'inconsistent' if best * 2 < count else 'unaligned' if high-low > MS else 'aligned',
            'lower_ns': low, 'upper_ns': high, 'width_ns': high-low, 'samples': count,
            'inliers': best, 'outliers': count - best}


def groups_of(timeline):
    groups = {}
    memberships = {}
    for index, live in enumerate(timeline.get('live_groups', [])):
        for batch in live.get('batches', []):
            memberships[(batch['epoch'], batch['sequence'])] = (index, live)
    for record in timeline.get('records', []):
        events = record.get('events', [])
        by_stage = {stage(e): e for e in events}
        end = by_stage.get('live_io_completed')
        start = by_stage.get('live_io_submitted', by_stage.get('live_io_started'))
        if not start or not end:
            continue
        batch = record.get('batch') or {}
        membership = memberships.get((batch.get('epoch'), batch.get('sequence')))
        key = ('live_group', membership[0]) if membership else (start.get('owner', 0), start['elapsed_ns'], end['elapsed_ns'])
        if key not in groups:
            live = membership[1] if membership else {}
            groups[key] = {'owner': live.get('owner', start.get('owner', 0)),
                           'start_ns': start['elapsed_ns'], 'end_ns': end['elapsed_ns'],
                           'fallback_started': stage(start) == 'live_io_started',
                           'members': [], 'events': events,
                           'geometry': {**live, **record.get('io', {}), **attributes(start)}}
        groups[key]['members'].append(record.get('batch', record.get('caller_sample_id')))
    return sorted(groups.values(), key=lambda g: g['start_ns'])


TRACE = re.compile(r'^\s*(?P<worker>.*?)\s+\[\d+\].*?\s(?P<time>\d+\.\d+):\s+(?P<event>\w+):\s*(?P<args>.*)$')
FIELDS = re.compile(r'(\w+)=\s*(0x[0-9a-fA-F]+|-?\d+|[^\s,]+)')


def parse_ftrace(text):
    points, intervals, pending, unmatched = [], [], {}, []
    for number, line in enumerate(text.splitlines(), 1):
        m = TRACE.match(line)
        if not m:
            if line.strip() and not line.lstrip().startswith('#'):
                unmatched.append({'line': number, 'reason': 'unparsed'})
            continue
        p = {'time_ns': int(Decimal(m['time']) * 1_000_000_000),
             'event': m['event'], 'worker': m['worker'], 'line': number,
             'fields': dict(FIELDS.findall(m['args']))}
        points.append(p)
        name, f = p['event'], p['fields']
        kind = 'nvmet' if name.startswith('nvmet_req_') else 'jbd2' if name in ('jbd2_start_commit', 'jbd2_end_commit') else 'ext4' if name.startswith('ext4_') else None
        if kind:
            identity = tuple(f.get(k) for k in (('req', 'ctrl_id', 'qid', 'cid', 'cmdid') if kind == 'nvmet' else ('dev', 'transaction') if kind == 'jbd2' else ('dev', 'ino')))
            key = (kind, identity, p['worker'] if kind == 'ext4' else None)
            is_start = name.endswith(('_init', '_enter')) or name == 'jbd2_start_commit'
            if is_start:
                if key in pending:
                    unmatched.append({'point': pending[key], 'reason': 'missing_end'})
                pending[key] = p
            elif key in pending:
                first = pending.pop(key)
                intervals.append({'kind': kind, 'start_ns': first['time_ns'], 'end_ns': p['time_ns'],
                                  'worker': first['worker'], 'fields': first['fields'],
                                  'start_line': first['line'], 'end_line': number})
            else:
                unmatched.append({'point': p, 'reason': 'missing_start'})
    unmatched.extend({'point': p, 'reason': 'missing_end'} for p in pending.values())
    return points, intervals, unmatched


def overlap(interval, window):
    return interval['start_ns'] < window[1] and interval['end_ns'] > window[0]


def number(fields, *keys):
    for key in keys:
        if fields.get(key) is not None:
            value = fields[key]
            return int(value, 0) if isinstance(value, str) and value.startswith('0x') else int(value)
    return None


def own_command(command, geometry):
    f = command['fields']
    ns, expected = number(f, 'nsid', 'namespace'), number(geometry, 'nsid', 'namespace_id', 'namespace')
    lba = number(f, 'slba', 'lba')
    offset = number(geometry, 'file_offset', 'offset', 'offset_bytes')
    block = number(geometry, 'namespace_block_size', 'block_size', 'logical_block_size')
    frame = number(geometry, 'frame_len')
    length = number(f, 'len', 'nlb')
    return (lba is not None and offset is not None and block is not None and block > 0
            and lba * block == offset and (expected is None or ns == expected)
            and frame is not None and length is not None
            and (length + 1) * block == frame)


def analyze(timeline, owner_events=(), ftrace=''):
    timeline = timeline.get('timeline', timeline)
    clock = alignment(timeline, owner_events)
    if clock['status'] == 'inconsistent':
        return {'clock': clock, 'counts': {k: 0 for k in CLASSES}, 'joins': [],
                'error': 'inconsistent owner clock bounds; join stopped',
                'unmatched_ftrace': [], 'missing_trace': not bool(ftrace)}
    points, intervals, unmatched = parse_ftrace(ftrace)
    groups = groups_of(timeline)
    joins = []
    seen_files = set()
    previous_end = -1
    for group in groups:
        geometry = group['geometry']
        file_id = geometry.get('wal_file', geometry.get('file_id'))
        new_file = file_id is not None and file_id not in seen_files
        if file_id is not None:
            seen_files.add(file_id)
        rebound = any(stage(e) == 'live_queue_rebound' and previous_end < e['elapsed_ns'] <= group['start_ns'] for e in group['events'])
        prior_end = previous_end
        previous_end = group['end_ns']
        if group['end_ns'] - group['start_ns'] <= 3 * MS:
            continue
        row = {k: v for k, v in group.items() if k not in ('events', 'geometry')}
        row.update(classification='unattributed', duration_ns=group['end_ns']-group['start_ns'], commands=[], fs_overlaps=[], dirty_pauses=[], control_starts=[], missing_points=[])
        if clock['status'] == 'no_owner_clock' or group['owner'] != 0:
            row['missing_points'].append(clock['status'] if group['owner'] == 0 else 'owner_not_captured')
            joins.append(row)
            continue
        origin = timeline['origin_monotonic_ns']
        lo, hi = clock['lower_ns'], clock['upper_ns']
        possible = (origin+group['start_ns']+lo, origin+group['end_ns']+hi)
        certain = (origin+group['start_ns']+hi, origin+group['end_ns']+lo)
        rebound = rebound or any(stage(e) == 'live_queue_rebound' and stamp(e) is not None
                                 and origin+prior_end+lo < stamp(e) <= possible[0]
                                 for e in owner_events)
        row['owner_window_ns'] = {'possible': possible, 'certain': certain}
        row['alignment_status'] = clock['status']
        row['owner_events'] = [e for e in owner_events if stamp(e) is not None and possible[0] <= stamp(e) <= possible[1]]
        row['ftrace_points'] = [p for p in points if possible[0] <= p['time_ns'] <= possible[1]]
        if not all(number(geometry, *keys) is not None for keys in
                   (('file_offset', 'offset', 'offset_bytes'), ('frame_len',),
                    ('namespace_block_size', 'block_size', 'logical_block_size'))):
            row['missing_points'].append('live_group_geometry_unavailable')
        commands = [i for i in intervals if i['kind'] == 'nvmet' and overlap(i, possible) and own_command(i, geometry)]
        row['commands'] = commands
        row['command_match_status'] = 'ambiguous' if len(commands) > 1 else 'matched' if commands else 'unavailable'
        if not commands:
            row['missing_points'].append('matching_nvmet_command')
        fs = [i for i in intervals if i['kind'] in ('jbd2', 'ext4') and overlap(i, possible)]
        row['fs_overlaps'] = fs
        pauses = []
        for p in points:
            if p['event'] != 'balance_dirty_pages':
                continue
            # Kernel tracepoint pause is in milliseconds (negative means no sleep).
            pause = number(p['fields'], 'pause')
            if pause is not None and pause >= 1:
                interval = {**p, 'start_ns': p['time_ns']-pause*MS, 'end_ns': p['time_ns']}
                if overlap(interval, possible):
                    pauses.append(interval)
        row['dirty_pauses'] = pauses
        starts = [e for e in owner_events if stamp(e) is not None and stage(e).startswith(('prepare_', 'lease_namespace_sync_', 'proof_', 'frontier_sync_')) and stage(e).endswith(('start', 'started'))]
        row['control_starts'] = [e for e in starts if any(i['kind'] == 'jbd2' and 0 <= i['start_ns']-stamp(e) <= 5*MS for i in fs)]
        nvmet_workers = {p['worker'] for p in points if p['event'].startswith('nvmet_req_')}
        if any(('nvmet' in p['worker'] or p['worker'] in nvmet_workers) and overlap(p, possible) for p in pauses):
            row['classification'] = 'M1_dirty_throttle'
        elif row['control_starts'] and any(i['kind'] == 'jbd2' and overlap(i, possible) and any(0 <= i['start_ns']-stamp(e) <= 5*MS for e in row['control_starts']) for i in fs):
            row['classification'] = 'M2_journal_commit'
        elif (rebound or new_file) and not fs:
            row['classification'] = 'M3_queue_rebind'
        elif len(commands) == 1 and commands[0]['end_ns']-commands[0]['start_ns'] <= MS:
            row['classification'] = 'M4_target_fast'
        joins.append(row)
    counts = Counter(r['classification'] for r in joins)
    return {'clock': clock, 'counts': {k: counts[k] for k in CLASSES}, 'joins': joins,
            'unmatched_ftrace': unmatched, 'missing_trace': not bool(points),
            'interpretation': 'First-match evidence classes; overlap is not proof of causality.'}


def load_case(case_dir):
    root = Path(case_dir)
    timeline = json.loads((root/'result/write-timeline.json').read_text())
    owner_path = root/'owner-events/node-0.json'
    owner = json.loads(owner_path.read_text()) if owner_path.exists() else []
    if isinstance(owner, dict):
        owner = owner.get('events', [])
    trace_path = root/'owner-ftrace.txt'
    return analyze(timeline, owner, trace_path.read_text() if trace_path.exists() else '')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('case_dir', type=Path)
    parser.add_argument('--json', type=Path)
    args = parser.parse_args(argv)
    try:
        report = load_case(args.case_dir)
    except (OSError, ValueError) as error:
        parser.exit(2, str(error)+'\n')
    print('Clock bounds/status: ' + json.dumps(report['clock'], sort_keys=True))
    print('class          groups')
    for name, count in report['counts'].items():
        print(f'{name:14s} {count}')
    print(json.dumps(report, indent=2, sort_keys=True))
    if args.json:
        args.json.write_text(json.dumps(report, indent=2, sort_keys=True)+'\n')
    if report['clock']['status'] == 'inconsistent':
        parser.exit(2, report['error']+'\n')


if __name__ == '__main__':
    main()
