"""Bounded owner tracing. Only the owned tracefs instance is changed."""
import json
from pathlib import Path
import re

EVENTS = ('jbd2/jbd2_start_commit', 'jbd2/jbd2_end_commit', 'jbd2/jbd2_run_stats',
          'writeback/balance_dirty_pages', 'ext4/ext4_fallocate_enter',
          'ext4/ext4_fallocate_exit', 'ext4/ext4_sync_file_enter',
          'ext4/ext4_sync_file_exit', 'nvmet/nvmet_req_init', 'nvmet/nvmet_req_complete')
MAX_BYTES = 64 * 1024**2
START_STATES = {}


def probe(tracefs=Path('/sys/kernel/tracing')):
    tracefs = Path(tracefs)
    return {'events_enabled': [e for e in EVENTS if (tracefs / 'events' / e / 'enable').exists()],
            'events_missing': [e for e in EVENTS if not (tracefs / 'events' / e / 'enable').exists()]}


def _remove(instance, errors):
    try:
        instance.rmdir()
    except OSError as error:
        errors.append('remove: ' + str(error))


def start(case_id, tracefs=Path('/sys/kernel/tracing')):
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,160}', case_id):
        raise ValueError('invalid owner trace case identity')
    instance = Path(tracefs) / 'instances' / ('th-' + case_id)
    state = probe(tracefs)
    state['errors'] = []
    state['events_enabled'] = []
    try:
        instance.mkdir()
        for name, value in (('trace_clock', 'mono'), ('buffer_size_kb', '16384')):
            try:
                (instance / name).write_text(value)
            except OSError as error:
                state['errors'].append(name + ': ' + str(error))
        for event in EVENTS:
            if event in state['events_missing']:
                continue
            try:
                (instance / 'events' / event / 'enable').write_text('1')
                state['events_enabled'].append(event)
            except OSError as error:
                state['errors'].append(event + ': ' + str(error))
        (instance / 'tracing_on').write_text('1')
    except OSError as error:
        state['errors'].append('start: ' + str(error))
        _remove(instance, state['errors'])
    # State lives outside tracefs, which does not allow normal files.
    START_STATES[str(instance)] = state
    return instance


def stop(instance, output_dir, state=None):
    instance, output_dir = Path(instance), Path(output_dir)
    state = dict(state or {'events_enabled': [], 'events_missing': list(EVENTS), 'errors': []})
    state.update(truncated=False, buffer_overruns=0)
    try:
        try:
            output_dir.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            state['errors'].append('output: ' + str(error))
        try:
            (instance / 'tracing_on').write_text('0')
        except OSError as error:
            state['errors'].append('stop: ' + str(error))
        try:
            with (instance / 'trace').open('rb') as source, (output_dir / 'owner-ftrace.txt').open('wb') as target:
                remaining = MAX_BYTES
                while remaining:
                    block = source.read(min(1024**2, remaining))
                    if not block:
                        break
                    target.write(block)
                    remaining -= len(block)
                state['truncated'] = bool(source.read(1))
        except OSError as error:
            state['errors'].append('trace: ' + str(error))
        for stats in instance.glob('per_cpu/cpu*/stats'):
            try:
                for line in stats.read_text().splitlines():
                    if line.split(':', 1)[0].strip() == 'overrun':
                        state['buffer_overruns'] += int(line.split(':', 1)[1])
            except (OSError, ValueError) as error:
                state['errors'].append('stats: ' + str(error))
    finally:
        _remove(instance, state['errors'])
        try:
            (output_dir / 'owner-ftrace.json').write_text(json.dumps(state, indent=2) + '\n')
        except OSError as error:
            state['errors'].append('metadata: ' + str(error))
    return state
