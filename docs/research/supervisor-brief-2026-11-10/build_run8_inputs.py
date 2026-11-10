#!/usr/bin/env python3
"""Copy small tables and extract selected owner timelines, stalls and sustained rates.

Never invoke the old build_inputs.py. It needs the unavailable full-matrix export.
No network access. Source CSVs remain unchanged. Every copy/extract is hashed.
"""
import csv
import hashlib
import io
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
WORKSPACE = HERE.parents[3]
OLD = HERE.parent / 'supervisor-brief-2026-11-07'
RUN8 = WORKSPACE / 'checkpoint-work/focused-launch-20261109-unified2/analysis'
RUN7 = WORKSPACE / 'checkpoint-work/focused-launch-20261109-g10b/analysis-final-v2'
CARRIED = ['populations', 'previous-populations', 'device-reads', 'external-spread',
           'write-timeline-spans', 'readmany-trace', 'stage-evidence']
R8 = ['ops_summary', 'component_summary', 'sync_step', 'net_floor', 'ssd_floor',
      'ssd_write_floor', 'stall', 'failed_cases', 'unqualified', 'cold_resident']
R7 = ['device_reads', 'device_puts', 'device_read_spread', 'device_put_spread',
      'sustained_summary', 'put_durability_ratio']


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_rows(path):
    with path.open(newline='') as handle:
        return list(csv.DictReader(handle))


def main():
    manifest = {'sources': {}, 'outputs': {}, 'runs': {
        'full-matrix': {'fleet': 'bench-0d99e8256fff4891', 'date': '6 to 7 November 2026',
                        'repetitions': 'TH/native: 1; external stores: prior per-run medians'},
        'run7': {'fleet': 'bench-6e40ca83d74146cf', 'date': '9 November 2026', 'repetitions': 3},
        'run8': {'fleet': 'bench-0a8a6af114a64287', 'date': '9 November 2026',
                 'repetitions': 'profiled: 1; operations: 1 or 2; network/read floors: 3; write floors: 2'}},
        'excluded': ['decomposition_write.csv', 'focused-summary.json', 'raw exports']}
    for prefix, directory, names in [('', OLD, CARRIED), ('run8-', RUN8 / 'output', R8),
                                      ('run7-', RUN7 / 'output', R7)]:
        for name in names:
            source = directory / (name + '.csv')
            target = HERE / (prefix + name + '.csv')
            target.write_bytes(source.read_bytes())
            manifest['sources'][str(source)] = sha(source)
            manifest['outputs'][target.name] = sha(target)
    for source in [RUN8 / 'REPORT-unified.md', RUN8 / 'output/stall.md',
                   RUN7.parent / 'analysis-final/REPORT-run7.md', RUN7.parent / 'analysis-final/APPLES-TO-APPLES.md',
                   HERE.parents[1] / 'distributed/FOCUSED_CAMPAIGN_2026-11-07.md']:
        manifest['sources'][str(source)] = sha(source)

    def extract(name, records):
        target = HERE / name
        with target.open('w', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=list(records[0]))
            writer.writeheader()
            writer.writerows(records)
        manifest['outputs'][name] = sha(target)

    fits, points = [], []
    for row in read_rows(HERE / 'run8-stall.csv'):
        for policy in ['overwrite', 'new']:
            case = json.loads(row[policy])
            fit = case['duration_fit']
            phases = case.get('phase_counts', {})
            fits.append({'case_id': case['case_id'], 'mode': row['mode'], 'policy': policy,
                         'completions': case['completion_count'], 'stalls': case['count'],
                         'spacing': case['mean_spacing_puts'], 'median_spacing': case['median_spacing_puts'],
                         'intercept_us': fit['intercept_us'],
                         'slope_us_per_put': fit['slope_us_per_put'],
                         'queue_phase': phases.get('owner_enqueue_started -> owner_dequeued', ''),
                         'prepare_phase': phases.get('prepared -> admitted', '')})
            for point in case['stalls']:
                points.append(dict(case_id=case['case_id'], mode=row['mode'], policy=policy,
                                   cumulative_puts=point['cumulative_puts'], duration_us=point['duration_us']))
    extract('run8-stall-fits.csv', fits)
    extract('run8-stall-points.csv', points)
    sustained = []
    for row in read_rows(HERE / 'run7-sustained_summary.csv'):
        late = json.loads(row['last_five_seconds'])
        sustained.append(dict(case_id=row['case_id'], route=row['route'], durability=row['durability'],
                              repeat=row['repeat'], first_puts_s=json.loads(row['first_second'])['puts_per_second'],
                              late_puts_s=sum(x['puts_per_second'] for x in late) / len(late),
                              late_seconds=len(late), average_puts_s=row['successful_calls_per_second']))
    completed8 = set()
    for table in ['component_summary', 'net_floor', 'ssd_floor', 'ssd_write_floor']:
        for row in read_rows(HERE / ('run8-' + table + '.csv')):
            if row.get('status', 'completed') == 'completed':
                completed8.add(row['case_id'])
    completed7 = {r['case_id'] for name in ['device_reads', 'device_puts', 'sustained_summary']
                  for r in read_rows(HERE / ('run7-' + name + '.csv'))}
    failures8 = read_rows(HERE / 'run8-failed_cases.csv')
    extract('run-summary.csv', [
        dict(run='run8', planned=len(completed8) + len(failures8), completed=len(completed8),
             failed=len(failures8), unqualified=len(read_rows(HERE / 'run8-unqualified.csv'))),
        dict(run='run7', planned=len(completed7), completed=len(completed7), failed=0, unqualified=0)])
    extract('run7-sustained-extract.csv', sustained)
    owner_stages = []
    fields = ['handler_ns', 'control_queue_ns', 'appender_queue_ns', 'flusher_queue_ns',
              'append_wal_ns', 'append_validation_ns', 'append_descriptor_preparation_ns',
              'append_persist_ns', 'wal_file_persist_ns']
    selections = [('v1', 'G12D-r0-08a9a20ff91943a18559'),
                  ('t1', 'G12D-r0-946e08ec43aceab58b29'),
                  ('v64', 'G12D-r0-9eaa39f00ad063fef4cf')]
    for alias, case_id in selections:
        source = RUN8 / 'report-root/cases' / case_id / 'result/write-timeline.json'
        records = json.loads(source.read_text())['timeline']['owner_records']
        assert len(records) == 4096
        manifest['sources'][str(source)] = sha(source)
        for record in records:
            # Handler starts after appender dequeue. Its flusher wait is inside.
            # Validation/descriptor stages are nested in append_wal_ns.
            record['handler_remainder_ns'] = (record['handler_ns'] - record['flusher_queue_ns']
                                               - record['append_wal_ns'] - record['append_persist_ns'])
            record['persist_software_ns'] = record['append_persist_ns'] - record['wal_file_persist_ns']
        for field in fields + ['handler_remainder_ns', 'persist_software_ns']:
            values = sorted(record[field] for record in records if record.get(field) is not None)
            assert len(values) == len(records)
            owner_stages.append(dict(case_id=case_id, selection=alias, field=field, n=len(values),
                                     mean_us=sum(values) / len(values) / 1000,
                                     p50_us=values[(len(values) - 1) // 2] / 1000))
    extract('owner-stages.csv', owner_stages)
    source = RUN8 / 'report-root/cases/G12D-r0-0ce5f8eaa8fde37f6566/result/write-diagnostics.json'
    profile = json.loads(source.read_text())['profile']
    stages = profile['stages']
    manifest['sources'][str(source)] = sha(source)
    fields = ['connection_handler_ns', 'connection_control_queue_ns',
              'connection_request_decode_ns', 'connection_append_decode_ns',
              'connection_append_group_build_ns', 'connection_append_wal_ns',
              'connection_append_persist_ns', 'connection_response_encode_ns',
              'storage_persist_total_ns', 'storage_persist_wal_file_ns',
              'storage_persist_publication_ns', 'storage_persist_serialization_lock_wait_ns',
              'storage_persist_state_lock_wait_ns', 'storage_persist_validation_ns']
    sample_count = stages['write_profile_sampled_requests']
    assert sample_count == 4096
    profile_rows = []
    for field in fields:
        denominator = (stages['write_profile_connection_control_queue_samples']
                       if field == 'connection_control_queue_ns' else sample_count)
        total_ns = stages['write_profile_' + field]
        profile_rows.append(dict(case_id='G12D-r0-0ce5f8eaa8fde37f6566', field=field,
                                 total_ns=total_ns, n=denominator, mean_us=total_ns / denominator / 1000))
    for field, summands in [
        ('codec_sum_ns', ['connection_request_decode_ns', 'connection_append_decode_ns',
                          'connection_append_group_build_ns', 'connection_response_encode_ns']),
        ('locks_publication_sum_ns', ['storage_persist_publication_ns',
                                     'storage_persist_serialization_lock_wait_ns',
                                     'storage_persist_state_lock_wait_ns'])]:
        total_ns = sum(stages['write_profile_' + f] for f in summands)
        profile_rows.append(dict(case_id='G12D-r0-0ce5f8eaa8fde37f6566', field=field,
                                 total_ns=total_ns, n=sample_count, mean_us=total_ns / sample_count / 1000))
    extract('owner-profile.csv', profile_rows)
    (HERE / 'inputs.json').write_text(json.dumps(manifest, indent=2) + '\n')
    from build_coherence_inputs import main as coherence_inputs
    coherence_inputs()
    from build_size_spike_inputs import main as size_spike_inputs
    size_spike_inputs()
    print('Copied/extracted', len(manifest['outputs']), 'CSV files. No raw export copied.')


if __name__ == '__main__':
    main()
