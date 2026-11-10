#!/usr/bin/env python3
"""Render the brief's GCP figures from the retained comparison and record provenance."""
import csv
import hashlib
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

HERE = Path(__file__).resolve().parent
CAMPAIGN = HERE.parents[3] / 'checkpoint-work' / 'gcp-kv-comparison-2026-11-03-b35a62e24b47faaf'
CSV = CAMPAIGN / 'metrics-all-populations.csv'
COMPARISON = CAMPAIGN / 'input' / 'comparison.json'
SIZES = (64, 256, 1024, 8192, 65536)
BACKENDS = (
    ('native', 'Native (4 MiB WAL files)'), ('tidehunter_tcp', 'Tidehunter TCP'),
    ('tidehunter_verbs_native', 'Tidehunter Falcon/verbs'), ('valkey_tcp', 'Valkey TCP'),
    ('valkey_rdma', 'Valkey RDMA'), ('garnet', 'Garnet'), ('kvrocks', 'Kvrocks'),
    ('cassandra', 'Cassandra'), ('tikv', 'TiKV'), ('foundationdb', 'FoundationDB'),
)
STYLES = ('-', '--', '-.', ':', '-', '--', '-.', ':', '-', '--')
MARKERS = ('o', 's', '^', 'D', 'v', 'P', 'X', '<', '>', '*')
COLORS = ('#111111', '#0072B2', '#0072B2', '#009E73', '#009E73', '#D55E00',
          '#CC79A7', '#E69F00', '#6A3D9A', '#8C564B')


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


cases = json.loads(COMPARISON.read_text())['cases']
selected = {}
for case in cases:
    job = case['job']
    if (job['cohort'] != 'primary_rf1' or job['profile'] not in ('read-concurrent', 'write-concurrent')
            or job['value_size'] not in SIZES or job['repeat'] != 0):
        continue
    key = (job['profile'], job['backend'], job['value_size'])
    if key in selected or case['status'] != 'completed':
        raise ValueError(f'duplicate or incomplete case: {key}')
    operation = 'read' if job['profile'] == 'read-concurrent' else 'put'
    populations = [p for p in case['metrics']['populations']
                   if p['operation'] == operation and p['outcome'] == 'success']
    if len(populations) != 1:
        raise ValueError(f'expected one successful population: {key}')
    population = populations[0]
    selected[key] = {
        'case_id': case['case_id'], 'operation': operation, 'outcome': 'success',
        'sample_count_api': population['api_ns']['sample_count'],
        'sample_count_response': population['response_ns']['sample_count'],
        'api_p50_ns': population['api_ns']['p50'], 'api_p99_ns': population['api_ns']['p99'],
        'response_p99_ns': population['response_ns']['p99'],
        'api_p999_ns': population['api_ns']['p999'], 'api_max_ns': population['api_ns']['max'],
        'elapsed_seconds': case['metrics']['elapsed_seconds'],
        'value_payload_mib_per_second': case['metrics']['success_calls_per_second'] * job['value_size'] / 1024**2,
        'success_calls_per_second': case['metrics']['success_calls_per_second'],
    }

plt.rcParams.update({'font.size': 10, 'axes.titlesize': 10, 'axes.labelsize': 10,
                     'xtick.labelsize': 9, 'ytick.labelsize': 9, 'legend.fontsize': 9,
                     'pdf.fonttype': 42})
figure_metadata = {}
standard_panels = (('api_p50_ns', 'API p50', 'Latency (µs)', 0.001),
                   ('api_p99_ns', 'API p99', 'Latency (µs)', 0.001),
                   ('response_p99_ns', 'Response p99', 'Latency (µs)', 0.001),
                   ('success_calls_per_second', 'Whole-case successful calls', 'Calls/s', 1))
diagnostic_panels = (('api_p999_ns', 'API p99.9 — descriptive only', 'Latency (µs)', 0.001),
                     ('api_max_ns', 'Longest observed API call', 'Latency (µs)', 0.001),
                     ('elapsed_seconds', 'Recorded measurement exposure', 'Seconds', 1),
                     ('value_payload_mib_per_second', 'Successful value payload', 'Value MiB/s', 1))
figures = (
    ('write-concurrent', 'gcp-primary-write-concurrent.pdf', 'Concurrent writes · C16', standard_panels),
    ('read-concurrent', 'gcp-primary-read-concurrent.pdf', 'Concurrent single-value reads · C16', standard_panels),
    ('write-concurrent', 'gcp-write-diagnostics.pdf', 'Write tails, exposure and value bandwidth · C16', diagnostic_panels),
)
for profile, name, heading, panels in figures:
    fig, axes = plt.subplots(2, 2, figsize=(7.0, 6.8))
    for index, (backend, label) in enumerate(BACKENDS):
        values = [selected[(profile, backend, size)] for size in SIZES]
        for ax, (field, title, ylabel, factor) in zip(axes.flat, panels):
            ax.plot(SIZES, [entry[field] * factor for entry in values],
                    label=label, color=COLORS[index], linestyle=STYLES[index],
                    marker=MARKERS[index], markersize=4, linewidth=1.3)
            ax.set_yscale('log')
            ax.set_xscale('log', base=2)
            ax.set_title(title)
            ax.set_ylabel(ylabel)
            ax.set_xticks(SIZES, ('64', '256', '1K', '8K', '64K'))
            ax.set_xlabel('Value size (bytes)')
            ax.grid(axis='y', which='major', alpha=0.25)
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.suptitle(heading, y=0.997, fontsize=12)
    fig.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5, 0.96),
               ncol=2, frameon=False, columnspacing=1.3, handlelength=2.4)
    fig.subplots_adjust(top=0.74, bottom=0.10, left=0.11, right=0.98, hspace=0.43, wspace=0.37)
    calls = '4,096 writes' if profile == 'write-concurrent' else '8,192 reads'
    fig.text(0.5, 0.012, f'Primary RF1 · {calls}/case · 1 repetition · finite exposure, not sustained capacity',
             ha='center', fontsize=8)
    output = HERE / 'figures' / name
    fig.savefig(output)
    plt.close(fig)
    figure_metadata[name] = {
        'sha256': digest(output), 'source': 'input/comparison.json',
        'profile': profile, 'cohort': 'primary_rf1', 'concurrency': 16, 'repeat': 0,
        'metrics': [panel[0] for panel in panels],
        'points': [{'backend': backend, 'value_bytes': size, **selected[(profile, backend, size)]}
                   for backend, _ in BACKENDS for size in SIZES],
        'note': 'Successful operation quantiles; whole-case rates; log-scaled actual value sizes; '
                'one independent repetition, no error bars. p99.9 is descriptive with this sample count.',
    }

with CSV.open(newline='') as source:
    rows = [row for row in csv.DictReader(source)
            if row['cohort'] == 'primary_rf1' and row['value_bytes'] == '1024'
            and row['profile'] in ('read-concurrent', 'write-concurrent', 'read-many32')
            and row['outcome'] == 'success']
record = {
    'run_id': 'bench-b35a62e24b47faaf', 'campaign_directory': str(CAMPAIGN),
    'source_sha256': '98cc1eb67e1d2c269294a391acf0bcdfc9ef029813ef348a89e92939f10d6c0a',
    'binary_sha256': '34bb9d91cf9bf3c2e343827cd20ee8b1526cdcd764bc16800c4c54476de1c065',
    'independent_repetitions': 1, 'monitoring_complete': False,
    'metrics_csv_sha256': digest(CSV), 'comparison_json_sha256': digest(COMPARISON),
    'figure_manifest_sha256': digest(CAMPAIGN / 'figures' / 'plots.json'),
    'figures': figure_metadata,
    'selection': '1 KiB primary RF1 successful concurrent read, write and 32-value read; all available clocks; raw nanoseconds',
    'rows': rows,
}
(HERE / 'evidence.json').write_text(json.dumps(record, indent=2) + '\n')
