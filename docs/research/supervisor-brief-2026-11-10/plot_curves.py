"""Contract-labelled curves with per-run evidence, never products of medians."""
import math
import statistics

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter

from data import SIZES, read_csv

SERIES = (
    ('native', 'Native (1 process, 1 SSD)'), ('th_tcp', 'Tidehunter TCP'), ('th_verbs', 'Tidehunter verbs'),
    ('valkey_tcp', 'Valkey TCP'), ('valkey_rdma', 'Valkey RDMA'), ('garnet', 'Garnet'),
    ('kvrocks', 'Kvrocks'), ('cassandra', 'Cassandra'), ('tikv', 'TiKV'),
    ('foundationdb', 'FoundationDB'), ('th_kernel', 'Tidehunter NVMe-oF kernel'),
    ('th_spdk', 'Tidehunter NVMe-oF SPDK'),
)
STYLES = ('-', '--', '-.', ':', '-', '--', '-.', ':', '-', '--',
          (0, (5, 1, 1, 1, 1, 1)), (0, (3, 1, 1, 1, 1, 1, 1, 1)))
MARKERS = ('o', 's', '^', 'D', 'v', 'P', 'X', '<', '>', '*', 'h', 'p')
COLORS = ('#111111', '#0072B2', '#0072B2', '#009E73', '#009E73', '#D55E00',
          '#CC79A7', '#E69F00', '#6A3D9A', '#8C564B', '#56B4E9', '#332288')
SPECS = (
    ('write-c16', 'write-concurrent', 'put', 'Concurrent writes · C16'),
    ('read-c16', 'read-concurrent', 'read', 'Concurrent single-value reads · C16'),
    ('read32-c16', 'read-many32', 'get_many', 'Concurrent 32-key reads · C16'),
    ('point-read-c1', 'point-mixed', 'read', 'Point reads · C1 (mixed stream)'),
    ('point-put-c1', 'point-mixed', 'put', 'Point puts · C1 (mixed stream)'),
)
LATENCY_PANELS = (('api_p50_us', 'API p50', 'Latency (µs)'),
                  ('api_p99_us', 'API p99', 'Latency (µs)'),
                  ('response_p99_us', 'Response p99', 'Latency (µs)'))


def tick_label(value, _position=None):
    if value >= 1_000_000:
        return f'{value / 1_000_000:g}M'
    if value >= 1_000:
        return f'{value / 1_000:g}k'
    return f'{value:g}'


def render_curves(data, save, manifest):
    run_rows = read_csv('curve-runs.csv')
    with plt.rc_context({'font.size': 10, 'axes.titlesize': 10, 'axes.labelsize': 10,
                         'xtick.labelsize': 9, 'ytick.labelsize': 9, 'legend.fontsize': 9,
                         'pdf.fonttype': 42, 'axes.grid': False}):
        for stem, profile, operation, heading in SPECS:
            fourth = (('api_mean_us', 'API mean', 'Latency (µs)') if profile == 'point-mixed' else
                      ('case_success_calls_per_second', 'Successful calls/s (finite case)', 'Calls/s') if operation == 'put' else
                      ('mean_inflight_calls', 'Mean in-flight successful API calls', 'Calls'))
            panels = LATENCY_PANELS + (fourth,)
            fig, axes = plt.subplots(2, 2, figsize=(7.0, 7.8))
            points, missing, selected = [], [], []
            series = list(SERIES)
            if operation == 'put':
                series.append(('native_pagecache', 'Native, page-cache ack (no foreground fsync)'))
            for index, (label, display) in enumerate(series):
                cohort = ('native_pagecache' if profile == 'point-mixed' else 'native_async_write') if label == 'native_pagecache' else 'primary_rf1'
                rows = []
                for size in SIZES:
                    found = [r for r in data.rows if r['label'] == ('native' if label == 'native_pagecache' else label)
                             and r['cohort'] == cohort and r['profile'] == profile and r['operation'] == operation
                             and r['size'] == size and r['outcome'] == 'success' and r['durability_tier'] != 'native']
                    assert len(found) == 1, (label, cohort, profile, size)
                    rows.append(found[0])
                selected.extend(rows)
                color = COLORS[index] if index < len(SERIES) else '#777777'
                for ax, (metric, _, _) in zip(axes.flat, panels):
                    if label == 'native_pagecache' and metric == 'case_success_calls_per_second':
                        continue
                    values = []
                    for row in rows:
                        contributors = [r for r in run_rows if r['label'] == row['label'] and r['cohort'] == cohort
                                        and r['profile'] == profile and r['operation'] == operation and int(r['size']) == row['size']]
                        assert len(contributors) == int(row['median_runs'] or 1), (label, row['size'], len(contributors))
                        value = statistics.median(float(r[metric]) for r in contributors)
                        if metric != 'mean_inflight_calls':
                            assert math.isclose(value, row[metric], rel_tol=1e-12), (label, metric, value, row[metric])
                        values.append(value)
                        points.append({key: row[key] for key in (
                            'case_id', 'cohort', 'profile', 'operation', 'outcome', 'size', 'conc', 'median_runs',
                            'population_calls', 'planned_calls', 'successful_calls', 'durability_tier', 'native_durability', 'nvmeof_target')}
                            | dict(label=label, metric=metric, value=value, repetition_count=len(contributors),
                                   per_run=[dict(run=r['run'], case_id=r['case_id'], source=r['source'], value=float(r[metric]),
                                                 occupancy_method=r['occupancy_method']) for r in contributors]))
                    hollow = operation == 'put' and label in {'cassandra', 'foundationdb', 'tikv'}
                    ax.plot(SIZES, values, label=display, color=color,
                            linestyle=STYLES[index] if index < len(SERIES) else ':',
                            marker=MARKERS[index] if index < len(SERIES) else 'o',
                            markerfacecolor='white' if hollow else color, markersize=5.5 if hollow else 4,
                            markeredgewidth=1.2 if hollow else .7, linewidth=1.3)
            y_ticks = []
            for ax, (metric, title, ylabel) in zip(axes.flat, panels):
                ax.set_title(title, fontsize=9 if len(title) > 25 else 10)
                ax.set_yscale('log')
                if metric == 'mean_inflight_calls':
                    ax.set_ylim(.2 if profile == 'read-concurrent' else 1, 20)
                    ax.axhline(16, color='#aaaaaa', linewidth=.6)
                lower, upper = ax.get_ylim()
                locator = LogLocator(base=10, subs=(1,))
                ticks = locator.tick_values(lower, upper)
                if math.log10(upper / lower) < 2 or sum(lower <= value <= upper for value in ticks) < 3:
                    locator = LogLocator(base=10, subs=(1, 2, 5))
                ax.yaxis.set_major_locator(locator)
                ax.yaxis.set_major_formatter(FuncFormatter(tick_label))
                ax.yaxis.set_minor_formatter(NullFormatter())
                visible = [float(value) for value in locator.tick_values(lower, upper) if lower <= value <= upper]
                assert len(visible) >= 3
                y_ticks.append(dict(metric=metric, values=visible, labels=[tick_label(value) for value in visible]))
                ax.set_xscale('log', base=2)
                ax.set_ylabel(ylabel)
                ax.set_xticks(SIZES, ('64', '256', '1K', '8K', '64K'))
                ax.set_xlabel('Value size (bytes)')
                ax.grid(axis='y', which='major', alpha=.25)
            calls = {int(row['population_calls']) for row in selected}.pop()
            total = {int(row['planned_calls']) for row in selected}.pop()
            runs = [int(row['median_runs']) for row in selected if row['median_runs']]
            handles, labels = axes[0, 0].get_legend_handles_labels()
            fig.suptitle(heading, y=.998, fontsize=12)
            fig.legend(handles, labels, loc='upper center', bbox_to_anchor=(.5, .962), ncol=2,
                       frameon=False, columnspacing=1.0, handlelength=2.4)
            fig.subplots_adjust(top=.68, bottom=.31 if operation == 'put' else .27,
                                left=.11, right=.98, hspace=.65, wspace=.37)
            lines = [
                'Native/TH: one full-matrix run; others: median over 2–6 GCP runs',
                '(different fleets, days, binaries); hardware/configuration not fully matched.',
                'Native: 1 process / 1 SSD. Tidehunter: 5 configured owners.',
                'Other external contributors record five configured or observed storage nodes;',
                "Cassandra's 3 November run records five contacts, not a verified five-node ring.",
                'Five storage-host SSD mounts attested only for inspected 4 November/final deployments.',
                'FoundationDB has a separate log/coordinator host. Even spread is not measured.',
                f'{calls:,} successful {"32-key calls" if operation == "get_many" else operation + "s"}/point/run; finite exposure, not sustained capacity.',
                'X: K = 1024 bytes. Y: k = 1000, M = 10⁶. Cache paths differ; no SSD-read claim.',
            ]
            if operation == 'put':
                lines += ['Native: per-put file + directory fsync; TH: covering fdatasync; Valkey: AOF always.',
                          'Kvrocks: synced WAL; Garnet: AOF commit. Hollow: Cassandra/FDB/TiKV persistence unverified.']
            notes = [fig.text(.04, .015 + (len(lines) - index - 1) * .022, line, ha='left', fontsize=7.5)
                     for index, line in enumerate(lines)]
            fig.canvas.draw()
            renderer = fig.canvas.get_renderer()
            assert max(note.get_window_extent(renderer).y1 for note in notes) < min(
                ax.get_tightbbox(renderer).y0 for ax in axes.flat), 'Figure notes overlap panel labels'
            name = f'curves-{stem}.pdf'
            save(fig, name, points, ' '.join(lines))
            manifest['figures'][name].update(
                run='cross-store-context', fleet='Native/TH: bench-0d99e8256fff4891; external: historical GCP fleets',
                source='populations.csv + curve-runs.csv', cohort='primary_rf1 (page-cache Native separately labelled)',
                profile=profile, operation=operation, metrics=[p[0] for p in panels], missing_points=missing,
                population_calls_per_point_per_run=calls, planned_calls_per_case=total,
                external_median_runs_min=min(runs), external_median_runs_max=max(runs), y_ticks=y_ticks,
                aggregation='one statistic per run, then equal-run median; never pooled calls or products of medians',
                native_pagecache_rate_omission=('No per-put foreground fsync or per-put persistence guarantee; in-flight <=1.26 through 8 KiB, 8.40 at 64 KiB' if profile == 'write-concurrent' else None),
                contracts='Native per-put all-file/directory fsync; TH covering fdatasync; Valkey AOF always; Kvrocks synced WAL; Garnet AOF commit; Cassandra/FDB/TiKV device persistence unverified. Native resident-oriented read workload; recent mapped and older positional-read paths may both serve values; per-call source not established. Valkey RAM GET; owner cache off at coordinator; hybrid target cache contracts differ; owner TCP mTLS versus trusted verbs.')
