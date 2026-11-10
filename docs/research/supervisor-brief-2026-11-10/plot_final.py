#!/usr/bin/env python3
"""Make the vector figures of the 2026-11-07 brief and the point-level evidence manifest.

Inputs: populations.csv, previous-populations.csv, device-reads.csv (see build_inputs.py).
Outputs: figures/*.pdf and evidence.json (source hashes, figure hashes, plotted case ids and values).
"""
import hashlib
import json
import random
import statistics
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import LinearSegmentedColormap, LogNorm
from matplotlib.lines import Line2D
from matplotlib.text import Text
from matplotlib.ticker import FuncFormatter
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

from data import COLORS, LABELS, MARKERS, ORDER, SIZES, Data, build_stats

from plot_curves import render_curves
HERE = Path(__file__).resolve().parent
FIGURES = HERE / 'figures'
FIGURES.mkdir(exist_ok=True)
data = Data()
stats = build_stats(data)
FONT_PT = 9.5
plt.rcParams.update({'font.size': FONT_PT, 'axes.titlesize': 10, 'axes.labelsize': FONT_PT,
                     'legend.fontsize': FONT_PT, 'xtick.labelsize': FONT_PT, 'ytick.labelsize': FONT_PT,
                     'pdf.fonttype': 42, 'axes.grid': True, 'grid.alpha': .2, 'axes.axisbelow': True})
SIZE_TICKS = ['64', '256', '1K', '8K', '64K']
CORE = ['native', 'th_tcp', 'th_verbs', 'th_kernel', 'th_spdk', 'valkey_tcp', 'valkey_rdma']
UNVERIFIED_FSYNC = {'cassandra', 'foundationdb', 'tikv'}
manifest = {'independent_repetitions': 1, 'figures': {}}


def sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def record(row):
    return {k: row[k] for k in ('case_id', 'label', 'cohort', 'profile', 'operation', 'size', 'conc',
                                'api_p50_us', 'api_p99_us', 'case_success_calls_per_second',
                                'successful_calls', 'flag_spdk_accounting')}


def save(fig, name, points, note=''):
    fig.canvas.draw()
    text = [item for item in fig.findobj(Text) if item.get_visible() and item.get_text()]
    source_min_pt = min(item.get_fontsize() for item in text)
    page_width_inches = fig.get_tightbbox(fig.canvas.get_renderer()).width + 2 * plt.rcParams['savefig.pad_inches']
    printed_width_inches = (210 / 25.4 - 1.5) * (.86 if name == 'sync-step.pdf' else 1)
    printed_min_pt = source_min_pt * printed_width_inches / page_width_inches
    assert printed_min_pt >= 7, (name, printed_min_pt)
    fig.savefig(FIGURES / name, bbox_inches='tight')
    plt.close(fig)
    manifest['figures'][name] = {
        'sha256': sha256(FIGURES / name), 'note': note, 'points': points,
        'text_size': {'source_min_pt': source_min_pt, 'pdf_width_inches': page_width_inches,
                      'printed_width_inches': printed_width_inches, 'printed_min_pt': printed_min_pt}}


def size_axis(ax):
    ax.set_xscale('log', base=2)
    ax.set_xticks(SIZES)
    ax.set_xticklabels(SIZE_TICKS)
    ax.minorticks_off()


def legend(fig, labels, ncol=7, y=-0.01):
    handles = [Line2D([], [], color=COLORS[k], marker=MARKERS[k], ms=3.5, lw=1.1) for k in labels]
    fig.legend(handles, [LABELS[k] for k in labels], loc='upper center', bbox_to_anchor=(.5, y), ncol=ncol,
               frameon=False)


def curve(ax, label, rows, field, scale=1.0):
    rows = sorted(rows, key=lambda r: r['size'])
    xs = [r['size'] for r in rows]
    ys = [r[field] * scale for r in rows]
    ax.plot(xs, ys, marker=MARKERS[label], ms=3, lw=1.1, color=COLORS[label])
    flagged = [(x, y) for x, y, r in zip(xs, ys, rows) if r['flag_spdk_accounting']]
    if flagged:  # latency is valid; SPDK target counters of these cases are not admitted
        ax.plot(*zip(*flagged), ls='', marker=MARKERS[label], ms=4.2, mfc='white', color=COLORS[label])


COLUMNS = [('api_p50_us', 'API p50 (µs)', 1), ('api_p99_us', 'API p99 (µs)', 1),
           ('case_success_calls_per_second', 'Successful calls/s', 1)]


def grid_figure(name, rows_spec, labels, caption_note):
    fig, axes = plt.subplots(len(rows_spec), 3, figsize=(7.1, (1.95 if len(rows_spec) > 2 else 2.35) * len(rows_spec) + .55))
    axes = np.atleast_2d(axes)
    points = []
    for i, (title, cohort, profile, op) in enumerate(rows_spec):
        for label in labels:
            rows = [data.maybe(label, cohort, profile, op, s) for s in SIZES]
            rows = [r for r in rows if r]
            if not rows:
                continue
            points += [record(r) for r in rows]
            for j, (field, _, scale) in enumerate(COLUMNS):
                curve(axes[i, j], label, rows, field, scale)
        for j, (_, ctitle, _) in enumerate(COLUMNS):
            ax = axes[i, j]
            size_axis(ax)
            ax.set_yscale('log')
            ax.set_title(f'{title}: {ctitle}' if j == 0 else ctitle, loc='left')
            if i == len(rows_spec) - 1:
                ax.set_xlabel('Value bytes')
    present = [k for k in labels if any(p['label'] == k for p in points)]
    legend(fig, present, y=0.0)
    fig.tight_layout()
    save(fig, name, points, caption_note)
# 2. Scoreboard heat maps: Tidehunter over Valkey
WORKLOADS = [('read c1', 'primary_rf1', 'point-mixed', 'read'), ('read c16', 'primary_rf1', 'read-concurrent', 'read'),
             ('put c1', 'primary_rf1', 'point-mixed', 'put'), ('put c16', 'primary_rf1', 'write-concurrent', 'put'),
             ('get_many32 c16', 'primary_rf1', 'read-many32', 'get_many')]
COMPARATORS = [('th_tcp', 'valkey_tcp'), ('th_verbs', 'valkey_rdma'), ('th_kernel', 'valkey_rdma')]
SHORT = {'th_tcp': 'TH TCP', 'th_verbs': 'TH verbs', 'th_kernel': 'TH NVMe-oF', 'valkey_tcp': 'Valkey TCP',
         'valkey_rdma': 'Valkey RDMA'}


def ratio_grid(a, b, field):
    out = np.full((len(WORKLOADS), len(SIZES)), np.nan)
    cells = []
    for i, (_, cohort, profile, op) in enumerate(WORKLOADS):
        for j, size in enumerate(SIZES):
            x, y = data.maybe(a, cohort, profile, op, size), data.maybe(b, cohort, profile, op, size)
            if x and y:
                out[i, j] = x[field] / y[field]
                cells.append({'a': x['case_id'], 'b': y['case_id'], 'ratio': out[i, j]})
    return out, cells


def scoreboard():
    fig, axes = plt.subplots(2, 3, figsize=(7.1, 4.9), gridspec_kw={'hspace': .42})
    norm = LogNorm(vmin=0.25, vmax=4)
    cmap = LinearSegmentedColormap.from_list('blue_neutral_vermilion', ['#0072b2', '#f7f7f7', '#d55e00'])
    fig.subplots_adjust(left=.18, right=.86, bottom=.12, top=.88, wspace=.13, hspace=.62)
    points = []
    for r, (field, name) in enumerate((('api_p50_us', 'p50'), ('api_p99_us', 'p99'))):
        for c, (a, b) in enumerate(COMPARATORS):
            ax = axes[r, c]
            grid, cells = ratio_grid(a, b, field)
            points += [dict(cell, metric=name) for cell in cells]
            image = ax.imshow(np.where(np.isnan(grid), 1.0, grid), cmap=cmap, norm=norm, aspect='auto')
            for i in range(grid.shape[0]):
                for j in range(grid.shape[1]):
                    v = grid[i, j]
                    if np.isnan(v):
                        ax.text(j, i, 'n/a', ha='center', va='center', fontsize=8, color='#777777')
                        continue
                    shown = f'{v:.2f}' if v < 9.995 else f'{v:.0f}'
                    ax.text(j, i, shown, ha='center', va='center', fontsize=8,
                            color='white' if (v > 2.8 or v < 0.36) else 'black')
            ax.set_xticks(range(len(SIZES)))
            ax.set_xticklabels(SIZE_TICKS)
            ax.set_yticks(range(len(WORKLOADS)))
            ax.set_yticklabels([w[0] for w in WORKLOADS] if c == 0 else [])
            ax.grid(False)
            ax.set_title(f'{name}: {SHORT[a]}\n/ {SHORT[b]}', fontsize=FONT_PT, loc='left')
            if r == 1:
                ax.set_xlabel('Value bytes')
    cbar = fig.colorbar(image, ax=axes, ticks=[.25, .5, 1, 2, 4], fraction=.025, pad=.02)
    cbar.ax.minorticks_off()
    cbar.ax.set_yticklabels(['0.25', '0.5', '1', '2', '4'])
    cbar.set_label('Tidehunter / Valkey (blue: lower; vermilion: higher)', fontsize=FONT_PT)
    save(fig, 'scoreboard.pdf', points, 'Success-only quantiles. TH: one full-matrix run; Valkey: medians over historical GCP fleets, days and binaries. Contracts differ.')
# 6. All stores at 1 KiB
def others_figure():
    panels = [('read c16', 'read-concurrent', 'read'), ('put c16', 'write-concurrent', 'put'),
              ('get_many32 c16', 'read-many32', 'get_many')]
    fig, axes = plt.subplots(1, 3, figsize=(7.1, 3.2), sharex=False)
    points = []
    for ax, (title, profile, op) in zip(axes, panels):
        rows = [(label, data.maybe(label, 'primary_rf1', profile, op, 1024)) for label in ORDER]
        rows = [(l, r) for l, r in rows if r]
        rows.sort(key=lambda lr: lr[1]['api_p50_us'])
        points += [record(r) for _, r in rows]
        for y, (label, r) in enumerate(rows):
            hollow = op == 'put' and label in UNVERIFIED_FSYNC
            ax.plot([r['api_p50_us'], r['api_p99_us']], [y, y], color=COLORS[label], lw=.8, alpha=.6)
            ax.plot(r['api_p50_us'], y, 'o', color=COLORS[label], mfc='white' if hollow else COLORS[label], ms=4.5)
            ax.plot(r['api_p99_us'], y, 'x', color=COLORS[label], ms=4.5)
        ax.set_yticks(range(len(rows)))
        plot_labels = {'th_kernel': 'TH kernel', 'th_spdk': 'TH SPDK'}
        ax.set_yticklabels([plot_labels.get(l, LABELS[l]) for l, _ in rows], fontsize=FONT_PT)
        ax.invert_yaxis()
        ax.set_xscale('log')
        ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f'{value:g}' if value < 1000 else f'{value / 1000:g}k'))
        ax.set_title(f'1 KiB {title}', loc='left')
        ax.set_xlabel('API µs: ● p50, × p99')
    fig.tight_layout()
    save(fig, 'others.pdf', points, 'Native/TH: one full-matrix run; others: historical GCP per-run medians across fleets, days and binaries. Hollow put p50: device persistence unverified.')
def family(key):
    cohort, profile, size, conc, op, keys, rate = key
    if cohort == 'sealed_read':
        mode = 'first-touch' if profile.startswith('first_touch') else 'repeat'
        shape = 'batch32' if 'batch32' in profile else 'scalar'
        return f'sealed {mode} {shape}'
    if cohort == 'cold_os_cache':
        return 'cold OS 64 KiB'
    if profile == 'coordinator-cache':
        return 'coordinator cache'
    if profile.startswith('offered'):
        return 'offered load'
    if profile == 'point-mixed':
        return f'c1 mixed {op}' if op in ('read', 'put') else 'c1 mixed other'
    return {'read-concurrent': 'read c16', 'write-concurrent': 'put c16', 'read-many32': 'get_many32 c16',
            'write-batch32': 'put_batch32 c16', 'zipf-read': 'zipf read'}.get(profile, 'other ' + profile)


FAMILIES = ['sealed first-touch scalar', 'sealed first-touch batch32', 'sealed repeat scalar',
            'sealed repeat batch32', 'cold OS 64 KiB', 'coordinator cache', 'read c16', 'get_many32 c16',
            'c1 mixed read', 'c1 mixed put', 'c1 mixed other', 'put c16', 'put_batch32 c16', 'offered load',
            'zipf read']


def coherence_figure():
    fig, ax = plt.subplots(figsize=(7.1, 4.4))
    rng = random.Random(7)
    points = []
    th = data.pairs('th_tcp', 'th_verbs')
    vk = data.pairs('valkey_tcp', 'valkey_rdma')
    lo, hi = stats['twin_p50_lo'], stats['twin_p50_hi']
    ax.axhspan(lo, hi, color='#bbbbbb', alpha=.35, lw=0)
    ax.axhline(1, color='black', lw=.7)
    for pairs, offset, color, marker in ((th, -.17, COLORS['th_verbs'], 'o'), (vk, .17, COLORS['valkey_rdma'], 's')):
        for key, tcp, rdma in pairs:
            fam = family(key)
            x = FAMILIES.index(fam) + offset + rng.uniform(-.09, .09)
            ratio = rdma['api_p50_us'] / tcp['api_p50_us']
            differs = pairs is th and abs(data.device_per_call(rdma)['dev'] - data.device_per_call(tcp)['dev']) > .1
            ax.plot(x, ratio, marker='D' if differs else marker, ms=4.5 if differs else 3.6,
                    mfc='white' if differs else color, mec='#d55e00' if differs else color, mew=1.1 if differs else .6,
                    ls='')
            points.append({'family': fam, 'tcp': tcp['case_id'], 'rdma': rdma['case_id'], 'ratio_p50': ratio,
                           'device_reads_differ': bool(differs)})
    ax.set_yscale('log')
    ax.set_xticks(range(len(FAMILIES)))
    ax.set_xticklabels(FAMILIES, rotation=45, ha='right', fontsize=FONT_PT)
    ax.set_ylabel('RDMA / TCP API p50 (below 1: RDMA faster)')
    handles = [Line2D([], [], marker='o', ls='', color=COLORS['th_verbs'], ms=4, label='TH verbs / TCP'),
               Line2D([], [], marker='D', ls='', mfc='white', mec='#d55e00', ms=4.5,
                      label='TH: SSD accesses differ'),
               Line2D([], [], marker='s', ls='', color=COLORS['valkey_rdma'], ms=4, label='Valkey RDMA / TCP'),
               plt.Rectangle((0, 0), 1, 1, color='#bbbbbb', alpha=.5, label='Twin-run p50 range')]
    ax.set_ylim(.25, 2.6)
    ax.set_yticks([.3, .4, .5, .7, 1, 1.5, 2])
    ax.set_yticklabels(['0.3', '0.4', '0.5', '0.7', '1', '1.5', '2'])
    ax.minorticks_off()
    ax.legend(handles=handles, loc='lower left', bbox_to_anchor=(0, 1.02), ncol=2, frameon=False, fontsize=FONT_PT)
    fig.tight_layout()
    save(fig, 'coherence.pdf', points, 'Workload-matched populations. TH pairs: full-matrix fleet; Valkey: ratios of historical per-run medians; pair-level same-fleet provenance not established.')


from data import Focused, COMPONENTS, SHORT as FOCUSED_SHORT, ROUTES7
SHORT.update(FOCUSED_SHORT)
focused = Focused()
COMP_COLORS = dict(zip(COMPONENTS, [COLORS[k] for k in ORDER]))


def focused_point(series):
    row = focused.series[series]
    return {'series': series, 'case_ids': json.loads(row['case_ids']), 'run': 8,
            'fleet': 'bench-0a8a6af114a64287', 'route': row['route'],
            'contract': row['durability'], 'n_by_repetition': json.loads(row['n']),
            'repetitions': int(row['repetition_count']), 'mean_us': row['mean_us'], 'p50_us': row['p50_us']}


def stack_panel(ax, series, title):
    points = []
    for y, s in enumerate(series):
        point = focused_point(s)
        point['components'] = []
        left = 0
        for token, name in COMPONENTS.items():
            row = focused.comp(s, token)
            if row is None or row['mean_us'] == 'missing':
                continue
            value = float(row['mean_us'])
            assert value >= 0, (s, name, value)
            ax.barh(y, value, left=left, color=COMP_COLORS[token], height=.72,
                    edgecolor='white', linewidth=.35, label=token if y == 0 else None)
            point['components'].append(dict(component=name, value_us=value,
                                            exactness=row['exactness'], population_n=row['population_n'],
                                            selection=row['selection']))
            left += value
        total = float(focused.series[s]['mean_us'])
        if not point['components']:
            ax.barh(y, total, facecolor='none', edgecolor='black', hatch='///', height=.72)
            point['whole_call_only'] = True
        else:
            assert abs(left - total) < 1e-7, (s, left, total)
        ax.text(total + max(float(focused.series[x]['mean_us']) for x in series) * .018,
                y, f'{total:.1f}', va='center', fontsize=FONT_PT)
        points.append(point)
    ax.set_yticks(range(len(series)))
    ax.set_yticklabels([SHORT[focused.series[s]['route']] for s in series], fontsize=FONT_PT)
    ax.invert_yaxis()
    ax.set_xlim(0, max(float(focused.series[x]['mean_us']) for x in series) * 1.35)
    ax.set_xlabel('API mean (µs); TOTAL at bar end')
    ax.set_title(title, loc='left')
    return points


def component_legend(fig, tokens, y):
    names = {'persist': 'Owner append-persist', 'network': 'Network / stack',
             'coordinator': 'Coordinator / native insert', 'owner': 'Owner software',
             'durable': 'Durable wait / native file sync', 'coalescing': 'Waiting / coalescing',
             'queue': 'Queue / wake-up', 'residual': 'Residual', 'messaging': 'Client messaging',
             'device': 'SSD device', 'combined': 'Device + fabric + stack', 'exchange': 'Whole exchange'}
    handles = [plt.Rectangle((0, 0), 1, 1, color=COMP_COLORS[t], label=names[t]) for t in tokens]
    fig.legend(handles=handles, loc='lower center', bbox_to_anchor=(.5, y), ncol=3, frameon=False, fontsize=FONT_PT)


def durable_figure():
    fig, axes = plt.subplots(1, 2, figsize=(7.1, 3.7), gridspec_kw={'width_ratios': [1.45, 1]})
    points = stack_panel(axes[0], ['p1v', 'p1t', 'p1m', 'p1k', 'p1s', 'p1n'], 'Run 8: durable put, 1 KiB c1')
    points += stack_panel(axes[1], ['p64v', 'p64t', 'p64n'], '64 KiB c1')
    component_legend(fig, ['persist', 'network', 'coordinator', 'owner', 'durable', 'coalescing', 'queue', 'residual'], .005)
    fig.tight_layout(rect=(0, .21, 1, 1))
    save(fig, 'durable-components.pdf', points, 'Focused unified fleet; one profiled repetition per case; synchronous owner or native barrier. Native file sync is in durable, not owner persist.')


def read_figure():
    fig, axes = plt.subplots(2, 2, figsize=(7.1, 6.1))
    points = []
    for ax, series, title in [
        (axes[0, 0], ['mem1v', 'mem1t', 'mem1m', 'mem1k'], 'Run 8: memory, 1 KiB c1'),
        (axes[0, 1], ['ssd1v', 'ssd1t', 'ssd1m', 'ssd1k', 'ssd1s'], 'Run 8: SSD, 1 KiB c1'),
        (axes[1, 0], ['mem64v', 'mem64t', 'mem64k', 'mem64n'], 'Memory, 64 KiB c1'),
        (axes[1, 1], ['ssd64v', 'ssd64t', 'ssd64k', 'ssd64s', 'ssd64n'], 'SSD, 64 KiB c1')]:
        points += stack_panel(ax, series, title)
    component_legend(fig, ['network', 'owner', 'device', 'combined', 'messaging', 'coordinator', 'queue', 'residual'], .005)
    fig.tight_layout(rect=(0, .13, 1, 1))
    save(fig, 'read-components.pdf', points, 'Focused unified fleet; owner/hybrid: one profiled repetition. Native: whole-call median of two repetition statistics, hatched and unsplit. Memory is serving-node memory, not coordinator-cache hits.')


def sync_figure():
    fig, axes = plt.subplots(1, 3, figsize=(7.1, 3.0))
    points = []
    for ax, size, c in zip(axes, [1024, 1024, 65536], [1, 16, 1]):
        rows = [r for r in focused.table('run8-sync_step') if r['size_bytes'] == str(size)
                and r['concurrency'] == str(c) and r['route'] in
                ('native', 'owner TCP', 'owner verbs', 'owner0 overwrite floor')
                and r['step'] in ('file fdatasync', 'file + directory fsync') and '-r1-' not in r['case_id']]
        order = ['native', 'owner TCP', 'owner verbs', 'owner0 overwrite floor']
        rows.sort(key=lambda r: order.index(r['route']))
        for x, r in enumerate(rows):
            ax.bar(x-.17, float(r['mean_us']), width=.32, color=COLORS['th_tcp'], label='Mean' if x == 0 else None)
            if r['p50_us'] != 'missing':
                ax.bar(x+.17, float(r['p50_us']), width=.32, color=COLORS['th_verbs'], label='p50' if x == 0 else None)
            else:
                ax.text(x+.17, 2, 'n/c', rotation=90, fontsize=FONT_PT, ha='center')
            points.append(dict(r, run=8, fleet='bench-0a8a6af114a64287', repetitions=1))
        ax.set_xticks(range(len(rows)))
        ax.set_xticklabels([{'native': 'Native', 'owner TCP': 'TCP', 'owner verbs': 'Verbs',
                             'owner0 overwrite floor': 'Overwrite'}[r['route']] for r in rows], rotation=35, ha='right')
        ax.set_title(f'{size//1024} KiB c{c}', loc='left')
        ax.set_ylabel('Sync step (µs)')
    fig.legend(handles=[plt.Rectangle((0,0),1,1,color=COLORS['th_tcp'],label='Mean'),
                        plt.Rectangle((0,0),1,1,color=COLORS['th_verbs'],label='p50')],
               loc='upper center', ncol=2, frameon=False)
    fig.tight_layout(rect=(0,0,1,.94))
    save(fig, 'sync-step.pdf', points, 'Focused unified fleet; r0 selected. Owner fdatasync per retained record; native file+directory fsync per API put. Overwrite is a pre-written file, not an append reference.')


def stall_figure():
    fig, axes = plt.subplots(1, 2, figsize=(7.1, 2.8), sharey=True)
    points = []
    for ax, mode in zip(axes, ['timeline', 'profile']):
        for policy, color, marker, style in [('overwrite', '#0072b2', 'o', '-'),
                                              ('new', '#d55e00', '^', '--')]:
            selected = [r for r in focused.table('run8-stall-points') if r['mode'] == mode and r['policy'] == policy]
            ax.scatter([int(r['cumulative_puts']) for r in selected],
                       [float(r['duration_us']) / 1000 for r in selected], s=13, color=color,
                       marker=marker, label=policy + ' keys')
            fit = next(r for r in focused.table('run8-stall-fits') if r['mode'] == mode and r['policy'] == policy)
            xs = [0, 200000]
            ys = [(float(fit['intercept_us']) + float(fit['slope_us_per_put']) * x) / 1000 for x in xs]
            ax.plot(xs, ys, color=color, linestyle=style, lw=1.0)
            points += [dict(r, run=8, fleet='bench-0a8a6af114a64287', repetitions=1) for r in selected]
            points.append(dict(fit, plotted_fit_endpoints=list(zip(xs, ys))))
        ax.set_xlabel('Cumulative successful puts')
        ax.set_xticks([0, 50000, 100000, 150000, 200000])
        ax.set_xticklabels(['0', '50k', '100k', '150k', '200k'])
        ax.set_title('Run 8: ' + mode + ' copy', loc='left')
        ax.set_ylim(bottom=0)
    axes[0].set_ylabel('Stall duration (ms)')
    axes[1].legend(frameon=False)
    fig.tight_layout()
    save(fig, 'put-stall.pdf', points, 'Focused unified fleet; owner verbs synchronous, 1 KiB c16, one repetition per policy and instrument.')


def run7_figure(kind):
    if kind == 'reads':
        panels = [(1024,1),(8192,1),(65536,1),(1024,16),(65536,16)]
        fig, axes = plt.subplots(1, 5, figsize=(7.1, 2.8), sharey=True)
    else:
        panels = [(1024,1),(65536,1),(1024,16),(65536,16)]
        fig, axes = plt.subplots(1, 4, figsize=(7.1, 2.8), sharey=False)
    points = []
    for ax, (size,c) in zip(axes, panels):
        if kind == 'reads':
            selections = [(t, 'synchronous') for t in ROUTES7]
        else:
            selections = [(t, tier) for t in ['v','t'] for tier in ['synchronous','native']]
        for x,(token,tier) in enumerate(selections):
            row = focused.run7(kind, token, str(size), str(c), tier, 'api_p50_us')
            v,lo,hi = [float(row[k]) for k in ['median','min','max']]
            color = COLORS[{'v':'th_verbs','t':'th_tcp','k':'th_kernel','s':'th_spdk'}[token]]
            ax.bar(x,v,color=color,hatch='///' if tier=='native' else None)
            ax.errorbar(x,v,yerr=[[v-lo],[hi-v]],color='black',capsize=2,lw=.7)
            points.append(dict(row,run=7,fleet='bench-6e40ca83d74146cf',repetition_count=3))
        ax.set_xticks(range(len(selections)))
        ax.set_xticklabels([({'v':'V','t':'T','k':'K','s':'S'}[t] + ('/P' if tier=='native' else ''))
                            for t,tier in selections],fontsize=FONT_PT)
        ax.set_title(f'{size//1024} KiB c{c}',loc='left')
        ax.set_ylim(bottom=0)
    if kind == 'reads':
        axes[0].set_ylim(0, max(float(point['max']) for point in points) * 1.10)
    axes[0].set_ylabel('API p50 (µs), median of 3')
    fig.tight_layout()
    save(fig,'run7-'+kind+'.pdf',points,'Focused device fleet; 3 repetitions; error bars = min to max, not confidence intervals. V verbs, T TCP mTLS, K kernel direct, S SPDK. P = page-cache acknowledgement; other puts synchronous.')


if __name__ == '__main__':
    scoreboard()
    others_figure()
    coherence_figure()
    for name in ['scoreboard.pdf', 'others.pdf', 'coherence.pdf']:
        manifest['figures'][name]['run'] = 'cross-store-context'
        manifest['figures'][name]['fleet'] = 'Native/TH: bench-0d99e8256fff4891; external: historical GCP fleets'
        manifest['figures'][name]['repetitions'] = 'Native/TH: one run; external: medians over 2 to 6 runs, different fleets/days/binaries'
    durable_figure()
    read_figure()
    sync_figure()
    stall_figure()
    run7_figure('reads')
    run7_figure('puts')
    render_curves(data, save, manifest)
    manifest['per_node_table'] = {'source': 'per-node-64k.csv',
                                  'topology': 'storage-topology.csv',
                                  'assumption': 'even spread assumed; not measured per-node API throughput',
                                  'rows': focused.table('per-node-64k')}
    manifest.pop('independent_repetitions')
    manifest['sources'] = json.loads((HERE / 'inputs.json').read_text())['outputs']
    manifest['accounting'] = 'No cross-fleet subtraction; components are from one case only. Missing evidence is not zero.'
    (HERE / 'evidence.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print('Wrote', len(manifest['figures']), 'vector PDFs.')
