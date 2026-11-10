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
from matplotlib.colors import LogNorm
from matplotlib.lines import Line2D
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

from data import COLORS, LABELS, MARKERS, ORDER, SIZES, Data, build_stats

HERE = Path(__file__).resolve().parent
FIGURES = HERE / 'figures'
FIGURES.mkdir(exist_ok=True)
data = Data()
stats = build_stats(data)
plt.rcParams.update({'font.size': 8, 'axes.titlesize': 8.5, 'axes.labelsize': 8, 'legend.fontsize': 7,
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
    fig.savefig(FIGURES / name, bbox_inches='tight')
    plt.close(fig)
    manifest['figures'][name] = {'sha256': sha256(FIGURES / name), 'note': note, 'points': points}


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


# ---------------------------------------------------------------------------------------------
# 1. Read routes (structure only)
def readpaths():
    fig, ax = plt.subplots(figsize=(7.1, 3.2))
    ax.set_xlim(0, 100)
    ax.set_ylim(-1, 46)
    ax.axis('off')
    ax.grid(False)

    def box(x, y, w, h, text, face, bold=False):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle='round,pad=0.2,rounding_size=0.8', fc=face,
                                    ec='#3b4a5a', lw=.8))
        ax.text(x + w / 2, y + h / 2, text, ha='center', va='center', fontsize=7,
                fontweight='bold' if bold else 'normal', linespacing=1.15)

    def arrow(x0, y0, x1, y1):
        ax.add_patch(FancyArrowPatch((x0, y0), (x1, y1), arrowstyle='-|>', mutation_scale=7, lw=.8,
                                     color='#3b4a5a'))

    def label(x, y, text, ha='center', va='bottom'):
        ax.text(x, y, text, ha=ha, va=va, fontsize=6.4, color='#222222', linespacing=1.1)

    box(1, 14, 17, 18, 'Coordinator\n(client process)\nindex lookup\nCRC check\noptional RAM\nread cache', '#fff3d6')
    # route 1: coordinator cache
    box(30, 37, 19, 7, 'RAM cache hit\nno I/O', '#e4f3e4')
    arrow(18.4, 30, 29.6, 40.5)
    label(19.5, 38.5, 'hit', ha='left')
    # route 2: owner RPC
    box(30, 18.5, 19, 9, 'Owner process\nTCP (mTLS) or\nlibfabric verbs', '#e7eefb')
    arrow(18.4, 23, 29.6, 23)
    label(24, 23.8, 'RPC read')
    box(58, 18.5, 13, 9, 'Owner page\ncache', '#f4e6f4')
    arrow(49.4, 23, 57.6, 23)
    label(53.5, 23.8, 'pread /\nio_uring')
    arrow(71.4, 23, 83.6, 23)
    label(77.5, 23.8, 'on miss')
    # route 3: NVMe-oF
    box(30, 3, 19, 10, 'NVMe/RDMA read\ncommand (initiator,\nO_DIRECT)', '#fde8dc')
    arrow(18.4, 16, 29.6, 9.5)
    label(19.5, 8.8, 'miss', ha='left')
    box(57, 8, 20, 7, 'Kernel nvmet\nbuffered file I/O', '#fbe0d0')
    box(57, 0, 20, 6, 'SPDK nvmf\nO_DIRECT file AIO', '#f8d4e6')
    arrow(49.4, 10.5, 56.6, 11.5)
    arrow(49.4, 6.5, 56.6, 3.5)
    arrow(77.4, 12, 83.6, 14.5)
    label(83.2, 15.6, 'on page-\ncache miss', ha='right')
    arrow(77.4, 3, 83.6, 8)
    label(80.5, 0, 'every read', va='top')
    box(84, 6, 15, 26, 'Local SSD\n(owner host)\next4 file\n/dev/nvme0n1', '#e6e6e6', bold=True)
    save(fig, 'readpaths.pdf', [], 'structure only; no measured data')


# ---------------------------------------------------------------------------------------------
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
    cmap = plt.get_cmap('RdYlGn_r')
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
                        ax.text(j, i, 'n/a', ha='center', va='center', fontsize=6, color='#777777')
                        continue
                    shown = f'{v:.2f}' if v < 9.995 else f'{v:.0f}'
                    ax.text(j, i, shown, ha='center', va='center', fontsize=6.4,
                            color='white' if (v > 2.8 or v < 0.36) else 'black')
            ax.set_xticks(range(len(SIZES)))
            ax.set_xticklabels(SIZE_TICKS)
            ax.set_yticks(range(len(WORKLOADS)))
            ax.set_yticklabels([w[0] for w in WORKLOADS] if c == 0 else [])
            ax.grid(False)
            ax.set_title(f'{name}: {SHORT[a]} / {SHORT[b]}', fontsize=7.4, loc='left')
            if r == 1:
                ax.set_xlabel('Value bytes')
    cbar = fig.colorbar(image, ax=axes, ticks=[.25, .5, 1, 2, 4], fraction=.025, pad=.02)
    cbar.ax.minorticks_off()
    cbar.ax.set_yticklabels(['0.25', '0.5', '1', '2', '4'])
    cbar.set_label('Tidehunter / Valkey (green: Tidehunter lower)', fontsize=7)
    save(fig, 'scoreboard.pdf', points, 'ratio of success-only API quantiles; one repetition; contracts differ')


# ---------------------------------------------------------------------------------------------
# 5. Concurrency-1 mixed point operations
def point_figure():
    fig, axes = plt.subplots(2, 2, figsize=(7.1, 4.6))
    points = []
    for i, op in enumerate(('read', 'put')):
        for label in CORE:
            rows = [data.maybe(label, 'primary_rf1', 'point-mixed', op, s) for s in SIZES]
            rows = [r for r in rows if r]
            points += [record(r) for r in rows]
            for j, field in enumerate(('api_p50_us', 'api_p99_us')):
                curve(axes[i, j], label, rows, field)
        for j, t in enumerate(('API p50 (µs)', 'API p99 (µs)')):
            ax = axes[i, j]
            size_axis(ax)
            ax.set_yscale('log')
            ax.set_title(f'c1 mixed {op}: {t}', loc='left')
            if i == 1:
                ax.set_xlabel('Value bytes')
    legend(fig, CORE, y=0.0)
    fig.tight_layout()
    save(fig, 'point.pdf', points, 'point-mixed profile, concurrency 1')


# ---------------------------------------------------------------------------------------------
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
        ax.set_yticklabels([LABELS[l] for l, _ in rows], fontsize=6.6)
        ax.invert_yaxis()
        ax.set_xscale('log')
        ax.set_title(f'1 KiB {title}', loc='left')
        ax.set_xlabel('API µs: ● p50, × p99')
    fig.tight_layout()
    save(fig, 'others.pdf', points, '1 KiB; hollow p50 marker in the put panel: device fsync not verified')


# ---------------------------------------------------------------------------------------------
# 7. Sealed reads
def sealed_figure():
    fig, axes = plt.subplots(2, 2, figsize=(7.1, 4.7))
    paths = ['native', 'th_tcp', 'th_verbs', 'th_kernel', 'th_spdk']
    panels = [('first_touch', 'c1'), ('repeat_read', 'c1'), ('first_touch', 'c16'), ('repeat_read', 'c16')]
    points = []
    for ax, (mode, conc) in zip(axes.flat, panels):
        profile = f'{mode}-scalar-{conc}-baseline'
        for label in paths:
            rows = [data.maybe(label, 'sealed_read', profile, 'read', s) for s in SIZES]
            rows = [r for r in rows if r]
            if not rows:
                continue
            points += [record(r) for r in rows]
            xs = [r['size'] for r in rows]
            ax.plot(xs, [r['api_p50_us'] for r in rows], marker=MARKERS[label], ms=3, lw=1.1, color=COLORS[label])
            ax.plot(xs, [r['api_p99_us'] for r in rows], lw=.8, ls=(0, (3, 2)), color=COLORS[label], alpha=.75)
        size_axis(ax)
        ax.set_yscale('log')
        ax.set_title(f'{mode.replace("_", "-")} scalar {conc}', loc='left')
        ax.set_ylabel('API µs: solid p50, dashed p99')
        if conc == 'c16':
            ax.set_xlabel('Value bytes')
            ax.set_xlim(700, 11500)
            ax.set_xticks([1024, 8192])
            ax.set_xticklabels(['1K', '8K'])
    legend(fig, paths, ncol=5, y=0.0)
    fig.tight_layout()
    save(fig, 'sealed.pdf', points, 'sealed read cohort, synchronous-tier dataset')


# ---------------------------------------------------------------------------------------------
# 8. Cold OS cache, 64 KiB
def cold_figure():
    rows = [(l, data.maybe(l, 'cold_os_cache', 'cold-os-large', 'read', 65536)) for l in ORDER]
    rows = [(l, r) for l, r in rows if r]
    rows.sort(key=lambda lr: lr[1]['api_p50_us'])
    fig, (ax, bx) = plt.subplots(1, 2, figsize=(7.1, 3.5), gridspec_kw={'width_ratios': [3.1, 1.15]}, sharey=True)
    points = []
    for y, (label, r) in enumerate(rows):
        points.append(record(r))
        ax.plot([r['api_p50_us'], r['api_p999_us']], [y, y], color=COLORS[label], lw=.8, alpha=.5)
        ax.plot(r['api_p50_us'], y, 'o', color=COLORS[label], ms=4.5)
        ax.plot(r['api_p95_us'], y, '|', color=COLORS[label], ms=7)
        ax.plot(r['api_p99_us'], y, 'x', color=COLORS[label], ms=4.5)
        ax.plot(r['api_p999_us'], y, 'D', color=COLORS[label], mfc='white', ms=4)
        reads = data.device_per_call(r)['dev']
        bx.barh(y, reads, color=COLORS[label], height=.6)
        bx.text(reads + .1, y, f'{reads:.2f}', va='center', fontsize=6.4)
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels([LABELS[l] for l, _ in rows])
    ax.invert_yaxis()
    ax.set_xscale('log')
    ax.set_xlabel('API µs: ● p50, | p95, × p99, ◇ p99.9')
    ax.set_title('64 KiB cold-OS reads, c16, 4,096 calls', loc='left')
    bx.set_xlim(0, 7.3)
    bx.set_xlabel('Device reads per call')
    bx.set_title('Block-device evidence', loc='left')
    fig.tight_layout()
    save(fig, 'cold.pdf', points, 'cold-os-large cohort; device reads from /proc/diskstats between gates')


# ---------------------------------------------------------------------------------------------
# 9. Coordinator cache
def cache_figure():
    fig, axes = plt.subplots(1, 2, figsize=(7.1, 3.0), sharey=True)
    order = ['native', 'th_tcp', 'th_verbs', 'th_kernel', 'th_spdk', 'valkey_tcp', 'valkey_rdma']
    points = []
    for ax, size in zip(axes, (256, 8192)):
        for x, label in enumerate(order):
            remote = data.maybe(label, 'primary_rf1', 'point-mixed', 'read', size)
            hit = data.maybe(label, 'coordinator_cache_configured', 'coordinator-cache', 'read', size)
            for row, marker, mfc in ((remote, 'o', COLORS[label]), (hit, 's', 'white')):
                if not row:
                    continue
                points.append(record(row))
                ax.plot(x, row['api_p50_us'], marker=marker, color=COLORS[label], mfc=mfc, ms=5)
                ax.plot(x, row['api_p99_us'], 'x', color=COLORS[label], ms=5)
        ax.set_xticks(range(len(order)))
        ax.set_xticklabels([LABELS[l] for l in order], rotation=40, ha='right', fontsize=6.6)
        ax.set_yscale('log')
        ax.set_title(f'{size} B' if size < 1024 else f'{size // 1024} KiB', loc='left')
    axes[0].set_ylabel('API µs at c1: ● read, ◻ cache hit (p50); × p99')
    fig.tight_layout()
    save(fig, 'cache.pdf', points, 'point-mixed read at c1 versus coordinator-cache cohort at c1')


# ---------------------------------------------------------------------------------------------
# 10. Offered load
def offered_figure():
    fig, axes = plt.subplots(1, 3, figsize=(7.1, 3.0), sharey=False)
    points = []
    for ax, rate in zip(axes, ('1000', '10000', '50000')):
        profile = f'offered-{rate}'
        sel = []
        for label in ORDER:
            rows = [r for r in data.select(label, 'primary_rf1', profile, outcome=None)]
            if rows:
                sel.append((label, rows))
        for x, (label, rows) in enumerate(sel):
            success = rows[0]['successful_calls']
            rejected = rows[0]['unsuccessful_calls']
            points.append({'case_id': rows[0]['case_id'], 'label': label, 'success': success, 'rejected': rejected})
            ax.bar(x, success, color=COLORS[label])
            if rejected:
                ax.bar(x, rejected, bottom=success, color='#bb2222', hatch='///', edgecolor='white', lw=0)
        ax.set_ylim(0, max(r[0]['successful_calls'] + r[0]['unsuccessful_calls'] for _, r in sel) * 1.04)
        ax.set_xticks(range(len(sel)))
        ax.set_xticklabels([LABELS[l] for l, _ in sel], rotation=55, ha='right', fontsize=6)
        ax.set_title(f'Offered {int(rate):,}/s', loc='left')
        ax.set_ylabel('Calls (color success, hatched rejected)' if rate == '1000' else '')
    fig.tight_layout()
    save(fig, 'offered.pdf', points, 'mixed 70/30 offered load, 1 KiB, concurrency 32, 8 s')


# ---------------------------------------------------------------------------------------------
# 11. Coherence of RDMA against TCP
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
    fig, ax = plt.subplots(figsize=(7.1, 3.6))
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
                    mfc='white' if differs else color, mec='#bb2222' if differs else color, mew=1.1 if differs else .6,
                    ls='')
            points.append({'family': fam, 'tcp': tcp['case_id'], 'rdma': rdma['case_id'], 'ratio_p50': ratio,
                           'device_reads_differ': bool(differs)})
    ax.set_yscale('log')
    ax.set_xticks(range(len(FAMILIES)))
    ax.set_xticklabels(FAMILIES, rotation=45, ha='right', fontsize=6.6)
    ax.set_ylabel('RDMA / TCP API p50 (below 1: RDMA faster)')
    handles = [Line2D([], [], marker='o', ls='', color=COLORS['th_verbs'], ms=4, label='TH verbs / TH TCP'),
               Line2D([], [], marker='D', ls='', mfc='white', mec='#bb2222', ms=4.5,
                      label='TH, device reads per call differ by more than 0.1'),
               Line2D([], [], marker='s', ls='', color=COLORS['valkey_rdma'], ms=4, label='Valkey RDMA / Valkey TCP'),
               plt.Rectangle((0, 0), 1, 1, color='#bbbbbb', alpha=.5, label='range of 56 twin-run p50 ratios')]
    ax.set_ylim(.25, 2.6)
    ax.set_yticks([.3, .4, .5, .7, 1, 1.5, 2])
    ax.set_yticklabels(['0.3', '0.4', '0.5', '0.7', '1', '1.5', '2'])
    ax.minorticks_off()
    ax.legend(handles=handles, loc='lower left', frameon=True, fontsize=6.4)
    fig.tight_layout()
    save(fig, 'coherence.pdf', points, 'matched populations: same cohort, profile, size, concurrency, op, keys, rate')


# ---------------------------------------------------------------------------------------------
# 12. Previous campaign against this run
def crossrun_figure():
    def jk(r):
        return (r['backend'], r['nvmeof_target'], r['cohort'], r['profile'], r['value_bytes'], r['concurrency'],
                r['operation'], r['outcome'], r['keys'], r['offered_ops_per_second'])
    old = {jk(r): r for r in data.previous if r['outcome'] == 'success'}
    new = {jk(r): r for r in data.select()}
    common = [k for k in new if k in old]
    labels = [l for l in ORDER if any(new[k]['label'] == l for k in common)]
    fig, ax = plt.subplots(figsize=(7.1, 3.1))
    rng = random.Random(11)
    points = []
    for x, label in enumerate(labels):
        ratios = [new[k]['raw_api_p50_us'] / old[k]['api_p50_us'] for k in common if new[k]['label'] == label]
        ax.plot([x + rng.uniform(-.28, .28) for _ in ratios], ratios, '.', color=COLORS[label], ms=3.2, alpha=.55)
        med = statistics.median(ratios)
        ax.plot([x - .35, x + .35], [med, med], color='black', lw=1.4)
        points.append({'label': label, 'n': len(ratios), 'median': med})
    ax.axhline(1, color='#555555', lw=.7)
    ax.set_yscale('log')
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels([LABELS[l] for l in labels], rotation=25, ha='right')
    ax.set_ylabel('This run / previous run, API p50')
    fig.tight_layout()
    save(fig, 'crossrun.pdf', points, 'matched success populations; black bar is the median per backend')


if __name__ == '__main__':
    readpaths()
    scoreboard()
    grid_figure('primary.pdf', [('read c16', 'primary_rf1', 'read-concurrent', 'read'),
                                ('put c16', 'primary_rf1', 'write-concurrent', 'put'),
                                ('get_many32 c16', 'primary_rf1', 'read-many32', 'get_many'),
                                ('put_batch32 c16', 'primary_rf1', 'write-batch32', 'put_batch')], CORE,
                'RF1 scalar and 32-key calls at concurrency 16; Valkey has no batch-write case')
    point_figure()
    others_figure()
    sealed_figure()
    cold_figure()
    cache_figure()
    offered_figure()
    coherence_figure()
    crossrun_figure()
    manifest['stats'] = {k: v for k, v in stats.items()}
    manifest['sources'] = json.loads((HERE / 'inputs.json').read_text())['outputs']
    (HERE / 'evidence.json').write_text(json.dumps(manifest, indent=1, default=str) + '\n')
    print('wrote', len(manifest['figures']), 'figures')
