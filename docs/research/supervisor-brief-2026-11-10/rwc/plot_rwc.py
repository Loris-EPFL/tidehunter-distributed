#!/usr/bin/env python3
"""Plot each remote_rw.py case separately. No pooled repetitions or floor residuals."""
import argparse
import hashlib
import json
import math
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.colors import ListedColormap
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter, MaxNLocator
import numpy as np

from rwc_data import LABELS, arm, availability_counts, concurrency, floor_factor, floor_spec, load, selected_floor

# Same Okabe--Ito blue/vermilion/neutral family as plot_curves.py; shape is redundant.
COLORS = {'native': '#111111', 'native page_cache': '#777777', 'native value_wal_barrier': '#111111',
          'native value_wal_group_barrier': '#009E73', 'tcp': '#0072B2', 'verbs': '#D55E00'}
MARKERS = {'native': 'o', 'native page_cache': 'v', 'native value_wal_barrier': 'o',
           'native value_wal_group_barrier': 'D', 'tcp': 's', 'verbs': '^'}
STYLES = ('-', '--', ':')
WRITE_CONTRACT = 'Native: page cache (not durable), per-put fsync or grouped barrier\nOwner: covering fdatasync; coordinator cache off; one data SSD per arm'
READ_CONTRACT = 'Coordinator cache off; Native in-process, owner RPC; one data SSD on different hosts\nCold: device-qualified; warm: prewarmed page cache. Read lanes: Native 0; owner 1 at c1, 4 at c16'


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True, help='remote_rw.py OUTPUT')
    p.add_argument('--brief', type=Path, default=Path(__file__).resolve().parent.parent)
    p.add_argument('--synthetic', action='store_true')
    args = p.parse_args(); data = load(args.output); brief = args.brief.resolve()
    fixture = ('SYNTHETIC' in str(args.output.resolve()) or any((x / 'SYNTHETIC').exists() for x in [args.output, *args.output.parents]))
    if fixture and not args.synthetic: p.error('SYNTHETIC input requires --synthetic (no unwatermarked fixtures)')
    prior_manifest = brief / 'rwc/generated/inputs-rwc.json'
    if prior_manifest.is_file():
        if json.loads(prior_manifest.read_text())['synthetic'] != args.synthetic:
            p.error('table/figure synthetic state differs; rebuild tables first')
    figdir = brief / ('rwc/synthetic-figures' if args.synthetic else 'figures'); figdir.mkdir(parents=True, exist_ok=True)
    evidence = {'synthetic': args.synthetic, 'sources': {str((args.output / (f + '.csv')).resolve()): hashlib.sha256((args.output / (f + '.csv')).read_bytes()).hexdigest() for f in data}, 'figures': {}}
    plt.rcParams.update({'font.size': 9, 'axes.titlesize': 9, 'axes.labelsize': 9,
                         'xtick.labelsize': 8, 'ytick.labelsize': 8, 'legend.fontsize': 8,
                         'pdf.fonttype': 42, 'axes.spines.top': False, 'axes.spines.right': False})
    points = []

    def value(family, i, field, factor=1):
        row = data[family][i]
        if row['status'] != 'completed' or row[field] == '': return math.nan
        v = float(row[field]) * factor
        if not math.isfinite(v) or v < 0: raise ValueError('invalid metric')
        points.append(dict(family=family, row=i, case_id=row['case_id'], repetition=row['repetition'],
                           readers=row['readers'], writers=row['writers'], field=field, factor=factor, value=v,
                           mean_inflight_calls=row['mean_inflight_calls'], n=row['n'],
                           read_lanes=row.get('read_lanes'), native_shards=row.get('native_shards'),
                           durability_tier=row.get('durability_tier'), native_durability=row.get('native_durability'),
                           offered_calls=row['offered_calls'], unsuccessful_calls=row['unsuccessful_calls']))
        return v

    def finish(fig, stem, note, legend=True):
        nonlocal points
        if legend:
            handles = [Line2D([], [], color=COLORS[a], marker=MARKERS[a], label=LABELS[a]) for a in
                       ('native page_cache', 'native value_wal_barrier', 'native value_wal_group_barrier', 'tcp', 'verbs')]
            if stem not in ('write-latency', 'throughput'):
                handles = [Line2D([], [], color=COLORS[a], marker=MARKERS[a], label=LABELS[a]) for a in ('native', 'tcp', 'verbs')]
            if stem == 'throughput':
                handles.append(Line2D([], [], color=COLORS['native'], marker=MARKERS['native'], label='Native reads'))
            fig.legend(handles=handles, loc='upper center', bbox_to_anchor=(.5, 1), ncol=3, frameon=False)
        if args.synthetic:
            fig.text(.5, .5, 'SYNTHETIC', fontsize=44, color='#444444', alpha=.13, rotation=28, ha='center', va='center')
            fig.text(.99, .985, 'SYNTHETIC', ha='right', va='top', fontsize=8, color='#444444')
        fig.text(.5, .01, note, ha='center', va='bottom', fontsize=8)
        filename = 'rwc-' + ('SYNTHETIC-' if args.synthetic else '') + stem + '.pdf'
        fig.savefig(figdir / filename)
        evidence['figures'][filename] = dict(points=points, note=note, repetitions='separate; r0 solid/filled, r1 dashed/open, r2 dotted',
                                             synthetic_watermark=args.synthetic, figure_inches=list(fig.get_size_inches()))
        points = []; plt.close(fig)

    def axes_style(ax, title, xlabel='Value size (KiB)', ylabel='API latency (µs)', sizes=(1, 8, 64)):
        ax.set_title(title, pad=7); ax.set_xlabel(xlabel); ax.set_ylabel(ylabel)
        ax.set_xscale('log', base=2); ax.set_xticks(sizes, [str(s) for s in sizes]); ax.set_yscale('log')
        ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1, 2, 5)))
        ax.yaxis.set_minor_formatter(NullFormatter())
        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x:g}'))
        ax.grid(axis='y', alpha=.22)

    def curves(ax, family, predicate, xfield='size_bytes', metric='p50_us', tails=False):
        rows = [(i, r) for i, r in enumerate(data[family]) if predicate(r)]
        for a in sorted({arm(r) for _, r in rows}):
            for rep in sorted({r['repetition'] for _, r in rows}):
                selected = sorted([(i, r) for i, r in rows if arm(r) == a and r['repetition'] == rep], key=lambda ir: int(ir[1][xfield]))
                if not selected: continue
                xs = [int(r[xfield]) / (1024 if xfield == 'size_bytes' else 1) for _, r in selected]
                ys = [value(family, i, metric) for i, _ in selected]
                color = COLORS[a]; marker = MARKERS[a]
                ax.plot(xs, ys, color=color, marker=marker, linestyle=STYLES[int(rep)], linewidth=1.1,
                        markersize=4.8, markerfacecolor=color if rep == '0' else 'white', markeredgewidth=1)
                if tails:
                    ax.plot(xs, [value(family, i, 'p99_us') for i, _ in selected], color=color, marker='+',
                            linestyle='none', markersize=6)

    fig, axes = plt.subplots(1, 2, figsize=(7.0, 2.75)); fig.subplots_adjust(top=.78, bottom=.30, wspace=.33, left=.09, right=.98)
    for ax, c in zip(axes, (1, 16)):
        curves(ax, 'latency_vs_size', lambda r: r['group'] in ('G14N', 'G14O1') and concurrency(r) == c, tails=True)
        axes_style(ax, f'{c} writer' + ('s' if c > 1 else ''))
    finish(fig, 'write-latency', WRITE_CONTRACT + '\np50 lines; + p99; r0 solid/filled, r1 dashed/open. Page-cache arm is not a durable ranking.')

    fig, axes = plt.subplots(2, 2, figsize=(7.0, 4.5)); fig.subplots_adjust(top=.88, bottom=.21, hspace=.8, wspace=.34, left=.09, right=.98)
    for row, group in enumerate(('G15C', 'G15M')):
        for ax, c in zip(axes[row], (1, 16)):
            curves(ax, 'latency_vs_size', lambda r: r['group'] == group and concurrency(r) == c, tails=True)
            axes_style(ax, ('Qualified cold' if group == 'G15C' else 'Prewarmed cache') + f' · {c} reader' + ('s' if c > 1 else ''))
    finish(fig, 'read-latency', READ_CONTRACT + '\np50 lines; + p99; r0 solid/filled, r1 dashed/open. Unqualified cases have no latency marker.')

    # Six rate panels: each marker has exactly one configured concurrency.
    fig, axes = plt.subplots(3, 2, figsize=(7.0, 6.6)); fig.subplots_adjust(top=.82, bottom=.19, hspace=1.35, wspace=.35, left=.11, right=.98)
    for rr, (groups, title) in enumerate(((('G14N', 'G14O1'), 'Writes'), (('G15C',), 'Qualified cold reads'), (('G15M',), 'Prewarmed reads'))):
        for ax, size in zip(axes[rr], (1024, 65536)):
            field = 'writers' if rr == 0 else 'readers'
            curves(ax, 'throughput_vs_concurrency', lambda r: r['group'] in groups and int(r['size_bytes']) == size, xfield=field, metric='calls_per_second')
            axes_style(ax, f'{title} · {size//1024} KiB', xlabel='Writers' if rr == 0 else 'Readers', ylabel='Successful calls/s', sizes=(1, 5, 16))
            if rr:
                ax.axvspan(3.5, 7.5, color='#E69F00', alpha=.13, zorder=0)
                ax.set_xticks((1, 5, 16), ('1', '5*', '16'))
                ax.set_title(f'{title} · {size//1024} KiB\nc5*: one owner lane (lane-limited)', fontsize=8, pad=7)
            lo, hi = ax.get_ylim()
            ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1,) if hi/lo >= 100 else (1, 2, 5)))
            ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x/1e6:g}M' if x >= 1e6 else f'{x/1000:g}k' if x >= 1000 else f'{x:g}'))
    finish(fig, 'throughput', 'Writes: Native page cache is not durable; per-put/grouped barriers vs owner covering fdatasync\nReads: coordinator cache off; cold device-qualified, warm prewarmed; Native in-process (0 lanes)\nOwner c1/c5: 1 lane, c16: 4. Shaded c5* is lane-limited, not a transport ranking.\nFinite-case rates, not capacity; r0 filled/solid, r1 open/dashed; in-flight/offered counts in evidence.')

    fig, axes = plt.subplots(1, 3, figsize=(7.0, 3.1)); fig.subplots_adjust(top=.76, bottom=.32, wspace=.55, left=.09, right=.98)
    source = data['cold_vs_warm']
    for ax, a in zip(axes, ('native', 'tcp', 'verbs')):
        for rep in ('0', '1'):
            for size in ('1024', '8192', '65536'):
                rows = {r['group']: (i, r) for i, r in enumerate(source) if r['readers'] == '1' and arm(r) == a and r['size_bytes'] == size and r['repetition'] == rep}
                if not all(g in rows for g in ('G15C', 'G15M')): continue
                x = value('cold_vs_warm', rows['G15M'][0], 'mean_us'); y = value('cold_vs_warm', rows['G15C'][0], 'mean_us')
                ax.plot(x, y, marker={'1024': 'o', '8192': 'D', '65536': '^'}[size], color=COLORS[a],
                        markerfacecolor=COLORS[a] if rep == '0' else 'white', markersize=5)
        ax.set_title(LABELS[a] + ' · 1 reader')
        ax.margins(.3, .3)
        ax.xaxis.set_major_locator(MaxNLocator(3)); ax.yaxis.set_major_locator(MaxNLocator(4))
        ax.set_xlabel('Warm API mean (µs)'); ax.set_ylabel('Cold API mean (µs)'); ax.grid(alpha=.2)
    fig.legend(handles=[Line2D([], [], color='#111111', marker=m, linestyle='none', label=s)
                        for m, s in [('o', '1 KiB'), ('D', '8 KiB'), ('^', '64 KiB')]],
               loc='upper center', ncol=3, frameon=False)
    finish(fig, 'cold-warm', 'Coordinator cache off; cold device-qualified; warm serving-node page cache after prewarm\nEach point is one size and repetition: filled r0, open r1. Different cold/warm dataset geometry.\nLinear axes; no latency subtraction; no cross-reader comparisons.', legend=False)

    fig, axes = plt.subplots(2, 2, figsize=(7.0, 5.5)); fig.subplots_adjust(top=.9, bottom=.32, hspace=.78, wspace=.34, left=.1, right=.98)
    floor_source = data['floors']
    floor_styles = {}; palette = ['#111111', '#0072B2', '#D55E00', '#009E73', '#CC79A7', '#6A3D9A']
    for i, r in enumerate(floor_source):
        if not selected_floor(r): continue
        spec = floor_spec(r); panel = 0 if r['group'] == 'G13W' else 1 if r['group'] in ('G11S', 'G15F') else 2
        label = (r['arm'] + ' ' + spec['mode'] if panel == 0 else r['arm'] if panel == 1 else r['arm'])
        style_key = (panel, label)
        if style_key not in floor_styles: floor_styles[style_key] = palette[len([k for k in floor_styles if k[0] == panel])]
    for (panel, label), color in floor_styles.items():
        ax = axes.flat[panel]
        for rep in ('0', '1', '2'):
            chosen = []
            for i, r in enumerate(floor_source):
                if not selected_floor(r) or r['repetition'] != rep: continue
                spec = floor_spec(r); pp = 0 if r['group'] == 'G13W' else 1 if r['group'] in ('G11S', 'G15F') else 2
                ll = r['arm'] + ' ' + spec['mode'] if pp == 0 else r['arm']
                if (pp, ll) == (panel, label): chosen.append((i, r))
            chosen.sort(key=lambda ir: int(ir[1]['size_bytes']))
            if chosen:
                ax.plot([int(r['size_bytes'])/1024 for _, r in chosen], [value('floors', i, 'mean_us', floor_factor(r)) for i, r in chosen],
                        label=label if rep == '0' else None, color=color, marker='o', linestyle=STYLES[int(rep)],
                        markersize=4, markerfacecolor=color if rep == '0' else 'white')
        ax.legend(fontsize=8, frameon=False, loc='upper left')
    ax = axes.flat[3]
    for group, mark, linestyle in [('G14O1', 's', '-'), ('G15C', '^', '--'), ('G15M', 'o', ':')]:
        for transport in ('tcp', 'verbs'):
            for rep in ('0', '1'):
                chosen = [(i, r) for i, r in enumerate(data['latency_vs_size']) if r['group'] == group and r['transport'] == transport and concurrency(r) == 1 and r['repetition'] == rep]
                chosen.sort(key=lambda ir: int(ir[1]['size_bytes']))
                ax.plot([int(r['size_bytes'])/1024 for _, r in chosen], [value('latency_vs_size', i, 'mean_us') for i, r in chosen],
                        color=COLORS[transport], marker=mark, linestyle=linestyle, markersize=4,
                        markerfacecolor=COLORS[transport] if rep == '0' else 'white',
                        label=transport + ' ' + {'G14O1':'write', 'G15C':'cold read', 'G15M':'warm read'}[group] if rep == '0' else None)
    ax.legend(fontsize=8, frameon=False, ncol=2, loc='upper center', bbox_to_anchor=(.5, -.48), borderaxespad=0)
    for ax, title, sizes in zip(axes.flat, ('QD1 append + sync (both hosts)', 'QD1 direct random read (both hosts)', 'Standalone network round trips', 'Owner whole API means · c1'), ((1,8,64),(4,64),(.0078125,1,4,64),(1,8,64))):
        axes_style(ax, title, ylabel='Mean latency (µs)', sizes=sizes)
        ax.margins(y=.15)
        lower, upper = ax.get_ylim()
        ax.set_ylim(lower, upper * 5)
    axes.flat[2].set_xticks((8/1024, 1, 4, 64), ('8 B', '1 KiB', '4 KiB', '64 KiB'))
    axes.flat[2].set_xlabel('Transfer size')
    failed_event = {r['case_id'] for r in floor_source if r['group']=='G11N' and r['status']=='failed'
                    and floor_spec(r)['tool']=='ib_write_lat' and floor_spec(r)['mode']=='event'}
    if failed_event:
        axes.flat[2].text(.02, .48, 'ib_write_lat event: failed, r0–r2', transform=axes.flat[2].transAxes, fontsize=8,
                          bbox=dict(facecolor='white', edgecolor='none', alpha=.9))
    finish(fig, 'floors', '[INFERENCE] Separate populations: no stack or subtraction. Failed controls stay unavailable.\nAppend: pwrite/fdatasync or mmap/fsync; direct reads: fresh 2 GiB; network: polled send / blocking TCP\nAPI c1: covering-sync writes; cold qualified / warm prewarmed; coordinator cache off; 1 owner read lane\nFilled r0, open r1/r2; network ticks specify units; other x axes in KiB. No 8 KiB network or 1/8 KiB SSD read floor.', legend=False)

    groups = [('G14O1', 'Write'), ('G15C', 'Cold read'), ('G15M', 'Warm read')]
    counts = []; labels = []
    for group, title in groups:
        for rep in ('0', '1'):
            counts.append(availability_counts(data['parity_matrix'], group, rep)); labels.append(title + ' / r' + rep)
    fig, ax = plt.subplots(figsize=(7.0, 3.4)); fig.subplots_adjust(top=.87, bottom=.33, left=.18, right=.98)
    array = np.array(counts); ax.imshow(np.where(array > 0, np.arange(5)[None,:]+1, 0), aspect='auto', vmin=0, vmax=5,
                                         cmap=ListedColormap(['#ffffff', '#d8eef8', '#d5d5d5', '#f4c4ae', '#f3e6ac', '#e1d4ee']))
    for rr in range(6):
        for cc in range(5): ax.text(cc, rr, str(counts[rr][cc]), ha='center', va='center', fontsize=10)
    ax.set_xticks(range(5), ['Matched\npairs', 'Missing\npairs', 'Failed\ncases', 'Unqualified\ncold cases', 'Other\ncases']); ax.set_yticks(range(6), labels)
    ax.set_title('Pair availability · all sizes · matched c1/c5/c16 only')
    finish(fig, 'parity', 'First two columns count pairs; last three count distinct cases (including Native)\nUpstream job-level parity gate; cold device-qualified; coordinator cache off\nWrite controls: page cache (not durable), per-put/grouped barriers; owner covering sync.\nMissing = absent Native partner; Other = nonterminal/not available. Columns are not a partition.', legend=False)
    evidence['figures']['rwc-' + ('SYNTHETIC-' if args.synthetic else '') + 'parity.pdf']['availability_counts'] = counts
    dest = brief / 'rwc/generated'; dest.mkdir(parents=True, exist_ok=True)
    (dest / 'figure-evidence.json').write_text(json.dumps(evidence, indent=2) + '\n')
    manifest_path = dest / 'inputs-rwc.json'
    if manifest_path.is_file():
        manifest = json.loads(manifest_path.read_text())
        assert manifest['synthetic'] == args.synthetic, 'table/figure synthetic state differs'
        for name in evidence['figures']:
            path = figdir / name
            manifest['outputs'][str(path.relative_to(brief))] = hashlib.sha256(path.read_bytes()).hexdigest()
        manifest['outputs']['rwc/generated/figure-evidence.json'] = hashlib.sha256((dest / 'figure-evidence.json').read_bytes()).hexdigest()
        manifest_path.write_text(json.dumps(manifest, indent=2) + '\n')
        registration = brief / 'inputs.json'
        old = json.loads(registration.read_text())
        if 'rwc' in old:
            old['outputs'].update(manifest['outputs'])
            old['outputs']['rwc/generated/inputs-rwc.json'] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
            registration.write_text(json.dumps(old, indent=2) + '\n')
    print('Wrote six figures and rwc/generated/figure-evidence.json')


if __name__ == '__main__': main()
