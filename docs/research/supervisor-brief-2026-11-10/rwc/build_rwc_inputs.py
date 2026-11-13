#!/usr/bin/env python3
"""Build source-traced run-9 tables, findings and provenance; never pool cases."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import shutil

from rwc_data import FAMILIES, LABELS, UNMATCHED, arm, availability_counts, concurrency, floor_factor, floor_spec, load, selected_floor


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tex(text):
    return ''.join({'&': r'\&', '%': r'\%', '_': r'\_', '#': r'\#', '$': r'\$',
                    '{': r'\{', '}': r'\}', '\\': r'\textbackslash{}'}.get(c, c) for c in str(text))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--provenance', type=Path, required=True)
    parser.add_argument('--export', type=Path)
    parser.add_argument('--brief', type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument('--register', action='store_true')
    args = parser.parse_args()
    brief = args.brief.resolve(); generated = brief / 'rwc/generated'; generated.mkdir(parents=True, exist_ok=True)
    provenance = json.loads(args.provenance.read_text()); synthetic = provenance.get('synthetic', False)
    for field in ('run_id', 'package_sha256', 'source_revision', 'source_dirty', 'launch_time', 'vm_names'):
        if field not in provenance or provenance[field] in ('', None, []): raise ValueError('missing provenance: ' + field)
    if not isinstance(provenance['source_dirty'], bool) or len(provenance['vm_names']) != 2:
        raise ValueError('source_dirty must be bool; list coordinator and owner VM names')
    if not synthetic and ('SYNTHETIC' in str(args.output) or any('SYNTHETIC' in str(v) for v in provenance.values())):
        raise ValueError('refusing synthetic input labelled real')
    export = args.export or args.provenance.parent / 'export'
    data_dir = brief / ('rwc/data/SYNTHETIC-output' if synthetic else 'rwc/data/run9-output')
    data_dir.mkdir(parents=True, exist_ok=True)
    sources = {}
    for name in (*[f + '.csv' for f in FAMILIES], 'remote_rw.md'):
        src = args.output / name; dst = data_dir / name
        if src.resolve() != dst.resolve(): shutil.copyfile(src, dst)
        sources[str(src.resolve())] = digest(src)
    for path in [args.provenance, export / 'matrix.json', export / 'comparison.json',
                 *[Path(p) for p in provenance.get('receipt_sources', [])]]:
        sources[str(path.resolve())] = digest(path)
    matrix = json.loads((export / 'matrix.json').read_text())
    report = json.loads((export / 'comparison.json').read_text())
    planned = {j['case_id']: j for j in matrix['jobs']}
    if len(report['cases']) != len(planned) or any(c['job'] != planned[c['case_id']] for c in report['cases']):
        raise ValueError('report/matrix mismatch')
    prov_path = generated / 'provenance.json'; prov_path.write_text(json.dumps(provenance, indent=2) + '\n')
    data = load(data_dir); numbers = {}; number_sources = {}; generated_files = []

    def csv_file(family, entries, fields):
        data[family] = entries
        with (data_dir / (family + '.csv')).open('w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fields); writer.writeheader(); writer.writerows(entries)

    csv_file('campaign_cases', [dict(case_id=c['case_id'], group=c['job']['group'], status=c['status'],
                                   error=c.get('error') or '') for c in report['cases']], ('case_id', 'group', 'status', 'error'))
    qualification = []; text = (data_dir / 'remote_rw.md').read_text()
    block = text.split('## Cold device qualification\n', 1)[1].split('\n## ', 1)[0]
    cold_by_id = {r['case_id']: r for r in data['parity_matrix'] if r['group'] == 'G15C'}
    for line in block.splitlines():
        columns = [s.strip() for s in line.strip().strip('|').split('|')]
        if len(columns) != 9 or columns[0] not in cold_by_id: continue
        row = cold_by_id[columns[0]]
        if columns[6] != row['status']: raise ValueError('qualification status differs from CSV')
        qualification.append(dict(case_id=columns[0], status=columns[6],
                                  device_reads_per_call='' if columns[7] == '—' else columns[7], reasons=columns[8]))
    if {r['case_id'] for r in qualification} != cold_by_id.keys(): raise ValueError('missing qualification rows')
    csv_file('cold_qualification', qualification, ('case_id', 'status', 'device_reads_per_call', 'reasons'))

    def number(family, index, field, fmt='.1f', factor=1):
        value = data[family][index][field]
        if value == '': return 'n/c'
        key = f'RWC:{family}:{index}:{field}:{factor}|{fmt}'
        numbers[key] = format(float(value) * factor, fmt)
        number_sources[key] = dict(family=family, row=index, field=field, factor=factor, format=fmt)
        return r'\N{' + key + '}'

    def count(label, family, filters, statuses=None):
        chosen = [r for r in data[family] if all(r[k] in v if isinstance(v, list) else r[k] == v for k, v in filters.items())]
        value = sum(r['status'] in statuses for r in chosen) if statuses is not None else len(chosen)
        key = 'RWC:count:' + label; numbers[key] = str(value)
        number_sources[key] = dict(family=family, filters=filters, statuses=statuses, operation='count')
        return r'\N{' + key + '}'

    def case_index(group, label, size, c, rep):
        found = [i for i,r in enumerate(data['latency_vs_size']) if r['group'] == group and arm(r) == label
                 and r['size_bytes'] == str(size) and concurrency(r) == c and r['repetition'] == str(rep)]
        if len(found) != 1: raise ValueError(('nonunique case', group, label, size, c, rep))
        return found[0]

    def means(group, label, size=1024, c=1, field='mean_us', fmt='.1f'):
        values = []
        for rep in (0,1):
            i = case_index(group,label,size,c,rep); r = data['latency_vs_size'][i]
            values.append(number('latency_vs_size',i,field,fmt) if r['status'] == 'completed' else tex(r['status']))
        return '/'.join(values)

    def device_ratios(label, size, c=1):
        values = []
        for rep in (0,1):
            case = data['latency_vs_size'][case_index('G15C',label,size,c,rep)]
            qi = next(i for i,q in enumerate(qualification) if q['case_id'] == case['case_id'])
            values.append(number('cold_qualification',qi,'device_reads_per_call',fmt='.2f'))
        return '/'.join(values)

    def ratios(group, label, field):
        values = []
        for rep in (0,1):
            numerator = case_index(group,label,1024,5,rep); denominator = case_index(group,label,1024,1,rep)
            key = f'RWC:ratio:{group}:{label}:{rep}:{field}'
            numbers[key] = format(float(data['latency_vs_size'][numerator][field]) / float(data['latency_vs_size'][denominator][field]), '.2f')
            number_sources[key] = dict(operation='ratio', family='latency_vs_size', numerator=numerator,
                                       denominator=denominator, field=field, format='.2f')
            values.append(r'\N{' + key + '}')
        return '/'.join(values)

    def write(name, text):
        path = generated / name; path.write_text(text + '\n'); generated_files.append(path)

    def table(name, headers, rows, caption):
        if name in ('tab-rwc-write.tex', 'tab-rwc-parity.tex'):
            caption = 'Separate controls: Class A Native page cache; Class B Native barriers and owner sync. No cross-class ranking. ' + caption
        elif name in ('tab-rwc-read-cold.tex', 'tab-rwc-read-warm.tex', 'tab-rwc-qualification.tex'):
            caption = 'Read preparation: Class B write settings, not a read durability guarantee. ' + caption
        banner = r'\textbf{SYNTHETIC --- layout test, not measurements.} ' if synthetic else ''
        text = (r'\begin{table}[H]\centering\small\setlength{\tabcolsep}{3pt}' + '\n' +
                r'\begin{tabular}{' + 'l' + 'r' * (len(headers)-1) + r'}\toprule' + '\n' +
                ' & '.join(headers) + r'\\\midrule' + '\n' +
                '\n'.join(' & '.join(row) + r'\\' for row in rows) + '\n' +
                r'\bottomrule\end{tabular}' + '\n' + r'\caption{' + banner + caption + ' ' + UNMATCHED + '}\n' + r'\end{table}')
        write(name, text)

    # Every repetition is a real row, so labels and statistics share a baseline.
    for title, groups, name in [('Writes', ('G14N','G14O1'),'write'), ('Cold reads',('G15C',),'read-cold'), ('Warm reads',('G15M',),'read-warm')]:
        source = data['latency_vs_size']; rows = []
        shapes = sorted({(r['size_bytes'],arm(r)) for r in source if r['group'] in groups}, key=lambda s:(int(s[0]),s[1]))
        for size,label in shapes:
            for rep in (0,1):
                cells = []
                for c in (1,5,16):
                    found = [(i,r) for i,r in enumerate(source) if r['group'] in groups and
                             (r['size_bytes'],concurrency(r),arm(r),r['repetition']) == (size,c,label,str(rep))]
                    if len(found) != 1: raise ValueError('missing or duplicate compact case')
                    i,row = found[0]
                    if row['status'] != 'completed': cells.append(tex(row['status'])); continue
                    values = []
                    for field in ('p50_us','p99_us'):
                        v = float(row[field]); fmt = '.2f' if v < 10 else '.1f' if v < 100 else '.0f'
                        values.append(number('latency_vs_size',i,field,fmt))
                    cells.append('/'.join(values))
                rows.append([f'{int(size)//1024} KiB', LABELS[label], 'r'+str(rep), *cells])
        caption = title + r'; successful API p50/p99 (\us{}), repetitions separate; c1/c5/c16. '
        if name != 'write':
            caption += 'Coordinator cache off. Unqualified cold cells have no latency.'
        table('tab-rwc-'+name+'.tex', ['Size','Arm','Rep.','c1','c5*' if name != 'write' else 'c5','c16'], rows, caption)

    qrows = []
    for size in ('1024','8192','65536'):
        for c in ('1','5','16'):
            for label in ('native','tcp','verbs'):
                cells = []
                for rep in ('0','1'):
                    row = next(r for r in data['parity_matrix'] if r['group']=='G15C' and
                               (r['size_bytes'],r['readers'],arm(r),r['repetition']) == (size,c,label,rep))
                    qi = next(i for i,q in enumerate(qualification) if q['case_id']==row['case_id'])
                    cells += [tex(row['status']),number('cold_qualification',qi,'device_reads_per_call',fmt='.2f')]
                qrows.append([f'{int(size)//1024} KiB / c{c}',LABELS[label],*cells])
    table('tab-rwc-cold-qualification.tex',['Size / readers','Arm','r0 verdict','r0 reads/call','r1 verdict','r1 reads/call'],qrows,
          'Cold device qualification per case. Completed requires a qualified receipt and device reads per success at least 0.9; '
          r'this is not per-call NAND proof. Ratios/reasons are indexed from remote\_rw.md into cold\_qualification.csv. '
          'Owner c5 uses one lane and is lane-limited; no transport ranking follows.')

    parity_rows = []
    for group,label in [('G14O1','Write'),('G15C','Cold read'),('G15M','Warm read')]:
        for rep in ('0','1'):
            cells = []
            for status,value in zip(('matched','missing partner','failed','unqualified cold','not available'),availability_counts(data['parity_matrix'],group,rep)):
                key=f'RWC:availability:{group}:{rep}:{status}'; numbers[key]=str(value)
                number_sources[key]=dict(operation='availability',family='parity_matrix',group=group,repetition=rep,status=status)
                cells.append(r'\N{'+key+'}')
            parity_rows.append([label+' / r'+rep,*cells])
    table('tab-rwc-parity.tex',['Grid / rep.','Matched pairs','Missing pairs','Failed cases','Unqual. cases','Other cases'],parity_rows,
          'First two columns count pairs; last three distinct main-grid cases including Native. An owner write has three Native controls, '
          'an owner read one. Columns are not a partition. The three failed network-floor cases are separate, not failed KV calls. '
          'All sizes and c1/c5/c16 included; pairing does not equalize read lanes or durability.')

    floor_rows=[]; grouped={}
    for i,row in enumerate(data['floors']):
        if not selected_floor(row): continue
        spec=floor_spec(row)
        label=(row['arm']+' '+spec['mode'] if row['group']=='G13W' else row['arm']+' direct fresh 2 GiB' if row['group'] in ('G11S','G15F') else row['arm']+' '+str(spec.get('mode')))
        grouped.setdefault((row['group'],label,row['size_bytes']),[]).append((i,row))
    for (group,label,size),entries in sorted(grouped.items(),key=lambda x:(x[0][0],x[0][1],int(x[0][2]))):
        cells=[]
        for rep in ('0','1','2'):
            found=[(i,r) for i,r in entries if r['repetition']==rep]
            if not found: cells.append('--'); continue
            i,row=found[0]
            cells.append(number('floors',i,'mean_us',factor=floor_factor(row)) if row['status']=='completed' else tex(row['status']))
        floor_rows.append([tex(label),f'{int(size)} B',*cells])
    table('tab-rwc-floors.tex',['Standalone control','Transfer','r0 mean','r1 mean','r2 mean'],floor_rows,
          r'Independent floors (\us), not additive stages. QD1 append-pwrite/fdatasync and mmap/fsync on both hosts; '
          r'direct random reads: fresh 2 GiB. Network: blocking eth0 TCP RTT and polled ib\_send\_lat (half RTT doubled). '
          'Append controls have r0/r1; network/read floors also r2. No 8 KiB network or 1/8 KiB SSD-read floor; no interpolation. '
          'Other modes/regions remain in floors.csv. Failed event-write controls are listed below.')
    failed_floors={r['case_id']:r for r in data['floors'] if r['status']=='failed'}
    failures=[r'\small\textbf{Failed network controls (not failed KV calls).}']
    errors={r['error'] for r in failed_floors.values()}
    if len(errors)==1:
        failures.append(tex(next(iter(errors)))+'.')
    for case_id,row in sorted(failed_floors.items(),key=lambda x:x[1]['repetition']):
        spec=floor_spec(row)
        detail='' if len(errors)==1 else ' '+tex(row['error'])+'.'
        failures.append(r'\par r'+row['repetition']+r': \texttt{'+tex(case_id)+'}; '+tex(spec['tool'])+' '+tex(spec['mode'])+'.'+detail)
    write('rwc-failures.tex','\n'.join(failures)+r'\normalsize')

    findings={}
    findings['parity']=(count('campaign-completed','campaign_cases',{},['completed'])+' of '+count('planned','campaign_cases',{})+
                        ' controller-completed; '+count('campaign-failed','campaign_cases',{},['failed'])+' failed floor controls; '+
                        count('unqualified-cold','parity_matrix',{'group':'G15C'},['unqualified'])+' Native cold cases unqualified.')
    findings['results']=(r'1 KiB c1 means (r0/r1, \us{}): writes --- Native per-put '+means('G14N','native value_wal_barrier')+
                         ', grouped '+means('G14N','native value_wal_group_barrier')+'; owner TCP '+means('G14O1','tcp')+
                         ', verbs '+means('G14O1','verbs')+'. Warm reads --- Native '+means('G15M','native',fmt='.2f')+
                         ', owner TCP '+means('G15M','tcp')+', verbs '+means('G15M','verbs')+
                         '; cold --- owner TCP '+means('G15C','tcp')+', verbs '+means('G15C','verbs')+
                         ' (both device-qualified), Native 1 KiB unqualified.')
    findings['writes']=(r'At 64 KiB c16, Native per-put barrier means are '+means('G14N','native value_wal_barrier',65536,16)+
                        r'~\us{}, grouped barrier '+means('G14N','native value_wal_group_barrier',65536,16)+
                        r'~\us{}, owner verbs covering sync '+means('G14O1','verbs',65536,16)+
                        r'~\us{} (r0/r1). These are distinct acknowledgement mechanics, not a general ranking. Native page cache is not durable.')
    findings['cold']=(r'At 1 KiB c1, TCP means are '+means('G15C','tcp')+r'~\us{} and verbs '+means('G15C','verbs')+
                      r'~\us{} (r0/r1); Native is unqualified. Native 8 KiB c1 p50 is '+means('G15C','native',8192,1,'p50_us','.2f')+
                      r'~\us{} despite passing the case-level device gate: qualification is not proof of a unimodal or every-call SSD population.')
    findings['cold-populations']=(r'Native 8 KiB c1 passes the device-count gate but has memory-like p50 '+means('G15C','native',8192,1,'p50_us','.2f')+
                                  r'~\us{}, whereas Native 64 KiB c1 records '+device_ratios('native',65536)+
                                  r' device reads per successful call (r0/r1; request counts, not latency), so the gate-qualified 8/64 KiB cells are not equivalent device-latency populations (Appendix~\ref{app:rwc-qualification}).')
    findings['warm']=(r'At 1 KiB c1, prewarmed Native means are '+means('G15M','native',fmt='.2f')+r'~\us{}, owner TCP '+means('G15M','tcp')+
                      r'~\us{} and owner verbs '+means('G15M','verbs')+r'~\us{} (r0/r1). The owner still traverses RPC and completion; this is not an isolated wire-time measurement.')
    findings['lanes']=(r'At 1 KiB, c5/c1 cold mean ratios are '+ratios('G15C','tcp','mean_us')+' for TCP and '+ratios('G15C','verbs','mean_us')+
                       '; corresponding rate ratios are '+ratios('G15C','tcp','calls_per_second')+' and '+ratios('G15C','verbs','calls_per_second')+
                       r' (r0/r1). Warm mean ratios are '+ratios('G15M','tcp','mean_us')+' and '+ratios('G15M','verbs','mean_us')+
                       '; rate ratios '+ratios('G15M','tcp','calls_per_second')+' and '+ratios('G15M','verbs','calls_per_second')+
                       '. One owner lane limits c5; this is not a transport limit or a basis to rank transports. At c16 the owners use four lanes. '
                       r'The 64 KiB c5 cold verbs means differ across r0/r1: '+means('G15C','verbs',65536,5)+r'~\us{}; both remain visible.')
    findings['qualification']=(count('owner-cold-qualified','parity_matrix',{'group':'G15C','transport':['tcp','verbs']},['completed'])+
                              ' owner cold cells qualify. Native qualifies at 8/64 KiB, but all six 1 KiB cells (c1/c5/c16, r0/r1) are unqualified. '
                              'Native 1 KiB device reads/call, r0/r1: ')
    qualification_parts=[]
    for c in ('1','5','16'):
        values=[]
        for rep in ('0','1'):
            case=next(r for r in data['parity_matrix'] if r['group']=='G15C' and r['arm']=='native' and r['size_bytes']=='1024' and r['readers']==c and r['repetition']==rep)
            qi=next(i for i,q in enumerate(qualification) if q['case_id']==case['case_id'])
            values.append(number('cold_qualification',qi,'device_reads_per_call',fmt='.2f'))
        qualification_parts.append('c'+c+' '+ '/'.join(values))
    findings['qualification'] += '; '.join(qualification_parts)+'. The threshold is 0.9, with a qualified receipt also required.'
    findings['append'] = count('append-completed','campaign_cases',{'group':'G13W'},['completed'])+' of '+count('append-planned','campaign_cases',{'group':'G13W'})+' G13W controls completed on the coordinator/client and owner SSDs.'
    lines=[r'\newcommand{\RWCFINDING}[1]{\ifcsname rwcfinding@#1\endcsname\csname rwcfinding@#1\endcsname\else\errmessage{Missing run-9 finding #1}\fi}']
    for label,value in findings.items():
        lines.append(r'\expandafter\def\csname rwcfinding@'+label+r'\endcsname{'+('SYNTHETIC fixture: ' if synthetic else '')+value+'}')
    write('rwc-findings.tex','\n'.join(lines))
    write('rwc-numbers.tex','% Generated from source-traced CSV inputs.\n'+'\n'.join(
        r'\expandafter\gdef\csname n@\detokenize{'+key+r'}\endcsname{'+value+'}' for key,value in sorted(numbers.items())))
    if synthetic:
        provenance_tex=r'\textbf{SYNTHETIC fixture; not a cloud run.}'
    else:
        cost=provenance['cost_model']; estimated=cost['hours_estimate']*cost['hosts']*cost['list_price_usd_per_host_hour']
        provenance_tex=(r'\small\textbf{Run-9 launched-package provenance.} Parent \texttt{'+tex(provenance['parent_run_id'])+
                        r'}; fleet \texttt{'+tex(provenance['run_id'])+r'}. VMs: \texttt{'+tex(provenance['vm_names'][0])+r'} (coordinator), \texttt{'+tex(provenance['vm_names'][1])+r'} (owner); '+
                        tex(provenance['machine_type'])+', '+tex(provenance['zone'])+', compact placement. Created '+tex(provenance['launch_time'])+'--'+tex(provenance['creation_completed'])+'; deleted '+tex(provenance['deletion_time'])+
                        '. Cleanup verified: no VM, disk or placement policy left.\n\n'+
                        r'Source revision \path{'+provenance['source_revision']+r'}, dirty: true. SHA-256: package \path{'+provenance['package_sha256']+r'}; binary \path{'+provenance['binary_sha256']+r'}; manifest \path{'+provenance['manifest_sha256']+r'}; source tree \path{'+provenance['source_tree_sha256']+r'}; matrix \path{'+provenance['matrix_sha256']+'}.\n\n'+
                        r'\textbf{[INFERENCE]} Compute estimate: '+str(cost['hours_estimate'])+' h $\\times$ '+str(cost['hosts'])+' hosts $\\times$ \\$'+str(cost['list_price_usd_per_host_hour'])+r'/host-hour $\simeq$ \$'+format(estimated,'.1f')+'; list price, not an invoice.\n\n'+
                        r'Excluded attempt: fleet \texttt{bench-f369f00616c841e4}, parent \texttt{bench-7513b3e4178e470f}; about seven minutes, eight of nine initial append controls before a controller descriptor-inspection race aborted it. Bounded consistent inspection fixed the race before attempt two; the 46-case canary head was reordered, but the 342 jobs are identical. No first-attempt results are used. Its approximately \$0.8 cost is an estimate, not an invoice.\normalsize')
    write('rwc-provenance.tex',provenance_tex)
    figure_directory = 'rwc/synthetic-figures/' if synthetic else 'figures/'
    prefix='rwc-SYNTHETIC-' if synthetic else 'rwc-'
    write('rwc-figure-hooks.tex',r'\newcommand{\RWCfigure}[2][\linewidth]{\includegraphics[width=#1]{'+figure_directory+prefix+'#2.pdf}}\n'+
          r'\newcommand{\RWCstate}{'+('SYNTHETIC: invented test values, not measurements.' if synthetic else 'Run 9: one compact two-VM fleet; r0/r1 separate.')+'}')
    outputs={str(p.relative_to(brief)):digest(p) for p in generated_files}
    outputs.update({str(p.relative_to(brief)):digest(p) for p in data_dir.iterdir() if p.is_file()})
    outputs[str(prov_path.relative_to(brief))]=digest(prov_path)
    manifest=dict(synthetic=synthetic,data_directory=str(data_dir.relative_to(brief)),sources=sources,outputs=outputs,
                  number_sources=number_sources,unmatched_dimensions=UNMATCHED,provenance=str(prov_path.relative_to(brief)),
                  report_path=str((export/'comparison.json').resolve()),matrix_path=str((export/'matrix.json').resolve()),
                  report_complete=report.get('complete'),table_selection='all sizes, c1/c5/c16; r0/r1 separate; declared floor controls')
    (generated/'inputs-rwc.json').write_text(json.dumps(manifest,indent=2)+'\n')
    if args.register:
        path=brief/'inputs.json'; old=json.loads(path.read_text())
        # Keep fixtures on disk, never in the final input registry.
        old['sources']={k:v for k,v in old['sources'].items() if 'SYNTHETIC' not in k}
        old['outputs']={k:v for k,v in old['outputs'].items() if not k.startswith('rwc/') and 'rwc-SYNTHETIC-' not in k}
        old['sources'].update(sources); old['outputs'].update(outputs)
        old['outputs']['rwc/generated/inputs-rwc.json']=digest(generated/'inputs-rwc.json')
        old['rwc']=dict(manifest='rwc/generated/inputs-rwc.json',synthetic=synthetic)
        path.write_text(json.dumps(old,indent=2)+'\n')
    print(f'{len(numbers)} traced numbers; {len(generated_files)} generated TeX files; {generated}')


if __name__ == '__main__': main()
