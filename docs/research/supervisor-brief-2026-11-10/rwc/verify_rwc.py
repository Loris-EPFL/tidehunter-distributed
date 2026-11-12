#!/usr/bin/env python3
"""Independent run-9 CSV, table, figure and final-PDF acceptance; no generator imports."""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import re
import subprocess


def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()


def row_arm(row): return row['arm'] if row['transport'] == 'in-process' else row['transport']


def availability_count(rows, group, rep, status):
    if status in ('failed','unqualified cold','not available'):
        groups=('G14N','G14O1') if group=='G14O1' else (group,)
        cases=[r for r in rows if r['group'] in groups and r['repetition']==rep]
        if status=='not available': return sum(r['status'] not in ('completed','failed','unqualified') for r in cases)
        return sum(r['status']==('unqualified' if status=='unqualified cold' else 'failed') for r in cases)
    total=0
    for owner in rows:
        if owner['group']!=group or owner['repetition']!=rep or owner['transport'] not in ('tcp','verbs'): continue
        ng='G14N' if group=='G14O1' else group
        modes=('native page_cache','native value_wal_barrier','native value_wal_group_barrier') if ng=='G14N' else ('native',)
        for mode in modes:
            found=[n for n in rows if n['group']==ng and n['arm']==mode and n['transport']=='in-process'
                   and n['size_bytes']==owner['size_bytes'] and n['repetition']==rep
                   and (n['readers'] or n['writers'])==(owner['readers'] or owner['writers'])]
            assert len(found)<=1
            if status=='missing partner': total+=not found
            elif found: total+=owner['status']==found[0]['status']=='completed'
    return total


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--brief',type=Path,default=Path(__file__).resolve().parent.parent)
    p.add_argument('--pdf',action='store_true')
    args=p.parse_args(); brief=args.brief.resolve(); generated=brief/'rwc/generated'
    manifest=json.loads((generated/'inputs-rwc.json').read_text()); data_dir=brief/manifest['data_directory']
    data={}
    for path in data_dir.glob('*.csv'):
        with path.open(newline='') as f: data[path.stem]=list(csv.DictReader(f))
    assert set(data)=={'latency_vs_size','latency_vs_concurrency','throughput_vs_concurrency','cold_vs_warm',
                       'floors','parity_matrix','cold_qualification','campaign_cases'}
    for name,wanted in manifest['outputs'].items(): assert sha(brief/name)==wanted,name
    for name,wanted in manifest['sources'].items(): assert sha(Path(name))==wanted,name
    registry=json.loads((brief/'inputs.json').read_text())
    if 'rwc' in registry:
        assert registry['rwc']['synthetic']==manifest['synthetic']
        assert registry['outputs']['rwc/generated/inputs-rwc.json']==sha(generated/'inputs-rwc.json')
        for name,wanted in manifest['outputs'].items(): assert registry['outputs'][name]==wanted,name
        for name,wanted in manifest['sources'].items(): assert registry['sources'][name]==wanted,name
    if not manifest['synthetic']:
        assert not any('SYNTHETIC' in k for group in ('sources','outputs') for k in registry[group])
        assert not list((brief/'figures').glob('*SYNTHETIC*'))
    report=json.loads(Path(manifest['report_path']).read_text()); matrix=json.loads(Path(manifest['matrix_path']).read_text())
    expected_cases=[dict(case_id=c['case_id'],group=c['job']['group'],status=c['status'],error=c.get('error') or '') for c in report['cases']]
    assert data['campaign_cases']==expected_cases
    assert len({r['case_id'] for r in expected_cases})==len(matrix['jobs'])
    assert {r['case_id'] for r in expected_cases}=={r['case_id'] for r in matrix['jobs']}
    assert manifest['report_complete']==report.get('complete')
    for family in ('latency_vs_concurrency','throughput_vs_concurrency','parity_matrix'):
        assert data[family]==data['latency_vs_size']
    assert data['cold_vs_warm']==[r for r in data['latency_vs_size'] if r['group'] in ('G15C','G15M')]
    if not manifest['synthetic']:
        assert len(expected_cases)==342
        assert sum(r['status']=='completed' for r in expected_cases)==339
        assert sum(r['status']=='failed' for r in expected_cases)==3
        for r in data['cold_vs_warm']:
            expected=0 if r['transport']=='in-process' else 4 if r['readers']=='16' else 1
            assert int(r['read_lanes'])==expected
    markdown=(data_dir/'remote_rw.md').read_text()
    block=markdown[markdown.index('## Cold device qualification\n'):].split('\n## ',1)[0]
    cold_ids={r['case_id'] for r in data['parity_matrix'] if r['group']=='G15C'}
    assert {r['case_id'] for r in data['cold_qualification']}==cold_ids
    for row in data['cold_qualification']:
        lines=[s for s in block.splitlines() if s.startswith('| '+row['case_id']+' |')]; assert len(lines)==1
        columns=[c.strip() for c in lines[0].strip('|').split('|')]
        assert row['status']==columns[6] and row['reasons']==columns[8]
        assert row['device_reads_per_call']==('' if columns[7]=='—' else columns[7])
        if row['status']=='completed': assert float(row['device_reads_per_call'])>=.9
    if not manifest['synthetic']:
        uq=[r for r in data['parity_matrix'] if r['group']=='G15C' and r['status']=='unqualified']
        assert len(uq)==6 and all(r['arm']=='native' and r['size_bytes']=='1024' for r in uq)
        assert {(r['readers'],r['repetition']) for r in uq}=={(str(c),str(rep)) for c in (1,5,16) for rep in (0,1)}
        assert all(not r['mean_us'] and not r['p50_us'] for r in uq)
        assert all(r['status']=='completed' for r in data['parity_matrix'] if r['group']=='G15C' and r['transport'] in ('tcp','verbs'))
    definitions=dict(re.findall(r'\\detokenize\{([^{}]+)\}\\endcsname\{([^{}]*)\}',(generated/'rwc-numbers.tex').read_text()))
    assert definitions.keys()==manifest['number_sources'].keys()
    for key,source in manifest['number_sources'].items():
        rows=data[source['family']]; operation=source.get('operation')
        if operation=='count':
            chosen=[r for r in rows if all(r[k] in v if isinstance(v,list) else r[k]==v for k,v in source['filters'].items())]
            expected=str(len(chosen) if source['statuses'] is None else sum(r['status'] in source['statuses'] for r in chosen))
        elif operation=='availability':
            assert key==f"RWC:availability:{source['group']}:{source['repetition']}:{source['status']}"
            expected=str(availability_count(rows,source['group'],source['repetition'],source['status']))
        elif operation=='ratio':
            a,b=rows[source['numerator']],rows[source['denominator']]
            assert a['readers']=='5' and b['readers']=='1'
            assert all(a[f]==b[f] for f in ('group','size_bytes','repetition','transport','arm'))
            assert a['status']==b['status']=='completed'
            expected=format(float(a[source['field']])/float(b[source['field']]),source['format'])
        else:
            i=source['row']; field=source['field']; factor=source['factor']; fmt=source['format']; row=rows[i]
            assert key==f"RWC:{source['family']}:{i}:{field}:{factor}|{fmt}"
            assert row['status']=='completed' or (source['family']=='cold_qualification' and field=='device_reads_per_call')
            if factor!=1:
                assert source['family']=='floors' and factor==2
                assert json.loads(row['floor_contract'])['latency_semantics']=='half_round_trip'
            expected=format(float(row[field])*factor,fmt)
        assert definitions[key]==expected,(key,definitions[key],expected)
    tables=sorted(generated.glob('tab-rwc-*.tex')); assert len(tables)==6
    referenced=set()
    for path in [*tables,generated/'rwc-findings.tex']:
        text=path.read_text(); refs=re.findall(r'\\N\{([^{}]+)\}',text); referenced.update(refs)
        assert all(k in definitions for k in refs)
        if path in tables:
            assert 'Unmatched dimensions:' in text and 'lane-limited' in text,path
            body=text.split(r'\midrule',1)[1].split(r'\bottomrule')[0]
            for line in body.strip().splitlines():
                first_metric = 3 if path.stem in ('tab-rwc-write','tab-rwc-read-cold','tab-rwc-read-warm') else 2
                for cell in line.rstrip('\\').split(' & ')[first_metric:]:
                    residue=re.sub(r'\\N\{[^{}]+\}','',cell)
                    residue=re.sub(r'\\\\\[-?\d+pt\]','',residue)
                    assert not re.search(r'\d',residue),(path,cell)
            assert ('SYNTHETIC' in text)==manifest['synthetic']
            assert text.count('Read lanes:') == 1, path
    for path in [*generated.glob('*.tex'),brief/'rwc/section-rwc.tex',brief/'rwc/appendix-rwc.tex']:
        assert not re.search(r'\\us(?:[A-Za-z]|\s+[A-Za-z])',path.read_text()),path
    assert referenced==definitions.keys()
    labels={'native':'Native','native page_cache':'Native page cache','native value_wal_barrier':'Native per-put barrier',
            'native value_wal_group_barrier':'Native grouped barrier','tcp':'Owner TCP','verbs':'Owner verbs'}
    # Independently reconstruct every compact cell, including every c5 case.
    for suffix,groups in [('write',('G14N','G14O1')),('read-cold',('G15C',)),('read-warm',('G15M',))]:
        text=(generated/f'tab-rwc-{suffix}.tex').read_text()
        lines=[s.rstrip('\\') for s in text.splitlines() if re.match(r'^\d+ KiB & ',s)]
        expected_shapes={(r['size_bytes'],labels[row_arm(r)],'r'+r['repetition']) for r in data['latency_vs_size'] if r['group'] in groups}
        seen=set()
        for line in lines:
            cells=line.split(' & '); assert len(cells)==6
            size=str(int(cells[0].split()[0])*1024); label=cells[1]; rep=cells[2]
            assert rep in ('r0','r1') and (size,label,rep) not in seen
            seen.add((size,label,rep))
            for column,c in enumerate((1,5,16),3):
                found=[(i,r) for i,r in enumerate(data['latency_vs_size']) if r['group'] in groups and r['size_bytes']==size
                       and labels[row_arm(r)]==label and int(r['readers'] or r['writers'])==c and r['repetition']==rep[1:]]
                assert len(found)==1; i,row=found[0]
                if row['status']!='completed':
                    expected=row['status'].replace('_',r'\_')
                else:
                    values=[]
                    for field in ('p50_us','p99_us'):
                        v=float(row[field]); fmt='.2f' if v<10 else '.1f' if v<100 else '.0f'
                        values.append(r'\N{'+f'RWC:latency_vs_size:{i}:{field}:1|{fmt}'+'}')
                    expected='/'.join(values)
                assert cells[column]==expected,(suffix,size,label,rep,c,cells[column],expected)
        assert seen==expected_shapes and len(lines)==len(expected_shapes)
    cold_text=(generated/'tab-rwc-cold-qualification.tex').read_text()
    for row in data['parity_matrix']:
        if row['group']!='G15C': continue
        prefix=f"{int(row['size_bytes'])//1024} KiB / c{row['readers']} & {labels[row_arm(row)]} & "
        lines=[s for s in cold_text.splitlines() if s.startswith(prefix)]; assert len(lines)==1
        cells=lines[0].rstrip('\\').split(' & '); column=2+2*int(row['repetition'])
        assert cells[column]==row['status'].replace('_',r'\_')
        qi=next(i for i,q in enumerate(data['cold_qualification']) if q['case_id']==row['case_id'])
        q=data['cold_qualification'][qi]
        expected=r'\N{'+f'RWC:cold_qualification:{qi}:device_reads_per_call:1|.2f'+'}' if q['device_reads_per_call'] else 'n/c'
        assert cells[column+1]==expected
    expected_floors={}
    for i,row in enumerate(data['floors']):
        contract=json.loads(row['floor_contract']); spec=contract.get('spec',contract); g=row['group']
        if g=='G13W': selected=spec['mode'] in ('append_pwrite','append_mmap_fsync'); label=row['arm']+' '+spec['mode']
        elif g in ('G11S','G15F'): selected=spec['region_bytes']==2147483648 and spec['state']=='fresh'; label=row['arm']+' direct fresh 2 GiB'
        else:
            selected=(spec['tool']=='tcp_rtt' and spec['interface']=='eth0' and spec['mode']=='block') or (spec['tool']=='ib_send_lat' and spec['mode']=='polled')
            label=row['arm']+' '+str(spec.get('mode'))
        if not selected: continue
        factor=2 if g=='G11N' and contract.get('latency_semantics')=='half_round_trip' else 1
        cell=row['status'] if row['status']!='completed' else r'\N{'+f'RWC:floors:{i}:mean_us:{factor}|.1f'+'}' if row['mean_us'] else 'n/c'
        expected_floors.setdefault((label.replace('_',r'\_'),row['size_bytes']),{})[row['repetition']]=cell
    body=(generated/'tab-rwc-floors.tex').read_text().split(r'\midrule',1)[1].split(r'\bottomrule')[0]
    floor_lines=body.strip().splitlines(); assert len(floor_lines)==len(expected_floors)
    for line in floor_lines:
        cells=line.rstrip('\\').split(' & '); entry=expected_floors[(cells[0],str(int(cells[1].split()[0])))]
        assert cells[2:]==[entry.get(rep,'--') for rep in ('0','1','2')]
    parity=(generated/'tab-rwc-parity.tex').read_text()
    for g,label in [('G14O1','Write'),('G15C','Cold read'),('G15M','Warm read')]:
        for rep in ('0','1'):
            lines=[s for s in parity.splitlines() if s.startswith(label+' / r'+rep+' & ')]; assert len(lines)==1
            expected=[r'\N{'+f'RWC:availability:{g}:{rep}:{s}'+'}' for s in ('matched','missing partner','failed','unqualified cold','not available')]
            assert lines[0].rstrip('\\').split(' & ')[1:]==expected
    failed={r['case_id']:r for r in data['floors'] if r['status']=='failed'}
    failure_tex=(generated/'rwc-failures.tex').read_text()
    for case_id,row in failed.items():
        assert case_id in failure_tex and 'r'+row['repetition'] in failure_tex
        assert 'network floor server exited before listener readiness' in row['error'] if not manifest['synthetic'] else True
        if not manifest['synthetic']:
            spec=json.loads(row['floor_contract'])['spec']; assert spec['tool']=='ib_write_lat' and spec['mode']=='event'
    assert set(failed)=={c['case_id'] for c in expected_cases if c['status']=='failed'}
    evidence=json.loads((generated/'figure-evidence.json').read_text()); assert len(evidence['figures'])==6
    assert evidence['synthetic']==manifest['synthetic']
    parity_name='rwc-'+('SYNTHETIC-' if manifest['synthetic'] else '')+'parity.pdf'
    assert evidence['figures'][parity_name]['availability_counts']==[[availability_count(data['parity_matrix'],g,rep,s)
            for s in ('matched','missing partner','failed','unqualified cold','not available')]
            for g in ('G14O1','G15C','G15M') for rep in ('0','1')]
    points=0
    for filename,figure in evidence['figures'].items():
        path=brief/('rwc/synthetic-figures' if manifest['synthetic'] else 'figures')/filename
        assert path.is_file() and path.stat().st_size>1000
        assert ('SYNTHETIC' in filename)==manifest['synthetic']
        for point in figure['points']:
            row=data[point['family']][point['row']]
            assert row['status']=='completed' and row['case_id']==point['case_id'] and row['repetition']==point['repetition']
            assert math.isclose(float(row[point['field']])*point['factor'],point['value'],rel_tol=1e-12)
            if point['factor']==2: assert json.loads(row['floor_contract'])['latency_semantics']=='half_round_trip'
            for field in ('readers','writers','read_lanes','native_shards','durability_tier','native_durability'):
                assert point.get(field)==row.get(field)
            points+=1
    pages=None
    if args.pdf:
        pdf=brief/'main.pdf'; text=subprocess.check_output(['pdftotext','-layout',str(pdf),'-'],text=True)
        pages=[s for s in text.split('\f') if s.strip()]; assert len(pages)<=56,len(pages)
        assert 'Two-VM remote reads and writes (run 9)' in text and 'lane-limited' in text
        assert 'Run 9: cold qualification' in text and 'Run 9: floors and provenance' in text
        if not manifest['synthetic']: assert 'SYNTHETIC' not in text
        log=(brief/'main.log').read_text(); bad=[s for s in log.splitlines() if re.search(r'Overfull|Underfull|undefined reference|LaTeX Warning|Missing number|Missing character',s)]
        assert not bad,bad
        definitions_old=dict(re.findall(r'\\detokenize\{([^{}]+)\}\\endcsname\{((?:[^{}]|\{,\})*)\}',(brief/'numbers.tex').read_text()))
        baseline=json.loads((brief/'number-baseline.json').read_text()); assert set(baseline)<=definitions_old.keys()
        changed=[k for k in baseline if baseline[k]!=definitions_old[k]]
        assert len(changed)==12 and all(re.fullmatch(r'Q:run7-sustained-extract:\d+:late_puts_s\|\.0f',k) for k in changed)
        fls=(brief/'main.fls').read_text()
        assert 'SYNTHETIC' not in '\n'.join(s for s in fls.splitlines() if s.startswith('INPUT '))
    result=dict(pass_check=True,numbers=len(definitions),tables=6,figure_points=points,figures=6,
                pages=len(pages) if pages else None,synthetic=manifest['synthetic'],
                controller_completed=sum(c['status']=='completed' for c in expected_cases),failed_cases=len(failed),
                pdf_sha256=sha(brief/'main.pdf') if args.pdf else None,
                visual_inspection='separate image inspection; not asserted by this checker')
    (generated/'validation-rwc.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,sort_keys=True))


if __name__=='__main__': main()
