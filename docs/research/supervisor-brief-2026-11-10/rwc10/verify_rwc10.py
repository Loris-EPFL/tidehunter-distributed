#!/usr/bin/env python3
"""Independent CSV, numeric-cell, matched-ratio, figure-evidence and PDF checks."""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import re
import subprocess


def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--brief',type=Path,default=Path(__file__).resolve().parent.parent)
    p.add_argument('--synthetic',action='store_true'); p.add_argument('--pdf',action='store_true')
    p.add_argument('--excluded-attempt',action='store_true',help='verify quarantined aborted-attempt layout products only')
    args=p.parse_args(); brief=args.brief.resolve()
    assert not (args.synthetic and args.excluded_attempt)
    mode='SYNTHETIC' if args.synthetic else 'EXCLUDED' if args.excluded_attempt else ''
    generated=brief/'rwc10'/((mode+'-generated') if mode else 'generated')
    manifest=json.loads((generated/'inputs-rwc10.json').read_text()); assert manifest['synthetic']==args.synthetic
    assert manifest.get('excluded_from_final',False)==args.excluded_attempt
    data={}
    for path in (brief/manifest['data_directory']).glob('*.csv'):
        with path.open(newline='') as stream: data[path.stem]=list(csv.DictReader(stream))
    assert set(data)=={'ab_warm','ab_pairs','cold_fabric','cold_pairs','parity_matrix','run9-floors','campaign_cases'}
    for name,wanted in manifest['sources'].items(): assert sha(name)==wanted,name
    for name,wanted in manifest['outputs'].items(): assert sha(brief/name)==wanted,name
    matrix=json.loads(Path(manifest['matrix_path']).read_text()); raw_report=json.loads(Path(manifest['report_path']).read_text())
    planned={j['case_id']:j for j in matrix['jobs']}
    assert len(planned)==80 and len(raw_report['cases'])==80
    assert all(c['job']==planned[c['case_id']] for c in raw_report['cases'])
    assert data['campaign_cases']==[dict(case_id=c['case_id'],group=c['job']['group'],status=c['status'],error=c.get('error') or '') for c in raw_report['cases']]
    assert manifest['report_complete']==raw_report.get('complete')
    registry=json.loads((brief/'inputs.json').read_text())
    if registry.get('rwc10',{}).get('manifest')==str((generated/'inputs-rwc10.json').relative_to(brief)):
        assert registry['outputs'][registry['rwc10']['manifest']]==sha(generated/'inputs-rwc10.json')
        for name,wanted in manifest['outputs'].items(): assert registry['outputs'][name]==wanted,name
    warm=('tcp-mtls-adaptive','tcp-trusted-adaptive','tcp-trusted-busy','verbs-adaptive','verbs-busy')
    cold=('owner-tcp-trusted','owner-verbs','nvmeof-rdma-kernel','nvmeof-rdma-spdk','nvmeof-tcp-kernel')
    cells=((1024,1),(1024,16),(65536,1),(65536,16))
    definitions_pairs={'TLS cost':(warm[0],warm[1]),'transport adaptive':(warm[1],warm[3]),'transport busy':(warm[2],warm[4]),'parking TCP':(warm[1],warm[2]),'parking verbs':(warm[3],warm[4])}
    codes=dict(zip(warm+cold,('TM-A','TT-A','TT-B','V-A','V-B','O-T','O-V','K-R','S-R','K-T')))
    assert len(data['ab_warm'])==len(data['cold_fabric'])==40 and len(data['parity_matrix'])==80
    assert {r['case_id']:r for r in data['parity_matrix']}=={r['case_id']:r for r in data['ab_warm']+data['cold_fabric']}
    assert {r['case_id'] for r in data['parity_matrix']}==planned.keys()
    controllers={c['case_id']:c for c in raw_report['cases']}
    for row in data['parity_matrix']:
        job=planned[row['case_id']]
        assert row['group']==job['group'] and int(row['size_bytes'])==job['value_size']
        assert int(row['readers'])==job['concurrency'] and int(row['repetition'])==job['repeat']
        assert row['backend']==job['backend']
        if job['group']=='G16P':
            expected_arm={('tidehunter_tcp','adaptive'):warm[0],('tidehunter_tcp_trusted','adaptive'):warm[1],
                          ('tidehunter_tcp_trusted','busy'):warm[2],('tidehunter_verbs_native','adaptive'):warm[3],
                          ('tidehunter_verbs_native','busy'):warm[4]}[job['backend'],job['owner_polling']]
        else:
            expected_arm={('tidehunter_tcp_trusted',None):cold[0],('tidehunter_verbs_native',None):cold[1],
                          ('tidehunter_nvme_rdma','kernel'):cold[2],('tidehunter_nvme_rdma','spdk'):cold[3],
                          ('tidehunter_nvme_tcp','kernel'):cold[4]}[job['backend'],job.get('nvmeof_target')]
        assert row['arm']==expected_arm
        if 'owner_polling' in job: assert row['owner_polling']==job['owner_polling']
        if controllers[row['case_id']]['status']!='completed':
            assert row['status']==controllers[row['case_id']]['status']
    def close_window(row):
        return math.isclose(float(row['mean_inflight_calls']),float(row['calls_per_second'])*float(row['mean_us'])/1e6,rel_tol=1e-12,abs_tol=1e-12)
    for family,arms in [('ab_warm',warm),('cold_fabric',cold)]:
        shapes={(r['arm'],r['size_bytes'],r['readers'],r['repetition']) for r in data[family]}
        assert shapes=={(a,str(s),str(c),str(rep)) for a in arms for s,c in cells for rep in (0,1)}
        for row in data[family]:
            assert int(row['read_lanes'])==(1 if row['readers']=='1' else 4)
            if row['status']=='completed':
                assert row['security_label']==('mutual_tls' if row['arm']==warm[0] else 'trusted_network_no_tls')
                assert all(row[f] and float(row[f])>=0 for f in ('mean_us','p50_us','p99_us','n','calls_per_second'))
                assert close_window(row),('rate-latency occupancy identity',row['case_id'])
            if family=='cold_fabric':
                if row['status']=='completed':
                    assert row['qualification']=='qualified' and float(row['device_reads_per_call'])>=.9
                    if row['arm'] in cold[2:]: assert row['nvme_path_qualified']==row['sealed_accounting_qualified']=='True'
                elif row['status']=='unqualified': assert all(not row[f] for f in ('mean_us','p50_us','p99_us'))
            if args.synthetic and family=='ab_warm':
                for field in ('client_cores','owner_cores'):
                    assert (.9<=float(row[field])<=1.3) if row['owner_polling']=='busy' else (.05<=float(row[field])<=.15)
                if row['arm'].startswith('verbs'): assert (float(row['cq_idle_waits'])==0) if row['owner_polling']=='busy' else float(row['cq_idle_waits'])>0
                else: assert row['cq_idle_waits']=='not recorded'
    def close(a,b): return math.isclose(float(a),float(b),rel_tol=1e-12,abs_tol=1e-12)
    for family,case_family in [('ab_pairs','ab_warm'),('cold_pairs','cold_fabric')]:
        assert len(data[family])==(40 if family=='ab_pairs' else 48)
        expected_arms=set(definitions_pairs.values()) if family=='ab_pairs' else {(a,b) for a in cold[:2] for b in cold[2:]}
        seen=set()
        for pair in data[family]:
            assert (pair['arm_a'],pair['arm_b']) in expected_arms
            if family=='ab_pairs': assert (pair['arm_a'],pair['arm_b'])==definitions_pairs[pair['comparison']]
            identity=tuple(pair[k] for k in ('arm_a','arm_b','size_bytes','readers','repetition')); assert identity not in seen; seen.add(identity)
            a=next(r for r in data[case_family] if r['case_id']==pair['case_a']); b=next(r for r in data[case_family] if r['case_id']==pair['case_b'])
            assert all(a[k]==b[k]==pair[k] for k in ('size_bytes','readers','repetition'))
            assert a['arm']==pair['arm_a'] and b['arm']==pair['arm_b']
            eligible=pair['parity']=='matched' and a['status']==b['status']=='completed'
            if eligible and a['mean_us'] and b['mean_us']: assert close(pair['mean_ratio_a_over_b'],float(a['mean_us'])/float(b['mean_us']))
            else: assert not pair['mean_ratio_a_over_b']
            for field in ('client_cores','owner_cores'):
                for side,row in [('a',a),('b',b)]: assert pair[field+'_'+side]==row[field]
                delta=pair[field+'_b_minus_a']
                if eligible and a[field] and b[field]: assert close(delta,float(b[field])-float(a[field]))
                else: assert not delta
    defs=dict(re.findall(r'\\detokenize\{([^{}]+)\}\\endcsname\{([^{}]*)\}',(generated/'rwc10-numbers.tex').read_text()))
    assert defs.keys()==manifest['number_sources'].keys()
    def check_factor(source,rows):
        factor=source.get('factor',1)
        if factor==1: return
        if source['family']=='run9-floors':
            assert factor==2 and source['field']=='mean_us'
            assert all(json.loads(row['floor_contract'])['latency_semantics']=='half_round_trip' for row in rows)
        else:
            assert source['family']=='ab_warm' and source['field']=='calls_per_second'
            assert factor==.001 or all(factor==int(row['size_bytes'])/1e9 for row in rows)
    for key,source in manifest['number_sources'].items():
        rows=data[source['family']]
        if source.get('operation')=='count': expected=str(sum(r[source['field']]==source['value'] for r in rows))
        elif source.get('operation') in ('minimum','maximum','sum'):
            selected=[r for r in rows if all(r[k] in (v if isinstance(v,(list,tuple,set)) else [v]) for k,v in source['filters'].items()) and r[source['field']] not in ('','not recorded')]
            assert selected and key.startswith('F10:summary:') and key.endswith(':'+source['operation'])
            check_factor(source,selected)
            values=[float(r[source['field']]) for r in selected]
            expected=format({'minimum':min,'maximum':max,'sum':sum}[source['operation']](values)*source['factor'],source['format'])
        else:
            assert key==f"F10:{source['family']}:{source['row']}:{source['field']}:{source['factor']}|{source['format']}"
            row=rows[source['row']]; assert row[source['field']] not in ('','not recorded')
            check_factor(source,[row])
            expected=format(float(row[source['field']])*source['factor'],source['format'])
        assert defs[key]==expected,(key,defs[key],expected)
    # Select these scientific cohorts directly, independent of the builder's filter helper.
    for label,owner,size,readers in [('verbsNamespace',cold[1],None,None),('tcpNamespace1k16',cold[0],'1024','16'),('tcpNamespace64k',cold[0],'65536',None)]:
        values=[float(r['mean_ratio_a_over_b']) for r in data['cold_pairs'] if r['arm_a']==owner and r['arm_b'] in (cold[2],cold[3])
                and r['parity']=='matched' and r['status_a']==r['status_b']=='completed'
                and (size is None or r['size_bytes']==size) and (readers is None or r['readers']==readers)]
        if values:
            for operation,expected in [('minimum',min(values)),('maximum',max(values))]:
                key='F10:summary:'+label+':'+operation
                assert defs[key]==format(expected,'.2f'),('completed namespace span must be numeric',label)
    referenced=set(); tables=sorted(generated.glob('tab-rwc10-*.tex')); assert len(tables)==6
    for path in [*tables,generated/'rwc10-findings.tex']:
        text=path.read_text(); refs=re.findall(r'\\N\{([^{}]+)\}',text); referenced.update(refs)
        assert set(refs)<=defs.keys()
        assert not re.search(r'\\us(?:[A-Za-z]|\s+[A-Za-z])',text)
        if path in tables: assert 'Unmatched dimensions:' in text and 'coordinator read driver' in text
    assert referenced==defs.keys()
    def macro(family,i,field,fmt='.1f',factor=1):
        return r'\N{'+f'F10:{family}:{i}:{field}:{factor}|{fmt}'+'}' if data[family][i][field] not in ('','not recorded') else 'NR'
    for suffix,family,arms in [('warm','ab_warm',warm),('cpu','ab_warm',warm),('cold','cold_fabric',cold)]:
        text=(generated/f'tab-rwc10-{suffix}.tex').read_text()
        body=text.split(r'\midrule',1)[1].split(r'\bottomrule')[0]
        rows=[line.rstrip('\\').split(' & ') for line in body.strip().splitlines()]; assert len(rows)==10
        for arm in arms:
            for rep in ('0','1'):
                found=[r for r in rows if r[:2]==[codes[arm],'r'+rep]]; assert len(found)==1
                assert len(found[0])==6
                for column,(size,c) in enumerate(cells,2):
                    i,row=next((i,r) for i,r in enumerate(data[family]) if (r['arm'],r['size_bytes'],r['readers'],r['repetition'])==(arm,str(size),str(c),rep))
                    if suffix=='cold': expected=(macro(family,i,'mean_us') if row['status']=='completed' else row['status'].replace('_',' '))+'; '+macro(family,i,'device_reads_per_call','.2f')
                    elif row['status']!='completed': expected=row['status'].replace('_',' ')
                    elif suffix=='warm': expected='/'.join(macro(family,i,f) for f in ('mean_us','p50_us','p99_us'))
                    else: expected='/'.join(macro(family,i,f,'.2f') for f in ('client_cores','owner_cores'))+'; '+macro(family,i,'cq_idle_waits','.0f')
                    assert found[0][column]==expected,(suffix,arm,rep,size,c)
    pair_table=(generated/'tab-rwc10-pairs.tex').read_text()
    for comparison in definitions_pairs:
        for rep in ('0','1'):
            lines=[s.rstrip('\\').split(' & ') for s in pair_table.splitlines() if s.startswith(comparison+' & r'+rep+' & ')]; assert len(lines)==1
            for column,(size,c) in enumerate(cells,2):
                i,row=next((i,r) for i,r in enumerate(data['ab_pairs']) if (r['comparison'],r['size_bytes'],r['readers'],r['repetition'])==(comparison,str(size),str(c),rep))
                expected=macro('ab_pairs',i,'mean_ratio_a_over_b','.2f') if row['parity']=='matched' and row['status_a']==row['status_b']=='completed' else 'not run' if 'not_run' in (row['status_a'],row['status_b']) else 'unavailable'
                assert lines[0][column]==expected
    floors={}
    for i,row in enumerate(data['run9-floors']):
        if row['group']!='G11N' or row['size_bytes']!='1024': continue
        contract=json.loads(row['floor_contract']); s=contract['spec']; tool,mode=s['tool'],s['mode']; label=None
        if tool=='tcp_rtt' and s['interface']=='eth0': label={'block':'TCP blocking','spin':'TCP user spin','busy_poll':'TCP SO_BUSY_POLL'}.get(mode)
        elif tool=='ib_send_lat': label={'polled':'RDMA SEND polled','event':'RDMA SEND event'}.get(mode)
        elif tool=='ib_read_lat' and mode=='polled': label='RDMA READ polled'
        elif tool=='fi_pingpong' and s['provider']=='verbs;ofi_rxm' and s['endpoint']=='rdm': label='verbs;ofi_rxm'
        if label: floors[label,row['repetition']]=(i,2 if contract['latency_semantics']=='half_round_trip' else 1)
    assert len(floors)==21
    ladder=(generated/'tab-rwc10-ladder.tex').read_text()
    for (label,rep),(i,factor) in floors.items():
        lines=[s.rstrip('\\').split(' & ') for s in ladder.splitlines() if s.startswith(label.replace('_',r'\_')+' & ')]; assert len(lines)==1
        row=data['run9-floors'][i]
        assert lines[0][1+int(rep)]==(macro('run9-floors',i,'mean_us',factor=factor) if row['status']=='completed' else row['status'])
    evidence=json.loads((generated/'figure-evidence.json').read_text()); assert evidence['synthetic']==args.synthetic and len(evidence['figures'])==5
    assert evidence.get('excluded_from_final',False)==args.excluded_attempt
    points=0
    for filename,figure in evidence['figures'].items():
        assert (brief/filename).is_file() and ('SYNTHETIC' in filename)==args.synthetic
        assert ('EXCLUDED' in filename)==args.excluded_attempt
        for point in figure['points']:
            row=data[point['family']][point['row']]; assert close(point['value'],float(row[point['field']])*point['factor'])
            for field in ('case_id','repetition','arm','size_bytes','readers','read_lanes','security_label','owner_polling'): assert point[field]==row.get(field)
            if row.get('status')=='unqualified': assert point['field']=='device_reads_per_call' and point['kind']=='annotation'
            points+=1
    pages=None
    if args.pdf:
        text=subprocess.check_output(['pdftotext','-layout',str(brief/'main.pdf'),'-'],text=True)
        pages=[s for s in text.split('\f') if s.strip()]; assert len(pages)<=56
        assert 'Run 10: fabric, security and polling' in text
        assert 'SYNTHETIC' in text if args.synthetic else 'SYNTHETIC' not in text
        log=(brief/'main.log').read_text(); bad=[s for s in log.splitlines() if re.search(r'Overfull|Underfull|undefined reference|LaTeX Warning|Missing character',s)]; assert not bad,bad
        assert len(re.findall(r'\\includegraphics', (brief/'rwc10/section-rwc10.tex').read_text()))==0,'use generated hooks'
    result=dict(pass_check=True,numbers=len(defs),tables=6,figure_points=points,figures=5,synthetic=args.synthetic,
                excluded_from_final=args.excluded_attempt,pages=len(pages) if pages else None,
                pdf_sha256=sha(brief/'main.pdf') if args.pdf else None,visual_inspection='separate rendered-image review, not asserted by this checker')
    (generated/'validation-rwc10.json').write_text(json.dumps(result,indent=2)+'\n'); print(json.dumps(result,sort_keys=True))


if __name__=='__main__': main()
