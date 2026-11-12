#!/usr/bin/env python3
"""Generate run-10 number macros, tables, findings and source/output registration."""
import argparse
import csv
import json
from pathlib import Path
import shutil
from rwc10_data import FAMILIES, WARM, COLD, CELLS, COMPARISONS, CODES, LABELS, TEX_CONTRACT, DEFAULT_FLOORS, FLOOR_LABELS, load, one, number_value, selected_floor, sha, tex


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--provenance', type=Path, required=True)
    p.add_argument('--export', type=Path, help='raw selected export; defaults to provenance parent for fixtures only')
    p.add_argument('--report', type=Path, help='raw comparison directory; required explicitly for real data')
    p.add_argument('--floors', type=Path, default=DEFAULT_FLOORS)
    p.add_argument('--brief', type=Path, default=Path(__file__).resolve().parent.parent)
    p.add_argument('--register', action='store_true')
    args = p.parse_args(); brief = args.brief.resolve()
    provenance = json.loads(args.provenance.read_text()); synthetic = provenance['synthetic']
    for key in ('run_id','parent_run_id','package_sha256','source_revision','source_dirty','launch_time','vm_names'):
        if provenance.get(key) in (None,'',[]): raise ValueError('missing provenance: '+key)
    if not isinstance(provenance['source_dirty'],bool) or len(provenance['vm_names']) != 2:
        raise ValueError('need dirty bool and coordinator/owner VM names')
    if not isinstance(synthetic,bool): raise ValueError('synthetic must be a Boolean')
    if not synthetic and ('SYNTHETIC' in str(args.output.resolve()) or 'SYNTHETIC' in json.dumps(provenance)):
        raise ValueError('synthetic input cannot be labelled real')
    if not synthetic and not provenance.get('receipt_sources'):
        raise ValueError('real provenance needs its source receipt paths')
    if not synthetic and (args.export is None or args.report is None):
        raise ValueError('real inputs require explicit --export and --report paths')
    excluded=provenance.get('excluded_from_final',False)
    if not isinstance(excluded,bool) or (synthetic and excluded): raise ValueError('excluded_from_final must be a separate Boolean mode')
    if provenance['run_id']=='bench-9dc7f51e57e9449f' and not excluded:
        raise ValueError('aborted attempt 1 cannot supply final results; use excluded_from_final for a quarantined layout test')
    if excluded and args.register: raise ValueError('excluded attempt inputs must never enter the final root registry')
    mode='SYNTHETIC' if synthetic else 'EXCLUDED' if excluded else ''
    export=args.export or args.provenance.parent
    report=args.report or export
    if 'focused-launch-20261110-fabricab1' in str(export.resolve()) and not excluded:
        raise ValueError('attempt-1 export is excluded from the final brief')
    matrix_path=export/'matrix.json'; report_path=report/'comparison.json'
    matrix=json.loads(matrix_path.read_text()); raw_report=json.loads(report_path.read_text())
    planned={j['case_id']:j for j in matrix['jobs']}
    if len(planned)!=80 or len(raw_report['cases'])!=80 or len({c['case_id'] for c in raw_report['cases']})!=80 or any(c['job']!=planned.get(c['case_id']) for c in raw_report['cases']):
        raise ValueError('raw matrix/report mismatch for the reviewed 80-job export')
    generated = brief/'rwc10'/((mode+'-generated') if mode else 'generated')
    copied = brief/'rwc10'/((mode+'-data') if mode else 'data')
    generated.mkdir(parents=True,exist_ok=True); copied.mkdir(parents=True,exist_ok=True)
    sources = {str(args.provenance.resolve()):sha(args.provenance),str(args.floors.resolve()):sha(args.floors)}
    for receipt in provenance.get('receipt_sources',[]):
        sources[str(Path(receipt).resolve())]=sha(receipt)
    for path in (matrix_path,report_path):
        sources[str(path.resolve())]=sha(path)
    for case in raw_report['cases']:
        for relative in ('receipt.json','resource-interval.json','media_evidence.json','result/manifest.json','result/measurement-read-evidence.json'):
            path=export/'cases'/case['case_id']/relative
            if path.is_file(): sources[str(path.resolve())]=sha(path)
    for name in [f+'.csv' for f in FAMILIES]+['fabric_ab.md']:
        src=args.output/name; dst=copied/name
        if src.resolve()!=dst.resolve(): shutil.copyfile(src,dst)
        sources[str(src.resolve())]=sha(src)
    shutil.copyfile(args.floors,copied/'run9-floors.csv')
    (generated/'provenance.json').write_text(json.dumps(provenance,indent=2)+'\n')
    data=load(copied)
    with (copied/'run9-floors.csv').open(newline='') as stream: data['run9-floors']=list(csv.DictReader(stream))
    data['campaign_cases']=[dict(case_id=c['case_id'],group=c['job']['group'],status=c['status'],error=c.get('error') or '') for c in raw_report['cases']]
    with (copied/'campaign_cases.csv').open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=('case_id','group','status','error'))
        writer.writeheader(); writer.writerows(data['campaign_cases'])
    numbers={}; definitions={}; files=[]

    def number(family,index,field,fmt='.1f',factor=1):
        row=data[family][index]; value=number_value(row,field)
        if value is None: return 'NR'
        key=f'F10:{family}:{index}:{field}:{factor}|{fmt}'
        numbers[key]=format(value*factor,fmt)
        definitions[key]=dict(family=family,row=index,field=field,factor=factor,format=fmt)
        return r'\N{'+key+'}'

    def metric(family,index,field,fmt='.1f'):
        row=data[family][index]
        return number(family,index,field,fmt) if row['status']=='completed' else tex(row['status'].replace('_',' '))

    def pair_number(index):
        row=data['ab_pairs'][index]
        if 'not_run' in (row['status_a'],row['status_b']): return 'not run'
        if row['parity']!='matched' or row['status_a']!=row['status_b'] or row['status_a']!='completed': return 'unavailable'
        return number('ab_pairs',index,'mean_ratio_a_over_b','.2f')

    def count(label,family,field,value):
        key='F10:count:'+label; numbers[key]=str(sum(r[field]==value for r in data[family]))
        definitions[key]=dict(operation='count',family=family,field=field,value=value)
        return r'\N{'+key+'}'

    def summary(label,family,field,filters,operation='minimum',fmt='.1f',factor=1):
        filters={k:(sorted(v) if isinstance(v,set) else list(v) if isinstance(v,tuple) else v) for k,v in filters.items()}
        values=[number_value(row,field) for row in data[family]
                if all(row[k] in (v if isinstance(v,(list,tuple,set)) else [v]) for k,v in filters.items())]
        values=[v for v in values if v is not None]
        if not values: return 'NR'
        key='F10:summary:'+label+':'+operation
        numbers[key]=format({'minimum':min,'maximum':max,'sum':sum}[operation](values)*factor,fmt)
        definitions[key]=dict(operation=operation,family=family,field=field,filters=filters,format=fmt,factor=factor)
        return r'\N{'+key+'}'

    def span(label,family,field,filters,fmt='.1f',factor=1):
        low=summary(label,family,field,filters,'minimum',fmt,factor)
        high=summary(label,family,field,filters,'maximum',fmt,factor)
        return 'NR' if low==high=='NR' else low+'--'+high

    def series(family,field,filters,fmt='.1f',factor=1):
        values=[]
        for rep in ('0','1'):
            found=[(i,row) for i,row in enumerate(data[family]) if row['repetition']==rep and all(row[k]==str(v) for k,v in filters.items())]
            if len(found)!=1: raise ValueError(('finding selection',family,filters,rep))
            i,row=found[0]
            eligible=row.get('status','completed')=='completed' and row.get('status_a','completed')==row.get('status_b','completed')=='completed' and row.get('parity','matched')=='matched'
            values.append(number(family,i,field,fmt,factor) if eligible else 'not run' if 'not_run' in (row.get('status'),row.get('status_a'),row.get('status_b')) else 'unavailable')
        return '/'.join(values)

    def write(name,text):
        path=generated/name; path.write_text(text+'\n'); files.append(path)

    def table(suffix,headers,rows,caption):
        prefix=r'\textbf{SYNTHETIC --- invented layout fixture, not results.} ' if synthetic else ''
        if synthetic and suffix=='ladder':
            prefix=r'\textbf{SYNTHETIC run-10 layout; these run-9 floors are real measurements.} '
        if excluded: prefix=r'\textbf{EXCLUDED ABORTED ATTEMPT --- layout test only, never final results.} '
        write('tab-rwc10-'+suffix+'.tex',r'\begin{table}[H]\centering\small\setlength{\tabcolsep}{3pt}'+'\n'+
              r'\begin{tabular}{'+'l'*len(headers)+r'}\toprule'+'\n'+' & '.join(headers)+r'\\\midrule'+'\n'+
              '\n'.join(' & '.join(row)+r'\\' for row in rows)+'\n'+r'\bottomrule\end{tabular}'+'\n'+
              r'\caption{'+prefix+caption+' '+TEX_CONTRACT+'}\n'+r'\end{table}')

    arm_rows=[]
    for arm in WARM+COLD:
        backend = ('tidehunter_tcp' if arm==WARM[0] else 'tidehunter_tcp_trusted' if arm in (WARM[1],WARM[2],COLD[0]) else
                   'tidehunter_verbs_native' if arm in (WARM[3],WARM[4],COLD[1]) else 'tidehunter_nvme_tcp' if arm==COLD[4] else 'tidehunter_nvme_rdma')
        security='mutual TLS' if arm==WARM[0] else 'trusted; no TLS'
        policy='Busy spins' if arm in (WARM[2],WARM[4]) else 'adaptive'
        route='warm owner RPC' if arm in WARM else 'cold owner RPC' if arm in COLD[:2] else 'SPDK file AIO' if arm==COLD[3] else 'kernel direct'
        arm_rows.append([CODES[arm],r'\texttt{'+tex(backend)+'}',security,policy,route])
    table('arms',['Code','Backend','Security/control','Polling','Data path'],arm_rows,
          'Configured arm definitions, not measurements. TM/TT: TCP mTLS/trusted; V: verbs trusted; A: adaptive; B: Busy. '
          'O-T/O-V: cold owner RPC; K-R/S-R/K-T: kernel-RDMA/SPDK-RDMA/kernel-TCP namespace reads. '
          'NVMe-oF polling/security describe its owner control channel, not kernel target polling. No SPDK/TCP arm.')
    headers=['Arm','Rep.','1 KiB c1','1 KiB c16','64 KiB c1','64 KiB c16']
    warm_rows=[]; cpu_rows=[]; cold_rows=[]; pair_rows=[]
    for arm in WARM:
        for rep in (0,1):
            latency=[]; cores=[]
            for size,readers in CELLS:
                i,row=one(data['ab_warm'],arm,size,readers,rep)
                latency.append('/'.join(metric('ab_warm',i,f) for f in ('mean_us','p50_us','p99_us')) if row['status']=='completed' else tex(row['status'].replace('_',' ')))
                cores.append('/'.join(number('ab_warm',i,f,'.2f') for f in ('client_cores','owner_cores'))+'; '+number('ab_warm',i,'cq_idle_waits','.0f') if row['status']=='completed' else tex(row['status'].replace('_',' ')))
            warm_rows.append([CODES[arm],'r'+str(rep),*latency]); cpu_rows.append([CODES[arm],'r'+str(rep),*cores])
    table('warm',headers,warm_rows,r'Warm owner reads: mean/p50/p99 API latency (\us{}). One real row per arm/repetition; all four matched cells. '
          'Full arm names/security/polling are in the arm table. NR means not recorded, never zero.')
    for comparison in COMPARISONS:
        for rep in (0,1):
            cells=[]
            for size,readers in CELLS:
                i,row=one(data['ab_pairs'],comparison,size,readers,rep,field='comparison')
                cells.append(pair_number(i))
            pair_rows.append([tex(comparison),'r'+str(rep),*cells])
    table('pairs',['A / B','Rep.',*headers[2:]],pair_rows,
          'Ratios of matched per-case means, A/B: TLS = TCP mTLS/TCP trusted (adaptive); transport = trusted TCP/trusted verbs at the named policy; '
          'parking = adaptive/Busy for the named trusted transport. Ratios are not differences or isolated wire/crypto timers. '
          'No ratio for failed, unqualified, missing or unmatched partners.')
    table('cpu',headers,cpu_rows,'Client/owner process cores; CQ idle-wait count. Cores = CPU seconds / elapsed host-gate seconds, '
          'not per-call attribution. TCP CQ = NR, not zero. NVMe kernel workers are outside the owner cgroup. '
          'CPU and CQ counts are evidence, not proof of policy.')
    for arm in COLD:
        for rep in (0,1):
            cells=[]
            for size,readers in CELLS:
                i,row=one(data['cold_fabric'],arm,size,readers,rep)
                mean=number('cold_fabric',i,'mean_us') if row['status']=='completed' else tex(row['status'].replace('_',' '))
                cells.append(mean+'; '+number('cold_fabric',i,'device_reads_per_call','.2f'))
            cold_rows.append([CODES[arm],'r'+str(rep),*cells])
    table('cold',headers,cold_rows,r'Each cell: qualified mean API latency (\us{}); device reads per successful call. '
          'Completed cold rows require the qualified media receipt and ratio >=0.9; NVMe-oF also requires path and sealed-accounting qualification. '
          'Unqualified latency remains unavailable; its device ratio/reasons remain in the CSV. Counts do not prove every call reached NAND. '
          'Owner RPC is buffered/exact-range; namespace controls use kernel direct or SPDK file AIO and a kernel initiator.')
    floor_rows=[]
    for label in FLOOR_LABELS:
        cells=[]
        for rep in ('0','1','2'):
            found=[(i,r,selected_floor(r)) for i,r in enumerate(data['run9-floors']) if r['repetition']==rep and selected_floor(r) and selected_floor(r)[0]==label]
            if len(found)!=1: raise ValueError(('floor population',label,rep))
            i,row,(_,factor)=found[0]
            cells.append(number('run9-floors',i,'mean_us',factor=factor) if row['status']=='completed' else tex(row['status']))
        floor_rows.append([tex(label),*cells])
    table('ladder',['Run-9 network tool','r0 mean','r1 mean','r2 mean'],floor_rows,
          r'Standalone run-9 1-KiB round-trip means (\us{}), CROSS-FLEET context, not run-10 stage timers. '
          r'eth0 TCP blocking/spin/SO\_BUSY\_POLL; RDMA SEND polled/event, READ polled, RDM verbs;ofi\_rxm. '
          'Half-RTT values are doubled only where their receipt says so. All three repetitions remain separate. Never subtract these from engine means.')
    def means(arm):
        return '/'.join(metric('ab_warm',one(data['ab_warm'],arm,1024,1,rep)[0],'mean_us') for rep in (0,1))
    cold_labels={'completed':'qualified','unqualified':'unqualified','failed':'failed','not_run':'not run'}
    cold_statuses=[*cold_labels,*sorted({r['status'] for r in data['cold_fabric']}-cold_labels.keys())]
    cold_summary='; '.join(count('cold-'+status,'cold_fabric','status',status)+' '+tex(cold_labels.get(status,status.replace('_',' '))) for status in cold_statuses)
    findings=dict(warm='At 1 KiB c1, warm means (r0/r1, '+r'\us{}): TCP mTLS adaptive '+means(WARM[0])+', TCP trusted adaptive '+means(WARM[1])+
                  ', trusted Busy '+means(WARM[2])+'; verbs adaptive '+means(WARM[3])+', Busy '+means(WARM[4])+'. These are whole RPCs, not wire timers.',
                  cold='Cold cells: '+cold_summary+'.')
    def ratios(comparison,size=1024,readers=1):
        return series('ab_pairs','mean_ratio_a_over_b',dict(comparison=comparison,size_bytes=size,readers=readers),'.2f')
    def warm_values(arm,field,size=65536,readers=16,fmt='.1f',factor=1):
        return series('ab_warm',field,dict(arm=arm,size_bytes=size,readers=readers),fmt,factor)
    warm_ok=dict(status='completed')
    parking_ok=dict(parity='matched',status_a='completed',status_b='completed',comparison=['parking TCP','parking verbs'])
    trusted=[WARM[1],WARM[2],WARM[3],WARM[4]]
    findings['answer']=(r'\textbf{What changed in the small-gain explanation.} At 1 KiB c1 the mTLS/trusted TCP ratios are '+ratios('TLS cost')+
        '; at 64 KiB they are '+ratios('TLS cost',65536,1)+' (c1) and '+ratios('TLS cost',65536,16)+
        ' (c16). Run 9 mixed security classes; run 10 exposes a larger-size security-associated whole-case penalty, not an isolated crypto timer. '
        'Trusted TCP/verbs ratios at 1 KiB c16 are '+ratios('transport adaptive',1024,16)+' (Adaptive) and '+ratios('transport busy',1024,16)+
        ' (Busy). Verbs Adaptive/Busy is '+ratios('parking verbs')+' at 1 KiB c1 and '+ratios('parking verbs',65536,1)+
        ' at 64 KiB c1; c16 gives '+ratios('parking verbs',1024,16)+' and '+ratios('parking verbs',65536,16)+
        ', respectively: no consistent c16 benefit. Trusted TCP Adaptive/Busy at c1 is '+ratios('parking TCP')+' and '+ratios('parking TCP',65536,1)+
        '. These are whole-case ratios, not isolated crypto or wake-up intervals; spinning changes both hosts.')
    findings['capacity']=('At 1 KiB c16, trusted TCP achieves '+span('tcp1kRate','ab_warm','calls_per_second',dict(warm_ok,arm=trusted[:2],size_bytes='1024',readers='16'),factor=.001)+
        ' versus verbs '+span('verbs1kRate','ab_warm','calls_per_second',dict(warm_ok,arm=trusted[2:],size_bytes='1024',readers='16'),factor=.001)+
        ' thousand calls/s. At 64 KiB c16 the trusted arms span '+span('rate64k','ab_warm','calls_per_second',dict(warm_ok,arm=trusted,size_bytes='65536',readers='16'),factor=.001)+
        ' thousand calls/s, or '+span('payload64k','ab_warm','calls_per_second',dict(warm_ok,arm=trusted,size_bytes='65536',readers='16'),fmt='.2f',factor=65536/1e9)+
        ' GB/s of value payload (decimal; no protocol bytes). Verbs Busy p50/p99 are '+warm_values(WARM[4],'p50_us')+' / '+warm_values(WARM[4],'p99_us')+
        r' \us{} (r0/r1): the distribution is wide, not captured by the mean alone. \textbf{[INFERENCE]} A shared ceiling is plausible; its bottleneck is not measured.')
    findings['cpu']=('Matched Busy-minus-Adaptive owner CPU differences span '+span('ownerDeltaC1','ab_pairs','owner_cores_b_minus_a',dict(parking_ok,readers='1'))+
        ' cores at c1 and '+span('ownerDeltaC16','ab_pairs','owner_cores_b_minus_a',dict(parking_ok,readers='16'))+
        '; client differences span '+span('clientDelta','ab_pairs','client_cores_b_minus_a',parking_ok)+
        '. Adaptive owner CPU spans '+span('adaptiveOwner','ab_warm','owner_cores',dict(warm_ok,owner_polling='adaptive'),fmt='.2f')+
        ' cores. These are independent host-gate observations, not CPU per call or proof of polling policy.')
    findings['coldcomparison']=('Device reads/call span '+span('coldDevice','cold_fabric','device_reads_per_call',dict(status='completed'),fmt='.2f')+
        '; unsuccessful calls total '+summary('coldUnsuccessful','cold_fabric','unsuccessful_calls',dict(status='completed'),'sum','.0f')+
        '. Owner-verbs/RDMA-namespace mean ratios span '+span('verbsNamespace','cold_pairs','mean_ratio_a_over_b',dict(parity='matched',status_a='completed',status_b='completed',arm_a=COLD[1],arm_b=COLD[2:4]),fmt='.2f')+
        ': neither path consistently wins. Trusted-TCP/RDMA-namespace ratios span '+
        span('tcpNamespace1k16','cold_pairs','mean_ratio_a_over_b',dict(parity='matched',status_a='completed',status_b='completed',arm_a=COLD[0],arm_b=COLD[2:4],size_bytes='1024',readers='16'),fmt='.2f')+
        ' at 1 KiB c16 and '+span('tcpNamespace64k','cold_pairs','mean_ratio_a_over_b',dict(parity='matched',status_a='completed',status_b='completed',arm_a=COLD[0],arm_b=COLD[2:4],size_bytes='65536'),fmt='.2f')+
        ' at 64 KiB. At 64 KiB c16, owner-verbs/kernel-NVMe-TCP ratios are '+
        series('cold_pairs','mean_ratio_a_over_b',dict(arm_a=COLD[1],arm_b=COLD[4],size_bytes=65536,readers=16),'.2f')+
        '; trusted-owner-TCP/kernel-NVMe-TCP gives '+series('cold_pairs','mean_ratio_a_over_b',dict(arm_a=COLD[0],arm_b=COLD[4],size_bytes=65536,readers=16),'.2f')+
        ' (below one means owner RPC is faster). Low NVMe-oF owner-cgroup CPU omits kernel/SPDK target work, not evidence of free I/O.')
    findings['scopewarm']=('Run 9 used mutual TLS for owner TCP but trusted verbs. Run-10 mTLS/trusted TCP mean ratios (r0/r1) are '+
        ratios('TLS cost')+' at 1 KiB c1 and '+ratios('TLS cost',65536,1)+
        ' at 64 KiB c1: the earlier large-value TCP/verbs gap included an unmatched security cost, not just transport.')
    findings['scopecold']=('Cold cells: '+cold_summary+'. Owner-verbs/RDMA-namespace mean ratios span '+
        span('verbsNamespace','cold_pairs','mean_ratio_a_over_b',dict(parity='matched',status_a='completed',status_b='completed',arm_a=COLD[1],arm_b=COLD[2:4]),fmt='.2f')+
        '; neither path consistently wins. Kernel NVMe-oF/TCP is slowest at 64 KiB c16: owner-verbs/kernel-NVMe-TCP '+
        series('cold_pairs','mean_ratio_a_over_b',dict(arm_a=COLD[1],arm_b=COLD[4],size_bytes=65536,readers=16),'.2f')+' (r0/r1).')
    command=[r'\newcommand{\RWCtenFinding}[1]{\ifcsname rwctenfinding@#1\endcsname\csname rwctenfinding@#1\endcsname\else\errmessage{Missing run-10 finding #1}\fi}']
    for name,text in findings.items(): command.append(r'\expandafter\def\csname rwctenfinding@'+name+r'\endcsname{'+('SYNTHETIC fixture: ' if synthetic else 'EXCLUDED attempt-1 layout test: ' if excluded else '')+text+'}')
    write('rwc10-findings.tex','\n'.join(command))
    write('rwc10-numbers.tex','% Source-traced; no pooled repetitions.\n'+'\n'.join(r'\expandafter\gdef\csname n@\detokenize{'+key+r'}\endcsname{'+value+'}' for key,value in sorted(numbers.items())))
    provenance_text=(r'\textbf{'+('SYNTHETIC --- not launched. ' if synthetic else '')+'Run-10 provenance.} Parent '+r'\path{'+tex(provenance['parent_run_id'])+'}; fleet '+r'\path{'+tex(provenance['run_id'])+'}; package SHA-256 '+r'\path{'+provenance['package_sha256']+'}; source '+r'\path{'+tex(provenance['source_revision'])+'}; dirty '+str(provenance['source_dirty']).lower()+'. Launch '+tex(provenance['launch_time'])+'; VMs '+', '.join(r'\path{'+tex(v)+'}' for v in provenance['vm_names'])+'.')
    extra_hashes=[label+r' \path{'+provenance[key]+'}' for key,label in
                  [('manifest_sha256','manifest'),('binary_sha256','binary'),('source_tree_sha256','source tree'),('matrix_sha256','matrix')]
                  if provenance.get(key)]
    if extra_hashes: provenance_text += '\n\nSHA-256: '+'; '.join(extra_hashes)+'.'
    provenance_text += '\n\n'+r'\textbf{Excluded first attempt.} '+tex(provenance.get('excluded_attempt_description','The monitoring exporter required five owner rows for sealed SPDK target metrics. Its one-or-five-owner admission was fixed before attempt two. No attempt-one measurement is used in the final brief.'))
    for key,label in [('timing_description','Timing and derivation.'),('cleanup_description','Terminal outcome and cleanup.'),('cost_description','[INFERENCE] List-price estimate.')]:
        if provenance.get(key): provenance_text += '\n\n'+r'\textbf{'+label+'} '+tex(provenance[key])
    if excluded: provenance_text=r'\textbf{EXCLUDED --- aborted attempt, layout test only; not final results.} '+provenance_text
    write('rwc10-provenance.tex',provenance_text)
    figure_dir=('rwc10/'+mode+'-figures') if mode else 'figures'; prefix='rwc10-'+(mode+'-' if mode else '')
    write('rwc10-hooks.tex',r'\newcommand{\RWCtenFigure}[2][\linewidth]{\includegraphics[width=#1]{'+figure_dir+'/'+prefix+r'#2.pdf}}'+'\n'+
          r'\newcommand{\RWCtenTable}[1]{\input{'+str(generated.relative_to(brief))+r'/tab-rwc10-#1.tex}}'+'\n'+
          r'\newcommand{\RWCtenProvenance}{\input{'+str(generated.relative_to(brief))+r'/rwc10-provenance.tex}}')
    outputs={str(f.relative_to(brief)):sha(f) for f in [*files,*copied.iterdir(),generated/'provenance.json'] if f.is_file()}
    manifest=dict(synthetic=synthetic,data_directory=str(copied.relative_to(brief)),sources=sources,outputs=outputs,number_sources=definitions,provenance=str((generated/'provenance.json').relative_to(brief)))
    manifest.update(matrix_path=str(matrix_path.resolve()),report_path=str(report_path.resolve()),report_complete=raw_report.get('complete'))
    manifest['excluded_from_final']=excluded
    path=generated/'inputs-rwc10.json'; path.write_text(json.dumps(manifest,indent=2)+'\n')
    if args.register:
        old=json.loads((brief/'inputs.json').read_text())
        old['outputs']={k:v for k,v in old['outputs'].items() if not k.startswith('rwc10/') and not k.startswith('figures/rwc10-')}
        old['sources'].update(sources); old['outputs'].update(outputs); old['outputs'][str(path.relative_to(brief))]=sha(path)
        old['rwc10']=dict(synthetic=synthetic,manifest=str(path.relative_to(brief)))
        (brief/'inputs.json').write_text(json.dumps(old,indent=2)+'\n')
    print(f'{len(numbers)} traced numbers; six tables; '+str(generated))


if __name__=='__main__': main()
