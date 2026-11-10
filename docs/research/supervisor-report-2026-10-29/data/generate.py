#!/usr/bin/env python3
"""Generate offline report data, table text and historical figure from retained exports.

Usage: python data/generate.py /path/to/2026-10-29-run-40848651463
Run from the report root. Install matplotlib for the additional historical PDF.
The *.fragment.txt outputs are LaTeX include bodies; import each into tables/*.tex
using the LaTeX MCP, preserving the bodies verbatim and removing document wrappers.
No benchmark is run; no producer percentile is recomputed for publication.
"""
import csv, json, sys, hashlib, shutil, collections, math
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

root=Path.cwd(); source=Path(sys.argv[1]); data=root/'data'; figures=root/'figures/eval'; tables=root/'tables'
for p in (data, figures, tables): p.mkdir(parents=True, exist_ok=True)
a=source/'analysis'; comparison=source/'aws-efa/validated-results/comparison.json'
for name in ('all-results.csv','historical-comparison.csv','summary.json'):
    shutil.copy2(a/name,data/name)
shutil.copy2(comparison,data/'comparison.json')
for name in ('plots.json','plot-data.csv'): shutil.copy2(a/'figures'/name,data/name)
rows=list(csv.DictReader((data/'all-results.csv').open())); hist=list(csv.DictReader((data/'historical-comparison.csv').open())); document=json.loads((data/'comparison.json').read_text())
cases={r['case_id']:r for r in rows}; assert len(cases)==258 and len(rows)==453 and len(hist)==463

def esc(s):
    s=str(s); return ''.join({'\\':r'\textbackslash{}','_':r'\_\allowbreak{}','%':r'\%','&':r'\&','#':r'\#','{':r'\{','}':r'\}','$':r'\$'}.get(c,c) for c in s)
def num(x):
    if x is None or x=='': return '--'
    try:
        x=float(x)
        return format(x,'.5g')
    except ValueError: return esc(x)
def cid(x): return 'C'+x[:4]
backend_code={'native':'Native','tidehunter_tcp':'TCP','tidehunter_efa':'EFA','tidehunter_efa_direct':'Direct','tidehunter_efa_native':'EFAnative','tikv':'TiKV','cassandra':'Cassandra','foundationdb':'FDB','valkey_tcp':'Valkey'}
cohort_code={'primary_rf1':'P','cold_os_cache':'Cold','coordinator_cache_configured':'Coord','owner_cache_configured':'Owner','registered_ram_configured':'RMA','native_pagecache':'Page','write_stage_diagnostics':'Diag'}
profile_code={'write-concurrent':'Write','read-concurrent':'Read','read-many32':'Batch32','point-mixed':'Mixed','zipf-read':'Zipf','offered-1000':'Load1k','offered-10000':'Load10k','offered-50000':'Load50k','cold-os-large':'Cold','coordinator-cache':'Coord','owner-cache':'Owner','registered-ram':'RMA'}
def fragment(name, text): (data/(name+'.fragment.txt')).write_text(text+'\n')
def table(header,fmt,title,label,body):
    return '\n'.join([r'\begin{landscape}',r'\scriptsize',r'\setlength{\tabcolsep}{2pt}',r'\sloppy',r'\begin{longtable}{'+fmt+'}',r'\caption{'+title+r'}\label{data-'+label+r'}\\',r'\toprule',header+r'\\',r'\midrule',r'\endfirsthead',r'\toprule',header+r'\\',r'\midrule',r'\endhead',r'\bottomrule',r'\endfoot',*body,r'\end{longtable}',r'\normalsize',r'\end{landscape}'])

# One row per case, no replicated whole-case rate across populations.
body=[]
for r in sorted(cases.values(),key=lambda x:x['case_id']):
    fields=[cid(r['case_id']),backend_code[r['backend']],cohort_code[r['cohort']],profile_code[r['profile']],r['value_bytes'],r['keys'],r['concurrency'],num(r['offered_ops_per_second']),r['observed_calls'],r['successful_calls'],r['unsuccessful_calls'],num(r['elapsed_seconds']),num(r['case_success_calls_per_second']),num(r['case_success_logical_keys_per_second']),num(r['case_success_value_mib_per_second'])]
    body.append(' & '.join(fields)+r'\\')
fragment('cases',table(r'ID & Backend & Cohort & Profile & B & Keys & C & Offered/s & Calls & OK & Rej. & s & OK/s & Keys/s & MiB/s',r'llllrrrrrrrrrrr','Case index and whole-case throughput. Backend abbreviations: TCP/EFA/Direct/EFAnative denote Tidehunter variants; FDB denotes FoundationDB; Valkey denotes Valkey TCP. Cohorts P, Cold, Coord, Owner, RMA, Page and Diag denote primary RF1, cold OS cache, coordinator cache, owner cache, registered RAM, native page cache and write diagnostics respectively. Profiles Write, Read, Batch32, Mixed, Zipf and Load denote concurrent write, concurrent read, 32-key read, point mixed, Zipf read and offered-load cases; Cold, Coord, Owner and RMA denote the matching cohort profiles. C denotes concurrency; offered rate is blank for closed-loop cases. MiB/s is successful value bandwidth. Full case IDs and unrounded precision are in data/all-results.csv.','cases',body))

body=[]
for r in sorted(rows,key=lambda x:(x['case_id'],x['operation'],x['outcome'])):
    for clock in ('api','scheduling','response'):
        values=[cid(r['case_id']),esc(r['operation']),esc(r['outcome']),clock,r['population_calls'],r['population_logical_keys'],r[clock+'_sample_count']]+[num(r[clock+'_'+f+'_us']) for f in ('mean','stddev','min','p50','p95','p99','p999','max')]
        body.append(' & '.join(values)+r'\\')
fragment('latencies',table(r'ID & Operation & Outcome & Clock & Pop. & Keys & $n$ & Mean & SD & Min & p50 & p95 & p99 & p99.9 & Max',r'llllrrrrrrrrrrr','All operation/outcome populations and clock summaries in microseconds; each population has three rows. n is clock sample count; -- means unobserved, not zero. SD is within-case. Tails are descriptive for n below 10,000 (p99) or 100,000 (p99.9); producer quantiles are retained. Full precision is in data/all-results.csv.','latencies',body))

groups=collections.defaultdict(list)
for r in cases.values(): groups[(r['cohort'],r['profile'],r['concurrency'],r['offered_ops_per_second'])].append(r)
body=[]
for (cohort,profile,concurrency,offered), rs in sorted(groups.items()):
    sizes=', '.join(map(str,sorted({int(r['value_bytes']) for r in rs})))
    backs=', '.join(backend_code[b] for b in sorted({r['backend'] for r in rs}))
    body.append(' & '.join([esc(cohort),esc(profile),esc(sizes),concurrency,num(offered),str(len(rs)),backs])+r'\\')
fragment('matrix',table(r'Cohort & Profile & Value bytes & C & Offered/s & Cases & Backends',r'p{3cm}p{2.5cm}p{3.1cm}rrrp{7cm}','Exact case matrix: 258 cases, one repetition each; key counts are in the case index. TCP/EFA/Direct/EFAnative denote Tidehunter variants; FDB denotes FoundationDB; Valkey denotes Valkey TCP. Backend names and values refer to observed cases, not a claim of every possible combination.','matrix',body))

contracts={}
for c in document['cases']:
    if c['job']['cohort']=='primary_rf1': contracts.setdefault(c['job']['backend'], c['backend'])
(data/'backend-contracts.json').write_text(json.dumps(contracts,indent=2,sort_keys=True)+'\n')
body=[]
for name,b in sorted(contracts.items()):
    body.append(' & '.join(esc(s) for s in (backend_code[name],b['acknowledgement'],b['consistency'],b['get_many_strategy'],b['batch_atomicity']))+r'\\')
fragment('backend-contracts',table(r'Backend & Acknowledgement & Consistency & get-many implementation & Batch atomicity',r'p{2cm}p{5cm}p{4.4cm}p{5cm}p{2cm}','Observed primary RF1 backend contracts from comparison.json; device persistence and security settings are not inferred. TCP/EFA/Direct/EFAnative denote Tidehunter variants; FDB denotes FoundationDB; Valkey denotes Valkey TCP. Atomicity describes adapter metadata, not a measured batch-write benchmark. Full configuration and version evidence are in data/backend-contracts.json and data/comparison.json.','backend-contracts',body))

# One population and workload: historical p99 trends; never interpolate missing.
styles={'native':('#333333','o'),'tidehunter_tcp':('#0072B2','s'),'tidehunter_efa':('#56B4E9','^'),'tidehunter_efa_direct':('#009E73','D'),'tidehunter_efa_native':('#D55E00','v'),'tikv':('#CC79A7','P'),'cassandra':('#8C6500','X'),'foundationdb':('#7854A3','h'),'valkey_tcp':('#C23B83','*')}
fig,ax=plt.subplots(figsize=(6.5,5.5),layout='constrained')
for b,(color,marker) in styles.items():
    matched=[r for r in hist if r['backend']==b and r['cohort']=='primary_rf1' and r['profile']=='write-concurrent' and r['value_bytes']=='1024' and r['operation']=='put' and r['outcome']=='success']
    if len(matched)==1:
        r=matched[0]; y=[float(r[k])/1000 if r[k] else math.nan for k in ('previous_api_p99_us','current_api_p99_us')]
        ax.plot([0,1],y,color=color,marker=marker,label=backend_code[b],linewidth=1.5,markersize=5)
ax.set_xticks([0,1],['27 Oct: Frankfurt','29 Oct: N. Virginia']); ax.set_ylabel('API write p99 (ms, logarithmic)'); ax.set_yscale('log'); ax.grid(True,which='both',alpha=.25); ax.legend(ncol=3,fontsize=9); ax.set_title('1 KiB writes, C16: historical, not code-only A/B',fontsize=10)
fig.savefig(figures/'historical-write-p99.pdf'); plt.close(fig)

# Bind retained source and copied snapshots, without copying raw sample files.
hashes={}
paths=[a/'all-results.csv',a/'historical-comparison.csv',a/'summary.json',comparison,a/'figures/plots.json',a/'figures/plot-data.csv']+sorted((a/'figures').glob('*.pdf'))
for p in paths: hashes[str(p.relative_to(source))]=hashlib.sha256(p.read_bytes()).hexdigest()
repo=root.parents[2]
extra={'historical-all-results.csv':repo/'docs/distributed/reports/2026-10-27-kv-comparison-ci-40573961972/all-results.csv',
       'plot_cloud_comparison.py':repo/'experiments/kv-comparison/plot_cloud_comparison.py',
       'analyze.py':repo/'experiments/kv-comparison/analyze.py',
       'local.py':repo/'experiments/kv-comparison/local.py'}
additional={}
for name,p in extra.items():
    shutil.copy2(p,data/name)
    additional[str(p)]=hashlib.sha256(p.read_bytes()).hexdigest()
audit=data/'audit.json'
raw={x['source']:x['sha256'] for x in json.loads(audit.read_text())['raw_audit']} if audit.exists() else {}
(data/'source-hashes.json').write_text(json.dumps({'algorithm':'sha256','source_root':str(source),'files':hashes,'additional_sources':additional,'raw_sample_sources':raw},indent=2,sort_keys=True)+'\n')
print('Generated',len(cases),'cases,',len(rows),'populations and',len(hist),'historical union populations.')
