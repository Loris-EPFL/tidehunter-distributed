#!/usr/bin/env python3
"""Make final-campaign vector figures and a point-level evidence manifest."""
import csv, json, hashlib
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent
WORK = ROOT / 'checkpoint-work/spdk-matched-storage-20261105T004442Z/final-full-report'
OLD = ROOT / 'checkpoint-work/gcp-kv-comparison-2026-11-03-b35a62e24b47faaf/metrics-all-populations.csv'
CSV = WORK / 'all-populations.csv'
LONG = WORK / 'longitudinal-347.csv'
def rows(p):
    with p.open(newline='') as f: return list(csv.DictReader(f))
def sha(p):
    h = hashlib.sha256()
    with p.open('rb') as f:
        for chunk in iter(lambda: f.read(1024*1024), b''): h.update(chunk)
    return h.hexdigest()
data, old, longitudinal = rows(CSV), rows(OLD), rows(LONG)
labels = {'native':'Native', 'tidehunter_tcp':'TH TCP', 'tidehunter_verbs_native':'TH verbs', 'kernel':'TH NVMe kernel', 'spdk':'TH NVMe SPDK', 'valkey_tcp':'Valkey TCP', 'valkey_rdma':'Valkey RDMA', 'tikv':'TiKV','cassandra':'Cassandra','foundationdb':'FoundationDB','kvrocks':'Kvrocks','garnet':'Garnet'}
colors = dict(zip(labels, ['#111111','#0072b2','#56b4e9','#d55e00','#cc79a7','#009e73','#006442','#e69f00','#882255','#44aa99','#aa4499','#999933']))
order = list(labels)
def key(r): return r['target'] if r['backend']=='tidehunter_nvme_rdma' else r['backend']
def pick(cohort, profile, sizes=None):
    return [r for r in data if r['cohort']==cohort and r['profile']==profile and r['outcome']=='success' and (sizes is None or int(r['value_size']) in sizes)]
manifest = {'sources': {str(p):sha(p) for p in (CSV,LONG,OLD,WORK/'input/comparison.json')}, 'run_id':'bench-ce0007878bef48e8','independent_repetitions':1,'figures':{}}
plt.rcParams.update({'font.size':8,'axes.titlesize':9,'axes.labelsize':8,'legend.fontsize':7,'pdf.fonttype':42})
def save(fig,name,points):
    fig.savefig(HERE/'figures'/name, bbox_inches='tight'); plt.close(fig)
    manifest['figures'][name]={'sha256':sha(HERE/'figures'/name),'points':points}
def lines(name, title, points, sizes=(1024,8192,65536), note=''):
    fig, ax=plt.subplots(1,3,figsize=(9,3.2))
    fields=[('api_p50_ns','API p50 (µs)',.001),('api_p99_ns','API p99 (µs)',.001),('success_calls_per_second','Successful calls/s',1)]
    for k in order:
        group=sorted([r for r in points if key(r)==k and int(r['value_size']) in sizes],key=lambda r:int(r['value_size']))
        if not group: continue
        for a,(f,t,m) in zip(ax,fields):
            a.plot([int(r['value_size']) for r in group],[float(r[f])*m for r in group],marker='o',lw=1.2,ms=3,label=labels[k],color=colors[k])
    for a,(_,t,_) in zip(ax,fields):
        a.set_xscale('log',base=2); a.set_yscale('log'); a.set_xticks(sizes,[f'{s//1024}K' if s>=1024 else str(s) for s in sizes]); a.set_xlabel('Value bytes'); a.set_title(t); a.grid(alpha=.2)
    handles, labs=ax[0].get_legend_handles_labels(); fig.legend(handles,labs,loc='lower center',bbox_to_anchor=(.5,-.10),ncol=4,frameon=False)
    fig.suptitle(title+' · one repetition',y=1.02); fig.tight_layout(); save(fig,name,points)
for profile,name,title in [('read-concurrent','read.pdf','Primary RF1 scalar reads, c16'),('write-concurrent','write.pdf','Primary RF1 scalar writes, c16'),('read-many32','read32.pdf','Primary RF1 read-many32, c16'),('write-batch32','write32.pdf','Primary RF1 write-batch32, c16')]:
    lines(name,title,pick('primary_rf1',profile,{1024,8192,65536}))
# Eight sealed populations, kept separate; each subplot uses a distinct workload.
sealed=[r for r in data if r['cohort']=='sealed_read' and r['outcome']=='success' and r['value_size']=='1024' and r['profile'].endswith('-baseline')]
fig,axes=plt.subplots(2,4,figsize=(10,5),sharey=True)
for i,shape in enumerate(('scalar','batch32')):
 for j,(mode,c) in enumerate((('first_touch','c1'),('repeat_read','c1'),('first_touch','c16'),('repeat_read','c16'))):
    ax=axes[i,j]; prof=f'{mode}-{shape}-{c}-baseline'; sub=[r for r in sealed if r['profile']==prof]
    for n,k in enumerate(['native','tidehunter_tcp','tidehunter_verbs_native','kernel','spdk']):
        rr=[r for r in sub if key(r)==k]
        if rr: ax.plot([n],[int(rr[0]['api_p50_ns'])/1000],marker='o',color=colors[k]); ax.plot([n],[int(rr[0]['api_p99_ns'])/1000],marker='x',color=colors[k])
    ax.set_xticks(range(5),['N','TCP','verbs','kernel','SPDK'],rotation=45); ax.set_yscale('log'); ax.grid(alpha=.2); ax.set_title(f'{shape} {mode.replace("_"," ")} {c}'); ax.set_ylabel('API µs: ● p50, × p99')
fig.suptitle('Sealed 1 KiB reads · one repetition'); fig.tight_layout(); save(fig,'sealed.pdf',sealed)
# Cold single-size comparisons.
cold=pick('cold_os_cache','cold-os-large',{65536})
fig,ax=plt.subplots(figsize=(8,3));
for i,r in enumerate(sorted(cold,key=lambda x:order.index(key(x)))):
 k=key(r); ax.plot(i,int(r['api_p50_ns'])/1000,'o',color=colors[k]); ax.plot(i,int(r['api_p99_ns'])/1000,'x',color=colors[k])
ax.set_xticks(range(len(cold)),[labels[key(r)] for r in sorted(cold,key=lambda x:order.index(key(x)))],rotation=25,ha='right'); ax.set_yscale('log'); ax.set_ylabel('API µs: ● p50, × p99'); ax.set_title('64 KiB cold-OS reads, c16 · one repetition'); ax.grid(alpha=.2); fig.tight_layout(); save(fig,'cold.pdf',cold)
# Separate cohorts, not a paired speedup.
cache=pick('coordinator_cache_configured','coordinator-cache',{256,8192}); remote=pick('primary_rf1','read-concurrent',{256,8192});
fig,axes=plt.subplots(1,2,figsize=(9,3),sharey=True)
for ax,size in zip(axes,(256,8192)):
 sub=[r for r in remote+cache if int(r['value_size'])==size]; sub.sort(key=lambda r:(order.index(key(r)),r['cohort']))
 for i,r in enumerate(sub):
  k=key(r); ax.plot(i,int(r['api_p50_ns'])/1000,marker='s' if r['cohort']=='coordinator_cache_configured' else 'o',color=colors[k]); ax.plot(i,int(r['api_p99_ns'])/1000,'x',color=colors[k])
 ax.set_xticks(range(len(sub)),[labels[key(r)]+(' cache' if r['cohort']=='coordinator_cache_configured' else ' remote' if key(r)!='native' else '') for r in sub],rotation=55,ha='right',fontsize=6); ax.set_yscale('log'); ax.set_title(f'{size} bytes'); ax.grid(alpha=.2)
axes[0].set_ylabel('API µs: ●/■ p50, × p99'); fig.suptitle('Separate configured-cache and primary cases · one repetition'); fig.tight_layout(); save(fig,'cache.pdf',remote+cache)
# Offered load count, one case per bar; don't sum repeated population-level case totals.
offer=[r for r in data if r['cohort']=='primary_rf1' and r['profile'].startswith('offered-')]; unique={r['case_id']:r for r in offer}
fig,axes=plt.subplots(1,3,figsize=(10,3))
for ax,rate in zip(axes,('1000','10000','50000')):
 sub=sorted([r for r in unique.values() if r['profile']=='offered-'+rate],key=lambda r:order.index(key(r)))
 ax.bar(range(len(sub)),[int(r['success_calls']) for r in sub],color=[colors[key(r)] for r in sub]); ax.bar(range(len(sub)),[int(r['unsuccessful_calls']) for r in sub],bottom=[int(r['success_calls']) for r in sub],color='#bb2222',hatch='//')
 ax.set_xticks(range(len(sub)),[labels[key(r)] for r in sub],rotation=55,ha='right',fontsize=6); ax.set_title(f'Offered {rate}/s'); ax.set_ylabel('Calls: color success, hatched rejected')
fig.suptitle('1 KiB mixed offered-load accounting · one repetition'); fig.tight_layout(); save(fig,'offered.pdf',offer)
# Plot every successful, matched population separately; no percentile pooling.
long=[r for r in longitudinal if r['outcome']=='success' and r['historical_api_p50_ns'] and r['current_api_p50_ns'] and float(r['historical_api_p50_ns'])>0]
def longitudinal_key(r):
    backend = json.loads(r['join_key_json'])['backend']
    if backend != 'tidehunter_nvme_rdma':
        return backend
    return 'spdk' if 'tidehunter_spdk' in r['current_case_id'] else 'kernel'
fig,ax=plt.subplots(figsize=(8,3.2))
for k in order:
 sub=[]
 for r in long:
  kk=longitudinal_key(r)
  if kk==k: sub.append(r)
 if sub:
  ax.scatter([order.index(k)]*len(sub),[float(r['current_api_p50_ns'])/float(r['historical_api_p50_ns']) for r in sub],s=9,alpha=.55,color=colors[k])
active=[k for k in order if any(longitudinal_key(r)==k for r in long)]
ax.set_xticks([order.index(k) for k in active],[labels[k] for k in active],rotation=30,ha='right'); ax.set_yscale('log'); ax.axhline(1,color='black',lw=.7); ax.set_ylabel('New / old API p50'); ax.set_title('Matched 347 jobs · binary and live queue policy differ'); ax.grid(alpha=.2); fig.tight_layout(); save(fig,'longitudinal.pdf',long)
# Reference data are exclusively from the Nov 3 comparison.
ref=[r for r in old if r['backend'] in ('tikv','cassandra','foundationdb','kvrocks','garnet') and r['cohort']=='primary_rf1' and r['profile']=='read-concurrent' and r['outcome']=='success' and r['timing']=='api' and int(r['value_bytes']) in (1024,8192,65536)]
fig,axes=plt.subplots(1,3,figsize=(9,3))
for k in ('tikv','cassandra','foundationdb','kvrocks','garnet'):
 sub=sorted([r for r in ref if r['backend']==k],key=lambda r:int(r['value_bytes']))
 for ax,f,m in zip(axes,('p50','p99','case_success_calls_per_second'),(.001,.001,1)):
  ax.plot([int(r['value_bytes']) for r in sub],[float(r[f])*m for r in sub],label=labels[k],color=colors[k],marker='o')
for ax,title in zip(axes,('API p50 µs','API p99 µs','Successful calls/s')):
 ax.set_xscale('log',base=2); ax.set_yscale('log'); ax.set_xticks([1024,8192,65536],['1K','8K','64K']); ax.set_xlabel('Value bytes'); ax.set_title(title); ax.grid(alpha=.2)
fig.legend(*axes[0].get_legend_handles_labels(),loc='lower center',bbox_to_anchor=(.5,-.12),ncol=5,frameon=False); fig.suptitle('Nov 3: different run / binary / queue policy; NOT matched with final',fontsize=9); fig.tight_layout(); save(fig,'historical-reference.pdf',ref)
# Exact 1 KiB table rows, including absence of Valkey write-batch cases.
manifest['table_1k']=[r for r in data if r['cohort']=='primary_rf1' and r['profile'] in ('read-concurrent','write-concurrent','read-many32','write-batch32') and r['value_size']=='1024' and r['outcome']=='success']
(HERE/'evidence.json').write_text(json.dumps(manifest,indent=2)+'\n')
