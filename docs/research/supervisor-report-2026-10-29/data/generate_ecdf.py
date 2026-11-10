#!/usr/bin/env python3
"""Render representative per-case empirical CDFs; never pool case populations.

From report root: python data/generate_ecdf.py [CAMPAIGN_ROOT]
With CAMPAIGN_ROOT, extract only successful measurement-phase dispatched raw API
samples for the specified cases into a small snapshot and record raw SHA-256.
Without it, rerender using only the included snapshot. No benchmarks are run.
"""
import csv,sys,json,hashlib
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

root=Path.cwd(); data=root/'data'; out=root/'figures/eval'; snapshot=data/'ecdf-samples.csv'; index=list(csv.DictReader((data/'all-results.csv').open()))
styles={'native':('Native','#333333','o','-'),'tidehunter_tcp':('TH TCP','#0072B2','s','-'),'tidehunter_efa':('TH EFA','#56B4E9','^','--'),'tidehunter_efa_direct':('TH EFA direct','#009E73','D','-.'),'tidehunter_efa_native':('TH EFA native','#D55E00','v','-')}
profiles={'read-concurrent':'read','write-concurrent':'put'}
if len(sys.argv)>1:
    campaign=Path(sys.argv[1]); records=[]; metadata=[]
    for profile,operation in profiles.items():
        for backend in styles:
            selected=[r for r in index if r['backend']==backend and r['cohort']=='primary_rf1' and r['profile']==profile and r['value_bytes']=='1024' and r['operation']==operation and r['outcome']=='success']
            if len(selected)!=1: raise ValueError((backend,profile,'unexpected selection count'))
            expected=selected[0]; cid=expected['case_id']; path=campaign/'aws-efa/validated-results/cases'/cid/'result/samples.csv'; n=0
            with path.open(newline='') as f:
                for row in csv.DictReader(f):
                    if row['phase']=='measurement' and row['operation']==operation and row['outcome']=='success' and row['dispatch_observed']=='true':
                        records.append((backend,profile,row['api_ns'])); n+=1
            if n!=int(expected['api_sample_count']): raise ValueError((cid,n,expected['api_sample_count']))
            metadata.append({'case_id':cid,'measurement_success_dispatched_rows':n,'source':str(path.relative_to(campaign)),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
    with snapshot.open('w',newline='') as f:
        w=csv.writer(f); w.writerow(['backend','profile','api_ns']); w.writerows(records)
    (data/'ecdf-source-hashes.json').write_text(json.dumps({'algorithm':'sha256','phase':'measurement','outcome':'success','dispatch_observed':True,'operation_by_profile':profiles,'cases':metadata},indent=2)+'\n')
records=list(csv.DictReader(snapshot.open()))
for profile,operation in profiles.items():
    fig,ax=plt.subplots(figsize=(6.5,4.8),layout='constrained')
    for backend,(name,color,marker,line) in styles.items():
        values=sorted(int(r['api_ns'])/1000 for r in records if r['backend']==backend and r['profile']==profile); n=len(values)
        if not n: raise ValueError((backend,profile,'no samples'))
        ax.step(values,[(i+1)/n for i in range(n)],where='post',color=color,marker=marker,markevery=max(1,n//8),markersize=3,linestyle=line,linewidth=1.5,label=f'{name} (n={n})')
    ax.set_xscale('symlog',linthresh=.001); ax.set_ylim(0,1); ax.grid(True,which='major',alpha=.25); ax.set_xlabel('API latency (µs, symmetric log)'); ax.set_ylabel('Fraction of calls at or below latency'); ax.set_title('1 KiB '+('reads' if operation=='read' else 'writes')+', C16; successful measurement calls',fontsize=10); ax.legend(fontsize=9,loc='lower right'); fig.savefig(out/('ecdf-'+operation+'-1k.pdf')); plt.close(fig)
print('Wrote ecdf-read-1k.pdf and ecdf-put-1k.pdf from filtered per-case samples.')
