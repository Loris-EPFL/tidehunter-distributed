#!/usr/bin/env python3
"""Render the historical 1 KiB write chart using only the report CSV snapshot."""
import csv,math
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
root=Path.cwd(); rows=list(csv.DictReader((root/'data/historical-comparison.csv').open()))
styles={'native':('Native','#333333','o'),'tidehunter_tcp':('TCP','#0072B2','s'),'tidehunter_efa':('EFA','#56B4E9','^'),'tidehunter_efa_direct':('Direct','#009E73','D'),'tidehunter_efa_native':('EFAnative','#D55E00','v'),'tikv':('TiKV','#CC79A7','P'),'cassandra':('Cassandra','#8C6500','X'),'foundationdb':('FDB','#7854A3','h'),'valkey_tcp':('Valkey','#C23B83','*')}
fig,ax=plt.subplots(figsize=(6.5,5.5),layout='constrained')
for backend,(label,color,marker) in styles.items():
    selected=[r for r in rows if r['backend']==backend and r['cohort']=='primary_rf1' and r['profile']=='write-concurrent' and r['value_bytes']=='1024' and r['operation']=='put' and r['outcome']=='success']
    if len(selected)==1:
        r=selected[0]; ys=[float(r[k])/1000 if r[k] else math.nan for k in ('previous_api_p99_us','current_api_p99_us')]
        ax.plot([0,1],ys,color=color,marker=marker,label=label,linewidth=1.5,markersize=5)
ax.set_xticks([0,1],['27 Oct: Frankfurt','29 Oct: N. Virginia']); ax.set_ylabel('API write p99 (ms, logarithmic)'); ax.set_yscale('log'); ax.grid(True,which='both',alpha=.25); ax.legend(ncol=3,fontsize=9); ax.set_title('1 KiB writes, C16: historical, not code-only A/B',fontsize=10)
fig.savefig(root/'figures/eval/historical-write-p99.pdf'); plt.close(fig)
