#!/usr/bin/env python3
"""Render supplementary statistics from producer summaries; no quantiles recalculated.

Run from report root: python data/supplemental_charts.py
Outputs three seven-page vector PDFs. Page mapping is printed and stored alongside
figures. Each page isolates one workload and operation; successful outcomes only.
"""
import csv
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.lines import Line2D

root=Path.cwd(); rows=list(csv.DictReader((root/'data/all-results.csv').open()))
out=root/'figures/eval'; out.mkdir(parents=True,exist_ok=True)
styles={'native':('Native','#333333','o','-'),'tidehunter_tcp':('TH TCP','#0072B2','s','-'),'tidehunter_efa':('TH EFA','#56B4E9','^','--'),'tidehunter_efa_direct':('TH EFA direct','#009E73','D','-.'),'tidehunter_efa_native':('TH EFA native','#D55E00','v','-'),'tikv':('TiKV','#CC79A7','P','--'),'cassandra':('Cassandra','#8C6500','X',':'),'foundationdb':('FoundationDB','#7854A3','h','-.'),'valkey_tcp':('Valkey TCP','#C23B83','*','--')}
pages=[('write-concurrent','put','Scalar write (C16)'),('read-concurrent','read','Scalar read (C16)'),('read-many32','get_many','Batch32 read per call (C16)'),('point-mixed','read','Mixed: read (C1)'),('point-mixed','put','Mixed: put (C1)'),('point-mixed','delete','Mixed: delete (C1)'),('point-mixed','missing_read','Mixed: missing read (C1)')]
stats=[('mean','Mean'),('stddev','Within-case SD'),('min','Minimum'),('p50','p50'),('p95','p95'),('p99','p99'),('p999','p99.9'),('max','Maximum')]
sizes=[64,256,1024,8192,65536]; labels=['64 B','256 B','1 KiB','8 KiB','64 KiB']
for clock in ('api','scheduling','response'):
    with PdfPages(out/('supplemental-'+clock+'.pdf')) as pdf:
        for profile,operation,title in pages:
            fig,axes=plt.subplots(2,4,figsize=(10,5.5),sharex=True,layout='constrained')
            selected=[r for r in rows if r['cohort']=='primary_rf1' and r['profile']==profile and r['operation']==operation and r['outcome']=='success']
            for ax,(stat,label) in zip(axes.flat,stats):
                for backend,(name,color,marker,ls) in styles.items():
                    series={int(r['value_bytes']):r for r in selected if r['backend']==backend}
                    x=[]; y=[]
                    for i,size in enumerate(sizes):
                        row=series.get(size); value=row.get(clock+'_'+stat+'_us','') if row else ''
                        if value!='': x.append(i); y.append(float(value))
                    if x: ax.plot(x,y,color=color,marker=marker,linestyle=ls,markersize=3.5,linewidth=1.1)
                ax.set_title(label,fontsize=10); ax.set_yscale('symlog',linthresh=.001); ax.grid(True,which='major',alpha=.25)
                ax.set_xticks(range(5),labels,rotation=30,fontsize=8); ax.tick_params(axis='y',labelsize=8); ax.set_ylabel('µs (symlog)',fontsize=9)
            handles=[Line2D([0],[0],color=c,marker=m,linestyle=ls,label=n,markersize=4,linewidth=1.1) for n,c,m,ls in styles.values()]
            fig.legend(handles=handles,loc='outside lower center',ncol=5,fontsize=9)
            fig.suptitle(title+' — '+clock.upper()+' clock; successful calls only\nOne independent repetition; p99 below 10,000 and p99.9 below 100,000 calls are descriptive',fontsize=11)
            pdf.savefig(fig); plt.close(fig)
    print(out/('supplemental-'+clock+'.pdf'))
(out/'supplemental-pages.txt').write_text('Each clock PDF: primary RF1, successful populations, value bytes 64, 256, 1024, 8192, 65536.\n'+''.join(f'{i}. {title}: profile={profile}, operation={operation}\n' for i,(profile,operation,title) in enumerate(pages,1))+'Each page: mean, within-case SD, min, p50, p95, p99, p99.9, max; µs; symmetric-log axes preserve zero. The cohort/case counts and exact values are in the tables and data/all-results.csv. Tail quantiles are descriptive; no error bars or pooling. get_many values are per 32-key call.\n')
