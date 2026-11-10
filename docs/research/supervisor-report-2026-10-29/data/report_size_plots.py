#!/usr/bin/env python3
"""Split retained chart panels into 6.5-inch-wide vector PDF pages.

Run at report root: python data/report_size_plots.py
The original manifest is authoritative for matched cells, missing observations,
units and captions; this renderer never recomputes summaries or adds error bars.
"""
import json, math
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.lines import Line2D

root=Path.cwd(); manifest=json.loads((root/'data/plots.json').read_text()); output=root/'figures/eval/panels'; output.mkdir(parents=True,exist_ok=True)
styles={'native':('Native','#333333','o','-'),'tidehunter_tcp':('TH TCP','#0072B2','s','-'),'tidehunter_efa':('TH EFA','#56B4E9','^','--'),'tidehunter_efa_direct':('TH EFA direct','#009E73','D','-.'),'tidehunter_efa_native':('TH EFA native','#D55E00','v','-'),'tikv':('TiKV','#CC79A7','P','--'),'cassandra':('Cassandra','#8C6500','X',':'),'foundationdb':('FoundationDB','#7854A3','h','-.'),'valkey_tcp':('Valkey TCP','#C23B83','*','--')}
timings={'api_ns','response_ns','scheduling_ns'}
units={'success_calls_per_second':'Successful API calls/s','success_logical_keys_per_second':'Successful logical keys/s','success_value_mib_per_second':'Successful value MiB/s','rejection_fraction':'Rejected / offered arrivals'}
plt.rcParams.update({'font.size':9,'axes.titlesize':10,'axes.labelsize':9,'xtick.labelsize':9,'ytick.labelsize':9,'legend.fontsize':9,'pdf.fonttype':42})
index=['Portrait panel PDFs: 6.5 inches wide, one vector page per retained panel.','Source: data/plots.json. Curves are producer statistics; no run-to-run bars or pooling.','Caption correction: the retained plotter incorrectly describes Valkey get-many; case metadata and the measured Rust adapter establish redis-rs MGET split by slot and ordered reassembly. The data and original source manifest are unchanged.']
stale='Valkey get-many uses ordered bounded per-key GET routing, not cross-slot MGET; no atomic PUT-batch support.'
correct='Valkey get-many issues one MGET through redis-rs, which splits by slot and reassembles ordered results; no atomic PUT-batch support.'
for figure in manifest['figures']:
    name=figure['name']; index.append('\n'+name+'.pdf'); index.append('  Context: '+figure['title'].replace('\n','; ')); index.append('  Caption: '+figure['caption'].replace(stale,correct))
    with PdfPages(output/(name+'.pdf')) as pdf:
        for number,item in enumerate(figure['panels'],1):
            index.append(f'  Page {number}: {item["title"]} (metric={item["metric"]}, operation={item.get("operation")}, outcome={item.get("outcome")})')
            fig,ax=plt.subplots(figsize=(6.5,5.6),layout='constrained'); ax.set_title(figure['title'].split('\n')[0]+'\n'+item['title'])
            ax.set_ylabel('Latency (µs per API call)' if item['metric'] in timings else units[item['metric']])
            handles={}; positive=zero=False
            if item['status']=='unavailable': ax.text(.5,.5,'Unavailable: no matched timing population',ha='center',transform=ax.transAxes)
            for series in item['series']:
                _,color,marker,line=styles[series['backend']]; variant=series.get('variant')
                if variant is not None: line=('-','--','-.',':')[variant]; marker=('o','s','^','D')[variant]
                points=series['points']; scale=.001 if item['metric'] in timings else 1
                x=[p['x'] for p in points]; y=[p['median']*scale for p in points]; positive|=any(v>0 for v in y); zero|=any(p['observed_min']==0 for p in points)
                missing={e['x'] for e in item['unavailable'] if e['cell'][0]==series['backend']}
                if missing:
                    lookup=dict(zip(x,y)); xx=sorted(set(x)|missing); ax.plot(xx,[lookup.get(v,math.nan) for v in xx],color=color,linestyle=line,linewidth=1.2)
                    line='none'
                ax.plot(x,y,color=color,marker=marker,linestyle='none' if figure['xkind']=='backend' else line,markersize=5,linewidth=1.2)
                handles[series['label']]=Line2D([],[],color=color,marker=marker,linestyle=line,linewidth=1.2,markersize=5)
            if item['metric']=='rejection_fraction': ax.set_ylim(-.02,1.02)
            elif positive and not zero: ax.set_yscale('log')
            elif positive: ax.set_yscale('symlog',linthresh=1)
            if figure['xkind']=='size':
                ax.set_xscale('log',base=2); ticks=sorted({p['x'] for s in item['series'] for p in s['points']}); ax.set_xticks(ticks,[str(v) for v in ticks]); ax.set_xlabel('Value size (bytes)')
            elif figure['xkind']=='offered': ax.set_xscale('log'); ax.set_xticks([1000,10000,50000],['1k','10k','50k']); ax.set_xlabel('Configured offered API calls/s')
            else:
                ax.set_xticks(range(len(figure['xticks'])),figure['xticks'],rotation=40 if figure['xkind']=='backend' else 0,ha='right' if figure['xkind']=='backend' else 'center')
            ax.grid(True,which='major',alpha=.25)
            if handles: fig.legend(list(handles.values()),list(handles.keys()),loc='outside lower center',ncol=min(3,len(handles)),fontsize=9)
            pdf.savefig(fig); plt.close(fig)
coverage=manifest['coverage']; index.append('\ncoverage.pdf\n  Page 1: case status by backend.\n  Page 2: case status by cohort.\n  Caption: '+coverage['caption'])
with PdfPages(output/'coverage.pdf') as pdf:
    for key,title in [('by_backend','Case status by backend'),('by_cohort','Case status by cohort')]:
        fig,ax=plt.subplots(figsize=(6.5,5.5),layout='constrained'); names=list(coverage[key]); bottom=[0]*len(names)
        for status,color in [('completed','#009E73'),('failed','#D55E00'),('not_run','#999999')]:
            counts=[coverage[key][n].get(status,0) for n in names]; ax.bar(range(len(names)),counts,bottom=bottom,color=color,label=status); bottom=[a+b for a,b in zip(bottom,counts)]
        labels=[styles[n][0] if key=='by_backend' else n.replace('_',' ') for n in names]; ax.set_xticks(range(len(names)),labels,rotation=40,ha='right'); ax.set_ylabel('Planned cases'); ax.set_title(title); fig.legend(loc='outside lower center',ncol=3); pdf.savefig(fig); plt.close(fig)
(output/'page-index.txt').write_text('\n'.join(index)+'\n')
print('Wrote',len(manifest['figures'])+1,'report-sized multipage vector PDFs and page-index.txt')
