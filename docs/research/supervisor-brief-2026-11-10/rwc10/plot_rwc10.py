#!/usr/bin/env python3
"""Plot run-10 cases separately; cross-fleet floors are context, never residuals."""
import argparse
import json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import MaxNLocator
from rwc10_data import WARM, COLD, CELLS, COMPARISONS, CODES, LABELS, PAIR_LABELS, COLORS, CONTRACT, DEFAULT_FLOORS, FLOOR_LABELS, load, one, number_value, selected_floor, sha
import csv


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--brief',type=Path,default=Path(__file__).resolve().parent.parent)
    p.add_argument('--floors',type=Path,default=DEFAULT_FLOORS)
    p.add_argument('--synthetic',action='store_true')
    p.add_argument('--excluded-attempt',action='store_true',help='quarantined aborted-attempt layout test, never final figures')
    args=p.parse_args(); brief=args.brief.resolve(); data=load(args.output)
    if args.synthetic and args.excluded_attempt: p.error('choose synthetic or excluded-attempt mode, not both')
    mode='SYNTHETIC' if args.synthetic else 'EXCLUDED' if args.excluded_attempt else ''
    if 'SYNTHETIC' in str(args.output.resolve()) and not args.synthetic: p.error('fixture input requires --synthetic')
    with args.floors.open(newline='') as stream: data['run9-floors']=list(csv.DictReader(stream))
    figdir=brief/('rwc10/'+mode+'-figures' if mode else 'figures'); figdir.mkdir(parents=True,exist_ok=True)
    generated=brief/'rwc10'/((mode+'-generated') if mode else 'generated'); generated.mkdir(parents=True,exist_ok=True)
    plt.rcParams.update({'font.size':9,'axes.titlesize':8,'axes.labelsize':9,'xtick.labelsize':8,'ytick.labelsize':8,
                         'legend.fontsize':8,'pdf.fonttype':42,'axes.spines.top':False,'axes.spines.right':False})
    evidence=dict(synthetic=args.synthetic,excluded_from_final=args.excluded_attempt,sources={str((args.output/(f+'.csv')).resolve()):sha(args.output/(f+'.csv')) for f in data if f!='run9-floors'},figures={})
    evidence['sources'][str(args.floors.resolve())]=sha(args.floors); points=[]

    def value(family,index,field,factor=1,kind='marker'):
        row=data[family][index]; raw=number_value(row,field)
        if raw is None: return None
        v=raw*factor
        points.append(dict(family=family,row=index,field=field,factor=factor,value=v,kind=kind,
                           case_id=row.get('case_id'),repetition=row['repetition'],arm=row.get('arm'),size_bytes=row['size_bytes'],
                           readers=row.get('readers'),read_lanes=row.get('read_lanes'),security_label=row.get('security_label'),owner_polling=row.get('owner_polling')))
        return v

    def finish(fig,stem,note):
        nonlocal points
        if args.synthetic:
            fig.text(.5,.5,'SYNTHETIC',fontsize=42,color='#555555',alpha=.14,rotation=28,ha='center',va='center')
            fig.text(.99,.99,'SYNTHETIC',fontsize=9,ha='right',va='top')
        if args.excluded_attempt:
            fig.text(.5,.5,'EXCLUDED ATTEMPT',fontsize=31,color='#555555',alpha=.16,rotation=28,ha='center',va='center')
            fig.text(.99,.99,'EXCLUDED — LAYOUT TEST ONLY',fontsize=8,ha='right',va='top')
        footer=fig.text(.5,.012,note,ha='center',va='bottom',fontsize=8)
        fig.canvas.draw()
        renderer=fig.canvas.get_renderer()
        titles=[ax.title.get_window_extent(renderer) for ax in fig.axes]
        xlabels=[ax.xaxis.label.get_window_extent(renderer) for ax in fig.axes]
        legends=[legend.get_window_extent(renderer) for legend in fig.legends]
        assert not any(legend.overlaps(title) for legend in legends for title in titles),(stem,'legend/title overlap')
        assert not any(label.overlaps(title) for label in xlabels for title in titles),(stem,'xlabel/title overlap')
        labels=[text.get_window_extent(renderer) for ax in fig.axes for text in [ax.xaxis.label,ax.yaxis.label,*ax.get_xticklabels(),*ax.get_yticklabels()] if text.get_text()]
        assert not any(footer.get_window_extent(renderer).overlaps(label) for label in labels),(stem,'footer/label overlap')
        name='rwc10-'+(mode+'-' if mode else '')+stem+'.pdf'; path=figdir/name
        fig.savefig(path)
        evidence['figures'][str(path.relative_to(brief))]=dict(points=points,note=note,synthetic_watermark=args.synthetic,figure_inches=list(fig.get_size_inches()),layout_checks=dict(legend_title_disjoint=True,xlabel_title_disjoint=True,footer_label_disjoint=True))
        points=[]; plt.close(fig)

    def panels(height=4.6,categorical=True):
        fig,axes=plt.subplots(2,2,figsize=(7.0,height))
        fig.subplots_adjust(left=.23 if categorical else .1,right=.98,top=.86,bottom=.23,hspace=1.05,wspace=1.15 if categorical else .42)
        fig.legend(handles=[Line2D([],[],marker='o',linestyle='none',color='#222222',label='r0',markerfacecolor='#222222'),
                            Line2D([],[],marker='o',linestyle='none',color='#222222',label='r1',markerfacecolor='white')],
                   loc='upper center',ncol=2,frameon=False)
        for ax,(size,c) in zip(axes.flat,CELLS):
            ax.set_title(f'{size//1024} KiB · c{c} · lanes {1 if c==1 else 4}\ncoord cache off · r0/r1 separate',pad=8)
            ax.xaxis.set_major_locator(MaxNLocator(3)); ax.grid(axis='x',alpha=.2)
        return fig,axes

    def pad_x(ax,minimum=0):
        maximum=max((float(line.get_xdata()[0]) for line in ax.lines if len(line.get_xdata())),default=1.)
        maximum=max(maximum,minimum)
        ax.set_xlim(0,1.08*maximum if maximum>0 else 1.)

    def dots(ax,family,keys,size,c,field,labels):
        statuses_by_rep={0:[],1:[]}
        for y,key in enumerate(keys):
            color = COLORS[y if family=='ab_pairs' else WARM.index(key) if key in WARM else COLD.index(key)]
            for rep in (0,1):
                i,row=one(data[family],key,size,c,rep,field='comparison' if family=='ab_pairs' else 'arm')
                statuses_by_rep[rep].extend((row['status_a'],row['status_b']) if family=='ab_pairs' else (row['status'],))
                eligible=(row['parity']=='matched' and row['status_a']==row['status_b']=='completed') if family=='ab_pairs' else row['status']=='completed'
                v=value(family,i,field) if eligible else None
                if v is not None:
                    ax.plot(v,y+(.12 if rep else -.12),marker='o',linestyle='none',color=color,markersize=5,
                            markerfacecolor='white' if rep else color)
        statuses=[s for values in statuses_by_rep.values() for s in values]
        if all(s=='not_run' for s in statuses):
            ax.text(.5,.5,'NOT RUN',transform=ax.transAxes,ha='center',va='center',fontsize=10,color='#555555')
        ax.set_yticks(range(len(keys)),labels); ax.set_ylim(len(keys)-.55,-.55)
        pad_x(ax,minimum=1. if family=='ab_pairs' else 0)
        missing_reps=[rep for rep,values in statuses_by_rep.items() if all(s=='not_run' for s in values)]
        if len(missing_reps)==1:
            ax.set_ylim(len(keys)+.35,-.55)
            ax.text(.5,.02,f'r{missing_reps[0]} not run',transform=ax.transAxes,ha='center',va='bottom',fontsize=8,color='#555555')

    fig,axes=panels()
    for ax,(size,c) in zip(axes.flat,CELLS):
        dots(ax,'ab_warm',WARM,size,c,'mean_us',[LABELS[a].replace('\n',' ') for a in WARM]); ax.set_xlabel('Mean API latency (µs)')
    finish(fig,'warm',CONTRACT+'\nWhole successful calls; separate case means, not isolated transport or crypto timers.')

    fig,axes=panels()
    for ax,(size,c) in zip(axes.flat,CELLS):
        dots(ax,'ab_pairs',COMPARISONS,size,c,'mean_ratio_a_over_b',PAIR_LABELS)
        ax.axvline(1,color='#777777',linewidth=.8,linestyle=':'); ax.set_xlabel('Matched mean ratio A / B')
    finish(fig,'gap',CONTRACT+'\nEvery ratio uses the same size/readers/repetition; A/B direction is in each row; unavailable pairs have no ratio.')

    fig,axes=panels(height=4.8,categorical=False)
    # Replace the repetition-only legend with security/policy colors plus role and repetition conventions.
    for legend in fig.legends: legend.remove()
    handles=[Line2D([],[],marker='o',color=COLORS[i],linestyle='none',label=CODES[a]+': '+LABELS[a].replace('\n',' ')) for i,a in enumerate(WARM)]
    fig.legend(handles=handles,loc='upper center',bbox_to_anchor=(.5,.96),ncol=3,frameon=False,fontsize=8)
    fig.subplots_adjust(top=.78,bottom=.29)
    for ax,(size,c) in zip(axes.flat,CELLS):
        panel_rows=[r for r in data['ab_warm'] if (r['size_bytes'],r['readers'])==(str(size),str(c))]
        for k,arm in enumerate(WARM):
            for rep in (0,1):
                i,row=one(data['ab_warm'],arm,size,c,rep)
                if row['status']!='completed': continue
                for field,marker in [('client_cores','o'),('owner_cores','^')]:
                    core=number_value(row,field); mean=number_value(row,'mean_us')
                    if core is None or mean is None: continue
                    ax.plot(value('ab_warm',i,'mean_us',kind='x'),value('ab_warm',i,field),marker=marker,linestyle='none',color=COLORS[k],
                            markersize=5,markerfacecolor='white' if rep else COLORS[k],clip_on=False)
        ax.set_xlabel('Mean API latency (µs)'); ax.set_ylabel('Process CPU (cores)'); pad_x(ax)
        ymax=max((float(line.get_ydata()[0]) for line in ax.lines if len(line.get_ydata())),default=1.)
        ax.set_ylim(0,1.10*ymax if ymax>0 else 1.)
        if all(r['status']=='not_run' for r in panel_rows):
            ax.text(.5,.5,'NOT RUN',transform=ax.transAxes,ha='center',va='center',fontsize=10,color='#555555')
        missing_reps=[rep for rep in (0,1) if all(r['status']=='not_run' for r in panel_rows if r['repetition']==str(rep))]
        if len(missing_reps)==1:
            ax.set_title(ax.get_title()+f'\nr{missing_reps[0]} not run',pad=9)
    finish(fig,'cpu',CONTRACT+'\nCircles: client; triangles: owner. Filled r0, open r1. Cores = CPU seconds / elapsed seconds.\nIndependent host/cgroup gate windows, not API CPU attribution; missing CPU is not zero.')

    fig,axes=panels(height=4.8)
    for legend in fig.legends: legend.remove()
    handles=[Line2D([],[],marker='o',linestyle='none',color=COLORS[i],label=CODES[a]+': '+LABELS[a].replace('\n','; ')) for i,a in enumerate(COLD)]
    handles += [Line2D([],[],marker='o',linestyle='none',color='#222222',markerfacecolor=face,label=label)
                for face,label in [('#222222','r0'),('white','r1')]]
    fig.legend(handles=handles,loc='upper center',bbox_to_anchor=(.5,.96),ncol=2,frameon=False,fontsize=8)
    # Reserve a legend band above both title rows; the four-row legend must not touch the grid.
    fig.subplots_adjust(top=.70,hspace=1.25)
    for ax,(size,c) in zip(axes.flat,CELLS):
        qlabels=[]
        for arm in COLD:
            ratios=[]
            for rep in (0,1):
                i,row=one(data['cold_fabric'],arm,size,c,rep); ratio=value('cold_fabric',i,'device_reads_per_call',kind='annotation')
                ratios.append('not run' if row['status']=='not_run' else ('U:' if row['status']=='unqualified' else '')+(f'{ratio:.2f}' if ratio is not None else 'NR'))
            qlabels.append(CODES[arm]+': '+ '/'.join(ratios))
        dots(ax,'cold_fabric',COLD,size,c,'mean_us',qlabels); ax.set_xlabel('Qualified mean API (µs)')
    finish(fig,'cold','Coordinator cache off; cold owner RPC (trusted adaptive) versus kernel direct / SPDK file-AIO namespace reads.\nNVMe-oF owner control: verbs trusted adaptive; kernel initiator; not engine one-sided RDMA READ.\nLanes 1 at c1, 4 at c16; labels show device reads/call r0/r1. U = unqualified, no latency marker.\nBusy means owner+coordinator user-space spinning, not SO_BUSY_POLL (no Busy cold arm); repetitions separate.')

    fig,axes=plt.subplots(1,2,figsize=(7.0,3.8)); fig.subplots_adjust(left=.23,right=.98,top=.86,bottom=.28,wspace=1.15)
    for y,label in enumerate(FLOOR_LABELS):
        for rep in ('0','1','2'):
            found=[(i,r,selected_floor(r)) for i,r in enumerate(data['run9-floors']) if r['repetition']==rep and selected_floor(r) and selected_floor(r)[0]==label]
            if len(found)!=1: raise ValueError(('floor population',label,rep))
            i,row,(_,factor)=found[0]
            if row['status']!='completed': continue
            axes[0].plot(value('run9-floors',i,'mean_us',factor),y+(int(rep)-1)*.15,marker=('o','o','x')[int(rep)],linestyle='none',color='#555555',
                         markersize=4,markerfacecolor='#555555' if rep=='0' else 'white')
    axes[0].set_yticks(range(7),[s.replace('SO_BUSY_POLL','SO_BUSY_POLL\n(kernel floor)') for s in FLOOR_LABELS]); axes[0].set_ylim(6.6,-.6)
    engine=(WARM[1],WARM[2],WARM[3],WARM[4])
    dots(axes[1],'ab_warm',engine,1024,1,'mean_us',[LABELS[a] for a in engine])
    axes[0].set_title('Run 9: REAL network tools\nCROSS-FLEET, not owner RPC',pad=9)
    axes[1].set_title('Run 10: warm owner RPC\ntrusted; cache off; c1/lane 1',pad=9)
    for ax in axes:
        ax.set_xlabel('Mean round trip / API (µs)'); pad_x(ax); ax.xaxis.set_major_locator(MaxNLocator(3)); ax.grid(axis='x',alpha=.2)
    finish(fig,'ladder','1 KiB: run-9 floors r0/r1/r2 (filled/open/x) beside run-10 case means r0/r1 (filled/open).\nDifferent fleets and populations: NEVER subtract floors from engine means or stack them. No isolated software remainder.\nRun-10 coordinator cache off; owner RPC trusted; Busy spins owner AND coordinator in user space, not SO_BUSY_POLL.\nEngine lanes 1 at c1 (4 at c16 elsewhere); no pooled repetitions. Raw RDMA READ floor is NOT an engine feature.')

    path=generated/'figure-evidence.json'; path.write_text(json.dumps(evidence,indent=2)+'\n')
    manifest_path=generated/'inputs-rwc10.json'
    if manifest_path.is_file():
        manifest=json.loads(manifest_path.read_text()); assert manifest['synthetic']==args.synthetic
        assert manifest.get('excluded_from_final',False)==args.excluded_attempt
        for name in evidence['figures']: manifest['outputs'][name]=sha(brief/name)
        manifest['outputs'][str(path.relative_to(brief))]=sha(path)
        manifest_path.write_text(json.dumps(manifest,indent=2)+'\n')
        registry=brief/'inputs.json'; old=json.loads(registry.read_text())
        if old.get('rwc10',{}).get('manifest')==str(manifest_path.relative_to(brief)):
            old['outputs'].update(manifest['outputs']); old['outputs'][str(manifest_path.relative_to(brief))]=sha(manifest_path)
            registry.write_text(json.dumps(old,indent=2)+'\n')
    print('Five figures and per-marker/annotation evidence: '+str(path))


if __name__=='__main__': main()
