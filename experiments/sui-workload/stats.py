#!/usr/bin/env python3
"""Summarize checkpoint CSVs; percentiles use linear interpolation."""
import collections
import csv
import math
import pathlib
import sys
import numpy as np
from metrics import percentiles

root=pathlib.Path(sys.argv[1])
def load(name):
    with (root/name).open(newline='') as f:return list(csv.DictReader(f))
def save(name,header,rows):
    with (root/name).open('w',newline='') as f:
        w=csv.writer(f);w.writerow(header);w.writerows(rows)
cp=load('checkpoints.csv');tx=load('transactions.csv');ins=load('input_objects.csv');outs=load('output_objects.csv');commands=load('commands.csv');events=load('events.csv')
sets=[('all',None)]+[(str(i),str(i)) for i in sorted({int(r['window']) for r in cp})]
def selected(rows,w):return rows if w is None else [r for r in rows if r['window']==w]
metrics=[]; mixes=[]; popularity=[]; concentration=[]; types=[]; hottest=[]; working=[]; windows=[]; targets=[]
for label,w in sets:
    c=selected(cp,w);t=selected(tx,w);ii=selected(ins,w);oo=selected(outs,w);cc=selected(commands,w);ee=selected(events,w)
    for name,rs,cols in [('checkpoint',c,['transactions','interval_ms','raw_bytes','events','objects_written','transaction_bcs_bytes','effects_bcs_bytes','events_bcs_bytes','output_object_bcs_bytes']),('transaction',t,['commands','input_objects','output_objects','created','mutated','deleted','wrapped','unwrapped','transaction_bytes','effects_bytes','events_bytes','gas_computation','gas_storage','gas_rebate']),('input',ii,['age_checkpoints','age_transactions']),('output',oo,['bcs_bytes']),('event',ee,['bcs_bytes'])]:
        for col in cols:
            vals=[int(r[col]) for r in rs if r[col]!='']
            metrics.append([label,name+'.'+col,*percentiles(vals)])
    parts=('transaction_bcs_bytes','effects_bcs_bytes','events_bcs_bytes','output_object_bcs_bytes')
    metrics.append([label,'checkpoint.component_bcs_bytes',*percentiles([sum(int(r[k]) for k in parts) for r in c])])
    ptb=[r for r in t if r['kind']=='ProgrammableTransaction']
    metrics.append([label,'ptb.commands',*percentiles([int(r['commands']) for r in ptb])])
    for value,n in collections.Counter(r['shared'] for r in ptb).items():
        mixes.append([label,'ptb_shared',value,n,n/len(ptb)])
    for name,rs,key in [('kind',t,'kind'),('class',t,'class'),('success',t,'success'),('shared',t,'shared'),('command',cc,'command'),('output_change',oo,'change')]:
        counter=collections.Counter(r[key] for r in rs)
        for val,n in counter.most_common(): mixes.append([label,name,val,n,n/len(rs) if rs else 0])
    targets_count=collections.Counter(r['target'] for r in cc if r['target']); count=sum(targets_count.values());cum=0
    for rank,(target,n) in enumerate(targets_count.most_common(30),1):
        cum+=n;targets.append([label,rank,target,n,n/count,cum/count])
    count=collections.Counter(r['object_id'] for r in ii);total=sum(count.values());ordered=count.most_common();cum=0
    for rank,(oid,n) in enumerate(ordered,1):
        cum+=n;popularity.append([label,rank,oid,n,cum/total])
    if total:
        for k in (1,10,100,1000,10000):concentration.append([label,k,sum(n for _,n in ordered[:k])/total,total,len(count)])
    lo,hi=10,min(10000,len(ordered)); alpha=''
    if hi>=lo+1:
        x=np.log(np.arange(lo,hi+1)); y=np.log([n for _,n in ordered[lo-1:hi]]);alpha=-float(np.polyfit(x,y,1)[0])
    concentration.append([label,'zipf_exponent',alpha,lo,hi])
    groups=collections.defaultdict(lambda:[0,0])
    for r in oo:groups[r['object_type']][0]+=1;groups[r['object_type']][1]+=int(r['bcs_bytes'])
    total_bytes=sum(v[1] for v in groups.values())
    for typ,(n,b) in groups.items():types.append([label,typ,n,b,n/len(oo) if oo else 0,b/total_bytes if total_bytes else 0])
    share=collections.Counter(r['object_id'] for r in ii if r['shared']=='1'); type_by_id={r['object_id']:r['object_type'] for r in ii if r['object_type']}
    for rank,(oid,n) in enumerate(share.most_common(20),1):hottest.append([label,rank,oid,type_by_id.get(oid,''),n,n/sum(share.values())])
    seen=set(); bycp=collections.defaultdict(list)
    for r in ii:bycp[int(r['checkpoint'])].append(r['object_id'])
    new_by_checkpoint=collections.Counter()
    for row in t: new_by_checkpoint[int(row['checkpoint'])]+=int(row['created'])
    # Global working set is shown per window because windows are not contiguous.
    if w is not None:
        for r in c:
            ids=bycp[int(r['sequence'])];before=len(seen);seen.update(ids);working.append([label,r['sequence'],len(seen),len(seen)-before,new_by_checkpoint[int(r['sequence'])]])
        known=sum(bool(r['version_created_checkpoint']) for r in ii)
        windows.append([label,c[0]['sequence'],c[-1]['sequence'],c[0]['timestamp_ms'],c[-1]['timestamp_ms'],len(t),len(ii),known,known/len(ii) if ii else 0,sum(int(r['created']) for r in t)])
save('percentiles.csv',['window','metric','count','p50','p90','p99','p99_9','max','mean'],metrics)
save('mix.csv',['window','metric','value','count','share'],mixes)
save('move_targets.csv',['window','rank','target','count','share','cumulative_share'],targets)
save('popularity.csv',['window','rank','object_id','accesses','cumulative_share'],popularity)
save('concentration.csv',['window','rank_or_metric','value','accesses_or_fit_first_rank','unique_or_fit_last_rank'],concentration)
save('object_types.csv',['window','type','count','bytes','count_share','byte_share'],types)
top=[]
for label,_ in sets:
    subset=[row for row in types if row[0]==label]
    for criterion,index in [('count',2),('bytes',3)]:
        for rank,row in enumerate(sorted(subset,key=lambda row:row[index],reverse=True)[:20],1):
            top.append([label,criterion,rank,*row[1:]])
save('top_object_types.csv',['window','criterion','rank','type','count','bytes','count_share','byte_share'],top)
save('hottest_shared.csv',['window','rank','object_id','type','accesses','share_of_shared_inputs'],hottest)
save('working_set.csv',['window','checkpoint','unique_objects','new_unique_objects','new_objects_created'],working)
save('windows.csv',['window','first_checkpoint','last_checkpoint','first_timestamp_ms','last_timestamp_ms','transactions','input_accesses','versions_created_in_window','fraction_versions_created_in_window','new_objects'],windows)
