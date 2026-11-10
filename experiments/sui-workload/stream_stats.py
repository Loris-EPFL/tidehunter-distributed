#!/usr/bin/env python3
"""Calculate sample statistics in one CSV pass with numeric arrays and counters."""
from array import array
import collections
import csv
import pathlib
import sys
import numpy as np
from metrics import percentiles

root=pathlib.Path(sys.argv[1])
def rows(name):
    with (root/name).open(newline='') as f: yield from csv.DictReader(f)
def save(name,head,vals):
    with (root/name).open('w',newline='') as f:
        w=csv.writer(f);w.writerow(head);w.writerows(vals)
cp=list(rows('checkpoints.csv')); windows=sorted({int(r['window']) for r in cp}); labels=['all']+list(map(str,windows))
dist=collections.defaultdict(lambda:array('q'));mix=collections.defaultdict(collections.Counter); targets=collections.defaultdict(collections.Counter)
pop=collections.defaultdict(collections.Counter); shared=collections.defaultdict(collections.Counter);shared_types={};types=collections.defaultdict(lambda:collections.defaultdict(lambda:[0,0]))
seen=collections.defaultdict(set);first_seen=collections.Counter();known=collections.Counter();input_count=collections.Counter();created_count=collections.Counter();created_cp=collections.Counter();tx_count=collections.Counter()
def add(label,metric,val):
    if val!='':dist[(label,metric)].append(int(val))
def addmix(label,metric,val):mix[(label,metric)][val]+=1
cpcols=['transactions','interval_ms','raw_bytes','events','objects_written','transaction_bcs_bytes','effects_bcs_bytes','events_bcs_bytes','output_object_bcs_bytes']
for r in cp:
    for w in ('all',r['window']):
        for col in cpcols:add(w,'checkpoint.'+col,r[col])
        add(w,'checkpoint.component_bcs_bytes',sum(int(r[k]) for k in ('transaction_bcs_bytes','effects_bcs_bytes','events_bcs_bytes','output_object_bcs_bytes')))
txcols=['commands','input_objects','output_objects','created','mutated','deleted','wrapped','unwrapped','transaction_bytes','effects_bytes','events_bytes','gas_computation','gas_storage','gas_rebate']
for r in rows('transactions.csv'):
    w=r['window'];tx_count[w]+=1;created_count[w]+=int(r['created']);created_cp[(w,r['checkpoint'])]+=int(r['created'])
    for label in ('all',w):
        for col in txcols:add(label,'transaction.'+col,r[col])
        add(label,'transaction.net_gas_mist',int(r['gas_computation'])+int(r['gas_storage'])-int(r['gas_rebate']))
        for metric,key in [('kind','kind'),('class','class'),('success','success'),('shared','shared')]:addmix(label,metric,r[key])
        if r['kind']=='ProgrammableTransaction':
            add(label,'ptb.commands',r['commands']);addmix(label,'ptb_shared',r['shared'])
for r in rows('commands.csv'):
    for label in ('all',r['window']):
        addmix(label,'command',r['command'])
        if r['target']:targets[label][r['target']]+=1
for r in rows('input_objects.csv'):
    w=r['window'];oid=r['object_id'];input_count[w]+=1;known[w]+=bool(r['version_created_checkpoint'])
    if oid not in seen[w]:seen[w].add(oid);first_seen[(w,r['checkpoint'])]+=1
    for label in ('all',w):
        pop[label][oid]+=1
        for col in ('age_checkpoints','age_transactions'):add(label,'input.'+col,r[col])
        if r['shared']=='1':shared[label][oid]+=1
    if r['object_type']:shared_types[oid]=r['object_type']
for r in rows('output_objects.csv'):
    typ=r['object_type'];size=int(r['bcs_bytes'])
    for label in ('all',r['window']):
        add(label,'output.bcs_bytes',size);addmix(label,'output_change',r['change']);types[label][typ][0]+=1;types[label][typ][1]+=size
for r in rows('events.csv'):
    for label in ('all',r['window']):add(label,'event.bcs_bytes',r['bcs_bytes'])
metrics=[]
for (label,name),values in dist.items():metrics.append([label,name,*percentiles(values)])
# Keep empty age/interval metrics explicit if a whole group has none.
save('percentiles.csv',['window','metric','count','p50','p90','p99','p99_9','max','mean'],metrics)
mixrows=[]
for (label,metric),counter in mix.items():
    total=sum(counter.values())
    for value,n in counter.most_common():mixrows.append([label,metric,value,n,n/total])
save('mix.csv',['window','metric','value','count','share'],mixrows)
targetrows=[]
for label,counter in targets.items():
    total=sum(counter.values());cumulative=0
    for rank,(target,n) in enumerate(counter.most_common(30),1):
        cumulative+=n;targetrows.append([label,rank,target,n,n/total,cumulative/total])
save('move_targets.csv',['window','rank','target','count','share','cumulative_share'],targetrows)
with (root/'popularity.csv').open('w',newline='') as f:
    writer=csv.writer(f);writer.writerow(['window','rank','object_id','accesses','cumulative_share'])
    concentration=[]
    for label in labels:
        ordered=pop[label].most_common();total=sum(pop[label].values());cumulative=0
        for rank,(oid,n) in enumerate(ordered,1):
            cumulative+=n;writer.writerow([label,rank,oid,n,cumulative/total])
        for k in (1,10,100,1000,10000):concentration.append([label,k,sum(n for _,n in ordered[:k])/total,total,len(ordered)])
        lo,hi=10,min(10000,len(ordered)); exponent=''
        if hi>lo:exponent=-float(np.polyfit(np.log(np.arange(lo,hi+1)),np.log([n for _,n in ordered[lo-1:hi]]),1)[0])
        concentration.append([label,'zipf_exponent',exponent,lo,hi])
save('concentration.csv',['window','rank_or_metric','value','accesses_or_fit_first_rank','unique_or_fit_last_rank'],concentration)
typerows=[];toprows=[]
for label in labels:
    group=types[label];count=sum(v[0] for v in group.values());bytecount=sum(v[1] for v in group.values());raw=[]
    for typ,(n,b) in group.items():raw.append([label,typ,n,b,n/count,b/bytecount])
    typerows.extend(raw)
    for criterion,index in [('count',2),('bytes',3)]:
        for rank,row in enumerate(sorted(raw,key=lambda r:r[index],reverse=True)[:20],1):toprows.append([label,criterion,rank,*row[1:]])
save('object_types.csv',['window','type','count','bytes','count_share','byte_share'],typerows)
save('top_object_types.csv',['window','criterion','rank','type','count','bytes','count_share','byte_share'],toprows)
hot=[]
for label in labels:
    total=sum(shared[label].values())
    for rank,(oid,n) in enumerate(shared[label].most_common(20),1):hot.append([label,rank,oid,shared_types.get(oid,''),n,n/total])
save('hottest_shared.csv',['window','rank','object_id','type','accesses','share_of_shared_inputs'],hot)
working=[];windowrows=[]
for w in map(str,windows):
    cs=[r for r in cp if r['window']==w];unique=0
    for r in cs:
        n=first_seen[(w,r['sequence'])];unique+=n;working.append([w,r['sequence'],unique,n,created_cp[(w,r['sequence'])]])
    windowrows.append([w,cs[0]['sequence'],cs[-1]['sequence'],cs[0]['timestamp_ms'],cs[-1]['timestamp_ms'],tx_count[w],input_count[w],known[w],known[w]/input_count[w] if input_count[w] else 0,created_count[w]])
save('working_set.csv',['window','checkpoint','unique_objects','new_unique_objects','new_objects_created'],working)
save('windows.csv',['window','first_checkpoint','last_checkpoint','first_timestamp_ms','last_timestamp_ms','transactions','input_accesses','versions_created_in_window','fraction_versions_created_in_window','new_objects'],windowrows)
