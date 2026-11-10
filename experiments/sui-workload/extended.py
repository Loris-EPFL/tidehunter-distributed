#!/usr/bin/env python3
"""Produce hourly, byte-rate, first-package and workload model profiles."""
import collections
import csv
import datetime as dt
import json
import math
import pathlib
import sys
import numpy as np
from metrics import complete_component_buckets

root=pathlib.Path(sys.argv[1])
def rows(name):
    with (root/name).open(newline='') as f:
        yield from csv.DictReader(f)
def save(name,header,values):
    with (root/name).open('w',newline='') as f:
        w=csv.writer(f);w.writerow(header);w.writerows(values)
def quantiles(v):
    if not len(v): return None
    a=np.asarray(v,dtype=float)
    return dict(zip(('count','p50','p90','p99','p999','max'),[len(a),*map(float,np.percentile(a,[50,90,99,99.9])),float(a.max())]))
cp=list(rows('checkpoints.csv')); wins=list(rows('windows.csv')); per=list(rows('percentiles.csv'));mix=list(rows('mix.csv'));conc=list(rows('concentration.csv'))
by_window=collections.defaultdict(list)
for c in cp: by_window[c['window']].append(c)
components=['transaction_bcs_bytes','effects_bcs_bytes','events_bcs_bytes','output_object_bcs_bytes']
hours=collections.defaultdict(list)
for c in cp:
    hour=dt.datetime.fromtimestamp(int(c['timestamp_ms'])/1000,dt.timezone.utc).hour
    hours[hour].append(c)
hour_rows=[]
for hour,cs in sorted(hours.items()):
    intervals=[int(c['interval_ms']) for c in cs if c['interval_ms']]
    hour_rows.append([hour,len(cs),sum(int(c['transactions']) for c in cs),float(np.median([int(c['transactions']) for c in cs])),sum(int(c['transactions']) for c in cs)/len(cs),1000*len(intervals)/sum(intervals) if intervals and sum(intervals) else '',len({c['window'] for c in cs})])
save('hourly_profile.csv',['utc_hour','checkpoints','transactions','median_tx_per_checkpoint','mean_tx_per_checkpoint','checkpoint_rate_per_second','windows'],hour_rows)
rate_rows=[]
for window,cs in by_window.items():
    for seconds in (1,10):
        for start,vals in complete_component_buckets(cs,seconds,components):
            rate_rows.append([window,seconds,start,*vals,sum(vals)])
save('component_rates.csv',['window','bucket_seconds','start_timestamp_ms','transaction_bytes_per_s','effects_bytes_per_s','event_bytes_per_s','output_object_bytes_per_s','total_bytes_per_s'],rate_rows)
rate_summary=[]
for win in ['all']+list(by_window):
    for sec in (1,10):
        for i,name in enumerate(['transaction','effects','events','output_objects','total'],3):
            v=[r[i] for r in rate_rows if r[1]==sec and (win=='all' or r[0]==win)]
            q=quantiles(v)
            if q: rate_summary.append([win,sec,name,q['count'],q['p50'],q['p99'],q['max']])
save('component_rate_summary.csv',['window','bucket_seconds','component','count','p50_bytes_s','p99_bytes_s','max_bytes_s'],rate_summary)
growth=[]
for window in by_window:
    working=[r for r in rows('working_set.csv') if r['window']==window]
    previous=0
    for index,r in enumerate(working,1):
        if index%1000==0 or index==len(working):
            current=int(r['unique_objects'])
            growth.append([window,index,r['checkpoint'],current,current-previous])
            previous=current
save('working_set_1000.csv',['window','checkpoints','sequence','unique_objects','new_unique_since_previous'],growth)
# One package per user PTB: command rows contain only user PTB commands.
first={}
for row in rows('commands.csv'):
    if not row['target']: continue
    key=(row['window'],row['checkpoint'],row['transaction_index'])
    if key not in first: first[key]=row['target'].split('::',2)[:2]
pkg=collections.Counter();modules=collections.defaultdict(collections.Counter);ptb=collections.Counter();changes=collections.Counter()
for t in rows('transactions.csv'):
    for k in ('created','mutated','deleted','wrapped','unwrapped'): changes[k]+=int(t[k])
    if t['kind']!='ProgrammableTransaction': continue
    ptb[t['window']]+=1;ptb['all']+=1
    key=(t['window'],t['checkpoint'],t['index'])
    if key in first:
        package,module=first[key]
        for w in ('all',t['window']):pkg[(w,package)]+=1;modules[(w,package)][module]+=1
    else:
        for w in ('all',t['window']):pkg[(w,'<no MoveCall>')]+=1
pkgrows=[]
for w in ['all']+list(by_window):
    pairs=sorted(((p,n) for (ww,p),n in pkg.items() if ww==w and p!='<no MoveCall>'),key=lambda x:(-x[1],x[0]))
    cumulative=0
    for rank,(p,n) in enumerate(pairs[:20],1):
        cumulative+=n
        pkgrows.append([w,rank,p,n,n/ptb[w],cumulative/ptb[w],';'.join(f'{m}:{k}' for m,k in modules[(w,p)].most_common())])
    no_call=pkg[(w,'<no MoveCall>')]
    pkgrows.append([w,0,'<no MoveCall>',no_call,no_call/ptb[w],'',''])
save('first_move_packages.csv',['window','rank','package','transactions','share_of_user_ptbs','cumulative_share','modules_and_counts'],pkgrows)
# Variation of each per-window percentile and mix statistic; comparisons are not independent samples.
variation=[]
for metric in sorted({r['metric'] for r in per}):
    for field in ('p50','p90','p99','p99_9','max','mean'):
        vals=[float(r[field]) for r in per if r['window']!='all' and r['metric']==metric and r[field]!='']
        if vals:variation.append([metric,field,min(vals),float(np.median(vals)),max(vals),len(vals)])
for metric,value in sorted({(r['metric'],r['value']) for r in mix}):
    vals=[float(r['share']) for r in mix if r['window']!='all' and r['metric']==metric and r['value']==value]
    # Missing categories represent zero share in those windows.
    vals += [0.]*(len(by_window)-len(vals))
    variation.append(['mix.'+metric+'.'+value,'share',min(vals),float(np.median(vals)),max(vals),len(vals)])
for rank in ('1','10','100','1000','10000'):
    vals=[float(r['value']) for r in conc if r['window']!='all' and r['rank_or_metric']==rank]
    variation.append(['concentration.top_'+rank,'share',min(vals),float(np.median(vals)),max(vals),len(vals)])
for seconds in (1,10):
    for component in ('transaction','effects','events','output_objects','total'):
        for column,stat in ((4,'p50'),(5,'p99')):
            vals=[r[column] for r in rate_summary if r[0]!='all' and r[1]==seconds and r[2]==component]
            if vals:variation.append([f'component_rate.{seconds}s.{component}',stat,min(vals),float(np.median(vals)),max(vals),len(vals)])
vals=[float(r['fraction_versions_created_in_window']) for r in wins]
variation.append(['input.fraction_versions_created_in_window','share',min(vals),float(np.median(vals)),max(vals),len(vals)])
save('window_variation.csv',['metric','statistic','min','median','max','windows'],variation)
# Output size distributions for frequent types and a separately labelled remainder.
type_counts=collections.Counter()
for o in rows('output_objects.csv'):type_counts[o['object_type']]+=1
leading=[x for x,n in type_counts.most_common(20)];groups={x:[] for x in leading};groups['<other types>']=[]
for o in rows('output_objects.csv'):
    typ=o['object_type'];groups[typ if typ in groups else '<other types>'].append(int(o['bcs_bytes']))
type_dist=[{'type':typ,'size_bytes':quantiles(v)} for typ,v in groups.items()]
# Rank buckets preserve accesses and object counts; last bucket is open-ended.
rank_buckets=[]; bounds=[(1,1),(2,10),(11,100),(101,1000),(1001,10000),(10001,None)]
for lo,hi in bounds:
    count=accesses=0
    for row in rows('popularity.csv'):
        if row['window']!='all':continue
        rank=int(row['rank'])
        if rank>=lo and (hi is None or rank<=hi):count+=1;accesses+=int(row['accesses'])
    rank_buckets.append({'rank_start':lo,'rank_end':hi,'objects':count,'accesses':accesses,'mean_accesses':accesses/count if count else 0})
def metric(name):
    r=next(r for r in per if r['window']=='all' and r['metric']==name)
    return {'count':int(r['count']),'p50':float(r['p50']),'p90':float(r['p90']),'p99':float(r['p99']),'p999':float(r['p99_9']),'max':float(r['max'])}
change_total=sum(changes.values())
model={'schema_version':1,'source':'primary checkpoint sample; input accesses are reconstructed, not KV reads',
       'transactions_per_checkpoint':metric('checkpoint.transactions'),
       'output_object_size_bytes':metric('output.bcs_bytes'),
       'output_object_sizes_by_type':type_dist,
       'input_object_popularity_rank_buckets':rank_buckets,
       'input_version_age_checkpoints_conditional':metric('input.age_checkpoints'),
       'input_version_age_transactions_conditional':metric('input.age_transactions'),
       'input_version_age_coverage':sum(int(w['versions_created_in_window']) for w in wins)/sum(int(w['input_accesses']) for w in wins),
       'window_censor_horizon_checkpoints':max(int(w['last_checkpoint'])-int(w['first_checkpoint']) for w in wins),
       'object_change_counts':dict(changes),'object_change_shares':{k:v/change_total for k,v in changes.items()},
       'definitions':{'quantiles':'Linear interpolation; pooled across all primary windows.',
          'schema_version':'Version of this JSON schema.',
          'source':'Scope and limitations of the sampled data.',
          'transactions_per_checkpoint':'Empirical checkpoint transaction-count quantiles.',
          'output_object_size_bytes':'Empirical BCS size quantiles for output objects.',
          'object_change_counts':'Counts of each effects-derived object change across all sampled transactions.',
          'quantile_fields':'count is the sample count; p50, p90, p99, p999 and max are empirical values.',
          'output_object_sizes_by_type':'Top 20 types by output count plus aggregate other types; BCS bytes.',
          'input_object_popularity_rank_buckets':'Ranks refer to pooled reconstructed input ID frequencies; objects and accesses are counts within each rank range.',
          'input_version_age_checkpoints_conditional':'Only versions first written earlier within the same sampled window; unknown earlier versions are excluded.',
          'input_version_age_coverage':'Fraction of reconstructed input accesses whose exact version was written in the same window.',
          'window_censor_horizon_checkpoints':'Maximum possible checkpoint age within a sampled window; unknown older versions are excluded.',
          'input_version_age_transactions_conditional':'Same censoring, measured in transactions within a window.',
          'object_change_shares':'Counts of created, mutated, deleted, wrapped and unwrapped changes divided by their total; not a distribution over transactions.'}}
(root/'workload-model.json').write_text(json.dumps(model,indent=2)+'\n')
