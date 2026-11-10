#!/usr/bin/env python3
"""Audit exported metrics and selected raw evidence; never rewrite producer percentiles.

Usage from report root: python data/audit_raw.py CAMPAIGN_ROOT KV_COMPARISON_SOURCE
The source directory must contain the measured analyze.py and local.py.
"""
import collections, csv, hashlib, importlib, json, math, sys
from pathlib import Path
root=Path(sys.argv[1]); sys.path.insert(0,sys.argv[2]); analyze=importlib.import_module('analyze')
report=Path('data'); document=json.loads((root/'aws-efa/validated-results/comparison.json').read_text())
rows=list(csv.DictReader((report/'all-results.csv').open()))
cases={c['case_id']:c for c in document['cases']}; mismatches=[]
for row in rows:
    c=cases[row['case_id']]; m=c['metrics']; matching=[p for p in m['populations'] if p['operation']==row['operation'] and p['outcome']==row['outcome']]
    if len(matching)!=1: mismatches.append([row['case_id'],'identity']); continue
    p=matching[0]
    for col,key in [('population_calls','count'),('population_logical_keys','logical_keys')]:
        if int(row[col])!=p[key]: mismatches.append([row['case_id'],col,row[col],p[key]])
    for kind in ('api','scheduling','response'):
        summary=p[kind+'_ns']
        for field in ('mean','stddev','min','p50','p95','p99','p999','max','sample_count'):
            actual=row[kind+'_'+field+('' if field=='sample_count' else '_us')]
            expected=(summary[field] if field in ('p50','p95','p99','p999','max','sample_count') else p[kind+'_'+field+'_ns'])
            if expected is not None and field!='sample_count': expected/=1000
            if (expected is None and actual!='') or (expected is not None and (not actual or not math.isclose(float(actual),expected,rel_tol=1e-12,abs_tol=1e-9))): mismatches.append([row['case_id'],kind,field,actual,expected])
    for col,key in [('observed_calls','offered_calls'),('successful_calls','success_calls'),('unsuccessful_calls','unsuccessful_calls'),('elapsed_seconds','elapsed_seconds'),('case_success_calls_per_second','success_calls_per_second'),('case_success_logical_keys_per_second','success_logical_keys_per_second'),('case_success_value_mib_per_second','success_value_mib_per_second')]:
        if not math.isclose(float(row[col]),m[key],rel_tol=1e-12,abs_tol=1e-9): mismatches.append([row['case_id'],col,row[col],m[key]])
ids=['0180-native-1024-read-concurrent-primary_rf1-r0','0063-tidehunter_efa_native-1024-read-concurrent-primary_rf1-r0','0000-foundationdb-8192-read-many32-primary_rf1-r0','0245-cassandra-1024-offered-10000-primary_rf1-r0']
raw=[]
for case_id in ids:
    path=root/'aws-efa/validated-results/cases'/case_id/'result/samples.csv'; observed=analyze.aggregate(analyze.read_samples(path)); recorded=cases[case_id]['metrics']; errors=[]
    for key in ('offered_calls','success_calls','unsuccessful_calls'):
        if observed[key]!=recorded[key]: errors.append(key)
    for p in observed['populations']:
        original=next(q for q in recorded['populations'] if q['operation']==p['operation'] and q['outcome']==p['outcome'])
        for key in ('count','logical_keys','api_ns','scheduling_ns','response_ns'):
            if p[key]!=original[key]: errors.append(p['operation']+'/'+p['outcome']+'/'+key)
    raw.append({'case_id':case_id,'source':str(path.relative_to(root)),'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'measurement_rows':observed['offered_calls'],'errors':errors})
result={'exported_population_rows':len(rows),'case_count':len(cases),'csv_comparison_mismatches':mismatches,'raw_audit':raw,'method':'analyze.py measurement-only; validate per-row monotonic clocks and dispatch presence, nearest-rank producer quantiles by exact operation/outcome, compare all three clock summaries and counts. No quantile recalculated for publication.'}
(report/'audit.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
if mismatches or any(x['errors'] for x in raw): raise SystemExit('audit mismatch')
