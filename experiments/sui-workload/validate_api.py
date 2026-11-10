#!/usr/bin/env python3
"""Compare downloaded checkpoint counts and timestamps with saved GraphQL results."""
import csv
import datetime
import json
import pathlib
import sys

root=pathlib.Path(sys.argv[1])
result=json.loads((root/'graphql-validation.json').read_text())['response']['data']
if '--raw' in sys.argv:
    from analyze import read_checkpoint
    checkpoints={}
    for i in range(len(result)//2):
        seq=result[f'c{2*i+1}']['sequenceNumber']
        actual,summary,transactions,_=read_checkpoint(root/'raw'/f'{seq}.binpb.zst')
        if actual!=seq: raise ValueError('checkpoint sequence mismatch')
        checkpoints[seq]={'transactions':len(transactions),'timestamp_ms':summary['timestamp_ms']}
else:
    with (root/'checkpoints.csv').open(newline='') as f:
        checkpoints={int(r['sequence']):r for r in csv.DictReader(f)}
out=[]
for i in range(len(result)//2):
    previous=result[f'c{2*i}']; current=result[f'c{2*i+1}']; seq=current['sequenceNumber']
    assert previous['sequenceNumber']==seq-1
    row=checkpoints[seq]
    count=current['networkTotalTransactions']-previous['networkTotalTransactions']
    timestamp=int(datetime.datetime.fromisoformat(current['timestamp'].replace('Z','+00:00')).timestamp()*1000)
    matches=count==int(row['transactions']) and timestamp==int(row['timestamp_ms'])
    out.append([seq,row['transactions'],count,row['timestamp_ms'],timestamp,matches])
with (root/'api_validation.csv').open('w',newline='') as f:
    writer=csv.writer(f);writer.writerow(['sequence','archive_transactions','graphql_transactions','archive_timestamp_ms','graphql_timestamp_ms','match']);writer.writerows(out)
if not all(r[-1] for r in out): raise ValueError('GraphQL validation failed')
print(f'{len(out)} checkpoint counts and timestamps match')
