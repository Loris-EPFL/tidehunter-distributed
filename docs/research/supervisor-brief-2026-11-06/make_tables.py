#!/usr/bin/env python3
"""Generate appendix tables directly from the audited final population CSV."""
import csv
from pathlib import Path
ROOT = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent
source = ROOT/'checkpoint-work/spdk-matched-storage-20261105T004442Z/final-full-report/all-populations.csv'
with source.open(newline='') as f: data = list(csv.DictReader(f))
backends = [('Native','native',''),('TH TCP','tidehunter_tcp',''),('TH verbs','tidehunter_verbs_native',''),('NVMe kernel live','tidehunter_nvme_rdma','kernel'),('NVMe kernel sealed','tidehunter_nvme_rdma','kernel'),('NVMe SPDK live','tidehunter_nvme_rdma','spdk'),('NVMe SPDK sealed','tidehunter_nvme_rdma','spdk'),('Valkey TCP','valkey_tcp',''),('Valkey RDMA','valkey_rdma','')]
profiles=['read-concurrent','write-concurrent','read-many32','write-batch32']
def cell(backend,target,profile,size,sealed):
    cohort='sealed_read' if sealed else 'primary_rf1'
    if sealed:
        profile={'read-concurrent':'first_touch-scalar-c16-baseline','read-many32':'first_touch-batch32-c16-baseline'}.get(profile)
        if profile is None: return '--'
    rows=[r for r in data if r['cohort']==cohort and r['profile']==profile and r['value_size']==str(size) and r['backend']==backend and r['target']==target and r['outcome']=='success']
    if not rows: return '--'
    if len(rows)!=1: raise ValueError((backend,target,profile,size,len(rows)))
    r=rows[0]
    return f"{int(r['api_p50_ns'])/1000:.1f}/{int(r['api_p99_ns'])/1000:.1f}/{float(r['success_calls_per_second']):.0f}"
text=['\\appendix','\\section{Full 1, 8 and 64 KiB comparison}',r'Each entry is API p50/p99 in \us\ followed by successful calls/s. Primary live, native and Valkey cells are c16, RF1; scalar read $n=8192$, scalar write $n=4096$, and batch $n=2048$. Sealed cells are separate first-touch c16 read cases, not matched to primary live; they have the same per-operation counts. Sealed writes were not measured in this cohort, and 64 KiB sealed c16 cases are absent. Valkey write-batch32 is unsupported. A dash means no comparable case, not zero. One repetition; cache, resource and persistence contracts differ.']
for size in (1024,8192,65536):
 text += [r'\begin{center}\scriptsize',r'\resizebox{\linewidth}{!}{%',r'\begin{tabular}{lrrrr}\toprule',r'Backend & Scalar read & Scalar write & Read-many32 & Write-batch32\\\midrule']
 for name,b,t in backends:
  vals=[cell(b,t,p,size,'sealed' in name) for p in profiles]
  text.append(name+' & '+' & '.join(vals)+r'\\')
 text += [r'\bottomrule\end{tabular}}\\[3pt]',f'{size//1024} KiB: successful API p50/p99 (\\us) / calls per second.',r'\end{center}']
(HERE/'appendix-tables.tex').write_text('\n'.join(text)+'\n')
