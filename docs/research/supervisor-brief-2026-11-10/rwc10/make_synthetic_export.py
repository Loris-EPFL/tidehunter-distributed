#!/usr/bin/env python3
"""Build a clearly SYNTHETIC 80-job export; never invoke a benchmark or cloud API."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import sys

from rwc10_data import WARM, COLD


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=Path(__file__).resolve().parent / 'SYNTHETIC-export')
    args = parser.parse_args()
    if 'SYNTHETIC' not in args.output.name:
        parser.error('fixture output directory must contain SYNTHETIC')
    repo = Path(__file__).resolve().parents[4]
    sys.path.insert(0, str(repo / 'experiments/kv-comparison'))
    import cloud_campaign
    import fabric_ab
    jobs = cloud_campaign.make_focused_matrix('fabric_ab_compact')
    assert len(jobs) == 80 and not cloud_campaign.fabric_parity_violations(jobs)
    root = args.output.resolve(); root.mkdir(parents=True, exist_ok=True)

    def dump(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, indent=2) + '\n')

    cases = []
    for job in jobs:
        arm = fabric_ab._arm(job); warm = job['group'] == 'G16P'; rep = job['repeat']
        index = (WARM if warm else COLD).index(arm); busy = job.get('owner_polling') == 'busy'
        base = ((37, 31, 19, 29, 15) if warm else (108, 99, 90, 86, 113))[index]
        mean = (base + (25 if warm else 65) * (job['value_size'] / 65536)) * (1 + .6 * (job['concurrency'] == 16)) * (1 + .025 * rep)
        n = job['measurement_operations']
        samples = sorted(round(mean * 1000 * (1 + ((i % 21) - 10) * .008 + (.7 if i % 101 == 0 else 0))) for i in range(n))
        total = sum(samples); seconds = total / 1e9 / job['concurrency'] + .06
        case = dict(case_id=job['case_id'], job=job, status='completed', synthetic=True,
                    metrics=dict(success_calls_per_second=n / seconds, offered_calls=n, unsuccessful_calls=0,
                                 measurement_timeline=dict(api_time_sum_ns=total, duration_ns=seconds * 1e9),
                                 populations=[dict(operation='read', outcome='success', count=n, api_mean_ns=total/n,
                                                   api_ns=dict(p50=samples[math.ceil(.5*n)-1], p99=samples[math.ceil(.99*n)-1]))]))
        directory = root / 'cases' / job['case_id']
        label = {'tidehunter_tcp': 'tcp', 'tidehunter_tcp_trusted': 'tcp-trusted'}.get(job['backend'], 'ofi-verbs-trusted')
        security = 'mutual_tls' if job['backend'] == 'tidehunter_tcp' else 'trusted_network_no_tls'
        case['backend'] = dict(configuration=dict(transport_security=security))
        dump(directory/'result/manifest.json', dict(synthetic=True, backend_info=dict(configuration=dict(transport=label))))
        client = 1.02 + .03*rep + .04*(job['concurrency']==16) if busy else .06 + .01*index + .01*rep + .01*(job['concurrency']==16)
        owner = 1.0 + .04*rep + .03*(job['concurrency']==16) if busy else .07 + .008*index + .01*rep + .01*(job['concurrency']==16)
        dump(directory/'resource-interval.json', dict(synthetic=True, processes=[dict(process_role=role, available=True,
             cpu_seconds=cores*elapsed, elapsed_seconds=elapsed) for role,cores,elapsed in [('client',client,3.2),('storage-0',owner,3.4)]]))
        if job['backend'] == 'tidehunter_verbs_native':
            dump(directory/'result/measurement-read-evidence.json', dict(synthetic=True,
                 deltas=dict(owner_executor=dict(partitions={'0':dict(counters={'transport.cq_idle_waits':0 if busy else n+125+rep})}))))
        if not warm:
            # Deliberately retain one unqualified cell to exercise missing markers and pair suppression.
            unqualified = arm == 'nvmeof-rdma-spdk' and job['value_size']==65536 and job['concurrency']==16 and rep==1
            ratio = .35 if unqualified else .95 + .01*rep + (.05 if job['value_size']==65536 else 0)
            case['focused_evidence'] = dict(media_evidence=dict(status='unqualified' if unqualified else 'qualified',
                read_ops_per_success=ratio, reason_codes=['SYNTHETIC_low_device_reads'] if unqualified else []))
            if job['backend'] in ('tidehunter_nvme_rdma','tidehunter_nvme_tcp'):
                case['nvmeof_measurement_qualification'] = dict(qualified=True,reasons=[])
                case['sealed_read_accounting'] = dict(qualified=True,reasons=[])
        dump(directory/'receipt.json', dict(synthetic=True,case_id=job['case_id'],job=job,owner_transport_security_label=security))
        cases.append(case)
    matrix_hash = hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(',',':')).encode()).hexdigest()
    dump(root/'matrix.json', dict(synthetic=True,spec=dict(selection='fabric_ab_compact'),jobs=jobs))
    dump(root/'comparison.json', dict(synthetic=True,complete=True,cases=cases))
    dump(root/'SYNTHETIC-provenance.json', dict(synthetic=True,run_id='SYNTHETIC-run10',parent_run_id='SYNTHETIC-parent',
         package_sha256=hashlib.sha256(b'SYNTHETIC package, never launched').hexdigest(),source_revision='SYNTHETIC-not-a-revision',
         source_dirty=True,launch_time='SYNTHETIC-not-launched',vm_names=['SYNTHETIC-coordinator','SYNTHETIC-owner'],
         machine_type='z4m-highmem-16',matrix_sha256=matrix_hash))
    (root/'SYNTHETIC').write_text('Invented layout and pipeline fixture. NOT measurements.\n')
    print(f'SYNTHETIC: {len(jobs)} reviewed jobs; {matrix_hash}; {root}')


if __name__ == '__main__':
    main()
