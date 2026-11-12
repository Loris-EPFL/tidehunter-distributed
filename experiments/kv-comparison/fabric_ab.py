#!/usr/bin/env python3
"""Analyze a single compact fabric A/B export, without pooling repetitions."""
import argparse
import csv
import json
import math
from collections import defaultdict
from pathlib import Path

import cloud_campaign
import focused_analysis

WARM = ('tcp-mtls-adaptive', 'tcp-trusted-adaptive', 'tcp-trusted-busy', 'verbs-adaptive', 'verbs-busy')
COLD = ('owner-tcp-trusted', 'owner-verbs', 'nvmeof-rdma-kernel', 'nvmeof-rdma-spdk', 'nvmeof-tcp-kernel')
WARM_PAIRS = (('TLS cost', WARM[0], WARM[1]), ('transport adaptive', WARM[1], WARM[3]),
              ('transport busy', WARM[2], WARM[4]), ('parking TCP', WARM[1], WARM[2]),
              ('parking verbs', WARM[3], WARM[4]))
COLD_PAIRS = tuple(('owner RPC vs direct namespace', a, b) for a in COLD[:2] for b in COLD[2:])
CASE_FIELDS = ('case_id', 'group', 'arm', 'backend', 'owner_polling', 'transport_label', 'security_label', 'size_bytes',
               'readers', 'read_lanes', 'repetition', 'n', 'mean_us', 'p50_us', 'p99_us',
               'calls_per_second', 'mean_inflight_calls', 'offered_calls', 'unsuccessful_calls',
               'client_cores', 'owner_cores', 'cq_idle_waits', 'polling_check',
               'device_reads_per_call', 'qualification', 'qualification_reasons',
               'nvme_path_qualified', 'sealed_accounting_qualified', 'status', 'error')
PAIR_FIELDS = ('group', 'comparison', 'size_bytes', 'readers', 'repetition', 'arm_a', 'arm_b',
               'case_a', 'case_b', 'status_a', 'status_b', 'parity', 'violations',
               'mean_a_us', 'mean_b_us', 'mean_ratio_a_over_b', 'client_cores_a', 'client_cores_b',
               'client_cores_b_minus_a', 'owner_cores_a', 'owner_cores_b', 'owner_cores_b_minus_a')
CAPTION = ('Caption: One fleet and two independent repetitions, not pooled; lane count is 1 at 1 reader and 4 at 16. '
           'Unmatched dimensions include TLS or trusted/no TLS, polling policy and owner RPC versus direct namespace reads. '
           'Busy is user-space spinning, not kernel busy poll; the coordinator read driver also spins under Busy. '
           'NVMe-oF owner control channel is verbs trusted. Client and owner cores are CPU seconds / elapsed seconds; '
           'hosts are sampled separately and intervals include gate overlap. For NVMe-oF, owner cores exclude kernel '
           'nvmet workers outside the cgroup. Missing counters are not recorded, not zero. '
           '[INFERENCE] Busy owner CPU below 0.5 cores is a screening flag, not proof of policy failure; '
           'higher CPU and zero CQ waits also do not prove the policy.')


def _load(path):
    return json.loads(path.read_text()) if path.is_file() else {}


def _arm(job):
    backend, polling, group = job.get('backend'), job.get('owner_polling'), job.get('group')
    if group == 'G16P':
        return {('tidehunter_tcp', 'adaptive'): WARM[0], ('tidehunter_tcp_trusted', 'adaptive'): WARM[1],
                ('tidehunter_tcp_trusted', 'busy'): WARM[2], ('tidehunter_verbs_native', 'adaptive'): WARM[3],
                ('tidehunter_verbs_native', 'busy'): WARM[4]}.get((backend, polling), 'unknown')
    return {('tidehunter_tcp_trusted', None): COLD[0], ('tidehunter_verbs_native', None): COLD[1],
            ('tidehunter_nvme_rdma', 'kernel'): COLD[2], ('tidehunter_nvme_rdma', 'spdk'): COLD[3],
            ('tidehunter_nvme_tcp', 'kernel'): COLD[4]}.get((backend, job.get('nvmeof_target')), 'unknown')


def _security(case, directory):
    manifest = _load(directory / 'result' / 'manifest.json')
    transport_label = ((manifest.get('backend_info') or {}).get('configuration') or {}).get('transport')
    if transport_label == 'tcp' or (isinstance(transport_label, str) and transport_label.startswith('ofi-') and transport_label.endswith('-mutual-tls')):
        return transport_label, 'mutual_tls'
    if transport_label == 'tcp-trusted' or (isinstance(transport_label, str) and transport_label.startswith('ofi-') and transport_label.endswith('-trusted')):
        return transport_label, 'trusted_network_no_tls'
    if transport_label is not None:
        return transport_label, 'not recorded'
    receipt = _load(directory / 'receipt.json')
    config = (case.get('backend') or {}).get('configuration') or {}
    label = (receipt.get('owner_transport_security_label') or receipt.get('transport_security')
             or receipt.get('security_label') or config.get('transport_security'))
    if label in ('mutual_tls', 'trusted_network_no_tls'):
        return None, label
    normalized = _load(directory / 'normalized-config.json')
    transport = ((normalized.get('backend') or {}).get('options') or {}).get('transport') or {}
    kind, security = transport.get('kind'), transport.get('security')
    if kind == 'tcp':
        return None, 'mutual_tls'
    if kind == 'tcp-trusted' or (kind == 'ofi' and security == 'trusted'):
        return None, 'trusted_network_no_tls'
    if kind == 'ofi' and security in ('mutual_tls', 'tls'):
        return None, 'mutual_tls'
    return None, 'not recorded'


def _cores(directory):
    entries = _load(directory / 'resource-interval.json').get('processes', [])
    result = {}
    for role in ('client', 'storage-0'):
        matches = [x for x in entries if x.get('process_role') == role]
        if len(matches) != 1:
            result[role] = None
            continue
        x = matches[0]; cpu = x.get('cpu_seconds'); elapsed = x.get('elapsed_seconds')
        result[role] = (cpu / elapsed if x.get('available') is True and
                        isinstance(cpu, (float, int)) and isinstance(elapsed, (float, int)) and
                        math.isfinite(cpu) and math.isfinite(elapsed) and cpu >= 0 and elapsed > 0 else None)
    return result


def _cq(directory, backend):
    # TCP does not expose an OFI CQ, even when a generic zero is present.
    if backend != 'tidehunter_verbs_native':
        return None
    evidence = _load(directory / 'result' / 'measurement-read-evidence.json')
    return (((((evidence.get('deltas') or {}).get('owner_executor') or {}).get('partitions') or {})
              .get('0') or {}).get('counters') or {}).get('transport.cq_idle_waits')


def _qualification(case, directory, nvme):
    media = (case.get('focused_evidence') or {}).get('media_evidence') or _load(directory / 'media_evidence.json')
    reads = media.get('read_ops_per_success')
    reasons = list(media.get('reason_codes') or [])
    valid = (media.get('status') == 'qualified' and isinstance(reads, (int, float)) and
             math.isfinite(reads) and reads >= .9)
    target = case.get('target_io_qualification')
    media_q = case.get('media_qualification')
    if media_q and media_q.get('status') != 'qualified':
        valid = False; reasons.extend(media_q.get('reason_codes') or [])
    if target and target.get('qualified') is not True:
        valid = False; reasons.extend(target.get('reasons') or [])
    path = case.get('nvmeof_measurement_qualification') or _load(directory / 'receipt.json').get('nvmeof_measurement_qualification') or {}
    accounting = case.get('sealed_read_accounting') or {}
    if nvme:
        if path.get('qualified') is not True:
            valid = False; reasons.extend(path.get('reasons') or ['nvme_path_not_qualified'])
        if accounting.get('qualified') is not True:
            valid = False; reasons.extend(accounting.get('reasons') or ['sealed_accounting_not_qualified'])
    if not media:
        reasons.append('media_evidence_missing')
    elif not isinstance(reads, (int, float)) or not math.isfinite(reads) or reads < .9:
        reasons.append('device_read_ratio_below_threshold_or_missing')
    return reads, ('qualified' if valid else 'unqualified'), ', '.join(dict.fromkeys(reasons)), path.get('qualified'), accounting.get('qualified')


def _row(case, export):
    job = case['job']; directory = export / 'cases' / case['case_id']
    status = case.get('status', 'not_run'); group = job['group']; backend = job.get('backend')
    transport_label, security = _security(case, directory)
    expected = {'tidehunter_tcp': 'tcp', 'tidehunter_tcp_trusted': 'tcp-trusted',
                'tidehunter_verbs_native': 'ofi-verbs-trusted',
                'tidehunter_nvme_rdma': 'ofi-verbs-trusted', 'tidehunter_nvme_tcp': 'ofi-verbs-trusted'}.get(backend)
    mismatch = transport_label is not None and transport_label != expected
    if status == 'completed' and mismatch:
        status = 'failed'
    row = dict.fromkeys(CASE_FIELDS)
    cores = _cores(directory); cq = _cq(directory, backend)
    polling = job.get('owner_polling')
    checks = []
    if backend == 'tidehunter_verbs_native':
        if cq is None:
            checks.append('CQ idle waits not recorded')
        elif polling == 'busy' and cq != 0:
            checks.append('unexpected parking: Busy CQ idle waits nonzero')
        elif polling == 'adaptive' and cq == 0:
            checks.append('suspect: Adaptive CQ idle waits zero')
        else:
            checks.append('CQ idle waits observed; not proof of polling policy')
    if polling == 'busy':
        owner_cores = cores['storage-0']
        if owner_cores is None:
            checks.append('Busy owner CPU not recorded')
        elif owner_cores < .5:
            checks.append('[INFERENCE] Busy owner CPU below 0.5 cores; inspect gate overlap')
        else:
            checks.append('Busy owner CPU observed; gate overlap prevents policy verification')
    check = '; '.join(checks) if checks else 'not applicable'
    row.update(case_id=case['case_id'], group=group, arm=_arm(job), backend=backend,
               owner_polling=polling if polling is not None else 'not applicable (NVMe-oF owner control adaptive)',
               transport_label=transport_label or 'not recorded', security_label=security, size_bytes=job.get('value_size'), readers=job.get('concurrency'),
               read_lanes=job.get('read_lanes'), repetition=job.get('repeat'), client_cores=cores['client'],
               owner_cores=cores['storage-0'], cq_idle_waits=cq if cq is not None else 'not recorded',
               polling_check=check, status=status, error=(f'transport mismatch: expected {expected}, observed {transport_label}' if mismatch else case.get('error')))
    if group == 'G16C' and status == 'completed':
        reads, verdict, reasons, path_ok, accounting_ok = _qualification(case, directory, backend in ('tidehunter_nvme_rdma', 'tidehunter_nvme_tcp'))
        row.update(device_reads_per_call=reads, qualification=verdict, qualification_reasons=reasons,
                   nvme_path_qualified=path_ok, sealed_accounting_qualified=accounting_ok)
        if verdict != 'qualified':
            row['status'] = 'unqualified'
    if row['status'] == 'completed':
        pop = focused_analysis.population(case, 'read'); metrics = case.get('metrics') or {}
        mean = pop.get('api_mean_ns'); timeline = metrics.get('measurement_timeline') or {}
        inflight = timeline.get('mean_observed_api_concurrency')
        if inflight is None and timeline.get('api_time_sum_ns') is not None and timeline.get('duration_ns'):
            inflight = timeline['api_time_sum_ns'] / timeline['duration_ns']
        row.update(n=pop.get('count'), mean_us=mean / 1000 if mean is not None else None,
                   p50_us=focused_analysis.latency(pop, 'api', 'p50'), p99_us=focused_analysis.latency(pop, 'api', 'p99'),
                   calls_per_second=metrics.get('success_calls_per_second'), mean_inflight_calls=inflight,
                   offered_calls=metrics.get('offered_calls'), unsuccessful_calls=metrics.get('unsuccessful_calls'))
    return row


def _pairs(rows, cases, group, definitions):
    cells = defaultdict(list)
    for row in rows:
        if row['group'] == group:
            cells[(row['size_bytes'], row['readers'], row['repetition'])].append(row)
    result = []
    for (size, readers, repeat), members in sorted(cells.items()):
        violations = cloud_campaign.fabric_parity_violations([cases[r['case_id']]['job'] for r in members], complete=False)
        for comparison, arm_a, arm_b in definitions:
            aa = [r for r in members if r['arm'] == arm_a]; bb = [r for r in members if r['arm'] == arm_b]
            a = aa[0] if len(aa) == 1 else {}; b = bb[0] if len(bb) == 1 else {}
            parity = 'matched' if not violations and a and b else 'unmatched'
            eligible = parity == 'matched' and a.get('status') == b.get('status') == 'completed'
            mean_a, mean_b = a.get('mean_us'), b.get('mean_us')
            ratio = mean_a / mean_b if eligible and mean_a is not None and mean_b else None
            def delta(field):
                left, right = a.get(field), b.get(field)
                return right - left if eligible and left is not None and right is not None else None
            result.append(dict(group=group, comparison=comparison, size_bytes=size, readers=readers, repetition=repeat,
                               arm_a=arm_a, arm_b=arm_b, case_a=a.get('case_id'), case_b=b.get('case_id'),
                               status_a=a.get('status', 'missing'), status_b=b.get('status', 'missing'), parity=parity,
                               violations='; '.join(map(str, violations)), mean_a_us=mean_a, mean_b_us=mean_b,
                               mean_ratio_a_over_b=ratio, client_cores_a=a.get('client_cores'), client_cores_b=b.get('client_cores'),
                               client_cores_b_minus_a=delta('client_cores'), owner_cores_a=a.get('owner_cores'),
                               owner_cores_b=b.get('owner_cores'), owner_cores_b_minus_a=delta('owner_cores')))
    return result


def _table(lines, title, fields, rows):
    lines.extend(['## ' + title, '', CAPTION])
    lines.extend(focused_analysis._md_table(fields, [[str(row.get(k)).replace('|', '\\|').replace('\n', ' ') if row.get(k) is not None else '—' for k in fields] for row in rows]))
    lines.append('')


def generate(export, report, output):
    export, report, output = map(Path, (export, report, output))
    matrix = json.loads((export / 'matrix.json').read_text()); jobs = matrix['jobs']
    cases = json.loads((report / 'comparison.json').read_text())['cases']
    planned = {job['case_id']: job for job in jobs}
    if (len(planned) != len(jobs) or len(cases) != len(jobs) or len({c['case_id'] for c in cases}) != len(cases)
            or any(c.get('job') != planned.get(c.get('case_id')) for c in cases)):
        raise ValueError('report cases do not match export matrix')
    case_by_id = {case['case_id']: case for case in cases}
    selected = [c for c in cases if c['job'].get('group') in ('G16P', 'G16C')]
    rows = [_row(c, export) for c in selected]
    warm = [r for r in rows if r['group'] == 'G16P']; cold = [r for r in rows if r['group'] == 'G16C']
    warm_pairs = _pairs(rows, case_by_id, 'G16P', WARM_PAIRS)
    cold_pairs = _pairs(rows, case_by_id, 'G16C', COLD_PAIRS)
    violations = cloud_campaign.fabric_parity_violations(jobs)
    lines = ['# Compact fabric A/B', '',
             'One export is one fleet. Successful API latency uses per-case populations and nearest-rank quantiles; '
             'throughput covers the entire case. Device-read counters are a proxy, not proof of physical service. '
             'Qualification, failures, and missing evidence are retained; no ratio is emitted for unqualified, '
             'failed, not-run or unmatched cases. CPU differences compare independent case intervals.', '',
             'Full matrix parity: ' + ('passed' if not violations else '; '.join(map(str, violations))), '']
    for title, fields, data in [('Warm cases', CASE_FIELDS, warm), ('Warm matched comparisons', PAIR_FIELDS, warm_pairs),
                                ('Cold cases', CASE_FIELDS, cold), ('Cold matched comparisons', PAIR_FIELDS, cold_pairs)]:
        _table(lines, title, fields, data)
    output.mkdir(parents=True, exist_ok=True)
    (output / 'fabric_ab.md').write_text('\n'.join(lines) + '\n')
    for name, fields, data in (('ab_warm', CASE_FIELDS, warm), ('ab_pairs', PAIR_FIELDS, warm_pairs),
                               ('cold_fabric', CASE_FIELDS, cold), ('cold_pairs', PAIR_FIELDS, cold_pairs),
                               ('parity_matrix', CASE_FIELDS, rows)):
        with (output / (name + '.csv')).open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields); writer.writeheader(); writer.writerows(data)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--export', required=True); parser.add_argument('--report', required=True); parser.add_argument('--output', required=True)
    args = parser.parse_args(); generate(args.export, args.report, args.output)


if __name__ == '__main__':
    main()
