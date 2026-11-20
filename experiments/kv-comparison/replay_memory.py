"""Admission and evidence policy for the GCP replay client memory budget."""
import argparse
import hashlib
import json
from pathlib import Path
import re


MAX_ARTIFACT_BYTES = 16 * 1024**2
PRESSURE_EVENTS = ('max', 'high', 'oom', 'oom_kill', 'oom_group_kill')


def workload_sha256(job, fixture_sha256):
    workload = {k: v for k, v in job.items() if k not in ('case_id', 'repeat')}
    return hashlib.sha256(json.dumps([fixture_sha256, workload], sort_keys=True,
                                     separators=(',', ':')).encode()).hexdigest()


def observation(client, cap_bytes):
    """Use all sampled counters, including pressure before the first sample."""
    if type(cap_bytes) is not int or cap_bytes <= 0:
        raise ValueError('invalid replay client memory cap')
    if client.get('available') is not True or not client.get('samples'):
        raise ValueError('replay client memory samples unavailable')
    peak = 0
    reasons = set()
    for sample in client['samples']:
        if type(sample['memory.max']) is not int or sample['memory.max'] != cap_bytes:
            raise ValueError('replay client memory.max differs from admitted cap')
        peak = max(peak, sample['memory.current'], sample['memory.peak'] or 0)
        events = sample['memory.events']
        if not {'max', 'high', 'oom'} <= events.keys():
            raise ValueError('replay client memory.events lacks required pressure counters')
        reasons.update(name for name in PRESSURE_EVENTS if events.get(name, 0) > 0)
    if peak * 100 >= cap_bytes * 95:
        reasons.add('peak_within_5_percent_of_cap')
    return {'peak_bytes': peak, 'censored': bool(reasons), 'reasons': sorted(reasons)}


def read_artifact(path, sha256):
    with path.open('rb') as src:
        data = src.read(MAX_ARTIFACT_BYTES + 1)
    if len(data) > MAX_ARTIFACT_BYTES or hashlib.sha256(data).hexdigest() != sha256:
        raise ValueError(f'invalid replay memory artifact size or digest: {path}')
    return json.loads(data)


def assess(artifact, admission):
    if artifact['admission'] != admission or artifact['measurement_completed'] is not True:
        raise ValueError('replay memory admission or completion differs from artifact')
    result = observation(artifact['client'], admission['cap_bytes'])
    prior = admission['qualified_peak_bytes']
    if prior is not None and (type(prior) is not int or prior <= 0
                              or admission['cap_bytes'] * 4 < prior * 5):
        raise ValueError('invalid replay memory headroom')
    status = 'censored' if result['censored'] else 'exploratory' if prior is None else 'qualified'
    return {**admission, **result, 'status': status}


def admit(policy, jobs, repository, cap_bytes):
    """Return reviewed uncensored peaks. Unlisted workloads are exploratory."""
    if not isinstance(policy, dict):
        raise ValueError('replay_memory must map workload digests to reviewed evidence')
    if not policy:
        return {}
    source = Path(repository).resolve(strict=True)
    fixture = source / 'experiments/sui-mainnet/fixture/manifest.json'
    fixture_sha256 = hashlib.sha256(fixture.read_bytes()).hexdigest()
    workloads = {workload_sha256(job, fixture_sha256) for job in jobs
                 if job.get('case_kind') == 'mainnet_replay'}
    peaks = {}
    for workload, entry in policy.items():
        if (workload not in workloads or not isinstance(entry, dict)
                or set(entry) != {'qualified_peak_bytes', 'evidence'}
                or not isinstance(entry['evidence'], list) or not entry['evidence']):
            raise ValueError(f'invalid replay_memory entry for {workload}')
        observed = []
        for binding in entry['evidence']:
            if (not isinstance(binding, dict) or set(binding) != {'path', 'sha256'}
                    or not isinstance(binding['path'], str) or not isinstance(binding['sha256'], str)
                    or not re.fullmatch(r'[a-f0-9]{64}', binding['sha256'])):
                raise ValueError('replay_memory evidence requires path and SHA-256')
            relative = Path(binding['path'])
            path = (source / relative).resolve(strict=True)
            if relative.is_absolute() or not path.is_relative_to(source):
                raise ValueError('replay_memory evidence must be inside the sealed source')
            artifact = read_artifact(path, binding['sha256'])
            budget = artifact['admission']
            evidence_cap = budget['cap_bytes']
            if budget['workload_sha256'] != workload or artifact['measurement_completed'] is not True:
                raise ValueError('replay_memory evidence has a different workload or incomplete gate')
            result = observation(artifact['client'], evidence_cap)
            if result['censored'] or result['peak_bytes'] <= 0:
                raise ValueError('replay_memory evidence is censored or has no measured peak')
            observed.append(result['peak_bytes'])
        largest = max(observed)
        if type(entry['qualified_peak_bytes']) is not int or largest != entry['qualified_peak_bytes']:
            raise ValueError('replay_memory qualified_peak_bytes differs from largest measured peak')
        if cap_bytes * 4 < largest * 5:
            raise ValueError('replay_memory cap needs at least 25% headroom above the measured peak')
        peaks[workload] = largest
    return peaks


def classify_archive(root):
    """Classify receipt-bound land4 samples without changing the archived report."""
    root = Path(root)
    matrix = json.loads((root / 'matrix.json').read_text())
    rows = []
    for job in matrix['jobs']:
        if job.get('case_kind') != 'mainnet_replay':
            continue
        case = job['case_id']
        if not re.fullmatch(r'[A-Za-z0-9_-]+', case):
            raise ValueError(f'invalid archived replay case ID: {case!r}')
        directory = root / 'cases' / case
        row = {'case_id': case}
        try:
            receipt = json.loads((directory / 'receipt.json').read_text())
            if receipt['job'] != job or receipt['status'] != 'completed':
                raise ValueError('replay receipt differs from matrix or case did not complete')
            binding = receipt['cgroup_memory_evidence']
            if binding['path'] != 'cgroup-memory.json':
                raise ValueError('invalid archived cgroup artifact path')
            client = read_artifact(directory / binding['path'], binding['sha256'])['client']
            cap = client['samples'][0]['memory.max']
            observed = observation(client, cap)
            row.update(observed, cap_bytes=cap,
                       status='censored' if observed['censored'] else 'valid')
        except (OSError, ValueError, KeyError, TypeError, IndexError) as exc:
            row.update(status='memory_unqualified', reason=str(exc))
        rows.append(row)
    return rows


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Classify saved replay cgroup evidence without modifying it.')
    parser.add_argument('results', type=Path, help='archive directory containing matrix.json and cases/')
    print(json.dumps(classify_archive(parser.parse_args().results), indent=2))
