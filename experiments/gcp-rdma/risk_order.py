"""Validate a risk-first permutation without splitting retained sealed datasets."""
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'kv-comparison'))
import cloud_campaign


def dataset_identity(job):
    dataset = job.get('sealed_dataset')
    if not dataset or job['backend'] == 'native':
        return None
    backend = job['backend']
    transport = 'tcp' if backend == 'tidehunter_tcp' or backend in cloud_campaign.NVME_BACKENDS else backend
    return (dataset['source_id'], transport, cloud_campaign.durability_tier(job))


def validate_order(jobs, document):
    digest = hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if document.get('matrix_sha256') != digest or document.get('planned_cases') != len(jobs):
        raise ValueError('order does not bind the admitted matrix')
    original = [job['case_id'] for job in jobs]
    ordered = document.get('case_ids')
    if (not isinstance(ordered, list) or len(set(original)) != len(original)
            or len(ordered) != len(original) or len(set(ordered)) != len(ordered)
            or set(ordered) != set(original)):
        raise ValueError('order must be an exact matrix permutation')
    if jobs and jobs[0].get('selection') == 'fair_native' and ordered[0] != original[0]:
        raise ValueError('fair native canary must run first')
    blocks = {}
    for job in jobs:
        identity = dataset_identity(job)
        if identity is not None:
            blocks.setdefault(identity, []).append(job['case_id'])
    positions = {case: index for index, case in enumerate(ordered)}
    for cases in blocks.values():
        actual = sorted(positions[case] for case in cases)
        if actual != list(range(actual[0], actual[0] + len(cases))):
            raise ValueError('sealed dataset block is not contiguous')
        if [ordered[index] for index in actual] != cases:
            raise ValueError('sealed dataset read order differs from admitted matrix')
    # Native write-regression deletes native retained sealed data. Never move a
    # native sealed reader past its first such cleanup boundary.
    native_readers = [job['case_id'] for job in jobs if job['backend'] == 'native' and 'sealed_dataset' in job]
    cleanup = [job['case_id'] for job in jobs if job.get('cohort') == 'native_write_regression']
    if native_readers and cleanup and max(positions[c] for c in native_readers) > min(positions[c] for c in cleanup):
        raise ValueError('native sealed read occurs after write-regression cleanup')
    return True


def generate_order(jobs):
    """Prioritize live storage risks without splitting or reordering sealed sources."""
    blocks, seen = [], {}
    for job in jobs:
        identity = dataset_identity(job)
        if identity is not None and identity in seen:
            seen[identity].append(job)
        else:
            block = [job]
            blocks.append(block)
            if identity is not None:
                seen[identity] = block

    def priority(block):
        job = block[0]
        if job.get('nvmeof_route') == 'live' and job.get('cohort') == 'cold_os_cache':
            return 0
        if (job.get('nvmeof_route') == 'live' and job.get('cohort') == 'primary_rf1'
                and job.get('value_size') == 1024 and job.get('profile') == 'write-concurrent'):
            return 1
        if job.get('sealed_dataset'):
            return 2
        return 3

    # G14N and G14S cover one and five native Dbs; G14O1 and G14O5
    # cover one and five RPC owners. G14P profiles paired c1 and c16 writes.
    # Preserve the reviewed order and run the native group-barrier canary first.
    ordered = ([job['case_id'] for job in jobs] if jobs and all(job.get('selection') in ('device', 'floors', 'stages', 'unified', 'append_floors', 'fair_native', 'parity', 'parity_compact', 'fair_five_owner') for job in jobs)
               else [job['case_id'] for block in sorted(blocks, key=priority) for job in block])
    document = {
        'matrix_sha256': hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
        'planned_cases': len(jobs), 'case_ids': ordered,
    }
    validate_order(jobs, document)
    return document


def main():
    import argparse
    import manual_campaign

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('order', type=Path)
    parser.add_argument('--generate', action='store_true', help='create a new order file without overwriting')
    parser.add_argument('--selection', choices=cloud_campaign.FOCUSED_SELECTIONS,
                        help='use a reviewed focused matrix instead of the original campaign')
    args = parser.parse_args()
    if args.selection is None:
        spec = {key: value for key, value in manual_campaign.REVIEWED_SPEC.items() if key != 'suite'}
        jobs = cloud_campaign.make_matrix(spec)
    else:
        jobs = manual_campaign._reviewed_jobs(manual_campaign.REVIEWED_SPECS[args.selection])
    if args.generate:
        with args.order.open('x') as stream:
            json.dump(generate_order(jobs), stream, indent=2, sort_keys=True)
            stream.write('\n')
    else:
        validate_order(jobs, json.loads(args.order.read_text()))
    print(json.dumps({'valid': True, 'cases': len(jobs)}, sort_keys=True))


if __name__ == '__main__':
    main()
