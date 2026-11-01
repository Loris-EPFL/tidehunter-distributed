#!/usr/bin/env python3
"""Admit explicit campaign selections and retrieve an immutable real fixture."""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'kv-comparison'))
import cloud_campaign

BACKENDS = {'native', 'tidehunter_tcp', 'tidehunter_nvme_tcp', 'tidehunter_nvme_rdma',
            'tikv', 'cassandra', 'foundationdb', 'valkey_tcp'}
FIXTURE_SHA = '7eb43b71435547939760598e541d13b9a77f722c5a9d0f8c8ee5bd583b751f15'


def specification(suite, backends, mode, fixture=None, sui_transports='tcp'):
    selected = backends.split(',')
    if suite not in {'kv', 'sui', 'both'} or mode not in {'standard', 'nvmeof-focused'}:
        raise ValueError('unsupported suite or matrix mode')
    if not selected or len(set(selected)) != len(selected) or any(x not in BACKENDS for x in selected):
        raise ValueError('select unique supported backend IDs; EFA is not a GCP backend')
    if mode != 'nvmeof-focused' and any('nvme_' in x for x in selected):
        raise ValueError('NVMe backends require nvmeof-focused mode')
    spec = {'schema_version': 1, 'suite': suite, 'mode': mode, 'seed': 1775, 'repeats': 1,
            'value_sizes': [64, 256, 1024, 8192, 65536], 'backends': selected,
            'keys': 4096, 'max_cases': 2000, 'offered_rates': [1000, 10000, 50000],
            'offered_seconds': 8, 'case_deadline_seconds': 600, 'hard_deadline_seconds': 10800,
            'export_reserve_seconds': 600, 'diagnostics': {'write_profile_limit': 256, 'read_profile_limit': 256}}
    if suite in {'kv', 'both'}:
        cloud_campaign.make_matrix({key: value for key, value in spec.items() if key != 'suite'})
    if suite in {'sui', 'both'}:
        if not fixture or not fixture.get('key') or not re.fullmatch('[1-9][0-9]*', str(fixture.get('generation', ''))) or fixture.get('sha256') != FIXTURE_SHA:
            raise ValueError('Sui requires the exact reviewed real fixture key, generation and SHA-256')
        transports = sui_transports.split(',')
        supported = {'tcp', 'tidehunter_verbs', 'tidehunter_verbs_native'}
        if not transports or len(transports) > 4 or len(set(transports)) != len(transports) or any(t not in supported for t in transports):
            raise ValueError('unsupported or duplicate Sui transport')
        spec['sui_transports'] = transports
        spec['sui_fixture'] = fixture
    return spec


def download_fixture(config_path, spec_path, destination):
    from gcp_api import GoogleApi
    config = json.loads(Path(config_path).read_text())
    fixture = json.loads(Path(spec_path).read_text())['sui_fixture']
    destination = Path(destination)
    if destination.exists():
        raise ValueError('fixture destination already exists')
    GoogleApi(config['project_id']).objects(config['bucket']).download(
        fixture['key'], destination, generation=fixture['generation'], max_bytes=1 << 30)
    with destination.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    if digest != fixture['sha256']:
        destination.unlink()
        raise ValueError('downloaded fixture SHA-256 differs')


def validate_policy(config_path, budget_path):
    import gcp_campaign
    config = json.loads(Path(config_path).read_text())
    budget = json.loads(Path(budget_path).read_text())
    duration = budget.get('max_duration_seconds')
    seconds = min(14400, duration - 1) if type(duration) is int and duration > 0 else 901
    deadline = (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()
    gcp_campaign.validate(config, budget, 'th-' + '0' * 16, '0' * 64, deadline)
    return deadline

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    choose = sub.add_parser('spec')
    choose.add_argument('--suite', required=True)
    choose.add_argument('--backends', required=True)
    choose.add_argument('--mode', required=True)
    choose.add_argument('--fixture-key', default='')
    choose.add_argument('--fixture-generation', default='')
    choose.add_argument('--output', type=Path, required=True)
    download = sub.add_parser('fixture')
    download.add_argument('--config', type=Path, required=True)
    choose.add_argument('--sui-transports', default='tcp')
    download.add_argument('--spec', type=Path, required=True)
    download.add_argument('--output', type=Path, required=True)
    policy = sub.add_parser('policy')
    policy.add_argument('--config', type=Path, required=True)
    policy.add_argument('--budget', type=Path, required=True)
    policy.add_argument('--deadline-output', type=Path)
    args = parser.parse_args()
    if args.action == 'spec':
        fixture = {'key': args.fixture_key, 'generation': args.fixture_generation, 'sha256': FIXTURE_SHA}
        value = specification(args.suite, args.backends, args.mode, fixture, args.sui_transports)
        args.output.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    elif args.action == 'fixture':
        download_fixture(args.config, args.spec, args.output)
    else:
        deadline = validate_policy(args.config, args.budget)
        if args.deadline_output:
            args.deadline_output.write_text(deadline)


if __name__ == '__main__':
    main()
