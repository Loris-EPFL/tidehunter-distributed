#!/usr/bin/env python3
"""Admit explicit KV campaign selections."""
import argparse
import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'kv-comparison'))
import cloud_campaign

BACKENDS = {'native', 'tidehunter_tcp', 'tidehunter_tcp_trusted', 'tidehunter_verbs', 'tidehunter_verbs_native',
            'tidehunter_nvme_tcp', 'tidehunter_nvme_rdma',
            'tikv', 'cassandra', 'foundationdb', 'valkey_tcp', 'valkey_rdma', 'garnet', 'kvrocks'}


def specification(suite, backends, mode, selection=None):
    selected = backends.split(',')
    if suite != 'kv' or mode not in {'standard', 'nvmeof-focused', 'standard-with-nvmeof', 'standard-with-live-nvmeof', 'focused'}:
        raise ValueError('unsupported suite or matrix mode')
    if not selected or len(set(selected)) != len(selected) or any(x not in BACKENDS for x in selected):
        raise ValueError('select unique supported backend IDs; EFA is not a GCP backend')
    if mode == 'focused':
        import manual_campaign
        if (suite != 'kv' or selection not in cloud_campaign.FOCUSED_SELECTIONS
                or selected != manual_campaign.REVIEWED_SPECS[selection]['backends']):
            raise ValueError('focused mode requires the exact reviewed selection and backends')
        spec = copy.deepcopy(manual_campaign.REVIEWED_SPECS[selection])
        manual_campaign._reviewed_jobs(spec)
        if selection in (*cloud_campaign.M3_SELECTIONS, *cloud_campaign.TOOL_SELECTIONS):
            spec.setdefault("reservation_hours", 2)
        return spec
    if selection is not None:
        raise ValueError('selection is only valid in focused mode')
    if 'valkey_rdma' in selected and 'valkey_tcp' not in selected:
        raise ValueError('Valkey RDMA comparison requires its matched Valkey TCP control')
    if mode == 'standard' and any('nvme_' in x for x in selected):
        raise ValueError('NVMe backends require an NVMe-oF mode')
    spec = {'schema_version': 1, 'suite': suite, 'mode': mode, 'seed': 1775, 'repeats': 1,
            'value_sizes': [64, 256, 1024, 8192, 65536], 'backends': selected,
            'keys': 4096, 'max_cases': 2000, 'offered_rates': [1000, 10000, 50000],
            'offered_seconds': 8, 'case_deadline_seconds': 600, 'hard_deadline_seconds': 10800,
            'export_reserve_seconds': 600, 'diagnostics': {'write_profile_limit': 256, 'read_profile_limit': 256},
            # The strict industry comparison: explicit since native became the matrix default.
            'durability_tier': 'synchronous'}
    cloud_campaign.make_matrix({key: value for key, value in spec.items() if key != 'suite'})
    return spec


def deployment_configuration(config_path, image, qualification, output):
    config = json.loads(Path(config_path).read_text())
    if bool(image) != bool(qualification):
        raise ValueError('image and qualification must be supplied together')
    if image:
        if config.get('image') or config.get('image_qualification'):
            raise ValueError('image is configured in both repository and environment')
        parts = image.split('/')
        if (len(parts) != 5 or parts[:4] != ['projects', config['project_id'], 'global', 'images']
                or not re.fullmatch(r'[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?', parts[4])):
            raise ValueError('exact custom image in the configured project required')
        ref = json.loads(qualification)
        if (not isinstance(ref, dict) or set(ref) != {'bucket', 'object', 'generation', 'sha256'}
                or ref['bucket'] != config['bucket'] or not isinstance(ref['object'], str) or not ref['object']
                or len(ref['object']) > 1024 or '\x00' in ref['object']
                or not isinstance(ref['generation'], str) or not re.fullmatch(r'[1-9][0-9]*', ref['generation'])
                or not isinstance(ref['sha256'], str) or not re.fullmatch(r'[0-9a-f]{64}', ref['sha256'])):
            raise ValueError('generation- and SHA-pinned qualification in the configured bucket required')
        config.update(image=image, image_qualification=ref)
    with Path(output).open('x') as destination:
        json.dump(config, destination, sort_keys=True, indent=2)



def validate_policy(config_path, budget_path):
    import gcp_campaign
    config = json.loads(Path(config_path).read_text())
    budget = json.loads(Path(budget_path).read_text())
    duration = budget.get('max_duration_seconds')
    seconds = min(14400, duration - 1) if type(duration) is int and duration > 0 else 901
    deadline = (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()
    gcp_campaign.validate(config, budget, 'bench-' + '0' * 16, '0' * 64, deadline)
    return deadline

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    choose = sub.add_parser('spec')
    choose.add_argument('--suite', required=True)
    choose.add_argument('--backends', required=True)
    choose.add_argument('--mode', required=True)
    choose.add_argument('--selection', choices=cloud_campaign.FOCUSED_SELECTIONS)
    choose.add_argument('--output', type=Path, required=True)
    policy = sub.add_parser('policy')
    policy.add_argument('--config', type=Path, required=True)
    policy.add_argument('--budget', type=Path, required=True)
    policy.add_argument('--deadline-output', type=Path)
    configuration = sub.add_parser('configuration')
    configuration.add_argument('--config', type=Path, required=True)
    configuration.add_argument('--image', default='')
    configuration.add_argument('--qualification', default='')
    configuration.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.action == 'spec':
        value = specification(args.suite, args.backends, args.mode, args.selection)
        args.output.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    elif args.action == 'configuration':
        deployment_configuration(args.config, args.image, args.qualification, args.output)
    else:
        deadline = validate_policy(args.config, args.budget)
        if args.deadline_output:
            args.deadline_output.write_text(deadline)


if __name__ == '__main__':
    main()
