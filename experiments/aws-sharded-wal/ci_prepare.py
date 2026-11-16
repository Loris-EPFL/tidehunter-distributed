#!/usr/bin/env python3
"""Admit protected workflow settings without obtaining credentials."""
import argparse
import json
import os
import re

REPOSITORY = 'Loris-EPFL/tidehunter-distributed'
BRANCH = 'bench/aws-efa'


def trusted_environment(env):
    if (env.get('GITHUB_REPOSITORY') != REPOSITORY
            or env.get('GITHUB_REF') != 'refs/heads/' + BRANCH
            or env.get('GITHUB_EVENT_NAME') not in ('push', 'workflow_dispatch')):
        raise ValueError('AWS benchmark preparation requires the trusted repository and benchmark branch')
    if not re.fullmatch(r'[0-9a-f]{40}', env.get('GITHUB_SHA', '')):
        raise ValueError('exact engine commit SHA required')


def settings(env):
    trusted_environment(env)
    if env.get('AWS_BENCH_ENABLED') != 'true':
        return {'enabled': False, 'reason': 'AWS benchmarks are disarmed; no AWS operation or build requested'}
    required = ['AWS_BENCH_ROLE_ARN', 'AWS_BENCH_BUCKET', 'AWS_BENCH_BUCKET_REGION']
    if env.get('AWS_BENCH_OPERATION', 'benchmark') != 'cost-report':
        required += ['AWS_BENCH_GUEST_BOUNDARY_ARN',
                     'AWS_BENCH_WATCHDOG_FUNCTION', 'AWS_BENCH_WATCHDOG_RULE']
    missing = [key for key in required if not env.get(key)]
    if missing:
        raise ValueError('missing GitHub environment variables: ' + ', '.join(missing))
    from ci_budget import load_budget
    budget = load_budget()
    return {'enabled': True, 'engine_sha': env['GITHUB_SHA'],
            'budget_policy_sha256': budget['sha256']}




def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('settings',))
    parser.parse_args()
    value = settings(os.environ)
    print(json.dumps(value))
    if os.environ.get('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as stream:
            stream.write('enabled=' + str(value['enabled']).lower() + '\n')


if __name__ == '__main__':
    main()
