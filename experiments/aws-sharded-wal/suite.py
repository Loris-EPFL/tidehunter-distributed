#!/usr/bin/env python3
"""Select exactly one protected campaign; this helper never obtains credentials."""
import argparse
import json
import os
from pathlib import Path

import ci_prepare
from ci_budget import load_budget

SUITES = ('sui-transport', 'kv-comparison')
CONFIG = Path(__file__).with_name('suite.json')


def selection(path=CONFIG, override='configured', event=None):
    config = json.loads(Path(path).read_text())
    if (set(config) != {'schema_version', 'suite'} or type(config['schema_version']) is not int
            or config['schema_version'] != 1 or config['suite'] not in SUITES):
        raise ValueError('suite selector must name exactly one known campaign')
    if override not in ('configured', *SUITES):
        raise ValueError('unknown campaign override')
    if override != 'configured' and event != 'workflow_dispatch':
        raise ValueError('only manual dispatch may override the committed suite')
    return config['suite'] if override == 'configured' else override


def settings(env, path=CONFIG):
    ci_prepare.trusted_environment(env)
    suite = selection(path, env.get('AWS_BENCH_SUITE', 'configured'), env.get('GITHUB_EVENT_NAME'))
    if suite == 'sui-transport':
        return ci_prepare.settings(env) | {'suite': suite}
    if env.get('AWS_BENCH_ENABLED') != 'true':
        return {'enabled': False, 'suite': suite, 'reason': 'AWS benchmarks are disarmed'}
    required = ('AWS_BENCH_ROLE_ARN', 'AWS_BENCH_BUCKET', 'AWS_BENCH_BUCKET_REGION',
                'AWS_BENCH_GUEST_BOUNDARY_ARN', 'AWS_BENCH_WATCHDOG_FUNCTION', 'AWS_BENCH_WATCHDOG_RULE')
    missing = [key for key in required if not env.get(key)]
    if missing:
        raise ValueError('missing GitHub environment variables: ' + ', '.join(missing))
    return {'enabled': True, 'suite': suite, 'engine_sha': env['GITHUB_SHA'],
            'budget_policy_sha256': load_budget()['sha256']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['settings'])
    parser.parse_args()
    result = settings(os.environ)
    if os.environ.get('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as stream:
            for key in ('enabled', 'suite'):
                value = str(result[key]).lower() if isinstance(result[key], bool) else result[key]
                stream.write(f'{key}={value}\n')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
