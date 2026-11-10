#!/usr/bin/env python3
"""Exercise a real local observer capture/replay with a GCP-shaped run identity."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import time

import campaign_monitor as monitor


def require(receipt, stage):
    if not receipt.get('complete'):
        raise RuntimeError(stage + ' incomplete: ' + json.dumps(receipt.get('gaps', [])))
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True,
                        help='new absolute directory on root filesystem; evidence is retained')
    args = parser.parse_args()
    root = args.output
    if not root.is_absolute() or root.exists():
        parser.error('--output must be a new absolute directory')
    root.mkdir(mode=0o700)
    results = root / 'results'
    results.mkdir()
    run = 'bench-b35a62e24b47faaf'
    spec = {'schema_version': 1, 'run_id': run, 'state_dir': str(root / 'live'),
            'results_root': str(results),
            'deadline_utc': datetime.fromtimestamp(time.time() + 900, timezone.utc).isoformat(),
            'hosts': [{'index': i, 'role': 'coordinator' if i == -1 else f'storage-{i}',
                       'address': f'127.0.0.{i + 3}'} for i in range(-1, 5)],
            'ports': {'prometheus': 19090, 'grafana': 13000, 'benchmark': 19899,
                      'node': 19100, 'host': 19898}, 'allow_loopback_hosts': True}
    job = {'case_id': 'observer-qualification', 'backend': 'native', 'profile': 'read',
           'cohort': 'warm', 'value_size': 64, 'repeat': 0}
    case = results / 'cases' / job['case_id']
    case.mkdir(parents=True)
    archive = root / 'monitoring.tar.gz'
    replay_state = root / 'replay'
    replay_spec = None
    cleanup_errors = []
    try:
        monitor.atomic_json(root / 'spec.json', spec)
        require(monitor.start_local(spec), 'online startup')
        monitor.select_case(spec, job, case / 'result')
        # Deliberately failed fixture: tests case-label plumbing without invented benchmark metrics.
        require(monitor.finish_case(spec, job, case / 'result', status='failed'), 'terminal observation')
        # Host exporters scrape every five seconds; wait for first full target population.
        time.sleep(7)
        live = require(monitor.verify(spec), 'online verification')
        if job['case_id'] not in live['evidence']['actual_seen_case_ids']:
            raise RuntimeError('online verification did not observe qualification case')
        monitor.atomic_json(root / 'live-verification.json', live)
        require(monitor.export_session(spec, archive), 'capture')
        require(monitor.stop_collector(spec), 'live collector cleanup')
        for host in spec['hosts']:
            require(monitor.stop_host(spec, host['index']), 'live host cleanup')
        replay = require(monitor.replay(archive, replay_state), 'offline replay')
        replay_spec = replay['spec']
        if job['case_id'] not in replay['verification']['evidence']['actual_seen_case_ids']:
            raise RuntimeError('offline verification did not retain qualification case')
        monitor.atomic_json(root / 'replay-verification.json', replay['verification'])
    finally:
        # Startup guards also clean partial starts; exact-owner stop remains safe to retry.
        for candidate, hosts in ((spec, spec['hosts']), (replay_spec, [])):
            if candidate is None and (replay_state / 'spec.json').is_file():
                candidate = monitor.load_json(replay_state / 'spec.json')
            if candidate is None or not (Path(candidate['state_dir']) / 'owner.json').is_file():
                continue
            actions = [('collector', lambda: monitor.stop_collector(candidate))]
            actions.extend((f'host-{host["index"]}', lambda index=host['index']: monitor.stop_host(candidate, index))
                           for host in hosts)
            for name, action in actions:
                try:
                    require(action(), name + ' cleanup')
                except Exception as error:
                    cleanup_errors.append(name + ': ' + str(error))
        if cleanup_errors:
            raise RuntimeError('observer cleanup incomplete: ' + '; '.join(cleanup_errors))
    print(json.dumps({'complete': True, 'output': str(root), 'run_id': run,
                      'case_id': job['case_id'], 'case_status': 'failed fixture',
                      'scope': 'local shared-kernel observer functionality; no cloud or performance claim'}))


if __name__ == '__main__':
    main()
