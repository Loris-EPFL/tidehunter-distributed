#!/usr/bin/env python3
"""Qualify the generated matrix against this source tree's offline observer.

This checks selection and waiting-state exposition only, not benchmark execution,
terminal measurements, cloud health, or the authenticity of this source tree.
"""
import hashlib
import json
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
KV = ROOT / 'experiments/kv-comparison'
sys.path.insert(0, str(KV))
sys.path.insert(0, str(KV / 'observability'))
import cloud_campaign
import campaign_exporter
import campaign_monitor


def matrix_digest(jobs):
    return hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def qualify_jobs(jobs):
    with tempfile.TemporaryDirectory(prefix='observer-matrix-') as directory:
        root = Path(directory)
        config = {'run_id': 'bench-0000000000000000', 'stale_seconds': 15}
        for job in jobs:
            try:
                campaign_monitor.validate_job(job)
                case = root / job['case_id']
                case.mkdir()
                result = campaign_monitor.result_path({'results_root': str(root)}, job, case / 'result')
                selection = {'job': job, 'results': str(result), 'terminal': None}
                if len((json.dumps(selection, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()) > campaign_exporter.MAX_SELECTION:
                    raise ValueError('selection exceeds the bounded size')
                payload = campaign_exporter.benchmark_payload(config, selection).decode('utf-8')
                lines = payload.splitlines()
                if not any(line.startswith('kv_observer_data_available{') and line.endswith(' 0') for line in lines):
                    raise ValueError('waiting selection did not report unavailable data')
                if any(line.startswith(('kv_case_success_calls{', 'kv_case_offered_calls{',
                                        'kv_case_api_quantile_seconds{')) for line in lines):
                    raise ValueError('waiting selection fabricated measurement samples')
            except Exception as exc:
                raise ValueError(f"observer qualification failed for {job.get('case_id', '<unknown>')}: {exc}") from exc
    return {'planned_cases': len(jobs), 'matrix_sha256': matrix_digest(jobs),
            'scope': 'offline selection and waiting-state rendering; no hardware or measurement evidence'}


def main():
    spec = json.load(sys.stdin)
    jobs = (cloud_campaign.make_focused_matrix(spec['selection'])
            if spec.get('mode') == 'focused'
            else cloud_campaign.make_matrix({key: value for key, value in spec.items() if key not in ('suite', 'reservation_hours')}))
    print(json.dumps(qualify_jobs(jobs), sort_keys=True))


if __name__ == '__main__':
    main()
