#!/usr/bin/env python3
"""Run host-local kernel NVMe/TCP owners through cloud harness helpers.

Functional evidence only: one host, file backing, TCP control, no RDMA claim.
Run in the documented privileged host namespace with the project Python.
"""
import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
import traceback
import uuid

import cloud_campaign
import cloud_controller as cloud
from local_dry_run import LocalMode, LocalObjects, save_json


class MultiownerMode(LocalMode):
    def request(self, index, request, *, case, timeout, export):
        if request['action'] == 'tide_slice_cleanup':
            return cloud.cleanup_owner_slice(self.config, case, index)
        logical = request.get('owner_index', index)
        return super().request(logical, request, case=case, timeout=timeout, export=export)

    def start_owner(self, index, request, case, timeout):
        cloud.configure_owner_slice(self.config, case)
        proof = super().start_owner(index, request, case, timeout)
        name = cloud.unit_name(self.config, case, f'storage-{index}')
        proof['slice_observed'] = cloud.command(['systemctl', 'show', name,
            '--property=Slice,ControlGroup,MemoryMax,CPUQuotaPerSecUSec'], timeout=10)
        return proof


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--libs', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--owners', type=int, choices=(4, 16), default=16)
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise SystemExit('requires the privileged host namespace wrapper')
    uptime = subprocess.check_output(['uptime'], text=True).strip()
    load = os.getloadavg()
    if max(load) >= 6:
        raise SystemExit('local functional execution requires all load averages below 6: ' + uptime)
    binary = args.binary.resolve(strict=True)
    libs = args.libs.resolve(strict=True)
    os.environ['LD_LIBRARY_PATH'] = str(libs)
    output = args.output.absolute()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    token = 'th-' + uuid.uuid4().hex[:16]
    fixture = Path('/mnt/tidehunter-kv-comparison') / token
    fixture.mkdir(mode=0o700, parents=True)
    # The reviewed topology supplies cloud slice/owner mapping only. The local
    # workload below is deliberately not a reviewed RDMA campaign cell.
    topology = next(j for j in cloud_campaign.make_focused_matrix('nvmeof_m3_t1')
                    if j.get('owner_count') == 16 and j.get('owner_topology') == 'colocated')
    case = topology['case_id']
    job = dict(topology)
    job.update(owner_count=args.owners, owner_topology='colocated')
    for key in ('case_kind', 'measurement_sizing', 'device_counters', 'thread_cpu_sampling'):
        job.pop(key, None)
    job.update(selection='local_functional', backend='tidehunter_nvme_tcp', keys=args.owners,
               warmup_operations=0, measurement_operations=args.owners, concurrency=args.owners,
               owner_wal_geometry='research', owner_wal_file_bytes=8388608,
               owner_read_memory_allowance_bytes=128 * 1024**2)
    now = datetime.now(timezone.utc)
    config = {'prefix': token, 'binary': str(binary), 'binary_sha256': cloud.digest(binary),
              'source_revision': 'local-functional-unsealed',
              'repository_root': str(Path(__file__).resolve().parents[2]),
              'fdb_library_dir': str(libs), 'deadline_utc': (now + timedelta(seconds=600)).isoformat(),
              'fleet': {'run_id': token, 'data_root': str(fixture),
                        'coordinator': {'instance_id': 'local-client', 'address': '127.0.0.1'},
                        'nodes': [{'index': 0, 'instance_id': 'local-owner-host', 'address': '127.0.0.1'}],
                        'client_cpus': 4, 'client_memory_mib': 2048,
                        'server_cpus': 4, 'server_memory_mib': 8192},
              'spec': {'mode': 'standard-with-live-nvmeof', 'selection': 'nvmeof_m3_t1',
                       'backends': ['tidehunter_nvme_tcp'], 'hard_deadline_seconds': 600,
                       'case_deadline_seconds': 180, 'export_reserve_seconds': 120},
              'jobs': [job]}
    objects = fixture / 'objects'
    objects.mkdir()
    mode = MultiownerMode(fixture, config, libs)
    controller = cloud.Controller(config, {'objects': LocalObjects(objects)}, local_mode=mode)
    mode.controller = controller
    controller.root.mkdir()
    directory = controller.root / 'cases' / case
    directory.mkdir(parents=True)
    evidence = {'scope': __doc__, 'uptime_before': uptime, 'load_before': load,
                'binary': str(binary), 'binary_sha256': cloud.digest(binary),
                'route': 'kernel NVMe/TCP leased FUA writes; buffered kernel target reads; TCP/TLS control',
                'owner_count': args.owners, 'complete': False, 'cleanup_complete': False,
                'fixture': str(fixture), 'errors': []}
    save_json(output / 'input.json', {'config': config, 'job': job})
    kernel_before = subprocess.check_output(['dmesg', '--raw'], text=True)
    (output / 'dmesg-before.txt').write_text(kernel_before)
    try:
        options = controller.prepare_tide(job, directory)
        identities = options['external_remote_config']['live_nvme']['owner_identities']
        if (len(identities) != args.owners
                or len({x['host_nqn'] for x in identities.values()}) != args.owners):
            raise RuntimeError('one unique initiator identity required per owner')
        # Build a generic TCP config, then use the exact cloud admission runner.
        # The matrix job supplies only the shared slice/placement topology.
        import copy
        cell = directory / 'owner-admission'
        cell.mkdir()
        selected = copy.deepcopy(options)
        selected.update(path=str(directory / 'database'), evidence=str(cell / 'read-evidence'),
                        coordinator_read_cache_bytes=0)
        template = {key: job[key] for key in ('repeat', 'case_id', 'keys', 'value_size', 'mix',
                    'concurrency', 'measurement_operations', 'warmup_operations',
                    'offered_ops_per_second', 'distribution', 'durability_tier')}
        template['backend'] = 'tidehunter_nvme_tcp'
        admission_config = cloud_campaign.make_run_config(template, cell, selected,
            config['source_revision'], cloud_campaign.M3_OWNER_ADMISSION_TIMEOUT_SECONDS)
        controller.run_logical_owner_admission(job, directory, admission_config)
        evidence['admission'] = json.loads((directory / 'owner-admission.json').read_text())
        # The owners stay live. A second coordinator must recover the exact
        # authority that granted their admission epoch, just as the fleet does.
        measured = directory / 'second-open'
        measured.mkdir()
        second_template = {**template, 'mix': cloud_campaign.mix(read=100)}
        second = cloud_campaign.make_run_config(second_template, measured, options,
            config['source_revision'], cloud_campaign.M3_OWNER_ADMISSION_TIMEOUT_SECONDS)
        second.update(namespace='local-second-open', measurement_gate=None,
                      measurement_completion_gate=None)
        second['phases'].update(warmup_operations=0, measurement_operations=args.owners,
                                measurement_key_sweep=True)
        save_json(measured / 'config.json', second)
        cloud.command([str(binary), '--config', measured / 'config.json', '--output', measured / 'result'],
            timeout=cloud_campaign.M3_OWNER_ADMISSION_TIMEOUT_SECONDS, env=cloud.environment(config))
        summary = json.loads((measured / 'result/summary.json').read_text())
        if summary.get('status') != 'completed':
            raise RuntimeError('second coordinator did not complete the benchmark namespace')
        evidence['second_open'] = {'status': summary['status'],
            'authority_database': options['path'], 'owners_remained_live': True}
        slice_name, expected_limits = cloud.owner_slice_limits(config, case)
        evidence['slice'] = {'name': slice_name, 'expected_limits': expected_limits,
            'observed': cloud.command(['systemctl', 'show', slice_name,
                '--property=ControlGroup,CPUQuotaPerSecUSec,MemoryMax,MemorySwapMax,ActiveState'], timeout=10)}
        evidence['complete'] = True
    except Exception:
        evidence['errors'].append(traceback.format_exc())
    finally:
        try:
            controller.stop_tide()
            evidence['cleanup_complete'] = True
        except Exception:
            evidence['errors'].append(traceback.format_exc())
        kernel_after = subprocess.check_output(['dmesg', '--raw'], text=True)
        (output / 'dmesg-after.txt').write_text(kernel_after)
        kernel_complete = kernel_after.startswith(kernel_before)
        added = kernel_after[len(kernel_before):] if kernel_complete else kernel_after
        kernel_errors = [line for line in added.splitlines() if re.search(
            r'starting error recovery|Property Set error|no (?:available|usable) path|'
            r'buffer I/O error|nvme.*(?:I/O error|controller.*(?:reset|recovery))', line, re.IGNORECASE)]
        evidence['kernel_log'] = {'before': 'dmesg-before.txt', 'after': 'dmesg-after.txt',
            'complete_interval': kernel_complete, 'io_errors_or_controller_recovery': kernel_errors}
        if not kernel_complete or kernel_errors:
            evidence['complete'] = False
            evidence['errors'].append('kernel log interval incomplete or contains I/O/controller errors')
        evidence['uptime_after'] = subprocess.check_output(['uptime'], text=True).strip()
        evidence['load_after'] = os.getloadavg()
        shutil.copytree(controller.root, output / 'harness-results')
        evidence['fixture_removed'] = False
        if evidence['cleanup_complete']:
            shutil.rmtree(fixture)
            evidence['fixture_removed'] = not fixture.exists()
        save_json(output / 'proof.json', evidence)
    print(json.dumps(evidence, indent=2))
    return 0 if evidence['complete'] and evidence['cleanup_complete'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
