#!/usr/bin/env python3
"""Raw-floor failure/provenance/deadline/ownership tests, no simulated FI_READ."""
from datetime import datetime, timedelta, timezone
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import raw_rma_floor as raw


class RawRmaFloorTests(unittest.TestCase):
    def stamp(self, seconds):
        return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()

    def payload(self):
        cfg = dict(run_id='th-0123456789abcdef', run_root='/mnt/tidehunter/th-0123456789abcdef',
            account='123456789012', region='us-east-1', instance_type='g6.8xlarge',
            nodes=[dict(partition=0, instance_id='i-0123456789abcdef0', private_ip='10.0.0.11')],
            coordinator_instance_id='i-0123456789abcdef1', port=24000,
            binaries={'transport-floor': 'a' * 64}, source_archive_sha256='b' * 64,
            deadline_utc=self.stamp(1200), export_reserve_seconds=600,
            s3_prefix='th-0123456789abcdef', transports=['tcp', 'efa-native'],
            kv_suite=dict(resident_reads=dict(source_bytes=8192, destination_slots=1)))
        return dict(config=cfg, case_id='raw-rma-0123456789abcdef-v256',
            instance_id=cfg['nodes'][0]['instance_id'], port=raw.owned_port(cfg, 256),
            payload=256, samples=raw.SAMPLES, warmup=raw.WARMUP, deadline_ms=raw.PROBE_MS,
            software=False, binary_sha256='a' * 64, work_deadline=self.stamp(500),
            artifact=dict(key=cfg['s3_prefix'] + '/case-artifacts/raw-rma-0123456789abcdef-binary/node-0.tar.gz',
                          sha256='c' * 64))

    def reports(self, read_mode, *, software=False, require_device_read=False):
        payload, samples, warmup = 256, 2, 1
        label = 'fabric-tcp' if software else 'efa-direct'
        provider = 'tcp;ofi_rxm' if software else 'efa'
        common = dict(benchmark='raw_provider_fi_read_floor', claim=raw.CLAIM,
            topology='two_process_control_tcp_and_provider_fabric',
            control_security=raw.SECURITY, provider=provider,
            fabric='tcp;ofi_rxm_explicit_software' if software else 'efa-direct',
            payload_bytes=payload, accepted_fi_reads=samples + warmup,
            samples_requested=samples, warmup_requested=warmup,
            warmup_completed=warmup, clean_samples_completed=samples,
            registered_region_bytes=payload + 24, deadline_ms=raw.PROBE_MS,
            polling='busy_spin')
        expected = dict(payload=payload, samples=samples, warmup=warmup,
            deadline_ms=raw.PROBE_MS, client_domain='lo', server_domain='lo',
            client_fabric_config=raw.fabric_config_for_case(label, 'lo'),
            server_fabric_config=raw.fabric_config_for_case(label, 'lo'),
            require_device_read=require_device_read)
        reports = []
        for role in ('client', 'server'):
            config = expected[role + '_fabric_config']
            provider_info = dict(backend=config['backend'], provider=provider,
                fabric='actual-fabric', domain='lo', source_address=None,
                device=None, driver=None, bus_address=None,
                rma_capabilities=dict(read_mode=read_mode, max_read_bytes=payload + 24),
                capability_evidence='provider capability discovery')
            caps = dict(provider=provider, domain='lo', read_mode=read_mode,
                efa_direct_requested=not software, software_provider_explicit=software,
                address_mode='virtual', max_read_bytes=payload + 24)
            report = dict(common, role=role, domain='lo', fabric_config=config,
                require_device_read=require_device_read, provider_info=provider_info,
                capabilities=caps, cpu_affinity='0', detailed_steps_ns=[
                    ['provider_source_setup_ns' if role == 'server' else 'provider_endpoint_setup_ns', 1],
                    ['drain_ack_retirement_ns' if role == 'server' else 'control_bootstrap_descriptor_ns', 1],
                    ['drain_ack_write_ns', 1]])
            report['raw_samples'] = ([] if role == 'server' else [
                dict(index=i if i < warmup else i - warmup, phase='warmup' if i < warmup else 'clean',
                     requested_payload_bytes=payload, actual_fi_read_bytes=payload + 24,
                     valid=True, post_would_block_retries=0, empty_completion_polls=0,
                     completion_latency_ns=1, api_validation_latency_ns=1)
                for i in range(samples + warmup)])
            reports.append(report)
        return expected, reports

    def test_device_read_policy_does_not_fabricate_mode(self):
        for mode in ('native', 'emulated', 'unknown'):
            expected, (client, server) = self.reports(mode)
            with self.subTest(mode=mode):
                raw.validate_reports(client, server, expected)

    def test_required_device_read_needs_native_on_both_roles(self):
        for mode in ('emulated', 'unknown'):
            expected, reports = self.reports(mode, require_device_read=True)
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                raw.validate_reports(*reports, expected)
        expected, reports = self.reports('native', require_device_read=True)
        raw.validate_reports(*reports, expected)

    def test_software_provider_never_claims_native(self):
        expected, reports = self.reports('native', software=True)
        with self.assertRaisesRegex(ValueError, 'requested read policy'):
            raw.validate_reports(*reports, expected, software=True)

    def test_provider_identity_is_bound_to_requested_source_and_backend(self):
        expected, reports = self.reports('emulated', software=True)
        requested_source = '10.0.0.21'
        for role, report in zip(('client', 'server'), reports):
            expected[role + '_fabric_config']['source_address'] = requested_source
            report['fabric_config']['source_address'] = requested_source
            report['provider_info']['source_address'] = requested_source
        raw.validate_reports(*reports, expected, software=True)
        mutations = (
            ('backend', 'efa-direct'),
            ('provider', 'efa'),
            ('domain', 'other-domain'),
            ('source_address', '10.0.0.22'),
        )
        for key, value in mutations:
            changed = json.loads(json.dumps(reports[0]))
            changed['provider_info'][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                raw.validate_reports(changed, reports[1], expected, software=True)


    def test_deadline_cannot_consume_export_reserve(self):
        payload = self.payload()
        payload['work_deadline'] = self.stamp(1000)
        with self.assertRaisesRegex(ValueError, 'export reserve'):
            raw.validate_payload(payload)

    def test_expired_or_reserve_only_budget_never_dispatches(self):
        for seconds, reserve in ((-1, 0), (10, 20), (0, 0)):
            with self.subTest(seconds=seconds), self.assertRaises(TimeoutError):
                raw.budget(self.stamp(seconds), reserve, 45)
        with self.assertRaisesRegex(ValueError, 'absolute UTC'):
            raw.budget('2030-01-01T00:00:00', 0, 45)

    def test_hung_client_deadline_preserves_finally_cleanup_time(self):
        work = self.stamp(120)
        child = raw.client_deadline(work)
        self.assertGreater(raw.left(work) - raw.left(child), 89)
        self.assertLessEqual(raw.left(child), raw.PROBE_MS / 1000 + 5)
        with self.assertRaises(TimeoutError):
            raw.client_deadline(self.stamp(90))

    def test_source_binary_or_scoped_input_mismatch_rejected(self):
        for field, value, message in (
            ('binary_sha256', 'c' * 64, 'six-binary'),
            ('artifact', dict(key='another-run/inputs/raw-rma/binary', sha256='a' * 64), 'scoped'),
            ('software', True, 'policy'),
            ('port', 7700, 'owned port')):
            payload = self.payload()
            payload[field] = value
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, message):
                raw.validate_payload(payload)

    def test_other_payload_cannot_reuse_previous_control_port(self):
        payload = self.payload()
        payload.update(payload=8192, case_id='raw-rma-0123456789abcdef-v8192')
        with self.assertRaisesRegex(ValueError, 'unused owned port'):
            raw.validate_payload(payload)

    def test_missing_floor_fails_registered_admission_not_rpc_default(self):
        cfg = self.payload()['config']
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(FileNotFoundError):
                raw.validate_receipt(folder, {}, cfg)
            cfg['transports'] = ['tcp', 'efa']
            self.assertIsNone(raw.validate_receipt(folder, {}, cfg))
            with self.assertRaisesRegex(ValueError, 'ordinary TCP'):
                raw.validate_receipt(folder, {'raw_rma_floor': {}}, cfg)

    def test_raw_probe_cannot_relabel_message_ram_exchange(self):
        expected = dict(payload=256, samples=2, warmup=1, deadline_ms=30000,
                        client_domain='efa_0-rdm', server_domain='efa_1-rdm')
        with self.assertRaisesRegex(ValueError, 'benchmark'):
            raw.validate_reports(dict(benchmark='session_ram_echo'), {}, expected)

    def test_incomplete_reports_do_not_create_cdfs_or_drain_success(self):
        expected = dict(payload=256, samples=2, warmup=1, deadline_ms=30000,
                        client_domain='efa_0-rdm', server_domain='efa_1-rdm')
        for report in ({}, {'benchmark': 'raw_provider_fi_read_floor'},
                       {'benchmark': 'raw_provider_fi_read_floor', 'claim': raw.CLAIM}):
            with self.subTest(report=report), self.assertRaises(ValueError):
                raw.validate_reports(report, {}, expected)

    def test_failure_or_forced_stop_cannot_prove_drain(self):
        unit = 'raw-rma-owned'
        for status in ({}, dict(MainPID='42'),
                       dict(MainPID='0', ExecMainCode='2', ExecMainStatus='15', Result='signal'),
                       dict(MainPID='0', ExecMainCode='1', ExecMainStatus='1', Result='exit-code')):
            cleanup = dict(cleanup_complete=True, unit_collected=True, no_owned_processes=True,
                           unit=unit, terminal_status=status)
            with self.subTest(status=status), self.assertRaisesRegex(ValueError, 'naturally'):
                raw.validate_cleanup(cleanup, unit)
        with self.assertRaisesRegex(ValueError, 'cleanup'):
            raw.validate_cleanup(dict(cleanup_complete=False), unit)

    def test_foreign_ready_is_not_client_admission(self):
        payload = self.payload()
        ready = dict(case_id=payload['case_id'], run_id=payload['config']['run_id'],
            instance_id='i-fffffffffffffffff', binary_sha256=payload['binary_sha256'],
            source_archive_sha256=payload['config']['source_archive_sha256'], ready=True,
            address='10.0.0.11:' + str(payload['port']), domain='efa_0-rdm', pid=123,
            unit='raw-rma-' + payload['config']['run_id'] + '-' + payload['case_id'])
        with self.assertRaisesRegex(ValueError, 'instance_id'):
            raw.validate_ready(ready, payload)

    def test_archive_duplicate_foreign_or_nested_member_rejected(self):
        for names in (('server.json', 'server.json'), ('../server.json',), ('sub/server.json',)):
            with self.subTest(names=names), tempfile.TemporaryDirectory() as folder:
                target = Path(folder) / 'result.tar.gz'
                with tarfile.open(target, 'w:gz') as archive:
                    for name in names:
                        value = b'{}'
                        member = tarfile.TarInfo(name)
                        member.size = len(value)
                        archive.addfile(member, io.BytesIO(value))
                with self.assertRaisesRegex(ValueError, 'foreign/duplicate'):
                    raw.archive_documents(target)

    def test_readiness_observation_does_not_consume_single_accept(self):
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            listener.listen(1)
            listener.setblocking(False)
            address = '127.0.0.1:' + str(listener.getsockname()[1])
            self.assertTrue(raw.listener_owned(os.getpid(), address))
            with self.assertRaises(BlockingIOError):
                listener.accept()

    def test_exact_output_process_is_reaped_on_failure(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / 'owned.json'
            child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)',
                                      '--output', str(output)], start_new_session=True)
            try:
                self.assertIn(child.pid, raw.matching_pids(output))
                raise TimeoutError('consumer deadline')
            except TimeoutError:
                raw.reap(child)
            finally:
                raw.reap(child)
            self.assertNotIn(child.pid, raw.matching_pids(output))
            self.assertIsNotNone(child.returncode)

    def test_unknown_systemd_cleanup_never_becomes_success(self):
        # Failure-only consumption: a lost systemd/bus response is not absence.
        with patch('subprocess.run', side_effect=subprocess.TimeoutExpired('systemctl', 1)):
            with self.assertRaises(subprocess.TimeoutExpired):
                raw.stop_owned('raw-rma-owned', '/no/owned/output', self.stamp(30))

    def test_actual_software_evidence_mutations_fail_closed(self):
        # Optional REAL run output, supplied by the parent local-software command.
        # No generated success report or copied producer schema is a substitute.
        folder = os.environ.get('RAW_RMA_SOFTWARE_EVIDENCE')
        if not folder:
            self.skipTest('set RAW_RMA_SOFTWARE_EVIDENCE to actual local-software output')
        case = Path(folder) / '256'
        client = raw.document(case / 'client.json')
        server = raw.document(case / 'server.json')
        client_fabric = raw.fabric_config_for_case('fabric-tcp', 'lo')
        server_fabric = raw.fabric_config_for_case('fabric-tcp', 'lo')
        expected = dict(payload=256, samples=client['samples_requested'], warmup=client['warmup_requested'],
                        deadline_ms=client['deadline_ms'], client_domain='lo', server_domain='lo',
                        client_fabric_config=client_fabric, server_fabric_config=server_fabric,
                        require_device_read=False)
        for mutate in (
            lambda row: row['raw_samples'].pop(),
            lambda row: row['raw_samples'][0].update(valid=False),
            lambda row: row['raw_samples'][0].update(actual_fi_read_bytes=256),
            lambda row: row['raw_samples'][0].update(index=10000),
            lambda row: row['capabilities'].update(read_mode='native'),
            lambda row: row.update(detailed_steps_ns=[])):
            changed = json.loads(json.dumps(client))
            mutate(changed)
            with self.subTest(mutate=mutate), self.assertRaises(ValueError):
                raw.validate_reports(changed, server, expected, software=True)
        with self.assertRaises(ValueError):
            raw.validate_reports(client, server, expected, software=False)


if __name__ == '__main__':
    unittest.main()
