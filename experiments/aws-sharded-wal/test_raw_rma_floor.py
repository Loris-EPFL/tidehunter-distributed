#!/usr/bin/env python3
"""Generic raw FI_READ producer validation and software-process ownership tests."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest

import raw_rma_floor as raw


class RawRmaFloorTests(unittest.TestCase):
    def reports(self, read_mode, *, software=False, require_device_read=False):
        payload, samples, warmup = 256, 2, 1
        label = 'fabric-tcp' if software else 'efa-direct'
        provider = 'tcp;ofi_rxm' if software else 'efa'
        common = dict(benchmark='raw_provider_fi_read_floor', claim=raw.CLAIM,
            topology='two_process_control_tcp_and_provider_fabric',
            control_security=raw.SECURITY,
            fabric='software-tcp' if software else 'efa-direct',
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
                rma_capabilities=dict(read_mode=read_mode, address_mode='virtual', max_read_bytes=payload + 24),
                capability_evidence='provider capability discovery')
            caps = dict(read_mode=read_mode, provider_info=provider_info,
                fabric_config=config, require_device_read=require_device_read,
                address_mode='virtual', max_read_bytes=payload + 24)
            report = dict(common, role=role, fabric_config=config,
                provider_info=provider_info,
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

    def test_command_reconstruction_does_not_create_remote_directories(self):
        with tempfile.TemporaryDirectory() as folder:
            remote = Path(folder) / 'absent-storage-host/run/server.json'
            command = raw.command('probe', 'server', '10.0.0.11:17700', 256, remote)
            self.assertEqual(command[command.index('--fabric-config') + 1],
                             str(remote.with_suffix('.fabric.json')))
            self.assertFalse(remote.parent.exists())

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
