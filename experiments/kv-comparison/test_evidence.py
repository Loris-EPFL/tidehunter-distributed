import copy
import csv
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import analyze


class EvidenceAdmission(unittest.TestCase):
    def binding(self, root):
        result = root / 'result'
        result.mkdir()
        config = {'seed': 17, 'dataset': {'value_size': 1024}}
        normalized = {'seed': 17, 'dataset': {'value_size': 1024, 'entropy': 1.0}}
        (root / 'normalized-config.json').write_text(json.dumps(normalized))
        metadata = {'name': 'native', 'acknowledgement': 'page_cache'}
        manifest = {'run': {'config': normalized, 'binary': {'blake2b512': 'binary-proof'}}, 'backend_info': metadata}
        summary = {'backend': metadata}
        files = {'manifest.json': json.dumps(manifest), 'summary.json': json.dumps(summary),
                 'samples.csv': 'one raw original sample\n', 'metrics.prom': 'observed_counter_total 1\n'}
        for name, text in files.items():
            (result / name).write_text(text)
        receipt = {'normalized_config_sha256': hashlib.sha256((root / 'normalized-config.json').read_bytes()).hexdigest(),
                   'binary_blake2b512': 'binary-proof',
                   'artifacts_sha256': {name: hashlib.sha256((result / name).read_bytes()).hexdigest() for name in files}}
        return config, receipt, manifest, summary

    def test_result_swap_rejected_even_when_its_artifact_hash_is_updated(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, receipt, manifest, summary = self.binding(root)
            analyze.verify_binding(root, config, receipt, manifest, summary)
            altered = copy.deepcopy(manifest)
            altered['run']['config']['dataset']['value_size'] = 8192
            (root / 'result/manifest.json').write_text(json.dumps(altered))
            receipt['artifacts_sha256']['manifest.json'] = hashlib.sha256((root / 'result/manifest.json').read_bytes()).hexdigest()
            with self.assertRaisesRegex(ValueError, 'configuration differs'):
                analyze.verify_binding(root, config, receipt, altered, summary)

    def test_raw_metrics_tampering_does_not_enter_a_comparison(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, receipt, manifest, summary = self.binding(root)
            (root / 'result/metrics.prom').write_text('observed_counter_total 0\n')
            with self.assertRaisesRegex(ValueError, 'integrity binding'):
                analyze.verify_binding(root, config, receipt, manifest, summary)

    def test_other_executable_and_changed_explicit_input_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, receipt, manifest, summary = self.binding(root)
            altered = copy.deepcopy(manifest)
            altered['run']['binary']['blake2b512'] = 'different-binary'
            with self.assertRaisesRegex(ValueError, 'executable differs'):
                analyze.verify_binding(root, config, receipt, altered, summary)
            config['seed'] = 18
            with self.assertRaisesRegex(ValueError, 'explicitly supplied'):
                analyze.verify_binding(root, config, receipt, manifest, summary)

    def test_volatile_process_and_tikv_identity_do_not_prevent_repetitions(self):
        first = {'process_id': 123, 'owners': 1, 'path': '/first'}
        second = {'process_id': 456, 'owners': 1, 'path': '/second'}
        self.assertEqual(analyze.stable_topology('native', first), analyze.stable_topology('native', second))
        base = {'store_metadata': {'stores': [{'store': {'id': 1, 'address': '127.0.0.1:20160',
                'version': '8.5.4', 'state_name': 'Up'}, 'status': {'last_heartbeat_ts': 'first'}}]}}
        changed = copy.deepcopy(base)
        changed['store_metadata']['stores'][0]['store']['id'] = 44
        changed['store_metadata']['stores'][0]['status']['last_heartbeat_ts'] = 'later'
        self.assertEqual(analyze.stable_topology('tikv_raw', base), analyze.stable_topology('tikv_raw', changed))
        changed['store_metadata']['stores'][0]['store']['version'] = 'different-release'
        self.assertNotEqual(analyze.stable_topology('tikv_raw', base), analyze.stable_topology('tikv_raw', changed))

    def test_host_memory_and_swap_capacity_are_part_of_the_contract(self):
        small = analyze.host_capacity('MemTotal: 1024 kB\nMemFree: 12 kB\nSwapTotal: 0 kB\n')
        equally_sized = analyze.host_capacity('MemTotal: 1024 kB\nMemFree: 900 kB\nSwapTotal: 0 kB\n')
        bigger = analyze.host_capacity('MemTotal: 2048 kB\nSwapTotal: 0 kB\n')
        self.assertEqual(small, equally_sized)
        self.assertNotEqual(small, bigger)
        self.assertEqual(small['MemTotal'], 1048576)
        with self.assertRaisesRegex(ValueError, 'capacity evidence'):
            analyze.host_capacity('MemFree: 1024 kB\n')


class ClockAdmission(unittest.TestCase):
    fields = ['phase','operation','outcome','scheduled_ns','dispatched_ns','dispatch_observed',
              'completed_ns','api_ns','response_ns','scheduling_ns','logical_keys','bytes','error_kind']

    def read_row(self, row):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'samples.csv'
            with path.open('w', newline='') as stream:
                writer = csv.DictWriter(stream, fieldnames=self.fields)
                writer.writeheader()
                writer.writerow(row)
            return list(analyze.read_samples(path))[0]

    def sample(self):
        return dict(zip(self.fields, ['measurement','read','success',100,110,'true',140,30,40,10,1,256,'']))

    def test_clock_decomposition_and_undispatched_rejection_are_distinct(self):
        row = self.read_row(self.sample())
        self.assertEqual(row['response_ns'], row['api_ns'] + row['scheduling_ns'])
        rejected = self.sample()
        rejected.update(outcome='rejected', dispatch_observed='false', dispatched_ns=0, api_ns=0, scheduling_ns=0)
        row = self.read_row(rejected)
        self.assertIsNone(row['api_ns'])
        self.assertIsNone(row['scheduling_ns'])
        self.assertEqual(row['response_ns'], 40)

    def test_inconsistent_or_invented_clocks_are_rejected(self):
        cases = [{'api_ns': 29}, {'response_ns': 41}, {'scheduling_ns': 9},
                 {'dispatched_ns': 150}, {'outcome': 'rejected'},
                 {'dispatch_observed': 'false'}, {'logical_keys': 0}]
        for changes in cases:
            with self.subTest(changes=changes):
                row = self.sample()
                row.update(changes)
                with self.assertRaises(ValueError):
                    self.read_row(row)


class TerminalTimingAdmission(unittest.TestCase):
    def test_incomplete_phases_and_early_dispatch_cannot_be_completed(self):
        import campaign
        with tempfile.TemporaryDirectory() as folder:
            result = Path(folder)
            config = {'dataset': {'keys': 1}, 'qualification_only': False,
                      'phases': {'preload': False, 'warmup_operations': 1,
                                 'aging_operations': 0, 'measurement_operations': 1}}
            warmup = ClockAdmission().sample()
            warmup['phase'] = 'warmup'
            measured = ClockAdmission().sample()
            for field in ('scheduled_ns', 'dispatched_ns', 'completed_ns'):
                measured[field] += 1000
            def publish(rows):
                with (result / 'samples.csv').open('w', newline='') as stream:
                    writer = csv.DictWriter(stream, fieldnames=ClockAdmission.fields)
                    writer.writeheader()
                    writer.writerows(rows)
            publish([warmup, measured])
            self.assertEqual(campaign.verify_timing_result(result, config),
                             {'preload': 0, 'warmup': 1, 'aging': 0, 'measurement': 1})
            publish([measured])
            with self.assertRaises(ValueError):
                campaign.verify_timing_result(result, config)
            warmup.update(dispatched_ns=90, api_ns=50, scheduling_ns=0)
            publish([warmup, measured])
            with self.assertRaises(ValueError):
                campaign.verify_timing_result(result, config)


class EffectiveRoleIdentity(unittest.TestCase):
    def test_cache_and_heap_changes_are_not_masked_as_ephemeral_identity(self):
        import local
        original = {'Config': {'Cmd': ['fdbserver', '--cache-memory', '128MiB'],
                               'Entrypoint': None,
                               'Env': ['HOSTNAME=first', 'CASSANDRA_CLUSTER_NAME=first',
                                       'MAX_HEAP_SIZE=512M']}}
        identity = local.runtime_configuration_hash(original)
        renamed = json.loads(json.dumps(original))
        renamed['Config']['Env'][:2] = ['HOSTNAME=second', 'CASSANDRA_CLUSTER_NAME=second']
        self.assertEqual(local.runtime_configuration_hash(renamed), identity)
        changed_cache = json.loads(json.dumps(original))
        changed_cache['Config']['Cmd'][-1] = '256MiB'
        self.assertNotEqual(local.runtime_configuration_hash(changed_cache), identity)
        changed_heap = json.loads(json.dumps(original))
        changed_heap['Config']['Env'][-1] = 'MAX_HEAP_SIZE=1G'
        self.assertNotEqual(local.runtime_configuration_hash(changed_heap), identity)


if __name__ == '__main__':
    unittest.main()
