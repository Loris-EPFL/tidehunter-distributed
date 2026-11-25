"""The shared `/proc` sampler: one parser, one evidence shape, CPU fields.

`manual_sui.py` and `stage3_campaign.py` both import this module, so the two
drivers cannot drift in what a sample or a host evidence row means.
"""
import json
from pathlib import Path
import tempfile
import unittest

import proc_sampler

STATUS = (b'VmRSS:\t  1024 kB\nVmHWM:\t  2048 kB\nVmSwap:\t     0 kB\n'
          b'VmSize:\t  4096 kB\n')
IO = (b'rchar: 11\nwchar: 22\nsyscr: 3\nsyscw: 4\nread_bytes: 55\n'
      b'write_bytes: 66\ncancelled_write_bytes: 0\n')


def stat_line(*, utime=700, stime=300, starttime=987654):
    """One `/proc/<pid>/stat` line with the CPU fields at 14 and 15."""
    after = ['S', '1', '123', '123', '0', '-1', '4194560', '100', '0', '0', '0',
             str(utime), str(stime), '0', '0', '20', '0', '1', '0', str(starttime)]
    after += ['0'] * 32
    return ('123 (kv-research) ' + ' '.join(after) + '\n').encode()


class ParseProc(unittest.TestCase):
    def test_cpu_ticks_come_from_the_same_stat_line(self):
        entry = proc_sampler.parse_proc(stat_line(), STATUS, IO)
        self.assertEqual(entry['utime_ticks'], 700)
        self.assertEqual(entry['stime_ticks'], 300)
        self.assertAlmostEqual(entry['cpu_seconds'], 1000 / proc_sampler.CLOCK_TICKS)
        self.assertEqual(entry['starttime_ticks'], 987654)
        self.assertEqual(entry['VmRSS'], 1024 * 1024)
        self.assertEqual(entry['VmHWM'], 2048 * 1024)
        self.assertEqual(entry['io']['write_bytes'], 66)

    def test_a_missing_stream_is_an_exited_process(self):
        self.assertIsNone(proc_sampler.parse_proc(b'', STATUS, IO))
        self.assertIsNone(proc_sampler.parse_proc(stat_line(), b'', IO))
        self.assertIsNone(proc_sampler.parse_proc(stat_line(), STATUS, b''))
        self.assertIsNone(proc_sampler.parse_proc(b'not a stat line\n', STATUS, IO))

    def test_a_resource_row_carries_the_cpu_seconds_column(self):
        blocks = {
            'vmstat': ['pswpin 7', 'pswpout 9'],
            'blk': ['nvme0n1 100 0 8 0 0 0 16 0 0 0 0'],
            '4242': (stat_line(utime=100, stime=50).decode() + '\n'
                     + STATUS.decode() + IO.decode()).splitlines(),
        }
        rows = proc_sampler.block_rows(blocks, {4242: 'store'}, 'H1', 1_000)
        self.assertEqual(len(rows), 2)
        host, process = rows
        self.assertEqual(host['role'], 'host')
        self.assertEqual(host['swap_in'], 7)
        self.assertEqual(host['device_read_bytes'], 8 * proc_sampler.SECTOR_BYTES)
        self.assertEqual(host['device_write_bytes'], 16 * proc_sampler.SECTOR_BYTES)
        self.assertEqual(process['role'], 'store')
        self.assertEqual(process['pid'], 4242)
        self.assertAlmostEqual(process['cpu_seconds'], 150 / proc_sampler.CLOCK_TICKS)
        self.assertEqual(process['rss_bytes'], 1024 * 1024)


class HostEvidence(unittest.TestCase):
    def test_the_runner_contract_sees_the_same_host_rows(self):
        hosts = [dict(host='H1', commands=[['storage-node'], ['storage-node']])]
        namespaces = {'H1': [dict(index=0, command=['storage-node'], pid=4242,
                                  process_group=4242, stopped=True,
                                  starttime_ticks=987654),
                             dict(index=1, command=['storage-node'], pid=4243,
                                  process_group=4243, stopped=True,
                                  starttime_ticks=987655)]}
        samples = {'H1': [
            dict(unix_ns=1, healthy=True, swap=dict(pswpin=1, pswpout=2),
                 diskstats='1 2 3', processes=[dict(pid=4242), dict(pid=4243)]),
            dict(unix_ns=2, healthy=True, swap=dict(pswpin=3, pswpout=4),
                 diskstats='4 5 6', processes=[dict(pid=4242), dict(pid=4243)])]}
        value = proc_sampler.host_evidence(
            hosts, namespaces, samples, dict(cell='S1-1'),
            binary_sha256='a' * 64, guest_root=Path('/mnt/tidehunter'))
        host = value['hosts']['H1']
        self.assertTrue(host['complete'] and host['all_namespaces_healthy'])
        self.assertEqual([row['pid'] for row in host['namespaces']], [4242, 4243])
        self.assertTrue(all(row['reaped'] for row in host['namespaces']))
        self.assertEqual(host['swap_before'], dict(pswpin=1, pswpout=2))
        self.assertEqual(host['swap_after'], dict(pswpin=3, pswpout=4))
        self.assertEqual(host['diskstats_after'], '4 5 6')
        self.assertEqual(host['samples'][1]['unix_ns'], 2)
        self.assertEqual(value['cell'], 'S1-1')

    def test_the_sampler_writes_resources_csv_and_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            sampler = proc_sampler.Sampler(interval=0)
            sampler.watch_local(1, 'coordinator')
            sampler.start()
            sampler.stop()
            value = sampler.write(Path(tmp), case='S3-A1', mode='rehearsal',
                                  label='local')
            header = (Path(tmp) / 'resources.csv').read_text().splitlines()[0]
            self.assertEqual(tuple(header.split(',')), proc_sampler.RESOURCE_COLUMNS)
            self.assertEqual(value['case'], 'S3-A1')
            self.assertTrue((Path(tmp) / 'resources.json').is_file())
            json.loads((Path(tmp) / 'resources.json').read_text())


if __name__ == '__main__':
    unittest.main()
