#!/usr/bin/env python3
"""Stage-3 cell driver: the cluster campaign and the labelled local rehearsal.

Each cell is one `kv-research` process on a fresh database and a fresh output
directory. Before a rerun, the previous output (and every local database) is
moved to `stage3/stale/<case>-attempt-<n>`; never reused. The ledger is saved
after every transition, locally and, when `--bucket-prefix` is given, to the
campaign ledger key in the bucket.

Cluster mode (default): arm A runs on the coordinator host against the two
NVMe-oF owners of the tier's `prepare_external.py` preparation; arm B runs on
the owner host, whose Local SSD holds the native database. The `kv-research`
native backend is in-process, so arm B's driver shares that host; every arm-B
record says so. Before an arm-A cell the driver installs the tier's owner
archive on each owner host, starts only the selected database's owner process
group, waits for its ready file, and copies each owner's
`ready.durability.json` to the coordinator host where the spec names it. Arm B
starts only after every owner process group is stopped. Every cell's results are
copied back to the campaign root, uploaded under the campaign prefix, and
verified by size and SHA-256 before the ledger may pass the cell: `passed`
means uploaded and verified.

Rehearsal mode (`--mode rehearsal`): every cell runs the single-host trusted TCP
ordinary-file fixture on this host. The results and the ledger entry say
`rehearsal`; the mode is not a campaign measurement.

The stage runs in the plan's phases. Phase A is one arm-B synchronous write
probe cell (`stage3_specs.probe_case`): it installs the native owner archive,
runs the cell on the owner host, and writes `stage3/probe.json` with the measured
per-write barrier rate and the projection for the eight arm-B synchronous grid
cells, so the watch sees the real cost before the grid. Phase B (`--calibrate`,
once, when `stage3/calibration.json` is absent) runs the three point cells at
total C=16 and freezes `measurement_operations = 40 s x rate` per operation
family in `stage3/calibration.json`; every later run reads that file, so the
counts never move under the grid. Phase C/D is the 72-cell grid.

Every cell also gets `resources.csv` and `resources.json` in its output: per
2 s `/proc` samples of the cell's process (CPU, RSS, VmHWM, swap, io counters)
and of each host that ran or owned the cell (`/proc/vmstat` swap,
`/sys/block/*/stat` device bytes), written with the same parser as
`proc_sampler`, the module `manual_sui` uses too, plus the cell's payload bytes
and the device-write ratio.

`--resume` skips passed cells whose config hash matches and reruns failed,
pending, invalid and interrupted ones in plan order: arm A native and
synchronous cells first, then arm B, each tier in one owner start. `--only` and
`--from` narrow the plan; unit members always run together.
"""
import argparse
import csv
import io
from pathlib import Path
import shlex
import shutil
import socket
import subprocess
import sys
import tarfile
import threading
import time
import json
import yaml

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / 'gcp-rdma'))
import campaign_ledger as ledger_module
import campaign_upload
import gcp_manual_storage
import host_exec
import mailbox_exec
import manual_sui
import proc_sampler
import stage3_specs

STAGE = 'stage3'
OWNER_READY_SECONDS = 120
OWNER_STOP_SECONDS = 30
TIER_ORDER = {'native': 0, 'synchronous': 1}


class CellFailure(RuntimeError):
    """A cell process failed; the record carries what did run."""

    def __init__(self, message, record):
        super().__init__(message)
        self.record = record


def quote(value):
    return shlex.quote(str(value))


def cell_label(cell):
    """What the run record states about where the drivers and the store ran."""
    if cell['arm'] == 'B':
        return ('native arm: the native backend is in-process, so the driver and '
                'the store share the owner host')
    return ('distributed arm: coordinator and callers on the coordinator host; '
            'two owners on their own hosts over RDMA NVMe-oF')


def spec_hash(path):
    return ledger_module.sha256_file(path)


def cells_for(root, *, engine_rev, sui_rev, binary_ids, spec_dir=None):
    root = Path(root)
    spec_dir = Path(spec_dir) if spec_dir else root / 'stage3' / 'spec'
    cells = []
    for case in stage3_specs.cells():
        path = spec_dir / f'{case["case_id"]}.json'
        if not path.is_file():
            raise ValueError(f'missing generated spec: {path}')
        cells.append(dict(id=case['case_id'], stage=STAGE,
                          unit=case['case_id'], arm=case['arm'], tier=case['tier'],
                          config_hash=ledger_module.config_hash(
                              spec=spec_hash(path), knobs=dict(case=case),
                              engine_rev=engine_rev, sui_rev=sui_rev,
                              binary_ids=binary_ids),
                          path=str(path)))
    return cells


def artifacts(output):
    rows = []
    for path in sorted(Path(output).rglob('*')):
        if path.is_file() and not path.is_symlink():
            rows.append(dict(path=str(path), size=path.stat().st_size,
                             sha256=ledger_module.sha256_file(path)))
    if not (Path(output) / 'summary.json').is_file():
        raise ValueError('cell produced no summary.json')
    return rows


def archive_previous(path, stale_root, case, attempt):
    if not Path(path).exists():
        return None
    target = Path(stale_root) / f'{case}-attempt-{attempt}'
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        shutil.rmtree(target)
    shutil.move(str(path), str(target))
    return str(target)


def write_record(output, record):
    output.mkdir(parents=True, exist_ok=True)
    path = Path(output) / 'run.json'
    path.write_text(json.dumps(record, indent=2, sort_keys=True) + '\n')
    return path


class LocalRunner:
    """`kv-research` in this process's host: arm A on the coordinator and every rehearsal cell."""

    def __init__(self, *, binary, data_root, stale_root, mode='cluster', remote=None):
        self.binary = Path(binary)
        self.data_root = Path(data_root)
        self.stale_root = Path(stale_root)
        self.mode = mode
        self.remote = remote

    def run(self, cell, *, output, attempt, timeout=3600, watchers=()):
        database = self.data_root / 'stage3' / 'db' / cell['id']
        archived = archive_previous(database, self.stale_root, cell['id'] + '-db', attempt)
        command = [str(self.binary), '--config', cell['path'], '--output', str(output)]
        sampler = proc_sampler.Sampler(self.remote)
        for host, pid, role in watchers:
            sampler.watch_host(host, pid, role)
        started = time.time_ns()
        process = subprocess.Popen(command, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True)
        sampler.watch_local(process.pid, 'coordinator' if cell['arm'] == 'A' else 'store')
        sampler.start()
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
            sampler.stop()
            resources = sampler.write(output, case=cell['id'], mode=self.mode,
                                      label=cell_label(cell))
            raise CellFailure(f'cell exceeded {timeout} s', self.record(
                cell, output, command, None, started, archived, stdout=stdout,
                stderr=stderr, resources=resources)) from None
        sampler.stop()
        resources = sampler.write(output, case=cell['id'], mode=self.mode,
                                  label=cell_label(cell))
        record = self.record(cell, output, command, process.returncode, started,
                             archived, stdout=stdout, stderr=stderr, resources=resources)
        if process.returncode != 0:
            raise CellFailure(f'exit {process.returncode}: {record["stderr_tail"]}', record)
        collect_evidence(database / 'evidence', output / 'evidence')
        return record

    def record(self, cell, output, command, exit_code, started, archived, *,
               stdout=None, stderr=None, resources=None):
        return dict(cell=cell['id'], mode=self.mode, execution='local',
                    label=cell_label(cell), resources=resources,
                    host=socket.gethostname(), client_host=socket.gethostname(),
                    store_host=socket.gethostname(), command=command,
                    exit_code=exit_code, started_unix_ns=started,
                    finished_unix_ns=time.time_ns(), archived_database=archived,
                    database=str(self.data_root / 'stage3' / 'db' / cell['id']),
                    output=str(output),
                    stdout_tail=(stdout or '')[-4000:], stderr_tail=(stderr or '')[-4000:])


class HostRunner:
    """`kv-research` on one cluster host; the output tree is copied back here."""

    def __init__(self, *, remote, host, binary, data_root, mode='cluster'):
        self.remote = remote
        self.host = host
        self.binary = Path(binary)
        self.data_root = Path(data_root)
        self.mode = mode

    def _pid(self, remote_spec):
        """The newest process whose command line names this cell's spec."""
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            code, out, _ = self.remote.shell(
                self.host, f'pgrep -n -f -- {quote(str(remote_spec))}', check=False)
            if code == 0:
                text = out.decode().strip()
                if text.isdigit():
                    return int(text)
            time.sleep(0.1)
        return None

    def run(self, cell, *, output, attempt, timeout=3600, watchers=()):
        spec = Path(cell['path']).read_bytes()
        remote_spec = self.data_root / 'stage3' / 'spec' / f'{cell["id"]}.json'
        database = self.data_root / 'stage3' / 'db' / cell['id']
        remote_output = self.data_root / 'stage3' / 'out' / cell['id']
        self.remote.shell(self.host,
                          f'rm -rf {quote(database)} {quote(remote_output)} && '
                          f'mkdir -p {quote(remote_spec.parent)} {quote(remote_output)}')
        self.remote.shell(self.host, f'cat > {quote(remote_spec)}', stdin=spec)
        command = [str(self.binary), '--config', str(remote_spec),
                   '--output', str(remote_output)]
        result = {}

        def call():
            result['value'] = self.remote.shell(
                self.host, ' '.join(quote(part) for part in command), check=False)

        thread = threading.Thread(target=call, daemon=True)
        sampler = proc_sampler.Sampler(self.remote)
        started = time.time_ns()
        thread.start()
        pid = self._pid(remote_spec)
        if pid is not None:
            sampler.watch_host(self.host, pid, 'store')
        sampler.start()
        thread.join(timeout)
        timed_out = thread.is_alive()
        if timed_out and pid is not None:
            self.remote.shell(self.host, f'kill -KILL {pid}', check=False)
            thread.join(60)
        sampler.stop()
        if timed_out:
            resources = sampler.write(output, case=cell['id'], mode=self.mode,
                                      label=cell_label(cell))
            raise CellFailure(
                f'{self.host}: cell exceeded {timeout} s', dict(
                    cell=cell['id'], mode=self.mode, execution='host',
                    label=cell_label(cell), resources=resources, host=self.host,
                    client_host=self.host, store_host=self.host, command=command,
                    sampled_pid=pid, exit_code=None, started_unix_ns=started,
                    finished_unix_ns=time.time_ns(), archived_database='remote-fresh',
                    database=str(database), output=str(remote_output),
                    local_output=str(output), timeout_seconds=timeout,
                    stdout_tail='', stderr_tail='cell timed out; the process was killed'))
        code, out, err = result['value']
        record = dict(cell=cell['id'], mode=self.mode, execution='host',
                      label=cell_label(cell), resources=None, host=self.host,
                      client_host=self.host, store_host=self.host, command=command,
                      sampled_pid=pid, exit_code=code, started_unix_ns=started,
                      finished_unix_ns=time.time_ns(), archived_database='remote-fresh',
                      database=str(database), output=str(remote_output),
                      local_output=str(output),
                      stdout_tail=out.decode(errors='replace')[-4000:],
                      stderr_tail=err.decode(errors='replace')[-4000:])
        if code != 0:
            raise CellFailure(f'{self.host}: exit {code}: {record["stderr_tail"]}', record)
        self.remote.shell(
            self.host, f'if [ -d {quote(database / "evidence")} ]; then '
                       f'mv {quote(database / "evidence")} {quote(remote_output / "evidence")}; fi')
        archive = self.remote.run(
            self.host, f'tar -czf - -C {quote(remote_output)} .')[1]
        extract_archive(archive, output)
        # Written after the copy-back so the payload bytes are in the output tree.
        resources = sampler.write(output, case=cell['id'], mode=self.mode,
                                  label=cell_label(cell))
        record['resources'] = resources
        return record


def collect_evidence(source, target):
    """Move the backend's read-evidence export into the uploaded output tree."""
    if Path(source).is_dir():
        shutil.move(str(source), str(target))
    return Path(target)


def extract_archive(data, target):
    """Extract a regular-file-only tar held in memory under `target`."""
    target = Path(target)
    target.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:gz') as archive:
        for member in archive.getmembers():
            relative = Path(member.name)
            if (relative.is_absolute() or '..' in relative.parts
                    or not (member.isfile() or member.isdir())):
                raise ValueError(f'unsafe archive member: {member.name}')
        if hasattr(tarfile, 'data_filter'):
            archive.extractall(target, filter='data')
        else:
            archive.extractall(target)
    return target


class Bucket:
    """Verified artifact upload under the campaign prefix; passed means uploaded."""

    def __init__(self, objects, prefix):
        self.objects = objects
        self.prefix = prefix
        self.evidence = campaign_upload.Evidence(objects, prefix)
        self.ledger_store = self.evidence.ledger_store

    def key(self):
        return self.evidence.ledger_key

    def publish_file(self, cell_id, path, attempt=1):
        """Upload one compact file and verify the stored size and SHA-256."""
        path = Path(path)
        row = campaign_upload.artifact(path, path.name)
        receipts = self.evidence.upload([row], stage=STAGE, cell_id=cell_id,
                                        attempt=attempt)
        return receipts[0]['key']

    def publish(self, case_id, produced, output, attempt=1):
        for artifact in produced:
            artifact['name'] = Path(artifact['path']).relative_to(output).as_posix()
        receipts = self.evidence.upload(produced, stage=STAGE, cell_id=case_id,
                                        attempt=attempt)
        keys = {row['path']: row['key'] for row in receipts}
        for artifact in produced:
            artifact['key'] = keys[artifact['path']]
        return produced

    def save(self, ledger):
        return self.evidence.save(ledger)[0]

    def merge(self, ledger):
        """Read the campaign ledger so a resume on another host starts from it."""
        return self.evidence.merge(ledger)


class Owners:
    """The stage-3 owner process groups of one tier's external preparation."""

    def __init__(self, remote, preparations, *, root, guest_root):
        self.remote = remote
        self.preparations = preparations
        self.root = Path(root)
        self.guest_root = Path(guest_root)
        self.active = None
        self.installed = None
        self.namespaces = {}
        self.receipts = []

    def _install(self, prepared):
        for record in prepared['manifest']['hosts']:
            archive = Path(record['archive'])
            if ledger_module.sha256_file(archive) != record['archive_sha256']:
                raise ValueError(f'owner archive changed after preparation: {archive}')
            host = record['host']
            self.remote.shell(host, f'rm -rf {quote(self.guest_root)} && '
                                    f'mkdir -p {quote(self.guest_root)}')
            self.remote.shell(host, f'tar -xzf - -C {quote(self.guest_root)}',
                              stdin=archive.read_bytes())
            self.remote.shell(host, f'chmod +x {quote(self.guest_root / "storage-node")}')

    def _verify(self, prepared):
        """The selected commands start the selected database's owners.

        `prepare_external.py` writes one command and one ready file per host in
        database order, so the database's index selects them. Its launch file's
        `listen` address must be that database's endpoint for the partition.
        """
        index = prepared['index']
        addresses = {endpoint['partition']: endpoint['address']
                     for endpoint in prepared['document']['endpoints']}
        for record in prepared['manifest']['hosts']:
            command = record['commands'][index]
            launch = Path(command[2])
            try:
                local = prepared['directory'] / launch.relative_to(self.guest_root)
            except ValueError:
                raise ValueError(f'owner command is outside the guest root: {launch}') from None
            listen = yaml.safe_load(local.read_text())['listen']
            if listen != addresses[record['partition']]:
                raise ValueError(
                    f'owner command for partition {record["partition"]} listens on '
                    f'{listen}, not {addresses[record["partition"]]}')

    def install(self, tier):
        """Put one tier's owner archive on its hosts without starting a process."""
        if self.installed == tier:
            return self.preparations[tier]
        self.stop()
        prepared = self.preparations[tier]
        self._verify(prepared)
        self._install(prepared)
        self.installed = tier
        return prepared

    def watchers(self):
        """`(host, pid, role)` for the running owner processes."""
        return [(host, entry['pid'], 'owner')
                for host, entry in self.namespaces.items()
                if not entry.get('stopped')]

    def start(self, tier):
        if self.active == tier:
            return self.receipts
        prepared = self.install(tier)
        index = prepared['index']
        # Published per host as it starts, so a partial start is still stopped.
        self.namespaces = {}
        for record in prepared['manifest']['hosts']:
            host = record['host']
            command = record['commands'][index]
            partition = record['partition']
            pidfile = self.guest_root / f'owner-{partition}.pid'
            log = self.guest_root / f'owner-{partition}.log'
            quoted = ' '.join(quote(part) for part in command)
            self.remote.shell(host, f'cd {quote(self.guest_root)} && '
                                    f'setsid sh -c {quote(f"echo $$ > {pidfile}; exec {quoted}")} '
                                    f'</dev/null >>{quote(log)} 2>&1 &')
            deadline = time.monotonic() + 30
            while not self.remote.exists(host, pidfile):
                if time.monotonic() > deadline:
                    raise manual_sui.LaunchError(f'{host}: owner did not publish a PID')
                time.sleep(0.1)
            pid = int(self.remote.read_bytes(host, pidfile).strip())
            self.namespaces[host] = dict(partition=partition, command=command, pid=pid,
                                         process_group=pid, log=str(log), stopped=False)
        for record in prepared['manifest']['hosts']:
            ready = record['ready_files'][index]
            deadline = time.monotonic() + OWNER_READY_SECONDS
            while not self.remote.exists(record['host'], ready):
                if time.monotonic() > deadline:
                    raise manual_sui.LaunchError(f'{record["host"]}: owner readiness expired')
                time.sleep(0.2)
        self.active = tier
        self.receipts = self._copy_receipts(prepared)
        return self.receipts

    def _copy_receipts(self, prepared):
        index = prepared['index']
        rows = []
        for record in prepared['manifest']['hosts']:
            host = record['host']
            partition = record['partition']
            published = str(record['ready_files'][index]) + '.durability.json'
            data = self.remote.read_bytes(host, published)
            receipt = json.loads(data)
            if (receipt.get('partition') != partition
                    or receipt.get('durability', {}).get('tier') != self.active
                    or not receipt.get('launch_nonce')):
                raise ValueError(f'{host}: owner receipt differs from partition/tier')
            target = stage3_specs.receipt_path(self.root, self.active, partition)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            rows.append(dict(host=host, partition=partition, published=published,
                             path=str(target), sha256=ledger_module.sha256_bytes(data),
                             launch_nonce=receipt['launch_nonce']))
        return rows

    def stop(self):
        failures = []
        for host, entry in self.namespaces.items():
            if entry.get('stopped'):
                continue
            self.remote.shell(host, f'kill -TERM -{entry["process_group"]}', check=False)
            deadline = time.monotonic() + OWNER_STOP_SECONDS
            while self.remote.exists(host, f'/proc/{entry["pid"]}'):
                if time.monotonic() > deadline:
                    break
                time.sleep(0.2)
            if self.remote.exists(host, f'/proc/{entry["pid"]}'):
                self.remote.shell(host, f'kill -KILL -{entry["process_group"]}', check=False)
                deadline = time.monotonic() + 10
                while self.remote.exists(host, f'/proc/{entry["pid"]}'):
                    if time.monotonic() > deadline:
                        break
                    time.sleep(0.1)
            entry['stopped'] = not self.remote.exists(host, f'/proc/{entry["pid"]}')
            if not entry['stopped']:
                failures.append(host)
        self.active = None if not failures else self.active
        return failures


ARM_B_SYNCHRONOUS_CELLS = 8
CALIBRATION_FAMILIES = ('read', 'mixed', 'write')


def calibration_case(family):
    """Plan section 9.4: a point cell of the family at total C=16, Native."""
    return dict(case_id=f'S3-CAL-{family}-c16-native', arm='A', mix=family, size=8192,
                distribution='uniform', tier='native', concurrency=16, batch=None,
                cache=stage3_specs.GRID_CACHE_ENTRIES, reference=False)


def write_calibration_specs(root, *, external):
    root = Path(root)
    written = []
    for family in CALIBRATION_FAMILIES:
        case = calibration_case(family)
        written.append(stage3_specs.write_one(
            root / 'stage3' / 'calibration' / 'spec' / f'{case["case_id"]}.json',
            stage3_specs.run_config(
                case, root, external=external,
                measurement_operations=stage3_specs.CALIBRATION_OPERATIONS)))
    return written


def entry_identity(path, *, knobs, engine_rev, sui_rev, binary_ids):
    return ledger_module.config_hash(spec=spec_hash(path), knobs=knobs,
                                     engine_rev=engine_rev, sui_rev=sui_rev,
                                     binary_ids=binary_ids)


def phase_cell(*, path, case, identity, group):
    return dict(id=case['case_id'], stage=STAGE, unit=group, arm=case['arm'],
                tier=case['tier'], config_hash=identity, path=str(path))


def run_phase_cell(cell, runner, *, root, ledger, bucket, stale_root, timeout,
                   watchers=(), allow_local_ledger=False):
    """One phase-A/B cell under the same ledger and upload rules as the grid."""
    if bucket is None and not allow_local_ledger:
        raise ValueError('a passed cell needs a verified evidence upload; '
                         'add --bucket-prefix, or --allow-local-ledger for a rehearsal')
    entry = ledger.begin(stage=STAGE, cell_id=cell['id'],
                         config_hash=cell['config_hash'], unit=cell['unit'],
                         start_from='fresh')
    save(ledger, bucket)
    output = Path(root) / 'stage3' / cell['unit'] / 'out' / cell['id']
    try:
        produced = run_cell(cell, runner, output=output, stale_root=stale_root,
                            attempt=entry['attempts'], timeout=timeout, watchers=watchers)
        if bucket is not None:
            bucket.publish(cell['id'], produced, output, attempt=entry['attempts'])
            ledger.mark_passed(entry, produced, store=bucket.ledger_store)
        else:
            campaign_upload.pass_local(ledger, entry, produced)
        save(ledger, bucket)
    except BaseException as error:
        ledger.fail(entry, error)
        save(ledger, bucket)
        raise
    return output


def run_probe(*, path, identity, runner, owners, root, ledger, bucket, timeout,
              allow_local_ledger=False):
    """Phase A: one arm-B synchronous write cell before anything else runs."""
    cell = phase_cell(path=path, case=stage3_specs.probe_case(), identity=identity,
                      group='probe')
    # Arm B only needs the storage binary on its host; the native archive is the
    # one the first grid tier uses, so it is installed once, not twice.
    owners.install('native')
    output = run_phase_cell(cell, runner, root=root, ledger=ledger, bucket=bucket,
                            stale_root=Path(root) / 'stage3' / 'stale', timeout=timeout,
                            allow_local_ledger=allow_local_ledger)
    totals = proc_sampler.sample_totals(output / 'samples.csv')
    record = json.loads((output / 'run.json').read_text())
    return dict(cell=cell['id'], host=record.get('host'), output=str(output),
                wall_seconds=(record['finished_unix_ns'] - record['started_unix_ns']) / 1e9,
                measurement_calls=totals['calls'],
                measurement_window_seconds=totals['window_seconds'],
                calls_per_second=totals['calls_per_second'],
                preload_seconds=max(0.0, (record['finished_unix_ns']
                                          - record['started_unix_ns']) / 1e9
                                    - totals['window_seconds']))


def projection(measurements, counts, *, counts_source):
    """What one probe cell predicts for the synchronous arm-B grid cells."""
    rate = measurements['calls_per_second']
    value = dict(measurements, counts_source=counts_source,
                 grid_measurement_operations=counts['write'],
                 arm_b_synchronous_cells_in_plan=ARM_B_SYNCHRONOUS_CELLS,
                 note=('arm B is one in-process native database with a per-write WAL '
                       'barrier; the projection replaces the probe measurement window '
                       'by 1.1 x the frozen write count at the measured rate and keeps '
                       'the measured preload time'))
    if not rate:
        value['error'] = 'the probe measurement window was empty'
        return value
    grid_calls = counts['write'] * (1 + stage3_specs.WARMUP_FRACTION)
    value['projected_grid_cell_seconds'] = (
        measurements['preload_seconds'] + grid_calls / rate)
    value['projected_arm_b_synchronous_seconds'] = (
        value['projected_grid_cell_seconds'] * ARM_B_SYNCHRONOUS_CELLS)
    return value


def calibrate(*, root, runner, owners, ledger, bucket, external, identity_for,
              timeout, allow_local_ledger=False):
    """Phase B: three calibration cells, then the frozen per-family counts."""
    root = Path(root)
    paths = write_calibration_specs(root, external=external)
    owners.start('native')
    rates, cells = {}, []
    for family, path in zip(CALIBRATION_FAMILIES, paths):
        case = calibration_case(family)
        cell = phase_cell(path=path, case=case, group='calibration',
                          identity=identity_for(path, dict(calibration=family)))
        output = run_phase_cell(cell, runner, root=root, ledger=ledger, bucket=bucket,
                                stale_root=root / 'stage3' / 'stale', timeout=timeout,
                                watchers=owners.watchers(),
                                allow_local_ledger=allow_local_ledger)
        totals = proc_sampler.sample_totals(output / 'samples.csv')
        rates[family] = totals['calls_per_second']
        cells.append(dict(cell=cell['id'], measurement_calls=totals['calls'],
                          measurement_window_seconds=totals['window_seconds'],
                          calls_per_second=totals['calls_per_second'],
                          expected_window_seconds=stage3_specs.WINDOW_SECONDS))
    counts = stage3_specs.calibration_counts(rates)
    document = dict(schema=1, window_seconds=stage3_specs.WINDOW_SECONDS,
                    pilot_operations=stage3_specs.CALIBRATION_OPERATIONS,
                    family_rates=rates, counts=counts, cells=cells)
    path = root / 'stage3' / 'calibration.json'
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + '\n')
    if bucket is not None:
        bucket.publish_file('calibration', path)
    return counts


def write_projection(root, bucket, document):
    root = Path(root)
    path = root / 'stage3' / 'probe.json'
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + '\n')
    if bucket is not None:
        bucket.publish_file('probe', path)
    return path


def schedule(cells):
    """Arm A before arm B, and one owner start per tier: native, then synchronous."""
    return sorted(cells, key=lambda cell: (cell.get('arm') == 'B',
                                           TIER_ORDER[cell['tier']]))


def run_cell(cell, runner, *, output, stale_root, attempt, timeout=3600, watchers=()):
    """Run one cell from a fresh database and output; return its artifacts."""
    archive_previous(output, stale_root, cell['id'], attempt)
    try:
        record = runner.run(cell, output=output, attempt=attempt, timeout=timeout,
                            watchers=watchers)
    except CellFailure as error:
        write_record(output, dict(error.record, failure=str(error)))
        raise
    write_record(output, record)
    return artifacts(output)


def run_campaign(cells, *, local, root, ledger, remote=None, owners=None, bucket=None,
                 only=None, from_=None, stop_on_failure=True, timeout=3600,
                 allow_local_ledger=False):
    if bucket is None and not allow_local_ledger:
        raise ValueError('a passed cell needs a verified evidence upload; '
                         'add --bucket-prefix, or --allow-local-ledger for a rehearsal')
    root = Path(root)
    ledger.path = Path(ledger.path) if ledger.path else root / 'campaign-ledger.json'
    stale_root = root / 'stage3' / 'stale'
    planned = schedule(ledger_module.plan(list(cells), ledger, only=only, from_=from_))
    if local.remote is None and owners is not None:
        # Owner processes are sampled through the same SSH callback.
        local.remote = owners.remote
    results = []
    try:
        for cell in planned:
            entry = ledger.begin(stage=cell['stage'], cell_id=cell['id'],
                                 config_hash=cell['config_hash'], unit=cell.get('unit'),
                                 start_from='fresh')
            save(ledger, bucket)
            runner = local if cell['arm'] == 'A' else remote
            if runner is None and local.mode == 'rehearsal':
                runner = local
            try:
                if runner is None:
                    raise ValueError(f'arm {cell["arm"]} cell {cell["id"]} has no runner')
                if cell['arm'] == 'B' and owners is not None:
                    owners.stop()
                if cell['arm'] == 'A' and owners is not None:
                    owners.start(cell['tier'])
                output = root / 'stage3' / 'out' / cell['id']
                produced = run_cell(
                    cell, runner, output=output, stale_root=stale_root,
                    attempt=entry['attempts'], timeout=timeout,
                    watchers=owners.watchers() if (owners and cell['arm'] == 'A') else ())
                if bucket is not None:
                    bucket.publish(cell['id'], produced, output, attempt=entry['attempts'])
                    ledger.mark_passed(entry, produced, store=bucket.ledger_store)
                else:
                    campaign_upload.pass_local(ledger, entry, produced)
                save(ledger, bucket)
                results.append(dict(id=cell['id'], status='passed', mode=runner.mode))
            except BaseException as error:
                ledger.fail(entry, error)
                save(ledger, bucket)
                results.append(dict(id=cell['id'], status='failed', mode=runner.mode,
                                    failure=str(error)))
                if stop_on_failure:
                    break
    finally:
        if owners is not None:
            failed = owners.stop()
            results.append(dict(id='owner-processes',
                                status='failed' if failed else 'stopped',
                                detail=sorted(failed)))
    return results, planned


def save(ledger, bucket):
    if bucket is None:
        ledger.save()
    else:
        bucket.save(ledger)


def bucket_store(prefix, account, deadline_seconds, *, guest=False):
    """One reviewed credential path: the delegated guest ObjectStore."""
    if account != gcp_manual_storage.ACCOUNT:
        raise SystemExit('only the reviewed guest identity is delegated: '
                         + gcp_manual_storage.ACCOUNT)
    if not prefix.startswith('gs://'):
        raise SystemExit('--bucket-prefix must be gs://<bucket>/<campaign prefix>')
    bucket, _, key_prefix = prefix[len('gs://'):].partition('/')
    if bucket != gcp_manual_storage.BUCKET or not key_prefix.strip('/'):
        raise SystemExit('--bucket-prefix must name '
                         f'gs://{gcp_manual_storage.BUCKET}/<campaign prefix>')
    if type(deadline_seconds) is not int or deadline_seconds <= 0:
        raise SystemExit('--bucket-deadline-seconds must be a positive integer')
    store = gcp_manual_storage.DelegatedArtifacts(
        bucket, deadline=time.time() + deadline_seconds, guest=guest)
    return Bucket(store, key_prefix.strip('/'))


def plan_hosts(owners):
    if owners is None or len(owners) != stage3_specs.OWNERS:
        raise SystemExit(f'--owners needs exactly {stage3_specs.OWNERS} owner hosts')
    if len(set(owners)) != len(owners):
        raise SystemExit('owner hosts must be distinct')
    return list(owners)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True,
                        help='campaign root with stage3/{spec,db,out,stale}')
    parser.add_argument('--binary', type=Path, required=True,
                        help='kv-research (the campaign bundle calls it storage-node)')
    parser.add_argument('--ledger', type=Path)
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--only', action='append', default=[])
    parser.add_argument('--from', dest='from_')
    parser.add_argument('--continue-on-failure', action='store_true')
    parser.add_argument('--engine-rev', default='unknown')
    parser.add_argument('--sui-rev', default='unknown')
    parser.add_argument('--timeout', type=int, default=3600)
    parser.add_argument('--measurement-operations', type=int,
                        default=stage3_specs.MEASUREMENT_OPERATIONS,
                        help='one count for every family when nothing is calibrated')
    parser.add_argument('--calibrate', action='store_true',
                        help='run the three phase-B calibration cells and freeze their counts')
    parser.add_argument('--skip-probe', action='store_true',
                        help='do not run the phase-A arm-B synchronous probe cell')
    parser.add_argument('--mode', choices=('cluster', 'rehearsal'), default='cluster')
    parser.add_argument('--external', action='append', default=[], metavar='TIER=DIR',
                        help='prepare_external.py output per tier, repeated (cluster)')
    parser.add_argument('--external-database', help='config.json database key every cell uses')
    parser.add_argument('--native-root', type=Path,
                        help='absolute store root of the owner host (cluster arm B)')
    parser.add_argument('--guest-root', type=Path,
                        help='absolute external deployment root, identical on every host')
    parser.add_argument('--owners', help='owner hosts, comma separated (H1,H2)')
    parser.add_argument('--native-host', help='owner host that runs arm B; defaults to owners[0]')
    parser.add_argument('--zone', help='zone of the three hosts, for the SSH callback')
    parser.add_argument('--iap', action='store_true',
                        help='rehearsal only: tunnel remote commands through IAP')
    parser.add_argument('--fleet-state', type=Path,
                        help='gcp_manual_compute.py fleet state; the campaign mailbox executor')
    parser.add_argument('--fleet-config', type=Path,
                        help='gcp-rdma config.json that names the project and bucket')
    parser.add_argument('--gcloud-ssh', action='store_true',
                        help='rehearsal only: run through `gcloud compute ssh`')
    parser.add_argument('--on-coordinator', action='store_true',
                        help=argparse.SUPPRESS)
    parser.add_argument('--guest-token', action='store_true',
                        help=argparse.SUPPRESS)
    parser.add_argument('--coordinator-seconds', type=int,
                        help='bound on the coordinator run; defaults to the fleet deadline')
    parser.add_argument('--bucket-prefix',
                        help='gs://mysten-research-bench-artifacts/<campaign prefix>')
    parser.add_argument('--impersonate-service-account', default=gcp_manual_storage.ACCOUNT)
    parser.add_argument('--bucket-deadline-seconds', type=int, default=7200)
    parser.add_argument('--allow-local-ledger', action='store_true',
                        help='labelled local rehearsal only: pass a cell without an '
                             'uploaded bundle')
    args = parser.parse_args()
    root = args.root.resolve()
    cluster = args.mode == 'cluster'
    if cluster:
        required = dict(external=args.external, guest_root=args.guest_root,
                        owners=args.owners, zone=args.zone, native_root=args.native_root,
                        bucket_prefix=args.bucket_prefix)
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise SystemExit('cluster mode needs --' + ', --'.join(
                name.replace('_', '-') for name in missing)
                + '; pass --mode rehearsal for the labelled single-host fixture')
        if not args.guest_root.is_absolute() or not args.native_root.is_absolute():
            raise SystemExit('--guest-root and --native-root must be absolute')
        if args.bucket_prefix and not args.bucket_prefix.startswith('gs://'):
            raise SystemExit('--bucket-prefix must be gs://<bucket>/<campaign prefix>')
    elif args.external or args.guest_root or args.owners or args.native_root:
        raise SystemExit('rehearsal mode runs the local fixture and takes no '
                         '--external, --guest-root, --owners or --native-root')
    elif not args.allow_local_ledger:
        raise SystemExit('a passed cell needs a verified evidence upload; give '
                         '--bucket-prefix, or --allow-local-ledger for a labelled rehearsal')
    external = (stage3_specs.external_map(stage3_specs.external_items(args.external),
                                          database=args.external_database)
                if cluster else None)
    if external is not None:
        expected = {preparation['manifest']['storage_binary_sha256']
                    for preparation in external.values()}
        if len(expected) != 1 or ledger_module.sha256_file(args.binary) not in expected:
            raise SystemExit('--binary is not the storage binary the external '
                             'preparation was built from')
    owner_hosts = plan_hosts(args.owners.split(',')) if cluster else []
    if cluster and not args.gcloud_ssh and not args.on_coordinator:
        # Stage-3 arm A and the probe are local children of this driver and must
        # dial the owners, so the campaign runs on the coordinator host. Arm B
        # already runs on `--native-host` through the same seam.
        remote = mailbox_exec.open_executor(args)
        coordinator = mailbox_exec.coordinator_of(args.fleet_state)
        state = json.loads(Path(args.fleet_state).read_text())
        campaign = Path('/mnt/tidehunter') / state['run_id'] / 'campaign'
        binary = Path(args.binary)
        remote.send_file(coordinator, campaign / 'binaries' / binary.name, binary)
        external = []
        for item in args.external or []:
            tier, _, local = item.partition('=')
            remote.stage_tree(coordinator, Path(local), campaign / 'external' / tier)
            external.append(tier + '=' + str(campaign / 'external' / tier))
        remote.send_file(coordinator, campaign / 'fleet-state.json', Path(args.fleet_state))
        remote.send_file(coordinator, campaign / 'config.json', Path(args.fleet_config).resolve())
        stale = set(args.external or [])
        argv = [item for item in sys.argv[1:]
                if item not in stale and item not in ('--external', '--on-coordinator')]
        argv = mailbox_exec.rewrite(
            argv, {'--root': campaign / 'root',
                   '--binary': campaign / 'binaries' / binary.name,
                   '--ledger': campaign / 'campaign-ledger.json'})
        extra = ['--on-coordinator', '--guest-token',
                 '--fleet-state', str(campaign / 'fleet-state.json'),
                 '--fleet-config', str(campaign / 'config.json')]
        # One `--external` per tier: argparse takes one value per flag.
        for value in external:
            extra += ['--external', value]
        argv = mailbox_exec.with_flags(argv, extra)
        code, out, err = remote.run_driver(
            coordinator,
            f'/mnt/tidehunter/{state["run_id"]}/package/source/experiments/sui-checkpoint/stage3_campaign.py',
            argv, timeout_seconds=args.coordinator_seconds)
        sys.stdout.write(out.decode('utf-8', 'replace'))
        sys.stderr.write(err.decode('utf-8', 'replace'))
        return code
    path = args.ledger or root / 'campaign-ledger.json'
    if Path(path).exists() and not args.resume:
        raise SystemExit(f'ledger exists; use --resume or a new --ledger: {path}')
    ledger = ledger_module.Ledger(path)
    if args.resume:
        ledger.load()
    bucket = (bucket_store(args.bucket_prefix, args.impersonate_service_account,
                           args.bucket_deadline_seconds, guest=args.guest_token)
              if args.bucket_prefix else None)
    if args.resume and bucket is not None:
        ledger = bucket.merge(ledger)
    binary_ids = {name: ledger_module.sha256_file(Path(args.binary))
                  for name in ('kv-research',)}
    native_root = args.native_root if cluster else None

    def identity_for(spec_path, knobs):
        return entry_identity(spec_path, knobs=knobs, engine_rev=args.engine_rev,
                              sui_rev=args.sui_rev, binary_ids=binary_ids)

    local = LocalRunner(binary=args.binary, data_root=root,
                        stale_root=root / 'stage3' / 'stale', mode=args.mode)
    remote = owners = None
    if cluster:
        remote_transport = mailbox_exec.open_executor(args)
        local.remote = remote_transport
        remote = HostRunner(remote=remote_transport,
                            host=args.native_host or owner_hosts[0],
                            binary=args.guest_root / 'storage-node',
                            data_root=args.native_root, mode=args.mode)
        owners = Owners(remote_transport, external, root=root,
                        guest_root=args.guest_root)

    # The probe only exists in cluster mode; the rehearsal set has no store host.
    probe_spec = (stage3_specs.write_probe(root, native_root=native_root,
                                           external=external)
                  if cluster else None)
    counts = None
    calibration_path = root / 'stage3' / 'calibration.json'
    if calibration_path.is_file():
        counts = stage3_specs.read_calibration(calibration_path)
    measurements = probe_note = None
    if cluster and owners is not None and not args.skip_probe:
        probe = phase_cell(path=probe_spec, case=stage3_specs.probe_case(),
                           identity=identity_for(probe_spec, dict(phase='probe')),
                           group='probe')
        previous = ledger.entries.get((STAGE, probe['id']))
        if (previous and previous['status'] == ledger_module.PASSED
                and previous['config_hash'] == probe['config_hash']):
            path = root / 'stage3' / 'probe.json'
            measurements = json.loads(path.read_text()) if path.is_file() else None
            if measurements is None:
                probe_note = 'the probe already passed here but probe.json is absent'
        else:
            try:
                measurements = run_probe(path=probe_spec, identity=probe['config_hash'],
                                         runner=remote, owners=owners, root=root,
                                         ledger=ledger, bucket=bucket,
                                         timeout=args.timeout,
                                         allow_local_ledger=args.allow_local_ledger)
            except BaseException as error:
                raise SystemExit(f'the phase-A probe cell failed: {error}') from error
    if counts is None and args.calibrate:
        if not cluster:
            raise SystemExit('--calibrate needs the cluster arm-A owner set')
        try:
            counts = calibrate(root=root, runner=local, owners=owners, ledger=ledger,
                               bucket=bucket, external=external,
                               identity_for=identity_for, timeout=args.timeout,
                               allow_local_ledger=args.allow_local_ledger)
        except BaseException as error:
            raise SystemExit(f'the phase-B calibration failed: {error}') from error
    if counts is None:
        counts = stage3_specs.operation_counts(args.measurement_operations)
    stage3_specs.write(root, args.measurement_operations, counts=counts,
                       native_root=native_root, external=external)
    cells = cells_for(root, engine_rev=args.engine_rev, sui_rev=args.sui_rev,
                      binary_ids=binary_ids)
    if measurements is not None:
        document = projection(measurements, counts,
                              counts_source=('frozen calibration'
                                             if calibration_path.is_file()
                                             else 'phase-B calibration of this run'))
        write_projection(root, bucket, document)
    results, planned = run_campaign(
        cells, local=local, root=root, ledger=ledger, remote=remote, owners=owners,
        bucket=bucket, only=args.only or None, from_=args.from_,
        stop_on_failure=not args.continue_on_failure, timeout=args.timeout,
        allow_local_ledger=args.allow_local_ledger)
    print(json.dumps(dict(mode=args.mode, counts=counts,
                          probe=str(root / 'stage3' / 'probe.json')
                          if measurements is not None else None,
                          probe_note=probe_note if cluster else None,
                          planned=[cell['id'] for cell in planned], results=results)))


if __name__ == '__main__':
    main()
