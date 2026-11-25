#!/usr/bin/env python3
"""Three-host Sui replay controller: one coordinator and two NVMe-oF owners.

The controller runs on the coordinator host. It prepares the external
deployment with the frozen `prepare_external.py`, installs each owner archive on
its host, starts the owner process groups, runs the paired `run.py` with
`--external-config/--external-resources/--external-stage-dir`, answers the
`resources-required` handshake with sampled `/proc` evidence, and stops only the
groups it started.

Remote commands go through one `execute(argv, stdin)` callback. The default
callback is `gcloud compute ssh --command`, so a test can substitute a fake
without a cloud account. Every owner archive is extracted into the same absolute
guest root on every host, as the engine manifest requires.
"""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'sui-checkpoint'))
import build
import campaign_ledger as ledger_module
import campaign_manifest
import checkpoint_support as checkpoint
import deployment
import gcp_api
import gcs_ledger

SAMPLE_SECONDS = 2.0
MAX_SAMPLES = 20000
OWNER_READY_SECONDS = 120
IO_COUNTERS = ('rchar', 'wchar', 'syscr', 'syscw', 'read_bytes', 'write_bytes',
               'cancelled_write_bytes')
BINARY_SET = {'sui-node-native', 'sui-node-remote', 'storage-node', 'state-audit',
              'rocksdb-audit'}


class LaunchError(RuntimeError):
    pass


def plan(coordinator, owners):
    """Validate the 1 coordinator + 2 owner shape and return the roles."""
    if not isinstance(owners, (list, tuple)) or len(owners) != 2:
        raise ValueError('exactly two owner hosts are required')
    if not coordinator or any(not host for host in owners):
        raise ValueError('coordinator and owner hosts must be nonempty')
    if len(set([coordinator, *owners])) != 3:
        raise ValueError('coordinator and owner hosts must be distinct')
    return dict(coordinator=coordinator, owners=list(owners))


class Remote:
    """One shell command per host through an injected gcloud argv builder."""

    def __init__(self, zone, *, execute=None, iap=False):
        self.zone, self.execute, self.iap = zone, execute or self._gcloud, iap

    def _gcloud(self, argv, stdin=None):
        completed = subprocess.run(argv, input=stdin, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, timeout=1800)
        return completed.returncode, completed.stdout, completed.stderr

    def argv(self, host, command):
        value = ['gcloud', 'compute', 'ssh', host, '--zone', self.zone,
                 '--quiet', f'--command={command}']
        if self.iap:
            value.append('--tunnel-through-iap')
        return value

    def run(self, host, command, *, stdin=None, check=True):
        code, out, err = self.execute(self.argv(host, command), stdin=stdin)
        if check and code != 0:
            raise LaunchError(f'{host}: command failed ({code}): '
                              f'{err.decode(errors="replace")[-2000:]}')
        return code, out, err

    def shell(self, host, command, *, stdin=None, check=True):
        return self.run(host, 'sh -lc ' + shlex.quote(command), stdin=stdin, check=check)

    def read_bytes(self, host, path):
        code, out, err = self.shell(host, f'cat {shlex.quote(str(path))}', check=False)
        if code != 0:
            raise LaunchError(f'{host}: cannot read {path}: {err.decode(errors="replace")}')
        return out

    def exists(self, host, path):
        code, _, _ = self.shell(host, f'test -e {shlex.quote(str(path))}', check=False)
        return code == 0


def parse_proc(stat, status, io):
    """One live process sample; a missing stream is an exited process."""
    if not stat or not status or not io:
        return None
    try:
        fields = stat.rsplit(b')', 1)[1].split()
        starttime = int(fields[19])
        values = {line.split(':')[0]: int(line.split()[1]) * 1024 for line in
                  status.decode().splitlines() if line.startswith(('VmRSS:', 'VmHWM:', 'VmSwap:'))}
        counters = {line.split(':')[0]: int(line.split()[1]) for line in
                    io.decode().splitlines()}
    except (IndexError, ValueError, UnicodeError):
        return None
    if set(values) != {'VmRSS', 'VmHWM', 'VmSwap'} or not set(IO_COUNTERS) <= set(counters):
        return None
    return dict(starttime_ticks=starttime, **values,
                io={key: counters[key] for key in IO_COUNTERS})


class SuiLauncher:
    def __init__(self, *, remote, build_root, fixture, output, guest_root, stage_dir,
                 config_dir, resources, tier='native', owners=2, fragment_mib=8,
                 deadline=900, extra_run=(), transport='efa', live_transport='rdma',
                 coordinator_cache_mib=8, shard_cache_mib=0, schema=None,
                 disk_sample_seconds=10, port=24000, ledger=None,
                 cell_id='sui-replay', stage='stage1', resume=False,
                 store=None, bucket=None, prefix=None, write_profile_limit=0,
                 resident_reads=None):
        self.remote = remote
        self.build_root = Path(build_root).resolve()
        self.fixture = Path(fixture).resolve()
        self.output = Path(output).resolve()
        self.guest_root = Path(guest_root)
        self.stage_dir = Path(stage_dir).resolve()
        self.config_dir = Path(config_dir).resolve()
        self.resources = Path(resources).resolve()
        self.tier = tier
        self.owners = owners
        self.fragment_mib = fragment_mib
        self.deadline = deadline
        self.extra_run = [item for item in extra_run if item != '--']
        self.transport = transport
        self.live_transport = live_transport
        self.coordinator_cache_mib = coordinator_cache_mib
        self.shard_cache_mib = shard_cache_mib
        self.schema = Path(schema).resolve() if schema else None
        self.disk_sample_seconds = disk_sample_seconds
        self.port = port
        self.manifest = json.loads((self.build_root / 'build-manifest.json').read_text())
        self.source = self.build_root / 'sources/tidehunter-sharded-wal/experiments/sui-checkpoint'
        self.hosts = []
        self.namespaces = {}
        self.samples = {}
        self.started = False
        self.external = None
        self.local_commands = []
        self.sample_thread = None
        self.sample_stop = threading.Event()
        self.ledger = ledger
        self.cell_id = cell_id
        self.stage = stage
        self.resume = resume
        self.store = store
        self.bucket = bucket
        self.prefix = prefix
        self.write_profile_limit = write_profile_limit
        self.resident_reads = Path(resident_reads).resolve() if resident_reads else None

    # ---- admission -----------------------------------------------------
    def admit(self, hosts):
        if set(self.manifest['binaries']) != BINARY_SET:
            raise ValueError('frozen build lacks the exact campaign executable set')
        for name, expected in self.manifest['binaries'].items():
            if build.digest(self.build_root / 'binaries' / name) != expected:
                raise ValueError(f'frozen binary differs: {name}')
        if not self.guest_root.is_absolute():
            raise ValueError('the guest root must be absolute and identical on every host')
        if self.schema is None or not self.schema.is_dir():
            raise ValueError('a native schema from a source-identical local run is required')
        if self.output.exists():
            raise ValueError('run output must be fresh')
        if self.stage_dir.exists() and any(self.stage_dir.iterdir()):
            raise ValueError('external stage directory must be fresh')
        if self.owners != 2 or len(hosts['owners']) != 2:
            raise ValueError('the Sui campaign uses exactly two owners')

    def remote_root(self):
        configuration = checkpoint.configuration(self.fixture, 'distributed')
        previous = Path(configuration['db-path'])
        relative = (previous.relative_to(self.fixture / 'distributed')
                    if previous.is_absolute() else previous.relative_to('distributed'))
        return self.output / 'remote' / relative

    # ---- preparation ---------------------------------------------------
    def prepare(self):
        manifest_path = self.config_dir / 'external-manifest.json'
        if manifest_path.exists():
            self.external = json.loads(manifest_path.read_text())
            if self.external.get('owners') != self.owners:
                raise ValueError('prepared owner count differs')
            return
        if self.config_dir.exists():
            raise ValueError('external config directory must be fresh')
        command = [sys.executable, str(self.source / 'prepare_external.py'),
                   '--build', str(self.build_root), '--schema', str(self.schema),
                   '--output', str(self.config_dir), '--db-root', str(self.remote_root()),
                   '--guest-root', str(self.guest_root),
                   '--hosts', ','.join(self.hosts), '--owners', str(self.owners),
                   '--port', str(self.port), '--transport', self.transport,
                   '--durability-tier', self.tier, '--live-transport', self.live_transport,
                   '--coordinator-cache-mib', str(self.coordinator_cache_mib),
                   '--shard-cache-mib', str(self.shard_cache_mib),
                   '--write-profile-limit', str(self.write_profile_limit)]
        if self.resident_reads is not None:
            command += ['--resident-reads', str(self.resident_reads)]
        subprocess.run(command, check=True, timeout=600)
        self.local_commands.append(command)
        self.external = json.loads(manifest_path.read_text())
        if self.external.get('owners') != self.owners:
            raise ValueError('prepared owner count differs')

    def install(self):
        for record in self.external['hosts']:
            host = record['host']
            archive = Path(record['archive'])
            if build.digest(archive) != record['archive_sha256']:
                raise ValueError('owner archive changed after preparation')
            self.remote.shell(host, f'rm -rf {shlex.quote(str(self.guest_root))} && '
                                    f'mkdir -p {shlex.quote(str(self.guest_root))}')
            self.remote.shell(host, f'tar -xzf - -C {shlex.quote(str(self.guest_root))}',
                              stdin=archive.read_bytes())
            self.remote.shell(host, f'chmod +x {shlex.quote(str(self.guest_root / "storage-node"))}')

    # ---- owner lifecycle ----------------------------------------------
    def start(self):
        for record in self.external['hosts']:
            host = record['host']
            namespaces = []
            for index, command in enumerate(record['commands']):
                pidfile = self.guest_root / f'owner-{record["partition"]}-{index}.pid'
                log = self.guest_root / f'owner-{record["partition"]}-{index}.log'
                quoted = ' '.join(shlex.quote(str(part)) for part in command)
                self.remote.shell(host,
                    f'cd {shlex.quote(str(self.guest_root))} && '
                    f'setsid sh -c {shlex.quote(f"echo $$ > {pidfile}; exec {quoted}")} '
                    f'</dev/null >>{shlex.quote(str(log))} 2>&1 &')
                deadline = time.monotonic() + 30
                while not self.remote.exists(host, pidfile):
                    if time.monotonic() > deadline:
                        raise LaunchError(f'{host}: owner did not publish a PID')
                    time.sleep(0.1)
                pid = int(self.remote.read_bytes(host, pidfile).strip())
                namespaces.append(dict(index=index, command=command, pid=pid,
                                       process_group=pid, log=str(log)))
            self.namespaces[host] = namespaces
        for record in self.external['hosts']:
            for ready in record['ready_files']:
                deadline = time.monotonic() + OWNER_READY_SECONDS
                while not self.remote.exists(record['host'], ready):
                    if time.monotonic() > deadline:
                        raise LaunchError(f'{record["host"]}: owner readiness expired')
                    time.sleep(0.2)
        self.started = True

    def sample_host(self, host):
        namespaces = self.namespaces[host]
        script = ['echo @@vmstat; cat /proc/vmstat', 'echo @@diskstats; cat /proc/diskstats']
        for namespace in namespaces:
            pid = namespace['pid']
            script.extend([
                f'echo @@{pid}', f'cat /proc/{pid}/stat 2>/dev/null',
                f'cat /proc/{pid}/status 2>/dev/null', f'cat /proc/{pid}/io 2>/dev/null'])
        code, out, err = self.remote.shell(host, '; '.join(script), check=False)
        stamp = time.time_ns()
        if code != 0:
            return dict(unix_ns=stamp, healthy=False,
                        reason=err.decode(errors='replace'))
        blocks, current = {}, None
        for line in out.decode().splitlines():
            if line.startswith('@@'):
                current = line[2:].strip()
                blocks[current] = []
            elif current is not None:
                blocks[current].append(line)
        processes, healthy = [], True
        for namespace in namespaces:
            pid = namespace['pid']
            raw = blocks.get(str(pid), [])
            stat = next((line for line in raw if ') ' in line), '')
            status = [line for line in raw if line.startswith(('VmRSS:', 'VmHWM:', 'VmSwap:'))]
            io = [line for line in raw if line.split(':')[0] in IO_COUNTERS]
            entry = parse_proc((stat + '\n').encode(),
                               ('\n'.join(status) + '\n').encode(),
                               ('\n'.join(io) + '\n').encode())
            if entry is None:
                healthy = False
                processes.append(dict(pid=pid, index=namespace['index'], exited=True))
                continue
            if namespace.setdefault('starttime_ticks', entry['starttime_ticks']) != entry['starttime_ticks']:
                healthy = False
            processes.append(dict(pid=pid, index=namespace['index'], exited=False,
                                  unavailable={}, **entry))
        vmstat = dict(line.split() for line in blocks.get('vmstat', []))
        swap = ({key: int(vmstat[key]) for key in ('pswpin', 'pswpout')}
                if vmstat else None)
        return dict(unix_ns=stamp, healthy=healthy, processes=processes, swap=swap,
                    diskstats='\n'.join(blocks.get('diskstats', [])))

    def sampling_loop(self):
        while not self.sample_stop.is_set():
            for host in self.namespaces:
                try:
                    row = self.sample_host(host)
                except BaseException as error:  # a dead transport is not a healthy sample
                    row = dict(unix_ns=time.time_ns(), healthy=False, reason=str(error))
                rows = self.samples.setdefault(host, [])
                if rows and row['unix_ns'] <= rows[-1]['unix_ns']:
                    row['unix_ns'] = rows[-1]['unix_ns'] + 1
                if len(rows) < MAX_SAMPLES:
                    rows.append(row)
            self.sample_stop.wait(SAMPLE_SECONDS)

    def start_sampling(self):
        # One synchronous sample before run.py starts anchors healthy[0] before
        # the remote-starting marker; the loop then covers the replay.
        for host in self.namespaces:
            self.samples.setdefault(host, []).append(self.sample_host(host))
        self.sample_thread = threading.Thread(target=self.sampling_loop, daemon=True)
        self.sample_thread.start()

    def final_sample(self):
        for host in self.namespaces:
            rows = self.samples.setdefault(host, [])
            row = self.sample_host(host)
            if rows and row['unix_ns'] <= rows[-1]['unix_ns']:
                row['unix_ns'] = rows[-1]['unix_ns'] + 1
            rows.append(row)

    def stop(self):
        self.sample_stop.set()
        if self.sample_thread is not None:
            self.sample_thread.join(timeout=10)
            self.sample_thread = None
        for host, namespaces in self.namespaces.items():
            for namespace in namespaces:
                if not namespace.get('stopped'):
                    self.remote.shell(host, f'kill -TERM -{namespace["process_group"]}',
                                      check=False)
            deadline = time.monotonic() + 30
            while any(self.remote.exists(host, f'/proc/{n["pid"]}') for n in namespaces):
                if time.monotonic() > deadline:
                    break
                time.sleep(0.2)
            for namespace in namespaces:
                if self.remote.exists(host, f'/proc/{namespace["pid"]}'):
                    self.remote.shell(host, f'kill -KILL -{namespace["process_group"]}',
                                      check=False)
                deadline = time.monotonic() + 10
                while self.remote.exists(host, f'/proc/{namespace["pid"]}'):
                    if time.monotonic() > deadline:
                        break
                    time.sleep(0.1)
                namespace['stopped'] = not self.remote.exists(
                    host, f'/proc/{namespace["pid"]}')
        self.started = False

    # ---- host evidence -------------------------------------------------
    def host_evidence(self, binding):
        rows = {}
        for record in self.external['hosts']:
            host = record['host']
            namespaces = self.namespaces.get(host, [])
            samples = self.samples.get(host, [])
            with_swap = [row for row in samples if row.get('swap')]
            before = with_swap[0] if with_swap else None
            after = with_swap[-1] if with_swap else None
            healthy = [row for row in samples if row['healthy']]
            rows[host] = dict(binding,
                storage_binary_sha256=self.manifest['binaries']['storage-node'],
                root=str(self.guest_root),
                complete=(len(namespaces) == len(record['commands'])
                          and all(n.get('stopped') for n in namespaces)),
                cleanup_complete=True,
                all_namespaces_healthy=bool(healthy) and all(
                    len(row['processes']) == len(namespaces) for row in healthy),
                failure=None, samples_dropped=0,
                namespaces=[dict(index=n['index'], command=n['command'], pid=n['pid'],
                                 process_group=n['process_group'],
                                 starttime_ticks=n.get('starttime_ticks', 0),
                                 reaped=bool(n.get('stopped')))
                            for n in namespaces],
                swap_before=before['swap'] if before else None,
                diskstats_before=before['diskstats'] if before else None,
                swap_after=after['swap'] if after else None,
                diskstats_after=after['diskstats'] if after else None,
                samples=[dict(unix_ns=row['unix_ns'], healthy=row['healthy'],
                              processes=row.get('processes', [])) for row in samples])
        return dict(binding, hosts=rows)

    def write_resources(self, binding):
        value = self.host_evidence(binding)
        self.resources.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.resources.with_name(self.resources.name + '.tmp')
        temporary.write_text(json.dumps(value, indent=2) + '\n')
        os.replace(temporary, self.resources)
        marker = dict(binding, unix_ns=time.time_ns())
        temporary = self.stage_dir / 'resources-ready.tmp'
        temporary.write_text(json.dumps(marker) + '\n')
        os.replace(temporary, self.stage_dir / 'resources-ready')

    # ---- orchestration -------------------------------------------------
    def identity(self):
        """Hash of every binding that can change this Sui replay's behaviour."""
        schema_files = {str(path.relative_to(self.schema)): build.digest(path)
                        for path in sorted(self.schema.rglob('*')) if path.is_file()}
        return ledger_module.config_hash(
            spec=dict(build_manifest=build.digest(self.build_root / 'build-manifest.json'),
                      fixture=build.digest(self.fixture / 'experiment/results.json'),
                      schema=schema_files),
            knobs=dict(transport=self.transport, live_transport=self.live_transport,
                       tier=self.tier, owners=self.owners,
                       fragment_mib=self.fragment_mib, run_args=self.extra_run,
                       coordinator_cache_mib=self.coordinator_cache_mib,
                       shard_cache_mib=self.shard_cache_mib,
                       write_profile_limit=self.write_profile_limit,
                       resident_reads=(build.digest(self.resident_reads)
                                       if self.resident_reads else None)),
            engine_rev=self.manifest.get('source_archive_sha256'),
            sui_rev=self.manifest.get('source_archive_sha256'),
            binary_ids=self.manifest['binaries'])

    def evidence_name(self, path):
        """Stable object name of one artifact, relative to its local root."""
        path = Path(path)
        for base, label in ((self.output, ''), (self.config_dir, 'external/')):
            try:
                return label + str(path.relative_to(base))
            except ValueError:
                continue
        return path.name

    def artifacts(self, attempt=1):
        rows = []
        for path in (self.output / 'experiment/results.json',
                     self.output / 'receipt.json',
                     self.output / 'evidence.tar.gz',
                     self.resources,
                     self.config_dir / 'config.json',
                     self.config_dir / 'external-manifest.json'):
            if not path.is_file():
                raise ValueError(f'missing replay artifact: {path}')
            row = dict(path=str(path), size=path.stat().st_size,
                       sha256=ledger_module.sha256_file(path))
            if self.prefix:
                row['key'] = campaign_manifest.evidence_key(
                    self.prefix, self.stage, self.cell_id, self.evidence_name(path),
                    attempt)
            rows.append(row)
        return rows

    def upload(self, artifacts):
        """Upload each artifact once, then verify it by size and SHA-256.

        An object that already exists must hold the same bytes; a different
        object is a hard failure, never an overwrite. The local files are the
        mirror: they are read again for the upload and must keep their hash.
        """
        receipts = []
        for artifact in artifacts:
            path = Path(artifact['path'])
            data = path.read_bytes()
            digest = ledger_module.sha256_bytes(data)
            if len(data) != artifact['size'] or digest != artifact['sha256']:
                raise ValueError(f'evidence changed before upload: {path}')
            key = artifact['key']
            try:
                receipt = self.store.put(key, data, if_generation_match=0,
                                         metadata={'kind': 'cell-evidence',
                                                   'cell': self.cell_id})
                generation = int(receipt['generation'])
                if (int(receipt.get('size', -1)) != len(data)
                        or receipt.get('metadata', {}).get('sha256') != digest):
                    raise ValueError(f'evidence upload receipt differs: {key}')
            except gcp_api.ApiError as error:
                if error.status not in (409, 412):
                    raise
                head = self.store.head(key)
                if (int(head.get('size', -1)) != len(data)
                        or head.get('metadata', {}).get('sha256') != digest):
                    raise ValueError(f'uploaded evidence differs: {key}') from None
                generation = int(head['generation'])
            if ledger_module.sha256_file(path) != digest:
                raise ValueError(f'local evidence mirror changed during upload: {path}')
            receipts.append(dict(key=key, path=str(path), size=len(data),
                                 sha256=digest, generation=generation))
        return receipts

    def persist(self, entry, artifacts):
        """Pass the cell only after its evidence and the ledger are verified."""
        target = gcs_ledger.ObjectStoreLedger(self.store)
        receipts = self.upload(artifacts)
        self.ledger.mark_passed(entry, artifacts, store=target)
        key = campaign_manifest.ledger_key(self.prefix)
        generation = gcs_ledger.save_ledger(self.store, self.ledger, key)
        payload = self.ledger.to_bytes()
        head = target.head(key)
        if (head['size'] != len(payload)
                or head['sha256'] != ledger_module.sha256_bytes(payload)
                or head['generation'] != generation):
            raise ValueError('uploaded ledger differs from the local ledger')
        local = (dict(path=str(self.ledger.path),
                      sha256=ledger_module.sha256_file(self.ledger.path))
                 if self.ledger.path is not None else None)
        receipt = dict(
            cell_id=self.cell_id, stage=self.stage, status='passed',
            attempt=entry['attempts'], bucket=self.bucket, prefix=self.prefix,
            ledger=dict(key=key, generation=generation, size=head['size'],
                        sha256=head['sha256'], local=local),
            evidence=receipts,
            mirror=dict(root=str(self.output),
                        note='the local artifact tree is the mirror; every uploaded '
                             'object is verified by size and SHA-256 on both sides'))
        path = self.output / 'upload-receipt.json'
        temporary = path.with_name(path.name + '.tmp')
        temporary.write_text(json.dumps(receipt, indent=2) + '\n')
        os.replace(temporary, path)
        return receipt

    def run(self, hosts):
        identity = self.identity()
        if self.ledger is None:
            self.admit(hosts)
            return self.execute(hosts)
        previous = self.ledger.entries.get((self.stage, self.cell_id))
        if (self.resume and previous and previous['status'] == 'passed'
                and previous['config_hash'] == identity):
            receipt = dict(complete=True, skipped=True, cell_id=self.cell_id,
                           tier=self.tier, owners=self.owners)
            print(json.dumps(receipt))
            return receipt
        self.admit(hosts)
        entry = self.ledger.begin(stage=self.stage, cell_id=self.cell_id,
                                  config_hash=identity, unit=self.cell_id)
        self.ledger.save()
        try:
            receipt = self.execute(hosts)
            if self.store is None:
                self.ledger.mark_passed(entry, self.artifacts())
                receipt['ledger'] = dict(local=str(self.ledger.path), uploaded=False)
            else:
                receipt['upload'] = self.persist(entry, self.artifacts(entry['attempts']))
                receipt['ledger'] = dict(local=str(self.ledger.path), uploaded=True,
                                         key=receipt['upload']['ledger']['key'])
            self.ledger.save()
            return receipt
        except BaseException as error:
            self.ledger.fail(entry, error)
            self.ledger.save()
            raise

    def run_command(self):
        """The exact run.py argv; every knob the external config also carries.

        The coordinator and shard cache bounds are duplicated here because
        `run.py` refuses an external deployment whose `read_cache_bytes` or
        `shard_cache_bytes` differ from these values. The operator gives each
        knob once, to this launcher.
        """
        command = ['--build', str(self.build_root), '--fixture', str(self.fixture),
                   '--output', str(self.output), '--deadline', str(self.deadline),
                   '--owners', str(self.owners), '--durability-tier', self.tier,
                   '--fragment-mib', str(self.fragment_mib),
                   '--coordinator-cache-mib', str(self.coordinator_cache_mib),
                   '--shard-cache-mib', str(self.shard_cache_mib),
                   '--write-profile-limit', str(self.write_profile_limit),
                   '--disk-sample-seconds', str(self.disk_sample_seconds),
                   '--external-config', str(self.config_dir / 'config.json'),
                   '--external-resources', str(self.resources),
                   '--external-stage-dir', str(self.stage_dir),
                   '--cell-id', self.cell_id]
        if self.resident_reads is not None:
            command += ['--resident-reads', str(self.resident_reads)]
        return command + list(self.extra_run)


    def execute(self, hosts):
        self.output.parent.mkdir(parents=True, exist_ok=True)
        self.config_dir.parent.mkdir(parents=True, exist_ok=True)
        self.hosts = hosts['owners']
        self.prepare()
        self.install()
        self.start()
        self.start_sampling()
        command = [sys.executable, str(self.source / 'run.py'), *self.run_command()]
        self.local_commands.append(command)
        child = subprocess.Popen(command)
        marker = self.stage_dir / 'resources-required.json'
        resources_written = False
        try:
            deadline = time.monotonic() + 3 * self.deadline + 1800
            while child.poll() is None:
                if marker.exists():
                    required = json.loads(marker.read_text())
                    binding = {key: required[key] for key in
                               ('config_sha256', 'build_manifest_sha256', 'checkpoint_digest')}
                    self.final_sample()
                    self.stop()
                    self.write_resources(binding)
                    resources_written = True
                    break
                if time.monotonic() > deadline:
                    raise TimeoutError('run.py did not request resource evidence')
                time.sleep(0.2)
            code = child.wait()
        finally:
            if self.started:
                self.stop()
            if child.poll() is None:
                code = child.wait()
        if code != 0:
            raise LaunchError(f'run.py failed with exit status {code}')
        if not resources_written:
            raise LaunchError('run.py exited without requesting resource evidence')
        receipt = dict(complete=True, exit_code=code, tier=self.tier,
                       owners=self.owners,
                       build_manifest_sha256=build.digest(
                           self.build_root / 'build-manifest.json'))
        (self.output / 'launcher-receipt.json').write_text(
            json.dumps(receipt, indent=2) + '\n')
        return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', type=Path, required=True)
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--schema', type=Path, required=True,
                        help='native-schema from a source-identical local run')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--guest-root', type=Path, required=True,
                        help='absolute owner storage root, identical on both owners')
    parser.add_argument('--coordinator', required=True)
    parser.add_argument('--owners', required=True, help='owner1,owner2')
    parser.add_argument('--zone', required=True)
    parser.add_argument('--external-config-dir', type=Path)
    parser.add_argument('--stage-dir', type=Path)
    parser.add_argument('--resources', type=Path)
    parser.add_argument('--ledger', type=Path,
                        help='campaign ledger (default: beside the run output)')
    parser.add_argument('--resume', action='store_true',
                        help='skip this cell if the ledger has it passed with the same binding')
    parser.add_argument('--cell-id', default='sui-replay')
    parser.add_argument('--stage', default='stage1')
    parser.add_argument('--transport', choices=('tcp', 'tcp-trusted',
                        *deployment.HARDWARE_TRANSPORTS), default='efa')
    parser.add_argument('--live-transport', choices=deployment.LIVE_TRANSPORTS,
                        default='rdma')
    parser.add_argument('--durability-tier', choices=('native', 'synchronous'),
                        default='native')
    parser.add_argument('--fragment-mib', type=int, default=8)
    parser.add_argument('--deadline', type=int, default=900)
    parser.add_argument('--disk-sample-seconds', type=int, default=10)
    parser.add_argument('--port', type=int, default=24000)
    parser.add_argument('--coordinator-cache-mib', type=int, default=8)
    parser.add_argument('--shard-cache-mib', type=int, default=0)
    parser.add_argument('--write-profile-limit', type=int, default=0,
                        help='bounded append profiling per owning connection, 0 disables')
    parser.add_argument('--resident-reads', type=Path,
                        help='resident-read policy forwarded to the owners and the runner')
    parser.add_argument('--tunnel-through-iap', action='store_true')
    parser.add_argument('--bucket-prefix',
                        help='campaign object prefix, gs://mysten-research-bench-artifacts/'
                             '<campaign-id>; a cell passes only after its verified upload')
    parser.add_argument('--impersonation-account', default=campaign_manifest.ACCOUNT,
                        help='service account that writes the campaign prefix')
    parser.add_argument('--allow-local-ledger', action='store_true',
                        help='local rehearsal only: pass a cell without an uploaded bundle')
    parser.add_argument('run_args', nargs=argparse.REMAINDER,
                        help='extra run.py arguments after --')
    args = parser.parse_args()
    hosts = plan(args.coordinator, args.owners.split(','))
    output = args.output.resolve()
    if not args.bucket_prefix and not args.allow_local_ledger:
        raise SystemExit('a passed cell needs a verified evidence upload; give '
                         '--bucket-prefix, or --allow-local-ledger for a local rehearsal')
    store = bucket = prefix = None
    if args.bucket_prefix:
        bucket, prefix = campaign_manifest.parse_bucket_prefix(args.bucket_prefix)
        api = gcp_api.GoogleApi(campaign_manifest.PROJECT)
        if args.impersonation_account:
            api = api.impersonated(args.impersonation_account)
        store = api.objects(bucket)
    ledger_path = args.ledger or output.with_name(output.name + '-ledger.json')
    if Path(ledger_path).exists() and not args.resume:
        raise SystemExit(f'ledger exists; use --resume or a new --ledger: {ledger_path}')
    resume_ledger = ledger_module.Ledger(ledger_path)
    if args.resume:
        resume_ledger.load()
    launcher = SuiLauncher(
        remote=Remote(args.zone, iap=args.tunnel_through_iap),
        build_root=args.build, fixture=args.fixture, output=output,
        guest_root=args.guest_root,
        stage_dir=args.stage_dir or output.with_name(output.name + '-stages'),
        config_dir=args.external_config_dir or output.with_name(output.name + '-external-config'),
        resources=args.resources or output.with_name(output.name + '-resources.json'),
        tier=args.durability_tier, owners=2, fragment_mib=args.fragment_mib,
        deadline=args.deadline, extra_run=args.run_args,
        transport=args.transport, live_transport=args.live_transport,
        coordinator_cache_mib=args.coordinator_cache_mib,
        shard_cache_mib=args.shard_cache_mib, schema=args.schema,
        disk_sample_seconds=args.disk_sample_seconds, port=args.port,
        ledger=resume_ledger, cell_id=args.cell_id, stage=args.stage,
        resume=args.resume, store=store, bucket=bucket, prefix=prefix,
        write_profile_limit=args.write_profile_limit,
        resident_reads=args.resident_reads)
    receipt = launcher.run(hosts)
    print(json.dumps(receipt))


if __name__ == '__main__':
    main()
