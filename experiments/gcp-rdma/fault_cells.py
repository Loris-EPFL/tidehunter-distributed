#!/usr/bin/env python3
"""Operator driver for the mechanism and fault cells of the cluster campaign.

One command per cell. The driver owns the three-host lifecycle around the
frozen `storage-node` (`kv-research`) binary:

  * `prepare_external.py` writes the external NVMe-oF deployment (mTLS TCP
    control, RDMA live path) for this cell;
  * the owner archive is installed on each owner host and every owner process
    group is started with `TIDEHUNTER_OWNER_EVENTS=1`, so the retained evidence
    carries the owner's own publication, sync, lease and retirement timeline;
  * the coordinator (this cell) runs locally over that deployment;
  * every injection waits on an observed event, never on a wall-clock premise:
    an acknowledged-batch count in `samples.csv.tmp`, an index-file growth step,
    or the disappearance of one owner WAL file;
  * after a kill the driver restarts the killed process on the same roots and
    runs the read-only full-dataset verification run, which is the plan's audit.

Cell definitions, specs and check lists live in
`experiments/sui-checkpoint/stage2_mechanism.py`; this module performs the
lifecycle and records timestamps. Remote commands go through
`host_exec.HostExecution`, the same seam the Sui stages use; the campaign
executor is `mailbox_exec.MailboxExec`. The coordinator process is a local child
that must reach the owners, so this driver runs on the coordinator host as one
bounded request (`--on-coordinator`); `--gcloud-ssh` selects the SSH rehearsal
instead.

Every cell here is a synthetic mechanism cell over the campaign topology, not a
Sui-adapter result. `--dry-run` prints the plan without touching a host.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import subprocess
import sys
import threading
import time
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'sui-checkpoint'))
import build
import campaign_ledger as ledger_module
import campaign_manifest
import campaign_upload
import disk_collector
import host_exec
import mailbox_exec
import manual_sui
import stage2_mechanism as mechanism
import stage3_specs

BINARY = 'storage-node'
OWNER_READY_SECONDS = 180
GATE_TIMEOUT_SECONDS = 600
# The coordinator declares its own close bound as `shutdown_grace_seconds` in
# the RunConfig this driver writes, so its stop allowance is the same constant.
COORDINATOR_STOP_GRACE_SECONDS = stage3_specs.SHUTDOWN_GRACE_SECONDS
# An owner is not the measured process and its close path tears down every live
# NVMe-oF target, then writes and syncs a shutdown proof per database; the old
# proven controller allowed 90 s for exactly that (`cleanup_timeout`).
OWNER_STOP_GRACE_SECONDS = 90
# A failed attempt uploads what it still holds before the fleet is deleted. The
# local coordinator database, TLS material and any bulk storage tree never
# travel, and one oversized file must not consume the whole failure budget.
FAILURE_ARTIFACT_LIMIT = 128 << 20
FAILURE_ARTIFACT_SKIP = ('host-*.tar.gz',)
FAILURE_SKIP_PARTS = ('coordinator', 'tls', 'storage')
GATE_WRITES = 200
FENCE_WRITES = 20
WAL = re.compile(r'^wal_([0-9a-f]{16})$')
DATABASE = re.compile(r'^db-[0-9a-f]{32}$')


class CellError(RuntimeError):
    """A precondition, gate or verification failed; evidence is still retained."""


def digest(path):
    return build.digest(Path(path))


def now_ns():
    return time.time_ns()


def quote(value):
    return shlex.quote(str(value))


def rows(path):
    """Completed rows of `samples.csv`/`samples.csv.tmp`, header-keyed."""
    return mechanism.read_rows(path)


def acknowledged(result, *, phase='measurement'):
    return mechanism.acknowledged(result, phase=phase)


def failures(result, *, phase='measurement'):
    return mechanism.failures(result, phase=phase)


def preflight(output, *, phase='measurement'):
    """Raise before a verdict when a column or key it parses is absent."""
    problems = mechanism.preflight(output, phase=phase)
    if problems:
        raise CellError('; '.join(problems))


class Cluster:
    """The three-host lifecycle of one cell: prepare, owners, hosts."""

    def __init__(self, *, remote, build_root, output, guest_root, config_dir,
                 owners, tier, transport='tcp', live_transport='rdma',
                 schema=None, port=manual_sui.OWNER_CONTROL_PORT,
                 coordinator_cache_mib=0,
                 shard_cache_mib=0):
        self.remote, self.owners = remote, list(owners)
        self.build_root = Path(build_root).resolve()
        self.output = Path(output).resolve()
        self.guest_root = Path(guest_root)
        self.config_dir = Path(config_dir).resolve()
        self.tier = tier
        self.transport = transport
        self.live_transport = live_transport
        self.schema = Path(schema).resolve() if schema else None
        self.port = port
        self.coordinator_cache_mib = coordinator_cache_mib
        self.shard_cache_mib = shard_cache_mib
        self.binary = self.build_root / 'binaries' / BINARY
        self.manifest = json.loads((self.build_root / 'build-manifest.json').read_text())
        self.external = None
        self.namespaces = {}
        self.stops = []

    # ---- admission -----------------------------------------------------
    def admit(self):
        expected = self.manifest['binaries'].get(BINARY)
        if expected is None or digest(self.binary) != expected:
            raise ValueError('frozen storage-node binary differs from the build manifest')
        if self.manifest.get('protocol') != 9:
            raise ValueError('build manifest protocol is not 9')
        if len(self.owners) != mechanism.OWNERS or len(set(self.owners)) != len(self.owners):
            raise ValueError(f'exactly {mechanism.OWNERS} distinct owner hosts are required')
        if not self.guest_root.is_absolute():
            raise ValueError('the guest root must be absolute and identical on every host')

    def local(self, name):
        return self.output / name

    def root(self, name):
        return self.local(name) / 'coordinator'

    def evidence(self, name):
        return self.local(name) / 'evidence'

    def database(self):
        config = json.loads((self.config_dir / 'config.json').read_text())
        return sorted(config['databases'])[0]

    def context(self, namespace, receipts):
        config = json.loads((self.config_dir / 'config.json').read_text())
        remote = config['databases'][namespace]
        # `external_owner_durability_receipts` is a list of path strings; the
        # copied receipts' partition and parsed body stay in the evidence rows.
        return dict(transport=remote['tls']['transport'], remote=remote,
                    receipts=[row['path'] if isinstance(row, dict) else str(row)
                              for row in receipts])

    # ---- preparation ---------------------------------------------------
    def prepare(self):
        manifest_path = self.config_dir / 'external-manifest.json'
        if manifest_path.exists():
            self.external = json.loads(manifest_path.read_text())
            if self.external.get('owners') != len(self.owners):
                raise ValueError('prepared owner count differs from this cell')
            if self.external.get('durability_tier') != self.tier:
                raise ValueError('prepared deployment tier differs from this cell')
            return
        if self.schema is None or not self.schema.is_dir():
            raise ValueError('a native schema from a source-identical run is required')
        if self.config_dir.exists():
            raise ValueError('external config directory must be fresh')
        self.output.mkdir(parents=True, exist_ok=True)
        control, live = manual_sui.fleet_hosts(
            getattr(self.remote, 'fleet', None), self.owners,
            transport=self.transport, live_transport=self.live_transport)
        # Every later host key is the prepared manifest's address, so the
        # deployment and the executor name the same host.
        self.owners = list(control)
        command = [sys.executable,
                   str(self.build_root / 'sources/tidehunter-sharded-wal/experiments/'
                       'sui-checkpoint/prepare_external.py'),
                   '--build', str(self.build_root), '--schema', str(self.schema),
                   '--output', str(self.config_dir),
                   '--db-root', str(self.root('measure')),
                   '--guest-root', str(self.guest_root),
                   '--hosts', ','.join(control), '--live-hosts', ','.join(live),
                   '--owners', str(len(self.owners)),
                   '--port', str(self.port), '--transport', self.transport,
                   '--durability-tier', self.tier, '--live-transport', self.live_transport,
                   '--coordinator-cache-mib', str(self.coordinator_cache_mib),
                   '--shard-cache-mib', str(self.shard_cache_mib)]
        subprocess.run(command, check=True, timeout=600)
        self.external = json.loads(manifest_path.read_text())

    def install(self):
        for record in self.external['hosts']:
            host, archive = record['host'], Path(record['archive'])
            if digest(archive) != record['archive_sha256']:
                raise ValueError('owner archive changed after preparation')
            self._stop_previous_owners(host)
            self.remote.shell(host, f'rm -rf {quote(self.guest_root)} && '
                                    f'mkdir -p {quote(self.guest_root)}')
            self.remote.shell(host, f'tar -xzf - -C {quote(self.guest_root)}',
                              stdin=archive.read_bytes())
            self.remote.shell(host, f'chmod +x {quote(self.guest_root / BINARY)}')

    def live_previous_owners(self, host):
        """`(pid, pidfile)` of owners of an earlier cell that are still running.

        The pidfile is the only durable record after this driver lost its
        workspace, so it is the first check. A recycled pid is not an owner:
        its command line must name the guest binary or this guest root.
        """
        script = ('for f in "$1"/owner-*.pid; do [ -f "$f" ] || continue; '
                  'read -r pid < "$f"; [ -n "$pid" ] && [ -d "/proc/$pid" ] && '
                  'echo "$pid $f"; done')
        code, out, err = self.remote.shell(
            host, f'sh -c {quote(script)} -- {quote(self.guest_root)}', check=False)
        if code != 0:
            raise CellError(f'{host}: cannot check for a previous owner: '
                            f'{err.decode(errors="replace")[-300:]}')
        found = []
        for line in out.decode().splitlines():
            parts = line.split()
            if len(parts) != 2 or not parts[0].isdigit():
                continue
            pid = int(parts[0])
            command = self.remote.read_bytes(host, f'/proc/{pid}/cmdline')
            if (str(self.guest_root).encode() not in command
                    and b'--experimental-storage-node' not in command):
                continue
            found.append((pid, parts[1]))
        return found

    def _stop_previous_owners(self, host):
        """A guest root is deleted only when no owner of an earlier cell is alive.

        A live owner is stopped through the normal group path (SIGTERM, then the
        owner grace). A forced kill is recorded as a failed stop, and a process
        that survives even that fails the cell instead of losing its storage.
        """
        previous = self.live_previous_owners(host)
        if not previous:
            return []
        records = []
        for pid, pidfile in previous:
            started = time.monotonic()
            self.remote.shell(host, f'kill -TERM -{pid}', check=False)
            deadline = time.monotonic() + OWNER_STOP_GRACE_SECONDS
            while self.remote.exists(host, f'/proc/{pid}'):
                if time.monotonic() > deadline:
                    break
                time.sleep(0.2)
            forced = self.remote.exists(host, f'/proc/{pid}')
            if forced:
                self.remote.shell(host, f'kill -KILL -{pid}', check=False)
                deadline = time.monotonic() + 10
                while self.remote.exists(host, f'/proc/{pid}'):
                    if time.monotonic() > deadline:
                        break
                    time.sleep(0.1)
            alive = self.remote.exists(host, f'/proc/{pid}')
            record = dict(host=host, partition=None, pid=pid, pidfile=pidfile,
                          signal='SIGTERM', grace_seconds=OWNER_STOP_GRACE_SECONDS,
                          duration_seconds=round(time.monotonic() - started, 3),
                          forced=forced, still_alive=alive,
                          source='previous cell', exit_code=None)
            records.append(record)
            if alive:
                self.stops.extend(records)
                raise CellError(f'{host}: previous owner {pid} is still alive; '
                                'refusing to remove its guest root')
        self.stops.extend(records)
        return records

    # ---- owner lifecycle ----------------------------------------------
    def nodes(self):
        return json.loads((self.config_dir / 'nodes.json').read_text())

    def bind(self, namespace):
        """Scope this cell to one coordinator database of the deployment."""
        self.namespace = namespace
        self.owner_nodes = {}
        for node in self.nodes():
            if node['namespace'] != namespace:
                continue
            base = Path(node['launch']).relative_to(self.config_dir)
            guest = self.guest_root / base
            launch = yaml.safe_load((self.config_dir / base).read_text())
            # `launch` is a controller path (the archive member); `ready` and
            # `stop` are guest paths because the owner writes them (deployment.prepare).
            ready = Path(node['ready'])
            entry = dict(node, guest_launch=str(guest),
                         guest_ready=str(ready),
                         guest_storage=str(guest.parent / 'storage'),
                         launch_nonce=launch.get('launch_nonce'),
                         listen=launch.get('listen'),
                         launch_tier=(launch.get('durability') or {}).get('tier'),
                         command=[str(self.guest_root / BINARY),
                                  '--experimental-storage-node', str(guest)])
            self.owner_nodes.setdefault(node['host'], []).append(entry)
        for host, rows in self.owner_nodes.items():
            rows.sort(key=lambda row: row['partition'])
            if len(rows) != 1:
                raise CellError(f'{host}: expected one owner node for {namespace}')
        if set(self.owner_nodes) != set(self.owners):
            raise CellError('the deployment does not cover every owner host')

    def _start_group(self, host, node, *, truncate):
        pidfile = self.guest_root / f'owner-{node["partition"]}.pid'
        log = self.guest_root / f'owner-{node["partition"]}.log'
        command = ' '.join(quote(part) for part in
                           ['env', 'TIDEHUNTER_OWNER_EVENTS=1', *node['command']])
        redirect = '>' if truncate else '>>'
        self.remote.shell(host,
            f'cd {quote(self.guest_root)} && rm -f {quote(pidfile)} && '
            f'setsid sh -c {quote(f"echo $$ > {pidfile}; exec {command}")} '
            f'</dev/null {redirect}{quote(log)} 2>&1 &')
        deadline = time.monotonic() + 30
        while not self.remote.exists(host, pidfile):
            if time.monotonic() > deadline:
                raise CellError(f'{host}: owner did not publish a PID')
            time.sleep(0.1)
        return int(self.remote.read_bytes(host, pidfile).strip())

    def start(self):
        if getattr(self, 'namespace', None) is None:
            raise CellError('bind a namespace before starting owners')
        for host, rows in self.owner_nodes.items():
            started = []
            for node in rows:
                pid = self._start_group(host, node, truncate=True)
                started.append(dict(partition=node['partition'], pid=pid,
                                    process_group=pid, stopped=False,
                                    stop=node.get('stop')))
            self.namespaces[host] = started
            for node in rows:
                self._wait_ready(host, node['guest_ready'])

    def _wait_ready(self, host, ready):
        deadline = time.monotonic() + OWNER_READY_SECONDS
        while not self.remote.exists(host, ready):
            if time.monotonic() > deadline:
                raise CellError(f'{host}: owner readiness expired')
            time.sleep(0.2)

    def restart_owner(self, host):
        """Start a new incarnation of the owner group on one host."""
        rows = self.owner_nodes[host]
        previous = [row['pid'] for row in self.namespaces.get(host, [])]
        for namespace in self.namespaces.get(host, []):
            if self.remote.exists(host, f'/proc/{namespace["pid"]}'):
                raise CellError(f'{host}: owner {namespace["pid"]} is still alive before restart')
        for node in rows:
            self.remote.shell(host, f'rm -f {quote(node["guest_ready"])}')
        started = []
        for node in rows:
            pid = self._start_group(host, node, truncate=False)
            if pid in previous:
                raise CellError(f'{host}: restarted owner reused PID {pid}')
            started.append(dict(partition=node['partition'], pid=pid,
                                process_group=pid, stopped=False,
                                stop=node.get('stop')))
        self.namespaces[host] = started
        for node in rows:
            self._wait_ready(host, node['guest_ready'])
        return dict(host=host, previous=previous, current=[row['pid'] for row in started],
                    restarted_unix_ns=now_ns())

    def running_pid(self, host, partition):
        for row in self.namespaces.get(host, []):
            if row['partition'] == partition:
                return row['pid']
        raise CellError(f'{host}: partition {partition} has no running owner')

    def confirm_receipt(self, host, node, receipt):
        """Refuse a receipt that is not this partition, tier and incarnation.

        This is `cloud_controller.confirm_owner_durability` plus the endpoint:
        the receipt must name the launch nonce written in this cell's launch
        file, the process this cell started, the listen address that process
        bound and the cell's durability tier. A receipt from an earlier
        incarnation or an earlier cell therefore cannot be admitted.
        """
        pid = self.running_pid(host, node['partition'])
        nonce = node.get('launch_nonce')
        if not isinstance(nonce, str) or not nonce:
            raise CellError(f'{host}: launch file has no launch nonce')
        problems = []
        if receipt.get('partition') != node['partition']:
            problems.append(f'partition {receipt.get("partition")} != {node["partition"]}')
        durability = receipt.get('durability')
        if not isinstance(durability, dict) or durability.get('tier') != self.tier:
            problems.append(f'tier {(durability or {}).get("tier")} != {self.tier}')
        if receipt.get('pid') != pid:
            problems.append(f'pid {receipt.get("pid")} != running {pid}')
        if receipt.get('launch_nonce') != nonce:
            problems.append(f'launch nonce {receipt.get("launch_nonce")} != {nonce}')
        if receipt.get('address') != node.get('listen'):
            problems.append(f'address {receipt.get("address")} != {node.get("listen")}')
        if problems:
            raise CellError(f'{host}: durability receipt is stale or foreign: '
                            + '; '.join(problems))
        return receipt

    def receipts(self, namespace):
        """Copy one owner durability receipt per partition to the controller."""
        nodes = [node for node in self.nodes() if node['namespace'] == namespace]
        nodes.sort(key=lambda node: node['partition'])
        directory = self.config_dir / 'receipts'
        directory.mkdir(parents=True, exist_ok=True)
        paths = []
        for node in nodes:
            host = node['host']
            bound = self.owner_nodes[host][0]
            if bound['partition'] != node['partition']:
                raise CellError(f'{host}: bound owner differs from the deployment')
            ready = Path(node['ready'])
            guest = ready.with_name(ready.name + '.durability.json')
            path = directory / f'node-{node["partition"]}.json'
            path.write_bytes(self.remote.read_bytes(host, guest))
            receipt = json.loads(path.read_text())
            self.confirm_receipt(host, bound, receipt)
            paths.append(dict(path=str(path), partition=node['partition'],
                              launch_nonce=receipt['launch_nonce'],
                              pid=receipt['pid'], receipt=receipt))
        if len(paths) != len(self.owners):
            raise CellError('durability receipts do not cover every owner')
        return paths

    def stop(self):
        """Close every owner group and record the outcome.

        The engine's own close path is its typed stop marker: the owner leaves
        `serve`, tears down every live target and syncs a shutdown proof per
        database. The declared owner grace covers that path. A group still
        alive after it gets SIGTERM, then SIGKILL, and a forced kill is a failed
        stop, never a clean one. A detached owner is reaped by init, so its exit
        code is not readable; `exit_code` is None and the record names the
        signal, the wait and the duration instead.
        """
        records = []
        for host, namespaces in self.namespaces.items():
            for namespace in namespaces:
                alive = self.remote.exists(host, f'/proc/{namespace["pid"]}')
                started = time.monotonic()
                record = dict(host=host, partition=namespace['partition'],
                              pid=namespace['pid'], signal=None,
                              grace_seconds=OWNER_STOP_GRACE_SECONDS,
                              duration_seconds=0.0, forced=False,
                              still_alive=False, source='cell', exit_code=None)
                if alive:
                    stop_file = namespace.get('stop')
                    if stop_file:
                        record['signal'] = 'stop-file'
                        self.remote.shell(host, f'touch {quote(stop_file)}',
                                          check=False)
                        deadline = time.monotonic() + OWNER_STOP_GRACE_SECONDS
                        while self.remote.exists(host, f'/proc/{namespace["pid"]}'):
                            if time.monotonic() > deadline:
                                break
                            time.sleep(0.2)
                    if self.remote.exists(host, f'/proc/{namespace["pid"]}'):
                        record['signal'] = ((record['signal'] + '+SIGTERM')
                                            if record['signal'] else 'SIGTERM')
                        self.remote.shell(
                            host, f'kill -TERM -{namespace["process_group"]}',
                            check=False)
                        deadline = time.monotonic() + 10
                        while self.remote.exists(host, f'/proc/{namespace["pid"]}'):
                            if time.monotonic() > deadline:
                                break
                            time.sleep(0.1)
                    record['forced'] = self.remote.exists(
                        host, f'/proc/{namespace["pid"]}')
                    if record['forced']:
                        self.remote.shell(
                            host, f'kill -KILL -{namespace["process_group"]}',
                            check=False)
                        deadline = time.monotonic() + 10
                        while self.remote.exists(host, f'/proc/{namespace["pid"]}'):
                            if time.monotonic() > deadline:
                                break
                            time.sleep(0.1)
                record['still_alive'] = self.remote.exists(
                    host, f'/proc/{namespace["pid"]}')
                record['duration_seconds'] = round(time.monotonic() - started, 3)
                namespace['stopped'] = not record['still_alive']
                records.append(record)
        self.namespaces = {}
        self.stops.extend(records)
        return records

    # ---- host evidence -------------------------------------------------
    def owner_root(self, host):
        """The guest storage root of this namespace's owner on one host."""
        return self.owner_nodes[host][0]['guest_storage']

    def owner_databases(self, host):
        code, out, _ = self.remote.shell(
            host, f'ls -1 {quote(self.owner_root(host))} 2>/dev/null | cat', check=False)
        if code != 0:
            return []
        return sorted(name for name in out.decode().splitlines()
                      if DATABASE.fullmatch(name))

    def disk(self, host):
        """`disk_collector` over stdin, one convention, no guest copy."""
        root = self.owner_root(host)
        names = self.owner_databases(host)
        if not names:
            return dict(unix_ns=now_ns(), databases={}, unavailable='no owner database yet')
        arguments = ['--root', str(root)]
        for name in names:
            arguments += ['--namespace', name]
        code, out, err = self.remote.run(
            host, 'python3 - ' + ' '.join(quote(part) for part in arguments),
            stdin=Path(disk_collector.__file__).read_text().encode(), check=False)
        if code != 0:
            raise CellError(f'{host}: disk collector failed: '
                            f'{err.decode(errors="replace")[-500:]}')
        value = json.loads(out)
        value['unix_ns'] = now_ns()
        return value

    def disks(self):
        return {host: self.disk(host) for host in self.owners}

    def wal(self, host):
        """Owner WAL files with size and mtime, and each durable root's mtime."""
        script = ('for d in "$1"/db-*; do [ -d "$d" ] || continue; '
                  'find "$d" -maxdepth 1 -name "wal_*" -printf "%p %s %T@\\n" 2>/dev/null; '
                  'stat -c "%n %s %Y" "$d/partition-wal-root-v1" 2>/dev/null; done')
        code, out, err = self.remote.shell(
            host, f'sh -c {quote(script)} -- {quote(self.owner_root(host))}', check=False)
        if code != 0:
            raise CellError(f'{host}: cannot list the owner WAL: '
                            f'{err.decode(errors="replace")[-300:]}')
        files, roots = {}, {}
        for line in out.decode().splitlines():
            parts = line.split()
            if len(parts) != 3:
                continue
            path, size, stamp = parts
            if path.endswith('/partition-wal-root-v1'):
                roots[path] = dict(size=int(size), mtime=float(stamp))
            else:
                files[path] = dict(size=int(size), mtime=float(stamp))
        return dict(host=host, files=files, roots=roots, unix_ns=now_ns())

    def seal_markers(self, host):
        """Owner databases that published a sealed WAL or live-target marker."""
        names = self.owner_databases(host)
        if not names:
            return dict(databases=0, sealed=0)
        script = ('for d in "$1"/db-*; do [ -d "$d" ] || continue; '
                  'n=$(ls -1 "$d/live-target/sealed" 2>/dev/null | wc -l); '
                  's=$(ls -1 "$d"/partition-seal-v5 2>/dev/null | wc -l); echo "$d $n $s"; done')
        code, out, err = self.remote.shell(
            host, f'sh -c {quote(script)} -- {quote(self.owner_root(host))}', check=False)
        if code != 0:
            raise CellError(f'{host}: cannot read the sealed markers')
        databases = sealed = 0
        for line in out.decode().splitlines():
            parts = line.split()
            if len(parts) == 3:
                databases += 1
                if int(parts[1]) > 0 or int(parts[2]) > 0:
                    sealed += 1
        return dict(host=host, databases=databases, sealed=sealed, unix_ns=now_ns())

    def owner_events(self, host):
        """The owner event log, written when the owner shuts down or is asked."""
        launch = Path(self.owner_nodes[host][0]['guest_launch'])
        events = launch.with_name(launch.stem + '.owner-events.json')
        if not self.remote.exists(host, events):
            return None
        return json.loads(self.remote.read_bytes(host, events))

    def retire_requests(self, events):
        if not events:
            return None
        return sum(1 for event in events['events']
                   if event['kind'] == 'control_received' and event['b'] in (2, 3, 20))

    def rolls(self, host):
        """Highest owner WAL file id: `wal_0000000000000002` proves two rolls."""
        files = self.wal(host)['files']
        ids = []
        for path in files:
            match = WAL.fullmatch(Path(path).name)
            if match:
                ids.append(int(match.group(1), 16))
        return dict(host=host, files=len(files), max_file_id=max(ids) if ids else None)


class Coordinator:
    """One local coordinator process over the prepared deployment."""

    def __init__(self, *, binary, spec, directory, name='run'):
        self.directory = Path(directory)
        self.output = self.directory / name
        self.output.mkdir(parents=True, exist_ok=False)
        self.log = self.directory / f'{name}.log'
        self.stream = self.log.open('xb')
        # The measurement window is binary-gated: the run creates its ready
        # marker and waits for the release this process writes. `prepare`
        # removes a stale pair and creates the marker directory.
        self.gates = mechanism.LocalGates(spec)
        self.gates.prepare()
        self.process = subprocess.Popen([str(binary), '--config', str(spec),
                                         '--output', str(self.output)],
                                        stdout=self.stream, stderr=subprocess.STDOUT,
                                        start_new_session=True)
        self.started_unix_ns = now_ns()
        self.killed_unix_ns = None
        self.stop_record = None
        self._gate_stop = threading.Event()
        self._gate_thread = threading.Thread(target=self.gates.run,
                                             args=(self._gate_stop,), daemon=True)
        self._gate_thread.start()

    def close_gates(self):
        self._gate_stop.set()
        self._gate_thread.join(timeout=5)

    def alive(self):
        return self.process.poll() is None

    def rows(self):
        return rows(self.output / 'samples.csv.tmp') or rows(self.output / 'samples.csv')

    def wait(self, timeout):
        deadline = time.monotonic() + timeout
        while self.process.poll() is None:
            if time.monotonic() > deadline:
                return None
            time.sleep(0.1)
        return self.process.returncode

    def kill(self):
        self.killed_unix_ns = now_ns()
        try:
            os.killpg(self.process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        return self.process.wait(timeout=30)

    def signal(self, value):
        try:
            os.killpg(self.process.pid, value)
        except ProcessLookupError:
            pass

    def stop(self):
        """Stop the coordinator with the engine's own declared grace.

        The record keeps the real exit code and the measured duration. A run
        that needs SIGKILL after the grace is a failed stop, never a clean one.
        """
        started = time.monotonic()
        record = dict(process='coordinator', signal=None,
                      grace_seconds=COORDINATOR_STOP_GRACE_SECONDS,
                      duration_seconds=0.0, forced=False, still_alive=False,
                      exit_code=self.process.returncode)
        if self.process.poll() is None:
            record['signal'] = 'SIGTERM'
            self.signal(signal.SIGTERM)
            try:
                record['exit_code'] = self.process.wait(
                    timeout=COORDINATOR_STOP_GRACE_SECONDS)
            except subprocess.TimeoutExpired:
                record['forced'] = True
                record['exit_code'] = self.kill()
        record['still_alive'] = self.process.poll() is None
        record['duration_seconds'] = round(time.monotonic() - started, 3)
        self.close_gates()
        self.stream.close()
        self.stop_record = record
        return record


class CellDriver:
    def __init__(self, cluster, cell, *, log=print):
        self.cluster, self.cell, self.log = cluster, cell, log
        self.parts = []
        self.fence = None
        self.namespace = None
        self.context = None

    # ---- spec and launch ----------------------------------------------
    def spec(self, run, name, *, path=None, receipts=None):
        path = path or self.cluster.root(name)
        path.parent.mkdir(parents=True, exist_ok=True)
        evidence = self.cluster.evidence(name)
        evidence.mkdir(parents=True, exist_ok=True)
        context = dict(self.context, receipts=receipts or self.context['receipts'])
        value = mechanism.spec(self.cell, run, context, path=str(path),
                               evidence=str(evidence))
        target = self.cluster.local(name) / 'spec.json'
        target.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
        return target

    def launch(self, run, name, *, path=None, receipts=None, run_name='run'):
        spec = self.spec(run, name, path=path, receipts=receipts)
        self.cluster.local(name).mkdir(parents=True, exist_ok=True)
        return Coordinator(binary=self.cluster.binary, spec=spec,
                           directory=self.cluster.local(name), name=run_name)

    def bind_context(self, *, receipts=None):
        """Establish the namespace, owner receipts and transport of this cell."""
        namespace = self.cluster.database()
        recorded = self.cluster.receipts(namespace)
        chosen = receipts or recorded
        self.namespace = namespace
        self.context = self.cluster.context(namespace, chosen)
        self.receipt_paths = [row['path'] for row in recorded]
        # `spec()` overrides its context receipts with this value, and the
        # backend admits `external_owner_durability_receipts` as paths, so the
        # raw rows would serialize maps into the spec and fail the open.
        return self.context['receipts']

    def open(self, run, name, *, receipts=None):
        chosen = self.bind_context(receipts=receipts)
        return self.launch(run, name, receipts=chosen)

    def wait_for(self, child, predicate, *, timeout, label, poll=0.2):
        started = time.monotonic()
        observations = []
        while True:
            observation = predicate()
            observations.append(observation)
            if observation.get('reached'):
                return dict(reached=True, seconds=time.monotonic() - started,
                            label=label, observations=observations[-20:])
            if not child.alive():
                return dict(reached=False, reason='coordinator exited before ' + label,
                            observations=observations[-20:])
            if time.monotonic() - started >= timeout:
                return dict(reached=False,
                            reason=f'{label} not observed within {timeout}s',
                            observations=observations[-20:])
            time.sleep(poll)

    def gate_writes(self, child, count, *, timeout=GATE_TIMEOUT_SECONDS):
        def predicate():
            done = acknowledged(child.rows())
            return dict(reached=done >= count, acknowledged=done)
        return self.wait_for(child, predicate, timeout=timeout,
                             label=f'{count} acknowledged writes')

    def gate_index_growth(self, child, *, timeout=GATE_TIMEOUT_SECONDS):
        state = dict(previous=self._index_files(child))

        def predicate():
            current = self._index_files(child)
            grew = {path: [state['previous'].get(path, 0), size]
                    for path, size in current.items()
                    if size > state['previous'].get(path, 0)}
            state['previous'] = current
            return dict(reached=bool(grew) and acknowledged(child.rows()) > 0,
                        grew=grew, files=len(current))
        return self.wait_for(child, predicate, timeout=timeout,
                             label='coordinator index file growth')

    def _index_files(self, child):
        values = {}
        for path in sorted(child.output.rglob('index_*')):
            if path.is_file():
                values[str(path)] = path.stat().st_size
        return values

    def gate_retirement(self, host, cut, *, timeout=GATE_TIMEOUT_SECONDS):
        before = self.cluster.wal(host)
        baseline = dict(before['files'])
        roots = dict(before['roots'])
        started = time.monotonic()
        observations = []
        while True:
            current = self.cluster.wal(host)
            gone = [path for path in baseline if path not in current['files']]
            moved = [path for path, row in current['roots'].items()
                     if path in roots and row['mtime'] > roots[path]['mtime']]
            observation = dict(gone=gone, root_rewritten=moved,
                               files=len(current['files']))
            observations.append(observation)
            reached = bool(gone) and (cut == 'unlink' or bool(moved))
            if reached:
                return dict(reached=True, cut=cut,
                            seconds=time.monotonic() - started,
                            observations=observations[-20:],
                            note=('after the unlink, before a durable root rewrite'
                                  if cut == 'unlink' else
                                  'after the retirement root rewrite past the directory sync'))
            if time.monotonic() - started >= timeout:
                return dict(reached=False, cut=cut,
                            reason=f'no retirement {cut} within {timeout}s',
                            observations=observations[-20:])
            time.sleep(0.1)

    # ---- kills and stops ---------------------------------------------
    def kill_coordinator(self, child):
        record = dict(pid=child.process.pid, kill_unix_ns=now_ns())
        record['exit_code'] = child.kill()
        return record

    def kill_owner(self, host, reason):
        record = dict(host=host, reason=reason, kill_unix_ns=now_ns(),
                      pids=[row['pid'] for row in self.cluster.namespaces.get(host, [])])
        for namespace in self.cluster.namespaces.get(host, []):
            self.cluster.remote.shell(host, f'kill -KILL -{namespace["process_group"]}',
                                      check=False)
            deadline = time.monotonic() + 30
            while self.cluster.remote.exists(host, f'/proc/{namespace["pid"]}'):
                if time.monotonic() > deadline:
                    record['still_alive'] = namespace['pid']
                    break
                time.sleep(0.1)
        return record

    def stop_on_failure(self, child, count=1, *, timeout=300):
        """Stop a coordinator whose write core failed; the error rows are evidence."""
        def predicate():
            found = failures(child.rows())
            reached = len(found) >= count
            return dict(reached=reached, errors=len(found),
                        unix_ns=now_ns() if reached else None)
        gate = self.wait_for(child, predicate, timeout=timeout,
                             label=f'{count} typed write failure')
        if gate.get('reached'):
            child.signal(signal.SIGTERM)
        code = child.wait(120)
        return dict(gate=gate, exit_code=code)

    # ---- run records --------------------------------------------------
    def part(self, name, run, child, observations, *, verdict=True):
        return dict(name=name, role=run['role'], run=run, verdict=verdict,
                    output=str(child.output), log=str(child.log),
                    pid=child.process.pid, started_unix_ns=child.started_unix_ns,
                    finished_unix_ns=now_ns(),
                    exit_code=child.process.returncode,
                    stop=getattr(child, 'stop_record', None),
                    observations=observations)

    # ---- families -----------------------------------------------------
    def measure(self):
        run = self.cell['runs'][0]
        child = self.open(run, 'measure')
        before = self.cluster.disks()
        try:
            code = child.wait(run['deadline'] + 300)
            if code is None:
                raise CellError('measure run did not finish within its deadline')
            if code != 0:
                raise CellError(f'measure run exited {code}; see {child.log}')
        finally:
            child.stop()
        after = self.cluster.disks()
        rolls = {host: self.cluster.rolls(host) for host in self.cluster.owners}
        self.parts.append(self.part('measure', run, child,
                                    dict(role='measure', owner_before=before,
                                         owner_after=after, owner_rolls=rolls)))
        return self.parts

    def crash(self):
        cut = self.cell['plan']['cut']
        run = self.cell['runs'][0]
        child = self.open(run, 'measure')
        record = dict(name='measure', role='measure', pid=child.process.pid,
                      started_unix_ns=child.started_unix_ns)
        try:
            gate = (self.gate_writes(child, GATE_WRITES) if cut == 'publication'
                    else self.gate_index_growth(child))
            record['gate'] = gate
            if not gate.get('reached'):
                raise CellError(f"gate missed: {gate.get('reason')}")
            record['kill'] = self.kill_coordinator(child)
        finally:
            child.stop()
        record['finished_unix_ns'] = now_ns()
        record['exit_code'] = child.process.returncode
        record['stop'] = getattr(child, 'stop_record', None)
        record['log'] = str(child.log)
        record['output'] = str(child.output)
        self.parts.append(dict(record, run=run, verdict=False, observations=dict(
            role='crash', cut=cut, gate=record.get('gate'), kill=record.get('kill'))))
        self.parts.append(self.after_restart('verify', self.cell['runs'][1],
                                            dict(cut=cut, gate=record.get('gate'),
                                                 kill=record.get('kill'))))
        return self.parts

    def after_restart(self, name, run, observations):
        """Reopen the killed coordinator's roots and run the audit.

        The output directory is fresh; the coordinator database path is the one
        the killed run used, so this is a reopen of the same data.
        """
        chosen = self.bind_context()
        child = self.launch(run, name, path=self.cluster.root('measure'),
                            receipts=chosen)
        try:
            code = child.wait(run['deadline'] + 300)
            if code is None:
                raise CellError(f'{name} did not finish within its deadline')
            if code != 0:
                raise CellError(f'{name} exited {code}; see {child.log}')
        finally:
            child.stop()
        return self.part(name, run, child, dict(observations, role='verify'))

    def owner_crash(self):
        host = self.cluster.owners[0]
        run = self.cell['runs'][0]
        child = self.open(run, 'measure')
        record = dict(name='measure', role='measure', pid=child.process.pid,
                      started_unix_ns=child.started_unix_ns)
        try:
            gate = self.gate_writes(child, GATE_WRITES)
            record['gate'] = gate
            if not gate.get('reached'):
                raise CellError(f"gate missed: {gate.get('reason')}")
            record['kill'] = self.kill_owner(host, 'owner crash after the write gate')
            record['failure'] = self.stop_on_failure(child)
        finally:
            child.stop()
        record['finished_unix_ns'] = now_ns()
        record['exit_code'] = child.process.returncode
        record['stop'] = getattr(child, 'stop_record', None)
        record['log'] = str(child.log)
        record['output'] = str(child.output)
        restart = self.cluster.restart_owner(host)
        self.parts.append(dict(record, run=run, verdict=False, observations=dict(
            role='crash', gate=record.get('gate'), kill=record.get('kill'),
            failure=record.get('failure'), restart=restart)))
        self.parts.append(self.after_restart('verify', self.cell['runs'][1],
                                            dict(restart=restart, gate=record.get('gate'),
                                                 kill=record.get('kill'))))
        return self.parts

    def retire(self):
        host = self.cluster.owners[0]
        cut = self.cell['plan']['cut']
        run = self.cell['runs'][0]
        child = self.open(run, 'measure')
        record = dict(name='measure', role='measure', pid=child.process.pid,
                      started_unix_ns=child.started_unix_ns)
        try:
            gate = self.gate_retirement(host, cut)
            record['gate'] = gate
            if not gate.get('reached'):
                raise CellError(f"gate missed: {gate.get('reason')}")
            record['kill'] = self.kill_owner(host, f'retirement {cut}')
            record['failure'] = self.stop_on_failure(child)
        finally:
            child.stop()
        record['finished_unix_ns'] = now_ns()
        record['exit_code'] = child.process.returncode
        record['stop'] = getattr(child, 'stop_record', None)
        record['log'] = str(child.log)
        record['output'] = str(child.output)
        restart = self.cluster.restart_owner(host)
        self.parts.append(dict(record, run=run, verdict=False, observations=dict(
            role='crash', cut=cut, gate=record.get('gate'), kill=record.get('kill'),
            failure=record.get('failure'), restart=restart)))
        self.parts.append(self.after_restart('verify', self.cell['runs'][1],
                                            dict(restart=restart, cut=cut,
                                                 gate=record.get('gate'),
                                                 kill=record.get('kill'))))
        return self.parts

    # ---- fencing -------------------------------------------------------
    def fence_cell(self):
        case = self.cell['case']
        run = self.cell['runs'][0]
        host = self.cluster.owners[0]
        child = self.open(run, 'fence-a')
        record = dict(case=case, tier=self.cell['tier'], owners=len(self.cluster.owners),
                      started_unix_ns=now_ns())
        try:
            gate = self.gate_writes(child, GATE_WRITES)
            record['gate'] = gate
            if not gate.get('reached'):
                raise CellError(f"gate missed: {gate.get('reason')}")
            if case == 'lapse':
                record.update(self._lapse(child, host))
            elif case in ('contend', 'stale'):
                record.update(self._contend(child, case))
            elif case == 'sealed':
                record.update(self._sealed(child, host))
            else:
                raise CellError(f'unknown fence case {case}')
        finally:
            child.stop()
        record['a_output'] = str(child.output)
        record['stop'] = getattr(child, 'stop_record', None)
        self.fence = record
        self.parts.append(self.part('fence-a', run, child, dict(role='fence-a'),
                                    verdict=None))
        return self.parts

    def _seal_wait(self, host, started, *, timeout=180):
        deadline = time.monotonic() + timeout
        while True:
            markers = self.cluster.seal_markers(host)
            if markers['sealed'] >= 1:
                return dict(sealed_partitions=markers['sealed'],
                            databases=markers['databases'],
                            write_protect_apply_ms=(now_ns() - started) / 1e6)
            if time.monotonic() > deadline:
                raise CellError('owners published no sealed marker after the lease lapse')
            time.sleep(0.2)

    def _lapse(self, child, host):
        result = dict(sealed_partitions=0)
        started = now_ns()
        child.signal(signal.SIGSTOP)
        result['paused_unix_ns'] = started
        result.update(self._seal_wait(host, started))
        resumed = now_ns()
        child.signal(signal.SIGCONT)
        result['resumed_unix_ns'] = resumed
        result['failure'] = self.stop_on_failure(child, count=1, timeout=180)
        result['a_exit'] = result['failure'].get('exit_code')
        rows_ = child.rows()
        outcomes = [row.get('outcome') for row in rows_]
        first_error = next((index for index, value in enumerate(outcomes)
                            if value == 'error'), None)
        last_success = max((index for index, value in enumerate(outcomes)
                            if value == 'success'), default=None)
        text = rows_[first_error].get('error_kind') if first_error is not None else None
        result['refusal_text'] = text
        result['typed_refusal'] = bool(text) and self.typed(text)
        if text:
            result['fence_reject_ms'] = (now_ns() - resumed) / 1e6
        result['stale_write_refused'] = first_error is not None
        result['no_success_after_refusal'] = (first_error is not None and
                                              (last_success is None or
                                               last_success < first_error))
        result['writers'] = 1 if result['no_success_after_refusal'] else 2
        return result

    def _contend(self, child, case):
        """Freeze A, copy its root, start B on the copy, then resume A.

        `samples.csv` is written by one writer thread in completion order, so the
        row order is the operation order: every success of A must precede its
        first refusal. That is the "a stale writer cannot change data" check and
        it needs no timestamp from A's own clock.
        """
        child.signal(signal.SIGSTOP)
        source = Path(child.output).parent / 'coordinator'
        copy = self.cluster.root('fence-b')
        if copy.exists():
            shutil.rmtree(copy)
        shutil.copytree(source, copy)
        run = self.cell['runs'][0]
        second = self.launch(run, 'fence-b', path=copy)
        result = dict(copied_root=str(copy))
        try:
            gate = self.gate_writes(second, FENCE_WRITES, timeout=300)
            result['second_gate'] = gate
            if not gate.get('reached'):
                raise CellError(f"second coordinator wrote nothing: {gate.get('reason')}")
            child.signal(signal.SIGCONT)
            result['resumed_unix_ns'] = now_ns()
            result['failure'] = self.stop_on_failure(child, count=1)
        finally:
            second.stop()
        first_rows = child.rows()
        outcomes = [row.get('outcome') for row in first_rows]
        first_error = next((index for index, value in enumerate(outcomes)
                            if value == 'error'), None)
        last_success = max((index for index, value in enumerate(outcomes)
                            if value == 'success'), default=None)
        result['first_error_row'] = first_error
        result['last_success_row'] = last_success
        result['first_acknowledged'] = acknowledged(first_rows)
        result['second_acknowledged'] = acknowledged(second.rows())
        result['stale_write_refused'] = first_error is not None
        result['no_success_after_refusal'] = (first_error is not None and
                                              (last_success is None or
                                               last_success < first_error))
        text = first_rows[first_error].get('error_kind') if first_error is not None else None
        result['refusal_text'] = text
        result['typed_refusal'] = bool(text) and self.typed(text)
        # The stale case's fence latency: from A's resume to the observed first
        # typed refusal. The coordinator's sample writer flushes once a second,
        # so this is the observation latency, never a claim of the engine's own
        # syscall-to-refusal time.
        failure = result.get('failure') or {}
        gate = failure.get('gate') or {}
        if case == 'stale' and gate.get('reached') and gate.get('unix_ns') is not None:
            result['fence_reject_ms'] = ((gate['unix_ns'] - result['resumed_unix_ns'])
                                         / 1e6)
        writers = (1 if result['second_acknowledged'] else 0) + \
                  (1 if (last_success is not None and first_error is not None and
                         last_success > first_error) else 0)
        result['writers'] = writers
        return result

    def _sealed(self, child, host):
        result = self._lapse(child, host)
        run = mechanism.read_run(4_096, value=4096, verify=True, deadline=300)
        # The reader opens A's own coordinator root: the sealed generation is the
        # data under test, not a fresh database.
        source = Path(child.output).parent / 'coordinator'
        reader = self.launch(run, 'fence-read', path=source)
        started = now_ns()
        try:
            code = reader.wait(180)
            result['sealed_read_exit'] = code
            result['sealed_read_ms'] = (now_ns() - started) / 1e6
            manifest = json.loads((reader.output / 'manifest.json').read_text())
            result['sealed_read_ok'] = code == 0 and manifest['errors_or_wrong_values'] == 0
        finally:
            reader.stop()
        result['sealed_write_refused'] = result.get('stale_write_refused', False)
        result['sealed_write_text'] = result.get('refusal_text')
        return result

    @staticmethod
    def typed(text):
        lowered = text.lower()
        return any(token in lowered for token in
                   ('lease', 'fenc', 'write protect', 'write-protect',
                    'write protected', 'permission denied', 'stale coordinator',
                    'read-only', 'read only'))


def verdict_parts(driver, parts):
    """One verdict per part, plus the fence verdict when the cell ran one."""
    records = []
    for part in parts:
        observations = dict(part['observations'])
        events = driver.cluster.owner_events(driver.cluster.owners[0])
        observations['retire_requests'] = driver.cluster.retire_requests(events)
        observations['owner_events'] = bool(events)
        if part.get('verdict', True) is None:
            record = dict(cell=driver.cell['id'], role=part['role'], synthetic=True,
                          ok=True, checks=[],
                          note='lifecycle evidence only; the verdict is the fence record')
        elif part.get('verdict', True):
            try:
                record = mechanism.verify(driver.cell, part['output'],
                                          observations=observations)
            except (OSError, ValueError) as error:
                record = dict(cell=driver.cell['id'], role=part['role'], ok=False,
                              synthetic=True,
                              checks=[mechanism._check('artifacts readable', False,
                                                       str(error))])
        else:
            record = dict(cell=driver.cell['id'], role=part['role'], synthetic=True,
                          checks=crash_checks(driver.cell, observations))
            record['ok'] = all(check['ok'] for check in record['checks'])
        record['part'] = part['name']
        record['record'] = {key: value for key, value in part.items()
                            if key not in ('observations', 'run')}
        record['observations'] = {key: value for key, value in observations.items()
                                  if key not in ('owner_before', 'owner_after')}
        records.append(record)
    if driver.fence is not None:
        records.append(fence_record(driver))
    return records


def stop_records(driver, parts):
    """Every stop of the cell: each coordinator part and every owner group."""
    records = []
    for part in parts:
        record = part.get('stop')
        if record:
            records.append(dict(record, part=part.get('name'),
                                process='coordinator', output=part.get('output')))
    records.extend(dict(row, part='owners') for row in driver.cluster.stops)
    return records


def stop_verdict(cell, records):
    """A forced kill is a failed stop; a survivor fails it too."""
    checks = []
    for row in records:
        label = (f"{row.get('part')} {row.get('process') or 'owner'}"
                 if row.get('process') else
                 f"{row.get('part')} owner {row.get('partition')}")
        if 'host' in row:
            label += f" on {row['host']}"
        checks.append(mechanism._check(label + ' stopped without a forced kill',
                                       not row.get('forced'), row))
        checks.append(mechanism._check(label + ' left no live process',
                                       not row.get('still_alive'), row))
    return dict(cell=cell['id'], role='stop', synthetic=True,
                ok=all(check['ok'] for check in checks), checks=checks,
                stops=records)


def crash_checks(cell, observations):
    """Checks for the killed run itself: the cut landed where the plan asks."""
    gate = observations.get('gate') or {}
    kill = observations.get('kill') or {}
    failure = observations.get('failure') or {}
    checks = [mechanism._check('cut gate reached before the kill',
                               gate.get('reached') is True,
                               dict(label=gate.get('label'), cut=gate.get('cut'),
                                    note=gate.get('note')))]
    if cell['family'] == 'S1-2':
        checks.append(mechanism._check('coordinator was killed, not shut down',
                                       kill.get('exit_code') not in (0, None),
                                       kill.get('exit_code')))
    else:
        checks.append(mechanism._check('owner was killed with SIGKILL',
                                       bool(kill.get('pids')) and
                                       'still_alive' not in kill,
                                       kill.get('pids')))
        found = failure.get('gate') or {}
        checks.append(mechanism._check('coordinator recorded a typed write failure',
                                       found.get('reached') is True,
                                       found.get('errors')))
    return checks


def fence_record(driver):
    """The M8/S1-5 verdict: one writer, a typed refusal and the fence latencies."""
    fence = driver.fence
    case = fence['case']
    checks = [mechanism._check('exactly one writer', fence.get('writers') == 1,
                               fence.get('writers')),
              mechanism._check('stale writer refused with a typed error',
                               bool(fence.get('typed_refusal')),
                               fence.get('refusal_text')),
              mechanism._check('no successful write after the first refusal',
                               fence.get('no_success_after_refusal') is True,
                               dict(first_error_row=fence.get('first_error_row'),
                                    last_success_row=fence.get('last_success_row')))]
    if case in ('lapse', 'sealed'):
        checks.append(mechanism._check('write-protect applied on an owner',
                                       fence.get('sealed_partitions', 0) >= 1,
                                       fence.get('sealed_partitions')))
    if case == 'sealed':
        checks.append(mechanism._check('sealed generation is readable',
                                       fence.get('sealed_read_ok') is True,
                                       fence.get('sealed_read_ms')))
        checks.append(mechanism._check('sealed generation is not writable',
                                       fence.get('sealed_write_refused') is True,
                                       fence.get('sealed_write_text')))
    if case in ('lapse', 'stale'):
        checks.append(mechanism._check('fence latency measured',
                                       fence.get('fence_reject_ms') is not None,
                                       fence.get('fence_reject_ms')))
    if case in ('lapse', 'sealed'):
        checks.append(mechanism._check('write-protect apply latency measured',
                                       fence.get('write_protect_apply_ms') is not None,
                                       fence.get('write_protect_apply_ms')))
    return dict(cell=driver.cell['id'], role='fence', synthetic=True,
                ok=all(check['ok'] for check in checks), checks=checks, fence=fence)


# ---- entry point ----------------------------------------------------------

def cell_ids():
    return [cell['id'] for cell in mechanism.cells()]


def cell_table(stage='all'):
    """The reviewed fault cells of one stage, as printable rows."""
    if stage not in ('stage1', 'stage2', 'all'):
        raise SystemExit('--list takes stage1, stage2 or all')
    return [dict(id=cell['id'], stage=cell['stage'], tier=cell['tier'],
                 family=cell['family'], synthetic=bool(cell.get('synthetic')),
                 note=cell.get('note'))
            for cell in mechanism.cells()
            if stage == 'all' or cell['stage'] == stage]


def list_request(argv):
    """The requested stage of `--list [stage]` / `--list=stage`, else None."""
    for index, item in enumerate(argv):
        if item == '--list':
            following = argv[index + 1] if index + 1 < len(argv) else None
            return (following if following and not following.startswith('-')
                    else 'all')
        if item.startswith('--list='):
            return item.split('=', 1)[1]
    return None


def list_cells(stage='all'):
    rows = cell_table(stage)
    print(json.dumps(dict(cells=rows, count=len(rows)), indent=1))
    return 0


def failure_artifacts(output, config_dir):
    """Every retained file of a failed attempt, within a bounded curated tree.

    A cell that dies at owner provisioning registers no part, so the curated
    pass-path list names files it never wrote. The failed attempt's coordinator
    standard output and replay logs are then the only evidence that the run
    reached the kernel wall. Walk the attempt's own trees and skip only what is
    rebuildable or private: the coordinator database, TLS material and bulk
    storage.
    """
    rows, seen, skipped = [], set(), []
    for root, label in ((Path(output), ''), (Path(config_dir), 'external/')):
        if not root.is_dir():
            continue
        for path in sorted(root.rglob('*')):
            if not path.is_file():
                continue
            relative = path.relative_to(root)
            if any(part in FAILURE_SKIP_PARTS for part in relative.parts):
                continue
            if any(path.match(pattern) for pattern in FAILURE_ARTIFACT_SKIP):
                skipped.append(dict(path=str(path),
                                    reason='rebuildable owner archive'))
                continue
            if path.stat().st_size > FAILURE_ARTIFACT_LIMIT:
                skipped.append(dict(path=str(path),
                                    reason='above the failure upload bound'))
                continue
            name = label + relative.as_posix()
            if name in seen:
                continue
            seen.add(name)
            rows.append(campaign_upload.artifact(path, name))
    return rows, skipped


def cell_artifacts(output, parts):
    """Every retained file of one cell: compact records and per-sample files.

    `campaign_manifest.evidence_group` sends the summaries to `evidence/` and
    `samples.csv`, `*.prom` and `*.jsonl` to `raw/`.
    """
    rows, output = [], Path(output)

    def add(path, name):
        path = Path(path)
        if path.is_file():
            rows.append(campaign_upload.artifact(path, name))

    add(output / 'receipt.json', 'receipt.json')
    for part in parts:
        base = Path(part['output'])
        name = part['name']
        add(base.parent / 'spec.json', f'{name}/spec.json')
        for member in ('manifest.json', 'summary.json', 'samples.csv',
                       'measurement-read-evidence.json'):
            add(base / member, f'{name}/run/{member}')
        for path in sorted(base.glob('*.prom')):
            add(path, f'{name}/run/{path.name}')
    return rows


def fleet_zone(state_path):
    """The zone the fleet state records for its first admitted host, if any."""
    state = json.loads(Path(state_path).read_text())
    fleet = state.get('fleet') or [record['fleet'] for record in state.get('instances', [])
                                   if record.get('fleet')]
    return fleet[0].get('zone') if fleet else None


def attempt_paths(output, config_dir, attempt):
    """A retry writes a fresh output tree and external directory.

    The coordinator requires a fresh output, and `prepare_external.py` creates
    its directory once, so attempt two must not name attempt one's paths.
    """
    if attempt <= 1:
        return output, config_dir
    suffix = f'-attempt-{attempt}'
    return (output.with_name(output.name + suffix),
            config_dir.with_name(config_dir.name + suffix))


def main():
    stage = list_request(sys.argv[1:])
    if stage is not None:
        return list_cells(stage)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cell', required=True, choices=cell_ids())
    parser.add_argument('--build', type=Path, required=True)
    parser.add_argument('--schema', type=Path)
    parser.add_argument('--fixture', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--guest-root', type=Path, required=True)
    parser.add_argument('--coordinator', required=True)
    parser.add_argument('--owners', required=True, help='owner1,owner2')
    parser.add_argument('--zone', help='zone of the three hosts, for the SSH callback')
    parser.add_argument('--config-dir', type=Path,
                        help='fresh external config directory (default: beside the output)')
    parser.add_argument('--transport', choices=('tcp', 'tcp-trusted'), default='tcp')
    parser.add_argument('--live-transport', choices=('tcp', 'rdma'), default='rdma')
    parser.add_argument('--durability-tier', choices=('native', 'synchronous'),
                        help='default: the cell tier')
    parser.add_argument('--ledger', type=Path,
                        help='campaign ledger (default: beside the output)')
    parser.add_argument('--bucket-prefix',
                        help='campaign object prefix, gs://mysten-research-bench-artifacts/'
                             '<campaign-id>; a cell passes only after its verified upload')
    parser.add_argument('--impersonation-account', default=campaign_manifest.ACCOUNT,
                        help='service account that writes the campaign prefix')
    parser.add_argument('--allow-local-ledger', action='store_true',
                        help='local rehearsal only: pass a cell without an uploaded bundle')
    parser.add_argument('--resume', action='store_true',
                        help='accepted for older command lines; the ledger is always '
                             'merged and a passed cell is always skipped')
    parser.add_argument('--dry-run', action='store_true',
                        help='print the plan and exit; no host is touched')
    parser.add_argument('--tunnel-through-iap', action='store_true',
                        help='rehearsal only: SSH to hosts with a reachable address')
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
    args = parser.parse_args()

    if not args.dry_run:
        # A dry run prints a plan and touches no host; every other run needs
        # the fleet's proved initiator stack before its first owner starts.
        manual_sui.kernel_gate(args.fleet_state, args.live_transport)
    cell = mechanism.find(args.cell)
    tier = args.durability_tier or cell['tier']
    if tier != cell['tier']:
        raise SystemExit(f'{cell["id"]} is a {cell["tier"]} cell; --durability-tier differs')
    output = args.output.resolve()
    config_dir = (args.config_dir or output.with_name(output.name + '-external')).resolve()
    if args.zone is None and args.fleet_state is not None:
        args.zone = fleet_zone(args.fleet_state)
    plan = dict(cell=cell['id'], stage=cell['stage'], unit=cell['unit'], tier=tier,
                synthetic=True, output=str(output), config_dir=str(config_dir),
                guest_root=str(args.guest_root), coordinator=args.coordinator,
                owners=args.owners.split(','), zone=args.zone,
                note=cell.get('note'),
                runs=[dict(role=run['role'], batch=run['batch'], value=run['value'],
                           concurrency=run['concurrency'], mix=run['mix'],
                           measurement=run['measurement']) for run in cell['runs']])
    if args.dry_run:
        print(json.dumps(plan, indent=2, sort_keys=True))
        return 0
    if args.fixture is None or args.schema is None:
        raise SystemExit('--fixture and --schema bind the ledger identity; both are required')
    if not args.bucket_prefix and not args.allow_local_ledger:
        raise SystemExit('a passed cell needs a verified evidence upload; give '
                         '--bucket-prefix, or --allow-local-ledger for a local rehearsal')
    store = prefix = None
    if args.bucket_prefix:
        store, _, prefix = campaign_upload.open_store(
            args.bucket_prefix, args.impersonation_account, guest=args.guest_token)
    hosts = manual_sui.plan(args.coordinator, args.owners.split(','))
    if not args.gcloud_ssh and not args.on_coordinator:
        # The coordinator process is local and dials the owners, so this cell
        # runs on H0 with the same knob set.
        return mailbox_exec.dispatch(
            args, coordinator=args.coordinator,
            script_relative='experiments/gcp-rdma/fault_cells.py',
            trees=[('build', args.build), ('fixture', args.fixture), ('schema', args.schema)],
            values={'--build': 'build', '--fixture': 'fixture', '--schema': 'schema',
                    '--output': Path('out') / args.output.name,
                    '--config-dir': 'external', '--ledger': 'campaign-ledger.json'})
    ledger_path = args.ledger or output.with_name(output.name + '-ledger.json')
    ledger, entry = ledger_module.Ledger(ledger_path), None
    evidence = (campaign_upload.Evidence(store, prefix) if store is not None else None)
    # The bucket ledger is the campaign record: load and merge it on every run,
    # not only with --resume, so a passed cell is never rerun on a new fleet.
    if Path(ledger_path).exists():
        ledger.load()
    if evidence is not None:
        ledger = evidence.merge(ledger)
    manifest = json.loads((Path(args.build) / 'build-manifest.json').read_text())
    identity = ledger_module.config_hash(
        spec=dict(build_manifest=digest(Path(args.build) / 'build-manifest.json'),
                  fixture=digest(Path(args.fixture) / 'experiment/results.json'),
                  schema={str(path.relative_to(args.schema)): digest(path)
                          for path in sorted(Path(args.schema).rglob('*')) if path.is_file()}),
        knobs=dict(tier=tier, cell=cell, transport=args.transport,
                   live_transport=args.live_transport, owners=hosts['owners']),
        engine_rev=manifest.get('source_archive_sha256'),
        sui_rev=manifest.get('source_archive_sha256'),
        binary_ids=manifest['binaries'])
    previous = ledger.entries.get((cell['stage'], cell['id']))
    if (previous and previous['status'] == 'passed'
            and previous['config_hash'] == identity):
        print(json.dumps(dict(cell=cell['id'], skipped=True)))
        return 0
    entry = ledger.begin(stage=cell['stage'], cell_id=cell['id'],
                         config_hash=identity, unit=cell['unit'])
    # A retry never reuses the previous attempt's tree: a fresh run needs a
    # fresh output, and the external preparation creates its directory once.
    output, config_dir = attempt_paths(output, config_dir, entry['attempts'])
    plan.update(output=str(output), config_dir=str(config_dir))
    ledger.save()
    cluster = Cluster(remote=mailbox_exec.open_executor(args),
                      build_root=args.build, output=output,
                      guest_root=args.guest_root, config_dir=config_dir,
                      owners=hosts['owners'], tier=tier, transport=args.transport,
                      live_transport=args.live_transport, schema=args.schema)
    cluster.admit()
    driver = CellDriver(cluster, cell)
    parts = []
    try:
        cluster.prepare()
        cluster.bind(cluster.database())
        cluster.install()
        cluster.start()
        try:
            family = cell['family']
            if family in ('M6', 'M9', 'M13'):
                parts = driver.measure()
            elif family == 'S1-2':
                parts = driver.crash()
            elif family == 'S1-3':
                parts = driver.owner_crash()
            elif family == 'S1-6':
                parts = driver.retire()
            elif family == 'S1-5':
                parts = driver.fence_cell()
            else:
                raise CellError(f'no driver for {family}')
        finally:
            cluster.stop()
        for part in parts:
            # The killed runs keep their rows in `samples.csv.tmp`; only a
            # completed run is parsed by the verdict.
            if part.get('verdict', True):
                preflight(part['output'])
        records = verdict_parts(driver, parts)
        records.append(stop_verdict(cell, stop_records(driver, parts)))
        ok = all(record['ok'] for record in records)
        receipt = dict(schema=1, cell=cell['id'], stage=cell['stage'],
                       unit=cell['unit'], tier=tier, synthetic=True,
                       plan=plan, ok=ok, records=records,
                       finished_unix_ns=now_ns())
        receipt_path = output / 'receipt.json'
        receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
        artifacts = cell_artifacts(output, parts)
        if ok:
            if evidence is not None:
                evidence.persist(ledger, entry, artifacts)
                ledger.save()
            else:
                campaign_upload.pass_local(ledger, entry, artifacts)
        else:
            reason = 'one or more cell checks failed'
            ledger.fail(entry, reason)
            if evidence is not None:
                failed, skipped = failure_artifacts(output, config_dir)
                entry['failure_skipped'] = skipped
                evidence.persist_failure(entry, failed, reason)
            ledger.save()
    except Exception as error:
        # The failure upload keeps this attempt's receipt, specs and samples: a
        # fleet DELETE must not destroy the only copy. The terminal status is
        # set here and never inside the exit path below.
        ledger.fail(entry, error)
        if evidence is not None:
            failed, skipped = failure_artifacts(output, config_dir)
            entry['failure_skipped'] = skipped
            evidence.persist_failure(entry, failed, error)
        ledger.save()
        raise
    print(json.dumps(dict(cell=cell['id'], ok=ok, receipt=str(receipt_path))))
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
