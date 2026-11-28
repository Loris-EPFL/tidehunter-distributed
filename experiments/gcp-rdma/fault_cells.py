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
from collections import deque
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
import gcp_runtime
import host_exec
import mailbox_exec
import manual_sui
import owner_lifecycle
import stage2_mechanism as mechanism
import stage3_specs

BINARY = 'storage-node'
OWNER_READY_SECONDS = 180
GATE_TIMEOUT_SECONDS = 600
# Relocator causes that name the owner partition they reached (relocation.rs,
# dead_wal_files.rs); group 1 is the partition.
RELOCATOR_CAUSE = re.compile(
    r'(?:relocation session to|relocation page of|retirement session to|'
    r'retirement request to|retirement list of|sealed retirement of) '
    r'partition (\d+)\b[^:]*? failed')
# The coordinator declares its own close bound as `shutdown_grace_seconds` in
# the RunConfig this driver writes, so its stop allowance is the same constant.
COORDINATOR_STOP_GRACE_SECONDS = stage3_specs.SHUTDOWN_GRACE_SECONDS
# An owner is not the measured process and its close path tears down every live
# NVMe-oF target, then writes and syncs a shutdown proof per database; the old
# proven controller allowed 90 s for exactly that (`cleanup_timeout`).
OWNER_STOP_GRACE_SECONDS = owner_lifecycle.OWNER_STOP_SECONDS
# A failed attempt uploads what it still holds before the fleet is deleted. The
# local coordinator database, TLS material and any bulk storage tree never
# travel, and one oversized file must not consume the whole failure budget.
FAILURE_ARTIFACT_LIMIT = 128 << 20
FAILURE_ARTIFACT_SKIP = ('host-*.tar.gz',)
SAMPLE_EXCERPT_ROWS = 200
FAILURE_SKIP_PARTS = ('coordinator', 'tls', 'storage')
GATE_WRITES = 200
FENCE_WRITES = 20
# The engine nests every per-generation owner file under
# `db-<database>/epoch_<generation>`, so each listing walks that pair instead of
# the database directory alone. The loops are quoted once and shared by every
# WAL and seal listing.
EPOCH_LISTING = ('for d in "$1"/db-*; do [ -d "$d" ] || continue; '
                 'for g in "$d"/epoch_*; do [ -d "$g" ] || continue; ')
WAL = re.compile(r'^wal_([0-9a-f]{16})$')
DATABASE = re.compile(r'^db-[0-9a-f]{32}$')
# The owner persists its fence ledger here (`tcp/inventory.rs` `MANIFEST`); the
# fence rewrites it before the new epoch can serve a write.
INVENTORY_MANIFEST = 'tcp-node-inventory-v1'
# `fencing_generations::check_owner_status` refuses a stale root with this text.
FENCING_DISAGREEMENT = 'fencing inventory disagrees'
HIGH_WATER = 'high-water'
# A stopped process reaches state `T` in microseconds; the allowance only covers
# a loaded host, never an unfrozen writer.
FREEZE_TIMEOUT_SECONDS = 5.0
FROZEN_STATES = ('T', 't')
GONE_STATES = ('Z', 'X')


class CellError(RuntimeError):
    """A precondition, gate or verification failed; evidence is still retained."""


def process_group_states(pgid):
    """State letter of every live process in `pgid` (`/proc/<pid>/stat`)."""
    states = {}
    for entry in Path('/proc').iterdir():
        if not entry.name.isdigit():
            continue
        try:
            text = (entry / 'stat').read_text()
        except OSError:
            continue
        # `comm` may contain spaces and parentheses, so split after the last
        # `)`: the fields are state, ppid, pgrp, session, ...
        try:
            fields = text.rsplit(')', 1)[1].split()
            state, pgrp = fields[0], int(fields[2])
        except (IndexError, ValueError):
            continue
        if pgrp == pgid:
            states[int(entry.name)] = state
    return states


def freeze_group(pgid, *, timeout=FREEZE_TIMEOUT_SECONDS):
    """SIGSTOP a process group and prove every member reached state `T`.

    A signal is queued, not applied: `killpg` returns while the writer still
    runs. A cell that copies a root or launches a second authority after an
    unverified stop can therefore race the very write it means to exclude.
    A zombie cannot write, so it is reported as gone instead of blocking.
    """
    started = time.monotonic()
    try:
        os.killpg(pgid, signal.SIGSTOP)
    except ProcessLookupError as error:
        raise CellError(f'cannot freeze process group {pgid}: it is gone') from error
    deadline = started + timeout
    while True:
        states = process_group_states(pgid)
        alive = {pid: state for pid, state in states.items()
                 if state not in GONE_STATES}
        if states and alive and all(state in FROZEN_STATES
                                    for state in alive.values()):
            return dict(pids=sorted(states), states=states,
                        gone=sorted(pid for pid in states if pid not in alive),
                        unix_ns=now_ns(),
                        waited_ms=round((time.monotonic() - started) * 1e3, 3))
        if not states:
            raise CellError(f'cannot freeze process group {pgid}: no live process')
        if not alive:
            raise CellError(f'process group {pgid} has no live process to freeze')
        if time.monotonic() > deadline:
            raise CellError(
                f'process group {pgid} did not stop within {timeout}s: '
                + ', '.join(f'{pid}={state}'
                            for pid, state in sorted(alive.items())))
        time.sleep(0.005)


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


def refused(result, *, phase='measurement'):
    """Mutation rows that did not succeed, in completion order.

    A lease or fence refusal reaches a mutation as a transport failure, so the
    engine records `unknown_mutation` (its mutation outcome is unknown), never
    `error`. The first non-success row is therefore the first refusal.
    """
    return mechanism.refused(result, phase=phase)


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
                 shard_cache_mib=0, cleanup_root=None):
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
        self.cleanup_root = Path(cleanup_root) if cleanup_root else None
        self.cleanups = []

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
        script = owner_lifecycle.PREVIOUS_OWNERS_SCRIPT
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
                if namespace.get('stopped'):
                    continue
                record = owner_lifecycle.stop_owner(
                    self.remote, host=host, pid=namespace['pid'],
                    process_group=namespace['process_group'],
                    stop_path=namespace.get('stop'),
                    partition=namespace.get('partition'), source='cell',
                    grace=OWNER_STOP_GRACE_SECONDS,
                    kill_grace=owner_lifecycle.OWNER_KILL_SECONDS)
                namespace['stopped'] = not record['still_alive']
                records.append(record)
        self.namespaces = {}
        self.stops.extend(records)
        self.cleanups = getattr(self, 'cleanups', [])
        survivors = sorted({row['host'] for row in records if row['still_alive']})
        if survivors:
            for host in survivors:
                self.cleanups.append(dict(
                    host=host, root=str(self.cleanup_root) if self.cleanup_root
                    else owner_lifecycle.CONFIGFS,
                    prefix=owner_lifecycle.NQN_PREFIX, before=[], removed=[],
                    remains=[], unavailable=False, complete=False, exit_code=None,
                    error='owner still alive; cleanup refused'))
        else:
            self.cleanups.extend(owner_lifecycle.cleanup_hosts(
                self.remote, self.owners,
                root=getattr(self, 'cleanup_root', None)))
        self.write_lifecycle_evidence()
        return records

    def write_lifecycle_evidence(self):
        """Owner stops and cleanups travel with the cell, passed or failed."""
        for name, rows in (('owner-stops.json', self.stops),
                           ('owner-cleanup.json', self.cleanups)):
            path = self.local(name)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(rows, indent=2, sort_keys=True) + '\n')

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
        """Owner WAL files with size and mtime, and each durable root's mtime.

        The root is replaced by rename, so it is absent for an instant while a
        pass stores a new one. A missing root is therefore not a failure: the
        script ends with `exit 0` and the parser keeps only complete lines. A
        transport failure still exits non-zero from the executor.
        """
        # `find -printf %T@` carries fractional seconds; `stat -c %Y` would
        # miss a root rewrite inside the same second.
        script = (EPOCH_LISTING +
                  'find "$g" -maxdepth 1 \\( -name "wal_*" -o -name "partition-wal-root-v1" \\) '
                  '-printf "%p %s %T@\\n" 2>/dev/null; done; done; exit 0')
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
        """Owner fence evidence: write-protect markers, generations and manifest.

        `partition-seal-v5` is a per-generation file under
        `db-<database>/epoch_<generation>` (`storage.rs` `SEAL`); the live
        write-protect marker is a file under `db-<database>/live-target/sealed`
        (`nvmeof/live_target.rs` `write_sealed`); the owner's fence ledger is the
        root file `tcp-node-inventory-v1` (`tcp/inventory.rs` `MANIFEST`).

        The marker count alone is not a fence invariant: `protect_live_targets`
        publishes nothing when the fenced epoch has no writable live binding.
        The generation directory and the ledger rewrite are unconditional, so
        all three are returned with their mtimes: a count proves a marker, a
        mtime names the fence instant on the owner clock.
        """
        db_script = (
            'for d in "$1"/db-*; do [ -d "$d" ] || continue; '
            'n=$(ls -1 "$d/live-target/sealed" 2>/dev/null | wc -l); '
            'echo "db $d $n"; '
            'find "$d/live-target/sealed" -maxdepth 1 -type f '
            '-printf "seal %p %s %T@\\n" 2>/dev/null; '
            'find "$d" -maxdepth 1 -type f '
            '-printf "rootfile %p %s %T@\\n" 2>/dev/null; '
            'find "$d" -maxdepth 1 -type d -name "epoch_*" '
            '-printf "epochdir %p %T@\\n" 2>/dev/null; done')
        epoch_script = (EPOCH_LISTING +
                        'c=$(ls -1 "$g"/partition-seal-v5 2>/dev/null | wc -l); '
                        'echo "epoch $d $c"; done; done')
        script = f'{db_script}; {epoch_script}'
        code, out, err = self.remote.shell(
            host, f'sh -c {quote(script)} -- {quote(self.owner_root(host))}', check=False)
        if code != 0:
            raise CellError(f'{host}: cannot read the sealed markers')
        live, epochs, markers, epoch_dirs, rootfiles = {}, {}, {}, {}, {}
        for line in out.decode().splitlines():
            parts = line.split()
            if not parts:
                continue
            kind = parts[0]
            try:
                if kind == 'db' and len(parts) == 3:
                    live[parts[1]] = int(parts[2])
                elif kind == 'epoch' and len(parts) == 3:
                    epochs[parts[1]] = epochs.get(parts[1], 0) + int(parts[2])
                elif kind == 'seal' and len(parts) == 4:
                    markers[parts[1]] = dict(size=int(parts[2]),
                                             mtime=float(parts[3]))
                elif kind == 'rootfile' and len(parts) == 4:
                    rootfiles[parts[1]] = dict(size=int(parts[2]),
                                               mtime=float(parts[3]))
                elif kind == 'epochdir' and len(parts) == 3:
                    epoch_dirs[parts[1]] = dict(mtime=float(parts[2]))
            except ValueError:
                continue
        databases = len(live)
        sealed = sum(1 for path, count in live.items()
                     if count > 0 or epochs.get(path, 0) > 0)
        return dict(host=host, databases=databases, sealed=sealed,
                    markers=markers, epoch_dirs=epoch_dirs,
                    rootfiles=rootfiles, unix_ns=now_ns())

    def generation_wal(self, host):
        """Owner `wal_*` files per `epoch_<generation>` directory.

        The engine nests a database's WAL under `db-<database>/epoch_<generation>`
        (`tcp/inventory.rs` `epoch_path`), so a generation's files are the unit
        that tells whether the stale generation received anything after the
        fence: the new coordinator writes its own generation.
        """
        script = (EPOCH_LISTING +
                  'find "$g" -maxdepth 1 -name "wal_*" -printf "%p %s %T@\\n" '
                  '2>/dev/null; done; done; exit 0')
        code, out, err = self.remote.shell(
            host, f'sh -c {quote(script)} -- {quote(self.owner_root(host))}', check=False)
        if code != 0:
            raise CellError(f'{host}: cannot list the generation WAL: '
                            f'{err.decode(errors="replace")[-300:]}')
        generations = {}
        for line in out.decode().splitlines():
            parts = line.split()
            if len(parts) != 3:
                continue
            path, size, stamp = parts
            generation = str(Path(path).parent)
            generations.setdefault(generation, {})[path] = dict(
                size=int(size), mtime=float(stamp))
        return dict(host=host, generations=generations, unix_ns=now_ns())

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

    def __init__(self, *, binary, spec, directory, name='run', hold_completion=False):
        self.directory = Path(directory)
        self.output = self.directory / name
        self.output.mkdir(parents=True, exist_ok=False)
        self.log = self.directory / f'{name}.log'
        self.stream = self.log.open('xb')
        # The measurement window is binary-gated: the run creates its ready
        # marker and waits for the release this process writes. `prepare`
        # removes a stale pair and creates the marker directory. A relocation
        # cell holds the completion gate until its owner-side retirement gate
        # passes, because the relocation worker has no non-test completion
        # signal.
        self.gates = mechanism.LocalGates(spec, hold_completion=hold_completion)
        self.gates.prepare()
        self.process = subprocess.Popen([str(binary), '--config', str(spec),
                                         '--output', str(self.output)],
                                        stdout=self.stream, stderr=subprocess.STDOUT,
                                        start_new_session=True)
        self.started_unix_ns = now_ns()
        self.killed_unix_ns = None
        self.stop_record = None
        self.freeze_record = None
        self._gate_stop = threading.Event()
        self._gate_thread = threading.Thread(target=self.gates.run,
                                             args=(self._gate_stop,), daemon=True)
        self._gate_thread.start()

    def completion_ready(self):
        return self.gates.completion_ready()

    def release_completion(self):
        self.gates.release_completion()

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

    def freeze(self, *, timeout=FREEZE_TIMEOUT_SECONDS):
        """Stop the coordinator and prove the stop, before anything depends on it."""
        if self.process.poll() is not None:
            raise CellError(f'the coordinator exited ({self.process.returncode}) '
                            'before it could be frozen')
        self.freeze_record = freeze_group(self.process.pid, timeout=timeout)
        return self.freeze_record

    def resume(self):
        """Release the coordinator; the instant before SIGCONT is the release.

        `killpg` returns while the signal is still in flight, so a stamp after
        it is later than the first post-release write. The caller compares rows
        against the pre-signal stamp and reports the bracket width.
        """
        released = now_ns()
        self.signal(signal.SIGCONT)
        return dict(released_unix_ns=released, release_returned_unix_ns=now_ns())

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
                           directory=self.cluster.local(name), name=run_name,
                           hold_completion=bool(run.get('relocation')))

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

    def gate_snapshot(self, child, *, timeout=GATE_TIMEOUT_SECONDS):
        """Wait until the snapshot pass stores a new control region.

        The coordinator's index lives in `cr` (db.rs `CONTROL_REGION_FILE`) under
        the database directory the backend derives from the spec
        (`options.path/coordinator`); the snapshot writes a temp and renames it,
        so a store changes the file's size or mtime. A fresh database has no
        `cr` until the first store, so its appearance is the store; a reopen
        starts from the region the previous run left, so a changed `cr` is the
        store. The caller waits for the write gate first, so a store is taken
        over data. This gate reads only files: the sample file is too large to
        parse on every poll.

        Each observation also carries the coordinator WAL's allocated filesystem
        footprint, which bounds what the engine had written at the cut. It is a
        footprint (`st_blocks`), not the engine's logical `index_value_bytes()`
        counter and not a value-byte count; `S1-2S-snapshot-native` missed the
        8 GiB default with a 101 s run and retained no written volume at all.
        """
        baseline = self._coordinator_wal_footprint(child)
        state = dict(previous=self._snapshot_files(child))

        def predicate():
            current = self._snapshot_files(child)
            previous = state['previous']
            wal = self._coordinator_wal_footprint(child)
            row = dict(coordinator_wal_footprint_bytes=wal,
                       coordinator_wal_footprint_delta=(
                           None if baseline is None or wal is None else wal - baseline))
            changed = {name: [previous.get(name), value]
                       for name, value in current.items()
                       if previous.get(name) != value}
            state['previous'] = current
            return dict(row, reached=bool(changed), changed=changed, files=len(current))
        gate = self.wait_for(child, predicate, timeout=timeout, poll=0.1,
                             label='snapshot control-region store')
        gate['coordinator_wal_footprint_baseline'] = baseline
        gate['wal_measure'] = ('allocated filesystem footprint (st_blocks x 512) of '
                               'the coordinator WAL files; not the engine index_value_bytes '
                               'and not a value-byte count')
        return gate

    def _coordinator_database(self, child):
        """The coordinator database directory of one run.

        `kv-research` opens `backend.options.path/coordinator` as the database
        (`benchmarks/kv-research/src/backends/tidehunter.rs`, `coordinator_path`)
        and the hybrid index writes `cr` and its WAL files there. The spec sits
        beside the child's output, so this reads the document the child opened
        instead of rebuilding the backend's path from the driver's side.
        """
        spec = json.loads((Path(child.output).parent / 'spec.json').read_text())
        return Path(spec['backend']['options']['path']) / 'coordinator'

    def _coordinator_wal_footprint(self, child):
        """Allocated filesystem footprint of the coordinator's WAL files, or None.

        `set_len` gives each new index WAL file its apparent size without
        allocating blocks, so the block footprint bounds the bytes actually
        written. A file that cannot be stat'ed is skipped, and no WAL at all
        answers None.
        """
        root = self._coordinator_database(child)
        total, seen = 0, False
        for path in root.glob('wal_*'):
            try:
                total += path.stat().st_blocks * 512
            except OSError:
                continue
            seen = True
        return total if seen else None

    def _snapshot_files(self, child):
        """Size and nanosecond mtime of the control region, or empty before it exists."""
        path = self._coordinator_database(child) / 'cr'
        try:
            stat = path.stat()
        except FileNotFoundError:
            return {}
        return {'cr': [stat.st_size, stat.st_mtime_ns]}

    def _wait_for_wal_baseline(self, host, *, timeout=GATE_TIMEOUT_SECONDS, child=None):
        """A WAL baseline taken after the owner wrote, not at launch.

        The measurement run preloads before the relocation phase, so the first
        files appear seconds after launch. Baselining an empty tree would make
        the later unlink unobservable. A run that has already exited has no
        baseline: `None` lets the gate name the exit instead of polling to its
        timeout.
        """
        deadline = time.monotonic() + timeout
        while True:
            if child is not None and not child.alive():
                return None
            view = self.cluster.wal(host)
            if view['files']:
                return view
            if time.monotonic() > deadline:
                raise CellError('owners wrote no WAL file to baseline')
            time.sleep(0.2)

    def await_owner_retirement(self, child, *, timeout=GATE_TIMEOUT_SECONDS):
        """Observe the post-measurement relocation pass retire owner WAL files.

        The worker publishes no non-test completion signal, so the driver holds
        the binary's measurement completion gate and watches the owner files:
        the pass copies survivors and then unlinks whole retired files. The gate
        is released once a file disappears, and the binary's own gate timeout
        bounds the wait if it never does.
        """
        before = {}
        for host in self.cluster.owners:
            view = self._wait_for_wal_baseline(host, child=child)
            if view is None:
                return dict(reached=False, observations=[],
                            reason='coordinator exited before the owner retirement')
            before[host] = view
        ready = self.wait_for(
            child, lambda: dict(reached=child.completion_ready()), timeout=timeout,
            label='measurement completion gate', poll=0.05)
        if not ready.get('reached'):
            return dict(reached=False, reason=ready.get('reason'),
                        observations=ready.get('observations', []))
        started = time.monotonic()
        observations = []
        while True:
            if not child.alive():
                return dict(reached=False,
                            reason='coordinator exited before the owner retirement',
                            observations=observations[-20:])
            current = {host: self.cluster.wal(host) for host in self.cluster.owners}
            gone = {host: [path for path in before[host]['files']
                           if path not in current[host]['files']]
                    for host in self.cluster.owners}
            observations.append(dict(gone=gone,
                                     files={host: len(view['files'])
                                            for host, view in current.items()}))
            if any(gone.values()):
                return dict(reached=True, seconds=time.monotonic() - started,
                            observations=observations[-20:],
                            note='owner WAL file retired by the relocation pass')
            if time.monotonic() - started >= timeout:
                return dict(reached=False,
                            reason=f'no owner retirement within {timeout}s',
                            observations=observations[-20:])
            time.sleep(0.1)

    def gate_retirement(self, host, cut, *, timeout=GATE_TIMEOUT_SECONDS, child=None):
        base_started = time.monotonic()
        before = (self._wait_for_wal_baseline(host, timeout=timeout, child=child)
                  if child is not None else self.cluster.wal(host))
        if before is None:
            return dict(reached=False, cut=cut,
                        reason='coordinator exited before the owner retirement')
        if cut == 'dir-sync':
            # A root exists only after the owner's first store. The rewrite
            # being cut on is a change of an existing root, so baseline once
            # one exists instead of treating its first creation as a rewrite.
            while not before['roots']:
                if child is not None and not child.alive():
                    return dict(reached=False, cut=cut,
                                reason='coordinator exited before the owner retirement')
                if time.monotonic() - base_started >= timeout:
                    return dict(reached=False, cut=cut,
                                reason='no durable root to baseline')
                time.sleep(0.2)
                before = self.cluster.wal(host)
        baseline = dict(before['files'])
        roots = dict(before['roots'])
        if child is not None:
            # The relocation pass starts just before the completion gate, so the
            # gate's ready marker names the moment the retirement request is in
            # flight; the files baseline above predates it.
            ready = self.wait_for(
                child, lambda: dict(reached=child.completion_ready()),
                timeout=timeout, label='measurement completion gate', poll=0.05)
            if not ready.get('reached'):
                return dict(reached=False, cut=cut, reason=ready.get('reason'))
        started = time.monotonic()
        observations = []
        while True:
            if child is not None and not child.alive():
                return dict(reached=False, cut=cut,
                            reason='coordinator exited before the owner retirement',
                            observations=observations[-20:])
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
        rows = self.cluster.namespaces.get(host, [])
        record = dict(host=host, reason=reason,
                      partition=rows[0].get('partition') if rows else None,
                      kill_unix_ns=now_ns(),
                      pids=[row['pid'] for row in rows])
        for namespace in rows:
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
        """Stop a coordinator whose write core refused; the rows are evidence.

        The refusal reaches a mutation as a transport failure, so the engine
        records `unknown_mutation`, not `error`; `refused` keeps every
        non-success mutation row.
        """
        found = []

        def predicate():
            found[:] = refused(child.rows())
            reached = len(found) >= count
            return dict(reached=reached, refusals=len(found),
                        text=found[0].get('error_kind') if reached else None,
                        unix_ns=now_ns() if reached else None)
        gate = self.wait_for(child, predicate, timeout=timeout,
                             label=f'{count} typed write refusal')
        if gate.get('reached'):
            child.signal(signal.SIGTERM)
        code = child.wait(120)
        return dict(gate=gate, exit_code=code,
                    refusal=found[0].get('error_kind') if found else None)

    @staticmethod
    def _log_offset(child):
        """Bytes in the coordinator log now, or `None` when it cannot be read.

        `None` is not `0`: a log that cannot be stat'ed has no watermark, and a
        caller that must attribute later lines cannot treat it as an empty file.
        Returning 0 here would make a later successful read scan the whole log,
        including lines written before the freeze.
        """
        try:
            return Path(child.log).stat().st_size
        except (AttributeError, OSError, TypeError):
            return None

    @staticmethod
    def _log_appended(child, offset):
        """Text the coordinator appended after `offset`; no watermark, no text."""
        if offset is None:
            return ''
        try:
            with Path(child.log).open('rb') as stream:
                stream.seek(offset)
                return stream.read().decode('utf-8', 'replace')
        except (AttributeError, OSError, TypeError, ValueError):
            return ''

    def stop_on_terminal_failure(self, child, *, partition, log_offset, timeout=180):
        """Wait for the coordinator's own typed failure after an owner kill.

        A retirement kill lands after the measurement window, so no sample row
        follows it and the write-refusal gate cannot see the failure. The
        failure is the coordinator's exit: the relocation pass reaches the dead
        owner, and the run ends with the typed first-failure record in
        `run.log`. The relocator's own failure poisons the write core through
        `begin_failure` with no partition (`owner=None ... stage=write_core`)
        and names the partition in its cause: `relocation.rs` "relocation
        session to partition N failed" / "relocation page of partition N
        generation G failed", `dead_wal_files.rs` "retirement session to
        partition N failed" / "retirement request to partition N failed" /
        "retirement list of partition N generation G failed" / "sealed
        retirement of partition N generation G failed". A per-owner worker
        records `owner=Some(N)`. Either names the killed owner; another
        partition does not. Only the bytes appended after the pre-signal
        watermark count, so a line written before the kill cannot satisfy the
        check.
        """
        code = child.wait(timeout)
        text = self._log_appended(child, log_offset).strip()
        owner = f'owner=Some({partition})'
        named = {int(match) for match in RELOCATOR_CAUSE.findall(text)}
        retired = 'stage=write_core' in text and partition in named
        typed = (partition is not None and 'first causal failure' in text
                 and (owner in text or retired))
        record = dict(exit_code=code, partition=partition,
                      log_offset=log_offset, text=text, typed=typed,
                      unix_ns=now_ns())
        if code is None:
            record['reason'] = (f'coordinator did not exit within {timeout}s '
                                'of the owner kill')
        return record

    def fence_owner_snapshot(self):
        """Timestamped owner-side fence evidence: seals and per-generation WAL.

        Read once before the stale writer may run again and once after the cell,
        so the verdict can show that the owners fenced first and that the stale
        generation's WAL gained nothing afterwards.
        """
        hosts = list(self.cluster.owners)
        return dict(unix_ns=now_ns(),
                    seals={host: self.cluster.seal_markers(host) for host in hosts},
                    wal={host: self.cluster.generation_wal(host) for host in hosts})

    @staticmethod
    def owner_fence_proof(baseline, at_fence, after, *, authority_unix_ns,
                          fence_kind='generation'):
        """The owner-side half of exclusivity, or the named reason it is absent.

        Three snapshots are required: `baseline` before the new authority is
        launched (or, for a lapse cell, before the pause), `at_fence` after the
        owners published the fence, and `after` once the writers stopped.
        `baseline` is also the stale writer's generation set: a generation
        present before the launch cannot belong to the new authority.

        `fence_kind` names the engine fence, because the two publish different
        records:

        - `generation` (contend/stale): **every** owner creates exactly one new
          `epoch_*` directory and rewrites the inventory ledger. One owner's
          advance never stands in for the others. That record is unconditional;
          the sealed-marker count is corroboration only because
          `protect_live_targets` writes no marker when the fenced epoch has no
          writable live binding.
        - `write-protect` (lapse/sealed): lease expiry seals the current
          generation in place, on **every** owner. No new epoch exists and the
          ledger is unchanged; the new write-protect markers are the record.

        The snapshot must bracket `authority_unix_ns`: taken after the baseline
        and no earlier than the fence. The engine publishes the fence before a
        new authority can serve a write (`tcp/control.rs` activate-after-fence),
        so the bracket plus the order is the proof; no owner clock is compared
        to a coordinator clock.

        The stale set is the generations present in **both** snapshots, and the
        fence snapshot is the reference for size and mtime: a write that landed
        between the baseline and the fence was accepted before it, so only a
        change after the fence is growth. An empty set is no evidence, never
        "nothing grew". All three snapshots are retained verbatim.
        """
        proof = dict(evidence_available=False, fence_kind=fence_kind,
                     fence_counters_increased=None,
                     fence_hosts=[], fence_hosts_missing=[], scraped_unix_ns=None,
                     baseline_unix_ns=None, after_unix_ns=None,
                     new_epoch_dirs={}, new_markers={}, manifest_rewritten={},
                     sealed_before={}, sealed_at_fence={},
                     fence_owner_unix_ns=None, stale_generations=[],
                     stale_reference={}, stale_hosts_missing=[],
                     snapshots=None, stale_growth=None, no_stale_growth=None,
                     reason=None)
        if not baseline or not at_fence or not after:
            proof['reason'] = ('no complete owner-side fence snapshot set (baseline, '
                               'at the fence, after the run) was captured')
            return proof
        proof['baseline_unix_ns'] = baseline.get('unix_ns')
        proof['scraped_unix_ns'] = at_fence.get('unix_ns')
        proof['after_unix_ns'] = after.get('unix_ns')
        before_seals = baseline.get('seals') or {}
        at_seals = at_fence.get('seals') or {}
        if not before_seals or not at_seals:
            proof['reason'] = 'no owner seal marker was readable at either snapshot'
            return proof
        if (set(before_seals) != set(at_seals)
                or set(at_seals) != set(after.get('seals') or {})):
            proof['reason'] = ('the owner snapshots do not cover the same hosts, so '
                               'a partial fence could read as a full one')
            return proof
        before_ns, at_ns = baseline.get('unix_ns'), at_fence.get('unix_ns')
        if (before_ns is None or at_ns is None or after.get('unix_ns') is None
                or not before_ns < at_ns <= after.get('unix_ns')):
            proof['reason'] = ('the owner snapshots are not ordered '
                               'baseline < at the fence <= after')
            return proof
        if authority_unix_ns is not None and not (before_ns < authority_unix_ns <= at_ns):
            proof['reason'] = ('the owner fence snapshot does not bracket the fence '
                               'instant')
            return proof
        raised, owner_instants = [], []
        for host, at in at_seals.items():
            before = before_seals.get(host) or {}
            new = sorted(set(at.get('epoch_dirs') or {})
                         - set(before.get('epoch_dirs') or {}))
            proof['new_epoch_dirs'][host] = new
            before_markers = before.get('markers') or {}
            new_markers = sorted(set(at.get('markers') or {}) - set(before_markers))
            proof['new_markers'][host] = new_markers
            before_files = before.get('rootfiles') or {}
            at_files = at.get('rootfiles') or {}
            rewritten = any(Path(path).name == INVENTORY_MANIFEST
                            and before_files.get(path) != entry
                            for path, entry in at_files.items())
            proof['manifest_rewritten'][host] = rewritten
            proof['sealed_before'][host] = int(before.get('sealed') or 0)
            proof['sealed_at_fence'][host] = int(at.get('sealed') or 0)
            for path in new:
                owner_instants.append((at['epoch_dirs'][path].get('mtime'), path))
            for path in new_markers:
                owner_instants.append((at['markers'][path].get('mtime'), path))
            if fence_kind == 'write-protect':
                protected = bool(new_markers) or (
                    int(at.get('sealed') or 0) > int(before.get('sealed') or 0))
                if protected:
                    raised.append(host)
            elif len(new) == 1 and rewritten:
                raised.append(host)
        if owner_instants:
            instants = sorted(instant for instant, _ in owner_instants
                              if instant is not None)
            proof['fence_owner_unix_ns'] = instants[0] if instants else None
        proof['fence_hosts'] = raised
        proof['fence_hosts_missing'] = sorted(set(at_seals) - set(raised))
        # One owner's advance is not "the owners fenced": every owner must show
        # its own record, or a second owner that never fenced reads as covered.
        proof['fence_counters_increased'] = (bool(at_seals)
                                             and set(raised) == set(at_seals))

        # The stale generations exist in both snapshots; the fence snapshot is
        # the reference. A write that landed between the baseline and the fence
        # was accepted before the fence and is not post-fence growth.
        stale = {}
        for host, at_row in (at_fence.get('wal') or {}).items():
            base_row = (baseline.get('wal') or {}).get(host) or {}
            stale[host] = {
                generation: files
                for generation, files in (at_row.get('generations') or {}).items()
                if generation in (base_row.get('generations') or {})}
            proof['stale_generations'].extend(
                sorted(f'{host}:{generation}' for generation in stale[host]))
        proof['stale_generations'].sort()
        proof['stale_reference'] = {
            f'{host}:{generation}': files
            for host, generations in stale.items()
            for generation, files in generations.items()}
        missing, grown, new_files = [], [], []
        for host, generations in stale.items():
            later = ((after.get('wal') or {}).get(host) or {}).get('generations') or {}
            for generation, files in generations.items():
                current = later.get(generation)
                if current is None:
                    missing.append(generation)
                    continue
                for path, entry in files.items():
                    later_entry = current.get(path)
                    if later_entry is None:
                        missing.append(path)
                    elif (later_entry.get('size'), later_entry.get('mtime')) != (
                            entry.get('size'), entry.get('mtime')):
                        grown.append(path)
                for path in current:
                    if path not in files:
                        new_files.append(path)
        proof['stale_growth'] = dict(missing=missing, new_files=new_files,
                                     grown=grown)
        # An empty intersection is no evidence: every owner must hold at least
        # one generation that existed before the fence and at it, or a host
        # with nothing to compare must never read as "nothing grew".
        empty = sorted(host for host, generations in stale.items() if not generations)
        proof['stale_hosts_missing'] = empty
        proof['no_stale_growth'] = (bool(stale) and not empty
                                    and not (missing or new_files or grown))
        proof['snapshots'] = dict(
            baseline=dict(unix_ns=before_ns, seals=before_seals),
            at_fence=dict(unix_ns=at_ns, seals=at_seals),
            after=dict(unix_ns=after.get('unix_ns'),
                       seals=after.get('seals') or {}))
        proof['evidence_available'] = True
        if not proof['fence_counters_increased']:
            proof['reason'] = (
                'the owners published no new write-protect marker between the '
                'baseline and the fence'
                if fence_kind == 'write-protect' else
                'the owners published no new generation and no ledger rewrite '
                'between the baseline and the fence')
        elif not proof['no_stale_growth'] and proof['stale_hosts_missing']:
            proof['reason'] = ('no generation is present in both owner snapshots on '
                               + ', '.join(proof['stale_hosts_missing'])
                               + ', so stale growth cannot be ruled out')
        elif not stale:
            proof['reason'] = ('no owner listed a generation at the fence, so stale '
                               'growth cannot be ruled out')
        elif missing:
            proof['reason'] = ('the stale generation\'s WAL listing is incomplete after '
                               'the fence: ' + ', '.join(sorted(missing)[:3]))
        elif new_files or grown:
            proof['reason'] = ('the stale generation\'s WAL gained files or bytes after '
                               'the fence')
        return proof

    # ---- run records --------------------------------------------------
    def fence_audit(self, child):
        """A read-only integrity pass over A's root after the fence.

        kv-research's `verify_existing_dataset` reads every dataset key and
        compares the deterministic payload. That is an **integrity diagnostic
        only**: the payload is a function of the seed and the key, so a stale
        idempotent write reads back identically and a clean pass says nothing
        about whether the stale writer's frame was adopted. Exclusivity needs
        stale-generation adoption evidence, which this run cannot produce.

        The engine opens a database by fencing a new generation, so this is only
        meaningful while A's root still holds the current generation (`lapse`,
        `sealed`). A root the owners have out-ranked is refused at open; that is
        `fence_authority_refusal`, not an integrity pass.
        """
        audit = dict(audit_available=False, audit_read_ms=None,
                     audit_integrity_ok=None, audit_text=None,
                     audit_exit_code=None)
        run = mechanism.read_run(4_096, value=4_096, verify=True, deadline=300)
        source = Path(child.output).parent / 'coordinator'
        started = now_ns()
        try:
            reader = self.launch(run, 'fence-verify', path=source)
        except (OSError, ValueError) as error:
            audit['audit_text'] = f'the audit run did not open: {error}'
            return audit
        try:
            code = reader.wait(300)
            audit['audit_exit_code'] = code
            audit['audit_read_ms'] = (now_ns() - started) / 1e6
            try:
                manifest = json.loads(
                    (Path(reader.output) / 'manifest.json').read_text())
            except (OSError, ValueError) as error:
                # The engine refuses the root before the runner creates its
                # output directory, so the reason is in the run log, never in
                # the missing path.
                audit['audit_text'] = (
                    f'the audit run produced no manifest (exit {code}): {error}; '
                    f'log: {self._run_tail(reader) or "<empty>"}')
                return audit
            wrong = manifest.get('errors_or_wrong_values')
            audit['audit_integrity_ok'] = code == 0 and wrong == 0
            audit['audit_available'] = True
            audit['audit_text'] = (f'verify_existing_dataset exit {code}, '
                                   f'errors_or_wrong_values {wrong}')
        finally:
            reader.stop()
        return audit

    @staticmethod
    def _run_tail(reader, limit=800):
        """The last bytes of one run's log, for a failure that wrote no artifact."""
        log = getattr(reader, 'log', None)
        try:
            return Path(log).read_text(errors='replace')[-limit:].strip()
        except (AttributeError, OSError):
            return ''

    def fence_authority_refusal(self, child):
        """Prove the owners out-rank the stale writer's root.

        A's root records the generation it allocated. Once the new authority has
        fenced a higher generation, `remote_db.rs` checks the owners' high-water
        against that allocation (`fencing_generations::check_owner_status`) and
        refuses the open with `owner high-water N exceeds the allocated
        generation M`. That refusal is durable owner-side evidence that the
        fence preceded any reopen; a run that opens proves nothing and is a
        failure here, never a pass.
        """
        probe = dict(available=False, refused=False, opened=False,
                     exit_code=None, read_ms=None, text=None)
        run = mechanism.read_run(4_096, value=4_096, verify=True, deadline=300)
        source = Path(child.output).parent / 'coordinator'
        started = now_ns()
        try:
            reader = self.launch(run, 'fence-verify', path=source)
        except (OSError, ValueError) as error:
            probe['text'] = f'the authority probe did not open: {error}'
            return probe
        try:
            code = reader.wait(300)
            probe['exit_code'] = code
            probe['read_ms'] = (now_ns() - started) / 1e6
            text = self._run_tail(reader)
            probe['opened'] = (Path(reader.output) / 'manifest.json').is_file()
            probe['text'] = text or f'the authority probe wrote no log (exit {code})'
            probe['refused'] = (not probe['opened'] and code not in (0, None)
                                and FENCING_DISAGREEMENT in text
                                and HIGH_WATER in text)
            probe['available'] = bool(text)
        finally:
            reader.stop()
        return probe

    def keep_rows(self, child, name):
        """Copy a bounded sample excerpt of one run into the cell output.

        `failure_artifacts` walks the output tree, so an excerpt written here
        ships with the cell; a raw `samples.csv.tmp` above the upload bound is
        retained but unread by any verdict.
        """
        output = getattr(child, 'output', None)
        if output is None:
            return None
        for member in ('samples.csv', 'samples.csv.tmp'):
            source = Path(output) / member
            if not source.is_file():
                continue
            target = Path(output).parent.parent / name / 'samples-excerpt.csv'
            target.parent.mkdir(parents=True, exist_ok=True)
            excerpt = sample_excerpt(source)
            shutil.copyfile(excerpt, target)
            return target
        return None

    def write_gate(self, child, gate):
        """Persist one gate record beside the run, before any verdict.

        `failure_artifacts` walks every file under the attempt's output tree, so
        a gate written here reaches the bucket even when the cell never records
        a part.
        """
        path = Path(child.output).parent / 'gate.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + '.tmp')
        temporary.write_text(json.dumps(gate, indent=1, sort_keys=True) + '\n')
        os.replace(temporary, path)
        return path

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
        retirement = None
        try:
            if run.get('relocation'):
                retirement = self.await_owner_retirement(child)
                if not retirement.get('reached'):
                    raise CellError(f"retirement missed: {retirement.get('reason')}")
                child.release_completion()
            code = child.wait(run['deadline'] + 300)
            if code is None:
                raise CellError('measure run did not finish within its deadline')
            if code != 0:
                raise CellError(f'measure run exited {code}; see {child.log}')
        finally:
            if run.get('relocation'):
                child.release_completion()
            child.stop()
        after = self.cluster.disks()
        rolls = {host: self.cluster.rolls(host) for host in self.cluster.owners}
        self.parts.append(self.part('measure', run, child,
                                    dict(role='measure', owner_before=before,
                                         owner_after=after, owner_rolls=rolls,
                                         retirement=retirement)))
        return self.parts

    def crash(self):
        cut = self.cell['plan']['cut']
        run = self.cell['runs'][0]
        child = self.open(run, 'measure')
        record = dict(name='measure', role='measure', pid=child.process.pid,
                      started_unix_ns=child.started_unix_ns)
        try:
            if cut == 'publication':
                gate = self.gate_writes(child, GATE_WRITES)
            else:
                writes = self.gate_writes(child, GATE_WRITES)
                if writes.get('reached'):
                    gate = self.gate_snapshot(child)
                    gate['writes'] = writes
                else:
                    # The write gate is the record itself here; a self-reference
                    # would make the persisted record unserializable.
                    gate = writes
            record['gate'] = gate
            if not gate.get('reached'):
                # A missed gate aborts before any part is appended, and the
                # failure upload walks files: without this the probe series of
                # the very run that missed the cut is lost again.
                self.write_gate(child, gate)
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
                # A missed gate aborts before any part is appended, and the
                # failure upload walks files: without this the probe series of
                # the very run that missed the cut is lost again.
                self.write_gate(child, gate)
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
            # The relocation pass that retires the file starts after the
            # measurement, just before this run's completion gate. The gate is
            # held until the owner-side cut is observed, then released.
            gate = self.gate_retirement(host, cut, child=child)
            record['gate'] = gate
            if not gate.get('reached'):
                # A missed gate aborts before any part is appended, and the
                # failure upload walks files: without this the probe series of
                # the very run that missed the cut is lost again.
                self.write_gate(child, gate)
                raise CellError(f"gate missed: {gate.get('reason')}")
            # The kill lands after the measurement window, so no sample row
            # follows it: the failure evidence is the coordinator's own
            # terminal error. The log watermark is taken and the owner killed
            # while the completion gate still holds the run, so the run cannot
            # exit before the kill and only text appended after it counts.
            log_offset = self._log_offset(child)
            record['kill'] = self.kill_owner(host, f'retirement {cut}')
            child.release_completion()
            record['failure'] = self.stop_on_terminal_failure(
                child, partition=record['kill'].get('partition'),
                log_offset=log_offset)
        finally:
            child.release_completion()
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
        child = self.open(run, 'fence-a')
        record = dict(case=case, tier=self.cell['tier'], owners=len(self.cluster.owners),
                      started_unix_ns=now_ns())
        try:
            gate = self.gate_writes(child, GATE_WRITES)
            record['gate'] = gate
            if not gate.get('reached'):
                # A missed gate aborts before any part is appended, and the
                # failure upload walks files: without this the probe series of
                # the very run that missed the cut is lost again.
                self.write_gate(child, gate)
                raise CellError(f"gate missed: {gate.get('reason')}")
            if case == 'lapse':
                record.update(self._lapse(child))
            elif case in ('contend', 'stale'):
                record.update(self._contend(child, case))
            elif case == 'sealed':
                record.update(self._sealed(child))
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

    def _seal_wait(self, hosts, started, *, timeout=180):
        """Wait until **every** owner published its write-protect marker.

        A lease lapses per owner, and the fence is complete only when every
        owner has write-protected. `S1-5S-lapse-native` attempt 4 reported
        `databases: 1` because the old loop scraped `owners[0]` alone: a resume
        in that gap can renew with an owner whose watchdog has not fired yet and
        then write. The fence instant is the **last** owner's, so the cell never
        proves a property it did not test.
        """
        hosts = list(hosts)
        deadline = time.monotonic() + timeout
        seen = {}
        while True:
            for host in hosts:
                if host in seen:
                    continue
                markers = self.cluster.seal_markers(host)
                if markers['sealed'] >= 1:
                    seen[host] = ((now_ns() - started) / 1e6, markers)
            if len(seen) == len(hosts):
                last = max(seen.items(), key=lambda item: item[1][0])
                return dict(
                    sealed_partitions=sum(row['sealed'] for _, row in seen.values()),
                    databases=max(row['databases'] for _, row in seen.values()),
                    write_protect_apply_ms=last[1][0],
                    sealed_hosts={host: elapsed for host, (elapsed, _) in seen.items()},
                    write_protect_last_host=last[0])
            if time.monotonic() > deadline:
                pending = [host for host in hosts if host not in seen]
                raise CellError('owners published no sealed marker after the lease '
                                'lapse: ' + ', '.join(pending))
            time.sleep(0.2)

    def _refusal(self, rows_, *, phase='measurement'):
        """First non-success and last success of one phase, in completion order.

        `samples.csv` is written by one writer thread in completion order, so
        the row order is the operation order. A mutation that the engine
        refused records `unknown_mutation` because its outcome is unknown.
        """
        first = next((index for index, row in enumerate(rows_)
                      if row.get('phase') == phase
                      and row.get('outcome') != 'success'), None)
        last_success = max((index for index, row in enumerate(rows_)
                            if row.get('phase') == phase
                            and row.get('outcome') == 'success'), default=None)
        text = rows_[first].get('error_kind') if first is not None else None
        return dict(first=first, last_success=last_success, text=text,
                    typed=bool(text) and self.typed(text),
                    refused=first is not None,
                    no_success_after=(first is not None
                                      and (last_success is None
                                           or last_success < first)))

    @staticmethod
    def _row_wall_ns(child, row):
        """Wall clock of one completed sample: process start + completed_ns."""
        return CellDriver._refusal_wall_ns(child, row)

    @staticmethod
    def _measurement_rows(rows_):
        return [row for row in rows_ if row.get('phase') == 'measurement']

    @classmethod
    def _stale_after_fence(cls, child, rows_, fence_ns):
        """How many stale-writer writes completed after the fence.

        Returns `None` when the comparison cannot be made — no fence instant or
        rows without a completion time — so a missing proof is never read as
        zero. A success strictly after the fence is a write the stale
        coordinator completed under the new authority.
        """
        if fence_ns is None:
            return None
        measurement = cls._measurement_rows(rows_)
        if not measurement:
            return None
        walls = [cls._row_wall_ns(child, row) for row in measurement]
        if any(wall is None for wall in walls):
            return None
        return sum(1 for wall, row in zip(walls, measurement)
                   if row.get('outcome') == 'success' and wall > fence_ns)

    @classmethod
    def _last_success_ns(cls, child, rows_):
        walls = [cls._row_wall_ns(child, row) for row in cls._measurement_rows(rows_)
                 if row.get('outcome') == 'success']
        walls = [wall for wall in walls if wall is not None]
        return max(walls) if walls else None

    @staticmethod
    def _wall_origin_ns(child):
        """Unix nanoseconds of the runner's `t0`, the base of `completed_ns`.

        Samples count from the runner's monotonic origin, whose wall clock is
        the summary's `started_unix_seconds`; the runner writes no earlier
        artifact. The spawn time is not a substitute: it is earlier by the
        process start and backend setup, so a row timed from it lands before it
        happened and a success after a fence reads as one before it. A part
        without a summary (an interrupted or force-stopped run) therefore has
        no row clock, and the verdict reports the missing evidence.
        """
        cached = getattr(child, 'wall_origin_ns', None)
        if cached is not None:
            return cached or None
        origin = None
        try:
            summary = json.loads((Path(child.output) / 'summary.json').read_text())
            started = summary.get('started_unix_seconds')
            if started is not None:
                origin = int(round(float(started) * 1e9))
        except (AttributeError, OSError, TypeError, ValueError):
            origin = None
        # 0 marks "no origin", so the next call does not re-read the directory.
        child.wall_origin_ns = origin or 0
        return origin

    @staticmethod
    def _refusal_wall_ns(child, row):
        """Wall time of a completed sample: runner origin plus completed_ns."""
        started = CellDriver._wall_origin_ns(child)
        try:
            completed = int(row.get('completed_ns', ''))
        except (TypeError, ValueError):
            return None
        return None if started is None else started + completed

    def _log_refusal(self, child, offset=None):
        """A typed causal line appended after `offset`, if any.

        The log is the fallback when a refusal row does not carry the typed
        class. Only bytes written **after** the stale writer was proven stopped
        may describe this fence: scanning the whole log would let a pre-freeze
        refusal, followed by unrelated post-release output, pass as this cell's
        self-fence. Without the frozen offset the line is not attributable and
        nothing is returned.
        """
        if offset is None:
            return None
        text = self._log_appended(child, offset)
        for line in text.splitlines():
            if 'causal failure' in line and self.typed(line):
                return line.strip()
        return None

    def _lapse(self, child):
        result = dict(sealed_partitions=0)
        baseline = self.fence_owner_snapshot()
        freeze = child.freeze()
        started = freeze['unix_ns']
        result['paused_unix_ns'] = started
        result['freeze'] = freeze
        result['log_offset_at_freeze'] = self._log_offset(child)
        result.update(self._seal_wait(self.cluster.owners, started))
        at_fence = self.fence_owner_snapshot()
        result['fence_owner_unix_ns'] = self._marker_instant(at_fence, baseline)
        release = child.resume()
        result['resumed_unix_ns'] = release['released_unix_ns']
        result['release_returned_unix_ns'] = release['release_returned_unix_ns']
        result['pause_ms'] = (release['released_unix_ns'] - started) / 1e6
        result['failure'] = self.stop_on_failure(child, count=1, timeout=180)
        result['a_exit'] = result['failure'].get('exit_code')
        after = self.fence_owner_snapshot()
        rows_ = child.rows()
        refusal = self._refusal(rows_)
        text, typed = refusal['text'], refusal['typed']
        if not typed and (logged := self._log_refusal(
                child, result.get('log_offset_at_freeze'))):
            text, typed = logged, True
            result['refusal_source'] = 'coordinator log'
        result['first_refusal_row'] = refusal['first']
        result['last_success_row'] = refusal['last_success']
        result['refusal_text'] = text
        result['typed_refusal'] = typed
        result['stale_write_refused'] = refusal['refused']
        result['no_success_after_refusal'] = refusal['no_success_after']
        # The fence of a lapse cell is the owner's write-protect, applied while
        # the coordinator was paused: after it, the sealed generation may not be
        # written by that coordinator again.
        result['fence_unix_ns'] = (started + int(result['write_protect_apply_ms'] * 1e6)
                                   if result.get('write_protect_apply_ms') is not None
                                   else None)
        result['first_started_unix_ns'] = getattr(child, 'started_unix_ns', None)
        result['stale_success_after_fence'] = self._stale_after_fence(
            child, rows_, result['fence_unix_ns'])
        refusal_kind, wall = self._refusal_kind(
            child, rows_, refusal, result.get('refusal_source'))
        result['refusal_kind'] = refusal_kind
        result['refusal_unix_ns'] = wall
        # The row-level verdict; `fence_record` adds the audit and is the one
        # that publishes `exclusive`/`writers`. The owner proof comes first: a
        # self-fence has no row, so its row proof is the owner proof.
        result['owner_fence'] = self.owner_fence_proof(
            baseline, at_fence, after, authority_unix_ns=result.get('fence_unix_ns'),
            fence_kind='write-protect')
        result['stale_rows'] = self._stale_row_proof(
            rows_, refusal, refusal_kind=refusal_kind,
            stale_after=result['stale_success_after_fence'],
            owner_fence=result['owner_fence'])
        result['stale_rows_clean'] = result['stale_rows']['clean']
        latency_kind, latency_reason = self._latency_kind(refusal_kind, wall)
        result['fence_reject_kind'] = latency_kind
        result['fence_reject_ms'] = (
            (wall - result['fence_unix_ns']) / 1e6
            if latency_kind == 'row' and result['fence_unix_ns'] is not None
            else None)
        result['fence_reject_window_ms'] = None
        result['fence_reject_reason'] = latency_reason
        if latency_kind == 'self-fence':
            result['self_fence_log_unix_ns'] = self._log_mtime_ns(child)
            result['fence_reject_window_ms'], result['fence_reject_reason'] = (
                self._self_fence_window(
                    result['fence_unix_ns'], resumed_ns=result.get('resumed_unix_ns'),
                    log_ns=result['self_fence_log_unix_ns'],
                    baseline_ns=(baseline or {}).get('unix_ns')))
        elif latency_kind == 'row' and result['fence_unix_ns'] is None:
            result['fence_reject_reason'] = 'the fence instant is missing'
        result.update(self.fence_audit(child))
        result['stale_rows_exclusive'], result['stale_rows_reason'] = self._exclusive(
            refusal_kind, result['stale_success_after_fence'],
            new_authority=bool(result.get('sealed_partitions')),
            owner_fence=result['owner_fence'],
            rows_clean=result['stale_rows_clean'],
            rows_reason=result['stale_rows'].get('reason'))
        return result

    @staticmethod
    def _marker_instant(at_fence, baseline):
        """Earliest owner-clock write-protect/seal instant new since the baseline."""
        before = {host: (row.get('markers') or {})
                  for host, row in ((baseline or {}).get('seals') or {}).items()}
        instants = []
        for host, row in ((at_fence or {}).get('seals') or {}).items():
            known = before.get(host) or {}
            for path, entry in (row.get('markers') or {}).items():
                if path not in known and entry.get('mtime') is not None:
                    instants.append(entry['mtime'])
        return min(instants) if instants else None

    @classmethod
    def _stale_row_proof(cls, rows_, refusal, *, refusal_kind, stale_after,
                         owner_fence):
        """The writer-side half, chosen by the refusal kind.

        An `owner-row` refusal is a sample row: every acknowledged success must
        precede it in completion order, and a cell with no acknowledged write
        proves nothing. A `coordinator-self-fence` is emitted before the frame
        is encoded, so it has no row and no order to read: its proof is the
        owner side (a new generation and a rewritten ledger on every owner, no
        growth of the stale generation), with acknowledged rows > 0 and the
        stale writer's row clock present. Successes that complete after the
        fence instant are reported, not convicted, as in the owner-row path:
        a frame sent before the freeze can be acknowledged after the resume,
        and any write the owner accepted after the fence grows the stale
        generation, which the owner proof convicts. Every missing part fails
        closed and is named.
        """
        acknowledged_rows = sum(
            1 for row in cls._measurement_rows(rows_) if row.get('outcome') == 'success')
        proof = dict(kind=refusal_kind, acknowledged=acknowledged_rows,
                     first_refusal_row=refusal.get('first'),
                     last_success_row=refusal.get('last_success'),
                     no_success_after_refusal=bool(refusal.get('no_success_after')),
                     stale_success_after_fence=stale_after, clean=False)
        if refusal_kind == 'owner-row':
            if refusal.get('refused') is None:
                proof['reason'] = 'the stale writer recorded no refusal row'
            elif acknowledged_rows == 0:
                proof['reason'] = ('the stale writer recorded no acknowledged write '
                                   'before its refusal')
            elif not refusal.get('no_success_after'):
                proof['reason'] = ('a stale-writer write succeeded after its first '
                                   'refusal')
            else:
                proof['clean'] = True
            return proof
        if refusal_kind != 'coordinator-self-fence':
            proof['reason'] = ('the stale writer recorded no typed refusal row and the '
                               'coordinator log carries no typed causal line')
            return proof
        owner = owner_fence or {}
        if acknowledged_rows == 0:
            proof['reason'] = 'the stale writer recorded no acknowledged write'
        elif stale_after is None:
            proof['reason'] = ('the stale writer\'s row clock is missing, so no success '
                               'after the fence instant can be ruled out')
        else:
            if stale_after:
                proof['completions_after_fence_reported'] = stale_after
            proof.update(cls._self_fence_owner_verdict(owner))
        return proof

    @staticmethod
    def _self_fence_owner_verdict(owner):
        """The owner-side half of a self-fence proof, failing closed."""
        if not owner.get('evidence_available'):
            return dict(reason='the self-fence has no owner-side evidence: no new '
                               'generation or write-protect marker was captured')
        if not owner.get('fence_counters_increased'):
            return dict(reason=owner.get('reason')
                        or 'the owners published no fence before the self-fence')
        if not owner.get('no_stale_growth'):
            return dict(reason=owner.get('reason')
                        or 'the stale generation\'s WAL gained files or bytes')
        if owner.get('reason'):
            return dict(reason=owner['reason'])
        return dict(clean=True)

    @classmethod
    def _refusal_kind(cls, child, rows_, refusal, source):
        """The stale writer's typed refusal, and its wall clock when known.

        Either an owner-side refusal row (`owner-row`) or the coordinator's own
        typed lease-lapse self-fence (`coordinator-self-fence`). A self-fence is
        emitted before the frame is encoded, so it has no sample row and no
        timestamp of its own.
        """
        if refusal['refused']:
            return 'owner-row', cls._refusal_wall_ns(child, rows_[refusal['first']])
        if refusal['typed'] or source == 'coordinator log':
            return 'coordinator-self-fence', None
        return None, None

    @staticmethod
    def _log_mtime_ns(child):
        """The final log write: the last instant the stale writer could run."""
        try:
            return Path(child.log).stat().st_mtime_ns
        except (AttributeError, OSError):
            return None

    @staticmethod
    def _self_fence_window(fence_ns, *, resumed_ns, log_ns, baseline_ns=None):
        """Upper-bound fence -> refusal for a self-fence, which has no sample row.

        The typed causal line is printed by the stale writer's final log write,
        after the release. That line dates the cause's report, not the cause:
        the engine caches its first cause and appends it to later errors, so
        the cause may have latched earlier. The bracket is therefore only an
        upper bound on the refusal (`report_bound`), with no lower bound. A log
        mtime before the release proves nothing and is rejected: a stale or
        truncated log must never read as a post-release report.

        The fence instant is bracketed by the owner snapshots: `(baseline_ns,
        fence_ns]`, where `fence_ns` is the driver's first observation of the
        fence (an upper bound on it) and `baseline_ns` the snapshot taken
        before the freeze. The bound uses `baseline_ns` so it contains the true
        interval; it spans the harness pause (`includes_pause`); the
        post-release part is kept separately (`post_release_upper_ms`).
        """
        if (fence_ns is None or resumed_ns is None or log_ns is None
                or baseline_ns is None):
            missing = [name for name, value in (('fence', fence_ns),
                                                ('release', resumed_ns),
                                                ('log write', log_ns),
                                                ('pre-freeze baseline', baseline_ns))
                       if value is None]
            return None, ('the self-fence instant is not measurable: '
                          + ', '.join(missing) + ' is missing')
        if log_ns < resumed_ns:
            return None, ('the final log write precedes the release, so it cannot '
                          'contain a post-release report')
        upper = (log_ns - baseline_ns) / 1e6
        if resumed_ns < fence_ns or upper < 0:
            return None, ('the self-fence bracket is inconsistent: the release or the '
                          'final log write precedes the fence')
        return dict(lower_ms=None, upper_ms=upper, report_bound=True,
                    includes_pause=True,
                    post_release_upper_ms=(log_ns - resumed_ns) / 1e6,
                    basis=('fence to the stale writer\'s cause report (final log '
                           'write), an upper bound on the refusal; the cause may '
                           'have latched earlier; spans the harness pause')), None

    @staticmethod
    def _latency_kind(refusal_kind, wall):
        """Which latency source is valid for this refusal, or why none is.

        A self-fence has no row, so only its release-to-log bracket applies. An
        owner-row refusal needs its own row clock; a missing clock is not a
        reason to fall back to the self-fence bracket.
        """
        if refusal_kind == 'coordinator-self-fence':
            return 'self-fence', None
        if refusal_kind == 'owner-row' and wall is not None:
            return 'row', None
        if refusal_kind is None:
            return None, ('no typed refusal, so no fence instant is defined')
        return None, ('the refusal row has no wall clock (no summary.json), so the '
                      'fence-reject time is not measurable')

    @staticmethod
    def _exclusive(refusal_kind, stale_after, *, new_authority, owner_fence=None,
                   rows_clean=None, rows_reason=None):
        """Whether exclusive writes are proven, and the first missing proof.

        `rows_clean` is the writer-side half, chosen by the kind: the completion
        order for an owner-row refusal, or the owner proof plus no post-fence
        success for a self-fence. `stale_after` is reported but not convicted: a
        row that *completed* after the fence instant can be a write the owner
        accepted before it and acknowledged after the frozen writer resumed.
        The accept-time evidence is the owner side: nothing landed in the stale
        generation after the write-protect, which the kernel enforces before the
        marker is written. `owner_fence` is that half.
        A deterministic-payload readback is not this evidence.
        """
        if refusal_kind is None:
            return False, 'the stale writer recorded no typed refusal (row or self-fence)'
        if not new_authority:
            return False, 'the new authority recorded no progress (no new writer, no seal)'
        if rows_clean is None:
            return False, ('the stale writer\'s write-side proof is missing '
                           '(no acknowledged write, refusal row or owner evidence)'
                           + (f'; the wall clock shows {stale_after} completion(s) '
                              'after the fence' if stale_after else ''))
        if rows_clean is not True:
            return False, (rows_reason
                           or 'the stale writer\'s write-side proof fails')
        if not owner_fence or not owner_fence.get('evidence_available'):
            return False, ('no owner-side fence evidence: no seal or write-protect marker '
                           'and no per-generation WAL listing was captured')
        if not owner_fence.get('fence_counters_increased'):
            return False, (owner_fence.get('reason')
                           or 'the owners published no fence marker before the new writer')
        if not owner_fence.get('no_stale_growth'):
            return False, (owner_fence.get('reason')
                           or 'the stale generation\'s WAL gained files or bytes')
        if owner_fence.get('reason'):
            return False, owner_fence['reason']
        return True, None

    @staticmethod
    def _contend_window(fence_ns, baseline_ns, refusal_ns, resumed_ns, pause_ms):
        """Bound fence -> observed refusal on the driver's own clock.

        `baseline` precedes the launch, so the fence is strictly after it; the
        `at_fence` snapshot is taken after the new authority's accepted writes,
        so the fence is strictly before it. Hence the fence instant lies in
        `(baseline, at_fence]` and the refusal bounds are computed from both
        ends. The window contains the controlled pause: it bounds the latency,
        it does not measure the engine's detection.
        """
        if None in (fence_ns, baseline_ns, refusal_ns, resumed_ns):
            return None, ('the fence, baseline or refusal instant is missing, so no '
                          'fence-to-refusal bound exists')
        if refusal_ns < resumed_ns:
            return None, ('the refusal row predates the release instant: the row '
                          'clock and the freeze bracket disagree')
        return dict(
            lower_ms=(refusal_ns - fence_ns) / 1e6,
            upper_ms=(refusal_ns - baseline_ns) / 1e6,
            resume_ms=(refusal_ns - resumed_ns) / 1e6,
            pause_ms=pause_ms,
            includes_pause=True), None

    def _contend(self, child, case):
        """Freeze A (proven), copy its root, start B on the copy, release A.

        `samples.csv` is written by one writer thread in completion order, so the
        row order is the operation order: every success of A must precede its
        first refusal. That is the "a stale writer cannot change data" check and
        it needs no timestamp from A's own clock.

        The owner snapshots bracket the fence: `baseline` is read after the
        proven freeze and before B is launched, `at_fence` after B has accepted
        writes while A is still frozen. The fence instant used for A's rows is
        the `at_fence` stamp, so the comparison never needs a clock from a
        process that is killed before it writes its summary.
        """
        freeze = child.freeze()
        # Only bytes appended after this point can describe this fence: the
        # writer is proven stopped, so the offset is a stable watermark.
        log_offset = self._log_offset(child)
        source = Path(child.output).parent / 'coordinator'
        copy = self.cluster.root('fence-b')
        if copy.exists():
            shutil.rmtree(copy)
        shutil.copytree(source, copy)
        run = self.cell['runs'][0]
        # Read before the launch: B fences at open, so a baseline taken after
        # the launch may already contain B's generation and ledger rewrite.
        baseline = self.fence_owner_snapshot()
        result = dict(copied_root=str(copy), freeze=freeze,
                      paused_unix_ns=freeze['unix_ns'],
                      log_offset_at_freeze=log_offset)
        second, at_fence, after = None, None, None
        try:
            second = self.launch(run, 'fence-b', path=copy)
            gate = self.gate_writes(second, FENCE_WRITES, timeout=300)
            result['second_gate'] = gate
            if not gate.get('reached'):
                raise CellError(f"second coordinator wrote nothing: {gate.get('reason')}")
            # At the fence, read while the stale writer is still stopped and
            # before it may run again.
            at_fence = self.fence_owner_snapshot()
            release = child.resume()
            result['resumed_unix_ns'] = release['released_unix_ns']
            result['release_returned_unix_ns'] = release['release_returned_unix_ns']
            result['pause_ms'] = (release['released_unix_ns'] - freeze['unix_ns']) / 1e6
            result['failure'] = self.stop_on_failure(child, count=1)
        finally:
            try:
                if second is not None:
                    second.stop()
                    # The new writer's own rows are the fence's evidence; the
                    # failure upload keeps 30 MiB of samples.csv.tmp that no
                    # verdict reads.
                    self.keep_rows(second, 'fence-b')
            finally:
                after = self.fence_owner_snapshot()
        first_rows = child.rows()
        refusal = self._refusal(first_rows)
        text, typed = refusal['text'], refusal['typed']
        if not typed and (logged := self._log_refusal(
                child, result.get('log_offset_at_freeze'))):
            text, typed = logged, True
            result['refusal_source'] = 'coordinator log'
        result['first_refusal_row'] = refusal['first']
        result['last_success_row'] = refusal['last_success']
        result['first_acknowledged'] = acknowledged(first_rows)
        result['stale_write_refused'] = refusal['refused']
        result['no_success_after_refusal'] = refusal['no_success_after']
        result['refusal_text'] = text
        result['typed_refusal'] = typed
        second_rows = second.rows() if second is not None else []
        result['second_acknowledged'] = acknowledged(second_rows)
        result['first_started_unix_ns'] = getattr(child, 'started_unix_ns', None)
        result['second_started_unix_ns'] = getattr(second, 'started_unix_ns', None)
        result['second_measurement_rows'] = len(self._measurement_rows(second_rows))
        result['second_last_success_unix_ns'] = self._last_success_ns(second, second_rows)
        result['fence_unix_ns'] = (at_fence or {}).get('unix_ns')
        result['fence_owner_unix_ns'] = self._marker_instant(at_fence, baseline)
        result['stale_success_after_fence'] = self._stale_after_fence(
            child, first_rows, result['fence_unix_ns'])
        result['root_refused'] = self.fence_authority_refusal(child)
        refusal_kind, wall = self._refusal_kind(
            child, first_rows, refusal, result.get('refusal_source'))
        result['refusal_kind'] = refusal_kind
        result['refusal_unix_ns'] = wall
        result['resume_reject_ms'] = (
            None if wall is None or result.get('resumed_unix_ns') is None
            else (wall - result['resumed_unix_ns']) / 1e6)
        # The row-level verdict; `fence_record` adds the owner-side proof and is
        # the one that publishes `exclusive`/`writers`. The owner proof comes
        # first: a self-fence has no row, so its row proof is the owner proof.
        result['owner_fence'] = self.owner_fence_proof(
            baseline, at_fence, after, authority_unix_ns=result.get('fence_unix_ns'))
        result['stale_rows'] = self._stale_row_proof(
            first_rows, refusal, refusal_kind=refusal_kind,
            stale_after=result['stale_success_after_fence'],
            owner_fence=result['owner_fence'])
        result['stale_rows_clean'] = result['stale_rows']['clean']
        latency_kind, latency_reason = self._latency_kind(refusal_kind, wall)
        result['fence_reject_kind'] = latency_kind
        result['fence_reject_ms'] = None
        result['fence_reject_window_ms'] = None
        result['fence_reject_reason'] = latency_reason
        if latency_kind == 'row':
            result['fence_reject_window_ms'], result['fence_reject_reason'] = (
                self._contend_window(
                    result['fence_unix_ns'], (baseline or {}).get('unix_ns'), wall,
                    result.get('resumed_unix_ns'), result.get('pause_ms')))
        elif latency_kind == 'self-fence':
            result['self_fence_log_unix_ns'] = self._log_mtime_ns(child)
            result['fence_reject_window_ms'], result['fence_reject_reason'] = (
                self._self_fence_window(
                    result['fence_unix_ns'], resumed_ns=result.get('resumed_unix_ns'),
                    log_ns=result['self_fence_log_unix_ns'],
                    baseline_ns=(baseline or {}).get('unix_ns')))
        result['stale_rows_exclusive'], result['stale_rows_reason'] = self._exclusive(
            refusal_kind, result['stale_success_after_fence'],
            new_authority=result['second_measurement_rows'] > 0,
            owner_fence=result['owner_fence'],
            rows_clean=result['stale_rows_clean'],
            rows_reason=result['stale_rows'].get('reason'))
        return result

    def _sealed(self, child):
        result = self._lapse(child)
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
        result['sealed_write_refused'] = self._sealed_write_refused(result)
        result['sealed_write_text'] = result.get('refusal_text')
        return result

    @staticmethod
    def _sealed_write_refused(result):
        """A sealed generation refused the stale writer: an owner refusal row,
        or a typed coordinator self-fence, which refuses before encoding and so
        leaves no row."""
        return bool(result.get('stale_write_refused')
                    or result.get('refusal_kind') == 'coordinator-self-fence')

    @staticmethod
    def typed(text):
        lowered = text.lower()
        return any(token in lowered for token in
                   ('lease', 'fenc', 'authority', 'write protect', 'write-protect',
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
    records.extend(dict(row, part='cleanup', process='configfs')
                   for row in getattr(driver.cluster, 'cleanups', []))
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
        if row.get('process') == 'configfs':
            checks.append(mechanism._check(label + ' removed every owned object',
                                           row.get('complete') is True, row))
            continue
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
        if cell['family'] == 'S1-6':
            # The retirement kill lands after the measurement window, so no
            # sample row follows it. The typed failure is the coordinator's own
            # terminal error, appended to run.log after the kill watermark.
            text = failure.get('text') or ''
            checks.append(mechanism._check(
                'coordinator recorded a typed write failure',
                (failure.get('typed') is True and bool(text)
                 and failure.get('exit_code') not in (0, None)),
                dict(exit_code=failure.get('exit_code'), text=text,
                     partition=failure.get('partition'),
                     log_offset=failure.get('log_offset'),
                     unix_ns=failure.get('unix_ns'))))
        else:
            found = failure.get('gate') or {}
            # `wait_for` returns the reached observation inside `observations`;
            # the gate itself carries no `refusals`/`text`. The refusal must name
            # an error class: a bare non-success row is not a typed failure.
            last = (found.get('observations') or [{}])[-1]
            text = last.get('text') or failure.get('refusal')
            checks.append(mechanism._check('coordinator recorded a typed write failure',
                                           found.get('reached') is True and bool(text),
                                           dict(refusals=last.get('refusals'),
                                                text=text, unix_ns=last.get('unix_ns'))))
    return checks


def fault_cells_exclusive(fence):
    """The fence record's final exclusivity, including the owner-side proof."""
    return CellDriver._exclusive(
        fence.get('refusal_kind'), fence.get('stale_success_after_fence'),
        new_authority=((fence.get('second_measurement_rows') or 0) > 0
                       if fence.get('case') in ('contend', 'stale')
                       else (fence.get('sealed_partitions') or 0) >= 1),
        owner_fence=fence.get('owner_fence'),
        rows_clean=fence.get('stale_rows_clean'),
        rows_reason=(fence.get('stale_rows') or {}).get('reason'))


def fence_record(driver):
    """The M8/S1-5 verdict: the stale writer cannot write under the new authority.

    The plan's property is "a stale coordinator cannot write after the new
    coordinator's fence". A refusal is accepted as an owner-side typed refusal
    row or as the coordinator's own typed lease-lapse self-fence
    (`coordinator_lease.rs` emits it before frame encoding). It only counts with
    exclusivity: the stale writer's writes all precede its first refusal in
    completion order (the clock-free proof), no success follows the fence on the
    corroborating wall clock, the new authority proceeded, the owners published
    their fence before it could write, and the stale generation's WAL gained
    nothing. A missing proof is a named failure, never a pass.
    """
    fence = driver.fence
    case = fence['case']
    audit_parts = [part for part in getattr(driver, 'parts', [])
                   if part.get('role') == 'verify']
    if case in ('lapse', 'sealed'):
        fence.setdefault('audit_available', bool(audit_parts))
        if not audit_parts:
            fence.setdefault('audit_note',
                             'the fence audit is recorded in the fence record itself, '
                             'not as a verdict part')
    fence.setdefault('owner_fence', None)
    fence['exclusive'], fence['exclusivity_reason'] = fault_cells_exclusive(fence)
    fence['writers'] = 1 if fence['exclusive'] else 2
    stale_after = fence.get('stale_success_after_fence')
    rows_clean = fence.get('stale_rows_clean')
    stale_rows = fence.get('stale_rows') or {}
    row_reason = stale_rows.get('reason')
    checks = [
        mechanism._check('the stale writer was refused (owner row or coordinator self-fence)',
                         fence.get('refusal_kind') is not None,
                         dict(kind=fence.get('refusal_kind'),
                              text=fence.get('refusal_text'),
                              first_refusal_row=fence.get('first_refusal_row'))),
        mechanism._check('the new authority proceeded',
                         (fence.get('second_measurement_rows') or 0) > 0
                         if case in ('contend', 'stale')
                         else (fence.get('sealed_partitions') or 0) >= 1,
                         dict(second_measurement_rows=fence.get('second_measurement_rows'),
                              sealed_partitions=fence.get('sealed_partitions'))),
        mechanism._check('no stale-writer write completed after the fence',
                         rows_clean is True,
                         dict(stale_rows_clean=rows_clean,
                              stale_rows=stale_rows or None,
                              stale_success_after_fence=stale_after,
                              fence_unix_ns=fence.get('fence_unix_ns'),
                              fence_owner_unix_ns=fence.get('fence_owner_unix_ns'),
                              resumed_unix_ns=fence.get('resumed_unix_ns'),
                              resume_reject_ms=fence.get('resume_reject_ms'),
                              note=('completions after the fence can be writes the '
                                    'owner accepted before it and acknowledged after '
                                    'the release; the sealed generation\'s growth is '
                                    'the accept-time evidence'),
                              reason=(row_reason or fence.get('exclusivity_reason')))),
        mechanism._check('exclusive writes are proven', fence.get('exclusive') is True,
                         dict(reason=fence.get('exclusivity_reason'),
                              refusal_kind=fence.get('refusal_kind'),
                              stale_rows_clean=rows_clean,
                              stale_success_after_fence=stale_after,
                              second_measurement_rows=fence.get('second_measurement_rows'),
                              sealed_partitions=fence.get('sealed_partitions'))),
    ]
    if case in ('contend', 'stale'):
        probe = fence.get('root_refused') or {}
        checks.append(mechanism._check(
            'the stale writer\'s root was refused as an authority after the fence',
            probe.get('refused') is True,
            dict(refused=probe.get('refused'),
                 opened=probe.get('opened'),
                 exit_code=probe.get('exit_code'),
                 read_ms=probe.get('read_ms'),
                 text=probe.get('text'),
                 scope='the owners out-rank the stale root; not an integrity readback')))
    else:
        checks.append(mechanism._check(
            'the fence audit ran over the stale writer\'s root (integrity only)',
            fence.get('audit_available') is True,
            dict(available=fence.get('audit_available'),
                 integrity_ok=fence.get('audit_integrity_ok'),
                 text=fence.get('audit_text'),
                 scope='deterministic-payload readback; not exclusivity evidence',
                 note=fence.get('audit_note'))))
    owner = fence.get('owner_fence') or {}
    if not owner:
        owner = dict(reason='no owner-side fence snapshot was taken',
                     evidence_available=None)
    checks.append(mechanism._check(
        'the owners fenced before the new authority\'s first accepted write',
        owner.get('evidence_available') is True
        and owner.get('fence_counters_increased') is True,
        dict(available=owner.get('evidence_available'),
             counters_increased=owner.get('fence_counters_increased'),
             fence_hosts=owner.get('fence_hosts'),
             fence_hosts_missing=owner.get('fence_hosts_missing'),
             new_epoch_dirs=owner.get('new_epoch_dirs'),
             manifest_rewritten=owner.get('manifest_rewritten'),
             sealed_before=owner.get('sealed_before'),
             sealed_at_fence=owner.get('sealed_at_fence'),
             baseline_unix_ns=owner.get('baseline_unix_ns'),
             scraped_unix_ns=owner.get('scraped_unix_ns'),
             fence_unix_ns=fence.get('fence_unix_ns'),
             fence_owner_unix_ns=owner.get('fence_owner_unix_ns'),
             reason=owner.get('reason'))))
    checks.append(mechanism._check(
        'the stale generation gained nothing after the fence',
        owner.get('evidence_available') is True
        and owner.get('no_stale_growth') is True,
        dict(available=owner.get('evidence_available'),
             generations=owner.get('stale_generations'),
             growth=owner.get('stale_growth'),
             hosts_missing=owner.get('stale_hosts_missing'),
             reason=owner.get('reason'))))
    if case == 'sealed':
        checks.append(mechanism._check('sealed generation is readable',
                                       fence.get('sealed_read_ok') is True,
                                       fence.get('sealed_read_ms')))
        checks.append(mechanism._check('sealed generation is not writable',
                                       fence.get('sealed_write_refused') is True,
                                       fence.get('sealed_write_text')))
    if case in ('contend', 'lapse', 'stale', 'sealed'):
        window = fence.get('fence_reject_window_ms')
        checks.append(mechanism._check(
            'fence latency measured or bounded',
            fence.get('fence_reject_ms') is not None or window is not None,
            dict(kind=fence.get('refusal_kind'),
                 source=fence.get('fence_reject_kind'),
                 exact_ms=fence.get('fence_reject_ms'),
                 window=window,
                 resume_reject_ms=fence.get('resume_reject_ms'),
                 pause_ms=fence.get('pause_ms'),
                 fence_owner_unix_ns=fence.get('fence_owner_unix_ns'),
                 self_fence_log_unix_ns=fence.get('self_fence_log_unix_ns'),
                 reason=fence.get('fence_reject_reason'))))
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


def sample_excerpt(path, *, limit=SAMPLE_EXCERPT_ROWS):
    """Bounded rows of an oversized samples.csv: head, refusals and tail.

    A fence run records one non-success row per refused mutation, so its
    samples.csv passes the failure upload bound. The verdict's evidence is the
    first refusal and the success rows around it, so an excerpt carries the
    header, the first successes, the first refusals and the last rows.
    """
    path = Path(path)
    excerpt = path.with_name(path.stem + '.excerpt.csv')
    head, refusals, tail = [], [], deque(maxlen=limit)
    with path.open(errors='replace') as stream:
        header = stream.readline()
        for line in stream:
            fields = line.split(',')
            outcome = fields[2] if len(fields) > 2 else ''
            if outcome != 'success':
                if len(refusals) < limit:
                    refusals.append(line)
            elif len(head) < limit:
                head.append(line)
            tail.append(line)
    excerpt.write_text(header + ''.join(head) + ''.join(refusals) + ''.join(tail))
    return excerpt


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
                if path.name == 'samples.csv':
                    # The refusal rows are the verdict's evidence, so a run
                    # above the upload bound still ships a bounded excerpt:
                    # header, first rows, every early non-success row, last rows.
                    excerpt = sample_excerpt(path)
                    name = label + excerpt.relative_to(root).as_posix()
                    if name not in seen:
                        seen.add(name)
                        rows.append(campaign_upload.artifact(excerpt, name))
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
    add(output / 'owner-stops.json', 'owner-stops.json')
    add(output / 'owner-cleanup.json', 'owner-cleanup.json')
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
    parser.add_argument('--guest-root', type=Path,
                        help='absolute owner storage root, identical on both owners; '
                             'default: <data root>/guest on the run local SSD')
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
    if args.guest_root is None and args.fleet_state is not None:
        args.guest_root = mailbox_exec.data_root(args) / 'guest'
    if not args.dry_run:
        mailbox_exec.admit_data_roots(args, [('--guest-root', args.guest_root)])
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
            data_paths=[args.guest_root],
            values={'--build': 'build', '--fixture': 'fixture', '--schema': 'schema',
                    '--output': Path('out') / args.output.name,
                    '--config-dir': 'external', '--ledger': 'campaign-ledger.json'})
    # The output and config trees this cell writes on its own host must sit on
    # the run's SSD; a retry adds only a suffix under the same parents.
    mailbox_exec.admit_data_roots(args, [('--output', output),
                                         ('--config-dir', config_dir)])
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
                   live_transport=args.live_transport, owners=hosts['owners'],
                   data_device=gcp_runtime.DATA_DEVICE),
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
