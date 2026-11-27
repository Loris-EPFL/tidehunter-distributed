#!/usr/bin/env python3
"""Three-host Sui replay controller: one coordinator and two NVMe-oF owners.

The controller runs on the coordinator host. It prepares the external
deployment with the frozen `prepare_external.py`, installs each owner archive on
its host, starts the owner process groups, runs the paired `run.py` with
`--external-config/--external-resources/--external-stage-dir`, answers the
`resources-required` handshake with sampled `/proc` evidence, and stops only the
groups it started.

Remote commands go through `host_exec.HostExecution`, the one seam shared with
`stage3_campaign.py` and `fault_cells.py`. The campaign executor is
`mailbox_exec.MailboxExec`: one immutable bucket request per host command,
because the guests have no external address and IAP is not enabled. The
orchestrator itself runs on the coordinator host as one bounded request
(`--on-coordinator`), because `run.py` and the Sui nodes it starts must reach
the owners; the workstation only stages the build, fixture and schema through
the bucket and relays the terminal receipt. `host_exec.GcloudSsh` remains only
for a rehearsal that passes `--gcloud-ssh`. Every owner archive is extracted
into the same absolute guest root on every host, as the engine manifest
requires.

`--controller-loss <point>` adds one W1 injection to a replay cell (plan S1-4
and M7). The launcher starts the replay unchanged, waits for the point on an
observed live sample (the checkpoint watermark plus the exported counter edge
for that moment), runs the package `fault_inject.py` on the host that holds the
controller through the same executor, and keeps the sample series and the
injector record as cell evidence. The cell passes only when the replay
completes, its audit is equal, the injector reports typed recovery, and a new
verified controller connection followed the loss.

`--list [stage1|stage2]` prints the reviewed cell table from
`campaign_ledger.MANUAL_SUI_CELLS`: the id, tier, production geometry, hold
window, required criteria and the exact `run.py` arguments. A campaign run (one
with `--bucket-prefix`) must name a reviewed `--cell-id`, and the launcher
refuses a fragment size, coordinator cache or hold that its row does not
declare. The campaign ledger is always loaded and merged with the bucket copy,
so a passed cell is never rerun; a retry derives its own `-attempt-<n>` output
tree, and a failed attempt uploads its evidence and ledger before the fleet is
deleted (`campaign_upload.Evidence.persist_failure`).
"""
import argparse
import json
import os
from pathlib import Path
import re
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
import campaign_upload as upload_module
import checkpoint_support as checkpoint
import deployment
import gcp_manual_compute
import host_exec
import mailbox_exec
import owner_lifecycle
import proc_sampler

OWNER_READY_SECONDS = 120
OWNER_STOP_SECONDS = owner_lifecycle.OWNER_STOP_SECONDS
OWNER_KILL_SECONDS = owner_lifecycle.OWNER_KILL_SECONDS
# The owner control plane listens here on every partition; `deployment.prepare`
# adds the partition index to this base. The coordinator is a client and does
# not listen, so it has no port of its own.
OWNER_CONTROL_PORT = 9400
BINARY_SET = {'sui-node-native', 'sui-node-remote', 'storage-node', 'state-audit',
              'rocksdb-audit'}
INJECTOR = Path(__file__).resolve().parents[1] / 'sui-checkpoint/fault_inject.py'
LIVE_PREFIX = 'tidehunter_nvmeof_live_'
# The plan's three injection points, and the exported live counter whose edge
# observes each one. The engine owns the write path, so these counters are the
# only external view of the moment the controller is lost.
CONTROLLER_LOSS_POINTS = ('mid-frame', 'after-adoption', 'during-flush')
CONTROLLER_LOSS_COUNTERS = {
    'mid-frame': ('completed_write_ops_total',),
    'after-adoption': ('adoption_async_exchanges_total', 'adoption_sync_exchanges_total'),
    'during-flush': ('leased_flushes_total', 'leased_flush_covered_frames_total'),
}
CONTROLLER_LOSS_CASE = {'stage1': 'S1-4', 'stage2': 'M7'}
MAX_CONTROLLER_LOSS_SAMPLES = 2048
# A udev partition probe after the controller deletion is expected noise. The
# evidence labels those lines; the cell never gates on the kernel log.
UDEV_PROBE = re.compile(r'udev|blkid|BLKRRPART|no available path|Buffer I/O error',
                        re.IGNORECASE)
# `run.py` bounds each audit command at 600 s; a cell runs at most 8 of them
# (state and RocksDB, for both modes, before and after a reopen) and adds one
# 900 s slack window. The launcher's own wait must cover that plus the replays
# and a hold, or it would kill a healthy coordinator while it audits.
AUDIT_SECONDS = 600
AUDIT_COMMANDS = 8
HANDSHAKE_SLACK_SECONDS = 900


# A failure uploads its evidence before the fleet is deleted. The per-host owner
# archives are rebuildable from the frozen build and are hundreds of MiB, so
# they stay out; a very large stray file is listed, never silently skipped.
FAILURE_ARTIFACT_LIMIT = 512 << 20
FAILURE_ARTIFACT_SKIP = ('host-*.tar.gz',)
# One owner log is retained up to this many bytes; a larger log keeps its tail.
OWNER_LOG_LIMIT = 4 << 20


def handshake_cap(deadline, *, hold_seconds=0, lifecycle=False):
    """Bound the coordinator by its replays, audits and hold (F26).

    A lifecycle cell runs the native replay, the remote replay and the reopen,
    each under the cell deadline; the plain cell runs two. Every audit command
    is bounded at `AUDIT_SECONDS`.
    """
    for name, value in (('deadline', deadline), ('hold', hold_seconds)):
        if type(value) is not int or value < 0:
            raise ValueError(f'{name} must be a nonnegative integer')
    if deadline < 30:
        raise ValueError('deadline must be at least 30 s')
    replays = 3 if lifecycle else 2
    return (replays * deadline + hold_seconds + AUDIT_COMMANDS * AUDIT_SECONDS
            + HANDSHAKE_SLACK_SECONDS)


# The engine exports the write-resume family; the M7 cell proves resent frames
# from these four deltas of one remote before/after scrape.
RESUME_COUNTERS = ('resume_attempts_total', 'resume_rewrites_total',
                   'resume_frames_total', 'resume_refused_total')


class LaunchError(RuntimeError):
    pass


class NotReached(LaunchError):
    """A cell whose point the replay cannot produce; not a correctness failure."""


def kernel_gate(fleet_state, live_transport):
    """Refuse this cell when the fleet never admitted the initiator stack.

    `gcp_manual_compute.py ready` publishes the record before any driver runs;
    a missing or incomplete record means an owner would fail at its first
    commissioning or write-protect, after the cell has started.
    """
    if fleet_state is None:
        return
    try:
        gcp_manual_compute.require_fleet_initiator(
            fleet_state, live_transport=live_transport)
    except gcp_manual_compute.ComputeError as error:
        raise SystemExit(str(error)) from None


def controller_loss_admission(point, tier):
    """Refuse a point that this durability tier cannot produce at all."""
    if point is None:
        return
    if point not in CONTROLLER_LOSS_POINTS:
        raise ValueError('unknown controller-loss point')
    if point == 'during-flush' and tier == 'native':
        raise ValueError(
            'during-flush is unreachable at the native tier: native forces '
            'durable_at_completion, so the replay has no flush edge')


def resume_deltas(delta):
    """The four write-resume counter deltas of one injector before/after pair."""
    counters = (delta or {}).get('by_counter') or {}
    return {name: (counters[name] if isinstance(counters.get(name), (int, float)) else None)
            for name in RESUME_COUNTERS}


def live_window(owner_index):
    """One owner host's live target service window (`deployment.prepare`).

    Every database of that host takes one slot at the two-port stride inside
    this partition window, so the window covers all of them.
    """
    if type(owner_index) is not int or owner_index < 0:
        raise ValueError('owner index must be a nonnegative integer')
    base = 4420 + deployment.LIVE_TARGET_WINDOW * owner_index
    return base, base + deployment.LIVE_TARGET_SLOTS - 1


def controller_loss_cell(stage, point, tier):
    """The plan's cell id for one injection point and tier."""
    if point not in CONTROLLER_LOSS_POINTS:
        raise ValueError('unknown controller-loss point')
    case = CONTROLLER_LOSS_CASE.get(stage)
    if case is None:
        raise ValueError('controller loss is a stage 1 or stage 2 cell')
    return f'{case}-{point}-{tier}'


def controller_loss_cell_id(stage, point, tier, cell_id='sui-replay'):
    """The operator's cell id, or the plan's id when the default was left in place."""
    expected = controller_loss_cell(stage, point, tier)
    if cell_id in (None, 'sui-replay'):
        return expected
    if cell_id != expected:
        raise ValueError(f'a {point} cell of {stage} at {tier} is {expected}, not {cell_id}')
    return cell_id


def controller_loss_addresses(fixture):
    """The live coordinator's JSON-RPC and metrics endpoints.

    `run.py` replays the remote backend from the fixture's `distributed.yaml`
    and writes its own `<output>/remote.yaml`; the fixture carries no
    `remote.yaml`, so the distributed template is the campaign's source.
    """
    fixture = Path(fixture)
    for name in ('remote', 'distributed'):
        if (fixture / f'{name}.yaml').is_file():
            config = checkpoint.configuration(fixture, name)
            break
    else:
        raise ValueError('the fixture has no remote or distributed config')
    values = []
    for name in ('json-rpc-address', 'metrics-address'):
        value = config.get(name)
        if not isinstance(value, str) or not value:
            raise ValueError(f'the fixture {name} is missing')
        values.append(value)
    return values[0], values[1]


def _live_decoder():
    """Load the Sui-checkpoint analyzer under a unique module name.

    Several experiment trees ship an `analyze` module earlier on `sys.path`
    (kv-comparison loads one), so the shared decoder is loaded by file path
    rather than by the ambiguous top-level name.
    """
    import importlib.util
    name = 'sui_checkpoint_analyze'
    module = sys.modules.get(name)
    if module is None:
        path = Path(__file__).resolve().parents[1] / 'sui-checkpoint' / 'analyze.py'
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return module


def live_counters(text):
    """Aggregate live counters from a scrape, pooled over the `db` label.

    Sui registers one metrics registry per database with the constant `db`
    label, so the engine family appears once per database. The shared decoder
    is the one reader of that schema; duplicates fail closed there.
    """
    decoder = _live_decoder()
    return decoder.live_nvmeof_stats(decoder.parse_metrics(text))['counters']


def live_counter(text, name):
    """One aggregate live counter sample; duplicates fail closed."""
    return live_counters(text).get(name)


def live_sample(text, names):
    counters = live_counters(text)
    return {name: counters.get(name) for name in names}


class ControllerLoss:
    """One plan injection point, waited for on an observed live sample.

    The plan names three moments inside the engine's write path. `run.py`
    publishes no inner-engine event, so a point fires on the edge of the live
    counter that the engine exports for that moment, and only after a completed
    checkpoint exists (the injector's own precondition). Every sample is kept:
    a point that never fires fails the cell with the observation series instead
    of injecting at an unproven time. A synchronous `during-flush` point has no
    guaranteed flush, so an unobserved point records the cell as not reached.
    """
    def __init__(self, *, point, host, address, service, service_max, transport,
                 metrics_url, rpc_address, output, nqn=None, intents=None, timeout=30.0,
                 gate_timeout=1200.0, probe_seconds=1.0, not_reached=False,
                 now=time.monotonic, latest=None, metrics=None):
        if point not in CONTROLLER_LOSS_POINTS:
            raise ValueError('unknown controller-loss point')
        self.point = point
        self.host = host
        self.address = address
        self.service, self.service_max = service, service_max
        self.transport = transport
        self.metrics_url = metrics_url
        self.rpc_address = rpc_address
        self.output = Path(output)
        self.nqn = nqn
        self.intents = Path(intents) if intents is not None else None
        self.timeout = timeout
        self.gate_timeout = gate_timeout
        self.probe_seconds = probe_seconds
        self.not_reached = not_reached
        self.now = now
        self.latest = latest or _latest_checkpoint
        self.metrics = metrics or _scrape_metrics
        self.samples = []
        self.started = None
        self.next_probe = 0.0
        self.evidence = None
        self.done = False

    def sample(self):
        row = dict(unix_ns=time.time_ns(), seconds=None, watermark=None,
                   counters={}, reason=None)
        try:
            row['watermark'] = int(self.latest(self.rpc_address))
        except (OSError, RuntimeError, ValueError) as error:
            row['reason'] = f'checkpoint RPC sample failed: {error}'
        try:
            text = self.metrics(self.metrics_url)
            row['counters'] = live_sample(text, CONTROLLER_LOSS_COUNTERS[self.point])
        except (OSError, ValueError) as error:
            row['reason'] = f'live metrics sample failed: {error}'
        return row

    def fires(self, previous, current):
        """Whether this sample is the point's observed edge."""
        if current['watermark'] is None or current['watermark'] < 1:
            return False, 'no completed checkpoint'
        if previous is None:
            return False, 'one sample'
        for name in CONTROLLER_LOSS_COUNTERS[self.point]:
            before = previous['counters'].get(name)
            after = current['counters'].get(name)
            if after is None:
                return False, f'{LIVE_PREFIX + name} is absent from the scrape'
            if before is not None and after > before:
                return True, None
        return False, 'no edge in ' + ', '.join(CONTROLLER_LOSS_COUNTERS[self.point])

    def poll(self, remote):
        """Probe once and inject when the point fires; True means injected."""
        if self.done:
            return True
        now = self.now()
        if self.started is None:
            self.started = now
        if now < self.next_probe:
            return False
        self.next_probe = now + self.probe_seconds
        row = self.sample()
        row['seconds'] = now - self.started
        previous = self.samples[-1] if self.samples else None
        if len(self.samples) < MAX_CONTROLLER_LOSS_SAMPLES:
            self.samples.append(row)
        fired, reason = self.fires(previous, row)
        if fired:
            self.inject(remote, row)
            return True
        if now - self.started > self.gate_timeout:
            self.evidence = dict(point=self.point, host=self.host, injected=False,
                                 reason=reason, gate_timeout=self.gate_timeout,
                                 samples=self.samples)
            self.write_evidence()
            raise self.miss(f'not observed before its gate deadline: {reason}')
        return False

    def miss(self, reason):
        """The exception for a point this replay did not reach."""
        if self.not_reached:
            return NotReached('not reached: no flush in replay (' + reason + ')')
        return LaunchError(f'controller-loss point {self.point} was {reason}')

    def argv(self, output):
        argv = [sys.executable, str(INJECTOR),
                '--transport', self.transport, '--address', self.address,
                '--service', str(self.service), '--service-max', str(self.service_max),
                '--sui-rpc', self.rpc_address, '--timeout', str(self.timeout),
                '--output', str(output)]
        if self.nqn is not None:
            argv += ['--nqn', self.nqn]
        if self.intents is not None:
            argv += ['--intents', str(self.intents)]
        if self.metrics_url:
            argv += ['--metrics-url', self.metrics_url]
        return argv

    def host_time(self, remote):
        """The host wall clock, used as the kernel-log window start."""
        code, out, err = remote.run(self.host, 'date -u +%Y-%m-%dT%H:%M:%SZ', check=False)
        if code != 0:
            return None
        text = out.decode('utf-8', 'replace').strip()
        return text or None

    def kernel_log(self, remote, since, until=None):
        """The host kernel log of the injection window, udev probes labelled.

        The injector deletes a controller outside the engine's teardown guards,
        so a udev partition probe can log an expected error. Those lines are
        kept and labelled; the cell never gates on this window.
        """
        window = dict(host=self.host, since=since, until=until, available=False,
                      reason=None, lines=[], udev_probe=[], other=[])
        if since is None:
            window['reason'] = 'the host clock was unreadable'
            return window
        code, out, err = remote.run(
            self.host, 'dmesg --time-format=iso --since ' + shlex.quote(since),
            check=False)
        if code != 0:
            window['reason'] = (err.decode('utf-8', 'replace').strip()[-500:]
                                or f'dmesg exited {code}')
            return window
        window['available'] = True
        window['collected_unix_ns'] = time.time_ns()
        for line in out.decode('utf-8', 'replace').splitlines():
            if not line.strip():
                continue
            window['lines'].append(line)
            (window['udev_probe'] if UDEV_PROBE.search(line) else window['other']).append(line)
        return window

    def inject(self, remote, gate):
        """Run the package injector on the controller's host and retain its record."""
        output = self.output / 'controller-loss' / f'{self.point}.json'
        remote.shell(self.host, f'mkdir -p {shlex.quote(str(output.parent))}')
        since = self.host_time(remote)
        command = 'exec ' + ' '.join(shlex.quote(str(part)) for part in self.argv(output))
        code, out, err = remote.run(self.host, command, check=False)
        until = self.host_time(remote)
        record = None
        try:
            record = json.loads(remote.read_bytes(self.host, output))
        except (host_exec.HostCommandError, ValueError) as error:
            record = dict(ok=False, reason=f'injector record unavailable: {error}')
        self.evidence = dict(
            point=self.point, host=self.host, address=self.address,
            service=self.service, service_max=self.service_max,
            transport=self.transport, nqn=self.nqn,
            intents=None if self.intents is None else str(self.intents),
            injected=True,
            exit_code=code, stdout=out.decode('utf-8', 'replace')[-4000:],
            stderr=err.decode('utf-8', 'replace')[-4000:],
            resume=resume_deltas((record or {}).get('live_delta')),
            gate=gate, samples=self.samples, injector=record,
            kernel_log=self.kernel_log(remote, since, until))
        write_json(self.output / 'controller-loss' / f'{self.point}.json', record)
        self.done = True

    def write_evidence(self):
        write_json(self.output / 'controller-loss' / 'evidence.json', self.evidence)


def _latest_checkpoint(address):
    import fault_inject
    return fault_inject.latest_checkpoint(address)


def _scrape_metrics(url):
    import fault_inject
    return fault_inject.scrape_metrics(url)


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    os.replace(temporary, path)


def controller_loss_verdict(evidence):
    """The plan's S1-4/M7 conditions that the injector record can prove."""
    failures = []
    if evidence is None:
        return ['no controller-loss evidence was recorded']
    if evidence.get('injected') is not True:
        return ['the injection point was not reached: ' + str(evidence.get('reason'))]
    if evidence.get('exit_code') != 0:
        failures.append(f'the injector exited {evidence.get("exit_code")}')
    record = evidence.get('injector') or {}
    if record.get('ok') is not True:
        failures.append('the injector refused or failed: ' + str(record.get('reason')))
    if record.get('fail_stop_count'):
        failures.append('an owned process fail-stopped during the injection')
    if record.get('readback_equal') is not True:
        failures.append('the pre-fault checkpoint summary did not read back identically')
    delta = record.get('live_delta') or {}
    if delta.get('available') is not True:
        failures.append('the aggregate live counters are not a usable before/after pair')
    elif (delta.get('errors') or 0) > 0:
        failures.append('live data-plane errors increased during the injection')
    elif (delta.get('namespace_connects') or 0) < 1:
        failures.append('no new verified controller connection followed the injection')
    resume = evidence.get('resume') or resume_deltas(delta)
    missing = sorted(name for name in RESUME_COUNTERS if resume.get(name) is None)
    if missing:
        failures.append('the remote scrape lacks the write-resume counters: '
                        + ', '.join(missing))
    return failures


def fleet_hosts(fleet, names, *, transport, live_transport):
    """Resolve owner fleet names to their control and live-target addresses.

    A campaign guest has two NICs: `internal_ip` on the default subnet and
    `rdma_ip` on the falcon-rdma subnet. TCP uses the default subnet. A verbs
    control fabric and NVMe/RDMA use the falcon-rdma subnet, because the verbs
    provider binds the IRDMA NIC and `nvmet` exports its target there.
    `prepare_external.py` writes the control listen address and, separately,
    the live target pool address from these two lists.
    """
    names = list(names)
    if not isinstance(fleet, (list, tuple)) or not fleet:
        # A rehearsal passes reachable addresses instead of fleet names.
        return names, names
    by_name = {host['name']: host for host in fleet if isinstance(host, dict)}
    verbs = deployment.canonical_transport(transport).get('fabric', {}).get('backend') == 'verbs'
    control_key = 'rdma_ip' if verbs else 'internal_ip'
    live_key = 'rdma_ip' if live_transport == 'rdma' else 'internal_ip'
    control, live = [], []
    for name in names:
        host = by_name.get(name)
        if host is None:
            raise ValueError(f'{name}: not a host of this fleet state')
        for key in (control_key, live_key):
            if not isinstance(host.get(key), str) or not host[key]:
                raise ValueError(f'{name}: fleet state has no {key}')
        control.append(host[control_key])
        live.append(host[live_key])
    if len(set(control)) != len(control) or len(set(live)) != len(live):
        raise ValueError('owner control and live-target addresses must be distinct')
    return control, live


def controller_loss_intents(args, launcher):
    """The coordinator's live connection intents directory on the injector host.

    The engine publishes one intent per controller under the coordinator
    database root, which the rehearsal already hands to `prepare_external` as
    `--db-root`. An explicit `--controller-loss-intents` still wins.
    """
    if args.controller_loss_intents is not None:
        return Path(args.controller_loss_intents)
    return launcher.remote_root() / 'live-nvme-connections'


def plan(coordinator, owners):
    """Validate the 1 coordinator + 2 owner shape and return the roles."""
    if not isinstance(owners, (list, tuple)) or len(owners) != 2:
        raise ValueError('exactly two owner hosts are required')
    if not coordinator or any(not host for host in owners):
        raise ValueError('coordinator and owner hosts must be nonempty')
    if len(set([coordinator, *owners])) != 3:
        raise ValueError('coordinator and owner hosts must be distinct')
    return dict(coordinator=coordinator, owners=list(owners))


class SuiLauncher:
    def __init__(self, *, remote, build_root, fixture, output, guest_root, stage_dir,
                 config_dir, resources, tier='native', owners=2,
                 fragment_mib=None, deadline=900, extra_run=(), transport='efa',
                 live_transport='rdma', coordinator_cache_mib=None, shard_cache_mib=0,
                 schema=None, disk_sample_seconds=10, port=OWNER_CONTROL_PORT,
                 ledger=None, cell_id='sui-replay', stage='stage1', resume=False,
                 store=None, bucket=None, prefix=None, write_profile_limit=0,
                 resident_reads=None, allow_local_ledger=False, controller_loss=None,
                 hold_seconds=0, cleanup_root=None):
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
        self.fragment_mib = (ledger_module.DEFAULT_FRAGMENT_MIB
                             if fragment_mib is None else fragment_mib)
        self.deadline = deadline
        self.extra_run = [item for item in extra_run if item != '--']
        self.transport = transport
        self.live_transport = live_transport
        self.coordinator_cache_mib = (ledger_module.DEFAULT_COORDINATOR_CACHE_MIB
                                      if coordinator_cache_mib is None
                                      else coordinator_cache_mib)
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
        self.allow_local_ledger = allow_local_ledger
        self.controller_loss = controller_loss
        self.hold_seconds = hold_seconds
        self.cleanup_root = Path(cleanup_root) if cleanup_root else None
        self.stops = []
        self.cleanups = []
        self.failure_skipped = []

    # ---- admission -----------------------------------------------------
    def admit(self, hosts):
        if self.controller_loss is not None:
            controller_loss_admission(self.controller_loss.point, self.tier)
            # The launcher owns the tier decision: only a synchronous replay can
            # reach a flush edge, and only then is an unobserved point benign.
            self.controller_loss.not_reached = (
                self.controller_loss.point == 'during-flush'
                and self.tier == 'synchronous')
        if set(self.manifest['binaries']) != BINARY_SET:
            raise ValueError('frozen build lacks the exact campaign executable set')
        source_manifest = self.build_root / 'source-manifest.json'
        if (not source_manifest.is_file()
                or build.digest(source_manifest) != self.manifest.get('source_manifest_sha256')):
            raise ValueError('the frozen build source manifest is missing or differs')
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
        control, live = self.endpoints()
        command = [sys.executable, str(self.source / 'prepare_external.py'),
                   '--build', str(self.build_root), '--schema', str(self.schema),
                   '--output', str(self.config_dir), '--db-root', str(self.remote_root()),
                   '--guest-root', str(self.guest_root),
                   '--hosts', ','.join(control), '--live-hosts', ','.join(live),
                   '--owners', str(self.owners),
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

    def endpoints(self):
        """The owner control and live-target addresses this replay dials."""
        return fleet_hosts(getattr(self.remote, 'fleet', None), self.hosts,
                           transport=self.transport,
                           live_transport=self.live_transport)

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
    def cleanup_owners(self, *, required=True):
        """Run the exact-owned configfs gate on every owner host.

        A previous incarnation may have died before its typed stop marker, and
        its port links would make the next owner's reconcile refuse to share a
        configfs object. Only this engine's NQN prefix is removed.
        """
        hosts = [record['host'] for record in self.external['hosts']]
        records = owner_lifecycle.cleanup_hosts(
            self.remote, hosts, root=self.cleanup_root)
        self.cleanups.extend(records)
        self.write_lifecycle_evidence()
        failed = [row['host'] for row in records if not row['complete']]
        if failed and required:
            raise LaunchError('owner configfs cleanup failed on '
                              + ', '.join(sorted(failed)))
        return failed

    def write_lifecycle_evidence(self):
        for name, rows in (('owner-stops.json', self.stops),
                           ('owner-cleanup.json', self.cleanups)):
            if not rows:
                continue
            path = self.output / name
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(path.name + '.tmp')
            temporary.write_text(json.dumps(rows, indent=2, sort_keys=True) + '\n')
            os.replace(temporary, path)

    def start(self):
        self.cleanup_owners()
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
                                       process_group=pid, log=str(log),
                                       partition=record['partition'],
                                       stop=record['stop_files'][index]))
            self.namespaces[host] = namespaces
        for record in self.external['hosts']:
            for ready in record['ready_files']:
                deadline = time.monotonic() + OWNER_READY_SECONDS
                while not self.remote.exists(record['host'], ready):
                    if time.monotonic() > deadline:
                        raise LaunchError(f'{record["host"]}: owner readiness expired')
                    time.sleep(0.2)
        self.started = True

    def sampling_loop(self):
        while not self.sample_stop.is_set():
            for host in self.namespaces:
                try:
                    row = proc_sampler.sample_host(self.remote, host, self.namespaces[host])
                except BaseException as error:  # a dead transport is not a healthy sample
                    row = dict(unix_ns=time.time_ns(), healthy=False, reason=str(error))
                rows = self.samples.setdefault(host, [])
                if rows and row['unix_ns'] <= rows[-1]['unix_ns']:
                    row['unix_ns'] = rows[-1]['unix_ns'] + 1
                if len(rows) < proc_sampler.MAX_SAMPLES:
                    rows.append(row)
            self.sample_stop.wait(proc_sampler.SAMPLE_SECONDS)

    def start_sampling(self):
        # One synchronous sample before run.py starts anchors healthy[0] before
        # the remote-starting marker; the loop then covers the replay.
        for host in self.namespaces:
            self.samples.setdefault(host, []).append(
                proc_sampler.sample_host(self.remote, host, self.namespaces[host]))
        self.sample_thread = threading.Thread(target=self.sampling_loop, daemon=True)
        self.sample_thread.start()

    def final_sample(self):
        for host in self.namespaces:
            rows = self.samples.setdefault(host, [])
            row = proc_sampler.sample_host(self.remote, host, self.namespaces[host])
            if rows and row['unix_ns'] <= rows[-1]['unix_ns']:
                row['unix_ns'] = rows[-1]['unix_ns'] + 1
            rows.append(row)

    def stop(self, *, fail_on_forced=True):
        """Close every owned group, then remove this engine's configfs objects.

        The typed stop marker is tried first, so the owner runs its own
        teardown and writes its shutdown proof. SIGTERM has no handler in the
        owner, so using it first killed the process before teardown and left
        its port links. A forced kill or a failed cleanup fails the cell; the
        next cell's `start` also refuses to run if leftovers remain.
        """
        alive = any(not namespace.get('stopped')
                    for namespaces in self.namespaces.values()
                    for namespace in namespaces)
        if not self.started and not alive:
            return []
        self.sample_stop.set()
        if self.sample_thread is not None:
            self.sample_thread.join(timeout=10)
            self.sample_thread = None
        forced = []
        for host, namespaces in self.namespaces.items():
            for namespace in namespaces:
                if namespace.get('stopped'):
                    continue
                record = owner_lifecycle.stop_owner(
                    self.remote, host=host, pid=namespace['pid'],
                    process_group=namespace['process_group'],
                    stop_path=namespace.get('stop'),
                    partition=namespace.get('partition'),
                    grace=OWNER_STOP_SECONDS, kill_grace=OWNER_KILL_SECONDS)
                namespace['stopped'] = not record['still_alive']
                self.stops.append(record)
                if record['forced'] or record['still_alive']:
                    forced.append(host)
        self.started = False
        survivors = sorted({host for host, namespaces in self.namespaces.items()
                            for namespace in namespaces
                            if not namespace.get('stopped')})
        failed = list(survivors)
        if survivors:
            for host in survivors:
                self.cleanups.append(dict(
                    host=host, root=str(self.cleanup_root) if self.cleanup_root
                    else owner_lifecycle.CONFIGFS,
                    prefix=owner_lifecycle.NQN_PREFIX, before=[], removed=[],
                    remains=[], unavailable=False, complete=False, exit_code=None,
                    error='owner still alive; cleanup refused'))
        else:
            failed.extend(self.cleanup_owners(required=False))
        self.write_lifecycle_evidence()
        if fail_on_forced and (forced or failed):
            detail = sorted(set(forced) | set(failed))
            raise LaunchError('owner stop or configfs cleanup failed on '
                              + ', '.join(detail))
        return forced

    # ---- host evidence -------------------------------------------------
    def write_resources(self, binding):
        value = proc_sampler.host_evidence(
            self.external['hosts'], self.namespaces, self.samples, binding,
            binary_sha256=self.manifest['binaries']['storage-node'],
            guest_root=self.guest_root)
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
                      package_manifest=build.digest(self.build_root / 'source-manifest.json'),
                      source_archive=self.manifest.get('source_archive_sha256'),
                      fixture=build.digest(self.fixture / 'experiment/results.json'),
                      schema=schema_files),
            knobs=dict(transport=self.transport, live_transport=self.live_transport,
                       tier=self.tier, owners=self.owners,
                       fragment_mib=self.fragment_mib, run_args=self.extra_run,
                       coordinator_cache_mib=self.coordinator_cache_mib,
                       shard_cache_mib=self.shard_cache_mib,
                       deadline=self.deadline,
                       disk_sample_seconds=self.disk_sample_seconds,
                       port=self.port,
                       hold_seconds=self.hold_seconds,
                       write_profile_limit=self.write_profile_limit,
                       resident_reads=(build.digest(self.resident_reads)
                                       if self.resident_reads else None),
                       controller_loss=(None if self.controller_loss is None else dict(
                           point=self.controller_loss.point,
                           host=self.controller_loss.host,
                           address=self.controller_loss.address,
                           service=self.controller_loss.service,
                           service_max=self.controller_loss.service_max,
                           nqn=self.controller_loss.nqn,
                           timeout=self.controller_loss.timeout,
                           gate_timeout=self.controller_loss.gate_timeout))),
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
            rows.append(upload_module.artifact(path, self.evidence_name(path)))
        # Per-sample files also travel on their own so the bucket rule on raw/
        # can expire them; the evidence bundle keeps the complete copy.
        experiment = self.output / 'experiment'
        for pattern in ('*.prom', '*-samples.jsonl', 'samples.csv', 'resources.csv'):
            for path in sorted(experiment.glob(pattern)):
                rows.append(upload_module.artifact(path, self.evidence_name(path)))
        # The controller-loss record and its gate series are cell evidence, not
        # a per-sample stream: they stay under evidence/ with the summary.
        for path in sorted((self.output / 'controller-loss').glob('*.json')):
            rows.append(upload_module.artifact(path, self.evidence_name(path)))
        # The owner close and the between-cell cleanup are their own records:
        # a shutdown proof is useful only next to the signal that was used.
        for path in (self.output / 'owner-stops.json',
                     self.output / 'owner-cleanup.json'):
            if path.is_file():
                rows.append(upload_module.artifact(path, self.evidence_name(path)))
        return rows

    def persist(self, entry, artifacts):
        """Pass the cell only after its evidence and the ledger are verified."""
        evidence = upload_module.Evidence(self.store, self.prefix)
        upload = evidence.persist(self.ledger, entry, artifacts)
        receipt = dict(
            cell_id=self.cell_id, stage=self.stage, status='passed',
            attempt=entry['attempts'], bucket=self.bucket, prefix=self.prefix,
            ledger=upload['ledger'], evidence=upload['evidence'],
            mirror=dict(root=str(self.output),
                        note='the local artifact tree is the mirror; every uploaded '
                             'object is verified by size and SHA-256 on both sides'))
        path = self.output / 'upload-receipt.json'
        temporary = path.with_name(path.name + '.tmp')
        temporary.write_text(json.dumps(receipt, indent=2) + '\n')
        os.replace(temporary, path)
        return receipt

    def collect_owner_logs(self):
        """Copy each owner's bounded `owner-*.log` off the host before it dies.

        The boot disk is deleted at the stage deadline, so a failure that
        happens before `run.py` writes anything would otherwise upload no
        reason at all. Each host is read through the same executor as the
        cell, one bounded tail per file, and a bad host never masks the cell
        failure.
        """
        rows = []
        external = getattr(self, 'external', None) or {}
        if self.remote is None or not external.get('hosts'):
            return rows
        root = Path(self.output) / 'owner-logs'
        for record in external['hosts']:
            host = record.get('host')
            if not isinstance(host, str) or not host:
                continue
            try:
                code, listing, error = self.remote.run(
                    host, f'ls -1 {shlex.quote(str(self.guest_root))}/owner-*.log 2>/dev/null',
                    check=False)
                if code != 0:
                    rows.append(dict(host=host, error=error.decode('utf-8', 'replace')[-2000:]))
                    continue
                for name in sorted(set(listing.decode('utf-8', 'replace').split())):
                    if not Path(name).name.startswith('owner-'):
                        continue
                    quoted = shlex.quote(name)
                    size_code, size_out, _ = self.remote.run(
                        host, f'stat -c %s {quoted} 2>/dev/null', check=False)
                    if size_code != 0 or not size_out.strip().isdigit():
                        rows.append(dict(host=host, name=Path(name).name,
                                         error='stat failed'))
                        continue
                    size = int(size_out.strip())
                    truncated = size > OWNER_LOG_LIMIT
                    if truncated:
                        read_code, data, _ = self.remote.run(
                            host, f'tail -c {OWNER_LOG_LIMIT} {quoted}', check=False)
                    else:
                        read_code, data, _ = self.remote.run(host, f'cat {quoted}', check=False)
                    if read_code != 0:
                        rows.append(dict(host=host, name=Path(name).name,
                                         error='read failed'))
                        continue
                    destination = root / host / Path(name).name
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    destination.write_bytes(data)
                    rows.append(dict(host=host, name=Path(name).name,
                                     bytes=len(data), truncated=truncated))
            except BaseException as error:  # a dead executor must not mask the cell error
                rows.append(dict(host=host, error=str(error)[-2000:]))
        if rows:
            write_json(root / 'index.json', dict(limit=OWNER_LOG_LIMIT, files=rows))
        return rows

    def failure_artifacts(self):
        """Every artifact the failed attempt left, existing files only.

        The boot disk is deleted at the stage deadline, so a failure uploads
        what it has before the fleet goes away. Private TLS material, bulk
        DB/WAL trees and the rebuildable per-host owner archives never travel;
        a missing path is skipped, never invented.
        """
        rows, seen, skipped = [], set(), []
        roots = ((self.output, ''), (self.config_dir, 'external/'),
                 (self.stage_dir, 'stages/'))
        for root, label in roots:
            root = Path(root)
            if not root.is_dir():
                continue
            for path in sorted(root.rglob('*')):
                if not path.is_file():
                    continue
                parts = path.relative_to(root).parts
                if any(part in ('tls', 'storage') for part in parts):
                    continue
                if any(path.match(pattern) for pattern in FAILURE_ARTIFACT_SKIP):
                    skipped.append(dict(path=str(path),
                                        reason='rebuildable owner archive'))
                    continue
                if path.stat().st_size > FAILURE_ARTIFACT_LIMIT:
                    skipped.append(dict(path=str(path),
                                        reason='above the failure upload bound'))
                    continue
                name = label + str(path.relative_to(root))
                if name in seen:
                    continue
                seen.add(name)
                rows.append(upload_module.artifact(path, name))
        if (self.resources.is_file()
                and self.resources.stat().st_size <= FAILURE_ARTIFACT_LIMIT
                and self.evidence_name(self.resources) not in seen):
            rows.append(upload_module.artifact(
                self.resources, self.evidence_name(self.resources)))
        self.failure_skipped = skipped
        return rows

    def record_failure(self, entry, error):
        """Upload the failed attempt's evidence and ledger; never mask the error."""
        if self.store is None:
            return None
        try:
            artifacts = self.failure_artifacts()
            entry['failure_skipped'] = self.failure_skipped
            upload = upload_module.Evidence(self.store, self.prefix).persist_failure(
                entry, artifacts, error, ledger=self.ledger)
        except BaseException as upload_error:  # the caller's error is the report
            print(f'failure evidence upload skipped: {upload_error}', file=sys.stderr)
            return None
        print(json.dumps(dict(cell_id=self.cell_id, stage=self.stage,
                              status='failed', attempt=entry['attempts'],
                              upload=upload)))
        return upload

    def record_observation(self, entry):
        """Upload a non-failure attempt's evidence and ledger, never masking."""
        if self.store is None:
            return None
        try:
            evidence = upload_module.Evidence(self.store, self.prefix)
            receipts = evidence.upload(self.failure_artifacts(),
                                       stage=entry['stage'], cell_id=entry['id'],
                                       attempt=entry['attempts'])
            evidence.save(self.ledger)
        except BaseException as upload_error:
            print(f'observation upload skipped: {upload_error}', file=sys.stderr)
            return None
        return receipts

    def not_reached(self, error, *, terminal=False):
        """Record a cell the replay could not produce; it is not a failure."""
        receipt = dict(complete=False, not_reached=True, status='not_reached',
                       cell_id=self.cell_id, stage=self.stage, tier=self.tier,
                       reason=str(error), terminal=terminal)
        print(json.dumps(receipt))
        return receipt

    def run(self, hosts):
        identity = self.identity()
        if self.ledger is None:
            self.admit(hosts)
            try:
                return self.execute(hosts)
            except NotReached as error:
                return self.not_reached(error)
        previous = self.ledger.entries.get((self.stage, self.cell_id))
        if previous and previous['status'] == 'invalid':
            # A cell whose injection point was not reached twice is terminal;
            # rerunning it would spend the fleet on the same observation.
            receipt = dict(complete=False, status='invalid', terminal=True,
                           skipped=True, cell_id=self.cell_id, tier=self.tier,
                           attempt=previous['attempts'],
                           reason=previous.get('failure'))
            print(json.dumps(receipt))
            return receipt
        # A passed cell with the same binding is never rerun, whether or not the
        # operator passed --resume: the merged bucket ledger is the record.
        if (previous and previous['status'] == 'passed'
                and previous['config_hash'] == identity):
            receipt = dict(complete=True, skipped=True, cell_id=self.cell_id,
                           tier=self.tier, owners=self.owners,
                           attempt=previous['attempts'])
            print(json.dumps(receipt))
            return receipt
        self.admit(hosts)
        entry = self.ledger.begin(stage=self.stage, cell_id=self.cell_id,
                                  config_hash=identity, unit=self.cell_id)
        self.ledger.save()
        try:
            receipt = self.execute(hosts)
            if self.store is None:
                if not self.allow_local_ledger:
                    raise ValueError('a passed cell needs a verified evidence upload; '
                                     'add --bucket-prefix, or --allow-local-ledger for a '
                                     'local rehearsal')
                receipt['ledger'] = upload_module.pass_local(
                    self.ledger, entry, self.artifacts())
            else:
                receipt['upload'] = self.persist(entry, self.artifacts(entry['attempts']))
                receipt['ledger'] = dict(local=str(self.ledger.path), uploaded=True,
                                         key=receipt['upload']['ledger']['key'])
            self.ledger.save()
            return receipt
        except NotReached as error:
            # A point the replay cannot produce runs once more; a second miss is
            # terminal, so a resume loop cannot spend the fleet on it.
            terminal = entry['attempts'] >= 2
            if terminal:
                self.ledger.invalidate(
                    entry, f'not reached on attempt {entry["attempts"]}: {error}')
            else:
                self.ledger.not_reached(entry, str(error))
            self.record_observation(entry)
            self.ledger.save()
            return self.not_reached(error, terminal=terminal)
        except BaseException as error:
            self.ledger.fail(entry, error)
            if getattr(self, 'namespaces', None) or getattr(self, 'started', False):
                self.collect_owner_logs()
            self.record_failure(entry, error)
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
        if self.hold_seconds:
            command += ['--hold-seconds', str(self.hold_seconds)]
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
            deadline = time.monotonic() + handshake_cap(
                self.deadline, hold_seconds=self.hold_seconds,
                lifecycle=('--lifecycle-checks' in self.extra_run))
            while child.poll() is None:
                if marker.exists():
                    required = json.loads(marker.read_text())
                    binding = {key: required[key] for key in
                               ('config_sha256', 'build_manifest_sha256', 'checkpoint_digest')}
                    self.final_sample()
                    self.stop()
                    self.write_resources(binding)
                    resources_written = True
                    if self.controller_loss is not None and not self.controller_loss.done:
                        raise self.controller_loss.miss(
                            'not observed before the replay requested its final evidence')
                if resources_written and (self.controller_loss is None
                                          or self.controller_loss.done):
                    break
                if self.controller_loss is not None and not resources_written:
                    self.controller_loss.poll(self.remote)
                if time.monotonic() > deadline:
                    raise TimeoutError('run.py did not request resource evidence')
                time.sleep(0.2)
            code = child.wait()
        finally:
            if self.started:
                self.stop(fail_on_forced=False)
            if child.poll() is None:
                code = child.wait()
        if self.controller_loss is not None:
            self.controller_loss.write_evidence()
        if code != 0:
            raise LaunchError(f'run.py failed with exit status {code}')
        if not resources_written:
            raise LaunchError('run.py exited without requesting resource evidence')
        receipt = dict(complete=True, exit_code=code, tier=self.tier,
                       owners=self.owners,
                       build_manifest_sha256=build.digest(
                           self.build_root / 'build-manifest.json'))
        if self.controller_loss is not None:
            failures = controller_loss_verdict(self.controller_loss.evidence)
            if not failures and not self.controller_loss.evidence.get('resume'):
                failures.append('the controller-loss evidence lacks the resume deltas')
            results = json.loads((self.output / 'experiment/results.json').read_text())
            if results.get('complete') is not True:
                failures.append('run.py did not record a complete replay')
            if (results.get('audit') or {}).get('all_equal') is not True:
                failures.append('the replay audit is absent or not equal')
            if failures:
                raise LaunchError('controller-loss cell failed: ' + '; '.join(failures))
            receipt['controller_loss'] = dict(
                point=self.controller_loss.point,
                host=self.controller_loss.host,
                injector_ok=(self.controller_loss.evidence.get('injector') or {}).get('ok'))
        (self.output / 'launcher-receipt.json').write_text(
            json.dumps(receipt, indent=2) + '\n')
        return receipt


def attempt_output(output, attempt):
    """Attempt-scoped output family; a retry never reuses a directory (F20).

    The ledger's attempt number is authoritative. The base name is kept, so an
    operator can see which cell an attempt belongs to, and a suffix is added
    until a free name is found; the stage, config and resource paths derive
    from the returned path.
    """
    if type(attempt) is not int or attempt < 1:
        raise ValueError('attempt must be a positive integer')
    output = Path(output)
    if attempt == 1 and not output.exists():
        return output
    number = max(attempt, 2)
    while True:
        candidate = output.with_name(f'{output.name}-attempt-{number}')
        if not candidate.exists():
            return candidate
        number += 1


def cell_row_checks(cell, args):
    """Refuse a reviewed cell whose launcher knobs differ from its row (F8)."""
    if args.stage != cell['stage']:
        raise SystemExit(f"{cell['id']} is a {cell['stage']} cell; "
                         f"give --stage {cell['stage']}")
    if args.durability_tier != cell['tier']:
        raise SystemExit(f"{cell['id']} is a {cell['tier']} cell, not "
                         f"{args.durability_tier}")
    for flag, actual, declared in (
            ('--fragment-mib', args.fragment_mib, cell['fragment_mib']),
            ('--coordinator-cache-mib', args.coordinator_cache_mib,
             cell['coordinator_cache_mib']),
            ('--hold-seconds', args.hold_seconds, cell['hold_seconds'])):
        if actual != declared:
            raise SystemExit(f"{cell['id']} declares {flag} {declared}, "
                             f"not {actual}; the campaign geometry is fixed")


def cell_table(stage='all'):
    """The reviewed cell table as printable rows, one per cell."""
    rows = []
    for cell in ledger_module.cells(None if stage == 'all' else stage):
        run = ' '.join(ledger_module.run_arguments(cell))
        rows.append(dict(id=cell['id'], stage=cell['stage'], tier=cell['tier'],
                         fragment_mib=cell['fragment_mib'],
                         coordinator_cache_mib=cell['coordinator_cache_mib'],
                         max_maps=cell['max_maps'], max_index_maps=cell['max_index_maps'],
                         hold_seconds=cell['hold_seconds'],
                         requires=','.join(cell['requires']) or '-',
                         run=run or '-'))
    return rows


def list_cells(stage='all'):
    """Print the reviewed table and exit zero."""
    if stage not in (*ledger_module.REVIEWED_STAGES, 'all'):
        raise SystemExit('--list takes stage1, stage2 or all')
    rows = cell_table(stage)
    print(json.dumps(dict(cells=rows, count=len(rows)), indent=1))
    return 0


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


def main():
    stage = list_request(sys.argv[1:])
    if stage is not None:
        return list_cells(stage)
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
    parser.add_argument('--zone',
                        help='zone of the three hosts; only --gcloud-ssh needs it, a '
                             'campaign run takes the zone from the fleet state')
    parser.add_argument('--external-config-dir', type=Path)
    parser.add_argument('--stage-dir', type=Path)
    parser.add_argument('--resources', type=Path)
    parser.add_argument('--ledger', type=Path,
                        help='campaign ledger (default: beside the run output); it is '
                             'always loaded and merged with the bucket copy')
    parser.add_argument('--resume', action='store_true',
                        help='accepted for older command lines; the ledger is always '
                             'merged and a passed cell is always skipped')
    parser.add_argument('--cell-id',
                        help='reviewed campaign cell id (required for a bucket run); '
                             '--list prints the reviewed table')
    parser.add_argument('--stage', default='stage1')
    parser.add_argument('--list', nargs='?', const='all', choices=('stage1', 'stage2', 'all'),
                        help='print the reviewed manual_sui cell table and exit')
    parser.add_argument('--transport', choices=('tcp', 'tcp-trusted',
                        *deployment.HARDWARE_TRANSPORTS), default='tcp')
    parser.add_argument('--live-transport', choices=deployment.LIVE_TRANSPORTS,
                        default='rdma')
    parser.add_argument('--durability-tier', choices=('native', 'synchronous'),
                        default='native')
    parser.add_argument('--fragment-mib', type=int,
                        default=ledger_module.DEFAULT_FRAGMENT_MIB)
    parser.add_argument('--deadline', type=int, default=900)
    parser.add_argument('--disk-sample-seconds', type=int, default=10)
    parser.add_argument('--hold-seconds', type=int, default=0,
                        help='M5 hold: keep the measured node resident this long; a '
                             'reviewed cell declares its own value')
    parser.add_argument('--port', type=int, default=OWNER_CONTROL_PORT,
                        help='owner control port; each partition adds its index')
    parser.add_argument('--coordinator-cache-mib', type=int,
                        default=ledger_module.DEFAULT_COORDINATOR_CACHE_MIB)
    parser.add_argument('--shard-cache-mib', type=int, default=0)
    parser.add_argument('--write-profile-limit', type=int, default=0,
                        help='bounded append profiling per owning connection, 0 disables')
    parser.add_argument('--resident-reads', type=Path,
                        help='resident-read policy forwarded to the owners and the runner')
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
    parser.add_argument('--bucket-prefix',
                        help='campaign object prefix, gs://mysten-research-bench-artifacts/'
                             '<campaign-id>; a cell passes only after its verified upload')
    parser.add_argument('--impersonation-account', default=campaign_manifest.ACCOUNT,
                        help='service account that writes the campaign prefix')
    parser.add_argument('--allow-local-ledger', action='store_true',
                        help='local rehearsal only: pass a cell without an uploaded bundle')
    parser.add_argument('--controller-loss', choices=CONTROLLER_LOSS_POINTS,
                        help='inject one W1 controller loss during the remote replay; '
                             'the cell is S1-4-<point>-<tier> (stage1) or '
                             'M7-<point>-<tier> (stage2)')
    parser.add_argument('--controller-loss-owner', type=int, default=1,
                        help='1-based owner whose target controller is deleted')
    parser.add_argument('--controller-loss-host',
                        help='host that holds the controller (default: the coordinator, '
                             'the only NVMe-oF initiator; override for a rehearsal)')
    parser.add_argument('--controller-loss-nqn',
                        help='exact target NQN; otherwise --controller-loss-intents '
                             'names the coordinator intents directory that selects the '
                             'active write controller')
    parser.add_argument('--controller-loss-intents',
                        help="override the coordinator's live-nvme-connections "
                             'directory; by default it is derived from the '
                             "coordinator database root this cell prepares")
    parser.add_argument('--controller-loss-timeout', type=float, default=30.0,
                        help='injector reconnect and write-progress deadline in seconds')
    parser.add_argument('--controller-loss-gate-timeout', type=float,
                        help='deadline for observing the point; default: 2 * deadline + 600 s')
    parser.add_argument('--controller-loss-probe-seconds', type=float, default=1.0,
                        help='live sample interval while waiting for the point')
    parser.add_argument('run_args', nargs=argparse.REMAINDER,
                        help='extra run.py arguments after --')
    args = parser.parse_args()
    expected_transport = campaign_manifest.TOPOLOGY['control_transport']
    if args.transport != expected_transport:
        raise SystemExit(f'--transport {args.transport} differs from the campaign '
                         f'topology {expected_transport}')
    kernel_gate(args.fleet_state, args.live_transport)
    hosts = plan(args.coordinator, args.owners.split(','))
    output = args.output.resolve()
    if args.controller_loss is not None:
        try:
            controller_loss_admission(args.controller_loss, args.durability_tier)
            args.cell_id = controller_loss_cell_id(
                args.stage, args.controller_loss, args.durability_tier, args.cell_id)
        except ValueError as error:
            raise SystemExit(str(error)) from None
        if not 1 <= args.controller_loss_owner <= 2:
            raise SystemExit('--controller-loss-owner must be 1 or 2')
    cell = ledger_module.cell(args.cell_id) if args.cell_id else None
    if cell is not None:
        cell_row_checks(cell, args)
    elif args.bucket_prefix:
        # One reviewed id per cell: two different cells must never share an id
        # and be skipped as one, and a campaign result needs its declared knobs.
        raise SystemExit(
            f'{args.cell_id or "no --cell-id"} is not a reviewed manual_sui '
            'cell; --list prints the reviewed ids')
    if not args.gcloud_ssh and not args.on_coordinator:
        # The replay runs on H0: run.py starts the producer, the native control
        # and the remote coordinator, and they must reach the owners.
        files = ([('resident-reads.json', args.resident_reads)]
                 if args.resident_reads is not None else [])
        return mailbox_exec.dispatch(
            args, coordinator=args.coordinator,
            script_relative='experiments/gcp-rdma/manual_sui.py',
            trees=[('build', args.build), ('fixture', args.fixture), ('schema', args.schema)],
            files=files,
            values={'--build': 'build', '--fixture': 'fixture', '--schema': 'schema',
                    '--output': Path('out') / args.output.name,
                    '--ledger': 'campaign-ledger.json',
                    '--resident-reads': 'resident-reads.json'})
    if not args.bucket_prefix and not args.allow_local_ledger:
        raise SystemExit('a passed cell needs a verified evidence upload; give '
                         '--bucket-prefix, or --allow-local-ledger for a local rehearsal')
    store = bucket = prefix = None
    if args.bucket_prefix:
        store, bucket, prefix = upload_module.open_store(
            args.bucket_prefix, args.impersonation_account, guest=args.guest_token)
    ledger_path = args.ledger or output.with_name(output.name + '-ledger.json')
    resume_ledger = ledger_module.Ledger(ledger_path)
    # The bucket ledger is the campaign record: load and merge it on every run,
    # not only with --resume, so a passed cell is never rerun on a new fleet.
    if Path(ledger_path).exists():
        resume_ledger.load()
    if store is not None:
        resume_ledger = upload_module.Evidence(store, prefix).merge(resume_ledger)
    cell_id = args.cell_id or 'sui-replay'
    previous = resume_ledger.entries.get((args.stage, cell_id))
    attempt = (previous['attempts'] + 1) if previous else 1
    if args.on_coordinator or args.gcloud_ssh:
        # A retry derives its own attempt-scoped tree; the base name still names
        # the cell, and the ledger path stays stable (F20).
        output = attempt_output(output, attempt)
    remote = mailbox_exec.open_executor(args)
    controller_loss = None
    if args.controller_loss is not None:
        owner = args.controller_loss_owner - 1
        service, service_max = live_window(owner)
        _control, live = fleet_hosts(getattr(remote, 'fleet', None), hosts['owners'],
                                     transport=args.transport,
                                     live_transport=args.live_transport)
        rpc_address, metrics_address = controller_loss_addresses(args.fixture)
        controller_loss = ControllerLoss(
            point=args.controller_loss,
            host=args.controller_loss_host or hosts['coordinator'],
            address=live[owner], service=service, service_max=service_max,
            transport=args.live_transport,
            metrics_url='http://' + metrics_address + '/metrics',
            rpc_address=rpc_address, output=output, nqn=args.controller_loss_nqn,
            timeout=args.controller_loss_timeout,
            gate_timeout=(args.controller_loss_gate_timeout
                          if args.controller_loss_gate_timeout is not None
                          else 2 * args.deadline + 600.0),
            probe_seconds=args.controller_loss_probe_seconds,
            not_reached=(args.controller_loss == 'during-flush'
                         and args.durability_tier == 'synchronous'))
    launcher = SuiLauncher(
        remote=remote,
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
        ledger=resume_ledger, cell_id=cell_id, stage=args.stage,
        resume=args.resume, store=store, bucket=bucket, prefix=prefix,
        write_profile_limit=args.write_profile_limit,
        resident_reads=args.resident_reads,
        allow_local_ledger=args.allow_local_ledger,
        controller_loss=controller_loss, hold_seconds=args.hold_seconds)
    if controller_loss is not None:
        controller_loss.intents = controller_loss_intents(args, launcher)
    receipt = launcher.run(hosts)
    print(json.dumps(receipt))
    if receipt.get('status') in ('not_reached', 'invalid'):
        # A required cell that cannot start or reach its point is not a clean
        # stage result (F22).
        return 1 if cell is not None else 0
    return 0


if __name__ == '__main__':
    sys.exit(main())
