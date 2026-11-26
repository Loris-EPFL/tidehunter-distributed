#!/usr/bin/env python3
"""Free local rehearsal of the campaign launch path.

Three containers from the Rocky 9 (glibc 2.34) build image act as H0, H1 and H2
on two Docker networks. Every guest runs the real `gcp_worker.serve`; only the
GCE metadata identity, the GCE instance describe and the Local SSD admission are
local (see `rehearse_guest.py`). The laptop runs the real operator sequence:

  1. publish the sealed package and the runtime, then release every gate
     (`mailbox_exec.publish` / `mailbox_exec.release`);
  2. re-create on the same run id: a second package with other archive bytes is
     refused while the dead attempt's `package.tar` exists, then the run prefix
     is purged with `create`'s control-object predicate and the publish
     succeeds (`sweep_prefix`);
  3. start the three guests and wait for every worker's readiness object
     (`MailboxExec.wait_workers`);
  4. dispatch `manual_sui.py` S1-0 to H0, which stages the build, fixture and
     schema through the mailbox, runs `prepare_external.py` on the guest,
     installs the owner archives and starts the owner process groups. The
     admission cell runs on both tiers (`S1-0-native`, `S1-0-synchronous`);
  5. dispatch one `manual_sui.py` controller-loss cell, the stage-2 M5-hold
     cell, two `fault_cells.py` cells and the `stage3_campaign.py` cluster
     probe. The first fault cell publishes to the store, so its failed attempt
     must leave attempt-scoped evidence and a `failed` bucket ledger; the
     stage-3 driver prepares both tier externals on H0 itself;
  6. optionally exercise the idle guard (`--cells idle_stop`): a request longer
     than the test idle window holds every guest up, and every guest records
     its poweroff after the request ends and the window expires. The poweroff
     is a recorded action inside each container, not a real stop.

The containers have no nvmet, so every cell must stop exactly at kernel NVMe-oF
provisioning. A cell passes only when it reached the owner start and an owner
log names the missing kernel facility. The run is free: the store is local.

The store is a directory held by a local HTTP server that speaks the GCS JSON
API the real `gcp_api.ObjectStore` uses, so publish, the mailbox, transfers,
receipts and generations are all the production code path.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import socket
import subprocess
import sys
import tarfile
import time
import urllib.request

HERE = Path(__file__).resolve().parent
OWNER_CONTROL_PORT = 9400
ENGINE = HERE.parents[1]
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ENGINE / 'experiments' / 'kv-comparison'))
sys.path.insert(0, str(ENGINE / 'experiments' / 'sui-checkpoint'))

import campaign_ledger
import campaign_manifest
import gcp_api
import gcp_manual_compute
import mailbox_exec
import stage2_mechanism

BUCKET = 'mysten-research-bench-artifacts'
PROJECT = 'mysten-research'
PROJECT_NUMBER = '514867804006'
ZONE = 'us-central1-a'
MACHINE_TYPE = 'z4m-highmem-16'
BASE_IMAGE = 'tidehunter-gcp-build:latest'
IMAGE = 'tidehunter-rehearsal:latest'
CONTROL_SUBNET = '10.90.0.0/24'
DATA_SUBNET = '10.91.0.0/24'
GUEST_ROOT = Path('/mnt/tidehunter/sui')
# An owner that reaches kernel NVMe-oF provisioning names one of these in its
# log before it exits. The rehearsal stops at that exact wall.
WALL_MARKERS = ('nvmet', 'configfs', 'rdma_cm', 'nldev', 'libibverbs',
                'No such device', 'Protocol not supported', 'Operation not permitted')


class Failure(RuntimeError):
    pass


def run(argv, *, check=True, capture=True, env=None):
    result = subprocess.run([str(item) for item in argv], text=True,
                            capture_output=capture, env=env, timeout=1800)
    if check and result.returncode != 0:
        raise Failure('command failed: ' + ' '.join(map(str, argv)) + '\n'
                      + (result.stderr or '')[-4000:])
    return result


def docker(*argv, check=True):
    return run(['docker', *argv], check=check)


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def second_package(source, destination):
    """A second sealed package of the same tree, with different bundle bytes.

    The run id is derived from the campaign, so a re-create publishes over the
    prefix the dead attempt left. `package.tar` is immutable by bytes
    (`mailbox_exec.upload_identical`), so a rebuild that seals a different
    archive must find that object purged first. Re-encoding the bundle keeps the
    receipt's verified fields (source, binary, ABI, manifest) equal to the
    admitted directory and changes only the archive bytes and its `sha256`.
    """
    source, destination = Path(source).resolve(), Path(destination)
    if destination.exists():
        raise Failure('the second package destination already exists: ' + str(destination))
    # Admission re-reads the sealed runtime directory and the qualification
    # inventory beside it; both must travel with the receipt.
    for name in ('package', 'qualification'):
        shutil.copytree(source / name, destination / name)
    receipt = json.loads((source / 'package-receipt.json').read_text())
    bundle = destination / 'package' / 'bundle.tar.gz'
    bundle.unlink()
    with tarfile.open(source / 'package/bundle.tar.gz', 'r:gz') as old, \
            tarfile.open(bundle, 'w:gz', compresslevel=1) as new:
        for member in old.getmembers():
            new.addfile(member, old.extractfile(member))
    receipt['sha256'] = sha256_file(bundle)
    receipt['archive_path'] = str(bundle.resolve())
    (destination / 'package-receipt.json').write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + '\n')
    return destination


def purge_control_objects(store, run_id):
    """Delete the run-prefix control objects a gone attempt left behind.

    `gcp_manual_compute.Compute.sweep_prefix` does exactly this once it has
    proved no instance or boot disk of the run exists: the package, runtime and
    transfer objects and the per-host gate and readiness objects. The rehearsal
    has no cloud instance to prove absent, so it applies the same predicate to
    the same store.
    """
    removed = []
    for item in store.list('runs/' + run_id + '/'):
        name = item.get('name')
        if not gcp_manual_compute._control_object(name, run_id):
            continue
        store.delete(name, if_generation_match=item['generation'])
        removed.append(name)
    return sorted(removed)


class Rehearsal:
    def __init__(self, args):
        self.args = args
        self.work = Path(args.work).resolve()
        self.work.mkdir(parents=True, exist_ok=True)
        self.tag = args.tag
        self.networks = {'control': f'tidehunter-rehearsal-control-{self.tag}',
                         'data': f'tidehunter-rehearsal-data-{self.tag}'}
        self.containers = []
        self.state_path = self.work / 'fleet-state.json'
        self.config_path = self.work / 'config.json'
        self.results = []
        self.server = None
        self.gateway = None
        self.deadline = None
        self.run_id = 'bench-rehearsal-' + self.tag
        self.store = None
        self.config = None
        self.package = Path(args.package)
        # The guests mount the sealed libfabric prefix from the first package:
        # a re-create rebinds the archive and runtime, never the fabric.
        self.fabric = Path(args.package) / 'fabric'

    # ---- reporting -----------------------------------------------------
    def step(self, name, ok, detail=''):
        mark = 'PASS' if ok else 'FAIL'
        print(f'[{mark}] {name}' + (f': {detail}' if detail else ''), flush=True)
        self.results.append(dict(step=name, ok=ok, detail=detail))
        if not ok:
            raise Failure(f'{name}: {detail}')

    # ---- preflight -----------------------------------------------------
    def stage_build(self):
        """Freeze this branch's sources and pair them with the guest binaries.

        The staged build binds its source inventory: `run.py` refuses a tree
        whose files changed. The rehearsal re-freezes the branch with the real
        `build.py prepare` and reuses the guest-compatible binaries built from
        the same engine commit, so the drivers under test are this branch and
        the executables stay the admitted guest build.
        """
        target = self.work / 'build'
        source = Path(self.args.build).resolve()
        if not (target / 'build-manifest.json').exists():
            old = json.loads((source / 'build-manifest.json').read_text())
            command = [sys.executable, ENGINE / 'experiments/sui-checkpoint/build.py',
                       'prepare', '--sui', self.args.sui, '--engine', ENGINE,
                       '--output', target, '--jobs', '2']
            if old.get('fabric'):
                command.append('--fabric')
            run(command)
            plan = json.loads((target / 'plan.json').read_text())
            shutil.copytree(source / 'binaries', target / 'binaries')
            manifest = dict(plan, commands=old['commands'], binaries=old['binaries'],
                            environment=old.get('environment', {}),
                            resolved_locks=old.get('resolved_locks', {}),
                            executable_binding=old.get('executable_binding'))
            if 'guest_abi' in old:
                manifest['guest_abi'] = old['guest_abi']
            (target / 'build-manifest.json').write_text(json.dumps(manifest))
        self.build = target
        self.step('build staging', True,
                  str(target) + ' (branch sources re-frozen, guest binaries reused)')

    def clean_work(self):
        """Remove an earlier run's tree, including root-owned container files."""
        if not self.work.exists():
            return
        run(['docker', 'run', '--rm', '-v', str(self.work.parent) + ':/w', IMAGE,
             'sh', '-c', 'rm -rf /w/' + self.work.name])
        self.step('work tree', True, str(self.work) + ' (cleaned)')

    def preflight(self):
        missing = [str(path) for path in
                   (self.args.build, self.args.fixture, self.args.schema,
                    self.package, self.package / 'package-receipt.json')
                   if not Path(path).exists()]
        if missing:
            raise Failure('missing launch inputs: ' + ', '.join(missing))
        docker('image', 'inspect', BASE_IMAGE)
        if run(['docker', 'image', 'inspect', IMAGE], check=False).returncode != 0:
            run(['docker', 'build', '-f', str(HERE / 'Dockerfile.rehearsal'),
                 '-t', IMAGE, str(HERE)])
        self.step('preflight', True,
                  'image ' + run(['docker', 'image', 'inspect', '-f',
                                  '{{.Id}}', IMAGE]).stdout.strip()[:19])

    # ---- store ---------------------------------------------------------
    def start_store(self):
        import rehearse_store
        self.server = rehearse_store.Server(self.work / 'store', BUCKET,
                                            host='0.0.0.0', port=0)
        self.server.__enter__()
        endpoint = 'http://127.0.0.1:' + str(self.server.port)
        os.environ[gcp_api.REHEARSAL_ENDPOINT] = endpoint
        self.local_endpoint = endpoint
        self.step('local store', True, endpoint + ' (GCS JSON API, one bucket)')

    def make_networks(self):
        for kind, subnet in (('control', CONTROL_SUBNET), ('data', DATA_SUBNET)):
            docker('network', 'create', '--subnet', subnet, self.networks[kind])
        self.gateway = run(['docker', 'network', 'inspect', '-f',
                            '{{(index .IPAM.Config 0).Gateway}}',
                            self.networks['control']]).stdout.strip()

    def fleet(self):
        hosts = []
        for index in range(3):
            hosts.append({'index': index,
                          'role': 'coordinator' if index == 0 else 'owner',
                          'name': self.run_id + '-' + str(index),
                          'id': str(9100 + index),
                          'zone': ZONE,
                          'internal_ip': '10.90.0.' + str(11 + index),
                          'rdma_ip': '10.91.0.' + str(11 + index)})
        return hosts

    def start_guests(self, fleet, deadline):
        guest_endpoint = 'http://' + self.gateway + ':' + str(self.server.port)
        for host in fleet:
            directory = self.work / 'guests' / host['name']
            (directory / 'mnt').mkdir(parents=True, exist_ok=True)
            identity = dict(host, project_id=PROJECT, project_number=PROJECT_NUMBER,
                            bucket=BUCKET, run_id=self.run_id,
                            source_sha256=self.source_sha256,
                            deadline_utc=deadline)
            (directory / 'identity.json').write_text(json.dumps(identity))
            docker('run', '-d', '--name', host['name'],
                   '--network', self.networks['control'], '--ip', host['internal_ip'],
                   '--hostname', host['name'],
                   '--ulimit', 'nofile=1048576:1048576',
                   '-v', str(ENGINE) + ':/rehearsal/src:ro',
                   '-v', str(directory) + ':/rehearsal/work',
                   '-v', str(directory / 'mnt') + ':/mnt/tidehunter',
                   # The guest image installs libfabric 1.50 into the default
                   # loader path; the rehearsal mounts the sealed prefix there,
                   # because the worker replaces LD_LIBRARY_PATH with the
                   # package's own runtime/lib.
                   '-v', str(self.fabric) + ':/rehearsal/fabric:ro',
                   '-v', str(self.fabric / 'lib/libfabric.so.1')
                   + ':/usr/lib64/libfabric.so.1:ro',
                   '-v', str(self.fabric / 'bin/fi_info')
                   + ':/usr/local/bin/fi_info:ro',
                   '-e', 'REHEARSAL_IDENTITY=/rehearsal/work/identity.json',
                   '-e', gcp_api.REHEARSAL_ENDPOINT + '=' + guest_endpoint,
                   IMAGE, '/opt/tidehunter-python/bin/python3',
                   '/rehearsal/src/experiments/gcp-rdma/rehearse_guest.py')
            docker('network', 'connect', '--ip', host['rdma_ip'],
                   self.networks['data'], host['name'])
            self.containers.append(host['name'])

    # ---- operator sequence ---------------------------------------------
    def publish(self):
        receipt = json.loads((self.package / 'package-receipt.json').read_text())
        derived = gcp_manual_compute.package_source_sha256(self.package)
        if derived != receipt['source_sha256']:
            raise Failure('the package receipt source is not self-consistent')
        self.source_sha256 = derived
        self.deadline = time.strftime('%Y-%m-%dT%H:%M:%SZ',
                                      time.gmtime(time.time() + self.args.hours * 3600))
        self.spec = {'suite': 'sui', 'mode': 'sui', 'owners': 2, 'backends': ['native']}
        self.fleet_hosts = self.fleet()
        store = self.store = gcp_api.campaign_client(
            {'project_id': PROJECT}).objects(BUCKET)
        state = {'run_id': self.run_id, 'campaign': 'rehearsal', 'role': 'campaign',
                 'source_sha256': self.source_sha256, 'deadline': self.deadline,
                 'host_count': 3, 'bucket': BUCKET, 'owner_nonce': '0' * 32,
                 'instances': [{'name': host['name'], 'fleet': host}
                               for host in self.fleet_hosts],
                 'workers_released': []}
        self.state = state
        self.state_path.write_text(json.dumps(state, indent=2) + '\n')
        self.config_path.write_text(json.dumps(
            {'project_id': PROJECT, 'project_number': PROJECT_NUMBER,
             'bucket': BUCKET, 'service_account':
                 'bench-guest@mysten-research.iam.gserviceaccount.com',
             'machine_type': MACHINE_TYPE,
             'idle_stop_seconds': self.args.idle_seconds}, indent=2) + '\n')
        config = self.config = json.loads(self.config_path.read_text())
        # The receipt source is the fleet identity; a build-bundle SHA would
        # bind every gate to an identity no guest can match.
        runtime, reference = mailbox_exec.publish(
            store, config, state, self.package, self.args.revision,
            self.spec, self.fleet_hosts, guest_roots=[str(GUEST_ROOT)])
        script = mailbox_exec.gate_script(BUCKET, state)
        for host in self.fleet_hosts:
            mailbox_exec.release(store, state, host, script)
        # A second release must reuse the identical gate instead of failing 412.
        mailbox_exec.release(store, state, self.fleet_hosts[0], script)
        again, same = mailbox_exec.publish(store, config, state, self.package,
                                           self.args.revision, self.spec,
                                           self.fleet_hosts, guest_roots=[str(GUEST_ROOT)])
        if again != runtime or same['generation'] != reference['generation']:
            raise Failure('a repeated publish changed the runtime identity')
        self.step('publish and release', True,
                  'source ' + self.source_sha256[:12] + ', runtime generation '
                  + reference['generation'] + ', three gates (idempotent retry)')

    def recreate(self):
        """A dead attempt's prefix must be purged before a different package.

        The first attempt published its archive under the deterministic run id.
        A re-create whose new package holds other bytes is refused by the
        immutable `package.tar` (`upload_identical`), and the earlier gate and
        readiness objects would make the fresh guests exit before their first
        poll. `create` calls `sweep_prefix` once the fleet is verifiably gone;
        the rehearsal applies the same predicate, then publishes the second
        package and rebinds every gate to it.
        """
        second = second_package(self.package, self.work / 'package-second')
        state = self.state
        refused = None
        try:
            mailbox_exec.publish(self.store, self.config, state, second,
                                 self.args.revision, self.spec, self.fleet_hosts,
                                 guest_roots=[str(GUEST_ROOT)])
        except mailbox_exec.MailboxError as error:
            refused = str(error)
        if refused is None or 'immutable blob already holds other bytes' not in refused:
            raise Failure('a re-create with other package bytes was not refused: '
                          + str(refused))
        removed = purge_control_objects(self.store, self.run_id)
        expected = {'runs/' + self.run_id + '/package.tar',
                    'runs/' + self.run_id + '/runtime.json'}
        if not expected <= set(removed):
            raise Failure('the prefix purge missed the attempt control objects: '
                          + ', '.join(sorted(expected - set(removed))))
        _runtime, reference = mailbox_exec.publish(
            self.store, self.config, state, second, self.args.revision, self.spec,
            self.fleet_hosts, guest_roots=[str(GUEST_ROOT)])
        package_object = json.loads(self.store.get('runs/' + self.run_id + '/package.json'))
        receipt = json.loads((second / 'package-receipt.json').read_text())
        if package_object.get('sha256') != receipt['sha256']:
            raise Failure('the re-create kept the dead attempt package identity')
        script = mailbox_exec.gate_script(BUCKET, state)
        for host in self.fleet_hosts:
            mailbox_exec.release(self.store, state, host, script)
        self.package = second
        self.step('re-create after prefix purge', True,
                  'refused ' + refused[:60] + '...; purged ' + str(len(removed))
                  + ' objects; runtime generation ' + reference['generation'])

    def start(self):
        self.start_guests(self.fleet_hosts, self.deadline)
        self.step('guests started', True,
                  ', '.join(host['name'] for host in self.fleet_hosts))

    def ready(self, timeout):
        api = gcp_api.campaign_client({'project_id': PROJECT})
        executor = mailbox_exec.MailboxExec.from_state(
            self.state_path, api, bucket=BUCKET, timeout_seconds=timeout)
        prepared = executor.wait_workers(require=json.loads(
            (self.package / 'package-receipt.json').read_text())['manifest_sha256'])
        self.step('worker ready', True, ', '.join(sorted(prepared)))

    # ---- cells ---------------------------------------------------------
    def cell_env(self):
        env = dict(os.environ)
        env[gcp_api.REHEARSAL_ENDPOINT] = self.local_endpoint
        return env

    def reset_cell(self, output_name, *extra):
        """Remove the previous cell's guest roots and outputs.

        `wall()` accepts an owner log only when this cell produced it; a stale
        log from an earlier cell would otherwise pass a cell that never started.
        """
        for host in self.fleet_hosts[1:]:
            docker('exec', host['name'], 'rm', '-rf', str(GUEST_ROOT))
        campaign = f'/mnt/tidehunter/{self.run_id}/campaign'
        targets = [campaign + '/out/' + output_name,
                   campaign + '/out/' + output_name + '-external-config',
                   campaign + '/out/' + output_name + '-stages',
                   campaign + '/out/' + output_name + '-resources.json']
        targets += [campaign + '/' + name for name in extra]
        targets.append(campaign + '/campaign-ledger.json')
        docker('exec', self.fleet_hosts[0]['name'], 'rm', '-rf', *targets)

    def dispatch(self, cell, argv, timeout):
        print('$ ' + ' '.join(shlex.quote(str(part)) for part in argv), flush=True)
        result = subprocess.run([str(part) for part in argv], text=True,
                                capture_output=True, env=self.cell_env(),
                                timeout=timeout)
        print((result.stdout or '')[-8000:], flush=True)
        print((result.stderr or '')[-8000:], file=sys.stderr, flush=True)
        return result

    def guest_mount(self, host):
        """One guest's `/mnt/tidehunter` as seen from the laptop."""
        return self.work / 'guests' / host['name'] / 'mnt'

    def campaign_mount(self):
        """H0's staged campaign directory as seen from the laptop."""
        return self.guest_mount(self.fleet_hosts[0]) / self.run_id / 'campaign'

    def owner_logs(self):
        lines = []
        root_relative = GUEST_ROOT.relative_to('/mnt/tidehunter')
        for host in self.fleet_hosts[1:]:
            root = self.guest_mount(host) / root_relative
            for log in sorted(root.glob('*.log')):
                text = log.read_text(errors='replace')
                lines.append((host['name'], log.name, text))
        return lines

    def experiment_logs(self):
        """The coordinator's replay logs on H0, read through the guest image.

        The guest writes them as root, so the laptop reads them with a container
        that mounts the same campaign directory read-only.
        """
        campaign = self.campaign_mount()
        if not campaign.is_dir():
            return []
        result = run(['docker', 'run', '--rm', '-v', str(campaign) + ':/c:ro', IMAGE,
                      'sh', '-c', 'for f in /c/out/*/experiment/*.log; do '
                                  'echo "== $f"; cat "$f"; done'], check=False)
        return result.stdout or ''

    def wall(self, cell, result):
        """A cell passes only when it stopped at kernel NVMe-oF provisioning.

        The owners start before the engine provisions the target, so the failure
        appears in the coordinator's replay log on H0 (`nvmet` on `configfs`),
        and can also appear in an owner log or the printed output. Another
        failure after owner start stays a failure.
        """
        logs = self.owner_logs()
        if not logs:
            return False, 'no owner log: the cell did not reach the owner lifecycle'
        sources = [(f'{host}/{name}', text) for host, name, text in logs]
        sources.append(('H0 output', (result.stdout or '') + (result.stderr or '')))
        sources.append(('H0 experiment logs', self.experiment_logs()))
        for label, text in sources:
            for marker in WALL_MARKERS:
                if marker in text:
                    line = next(row for row in text.splitlines() if marker in row)
                    return True, (f'stopped at kernel NVMe-oF provisioning '
                                  f'({label}): {line.strip()[:200]}')
        tail = ((result.stdout or '') + (result.stderr or '')).strip().splitlines() or ['<empty>']
        return False, 'no kernel NVMe-oF failure; last lines: ' + ' | '.join(tail[-3:])

    def manual_sui(self):
        output = self.work / 'cells' / 'manual_sui'
        self.reset_cell('manual_sui')
        argv = [sys.executable, HERE / 'manual_sui.py',
                '--build', self.build, '--fixture', self.args.fixture,
                '--schema', self.args.schema, '--output', output,
                '--guest-root', GUEST_ROOT, '--coordinator', self.fleet_hosts[0]['name'],
                '--owners', ','.join(host['name'] for host in self.fleet_hosts[1:]),
                '--transport', 'tcp', '--live-transport', 'rdma',
                '--cell-id', 'S1-0-native', '--stage', 'stage1',
                '--fleet-state', self.state_path, '--fleet-config', self.config_path,
                '--allow-local-ledger', '--coordinator-seconds', str(self.args.cell_seconds),
                '--', '--checkpoints', '4']
        result = self.dispatch('manual_sui S1-0-native', argv, self.args.cell_seconds + 300)
        ok, detail = self.wall('manual_sui', result)
        self.step('manual_sui S1-0-native (checkpoints 4)', ok, detail)
        self.check_launch_addresses('manual_sui')

    def manual_sui_synchronous(self):
        """The synchronous admission cell: the first FUA path on this image."""
        output = self.work / 'cells' / 'manual_sui_synchronous'
        self.reset_cell('manual_sui_synchronous')
        argv = [sys.executable, HERE / 'manual_sui.py',
                '--build', self.build, '--fixture', self.args.fixture,
                '--schema', self.args.schema, '--output', output,
                '--guest-root', GUEST_ROOT, '--coordinator', self.fleet_hosts[0]['name'],
                '--owners', ','.join(host['name'] for host in self.fleet_hosts[1:]),
                '--transport', 'tcp', '--live-transport', 'rdma',
                '--durability-tier', 'synchronous',
                '--cell-id', 'S1-0-synchronous', '--stage', 'stage1',
                '--fleet-state', self.state_path, '--fleet-config', self.config_path,
                '--allow-local-ledger', '--coordinator-seconds', str(self.args.cell_seconds),
                '--', '--checkpoints', '4']
        result = self.dispatch('manual_sui S1-0-synchronous', argv,
                               self.args.cell_seconds + 300)
        ok, detail = self.wall('manual_sui_synchronous', result)
        self.step('manual_sui S1-0-synchronous (checkpoints 4)', ok, detail)
        self.check_launch_addresses('manual_sui_synchronous')

    def manual_sui_controller_loss(self):
        """Dispatch S1-4 mid-frame (native) to the same wall.

        The injector gate polls a live Sui RPC, so the cell cannot reach a
        checkpoint here; the point of the rehearsal is that admission, cell id
        and injector plumbing dispatch, and that the owner lifecycle stops at
        the same kernel wall first.
        """
        output = self.work / 'cells' / 'manual_sui_loss'
        self.reset_cell('manual_sui_loss')
        argv = [sys.executable, HERE / 'manual_sui.py',
                '--build', self.build, '--fixture', self.args.fixture,
                '--schema', self.args.schema, '--output', output,
                '--guest-root', GUEST_ROOT, '--coordinator', self.fleet_hosts[0]['name'],
                '--owners', ','.join(host['name'] for host in self.fleet_hosts[1:]),
                '--transport', 'tcp', '--live-transport', 'rdma',
                '--fleet-state', self.state_path, '--fleet-config', self.config_path,
                '--allow-local-ledger', '--coordinator-seconds', str(self.args.cell_seconds),
                '--controller-loss', 'mid-frame', '--durability-tier', 'native',
                '--cell-id', 'S1-4-mid-frame-native', '--stage', 'stage1',
                '--', '--checkpoints', '4']
        result = self.dispatch('manual_sui S1-4', argv, self.args.cell_seconds + 300)
        ok, detail = self.wall('manual_sui S1-4', result)
        self.step('manual_sui S1-4 mid-frame native', ok, detail)
        self.check_launch_addresses('manual_sui_loss')

    def manual_sui_hold(self):
        """Dispatch the stage-2 M5-hold cell with its reviewed hold window.

        The owner lifecycle stops at the same kernel wall before any hold
        begins, so the reviewed 1800 s window never elapses here; the driver
        still admits the cell row (`hold-seconds`) and writes the disk samples.
        """
        output = self.work / 'cells' / 'manual_sui_hold'
        self.reset_cell('manual_sui_hold')
        argv = [sys.executable, HERE / 'manual_sui.py',
                '--build', self.build, '--fixture', self.args.fixture,
                '--schema', self.args.schema, '--output', output,
                '--guest-root', GUEST_ROOT, '--coordinator', self.fleet_hosts[0]['name'],
                '--owners', ','.join(host['name'] for host in self.fleet_hosts[1:]),
                '--transport', 'tcp', '--live-transport', 'rdma',
                '--fleet-state', self.state_path, '--fleet-config', self.config_path,
                '--allow-local-ledger', '--coordinator-seconds', str(self.args.cell_seconds),
                '--durability-tier', 'native', '--hold-seconds', '1800',
                '--disk-sample-seconds', '10',
                '--cell-id', 'M5-hold', '--stage', 'stage2',
                '--', '--checkpoints', '4']
        result = self.dispatch('manual_sui M5-hold', argv, self.args.cell_seconds + 300)
        ok, detail = self.wall('manual_sui_hold', result)
        self.step('manual_sui M5-hold (hold-seconds 1800)', ok, detail)
        self.check_launch_addresses('manual_sui_hold')

    def launch_files(self, wanted):
        """One cell's prepared `launch.yaml` files, read through the guest image.

        The guest creates its preparation as root with `0700`, so the laptop
        cannot traverse it; a container that mounts the campaign read-only can.
        """
        campaign = self.campaign_mount()
        if not campaign.is_dir():
            return []
        script = ('find /c -type f -name launch.yaml | while read f; do '
                  'case "$f" in *"/' + wanted + '/"*) echo "== $f"; cat "$f";; esac; done')
        result = run(['docker', 'run', '--rm', '-v', str(campaign) + ':/c:ro', IMAGE,
                      'sh', '-c', script], check=False)
        files, name, buffer = [], None, []
        for line in (result.stdout or '').splitlines():
            if line.startswith('== '):
                if name is not None:
                    files.append((name, '\n'.join(buffer)))
                name, buffer = line[3:], []
            elif name is not None:
                buffer.append(line)
        if name is not None:
            files.append((name, '\n'.join(buffer)))
        return files

    def check_launch_addresses(self, cell, config_dir=None):
        """The prepared launch must listen on the control IP and export the RDMA IP."""
        wanted = config_dir or (cell + '-external-config')
        files = self.launch_files(wanted)
        if not files:
            raise Failure(f'{cell}: no prepared owner launch file under a {wanted} directory')
        for name, text in files:
            marker = name.rsplit('/node-', 1)
            if len(marker) != 2 or not marker[1][:1].isdigit():
                raise Failure(f'{cell}: {name} is not a node launch file')
            partition = int(marker[1].split('/')[0])
            if not 0 <= partition < len(self.fleet_hosts) - 1:
                raise Failure(f'{cell}: {name} names partition {partition} outside the fleet')
            expected = self.fleet_hosts[1 + partition]
            listens = re.findall(r'^listen: (\S+):(\d+)$', text, flags=re.MULTILINE)
            if len(listens) != 1:
                raise Failure(f'{cell}: {name} names no single listen address')
            address, port = listens[0]
            port = int(port)
            owners = len(self.fleet_hosts) - 1
            # One owner listens on 9400 + database * owners + partition.
            if (address != expected['internal_ip'] or port < OWNER_CONTROL_PORT
                    or (port - OWNER_CONTROL_PORT - partition) % owners != 0):
                raise Failure(f'{cell}: {name} listens on {address}:{port}, not the owner '
                              f"control address {expected['internal_ip']} in the "
                              f'{OWNER_CONTROL_PORT}+2n+{partition} window')
            if f"address: {expected['rdma_ip']}" not in text:
                raise Failure(f'{cell}: {name} does not export the RDMA target '
                              f"address {expected['rdma_ip']}")

    def fault_cells(self):
        """Two fault cells; the first one passes through the store's failure path.

        A cell stops at the kernel wall before its family driver runs, so both
        attempts fail. The first runs with `--bucket-prefix` and no local-ledger
        escape: its failed receipt, specs and samples must be uploaded under the
        attempt-scoped keys and the bucket ledger must record `failed`. The
        second keeps the local ledger, so a rehearsal that never reaches the
        store still reports its wall.
        """
        for index, cell_id in enumerate(
                name.strip() for name in self.args.fault_cells.split(',') if name.strip()):
            prefix = 'gs://' + BUCKET + '/rehearsal-' + self.tag
            output = self.work / 'cells' / ('fault_cells_' + cell_id)
            self.reset_cell('fault_cells_' + cell_id, 'external')
            argv = [sys.executable, HERE / 'fault_cells.py',
                    '--cell', cell_id,
                    '--build', self.build, '--fixture', self.args.fixture,
                    '--schema', self.args.schema, '--output', output,
                    '--guest-root', GUEST_ROOT, '--coordinator', self.fleet_hosts[0]['name'],
                    '--owners', ','.join(host['name'] for host in self.fleet_hosts[1:]),
                    '--transport', 'tcp', '--live-transport', 'rdma',
                    '--fleet-state', self.state_path, '--fleet-config', self.config_path,
                    '--coordinator-seconds', str(self.args.cell_seconds)]
            argv += (['--bucket-prefix', prefix] if index == 0
                     else ['--allow-local-ledger'])
            result = self.dispatch('fault_cells ' + cell_id, argv,
                                   self.args.cell_seconds + 300)
            ok, detail = self.wall('fault_cells', result)
            self.step('fault_cells ' + cell_id, ok, detail)
            if index == 0:
                self.failure_evidence(cell_id, prefix)

    def failure_evidence(self, cell_id, bucket_prefix):
        """A failed attempt leaves its evidence and a `failed` ledger in the store."""
        bucket, prefix = campaign_manifest.parse_bucket_prefix(bucket_prefix)
        if bucket != BUCKET:
            raise Failure('the failure evidence bucket differs from the store')
        key = campaign_manifest.ledger_key(prefix)
        try:
            payload = self.store.get(key)
        except (gcp_api.ApiError, KeyError) as error:
            raise Failure(f'the failed attempt wrote no bucket ledger: {error}') from None
        ledger = campaign_ledger.Ledger.from_bytes(payload)
        stage = stage2_mechanism.find(cell_id)['stage']
        entry = ledger.entries.get((stage, cell_id))
        if entry is None or entry.get('status') != 'failed':
            raise Failure(f'{cell_id} has no failed ledger entry: {entry and entry.get("status")}')
        uploaded = (entry.get('failure_evidence') or {}).get('uploaded') or []
        if not uploaded:
            raise Failure(f'{cell_id} uploaded no failure evidence')
        wanted = f'/{stage}/evidence/{cell_id}/attempt-'
        names = {item.get('name') for item in self.store.list(prefix + '/')}
        if not any(wanted in (name or '') for name in names):
            raise Failure(f'{cell_id} has no attempt-scoped evidence object')
        self.step('failure evidence ' + cell_id, True,
                  f'{len(uploaded)} artifacts, ledger {key}')

    def stage3(self):
        output = self.work / 'cells' / 'stage3'
        self.reset_cell('root', 'external')
        argv = [sys.executable, ENGINE / 'experiments/sui-checkpoint/stage3_campaign.py',
                '--root', output, '--binary', Path(self.build) / 'binaries/storage-node',
                '--build', self.build, '--schema', self.args.schema,
                '--native-root', str(GUEST_ROOT / 'native'),
                '--guest-root', GUEST_ROOT,
                '--owners', ','.join(host['name'] for host in self.fleet_hosts[1:]),
                '--bucket-prefix', 'gs://' + BUCKET + '/rehearsal',
                '--fleet-state', self.state_path, '--fleet-config', self.config_path,
                '--mode', 'cluster', '--coordinator-seconds',
                str(self.args.cell_seconds)]
        result = self.dispatch('stage3_campaign', argv, self.args.cell_seconds + 300)
        ok, detail = self.wall('stage3', result)
        self.step('stage3_campaign cluster mode (tiers prepared on H0)', ok, detail)

    def stage3_database(self):
        manifests = sorted(Path(self.args.schema).rglob('manifest.json'))
        if not manifests:
            raise Failure('the schema names no database')
        return json.loads(manifests[0].read_text())['namespace']

    # ---- idle guard ----------------------------------------------------
    def guest_file(self, host, name):
        return self.work / 'guests' / host['name'] / name

    def poweroff_record(self, host):
        path = self.guest_file(host, 'poweroff.json')
        return json.loads(path.read_text()) if path.exists() else None

    def idle_stop(self):
        """A long request holds the fleet; an idle fleet records poweroff.

        The rehearsal window is intentionally short (`--idle-seconds`). The
        worker and the real decision code run unchanged; only the ACPI poweroff
        is a recorded action (`rehearse_guest.recorded_poweroff`).
        """
        idle = self.args.idle_seconds
        coordinator = self.fleet_hosts[0]['name']
        args = type('Args', (), {
            'gcloud_ssh': False, 'zone': ZONE, 'fleet_state': self.state_path,
            'fleet_config': self.config_path, 'guest_token': False,
            'iap': False, 'tunnel_through_iap': False})()
        remote = mailbox_exec.open_executor(args)
        hold = idle + 20
        marker = self.guest_file(self.fleet_hosts[0], 'long-request.started')
        reference = remote.submit(
            coordinator, f'echo started > /rehearsal/work/long-request.started; sleep {hold}',
            timeout_seconds=hold + 60)
        deadline = time.monotonic() + 60
        while not marker.exists():
            if time.monotonic() > deadline:
                raise Failure('the long request did not start')
            time.sleep(1)
        # The request must still be running when the idle window has passed.
        time.sleep(idle + 10)
        stopped = [host['name'] for host in self.fleet_hosts
                   if self.poweroff_record(host) is not None]
        self.step('idle stop: a long request holds the fleet', not stopped,
                  ('running=' + coordinator + ', no poweroff after '
                   + str(idle + 10) + ' s') if not stopped
                  else 'stopped during the request: ' + ', '.join(stopped))
        remote.finish(coordinator, reference)
        # H0 stops one window after its last activity; the owners observe a
        # stale heartbeat one poll window later, so allow two windows.
        deadline = time.monotonic() + 2 * idle + 120
        pending = {host['name'] for host in self.fleet_hosts}
        while pending and time.monotonic() < deadline:
            pending = {name for name in pending
                       if self.poweroff_record(next(h for h in self.fleet_hosts
                                                    if h['name'] == name)) is None}
            if pending:
                time.sleep(2)
        records = {host['name']: self.poweroff_record(host) for host in self.fleet_hosts}
        reasons = {name: (record or {}).get('reason') for name, record in records.items()}
        ok = not pending and all(reason == 'poweroff_idle' for reason in reasons.values())
        self.step('idle stop: every guest powered off when idle', ok,
                  ', '.join(f'{name}={reasons[name]}' for name in sorted(reasons)))

    # ---- teardown and summary ------------------------------------------
    def stop(self):
        for name in reversed(self.containers):
            docker('rm', '-f', name, check=False)
        for name in self.networks.values():
            docker('network', 'rm', name, check=False)
        if self.server is not None:
            self.server.__exit__(None, None, None)
            self.server = None

    def summary(self):
        failed = [row for row in self.results if not row['ok']]
        print('', flush=True)
        print('rehearsal summary', flush=True)
        for row in self.results:
            print(('  PASS  ' if row['ok'] else '  FAIL  ') + row['step']
                  + (': ' + row['detail'] if row['detail'] else ''), flush=True)
        print('', flush=True)
        print(('RESULT: PASS (' + str(len(self.results)) + ' steps)') if not failed
              else 'RESULT: FAIL (' + str(len(failed)) + ' failed steps)', flush=True)
        return 0 if not failed else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', type=Path,
                        default=Path(os.environ.get(
                            'REHEARSAL_BUILD',
                            '/home/hybr1d/Desktop/PDM/TideHunter/checkpoint-work/'
                            'cluster-campaign-2026-11-25/build-guest3')))
    parser.add_argument('--fixture', type=Path,
                        default=Path('/home/hybr1d/Desktop/PDM/TideHunter/'
                                     'checkpoint-fixtures/sui-genesis-1775/fixture'))
    parser.add_argument('--schema', type=Path,
                        default=Path('/home/hybr1d/Desktop/PDM/TideHunter/checkpoint-work/'
                                     'software-startup-repro-2026-10-24/'
                                     'replay-final-registered-software-clean/'
                                     'experiment/native-schema'))
    parser.add_argument('--package', type=Path,
                        default=Path('/home/hybr1d/Desktop/PDM/TideHunter/checkpoint-work/'
                                     'cluster-campaign-2026-11-25/package3'))
    parser.add_argument('--revision', help='source revision; defaults to the engine HEAD')
    parser.add_argument('--sui', type=Path,
                        default=Path('/home/hybr1d/Desktop/PDM/TideHunter/'
                                     'scratch/SuiPinFinal/sui'))
    parser.add_argument('--work', type=Path,
                        default=Path('/home/hybr1d/Desktop/PDM/TideHunter/checkpoint-work/'
                                     'cluster-campaign-2026-11-25/rehearsal-local'))
    parser.add_argument('--tag', default=time.strftime('%H%M%S'))
    parser.add_argument('--hours', type=float, default=4)
    parser.add_argument('--cell-seconds', type=int, default=1800,
                        help='bound of one dispatched coordinator request')
    parser.add_argument('--ready-seconds', type=int, default=1800)
    parser.add_argument('--fault-cells', default='S1-2S-publication-native,M6-w1-c16',
                        help='comma-separated fault cells; the first uploads its '
                             'failure evidence to the store, the rest keep the ledger local')
    parser.add_argument('--cells',
                        default=('manual_sui,manual_sui_synchronous,'
                                 'manual_sui_controller_loss,manual_sui_hold,'
                                 'fault_cells,stage3'),
                        help='comma-separated cells to run; add idle_stop for the '
                             'guest idle guard (use it alone with a short window)')
    parser.add_argument('--idle-seconds', type=int, default=None,
                        help='guest idle window; defaults to 60 for --cells idle_stop '
                             'and 1800 otherwise')
    parser.add_argument('--keep', action='store_true',
                        help='keep the containers and the store for inspection')
    args = parser.parse_args()
    if args.idle_seconds is None:
        args.idle_seconds = 60 if args.cells.strip() == 'idle_stop' else 1800
    if args.revision is None:
        args.revision = run(['git', '-C', str(ENGINE), 'rev-parse', 'HEAD']).stdout.strip()
    rehearsal = Rehearsal(args)
    code = 1
    try:
        rehearsal.preflight()
        rehearsal.clean_work()
        rehearsal.stage_build()
        rehearsal.start_store()
        rehearsal.make_networks()
        rehearsal.publish()
        rehearsal.recreate()
        rehearsal.start()
        rehearsal.ready(args.ready_seconds)
        for cell in args.cells.split(','):
            getattr(rehearsal, cell.strip())()
        code = rehearsal.summary()
    except Failure as error:
        print('\nFAILURE: ' + str(error), file=sys.stderr)
        rehearsal.results.append(dict(step='aborted', ok=False, detail=str(error)))
        rehearsal.summary()
    finally:
        if args.keep:
            print('kept: containers ' + ', '.join(rehearsal.containers)
                  + '; store ' + str(rehearsal.work / 'store'))
        else:
            rehearsal.stop()
    return code


if __name__ == '__main__':
    raise SystemExit(main())
