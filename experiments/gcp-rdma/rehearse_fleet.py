#!/usr/bin/env python3
"""Free local rehearsal of the campaign launch path.

Three containers from the Rocky 9 (glibc 2.34) build image act as H0, H1 and H2
on two Docker networks. Every guest runs the real `gcp_worker.serve`; only the
GCE metadata identity, the GCE instance describe and the Local SSD admission are
local (see `rehearse_guest.py`). The laptop runs the real operator sequence:

  1. publish the sealed package and the runtime, then release every gate
     (`mailbox_exec.publish` / `mailbox_exec.release`);
  2. wait for every worker's readiness object (`MailboxExec.wait_workers`);
  3. dispatch `manual_sui.py` S1-0 to H0, which stages the build, fixture and
     schema through the mailbox, runs `prepare_external.py` on the guest,
     installs the owner archives and starts the owner process groups;
  4. dispatch one `fault_cells.py` cell and the `stage3_campaign.py` cluster
     probe the same way.

The containers have no nvmet, so every cell must stop exactly at kernel NVMe-oF
provisioning. A cell passes only when it reached the owner start and an owner
log names the missing kernel facility. The run is free: the store is local.

The store is a directory held by a local HTTP server that speaks the GCS JSON
API the real `gcp_api.ObjectStore` uses, so publish, the mailbox, transfers,
receipts and generations are all the production code path.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import socket
import subprocess
import sys
import time
import urllib.request

HERE = Path(__file__).resolve().parent
OWNER_CONTROL_PORT = 9400
ENGINE = HERE.parents[1]
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ENGINE / 'experiments' / 'kv-comparison'))
sys.path.insert(0, str(ENGINE / 'experiments' / 'sui-checkpoint'))

import gcp_api
import gcp_manual_compute
import mailbox_exec

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
                    self.args.package, Path(self.args.package) / 'package-receipt.json')
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
                   '-v', str(Path(self.args.package) / 'fabric') + ':/rehearsal/fabric:ro',
                   '-v', str(Path(self.args.package) / 'fabric/lib/libfabric.so.1')
                   + ':/usr/lib64/libfabric.so.1:ro',
                   '-v', str(Path(self.args.package) / 'fabric/bin/fi_info')
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
        receipt = json.loads((Path(self.args.package) / 'package-receipt.json').read_text())
        derived = gcp_manual_compute.package_source_sha256(self.args.package)
        if derived != receipt['source_sha256']:
            raise Failure('the package receipt source is not self-consistent')
        self.source_sha256 = derived
        self.deadline = time.strftime('%Y-%m-%dT%H:%M:%SZ',
                                      time.gmtime(time.time() + self.args.hours * 3600))
        self.spec = {'suite': 'sui', 'mode': 'sui', 'owners': 2, 'backends': ['native']}
        self.fleet_hosts = self.fleet()
        self.start_guests(self.fleet_hosts, self.deadline)
        store = self.store = gcp_api.campaign_client(
            {'project_id': PROJECT}).objects(BUCKET)
        state = {'run_id': self.run_id, 'campaign': 'rehearsal', 'role': 'campaign',
                 'source_sha256': self.source_sha256, 'deadline': self.deadline,
                 'host_count': 3, 'bucket': BUCKET, 'owner_nonce': '0' * 32,
                 'instances': [{'name': host['name'], 'fleet': host}
                               for host in self.fleet_hosts],
                 'workers_released': []}
        self.state_path.write_text(json.dumps(state, indent=2) + '\n')
        self.config_path.write_text(json.dumps(
            {'project_id': PROJECT, 'project_number': PROJECT_NUMBER,
             'bucket': BUCKET, 'service_account':
                 'bench-guest@mysten-research.iam.gserviceaccount.com',
             'machine_type': MACHINE_TYPE}, indent=2) + '\n')
        config = json.loads(self.config_path.read_text())
        # The receipt source is the fleet identity; a build-bundle SHA would
        # bind every gate to an identity no guest can match.
        runtime, reference = mailbox_exec.publish(
            store, config, state, self.args.package, self.args.revision,
            self.spec, self.fleet_hosts, guest_roots=[str(GUEST_ROOT)])
        script = mailbox_exec.gate_script(BUCKET, state)
        for host in self.fleet_hosts:
            mailbox_exec.release(store, state, host, script)
        # A second release must reuse the identical gate instead of failing 412.
        mailbox_exec.release(store, state, self.fleet_hosts[0], script)
        again, same = mailbox_exec.publish(store, config, state, self.args.package,
                                           self.args.revision, self.spec,
                                           self.fleet_hosts, guest_roots=[str(GUEST_ROOT)])
        if again != runtime or same['generation'] != reference['generation']:
            raise Failure('a repeated publish changed the runtime identity')
        self.step('publish and release', True,
                  'source ' + self.source_sha256[:12] + ', runtime generation '
                  + reference['generation'] + ', three gates (idempotent retry)')

    def ready(self, timeout):
        api = gcp_api.campaign_client({'project_id': PROJECT})
        executor = mailbox_exec.MailboxExec.from_state(
            self.state_path, api, bucket=BUCKET, timeout_seconds=timeout)
        prepared = executor.wait_workers(require=json.loads(
            (Path(self.args.package) / 'package-receipt.json').read_text())['manifest_sha256'])
        self.step('worker ready', True, ', '.join(sorted(prepared)))

    # ---- cells ---------------------------------------------------------
    def cell_env(self):
        env = dict(os.environ)
        env[gcp_api.REHEARSAL_ENDPOINT] = self.local_endpoint
        return env

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
            for log in sorted(root.glob('owner-*.log')):
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
        if not logs or not any('storage_node_pid=' in text for _, _, text in logs):
            return False, 'no owner start: the cell did not reach the owner lifecycle'
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
        argv = [sys.executable, HERE / 'manual_sui.py',
                '--build', self.build, '--fixture', self.args.fixture,
                '--schema', self.args.schema, '--output', output,
                '--guest-root', GUEST_ROOT, '--coordinator', self.fleet_hosts[0]['name'],
                '--owners', ','.join(host['name'] for host in self.fleet_hosts[1:]),
                '--transport', 'tcp', '--live-transport', 'rdma',
                '--fleet-state', self.state_path, '--fleet-config', self.config_path,
                '--allow-local-ledger', '--coordinator-seconds', str(self.args.cell_seconds),
                '--', '--checkpoints', '4']
        result = self.dispatch('manual_sui S1-0', argv, self.args.cell_seconds + 300)
        ok, detail = self.wall('manual_sui', result)
        self.step('manual_sui S1-0 (checkpoints 4)', ok, detail)
        self.check_launch_addresses('manual_sui')

    def manual_sui_controller_loss(self):
        """Dispatch S1-4 mid-frame (native) to the same wall.

        The injector gate polls a live Sui RPC, so the cell cannot reach a
        checkpoint here; the point of the rehearsal is that admission, cell id
        and injector plumbing dispatch, and that the owner lifecycle stops at
        the same kernel wall first.
        """
        output = self.work / 'cells' / 'manual_sui_loss'
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
        output = self.work / 'cells' / 'fault_cells'
        argv = [sys.executable, HERE / 'fault_cells.py',
                '--cell', self.args.fault_cell,
                '--build', self.build, '--fixture', self.args.fixture,
                '--schema', self.args.schema, '--output', output,
                '--guest-root', GUEST_ROOT, '--coordinator', self.fleet_hosts[0]['name'],
                '--owners', ','.join(host['name'] for host in self.fleet_hosts[1:]),
                '--zone', ZONE, '--transport', 'tcp', '--live-transport', 'rdma',
                '--fleet-state', self.state_path, '--fleet-config', self.config_path,
                '--allow-local-ledger', '--coordinator-seconds', str(self.args.cell_seconds)]
        result = self.dispatch('fault_cells', argv, self.args.cell_seconds + 300)
        ok, detail = self.wall('fault_cells', result)
        self.step('fault_cells ' + self.args.fault_cell, ok, detail)

    def stage3(self):
        output = self.work / 'cells' / 'stage3'
        external = self.prepare_stage3_external(output)
        argv = [sys.executable, ENGINE / 'experiments/sui-checkpoint/stage3_campaign.py',
                '--root', output, '--binary', Path(self.build) / 'binaries/storage-node',
                '--external', 'native=' + str(external),
                '--native-root', str(GUEST_ROOT / 'native'),
                '--guest-root', GUEST_ROOT,
                '--owners', ','.join(host['name'] for host in self.fleet_hosts[1:]),
                '--zone', ZONE,
                '--bucket-prefix', 'gs://' + BUCKET + '/rehearsal',
                '--fleet-state', self.state_path, '--fleet-config', self.config_path,
                '--mode', 'cluster', '--coordinator-seconds',
                str(self.args.cell_seconds)]
        result = self.dispatch('stage3_campaign', argv, self.args.cell_seconds + 300)
        ok, detail = self.wall('stage3', result)
        self.step('stage3_campaign cluster mode', ok, detail)

    def prepare_stage3_external(self, output):
        """Prepare stage-3's external deployment with the branch's prepare_external."""
        directory = output / 'external'
        directory.mkdir(parents=True, exist_ok=True)
        database = self.stage3_database()
        control = [host['internal_ip'] for host in self.fleet_hosts[1:]]
        live = [host['rdma_ip'] for host in self.fleet_hosts[1:]]
        result = run([sys.executable, ENGINE / 'experiments/sui-checkpoint/prepare_external.py',
                      '--build', self.build, '--schema', self.args.schema,
                      '--output', directory, '--db-root', str(GUEST_ROOT / 'native'),
                      '--guest-root', GUEST_ROOT,
                      '--hosts', ','.join(control), '--live-hosts', ','.join(live),
                      '--port', '9400', '--transport', 'tcp', '--live-transport', 'rdma',
                      '--durability-tier', 'native'], check=True)
        return directory

    def stage3_database(self):
        manifests = sorted(Path(self.args.schema).rglob('manifest.json'))
        if not manifests:
            raise Failure('the schema names no database')
        return json.loads(manifests[0].read_text())['namespace']

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
                            'cluster-campaign-2026-11-25/build-guest')))
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
                                     'rehearsal-2026-11-26/package'))
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
    parser.add_argument('--fault-cell', default='M6-w1-c16')
    parser.add_argument('--cells',
                        default='manual_sui,manual_sui_controller_loss,fault_cells,stage3',
                        help='comma-separated cells to run')
    parser.add_argument('--keep', action='store_true',
                        help='keep the containers and the store for inspection')
    args = parser.parse_args()
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
