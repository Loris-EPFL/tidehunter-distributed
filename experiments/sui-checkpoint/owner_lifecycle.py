#!/usr/bin/env python3
"""One owner close and one exact-owned configfs cleanup for every driver.

The engine owns two close paths. A typed stop marker lets `serve` return, so
the owner tears down every live target and syncs a shutdown proof; SIGTERM has
no handler and kills the process before that teardown. This module therefore
touches the marker first, and only signals a group that did not leave after the
declared owner grace.

Between cells, `cleanup_host` removes only objects whose NQN starts with the
engine's prefix. It lists port subsystem links, subsystems and hosts, removes
port links before their subsystems, and disables then removes each namespace,
in the order `experiments/nvmeof/manage.py:target_cleanup` uses for one owned
target. The selection is the CI owner-layout scope
(`.github/scripts/kernel_nvme_tcp_ci.py`, `layout_teardown_defect`): a foreign
NQN is never touched. The command runs through the campaign executor, which is
root on an owner guest, and refuses to run a root-scoped deletion otherwise.
"""
import shlex
import time

NQN_PREFIX = 'nqn.2026-01.local.tidehunter:'
CONFIGFS = '/sys/kernel/config/nvmet'
OWNER_STOP_SECONDS = 90
OWNER_KILL_SECONDS = 10

CLEANUP_SCRIPT = """\
# tidehunter-owner-cleanup
set -u
root={root}
prefix={prefix}
require_root={require_root}
if [ ! -d "$root" ]; then
  printf 'UNAVAILABLE\\t%s\\n' "$root"
  exit 0
fi
if [ "$require_root" -eq 1 ] && [ "$(id -u)" -ne 0 ]; then
  printf 'ERROR\\towner cleanup requires root\\n'
  exit 2
fi
status=0
for link in "$root"/ports/*/subsystems/"$prefix"*; do
  [ -L "$link" ] || continue
  printf 'BEFORE\\tlink\\t%s\\n' "$link"
done
for sub in "$root"/subsystems/"$prefix"*; do
  [ -d "$sub" ] || continue
  printf 'BEFORE\\tsubsystem\\t%s\\n' "$sub"
done
for host in "$root"/hosts/"$prefix"*; do
  [ -d "$host" ] || continue
  printf 'BEFORE\\thost\\t%s\\n' "$host"
done
for link in "$root"/ports/*/subsystems/"$prefix"*; do
  [ -L "$link" ] || continue
  if rm -f -- "$link"; then
    printf 'REMOVED\\tlink\\t%s\\n' "$link"
  else
    printf 'FAILED\\tlink\\t%s\\n' "$link"
    status=1
  fi
done
for sub in "$root"/subsystems/"$prefix"*; do
  [ -d "$sub" ] || continue
  for ns in "$sub"/namespaces/*; do
    [ -d "$ns" ] || continue
    if [ -e "$ns/enable" ]; then
      printf 0 > "$ns/enable" 2>/dev/null || true
    fi
    if rmdir -- "$ns"; then
      printf 'REMOVED\\tnamespace\\t%s\\n' "$ns"
    else
      printf 'FAILED\\tnamespace\\t%s\\n' "$ns"
      status=1
    fi
  done
  for link in "$sub"/allowed_hosts/*; do
    [ -L "$link" ] || continue
    name=${{link##*/}}
    case "$name" in
      "$prefix"*) ;;
      *) continue ;;
    esac
    if rm -f -- "$link"; then
      printf 'REMOVED\\tallowed-host\\t%s\\n' "$link"
    else
      printf 'FAILED\\tallowed-host\\t%s\\n' "$link"
      status=1
    fi
  done
  # configfs removes its default groups with the subsystem; a plain test tree
  # does not. Removing an empty default group is harmless on configfs, where
  # the kernel may refuse it, and the subsystem rmdir below still decides.
  [ -d "$sub/namespaces" ] && rmdir -- "$sub/namespaces" 2>/dev/null || true
  [ -d "$sub/allowed_hosts" ] && rmdir -- "$sub/allowed_hosts" 2>/dev/null || true
  if rmdir -- "$sub"; then
    printf 'REMOVED\\tsubsystem\\t%s\\n' "$sub"
  else
    printf 'FAILED\\tsubsystem\\t%s\\n' "$sub"
    status=1
  fi
done
for host in "$root"/hosts/"$prefix"*; do
  [ -d "$host" ] || continue
  if rmdir -- "$host"; then
    printf 'REMOVED\\thost\\t%s\\n' "$host"
  else
    printf 'FAILED\\thost\\t%s\\n' "$host"
    status=1
  fi
done
left=0
for link in "$root"/ports/*/subsystems/"$prefix"*; do
  [ -L "$link" ] || continue
  printf 'REMAINS\\tlink\\t%s\\n' "$link"
  left=1
done
for sub in "$root"/subsystems/"$prefix"*; do
  [ -d "$sub" ] || continue
  printf 'REMAINS\\tsubsystem\\t%s\\n' "$sub"
  left=1
done
for host in "$root"/hosts/"$prefix"*; do
  [ -d "$host" ] || continue
  printf 'REMAINS\\thost\\t%s\\n' "$host"
  left=1
done
if [ "$status" -eq 0 ] && [ "$left" -eq 0 ]; then
  printf 'COMPLETE\\n'
  exit 0
fi
printf 'INCOMPLETE\\n'
exit 1
"""


def cleanup_script(root=None):
    """The one POSIX shell command that removes only this engine's objects."""
    if root is None:
        value = CONFIGFS
    else:
        value = str(root)
    return CLEANUP_SCRIPT.format(root=shlex.quote(value), prefix=NQN_PREFIX,
                                 require_root=1 if root is None else 0)


def cleanup_host(remote, host, *, root=None):
    """Remove every tidehunter-prefixed configfs object on one owner host.

    The returned record is cell evidence: what was present, what was removed
    and what remained. `complete` is false when a removal failed or an object
    survived, so a caller must refuse to start the next cell.
    """
    code, out, err = remote.shell(host, cleanup_script(root), check=False)
    record = dict(host=host, root=str(root) if root is not None else CONFIGFS,
                  prefix=NQN_PREFIX, before=[], removed=[], remains=[],
                  unavailable=False, complete=False, exit_code=code,
                  error=None)
    for line in out.decode(errors='replace').splitlines():
        parts = line.split('\t')
        if parts[:1] == ['UNAVAILABLE']:
            record['unavailable'] = True
            continue
        if parts[:1] == ['ERROR']:
            record['error'] = '\t'.join(parts[1:])
            continue
        if len(parts) != 3:
            continue
        kind, path = parts[0], parts[2]
        if kind == 'BEFORE' or kind == 'REMOVED' or kind == 'REMAINS':
            record[{'BEFORE': 'before', 'REMOVED': 'removed',
                    'REMAINS': 'remains'}[kind]].append(
                        dict(kind=parts[1], path=path))
        elif kind == 'FAILED':
            record['error'] = (record['error'] or '') + f'failed {parts[1]} {path}; '
    if record['unavailable']:
        record['complete'] = True
    elif code != 0:
        record['error'] = record['error'] or err.decode(errors='replace')[-500:]
        record['complete'] = False
    else:
        record['complete'] = not record['remains']
    return record


def cleanup_hosts(remote, hosts, *, root=None):
    """One exact-owned cleanup record per distinct owner host."""
    records = []
    for host in sorted(set(hosts)):
        records.append(cleanup_host(remote, host, root=root))
    return records


def stop_owner(remote, *, host, pid, process_group, stop_path=None,
               partition=None, source=None, grace=OWNER_STOP_SECONDS,
               kill_grace=OWNER_KILL_SECONDS, clock=time.monotonic,
               sleep=time.sleep):
    """Close one owner with the marker first, then SIGTERM, then SIGKILL.

    A group that survives the marker inside `grace` gets SIGTERM. A group that
    survives SIGTERM gets SIGKILL and the record says `forced`; a forced or
    surviving stop is a failed cell, never a clean stop. An owner without a
    marker keeps the older 90 s SIGTERM allowance before SIGKILL.
    """
    started = clock()
    record = dict(host=host, partition=partition, pid=pid, signal=None,
                  grace_seconds=grace, duration_seconds=0.0, forced=False,
                  still_alive=False, exit_code=None)
    if source is not None:
        record['source'] = source
    alive = remote.exists(host, f'/proc/{pid}')
    if alive and stop_path:
        record['signal'] = 'stop-file'
        remote.shell(host, f'touch {shlex.quote(str(stop_path))}', check=False)
        deadline = clock() + grace
        while remote.exists(host, f'/proc/{pid}'):
            if clock() > deadline:
                break
            sleep(0.2)
    if remote.exists(host, f'/proc/{pid}'):
        record['signal'] = ((record['signal'] + '+SIGTERM')
                            if record['signal'] else 'SIGTERM')
        remote.shell(host, f'kill -TERM -{process_group}', check=False)
        deadline = clock() + (kill_grace if stop_path else grace)
        while remote.exists(host, f'/proc/{pid}'):
            if clock() > deadline:
                break
            sleep(0.1)
    record['forced'] = remote.exists(host, f'/proc/{pid}')
    if record['forced']:
        remote.shell(host, f'kill -KILL -{process_group}', check=False)
        deadline = clock() + kill_grace
        while remote.exists(host, f'/proc/{pid}'):
            if clock() > deadline:
                break
            sleep(0.1)
    record['still_alive'] = remote.exists(host, f'/proc/{pid}')
    record['duration_seconds'] = round(clock() - started, 3)
    return record
