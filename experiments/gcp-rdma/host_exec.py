#!/usr/bin/env python3
"""The one injectable host-execution seam of the campaign drivers.

`manual_sui.py`, `stage3_campaign.py` and `fault_cells.py` run a command on a
host and copy bytes to and from a host through this interface only, so a
bucket-mailbox executor is a constructor swap for the default `GcloudSsh`. The
seam has one required primitive:

    execute(host, command, *, stdin=None) -> (returncode, stdout, stderr)

`run`, `shell`, `read_bytes`, `write_bytes` and `exists` are derived from it.
`GcloudSsh` implements the primitive with `gcloud compute ssh --command`; its
`runner` argument is the argv-level boundary the tests replace.
"""
from pathlib import Path
import shlex
import subprocess


class HostCommandError(RuntimeError):
    """A host command failed; the last 2,000 bytes of stderr are kept."""


class HostExecution:
    """Run a command on a host; write bytes to it and read bytes from it."""

    def execute(self, host, command, *, stdin=None):
        """Run `command` on `host`; return (returncode, stdout, stderr)."""
        raise NotImplementedError

    def run(self, host, command, *, stdin=None, check=True):
        code, out, err = self.execute(host, command, stdin=stdin)
        if check and code != 0:
            raise HostCommandError(
                f'{host}: command failed ({code}): '
                f'{err.decode(errors="replace")[-2000:]}')
        return code, out, err

    def shell(self, host, command, *, stdin=None, check=True):
        return self.run(host, 'sh -lc ' + shlex.quote(command),
                        stdin=stdin, check=check)

    def read_bytes(self, host, path):
        code, out, err = self.shell(host, f'cat {shlex.quote(str(path))}', check=False)
        if code != 0:
            raise HostCommandError(
                f'{host}: cannot read {path}: {err.decode(errors="replace")}')
        return out

    def write_bytes(self, host, path, data):
        path = Path(path)
        return self.shell(host, f'mkdir -p {shlex.quote(str(path.parent))} && '
                                f'cat > {shlex.quote(str(path))}', stdin=data)

    def exists(self, host, path):
        code, _, _ = self.shell(host, f'test -e {shlex.quote(str(path))}', check=False)
        return code == 0


class GcloudSsh(HostExecution):
    """The default seam: one `gcloud compute ssh --command` per host."""

    def __init__(self, zone, *, runner=None, iap=False):
        self.zone, self.runner, self.iap = zone, runner or self._gcloud, iap

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

    def execute(self, host, command, *, stdin=None):
        return self.runner(self.argv(host, command), stdin=stdin)
