"""Fixed EFA progress identities and fail-closed hardware evidence primitives."""
import csv
from pathlib import Path

CACHE_BYTES = 8 * 1024 * 1024
RSS_NUMERATOR, RSS_DENOMINATOR = 4, 5
BASELINE_SOURCE_SHA256 = 'f11eb057c71fb7ec149ce2827757a7041b68e47da6f6a6318ad2d7c3bacd6398'


def case(family, build, value, batch, window, rep, transport, cache=0,
         lanes=4, workers=1, read_window=32, member='mapped', cold=False):
    policy = f'{family}-c{cache}-l{lanes}-s{workers}-q{read_window}-{member}'
    group = policy + '-' + transport
    identity = f'{group}-{build}-v{value}-b{batch}-w{window}-r{rep}'
    qualification = family == 'qual'
    return dict(case_id=identity, family=group, build=build, value=value, batch=batch,
                window=window, rep=rep, transport=transport, cache=cache, lanes=lanes,
                workers=workers, read_window=read_window, member=member, cold=cold,
                diagnostics=family == 'diag', qualification=qualification,
                batches=64 if qualification else 320, mixed=16 if qualification else 64,
                reads=256 if qualification else 4096,
                native_workers=32 if build == 'current' else 1,
                native_control_id=f'{build}-v{value}-b{batch}-w{window}-r{rep}-c{cache}-q{read_window}'
                                  f'-n{32 if build == "current" else 1}-cold{int(cold)}'
                                  f'-diag{int(family == "diag")}-qual{int(qualification)}')


def qualification_cases():
    return [case('qual', 'current', v, 32, 32, 0, 'efa', lanes=lanes, cold=True)
            for v in (1024, 8192) for lanes in (1, 2, 4)]


def main_cases():
    result = []
    policies = [('baseline', 'tcp', 0), ('baseline', 'efa', 0),
                ('current', 'tcp', 0), ('current', 'efa', 0),
                ('current', 'tcp', CACHE_BYTES), ('current', 'efa', CACHE_BYTES)]
    for rep in range(3):
        rotated = policies[rep:] + policies[:rep]
        for value in (1024, 8192):
            for batch, window in ((1, 1), (32, 32)):
                for build, transport, cache in rotated:
                    result.append(case('core', build, value, batch, window, rep, transport,
                                       cache=cache, lanes='legacy' if build == 'baseline' else 4,
                                       workers='legacy' if build == 'baseline' else 1,
                                       member='legacy' if build == 'baseline' else 'mapped',
                                       read_window=1 if build == 'baseline' else 32))
    policies = [(member, lanes, workers, window) for member in ('mapped', 'positional')
                for lanes, workers, window in ((1, 1, 1), (4, 1, 32), (4, 4, 32))]
    for rep in range(3):
        for value in (1024, 8192):
            for member, lanes, workers, window in policies[rep:] + policies[:rep]:
                result.append(case('cold', 'current', value, 32, 32, rep, 'efa',
                                   member=member, lanes=lanes, workers=workers,
                                   read_window=window, cold=True))
    for value in (1024, 8192):
        for transport in ('tcp', 'efa'):
            result.append(case('diag', 'current', value, 32, 32, 0, transport, cold=True))
    return result


def plan():
    return qualification_cases() + main_cases()


def expected_cases():
    return {c['case_id'] + '-' + mode:
            (c['family'], c['build'], c['value'], c['batch'], c['window'], c['rep'], mode, c['case_id'])
            for c in plan() for mode in ('native-sync', 'tcp')}


def command_options(command):
    if (not isinstance(command, list) or not command or len(command) % 2 != 1
            or any(not isinstance(arg, str) for arg in command)):
        raise ValueError('invalid benchmark command')
    result = dict(zip(command[1::2], command[2::2]))
    if len(result) != len(command) // 2:
        raise ValueError('duplicate benchmark command option')
    return result


def memory_bytes(evidence):
    fields = dict(line.split(':', 1) for line in evidence['meminfo'].splitlines())
    values = {}
    for key in ('MemTotal', 'MemAvailable', 'SwapTotal', 'SwapFree'):
        parts = fields[key].split()
        if len(parts) != 2 or parts[1] != 'kB' or int(parts[0]) < 0:
            raise ValueError('invalid actual guest memory evidence')
        values[key] = int(parts[0]) * 1024
    if values['MemTotal'] <= 0 or not 0 < values['MemAvailable'] <= values['MemTotal']:
        raise ValueError('guest memory exhausted or invalid')
    if values['SwapTotal'] != values['SwapFree']:
        raise ValueError('guest has allocated swap')
    return values['MemTotal']


def hardware_interval(before, after, efa=False):
    ram = memory_bytes(before)
    if memory_bytes(after) != ram:
        raise ValueError('actual guest RAM changed')
    for name in ('pswpin', 'pswpout'):
        counters = [dict(line.split() for line in e['vmstat'].splitlines()) for e in (before, after)]
        values = [int(c[name]) for c in counters]
        if min(values) < 0 or values[0] != values[1]:
            raise ValueError('guest swapped during case')
    traffic = 0
    if efa:
        previous, current = before.get('efa_counters', {}), after.get('efa_counters', {})
        if not previous or set(previous) != set(current):
            raise ValueError('missing stable hardware EFA counters')
        deltas = {key: int(current[key]) - int(previous[key]) for key in previous}
        if any(int(previous[key]) < 0 or delta < 0 for key, delta in deltas.items()):
            raise ValueError('hardware EFA counter reset')
        traffic = sum(delta for key, delta in deltas.items()
                      if Path(key).name in ('send_bytes', 'recv_bytes', 'tx_bytes', 'rx_bytes'))
        if traffic <= 0:
            raise ValueError('no actual EFA hardware traffic')
    return dict(ram_bytes=ram, efa_traffic_bytes=traffic)


def resource_metrics(path, mode, spec, process, controller, nodes, summaries, stages=None):
    """Account all seven benchmark processes (native plus distributed six), not providers."""
    roles = {('native', '')} if mode == 'native-sync' else {('frontend', '')} | {
        ('storage', str(p)) for p in range(5)}
    if stages is None:
        stages = ['opened', 'loaded', 'point_get_window']
        if spec['cold']:
            stages.append('cache_evicted_get_window')
    inventory, previous, result = {}, {}, {}
    with Path(path).open() as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != ['stage', 'role', 'partition', 'pid', 'rss_bytes', 'peak_rss_bytes',
                                  'user_cpu_ns', 'system_cpu_ns']:
            raise ValueError('invalid resource snapshot schema')
        for row in reader:
            stage, role = row['stage'], (row['role'], row['partition'])
            if stage not in stages or role not in roles or (stage, role) in inventory:
                raise ValueError('unexpected/duplicate resource process inventory')
            values = {key: int(row[key]) for key in ('pid', 'rss_bytes', 'peak_rss_bytes',
                                                     'user_cpu_ns', 'system_cpu_ns')}
            if min(values.values()) < 0 or values['pid'] <= 0 or values['rss_bytes'] <= 0:
                raise ValueError('invalid process resource measurement')
            host = controller if role[0] != 'storage' else nodes[int(role[1])]
            pid = process['pid'] if role[0] != 'storage' else host['pid']
            if values['pid'] != pid:
                raise ValueError('resource PID differs from actual process inventory')
            if (values['rss_bytes'] > values['peak_rss_bytes'] or
                    values['peak_rss_bytes'] * RSS_DENOMINATOR > host['ram_bytes'] * RSS_NUMERATOR):
                raise ValueError('process RSS exceeds actual guest RAM bound')
            inventory[stage, role] = values
    if set(inventory) != {(stage, role) for stage in stages for role in roles}:
        raise ValueError('missing opened/active resource process inventory')
    # VmHWM can fall after page discard; retain the largest observed report.
    # Raw samples and every per-sample RAM bound remain unchanged.
    # https://man7.org/linux/man-pages/man5/proc_pid_status.5.html
    observed_peaks = dict.fromkeys(roles, 0)
    for stage in stages:
        rss = peak = cpu = 0
        for role in roles:
            value = inventory[stage, role]
            rss += value['rss_bytes']
            observed_peaks[role] = max(observed_peaks[role], value['peak_rss_bytes'])
            peak += observed_peaks[role]
            if role in previous:
                for key in ('user_cpu_ns', 'system_cpu_ns'):
                    if value[key] < previous[role][key]:
                        raise ValueError('process CPU counter decreased: ' + key)
                cpu += sum(value[key] - previous[role][key] for key in ('user_cpu_ns', 'system_cpu_ns'))
            previous[role] = value
        result['resource', stage, 'aggregate_rss_bytes'] = rss
        result['resource', stage, 'sum_observed_process_peak_rss_bytes'] = peak
        result['resource', stage, 'cpu_ns_since_previous_snapshot'] = cpu
        if stage == 'point_get_window':
            # Includes resource/control snapshot work: never label this pure API CPU.
            if float(summaries[stage]['phase_wall_ns']) <= 0:
                raise ValueError('resource window requires positive measured wall time')
            result['resource', stage, 'cpu_ns_per_window_wall_ns_upper_bound'] = (
                cpu / float(summaries[stage]['phase_wall_ns']))
    return result
