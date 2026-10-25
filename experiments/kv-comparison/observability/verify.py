"""Verify real Prometheus and Grafana dashboard queries, without creating data."""
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import math
from pathlib import Path
import re
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from exporter import STATUS, TIMESTAMP, scalar, snapshot

DATASOURCE = 'kv-comparison-prometheus'
DASHBOARD = 'kv-comparison-local'
MAX_RESPONSE = 16 * 1024 * 1024
HTTP_TIMEOUT = 8
CONCURRENCY = 8


class VerificationError(ValueError):
    pass


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise VerificationError('unexpected redirect from fixed local verification endpoint')


def request_json(url, authorization=None, payload=None):
    headers = {'Accept': 'application/json'}
    if authorization:
        headers['Authorization'] = authorization
    data = None
    if payload is not None:
        headers['Content-Type'] = 'application/json'
        data = json.dumps(payload).encode()
    # Never send the generated secret through environment-configured proxies.
    opener = build_opener(ProxyHandler({}), NoRedirect())
    request = Request(url, data=data, headers=headers)
    with opener.open(request, timeout=HTTP_TIMEOUT) as response:
        if response.status != 200:
            raise VerificationError(f'HTTP status {response.status} from local verification endpoint')
        raw = response.read(MAX_RESPONSE + 1)
    if len(raw) > MAX_RESPONSE:
        raise VerificationError('verification response exceeds size limit')
    result = json.loads(raw)
    if not isinstance(result, dict):
        raise VerificationError('verification response is not a JSON object')
    return result


def prometheus_query(expression):
    answer = request_json('http://127.0.0.1:9090/api/v1/query?' + urlencode({'query': expression, 'timeout': '5s'}))
    if answer.get('status') != 'success' or answer.get('error'):
        raise VerificationError('Prometheus query returned an error')
    data = answer.get('data', {})
    if data.get('resultType') != 'vector' or not isinstance(data.get('result'), list):
        raise VerificationError('Prometheus verification query did not return a vector')
    return data['result']


def one_value(rows):
    if len(rows) != 1:
        return None
    value = float(rows[0]['value'][1])
    if not math.isfinite(value):
        raise VerificationError('nonfinite verification observation')
    return value


def expected_selection(config):
    selected = config['results']
    current = snapshot(selected, config['stale_seconds'])
    if current.state == 'no_run':
        return current, None, None
    if current.state == 'waiting' and selected is not None and not Path(selected).exists():
        return current, None, None
    if not current.payload:
        raise VerificationError('selected existing result is not fresh/valid: ' + current.state)
    text = current.payload.decode('utf-8')
    return current, scalar(STATUS, text), scalar(TIMESTAMP, text)


def wait_for_observation(config, readiness_seconds):
    deadline = time.monotonic() + readiness_seconds
    attempts = 0
    last_reason = 'no scrape observed'
    while True:
        attempts += 1
        expected, status, stamp = expected_selection(config)
        try:
            states = prometheus_query('kv_observer_state{job="comparison"}')
            available = one_value(prometheus_query('kv_observer_data_available{job="comparison"}'))
            final = one_value(prometheus_query('kv_observer_snapshot_final{job="comparison"}'))
            host = one_value(prometheus_query('up{job="host"}'))
            exporter = one_value(prometheus_query('up{job="comparison"}'))
            observed_state = states[0].get('metric', {}).get('state') if len(states) == 1 else None
            matches = (one_value(states) == 1 and observed_state == expected.state
                       and available == int(bool(expected.payload)) and final == int(expected.final)
                       and host == 1 and exporter == 1)
            if status is not None:
                observed_status = one_value(prometheus_query('kv_comparison_run_status{job="comparison"}'))
                observed_stamp = one_value(prometheus_query('kv_comparison_snapshot_timestamp_seconds{job="comparison"}'))
                matches = matches and observed_status == status and observed_stamp is not None
                # Producer snapshots can advance while verifying a live run.
                matches = matches and (abs(observed_stamp - stamp) < 0.000001 if expected.final
                                       else abs(observed_stamp - stamp) <= config['stale_seconds'])
            if matches:
                return {'state': expected.state, 'available': available, 'final': final,
                        'run_status': status, 'snapshot_timestamp_seconds': stamp,
                        'host_up': host, 'exporter_up': exporter, 'readiness_attempts': attempts}
            last_reason = 'current selection/snapshot and host scrape have not been observed'
        except HTTPError as error:
            if error.code != 503:
                raise
            last_reason = 'Prometheus is not ready (503)'
        except URLError:
            last_reason = 'Prometheus readiness connection unavailable'
        if time.monotonic() >= deadline:
            raise VerificationError('readiness deadline exceeded: ' + last_reason)
        time.sleep(min(1, max(0, deadline - time.monotonic())))


def interpolate(expression):
    substitutions = {'backend': '.*', 'phase': '.*', '__rate_interval': '20s',
                     '__interval': '5s', '__interval_ms': '5000', '__range': '30m',
                     '__range_s': '1800', '__range_ms': '1800000'}

    def replace(match):
        variable = match.group(1) or match.group(2)
        if variable not in substitutions:
            raise VerificationError('unsupported dashboard query variable: ' + variable)
        return substitutions[variable]

    rendered = re.sub(r'\$\{([A-Za-z_][A-Za-z_0-9]*)\}|\$([A-Za-z_][A-Za-z_0-9]*)', replace, expression)
    if '$' in rendered:
        raise VerificationError('unresolved dashboard query interpolation')
    return rendered


def panel_querysets(dashboard):
    queue = list(dashboard.get('panels', []))
    sets = []
    visited = 0
    while queue:
        visited += 1
        if visited + len(queue) > 129:
            raise VerificationError('dashboard exceeds bounded panel count')
        panel = queue.pop(0)
        queue.extend(panel.get('panels', []))
        queries = []
        for target in panel.get('targets', []):
            if target.get('hide'):
                continue
            if len(queries) >= 16:
                raise VerificationError('dashboard panel exceeds bounded target count')
            source = target.get('datasource', panel.get('datasource', {}))
            if source.get('uid') != DATASOURCE or source.get('type') != 'prometheus':
                raise VerificationError('dashboard target references an unexpected datasource')
            expression = target.get('expr')
            if not isinstance(expression, str) or not expression or len(expression) > 16384:
                raise VerificationError('dashboard target has no bounded PromQL expression')
            queries.append({'refId': target['refId'], 'expr': interpolate(expression),
                            'datasource': {'uid': DATASOURCE, 'type': 'prometheus'},
                            'intervalMs': 5000, 'maxDataPoints': 360, 'format': 'time_series',
                            'instant': bool(target.get('instant')), 'range': not bool(target.get('instant'))})
        if queries:
            if len({query['refId'] for query in queries}) != len(queries):
                raise VerificationError('dashboard panel has duplicate query reference IDs')
            sets.append({'panel': panel.get('id'), 'title': panel.get('title'), 'queries': queries})
    if not sets:
        raise VerificationError('provisioned dashboard contains no executable queries')
    return sets


def numeric_values(frames):
    values = []
    for frame in frames:
        fields = frame.get('schema', {}).get('fields', [])
        columns = frame.get('data', {}).get('values', [])
        for field, column in zip(fields, columns):
            if field.get('type') == 'number':
                values.extend(value for value in column if value is not None)
    return values


def execute_queryset(item, authorization, now_ms, expectations=None):
    response = request_json('http://127.0.0.1:3000/api/ds/query', authorization,
                            {'from': str(now_ms - 1800000), 'to': str(now_ms), 'queries': item['queries']})
    if response.get('error') or response.get('message') or not isinstance(response.get('results'), dict):
        raise VerificationError('Grafana datasource query request returned an error')
    results = response['results']
    counts = {}
    for query in item['queries']:
        reference = query['refId']
        result = results.get(reference)
        if not isinstance(result, dict) or result.get('status', 200) != 200 or result.get('error'):
            raise VerificationError(f'Grafana panel {item["panel"]} query {reference} failed')
        frames = result.get('frames', [])
        if not isinstance(frames, list):
            raise VerificationError(f'Grafana panel {item["panel"]} query {reference} has no frame result')
        values = numeric_values(frames)
        if expectations is not None:
            expected = expectations[reference]
            if not values or any(value != expected for value in values):
                raise VerificationError('Grafana real-observation anchor mismatch: ' + reference)
        counts[reference] = {'frames': len(frames), 'numeric_points': len(values),
                             'http_status': 200, 'query_status': result.get('status')}
    return {'panel': item['panel'], 'title': item['title'], 'queries': counts}


def verify(state_directory, config, readiness_seconds=45):
    started = time.time()
    receipt = {'schema_version': 1, 'success': False, 'started_timestamp_seconds': started,
               'dashboard_uid': DASHBOARD, 'datasource_uid': DATASOURCE,
               'max_concurrent_querysets': CONCURRENCY, 'visual_verification': False, 'querysets': []}
    try:
        password = (state_directory / 'grafana_admin_password').read_text().strip()
        if not password or len(password) > 1024:
            raise VerificationError('invalid generated Grafana secret')
        authorization = 'Basic ' + base64.b64encode(('researcher:' + password).encode()).decode()
        receipt['observation'] = wait_for_observation(config, readiness_seconds)
        source = request_json(f'http://127.0.0.1:3000/api/datasources/uid/{DATASOURCE}', authorization)
        if source.get('type') != 'prometheus' or source.get('url') != 'http://127.0.0.1:9090':
            raise VerificationError('provisioned datasource identity or URL mismatch')
        health = request_json(f'http://127.0.0.1:3000/api/datasources/uid/{DATASOURCE}/health', authorization)
        if health.get('status') != 'OK':
            raise VerificationError('Grafana datasource/plugin health failed')
        receipt['datasource_health'] = health['status']
        document = request_json(f'http://127.0.0.1:3000/api/dashboards/uid/{DASHBOARD}', authorization)
        dashboard = document.get('dashboard', {})
        if dashboard.get('uid') != DASHBOARD:
            raise VerificationError('provisioned dashboard identity mismatch')
        querysets = panel_querysets(dashboard)
        receipt['dashboard_queries_sha256'] = hashlib.sha256(json.dumps(querysets, sort_keys=True).encode()).hexdigest()
        now_ms = int(time.time() * 1000)
        observation = receipt['observation']
        anchors = {'State': (f'kv_observer_state{{job="comparison",state="{observation["state"]}"}}', 1),
                   'NodeUp': ('up{job="host"}', 1),
                   'Available': ('kv_observer_data_available{job="comparison"}', observation['available'])}
        if observation['run_status'] is not None:
            anchors['RunStatus'] = ('kv_comparison_run_status{job="comparison"}', observation['run_status'])
        anchor_set = {'panel': 'real-observation-anchors', 'title': 'Real current selection and node scrape',
                      'queries': [{'refId': key, 'expr': expression, 'datasource': {'uid': DATASOURCE, 'type': 'prometheus'},
                                   'intervalMs': 5000, 'maxDataPoints': 1, 'format': 'time_series',
                                   'instant': True, 'range': False} for key, (expression, _) in anchors.items()]}
        querysets.append(anchor_set)
        failures = []
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
            futures = {pool.submit(execute_queryset, item, authorization, now_ms,
                                   {key: value for key, (_, value) in anchors.items()} if item is anchor_set else None): item
                       for item in querysets}
            for future in as_completed(futures):
                item = futures[future]
                try:
                    receipt['querysets'].append(future.result())
                except (OSError, ValueError, KeyError, TypeError) as error:
                    failures.append({'panel': item['panel'], 'error': type(error).__name__, 'detail': str(error)[:300]})
        if failures:
            receipt['failures'] = failures
            raise VerificationError(f'{len(failures)} actual Grafana dashboard querysets failed')
        receipt['success'] = True
    except (OSError, ValueError, KeyError, TypeError) as error:
        # No Authorization headers, generated password, response bodies or metric rows.
        receipt['error'] = {'type': type(error).__name__, 'detail': str(error)[:300]}
    receipt['elapsed_seconds'] = round(time.time() - started, 3)
    receipt['querysets'].sort(key=lambda item: str(item['panel']))
    return receipt
