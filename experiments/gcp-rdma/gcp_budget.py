"""Conservative estimates, not a billing invoice. All values are USD cents."""
import datetime as dt
import json
from pathlib import Path
from gcp_api import ApiError

KEY = 'budget/ledger.json'
RESERVATION_ROOT = 'budget/reservations/'
SPEND_ROOT = 'budget/spend/'
BUDGET_PATH = Path(__file__).with_name('budget.json')


def load(path=None):
    """The reviewed budget document next to this module."""
    return json.loads(Path(path or BUDGET_PATH).read_text())


def _campaign(value):
    campaign = value.get('campaign')
    if not isinstance(campaign, dict):
        raise ValueError('budget.json carries no campaign cap block')
    for key in ('cap_usd', 'spent_usd', 'hosts', 'nanodollars_per_host_hour',
                'ceiling_numer', 'ceiling_denom'):
        if key not in campaign:
            raise ValueError('budget.json campaign cap block lacks ' + key)
    for key in ('hosts', 'nanodollars_per_host_hour', 'ceiling_numer', 'ceiling_denom'):
        if type(campaign[key]) is not int or campaign[key] <= 0:
            raise ValueError('budget.json campaign ' + key + ' must be a positive integer')
    for key in ('cap_usd', 'spent_usd'):
        if type(campaign[key]) not in (int, float) or campaign[key] < 0:
            raise ValueError('budget.json campaign ' + key + ' must be nonnegative USD')
    return campaign


def _cents(usd):
    return int(round(float(usd) * 100))


def fleet_ceiling_cents(hours, hosts, value=None):
    """The reviewed ceiling of one fleet window, rounded up to whole cents.

    Same price and same 14500/11834 compute-to-ceiling ratio as the campaign
    manifest, so the published row and this refusal cannot drift.
    """
    if type(hours) is not int or hours <= 0:
        raise ValueError('fleet window must be a positive integer hour count')
    if type(hosts) is not int or hosts <= 0:
        raise ValueError('fleet host count must be a positive integer')
    campaign = _campaign(value if value is not None else load())
    numerator = (campaign['nanodollars_per_host_hour'] * hosts * hours
                 * campaign['ceiling_numer'] * 100)
    denominator = campaign['ceiling_denom'] * 10 ** 9
    return -(-numerator // denominator)


def _immutable(store, key, entry):
    """Write one exact receipt; identical bytes from a retry are reused."""
    data = json.dumps(entry, sort_keys=True).encode()
    try:
        store.put(key, data, content_type='application/json', if_generation_match=0)
    except ApiError as exc:
        if exc.status != 412:
            raise
        meta = store.head(key)
        if store.get(key, generation=meta['generation']) != data:
            raise ValueError('immutable budget receipt conflicts') from exc
    return entry


def _utc(now=None):
    value = now or dt.datetime.now(dt.timezone.utc)
    return value.replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def receipt_key(root, run_id, stage):
    return f'{root}{run_id}/{stage}.json'


def reserve_window(store, run_id, stage, hours, hosts, *, now=None):
    """Record one fleet window's conservative ceiling under its run and stage.

    The run id is derived from the campaign, so the three stage fleets share it;
    the stage keeps their receipts distinct. The object is immutable, so a
    changed window is a conflict, never a silent re-quote.
    """
    if not isinstance(run_id, str) or not run_id:
        raise ValueError('a budget reservation needs a run id')
    if (not isinstance(stage, str) or not stage
            or any(character not in 'abcdefghijklmnopqrstuvwxyz0123456789-'
                   for character in stage)):
        raise ValueError('a budget reservation needs a lowercase stage name')
    try:
        store.head(receipt_key(SPEND_ROOT, run_id, stage))
    except ApiError as exc:
        if exc.status != 404:
            raise
    else:
        raise ValueError('this run and stage already spent its window')
    cents = fleet_ceiling_cents(hours, hosts)
    entry = {'schema_version': 1, 'kind': 'fleet-window', 'run_id': run_id,
             'stage': stage, 'hours': hours, 'hosts': hosts,
             'ceiling_cents': cents, 'reserved_utc': _utc(now)}
    return _immutable(store, receipt_key(RESERVATION_ROOT, run_id, stage), entry)


def _receipts(store, root, field):
    rows = {}
    for meta in store.list(root):
        name = meta.get('name')
        if not isinstance(name, str) or not name.startswith(root):
            continue
        generation = str(meta.get('generation', ''))
        if not generation:
            raise ValueError('budget receipt has no generation')
        raw = store.get(name, generation=generation)
        entry = json.loads(raw)
        if (entry.get('schema_version') != 1
                or not isinstance(entry.get('run_id'), str)
                or not isinstance(entry.get('stage'), str)
                or type(entry.get(field)) is not int or entry[field] < 0):
            raise ValueError('invalid budget receipt: ' + name)
        rows[(entry['run_id'], entry['stage'])] = entry[field]
    return rows


def committed_cents(store):
    """The conservative total of every fleet receipt: a spend replaces its ceiling."""
    reserved = _receipts(store, RESERVATION_ROOT, 'ceiling_cents')
    spent = _receipts(store, SPEND_ROOT, 'estimated_cents')
    return sum(spent.get(key, value) for key, value in reserved.items()) + sum(
        value for key, value in spent.items() if key not in reserved)


def reservation_cents(store, run_id, stage):
    key = receipt_key(RESERVATION_ROOT, run_id, stage)
    try:
        meta = store.head(key)
    except ApiError as exc:
        if exc.status == 404:
            return None
        raise
    entry = json.loads(store.get(key, generation=str(meta['generation'])))
    if (entry.get('schema_version') != 1 or entry.get('kind') != 'fleet-window'
            or entry.get('run_id') != run_id or entry.get('stage') != stage
            or type(entry.get('ceiling_cents')) is not int
            or entry['ceiling_cents'] < 0):
        raise ValueError('invalid fleet window receipt')
    return entry['ceiling_cents']


def admit_campaign(hours, hosts, value=None, *, store=None, run_id=None, stage=None):
    """Refuse a fleet whose ceiling crosses the campaign cap.

    `spent_usd` is the conservative historical total before any receipt. Every
    stage fleet then reserves its own immutable window receipt, and a retry of
    the same run and stage reuses that receipt instead of buying a second one.
    The ceiling is the price-based bound of the requested window, so a plan row
    and this refusal cannot describe different money.
    """
    campaign = _campaign(value if value is not None else load())
    ceiling = fleet_ceiling_cents(hours, hosts, value)
    cap, spent = _cents(campaign['cap_usd']), _cents(campaign['spent_usd'])
    committed = committed_cents(store) if store is not None else 0
    existing = (reservation_cents(store, run_id, stage)
                if store is not None and run_id is not None and stage is not None
                else None)
    if existing is not None:
        try:
            store.head(receipt_key(SPEND_ROOT, run_id, stage))
        except ApiError as exc:
            if exc.status != 404:
                raise
        else:
            existing = None  # the window is closed; a new one must be reserved
    if existing is not None:
        if existing != ceiling:
            raise ValueError('this run and stage already reserved a different window')
        additional = 0
    else:
        additional = ceiling
    if spent + committed + additional > cap:
        raise ValueError(
            f'campaign cap: ${spent / 100:.2f} spent + ${committed / 100:.2f} committed + '
            f'${additional / 100:.2f} for {hosts} hosts x {hours} h exceeds '
            f'${cap / 100:.2f}')
    return {'cap_usd': cap / 100, 'spent_usd': spent / 100,
            'committed_usd': committed / 100,
            'fleet_ceiling_usd': ceiling / 100,
            'remaining_after_usd': (cap - spent - committed - additional) / 100}


def record_spend(store, run_id, stage, *, now=None):
    """Replace one window's ceiling with the actual elapsed-time estimate.

    A stopped VM keeps billing its boot disk, so the day-2 delete is also the
    point at which the fleet's money is known. If no reservation exists, the
    ceiling is already the conservative total and nothing is written.
    """
    key = receipt_key(RESERVATION_ROOT, run_id, stage)
    try:
        meta = store.head(key)
    except ApiError as exc:
        if exc.status == 404:
            return None
        raise
    entry = json.loads(store.get(key, generation=str(meta['generation'])))
    if entry.get('run_id') != run_id or entry.get('stage') != stage:
        raise ValueError('invalid fleet window receipt')
    reserved = dt.datetime.fromisoformat(entry['reserved_utc'].replace('Z', '+00:00'))
    moment = now or dt.datetime.now(dt.timezone.utc)
    seconds = max(0, int((moment - reserved).total_seconds()))
    campaign = _campaign(load())
    numerator = (campaign['nanodollars_per_host_hour'] * entry['hosts'] * seconds
                 * campaign['ceiling_numer'] * 100)
    denominator = campaign['ceiling_denom'] * 10 ** 9 * 3600
    cents = -(-numerator // denominator)
    spend = {'schema_version': 1, 'kind': 'fleet-spend', 'run_id': run_id,
             'stage': stage, 'seconds': seconds, 'estimated_cents': cents,
             'ceiling_cents': entry['ceiling_cents'], 'recorded_utc': _utc(moment)}
    return _immutable(store, receipt_key(SPEND_ROOT, run_id, stage), spend)


def immutable_receipt(store, run_id, entry):
    key = 'budget/runs/' + run_id + '.json'
    data = json.dumps(entry, sort_keys=True).encode()
    try:
        store.put(key, data, content_type='application/json', if_generation_match=0)
    except ApiError as exc:
        if exc.status != 412:
            raise
        meta = store.head(key)
        if store.get(key, generation=meta['generation']) != data:
            raise ValueError('immutable budget receipt conflicts') from exc


def reserve(store, run_id, cents, weekly_limit_cents, now=None):
    if not isinstance(cents, int) or cents <= 0 or not isinstance(weekly_limit_cents, int) or weekly_limit_cents <= 0:
        raise ValueError('positive integer quote and weekly budget (cents) required')
    now = now or dt.datetime.now(dt.timezone.utc)
    date = now.date().isoformat()
    week = now.date().isocalendar()
    week_key = f'{week.year}-W{week.week:02d}'
    entry = {'date': date, 'week': week_key, 'month': date[:7], 'estimated_cents': cents}
    for _ in range(30):
        try:
            meta = store.head(KEY)
            generation = meta['generation']
            ledger = json.loads(store.get(KEY, generation=generation))
        except ApiError as exc:
            if exc.status != 404:
                raise
            ledger, generation = {'version': 1, 'runs': {}}, 0
        runs = ledger['runs']
        if run_id in runs:
            if runs[run_id]['estimated_cents'] != cents:
                raise ValueError('run already reserved with different estimate')
            immutable_receipt(store, run_id, runs[run_id])
            return report(ledger, date, week_key)
        total = sum(row['estimated_cents'] for row in runs.values() if row['week'] == week_key)
        if total + cents > weekly_limit_cents:
            raise ValueError('weekly budget exceeded by conservative reservation')
        runs[run_id] = entry
        try:
            store.put(KEY, json.dumps(ledger, sort_keys=True).encode(), content_type='application/json', if_generation_match=generation)
            immutable_receipt(store, run_id, entry)
            return report(ledger, date, week_key)
        except ApiError as exc:
            if exc.status != 412:
                raise
    raise RuntimeError('budget CAS contention exceeded retry bound')


def report(ledger, date, week):
    rows = list(ledger['runs'].values())
    total = lambda pred: sum(x['estimated_cents'] for x in rows if pred(x))
    return {'currency': 'USD', 'unit': 'cents', 'kind': 'conservative estimates, not invoices',
            'daily': total(lambda x: x['date'] == date),
            'weekly': total(lambda x: x['week'] == week),
            'monthly': total(lambda x: x['month'] == date[:7]),
            'lifetime': total(lambda x: True)}
