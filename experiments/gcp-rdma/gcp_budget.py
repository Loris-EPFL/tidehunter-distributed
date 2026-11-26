"""Conservative estimates, not a billing invoice. All values are USD cents."""
import datetime as dt
import json
from pathlib import Path
from gcp_api import ApiError

KEY = 'budget/ledger.json'
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


def admit_campaign(hours, hosts, value=None):
    """Refuse a fleet whose conservative ceiling crosses the campaign cap.

    `spent_usd` is the conservative total before this fleet: stage 0 plus the
    failed stage-1 fleet. The ceiling is the price-based bound of the requested
    window, so a plan row and this refusal cannot describe different money.
    """
    campaign = _campaign(value if value is not None else load())
    ceiling = fleet_ceiling_cents(hours, hosts, value)
    cap, spent = _cents(campaign['cap_usd']), _cents(campaign['spent_usd'])
    if spent + ceiling > cap:
        raise ValueError(
            f'campaign cap: ${spent / 100:.2f} spent + ${ceiling / 100:.2f} for '
            f'{hosts} hosts x {hours} h exceeds ${cap / 100:.2f}')
    return {'cap_usd': cap / 100, 'spent_usd': spent / 100,
            'fleet_ceiling_usd': ceiling / 100,
            'remaining_after_usd': (cap - spent - ceiling) / 100}


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
