"""Conservative estimates, not a billing invoice. All values are USD cents."""
import datetime as dt
import json
from gcp_api import ApiError

KEY = 'budget/ledger.json'


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
