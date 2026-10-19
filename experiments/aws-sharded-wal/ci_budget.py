#!/usr/bin/env python3
"""Shared, fail-closed AWS benchmark admission and delayed billing accounting.

Policy: unresolved runs charge max(reserved, usage-estimated, AWS-billed-to-date)
until verified cleanup. Cleanup releases the admission hold immediately; recent
closed runs charge max(usage-estimated, recent AWS billing), never the original
reservation. Later increases to old usage bills are recognized when observed.
Shared/unattributed/untracked recent billing is charged once, separately from
known runs. Billed, reserved, estimated and projected totals are NOT additive.

Bootstrap must explicitly initialize the ledger; a missing, corrupt or unreachable
object never creates an empty budget during admission. S3 conditional writes are
the serialization point. After an ambiguous write, only observing the identical
operation authorizes success; otherwise the caller must stop and reconcile.
The tracked budget.json file is the sole editable budget authority. Ledger
policy snapshots are audit evidence, never an independently editable cap.

Cost Explorer is an optional, paid, read-only reconciliation, not an admission
oracle. It requires activated Repository/BenchmarkBranch/RunId allocation tags.
Tag shared infrastructure RunId=shared-infrastructure; absent RunId is explicitly
unattributed, not guessed to be shared. All billing is delayed/provisional,
including zero/non-estimated results. Reconciliation queries complete UTC days
since ledger initialization, at most max_billing_days (default 366). It retains
one bounded snapshot, not unbounded request history, and fails visibly beyond
that horizon. Partial tag coverage retains per-day/group maxima of earlier
and new observations, marked as conservatively merged rather than a single CE
result. Lifetime means tracked lifetime since initialization, NOT costs
before tracking. Cost tags cannot attribute every charge (for example some API
overhead); caller reservations must include independently priced overhead.
Generated reports under ci-control/reports are rebuilt from the authoritative
ledger, never incremented blindly. Publication failure cannot block settlement
or resource cleanup. Reports are derived accounting, not immutable against an
AWS administrator. Period estimates are recognized on UTC settlement dates,
reservations on UTC reservation dates, and billed costs on CE usage dates.
No EC2 APIs or mutations are performed by this module.
"""
import argparse
import copy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, localcontext
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time
import tempfile

REPOSITORY = 'Loris-EPFL/tidehunter-distributed'
BRANCH = 'bench/aws-efa'
DEFAULT_BUDGET = Path(__file__).with_name('budget.json')
WEEK = timedelta(days=7)
ZERO = Decimal('0')
QUANTUM = Decimal('0.000000000001')
MAX_USAGE_RECONCILIATIONS = 16


class BudgetError(RuntimeError):
    pass


class LedgerUnavailable(BudgetError):
    pass


class LedgerMalformed(BudgetError):
    pass


class BudgetExceeded(BudgetError):
    pass


class IdentityConflict(BudgetError):
    pass


class UnknownWrite(BudgetError):
    pass


def _decimal(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError('money and elapsed values require Decimal, string or integer, not float')
    try:
        number = Decimal(value)
    except InvalidOperation as error:
        raise ValueError('invalid decimal') from error
    if (not number.is_finite() or number < 0 or number > Decimal('1000000000000')
            or not -18 <= number.as_tuple().exponent <= 12):
        raise ValueError('decimal must be finite, nonnegative and bounded to 18 fractional digits')
    return number

def load_budget(path=DEFAULT_BUDGET):
    """Read the sole editable policy, bounded and hashed as exact source bytes."""
    try:
        resolved = Path(path).resolve(strict=True)
        with resolved.open('rb') as source:
            raw = source.read(8193)
        if len(raw) > 8192:
            raise ValueError('budget file exceeds 8 KiB')
        def unique(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError('duplicate budget key')
                result[key] = value
            return result
        value = json.loads(raw, object_pairs_hook=unique)
        if (not isinstance(value, dict) or set(value) != {'weekly_budget_usd'}
                or not isinstance(value['weekly_budget_usd'], str)):
            raise ValueError('budget file must contain only weekly_budget_usd as a decimal string')
        return dict(weekly_budget_usd=_money(value['weekly_budget_usd']),
                    sha256=hashlib.sha256(raw).hexdigest(), path=str(resolved))
    except (OSError, TypeError, ValueError) as error:
        raise BudgetError('cannot load authoritative budget file: ' + str(error)) from error


def _validate_policy(policy):
    if (not isinstance(policy, dict) or not isinstance(policy.get('path'), str)
            or not policy['path'] or not isinstance(policy.get('sha256'), str)
            or not re.fullmatch(r'[a-f0-9]{64}', policy['sha256'])):
        raise ValueError('invalid source-bound budget policy')
    _decimal(policy['weekly_budget_usd'])
    if 'observed_at' in policy:
        _utc(policy['observed_at'])



def _money(value):
    text = format(_decimal(value), 'f')
    return (text.rstrip('0').rstrip('.') if '.' in text else text) or '0'


def _sum(values):
    with localcontext() as context:
        context.prec = 60
        return sum(values, ZERO)


def usage_cost_usd(hourly_rate_usd, elapsed_seconds, count=1, minimum_seconds=60):
    """Conservative Linux per-second compute estimate, never an AWS bill.

    Pass actual charged elapsed time/rate evidence. Partial seconds round up;
    minimum defaults to the EC2 Linux 60-second minimum per instance. Other
    priced units (EBS, IPv4, S3, transfer) must be estimated separately.
    """
    if isinstance(count, bool) or not isinstance(count, int) or not 1 <= count <= 100000:
        raise ValueError('positive bounded integer instance count required')
    rate, elapsed, minimum = map(_decimal, (hourly_rate_usd, elapsed_seconds, minimum_seconds))
    with localcontext() as context:
        context.prec = 60
        seconds = max(elapsed.to_integral_value(rounding=ROUND_CEILING), minimum)
        return (rate * seconds * count / Decimal(3600)).quantize(QUANTUM, rounding=ROUND_CEILING)


def _utc(value=None):
    if value is None:
        value = datetime.now(timezone.utc)
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if not isinstance(value, datetime) or value.utcoffset() != timedelta(0):
        raise ValueError('absolute UTC datetime required')
    return value


def _stamp(value):
    return _utc(value).isoformat()


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def _evidence(value):
    def validate(item, depth=0):
        if depth > 16:
            raise ValueError('evidence nesting limit exceeded')
        if isinstance(item, dict):
            if not all(isinstance(key, str) for key in item):
                raise ValueError('evidence requires string keys')
            for child in item.values():
                validate(child, depth + 1)
        elif isinstance(item, list):
            for child in item:
                validate(child, depth + 1)
        elif item is not None and not isinstance(item, (str, int, bool)):
            raise ValueError('evidence must be JSON data; encode decimal values as strings')
    if not isinstance(value, dict) or not value:
        raise ValueError('nonempty evidence object required')
    validate(value)
    raw = _json(value)
    if len(raw) > 65536:
        raise ValueError('evidence exceeds 64 KiB')
    return json.loads(raw)


def _digest(value):
    return hashlib.sha256(_json(value)).hexdigest()


def _error_code(error):
    return getattr(error, 'response', {}).get('Error', {}).get('Code', type(error).__name__)


def _run_id(value):
    if not isinstance(value, str) or not re.fullmatch(r'th-[a-f0-9]{16}', value):
        raise ValueError('RunId must match th-[a-f0-9]{16}')
    return value


def settlement_digest(run_id, usage_usd, receipt):
    """Canonical public binding used by settle and guarded usage reconciliation."""
    return _digest([_run_id(run_id), _money(usage_usd), _evidence(receipt)])


class BudgetLedger:
    def __init__(self, s3, bucket, account, repository=REPOSITORY, branch=BRANCH,
                 budget_path=DEFAULT_BUDGET, *, key=None, max_attempts=8,
                 max_bytes=4 * 1024 * 1024, max_runs=4096, max_billing_days=366,
                 max_pages=64, max_groups=100000, kms_key_id=None,
                 clock=None, sleeper=time.sleep, max_report_days=3660):
        if not re.fullmatch(r'[0-9]{12}', account) or not bucket or not repository or not branch:
            raise ValueError('bucket, account, repository and branch required')
        if (not 1 <= max_attempts <= 32 or not 1024 <= max_bytes <= 16 * 1024 * 1024
                or not 1 <= max_runs <= 10000 or not 1 <= max_billing_days <= 400
                or not 1 <= max_pages <= 128 or not 1 <= max_groups <= 1000000
                or not 1 <= max_report_days <= 10000):
            raise ValueError('invalid ledger bounds')
        self.s3, self.bucket, self.account = s3, bucket, account
        self.repository, self.branch = repository, branch
        self.budget_path = Path(budget_path)
        self.key = key or 'ci-control/budget/' + _digest([account, repository, branch]) + '.json'
        self.report_key = 'ci-control/reports/' + _digest([account, repository, branch]) + '.json'
        self.max_report_days = max_report_days
        self.last_publication = {'status': 'not_attempted'}
        self.max_attempts, self.max_bytes, self.max_runs = max_attempts, max_bytes, max_runs
        self.max_billing_days, self.max_pages, self.max_groups = max_billing_days, max_pages, max_groups
        self.kms_key_id, self.clock, self.sleeper = kms_key_id, clock or (lambda: datetime.now(timezone.utc)), sleeper

    def _now(self, now):
        return _utc(self.clock() if now is None else now)

    def _validate(self, ledger):
        try:
            if (not isinstance(ledger, dict) or ledger['schema'] != 2
                    or ledger['account'] != self.account or ledger['repository'] != self.repository
                    or ledger['branch'] != self.branch or not isinstance(ledger['runs'], dict)
                    or len(ledger['runs']) > self.max_runs
                    or not isinstance(ledger['revision'], int) or ledger['revision'] < 0):
                raise ValueError('ledger identity/schema/bounds mismatch')
            created = _utc(ledger['created_at'])
            _utc(ledger['updated_at'])
            _validate_policy(ledger['budget_policy'])
            for run_id, run in ledger['runs'].items():
                _run_id(run_id)
                _validate_policy(run['budget_policy'])
                reserved = _utc(run['reserved_at'])
                if reserved < created:
                    raise ValueError('reservation predates ledger')
                amount = _money(run['reserved_usd'])
                evidence = _evidence(run['provenance'])
                if run['reservation_digest'] != _digest([run_id, amount, evidence]):
                    raise ValueError('reservation binding mismatch')
                if run['usage_estimated_usd'] is not None:
                    usage = _money(run['usage_estimated_usd'])
                    receipt = _evidence(run['receipt'])
                    self._validate_receipt(receipt)
                    if receipt.get('usage_evidence', {}).get('accounting_basis') == 'observed-resources-v2':
                        self._validate_measured_receipt(receipt, run_id, require_cleanup=False)
                    if (run['settlement_digest'] != _digest([run_id, usage, receipt])
                            or _utc(run['settled_at']) < reserved):
                        raise ValueError('settlement binding mismatch')
                    history = run.get('usage_reconciliation_history', [])
                    if not isinstance(history, list) or len(history) > MAX_USAGE_RECONCILIATIONS:
                        raise ValueError('usage reconciliation history bounds exceeded')
                    for index, previous in enumerate(history):
                        previous_usage = _money(previous['usage_estimated_usd'])
                        previous_receipt = _evidence(previous['receipt'])
                        self._validate_receipt(previous_receipt)
                        if (not previous_receipt['cleanup_complete']
                                or previous['settlement_digest'] != _digest([run_id, previous_usage, previous_receipt])
                                or previous['settled_at'] != run['settled_at']
                                or _utc(previous['reconciled_at']) < _utc(previous['settled_at'])):
                            raise ValueError('invalid prior settlement binding')
                        following = history[index + 1] if index + 1 < len(history) else run
                        if previous['replacement_settlement_digest'] != following['settlement_digest']:
                            raise ValueError('broken usage reconciliation chain')
                        self._validate_measured_receipt(following['receipt'], run_id)
                        if index and _utc(previous['reconciled_at']) < _utc(history[index - 1]['reconciled_at']):
                            raise ValueError('usage reconciliation time moved backwards')
                elif (any(run.get(name) is not None for name in ('receipt', 'settled_at', 'settlement_digest'))
                      or run.get('usage_reconciliation_history')):
                    raise ValueError('partial settlement')
            billing = ledger['billing']
            if not isinstance(billing, dict) or billing['status'] not in ('never_reconciled', 'available', 'partial', 'unavailable'):
                raise ValueError('invalid billing status')
            high_water = billing.get('run_billed_high_water_usd', {})
            if not isinstance(high_water, dict) or len(high_water) > self.max_runs:
                raise ValueError('invalid billed high-water bounds')
            for identity, amount in high_water.items():
                if identity not in ledger['runs']:
                    raise ValueError('foreign billed high-water run')
                _decimal(amount)
            adjustments = billing.get('late_usage_adjustments', [])
            if not isinstance(adjustments, list) or len(adjustments) > self.max_groups:
                raise ValueError('late billing history bounds exceeded')
            for adjustment in adjustments:
                if adjustment['run_id'] not in ledger['runs']:
                    raise ValueError('foreign late billing run')
                if _utc(adjustment['observed_at']) < created:
                    raise ValueError('late billing predates ledger')
                _decimal(adjustment['amount_usd'])
            snapshot = billing.get('snapshot')
            if snapshot is not None:
                if (snapshot['final'] is not False or snapshot['currency'] != 'USD'
                        or not isinstance(snapshot['estimated'], bool)
                        or not isinstance(snapshot['tag_coverage_complete'], bool)):
                    raise ValueError('billing must be provisional USD with explicit coverage')
                start, end = _utc(snapshot['start'] + 'T00:00:00+00:00'), _utc(snapshot['end'] + 'T00:00:00+00:00')
                if end < start or (end - start).days > self.max_billing_days:
                    raise ValueError('billing date bounds exceeded')
                _utc(snapshot['observed_at'])
                if not isinstance(snapshot['daily'], dict) or len(snapshot['daily']) > self.max_billing_days:
                    raise ValueError('billing history bounds/type exceeded')
                expected_days = {(start + timedelta(days=offset)).date().isoformat()
                                 for offset in range((end - start).days)}
                if set(snapshot['daily']) != expected_days:
                    raise ValueError('incomplete persisted billing daily coverage')
                if not isinstance(snapshot.get('conservative_merge', False), bool):
                    raise ValueError('invalid conservative billing merge flag')
                groups = 0
                for day, values in snapshot['daily'].items():
                    if not start.date().isoformat() <= day < end.date().isoformat():
                        raise ValueError('billing day outside requested range')
                    datetime.strptime(day, '%Y-%m-%d')
                    if not isinstance(values, dict):
                        raise ValueError('invalid daily costs')
                    groups += len(values)
                    for key, amount in values.items():
                        if not isinstance(key, str):
                            raise ValueError('invalid attribution')
                        _decimal(amount)
                if groups > self.max_groups:
                    raise ValueError('billing group bounds exceeded')
            return ledger
        except (KeyError, TypeError, ValueError, OverflowError, IdentityConflict) as error:
            raise LedgerMalformed('ledger malformed; refusing to reset budget: ' + str(error)) from error

    def _load(self, missing_ok=False):
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=self.key, ExpectedBucketOwner=self.account)
        except Exception as error:
            if _error_code(error) in ('NoSuchKey', '404') and missing_ok:
                return None, None
            raise LedgerUnavailable('cannot read existing budget ledger; no admission: ' + str(error)) from error
        try:
            with response['Body'] as body:
                if response.get('ContentLength', 0) > self.max_bytes:
                    raise LedgerMalformed('oversized budget ledger')
                raw = body.read(self.max_bytes + 1)
            if len(raw) > self.max_bytes or not response.get('ETag'):
                raise LedgerMalformed('oversized ledger or missing CAS ETag')
            def unique(pairs):
                result = {}
                for key, value in pairs:
                    if key in result:
                        raise ValueError('duplicate JSON key')
                    result[key] = value
                return result
            ledger = json.loads(raw, object_pairs_hook=unique,
                                parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
            return self._validate(ledger), response['ETag']
        except BudgetError:
            raise
        except Exception as error:
            raise LedgerMalformed('cannot decode budget ledger; refusing reset: ' + str(error)) from error

    def _put(self, ledger, etag):
        raw = _json(ledger)
        if len(raw) > self.max_bytes:
            raise BudgetError('ledger capacity exhausted; no history discarded')
        kwargs = dict(Bucket=self.bucket, Key=self.key, Body=raw,
                      ContentType='application/json', ExpectedBucketOwner=self.account,
                      ServerSideEncryption='aws:kms' if self.kms_key_id else 'AES256')
        if self.kms_key_id:
            kwargs['SSEKMSKeyId'] = self.kms_key_id
        if etag is None:
            kwargs['IfNoneMatch'] = '*'
        else:
            kwargs['IfMatch'] = etag
        self.s3.put_object(**kwargs)

    def initialize(self, now=None):
        """Explicit one-time create; never overwrite existing state or reset totals."""
        now = self._now(now)
        policy = load_budget(self.budget_path)
        existing, _ = self._load(missing_ok=True)
        if existing is not None:
            self._try_publish(now)
            return self._report(existing, None, now, policy=policy)
        ledger = dict(schema=2, account=self.account, repository=self.repository, branch=self.branch,
                      budget_policy=policy | {'observed_at': _stamp(now)},
                      created_at=_stamp(now), updated_at=_stamp(now),
                      revision=0, runs={}, billing={'status': 'never_reconciled', 'snapshot': None})
        try:
            self._put(ledger, None)
        except Exception as error:
            try:
                observed, _ = self._load()
            except BudgetError as read_error:
                raise UnknownWrite('initialization outcome unknown; reconcile before use') from read_error
            if _error_code(error) not in ('PreconditionFailed', 'ConditionalRequestConflict', '412', '409') and observed != ledger:
                raise UnknownWrite('initialization outcome unknown') from error
            ledger = observed
        self._try_publish(now)
        return self._report(ledger, None, now, policy=policy)

    def _mutate(self, action, now):
        for attempt in range(self.max_attempts):
            ledger, etag = self._load()
            effective_now = max(now, _utc(ledger['updated_at']))
            changed, result = action(ledger)
            if not changed:
                return copy.deepcopy(result)
            ledger['revision'] += 1
            ledger['updated_at'] = _stamp(effective_now)
            self._validate(ledger)
            try:
                self._put(ledger, etag)
                return copy.deepcopy(result)
            except Exception as error:
                if _error_code(error) in ('PreconditionFailed', 'ConditionalRequestConflict', '412', '409'):
                    if attempt + 1 < self.max_attempts:
                        self.sleeper(min(0.01 * 2 ** attempt, 0.25))
                    continue
                try:
                    observed, _ = self._load()
                    changed_again, observed_result = action(copy.deepcopy(observed))
                except Exception as read_error:
                    raise UnknownWrite('budget write outcome unknown; stop before spend and reconcile') from read_error
                if not changed_again:
                    return copy.deepcopy(observed_result)
                raise UnknownWrite('budget write not confirmed; stop before spend and reconcile') from error
        raise LedgerUnavailable('concurrent ledger contention exceeded bounded retries; no admission')

    def reserve(self, run_id, estimate_usd, provenance, now=None):
        run_id, now = _run_id(run_id), self._now(now)
        policy = load_budget(self.budget_path)
        amount, evidence = _money(estimate_usd), _evidence(provenance)
        if _decimal(amount) <= 0:
            raise ValueError('positive conservative reservation required')
        binding = _digest([run_id, amount, evidence])
        def apply(ledger):
            existing = ledger['runs'].get(run_id)
            if existing is not None:
                if existing['reservation_digest'] != binding:
                    raise IdentityConflict('RunId already bound to different reservation/provenance')
                return False, existing
            if len(ledger['runs']) >= self.max_runs:
                raise BudgetError('run history capacity exhausted; no history discarded')
            available = _decimal(self._report(ledger, None, now, policy=policy)['available_usd'])
            if _decimal(amount) > available:
                raise BudgetExceeded('rolling-seven-day budget exhausted; available USD ' + _money(available))
            run = dict(reserved_at=_stamp(now), reserved_usd=amount, provenance=evidence,
                       budget_policy=policy | {'observed_at': _stamp(now)},
                       reservation_digest=binding, usage_estimated_usd=None,
                       receipt=None, settled_at=None, settlement_digest=None)
            ledger['runs'][run_id] = run
            return True, run
        result = self._mutate(apply, now)
        self._try_publish(now)
        return result

    @staticmethod
    def _validate_receipt(receipt):
        if not isinstance(receipt.get('cleanup_complete'), bool):
            raise ValueError('settlement requires cleanup_complete boolean')
        if 'usage_evidence' in receipt and not isinstance(receipt['usage_evidence'], dict):
            raise ValueError('usage_evidence must be an object when supplied')
        if receipt['cleanup_complete']:
            _evidence(receipt.get('cleanup_evidence'))
            _evidence(receipt.get('usage_evidence'))
            usage = receipt['usage_evidence']
            if 'elapsed_seconds' not in usage:
                raise ValueError('verified settlement requires actual elapsed_seconds usage evidence')
            _decimal(usage['elapsed_seconds'])

        usage = receipt.get('usage_evidence')
        if isinstance(usage, dict) and usage.get('accounting_basis') == 'observed-resources-v2':
            projections = usage.get('projections')
            if projections is not None:
                _evidence(projections)
                if projections['amount_usd'] is not None:
                    _decimal(projections['amount_usd'])

    def _validate_measured_receipt(self, receipt, run_id, require_cleanup=True):
        self._validate_receipt(receipt)
        if ((require_cleanup and not receipt['cleanup_complete'])
                or receipt.get('usage_evidence', {}).get('accounting_basis') != 'observed-resources-v2'):
            raise ValueError('usage reconciliation requires verified cleanup and observed-resources-v2 evidence')
        for name, expected in (('account', self.account), ('run_id', run_id),
                               ('repository', self.repository), ('branch', self.branch)):
            if receipt.get(name) != expected:
                raise IdentityConflict('measured receipt identity mismatch: ' + name)

    def settle(self, run_id, usage_usd, receipt, now=None):
        run_id, now = _run_id(run_id), self._now(now)
        usage, evidence = _money(usage_usd), _evidence(receipt)
        self._validate_receipt(evidence)
        if evidence.get('usage_evidence', {}).get('accounting_basis') == 'observed-resources-v2':
            self._validate_measured_receipt(evidence, run_id, require_cleanup=False)
        binding = _digest([run_id, usage, evidence])
        def apply(ledger):
            if run_id not in ledger['runs']:
                raise IdentityConflict('cannot settle unreserved RunId')
            run = ledger['runs'][run_id]
            if run['settlement_digest'] == binding:
                return False, run
            # Unknown cleanup may later be verified, but completed evidence is immutable.
            if run['receipt'] is not None and run['receipt']['cleanup_complete']:
                raise IdentityConflict('completed settlement already bound to different evidence')
            if run['usage_estimated_usd'] is not None and _decimal(usage) < _decimal(run['usage_estimated_usd']):
                raise IdentityConflict('cannot lower an existing incomplete usage estimate')
            run.update(usage_estimated_usd=usage, receipt=evidence, settled_at=_stamp(now), settlement_digest=binding)
            return True, run
        result = self._mutate(apply, now)
        self._try_publish(now)
        return result

    def reconcile_usage(self, run_id, usage_usd, receipt, previous_settlement_digest, now=None):
        """Explicit CAS correction of a completed estimate, retaining every prior receipt.

        The expected digest is the existing settlement_digest, not a caller-created
        receipt hash. History is append-only and bounded; exhaustion rejects the
        correction rather than deleting audit evidence. An exact retry remains
        idempotent even if a later correction has already been committed.
        """
        run_id, now = _run_id(run_id), self._now(now)
        usage, evidence = _money(usage_usd), _evidence(receipt)
        self._validate_measured_receipt(evidence, run_id)
        if (not isinstance(previous_settlement_digest, str)
                or not re.fullmatch(r'[a-f0-9]{64}', previous_settlement_digest)):
            raise ValueError('expected previous settlement SHA256 digest required')
        binding = _digest([run_id, usage, evidence])
        def apply(ledger):
            run = ledger['runs'].get(run_id)
            if run is None or run['receipt'] is None or not run['receipt']['cleanup_complete']:
                raise IdentityConflict('usage reconciliation requires an existing completed settlement')
            history = run.get('usage_reconciliation_history', [])
            for previous in history:
                if (previous['settlement_digest'] == previous_settlement_digest
                        and previous['replacement_settlement_digest'] == binding):
                    return False, run
            if run['settlement_digest'] != previous_settlement_digest:
                raise IdentityConflict('previous settlement digest changed; reread before reconciliation')
            if run['settlement_digest'] == binding:
                return False, run
            if len(history) >= MAX_USAGE_RECONCILIATIONS:
                raise BudgetError('usage reconciliation history capacity exhausted; no evidence discarded')
            reconciled_at = max(now, _utc(run['settled_at']),
                _utc(history[-1]['reconciled_at']) if history else _utc(run['settled_at']))
            previous = {name: copy.deepcopy(run[name]) for name in (
                'usage_estimated_usd', 'receipt', 'settled_at', 'settlement_digest')}
            previous.update(reconciled_at=_stamp(reconciled_at), replacement_settlement_digest=binding)
            run['usage_reconciliation_history'] = history + [previous]
            run.update(usage_estimated_usd=usage, receipt=evidence, settlement_digest=binding)
            return True, run
        result = self._mutate(apply, now)
        self._try_publish(now)
        return result


    def _report(self, ledger, run_id, now, policy=None):
        policy = load_budget(self.budget_path) if policy is None else policy
        cutoff = now - WEEK
        billing = copy.deepcopy(ledger['billing'])
        snapshot = billing.get('snapshot')
        totals = {name: ZERO for name in ('runs', 'shared_infrastructure', 'unattributed', 'untracked_runs')}
        recent = totals.copy()
        per_run, per_run_recent = {}, {}
        if snapshot is not None:
            for day, groups in snapshot['daily'].items():
                # Include an entire boundary UTC day, conservatively, for a rolling window.
                is_recent = day >= cutoff.date().isoformat()
                for identity, amount in groups.items():
                    amount = _decimal(amount)
                    if identity in ledger['runs']:
                        category = 'runs'
                        per_run[identity] = _sum([per_run.get(identity, ZERO), amount])
                        if is_recent:
                            per_run_recent[identity] = _sum([per_run_recent.get(identity, ZERO), amount])
                    elif identity in ('shared', 'shared-infrastructure'):
                        category = 'shared_infrastructure'
                    elif not identity:
                        category = 'unattributed'
                    else:
                        category = 'untracked_runs'
                    totals[category] = _sum([totals[category], amount])
                    if is_recent:
                        recent[category] = _sum([recent[category], amount])
        late_recent = {}
        for adjustment in billing.get('late_usage_adjustments', []):
            if _utc(adjustment['observed_at']) >= cutoff:
                identity = adjustment['run_id']
                late_recent[identity] = _sum([late_recent.get(identity, ZERO), _decimal(adjustment['amount_usd'])])
        charge, reservations, usage_lifetime, usage_recent = ZERO, ZERO, ZERO, ZERO
        selected, unresolved, estimated_runs = {}, 0, 0
        for identity, run in ledger['runs'].items():
            cleaned = run['receipt'] is not None and run['receipt']['cleanup_complete']
            usage = _decimal(run['usage_estimated_usd'] or '0')
            recent_usage = usage if run['settled_at'] and _utc(run['settled_at']) >= cutoff else ZERO
            billed = per_run.get(identity, ZERO)
            billed_recent = per_run_recent.get(identity, ZERO)
            late = late_recent.get(identity, ZERO)
            held = ZERO if cleaned else _decimal(run['reserved_usd'])
            conservative = (_sum([max(recent_usage, billed_recent), late]) if cleaned
                            else max(held, usage, billed, _sum([billed_recent, late])))
            charge = _sum([charge, conservative])
            reservations = _sum([reservations, held])
            usage_lifetime = _sum([usage_lifetime, usage])
            usage_recent = _sum([usage_recent, recent_usage])
            estimated_runs += run['usage_estimated_usd'] is not None
            if not cleaned:
                unresolved += 1
            if run_id is None or run_id == identity:
                selected[identity] = copy.deepcopy(run) | self._usage_presentation(run) | dict(cleanup_verified=cleaned,
                    active_reservation_usd=_money(held), reservation_released=cleaned,
                    conservative_budget_charge_usd=_money(conservative),
                    late_billing_adjustment_recent_usd=_money(late),
                    aws_billed_to_date_usd=_money(billed) if snapshot is not None else None,
                    aws_billed_recent_usd=_money(billed_recent) if snapshot is not None else None,
                    aws_billing_final=False)
        extra = _sum(recent[name] for name in ('shared_infrastructure', 'unattributed', 'untracked_runs'))
        charge = _sum([charge, extra])
        limit = _decimal(policy['weekly_budget_usd'])
        with localcontext() as context:
            context.prec = 60
            available = max(ZERO, limit - charge)
        branch_billing = dict(status=billing['status'], final=False, currency='USD',
            lifetime_total_usd=_money(_sum(totals.values())) if snapshot is not None else None,
            recent_total_usd=_money(_sum(recent.values())) if snapshot is not None else None,
            lifetime_by_scope_usd={key: _money(value) for key, value in totals.items()} if snapshot is not None else None,
            recent_by_scope_usd={key: _money(value) for key, value in recent.items()} if snapshot is not None else None,
            observed_at=snapshot['observed_at'] if snapshot is not None else None,
            coverage_start=snapshot['start'] if snapshot is not None else None,
            coverage_end_exclusive=snapshot['end'] if snapshot is not None else None,
            estimated=snapshot['estimated'] if snapshot is not None else None,
            last_attempt=billing.get('last_attempt'), error=billing.get('error'),
            tag_coverage_complete=snapshot['tag_coverage_complete'] if snapshot is not None else False,
            conservative_merged_observations=snapshot.get('conservative_merge', False) if snapshot is not None else False,
            late_usage_adjustments_recent_usd=_money(_sum(late_recent.values())),
            note=('Delayed tag-attributed AWS billed-to-date since ledger initialization, never final; '
                  'zero does not prove no spend. Missing RunId is unattributed. Costs lacking '
                  'Repository/BenchmarkBranch allocation tags, including unallocatable API overhead, are excluded. '
                  'Partial tag coverage conservatively retains higher earlier daily/group observations.'))
        summary = self._costs_summary(ledger, now)
        return dict(account=self.account, repository=self.repository, branch=self.branch,
                    as_of=_stamp(now), ledger_key=self.key, revision=ledger['revision'],
                    budget_policy=policy, weekly_budget_usd=_money(limit),
                    conservative_budget_charge_usd=_money(charge), available_usd=_money(available),
                    admission_reservations_usd=_money(reservations),
                    usage_estimated_usd=_money(usage_lifetime) if estimated_runs else None,
                    usage_estimated_recent_usd=_money(usage_recent) if estimated_runs else None, unresolved_runs=unresolved,
                    observed_usage_estimated_usd=summary['total']['observed_usage_estimated_usd'],
                    legacy_usage_estimated_usd=summary['total']['legacy_usage_estimated_usd'],
                    projected_usd=summary['total']['projected_usd'],
                    usage_coverage_complete=summary['total']['usage_coverage_complete'],
                    projection_coverage_complete=summary['total']['projection_coverage_complete'],
                    branch_billing=branch_billing, runs=selected,
                    generated_report_key=self.report_key,
                    generated_report_uri='s3://' + self.bucket + '/' + self.report_key,
                    aggregate_publication=copy.deepcopy(self.last_publication),
                    costsSummary=summary,
                    policy='unresolved max(reserved,usage,billed); verified cleanup releases hold; closed recent max(usage,billed), plus late old-bill increments once')

    def report(self, run_id=None, now=None):
        if run_id is not None:
            _run_id(run_id)
        ledger, _ = self._load()
        if run_id is not None and run_id not in ledger['runs']:
            raise IdentityConflict('unknown RunId')
        return self._report(ledger, run_id, self._now(now))

    @staticmethod
    def _usage_presentation(run):
        evidence = (run['receipt'] or {}).get('usage_evidence') or {}
        basis = evidence.get('accounting_basis', 'legacy-estimate' if run['usage_estimated_usd'] is not None else 'unavailable')
        observed = basis == 'observed-resources-v2'
        projections = evidence.get('projections') if observed else None
        coverage = evidence.get('coverage', 'partial' if observed else 'unavailable')
        return dict(usage_accounting_basis=basis, usage_coverage=coverage,
            observed_usage_estimated_usd=run['usage_estimated_usd'] if observed else None,
            legacy_usage_estimated_usd=run['usage_estimated_usd'] if not observed else None,
            projected_usd=projections.get('amount_usd') if projections else None,
            projections=copy.deepcopy(projections))

    def _costs_summary(self, ledger, now):
        """Derived calendar accounting; amounts of different kinds never sum."""
        first = _utc(ledger['created_at']).date()
        last = max(now.date(), _utc(ledger['updated_at']).date())
        if not 0 <= (last - first).days < self.max_report_days:
            raise BudgetError('generated report calendar history bound exceeded')
        snapshot = ledger['billing'].get('snapshot')
        daily = {}
        def bucket(start, end):
            return dict(start=start.isoformat(), end_exclusive=end.isoformat(),
                        admission_reserved_usd='0', active_reservation_usd='0',
                        usage_estimated_usd=None, observed_usage_estimated_usd=None,
                        legacy_usage_estimated_usd=None, projected_usd=None,
                        aws_billed_to_date_usd=None, reservation_run_ids=[],
                        active_reservation_run_ids=[], usage_estimated_run_ids=[],
                        observed_usage_run_ids=[], legacy_usage_run_ids=[], projected_run_ids=[],
                        unknown_usage_run_ids=[], partial_usage_run_ids=[], unknown_projection_run_ids=[],
                        aws_billing_final=False,
                        aws_billing_status=ledger['billing']['status'],
                        aws_coverage_start=None, aws_coverage_end_exclusive=None)
        for offset in range((last - first).days + 1):
            day = first + timedelta(days=offset)
            daily[day.isoformat()] = bucket(day, day + timedelta(days=1))
        for run_id, run in sorted(ledger['runs'].items()):
            day = _utc(run['reserved_at']).date().isoformat()
            item = daily[day]
            item['admission_reserved_usd'] = _money(_sum([
                _decimal(item['admission_reserved_usd']), _decimal(run['reserved_usd'])]))
            item['reservation_run_ids'].append(run_id)
            cleaned = run['receipt'] is not None and run['receipt']['cleanup_complete']
            if not cleaned:
                item['active_reservation_usd'] = _money(_sum([
                    _decimal(item['active_reservation_usd']), _decimal(run['reserved_usd'])]))
                item['active_reservation_run_ids'].append(run_id)
            presentation = self._usage_presentation(run)
            if run['usage_estimated_usd'] is None:
                item['unknown_usage_run_ids'].append(run_id)
                item['unknown_projection_run_ids'].append(run_id)
            if run['usage_estimated_usd'] is not None:
                item = daily[_utc(run['settled_at']).date().isoformat()]
                item['usage_estimated_usd'] = _money(_sum([
                    _decimal(item['usage_estimated_usd'] or '0'), _decimal(run['usage_estimated_usd'])]))
                item['usage_estimated_run_ids'].append(run_id)
                observed = presentation['usage_accounting_basis'] == 'observed-resources-v2'
                category = 'observed' if observed else 'legacy'
                name = category + '_usage_estimated_usd'
                item[name] = _money(_sum([_decimal(item[name] or '0'), _decimal(run['usage_estimated_usd'])]))
                item[category + '_usage_run_ids'].append(run_id)
                if presentation['usage_coverage'] != 'complete':
                    item['partial_usage_run_ids'].append(run_id)
                if presentation['projected_usd'] is not None:
                    item['projected_usd'] = _money(_sum([
                        _decimal(item['projected_usd'] or '0'), _decimal(presentation['projected_usd'])]))
                    item['projected_run_ids'].append(run_id)
                else:
                    item['unknown_projection_run_ids'].append(run_id)
        if snapshot is not None:
            for day, groups in snapshot['daily'].items():
                item = daily[day]
                item['aws_billed_to_date_usd'] = _money(_sum(_decimal(value) for value in groups.values()))
                item['aws_coverage_start'] = day
                item['aws_coverage_end_exclusive'] = (datetime.fromisoformat(day).date() + timedelta(days=1)).isoformat()
        def combine(target, item):
            for name in ('admission_reserved_usd', 'active_reservation_usd', 'usage_estimated_usd',
                         'observed_usage_estimated_usd', 'legacy_usage_estimated_usd', 'projected_usd',
                         'aws_billed_to_date_usd'):
                if item[name] is not None:
                    target[name] = _money(_sum([_decimal(target[name] or '0'), _decimal(item[name])]))
            for name in ('reservation_run_ids', 'active_reservation_run_ids', 'usage_estimated_run_ids',
                         'observed_usage_run_ids', 'legacy_usage_run_ids', 'projected_run_ids',
                         'unknown_usage_run_ids', 'partial_usage_run_ids', 'unknown_projection_run_ids'):
                target[name] = sorted(set(target[name]) | set(item[name]))
            if item['aws_coverage_start'] is not None:
                target['aws_coverage_start'] = min(filter(None, [
                    target['aws_coverage_start'], item['aws_coverage_start']]))
                target['aws_coverage_end_exclusive'] = max(filter(None, [
                    target['aws_coverage_end_exclusive'], item['aws_coverage_end_exclusive']]))
        monthly, weekly = {}, {}
        total = bucket(first, last + timedelta(days=1))
        for day, item in daily.items():
            parsed = datetime.fromisoformat(day).date()
            month = parsed.replace(day=1)
            month_end = (month.replace(day=28) + timedelta(days=4)).replace(day=1)
            week = parsed - timedelta(days=parsed.weekday())
            month_key, week_key = month.isoformat(), week.isoformat()
            if month_key not in monthly:
                monthly[month_key] = bucket(month, month_end)
            if week_key not in weekly:
                weekly[week_key] = bucket(week, week + timedelta(days=7))
            combine(monthly[month_key], item)
            combine(weekly[week_key], item)
            combine(total, item)
        for item in [total, *monthly.values(), *weekly.values(), *daily.values()]:
            item['usage_coverage_complete'] = bool(item['usage_estimated_run_ids']
                and not item['unknown_usage_run_ids'] and not item['partial_usage_run_ids'])
            item['projection_coverage_complete'] = bool(item['projected_run_ids']
                and not item['unknown_projection_run_ids'])
            item['aws_coverage_complete'] = bool(snapshot is not None
                and snapshot['tag_coverage_complete']
                and item['aws_coverage_start'] == item['start']
                and item['aws_coverage_end_exclusive'] == item['end_exclusive'])
        return dict(currency='USD', as_of=_stamp(now), total=total,
                    monthly=list(monthly.values()), weekly=list(weekly.values()),
                    daily=list(daily.values()), week_starts='Monday', timezone='UTC',
                    estimate_attribution='latest corrected usage on original settlement UTC date; original reservations on reservation date; active holds are current as-of',
                    projection_attribution='future estimates from latest receipt, displayed with its original settlement date; excluded from usage and admission charges',
                    billing_attribution='Cost Explorer UTC daily usage interval; delayed, never final',
                    measures='Historical reservations, current active holds, observed estimates, legacy estimates, projections and AWS bills are distinct; never add them together',
                    lifetime_start=first.isoformat(), initialized_at=ledger['created_at'],
                    unique_run_count=len(ledger['runs']))

    def _read_generated(self):
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=self.report_key,
                                          ExpectedBucketOwner=self.account)
        except Exception as error:
            if _error_code(error) in ('NoSuchKey', '404'):
                return None, None
            raise LedgerUnavailable('cannot read generated aggregate: ' + str(error)) from error
        with response['Body'] as body:
            if response.get('ContentLength', 0) > self.max_bytes:
                raise LedgerMalformed('oversized generated aggregate')
            raw = body.read(self.max_bytes + 1)
        if len(raw) > self.max_bytes or not response.get('ETag'):
            raise LedgerMalformed('oversized generated aggregate or absent CAS ETag')
        try:
            value = json.loads(raw)
        except (ValueError, UnicodeError):
            # A derived report can be repaired, but only under its exact ETag.
            return {'malformed': True}, response['ETag']
        if not isinstance(value, dict):
            return {'malformed': True}, response['ETag']
        if (value.get('account') != self.account or value.get('repository') != self.repository
                or value.get('branch') != self.branch):
            raise IdentityConflict('generated aggregate belongs to another identity')
        return value, response['ETag']

    def _generated(self, ledger, now, policy):
        value = self._report(ledger, None, now, policy=policy)
        names = ('reserved_at', 'reserved_usd', 'settled_at', 'usage_estimated_usd',
                 'budget_policy', 'cleanup_verified', 'active_reservation_usd', 'reservation_released',
                 'usage_accounting_basis', 'usage_coverage', 'observed_usage_estimated_usd',
                 'legacy_usage_estimated_usd', 'projected_usd', 'projections',
                 'conservative_budget_charge_usd', 'late_billing_adjustment_recent_usd',
                 'aws_billed_to_date_usd', 'aws_billed_recent_usd')
        value['runs'] = {run_id: {name: record[name] for name in names}
                         for run_id, record in value['runs'].items()}
        value.update(schema=1, generated=True, ledger_revision=ledger['revision'],
                     ledger_sha256=_digest(ledger),
                     aggregate_publication=dict(status='published', key=self.report_key,
                         uri='s3://' + self.bucket + '/' + self.report_key,
                         ledger_revision=ledger['revision']))
        return value

    def publish_report(self, now=None):
        """CAS-publish a complete rebuild; never trust stored running totals.

        Read report BEFORE ledger, so an old publisher cannot CAS over a report
        derived from a newer revision. Recheck the authoritative ledger after
        writing and rebuild if it advanced. A separate S3 object cannot be
        transactionally simultaneous with every ledger update; revisions and
        source hashes make any lag explicit and safely recoverable.
        """
        requested = self._now(now)
        for attempt in range(self.max_attempts):
            previous, etag = self._read_generated()
            ledger, source_etag = self._load()
            policy = load_budget(self.budget_path)
            as_of = max(requested, _utc(ledger['updated_at']))
            if previous and isinstance(previous.get('ledger_revision'), int):
                if previous['ledger_revision'] > ledger['revision']:
                    raise BudgetError('aggregate revision exceeds authoritative ledger; refusing rollback')
                if previous.get('as_of'):
                    try:
                        as_of = max(as_of, _utc(previous['as_of']))
                    except ValueError:
                        pass
            value = self._generated(ledger, as_of, policy)
            repaired = bool(previous and previous.get('malformed'))
            if (previous and previous.get('ledger_revision') == ledger['revision']
                    and previous.get('ledger_sha256') == value['ledger_sha256']):
                try:
                    expected = self._costs_summary(ledger, _utc(previous['as_of']))
                    repaired = previous.get('costsSummary') != expected
                except (KeyError, TypeError, ValueError, BudgetError):
                    repaired = True
            if previous != value:
                raw = _json(value)
                if len(raw) > self.max_bytes:
                    raise BudgetError('generated aggregate capacity exceeded')
                kwargs = dict(Bucket=self.bucket, Key=self.report_key, Body=raw,
                    ExpectedBucketOwner=self.account, ContentType='application/json',
                    ServerSideEncryption='aws:kms' if self.kms_key_id else 'AES256')
                if self.kms_key_id:
                    kwargs['SSEKMSKeyId'] = self.kms_key_id
                if etag is None:
                    kwargs['IfNoneMatch'] = '*'
                else:
                    kwargs['IfMatch'] = etag
                try:
                    self.s3.put_object(**kwargs)
                except Exception as error:
                    if _error_code(error) in ('PreconditionFailed', 'ConditionalRequestConflict', '412', '409'):
                        if attempt + 1 < self.max_attempts:
                            self.sleeper(min(0.01 * 2 ** attempt, 0.25))
                        continue
                    try:
                        observed, _ = self._read_generated()
                    except BudgetError as read_error:
                        raise UnknownWrite('aggregate publication outcome unknown') from read_error
                    if observed != value:
                        raise UnknownWrite('aggregate publication not confirmed') from error
            latest, latest_etag = self._load()
            if latest_etag != source_etag or latest['revision'] != ledger['revision']:
                continue
            if load_budget(self.budget_path)['sha256'] != policy['sha256']:
                continue
            self.last_publication = value['aggregate_publication'] | {'repaired': repaired}
            return value
        raise LedgerUnavailable('aggregate publication contention exceeded bounded retries')

    def _try_publish(self, now):
        try:
            self.publish_report(now=now)
        except Exception as error:
            self.last_publication = dict(status='failed', key=self.report_key,
                uri='s3://' + self.bucket + '/' + self.report_key,
                error=(type(error).__name__ + ': ' + str(error))[:2048])

    def _pages(self, client, method, params):
        if method == 'get_cost_and_usage':
            def fetch_page(**kwargs):
                return client.get_cost_and_usage(**kwargs)
            operation, result_key, token = 'GetCostAndUsage', 'ResultsByTime', 'NextPageToken'
            paginator = (client.get_paginator('get_cost_and_usage')
                         if client.can_paginate('get_cost_and_usage') else None)
        elif method == 'list_cost_allocation_tags':
            def fetch_page(**kwargs):
                return client.list_cost_allocation_tags(**kwargs)
            operation, result_key, token = 'ListCostAllocationTags', 'CostAllocationTags', 'NextToken'
            paginator = (client.get_paginator('list_cost_allocation_tags')
                         if client.can_paginate('list_cost_allocation_tags') else None)
        else:
            raise ValueError('unsupported billing pagination operation')
        if paginator is None:
            # CE operations lacking a modeled boto3 paginator still use the SDK
            # paginator rather than a handwritten NextPageToken loop.
            from botocore.paginate import Paginator
            paginator = Paginator(fetch_page,
                {'input_token': token, 'output_token': token, 'result_key': result_key},
                client.meta.service_model.operation_model(operation))
        for index, page in enumerate(paginator.paginate(**params)):
            if index >= self.max_pages:
                raise BudgetError('Cost Explorer page bound exceeded; snapshot not replaced')
            yield page

    def reconcile(self, ce, now=None):
        """Persist a provisional CE snapshot or visible failure; never release holds.

        Reads the current UTC runtime clock before ANY billing API call. A supplied
        as-of time must agree within five minutes; tests may inject the clock.
        Failed/inactive/partial-tag queries retain, but visibly stale, prior data.
        """
        runtime = _utc(self.clock())
        now = runtime if now is None else _utc(now)
        if abs((runtime - now).total_seconds()) > 300:
            raise ValueError('billing as-of time does not match current runtime UTC clock')
        ledger, _ = self._load()
        start, end = _utc(ledger['created_at']).date(), now.date()
        try:
            if not 0 <= (end - start).days <= self.max_billing_days:
                raise BudgetError('billing lifetime exceeds bounded query horizon; no partial lifetime substitution')
            required, active, tag_count, activation_dates = {'Repository', 'BenchmarkBranch', 'RunId'}, set(), 0, {}
            for page in self._pages(ce, 'list_cost_allocation_tags', {'Type': 'UserDefined', 'Status': 'Active'}):
                for tag in page.get('CostAllocationTags', []):
                    tag_count += 1
                    if tag_count > self.max_groups:
                        raise BudgetError('cost allocation tag bound exceeded')
                    if tag.get('Status') == 'Active':
                        active.add(tag['TagKey'])
                        if tag['TagKey'] in required and tag.get('LastUpdatedDate'):
                            activation_dates[tag['TagKey']] = _utc(tag['LastUpdatedDate'])
            if not required <= active:
                raise BudgetError('cost allocation tags inactive/unavailable: ' + ','.join(sorted(required - active)))
            daily, estimated, seen, group_count = {}, False, set(), 0
            if start < end:
                params = dict(TimePeriod={'Start': start.isoformat(), 'End': end.isoformat()},
                    Granularity='DAILY', Metrics=['UnblendedCost'], GroupBy=[{'Type': 'TAG', 'Key': 'RunId'}],
                    Filter={'And': [
                        {'Dimensions': {'Key': 'LINKED_ACCOUNT', 'Values': [self.account]}},
                        {'Tags': {'Key': 'Repository', 'Values': [self.repository], 'MatchOptions': ['EQUALS']}},
                        {'Tags': {'Key': 'BenchmarkBranch', 'Values': [self.branch], 'MatchOptions': ['EQUALS']}},
                        {'Not': {'Dimensions': {'Key': 'RECORD_TYPE', 'Values': ['Credit', 'Refund']}}}]})
                for page in self._pages(ce, 'get_cost_and_usage', params):
                    for result in page.get('ResultsByTime', []):
                        day = result['TimePeriod']['Start']
                        day_end = result['TimePeriod']['End']
                        if (not start.isoformat() <= day < end.isoformat()
                                or (datetime.fromisoformat(day_end) - datetime.fromisoformat(day)).days != 1
                                or not isinstance(result.get('Estimated'), bool)):
                            raise BudgetError('invalid Cost Explorer daily interval/estimated status')
                        seen.add(day)
                        estimated = estimated or result['Estimated']
                        groups = daily.setdefault(day, {})
                        for group in result.get('Groups', []):
                            group_count += 1
                            if group_count > self.max_groups:
                                raise BudgetError('billing group bound exceeded')
                            keys = group['Keys']
                            metric = group['Metrics']['UnblendedCost']
                            if len(keys) != 1 or not keys[0].startswith('RunId$') or metric['Unit'] != 'USD':
                                raise BudgetError('unsupported billing tag keys/currency')
                            identity = keys[0].split('$', 1)[1]
                            # A group/day must occur once, even across pages.
                            if identity in groups:
                                raise BudgetError('duplicate daily cost group; refusing double count')
                            groups[identity] = _money(metric['Amount'])
                if len(seen) != (end - start).days:
                    raise BudgetError('incomplete Cost Explorer daily coverage')
            coverage_complete = all(key in activation_dates and activation_dates[key].date() <= start for key in required)
            snapshot = dict(start=start.isoformat(), end=end.isoformat(), observed_at=_stamp(now),
                            currency='USD', estimated=estimated, final=False, daily=daily,
                            runtime_date_verified=runtime.date().isoformat(), tags_active=sorted(required),
                            tag_coverage_complete=coverage_complete)
            update = dict(status='available' if coverage_complete else 'partial', snapshot=snapshot,
                          last_attempt=_stamp(now), error=None if coverage_complete else
                          'Cost tag activation history does not prove complete coverage since ledger initialization')
        except Exception as error:
            update = dict(status='unavailable', snapshot=ledger['billing'].get('snapshot'),
                          last_attempt=_stamp(now), error=(type(error).__name__ + ': ' + str(error))[:2048])
        def apply(current):
            if update['status'] == 'unavailable':
                update['snapshot'] = current['billing'].get('snapshot')
            elif update['status'] == 'partial' and current['billing'].get('snapshot') is not None:
                previous = current['billing']['snapshot']
                merged = copy.deepcopy(update['snapshot'])
                if previous['start'] != merged['start'] or previous['end'] > merged['end']:
                    raise BudgetError('partial billing cannot narrow prior accounting coverage')
                for day, groups in previous['daily'].items():
                    observed_groups = merged['daily'][day]
                    for identity, amount in groups.items():
                        observed_groups[identity] = _money(max(_decimal(amount),
                            _decimal(observed_groups.get(identity, '0'))))
                merged['conservative_merge'] = True
                merged['estimated'] = merged['estimated'] or previous['estimated']
                update['snapshot'] = merged
            update['run_billed_high_water_usd'] = copy.deepcopy(current['billing'].get('run_billed_high_water_usd', {}))
            update['late_usage_adjustments'] = copy.deepcopy(current['billing'].get('late_usage_adjustments', []))
            if update['status'] != 'unavailable':
                previous_daily = (current['billing'].get('snapshot') or {}).get('daily', {})
                cutoff_day = (now - WEEK).date().isoformat()
                for identity, run in current['runs'].items():
                    previous_total = _sum(_decimal(groups.get(identity, '0')) for groups in previous_daily.values())
                    billed_total = _sum(_decimal(groups.get(identity, '0')) for groups in update['snapshot']['daily'].values())
                    high_water = max(previous_total, _decimal(update['run_billed_high_water_usd'].get(identity, '0')))
                    if (run['receipt'] is not None and run['receipt']['cleanup_complete']
                            and _utc(run['settled_at']) < now - WEEK):
                        # Estimates and ordinary recent usage bills already cover
                        # their own dollars. Only newly observed excess for old
                        # usage dates needs a separate rolling admission charge.
                        # High-water marks prevent rebilling corrected/repeated
                        # observations. The original CE snapshot remains intact.
                        with localcontext() as context:
                            context.prec = 60
                            old_increases = _sum(max(ZERO, _decimal(groups.get(identity, '0'))
                                - _decimal(previous_daily.get(day, {}).get(identity, '0')))
                                for day, groups in update['snapshot']['daily'].items() if day < cutoff_day)
                            billed_recent = _sum(_decimal(groups.get(identity, '0'))
                                for day, groups in update['snapshot']['daily'].items() if day >= cutoff_day)
                            excess = max(ZERO, billed_total - max(high_water,
                                _sum([_decimal(run['usage_estimated_usd']), billed_recent])))
                            adjustment = min(old_increases, excess)
                        if adjustment:
                            if len(update['late_usage_adjustments']) >= self.max_groups:
                                raise BudgetError('late billing history capacity exhausted; no evidence discarded')
                            update['late_usage_adjustments'].append(dict(run_id=identity,
                                observed_at=_stamp(now), amount_usd=_money(adjustment)))
                    update['run_billed_high_water_usd'][identity] = _money(max(high_water, billed_total))
            if current['billing'] == update:
                return False, current['billing']
            # Do not replace a newer observation from a concurrent reconciler.
            if current['billing'].get('last_attempt') and _utc(current['billing']['last_attempt']) > now:
                raise BudgetError('newer billing reconciliation already persisted')
            current['billing'] = update
            return True, current['billing']
        self._mutate(apply, now)
        self._try_publish(now)
        return self.report(now=now)


def github_summary(report):
    billing = report['branch_billing']
    def shown(value):
        return 'unavailable' if value is None else '$' + value
    rows = [('Weekly budget (budget file)', shown(report['weekly_budget_usd'])),
            ('Conservative budget charge', shown(report['conservative_budget_charge_usd'])),
            ('Available for admission', shown(report['available_usd'])),
            ('Admission reservations — active holds only (not additive)', shown(report['admission_reservations_usd'])),
            ('Usage-estimated lifetime (not additive; not an AWS bill)', shown(report['usage_estimated_usd'])),
            ('Observed-resource usage estimate', shown(report['observed_usage_estimated_usd'])),
            ('Legacy usage estimate — uncorrected assumptions', shown(report['legacy_usage_estimated_usd'])),
            ('Future projections — excluded from usage and admission', shown(report['projected_usd'])),
            ('Usage coverage complete', str(report['usage_coverage_complete'])),
            ('AWS billed-to-date tracked branch lifetime', shown(billing['lifetime_total_usd'])),
            ('AWS billed-to-date branch recent', shown(billing['recent_total_usd'])),
            ('Billing status / final', billing['status'] + ' / never final'),
            ('Billing UTC coverage (end exclusive)', (billing['coverage_start'] or 'unavailable') + ' .. ' + (billing['coverage_end_exclusive'] or 'unavailable')),
            ('Unresolved cleanup reservations', str(report['unresolved_runs']))]
    periods = report['costsSummary']
    for label, item in [('Total', periods['total']),
                        ('Current UTC month', periods['monthly'][-1]),
                        ('Current Monday UTC week', periods['weekly'][-1]),
                        ('Current UTC day', periods['daily'][-1])]:
        rows.append((label + ' (' + item['start'] + ' .. ' + item['end_exclusive'] + ')',
                     'observed usage estimate ' + shown(item['observed_usage_estimated_usd'])
                     + '; legacy estimate ' + shown(item['legacy_usage_estimated_usd'])
                     + '; future projection ' + shown(item['projected_usd'])
                     + '; active holds ' + shown(item['active_reservation_usd'])
                     + '; historical reservation ' + shown(item['admission_reserved_usd'])
                     + '; delayed AWS bill ' + shown(item['aws_billed_to_date_usd'])))
    rows.append(('Generated aggregate JSON', report['generated_report_uri']))
    text = '## AWS benchmark budget\n\n| Measure | Value |\n|---|---|\n'
    text += ''.join('| ' + name + ' | ' + str(value).replace('|', '\\|').replace('\n', ' ') + ' |\n' for name, value in rows)
    if billing['lifetime_by_scope_usd'] is not None:
        text += '\nAWS billed-to-date scopes (already included above):\n'
        text += ''.join('- ' + name + ': $' + value + '\n' for name, value in billing['lifetime_by_scope_usd'].items())
    text += '\n' + billing['note'] + '\n'
    if billing['error']:
        text += '\nBilling reconciliation error: ' + billing['error'].replace('\n', ' ').replace('`', '') + '\n'
    return text


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['report', 'reconcile', 'initialize', 'publish-report'])
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--account', required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--repository', default=REPOSITORY)
    parser.add_argument('--branch', default=BRANCH)
    parser.add_argument('--budget-file', type=Path, default=DEFAULT_BUDGET)
    parser.add_argument('--key')
    parser.add_argument('--kms-key-id')
    parser.add_argument('--run-id')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--format', choices=['json', 'github'], default='json')
    args = parser.parse_args(argv)
    import boto3
    from botocore.config import Config
    session = boto3.Session(region_name=args.region)
    config = Config(connect_timeout=5, read_timeout=15, retries={'total_max_attempts': 2, 'mode': 'standard'})
    ledger = BudgetLedger(session.client('s3', config=config), args.bucket, args.account,
        repository=args.repository, branch=args.branch, budget_path=args.budget_file,
        key=args.key, kms_key_id=args.kms_key_id)
    try:
        if args.command == 'initialize':
            report = ledger.initialize()
        elif args.command == 'publish-report':
            report = ledger.publish_report()
        elif args.command == 'reconcile':
            report = ledger.reconcile(session.client('ce', region_name='us-east-1', config=config))
        else:
            report = ledger.report(run_id=args.run_id)
        if args.output:
            if args.command != 'publish-report':
                raise ValueError('--output is only supported by publish-report')
            args.output.parent.mkdir(parents=True, exist_ok=True)
            temporary = None
            try:
                with tempfile.NamedTemporaryFile(mode='wb', dir=args.output.parent, delete=False) as target:
                    temporary = Path(target.name)
                    target.write(_json(report) + b'\n')
                temporary.chmod(0o444)
                os.replace(temporary, args.output)
                temporary = None
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
        print(github_summary(report) if args.format == 'github' else json.dumps(report, indent=2, sort_keys=True))
        return 2 if args.command == 'reconcile' and report['branch_billing']['status'] != 'available' else 0
    except (BudgetError, ValueError, OSError) as error:
        print('AWS budget: ' + str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
