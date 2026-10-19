"""Offline behavioral tests for admission serialization and delayed AWS billing."""
import copy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

import ci_budget as budget

NOW = datetime(2026, 10, 18, 12, tzinfo=timezone.utc)
ACCOUNT = '354775971805'
RUN_A = 'th-' + 'a' * 16
RUN_B = 'th-' + 'b' * 16
RUN_C = 'th-' + 'c' * 16
PROVENANCE = {'source_sha256': 'f' * 64, 'fleet': 6, 'ttl_seconds': 3600,
              'rate_evidence': {'usd_per_hour': '1.25', 'observed_at': NOW.isoformat()}}


def receipt(clean=True):
    return {'cleanup_complete': clean,
            'cleanup_evidence': {'instances': {'i-example': 'terminated'}, 'observed_at': NOW.isoformat()},
            'usage_evidence': {'elapsed_seconds': '600', 'hourly_rate_usd': '1.25', 'count': 6}}


def measured_receipt(run_id=RUN_A, clean=True, projection='0.01'):
    return dict(receipt(clean), account=ACCOUNT, run_id=run_id,
        repository=budget.REPOSITORY, branch=budget.BRANCH,
        usage_evidence=dict(accounting_basis='observed-resources-v2', elapsed_seconds='0',
            coverage='partial', ec2={'instances': [], 'seconds': '0'},
            s3={'input_bytes': 245450666, 'coverage': 'partial',
                'unknown_components': ['request_counts', 'transfer_usage']},
            projections={'amount_usd': projection, 's3_retention_days': 30,
                'basis': 'future retained-current-bytes storage only; excluded from usage_usd'},
            billing={'status': 'unavailable', 'amount_usd': None}))


class ServiceError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {'Error': {'Code': code}}


class Body(io.BytesIO):
    def __init__(self, value):
        super().__init__(value)
        self.read_sizes = []

    def read(self, size=-1):
        self.read_sizes.append(size)
        return super().read(size)


class S3:
    """A strongly-consistent, conditional-write object store, not a ledger stub."""
    def __init__(self):
        self.raw = None
        self.generation = 0
        self.lock = threading.Lock()
        self.puts, self.reads, self.bodies = [], [], []
        self.write_mode = None
        self.read_error = None
        self.conflicts = 0
        self.barrier = None
        self.barrier_readers = set()
        self.report_objects = {}
        self.report_puts, self.report_reads = [], []
        self.report_error = None
        self.report_write_mode = None
        self.report_before_put = None

    def get_object(self, **kwargs):
        if '/reports/' in kwargs['Key']:
            with self.lock:
                self.report_reads.append(kwargs)
                if self.read_error or self.report_error:
                    raise self.read_error or self.report_error
                stored = self.report_objects.get(kwargs['Key'])
                if stored is None:
                    raise ServiceError('NoSuchKey')
                body = Body(stored['raw'])
                self.bodies.append(body)
                return {'Body': body, 'ETag': '"' + str(stored['generation']) + '"',
                        'ContentLength': len(stored['raw'])}
        with self.lock:
            self.reads.append(kwargs)
            if self.read_error:
                raise self.read_error
            if self.raw is None:
                raise ServiceError('NoSuchKey')
            body = Body(self.raw)
            self.bodies.append(body)
            result = {'Body': body, 'ETag': '"' + str(self.generation) + '"', 'ContentLength': len(self.raw)}
            name = threading.current_thread().name
            barrier = self.barrier if self.barrier is not None and name not in self.barrier_readers else None
            if barrier is not None:
                self.barrier_readers.add(name)
        if barrier is not None:
            barrier.wait(timeout=5)
        return result

    def put_object(self, **kwargs):
        if '/reports/' in kwargs['Key']:
            if self.report_before_put is not None:
                self.report_before_put(kwargs)
            with self.lock:
                self.report_puts.append(kwargs)
                if self.report_error:
                    raise self.report_error
                stored = self.report_objects.get(kwargs['Key'])
                etag = '"' + str(stored['generation']) + '"' if stored is not None else None
                if kwargs.get('IfNoneMatch') == '*':
                    if stored is not None:
                        raise ServiceError('PreconditionFailed')
                elif stored is None or kwargs.get('IfMatch') != etag:
                    raise ServiceError('PreconditionFailed')
                mode, self.report_write_mode = self.report_write_mode, None
                if mode == 'unknown_uncommitted':
                    raise TimeoutError('aggregate request outcome unknown')
                generation = stored['generation'] + 1 if stored is not None else 1
                self.report_objects[kwargs['Key']] = {'raw': kwargs['Body'], 'generation': generation}
                if mode == 'unknown_committed':
                    raise TimeoutError('aggregate response lost')
                return {'ETag': '"' + str(generation) + '"'}
        with self.lock:
            self.puts.append(kwargs)
            etag = '"' + str(self.generation) + '"'
            if self.conflicts:
                self.conflicts -= 1
                raise ServiceError('ConditionalRequestConflict')
            if kwargs.get('IfNoneMatch') == '*':
                if self.raw is not None:
                    raise ServiceError('PreconditionFailed')
            elif kwargs.get('IfMatch') != etag or self.raw is None:
                raise ServiceError('PreconditionFailed')
            mode, self.write_mode = self.write_mode, None
            if mode == 'unknown_uncommitted':
                raise TimeoutError('request outcome unknown')
            self.raw = kwargs['Body']
            self.generation += 1
            if mode == 'unknown_committed':
                raise TimeoutError('response lost after commit')
            if mode == 'unknown_unreadable':
                self.read_error = OSError('S3 unavailable after write')
                raise TimeoutError('response lost after commit')
            return {'ETag': '"' + str(self.generation) + '"'}

    def replace(self, value):
        with self.lock:
            self.raw = value if isinstance(value, bytes) else json.dumps(value).encode()
            self.generation += 1


class Pages:
    def __init__(self, ce, method):
        self.ce, self.method = ce, method

    def paginate(self, **kwargs):
        self.ce.calls.append((self.method, kwargs))
        pages = self.ce.tag_pages if self.method == 'list_cost_allocation_tags' else self.ce.cost_pages
        for page in pages:
            self.ce.pages_read += 1
            if isinstance(page, Exception):
                raise page
            yield copy.deepcopy(page)


class CE:
    def __init__(self, cost_pages=None):
        self.tag_pages = [{'CostAllocationTags': [
            {'TagKey': key, 'Status': 'Active', 'LastUpdatedDate': (NOW - timedelta(days=30)).isoformat()}
            for key in ('Repository', 'BenchmarkBranch', 'RunId')]}]
        self.cost_pages = cost_pages or []
        self.calls = []
        self.pages_read = 0

    def can_paginate(self, method):
        return True

    def get_paginator(self, method):
        return Pages(self, method)


def group(run_id, amount, unit='USD'):
    return {'Keys': ['RunId$' + run_id], 'Metrics': {'UnblendedCost': {'Amount': amount, 'Unit': unit}}}


def daily(day, groups=(), estimated=True):
    return {'TimePeriod': {'Start': day.date().isoformat(), 'End': (day + timedelta(days=1)).date().isoformat()},
            'Estimated': estimated, 'Groups': list(groups)}


class BudgetTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.budget_path = Path(self.temp.name) / 'budget.json'
        self.budget_path.write_text('{"weekly_budget_usd":"60"}')
        self.s3 = S3()
        self.ledger = self.make()
        self.ledger.initialize(now=NOW - timedelta(days=3))

    def make(self, **kwargs):
        return budget.BudgetLedger(self.s3, 'results-bucket', ACCOUNT, clock=lambda: NOW,
                                   budget_path=self.budget_path, sleeper=lambda seconds: None, **kwargs)

    def reserve(self, run_id=RUN_A, amount='20', now=NOW):
        return self.ledger.reserve(run_id, amount, PROVENANCE, now=now)

    def ce(self, groups=(), estimated=True):
        return CE([{'ResultsByTime': [daily(NOW - timedelta(days=3)),
            daily(NOW - timedelta(days=2)), daily(NOW - timedelta(days=1), groups, estimated)]}])

    def test_missing_read_requires_explicit_bootstrap_and_does_not_spend(self):
        self.s3.raw = None
        before = len(self.s3.puts)
        with self.assertRaises(budget.LedgerUnavailable):
            self.reserve()
        with self.assertRaises(budget.LedgerUnavailable):
            self.ledger.report()
        self.assertEqual(len(self.s3.puts), before)
        self.ledger.initialize()
        self.reserve()

    def test_initialize_preserves_existing_reservations_after_budget_file_change(self):
        original = self.reserve()
        before = len(self.s3.puts)
        self.budget_path.write_text('{"weekly_budget_usd":"1000"}')
        report = self.make().initialize()
        self.assertEqual(report['weekly_budget_usd'], '1000')
        self.assertEqual(report['conservative_budget_charge_usd'], '20')
        self.assertEqual(report['runs'][RUN_A]['budget_policy'], original['budget_policy'])
        self.assertEqual(len(self.s3.puts), before)

    def test_s3_requests_are_owned_encrypted_and_conditional(self):
        self.reserve()
        for request in self.s3.reads + self.s3.puts:
            self.assertEqual(request['ExpectedBucketOwner'], ACCOUNT)
        self.assertEqual(self.s3.puts[0]['IfNoneMatch'], '*')
        self.assertIn('IfMatch', self.s3.puts[-1])
        self.assertEqual(self.s3.puts[-1]['ServerSideEncryption'], 'AES256')
        self.assertTrue(all(body.closed for body in self.s3.bodies))
        self.assertTrue(all(sizes == [self.ledger.max_bytes + 1] for sizes in
                            (body.read_sizes for body in self.s3.bodies)))

    def test_kms_is_explicit_and_cas_still_used(self):
        self.make(kms_key_id='arn:aws:kms:region:account:key/example').reserve(RUN_A, '5', PROVENANCE)
        self.assertEqual(self.s3.puts[-1]['ServerSideEncryption'], 'aws:kms')
        self.assertIn('SSEKMSKeyId', self.s3.puts[-1])
        self.assertIn('IfMatch', self.s3.puts[-1])

    def test_duplicate_numeric_admission_is_idempotent_and_provenance_bound(self):
        first = self.reserve(amount='20.000')
        revision = self.ledger.report()['revision']
        second = self.ledger.reserve(RUN_A, Decimal('20.0'), copy.deepcopy(PROVENANCE))
        self.assertEqual(first, second)
        self.assertEqual(self.ledger.report()['revision'], revision)
        with self.assertRaises(budget.IdentityConflict):
            self.ledger.reserve(RUN_A, '21', PROVENANCE)
        with self.assertRaises(budget.IdentityConflict):
            self.ledger.reserve(RUN_A, '20', PROVENANCE | {'ttl_seconds': 1800})

    def test_concurrent_distinct_admission_cannot_overspend(self):
        self.s3.barrier = threading.Barrier(2)
        def attempt(run_id):
            try:
                self.make().reserve(run_id, '40', PROVENANCE)
                return 'admitted'
            except budget.BudgetExceeded:
                return 'denied'
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(attempt, (RUN_A, RUN_B)))
        self.s3.barrier = None
        self.assertCountEqual(outcomes, ['admitted', 'denied'])
        report = self.ledger.report()
        self.assertEqual(report['conservative_budget_charge_usd'], '40')
        self.assertEqual(len(report['runs']), 1)

    def test_concurrent_duplicate_admission_spends_once(self):
        self.s3.barrier = threading.Barrier(2)
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(lambda ignored: self.make().reserve(RUN_A, '40', PROVENANCE), range(2)))
        self.s3.barrier = None
        self.assertEqual(outcomes[0], outcomes[1])
        self.assertEqual(self.ledger.report()['conservative_budget_charge_usd'], '40')

    def test_concurrent_distinct_admission_both_fit(self):
        self.s3.barrier = threading.Barrier(2)
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda run: self.make().reserve(run, '30', PROVENANCE), (RUN_A, RUN_B)))
        self.s3.barrier = None
        self.assertEqual(self.ledger.report()['available_usd'], '0')
        with self.assertRaises(budget.BudgetExceeded):
            self.reserve(RUN_C, '0.000000000001')

    def test_reservation_has_no_separate_per_run_dollar_cap(self):
        self.reserve(amount='60')
        self.assertEqual(self.ledger.report()['available_usd'], '0')

    def test_unknown_committed_write_is_observed_before_admission(self):
        self.s3.write_mode = 'unknown_committed'
        result = self.reserve()
        self.assertEqual(result['reserved_usd'], '20')
        self.assertEqual(self.ledger.report()['conservative_budget_charge_usd'], '20')
        self.reserve()
        self.assertEqual(self.ledger.report()['revision'], 1)

    def test_unknown_uncommitted_write_stops_without_retrying_spend(self):
        self.s3.write_mode = 'unknown_uncommitted'
        before = len(self.s3.puts)
        with self.assertRaises(budget.UnknownWrite):
            self.reserve()
        self.assertEqual(len(self.s3.puts), before + 1)
        self.assertEqual(self.ledger.report()['runs'], {})
        self.reserve()
        self.assertEqual(self.ledger.report()['conservative_budget_charge_usd'], '20')

    def test_unknown_committed_write_with_unavailable_read_fails_closed(self):
        self.s3.write_mode = 'unknown_unreadable'
        with self.assertRaises(budget.UnknownWrite):
            self.reserve()
        with self.assertRaises(budget.LedgerUnavailable):
            self.reserve(RUN_B)
        self.s3.read_error = None
        self.assertEqual(self.ledger.report()['conservative_budget_charge_usd'], '20')

    def test_unknown_initialization_outcome_never_blindly_overwrites(self):
        self.s3.raw = None
        self.s3.write_mode = 'unknown_committed'
        self.ledger.initialize()
        self.reserve()
        self.assertEqual(self.ledger.report()['conservative_budget_charge_usd'], '20')

    def test_conflict_retries_are_bounded(self):
        self.s3.conflicts = 10
        before = len(self.s3.puts)
        with self.assertRaises(budget.LedgerUnavailable):
            self.make(max_attempts=3).reserve(RUN_A, '10', PROVENANCE)
        self.assertEqual(len(self.s3.puts), before + 3)
        self.assertEqual(self.ledger.report()['runs'], {})

    def test_malformed_unreachable_and_foreign_ledgers_never_reset(self):
        original = self.s3.raw
        for malformed in (b'{broken', b'{"schema":1,"schema":1}',
                          json.dumps(json.loads(original) | {'account': '999999999999'}).encode()):
            with self.subTest(malformed=malformed):
                self.s3.replace(malformed)
                before = len(self.s3.puts)
                with self.assertRaises(budget.LedgerMalformed):
                    self.reserve()
                with self.assertRaises(budget.LedgerMalformed):
                    self.ledger.initialize()
                self.assertEqual(len(self.s3.puts), before)
        self.s3.replace(original)
        self.s3.read_error = OSError('network unavailable')
        with self.assertRaises(budget.LedgerUnavailable):
            self.ledger.initialize()

    def test_oversized_read_is_closed_and_never_overwritten(self):
        self.s3.replace(b' ' * 2049)
        before = len(self.s3.puts)
        with self.assertRaises(budget.LedgerMalformed):
            self.make(max_bytes=1024).reserve(RUN_A, '1', PROVENANCE)
        self.assertTrue(self.s3.bodies[-1].closed)
        self.assertEqual(len(self.s3.puts), before)

    def test_tampered_reservation_binding_fails_closed(self):
        self.reserve()
        document = json.loads(self.s3.raw)
        document['runs'][RUN_A]['reserved_usd'] = '1'
        self.s3.replace(document)
        with self.assertRaises(budget.LedgerMalformed):
            self.ledger.report()

    def test_old_unresolved_reservations_never_age_out(self):
        self.reserve(now=NOW - timedelta(days=2))
        report = self.ledger.report(now=NOW + timedelta(days=100))
        self.assertEqual(report['conservative_budget_charge_usd'], '20')
        self.assertEqual(report['unresolved_runs'], 1)
        with self.assertRaises(budget.BudgetExceeded):
            self.reserve(RUN_B, '41', now=NOW + timedelta(days=100))

    def test_incomplete_cleanup_remains_charged_then_can_be_verified(self):
        self.reserve(now=NOW - timedelta(days=2))
        self.ledger.settle(RUN_A, '5', receipt(False), now=NOW)
        self.assertEqual(self.ledger.report(now=NOW + timedelta(days=30))['conservative_budget_charge_usd'], '20')
        self.ledger.settle(RUN_A, '5', receipt(True), now=NOW + timedelta(days=30))
        self.assertEqual(self.ledger.report(now=NOW + timedelta(days=36))['conservative_budget_charge_usd'], '5')
        self.assertEqual(self.ledger.report(now=NOW + timedelta(days=38))['conservative_budget_charge_usd'], '0')

    def test_clean_settlement_is_evidence_bound_idempotent_and_not_final_bill(self):
        self.reserve()
        settled = self.ledger.settle(RUN_A, '5', receipt())
        self.assertEqual(self.ledger.settle(RUN_A, '5.0', receipt()), settled)
        report = self.ledger.report(run_id=RUN_A)
        self.assertEqual(report['conservative_budget_charge_usd'], '5')
        self.assertEqual(report['usage_estimated_usd'], '5')
        self.assertFalse(report['runs'][RUN_A]['aws_billing_final'])
        self.assertIsNone(report['branch_billing']['lifetime_total_usd'])
        with self.assertRaises(budget.IdentityConflict):
            self.ledger.settle(RUN_A, '6', receipt())

    def test_settlement_requires_cleanup_and_usage_evidence(self):
        self.reserve()
        for bad in ({}, {'cleanup_complete': 'yes'}, {'cleanup_complete': True},
                    {'cleanup_complete': True, 'cleanup_evidence': {'ids': []}, 'usage_evidence': {}}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                self.ledger.settle(RUN_A, '0', bad)
        with self.assertRaises(budget.IdentityConflict):
            self.ledger.settle(RUN_B, '0', receipt())

    def test_no_launch_completed_legacy_receipt_releases_hold_without_rewriting_history(self):
        original_reservation = self.reserve(amount='41.266072308739')
        legacy_receipt = receipt()
        legacy_receipt['cleanup_evidence'] = {'regions_checked': [
            'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2'], 'remaining_instances': []}
        legacy_receipt['usage_evidence'] = {'elapsed_seconds': '0',
            'assumed_requests': 100000, 'assumed_input_transfers': 7, 'assumed_storage_days': 30}
        settled = self.ledger.settle(RUN_A, '0.665648825373', legacy_receipt)
        durable = self.s3.raw
        report = self.ledger.report()
        self.assertEqual(self.s3.raw, durable)
        run = report['runs'][RUN_A]
        self.assertEqual(run['reserved_usd'], original_reservation['reserved_usd'])
        self.assertEqual(run['reservation_digest'], original_reservation['reservation_digest'])
        self.assertEqual(run['settlement_digest'], settled['settlement_digest'])
        self.assertEqual(run['receipt'], legacy_receipt)
        self.assertTrue(run['reservation_released'])
        self.assertEqual(run['active_reservation_usd'], '0')
        self.assertEqual(report['admission_reservations_usd'], '0')
        self.assertEqual(report['conservative_budget_charge_usd'], '0.665648825373')
        self.assertEqual(report['legacy_usage_estimated_usd'], '0.665648825373')
        self.assertIsNone(report['observed_usage_estimated_usd'])
        self.assertIsNone(report['branch_billing']['lifetime_total_usd'])
        self.assertEqual(report['costsSummary']['total']['admission_reserved_usd'], '41.266072308739')
        self.reserve(RUN_B, '41.266072308739')

    def test_measured_no_launch_correction_preserves_original_and_excludes_forecasts(self):
        reservation = self.reserve(amount='41.266072308739')
        original = self.ledger.settle(RUN_A, '0.665648825373', receipt())
        evidence = measured_receipt(projection='12')
        corrected = self.ledger.reconcile_usage(RUN_A, '0.0001', evidence,
            original['settlement_digest'], now=NOW + timedelta(days=1))
        self.assertEqual(corrected['reserved_usd'], reservation['reserved_usd'])
        self.assertEqual(corrected['reservation_digest'], reservation['reservation_digest'])
        self.assertEqual(corrected['settled_at'], original['settled_at'])
        self.assertEqual(corrected['settlement_digest'], budget.settlement_digest(RUN_A, '0.00010', evidence))
        history = corrected['usage_reconciliation_history']
        self.assertEqual(len(history), 1)
        self.assertEqual({name: history[0][name] for name in
            ('usage_estimated_usd', 'receipt', 'settled_at', 'settlement_digest')},
            {name: original[name] for name in
            ('usage_estimated_usd', 'receipt', 'settled_at', 'settlement_digest')})
        self.assertEqual(history[0]['replacement_settlement_digest'], corrected['settlement_digest'])
        report = self.ledger.report(now=NOW + timedelta(days=1))
        self.assertEqual(report['conservative_budget_charge_usd'], '0.0001')
        self.assertEqual(report['admission_reservations_usd'], '0')
        self.assertEqual(report['observed_usage_estimated_usd'], '0.0001')
        self.assertIsNone(report['legacy_usage_estimated_usd'])
        self.assertEqual(report['projected_usd'], '12')
        self.assertFalse(report['usage_coverage_complete'])
        self.assertIsNone(report['branch_billing']['lifetime_total_usd'])
        self.assertIsNone(report['runs'][RUN_A]['receipt']['usage_evidence']['billing']['amount_usd'])
        total = self.generated()['costsSummary']['total']
        self.assertEqual(total['usage_estimated_usd'], '0.0001')
        self.assertEqual(total['observed_usage_estimated_usd'], '0.0001')
        self.assertEqual(total['projected_usd'], '12')
        self.assertEqual(total['partial_usage_run_ids'], [RUN_A])
        self.assertEqual(total['active_reservation_usd'], '0')
        with self.assertRaises(budget.IdentityConflict):
            self.ledger.settle(RUN_A, '0.0002', measured_receipt())

    def test_usage_reconciliation_requires_completed_measured_bound_identity_and_prior_digest(self):
        self.reserve()
        with self.assertRaises(budget.IdentityConflict):
            self.ledger.reconcile_usage(RUN_A, '0', measured_receipt(), 'f' * 64)
        incomplete = self.ledger.settle(RUN_A, '1', receipt(False))
        with self.assertRaises(budget.IdentityConflict):
            self.ledger.reconcile_usage(RUN_A, '0', measured_receipt(), incomplete['settlement_digest'])
        original = self.ledger.settle(RUN_A, '1', receipt())
        before = self.s3.raw
        for evidence in (receipt(), measured_receipt(clean=False)):
            with self.subTest(evidence=evidence), self.assertRaises(ValueError):
                self.ledger.reconcile_usage(RUN_A, '0', evidence, original['settlement_digest'])
        for name, foreign in (('account', '999999999999'), ('run_id', RUN_B),
                              ('repository', 'foreign/repo'), ('branch', 'foreign')):
            evidence = measured_receipt()
            evidence[name] = foreign
            with self.subTest(name=name), self.assertRaises(budget.IdentityConflict):
                self.ledger.reconcile_usage(RUN_A, '0', evidence, original['settlement_digest'])
        with self.assertRaises(budget.IdentityConflict):
            self.ledger.reconcile_usage(RUN_A, '0', measured_receipt(), 'f' * 64)
        with self.assertRaises(ValueError):
            self.ledger.reconcile_usage(RUN_A, '0', measured_receipt(), 'not-a-digest')
        with self.assertRaises(budget.IdentityConflict):
            self.ledger.reconcile_usage(RUN_B, '0', measured_receipt(RUN_B), original['settlement_digest'])
        self.assertEqual(self.s3.raw, before)

    def test_measured_incomplete_cleanup_keeps_hold_and_unknown_projection(self):
        self.reserve(amount='41')
        self.ledger.settle(RUN_A, '0.0001', measured_receipt(clean=False, projection=None))
        report = self.ledger.report(now=NOW + timedelta(days=100))
        self.assertEqual(report['admission_reservations_usd'], '41')
        self.assertEqual(report['conservative_budget_charge_usd'], '41')
        self.assertIsNone(report['projected_usd'])
        self.assertEqual(report['costsSummary']['total']['unknown_projection_run_ids'], [RUN_A])
        self.assertFalse(report['costsSummary']['total']['projection_coverage_complete'])
        self.assertEqual(report['unresolved_runs'], 1)
        with self.assertRaises(budget.BudgetExceeded):
            self.reserve(RUN_B, '20', now=NOW + timedelta(days=100))

    def test_competing_usage_reconciliations_compare_and_swap_previous_digest(self):
        self.reserve()
        original = self.ledger.settle(RUN_A, '5', receipt())
        self.s3.barrier = threading.Barrier(2)
        def attempt(amount):
            try:
                return self.make().reconcile_usage(RUN_A, amount, measured_receipt(),
                    original['settlement_digest'])['usage_estimated_usd']
            except budget.IdentityConflict:
                return 'conflict'
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(attempt, ('1', '2')))
        self.s3.barrier = None
        self.assertEqual(outcomes.count('conflict'), 1)
        run = self.ledger.report()['runs'][RUN_A]
        self.assertIn(run['usage_estimated_usd'], ('1', '2'))
        self.assertEqual(len(run['usage_reconciliation_history']), 1)
        self.assertEqual(run['usage_reconciliation_history'][0]['settlement_digest'], original['settlement_digest'])

    def test_usage_reconciliation_retries_are_idempotent_after_later_corrections(self):
        self.reserve()
        original = self.ledger.settle(RUN_A, '5', receipt())
        self.s3.write_mode = 'unknown_committed'
        first = self.ledger.reconcile_usage(RUN_A, '1', measured_receipt(), original['settlement_digest'])
        revision = self.ledger.report()['revision']
        repeated = self.ledger.reconcile_usage(RUN_A, '1.00', measured_receipt(), original['settlement_digest'])
        self.assertEqual(repeated, first)
        self.assertEqual(self.ledger.report()['revision'], revision)
        second_evidence = measured_receipt(projection='0.02')
        second = self.ledger.reconcile_usage(RUN_A, '2', second_evidence, first['settlement_digest'])
        revision = self.ledger.report()['revision']
        self.assertEqual(self.ledger.reconcile_usage(RUN_A, '1', measured_receipt(),
            original['settlement_digest']), second)
        self.assertEqual(self.ledger.report()['revision'], revision)
        self.assertEqual(len(second['usage_reconciliation_history']), 2)
        with self.assertRaises(budget.IdentityConflict):
            self.ledger.reconcile_usage(RUN_A, '3', measured_receipt(), first['settlement_digest'])

    def test_usage_reconciliation_history_is_bounded_without_discarding_evidence(self):
        self.reserve()
        run = self.ledger.settle(RUN_A, '50', receipt())
        for index in range(budget.MAX_USAGE_RECONCILIATIONS):
            run = self.ledger.reconcile_usage(RUN_A, str(index), measured_receipt(), run['settlement_digest'])
        before = self.s3.raw
        with self.assertRaises(budget.BudgetError):
            self.ledger.reconcile_usage(RUN_A, '40', measured_receipt(), run['settlement_digest'])
        self.assertEqual(self.s3.raw, before)
        self.assertEqual(run['usage_reconciliation_history'][0]['usage_estimated_usd'], '50')
        self.assertEqual(len(run['usage_reconciliation_history']), budget.MAX_USAGE_RECONCILIATIONS)

    def test_tampered_usage_reconciliation_chain_fails_closed(self):
        self.reserve()
        original = self.ledger.settle(RUN_A, '5', receipt())
        self.ledger.reconcile_usage(RUN_A, '1', measured_receipt(), original['settlement_digest'])
        document = json.loads(self.s3.raw)
        document['runs'][RUN_A]['usage_reconciliation_history'][0]['receipt']['usage_evidence']['elapsed_seconds'] = '1'
        self.s3.replace(document)
        with self.assertRaises(budget.LedgerMalformed):
            self.ledger.report()
        with self.assertRaises(budget.LedgerMalformed):
            self.reserve(RUN_B)

    def test_usage_correction_does_not_restart_rolling_window(self):
        self.reserve()
        original = self.ledger.settle(RUN_A, '5', receipt())
        corrected_at = NOW + timedelta(days=6)
        corrected = self.ledger.reconcile_usage(RUN_A, '2', measured_receipt(),
            original['settlement_digest'], now=corrected_at)
        self.assertEqual(corrected['settled_at'], original['settled_at'])
        at_boundary = self.ledger.report(now=NOW + timedelta(days=7))
        self.assertEqual(at_boundary['conservative_budget_charge_usd'], '2')
        after_boundary = self.ledger.report(now=NOW + timedelta(days=7, microseconds=1))
        self.assertEqual(after_boundary['conservative_budget_charge_usd'], '0')
        self.assertEqual(after_boundary['usage_estimated_usd'], '2')
        self.assertEqual(after_boundary['admission_reservations_usd'], '0')

    def test_usage_correction_retains_calendar_attribution_across_months(self):
        self.s3 = S3()
        self.ledger = self.make()
        reserved = datetime(2026, 8, 31, 23, 59, tzinfo=timezone.utc)
        settled = datetime(2026, 9, 1, 0, 1, tzinfo=timezone.utc)
        corrected_at = datetime(2026, 10, 1, 0, 1, tzinfo=timezone.utc)
        self.ledger.initialize(now=reserved)
        self.reserve(now=reserved)
        original = self.ledger.settle(RUN_A, '5', receipt(), now=settled)
        self.ledger.reconcile_usage(RUN_A, '2', measured_receipt(), original['settlement_digest'],
            now=corrected_at)
        report = self.ledger.report(now=corrected_at)
        summary = report['costsSummary']
        months = {item['start']: item for item in summary['monthly']}
        weeks = {item['start']: item for item in summary['weekly']}
        days = {item['start']: item for item in summary['daily']}
        self.assertEqual(report['conservative_budget_charge_usd'], '0')
        self.assertEqual(months['2026-08-01']['admission_reserved_usd'], '20')
        self.assertEqual(months['2026-08-01']['active_reservation_usd'], '0')
        self.assertEqual(months['2026-09-01']['observed_usage_estimated_usd'], '2')
        self.assertIsNone(months['2026-09-01']['legacy_usage_estimated_usd'])
        self.assertIsNone(months['2026-10-01']['usage_estimated_usd'])
        self.assertEqual(weeks['2026-08-31']['usage_estimated_usd'], '2')
        self.assertEqual(days['2026-09-01']['observed_usage_run_ids'], [RUN_A])
        self.assertEqual(summary['total']['usage_estimated_usd'], '2')
        self.assertEqual(summary['total']['admission_reserved_usd'], '20')
        self.assertEqual(summary['total']['active_reservation_run_ids'], [])

    def test_usage_reconciliation_unknown_uncommitted_write_leaves_prior_settlement(self):
        self.reserve()
        original = self.ledger.settle(RUN_A, '5', receipt())
        before = self.s3.raw
        self.s3.write_mode = 'unknown_uncommitted'
        with self.assertRaises(budget.UnknownWrite):
            self.ledger.reconcile_usage(RUN_A, '1', measured_receipt(), original['settlement_digest'])
        self.assertEqual(self.s3.raw, before)
        self.assertEqual(self.ledger.report()['runs'][RUN_A]['settlement_digest'], original['settlement_digest'])

    def test_history_capacity_is_bounded_without_discarding_costs(self):
        one = self.make(max_runs=1)
        one.reserve(RUN_A, '1', PROVENANCE)
        with self.assertRaises(budget.BudgetError):
            one.reserve(RUN_B, '1', PROVENANCE)
        self.assertIn(RUN_A, one.report()['runs'])

    def test_budget_file_is_the_only_limit_and_does_not_reset_cumulative_costs(self):
        original = self.reserve(amount='50')
        before = self.ledger.report()
        with self.assertRaises(budget.BudgetExceeded):
            self.reserve(RUN_B, '11')
        self.budget_path.write_text('{"weekly_budget_usd":"100"}')
        self.reserve(RUN_B, '50')
        report = self.ledger.report()
        self.assertEqual(report['weekly_budget_usd'], '100')
        self.assertEqual(report['available_usd'], '0')
        self.assertEqual(report['costsSummary']['total']['admission_reserved_usd'], '100')
        self.assertNotEqual(report['budget_policy']['sha256'], before['budget_policy']['sha256'])
        self.assertEqual(self.reserve(amount='50')['budget_policy'], original['budget_policy'])
        self.assertEqual(report['runs'][RUN_A]['budget_policy']['weekly_budget_usd'], '60')

    def test_cost_explorer_aggregation_is_disjoint_not_double_counted(self):
        self.reserve(amount='20')
        self.ledger.settle(RUN_A, '10', receipt())
        ce = self.ce([group(RUN_A, '25.125'), group('shared-infrastructure', '1.25'),
                      group('', '2.5'), group(RUN_B, '3.75')])
        report = self.ledger.reconcile(ce)
        billing = report['branch_billing']
        self.assertEqual(billing['lifetime_total_usd'], '32.625')
        self.assertEqual(billing['recent_total_usd'], '32.625')
        self.assertEqual(billing['lifetime_by_scope_usd'],
                         {'runs': '25.125', 'shared_infrastructure': '1.25',
                          'unattributed': '2.5', 'untracked_runs': '3.75'})
        self.assertEqual(report['conservative_budget_charge_usd'], '32.625')
        self.assertEqual(report['usage_estimated_usd'], '10')
        self.assertEqual(report['admission_reservations_usd'], '0')
        self.assertFalse(billing['final'])
        with self.assertRaises(budget.BudgetExceeded):
            self.reserve(RUN_C, '27.375000000001')
        params = ce.calls[-1][1]
        self.assertEqual(params['TimePeriod']['End'], NOW.date().isoformat())
        self.assertEqual(params['GroupBy'], [{'Type': 'TAG', 'Key': 'RunId'}])
        self.assertIn({'Dimensions': {'Key': 'LINKED_ACCOUNT', 'Values': [ACCOUNT]}}, params['Filter']['And'])

    def test_old_usage_ages_out_but_late_old_bill_increases_charge_once(self):
        self.s3 = S3()
        self.ledger = self.make()
        self.ledger.initialize(now=NOW - timedelta(days=10))
        self.reserve(now=NOW - timedelta(days=9))
        self.ledger.settle(RUN_A, '7', receipt(), now=NOW - timedelta(days=8))
        days = [daily(NOW - timedelta(days=offset)) for offset in range(10, 0, -1)]
        days[0]['Groups'] = [group(RUN_A, '10'), group('shared', '2')]
        days[-1]['Groups'] = [group(RUN_A, '3'), group('shared', '1')]
        report = self.ledger.reconcile(CE([{'ResultsByTime': days}]))
        self.assertEqual(report['branch_billing']['lifetime_total_usd'], '16')
        self.assertEqual(report['branch_billing']['recent_total_usd'], '4')
        self.assertEqual(report['usage_estimated_usd'], '7')
        self.assertEqual(report['usage_estimated_recent_usd'], '0')
        self.assertEqual(report['conservative_budget_charge_usd'], '7')
        self.assertEqual(report['branch_billing']['late_usage_adjustments_recent_usd'], '3')
        self.assertEqual(report['admission_reservations_usd'], '0')

    def test_late_higher_bills_override_corrected_estimate_without_double_counting(self):
        self.reserve(amount='41.266072308739')
        original = self.ledger.settle(RUN_A, '0.665648825373', receipt())
        corrected = self.ledger.reconcile_usage(RUN_A, '0.0001', measured_receipt(),
            original['settlement_digest'])
        first = self.ledger.reconcile(self.ce([group(RUN_A, '2')], estimated=False))
        self.assertEqual(first['conservative_budget_charge_usd'], '2')
        self.assertEqual(first['admission_reservations_usd'], '0')
        self.ledger.reconcile_usage(RUN_A, '0.0002', measured_receipt(),
            corrected['settlement_digest'])
        self.assertEqual(self.ledger.report()['conservative_budget_charge_usd'], '2')
        later = self.ledger.reconcile(self.ce([group(RUN_A, '5')], estimated=False))
        self.assertEqual(later['conservative_budget_charge_usd'], '5')
        self.assertEqual(later['observed_usage_estimated_usd'], '0.0002')
        self.assertEqual(later['branch_billing']['lifetime_total_usd'], '5')
        self.assertFalse(later['branch_billing']['final'])
        self.assertEqual(later['runs'][RUN_A]['reserved_usd'], '41.266072308739')
        self.assertEqual(later['costsSummary']['total']['aws_billed_to_date_usd'], '5')
        self.assertEqual(later['costsSummary']['total']['usage_estimated_usd'], '0.0002')
        with self.assertRaises(budget.BudgetExceeded):
            self.reserve(RUN_B, '55.000000000001')

    def test_late_old_usage_bill_adjustments_are_idempotent_bounded_and_age_out(self):
        self.s3 = S3()
        self.ledger = self.make()
        self.ledger.initialize(now=NOW - timedelta(days=10))
        self.reserve(now=NOW - timedelta(days=9))
        self.ledger.settle(RUN_A, '2', measured_receipt(), now=NOW - timedelta(days=8))
        def old_bill(amount):
            days = [daily(NOW - timedelta(days=offset)) for offset in range(10, 0, -1)]
            days[1]['Groups'] = [group(RUN_A, amount)]
            return CE([{'ResultsByTime': days}])
        first = self.ledger.reconcile(old_bill('5'))
        self.assertEqual(first['conservative_budget_charge_usd'], '3')
        self.assertEqual(first['branch_billing']['recent_total_usd'], '0')
        repeated = self.ledger.reconcile(old_bill('5'))
        self.assertEqual(first['revision'], repeated['revision'])
        self.assertEqual(repeated['conservative_budget_charge_usd'], '3')
        lowered = self.ledger.reconcile(old_bill('4'))
        self.assertEqual(lowered['conservative_budget_charge_usd'], '3')
        higher = self.ledger.reconcile(old_bill('7'))
        self.assertEqual(higher['conservative_budget_charge_usd'], '5')
        self.assertEqual(higher['branch_billing']['lifetime_total_usd'], '7')
        persisted = json.loads(self.s3.raw)['billing']
        self.assertEqual(persisted['run_billed_high_water_usd'][RUN_A], '7')
        self.assertEqual(len(persisted['late_usage_adjustments']), 2)
        unavailable = self.ledger.reconcile(CE([OSError('billing unavailable')]))
        self.assertEqual(unavailable['branch_billing']['status'], 'unavailable')
        self.assertEqual(unavailable['conservative_budget_charge_usd'], '5')
        aged = self.ledger.report(now=NOW + timedelta(days=7, microseconds=1))
        self.assertEqual(aged['conservative_budget_charge_usd'], '0')
        self.assertEqual(aged['branch_billing']['lifetime_total_usd'], '7')
        self.assertEqual(aged['usage_estimated_usd'], '2')

    def test_small_billing_never_releases_unresolved_reservation(self):
        self.reserve(amount='40')
        report = self.ledger.reconcile(self.ce([group(RUN_A, '0.01')], estimated=False))
        self.assertEqual(report['conservative_budget_charge_usd'], '40')
        self.assertEqual(report['branch_billing']['lifetime_total_usd'], '0.01')
        self.assertFalse(report['branch_billing']['final'])

    def test_zero_and_empty_billing_never_become_final(self):
        self.reserve()
        report = self.ledger.reconcile(self.ce(estimated=False))
        self.assertEqual(report['branch_billing']['lifetime_total_usd'], '0')
        self.assertFalse(report['branch_billing']['final'])
        self.assertEqual(report['conservative_budget_charge_usd'], '20')

    def test_cost_explorer_pagination_sums_distinct_groups_once(self):
        self.reserve()
        ce = self.ce()
        ce.tag_pages = [{'CostAllocationTags': ce.tag_pages[0]['CostAllocationTags'][:1]},
                        {'CostAllocationTags': ce.tag_pages[0]['CostAllocationTags'][1:]}]
        ce.cost_pages = [
            {'ResultsByTime': [daily(NOW - timedelta(days=3)), daily(NOW - timedelta(days=2)),
                               daily(NOW - timedelta(days=1), [group(RUN_A, '0.1')])]},
            {'ResultsByTime': [daily(NOW - timedelta(days=1), [group('shared', '0.2')])]}]
        report = self.ledger.reconcile(ce)
        self.assertEqual(report['branch_billing']['lifetime_total_usd'], '0.3')
        self.assertEqual(report['conservative_budget_charge_usd'], '20.2')
        self.assertEqual(ce.pages_read, 4)

    def test_partial_daily_billing_and_duplicate_groups_retain_last_snapshot(self):
        self.reserve()
        good = self.ledger.reconcile(self.ce([group(RUN_A, '30')]))
        bad_pages = [
            [{'ResultsByTime': [daily(NOW - timedelta(days=1), [group(RUN_A, '1')])]}],
            [{'ResultsByTime': [daily(NOW - timedelta(days=3)), daily(NOW - timedelta(days=2)),
                                daily(NOW - timedelta(days=1), [group(RUN_A, '1'), group(RUN_A, '1')])]}],
            [OSError('CE unavailable')]]
        for pages in bad_pages:
            with self.subTest(pages=pages):
                failed = self.ledger.reconcile(CE(pages))
                self.assertEqual(failed['branch_billing']['status'], 'unavailable')
                self.assertEqual(failed['branch_billing']['lifetime_total_usd'], good['branch_billing']['lifetime_total_usd'])
                self.assertEqual(failed['conservative_budget_charge_usd'], '30')
                self.assertIsNotNone(failed['branch_billing']['error'])

    def test_inactive_unsupported_tags_are_unknown_not_zero(self):
        self.reserve()
        ce = self.ce()
        ce.tag_pages[0]['CostAllocationTags'] = ce.tag_pages[0]['CostAllocationTags'][:2]
        report = self.ledger.reconcile(ce)
        self.assertEqual(report['branch_billing']['status'], 'unavailable')
        self.assertIsNone(report['branch_billing']['lifetime_total_usd'])
        self.assertEqual(report['conservative_budget_charge_usd'], '20')
        self.assertEqual(len(ce.calls), 1)
        ce.tag_pages = [ServiceError('UnknownOperationException')]
        report = self.ledger.reconcile(ce)
        self.assertIsNone(report['branch_billing']['lifetime_total_usd'])
        self.assertFalse(report['branch_billing']['final'])

    def test_recent_or_unknown_tag_activation_is_explicitly_partial(self):
        for missing in (False, True):
            ce = self.ce()
            for tag in ce.tag_pages[0]['CostAllocationTags']:
                if missing:
                    tag.pop('LastUpdatedDate')
                else:
                    tag['LastUpdatedDate'] = NOW.isoformat()
            report = self.ledger.reconcile(ce)
            self.assertEqual(report['branch_billing']['status'], 'partial')
            self.assertFalse(report['branch_billing']['tag_coverage_complete'])
            self.assertFalse(report['branch_billing']['final'])

    def test_partial_tag_coverage_preserves_known_charges_and_merges_higher_groups_once(self):
        self.reserve()
        before = self.ledger.reconcile(self.ce([
            group(RUN_A, '5'), group('shared-infrastructure', '10'),
            group('', '4'), group(RUN_B, '6')]))
        self.assertEqual(before['conservative_budget_charge_usd'], '40')
        self.assertEqual(before['available_usd'], '20')
        partial = self.ce([group(RUN_A, '0'), group('shared-infrastructure', '0'),
                           group('', '7'), group(RUN_C, '2')])
        partial.tag_pages[0]['CostAllocationTags'][0]['LastUpdatedDate'] = NOW.isoformat()
        after = self.ledger.reconcile(partial)
        self.assertEqual(after['branch_billing']['status'], 'partial')
        self.assertTrue(after['branch_billing']['conservative_merged_observations'])
        self.assertFalse(after['branch_billing']['final'])
        self.assertEqual(after['branch_billing']['lifetime_total_usd'], '30')
        self.assertEqual(after['branch_billing']['lifetime_by_scope_usd'],
                         {'runs': '5', 'shared_infrastructure': '10',
                          'unattributed': '7', 'untracked_runs': '8'})
        self.assertEqual(after['conservative_budget_charge_usd'], '45')
        self.assertEqual(after['available_usd'], '15')
        repeated = self.ledger.reconcile(partial)
        self.assertEqual(repeated['revision'], after['revision'])
        self.assertEqual(repeated['conservative_budget_charge_usd'], '45')
        with self.assertRaises(budget.BudgetExceeded):
            self.ledger.reserve('th-' + 'd' * 16, '15.000000000001', PROVENANCE)

    def test_incomplete_persisted_billing_days_fail_closed_for_every_snapshot_status(self):
        self.reserve()
        self.ledger.reconcile(self.ce([group('shared-infrastructure', '12')]))
        original = json.loads(self.s3.raw)
        charged_day = (NOW - timedelta(days=1)).date().isoformat()
        for status in ('available', 'partial', 'unavailable'):
            for remove_all in (False, True):
                with self.subTest(status=status, remove_all=remove_all):
                    document = copy.deepcopy(original)
                    document['billing']['status'] = status
                    if remove_all:
                        document['billing']['snapshot']['daily'] = {}
                    else:
                        del document['billing']['snapshot']['daily'][charged_day]
                    self.s3.replace(document)
                    before = len(self.s3.puts)
                    with self.assertRaises(budget.LedgerMalformed):
                        self.ledger.report()
                    with self.assertRaises(budget.LedgerMalformed):
                        self.reserve(RUN_B, '40')
                    with self.assertRaises(budget.LedgerMalformed):
                        self.ledger.initialize()
                    self.assertEqual(len(self.s3.puts), before)

    def test_billing_currency_and_history_horizon_fail_visibly(self):
        report = self.ledger.reconcile(self.ce([group(RUN_A, '1', unit='EUR')]))
        self.assertEqual(report['branch_billing']['status'], 'unavailable')
        constrained = self.make(max_billing_days=2)
        ce = self.ce()
        report = constrained.reconcile(ce)
        self.assertEqual(report['branch_billing']['status'], 'unavailable')
        self.assertEqual(ce.calls, [])

    def test_billing_runtime_date_is_verified_before_any_api(self):
        ce = self.ce()
        with self.assertRaises(ValueError):
            self.ledger.reconcile(ce, now=NOW - timedelta(days=365))
        self.assertEqual(ce.calls, [])
        self.ledger.reconcile(ce, now=NOW)
        document = json.loads(self.s3.raw)
        self.assertEqual(document['billing']['snapshot']['runtime_date_verified'], NOW.date().isoformat())

    def test_billing_page_count_is_bounded_and_no_partial_snapshot_committed(self):
        ce = self.ce()
        ce.cost_pages = ce.cost_pages * 5
        report = self.make(max_pages=1).reconcile(ce)
        self.assertEqual(report['branch_billing']['status'], 'unavailable')
        self.assertIsNone(report['branch_billing']['lifetime_total_usd'])
        self.assertLessEqual(ce.pages_read, 3)

    def test_report_distinguishes_usage_reservation_unknown_and_zero_bill(self):
        self.reserve()
        self.ledger.settle(RUN_A, '4', measured_receipt(projection='12'))
        report = self.ledger.report()
        self.assertEqual(report['admission_reservations_usd'], '0')
        self.assertEqual(report['observed_usage_estimated_usd'], '4')
        self.assertEqual(report['projected_usd'], '12')
        self.assertIsNone(report['branch_billing']['lifetime_total_usd'])
        self.assertFalse(report['branch_billing']['final'])
        text = budget.github_summary(report)
        self.assertIn('unavailable', text)
        self.assertIn('$12', text)
        billed = self.ledger.reconcile(self.ce())
        self.assertEqual(billed['branch_billing']['lifetime_total_usd'], '0')
        self.assertEqual(billed['conservative_budget_charge_usd'], '4')
        self.assertFalse(billed['branch_billing']['final'])
        self.assertIn('$0', budget.github_summary(billed))

    def generated(self):
        return json.loads(self.s3.report_objects[self.ledger.report_key]['raw'])

    def test_budget_file_validation_hash_and_missing_file_fail_closed(self):
        policy = budget.load_budget(self.budget_path)
        import hashlib
        self.assertEqual(policy['sha256'], hashlib.sha256(self.budget_path.read_bytes()).hexdigest())
        self.assertEqual(policy['path'], str(self.budget_path.resolve()))
        for raw in ('{"weekly_budget_usd":60}', '{"weekly_budget_usd":"NaN"}',
                    '{"weekly_budget_usd":"60","other_cap":"10"}',
                    '{"weekly_budget_usd":"60","weekly_budget_usd":"100"}', ' ' * 8193):
            with self.subTest(raw=raw):
                self.budget_path.write_text(raw)
                before = len(self.s3.puts)
                with self.assertRaises(budget.BudgetError):
                    self.reserve()
                self.assertEqual(len(self.s3.puts), before)
        self.budget_path.unlink()
        self.make()  # Construction itself must not interfere with safety cleanup.
        with self.assertRaises(budget.BudgetError):
            self.reserve()

    def test_config_only_edit_changes_admission_not_durable_accounting(self):
        self.reserve()
        self.ledger.settle(RUN_A, '3', receipt())
        before = self.ledger.publish_report()
        source_before, generation_before = self.s3.raw, self.s3.generation
        self.budget_path.write_text('{"weekly_budget_usd":"10"}')
        read_only = self.ledger.report()
        self.assertEqual(read_only['weekly_budget_usd'], '10')
        self.assertEqual(read_only['available_usd'], '7')
        after = self.ledger.publish_report()
        self.assertEqual(after['costsSummary'], before['costsSummary'])
        self.assertEqual(self.s3.raw, source_before)
        self.assertEqual(self.s3.generation, generation_before)
        self.assertNotEqual(after['budget_policy']['sha256'], before['budget_policy']['sha256'])
        self.assertEqual(after['generated_report_uri'], 's3://results-bucket/' + self.ledger.report_key)

    def test_generated_report_counts_duplicate_runs_once_and_corrects_usage_in_place(self):
        self.reserve()
        self.reserve()
        self.ledger.settle(RUN_A, '3', receipt(False))
        self.ledger.settle(RUN_A, '5', receipt(False))
        self.ledger.settle(RUN_A, '5', receipt(False))
        result = self.generated()
        total = result['costsSummary']['total']
        self.assertEqual(result['costsSummary']['unique_run_count'], 1)
        self.assertEqual(total['admission_reserved_usd'], '20')
        self.assertEqual(total['usage_estimated_usd'], '5')
        self.assertEqual(total['reservation_run_ids'], [RUN_A])
        self.assertEqual(total['usage_estimated_run_ids'], [RUN_A])
        self.assertIsNone(total['aws_billed_to_date_usd'])
        self.assertEqual(set(result['runs']), {RUN_A})

    def test_delayed_billing_corrections_replace_bills_without_adding_estimates(self):
        self.reserve()
        self.ledger.settle(RUN_A, '5', receipt())
        self.ledger.reconcile(self.ce([group(RUN_A, '7')]))
        initial = self.generated()['costsSummary']['total']
        self.assertEqual(initial['usage_estimated_usd'], '5')
        self.assertEqual(initial['aws_billed_to_date_usd'], '7')
        self.ledger.reconcile(self.ce([group(RUN_A, '6')]))
        corrected = self.generated()['costsSummary']['total']
        self.assertEqual(corrected['usage_estimated_usd'], '5')
        self.assertEqual(corrected['aws_billed_to_date_usd'], '6')
        self.assertFalse(corrected['aws_billing_final'])
        self.assertEqual(self.generated()['conservative_budget_charge_usd'], '6')

    def test_generated_periods_use_utc_month_and_monday_week_boundaries(self):
        self.s3 = S3()
        self.ledger = self.make()
        start = datetime(2026, 8, 31, 23, 59, tzinfo=timezone.utc)
        september = datetime(2026, 9, 1, 0, 1, tzinfo=timezone.utc)
        sunday = datetime(2026, 9, 6, 23, 59, tzinfo=timezone.utc)
        monday = datetime(2026, 9, 7, 0, 1, tzinfo=timezone.utc)
        self.ledger.initialize(now=start)
        self.reserve(now=start)
        self.ledger.settle(RUN_A, '3', receipt(), now=september)
        self.reserve(RUN_B, '10', now=sunday)
        self.ledger.settle(RUN_B, '4', receipt(), now=monday)
        summary = self.ledger.report(now=monday)['costsSummary']
        months = {item['start']: item for item in summary['monthly']}
        weeks = {item['start']: item for item in summary['weekly']}
        days = {item['start']: item for item in summary['daily']}
        self.assertEqual(months['2026-08-01']['end_exclusive'], '2026-09-01')
        self.assertIsNone(months['2026-08-01']['usage_estimated_usd'])
        self.assertEqual(months['2026-09-01']['usage_estimated_usd'], '7')
        self.assertEqual(weeks['2026-08-31']['end_exclusive'], '2026-09-07')
        self.assertEqual(weeks['2026-08-31']['usage_estimated_usd'], '3')
        self.assertEqual(weeks['2026-09-07']['usage_estimated_usd'], '4')
        self.assertEqual(days['2026-08-31']['admission_reserved_usd'], '20')
        self.assertEqual(days['2026-09-01']['usage_estimated_usd'], '3')
        self.assertEqual(days['2026-09-07']['usage_estimated_usd'], '4')
        self.assertEqual(summary['total']['usage_estimated_usd'], '7')
        self.assertEqual(summary['total']['admission_reserved_usd'], '30')
        self.assertIsNone(summary['total']['aws_billed_to_date_usd'])
        self.assertFalse(summary['total']['aws_coverage_complete'])

    def test_report_publication_failure_never_prevents_persisted_settlement(self):
        self.reserve()
        self.s3.report_error = OSError('derived report unavailable')
        result = self.ledger.settle(RUN_A, '5', receipt())
        self.assertEqual(result['usage_estimated_usd'], '5')
        persisted = json.loads(self.s3.raw)['runs'][RUN_A]
        self.assertTrue(persisted['receipt']['cleanup_complete'])
        self.assertEqual(self.ledger.last_publication['status'], 'failed')
        self.s3.report_error = None
        self.assertEqual(self.ledger.publish_report()['costsSummary']['total']['usage_estimated_usd'], '5')

    def test_administrator_can_initialize_and_reconcile_when_report_writes_denied(self):
        self.s3.report_error = ServiceError('AccessDenied')
        initialized = self.ledger.initialize()
        self.assertEqual(initialized['aggregate_publication']['status'], 'failed')
        self.reserve()
        reconciled = self.ledger.reconcile(self.ce([group(RUN_A, '5')]))
        self.assertEqual(reconciled['branch_billing']['status'], 'available')
        self.assertEqual(reconciled['branch_billing']['lifetime_total_usd'], '5')
        self.assertEqual(reconciled['aggregate_publication']['status'], 'failed')
        self.assertEqual(json.loads(self.s3.raw)['billing']['status'], 'available')
        with self.assertRaises(budget.BudgetError):
            self.ledger.publish_report()

    def test_missing_budget_file_does_not_block_settlement_receipt(self):
        self.reserve()
        self.budget_path.unlink()
        settled = self.ledger.settle(RUN_A, '5', receipt())
        self.assertEqual(settled['usage_estimated_usd'], '5')
        self.assertEqual(self.ledger.last_publication['status'], 'failed')
        self.assertEqual(json.loads(self.s3.raw)['runs'][RUN_A]['usage_estimated_usd'], '5')

    def test_read_only_report_does_not_publish_or_mutate_any_s3_object(self):
        self.reserve()
        before = (len(self.s3.puts), len(self.s3.report_puts))
        report = self.ledger.report()
        self.assertEqual((len(self.s3.puts), len(self.s3.report_puts)), before)
        self.assertIn('costsSummary', report)
        self.assertIn('generated_report_uri', report)

    def test_publish_command_exports_generated_read_only_json_and_one_summary(self):
        from contextlib import redirect_stdout
        import stat
        from types import SimpleNamespace
        from unittest.mock import patch
        self.reserve()
        self.ledger.settle(RUN_A, '5', receipt())
        target = Path(self.temp.name) / 'artifacts' / 'aggregate.json'
        session = SimpleNamespace(client=lambda name, **kwargs: self.s3)
        stdout = io.StringIO()
        with patch('boto3.Session', return_value=session), redirect_stdout(stdout):
            status = budget.main(['publish-report', '--bucket', 'results-bucket', '--account', ACCOUNT,
                '--region', 'us-east-1', '--budget-file', str(self.budget_path),
                '--output', str(target), '--format', 'github'])
        self.assertEqual(status, 0)
        local = json.loads(target.read_bytes())
        self.assertEqual(local, self.generated())
        self.assertEqual(local['costsSummary']['total']['usage_estimated_usd'], '5')
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o444)
        self.assertEqual(stdout.getvalue().count('## AWS benchmark budget'), 1)
        self.assertIn('Current UTC month', stdout.getvalue())

    def test_generated_totals_are_rebuilt_from_ledger_after_report_tampering(self):
        self.reserve()
        self.ledger.settle(RUN_A, '5', receipt())
        stored = self.s3.report_objects[self.ledger.report_key]
        damaged = json.loads(stored['raw'])
        damaged['costsSummary']['total']['usage_estimated_usd'] = '999'
        damaged['costsSummary']['unique_run_count'] = 999
        stored['raw'] = json.dumps(damaged).encode()
        stored['generation'] += 1
        restored = self.ledger.publish_report()
        self.assertEqual(restored['costsSummary']['total']['usage_estimated_usd'], '5')
        self.assertEqual(restored['costsSummary']['unique_run_count'], 1)
        self.assertTrue(self.ledger.last_publication['repaired'])
        self.assertEqual(self.generated(), restored)

    def test_unknown_report_write_is_confirmed_or_fails_without_financial_reset(self):
        self.reserve()
        self.s3.report_write_mode = 'unknown_committed'
        result = self.ledger.publish_report(now=NOW + timedelta(seconds=1))
        self.assertEqual(self.generated(), result)
        self.s3.report_write_mode = 'unknown_uncommitted'
        before = self.s3.raw
        with self.assertRaises(budget.UnknownWrite):
            self.ledger.publish_report(now=NOW + timedelta(seconds=2))
        self.assertEqual(self.s3.raw, before)

    def test_older_publisher_cannot_overwrite_concurrent_newer_ledger_report(self):
        self.reserve()
        paused, proceed = threading.Event(), threading.Event()
        def pause_old(kwargs):
            if threading.current_thread().name.startswith('old-publisher'):
                paused.set()
                if not proceed.wait(timeout=5):
                    raise AssertionError('new publisher did not finish')
        self.s3.report_before_put = pause_old
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix='old-publisher') as pool:
            older = pool.submit(self.ledger.publish_report, NOW + timedelta(seconds=1))
            self.assertTrue(paused.wait(timeout=5))
            try:
                self.reserve(RUN_B, '10', now=NOW + timedelta(seconds=2))
                newer_revision = json.loads(self.s3.raw)['revision']
            finally:
                proceed.set()
            result = older.result(timeout=5)
        self.s3.report_before_put = None
        self.assertEqual(result['ledger_revision'], newer_revision)
        self.assertEqual(self.generated()['ledger_revision'], newer_revision)
        self.assertEqual(self.generated()['costsSummary']['total']['admission_reserved_usd'], '30')
        self.assertEqual(self.generated()['costsSummary']['unique_run_count'], 2)

    def test_unmodeled_cost_explorer_calls_use_actual_sdk_token_pagination(self):
        from botocore.session import Session
        from types import SimpleNamespace
        model = Session().get_service_model('ce')
        base = self.ce([group('shared', '0.2')])

        class UnmodeledCE:
            meta = SimpleNamespace(service_model=model)

            def __init__(self):
                self.calls = []

            def can_paginate(self, method):
                return False

            def list_cost_allocation_tags(self, **kwargs):
                self.calls.append(('tags', kwargs))
                tags = base.tag_pages[0]['CostAllocationTags']
                if 'NextToken' not in kwargs:
                    return {'CostAllocationTags': tags[:1], 'NextToken': 'tags-second'}
                if kwargs['NextToken'] != 'tags-second':
                    raise AssertionError('incorrect cost allocation pagination token')
                return {'CostAllocationTags': tags[1:]}

            def get_cost_and_usage(self, **kwargs):
                self.calls.append(('costs', kwargs))
                if 'NextPageToken' not in kwargs:
                    return {'ResultsByTime': base.cost_pages[0]['ResultsByTime'],
                            'NextPageToken': 'costs-second'}
                if kwargs['NextPageToken'] != 'costs-second':
                    raise AssertionError('incorrect cost query pagination token')
                return {'ResultsByTime': [daily(NOW - timedelta(days=1), [group(RUN_A, '0.1')])]}

        ce = UnmodeledCE()
        report = self.ledger.reconcile(ce)
        self.assertEqual(report['branch_billing']['status'], 'available')
        self.assertEqual(report['branch_billing']['lifetime_total_usd'], '0.3')
        self.assertEqual(len(ce.calls), 4)


class DecimalCostTests(unittest.TestCase):
    def test_usage_math_preserves_decimal_precision_under_hostile_context(self):
        with localcontext() as context:
            context.prec = 3
            result = budget.usage_cost_usd('0.1', '3600', count=3)
        self.assertEqual(result, Decimal('0.300000000000'))

    def test_compute_minimum_partial_seconds_and_conservative_rounding(self):
        self.assertEqual(budget.usage_cost_usd('3.6', '0', count=6), Decimal('0.360000000000'))
        self.assertEqual(budget.usage_cost_usd('3.6', '60.01'), Decimal('0.061000000000'))
        self.assertEqual(budget.usage_cost_usd('0.1', '1', minimum_seconds=0), Decimal('0.000027777778'))

    def test_no_float_nan_negative_or_unbounded_costs(self):
        for value in (0.1, float('nan'), 'NaN', 'Infinity', '-1', True, '0.0000000000000000001'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                budget.usage_cost_usd(value, '60')
        for count in (True, 0, -1, 1.5):
            with self.subTest(count=count), self.assertRaises(ValueError):
                budget.usage_cost_usd('1', '60', count=count)


if __name__ == '__main__':
    unittest.main()
