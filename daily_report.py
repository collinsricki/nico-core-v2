"""Local daily projections of accepted evidence; never calls a bank or changes funds."""
import hashlib
import json
import logging
import re
import time
from datetime import timedelta

from budget_rules import SOURCE_HASH, calculate, canonical, contract
from finance_summary import MAX_REPORT, projection
from finance_worker import date, stamp, transaction, utcnow
from planning_report import finish

LOG = logging.getLogger(__name__)


def schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS budget_daily_selection (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),seed_report_id TEXT NOT NULL,
        selected_report_id TEXT,last_reconciliation_id TEXT,expires_at TEXT,
        status TEXT NOT NULL,next_attempt_at TEXT,failure_count INTEGER NOT NULL DEFAULT 0)''')
    db.execute('''CREATE TABLE IF NOT EXISTS budget_daily_jobs (
        seed_report_id TEXT NOT NULL,reconciliation_id TEXT NOT NULL,report_id TEXT NOT NULL,
        generated_at TEXT NOT NULL,PRIMARY KEY(seed_report_id,reconciliation_id))''')


def refreshed_report(db, document, evidence, report, run, now):
    """Advance supported observation fields, never an evidence/pagination timestamp."""
    spending = report['up_spending']
    base = db.execute('SELECT * FROM budget_cycle_runs WHERE run_id=?',
                      (evidence.get('calculation', {}).get('runId'),)).fetchone()
    if not base or base['status'] != 'COMPLETE':
        raise ValueError('Accepted cycle capture required')
    if not date(base['start_at']) <= now < date(base['end_at']):
        raise ValueError('Outside accepted cycle')
    bound = {role: item['accountId'] for role, item in evidence['bindings'].items()}
    snapshots = {row['account_id']: row for row in db.execute(
        'SELECT * FROM balance_snapshots WHERE run_id=?', (run['run_id'],))}
    for role, identity in bound.items():
        row = snapshots.get(identity)
        if (not row or row['currency_code'] != 'AUD' or date(row['observed_at']) > now
                or date(row['observed_at']) < date(base['through_at'])):
            raise ValueError('Bound observation unavailable or regressed')
        observation = {'account_id': identity, 'available_after_holds_minor': row['balance_base_units'],
            'currency': 'AUD', 'as_of': row['observed_at'], 'source': 'existing_render_balance_snapshot',
            'currentness': 'OBSERVATION_DATE_ONLY',
            'coverage': 'INCREMENTAL_MIRROR_NOT_FULL_CYCLE_PAGINATION'}
        if role == 'up.spending':
            spending['observation'] = observation
        else:
            report['protected_essentials_savers']['observations'][role] = observation
    if 'up.spending' not in bound:
        raise ValueError('Spending binding required')

    gaps = set(spending['arithmetic_blockers'])
    qualifiers = set(spending['qualifications'])
    # These runs record actual completed reconciliation, not just ordering. Even a
    # continuous chain is only mirror coverage, never new full-cycle pagination.
    cursor = date(base['through_at'])
    for item in db.execute('''SELECT * FROM reconciliation_runs
            WHERE status='OBSERVED' AND held_recheck_complete=1 AND error_code IS NULL
            AND catchup_until<=? ORDER BY catchup_until''', (run['catchup_until'],)):
        if date(item['catchup_since']) <= cursor < date(item['catchup_until']):
            cursor = date(item['catchup_until'])
    if cursor < date(run['catchup_until']):
        gaps.add('INCREMENTAL_COVERAGE_UNVERIFIED')
    if run['status'] != 'OBSERVED' or not run['held_recheck_complete']:
        gaps.add('INCREMENTAL_COVERAGE_UNVERIFIED')
    spending['cycle_coverage']['mirror_through_at'] = run['catchup_until']
    spending['cycle_coverage']['mirror_coverage_verified'] = cursor >= date(run['catchup_until'])
    qualifiers.add('INCREMENTAL_OBSERVATIONS_NOT_FULL_CYCLE_PAGINATION')

    # The old completeness declaration does not cover new activity, including
    # changes created after the reconciliation collection boundary via webhooks.
    for count, item in enumerate(db.execute('SELECT * FROM transactions WHERE created_at>=? AND created_at<? LIMIT 10001',
                           (base['start_at'], base['end_at']))):
        if count == 10000:
            raise ValueError('Bounded local observation limit')
        if item['account_id'] not in bound.values():
            continue
        member = db.execute('SELECT resource_json FROM budget_cycle_members '
                            'WHERE run_id=? AND transaction_id=?',
                            (base['run_id'], item['transaction_id'])).fetchone()
        if item['ordering_conflict'] or item['is_deleted'] or not member or member[0] != item['resource_json']:
            gaps.add('TRANSACTION_CHANGED_OR_UNRESOLVED:' + item['transaction_id'])
        if not member:
            if item['amount_base_units'] is None or item['currency_code'] != 'AUD':
                gaps.add('TRANSACTION_CHANGED_OR_UNRESOLVED:' + item['transaction_id'])
            elif item['amount_base_units'] < 0:
                gaps.add('UNCLASSIFIED_UP_DEBIT:' + item['transaction_id'])
            elif item['amount_base_units'] > 0:
                gaps.add('UNCLASSIFIED_UP_CREDIT_OR_REFUND:' + item['transaction_id'])

    # Keep historical paid actuals/targets, but do not promote manual all-bank row
    # completeness into a fresh assertion. Known-match planning remains qualified.
    for row in report['rows']:
        if row['coverage'] == 'COMPLETE_EXPLICIT_ALL_BANK_ROW_EVIDENCE':
            row['coverage'] = 'HISTORICAL_ROW_EVIDENCE_NOT_CURRENT_COMPLETE'
            row['actual_minor'] = row['remaining_minor'] = None
            qualifiers.add('INCOMPLETE_ALL_BANK_CATEGORY:' + row['budget_id'])
    report['actual_total_minor'] = report['remaining_total_minor'] = None

    # Recheck the same earmarks against fresh protected observations. No cash is
    # moved and no original target is added as a second deduction.
    funded = {}
    for row in report['rows']:
        amount = row.get('funding_check_amount_minor')
        if amount is not None:
            if type(amount) is not int or amount < 0:
                raise ValueError('Invalid accepted funding amount')
            role = row['payment_account']
            funded[role] = funded.get(role, 0) + amount
    gaps = {x for x in gaps if not x.startswith('PROTECTED_ACCOUNT_UNDERFUNDED:')}
    for role, amount in funded.items():
        if role == 'up.spending' or role not in bound or snapshots[bound[role]]['balance_base_units'] < amount:
            gaps.add('PROTECTED_ACCOUNT_UNDERFUNDED:' + role)
    spending['arithmetic_blockers'] = sorted(gaps)
    spending['qualifications'] = sorted(qualifiers)
    spending['conditional_gaps'] = sorted(gaps | qualifiers)
    spending['conditional_balance_as_of'] = spending['observation']['as_of']
    values = [x['amount_minor'] for x in spending['deductions']]
    if any(type(x) is not int or x < 0 for x in values):
        spending['arithmetic_blockers'].append('DEDUCTION_UNKNOWN')
    supported = not spending['arithmetic_blockers']
    spending['current_reserve_minor'] = sum(values) if supported else None
    spending['conditional_cash_room_minor'] = (
        spending['observation']['available_after_holds_minor'] - sum(values) if supported else None)
    spending['assured_safe_withdrawal_minor'] = None
    # All original dates remain untouched; the retained capture is the applicability
    # anchor for already accepted historical planning facts, not today's collection.
    finish(evidence['calculation'], document, report, base['through_at'],
           spending.get('prior_category_planning_allocations', []))
    report['daily_generation'] = {'reconciliation_id': run['run_id'],
        'reconciliation_through_at': run['catchup_until'],
        'historical_evidence_not_redated': True, 'cycle_expires_at': base['end_at']}
    return report, base['end_at']


class DailyReports:
    def __init__(self, factory, seed_provider, clock=utcnow):
        self.factory, self.seed_provider, self.clock = factory, seed_provider, clock

    def run_once(self):
        seed = self.seed_provider()
        if not isinstance(seed, str) or not re.fullmatch('[0-9a-f]{64}', seed):
            return  # Missing explicit owner-selected seed never falls back to latest.
        now = self.clock()
        with transaction(self.factory) as db:
            schema(db)
            db.execute('''INSERT INTO budget_daily_selection(singleton,seed_report_id,status)
                VALUES(1,?,'PENDING') ON CONFLICT(singleton) DO NOTHING''', (seed,))
            state = db.execute('SELECT * FROM budget_daily_selection WHERE singleton=1').fetchone()
            if state['seed_report_id'] != seed:
                db.execute('''UPDATE budget_daily_selection SET seed_report_id=?,selected_report_id=NULL,
                    last_reconciliation_id=NULL,expires_at=NULL,status='PENDING',next_attempt_at=NULL,
                    failure_count=0 WHERE singleton=1''', (seed,))
                state = db.execute('SELECT * FROM budget_daily_selection WHERE singleton=1').fetchone()
            if state['next_attempt_at'] and date(state['next_attempt_at']) > now:
                return
        try:
            with transaction(self.factory) as db:
                deadline = time.monotonic() + 1
                db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
                template = db.execute('SELECT * FROM budget_reports WHERE report_id=?', (seed,)).fetchone()
                if not template or template['source_hash'] != SOURCE_HASH:
                    raise ValueError('Accepted selected template missing')
                if len(template['evidence_json'].encode()) > MAX_REPORT:
                    raise ValueError('Evidence size limit')
                evidence = json.loads(template['evidence_json'])
                base = db.execute('SELECT * FROM budget_cycle_runs WHERE run_id=?',
                    (evidence.get('calculation', {}).get('runId'),)).fetchone()
                if not base:
                    raise ValueError('Accepted cycle capture missing')
                if now >= date(base['end_at']):
                    db.execute("UPDATE budget_daily_selection SET status='CYCLE_EXPIRED',expires_at=? WHERE singleton=1", (base['end_at'],))
                    return
                run = db.execute("SELECT * FROM reconciliation_runs WHERE status='OBSERVED' ORDER BY finished_at DESC LIMIT 1").fetchone()
                if not run or date(run['finished_at']) > now:
                    return
                if db.execute('SELECT 1 FROM budget_daily_jobs WHERE seed_report_id=? AND reconciliation_id=?',
                              (seed, run['run_id'])).fetchone():
                    return
                stored = db.execute('SELECT raw_json FROM budget_sources WHERE source_hash=?', (SOURCE_HASH,)).fetchone()
                if not stored:
                    raise ValueError('Accepted rule map missing')
                document = contract(stored[0])
                report = calculate(db, document, evidence, stamp(now))
                report, expiry = refreshed_report(db, document, evidence, report, run, now)
                projection(report, '0' * 64, stamp(now))  # Exact existing allowlist validation.
                payload = canonical(report)
                if len(payload.encode()) > MAX_REPORT:
                    raise ValueError('Report size limit')
                identity = hashlib.sha256(canonical({'report': report, 'evidence': evidence}).encode()).hexdigest()
                db.execute('INSERT OR IGNORE INTO budget_reports VALUES(?,?,?,?,?)',
                           (identity, SOURCE_HASH, stamp(now), canonical(evidence), payload))
                db.execute('INSERT INTO budget_daily_jobs VALUES(?,?,?,?)', (seed, run['run_id'], identity, stamp(now)))
                db.execute('''UPDATE budget_daily_selection SET selected_report_id=?,last_reconciliation_id=?,
                    expires_at=?,status='READY',next_attempt_at=NULL,failure_count=0 WHERE singleton=1''',
                           (identity, run['run_id'], expiry))
        except Exception:
            # Rollback leaves the previous immutable selection intact. Never log
            # exception messages, report data, private identifiers or credentials.
            LOG.warning('DAILY_REPORT_UNAVAILABLE')
            with transaction(self.factory) as db:
                row = db.execute('SELECT failure_count FROM budget_daily_selection WHERE singleton=1').fetchone()
                count = row[0] + 1
                retry = stamp(now + timedelta(seconds=min(3600, 30 * 2 ** min(count - 1, 7))))
                db.execute("UPDATE budget_daily_selection SET status='RETRY',failure_count=?,next_attempt_at=? WHERE singleton=1",
                           (count, retry))
