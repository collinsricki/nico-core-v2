"""Explicit bounded cycle collection and conditional cash arithmetic; no auto-start."""

from datetime import datetime
import json
from urllib.parse import urlencode
from uuid import uuid4

from finance_worker import (UpFailure, date, identifier, money, normalize_transaction,
                            project, stamp, transaction, utcnow)


# Accepted fact in the pinned budget_rules.SOURCE_HASH contract; not a payment
# match or a future route. Do not infer it from an unmatched historical debit.
ACCEPTED_CHATGPT_NEXT_RENEWAL = '2026-11-19'


def schema(db):
    if db.execute('PRAGMA user_version').fetchone()[0] != 2:
        raise ValueError('Accepted core schema 2 required')
    db.execute('''CREATE TABLE IF NOT EXISTS budget_cycle_runs (
        run_id TEXT PRIMARY KEY,start_at TEXT NOT NULL,end_at TEXT NOT NULL,
        through_at TEXT NOT NULL,started_at TEXT NOT NULL,finished_at TEXT,
        status TEXT NOT NULL,error_code TEXT)''')
    db.execute('''CREATE TABLE IF NOT EXISTS budget_cycle_accounts (
        run_id TEXT NOT NULL,account_id TEXT NOT NULL,balance_minor INTEGER NOT NULL,
        currency TEXT NOT NULL,balance_window_start TEXT NOT NULL,balance_window_end TEXT NOT NULL,
        next_url TEXT,pages INTEGER NOT NULL DEFAULT 0,complete INTEGER NOT NULL DEFAULT 0,
        conflicts INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(run_id,account_id))''')
    db.execute('''CREATE TABLE IF NOT EXISTS budget_cycle_members (
        run_id TEXT NOT NULL,account_id TEXT NOT NULL,transaction_id TEXT NOT NULL,
        resource_json TEXT NOT NULL,PRIMARY KEY(run_id,transaction_id))''')


def boundaries(start, end):
    # Calendar dates were approved; their timezone was not. Require explicit offsets.
    values = [datetime.fromisoformat(x) for x in (start, end)]
    if (any(x.tzinfo is None or x.time().isoformat() != '00:00:00' for x in values)
            or [x.date().isoformat() for x in values] != ['2026-10-14', '2026-11-15']):
        raise ValueError('Explicit local-midnight offsets for the accepted cycle required')
    return tuple(stamp(x) for x in values)


def read(factory, sql, args=()):
    db = factory()
    try:
        db.row_factory = __import__('sqlite3').Row
        return [dict(x) for x in db.execute(sql, args)]
    finally:
        db.close()


def catch_up(factory, client, bindings, start, end, clock=utcnow, resume=None):
    """GET only, at most ten pages/account/invocation. Explicit resume retains checkpoint.

    Balances and pagination are not a bank-atomic snapshot. Preserve their windows;
    conditional reports explicitly assume no intervening collection-window activity.
    """
    start, end = boundaries(start, end)
    ids = sorted({identifier(x['accountId']) for x in bindings.values()})
    if not ids or len(ids) > 20:
        raise ValueError('One to twenty verified account bindings required')
    started = stamp(clock())
    if not start <= started < end:
        raise ValueError('Catch-up is limited to observation time inside the approved cycle')
    with transaction(factory) as db:
        schema(db)
    if resume:
        runs = read(factory, 'SELECT * FROM budget_cycle_runs WHERE run_id=?', (resume,))
        if not runs or runs[0]['start_at'] != start or runs[0]['end_at'] != end:
            raise ValueError('Resume does not identify this approved cycle')
        run = runs[0]
        if sorted(x['account_id'] for x in read(factory, 'SELECT account_id FROM budget_cycle_accounts WHERE run_id=?', (resume,))) != ids:
            raise ValueError('Resume bindings differ')
        if run['status'] == 'COMPLETE':
            return {'run_id': resume, 'status': 'COMPLETE', 'already_complete': True}
        run_id, through = resume, run['through_at']
    else:
        accounts = []
        for account_id in ids:
            path = '/accounts/' + account_id
            window_start = stamp(clock())
            resource = client.get(client.BASE + path, path).get('data', {})
            if resource.get('type') != 'accounts' or resource.get('id') != account_id:
                raise UpFailure('ACCOUNT_RESPONSE_MISMATCH', 'permanent')
            balance, currency = money(resource.get('attributes', {}).get('balance'))
            if currency != 'AUD':
                raise UpFailure('NON_AUD_BOUND_ACCOUNT', 'permanent')
            accounts.append((account_id, balance, currency, window_start, stamp(clock())))
        through = min(stamp(clock()), end)
        run_id = str(uuid4())
        with transaction(factory) as db:
            db.execute('INSERT INTO budget_cycle_runs VALUES (?,?,?,?,?,?,?,?)',
                       (run_id, start, end, through, started, None, 'INCOMPLETE', None))
            for account_id, balance, currency, first, last in accounts:
                path = '/accounts/' + account_id + '/transactions'
                url = client.BASE + path + '?' + urlencode({'page[size]': 100, 'filter[since]': start, 'filter[until]': through})
                db.execute('''INSERT INTO budget_cycle_accounts
                    (run_id,account_id,balance_minor,currency,balance_window_start,balance_window_end,next_url)
                    VALUES (?,?,?,?,?,?,?)''', (run_id, account_id, balance, currency, first, last, url))
    try:
        for account in read(factory, 'SELECT * FROM budget_cycle_accounts WHERE run_id=? ORDER BY account_id', (run_id,)):
            url = account['next_url']
            path = '/accounts/' + account['account_id'] + '/transactions'
            for _ in range(10):
                if url is None:
                    break
                page = client.get(url, path)  # Existing client enforces exact HTTPS host/path.
                if (not isinstance(page.get('data'), list) or len(page['data']) > 100
                        or not isinstance(page.get('links'), dict) or 'next' not in page['links']):
                    raise UpFailure('INVALID_PAGE', 'permanent')
                next_url = page['links']['next']
                if next_url is not None and (not isinstance(next_url, str) or next_url == url):
                    raise UpFailure('INVALID_PAGE_URL', 'permanent')
                snapshots = [normalize_transaction(x) for x in page['data']]
                if any(x['account_id'] != account['account_id'] or not start <= x['created_at'] <= through
                       or x['currency_code'] != 'AUD' for x in snapshots):
                    raise UpFailure('CYCLE_RESOURCE_OUTSIDE_SCOPE', 'permanent')
                with transaction(factory) as db:
                    conflicts = 0
                    for item in snapshots:
                        old = db.execute('SELECT resource_json FROM budget_cycle_members WHERE run_id=? AND transaction_id=?',
                                         (run_id, item['transaction_id'])).fetchone()
                        if old and old[0] != item['resource_json']:
                            raise UpFailure('DUPLICATE_TRANSACTION_CONFLICT')
                        conflicts += int(bool(project(db, item, clock())))
                        db.execute('INSERT OR IGNORE INTO budget_cycle_members VALUES (?,?,?,?)',
                                   (run_id, account['account_id'], item['transaction_id'], item['resource_json']))
                    db.execute('''UPDATE budget_cycle_accounts SET next_url=?,pages=pages+1,
                        complete=?,conflicts=conflicts+? WHERE run_id=? AND account_id=?''',
                               (next_url, int(next_url is None), conflicts, run_id, account['account_id']))
                url = next_url
        with transaction(factory) as db:
            incomplete = db.execute('SELECT COUNT(*) FROM budget_cycle_accounts WHERE run_id=? AND (complete=0 OR conflicts>0)', (run_id,)).fetchone()[0]
            status = 'INCOMPLETE' if incomplete else 'COMPLETE'
            db.execute('UPDATE budget_cycle_runs SET finished_at=?,status=?,error_code=NULL WHERE run_id=?',
                       (stamp(clock()), status, run_id))
        return {'run_id': run_id, 'status': status, 'through_at': through}
    except UpFailure as error:
        with transaction(factory) as db:
            db.execute('UPDATE budget_cycle_runs SET status=?,error_code=? WHERE run_id=?',
                       ('AUTH_REQUIRED' if error.kind == 'auth' else 'INCOMPLETE', error.code, run_id))
        return {'run_id': run_id, 'status': 'INCOMPLETE', 'error_code': error.code}


def calculate(db, document, evidence, report):
    """Extend the retained report; evidence declarations never substitute for API coverage."""
    from budget_rules import EvidenceError, occurrences, references
    proof = evidence.get('calculation', {})
    if not isinstance(proof, dict) or set(proof) - {'runId', 'rowCoverage', 'routes', 'fundedElsewhere', 'deferredContributions'}:
        raise EvidenceError('Unsupported calculation evidence')
    spending = report['up_spending']
    spending['conditional_gaps'] = gaps = []
    spending['deductions'] = deductions = []
    spending['arithmetic_blockers'] = []
    spending['qualifications'] = []
    spending['assured_safe_withdrawal_minor'] = None
    spending['assumptions'] = ['NO_UNOBSERVED_ACTIVITY_DURING_COLLECTION_WINDOW',
        'ACCEPTED_ESTIMATES_AND_OCCURRENCE_PLANS_REMAIN_CONDITIONAL',
        'COMPLETE_ROW_EVIDENCE_INCLUDES_PRIOR_BANK_AND_SHARED_CATEGORY_SPENDING',
        'KNOWN_MATCH_RESIDUALS_ARE_PLANNING_PROTECTIONS_NOT_COMPLETE_ACTUALS',
        'UNALLOCATED_PAST_ACTUALS_DO_NOT_CREATE_NEW_RECURRING_RESERVES']
    def unavailable(reason):
        gaps.append(reason)
        spending['arithmetic_blockers'] = list(gaps)
        return report
    def finite_minor(value):
        return type(value) is int and 0 <= value < 2**63
    if not proof:
        return unavailable('NO_CYCLE_CATCHUP_AND_COMPLETENESS_EVIDENCE')
    if not db.execute("SELECT 1 FROM sqlite_schema WHERE name='budget_cycle_runs'").fetchone():
        return unavailable('NO_CYCLE_CATCHUP')
    run = db.execute('SELECT * FROM budget_cycle_runs WHERE run_id=?', (proof.get('runId'),)).fetchone()
    if not run or run['status'] != 'COMPLETE' or run['through_at'] > report['as_of']:
        return unavailable('CYCLE_PAGINATION_INCOMPLETE_OR_FUTURE')
    accounts = {x['account_id']: dict(x) for x in db.execute('SELECT * FROM budget_cycle_accounts WHERE run_id=?', (run['run_id'],))}
    bound = {key: value['accountId'] for key, value in evidence.get('bindings', {}).items()}
    for role in ('up.spending', 'up.essentials'):
        if bound.get(role) not in accounts:
            gaps.append('CATCHUP_BINDING_MISSING:' + role)
    spending['cycle_coverage'] = {'run_id': run['run_id'], 'start_at': run['start_at'],
        'through_at': run['through_at'], 'pagination_complete': True, 'report_as_of': report['as_of']}
    for role, account_id in bound.items():
        account = accounts.get(account_id)
        if not account:
            continue
        value = {'account_id': account_id, 'available_after_holds_minor': account['balance_minor'],
                 'currency': account['currency'], 'as_of': account['balance_window_end'],
                 'balance_window_start': account['balance_window_start'], 'source': 'explicit_cycle_catchup',
                 'currentness': 'OBSERVATION_WINDOW_ONLY', 'coverage': 'CYCLE_PAGINATION_COMPLETE_NOT_BANK_ATOMIC'}
        if role == 'up.spending':
            spending['observation'] = value
        else:
            report['protected_essentials_savers']['observations'][role] = value
    rows = {x['budget_id']: x for x in report['rows']}
    originals = {x['id']: x for x in document['budgetRows']}
    routes = proof.get('routes', {})
    coverage = proof.get('rowCoverage', {})
    funding = proof.get('fundedElsewhere', {})
    deferrals = proof.get('deferredContributions', {})
    if any(not isinstance(x, dict) for x in (routes, coverage, funding, deferrals)) or (set(routes) | set(coverage) | set(funding) | set(deferrals)) - set(rows):
        raise EvidenceError('Evidence must reference accepted budget identities')
    def dated(item):
        references(item.get('provenance'))
        return run['through_at'] <= stamp(date(item['asOf'])) <= report['as_of']
    for key, item in deferrals.items():
        if (not isinstance(item, dict)
                or set(item) != {'deferred', 'cycle', 'asOf', 'provenance'}
                or item.get('deferred') is not True
                or item.get('cycle') != {name: report['cycle'][name] for name in ('start', 'endExclusive')}
                or originals[key].get('reserveClass') != 'provision_component_not_extra_expense'
                or rows[key]['reserve_class'] != 'provision_component_not_extra_expense'
                or not dated(item)
                or any(x.get('budgetId') == key and x.get('scheduled')
                       and report['cycle']['start'] <= x['scheduled'][:10] < report['cycle']['endExclusive']
                       for x in occurrences(document))
                or (key in funding and funding[key].get('funded') is True)):
            raise EvidenceError('Deferral requires dated cycle-specific contribution authority without due bill or funding conflict')
        rows[key]['contribution_state'] = 'OWNER_DEFERRED_THIS_CYCLE_ONLY'
        rows[key]['deferred_contribution_target_minor'] = rows[key]['target_minor']
        rows[key]['current_cycle_contribution_required_minor'] = 0
        gaps.append('OWNER_DEFERRED_CURRENT_CYCLE_CONTRIBUTION:' + key)
    for budget_id, item in routes.items():
        if originals[budget_id]['bank'] != 'UNKNOWN_CURRENT_MAPPING' or not dated(item):
            raise EvidenceError('Only dated evidence may resolve an unknown route')
        role = item.get('paymentAccount')
        if role != 'commbank.credit' and role not in bound:
            raise EvidenceError('Route requires a verified bound role or separate card')
        rows[budget_id]['payment_account'] = role
    report['up_spending']['unresolved_current_merchant_mappings'] = [key for key, row in rows.items() if row['payment_account'] == 'UNKNOWN_CURRENT_MAPPING']
    for key in report['up_spending']['unresolved_current_merchant_mappings']:
        scheduled = [item['scheduled'][:10] for item in occurrences(document)
                     if item.get('budgetId') == key and item.get('scheduled')]
        outside_renewal = (key == 'chatgpt'
            and ACCEPTED_CHATGPT_NEXT_RENEWAL >= report['cycle']['endExclusive']
            and not any(report['cycle']['start'] <= due < report['cycle']['endExclusive']
                        for due in scheduled))
        if outside_renewal:
            rows[key]['next_accepted_occurrence'] = ACCEPTED_CHATGPT_NEXT_RENEWAL
            rows[key]['current_cycle_future_payment_reserve_minor'] = 0
            gaps.append('OUTSIDE_CYCLE_RENEWAL_ROUTE_UNKNOWN:' + key)
        else:
            gaps.append('UNRESOLVED_MERCHANT_ROUTE:' + key)
    matches = report['matched_occurrences']
    aliases = {key for item in matches for key in item.get('resource_aliases', [item['resource_key']])}
    # Every movement needs classification. Unsupported credits/refunds cannot silently
    # become zero category actuals; internal funding can use existing transfer matches.
    for current in db.execute('SELECT * FROM transactions WHERE account_id IN (SELECT account_id FROM budget_cycle_accounts WHERE run_id=?) AND created_at>=? AND created_at<=?',
                              (run['run_id'], run['start_at'], run['through_at'])):
        if current['is_deleted']:
            continue
        member = db.execute('SELECT resource_json FROM budget_cycle_members WHERE run_id=? AND transaction_id=?', (run['run_id'], current['transaction_id'])).fetchone()
        consistent = not (current['ordering_conflict'] or not member or member[0] != current['resource_json']
                          or current['currency_code'] != 'AUD')
        if not consistent:
            gaps.append('TRANSACTION_CHANGED_OR_UNRESOLVED:' + current['transaction_id'])
        if current['amount_base_units'] is not None and current['amount_base_units'] < 0 and 'UP:' + current['transaction_id'] not in aliases:
            account = accounts.get(current['account_id'])
            # A hold predating its balance window is reflected under the explicit
            # no-window-activity assumption. It is not a second cash deduction.
            reflected_hold = (consistent and current['up_status'] == 'HELD' and account
                              and current['created_at'] <= account['balance_window_start'])
            gaps.append(('UNCLASSIFIED_REFLECTED_UP_HOLD:' if reflected_hold else 'UNCLASSIFIED_UP_DEBIT:')
                        + current['transaction_id'])
        if current['amount_base_units'] is not None and current['amount_base_units'] > 0 and 'UP:' + current['transaction_id'] not in aliases:
            gaps.append('UNCLASSIFIED_UP_CREDIT_OR_REFUND:' + current['transaction_id'])
    if report['unresolved_matches']:
        gaps.append('UNRESOLVED_OCCURRENCE_MATCHES')
    ledger_complete = not any(x.startswith(('TRANSACTION_CHANGED_OR_UNRESOLVED:', 'UNCLASSIFIED_UP_',
        'UNCLASSIFIED_REFLECTED_UP_HOLD:', 'UNRESOLVED_OCCURRENCE_MATCHES')) for x in gaps)
    accepted_old = {x['budgetId']: x for x in occurrences(document)
                    if x['kind'] == 'OLD_BANK_PAID' and x['budgetId'] in ('rent', 'tesla-payment')}
    satisfied_old = set()
    for key, row in rows.items():
        complete = ledger_complete and key in coverage and coverage[key].get('complete') is True and dated(coverage[key])
        row['coverage'] = 'COMPLETE_EXPLICIT_ALL_BANK_ROW_EVIDENCE' if complete else row['coverage']
        if complete:
            row['actual_minor'] = row['known_committed_spend_minor']
            row['remaining_minor'] = max(0, row['target_minor'] - row['actual_minor'])
        # A full satisfied unique bill is not an unspent allocation to reserve again.
        paid = any(x['budget_id'] == key and x['kind'] == 'EXPENSE' and x['status'] in ('POSTED','SETTLED') for x in matches)
        accepted = accepted_old.get(key)
        paid_old_outside_next = (accepted is not None
            and originals[key].get('nextScheduled', '')[:10] >= report['cycle']['endExclusive']
            and any(x['budget_id'] == key and x['occurrence_id'] == accepted['id']
                    and x['resource_key'] == 'LEGACY:' + accepted['transactionId']
                    and x['amount_minor'] == accepted['amountMinor']
                    and x['kind'] == 'EXPENSE' and x['status'] in ('POSTED', 'SETTLED')
                    for x in matches))
        if row['reserve_class'] == 'inactive_zero':
            row['current_reserve_minor'] = 0
        elif row['reserve_class'] == 'zero_routine_allowance_incidental_from_discretionary' and row['target_minor'] == 0:
            row['current_reserve_minor'] = 0
            row['conditional_reserve_basis'] = 'ACCEPTED_ZERO_ROUTINE_ALLOWANCE_ACTUALS_RETAINED'
        elif paid_old_outside_next:
            row['current_reserve_minor'] = 0
            satisfied_old.add(key)
        elif row['payment_account'] == 'up.spending':
            row['current_reserve_minor'] = row['remaining_minor'] if complete else max(0, row['target_minor'] - row['known_committed_spend_minor'])
            row['conditional_reserve_basis'] = 'COMPLETE_ROW_EVIDENCE' if complete else 'KNOWN_MATCHES_ONLY'
            if not complete:
                gaps.append('INCOMPLETE_ALL_BANK_CATEGORY:' + key)
        elif originals[key]['bank'] == 'UP' or row['payment_account'] in bound:
            if key in deferrals:
                continue  # Only this cycle's new contribution is deferred; retained cash is untouched.
            if row['reserve_class'] == 'upcoming_unique_bill_from_essentials' and paid and complete:
                row['current_reserve_minor'] = 0
            elif key not in funding or not dated(funding[key]) or funding[key].get('funded') is not True:
                gaps.append('PROTECTED_FUNDING_UNVERIFIED:' + key)
    funded_totals = {}
    for key, item in funding.items():
        if key in satisfied_old:
            continue  # No current-cycle obligation remains to fund a second time.
        role = rows[key]['payment_account']
        if role == 'up.spending' or role not in bound or not dated(item) or item.get('funded') is not True:
            raise EvidenceError('Funding evidence must identify a dated protected bound account')
        # Provision allocations remain earmarks, never an extra spending deduction.
        amount = rows[key]['target_minor'] if rows[key]['reserve_class'] == 'provision_component_not_extra_expense' else rows[key]['remaining_minor']
        next_due = originals[key].get('nextScheduled', '')[:10]
        if (amount is None and rows[key]['reserve_class'] == 'upcoming_unique_bill_from_essentials'
                and report['cycle']['start'] <= next_due < report['cycle']['endExclusive']
                and finite_minor(rows[key]['target_minor'])):
            # Verify a positive funding proof against the whole accepted current
            # allocation, not a fabricated paid state or remaining amount.
            amount = rows[key]['target_minor']
            rows[key]['funding_check_basis'] = 'FULL_ACCEPTED_ALLOCATION_FUNDING_CHECK'
            gaps.append('FULL_ACCEPTED_ALLOCATION_FUNDING_CHECK:' + key)
        if not finite_minor(amount):
            gaps.append('PROTECTED_REMAINING_UNKNOWN:' + key)
        else:
            rows[key]['funding_check_amount_minor'] = amount
            funded_totals[role] = funded_totals.get(role, 0) + amount
    for role, amount in funded_totals.items():
        if bound[role] not in accounts or accounts[bound[role]]['balance_minor'] < amount:
            gaps.append('PROTECTED_ACCOUNT_UNDERFUNDED:' + role)
    # Reuse accepted component arithmetic; paid/held invoice reflects the observed balance.
    for component in spending['planning_components']:
        value = component['conditional_planning_protection_minor']
        if component['id'] == '1password-current-invoice':
            found = next((x for x in matches if x['occurrence_id'] == 'supplemental:1password-current-invoice' and x['kind'] == 'EXPENSE'), None)
            if found:
                reflected = any(key.startswith('UP:') and db.execute('SELECT 1 FROM budget_cycle_members WHERE run_id=? AND transaction_id=?', (run['run_id'], key[3:])).fetchone() for key in found.get('resource_aliases', [found['resource_key']]))
                value = 0 if reflected else None
        if value is None:
            gaps.append('UNRESOLVED_COMPONENT:' + component['id'])
        component['current_reserve_minor'] = value
        deductions.append({'id': component['id'], 'amount_minor': value, 'source': 'ACCEPTED_CONDITIONAL_RULE'})
    represented = {key for ids in __import__('budget_rules').VARIABLE_GROUPS.values() for key in ids} | {'discretionary'}
    for key, row in rows.items():
        if row['payment_account'] == 'up.spending' and key not in represented:
            value = row.get('current_reserve_minor')
            deductions.append({'id': 'row:' + key, 'amount_minor': value, 'source': 'ACCEPTED_ROW_REMAINING'})
    if any(not finite_minor(x['amount_minor']) for x in deductions):
        gaps.append('DEDUCTION_UNKNOWN')
    gaps[:] = sorted(set(gaps))
    qualification_prefixes = ('INCOMPLETE_ALL_BANK_CATEGORY:', 'UNCLASSIFIED_REFLECTED_UP_HOLD:',
                              'FULL_ACCEPTED_ALLOCATION_FUNDING_CHECK:', 'OUTSIDE_CYCLE_RENEWAL_ROUTE_UNKNOWN:',
                              'OWNER_DEFERRED_CURRENT_CYCLE_CONTRIBUTION:')
    spending['qualifications'] = [x for x in gaps if x.startswith(qualification_prefixes)]
    spending['arithmetic_blockers'] = [x for x in gaps if not x.startswith(qualification_prefixes)]
    spending['known_deductions_subtotal_minor'] = sum(x['amount_minor'] for x in deductions if finite_minor(x['amount_minor']))
    spending['unknown_deduction_ids'] = [x['id'] for x in deductions if not finite_minor(x['amount_minor'])]
    spending['known_component_planning_subtotal_minor'] = (sum(x['current_reserve_minor'] for x in spending['planning_components'])
        if all(finite_minor(x['current_reserve_minor']) for x in spending['planning_components']) else None)
    spending['planning_subtotal_is_spendable'] = False
    spending['protected_cash_included_minor'] = 0
    spending['conditional_balance_as_of'] = spending['observation']['as_of'] if spending['observation'] else None
    if not spending['arithmetic_blockers']:
        total = sum(x['amount_minor'] for x in deductions)
        spending['current_reserve_minor'] = total
        spending['conditional_cash_room_minor'] = spending['observation']['available_after_holds_minor'] - total
        spending['qualification'] = 'CONDITIONAL_AT_OBSERVATION_TIME_NOT_ASSURED_OR_BANK_ATOMIC'
    else:
        spending['current_reserve_minor'] = None
        spending['conditional_cash_room_minor'] = None
    report['actual_total_minor'] = sum(x['actual_minor'] for x in rows.values()) if all(x['actual_minor'] is not None for x in rows.values()) else None
    report['remaining_total_minor'] = sum(x['remaining_minor'] for x in rows.values()) if all(x['remaining_minor'] is not None for x in rows.values()) else None
    return report
