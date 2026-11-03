"""Authenticated projection of one explicitly selected report; no financial writes."""
import asyncio
from datetime import date, datetime, timezone
import hmac
import json
import os
from pathlib import Path
import re
import sqlite3
import time

from fastapi import Request
from starlette.concurrency import run_in_threadpool
from starlette.requests import ClientDisconnect
from starlette.responses import JSONResponse

RULE_HASH = '62e8a58e185d192ef757b810f200b9da5b14d7c8d7a4690a7c6cc8030e97e878'
MAX_REPORT = 2 * 1024 * 1024
MAX_RESPONSE = 64 * 1024
HEADERS = {'Cache-Control': 'private, no-store'}
FLEXIBLE = ('groceries', 'pet-supplies', 'ptv', 'charging', 'other-irregular', 'discretionary')
BLOCKERS = {'NO_CYCLE_CATCHUP_AND_COMPLETENESS_EVIDENCE', 'NO_CYCLE_CATCHUP',
    'CYCLE_PAGINATION_INCOMPLETE_OR_FUTURE', 'CATCHUP_BINDING_MISSING', 'UNRESOLVED_MERCHANT_ROUTE',
    'TRANSACTION_CHANGED_OR_UNRESOLVED', 'UNCLASSIFIED_UP_DEBIT', 'UNCLASSIFIED_UP_CREDIT_OR_REFUND',
    'UNRESOLVED_OCCURRENCE_MATCHES', 'PROTECTED_FUNDING_UNVERIFIED', 'PROTECTED_REMAINING_UNKNOWN',
    'PROTECTED_ACCOUNT_UNDERFUNDED', 'UNRESOLVED_COMPONENT', 'DEDUCTION_UNKNOWN',
    'INCREMENTAL_COVERAGE_UNVERIFIED'}
QUALIFICATIONS = {'FULL_ACCEPTED_ALLOCATION_FUNDING_CHECK', 'INCOMPLETE_ALL_BANK_CATEGORY',
    'OUTSIDE_CYCLE_RENEWAL_ROUTE_UNKNOWN', 'OWNER_DEFERRED_CURRENT_CYCLE_CONTRIBUTION',
    'UNCLASSIFIED_REFLECTED_UP_HOLD', 'PRIOR_CATEGORY_ALLOCATIONS_PARTIAL_PLANNING_ONLY',
    'INCREMENTAL_OBSERVATIONS_NOT_FULL_CYCLE_PAGINATION'}
ASSUMPTIONS = {'ASSOCIATED_PENDING_CARD_OBSERVATION_REQUIRES_NO_ADDITIONAL_UP_CASH_IN_THIS_CYCLE',
    'NO_OTHER_CURRENT_CASH_LIABILITY_BEYOND_SUPPORTED_EXISTING_GATES',
    'ACCEPTED_FUTURE_BOOKING_ESTIMATES_AND_UP_ROUTE_APPLY', 'NO_UNOBSERVED_ACTIVITY_SINCE_BALANCE_OBSERVATION',
    'NO_UNOBSERVED_ACTIVITY_DURING_COLLECTION_WINDOW', 'ACCEPTED_ESTIMATES_AND_OCCURRENCE_PLANS_REMAIN_CONDITIONAL',
    'COMPLETE_ROW_EVIDENCE_INCLUDES_PRIOR_BANK_AND_SHARED_CATEGORY_SPENDING',
    'KNOWN_MATCH_RESIDUALS_ARE_PLANNING_PROTECTIONS_NOT_COMPLETE_ACTUALS',
    'UNALLOCATED_PAST_ACTUALS_DO_NOT_CREATE_NEW_RECURRING_RESERVES'}


class SummaryUnavailable(ValueError):
    pass


def minor(value):
    if value is not None and (type(value) is not int or not -2**63 < value < 2**63):
        raise SummaryUnavailable()
    return value


def source_time(value):
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > 40:
        raise SummaryUnavailable()
    if re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        date.fromisoformat(value)
    else:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            raise SummaryUnavailable()
    return value


def codes(values, allowed, other):
    if not isinstance(values, list) or len(values) > 1000 or any(not isinstance(v, str) for v in values):
        raise SummaryUnavailable()
    return sorted({v.split(':', 1)[0] if v.split(':', 1)[0] in allowed else other for v in values})


def projection(report, report_id, served_at):
    if report['schema'] != 'nico-budget-report/v1' or report['source_hash'] != RULE_HASH:
        raise SummaryUnavailable()
    generated = source_time(report['as_of'])
    if generated is None or datetime.fromisoformat(generated.replace('Z', '+00:00')).tzinfo is None:
        raise SummaryUnavailable()
    cycle = report['cycle']
    start, end = date.fromisoformat(cycle['start']), date.fromisoformat(cycle['endExclusive'])
    if start >= end:
        raise SummaryUnavailable()
    s = report['up_spending']
    obs = s['observation']
    if obs is not None and obs['currency'] != 'AUD':
        raise SummaryUnavailable()
    observed_at = source_time(obs['as_of']) if obs else None
    deductions = s['deductions']
    if not isinstance(deductions, list) or len(deductions) > 100:
        raise SummaryUnavailable()
    amounts = {}
    for item in deductions:
        if item['id'] in amounts:
            raise SummaryUnavailable()
        amounts[item['id']] = minor(item['amount_minor'])
    components = [{'id': k, 'amount_minor': amounts.get(k if k == 'discretionary' else 'variable:' + k)} for k in FLEXIBLE]
    booked = s['booked_commitment_occurrences']
    if not isinstance(booked, list) or len(booked) > 100:
        raise SummaryUnavailable()
    future, pending, past_unknown = [], [], False
    quotes = []
    for item in booked:
        quote = item.get('provider_quote_verified')
        if quote is not None and type(quote) is not bool:
            raise SummaryUnavailable()
        quotes.append(quote)
        state = item['state']
        if state == 'FUTURE_BOOKED_ESTIMATE_NOT_QUOTE':
            future.append(minor(item['planning_estimate_minor']))
        elif state == 'ASSOCIATED_PENDING_CARD_NOT_SETTLEMENT':
            p = item['pending_card_observation']
            if p['currency'] != 'AUD' or p['status'] != 'HELD':
                raise SummaryUnavailable()
            pending.append(minor(p['amountMinor']))
        elif state == 'PAST_PLAN_PAYMENT_UNRESOLVED':
            past_unknown = True
        elif state != 'EXPLICIT_MATCH_RETAINED':
            raise SummaryUnavailable()
    def total(values):
        return minor(sum(values)) if all(v is not None for v in values) else None
    scenario = s['cash_after_committed_obligations_scenario']
    display_scenario = None
    if scenario is not None:
        if scenario['basis'] != 'ASSUMPTION_BASED_NOT_ASSURED_NOT_SETTLEMENT':
            raise SummaryUnavailable()
        display_scenario = {'amount_minor': minor(scenario['amount_minor']),
            'balance_as_of': source_time(scenario['balance_as_of']),
            'assumption_codes': codes(scenario['assumptions'], ASSUMPTIONS, 'OTHER_ASSUMPTION'), 'assured': False}
    card = report['commbank_credit']
    current_card = card['observation']
    if current_card is not None:
        if current_card['currency'] != 'AUD':
            raise SummaryUnavailable()
        card_amount = minor(current_card['availableCreditMinor'])
        card_at = source_time(current_card['asOf'])
        card_state = 'OBSERVATION_ONLY'
    else:
        dated = card.get('retained_dated_observation')
        card_amount = minor(dated['available_credit_minor']) if dated else None
        card_at = source_time(dated['as_of']) if dated else None
        card_state = 'HISTORICAL_NOT_CURRENT' if dated else 'UNAVAILABLE'
    blockers = codes(s['arithmetic_blockers'], BLOCKERS, 'OTHER_UNRESOLVED')
    qualifiers = codes(s['qualifications'], QUALIFICATIONS, 'OTHER_QUALIFICATION')
    limits = {'OBSERVATION_NOT_REAL_TIME', 'CARD_CREDIT_NOT_CASH'}
    limits.add('BANK_SNAPSHOT_NOT_ATOMIC' if obs and obs.get('coverage') == 'CYCLE_PAGINATION_COMPLETE_NOT_BANK_ATOMIC' else 'SOURCE_COVERAGE_UNKNOWN')
    if 'INCOMPLETE_ALL_BANK_CATEGORY' in qualifiers:
        limits.add('INCOMPLETE_ALL_BANK_COVERAGE')
    if pending or past_unknown:
        limits.add('PENDING_CARD_REFLECTION_UNRESOLVED')
    if scenario is not None:
        limits.add('ASSUMPTION_SCENARIO_NOT_ASSURED')
    if 'UNCLASSIFIED_REFLECTED_UP_HOLD' in qualifiers:
        limits.add('HELD_AMOUNT_NOT_DEDUCTED_AGAIN')
    if 'OWNER_DEFERRED_CURRENT_CYCLE_CONTRIBUTION' in qualifiers:
        limits.add('SAVINGS_CONTRIBUTIONS_DEFERRED_THIS_CYCLE')
    result = {'schema': 'nico.finance-summary/v1',
        'report': {'report_id': report_id, 'selection_state': 'OWNER_SELECTED',
            'generated_at': generated, 'served_at': served_at,
            'cycle': {'start': start.isoformat(), 'end_exclusive': end.isoformat()}, 'currency': 'AUD'},
        'up_cash': {'observed_available_minor': minor(obs['available_after_holds_minor']) if obs else None,
            'observed_at': observed_at, 'observation_state': 'OBSERVATION_ONLY' if obs else 'UNAVAILABLE'},
        'planning': {'flexible_total_minor': minor(s['remaining_flexible_plan_minor']), 'flexible_components': components},
        'commitments': {'booked_planning_total_minor': minor(s['booked_commitment_planning_minor']),
            'future_booked_estimate_minor': total(future), 'past_pending_card_observation_minor': total(pending),
            'pending_state': 'ASSOCIATED_PENDING_CARD_NOT_SETTLEMENT' if pending else ('UNKNOWN' if past_unknown else 'NO_ASSOCIATED_PENDING_OBSERVATION'),
            'provider_quote_verified': (quotes[0] if quotes and all(v is quotes[0] for v in quotes) else None)},
        'scenarios': {'full_budget_plan_room_minor': minor(s['full_budget_plan_cash_room_minor']),
            'full_budget_plan_basis': 'FULL_BUDGET_PLAN_SCENARIO_NOT_COMMITTED_DEBT',
            'cash_after_committed_obligations_minor': minor(s['cash_after_committed_obligations_minor']),
            'assumption_based': display_scenario, 'assured': False},
        'card_credit': {'observed_available_credit_minor': card_amount, 'observed_at': card_at,
            'observation_state': card_state, 'credit_is_cash': False},
        'coverage': {'arithmetic_blocker_codes': blockers, 'qualification_codes': qualifiers,
            'assumption_codes': codes(s['assumptions'], ASSUMPTIONS, 'OTHER_ASSUMPTION'), 'limitation_codes': sorted(limits)}}
    if len(json.dumps(result, allow_nan=False).encode()) > MAX_RESPONSE:
        raise SummaryUnavailable()
    return result


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise SummaryUnavailable()
        result[key] = value
    return result


def read_selected(root, require_mount, report_id):
    root = Path(root)
    directory, database = root / 'nico-ingress', root / 'nico-ingress/events.sqlite3'
    if (root.is_symlink() or directory.is_symlink() or database.is_symlink()
            or (require_mount and not os.path.ismount(root)) or not database.is_file()):
        raise SummaryUnavailable()
    deadline = time.monotonic() + 2
    db = sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True, timeout=1)
    try:
        db.execute('PRAGMA query_only=ON')
        db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
        selection_state = 'OWNER_SELECTED'
        if db.execute("SELECT 1 FROM sqlite_schema WHERE name='budget_daily_selection'").fetchone():
            daily = db.execute('''SELECT selected_report_id,expires_at,status FROM budget_daily_selection
                WHERE singleton=1 AND seed_report_id=?''', (report_id,)).fetchone()
            if daily:
                if daily[2] == 'CYCLE_EXPIRED' or (daily[1] and datetime.now(timezone.utc) >= datetime.fromisoformat(daily[1])):
                    raise SummaryUnavailable()
                if daily[0]:
                    report_id = daily[0]
                    selection_state = 'DAILY_GENERATED'
        row = db.execute('''SELECT source_hash,as_of,
            CASE WHEN length(report_json)<=? THEN report_json ELSE NULL END
            FROM budget_reports WHERE report_id=?''', (MAX_REPORT, report_id)).fetchone()
        if not row or row[0] != RULE_HASH or row[2] is None or len(row[2].encode()) > MAX_REPORT:
            raise SummaryUnavailable()
        report = json.loads(row[2], object_pairs_hook=_pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(SummaryUnavailable()))
        if report['as_of'] != row[1]:
            raise SummaryUnavailable()
        result = projection(report, report_id, datetime.now(timezone.utc).isoformat())
        result['report']['selection_state'] = selection_state
        return result
    finally:
        db.close()


def install_summary_route(application, *, storage_root=Path('/data'), require_mount=True, settings_provider=None):
    def settings():
        return (os.environ.get('SUMMARY_AUTH_TOKEN'), os.environ.get('SUMMARY_REPORT_ID'))
    provider = settings_provider or settings
    def error(status, code):
        return JSONResponse({'error': {'code': code}}, status_code=status, headers=HEADERS)
    @application.api_route('/api/v1/summary', methods=['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'], include_in_schema=False)
    async def summary(request: Request):
        if request.method != 'GET':
            return error(405, 'UNSUPPORTED_METHOD')
        token, selected = provider()
        if (not isinstance(token, str) or not 32 <= len(token) <= 512 or not token.isascii()
                or any(c.isspace() for c in token) or not isinstance(selected, str)
                or not re.fullmatch('[0-9a-f]{64}', selected)):
            return error(503, 'SUMMARY_UNAVAILABLE')
        auth = request.headers.getlist('authorization')
        if len(auth) != 1 or len(auth[0]) > 600 or not hmac.compare_digest(auth[0].encode(), ('Bearer ' + token).encode()):
            return error(401, 'UNAUTHENTICATED')
        if request.scope.get('query_string') or request.headers.get('transfer-encoding') or request.headers.get('content-length', '0') != '0':
            return error(400, 'UNSUPPORTED_REQUEST')
        try:
            async with asyncio.timeout(1):
                async for chunk in request.stream():
                    if chunk:
                        return error(400, 'UNSUPPORTED_REQUEST')
        except (TimeoutError, RuntimeError, ClientDisconnect):
            return error(400, 'UNSUPPORTED_REQUEST')
        try:
            async with asyncio.timeout(3):
                payload = await run_in_threadpool(read_selected, storage_root, require_mount, selected)
            return JSONResponse(payload, headers=HEADERS)
        except (SummaryUnavailable, OSError, sqlite3.Error, ValueError, KeyError, TypeError, RecursionError, TimeoutError):
            return error(503, 'SUMMARY_UNAVAILABLE')
