"""Bounded owner-budget projection; no banking, storage writes or accounting actions."""
from datetime import date, datetime, timezone
import json

from finance_summary import (MAX_RESPONSE, RULE_HASH, SummaryUnavailable,
                             projection, read_selected_record)

EXPECTED_ITEMS = 31
EXPECTED_IDS = ('rent', 'electricity', 'water', 'phone', 'internet', 'fines', 'groceries',
    'pet-supplies', 'salon', 'brows', 'tesla-payment', 'tesla-insurance', 'ptv', 'charging',
    'registration', 'servicing', 'tolls', 'health-insurance', 'medical', 'chatgpt',
    'apple-music', 'icloud-plus', 'newshosting', 'plex', 'microsoft-365', 'discretionary',
    'other-irregular', 'storage', 'tesla-fsd', 'google-one', 'health-auto-sync')
SAFE_INTEGER = 9007199254740991
# Technical privacy policy only. No personal amounts or private records in source.
PRIVATE_LABEL_IDS = frozenset(('health-insurance', 'medical', 'health-auto-sync'))
ROLE_LABELS = {'up.spending': 'Up Spending', 'up.essentials': 'Up Essentials',
    'Car servicing and tyres Saver': 'Protected Saver', 'Medical and dental Saver': 'Protected Saver'}
CARD_ROLES = frozenset(('legacy.frollo.2', 'commbank.credit'))


def fail():
    raise SummaryUnavailable()


def amount(value, nullable=True):
    if value is None and nullable:
        return None
    if type(value) is not int or not 0 <= value <= SAFE_INTEGER:
        fail()
    return value


def position(value):
    if value is None:
        return None
    if type(value) is not int or not -SAFE_INTEGER <= value <= SAFE_INTEGER:
        fail()
    return value


def text(value, maximum=80):
    if not isinstance(value, str) or not 0 < len(value) <= maximum or any(ord(c) < 32 for c in value):
        fail()
    return value


def calendar(value):
    if value is None:
        return None
    if not isinstance(value, str):
        fail()
    return date.fromisoformat(value[:10]).isoformat()


def timestamp(value):
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    return value if parsed.tzinfo is not None else None


def build_budget(report, evidence, selected, selection_state, served_at):
    meta = projection(report, selected, served_at)  # Existing report/source validation.
    rows = report['rows']
    if not isinstance(rows, list) or len(rows) != EXPECTED_ITEMS:
        fail()
    ids = [text(row['budget_id']) for row in rows]
    if tuple(ids) != EXPECTED_IDS or report['source_hash'] != RULE_HASH:
        fail()
    # source_row is part of this immutable accepted report, not today's live mirror.
    sources = [row['source_row'] for row in rows]
    if any(source.get('id') != identity for source, identity in zip(sources, ids)):
        fail()
    matches = report['matched_occurrences']
    if not isinstance(matches, list) or len(matches) > 1000:
        fail()
    by_budget = {identity: [] for identity in ids}
    occurrences, aliases = set(), set()
    for match in matches:
        identity = text(match['occurrence_id'], 256)
        resources = match.get('resource_aliases', [match['resource_key']])
        if identity in occurrences or not isinstance(resources, list) or not resources or len(resources) > 10 or len(set(resources)) != len(resources):
            fail()
        if any(not isinstance(key, str) or not key or key in aliases for key in resources):
            fail()
        occurrences.add(identity); aliases.update(resources)
        if match['kind'] == 'EXPENSE' and match.get('budget_id') in by_budget:
            amount(match['amount_minor'], False)
            if match['status'] not in ('POSTED', 'SETTLED', 'HELD') or match['source'] not in ('UP', 'COMMBANK', 'LEGACY'):
                fail()
            by_budget[match['budget_id']].append(match)

    supplied = evidence.get('matches', [])
    if not isinstance(supplied, list) or len(supplied) > 1000:
        fail()
    support = {}
    for item in supplied:
        support.setdefault(item['occurrenceId'], []).append(item)
    proof = evidence.get('calculation', {})
    prior = report['up_spending'].get('prior_category_planning_allocations', [])
    if not isinstance(prior, list) or len(prior) > 100:
        fail()
    prior_totals, prior_seen = {}, set()
    for item in prior:
        allocation = item['allocation']; budget_id = allocation['budgetId']
        key = (allocation['source'], allocation['accountRole'], allocation['sourceId'], budget_id)
        if key in prior_seen or budget_id not in by_budget:
            fail()
        prior_seen.add(key)
        reduction = amount(item['planning_reduction_minor'], False)
        if item['state'] == 'ALREADY_IN_MATCHES' and reduction != 0:
            fail()
        if item['state'] not in ('ALREADY_IN_MATCHES', 'QUALIFIED_PLANNING_ONLY'):
            fail()
        prior_totals[budget_id] = prior_totals.get(budget_id, 0) + reduction

    items = []
    for index, row in enumerate(rows, 1):
        identity, source = row['budget_id'], row['source_row']
        private = identity in PRIVATE_LABEL_IDS
        label = f'Private budget item {index:02}' if private else text(row['name'])
        allocation = amount(row['target_minor'], False)
        if (allocation != amount(source['budgetAmountMinor'], False)
                or row['currency'] != 'AUD' or source.get('currency') != 'AUD'):
            fail()
        cadence = source.get('serviceCadence')
        if cadence is not None:
            cadence = text(cadence)
        role = source['paymentAccount']
        funding_label = ROLE_LABELS.get(role)
        funding = {'label': funding_label, 'state': 'ACCEPTED_PLAN' if funding_label else 'UNKNOWN',
                   'evidence_date': calendar(source.get('paymentEvidenceAsOf')) if funding_label else None}
        expense = by_budget[identity]
        posted = [m for m in expense if m['status'] in ('POSTED', 'SETTLED')]
        pending = [m for m in expense if m['status'] == 'HELD']
        scope, payer_labels, evidence_dates = set(), set(), []
        for match in expense:
            source_type = match['source']
            scope.add({'UP': 'UNRESOLVED', 'COMMBANK': 'COMMBANK_CREDIT', 'LEGACY': 'PRIOR_BANK'}[source_type])
            payer_labels.add({'UP': 'Up (specific account not established)',
                              'COMMBANK': 'Mastercard (dated evidence)', 'LEGACY': 'Prior bank (dated evidence)'}[source_type])
            for declared in support.get(match['occurrence_id'], []):
                if declared.get('source') == source_type:
                    when = declared.get('asOf') or declared.get('cycleDate')
                    if when:
                        evidence_dates.append(calendar(when))
        if payer_labels:
            payment = {'label': next(iter(payer_labels)) if len(payer_labels) == 1 else 'Multiple dated payment sources',
                       'state': 'DATED_OBSERVATION', 'evidence_date': max(evidence_dates) if evidence_dates else None}
        elif role in CARD_ROLES:
            payment = {'label': 'Mastercard (retained plan)', 'state': 'ACCEPTED_PLAN',
                       'evidence_date': calendar(source.get('paymentEvidenceAsOf'))}
        else:
            payment = {'label': None, 'state': 'UNKNOWN', 'evidence_date': None}
        prior_amount = prior_totals.get(identity)
        if prior_amount is not None:
            scope.add('PRIOR_BANK')
        reasons = []
        if pending:
            reasons.append('PENDING_SEPARATE')
        if prior_amount is not None:
            reasons.extend(('PRIOR_PLANNING_ONLY', 'HISTORICAL_EVIDENCE'))
        complete_claim = row['coverage'] == 'COMPLETE_EXPLICIT_ALL_BANK_ROW_EVIDENCE' and row['actual_minor'] is not None
        row_proof = proof.get('rowCoverage', {}).get(identity, {})
        through = timestamp(row_proof.get('asOf')) if complete_claim else None
        # Legacy actual_minor includes matched holds. Validate that full canonical
        # basis, then project POSTED actuals separately. A pending amount does not
        # itself invalidate independently complete posted activity.
        posted_total = sum(m['amount_minor'] for m in posted)
        pending_total = sum(m['amount_minor'] for m in pending)
        canonical_agrees = (row.get('known_committed_spend_minor') == posted_total + pending_total
            and row.get('known_held_minor') == pending_total
            and row['actual_minor'] == posted_total + pending_total)
        period_agrees = all(any(declared.get('source') == match['source']
            and declared.get('cycleDate') is not None
            and report['cycle']['start'] <= calendar(declared['cycleDate']) < report['cycle']['endExclusive']
            for declared in support.get(match['occurrence_id'], [])) for match in expense)
        complete = (complete_claim and row_proof.get('complete') is True and through is not None
            and datetime.fromisoformat(through.replace('Z', '+00:00')) <= datetime.fromisoformat(report['as_of'].replace('Z', '+00:00'))
            and report['cycle']['start'] <= calendar(through) < report['cycle']['endExclusive']
            and canonical_agrees and period_agrees
            and prior_amount in (None, 0))
        if complete_claim and not canonical_agrees:
            reasons.append('CANONICAL_MISMATCH')
        comparable = (source.get('budgetFrequency') == 'monthly / current-cycle allocation'
            and row['reserve_class'] not in ('provision_component_not_extra_expense',
                'zero_routine_allowance_incidental_from_discretionary', 'excluded')
            and source['bank'] != 'not_applicable' and identity not in ('salon', 'brows'))
        if complete_claim and (not comparable or not period_agrees):
            reasons.append('INCOMPARABLE_BASIS')
        known = bool(expense) or prior_amount is not None
        coverage = 'COMPLETE_VERIFIED' if complete else ('PARTIAL_KNOWN' if known else 'UNKNOWN')
        if not complete:
            reasons.append('PARTIAL_MATCH_SET' if known else 'NO_VERIFIED_MATCHES')
        contribution = row.get('contribution_state') == 'OWNER_DEFERRED_THIS_CYCLE_ONLY'
        qualifiers = ['FUNDING_NOT_PAYER']
        if private:
            qualifiers.append('PRIVACY_ALIAS')
        if contribution:
            qualifiers.append('CONTRIBUTION_DEFERRED')
        if identity in ('salon', 'brows'):
            qualifiers.append('GROUP_RESERVE_NOT_ALLOCATED')
        next_date = row.get('next_accepted_occurrence') or source.get('nextScheduled')
        item = {'ref': f'budget_{index:03}', 'label': label,
            'label_state': 'PRIVACY_ALIAS' if private else 'APPROVED_DISPLAY',
            'allocation': {'amount_minor': allocation, 'currency': 'AUD',
                'basis': 'MONTHLY_CURRENT_CYCLE_ALLOCATION', 'service_cadence': cadence},
            'funding': funding, 'payment': payment,
            'future_up_merchant_switch': 'NOT_APPLICABLE' if source['bank'] == 'not_applicable' else 'NOT_ESTABLISHED',
            'next_planned_date': calendar(next_date),
            'spend_to_date': {'known_posted_minor': amount(sum(m['amount_minor'] for m in posted), False) if posted else (0 if complete else None),
                'known_pending_minor': amount(sum(m['amount_minor'] for m in pending), False) if pending else None,
                'qualified_prior_planning_minor': amount(prior_amount),
                'complete_actual_minor': amount(posted_total, False) if complete else None,
                # Signed display position bypasses legacy max(0, ...) remaining.
                # It is not debt, available cash or a second cash deduction.
                'complete_remaining_minor': position(allocation - posted_total) if complete and comparable else None,
                'window': {'start': report['cycle']['start'], 'end_exclusive': report['cycle']['endExclusive'],
                           'covered_through_at': through if complete else None},
                'source_scope': sorted(scope or {'UNRESOLVED'}), 'coverage': coverage, 'reason_codes': sorted(set(reasons))},
            'new_contribution': {'required_minor': amount(row.get('current_cycle_contribution_required_minor')) if contribution else None,
                                 'state': 'DEFERRED_THIS_CYCLE' if contribution else ('UNKNOWN' if row['reserve_class'] == 'provision_component_not_extra_expense' else 'NOT_APPLICABLE')},
            'accepted_protection_minor': amount(row.get('approved_remaining_reserve_minor')),
            'qualification_codes': qualifiers}
        items.append(item)
    total = amount(sum(item['allocation']['amount_minor'] for item in items), False)
    if total != report['allocation_total_minor']:
        fail()
    supplemental = []
    labels = {'1password-current-invoice': 'Supplemental annual subscription',
              'mastercard-minimum': 'Dated card payment-due protection'}
    for component in report['up_spending']['planning_components']:
        if component['id'] in labels:
            supplemental.append({'ref': f'supplement_{len(supplemental)+1:03}', 'label': labels[component['id']],
                'label_state': 'APPROVED_DISPLAY', 'planning_amount_minor': amount(component.get('current_reserve_minor')),
                'currency': 'AUD', 'included_in_allocation_baseline': False,
                'coverage': 'PARTIAL_KNOWN', 'qualification_codes': ['HISTORICAL_EVIDENCE']})
    result = {'schema': 'nico.finance-budget/v1',
        'report': {**meta['report'], 'rules_ref': RULE_HASH, 'selection_state': selection_state},
        'source_dates': {'up_observed_at': meta['up_cash']['observed_at'],
                         'card_observed_at': meta['card_credit']['observed_at']},
        'allocation_baseline_minor': total, 'allocation_is_cash_requirement': False,
        'item_coverage': {'expected_count': EXPECTED_ITEMS, 'returned_count': len(items),
                          'state': 'COMPLETE_LIST_NOT_COMPLETE_SPENDING'},
        'items': items, 'supplemental_items': supplemental,
        'limitations': ['ALLOCATION_NOT_CASH_DUE', 'PARTIAL_SPENDING_NOT_COMPLETE_ACTUALS',
            'FUNDING_NOT_PAYMENT', 'NO_BROWSER_FINANCIAL_CALCULATION', 'SOURCE_DATES_NOT_GENERATION_TIME',
            'PRIVACY_ALIASES_PRESENT', 'CURRENT_CYCLE_ONLY', 'SUPPLEMENTALS_NOT_ADDITIVE_TO_BASELINE']}
    if len(json.dumps(result, allow_nan=False).encode()) > MAX_RESPONSE:
        fail()
    return result


def read_budget(root, require_mount, report_id):
    report, evidence, selected, selection_state = read_selected_record(root, require_mount, report_id, include_evidence=True)
    return build_budget(report, evidence, selected, selection_state, datetime.now(timezone.utc).isoformat())
