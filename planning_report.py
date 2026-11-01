"""Qualified planning views only; never posts transactions or moves funds."""
from datetime import date as calendar_date

from budget_rules import EvidenceError, references
from finance_worker import date, stamp


def dated(item, report, through):
    references(item.get('provenance'))
    if not through <= stamp(date(item['asOf'])) <= report['as_of']:
        raise EvidenceError('Planning evidence must be dated within observation/report bounds')


def prior_allocations(proof, report, through):
    bundle = proof.get('priorPlanningAllocations')
    if bundle is None:
        return {}, []
    if (not isinstance(bundle, dict) or set(bundle) != {'cycle', 'asOf', 'provenance', 'allocations'}
            or bundle['cycle'] != {k: report['cycle'][k] for k in ('start', 'endExclusive')}
            or not isinstance(bundle['allocations'], list) or len(bundle['allocations']) > 100):
        raise EvidenceError('Invalid cycle-specific prior planning allocations')
    dated(bundle, report, through)
    seen, totals, retained = {}, {}, []
    for item in bundle['allocations']:
        key = (item.get('source'), item.get('accountRole'), item.get('sourceId'), item.get('budgetId'))
        amount, whole = item.get('allocatedAmountMinor'), item.get('wholeTransactionAmountMinor')
        if (item.get('source') != 'FROLLO' or not all(isinstance(x, str) and x for x in key)
                or key[3] not in ('groceries', 'charging', 'ptv') or item.get('currency') != 'AUD'
                or type(amount) is not int or not 0 < amount < 2**63
                or (whole is not None and (type(whole) is not int or whole < amount))
                or not isinstance(item.get('provenance'), list) or not item['provenance']):
            raise EvidenceError('Prior planning allocation identity/amount/provenance invalid')
        when = item.get('transactionDate')
        if when is not None and not report['cycle']['start'] <= calendar_date.fromisoformat(when).isoformat() < report['cycle']['endExclusive']:
            raise EvidenceError('Prior planning date outside accepted cycle')
        if key in seen:
            if seen[key] != item:
                raise EvidenceError('Conflicting duplicate prior allocation')
            continue
        seen[key] = item
        # Frollo observation identities remain distinct from canonical occurrences.
        # An existing explicit manual match for the same provider observation must
        # not also reduce planning. Conflicting/split overlap requires reconciliation.
        represented = [m for m in report['matched_occurrences']
            if m.get('source') in ('COMMBANK', 'LEGACY') and any(
                alias.rsplit(':', 1)[-1] in (key[2], 'frollo-' + key[2])
                for alias in m.get('resource_aliases', [m['resource_key']]))]
        if represented and any(m['budget_id'] != key[3] or m['amount_minor'] != amount
                or m['kind'] != 'EXPENSE' or m['status'] not in ('POSTED', 'SETTLED') for m in represented):
            raise EvidenceError('Existing match conflicts with prior planning allocation')
        applied = 0 if represented else amount
        totals['variable:' + key[3]] = totals.get('variable:' + key[3], 0) + applied
        retained.append({'allocation': item, 'planning_reduction_minor': applied,
                         'state': 'ALREADY_IN_MATCHES' if represented else 'QUALIFIED_PLANNING_ONLY'})
    return totals, retained


def finish(proof, document, report, through, retained):
    spending = report['up_spending']
    spending['conditional_cash_room_basis'] = 'FULL_BUDGET_PLAN_SCENARIO_NOT_COMMITTED_DEBT'
    spending['full_budget_plan_cash_room_minor'] = spending['conditional_cash_room_minor']
    spending['prior_category_planning_allocations'] = retained
    if retained:
        spending['qualifications'].append('PRIOR_CATEGORY_ALLOCATIONS_PARTIAL_PLANNING_ONLY')
    flexible = {'variable:groceries', 'variable:pet-supplies', 'variable:ptv',
                'variable:charging', 'variable:other-irregular', 'discretionary'}
    values = {x['id']: x['amount_minor'] for x in spending['deductions']}
    flexible_values = [v for k, v in values.items() if k in flexible]
    spending['remaining_flexible_plan_minor'] = (sum(flexible_values)
        if all(type(x) is int for x in flexible_values) else None)
    spending['booked_commitment_planning_minor'] = values.get('variable:personal-care')
    spending['cash_after_committed_obligations_minor'] = None
    spending['cash_after_commitments_assured'] = False
    spending['cash_after_committed_obligations_scenario'] = None
    plan = next(x for x in document['supplementalProtections'] if x['id'] == 'personal-care-occurrences')
    pending = proof.get('pendingCommitmentObservations', {})
    if not isinstance(pending, dict):
        raise EvidenceError('Pending commitment observations must be keyed by accepted occurrence')
    allowed = {'personal-care:' + x['date']: x for x in plan['occurrences']}
    if set(pending) - set(allowed):
        raise EvidenceError('Unknown pending commitment occurrence')
    unresolved, booked, future_cost = [], [], 0
    today = report['as_of'][:10]
    for identity, occurrence in allowed.items():
        known = [x for x in report['matched_occurrences'] if x['occurrence_id'] == identity and x['kind'] == 'EXPENSE']
        item = {'occurrence_id': identity, 'date': occurrence['date'],
                'planning_estimate_minor': occurrence['amountMinor'], 'provider_quote_verified': False}
        if known:
            if identity in pending:
                raise EvidenceError('Pending association conflicts with explicit occurrence match')
            item['state'] = 'EXPLICIT_MATCH_RETAINED'
        elif identity in pending:
            observed = pending[identity]
            if (not isinstance(observed, dict) or set(observed) != {'source', 'sourceId', 'status', 'amountMinor', 'currency', 'asOf', 'provenance'}
                    or observed['source'] != 'COMMBANK' or not isinstance(observed['sourceId'], str) or not observed['sourceId']
                    or observed['status'] != 'HELD' or observed['currency'] != 'AUD'
                    or type(observed['amountMinor']) is not int or observed['amountMinor'] <= 0
                    or occurrence['date'] >= today):
                raise EvidenceError('Only dated past pending-card associations supported')
            dated(observed, report, through)
            item.update(state='ASSOCIATED_PENDING_CARD_NOT_SETTLEMENT', pending_card_observation=observed)
            unresolved.append('PAST_PENDING_CARD_PAYMENT_REFLECTION_UNRESOLVED:' + identity)
        elif occurrence['date'] < today:
            item['state'] = 'PAST_PLAN_PAYMENT_UNRESOLVED'
            unresolved.append('PAST_COMMITMENT_PAYMENT_UNKNOWN:' + identity)
        else:
            item['state'] = 'FUTURE_BOOKED_ESTIMATE_NOT_QUOTE'
            future_cost += occurrence['amountMinor']
        booked.append(item)
    spending['booked_commitment_occurrences'] = booked
    spending['committed_obligation_unresolved_reasons'] = list(spending['arithmetic_blockers']) + unresolved
    other = [v for k, v in values.items() if k not in flexible | {'variable:personal-care'}]
    if (not spending['arithmetic_blockers'] and all(type(v) is int and v >= 0 for v in other)
            and not any(x.startswith('PAST_COMMITMENT_PAYMENT_UNKNOWN:') for x in unresolved)
            and spending['observation'] is not None):
        amount = spending['observation']['available_after_holds_minor'] - future_cost - sum(other)
        if not unresolved:
            spending['cash_after_committed_obligations_minor'] = amount
        else:
            spending['cash_after_committed_obligations_scenario'] = {
                'amount_minor': amount, 'future_booked_estimate_minor': future_cost,
                'other_known_commitments_minor': sum(other), 'balance_as_of': spending['observation']['as_of'],
                'basis': 'ASSUMPTION_BASED_NOT_ASSURED_NOT_SETTLEMENT',
                'assumptions': ['ASSOCIATED_PENDING_CARD_OBSERVATION_REQUIRES_NO_ADDITIONAL_UP_CASH_IN_THIS_CYCLE',
                    'NO_OTHER_CURRENT_CASH_LIABILITY_BEYOND_SUPPORTED_EXISTING_GATES',
                    'ACCEPTED_FUTURE_BOOKING_ESTIMATES_AND_UP_ROUTE_APPLY',
                    'NO_UNOBSERVED_ACTIVITY_SINCE_BALANCE_OBSERVATION'],
                'pending_card_liabilities_remain_separate': True}
    return report
