"""Private, evidence-admitted card planning; no network, datastore or bank mutation.

The caller must supply reviewed private evidence. Validation checks its structure,
not provider provenance. No recurrence, reflection or settlement is inferred here.
Numbers under hypothetical are arithmetic over enumerated assumptions, never a
lower bound or an assured spending recommendation.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re

LIMIT = 2**53 - 1
MAX_BYTES = 128 * 1024


class InvalidEvidence(ValueError):
    pass


def require(condition):
    if not condition:
        raise InvalidEvidence('INVALID_CARD_PLANNING_EVIDENCE')


def exact(value, keys):
    require(type(value) is dict and set(value) == set(keys.split()))


def integer(value, *, signed=False):
    require(type(value) is int and (-LIMIT if signed else 0) <= value <= LIMIT)
    return value


def instant(value, *, nullable=False):
    if value is None and nullable:
        return None
    require(type(value) is str and len(value) <= 40)
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
        require(result.tzinfo is not None and result.utcoffset() is not None)
        return result.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        raise InvalidEvidence('INVALID_CARD_PLANNING_TIME') from None


def reference(value):
    require(type(value) is str and re.fullmatch(r'[a-z][a-z0-9-]{0,63}', value))
    return value


def refs(value):
    require(type(value) is list and 0 < len(value) <= 16)
    require(all(type(x) is str and 0 < len(x) <= 256 for x in value))


def build(evidence, generated_at):
    exact(evidence, 'schema position horizon charges hypothetical')
    require(evidence['schema'] == 'nico.card-planning-evidence/v1')
    now = instant(generated_at)
    p = evidence['position']
    exact(p, 'currency available_minor pending_minor balance_minor statement_owing_minor statement_due_date uploaded_at captured_at provenance')
    require(p['currency'] == 'AUD')
    integer(p['available_minor']); integer(p['pending_minor'])
    integer(p['balance_minor'], signed=True)
    if p['statement_owing_minor'] is not None:
        integer(p['statement_owing_minor'])
    require(p['statement_due_date'] is None or (
        type(p['statement_due_date']) is str and
        re.fullmatch(r'\d{4}-\d{2}-\d{2}', p['statement_due_date'])))
    if p['statement_due_date']:
        from datetime import date
        try:
            date.fromisoformat(p['statement_due_date'])
        except ValueError:
            raise InvalidEvidence('INVALID_STATEMENT_DATE') from None
    uploaded = instant(p['uploaded_at'])
    captured = instant(p['captured_at'], nullable=True)
    require(uploaded <= now and (captured is None or captured <= uploaded))
    refs(p['provenance'])
    h = evidence['horizon']
    exact(h, 'planned_at funding_verified provenance')
    planned = instant(h['planned_at'])
    require(type(h['funding_verified']) is bool and uploaded < planned)
    refs(h['provenance'])
    charges = evidence['charges']
    require(type(charges) is list and len(charges) <= 64)
    by_id = {}
    supported = []
    unresolved = []
    for c in charges:
        exact(c, 'id amount_minor due_at route reflection reflection_evidence provenance')
        key = reference(c['id']); require(key not in by_id)
        if c['amount_minor'] is not None:
            integer(c['amount_minor'])
        due = instant(c['due_at'], nullable=True)
        require(c['route'] in ('CARD', 'OTHER', 'UNKNOWN'))
        require(c['reflection'] in ('REFLECTED', 'UNREFLECTED', 'UNKNOWN'))
        refs(c['provenance'])
        require(type(c['reflection_evidence']) is list)
        if c['reflection'] != 'UNKNOWN':
            refs(c['reflection_evidence'])
            require(captured is not None)
        else:
            require(c['reflection_evidence'] == [])
        # These are already admitted observation-specific claims, not a timestamp
        # shortcut or automatic match of a CSV row to a screenshot.
        by_id[key] = c
        if (c['route'] == 'CARD' and c['reflection'] == 'UNREFLECTED'
                and c['amount_minor'] is not None and due is not None
                and captured <= due < planned):
            supported.append({'id': key, 'amount_minor': c['amount_minor'],
                              'due_at': c['due_at']})
        if (c['route'] == 'UNKNOWN' or c['reflection'] == 'UNKNOWN'
                or c['amount_minor'] is None or due is None
                or (c['route'] == 'CARD' and c['reflection'] == 'UNREFLECTED'
                    and captured is not None and due < captured)):
            unresolved.append(key)
    integer(sum(x['amount_minor'] for x in supported))
    result = {'schema': 'nico.card-planning-report/v1', 'generated_at': generated_at,
        'currency': 'AUD', 'credit_is_cash': False,
        'observation': {k: p[k] for k in ('available_minor', 'pending_minor', 'balance_minor',
            'statement_owing_minor', 'statement_due_date', 'uploaded_at', 'captured_at')},
        'horizon': {'planned_at': h['planned_at'], 'funding_verified': h['funding_verified']},
        'supported_protections': supported, 'unresolved_occurrences': unresolved,
        'conditional_credit_minor': None, 'assured_credit_minor': None,
        'minimum_payment_minor': None, 'repayment_funding_minor': None,
        'hypothetical': None,
        'limitations': ['OBSERVATION_NOT_REAL_TIME', 'CANDIDATE_SET_NOT_PROVEN_COMPLETE',
            'NO_ASSURED_OR_LOWER_BOUND_CAPACITY', 'REPAYMENT_FUNDING_NOT_ESTABLISHED',
            'OBSERVED_PENDING_NOT_REDEDUCTED']}
    if captured is None:
        result['limitations'].append('UPLOAD_TIME_NOT_BANK_CAPTURE_TIME')
    if not h['funding_verified']:
        result['limitations'].append('PLANNED_BOUNDARY_NOT_VERIFIED_FUNDING')
    if now >= planned:
        result['limitations'].append('PLANNED_HORIZON_EXPIRED')
        return result
    scenario = evidence['hypothetical']
    if scenario is None:
        return result
    exact(scenario, 'deduct assume_reflected assume_other_route assume_after_horizon')
    assigned = {}
    for action, values in scenario.items():
        require(type(values) is list and len(values) <= 64)
        for key in values:
            reference(key); require(key in by_id and key not in assigned)
            assigned[key] = action
    require(set(assigned) == set(by_id))
    deductions = []
    for key, action in assigned.items():
        c = by_id[key]
        # A hypothetical must not contradict an already admitted fact.
        if c['reflection'] == 'REFLECTED':
            require(action == 'assume_reflected')
        elif c['route'] == 'OTHER':
            require(action == 'assume_other_route')
        elif any(x['id'] == key for x in supported):
            require(action == 'deduct')
        require(not (c['reflection'] == 'UNREFLECTED' and action == 'assume_reflected'))
        require(not (c['route'] == 'CARD' and action == 'assume_other_route'))
        due = instant(c['due_at'], nullable=True)
        require(not (due is not None and due < planned and action == 'assume_after_horizon'))
        if action == 'deduct':
            require(c['amount_minor'] is not None)
            deductions.append({'id': key, 'amount_minor': c['amount_minor']})
    total = integer(sum(x['amount_minor'] for x in deductions))
    amount = integer(p['available_minor'] - total, signed=True)
    result['hypothetical'] = {'kind': 'BOUNDED_HYPOTHETICAL_NOT_SPENDING_CAPACITY',
        'amount_minor': amount, 'deductions_minor': total, 'deductions': deductions,
        'occurrence_assumptions': scenario,
        'assumptions': ['SUPPLIED_AVAILABLE_CREDIT_USED_AS_SCENARIO_START',
            'NO_OTHER_CHARGES_OR_FEES_BEFORE_PLANNED_BOUNDARY',
            'NO_UNOBSERVED_ACTIVITY_AFTER_POSITION',
            'ENUMERATED_REFLECTION_ROUTE_AND_TIMING_ASSUMPTIONS_HOLD',
            'PLANNED_BOUNDARY_USED_WITHOUT_GUARANTEED_SALARY_ARRIVAL'],
        'assured': False, 'lower_bound': False}
    return result


def pairs(items):
    result = {}
    for key, value in items:
        require(key not in result); result[key] = value
    return result


def main():
    parser = argparse.ArgumentParser(description='Private hypothetical card report; no financial writes')
    parser.add_argument('--evidence-json', required=True, type=Path)
    parser.add_argument('--generated-at', required=True)
    args = parser.parse_args()
    try:
        with args.evidence_json.open('rb') as source:
            raw = source.read(MAX_BYTES + 1)
        require(0 < len(raw) <= MAX_BYTES)
        evidence = json.loads(raw, object_pairs_hook=pairs,
            parse_constant=lambda _: require(False))
        print(json.dumps(build(evidence, args.generated_at), sort_keys=True, allow_nan=False))
    except (ValueError, TypeError, KeyError, OSError, UnicodeError):
        parser.exit(2, 'CARD_REPORT_UNAVAILABLE\n')


if __name__ == '__main__':
    main()
