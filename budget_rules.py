"""Private, deterministic accepted-budget import/report; no network or public route."""

import argparse
from datetime import date as calendar_date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
from uuid import UUID

from finance_worker import date, stamp, transaction

SOURCE_HASH = "62e8a58e185d192ef757b810f200b9da5b14d7c8d7a4690a7c6cc8030e97e878"
LIBRARY_ID = "libfile_4653403114f48191ab38c31d80693861"
CYCLE = {"start": "2026-10-14", "endInclusive": "2026-11-14", "endExclusive": "2026-11-15"}
VARIABLE_GROUPS = {"groceries": ("groceries",), "pet-supplies": ("pet-supplies",),
                   "personal-care": ("salon", "brows"), "ptv": ("ptv",),
                   "charging": ("charging",), "other-irregular": ("other-irregular",)}


class EvidenceError(ValueError):
    pass


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def references(value):
    if not isinstance(value, list) or not value or not all(isinstance(v, str) and v.strip() for v in value):
        raise EvidenceError("Explicit provenance references required")
    return value


def minor(value):
    if type(value) is not int or not 0 <= value < 2**63:
        raise EvidenceError("Expected non-negative integer minor units")
    return value


def contract(raw):
    if len(raw) != 48248 or hashlib.sha256(raw).hexdigest() != SOURCE_HASH:
        raise EvidenceError("Rule map does not match the approved Library version/hash")
    result = json.loads(raw.decode("utf-8"))
    rows = result["budgetRows"]
    if (result["cycle"] != CYCLE or len(rows) != 31 or len({r["id"] for r in rows}) != 31
            or sum(minor(r["budgetAmountMinor"]) for r in rows) != 667091
            or any(r["currency"] != "AUD" for r in rows)):
        raise EvidenceError("Approved budget contract invariant failed")
    return result


def schema(connection):
    # Additive component schema: leave the accepted worker's user_version=2 intact.
    if connection.execute("PRAGMA user_version").fetchone()[0] != 2:
        raise EvidenceError("Accepted core schema 2 required; no automatic core migration")
    connection.execute("CREATE TABLE IF NOT EXISTS budget_component_version (singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL)")
    row = connection.execute("SELECT version FROM budget_component_version WHERE singleton=1").fetchone()
    if row and row[0] != 1:
        raise EvidenceError("Unsupported budget component version")
    connection.execute("INSERT OR IGNORE INTO budget_component_version VALUES (1,1)")
    connection.execute("""CREATE TABLE IF NOT EXISTS budget_sources (
        source_hash TEXT PRIMARY KEY,library_id TEXT NOT NULL,library_version INTEGER NOT NULL,
        imported_at TEXT NOT NULL,raw_json BLOB NOT NULL)""")
    connection.execute("""CREATE TABLE IF NOT EXISTS budget_rows (
        source_hash TEXT NOT NULL REFERENCES budget_sources(source_hash),budget_id TEXT NOT NULL,
        name TEXT NOT NULL,target_minor INTEGER NOT NULL,currency TEXT NOT NULL,
        bank TEXT NOT NULL,payment_account TEXT NOT NULL,reserve_class TEXT NOT NULL,
        remaining_reserve_minor INTEGER,cycle_payment_state TEXT NOT NULL,source_json TEXT NOT NULL,
        PRIMARY KEY(source_hash,budget_id))""")
    connection.execute("""CREATE TABLE IF NOT EXISTS budget_occurrences (
        source_hash TEXT NOT NULL REFERENCES budget_sources(source_hash),occurrence_id TEXT NOT NULL,
        budget_id TEXT,kind TEXT NOT NULL,amount_minor INTEGER,currency TEXT,scheduled TEXT,
        state TEXT NOT NULL,source_json TEXT NOT NULL,PRIMARY KEY(source_hash,occurrence_id))""")
    connection.execute("""CREATE TABLE IF NOT EXISTS budget_rule_facts (
        source_hash TEXT NOT NULL REFERENCES budget_sources(source_hash),rule_id TEXT NOT NULL,
        rule_text TEXT NOT NULL,PRIMARY KEY(source_hash,rule_id))""")
    connection.execute("""CREATE TABLE IF NOT EXISTS budget_reports (
        report_id TEXT PRIMARY KEY,source_hash TEXT NOT NULL REFERENCES budget_sources(source_hash),
        as_of TEXT NOT NULL,evidence_json TEXT NOT NULL,report_json TEXT NOT NULL)""")


def occurrences(document):
    result = []
    for row in document["budgetRows"]:
        if row["cyclePaymentState"] == "CURRENT_CYCLE_POSTED_OLD_BANK; next UP occurrence outside cycle":
            source = document["evidence"]["rent" if row["id"] == "rent" else "pepper"]["oldPaymentEvidence"]
            identity = re.search(r"POSTED ([^ ,;]+)", source)
            amount = re.search(r",(\d+)c,", source)
            if not identity or not amount:
                raise EvidenceError("Accepted old-bank occurrence identity missing")
            result.append({"id": "cycle:" + CYCLE["start"] + ":" + row["id"], "budgetId": row["id"],
                           "kind": "OLD_BANK_PAID", "amountMinor": int(amount.group(1)), "currency": "AUD",
                           "transactionId": identity.group(1), "state": "SATISFIED_OLD_BANK",
                           "sourceEvidence": source, "evidence": row["evidence"]})
        if row.get("nextScheduled"):
            result.append({"id": "scheduled:" + row["id"] + ":" + row["nextScheduled"],
                           "budgetId": row["id"], "kind": "SCHEDULE", "amountMinor": row["budgetAmountMinor"],
                           "currency": "AUD", "scheduled": row["nextScheduled"],
                           "state": "SCHEDULE_NOT_EXECUTION_PROOF", "sourceRow": row})
    for item in document["supplementalProtections"]:
        result.append({"id": "supplemental:" + item["id"], "kind": "SUPPLEMENTAL",
                       "state": item["state"], "source": item,
                       "amountMinor": item.get("nativeAmountMinor"), "currency": item.get("nativeCurrency")})
        if item["id"] == "personal-care-occurrences":
            for planned in item["occurrences"]:
                result.append({"id": "personal-care:" + planned["date"], "budgetId": "salon", "kind": "PLANNED_PERSONAL_CARE",
                               "amountMinor": planned["amountMinor"], "currency": "AUD", "scheduled": planned["date"],
                               "state": "PAYMENT_UNKNOWN_NOT_ZERO", "evidence": item["evidence"]})
    return result


def import_rules(factory, raw, imported_at):
    document = contract(raw)
    with transaction(factory) as connection:
        schema(connection)
        old = connection.execute("SELECT raw_json FROM budget_sources WHERE source_hash=?", (SOURCE_HASH,)).fetchone()
        if old:
            if old[0] != raw:
                raise EvidenceError("Retained source conflicts with approved bytes")
            return {"source_hash": SOURCE_HASH, "budget_rows": 31, "already_imported": True}
        connection.execute("INSERT INTO budget_sources VALUES (?,?,?,?,?)", (SOURCE_HASH, LIBRARY_ID, 1, stamp(imported_at), raw))
        for row in document["budgetRows"]:
            connection.execute("INSERT INTO budget_rows VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (SOURCE_HASH, row["id"], row["name"], row["budgetAmountMinor"], row["currency"], row["bank"],
                 row["paymentAccount"], row["reserveClass"], row.get("remainingReserveMinor"), row["cyclePaymentState"], canonical(row)))
        for item in occurrences(document):
            connection.execute("INSERT INTO budget_occurrences VALUES (?,?,?,?,?,?,?,?,?)",
                (SOURCE_HASH, item["id"], item.get("budgetId"), item["kind"], item.get("amountMinor"),
                 item.get("currency"), item.get("scheduled"), item["state"], canonical(item)))
        for index, rule in enumerate(document["reusedCalculationRules"], 1):
            connection.execute("INSERT INTO budget_rule_facts VALUES (?,?,?)", (SOURCE_HASH, "accepted-rule:" + str(index), rule))
    return {"source_hash": SOURCE_HASH, "budget_rows": 31, "already_imported": False}


def observation(connection, account_id, as_of):
    row = connection.execute("""SELECT b.* FROM balance_snapshots b JOIN reconciliation_runs r USING(run_id)
        WHERE b.account_id=? AND b.observed_at<=? AND r.status IN ('OBSERVED','PARTIAL')
        ORDER BY b.observed_at DESC,b.run_id DESC LIMIT 1""", (account_id, as_of)).fetchone()
    return ({"account_id": account_id, "available_after_holds_minor": row["balance_base_units"],
             "currency": row["currency_code"], "as_of": row["observed_at"], "coverage": row["coverage"],
             "source": "existing_render_balance_snapshot", "currentness": "OBSERVATION_DATE_ONLY"} if row else None)


def bound_observations(connection, document, evidence, as_of):
    roles = {r["paymentAccount"] for r in document["budgetRows"] if r["bank"] == "UP"}
    roles.update(("up.spending", "up.essentials"))
    result = {}
    used = set()
    for role, binding in evidence.get("bindings", {}).items():
        if role not in roles or not isinstance(binding, dict):
            raise EvidenceError("Unrecognised account-role binding")
        account_id = binding.get("accountId")
        try:
            valid = isinstance(account_id, str) and str(UUID(account_id)) == account_id
        except (ValueError, TypeError):
            valid = False
        if not valid or account_id in used:
            raise EvidenceError("Invalid or duplicated account-role identity")
        references(binding.get("provenance"))
        if stamp(date(binding["verifiedAt"])) > as_of:
            raise EvidenceError("Future account binding cannot establish this report")
        value = observation(connection, account_id, as_of)
        if value is None or value["currency"] != "AUD":
            raise EvidenceError("Binding lacks an existing AUD observation")
        result[role] = {**value, "binding_provenance": binding["provenance"]}
        used.add(account_id)
    return result


def matched_occurrences(connection, document, evidence, as_of):
    budget_ids = {r["id"] for r in document["budgetRows"]}
    candidates = []
    for item in occurrences(document):
        if item["kind"] == "OLD_BANK_PAID":
            candidates.append({"occurrence_id": item["id"], "budget_id": item["budgetId"], "kind": "EXPENSE",
                "resource_key": "LEGACY:" + item["transactionId"], "amount_minor": item["amountMinor"],
                "status": "POSTED", "source": "LEGACY", "provenance": item["evidence"]})
    supplied = evidence.get("matches", [])
    if not isinstance(supplied, list) or len(supplied) > 1000:
        raise EvidenceError("At most 1000 explicit verified matches supported")
    unresolved = []
    for match in supplied:
        references(match.get("provenance"))
        budget_id = match.get("budgetId")
        if budget_id is not None and budget_id not in budget_ids:
            raise EvidenceError("Unknown budget identity in verified match")
        occurrence_id = match.get("occurrenceId")
        if not isinstance(occurrence_id, str) or not occurrence_id.strip():
            raise EvidenceError("Canonical occurrence identity required")
        cycle_date = calendar_date.fromisoformat(match["cycleDate"]).isoformat()
        if not CYCLE["start"] <= cycle_date < CYCLE["endExclusive"]:
            unresolved.append({"occurrence_id": occurrence_id, "reason": "OUTSIDE_ACCEPTED_CYCLE"})
            continue
        source = match.get("source")
        kind = match.get("kind")
        identity = match.get("transactionId")
        if source not in ("UP", "COMMBANK", "LEGACY") or kind not in ("EXPENSE", "INTERNAL_TRANSFER", "NON_BUDGET_CREDIT") or not isinstance(identity, str) or not identity:
            raise EvidenceError("Invalid explicit match source/type/identity")
        if kind == "NON_BUDGET_CREDIT" and (source != "UP" or budget_id is not None):
            raise EvidenceError("Nonbudget credit requires an explicit unbudgeted Up identity")
        if source == "UP":
            row = connection.execute("SELECT * FROM transactions WHERE transaction_id=?", (identity,)).fetchone()
            if not row or row["is_deleted"] or row["ordering_conflict"] or stamp(date(row["last_observed_at"])) > as_of:
                unresolved.append({"occurrence_id": occurrence_id, "reason": "UP_RECORD_MISSING_DELETED_CONFLICTED_OR_FUTURE"})
                continue
            if (row["currency_code"] != "AUD" or row["amount_base_units"] is None
                    or (row["amount_base_units"] > 0 and kind not in ("INTERNAL_TRANSFER", "NON_BUDGET_CREDIT"))
                    or (kind == "NON_BUDGET_CREDIT" and row["amount_base_units"] <= 0)):
                unresolved.append({"occurrence_id": occurrence_id, "reason": "UNRESOLVED_CURRENCY_OR_CREDIT"})
                continue
            amount, status = abs(row["amount_base_units"]), row["up_status"]
        else:
            if stamp(date(match["asOf"])) > as_of or match.get("currency") != "AUD" or match.get("status") not in ("POSTED", "SETTLED", "HELD"):
                raise EvidenceError("Dated AUD manual/legacy evidence required")
            amount, status = minor(match["amountMinor"]), match["status"]
        candidates.append({"occurrence_id": occurrence_id, "budget_id": budget_id, "kind": kind,
            "resource_key": source + ":" + identity, "amount_minor": amount, "status": status,
            "source": source, "provenance": match["provenance"]})
    resources = {}
    groups = {}
    for item in candidates:
        previous = resources.get(item["resource_key"])
        if previous and (previous["occurrence_id"], previous["budget_id"], previous["kind"]) != (item["occurrence_id"], item["budget_id"], item["kind"]):
            raise EvidenceError("One transaction cannot be assigned to conflicting occurrences/budgets")
        if previous:
            if (previous["amount_minor"], previous["status"]) != (item["amount_minor"], item["status"]):
                raise EvidenceError("Conflicting evidence for the same transaction")
            item = {**item, "provenance": sorted(set(previous["provenance"] + item["provenance"]))}
        resources[item["resource_key"]] = item
        groups.setdefault(item["occurrence_id"], {})[item["resource_key"]] = item
    chosen = []
    for occurrence_id, group in groups.items():
        items = list(group.values())
        if len({(x["budget_id"], x["kind"]) for x in items}) != 1:
            raise EvidenceError("Canonical occurrence has conflicting budget/type")
        final = [x for x in items if x["status"] in ("POSTED", "SETTLED")]
        authoritative = final or items
        if len({x["amount_minor"] for x in authoritative}) != 1:
            unresolved.append({"occurrence_id": occurrence_id, "reason": "CONFLICTING_OCCURRENCE_AMOUNTS"})
            continue
        selected = dict(sorted(authoritative, key=lambda x: x["resource_key"])[0])
        selected["resource_aliases"] = sorted(group)
        chosen.append(selected)
    return sorted(chosen, key=lambda x: x["occurrence_id"]), unresolved


def calculate(connection, document, evidence, as_of):
    if not isinstance(evidence, dict) or set(evidence) - {"schema", "bindings", "matches", "cardObservation", "cardPlanning", "calculation"}:
        raise EvidenceError("Unrecognised evidence input fields")
    if evidence.get("schema", "nico-budget-evidence/v1") != "nico-budget-evidence/v1":
        raise EvidenceError("Unsupported evidence contract")
    bound = bound_observations(connection, document, evidence, as_of)
    matches, unresolved = matched_occurrences(connection, document, evidence, as_of)
    rows = {r["id"]: r for r in document["budgetRows"]}
    known = {budget_id: 0 for budget_id in rows}
    held = {budget_id: 0 for budget_id in rows}
    counts = {budget_id: 0 for budget_id in rows}
    for item in matches:
        if item["budget_id"] and item["kind"] == "EXPENSE":
            known[item["budget_id"]] += item["amount_minor"]
            held[item["budget_id"]] += item["amount_minor"] if item["status"] == "HELD" else 0
            counts[item["budget_id"]] += 1
    supplemental = {x["id"]: x for x in document["supplementalProtections"]}
    components = []
    for group, ids in VARIABLE_GROUPS.items():
        target = sum(rows[key]["budgetAmountMinor"] for key in ids)
        spent = sum(known[key] for key in ids)
        residual = max(0, target - spent)
        item = {"id": "variable:" + group, "account_role": "up.spending", "target_minor": target,
                "known_committed_spend_minor": spent, "known_only_positive_residual_minor": residual,
                "conditional_planning_protection_minor": residual, "current_reserve_minor": None,
                "qualification": "KNOWN_MATCHES_ONLY_NOT_COMPLETE_ACTUALS"}
        if group == "personal-care":
            plan = supplemental["personal-care-occurrences"]
            remaining = 0
            ambiguous = False
            for scheduled in plan["occurrences"]:
                match = next((m for m in matches if m["occurrence_id"] == "personal-care:" + scheduled["date"] and m["kind"] == "EXPENSE"), None)
                if match and match["status"] == "HELD":
                    ambiguous = True
                elif not match:
                    remaining += scheduled["amountMinor"]
            item.update(conditional_planning_protection_minor=None if ambiguous else remaining,
                        override="ACCEPTED_OCCURRENCES_REPLACE_RESIDUAL_NOT_ADDITIVE",
                        nominal_residual_not_added=True, source=plan,
                        qualification="UNQUOTED_PLANNING_RATES_PAID_STATE_NOT_INFERRED_FROM_DATE")
        components.append(item)
    components.append({"id": "discretionary", "account_role": "up.spending", "target_minor": rows["discretionary"]["budgetAmountMinor"],
                       "conditional_planning_protection_minor": max(0, rows["discretionary"]["budgetAmountMinor"] - known["discretionary"]),
                       "current_reserve_minor": None, "qualification": "SEPARATE_ALLOWANCE_INCOMPLETE_ACTUALS"})
    invoice = supplemental["1password-current-invoice"]
    invoice_match = next((m for m in matches if m["occurrence_id"] == "supplemental:1password-current-invoice" and m["kind"] == "EXPENSE"), None)
    components.append({"id": "1password-current-invoice", "account_role": "up.spending",
        "native_invoice_minor": invoice["nativeAmountMinor"], "native_currency": invoice["nativeCurrency"],
        "retained_aud_estimate_minor": invoice["estimatedProtectionMinor"],
        "conditional_planning_protection_minor": invoice["estimatedProtectionMinor"] if not invoice_match else None,
        "current_reserve_minor": None, "qualification": "ESTIMATE_NOT_FX_OR_PAID_PROOF; MATCH_REQUIRES_REFLECTION_REVIEW", "source": invoice})
    minimum = supplemental["mastercard-minimum"]
    latest = minimum["latestOwnerReport"]
    components.append({"id": "mastercard-minimum", "account_role": "up.spending",
        "conditional_planning_protection_minor": latest["currentMinimumReserveMinor"], "current_reserve_minor": None,
        "qualification": latest["basis"], "owner_report_date": latest["date"], "payment_due_owner_report": latest["paymentDue"],
        "bank_verified_payment_due": minimum["bankVerifiedPaymentDue"], "historical_superseded": minimum["historicalObligation"]})
    reported_rows = []
    for row in document["budgetRows"]:
        reported_rows.append({"budget_id": row["id"], "name": row["name"], "target_minor": row["budgetAmountMinor"],
            "currency": row["currency"], "bank": row["bank"], "payment_account": row["paymentAccount"],
            "mapping_state": row["mappingState"], "reserve_class": row["reserveClass"],
            "known_match_count": counts[row["id"]], "known_committed_spend_minor": known[row["id"]],
            "known_held_minor": held[row["id"]], "actual_minor": None, "remaining_minor": None,
            "approved_remaining_reserve_minor": row.get("remainingReserveMinor"),
            "coverage": "PARTIAL_KNOWN_MATCHES" if counts[row["id"]] else "NO_VERIFIED_MATCHES_NOT_ZERO_ACTUALS",
            "cycle_payment_state": row["cyclePaymentState"], "source_row": row})
    unknown_routes = [r["id"] for r in document["budgetRows"] if r["bank"] == "UNKNOWN_CURRENT_MAPPING"]
    provisions = [r for r in document["budgetRows"] if r["reserveClass"] == "provision_component_not_extra_expense"]
    card_observation = evidence.get("cardObservation")
    if card_observation is not None:
        references(card_observation.get("provenance"))
        if card_observation.get("currency") != "AUD" or stamp(date(card_observation["asOf"])) > as_of:
            raise EvidenceError("Dated AUD card observation required")
        value = card_observation["availableCreditMinor"]
        if type(value) is not int or not -(2**63) <= value < 2**63:
            raise EvidenceError("Card credit observation must be signed integer minor units")
        card_observation = {key: card_observation[key] for key in ("availableCreditMinor", "currency", "asOf", "provenance")}
    dated = document["datedRegressionIdentityNotCurrentAdvice"]
    totals = [x["conditional_planning_protection_minor"] for x in components]
    report = {"schema": "nico-budget-report/v1", "source_hash": SOURCE_HASH, "as_of": as_of, "cycle": CYCLE,
        "qualification": "QUALIFIED_COMPONENTS_ONLY_NOT_CURRENT_ASSURED_SPENDING",
        "cycle_is_explicit_not_rolling": True, "cycle_timezone": "NOT_INFERRED",
        "allocation_total_minor": 667091, "allocation_is_cash_requirement": False,
        "actual_total_minor": None, "remaining_total_minor": None,
        "rows": reported_rows, "matched_occurrences": matches, "unresolved_matches": unresolved,
        "up_spending": {"observation": bound.get("up.spending"), "hold_re_deduction_minor": 0,
            "planning_components": components, "known_component_planning_subtotal_minor": sum(totals) if all(x is not None for x in totals) else None,
            "subtotal_excludes_unresolved_obligations": True, "conditional_cash_room_minor": None, "assured_safe_withdrawal_minor": None,
            "unresolved_current_merchant_mappings": unknown_routes,
            "qualification": "INCOMPLETE_CYCLE_SPENDING_UNPAID_OCCURRENCES_AND_FUNDING_COVERAGE"},
        "protected_essentials_savers": {"observations": {k: v for k, v in bound.items() if k != "up.spending"},
            "provision_allocation_minor": sum(r["budgetAmountMinor"] for r in provisions), "provision_rows": [r["id"] for r in provisions],
            "funded_remaining_minor": None, "included_in_free_cash_minor": 0,
            "qualification": "PROTECTED_ACCOUNTS; PROVISIONS_NOT_AN_EXTRA_EXPENSE_OR_VERIFIED_FUNDING"},
        "commbank_credit": {"observation": card_observation, "credit_is_cash": False, "pending_re_deduction_minor": 0,
            "retained_dated_observation": {"as_of": dated["cardAsOf"], "available_credit_minor": dated["cardObservedAvailableMinor"], "qualification": "HISTORICAL_NOT_CURRENT"},
            "retained_dated_protections": [{"budget_id": r["id"], "amount_minor": r["datedProtectionMinor"], "as_of": r["protectionAsOf"], "state": r["cyclePaymentState"]} for r in document["budgetRows"] if "datedProtectionMinor" in r],
            "conditional_additional_card_spending_minor": None, "assured_additional_card_spending_minor": None,
            "qualification": "DATED_OR_MISSING_CARD_OBSERVATION; REFLECTION_FEES_INTEREST_AND_COVERAGE_UNVERIFIED"},
        "unbound_roles": sorted({"up.spending", "up.essentials"} - set(bound)),
        "source_dates_and_provenance": {"remaining_inputs": document["remainingDataInputs"], "evidence": document["evidence"]},
        "no_automatic_actions": True}
    from cycle_calculation import calculate as conditional_calculate
    report = conditional_calculate(connection, document, evidence, report)
    if evidence.get("cardPlanning") is not None:
        from card_report import build
        report["commbank_credit"]["planning"] = build(evidence["cardPlanning"], as_of)
    return report


def persist_report(factory, evidence, as_of):
    timestamp = stamp(date(as_of))
    with transaction(factory) as connection:
        schema(connection)
        stored = connection.execute("SELECT raw_json FROM budget_sources WHERE source_hash=?", (SOURCE_HASH,)).fetchone()
        if not stored:
            raise EvidenceError("Approved rule map must be imported first")
        document = contract(stored[0])
        report = calculate(connection, document, evidence, timestamp)
        report_json = canonical(report)
        report_id = hashlib.sha256(canonical({"report": report, "evidence": evidence}).encode()).hexdigest()
        connection.execute("INSERT OR IGNORE INTO budget_reports VALUES (?,?,?,?,?)", (report_id, SOURCE_HASH, timestamp, canonical(evidence), report_json))
    return report_id, report


def main():
    parser = argparse.ArgumentParser(description="Explicit private budget import/report; no bank or Sheets calls")
    parser.add_argument("--rules", required=True, type=Path)
    parser.add_argument("--evidence-json", type=Path, help="Optional existing verified bindings/joins and dated card evidence")
    parser.add_argument("--as-of", default=stamp(datetime.now(timezone.utc)))
    parser.add_argument("--catch-up", action="store_true", help="Explicit read-only current-cycle collection; never runs at startup")
    parser.add_argument("--cycle-start-at", help="Approved 14 October local midnight with explicit RFC3339 offset")
    parser.add_argument("--cycle-end-at", help="Approved 15 November local midnight with explicit RFC3339 offset")
    parser.add_argument("--resume-catchup", help="Resume a retained bounded collection checkpoint")
    args = parser.parse_args()
    from main import open_database
    factory = lambda: open_database(Path("/data"), True)
    evidence = json.loads(args.evidence_json.read_text()) if args.evidence_json else {}
    if args.catch_up:
        if not args.cycle_start_at or not args.cycle_end_at or not evidence.get("bindings"):
            parser.error("Catch-up requires explicit cycle offsets and verified evidence bindings")
        from cycle_calculation import boundaries, catch_up
        boundaries(args.cycle_start_at, args.cycle_end_at)
        token = os.environ.get("UP_PAT")
        if not token:
            parser.error("UP_PAT required for explicit read-only catch-up")
        from finance_worker import UpClient
        client = UpClient(token)
        try:
            receipt = catch_up(factory, client, evidence["bindings"], args.cycle_start_at,
                               args.cycle_end_at, resume=args.resume_catchup)
        finally:
            client.close()
        print(canonical(receipt))
        return  # Collection and report publication are separate explicit operations.
    if args.resume_catchup or args.cycle_start_at or args.cycle_end_at:
        parser.error("Cycle collection arguments require --catch-up")
    result = import_rules(factory, args.rules.read_bytes(), datetime.now(timezone.utc))
    report_id, report = persist_report(factory, evidence, args.as_of)
    output = Path("/data/nico-ingress") / ("budget-report-" + report_id + ".json")
    data = canonical(report).encode()
    try:
        fd = os.open(output, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        if output.is_symlink() or output.read_bytes() != data:
            raise EvidenceError("Existing private report file conflicts")
    else:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
    print(canonical({**result, "report_id": report_id, "private_report_basename": output.name, "qualification": report["qualification"]}))


if __name__ == "__main__":
    main()
