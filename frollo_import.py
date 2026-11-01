"""Explicit private Frollo observations only; no canonical ledger or network writes."""

import argparse
import csv
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import io
import json
import re
from pathlib import Path

from finance_worker import transaction

HEADERS = ('transaction_id', 'description', 'user_description', 'amount', 'currency',
           'transaction_date', 'posted_date', 'account_number', 'account_name',
           'credit_debit', 'transaction_type', 'provider_name', 'merchant_name',
           'budget_category', 'category_name', 'user_tags', 'notes', 'included')
TYPES = {'payment', 'fee', 'interest_charged', 'other', 'transfer_incoming', 'transfer_outgoing'}
IDENTITY = ('provider_name', 'account_number', 'account_name')
MAX_BYTES = 8 * 1024 * 1024
MAX_ROWS = 10000


class ImportError(ValueError):
    """Safe fixed-code validation error; never includes financial values."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def calendar(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        raise ValueError()
    return date.fromisoformat(value).isoformat()


def binding(raw):
    try:
        if len(raw) > 65536:
            raise ValueError()
        value = json.loads(raw.decode('utf-8'))
        if (not isinstance(value, dict)
                or set(value) != {'schema', 'account_role', *IDENTITY, 'verified_on', 'provenance'}
                or value['schema'] != 'nico-frollo-binding/v1'
                or value['account_role'] != 'commbank.mastercard'
                or any(not isinstance(value[k], str) or not value[k].strip() for k in IDENTITY)
                or not isinstance(value['provenance'], list) or not value['provenance']
                or any(not isinstance(x, str) or not x.strip() for x in value['provenance'])):
            raise ValueError()
        calendar(value['verified_on'])
        identity = tuple(value[k] for k in IDENTITY)
        namespace = 'frollo.csv:' + digest(canonical(identity).encode())
        return value, identity, namespace
    except (ValueError, KeyError, TypeError, UnicodeError):
        raise ImportError('INVALID_EXPLICIT_MASTERCARD_BINDING') from None


def records(raw):
    if not raw or len(raw) > MAX_BYTES:
        raise ImportError('CSV_SIZE_OUTSIDE_LIMIT')
    try:
        text = raw.decode('utf-8')
        reader = csv.reader(io.StringIO(text, newline=''), strict=True)
        if tuple(next(reader)) != HEADERS:
            raise ImportError('UNSUPPORTED_CSV_HEADERS')
        physical = io.StringIO(text, newline='').readlines()
        result = []
        previous = reader.line_num
        for number, values in enumerate(reader, 1):
            if number > MAX_ROWS or len(values) != len(HEADERS):
                raise ImportError('CSV_ROW_SHAPE_OR_COUNT_UNSUPPORTED')
            result.append((number, previous + 1, reader.line_num,
                           ''.join(physical[previous:reader.line_num]).encode('utf-8'), dict(zip(HEADERS, values))))
            previous = reader.line_num
        return result
    except (UnicodeError, csv.Error, StopIteration):
        raise ImportError('INVALID_UTF8_OR_CSV') from None


def observation(values):
    flags = ['NO_EXPLICIT_PROCESSING_STATUS', 'CROSS_EXPORT_ID_STABILITY_UNPROVEN']
    native_minor = None
    try:
        if len(values['amount']) > 64 or not re.fullmatch(r'-?\d+(?:\.\d{1,2})?', values['amount']):
            raise ValueError()
        # No binary floating point, rounding or invented exchange rate.
        native_minor = int(Decimal(values['amount']) * 100)
        if not -(2**63) <= native_minor < 2**63:
            raise ValueError()
    except (ValueError, ArithmeticError):
        native_minor = None
        flags.append('INVALID_AMOUNT')
    if not re.fullmatch(r'[0-9]+', values['transaction_id']):
        flags.append('INVALID_TEXT_SOURCE_ID')
    if values['currency'] != 'AUD':
        flags.append('UNSUPPORTED_CURRENCY')
    direction = values['credit_debit']
    if direction not in ('credit', 'debit'):
        flags.append('UNSUPPORTED_CREDIT_DEBIT')
    elif native_minor is not None and ((direction == 'credit' and native_minor < 0)
                                      or (direction == 'debit' and native_minor > 0)):
        flags.append('AMOUNT_DIRECTION_CONFLICT')
    dates = {}
    for key in ('transaction_date', 'posted_date'):
        try:
            dates[key] = calendar(values[key])
        except ValueError:
            dates[key] = None
            flags.append('POSTED_DATE_MISSING' if key == 'posted_date' and values[key] == '' else 'INVALID_' + key.upper())
    if values['transaction_type'] not in TYPES:
        flags.append('UNSUPPORTED_TRANSACTION_TYPE')
    if values['transaction_type'] == 'transfer_incoming':
        flags.append('MERCHANT_CREDIT_OR_REPAYMENT_UNRESOLVED')
    if values['included'] != 'true':
        flags.append('SOURCE_EXCLUDED' if values['included'] == 'false' else 'INCLUSION_UNRESOLVED')
    if values['merchant_name'] in ('', 'Unknown'):
        flags.append('MERCHANT_UNRESOLVED')
    return native_minor, dates, sorted(set(flags))


def schema(db):
    if db.execute('PRAGMA user_version').fetchone()[0] != 2:
        raise ImportError('ACCEPTED_CORE_SCHEMA_2_REQUIRED')
    db.execute('''CREATE TABLE IF NOT EXISTS frollo_sources (
        source_hash TEXT PRIMARY KEY,original_bytes BLOB NOT NULL,byte_count INTEGER NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS frollo_bindings (
        binding_hash TEXT PRIMARY KEY,namespace TEXT NOT NULL,original_bytes BLOB NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS frollo_imports (
        source_hash TEXT NOT NULL,binding_hash TEXT NOT NULL,namespace TEXT NOT NULL,
        export_date TEXT NOT NULL,imported_at TEXT NOT NULL,selected_rows INTEGER NOT NULL,
        excluded_rows INTEGER NOT NULL,PRIMARY KEY(source_hash,namespace))''')
    db.execute('''CREATE TABLE IF NOT EXISTS frollo_observations (
        observation_key TEXT PRIMARY KEY,namespace TEXT NOT NULL,source_transaction_id TEXT NOT NULL,
        row_hash TEXT NOT NULL,row_json TEXT NOT NULL,native_amount_minor INTEGER,currency TEXT NOT NULL,
        transaction_date TEXT,posted_date TEXT,credit_debit TEXT NOT NULL,transaction_type TEXT NOT NULL,
        included_text TEXT NOT NULL,flags_json TEXT NOT NULL,state TEXT NOT NULL
            CHECK(state IN ('UNACCEPTED_OBSERVATION','UNRESOLVED_CONFLICT')),
        UNIQUE(namespace,source_transaction_id,row_hash))''')
    db.execute('''CREATE INDEX IF NOT EXISTS frollo_observation_identity
        ON frollo_observations(namespace,source_transaction_id)''')
    db.execute('''CREATE TABLE IF NOT EXISTS frollo_row_provenance (
        source_hash TEXT NOT NULL,namespace TEXT NOT NULL,row_number INTEGER NOT NULL,
        line_start INTEGER NOT NULL,line_end INTEGER NOT NULL,original_record BLOB NOT NULL,
        observation_key TEXT,selection TEXT NOT NULL,PRIMARY KEY(source_hash,namespace,row_number))''')


def import_csv(factory, raw, binding_raw, export_date, imported_at=None):
    """One atomic observation import. Does not call the accepted ledger/report engine."""
    try:
        export_date = calendar(export_date)
    except ValueError:
        raise ImportError('EXPLICIT_EXPORT_DATE_REQUIRED') from None
    config, identity, namespace = binding(binding_raw)
    parsed = records(raw)
    selected = []
    excluded = []
    for item in parsed:
        values = item[-1]
        account = tuple(values[k] for k in IDENTITY)
        # A changed name under the same provider/masked number is not a guessed rename.
        if account[:2] == identity[:2] and account[2] != identity[2]:
            raise ImportError('AMBIGUOUS_ACCOUNT_BINDING')
        (selected if account == identity else excluded).append(item)
    if not selected:
        raise ImportError('BOUND_MASTERCARD_ACCOUNT_NOT_PRESENT')
    source_hash = digest(raw)
    binding_hash = digest(binding_raw)
    imported_at = imported_at or datetime.now(timezone.utc).isoformat()
    created = 0
    reused = 0
    conflicts = 0
    flag_counts = {}
    with transaction(factory) as db:
        schema(db)
        old = db.execute('SELECT export_date,selected_rows,excluded_rows FROM frollo_imports WHERE source_hash=? AND namespace=?', (source_hash, namespace)).fetchone()
        if old:
            if old['export_date'] != export_date:
                raise ImportError('RETAINED_EXPORT_DATE_CONFLICT')
            return {'source_hash': source_hash, 'already_imported': True,
                    'selected_rows': old['selected_rows'], 'excluded_rows': old['excluded_rows'],
                    'canonical_effect': 'NONE'}
        existing = db.execute('SELECT original_bytes FROM frollo_sources WHERE source_hash=?', (source_hash,)).fetchone()
        if existing and existing[0] != raw:
            raise ImportError('SOURCE_BYTES_CONFLICT')
        db.execute('INSERT OR IGNORE INTO frollo_sources VALUES (?,?,?)', (source_hash, raw, len(raw)))
        db.execute('INSERT OR IGNORE INTO frollo_bindings VALUES (?,?,?)', (binding_hash, namespace, binding_raw))
        for number, first, last, original_record, values in selected:
            row_json = canonical(values)
            row_hash = digest(row_json.encode())
            source_id = values['transaction_id']  # Preserve digit strings, including leading zeros.
            key = digest(canonical((namespace, source_id, row_hash)).encode())
            amount, dates, flags = observation(values)
            for flag in flags:
                flag_counts[flag] = flag_counts.get(flag, 0) + 1
            same = db.execute('SELECT 1 FROM frollo_observations WHERE observation_key=?', (key,)).fetchone()
            other = db.execute('SELECT 1 FROM frollo_observations WHERE namespace=? AND source_transaction_id=? AND row_hash<>?', (namespace, source_id, row_hash)).fetchone()
            state = 'UNRESOLVED_CONFLICT' if other else 'UNACCEPTED_OBSERVATION'
            if other:
                db.execute("UPDATE frollo_observations SET state='UNRESOLVED_CONFLICT' WHERE namespace=? AND source_transaction_id=?", (namespace, source_id))
                conflicts += 1
            if same:
                reused += 1
            else:
                db.execute('INSERT INTO frollo_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                           (key, namespace, source_id, row_hash, row_json, amount, values['currency'],
                            dates['transaction_date'], dates['posted_date'], values['credit_debit'],
                            values['transaction_type'], values['included'], canonical(flags), state))
                created += 1
            db.execute('INSERT INTO frollo_row_provenance VALUES (?,?,?,?,?,?,?,?)',
                       (source_hash, namespace, number, first, last, original_record, key, 'BOUND_MASTERCARD'))
        for number, first, last, original_record, values in excluded:
            db.execute('INSERT INTO frollo_row_provenance VALUES (?,?,?,?,?,?,?,?)',
                       (source_hash, namespace, number, first, last, original_record, None, 'EXCLUDED_OTHER_ACCOUNT'))
        db.execute('INSERT INTO frollo_imports VALUES (?,?,?,?,?,?,?)',
                   (source_hash, binding_hash, namespace, export_date, imported_at, len(selected), len(excluded)))
    return {'source_hash': source_hash, 'already_imported': False, 'selected_rows': len(selected),
            'excluded_rows': len(excluded), 'new_observations': created, 'reused_observations': reused,
            'conflicting_observations': conflicts, 'flag_counts': flag_counts, 'canonical_effect': 'NONE'}


def main():
    parser = argparse.ArgumentParser(description='Explicit private Frollo Mastercard observation import; no canonical totals or network')
    parser.add_argument('--csv', required=True, type=Path)
    parser.add_argument('--binding', required=True, type=Path)
    parser.add_argument('--export-date', required=True, help='Explicit source export date, YYYY-MM-DD; no inferred timezone')
    parser.add_argument('--expected-sha256', help='Optional independently verified original-file SHA256')
    args = parser.parse_args()
    try:
        with args.csv.open('rb') as source:
            raw = source.read(MAX_BYTES + 1)
        with args.binding.open('rb') as source:
            config_raw = source.read(65537)
        if args.expected_sha256 and digest(raw) != args.expected_sha256:
            raise ImportError('EXPECTED_SOURCE_HASH_MISMATCH')
        from main import open_database
        result = import_csv(lambda: open_database(Path('/data'), True), raw, config_raw, args.export_date)
    except ImportError as error:
        parser.exit(2, str(error) + '\n')
    except (OSError, __import__('sqlite3').Error):
        parser.exit(2, 'PRIVATE_INPUT_OR_DATASTORE_UNAVAILABLE\n')
    print(canonical(result))  # Metadata only: no descriptions, account identifiers or values.


if __name__ == '__main__':
    main()
