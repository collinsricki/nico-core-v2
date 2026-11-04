"""Accounts-only observations; never advances transaction reconciliation coverage."""
import hashlib
import json
import logging
import sqlite3
import shutil
from pathlib import Path
from datetime import timedelta

from finance_worker import UpFailure, date, delay, identifier, money, stamp, transaction

INTERVAL_SECONDS = 3600
MAX_ACCOUNTS = 1000
MAX_BATCH_BYTES = 512 * 1024
MIN_FREE_BYTES = 32 * 1024 * 1024
LOG = logging.getLogger(__name__)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def report_jobs_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS budget_balance_jobs (
        seed_report_id TEXT NOT NULL,reconciliation_id TEXT NOT NULL,balance_batch_id TEXT NOT NULL,
        report_id TEXT NOT NULL,generated_at TEXT NOT NULL,
        PRIMARY KEY(seed_report_id,reconciliation_id,balance_batch_id))''')


def schema(db, now):
    """Additive, transactional extension; core schema/version and old data stay intact."""
    if db.execute('PRAGMA user_version').fetchone()[0] != 2:
        raise ValueError('Accepted core schema required')
    db.execute('''CREATE TABLE IF NOT EXISTS balance_refresh_batches(
        batch_id TEXT PRIMARY KEY,observed_at TEXT NOT NULL,source_kind TEXT NOT NULL
        CHECK(source_kind IN ('ACCOUNTS_GET','DAILY_REUSE')),source_reference TEXT,
        accounts_json TEXT NOT NULL CHECK(length(accounts_json)<=524288))''')
    db.execute('''CREATE TABLE IF NOT EXISTS balance_refresh_state(
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),schema_version INTEGER NOT NULL CHECK(schema_version=1),
        status TEXT NOT NULL CHECK(status IN ('PENDING','READY','RETRY','AUTH_REQUIRED','PERMANENT_ERROR')),
        selected_batch_id TEXT REFERENCES balance_refresh_batches(batch_id),
        last_success_at TEXT,next_attempt_at TEXT,failure_count INTEGER NOT NULL DEFAULT 0 CHECK(failure_count>=0),
        last_error TEXT,attention_required INTEGER NOT NULL DEFAULT 0 CHECK(attention_required IN (0,1)))''')
    report_jobs_schema(db)
    old = db.execute('SELECT max(observed_at) FROM balance_snapshots').fetchone()[0]
    due = max(now, date(old) + timedelta(seconds=INTERVAL_SECONDS)) if old else now
    db.execute('''INSERT INTO balance_refresh_state(singleton,schema_version,status,next_attempt_at)
        VALUES(1,1,'PENDING',?) ON CONFLICT(singleton) DO NOTHING''', (stamp(due),))
    row = db.execute('SELECT schema_version FROM balance_refresh_state WHERE singleton=1').fetchone()
    if not row or row[0] != 1:
        raise ValueError('Unsupported balance extension version')


def validate_accounts(accounts):
    if type(accounts) is not dict or not 0 < len(accounts) <= MAX_ACCOUNTS:
        raise UpFailure('INVALID_BALANCE_BATCH', 'permanent')
    for key, value in accounts.items():
        identifier(key)
        if (type(value) is not dict or set(value) != {'balance_minor', 'currency'}
                or type(value['balance_minor']) is not int
                or not -(2**63) < value['balance_minor'] < 2**63
                or type(value['currency']) is not str or len(value['currency']) != 3):
            raise UpFailure('INVALID_BALANCE_BATCH', 'permanent')
    encoded = canonical(accounts)
    if len(encoded.encode()) > MAX_BATCH_BYTES:
        raise UpFailure('BALANCE_BATCH_LIMIT', 'permanent')
    return encoded


def insert_batch(db, accounts, observed_at, source_kind, source_reference=None):
    encoded = validate_accounts(accounts)
    date(observed_at)
    payload = {'observed_at': observed_at, 'source_kind': source_kind,
               'source_reference': source_reference, 'accounts': accounts}
    identity = hashlib.sha256(canonical(payload).encode()).hexdigest()
    db.execute('INSERT OR IGNORE INTO balance_refresh_batches VALUES(?,?,?,?,?)',
               (identity, observed_at, source_kind, source_reference, encoded))
    return identity


def reuse_daily(db, run_id):
    """Reuse only a committed, successful daily batch; retain original times/errors."""
    run = db.execute('SELECT status,error_code FROM reconciliation_runs WHERE run_id=?', (run_id,)).fetchone()
    if not run or run[0] != 'OBSERVED' or run[1] is not None:
        return None
    rows = db.execute('SELECT account_id,balance_base_units,currency_code,observed_at FROM balance_snapshots WHERE run_id=?',
                      (run_id,)).fetchall()
    if not rows or len({r[3] for r in rows}) != 1:
        raise ValueError('Incomplete daily balance batch')
    accounts = {r[0]: {'balance_minor': r[1], 'currency': r[2]} for r in rows}
    if len(accounts) != len(rows):
        raise ValueError('Duplicate daily account')
    observed = rows[0][3]
    identity = insert_batch(db, accounts, observed, 'DAILY_REUSE', run_id)
    state = db.execute('SELECT last_success_at,status FROM balance_refresh_state WHERE singleton=1').fetchone()
    if state[0] and date(state[0]) >= date(observed):
        return identity
    # Do not clear an accounts-only retry/permanent/auth error on reuse.
    db.execute('UPDATE balance_refresh_state SET selected_batch_id=?,last_success_at=? WHERE singleton=1',
               (identity, observed))
    if state[1] in ('PENDING', 'READY'):
        db.execute("UPDATE balance_refresh_state SET status='READY',next_attempt_at=? WHERE singleton=1",
                   (stamp(date(observed) + timedelta(seconds=INTERVAL_SECONDS)),))
    return identity


def reuse_daily_isolated(db, run_id):
    """Optional reuse may fail without discarding valid daily transaction work.

    If SQLite has aborted the outer transaction, or savepoint recovery fails,
    propagate: there is then no valid daily commit to preserve or claim.
    """
    if not db.in_transaction:
        raise ValueError('Daily reuse requires an existing transaction')
    db.execute('SAVEPOINT hourly_balance_daily_reuse')
    try:
        result = reuse_daily(db, run_id)
    except (UpFailure, ValueError, TypeError, KeyError, sqlite3.Error):
        if not db.in_transaction:
            raise
        db.execute('ROLLBACK TO SAVEPOINT hourly_balance_daily_reuse')
        db.execute('RELEASE SAVEPOINT hourly_balance_daily_reuse')
        LOG.warning('BALANCE_REUSE_UNAVAILABLE')
        return None
    db.execute('RELEASE SAVEPOINT hourly_balance_daily_reuse')
    return result


def due(worker):
    rows = worker._read('SELECT * FROM balance_refresh_state WHERE singleton=1')
    state = rows[0]
    return (state if state['next_attempt_at'] and date(state['next_attempt_at']) <= worker.clock()
            and state['status'] in ('PENDING', 'READY', 'RETRY') else None)


def collect(worker, state):
    """Only accounts(), using the existing client limits/timeouts. One atomic batch."""
    started = worker.clock()
    try:
        db = worker.connection_factory()
        try:
            filename = db.execute('PRAGMA database_list').fetchone()[2]
            if filename and shutil.disk_usage(Path(filename).parent).free < MIN_FREE_BYTES:
                raise UpFailure('BALANCE_STORAGE_HEADROOM_REQUIRED', 'permanent')
        finally:
            db.close()
        accounts = {}
        for resource in worker.client.accounts():
            if worker.stopping.is_set():
                raise UpFailure('WORKER_STOPPING')
            try:
                if resource['type'] != 'accounts':
                    raise ValueError()
                key = identifier(resource['id'])
                amount, currency = money(resource['attributes']['balance'])
            except (KeyError, TypeError, ValueError):
                raise UpFailure('INVALID_ACCOUNT', 'permanent') from None
            if key in accounts or len(accounts) >= MAX_ACCOUNTS:
                raise UpFailure('DUPLICATE_OR_EXCESS_ACCOUNTS', 'permanent')
            accounts[key] = {'balance_minor': amount, 'currency': currency}
        validate_accounts(accounts)
        observed = worker.clock()
        if observed < started:
            raise UpFailure('BALANCE_CLOCK_REGRESSED', 'permanent')
        with transaction(worker.connection_factory) as db:
            row = db.execute('SELECT next_attempt_at,last_success_at,status,failure_count,last_error FROM balance_refresh_state WHERE singleton=1').fetchone()
            if tuple(row) != (state['next_attempt_at'], state['last_success_at'], state['status'],
                              state['failure_count'], state['last_error']):
                return  # A concurrently advanced checkpoint is never overwritten.
            if state['last_success_at'] and observed < date(state['last_success_at']):
                raise UpFailure('BALANCE_CLOCK_REGRESSED', 'permanent')
            identity = insert_batch(db, accounts, stamp(observed), 'ACCOUNTS_GET')
            db.execute('''UPDATE balance_refresh_state SET selected_batch_id=?,last_success_at=?,
                next_attempt_at=?,status='READY',failure_count=0,last_error=NULL,attention_required=0 WHERE singleton=1''',
                       (identity, stamp(observed), stamp(observed + timedelta(seconds=INTERVAL_SECONDS))))
    except UpFailure as error:
        count = state['failure_count'] + 1
        status = 'AUTH_REQUIRED' if error.kind == 'auth' else ('PERMANENT_ERROR' if error.kind == 'permanent' else 'RETRY')
        retry = (stamp(worker.clock() + timedelta(seconds=delay(count, error.retry_after)))
                 if status == 'RETRY' else None)
        with transaction(worker.connection_factory) as db:
            db.execute('''UPDATE balance_refresh_state SET status=?,next_attempt_at=?,failure_count=?,last_error=?,
                attention_required=? WHERE singleton=1 AND next_attempt_at IS ? AND last_success_at IS ? AND status=?
                AND failure_count=? AND last_error IS ?''',
                       (status, retry, count, error.code, int(count >= 10 or status != 'RETRY'),
                        state['next_attempt_at'], state['last_success_at'], state['status'],
                        state['failure_count'], state['last_error']))
        if error.kind == 'auth':
            worker.auth_paused = True
            worker._control('AUTH_REQUIRED')


def selected(db, now):
    if not db.execute("SELECT 1 FROM sqlite_schema WHERE name='balance_refresh_state'").fetchone():
        return None
    row = db.execute('''SELECT b.* FROM balance_refresh_batches b JOIN balance_refresh_state s
        ON b.batch_id=s.selected_batch_id WHERE s.singleton=1''').fetchone()
    if not row:
        return None
    item = dict(row)
    if date(item['observed_at']) > now:
        raise ValueError('Future balance observation')
    accounts = json.loads(item['accounts_json'])
    validate_accounts(accounts)
    payload = {'observed_at': item['observed_at'], 'source_kind': item['source_kind'],
               'source_reference': item['source_reference'], 'accounts': accounts}
    if hashlib.sha256(canonical(payload).encode()).hexdigest() != item['batch_id']:
        raise ValueError('Balance batch integrity mismatch')
    item['accounts'] = accounts
    return item
