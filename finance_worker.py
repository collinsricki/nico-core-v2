"""Single-process SQLite transaction mirror; no payment or Sheets writes."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import logging
import re
import sqlite3
import threading
from urllib.parse import urlencode, urlsplit
from uuid import UUID, uuid4

import requests

SCHEMA_VERSION = 2
BATCH_SIZE = 25
PAGE_LIMIT = 10
HELD_LIMIT = 100
DAY_SECONDS = 86400
OVERLAP_SECONDS = 300
EVENT_COLUMNS = ("event_id", "event_type", "event_created_at", "webhook_id",
                 "transaction_id", "received_at", "raw_body", "payload_sha256", "status")
PROJECTION_COLUMNS = ("account_id", "up_status", "amount_base_units", "currency_code",
                      "description", "created_at", "settled_at", "resource_json")
LOG = logging.getLogger(__name__)


def utcnow():
    return datetime.now(timezone.utc)


def date(value):
    try:
        result = datetime.fromisoformat(value)
        if result.tzinfo is None:
            raise ValueError()
        return result.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        raise UpFailure("INVALID_TIMESTAMP", "permanent") from None


def stamp(value):
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def identifier(value):
    try:
        if not isinstance(value, str) or str(UUID(value)) != value:
            raise ValueError()
        return value
    except (ValueError, TypeError, AttributeError):
        raise UpFailure("INVALID_RESOURCE_ID", "permanent") from None


def delay(count, retry_after=None):
    # No retry-count ceiling discards recoverable work.
    return min(3600, max(5 * 2 ** min(max(count - 1, 0), 10), retry_after or 0))


class UpFailure(Exception):
    def __init__(self, code, kind="retryable", http_status=None, retry_after=None):
        super().__init__(code)
        self.code = code
        self.kind = kind
        self.http_status = http_status
        self.retry_after = retry_after


class UpClient:
    BASE = "https://api.up.com.au/api/v1"

    def __init__(self, token, session=None):
        self._token = token
        self.session = session or requests.Session()
        self.session.trust_env = False

    def close(self):
        self.session.close()

    def get(self, url, path):
        target = urlsplit(url)
        if (target.scheme != "https" or target.netloc != "api.up.com.au" or
                target.path != "/api/v1" + path or target.fragment or target.username):
            raise UpFailure("UNSAFE_API_URL", "permanent")
        response = None
        try:
            response = self.session.get(url, headers={"Authorization": "Bearer " + self._token},
                                        timeout=(3, 10), allow_redirects=False, stream=True)
            status = response.status_code
            if status != 200:
                if status in (401, 403):
                    raise UpFailure("AUTH_REQUIRED", "auth", status)
                if status == 404:
                    raise UpFailure("RESOURCE_NOT_FOUND", "not_found", status)
                if status == 429 or 500 <= status <= 599:
                    retry = response.headers.get("Retry-After", "")
                    retry = min(int(retry), 3600) if retry.isdigit() else None
                    raise UpFailure("API_TEMPORARY", "retryable", status, retry)
                raise UpFailure("API_REQUEST_REJECTED", "permanent", status)
            content = bytearray()
            for chunk in response.iter_content(65536):
                if len(content) + len(chunk) > 4 * 1024 * 1024:
                    raise UpFailure("API_RESPONSE_TOO_LARGE", "permanent")
                content.extend(chunk)
            try:
                payload = json.loads(bytes(content).decode("utf-8"))
                if not isinstance(payload, dict):
                    raise ValueError()
                return payload
            except (ValueError, UnicodeError, RecursionError):
                raise UpFailure("API_INVALID_JSON", "permanent") from None
        except requests.RequestException:
            # Never persist/log exception text, response bodies or auth headers.
            raise UpFailure("API_NETWORK", "retryable") from None
        finally:
            if response is not None:
                response.close()

    def transaction(self, transaction_id):
        path = "/transactions/" + identifier(transaction_id)
        result = self.get(self.BASE + path, path)
        data = result.get("data")
        if not isinstance(data, dict) or data.get("id") != transaction_id:
            raise UpFailure("TRANSACTION_RESPONSE_MISMATCH", "permanent")
        return data

    def pages(self, path, parameters=None):
        url = self.BASE + path + "?" + urlencode({"page[size]": 100, **(parameters or {})})
        items = []
        for _ in range(PAGE_LIMIT):
            page = self.get(url, path)
            if (not isinstance(page.get("data"), list) or not isinstance(page.get("links"), dict)
                    or "next" not in page["links"]):
                raise UpFailure("INVALID_PAGE", "permanent")
            if len(page["data"]) > 100:
                raise UpFailure("PAGE_SIZE_EXCEEDED", "permanent")
            items.extend(page["data"])
            url = page["links"]["next"]
            if url is None:
                return items
            if not isinstance(url, str):
                raise UpFailure("INVALID_PAGE_URL", "permanent")
        raise UpFailure("PAGE_CAP_REACHED", "retryable")

    def accounts(self):
        return self.pages("/accounts")

    def transactions(self, since, until):
        return self.pages("/transactions", {"filter[since]": since, "filter[until]": until})


@contextmanager
def transaction(factory):
    connection = factory()
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("BEGIN IMMEDIATE")
        yield connection
        connection.commit()
    except BaseException:
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
        connection.close()


def event_table(name):
    return f"""CREATE TABLE {name} (
        event_id TEXT PRIMARY KEY NOT NULL,
        event_type TEXT NOT NULL CHECK(event_type IN
          ('PING','TRANSACTION_CREATED','TRANSACTION_SETTLED','TRANSACTION_DELETED')),
        event_created_at TEXT NOT NULL, webhook_id TEXT NOT NULL, transaction_id TEXT,
        received_at TEXT NOT NULL, raw_body BLOB NOT NULL, payload_sha256 TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDING' CHECK(status IN
          ('PENDING','PROCESSED','FAILED_RETRYABLE','FAILED_FATAL','IGNORED')),
        retry_count INTEGER NOT NULL DEFAULT 0 CHECK(retry_count >= 0),
        next_attempt_at TEXT, last_error TEXT,
        attention_required INTEGER NOT NULL DEFAULT 0 CHECK(attention_required IN (0,1)))"""


def ensure_schema(factory, now):
    with transaction(factory) as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        if version == SCHEMA_VERSION:
            return
        if version != 0:
            raise sqlite3.DatabaseError("Unsupported schema version")
        exists = connection.execute("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='webhook_events'").fetchone()
        retained_indexes = []
        if exists:
            columns = tuple(row[1] for row in connection.execute("PRAGMA table_info(webhook_events)"))
            if columns != EVENT_COLUMNS:
                raise sqlite3.DatabaseError("Unexpected intake schema")
            retained_indexes = [row[0] for row in connection.execute(
                "SELECT sql FROM sqlite_schema WHERE type='index' AND tbl_name='webhook_events' AND sql IS NOT NULL")]
            connection.execute(event_table("webhook_events_new"))
            names = ",".join(EVENT_COLUMNS)
            connection.execute(f"INSERT INTO webhook_events_new ({names}) SELECT {names} FROM webhook_events")
            connection.execute("DROP TABLE webhook_events")
            connection.execute("ALTER TABLE webhook_events_new RENAME TO webhook_events")
        else:
            connection.execute(event_table("webhook_events"))
        for sql in retained_indexes:
            connection.execute(sql)
        connection.execute("CREATE INDEX IF NOT EXISTS webhook_events_transaction ON webhook_events(transaction_id)")
        connection.execute("CREATE INDEX webhook_events_due ON webhook_events(status,next_attempt_at,event_created_at)")
        connection.execute("""CREATE TABLE transactions (
            transaction_id TEXT PRIMARY KEY NOT NULL, account_id TEXT,
            up_status TEXT CHECK(up_status IN ('HELD','SETTLED')),
            is_deleted INTEGER NOT NULL DEFAULT 0 CHECK(is_deleted IN (0,1)),
            amount_base_units INTEGER, currency_code TEXT, description TEXT,
            created_at TEXT, settled_at TEXT, resource_json TEXT,
            last_event_at TEXT, last_event_type TEXT, last_observed_at TEXT NOT NULL,
            ordering_conflict INTEGER NOT NULL DEFAULT 0,
            last_conflict TEXT)""")
        connection.execute("CREATE INDEX transactions_account ON transactions(account_id)")
        connection.execute("""CREATE TABLE reconciliation_checkpoint (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1), enabled_at TEXT NOT NULL,
            catchup_through TEXT, last_completed_at TEXT, next_attempt_at TEXT,
            failure_count INTEGER NOT NULL DEFAULT 0, last_error TEXT,
            attention_required INTEGER NOT NULL DEFAULT 0)""")
        connection.execute("INSERT INTO reconciliation_checkpoint(singleton,enabled_at,next_attempt_at) VALUES (1,?,?)", (stamp(now), stamp(now)))
        connection.execute("""CREATE TABLE reconciliation_runs (
            run_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('OBSERVED','PARTIAL','FAILED')),
            catchup_since TEXT NOT NULL, catchup_until TEXT NOT NULL,
            error_code TEXT, held_recheck_complete INTEGER NOT NULL)""")
        connection.execute("""CREATE TABLE balance_snapshots (
            run_id TEXT NOT NULL REFERENCES reconciliation_runs(run_id),
            account_id TEXT NOT NULL, balance_base_units INTEGER NOT NULL,
            currency_code TEXT NOT NULL, observed_at TEXT NOT NULL,
            coverage TEXT NOT NULL, calculated_balance_base_units INTEGER,
            discrepancy_base_units INTEGER, PRIMARY KEY(run_id,account_id),
            CHECK(calculated_balance_base_units IS NULL AND discrepancy_base_units IS NULL))""")
        connection.execute("""CREATE TABLE worker_control (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1), status TEXT NOT NULL,
            updated_at TEXT NOT NULL)""")
        connection.execute("INSERT INTO worker_control VALUES (1,'STARTING',?)", (stamp(now),))
        connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")


def money(value):
    if (not isinstance(value, dict) or type(value.get("valueInBaseUnits")) is not int
            or not -(2**63) <= value["valueInBaseUnits"] < 2**63
            or not isinstance(value.get("currencyCode"), str)
            or not re.fullmatch("[A-Z]{3}", value["currencyCode"])):
        raise UpFailure("INVALID_MONEY", "permanent")
    return value["valueInBaseUnits"], value["currencyCode"]


def normalize_transaction(resource):
    try:
        if resource["type"] != "transactions":
            raise ValueError()
        transaction_id = identifier(resource["id"])
        attributes = resource["attributes"]
        account_id = identifier(resource["relationships"]["account"]["data"]["id"])
        if resource["relationships"]["account"]["data"]["type"] != "accounts":
            raise ValueError()
        status = attributes["status"]
        if status not in ("HELD", "SETTLED"):
            raise ValueError()
        amount, currency = money(attributes["amount"])
        description = attributes.get("description")
        if description is not None and not isinstance(description, str):
            raise ValueError()
        if description is not None:
            description.encode("utf-8")
        return {"transaction_id": transaction_id, "account_id": account_id, "up_status": status,
                "amount_base_units": amount, "currency_code": currency, "description": description,
                "created_at": stamp(date(attributes["createdAt"])),
                "settled_at": stamp(date(attributes["settledAt"])) if attributes.get("settledAt") else None,
                "resource_json": json.dumps(resource, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)}
    except (KeyError, TypeError, ValueError, UnicodeError, RecursionError):
        raise UpFailure("INVALID_TRANSACTION", "permanent") from None


def _conflict(connection, transaction_id, code):
    connection.execute("UPDATE transactions SET ordering_conflict=1,last_conflict=? WHERE transaction_id=?", (code, transaction_id))
    return code


def event_conflict(old, event):
    if not old:
        return None
    event_at = date(event["event_created_at"])
    if old["last_event_at"] and event_at < date(old["last_event_at"]):
        return "STALE_EVENT"
    if (old["last_event_at"] and event_at == date(old["last_event_at"])
            and event["event_type"] != old["last_event_type"]):
        return "AMBIGUOUS_EVENT_ORDER"
    if event["event_type"] == "TRANSACTION_DELETED" and old["up_status"] == "SETTLED":
        return "DELETE_AFTER_SETTLEMENT"
    if event["event_type"] != "TRANSACTION_DELETED" and old["is_deleted"]:
        return "TOMBSTONE_PROTECTED"
    return None


def project(connection, snapshot, observed_at, event=None):
    transaction_id = event["transaction_id"] if event else snapshot["transaction_id"]
    old = connection.execute("SELECT * FROM transactions WHERE transaction_id=?", (transaction_id,)).fetchone()
    deletion = event is not None and event["event_type"] == "TRANSACTION_DELETED"
    event_at = date(event["event_created_at"]) if event else None
    if old:
        conflict = event_conflict(old, event) if event else None
        if conflict:
            return _conflict(connection, transaction_id, conflict)
        if not deletion:
            if old["is_deleted"]:
                return _conflict(connection, transaction_id, "TOMBSTONE_PROTECTED")
            if old["up_status"] == "SETTLED" and snapshot["up_status"] == "HELD":
                return _conflict(connection, transaction_id, "STATUS_REGRESSION")
            differs = any(old[key] != snapshot[key] for key in PROJECTION_COLUMNS)
            advances = old["up_status"] == "HELD" and snapshot["up_status"] == "SETTLED"
            if differs and not advances:
                if not event or (old["last_event_at"] and event_at == date(old["last_event_at"])):
                    return _conflict(connection, transaction_id, "UNVERSIONED_SNAPSHOT_CHANGE")
                if event_at < date(old["last_observed_at"]):
                    return _conflict(connection, transaction_id, "EVENT_PRECEDES_OBSERVATION")
    fields = {key: (old[key] if old else None) for key in PROJECTION_COLUMNS} if deletion else {key: snapshot[key] for key in PROJECTION_COLUMNS}
    fields.update(transaction_id=transaction_id, is_deleted=int(deletion),
                  last_event_at=stamp(event_at) if event_at else (old["last_event_at"] if old else None),
                  last_event_type=event["event_type"] if event else (old["last_event_type"] if old else None),
                  last_observed_at=stamp(observed_at))
    columns = tuple(fields)
    connection.execute("INSERT INTO transactions (" + ",".join(columns) + ") VALUES (" +
        ",".join("?" for _ in columns) + ") ON CONFLICT(transaction_id) DO UPDATE SET " +
        ",".join(key + "=excluded." + key for key in columns if key != "transaction_id"), tuple(fields.values()))
    return None


def _complete_event(connection, event_id, status, conflict=None):
    connection.execute("UPDATE webhook_events SET status=?,next_attempt_at=NULL,last_error=?,attention_required=? WHERE event_id=?",
                       (status, conflict, int(bool(conflict)), event_id))


def event_ready(connection, event_id, now):
    row = connection.execute("SELECT status,next_attempt_at FROM webhook_events WHERE event_id=?", (event_id,)).fetchone()
    return bool(row and row["status"] in ("PENDING", "FAILED_RETRYABLE") and
                (not row["next_attempt_at"] or date(row["next_attempt_at"]) <= now))


class FinanceWorker:
    def __init__(self, connection_factory, client_factory, clock=utcnow, poll_seconds=2, report_generator=None):
        self.connection_factory = connection_factory
        self.client_factory = client_factory
        self.clock = clock
        self.poll_seconds = poll_seconds
        self.client = None
        self.auth_paused = False
        self.initialized = False
        self.stopping = threading.Event()
        self.thread = None
        self.report_generator = report_generator

    def start(self):
        self.thread = threading.Thread(target=self._run, name="nico-finance-worker", daemon=True)
        self.thread.start()

    def stop(self):
        self.stopping.set()
        if self.thread:
            self.thread.join(timeout=15)
        if self.client and (not self.thread or not self.thread.is_alive()):
            close = getattr(self.client, "close", None)
            if close:
                close()

    def _run(self):
        failures = 0
        while not self.stopping.is_set():
            try:
                self.run_once()
                failures = 0
            except Exception:
                # No exception payloads, credentials, or transaction data in logs.
                LOG.error("FINANCE_WORKER_UNAVAILABLE")
                failures += 1
            self.stopping.wait(min(60, 5 * 2 ** min(failures - 1, 4)) if failures else self.poll_seconds)

    def initialize(self):
        ensure_schema(self.connection_factory, self.clock())
        self.client = self.client_factory()
        self.auth_paused = self.client is None
        self.initialized = True
        self._control("AUTH_REQUIRED" if self.auth_paused else "RUNNING")

    def _control(self, status):
        with transaction(self.connection_factory) as connection:
            connection.execute("UPDATE worker_control SET status=?,updated_at=? WHERE singleton=1", (status, stamp(self.clock())))

    def _read(self, sql, parameters=()):
        connection = self.connection_factory()
        connection.row_factory = sqlite3.Row
        try:
            return [dict(row) for row in connection.execute(sql, parameters)]
        finally:
            connection.close()

    def run_once(self):
        if not self.initialized:
            self.initialize()
        now = stamp(self.clock())
        events = self._read("""SELECT * FROM webhook_events WHERE status IN ('PENDING','FAILED_RETRYABLE')
            AND (next_attempt_at IS NULL OR next_attempt_at<=?) AND (?=0 OR event_type IN ('PING','TRANSACTION_DELETED'))
            ORDER BY event_created_at,event_id LIMIT ?""", (now, int(self.auth_paused), BATCH_SIZE))
        processed = 0
        for event in events:
            if self.stopping.is_set():
                break
            if event["event_type"] not in ("PING", "TRANSACTION_DELETED") and self.auth_paused:
                continue
            self.process_event(event)
            processed += 1
        checkpoint = self._read("SELECT * FROM reconciliation_checkpoint WHERE singleton=1")[0]
        reconciled = False
        if (not self.stopping.is_set() and not self.auth_paused and checkpoint["next_attempt_at"]
                and date(checkpoint["next_attempt_at"]) <= self.clock()):
            self.reconcile(checkpoint)
            reconciled = True
        if not self.stopping.is_set() and self.report_generator is not None:
            try:
                self.report_generator()
            except Exception:
                LOG.error("DAILY_REPORT_UNAVAILABLE")
        return {"events_attempted": processed, "reconciliation_attempted": reconciled}

    def process_event(self, event):
        snapshot = None
        try:
            # Resolve already-known ordering/deletion conflicts without an API call.
            with transaction(self.connection_factory) as connection:
                if not event_ready(connection, event["event_id"], self.clock()):
                    return
                if event["event_type"] == "PING":
                    _complete_event(connection, event["event_id"], "IGNORED")
                    return
                old = connection.execute("SELECT * FROM transactions WHERE transaction_id=?", (event["transaction_id"],)).fetchone()
                conflict = event_conflict(old, event)
                if conflict:
                    _conflict(connection, event["transaction_id"], conflict)
                    _complete_event(connection, event["event_id"], "PROCESSED", conflict)
                    return
            if event["event_type"] not in ("PING", "TRANSACTION_DELETED"):
                snapshot = normalize_transaction(self.client.transaction(event["transaction_id"]))
                if snapshot["transaction_id"] != event["transaction_id"]:
                    raise UpFailure("TRANSACTION_RESPONSE_MISMATCH", "permanent")
                if event["event_type"] == "TRANSACTION_SETTLED" and snapshot["up_status"] != "SETTLED":
                    raise UpFailure("SETTLEMENT_NOT_VISIBLE")
            with transaction(self.connection_factory) as connection:
                if not event_ready(connection, event["event_id"], self.clock()):
                    return
                conflict = project(connection, snapshot, self.clock(), event)
                _complete_event(connection, event["event_id"], "PROCESSED", conflict)
        except UpFailure as error:
            if error.kind == "not_found":
                error = UpFailure("TRANSACTION_404_UNRESOLVED", "retryable", 404)
            with transaction(self.connection_factory) as connection:
                row = connection.execute("SELECT status,retry_count FROM webhook_events WHERE event_id=?", (event["event_id"],)).fetchone()
                if row and row["status"] in ("PENDING", "FAILED_RETRYABLE"):
                    count = row["retry_count"] + 1
                    status = "FAILED_FATAL" if error.kind == "permanent" else "FAILED_RETRYABLE"
                    due = None if status == "FAILED_FATAL" else stamp(self.clock() + timedelta(seconds=delay(count, error.retry_after)))
                    connection.execute("UPDATE webhook_events SET status=?,retry_count=?,next_attempt_at=?,last_error=?,attention_required=? WHERE event_id=?",
                                       (status, count, due, error.code, int(count >= 10 or error.kind in ("auth", "permanent")), event["event_id"]))
            if error.kind == "auth":
                self.auth_paused = True
                self._control("AUTH_REQUIRED")

    def reconcile(self, checkpoint):
        started = self.clock()
        until = stamp(started)
        since = stamp(max(date(checkpoint["enabled_at"]), date(checkpoint["catchup_through"]) - timedelta(seconds=OVERLAP_SECONDS))) if checkpoint["catchup_through"] else checkpoint["enabled_at"]
        run_id = str(uuid4())
        try:
            accounts = []
            seen_accounts = set()
            for resource in self.client.accounts():
                try:
                    if resource["type"] != "accounts":
                        raise ValueError()
                    account_id = identifier(resource["id"])
                    balance, currency = money(resource["attributes"]["balance"])
                except (KeyError, TypeError, ValueError):
                    raise UpFailure("INVALID_ACCOUNT", "permanent") from None
                if account_id in seen_accounts:
                    raise UpFailure("DUPLICATE_ACCOUNT", "permanent")
                seen_accounts.add(account_id)
                accounts.append((account_id, balance, currency))
            observed = self.clock()
            snapshots = {}
            for resource in self.client.transactions(since, until):
                snapshot = normalize_transaction(resource)
                previous = snapshots.get(snapshot["transaction_id"])
                if previous and previous != snapshot:
                    raise UpFailure("DUPLICATE_TRANSACTION_CONFLICT", "retryable")
                snapshots[snapshot["transaction_id"]] = snapshot
            held = self._read("SELECT transaction_id FROM transactions WHERE up_status='HELD' AND is_deleted=0 ORDER BY last_observed_at,transaction_id LIMIT ?", (HELD_LIMIT + 1,))
            held_complete = len(held) <= HELD_LIMIT
            unresolved = []
            for row in held[:HELD_LIMIT]:
                if self.stopping.is_set():
                    raise UpFailure("WORKER_STOPPING")
                transaction_id = row["transaction_id"]
                if transaction_id in snapshots:
                    continue
                try:
                    snapshots[transaction_id] = normalize_transaction(self.client.transaction(transaction_id))
                except UpFailure as error:
                    if error.kind != "not_found":
                        raise
                    unresolved.append(transaction_id)
                    held_complete = False
            with transaction(self.connection_factory) as connection:
                conflicts = 0
                for snapshot in snapshots.values():
                    conflicts += int(bool(project(connection, snapshot, observed)))
                for transaction_id in unresolved:
                    _conflict(connection, transaction_id, "HELD_404_UNRESOLVED")
                    connection.execute("UPDATE transactions SET last_observed_at=? WHERE transaction_id=?", (stamp(observed), transaction_id))
                partial = bool(conflicts or not held_complete)
                connection.execute("INSERT INTO reconciliation_runs VALUES (?,?,?,?,?,?,?,?)",
                                   (run_id, until, stamp(self.clock()), "PARTIAL" if partial else "OBSERVED", since, until,
                                    "PROJECTION_REVIEW_REQUIRED" if partial else None, int(held_complete)))
                for account_id, balance, currency in accounts:
                    connection.execute("""INSERT INTO balance_snapshots
                        (run_id,account_id,balance_base_units,currency_code,observed_at,coverage)
                        VALUES (?,?,?,?,?,?)""", (run_id, account_id, balance, currency, stamp(observed),
                                               "PARTIAL_NO_BASELINE" if partial else "NO_BASELINE"))
                connection.execute("""UPDATE reconciliation_checkpoint SET catchup_through=?,last_completed_at=?,
                    next_attempt_at=?,failure_count=0,last_error=?,attention_required=? WHERE singleton=1""",
                    (until, stamp(self.clock()), stamp(started + timedelta(seconds=DAY_SECONDS)),
                     "PROJECTION_REVIEW_REQUIRED" if partial else None, int(partial)))
        except UpFailure as error:
            count = checkpoint["failure_count"] + 1
            due = None if error.kind == "permanent" else stamp(self.clock() + timedelta(seconds=delay(count, error.retry_after)))
            with transaction(self.connection_factory) as connection:
                connection.execute("INSERT INTO reconciliation_runs VALUES (?,?,?,?,?,?,?,?)",
                                   (run_id, until, stamp(self.clock()), "FAILED", since, until, error.code, 0))
                connection.execute("UPDATE reconciliation_checkpoint SET next_attempt_at=?,failure_count=?,last_error=?,attention_required=? WHERE singleton=1",
                                   (due, count, error.code, int(count >= 10 or error.kind in ("auth", "permanent"))))
            if error.kind == "auth":
                self.auth_paused = True
                self._control("AUTH_REQUIRED")
