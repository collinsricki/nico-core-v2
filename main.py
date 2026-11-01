"""Authenticated, durable Up event intake only; no financial processing."""

import asyncio
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
from uuid import UUID

from fastapi import FastAPI, HTTPException, Request
from starlette.concurrency import run_in_threadpool
from starlette.requests import ClientDisconnect

MAX_BODY_BYTES = 64 * 1024
BODY_TIMEOUT_SECONDS = 5
DB_BUSY_SECONDS = 2
EVENT_TYPES = {"PING", "TRANSACTION_CREATED", "TRANSACTION_SETTLED", "TRANSACTION_DELETED"}
TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})\Z")


class EventConflict(Exception):
    pass


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def _invalid_constant(value):
    raise ValueError("Invalid JSON constant")


def _resource(value, resource_type):
    if not isinstance(value, dict) or value.get("type") != resource_type:
        raise ValueError("Invalid resource")
    identifier = value.get("id")
    if not isinstance(identifier, str) or str(UUID(identifier)) != identifier:
        raise ValueError("Invalid resource ID")
    return identifier


def parse_event(raw):
    try:
        payload = json.loads(raw.decode("utf-8"), object_pairs_hook=_object, parse_constant=_invalid_constant)
        data = payload["data"]
        event_id = _resource(data, "webhook-events")
        attributes = data["attributes"]
        event_type = attributes["eventType"]
        if not isinstance(event_type, str) or event_type not in EVENT_TYPES:
            raise ValueError("Unsupported event")
        created_at = attributes["createdAt"]
        if not isinstance(created_at, str) or not TIMESTAMP.fullmatch(created_at):
            raise ValueError("Invalid timestamp")
        created_at = datetime.fromisoformat(created_at).astimezone(timezone.utc).isoformat()
        relationships = data["relationships"]
        webhook_id = _resource(relationships["webhook"]["data"], "webhooks")
        transaction_id = None
        if event_type != "PING" or "transaction" in relationships:
            transaction_id = _resource(relationships["transaction"]["data"], "transactions")
        return event_id, event_type, created_at, webhook_id, transaction_id
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError, OverflowError):
        raise HTTPException(400, "Malformed or unsupported webhook event") from None


def _private_path(path, directory=False):
    info = path.lstat()
    expected = stat.S_ISDIR if directory else stat.S_ISREG
    if path.is_symlink() or not expected(info.st_mode):
        raise OSError("Unsafe storage path")
    if os.name == "posix" and (info.st_uid != os.geteuid() or info.st_mode & 0o077):
        raise OSError("Storage permissions must be private")


def store_event(storage_root, require_mount, event, raw):
    """Commit before returning; retries never overwrite existing event bytes."""
    if not storage_root.is_dir() or storage_root.is_symlink():
        raise OSError("Persistent storage unavailable")
    if require_mount and (storage_root != Path("/data") or not os.path.ismount(storage_root)):
        raise OSError("Persistent /data mount required")
    directory = storage_root / "nico-ingress"
    directory.mkdir(mode=0o700, exist_ok=True)
    _private_path(directory, directory=True)
    database = directory / "events.sqlite3"
    try:
        fd = os.open(database, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        pass
    else:
        os.close(fd)
    _private_path(database)
    connection = sqlite3.connect(database, timeout=DB_BUSY_SECONDS, isolation_level=None)
    try:
        if connection.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal":
            raise sqlite3.OperationalError("WAL unavailable")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA trusted_schema=OFF")
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("""
            CREATE TABLE IF NOT EXISTS webhook_events (
                event_id TEXT PRIMARY KEY NOT NULL,
                event_type TEXT NOT NULL CHECK(event_type IN
                    ('PING','TRANSACTION_CREATED','TRANSACTION_SETTLED','TRANSACTION_DELETED')),
                event_created_at TEXT NOT NULL,
                webhook_id TEXT NOT NULL,
                transaction_id TEXT,
                received_at TEXT NOT NULL,
                raw_body BLOB NOT NULL,
                payload_sha256 TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING' CHECK(status = 'PENDING')
            )
        """)
        connection.execute("CREATE INDEX IF NOT EXISTS webhook_events_transaction ON webhook_events(transaction_id)")
        previous = connection.execute("SELECT raw_body FROM webhook_events WHERE event_id = ?", (event[0],)).fetchone()
        if previous is not None:
            if previous[0] != raw:
                raise EventConflict()
            duplicate = True
        else:
            connection.execute("""
                INSERT INTO webhook_events
                    (event_id,event_type,event_created_at,webhook_id,transaction_id,
                     received_at,raw_body,payload_sha256)
                VALUES (?,?,?,?,?,?,?,?)
            """, (*event, datetime.now(timezone.utc).isoformat(), raw, hashlib.sha256(raw).hexdigest()))
            duplicate = False
        connection.commit()
        return duplicate
    except BaseException:
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
        connection.close()


def create_app(*, storage_root=Path("/data"), require_mount=True, secret_provider=None):
    """Storage overrides are explicit test injection, never environment fallbacks."""
    application = FastAPI(title="Nico Mercer Core")
    write_lock = asyncio.Lock()

    @application.get("/")
    def read_root():
        return {"status": "alive", "financial_processing": "not_implemented"}

    @application.post("/webhooks/upbank")
    async def handle_upbank(request: Request):
        secret = secret_provider() if secret_provider else os.environ.get("UP_WEBHOOK_SECRET")
        if not isinstance(secret, str) or not secret.strip():
            raise HTTPException(503, "Webhook intake not configured")
        signatures = request.headers.getlist("x-up-authenticity-signature")
        if len(signatures) != 1 or not re.fullmatch(r"[0-9a-fA-F]{64}", signatures[0]):
            raise HTTPException(401, "Invalid webhook signature")
        if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
            raise HTTPException(415, "Expected application/json")
        raw = bytearray()
        try:
            async with asyncio.timeout(BODY_TIMEOUT_SECONDS):
                async for chunk in request.stream():
                    if len(raw) + len(chunk) > MAX_BODY_BYTES:
                        raise HTTPException(413, "Webhook body too large")
                    raw.extend(chunk)
        except TimeoutError:
            raise HTTPException(408, "Webhook body timed out") from None
        except ClientDisconnect:
            raise HTTPException(400, "Incomplete webhook body") from None
        raw = bytes(raw)
        expected = hmac.digest(secret.encode("utf-8"), raw, "sha256")
        if not hmac.compare_digest(expected, bytes.fromhex(signatures[0])):
            raise HTTPException(401, "Invalid webhook signature")
        event = parse_event(raw)
        try:
            await asyncio.wait_for(write_lock.acquire(), timeout=DB_BUSY_SECONDS)
        except TimeoutError:
            raise HTTPException(503, "Webhook storage busy") from None
        try:
            duplicate = await run_in_threadpool(store_event, storage_root, require_mount, event, raw)
        except EventConflict:
            raise HTTPException(409, "Event ID conflicts with retained content") from None
        except (OSError, sqlite3.Error):
            raise HTTPException(503, "Webhook storage unavailable") from None
        finally:
            write_lock.release()
        return {"status": "PENDING", "duplicate": duplicate}

    return application


app = create_app()
