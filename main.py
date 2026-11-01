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
