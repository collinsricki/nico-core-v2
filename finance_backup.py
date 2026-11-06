"""Local SQLite recovery artifact preparation only; no upload or scheduler.

Production privacy guard targets Render/POSIX. Importing this module never opens
the app, database, environment, network or worker. No production restore exists.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import time
from datetime import date, datetime, timezone

VERSION = 1
MAX_BYTES = 64 * 1024 * 1024
RESERVE_BYTES = 32 * 1024 * 1024
REQUIRED = {
    'webhook_events': {'event_id', 'raw_body', 'payload_sha256'},
    'transactions': {'transaction_id', 'resource_json'},
    'reconciliation_runs': {'run_id', 'held_recheck_complete'},
    'reconciliation_checkpoint': {'singleton', 'catchup_through'},
    'balance_snapshots': {'run_id', 'observed_at'},
    'worker_control': {'singleton', 'status'},
    'budget_sources': {'source_hash', 'raw_json'},
    'budget_rows': {'budget_id', 'source_hash'},
    'budget_occurrences': {'occurrence_id'},
    'budget_rule_facts': {'rule_id'},
    'budget_reports': {'report_id', 'report_json', 'evidence_json'},
    'budget_cycle_runs': {'run_id', 'through_at'},
    'budget_cycle_accounts': {'run_id'},
    'budget_cycle_members': {'run_id', 'resource_json'},
    'budget_component_version': {'singleton', 'version'},
    'budget_daily_selection': {'singleton', 'selected_report_id'},
    'budget_daily_jobs': {'report_id'},
    'budget_balance_jobs': {'report_id', 'balance_batch_id'},
    'balance_refresh_batches': {'batch_id', 'accounts_json'},
    'balance_refresh_state': {'singleton', 'schema_version'},
    'frollo_sources': {'source_hash', 'original_bytes', 'byte_count'},
    'frollo_bindings': {'binding_hash', 'original_bytes'},
    'frollo_imports': {'source_hash'},
    'frollo_observations': {'observation_key'},
    'frollo_row_provenance': {'source_hash', 'original_record'},
}


class BackupUnavailable(ValueError):
    """Constant, secret-free error; never log underlying DB/path/row errors."""


def require(ok, code):
    if not ok:
        raise BackupUnavailable(code)


def stamp():
    return datetime.now(timezone.utc).isoformat()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def private(path, directory=False):
    require(os.name == 'posix', 'POSIX_PRIVACY_CHECK_REQUIRED')
    require(not any(p.is_symlink() for p in (path, *path.parents)), 'SYMLINK_REFUSED')
    s = path.stat()
    require((stat.S_ISDIR(s.st_mode) if directory else stat.S_ISREG(s.st_mode)), 'PRIVATE_TYPE_REQUIRED')
    require(s.st_uid == os.getuid() and not s.st_mode & 0o077, 'PRIVATE_MODE_REQUIRED')


def check(deadline):
    require(time.monotonic() < deadline, 'TIME_BUDGET_EXCEEDED')


def digest(path, deadline):
    h = hashlib.sha256()
    with path.open('rb') as f:
        while data := f.read(1024 * 1024):
            check(deadline)
            h.update(data)
    return h.hexdigest()


def connect_readonly(path, deadline):
    private(path)
    db = sqlite3.connect(path.absolute().as_uri() + '?mode=ro', uri=True, timeout=0.1)
    db.execute('PRAGMA query_only=ON')
    db.execute('PRAGMA trusted_schema=OFF')
    db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
    return db


def quote(name):
    require(re.fullmatch('[A-Za-z_][A-Za-z_0-9]{0,127}', name) is not None, 'UNSUPPORTED_SCHEMA_NAME')
    return '"' + name + '"'


def inventory(db, deadline):
    check(deadline)
    require(db.execute('PRAGMA user_version').fetchone()[0] == 2, 'CORE_SCHEMA_MISMATCH')
    schema = db.execute('SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name').fetchall()
    require(len(schema) <= 512, 'SCHEMA_LIMIT')
    tables = [x[1] for x in schema if x[0] == 'table' and not x[1].startswith('sqlite_')]
    require(len(tables) <= 128 and set(REQUIRED) <= set(tables), 'REQUIRED_TABLE_MISSING')
    for table, needed in REQUIRED.items():
        columns = {r[1] for r in db.execute('PRAGMA table_info(' + quote(table) + ')')}
        require(needed <= columns, 'REQUIRED_COLUMN_MISSING')
    require(db.execute('SELECT version FROM budget_component_version WHERE singleton=1').fetchone() == (1,), 'BUDGET_VERSION_MISMATCH')
    require(db.execute('SELECT schema_version FROM balance_refresh_state WHERE singleton=1').fetchone() == (1,), 'BALANCE_VERSION_MISMATCH')
    counts = {}
    for table in tables:
        check(deadline)
        counts[table] = db.execute('SELECT count(*) FROM ' + quote(table)).fetchone()[0]
    return {'core_schema': 2, 'budget_version': 1, 'balance_extension_version': 1,
            'schema_sha256': hashlib.sha256(canonical(schema)).hexdigest(), 'table_counts': counts}


def verify_database(path, expected, deadline):
    db = connect_readonly(path, deadline)
    try:
        db.execute('BEGIN')
        require(db.execute('PRAGMA integrity_check').fetchall() == [('ok',)], 'INTEGRITY_CHECK_FAILED')
        require(db.execute('PRAGMA foreign_key_check').fetchone() is None, 'FOREIGN_KEY_CHECK_FAILED')
        require(inventory(db, deadline) == expected, 'SNAPSHOT_INVENTORY_MISMATCH')
        # Read-only structural reader probes, not finance-calculation acceptance.
        require(db.execute('SELECT count(*) FROM budget_reports WHERE NOT json_valid(report_json) OR NOT json_valid(evidence_json)').fetchone()[0] == 0, 'REPORT_JSON_INVALID')
        require(db.execute('SELECT count(*) FROM budget_daily_selection s LEFT JOIN budget_reports r ON r.report_id=s.selected_report_id WHERE s.selected_report_id IS NOT NULL AND r.report_id IS NULL').fetchone()[0] == 0, 'SELECTED_REPORT_MISSING')
        for key, raw, length in db.execute('SELECT source_hash,original_bytes,byte_count FROM frollo_sources'):
            check(deadline)
            require(type(raw) is bytes and len(raw) == length and hashlib.sha256(raw).hexdigest() == key, 'IMPORTED_SOURCE_INTEGRITY_FAILED')
        for key, raw in db.execute('SELECT binding_hash,original_bytes FROM frollo_bindings'):
            check(deadline)
            require(type(raw) is bytes and hashlib.sha256(raw).hexdigest() == key, 'IMPORTED_BINDING_INTEGRITY_FAILED')
        # query_only and mode=ro both remain in force; no main/app import or worker.
        require(db.execute('PRAGMA query_only').fetchone() == (1,), 'READER_GUARD_FAILED')
    finally:
        db.close()


def exclusive_file(path):
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    return os.fdopen(fd, 'wb')


def fsync_file(path):
    # This is only called on the new staging artifact. Windows fsync requires
    # a writable descriptor; it never opens the authoritative source this way.
    with path.open('r+b') as f:
        os.fsync(f.fileno())


def fsync_directory(path):
    if os.name == 'posix':
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def _prepare_backup(source, staging_root, source_commit, slot, *, kind='DAILY', seconds=60,
                   max_bytes=MAX_BYTES):
    """One immutable slot. Incomplete slots block retry; nothing is auto-deleted.

    source_commit is caller-attested provenance, not a detected live revision.
    DAILY slots are YYYY-MM-DD; MONTHLY_ARCHIVE slots YYYY-MM. Each creates a
    distinct object. No destination, environment secret, network or schedule.
    """
    require(type(seconds) in (int, float) and 0 < seconds <= 120, 'INVALID_TIME_BUDGET')
    require(type(max_bytes) is int and 0 < max_bytes <= MAX_BYTES, 'INVALID_SIZE_BUDGET')
    require(isinstance(source_commit, str) and re.fullmatch('[0-9a-f]{40}', source_commit), 'SOURCE_COMMIT_REQUIRED')
    require(kind in ('DAILY', 'MONTHLY_ARCHIVE') and isinstance(slot, str), 'INVALID_SLOT')
    try:
        parsed = date.fromisoformat(slot if kind == 'DAILY' else slot + '-01')
        require(slot == (parsed.isoformat() if kind == 'DAILY' else parsed.isoformat()[:7]), 'INVALID_SLOT')
    except (TypeError, ValueError):
        raise BackupUnavailable('INVALID_SLOT') from None
    deadline = time.monotonic() + seconds
    source, staging_root = Path(source).absolute(), Path(staging_root).absolute()
    private(source.parent, True)
    private(source)
    private(staging_root, True)  # Existing private directory; no recursive mkdir.
    require(staging_root != source.parent and source not in staging_root.parents, 'INVALID_STAGING_ROOT')
    target = staging_root / (kind.lower() + '-' + slot)
    require(not target.is_symlink(), 'SYMLINK_REFUSED')
    if target.exists():
        private(target, True)
        manifest_path = target / 'manifest.json'
        require(manifest_path.is_file(), 'INCOMPLETE_SLOT_REQUIRES_RECONCILIATION')
        private(manifest_path)
        require(manifest_path.stat().st_size <= 65536, 'MANIFEST_SIZE_LIMIT')
        try:
            manifest = json.loads(manifest_path.read_bytes())
        except RecursionError:
            raise BackupUnavailable('BACKUP_PREPARATION_UNAVAILABLE') from None
        require(type(manifest) is dict, 'BACKUP_PREPARATION_UNAVAILABLE')
        require(manifest.get('source_commit') == source_commit and manifest.get('slot') == slot
                and manifest.get('kind') == kind and manifest.get('schema') == 'nico.sqlite-backup/v1', 'EXISTING_SLOT_CONFLICT')
        snapshot = target / 'snapshot.sqlite3'
        private(snapshot)
        require(snapshot.stat().st_size == manifest['byte_length'] <= max_bytes
                and digest(snapshot, deadline) == manifest['sha256'], 'EXISTING_SNAPSHOT_MISMATCH')
        verify_database(snapshot, manifest['inventory'], deadline)
        # An earlier attempt may have written valid manifest bytes but failed
        # its file fsync. Directory sync alone cannot repair that data durability.
        fsync_file(manifest_path)
        # Also repair directory-entry durability for slots made by an older
        # preparer or recovered after an uncertain completion. Refuse if either
        # sync fails; existence and valid bytes alone do not prove durability.
        fsync_directory(target)
        fsync_directory(staging_root)
        return {'state': 'VERIFIED_EXISTING_LOCAL_ONLY', 'artifact_directory': str(target),
                'manifest_sha256': digest(manifest_path, deadline), 'upload': 'DISABLED'}
    require(shutil.disk_usage(staging_root).free >= 2 * max_bytes + RESERVE_BYTES, 'INSUFFICIENT_STAGING_SPACE')
    target.mkdir(mode=0o700, exist_ok=False)  # Cooperative claim; never overwrite.
    private(target, True)
    snapshot, restore = target / 'snapshot.sqlite3', target / 'isolated-restore.sqlite3'
    started = stamp()
    src = connect_readonly(source, deadline)
    try:
        src.execute('BEGIN')
        # This first read establishes the WAL snapshot. A concurrent writer may
        # advance after it, but inventory and backup use this same read snapshot.
        expected = inventory(src, deadline)
        snapshot_acquired = stamp()
        page_size = src.execute('PRAGMA page_size').fetchone()[0]
        require(src.execute('PRAGMA page_count').fetchone()[0] * page_size <= max_bytes, 'SNAPSHOT_SIZE_LIMIT')
        with exclusive_file(snapshot):
            pass
        private(snapshot)
        dst = sqlite3.connect(snapshot, timeout=0.1)
        try:
            dst.execute('PRAGMA journal_mode=DELETE')
            dst.execute('PRAGMA synchronous=FULL')
            dst.execute('PRAGMA max_page_count=' + str(max_bytes // page_size))
            def progress(status, remaining, total):
                check(deadline)
                require(total * page_size <= max_bytes, 'SNAPSHOT_SIZE_LIMIT')
                require(shutil.disk_usage(target).free >= max_bytes + RESERVE_BYTES, 'INSUFFICIENT_STAGING_SPACE')
            src.backup(dst, pages=128, progress=progress, sleep=0.01)
        finally:
            dst.close()
    finally:
        src.close()
    private(snapshot)
    require(0 < snapshot.stat().st_size <= max_bytes, 'SNAPSHOT_SIZE_LIMIT')
    fsync_file(snapshot)
    verify_database(snapshot, expected, deadline)
    original_hash = digest(snapshot, deadline)
    # Copy into a distinct file, never the production path. No service boot,
    # worker, bank client, application import, migration or production restore.
    with snapshot.open('rb') as incoming, exclusive_file(restore) as outgoing:
        while chunk := incoming.read(1024 * 1024):
            check(deadline)
            outgoing.write(chunk)
        outgoing.flush()
        os.fsync(outgoing.fileno())
    private(restore)
    require(digest(restore, deadline) == original_hash, 'RESTORE_COPY_HASH_MISMATCH')
    verify_database(restore, expected, deadline)
    manifest = {'schema': 'nico.sqlite-backup/v1', 'component_version': VERSION,
        'source_commit': source_commit, 'source_commit_basis': 'CALLER_ATTESTED',
        'slot': slot, 'kind': kind, 'snapshot_started_at': started,
        'snapshot_acquired_by': snapshot_acquired, 'completed_at': stamp(),
        'snapshot_method': 'SQLITE_ONLINE_BACKUP_PINNED_READ_TRANSACTION',
        'byte_length': snapshot.stat().st_size, 'sha256': original_hash,
        'sqlite_version': sqlite3.sqlite_version, 'inventory': expected,
        'verification': {'integrity_check': 'ok', 'foreign_key_check': 'ok',
            'isolated_copy_hash': 'matched', 'isolated_copy_inventory': 'matched',
            'reader_probe': 'STRUCTURAL_READ_ONLY_NO_WORKER',
            'imported_source_hashes': 'verified', 'imported_binding_hashes': 'verified'},
        'custody': 'LOCAL_PRIVATE_STAGING_ONLY_NOT_OFFSITE', 'upload': 'DISABLED',
        'encryption': 'NO_CLIENT_SIDE_ENCRYPTION_CONFIGURED', 'deletion': 'DISABLED'}
    require(len(canonical(manifest)) <= 65536, 'MANIFEST_SIZE_LIMIT')
    with exclusive_file(target / 'manifest.json') as f:
        f.write(canonical(manifest) + b'\n')
        f.flush()
        os.fsync(f.fileno())
    private(target / 'manifest.json')
    fsync_directory(target)
    # Persist the slot's entry in its parent only after its own files/entries.
    # A completed manifest inside an unpersisted slot is not a durable backup.
    fsync_directory(staging_root)
    return {'state': 'VERIFIED_NEW_LOCAL_ONLY', 'artifact_directory': str(target),
            'manifest_sha256': digest(target / 'manifest.json', deadline), 'upload': 'DISABLED'}


def prepare_backup(source, staging_root, source_commit, slot, *, kind='DAILY', seconds=60,
                   max_bytes=MAX_BYTES):
    """Safe public entry point; error detail never contains rows or SQLite text."""
    try:
        return _prepare_backup(source, staging_root, source_commit, slot,
                               kind=kind, seconds=seconds, max_bytes=max_bytes)
    except BackupUnavailable:
        raise
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError):
        raise BackupUnavailable('BACKUP_PREPARATION_UNAVAILABLE') from None
