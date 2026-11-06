"""Inactive backup delivery component. No CLI, credentials, factory or deletion.

The future binding must supply a reviewed, scope-guarded OAuth session and
verified account/folder/quota admission. Importing this module does no work.
Only backup bookkeeping is written; snapshot preparation remains unchanged.
"""
import hashlib
import json
import re
import os
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit
from datetime import datetime, timezone, timedelta

FILES = 'https://www.googleapis.com/drive/v3/files'
UPLOAD = 'https://www.googleapis.com/upload/drive/v3/files'
FIELDS = 'id,size,sha256Checksum,parents,appProperties,trashed'


class BackupFailure(Exception):
    def __init__(self, code='BACKUP_UNAVAILABLE'):
        super().__init__(code)


def require(value, code='BACKUP_UNAVAILABLE'):
    if not value:
        raise BackupFailure(code)


class DriveDelivery:
    """No auth implementation. Session must have guarded refresh before binding.

    HTTP errors are constant; no URLs/response bodies/tokens in error messages.
    A transport exception during create is uncertain, never blindly retried.
    """
    def __init__(self, session, folder):
        require(type(folder) is str and re.fullmatch(r'[A-Za-z0-9_-]+', folder))
        self.session, self.folder = session, folder

    def request(self, method, url, **kwargs):
        try:
            r = self.session.request(method, url, timeout=(10, 60),
                                     allow_redirects=False, **kwargs)
            require(r.status_code not in (401,403), 'DRIVE_AUTH_REQUIRED')
            require(r.status_code != 429 and r.status_code < 500, 'DRIVE_REQUEST_RETRYABLE')
            require(r.status_code in (200, 201), 'DRIVE_REQUEST_FAILED')
            return r
        except BackupFailure:
            raise
        except Exception:
            raise BackupFailure('DRIVE_REQUEST_UNCERTAIN') from None

    def find(self, key):
        require(re.fullmatch(r'[0-9a-f]{64}', key))
        q = ("trashed=false and '" + self.folder + "' in parents and "
             "appProperties has { key='backup_key' and value='" + key + "' }")
        found, token, seen = [], None, set()
        while True:
            params = {'q': q, 'fields': 'files(' + FIELDS + '),nextPageToken',
                      'pageSize': 100}
            if token:
                params['pageToken'] = token
            body = self.request('GET', FILES, params=params).json()
            require(type(body) is dict and type(body.get('files')) is list)
            found.extend(body['files'])
            require(len(found) <= 1, 'DRIVE_DUPLICATE_IDENTITY')
            token = body.get('nextPageToken')
            if not token:
                return found
            require(type(token) is str and token not in seen and len(seen) < 100)
            seen.add(token)

    def create(self, path, key, sha):
        size = path.stat().st_size
        require(0 < size <= 64 * 1024 * 1024)
        metadata = {'name': key + path.suffix, 'parents': [self.folder],
                    'appProperties': {'backup_key': key, 'sha256': sha}}
        r = self.request('POST', UPLOAD,
                         params={'uploadType': 'resumable', 'fields': FIELDS},
                         headers={'X-Upload-Content-Length': str(size),
                                  'X-Upload-Content-Type': 'application/octet-stream'},
                         json=metadata)
        location = r.headers.get('Location', '')
        u = urlsplit(location)
        require(u.scheme == 'https' and u.netloc == 'www.googleapis.com'
                and u.path == '/upload/drive/v3/files' and not u.fragment,
                'DRIVE_UPLOAD_LOCATION_REJECTED')
        with path.open('rb') as data:
            self.request('PUT', location, headers={'Content-Length': str(size)}, data=data)
        # Reconcile through provider metadata, not just the create response.
        return self.find(key)

    def verify(self, files, key, size, sha):
        require(len(files) == 1, 'DRIVE_DELIVERY_UNCONFIRMED')
        f = files[0]
        require(type(f) is dict and type(f.get('id')) is str
                and re.fullmatch(r'[A-Za-z0-9_-]+', f['id'])
                and f.get('trashed') is False
                and f.get('parents') == [self.folder]
                and f.get('size') == str(size)
                and f.get('sha256Checksum') == sha
                and f.get('appProperties', {}).get('backup_key') == key,
                'DRIVE_DELIVERY_MISMATCH')
        return f['id']


class RecurringBackup:
    """Disabled unless an explicit future factory supplies all capabilities.

    Single existing worker owns execution. No independent process/lock framework.
    Existing source directory and inputs must satisfy the proven preparer's guard.
    Admission is a future binding contract, not proof supplied by booleans here.
    No local or Drive deletion is implemented. Two retained slots then block.
    """
    def __init__(self, connection_factory, prepare, delivery, admit, source,
                 root, commit, *, enabled=False, cleanup_enabled=False,
                 private_check=None, clock=None):
        self.cf, self.prepare, self.drive, self.admit = connection_factory, prepare, delivery, admit
        self.source, self.root, self.commit = source, Path(root), commit
        self.enabled = enabled
        self.cleanup_enabled, self.private_check = cleanup_enabled, private_check
        self.clock = clock or (lambda:datetime.now(timezone.utc))

    @contextmanager
    def db(self):
        c = self.cf()
        try:
            c.execute('PRAGMA synchronous=FULL')
            c.execute('BEGIN IMMEDIATE')
            c.execute('CREATE TABLE IF NOT EXISTS backup_delivery_state '
                      '(singleton INTEGER PRIMARY KEY CHECK(singleton=1), last_slot TEXT, '
                      'pending_slot TEXT, status TEXT NOT NULL, error TEXT,next_attempt_at TEXT)')
            c.execute("INSERT OR IGNORE INTO backup_delivery_state VALUES(1,NULL,NULL,'READY',NULL,NULL)")
            c.execute('CREATE TABLE IF NOT EXISTS backup_delivery_objects '
                      '(slot TEXT,name TEXT,sha TEXT,size INTEGER,phase TEXT,file_id TEXT, '
                      'PRIMARY KEY(slot,name))')
            c.execute('CREATE TABLE IF NOT EXISTS backup_delivery_cleanup '
                      '(slot TEXT PRIMARY KEY,state TEXT NOT NULL)')
            yield c
            c.commit()
        except Exception:
            c.rollback()
            raise
        finally:
            c.close()

    def run(self, day):
        if not self.enabled:
            return {'status': 'DISABLED'}
        from datetime import date
        require(type(day) is str and date.fromisoformat(day).isoformat() == day)
        try:
            return self._run(day)
        except Exception as e:
            code = str(e) if type(e) is BackupFailure else 'BACKUP_UNAVAILABLE'
            retry = code in {'DRIVE_REQUEST_UNCERTAIN','DRIVE_REQUEST_RETRYABLE','BACKUP_ADMISSION_RETRYABLE'}
            due = (self.clock().astimezone(timezone.utc)+timedelta(minutes=5)).isoformat() if retry else None
            # Only our fixed codes are retained; never arbitrary exception text.
            with self.db() as c:
                c.execute("UPDATE backup_delivery_state SET status='FAILED',error=?,next_attempt_at=? WHERE singleton=1", (code,due))
            return {'status': 'FAILED', 'error': code}

    def _run(self, day):
        with self.db() as c:
            last, pending, status, error, due = c.execute(
                'SELECT last_slot,pending_slot,status,error,next_attempt_at FROM backup_delivery_state WHERE singleton=1').fetchone()
            if status == 'FAILED':
                if due is None:
                    return {'status':'BLOCKED','error':error}
                if self.clock().astimezone(timezone.utc) < datetime.fromisoformat(due):
                    return {'status':'WAITING','error':error}
            if pending is None and last is not None and day <= last:
                return {'status': 'ALREADY_DELIVERED'}
            slot = pending or day  # Resolve old pending slot before any new snapshot.
        # Must establish actual refresh scopes, identity, folder/child proof,
        # local free space and sufficient Drive quota before snapshot/upload.
        self.admit()
        if last and self.cleanup_enabled:
            self.cleanup(last, pending)
        slots = list(self.root.glob('daily-*'))
        require(all(p.is_dir() and not p.is_symlink() for p in slots))
        target = self.root / ('daily-' + slot)
        require(target.exists() or len(slots) < 2, 'BACKUP_LOCAL_CAPACITY_BLOCKED')
        with self.db() as c:
            c.execute("UPDATE backup_delivery_state SET pending_slot=?,status='PENDING',error=NULL,next_attempt_at=NULL WHERE singleton=1", (slot,))
        prepared = self.prepare(self.source, self.root, self.commit, slot)
        require(Path(prepared['artifact_directory']) == target)
        for name in ('snapshot.sqlite3', 'manifest.json'):
            path = target / name
            require(path.is_file() and not path.is_symlink())
            size = path.stat().st_size
            require(0 < size <= (64 * 1024 * 1024 if name.endswith('sqlite3') else 65536))
            with path.open('rb') as f:
                sha = hashlib.file_digest(f, 'sha256').hexdigest()
            key = hashlib.sha256((slot + '/' + name + '/' + sha).encode()).hexdigest()
            with self.db() as c:
                row = c.execute('SELECT sha,size,phase,file_id FROM backup_delivery_objects WHERE slot=? AND name=?', (slot,name)).fetchone()
                if row:
                    require(row[:2] == (sha,size), 'BACKUP_LOCAL_OBJECT_CHANGED')
                else:
                    c.execute("INSERT INTO backup_delivery_objects VALUES(?,?,?,?, 'NEW', NULL)", (slot,name,sha,size))
            found = self.drive.find(key)
            if not found:
                require(row is None or row[2] == 'NEW', 'BACKUP_UPLOAD_OUTCOME_UNCERTAIN')
                # Commit intent before network. Even a crash before sending stays
                # uncertain; lack of a listing match never authorizes recreation.
                with self.db() as c:
                    c.execute("UPDATE backup_delivery_objects SET phase='CREATING' WHERE slot=? AND name=?", (slot,name))
                found = self.drive.create(path,key,sha)
            file_id = self.drive.verify(found,key,size,sha)
            with self.db() as c:
                if row and row[3]:
                    require(file_id == row[3], 'BACKUP_REMOTE_OBJECT_CHANGED')
                c.execute("UPDATE backup_delivery_objects SET phase='VERIFIED',file_id=? WHERE slot=? AND name=?", (file_id,slot,name))
        # Full object receipts and daily checkpoint commit together.
        with self.db() as c:
            require(c.execute("SELECT count(*) FROM backup_delivery_objects WHERE slot=? AND phase='VERIFIED'", (slot,)).fetchone()[0] == 2)
            c.execute("UPDATE backup_delivery_state SET last_slot=?,pending_slot=NULL,status='DELIVERED',error=NULL,next_attempt_at=NULL WHERE singleton=1", (slot,))
        if self.cleanup_enabled:
            self.cleanup(slot, None)
        return {'status': 'DELIVERED', 'slot': slot}

    def cleanup(self, keep, pending):
        """Approved future local-only retention. Never follows arbitrary paths.

        Disabled by default. Durable intent makes an interrupted three-file
        cleanup resumable. No recursive directory deletion, no Drive deletion.
        private_check must be the proven POSIX preparer guard in real binding.
        """
        require(self.cleanup_enabled and self.private_check is not None,
                'BACKUP_CLEANUP_NOT_ADMITTED')
        self.private_check(self.root, True)
        with self.db() as c:
            old = c.execute("SELECT DISTINCT slot FROM backup_delivery_objects WHERE slot<?", (keep,)).fetchall()
        for (slot,) in old:
            require(re.fullmatch(r'\d{4}-\d{2}-\d{2}', slot))
            if slot == pending:
                continue
            target = self.root / ('daily-' + slot)
            with self.db() as c:
                rows = c.execute('SELECT name,sha,phase,file_id FROM backup_delivery_objects WHERE slot=?', (slot,)).fetchall()
                require(len(rows) == 2 and {r[0] for r in rows} == {'snapshot.sqlite3','manifest.json'}
                        and all(r[2] == 'VERIFIED' and r[3] for r in rows), 'BACKUP_CLEANUP_RECEIPT_MISSING')
                intent = c.execute('SELECT state FROM backup_delivery_cleanup WHERE slot=?', (slot,)).fetchone()
            if intent == ('CLEANED',):
                require(not target.exists() and not target.is_symlink(), 'BACKUP_CLEANUP_CONFLICT')
                continue
            hashes = {r[0]:r[1] for r in rows}
            hashes['isolated-restore.sqlite3'] = hashes['snapshot.sqlite3']
            if target.exists():
                self.private_check(target, True)
                names = {p.name for p in target.iterdir()}
                require(names <= hashes.keys() and (intent or names == hashes.keys()), 'BACKUP_CLEANUP_CONTENT_CONFLICT')
                for name in names:
                    p = target / name
                    self.private_check(p)
                    require(p.stat().st_nlink == 1, 'BACKUP_CLEANUP_LINK_REJECTED')
                    with p.open('rb') as f:
                        require(hashlib.file_digest(f, 'sha256').hexdigest() == hashes[name], 'BACKUP_CLEANUP_HASH_MISMATCH')
            else:
                require(intent and not target.is_symlink(), 'BACKUP_CLEANUP_SLOT_MISSING')
            with self.db() as c:
                c.execute("INSERT OR IGNORE INTO backup_delivery_cleanup VALUES(?,'CLEANING')", (slot,))
            if target.exists():
                # All remaining entries were validated; a crash leaves intent
                # and requires the same receipt/hash checks on the next call.
                for name in ('isolated-restore.sqlite3','snapshot.sqlite3','manifest.json'):
                    p = target / name
                    if p.exists(): p.unlink()
                target.rmdir()
                fd = os.open(self.root, os.O_RDONLY)
                try: os.fsync(fd)
                finally: os.close(fd)
            with self.db() as c:
                c.execute("UPDATE backup_delivery_cleanup SET state='CLEANED' WHERE slot=?", (slot,))


def make_backup_callback(*, enabled=False, cleanup_enabled=False,
                         connection_factory=None, prepare=None, delivery=None,
                         admit=None, source=None, root=None, commit=None,
                         day_zone=None, clock=None, private_check=None):
    """Explicit factory only; no env/credential access and no live default.

    Future Render binding: NICO_FINANCE_BACKUP_OAUTH_JSON (new, backup-only),
    NICO_FINANCE_BACKUP_FOLDER_ID (app-authorized child, not Recovery parent).
    Neither is read here. Supply ZoneInfo('Australia/Melbourne') in reviewed
    Linux wiring. One slot per local date, first worker tick after local midnight;
    no bank schedules change. Refresh/folder/quota admission remains mandatory.
    """
    if not enabled:
        return None
    require(all(callable(x) for x in (connection_factory,prepare,admit,clock,private_check))
            and delivery is not None and day_zone is not None)
    job = RecurringBackup(connection_factory,prepare,delivery,admit,source,root,commit,
                          enabled=True,cleanup_enabled=cleanup_enabled,
                          private_check=private_check,clock=clock)
    def callback():
        now = clock()
        require(now.tzinfo is not None and now.utcoffset() is not None)
        return job.run(now.astimezone(day_zone).date().isoformat())
    return callback
