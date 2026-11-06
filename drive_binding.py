"""Inactive concrete binding. No consent, output, env defaults or live runner.

Future grant receipt must establish original raw issuance scope. Runtime refresh
independently checks raw actual scope. This code cannot prove a historical grant
from arbitrary JSON; trusted secure bootstrap/binding remains an acceptance gate.
"""
import json
import re
from datetime import datetime, timezone, timedelta
from urllib.parse import urlsplit
from recurring_backup import (BackupFailure, DriveDelivery, make_backup_callback,
                              require, FILES, UPLOAD)

TOKEN = 'https://oauth2.googleapis.com/token'
ABOUT = 'https://www.googleapis.com/drive/v3/about'
SCOPE = 'https://www.googleapis.com/auth/drive.file'
ID = r'[A-Za-z0-9_-]+'


def identifier(value):
    require(type(value) is str and re.fullmatch(ID,value), 'BACKUP_CONFIG_REJECTED')
    return value


class GuardedSession:
    def __init__(self, raw, credentials, clock):
        require(type(credentials) is dict and set(credentials) ==
                {'client_id','client_secret','refresh_token','issued_scope'}, 'BACKUP_CONFIG_REJECTED')
        require(all(type(v) is str and 0 < len(v) <= 8192 for v in credentials.values())
                and credentials['issued_scope'] == SCOPE, 'BACKUP_CONFIG_REJECTED')
        self.raw, self.credentials, self.clock = raw, credentials, clock
        self.token, self.expires = None, None

    def refresh(self):
        self.token, self.expires = None, None  # Failed refresh invalidates admission.
        try:
            r = self.raw.request('POST',TOKEN,data={
                'grant_type':'refresh_token',
                **{k:self.credentials[k] for k in ('client_id','client_secret','refresh_token')}},
                timeout=(10,30),allow_redirects=False)
            require(r.status_code not in (400,401,403), 'DRIVE_AUTH_REQUIRED')
            require(r.status_code != 429 and r.status_code < 500, 'BACKUP_ADMISSION_RETRYABLE')
            require(r.status_code == 200 and type(r.content) is bytes
                    and len(r.content) <= 65536, 'DRIVE_AUTH_REQUIRED')
            t = json.loads(r.content)
            require(type(t) is dict and t.get('scope') == SCOPE
                    and type(t.get('access_token')) is str and 0 < len(t['access_token']) <= 16384
                    and type(t.get('expires_in')) is int and 30 < t['expires_in'] <= 86400
                    and t.get('token_type','').lower() == 'bearer', 'DRIVE_AUTH_REQUIRED')
            require('refresh_token' not in t or t['refresh_token'] == self.credentials['refresh_token'],
                    'BACKUP_SECURE_REBIND_REQUIRED')
            self.token = t['access_token']
            self.expires = self.clock()+timedelta(seconds=t['expires_in']-30)
        except BackupFailure:
            raise
        except (ValueError,TypeError,KeyError,RecursionError):
            raise BackupFailure('DRIVE_AUTH_REQUIRED') from None
        except Exception:
            raise BackupFailure('BACKUP_ADMISSION_RETRYABLE') from None

    def request(self, method, url, **kwargs):
        u = urlsplit(url)
        allowed = (method == 'GET' and (url == ABOUT or url == FILES or
                   (u.scheme == 'https' and u.netloc == 'www.googleapis.com'
                    and u.path.startswith('/drive/v3/files/')
                    and re.fullmatch(ID,u.path.removeprefix('/drive/v3/files/'))
                    and not u.query and not u.fragment))) or (
                   method == 'POST' and url == UPLOAD) or (
                   method == 'PUT' and u.scheme == 'https' and u.netloc == 'www.googleapis.com'
                   and u.path == '/upload/drive/v3/files' and not u.fragment and bool(u.query))
        require(allowed, 'BACKUP_ENDPOINT_REJECTED')
        require(kwargs.get('allow_redirects') is False, 'BACKUP_ENDPOINT_REJECTED')
        headers = dict(kwargs.pop('headers',{}))
        require(not any(k.lower() == 'authorization' for k in headers), 'BACKUP_ENDPOINT_REJECTED')
        if self.token is None or self.clock() >= self.expires:
            self.refresh()
        headers['Authorization'] = 'Bearer '+self.token
        try:
            return self.raw.request(method,url,headers=headers,**kwargs)
        except Exception:
            raise BackupFailure('DRIVE_REQUEST_UNCERTAIN') from None


class Admission:
    def __init__(self, session, account, writer, parent, folder, proof_child):
        require(type(account) is str and type(writer) is str and account != writer,
                'BACKUP_CONFIG_REJECTED')
        self.session, self.account, self.writer = session, account, writer
        self.parent, self.folder, self.proof = map(identifier,(parent,folder,proof_child))

    def get(self,url,fields):
        r = self.session.request('GET',url,params={'fields':fields},
                                 timeout=(10,30),allow_redirects=False)
        require(r.status_code not in (401,403), 'DRIVE_AUTH_REQUIRED')
        require(r.status_code != 429 and r.status_code < 500, 'BACKUP_ADMISSION_RETRYABLE')
        require(r.status_code == 200 and type(r.content) is bytes
                and len(r.content) <= 65536, 'BACKUP_ADMISSION_FAILED')
        body = json.loads(r.content)
        require(type(body) is dict, 'BACKUP_ADMISSION_FAILED')
        return body

    def private_file(self, f, parent):
        require(f.get('trashed') is False and f.get('parents') == [parent]
                and f.get('owners') == [{'emailAddress':self.account}], 'BACKUP_FOLDER_REJECTED')
        permissions = f.get('permissions')
        require(type(permissions) is list and len(permissions) == 2
                and all(type(p) is dict and p.get('type') == 'user' for p in permissions)
                and {(p.get('emailAddress'),p.get('role')) for p in permissions} ==
                {(self.account,'owner'),(self.writer,'writer')}, 'BACKUP_FOLDER_REJECTED')

    def __call__(self):
        try:
            self.session.refresh()
            a = self.get(ABOUT,'user(emailAddress),storageQuota(limit,usage)')
            require(a.get('user') == {'emailAddress':self.account}, 'BACKUP_ACCOUNT_REJECTED')
            quota = a.get('storageQuota',{})
            require(type(quota) is dict and all(type(quota.get(k)) is str and
                    re.fullmatch(r'\d+',quota[k]) for k in ('limit','usage')), 'BACKUP_QUOTA_REQUIRED')
            require(int(quota['limit'])-int(quota['usage']) >= 64*1024*1024+65536,
                    'BACKUP_QUOTA_REQUIRED')
            fields='id,mimeType,trashed,parents,owners(emailAddress),permissions(type,role,emailAddress)'
            folder = self.get(FILES+'/'+self.folder,fields)
            self.private_file(folder,self.parent)
            require(folder.get('id') == self.folder and folder.get('mimeType') ==
                    'application/vnd.google-apps.folder','BACKUP_FOLDER_REJECTED')
            child = self.get(FILES+'/'+self.proof,fields)
            self.private_file(child,self.folder)
            require(child.get('id') == self.proof and child.get('mimeType') !=
                    'application/vnd.google-apps.folder','BACKUP_FOLDER_REJECTED')
        except BackupFailure:
            raise
        except Exception:
            raise BackupFailure('BACKUP_ADMISSION_FAILED') from None


def configured_callback(config, *, enabled=False, cleanup_enabled=False,
                        expected_account=None, approved_writer=None, recovery_parent=None,
                        connection_factory=None, prepare=None, private_check=None,
                        source=None, root=None, commit=None, clock=None, day_zone=None,
                        session_factory=None):
    """No reading of os.environ. Future main passes only approved new config.

    Default returns None before inspecting config or constructing any session.
    Future binding must separately provide expected identities/recovery parent,
    trusted issuance receipt, actual current commit and private staging root.
    Caller owns shutdown/closing raw Session once worker is stopped.
    """
    if not enabled:
        return None
    raw = None
    try:
        text = config['NICO_FINANCE_BACKUP_OAUTH_JSON']
        require(type(text) is str and len(text.encode()) <= 65536,'BACKUP_CONFIG_REJECTED')
        credentials = json.loads(text)
        folder=identifier(config['NICO_FINANCE_BACKUP_FOLDER_ID'])
        proof=identifier(config['NICO_FINANCE_BACKUP_FOLDER_PROOF_ID'])
        require(all(callable(x) for x in (connection_factory,prepare,private_check,clock))
                and day_zone is not None,'BACKUP_CONFIG_REJECTED')
        if session_factory is None:
            import requests  # Existing pinned runtime dependency; no install.
            session_factory=requests.Session
        raw=session_factory()
        session=GuardedSession(raw,credentials,clock)
        admit=Admission(session,expected_account,approved_writer,recovery_parent,folder,proof)
        callback=make_backup_callback(enabled=True,cleanup_enabled=cleanup_enabled,
            connection_factory=connection_factory,prepare=prepare,private_check=private_check,
            source=source,root=root,commit=commit,clock=clock,day_zone=day_zone,
            delivery=DriveDelivery(session,folder),admit=admit)
        callback.close=raw.close
        return callback
    except Exception:
        if raw is not None:
            try:raw.close()
            except Exception:pass
        raise BackupFailure('BACKUP_CONFIG_REJECTED') from None
