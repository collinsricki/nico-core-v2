"""Default-disabled existing-worker binding; no credentials or live defaults.

Independent identity policy is deployment configuration, never public source.
Importing this module makes no filesystem, network or environment reads.
"""
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from finance_backup import prepare_backup, private
from drive_binding import configured_callback

LOG = logging.getLogger(__name__)


def build_callback(connection_factory, storage_root, config):
    """Return None whenever disabled or non-operational, with safe failure logs.

    config is explicit injection (future main passes its environment mapping).
    Reads only the enable switch while disabled. No existing Google credential
    is read or reused. No provider request occurs during construction.
    """
    try:
        enabled = config.get('NICO_FINANCE_BACKUP_ENABLED', '0')
        if enabled == '0':
            return None
        if enabled != '1':
            raise ValueError()
        policy = [config.get(k) for k in (
            'NICO_FINANCE_BACKUP_EXPECTED_ACCOUNT',
            'NICO_FINANCE_BACKUP_APPROVED_WRITER',
            'NICO_FINANCE_BACKUP_RECOVERY_PARENT_ID')]
        if not all(type(x) is str and bool(x) and x == x.strip() for x in policy):
            raise ValueError()
        if policy[0] == policy[1]:
            raise ValueError()
        commit = config.get('RENDER_GIT_COMMIT')
        if type(commit) is not str or re.fullmatch(r'[0-9a-f]{40}', commit) is None:
            LOG.error('BACKUP_INVALID_SOURCE_REVISION')
            raise ValueError()
        ingress = Path(storage_root) / 'nico-ingress'
        source = ingress / 'events.sqlite3'
        private(ingress, True)
        private(source)
        staging = ingress / 'finance-backup-staging'
        staging.mkdir(mode=0o700, exist_ok=True)
        private(staging, True)
        return configured_callback(config, enabled=True, cleanup_enabled=True,
            expected_account=policy[0], approved_writer=policy[1],
            recovery_parent=policy[2], connection_factory=connection_factory,
            prepare=prepare_backup, private_check=private, source=source,
            root=staging, commit=commit,
            clock=lambda: datetime.now(timezone.utc),
            day_zone=ZoneInfo('Australia/Melbourne'))
    except Exception:
        # Do not include exception text, configuration values or traceback.
        LOG.error('BACKUP_CONFIGURATION_UNAVAILABLE')
        return None
