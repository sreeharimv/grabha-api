"""
Grabha — Instagram Cookie Refresh
Uses yt-dlp to extract Instagram cookies from Firefox and pushes to the home server.

Requirements:
    pip install yt-dlp paramiko

Run manually:  python refresh_cookies.py
Scheduled:     via Task Scheduler (see schedule_task.bat)
"""

import os
import sys
import logging
import datetime
import subprocess
import tempfile
from pathlib import Path

import paramiko

# ── Config ────────────────────────────────────────────────────────────────────
SERVER_IP      = '100.73.205.101'
SERVER_USER    = 'sreeh007'
SSH_KEY        = '/home/sreeh007/.ssh/anjaneya_key'
REMOTE_COOKIE  = '/home/sreeh007/grabha-cookies/instagram.txt'
CONTAINER_NAME = 'grabha-api'
LOG_FILE       = Path(__file__).parent / 'cookie_refresh.log'
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)
log = logging.getLogger(__name__)


def extract_cookies() -> tuple:
    """Use yt-dlp to extract Instagram cookies from Firefox."""
    tmp = tempfile.mktemp(suffix='.txt')
    result = subprocess.run(
        [
            'yt-dlp',
            '--cookies-from-browser', 'firefox',
            '--cookies', tmp,
            '--skip-download',
            'https://www.instagram.com',
        ],
        capture_output=True,
        text=True,
    )

    if not os.path.exists(tmp):
        raise RuntimeError(f'yt-dlp failed to create cookies file:\n{result.stderr}')

    with open(tmp, 'r', encoding='utf-8') as f:
        content = f.read()
    os.unlink(tmp)

    # Count instagram cookie lines
    count = sum(1 for line in content.splitlines()
                if 'instagram.com' in line and not line.startswith('#'))

    if count == 0:
        raise RuntimeError('No Instagram cookies found — are you logged in to Instagram in Firefox?')

    log.info('extracted %d Instagram cookies from Firefox', count)
    return content, count


def push_to_server(cookie_text: str):
    key = paramiko.Ed25519Key.from_private_key_file(SSH_KEY)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(SERVER_IP, username=SERVER_USER, pkey=key)

    sftp = client.open_sftp()
    with sftp.open(REMOTE_COOKIE, 'w') as f:
        f.write(cookie_text)
    sftp.close()

    _, stdout, _ = client.exec_command(f'docker restart {CONTAINER_NAME}')
    stdout.channel.recv_exit_status()
    client.close()


def main():
    log.info('starting cookie refresh')
    try:
        cookie_text, count = extract_cookies()
        push_to_server(cookie_text)
        log.info('pushed to server and restarted container — done')
        print(f'[grabha] {datetime.datetime.now():%Y-%m-%d %H:%M} — refreshed {count} cookies OK')

    except Exception as e:
        log.exception('cookie refresh failed: %s', e)
        print(f'[grabha] ERROR: {e}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
