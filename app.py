from flask import Flask, request, jsonify, send_file, Response, send_from_directory
from flask_cors import CORS
import yt_dlp
import os, sys, uuid, threading, time, re, sqlite3, logging, urllib.request, json, hmac, hashlib, shutil, signal, base64
from urllib.parse import urlparse, parse_qs
from datetime import datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo('Asia/Kolkata')

app = Flask(__name__)
CORS(app)


def get_cookiefile(url: str):
    """Return appropriate cookies file based on URL domain."""
    cookie_map = {
        'instagram.com': '/app/cookies/instagram.txt',
        'youtube.com':   '/app/cookies/youtube.txt',
        'youtu.be':      '/app/cookies/youtube.txt',
    }
    for domain, path in cookie_map.items():
        if domain in url and os.path.exists(path):
            return path
    return None


_IG_SHORTCODE_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'


def resolve_instagram_share(url: str):
    """Rewrite an Instagram highlight share link into a URL yt-dlp supports.

    The app's Share button produces instagram.com/s/<base64>?story_media_id=<pk>,
    where <base64> decodes to "highlight:<id>". yt-dlp rejects that as an
    unsupported URL; it only knows /stories/highlights/<id>/, which extracts
    the *whole* highlight as a playlist. Returns (url, shortcode) — shortcode
    identifies the one shared item within it (None if not a share link).
    """
    m = re.match(r'https?://(?:www\.)?instagram\.com/s/([A-Za-z0-9_=-]+)', url)
    if not m:
        return url, None
    token = m.group(1)
    try:
        decoded = base64.urlsafe_b64decode(token + '=' * (-len(token) % 4)).decode()
    except Exception:
        return url, None
    kind, _, highlight_id = decoded.partition(':')
    if kind != 'highlight' or not highlight_id.isdigit():
        return url, None
    new_url = f'https://www.instagram.com/stories/highlights/{highlight_id}/'

    # story_media_id is the numeric media pk (sometimes "<pk>_<user_id>");
    # yt-dlp identifies entries by shortcode, which is the pk in base64.
    pk = parse_qs(urlparse(url).query).get('story_media_id', [''])[0].split('_')[0]
    if not pk.isdigit():
        return new_url, None
    n, shortcode = int(pk), ''
    while n:
        n, r = divmod(n, 64)
        shortcode = _IG_SHORTCODE_ALPHABET[r] + shortcode
    return new_url, shortcode


def extract_story_item(ydl, url, shortcode, download):
    """Extract (and optionally download) one item of a highlight.

    Resolves the highlight's entries without processing them, then processes
    only the matching one — so the returned info (title, thumbnail, duration)
    is that item's, not the highlight's.
    """
    playlist = ydl.extract_info(url, download=False, process=False)
    entry = next((e for e in (playlist or {}).get('entries') or []
                  if e and e.get('id') == shortcode), None)
    if entry is None:
        raise Exception('That story is no longer in the highlight (or needs a login we don\'t have)')
    return ydl.process_ie_result(entry, download=download)

DOWNLOAD_DIR = '/tmp/grabha'
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Hard ceiling on a single download. Without one a wedged ffmpeg holds its
# thread forever and leaves its activity row stuck on 'pending' (one such row
# sat there for four months). 15 min is generous: the slowest legitimate job
# observed — a clip seek through a 2.5h HLS stream — took 8m25s.
MAX_JOB_SECONDS = int(os.environ.get('MAX_JOB_SECONDS', 900))

# ── Activity logging setup ────────────────────────────────────────────────────
LOG_DIR  = os.path.expanduser('~/grabha/logs')
LOG_FILE = os.path.join(LOG_DIR, 'activity.log')
DB_FILE  = os.path.join(LOG_DIR, 'activity.db')
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(message)s',
)
# basicConfig points the *root* logger at a file, and werkzeug propagates to
# root — so every request line and every traceback went to activity.log and
# `docker logs grabha-api` stayed empty but for the two Flask banner lines.
# Mirror to stdout so container logs are actually usable for debugging.
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
_db_lock = threading.Lock()


def _init_db():
    with sqlite3.connect(DB_FILE) as con:
        con.execute('''
            CREATE TABLE IF NOT EXISTS downloads (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp  TEXT,
                ip_address TEXT,
                url        TEXT,
                platform   TEXT,
                format     TEXT,
                quality    TEXT,
                title      TEXT,
                status     TEXT,
                error_msg  TEXT,
                device     TEXT,
                country    TEXT,
                city       TEXT,
                isp        TEXT
            )
        ''')
        # Migrate existing databases that predate the geo columns
        for col in ('country', 'city', 'isp'):
            try:
                con.execute(f'ALTER TABLE downloads ADD COLUMN {col} TEXT')
            except sqlite3.OperationalError:
                pass
        con.commit()

        con.commit()

_init_db()


def _reconcile_orphaned_jobs():
    """Fail any row left 'pending' by a previous process.

    The job store is in-memory only, so a restart kills every running worker
    thread while its row stays 'pending' forever — nothing else will ever
    close it out. Anything still pending at startup is by definition dead.
    """
    with _db_lock:
        with sqlite3.connect(DB_FILE) as con:
            cur = con.execute(
                "UPDATE downloads SET status='error', error_msg=? WHERE status='pending'",
                ('Interrupted — the server restarted while this download was running',),
            )
            con.commit()
            n = cur.rowcount
    if n:
        logging.info('%s | Reconciled %d orphaned pending download(s) at startup',
                     datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S'), n)


_reconcile_orphaned_jobs()


def _detect_device(ua: str) -> str:
    ua = (ua or '').lower()
    if any(k in ua for k in ('mobile', 'android', 'iphone', 'ipad', 'tablet')):
        return 'Mobile'
    return 'Desktop'


def _get_ip() -> str:
    # CF-Connecting-IP is set by Cloudflare (incl. the tunnel) and is not
    # client-spoofable, unlike X-Forwarded-For which Cloudflare only
    # appends to rather than strips — a client can send its own
    # X-Forwarded-For value and have it land first in the list.
    cf_ip = request.headers.get('CF-Connecting-IP', '')
    if cf_ip:
        return cf_ip.strip()
    forwarded = request.headers.get('X-Forwarded-For', '')
    if forwarded:
        return forwarded.split(',')[0].strip()
    return request.remote_addr or '0.0.0.0'


def _geo_lookup(ip: str) -> tuple:
    """Return (country, city, isp) for the given IP, or empty strings on failure."""
    if not ip or ip in ('127.0.0.1', '0.0.0.0'):
        return '', '', ''
    try:
        with urllib.request.urlopen(f'https://ipinfo.io/{ip}/json', timeout=3) as r:
            data = json.loads(r.read())
        country = data.get('country', '')
        city    = data.get('city', '')
        isp     = data.get('org', '')
        return country, city, isp
    except Exception:
        return '', '', ''


def _backfill_geo():
    """Backfill city/country for rows inserted before geo columns existed. Runs once at startup."""
    with _db_lock:
        with sqlite3.connect(DB_FILE) as con:
            con.row_factory = sqlite3.Row
            missing = con.execute(
                "SELECT id, ip_address FROM downloads "
                "WHERE ip_address IS NOT NULL AND ip_address != '' "
                "AND (city IS NULL OR city = '') LIMIT 50"
            ).fetchall()
            for row in missing:
                country, city, isp = _geo_lookup(row['ip_address'])
                if country or city:
                    con.execute(
                        "UPDATE downloads SET country=?, city=?, isp=? WHERE id=?",
                        (country, city, isp, row['id'])
                    )
            if missing:
                con.commit()

_backfill_geo()


def log_attempt(url: str, fmt: str, quality: str, ip: str, device: str) -> int:
    """Insert a pending download record; return the row id."""
    ts = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
    country, city, isp = _geo_lookup(ip)
    with _db_lock:
        with sqlite3.connect(DB_FILE) as con:
            cur = con.execute(
                '''INSERT INTO downloads
                   (timestamp, ip_address, url, format, quality, status, device, country, city, isp)
                   VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?)''',
                (ts, ip, url, fmt, quality, device, country, city, isp),
            )
            con.commit()
            return cur.lastrowid


def update_log_record(row_id: int, title: str, platform: str, status: str, error_msg: str = ''):
    """Update the row once the download finishes or fails, and write to log file."""
    ts = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
    with _db_lock:
        with sqlite3.connect(DB_FILE) as con:
            con.execute(
                '''UPDATE downloads
                   SET title=?, platform=?, status=?, error_msg=?, timestamp=?
                   WHERE id=?''',
                (title, platform, status, error_msg, ts, row_id),
            )
            row = con.execute(
                'SELECT ip_address, country, city FROM downloads WHERE id=?',
                (row_id,),
            ).fetchone()
            con.commit()

    if row:
        ip, country, city = row
        logging.info(
            '%s | IP: %s | Country: %s | City: %s | Status: %s',
            ts, ip, country or '', city or '', status,
        )

# ─────────────────────────────────────────────────────────────────────────────

# In-memory job store
jobs = {}


def cleanup_file(path, delay=300):
    def _cleanup():
        time.sleep(delay)
        try:
            if os.path.exists(path):
                os.remove(path)
            parent = os.path.dirname(path)
            if os.path.isdir(parent) and not os.listdir(parent):
                os.rmdir(parent)
        except Exception:
            pass
    threading.Thread(target=_cleanup, daemon=True).start()


def _kill_job_processes(job_id):
    """SIGKILL any ffmpeg child still working on this job.

    yt-dlp's progress hook is our only cancellation point, and it is never
    called while ffmpeg runs as a subprocess — so a wedged ffmpeg can only be
    stopped from outside. Children are matched on the job_id, which appears in
    their cmdline via the output path.
    """
    killed = 0
    for pid in os.listdir('/proc'):
        if not pid.isdigit():
            continue
        try:
            with open('/proc/%s/cmdline' % pid, 'rb') as fh:
                cmdline = fh.read().decode('utf-8', 'replace')
        except OSError:
            continue
        if job_id in cmdline and 'ffmpeg' in cmdline:
            try:
                os.kill(int(pid), signal.SIGKILL)
                killed += 1
            except OSError:
                pass
    return killed


def _start_watchdog(job_id, log_id, timeout=None):
    """Fail a job that outlives MAX_JOB_SECONDS instead of leaving it hanging."""
    timeout = timeout or MAX_JOB_SECONDS

    def _watch():
        deadline = time.time() + timeout
        while time.time() < deadline:
            time.sleep(5)
            j = jobs.get(job_id)
            if not j or j['status'] in ('done', 'error', 'cancelled'):
                return
        j = jobs.get(job_id)
        if not j or j['status'] in ('done', 'error', 'cancelled'):
            return
        msg = 'Timed out after %ds' % timeout
        j['cancelled'] = True
        j['status']    = 'error'
        j['error']     = msg
        j['log'].append('[error] %s — killing download' % msg)
        _kill_job_processes(job_id)
        if log_id:
            update_log_record(log_id, j.get('title', ''), 'Unknown', 'error', msg)

    threading.Thread(target=_watch, daemon=True).start()


def parse_progress(line):
    """Extract percent, speed, eta from a yt-dlp progress line."""
    pct   = re.search(r'(\d+\.?\d*)%', line)
    speed = re.search(r'at\s+([\d.]+\s*\w+/s)', line)
    eta   = re.search(r'ETA\s+(\d+:\d+)', line)
    return {
        'pct':   pct.group(1)   if pct   else None,
        'speed': speed.group(1) if speed else None,
        'eta':   eta.group(1)   if eta   else None,
    }


def _ts(t):
    """Convert hh:mm:ss string to total seconds (float)."""
    try:
        parts = t.strip().split(':')
        parts = [float(p) for p in parts]
        if len(parts) == 3:
            return parts[0] * 3600 + parts[1] * 60 + parts[2]
        elif len(parts) == 2:
            return parts[0] * 60 + parts[1]
        return float(parts[0])
    except Exception:
        return 0.0


def run_download(job_id, url, format_type, quality, clip_start=None, clip_end=None):
    jobs[job_id]['status'] = 'downloading'
    url, story_item = resolve_instagram_share(url)
    output_path = os.path.join(DOWNLOAD_DIR, job_id)
    os.makedirs(output_path, exist_ok=True)
    log_id = jobs[job_id].get('log_id')
    _start_watchdog(job_id, log_id)

    # Log clip info if set
    if clip_start or clip_end:
        s = clip_start or '0:00:00'
        e = clip_end   or 'end'
        jobs[job_id]['log'].append(f'[info] clip section: {s} → {e}')

    def progress_hook(d):
        if jobs[job_id].get('cancelled'):
            raise yt_dlp.utils.DownloadCancelled('Cancelled by user')
        if d['status'] == 'downloading':
            pct   = d.get('_percent_str', '').strip()
            speed = d.get('_speed_str', '').strip()
            eta   = d.get('_eta_str', '').strip()
            line  = f'[download]  {pct}  at {speed}  ETA {eta}'
            jobs[job_id]['log'].append(line)
            jobs[job_id]['progress'] = pct
            jobs[job_id]['progress_detail'] = {'speed': speed, 'eta': eta}
        elif d['status'] == 'finished':
            jobs[job_id]['log'].append('[download] processing file…')

    # Instagram/Facebook only serve VP9 DASH streams; merging them produces a
    # file most players show as audio-only, so prefer their pre-muxed mp4
    # (picks the H.264 variant) first. YouTube's pre-muxed mp4 (itag 18) has
    # no such issue but is sometimes served truncated/throttled regardless of
    # auth, so for everything else prefer the adaptive bestvideo+bestaudio.
    if any(d in url for d in ('instagram.com', 'facebook.com', 'fb.watch')):
        quality_map = {
            'best': 'best[ext=mp4]/bestvideo+bestaudio/best',
            '1080': 'best[ext=mp4][height<=1080]/bestvideo[height<=1080]+bestaudio/best[height<=1080]/best',
            '720':  'best[ext=mp4][height<=720]/bestvideo[height<=720]+bestaudio/best[height<=720]/best',
            '480':  'best[ext=mp4][height<=480]/bestvideo[height<=480]+bestaudio/best[height<=480]/best',
            '360':  'best[ext=mp4][height<=360]/bestvideo[height<=360]+bestaudio/best[height<=360]/best',
        }
    else:
        quality_map = {
            'best': 'bestvideo+bestaudio/best[ext=mp4]/best',
            '1080': 'bestvideo[height<=1080]+bestaudio/best[ext=mp4][height<=1080]/best[height<=1080]',
            '720':  'bestvideo[height<=720]+bestaudio/best[ext=mp4][height<=720]/best[height<=720]',
            '480':  'bestvideo[height<=480]+bestaudio/best[ext=mp4][height<=480]/best[height<=480]',
            '360':  'bestvideo[height<=360]+bestaudio/best[ext=mp4][height<=360]/best[height<=360]',
        }

    cookiefile = get_cookiefile(url)

    if format_type == 'mp3':
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': os.path.join(output_path, '%(title).60s.%(ext)s'),
            'trim_file_name': 200,
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
            'progress_hooks': [progress_hook],
            'quiet': True,
            'no_warnings': True,
            'noplaylist': True,
            'cookiefile': cookiefile,
        }
    else:
        ydl_opts = {
            'format': quality_map.get(quality, 'bestvideo+bestaudio/best'),
            'outtmpl': os.path.join(output_path, '%(title).60s.%(ext)s'),
            'trim_file_name': 200,
            'merge_output_format': 'mp4',
            'progress_hooks': [progress_hook],
            'quiet': True,
            'no_warnings': True,
            'noplaylist': True,
            'cookiefile': cookiefile,
        }

    # Required for YouTube JS challenge solving (EJS)
    ydl_opts['js_runtimes'] = {'node': {}}
    ydl_opts['remote_components'] = {'ejs:github'}

    # yt-dlp defaults to no socket timeout, so a stalled read blocks forever.
    ydl_opts['socket_timeout'] = 30

    # Apply clip section if provided
    if clip_start or clip_end:
        s = clip_start or '0:00:00'
        e = clip_end   or 'inf'
        ydl_opts['download_ranges'] = lambda info, ytdl: [{'start_time': _ts(s), 'end_time': _ts(e)}]
        ydl_opts['force_keyframes_at_cuts'] = True
        # Clipping runs through ffmpeg's -ss, which can only seek cheaply on a
        # plain HTTPS URL (HTTP range request). On an HLS playlist ffmpeg walks
        # segments from the start and discards them: a 17s clip at 9:25 into a
        # 2h35m video read ~150MB and wrote nothing for 8 minutes. YouTube's
        # "Premium" renditions (e.g. itag 616) are HLS-only and outrank the
        # equivalent progressive format on codec, so they get picked by
        # default — sort protocol first, then fall back to the usual quality
        # ordering, to keep the highest resolution that is actually seekable.
        ydl_opts['format_sort'] = ['proto', 'res', 'br']

    # YouTube's PO-token fetch is occasionally flaky (yt-dlp-ejs/network),
    # producing a transient "ffmpeg exited with code 8" / HTTP 403 that a
    # same-URL retry reliably clears. Retry once for that specific pattern
    # rather than surfacing it to the user; anything else fails immediately.
    max_attempts = 2
    for attempt in range(1, max_attempts + 1):
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # A highlight share link names one item; the highlight URL
                # would otherwise download every item in it.
                if story_item:
                    info = extract_story_item(ydl, url, story_item, download=True)
                else:
                    info = ydl.extract_info(url, download=True)
                if info is None:
                    raise Exception('Could not extract info — content may be private, expired, or require login')
                title    = info.get('title', 'video')
                platform = info.get('extractor_key', 'Unknown')
                jobs[job_id]['title'] = title
                jobs[job_id]['log'].append(f'[info] title: {title}')

            files = os.listdir(output_path)
            if files:
                file_path = os.path.join(output_path, files[0])
                jobs[job_id]['status']   = 'done'
                jobs[job_id]['file']     = file_path
                jobs[job_id]['filename'] = files[0]
                jobs[job_id]['log'].append(f'[done] ready: {files[0]}')
                cleanup_file(file_path, 300)
                if log_id:
                    update_log_record(log_id, title, platform, 'success')
            else:
                jobs[job_id]['status'] = 'error'
                jobs[job_id]['error']  = 'No output file produced'
                if log_id:
                    update_log_record(log_id, '', platform, 'error', 'No output file produced')
            return

        except yt_dlp.utils.DownloadCancelled:
            jobs[job_id]['status'] = 'cancelled'
            jobs[job_id]['log'].append('[cancelled] download cancelled by user')
            if log_id:
                update_log_record(log_id, '', 'Unknown', 'cancelled', 'Cancelled by user')
            # Clean up partial files
            if os.path.isdir(output_path):
                shutil.rmtree(output_path, ignore_errors=True)
            return

        except Exception as e:
            # A watchdog kill surfaces as "ffmpeg exited with code ..." too;
            # never retry a job that was deliberately stopped.
            transient = ('ffmpeg exited with code' in str(e) or 'HTTP Error 403' in str(e)) \
                and not jobs[job_id].get('cancelled')
            if transient and attempt < max_attempts:
                jobs[job_id]['log'].append(f'[retry] transient error, retrying: {e}')
                shutil.rmtree(output_path, ignore_errors=True)
                os.makedirs(output_path, exist_ok=True)
                time.sleep(2)
                continue
            jobs[job_id]['status'] = 'error'
            jobs[job_id]['error']  = str(e)
            jobs[job_id]['log'].append(f'[error] {e}')
            if log_id:
                update_log_record(log_id, '', 'Unknown', 'error', str(e))
            return


@app.route('/api/info', methods=['POST'])
def get_info():
    url = (request.json or {}).get('url', '').strip()
    if not url:
        return jsonify({'error': 'No URL'}), 400
    url, story_item = resolve_instagram_share(url)
    try:
        with yt_dlp.YoutubeDL({'quiet': True, 'no_warnings': True, 'cookiefile': get_cookiefile(url), 'js_runtimes': {'node': {}}, 'remote_components': {'ejs:github'}}) as ydl:
            if story_item:
                info = extract_story_item(ydl, url, story_item, download=False)
            else:
                info = ydl.extract_info(url, download=False)
        if info is None:
            raise Exception('Could not extract info — content may be private, expired, or require login')
        return jsonify({
            'title':     info.get('title', 'Unknown'),
            'thumbnail': info.get('thumbnail', ''),
            'duration':  info.get('duration', 0),
            'uploader':  info.get('uploader', 'Unknown'),
            'platform':  info.get('extractor_key', 'Unknown'),
            'chapters':  info.get('chapters', []),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@app.route('/api/download', methods=['POST'])
def start_download():
    data    = request.json or {}
    url     = data.get('url', '').strip()
    fmt     = data.get('format', 'mp4')
    quality = data.get('quality', 'best')
    clip_start = data.get('clip_start', None)
    clip_end   = data.get('clip_end', None)

    if not url:
        return jsonify({'error': 'No URL'}), 400

    ip     = _get_ip()
    device = _detect_device(request.headers.get('User-Agent', ''))
    log_id = log_attempt(url, fmt, quality, ip, device)

    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        'status':          'queued',
        'log':             ['[queue] job created', f'[queue] url: {url}'],
        'progress':        '0%',
        'progress_detail': {},
        'log_id':          log_id,
    }

    t = threading.Thread(target=run_download, args=(job_id, url, fmt, quality, clip_start, clip_end))
    t.daemon = True
    t.start()

    return jsonify({'job_id': job_id})


@app.route('/api/status/<job_id>')
def job_status(job_id):
    if job_id not in jobs:
        # The job store is in-memory, so a restart loses every job. Report a
        # terminal status rather than a bare 404: the frontend poller reads
        # only the JSON body and would otherwise poll a dead job forever.
        return jsonify({
            'status': 'error',
            'error':  'Job no longer exists — the server restarted. Please try again.',
            'log':    [],
            'progress': '0%',
            'progress_detail': {},
            'title': '',
            'filename': '',
        }), 404
    j = jobs[job_id]
    return jsonify({
        'status':          j['status'],
        'log':             j.get('log', []),
        'progress':        j.get('progress', '0%'),
        'progress_detail': j.get('progress_detail', {}),
        'title':           j.get('title', ''),
        'filename':        j.get('filename', ''),
        'error':           j.get('error', ''),
    })


@app.route('/api/download/<job_id>')
def download_file(job_id):
    if job_id not in jobs:
        return jsonify({'error': 'Job not found'}), 404
    j = jobs[job_id]
    if j['status'] != 'done':
        return jsonify({'error': 'File not ready'}), 400
    return send_file(j['file'], as_attachment=True, download_name=j['filename'])


@app.route('/api/cancel/<job_id>', methods=['POST'])
def cancel_job(job_id):
    if job_id not in jobs:
        return jsonify({'error': 'Job not found'}), 404
    j = jobs[job_id]
    if j['status'] in ('done', 'error', 'cancelled'):
        return jsonify({'status': j['status'], 'message': 'Job already finished'})
    j['cancelled'] = True
    return jsonify({'status': 'cancelling', 'message': 'Cancel signal sent'})


@app.route('/api/proxy-thumb')
def proxy_thumb():
    url = request.args.get('url', '').strip()
    if not url:
        return jsonify({'error': 'No URL'}), 400
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=10) as r:
            content_type = r.headers.get('Content-Type', 'image/jpeg')
            return Response(r.read(), content_type=content_type)
    except Exception as e:
        return jsonify({'error': str(e)}), 502


@app.route("/")
def index():
    return send_from_directory("/app/web", "index.html")

# The web root is a bind-mounted checkout, so anything that lands in it is
# served publicly — a stray activity.db there was reachable at /activity.db.
_BLOCKED_STATIC_EXT = ('.db', '.sqlite', '.sqlite3', '.env', '.log', '.py')


@app.route("/<path:filename>")
def static_files(filename):
    import os as _os
    parts = filename.split('/')
    if any(part.startswith('.') for part in parts) or \
            filename.lower().endswith(_BLOCKED_STATIC_EXT):
        return jsonify({'error': 'Not found'}), 404
    full = _os.path.join("/app/web", filename)
    if _os.path.isdir(full):
        filename = filename.rstrip("/") + "/index.html"
    return send_from_directory("/app/web", filename)



_ADMIN_USER     = os.environ.get('ADMIN_USER', 'grabha_admin')
_ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'grabha!@#')
_TOKEN_SECRET   = os.environ.get('TOKEN_SECRET', 'grabha-token-secret-key')

def _make_token():
    import hmac, hashlib
    from datetime import datetime
    raw = f'{_TOKEN_SECRET}:{datetime.now().date().isoformat()}'
    return hmac.new(_TOKEN_SECRET.encode(), raw.encode(), hashlib.sha256).hexdigest()

def _valid_token(token):
    import hmac
    return hmac.compare_digest(token or '', _make_token())

@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'service': 'grabha'})



@app.route('/admin/login', methods=['POST'])
def admin_login():
    data = request.json or {}
    if data.get('username') == _ADMIN_USER and data.get('password') == _ADMIN_PASSWORD:
        return jsonify({'token': _make_token()})
    return jsonify({'error': 'Invalid credentials'}), 401


@app.route('/admin/data')
def admin_data():
    token = request.headers.get('Authorization', '').replace('Bearer ', '')
    if not _valid_token(token):
        return jsonify({'error': 'Unauthorized'}), 401

    page     = max(1, int(request.args.get('page', 1)))
    per_page = 20
    offset   = (page - 1) * per_page
    search   = request.args.get('q', '').strip()
    status_f = request.args.get('status', '').strip()

    with sqlite3.connect(DB_FILE) as con:
        con.row_factory = sqlite3.Row

        where_clauses, params = [], []
        if search:
            where_clauses.append("(ip_address LIKE ? OR country LIKE ? OR city LIKE ? OR title LIKE ? OR platform LIKE ?)")
            params.extend([f'%{search}%'] * 5)
        if status_f:
            where_clauses.append("status = ?")
            params.append(status_f)
        where = ('WHERE ' + ' AND '.join(where_clauses)) if where_clauses else ''

        total = con.execute(f'SELECT COUNT(*) FROM downloads {where}', params).fetchone()[0]
        rows  = con.execute(
            f'SELECT id, timestamp, ip_address, country, city, platform, format, quality, device, status, title, error_msg '
            f'FROM downloads {where} ORDER BY id DESC LIMIT ? OFFSET ?',
            params + [per_page, offset]
        ).fetchall()
        stats = con.execute(
            "SELECT COUNT(*) total, "
            "SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) success, "
            "SUM(CASE WHEN status='error'   THEN 1 ELSE 0 END) errors, "
            "SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) pending "
            "FROM downloads"
        ).fetchone()
        top_countries = con.execute(
            "SELECT country, COUNT(*) n FROM downloads WHERE status='success' AND country != '' "
            "GROUP BY country ORDER BY n DESC LIMIT 5"
        ).fetchall()
        top_platforms = con.execute(
            "SELECT platform, COUNT(*) n FROM downloads WHERE status='success' AND platform != '' "
            "GROUP BY platform ORDER BY n DESC LIMIT 5"
        ).fetchall()
        top_cities = con.execute(
            "SELECT city, COUNT(*) n FROM downloads WHERE status='success' AND city IS NOT NULL AND city != '' "
            "GROUP BY city ORDER BY n DESC LIMIT 5"
        ).fetchall()

    return jsonify({
        'rows':          [dict(r) for r in rows],
        'total':         total,
        'pages':         max(1, (total + per_page - 1) // per_page),
        'page':          page,
        'stats':         dict(stats),
        'top_countries': [dict(r) for r in top_countries],
        'top_platforms': [dict(r) for r in top_platforms],
        'top_cities':    [dict(r) for r in top_cities],
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
