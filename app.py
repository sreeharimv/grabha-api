from flask import Flask, request, jsonify, send_file, Response
from flask_cors import CORS
import yt_dlp
import os, uuid, threading, time, re, sqlite3, logging, urllib.request, json, hmac, hashlib, shutil
from datetime import datetime

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

DOWNLOAD_DIR = '/tmp/grabha'
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

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

_init_db()


def _detect_device(ua: str) -> str:
    ua = (ua or '').lower()
    if any(k in ua for k in ('mobile', 'android', 'iphone', 'ipad', 'tablet')):
        return 'Mobile'
    return 'Desktop'


def _get_ip() -> str:
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


def log_attempt(url: str, fmt: str, quality: str, ip: str, device: str) -> int:
    """Insert a pending download record; return the row id."""
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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
    output_path = os.path.join(DOWNLOAD_DIR, job_id)
    os.makedirs(output_path, exist_ok=True)
    log_id = jobs[job_id].get('log_id')

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

    quality_map = {
        # Prefer pre-muxed mp4 first to avoid blank-video issues on Facebook/Meta
        # (separate bestvideo+bestaudio merges can produce a blank video track)
        'best': 'bestvideo+bestaudio/best[ext=mp4]/best',
        '1080': 'bestvideo[height<=1080]+bestaudio/best[height<=1080][ext=mp4]/best[height<=1080]/best',
        '720':  'bestvideo[height<=720]+bestaudio/best[height<=720][ext=mp4]/best[height<=720]/best',
        '480':  'bestvideo[height<=480]+bestaudio/best[height<=480][ext=mp4]/best[height<=480]/best',
        '360':  'bestvideo[height<=360]+bestaudio/best[height<=360][ext=mp4]/best[height<=360]/best',
    }

    cookiefile = get_cookiefile(url)

    if format_type == 'mp3':
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': os.path.join(output_path, '%(title)s.%(ext)s'),
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
            'outtmpl': os.path.join(output_path, '%(title)s.%(ext)s'),
            'merge_output_format': 'mp4',
            'progress_hooks': [progress_hook],
            'quiet': True,
            'no_warnings': True,
            'noplaylist': True,
            'cookiefile': cookiefile,
        }

    # Apply clip section if provided
    if clip_start or clip_end:
        s = clip_start or '0:00:00'
        e = clip_end   or 'inf'
        ydl_opts['download_ranges'] = lambda info, ytdl: [{'start_time': _ts(s), 'end_time': _ts(e)}]
        ydl_opts['force_keyframes_at_cuts'] = True

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
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

    except yt_dlp.utils.DownloadCancelled:
        jobs[job_id]['status'] = 'cancelled'
        jobs[job_id]['log'].append('[cancelled] download cancelled by user')
        if log_id:
            update_log_record(log_id, '', 'Unknown', 'cancelled', 'Cancelled by user')
        # Clean up partial files
        if os.path.isdir(output_path):
            shutil.rmtree(output_path, ignore_errors=True)

    except Exception as e:
        jobs[job_id]['status'] = 'error'
        jobs[job_id]['error']  = str(e)
        jobs[job_id]['log'].append(f'[error] {e}')
        if log_id:
            update_log_record(log_id, '', 'Unknown', 'error', str(e))


@app.route('/api/info', methods=['POST'])
def get_info():
    url = (request.json or {}).get('url', '').strip()
    if not url:
        return jsonify({'error': 'No URL'}), 400
    try:
        with yt_dlp.YoutubeDL({'quiet': True, 'no_warnings': True, 'cookiefile': get_cookiefile(url)}) as ydl:
            info = ydl.extract_info(url, download=False)
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
        return jsonify({'error': 'Job not found'}), 404
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


@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'service': 'grabha'})


@app.route('/admin')
def admin():
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
            f'SELECT * FROM downloads {where} ORDER BY id DESC LIMIT ? OFFSET ?',
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

    pages      = max(1, (total + per_page - 1) // per_page)
    rows_dicts = [dict(r) for r in rows]
    q_str      = f'&q={search}' if search else ''
    q_str     += f'&status={status_f}' if status_f else ''

    def page_link(p):
        return f'/admin?page={p}{q_str}'

    html_rows = ''
    for r in rows_dicts:
        st = r['status'] or ''
        badge = {'success': '#2d6a4f', 'error': '#9b2226', 'pending': '#6c757d'}.get(st, '#555')
        html_rows += f'''<tr>
            <td>{r["id"]}</td>
            <td>{r["timestamp"] or ""}</td>
            <td>{r["ip_address"] or ""}</td>
            <td>{r["country"] or ""}</td>
            <td>{r["city"] or ""}</td>
            <td>{r["platform"] or ""}</td>
            <td>{r["format"] or ""}</td>
            <td>{r["quality"] or ""}</td>
            <td>{r["device"] or ""}</td>
            <td><span style="background:{badge};color:#fff;padding:2px 8px;border-radius:4px;font-size:12px">{st}</span></td>
            <td style="max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="{r["title"] or ""}">{r["title"] or ""}</td>
            <td style="color:#e07b00;font-size:12px;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="{r["error_msg"] or ""}">{r["error_msg"] or ""}</td>
        </tr>'''

    stat_cards = ''.join(f'''
        <div style="background:#1e1e1e;border:1px solid #333;border-radius:8px;padding:16px 24px;text-align:center">
            <div style="font-size:28px;font-weight:700;color:{c}">{v}</div>
            <div style="color:#888;font-size:13px;margin-top:4px">{l}</div>
        </div>''' for l, v, c in [
        ('Total', stats['total'], '#ccc'),
        ('Success', stats['success'], '#52b788'),
        ('Errors', stats['errors'], '#e63946'),
        ('Pending', stats['pending'], '#aaa'),
    ])

    country_rows = ''.join(f'<tr><td>{r["country"]}</td><td style="color:#52b788">{r["n"]}</td></tr>' for r in top_countries)
    platform_rows = ''.join(f'<tr><td>{r["platform"]}</td><td style="color:#52b788">{r["n"]}</td></tr>' for r in top_platforms)

    pagination = ''
    if pages > 1:
        if page > 1:
            pagination += f'<a href="{page_link(page-1)}" style="margin:0 4px;color:#aaa;text-decoration:none">← Prev</a>'
        pagination += f'<span style="margin:0 8px;color:#555">Page {page} of {pages}</span>'
        if page < pages:
            pagination += f'<a href="{page_link(page+1)}" style="margin:0 4px;color:#aaa;text-decoration:none">Next →</a>'

    return f'''<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Grabha — Logs</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0 }}
  body {{ background: #121212; color: #ccc; font-family: "JetBrains Mono", monospace, sans-serif; font-size: 13px; padding: 24px }}
  h1 {{ color: #fff; font-size: 20px; margin-bottom: 20px }}
  .stat-grid {{ display: grid; grid-template-columns: repeat(4,1fr); gap: 12px; margin-bottom: 24px }}
  .side-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 24px }}
  .box {{ background: #1e1e1e; border: 1px solid #333; border-radius: 8px; padding: 16px }}
  .box h3 {{ color: #888; font-size: 12px; text-transform: uppercase; margin-bottom: 12px }}
  table {{ width: 100%; border-collapse: collapse }}
  th {{ color: #555; font-size: 11px; text-transform: uppercase; text-align: left; padding: 6px 10px; border-bottom: 1px solid #2a2a2a }}
  td {{ padding: 7px 10px; border-bottom: 1px solid #1e1e1e; vertical-align: middle }}
  tr:hover td {{ background: #1a1a1a }}
  .toolbar {{ display: flex; gap: 10px; margin-bottom: 16px; flex-wrap: wrap }}
  input, select {{ background: #1e1e1e; border: 1px solid #333; color: #ccc; padding: 7px 12px; border-radius: 6px; font-size: 13px; outline: none }}
  input:focus, select:focus {{ border-color: #555 }}
  button {{ background: #2a2a2a; border: 1px solid #444; color: #ccc; padding: 7px 16px; border-radius: 6px; cursor: pointer; font-size: 13px }}
  button:hover {{ background: #333 }}
  .pager {{ text-align: center; margin-top: 16px; color: #555 }}
  @media(max-width:700px) {{ .stat-grid {{ grid-template-columns: repeat(2,1fr) }} .side-grid {{ grid-template-columns: 1fr }} }}
</style>
</head><body>
<h1>⌗ Grabha — Activity Logs</h1>
<div class="stat-grid">{stat_cards}</div>
<div class="side-grid">
  <div class="box"><h3>Top Countries</h3><table><tbody>{country_rows}</tbody></table></div>
  <div class="box"><h3>Top Platforms</h3><table><tbody>{platform_rows}</tbody></table></div>
</div>
<form method="get" action="/admin">
  <div class="toolbar">
    <input name="q" placeholder="Search IP, country, city, title…" value="{search}" style="flex:1;min-width:200px">
    <select name="status">
      <option value="">All statuses</option>
      {''.join(f'<option value="{s}"{"selected" if status_f==s else ""}>{s.capitalize()}</option>' for s in ["success","error","pending"])}
    </select>
    <button type="submit">Filter</button>
    <a href="/admin" style="padding:7px 16px;background:#1e1e1e;border:1px solid #333;border-radius:6px;color:#888;text-decoration:none">Reset</a>
  </div>
</form>
<div class="box" style="overflow-x:auto">
  <table>
    <thead><tr>
      <th>#</th><th>Timestamp</th><th>IP</th><th>Country</th><th>City</th>
      <th>Platform</th><th>Format</th><th>Quality</th><th>Device</th><th>Status</th><th>Title</th><th>Error</th>
    </tr></thead>
    <tbody>{html_rows}</tbody>
  </table>
  <div class="pager">{pagination}</div>
</div>
</body></html>'''


_ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'grabha!@#')
_TOKEN_SECRET   = os.environ.get('TOKEN_SECRET', 'grabha-token-secret-key')

def _make_token():
    raw = f'{_TOKEN_SECRET}:{datetime.now().date().isoformat()}'
    return hmac.new(_TOKEN_SECRET.encode(), raw.encode(), hashlib.sha256).hexdigest()

def _valid_token(token):
    return hmac.compare_digest(token or '', _make_token())


@app.route('/admin/login', methods=['POST'])
def admin_login():
    data = request.json or {}
    if data.get('password') == _ADMIN_PASSWORD:
        return jsonify({'token': _make_token()})
    return jsonify({'error': 'Invalid password'}), 401


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

    return jsonify({
        'rows':          [dict(r) for r in rows],
        'total':         total,
        'pages':         max(1, (total + per_page - 1) // per_page),
        'page':          page,
        'stats':         dict(stats),
        'top_countries': [dict(r) for r in top_countries],
        'top_platforms': [dict(r) for r in top_platforms],
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
