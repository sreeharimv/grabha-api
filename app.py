from flask import Flask, request, jsonify, send_file, Response
from flask_cors import CORS
import yt_dlp
import os, uuid, threading, time, re, sqlite3, logging, urllib.request, json
from datetime import datetime

app = Flask(__name__)
CORS(app)

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
        'best': 'bestvideo+bestaudio/best',
        '1080': 'bestvideo[height<=1080]+bestaudio/best[height<=1080]/best',
        '720':  'bestvideo[height<=720]+bestaudio/best[height<=720]/best',
        '480':  'bestvideo[height<=480]+bestaudio/best[height<=480]/best',
        '360':  'bestvideo[height<=360]+bestaudio/best[height<=360]/best',
    }

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
        }
    else:
        ydl_opts = {
            'format': quality_map.get(quality, 'bestvideo+bestaudio/best'),
            'outtmpl': os.path.join(output_path, '%(title)s.%(ext)s'),
            'merge_output_format': 'mp4',
            'progress_hooks': [progress_hook],
            'quiet': True,
            'no_warnings': True,
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
        with yt_dlp.YoutubeDL({'quiet': True, 'no_warnings': True}) as ydl:
            info = ydl.extract_info(url, download=False)
        return jsonify({
            'title':     info.get('title', 'Unknown'),
            'thumbnail': info.get('thumbnail', ''),
            'duration':  info.get('duration', 0),
            'uploader':  info.get('uploader', 'Unknown'),
            'platform':  info.get('extractor_key', 'Unknown'),
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


@app.route('/api/proxy-thumb')
def proxy_thumb():
    url = request.args.get('url', '').strip()
    if not url:
        return jsonify({'error': 'No URL'}), 400
    try:
        r = http_requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10, stream=True)
        content_type = r.headers.get('Content-Type', 'image/jpeg')
        return Response(r.content, content_type=content_type)
    except Exception as e:
        return jsonify({'error': str(e)}), 502


@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'service': 'grabha'})


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
