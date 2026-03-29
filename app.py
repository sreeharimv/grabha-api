from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import yt_dlp
import os, uuid, threading, time, re

app = Flask(__name__)
CORS(app)

DOWNLOAD_DIR = '/tmp/grabha'
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

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
            title = info.get('title', 'video')
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
        else:
            jobs[job_id]['status'] = 'error'
            jobs[job_id]['error']  = 'No output file produced'

    except Exception as e:
        jobs[job_id]['status'] = 'error'
        jobs[job_id]['error']  = str(e)
        jobs[job_id]['log'].append(f'[error] {e}')


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

    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        'status':          'queued',
        'log':             ['[queue] job created', f'[queue] url: {url}'],
        'progress':        '0%',
        'progress_detail': {},
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


@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'service': 'grabha'})


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
