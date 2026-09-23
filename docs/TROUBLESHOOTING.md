# Troubleshooting / Known Issues

Historical bugs and operational incidents, with root cause and fix — read before assuming something here can't recur.

## 1. Stuck `pending` activity rows (job store is in-memory, not persisted)

A job that hangs (wedged ffmpeg, stalled network read) with no failsafe leaves its activity-log row `pending` forever, because the row is only closed out when the job reaches a terminal state and the in-memory job store doesn't survive a restart. One row sat `pending` from April to September 2026 before this was fixed. Three mechanisms now prevent it — see [ARCHITECTURE.md](ARCHITECTURE.md#job-lifecycle-and-terminal-states) for how each works: the watchdog (`MAX_JOB_SECONDS` timeout), `_kill_job_processes()` (SIGKILL on the ffmpeg child), and `_reconcile_orphaned_jobs()` (fails leftover `pending` rows at startup). Also fixed: yt-dlp's `socket_timeout` was previously unset, so a stalled read blocked forever — now set to 30s.

## 2. Clip downloads stalling for minutes on HLS-only formats

A 17-second clip from a 2h35m video read ~150MB and wrote nothing for 8m25s. Root cause: ffmpeg's `-ss` seek is cheap only on plain HTTPS; on HLS it walks segments from the start. YouTube's HLS-only "Premium" renditions won on codec preference by default. Fixed by sorting `proto` before quality in `format_sort` whenever a clip is requested — see [DECISIONS.md](DECISIONS.md#clipped-downloads-must-not-select-an-hls-format).

## 3. `activity.db` briefly publicly reachable at `/activity.db`

The catch-all static route serves the bind-mounted `grabha-web` checkout, so anything dropped into that directory is public. A stray `activity.db` ended up there and was reachable at `grabha.in/activity.db` before being found and deleted (2026-09-08). Fixed by rejecting dotfile path segments and a blocklist of extensions (`.db`, `.sqlite`, `.sqlite3`, `.env`, `.log`, `.py`) in the static route.

## 4. `docker logs` showing nothing but two Flask banner lines

`logging.basicConfig(filename=...)` binds the *root* logger to a file, and werkzeug propagates to root — so every request line and every traceback went only to `activity.log`, never to stdout, for months. `PYTHONUNBUFFERED=1` does not fix this on its own. Fixed by adding an explicit `StreamHandler(sys.stdout)` to the root logger, so logs go to both the file and stdout (and thus `docker logs`).

## 5. YouTube downloads intermittently producing tiny/truncated files

YouTube's pre-muxed mp4 (itag 18) is sometimes served truncated or throttled regardless of auth/cookies — the download still "succeeds." A fix that made pre-muxed mp4 preferred (needed for Instagram/Facebook) was accidentally applied platform-wide and reintroduced this on YouTube; reverted to adaptive-first for everything except Instagram/Facebook on 2026-07-13. See [DECISIONS.md](DECISIONS.md#pre-muxed-mp4-first-is-instagramfacebook-only-not-global).

## 6. YouTube downloads playing audio-only on phones / refused by iOS Photos

YouTube's top adaptive video track became AV1 + Opus; muxed to mp4 it plays audio-only on most phones and iOS Photos won't save it at all, while the server still reported success. Fixed 2026-09-17 by preferring H.264+AAC explicitly. See [DECISIONS.md](DECISIONS.md#youtube-must-prefer-h264-aac).

## 7. Vertical video (Shorts) quality cap producing the wrong dimensions

A `[height<=N]` quality filter caps the long side of the frame; for a vertical 1080×1920 Short requesting "480" it produced 240×426 instead of ~480-wide. Fixed 2026-09-17 by switching to `format_sort`'s `res:N` (short-side-based) instead of a height filter. See [DECISIONS.md](DECISIONS.md#quality-caps-the-short-side-not-a-height-filter).

## 8. Post-live / in-progress livestreams reported as successful downloads

A stream that's live, upcoming, or just-ended has no finished file to download; in the `post_live` state only a live-DVR HLS manifest is served, which ffmpeg turns into a 261-byte file with a valid header and empty `mdat` — a "successful" download that plays nothing. Fixed by rejecting `is_live`/`is_upcoming`/`post_live` states via `match_filter` before download starts, and by validating output with `ffprobe` (`_validate_media()`) after every download regardless. See [ARCHITECTURE.md](ARCHITECTURE.md#output-validation).

## 9. Production running code matching no git commit at all

A manual `docker build`/`push` from a stale or uncommitted local `app.py` overwrote the CI-built `latest` tag on Docker Hub, and production ran that hand-built image — which corresponded to *no* commit in git history — until this was caught (2026-07-14), causing a full outage in the meantime. **Never rebuild/push the image by hand on the server as a deploy shortcut** — see [DEPLOYMENT.md](DEPLOYMENT.md).

## 10. Cron auto-deploy failing silently for weeks

The auto-deploy script's log (`/tmp/grabha-deploy.log`) held 639 lines of nothing but `Could not resolve host: github.com` — but a manual run at the time showed DNS/networking was fine, so those failures were stale (from an earlier version of the script or a past outage window, neither timestamped). The real problem was **observability, not DNS**: the old script was silent on every outcome (no-op, success, and failure all looked identical from outside), so a real failure could go unnoticed for weeks. Fixed 2026-07-14: the script now writes a timestamped line every run (`up to date`, `deploying...`, `FAILED: <step>`, or `deploy done`), and fetch errors are captured instead of discarded. Confirmed working 2026-09-08 against two real pushes. See [DEPLOYMENT.md](DEPLOYMENT.md).
