# Architecture

## Overview

`app.py` is a single-file Flask app (~850 lines) with three responsibilities: a `yt-dlp` wrapper for probing/downloading media, an in-memory job store with background-thread execution, and a SQLite-backed activity log. It also serves the static `grabha-web` frontend in production.

## Job Lifecycle and Terminal States

`POST /api/download` creates a job (`jobs[job_id]`, an in-memory dict — not persisted) and starts `run_download()` in a daemon thread. A job's `status` moves through `queued → downloading → {done, error, cancelled}`.

The paired activity-log row is written `pending` at request time (`log_attempt()`) and only closed out (`update_log_record()`) when the job reaches a terminal state. Because the job store is in-memory, **anything that can hang leaves that row stuck `pending` forever** — this happened in production (one row sat `pending` from April to September 2026 before the fix below). Three mechanisms close this gap; do not remove any of them:

1. **`_start_watchdog()`** — a background thread that fails the job after `MAX_JOB_SECONDS` (default 900s, env-overridable), killing the job's ffmpeg subprocess and marking the job/log row `error`.
2. **`_kill_job_processes()`** — SIGKILLs the ffmpeg child, matched by `job_id` appearing in its cmdline (via the output path). Necessary because yt-dlp's progress hook — the only cancellation point available — is never invoked while ffmpeg is running as a subprocess, so a wedged ffmpeg can only be stopped from outside.
3. **`_reconcile_orphaned_jobs()`** — runs once at startup; fails any row still `pending` from a previous process (a restart kills every worker thread but leaves rows behind, since the job store isn't persisted).

A cancelled/killed job must never be retried: the transient-error retry in `run_download()` checks `not jobs[job_id].get('cancelled')` before retrying an `ffmpeg exited with code ...` error, because a watchdog kill surfaces as that exact same message.

`/api/status/<job_id>` returns a terminal `status: 'error'` JSON body (with HTTP 404) for an unknown job id, rather than a bare `{'error': ...}` — the frontend poller reads only the JSON body, so a bare error response made it poll a dead job forever.

## Output Validation

yt-dlp exiting 0 does not guarantee playable output — a post-live HLS manifest can yield a 261-byte file with a valid mp4 header and an empty `mdat` atom, which looks like success but plays nothing. `_validate_media()` runs `ffprobe` on the output after every download: checks file size, decodable stream presence, and non-zero duration before reporting success. Livestream state is also checked before download even starts, via `match_filter` (`_reject_unready_live`): `is_live`/`is_upcoming`/`post_live` are all rejected with an explanatory message rather than producing an empty "successful" file.

## Format Selection

The `format`/`format_sort` passed to yt-dlp vary along three independent axes: requested `quality` (mapped to `format_sort`'s `res:N`, not a `[height<=N]` filter), whether a clip (`clip_start`/`clip_end`) was requested (reorders `format_sort` to put `proto` first, preferring seekable plain-HTTPS formats over HLS), and platform (Instagram/Facebook use a pre-muxed-mp4-first `video_format`; everywhere else prefers adaptive H.264+AAC first). Each of these is a deliberate choice made to fix a specific regression — see [DECISIONS.md](DECISIONS.md) for the full rationale and incident history behind each one before changing any of them.

## Concurrency

`jobs` (in-memory dict) is accessed from multiple threads (request handler, background download thread, watchdog thread) without an explicit lock. **All SQLite writes**, by contrast, go through `_db_lock` (`threading.Lock()`), since Flask runs multi-threaded and SQLite connections aren't safely shared across threads without it.

## Activity Logging

Writes to two places: `~/grabha/logs/activity.db` (SQLite, structured) and `~/grabha/logs/activity.log` (plaintext) — and both are mirrored to stdout. `logging.basicConfig(filename=...)` binds Python's *root* logger to the file, and werkzeug's logger propagates to root, so without an explicit `StreamHandler(sys.stdout)` added to the root logger, `docker logs` shows nothing but the two Flask startup banner lines — `PYTHONUNBUFFERED=1` alone does not fix this.

Schema migrations use `ALTER TABLE ... ADD COLUMN` in place at startup (`_init_db()`), guarded by catching `sqlite3.OperationalError` if the column already exists — this keeps old databases compatible without a migration framework.

## Geo-IP

`_geo_lookup()` calls `ipinfo.io` for country/city/ISP on each logged attempt; failures are swallowed silently (best-effort — never blocks or fails the request). Client IP is read from `CF-Connecting-IP` (Cloudflare-set, not client-spoofable) in preference to `X-Forwarded-For` (which Cloudflare only appends to, so a client-supplied value can land first in the list).

## Admin Auth

`/admin/login` checks `username`/`password` against `ADMIN_USER`/`ADMIN_PASSWORD` env vars and, on match, issues an HMAC-SHA256 token scoped to the current calendar date (`_make_token()`/`_valid_token()`, keyed by `TOKEN_SECRET`). `/admin/data` validates the token with `hmac.compare_digest`. Not session-based — the token is valid for anyone who has it until the date rolls over, not tied to a particular login.

All three of `ADMIN_USER`, `ADMIN_PASSWORD`, and `TOKEN_SECRET` have hardcoded fallback defaults in `app.py`, used whenever the corresponding environment variable isn't set. As currently wired in `docker-compose.yml`:

- `ADMIN_PASSWORD` **is** set, from the host environment via `${ADMIN_PASSWORD}`.
- `ADMIN_USER` is **not** referenced in `docker-compose.yml` at all.
- `TOKEN_SECRET` is present only as a commented-out line (`# - TOKEN_SECRET=your-secret`).

So, as configured in this repository, production runs with `ADMIN_USER` and `TOKEN_SECRET` on their hardcoded `app.py` defaults unless something outside this repo (e.g. a server-local override not tracked here) sets them. **Security implication:** those two defaults are visible in the repository's own source history to anyone with read access to it, so relying on them in production means the username is guessable and the HMAC token's signing key is not actually secret — an attacker who knows the default `TOKEN_SECRET` could mint a valid admin token without ever calling `/admin/login`. This is a real gap in the current deployment, not addressed by this documentation pass (see the outer audit's "production credential environment wiring" finding) — flagged here for accuracy, not fixed.

## Instagram Highlight Share Links

The app's Share button on Instagram produces `instagram.com/s/<base64>?story_media_id=<pk>`, which yt-dlp doesn't recognize — it only understands the full-highlight URL (`instagram.com/stories/highlights/<id>/`), which is a playlist of every item in the highlight. `resolve_instagram_share()` decodes the share token to recover the highlight ID and rewrites the URL; `extract_story_item()` then extracts the playlist without processing it, finds the one entry matching the share's shortcode (derived from `story_media_id`), and processes only that entry — so `/api/info` and `/api/download` return/download the single shared item, not the whole highlight.

## Static File Serving

The catch-all route (`GET /<path:filename>`) serves the bind-mounted `grabha-web` checkout from `/app/web`. It rejects any path segment starting with `.` and any path ending in a blocked extension (`.db`, `.sqlite`, `.sqlite3`, `.env`, `.log`, `.py`) — added after a stray `activity.db` in that directory was found publicly reachable at `/activity.db` in production. See [TROUBLESHOOTING.md](TROUBLESHOOTING.md#3-activitydb-briefly-publicly-reachable-at-activitydb).
