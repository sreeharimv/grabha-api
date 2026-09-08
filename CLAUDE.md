# grabha-api

Single-file Flask API wrapping yt-dlp. All logic in `app.py` — keep it that way.

## Endpoints
(Corrected 2026-09-08 against `grep '@app.route' app.py` — the old list here
was missing the `/api` prefix and named routes that don't exist.)
- `POST /api/info` → probe a URL without downloading
- `POST /api/download` → job ID
- `GET  /api/status/<job_id>` → progress
- `GET  /api/download/<job_id>` → stream the finished file
- `POST /api/cancel/<job_id>` → request cancellation
- `GET  /api/proxy-thumb` → thumbnail proxy
- `GET  /health`
- `POST /admin/login`, `GET /admin/data` → activity log/stats (HMAC-protected)
- `GET  /` and `GET /<path:filename>` → serve `grabha-web` from the `/app/web`
  bind-mount

## Rules
- All SQLite writes require `_db_lock` (Flask is multi-threaded).
- **Clipping must not select an HLS format.** When `clip_start`/`clip_end` are
  set, `run_download()` sets `ydl_opts['format_sort'] = ['proto', 'res', 'br']`.
  Clipping runs through ffmpeg's `-ss`, which seeks cheaply only on a plain
  HTTPS URL (HTTP range request); on an HLS playlist ffmpeg walks segments
  from the start and discards them. YouTube's "Premium" renditions (itag 616
  and friends) are HLS-only and win on codec preference by default, so
  without that sort a 17s clip from a 2h35m video read ~150 MB and wrote
  nothing for 8m25s. Don't drop it (see outer `grabha/CLAUDE.md` Known
  Issue #8).
- **Every job must have a terminal state.** The activity row is written
  `pending` at request time and only closed out when the job ends, and the
  job store is in-memory, so anything that can hang leaves a row stuck
  forever (one sat `pending` from April to September). Three things keep
  that from recurring — don't remove any of them: `_start_watchdog()`
  (fails the job after `MAX_JOB_SECONDS`, default 900, env-overridable) and
  `_kill_job_processes()` (SIGKILLs the ffmpeg child, matched on job_id in
  its cmdline — necessary because yt-dlp's progress hook, our only
  cancellation point, is never called while ffmpeg runs); `socket_timeout`
  (yt-dlp leaves it unset, so a stalled read blocks forever); and
  `_reconcile_orphaned_jobs()` at startup, which fails any row left
  `pending` by a dead process.
- A cancelled/killed job must never be retried. The transient-error retry
  checks `not jobs[job_id].get('cancelled')` — a watchdog kill surfaces as
  "ffmpeg exited with code ..." and would otherwise be retried as transient.
- `/api/status` returns a terminal `status: 'error'` body (with HTTP 404) for
  an unknown job id. The frontend poller reads only the JSON body, so a bare
  `{'error': ...}` made it poll a dead job forever.
- The catch-all static route serves the bind-mounted web checkout, so
  anything dropped in that directory is public. It rejects dotfiles and
  `_BLOCKED_STATIC_EXT` (`.db`, `.sqlite`, `.env`, `.log`, `.py`) — a stray
  `activity.db` there really was reachable at `grabha.in/activity.db`
  (found and deleted 2026-09-08).
- Logging goes to **both** `~/grabha/logs/activity.log` and stdout.
  `logging.basicConfig(filename=...)` binds the *root* logger to a file and
  werkzeug propagates to it, which is why `docker logs` showed nothing but
  two Flask banner lines for months. `PYTHONUNBUFFERED` alone does not fix
  this — the explicit `StreamHandler(sys.stdout)` is what does.
- Schema migrations use `ALTER TABLE` in-place (backwards compat).
- Geo-IP lookup is best-effort; failures are swallowed silently.
- yt-dlp pinned to `>=2025.1.15` — keep recent.
- Node.js must be **>=22** (installed via NodeSource in `Dockerfile`, not Debian's
  default `apt install nodejs` which is v20) and `yt-dlp-ejs` must be in
  `requirements.txt`. Without both, yt-dlp's JS-challenge solver silently
  reports every runtime as "unsupported" and YouTube extraction degrades —
  no exception is raised, so this fails silently.
- `run_download()`'s `quality_map` is platform-conditional: pre-muxed mp4
  (`best[ext=mp4]` first) only for Instagram/Facebook, where it's needed to
  avoid VP9-DASH merges that play back as audio-only. Everywhere else
  (notably YouTube) prefer adaptive `bestvideo+bestaudio` first — YouTube's
  pre-muxed itag-18 stream is sometimes served truncated/throttled by
  YouTube regardless of auth/cookies, producing a tiny "successful" download.
  Don't re-widen the mp4-first branch to apply globally again (this was
  the exact regression fixed 2026-07-13, originally introduced by a fix
  for the Instagram-only issue that was accidentally applied everywhere).

## Deploy
Container via `Dockerfile`, built and pushed to `sreeh007/grabha-api:latest`
on Docker Hub by `.github/workflows/docker-publish.yml` on every push to
`main`. Redeploying to Anjaneya normally happens on its own via the cron below;
the manual equivalent is SSHing in and running
`docker compose pull && docker compose up -d` in `/home/sreeh007/grabha-api-git`
(note: this is a separate checkout from the repo you're likely working in —
it's the one actually wired into the running container, along with its own
`docker-compose.yml` that binds `127.0.0.1:5000` and bind-mounts
`/home/sreeh007/grabha-web:/app/web:ro`). Render.com is no longer used
(`render.yaml` was removed) — don't reintroduce it as a deploy target.

There is also a cron auto-deploy script, `~/grabha-deploy.sh` on Anjaneya
(`*/15 * * * *`), meant to `git fetch`/`pull` + `docker compose pull && up -d`
automatically when `main` moves. As of 2026-07-14, `/tmp/grabha-deploy.log`
was 639 lines of nothing but `Could not resolve host: github.com` — but a
manual cron-equivalent run at the time showed current DNS/networking is
fine, so those failures were stale (likely from an earlier version of the
script, or a past network outage window, neither timestamped). **Root
problem was observability, not just DNS:** the old script was silent on
every outcome — no-op, success, and failure all looked identical from
outside — so this could fail again for weeks without anyone noticing. Fixed
2026-07-14: the script now writes a timestamped line on every run (`up to
date`, `deploying...`, `FAILED: <step>`, or `deploy done`), and fetch
errors are captured instead of discarded. Old log archived to
`/tmp/grabha-deploy.log.old-2026-07-14`; old script backed up to
`~/grabha-deploy.sh.bak-2026-07-14`. ~~**Still worth a periodic glance at `/tmp/grabha-deploy.log`**~~ —
**confirmed working 2026-09-08.** Watched two real pushes go out the same
hour: push → Actions build (~1m30s) → the next `*/15` tick pulls and
recreates the container, logging `deploy done (now at commit <sha>, image
<sha256>)`; idle ticks log `up to date ... nothing to do`. So push-to-live
is CI time plus up to 15 minutes of cron wait — don't hand-deploy inside
that window, just wait for the tick.

**Do not rebuild the image locally on the server as a shortcut.** This is
exactly how production ended up running `app.py` that matched *no* commit
in git history (confirmed 2026-07-14, see outer `grabha/CLAUDE.md` Known
Issue #4): a manual `docker build`/`push` was done from a stale/uncommitted
local `app.py`, overwriting the correct CI-built `latest` tag on Docker Hub
and causing a full outage. If you need to deploy a fix, commit and push it
to `main`, let GitHub Actions build the image, then `docker compose pull`
on Anjaneya — don't build-and-push by hand.

**Known drift risk:** git history and the running container can silently
diverge, and not just by lagging behind `main` — the container's `app.py`
may not correspond to *any* commit at all if it was hand-built (see above).
Before assuming deployed behavior matches a given commit, check what's
actually running: `docker exec grabha-api md5sum app.py` and diff/hash-match
against `git show <commit>:app.py`, or `docker exec grabha-api pip show ...`,
`docker logs`, or a live test — never trust `git log` alone.
