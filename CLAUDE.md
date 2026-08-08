# grabha-api

Single-file Flask API wrapping yt-dlp. All logic in `app.py` — keep it that way.

## Endpoints
- `POST /download` → job ID
- `GET /status/<job_id>` → progress
- `GET /file/<job_id>` → stream file
- `GET /admin/*` → activity log/stats (HMAC-protected)

## Rules
- All SQLite writes require `_db_lock` (Flask is multi-threaded).
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
`main`. Redeploying to Anjaneya requires manually SSHing in and running
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
`~/grabha-deploy.sh.bak-2026-07-14`. **Still worth a periodic glance at
`/tmp/grabha-deploy.log`** for a few days to confirm it's actually
deploying on new pushes, not just heartbeating.

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
