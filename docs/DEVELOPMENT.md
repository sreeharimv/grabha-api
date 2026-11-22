# Development

## Structure

`app.py` is the entire backend — a deliberate single-file design (see [AGENTS.md](../AGENTS.md) Hard Constraints). Don't split it into modules.

## Running Locally

There is no dev server wrapper — `app.py` *is* the entry point, in both local and production contexts.

1. `pip install -r requirements.txt`
2. Install the host prerequisites below.
3. `python app.py` — reads `PORT` from the environment (default `5000`); everything else (`ADMIN_USER`, `ADMIN_PASSWORD`, `TOKEN_SECRET`, `MAX_JOB_SECONDS`) also has an in-code default, so the app runs with no environment configuration at all. See [ARCHITECTURE.md](ARCHITECTURE.md#admin-auth) for why that's not safe to rely on for anything beyond local testing.
4. Exercise the API directly (`curl`/Postman/etc. against `/api/info`, `/api/download`, ...) — see [API.md](API.md).

### Host prerequisites

- **ffmpeg** and **ffprobe** — ffmpeg merges/transcodes downloads (`mp4` muxing, `mp3` extraction, clip trimming); ffprobe is used by `_validate_media()` to confirm output is playable. Both are installed via `apt` in the `Dockerfile`; a local run needs them on `PATH` some other way (e.g. the system package manager).
- **Node.js ≥ 22** — required by `yt-dlp`'s JS-challenge solver for YouTube extraction. See [DECISIONS.md](DECISIONS.md#yt-dlp-tracks-the-nightly-channel-not-stable-pypi-releases) and the Node requirement note below — the same "silently degrades, no exception raised" failure mode applies locally, not just in the container.

### The `/app/web` static-file limitation outside Docker

`index()` and `static_files()` hardcode `/app/web` as the frontend root (`send_from_directory("/app/web", ...)`) — there's no environment variable or config to point this elsewhere. In production this path exists because `docker-compose.yml` bind-mounts the `grabha-web` checkout there (see [DEPLOYMENT.md](DEPLOYMENT.md)). Running `app.py` directly on a dev machine, `/app/web` won't exist unless you create it (e.g. `sudo mkdir -p /app/web` and copy or symlink a `grabha-web` checkout into it) — without that, `/` and any non-`/api`/`/admin`/`/health` path will 404/error. This doesn't affect API endpoints, which don't depend on that path at all — testing the backend in isolation doesn't require the frontend to be served.

### Local development vs. production

| | Local | Production |
|---|---|---|
| Process | `python app.py` directly | Docker container (`Dockerfile`), `gunicorn`/`flask` not used — same `python app.py` entrypoint inside the image |
| Frontend | Not served unless `/app/web` is manually populated (see above) | Bind-mounted read-only from a separate `grabha-web` checkout |
| Cookies | `/app/cookies/*.txt` won't exist unless created manually — `get_cookiefile()` just returns `None` and yt-dlp proceeds unauthenticated | Bind-mounted from `/home/sreeh007/grabha-cookies` — see Cookie Provisioning below |
| Client IP | `_get_ip()` falls through to `request.remote_addr` (no Cloudflare headers present) | Behind Cloudflare — `CF-Connecting-IP` is used |
| Auth secrets | In-code defaults apply unless you export `ADMIN_USER`/`ADMIN_PASSWORD`/`TOKEN_SECRET` yourself | `ADMIN_PASSWORD` supplied via `docker-compose.yml`'s `${ADMIN_PASSWORD}`; see [ARCHITECTURE.md](ARCHITECTURE.md#admin-auth) for the current gap on the other two |
| Deploy mechanism | N/A | CI build + cron auto-pull — see [DEPLOYMENT.md](DEPLOYMENT.md) |

## Dependencies

`requirements.txt` pins `yt-dlp>=2025.1.15` (keep recent) plus `yt-dlp-ejs`, `flask`, `flask-cors`, `requests`. The Docker build additionally force-upgrades `yt-dlp`/`yt-dlp-ejs` to the nightly (`--pre`) channel after the pinned install — see [DECISIONS.md](DECISIONS.md#yt-dlp-tracks-the-nightly-channel-not-stable-pypi-releases).

## Runtime requirement: Node.js ≥ 22

`yt-dlp`'s JS-challenge solver (needed for YouTube extraction) requires Node ≥22. Debian's default `apt install nodejs` installs v20, which yt-dlp silently treats as unsupported — no exception is raised, so extraction just degrades without any visible error. The `Dockerfile` installs Node 22 explicitly via NodeSource; don't switch back to the Debian package.

## Schema Migrations

The `downloads` SQLite table is created/migrated with `ALTER TABLE ... ADD COLUMN` in place at startup (`_init_db()`), guarded against `sqlite3.OperationalError` for columns that already exist. This keeps existing production databases compatible without a migration framework — follow the same pattern for new columns rather than introducing one.

## Cookie Provisioning

`get_cookiefile()` in `app.py` maps requested URLs to a cookie file: `instagram.com` → `/app/cookies/instagram.txt`, `youtube.com`/`youtu.be` → `/app/cookies/youtube.txt`. If the mapped file doesn't exist, `get_cookiefile()` returns `None` and the download proceeds unauthenticated (which fails or degrades for content that requires a logged-in session). In production, `/app/cookies` is bind-mounted from `/home/sreeh007/grabha-cookies` on the host (see `docker-compose.yml` and [DEPLOYMENT.md](DEPLOYMENT.md)).

**Instagram (`instagram.txt`):** `tools/refresh_cookies.py` extracts cookies from a local Firefox profile via yt-dlp's `--cookies-from-browser firefox` and pushes the result to the server over SSH into `/home/sreeh007/grabha-cookies/instagram.txt` (see the script for connection details — not reproduced here). `tools/schedule_task.bat`/`.ps1` register it as a Windows Scheduled Task (every 3 days + on logon, with catch-up for missed runs). This tooling runs on a developer machine, not in the container — it's not part of the deployed API's runtime.

**Dead-session fallback (Instagram only):** if a cookie-authenticated Instagram request fails with what looks like a rejected session (`HTTP Error 401`, `Instagram sent an empty media response`, `You need to log in`, `login required`), both `/api/info` and `run_download` retry once **without** cookies (`should_retry_without_cookies()`), so public reels and posts keep working while the cookie is dead. The retry doesn't count against the transient-error retry. If it also fails, the user sees the original error, and the job log shows a `[retry] Instagram rejected the saved login…` line. Content that genuinely needs a login (stories, private accounts) still fails, so a dead cookie still needs `refresh_cookies.py`.

**YouTube (`youtube.txt`):** `app.py` looks for this file and will use it if present, but **no tooling in this repository creates, refreshes, or transfers it** — `tools/refresh_cookies.py` only targets Instagram. If `youtube.txt` exists in production, it was placed there by some process outside this repo. Treat its provisioning as undocumented/manual until someone adds equivalent tooling; don't assume a refresh cadence or mechanism for it beyond what's stated here.
