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

## Deploy
Render.com via `render.yaml`. Container via `Dockerfile`.
