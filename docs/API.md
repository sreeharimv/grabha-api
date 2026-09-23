# API Reference

All endpoints are on `app.py`. Request/response bodies are JSON unless noted.

## `POST /api/info`

Probe a URL without downloading.

- Body: `{"url": "<string>"}`
- `200`: `{"title", "thumbnail", "duration", "uploader", "platform", "chapters"}`
- `400`: `{"error": "..."}` — missing URL, or extraction failed (private/expired/login-required content)

Resolves Instagram highlight share links (`instagram.com/s/<token>?story_media_id=...`) to the single shared story item rather than the whole highlight — see [ARCHITECTURE.md](ARCHITECTURE.md#instagram-highlight-share-links).

## `POST /api/download`

Start a download job.

- Body: `{"url", "format" (default "mp4"), "quality" (default "best"), "clip_start", "clip_end"}` — `clip_start`/`clip_end` optional, `hh:mm:ss` or `mm:ss` strings
- `format`: `mp4` (video) or `mp3` (audio-only, via `FFmpegExtractAudio`)
- `quality`: `1080`, `720`, `480`, `360`, or `best` (any other value falls back to unconstrained `res` sort — see [ARCHITECTURE.md](ARCHITECTURE.md#format-selection))
- `200`: `{"job_id": "<uuid>"}`

Runs in a background thread; poll `/api/status/<job_id>` for progress. Every call writes an activity-log row with status `pending` immediately, before the job starts — see [ARCHITECTURE.md](ARCHITECTURE.md#job-lifecycle-and-terminal-states) for why that row is guaranteed to reach a terminal state.

Like `/api/info`, this endpoint also resolves Instagram highlight share links (`instagram.com/s/<token>?story_media_id=...`) to the single shared item before downloading — see [ARCHITECTURE.md](ARCHITECTURE.md#instagram-highlight-share-links).

## `GET /api/status/<job_id>`

Poll job progress.

- `200`: `{"status", "log", "progress", "progress_detail", "title", "filename", "error"}` — `status` is one of `queued`, `downloading`, `done`, `error`, `cancelled`.
- `404`: same shape, with `status: "error"` and an explanatory message.

**The frontend poller only reads the JSON body, not the HTTP status.** An unknown `job_id` (e.g. after a server restart, since the job store is in-memory) must still return a terminal `status` in the body, or the frontend polls forever.

## `GET /api/download/<job_id>`

Stream the finished file as an attachment. `404` if the job is unknown, `400` if not yet `done`.

**The file is only available for 300 seconds (5 minutes) after the job reaches `done`** — `run_download()` schedules `cleanup_file(file_path, 300)`, which deletes the output file (and its per-job directory, if left empty) on a timer regardless of whether it was ever downloaded. A request made after that window still passes the `status == 'done'` check (the in-memory job record isn't cleared), but `send_file()` then fails against a missing path — this currently surfaces as an unhandled server error rather than a clean `404`/`410`, which is a known gap (not addressed by this documentation pass; see the outer audit's engineering findings).

## `POST /api/cancel/<job_id>`

Request cancellation. Sets an in-memory flag; actual cancellation happens next time yt-dlp's progress hook fires, or via the ffmpeg-kill path if a subprocess is already running — see [ARCHITECTURE.md](ARCHITECTURE.md#job-lifecycle-and-terminal-states). Returns immediately with `{"status": "cancelling", ...}`, or `{"status": "<terminal-status>", ...}` if the job already finished. `404` if unknown.

## `GET /api/proxy-thumb?url=<url>`

Proxies a thumbnail image (avoids CORS/hotlink issues fetching thumbnails client-side). `502` on fetch failure.

## `GET /health`

`{"status": "ok", "service": "grabha"}`. No auth.

## `POST /admin/login`

- Body: `{"username", "password"}`
- On match against `ADMIN_USER`/`ADMIN_PASSWORD` env vars: `{"token": "<hmac-hex>"}` — valid for the current calendar date (see [ARCHITECTURE.md](ARCHITECTURE.md#admin-auth)).
- `401` on mismatch.

## `GET /admin/data`

`Authorization: Bearer <token>` header required (token from `/admin/login`). Returns paginated activity log rows plus aggregate stats. HMAC-protected, not session-based — a valid token is date-scoped, not tied to a particular login.

Query parameters (all optional):
- `page` — 1-indexed page number, default `1`, clamped to a minimum of `1`. Page size is fixed at 20 rows.
- `q` — free-text search, matched with `LIKE '%...%'` against `ip_address`, `country`, `city`, `title`, and `platform` (any one matching is enough).
- `status` — exact match against the `status` column (e.g. `success`, `error`, `pending`, `cancelled`).

Response `200`:
```
{
  "rows": [ { "id", "timestamp", "ip_address", "country", "city", "platform",
              "format", "quality", "device", "status", "title", "error_msg" }, ... ],
  "total": <int>,           // rows matching the current q/status filter
  "pages": <int>,           // ceil(total / 20), minimum 1
  "page": <int>,            // echoes the requested page
  "stats": { "total", "success", "errors", "pending" },     // unfiltered, across all rows
  "top_countries": [ { "country", "n" }, ... up to 5 ],      // status='success' only
  "top_platforms": [ { "platform", "n" }, ... up to 5 ],     // status='success' only
  "top_cities":    [ { "city", "n" }, ... up to 5 ]          // status='success' only
}
```
`stats`/`top_*` are computed over the *entire* table, independent of `q`/`status` — only `rows`/`total`/`pages` respect the filter.

## `GET /`, `GET /<path:filename>`

Serves `grabha-web`'s static checkout from `/app/web` (production bind-mount). Rejects dotfile path segments and blocked extensions (`.db`, `.sqlite`, `.sqlite3`, `.env`, `.log`, `.py`) — see [TROUBLESHOOTING.md](TROUBLESHOOTING.md#3-activitydb-briefly-publicly-reachable-at-activitydb) for the incident that motivated the blocklist.
