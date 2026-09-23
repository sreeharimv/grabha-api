# AGENTS.md

Vendor-neutral operating contract for AI coding agents working in `grabha-api`. If you're an AI agent (Claude Code, Gemini CLI/Antigravity, OpenAI Codex, or similar), start here — this file is self-contained; you don't need to read anything outside this repository to operate in it safely.

## What This Is

`grabha-api` is the Flask backend for Grabha, a self-hosted video/audio downloader PWA. It wraps `yt-dlp` to probe and download media (video/audio) from arbitrary URLs pasted by a user, runs downloads as background jobs with progress polling, logs activity to SQLite, and serves the companion static frontend (`grabha-web`) via a catch-all route.

This repository is one part of a larger project. The outer `grabha/` folder is itself a local git repository — but with no commits and no remote, a purely local organizational wrapper, distinct from this repository's own independent history (see Repository Identity below). It documents the overall product and the relationship between this backend, the `grabha-web` frontend, and an unrelated `starmaker-downloads/` utility — treat it as parent context if you encounter it, but nothing in this file depends on it.

## Repository Identity

- Independent git repository, own remote and own history (not a subdirectory of another repo's git tree).
- Single production Python file: `app.py`. All backend logic lives there by design — see Hard Constraints.
- Companion frontend repo `grabha-web` is served by this backend in production via a read-only bind-mount (`/app/web`), but there is no code dependency between the two repos.

## What It Owns

- Media probing and downloading (`yt-dlp` wrapper) with job lifecycle management (queue → downloading → done/error/cancelled).
- Activity logging to SQLite (`activity.db`) plus a plaintext log, both under `~/grabha/logs`.
- An admin endpoint pair (`/admin/login`, `/admin/data`) protected by a daily HMAC token.
- Serving the frontend's static files in production.

## Where Authoritative Documentation Lives

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — system structure, job lifecycle, concurrency, logging, format-selection mechanics.
- [docs/API.md](docs/API.md) — endpoint reference.
- [docs/DECISIONS.md](docs/DECISIONS.md) — deliberate format/quality-selection tradeoffs and why, with explicit "don't revert" warnings.
- [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) — local dev setup, dependency/runtime requirements, schema-migration convention.
- [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) — CI/CD pipeline, auto-deploy mechanics, manual deploy procedure, drift detection.
- [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) — known issues, root causes, fixes, and operational incidents — read before assuming a past bug is fixed everywhere it could recur.
- `CLAUDE.md` — Claude Code-specific behavior only, not project knowledge.
- `GEMINI.md` — Gemini-specific adapter.

## Hard Constraints — do not change without explicit user authorization

- **`app.py` stays a single file.** Don't split it into modules.
- **Never commit `.env`**, cookie files, or anything under `logs/` — see `.gitignore`.
- Don't remove or weaken any of the three job-terminal-state mechanisms (watchdog, ffmpeg-kill-on-`job_id`, startup reconciliation of orphaned `pending` rows) — see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#job-lifecycle-and-terminal-states). Removing any one of them can leave activity rows stuck `pending` indefinitely; this has happened in production before.
- Don't drop `format_sort = ['proto', res_sort, 'br']` for clipped downloads, or widen the Instagram/Facebook pre-muxed-mp4-first format string to apply platform-wide — both were fixes for specific regressions. See [docs/DECISIONS.md](docs/DECISIONS.md).
- Don't rebuild/push the Docker image by hand on the server as a deploy shortcut — see [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md). This has previously put production on code matching no git commit at all.
- Always confirm with the user before pushing to `main` — pushes trigger a real production auto-deploy within roughly 15 minutes (via cron) once CI finishes building the image.

## When To Inspect Related Repositories

- Frontend behavior (what the app sends to these endpoints, how it renders progress/errors) lives in `grabha-web`, not here — check there if an endpoint's *consumer* behavior is in question, not its implementation.
- The outer `grabha/` folder documents the three-repo boundary and product-level requirements/status. Consult it only if you need product context beyond this backend's own scope — it is not required for ordinary backend work.

## Handling Uncertainty

- If documentation conflicts with what you observe in `app.py` or in production behavior, trust what you observe and flag the conflict rather than silently resolving it — see the drift-detection note in [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) (deployed code can diverge from git history in this project, not just by lagging behind).
- Before asserting what's actually running in production, verify (`docker exec grabha-api md5sum app.py` against `git show <commit>:app.py`) rather than trusting `git log` alone.
- When a change affects `main` or triggers a production deploy, ask before acting rather than guessing at intent.
