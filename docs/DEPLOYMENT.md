# Deployment

## Pipeline

`.github/workflows/docker-publish.yml` builds and pushes `sreeh007/grabha-api:latest` to Docker Hub on every push to `main`, plus a daily scheduled rebuild (`17 3 * * *`) so the image picks up `yt-dlp` nightly fixes even with no code changes.

## Production Topology

Runs via `docker-compose.yml` on the Anjaneya server, binding `127.0.0.1:5000` and bind-mounting the `grabha-web` checkout read-only at `/app/web`. **The compose file and the running container live in a separate checkout** (`/home/sreeh007/grabha-api-git` on the server) from whatever repo checkout you're working in locally — don't assume they're the same clone.

## Auto-Deploy

A cron job on the server (`~/grabha-deploy.sh`, `*/15 * * * *`) runs `git fetch`/`pull` + `docker compose pull && up -d` automatically whenever `main` moves. As of the 2026-07-14 fix, it logs a timestamped line every run (`up to date`, `deploying...`, `FAILED: <step>`, or `deploy done`) to `/tmp/grabha-deploy.log` — see [TROUBLESHOOTING.md](TROUBLESHOOTING.md#10-cron-auto-deploy-failing-silently-for-weeks) for why that logging exists. Confirmed working 2026-09-08.

**Timing:** push → CI build (~1m30s) → next `*/15` cron tick pulls and recreates the container. So push-to-live is CI time plus up to 15 minutes of cron wait. Don't hand-deploy inside that window — just wait for the tick.

## Manual Deploy (if ever needed)

SSH in and run, in `/home/sreeh007/grabha-api-git`:

```
docker compose pull && docker compose up -d
```

## Do Not Rebuild The Image On The Server

**Never** run `docker build`/`docker push` by hand on the server as a deploy shortcut. This is exactly how production once ended up running an `app.py` matching *no* commit in git history — a manual build from a stale/uncommitted local file overwrote the correct CI-built `latest` tag on Docker Hub and caused a full outage. See [TROUBLESHOOTING.md](TROUBLESHOOTING.md#9-production-running-code-matching-no-git-commit-at-all). If a fix is needed: commit and push to `main`, let GitHub Actions build the image, then `docker compose pull` on the server.

## Verifying What's Actually Deployed

Git history and the running container can silently diverge — not just by lagging `main`, but potentially by running code that was hand-built and corresponds to no commit at all. Before assuming deployed behavior matches a given commit:

```
docker exec grabha-api md5sum app.py
git show <commit>:app.py | md5sum
```

Compare the hashes. Also useful: `docker exec grabha-api pip show yt-dlp`, `docker logs grabha-api`, or a live test — never trust `git log` alone.

## Render.com

No longer used (`render.yaml` was removed). Don't reintroduce it as a deploy target.
