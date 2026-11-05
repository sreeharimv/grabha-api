# Decisions

Deliberate tradeoffs in format/quality selection, with the regression each one fixed. Don't revert these without re-reading the "why" — several were reintroduced accidentally once already.

## YouTube must prefer H.264 + AAC

`video_format` for non-Instagram/Facebook platforms is
`bestvideo[vcodec~='^(avc|h264)']+bestaudio[acodec^=mp4a]/bestvideo+bestaudio/best[ext=mp4]/best` —
H.264+AAC first, generic adaptive as fallback.

**Why:** YouTube's top adaptive video track is now AV1 with Opus audio. Muxed into an mp4 container, that plays audio-only on most phones, and iOS Photos refuses to save it at all — yet the server still reports the download as successful. Fixed 2026-09-17.

**Cost:** "best" now tops out at 1080p, the highest rung with an H.264 track. Accepted tradeoff for playback compatibility.

## Quality caps the short side, not a height filter

Quality is applied via `format_sort`'s `res:N` field (`res:1080`, `res:720`, etc. — `min(width, height)`), never a `[height<=N]` filter.

**Why:** A height filter caps the *long* side of the frame. For landscape video that's usually also the short side, so it looked correct — but for vertical video (e.g. a YouTube Short) a `[height<=480]` filter constrains height while width is unconstrained, so a 1080×1920 source at "480" came out **240×426**, not the intended ~480-wide frame. Fixed 2026-09-17.

**Caveat:** on Instagram/Facebook the cap often has no visible effect — usually only one H.264 rendition exists at all, and smaller renditions are VP9/AV1-only. Compatible playback wins over hitting the exact requested size on those platforms.

## Clipped downloads must not select an HLS format

When `clip_start`/`clip_end` are set, `run_download()` sets `ydl_opts['format_sort'] = ['proto', res_sort, 'br']` (protocol sorted before quality).

**Why:** Clipping runs through ffmpeg's `-ss`, which seeks cheaply only on a plain HTTPS URL (HTTP range request). On an HLS playlist, ffmpeg walks every segment from the start of the stream and discards the ones before the clip point. YouTube's "Premium" renditions (itag 616 and similar) are HLS-only and win on codec preference by default without this sort — a 17-second clip from a 2h35m video read ~150MB and wrote nothing for 8m25s before this fix.

## Pre-muxed mp4 first is Instagram-only, not global or Facebook

`video_format` branches on platform: `best[ext=mp4]/bestvideo+bestaudio/best` for Instagram only; everything else (including Facebook and YouTube) prefers adaptive H.264+AAC first (`bestvideo[vcodec~='^(avc|h264)']+bestaudio[acodec^=mp4a]/bestvideo+bestaudio/best[ext=mp4]/best`).

**Why:** Instagram only serves VP9-DASH adaptive streams, and merging those produces a file most players show as audio-only — so pre-muxed mp4 is necessary there. Facebook, however, serves DASH video in H.264 (e.g. 720p/360p) with AAC audio while its pre-muxed 'hd' mp4 is VP9. Grouping Facebook with Instagram caused Facebook downloads to pick the VP9 file, which WhatsApp and mobile galleries reject. YouTube's pre-muxed mp4 (itag 18) is throttled/truncated by YouTube. Thus, only Instagram uses pre-muxed mp4 first. Don't re-widen the Instagram branch to apply to Facebook or platform-wide again.

## yt-dlp tracks the nightly channel, not stable PyPI releases

The `Dockerfile` force-upgrades to `--pre -U yt-dlp yt-dlp-ejs` after the pinned `requirements.txt` install, and the image rebuilds daily via a scheduled GitHub Actions run.

**Why:** yt-dlp's stable PyPI releases lag YouTube's PO-token/signature churn by weeks; nightlies ship extractor fixes within a day or two. Stale extractors caused intermittent `ffmpeg exited with code 8` / HTTP 403 errors on YouTube downloads that a fresh nightly reliably resolved.
