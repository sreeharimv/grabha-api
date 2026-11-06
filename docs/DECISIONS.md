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

## Pre-muxed mp4 fallback and mobile codec compatibility (Instagram, Facebook, YouTube)

`video_format` branches by platform:
- **Instagram**: `best[ext=mp4]/bestvideo+bestaudio/best` (Instagram only serves VP9-DASH adaptive streams, so pre-muxed H.264 mp4 is preferred first).
- **Facebook**: `bestvideo[vcodec~='^(avc|h264)']+bestaudio[acodec^=mp4a]/best[ext=mp4]/bestvideo+bestaudio/best`. Facebook encoding varies per video: some videos have H.264 DASH streams with a VP9 pre-muxed 'hd' mp4, while others have VP9-only DASH streams with an H.264 pre-muxed 'hd' mp4. Preferring adaptive H.264 first, then falling back to pre-muxed mp4 before generic `bestvideo+bestaudio`, captures native H.264 in both cases.
- **YouTube and others**: `bestvideo[vcodec~='^(avc|h264)']+bestaudio[acodec^=mp4a]/bestvideo+bestaudio/best[ext=mp4]/best` (YouTube's pre-muxed mp4 is throttled/truncated, so adaptive H.264 is preferred).

**Safety Net (`_ensure_mobile_compatible_video`):**
Any non-H.264 video codec (`vp9`, `av01`) or non-AAC/MP3 audio inside an MP4 is automatically transcoded via FFmpeg (`-c:v libx264 -preset veryfast -crf 23 -movflags +faststart`) as a post-download step so no video plays as a black screen or fails to save to mobile galleries/iOS Photos.

## yt-dlp tracks the nightly channel, not stable PyPI releases

The `Dockerfile` force-upgrades to `--pre -U yt-dlp yt-dlp-ejs` after the pinned `requirements.txt` install, and the image rebuilds daily via a scheduled GitHub Actions run.

**Why:** yt-dlp's stable PyPI releases lag YouTube's PO-token/signature churn by weeks; nightlies ship extractor fixes within a day or two. Stale extractors caused intermittent `ffmpeg exited with code 8` / HTTP 403 errors on YouTube downloads that a fresh nightly reliably resolved.
