FROM python:3.11-slim

# ffmpeg for merging/mp3; nodejs required by yt-dlp for YouTube JS extraction.
# yt-dlp requires Node >=22 for its JS challenge solver — Debian's default
# "nodejs" apt package is v20, which yt-dlp silently treats as unsupported.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg curl ca-certificates gnupg \
    && curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# app.py writes logs to ~/grabha/logs which resolves to /root/grabha/logs
RUN mkdir -p /root/grabha/logs

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# yt-dlp's stable PyPI releases lag YouTube's PO-token/signature churn by
# weeks (nightlies ship fixes within a day or two); stale extractors cause
# intermittent "ffmpeg exited with code 8" / HTTP 403 on YouTube downloads
# that a fresh nightly resolves. Force-upgrade to the nightly channel here
# so the daily scheduled image rebuild keeps this current.
RUN pip install --no-cache-dir --pre -U yt-dlp yt-dlp-ejs

COPY app.py .

EXPOSE 5000

CMD ["python", "app.py"]
