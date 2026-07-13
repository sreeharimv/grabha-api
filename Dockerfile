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

COPY app.py .

EXPOSE 5000

CMD ["python", "app.py"]
