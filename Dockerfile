FROM python:3.11-slim

# ffmpeg is required by yt-dlp for merging streams and mp3 conversion
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg nodejs \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# app.py writes logs to ~/grabha/logs which resolves to /root/grabha/logs
RUN mkdir -p /root/grabha/logs

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 5000

CMD ["python", "app.py"]
