# botaghota — zero-dependency crawler image.
# Build:  docker build -t botaghota .
# Run:    docker run -v botaghota-data:/data botaghota https://example.com/
FROM python:3.12-slim

WORKDIR /app
COPY botaghota.py /app/botaghota.py

# Crawl state lives here; mount a volume to persist it.
VOLUME /data
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "/app/botaghota.py", "--data-dir", "/data"]
CMD ["--help"]
