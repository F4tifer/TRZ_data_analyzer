FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    ANALYZER_DB_URL=sqlite:////data/analyzer_meta.db \
    ANALYZER_ARTIFACT_DIR=/data/artifacts

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

COPY . /app

RUN mkdir -p /data/artifacts

EXPOSE 8000

CMD ["sh", "-c", "uvicorn hybrid_app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
