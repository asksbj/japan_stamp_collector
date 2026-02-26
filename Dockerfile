FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    APP_HOME=/app

WORKDIR ${APP_HOME}

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pip-requirements ./pip-requirements
RUN pip install --no-cache-dir -r pip-requirements

COPY . .

EXPOSE 8000

# Default run FastAPI server; can be overridden via args/env
ENV APP_MODE=web \
    APP_HOST=0.0.0.0 \
    APP_PORT=8000 \
    SCHEDULER_THREADS=5

ENTRYPOINT ["python", "container_entry.py"]

