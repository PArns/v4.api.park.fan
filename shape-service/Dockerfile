# Shape service (Level×Shape day-curve expander) — SHADOW. CPU-only, lightweight: the
# served NP additive+smooth model is pure pandas/numpy (no torch).
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/models

EXPOSE 8000

# Batch/precompute-oriented (nightly build, daily forecast/score); single worker. The batch
# tools stay runnable via `docker exec <shape> python3 {build,forecast,score}.py`.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log", "--workers", "1", "--timeout-keep-alive", "120"]
