FROM python:3.13-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create models directory
RUN mkdir -p /app/models

# Expose port
EXPOSE 8000

# Run FastAPI with uvicorn (production mode)
# --no-access-log: Suppress HTTP request logs (GET /health spam)
# --workers 1: single worker process. Lowered from 2 (and earlier 4) — the
#   service is internal (NestJS calls it, not high-QPS public). Each worker
#   loads the CatBoost model + feature pipeline into its own memory (~4 GiB),
#   so two workers held ~8.5 GiB at serving time; stacked on the 9 GiB training
#   subprocess + the API container, that exhausted the 28 GiB host and the
#   global OOM-killer killed the nightly training. One worker roughly halves
#   the serving-side floor; predict requests serialize but the batch/low-QPS
#   internal traffic tolerates that.
# --timeout-keep-alive 120: Increase keep-alive timeout to prevent connection resets
# --timeout-graceful-shutdown 30: Allow time for graceful shutdown
# Application logs (logger.info) are still shown
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log", "--workers", "1", "--timeout-keep-alive", "120", "--timeout-graceful-shutdown", "30"]
