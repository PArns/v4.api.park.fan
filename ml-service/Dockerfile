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
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create models directory
RUN mkdir -p /app/models

# Expose port
EXPOSE 8000

# Run via gunicorn managing uvicorn workers (config: gunicorn.conf.py).
#   - preload_app=True loads the ~4 GiB CatBoost model once in the master; the 2
#     workers share it copy-on-write instead of each loading their own copy. This
#     is what makes >1 worker affordable again: 2 plain uvicorn workers held
#     ~8.5 GiB and, stacked on the 9 GiB nightly training subprocess + the API
#     container, exhausted the 28 GiB host and got the training OOM-killed (the
#     reason it was previously cut to 1 worker).
#   - Predict endpoints are sync def, so each worker also serves requests
#     concurrently from a threadpool (their DB queries overlap) instead of
#     serializing on the event loop.
# Worker count is overridable via ML_WORKERS.
CMD ["gunicorn", "main:app", "-c", "gunicorn.conf.py"]
