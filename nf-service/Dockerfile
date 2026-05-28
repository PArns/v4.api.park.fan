# NeuralForecast (Nixtla) service — parallel/experimental forecaster for the
# far-future daily-peak surface (holidays as known-future covariates).
# Separate image from ml-service (CatBoost) so the heavy PyTorch dependency
# tree stays isolated. Python 3.11 (neuralforecast needs >=3.10; torch has
# solid 3.11 CPU wheels).
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install PyTorch with CUDA 12.8 support (required for Blackwell sm_120 / RTX 5080).
# With runtime:nvidia the host driver libs are injected; the container only needs
# the CUDA-compiled torch wheel — no toolkit install required.
# Build arg allows overriding to cpu for CI or CPU-only hosts.
ARG TORCH_INDEX=https://download.pytorch.org/whl/cu128
RUN pip install --no-cache-dir \
    --index-url ${TORCH_INDEX} \
    "torch>=2.7.0"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/models

EXPOSE 8000

# Single worker: TFT/NHITS training is memory-heavy and the CatBoost OOM
# lesson (4 workers + training spike) applies even more to PyTorch. One worker
# keeps memory bounded; this service is batch/precompute-oriented, not
# high-QPS per-request.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log", "--workers", "1", "--timeout-keep-alive", "120", "--timeout-graceful-shutdown", "30"]
