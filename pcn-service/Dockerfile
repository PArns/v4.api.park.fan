# PCN service (Park-Crowd Nowcaster) — cross-ride tensor pipeline + GP-STGNN bake-off.
# Python 3.11 (torch has solid 3.11 wheels; AGCRN/STGNN needs >=3.10).
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# PyTorch with CUDA 12.8 — REQUIRED for Blackwell sm_120 (RTX 5080). With
# runtime:nvidia the host driver libs are injected; the container only needs the
# CUDA-compiled wheel. Override TORCH_INDEX=.../whl/cpu for CI / CPU-only hosts.
# (Same base as nf-service so both GPU services share the toolchain.)
ARG TORCH_INDEX=https://download.pytorch.org/whl/cu128
RUN pip install --no-cache-dir --index-url ${TORCH_INDEX} "torch>=2.7.0"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/models

# Batch/precompute tool, not a server. Default to building the tensor for all parks in
# scope; override args at `docker run` (e.g. `backtest.py --tensor …` for the bake-off).
ENTRYPOINT ["python3", "build_cross_ride_tensor.py"]
