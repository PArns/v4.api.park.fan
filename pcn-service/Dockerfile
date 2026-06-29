# PCN service (Park-Crowd Nowcaster) — Phase 0: cross-ride tensor pipeline.
# CPU-only for now (pure pandas/numpy assembly). The CUDA torch wheel + STGNN libs
# are added to this image once the bake-off (§11.6) picks a backbone — at which point
# this mirrors nf-service's cu128 / torch>=2.7 base for Blackwell (RTX 5080, sm_120).
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

# Phase 0 is a batch tool, not a server. Default to building the tensor for all parks
# in scope; override the args at `docker run` for a single park / --save.
ENTRYPOINT ["python3", "build_cross_ride_tensor.py"]
