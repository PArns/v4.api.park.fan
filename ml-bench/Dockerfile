# ml-bench — offline forecasting benchmark for Park Fan (PAR-827).
# NOT part of docker-compose: an offline tool, run by hand on celestrial.
#
# GPU-ready so model adapters (Chronos-2, TimesFM-3, TiDE/TSMixerx …) can run in
# the same image: PyTorch with CUDA 12.8, the first toolkit with Blackwell
# sm_120 (RTX 5080) kernels. Check with `python -m mlbench gpu-check`.
#
# The ml-service holiday implementation is passed in as a second build context
# rather than copied into this directory: the benchmark's holiday flags must be
# exactly the ones the production models are trained on.
#   docker build -t ml-bench --build-arg GIT_SHA=$(git rev-parse HEAD) \
#     --build-context mlsvc=ml-service ml-bench
FROM python:3.11-slim

WORKDIR /app

ARG TORCH_INDEX=https://download.pytorch.org/whl/cu128
RUN pip install --no-cache-dir --index-url ${TORCH_INDEX} "torch>=2.7.0"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Optional model dependencies, one requirements file per model family, e.g.
#   docker build --build-arg EXTRA_REQUIREMENTS=requirements-foundation.txt ...
# (PAR-828: Chronos-2 + TimesFM-3). Installed before the code is copied, so a code
# change does not reinstall them.
ARG EXTRA_REQUIREMENTS=
COPY requirements*.txt ./
RUN if [ -n "$EXTRA_REQUIREMENTS" ]; then pip install --no-cache-dir -r "$EXTRA_REQUIREMENTS"; fi

COPY --from=mlsvc holiday_features.py holiday_utils.py /opt/ml-service/
ENV MLBENCH_ML_SERVICE_DIR=/opt/ml-service

COPY . .

# Provenance: every run records this SHA plus a hash of the mlbench sources
# (`docker build --build-arg GIT_SHA=$(git rev-parse HEAD) …`). A reference run
# (`run --reference`) refuses to start without it.
ARG GIT_SHA=unknown
ENV MLBENCH_GIT_SHA=${GIT_SHA}
# Also as a file: the env var is easy to lose (an entrypoint override, a shell that
# re-execs) and the image has neither a git binary nor a repository to fall back on.
# PAR-828's first subset run recorded `git_sha: "unknown"` for exactly that reason.
RUN printf '%s' "${GIT_SHA}" > /app/GIT_SHA

ENTRYPOINT ["python", "-m", "mlbench"]
CMD ["--help"]
