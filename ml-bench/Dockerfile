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
#   docker build -t ml-bench --build-context mlsvc=ml-service ml-bench
FROM python:3.11-slim

WORKDIR /app

ARG TORCH_INDEX=https://download.pytorch.org/whl/cu128
RUN pip install --no-cache-dir --index-url ${TORCH_INDEX} "torch>=2.7.0"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --from=mlsvc holiday_features.py holiday_utils.py /opt/ml-service/
ENV MLBENCH_ML_SERVICE_DIR=/opt/ml-service

COPY . .

ENTRYPOINT ["python", "-m", "mlbench"]
CMD ["--help"]
