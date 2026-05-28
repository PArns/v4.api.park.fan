"""
Gunicorn config for the ML service.

We run uvicorn workers under gunicorn so we can preload the app (preload_app=True):
the app module — and therefore the ~4 GiB CatBoost model loaded at import time in
main.py — is loaded once in the master process, and the worker processes share it
copy-on-write after fork. Two plain uvicorn workers would each load their own copy
(~8.5 GiB serving floor); combined with the nightly training subprocess that
exhausted host memory and got the training OOM-killed (the reason the service was
cut to a single worker). COW sharing keeps the serving floor near a single copy
during the training-peak window — workers only diverge to their own copy once they
reload a newly trained model, by which point training has already finished.
"""

import os

bind = "0.0.0.0:8000"
worker_class = "uvicorn.workers.UvicornWorker"
workers = int(os.environ.get("ML_WORKERS", "2"))
preload_app = True

# Mirror the timeouts the service ran with under plain uvicorn — predicts plus
# their DB queries can be slow, and we don't want gunicorn reaping a busy worker.
timeout = 120
graceful_timeout = 30
keepalive = 120

# No access log (was uvicorn --no-access-log): keeps GET /health out of the logs.
accesslog = None


def post_fork(server, worker):
    # The SQLAlchemy engine is created at import — i.e. inside the preloaded master —
    # so the worker inherits its connection pool across the fork. Sharing a TCP
    # connection between processes corrupts the wire protocol, so drop the inherited
    # pool and let the worker open its own connections. close=False abandons the
    # connections without closing the sockets (which the master still references),
    # per SQLAlchemy's multiprocessing guidance.
    #
    # Imported here, not at module top: gunicorn execs this config file before it
    # puts the app dir on sys.path, so a top-level `from db import ...` would fail.
    from db import engine

    engine.dispose(close=False)
