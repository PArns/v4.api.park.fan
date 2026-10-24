"""
Gunicorn config for the ML service.

We run uvicorn workers under gunicorn with preload_app=True so the app module is
imported once in the master and forked to the workers (cheap shared startup; the
CatBoost model itself is only ~12 MB on disk, so model memory is negligible — the
old "~4 GiB model / COW serving floor" reasoning was wrong and is removed).

The real memory risk is per-worker GROWTH, not the model. The serving caches in
predict.py/db.py (_recent_wait_times_cache, _weather_historical_cache, …) check a
TTL on read but never EVICT stale entries, and their keys include base_time +
attraction combos, so the keyspace is effectively unbounded. Left unbounded the
workers grew to ~15 GiB combined over ~25 h and pushed the host into swap.

max_requests recycles each worker after a bounded number of requests (re-forking
fresh from the preloaded master), which caps that growth regardless of the cache
leak. It's the safety net; properly bounding/evicting those caches is the real
follow-up fix.
"""

import os

bind = "0.0.0.0:8000"
# uvicorn.workers is deprecated upstream; the same worker now ships as uvicorn-worker.
worker_class = "uvicorn_worker.UvicornWorker"
workers = int(os.environ.get("ML_WORKERS", "2"))
preload_app = True
# gunicorn >= 25.1 opens a gunicornc control socket by default (a runtime-management
# surface that can resize or stop the service, restarted around every fork); unused here.
control_socket_disable = True

# Mirror the timeouts the service ran with under plain uvicorn — predicts plus
# their DB queries can be slow, and we don't want gunicorn reaping a busy worker.
timeout = 120
graceful_timeout = 30
keepalive = 120

# No access log (was uvicorn --no-access-log): keeps GET /health out of the logs.
accesslog = None

# Recycle workers periodically to bound the never-evicting serving caches (see the
# module docstring). At recycle a worker re-forks from the preloaded master, so it
# starts back at the small serving floor. jitter staggers the two workers so they
# don't recycle on the same request and briefly drop capacity together. Tunable via
# env if the per-request leak rate changes.
max_requests = int(os.environ.get("ML_MAX_REQUESTS", "1000"))
max_requests_jitter = int(os.environ.get("ML_MAX_REQUESTS_JITTER", "200"))


def pre_fork(server, worker):
    # Every fork copies the master's model, and the master loaded it at import.
    # After a retrain (/model/reload wrote a new sentinel) a worker recycled by
    # max_requests would start on the stale model and swap on its first request;
    # refresh the master first so the fork already carries the active version.
    # Never blocks the fork: a failed reload leaves the old model in place.
    try:
        import main

        main._get_active_model()
    except Exception as e:
        server.log.warning(f"pre_fork model refresh failed: {e}")


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
