#!/usr/bin/env bash
# SessionStart hook — make pcn-service runnable in web sessions.
# Idempotent + non-blocking: installs only the LIGHTWEIGHT python test deps (torch is
# intentionally skipped — the GP-STGNN model tests skip gracefully without it, so the
# pure tensor/windowing/metrics/score suite still runs). Never fails the session.
python3 -c "import pandas, numpy, pytest" 2>/dev/null \
  || pip install -q pandas==2.2.3 numpy==1.26.4 pytest ruff 2>/dev/null \
  || true
echo "pcn-service ready → cd pcn-service && python3 -m pytest -q"
exit 0
