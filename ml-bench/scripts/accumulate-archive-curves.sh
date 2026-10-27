#!/usr/bin/env bash
#
# PAR-1043: keep a copy of the forward archive's served curves before they expire.
#
# forecast_archive_curves deletes a row 14 days past its TARGET date, so one export
# only ever covers about two weeks of short-lead targets — too few for the served
# arm's verdicts (docs/ml/ml-bench/served-arm.md#the-window-and-the-weekly-accumulation). Rows are never updated
# after their capture, so a complete UTC origin day exported once holds every row it
# will ever hold: exporting each new day into ONE persistent directory accumulates
# the window. `mlbench export` skips days already in that directory's manifest, so
# each run reads only the origin days since the last one (through yesterday UTC).
#
# Read-only: every query goes through scripts/prod-psql.sh (statement, idle-in-
# transaction and lock timeouts; one COPY per origin day, autocommit, one at a time).
# System python3 only — export.py is standard library.
#
# Usage (on the database host, from a checkout of this repo):
#   MLBENCH_ARCHIVE_DIR=<persistent dir> ml-bench/scripts/accumulate-archive-curves.sh
# Optional: MLBENCH_ARCHIVE_FROM (first origin day, default the archive's start).
#
# Run it at least weekly: a gap longer than ~14 days loses short-lead rows for good.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DIR="${MLBENCH_ARCHIVE_DIR:?set MLBENCH_ARCHIVE_DIR to the persistent archive directory}"
FROM="${MLBENCH_ARCHIVE_FROM:-2026-10-09}"   # forecast_archive_curves' first origin day

exec python3 "$REPO/ml-bench/mlbench/export.py" --out "$DIR" --repo "$REPO" \
  --tables archive_curves --archive-from "$FROM" --pause 1
