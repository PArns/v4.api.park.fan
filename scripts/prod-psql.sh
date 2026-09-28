#!/usr/bin/env bash
#
# Ad-hoc SQL against the production database, with the time limits an
# interactive session does not get on its own.
#
# Why this exists: on 2026-09-28 a research SELECT run through
# `docker exec … psql` stayed open after the runner process that started it had
# already exited. The TimescaleDB compression policy on `queue_data` then asked
# for a strong lock, queued behind that snapshot, and every reader queued behind
# the policy — the whole public API answered HTTP 500 for 32 minutes. Nothing
# about the query was wrong except that it had no deadline.
#
# The limits are set per session via PGOPTIONS and therefore apply to this
# invocation only. They deliberately do NOT touch the application pool, which
# connects as the same `parkfan` role (see docs/troubleshooting/db-health-runbook.md).
#
# Usage:
#   scripts/prod-psql.sh -c 'SELECT count(*) FROM parks;'
#   scripts/prod-psql.sh < query.sql
#   scripts/prod-psql.sh          # interactive
#
# Overrides, per invocation, when a query legitimately needs longer:
#   PARKFAN_PSQL_STATEMENT_TIMEOUT=15min scripts/prod-psql.sh -c '…'
#
set -euo pipefail

# 5min is generous: over the 117 days since the last pg_stat_statements reset,
# 2 of 4962 recorded statements ever exceeded it, and both were background jobs
# rather than research queries (6 exceeded one minute).
STATEMENT_TIMEOUT="${PARKFAN_PSQL_STATEMENT_TIMEOUT:-5min}"
# statement_timeout does not fire while no statement runs, so a session parked
# between BEGIN and COMMIT would hold its snapshot forever. This is the second,
# separate gap.
IDLE_TX_TIMEOUT="${PARKFAN_PSQL_IDLE_TX_TIMEOUT:-1min}"
# Keep a writing session from queueing on a lock and collecting waiters behind
# itself — the mirror image of the incident above.
LOCK_TIMEOUT="${PARKFAN_PSQL_LOCK_TIMEOUT:-10s}"

DB_USER="${PARKFAN_PSQL_USER:-parkfan}"
DB_NAME="${PARKFAN_PSQL_DB:-parkfan}"

# The container name carries a Coolify deploy id and changes with every deploy,
# so it is looked up, never hard-coded.
if [ -n "${PARKFAN_PSQL_CONTAINER:-}" ]; then
  CONTAINER="$PARKFAN_PSQL_CONTAINER"
else
  mapfile -t candidates < <(
    docker ps --filter name=postgres --format '{{.Names}}' | grep -v coolify || true
  )

  if [ "${#candidates[@]}" -eq 0 ]; then
    echo "prod-psql: no running postgres container matched 'name=postgres'." >&2
    exit 1
  fi
  if [ "${#candidates[@]}" -gt 1 ]; then
    echo "prod-psql: more than one postgres container matched; refusing to guess." >&2
    printf '  %s\n' "${candidates[@]}" >&2
    echo "Pass the one you mean via PARKFAN_PSQL_CONTAINER." >&2
    exit 1
  fi
  CONTAINER="${candidates[0]}"
fi

# -t only when stdin really is a terminal, so the script stays usable in a pipe.
tty_flag=()
if [ -t 0 ]; then tty_flag=(-t); fi

exec docker exec -i "${tty_flag[@]}" \
  -e PGOPTIONS="-c statement_timeout=${STATEMENT_TIMEOUT} -c idle_in_transaction_session_timeout=${IDLE_TX_TIMEOUT} -c lock_timeout=${LOCK_TIMEOUT}" \
  "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" "$@"
