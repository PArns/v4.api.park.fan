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
#   PARKFAN_PSQL_IDLE_TX_TIMEOUT, PARKFAN_PSQL_LOCK_TIMEOUT — the other two limits
#   PARKFAN_PSQL_CONTAINER, PARKFAN_PSQL_USER, PARKFAN_PSQL_DB — target and role
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
  # Keep `docker ps` failing (daemon down, no socket permission) apart from it
  # succeeding with no match: the two need different answers, and reporting the
  # first as the second sends whoever is debugging down the wrong path.
  if ! running=$(docker ps --filter name=postgres --format '{{.Names}}' 2>&1); then
    echo "prod-psql: 'docker ps' failed — is the daemon reachable, and are you in the docker group?" >&2
    printf '  %s\n' "$running" >&2
    exit 1
  fi

  candidates=()
  while IFS= read -r name; do
    [ -n "$name" ] || continue
    case "$name" in *coolify*) continue ;; esac
    candidates+=("$name")
  done <<<"$running"

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

# -t only when both ends really are a terminal. Testing stdin alone is not
# enough: called from an interactive shell with the output redirected, docker's
# pseudo-TTY would turn every \n into \r\n and psql would start its pager,
# which is exactly the case where the output is being captured.
tty_flag=()
if [ -t 0 ] && [ -t 1 ]; then tty_flag=(-t); fi

exec docker exec -i "${tty_flag[@]}" \
  -e PGOPTIONS="-c statement_timeout=${STATEMENT_TIMEOUT} -c idle_in_transaction_session_timeout=${IDLE_TX_TIMEOUT} -c lock_timeout=${LOCK_TIMEOUT}" \
  "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" "$@"
