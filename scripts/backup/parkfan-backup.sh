#!/usr/bin/env bash
# parkfan-backup.sh — Daily backup: PostgreSQL base backup + ML/NF models → Samba NAS
#
# NAS layout:
#   Backups/
#   └── parkfan/
#       ├── db/
#       │   ├── 2026-05-28/
#       │   │   └── parkfan_20260528_030001.tar.gz   ← pg_basebackup (full cluster)
#       │   └── 2026-05-29/
#       │       └── parkfan_20260529_030012.tar.gz
#       ├── ml-models/
#       │   ├── catboost_v20260528_1809.cbm
#       │   ├── metadata_v20260528_1809.pkl
#       │   └── active_version.txt
#       └── nf-models/
#           └── (TFT model files)
#
# Deploy to host:
#   scp scripts/backup/parkfan-backup.sh <user>@<host>:/opt/parkfan/backup.sh
#   chmod +x /opt/parkfan/backup.sh
#   cp scripts/backup/backup.env.example /opt/parkfan/backup.env   # fill in secrets
#
# Cron (root): 0 3 * * * /opt/parkfan/backup.sh >> /var/log/parkfan-backup.log 2>&1
#
# Restore:
#   1. Stop postgres container
#   2. rm -rf /data/parkfan/postgres && mkdir -p /data/parkfan/postgres
#   3. tar -xzf parkfan_<date>.tar.gz -C /data/parkfan/postgres
#   4. Start postgres container — it will find the cluster and skip re-init

set -euo pipefail

# ── Load config ────────────────────────────────────────────────────────────────
ENV_FILE="${BACKUP_ENV_FILE:-/opt/parkfan/backup.env}"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck source=/dev/null
  set -a; source "$ENV_FILE"; set +a
fi

: "${BACKUP_NAS_HOST:?BACKUP_NAS_HOST not set}"
: "${BACKUP_NAS_SHARE:?BACKUP_NAS_SHARE not set}"
: "${BACKUP_NAS_USER:?BACKUP_NAS_USER not set}"
: "${BACKUP_NAS_PASSWORD:?BACKUP_NAS_PASSWORD not set}"
: "${DB_USERNAME:?DB_USERNAME not set}"
: "${DB_PASSWORD:?DB_PASSWORD not set}"

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5433}"
DB_DATABASE="${DB_DATABASE:-parkfan}"
POSTGRES_DATA_DIR="${POSTGRES_DATA_DIR:-/data/parkfan/postgres}"
ML_MODELS_DIR="${ML_MODELS_DIR:-/data/parkfan/ml-models}"
NF_MODELS_DIR="${NF_MODELS_DIR:-/data/parkfan/nf-models}"
BACKUP_RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-7}"
BACKUP_ML_MODELS_KEEP="${BACKUP_ML_MODELS_KEEP:-7}"

MOUNT_POINT="/mnt/parkfan-nas-backup"
DATE=$(date +%Y-%m-%d)
DATETIME=$(date +%Y%m%d_%H%M%S)
LOG_PREFIX="[parkfan-backup $DATETIME]"

log()  { echo "$LOG_PREFIX $*"; }
die()  { log "ERROR: $*" >&2; exit 1; }

# ── Mount / Unmount ────────────────────────────────────────────────────────────
cleanup() {
  if mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
    umount "$MOUNT_POINT" && log "NAS unmounted"
  fi
}
trap cleanup EXIT

mkdir -p "$MOUNT_POINT"

if mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
  log "NAS already mounted, reusing"
else
  mount -t cifs "//$BACKUP_NAS_HOST/$BACKUP_NAS_SHARE" "$MOUNT_POINT" \
    -o "username=$BACKUP_NAS_USER,password=$BACKUP_NAS_PASSWORD,vers=3.0,iocharset=utf8,file_mode=0600,dir_mode=0700" \
    || die "Failed to mount NAS at //$BACKUP_NAS_HOST/$BACKUP_NAS_SHARE"
  log "NAS mounted: //$BACKUP_NAS_HOST/$BACKUP_NAS_SHARE → $MOUNT_POINT"
fi

NAS_ROOT="$MOUNT_POINT/parkfan"
NAS_DB_DATE_DIR="$NAS_ROOT/db/$DATE"
NAS_ML_DIR="$NAS_ROOT/ml-models"
NAS_NF_DIR="$NAS_ROOT/nf-models"
mkdir -p "$NAS_DB_DATE_DIR" "$NAS_ML_DIR" "$NAS_NF_DIR"

# ── PostgreSQL Backup (pg_basebackup) ─────────────────────────────────────────
# pg_basebackup creates a consistent binary copy of the entire cluster including
# TimescaleDB hypertables and chunks — no TimescaleDB-specific restore steps
# needed. Restore = extract tar.gz to /data/parkfan/postgres, start container.
DUMP_FILE="$NAS_DB_DATE_DIR/parkfan_${DATETIME}.tar.gz"

log "Starting pg_basebackup (host=$DB_HOST port=$DB_PORT)..."

PG_CONTAINER=$(docker ps --filter name=postgres --format '{{.Names}}' | grep -v coolify | head -1)
[[ -z "$PG_CONTAINER" ]] && die "No running postgres container found"
log "Using postgres container: $PG_CONTAINER"

# Write backup to a temp dir inside the container (needed because -Xs WAL
# streaming requires a real directory target, not stdout). Then tar+gzip the
# result to the NAS. Temp dir is cleaned up in the container afterwards.
docker exec "$PG_CONTAINER" bash -c "
  rm -rf /tmp/pgbackup && mkdir -p /tmp/pgbackup
  PGPASSWORD='$DB_PASSWORD' pg_basebackup \
    -h localhost -U $DB_USERNAME \
    -Ft -z -Xs -P \
    -D /tmp/pgbackup
" || die "pg_basebackup failed"

# Stream the backup files from the container to the NAS
docker exec "$PG_CONTAINER" tar -cf - -C /tmp/pgbackup . \
  | gzip > "$DUMP_FILE" \
  || die "Failed to stream backup from container"

# Clean up temp dir in container
docker exec "$PG_CONTAINER" rm -rf /tmp/pgbackup

DUMP_SIZE=$(du -sh "$DUMP_FILE" | cut -f1)
log "DB backup done: parkfan/db/$DATE/$(basename "$DUMP_FILE") ($DUMP_SIZE)"

# Rolling retention
DELETED_DIRS=0
while IFS= read -r -d '' dir; do
  DIR_DATE=$(basename "$dir")
  if [[ $(date -d "$DIR_DATE" +%s 2>/dev/null || echo 0) -lt $(date -d "$BACKUP_RETENTION_DAYS days ago" +%s) ]]; then
    rm -rf "$dir"
    log "Removed old backup dir: parkfan/db/$DIR_DATE"
    (( DELETED_DIRS++ )) || true
  fi
done < <(find "$NAS_ROOT/db" -mindepth 1 -maxdepth 1 -type d -print0 2>/dev/null)
(( DELETED_DIRS == 0 )) && log "No old DB backup dirs to remove (retention: ${BACKUP_RETENTION_DAYS}d)"

# ── ML Model Backup (CatBoost) ─────────────────────────────────────────────────
log "Backing up last $BACKUP_ML_MODELS_KEEP CatBoost model versions from $ML_MODELS_DIR"

mapfile -t SOURCE_VERSIONS < <(
  find "$ML_MODELS_DIR" -maxdepth 1 -name "catboost_*.cbm" \
    -printf "%T@ %f\n" 2>/dev/null \
  | sort -n | awk '{print $2}' \
  | sed 's/^catboost_\(.*\)\.cbm$/\1/' \
  | tail -n "$BACKUP_ML_MODELS_KEEP"
)

if (( ${#SOURCE_VERSIONS[@]} == 0 )); then
  log "No CatBoost model files found — skipping"
else
  COPIED=0
  for version in "${SOURCE_VERSIONS[@]}"; do
    for src in \
      "$ML_MODELS_DIR/catboost_${version}.cbm" \
      "$ML_MODELS_DIR/metadata_${version}.pkl"
    do
      [[ -f "$src" ]] || continue
      dest="$NAS_ML_DIR/$(basename "$src")"
      if [[ ! -f "$dest" ]]; then
        cp "$src" "$dest"
        log "  Copied: $(basename "$src")"
        (( COPIED++ )) || true
      fi
    done
  done
  [[ $COPIED -eq 0 ]] && log "All CatBoost model files already on NAS"

  [[ -f "$ML_MODELS_DIR/active_version.txt" ]] && \
    cp "$ML_MODELS_DIR/active_version.txt" "$NAS_ML_DIR/active_version.txt"

  mapfile -t NAS_VERSIONS < <(
    find "$NAS_ML_DIR" -maxdepth 1 -name "catboost_*.cbm" \
      -printf "%T@ %f\n" 2>/dev/null \
    | sort -n | awk '{print $2}' \
    | sed 's/^catboost_\(.*\)\.cbm$/\1/'
  )
  NAS_COUNT=${#NAS_VERSIONS[@]}
  if (( NAS_COUNT > BACKUP_ML_MODELS_KEEP )); then
    DELETE_COUNT=$(( NAS_COUNT - BACKUP_ML_MODELS_KEEP ))
    for version in "${NAS_VERSIONS[@]:0:$DELETE_COUNT}"; do
      rm -f "$NAS_ML_DIR/catboost_${version}.cbm" "$NAS_ML_DIR/metadata_${version}.pkl"
      log "  Pruned old CatBoost version: $version"
    done
  fi
fi

# ── NF Model Backup (TFT) ──────────────────────────────────────────────────────
log "Backing up NF/TFT models from $NF_MODELS_DIR"

if [[ -d "$NF_MODELS_DIR" ]] && [[ -n "$(ls -A "$NF_MODELS_DIR" 2>/dev/null)" ]]; then
  NF_BACKUP="$NAS_NF_DIR/nf_models_${DATETIME}.tar.gz"
  tar -czf "$NF_BACKUP" -C "$NF_MODELS_DIR" . 2>/dev/null || true
  NF_SIZE=$(du -sh "$NF_BACKUP" | cut -f1)
  log "NF models backed up: $(basename "$NF_BACKUP") ($NF_SIZE)"

  # Keep last 3 NF backups (models are large)
  mapfile -t NF_BACKUPS < <(find "$NAS_NF_DIR" -maxdepth 1 -name "nf_models_*.tar.gz" | sort)
  NF_COUNT=${#NF_BACKUPS[@]}
  if (( NF_COUNT > 3 )); then
    for old in "${NF_BACKUPS[@]:0:$(( NF_COUNT - 3 ))}"; do
      rm -f "$old"
      log "  Pruned old NF backup: $(basename "$old")"
    done
  fi
else
  log "No NF model files found — skipping"
fi

# ── Summary ────────────────────────────────────────────────────────────────────
DB_DAYS=$(find "$NAS_ROOT/db" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l)
ML_VERSIONS=$(find "$NAS_ML_DIR" -maxdepth 1 -name "catboost_*.cbm" 2>/dev/null | wc -l)
NF_BACKUPS_COUNT=$(find "$NAS_NF_DIR" -maxdepth 1 -name "nf_models_*.tar.gz" 2>/dev/null | wc -l)
log "Done. DB days: $DB_DAYS/${BACKUP_RETENTION_DAYS}, CatBoost versions: $ML_VERSIONS/${BACKUP_ML_MODELS_KEEP}, NF backups: $NF_BACKUPS_COUNT/3"
