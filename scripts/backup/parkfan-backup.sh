#!/usr/bin/env bash
# parkfan-backup.sh — Daily backup: PostgreSQL dump + ML models → Samba NAS
#
# NAS layout:
#   Backups/
#   └── parkfan/
#       ├── db/
#       │   └── 2026-04-06/
#       │       └── parkfan_20260406_030001.sql.gz
#       └── ml-models/
#           ├── catboost_v20260329_085046.cbm
#           ├── metadata_v20260329_085046.pkl
#           └── active_version.txt
#
# Deploy to dockerhost:
#   scp scripts/backup/parkfan-backup.sh <user>@<dockerhost>:/opt/parkfan/backup.sh
#   chmod +x /opt/parkfan/backup.sh
#   cp scripts/backup/backup.env.example /opt/parkfan/backup.env   # fill in secrets
#
# Cron (root): 0 3 * * * /opt/parkfan/backup.sh >> /var/log/parkfan-backup.log 2>&1

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
DB_PORT="${DB_PORT:-5432}"
DB_DATABASE="${DB_DATABASE:-parkfan}"
ML_MODELS_DIR="${ML_MODELS_DIR:-/data/parkfan/ml-models}"
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

# Paths on NAS — organized by project / type / date
NAS_ROOT="$MOUNT_POINT/parkfan"
NAS_DB_DATE_DIR="$NAS_ROOT/db/$DATE"
NAS_ML_DIR="$NAS_ROOT/ml-models"
mkdir -p "$NAS_DB_DATE_DIR" "$NAS_ML_DIR"

# ── PostgreSQL Backup ──────────────────────────────────────────────────────────
DUMP_FILE="$NAS_DB_DATE_DIR/parkfan_${DATETIME}.sql.gz"

log "Starting pg_dump (host=$DB_HOST port=$DB_PORT db=$DB_DATABASE)..."
PGPASSWORD="$DB_PASSWORD" pg_dump \
  -h "$DB_HOST" -p "$DB_PORT" \
  -U "$DB_USERNAME" -d "$DB_DATABASE" \
  --no-password --format=plain \
  | gzip > "$DUMP_FILE" \
  || die "pg_dump failed"

DUMP_SIZE=$(du -sh "$DUMP_FILE" | cut -f1)
log "DB backup done: parkfan/db/$DATE/$(basename "$DUMP_FILE") ($DUMP_SIZE)"

# Rolling retention: remove date folders older than BACKUP_RETENTION_DAYS days
DELETED_DIRS=0
while IFS= read -r -d '' dir; do
  DIR_DATE=$(basename "$dir")
  # Delete if the directory date is older than retention window
  if [[ $(date -d "$DIR_DATE" +%s 2>/dev/null || echo 0) -lt $(date -d "$BACKUP_RETENTION_DAYS days ago" +%s) ]]; then
    rm -rf "$dir"
    log "Removed old backup dir: parkfan/db/$DIR_DATE"
    (( DELETED_DIRS++ )) || true
  fi
done < <(find "$NAS_ROOT/db" -mindepth 1 -maxdepth 1 -type d -print0 2>/dev/null)

(( DELETED_DIRS == 0 )) && log "No old DB backup dirs to remove (retention: ${BACKUP_RETENTION_DAYS}d)"

# ── ML Model Backup ────────────────────────────────────────────────────────────
log "Backing up last $BACKUP_ML_MODELS_KEEP ML model versions from $ML_MODELS_DIR"

# Find the newest N versions sorted by mtime of the .cbm file
mapfile -t SOURCE_VERSIONS < <(
  find "$ML_MODELS_DIR" -maxdepth 1 -name "catboost_*.cbm" \
    -printf "%T@ %f\n" 2>/dev/null \
  | sort -n | awk '{print $2}' \
  | sed 's/^catboost_\(.*\)\.cbm$/\1/' \
  | tail -n "$BACKUP_ML_MODELS_KEEP"
)

if (( ${#SOURCE_VERSIONS[@]} == 0 )); then
  log "No ML model files found in $ML_MODELS_DIR — skipping model backup"
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
  [[ $COPIED -eq 0 ]] && log "All model files already on NAS — nothing new to copy"

  # Keep active_version.txt in sync
  if [[ -f "$ML_MODELS_DIR/active_version.txt" ]]; then
    cp "$ML_MODELS_DIR/active_version.txt" "$NAS_ML_DIR/active_version.txt"
  fi

  # Prune NAS: keep only last BACKUP_ML_MODELS_KEEP versions (oldest removed first)
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
      log "  Pruned old model version: $version"
    done
  fi
fi

DB_DAYS=$(find "$NAS_ROOT/db" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l)
ML_VERSIONS=$(find "$NAS_ML_DIR" -maxdepth 1 -name "catboost_*.cbm" 2>/dev/null | wc -l)
log "Done. DB backup days on NAS: $DB_DAYS/${BACKUP_RETENTION_DAYS}, ML versions: $ML_VERSIONS/${BACKUP_ML_MODELS_KEEP}"
