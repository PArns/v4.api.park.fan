# Backups

The nightly host backup to the NAS: what it copies, how it is installed on the docker host, and how to
restore. Open it before changing `scripts/backup/parkfan-backup.sh`, moving a data directory, or restoring.

## What runs

`scripts/backup/parkfan-backup.sh`, installed as `/opt/parkfan/backup.sh` and run by host cron at 03:00
(server time) as root. It mounts the NAS share over CIFS (`//$BACKUP_NAS_HOST/$BACKUP_NAS_SHARE` at
`/mnt/parkfan-nas-backup`), writes, prunes and unmounts. It runs on the host, not in a container, because
`mount -t cifs` needs `CAP_SYS_ADMIN` (only `--privileged` inside Docker) and the script reads host paths;
it also runs while app containers are down.

| Data                                  | Backed up                                                                                   | Retention                  |
| ------------------------------------- | ------------------------------------------------------------------------------------------- | -------------------------- |
| PostgreSQL (`/data/parkfan/postgres`) | `pg_basebackup -Ft -z -Xs` of the whole cluster, run inside the postgres container, streamed as `parkfan/db/<date>/parkfan_<datetime>.tar.gz` | `BACKUP_RETENTION_DAYS` (7) days |
| CatBoost models (`/data/parkfan/ml-models`) | the newest `catboost_<version>.cbm` + `metadata_<version>.pkl` pairs, chosen by version name, plus `active_version.txt` | `BACKUP_ML_MODELS_KEEP` (7) versions |
| nf-service models (`/data/parkfan/nf-models`) | the whole directory as `parkfan/nf-models/nf_models_<datetime>.tar.gz`                 | 3 archives                 |
| pcn-service models (`/data/parkfan/pcn-models`) | **not backed up**                                                                   | —                          |
| Redis, GeoIP, logs                    | not backed up: cache and state rebuilt by the app, GeoLite2 is downloaded again, logs are ephemeral | —              |

CatBoost versions are pruned by name (`vYYYYMMDD_HHMM` sorts chronologically), not by mtime: on the NAS
mtime is the copy time, and pruning by it deleted versions a later run copied back (2026-10-22).

Because the PostgreSQL backup is a binary base backup, the host needs no PostgreSQL client, and restoring
needs no TimescaleDB-specific steps.

## Install

```bash
scp scripts/backup/parkfan-backup.sh <user>@<host>:/opt/parkfan/backup.sh
ssh <user>@<host> 'chmod +x /opt/parkfan/backup.sh && sudo apt-get install -y cifs-utils'
# /opt/parkfan/backup.env from scripts/backup/backup.env.example, chmod 600 (NAS and DB credentials)
echo "0 3 * * * root /opt/parkfan/backup.sh >> /var/log/parkfan-backup.log 2>&1" \
  | sudo tee /etc/cron.d/parkfan-backup
```

`backup.env` sets the NAS (`BACKUP_NAS_HOST`, `_SHARE`, `_USER`, `_PASSWORD`) and the database role
(`DB_USERNAME`, `DB_PASSWORD`; `DB_HOST`, `DB_PORT` (default 5433), `DB_DATABASE` are optional). Paths and
retention have defaults (`POSTGRES_DATA_DIR`, `ML_MODELS_DIR`, `NF_MODELS_DIR`, `BACKUP_RETENTION_DAYS`,
`BACKUP_ML_MODELS_KEEP`). Another job that needs the NAS (the TFT forecast export in
[nightly DB maintenance](../troubleshooting/nightly-db-maintenance.md#tft-retention)) mounts it the same way
with these credentials.

Test by hand with `sudo /opt/parkfan/backup.sh` and `tail -f /var/log/parkfan-backup.log`. A
`perl: warning: Setting locale failed` line is harmless.

## Restore

**Database.** The archive is a tar of `pg_basebackup`'s tar-format output: it contains `base.tar.gz` and
`pg_wal.tar.gz`.

1. Stop the postgres container.
2. Move the old cluster aside and create an empty data directory. The container mounts
   `/data/parkfan/postgres` at `/var/lib/postgresql`; check where `$PGDATA` lies inside it
   (`docker exec <postgres> printenv PGDATA`).
3. Extract the archive, then `base.tar.gz` into the data directory and `pg_wal.tar.gz` into its `pg_wal/`.
4. Start the container; it finds the cluster and skips initialisation.

The script's own header gives a shorter four-step restore that extracts the outer archive straight into
`/data/parkfan/postgres`; that leaves the two inner archives unpacked. Neither procedure has been
rehearsed.

**CatBoost model.** Copy `catboost_<version>.cbm` and `metadata_<version>.pkl` back to
`/data/parkfan/ml-models/` and write the version into `active_version.txt`; ml-service workers load it on
their next request.
