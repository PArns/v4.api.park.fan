# Backup Strategy — Park Fan API

## Overview

Daily automated backups to a UniFi NAS via Samba (CIFS).

- **Database**: Daily `pg_dump` snapshots, 7-day rolling retention
- **ML Models**: Last 7 versions (`catboost_*.cbm` + `metadata_*.pkl`)
- **Schedule**: Daily at 03:00 (server local time)
- **Target**: `//192.168.100.100/Backups`

---

## NAS Folder Layout

```
Backups/
└── parkfan/
    ├── db/
    │   ├── 2026-04-06/
    │   │   └── parkfan_20260406_030001.sql.gz
    │   └── 2026-04-07/
    │       └── parkfan_20260407_030012.sql.gz
    └── ml-models/
        ├── catboost_v20260329_085046.cbm
        ├── metadata_v20260329_085046.pkl
        └── active_version.txt
```

---

## Files on the Docker Host

| Path | Description |
|---|---|
| `/opt/parkfan/backup.sh` | The backup script |
| `/opt/parkfan/backup.env` | Credentials & config (`chmod 600`, never commit) |
| `/etc/cron.d/parkfan-backup` | Cron entry (runs as root) |
| `/var/log/parkfan-backup.log` | Log output |

---

## Why Host Cron Instead of Docker?

`mount -t cifs` requires `CAP_SYS_ADMIN` (kernel support). Inside Docker this is only possible with `--privileged`, which is a security risk. Since the script also needs direct access to the host path `/data/parkfan/ml-models`, a host cron is the cleaner approach. The backup also runs when app containers are temporarily down.

---

## What Is Backed Up

| Data | Location | Backed up? | Notes |
|---|---|---|---|
| PostgreSQL DB | `/data/parkfan/postgres` | ✅ `pg_dump` | Full plain-text dump, gzipped |
| ML Models | `/data/parkfan/ml-models` | ✅ Last 7 versions | `.cbm` + `.pkl` pairs |
| Redis | in-memory / AOF | ❌ | Cache only — no persistent state worth backing up |
| GeoIP DB | `/data/parkfan/geoip` | ❌ | Downloaded fresh on deploy via MaxMind |
| Logs | `/data/parkfan/logs` | ❌ | Ephemeral, not worth backing up |
| App config | `.env` / Git repo | ➡️ Git | Code and config live in the repo |

---

## Initial Setup

### Prerequisites

```bash
# cifs-utils for Samba mount
sudo apt-get install -y cifs-utils

# PostgreSQL 18 client (must match server version)
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
  | sudo gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg
echo "deb http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
  | sudo tee /etc/apt/sources.list.d/pgdg.list
sudo apt-get update && sudo apt-get install -y postgresql-client-18
```

### Deploy the Script

```bash
scp scripts/backup/parkfan-backup.sh <user>@<dockerhost>:/opt/parkfan/backup.sh
ssh <user>@<dockerhost> "chmod +x /opt/parkfan/backup.sh"
```

### Create the Env File

```bash
# Use scripts/backup/backup.env.example as template
vim /opt/parkfan/backup.env
chmod 600 /opt/parkfan/backup.env
```

Required variables (values from `.env.live_debug`):

```bash
BACKUP_NAS_HOST=192.168.100.100
BACKUP_NAS_SHARE=Backups
BACKUP_NAS_USER=...
BACKUP_NAS_PASSWORD=...
DB_HOST=localhost
DB_PORT=5432
DB_USERNAME=parkfan
DB_PASSWORD=...
DB_DATABASE=parkfan
BACKUP_RETENTION_DAYS=7
BACKUP_ML_MODELS_KEEP=7
ML_MODELS_DIR=/data/parkfan/ml-models
```

### Set Up Cron

```bash
echo "0 3 * * * root /opt/parkfan/backup.sh >> /var/log/parkfan-backup.log 2>&1" \
  | sudo tee /etc/cron.d/parkfan-backup
sudo chmod 644 /etc/cron.d/parkfan-backup
```

---

## Manual Test

```bash
sudo /opt/parkfan/backup.sh
tail -f /var/log/parkfan-backup.log
```

Expected warnings (harmless):
- `perl: warning: Setting locale failed` — locale not configured on host, does not affect the backup
- `pg_dump: warning: circular foreign-key constraints on hypertable/chunk` — TimescaleDB internal tables, restore still works

---

## Restore

### Database

```bash
# Mount NAS
sudo mount -t cifs //192.168.100.100/Backups /mnt/nas \
  -o username=backup-user,password=...,vers=3.0

# Restore dump
gunzip -c /mnt/nas/parkfan/db/2026-04-06/parkfan_20260406_030001.sql.gz \
  | PGPASSWORD=... psql -h localhost -p 5432 -U parkfan -d parkfan
```

### ML Model

```bash
# Copy model files back to host volume
cp /mnt/nas/parkfan/ml-models/catboost_v20260329_085046.cbm \
   /data/parkfan/ml-models/
cp /mnt/nas/parkfan/ml-models/metadata_v20260329_085046.pkl \
   /data/parkfan/ml-models/

# Update sentinel → all uvicorn workers reload on next request
echo "v20260329_085046" > /data/parkfan/ml-models/active_version.txt
```
