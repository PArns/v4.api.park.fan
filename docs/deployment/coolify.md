# Coolify Deployment

This guide details how to deploy Park Fan API v4 on Coolify.

## Quick Start

1. **Project**: Create a new Docker Compose resource in Coolify.
2. **Repo**: Select `PArns/v4.api.park.fan`.
3. **Branch**: `main`.
4. **Compose File**: `docker-compose.production.yml`.

## Environment Variables

Configure these in Coolify's environment variable section:

### Application
- `NODE_ENV=production`
- `PORT=3000`
- `API_PREFIX=v1`

### Database
- `DB_HOST=postgres`
- `DB_PORT=5432`
- `DB_USERNAME=parkfan`
- `DB_PASSWORD=<SECURE_PASSWORD>`
- `DB_DATABASE=parkfan`
- `DB_SYNCHRONIZE=false` (Use migrations in prod if available, else be careful)

### Redis
- `REDIS_HOST=redis`
- `REDIS_PORT=6379`

### ML Service
- `ML_SERVICE_URL=http://ml-service:8000`
- `MODEL_DIR=/app/models`

## Volumes

Coolify manages these persistent volumes automatically:
- `pgdata`
- `redisdata`
- `ml-models`

## Verification

After deployment, check health endpoints:
- API: `https://api.yourdomain.com/health`
- Queue Board: `https://queue.yourdomain.com`

## Container memory limits — analysis and owner decision (PAR-1054)

**Status: recommendation only. `docker-compose.production.yml` is unchanged.**
A limit set too low turns a nightly training into an OOM-kill and a limit on
Postgres turns one backend's kill into a crash-restart of the whole database, so
the values below need the measurement step first and the owner's call.

**What the host has and what is promised.** celestrial has 29 GB RAM (+32 GB swap).

| container / process | limit today | known peak | source |
|---|---|---|---|
| `ml-service` | 20 g | ~13 GB training subprocess + 2–4 GB serving; 6.4 GB RSS when killed at 06:15 | compose comment, runbook §5a |
| `nf-service` | 14 g | ">10 g at dataset build" for the *full* panel; the chunked fit (10 parks) is documented at ~1–2 GB | compose comment, `forecast.py` |
| `pcn-service` | 8 g | not measured | — |
| `postgres` | none | 0.51 GB private RSS at the 10-12 OOM, plus shared buffers; ~5.3 GB "committed" per the nf comment | runbook §5a |
| `api` | none | V8 heap capped at 4 GB (`--max-old-space-size=4096`), so ~5 GB RSS | compose |
| `redis` | none | `maxmemory 2gb` + overhead | compose |
| Linear runner (host, systemd user unit) | `MemoryMax=18G` | 15.3 GB (28 jest workers) | runbook §5a |

The three hard limits alone add up to 42 GB on a 29 GB host; with the unlimited
ones and the runner, ~72 GB. That is safe only because the peaks are separated in
time — nf 03:00, CatBoost 06:00, PCN 08:30, the runner at 09:45/13:45/17:45/21:45.
Any two of the big peaks together (a manual `/train`, a retried job, a runner
moved back) exceed the host, and the kernel then picks the largest process, which
was the training on 2026-10-12. The limits today bound nothing that matters: they
are all above what fits next to the rest.

**Measure first** (on the host, after one full night and one runner run):

```bash
for c in $(docker ps --format '{{.Names}}' | grep -E 'ml-service|nf-service|pcn-service|postgres|api|redis'); do
  id=$(docker inspect -f '{{.Id}}' "$c")
  echo "$c $(cat /sys/fs/cgroup/system.slice/docker-$id.scope/memory.peak 2>/dev/null)"
done
```

`memory.peak` is the high-water mark since the container started; read it the
morning after a deploy-free night.

**Recommended limits** — each = measured peak + ~25 %, so that one runaway
container is killed inside its own cgroup instead of taking the host:

| container | recommended | why |
|---|---|---|
| `ml-service` | `mem_limit: 18g` | 13 GB training + 4 GB serving; 20 g leaves no room for anything else at 06:00 |
| `nf-service` | `mem_limit: 8g` after `memory.peak` confirms the chunked fit stays under ~6 GB; keep 14 g otherwise | the 14 g was sized for full-panel training, which no longer runs |
| `pcn-service` | `mem_limit: 6g` if `memory.peak` < 4.5 GB, else keep 8 g | not measured yet |
| `postgres` | **no hard limit**; `mem_reservation: 6g` (cgroup v2 `memory.low`) | a cgroup OOM kills a backend and the postmaster restarts every connection; protect it from reclaim instead, and bound it by its own settings (`shared_buffers`, `work_mem`, `autovacuum_work_mem` 256 MB × 4 workers) |
| `api` | `mem_limit: 6g` | heap 4 GB + native; a leak should restart the API, not trigger a host OOM |
| `redis` | `mem_limit: 3g` | `maxmemory 2gb` + fragmentation |

Worst case at 06:00 with these: 18 + 8 + 6 + ~6 (postgres) + 6 + 3 ≈ 47 GB of
ceilings, but the realistic concurrent use is ~13 + 2 + 1 + 6 + 5 + 2 ≈ 29 GB —
still at the host's edge, so **the time separation stays the real protection**
and the limits decide only *who* dies when it fails.

**Options for the owner.**

1. Keep today's limits (nothing changes; a coincident peak OOMs the largest
   process, as on 10-12).
2. Apply the table above after one night of `memory.peak` readings
   (recommended).
3. Option 2 plus moving nf-service's training off the CPU-RAM path entirely
   (GPU-only data loading) before lowering it further.
