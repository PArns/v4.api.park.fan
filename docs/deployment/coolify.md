# Production deployment (Coolify)

How the API and its services run in production: the Coolify resource, the containers in
`docker-compose.production.yml`, where their data lives, the environment and what to check after a
deploy. Open it before changing the compose file, an environment variable or the host.

## The Coolify resource

- A **Docker Compose** resource on repository `PArns/v4.api.park.fan`, branch `main`, compose file
  `docker-compose.production.yml`.
- **Coolify deploys on every push to `main`.** The build takes a few minutes; check a change only after
  the new container is up.
- The schema follows the entities at boot (`DB_SYNCHRONIZE=true`, no migrations), so an entity change
  reaches production with the deploy: [the entities are the schema](../rules/the-entities-are-the-schema.md).
- Container names carry a Coolify deploy id and change with every deploy. Ad-hoc SQL goes through
  `scripts/prod-psql.sh`, which finds the container itself
  ([DB health runbook §0](../troubleshooting/db-health-runbook.md#0-time-limits-on-ad-hoc-sql)).

## Containers

| Service       | Image / build                       | Limits and settings                                                                                                                                       | Port                    |
| ------------- | ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------- |
| `postgres`    | `timescale/timescaledb:latest-pg18` | `shm_size: 512m` (parallel-query shared memory), `max_parallel_workers_per_gather=2`, `log_lock_waits=on`, server log to files in `$PGDATA/log` ([below](#the-postgres-server-log)) | `${DB_PORT:-5433}:5432` |
| `redis`       | `redis:8.4-alpine`                  | `--maxmemory 2gb --maxmemory-policy volatile-lru`, AOF on, RDB snapshots off                                                                              | internal                |
| `ml-service`  | `./ml-service` (CatBoost)           | `mem_limit: 20g`, `ML_WORKERS` default 2 (each extra worker eats the 06:00 training margin)                                                               | 8000 internal           |
| `nf-service`  | `./nf-service` (TFT, Chronos)       | `mem_limit: 14g`, `shm_size: 2gb`; never trigger its `/train` during ml-service's 06:00 training                                                          | 8000 internal           |
| `pcn-service` | [`./pcn-service`](../../pcn-service/README.md) (intraday nowcast) | `mem_limit: 8g`, `shm_size: 2gb`; trains 08:30 UTC so GPU peaks do not overlap                                                                             | 8000 internal           |
| `api`         | `./Dockerfile` (NestJS, Node 26)    | `stop_grace_period: 30s`; heap 6 GB, set only by `NODE_OPTIONS=--max-old-space-size=6144` (a flag in the Dockerfile's `CMD` would override it); healthcheck `GET /v1/health/ready`                                                    | `${PORT:-3000}:3000`    |
| `bull-board`  | `venatum/bull-board:latest`         | Reads Bull keys under `BULL_PREFIX` (default `parkfan`)                                                                                                    | `3001:3000`             |

Those limits only fit the host because the nightly peaks are spaced in time
([container memory limits](container-memory-limits.md)); new limits are PAR-1148.

Two settings that look removable and are not:

- **`api` does not wait for `ml-service` to be healthy.** The ML services run on the NVIDIA runtime on
  the GPU host; a driver fault ("Driver/library version mismatch" after an unattended update) stops
  their containers from starting. When `api` depended on `ml-service: service_healthy`, that took every
  endpoint down with a 502. The ML services are HTTP dependencies that degrade per request, so `api`
  waits only for `postgres` and `redis`. Do not add them back, not even as `service_started`.
- **`stop_grace_period: 30s` on `api`.** The 20 s shutdown deadline in
  `src/common/utils/graceful-shutdown.ts` needs it: Coolify passes `--timeout=30` on a deploy, but a
  `docker.service` restart uses the container's own `StopTimeout`, which defaulted to 10 s and SIGKILLed
  the API with 31 Bull job locks held (2026-10-21, PAR-1004). After a deploy,
  `docker inspect -f '{{.Config.StopTimeout}}' <api-container>` must print `30`.

## Data on the host

Every service writes to a bind mount under `/data/parkfan/` (not to Docker named volumes), so a
redeploy keeps it:

| Path                       | Service                        |
| -------------------------- | ------------------------------ |
| `/data/parkfan/postgres`   | `postgres` (`/var/lib/postgresql`) |
| `/data/parkfan/redis`      | `redis`                        |
| `/data/parkfan/ml-models`  | `ml-service` (`/app/models`)   |
| `/data/parkfan/nf-models`  | `nf-service`                   |
| `/data/parkfan/pcn-models` | `pcn-service`                  |
| `/data/parkfan/geoip`      | `api` (GeoLite2-City database) |
| `/data/parkfan/logs`       | `api` (slow-query log)         |

`api` also mounts `/sys` read-only at `/host/sys` so `system-health` can read the CPU temperature.
What is backed up: [backup](backup.md).

## Container logs

Every service in `docker-compose.production.yml` sets `logging: json-file` with `max-size: 10m` and
`max-file: 5`, so a container keeps at most 50 MB of stdout and stderr, and a container a deploy
replaces starts an empty log. Without the block the host's Docker default applies, which may not
rotate at all. A new service copies the block. Rotation is by size, not by age, so these logs have no
fixed retention in days; the API's own file logs under `/data/parkfan/logs` do (seven days,
`src/common/utils/file-logger.util.ts`). The block is part of a container's configuration: changing
it makes the next deploy recreate that container, Postgres and Redis included.

What the API writes there carries no full address, an admin request line at most a /24 or /48
([no address in any log line](../rules/no-address-in-logs.md)). Requests from the internet reach the
API through a Cloudflare Tunnel ([edge and throttling](edge-and-throttling.md)); whether the tunnel's
`cloudflared`, the Coolify proxy (if the tunnel points at it) or Cloudflare Logpush keep the visitor's
address is set outside this repo. What the containers log, and through which driver, shows on the host:

```bash
docker inspect -f '{{.HostConfig.LogConfig}}' <api-container>
docker ps --format '{{.Names}}\t{{.Command}}' | grep -iE 'cloudflared|proxy|traefik'
```

## The Postgres server log

Postgres writes its server log (slow statements over 500 ms, lock waits, errors) through
`logging_collector` into `$PGDATA/log`, which is `/data/parkfan/postgres/18/docker/log` on the host. The
directory sits on the bind mount, so a deploy that replaces the container keeps it. `docker logs` on
the postgres container shows only the lines before the collector starts.

| Setting                    | Value                  | Effect                                                    |
| -------------------------- | ---------------------- | --------------------------------------------------------- |
| `log_filename`             | `postgresql-%a-%H.log` | one file per weekday and hour, 168 names                  |
| `log_rotation_age`         | `1h`                   | a new file every hour                                     |
| `log_truncate_on_rotation` | `on`                   | the file a week old is emptied when its name comes round |
| `log_rotation_size`        | `0`                    | no size rotation: it would reopen the same hour's file    |

Postgres never deletes a log file, so the cycling names are the bound: the log reaches back seven days
and holds one week of output. At the rate measured on 2026-10-11 (30 MB in 7 h 27 min, about 4 MB an
hour) that is about 0.7 GB. A burst only fills the hour's file; it never adds files. The directory lies
inside `$PGDATA`, so `pg_basebackup` copies it into the nightly backup ([backups](backup.md)), compressed.
A restart or a new container appends to the current hour's file, so the previous container's last lines
stay in it.

```bash
pg=$(docker ps --format '{{.Names}}' | grep postgres | grep -v coolify)
docker exec "$pg" sh -c 'ls -lt $PGDATA/log | head'
docker exec "$pg" sh -c 'ls -tr $PGDATA/log/postgresql-*.log | xargs cat' | grep -A1 'duration:' | tail -100
```

The settings are `-c` flags on the postgres `command:`, so changing them recreates the Postgres
container on the next deploy.

## Environment

Coolify holds one `.env`; `.env.production.example` is the template and the list of every variable
with its comment. For `api`, the compose file overrides the addresses: `DB_HOST=postgres`,
`REDIS_HOST=redis`, `ML_SERVICE_URL`, `NF_SERVICE_URL`, `PCN_SERVICE_URL` (all `http://<service>:8000`),
`NODE_ENV=production` and `GEOIP_DATABASE_PATH=/app/geoip/GeoLite2-City.mmdb`.

Switches set in the compose file, each with its default:

| Variable                                    | Default  | Effect                                                                                     |
| ------------------------------------------- | -------- | ------------------------------------------------------------------------------------------ |
| `SERVE_PCN_INTRADAY`                        | `false`  | Serve PCN's 15-minute intraday forecast instead of CatBoost's hourly one.                  |
| `SERVE_CHRONOS_INTRADAY`                    | `false`  | Serve Chronos-2 on the 45–120 minute intraday slots.                                       |
| `CHRONOS_SHADOW_ENABLED`                    | `true`   | Capture Chronos-2 into the forward archive (never served).                                 |
| `CHRONOS_ENABLED` (nf-service)              | `true`   | Run Chronos-2 in nf-service at all.                                                        |
| `DOCKER_RUNTIME`, `NVIDIA_VISIBLE_DEVICES`  | `runc`, empty | Set to `nvidia` / `all` on a GPU host (needs `nvidia-container-toolkit`).             |
| `CATBOOST_TASK_TYPE`                        | `CPU`    | CatBoost training device.                                                                  |

`API_PREFIX` appears in `.env.production.example` but nothing reads it: the global prefix is the
constant `GLOBAL_PREFIX = "v1"` (`src/common/swagger/swagger-config.ts`).

## After a deploy

- `GET https://api.park.fan/v1/health/ready` answers 200 (this is what the compose healthcheck asks;
  Coolify's own label checks `/v1/health`).
- The ML services answer `GET /health` on port 8000 inside the network only.
- Bull Board on port 3001 shows the queues; a failed job is retried from there.
- Do not deploy on Sunday between 19:10 and 20:05 UTC: a pcn-service restart during the weekly
  `REINDEX` of `pcn_forecasts_pkey` stalls PCN writes
  ([nightly DB maintenance](../troubleshooting/nightly-db-maintenance.md#weekly-reindex-of-the-pcn-primary-key)).

### A deploy that changes a served forecast

`/plan/day` and Parky build a day's rides from the served daily rows (`getServingDailyPredictions`),
so a deploy that changes them (the [season level](../ml/season-level.md), the serving merge, a
model's serving path) reaches both with the first request after it. The calendar months
(`calendar:month:v2:…`), the best-days snapshots (`best-days:v2:…`) and park.fan's copies of both
keep the old level until the next `warmup-calendar-daily` (08:00 and 20:00 UTC) evicts and rebuilds
them and rings the frontend ([the forecast ring](../architecture/caching-strategy.md#the-forecast-ring)).
Until then a park page shows a day at one level in the calendar and at another in the trip planner
and in Parky's answers. So after such a deploy:

1. Wait for the new `api` container: `GET /v1/health/ready` answers 200.
2. **Run the calendar warmup:** `POST /v1/admin/warmup-calendar` (editor; `run_job`
   `warmup-calendar` in the admin MCP). It is done when the API logs
   `✅ Calendar warmup complete: … parks in …s (best-days: …)`, after which it rings the frontend.
3. **Then deploy the frontend.** Its deploy purges Cloudflare, which no ring reaches (park.fan's
   `docs/architecture/cdn-caching.md#a-deploy-purges-cloudflare`): after the ring, the purge clears
   the hour of old copies Cloudflare still holds; before it, Cloudflare refills from the old ones.

Start the warmup outside the scheduled runs (from 08:00 and 20:00 UTC to their completion line).
The route answers with the warmup already running or waiting instead of queueing a second, since two
park builds in parallel saturated the database's disk
([cache warmup](../architecture/caching-strategy.md#cache-warmup)), but it cannot see a run that is
about to start from the cron. Keep it out of 05:50–07:30
too, while CatBoost trains.

### A deploy that changes what a model input means

A trained model keeps the meaning it learned an input with. When a deploy changes what an existing
input means, the models are served the new values with the old meaning until their next nightly
training: CatBoost (06:00 UTC) and PCN (08:30) through their saved models, TFT (03:00) through the
forecast its last run persisted, fitted on the old inputs. The release of 2026-11-17 is such a
deploy: for parks in countries whose weekend is not Saturday and Sunday (Saudi Arabia, Qatar,
Bahrain, Kuwait, Oman, Israel) CatBoost's weekday/weekend rolling features and TFT's and PCN's
`is_weekend` now split on the country's weekend (PAR-1379), and CatBoost's `is_weekend` reads
Saturday and Sunday for the UAE (PAR-1375). After such a deploy, train all three by hand once the
new containers are up, one after the other:

1. **TFT:** `POST /v1/admin/train-nf` (editor; `run_job` `train-nf`), about 40–50 minutes
   ([TFT](../ml/tft.md)). Then run the calendar warmup
   ([above](#a-deploy-that-changes-a-served-forecast)): `train-nf` evicts `ml:tft-daily:*` when it
   completes and rings nothing, so `/plan/day` and Parky read the new forecast at once while the
   calendar and best-days keep the old one.
2. **CatBoost:** `POST /v1/admin/train-ml-model` (editor; `run_job` `train-ml-model` in the admin
   MCP), 35–70 minutes ([CatBoost training](../ml/catboost-training.md)). Then run the calendar
   warmup again: the served CatBoost rows are cached per park and day until the warmup evicts them,
   so without it the new model reaches each surface when its own cache runs out, unrung, the first
   at each park's midnight.
3. **PCN:** `POST /v1/admin/pcn/train` (editor; `run_job` `pcn-train`), [PCN](../ml/pcn.md). It
   serves the intraday forecast only, which no calendar cache holds, so no warmup follows.

Start each only when the one before has finished, a warmup included: Bull Board shows the job, and
the rows `tft`, `catboost` and `pcn` of `GET /v1/admin/ml-overview` read each service's
`/train/status` ([ML overview](../ml/ml-overview.md)). Two trainings at once, or one next to a
nightly training or the Linear runner (09:45, 13:45, 17:45 and 21:45 UTC), exceed the host's memory
([container memory limits](container-memory-limits.md)), so pick a gap that fits all three.

A training does not queue the warmup itself. After the nightly TFT run it would be a third warmup a
day inside the nightly batch window, so there the calendar keeps the previous forecast until 08:00
while `/plan/day` reads the new one. After a hand-started run it would start beside the next
training of this list and, with nothing to stop two warmups at once, possibly beside a scheduled
one.
