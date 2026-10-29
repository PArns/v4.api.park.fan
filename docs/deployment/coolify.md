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
| `postgres`    | `timescale/timescaledb:latest-pg18` | `shm_size: 512m` (parallel-query shared memory), `max_parallel_workers_per_gather=2`, `log_lock_waits=on`                                                  | `${DB_PORT:-5433}:5432` |
| `redis`       | `redis:8.4-alpine`                  | `--maxmemory 2gb --maxmemory-policy volatile-lru`, AOF on, RDB snapshots off                                                                              | internal                |
| `ml-service`  | `./ml-service` (CatBoost)           | `mem_limit: 20g`, `ML_WORKERS` default 2 (each extra worker eats the 06:00 training margin)                                                               | 8000 internal           |
| `nf-service`  | `./nf-service` (TFT, Chronos)       | `mem_limit: 14g`, `shm_size: 2gb`; never trigger its `/train` during ml-service's 06:00 training                                                          | 8000 internal           |
| `pcn-service` | [`./pcn-service`](../../pcn-service/README.md) (intraday nowcast) | `mem_limit: 8g`, `shm_size: 2gb`; trains 08:30 UTC so GPU peaks do not overlap                                                                             | 8000 internal           |
| `api`         | `./Dockerfile` (NestJS, Node 26)    | `stop_grace_period: 30s`; heap 6 GB (the Dockerfile's `--max-old-space-size=6144` overrides the compose `NODE_OPTIONS` 4096); healthcheck `GET /v1/health/ready`                                                    | `${PORT:-3000}:3000`    |
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
`max-file: 5`, so a container keeps at most 50 MB of stdout, and a container a deploy replaces starts
an empty log. Without the block the host's Docker default applies, which may not rotate at all. A
new service copies the block. Rotation is by size, not by age, so these logs have no fixed
retention in days; the API's own file logs under `/data/parkfan/logs` do (seven days,
`src/common/utils/file-logger.util.ts`).

What the API writes there carries no address ([no address in any log line](../rules/no-address-in-logs.md)).
Requests reach the API through a Cloudflare Tunnel ([edge and throttling](edge-and-throttling.md)).
Outside this repo and still to check for the privacy policy (PAR-1163): whether anything on that path
writes an access log with the visitor's address (the tunnel's `cloudflared` and, if the tunnel points
at it, the Coolify proxy), and whether Cloudflare Logpush is on. On the host:

```bash
docker inspect -f '{{.HostConfig.LogConfig}}' <api-container>
docker ps --format '{{.Names}}\t{{.Command}}' | grep -iE 'cloudflared|proxy|traefik'
```

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
