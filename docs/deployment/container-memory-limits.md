# Container memory limits (open owner decision, PAR-1054)

Whether the production containers' memory limits fit the 29 GB host, what to measure first and the
recommended values. **Recommendation only: `docker-compose.production.yml` is unchanged.** Open it before
changing a `mem_limit` or moving a nightly job's time.

## What the host has and what is promised

The production host (celestrial) has 29 GB RAM and 32 GB swap.

| Container / process                       | Limit today      | Known peak                                                                                       |
| ----------------------------------------- | ---------------- | ------------------------------------------------------------------------------------------------ |
| `ml-service`                              | 20 g             | ~13 GB training subprocess + 2–4 GB serving; 6.4 GB RSS when OOM-killed at 06:15 on 2026-10-12  |
| `nf-service`                              | 14 g             | ">10 g at dataset build" for the full panel; the chunked fit (10 parks) is ~1–2 GB              |
| `pcn-service`                             | 8 g              | not measured                                                                                     |
| `postgres`                                | none             | 0.51 GB private RSS at the 2026-10-12 OOM, plus shared buffers                                   |
| `api`                                     | none             | V8 heap 6 GB: the Dockerfile's `--max-old-space-size=6144` overrides the compose `NODE_OPTIONS` 4096 |
| `redis`                                   | none             | `maxmemory 2gb` + overhead                                                                       |
| Linear runner (host systemd user unit)    | `MemoryMax=18G`  | 15.3 GB (28 jest workers)                                                                        |

The three hard limits alone add up to 42 GB, and with everything else ~72 GB. That is safe only because
the peaks are separated in time: nf-service 03:00, CatBoost 06:00, PCN 08:30, the runner at
09:45/13:45/17:45/21:45 UTC. Any two big peaks together (a manual `/train`, a retried job, a moved
runner) exceed the host, and the kernel kills the largest process, which on 2026-10-12 was the training
([nightly DB maintenance](../troubleshooting/nightly-db-maintenance.md#the-linear-runner-shares-the-hosts-memory)).
Today's limits bound nothing that matters: each is above what fits next to the rest.

## Measure first

On the host, the morning after a night with no deploy and one runner run:

```bash
for c in $(docker ps --format '{{.Names}}' | grep -E 'ml-service|nf-service|pcn-service|postgres|api|redis'); do
  id=$(docker inspect -f '{{.Id}}' "$c")
  echo "$c $(cat /sys/fs/cgroup/system.slice/docker-$id.scope/memory.peak 2>/dev/null)"
done
```

`memory.peak` is the high-water mark since the container started.

## Recommended limits

Each is the measured peak plus ~25 %, so a runaway container dies inside its own cgroup instead of
taking the host.

| Container     | Recommended                                                              | Why                                                                                                                                                                            |
| ------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `ml-service`  | `mem_limit: 18g`                                                         | 13 GB training + 4 GB serving; 20 g leaves nothing for anything else at 06:00                                                                                                  |
| `nf-service`  | `8g` once `memory.peak` shows the chunked fit under ~6 GB, else keep 14 g | 14 g was sized for full-panel training, which no longer runs                                                                                                                  |
| `pcn-service` | `6g` if `memory.peak` < 4.5 GB, else keep 8 g                            | not measured yet                                                                                                                                                               |
| `postgres`    | **no hard limit**; `mem_reservation: 6g` (cgroup v2 `memory.low`)       | a cgroup OOM kills a backend and the postmaster restarts every connection; protect it from reclaim and bound it by its own settings (`shared_buffers`, `work_mem`, `autovacuum_work_mem` 256 MB × 4 workers) |
| `api`         | `mem_limit: 6g`                                                          | sized for a 4 GB heap; the effective heap limit is 6 GB, so read `memory.peak` first. A leak should restart the API, not trigger a host OOM |
| `redis`       | `mem_limit: 3g`                                                          | `maxmemory 2gb` + fragmentation                                                                                                                                                |

With these the ceilings at 06:00 add up to ~47 GB, and realistic concurrent use is ~29 GB, still at the
host's edge. **The time separation stays the real protection; the limits only decide who dies.**

## Options

1. Keep today's limits: a coincident peak OOMs the largest process, as on 2026-10-12.
2. Apply the table above after one night of `memory.peak` readings (recommended).
3. Option 2, plus moving nf-service's training off the CPU-RAM path (GPU-only data loading) before
   lowering it further.
