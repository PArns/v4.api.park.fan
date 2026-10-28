# Container memory limits

The production containers' memory limits against the 29 GB host, and how to read a container's peak.
Open it before changing a `mem_limit` or moving a nightly job's time.

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

## Reading a container's peak

On the host, the morning after a night with no deploy and one runner run:

```bash
for c in $(docker ps --format '{{.Names}}' | grep -E 'ml-service|nf-service|pcn-service|postgres|api|redis'); do
  id=$(docker inspect -f '{{.Id}}' "$c")
  echo "$c $(cat /sys/fs/cgroup/system.slice/docker-$id.scope/memory.peak 2>/dev/null)"
done
```

`memory.peak` is the high-water mark since the container started.

Open work: new limits from one night of `memory.peak` readings (measured peak + ~25 %, Postgres
reserved rather than capped, the API's two heap settings reconciled) are PAR-1148, which holds the
recommended values and the options. Even with them, the time separation stays the real protection;
limits only decide which container dies.
