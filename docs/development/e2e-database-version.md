# Which database version the E2E suite runs

`test/global-setup.ts` starts the E2E database from a **pinned** image, `timescale/timescaledb:2.24.0-pg18`,
matching production's TimescaleDB. Why it is pinned, why the PostgreSQL patch level still differs, and how
to move the pin when production changes.

## Measured (2026-10-06, PAR-704/709)

| Environment                                   | PostgreSQL | TimescaleDB |
| --------------------------------------------- | ---------- | ----------- |
| Production (`scripts/prod-psql.sh`)           | 18.6       | 2.24.0      |
| E2E before, `latest-pg16`                     | 16.15      | 2.30.2      |
| E2E now, `2.24.0-pg18`                        | 18.1       | 2.24.0      |

It mattered concretely: from TimescaleDB 2.30, `UPDATE … SET id = …` on a primary-key column of a
compressed chunk is refused, while production's 2.24.0 accepts it. The suite that measures what a merge
does to compressed chunks was asking a different server.

## Why TimescaleDB is matched and PostgreSQL is not

The image couples the two: `2.24.0-pg18` is built on PostgreSQL 18.1 and always will be. Production reached
18.6 because its `latest-pg18` pulled a newer base. An image on 18.6 comes with TimescaleDB 2.30.x, the
larger gap. DML on compressed chunks is TimescaleDB behaviour, and PostgreSQL patch releases do not change
DML semantics, so the pin takes the TimescaleDB match and accepts a five-patch PostgreSQL gap. Do not
"fix" the remaining gap.

## Why a pin and not `latest-pg18`

Production's container tag is `latest-pg18` and runs 2.24.0, while the same tag on Docker Hub serves
2.30.2: a running instance is the state of its last pull. Only a pin names a version, and moving it is a
diff.

`docker-compose.yml` and `docker-compose.production.yml` stay on `latest-pg18` on purpose: pinning them
changes what the next deploy runs, which is a deploy decision (PAR-709). So **production's version can
move without a commit, and then this pin is what goes stale** — silently.

## Moving the pin

```bash
scripts/prod-psql.sh -c "SELECT current_setting('server_version') AS pg,
  (SELECT extversion FROM pg_extension WHERE extname='timescaledb') AS timescaledb;"
```

Set `timescale/timescaledb:<timescaledb>-pg<major>` in `test/global-setup.ts`, matching TimescaleDB. Not
every patch exists for every line (`-pg15` to `-pg18`, each also `-oss`), so check the tag first:

```bash
curl -s -o /dev/null -w '%{http_code}\n' \
  https://hub.docker.com/v2/repositories/timescale/timescaledb/tags/2.24.0-pg18/
```
