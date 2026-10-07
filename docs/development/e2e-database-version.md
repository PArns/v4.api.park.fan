# Which database version the E2E suite runs

**2026-10-06.** `test/global-setup.ts` starts the E2E database from a **pinned**
tag, `timescale/timescaledb:2.24.0-pg18` — the version production is running.
It used to start `latest-pg16`.

## What was measured

| Environment | PostgreSQL | TimescaleDB |
| -- | -- | -- |
| Production (read-only, `scripts/prod-psql.sh`) | **18.6** | **2.24.0** |
| E2E container, old `latest-pg16` | 16.15 | **2.30.2** |
| E2E container, pinned `2.24.0-pg18` | **18.1** | **2.24.0** |

Both axes were off, and in opposite directions: PostgreSQL was a major version
behind in the test, TimescaleDB six minor versions ahead. After the pin
TimescaleDB matches exactly and PostgreSQL matches at the major version, with
18.1 against production's 18.6.

## Why the PostgreSQL patch level still differs — and why that is the better trade

The image couples the two versions: `2.24.0-pg18` was built on PostgreSQL 18.1,
and that is the only PostgreSQL 18 that tag will ever carry. Production reached
18.6 because its `latest-pg18` has pulled a newer base since.

So the two axes cannot both be matched from one published tag. Picking an image
whose base is 18.6 means taking the TimescaleDB version it was built with —
2.30.x, the six-minor jump this ticket exists to remove. The pin takes the
TimescaleDB match and accepts a five-patch PostgreSQL gap, because that is where
the measured behaviour differences were (DML on compressed chunks is a
TimescaleDB feature, and PostgreSQL patch releases do not change DML semantics).

This is worth knowing before anyone "fixes" the remaining gap: moving to 18.6
would re-open the larger one.

## Why a pin and not `latest-pg18`

The production container's image tag **is** `timescale/timescaledb:latest-pg18`,
and it runs 2.24.0. The same tag on Docker Hub serves 2.30.2 today. A running
instance is the state of its last pull, so `latest-*` does not name a version —
it names whatever the next `docker pull` happens to fetch. Matching the tag
would not have matched the version, and the test's server version would keep
moving without a commit.

A pin is the only form of this that holds: the suite now runs one known version,
and changing it is a diff.

## Why this mattered concretely

From PAR-704, both directions in one ticket:

- `UPDATE … SET id = …` on a primary-key column of a **compressed chunk** is
  refused from TimescaleDB 2.30 on (`cannot update column "id" of a compressed
  chunk`). On 2.24.0 — the running server — the statement goes through. The old
  E2E container would have failed the write that production accepts.
- The mirror image: behaviour that only holds up to 2.24 is invisible in a 2.30
  test, and a bug that only PostgreSQL 18 shows never appears on 16.

A suite whose job is to answer what a merge does to a compressed chunk has to
ask the server that will answer it in production.

## The two compose files are deliberately not pinned

`docker-compose.yml:3` and `docker-compose.production.yml:3` still read
`latest-pg18`. Pinning them changes what the next deploy runs, which is a
deploy decision and not a test decision (PAR-709, non-goal). The consequence is
worth stating plainly: **production's version can still move without a commit,
and when it does, this pin is what goes stale.**

## When production's version changes

The pin is then wrong in the other direction, and the symptom is silence rather
than a red test. Re-read both numbers and move the pin:

```bash
scripts/prod-psql.sh -c "SELECT current_setting('server_version') AS pg,
  (SELECT extversion FROM pg_extension WHERE extname='timescaledb') AS timescaledb;"
```

Then set the matching `timescale/timescaledb:<timescaledb>-pg<major>` tag in
`test/global-setup.ts` — the TimescaleDB version is the one to match, per the
trade above. Check the tag exists before committing — the registry carries
`<version>-pg15` through `-pg18`, each also as `-oss`, but not every patch
version of every line:

```bash
curl -s -o /dev/null -w '%{http_code}\n' \
  https://hub.docker.com/v2/repositories/timescale/timescaledb/tags/2.24.0-pg18/
```
