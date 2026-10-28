# Admin endpoints

Every route under `/v1/admin`, grouped by purpose, with the role it needs and what it starts. Open it
to find how to trigger a job, a merge or a cache reset by hand; each group links the page that explains
the mechanism. Roles, sessions, the shared pass and the login limits: [authentication](authentication.md).

**Role** is the minimum (`owner > editor > author > viewer`); `viewer` means any signed-in account,
because an endpoint without `@AdminMinRole` needs nothing more. Every admin answer is
`private, no-store`, and every write on `AdminController` and `AdminContentController` is audited
(`AdminAuditInterceptor`). A **job** row returns as soon as the job is queued; the queue and its cron are
in [job queues](../architecture/job-queues.md).

## Accounts and sessions (`admin/auth`)

The deprecated shared pass is refused here except on `me` and `logout`.

| Method | Path | Role | Does |
| --- | --- | --- | --- |
| POST | `auth/login` | public | session token; a solved Turnstile, once configured, for callers outside the frontend; `@Throttle` 10/min |
| POST | `auth/logout` | viewer | ends this session |
| GET | `auth/me` | viewer | who this session is |
| POST | `auth/change-password` | viewer | own password; `@Throttle` 5/min |
| GET | `auth/sessions` | viewer | this account's sessions |
| DELETE | `auth/sessions/:id` | viewer | ends one of them |
| POST | `auth/sessions/revoke-all` | viewer | ends all of them, this one included |
| POST | `auth/totp/begin` | viewer | starts two-factor enrolment; `@Throttle` 10/min |
| POST | `auth/totp/confirm` | viewer | enables two-factor |
| POST | `auth/totp/disable` | viewer | needs password and code; `@Throttle` 5/min |
| GET | `auth/users` | owner | all accounts |
| POST | `auth/users` | owner | creates an account |
| PATCH | `auth/users/:id` | owner | name, role, activation |
| POST | `auth/users/:id/reset-totp` | owner | clears another account's second factor |
| POST | `auth/users/:id/reset-password` | owner | resets another account's password |

## Sync and background jobs

| Method | Path | Role | Does |
| --- | --- | --- | --- |
| POST | `sync-holidays` | editor | job `holidays` / `fetch-holidays` |
| POST | `sync-parks` | editor | job `park-metadata` / `sync-all-parks` |
| POST | `fill-schedule-gaps` | editor | job `park-metadata` / `fill-all-gaps` ([schedule sync](../architecture/schedule-sync-and-calendar.md)) |
| POST | `sync-attraction-details` | editor | job `children-metadata` / `sync-attraction-details` |
| POST | `enrich-parks` | editor | job `park-enrichment` / `enrich-all` |
| POST | `import-ride-stats` | editor | job `ride-stats` / `import-stats` (Wikidata), body `{ limit? }` |
| POST | `sync-six-flags-heights` | editor | job `six-flags-heights` / `sync-heights` |
| POST | `publish-ride-profiles` | editor | job `manual-metadata` / `publish-ride-profiles`, body `{ sinceHours? }` ([ride profiles](curation.md#ride-profiles)) |
| POST | `detect-seasonal` | editor | job `analytics` / `detect-seasonal` ([seasonality](../architecture/attraction-status/seasonality.md)) |
| POST | `aggregate-accuracy-stats` | editor | job `prediction-accuracy` / `aggregate-stats` |
| POST | `backfill-park-day-operation` | owner | job `park-day-operation` / `backfill-park-day-operation` ([calendar rules](../architecture/calendar-schedule-and-ml-rules.md)) |
| POST | `dedupe-percentile-aggregates` | owner | job `analytics` / `dedupe-percentile-aggregates` |
| POST | `rebuild-downtime?days=` | **viewer** | job `downtime` / `reconstruct-downtime`, one attempt, `days` 1–400 (default 30) ([ride downtime](../analytics/ride-downtime/reconstruction.md)). It carries no `@AdminMinRole`, so any account can start it |
| GET | `queue-status` | viewer | job counts for eleven queues, not all of them |
| GET | `queue-failures?queue=&limit=3` | viewer | failure reasons of recent failed jobs; seven queues accepted (`wait-times`, `park-metadata`, `children-metadata`, `manual-metadata`, `six-flags-heights`, `ride-stats`, `prediction-accuracy`) |

## Models and forecasts

| Method | Path | Role | Does |
| --- | --- | --- | --- |
| POST | `train-ml-model` | editor | job `ml-training` / `train-model` ([CatBoost training](../ml/catboost-training.md)) |
| POST | `pcn/:action` | editor | job `pcn-shadow` / `train-pcn`, `forecast-pcn` or `score-pcn` for `train`, `forecast`, `score` ([PCN](../ml/pcn.md)) |
| GET | `system-health` | viewer | host, Postgres, Redis and both model services, with training status |
| GET | `ml-overview` | viewer | open decisions, matrix summary, component status ([ML overview](../ml/ml-overview.md)) |
| GET | `ml-comparison` | viewer | the PCN intraday comparison board |
| GET | `forecast-archive` | viewer | forward-archive scores per lead ([forward archive](../ml/forward-archive/README.md)) |
| GET | `model-comparison` | viewer | question × model matrix ([model comparison](../ml/forward-archive/model-comparison.md)) |
| GET | `model-comparison/day/options` | viewer | days, parks and rides with archived curves |
| GET | `model-comparison/day` | viewer | one ride-day, curves against truth |

The `/v1/ml/*` routes (`dashboard`, `models/metrics-history`, `accuracy/system`, `monitoring/*`,
`health`) are not admin routes: they have no guard and are only kept out of shared caches by their path.

## Data quality and curation

| Method | Path | Role | Does |
| --- | --- | --- | --- |
| GET | `data-quality?windowDays=14` | viewer | silenced clusters, failing jobs, the "season or gone?" list, re-issue candidates |
| GET | `reissue-candidates` | viewer | rides the feed may have re-issued under a new id ([re-issue](../architecture/attraction-status/reclassification-and-absence.md#a-re-issue-is-not-a-rename)) |
| GET | `retirement-candidates` | viewer | rides whose feed went quiet, minus those already cleared |
| POST | `review-marks` | editor | records candidates as investigated and not a case ([review marks](../architecture/attraction-status/review-marks.md)) |
| GET | `downtime-measurement` | viewer | read-only outage counts ([ride downtime](../analytics/ride-downtime/README.md)) |
| GET | `downtime-erasure` | viewer | read-only count of rewritten `DOWN` readings |
| GET | `ride-profile-term-audit` | viewer | ride-profile term ids the glossary no longer defines |
| GET | `page-views?days=30&limit=25&locale=` | viewer | page views by kind, page and language ([page views](../frontend/page-views.md)) |
| GET | `content/fields`, `content/resolve`, `content/overview`, `content/parks`, `content/parks/:id`, `content/parks/:id/attractions`, `content/attractions`, `content/attractions/:id`, `content/seasons`, `content/history` | viewer | the curation editor's reads ([curation](curation.md)) |
| PATCH | `content/parks/:id`, `content/attractions/:id`, `content/parks/:id/attractions` | editor | writes curated fields and publishes them |
| POST | `content/parks/:id/verify` | editor | records a check that changed nothing |
| PUT, DELETE | `content/attractions/:id/ride-profile` | editor | writes or removes a ride's glossary profile |
| POST, PATCH, DELETE | `content/parks/:id/seasons`, `content/seasons/:id` | editor | season rows ([park seasons](../frontend/park-seasons.md)) |
| POST | `content/history/:id/undo` | editor | undoes one curation ([undo](curation.md#undo)) |
| POST | `parks/:id/correct-location` | owner | corrects a park's city and coordinates (`ParkRenameService.correctLocation`) |
| POST | `validate-and-repair-parks` | owner | checks every park against Queue-Times and Wartezeiten.app; repairs ids with `{ autoFix: true }` |

## Merges and retirement

| Method | Path | Role | Does |
| --- | --- | --- | --- |
| GET | `duplicate-attractions` | viewer | duplicate ride rows within parks |
| GET | `duplicate-parks` | viewer | parks that look like one place, with the likely winner |
| POST | `merge-duplicate-attractions` | owner | dry run unless `dryRun: false`; one pair with `winnerId`/`loserId`, optionally `adoptLoserExternalId` |
| POST | `merge-duplicate-parks` | owner | `autoDetect` is a dry run unless `dryRun: false` and merges only pairs marked safe; a manual `park1Id`/`park2Id` pair merges for real unless `dryRun: true` |
| POST | `retire-attractions` | owner | retires rides with date and source |
| POST | `unretire-attraction/:id` | owner | undoes a retirement |
| GET | `retired-attractions` | viewer | everything retired, with kind and whether the park page lists it |
| POST | `retired-attractions/:id/hidden` | owner | `{ hidden }`: takes a closed ride off its park page; ride page and sitemap stay |
| POST | `retire-parks` | owner | retires a park and its active rides ([retired parks](../frontend/retired-parks.md)) |

Detection and the merge gates: [duplicates](../architecture/attraction-status/duplicates.md);
what a merge carries over: [curation](curation.md#merges); retirement by the sync and its undo:
[reclassification and absence](../architecture/attraction-status/reclassification-and-absence.md);
the rules for a destructive run: [automated retirement](../rules/automated-retirement-and-identity.md).

## Cache

| Method | Path | Role | Does |
| --- | --- | --- | --- |
| POST | `flush-cache` | owner | deletes the Redis keys in `PARK_CACHE_FLUSH_PATTERNS` (`src/admin/flush-cache-patterns.ts`) |
| POST | `cache/reset?confirm=true` | owner | the same flush, then queues `fetch-holidays`, `sync-all-parks`, `fetch-all-children` and `fetch-wait-times`; Bull keys and admin sessions stay. Without `confirm=true` it refuses |

Neither touches Cloudflare or the frontend's cache ([publish outside-in](../rules/publish-outside-in.md));
when to flush after a logic change: [data recalculation](../analytics/data-recalculation.md).
