# API Performance & Cache Audit (2026-06-01)

> Investigation into "park endpoints load forever". Root cause, what was shipped, and a
> prioritized roadmap for the full Redis/caching overhaul. **Page speed is the goal.**

## TL;DR

The park **integrated** endpoint, **stats**, and **weather/nowcast** are all fast (~0.4 s).
The **calendar** endpoint is the only slow one: **~15–20 s on a cold cache**, and it
**blocks the whole SSR park page** even though the calendar is behind a non-default tab.

Two distinct problems:

1. **Cold calendar build = ~15 s** — dominated by the cold `getParkPredictions("daily")`
   CatBoost call (long-tail days 31–365 × all active attractions, CPU). TFT is *not* the
   problem (nightly GPU → `tft_forecasts` table → cheap read).
2. **Periodic DB saturation** — a few background analytics queries are catastrophically slow
   (one CTE peaked at **227 s**), and while they run, normal user queries queue behind them
   (p90 of slow-query log = **14.5 s**). This is what makes otherwise-fast endpoints
   occasionally spike to 400 ms+.

## Measurements (live, cache-busted to hit origin)

| Endpoint | Cold (origin) | Warm |
|---|---|---|
| `/parks/.../` (integrated) | 0.45 s | 0.35 s |
| `/parks/.../stats` | 0.41 s | 0.32 s |
| `/parks/.../weather/nowcast` | 0.41 s | — |
| `/parks/.../attractions/{slug}` | 0.39 s | — |
| `/parks/.../calendar` (90 d) | **15–20 s** | 0.05–0.9 s |

## Slow-query log (`/data/parkfan/logs/slow-queries.YYYY-MM-DD.log`, >500 ms)

487 slow queries in a day. **p50 = 1.0 s, p90 = 14.5 s, p99 = 25.7 s, max = 227.9 s.**
Top offenders by total time — **all in background processors, not the request path**, but
they saturate the DB:

| n | total | max | query | source |
|---|---|---|---|---|
| 51 | 876 s | 25.7 s | `ParkP50Baseline` read | contention-blocked behind the monsters |
| 6 | 466 s | **227.9 s** | `WITH park_open_days …` CTE | `queue-percentile.processor.ts` |
| 22 | 179 s | 14.1 s | `holiday` SELECT | `holidays.service.ts` (uncached) |
| 33 | 93 s | 14.8 s | `AVG + PERCENTILE_CONT(waitTime)` | `getTypicalStatsForHour` (per attr/hour/dow) |

The 14 s "floor" hit by many *different* queries = everything queuing behind the monsters.

## Redis keyspace audit (live)

69 318 keys, 318 MB / 1 GB, `allkeys-lru`, 68 671 with expiry.

| count | prefix | note |
|---|---|---|
| **29 526** | `analytics:typical:{attractionId}:{hour}:{dow}` | **43 % of the keyspace.** Lazily filled by the 14.8 s `getTypicalStatsForHour` query. Caching exists (22 h TTL) but cold misses are 14.8 s each → should be **batch-precomputed**, not lazy. |
| 5 372 / 4 385 | `parkfan:attraction*` / `parkfan:queue*` | BullMQ internal |
| 4 594 | `attraction:last-seen` | ~14 d TTL, fine |
| 3 310 | `analytics:crowdlevel:park:{id}:{date}` | per park/day historical crowd cache |
| 2 780 / 2 653 | `attraction:p50` / `attraction:p90` | per-attraction baselines |
| 24 | `calendar:month:{id}:{ym}:{variant}` | **the calendar month cache** |

No significant orphan/no-TTL problem (only ~647 of 69 k keys lack expiry — mostly Bull).
The real Redis issue is not orphans, it's **shape**: 43 % of keys come from one lazy,
expensive aggregation that should be precomputed.

## Hot-path methods with NO Redis cache (called on every cold calendar build)

- `getOperatingDateRange` (parks.service) — MIN/MAX over `schedule_entries`
- `getHeadlinerAttractions` (analytics.service) — `headlinerAttractionRepository.find`
- `getHolidays` (holidays.service) — uncached, 22 hits/day at up to 14 s

(`isParkSeasonal` and `getDerivedHistoricalHours` *are* cached.)

## Shipped 2026-06-01 (this pass)

1. **Prewarm key fix** — the daily calendar warmup warmed `includeHourly="today+tomorrow"`
   while the frontend requests `"none"`. Different month-cache keys → the warmup never
   served a single real request. Now warms `"none"`. (`cache-warmup.service.ts`)
2. **Daily ML TTL 6 h → 13 h** — was going cold ~6 h after the once-daily warmup; first
   visitor then paid the 15 s. Key already self-invalidates per park-local date.
   (`ml.service.ts`)
3. **12 h force-refresh warmup** — `warmup-calendar-daily` cron `0 5` → `0 8,20` UTC (both
   *after* the 03:00 TFT + 06:00 CatBoost training window, done before 09:00), and the
   warmup now **force-evicts** `ml:park:daily` + the month caches so each run actually
   refreshes (weather/model). Users never hit the cold path. (`cache-warmup` + scheduler)
4. **Partial month-cache assembly** — the all-or-nothing `monthCached.every()` rebuilt the
   whole range whenever one trailing month was uncached. Now assembles cached months and
   builds only the missing ones. (`calendar.service.ts`)

## Shipped 2026-06-01 — part 2

5. **P0 Frontend lazy-load** (park.fan repo) — the park page no longer awaits the calendar
   in its blocking `Promise.all`. The calendar tab already client-fetches per visible month
   (`useCalendarData`); best-days + FAQ now stream via `<Suspense>` over a non-blocking
   `calendarPromise`. Shell paints in ~0.4 s regardless of calendar backend latency.
   (`page.tsx`, `live-park-data`, `tabs-with-hash`, `park-calendar-grid` — calendar seed
   made optional). Typecheck + lint clean; **browser verification still pending.**
6. **P2 read-through caches** — `getHeadlinerAttractions` (6 h), `getOperatingDateRange`
   (1 h), `getHolidays` (24 h) now cache in Redis. Cuts warmup + nearby/favorites + DB load.

## Prioritized roadmap (remaining)

**P1 — Kill the DB saturation.** The 227 s `park_open_days` CTE
(`queue-percentile.processor`) and the 14.8 s `getTypicalStatsForHour` are the root of the
14 s contention floor that spikes every other endpoint (incl. nearby/favorites/400 ms).
Options: index/rewrite the CTE (TimescaleDB chunk exclusion, avoid `EXTRACT` on un-indexed
timestamps), cap job concurrency, and **precompute** the typical-hour stats into a
materialized table on a daily cron instead of lazy per-request.

**P3 — Redis shape** — precompute `analytics:typical:*` (43 % of keyspace) so it's never a
cold 14.8 s miss. Same materialization as P1's typical-hour stats.

## Shipped 2026-06-01 — part 3 (P1/P3, signed off)

7. **`getTypicalStatsForHour` reads `queue_data_aggregates`** instead of scanning 2 years of
   raw `queue_data` (`analytics.service.ts`). **Live-verified: 224 ms → 8 ms**, avg exact,
   p95 (95th-pct of bucket p95s, matching the existing `getParkStatistics` pattern) tracks
   raw well (raw 55/70/35 → 56/70/37). This was both a P3 item (kills the lazy 14.8 s misses
   behind the 29.5 k `analytics:typical:*` keys) and the user-facing half of P1. tsc + 75
   analytics tests green. **p95 kept** (deliberate public DTO field, not the P50/P90 system).

### ✅ Refurb-detection CTE (227 s → ~3 s) — dedicated `attraction_day_operating` rollup

Done. A new entity `attraction_day_operating(attractionId, parkId, op_day)` — PK
`(attractionId, op_day)`, index `(parkId, op_day)` — records, per **any-OPERATING /
any-queueType** park-local day, that an attraction operated (semantics the STANDBY-only
`queue_data_aggregates` could NOT provide → would cause false refurb flags). The 3 CTE blocks
in `detect-seasonal` (`queue-percentile.processor`) now read `park_open_days` /
`attraction_operating_days` from this small table instead of re-scanning 60 days of raw
`queue_data`. `refreshOperatingDayRollup()` runs at the start of the job: incremental upsert
(from 2 local days before the last stored day, `ON CONFLICT DO NOTHING`), full 65-day backfill
only when empty, prunes beyond lookback+30d.

**Deploy backfill:** automatic — the existing bootstrap trigger enqueues `detect-seasonal`
~90 s after boot; the empty-table branch backfills then.

**Live-verified (temp-table simulation):** backfill 175 k rows in 2.1 s; full rewritten
detection query **227 s → 1.08 s** (Postgres inlines the rollup CTEs and uses the PK index
for the `NOT EXISTS`). Equivalence check over a 14-day window: `in_old_not_new = 0` (zero open
days lost → no missed/false flags); the rollup is marginally *more* complete at the window
edge. tsc + build green.

## P1 + P3 design (diagnosed 2026-06-01 — needs prod-schema sign-off before implementing)

`queue_data` = **3.7 GB hypertable, 160 chunks.** It already has a good partial index
`idx_queue_data_operating (attractionId, timestamp) WHERE status='OPERATING'`, so the slow
queries are **not** missing an index — they re-aggregate millions of raw rows live:

- **227 s `park_open_days` CTE** (`queue-percentile.processor`, refurb/seasonal detection,
  60-day window): cost = repeated 60-day scans + `DISTINCT (park/attr, day)` +
  the `days_fully_closed` cross-join × `NOT EXISTS`, with `DATE(timestamp AT TIME ZONE tz)`
  computed per row (defeats pure index-only / chunk pruning on the bucket).
- **14.8 s `getTypicalStatsForHour`**: `AVG + PERCENTILE_CONT(0.95)` over **2 years** per
  `(attractionId, hour, dow)`, lazily on request → also the source of the 29.5 k
  `analytics:typical:*` Redis keys (P3).

**Recommended fix (solves P1 + P3 together): a daily-materialized summary table**, not a
TimescaleDB continuous aggregate — CAggs bucket on a fixed tz, but every park needs its own
`AT TIME ZONE`, which a daily cron can do per-park:

- `attraction_hourly_stats(attractionId, hour, dow, avg_wait, p95_wait, updated_at)` —
  refreshed nightly. `getTypicalStatsForHour` reads one indexed row instead of a 14.8 s scan
  (and the lazy `analytics:typical:*` cache becomes unnecessary).
- `attraction_day_operating(attractionId, parkId, op_day)` (or a park-day presence rollup)
  refreshed nightly so the refurb job joins small precomputed tables instead of re-scanning
  60 days of `queue_data` every run. Also cap that job's window/concurrency.

⚠️ Prod schema change + backfill + new crons → get explicit go-ahead and verify with
`EXPLAIN ANALYZE` before/after.

## Other reports to triage (2026-06-01, not yet investigated in depth)

- **INP > 200 ms (mobile), homepage `/en`** — GSC, 29 URLs, group INP 245 ms. Client-side.
  The obvious suspect (the rAF marquee ticker) is already optimized (width measured once,
  GPU-composited transform, IntersectionObserver-paused), so the real culprit needs **field
  attribution** — which wasn't being collected. **Instrumented (park.fan):** Next's
  `useReportWebVitals` + `experimental.webVitalsAttribution: ['INP','LCP','CLS']` →
  `WebVitalsReporter` reports `interactionTarget` + the input/processing/presentation
  breakdown to Umami (events `web-vital-inp/lcp/cls`). Once a few days of field data land,
  the exact slow interaction/element is identifiable → targeted fix. No extra dep (Next
  bundles web-vitals).
- **nearby + favorites** — favorites already MGETs `park:integrated` / `attraction:integrated`
  (fast path); nearby now does too (`findNearbyParks` MGET, batch queries only for misses).
  favorites cold ~0.67 s = first-ever favorite of a niche attraction (full build + caches it,
  warming the shared cache the detail page also uses — deliberately NOT made "lightweight",
  that would lose the shared warming). Search "epuc"/"epuc univ" → Universal Epic Universe
  **verified live**.

## Post-deploy load (2026-06-01)

Redis persists across a deploy, so the boot should read from cache, not re-run everything:
- Startup warmup is no longer forced (`warmupTopParksOnStartup` force=false) → warm parks skip.
- Boot job triggers gated on data presence: accuracy-stats only when its table is empty;
  detect-seasonal only when the `attraction_day_operating` rollup is empty. Crons own the
  steady-state timing.
- Minimum cache TTLs raised to 5 min (wait-times only sync every 5 min, so shorter = churn):
  favorites 2→5 min; HTTP discovery/search/stats 5 min; `/health` stays 2 s (liveness).
- ~~**Search misses "epuc universe" → Epic Universe**~~ ✅ **Done** — root cause: the
  *primary* matcher is in-process (`searchParksInProcess`, not Postgres), and it only had
  whole-string trigram similarity (a 1-char typo in a short word tanks it). Added word-level
  Levenshtein typo tolerance (tier 7) in-process + lowered `pg_trgm.word_similarity_threshold`
  to 0.4 for the Postgres fallback. See `docs/architecture/search.md`.
