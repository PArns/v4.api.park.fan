# Post-deploy verification (2026-09-14)

> Four shipped fixes that could only be confirmed against production. None of them
> changed code here; each was a question the repo could not answer about itself.
> Run on `celestrial` against the deployed build, `origin/main` @ `2850e7e` — the
> running `api-*` image carries that exact SHA and `main` had **zero** commits
> beyond it, so everything measured below is what visitors were being served.
>
> Ticket: PAR-42. Queries went to `http://localhost:3000` (the same container the
> public API runs in) rather than `https://api.park.fan`, which skips Cloudflare and
> the global throttler without changing what is measured.

## TL;DR

| # | Question | Answer |
| - | -------- | ------ |
| 1 | Does `/plan/day` serve parks that close after midnight? | **Yes.** 13 of the 21 wrapping parks serve hours past 23 (max 25). The 8 that do not are data availability, not the wrap. |
| 2 | Is `hourly_agg` still hot after the cache fix? | **Yes — decisively.** 49.1 % of one core in a fresh 23.5-min window. The decision gate opens. |
| 3 | Does the ride-profile term audit cron run cleanly? | **Yes.** 30 firings recorded; the handler returns 152 ids, 0 broken. |
| 4 | Does `curated_may_get_wet` override a diverging sync? | **Yes**, proven on the first real divergence, 2026-09-13. |

**No regression was found in any of the four.** Two follow-ups were filed: PAR-192
(La Ronde has been silent for 82 days) and PAR-193 (the frontend can drop its
double lookup now that the wrap contract is measured).

---

## 1 · Midnight-wrap parks

The fix (`unfoldedCloseHour`) makes `/plan/day` answer for a park whose operating
day runs past local midnight; before it, such a park answered `rides: []`.

**Six Flags Qiddiya City, 2026-09-02** — the case the todo entry named as sharpest:

```
openHour: 16 · closeHour: 0 · rides: 24 · hours run through 24
```

Matches the expectation exactly, and the park previously answered with an empty
list. Cedar Point and Parque Warner Madrid on 2026-10-31 likewise serve 15 and 31
rides with hours through 24.

### Widened to every wrapping park

Three samples cannot separate "the fix works" from "these three happen to work", so
the check was run against **all** parks whose park-local operating day crosses
midnight in a three-month window:

```sql
SELECT DISTINCT ON (p.id) p.name, s.date
FROM schedule_entries s JOIN parks p ON p.id = s."parkId"
WHERE s."attractionId" IS NULL AND s."scheduleType" = 'OPERATING'
  AND s.date BETWEEN '2026-09-01' AND '2026-11-30'
  AND (s."closingTime" AT TIME ZONE p.timezone)::date
    > (s."openingTime" AT TIME ZONE p.timezone)::date
ORDER BY p.id, s.date;
```

Group by `p.id`, never by `p.slug` — `disneyland-park` exists twice and a slug-keyed
count silently merges Anaheim with Paris.

**21 parks. 13 serve hours past 23**, the highest observed hour being **25** (Parc
Astérix and Walibi Rhône-Alpes, both running 19:00–01:00 park-local).

### The eight that did not, and why none of them is the wrap

This is the part worth keeping, because every one of them *looks* like a wrap bug:

| Park | Reading | Actual cause |
| ---- | ------- | ------------ |
| La Ronde | `rides: 0` | Both upstream feeds silent since June → **PAR-192**. Also answers `0` on non-wrap days. |
| Movieland | `rides: 0` | No `queue_data` row has ever existed for it. |
| Six Flags Great America, 09-12 | `rides: 0` | The rollup for that park-local day **did not exist yet** (see below). |
| Six Flags Mexico, 09-03 | `rides: 0` | Rolled up, but **0 qualifying samples** (41 rows, 0 slots). |
| Mid-America Parks, Mirabilandia, Six Flags Over Texas | `rides: 0` | Every *future* date returns 0 for these parks, wrap or not. |
| Parque Warner Madrid, 09-05 | hours stop at 23 | Hour 0 of 09-06 has 38 raw rows and **0** that are `OPERATING` + `STANDBY` + `waitTime >= 5`. |

> **The trap, written down because it cost a wrong conclusion first.**
> For a past date, `buildPlanDay` takes the `!isFuture` branch
> (`plan-day.service.ts:273-284`) into `observedRides()` (`:901-1000`), which reads
> **only** the pre-aggregated `attraction_hourly_history` — never `queue_data`. On a
> wrap day it additionally reads the *next* day's row for the midnight hour
> (`:913-918`). Abundant raw `queue_data` therefore says nothing about whether the
> endpoint should answer; the entity comment puts it exactly right: *"absence is 'not
> rolled up yet' … which is not the same statement as an empty queue."*

Six Flags Great America on 2026-09-12 is the worked example. Raw data was complete —
1 503 rows, 55 attractions, `OPERATING` measurements through hours 10–23 plus the
midnight hour — and the rollup row was simply absent, because **the rollup job keys
on the park-local previous day**. It runs 04:30 UTC, which in `America/Chicago` is
still 23:30 of the day before; so at 2026-09-13 04:30 UTC it computed 09-11 for that
park while computing 09-12 for European parks. Globally 09-12 *was* rolled up (6 135
rows); for this park it was due at 2026-09-14 04:30 UTC.

**Before calling a stale-looking answer a bug, ask which table the read path uses.**

### The wrap contract, now measured

| Park | Date | `context.openHour` | `context.closeHour` | `hours[].hour` |
| ---- | ---- | ------------------ | ------------------- | -------------- |
| Parc Astérix | 2026-10-16 | 19 | **1** | `[19,20,21,22,23,24,25]` |
| Walibi Rhône-Alpes | 2026-10-31 | 19 | **1** | `[19,20,21,22,23,24,25]` |

`hours[].hour` is **unfolded** (24 = midnight, 25 = 01:00); `context.closeHour` is the
**folded** wall clock. The two fields speak different languages, which is what left
the frontend looking a ride's curve up twice. Note the range runs past 24 — the todo
entry only claimed "24 = midnight". Filed as **PAR-193**.

---

## 2 · ML `hourly_agg` cache

`fetch_recent_wait_times` (`ml-service/predict.py`) is the steady-state DB load. The
shipped fix buckets `end_time` to the cache-TTL window, raises the TTL to 900 s and
evicts expired entries on write — all three are present in the deployed code
(`predict.py:196-215, 241-254, 369-376`).

### The 2026-06-03 baseline could not simply be re-run

`pg_stat_statements` has **never been reset since 2026-06-03 07:50 UTC** — the very
reset the old baseline was taken after. Its counters therefore span 102.8 days and
mix pre-fix and post-fix behaviour; they cannot answer "is it hot *now*".

The documented protocol says to reset and re-measure. **That reset was deliberately
not performed**: it destroys 103 days of accumulated history that nothing else
records, it is irreversible, and the ticket does not ask for it. A **delta of the
cumulative counters** over a fresh window gives the same rate, read-only.

### Measured, 2026-09-14 02:12–02:36 UTC, 23.5 min, 15 samples

| | calls/min | DB ms/min | share of one core | mean ms/call |
| - | --------- | --------- | ----------------- | ------------ |
| Pre-fix baseline, 2026-06-03, 133 min | 29.7 | 8 551 | 14 % | 275 / 373 |
| **Fresh window, 2026-09-14, 23.5 min** | **48.5** | **29 478** | **49.1 %** | **608** |
| Lifetime average since 2026-06-03 (102.8 d) | 15.6 | — | 12.1 % (298.8 DB-hours) | 446 / 513 |

**The load is extremely bursty.** Per-interval rates across the window:
`29, 3, 44, 169, 8, 43, 86, 7, 18, 24, 13, 58, 168, 10` calls/min. Any window shorter
than a few minutes produces an arbitrary number — an early 2.8-min sample read 7.1
calls/min, a later one 61.

> **What this does *not* establish.** The fresh window is higher than the 2026-06-03
> baseline, and it is tempting to read that as "the fix did not work". That reading is
> not supported: the two measurements differ in time of day, in three months of
> traffic growth, and in the size of `queue_data` the query scans — per-call cost
> alone has risen from ~275/373 ms to ~608 ms. The lifetime average (15.6 calls/min)
> sits *below* both endpoints, which means this window caught a busy stretch rather
> than a representative one. Comparing a 23.5-min night window against a 133-min
> morning window three months earlier is a category error, and it is precisely why
> the protocol was marked stale. What the number *does* settle is the only question
> the decision gate asks: **is it still hot? Yes.**

### Decision gate: the single-attraction path dominates

Two fingerprints share the `hourly_agg` text. They differ in exactly one place:

```
queryid 3495971031891626369 … ANY(ARRAY[$3])           ← one id  → single-attraction
queryid 8118237830757439491 … ANY(ARRAY[$3 /*, ... */]) ← a list → park-level
```

| Path | Lifetime calls | Lifetime DB time | **Fresh-window calls** | **Fresh-window DB time** |
| ---- | -------------- | ---------------- | ---------------------- | ------------------------ |
| Single-attraction | 70.5 % | 67.4 % (201.4 h) | **90.8 %** | **90.5 %** |
| Park-level | 29.5 % | 32.6 % (97.2 h) | 9.2 % | 9.5 % |

`getAttractionPredictions` (`src/ml/ml.service.ts`, attraction-detail pages) uses a
per-single-attraction cache key and does **not** reuse the park-level fetch. A
park-level call returns ~21 500 rows against ~1 100 for a single attraction — roughly
twenty attractions' worth of data that the single-attraction path re-fetches one at a
time.

**Verdict: hot. The follow-up in the todo entry — cache split by `attractionId` so
both paths share entries — is warranted**, and the window functions are already
`PARTITION BY "attractionId"`, so the split is safe.

### Two things found alongside

**The TypeORM slow-query log cannot see this query at all.** It has **zero**
`hourly_agg` hits across 2026-09-10…09-14. That is not evidence of health: the
threshold lives in `src/config/typeorm.config.ts:24` (`maxQueryExecutionTime: 500`)
and the logger is a *TypeORM* logger, while this query is issued by the Python
ml-service through its own SQLAlchemy engine (`ml-service/db.py:67`). It could never
appear there at any duration. **Closing this out "against the current slow-query log"
would have read structural blindness as a clean bill of health.**

**Worker recycling bounds the cache below its own TTL.** The cache is a module-global
dict, so it dies with its worker. `gunicorn.conf.py` sets `max_requests = 1000`
(jitter 200) across 2 workers; 25 worker boots over 6 h 35 min give a mean worker
lifetime of ≈ 32 min against a 900 s TTL — about two cache generations before a cold
start. Each worker also holds its own copy, so a park must be fetched once per worker.
A real effect, though a moderate one, and worth having in view before the
per-attraction split is sized.

---

## 3 · Ride-profile term audit cron

Registration was previously evidenced only by `delayed: 1`. Redis says more — the
repeatable job `audit-ride-profile-terms:ride-profile-term-audit-cron:::30 6 * * *`
carries `"repeat": {"count": 30, …}`: **30 firings**, the last at 2026-09-13 06:30 UTC,
the next due 2026-09-14 06:30 UTC.

That the *handler* completes cleanly was shown without waiting for 06:30.
`handleAuditRideProfileTerms` (`curated-data.processor.ts:99`) does nothing but call
`rideProfileAudit.audit()`, and the same method hangs synchronously off
`GET /v1/admin/ride-profile-term-audit` (`admin.controller.ts:1229`) — same path, same
logger, different trigger:

| storedTermIds | glossaryTermIds | unusedGlossaryTermIds | broken |
| ------------- | --------------- | --------------------- | ------ |
| 152 | 274 | 122 | **0** |

The live container logged the line the todo entry asked for:

```
[RideProfileAuditService] 🎢 Ride-profile term audit clean: 152 stored ids all resolve
```

That is the `else` branch at `ride-profile-audit.service.ts:100`, reachable only when
`broken.length === 0`. 122 of 274 glossary terms are unused by any curation — expected,
the glossary is the superset.

---

## 4 · `curated_may_get_wet` divergence

The correction layer could only prove itself once a sync wrote a value differing from
the curated one. **That happened on 2026-09-13 04:00:23 UTC.**

| | attractions | curated | **diverging** | curated without raw value |
| - | ----------- | ------- | ------------- | ------------------------- |
| | 7 211 | 131 | **1** | 60 |

The single case is the ride the todo entry named — listed there as "Genting's Shot
Tower", today **Terraform Tower Challenge**
(`089adee1-d196-4c2c-8581-29f46943808a`, Genting SkyWorlds Theme Park):
`may_get_wet = true` raw, `curated_may_get_wet = false`.

The ride endpoint serves **`mayGetWet: false`** — the curated value overrides the sync.
Control cases where curated and raw agree (`river-rapids`, `stanley-falls-flume`,
`splash-battle`) all serve `true`, so the read path is returning neither column
unconditionally.

> In the JSON response the field is top-level `mayGetWet` and is `null` for most of the
> 7 211 rides. A `jq 'paths(scalars)'` filter skips nulls and will report the field as
> absent.
