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
| 1 | Does `/plan/day` serve parks that close after midnight? | **Yes.** 15 of the 21 wrapping parks serve hours past 23 (max 25). The 6 that do not are data availability, not the wrap. |
| 2 | Is `hourly_agg` still hot after the cache fix? | **Yes — decisively.** 49.1 % of one core in a fresh 23.5-min window. The decision gate opens. |
| 3 | Does the ride-profile term audit cron run cleanly? | **Handler yes**, 152 ids and 0 broken; the cron is evidenced firing 29 times. A clean run *triggered by the cron* is the one thing still open. |
| 4 | Does `curated_may_get_wet` override a diverging sync? | **Yes**, proven on the first real divergence, 2026-09-13. |

**No regression was found in any of the four.** Three follow-ups were filed, none of
them a regression *of* these fixes: PAR-192 (La Ronde has been silent for 82 days),
PAR-193 (the frontend can drop its double lookup now that the wrap contract is
measured) and PAR-194 (26 % of parks that are open on a given day 30 days out serve
no plan at all — found while explaining away the wrap misses, and the reason those
misses could be explained away so quickly).

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

> **One wrap day per park is not a verdict on the park.** The query above takes
> `DISTINCT ON (p.id) … ORDER BY p.id, s.date`, i.e. each park's *earliest* wrap date.
> A park whose earliest wrap day happens to be thin then lands in the "does not serve"
> column while serving fine a month later — which is exactly what happened on the
> first pass here, to Parque Warner Madrid and Six Flags Great America, producing a
> count that contradicted this document's own first table. The survey was therefore
> re-run over **up to five wrap dates per park** (first, quartiles, last): 72 park-days
> across the 21 parks.

**21 parks. 15 serve hours past 23** on at least one of their wrap days, the highest
observed hour being **25** (Parc Astérix, Walibi Rhône-Alpes and Six Flags Magic
Mountain). Twelve of the fifteen serve wrap hours on *every* date sampled.

### The six that did not, and why none of them is the wrap

This is the part worth keeping, because every one of them *looks* like a wrap bug:

| Park | Wrap days sampled | Actual cause |
| ---- | ----------------- | ------------ |
| La Ronde | 5 | Both upstream feeds silent since June → **PAR-192**. Also answers `0` on non-wrap days. Numbers below. |
| Six Flags Mexico | 5 | Rolled up every day, **0 qualifying samples** every day (41 rows, 0 slots). Also `0` on non-wrap days. |
| Movieland | 1 | No `queue_data` row has ever existed for it. |
| Mirabilandia | 1 | Serves 20 rides on a past non-wrap day; returns `0` for *every* future date, wrap or not. |
| Mid-America Parks | 1 | Returns `0` on past and future dates alike, wrap or not. |
| Six Flags Over Texas | 1 | Its one wrap day is lead 60, but the horizon is not the cause — Cedar Point serves 15 rides at leads 59–61. This park serves at most 1 ride on any future date. |

All six are data availability. None of them answers differently on a wrap day than it
does on a comparable non-wrap day, which is the test that separates the two causes.

La Ronde is the one that most looks like a wrap bug — it wraps on 91 of the 91 days in
the window — so its numbers belong here rather than only in PAR-192:

```sql
SELECT q.data_source, q.is_heartbeat, count(*), max(q.timestamp)
FROM queue_data q JOIN attractions a ON a.id = q."attractionId"
WHERE a."parkId" = '143165d2-31a6-4404-86b3-7d96617758a0'
  AND q.timestamp > now() - interval '120 days'
GROUP BY 1,2 ORDER BY max(q.timestamp) DESC;
```

| data_source | rows | last row | silent for |
| ----------- | ---- | -------- | ---------- |
| queue-times | 7 269 | 2026-06-24 | **82 days** |
| themeparks-wiki | 32 163 | 2026-06-17 | **89 days** |

No heartbeat rows. Both upstream APIs still answer and still know the park — they just
carry nothing: `api.themeparks.wiki/v1/entity/…/live` returns `liveData: []` and
`queue-times.com/parks/48/queue_times.json` returns `{"lands":[],"rides":[]}`, while
Cedar Point through the same API returns 86 live entries. The schedule meanwhile runs
351 further operating days, to 2027-08-31.

Four of the six turned out to be instances of something wider, measured afterwards and
filed as **PAR-194**: at lead 30, **19 of the 73 parks that are open that day serve no
plan at all**. The wrap survey did not find a wrap bug; it found the edge of that hole.

Two further readings that looked like misses and are not:

| Case | Reading | Actual cause |
| ---- | ------- | ------------ |
| Six Flags Great America, 09-12 | `rides: 0` | The rollup for that park-local day **did not exist yet** (see below). The park serves wrap hours on 3 of its 4 sampled wrap days. |
| Parque Warner Madrid, 09-05 | hours stop at 23 | Hour 0 of 09-06 has 38 raw rows and **0** that are `OPERATING` + `STANDBY` + `waitTime >= 5`. The park serves hours through 24 on 4 of its 5 sampled wrap days. |

> **The trap, written down because it cost a wrong conclusion first.**
> For a past date, `buildPlanDay` takes the `!isFuture` branch
> (`plan-day.service.ts:273-284`) into `observedRides()` (`:901-1000`), which reads
> **only** the pre-aggregated `attraction_hourly_history` — never `queue_data`. On a
> wrap day it additionally reads the *next* day's row for the midnight hour
> (`:913-918`). Abundant raw `queue_data` therefore says nothing about whether the
> endpoint should answer. The code says so itself, in two places: the `observedRides`
> docblock (`plan-day.service.ts:897-900`) — *"which is not the same statement as an
> empty queue"* — and `analytics.service.ts:5720`, *"Absence is 'not rolled up yet'"*.

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

### Measured, 2026-09-14 02:12:10–02:35:38 UTC, 23.5 min, 15 samples

The protocol asks for 30–60 min; this window is 23.5. What that duration is *for* is
the binding condition, and it was checked rather than assumed: the window contains
**both** `*/15` firings of `generate-hourly:hourly-predictions-cron` (02:15:00,
02:30:00) and five `*/5` firings of `fetch-wait-times:wait-times-cron`, so "≥2
prediction crons + on-demand traffic" is satisfied.

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

**The cache's real lifetime is the worker's, not the TTL's — and they are the same
order of magnitude.** The cache is a module-global dict, so it dies with its worker.
`gunicorn.conf.py` sets `max_requests = 1000` (jitter 200) across 2 workers; 25 worker
boots over 6 h 35 min give a mean worker lifetime of ≈ **32 min** against a **15 min**
TTL. So recycling does not truncate the TTL — it allows roughly two cache generations
before a cold start, and each worker holds its own copy, so a park is fetched once per
worker. A real effect, a moderate one, and the number to size the per-attraction split
against: raising the TTL past ~30 min buys nothing until `max_requests` moves too.

---

## 3 · Ride-profile term audit cron

Registration was previously evidenced only by `delayed: 1`. Redis says more — the
repeatable job `audit-ride-profile-terms:ride-profile-term-audit-cron:::30 6 * * *` has
a pending delayed instance scheduled for 2026-09-14 06:30 UTC carrying
`"repeat": {"count": 30, …}`. Bull increments `count` per scheduled instance and this
one has not run yet, so it evidences **29 firings**, the last on 2026-09-13 06:30 UTC.

> **What `count` does and does not prove.** It counts instances Bull *created and
> handed to a worker*, not handlers that finished cleanly — and `removeOnComplete` /
> `removeOnFail` are both `true`, so a completed run and a failed one leave the same
> trace, namely none. The cron's *delivery* is therefore established; its *outcome* is
> not, and could not be read from the log either: the running container started
> 2026-09-13 16:47 UTC, after the last 06:30 run, and Docker keeps no log from its
> predecessor. Grepping the live log for both branches returns nothing at all.

That the *handler* completes cleanly was therefore shown a different way, without
waiting for 06:30.
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

**Net:** the handler is proven clean and the cron is proven to fire. What remains
unproven is the conjunction — a clean run *triggered by the cron* — and the cheapest
way to close it is to read the log after any 06:30 UTC run, looking for **either**
branch: the clean line above or `🎢 Ride-profile term audit could not run`
(`curated-data.processor.ts:105-106`), which is what an unreachable frontend produces.

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

Two controls, because one of them does not actually discriminate:

| Control | DB `may_get_wet` | DB `curated_may_get_wet` | API `mayGetWet` | Rules out |
| ------- | ---------------- | ------------------------ | --------------- | --------- |
| the divergence above | `true` | `false` | `false` | "always raw" |
| `river-rapids`, `stanley-falls-flume`, `splash-battle` | `true` | `true` | `true` | **nothing** — both candidate rules agree here |
| `pirates-…-sunken-treasure`, `river-quest` | `true` | `NULL` | `true` | "always curated" |

Only the first and third rows carry weight. Together they pin the behaviour to
`curated ?? raw`: the curated value wins when present, the raw value is served when it
is not. The middle row was the first control run here and proves nothing on its own —
where the two columns agree, every candidate rule returns the same answer.

> In the JSON response the field is top-level `mayGetWet` and is `null` for most of the
> 7 211 rides. A `jq 'paths(scalars)'` filter skips nulls and will report the field as
> absent.
