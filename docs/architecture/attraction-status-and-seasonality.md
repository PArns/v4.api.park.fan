# Attraction Status & Seasonality

> **2026-08-15.** Written after an investigation that started with two
> playgrounds reading CLOSED and ended with ~140 attractions across ten parks
> doing the same for the wrong reason, a background job that had been dead for
> 73 days, and every `season_months` value in the database turning out to
> describe our recording window rather than a season.

This document is the one place that says **who owns which cell** and **what an
absent fact is allowed to become**. Both questions have been answered
inconsistently before, in ways that shipped.

---

## 1. The governing rule

> An absent fact never becomes a confident one.

The codebase already applied this to crowd levels (`unknown` rather than a
placeholder `moderate`, see [Crowd Levels](../analytics/crowd-levels.md)). It
now applies to attraction status and to seasonality as well. Each of the bugs
below is the same mistake in a different costume: **our own bookkeeping served
as somebody else's statement.**

---

## 2. Attraction status: three regimes

`AttractionStatus` = `OPERATING | CLOSED | DOWN | REFURBISHMENT | UNKNOWN`,
from `ATTRACTION_STATUS_VALUES` in `common/types/status.type.ts`. Never
hand-write that list in a Swagger `enum:` — `UNKNOWN` was served for months
while the union claimed four values, the same drift that kept `unknown` out of
the published crowd-level contract.

### 2.1 A real feed reported it
The normal path. The status comes off the latest `queue_data` row.

### 2.2 Free-flow: the flag reports it
Playgrounds, splash pads and climbing structures have no queue, so the feed
calls them CLOSED all day. `attractions.open_with_park` overrules that.

**One function, three callers** — `common/utils/free-flow-status.util.ts`,
used by `park-integration`, `attraction-integration` and `favorites`. It lives
in one place because the rule was written three times and one copy was missing:
the park's ride list, which is the surface people actually look at.

`isFreeFlowOpen({ openWithPark, parkStatus, waitTimesReadable, seasonMonths, parkTimezone })`
is true only when all of:

| condition | why |
|---|---|
| the flag is set | it is what marks a free-flow attraction |
| park is `OPERATING` | a playground in a closed park is behind a gate |
| wait times are readable | the Hansa-Park rule: nothing below an unreadable park may claim to run |
| today is in season | see §3 — added so seasonal free-flow areas can carry the flag at all |

Free-flow **outranks** the source-absence rule in §2.3: its flag, not the feed,
is what makes it open. In `favorites` that ordering needs an explicit guard,
because the absence check reads the raw rows rather than the overridden ones.

### 2.3 No source reports it at all → `UNKNOWN`
Reverse-reconciliation (`wait-times.processor`) writes a CLOSED `queue_data`
row for anything no upstream source has mentioned in 24h, so `detect-seasonal`
has something to read. The write is deliberate; the **status** was not
defensible. It said the operator closed the ride when all that happened is that
our data stopped arriving.

`common/utils/source-absent-status.util.ts` decides this. A ride whose current
rows were *all* written by `system-reconciliation`, in an operating park, reads
`UNKNOWN` and its `queues` are emptied. Requiring *all* rows matters: a ride
still publishing a real STANDBY queue is being observed, whatever sits beside it.

---

## 3. Seasonality: two columns, and only one of them can be trusted

### 3.1 What each column means
- **`is_seasonal`** — "this was closed on ≥7 park-open days while the park ran".
  Observationally true regardless of how long we have been watching.
- **`season_months`** — 1-based months, "when it actually runs". This is the one
  that has repeatedly lied.

`isCurrentlyInSeason` is `null` unless *both* exist, so a client cannot tell
"out of season" from "we don't know". That gap is real and tracked in `todo.md`.

### 3.2 The observation-window artefact
`detect-seasonal` derived months from "every month we ever saw it OPERATING".
With less than a year of history that is not a season — it is **the span in
which we happened to be recording**.

On 2026-08-15 the entire `queue_data` history was **234 days** (from
2025-12-24), and every stored list was a contiguous run anchored at the start of
that window:

```
[1, 2, 3, 4, 12]        69 attractions
[1, 2, 3, 4, 5, 6, 12]  64
[4]                     22
[1, 12]                 18
```

Phantasialand's **Avoras** made it undeniable: a climbing course the park
advertises as open *"ganzjährig im Sommer wie auch im Wintertraum"*, reported
out of season all summer. Its neighbours *Berliner Eislaufen* and *Ice skate
hire* share the identical first-seen date and derived months — and for them
`[1,12]` is correct. **The data alone cannot separate the two cases.**

**The guard:** months are derived only for entities watched at least
`MIN_OBSERVED_DAYS` (330). The span is measured over **all** rows, not the
OPERATING ones — a genuine winter attraction only operates ~40 days, which says
nothing about how long we have looked at it. Under-observed entities are
*excluded from the month query* rather than skipped, so the job writes NULL and
clears artefacts on every run. Applies to **shows** too (1096 of them carried
the same artefact; that table had gone unexamined because every query in the
investigation was attractions-only).

All 340 attraction and 954 show month lists were cleared on 2026-08-15.
`is_seasonal` was kept. Months become derivable again from roughly
**2026-11-19** (2025-12-24 + 330 days).

### 3.3 Free-flow attractions are not seasonal
A playground's feed says CLOSED every day, which is this detector's exact
signature for a season. Three of Phantasialand's four free-flow attractions
were marked seasonal with no months. Both candidate searches now skip
`open_with_park`, and the job clears rows it previously mislabelled.

---

## 4. Ownership: two writers, never one cell

The recurring fix. A sync owns its column; a human owns theirs; neither writes
the other's. Read the curated value first, fall back to the synced one.

| synced cell | curated cell | why |
|---|---|---|
| `may_get_wet` | `curated_may_get_wet` | the wiki populates it for a few dozen of ~7000 and is occasionally wrong |
| `minimum_height` | `curated_minimum_height` | the wiki conflates "you must be this tall" with "below this you need an adult" |
| `stats` | `curated_stats` (ride profiles) | same rule, established first |

`curated_minimum_height` is always centimetres. `null` = nothing to correct;
**`0` = no minimum height** — barely a sentinel, since a 0 cm minimum excludes
nobody, and the only way a correction can override an upstream number with
*nothing*. Resolve both through `resolveCuratedFacts`
(`attractions/utils/curated-attraction-facts.util.ts`), never inline: the `??`
expressions had already been copied into both DTO mappers.

**The same boundary applies to `season_months` on a free-flow row.** The
detector cannot derive months for something it never sees OPERATING, so months
there are human-written. `detect-seasonal` therefore only clears free-flow rows
whose months are already NULL, and its recently-operating reset skips
`open_with_park` entirely — Avoras emitted 154 OPERATING records while
free-flow, which would otherwise have wiped a curated season.

---

## 5. Incidents worth remembering

### 5.1 `detect-seasonal` was dead for 73 days
`9046535` (2026-06-03) refactored the zero-history query into a CTE chain and
dropped the leading `WITH`. Postgres threw `42601`; the handler died *after*
step 3 computed its candidates but *before* the UPDATE that persists them. Every
run found 262 candidates and wrote none.

Two lessons:
- **A SQL string in a template literal compiles and lints exactly as well broken
  as whole.** Hand any modified job SQL to Postgres as `EXPLAIN` before shipping.
- A failure inside a Bull processor surfaces nowhere. It was found only by
  triggering the job by hand to verify something unrelated.

### 5.2 ThemeParks.wiki dropped whole clusters of attractions
Ten parks lost a block of attractions from the wiki's **live** feed on a single
day each:

| park | rides | date |
|---|---|---|
| Europa-Park | 44 | 2026-06-07 |
| Rulantica | 18 | 2026-06-07 |
| Universal Studios Singapore | 17 | 2026-04-25 |
| Wet'n'Wild ×2 | 13 + 13 | 2026-06-29 |
| Busch Gardens Tampa | 9 | 2026-06-13 |
| Ocean Park | 7 | 2026-06-30 |

**Every affected ride lacks a `queue_times_entity_id`** — the dual-sourced ones
kept working. That is both the tell and the hint at a mitigation: broaden
Queue-Times matching for these parks. Ten weeks later Europa-Park's page still
showed a Ball Pool and a London Bus as closed, in August. §2.3 is the fix for
the symptom; the cause is still open in `todo.md`.

---

## 6. Diagnostic SQL

**Is a "closure" ours or the operator's?**
```sql
SELECT data_source, status, count(*), max(timestamp)::date
  FROM queue_data
 WHERE "attractionId" = '<id>' AND timestamp > now() - interval '3 days'
 GROUP BY 1,2;
```
`system-reconciliation` means no source is reporting it.

**Which parks have a silenced cluster?**
```sql
WITH last_op AS (
  SELECT "attractionId",
         max(timestamp) FILTER (WHERE status='OPERATING') AS last_op,
         max(timestamp) AS last_row
    FROM queue_data WHERE timestamp > now() - interval '120 days' GROUP BY 1
)
SELECT p.name, count(*), min(l.last_op)::date, max(l.last_op)::date
  FROM last_op l
  JOIN attractions a ON a.id = l."attractionId"
  JOIN parks p ON p.id = a."parkId"
 WHERE l.last_op < now() - interval '30 days'
   AND l.last_row > now() - interval '2 days'
 GROUP BY p.name HAVING count(*) >= 6 ORDER BY 2 DESC;
```
Identical min/max dates = a feed event, not N independent closures.

**Are month lists artefacts?**
```sql
SELECT season_months::text, count(*) FROM attractions
 WHERE season_months IS NOT NULL GROUP BY 1 ORDER BY 2 DESC;
```
Contiguous runs anchored at the start of your history are the tell.

**How deep is our history at all?**
```sql
SELECT min(timestamp)::date, max(timestamp)::date FROM queue_data;
```

---

## 7. Curating a free-flow attraction

1. **Research it against the operator's own pages** — never from model
   knowledge. A name pattern is not evidence: Toverland's "Kletterparcours"
   looked like a climbing net and was a harnessed high-ropes course with a
   140 cm minimum, demolished after 2026-11-02.
2. Confirm it has no queue, no ride vehicle, no separate ticket.
3. Establish **seasonality**. If it is seasonal and its park is open year-round,
   it needs `season_months` before the flag is safe — otherwise a snow
   playground reads open in July.
4. `UPDATE attractions SET open_with_park = true WHERE ...`
5. Evict the park's `park:integrated:<parkId>` Redis key. A single key, not a
   global flush — cold rebuilds have saturated the DB before.

Currently flagged: 19 attractions across 11 parks. Held deliberately, pending
season-date research: Europa-Park's two water playgrounds, Everland's snow
playground, Bellewaerde's Christmas playground. See `todo.md`.
