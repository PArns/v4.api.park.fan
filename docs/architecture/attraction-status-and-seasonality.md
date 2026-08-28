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

### 2.4 No row inside the window → the optimistic fallback
The park's ride list has one more branch, and it is the only one that invents
an answer: an open park whose ride has no current row at all serves that ride as
`OPERATING` (`common/utils/no-live-data-status.util.ts`). It exists so a feed
that goes quiet mid-day does not produce the "Park geöffnet, alle Bahnen zu"
page, and a ride the season has ruled out (`isCurrentlyInSeason === false`)
never takes it.

**What "no current row" means is a query decision, and it used to be wrong.**
`findCurrentStatusByPark` fetches the latest row per attraction *since today's
opening time*. But a `queue_data` row is only written when a value changes, plus
an hourly heartbeat — so a ride that has been shut for months carries a reading
timestamped **before** the gates opened, and cutting the window at the opening
time threw exactly that reading away. Phantasialand opens at 09:00 and its
source is polled roughly hourly: from 09:00 until the poll landed at 09:23, not
one of its 40 attractions had a row inside the window, and the whole park was
served as running. Among them *Berliner Eislaufen* and *Ice skate hire*, in
August, while ThemeParks.wiki had said `CLOSED` for both since April. A
statically rendered page that happens to be built inside those 23 minutes keeps
the invented answer for as long as it is cached.

Today's opening time may therefore only **widen** that window, never narrow it:
the cutoff is the earlier of the opening time and the `maxAgeMinutes` fallback
(6 h). The last word the feed said is the answer to serve; the fallback is for
silence, not for "unchanged".

**Still open:** a ride no source has *ever* reported still reads `OPERATING`
rather than `UNKNOWN` — Disneyland's *Main Street Pumpkin Festival* has no
`queue_data` row at all and is served as running in August, and the four
wait-time-less parks (Six Flags Great Escape, La Ronde, Hurricane Harbor
Arlington, Water Country USA) report every one of their rides open the same way.
That is §1's rule broken in the other direction, and it is tracked in
`todo.md`.

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

## 4a. Identity: the externalId, never the slug

A slug is a **frozen name**. When a ride is renamed the sync updates `name` and
deliberately leaves `slug` alone, so the public URL survives — 281 rows carry a
slug that no longer matches their name, and that is the system working.

So a slug can look incriminating and be nothing of the sort. On 2026-08-17 three
rows had names that did not match their slugs (`wahoo-racer-twisted-whizzard`
carrying "Wahoo Racer"), and the slugs were treated as the truth. Research
proved that **no "Twisted Whizzard" and no "Discovery Bay - Treehouse" have ever
existed** — two park indexes, two sitemaps, 650KB of raw pages and the Wayback
CDX index, zero hits. Trusting the slugs would have invented two attractions.

**Resolve identity from `externalId` against the upstream entity.** For a wiki
UUID: `GET https://api.themeparks.wiki/v1/entity/<externalId>` returns the name
that row is entitled to.

### The one signature that does mean damage

Not "slug does not match name" — that is 281 rows of normal history. The
damaging shape is narrower:

> **This row carries the name that another row's slug says it should have.**

```sql
WITH n AS (
  SELECT a.id, a."parkId", a.name, a.slug, p.name AS park,
         lower(regexp_replace(a.name, '[^A-Za-z0-9]', '', 'g')) AS name_norm,
         replace(regexp_replace(a.slug, '-\d+$', ''), '-', '')  AS slug_norm
    FROM attractions a JOIN parks p ON p.id = a."parkId"
   WHERE a.retired_at IS NULL AND a."externalId" NOT LIKE 'qt-ride-%'
)
SELECT a.park, a.name, a.slug AS its_slug, b.slug AS name_belongs_to
  FROM n a JOIN n b ON b."parkId" = a."parkId" AND b.id <> a.id
 WHERE a.name_norm = b.slug_norm AND a.slug_norm <> a.name_norm;
```

Four rows matched, in three parks. The cause was `findExistingAttraction`'s name
fallback: when an upstream entity is **renamed**, its new name stops matching its
own row and matches the neighbour that currently holds that name — handing one
ride's row to another. The fallback now refuses rows that already answer to
another id from the same source; see `attraction-match.util.ts`.

---

## 4b. Review marks: what a human already settled

Duplicate detection and retirement detection are **behavioural**. They describe
what the feed is doing, not what is true, so every candidate they surface comes
back tomorrow however carefully it was investigated.

`attraction_review_marks` is where a verdict lives:

| kind | lifetime | example |
|---|---|---|
| `not_a_duplicate` | permanent | Cedar Creek is a lazy river beside Cedar Creek Mine Ride, a coaster |
| `not_retired` | optional `recheck_after` | Shock Wave has stood unused since March 2026 with no announcement either way |

Set `recheck_after` whenever the answer can change. A permanent mark on Shock
Wave would hide its eventual retirement forever; a permanent mark on Marvel Cave
is right, because a landmark that plainly operates will not quietly stop.

Pairs are stored in canonical id order behind a CHECK constraint: a pair fact has
no direction, and storing both ways is how the detector once counted 63 rows for
53 real pairs.

`detect-seasonal` and reverse-reconciliation deliberately do **not** consult
marks. They describe the feed; a human's verdict does not change that.

### What research is worth

Of 73 attractions whose feed went quiet, **16 were genuinely gone**. The rest:
23 still operating, 5 mid-refurbishment with reopening dates, 5 seasonal, 5 not
attractions at all (a land, a station, a show, a workshop), 2 renamed, the
remainder unresolved. *Jurassic Park - The Ride* runs under an anniversary
overlay name; *Marvel Cave* is central to a 2027 project. Acting on the feed
signal alone would have deleted 57 existing rides.

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
kept working. Ten weeks later Europa-Park's page still showed a Ball Pool and a
London Bus as closed, in August. §2.3 is the fix for the symptom.

**There is no other source to switch to.** All three live sources were checked
directly on 2026-08-15:

| source | Europa-Park | the silenced rides |
|---|---|---|
| ThemeParks.wiki | entity still exists, correct `parentId` | `liveData: []` |
| Queue-Times (park 51) | 39 rides | absent |
| wartezeiten.app (`europapark`) | 36 entries | absent |

Queue-Times and wartezeiten.app publish only the marquee rides. The wiki was the
only source that ever carried the rest, and it stopped.

**What was actually lost is much smaller than 59 rides.** Of Europa-Park's 59
silenced attractions, only **four** ever recorded a wait above zero — the EP
Express stations, up to 65 minutes. The other 55 were walk-on for their entire
recorded history: carousels, monorails, panorama trains, boat rides,
playgrounds, the Christmas market. They lost a *status*, not a wait time.

That reframes the fix. For the four Express stations this is a genuine gap with
no remedy available. For the other 55 the answer is not a new source but
**curation**: many are free-flow and belong under `open_with_park` (§2.2) —
Adventure Playground and Würmchen Wies'n Playground were flagged on 2026-08-15
and read OPERATING again immediately, and Lítill Island and Water Playground are
held only pending season dates. A carousel with a zero wait is *not*
automatically free-flow though — it has an operator and can be closed — so each
needs researching individually (§7).

---

### 5.3 A map number in a ride name hid eight duplicates

Queue-Times publishes some parks' own map numbers inside the ride name —
Energylandia's feed says `Draken (155)` where the wiki says `Draken`. The park
held **138 rows for 46 rides**. Stripping a trailing `(number)` before comparing
names collapsed them to 97, with heights and RCDB links intact.

Two traps: the strip is limited to **three digits**, because
`HAUNTED HOUSE: Texas Chainsaw Massacre (2022)` is a year and a 2022 maze is not
the 2023 one. And a shared Queue-Times id **finds** a pair but no longer
**decides** it — Carowinds runs two slide complexes under one id.

### 5.4 One source naming two things alike

ThemeParks.wiki publishes three separate attraction entities at Heide Park all
called `PLAYGROUND`. Their names agree perfectly, so name matching offered them
as *safe* auto-merges. Two ids from the **same** source are that source saying
these are two things; auto-merge now requires a pair to span two sources.

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
