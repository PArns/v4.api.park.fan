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

**What was actually lost is much smaller than 59 rides, and the 59 was never
the casualty list.** It is the *population* — every active Europa-Park ride with
no `queue_times_entity_id`, which is what the drop could reach at all. Split on
2026-09-09 against production, it accounts for itself exactly:

| count | what these are |
|---|---|
| **45** | went quiet and stayed quiet: >100 OPERATING rows before 2026-06-07 13:18Z, none since. This is the sweep below. |
| **4** | the EP Express stations — **still reporting OPERATING**, 11 500–12 400 rows each since the drop. |
| **10** | never reported OPERATING in their whole history, before or after; every row they have is CLOSED. Eight are winter-only (the two Christmas markets, the ice rink, Children's carousel Winterland, Skitty World Nordic, Day maze 'Niflheim', FIS Snowkidz, Winter World of Wonder). *Vintage Cars* and *Yomi Adventure Trail* are not, and the operator lists both as running — a separate gap, tracked in `todo.md`. |

```sql
-- the 45. The 13:18Z / 14:00Z pair straddles the drop; the >100 floor is what
-- makes "went quiet" mean a ride that was being reported, not one we barely saw.
-- `rows_after` is every row of any status, and it is meant to be large: §2.3's
-- reconciliation keeps writing CLOSED. What defines the set is the second HAVING
-- clause, "no OPERATING row since" — not this column.
-- `GROUP BY a.name` is safe HERE and not in general: all 96 active Europa-Park
-- attractions have distinct names. §5.4 is a park where they do not.
WITH ep AS (SELECT id FROM parks WHERE slug = 'europa-park' AND "citySlug" = 'rust')
SELECT a.name,
  count(*) FILTER (WHERE q.status = 'OPERATING' AND q.timestamp <  '2026-06-07 13:18Z') AS op_before,
  count(*) FILTER (WHERE q.timestamp >= '2026-06-07 14:00Z')                            AS rows_after
FROM attractions a
JOIN queue_data q ON q."attractionId" = a.id
WHERE a."parkId" = (SELECT id FROM ep) AND a.retired_at IS NULL
GROUP BY a.name
HAVING count(*) FILTER (WHERE q.status = 'OPERATING' AND q.timestamp <  '2026-06-07 13:18Z') > 100
   AND count(*) FILTER (WHERE q.status = 'OPERATING' AND q.timestamp >= '2026-06-07 14:00Z') = 0
ORDER BY op_before DESC;
```

The **44** in the table above is the feed-side count of what stopped arriving
that day; 45 is what stayed gone. Near-identical lists, different questions.

Drop the `> 100` floor to `> 0` and the same cut returns **46**. The extra row is
*'Bellevue' Ferris Wheel* with 81 OPERATING rows before the drop and none after
— and it is dual-sourced, so it was never in the 59 to begin with. A different
population and a different threshold, which is exactly the pair of confusions
this table exists to end.

**The four Express stations are no longer a gap.** This section called them "a
genuine gap with no remedy available" — measurably not so any more: they are the
only four of the 59 that ever recorded a wait above zero, up to 65 minutes, and
they are also the only four still publishing. Whatever silenced the rest did not
keep them.

That leaves the 45, and for them the answer is not a new source but
**curation**: some are free-flow and belong under `open_with_park` (§2.2). A
carousel with a zero wait is *not* automatically free-flow though — it has an
operator and can be closed — so each needs researching individually (§7).

**The sweep is done, and the estimate that used to stand here — that *many* of
the 45 would turn out free-flow — was too high.** Every one of them was
researched against the operator's own pages:

| verdict | rides |
|---|---|
| free-flow, `open_with_park` set | **10** |
| operated ride, correctly left alone | **34** |
| no operator source to decide on | **1** |

The ten are Adventure Playground, Ball Pool, Casa da Aventura, Limerick Castle,
Little Lamb's Land, Lítill Island, Paul's Playboat, Root Slides, Water
Playground and Würmchen Wies'n Playground. Eight were already flagged;
**Limerick Castle** (audit `d98a7513-2799-42ae-8bf9-1a1d07d1fc54`) and **Paul's
Playboat** (audit `ed34c229-1220-4728-ba51-cc10527374ea`) were written on
2026-09-09.

**Both took no months, deliberately**, and that is a finding rather than an
omission. Most free-flow rows carry no months: of the 32 flagged today, 4 have
months and 28 do not — these two among them. What sets them apart is not the
empty list but that somebody **asked**. Europa-Park lists both under all four of
its seasons — Summer, Halloween, HALLOWinter, Winter — so they run exactly when
the park runs, and §7a has nothing to decide. Of the four rows whose season had
been researched before them, all four needed months, which is what made §7 step 3
read as if months were always the answer. They are not.

(The other 26 month-less rows are not evidence either way. Only eight
`openWithPark` writes exist in `admin_audit_log` at all — two for *Ice skate
hire* on 2026-08-20/21, the four §7a lists just after midnight on 2026-09-09,
and these two — because the sweeps before that followed §7's old advice and
wrote the column with a raw `UPDATE`, which is the reason step 5 now says
otherwise. Audit dates in this document are park-local; those four are
2026-09-08 22:38 UTC.)

The one that could not be decided is **Rocking Bridge & Chute**: it is on
neither the operator's attraction list nor its children's page, and its own page
answers 403. A fan description would make it a playground; §7 step 1 rules that
out as evidence, and its absence from the list may equally mean it no longer
stands — which would be `retired_at`, not the free-flow flag.

**Two traps this sweep hit, worth knowing before the next park:**

- The operator can contradict itself. **Dwarf City** sits under *playgrounds* on
  Europa-Park's children's page while its own detail page gives it decorated
  wagons, a 2:30 ride time and 280 riders/hour. The detail page is the more
  specific claim and wins; a category filter is a navigation aid, not a fact
  about the attraction.
- The name that survives is the wiki's. Two of the 45 (**Rocking Bridge &
  Chute**, **Children's carousel**) do not appear under those names on the
  operator's English list at all, so a research pass that only matches names
  silently drops them.

**What this leaves is not a bug.** Measured the same day with the park
`OPERATING`, the 45 serve as 10 `OPERATING` and 35 `UNKNOWN` — no `CLOSED`
anywhere. §2.3 is doing its job: the 34 operated rides say "we cannot read this",
which is true, and the alternative would be inventing a status for a ride no
source reports.

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
   140 cm minimum, demolished after 2025-11-02.
2. Confirm it has no queue, no ride vehicle, no separate ticket.
3. Establish **seasonality**, and treat both answers as answers. If the park
   keeps running while the area does not, it needs months before the flag is
   safe, or a snow playground reads open in July (§7a). If the operator lists
   the area under every season the park has, it correctly takes **none** — a
   null list is `isInSeason`'s "runs all year" and is the state most free-flow
   rows are in. Europa-Park's Limerick Castle and Paul's Playboat are the worked
   example (§5.2). What is not allowed is a month list invented to be safe: once
   a list exists, `isInSeason` answers purely from it, so every month left out
   is a hard close.
4. **If step 3 produced months**, write them to **`curated_season_months`** and
   set **`curated_is_seasonal = true`** beside them. The pair, not just the
   months. If it produced none, write **neither**: `curated_is_seasonal = true`
   over an empty month list makes `resolveCuratedFacts` report `isSeasonal: true`
   for an area that runs all year, which is §3.3 backwards. Leaving both null is
   right — the resolved answer then falls back to the synced `is_seasonal`, and
   Step 2b clears exactly this shape. `curated_is_seasonal = **false**` is the
   deliberate pin for the one case where that is not enough: a free-flow area the
   detector keeps calling seasonal because the park shut around it.
   `resolveCuratedFacts` does infer `isSeasonal: true` from non-empty curated
   months alone, so the flag looks redundant — it is not. Step 2b of the nightly
   `detect-seasonal` runs
   `UPDATE attractions SET is_seasonal = false WHERE open_with_park AND is_seasonal
   AND season_months IS NULL`, and its `season_months IS NULL` guard reads the
   **synced** column while curation writes the curated one. On 2026-09-09 not one
   free-flow row in the database had a non-NULL `season_months`, so that guard
   protects nothing and Step 2b matches every hand-curated free-flow row. What
   keeps the resolved answer stable through the night is `curated_is_seasonal`.
5. Write it through `PATCH /v1/admin/content/attractions/:id` with a `reason`
   and a `sourceUrl`, **not** with `UPDATE attractions SET open_with_park`. The
   endpoint carries the four-step publish order — write, evict, revalidate,
   revalidate again after the CDN window — and a raw UPDATE carries none of it,
   so the correction lands in the database and not on the page. It also writes
   the audit row that makes the curation reviewable later.

### 7a. Turning an operator's season into `curated_season_months`

`season_months` is months, and a season almost never starts on the 1st. So the
translation loses days no matter which way it is decided, and the only question
is which side to lose them on.

**The test: a month goes in unless the operator's season covers no more than a
tenth of the days the park is open that month.** The open days, not the calendar
days — a month the park sits closed through cannot be got wrong at all.

The denominator is a query rather than a judgement, and this is the whole of it:

```sql
SELECT extract(month FROM s.date)::int AS month, count(DISTINCT s.date) AS open_days
FROM schedule_entries s
WHERE s."parkId" = $1                  -- the id. `parks.name` is sync-owned, not unique,
                                       -- and not even the displayed name (`curated_name`)
  AND s."attractionId" IS NULL         -- park-level rows; attraction ones are never written
  AND s."scheduleType" = 'OPERATING'   -- EXTRA_HOURS and the two event types are not "open to everybody"
  AND s.date >= $2 AND s.date < $3     -- one full season cycle, not a calendar year
GROUP BY month ORDER BY month;
```

`count(DISTINCT s.date)` rather than `count(*)`: a park may publish two rows for
one day, and duplicate `schedule_entries` are a known open item. Counting days
is all this needs — anything that needs open *minutes* must go through
`park-open-window.sql.ts` instead, which also repairs misdated closings and
flattens overlapping windows.

**A month with no rows is ambiguous**, and the query cannot tell you which it
is: the park is shut all month, or the operator has not published that far yet.
Run it over a cycle the operator has already released end to end — a completed
one is safest — and treat a zero in a month you expected to be open as "come
back later", never as "closed through".

Worked, from the 2026-09-09 curation:

| ride | operator's season | month at stake | open days | covered | share | verdict |
|---|---|---|---|---|---|---|
| Europa-Park, Lítill Island | Summer, 28 Mar – 25 Sep | March | 10 (park opens 22 Mar) | 4 | 40 % | **in** |
| Bellewaerde, Snowmen Playground | Christmas, from 28 Nov | November | 8 | 2 | 25 % | **in** |
| Europa-Park, Water Playground | + Halloween to 1 Nov | November | 30 (open daily) | 1 | 3 % | **out** |
| Everland, Snow playground | mid-Dec to about 1 Mar | March | 31 | ~1 | ~3 % | **out**, softly |

**Why the threshold sits low rather than at a majority.** The two errors are not
symmetric. Omitting a month is a *hard* close: once a list exists at all,
`isInSeason` answers purely from it, and the permissive branch that lets a ride
run only fires for a null or empty list. So dropping March would shut
Europa-Park's water playgrounds for every open day from the 28th to the 31st.
Including a month only over-reports on the days between the month's start and
the season's, which at the edges of a season is often a stretch the park is
closed through anyway. At the margin, include.

**Everland's March is the one call in that table that is not safe**, and it is
worth naming rather than burying. Everland's own pages never state a closing
date for the snow park; the ~1 March comes from trade press, and the operator's
newsroom says only "until March 2026". A close on the 5th or the 8th would put
March at 16–26 %, which this rule admits. It was excluded because the errors are
lopsided the other way here: Everland is open all 31 March days, so including
March on a snow area that shut on the 1st reads open for thirty days it is gone,
against roughly a week the other way. Revisit when Everland publishes a date.

**Seasonality resolves as a pair, and the months can stand alone.** Non-empty
`curated_season_months` already make `resolveCuratedFacts` report
`isSeasonal: true` — the months do not need `curated_is_seasonal` beside them to
survive the `seasonMonths = !isSeasonal ? null : …` gate. Setting it anyway is
harmless and pins the value against the nightly detector; setting it to `false`
takes the months down with it, which is the whole point of the pairing.

Flagged as of 2026-09-09, counted after the Europa-Park sweep in §5.2: **32
attractions across 17 parks** — the "19 across 11" that stood here was already
stale, and the "30 across 17" that replaced it that morning was overtaken the
same afternoon. Curated on that date, with the source on each audit row:
Europa-Park's _Lítill Island_ `[3–9]` and _Water Playground_ `[3–10]`,
Everland's _Snow playground_ `[12, 1, 2]`, Bellewaerde's _Snowmen Playground_
`[11, 12, 1]`, and — with **no months, deliberately** — Europa-Park's _Limerick
Castle_ and _Paul's Playboat_. Exactly four of the 32 carry curated months;
everything else runs with its park. Still held for want of any stated operating
window: Peppa Pig's _Muddy Puddles Splash Pad_ and Walibi Rhône-Alpes' two
_Exotic Island_ play areas. See `todo.md`.
