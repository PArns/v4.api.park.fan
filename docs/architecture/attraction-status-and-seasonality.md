# Attraction Status & Seasonality

> **2026-08-15, re-measured 2026-09-11.** Written after an investigation that
> started with two playgrounds reading CLOSED and ended with a large cluster of
> attractions across ten parks doing the same for the wrong reason, a background
> job that had been dead for 73 days, and every `season_months` value in the
> database turning out to describe our recording window rather than a season.
>
> The cluster figure was "~140 attractions across ten parks" and did not survive
> being measured: **125 rides are genuinely silent, 62 of the same cut were
> never silent at all, and 17 were recategorised upstream.** §5.2a has the split
> and the two checks that produce it; every number in §5.2 above it is the older
> reading.

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
row for anything no upstream source has mentioned in 24h. The write is
deliberate; the **status** was not defensible. It said the operator closed the
ride when all that happened is that our data stopped arriving.

That write used to be justified here as giving `detect-seasonal` something to
read, and that justification is gone (PAR-32): the detector now reads observed
rows only, so these rows are no longer seasonal evidence. What the row is still
for is the outage reconstruction, where it **ends** a run instead of joining two
`DOWN` states across the silence, and this read path, which turns it into
`UNKNOWN`.

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

### The park payload's grouping is the thing that hides the damage

The fix stopped new rows being handed over. It did not repair the rows already
handed over, and on **2026-09-20** the same three parks still held them:
`wahoo-racer` named "Typhoon Twister" at Hurricane Harbor Arlington,
`castaway-bay-sky-climb` named "Wally the Walrus" at Sea World, and
`discovery-bay-mini-waves` named "Discovery Bay" at Hurricane Harbor New
Jersey. Each sits in a name group of two, because the row it was handed the
name from is still there as well.

Nobody sees them, because the park payload groups attractions by `name` and
serves one row per name (`deduplicateEntities` in `park-integration.service.ts`).
That is a **coincidence that happens to land right**, not a design: the rows
collide on the name precisely because one was handed the other's.

**Do not upgrade that key to the slug.** It is the obvious repair — a name
collision between two real rides would hide one, and the slug looks like the
discriminator that would save it. Measured over all 210 parks and 7300 rows on
2026-09-20: grouping by name serves 7252 attractions, grouping by name plus slug
base serves 7255. Exactly three groups split, and in each one **the ride the odd
slug names is already in the catalog as its own row**:

| the group | the odd slug | the ride that slug names, already served as |
|---|---|---|
| Arlington, "Typhoon Twister" | `wahoo-racer`, sitting on Typhoon Twister's coordinates | `wahoo-racer-twisted-whizzard`, on Wahoo Racer's own |
| Sea World, "Wally the Walrus" | `castaway-bay-sky-climb`, sitting on Wally the Walrus's | `castaway-bay-sky-fortress`, the Castaway Bay climb |
| New Jersey, "Discovery Bay" | `discovery-bay-mini-waves` | `discovery-bay-treehouse`, carrying "Discovery Bay - Mini Waves" |

So the split recovers nothing. It publishes "Typhoon Twister", "Wally the
Walrus" and "Discovery Bay" twice each — §4a's "trusting the slugs would have
invented two attractions", one layer up. `park-integration.dedupe.spec.ts` pins
the three pairs so the next attempt goes red instead of live (PAR-259).

The three groups are not the same case underneath, and only Arlington and Sea
World are a row sitting on its neighbour's upstream entity. **New Jersey is the
harder one:** its two rows bind exactly, but to two *different* upstream
entities that the feed itself names "Discovery Bay" alike. Whether those two are
one ride, the feed does not say — one of them shares its coordinates with
upstream's "Discovery Bay - Mini Waves", which we also hold separately.

Two of the six rows cannot be bound upstream at all: Arlington's
`typhoon-twister` carries no coordinates, and Sea World's `wally-the-walrus`
matches no upstream entity exactly. **That is a reason not to trust a split, not
evidence for one** — an unbindable row is the state §4a is about.

And the `externalId` is not the waiting upgrade either. Because New Jersey's two
rows answer to two different upstream ids, keying on it would publish "Discovery
Bay" twice as well. Which of these rows are one ride is a curation question
(PAR-160 / PAR-179 / PAR-205), not a question the payload's grouping key can
answer.

### Which row of the group survives, and why the sitemap has to agree

The key is settled above. **Which row the key keeps is a second decision, and it
owns a public URL** — the frontend resolves a ride page from the park payload, so
the row that loses its name group has no page, and `/v1/sitemap/attractions`
listing it advertises a 404.

Until PAR-498 that decision read the live feed: prefer the OPERATING row, then
the row with coordinates. Both move. Measured on **2026-09-26** over all 201
parks in the sitemap, park by park against the running API:

| | |
|---|---|
| sitemap entries | 7,304 |
| sitemap slugs with no row in the park payload | **48** |
| payload slugs not in the sitemap | 0 |
| duplicate name groups / rows / parks | 45 / 93 / 15 |

The 48 were **exactly** the 48 losers of the 45 groups — every group had exactly
one winner, the sitemap's row set was character-identical to the table's, and the
divergence ran one way only. So the gap had a single cause, and the frontend
repo's `docs/seo/analysis.md` item 8 — "~400 hard 404s across 6 locales", and "no
fix is possible on the sitemap side" — is that cause moving: PAR-498's ticket lists
`paultons-park/raven` and `disneyland-park/disneyland-railroad-main-street-station`
as 404 on 2026-09-24, and on 2026-09-26 both were served and their `-2` siblings
were the missing ones. No row had changed in between.

**The rule now reads `slug` and `name` and nothing else**
(`outranksNameDuplicate` in `src/common/utils/name-duplicate.util.ts`), and both
`deduplicateEntities` and `SitemapService.oneRowPerName` call it, so there is one
rule and not two:

1. **The slug without a `-N` counter wins.** A counter means
   `generateUniqueSlug` handed that row the leftover name. Settles 42 of the 45
   groups, and keeps the winner on the slug the sitemap already advertised.
2. **Then the slug `generateSlug(name)` would produce.** Settles the groups where
   every candidate carries a counter (Walibi Holland's "Walibi Express Station 2"
   against `walibi-express-station-2-2`) and the two above where none does — Sea
   World keeps `wally-the-walrus` rather than `castaway-bay-sky-climb`, which is a
   different ride's name.
3. **Then the slug itself, ascending.** Only so the order is total; without it a
   remainder is decided by the order rows arrive in.

Nine of the 45 groups changed winner, seven of them from a `-N` slug onto the
base slug that was 404ing. **Nothing is lost by dropping the status:** the choice
was always between two rows of one name, and a reader had no way to ask for the
other one either way.

**The groups have to match before the winners can.** Both sides therefore build
the key the same way: `resolveAttractionName` — the same function
`resolveCuratedFacts` uses, so a curated name cannot split a group on one side
only — and then `nameDuplicateKey`, which trims it and returns nothing for a
blank name. The payload has always trimmed and always dropped blank-named rows;
the sitemap did neither, so "Raven " and "Raven" were one group to the payload and
two to the sitemap. Five active rows carry a name with trailing whitespace
(2026-09-26, all at Beto Carrero World) and none of them collides with a sibling
today, which is why it stayed invisible.

Two things about the transition, because neither is in the diff:

- **The sitemap is one flat 24 h-cached list**, so a change to what it contains
  needs a bump of `CacheKeys.sitemapAttractions()`. Without it the old list is
  served for up to a day after the deploy and the measurement above reproduces the
  old number.
- **The park payload cache is not versioned** (`park:integrated:<id>`, TTL three
  minutes for an OPERATING park and up to about six hours for a closed one). So
  right after a deploy the fresh sitemap can name the new winner while a cached
  payload still serves the old one. For the nine groups whose winner changed that
  is the state those URLs were already in, not a new break, and it clears itself
  within one TTL — busting every park payload to shorten it would cold-start 201
  rebuilds for nine URLs.

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

> **Re-measured against production on 2026-09-11 (PAR-38). Half of the opening
> claim below did not survive it.** "A cluster went quiet" turned out to be
> three different events wearing one symptom, and no single column tells them
> apart. Read **§5.2a**, at the end of this section, before using any number
> from the paragraphs that follow.

The reading of 2026-08-15 — parks that lost a block of attractions from the
wiki's **live** feed on a single day each. It was written up as ten; the table
names seven, in six rows:

| park | rides | date |
|---|---|---|
| Europa-Park | 44 | 2026-06-07 |
| Rulantica | 18 | 2026-06-07 |
| Universal Studios Singapore | 17 | 2026-04-25 |
| Wet'n'Wild ×2 | 13 + 13 | 2026-06-29 |
| Busch Gardens Tampa | 9 | 2026-06-13 |
| Ocean Park | 7 | 2026-06-30 |

It also said **every affected ride lacks a `queue_times_entity_id`** — the
dual-sourced ones kept working. That is false as a rule: of the 204 rides the
widened cut in §5.2a returns, **84 carry one**, among them eight of the nine
at
Knott's Berry Farm, which are as silent as the rest. Busch Gardens Tampa is the
sharper counter-example, from outside that set: **all nine** of its rides carry
a Queue-Times id and all nine went quiet anyway on 2026-06-13. They are not
among the 204 because they came back — §5.2a. A Queue-Times mapping is not armour —
where the wiki dropped a non-ride facility, Queue-Times usually dropped it too.
What the sentence described correctly was Europa-Park, where it was measured.

Ten weeks later Europa-Park's page still showed a Ball Pool and a London Bus as
closed, in August. §2.3 is the fix for the symptom.

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
they are also the only four still reporting OPERATING. (The other ten still
publish rows — they just never say anything but CLOSED.) Whatever silenced the
rest did not keep them.

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

#### 5.2a Three events, one symptom — measured 2026-09-11

The §6 query answers "which rides stopped saying OPERATING". That is not the
same question as "which rides did we lose", and the whole of this section's
early confusion lives in the gap between them.

Re-run on 2026-09-11 **over 270 days instead of 120**, with the `HAVING` floor
at 5 rides instead of 6 and grouped by `parks.id`, it returns **16 park rows
and 204 rides** — 15 real parks, since §5.5's pair is one water park entered
twice and its 13 slides are counted twice with it. 270 days reaches the whole
of `queue_data` retention (oldest row 2025-12-24), and the window is where most
of the difference from the
2026-08-15 reading comes from, not the four weeks between the two runs. The
reason is worth keeping: **a ride whose last OPERATING row falls outside the
window has no `last_op` at all, so it drops out of the result entirely instead
of showing up as silent.** The same query run both ways on 2026-09-11 says it
plainly:

| window | what comes back |
|---|---|
| 120 days, `>= 6` | 8 rows: Europa-Park **44**, Rulantica 18, Wet'n'Wild 13 + 13, Mid-America **9**, Traumatica 7, USJ 6, Ocean Park 6 |
| 270 days, `>= 5` | 16 rows, 204 rides: Europa-Park 46, Rulantica 18, USS 17, Fiesta Texas 15, Gröna Lund 14, Mid-America 13, Wet'n'Wild 13 + 13, Knott's 9, Bellewaerde 8, USJ 8, Traumatica 7, Cinecittà World 6, Ocean Park 6, Everland 6, Hollywood 5 |

The eight the shorter window cannot see are USS (last OPERATING 2026-04-25),
Fiesta Texas (04-12), Knott's (04-14), Gröna Lund (01-05), Bellewaerde (02-11),
Cinecittà World (01-19), Everland (02-07) and Hollywood, which the old `>= 6`
floor excluded rather than the window.

**Eight of the sixteen are invisible at 120 days** — including every one whose
drop is older than that, which is to say the very cases the cut exists to find.

**And while we are counting Europa-Park, it carries three different numbers in
this section, all correct.** They are not versions of one figure:

| | what it counts |
|---|---|
| **44** | entities that stopped arriving in the feed on 2026-06-07 — the feed-side event |
| **45** | the sweep population above: rides with >100 OPERATING rows before the drop and none since |
| **46** | today's 270-day cut, which is the sweep's own `> 0` variant: the 45 plus the dual-sourced *'Bellevue' Ferris Wheel* (81 OPERATING rows before, none after) |

The 120-day run returns 44 as well, by dropping *Children's carousel* (last
OPERATING 2026-04-14) and the Ferris wheel (2026-01-18) out of the 46 — and
since the carousel went quiet eight weeks before 2026-06-07, it was never part
of the feed event either. The two 44s are very probably the same rides. They
are still answers to different questions, and the feed-side list was never
written down, so this is a likelihood and not a proof. The sweep's verdicts
cover the 45; the Ferris wheel has none, and correctly so — it is dual-sourced
and was never part of the population this section is about.

Two follow-up checks turn that count into an answer, and **they are not
interchangeable**:

1. **`data_source` over the last three days, restricted to the silent rides.**
   If anything other than `system-reconciliation` is still writing, the feed is
   fine and the ride is merely closed.
2. **`GET /v1/entity/{id}` upstream.** Does the entity still exist, under which
   `entityType`, and does the park's `/children` and `/live` still list it?

Check 1 has to be decided **per ride** and only then counted per park — five of
the sixteen (Mid-America, Bellewaerde, Cinecittà World, Everland, Universal
Studios Japan) have rides on both sides of it, and a
`GROUP BY (park, data_source)` would show them twice without saying which rides
went where:

The predicate is `observedReadingsSql()` from `closure-gap.sql.ts`, not a bare
`data_source <> 'system-reconciliation'`: a heartbeat carries the previous row's
`data_source` forward, so the loose form would read a feed that stopped
yesterday as one that is still writing. (Both forms happen to return the same
sixteen-park split here — checked — but only because none of these rides is
being carried.)

`$1` is the `silent_set` column of the §6 cluster query (unnest it across the
rows you care about, or pass one park's array). Run check 1 **straight after**
that query and every ride in it has a row inside the three days, since the set
was built with `last_row > now() - 2 days`. Run it later and that stops being true: a ride that has since been
retired gets no reverse-reconciliation rows either, falls out of the `src` CTE,
and is then counted in neither column with nothing in the output to say so. If
the two are not run together, drive the join from `unnest($1)` with a `LEFT
JOIN` so a missing ride shows up as a third category instead of disappearing.

```sql
-- check 1: does anything other than reconciliation still OBSERVE this ride?
WITH src AS (
  SELECT q."attractionId",
         COALESCE(bool_or(COALESCE(q.data_source, '') NOT IN
                   ('system-reconciliation', 'system-heartbeat')
                 AND NOT COALESCE(q.is_heartbeat,
                                  q."lastUpdated" = q.timestamp)), false) AS still_reported
    FROM queue_data q
   WHERE q."attractionId" = ANY($1::uuid[])          -- the silent set
     AND q.timestamp > now() - interval '3 days'
   GROUP BY 1
)
SELECT p.id, p.name,
       count(*) FILTER (WHERE NOT src.still_reported) AS really_silent,
       count(*) FILTER (WHERE     src.still_reported) AS merely_closed
  FROM src
  JOIN attractions a ON a.id = src."attractionId"
  JOIN parks p ON p.id = a."parkId"
 GROUP BY p.id, p.name ORDER BY 3 DESC;
```

| group | what it is | parks · rides |
|---|---|---|
| **A · recategorised upstream** | same entity id, `entityType` changed, live data still flowing — into a *different table* | Universal Studios Singapore 17 |
| **B · genuinely silent** | nothing reports them, and where there is an entity to look up, upstream still carries it | Europa-Park 46, Rulantica 18, Fiesta Texas 15, Gröna Lund 14, Knott's 9, Bellewaerde 6, Hollywood 5, Cinecittà World 4, Everland 4, Mid-America 3, USJ 1 — **125** |
| **C · not silent at all, just closed** | the wiki or Queue-Times keeps writing CLOSED, newest row today | Wet'n'Wild 13, Wet'n'Wild Gold Coast 13, Mid-America 10, Traumatica 7, USJ 7, Ocean Park 6, Bellewaerde 2, Cinecittà World 2, Everland 2 — **62** |

**Check 1 alone cannot tell A from B, and that is the trap worth naming.** It
separates C from everything else and nothing more. Group A's `attractions` rows
look exactly like group B's — only `system-reconciliation`, 1,190 rows in three
days — because their live data no longer arrives in `queue_data` at all. It
arrives in `show_live_data`, against a `shows` row that shares nothing with the
attraction but the upstream `externalId`. Only check 2 sees that.

**Group C is the correction that matters**, because three of the seven parks
the 2026-08-15 table names in its six rows live there: both Wet'n'Wild rows,
which are in the southern winter, and Ocean Park, whose six are reported CLOSED
by the wiki every few minutes. Traumatica — Europa-Park's Halloween event,
which does not open until autumn — is the same shape and was simply never in
that table. Nothing dropped in any of them; a 30-day "no OPERATING" cut cannot
tell a lost ride from a shut one.

**Busch Gardens Tampa is a fourth case and is over.** Its nine went quiet on
2026-06-13 and **came back by themselves on 2026-08-17** — a 65-day gap, all
nine dual-sourced. It is not in the 204 at all, because a ride that reports
OPERATING again no longer satisfies `last_op < now() - 30 days`. Worth knowing
before the next cluster is treated as permanent.

**Group A is solved upstream and broken here.** On 2026-04-25 the wiki moved 17
Universal Studios Singapore entities from `ATTRACTION` to `SHOW` — meet &
greets and character sets, which is what they are. The ids never changed, and
`GET /v1/entity/{park}/live` carries all 17 today (5 OPERATING, 12 CLOSED). The
sync followed the change and created 17 **shows**, which receive 4,493
`show_live_data` rows in three days. What it did not do is retire the
`attractions` row: all 17 externalIds now exist **twice**, once alive as a show
and once dead as a ride that `system-reconciliation` keeps marking CLOSED.
Re-matching by id is therefore not the remedy — the match already happened, and
the leftover is a data-repair job (PAR-159: retire the row whose `entityType`
moved, and clean up these 17).

**Group B is neither "recategorised" nor "removed" — it is a third thing.** 106
of the 125 have a wiki entity; every one of those was looked up in its park's
`/children` and `/live`, and 8 of the 41 that are missing from `/children` were
fetched individually as `GET /v1/entity/{id}` as well. All eight came back
intact: `entityType` still `ATTRACTION`, `parentId` still the right park. What
differs is whether the park's own index still lists them:

| park | in `/children` | in `/live` |
|---|---|---|
| Europa-Park | 44 of 46 | 0 |
| Rulantica | 18 of 18 | 0 |
| Mid-America Parks | 3 of 3 | 0 |
| Knott's Berry Farm | 0 of 9 | 0 |
| Six Flags Fiesta Texas | 0 of 15 | 0 |
| Bellewaerde | 0 of 6 | 0 |
| Universal Studios Hollywood | 0 of 5 | 0 |
| Everland | 0 of 4 | 0 |

The other 19 have no wiki entity to look up — Gröna Lund's 14 and Cinecittà
World's 4 are Queue-Times-only parks, and so is the last one: *Sesame Street
4-D Movie Magic™* at
Universal Studios Japan is a Queue-Times-only ride (`qt-ride-12083`), and
Queue-Times **does** publish it — with the same shape of mapping as its
neighbour *Shrek's 4-D Adventure™*, which gets real `queue-times` rows while
this one gets only reconciliation. That is its own defect (PAR-161), not a
dropped cluster.

So there are two shapes inside group B. Europa-Park (44 of 46), Rulantica and
Mid-America are still listed as children and only lost their live rows;
Knott's, Fiesta Texas, Bellewaerde, Hollywood and Everland fell out of the
children index as well — *while the entity document kept working*. Europa-Park's remaining two, *Children's
carousel* and the *'Bellevue' Ferris Wheel*, sit in the second shape. `GET
/v1/entity/6e2fd5cd-959a-4fc2-92e4-f8170fe320f7` returns Knott's *Games and
Arcade* in full, `parentId` correct; Knott's own `/children` (134 entries) does
not contain it. **A re-match by id has nothing to re-match to** in either
shape: the id we hold is the id upstream still publishes.

What the two shapes have in common is subject matter, and one theme dominates:
**an event ends and its attractions leave the feed with it.** Fiesta Texas's 15
are the entire Fright Fest maze line-up. Bellewaerde's six are Santa's Balloons,
Santa's Kitchen, Santa's Candyshop, Rudolph's Ride, Winter Express and the Aztec
Roller, last seen 2026-02-11. Cinecittà World's four are the Casa di Babbo
Natale, the Nevicata di Natale, the ice rink and a Christmas face-painting
stall, last seen 2026-01-19. Everland's four are Snowyard, a snow playground and
both Snow Buster courses, gone since February. Gröna Lund's 14 — a summer park
in Stockholm — all stop on the same day, 2026-01-05, and include most of its
haunted-house line-up. None of them is a lost ride; all of them are a season
that closed and took its entities out of the index.
Knott's nine are two arcades, a blacksmith, a livery stable, two museums, a
schoolhouse, Independence Hall and a gold panning trough — facilities, not
rides, and Queue-Times dropped the same eight of them it once published (park
61 returns 46 rides today, none of these among them).
Knott's nine and Hollywood's five are the exception to the theme — facilities
and limited-run walkthroughs rather than a season. Only Europa-Park, Rulantica
and Mid-America Parks lost *operating rides* — and Mid-America's three are the
sharpest of the lot: *MR. FREEZE: Reverse Blast*, *JUSTICE LEAGUE* and *THE
JOKER: Carnival of Chaos* — a launch coaster, a dark ride and a flat ride — are
operating attractions at an open park, listed as children upstream and absent
from its live feed. (Universal Studios Japan's one
is the Queue-Times defect above, not a drop.) That is why the sweep above found
work to do at Europa-Park and would find much less at most of the others.

**The practical consequence for anyone reading a cluster count:** the number on
its own says nothing, and neither does check 1 on its own. Both have to run
before a "silenced cluster" is a finding rather than a symptom.

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

### 5.5 Two rows for one water park, invisible to every name-led branch

`Wet'n'Wild` (wiki `ee018a72-…`, created 2026-04-08) and `Wet 'n' Wild Gold
Coast` (`qt-park-146`, created 2026-06-22) are the same park in Oxenford,
Queensland. They carry **the same latitude and longitude to seven decimals**
(-27.9149499, 153.3167716), the same city and the same `park_type`, and hold 13
attractions each — the same 13 slides, reported by the wiki under one row and by
Queue-Times under the other.

`GET /v1/admin/duplicate-parks` returned `{"total":0,"pairs":[]}`. Every branch
of `ParkValidatorService.findDuplicates` required a name similarity of at least
0.85, and this pair scores **0.6923**: `calculateStringSimilarity` strips
whitespace, so `Wet'n'Wild` against `Wet 'n' Wild` would be a perfect 1.0, and
it is the nine characters of `GoldCoast` — eight bigrams the other name cannot
match — that dropped it below every threshold.
Geography never got a vote of its own: `geoProximity` only ever appeared in a
conjunction with a name score.

The lesson is the mirror image of §5.4. There, two identical names had to be
kept apart because they came from one source; here, two rows that agree on
*every* physical fact are kept apart because their names disagree. **A distance
of 0.000 km between two parks is a stronger statement than any string
comparison**, and the detector had no way to say so.

**Fixed in PAR-160** by a fifth branch, `sharedPoint`, which lets the physical
facts lead: coordinates under `SHARED_POINT_KM`, sources disjoint, and a name
score over `SHARED_POINT_NAME_SIMILARITY` — a floor rather than a verdict. Three
conditions, of which two are numbers, and both numbers were placed against the
whole catalogue (213 parks, 22 578 pairs) rather than chosen, because
`findDuplicates` is what `POST merge-duplicate-parks` acts on. Since PAR-247 a
false positive there is reported rather than merged (§5.5a), but the numbers
still decide what an operator is shown, so they stay measured.
What that measurement says, and what it constrains:

| km | name | pair |
| -- | -- | -- |
| 0.0000 | 0.1600 | Caribe Aquatic Park ↔ Ferrari Land (Vila-seca) |
| 0.0000 | 0.2000 | Caribe Aquatic Park ↔ PortAventura Park |
| 0.0000 | 0.1600 | Ferrari Land ↔ PortAventura Park |
| 0.0000 | **0.6923** | **Wet 'n' Wild Gold Coast ↔ Wet'n'Wild** |
| 0.0424 | 0.6122 | Hurricane Harbor Chicago ↔ Six Flags Hurricane Harbor, Rockford |

- **The radius is 0.01 km, not the 0.05 the ticket proposed.** At 0.05 the
  Rockford row comes with it — a real park 110 km away whose geocode says
  Gurnee, and whose sources are disjoint too, so at 0.05 km that pair would
  have rested on the name floor alone. Between 0.0000 and 0.0424 the catalogue
  is empty.
- **`0, 0` is not a point.** Null Island is a failed geocode, and two rows that
  both failed are 0.0000 km apart on no location information at all;
  `usableCoordinate` refuses it, as `source-id-inheritance.util.ts` already
  did. The same helper coerces the `decimal` columns (Postgres returns them as
  strings) and stops treating a park on the prime meridian as unlocated — that
  last part only ever bit on numeric input, because Postgres hands back
  `"0.0000000"` and that string is truthy.

  For the same reason this **narrows the four name-led branches**, deliberately:
  two failed geocodes used to pass the truthiness check, `geoProximity` read
  them as 0 km apart, and `geoProximity && nameSimilarity >= 0.85` could fire on
  rows carrying no location information. It no longer can. A real ghost pair
  stays reachable through `sameCity` and through
  `nameSimilarity >= 0.95 && sharedEntityId`, neither of which asks about
  geometry, and a spec case pins that so a future hoist of the refusal cannot
  take ghost detection with it.
- **Disjoint sources is the §5.4 rule one level up:** Queue-Times carries 19 for
  PortAventura Park and 277 for Ferrari Land, which is that source saying it
  knows two parks on this geocode. The test reads what a row *is* (which source
  columns it fills), never when it was last heard from.

  It is worth being exact about what it does and does not do. At the current
  floor it does **not** refuse the PortAventura rows — their names score
  0.1600–0.2000 against 0.65, so the floor refuses all three pairs on its own,
  and disabling the disjointness condition entirely leaves that case green
  (five other cases go red). It is the condition that would carry them if the
  floor were ever lowered, and the one that does the work on any pair whose
  name clears the floor.
- **The name floor is 0.65 and cannot carry more than it does.** A water park
  beside its theme park scores at or above the pair we must catch — Legoland
  Windsor against its water park 0.7429, Alton Towers against its waterpark
  0.6923. No threshold separates that class. (A row against its own `… Resort`
  spelling, such as Heide Park at 0.7273, is not this class at all: that is the
  Wet'n'Wild shape, one place under two names, and merging it would be right.) (Another park of the same brand is a different case and the floor does
  separate it: `Wet 'n' Wild Las Vegas` against the Gold Coast row is 0.6061,
  and a spec case pins the floor on exactly that pair.) The radius carries the
  first class, **as long as the
  venue carries its own geocode**: with the floor at 0.65 no two such rows in
  the catalogue sit closer than 0.1174 km (`Boonie Bears Adventure Park Linhai`
  against its water park, 0.6923), 11.7× the radius.

  **Where it does not is the residual risk of this branch, and it is worth
  writing down.** A second venue that inherits its resort's geocode sits at
  0.0000 km, so the radius has no vote on it at all and only the name floor and
  `sourcesDisjoint` are left. Three rows are that shape today — PortAventura
  Park, Ferrari Land and Caribe Aquatic Park on one point — and the name holds
  all three pairs, at 0.1600–0.2000 against a floor of 0.65. `sourcesDisjoint`
  additionally fails for PortAventura Park against Ferrari Land, the pair
  Queue-Times lists twice; Caribe Aquatic Park carries a wartezeiten id and
  nothing else, so both of its pairs are disjoint and rest on the floor alone.
  The same hazard covers **any** shared placeholder geocode, not only a
  resort's: a source falling back to a city centroid puts two rows on one point
  just as exactly, and refusing `0, 0` by value cannot see that. But a water
  park
  that synced in on its resort's point, from a source the theme-park row does
  not carry, scoring like `Legoland Windsor` against its water park (0.7429),
  would satisfy all three conditions. No such row is in the catalogue today.
  What makes that safe rather than merely unlikely is the review gate (§5.5a),
  not another threshold here: a `sharedPoint` pair is never `safe`, so the
  endpoint reports it and waits.

  The floor is 0.65 and not 0.6 for one measured reason: at 0.6 the Rockford
  pair cleared both the name floor and `sourcesDisjoint`, leaving the radius as
  its only refusal — and that radius rests on coordinates Queue-Times
  publishes, not on ones we derive. 63 catalogue pairs score in [0.60, 0.65),
  all of them genuinely different parks. The closest of them is the Rockford
  pair itself at 0.0424 km — it is in this band, which is why the band matters —
  and the closest of the other 62 is 0.1901 km away, so all 63 are outside the
  radius already and raising the floor refuses nothing the radius was not
  refusing.

### 5.5a The gate in front of the merge

§5.5 added the detector. It did not add anything between a detected pair and a
deletion, and there was nothing there before it either:
`POST /v1/admin/merge-duplicate-parks` with `autoDetect: true` merged every pair
`findDuplicates` returned, in one call, inside a transaction with no undo — so
the only way to ask what it would merge was to let it merge. **PAR-247** gives
it the two halves the attraction side already had.

**Every pair carries `safe`.** It is true for one combination: a shared upstream
entity **value** — one external park cannot be two parks, which is why both real
production duplicates carry one (Universal Studios Hollywood on `qt-park-66`,
Islands of Adventure on one wartezeiten id; both are pinned as fixtures in
`park-validator.service.spec.ts`) — together with a name score of at least
0.95. That is word for word the existing `nameSimilarity >= 0.95 &&
sharedEntityId` branch, so the safe set is a subset of the detected set rather
than a second rule beside it, and a shared id that arrived by mis-assignment
still needs the name to agree before anything is deleted unattended.

**`sharedPoint` is never safe, and not by a special case.** It requires
`sourcesDisjoint`, which is false as soon as both rows carry an id from the same
source — and an equal value means exactly that. So `sharedPoint` implies no
shared id implies not safe. There is no second condition here that could drift
away from that one.

**`autoDetect` writes nothing without `dryRun: false`**, and `dryRun: false` is
permission to write, not permission to decide: a pair marked for review is
reported under `skipped`, with its winner already resolved so an operator can
send it back as a manual pair. The response carries `dryRun`, `planned` and
`skipped`; `GET /v1/admin/duplicate-parks` counts `safe` and `needsReview`
apart, as the attraction listing does.

**The manual pair (`park1Id` + `park2Id`) keeps its default, which is a real
merge.** Two ids typed into a form are the human judgement `autoDetect` lacks,
and the admin's merge button sends that body with no `dryRun` — a flipped
default would be a button that quietly stops working. It does honour an explicit
`dryRun: true`, because the alternative is the trap the attraction endpoint
sprang once: a request that says dry run, deletes the row, and prints the
outcome as a preview.

**A flag this endpoint cannot read is a 400, not a guess.** The body takes no
DTO, so nothing coerces it, and Nest parses form-encoded requests out of the box
— where every value is a string, and `dryRun=true` from a curl one-liner is
`"true"`. Under a strict comparison that is not `true`, so the request that
asked for a dry run in as many words would have deleted a park. `"true"` and
`"false"` are read; anything else on `dryRun` or `autoDetect` is refused.

**What this gate does not cover, said out loud so the paragraphs above are not
read as covering it:** `ParksService.repairDuplicates()` is a different path
with the same effect. It groups parks by a shared `queue_times_entity_id` in SQL
of its own, never calls `findDuplicates`, and merges every ghost it finds
without asking the name — the shared id is the strong criterion, but on its own
it is less than `safe` asks for here.

**How often it runs is a counted thing, not an impression.** It has one caller,
at the end of `syncParks()`, which has two of its own — `ensureParksLoaded()`
and the children-metadata processor — and both call it only where `findAll()`
returned zero parks. On a populated catalogue it does not run at all, which is
most days; it runs when the catalogue is bootstrapped. That is the reason this
is a follow-up (PAR-262) rather than the second half of this section.

### 5.6 An entity changed its `entityType` and left its old row behind

ThemeParks.wiki reclassifies entities **without changing their id**. On
2026-04-25 it moved 17 Universal Studios Singapore meet-and-greets and character
shows from `ATTRACTION` to `SHOW`; on 2026-04-23 it did the same to ten entities
at Tokyo Disneyland and five at Tokyo DisneySea.

`ChildrenMetadataProcessor.handleFetchChildren` fans the children of a park out
by `entityType` and writes each group into its own table. It followed the
reclassification into `shows` — and left the `attractions` row exactly where it
was, because `externalId` is unique **per table** and nothing compared the two.

The abandoned row does not go quiet. No source reports it any more, so
reverse-reconciliation writes a CLOSED row every poll cycle, and the park page
shows a permanently closed ride that no longer exists as a ride. Measured
against production on 2026-09-15, in the 30 days before the fix:

| Park | abandoned rows | `system-reconciliation` rows, 30 d | last real reading |
| -- | -- | -- | -- |
| Universal Studios Singapore | 17 | 11,832 | 2026-04-26 |
| Tokyo Disneyland | 10 | 6,970 | 2026-04-23 |
| Tokyo DisneySea | 5 | 3,485 | 2026-04-23 |
| Disney's Animal Kingdom | 1 | 21 | never |
| Disneyland Park (Paris) | 1 | 617 | 2026-08-29 |

`retireReclassifiedAttractions` now runs after the show and restaurant syncs of
each park and retires the rows that were left behind. The query below finds the show case, which is
all that has been observed; the code also covers an entity that became a
`RESTAURANT`, and applies the two second-source filters described further down:

```sql
SELECT p.name AS park, count(*) AS total,
       count(*) FILTER (WHERE a.queue_times_entity_id IS NULL)     AS only_wiki,
       count(*) FILTER (WHERE a.queue_times_entity_id IS NOT NULL) AS also_queue_times
FROM attractions a
JOIN shows s ON s."externalId" = a."externalId"
JOIN parks p ON p.id = a."parkId"
WHERE a.retired_at IS NULL
GROUP BY ROLLUP (p.name);
```

**The two rows in the last column are the reason this is not a blanket cleanup.**
`queue_times_entity_id` means Queue-Times reports the same entity, and it reports
it as an attraction with a wait time. Disneyland Paris' `Mickey's PhilharMagic`
is a show to the wiki and a queueing ride to Queue-Times, and it was still
receiving real `OPERATING` readings on 2026-08-29 while the wiki had it as a
show. Retiring it would delete a live ride over a disagreement between two
sources — a curation decision, not a sync one. Only rows that exist purely
because the wiki once called them attractions are retired.

**That column alone does not find every second source.** The entity mapping job
writes `queue_times_entity_id` for a Queue-Times match only; a `wartezeiten-app`
match leaves nothing but the `external_entity_mapping` row, and
`WaitTimesProcessor` resolves live data through that mapping all the same. On
2026-09-15, **39 attractions** carried exactly that combination — a wartezeiten
mapping and no Queue-Times id — so the retirement also skips any row with a
mapping whose `external_source` is not `themeparks-wiki`:

```sql
SELECT count(*) FROM attractions a
WHERE a.queue_times_entity_id IS NULL AND EXISTS (
  SELECT 1 FROM external_entity_mapping m
   WHERE m.internal_entity_id = a.id::text AND m.internal_entity_type = 'attraction'
     AND m.external_source = 'wartezeiten-app');
-- 39
```

None of them is among today's candidates, so this changes no number above. It is
the difference between a filter that happens to be right and one that is right
for a reason.

**And it undoes itself.** A row retired this way carries
`RECLASSIFIED_UPSTREAM_REASON` verbatim, and `syncAttraction` lifts the
retirement the moment the wiki lists the entity as an `ATTRACTION` again. That
is what makes it safe to run unattended: one malformed `/children` response
cannot strand a park's rides, because the next correct run brings them back.
Only rows carrying that exact reason are lifted — a retirement a human entered
through `POST /admin/retire-attractions` survives every nightly run, and that is
why the marker is an exact string rather than a prefix. The string is also
**user-facing** (`AttractionResponseDto` serves `retiredReason` on the public
attraction detail endpoint), so it reads as a sentence with its source and
carries no issue numbers or file paths.

**The data supply comes back with the row, in the normal case.** The `shows`
row keeps existing, and `WaitTimesProcessor` builds its entity lookup with the
shows after the attractions, so an un-retired attraction used to lose
`themeparks-wiki:<externalId>` to the stale show and go straight back to
collecting `system-reconciliation` CLOSED rows. §5.7 closes that from both ends:
the show is retired in the same pass, and the lookup skips retired rows. What
remains is the row a second source claims, which
`withoutForeignSourceMappings` holds back on purpose — there the shadowing is
the lesser evil, because the alternative is retiring a row another feed is
still filling.

**The protection runs one way.** A retirement a human entered survives every
run, because its reason is not one the sync wrote. An un-retirement entered by
hand does not: `POST /admin/unretire-attraction/:id` clears `retired_reason`,
the row matches the filter again, and the next run retires it while the wiki
still calls the entity a show. That is the sync winning an argument with a
person, which is the point of a sync — the wiki is the source for what an entity
*is*. To override it, correct the entity upstream or add its id to
`THEMEPARKS_EXCLUSIONS`.

**The test for a second source is structural, and deliberately so.** A draft of
this change also held back rows that had received a genuine reading in the last
30 days. It was withdrawn, because it would have made the fix arrive a month
late every time: once the wiki flips an entity's type, `WaitTimesProcessor`
resolves `themeparks-wiki:<externalId>` to the shows row, so the attraction's
last genuine reading *is* the day of the reclassification — and the wrongly
CLOSED ride would have stayed on the park page for the whole window. A row's own
columns say what it is; its readings say when we last heard, which is a
different question.

Three more limits worth knowing before trusting the round trip:

- **The way back is park-scoped.** The retirement is not: `externalId` is
  globally unique, so a row whose park changed upstream is still found and
  retired, but `syncAttraction` only ever looks at its own park's rows, so that
  one row has to be brought back by hand.
- **An id that arrives as an `ATTRACTION` in the same response is excluded**
  from the retirement list. `/children` listing one entity under two types is
  the same upstream fault `dedupePollEntities` handles for live data, and
  without the exclusion the row would flip between retired and not on every
  run, evicting caches and revalidating the frontend each time.
- **`detect-seasonal` used to erase the season of any retired row**, on the
  reasoning that a demolished ride never reports OPERATING again. A row retired
  this way can come back, and while it is retired it receives no readings at
  all (`wait-times.processor.ts` loads its attractions with
  `retiredAt: IsNull()`) — so the detector could never re-derive what it had
  cleared. Step 2c now skips retirements carrying a
  `RECLASSIFIED_UPSTREAM_REASONS` value; every other retirement stays permanent.

And the 17 rows retired by hand for this issue on 2026-09-15 carry their own
reason, with the entity URL in it, rather than the constant. They were an admin
write, so the sync treats them the way it treats any human retirement: it will
not lift them. That is the intended reading, not an oversight.

### 5.7 · The other direction, `SHOW`/`RESTAURANT` -> `ATTRACTION`

Both tables now carry `retired_at` / `retired_reason`, nullable and with the
same partial index on `retired_at IS NULL`, and
`retireReclassifiedChildEntities` runs straight after its attraction-side twin
in the same pass. A show whose entity the wiki now calls an `ATTRACTION` is
retired with `RECLASSIFIED_AS_ATTRACTION_REASON`; `syncShow` and
`syncRestaurant` lift exactly that reason again, and nothing else.

**The two are not each other's undo.** Each direction excludes the ids that
arrived under the other type in the same `/children` response, so a payload
listing one entity twice cannot make the pair retire and un-retire each other
every run.

**Three differences from the attraction side, all deliberate:**

- **The way back is not park-scoped.** `syncShow` looks a row up by
  `externalId` alone, which is unique across the whole table, so a child entity
  that moved parks upstream is still found and un-retired — where an attraction
  in the same position is not, because `syncAttraction` is park-scoped. It
  comes back under its **old** `parkId` either way: neither sync method moves
  that column, so such a row reappears in the wrong park's payload and still
  has to be moved by hand.
- **The second-source guard is inert today, and that is measured rather than
  assumed.** `external_entity_mapping` held 5,486 `queue-times` and 1,295
  `wartezeiten-app` rows against `internal_entity_type = 'attraction'` on
  2026-09-16, and **zero** against `'show'` or `'restaurant'`. `show_live_data`
  has no `data_source` column at all, because the wiki is its only feeder. The
  check is kept because it is structural and starts working by itself the day a
  second source claims a show — not because it is holding anything back now.
- **There is no retirement service.** The attraction side has one because the
  admin endpoints write there too; here the sync is the only writer, so the
  eviction and revalidation hang off the write itself.

This direction is **not observed in production**: all 34 collisions measured on
2026-09-15 run from `ATTRACTION` to `SHOW`, and `restaurants` has none. The
column is what makes the repair possible when it does happen, and what stops a
reclassified show from standing in the park payload beside its own
replacement.

### 5.8 A whole park went silent while its schedule kept publishing

**Measured 2026-09-16 (PAR-192).** La Ronde (Montréal, `multi-source`) had not
produced a single live reading since **2026-06-24** from Queue-Times and
**2026-06-17** from ThemeParks.wiki — 84 and 91 days. Both upstreams still
answer, and both answer empty: `liveData: []` and `{"lands":[],"rides":[]}`.
Nothing in the catalog records it. All 38 attractions are un-retired, none
carries `season_out_since`, and the schedule publishes **349** future
`OPERATING` days through 2027-08-31.

**What the API served while that was true.** Not an empty ride list — the
opposite:

```
GET /v1/parks/north-america/canada/montreal/la-ronde
  park.status              OPERATING
  liveWaitTimes.available  true
  38 attractions           effectiveStatus OPERATING × 38, crowdLevel very_low × 38

GET /v1/parks/.../la-ronde/attractions/ednoer
  status                   CLOSED
  hourlyForecast           30 min at 93.6 % confidence
```

Three surfaces of the same silence disagreeing with each other. The park page's
`statusWithoutLiveData` fallback is written for **one** ride going quiet at a
park whose feed works (`no-live-data-status.util.ts`); a park where every ride
is quiet falls into it 38 times over, and its own docblock's counter-example —
"one ride, one request apart, two answers" — came back at park scale. The
`crowdLevel` chain has the same shape: `very_low` is its last-resort default and
is only skipped for a park with no readable source, which La Ronde is not.

**It is a class, not a case.** The ticket's own query found one row because it
compared `max(timestamp)` against a cut-off, which drops parks that have **never**
produced a reading. Counting those too, on 2026-09-16:

| Park | Rides | Last reading | Future OPERATING days | Open days in the next 7 |
| -- | -- | -- | -- | -- |
| La Ronde | 38 | 2026-06-24 | 349 | 7 |
| Movieland The Hollywood Park | 25 | never | 29 | 5 |
| Water Country USA | 19 | never | 105 | 2 |
| Adventure Island Tampa | 16 | never | 192 | 4 |
| Paradise Country | 12 | never | 118 | 7 |

110 attractions, 25 park-days in the week after the measurement. Four of the
five read `CLOSED` in that same snapshot only because they were outside their own
opening hours at the minute it was taken; the park-level `status` gates
everything below it, so each of them meets the same branch when its own window
opens.

**Two things changed.** `ParkIntegrationService` now reads
`waitTimesKnowable = waitTimesReadable && parkObservedRecently`, where the second
half is `QueueDataService.hasObservedReadingWithin(parkId,
PARK_FEED_SILENT_DAYS)` — a bounded `EXISTS` over `observedReadingsSql()`, so our
own reconciliation and heartbeat rows cannot clear a park. Ride status,
`crowdLevel`, best visit times and the park's wait statistics all read it, and a
silent park's rides land on `UNKNOWN` — the same place a park with no readable
source sends them.

`closedAttractions` reads 0 in that branch, **while the park itself is open**.
"0 operating is true, none is *known* to run" defends `operatingAttractions`,
which still computes itself and at a silent park counts exactly the free-flow
rides. Nothing defended the other half: `total - operating` served "38 of 38
closed" under an `OPERATING` badge, which is "Park geöffnet, alle Bahnen zu"
arrived at from the other direction. In a CLOSED park the rides really are
closed for a reason we can state, so the count stands there — zeroing it would
replace a true answer with a shrug. `total = operating + closed` therefore does
not hold at an open park whose waits are unknowable, and the DTO descriptions
say so: the remainder is the rides nobody can speak for.

`liveWaitTimes.available` stays `true`: it is the frontend's contract for
"publishes wait times nowhere" (`live-wait-time-sources.ts`), and La Ronde
published 7.307 rows through June. Its description now says outright that `true`
is not the same as "we have data", because a silent park keeps the flag.

**Thirty days, and the number it is measured against is not an incident.** Days
since the last observed reading, per park with at least one un-retired
attraction, 2026-09-16:

| 0–1 | 2–30 | 31–90 | 90+ | never in 400 days |
| -- | -- | -- | -- | -- |
| 195 | **0** | 2 | 3 | 4 |

Every park anyone is still reading answers inside 48 hours and the band between
two days and a month is empty, so the threshold sits in a gap at fifteen times
the observed maximum. Busch Gardens Tampa's 65-day recovery in §5.2a is *not*
the comparison: that was nine rides of a park whose feed kept working, which is
`findSilencedClusters`' subject and never reaches this rule.

The constant reaches further than the report does. Nine parks were over the
30-day line on 2026-09-16 and all nine lose the fallback; the five below are the
subset that is also scheduled open, which is the narrower question the detector
asks.

`DataQualityMonitorService.findScheduledButSilentParks()` reports the state
nightly. It is the complement of `findSilencedClusters`, which cannot be widened
to cover it: that detector's `park_health` CTE demands at least three attractions
with an `OPERATING` reading in the last two days, precisely so a park closing for
the season is not read as a dropped feed — and a park where everything is silent
fails that gate by construction. What separates the two here is the **schedule**,
read over the next `SILENT_PARK_LOOKAHEAD_DAYS` = 7 days. The window is what
keeps this from being a seasonal-park alarm: a park shut for the winter with next
summer already published has future operating days and an empty feed, and is
neither a fault nor news. At 7, 30 and unbounded the query returns the same five
parks on 2026-09-16, so the window costs nothing today and is the gate that keeps
January out of the log.

Each of the three detectors catches its own throw, and the job counts how many
ran: three caught throws leave three empty lists, and an empty list is what
"nothing is wrong" looks like — a ✅ printed under three ERROR lines is §5.1
with more output.

**What inherits the gate, and what does not.** `/location` and the favourites
park card read `park:integrated:<id>`, so on a cache hit they carry the new
values without knowing about them: a silent park shows the park page's own
counters and `crowdLevel: "unknown"` there too, which at La Ronde means
`operatingAttractions: 0` because it has no free-flow ride. Their own miss paths, which build from
`AnalyticsService` directly, do not.

Untouched and left for their own issue: the ride's own endpoint
(`AttractionIntegrationService`), the park list (`ParkEnrichmentService` serves
the attraction counters with no gate at all), and the ride-alert subscription
check, whose rule against accepting an alert that can never fire reads the
curated list alone. A silent park's ride therefore still answers `CLOSED` on its
own page and still carries its hourly forecast — a narrower contradiction than
the one removed (`UNKNOWN` against `CLOSED` rather than `OPERATING` against
`CLOSED`), but a contradiction. Each would need its own park-level probe.

The withholding block itself covers three claims and not every wait-derived one:
`avgWaitTime`, `avgWaitToday`, `peakWaitToday` and `occupancy` keep their
empty-set zeroes — Ø 0 min, 0 % occupancy and, out of `calculateParkOccupancy`'s
own no-data exit, `comparisonStatus: "typical"`, which reads as a verdict rather
than as the absence it is. That gap is older than this change — it already
applied to the one curated no-wait-times park — and is
[PAR-298](<https://linear.app/parkfan/issue/PAR-298>).

Measured against production on 2026-09-16. The detector runs in **1.2 s**. The
per-request probe costs 1.8 ms at Europa-Park, where the first row
short-circuits the `LIMIT`, and 7.0 ms at La Ronde, where all 38 index searches
come back empty — behind **45 ms** of hypertable planning on a warm backend and
**338 ms** on a cold one. The cold number is paid once per pooled connection
rather than per request, and `findCurrentStatusByPark` beside it in the same
`Promise.all` is a hypertable query of the same class, so the probe joins a band
the park payload already pays. Not on an hourly cache, though:
`calculateDynamicTTL` gives an `OPERATING` park the seconds to the next
five-minute boundary, and `OPERATING` is the only state in which the probe
changes an answer.

**Open:** whether La Ronde's 349 published days stay or are cut back. They are
not a season — they are **every single calendar day** from 2026-09-17 to
2027-08-31, 59 of them in January and February in Montréal, with no `CLOSED`
entry anywhere in the range. The feed is not coming back on its own, and a
calendar no measurement has confirmed since June is a claim about a park that
nobody has checked. That is a decision about data, not code, and it is not made
here.

---

## 6. Diagnostic SQL

**Is a "closure" ours or the operator's?**
```sql
SELECT data_source, is_heartbeat, status, count(*), max(timestamp)::date
  FROM queue_data
 WHERE "attractionId" = '<id>' AND timestamp > now() - interval '3 days'
 GROUP BY 1,2,3;
```
`system-reconciliation` means no upstream source reported the ride, and the
reverse-reconciliation write is ours. Two things that looks like and is not: an
entity the wiki has
recategorised to `SHOW` keeps reporting into `show_live_data`, under the same
upstream `externalId` but a different internal row, while its stale
`attractions` row sees only reconciliation (§5.2a, group A). And a
`data_source` that is neither of the system sources is not by itself a live
reading — a heartbeat carries the previous row's source forward, which is what
`observedReadingsSql()` exists to exclude.

**Which parks have a silenced cluster?**
```sql
WITH last_op AS (
  SELECT "attractionId",
         max(timestamp) FILTER (WHERE status='OPERATING') AS last_op,
         max(timestamp) AS last_row
    FROM queue_data WHERE timestamp > now() - interval '270 days' GROUP BY 1
)
SELECT p.id, p.name, p."citySlug", count(*),
       min(l.last_op)::date, max(l.last_op)::date,
       array_agg(a.id) AS silent_set   -- feeds §5.2a's check 1 as $1
  FROM last_op l
  JOIN attractions a ON a.id = l."attractionId"
  JOIN parks p ON p.id = a."parkId"
 WHERE l.last_op < now() - interval '30 days'
   AND l.last_row > now() - interval '2 days'
 GROUP BY p.id, p.name, p."citySlug" HAVING count(*) >= 5 ORDER BY 4 DESC;
```
`silent_set` is there so the follow-up in §5.2a has something to take: check 1
is parameterised on the ride ids, and they exist nowhere else — the rest of
this query is per-park aggregates, and reconstructing the set by hand means
re-applying the `HAVING` floor too.
Identical min/max dates = a feed event, not N independent closures.

**Group by `p.id`, never `p.slug` or `p.name`.** Neither is an identity.
`slug` is not unique — there are two `disneyland-park` rows, Anaheim and Paris,
and grouping over it silently adds two parks into one. A display name can be
edited or curated and is no safer.

Grouping correctly will not rescue you from §5.5, though, and it is worth being
clear which problem is which: there, **two `parks` rows are one real park**, so
every grouping — id included — reports it twice. No query fixes that; only a
merge does.

**The window is 270 days and the floor 5 on purpose** — this used to read 120
and 6, which on 2026-09-11 hid eight of the sixteen clusters (§5.2a). A ride whose
last OPERATING row falls outside the window has no `last_op` and vanishes from
the result rather than showing up as silent.

**270 is not a structural number and will go stale.** Nothing deletes from
`queue_data`; the oldest row is 2025-12-24, which was 261 days before the
2026-09-11 run, so 270 days reached the whole archive *then*. The archive keeps
growing and the window does not. The rule is "reach past the drop you are
chasing" — check `min(timestamp)` below and widen accordingly, or Knott's
(last OPERATING 2026-04-14) will drop out of this query the same way it dropped
out of the 120-day one. And remember the
count this returns is a symptom, not a finding. Two checks decide what it
means, and both are needed: §5.2a's `data_source` split says whether any source
still writes the rides, and `GET /v1/entity/{id}` says whether upstream still
knows them and as what. The first alone cannot tell a recategorised entity from
a dead one.

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
