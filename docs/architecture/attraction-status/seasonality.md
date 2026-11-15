# Seasonality

What `is_seasonal`, `season_months` and `season_out_since` mean, how the nightly `detect-seasonal`
job writes them, and how a human curates a season. Open this before touching the detector or
writing a season by hand. The seasonal rides' effect on what a park serves today is in
[status regimes](status-regimes.md); what a client reads, and the SQL twin that must change with
the TypeScript rule, in the [season contract](../../frontend/seasonality.md).

## The columns

| Column | Meaning | Writer |
| --- | --- | --- |
| `is_seasonal` | "fully closed on ≥ 7 park-open days while the park ran" — true regardless of how long we have watched | `detect-seasonal` |
| `season_months` | 1-based months it runs. The column that has lied | `detect-seasonal`, only after `MIN_OBSERVED_DAYS` |
| `season_out_since` | last park-local day the detector saw the ride running | `detect-seasonal` |
| `curated_is_seasonal`, `curated_season_months` | a human's season, read first | admin ([curation](../../admin/curation.md)) |

Both pairs resolve through `resolveCuratedFacts`; a curated `false` takes the months down with it.
`isCurrentlyInSeason` (`curated-attraction-facts.util.ts`) turns them into three values; what each
means and who reads it is the [season contract](../../frontend/seasonality.md#iscurrentlyinseason-has-three-values).
A third writer sits between the two pairs: a park season that names the ride, to run only during
it ([below](#a-ride-bound-to-a-park-season)) or not during it
([paused](#a-ride-a-park-season-pauses)).

## A ride bound to a park season

Phantasialand's ice rink and skate hire exist only during Wintertraum. Naming them in the
Wintertraum season's `attractionIds` makes them seasonal, with the season's days as their window.
Nothing is stored on the ride: `Attraction.seasonBindings` is a virtual column (a subquery in the
SELECT, never a table column, so `synchronize` leaves the schema alone) that lists the seasons
binding the row.

**What binds.** A season of a kind in `PARK_SEASON_BINDING_KINDS` (`halloween`, `christmas`,
`winter`, `summer_nights`, `special_event`) whose status is not `cancelled`; `expected` binds, since
the dates are still the best on file. `maintenance` keeps its own meaning (the named rides are down
inside a normal season), and `opening` and `closure` describe the park, so none of the three binds.
The predicate is `seasonBindsAttraction` (`season-window.sql.ts`). Ids are matched as lowercase
text, which `ParkSeasonService` writes.

**The window.** In season on the season's days: its `dates` when it has them, every day between its
bounds otherwise, park-local. Every other day is out of season. With several bound seasons (one per
year), any of them counts. A season binding an all-year ride it only re-themes would put that ride
out of season for the rest of the year, so a re-theme is not listed in `attractionIds`.

**Precedence**, the same in `resolveCuratedFacts` and its SQL twin `attractionIsOutOfSeason`:

| Writer | Wins over | Note |
| --- | --- | --- |
| the ride's curated `false` or curated months | everything | the ride's own page is the most specific statement |
| a binding park season | the detector | a curated `true` without months agrees and leaves the window to the season |
| `detect-seasonal` | nothing | `is_seasonal`, `season_months`, `season_out_since` |

When a binding decides, the resolved `seasonMonths` and `seasonOutSince` are null: the days say
more than either. `seasonBindings` on the resolved facts is empty whenever a binding does not
decide.

**Who reads it.** Every reader of the season goes through the two resolvers, so a bound ride needs
no code of its own: the park payload and its counters, the ride page, favourites, nearby, the free
flow gate (`isFreeFlowOpen` passes the bindings to `isCurrentlyInSeason`), `/plan/day` (judged on
the planned day), ride alerts, the rain-soon push, the SQL counters (park listings, discovery,
global stats), the outage and „not run today" lines, and the MCP and Parky tools. Out of its window
a bound ride reads `isCurrentlyInSeason: false` and carries `boundSeason`
([contract](../../frontend/seasonality.md#a-ride-bound-to-a-park-season)). The detectors read the
same answer: `findSilencedClusters` takes a bound ride as a known season, `findAbsenceRetiredUnreviewed`
does not ask about one, the retirement candidates leave it out while it is outside its days,
the children sync's absence step does not retire it (`isSeasonalRow`), and the
[maze rule](maze-rule.md) reads a bound season's months as its season.

**Explicit selects.** A `find` with `select` loads the virtual column only when it lists
`seasonBindings: true` (`/plan/day` and the children sync do); without it the ride resolves as not
bound. Raw SQL reads `attractionIsSeasonal`, `attractionIsSeasonBound` or `attractionIsOutOfSeason`.

**Publishing.** A season write evicts the park and the rides it binds, before and after the edit
(`AdminSeasonService`), so a ride page does not keep its old season for the cache's lifetime.

**Checked.** On 2026-11-14 the two resolvers were compared on a scratch Postgres over 756
combinations of the five seasonality columns and seven bindings (none, running, future,
cancelled, `maintenance`, a `dates` list with and without today): no disagreement.

## A ride a park season pauses

The inverse binding. Phantasialand's River Quest and Phenies close for Wintertraum and run the rest
of the year; curated months cannot say that (they would put the rides out of season every winter
month, Wintertraum or not, and on its days only by luck). The season says it instead: its
`paused_attraction_ids` (`ParkSeason.pausedAttractionIds`, a nullable jsonb column beside
`attraction_ids`, added by `synchronize` with no backfill) lists the rides that do not run during
it.

- **What pauses.** The same five kinds that bind (`PARK_SEASON_BINDING_KINDS`), not cancelled.
  `closure` is left out: it shuts the whole park, every ride with it, and the park calendar reads
  it ([calendar rules](../calendar-schedule-and-ml-rules.md)). `maintenance` keeps its own
  meaning, its rides down inside it, and `opening` describes the park. On another kind the list is
  stored and pauses nothing, as `attraction_ids` binds nothing there.
- **The window.** Out of season on the season's days (its `dates`, else every day between its
  bounds), in season on every other. A ride a season binds and another pauses is out of season on
  the pausing days whatever the binding says; past them the binding decides.
- **Precedence.** Where the binding sits: the ride's curated `false` or curated months override a
  pause, a pause overrides the detector. Paused, the ride is seasonal, and the resolved
  `seasonMonths` and `seasonOutSince` are null.
- **The twins.** `Attraction.seasonPauses` is the virtual column (`attractionSeasonPausesSql`,
  `seasonPausesAttraction`); `resolveCuratedFacts` returns `seasonPauses`, empty when a pause does
  not decide, and `isCurrentlyInSeason` reads it; the SQL twin is `attractionIsOutOfSeason`, and
  `attractionIsSeasonal` and `attractionIsSeasonBound` count a pause as they count a binding. A
  `find` with `select` lists `seasonPauses: true` beside `seasonBindings: true`.
- **What a client reads.** `pausedBySeason` beside `boundSeason`
  ([contract](../../frontend/seasonality.md#a-ride-a-park-season-pauses)). Parky's ride answer
  names the pausing season and the day after it as the reopening; `get_park` lists it as
  `pausedBy` on the rides out of season.
- **Writes.** One season never lists a ride both ways (`ParkSeasonService` refuses it), a park
  merge rewrites both lists and keeps a ride the merge leaves in both in `attraction_ids`
  (`remapSeasonAttractionIds`), and a season write evicts the rides of both lists, before and
  after the edit (`seasonRides` in `AdminSeasonService`).

## The observation-window artefact

`detect-seasonal` derived months from "every month we ever saw it OPERATING". With less than a year
of history that is the span we happened to be recording. On 2026-08-15 `queue_data` reached back
234 days (to 2025-12-24) and every stored list was a contiguous run anchored at its start
(`[1,2,3,4,12]` ×69, `[1,…,6,12]` ×64, `[4]` ×22, `[1,12]` ×18). Phantasialand's Avoras, open all
year, read out of season all summer with `[1,12]` — the same list that is correct for the two ice
rinks beside it. The data alone cannot separate them.

**The guard:** months are derived only for entities watched at least `MIN_OBSERVED_DAYS` (330,
`queue-percentile.processor.ts`), measured over **all** rows, not the OPERATING ones (a winter
attraction operates ~40 days). Under-observed entities are excluded from the month query, so each
run writes NULL and clears artefacts. Shows follow the same rule. All 340 attraction and 954 show
lists were cleared on 2026-08-15; months become derivable again from about **2026-11-19**.

## What the detector skips

- **Free-flow rows** (`open_with_park`): a playground's feed says CLOSED every day, which is the
  detector's exact signature for a season. Both candidate searches skip them.
- **Feed events** (step 3a): when, in one park, at least 5 candidates and 20 % of the tracked rides
  (`FEED_EVENT_CLUSTER_THRESHOLDS`) last reported OPERATING in the same minute, that is a source
  change, not forty seasons (Europa-Park 2026-06-07 13:18, 38 rides). They are not flagged, and
  rows an earlier run flagged on that evidence are cleared. Rows with `curated_is_seasonal` set are
  left alone.
- **Retired rows** lose a stale seasonal flag (step 2c), except a retirement the sync can undo
  (`RECLASSIFIED_UPSTREAM_REASONS`, `retired_kind` null): a retired row gets no readings, so a
  season cleared there could never be re-derived ([reclassification](reclassification-and-absence.md)).
- **Review marks** are not consulted: the detector describes the feed
  ([review marks](review-marks.md)).

Reverse-reconciliation rows are not evidence: the evidence queries read observed rows only
(`observedReadingsSql()`, PAR-32). The 44 Europa-Park rides ThemeParks.wiki dropped on 2026-06-07
had all derived `[1,2,3,4,5,6,12]` from them.

### Free-flow rows and their months

The detector cannot derive months for something it never sees OPERATING, so months on a free-flow
row are human-written. Step 2b only clears free-flow rows whose `season_months` is NULL
(`UPDATE … SET is_seasonal = false WHERE open_with_park AND is_seasonal AND season_months IS NULL`),
and the recently-operating reset skips `open_with_park` (Avoras emitted 154 OPERATING rows while
free-flow, which would have wiped a curated season). That guard reads the **synced** column while
curation writes `curated_season_months`, so on its own it protects nothing; what keeps a curated
free-flow season stable is `curated_is_seasonal` (below).

## Running and changing the detector

Run it without waiting for 02:30: `POST /v1/admin/detect-seasonal` (queues `detect-seasonal` on
`analytics`).

Hand modified job SQL to Postgres as `EXPLAIN` before shipping: a SQL string in a template literal
compiles and lints as well broken as whole. A dropped `WITH` once made every run throw `42601`
after computing its candidates, and the detector wrote nothing for 73 days before anyone noticed.
Failed jobs are now swept nightly ([job queues](../job-queues.md#failed-jobs-retention-and-monitoring)).

## Curating a free-flow attraction

1. **Research it against the operator's own pages**, never from recall. A name is not evidence:
   Toverland's "Kletterparcours" was a harnessed high-ropes course with a 140 cm minimum.
2. Confirm it has no queue, no ride vehicle, no separate ticket.
3. **Establish the season, and treat both answers as answers.** If the park runs while the area
   does not, it needs months before the flag is safe, or a snow playground reads open in July. If
   the operator lists it under every season the park has, it takes **no** months — null is
   `isInSeason`'s "all year", the state most free-flow rows are in (Europa-Park's Limerick Castle and
   Paul's Playboat). Never invent a list to be safe: once a list exists every month left out is a
   hard close.
4. **If step 3 produced months**, write `curated_season_months` **and** `curated_is_seasonal = true`.
   If it produced none, write neither: `curated_is_seasonal = true` over no months reports a seasonal
   area that runs all year. `curated_is_seasonal = false` pins a free-flow area the detector keeps
   calling seasonal because the park shut around it.
5. Write through `PATCH /v1/admin/content/attractions/:id` with a `reason` and a `sourceUrl`, never a
   raw `UPDATE`: the endpoint writes the audit row and publishes in the
   [outside-in order](../../rules/publish-outside-in.md), a raw UPDATE lands in the database and not
   on the page.

### Turning an operator's season into `curated_season_months`

**A month goes in unless the operator's season covers no more than a tenth of the days the park is
open that month.** The denominator is the park's open days, not calendar days:

```sql
SELECT extract(month FROM s.date)::int AS month, count(DISTINCT s.date) AS open_days
FROM schedule_entries s
WHERE s."parkId" = $1                  -- the id; parks.name is neither unique nor the displayed name
  AND s."attractionId" IS NULL         -- park-level rows
  AND s."scheduleType" = 'OPERATING'   -- EXTRA_HOURS and event rows are not "open to everybody"
  AND s.date >= $2 AND s.date < $3     -- one full, already published season cycle
GROUP BY month ORDER BY month;
```

`count(DISTINCT s.date)` because a day can hold several rows; anything that needs open *minutes*
goes through `park-open-window.sql.ts`. A month with no rows is ambiguous (shut, or not published
yet): run it over a cycle the operator has released end to end.

| ride | operator's season | month | open days | covered | share | verdict |
| --- | --- | --- | --- | --- | --- | --- |
| Europa-Park, Lítill Island | 28 Mar – 25 Sep | March | 10 | 4 | 40 % | in |
| Bellewaerde, Snowmen Playground | from 28 Nov | November | 8 | 2 | 25 % | in |
| Europa-Park, Water Playground | to 1 Nov | November | 30 | 1 | 3 % | out |
| Everland, Snow playground | to about 1 Mar | March | 31 | ~1 | ~3 % | out, softly |

The threshold sits low because the errors are lopsided: a missing month is a hard close for every
open day in it, an extra month only over-reports the stretch before the season starts. Everland's
March is the unsafe call — its own pages never state the closing date (trade press says ~1 March) —
and was excluded because the park is open all 31 days; revisit when Everland publishes a date.

As of 2026-09-09, 32 attractions in 17 parks carry `open_with_park`; four of them have curated
months (Lítill Island `[3–9]`, Water Playground `[3–10]`, Snow playground `[12,1,2]`, Snowmen
Playground `[11,12,1]`). Held for want of a stated window: Peppa Pig's *Muddy Puddles Splash Pad* and
Walibi Rhône-Alpes' two *Exotic Island* areas (PAR-1098).

## Checking month lists

```sql
-- contiguous runs anchored at the start of history are the tell
SELECT season_months::text, count(*) FROM attractions
 WHERE season_months IS NOT NULL GROUP BY 1 ORDER BY 2 DESC;

-- how deep the history is
SELECT min(timestamp)::date, max(timestamp)::date FROM queue_data;
```
