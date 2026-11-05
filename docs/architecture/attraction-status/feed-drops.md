# Feed drops: when a cluster of rides goes quiet

A set of rides in one park that stopped saying OPERATING on the same day is a **symptom**, and three
different events produce it. Open this before treating a "silenced cluster" as a finding. The nightly
detector for it is `DataQualityMonitorService.findSilencedClusters`; a park where *everything* is
silent is a different case ([status regimes](status-regimes.md#5-a-whole-park-gone-silent)).

## Three events, one symptom (measured 2026-09-11, PAR-38)

The cluster query below returned 16 park rows and 204 rides. Two checks split them:

| group | what it is | parks · rides |
| --- | --- | --- |
| **A · recategorised upstream** | same entity id, `entityType` changed, live data still flowing into a *different table* | Universal Studios Singapore 17 |
| **B · genuinely silent** | nothing reports them; upstream still carries the entity | Europa-Park 46, Rulantica 18, Fiesta Texas 15, Gröna Lund 14, Knott's 9, Bellewaerde 6, Hollywood 5, Cinecittà World 4, Everland 4, Mid-America 3, USJ 1 — **125** |
| **C · not silent, just closed** | the wiki or Queue-Times keeps writing CLOSED, newest row today | Wet'n'Wild ×2 26, Mid-America 10, Traumatica 7, USJ 7, Ocean Park 6, Bellewaerde 2, Cinecittà World 2, Everland 2 — **62** |

- **Group C** is where the first reading (2026-08-15, "~140 attractions across ten parks") went
  wrong: both Wet'n'Wild parks were in the southern winter, Ocean Park's rides are reported CLOSED
  every few minutes. A 30-day "no OPERATING" cut cannot tell a lost ride from a shut one.
- **Group A** is the wiki moving meet-and-greets from `ATTRACTION` to `SHOW` on 2026-04-25. The sync
  created the shows and left the attraction rows to collect reconciliation CLOSED — fixed since
  ([reclassification](reclassification-and-absence.md)).
- **Group B** has two shapes. Europa-Park (44 of 46), Rulantica and Mid-America are still listed in
  `/children` and only lost their `/live` rows; Knott's, Fiesta Texas, Bellewaerde, Hollywood and
  Everland fell out of `/children` too, while `GET /v1/entity/{id}` still returns the entity intact.
  Either way the id we hold is the id upstream publishes, so there is nothing to re-match. The theme
  is **an event ends and its attractions leave the feed with it** (Fiesta Texas' Fright Fest mazes,
  Bellewaerde's and Cinecittà's Christmas attractions, Everland's snow areas, Gröna Lund's haunted
  houses); Knott's nine are facilities. Only Europa-Park, Rulantica and Mid-America (*MR. FREEZE:
  Reverse Blast*, *JUSTICE LEAGUE*, *THE JOKER*) lost operating rides. Gröna Lund and Cinecittà are
  Queue-Times-only; USJ's one is a Queue-Times mapping defect (PAR-161).
- **Busch Gardens Tampa** is a fourth case: nine rides quiet from 2026-06-13, back by themselves on
  2026-08-17 (65 days), all nine dual-sourced. A Queue-Times mapping is not armour either: 84 of the
  204 carry one.

## Europa-Park, 2026-06-07

There is no other source to switch to: Queue-Times (park 51, 39 rides) and wartezeiten.app
(`europapark`, 36 entries) publish only the marquee rides, and the wiki entities still exist with
`liveData: []`. Three numbers, all correct and not versions of one figure: **44** entities stopped
arriving in the feed that day; **45** rides had > 100 OPERATING rows before 13:18Z and none since
(the sweep population); **46** is the 270-day cut, the 45 plus the dual-sourced *'Bellevue' Ferris
Wheel*. Of the 59 rides without a Queue-Times id, the other 14 are the four EP Express stations (still
reporting OPERATING) and ten that never reported OPERATING at all (eight winter-only).

The 45 were researched one by one against the operator's pages: **10 free-flow** (`open_with_park`
set; Limerick Castle and Paul's Playboat written 2026-09-09 with no months, because the park lists
them under all four seasons), **34 operated rides** correctly left alone, **1 undecided** (*Rocking
Bridge & Chute*: on neither of the operator's lists, own page 403 — possibly gone, which would be
`retired_at`, not the flag). With the park open they now serve 10 OPERATING and 35 UNKNOWN, no CLOSED.
Two traps: the operator can contradict itself (*Dwarf City* is under "playgrounds" while its own page
gives ride time and capacity — the more specific page wins), and the surviving name is the wiki's,
so a pass that only matches the operator's names silently drops rows.

## The cluster query

```sql
WITH last_op AS (
  SELECT "attractionId",
         max(timestamp) FILTER (WHERE status='OPERATING') AS last_op,
         max(timestamp) AS last_row
    FROM queue_data WHERE timestamp > now() - interval '270 days' GROUP BY 1
)
SELECT p.id, p.name, p."citySlug", count(*),
       min(l.last_op)::date, max(l.last_op)::date,
       array_agg(a.id) AS silent_set   -- input to check 1
  FROM last_op l
  JOIN attractions a ON a.id = l."attractionId"
  JOIN parks p ON p.id = a."parkId"
 WHERE l.last_op < now() - interval '30 days'
   AND l.last_row > now() - interval '2 days'
 GROUP BY p.id, p.name, p."citySlug" HAVING count(*) >= 5 ORDER BY 4 DESC;
```

- Identical min/max dates mean a feed event, not N closures.
- **Group by `p.id`**: two parks share the slug `disneyland-park`, and a name is curated. Grouping
  correctly does not help when two `parks` rows are one park ([duplicates](duplicates.md)).
- **The window must reach past the drop you are chasing.** A ride whose last OPERATING row is outside
  it has no `last_op` and vanishes instead of showing as silent: at 120 days and a floor of 6, eight
  of the sixteen clusters were invisible. 270 reached all of `queue_data` (oldest row 2025-12-24)
  on 2026-09-11; check `min(timestamp)` and widen.

## The two checks, and they are not interchangeable

**Check 1 — does anything other than us still observe these rides?** Decide per ride, then count per
park (five parks have rides on both sides). Run it straight after the cluster query; later, a ride
retired since falls out of `src` silently, so drive the join from `unnest($1)` with a `LEFT JOIN`.

```sql
WITH src AS (
  SELECT q."attractionId",
         COALESCE(bool_or(COALESCE(q.data_source, '') NOT IN
                   ('system-reconciliation', 'system-heartbeat')
                 AND NOT COALESCE(q.is_heartbeat,
                                  q."lastUpdated" = q.timestamp)), false) AS still_reported
    FROM queue_data q
   WHERE q."attractionId" = ANY($1::uuid[])          -- silent_set
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

The predicate is `observedReadingsSql()` written out: a heartbeat carries the previous row's
`data_source` forward, so a bare `data_source <> 'system-reconciliation'` reads a feed that stopped
yesterday as still writing.

**Check 2 — `GET /v1/entity/{id}` upstream**, and the park's `/children` and `/live`: does the entity
exist, as which `entityType`, and is it listed? Check 1 separates C from the rest and nothing more;
group A's rows look exactly like group B's in `queue_data`, because their data arrives in
`show_live_data`. Only check 2 sees that.

## A source that still answers and has stopped measuring (PAR-1023, measured 2026-10-27)

Everything above is about a source that went away: the ride leaves
`/children`, or no feed mentions it, and reverse-reconciliation writes the
CLOSED row the rest of this page is about. There is a second way for a source
to stop speaking, and none of those checks can see it. The feed keeps answering
about the ride, with a real `dataSource`, and stops looking at it.

The symptom is a frozen `lastUpdated`. **LEGOLAND Billund, 2026-10-27:** the
feed delivered a row for all 37 attractions at 09:15 UTC carrying `lastUpdated =
2026-04-06 13:54:00+00` on every one of them, identical to the second and 203
days old. The park was open, `season_out_since` was null on all 37, and rides
that run all year — `Frog Hopper`, `LEGONDOL®` — read CLOSED as a statement of
fact on the strength of a stamp from April. `isSourceAbsent` is false for every
one of those rows and `absent_since` is null: the row is listed, so the two
existing signals both say the source is fine.

**The discriminator is the silence, not the age of the current row** — and that
is the measurement that decided the rule. Taken at a fixed 2026-10-23 22:00 UTC
over the 1,107 CLOSED readings in open parks:

| rule | CLOSED → UNKNOWN | share | parks |
|---|---|---|---|
| current row's feed age > 6 h | 892 | 80.6 % | 30 |
| current row's feed age > 24 h | 655 | 59.2 % | 29 |
| current row's feed age > 7 d | 243 | 22.0 % | 24 |
| no observation in 14 d | 228 | 20.6 % | 23 |
| no observation in 14 d, no hard marker | **64** | 5.8 % | 13 |

The last row is the figure the decision was costed against. Re-measured on
2026-10-27 10:00 UTC with `observedReadingsSql` in place — which the original
measurement did not have — the rule fires on **281** rides in parks open by
their own schedule, of which **198 already read UNKNOWN** through the
absent-source rule beside it and **97** are held back by the live-OPERATING
guard. The published change is the remaining **83**.

A gate on the current row encodes the time of day: an evening Halloween event is
twelve hours stale at three in the afternoon, and a water park in October is
forty-five days stale because that is when it shut. Of the 97 frozen readings
sorted by cause on 2026-10-24, 41 were out of season or out of service (CLOSED
is right), 50 were evening events taking fresh readings every night (not silent
at all), and 6 were the real thing — playgrounds and walkthroughs in open parks
stamped once and never touched again, 186 days at Islands of Adventure.

**What is built.** `attractions.source_last_fresh_at` holds the newest reading
whose `timestamp - "lastUpdated"` is inside
`MEASURED_OPERATION_MAX_FEED_AGE_MINUTES` **and which we did not write
ourselves** (`observedReadingsSql`), stamped nightly at 02:45 by
`detect-silent-sources` (`queue-percentile.processor.ts`) over a 30-day window;
`readsUnknownFromSilentSource` reads it on the park, ride and favourites
payloads, with `SOURCE_SILENT_DAYS` = 14 and the hard markers `retired_at` and
the resolved `seasonOutSince`.

**`observedReadingsSql` is load-bearing, not decoration.** Both of our own
writers stamp the freshest possible reading: `writeHourlyHeartbeats` writes
`lastUpdated = now` on a row it copied forward, and reverse-reconciliation does
the same on the CLOSED row it invents. Feed age 0 on both. Without the predicate
the column reports a ride the feed dropped as measured for as long as the
heartbeat carries it, and a source-absent ride for ever. Measured 2026-10-27:
Europa-Park wrote 3,992 reconciliation rows in three days, and leaving them in
put 0 of its 97 rides in the silent set where 58 belong.

Five things about it are easy to get wrong:

- **A row with no observation in the window gets the window's lower bound, not
  null.** Null is reserved for "never scanned", which is the state a deploy
  leaves behind — the other reading would put every ride in the database on
  UNKNOWN until the job first runs, and the rows this rule exists for are
  exactly the ones that never earn a timestamp.
- **A frozen OPERATING is out of scope.** That is the wait-time side of the same
  column (PAR-759), and 391 readings carried it at the measured instant. Any
  current row still claiming the ride runs takes the ride out of this rule.
- **An empty row set is not this rule's business**, the same line
  `isSourceAbsent([])` draws. The rule retracts a PUBLISHED closure; with no
  current row there is nothing published, and the park payload answers that
  with its own optimism one branch further down.
- **The counters have to follow.** `closedAttractions` is derived as the
  remainder of total and operating, and the analytics and geo-listing
  populations count anything non-OPERATING as closed, so an unknown ride was
  served as unknown on the list and counted as closed in the headline beside
  it.
- **An unknown ride stays IN the total**, unlike a retired or out-of-season
  one. It is one of the park's rides today; we just cannot say whether it runs.
  Dropping it from the total instead of from the closed half reports a park
  whose whole feed has frozen — Billund, all 37 — as a park with no
  attractions, which is a worse answer than the closure being retracted. So
  every one of the three counters subtracts BOTH the closed and the unknown
  count from the total, and a reader that subtracts only one would publish the
  ride as *running*.

The read path is a column rather than a query because the query is a hot-path
cost: per park over 14 days it is 50 ms of execution behind **128 ms of
planning** (14 TimescaleDB chunks, 61,944 planning buffers), while the same
question asked once for all 7,631 attractions takes 0.78 s. A Redis key was the
other candidate and cannot work: "never seen fresh" and "key absent" are the
same state, and after an `attraction:*` flush every key is absent.

**Next to the never-observed rule (PAR-1100), not on top of it.** The two never meet on one ride:
this rule needs a current row (it retracts a published CLOSED), the never-observed rule needs none
(it replaces the park payload's optimistic no-data fallback). A ride with a frozen row is this
rule's; a ride with no row and no reading ever is that one's. Both leave a free-flow ride to the
free-flow override and an out-of-season ride on CLOSED.


## A frozen feed on a closed day (PAR-1236, 2026-11-05)

Queue-times and wartezeiten-app never see a park's calendar. On a closed day they can keep repeating
the last open day's waits with fresh timestamps (Energylandia, 2026-11-05: 33 rides, one value each for
12 hours), and the wiki keeps some entries on OPERATING for months (LEGOLAND Billund: source timestamp
2026-04-06). The wait-times poll therefore asks the park's own schedule first
(`src/common/utils/schedule-closed-now.util.ts`): while today's decisive rows say the park is closed
now (no OPERATING window, an hour of grace on both sides), `ConflictResolverService`'s CLOSED→OPERATING
override is taken back, an OPERATING from those two sources alone reads CLOSED, and so does one whose
source timestamp is over a day old. A fresh wiki reading stays; without a decisive schedule row nothing
changes.

Why the schedule and not a timestamp: queue-times' own answer for Energylandia at 23:10 local on that
closed day had 45 of 46 rides `is_open: true`, four Halloween houses at 10–20 minutes, and every ride's
`last_updated` on the same second (22:10:04 UTC), which is queue-times' fetch time, not a reading
([API page](https://queue-times.com/pages/api) documents only the 5-minute refresh and UTC times).
A freshness check on queue-times' timestamp can therefore never see a frozen feed. The wiki's
`lastUpdated` is the entity's own (its live route is uncached), so a months-old one there is a real
stale entry.

The same rows passed the cluster query's health gate, so parks between seasons were listed as silent
feeds. The cluster query now drops a cluster when the park's schedule decided every day since the
silence and opened on none of them; without schedule rows for those days the cluster stays.
