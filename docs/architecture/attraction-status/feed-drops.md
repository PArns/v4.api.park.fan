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
