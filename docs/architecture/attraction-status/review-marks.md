# Review marks and triaging retirement candidates

Duplicate and retirement detection are behavioural: they describe what the feed is doing, so every
candidate they surface comes back tomorrow however carefully it was investigated. Open this before
answering a candidate, or before researching a list of them.

## `attraction_review_marks`

Where a human verdict lives (`AttractionReviewMark`, `src/attractions/entities/attraction-review-mark.entity.ts`):

| kind | lifetime | example |
| --- | --- | --- |
| `not_a_duplicate` | permanent | Cedar Creek is a lazy river beside Cedar Creek Mine Ride, a coaster |
| `not_retired` | optional `recheck_after` | Shock Wave has stood unused since March 2026 with no announcement |
| `not_a_maze` | permanent | Fear Acres is a scare zone on Six Flags Fiesta Texas' own event page |

Set `recheck_after` whenever the answer can change: a permanent mark on Shock Wave would hide its
eventual retirement forever; on Marvel Cave, a landmark that plainly operates, it is right. Pairs are
stored in canonical id order behind a CHECK constraint — storing both directions is how the detector
once counted 63 rows for 53 pairs. `kind` is text, not an enum
([the entities are the schema](../../rules/the-entities-are-the-schema.md)).

**Who reads them.** `detect-seasonal` and reverse reconciliation do not: they describe the feed, and
`is_seasonal` is a statement about it. The [maze rule](maze-rule.md) does, because `attraction_kind`
is a judgement about the thing, and its blank-only write cannot otherwise hold "somebody decided no".

## What research is worth

Of 73 attractions whose feed went quiet, 16 were genuinely gone; 23 still operated, 5 were in
refurbishment, 5 seasonal, 5 not attractions at all, 2 renamed. Acting on the feed signal alone would
have deleted 57 existing rides.

## Sort the candidates before researching them

The list comes from `AttractionReviewService.findRetirementCandidates`
(`GET /v1/admin/retirement-candidates`): attractions **we** no longer see OPERATING. Whether upstream still delivers
real rows after the quiet day decides how much research a candidate is worth, and
`observedReadingsSql()` already expresses it — use it, never a hand-rolled predicate (one missed
`system-heartbeat` and credited a ride with 11 readings it does not have):

```ts
count(*) FILTER (WHERE ${observedReadingsSql("qd")})   -- rows since the quiet day
```

On 2026-09-28, 71 candidates split three ways: 45 upstream still delivering (CLOSED, REFURBISHMENT or
DOWN), 8 upstream dropped, 18 upstream stopped later. **No class is a verdict:** the "dropped" class
held a castle and a steam train that plainly operate beside one real demolition, and nine of the 18
shared one last-row date (a source change). What the split buys is the upstream's own verdict where
it has one — six said `REFURBISHMENT`. Result: 3 retired, 68 marked `not_retired`.

### The quiet day is not the closing date

All three retirements carry a `retired_at` earlier than our last OPERATING row: Kumba by one feed-lag
day, *Fast & Furious - Supercharged* by eleven (four days of real wiki waits after closing, then a
week of Queue-Times rows with a wait of exactly 0), *Maximus' Blitz Bahn* by two months — caused by a
single stray OPERATING row with a 5-minute wait among 27 CLOSED that day.
`max(timestamp) FILTER (WHERE status = 'OPERATING')` has no defence against one row. **The closing
date comes from the source that states it** (the operator, or press citing it); the feed only
corroborates, using the last *plausible* operating day (several rows, a wait above zero). The source
goes into `retired_reason`, which the public attraction endpoint serves.

### Queue variants mis-mapped as attractions

A single-rider queue carried as its own attraction (*Expedition Everest … Single Rider*) is a
duplicate, not a retirement. Eight existed at three parks (Animal Kingdom, Islands of Adventure,
Universal Studios Florida), each with its own `externalId` and `queue_times_entity_id`, writing
`queueType = 'STANDBY'`, beside a live parent with `has_single_rider = true`. They were retired as
`reclassified` (PAR-965; [reclassification](reclassification-and-absence.md)). The creation guard
belongs in `syncQtAttraction`, beside its show-name guard (PAR-37).

### A cheaper triage for the never-OPERATING net

Attractions that never report OPERATING in a park whose feed works (floor: three currently reporting
rides) numbered **736** on 2026-09-28. Sorted by free signals:

| bucket | rows | signal |
| --- | --- | --- |
| free-flow | 12 | `open_with_park` — never OPERATING is normal |
| Halloween / Christmas / winter / water / playground | 31 / 23 / 10 / 24 / 9 | name vocabulary |
| never measured by any source | 64 | a mapping question, not a retirement one |
| upstream dropped it over 60 days ago | 312 | needs research |
| remainder | 251 | needs research (Movie Park's *TMNT* ride: 4,737 rows, all CLOSED, ride open) |

The buckets order the work, they do not eliminate it. The name vocabulary is a sorting signal, never
a verdict.
