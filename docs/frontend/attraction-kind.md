# What an attraction is for (`attractionKind`)

The contract for the curated category on every attraction — `RIDE`, `TRANSPORT`,
`SHOW`, `WALKTHROUGH`, `MAZE` — why `null` is not `RIDE`, why it is independent of
the feed's `attractionType`, and what it does not change. On the ride endpoint and on
every ride in a park payload:

```json
{ "attractionKind": "TRANSPORT" }
```

The five values are `ATTRACTION_KIND_VALUES` (`src/common/types/attraction-kind.type.ts`),
which the Swagger enum and the admin editor's dropdown also read. Editors decide every
value except one: a job may write `MAZE` into an empty cell under a calibrated rule —
see [the `MAZE` rule](../architecture/attraction-status/maze-rule.md).

## Null is not `RIDE`

`null` means nobody has judged the attraction — the state of most of the catalogue
(the column started empty on 2026-09-22). Render nothing for it; treating absence as
"it is a ride" publishes our silence as a statement about the park
([absent facts](../rules/absent-facts.md)).

## It is not `attractionType`

`attractionType` is the upstream's free-text label (ThemeParks.wiki, Queue-Times). It
is not on the ride or park payload (only `/stats` `topAttractions` carries it, as
text). The kind is our verdict and the two are independent by design: upstream files
water rides as ATTRACTION and walkthroughs as RIDE, so a label may never seed a kind.
The editor shows both side by side.

## Why `TRANSPORT` exists

A park railway, cable car or monorail builds its queue from the timetable, not from
demand, so a 25-minute wait there is not the signal it is on a coaster.
**Neither the name nor the upstream label can tell you this**: of 151 attractions
whose name contains a transport word, 17 are headliners and only 3 of those are
transport systems (Disneyland Railroad Main Street Station in Paris and Ocean Park's
two cable cars); the rest are coasters and dark rides that sound the part — Big
Thunder Mountain Railroad, Seven Dwarfs Mine Train (measured 2026-09-22).

## What it does not change

Headliner tiering: a transport system still counts toward a park's baseline, and this
field re-ranks nothing (PAR-344 measured the two parks where an exclusion would have
mattered and rejected it). Treating transport differently is a separate, measured
change.
