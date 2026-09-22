# What an attraction is for (`attractionKind`)

A hand-decided category on every attraction, on the ride endpoint and on every
ride in a park payload:

```json
{ "attractionKind": "TRANSPORT" }
```

Four values, and they are the whole list: `RIDE`, `TRANSPORT`, `SHOW`,
`WALKTHROUGH`. They come from `ATTRACTION_KIND_VALUES`, which is also what the
Swagger enum and the admin editor's dropdown read, so the three cannot drift
apart.

## Null is not `RIDE`

`null` means nobody has judged this attraction. That is the state of nearly the
whole catalogue — the column started empty on 2026-09-22 and fills one editor
decision at a time. Render nothing for it. A client that treats the absence as
"it is a ride" publishes our own silence as a statement about the park, which
is the rule in [absent facts](../rules/absent-facts.md).

## It is not `attractionType`

`attractionType` is the upstream's free-text label (ThemeParks.wiki,
Queue-Times) and does not reach the public payload at all. This field is a
verdict we make, and the two are independent by design: upstream files water
rides as ATTRACTION and walkthroughs as RIDE, so a label may never seed a kind.
Both columns stay on the row, and the editor shows them side by side.

## Why `TRANSPORT` is the value it was built for

A park railway, a cable car or a monorail builds its queue from the timetable
rather than from demand — a train every 20 minutes collects a line whether or
not anybody is keen. So a 25-minute wait there is not the signal it is on a
coaster, and a ride card that frames it as one misleads.

**Neither the name nor the upstream label can tell you this.** Of 151
attractions whose name contains a transport word, 17 are headliners and exactly
3 of those are transport systems: `Disneyland Railroad Main Street Station`
(Paris) and Ocean Park's two cable cars. The other 14 are coasters and dark
rides that merely sound the part — `Big Thunder Mountain Railroad`,
`Seven Dwarfs Mine Train`, `Ghost Train`, `Mickey & Minnie's Runaway Railway`.
Measured against production on 2026-09-22.

## What it does not change

The headliner tiering. A transport system still counts toward a park's
baseline exactly as it did before, and this field does not re-rank anything —
see the changelog entry for PAR-344 for the two parks where an exclusion would
have had an effect, and why it is the wrong trade. Anything that wants to treat
transport differently is a separate, measurable change with a before-figure per
park.
