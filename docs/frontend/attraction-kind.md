# What an attraction is for (`attractionKind`)

The contract for the category on every attraction — `RIDE`, `TRANSPORT`, `SHOW`,
`WALKTHROUGH`, `MAZE`, `GAME` — who decides it, why `null` is not `RIDE`, why it is
independent of the feed's `attractionType`, and what it does not change. On the ride
endpoint and on every ride in a park payload:

```json
{ "attractionKind": "TRANSPORT", "attractionKindSource": "curated" }
```

The values are `ATTRACTION_KIND_VALUES` (`src/common/types/attraction-kind.type.ts`),
which the Swagger enum and the admin editor's dropdown also read. `GAME` (a midway game,
an arcade, a shooting gallery) came last (PAR-1453); **more may follow, so a client reads
a value it does not know as unknown**. The web and the app already do: the web's badges
test for one value each, the app's `AttractionKind.parse` falls back to `unknown`.

## Who decided it: `attractionKindSource`

- `curated`: an editor's `attraction_kind`, final. The one job that writes that column,
  into a blank only, is [the `MAZE` rule](../architecture/attraction-status/maze-rule.md),
  so a rule-filed maze reads `curated` too.
- `derived`: nobody decided, and park.fan's rule did — see below. It can be wrong, and
  a better rule replaces it.
- `null` with `attractionKind` null.

## Derived where nobody decided

`derived_attraction_kind` is rewritten for every row each night by `detect-seasonal`
(`attractions/utils/attraction-kind-rule.ts`, PAR-1453); the API serves it only where
`attraction_kind` is empty. The first piece of evidence that speaks decides:

1. **The ride profile's glossary type ids**, which a person chose: `park-railway`,
   `monorail`, `sky-ride` are `TRANSPORT`; any coaster term and the ride systems
   (`flat-ride`, `dark-ride`, `drop-tower`, `boat-ride`, `people-mover` …) are `RIDE`;
   `walkthrough`, `funhouse` are `WALKTHROUGH`; `live-show`, `parade`,
   `nighttime-spectacular`, `4d-cinema` are `SHOW`. In that order, so Mystery Castle
   (`drop-tower`, `pre-show`) is a ride. Tags about something else (`animatronics`,
   `pre-show`, `scare-maze`) are skipped.
2. **A published minimum height** is a ride.
3. **The name, only for a row that is not a headliner**: game, show, walkthrough and
   transport words at word boundaries. Headliners are where names lie (next section).

**Queues are not evidence.** At Magic Kingdom, Mickey's PhilharMagic, The Hall of
Presidents, the Laugh Floor and Country Bear post 10–15 minute waits and Swiss Family
Treehouse 5 (payload of 2026-11-27), so "it posts a wait" cannot tell a ride from a
show. The rule never derives `MAZE`. What it cannot settle stays `null`, and the admin's
data-quality report lists those rows per park (`attractionsWithoutKind`); a ride profile
with type tags, a minimum height or an editor's kind takes a row off it.

Measured on the payloads of 2026-11-27: Phantasialand 40 rides → 32 `RIDE` (21 by tag,
11 by height), 1 `WALKTHROUGH`, 7 unknown; Magic Kingdom 38 → 18 `RIDE`, 3 `SHOW`,
3 `TRANSPORT`, 1 `WALKTHROUGH`, 13 unknown.

## In the admin

The admin reads both columns apart, so an editor sees what the rule did without the rule's
value landing in the editor's column:

- **The park's ride list** (`GET /v1/admin/content/parks/:id/attractions`) carries per ride
  `attractionKind` (the editor's column, raw), `derivedAttractionKind` (the rule's, or null)
  and `attractionKindSource` (`curated`, `derived` or null, as above). What a reader is
  shown is `attractionKind ?? derivedAttractionKind`.
- **The ride editor's field view** serves the derived kind as the `attractionKind` field's
  `resolvedValue` where no editor chose one; `curatedValue` stays the editor's column. Null
  written there clears the editor's kind and the ride falls back to the derived one.
- **The data-quality report** lists per park the rows with neither (`attractionsWithoutKind`):
  `withoutKind`, their names, and `withoutKindRefs`, the same rows in the same order as
  `{ id, name, slug }`, so a client acts on a row without matching it by name. The names stay
  for clients that read only them.

([curation](../admin/curation.md), and the MCP tools `curate_attraction`,
`curate_park_attractions` with `adoptDerivedKind`, and `data_quality`:
[admin MCP](../admin/mcp.md).)

## Null is not `RIDE`

`null` means neither an editor nor the rule could tell. Render nothing for it; treating
absence as "it is a ride" publishes our silence as a statement about the park
([absent facts](../rules/absent-facts.md)).

## It is not `attractionType`

`attractionType` is the upstream's free-text label (ThemeParks.wiki, Queue-Times). It
is not on the ride or park payload (only `/stats` `topAttractions` carries it, as
text). The kind is ours and the two are independent by design: upstream files water
rides as ATTRACTION and walkthroughs as RIDE, so a label may never seed a kind. Nor can
the feed's entity type: ThemeParks.wiki's `SHOW` entities are their own records
([shows](./shows.md)); an attraction row is an `ATTRACTION` or a type the sync reads as
one, and the type is not stored.
The editor shows both side by side.

## Why `TRANSPORT` exists

A park railway, cable car or monorail builds its queue from the timetable, not from
demand, so a 25-minute wait there is not the signal it is on a coaster.
**Neither the name nor the upstream label can tell you this for a headliner**: of 151 attractions
whose name contains a transport word, 17 are headliners and only 3 of those are
transport systems (Disneyland Railroad Main Street Station in Paris and Ocean Park's
two cable cars); the rest are coasters and dark rides that sound the part — Big
Thunder Mountain Railroad, Seven Dwarfs Mine Train (measured 2026-09-22).

## What it does not change

Headliner tiering: a transport system still counts toward a park's baseline, and this
field re-ranks nothing (PAR-344 measured the two parks where an exclusion would have
mattered and rejected it). Treating transport differently is a separate, measured
change.
