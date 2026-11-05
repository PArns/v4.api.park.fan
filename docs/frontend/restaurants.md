# Restaurants on the park payload (`restaurants[]`)

The contract for the dining facts the park endpoint carries per restaurant: what `cuisineType` and
`requiresReservation` say, and what their absence does not. Open it when rendering a restaurant card.

```jsonc
// GET /v1/parks/:continent/:country/:city/:park → restaurants[]
{
  "id": "…",
  "name": "…",
  "slug": "…",
  "latitude": 28.37,
  "longitude": -81.55,
  "status": "OPERATING",
}
```

Mapped from ThemeParks.wiki in `ThemeParksMapper.mapRestaurant`, stored on `restaurants`, rendered by
`ParkWithAttractionsDto`.

## `cuisineType`

The source's `cuisines`, joined with `, `. ThemeParks.wiki sends none for most restaurants, so the
field is usually **absent**. Absent means nobody said, not "no particular cuisine": render nothing,
never a placeholder such as "International".

## `requiresReservation`

| Value   | Meaning                                                                 |
| ------- | ----------------------------------------------------------------------- |
| `true`  | A source or a curator says a table needs or should have a reservation.  |
| `false` | A source or a curator says walk-ins are fine.                           |
| absent  | **Unknown.** No source says. This is what every restaurant carries now. |

No source sends this fact today. The sync used to write `false` for every restaurant, an invented
"walk-ins welcome" ([absent facts](../rules/absent-facts.md)); since PAR-1135 it writes `null`, which the
null-stripping interceptor turns into an absent key. Rows synced before that change read `false` until
the next children sync rewrites them; a restaurant no feed lists any more keeps its old `false`.

**Client rule:** show the reservation badge only for `true`; show a "walk-in" statement only for an
explicit `false`, and treat absent as "no information" (no badge, no statement). Never default an absent
value to `false`.
