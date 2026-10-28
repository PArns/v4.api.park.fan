# Shows on the park payload (`shows[]`)

The contract for the shows the park endpoint lists: which shows appear, what `status` and the
showtimes mean while the park is open and while it is closed, and what the season fields can and
cannot say. Open it when rendering a park's show programme. A day's programme in the planner, with
projected times for future days: [`/plan/day` shows](./plan-day/shows.md). Following a show by push:
[push](./push.md).

```jsonc
// GET /v1/parks/:continent/:country/:city/:park → shows[]
{ "id": "…", "name": "Big Moments", "slug": "big-moments", "latitude": 48.266, "longitude": 7.722,
  "status": "OPERATING",
  "showtimes": [{ "startTime": "2026-08-29T12:30:00+02:00" }, { "startTime": "2026-08-29T14:30:00+02:00" }],
  "isSeasonal": false }
```

Built in `ParkIntegrationService.buildIntegratedResponse`; the show rows come from
`ParksService.loadParkRelations`, live data from `ShowsService`.

## Which shows are listed

- **Only shows with at least one showtime today.** A show without times, or whose live data is more
  than 48 hours old, is dropped from the list. Its absence says nothing about whether it still exists,
  and before the first snapshot of the park-local day arrives the list can be empty.
- Retired shows are never listed. Two shows of one name are one entry, chosen as for rides
  (`outranksNameDuplicate`).

## `status` and `showtimes`

| Park `status` | Show `status` | `showtimes` |
| --- | --- | --- |
| `OPERATING` | the show's newest reading (`OPERATING`, `CLOSED`, …) | from that reading; a time the feed dated another day is moved onto the park-local today |
| anything else | always `CLOSED` | the times of the show's newest `OPERATING` reading of the park-local today |

- **`CLOSED` next to times is today's programme of a park that is not open right now** (before opening
  or after closing), not a cancelled show.
- **Every time of the day is listed, past ones included.** Compare with the clock yourself.
- **Parse `startTime`, never cut the clock time out of the string.** It is ISO 8601 as the feed sent it,
  or in UTC (`…Z`) once `ShowsService.projectShowtimesToToday` has moved it onto today.
- On this payload a showtime has `startTime` only; `type` and `endTime` are dropped. The favorites list
  keeps them ([favorites](./favorites.md)).
- The list is day-scoped data inside a payload the frontend keeps for 24 hours, so the API sends a
  revalidation when a park opens or closes
  ([caching](../architecture/caching-strategy.md#park-opening-and-closing-parkstatusrevalidationservice)).

## Season fields

- `isSeasonal` and `seasonMonths` come from the show row, set by the nightly `detect-seasonal` job from
  how long a show's feed stopped updating. Shows have no curated columns, so no editor can correct them.
- **`isCurrentlyInSeason` is computed from the server's UTC month** (`ParkWithAttractionsDto`), not the
  park's, so around a month boundary it can be a day off for parks far from UTC. It is absent for a
  show that is not seasonal and for a seasonal one without months.
- A show out of season is still listed when it has times today: the times win.
