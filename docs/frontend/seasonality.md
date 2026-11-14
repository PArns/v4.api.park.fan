# Seasonal attractions (`isSeasonal`, `seasonMonths`, `isCurrentlyInSeason`)

The season fields on attractions and shows, what each of the three values of
`isCurrentlyInSeason` means, and how the API applies them to statuses, counters and
lists. Open it before branching on a season anywhere. Where the columns come from
(the detector, `MIN_OBSERVED_DAYS`, curation) is in
[seasonality](../architecture/attraction-status/seasonality.md).

```jsonc
{ "isSeasonal": true, "seasonMonths": [1, 11, 12], "isCurrentlyInSeason": false }
```

On every attraction and show of the park payload, on the attraction detail, and on
the rides of `/v1/discovery/nearby`. Curated values win over synced ones
(`resolveCuratedFacts`); a curated season also drops the detector's
`season_out_since` note. A park season that names the ride sits between the two
([below](#a-ride-bound-to-a-park-season)).

## A ride bound to a park season

```jsonc
{ "isSeasonal": true, "isCurrentlyInSeason": false,
  "boundSeason": { "id": "…", "kind": "winter", "name": "Wintertraum", "status": "expected",
                   "startDate": "2026-11-20", "endDate": "2027-01-18" } }
```

A ride a park season of kind `halloween`, `christmas`, `winter`, `summer_nights` or
`special_event` lists in its `attractionIds` runs only on that season's days (its
`dates`, or every day between its bounds). A `cancelled` season binds nothing; an
`expected` one binds. The ride's own curated months or a curated `false` override
the binding.

- `isSeasonal` is `true`, `isCurrentlyInSeason` is `true` on the season's days and
  `false` on every other, and `seasonMonths` is absent (null is stripped).
- `boundSeason` (park payload attractions and the ride detail) is the bound season
  whose bounds hold the park's today, else the next to start, else the last to end.
  It has the fields of [`/seasons`](./park-seasons.md); `name` and `dates` are
  absent when null. Absent on every ride no season binds.
- A client says "only during <name>" with the season's dates where it would say
  "winter only", and treats the ride as it treats any `false`: not one of the
  park's rides today, never closed or down.

How it resolves and who reads it:
[seasonality](../architecture/attraction-status/seasonality.md#a-ride-bound-to-a-park-season).

## `isCurrentlyInSeason` has three values

| value   | means                                                                                                   | a client does                          |
| ------- | ------------------------------------------------------------------------------------------------------- | -------------------------------------- |
| `false` | a bound season does not run today, or months are on file and this is not one — or, without months, the detector recorded when the ride last ran (`season_out_since`) | treat as closed; leave it out of counts |
| `true`  | in season                                                                                               | nothing special                        |
| `null`  | not seasonal, or seasonal and **nothing else known**                                                    | behave exactly as without the field    |

`null` must never collapse into "closed": it would hide a ride nobody has
understood yet, and most seasonal rides sit there, because the detector names no
months for anything watched under 330 days. **Every predicate is `!== false`, never
`=== true`.** The TypeScript rule is `isCurrentlyInSeason()`
(`src/attractions/utils/curated-attraction-facts.util.ts`).

Both this and its SQL twin (`attractionIsOutOfSeason`) read the **park's** month
(`park.timezone`), so on the 31st at 23:30 UTC a park in Tokyo is already in the
next month and one in New York is not.

## What the API does with it

- **Status without live data.** When a ride has no queue row and the park is open,
  `statusWithoutLiveData` (`src/common/utils/no-live-data-status.util.ts`) assumes it
  runs — unless `isCurrentlyInSeason === false`, which closes it. A live row is never
  overruled: a queue row is an observation, the season a description of the past.
- **Counters.** "12 von 45 geöffnet" counts only rides a visitor can queue for today;
  an out-of-season ride is in neither number. The sentence comes from four sources,
  three of them SQL over the whole catalogue, so the rule exists twice:

  | surface                                   | source                                              |
  | ----------------------------------------- | --------------------------------------------------- |
  | park page (`analytics.statistics`)        | `ParkIntegrationService` (TypeScript rule)          |
  | park listings                             | `AnalyticsService.getParkStatistics` (SQL)          |
  | park cards, geo pages (`/v1/discovery/…`) | `DiscoveryService` `LIVE_STATS_SQL` (SQL)           |
  | global realtime stats                     | `AnalyticsService.getGlobalRealtimeStats` (SQL)     |

  The SQL twin is `attractionIsOutOfSeason()` (`src/common/utils/season-window.sql.ts`).
  It was checked against a real Postgres over all 300 combinations of the five
  seasonality columns when it was written, and again over 756 with the park-season
  bindings; its spec (`season-window.sql.spec.ts`) pins only four decisions. **A
  change to either half is a change to both.**
- **Nearby** drops `isCurrentlyInSeason === false` rides from the `in_park` ride list.
- **`/plan/day`** leaves a ride out of a day its season excludes, judged against the
  planned month, or the planned day for a bound ride — see
  [plan-day availability](./plan-day/availability.md).
- **Ride alerts** may be set up out of season and fire only once the ride is not
  `false` — see [push](./push.md#ride-alerts-v1pushride-alerts).

## When the detector is wrong

It reports what the feed has been doing, so a long refurbishment looks like a
season. That is a curation under `/admin/attractions/<id>` (**Saisonal**,
**Betriebsmonate**; a curated `false` takes `season_out_since` with it), or a works
period ([works-period](./works-period.md)) — never a code change.
