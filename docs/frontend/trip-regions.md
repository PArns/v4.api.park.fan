# Trip regions (`/v1/trip-regions/*`)

The contract behind park.fan's Orlando hub and its vacation planner, and behind Parky's
`plan_vacation`, `get_park_events` and `get_travel_time`: a curated region of parks, their days
side by side, and the plan that puts each chosen park on the trip day where its headliners queue
least. Open it before reading one of the three routes or changing the optimizer. Code:
`src/trip-regions/`.

A region is curated in code (`regions.ts`), not synced: which parks belong to Orlando is a planning
decision (LEGOLAND Florida is in Winter Haven, Busch Gardens in Tampa, both day trips from an
Orlando hotel), not a fact any feed carries. Orlando is the only region.

| Route                                      | Key  | Edge cache          | What                                              |
| ------------------------------------------ | ---- | ------------------- | ------------------------------------------------- |
| `GET /v1/trip-regions/:region`             | none | a day               | parks, groups, hotel areas, travel times          |
| `GET /v1/trip-regions/:region/calendar`    | data | an hour, swr a day  | every park's days side by side, with its events   |
| `GET /v1/trip-regions/:region/plan`        | data | an hour, swr a day  | the optimized itinerary for a trip                |

An unknown region is a 404. A bad date, a range over 92 days or a window past a year is a 400.

## The region (`GET /v1/trip-regions/orlando`)

```jsonc
{
  "slug": "orlando",
  "name": "Orlando",
  "timezone": "America/New_York",
  "groups": [
    { "key": "walt-disney-world", "name": "Walt Disney World Resort", "kind": "resort" },
    { "key": "universal-orlando", "name": "Universal Orlando Resort", "kind": "resort" },
    { "key": "seaworld-orlando", "name": "SeaWorld Orlando", "kind": "resort" },
    { "key": "day-trips", "name": "Day trips", "kind": "day_trip" }
  ],
  "parks": [
    {
      "id": "1e1bfe68-…", "slug": "magic-kingdom-park", "name": "Magic Kingdom Park",
      "path": "/parks/north-america/united-states/orlando/magic-kingdom-park",
      "group": "walt-disney-world",
      "type": "theme_park",          // theme_park | water_park | reservation_park
      "headliner": true,             // one of the region's headliner parks (the default pick)
      "latitude": 28.416, "longitude": -81.5812, "timezone": "America/New_York"
    }
  ],
  "hotelAreas": [
    { "key": "disney", "name": "Walt Disney World", "latitude": 28.385, "longitude": -81.564,
      "perks": ["early_entry"] }
  ],
  "travel": {
    "source": "osrm", "computedOn": "2026-11-12", "attribution": "© OpenStreetMap contributors",
    "basis": "free_flow",
    "legs": [
      { "from": "magic-kingdom-park", "to": "epcot", "km": 8.0, "minutes": 14, "mode": "drive",
        "alternatives": [{ "mode": "monorail", "minutes": 25, "via": "ttc" }] }
    ]
  }
}
```

- **Travel times are a drive with no traffic**, read once from OpenStreetMap through OSRM and
  stored in code (`travel-matrix.generated.ts`, `scripts/trip-regions/build-travel-matrix.mjs`).
  Say "about" and name the attribution where they are shown. `alternatives` are the resort's own
  transport (monorail, Skyliner, boat, bus, Hogwarts Express, walking), curated in `regions.ts`;
  where one exists it is usually what a hotel guest takes. Legs run both ways; `from`/`to` may be a
  park slug or `hotel:<key>`.
- **Magic Kingdom's car park is the Transportation and Ticket Center**, not the gate: a drive to
  Magic Kingdom ends at the TTC and adds the monorail or ferry, which the leg says (`via: "ttc"`).

## The days (`GET /v1/trip-regions/orlando/calendar?from=&to=`)

`from` defaults to the region's today, `to` to `from` + 30; at most 92 days, from today to a year
ahead. Dates are region-local; every Orlando park runs on `America/New_York`.

```jsonc
{
  "meta": { "region": "orlando", "timezone": "America/New_York", "from": "2026-11-28",
            "to": "2026-12-06", "computedAt": "…", "forecastUntil": "2027-05-12",
            "publishedHoursUntil": { "magic-kingdom-park": "2027-01-20" } },
  "parks": ["magic-kingdom-park", "epcot", "…"],
  "days": [
    {
      "date": "2026-11-29",
      "isHoliday": false, "isSchoolVacation": true, "holidays": ["Thanksgiving break"],
      "parks": {
        "magic-kingdom-park": {
          "status": "OPERATING",                    // OPERATING | CLOSED | UNKNOWN
          "hoursPublished": true,
          "opensAt": "2026-11-29T09:00:00-05:00",
          "closesAt": "2026-11-29T18:00:00-05:00", // the day guest's close
          "early": { "kind": "early_entry", "opensAt": "…08:30…", "closesAt": "…09:00…" },
          "evening": { "kind": "ticketed_event", "opensAt": "…19:00…", "closesAt": "…24:00…" },
          "crowdLevel": "low",                      // very_low … extreme | unknown | closed
          "headlinerWait": 45,                      // minutes, see below; absent when unknown
          "basis": "forecast",                      // forecast | long_range | last_year | none
          "seasons": ["s:mvmcp-2026"]               // ids into `seasons`
        }
      }
    }
  ],
  "seasons": [
    { "id": "s:mvmcp-2026", "parkSlug": "magic-kingdom-park", "kind": "christmas",
      "name": "Mickey's Very Merry Christmas Party", "startDate": "2026-11-07",
      "endDate": "2026-12-19", "dates": ["…"], "status": "confirmed", "separateTicket": true,
      "opensAt": "19:00", "closesAt": "24:00", "url": "https://…" }
  ]
}
```

- **`headlinerWait`** is the day's mean expected standby wait of the park's headliners (the
  calendar's `headlinerForecast.avgWait`), rounded to five minutes. It is the optimizer's cost and
  the one number a day is compared by across parks. It is not a total and not a peak.
- **`crowdLevel`** is the calendar's own level for that park (the day against that park's typical
  day), so two parks' levels say how each compares with itself, not with each other. Compare parks
  by `headlinerWait`.
- **`basis`** says where the day's figures come from: `forecast` (the calendar's forecast, up to
  `meta.forecastUntil`, about six months for a region's parks, whose calendar months are warmed that
  far), `long_range` (the long-range model past about 60 days; wider error), `last_year` (past the
  forecast: the level and wait measured on the same weekday a year earlier, an estimate that must be
  labelled as one), `none` (nothing to say). Show `last_year` days as "last year", never as a
  forecast.
- **`early`** is the hour before opening that only some guests get: `early_entry` (Disney resort
  hotels, 30 minutes), `early_park_admission` (Universal hotels, an hour). **`evening`** is after the
  day guests' close: `extended_evening` (Disney deluxe hotels) or `ticketed_event` (a party night:
  a separate ticket, and the park closes early for everyone else). Both are read off the schedule
  feed's ticketed and extra-hours rows by their time of day; where the feed names the event, the
  name is in the matching season.
- **A day past `publishedHoursUntil`** is `UNKNOWN` with `hoursPublished: false`, never `CLOSED`
  ([absent facts](../rules/absent-facts.md)).
- **`seasons`** are the curated `park_seasons` rows overlapping the window
  ([park seasons](./park-seasons.md)), with their `status`: an `expected` season is last year's
  pattern, not an announcement. An empty list means nobody curated one.

## The plan (`GET /v1/trip-regions/orlando/plan`)

| Parameter    | Example                                       | Meaning                                                                                  |
| ------------ | --------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `from`, `to` | `2026-11-28`, `2026-12-06`                    | the trip, arrival and departure day included; at most 21 days                            |
| `parks`      | `magic-kingdom-park:2,epcot,universal-epic-universe` | the parks and how many days each (default 1, at most 3)                           |
| `hotel`      | `disney`                                      | `disney`, `disney_deluxe`, `universal`, `universal_premier`, `offsite`: which perks count |
| `area`       | `disney`                                      | a `hotelAreas` key: travel to each day's park                                            |
| `arrival`    | `free`                                        | `free` (no park on the arrival day, default) or `park`                                   |
| `departure`  | `free`                                        | the same for the last day                                                                |
| `blocks`     | `1`                                           | each resort's days in one block, in the best order                                        |
| `parties`    | `avoid`                                       | `avoid` (default), `allow`, `prefer`: party nights at Magic Kingdom and the like          |
| `rest`       | `auto`                                        | `auto` (spare days on the busiest days, a rest day suggested after four park days in a row), `none`, or `max:3` (at most three park days in a row) |
| `hop`        | `1`                                           | allow two parks on one day where the trip is shorter than the parks                       |
| `lock`       | `epcot@2026-12-01`                            | pins, comma-separated; `rest@2026-12-02` pins a rest day                                  |

```jsonc
{
  "meta": { "region": "orlando", "from": "2026-11-28", "to": "2026-12-06", "computedAt": "…",
            "objective": "headliner_wait", "exact": true },
  "days": [
    {
      "date": "2026-11-28", "kind": "arrival",             // arrival | park | hop | rest | departure
      "parks": []
    },
    {
      "date": "2026-11-30", "kind": "park",
      "parks": [{ "slug": "universal-epic-universe", "part": "full" }],
      "headlinerWait": 45, "crowdLevel": "low", "basis": "forecast",
      "travel": { "from": "hotel:disney", "km": 22.6, "minutes": 28, "mode": "drive" },
      "perks": ["early_park_admission"],                     // perks this hotel gets that day
      "reasons": ["quietest_in_window", "weekday"],
      "warnings": []
    },
    {
      "date": "2026-12-02", "kind": "rest", "parks": [],
      "reasons": ["busiest_day_spared"]
    }
  ],
  "parks": [
    { "slug": "universal-epic-universe", "dates": ["2026-11-30"],
      "alternatives": [{ "date": "2026-12-03", "headlinerWait": 50, "crowdLevel": "low" }] }
  ],
  "totals": {
    "headlinerWait": 41,          // mean over the planned park days
    "averageDayWait": 52,         // the same parks on an average open day of the window
    "savedMinutesPerRide": 11,    // the difference: per headliner ride, not per day
    "parkDays": 7, "restDays": 1
  },
  "unplaced": [{ "slug": "disneys-typhoon-lagoon-water-park", "reason": "no_open_day" }]
}
```

**The objective.** Minimize the sum over planned park days of the cost of that park on that day.
The cost is `headlinerWait`, multiplied up where the day is shorter for the day guest than the
park's median day in the window (a party night that closes at 18:00 costs more unless
`parties=prefer`), and lowered by a fixed share where the chosen hotel's perk applies that day.
A park is never put on a `CLOSED` day, and an `UNKNOWN` day only when nothing else is left (with a
warning). Water parks and Discovery Cove have no meaningful queues: their cost is flat, warm dry
days cheaper, and they fill the days the theme parks spare.

**The search** is exact: a min-cost assignment of park-days to trip days (Hungarian algorithm),
repeated over every block order and boundary when `blocks=1`, and over the pairings of parks that
can share a day when `hop=1` (same resort and a leg under 30 minutes). `rest=max:n` runs an exact
search while the combinations stay under a limit and a local search past it (`meta.exact: false`).
A lock fixes its cell before the search.

**`reasons` and `warnings`** are codes the client translates: `quietest_in_window`,
`runner_up_close` (the next best day is within five minutes), `weekday`, `holiday_ends`,
`early_entry`, `extended_evening`, `busiest_day_spared`, `party_night_short_hours`,
`hours_not_published`, `estimate_last_year`, `long_range`, `water_park_weather`,
`locked`.

**`totals`** compare like with like: the planned days' mean `headlinerWait` against the mean over
every open day of the window for the same parks. `savedMinutesPerRide` is that difference, per
headliner ride. Show it as such; it is not hours saved per day.

## Caching and cost

The days are assembled from the calendar's month caches (`calendar:month:{parkId}:{YYYY-MM}:none`),
the schedule rows and `park_seasons`; no ML call happens per request. The calendar warmup extends
to six months ahead for a region's parks, and reads the last year's months for the six after that
(`CacheWarmupService`). A plan is computed from one calendar read and cached by its full query for
an hour.
