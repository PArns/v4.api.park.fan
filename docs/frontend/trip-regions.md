# Trip regions (`/v1/trip-regions/*`)

The contract behind park.fan's Orlando hub and its vacation planner, and behind Parky's
`plan_vacation`, `get_park_events` and `get_travel_time`: a curated region of parks, their days
side by side, and the plan that puts each chosen park on the trip day where its headliners queue
least. Open it before reading one of the three routes or changing the optimizer. Code:
`src/trip-regions/`.
When to go (every window of a trip in a range, ranked by its plan) and the visitors' school
breaks: [trip regions: when](./trip-regions-when.md).

A region is curated in code (`regions.ts`), not synced: which parks belong to Orlando is a planning
decision (LEGOLAND Florida is in Winter Haven, Busch Gardens in Tampa, both day trips from an
Orlando hotel), not a fact any feed carries. Orlando is the only region.

| Route                                    | Key  | Cache                                                   | What                                            |
| ---------------------------------------- | ---- | ------------------------------------------------------- | ----------------------------------------------- |
| `GET /v1/trip-regions/:region`           | none | an hour in the browser, a day at the edge               | parks, groups, hotel areas, travel times        |
| `GET /v1/trip-regions/:region/calendar`  | data | private, an hour; Redis an hour                         | every park's days side by side, with its events |
| `GET /v1/trip-regions/:region/plan`      | data | private, an hour; Redis an hour by the normalized query | the optimized itinerary for a trip              |
| `GET /v1/trip-regions/:region/best-days` | data | private, an hour; built from the calendar's Redis entry | per park its quietest and busiest days          |

Before the routes are deployed, `pnpm trip-regions:dev` serves all four on `localhost:3001` from
production's data through the same modules
([the dev shim](../development/scripts.md#the-trip-region-dev-shim)).

The two keyed routes ask for `public, max-age=3600, stale-while-revalidate=86400` like `/best-days`,
and the key guard makes that private: Cloudflare caches by URL and ignores `X-API-Key`
([data API keys](./api-keys.md)). With `FORECAST_SHARED_CACHE=vary` the edge may keep them five
minutes. A key limited to some parks gets `403 api-key-park-required` here, because a region route
names no single park.

An unknown region is a 404. A bad date, a window in the past, over its length (92 calendar days,
21 trip days) or past a year from today is a 400, and so is an unknown query parameter.

## The region (`GET /v1/trip-regions/orlando`)

```jsonc
{
  "slug": "orlando",
  "name": "Orlando",
  "timezone": "America/New_York",
  "groups": [
    {
      "key": "walt-disney-world",
      "name": "Walt Disney World Resort",
      "kind": "resort",
    },
    {
      "key": "universal-orlando",
      "name": "Universal Orlando Resort",
      "kind": "resort",
    },
    {
      "key": "seaworld-orlando",
      "name": "SeaWorld Parks and Resorts Orlando",
      "kind": "resort",
    },
    { "key": "day-trips", "name": "Day trips", "kind": "day_trip" },
  ],
  "parks": [
    {
      "id": "1e1bfe68-…",
      "slug": "magic-kingdom-park",
      "name": "Magic Kingdom Park",
      "path": "/parks/north-america/united-states/orlando/magic-kingdom-park",
      "group": "walt-disney-world",
      "type": "theme_park", // theme_park | water_park | reservation_park
      "headliner": true, // one of the region's headliner parks (the default pick)
      "latitude": 28.416,
      "longitude": -81.5812,
      "timezone": "America/New_York",
    },
  ],
  "hotelAreas": [
    {
      "key": "disney",
      "name": "Walt Disney World",
      "latitude": 28.3668,
      "longitude": -81.544,
      "perks": ["early_entry"],
    },
  ],
  "venues": [
    {
      "key": "disney-springs",
      "name": "Disney Springs",
      "group": "walt-disney-world",
      "kind": "district", // district | viewpoint | resort_area
      "latitude": 28.3706,
      "longitude": -81.5152,
      "typicalHours": { "opens": "10:00", "closes": "23:00" }, // typical, not the day's
      "free": true,
      "url": "https://disneyworld.disney.go.com/destinations/disney-springs/",
      "notes": "shops and restaurants; entry and parking free",
    },
    {
      "key": "polynesian-beach",
      "name": "Disney's Polynesian Village Resort beach",
      "group": "walt-disney-world",
      "kind": "viewpoint",
      "viewOf": "magic-kingdom-park", // whose fireworks it looks at
      "free": true,
      "url": "https://disneyworld.disney.go.com/resorts/polynesian-resort/",
    },
  ],
  "travel": {
    "source": "osrm",
    "computedOn": "2026-11-12",
    "attribution": "© OpenStreetMap contributors",
    "basis": "free_flow",
    "legs": [
      {
        "from": "magic-kingdom-park",
        "to": "epcot",
        "km": 6.5,
        "minutes": 10,
        "mode": "drive",
        "via": "ttc",
        "transferMinutes": 15,
        "alternatives": [
          { "mode": "bus" },
          { "mode": "monorail", "minutes": 30, "via": "ttc" },
        ],
      },
      {
        "from": "universal-studios-florida",
        "to": "universal-islands-of-adventure",
        "km": 0.8,
        "minutes": 10,
        "mode": "walk",
        "alternatives": [
          { "mode": "walk", "minutes": 10, "via": "citywalk" },
          {
            "mode": "train",
            "minutes": 15,
            "name": "Hogwarts Express",
            "ticket": "park_to_park",
          },
        ],
      },
    ],
  },
}
```

- **The parks:** the four Disney theme parks and Typhoon Lagoon and Blizzard Beach; Universal
  Studios, Islands of Adventure, Epic Universe and Volcano Bay; SeaWorld, Aquatica and Discovery
  Cove (`reservation_park`); LEGOLAND Florida, Busch Gardens and Adventure Island as day trips. The
  headliners are the four Disney and three Universal theme parks. Each resolves by its geographic
  path; a park the database lacks or has retired is left out of every route.
- **A group's name** is the `destinations` row its parks share (marks such as ® stripped), else the
  curated name: the day trips span two destinations and read "Day trips".
- **Travel times are a drive with no traffic**, read once from OpenStreetMap through OSRM and
  stored in code (`travel-matrix.generated.ts`, built by
  `scripts/trip-regions/build-travel-matrix.mjs`). Both directions are averaged, so a leg serves
  both ways; `from`/`to` is a park slug or `hotel:<key>`. Say "about" and name the attribution
  where they are shown.
- **A drive ends at the car park, not the gate.** Magic Kingdom's car park is the Transportation
  and Ticket Center: the leg says `via: "ttc"` and `transferMinutes: 15` (the monorail or ferry,
  a curated estimate). Volcano Bay has no car park of its own (`via: "garage_shuttle"`). Each
  park's car park point and why it was picked are in `regions.ts`; the parks' own centroids snapped
  to service roads behind the parks, which is how a first draft put Volcano Bay 16.7 km from
  Universal Studios.
- **Parks that share a garage** (Universal Studios, Islands of Adventure, Volcano Bay) have no
  drive between them; their leg is the curated walk or shuttle itself (`mode: "walk"` or `"bus"`).
- **`alternatives`** are the resort's own transport, curated in `regions.ts`: Disney buses between
  every Disney hotel and park and park to park; the monorail between Magic Kingdom and EPCOT via
  the TTC; the Skyliner, the Friendship boat and the walk between EPCOT's International Gateway and
  Hollywood Studios; the walk through CityWalk and the Hogwarts Express (`ticket: "park_to_park"`)
  between Universal Studios and Islands of Adventure; Universal's shuttle to Epic Universe and
  Volcano Bay. Their `minutes` are curated estimates rounded to five, waiting included, and absent
  for the buses, whose waits vary too much to name one.
- **Hotel areas** stand for where a visitor sleeps: `disney` (Caribbean Beach, the Skyliner hub in
  the middle of the resort), `universal` (by the Royal Pacific and Sapphire Falls hotels),
  `international_drive`, `lake_buena_vista` (Hotel Plaza Boulevard by Disney Springs) and
  `kissimmee` (US-192). An area's `perks` are those of the hotel a stay there usually means;
  the plan's `hotel` parameter decides which count.
- **Venues** are where an evening goes outside the parks, curated in `regions.ts` with the
  operator's page as `url`: Disney Springs (`district`), Disney's BoardWalk (`resort_area`, a view
  of EPCOT's fireworks), the Polynesian beach (`viewpoint` across the lagoon from Magic Kingdom, on
  the resort monorail; at busy times Disney admits hotel guests and diners only), Universal
  CityWalk (`district`) and ICON Park on International Drive (`district`, for the SeaWorld group).
  `typicalHours` are typical, never the day's, and absent where the venue's own hours vary by
  shop. Their legs are `venue:<key>`, to and from the parks of their own resort only: OSRM's drive,
  with the walk from Universal Studios and Islands of Adventure to CityWalk, the walk from EPCOT's
  International Gateway to the BoardWalk and the monorail from Magic Kingdom to the Polynesian as
  curated alternatives.

## The days (`GET /v1/trip-regions/orlando/calendar?from=&to=`)

`from` defaults to the region's today, `to` to `from` + 30; at most 92 days, from today to a year
ahead. Dates are region-local; every Orlando park runs on `America/New_York`.

```jsonc
{
  "meta": {
    "region": "orlando",
    "timezone": "America/New_York",
    "from": "2026-11-28",
    "to": "2026-12-06",
    "computedAt": "…",
    "forecastUntil": "2027-05-12",
    "publishedHoursUntil": {
      "magic-kingdom-park": "2027-01-20",
      "disneys-typhoon-lagoon-water-park": null,
    },
  },
  "parks": ["magic-kingdom-park", "epcot", "…"],
  "days": [
    {
      "date": "2026-11-29",
      "isHoliday": false,
      "isSchoolVacation": true,
      "holidays": ["Thanksgiving break"],
      "parks": {
        "magic-kingdom-park": {
          "status": "OPERATING", // OPERATING | CLOSED | UNKNOWN
          "hoursPublished": true,
          "opensAt": "2026-11-29T09:00:00-05:00",
          "closesAt": "2026-11-29T18:00:00-05:00", // the day guest's close
          "early": {
            "kind": "early_entry",
            "opensAt": "…08:30…",
            "closesAt": "…09:00…",
          },
          "evening": {
            "kind": "ticketed_event",
            "opensAt": "…19:00…",
            "closesAt": "…24:00…",
          },
          "crowdLevel": "low", // very_low … extreme | unknown | closed
          "headlinerWait": 45, // minutes, see below; absent when unknown
          "basis": "forecast", // forecast | long_range | last_year | none
          "seasons": ["5b0c…"], // ids into `seasons`
        },
        "disneys-typhoon-lagoon-water-park": {
          "status": "CLOSED",
          "hoursPublished": false,
          "crowdLevel": "closed",
          "basis": "none",
          "seasons": ["9e41…"],
          "closedBy": "9e41…",
        },
      },
    },
  ],
  "seasons": [
    {
      "id": "5b0c…",
      "parkSlug": "magic-kingdom-park",
      "kind": "christmas",
      "name": "Mickey's Very Merry Christmas Party",
      "startDate": "2026-11-08",
      "endDate": "2026-12-22",
      "dates": ["…"],
      "status": "confirmed",
      "separateTicket": true,
      "opensAt": "19:00",
      "closesAt": "24:00",
      "url": "https://…",
    },
  ],
}
```

- **`headlinerWait`** is the calendar's `headlinerForecast.avgWait`, rounded to five minutes: the
  daily model predicts each headliner's wait at its busy time of the day (a day peak per ride),
  and this is the mean of those across the park's headliners. It is the optimizer's cost and the
  one number a day is compared by across parks. Call it "the big rides' wait at their busiest";
  it is not a total, not the wait at every hour and not the longest single queue. An hourly curve
  of the same day (`/plan/day`) runs lower for most of the day, so never put the two side by side
  without saying which is which.
- **`crowdLevel`** is the calendar's own level for that park (the day against that park's typical
  day), so two parks' levels say how each compares with itself, not with each other. Compare parks
  by `headlinerWait`.
- **`basis`** says where the day's figures come from:
  - `forecast`: the calendar's forecast, up to 60 days ahead (TFT serves the headliners there,
    `SERVING_TFT_DAYS`).
  - `long_range`: the calendar's forecast past 60 days, CatBoost's long tail; wider error
    ([long-range forecasting](../ml/long-range-forecasting.md)).
  - `last_year`: a day the forecast does not cover (past `forecastUntil`, or a gap in it) and that
    is not CLOSED reads the same weekday a year earlier: `lastYearDays(date).sameWeekday`, the day
    of that weekday nearest the same date a year back, at most three days off. Its `crowdLevel` is
    the level the calendar measured that day (the typical-day-peak rating of
    `calculateCrowdLevelForDate`), its `headlinerWait` the calendar's recorded
    `headlinerForecast.avgWait` for it (`actual: true`): the mean over the park's headliners of each
    ride's day-P90 standby wait, rounded to five. It is an estimate and must be labelled "last
    year", never shown as a forecast. A day that was closed last year gives no figure.
  - `none`: nothing to say.
- **`meta.forecastUntil`** is the last day any of the region's parks carries a forecast, as the
  calendar warmup last read it off the warm months (not a constant; CatBoost reaches about six
  months, as far as a park's published schedule). Absent until the warmup has run once.
- **`early`** is the slot before opening that only some guests get: `early_entry` (Disney resort
  hotels, 30 minutes) or `early_park_admission` (Universal hotels, an hour). **`evening`** is after
  the day guests' close: `extended_evening` (a slot under three hours: Disney's Extended Evening
  Hours for deluxe hotels, EPCOT on Mondays 21:00–23:00, Animal Kingdom on Wednesdays) or
  `ticketed_event` (three hours or more: a party night, a separate ticket). Both are read off the
  schedule feed's `TICKETED_EVENT` and `EXTRA_HOURS` rows by their time of day against the
  OPERATING row: a slot ending by the opening is early, one starting at or after the day guests'
  close is the evening, anything else is left out. `closesAt` is always the day guests' close, so
  a party night shows the early close; a Disney After Hours night (a ticketed event after a
  normal close) does not shorten it.
- **Where the feed has no evening row**, a curated season that runs that date, is `confirmed` or
  `announced`, has `separateTicket` and evening `opensAt`/`closesAt` supplies it
  (`source: "season"`). Until PAR-1290 the sync kept one `TICKETED_EVENT` row per day and lost
  either Early Entry or the party; it now keeps both
  ([schedule sync](../architecture/schedule-sync-and-calendar.md)), and the season stays the
  fallback for a party the feed does not list.
- **`closesEarly: true`** with **`dayGuestCloseAt`** (park offset) marks a day whose day-guest close
  is at least two hours before the park's usual close: the median over its open days within 14
  days of that date that have no ticketed evening (whatever window was asked for), so a party season
  where every other night closes at 18:00 cannot make 18:00 usual; with no such day, the latest close.
- **`best`** names per resort (`walt-disney-world`, `universal-orlando`, `seaworld-orlando`, never the
  day trips) the OPERATING theme park with the lowest day cost ([below](#how-a-day-is-priced));
  ties go to the longer day-guest day, then the lower level. Absent for a resort on `last_year`
  days and where none of its parks has a figure. **`bestWhy`**: `shortest_wait`, or
  `closes_early_elsewhere` when the park with the lowest raw wait lost because it closes early (Magic
  Kingdom's party night on 24 November 2026, 40 minutes but out at 18:00, against Animal Kingdom's
  full day at 45).
- **A day past `publishedHoursUntil`** (the park's last published operating day, `null` when it has
  none) is `UNKNOWN` with `hoursPublished: false`, never `CLOSED`
  ([absent facts](../rules/absent-facts.md)): the calendar's off-season rule is an inference, and
  only a CLOSED row the feed sent keeps the day closed.
- **A curated closure closes a day.** A `closure` season that is `confirmed` or `announced` and runs
  that date makes the day `CLOSED` with `closedBy` naming it, unless the feed says OPERATING: the
  operator's newer statement wins over a curated range. Typhoon Lagoon until 26 January and Volcano
  Bay until 23 March 2027 read this way; an `expected` closure does not close anything.
- **`seasons`** are the curated `park_seasons` rows overlapping the window
  ([park seasons](./park-seasons.md)), in the public season shape plus `parkSlug`. A day lists the
  seasons whose `dates` include it, or whose range covers it when `dates` is null; a cancelled
  season stays in the list and off the days. A range season may run on select days only (the
  Seven Seas Food Festival, whose single days are not published): its range is all the API can
  say. An `expected` season is last year's pattern, not an announcement. An empty list means
  nobody curated one.
- **`holidays`** are the names of the day's local holidays and school breaks; the day's flags are
  the region's, read off its first park. `schoolVacationUnknown: true` past the last stored school
  calendar, as on the park calendar.

## How a day is priced

One cost, `dayCost` in `trip-optimizer.ts`, for the plan, the calendar's `best`, `/best-days` and
Parky. A park's day costs its headliners' **unrounded** mean wait (the calendar's `exactWait`; the
rounded `headlinerWait` is only shown, because a ranking decided by rounding to five is a tie the
rounding invented) times **how much shorter the day guest's day is** than the park's usual one: its
median day-guest hours over its open days within 14 days of the date without a ticketed evening
(the longest day where every day has one; `usualDay`) ÷ that day's hours, at least 1, at most 1.5. So a party night at 18:00 against a usual 23:00 costs up to half as much again,
and is the pick only when its waits are low enough to make up for the hours. A Disney After Hours
night after a normal close costs nothing extra. On top, in the plan only: the hotel's perks take a
fixed share off, `fireworks=want` adds 500 to a night that costs the evening show, and a day
without published hours adds 1000. Water parks and Discovery Cove are a flat day the weather
moves. The constants are `PlanCostModel`.

Two terms price the season, in every route alike (`dayCostParts` names them):

- **Extreme heat** (`HeatModel`), theme parks only. The day's high is the forecast where the
  calendar has one (`weather.basis: "forecast"`, about 16 days), else the climate normal for the
  date (`climate_normal`, the same 2015–2024 ERA5 normals `months[]` reads). A forecast high counts
  from **35 °C**: Magic Kingdom's daily normals top out at 32.1 °C (late June) and run 30.9 °C at the
  June–October median (measured 2026-11-12), so 35 °C is clearly hotter than a usual summer day. A
  normal counts from **31.5 °C**, the top fifth of those June–October normals ("typically very
  hot", late June to mid-July), at half the weight: a normal is a mean over 70 days of ten
  summers, flatter than any one day, and says less about the day a visitor gets. The cost is
  multiplied by 1 + 0.08 per degree above the threshold (half that for a normal), at most 1.4: a
  36 °C forecast is 8 % dearer, 38 °C 24 %. Water parks and Discovery Cove take none: a hot dry
  day is already their good day (× 0.8), and that bonus is not doubled. The calendar's park day
  says `warnings: ["extreme_heat"]`, a best day has the reason `extreme_heat`, a plan day the
  warning.
- **The hurricane season** (`HURRICANE_WEIGHTS`), 1 June to 30 November, adds **8 minutes ×
  the date's weight** to every park's day: weight 1 at the peak, by half month 0.05 / 0.1 in June,
  0.15 / 0.2 in July, 0.4 / 0.8 in August (the peak starting on the 15th), 1 / 1 in September,
  0.8 / 0.4 in October, 0.15 / 0.05 in November. The shape follows the National Hurricane
  Center's climatology (nhc.noaa.gov/climo, 1991–2020: first named storm mid to late June, first
  hurricane early to mid August, peak 10 September, most activity mid-August to mid-October); the
  weights are curated relative to that peak, not NHC figures, and never a forecast. Eight minutes
  at the peak is about a fifth of a typical headliner figure, enough to rank a September window
  behind an equal May one without hiding a genuinely quieter one. Within one date every park
  carries the same minutes, so the calendar's `best` is unchanged; across dates (`/best-days`,
  the plan's days, `/when`'s windows) it counts. A plan day says `hurricane_season`, or
  `hurricane_peak` from 15 August to 15 October.

Neither term touches a shown wait. `headlinerWait` on a day, the plan's totals and a `/when`
window stays the plain figure; the plan's `totals.penalties` and a window's `penalties`
(`{ heat?, hurricane? }`, minutes per park day, one decimal, absent when zero) say what the
ranking added, so a client can say "ranked lower: hurricane peak".

## The best days (`GET /v1/trip-regions/orlando/best-days?from=&to=&limit=5`)

Read off the calendar for the window (default today … +30, at most 92 days), so it says nothing the
calendar does not.

```jsonc
{
  "meta": {
    "region": "orlando",
    "timezone": "America/New_York",
    "from": "…",
    "to": "…",
    "computedAt": "…",
    "forecastUntil": "…",
  },
  "parks": [
    {
      "slug": "magic-kingdom-park",
      "basis": "forecast",
      "quiet": [
        {
          "date": "2026-11-26",
          "headlinerWait": 35,
          "crowdLevel": "low",
          "basis": "forecast",
          "reasons": ["school_break", "early_entry", "quietest_in_window"],
        },
      ],
      "busy": [{ "…": "the 2 dearest days" }],
      "flat": { "min": 40, "max": 45 },
    },
  ],
  "groups": [
    {
      "key": "walt-disney-world",
      "quiet": [
        {
          "date": "2026-11-26",
          "slug": "magic-kingdom-park",
          "headlinerWait": 35,
        },
      ],
    },
  ],
}
```

- **`quiet`** (`limit` long) and **`busy`** (two) rank a park's days by their day cost. A night the
  day guests lose to a party ranks behind every full day whatever its waits. `last_year` days take
  part with their basis; label them.
- **`basis`** is the basis most of the park's ranked days have. **`flat`** is set when the window's
  rounded waits spread less than ten minutes: there is no better day to name.
- **`groups[].quiet`**: per resort, the three dates whose `best` park costs least.
- **Reasons:** `weekday`, `holiday_ends`, `school_break`, `party_night_short_hours`,
  `closes_early` (a short day that is no party night), `extended_evening`, `early_entry` or
  `early_park_admission`, `long_range`, `estimate_last_year`, `quietest_in_window`.

## The plan (`GET /v1/trip-regions/orlando/plan`)

| Parameter    | Example                                              | Meaning                                                                                                                                 |
| ------------ | ---------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `from`, `to` | `2026-11-28`, `2026-12-06`                           | the trip, arrival and departure day included; at most 21 days, from today to a year ahead                                               |
| `parks`      | `magic-kingdom-park:2,epcot,universal-epic-universe` | the parks and how many visits each (default 1, at most 3); default: the headliner parks; at most 16 visits                              |
| `hotel`      | `disney`                                             | `disney`, `disney_deluxe`, `universal`, `universal_premier`, `offsite`: which perks count; default: the area's hotel, else `offsite`    |
| `area`       | `disney`                                             | a `hotelAreas` key: each day's travel is read from there; without it the days carry no travel                                           |
| `arrival`    | `free`                                               | `free` (no park on the arrival day, default) or `park`                                                                                  |
| `departure`  | `free`                                               | the same for the last day                                                                                                               |
| `blocks`     | `1`                                                  | each resort's days in one block, in the best order (default `1`)                                                                        |
| `fireworks`  | `any`                                                | `any` (default): a day is priced by the hours a day guest gets; `want`: a night that costs the evening show only when nothing else fits |
| `split`      | `auto`                                               | `auto` (default): parks listed without a count may get a second visit where the trip has days to spare; `fixed`: never                  |
| `rest`       | `auto`                                               | `auto` (default, below), `none`, or `max:3` (at most three park days in a row)                                                          |
| `ticket`     | `hopper`                                             | `base` (default): one park a day; `hopper`: Disney's Park Hopper or Universal's park-to-park (see [Evenings](#evenings))                |
| `hop`        | `1`                                                  | allow two parks on one day where the trip is shorter than its visits; only with `ticket=hopper`                                         |
| `evenings`   | `1`                                                  | `1` (default): an `evening` on park days that end early ([Evenings](#evenings)); `0`: none                                              |
| `lock`       | `epcot@2026-12-01`                                   | pins, comma-separated; `rest@2026-12-02` pins a rest day                                                                                |

More visits than trip days is a 400 without `hop` (with it, more than two a day). `hop=1` without
`ticket=hopper` is ignored: the plan keeps one park a day and `meta.warnings` says
`hop_needs_hopper`. A lock on a park
not in `parks` or outside the trip is a 400; a lock the plan cannot keep (the park is closed that
day, the day is already pinned, more locks than visits) is listed in `ignoredLocks`.

```jsonc
{
  "meta": {
    "region": "orlando",
    "from": "2026-11-28",
    "to": "2026-12-06",
    "computedAt": "…",
    "objective": "headliner_wait",
    "exact": true,
    "ticket": "base", // base | hopper
    "warnings": ["hop_needs_hopper"], // absent when empty
  },
  "days": [
    {
      "date": "2026-11-28",
      "kind": "arrival", // arrival | park | hop | rest | departure
      "parks": [],
      "reasons": [],
      "warnings": [],
    },
    {
      "date": "2026-11-30",
      "kind": "park",
      "parks": [{ "slug": "universal-epic-universe", "part": "full" }],
      "headlinerWait": 45,
      "crowdLevel": "low",
      "basis": "forecast",
      "travel": {
        "from": "hotel:disney",
        "to": "universal-epic-universe",
        "km": 21.7,
        "minutes": 24,
        "mode": "drive",
      },
      "perks": ["early_park_admission"], // perks this hotel gets that day
      "reasons": ["quietest_in_window", "weekday"],
      "warnings": [],
      "evening": {
        // Epic Universe closes at 18:00; a one-park ticket, so a venue
        "kind": "venue", // hop | venue
        "venue": "universal-citywalk",
        "from": "2026-11-30T18:11:00-05:00", // arrival: the park's close plus the travel
        "to": "2026-12-01T02:00:00-05:00", // CityWalk's typical close
        "travel": { "minutes": 11, "mode": "drive" },
        "highlights": [{ "kind": "dining" }, { "kind": "shopping" }],
        "reasons": ["park_closes_early", "open_later", "free_entry", "short_transfer"],
      },
    },
    {
      "date": "2026-12-01",
      "kind": "hop",
      "parks": [
        {
          "slug": "universal-studios-florida",
          "part": "half",
          "headlinerWait": 40,
          "crowdLevel": "low",
          "basis": "forecast",
          "perks": ["express_included"],
        },
        {
          "slug": "universal-islands-of-adventure",
          "part": "half",
          "headlinerWait": 45,
          "crowdLevel": "low",
          "basis": "forecast",
        },
      ],
      "travel": { "…": "hotel to the first park" },
      "between": {
        "from": "universal-studios-florida",
        "to": "universal-islands-of-adventure",
        "km": 0.8,
        "minutes": 10,
        "mode": "walk",
      },
      "reasons": ["weekday"],
      "warnings": [],
    },
    {
      "date": "2026-12-02",
      "kind": "rest",
      "parks": [],
      "reasons": ["busiest_day_spared", "rest_after_streak"],
      "warnings": [],
    },
  ],
  "parks": [
    {
      "slug": "universal-epic-universe",
      "dates": ["2026-11-30"],
      "alternatives": [
        { "date": "2026-12-03", "headlinerWait": 50, "crowdLevel": "low" },
      ],
    },
  ],
  "totals": {
    "headlinerWait": 41, // mean over the planned visits
    "averageDayWait": 52, // the same parks on an average open day of the window
    "savedMinutesPerRide": 11, // the difference: per headliner ride, not per day
    "expectedError": 15, // the measured forecast error at those leads, per ride
    "orderMatters": true, // savedMinutesPerRide ≥ 3
    "parkDays": 7,
    "visits": 8,
    "restDays": 1,
  },
  "unplaced": [
    { "slug": "disneys-typhoon-lagoon-water-park", "reason": "no_open_day" },
  ],
}
```

**The objective.** Minimize the sum over planned visits of their [day cost](#how-a-day-is-priced).
A hotel perk takes a fixed share off the day it applies: `early_entry` and `early_park_admission`
5 %, `extended_evening` 10 %, `express_included` 60 % (Express Unlimited with a stay at the Royal
Pacific, Hard Rock and Portofino Bay hotels, Universal Studios and Islands of Adventure only). A
park is never put on a `CLOSED` day, and an `UNKNOWN` day only when nothing else is left (with a
warning). With `fireworks=want` a party night that sends day guests home early is used only when
nothing else fits, with `fireworks_missed`; with `any` it carries `party_night_short_hours`.

**Second visits (`split=auto`).** Parks listed without a count, and the default headliners, get a
second visit in the curated order (`secondVisitRank`: Magic Kingdom, EPCOT, Epic Universe, Islands
of Adventure, Hollywood Studios, Universal Studios, Animal Kingdom) while the trip has park days
left after the rest days `rest` keeps and the park has two open days. A written `slug:n` never
changes. `meta.split` lists `{ slug, visits, auto: true }` for each park that got one.

**Repeat visits.** A park takes up to three visits, never two on one day, and at least a day apart:
the search keeps the gap as a rule (branch and bound on the assignment) and breaks it only where no
placement can, with `adjacent_repeat` on the second day. When the trip has too few days for its visits, a repeat visit is
dropped before a park loses its only one (`unplaced`, `no_day_left`).

**Rest days.** Every available day the visits do not need is a rest day, and the optimizer puts the
visits on the quietest days, so the rest days fall on the busiest (`busiest_day_spared`). With
`rest=auto` no more than four park days run in a row; since arrival and departure days break a
run, that is also at least one rest day in every five days of a long trip, as many as the visitor's
chosen visits leave room for. A rest day straight after three or more park days says
`rest_after_streak`. When the chosen visits leave too few days for the rule, it is loosened as
little as possible, `meta.relaxed` says `streak_relaxed` and each day past the fourth in a row
carries `long_streak`; the plan never drops a visit to make a rest day. `rest=max:n` holds n the
same way. `rest=none` keeps no rule and gives the spare days no reasons. `rest@date` pins a rest
day; a free arrival or departure day stays `arrival`/`departure`, also when `arrival=park` left it
empty.

**Hop days.** With `ticket=hopper`, `hop=1` and more visits than days, two theme parks one hopper
ticket covers (Disney's four; Universal Studios and Islands of Adventure, never Epic Universe) whose
leg is under 30 minutes door to door (the drive plus its transfers, or a timed alternative) may
share a day: `kind: "hop"`, each park `part: "half"` with its own figures, `between` the leg. A hop
day costs half of each park plus 10. Water parks and Discovery Cove never hop. Pinned visits are
not paired.

**Locks** fix their cell before the search. A visit pinned to a free arrival or departure day turns
that day into a park day.

**The search** is a min-cost assignment of visits to trip days (Hungarian algorithm), over every
structure the options allow: the pairings of parks for hop days, and with `blocks=1` every order of
the resorts and every split of the trip into their blocks (a block may hold a rest day or a day
trip; day trips go anywhere). Structures are solved cheapest lower bound first and one that cannot
beat the best plan so far is skipped. The rest rule enumerates every pattern of rest days while
there are at most 5000 and searches locally past that; the repeat penalty is applied by a local
search after the assignment. `meta.exact` is true only when the plan is provably the best under the
model: no local search touched a structure that could have beaten it and no limit cut the search
short. When no order of the resorts fits (closed days, locks), the plan is made without blocks and
`meta.relaxed` says `blocks_relaxed`.

**`reasons` and `warnings`** are codes the client translates. Reasons: `quietest_in_window`,
`runner_up_close` (another day is within five minutes), `weekday`, `holiday_ends`, the hotel perks
(`early_entry`, `early_park_admission`, `extended_evening`, `express_included`),
`water_park_weather`, `busiest_day_spared`, `rest_after_streak`, `locked`. Warnings:
`party_night_short_hours`, `fireworks_missed`, `hours_not_published`, `estimate_last_year`, `long_range`,
`no_forecast`, `adjacent_repeat`, `long_streak`, `extreme_heat`, `hurricane_season`, `hurricane_peak`
([how a day is priced](#how-a-day-is-priced)). `unplaced[].reason`: `no_open_day` (closed on every
day of the trip, or fewer open days than visits), `no_day_left`. `ignoredLocks[].reason`:
`outside_trip`, `closed`, `day_taken`, `no_visit_left`.

**`totals`** compare like with like: the planned visits' mean `headlinerWait` against, for the same
visits, the mean over every open day of the window of that park. `savedMinutesPerRide` is that
difference, per headliner ride. Show it as such; it is not hours saved per day. `penalties`
(`{ heat?, hurricane? }`, minutes per park day) is what heat and the hurricane season added to the
plan's cost, never to these waits; absent when zero. `orderMatters` is
false when it is under 3 minutes: any order will do. `expectedError` is the measured error of the
served daily forecast at the planned visits' lead times (`leadTimeMae`,
[reader-facing accuracy](../ml/reader-facing-accuracy.md)), per headliner ride like the saving, so
the two can be read side by side; absent where nothing is measured at those distances (a `last_year`
visit never counts). `parkDays` counts days with a park, `visits` the visits placed.

## Evenings

A park day that ends early gets an `evening`: what the guest can do after it with their ticket.
It is added after the search (`trip-evening.ts`) and never changes a day's park, its cost or the
order. `evenings=0` leaves it out.

**When.** A `park` day whose park, with published hours, lets day guests out before 20:00, or at
least two hours before the latest close among its resort's open parks that day. A party night at
Magic Kingdom (day guests out at 18:00) is one. Never on rest, hop, arrival or departure days, nor
on the trip's first or last day, and never on a day without published hours.

**`ticket=hopper`: a second park** (`kind: "hop"`). One the hopper covers (Disney's four theme
parks; Universal Studios and Islands of Adventure), not the day's park, open at least 90 minutes
after the guest arrives, within 35 minutes door to door, and not on a party night. The travel is
the resort's own timed transport where it has one (the monorail, the Skyliner, the walk), else the
drive with its transfers. Ranked by a night show after arrival, then the lower day level and
headliner wait (the plan has no hourly crowd for the evening), then the shorter transfer. `to` is
that park's day-guest close.

**Universal's park-to-park** covers Universal Studios and Islands of Adventure: the official
store's "2-Park 1-Day Park-to-Park Ticket" names those two, and Epic Universe sells as a one-park
ticket. Whether a multi-day ticket allows a hop into Epic Universe could not be verified, so it is
never hopped to or from.

**`ticket=base`, and the alternatives: a venue** (`kind: "venue"`) of the day park's resort. A
district (Disney Springs, CityWalk, ICON Park) open at least 90 minutes after arrival by its
typical hours (ICON Park has none: its evening ends at 22:00); a fireworks view (the BoardWalk for
EPCOT, the Polynesian beach for Magic Kingdom) only on a night that park has fireworks starting
within two hours of arrival and no party night. A view ranks first, then the shorter transfer. A
view's `to` is half an hour after the fireworks start.

**Alternatives**, at most two: with a hopper, the next hop park and the best venue, then the rest
of either; with a base ticket, the next venues.

**Highlights.** `night_show` and `fireworks` (a hop park's), `fireworks_view` (a viewpoint's),
`dining`, `shopping` (a venue's). A show is named only from the shows data `/plan/day` serves, for
the curated night shows of each park in `regions.ts` (Magic Kingdom's Happily Ever After and Disney
Starlight, EPCOT's Luminous, Hollywood Studios' Fantasmic!, Wonderful World of Animation and
Disney Movie Magic); `at` is its first time after arrival and `source` says whether that is a
published showtime (`scheduled`) or last same weekday's (`projected`), which no feed publishes
ahead. Without a time from that data no show is named and no fireworks view is suggested. On a
party night Magic Kingdom's projected times are not read: day guests are gone and the night's
fireworks are the party's.

**`reasons`**: `park_closes_early` (always first), `open_later`, `night_show`, `free_entry`,
`fireworks_view`, `short_transfer` (15 minutes or less).

## Caching and cost

The days are assembled from the calendar's month caches (`calendar:month:{parkId}:{YYYY-MM}:none`),
the schedule rows and `park_seasons`; no ML call happens per request. Both keyed routes are kept an
hour in Redis (`trip-regions:calendar:v1:*`, `trip-regions:plan:v1:*`), the plan by its normalized
query, so the same trip asked with its parameters in another order is one entry. A plan reads the
calendar for its trip once, and for its evenings the shows of the parks with curated night shows on
each day that gets one (`PlanDayService.showsOn`, four at a time).

The calendar warmup (`CacheWarmupService`, twice a day) takes a region park's months to +13 instead
of +3 (a year, and the two-week reference span past it, so `/when` reads a whole year from the
month caches), records the last forecast day the months returned (`trip-regions:forecast-until:v1:{parkId}`)
and keeps the past months the `last_year` days read (−6 … −2; the current and previous month are
in the ordinary window) as week-long snapshots (`trip-regions:last-year:v1:{parkId}:{YYYY-MM}`).
A snapshot is its own key because a schedule sync evicts every calendar month of a park twice a
day, which is pointless for a month that has passed. What it adds:
[caching](../architecture/caching-strategy.md#cache-warmup).
