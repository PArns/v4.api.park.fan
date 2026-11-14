# Trip regions (`/v1/trip-regions/*`)

The contract behind park.fan's region hubs (Orlando's first) and their vacation planner, and behind
Parky's `plan_vacation`, `get_park_events` and `get_travel_time`: a curated region of parks, their
days side by side, and the plan that puts each chosen park on the trip day where its headliners
queue least. Open it before reading one of the routes or changing the optimizer. Code:
`src/trip-regions/`.
When to go (every window of a trip in a range, ranked by its plan) and the visitors' school
breaks: [trip regions: when](./trip-regions-when.md).

A region is curated in code (`regions.ts`), not synced: which parks belong to Orlando is a planning
decision (LEGOLAND Florida is in Winter Haven, Busch Gardens in Tampa, both day trips from an
Orlando hotel), not a fact any feed carries. The regions are `orlando`, `southern-california`,
`japan` and `paris` (release 2.21.0; `orlando` and `japan` are in, `southern-california` and
`paris` follow on the same model, a 404 until they land). Every route below takes any of them; the examples are Orlando's. What differs by region is
the region's own config, never a branch in the code: its hotel kinds and perks, the name of its
early slot, its storm season and heat thresholds, where its travel times come from, the markets
its tickets are sold in, and, in Japan, its bases ([below](#what-a-region-defines)).

| Route                                    | Key  | Cache                                                   | What                                            |
| ---------------------------------------- | ---- | ------------------------------------------------------- | ----------------------------------------------- |
| `GET /v1/trip-regions`                   | none | an hour in the browser, a day at the edge               | every region, for the navigation ([list](#the-list-get-v1trip-regions)) |
| `GET /v1/trip-regions/:region`           | none | an hour in the browser, a day at the edge               | parks, groups, hotel areas, travel times        |
| `GET /v1/trip-regions/:region/calendar`  | data | private, an hour; Redis an hour                         | every park's days side by side, with its events |
| `GET /v1/trip-regions/:region/plan`      | data | private, an hour; Redis an hour by the normalized query | the optimized itinerary for a trip              |
| `GET /v1/trip-regions/:region/best-days` | data | private, an hour; built from the calendar's Redis entry | per park its quietest and busiest days          |
| `GET /v1/trip-regions/:region/tickets`   | none | five minutes at the edge, ten more stale                | the ticket catalog ([tickets](#tickets))        |

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

**Dates are the region's.** "Today", every default window and every date of a route are read in
the region's `timezone` (`America/New_York`, `America/Los_Angeles`, `Asia/Tokyo`,
`Europe/Paris`), never the server's clock ([park timezone](../rules/park-timezone.md)); each park's
day and hours in its own (the same as the region's in every region so far).

**Cache tags.** The frontend tags its reads per region: `trip-region:<slug>` on the region route,
the calendar and the best days, `trip-tickets:<slug>` on the ticket catalog, which the API
revalidates on every catalog edit of that region ([editing](#editing-the-catalog)). The list route
changes only with a deploy.

**Metrics.** Every answer of `/plan`, `/when` and `/best-days` leaves a row of operational facts
for the planner's operators: no date, name, text, address or visitor id, and nothing a client sees
([what the server records](#what-the-server-records)).

## The list (`GET /v1/trip-regions`)

Open, no key; an hour in the browser, a day at the edge, like the region route. For the site's
navigation, which shows each region's park count and how long a trip it plans.

```jsonc
{
  "regions": [
    {
      "slug": "orlando",
      "name": "Orlando",
      "timezone": "America/New_York",
      "parkCount": 16, // the region route's `parks`: the parks the planner places
      "maxTripDays": 21,
      "groups": [
        { "key": "walt-disney-world", "name": "Walt Disney World Resort", "kind": "resort" },
        { "key": "day-trips", "name": "Day trips", "kind": "day_trip" },
      ],
    },
  ],
}
```

The order is `regions.ts`'s: Orlando first. `groups` are named as on the region route.

## What a region defines

Each region in `regions.ts` carries what differs between regions; the routes read it and the
region route serves the parts a client needs:

| In `regions.ts`                        | On the region route              | What                                                                                                                                                       |
| -------------------------------------- | -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `perks` (keys: the hotel kinds)        | `hotels: [{ key, perks }]`       | the `hotel=` values the plan takes and the perks each carries. `offsite` (no perks) in every region; any other value is a 400 naming the region's ones       |
| `perkNames`, a perk rule's `share`     | (the codes)                      | a perk's code, its English name (Parky) and what it takes off a day's cost                                                                                 |
| a group's `earlyKind`                  | (on the calendar's `early.kind`) | what the resort calls its before-opening slot                                                                                                               |
| `weather.heat`, `.storm`, `.notes`     | `weather`                        | heat thresholds, the storm season and the seasons the answers name but do not price ([how a day is priced](#how-a-day-is-priced))                           |
| `weather.climatePark`                  | `weather.climatePark`            | the park whose climate normals stand for the region's months (`/when`)                                                                                      |
| `travel`                               | `travel.source`, `basis`         | OSRM's drive, or legs curated from timetables ([below](#the-region-get-v1trip-regionsorlando))                                                              |
| `ticketMarkets`                        | `ticketMarkets`                  | the market codes its tickets and links name; `home` is sold to anyone, what plan first suggests from ([tickets](#tickets))                                  |
| `bases`, `baseLegs`, `stayRules`       | `bases`, `baseLegs`, `stayRules` | the cities a trip stays in, the moves between them and how both are priced (Japan, Paris; [bases](#bases-a-trip-across-several-cities))                     |
| a park's `code`                        | `parks[].code`                   | a short curated code for tight views (Tokyo Disneyland `DL`, Tokyo DisneySea `DS`, where initials would give both `TD`); absent: the client derives one  |

**The codes a client translates are the region's own.** A perk or early slot is a code like
Orlando's: `early_entry` and `early_park_admission` (Orlando, and `early_entry` for Disneyland
Resort's Early Entry), `happy_entry` (Tokyo Disney Resort's Happy Entry), `extra_magic_time`
(Disneyland Paris' Extra Magic Time): the same shape, a slot before the day guests' opening that
some hotel guests get, priced the same (5 %). The region data names the codes it uses; a client
shows nothing for a code it does not know. Orlando's codes and values are unchanged.

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
        "source": "curated", // not OSRM's: the region's own source is named above
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
  "hotels": [
    { "key": "disney", "perks": ["early_entry"] },
    { "key": "disney_deluxe", "perks": ["early_entry", "extended_evening"] },
    { "key": "universal", "perks": ["early_park_admission"] },
    {
      "key": "universal_premier",
      "perks": ["early_park_admission", "express_included"],
    },
    { "key": "offsite", "perks": [] },
  ],
  "ticketMarkets": { "home": "us", "codes": ["us", "uk", "intl"] },
  "weather": {
    "storm": {
      "kind": "hurricane", // hurricane | typhoon; absent where the region has none
      "from": "06-01",
      "to": "11-30",
      "peak": { "from": "08-15", "to": "10-15" },
    },
    // "notes": [{ "kind": "rainy_season", "from": "06-05", "to": "07-19" }] in Japan
    "climatePark": "magic-kingdom-park",
  },
  "tickets": [
    {
      "id": "wdw-date-based",
      "name": "Walt Disney World date-based ticket",
      "resort": "walt-disney-world",
      "…": "the ticket catalog, see Tickets",
    },
  ],
}
```

- **`tickets`** is the ticket catalog the planner's ticket step and Parky list
  ([tickets](#tickets)). No prices.
- **`hotels`** are the `hotel=` values this region's plan takes, each with the perks a stay there
  carries; a hotel area's `perks` are those of the hotel it usually means. **`ticketMarkets`**:
  the codes a product's and a link's `markets` take in this region. **`weather`**: what the
  region's days are priced and warned with ([how a day is priced](#how-a-day-is-priced)).
- **The parks:** the four Disney theme parks and Typhoon Lagoon and Blizzard Beach; Universal
  Studios, Islands of Adventure, Epic Universe and Volcano Bay; SeaWorld, Aquatica and Discovery
  Cove (`reservation_park`); LEGOLAND Florida, Busch Gardens and Adventure Island as day trips. The
  headliners are the four Disney and three Universal theme parks. Each resolves by its geographic
  path; a park the database lacks or has retired is left out of every route.
- **A group's name** is the `destinations` row its parks share (marks such as ® stripped), else the
  curated name: the day trips span two destinations and read "Day trips".
- **Where travel times come from is the region's (`travel.source`).** `osrm` (Orlando, Southern
  California, Paris): a drive with no traffic, read once from OpenStreetMap through OSRM and stored
  in code (`travel-matrix.generated.ts`, one entry per region, built by
  `scripts/trip-regions/build-travel-matrix.mjs <region>`), `basis: "free_flow"`. `curated` (Japan,
  which travels by train): every leg is curated in `regions.ts` from timetables and the operators'
  pages, door to door with the wait for the train, `basis: "timetable"`, no `attribution`. Inside an
  OSRM region a leg that is not OSRM's drive (the walk between parks that share a garage, a train
  from a Paris hotel) says `source: "curated"`. Both directions are averaged or curated as one, so
  a leg serves both ways; `from`/`to` is a park slug or `hotel:<key>`. Say "about" and name the
  attribution where one is given.
- **`mode`** is `drive` or one of the transport modes: `monorail`, `skyliner`, `boat`, `bus`,
  `walk`, `train` (a resort's own), `shinkansen`, `rail` (a regional or commuter line), `metro`,
  `highway_bus` (a coach between cities), `shuttle` (a hotel's or a park's own). A rail leg may
  carry `name`, the line ("Keiyo Line").
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
- **`early`** is the slot before opening that only some guests get, by the name its resort gives it
  (the group's `earlyKind`): in Orlando `early_entry` (Disney resort hotels, 30 minutes) or
  `early_park_admission` (Universal hotels, an hour); elsewhere `happy_entry`, `extra_magic_time`
  and so on ([what a region defines](#what-a-region-defines)). **`evening`** is after
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
  day trips) the open theme park with the lowest day cost ([below](#how-a-day-is-priced));
  ties go to the longer day-guest day, then the lower level. A day whose hours are not published
  yet (`UNKNOWN`, `hoursPublished: false`) counts when its figure is a `forecast` or `long_range`:
  it is priced as the park's usual day (`usualHours`, else no hours factor), since its real hours
  are not known, and the day says **`bestHoursUnpublished: true`** so the client can add "hours not
  published yet". From January 2027 that is every Disney and Universal day. Absent for a resort on
  `last_year` days and where none of its parks has a figure. **`bestWhy`**: `shortest_wait`, or
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

Two terms price the season, in every route alike (`dayCostParts` names them), each with the
region's own figures (`regions.ts`, `weather`):

- **Extreme heat** (`weather.heat`), theme parks only. The day's high is the forecast where the
  calendar has one (`weather.basis: "forecast"`, about 16 days), else the climate normal for the
  date (`climate_normal`, the same 2015–2024 ERA5 normals `months[]` reads). The thresholds are
  the region's, set the same way for each: the forecast threshold clearly above the region's usual
  summer day, the normal's at the top fifth of its summer's daily normals at its climate park. In
  Orlando a forecast high counts
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
- **The storm season** (`weather.storm`), where the region has one: Orlando's Atlantic
  hurricanes, Japan's typhoons; none in Southern California or Paris. It adds **its peak minutes ×
  the date's weight** to every park's day, weighted by half month to the season's climatology,
  curated relative to its peak and never a forecast. Its codes are the kind's own:
  `hurricane_season` and `hurricane_peak` in Orlando, `typhoon_season` and `typhoon_peak` in Japan,
  and `penalties` names it by its kind. We chose a code per kind over a generic `storm_*` with a
  `kind`: the client translates each code into one sentence, the two seasons differ in months and
  in wording, and Orlando's codes stay as they were. **Orlando's hurricane season**
  (`ATLANTIC_HURRICANES`), 1 June to 30 November, adds **8 minutes × the date's weight** to every
  park's day: weight 1 at the peak, by half month 0.05 / 0.1 in June,
  0.15 / 0.2 in July, 0.4 / 0.8 in August (the peak starting on the 15th), 1 / 1 in September,
  0.8 / 0.4 in October, 0.15 / 0.05 in November. The shape follows the National Hurricane
  Center's climatology (nhc.noaa.gov/climo, 1991–2020: first named storm mid to late June, first
  hurricane early to mid August, peak 10 September, most activity mid-August to mid-October); the
  weights are curated relative to that peak, not NHC figures, and never a forecast. Eight minutes
  at the peak is about a fifth of a typical headliner figure, enough to rank a September window
  behind an equal May one without hiding a genuinely quieter one. Within one date every park
  carries the same minutes, so the calendar's `best` is unchanged; across dates (`/best-days`,
  the plan's days, `/when`'s windows) it counts. A plan day says `hurricane_season`, or
  `hurricane_peak` from 15 August to 15 October. **Japan's typhoons** are set the same way from
  the Japan Meteorological Agency's climatology (most landfalls August to October, the peak in
  September) with the region's data.

Neither term touches a shown wait. `headlinerWait` on a day, the plan's totals and a `/when`
window stays the plain figure; the plan's `totals.penalties` and a window's `penalties`
(`{ heat?, hurricane? }`, or `{ heat?, typhoon? }` in Japan, minutes per park day, one decimal,
absent when zero) say what the ranking added, so a client can say "ranked lower: hurricane peak".

**A season named, not priced** (`weather.notes`): Japan's rainy season (tsuyu, early June to mid
July around Tokyo) makes a day wetter but its parks no emptier or fuller in a way a figure does not
already carry: the forecast and last year's days hold the crowds, and a rain shower is not a reason
to move a park to another day weeks ahead. So it costs nothing, and a plan day and a `/when` window
that touch it say `rainy_season`, a month `rainySeason: true` ([when to go](./trip-regions-when.md)).

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
| `hotel`      | `disney`                                             | a hotel kind of the region (its `hotels`; Orlando: `disney`, `disney_deluxe`, `universal`, `universal_premier`, `offsite`): which perks count; default: the area's hotel, else `offsite` |
| `area`       | `disney`                                             | a `hotelAreas` key: each day's travel is read from there; without it the days carry no travel                                           |
| `arrival`    | `free`                                               | `free` (no park on the arrival day, default) or `park`                                                                                  |
| `departure`  | `free`                                               | the same for the last day                                                                                                               |
| `blocks`     | `1`                                                  | each resort's days in one block, in the best order (default `1`)                                                                        |
| `fireworks`  | `any`                                                | `any` (default): a day is priced by the hours a day guest gets; `want`: a night that costs the evening show only when nothing else fits |
| `split`      | `auto`                                               | `auto` (default): parks listed without a count may get a second visit where the trip has days to spare; `fixed`: never                  |
| `rest`       | `auto`                                               | `auto` (default, below), `none`, or `max:3` (at most three park days in a row)                                                          |
| `tickets`    | `wdw-uk-14-day-magic@2027-02-01,uor-date-based.3.h`  | the tickets the visitor already has, at most 6 ([tickets](#tickets)); the parks none of them covers are planned "plan first"            |
| `ticket`     | `hopper`                                             | plan first: `base` (default): one park a day; `hopper`: Disney's Park Hopper or Universal's park-to-park (see [Evenings](#evenings))    |
| `hop`        | `1`                                                  | allow two parks on one day where the trip or a ticket has fewer days than visits; the day's ticket must allow it                        |
| `evenings`   | `1`                                                  | `1` (default): an `evening` on park days that end early ([Evenings](#evenings)); `0`: none                                              |
| `lock`       | `epcot@2026-12-01`                                   | pins, comma-separated; `rest@2026-12-02` pins a rest day                                                                                |
| `bases`, `startBase`, `endBase` | `tokyo:4@maihama`, `tokyo`, `osaka` | a region with bases only, in place of `area` and `hotel`: the stays the visitor fixes and the trip's ends ([bases](#bases-a-trip-across-several-cities)) |
| `moveBuffer` | `30` | a region with bases only: the visitor's own minutes for a hotel change on top of the train, one of `stayRules.moveBufferOptions` (30, 60, 90, 120); default the region's `moveOverheadMinutes` |

More visits than trip days is a 400 without `hop` (with it, more than two a day). `hop=1` without
a hopper (neither `ticket=hopper` nor an owned ticket with hopping) is ignored: the plan keeps one
park a day and `meta.warnings` says `hop_needs_hopper`. A lock on a park
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
by the region's codes (Orlando: `early_entry`, `early_park_admission`, `extended_evening`,
`express_included`; [what a region defines](#what-a-region-defines)), `water_park_weather`,
`busiest_day_spared`, `rest_after_streak`, `locked`. Warnings: `party_night_short_hours`,
`fireworks_missed`, `hours_not_published`, `estimate_last_year`, `long_range`, `no_forecast`,
`adjacent_repeat`, `long_streak`, `extreme_heat`, the region's storm season (`hurricane_season`,
`hurricane_peak`; `typhoon_season`, `typhoon_peak`) and `rainy_season`
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

**`ticket=hopper`: a second park** (`kind: "hop"`). With owned tickets the day's own ticket
decides instead: a hop evening needs its hopping and a hop set holding both parks
([tickets](#tickets)). One the hopper covers (Disney's four theme
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
view's `to` is half an hour after the fireworks start; a district's `to` is its typical close, but
never past 23:00 (CityWalk stays open to 02:00, and the plan suggests a dinner, not a night out,
before another park day).

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

## Bases: a trip across several cities

Japan's parks lie in four cities, two to three hours apart by train; no hotel serves them all.
Paris has three places to sleep (the Disney hotels, Parc Astérix's, central Paris). A region may
therefore define **bases**: the cities a trip sleeps in. A trip there is a sequence of **stays**,
one base for a run of nights, and the plan searches the stays with the parks. A region without
bases (Orlando, Southern California) has none of the fields, parameters or codes of this
section and plans exactly as before; `bases`, `startBase` and `endBase` there are a 400.

### The region's bases

```jsonc
// GET /v1/trip-regions/japan
{
  "parks": [{ "slug": "tokyo-disneyland", "code": "DL", "…": "…" }], // code: short, curated
  "bases": [
    {
      "key": "tokyo",
      "name": "Tokyo",
      "order": 1, // its place on the route line; absent on every base of a region without one
      "hotelAreas": ["maihama", "disneysea_hotels", "bayside", "shin_urayasu", "tokyo_station", "shinjuku"],
      "defaultArea": "maihama",
      "gateways": [
        { "code": "HND", "name": "Haneda Airport", "kind": "airport" },
        { "code": "NRT", "name": "Narita Airport", "kind": "airport" },
      ],
      "stayNote": "Maihama for both Disney parks; Shinjuku for the buses to Fuji-Q",
      "parks": [
        // the parks a day from here reaches: door to door, one way, curated
        { "slug": "tokyo-disneyland", "minutes": 10, "mode": "walk" },
        { "slug": "tokyo-disneysea", "minutes": 15, "mode": "monorail" },
        { "slug": "fuji-q-highland", "minutes": 110, "mode": "highway_bus" },
      ],
    },
  ],
  "baseLegs": [
    {
      "from": "tokyo",
      "to": "nagoya",
      "minutes": 150, // door to door between the bases' main areas, the wait included
      "mode": "shinkansen",
      "name": "Tokaido Shinkansen Nozomi",
      "parkDay": true, // a park fits the day of this move
      "fromStop": "Tokyo",
      "toStop": "Nagoya",
      "notes": ["reserve_seat", "forward_luggage"], // jr_pass | reserve_seat | forward_luggage
    },
  ],
  "stayRules": {
    "dayTripFreeMinutes": 45,
    "moveOverheadMinutes": 60,
    "parkDayMaxLegMinutes": 150,
    "moveCost": 20,
    "moveStartsAt": "08:00",
    "lastArrivalAt": "23:00",
  },
}
```

- **`stayRules`** override the defaults above per region. Japan moves at 07:30 with a 30-minute
  overhead, because most travellers there send their bags ahead by courier (takkyubin); every other
  region keeps the defaults.
- **`gateways`** are where a trip arrives or leaves: an airport, or a station a long-distance
  train reaches (Marne-la-Vallée–Chessy for the TGV and the Eurostar, `kind: "station"`). The
  client offers them as "you land in" and "you fly home from" and sends the base as `startBase`
  and `endBase`; the API is the one place that knows which airport belongs to which base.
- **Each hotel area belongs to one base**, and a stay's hotel kind (its perks) is its area's.
- **A base's `parks`** are the parks a day from it reaches. A stay whose area has a curated leg to
  the park (`hotel:<area>` in `travel.legs`) uses that leg's minutes instead.
- **`parkDay`** is the leg's own call where it has one (Fujikawaguchiko to Nagoya by bus and
  Kodama is a full travel day), else whether the leg is at most `parkDayMaxLegMinutes`.
- **`stayRules`** are the region's; the values above are the defaults every region gets
  (`DEFAULT_STAY_RULES`, owner-approved starting values) unless it sets its own.

### The plan's parameters

| Parameter   | Example                                         | Meaning                                                                                                                                                                                                 |
| ----------- | ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bases`     | `tokyo:4@maihama,fujikawaguchiko:0,nagoya:1`     | what the visitor fixes, per base: `:n` its nights (`:0` no stay there, its parks as day trips), `@area` its hotel area. A base listed without nights is a stay whose nights the plan picks. The order of the list does not matter |
| `startBase` | `tokyo`                                         | where the trip arrives: the first stay, unless `bases` rules it out with `:0`; then the arrival day moves on from it                                                                                    |
| `endBase`   | `osaka`                                         | where the trip leaves: the last stay, or, where that is elsewhere (a round trip from Tokyo), the departure day moves there                                                                              |

`area` and `hotel` are a 400 in a region with bases: the area goes with its base. Everything else
is the plan's: which bases (those that reach the chosen parks; a base counts when it reaches one
within `dayTripFreeMinutes`, or is named), in which order and with how many nights. A 400 names an
unknown base, an area of another base, an area on a `:0` base, and nights the trip does not have
("Your stays add up to 10 nights; the trip has 9").

**The order follows the route.** Where the bases lie on one line (Japan: Tokyo, Fujikawaguchiko,
Nagoya, Osaka), the stays follow it one way or the other and never double back; landing in Osaka
and leaving from Tokyo reverses it. Where they lie on none (Paris), the stays come in any order the
base legs connect. Each base is stayed in at most once.

### How a day is priced across cities

- **A day within one base** may go to a park the base reaches. Travel past
  `dayTripFreeMinutes` each way is taken off the day's hours and priced like a short day (the hours
  factor of [how a day is priced](#how-a-day-is-priced), at most 1.5 with an early close): Fuji-Q
  from Tokyo (about 2 hours from Shinjuku) loses 2 h 10 of a 12-hour day. The day warns
  `long_day_trip`.
- **A move day** (the morning in one base, the night in the next) is one of three, the cheapest the
  plan can use:
  - **a morning move**, `move.when: "morning"`: the move leaves at `moveStartsAt`, and the day's
    park is one the new base reaches within the free time; the guest gets in at `moveStartsAt` +
    the leg + `moveOverheadMinutes` (luggage, check-in) + the way to the park (`startAt`), and
    the day is priced by the hours left. Warns `move_day`. Only where the leg is a `parkDay`.
  - **an evening move**, `move.when: "evening"`: the day's park is one the old base reaches
    within the free time, the move leaves after its close and must arrive by `lastArrivalAt`; it
    costs the park nothing. Only where the leg is a `parkDay`.
  - **a travel day**, `kind: "travel"`, `move.when: "day"`: no park. It breaks a run of park days
    like a rest day but is not counted as one.
- **Each move adds `moveCost`** (20 minutes of headliner wait): the plan never changes hotels to
  save less. With one Fuji-Q visit the plan keeps it a day trip from Tokyo unless a night by the
  lake pays; a visitor who wants the night names `fujikawaguchiko` in `bases`.
- **A move to or from an airport's city** on the arrival or departure day takes no park.
- **The stay hotel's perks** count where the guest woke: a perk with `needs: "after_first_night"`
  (Tokyo Disney Resort's Happy Entry, which no feed carries and which does not hold on the
  check-in day) holds every morning after the first night, never on the day of a morning move into
  the base; `blackoutDates` are skipped.
- **Resort blocks** (`blocks`) are the stays here: a resort's parks are reached from its base, so
  its days fall in that stay. `blocks` changes nothing in a region with bases.

### The answer

```jsonc
{
  "meta": {
    "stays": [
      { "base": "tokyo", "from": "2027-04-10", "to": "2027-04-14", "nights": 4, "area": "maihama" },
      { "base": "fujikawaguchiko", "from": "2027-04-14", "to": "2027-04-15", "nights": 1, "auto": true },
    ],
    "staySuggestion": {
      // with nights the visitor fixed, the one-night change that saves most
      "nights": { "tokyo": 3 },
      "savedMinutesPerRide": 6,
      "park": "universal-studios-japan",
      "date": "2027-04-16",
    },
  },
  "days": [
    {
      "date": "2027-04-14",
      "kind": "park",
      "base": "fujikawaguchiko",
      "parks": [{ "slug": "fuji-q-highland", "part": "full" }],
      "move": {
        "from": "tokyo",
        "to": "fujikawaguchiko",
        "minutes": 150,
        "mode": "highway_bus",
        "name": "highway bus Busta Shinjuku–Kawaguchiko",
        "when": "morning", // morning | evening | day
        "departAt": "2027-04-14T08:00:00+09:00",
        "arriveAt": "2027-04-14T10:30:00+09:00",
      },
      "startAt": "2027-04-14T11:45:00+09:00", // after a morning move: when the guest gets in
      "warnings": ["move_day"],
    },
    {
      "date": "2027-04-15",
      "kind": "travel",
      "base": "nagoya",
      "parks": [],
      "move": { "from": "fujikawaguchiko", "to": "nagoya", "when": "day", "…": "…" },
    },
  ],
  "totals": { "…": "…", "moves": 3, "travelDays": 1 },
}
```

- **`stays[]`**: `from` the check-in day, `to` the check-out day (the next move day, or the
  departure day); `auto: true` where the plan picked the base or its nights; `area` the visitor's.
- **`days[].base`**: where that night is spent; the departure day's is where it starts.
  **`move`** on a move day; its times are typical (the leg's minutes from `moveStartsAt`, or from
  the park's close plus the way out), never a timetable. A park day's `travel` reads from the
  hotel area of the base its park is reached from.
- **`moveBuffer`** is the buffer the plan used for each hotel change: the visitor's `moveBuffer=`
  or the region's `moveOverheadMinutes`. The client asks the visitor rather than deciding: in
  Japan, bags sent ahead by courier (takkyubin) make 30 minutes enough, carrying them takes about
  60, and a slower pace may want more. The region route lists the choices in
  `stayRules.moveBufferOptions`.
- **`staySuggestion`** appears only where the visitor fixed nights: each fixed stay one night
  shorter or longer, the plan made again, the change that saves at least 3 minutes a ride and
  places every visit; `park` and `date` name the visit that moved to the cheaper day. With every
  night left to the plan there is nothing to suggest: the plan already has the best split.
- **Codes.** Kind `travel`. Warnings `long_day_trip`, `move_day`. `unplaced[].reason`: `no_base`
  (no base the trip may stay in reaches the park). `ignoredLocks[].reason`: `base_unreachable` (no
  stay the trip can make reaches the park that day). `totals.moves` and `totals.travelDays` only in
  a region with bases.

**The search.** The stays are one more structure, like the resort blocks: every set of bases that
reaches the chosen parks, in every order the route allows, with every split of the nights. Each
plan's quick bound (every visit on its cheapest allowed day, days shared, plus the moves) ranks
them; the 400 cheapest are solved, branch and bound as before, and a plan left out whose bound
could beat the result makes `meta.exact` false. `split=auto` keeps the days that can only be a
move out of the room it gives second visits. On the test region (four cities, five parks, ten
days) a plan takes 5–40 ms.

**`/when`** plans each window the same way, `bases`, `startBase` and `endBase` included, and ranks
it with the stays' cost (the hours day trips and moves take, and the moves) on top of its park
days; each window's `plan` carries its `stays` and each day its `base`.

## Tickets

The planner's ticket step asks "Do you already have tickets?". **Plan first** (no `tickets`, or
for the parks no owned ticket covers) keeps the plan as above, resort blocks included, and suggests
a ticket per resort (`meta.ticketSuggestion`). **Owned tickets** (`tickets=`) make the plan keep
each ticket's rules: its days lie inside its window, it is used on no more days than it has, a
park it caps gets no more days, and two parks share a day only on a ticket that hops between them.
The rules come from the operators' terms as researched for late 2026 and 2027 (PAR-1298); where a
rule is uncertain the plan takes the safe side, listed [below](#uncertain-rules). No prices,
anywhere: `links` lead to where a ticket is bought, the operator's shop among them (`shopUrl`).

**The catalog is data, edited in the admin** (`ticket_products`, [editing](#editing-the-catalog)):
products, their purchase links and every rule below change without a deploy.
`src/trip-regions/tickets.ts` is only the seed the table starts from. Code:
`ticket-catalog.service.ts` (the one provider), `trip-tickets.ts` and the optimizer.

### The catalog (`GET /v1/trip-regions/orlando/tickets`)

Open, no key. **Clients read the catalog here.** It is cached five minutes at the edge plus ten of
stale-while-revalidate (`public, max-age=300, s-maxage=300, stale-while-revalidate=600`), so an edit
reaches every reader within 15 minutes, and the frontend's tag `trip-tickets:orlando` is revalidated
on every edit and again after the edge window ([publishing](#editing-the-catalog)). The region route
`GET /v1/trip-regions/orlando` still carries `tickets` for older clients, but its own headers keep it
a day at the edge (`s-maxage=86400`, ours, not a Cloudflare rule), so an edit can take that long to
show there.

```jsonc
{
  "region": "orlando",
  "version": "3f9c2a1b7d4e", // changes with every edit; plans and /when are cached by it
  "updatedAt": "2026-11-20T09:12:00.000Z", // the newest edit
  "tickets": [/* the active products, in the admin's order, each as below */],
}
```

A product:

```jsonc
{
  "id": "uor-date-based", // stable, the contract's key
  "name": "Universal Orlando date-based ticket", // the operator's product name, not translated
  "resort": "universal-orlando", // the group whose operator sells it
  "markets": ["us", "intl"], // the region's ticketMarkets codes; in Orlando us: sold to anyone; intl: by sellers abroad; uk: UK and Ireland residents only
  "parks": [
    "universal-studios-florida",
    "universal-islands-of-adventure",
    "universal-epic-universe",
  ],
  "days": { "min": 1, "max": 5 }, // the lengths on sale; "unlimited" for a pass
  "windowTable": [
    // the days its use may spread over, from the start date (the start is day 1)
    { "days": 1, "window": 1 },
    { "days": 2, "window": 4 },
    { "days": 3, "window": 5 },
    { "days": 4, "window": 6 },
    { "days": 5, "window": 7 },
  ],
  "anchor": "start", // start: the date chosen at purchase; first_use: the first visit
  "hopping": "optional", // none | optional (the `.h` option) | included
  "hopSets": [
    {
      "parks": ["universal-studios-florida", "universal-islands-of-adventure"],
      "ticketDays": { "min": 1, "max": 2 },
    },
    {
      "parks": [
        "universal-studios-florida",
        "universal-islands-of-adventure",
        "universal-epic-universe",
      ],
      "ticketDays": { "min": 3, "max": 5 },
    },
  ],
  "caps": [
    {
      "parks": ["universal-epic-universe"],
      "max": 0, // 0: not covered at that length
      "ticketDays": { "min": 1, "max": 2 },
    },
  ],
  "confidence": "medium", // high | medium | low: how well the researched sources agree
  "description": "One to five park days …", // optional, English, not translated
  "links": [
    {
      "seller": "Universal Orlando Resort", // the seller's own name, not translated
      "url": "https://www.universalorlando.com/web/en/us/tickets-packages/park-tickets",
      "official": true, // the operator's own shop: exactly one per product
      "sponsored": false,
    },
    {
      "seller": "Attraction Tickets",
      "url": "https://www.attractiontickets.com/…",
      "sponsored": true, // an affiliate link: show it as such, rel="sponsored"
      "markets": ["uk"], // absent: everyone
      "locales": ["en"], // absent: every locale
    },
  ],
  "shopUrl": "https://www.universalorlando.com/web/en/us/tickets-packages/park-tickets", // the official link's url
  "notes": "Windows from authorized resellers quoting Universal's terms …",
}
```

- **`links`**: where to buy. Exactly one is the operator's own shop (`official: true`), whose url is
  also `shopUrl`. `sponsored: true` marks an affiliate or paid link: the client labels it and links
  it `rel="sponsored noopener"`. `markets` and `locales` say whom a link is for (a UK seller for
  `uk`, a German page for `de`); a link without them is for everyone. **The array order is the
  display order:** among the links that suit the reader equally (those for their locale or market
  first, then those for everyone), the client shows them in the order given, so the admin decides
  which link comes first. Every url is `https`. Purchase links show the sellers that the visitor
  comes from park.fan: the client renders them with `referrerPolicy="origin"` and never with
  `noreferrer`.
- **`description`**: an optional sentence about the product, in English, as the admin wrote it.

- **`windowTable`** lists, for each length on sale, the calendar days the ticket may be used in,
  counted from its start date. **`windowDays`** stands in for a product without a day count but with
  a window (SeaWorld's Four Parks, 14 days from the first visit). A pass has neither.
- **`hopSets`**: the parks one hop day, or a hop evening, may combine; `ticketDays` limits a set to
  those lengths. Absent with `hopping: "none"`.
- **`caps`**: at most `max` days on which any of `parks` is visited (a hop day between two of them is
  one day); `ticketDays` as above. `max: 0` means the length does not cover those parks.
- **`allowance`** (`{ parks, visits }`, `visits` a number or `"days"`, as many as the ticket's
  days): visits on a separate allowance that use no ticket day, the UK Magic Tickets' and the
  Plus option's water parks.
- **`reservedDayParks`**: parks used only on the start date, Discovery Cove's reserved day.
- **`onSale`** (`[{ from?, to? }]`): the start dates the product is sold for; absent when no limit
  is known. **`until`**: the last day a pass or Fun Card is valid.
- **`pass: true`**: an annual pass or Fun Card, unlimited days, no window. **`blockouts: true`**:
  blockout dates apply to some or all of its versions (the passes, the Fun Cards, the 2-Day
  Park-to-Park + 1 Day Epic promotion); the plan does not check them, and an owned one says
  `blockouts_unchecked`. **`blockoutUrl`**: the operator's page with the blockout calendar (Disney's
  passholder page, which links each pass's calendar). **`reservationUrl`**: where passholders reserve
  their park (Disney's pass).
- **`markets`** (and a link's) are codes of the region's `ticketMarkets`, whose `home` is sold to
  anyone at the operator's own shop: `us` in Orlando, the region's own elsewhere.
- **An evening ticket**: **`entryFrom`** (`"15:00"`, park-local) admits only from that time, and
  **`validDays`** (`weekday`, `weekend`, `holiday`; absent: every day) names the kinds of day it is
  used on: Tokyo Disney Resort's Early Evening Passport (from 15:00, weekends and holidays) and
  Weeknight Passport (from 17:00, weekdays). `weekday` is Monday to Friday that is no public
  holiday, `weekend` Saturday and Sunday, `holiday` a public holiday of the region's calendar. An
  owned one is planned on a day of its kinds only (a visit with none in the trip is `unplaced`,
  `no_ticket_day`), as the evening of that day: the day's park says `part: "evening"`, the day
  `entryFrom`, and it is priced as a day short by the hours before the entry (the hours factor of
  [how a day is priced](#how-a-day-is-priced), at most 1.5), without the early slot's perk. Its
  `headlinerWait` is the day's figure, the busiest time of the whole day, not the evening's. Plan
  first never suggests one: it plans whole days.
- **One park a day, several parks a day.** Tokyo Disney Resort's date-specific 1-Day Passport is a
  one-day product of both parks without hopping (the plan picks the park, a lock keeps it), like a
  1-day Disney ticket in Orlando; Disneyland Paris' 1-park and 2-park tickets are `hopping` `none`
  and `included` (or one product with `optional`, the `.h` 2-park option). The model needs nothing
  more for them.

The v1 catalog (Orlando's seed; each region seeds its own from `tickets.ts`, `TICKET_CATALOGS`): Walt Disney World date-based (1–10 days, `.h` Park Hopper) and with the Plus option
(Water Park and Sports; `.h` Park Hopper Plus), the 4-Park Magic Ticket, the UK 14-Day and 7-Day
Magic Tickets and the annual passes; Universal date-based (1–5 days, `.h` park-to-park), with
Volcano Bay, the 1-Day + 1-Day Epic, the 2-Day Park-to-Park + 1 Day Epic promotion, the Epic
Universe and Volcano Bay 1-day tickets, the UK All Parks and Three Park Adventure Tickets and the
annual passes; SeaWorld's single day, Two Park, Three-Day Three Park, Four Parks 14 days, the UK
3-Visit and Ultimate tickets, the Fun Card and the annual passes; Discovery Cove with and without
its sister parks; Busch Gardens' single day and Fun Card; LEGOLAND's 1- and 2-day tickets, the
Merlin 7 attractions ticket and its pass. A UK tour operator's combo is its component tickets, each
listed on its own. That is the seed; what the route lists is whatever the admin keeps active.

**An archived product** (`active: false` in the admin) leaves the catalog route and is never
suggested, but `tickets=` still takes it, since a shared link or a ticket already bought may name
it: the ticket then warns `product_archived`. A product is never deleted while plans may name it.

### Owned tickets (`tickets=`)

```
tickets=wdw-uk-14-day-magic@2027-02-01,uor-uk-three-park-adventure@2027-02-08
tickets=wdw-date-based.4.h@2027-02-02,uor-date-based.3.h?
```

One item per ticket, comma-separated: `<product>[.<days>][.h|.h?][@<YYYY-MM-DD>]`.

- **`.<days>`**: required where the product sells several lengths, optional where it sells one,
  never on a pass.
- **`.h`**: the hopper option of a product with `hopping: "optional"`; ignored on `included`.
  **`.h?`**: the visitor does not know whether theirs has it. The plan keeps it without hopping (the
  safe side) and the ticket carries `hopper: "unknown"` and `hopperVerdict`, what the hopper would
  add, in the shape of a suggestion's `hopper`.
- **`@date`**: the start chosen at purchase (`anchor: "start"`), the first visit (`first_use`), or
  Discovery Cove's reserved day. It may lie before the trip while the window still reaches into it
  (a ticket started on the travel day); the days before the trip do not count as used. Discovery
  Cove's reserved day must lie in the trip. Without it the plan picks the start that serves the
  trip best and says so (`startChosen`). Never on a pass.

A 400 names the reason: an unknown product, a length the product does not sell, `.h` or `.h?` on a
product without hopping, a date after the trip or whose window ends before it, a date outside
`onSale`, no start in the trip on sale, a date or a length on a pass, more than 6 tickets.

**Plan first's hopper.** `hopper=walt-disney-world,universal-orlando` gives the hopper option to the
plan-first resorts listed (the planner's "Show the plan with Park Hopper", per resort); an unknown
key or a group that is no resort is a 400. `ticket=hopper` still means every resort, and wins.
`meta.hopper` lists the resorts planned with the hopper. With `tickets` an owned ticket decides for
the parks it covers.

**What the plan keeps.**

- **A visit needs a ticket.** A planned park uses the owned ticket that covers it; a park none
  covers is planned "plan first" and gets a suggestion. Where several owned tickets cover a park,
  its visits are shared out in a fixed order (the park with the fewest covering tickets first, then
  the tickets in the order given), not searched.
- **The window.** Every day a ticket is used on lies in `start … start + window − 1`. A ticket
  without `@date` is searched over every start in the trip, its window together with the others.
- **The days.** The days a ticket is used on (a hop day is one) are at most its `days`. With
  `split=auto`, parks of an owned ticket listed without a count get second visits until the ticket's
  days are used, in `secondVisitRank` order and while the trip has room (`meta.split[].ticket`
  names it); fewer days used is `unusedDays`.
- **The caps**, e.g. one visit per park on the 4-Park Magic Ticket. A cap of `max: 0` means the
  ticket does not cover the park at that length: Epic Universe on a 1- or 2-day Universal
  date-based ticket is planned plan first, with the Epic Universe 1-day ticket suggested, never
  left out. The suggestion names the owned tickets of its resort that do not cover it
  (`notCoveredBy`), as it does for a pass.
- **Hopping per ticket.** Two parks share a day only with `hop=1`, on a ticket with hopping whose
  hop set holds both, under 30 minutes apart, theme parks only. The evening's second park follows
  the same ticket. Universal's 3- to 5-day park-to-park and the UK tickets hop to Epic Universe
  too (Universal's terms since 2026); plan first with `ticket=hopper` keeps Universal Studios and
  Islands of Adventure only, as before.
- **Mixing resorts.** Inside their windows, owned tickets' resorts may mix where that saves waiting,
  and only then: the trip is planned once with each resort in one block and once free, and the
  free plan is used where it places more visits, or as many saving at least 3 minutes per ride
  (the plan's "any order will do" threshold; a smaller lead sits inside a forecast's error).
  `meta.mixing` says `{ used, savedMinutesPerRide }`. `blocks=1` written in the query keeps the
  blocks and `blocks=0` mixes, without the comparison (`meta.mixing` then has no
  `savedMinutesPerRide`). `blocks` keeps applying to the resorts on plan first as before.
- **Allowances.** A water-park visit on the UK Magic Tickets' or the Plus option's allowance uses no
  ticket day; it lies in the window and counts against the allowance.
- **When nothing fits**, the plan relaxes in this order and says so: it puts a visit outside its
  ticket's window (`meta.relaxed: ["ticket_window_relaxed"]`, the day warns
  `outside_ticket_window`), and only then leaves it out (`unplaced`, reason `no_ticket_day`). A
  visit beyond a ticket's days or caps is left out with `no_ticket_day` at once.

**Passes** have no window and no day count. Their blockout dates are not known here: the ticket's
`warnings` and, once, `meta.warnings` say `blockouts_unchecked` (so does any product with
`blockouts`); a Disney pass also says
`park_reservation_needed` (passholders reserve their first park). Epic Universe is in no Universal
pass, so an Epic visit with a pass is planned plan first and gets an Epic day ticket suggested.

### Plan first: `meta.ticketSuggestion`

For each resort with park days on plan first, the plan as made (blocks as today) gets a ticket:

- **`product`**: the shortest product sold to everyone (`markets` has the region's home market,
  `us` in Orlando), no evening ticket, and on sale at the
  start that covers the resort's planned days: Disney date-based (with the Plus option where a
  Disney water park is planned); Universal date-based without Epic Universe for one or two days,
  the 1-Day + 1-Day Epic for two days with Epic, date-based from three days; the Epic Universe or
  Volcano Bay 1-day ticket alone; SeaWorld's single day, Two Park, Three-Day Three Park or Four
  Parks for its parks with Busch Gardens and Adventure Island; Discovery Cove (with its sister parks
  where they are planned); LEGOLAND's 1- or 2-day ticket. Only researched products are suggested:
  Aquatica or Adventure Island alone get the smallest multi-park ticket that covers them, with
  its `extraDays`, because their single-day tickets were not researched.
- **`days`** are the resort's park days (a hop day is one, an allowance visit none), **`start`** its
  first, **`end`** the window's last day. Where rest days inside the block need a longer window,
  `days` is the shortest length whose window reaches the last day and **`extraDays`** says by how
  much it exceeds the park days. A resort whose days no single ticket spans gets two suggestions.
- **`hopper`** (products with `hopping: "optional"`): what the hopper option adds, from the plan
  made a second time with it on for that resort and the product's hop sets, run only where it can
  differ:
  - `eveningHops`: park days whose evening becomes a second park ([evenings](#evenings));
  - `hopDays`: hop days that plan makes (only where the visits outnumber the days);
  - `fitsMoreParks`: visits only that plan places;
  - `savedMinutes`: the headliner-wait sum it saves over the visits both place, unpenalized, and
    `savedMinutesPerRide`, the same per headliner ride (the sum over those visits), as `totals`
    reports a saving;
  - `hopDayList` (`[{ date, parks }]`, the two parks of each hop day), `eveningHopList`
    (`[{ date, parks }]`, the day's park, then the evening's), `fitsOnlyWithHopper` (the parks of
    the visits in `fitsMoreParks`, a park once per visit);
  - `recommended`: an evening hop, a park only the hopper fits (`fitsOnlyWithHopper` not
    empty), or `savedMinutesPerRide` of at least 3. A hop day alone does not recommend it, nor a
    saving summed over the trip: 15 minutes over five days is 3 a ride, inside a forecast's error;
  - `why`: `evening_hops`, `hop_days`, `fits_more_parks`, `saves_minutes` (3 minutes a ride or
    more); `no_gain` when not recommended.

  With the hopper already on for that resort (`ticket=hopper`, or the resort in `hopper=`) the
  comparison is against the plan without it.

- **`alternatives`**: on a trip of 10 days or more, the UK 14-day tickets (`reason: "long_stay"`,
  `markets: ["uk"]`: UK and Ireland residents only), on a resort's first suggestion; a promotion
  (the 4-Park Magic Ticket, Universal's 2-Day Park-to-Park + 1 Day Epic) where its dates and caps
  fit the same visits without extra days (`reason: "promotion"`).
- **`id`**: `suggested:<resort>`, a second ticket for the resort `suggested:<resort>:2`.
- **`notCoveredBy`**: the ids of the owned tickets of that resort that do not cover these parks
  (Epic Universe with a pass or a 1- or 2-day Universal ticket); absent when there are none.

### The answer

```jsonc
// GET /v1/trip-regions/orlando/plan?from=2027-02-01&to=2027-02-14
//     &parks=…&tickets=wdw-uk-14-day-magic@2027-02-01
{
  "meta": {
    "…": "…",
    "ticket": "base", // plan first's option, for the parks no owned ticket covers
    "hopper": ["universal-orlando"], // plan-first resorts planned with the hopper; absent when none
    "mixing": { "used": true, "savedMinutesPerRide": 4 }, // with owned tickets
    "tickets": [
      {
        "id": "t1", // t1 … t6 in the order of `tickets=`
        "product": "wdw-uk-14-day-magic",
        "days": 14, // absent for a pass
        "hopper": true,
        "start": "2027-02-01",
        "end": "2027-02-18", // the window's last day; absent for a pass
        "usedDays": 6,
        "unusedDays": 8, // absent when 0 and for a pass
        "unusedReason": "parks_fewer_than_days", // with unusedDays
        "allowanceVisits": 1, // water-park visits on the allowance; absent when none
        "warnings": [], // blockouts_unchecked | park_reservation_needed | covers_no_planned_park | product_archived
      },
    ],
    "ticketSuggestion": [
      {
        "id": "suggested:universal-orlando",
        "resort": "universal-orlando",
        "product": "uor-date-based",
        "days": 3,
        "start": "2027-02-09",
        "end": "2027-02-13",
        "parks": [
          "universal-epic-universe",
          "universal-islands-of-adventure",
          "universal-studios-florida",
        ],
        "hopper": {
          "recommended": true,
          "savedMinutes": 0,
          "savedMinutesPerRide": 0,
          "hopDays": 0,
          "eveningHops": 1,
          "fitsMoreParks": 0,
          "hopDayList": [],
          "eveningHopList": [
            {
              "date": "2027-02-11",
              "parks": [
                "universal-epic-universe",
                "universal-islands-of-adventure",
              ],
            },
          ],
          "fitsOnlyWithHopper": [],
          "why": ["evening_hops"],
        },
        "shopUrl": "https://www.universalorlando.com/web/en/us/tickets-packages/park-tickets",
        "alternatives": [
          {
            "product": "uor-uk-three-park-adventure",
            "days": 14,
            "markets": ["uk", "intl"],
            "reason": "long_stay",
          },
        ],
      },
    ],
  },
  "days": [
    {
      "date": "2027-02-03",
      "kind": "park",
      "parks": [{ "slug": "epcot", "part": "full" }],
      "ticket": "t1", // the owned ticket's id, or a suggestion's id
      "ticketDay": 2, // the 2nd of its 14 days (an every-day ticket: the window's day)
      "…": "…",
    },
    {
      "date": "2027-02-05",
      "kind": "park",
      "parks": [
        { "slug": "disneys-typhoon-lagoon-water-park", "part": "full" },
      ],
      "ticket": "t1",
      "ticketAllowance": true, // on the water-park allowance: no ticketDay
      "…": "…",
    },
    {
      "date": "2027-02-10",
      "kind": "park",
      "parks": [{ "slug": "universal-epic-universe", "part": "full" }],
      "ticket": "suggested:universal-orlando",
      "ticketDay": 2,
      "…": "…",
    },
  ],
}
```

- **`start`** of an owned ticket is the given date, or, with `startChosen: true`, the day the plan
  picked: its first used day, from which the window reaches furthest.
- **`hopper`** of an owned ticket is `true`, `false` or `"unknown"` (`.h?`); with `"unknown"`,
  **`hopperVerdict`** has the shape of a suggestion's `hopper`.
- **Every-day tickets**: a product whose window has as many days as it sells, or that sells no
  day count but has a window (Universal's UK 14-day tickets, SeaWorld's Four Parks, Discovery Cove
  with its sister parks), has no days to count off. Its days' `ticketDay` is the day of the window
  ("day 10 of 14"), counted from `start`, and its `meta.tickets[]` entry gives `usedDays` and
  **`windowDays`** instead of `unusedDays` and `unusedReason`. A suggestion of one numbers its days
  the same way.
- **`unusedReason`**, with `unusedDays`: `parks_fewer_than_days` (the planned parks have every
  visit they get, second visits included, and need fewer days), `rest_days_kept` (the trip's rest
  rule kept the days second visits could have filled), or, where the visits would fill it,
  `window_outside_trip` (the trip holds fewer days of the window than the ticket has: it starts
  before or ends after it) and `no_open_day` (its parks were closed on the days left).
- **Days without a ticket**: arrival, departure and rest days carry no `ticket`; a park with no
  product in the catalog for its days (none in the seed; an edit could leave one) carries none
  either.
- **Codes.** `meta.relaxed`: `ticket_window_relaxed`. Day warnings: `outside_ticket_window`.
  `unplaced[].reason`: `no_ticket_day`. `meta.warnings`: `blockouts_unchecked`. Ticket warnings:
  `blockouts_unchecked`, `park_reservation_needed`, `covers_no_planned_park` (the ticket covers none
  of the planned parks), `product_archived` (the product left the catalog; its rules still apply).
- **Cache.** The plan's Redis key includes the canonical `tickets` value (each item in its full
  form, in the order given), so the same tickets written two ways are one entry, and the catalog's
  `version`, so an edited rule never serves a plan made under the old one (`/when` likewise). The
  answer names that version as `meta.ticketCatalog`.
- **Links in the answer.** Each `meta.tickets[]` entry and each `ticketSuggestion[]` carries its
  product's `links` and `shopUrl` (the official link), so the client needs no second lookup.
- **`/when`** takes `tickets` without dates (each window searches the starts) and `suggest=1` for a
  suggestion per window ([when to go](./trip-regions-when.md#tickets)).

### Editing the catalog

The products live in `ticket_products`, one row per product and region, edited under
`/v1/admin/content/regions/:region/tickets` ([admin endpoints](../admin/endpoints.md)). Reads need any
admin role, writes `editor`.

| Method | Path                      | What                                                              |
| ------ | ------------------------- | ----------------------------------------------------------------- |
| GET    | `…/tickets`               | every product, archived ones too, in order, with `version`        |
| GET    | `…/tickets/:slug`         | one product                                                       |
| POST   | `…/tickets`               | create (201), last in the region; `slug` fixed from then on       |
| PATCH  | `…/tickets/:slug`         | change any field but `slug`; `null` clears an optional one        |
| POST   | `…/tickets/:slug/archive` | `active: false` (a PATCH with `active: true` brings it back)      |
| PUT    | `…/tickets/order`         | `{ slugs: [...] }`: every product of the region, in the new order |
| POST   | `…/tickets/validate`      | the form's live check; `?slug=<slug>` checks an edit of it        |

An admin product is the public one plus `slug` (the public `id`), `region`, `active`, `sortOrder`,
`suggestRank` (the order plan first tries a resort's products in, lower first; `null`: never
suggested), `sources` (`[{ url, title?, accessed?, note? }]`, the research behind it, admin only),
`createdAt`, `updatedAt` and `updatedBy`. A write body is that shape without `id`, `shopUrl` (taken
from the official link) and the stamps.

**Writing.** Problem messages are English and meant to be shown as they come.

- **Where a new product goes.** A POST without `sortOrder` (or with `null`) puts the product last in
  its region: the highest `sortOrder` there plus 10. A `sortOrder` in the body is kept as given, and
  `PUT …/order` numbers the region 10, 20, 30 … again.
- **What a PATCH changes.** A field the body leaves out stays as stored; a field it gives replaces
  the stored one, and the merged product is checked whole. `null` clears an optional field:
  `description`, `notes`, `blockoutUrl`, `reservationUrl`, `until`, `suggestRank` (never suggested),
  `pass`, `blockouts`, `windowDays`, `allowance`, `onSale`, `hopSets`, `caps`, `reservedDayParks`,
  `entryFrom`, `validDays` and `sources`, and inside them a link's `markets`, `locales` and `official`, a source's `title`,
  `accessed` and `note`, an `onSale` range's `from` or `to`, a set's or cap's `ticketDays`. A
  required field sent as `null` is refused at its path (`name`: "is required"); `active` takes only
  `true` or `false`; `sortOrder: null` keeps the product where it is.
- **Checking an edit.** `POST …/validate` checks the body as a new product, whose `slug` must be
  free in the region. With `?slug=<slug>` it checks an edit of that product exactly as the PATCH
  would: the body is merged onto the stored product (a partial body works, `null` clears), the
  product's own slug is no conflict, a different `slug` in the body is refused (`path: "slug"`),
  and an unknown `<slug>` answers 404.

**Checks.** A write is refused with a 400 that lists every problem by its path (`{ statusCode: 400,
message, problems: [{ path: "windowTable[2].window", message }] }`), and `…/validate` answers
`{ ok, problems, dryRun }` with the same list:

- `slug` kebab-case, at most 60 characters, unique in the region; `name` 1–120 characters,
  `description` at most 1,000, `notes` at most 2,000;
- `resort` a group of the region; `markets` a non-empty subset of the region's `ticketMarkets`
  codes (Orlando: `us`, `uk`, `intl`); `parks` region parks, at least one, none twice;
- `entryFrom` a time `HH:MM`, and then `suggestRank` `null`; `validDays` a non-empty subset of
  `weekday`, `weekend`, `holiday`;
- `days` `"unlimited"` or `{ min, max }` with 1 ≤ min ≤ max ≤ 30; `windowTable` exactly one row per
  length from `min` to `max`, each `window` at least its `days`, never shorter than the row before,
  at most 366; `windowDays` only with unlimited days; `pass` only with unlimited days and no
  `windowDays`;
- `hopping: "none"` without `hopSets`, otherwise at least one set; every set at least two of the
  product's `parks`; every cap's parks the product's, `max` from 0 to `days.max`; `ticketDays` inside
  `days`;
- `allowance.parks` region parks outside `parks`, `visits` at least 1 or `"days"`; `reservedDayParks`
  among `parks`;
- `onSale` real dates, `from` ≤ `to`; `until` a real date;
- `links` 1–10, exactly one `official`; `seller` 1–80 characters; `markets` and `locales` from their
  lists (`en`, `de`, `nl`, `fr`, `es`, `it`); every url (links, `blockoutUrl`, `reservationUrl`,
  `sources`) an absolute `https` url of at most 500 characters, without user or password;
- `suggestRank` an integer 0–1000 or `null`; `confidence` `high`, `medium` or `low`.

**The dry run.** A product that passes the checks is planned on a fixed sample trip (10 days from
its first start on sale, a synthetic calendar with every park open): once owned (its shortest length,
with its hopper where it has one) and once plan first with the product in the catalog. A plan that
throws refuses the write (`path: "dryRun.owned"` or `"dryRun.planFirst"`); a product sold for no
start in the coming year skips the owned run and says so.

**Publishing**, in the order of [publish outside-in](../rules/publish-outside-in.md): the row is
written, the region's catalog leaves Redis and the memory of the instance that wrote it (the others
let go of theirs within 30 seconds), the frontend's tag `trip-tickets:<region>` is revalidated, and
again after `CDN_SETTLE_MS`. Every write leaves an audit row (`ticket_product.create`, `.update`,
`.archive`, `.reorder`) with the product before and after.

**The seed.** At boot, a region whose `ticket_products` has no row gets the products of
`tickets.ts`, once (a second instance booting at the same time inserts nothing twice), and the log
says so. Each seeded product gets plain links from `ticket-seed-links.ts`, in this order: the
cheapest authorized seller the research found, then Attraction Tickets' pages for the product
(`locales` set where a page is for one site language), then the official link; a link that repeats
one before it is left out. The research's bundles are not seeded as products. A seeded link is never `sponsored` and carries no affiliate or
tracking parameter. The seed runs only on an empty table: a region that has rows is never touched
again, and every later change, links included, is made in the admin.

### Uncertain rules

Where the research could not settle a rule, the plan keeps the side that cannot plan a day the
ticket does not allow, and the catalog's `confidence` and `notes` say so:

- **Disney's Plus option** (Park Hopper Plus, Water Park and Sports): Disney's FAQ adds a day to the
  window, a Disney planning panelist saw none on the ticket calendar. The plan uses the base window.
- **UK 7-Day Magic Ticket**: the number of water-park visits is not stated; the plan assumes 7.
- **Universal's windows** come from authorized resellers quoting Universal's terms and are anchored
  on the selected date; the 4- and 5-day base tickets take the park-to-park windows (no terms found
  for them).
- **Universal annual passes**: park-to-park is stated only for Premier, so it is the `.h` option.
- **SeaWorld's "within 14 days of the first visit"** is 14 days with the first; **LEGOLAND's
  "within 3 days"** a 4-day span from the start date; **Discovery Cove with its sister parks**:
  every day of it in one 14-day span that holds the reserved day.
- **A 1-day Disney ticket** is bought for one park; the plan picks the park, a lock keeps it.
- **Volcano Bay** is closed from 2026-10-26; Universal says it reopens on or before 2027-04-01 (the
  UK store says 2027-03-24). The curated closure in `park_seasons` (to 2027-03-23) closes those
  days; 24 to 31 March read as days without published hours, which the plan uses only when nothing
  else fits.

## Caching and cost

The days are assembled from the calendar's month caches (`calendar:month:{parkId}:{YYYY-MM}:none`),
the schedule rows and `park_seasons`; no ML call happens per request. Both keyed routes are kept an
hour in Redis (`trip-regions:calendar:v1:*`, `trip-regions:plan:v1:*`), the plan by its normalized
query, so the same trip asked with its parameters in another order is one entry. A plan reads the
calendar for its trip once, and for its evenings the shows of the parks with curated night shows on
each day that gets one (`PlanDayService.showsOn`, four at a time).

**Tickets' cost.** Plan first's suggestion is read off the plan; its hopper verdict plans the trip
a second time only where the hopper can change it (a visit without a day, a hop day). Owned
tickets plan the trip twice, with their resorts in blocks and free (the mixing comparison), and
search their free starts with the structures; inside a block one start per reach of the block is
tried. On the dev shim (production data, 1 to 21 February 2027, the 7 headliners and SeaWorld,
second visits on, upstream reads cached): plan first 0.26–0.33 s; a US 6-day and 4-day ticket with
their starts searched 0.22–0.25 s; two UK 14-day tickets with dates 0.21–0.25 s, without dates
0.62 s; three tickets without dates (Disney, Universal, SeaWorld's Two Park) 0.51 s. All exact.

The calendar warmup (`CacheWarmupService`, twice a day) takes a region park's months to +13 instead
of +3 (a year, and the two-week reference span past it, so `/when` reads a whole year from the
month caches), records the last forecast day the months returned (`trip-regions:forecast-until:v1:{parkId}`)
and keeps the past months the `last_year` days read (−6 … −2; the current and previous month are
in the ordinary window) as week-long snapshots (`trip-regions:last-year:v1:{parkId}:{YYYY-MM}`).
A snapshot is its own key because a schedule sync evicts every calendar month of a park twice a
day, which is pointless for a month that has passed. What it adds:
[caching](../architecture/caching-strategy.md#cache-warmup).

## What the server records

Every answer of `/plan`, `/when` and `/best-days` leaves one row in `trip_request_metrics`, so the
planner can be tuned from data: the MCP tool `trip_planner_ops` reads them
([admin MCP](../admin/mcp.md#the-trip-planners-tools)), beside `trip_planner_report` over the
frontend's `trip_*` events. Nothing of it reaches a client: no field, no header, no change to an
answer. Code: `src/trip-regions/metrics/`.

**Which answers.** Every one the route gives: a 200, a refusal of the query's own validation or of
the services (400, 404), a failure (5xx). Not recorded: a request the data-key guard turns away
(401, 403, 429: it never reaches the route), an answer Cloudflare serves from its cache, and
Parky's `plan_vacation` and `find_trip_dates`, which call the services without the route.

| Column                                                 | What                                                                                                                                                       |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bucket`                                               | the UTC hour the answer left in, never the moment                                                                                                          |
| `region`                                               | the region's slug; `(unknown)` for a slug that names none, since the slug is then the visitor's own text                                                  |
| `route`                                                | `plan`, `when`, `best_days`                                                                                                                                |
| `status`, `error_code`                                 | the HTTP status, and why a request was refused or failed (below); `error_code` is null on a 200                                                           |
| `latency_ms`                                           | from the route taking the request (after the key guard) to its answer, before serialization                                                                |
| `cache`                                                | the answer's own Redis entry: `hit`; `joined`, it waited for the same build in flight in this process; `miss`, built. `/best-days` has none: the calendar's |
| `calendar_cache`                                       | the region calendar a built answer read, the same three states                                                                                             |
| `exact`                                                | `meta.exact` (`/plan`) or `meta.search.exact` (`/when`): the search was proven, not stopped by its time budget                                            |
| `relaxed`                                              | the codes of `meta.relaxed`, sorted; `/when`: `ticket_window_relaxed` when a window answered carries that warning                                         |
| `unplaced_reasons`                                     | one entry per `unplaced[]`, its reason; `/when` names none, so one `unknown` per park its best window left out                                            |
| `trip_days`, `parks`, `parks_named`, `visits`, `locks` | the normalized query in counts: trip length with arrival and departure, parks (named or the headliners), visits with second ones, pinned days             |
| `ticket_kinds`                                         | one entry per owned ticket: `pass`, `evening`, `reserved_day`, `one_day`, `multi_day`, `unknown` (a product the catalog no longer has)                     |
| `hopper`                                               | plan first's hopper resorts, or an owned ticket with the hopper                                                                                            |
| `bases`, `moves`                                       | a region with bases: `meta.stays`' count and `totals.moves`; null in the others                                                                           |
| `range_days`, `windows`, `windows_planned`, `within`   | `/when`: the search range in days, the windows answered, those planned (`meta.search.planned`), a school-break filter. `/best-days`: `range_days`          |

The shape is read from the normalized query, so a request refused while its query is read has
only its route, region, status, code and latency.

**Error codes.** A 404 is `unknown_region`; a 400 of the query's own validation (a malformed date,
`days` out of range, an unknown parameter) is `invalid_query`; a 400 the services raise is told by
its message, which names the parameter at fault: `bad_date`, `window_in_past`, `window_reversed`,
`window_past_horizon`, `window_too_long`, `range_shorter_than_trip`, `unknown_park`,
`park_listed_twice`, `visits_per_park`, `too_many_visits`, `visits_exceed_days`, `bad_hopper`,
`unknown_area`, `bad_hotel`, `bad_lock`, `bad_tickets`, `bad_within`, and `bad_request` for a
message the list does not know yet (`TRIP_ERROR_CODES` in `trip-request-facts.ts`, pinned by its
spec against the messages the code throws). 503 is `unavailable`, any other failure `internal`.
Only the code is kept: the messages carry the visitor's own values.

**What a row never holds**: a date (the trip's, the range's, a lock's), a park, hotel, area or
ticket name, a message, free text, an address, a User-Agent or a visitor id. Every string is the
API's own vocabulary, a region only when `regions.ts` has it.

**How it is written.** The routes wear `@MeasureTripRequest`, an interceptor that opens a trace
(`AsyncLocalStorage`) around the route; the services note into it the caches that served and the
trip's shape, and outside a measured request (Parky's tools) a note does nothing. Each API process
keeps the rows in memory and writes them once a minute, 500 per `INSERT`, and on shutdown; a failed
write keeps its rows for the next one, and past 5,000 rows waiting a row is dropped and counted
(the ops tool's `writer`). Recording never touches the answer.

**Retention and schema.** Rows are kept 90 days (`TRIP_METRICS_RETENTION_DAYS`, as long as the
site statistic's hourly rows) and deleted nightly at 01:25 UTC by `trip-metrics` / `prune`, in
batches of 10,000 ([job queues](../architecture/job-queues.md)). The table came with the entity
(`synchronize`, [the entities are the schema](../rules/the-entities-are-the-schema.md)): additive,
a plain table with an index on `bucket`, its array columns without a default because every insert
names them. A column added later is nullable or has a default.
