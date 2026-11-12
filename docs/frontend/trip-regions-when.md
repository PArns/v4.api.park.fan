# When to go (`/v1/trip-regions/:region/when`)

The contract behind the Orlando hub's "find the best dates" mode, its "best time of year" chapter
and Parky's `find_trip_dates`. A visitor knows which parks and how long, not when. This route ranks
every trip window of that length in a search range by the plan it would get, and returns the best
few with their plans. Open it before reading the route or changing how windows are ranked. The
region, the calendar, the plan and how a day is priced are in [trip regions](./trip-regions.md).
The code is `src/trip-regions/trip-when*.ts`.

```
GET /v1/trip-regions/orlando/when?from=2027-01-01&to=2027-12-31&days=10&parks=…&within=DE-NW
```

It needs the data key and is private like `/plan`. It answers 400 for:

- a search range over 366 days, or one past a year from today;
- `days` outside 3–21;
- a `within` code the holiday data does not know.

## Parameters

| Parameter | Default | Meaning |
| --- | --- | --- |
| `from`, `to` | today … today + 365 | the search range; every window lies inside it |
| `days` | 10 | trip days, arrival and departure day included |
| `parks`, `hotel`, `area`, `arrival`, `departure`, `blocks`, `fireworks`, `rest`, `hop`, `split` | as `/plan` | the trip each window is planned for |
| `within` | none | a school-holiday region (`DE-NW`, `NL-north`, `GB-ENG` …, the codes the holiday data uses); only windows inside that region's school holidays count |
| `limit` | 5 | windows returned, at most 10 |
| `gap` | 7 | minimum days between two returned windows' start dates, so the list is not five shifts of one week |

## Response

```jsonc
{
  "meta": { "region": "orlando", "from": "2027-01-01", "to": "2027-12-31", "days": 10,
            "computedAt": "…", "forecastUntil": "2027-05-12",
            "within": { "code": "DE-NW", "name": "Nordrhein-Westfalen", "periods": [{ "from": "…", "to": "…", "name": "Osterferien" }] } },
  "windows": [
    {
      "rank": 1, "from": "2027-01-12", "to": "2027-01-21",
      "headlinerWait": 38,            // the window's plan: mean over its park days
      "rangeAverageWait": 52,         // the same parks' plan, averaged over every window in the range
      "basis": "forecast",            // forecast | mixed | last_year
      "reasons": ["no_school_break", "festival"],
      "warnings": [],
      "seasons": ["s:festival-of-the-arts-2027"],
      "closesEarlyDays": 0,
      "plan": { "days": [{ "date": "2027-01-12", "kind": "arrival", "parks": [] }, { "date": "2027-01-13", "kind": "park", "parks": ["magic-kingdom-park"] }] }
    }
  ],
  "weeks": [ { "start": "2027-01-04", "headlinerWait": 41, "basis": "forecast" } ],   // ISO weeks in the range
  "seasons": [ /* the seasons the windows name, shape as in /calendar */ ]
}
```

- **A window's figure is the figure of its plan**, the same objective `/plan` minimises, with the
  same day pricing: early closings, perks and fireworks count
  ([how a day is priced](./trip-regions.md)). `headlinerWait` is the plan's mean over its park
  days. `rangeAverageWait` is the mean over every window in the range, so "27 % below the range's
  average" compares like with like.
- **`basis`**: `forecast` when every park day is forecast, `last_year` when every one is last
  year's measured day, else `mixed`. A `last_year` window is a statement about last year and the
  client labels it so.
- **`weeks`** is the year strip: per ISO week in the range, the mean best-park cost of the chosen
  parks, from the same day pricing. It is an overview, not a plan.
- **Reasons**:
  - `no_school_break`
  - `after_holidays`
  - `before_spring_break`
  - `festival` (with `seasons`)
  - `long_days`
  - `mild_weather`, from the climate normals, never a forecast past about 14 days

  **Warnings**:
  - `school_break`
  - `hurricane_season` (June–November, a fact about the season, not a forecast)
  - `party_nights` (with `closesEarlyDays`)
  - `hours_not_published`
  - `estimate_last_year`
  - `water_park_closed`, when a chosen water park is closed for its season in the window

  The client translates the codes and shows nothing for an unknown one.
- **`within`** restricts windows to those that lie completely inside one of the region's
  school-holiday periods in the range. If no period is long enough, the answer is `windows: []`
  with `meta.within.periods` listed, so the client can say why.

## How it is computed

Every start date in the range is a candidate. A cheap lower bound ranks them all: each park's
cheapest days in the window, ignoring that two parks cannot share a day. The full optimizer then
plans the best candidates until the next bound can no longer beat the `limit`-th plan. Windows
closer than `gap` days to a better one are dropped. The days come from the same calendar read as
`/calendar`, and a full year of a region costs one read per park and month, served from the month
caches and the last-year snapshots. A result is cached by its full query for six hours.
