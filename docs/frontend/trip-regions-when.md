# When to go (`/v1/trip-regions/:region/when`, `/school-breaks`)

The contract behind the Orlando hub's "find the best dates" mode, the best-time landing pages and
Parky's `find_trip_dates`. A visitor knows which parks and how long, not when. `/when` ranks every
trip window of that length in a search range by the plan it would get, and returns the best few
with their plans and the year strip by week and month. `/school-breaks` lists the school breaks
of the visitors' home regions by the codes `within` takes. Open this page before reading either
route or changing how windows are ranked. The region, the calendar, the plan and how a day is
priced are in [trip regions](./trip-regions.md). Code: `src/trip-regions/trip-when.ts` (the
search), `trip-when.assembly.ts` (query to input), `trip-school-breaks.ts`, `trip-when.service.ts`.

```
GET /v1/trip-regions/orlando/when?from=2027-01-01&to=2027-11-12&days=10&parks=…&within=DE-NW
GET /v1/trip-regions/orlando/school-breaks?from=2027-01-01&to=2027-11-12&countries=DE,NL,GB
```

`/when` needs the data key and is private like `/plan` (`public, max-age=3600, …`, turned private
by the key guard). It answers 400 for:

- a search range over 366 days, one in the past, or one past a year from today (today + 365);
- a range shorter than the trip;
- `days` outside 3–21, `limit` outside 1–10, `gap` outside 1–60;
- a `within` code `/school-breaks` does not list for its country (the message names the ones it
  does);
- any trip option `/plan` refuses (an unknown park, more visits than days without `hop`, …), and
  `lock`, which no window can take;
- an unknown query parameter.

## Parameters

| Parameter                                                                                       | Default             | Meaning                                                                                                                         |
| ----------------------------------------------------------------------------------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `from`, `to`                                                                                    | today … today + 365 | the search range; every window lies inside it                                                                                   |
| `days`                                                                                          | 10                  | trip days, arrival and departure day included                                                                                   |
| `parks`, `hotel`, `area`, `arrival`, `departure`, `blocks`, `fireworks`, `split`, `rest`, `hop` | as `/plan`          | the trip each window is planned for, validated and defaulted by the plan's own `normalizePlanQuery`                             |
| `parties`                                                                                       | none                | deprecated name of `fireworks`: `avoid` reads as `want`, `allow` and `prefer` as `any`; `fireworks` wins                        |
| `within`                                                                                        | none                | a code from `/school-breaks` (`DE-NW`, `NL-GR`, `GB-ENG` …): only windows lying wholly inside one of that region's breaks count |
| `limit`                                                                                         | 5                   | windows returned, at most 10                                                                                                    |
| `gap`                                                                                           | 7                   | minimum days between two returned windows' start dates, so the list is not five shifts of one week                              |

## Response

```jsonc
{
  "meta": {
    "region": "orlando",
    "from": "2027-01-01",
    "to": "2027-11-12",
    "days": 10,
    "computedAt": "…",
    "forecastUntil": "2027-05-12",
    "within": {
      "code": "DE-NW",
      "name": "Nordrhein-Westfalen",
      "country": "DE",
      "through": "2028-11-04",
      "periods": [
        { "from": "2027-03-20", "to": "2027-04-04", "name": "Easter Holidays" },
      ],
    },
    "search": {
      "exact": true,
      "windows": 307,
      "candidates": 307,
      "planned": 32,
      "averageStride": 12,
      "averageSample": 26,
    },
    "climate": {
      "park": "magic-kingdom-park",
      "basis": "climate_normal",
      "period": "2015-2024",
    },
  },
  "windows": [
    {
      "rank": 1,
      "from": "2027-02-16",
      "to": "2027-02-25",
      "headlinerWait": 34, // the window's plan: mean over its park days
      "rangeAverageWait": 44, // the same trip's plan, averaged over the range's windows
      "basis": "forecast", // forecast | mixed | last_year | none
      "reasons": ["no_school_break", "after_holidays", "mild_weather"],
      "warnings": ["hours_not_published"],
      "seasons": [], // ids into `seasons`
      "closesEarlyDays": 0,
      "plan": {
        "days": [
          { "date": "2027-02-16", "kind": "arrival", "parks": [] },
          {
            "date": "2027-02-17",
            "kind": "park",
            "parks": ["disneys-animal-kingdom-theme-park"],
          },
        ],
        "unplaced": ["…"],
      }, // only when a visit found no day
    },
  ],
  "weeks": [
    { "start": "2026-12-28", "headlinerWait": 41, "basis": "forecast" },
  ],
  "months": [
    {
      "month": "2027-01",
      "headlinerWait": 37,
      "basis": "forecast",
      "climate": {
        "tempMax": 20.7,
        "tempMin": 11.3,
        "precipitationMm": 72,
        "basis": "climate_normal",
        "period": "2015-2024",
      },
      "usSchoolBreakDays": [
        {
          "date": "2027-01-01",
          "name": "Winter break (Orange County Public Schools), New Year's Day",
        },
      ],
      "seasons": ["…"],
      "partyNights": 1,
      "dayGuestHours": 11.5,
      "hurricaneSeason": false,
    },
  ],
  "seasons": [
    /* every season a window or a month names, shape as in /calendar */
  ],
}
```

- **A window's figure is the figure of its plan.** Each window is planned by `/plan`'s own steps
  (`optimizePlan`, with `split=auto`'s second visits) on the same calendar, and its days are priced
  by the plan's `dayCost` (through the adapter `priceDay`): early closings against the usual day,
  hotel perks and fireworks count. Two things differ, both on purpose. A day without published
  hours is priced as if open: the plan's 1000-minute penalty keeps such a day off a trip that has
  a published one, but between windows it would only say "too far ahead", and most of a year is.
  And `headlinerWait` leaves the `fireworks=want` penalty out (it ranks the window; it is no number
  of minutes) and says `fireworks_missed` instead. `headlinerWait` is the plan's mean over its park
  days (a hop day counts once, half of each park plus the move).
- **The ranking** is the plan's sum over its park days, plus 1000 for each visit the window cannot
  hold (`plan.unplaced`), so a window that drops a chosen park ranks behind every window that keeps
  it. Ties go to the earlier window.
- **`rangeAverageWait`** is the same trip's plan figure meaned over the range's windows, so "27 %
  below the range's average" compares like with like. It plans every `averageStride`-th window
  (about 26 over any range, every other week of a year), not all of them: on the 2027 production
  data the sample and the mean over all 307 windows agreed to the minute for 10- and 14-day trips.
  It ignores `within`.
- **`basis`**: `forecast` when every planned park day is forecast (`long_range` counts), `last_year`
  when every one is last year's measured day, `mixed` otherwise, `none` without a figure. A
  `last_year` window is a statement about last year and the client labels it so.
- **`weeks`** is the year strip: per ISO week in the range (a week cut by the range keeps only its
  days in it), each chosen park on its cheapest day of the week, the week priced as a window of its
  own, meaned over the parks open that week. It is an overview, not a plan.
- **`months`**, one per calendar month the range touches:
  - `headlinerWait`: the `weeks` figures, each weighted by its days in the month; `basis` combines
    theirs.
  - `climate`: the climate normals (2015–2024, ERA5) at Magic Kingdom, the reference park for the
    region, from `WeatherService.getClimateNormals`: the month's mean high and low and its total
    precipitation. Never a forecast. Absent while the normals are not cached yet (the service
    fetches them in the background). No `rainDays`: a normal is a mean per calendar day, and a mean
    of 3 mm does not say on how many days it rains.
  - `usSchoolBreakDays`: the days of the range in the month that the region's calendar flags
    `isSchoolVacation` or `isHoliday`, each with its holiday names joined.
  - `seasons`: the ids of the seasons the chosen parks' days carry in the month.
  - `partyNights`: park-days of the chosen parks that close early (`closesEarly`) for a ticketed
    evening.
  - `dayGuestHours`: the mean day-guest open hours over the month's OPERATING days of the chosen
    headliner parks, one decimal; absent where no hours are published.
  - `hurricaneSeason`: June to November.
- **Reasons** (the client translates the codes and shows nothing for an unknown one):
  - `no_school_break`: no day of the window is a US school break and none is unknown;
  - `after_holidays`: and a school break ended within 7 days before it;
  - `before_spring_break`: and the next school break starts within 14 days after it, in February
    to April;
  - `festival` (with `seasons`): a chosen park's `special_event`, `halloween`, `christmas` or
    `summer_nights` season that needs no separate ticket runs in the window;
  - `long_days`: the chosen headliner parks' mean day-guest hours in the window beat the range's by
    an hour or more;
  - `mild_weather`: the climate normals over the window give a mean high of 20–29 °C and at most
    3 mm of rain a day. Never a forecast.

  **Warnings**:
  - `school_break`: a day of the window is a US school break;
  - `hurricane_season`: the window touches June to November, a fact about the season;
  - `party_nights` (with `closesEarlyDays`, the planned park days that close early): a planned day
    closes early for a ticketed evening;
  - `fireworks_missed`: with `fireworks=want`, a planned night loses the evening show anyway;
  - `hours_not_published`: a planned day has no published hours;
  - `estimate_last_year`: a planned day reads last year's figure;
  - `water_park_closed`: a chosen water park has a curated closure in the window, or could not be
    placed.

  The school-break codes are about the region's own (US) school calendar, which is what fills the
  parks; `within` is about the visitor's.

- **`within`** restricts windows to those that lie completely inside one of the region's breaks as
  `/school-breaks` lists them. If no break is long enough, the answer is `windows: []` with
  `meta.within.periods` listed, so the client can say why; `weeks` and `months` still cover the
  range. `meta.within.through` is the last day the stored school data reaches; a break after it is
  unknown, not absent.

## How it is computed

Every start date in the range is a candidate. A lower bound ranks them all: the plan with its
rules dropped, i.e. the cheapest assignment of the window's visits to distinct days (Hungarian),
a visit left out at 1000; where the plan may pair parks on a hop day, each visit on its park's
cheapest days at the hop day's half price instead. The full optimizer then plans candidates in
bound order until the next bound cannot beat the `limit`-th window kept, and windows closer than
`gap` days to a better kept one are dropped (the greedy rule, in score order). That is exact: every
window left unplanned scores at least its bound, behind the last one returned. The specs check it
against planning every window.

**Time.** On the 2027 production data (7 headliner parks, 2027-01-01 … 2027-11-12, 307 windows) a
10-day search plans 32 windows plus the 26 of the average and takes 0.15–0.3 s; 14 days 0.3–0.5 s.
A 21-day trip with `split=auto` adds second visits and costs the optimizer about 0.3 s a window, so
a request stops planning after 2 s (and the average after 0.8 s more) and says
`meta.search.exact: false`; with `split=fixed` the same search takes 0.3 s. Without a clock (the
specs) the search always runs to the end.

**Reads.** The days come from the same calendar read as `/calendar`, over the whole range plus its
two-week reference span: one read per park and month from the month caches, the last-year
snapshots past the forecast, no ML call. The warmup keeps a region park's months to +13 so a
year's range is all cache hits ([trip regions](./trip-regions.md#caching-and-cost)). An answer is
kept six hours in Redis (`trip-regions:when:v1:{region}:{query}`) by its normalized query.

## School breaks (`GET /v1/trip-regions/:region/school-breaks`)

Open, no key: holidays are not forecasts. An hour in the browser, a day at the edge.

| Parameter    | Default                | Meaning                                                                                           |
| ------------ | ---------------------- | ------------------------------------------------------------------------------------------------- |
| `from`, `to` | today … today + 365    | the range; a break reaching into it is listed whole                                               |
| `countries`  | DE, NL, GB, BE, FR, AT | ISO codes; by default a country is listed only when its stored school data reaches 180 days ahead |

```jsonc
{
  "regions": [
    {
      "code": "DE-NW",
      "name": "Nordrhein-Westfalen",
      "country": "DE",
      "through": "2028-11-04",
      "periods": [
        { "from": "2027-03-20", "to": "2027-04-04", "name": "Easter Holidays" },
      ],
    },
  ],
}
```

- **`code` is the `holidays.region` value the sync stored**, verbatim, and the one `within` takes;
  `/when` resolves `within` through this list, one source. A region's periods are its own rows and
  the country's nationwide ones, merged into runs of consecutive days (a break ending on a Friday
  and one starting the Monday after are one run), each widened over the weekend before and after
  it, since there is no school on either. A period is named by the name most of its days carry.
- **The regions offered**, by country (the names are the regions' own):
  - DE: the 16 states, `DE-BW` … `DE-TH`.
  - AT: the 9 states as OpenHolidays codes them: `AT-BL`, `AT-KÄ`, `AT-NÖ`, `AT-OÖ`, `AT-SB`,
    `AT-SM`, `AT-TI`, `AT-VA`, `AT-WI`.
  - BE: `BE-NL` (Vlaamse Gemeenschap) and `BE-FR` (Fédération Wallonie-Bruxelles), OpenHolidays'
    groups; the German-speaking community has none.
  - FR: `FR-ZA`, `FR-ZB`, `FR-ZC`, the three zones.
  - GB: `GB-ENG`, England, curated from one local authority (Staffordshire) because no source has
    GB school data ([holiday sources](../architecture/holiday-sources-outside-europe.md)).
  - NL: `NL-GR` ("Regio Noord") only. **What the data holds:** OpenHolidays stores the Netherlands
    per subdivision, 354 codes: provinces wholly in one region (`NL-GR`, `NL-FR`, `NL-DR`, `NL-OV`,
    `NL-NH` for the north) and municipalities of split provinces (`NL-GE-AR` …); the provinces that
    span regions (`NL-GE`, `NL-ZH`, `NL-UT`, `NL-FL`, `NL-NB`) carry the union of two or three
    regions' breaks. A break all three regions share (Christmas, May) is listed with the north's
    subdivisions only and its groups `NL-NO`/`NL-MI`/`NL-ZU` beside them, and the sync stores
    subdivisions when an entry has any, so the middle and south codes lack those breaks (measured
    2026-11-12). "Noord" maps to `NL-GR`; "Midden" and "Zuid" have no code whose breaks are
    complete and are not offered until the sync also stores the groups.
  - Any other country asked for with `countries` lists every code its rows carry, named by the
    code.
- **`through`** is the last day the region's own rows or its country's nationwide rows reach, null
  when there are none. Unlike the calendar's coverage it never falls back to another region.
