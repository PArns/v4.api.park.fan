# Season level (PAR-1343)

What the served daily level is on a day of a running park season, why it is a rule and not a
model, and what it does to the evaluation. Open it before you change
`MLService.getServingDailyPredictions`, add a season input to a model, or read a calendar cell of
a park in Halloween or a winter season.

## Why

No model reads `park_seasons` ([CatBoost known limits](catboost.md#known-limits), PAR-1116). On
2026-11-14, the first day of Phantasialand's Wintertraum, the calendar read "very low" for every
day to 18 December with a headliner peak of 10–25 minutes, the Advent Saturdays included; the day
itself averaged 35 minutes. The models had a closed park behind them and nothing that said a
season had begun. How large the effect is per kind and what each model should read is the
research of PAR-1342; this rule is what serves until then.

## The rule

`applySeasonLevel` (`src/ml/utils/season-level.util.ts`), applied at the end of
`MLService.getServingDailyPredictions` while `SEASON_LEVEL=true`:

| Question | Answer |
|---|---|
| Which seasons | kind `halloween`, `christmas`, `winter`, `summer_nights` or `special_event`; status `confirmed` or `announced` (`expected` is a projection, `cancelled` is not happening); today, park-local, inside its bounds |
| Which days | the season's listed `dates` when it has them, else every day from `start_date` to `end_date` |
| Which day types | a **day off**, an **eve** (a working day before a day off) and a **working day**; a day of none of them is **unknown** ([day types](#day-types)) |
| Which measured days | per ride and day type, the **latest 6** measured days of that type in the season; a type needs **3** of its own ([recency](#recency)) |
| What a TFT row is served | `SEASON_LEVEL_MODE=ratio` (the default): TFT's own number × the **ratio**, the median of measured P90 ÷ TFT's forecast of the day over the type's latest 6 days with a forecast of at least 5, once there are 3 such days ([ratio](#the-ratio)) |
| What any other row is served | the **level**, the median measured P90 of the type's latest 6 days: CatBoost's rows (days 61+, rides TFT has no row for), a TFT row whose ratio has fewer than 3 days, and every row with `SEASON_LEVEL_MODE=level` |
| Otherwise | the model's number, unchanged; always on an unknown day |

A day type never borrows from another, and there is no level from all of a season's days together.
A season's first days are mostly its opening weekend, so their median is a day off's: on
2026-11-17, with Saturday, Sunday and Monday of Phantasialand's Wintertraum measured, that median
set every day of the season to "high" with a 45-minute headliner peak, every Tuesday included,
after the Monday had measured 25 minutes (PAR-1375). A day type waits for its own three days: in a
season that starts on a Saturday outside the school breaks, working days from the first Thursday,
days off from the second Sunday, eves from the fourth Friday.

## Day types

`seasonDayTypes` types every day of the running seasons:

- **Day off:** the country's weekend (`getWeekendDaysForCountry`), and every public holiday,
  school-break day and bridge day the calendar marks (`dayHolidayFlags`). A Tuesday of the
  Christmas break queues like a Saturday at Phantasialand, not like a Tuesday in November. A
  public holiday or school-break day in one of the park's influencing regions is a day off too
  ([neighbour regions](#neighbour-regions)).
- **Eve:** a working day whose next calendar day is a day off — Friday before a Saturday–Sunday
  weekend, Thursday before a Friday–Saturday one, the day before a public holiday or a break.
  In an evening-driven season (Wintertraum, Halloween) Fridays queue above Monday to Thursday;
  under the old working-day median a Friday read low.
- **Working day:** every other day, up to the park's last stored school-break row.
- **Unknown:** a weekday past the region's last stored school-break row
  (`schoolCoverageThrough`, PAR-1049), which no other rule makes a day off, and a working day
  before one, whose eve depends on it. A break there could be one nobody has published yet, so
  the day is measured into no reference and served the model's number. Counted as a working day,
  a break week past the data would have pulled every later working day up to break level. In a
  country without stored school breaks every weekday that is not a holiday is unknown, and only
  the days off get a season level.

Without the holiday rows the season level is not served at all, rather than serve every holiday a
working day's level.

## Neighbour regions

A public holiday or school break in one of the park's `influencingRegions` makes a day off, as it
does for TFT (`is_holiday_neighbor`) and CatBoost (`holiday_features.py`). The level-driver
analysis measures a neighbour's public holiday at +17 % and any school holiday, own or neighbour,
at +21 % ([level drivers §3](level-drivers.md#3-which-drivers)). For Phantasialand the neighbours
are Rhineland-Palatinate, Hesse, Lower Saxony, Limburg, Gelderland and Belgium.

- **Matching** (`influencingHolidayTest`, `src/common/utils/holiday.utils.ts`): an entry without
  a region (`{BE, null}`) matches every row of its country, as the calendar's neighbour marker
  (`CalendarService.neighborHolidayPriority`) reads it. An entry with one matches its region,
  through `normalizeRegionCode`, and its country's nationwide rows: Limburg has Koningsdag, which
  is stored for NL without a region. The calendar's marker leaves those nationwide rows out; the
  models and the season level take them.
- **Which rows:** `public`, `bank` and `school`. An observance closes nothing. A neighbour's bridge
  day and the weekends its break absorbs are not read; a weekend is a day off anyway.
- **Where from:** `HolidaysService.getHolidays` once per country, the park's own and each
  influencing one, all of them cached per year.
- **Coverage stays the park's own.** Whether a weekday is unknown is decided by the park's own
  region's school data. Past a neighbour's last stored school row, its break is not known, and
  the day stays a working day or an eve.

A park whose region has no entry in `REGION_INFLUENCES` gets the first three countries of
`COUNTRY_INFLUENCES` as whole countries (park enrichment), so every region's break in those
countries counts. In summer, when the German states' breaks follow each other from June to
September, that can make most weekdays of a season days off at a park with Germany as a whole
neighbour.

## Recency

Each reference reads the type's latest 6 measured days (`SEASON_RECENT_DAYS`), never all of them.
Under the median of all days the level was flat within a type — one number on every working day
for weeks — and it lagged a season that builds: Wintertraum's November days would have outvoted
its Advent days into late December. Six days of a type are about a week and a half of working
days, three weekends of days off, six weeks of Fridays; the median of six still absorbs one or two
rainy days.

## The ratio

For a TFT row (`modelVersion: "tft"`) the ratio mode serves `TFT × r`, rounded like every served
wait (`roundServedWait`), where `r` is the median over the type's latest 6 qualifying days of

```
measured daily P90 ÷ TFT's forecast of that day
```

- **The forecast** is TFT's freshest one made before the day: from `tft_forecasts`, the latest
  `forecast_date` before `target_date`. A day without one, or with one under 5 minutes
  (`MIN_RATIO_FORECAST`, the panel's own floor `NF_MIN_WAIT`), is passed over; a ratio over a
  near-zero forecast would multiply its noise into every served day. A type needs 3 such days.
- **Why a ratio.** The level is one number per type. TFT's own forecast carries the
  structure inside the type — weekday, holidays, weather — and what it misses at the start of a
  season is the level. The ratio keeps TFT's day-to-day shape and lifts it by what the ride
  measured over what TFT expected.
- **No feedback.** The ratio divides by TFT's own forecasts as `nf-service` stores them, never by
  a served (scaled) row. As the season's days enter TFT's input window (90 operating days) and
  its forecasts rise, the ratio falls with them.
- **Which rows.** Only TFT's: CatBoost's number was never measured against, so its rows (days
  61+, rides TFT has no row for) get the level. So does a TFT row whose ratio has fewer than 3
  days, for instance a ride TFT forecast under 5 minutes on most of the season's days.

## The switch

| Variable | Values | Effect |
|---|---|---|
| `SEASON_LEVEL` | `true` / anything else | turns the season level on (`docker-compose.production.yml`: on since PAR-1343) |
| `SEASON_LEVEL_MODE` | `ratio` (default), `level` | `level` serves the level on every row: the PAR-1375 rule with the three day types, the neighbours and the recency of this page |

Both are read per call. The cached inputs hold the level and the ratio, so a changed mode applies
on the next request without a cache flush. Rollback: `SEASON_LEVEL_MODE=level`, or
`SEASON_LEVEL=false` for the models' own numbers.

## Reads and cache

`loadSeasonLevelInputs` (`src/ml/utils/season-level.query.ts`):

1. The running seasons (`park_seasons`, `park_id = $1::uuid`).
2. The holiday rows of the park's country and of each influencing country, from three days before
   the first season day to four past the last: the bridge-day rule reads three days either side,
   and an eve reads the day after.
3. The park's school-break coverage (`getSchoolHolidayCoverage`).
4. One query for the measured days: per ride and past season day, TFT's target statistic
   (`nf-service` `fetch_daily_peak_panel`: the P90 of the day's OPERATING STANDBY waits of at least
   5 minutes, without heartbeat rows, from at least three readings) and, beside it, TFT's freshest
   forecast of the day made before it, one lookup on the primary key of `tft_forecasts`. Today is
   never a reference day.

`seasonReferences` turns the measured days into the references: the type of each day, the latest
6, the medians. The inputs are cached per park and park-local date for six hours
(`ml:season-level:v3:{parkId}:{date}`), so a new day's measurement is read on that day; a failed
read costs the season level, never the response.

A row the rule replaced carries `crowdLevel: "unknown"` (the calendar rates the day) and
`modelVersion` `<model>+season`. A ratio row and a level row are not told apart: every reader
files both under `season` (`servedLevelSource`), and in ratio mode a `tft+season` row is a ratio
row unless its ratio was short of days.

Every reader of the served daily level gets it: the calendar, `/plan/day` day levels, the yearly
view.

## What it does to the evaluation

A season-level row is its own source, `season` (`servedLevelSource`):

- **Lead snapshots** store it as `source = 'season'`, with its `model_version`, so its error by
  lead is measured next to TFT's and CatBoost's from the first night.
- **Forward archive**: `level_source = 'season'`. The daily candidates (CatBoost, the TFT grid
  shadows) are archived against the served level only where that is `tft`, so they are **not
  paired on season days**. That is the right population — on those days a replacement for TFT
  would not be served either — but a park in a long season contributes fewer paired days, which
  matters for the decisions dated December 2026.
- **`/plan/day`** reads the row's source with `servedLevelSource`, never `modelVersion === "tft"`:
  `dayPeak` is the season level (a ratio row included) and the H5 composer scales every ride's
  56-day profile by it, at every lead it serves (`composer: "h5_season"`, archive code `e`, scored
  under `level_season`).
  The TFT gates (busy rides, d0–d2) were measured on TFT's forecast error and do not apply to a
  measured level; why, and what stays unmeasured, is in
  [slots and H5](../frontend/plan-day/slots-and-h5.md#on-a-day-of-a-running-park-season). The
  routed shadow stays TFT's routing: on a season day it is the plain profile, so the `tfth5`
  column of `planner_h5_slots` never scores the season level.

## How to measure it (PAR-1345)

The season rows of the lead snapshots against TFT's own forecast of the same ride-day, made at the
same distance or earlier:

```sql
SELECT s.lead_days, count(*) AS scored,
       avg(s.absolute_error) AS season_mae,
       avg(abs(f.predicted_peak - s.actual_wait_time)) AS tft_mae,
       avg(s.predicted_wait_time - s.actual_wait_time) AS season_bias,
       avg(f.predicted_peak - s.actual_wait_time) AS tft_bias
  FROM prediction_lead_snapshots s
  CROSS JOIN LATERAL (
    SELECT predicted_peak FROM tft_forecasts
     WHERE attraction_id = s.attraction_id
       AND target_date = s.target_date
       AND forecast_date <= s.target_date - s.lead_days
     ORDER BY forecast_date DESC LIMIT 1
  ) f
 WHERE s.source = 'season' AND s.absolute_error IS NOT NULL
 GROUP BY 1 ORDER BY 1;
```

Split by `s.model_version` to keep TFT-based rows (`tft+season`) apart from CatBoost-based ones.
Ratio and level mode are told apart by the dates the switch was set.
`scripts/ml-review/park-season-coverage.sql` lists which parks have seasons that bind a level.

## A ride the season pauses

A season can also name rides that do not run while it lasts (`paused_attraction_ids`, River Quest
during Wintertraum). Such a ride measures nothing in the season, so it has no season level and
keeps the model's number — which is why two other rules take it out: the calendar and the yearly
view drop it from the day's average (`CALENDAR_SEASON_FILTER`, PAR-1326), `/plan/day` leaves it
out of the plan, and its own page serves no hourly forecast, best visit time or rope-drop
recommendation while it is shut.

## Known limits

- **Known limit:** not measured. Neither mode has been scored against TFT on season days; the
  query above is how, once the lead snapshots hold enough `season` rows.
- **Known limit:** the first days of a season have no reference, because only the running edition
  is read. The next step is the previous edition of the same season, once past editions are
  curated.
- **Known limit:** the ratio is measured on forecasts made a day or so ahead and applied to TFT
  rows up to 60 days ahead. Whether TFT's under-read of a season depends on the lead is not known.
- **Known limit:** `r` has no bound. A ride TFT forecast at 5–6 minutes that measured 30 carries a
  ratio of about 5 into every TFT day of its type, a forecast holiday spike included.
- **Known limit:** a season that runs on weekends only but is stored as a plain range counts its
  weekdays too. Listing its `dates` fixes it.
- **Known limit:** an evening event with its own ticket (`separate_ticket`, `opens_at`) is treated
  like an all-day season.
- **Known limit:** a weekday of a school break and a Saturday outside one share one type. A break
  week can queue above an ordinary Saturday; the ratio mode keeps TFT's holiday structure inside
  the type, the level mode does not.
- **Known limit:** a neighbour's school break past its own last stored row is not known, and the
  day reads as a working day or an eve, not as unknown.
- **Known limit:** a park without a region code in a country whose school breaks are regional
  reads only nationwide rows, so its break days are working days.
