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
| Which day types | a **day off** (the park's own), a **neighbour day** (a holiday or break in an influencing region only), an **eve** (a working day before an own day off) and a **working day**; a day whose type rests on a school break nobody has stored is **unknown** ([day types](#day-types)) |
| Which measured days | per ride and day type, the **latest 6** measured days of that type in the season; a type needs **3** of its own ([recency](#recency)) |
| What a row of a type with a reference is served | the **level**, the median measured P90 of the type's latest 6 days (`<model>+season`). With `SEASON_LEVEL_MODE=ratio` a TFT row is served TFT's own number × the **ratio** instead, held to 0.5–3 and to the highest P90 the type measured (`tft+season-ratio`, [ratio](#the-ratio)) |
| What a row of a type without one is served | the model's number, or a quieter type's level where that is higher (`<model>+season`, [floors](#floors)) |
| Otherwise | the model's number, unchanged; always on an unknown day |
| Band and confidence | none on a row the rule replaced: `uncertaintyMinutes` and `confidence` are null |

A day type never takes another type's level as its own. A season's first days are mostly its
opening weekend, so their median is a day off's: on 2026-11-17, with Saturday, Sunday and Monday of
Phantasialand's Wintertraum measured, that median set every day of the season to "high" with a
45-minute headliner peak, every Tuesday included, after the Monday had measured 25 minutes
(PAR-1375). A type waits for its own three days: in a season that starts on a Saturday outside the
school breaks, working days from the first Thursday, days off from the second Sunday, eves from the
fourth Friday. Until then a quieter type's level is a floor under the model's number, never the
type's level.

## Day types

`seasonDayTypes` types every day of the running seasons. The first rule that matches wins, so a
Friday with a neighbour's break is a neighbour day and a Saturday with one is a day off:

1. **Day off:** the park's own. The country's weekend (`isCountryWeekendDate`), and every public
   holiday, school-break day and bridge day the calendar marks for the park's region
   (`dayHolidayFlags`). A Tuesday of the Christmas break queues like a Saturday at Phantasialand,
   not like a Tuesday in November.
2. **Neighbour day:** a public holiday or a school-break day in one of the park's influencing
   regions only ([neighbour regions](#neighbour-regions)).
3. **Eve:** a working day whose next calendar day is an own day off: Friday before a
   Saturday–Sunday weekend, Thursday before a Friday–Saturday one, the day before a public holiday
   or a break. A neighbour day makes no eve. In an evening-driven season (Wintertraum, Halloween)
   Fridays queue above Monday to Thursday; under a working-day median a Friday read low.
4. **Working day:** every other day.

A day is **unknown** when its type rests on a school break nobody has stored (PAR-1049,
[school-holiday coverage](../architecture/school-holiday-coverage.md)):

- a day past the park's own region's last stored school-break row (`schoolCoverageThrough`) that
  is not a day off anyway, and the working day before one, whose eve depends on it. In a country
  without stored school breaks every weekday that is not a holiday is unknown, and only the days
  off get a season level;
- a day past an influencing region's last stored school-break row that no stored neighbour row
  makes a neighbour day, since a break there would ([below](#neighbour-regions)).

An unknown day is measured into no reference and served the model's number, without a floor.
Counted as a working day, a break week past the data would have pulled every later working day up
to break level. Without the holiday rows the season level is not served at all, rather than serve
every holiday a working day's level.

## Neighbour regions

A public holiday or school break in one of the park's `influencingRegions` makes a neighbour day.
TFT (`is_holiday_neighbor`) and CatBoost (`holiday_features.py`) read the same rows. The
level-driver analysis measures a neighbour's public holiday at +17 % and any school holiday, own or
neighbour, at +21 % ([level drivers §3](level-drivers.md#3-which-drivers)): a lift of about a
fifth, not a weekend. So a neighbour day is a type of its own and never pooled with the days off.
At Phantasialand 25 of the 30 weekdays from 28 September to 6 November 2026 carry an own or a
neighbour's break; with the neighbours' counted as days off, a Halloween season there had one type
for almost every day, the shape PAR-1375 removed. For Phantasialand the neighbours are
Rhineland-Palatinate, Hesse, Lower Saxony, Limburg, Gelderland and Belgium.

- **Matching** (`influencingHolidayTest`, `src/common/utils/holiday.utils.ts`): an entry without
  a region (`{BE, null}`) matches every row of its country, as the calendar's neighbour marker
  (`CalendarService.neighborHolidayPriority`) reads it. An entry with one matches its region,
  through `normalizeRegionCode`, and its country's nationwide rows: Limburg has Koningsdag, which
  is stored for NL without a region. The calendar's marker leaves those nationwide rows out; the
  models and the season level take them.
- **Which rows:** `public`, `bank` and `school`. An observance closes nothing. A neighbour's bridge
  day and the weekends its break absorbs are not read; a weekend is an own day off anyway.
- **Where from:** `HolidaysService.getHolidays` once per country, the park's own and each
  influencing one, all of them cached per year.
- **Coverage** (`seasonSchoolCoverage`): each influencing region's last stored school-break row,
  by `schoolCoverageThrough`, and the earliest of them is the neighbours' edge. Past it, a day no
  stored neighbour row makes a neighbour day is unknown: a break nobody has published could fall
  on it. A day a stored row already makes a neighbour day stays one. An influencing region that
  stores no school break at all is left out of the edge. Its breaks are never read, so it never
  makes a neighbour day by one, and counted, it would make every working day of every season
  unknown; its public holidays still count.

A park whose region has no entry in `REGION_INFLUENCES` gets the first three countries of
`COUNTRY_INFLUENCES` as whole countries (park enrichment), so every region's break in those
countries counts. In summer, when the German states' breaks follow each other from June to
September, that can make most weekdays of a season neighbour days at a park with Germany as a whole
neighbour; they stay apart from its weekends.

## Recency

Each reference reads the type's latest 6 measured days (`SEASON_RECENT_DAYS`), never all of them.
Under the median of all days the level was flat within a type — one number on every working day
for weeks — and it lagged a season that builds: Wintertraum's November days would have outvoted
its Advent days into late December. Six days of a type are about a week and a half of working
days, three weekends of days off, six weeks of Fridays; the median of six still absorbs one or two
rainy days.

## Floors

A type with fewer than 3 measured days has no reference, and its row would be the raw model's
number. After a deploy, or at a season's start, that is every day until a type has three:
working days from the first Thursday, days off from the second Sunday, and eves, one a week, from
the fourth Friday, reading below the Thursday before them for three weeks.

So a row of a type without its own reference is served `max(model, floor)`, where the floor is the
level of a type that queues below it (`SEASON_FLOOR_TYPES`):

| Type without a reference | Floor |
|---|---|
| eve | the working days' level |
| neighbour day | the working days' level |
| day off | the highest of the eves', the neighbour days' and the working days' levels |
| working day | none |

- Only where the floor, rounded like every served wait, is above the model's number is the row
  replaced, marked `<model>+season` like any measured level, in either mode. Where the model is
  higher the row stays the model's, band and confidence included.
- A floor is the quieter type's measured level, never its ratio.
- A type with its own reference is never floored, even where a quieter type measured more.
- An unknown day gets no floor.

A floor says "at least this". It never serves a day a busier type's level, so the opening weekend
cannot set the Tuesdays (PAR-1375), and the day off it floors can still read below its own
Saturdays until they reach three.

## The ratio

`SEASON_LEVEL_MODE=ratio`, off by default. For a TFT row (`modelVersion: "tft"`) whose type has a
reference it serves `TFT × r`, rounded like every served wait (`roundServedWait`), where `r` is the
median over the type's latest 6 qualifying days of

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

### Why it is not the default, and what holds it

The ratio of the early days is measured on forecasts TFT made before it had seen the season, and
applied to TFT's later runs, which have. Nothing has scored it. Unbounded, a ride TFT forecast at
6 minutes in the opening week that measured 30 carried a ratio of 5 into every day of its type:
160 minutes for a Saturday TFT put at 32. So in ratio mode:

- **The ratio is held to 0.5–3** (`SEASON_RATIO_MIN`, `SEASON_RATIO_MAX`) where it is served. The
  reference keeps the measured median, so a changed bound applies without a cache flush.
- **The product is at most the type's highest measured P90** (`maxPeak`): the highest daily P90
  the ride measured on the days behind the reference's level and ratio. A forecast holiday spike
  is served no higher than the season has queued so far.
- **No band and no confidence.** TFT's band and confidence describe TFT's number; the scaled one
  has neither measured, so both are null, absent rather than narrow.
- **Its own label**, `tft+season-ratio`, so every reader can tell it from a measured level:
  `servedLevelSource` files both under `season`, and `isSeasonRatioRow` says which.
- **`/plan/day` gates it like TFT** ([below](#what-it-does-to-the-evaluation)).

## The switch

| Variable | Values | Effect |
|---|---|---|
| `SEASON_LEVEL` | `true` / anything else | turns the season level on (`docker-compose.production.yml`: on since PAR-1343) |
| `SEASON_LEVEL_MODE` | `level` (default), `ratio` | `level` serves the measured level on every row of a type with a reference; `ratio` scales TFT's rows as above |

Both are read per call. The cached inputs hold the level and the ratio, so a changed mode applies
on the next request without a cache flush; the frontend's caches keep the old numbers until the
next warmup rings them. `docker-compose.production.yml` passes `level` unless the server's `.env`
sets the mode. Rollback: `SEASON_LEVEL=false` for the models' own numbers.

## Reads and cache

`loadSeasonLevelInputs` (`src/ml/utils/season-level.query.ts`):

1. The running seasons (`park_seasons`, `park_id = $1::uuid`).
2. The holiday rows of the park's country and of each influencing country, from three days before
   the first season day to four past the last: the bridge-day rule reads three days either side,
   and an eve reads the day after.
3. The school-break coverage of the park's region and of each influencing region
   (`getSchoolHolidayCoverage`, `seasonSchoolCoverage`).
4. One query for the measured days: per ride and past season day, TFT's target statistic
   (`nf-service` `fetch_daily_peak_panel`: the P90 of the day's OPERATING STANDBY waits of at least
   5 minutes, without heartbeat rows, from at least three readings) and, beside it, TFT's freshest
   forecast of the day made before it, one lookup on the primary key of `tft_forecasts`. Today is
   never a reference day.

`seasonReferences` turns the measured days into the references: the type of each day, the latest
6, the medians, the highest P90.

**The inputs change with the calendar warmup only.** They live under `ml:season-level:v4:{parkId}`
(`CacheKeys.seasonLevelInputs`), a key without a date. `warmup-calendar-daily` (08:00 and 20:00
UTC, [job queues](../architecture/job-queues.md#schedule-utc)) evicts it with the park's daily ML
cache and month caches, and the run's first build reads them again; the
[forecast ring](../architecture/caching-strategy.md#the-forecast-ring) after the run then tells the
frontend's calendar, best-days and Parky's tip at once. A key with the park-local date picked up a
new day at midnight: a type could reach its third day then and move every day of that type, and
`/plan/day` and Parky followed while the calendar grid kept the old level for up to nine hours,
since nothing rings at midnight. Now a day's measurement joins its type at the next warmup.

The key lives 14 hours (`SEASON_LEVEL_INPUTS_TTL`), an hour more than the warmed month caches
built from it, so a late warmup finds the months cold before the inputs. Past that a warmup has
failed, and the next read recomputes the inputs on demand: that change is **not rung**, and until
the next warmup the frontend's calendar can show the old level beside `/plan/day`'s new one. A
failed read costs the season level, never the response.

A row the rule replaced carries `crowdLevel: "unknown"` (the calendar rates the day), no band, no
confidence, and `modelVersion` `<model>+season` for a measured level or a floor, `tft+season-ratio`
for a ratio row. Every reader of the served daily level gets it: the calendar, `/plan/day` day
levels, the yearly view, whose day `confidencePercentage` averages the rows that carry a
confidence and is null where none does.

## What it does to the evaluation

A season row is its own source, `season` (`servedLevelSource`), a ratio row included:

- **Lead snapshots** store it as `source = 'season'`, with its `model_version`, so its error by
  lead is measured next to TFT's and CatBoost's from the first night, and a ratio row
  (`tft+season-ratio`) apart from a measured level (`tft+season`, `catboost+season`).
- **Forward archive**: `level_source = 'season'`. The daily candidates (CatBoost, the TFT grid
  shadows) are archived against the served level only where that is `tft`, so they are **not
  paired on season days**. That is the right population — on those days a replacement for TFT
  would not be served either — but a park in a long season contributes fewer paired days, which
  matters for the decisions dated December 2026.
- **`/plan/day`** reads the row's source with `servedLevelSource` and `isSeasonRatioRow`, never
  `modelVersion === "tft"`. `dayPeak` is the served row's level either way. The H5 composer
  scales the ride's 56-day profile by a **measured level** (`+season`, a floor included) on every
  ride at every lead it serves: the TFT gates were measured on TFT's forecast error, and a measured
  level is no forecast. A **ratio row** is TFT's forecast times a ratio and carries TFT's error at
  every lead, so it goes through the TFT gates: busy rides (`BUSY_Q90_MINUTES`) at leads 0–2
  (`TFT_LEVEL_MAX_LEAD_DAYS`), the plain profile elsewhere. Either is `composer: "h5_season"`,
  archive code `e`, scored under `level_season`
  ([slots and H5](../frontend/plan-day/slots-and-h5.md#on-a-day-of-a-running-park-season)). The
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

Split by `s.model_version` to keep a TFT row's measured level (`tft+season`), a CatBoost row's
(`catboost+season`) and a ratio row (`tft+season-ratio`) apart. Before PAR-1383 a ratio row was
labelled `tft+season` too; those rows are told apart by the dates the mode was set.
`scripts/ml-review/park-season-coverage.sql` lists which parks have seasons that bind a level.

## A ride the season pauses

A season can also name rides that do not run while it lasts (`paused_attraction_ids`, River Quest
during Wintertraum). Such a ride measures nothing in the season, so it has no season level and
keeps the model's number — which is why two other rules take it out: the calendar and the yearly
view drop it from the day's average (`CALENDAR_SEASON_FILTER`, PAR-1326), `/plan/day` leaves it
out of the plan, and its own page serves no hourly forecast, best visit time or rope-drop
recommendation while it is shut.

## Known limits

- **Known limit:** not measured. Neither mode, nor a floor, has been scored against TFT on season
  days; the query above is how, once the lead snapshots hold enough `season` rows.
- **Known limit:** the first days of a season have no reference, because only the running
  edition is read, and a floor is only the quieter types' level. The next step is the previous
  edition of the same season, once past editions are curated.
- **Known limit:** a floored day off reads the highest quieter level, which can sit well below the
  season's Saturdays until three of them are measured.
- **Known limit:** the ratio's bounds (0.5–3) and its cap at the highest measured P90 are
  judgement, not measurement. The ratio is measured on forecasts made a day or so ahead and applied
  to TFT rows up to 60 days ahead; whether TFT's under-read of a season depends on the lead is not
  known.
- **Known limit:** in ratio mode the forward archive's `composed_h5_season` (code `e`) holds both
  a measured level and a gated ratio row; the served `plan_day` row does not say which.
- **Known limit:** the inputs move only with the calendar warmup, so a day's measurement joins its
  type up to half a day late. A failed warmup leaves them to expire and be recomputed on the next
  read, which the frontend is not told of.
- **Known limit:** a season that runs on weekends only but is stored as a plain range counts its
  weekdays too. Listing its `dates` fixes it.
- **Known limit:** an evening event with its own ticket (`separate_ticket`, `opens_at`) is treated
  like an all-day season.
- **Known limit:** a weekday of the park's own school break and a Saturday outside one share one
  type. A break week can queue above an ordinary Saturday; the ratio mode keeps TFT's holiday
  structure inside the type, the level mode does not.
- **Known limit:** the neighbours' edge is the earliest among them, so one influencing region whose
  school data ends early makes every later working day unknown, though the others are known.
- **Known limit:** a park without a region code in a country whose school breaks are regional
  reads only nationwide rows, so its break days are working days.
