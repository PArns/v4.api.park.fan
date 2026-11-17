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
| Which day types | a **day off** — the country's weekend (`getWeekendDaysForCountry`), a public holiday, a school-break day or a bridge day, by the calendar's own rule (`dayHolidayFlags`) — and a **working day**, every other day up to the park's last stored school-break row (`seasonDayTypes`) |
| What is served | per ride, the **median of its measured daily P90** on the season's days already over, of the same day type, once that day type has 3 days |
| Otherwise | the model's number, unchanged; always on an **unknown** day, a weekday past the school-break data that no other rule makes a day off |

Holidays count with the weekend because that is how a season queues: a Tuesday of the Christmas
break is a Saturday at Phantasialand, not a Tuesday in November. The holidays are the park's own
(its country, nationwide or its region), the ones the calendar marks on a day; a neighbouring
region's break is the calendar's `neighborHolidays`, a separate signal, and stays a working day
here. Without the holiday rows the season level is not served at all, rather than serve every
holiday a working day's level.

A weekday past the region's last stored school-break row (`schoolCoverageThrough`, PAR-1049) could
be a break day nobody has published yet, so it is unknown: it is measured into no reference and
served the model's number. Counted as a working day, a break week past the data would have pulled
every later working day of the season up to break level. In a country without stored school
breaks every weekday that is not a public holiday is unknown, and only the days off get a season
level.

There is no level from all of a season's days together. A season's first days are mostly its
opening weekend, so their median is a day off's: on 2026-11-17, with Saturday, Sunday and Monday
of Phantasialand's Wintertraum measured, that median set every day of the season to "high" with
a 45-minute headliner peak, every Tuesday included, after the Monday had measured 25 minutes
(PAR-1375). A day type waits for its own three days instead: in a season that starts on a
Saturday outside the school breaks, working days from the first Thursday and days off from the
second Sunday.

The measured day value is TFT's target statistic (`nf-service` `fetch_daily_peak_panel`): the P90
of the day's OPERATING STANDBY waits of at least 5 minutes, without heartbeat rows, from at least
three readings. Today is never a reference day. The reads are in
`src/ml/utils/season-level.query.ts` (the running seasons, the country's holiday rows from the
first season day to the last, the school-break coverage, the measured days), cached per park and
park-local date for six hours (`ml:season-level:v2:{parkId}:{date}`), so a new day's measurement
is read on that day; a failed read costs the season level, never the response.

A row the rule replaced is rounded like every served wait, carries `crowdLevel: "unknown"` (the
calendar rates the day) and `modelVersion` `<model>+season`.

It is the weekday average, restricted to the season. In the model comparison the plain weekday
average is level with TFT from day 4 on; nothing has measured the restricted one. Every reader of
the served daily level gets it: the calendar, `/plan/day` day levels, the yearly view.

## What it does to the evaluation

A season-level row is its own source, `season` (`servedLevelSource`):

- **Lead snapshots** store it as `source = 'season'`, so its error by lead is measured next to
  TFT's and CatBoost's from the first night.
- **Forward archive**: `level_source = 'season'`. The daily candidates (CatBoost, the TFT grid
  shadows) are archived against the served level only where that is `tft`, so they are **not
  paired on season days**. That is the right population — on those days a replacement for TFT
  would not be served either — but a park in a long season contributes fewer paired days, which
  matters for the decisions dated December 2026.
- **`/plan/day`** reads the row's source with `servedLevelSource`, never `modelVersion === "tft"`:
  `dayPeak` is the season level and the H5 composer scales every ride's 56-day profile by it, at
  every lead it serves (`composer: "h5_season"`, archive code `e`, scored under `level_season`).
  The TFT gates (busy rides, d0–d2) were measured on TFT's forecast error and do not apply to a
  measured level; why, and what stays unmeasured, is in
  [slots and H5](../frontend/plan-day/slots-and-h5.md#on-a-day-of-a-running-park-season). The
  routed shadow stays TFT's routing: on a season day it is the plain profile, so the `tfth5`
  column of `planner_h5_slots` never scores the season level.

## A ride the season pauses

A season can also name rides that do not run while it lasts (`paused_attraction_ids`, River Quest
during Wintertraum). Such a ride measures nothing in the season, so it has no season level and
keeps the model's number — which is why two other rules take it out: the calendar and the yearly
view drop it from the day's average (`CALENDAR_SEASON_FILTER`, PAR-1326), `/plan/day` leaves it
out of the plan, and its own page serves no hourly forecast, best visit time or rope-drop
recommendation while it is shut.

## Known limits

- **Known limit:** the first days of a season have no reference, because only the running edition
  is read. The next step is the previous edition of the same season, once past editions are
  curated.
- **Known limit:** a season that runs on weekends only but is stored as a plain range counts its
  weekdays too. Listing its `dates` fixes it.
- **Known limit:** an evening event with its own ticket (`separate_ticket`, `opens_at`) is treated
  like an all-day season.
- **Known limit:** a weekday of a school break and a Saturday outside one share one level. A break
  week can queue above an ordinary Saturday; telling the two apart needs three measured days of
  each, more than most seasons have before their break starts.
- **Known limit:** Friday is a working day. In an evening-driven season (Wintertraum, Halloween)
  Fridays queue above Monday to Thursday, so a Friday is served the working days' median, which
  reads it low. A third day type would need three measured Fridays first.
- **Known limit:** every working day of a season carries one number and every day off another,
  the median over all of the season's days of that type so far, so the level is flat within a
  type and lags a season that builds (Advent): November's days outvote December's for weeks.
  Scaling the model's own number by the measured-to-forecast ratio on the same type would keep
  the model's day-to-day structure; it is not measured (PAR-1376).
- **Known limit:** a park without a region code in a country whose school breaks are regional
  reads only nationwide rows, so its break days are working days.
- Not measured against the models; `scripts/ml-review/park-season-coverage.sql` and the `season`
  rows of the lead snapshots are where to read it.
