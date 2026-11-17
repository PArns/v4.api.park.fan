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
| What is served | per ride, the **median of its measured daily P90** on the season's days already over, of the same day type (Saturday/Sunday or weekday), once that day type has 3 days |
| Otherwise | the model's number, unchanged |

There is no level from all of a season's days together. A season's first days are mostly its
opening weekend, so their median is a weekend's: on 2026-11-17, with Saturday, Sunday and Monday
of Phantasialand's Wintertraum measured, that median set every day of the season to "high" with
a 45-minute headliner peak, every Tuesday included, after the Monday had measured 25 minutes
(PAR-1375). A day type waits for its own three days instead: in a season that starts on a
Saturday, weekdays from the first Thursday and weekends from the second Sunday.

The measured day value is TFT's target statistic (`nf-service` `fetch_daily_peak_panel`): the P90
of the day's OPERATING STANDBY waits of at least 5 minutes, without heartbeat rows, from at least
three readings. Today is never a reference day. The two reads are in
`src/ml/utils/season-level.query.ts`, cached per park for six hours (`ml:season-level:v2:{parkId}`);
a failed read costs the season level, never the response.

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
- `/plan/day`'s H5 composer scales a busy ride by the TFT level at leads 0–2; a season-level row
  is not a TFT level, so such a ride gets the plain H5 profile.

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
- Not measured against the models; `scripts/ml-review/park-season-coverage.sql` and the `season`
  rows of the lead snapshots are where to read it.
