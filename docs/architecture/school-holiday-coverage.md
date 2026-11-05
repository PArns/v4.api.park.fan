# School-holiday coverage (PAR-1049)

Past a region's last stored school-holiday row, "no school holiday" is unknown, not false. This page
defines that coverage edge and what every consumer — the three forecasting models and the calendar —
does past it. Open it before reading or adding a school-holiday feature. Where the rows come from
outside Europe: [holiday sources outside Europe](holiday-sources-outside-europe.md). The rule behind it:
[absent facts](../rules/absent-facts.md).

## Why

A break the source has not published yet has no row in `holidays`, and a missing row used to read as
"no school holiday" in every model and on every calendar day. Measured 2026-10-24:

| Country | Last school row | Public rows reach |
| --- | --- | --- |
| ES | 2026-09-11 | 2028 |
| IT | 2026-09-15 | 2028 |
| HU | 2026-08-31 | 2028 |
| MX | 2026-07-15 | 2028 |
| IE | 2026-04-12 | 2028 |
| SE, SG | none | 2028 |

OpenHolidays holds no ES/IT/HU school holidays for 2026-09 … 2027-08 (DE has 110) and none for SE at
all. Nothing is projected or invented: past the edge the answer is unknown.

## The coverage rule

Derived from the stored rows, no schema: the last `school` date per `(country, region)`. A location's
coverage is the later of its own region's rows and the national rows. A region no row names falls back
to the latest row of any region of its country — **except** in `REGION_ONLY_SCHOOL_COVERAGE`
(US, AU, GB, JP), where school data is curated for some states only, so a state without rows (or an
influence given at country level) is unknown rather than covered by another state's calendar. A country
without any school row has no coverage.

The rule exists twice and is kept identical:

- Python: `holiday_features.SchoolCoverage` (ml-service, vendored byte-identical into nf-service);
- TypeScript: `buildSchoolCoverage` / `schoolCoverageThrough` and `REGION_ONLY_SCHOOL_COVERAGE` in
  `src/common/utils/holiday.utils.ts`, read via `HolidaysService.getSchoolHolidayCoverage()`.

Only the upper edge is handled: a day before a region's first row reads 0. That costs nothing in practice
because park history starts on 2025-12-24 (`queue_data`'s first row and the training window), and no
park's wait-time day lies before its region's first school row.

## Past the edge, per consumer

| Consumer | Behaviour |
| --- | --- |
| CatBoost (training and inference, `assign_holiday_features(coverage=…)`) | `is_school_holiday_primary`, `neighbor_school_holiday_count`, `school_holiday_count_total`, `is_school_holiday_any` are **NaN**. One neighbour past its edge makes the count NaN; `is_school_holiday_any` stays 1 when any covered location is on a break (three-valued OR). Public-holiday columns unchanged |
| TFT (nf-service `add_calendar_covariates`) | A futr_exog cannot be NaN, so the placeholders stay (`is_school_holiday` 0, distances 30) and `school_holiday_known` 0/1 marks them. It is in `FUTR_EXOG_GRID` only, so the default model's inputs are unchanged |
| Chronos-2 shadow | Values unchanged; the per-curve `covariates.holidays` flag and `holidayDataThrough` use `holidays_through`: the earlier of the country's last public row and the region's last school row, `none` when a type has no row |
| Calendar (`CalendarDay`), `/best-days`, `/plan/day` context | `isSchoolVacation` stays boolean; a day `false` only because the data ends also gets `schoolVacationUnknown: true` |
| `/plan/day` climatology | A planned or reference day with `schoolVacationUnknown` is `holidayState: "school_unknown"` and matches only such days, so Spain's unpublished days are not compared with its known regular days |

**CatBoost and NaN.** Training uses the default `nan_mode = Min`, which sends NaN to the side of the
lowest border, 0, so a model trained before this change serves as before. Later trainings may split NaN
from 0, but training rows are past days and most countries' data reaches past today, so the NaN branch
mostly learns a country effect (the countries without school data, plus Ireland since 2026-04-12, Mexico
since 2026-07-15, Hungary since 2026-08-31), which the park categoricals already carry. A future NaN day
in Spain lands in that branch too.

## The summer projection

`synthesizeProjectedSummerHolidays` stays: for the **current** year only, a region that published only
single-day "end of lessons" markers this summer (< 25 June–September days) but had a real summer range
last year gets last year's range shifted one year (`synth-summer:` rows). It counts toward the coverage
(why ES ends on 2026-09-11). It is not extended to other break types or years. Regions with curated
school rows (`CURATED_SCHOOL_REGIONS`, `holidays.service.ts`) are skipped, because a curated file that
ends inside a summer looks exactly like a marker-only region.

## Seeing the gaps

After each holiday sync `HolidaysService.logSchoolCoverageGaps` warns once, naming the synced countries
whose school data ends within 90 days (at most five regions shown, sorted by date) or holds none:

```
School-holiday data ends within 90 days, later days read as unknown — ES: MD 2026-09-10, CT 2026-09-11, … +N more; SE: none
```
