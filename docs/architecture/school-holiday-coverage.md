# School-holiday coverage (PAR-1049)

A school break that the upstream source has not published yet has no row in
`holidays`, and a missing row used to read as "no school holiday" — in every
model and on every calendar day. That is the rule in
[An absent fact never becomes a confident one](../rules/absent-facts.md) broken
by our own data edge.

## What was measured (2026-10-24)

| Country | Last school row | Public rows reach |
|---|---|---|
| ES | 2026-09-11 | 2028 |
| IT | 2026-09-15 | 2028 |
| HU | 2026-08-31 | 2028 |
| MX | 2026-07-15 | 2028 |
| IE | 2026-04-12 | 2028 |
| SE, SG | none | 2028 |

OpenHolidays itself holds no ES/IT/HU school holidays for 2026-09 … 2027-08
(DE has 110), and returns none for SE even for 2025. So nothing is projected or
invented: past the edge, the answer is **unknown**.

Sources outside Europe (KR from NEIS, curated SG / AU-QLD / CN / US / GB / JP files) and
how far each reaches: [Holiday sources outside Europe](holiday-sources-outside-europe.md).

| Country | School data (PAR-1071 / PAR-1074 update, 2026-10-25) |
|---|---|
| US | curated, one district per state: FL (Orange County) through 2028-08-09, TX (Northside ISD) through 2028-07-31, CA (Los Angeles Unified) through 2028-06-30; MO (North Kansas City), VA (Williamsburg-James City County) and NC (Charlotte-Mecklenburg) through 2028-06-30; PA (Derry Township), OH (Sandusky) and TN (Sevier County) through 2027-06-30; every other state unknown |
| AU | QLD through 2029-12-31; every other state unknown |
| GB | curated, England from one council (Staffordshire) through 2029-08-31; Scotland, Wales, Northern Ireland unknown (PAR-1072) |
| JP | curated per city: Aichi (Nagoya) through 2027-03-31; Chiba, Osaka, Yamanashi and every other prefecture unknown (PAR-1072) |

## Coverage

The coverage is derived from the stored rows, so there is no schema change: the
last `school` date per `(country, region)` (`MAX(date) GROUP BY country, region`).
A location's coverage is the later of its own region's rows and the national
rows. A region that no row names falls back to the latest row of any region of
its country — a region-matching gap is a different bug and stays as it was.
Not in the US, AU, GB and JP (`REGION_ONLY_SCHOOL_COVERAGE`, PAR-1071 / PAR-1072): there school
data is curated for some states only, so a state without rows is unknown
rather than covered by another state's calendar. A
country without a single school row has no coverage: every day is unknown.

The same rule exists twice, kept identical:

- Python: `holiday_features.SchoolCoverage` (ml-service, vendored byte-identical
  into nf-service).
- TypeScript: `buildSchoolCoverage` / `schoolCoverageThrough` in
  `src/common/utils/holiday.utils.ts`, read via
  `HolidaysService.getSchoolHolidayCoverage()`.

Only the upper edge is handled. Rows before the first synced school year read
as 0 as before. For GB and JP (PAR-1072) that is a change: a day before a
region's first curated row (England before 2025-05-05, Aichi before
2025-12-24) now reads 0 where it read NaN while the country had no school
row at all. It costs nothing in practice: park
history starts on 2025-12-24 (`queue_data`'s first row; the training window
in `ml-service/config.py` and [Model Overview](../ml/model-overview.md)
starts there too), so no wait-time day of a GB or Aichi park lies before
its first school row.

## What each consumer does past the edge

| Consumer | Behaviour |
|---|---|
| CatBoost (ml-service, training **and** inference via `assign_holiday_features(coverage=…)`) | `is_school_holiday_primary`, `neighbor_school_holiday_count`, `school_holiday_count_total`, `is_school_holiday_any` are **NaN**. A neighbour past its edge makes the count NaN; `is_school_holiday_any` stays 1 when any covered location is on a break (three-valued OR). Public-holiday columns are unchanged. |
| TFT (nf-service `add_calendar_covariates`) | NaN is not allowed in a futr_exog, so the placeholders stay (`is_school_holiday` 0, school distances 30) and a new `school_holiday_known` 0/1 says they are placeholders. It is in `FUTR_EXOG_GRID` only, so the default model's inputs are byte-identical. |
| Chronos-2 shadow | Covariate values unchanged (they must be finite). The per-curve `covariates.holidays` flag and `holidayDataThrough` use `holidays_through`: the earlier of the country's last public row and the park region's last school row, `none` when a type has no row. It used to be the last date over all types, which made Spain look covered to 2028. |
| Calendar (`CalendarDay`), `/best-days`, `/plan/day` context | `isSchoolVacation` keeps its boolean type (changing it would break clients); a day where it is `false` only because the data ends gets the additive `schoolVacationUnknown: true`. |
| `/plan/day` climatology | A planned or reference day with `schoolVacationUnknown` is `holidayState: "school_unknown"`, which matches only other such days. A country without any school data still looks back (unknown on both sides); Spain's unpublished days are not compared with its known past "regular" days. |

### CatBoost and NaN

The model is trained with the default `nan_mode = Min`. A model trained before
this change saw no NaN in these columns, and `Min` sends NaN to the side of the
lowest border — the side of 0 — so serving the current model is unchanged until
the next training. From the next daily training on, CatBoost may split NaN off
from 0 — but what it learns there is **not** "unknown school status". Training
rows are past days, and most countries' school data reaches past today, so
nearly all NaN rows in training come from the countries with no school data at
all (CN, GB, JP, KR, AU, SE, SG, …), plus the days since a short
coverage ran out (Ireland since 2026-04-12, Mexico since 2026-07-15, Hungary
since 2026-08-31). The NaN branch therefore mostly learns a country effect,
which the park/attraction categoricals already carry. At serve
time a future NaN day in Spain lands in that branch too. Rows inside the
coverage keep their 0/1 values.

## The summer projection

`synthesizeProjectedSummerHolidays` stays as it is: for the **current** year
only, a region that published only single-day "end of lessons" markers this
summer but had a real summer range last year gets last year's range shifted by
one year. It is a projection of one break type, not data, and it counts toward
the coverage (it is why ES ends on 2026-09-11). It is not extended to other
break types or years: the owner wants no invented values.

## Seeing the gaps

After each holiday sync, `HolidaysService.logSchoolCoverageGaps` logs one
warning naming the synced countries whose school data ends within 90 days
(regions sorted by date, at most five shown) or holds none:

```
School-holiday data ends within 90 days, later days read as unknown — ES: MD 2026-09-10, CT 2026-09-11, … +N more; SE: none
```
