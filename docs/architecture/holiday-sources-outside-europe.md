# Holiday sources outside Europe (PAR-1065)

On 2026-10-24, 6,972 rows with `externalId LIKE 'peak:%'` were deleted from
production. They were fixed yearly windows typed `school` (CN "Spring Festival"
always 20.01.–15.02., KR "Chuseok" always 10.–20.09.), written by a generator
that was itself deleted in April 2026. After the deletion these countries had
no school-holiday data, which [PAR-1049](school-holiday-coverage.md) reads as
**unknown**, not as "no break". This page records what replaced them and where
each fact comes from. The rule behind it:
[An absent fact never becomes a confident one](../rules/absent-facts.md).
Facts about real places come from the publisher's own page, never from recall.

## What is imported

| Country | Type | Source | Form | `externalId` prefix | Coverage on 2026-10-24 |
|---|---|---|---|---|---|
| KR | school, per region | NEIS open API `SchoolSchedule` | API, needs `NEIS_API_KEY` | `neis:KR:` | as far as schools have entered their calendar |
| KR | public | KASI 특일 정보 `getRestDeInfo` | API, needs `KASI_SERVICE_KEY` | `kasi:KR:` | — |
| SG | school, national | MOE press releases for [2026](https://www.moe.gov.sg/news/press-releases/20250730-school-terms-and-holidays-for-2026) and [2027](https://www.moe.gov.sg/news/press-releases/20260714-school-terms-and-holidays-for-2027) | curated file | `curated:SG:` | school through 2027-12-31 |
| AU (QLD) | school, `AU-QLD` | Department of Education, [2026](https://education.qld.gov.au/about-us/calendar/term-dates) and [2027–2029](https://education.qld.gov.au/about-us/calendar/future-dates) | curated file | `curated:AU:` | school through 2029-12-31 |
| US (FL, TX, CA) | school, `US-FL` / `US-TX` / `US-CA`, one district per state | [Orange County Public Schools](https://www.ocps.net/110680_3), [Northside ISD](https://www.nisd.net/schools/calendars), [Los Angeles Unified](https://www.lausd.org/apps/pages/index.jsp?uREC_ID=4432518&type=d&pREC_ID=2672183) | curated file | `curated:US:` | FL through 2028-08-09, TX through 2028-07-31, CA through 2028-06-30; other states unknown |
| CN | public blocks + `working_day` | State Council notice for 2026, [Xinhua's authorised release](https://www.news.cn/politics/20251104/88bcffd88ae249e58c699b8772548e3d/c.html) | curated file | `curated:CN:` | no school data, so unknown |

Public holidays for all of them still come from Nager.Date as well. The rows
sit side by side with different `externalId`s, and every reader treats a day
with two public rows as one public holiday.

## The curated files

`src/holidays/data/curated-holidays-<cc>.ts`, one file per country, listed in
`CURATED_HOLIDAY_SETS` (`src/holidays/data/curated-holidays.ts`). Each entry is
`{ from, to, type, region, name, localName?, sourceUrl, retrievedAt }`. Both
bounds are inclusive park-local days, and `region` is the stored form
(`AU-QLD`) or `null` for the whole country. `curated-holidays.spec.ts` fails
on any entry without an https `sourceUrl` on the publisher's own host
(`moe.gov.sg`, `education.qld.gov.au`, `news.cn`/`gov.cn`), with an impossible
date or with a region outside its country.

The holiday sync (`HolidaysProcessor`, `POST /v1/admin/sync-holidays`) writes
each file with `HolidaysService.replaceHolidaysBySource`. That upserts the rows
and deletes the source's rows that are no longer in the file, so a corrected
entry does not leave its old days behind.

**Nothing is extrapolated.** A year the publisher has not announced has no
entry. Past the last entry the coverage ends, and the day reads as unknown:

- **QLD** publishes through 2029. The end of the 2029/30 summer break is "to be
  published when approved by the Minister", so that entry stops on 2029-12-31.
  The page says the break runs into January 2030 but not to which day.
  Student-free (staff professional development) days are included as `school`
  because students are off on them.
- **SG** publishes the next year in July. The 2028 release is due around July
  2027. Children's Day is for primary schools only, and its name says so.
- **CN**: the 2027 notice was not out on 2026-10-24. It usually appears in early
  November. Add it from the notice once it is published, never before.

### Yearly upkeep

| File | When | Where |
|---|---|---|
| `curated-holidays-cn.ts` | early November, for the next year | the 国务院办公厅 notice (Xinhua "受权发布" or gov.cn) |
| `curated-holidays-sg.ts` | July, for the next year | MOE press release "School Terms and Holidays for YYYY" |
| `curated-holidays-au.ts` | when QLD adds a year (currently through 2029) | education.qld.gov.au "future dates" |
| `curated-holidays-us.ts` | when a district publishes its next school year (Orange County publishes three ahead; Northside ISD two, approved each December) | the district's own calendar page, never an aggregator |

## United States (PAR-1071)

Neither Nager.Date nor OpenHolidays has US school data, and no free service
publishes it: US school calendars are set by each school district, not by the
state. `curated-holidays-us.ts` copies the calendars of one district per state
from the district's own pages (PDFs read with `pdftotext`, every date
weekday-checked).

| State | District | Parks it stands for | School years |
|---|---|---|---|
| FL | Orange County Public Schools ([2026-27](https://www.ocps.net/110680_3), [2027-28](https://www.ocps.net/school-calendar-2027-28), [2028-29](https://www.ocps.net/school-calendar-2028-29), [2024-25 / 2025-26](https://www.ocps.net/110681_3)) | the 13 Orlando parks (Disney, Universal, SeaWorld) | summer 2025, 2025-26, 2026-27, 2027-28; 2028-29's first day closes summer 2028 |
| TX | Northside ISD, San Antonio ([calendars](https://www.nisd.net/schools/calendars)) | SeaWorld / Aquatica San Antonio, Six Flags Fiesta Texas (Schlitterbahn New Braunfels nearby) | 2025-26, 2026-27 (approved 12/09/25), 2027-28 (approved 12/10/25) |
| CA | Los Angeles Unified ([calendar page](https://www.lausd.org/apps/pages/index.jsp?uREC_ID=4432518&type=d&pREC_ID=2672183), PDFs [2026-27 rev 6.9.26](https://media.edlio.net/135dff18/cc2a29d3/f0dbab90/03abacb9bbd44b1c8b86d1828e9ad968?_=2026-27%20Instructional%20Cal_Eng_Month_rev%206.9.26.pdf) and [2027-28](https://media.edlio.net/135dff18/8fc3b7d4/7659581d/160a549f27d044bab5ea9fb555faddc1?_=2027-28%20Instructional%20Cal_Eng_Month.pdf)) | Universal Studios Hollywood, and the LA basin that feeds Disneyland, Knott's and Magic Mountain | 2026-27, 2027-28 ("Board Approved", adoption "tentative" pending union negotiations); summer 2026 from 07-01, the first month its 2026-27 calendar shows |

**What is listed.** Every day students have no school: Thanksgiving, winter,
spring and summer breaks, plus single student holidays (teacher workdays,
Labor Day, MLK, Presidents' Day, district holidays). A bad-weather make-up day
(NISD 2026-02-16, 2027-02-16, 2028-02-22; OCPS uses Presidents' Day and
Thanksgiving Monday) is listed as the holiday it is planned to be; if a
district uses it, that day was a school day. Weekends inside a break are part
of the range, the ones around it are not. Summer runs from the day after the
last day of school to the day before the next first day.

**Where it stops.** Nothing past a district's last published day: Northside
ISD has not published 2028-29, so its summer 2028 stops on 2028-07-31, the
last day its 2027-28 calendar shows. NISD's 2024-25 calendar is no longer on
nisd.net, so TX summer 2025 is not listed. 2025-26 is included so that
training history does not read last school year as "no break"; days before a
state's first row read as 0, as for every country (PAR-1049 handles only the
upper edge).

### One district per state

Parks match holidays by state: they store `regionCode = FL`, the rows say
`US-FL`, and `isLocalHoliday` / the coverage compare the normalised codes. So
a state carries one calendar for all its parks. That is a simplification:
Busch Gardens and Adventure Island in Tampa and LEGOLAND Florida / Peppa Pig
in Winter Haven get Orange County's calendar, Six Flags Over Texas in
Arlington gets San Antonio's. Writing a second district under the same key
would union the two, and every break they hold in different weeks would
read as twice as long, so none was added:

- **Osceola County** (south of Orlando, part of Walt Disney World's area):
  not added — the state key already carries Orange County, and its calendar
  page needs a ParentSquare sign-in for the listing.
- **Arlington ISD**: published 2026-27 only (approved 2026-01-22, Google Drive
  PDFs linked from [aisd.net](https://www.aisd.net/district/about/calendars/));
  Northside ISD has two years and stands for more of our TX parks.
- **Anaheim Elementary** (Disneyland, board-approved 2026-27 at
  [anaheimelementary.org](https://anaheimelementary.org/calendars/)) has not
  published 2027-28, and **Anaheim Union High**'s site (auhsd.us) refused
  every connection on 2026-10-25. LAUSD has two years and is the larger
  catchment, so it carries `US-CA`. San Diego (SeaWorld, LEGOLAND, Sesame
  Place) and the Bay Area (Great America, Discovery Kingdom, Hurricane Harbor
  Concord) get LA's calendar too.
- **LAUSD access**: lausd.org answers 403 to scripted requests (Cloudflare).
  The calendar page was read through its Wayback capture of 2026-10-12, the
  PDFs directly from LAUSD's host (media.edlio.net); every date was checked
  against the PDF grid. The 2027-28 footnote repeats the 2026-27 pupil-free
  dates (a copy-over); the grid's 2027-08-11 / 2028-06-08 were used. The
  optional second-semester pupil-free day (2027-01-11, 2028-01-10) is each
  school's choice and is not listed. LAUSD's 2025-26 calendar was not read,
  so CA has no 2025-26 rows.

Per-park districts need a sub-state key on the park (a county or district
code) that every reader — `isLocalHoliday`, the coverage, the Python
`HolidayIndex`/`SchoolCoverage` — would have to match. That is a follow-up,
not part of this change.

### A state without a district is unknown

The coverage rule (PAR-1049) lets a region without rows fall back to the
latest school row of any region of its country, for codes that fail to match.
For the US that would have put Cedar Point (OH) on Orlando's coverage: every
Ohio day through 2028 would have read "known, no school holiday". The US and
AU (where only QLD is curated) are therefore in `REGION_ONLY_SCHOOL_COVERAGE`
(`src/common/utils/holiday.utils.ts`, and the same set in ml-service's
`holiday_features.py`, vendored byte-identical into nf-service): there a
region without rows has no coverage, and an influence given at country
level (`{"countryCode": "US"}` from a Canadian or Mexican park) has none
either. The 39 US parks outside FL/TX/CA and their neighbour states stay
unknown, exactly as before this change.

### Truman Day was a school holiday

Nager.Date lists Truman Day (US-MO, May 8) with types `["School",
"Authorities"]`. `mapHolidayType` ranks School above Authorities, so it was
stored as `school` — the only US school row, which a Missouri park read as a
school break and which gave the US a school coverage of one day per year.
In the US, school days off are set per district, so a state-wide Nager tag
says nothing about a school: `NAGER_SCHOOL_IGNORED` (`holidays.service.ts`)
drops the School tag for the US and the row becomes `public` (Authorities: a
state holiday, offices closed). Elsewhere the tag stays meaningful (Austria's
patron saints' days are school-free) and is kept. The next sync rewrites the
2025–2028 rows (same `externalId`); a 2024 row, outside the sync window,
keeps `school`:

```sql
UPDATE holidays SET "holidayType" = 'public'
 WHERE country = 'US' AND name = 'Truman Day' AND "holidayType" = 'school';
```

### What the models see

US parks in FL/TX/CA used to have no school coverage, so their school columns
were NaN (CatBoost) and `school_holiday_known = 0` (TFT grid). After the sync
they are 0/1 inside the coverage. Nothing is retrained for it: the current
CatBoost model sends NaN to the side of 0 (`nan_mode = Min`), so serving it
on the new values changes only the days that are now 1; the next daily
training learns from them. TFT (nf-service) has no NaN: its default
`is_school_holiday` covariate goes from the 0 placeholder to the real 0/1
for these parks, and the grid-only `school_holiday_known` from 0 to 1; the
next TFT training learns from it. Chronos-2's `holidayDataThrough` for these parks
moves from `none` to the state's last school day.

## China's make-up working days (调休)

The notice moves weekend days to make the holiday blocks longer. In 2026 these
are 01-04, 02-14, 02-28, 05-09, 09-20 and 10-10: Saturdays and Sundays with work
**and school**, so they tend to be quiet days. No feature knew about them before.

They are stored as `holidayType = 'working_day'`, a new value of the
`holidays.holidayType` enum. TypeORM `synchronize` adds it on deploy by
recreating the enum type and converting the column, which rewrites `holidays`
(≈199k rows, 164 MB on 2026-10-24) under an ACCESS EXCLUSIVE lock — a few
seconds in which every holiday read waits. Deploy outside the busy window.
**They are not holidays:**

- The readers in `HolidaysService` that take every type (`getHolidays`,
  `getAllHolidays`, `isHoliday`) filter them out (`NOT_A_WORKING_DAY`), so
  calendars, schedule gap-fill and the bridge-day rule never see them.
- The Python services (ml-service, nf-service, ml-bench) match holiday types
  by name (`PUBLIC_HOLIDAY_TYPES`, `SCHOOL_HOLIDAY_TYPES`, `== "public"`), so
  they ignore the rows as well.

So the rows are available, but no model reads them yet. Turning them into a
feature (for example, an `is_working_weekend` flag that overrides `is_weekend`)
means a training change, and it needs the ml-bench measurement and shadow
period that the [model switch rule](../ml/ml-overview.md) requires.

**HK as a neighbour.** HK parks already list CN in `COUNTRY_INFLUENCES`, but
the neighbour logic counts school holidays only (`isSchoolHolidayInInfluenceZone`,
`neighbor_school_holiday_count`). China's public holiday blocks therefore do
not reach HK parks through it. Making them do so is a feature change, so it
was not made here.

## Korea

### NEIS school vacations

`src/external-apis/korea/neis.client.ts`. Korean schools set their own breaks
inside the education office's guidelines, so one school does not stand for a
region:

1. For each Korean region a park sits in or is influenced by
   (`ParksService.getSyncRegionCodes("KR")`), resolve the education office
   (`kr-regions.ts`: 17 ISO 3166-2:KR codes ↔ NEIS office codes, read off the
   NEIS `schoolInfo` endpoint on 2026-10-24).
2. Sample 15 elementary schools of that office, spread evenly over the list.
3. Fetch each school's `SchoolSchedule` for last year through two years ahead.
   A **vacation day** is a `휴업일` (no-lessons day) whose event ends in `방학`.
   The ceremony `여름방학식` and the first day `개학식` are school days.
4. A school files one event per day. A public holiday or Saturday inside its
   break therefore shows up as `제헌절` / `토요휴업일`, not as `방학` (recorded:
   경기초등학교, 2026-07-17..19). A gap of at most 3 days between two
   vacation days is bridged.
5. A day is a school holiday for the region when **more than half** of the
   sampled schools that returned a calendar are on vacation. A school with an
   empty calendar does not vote.

Rows are written under the ISO code (`KR-41`, the form `region-influences.ts`
uses) and under the park's own stored region code when that differs, so both
lookups find them. A park region code that names no Korean region is logged
(`KR region codes that name no Korean region …`) and gets no rows. Its days
then read as unknown or fall back to the country's coverage, as described in
the coverage page.

**Production region codes (read 2026-10-24).** The KR parks store ISO numbers:
Caribbean Bay and Everland `41`, Legoland Korea `42`, Lotte World Adventure
`11`. The readers build `KR-41`/`KR-42`/`KR-11` from them, which is exactly
the region the NEIS rows carry. Other formats seen: AU `QLD` (rows `AU-QLD`),
SG empty (national rows), HK `NT` / `Hong Kong Island`. KASI, SG and CN rows
are all national, so they apply whatever the park's region string is.
`src/holidays/data/production-region-codes.spec.ts` pins these literal values.

**Follow-up — CN region codes are inconsistent.** The CN parks hold `Zhejiang`,
`Jiangsu`, `Gan Su Sheng`, `Gansu`, `44`, `CN-44`, `31`, `11`, `Tianjin` and
so on: names, pinyin with a suffix, ISO numbers, prefixed and not. The CN rows
here are national, so nothing depends on it today. Province-level data
(school holidays, `region-influences.ts`'s `CN-31`/`CN-44` entries) will only
match once the parks are normalised to ISO 3166-2 `CN-xx` — do that first,
never per-province string matching.

**Check after the first import:** the geocoder stores `normalizeRegionCode()`
of Google's `administrative_area_level_1.short_name`. That function keeps the
part after the last `-`, so a name like `Gyeonggi-do` would be stored as `do`,
which names no province. The sync logs such a code as unknown, and it needs a
region-code fix on the park, not a guess here.

```sql
SELECT name, "regionCode", "influencingRegions" FROM parks WHERE "countryCode" = 'KR';
SELECT region, min(date), max(date), count(*) FROM holidays
 WHERE "externalId" LIKE 'neis:KR:%' GROUP BY region;
```

Terms: without a key NEIS answers with a 5-row sample and ignores paging
(measured), so `NEIS_API_KEY` is required. The key is free on sign-up at
open.neis.go.kr. A licence statement for the NEIS open API itself was not
found (unverified); the education offices' datasets on data.go.kr say
"이용허락범위 제한 없음".

### KASI public holidays

`src/external-apis/korea/kasi.client.ts`, the `getRestDeInfo` operation of
data.go.kr dataset 15012690 (한국천문연구원_특일 정보). It is free, the key is
auto-approved, the licence reads "이용허락범위 제한 없음" (no restriction on
use), and a development key allows 10,000 calls a day. The sync makes 4 calls,
one per year. Only `isHoliday = "Y"` days are stored. The source includes
substitute holidays (대체공휴일) and holidays declared at short notice. The
response parsing follows the provider's API guide v1.4: `item` is a single
object when there is one, and `items` is `""` when there is none.

### Environment

| Variable | Needed for | Without it |
|---|---|---|
| `NEIS_API_KEY` | KR school vacations | skipped with a warning, KR school data stays unknown |
| `KASI_SERVICE_KEY` | KR public holidays from KASI (the decoded data.go.kr "일반 인증키") | skipped with a warning, Nager.Date still supplies KR public holidays |

A failed fetch writes and deletes nothing, so the last good import stays.

## Universal Studios Hollywood regionCode

USH had `regionCode = FL` while its coordinates are in Los Angeles. Its merge
winner had been geocoded at "Bull Creek" (28.0/-81.0, central Florida), which
wrote region Florida / `FL`. `POST /v1/admin/parks/:id/correct-location` then
fixed the city and coordinates but left region and regionCode alone, and the
geocode job never revisits a park that already has them. So the park was
matched against Florida holidays and Florida neighbours.

Fix: `ParkRenameService.correctLocation` re-derives `region`/`regionCode` by
reverse-geocoding the corrected coordinates. Only a successful lookup changes
them: an error or a null result (the client returns null on failure too)
keeps the old values and logs a warning; a successful result without a region
clears them, and the geocode job, which retries parks without a region, fills
them in later.

Repair (owner, against production — re-sends the coordinates the park already
has):

```bash
curl -X POST "https://api.park.fan/v1/admin/parks/<ush-park-id>/correct-location" \
  -H "Authorization: Bearer <owner session>" -H "Content-Type: application/json" \
  -d '{"latitude": <current latitude>, "longitude": <current longitude>}'
```

`influencingRegions` follows on the next park-enrichment run, which recomputes
it from `regionCode` (`US-CA`). Then run a holiday sync so CA holidays exist
for it.
