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
| GB (England) | school, `GB-ENG`, one local authority | [Staffordshire County Council](https://www.staffordshire.gov.uk/schools-and-learning/school-term-dates) | curated file | `curated:GB:` | school through 2029-08-31 (PAR-1072); Scotland, Wales, Northern Ireland unknown |
| JP (Chiba, Aichi) | school, `JP-12` / `JP-23`, one city each | Urayasu City ([board minutes 2025-10](https://www.city.urayasu.lg.jp/_res/projects/default_project/_page_/001/045/711/202510teireikai.pdf)), [Nagoya City](https://www.city.nagoya.jp/kodomo/schools/1027693/1016802/1016803/1016804.html) | curated file | `curated:JP:` | Chiba through 2026-08-31, Aichi through 2027-03-31 (PAR-1072); Osaka, Yamanashi and every other prefecture unknown |
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
| `curated-holidays-gb.ts` | when Staffordshire adds 2029-30 (it publishes about three years ahead; the live page drops the year just ended) | staffordshire.gov.uk "School term dates" |
| `curated-holidays-jp.ts` | spring, for the school year starting in April (Nagoya updates its 年間スケジュール page; Urayasu: check 第18条の2 and the board's minutes for a change) | the city's own page or regulation |
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
| CA | Los Angeles Unified ([calendar page](https://www.lausd.org/apps/pages/index.jsp?uREC_ID=4432518&type=d&pREC_ID=2672183), PDFs [2025-26](https://media.edlio.net/135dff18/8fc3b7d4/7659581d/517e3313317f408baf4c01d7f43b5582), [2026-27 rev 6.9.26](https://media.edlio.net/135dff18/cc2a29d3/f0dbab90/03abacb9bbd44b1c8b86d1828e9ad968?_=2026-27%20Instructional%20Cal_Eng_Month_rev%206.9.26.pdf) and [2027-28](https://media.edlio.net/135dff18/8fc3b7d4/7659581d/160a549f27d044bab5ea9fb555faddc1?_=2027-28%20Instructional%20Cal_Eng_Month.pdf)) | Universal Studios Hollywood, and the LA basin that feeds Disneyland, Knott's and Magic Mountain | 2025-26, 2026-27, 2027-28 ("Board Approved", adoption "tentative" pending union negotiations); summer 2025 from 07-01, the first month the 2025-26 calendar shows |
| PA | Derry Township School District, Hershey ([2026-27 Year at a Glance](https://www.hershey.k12.pa.us/calendar/accessible-calendar), [event calendar](https://www.hershey.k12.pa.us/calendar/calendar-view) for 2025-26 and the end of 2024-25) | Hersheypark; Kennywood, Knoebels, Dorney Park and Sesame Place Langhorne get its calendar too | summer 2025, 2025-26, 2026-27; 2027-28 not published, so summer 2027 stops on 2027-06-30 |
| OH | Sandusky City Schools ([2026-27 announcement](https://www.scs-k12.net/protected/ArticleView.aspx?iid=6I3UUB0&dasi=3GGI); calendars are Google Sheets linked from [scs-k12.net](https://www.scs-k12.net/), the 2024-25 and 2025-26 links read from Wayback captures of the homepage) | Cedar Point, Cedar Point Shores; Kings Island gets it too | summer 2025, 2025-26, 2026-27; summer 2027 stops on 2027-06-30 |
| MO | North Kansas City Schools ([calendar page](https://www.nkcschools.org/calendar3): 2026-27 and 2027-28, both board-approved 2026-01-13; the [2025-26 PDF](https://resources.finalsite.net/images/v1746025243/nkcschoolsorg/wmwzhsobrfo09ejjifns/2025-26AcademicCalendar.pdf) and the revised [2024-25 PDF](https://resources.finalsite.net/images/v1706205818/nkcschoolsorg/gaewvmwdji6uqglgeelm/2024-25_Academic_Calendar_Traditional.pdf), linked from that page in 2025 per its Wayback captures, still on the district's file host) | Worlds of Fun, Oceans of Fun; the two Eureka parks (Six Flags St. Louis area) and Silver Dollar City get it too | summer 2025 through 2027-28; summer 2028 stops on 2028-06-30 |
| VA | Williamsburg-James City County Public Schools ([2024-25](https://wjccschools.org/wp-content/uploads/2023/10/2024-25-Academic-Calendar.pdf), [2025-26](https://wjccschools.org/wp-content/uploads/2021/07/2025-26-Academic-Calendar.pdf), [2026-27](https://wjccschools.org/wp-content/uploads/2025/10/2026-27-Academic-Calendar.pdf), [2027-28 adopted 4/1/26](https://wjccschools.org/wp-content/uploads/2021/07/2027-28-Academic-Calendar-4-1-26.pdf)) | Busch Gardens Williamsburg, Water Country USA; Kings Dominion gets it too | summer 2025 through 2027-28; summer 2028 stops on 2028-06-30 |
| TN | Sevier County Schools ([calendar page](https://www.sevier.org/7073): 2025-26 revised January 2026, 2026-27) | Dollywood | 2025-26 (summer 2025 from 08-01, see below), 2026-27; summer 2027 stops on 2027-06-30 |
| NC | Charlotte-Mecklenburg Schools ([calendar page](https://www.cmsk12.org/calendar): 2026-27 approved 2025-04-08, 2027-28 approved 2026-04-14; 2024-25 revised 01.23.25 and 2025-26 read from its Wayback capture of 2025-05-22) | Carowinds | summer 2025 through 2027-28; summer 2028 stops on 2028-06-30 |

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

**No projected summers.** `synthesizeProjectedSummerHolidays` projects last
year's summer forward for a region whose current summer looks marker-only
(< 25 June–September days). A curated file that ends inside a summer looks
exactly like that — LAUSD's ends on 2028-06-30, 23 days — and in 2028 the
sync would have written `synth-summer:US:US-CA:` rows for 06-05..08-11, an
invented break. Regions that appear in a curated file with `school` rows
(`CURATED_SCHOOL_REGIONS`, `holidays.service.ts`) are skipped; synced
countries (IT, ES, IE) are projected as before.

### Wave 2: PA, OH, MO, VA, TN, NC (PAR-1074)

The same rules, six more states, one district each. Production `regionCode`
for all 18 parks was read on 2026-10-25 and is the bare state code (PA, OH,
MO, VA, TN, NC).

**Which district.** The one at the park our crowd data cares most about,
where the choice was not obvious:

- **OH — Sandusky, not Mason.** Cedar Point and Cedar Point Shores are two of
  the three Ohio parks and Cedar Point is the larger draw; Kings Island
  (Mason City Schools) gets Sandusky's calendar.
- **MO — North Kansas City, not Branson or St. Louis.** Production has five
  MO parks: Worlds of Fun and Oceans of Fun in Kansas City's Northland,
  Hurricane Harbor and "Mid-America Parks" in Eureka (the Six Flags St. Louis
  site), and Silver Dollar City in Branson. Kansas City and Eureka tie at two
  parks. The Kansas City parks draw from their own metro, so a metro district
  says something about their crowds; Silver Dollar City draws tourists from
  across the Midwest, for whom Branson R-IV's calendar says little. North
  Kansas City Schools also has 2027-28 approved. Rockwood (Eureka) is the
  candidate if St. Louis ever needs its own key.
- **PA — Derry Township (Hershey)** for Hersheypark; Kennywood (Pittsburgh)
  and Dorney Park (Allentown) are far from it and get it anyway.
- **VA — Williamsburg-James City County** (Busch Gardens, Water Country USA);
  Kings Dominion is 80 km north.

**What is not listed.** Days off for some grades only (Derry's K-5
conference days and 6th-grade conference day, WJCC's PK-5 days), half days
and early releases (they are school days), and WJCC's asynchronous remote
learning day 2026-04-21 (instruction at home, not a day off). Derry
Township's two sources disagree on 2027-03-09 and 2027-03-18 (the Year at a
Glance says K-5 on 03-09 and all students on 03-18, the event calendar the
reverse), so neither is listed; 03-19 is off in both. A district's own
calendar wins over its plan: Derry's 2026-04-02 was planned as spring recess
and became a snow make-up day (its event calendar says "School in Session"),
so spring 2026 starts on 04-03. For the other districts the published 2025-26
calendar is used as published; Charlotte-Mecklenburg waived its 2025-26
make-up days (WFAE, 2026-03-17, a cross-check only). Make-up days scheduled
after the last day (NKC, CMS) are inside the summer, as planned. In Sandusky's
sheets a day is off when it is coloured "No School" or "Professional Learning
Day - No School"; conference days (grey) are school days. Sandusky 2026-27
contradicts itself like Derry: 2026-10-05 and 2027-03-19 both read
"Professional Learning Day (1/2 Day), Family Conferences (1/2 Day)", yet
10-05 is coloured "PL Day - No School" and 03-19 "Parent/Teacher
Conferences", so neither is listed. Its 2027-05-20 "District-wide Mental
Health Day" is coloured "District Event", which does not say whether students
attend, so it is not listed either. Sevier County's 2027-03-26 (Good Friday)
is a holiday in the PDF's holiday list but a half day ("HD") in the grid and
in the key dates; a half day is a school day, so it is not listed.

**Where it stops.** No district has published the year after its last
calendar, so each summer stops on June 30 of that calendar's last month
(LAUSD's rule): PA, OH and TN on 2027-06-30, MO, VA and NC on 2028-06-30.
Sevier County shows neither its 2024-25 calendar nor its last day anywhere
on sevier.org (its event API is empty before 2026), so summer 2025 for TN
starts on 2025-08-01, the first day its 2025-26 calendar shows; days before
read as 0. Every other new state starts with the day after its 2024-25 last
day.

**Neighbours.** `REGION_INFLUENCES` (`src/common/region-influences.ts`) lists US neighbour states (`US-FL` →
GA, AL, SC, **NC**, **TN**; `US-OH` → **PA**, …; `US-PA` → **OH**, …), so the
new rows also reach parks in neighbouring states, but not every neighbour
feature:

- **CatBoost `neighbor_school_holiday_count` stays NaN.** `holiday_features.py`
  sets `neighbors_known = False` as soon as one influencing region has no
  coverage, and every US influence list still contains an uncovered state.
- **CatBoost `is_school_holiday_any`** goes from NaN to 1 on days a curated
  neighbour is on break.
- **TFT `is_holiday_neighbor`** (`nf-service/db.py`) counts neighbour public
  or school dates with no coverage gate, so it goes from a confident 0 to 1 —
  for the 13 Orlando parks on every NC and TN break.
- **The calendar's `neighborHolidays`** lists them.

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
  school's choice and is not listed.

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
either. The 38 US parks outside FL/TX/CA and their neighbour states read as
unknown — which they were not before this change, see below. Since PAR-1074
that is the 21 parks outside the nine curated states.

### Truman Day was a school holiday

Nager.Date lists Truman Day (US-MO, May 8) with types `["School",
"Authorities"]`. `mapHolidayType` ranks School above Authorities, so it was
stored as `school` — the only US school rows. A Missouri park read May 8 as a
school break, and through the country fallback every US park got a school
coverage up to the latest of them, 2028-05-08: Cedar Point's calendar for
2027-07-01 said `isSchoolVacation: false` with no `schoolVacationUnknown`.
In the US, school days off are set per district, so a state-wide Nager tag
says nothing about a school: `NAGER_SCHOOL_IGNORED` (`holidays.service.ts`)
drops the School tag for the US and the row becomes `public` (Authorities: a
state holiday, offices closed). Elsewhere the tag stays meaningful (Austria's
patron saints' days are school-free) and is kept. The next sync rewrites the
2025–2028 rows (same `externalId`); a 2024 row, outside the sync window,
keeps `school` and must be fixed by hand after the deploy (it would otherwise
keep counting as US school data):

```sql
UPDATE holidays SET "holidayType" = 'public'
 WHERE country = 'US' AND name = 'Truman Day' AND "holidayType" = 'school';
```

### What the models see

Before this change every US park was "covered" through 2028-05-08 by the
Truman Day rows and the country fallback, with every day 0 (no break) except
May 8 in Missouri. After the sync and the 2024 fix:

| Parks | CatBoost school columns | TFT `is_school_holiday` | Calendar | Chronos-2 `holidayDataThrough` |
|---|---|---|---|---|
| FL / TX / CA (39), and since PAR-1074 PA / OH / MO / VA / TN / NC (18) | known 0 → real 0/1 inside the coverage, NaN past it | 0 → real 0/1 (`school_holiday_known` 1 inside, grid only) | breaks shown; `schoolVacationUnknown` past the coverage | ~2028-05-08 → the state's last school day |
| the other US states' parks (~38 before PAR-1074, 21 after) | known 0 → NaN | stays 0, `school_holiday_known` 1 → 0 | `schoolVacationUnknown: true` on every day | ~2028-05-08 → `none` |
| AU parks outside QLD, AU neighbours of QLD parks | known 0 (QLD fallback) → NaN | stays 0, `school_holiday_known` → 0 | unknown | QLD's date → `none` |
| FL / OH / PA / … parks whose `REGION_INFLUENCES` neighbours gained a district (PAR-1074) | `neighbor_school_holiday_count` stays NaN (an uncovered neighbour remains); `is_school_holiday_any` NaN → 1 on a neighbour's break | `is_holiday_neighbor` 0 → 1 on a neighbour's break | `neighborHolidays` lists the break | unchanged |

Nothing is retrained for it: the current CatBoost model sends NaN to the side
of 0 (`nan_mode = Min`), so serving it changes only on the days that are now
1; the next daily training learns from both the breaks and the NaN. TFT's
default inputs are finite; the next TFT training learns from the new 0/1. Until then
the served TFT sees `is_holiday_neighbor` = 1 on US neighbour breaks it never
had in training (PAR-1074), a train/serve gap that closes with the next TFT
retrain.

## Great Britain (PAR-1072)

Nager.Date has GB bank holidays per nation (`GB-ENG`, `GB-SCT`, `GB-WLS`,
`GB-NIR`) and no school data; OpenHolidays has no GB. In England term dates
are set by each local authority for its community and voluntary-controlled
schools, and academies may set their own. `curated-holidays-gb.ts` copies one
council's dates.

**Production region codes (read 2026-10-25).** All seven GB parks are in
England: Alton Towers, Blackpool Pleasure Beach, Chessington, LEGOLAND
Windsor, Paultons Park and Thorpe Park store `ENG`, Flamingo Land stores
`England` (the geocoder's long name, aliased to `ENG` by
`normalizeRegionCode` and `REGION_ALIASES` in both Python services). The rows
are keyed `GB-ENG`, so every park gets them.

| Council | Parks it stands for | School years |
|---|---|---|
| Staffordshire County Council ([school term dates](https://www.staffordshire.gov.uk/schools-and-learning/school-term-dates), updated 2026-10-08; 2024-25 / 2025-26 from its Wayback captures of [2025-09-10](http://web.archive.org/web/20250910231343/https://www.staffordshire.gov.uk/education/termdates.aspx) and [2026-01-27](http://web.archive.org/web/20260127195849/https://www.staffordshire.gov.uk/schools-and-learning/school-term-dates)) | all seven England parks; Alton Towers is in Staffordshire | May half term 2025, summer 2025, 2025-26, 2026-27, 2027-28, 2028-29 (summer 2029 to 08-31, the page's last day) |

**What is listed.** Every holiday the page names, the May Day bank holiday
(inside the term) and the one inset day per year the council fixes
(2025-09-01, 2026-09-01, 2027-09-02, 2028-09-04), which joins the summer. The
other four inset days are each school's choice and are not listed. A bank
holiday that sits between term and holiday is part of the holiday: Good
Friday 2027 (term ends Thursday 25 March) and 3 January 2028 (term starts
Tuesday 4 January). 2026-07-20 is a school day: the council only
"encourages" it as an inset day.

**One council for England.** The key is the nation, so one calendar stands
for all seven parks. Staffordshire was chosen because it publishes three
years ahead and its 2026-27 dates are the common pattern: Windsor &
Maidenhead (LEGOLAND, [2026/27](https://www.rbwm.gov.uk/schools-and-education/school-term-dates/school-term-dates-202627))
publishes the same 2026-27 year day for day, from the 1 September inset day
to the 21 July last day. Others are not: Surrey (Thorpe Park, next to
Chessington and LEGOLAND) has a two-week October half term in 2026
(19–30 October) and ends its 2026-27 year on 28 July; its 2028 Easter break
runs 3–17 April where Staffordshire's is 10–21 April. Those weeks are wrong
for Thorpe Park. Two councils under one key would be unioned and every break
they hold in different weeks would read twice as long, so none was added.
Hampshire's site (hants.gov.uk, Paultons Park) answered 403 to scripted
requests on 2026-10-25 and was not compared. Per-park councils need a sub-nation
key, the same follow-up as the US.

**Scotland, Wales and Northern Ireland have no rows** and read as unknown:
GB is in `REGION_ONLY_SCHOOL_COVERAGE`, so `GB-SCT` and `GB-WLS` — every GB
park lists both as influencing regions — are not covered by England's rows.

**What the models see.** Before this change GB had no school row, so every GB
day was unknown (CatBoost NaN, TFT `school_holiday_known` 0, calendar
`schoolVacationUnknown`). After the sync the seven parks read real 0/1 for
`is_school_holiday_primary` and the calendar through 2029-08-31. The
neighbour count stays NaN, because Scotland and Wales are unknown; the
three-valued `is_school_holiday_any` is 1 on England's breaks. Nothing is
retrained for it: the current CatBoost model sends NaN to the side of 0
(`nan_mode = Min`), so serving changes only on the days that are now 1.

## Japan (PAR-1072)

Nager.Date has Japan's public holidays (national rows) and no school data.
School breaks (長期休業日) of public elementary and junior high schools are
set by each municipal board of education in its school regulation
(学校管理規則), sometimes as fixed dates, sometimes as a window inside which
each school sets its own. `curated-holidays-jp.ts` copies them for the city a
park is in, where the city states the dates.

**Production region codes (read 2026-10-25).** Tokyo Disneyland and
DisneySea `12` (Chiba), LEGOLAND Japan `23` (Aichi), Universal Studios Japan
`27` (Osaka), Fuji-Q Highland `Yamanashi` (a name, not the ISO number 19).
The rows are keyed `JP-12` / `JP-23`, which `normalizeRegionCode` turns into
the parks' `12` / `23`.

| Key | City | Parks | Breaks listed | Basis |
|---|---|---|---|---|
| `JP-12` | Urayasu (浦安市教育委員会) | Tokyo Disneyland, Tokyo DisneySea | summer 2026 (2026-07-21..08-31) only | the [October 2025 minutes](https://www.city.urayasu.lg.jp/_res/projects/default_project/_page_/001/045/711/202510teireikai.pdf): "令和８年度から…夏季休業期間を７月21日から８月31日までとすることとした", ending a trial (令和3–7年度) in which summer ended on 8/24 |
| `JP-23` | Nagoya (名古屋市教育委員会) | LEGOLAND Japan | winter 2025-26, spring 2026, summer 2026, winter 2026-27, spring 2027 to 03-31 | [年間スケジュール](https://www.city.nagoya.jp/kodomo/schools/1027693/1016802/1016803/1016804.html), dates per school year (冬休み, 夏休み; spring from the 修了式 and 始業式 dates) |

**Rules versus dates.** Urayasu's other breaks are fixed in its school
regulation (浦安市立小学校及び中学校管理規則 第18条の2; reported as 学年始め
4/1–4/6, 冬季 12/24–1/5, 学年末 3/26–3/31 and 県民の日). The regulation names
no year, and its text sits behind the Reiki-Base viewer
(krg028.legal-square.com), which the independent cross-check could not
open — so the dates were not confirmed by a second reader and are **not
listed**. Add them once someone has read 第18条の2 in a browser; until then
Disney's days after 2026-08-31 read as unknown. Summer 2025 is not listed:
the trial ended it on 8/24 and its start is not published (elementary and
junior high differed). 令和9年度 is not listed for either city: neither has
published it. Nagoya's spring break is
the days between 修了式 (03-24) and the junior high 始業式 (2026-04-06;
elementary starts 04-07), without the weekend before it. Nagoya's single
days (県民の日学校ホリデー, ラーケーションの日) are a school's or a family's
choice and are not listed.

**Left out.**

- **Osaka (USJ, `27`).** 大阪市立学校管理規則 第2条の2 sets summer 7/21–8/24,
  winter 12/25–1/7 and spring 3/25–4/7 for elementary and junior high, as
  quoted in board agendas of [2021](https://www.city.osaka.lg.jp/kyoiku/cmsfiles/contents/0000531/531729/giann30.pdf)
  and (unchanged paragraph) [2024](https://www.city.osaka.lg.jp/kyoiku/cmsfiles/contents/0000623/623570/gian38.pdf).
  The current text could not be read (no public reiki found), no page states
  dates for a school year, and since 2021 each school may move a boundary
  by a day. Not listed.
- **Yamanashi (Fuji-Q).** Fuji-Q is in Fujiyoshida. The only readable copy
  of 富士吉田市立小・中学校管理規則 第29条 is a 2014 Wayback capture, and it
  gives windows (summer "7月20日から8月31日までの間", at most 75 days a
  year), not dates; the live reiki (g-reiki.net) answered 403. Not listed.
- **Neighbour prefectures** (Tokyo, Kanagawa, Saitama for Disney; Mie, Gifu,
  Shizuoka for LEGOLAND) have no rows.

JP is in `REGION_ONLY_SCHOOL_COVERAGE`, so Osaka, Yamanashi and every
neighbour prefecture read as unknown rather than borrowing Chiba's or
Aichi's coverage.

**What the models see.** Before: no JP school row, every day unknown. After
the sync, the three Chiba and Aichi parks read real 0/1 for
`is_school_holiday_primary` and the calendar inside their coverage (Disney
through 2026-08-31, i.e. history only; LEGOLAND through 2027-03-31); USJ and
Fuji-Q stay unknown on every day. The neighbour count stays NaN for Disney
and LEGOLAND, whose neighbour prefectures are unknown. Nothing is retrained.

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
