# Holiday sources outside Europe (PAR-1065)

Where school and public holidays come from for Korea, Singapore, Australia (QLD), China, the US, Great
Britain and Japan — the APIs and the hand-curated files — how far each reaches, what to update each
year, and why particular districts and dates were chosen. Open this before editing a
`curated-holidays-<cc>.ts` file or adding a country. Europe comes from Nager.Date and OpenHolidays; what a
day past the data's edge means is [school-holiday coverage](school-holiday-coverage.md).

On 2026-10-24 the 6,972 `peak:%` rows were deleted: fixed yearly windows typed `school` (CN "Spring
Festival" always 20.01.–15.02.) from a generator removed in April 2026. Facts about real places come from
the publisher's own page, never from recall ([absent facts](../rules/absent-facts.md)).

## What is imported

| Country | Type | Source | `externalId` prefix | Coverage (2026-10-25) |
| --- | --- | --- | --- | --- |
| KR | school, per region | NEIS open API `SchoolSchedule` (`NEIS_API_KEY`) | `neis:KR:` | as far as schools have entered their calendars |
| KR | public | KASI `getRestDeInfo` (`KASI_SERVICE_KEY`) | `kasi:KR:` | — |
| SG | school, national | MOE press releases | `curated:SG:` | through 2027-12-31 |
| AU-QLD | school | Queensland Department of Education | `curated:AU:` | through 2029-12-31; other states unknown |
| US | school, one district per state | the districts' own calendars | `curated:US:` | FL 2028-08-09, TX 2028-07-31, CA/MO/VA/NC 2028-06-30, PA/OH/TN 2027-06-30; other states unknown |
| GB-ENG | school | Staffordshire County Council | `curated:GB:` | through 2029-08-31; Scotland, Wales, NI unknown |
| JP-23 (Aichi) | school | Nagoya City | `curated:JP:` | through 2027-03-31; other prefectures unknown |
| CN | public blocks + `working_day` | State Council notice (Xinhua's authorised release) | `curated:CN:` | 2026; no school data |

Nager.Date still supplies public holidays for all of them; a day with two public rows is one public
holiday to every reader.

## The curated files

`src/holidays/data/curated-holidays-<cc>.ts`, listed in `CURATED_HOLIDAY_SETS`
(`curated-holidays.ts`). Each entry is `{ from, to, type, region, name, localName?, sourceUrl,
retrievedAt }`, both bounds inclusive park-local days, `region` in stored form (`AU-QLD`) or `null` for
national. `curated-holidays.spec.ts` fails on an entry whose `sourceUrl` is not https on the publisher's
own host (`OFFICIAL_HOSTS`), on an impossible date, or on a region outside its country. The source and
the reasoning per district sit in each file's comments.

The holiday sync (`HolidaysProcessor`, monthly and via `POST /v1/admin/sync-holidays`) writes each file
with `HolidaysService.replaceHolidaysBySource`: upsert, then delete the source's rows no longer in the
file. **Nothing is extrapolated:** a year the publisher has not announced has no entry, and past the
last entry the day reads unknown.

### Yearly upkeep

| File | When | Where |
| --- | --- | --- |
| `curated-holidays-cn.ts` | early November, for the next year (2027 was not out on 2026-10-24) | the 国务院办公厅 notice (Xinhua "受权发布" or gov.cn) |
| `curated-holidays-sg.ts` | July, for the next year | MOE press release "School Terms and Holidays for YYYY" |
| `curated-holidays-au.ts` | when QLD adds a year (the 2029/30 summer's end is "to be published when approved by the Minister", so the data stops on 2029-12-31) | education.qld.gov.au "future dates" |
| `curated-holidays-gb.ts` | when Staffordshire adds 2029-30 (it publishes about three years ahead; the live page drops the year just ended) | staffordshire.gov.uk "School term dates" |
| `curated-holidays-jp.ts` | spring, for the school year starting in April (Nagoya's 年間スケジュール; 令和9年度 not yet published) | the city's own page or regulation |
| `curated-holidays-us.ts` | when a district publishes its next school year (Orange County three ahead, Northside ISD two, approved each December) | the district's own calendar page, never an aggregator |

QLD student-free (staff development) days are listed as `school`; Singapore's Children's Day is for
primary schools only and says so in its name.

## United States (PAR-1071)

Neither Nager.Date nor OpenHolidays has US school data, and calendars are set per school district.
`curated-holidays-us.ts` copies one district per state from the district's own pages (PDFs read with
`pdftotext`, every date weekday-checked); PAR-1074 added six states. Production `regionCode` for US parks
is the bare state code; rows are keyed `US-FL` and compared normalised.

| State | District | Parks it stands for |
| --- | --- | --- |
| FL | Orange County Public Schools | the 13 Orlando parks; Busch Gardens Tampa, Adventure Island, LEGOLAND Florida get it too |
| TX | Northside ISD (San Antonio) | SeaWorld/Aquatica San Antonio, Fiesta Texas, Schlitterbahn; Six Flags Over Texas gets it too |
| CA | Los Angeles Unified | Universal Studios Hollywood and the LA basin (Disneyland, Knott's, Magic Mountain); San Diego and the Bay Area get it too |
| PA | Derry Township (Hershey) | Hersheypark; Kennywood, Knoebels, Dorney Park, Sesame Place get it too |
| OH | Sandusky City Schools | Cedar Point, Cedar Point Shores; Kings Island gets it too |
| MO | North Kansas City Schools | Worlds of Fun, Oceans of Fun; the Eureka parks and Silver Dollar City get it too |
| VA | Williamsburg-James City County | Busch Gardens Williamsburg, Water Country USA; Kings Dominion gets it too |
| TN | Sevier County Schools | Dollywood |
| NC | Charlotte-Mecklenburg Schools | Carowinds |

**What is listed:** every day students have no school — Thanksgiving, winter, spring and summer breaks
and single student holidays (teacher workdays, Labor Day, MLK, Presidents' Day). A planned bad-weather
make-up day is listed as the holiday it is planned to be. Summer runs from the day after the last day of
school to the day before the next first day; weekends inside a break are part of it. 2025-26 is included
so training history does not read last year as "no break".

**Where it stops:** nothing past a district's last published day. Without a next calendar a summer stops
on June 30 of its last year (LAUSD's rule); Northside's summer 2028 stops on 2028-07-31, the last day its
2027-28 calendar shows. TX summer 2025 is missing (NISD's 2024-25 is no longer online); TN starts on
2025-08-01, the first day Sevier County's 2025-26 calendar shows.

**Why one district, and which.** Parks match by state, so a state carries one calendar; a second
district under the same key would be unioned and every break held in different weeks would read twice as
long. Per-park districts need a sub-state key every reader (`isLocalHoliday`, the coverage, the Python
`HolidayIndex`/`SchoolCoverage`) matches — a follow-up. Choices where it was not obvious: Sandusky over
Mason (Cedar Point is the larger draw); North Kansas City over Branson or St. Louis (the Kansas City parks
draw from their own metro, Silver Dollar City from the whole Midwest; Rockwood is the candidate if
St. Louis ever needs its key); Osceola County not added (Orange County holds the key, its listing needs a
sign-in); Arlington ISD had only 2026-27; Anaheim Elementary lacked 2027-28 and Anaheim Union High's site
refused connections, so LAUSD carries CA. lausd.org answers 403 to scripts: its page was read through a
Wayback capture (2026-10-12), the PDFs from media.edlio.net, every date checked against the PDF grid.

**Dates deliberately left out:** days off for some grades only (Derry's K-5 and 6th-grade conference days,
WJCC's PK-5 days), half days and early releases, WJCC's asynchronous learning day 2026-04-21, LAUSD's
optional second-semester pupil-free day. Where a district contradicts itself, neither reading is listed:
Derry 2027-03-09 / 03-18 (Year at a Glance and event calendar swap them; 03-19 is off in both), Sandusky
2026-10-05 and 2027-03-19 (label and colour disagree), Sandusky's 2027-05-20 "District-wide Mental Health
Day" (coloured "District Event"), Sevier County's 2027-03-26 (a holiday in the list, a half day in the
grid). A district's own calendar beats its plan: Derry's 2026-04-02 became a snow make-up day, so spring
2026 starts on 04-03. Make-up days after the last day (NKC, CMS) sit inside the summer as planned.

**No projected summers.** A curated file ending inside a summer looks like a marker-only region to
`synthesizeProjectedSummerHolidays` (LAUSD's 2028 summer has 23 June–September days), which would have
written an invented `synth-summer:US:US-CA:` break; regions in `CURATED_SCHOOL_REGIONS` are skipped.

**A state without a district is unknown.** The US (and AU, GB, JP) is in `REGION_ONLY_SCHOOL_COVERAGE`,
so a state without rows has no coverage instead of borrowing Orlando's, and an influence given at
country level (`{"countryCode": "US"}` from a Canadian or Mexican park) has none either. 21 US parks
outside the nine states read unknown.

**Truman Day was a school holiday.** Nager.Date tags Truman Day (US-MO, May 8) `["School",
"Authorities"]`, and `mapHolidayType` ranks School first, so it was the only US school row — and through
the country fallback it gave every US park "known, no school holiday" up to 2028-05-08.
`NAGER_SCHOOL_IGNORED` (`holidays.service.ts`) drops the School tag for the US (elsewhere it is
meaningful: Austria's patron saints' days). The sync rewrites 2025–2028; a 2024 row outside the window
needs, once:

```sql
UPDATE holidays SET "holidayType" = 'public'
 WHERE country = 'US' AND name = 'Truman Day' AND "holidayType" = 'school';
```

**Neighbours.** `REGION_INFLUENCES` (`src/common/region-influences.ts`) lists neighbour states, so new rows
reach parks next door, but not every feature: CatBoost's `neighbor_school_holiday_count` stays NaN while
any influence is uncovered (every US list still has one); `is_school_holiday_any` becomes 1 on a
neighbour's break; TFT's `is_holiday_neighbor` (`nf-service/db.py`) has no coverage gate, so it went from
a confident 0 to 1 (the Orlando parks on every NC and TN break) — a train/serve gap until the next TFT
training; the calendar's `neighborHolidays` lists them.

## Great Britain (PAR-1072)

Nager.Date has bank holidays per nation and no school data; OpenHolidays has no GB. Term dates are set
per local authority (academies may differ). All seven GB parks are in England and store `ENG` (Flamingo
Land stores `England`, aliased by `normalizeRegionCode` and `REGION_ALIASES` in both Python services);
rows are keyed `GB-ENG`. Staffordshire (Alton Towers' council) stands for all seven: it publishes three
years ahead and its 2026-27 matches Windsor & Maidenhead (LEGOLAND) day for day. Surrey (Thorpe Park,
Chessington) differs — a two-week October half term in 2026, a later end of year, a different 2028 Easter
— and Hampshire (Paultons) answered 403; two councils under one key would be unioned. Listed: every
holiday the page names, the May Day bank holiday, and the one inset day per year the council fixes; the
other four inset days are each school's choice. A bank holiday between term and holiday is part of the
holiday (Good Friday 2027, 3 January 2028); 2026-07-20 is a school day. Scotland, Wales and Northern
Ireland have no rows and read unknown (GB is region-only), which also keeps every GB park's neighbour
count NaN.

## Japan (PAR-1072)

Nager.Date has national public holidays only. School breaks (長期休業日) are set by each municipal board
of education, sometimes as fixed dates, sometimes as a window inside which each school decides.
Production region codes: Tokyo Disney Resort `12` (Chiba), LEGOLAND Japan `23` (Aichi), USJ `27` (Osaka),
Fuji-Q `Yamanashi`. Rows are keyed `JP-23`. Nagoya lists winter 2025-26 through spring 2027; spring break
is the days between 修了式 and the junior-high 始業式; its single days (県民の日学校ホリデー,
ラーケーションの日) are a school's or family's choice and are not listed. Left out:

- **Urayasu (Tokyo Disney)** states only summer 2026; its other breaks are in 第18条の2 of its school
  regulation, behind a viewer the cross-check could not open. Summer 2026 alone would have turned
  Disney's real winter and spring breaks from unknown into a confident 0. Add it once 第18条の2 is read.
- **Osaka (USJ)**: the regulation's dates are quoted in 2021/2024 board agendas but the current text could
  not be read, and since 2021 each school may move a boundary by a day.
- **Fujiyoshida (Fuji-Q)**: only a 2014 Wayback copy, giving windows, not dates.

## China's make-up working days (调休)

The State Council moves weekend days to lengthen the holiday blocks (2026: 01-04, 02-14, 02-28, 05-09,
09-20, 10-10 — weekend days with work and school, so quiet days). They are stored as
`holidayType = 'working_day'`; adding that enum value rewrote `holidays` under a lock
([the entities are the schema](../rules/the-entities-are-the-schema.md)). **They are not holidays:** the
all-type readers in `HolidaysService` (`getHolidays`, `getAllHolidays`, `isHoliday`) filter them out
(`NOT_A_WORKING_DAY`), and the Python services match types by name, so no calendar, gap fill, bridge-day
rule or model reads them yet. Turning them into a feature is a training change under the
[model switch rule](../ml/ml-overview.md). HK parks list CN in `COUNTRY_INFLUENCES`, but neighbour logic
counts school holidays only (`isSchoolHolidayInInfluenceZone`), so China's public blocks do not reach HK.

CN parks store inconsistent region codes (`Zhejiang`, `Gan Su Sheng`, `44`, `CN-44`, `Tianjin`, …).
Today's CN rows are national, so nothing depends on it; province-level data needs the parks normalised to
ISO `CN-xx` first, never per-province string matching.

## Korea

**NEIS school vacations** (`src/external-apis/korea/neis.client.ts`, run from `HolidaysProcessor`).
Korean schools set their own breaks, so one school does not stand for a region:

1. For each KR region a park sits in or is influenced by (`ParksService.getSyncRegionCodes("KR")`),
   resolve the education office (`kr-regions.ts`: 17 ISO 3166-2:KR codes ↔ NEIS office codes).
2. Sample `NEIS_SCHOOLS_PER_OFFICE` (15) elementary schools, spread evenly over the office's list.
3. Fetch each school's `SchoolSchedule` from last year to two years ahead. A vacation day is a `휴업일`
   whose event ends in `방학` (the ceremonies `여름방학식` / `개학식` are school days). A school files one
   event per day, so a holiday or Saturday inside its break shows as `제헌절` / `토요휴업일`; gaps of up to
   `MAX_VACATION_GAP_DAYS` (3) between vacation days are bridged.
4. A day is a regional school holiday when **more than half** of the schools that returned a calendar are
   on vacation; an empty calendar does not vote.

Rows are written under the ISO code (`KR-41`) and under the park's own stored code when it differs. KR
parks store ISO numbers (Everland and Caribbean Bay `41`, Legoland Korea `42`, Lotte World `11`);
`production-region-codes.spec.ts` pins the stored values across countries. A park code that names no
Korean region is logged (`KR region codes that name no Korean region …`) and gets no rows; the geocoder's
`normalizeRegionCode()` keeps the part after the last `-`, so a name like `Gyeonggi-do` would become `do` —
fix the park, do not guess here. Without a key NEIS returns a 5-row sample and ignores paging, so
`NEIS_API_KEY` (free at open.neis.go.kr) is required; a licence for the API itself was not found.

**KASI public holidays** (`src/external-apis/korea/kasi.client.ts`, data.go.kr dataset 15012690): free,
auto-approved key, "이용허락범위 제한 없음", 10,000 calls a day on a development key; the sync makes four
(one per year) and stores `isHoliday = "Y"` days, including substitute and short-notice holidays. `item`
is an object when there is one entry and `items` is `""` when there are none (API guide v1.4).

| Variable | For | Without it |
| --- | --- | --- |
| `NEIS_API_KEY` | KR school vacations | skipped with a warning; KR school data stays unknown |
| `KASI_SERVICE_KEY` | KR public holidays from KASI (the decoded "일반 인증키") | skipped with a warning; Nager.Date still supplies them |

A failed fetch writes and deletes nothing.

## A park's region code must match its coordinates

Universal Studios Hollywood had `regionCode = FL`: its merge winner had been geocoded in central Florida,
and `POST /v1/admin/parks/:id/correct-location` fixed city and coordinates but not the region, which the
geocode job never revisits. So it was matched against Florida holidays and neighbours.
`ParkRenameService.correctLocation` now re-derives `region`/`regionCode` by reverse-geocoding the
corrected coordinates; only a successful lookup changes them (an error or null keeps the old values with
a warning; a success without a region clears them for the geocode job to refill). `influencingRegions`
follows on the next park-enrichment run; then run a holiday sync.
