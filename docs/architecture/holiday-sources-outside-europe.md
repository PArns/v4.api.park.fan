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

## China's make-up working days (调休)

The notice moves weekend days to make the holiday blocks longer. In 2026 these
are 01-04, 02-14, 02-28, 05-09, 09-20 and 10-10: Saturdays and Sundays with work
**and school**, so they tend to be quiet days. No feature knew about them before.

They are stored as `holidayType = 'working_day'`, a new value of the
`holidays.holidayType` enum (TypeORM `synchronize` adds it on deploy). **They
are not holidays:**

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
reverse-geocoding the corrected coordinates. A failed lookup clears them, and
the geocode job, which retries parks without a region, fills them in later.

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
