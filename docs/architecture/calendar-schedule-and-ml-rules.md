# Calendar, Schedule & ML Rules

Single source of truth for how **status** (OPERATING / CLOSED / UNKNOWN), **crowd level**, and **schedule sync** behave in the API and how the **ML service** aligns with these rules.

Related: [Schedule Sync & Calendar](schedule-sync-and-calendar.md), [ML Model Overview](../ml/model-overview.md), [Frontend: Calendar status](../frontend/calendar-schedule-status.md).

---

## 1. Calendar API Rules (Park has schedule in general)

**Endpoint:** `GET /v1/parks/:continent/:country/:city/:parkSlug/calendar?from=&to=`

**Source:** `CalendarService.buildCalendarResponse()` → `buildCalendarDay()` in `src/parks/services/calendar.service.ts`.

### 1.1 Status (ParkStatus)

| Rule | Implementation |
|------|----------------|
| **Future:** OPEN/CLOSED from schedule | `status` comes from `schedule?.scheduleType`: OPERATING → `"OPERATING"`, CLOSED → `"CLOSED"`, UNKNOWN or no entry → `"UNKNOWN"`. |
| **Future:** Days without schedule = UNKNOWN, but with crowd prediction | Future days without schedule keep `status: "UNKNOWN"`. They still get a **crowdLevel** (ML prediction or fallback `"moderate"`), not `"closed"`. |
| **Past + Today:** With crowd level treated as OPEN | Only for Parks **without** OPERATING entries in `schedule_entries`: if `status === "UNKNOWN"` and crowd level ≠ `"closed"` → `status = "OPERATING"`. Parks with OPERATING schedules keep UNKNOWN for days without schedule (DB-check via `hasOperatingSchedule`). |

### 1.2 Crowd Level

- **OPERATING:** `crowdLevel = inferredCrowdLevel` (historical: Redis/Stats/Queue/Analytics; future: ML).
- **UNKNOWN + future:** `crowdLevel = inferredCrowdLevel` (ML prediction or fallback `"moderate"`) – **not** `"closed"`.
- **CLOSED** or past without opening data: `crowdLevel = "closed"`.

### 1.3 Important

- **Crowd level does not influence status** (e.g. no override UNKNOWN → CLOSED just because crowdLevel is "closed"). Status comes from schedule or from the rule “past/today + crowd level = OPEN”.
- **UNKNOWN** = “Opening hours not yet available” (frontend); **CLOSED** = “Closed”.

### 1.4 Yearly predictions use the same closed rule

`GET /v1/parks/.../predictions/yearly` (`ParkIntegrationService.aggregateDailyPredictions`) asks the schedule the way the calendar does: a CLOSED entry, or a day with neither an OPERATING nor a CLOSED entry that `isClosedByOperatingRange()` (`src/parks/utils/schedule-closed-day.util.ts`) places strictly between the first and last OPERATING date, or outside that range for a seasonal park (`isParkSeasonal`). Such a day reads `crowdLevel: "closed"`, `recommendation: "closed"` and has no `avgWaitTime`. The calendar calls the same function, so the two endpoints cannot disagree on this. A day rated `unknown` carries no `recommendation`.

Before PAR-410 the yearly route never read the schedule: on 2026-09-22 it recommended 28 of the 49 days `/calendar` called CLOSED at Legoland Billund in an 89-day window, and all of February 2027 at Phantasialand. The response ends about 182 days out (the CatBoost daily horizon), not 365.

### 1.5 A past CLOSED day the measurement overrules (PAR-697)

For a **strictly past** day, measured ride activity outranks the operator's own park-level `CLOSED` entry — in the calendar (`status: "OPERATING"`, `isEstimated: true`, the reconstructed hours) and in the four callers of the shut-day rule (`src/analytics/closed-park-days.sql.ts`), so the two cannot disagree about which days count. Today and every future day are unaffected: a future day has no measurement to weigh, and a day in progress has no shape to judge.

**The rule** is `src/parks/utils/measured-operation.gate.ts`, four conditions over the day's qualifying raw readings, each with the production day it rejects:

| Condition | Rejects |
|-----------|---------|
| ≥ 3 different wait values across the day | a feed reporting one number on every ride (Walibi Holland 2026-09-11: 14 rides, same value, same minute) |
| 4–14 h from the first qualifying reading to the last, both bounds inclusive | a burst (the same day was 55 minutes) and a reading that never stops |
| no qualifying reading in 02:00–05:59 park-local | a feed stuck on its last value (352 of 551 candidate days carried waits through the night) |
| `timestamp - "lastUpdated"` ≤ 60 min, and `observedReadingsSql` | a frozen upstream and our own bookkeeping (PAR-748: the night rows are 0 heartbeats, 46.7 % stale feed). `lastUpdated IS NULL` is rejected with them — undecidable, and a statement against the operator is not made on an undecidable row |

**The verdict is materialised, not computed per read.** The rule needs raw `queue_data` for the day it judges; evaluated in the read path it cost the ML-accuracy query 1.4 s → 7.4 s. `park_day_operations` holds one row per (park, park-local day): the verdict, the derived hours it was taken from, and when. Readers do a primary-key lookup (`measuredOperationDayExists`), and **a day with no row keeps the operator's entry** — the pre-PAR-697 rule, by construction rather than as a special case.

**Who writes it:** queue `park-day-operation`.

- `calculate-yesterday-park-day-operation`, daily at **4:45**, every park's own yesterday. After the 4:30 `attraction-hourly-history` rollup, because the stored hours are read from it; before the 5:00 downtime job, because both read the same `queue_data` chunks.
- `backfill-park-day-operation` `{ parkId?, fromDate, toDate }` — the one-time fill of the past after a deploy, and a re-judgement after a threshold change. Run it in portions: a Coolify deploy renews the Postgres container and ends whatever is writing, so the portion size is the damage radius.

Both paths judge **finished** days only: `ParkDayOperationService.computeRange` clamps `toDate` to the park's last finished local day. A day in progress has no block length to judge, and a verdict for it would be honoured by the four statistics callers while the calendar refuses it — the two sides disagreeing about one day is what this rule exists to prevent.

Measured against production on 2026-10-07 over `2025-12-24` … `2026-10-07`: 6,174 park-level shut days, 1,238 of them with any measured activity, **210 opened by the gate** across 27 parks (151 of those `CLOSED` entries came from `fillScheduleGaps` rather than from the operator's feed).

---

## 2. Schedule Sync

**Jobs:** `sync-park-schedule` (on-demand), `sync-schedules-only` (daily 15:00), `sync-all-parks` (daily 03:00).

- **ThemeParks API:** The previous month plus 12 months ahead are requested (`getScheduleExtended`). All returned entries (OPERATING, CLOSED, …) are persisted.
- **Normalisation:** In `ParksService.saveScheduleData()`, `entry.type` for **CLOSED** is normalised: `"Closed"` / `"CLOSED"` (case-insensitive) → `ScheduleType.CLOSED`, so off-season (e.g. Phantasialand February) is stored as CLOSED when the API provides it.
- **Cleanup when saving from API:** Bidirectional: when we save **OPERATING** for a date, we delete any **CLOSED** row; when we save **CLOSED** for a date, we delete any **OPERATING** row. This ensures one source of truth per date.
- **Gaps:** `fillScheduleGaps(parkId, lookAheadDays, lookBackDays)` fills **missing** days. Range: (today - lookBackDays) through (today + lookAheadDays). Defaults: 182 days each (~½ year). Park timezone. Holiday/bridge metadata and either **CLOSED** or **UNKNOWN**:
  - **CLOSED:** There is at least one OPERATING day before and one after this date (in stored schedule) → gap is "in the middle", so we treat it as closed.
  - **UNKNOWN:** No OPERATING for the park, or this date is before the first OPERATING (e.g. before we have data) or on/after the last OPERATING (schedule not yet published). So we keep "opening hours not yet available".
  - **Demotion:** Gap-fill CLOSED (no opening/closing times) that is now after the last OPERATING gets demoted to UNKNOWN.

Details: [Schedule Sync & Calendar](schedule-sync-and-calendar.md).

---

## 3. Next Schedule (Park & Location APIs)

**Used by:** Park integrated response (`/v1/parks/.../integrated`), location/nearby, favorites.

- **Source:** `ParksService.getNextSchedule(parkId)` (and batch `getBatchSchedules`).
- **Behaviour:** Returns the **next OPERATING** day only: query filters by `scheduleType = OPERATING`, `openingTime IS NOT NULL`, `closingTime IS NOT NULL`, from park’s “tomorrow” up to 365 days ahead. **CLOSED** and **UNKNOWN** days are ignored.
- **Result:** `dto.nextSchedule` (openingTime, closingTime, scheduleType) is always an operating day when present; no UNKNOWN/CLOSED in next schedule. TTL and dynamic TTL for closed parks (e.g. expire before next opening) use this next OPERATING entry.

---

## 4. ML Service Rules & Alignment

### 4.1 Where schedule matters

| Component | File | Behaviour |
|-----------|------|-----------|
| **Schedule filter (after prediction)** | `ml-service/schedule_filter.py` | Filters predictions by `schedule_entries`. |
| **Inference (features + output)** | `ml-service/predict.py` | Sets `is_park_open`, `status`; skips model inference for CLOSED/UNKNOWN rows (reduces load). |

### 4.2 filter_predictions_by_schedule (schedule_filter.py)

- **Daily predictions:** Only days with an **OPERATING** entry are kept. Days with only CLOSED or UNKNOWN (no OPERATING on that day) are **removed**.
- **Fallback:** If the park has **no** schedule integration (no OPERATING entry in the query), **all** daily predictions are kept (“no schedule” → keep all).
- **Hourly predictions:** Only times **within** operating hours (openingTime–closingTime) are kept; times outside or days without OPERATING with times are filtered out.

**Effect on calendar:** For **future UNKNOWN days** the ML API returns **no** daily prediction (because the day is not OPERATING). The calendar then uses the fallback `mlPrediction?.crowdLevel || "moderate"` for those days – they get a display crowd level (e.g. “moderate”) but not a real ML prediction for that day. To show real ML predictions for UNKNOWN days, the ML filter would need to be extended to optionally return predictions for UNKNOWN days (for parks with schedule).

### 4.3 predict.py (Inference)

- **Schedule features:** From `schedule_entries`, OPERATING/CLOSED/UNKNOWN are evaluated per park/date. A park with no OPERATING entry at all is treated as “no schedule” → predictions are kept.
- **Inference skip for CLOSED/UNKNOWN:** Rows with `status === “CLOSED”` or `”UNKNOWN”` are **excluded before** model.predict by default. However, if `featureContext.parkLiveStatus[parkId] === “OPERATING”` (ride-based detection), UNKNOWN rows are overridden to `is_park_open=1` and inference IS run for those rows. Explicit CLOSED entries are always respected.
- **`parkLiveStatus` context:** NestJS sends `featureContext.parkLiveStatus` with every prediction request. Derived from `getBatchParkStatus`: schedule-based first, then ride-heuristic fallback (≥3 attractions with data, ≥25% with `waitTime ≥ 10` in last 2h). Parks with explicit CLOSED schedule today are excluded from the heuristic.

### 4.4 Summary: ML ↔ Calendar

| Aspect | Calendar (API) | ML Service |
|--------|----------------|------------|
| CLOSED day | `status: “CLOSED”`, `crowdLevel: “closed”` | No inference run; filter_predictions_by_schedule removes anyway. |
| UNKNOWN day (future) — park confirmed open via rides | `status: “UNKNOWN”`, `crowdLevel` = ML or fallback “moderate” | `parkLiveStatus=”OPERATING”` overrides `is_park_open=1` → real ML prediction returned. |
| UNKNOWN day (future) — park not confirmed open | `status: “UNKNOWN”`, `crowdLevel` = fallback “moderate” | Inference skipped; calendar uses fallback. |
| Past/today without schedule | With crowd level → `status: “OPERATING”` | Independent; calendar derives status from crowd data. |

---

## 5. Implementation checklist

- [x] Calendar: Status only from schedule + rule “past/today + crowd level = OPEN”; no status override by crowdLevel.
- [x] Calendar: Future UNKNOWN days always have a crowdLevel (ML or “moderate”), never blanket “closed”.
- [x] Schedule sync: API type “Closed”/“CLOSED” is stored as `ScheduleType.CLOSED`.
- [x] ML: Daily predictions only for OPERATING days (filter_predictions_by_schedule); fallback “no schedule integration” → keep all.
- [x] ML: CLOSED/UNKNOWN in predict.py → `crowdLevel: "closed"` in response.
