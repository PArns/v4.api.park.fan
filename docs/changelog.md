# Changelog

Notable changes to the Park Fan API. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Version and date align with releases or significant doc/code milestones.

---

## [Unreleased]

### Added — severe-weather warnings (MeteoGate → DWD/MeteoAlarm) (2026-06-19)

Official severe-weather warnings on the weather response. Open-Meteo provides
forecast data only (our existing "nowcast alert" is self-derived), so warnings
come from a new `WeatherWarningSource` (MeteoGate `api.meteogate.eu/warnings`,
EUMETNET → MeteoAlarm/national services; DWD for Germany), ~40 European
countries, auth via `METEOALARM_API_TOKEN`.

- `MeteoGateWarningsClient`: per-country EDR `locations` query (trailing-24h
  `datetime`, filters by `sent`), dedupe the CAP index by `alertId`, fetch the
  CAP as **JSON** (no XML), parse de/en `info`; Redis cache + circuit-breaker +
  singleflight, fail-soft.
- `weather_warnings` entity (per park × alert) + `WeatherWarningsService`:
  group parks by country, `expires > now` filter, park↔area matching (bbox →
  exact point-in-polygon), atomic per-country replace.
- `weather-warnings` cron every 15 min.
- Exposed as `warnings: WeatherWarningDto[]` on the embedded park `weather`
  object **and** the live `/weather/nowcast` (German + English).
- Frontend guide: [docs/frontend/weather-warnings.md](frontend/weather-warnings.md).

Production needs `METEOALARM_API_TOKEN` in the Coolify env.

**Follow-up (same day):** MeteoGate's German feed was found to lag badly (it
served only expired DE alerts while DWD had an active EXTREME-HEAT warning for
Brühl/Phantasialand). German parks now use **Bright Sky (DWD direct, no key,
point-based)** — warnings come pre-matched to a park's warncell (no
point-in-polygon); MeteoGate stays for the rest of Europe. Also added **segment
de-duplication**: services that slice one warning into many hourly CAP alerts
(same event/area/severity, walking `expires`) are collapsed to one row spanning
the full window (e.g. Toverland 34 → one per distinct event).

### Docs — frontend guide for the ride P50/P90 stats (`typicalWaits`) (2026-06-19)

Added [docs/frontend/ride-typical-waits.md](frontend/ride-typical-waits.md): how
to consume the typical-vs-busy peak-wait stats on the attraction detail endpoint
(`GET …/attractions/:attractionSlug` → `typicalWaits`). Documents the shape
(`weekday`/`weekend` buckets = P50/P90 of daily peaks, `byDayOfWeek`, record
`peak`), the `displayable` gate (≥ 20 operating days — render on this, not a
client threshold), country-aware weekends, the 365-day window, and 24h
freshness. The feature itself shipped 2026-06-18 (per-weekday + typical-vs-busy
peak waits on the ride route).

### Added — gate "thin-data" parks out of crowd levels, ML training & MAE (2026-06-18)

Parks with **< 30 operating days** of valid headliner data were emitting a
confident `moderate` crowd level from a median over a handful of days (Sesame
Place from 1 day, Knoebels 3, Nigloland 3, …) and polluting ML training + the
reported MAE. Now a park is "ratable" only with **≥ 30 operating days**; below
that:

- **Single source of truth:** `calculateTypicalDayPeak` returns the operating-day
  count alongside the median; `calculateP50Baseline` forces `typicalDayPeak = 0`
  (→ NULL column + no Redis key) below the threshold. Ratable ≡
  `park_p50_baselines."typicalDayPeak" IS NOT NULL`.
- **New `unknown` crowd level** ("keine Prognose"): every derived rating surface
  reads `unknown` for a non-ratable park — calendar prognosis, yearly,
  historical-stats, per-attraction, and the live/"today" rating. New helpers
  `rateOrUnknown` (typical-day-peak surfaces) and `AnalyticsService.isParkRatable`
  / `getRatableParkIds` (live/occupancy surfaces); `OccupancyDto` now carries a
  gated `crowdLevel`. Raw wait-time minutes and the numeric occupancy %
  (ML feature) are unchanged — only the rating string flips.
- **ML training** (`ml-service/db.py`) and the **reported aggregate MAE/accuracy**
  (`prediction-accuracy`, `ml-drift-monitoring`) `INNER JOIN park_p50_baselines …
  typicalDayPeak IS NOT NULL`, excluding thin parks from both.
- Frontend must render the new `unknown` enum value as
  "Keine Prognose / noch nicht genug Daten".

No schema change (`typicalDayPeak` is already nullable). The gate takes effect on
the next `calculate-park-baselines` cron; ML exclusion on the next nightly train.

### Fixed — green up the unit suite (stale ML specs) (2026-06-17)

18 pre-existing failures in three ML specs were all **stale tests, not code bugs**
(no service code changed; verified against the real wiring / recalibration commits):

- `ml-dashboard.service.spec`: the Redis mock's `set: jest.fn()` returned
  `undefined`, so `redis.set(...).catch(...)` in `getDashboard` threw. Real
  `ioredis.set` returns a Promise → mock now `mockResolvedValue("OK")`.
- `ml-alert.service.spec`: the MAE→severity ladder was recalibrated for the
  q0.8-quantile era (commit on 2026-06-13; thresholds 8→**13/17/22**) but the spec
  still asserted the RMSE-era values. Spec updated to the current thresholds.
- `ml.service.spec` (`storePredictions`): the chunked save
  `save(rows, { chunk: 1000 })` (stays under Postgres' 65535 bind-param limit)
  made `toHaveBeenCalledWith(array)` fail on the extra arg. Assertion updated.

Unit suite is now fully green (476 passed, 0 failed). Note: CI only runs CodeQL,
so these never gated — but the suite was misleading.

### Fixed — per-attraction & historical-stats crowd-level calibration (2026-06-17)

- **Per-attraction calendar divided a day's peak by the attraction P50 (median)**
  (`attraction-integration.service.ts`), i.e. peak-vs-median — the same structural
  miscalibration the park calendar already fixed: a day's peak is ~1.5-2× the
  median, so a *normal* day read elevated. It now divides by a new per-attraction
  **typical-day-peak** baseline (`AnalyticsService.getAttractionTypicalDayPeak` —
  median over operating days of the day's peak, computed on-demand from
  `queue_data_aggregates`, cached in Redis `attraction:typicalpeak:{id}`), so a
  normal day ≈ 100% = `moderate`. No P50 fallback. The previously-uncalled
  per-attraction branch of `calculateCrowdLevelForDate` was aligned to the same
  baseline (no longer P90→P50). Migrated attractions' key is evicted on merge.
- **`/historical-stats` used all-attraction `park_daily_stats`**, a different
  baseline definition than the calendar, so the same day could read a different
  crowd level on the two surfaces. It now computes **headliner-only** day-values
  from `queue_data_aggregates` and a self-consistent typical-day-peak (median of
  those day-values), matching the calendar's semantic (typical day ≈ 100%).
- **Doc reconciliation** (`crowd-levels.md`): the park calendar `day_value` is the
  raw-`queue_data` daily P90 (not hourly-slot P90s); documented the new
  per-attraction typical-day-peak.
- **New docs**: [`ml/quantile-serving-and-calibration.md`](ml/quantile-serving-and-calibration.md)
  (the full quantile→display/crowd mapping + the monotonic / `predicted_peak` /
  single-flight fixes) and
  [`development/full-db-validation-checklist.md`](development/full-db-validation-checklist.md)
  (the calibration invariants + SQL to verify against real parks, since the
  dev/CI container has no DB).
- _(Deferred: switching `stats.service.ts` percentiles from nearest-rank to linear
  is now moot for historical-stats — it no longer reads `park_daily_stats` — and
  is entangled with the outlier-cap heuristic + its unit tests, so it's left as an
  optional follow-up for the raw `/stats` endpoints.)_

### Fixed — P50/P90 consistency, cache invalidation, doc alignment (2026-06-17)

- **Yearly predictions used the abandoned peak-vs-median regime**
  (`park-integration.service.ts` `aggregateDailyPredictions`): P90-of-predicted-headliner-waits
  ÷ the **P50** baseline, while the monthly calendar's future path uses AVG-of-headliners ÷
  **typical-day-peak**. The same future date therefore read systematically busier on the yearly
  view (and for ≤10 headliners `floor(n*0.9)` was effectively the max). Now mirrors the calendar
  (AVG ÷ typical-day-peak; missing baseline → 'moderate', no P50/P90 fallback) so yearly and
  monthly agree.
- **Park merge/repair left analytics & attraction caches stale.** `invalidateParkCaches`
  (`park-cache-invalidation.ts`, keys centralized in `cache-keys.ts`) now also evicts
  `park:statistics`, `park:p50/p90`, `park:typicalpeak`, `analytics:headliners`,
  `analytics:crowdlevel:*`, `park:historical-stats`, `park:derivedHours`, `ml:park:*`, the migrated
  attractions' `attraction:integrated`/baseline caches, and the `discovery:geo:structure` skeleton.
- **Discovery geo-structure was never invalidated** (its invalidator had zero callers) — merge/repair
  now bust it.
- **Yearly ML cache was force-evicted by warmup but never re-warmed** (`cache-warmup.service.ts`),
  guaranteeing a ~15 s cold path twice a day. Warmup no longer evicts it (TTL-refreshed instead).
- **Baseline recompute didn't evict derived caches** (`analytics.service.ts` `saveP50Baselines`) —
  now evicts `park:statistics` + cached crowd levels.
- **ML dashboard "last accuracy check" was always fabricated** — `compareWithActuals` now persists
  the `ml:last-accuracy-check` marker the dashboard reads.
- **`/nearby` re-loaded every park from the DB on each request** (two full-table scans). It now
  reuses a shared, user-INDEPENDENT park-coordinate index cached in Redis (`location:parkcoords:v1`);
  the per-user distance computation and the nearby response itself stay per-request (never cached).
  Busted on merge/repair + admin flush.
- **No stampede guard on the ~15 s ML serving rebuild** — `getServingDailyPredictions` now
  single-flights concurrent cold rebuilds (calendar + yearly share one compute) via a new
  `SingleFlight` util. Also extended to discovery `getGeoStructure` (geo-skeleton build) and
  `getLiveStats` (the heavy live multi-CTE), each via an extracted `build*` method with an
  in-flight cache re-check. (Remaining follow-up: `getParkPredictions("daily")`, which needs a
  larger method extraction. Single-flight is in-process per instance — not cross-instance.)
- **`getCountrySummary` 404 path hit the DB on every probe** — now negative-cached, so crawlers
  probing bogus continent/country slugs no longer re-query.
- **Admin cache flush missed `ml:dashboard:*` and `location:*`** — added to the flush patterns.
- **Non-crossing MultiQuantile** (`ml-service/model.py`): per-row quantiles are now sorted
  monotonically so the crowd signal (q0.8) can't fall below the displayed median (q0.5) and the
  uncertainty band can't silently collapse; `nf-service` `predicted_peak` semantics documented
  (= E[daily-P90], a median forecast of a P90 target).
- **Documentation realigned** to the two-regime model (live = ratio-vs-P50, calendar =
  typical-day-peak; P90 no longer "primary"): schema, caching-strategy, system-overview,
  headliner-logic, crowd-levels, model-overview, feature-engineering-concepts, common-issues,
  neuralforecast-tft-evaluation. Fixed dead doc links, the nonexistent `OPEN_WEATHER_API_KEY`
  (Open-Meteo needs no key), `inference.py`→`main.py`, npm→pnpm, and removed the unused
  `@types/luxon`.
- **Removed dead code** `getCrowdLevelTrainingData` and corrected stale "P90 baseline /
  peak-vs-median" comments across analytics/calendar/location/park-integration.

### Added — TFT per-attraction board + MultiQuantile per-purpose serving (2026-06-13)

- **TFT best/worst board** (`ml-monitoring.controller.ts` `GET /v1/ml/monitoring/tft/performers`,
  `prediction-accuracy.service.ts` `getTftTopBottomPerformers`, frontend
  `app/admin/ml/page.tsx`). TFT daily-peak forecasts scored against realised daily P90 per
  attraction, same board hygiene as CatBoost (stddev≥8 floor). New "Per-attraction accuracy
  (TFT daily)" section in /admin/ml next to the (renamed) CatBoost hourly board.
- **MultiQuantile per-purpose serving** (`ml-service/config.py`, `model.py`, `predict.py`).
  Loss → `MultiQuantile:alpha=0.5,0.8,0.95`; serving picks the quantile per purpose: the
  wait DISPLAY uses the median (q0.5), the crowd-level uses q0.8. Backtest (21d, 392k slots):
  median cut overall MAE −24% / quiet −41% and removed the +3.4-min quiet over-read; q0.8 is
  busy-optimal (q0.95 over-shoots). Backward-compatible (falls back to the legacy path until a
  MultiQuantile model is trained). **Finding: the busy-tail "worst predictions" are inherent
  variance, not quantile- or ensemble-fixable** — q0.95 and a CatBoost+TFT daily mean both
  measured worse than the status quo. See docs/ml/tft-vs-catboost-clean-comparison.md §6.6.

### Fixed — ML dashboard hygiene: best-board, MAE alert, daily breakdown, anomalies (2026-06-13)

- **Anomaly board de-noised** (`ml-anomaly-detection.service.ts`). `unexpected_closure` was 89%
  of all anomalies (534/602 live), and ~90% of those were genuine ride closures during opening
  hours (CLOSED/DOWN/REFURBISHMENT) — operational reality, not a model defect (the model
  correctly predicted a wait for a ride that should have been running). It buried the ~68 real
  model anomalies (large_error / extreme_value on genuine rides). Removed closure detection from
  model-quality anomaly monitoring (enum kept for history); purged the 5883 existing closure
  rows. Board now shows 68 actionable anomalies instead of 602.


- **"Best predictions" board de-polluted** (`prediction-accuracy.service.ts`
  `getTopBottomPerformers`). It was dominated by 0.0-MAE non-rides — shows, walk-on/kiddie
  rides and transport mis-ingested as attractions (Hall of Presidents, Mickey's PhilharMagic,
  PEANUTS kiddie rides): their wait never varies (a 4D film "queues" a constant ~15 min), so
  the model predicts the constant perfectly. Added a stddev floor (≥8) — real rides swing
  widely (Taron 14.6, Manta 18.2, Wrath of Rakshasa 29.1) vs shows at 0-7 (Hall of Presidents
  1.9, Magiezijn 0.0). Verified live: board now shows real rides at ~3.6-4.2 MAE. Display
  filter only, no data deleted.
- **Accuracy-degradation alert recalibrated** (`ml-alert.service.ts`). Threshold 8 min
  (severity ladder 7/10/15) dated from the RMSE-loss era (MAE ~4-5); the current
  Quantile(0.8) loss predicts the upper conditional quantile by design, so live MAE sits at a
  structural ~10-12 and the alert fired permanently. Now 13 (severity 13/17/22) — catches a
  real climb, not the q0.8 baseline. Honest fix for the quiet over-read remains MultiQuantile
  serving.
- **Daily breakdown shows n/a, not 0%** (`prediction-accuracy.service.ts`, `ml-dashboard.dto.ts`).
  Per-type breakdown reported DAILY as 0% coverage / 0 MAE (reads as broken); daily predictions
  are intentionally never scored against actuals. Now `mae`/`coveragePercent` are null with an
  explicit `tracked: false` flag so the UI renders "n/a".

### Fixed — daily-prediction coverage + verified-coverage metric (2026-06-13)

Follow-up to the 2026-06-10 generate-daily fix; coverage had plateaued at ~110/160.

- **Daily park selection mirrored the hourly 3-stage net** (`prediction-generator.processor.ts`).
  `handleGenerateDaily` filtered parks with `isParkOperatingToday()` only, while the hourly
  generator also force-includes parks with recent ride activity. ~14 demonstrably-open parks
  (Energylandia, Beto Carrero, Grona Lund, Universal Studios Orlando, Chimelong, Warner Bros.
  Movie World, …) report no schedule (UNKNOWN), so the daily cron — running once at 01:00 UTC,
  local night for many — read them as closed and gave them thousands of HOURLY but ZERO daily
  predictions. Daily now uses the same net with a 24h activity window. Result: park selection
  134 → 151, successful 110 → **125 parks / 4068 attractions**.
- **Season-end filter loosened** (`ml.service.ts` storePredictions #4): only skips after the
  last OPERATING schedule day when that day is genuinely in the past (real off-season), not when
  it merely equals today (schedule-sync horizon). Energylandia (open daily, 0 future OPERATING
  entries) was discarding its whole future calendar.
- **"Verified coverage" metric corrected** (`prediction-accuracy.service.ts`). The homepage
  widget read 54% (tripping the <80% alert); the real rate is ~80-96%. `coveragePercent` reused
  the MAE-eligible count, which excludes ride closures and sub-5-min waits — conflating "did we
  check this against reality?" with "was the actual a non-trivial wait?". Measured live: 95% of
  the "uncovered" slots are rides closed/quiet *during opening hours* (the operating-hours filter
  already works). Coverage is now `COMPLETED / total`, separate from the strict MAE filter.
- **Open-but-null rides no longer counted as unplanned closures** (`prediction-accuracy.service.ts`).
  Status-only parks (Chimelong = #1 most-popular, many Asian/water parks) report status=OPERATING
  with waitTime=null; these were scored as closures with full error, inflating MAE and dragging
  coverage down (~37% of all "unplanned closures"). They are now left uncompared (PENDING → MISSED).

### Fixed — generate-daily silently failing for ~87 of 139 live parks (2026-06-10)

Live diagnosis (injected one-off `generate-daily` Bull job, watched the logs): two
independent bugs starved most parks of fresh CatBoost daily predictions — Magic Kingdom
had none since 2026-04-10, a large cluster (HK Disneyland, Universal Singapore, Kings
Dominion, …) since ~2026-05-24. Only ~53 parks/night succeeded.

- **`storePredictions` bind-parameter overflow** (`ml.service.ts`): one multi-row INSERT
  for a big park (60 attractions × 365 days × 11 columns ≈ 240k params) exceeds the
  Postgres wire-protocol limit of 65535 bind parameters → driver fails with
  `bind message has N parameter formats but 0 parameters` (N = total mod 65536).
  Fix: `repository.save(entities, { chunk: 1000 })`.
- **`deduplicatePredictions` TimescaleDB decompression abort** (`ml.service.ts`): the
  hypertable is partitioned on `createdAt` (compress_after 14d) but the dedupe DELETE
  filtered only on `predictedTime`, so it scanned every chunk and died with
  `tuple decompression limit exceeded` (100k tuples/transaction) on parks whose stale
  forward rows had been compressed — a permanent failure loop (the stale rows could
  never be deleted, so every night failed again, before storePredictions even ran).
  Fix: dedupe scoped to `createdAt >= now() - 13 days` (uncompressed chunks only;
  compressed-batch min/max metadata on predictedTime+createdAt skips the rest).
  `deleteOldPredictions` now lifts the limit via
  `SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0` (bounded
  nightly cleanup; native 90d retention policy exists as well).
- **One-time remediation executed on the live DB**: 4.46M superseded compressed forward
  rows purged in 4 batched transactions (~30s total), so the nightly jobs start clean.

### Changed — TFT daily horizon 30 → 45 days (2026-06-10 re-eval)

Scheduled re-evaluation (due ~2026-06-14) run early; all gates passed decisively:

- **Live matched scoreboard (14d, strict lead-1)**: TFT beats CatBoost on every segment —
  ALL 8.1/−0.1 vs 11.9/−4.1, busy(P90≥40) 16.1/−7.1 vs 27.4/−25.2, headliner 11.6/−0.1
  vs 17.3/−9.0 (MAE/bias).
- **Lead degradation is shallow**: TFT MAE 8.2 (lead 1-2) → 9.3 (lead 10-13); TFT at
  lead 10-13 still beats CatBoost at lead 1-2.
- **Horizon backtests** (`nf-service/backtest_horizon.py`, headliners): h=45
  (BASE 2026-04-26) lead 31-45 ALL 15.3/−3.2, busy≥40 20.8/−11.9 — better than CatBoost
  at lead 1. h=60 (BASE 2026-04-11) lead 46-60 ALL 17.6/+7.5, busy≥40 19.2/+1.0 — also
  viable, deferred until ~8 months of history (overall bias from the thinner window).
- Series maturity: headliner median 168 operating-day points (was 72 at the h=30 gate).
- Changes: `NF_HORIZON=45` (nf-service config), `getTftDailyPredictions`/
  `getServingDailyPredictions` defaults 30 → 45 (`ml.service.ts`). CatBoost now serves
  only day 46-365 + intraday. Intraday re-run (Shanghai) confirms TFT still has no busy
  edge there (busy≥60 22.9/−14.1 vs persistence 19.7/−0.5) — intraday stays CatBoost.

### Changed — Peak-vs-median crowd level (corrects PR #46)

- **Crowd-level semantic switched again — now peak-vs-median** (`analytics.service.ts`, `attraction-integration.service.ts`, `park-integration.service.ts`, `calendar.service.ts`). Baseline is now **P50** (median, "typical wait") instead of P90; current value is **P90 of a short window** (20 min live, P90-of-slot-P90s for calendar days). The previous P90-vs-P90 design (PR #46) was mathematically apples-to-apples but methodologically off: P90 baseline is "an exceptionally busy day in the last 18 months", so most days landed in "very_low" / "low" because they didn't touch that ceiling. Peak-vs-median reads 100% when the current peak matches a typical wait, 150%+ when the queue is materially above typical — matches user intuition. Threshold ladder is unchanged. See [Crowd Levels](analytics/crowd-levels.md) for the full design.
- **Live park current value**: window shrunk from 60 min to **20 min** (`getCurrentParkPeakWait`). With 5-min sampling that's ~4 samples per ride, so the MAX-then-avg reads as a recent P90. Window auto-expands to 60 → 240 min only when the 20-min window has no qualifying data (source lag, sparse-reporting ride).
- **Calendar daily value**: from "weighted avg of hourly P90s" to **P90 of in-hours slot P90s** (`attraction-integration.service.ts`). MAX would be too fragile against single outlier slots; P90 is robust and still represents the day's actual peak hour. Filters slots by the park's opening/closing schedule so off-hours samples don't pollute the reading.
- **`attraction_hourly_history` backfill expanded** from rolling 7 days to the full data window (2025-12-24 onward). Before the backfill, days without a row were misinterpreted as "Ganztägig geschlossen" by the frontend even when raw `queue_data` had operating samples. The backfill jobs are idempotent and re-runnable per date range.
- **`p90-crowd-levels.md` → `crowd-levels.md`** (rename + rewrite). The new doc describes the corrected peak-vs-median architecture; the P90 baseline tables and Redis keys are retained as the fallback path.

### Changed — Peak-vs-peak crowd level (PR #46, since corrected)

- **Crowd-level semantic across every user-facing surface switched from P50-vs-P50 (avg vs typical avg) to P90-vs-P90 (peak vs typical peak)** (`analytics.service.ts`, `attraction-integration.service.ts`, `park-integration.service.ts`, `location.service.ts`, `search.service.ts`, `calendar.service.ts`, `ml.service.ts`). "How busy was today" is what users remember as the peak headliner experience, not the day's median — apples-to-apples math now matches that intuition. P50 stays available as a fallback for brand-new entities without a P90 row yet (still apples-to-apples, just an avg-vs-avg reading until the next cron). The threshold table (very_low / low / moderate / high / very_high / extreme) is unchanged, so the labels keep their meaning. See [Crowd Levels](analytics/crowd-levels.md) for the full architecture (this doc was renamed from `analytics/p90-crowd-levels.md` and rewritten for the current two-regime model).

- **Park live occupancy now reads per-headliner MAX in the last 60 min, averaged across headliners** (`analytics.service.ts:getCurrentParkPeakWait`) — the live counterpart to the 548-day P90 baseline, with the same shape on both sides of the comparison.

- **Calendar `peakLoad` mixed-percentile bug fixed** (`calendar.service.ts:buildPredictedCrowdLevels`): the ML-prediction path used to divide the predicted P90 by the P50 baseline, which systematically inflated peakLoad readings. Now uses P90 baseline; peakLoad and crowdLevel are both peak-vs-peak.

- **ML pipeline forwards both baselines** (`ml.service.ts`, `prediction-request.dto.ts`): the Python service now receives both `p50Baseline` and `p90Baseline` in every prediction request. Training labels in `getCrowdLevelTrainingData` switched to day-P90 ÷ P90 baseline — models recalibrate within ~1 daily cycle.

- **Attraction history utilization fixed** (`attraction-integration.service.ts`): the per-day "utilization" badge on the attraction history chart used to compare the day's weighted avg wait against either P50 baseline or today's intra-day P90 — a mixed-percentile reading that drifted between days. Now uses weighted avg of hourly P90s ÷ attraction P90 baseline (peak-vs-peak), matching every other surface.

### Added — P90 baseline infrastructure (PR #46)

- **`park_p90_baselines`, `attraction_p90_baselines` tables** populated by the existing P50 cron at 3 AM / 4 AM. PostgreSQL produces both percentiles from one PERCENTILE_CONT sort, so the additional rows cost nothing on top of the existing P50 scan. Cached in Redis (`park:p90:{id}`, `attraction:p90:{id}`, 24 h TTL).

- **Read API**: `AnalyticsService.getP90BaselineFromCache`, `getAttractionP90BaselineFromCache`, `getBatchAttractionP90Baselines` (MGET + DB hydrate + pipeline writeback). Replaces the live-aggregation `getBatchAttractionP90s` on every hot path.

### Added — Pre-aggregated hourly history (PR #46)

- **`attraction_hourly_history` table + `AttractionHourlyHistoryProcessor`** (daily 04:30): pre-aggregates yesterday's per-attraction 15-min-slot P90/avg/sampleCount breakdown into one JSONB blob per `(attractionId, date)`. The attraction history endpoint now reads past days from this table (one indexed SELECT) and computes only today's slots live — replaces the previous always-live PERCENTILE_CONT scan of the entire window (typically 30 days × ~96 slots = ~2,880 percentile groupings per cold hit, per attraction).

### Removed (PR #46)

- **`OccupancyCalculationProcessor` + `occupancy-calculation` queue + `precompute-p90-sliding-window` job**: the precompute wrote a Redis cache (`analytics:percentile:sliding:*`) that nobody reads any more — the P90 baseline lives in the new tables. ~10 k heavy queries per night eliminated; the orphaned Redis keys TTL out within 24 h of deploy.

- **`AnalyticsService.get90thPercentileWithConfidence`, `get90thPercentileSlidingWindow`, `getBatchAttractionP90s`**: live 548-day PERCENTILE_CONT methods, fully replaced by the cache-table-backed read API.

- **`AttractionsMetadataProcessor`, `ShowsMetadataProcessor`, `RestaurantsMetadataProcessor` + their Bull queues** (`attractions-metadata`, `shows-metadata`, `restaurants-metadata`): all three were marked `@deprecated Phase 6.2` but still instantiated by Nest. `ChildrenMetadataProcessor` has covered their work since the combined sync landed.

### Performance (PR #46)

- **Wait-times processor `processLandData` N+1** (`wait-times.processor.ts`): the every-5-min land-info sync did a SELECT + 2 UPDATEs per attraction (~3,000 queries per run for a 100-attraction park). Now bulk-fetches current land/queueTimesEntityId in the same SELECT used for IDs, diffs in memory, and groups UPDATEs by target land. Steady-state: 0 UPDATEs.

- **`/v1/analytics/realtime` LATERAL subquery** (`analytics.service.ts:getGlobalRealtimeStats`): two correlated subqueries per park (a COUNT + a per-attraction latest-status LATERAL JOIN) replaced with a single `attraction_counts` CTE that LEFT JOINs the already-computed `latest_updates` and aggregates via FILTER. Cache-miss latency falls from seconds to ms.

- **ML alert auto-resolve N+1** (`ml-alert.service.ts`): per-alert `find()` + N×`save()` replaced with one UPDATE matching the same WHERE filter.

### Added

- **Full statistics on `/v1/analytics/realtime`** (`analytics.service.ts`, `global-stats.dto.ts`): `longestWaitRide` and `shortestWaitRide` now expose today's full statistics — `avgWaitToday`, `minWaitToday`, `peakWaitToday`, `peakWaitTimestamp`, `typicalWaitThisHour`, `currentVsTypical` — in addition to the existing `sparkline` field. Values are fetched via `getAttractionStatistics`, the same method used by the attraction detail endpoint, so they are always consistent. Frontend no longer needs to fill these fields with `null`.

- **Sparklines on `/v1/analytics/realtime`** (`analytics.service.ts`, `global-stats.dto.ts`): `longestWaitRide` and `shortestWaitRide` now include a `sparkline` field — an array of `{ timestamp, waitTime }` pairs covering today's operating window. Each ride uses its own park's timezone and schedule opening time as the window start (identical to the park controller), so rides from e.g. Tokyo and Orlando both show the correct local-day history.

- **`getAttractionSparklinesBatch`** (`analytics.service.ts`): New helper on `AnalyticsService` for fetching sparklines when attractions may span multiple parks. Groups by `parkId`, calls `getEffectiveStartTime` once per park, batches `getBatchAttractionWaitTimeHistory` per group, and merges results into a single `Map<attractionId, SparklinePoint[]>`. Use this for any multi-park context (global stats, recommendations, …); use `getBatchAttractionWaitTimeHistory` directly when you already hold a shared `startTime` for a single park. See [Sparklines](analytics/sparklines.md).

### Added
- **`crowdLevel` on `ParkReference.analytics.statistics`** (`discovery.service.ts`, `geo-structure.dto.ts`): The `/v1/discovery/geo`, `/v1/discovery/continents`, `/v1/discovery/continents/:continent`, and `/v1/discovery/continents/:continent/:country` endpoints now expose the park's current live crowd level inside `analytics.statistics` alongside `avgWaitTime`. Previously the only source of live crowd level on these endpoints was `currentLoad.crowdLevel`, which could be `null` even when wait-time statistics were available — causing the frontend's Popular Parks section to render wait times without a crowd badge. `analytics.statistics.crowdLevel` is now co-present with `avgWaitTime` whenever the park has a valid P50 baseline. The other discovery routes already expose live crowd level via their existing shapes: `/v1/discovery/:continent/:country` (`ParkResponseDto.analytics.statistics.crowdLevel`) and `/v1/discovery/nearby` (`analytics.crowdLevel` per park/ride).

### Changed
- **Shared crowd-level utility** (`common/utils/crowd-level.util.ts`): Extracted the P50-relative occupancy → CrowdLevel threshold ladder (very_low/low/moderate/high/very_high/extreme) into a single reusable function. `AnalyticsService.determineCrowdLevel` now delegates to it (all ~20 existing call sites unchanged), and `DiscoveryService.hydrateStructure` uses it directly. Thresholds now exist in exactly one place.

### Added 
- **Smart Gaps: Historical Hour Reconstruction** (`docs/analytics/smart-gaps.md`): Automatically reconstructs park opening/closing hours for past days using a 15-minute sliding window and 10% attraction activity threshold (rides with waitTime >= 5 min only). Includes rounding to nearest full hour and strict exclusion of service points (bars, snacks) via name-based blacklist. 
- **`isEstimated` flag for Calendar API**: New per-day flag in `CalendarDay` to signal reconstructed historical data. 
- **`hasOperatingSchedule` flag for Parks API**: New per-park flag to signal if a park provides an official API calendar (true) or relies on inference/estimates (false). Added to all park-related DTOs and Nearby responses. 
- **Automated Seasonal Detection**: Logic to identify "Seasonal Parks" (winter gaps > 21 days) to suppress crowd predictions during off-season while allowing them for year-round parks with UNKNOWN schedule. 

### Changed 
- **Optimized Seasonal Check**: Accelerated `isParkSeasonal` query by 120x (from 72ms to 0.6ms) using SQL Window Functions (`LEAD`). 
- **ML Feature Context Alignment**: ML service now receives real-time reconstructed opening hours instead of static 9/10 AM fallbacks, improving prediction accuracy for "No-Schedule" parks. 
- **Batch Processing for DTO Enrichment**: Introduced `getBatchHasOperatingSchedule` to prevent N+1 queries when listing parks. 


### Added

- **Training roadmap doc** (`docs/ml/training-roadmap.md`): Tracks known ML issues, data quality analysis, and next steps for training improvements including UNKNOWN park inclusion strategy.
- **Reverse reconciliation for stale attractions** (`wait-times.processor.ts`, docs: `docs/architecture/reverse-reconciliation.md`): Attractions that disappear from every upstream source (ThemeParks.wiki, Queue-Times, Wartezeiten) for >24h are now auto-closed. A Redis `attraction:last-seen:{id}` key is touched only by real source sightings (never by the heartbeat). After each park's 5-minute sync the processor diffs seen vs. known attractions and writes a `status=CLOSED` `queue_data` entry for any attraction stale for >24h. Grace period of 24h protects newly created rides from premature closure, and the safety guard `seenAttractionIds.size > 0` prevents mass-close during upstream outages. The hourly heartbeat now also skips stale attractions instead of preserving their last `OPERATING` status. Fixes Movie Park Germany's Halloween mazes (e.g. *A Quiet Place*) showing "open, 0 min" year-round.
- **`POST /admin/detect-seasonal`** (`admin.controller.ts`, `admin.module.ts`): Manual trigger for the `detect-seasonal` analytics job (normally daily at 2:30 am). Intended to re-evaluate seasonal flags after deploying the reverse-reconciliation fix so newly `CLOSED` attractions get `isSeasonal=true` + `seasonMonths` populated without waiting for the cron.

### Fixed

- **Stale "open with 0 min" status for disappeared attractions** (`wait-times.processor.ts`): Previously `writeHourlyHeartbeats` re-stamped `lastUpdated=now` with the previous `status` every hour for any attraction missing from the feed, so seasonal Halloween mazes and silently-removed rides remained `OPERATING` forever. The heartbeat now reads `attraction:last-seen:{id}` and skips attractions not seen in any source for >24h; the reverse-reconciliation step has already written `CLOSED` for them. Root cause was the missing counter-signal: no upstream source ever reports "this attraction no longer exists".
- **P50 baselines missing for UNKNOWN parks** (`analytics.service.ts`): `identifyHeadliners` and `calculateAttractionP50` both filtered `scheduleType = 'OPERATING'`. Parks with UNKNOWN schedule entries (USJ, Universal Studios, Warner Bros Movie World, Blackpool etc.) had no headliners identified → no P50 baseline → `getCurrentOccupancy` returned hardcoded 100 → `park_occupancy_pct = 100` flat for all UNKNOWN parks at inference (feature useless for 22+ parks). Fixed: changed both queries to `IN ('OPERATING', 'UNKNOWN')`. Safe because both queries already filter `qd.status = 'OPERATING'` AND `qd.waitTime >= 10` — truly closed parks produce 0 qualifying rows regardless of schedule type.

---

### ML: ride-based park open/closed detection

- **ML: ride-based park open/closed detection** (`parks.service.ts`): `getBatchParkStatus` and `isParkOperatingToday` now derive open/closed status from live ride data when no confirmed schedule exists. Threshold: ≥3 attractions with recent data AND ≥25% reporting `waitTime ≥ 5 min`. Window: 2h for real-time status, park-local today for daily planning. Parks with explicit `CLOSED` schedule today are excluded from the heuristic.
- **ML: `parkLiveStatus` feature context** (`ml.service.ts`, `predict.py`, `feature-context.type.ts`): NestJS now passes `featureContext.parkLiveStatus` to the Python ML service. In `predict.py`, UNKNOWN-schedule rows are corrected to `is_park_open=1` when the park is confirmed OPERATING via ride data. Explicit CLOSED entries are never overridden. Fixes predictions for parks like Six Flags, Universal, and other parks that report UNKNOWN schedule but are genuinely open.
- **ML dashboard: model metrics history endpoint** (`GET /v1/ml/models/metrics-history?limit=50`): Returns MAE, RMSE, MAPE, R² per trained model ordered oldest→newest for sparkline charts. See integration guide for frontend.

### Fixed

- **UNKNOWN schedule parks excluded from prediction generation** (`parks.service.ts` `isParkOperatingToday`): Parks with `scheduleType=UNKNOWN` (e.g. Six Flags, Universal Hollywood, 66 parks affected) were treated the same as CLOSED — no predictions generated. Fixed: UNKNOWN falls through to ride-data check; if no data, defaults to `true` (conservative).
- **`getBatchParkStatus` heuristic over-filtered** (`parks.service.ts`): Previous filter excluded parks that ever had any OPERATING schedule entry, making the heuristic dead code for most UNKNOWN parks. New filter: only exclude parks with explicit CLOSED schedule today (park-local timezone via `AT TIME ZONE` join). Threshold raised from `waitTime > 0` (any single ride) to ≥25% with `waitTime ≥ 5`.
- **`CURRENT_DATE` UTC vs. park-local** (`parks.service.ts`): CLOSED-schedule exclusion query used `date = CURRENT_DATE` (UTC), which could match wrong date for UTC+ parks at night. Fixed with `(CURRENT_TIMESTAMP AT TIME ZONE p.timezone)::date` via JOIN on parks.
- **Daily predictions: `parkLiveStatus` always "CLOSED" at night** (`prediction-generator.processor.ts`): Daily prediction generator called `getBatchParkStatus` at runtime (e.g. 02:00 UTC), getting `"CLOSED"` for parks outside operating hours → UNKNOWN override never fired. Fixed: parks that pass `isParkOperatingToday` now receive `liveStatus="OPERATING"` explicitly.
- **ML training UNKNOWN filter reverted** (`ml-service/db.py`): Including UNKNOWN-schedule parks in training data caused MAE to jump from ~5.9 → 14.4 min and R² to drop from 0.86 → 0.37. Root cause: UNKNOWN days include closed parks still sending 5-min sentinel values — the filter can't distinguish real operating data from sentinel data. Reverted to `scheduleType = 'OPERATING'` only. The training/inference asymmetry for UNKNOWN parks is accepted; `parkLiveStatus` correctly handles them at inference time without needing training examples.
- **`park_has_operating` UUID type mismatch** (`ml-service/predict.py`): Dict key built from `schedules_df["parkId"]` could be a UUID object while `row["parkId"]` was a string → silent dict miss → UNKNOWN override never fired. Fixed: `astype(str)` on groupby key + `str(row["parkId"])` at lookup.
- **Dead code in `features.py`** (`ml-service/features.py`): `parkLiveStatus` override block in `add_park_schedule_features` was unreachable (only called during training where `feature_context=None`). Removed; the authoritative override is in `predict.py`.

### Weather forecast in integrated park response

### Fixed (weather)

- **Weather DATE timezone off-by-one** (`weather.service.ts`): Two bugs caused non-UTC parks to show wrong weather. (1) Save used `fromZonedTime(midnight, tz)` → east-of-UTC parks (e.g. `Europe/Berlin`) stored dates shifted -1 day (March 31 saved as March 30). (2) Query used `DATE(weather.date AT TIME ZONE :tz)` — PostgreSQL casts DATE to midnight-UTC timestamptz first, then shifts back to local time, which for west-of-UTC parks (e.g. `America/New_York`) moves today's date to yesterday → `current` always null. Fixed: save uses noon-UTC (`new Date(\`${date}T12:00:00Z\`)`), query uses direct date-string comparison (`weather.date >= :start`).
- **Weather empty for US parks** (root cause above): Parks like "Universal's Epic Universe" returned `weather: { current: null, forecast: [] }`. The park has coordinates and Open-Meteo data; the off-by-one query excluded today's DB record. (`park-integration.service.ts`, `park-with-attractions.dto.ts`): The integrated park endpoint now returns `weather.forecast` (next 6 days) in addition to `weather.current`. Previously `getCurrentAndForecast()` fetched 16 days from DB but only `current` was mapped into the response. The API now exposes today + 6 forecast days (7 total).
- **Weather architecture doc** (`docs/architecture/weather.md`): Documents Open-Meteo sync strategy, storage schema, BullMQ jobs, timezone handling, DATE timezone bug pattern, and why parks may have empty weather (missing lat/lng coordinates).
- **Weather cache TTL extended** (`weather.service.ts`): Increased from 30 minutes to 2 hours. Weather data changes at most twice a day (sync at 00:00 and 12:00 UTC); frequent cache misses caused unnecessary DB load.

### Fixed

- **P50/headliner: `waitTime >= 10` filter** (`analytics.service.ts`, `calendar.service.ts`, `stats.service.ts`, `attraction-integration.service.ts`): All historical wait-time aggregations (headliner identification, P50 baseline calculation, weekday averages, percentiles, longest waits) used `waitTime > 0`, while the real-time path used `minWaitTime=5`. Queue-Times API reports `waitTime=1` as a walk-on/no-queue placeholder (common for water parks, e.g. Rulantica slides). This caused ~40–65% of water-park samples to be 1-minute placeholders, depressing P50 baselines and causing "Extreme" crowd level while individual rides showed normal waits. Fixed by aligning all historical queries to `waitTime >= 10`. The existence check `hasQueueDataInWindow` is intentionally kept at `> 0`.
- **P50/headliner: schedule-based closed-day exclusion** (`analytics.service.ts`, `calendar.service.ts`): Seasonal parks (Kennywood, Canada's Wonderland) accumulate queue data during off-season months. Without filtering, closed-day data drags P50 baselines down (e.g., Kennywood: 31 raw data days → 7 OPERATING days). Fixed by adding a `LEFT JOIN schedule_entries` (park-level, `attractionId IS NULL`) to all historical queries, using `DATE(qd.timestamp AT TIME ZONE <park_tz>)` for correct local-date matching. Days with no schedule entry are included; days with `OPERATING` are included; any other type is excluded.
- **ML training: same `>= 5` and schedule filters** (`ml-service/db.py`): Training data extraction used `waitTime >= 0` and had no schedule filter. Now applies `waitTime >= 10` and the same schedule JOIN (with `JOIN parks p` for timezone). Requires retraining to take effect.
- **ML training: `fetch_recent_wait_times` `>= 5` filter** (`ml-service/predict.py`): Inference recent-wait lookup also aligned to `waitTime >= 10` + schedule JOIN.
- **ML: historical occupancy DOW×hour timezone bug** (`ml-service/db.py`): `fetch_historical_park_occupancy` built the (DOW, hour) occupancy profile using `EXTRACT(DOW/HOUR FROM qd.timestamp)` (UTC), but inference looked up with local park time → systematic 1–2 hour shift for all non-UTC parks. Fixed by joining `parks` and using `AT TIME ZONE p.timezone` in the GROUP BY. Since `queue_data` has no `parkId`, the join path is `queue_data → attractions → parks`.

### Changed

- **DB indexes: remove unused** (`ml-prediction-request-log.entity.ts`, `park-p50-baseline.entity.ts`, `attraction-p50-baseline.entity.ts`, `attraction.entity.ts`, `park.entity.ts`, `ml-model.entity.ts`): Removed ~182 MB of unused indexes from `ml_prediction_request_log` (6 indexes with 0–2 scans) and 6 further duplicate/zero-scan indexes across other entities. TypeORM `synchronize: true` creates new indexes but does not drop removed ones; `scripts/drop-unused-indexes.sql` must be run once on production.
- **DB index: new partial index for schedule JOIN** (`schedule-entry.entity.ts`): Added `idx_schedule_park_date_no_attraction` — partial index on `(parkId, date) WHERE "attractionId" IS NULL`. Covers the `schedule_entries` lookup in all analytics and ML historical queries without touching attraction-level schedule rows.

- **ML: 5-minute prediction bug** (`model.py` `predict_with_uncertainty`): `virtual_ensembles_predict` was called with `prediction_type="TotalUncertainty"`, which returns uncertainty scalars `[knowledge_unc, data_unc]` (shape `(n, 2)`), not per-ensemble predictions. `np.mean(axis=1)` averaged the two ~2.77 values → `round_to_nearest_5` → **5 min** for all predictions. Fixed by switching to `prediction_type="VirtEnsembles"` (shape `(n, 10, 1)`), squeezing to `(n, 10)`, and taking `median ± std` instead of `p5/p95` (more stable at n=10).
- **ML: NoneType crash in `fetch_holidays`** (`db.py`): `sorted(country_codes)` failed when the list contained `None` (parks with missing country metadata). Fixed by filtering: `country_codes = [c for c in country_codes if c is not None]`.
- **ML: Weekend underprediction** (`features.py`, `predict.py`, `config.py`): `volatility_7d` dominated feature importance at 32.91% while `is_weekend` was 0.01% and `avg_wait_last_1h` was 0.00%. The model could not distinguish weekday vs weekend crowd levels. Fixed by:
  - Splitting `volatility_7d` into `volatility_weekday` + `volatility_weekend` in training pipeline (`calculate_trend_volatility`)
  - Adding `rolling_avg_weekday` + `rolling_avg_weekend` via SQL window functions in `fetch_recent_wait_times`
  - Adding `avg_wait_same_dow_4w` (mean of last 4 same-day-of-week observations) for a stable historical reference
  - Lowering `VOLATILITY_CAP_STD_MINUTES` from 40 → 15 to reduce volatility dominance
  - All new features propagated to inference in `predict.py`
  - Detailed analysis: [Prediction Quality Issues](ml/prediction-quality-issues.md)

- **ML: Flat future predictions / hour importance 0.84%** (`features.py`, `db.py`, `predict.py`): `park_occupancy_pct` (15% importance) was broadcast from the current real-time value to ALL prediction rows — including rows 24h or 14 days in the future — causing flat, hour-invariant predictions. Fixed in two stages:
  - **Inference fix** (`db.py`, `features.py`): `fetch_historical_park_occupancy()` computes expected park occupancy by (DOW, hour) over the last 8 weeks (via `attractions` JOIN, since `queue_data` has no `parkId`). `add_park_occupancy_feature` now applies real-time occupancy only to rows within ±2h of base_time; future rows use the DOW×hour historical profile.
  - **Training fix** (`features.py`, `config.py`): Occupancy Dropout — 30% of training rows have their actual `park_occupancy_pct` replaced with the DOW×hour mean from the same park's training data (`OCCUPANCY_DROPOUT_RATE=0.30`). This teaches the model to rely on `hour`/`day_of_week` when occupancy is approximate, closing the gap for future predictions.

- **Schedule date-shift bug** (`saveScheduleData`): ThemeParks.wiki returns dates as date-only strings (`"YYYY-MM-DD"`). These were passed to `new Date()`, producing midnight UTC, which `formatInParkTimezone` then shifted back by one day for parks west of UTC (e.g. a park with `date:"2026-03-02"` was stored as `2026-03-01` in America/New_York). Fix: detect date-only strings via regex and use them directly without timezone conversion. Full ISO timestamps (from wartezeiten/queue-times processors) still go through `formatInParkTimezone`. (Bug: today's schedule entry stored under yesterday's DB date; opening hours were 1–2 days off in live DB for US parks.)
- **Holiday date range in `saveScheduleData`**: Date range for holiday pre-fetch was built from `new Date(e.date)` (midnight UTC), causing `formatInParkTimezone` to shift the range back 1 day for US parks. Fixed: use noon-UTC timestamps (`${dateStr}T12:00:00Z`) consistent with the rest of `saveScheduleData`.
- **Weather service date filter** (`weather.service.ts`): `allWeather.find()` and `.filter()` used `formatInParkTimezone(new Date(w.date), tz)` on a TypeORM DATE column (midnight UTC). For US parks this shifts midnight UTC to the previous calendar day, causing today's weather entry to be lost (not matched as "current" and excluded from "forecast"). Fixed: extract date string via `w.date.toISOString().split("T")[0]`, which is always correct because midnight UTC IS the calendar date stored in the DB.
- **Schedule response missing today's entry** (`buildIntegratedResponse`): Added filter `date >= todayInParkTz` to trim past entries (DB query fetches from -2 days), and a synthetic OPERATING entry for today if the park is operating but its schedule row is missing.
- **`peakHour` timezone ambiguity** (`analytics.service.ts`): Changed from returning `"HH:mm"` (plain string, interpreted as UTC by frontend) to a full ISO-8601 datetime with timezone offset (`"2026-03-02T11:00:00-05:00"`), eliminating frontend UTC misinterpretation.
- **Cache invalidation on INSERT** (`saveScheduleData`): `invalidateScheduleCache` was only called after UPDATE, not after INSERT. New entries would remain stale for up to 1 hour. Fixed: call `invalidateScheduleCache` after INSERT too.

### Changed

- **Calendar API:** UNKNOWN→OPERATING upgrade only for parks **without** OPERATING entries in `schedule_entries`. Parks with schedule integration keep UNKNOWN for days without schedule (DB-check via `hasOperatingSchedule`). Fixes Phantasialand Jan 26–31 incorrectly showing OPERATING.
- **Gap-fill** (`fillScheduleGaps`): Look-back added. Range: (today - 182 days) through (today + 182 days). Past gaps (e.g. winter closure Jan–Mar) are re-evaluated when new OPERATING (e.g. March 28) arrives, so UNKNOWN→CLOSED is promoted correctly.

### Performance

#### Schedule Sync Optimizations (NestJS)
- **Schedule sync (`saveScheduleData`)**: Batch DELETE operations for cleanup placeholders (UNKNOWN/CLOSED removed when API provides real data) reduced from ~300 individual queries to **3 batch queries** (99% reduction). Code deduplication: normalize scheduleType once instead of 3× redundant iterations.
- **Gap-fill (`fillScheduleGaps`)**: Batch INSERT/UPDATE operations for gap-filled entries and status changes reduced from ~364 individual queries to **~5 batch queries** (98.6% reduction). All iterations collect entries/updates in-memory, then execute bulk operations using `createQueryBuilder().insert()` and `whereInIds()`.
- **Duplicate cleanup (`cleanupDuplicateScheduleEntries`)**: SQL window functions and CTEs replace N+1 queries; same-type and cross-type duplicate detection reduced from ~160 queries to **2 queries** (98.8% reduction). Uses PostgreSQL `ROW_NUMBER()` OVER (PARTITION BY) for efficient deduplication.
- **Per-park cleanup**: New `cleanupDuplicateScheduleEntriesForPark()` method called before gap-fill to prevent duplicates from parallel schedule syncs (runs targeted cleanup for single park instead of waiting for daily global cleanup).
- **Operating date range extraction**: New `getOperatingDateRange()` helper extracts min/max OPERATING date logic into reusable function (used by gap-fill classification and calendar fallback).

**Schedule sync impact**: Typical schedule sync reduced from ~924 database queries to ~12 queries (**98.7% reduction**), estimated duration improvement from ~92 seconds to ~1.2 seconds.

#### ML Service Optimizations (Python) – 2026-02-15
- **Database query caching**: Added in-memory caching for holidays (1h TTL), schedules (5min TTL), recent wait times (2min TTL), and weather historical data (1h TTL). Reduces repeated queries for unchanged data.
- **Query optimization with window functions**: `fetch_recent_wait_times` now pre-computes `rolling_avg_7d` and `rolling_std_7d` using PostgreSQL window functions instead of Python aggregation. Reduces data transfer and eliminates expensive Python loops.
- **Holiday lookup vectorization**: Replaced loop over 1000+ prediction rows with pandas `.map()` operations. Pre-processes park metadata once instead of per-row. Eliminates JSON parsing in loop.
- **Historical features optimization**: Uses pre-computed rolling averages and standard deviations directly from database instead of Python calculations.

**ML service impact**:
- First request (cold cache): **40-50% faster**
- Cached requests (warm cache): **70-85% faster**
- Daily predictions (365 days): up to **90% faster**
- Database query reduction for repeated requests: **80-90%** fewer queries

**Documentation**: [ML Performance Optimizations](ml/performance-optimizations.md)

---

## [4.6.2] – 2026-02-08

### Changed

- **Schedule sync / Gap-fill**
  - **Doc:** "When gap-fill runs (DB updates are automatic)" in [Schedule Sync & Calendar](architecture/schedule-sync-and-calendar.md): gap-fill runs after every schedule sync (sync-all-parks, sync-schedules-only, sync-park-schedule); optional job `fill-all-gaps` for all parks. No one-off DB correction needed when using park-timezone range.
  - **lookAheadDays:** Default increased from 90 to **120 days** so the DB is filled further ahead (typical 4‑month planning).
- **Calendar warmup:** Range extended from "current + 2 months" to **-1 to +3 months** (last month through 3 months ahead, park timezone) so the typical user range (recap + planning) is cache-hot after daily warmup.

---

## [4.6.1] – 2026-02-08

### Added

- **Calendar, Schedule & ML rules doc** (`docs/architecture/calendar-schedule-and-ml-rules.md`): Single source of truth for status (OPERATING/CLOSED/UNKNOWN), crowd level, schedule sync, next schedule, and ML alignment.
- **Frontend doc** (`docs/frontend/calendar-schedule-status.md`): How to display calendar status (UNKNOWN vs CLOSED) in the UI. Linked from CLAUDE.md.
- **Changelog** (`docs/changelog.md`): This file; linked from CLAUDE.md.
- **Timezone Audit** (`docs/development/timezone-audit.md`): Audit of all time operations against park timezone. Linked from CLAUDE.md.

### Changed

- **Calendar API**
  - Status is derived only from schedule (and the rule "past/today + crowd level = OPEN"); crowd level no longer overrides status.
  - Past and today: if no schedule but we have a (non-closed) crowd level → treat as OPERATING so we can show data.
  - Future: use schedule (OPEN/CLOSED/UNKNOWN); future days without schedule stay UNKNOWN but get a crowd prediction (ML or fallback "moderate"), not "closed".
- **Schedule sync**
  - `saveScheduleData`: API type "Closed"/"CLOSED" (case-insensitive) is normalised to `ScheduleType.CLOSED` so off-season (e.g. Phantasialand February) is stored when the API provides it. When saving OPERATING, any gap-fill CLOSED for that date is now deleted so the API entry takes precedence.
  - **Gap-fill** (`fillScheduleGaps`): Missing days are now classified as CLOSED or UNKNOWN:
    - **CLOSED** if there is at least one OPERATING day before and one after the gap (strictly between min/max OPERATING dates).
    - **UNKNOWN** if the park has no OPERATING entries, or the gap is before the first OPERATING date (e.g. before we have data), or on/after the last OPERATING date (schedule not yet published).
    - Existing UNKNOWN entries can be updated to CLOSED when re-running gap-fill if they are now "in the middle". OPERATING and API-provided CLOSED are never overwritten.
  - Gap-fill range uses **park timezone** (`getStartOfDayInTimezone`, `addDays`) so the filled range is always "today" through "today + 90" in the park's calendar.
- **Docs**
  - All relevant docs translated to English (frontend calendar status, review, troubleshooting peak hour section, calendar-schedule-and-ml-rules).
  - Schedule sync & calendar doc: new "Gap-fill rules" section; UNKNOWN vs CLOSED and Gaps sections updated.
  - CLAUDE.md: added Frontend section and link to Calendar, Schedule & ML Rules; Critical Rules strengthened (park timezone for all time operations); link to this changelog and Timezone Audit.

### Fixed

- Calendar no longer shows "Öffnungszeiten noch nicht verfügbar" for days that are known to be closed (gap-fill and API CLOSED now set status CLOSED where appropriate).
- When the API provides OPERATING for a date that had a gap-fill CLOSED, the calendar could show CLOSED (because `getSchedule` orders by scheduleType ASC). `saveScheduleData` now deletes any CLOSED row for that date when saving OPERATING.
- **Timezone audit:** All time operations now use park timezone. Fixed: `getUpcomingSchedule` (range in park TZ), `weather.service` fallback + `markPastDataAsHistorical` (per-park), `getBatchParkHours` (per-park today), `getParkPercentilesToday` / `getAttractionPercentilesToday` (startOfDay in park TZ), `tomorrowInParkTz` (getTomorrowDateInTimezone), `isParkCurrentlyOpen` / `isParkOperatingToday` (getCurrentDateInTimezone).

---

## [Older versions]

Older changes were not recorded in this changelog. From this version onward, notable changes will be listed here with version and date.

---

(Compare URLs can be added when using a Git remote, e.g. `[4.6.1]: https://github.com/owner/repo/compare/v4.5.0...v4.6.1`.)
