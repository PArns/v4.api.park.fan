/**
 * Builders for Redis cache keys that are constructed in more than one file.
 *
 * Single-file keys may stay inline; everything here exists because at least
 * two call sites build (or invalidate) the same key, and string drift between
 * them silently breaks invalidation — e.g. the park merge/repair services
 * used to delete `schedule:${parkId}:*` although the real keys are
 * `schedule:today:${parkId}:…` (parkId is the third segment), so the
 * schedule cache survived every park merge.
 *
 * Formats are kept exactly as they were so deploys don't orphan warm caches.
 */
export const CacheKeys = {
  /** Integrated park response (hottest endpoint; warmed by cache-warmup). */
  parkIntegrated: (parkId: string): string => `park:integrated:${parkId}`,

  /** Integrated attraction response. */
  attractionIntegrated: (attractionId: string): string =>
    `attraction:integrated:${attractionId}`,

  /** 16-day weather forecast per park. */
  weatherForecast: (parkId: string): string => `weather:forecast:${parkId}`,

  /** Park-level ML predictions for one park-local day. */
  mlParkPredictions: (
    parkId: string,
    predictionType: string,
    dateStr: string,
  ): string => `ml:park:${parkId}:${predictionType}:${dateStr}`,

  /** Today's schedule (dateStr = park-local YYYY-MM-DD). */
  scheduleToday: (parkId: string, dateStr: string): string =>
    `schedule:today:${parkId}:${dateStr}`,

  /** Next operating day's schedule. */
  scheduleNext: (parkId: string, dateStr: string): string =>
    `schedule:next:${parkId}:${dateStr}`,

  /** Upcoming schedule window. */
  scheduleUpcoming: (parkId: string, dateStr: string, days: number): string =>
    `schedule:upcoming:${parkId}:${dateStr}:${days}`,

  /** MIN/MAX operating date range per park. */
  parkOpDateRange: (parkId: string): string => `park:opdaterange:${parkId}`,

  /**
   * Live park occupancy snapshot (written by cache-warmup every 5 min;
   * read by analytics, calendar, discovery).
   */
  parkOccupancy: (parkId: string): string => `park:occupancy:${parkId}`,

  /**
   * One cached calendar month (ym = YYYY-MM). Written/read by the calendar
   * service; evicted by the 12h calendar warmup (includeHourly variant the
   * frontend reads is "none").
   */
  calendarMonth: (parkId: string, ym: string, includeHourly: string): string =>
    `calendar:month:${parkId}:${ym}:${includeHourly}`,

  /**
   * Precomputed best-days snapshot (rolling today → +90d projection of the
   * calendar). Materialized by the calendar warmup so the /best-days endpoint
   * serves it with a single GET and never triggers a lazy ML rebuild. TTL is
   * set > the warmup cadence so the snapshot survives between refreshes (and
   * across deploys, since Redis persists). Written by BestDaysService.
   */
  bestDays: (parkId: string): string => `best-days:${parkId}`,

  /** Glob matching every schedule:* key of a park (parkId is the 3rd segment!). */
  scheduleParkPattern: (parkId: string): string => `schedule:*:${parkId}:*`,

  /** Glob matching the calendar month cache of a park. */
  calendarMonthPattern: (parkId: string): string =>
    `calendar:month:${parkId}:*`,

  // ---------------------------------------------------------------------------
  // Park-scoped analytics/derived caches. These mirror keys still built inline
  // by their writers (analytics.service.ts, parks.service.ts,
  // park-historical-stats.service.ts); they live here so the merge/repair
  // invalidation path (invalidateParkCaches) can evict them without the string
  // drift that previously orphaned the schedule cache. Keep the formats in
  // sync with the writers.
  // ---------------------------------------------------------------------------

  /** Park statistics snapshot (5min TTL). Writer: analytics getParkStatistics. */
  parkStatistics: (parkId: string): string => `park:statistics:${parkId}`,

  /** Park P50 (median) baseline. Writer: analytics saveP50Baselines. */
  parkP50: (parkId: string): string => `park:p50:${parkId}`,

  /** Park P90 baseline (computed-for-free metadata). Writer: saveP50Baselines. */
  parkP90: (parkId: string): string => `park:p90:${parkId}`,

  /** Park typical-day-peak WAIT (calendar reference). Writer: cacheTypicalDayPeak. */
  parkTypicalDayPeak: (parkId: string): string => `park:typicalpeak:${parkId}`,

  /** Park typical peak HOUR ("HH:00"). Distinct from parkTypicalDayPeak. */
  parkTypicalPeakHour: (parkId: string): string =>
    `park:typical-peak:${parkId}`,

  /** Whether a park is seasonal (24h). Writer: parks.service. */
  parkIsSeasonal: (parkId: string): string => `park:isSeasonal:${parkId}`,

  /** Headliner attraction set for a park (6h). Writer: analytics. */
  analyticsHeadliners: (parkId: string): string =>
    `analytics:headliners:${parkId}`,

  /** Glob: every cached park crowd level (analytics:crowdlevel:park:<id>:<date>). */
  parkCrowdLevelPattern: (parkId: string): string =>
    `analytics:crowdlevel:park:${parkId}:*`,

  /** Glob: the park historical-stats cache (park:historical-stats:v2:<id>:…). */
  parkHistoricalStatsPattern: (parkId: string): string =>
    `park:historical-stats:v2:${parkId}:*`,

  /** Glob: the park derived-operating-hours cache (park:derivedHours:<id>:…). */
  parkDerivedHoursPattern: (parkId: string): string =>
    `park:derivedHours:${parkId}:*`,

  /** Glob: every ML prediction cache (daily/yearly) of a park. */
  mlParkPattern: (parkId: string): string => `ml:park:${parkId}:*`,

  // --- Attraction-scoped derived caches (evicted when attractions migrate). ---

  /** Attraction P50 (median) baseline. */
  attractionP50: (attractionId: string): string =>
    `attraction:p50:${attractionId}`,

  /** Attraction P90 baseline (computed-for-free metadata). */
  attractionP90: (attractionId: string): string =>
    `attraction:p90:${attractionId}`,

  /** Attraction rope-drop summary. */
  attractionRopeDrop: (attractionId: string): string =>
    `attraction:ropedrop:${attractionId}`,

  /** Attraction typical-day-peak baseline (per-attraction calendar reference). */
  attractionTypicalDayPeak: (attractionId: string): string =>
    `attraction:typicalpeak:${attractionId}`,

  /** Glob: an attraction's history cache (attraction:history:<id>:<days>). */
  attractionHistoryPattern: (attractionId: string): string =>
    `attraction:history:${attractionId}:*`,

  /** The discovery geo-structure skeleton (continent/country/city/park tree). */
  discoveryGeoStructure: (): string => "discovery:geo:structure:v4",

  /**
   * Shared, user-INDEPENDENT index of park coordinates (+ slugs) for the
   * /nearby distance queries. The per-user nearby RESULT is never cached —
   * only this static list, so every request reuses it instead of re-loading
   * every park from the DB. Busted on merge/repair (the park set changes).
   */
  parkLocationIndex: (): string => "location:parkcoords:v1",
} as const;
