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

  /** Glob matching every schedule:* key of a park (parkId is the 3rd segment!). */
  scheduleParkPattern: (parkId: string): string => `schedule:*:${parkId}:*`,

  /** Glob matching the calendar month cache of a park. */
  calendarMonthPattern: (parkId: string): string =>
    `calendar:month:${parkId}:*`,
} as const;
