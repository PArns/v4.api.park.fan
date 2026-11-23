/**
 * How long a warmed calendar month lives in Redis — and, because a projection
 * may not outlive its source, the ceiling for anything derived from one.
 *
 * The calendar warmup runs every 12h and rewrites these month caches; the extra
 * hour is the buffer that keeps a slightly late run from finding them cold.
 * `buildCalendarResponse` stores them at its own CALENDAR_CACHE_TTL (15–30 min),
 * which would expire long before the next warmup, so the warmup re-`expire`s
 * them to this.
 *
 * It lives here rather than beside either consumer because two of them have to
 * agree and used not to: the best-days snapshot is `calendar.days.map(…)` and
 * was given 26h against these 13h, so for half a day it served a forecast the
 * calendar had already replaced — the same Saturday reading "wenig Auslastung"
 * in a month grid and "SEHR HOCH" in the day panel beside it. One number is
 * what stops that being a comment somebody has to remember.
 */
export const CALENDAR_WARMUP_MONTH_TTL = 13 * 60 * 60;

/**
 * How long the season level's inputs live (`CacheKeys.seasonLevelInputs`):
 * the calendar warmup evicts and rebuilds them every 12 h, and they outlive
 * the month caches built from them, so a late run finds the months cold
 * before the inputs, and the months rebuild from the inputs the run left.
 * Past it, a warmup has failed and the next read recomputes them, which no
 * forecast ring announces.
 */
export const SEASON_LEVEL_INPUTS_TTL = CALENDAR_WARMUP_MONTH_TTL + 60 * 60;

/**
 * How long a failed load of the season level's inputs is remembered
 * (`CacheKeys.seasonLevelInputsFailed`). The usual failure is a read running
 * into its 20-second statement timeout on a loaded database; run again by
 * every serving read, it adds those 20 seconds to each and load to the
 * database that caused it. Five minutes, so a passing slowdown costs the
 * level for minutes, not until the next warmup.
 */
export const SEASON_LEVEL_FAILURE_TTL = 5 * 60;

/**
 * The CURRENT month is the exception and keeps its own, much shorter life: its
 * data still moves intraday (today's live level, today's forecast) and it is
 * what somebody checks right after a deploy, so it must not freeze for a whole
 * cycle. Anything projected from it inherits a window this does not close — see
 * the note on the best-days snapshot's TTL.
 */
export const CALENDAR_WARMUP_CURRENT_MONTH_TTL = 2 * 60 * 60;

/**
 * A trip region's calendar months and its plan and best-days answers: an hour,
 * the edge lifetime the contract gives them (docs/frontend/trip-regions.md).
 * They are assembled from the month caches above, so an hour is the most they
 * lag a warmup.
 */
export const TRIP_REGION_RESPONSE_TTL = 60 * 60;

/**
 * A `/when` answer (docs/frontend/trip-regions-when.md): six hours. It ranks a
 * year of windows by plans that the twice-daily warmup refreshes, and a whole
 * year is a few hundred milliseconds of planning to rebuild.
 */
export const TRIP_WHEN_TTL = 6 * 60 * 60;

/**
 * A trip region's forecast accuracy (`/accuracy`): a day. The archive it
 * reads is scored once a night, and the key carries the region's date, so a
 * new day starts a new entry.
 */
export const TRIP_ACCURACY_TTL = 24 * 60 * 60;

/**
 * A trip region's ticket catalog in Redis. An admin write evicts it, so the
 * hour only bounds a write made outside the admin (a raw UPDATE).
 */
export const TRIP_TICKETS_TTL = 60 * 60;

/**
 * A past month's measured figures, kept for the `last_year` days. A month that
 * has passed does not change, and unlike the month caches it is not evicted by
 * a schedule sync (that is about future hours), so a week is safe and spares
 * the warmup rebuilding five past months per park twice a day.
 */
export const TRIP_LAST_YEAR_TTL = 7 * 24 * 60 * 60;
