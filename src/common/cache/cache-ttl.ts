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
 * The CURRENT month is the exception and keeps its own, much shorter life: its
 * data still moves intraday (today's live level, today's forecast) and it is
 * what somebody checks right after a deploy, so it must not freeze for a whole
 * cycle. Anything projected from it inherits a window this does not close — see
 * the note on the best-days snapshot's TTL.
 */
export const CALENDAR_WARMUP_CURRENT_MONTH_TTL = 2 * 60 * 60;
