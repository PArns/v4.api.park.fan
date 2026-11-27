import type { KnowledgeEntry } from "./knowledge-entry";

/**
 * The seed of the topic on park.fan's crowd readings and forecasts, and why "now", "today's
 * forecast" and "today so far" differ. The facts follow docs/analytics/crowd-levels.md and the
 * Fancast page; once seeded, the stored topic is edited in the admin, not here.
 */
export const FORECASTS: KnowledgeEntry = {
  id: "forecasts",
  readWhen:
    'how our forecasts and crowd levels are made (Fancast), and why "now", "today\'s forecast" ' +
    'and "today so far" differ. Read it when the user says a forecast or a crowd level is wrong, ' +
    "doubts it, sees two different values, compares us with another site, or asks how we forecast.",
  text: `Three crowd readings exist, and they answer different questions.
- "Crowds now" (the park's header and today panel; get_park_live crowd.level): this moment. The big rides' current waits against their normal wait (their median), refreshed every five minutes. A busy hour reads high even on an average day.
- "Forecast today" and every day in the crowd calendar (get_park_live todayExpected, get_days crowdLevel): the whole day. The big rides' expected peak against the peak of a typical day at that park. Made in advance and kept for the day; it does not follow the live waits.
- "So far today" (the crowd calendar's day dialog, beside the forecast): today as measured since opening, on the same scale as the forecast. A quiet morning keeps it below the live reading at noon.
So "high now" beside "normal so far" or a moderate forecast is no contradiction: one is the moment, the others the whole day. All three use the same ladder (very low, low, moderate, high, very high, extreme), and moderate is a typical day for that park, not an empty one.

How the forecast is made: Fancast, park.fan's own model.
- It reads the wait history (one reading per queue every five minutes from every park on park.fan, without a gap since April 2026), weekends, public holidays and school breaks including the neighbouring regions', the weather forecast, events and opening hours.
- It retrains every day at 06:00 UTC on the day before, and gives an hourly wait forecast per ride and a crowd level per park day.
- Near days sharpen as the weather forecast and events come in. A day further out than about two months is estimated from last year (the tools' basis says so), not forecast.
- A park needs about 30 operating days before it is rated at all; until then it reads "no forecast".
- Every forecast is checked against the wait measured afterwards, and the Fancast page shows the current error in minutes, bad days included. A forecast can miss: an event, a ride down, a storm.

When the user says a forecast is wrong or sees two values:
1. Find out which value they mean: now, today's forecast, so far today, or another site's number.
2. Check it: get_park_live for now and today's forecast, get_days for a day.
3. Say in two or three sentences which reading is which and why they differ, without formulas or percentiles.
4. Another site measures something else (a day's average over all rides, an index from 1 to 10, a peak): compare like with like and ask for their number before judging.
5. Offer the Fancast page for the track record.
Pass it on with submit_feedback only when, after that, the same reading still shows two ways or the user asks you to.`,
};
