import type { KnowledgeEntry } from "./knowledge-entry";

/**
 * The seed of the topic on how a ride's best visit times today and the rope-drop tip are chosen.
 * The facts follow src/common/utils/best-visit-times.util.ts, docs/frontend/ride-live-crowd-level.md
 * and the "How park.fan works" page (chapter 03); once seeded, the stored topic is edited in the
 * admin.
 */
export const BEST_TIMES: KnowledgeEntry = {
  id: "best_times",
  readWhen:
    "how a ride's best visit times today and the rope-drop tip are chosen, and what optimal and " +
    "good mean. Read it when the user asks how the best times are picked, why a best time " +
    "differs from the wait now, or whether arriving at opening pays off.",
  text: `Best visit times today come from the forecast for the rest of today in 15-minute slots, up to closing:
- Optimal is the slot with the lowest forecast wait; good is at most a quarter above it (or 5 minutes, where that is more). At most two optimal and three good slots are shown, at least an hour apart.
- The slot running now uses the live wait instead of the forecast, so a sudden spike never shows as a best time. A ride that is not running gets no "now" slot; the later ones stay, since it may be back by then.
- A ride on set showtimes gets its next start instead. A ride with no measured day in the past year, or in a park whose waits cannot be read, gets none.
- They are a forecast and a tip, never a promise: an event, a breakdown or the weather moves them.

The rope-drop tip (be there at opening) is given only for a ride whose daily peak reaches at least 60 minutes and where the early start saves at least 45 of them. Its card names the typical wait at opening, the daily peak, the minutes saved and until when the head start holds. At some rides the time of day barely matters (Taron stays in a narrow band all day, and the weekday matters more), at others it does (Chiapas gets clearly busier into the afternoon), which is why every ride gets its own.`,
};
