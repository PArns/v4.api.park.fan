import type { KnowledgeEntry } from "./knowledge-entry";

/**
 * The seed of the topic on how a crowd level is rated and why one wait rates differently at two
 * rides. The facts follow docs/analytics/crowd-levels.md and the "How park.fan works" page
 * (chapters 01, 02 and 08); once seeded, the stored topic is edited in the admin.
 */
export const CROWD_SCALE: KnowledgeEntry = {
  id: "crowd_scale",
  readWhen:
    "how a crowd level is rated (the ladder from very low to extreme) and why the same wait rates " +
    "differently at two rides, parks or weekdays. Read it when the user asks what a level means, " +
    'why 70 minutes is "very high" here and normal elsewhere, or what normal and busy mean on a ' +
    "ride's page.",
  text: `Every level compares a number with that ride's or that park's own past, never with other parks.
- The ladder, as a share of the reference: very low up to 60 %, low 61 to 89 %, moderate 90 to 110 % (a typical day or moment for that ride or park, not an empty one), high 111 to 150 %, very high 151 to 200 %, extreme above 200 %.
- Right now (a ride or a park live): the current wait against the ride's normal wait, the median of its measured waits. Taron's normal in September was 45 minutes, so 70 minutes, about one and a half times that, read very high.
- A day (the crowd calendar, today's forecast, past days): the day's peak at the big rides against the peak of a typical day at that park. Busy seasons such as a park's winter event or Easter rightly read high to extreme.

On a ride's page, "Normal" is how long the day's longest queue at this ride usually gets and "Busy" how long it got on the busiest ten percent of days, per weekday: 70 minutes at Taron is a lot for a Monday and perfectly normal for a Saturday. The record is the longest queue ever measured; a single day, it barely moves the scale.`,
};
