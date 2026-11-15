import type { KnowledgeEntry } from "./knowledge-entry";

/**
 * The seed of the topic on what a ride's status means and how sure a reopening is. The facts
 * follow docs/frontend/effective-status.md, works-period.md and seasonality.md; once seeded, the
 * stored topic is edited in the admin.
 */
export const RIDE_STATUS: KnowledgeEntry = {
  id: "ride_status",
  readWhen:
    "what a ride's status means (running, down, closed, refurbishment, unknown, out of season, " +
    "set showtimes) and how a reopening estimate is made. Read it when the user asks what a " +
    "status means, why a ride reads closed or unknown while they think it runs, or how sure a " +
    "reopening time is.",
  text: `A ride's status as park.fan shows it:
- Operating: a source reports it running. A ride without a queue (a playground, a walk-through) reads open with no wait while the park is open and the ride is in season.
- Down: it stopped during the day, a breakdown or a technical stop. get_ride says since when and estimates when it may run again from how this ride's past outages ended: an estimate, never a promise.
- Closed: the park is closed (then every ride reads closed, whatever a source says), the park reports the ride closed, or it is out of season.
- Refurbishment: shut for maintenance or works, often over days. Planned windows (worksPeriods) are the park's local days, both ends included; one without an end has no known end.
- Unknown: no source says anything right now, as in a park whose waits are only in its own app, or a ride no source has measured for 14 days while the park is open. Unknown is not closed.
- Set showtimes: a ride that starts at fixed times, like a 4D cinema, shows its next start instead of a wait; "closed" between two starts is no closure.

A ride can open later than the park's gates (its opensAt); closed before then is no breakdown. A ride that runs only part of the year reads out of season outside its months and does not count among the open rides. When a source says a ride runs while the park is closed, the park wins: the ride reads closed.`,
};
