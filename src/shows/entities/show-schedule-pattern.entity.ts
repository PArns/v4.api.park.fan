import { Entity, PrimaryColumn, Column, Index } from "typeorm";

/**
 * What a show's day usually looks like, per weekday.
 *
 * **No source knows showtimes in advance.** Verified against ThemeParks.wiki's
 * live endpoint for Europa-Park (186 times for today, and beyond that only stale
 * entries reaching back to 2022) and against our own data: not one park carries a
 * park-local showtime for a future day. A visitor planning October gets nothing
 * from the feed, and the planner's `shows` field was therefore always `[]`.
 *
 * So a projection is the only honest answer, and this table is what makes it
 * cheap. Computing it per request is not an option: one park's eight-week window
 * is 47,000 snapshots and 350,000 showtime entries, and the global aggregation
 * takes 5.7 s — fine once a night, absurd on a request.
 *
 * WHY PER WEEKDAY. Measured at Europa-Park, the difference is real and not
 * subtle: "Big Moments – The Celebration-Show" runs 12:30 and 14:30 on a
 * Thursday and 12:30, 14:30 and 17:45 on a Saturday; "Carnival in Venice" runs
 * hourly to 18:00 midweek and to 19:00 on Saturday. A weekday-blind projection
 * would either drop the extra performance or promise it on a Tuesday.
 *
 * WHY THE MOST RECENT MATCHING DAY, not a union across the window. A union
 * merges two programmes — a summer schedule and an autumn one — into a day that
 * never existed. The most recent observation of that weekday is a day that did.
 *
 * `observedDays` is the guard against reading an event as a schedule. "Crazy
 * Summer with Ross Antony & Paul Reeves" was seen exactly once, on 23 July;
 * projecting it onto every remaining Thursday of the year would invent a concert.
 * A caller (see `PlanDayService`) requires more than one sighting before it
 * projects at all, and passes both numbers on so the reader can weigh them.
 */
@Entity("show_schedule_patterns")
// The read: every pattern for one park's shows on one weekday.
@Index("idx_show_patterns_weekday", ["weekday"])
export class ShowSchedulePattern {
  @PrimaryColumn({ name: "show_id", type: "uuid" })
  showId: string;

  /**
   * Park-local day of week, **Postgres convention: 0 = Sunday**.
   *
   * Stated because the two conventions in this codebase disagree and the bug is
   * silent: `EXTRACT(DOW)` counts from Sunday, JavaScript's `getDay()` also
   * counts from Sunday, but pandas' `dayofweek` counts from Monday, and the ML
   * code converts between them. This column is written by SQL and read by
   * JavaScript, which agree — so nothing converts here, and that is the point of
   * saying so.
   */
  @PrimaryColumn({ name: "weekday", type: "smallint" })
  weekday: number;

  /**
   * The park-local start times of the most recent day this show ran on this
   * weekday, as `HH:mm`, ascending.
   *
   * Times of day rather than instants, because that is the part that repeats.
   * The date is supplied by whoever projects them.
   */
  @Column({ name: "times", type: "jsonb" })
  times: string[];

  /** Distinct days in the window that had this show on this weekday. */
  @Column({ name: "observed_days", type: "int" })
  observedDays: number;

  /**
   * The day `times` was actually taken from.
   *
   * Travels all the way to the API response: a projection that cannot say what
   * it was projected FROM is indistinguishable from a schedule, which is the
   * failure this whole design is arranged against.
   */
  @Column({ name: "last_observed_on", type: "date" })
  lastObservedOn: string;

  @Column({ name: "computed_at", type: "timestamptz" })
  computedAt: Date;
}
