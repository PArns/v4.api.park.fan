import {
  SHOW_LATE_LEAD_MAX_MIN,
  SHOW_LATE_LEAD_MIN,
  SHOW_LEAD_MAX_MIN,
  SHOW_LEAD_MIN,
  dueShowNotifications,
  type FollowedShowStatus,
} from "./show-follow-notifications";

/**
 * `startTime` here is always a full ISO instant, already day-projected by
 * `ShowsService` — the property worth pinning is that this function needs no
 * timezone math to decide whether something is due (two absolute instants
 * subtract cleanly), and only reads the zone to format the display clock. A
 * park with no or unreadable timezone is skipped rather than defaulted to
 * UTC, the same rule `dueNotifications` follows for the trip planner.
 */
describe("dueShowNotifications", () => {
  // 2026-10-17, 18:00 UTC = 20:00 in Berlin (still CEST — DST ends 2026-10-25).
  const NOW = Date.parse("2026-10-17T18:00:00.000Z");
  const minutesFromNow = (minutes: number) =>
    new Date(NOW + minutes * 60_000).toISOString();

  const show = (
    over: Partial<FollowedShowStatus> = {},
  ): FollowedShowStatus => ({
    showId: "show-1",
    showName: "Feuerwerk",
    parkName: "Europa-Park",
    timezone: "Europe/Berlin",
    url: "/parks/europe/germany/rust/europa-park#shows",
    showtimes: [{ startTime: minutesFromNow(30) }],
    ...over,
  });

  it("notifies about a showtime inside the window", () => {
    const due = dueShowNotifications([show()], NOW);
    expect(due).toHaveLength(1);
    expect(due[0]).toMatchObject({
      showId: "show-1",
      what: "Feuerwerk",
      parkName: "Europa-Park",
      inMinutes: 30,
      atTime: "20:30",
      url: "/parks/europe/germany/rust/europa-park#shows",
    });
    expect(due[0].dedupeKey).toContain("show-1");
  });

  it("includes the near edge of the window (25 minutes)", () => {
    const due = dueShowNotifications(
      [show({ showtimes: [{ startTime: minutesFromNow(SHOW_LEAD_MIN) }] })],
      NOW,
    );
    expect(due).toHaveLength(1);
  });

  it("includes the far edge of the window (35 minutes)", () => {
    const due = dueShowNotifications(
      [
        show({
          showtimes: [{ startTime: minutesFromNow(SHOW_LEAD_MAX_MIN) }],
        }),
      ],
      NOW,
    );
    expect(due).toHaveLength(1);
  });

  it("does not notify one minute before the window opens", () => {
    const due = dueShowNotifications(
      [
        show({
          showtimes: [{ startTime: minutesFromNow(SHOW_LEAD_MIN - 1) }],
        }),
      ],
      NOW,
    );
    expect(due).toHaveLength(0);
  });

  it("does not notify one minute after the window closes", () => {
    const due = dueShowNotifications(
      [
        show({
          showtimes: [{ startTime: minutesFromNow(SHOW_LEAD_MAX_MIN + 1) }],
        }),
      ],
      NOW,
    );
    expect(due).toHaveLength(0);
  });

  it("does not notify about a showtime that has already passed", () => {
    const due = dueShowNotifications(
      [show({ showtimes: [{ startTime: minutesFromNow(-5) }] })],
      NOW,
    );
    expect(due).toHaveLength(0);
  });

  it("skips a show with no timezone rather than guessing UTC", () => {
    const due = dueShowNotifications([show({ timezone: null })], NOW);
    expect(due).toHaveLength(0);
  });

  it("skips a show whose timezone this deploy cannot format", () => {
    const due = dueShowNotifications([show({ timezone: "Not/AZone" })], NOW);
    expect(due).toHaveLength(0);
  });

  it("skips a malformed startTime rather than throwing", () => {
    expect(() =>
      dueShowNotifications(
        [show({ showtimes: [{ startTime: "not-a-date" }] })],
        NOW,
      ),
    ).not.toThrow();
    expect(
      dueShowNotifications(
        [show({ showtimes: [{ startTime: "not-a-date" }] })],
        NOW,
      ),
    ).toHaveLength(0);
  });

  it("evaluates every showtime of a show independently", () => {
    const due = dueShowNotifications(
      [
        show({
          showtimes: [
            { startTime: minutesFromNow(30) }, // due
            { startTime: minutesFromNow(120) }, // not due yet
          ],
        }),
      ],
      NOW,
    );
    expect(due).toHaveLength(1);
    expect(due[0].inMinutes).toBe(30);
  });

  it("gives two due showtimes of the same show distinct dedupe keys", () => {
    const due = dueShowNotifications(
      [
        show({
          showtimes: [
            { startTime: minutesFromNow(25) },
            { startTime: minutesFromNow(35) },
          ],
        }),
      ],
      NOW,
    );
    expect(due).toHaveLength(2);
    expect(due[0].dedupeKey).not.toBe(due[1].dedupeKey);
  });

  it("sorts multiple due shows soonest-first", () => {
    const due = dueShowNotifications(
      [
        show({
          showId: "later",
          showName: "Later show",
          showtimes: [{ startTime: minutesFromNow(35) }],
        }),
        show({
          showId: "sooner",
          showName: "Sooner show",
          showtimes: [{ startTime: minutesFromNow(25) }],
        }),
      ],
      NOW,
    );
    expect(due.map((d) => d.showId)).toEqual(["sooner", "later"]);
  });

  it("answers an empty list for no followed shows", () => {
    expect(dueShowNotifications([], NOW)).toEqual([]);
  });

  /**
   * The second window. Somebody who taps the bell twenty minutes before a
   * performance is already past the first one, and used to get nothing at
   * all for that performance. Whether the SAME follower is spared a second
   * banner is not decided here but by the dedupe key, which is identical in
   * both windows — the property pinned below.
   */
  describe("the late window", () => {
    it("notifies about a showtime too close for the first window", () => {
      const due = dueShowNotifications(
        [show({ showtimes: [{ startTime: minutesFromNow(10) }] })],
        NOW,
      );
      expect(due).toHaveLength(1);
      expect(due[0]).toMatchObject({ inMinutes: 10, atTime: "20:10" });
    });

    it("includes both edges", () => {
      for (const lead of [SHOW_LATE_LEAD_MIN, SHOW_LATE_LEAD_MAX_MIN]) {
        expect(
          dueShowNotifications(
            [show({ showtimes: [{ startTime: minutesFromNow(lead) }] })],
            NOW,
          ),
        ).toHaveLength(1);
      }
    });

    it("excludes both sides of it", () => {
      for (const lead of [SHOW_LATE_LEAD_MIN - 1, SHOW_LATE_LEAD_MAX_MIN + 1]) {
        expect(
          dueShowNotifications(
            [show({ showtimes: [{ startTime: minutesFromNow(lead) }] })],
            NOW,
          ),
        ).toHaveLength(0);
      }
    });

    it("leaves the gap between the two windows quiet", () => {
      const due = dueShowNotifications(
        [show({ showtimes: [{ startTime: minutesFromNow(20) }] })],
        NOW,
      );
      expect(due).toHaveLength(0);
    });

    it("carries the same dedupe key as the first window, so a follower already told stays quiet", () => {
      const startTime = new Date(NOW + 40 * 60_000).toISOString();
      const earlyTick = dueShowNotifications(
        [show({ showtimes: [{ startTime }] })],
        NOW + 10 * 60_000, // 30 minutes ahead — the first window
      );
      const lateTick = dueShowNotifications(
        [show({ showtimes: [{ startTime }] })],
        NOW + 30 * 60_000, // 10 minutes ahead — the second
      );
      expect(earlyTick).toHaveLength(1);
      expect(lateTick).toHaveLength(1);
      expect(lateTick[0].dedupeKey).toBe(earlyTick[0].dedupeKey);
    });
  });
});
