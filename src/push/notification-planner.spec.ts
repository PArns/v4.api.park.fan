import {
  LEAD_MAX_MIN,
  LEAD_MIN,
  dueNotifications,
} from "./notification-planner";

/**
 * This is the function that decides whether somebody's phone buzzes, and the
 * two ways it can be wrong are both invisible in a running system: it can wake
 * a person at the wrong hour, and it can wake them about something that already
 * happened. Neither shows up in a log, because a notification that should not
 * have gone out looks exactly like one that should.
 *
 * The timezone cases carry the most weight. A plan holding Phantasialand and
 * Magic Kingdom is on two different DATES at 23:00 in Berlin, and a job
 * reckoning in UTC would notify about the wrong day for one of them every
 * evening.
 */
describe("dueNotifications", () => {
  /** 2026-10-17, 09:45 in Berlin (UTC+2). */
  const BERLIN_0945 = Date.parse("2026-10-17T07:45:00.000Z");

  const plan = (
    over: {
      timezone?: string | null;
      date?: string;
      entries?: unknown[];
    } = {},
  ) => ({
    version: 2,
    parks: {
      phantasialand: {
        slug: "phantasialand",
        name: "Phantasialand",
        timezone: over.timezone === undefined ? "Europe/Berlin" : over.timezone,
        days: {
          [over.date ?? "2026-10-17"]: {
            date: over.date ?? "2026-10-17",
            entries: over.entries ?? [
              {
                id: "taron-1",
                attractionSlug: "taron",
                attractionName: "Taron",
                startMinute: 600, // 10:00 — fifteen minutes out
              },
            ],
          },
        },
      },
    },
  });

  it("notifies about a block starting inside the window", () => {
    const due = dueNotifications(plan(), BERLIN_0945);
    expect(due).toHaveLength(1);
    expect(due[0]).toMatchObject({
      topic: "next-up",
      what: "Taron",
      parkName: "Phantasialand",
      inMinutes: 15,
      atTime: "10:00",
    });
  });

  it("stays quiet before the window opens", () => {
    // 10:00 is 45 minutes out. A banner now is an interruption about something
    // the visitor cannot act on yet.
    const due = dueNotifications(
      plan(),
      Date.parse("2026-10-17T07:15:00.000Z"),
    );
    expect(due).toEqual([]);
  });

  it("stays quiet once the block has started", () => {
    // The window is AHEAD of the start. A block whose time has passed is behind
    // us, and a banner about it is an interruption about nothing.
    const due = dueNotifications(
      plan(),
      Date.parse("2026-10-17T08:05:00.000Z"),
    );
    expect(due).toEqual([]);
  });

  it("has a window wider than the job's tick", () => {
    // The job runs every five minutes. At exactly one tick's width a single
    // missed run drops the notification and nothing ever retries it.
    expect(LEAD_MAX_MIN - LEAD_MIN).toBeGreaterThan(5);
  });

  it("says nothing about a block already ticked off", () => {
    const due = dueNotifications(
      plan({
        entries: [
          {
            id: "taron-1",
            attractionName: "Taron",
            startMinute: 600,
            done: true,
          },
        ],
      }),
      BERLIN_0945,
    );
    expect(due).toEqual([]);
  });

  it("names a free block by its own label", () => {
    const due = dueNotifications(
      plan({
        entries: [
          {
            id: "lunch-1",
            startMinute: 600,
            custom: { label: "Mittagessen", icon: "food", durationMinutes: 60 },
          },
        ],
      }),
      BERLIN_0945,
    );
    expect(due[0]?.what).toBe("Mittagessen");
  });

  it("says nothing it cannot name", () => {
    // "Als Nächstes:" with an empty subject is worse than silence.
    const due = dueNotifications(
      plan({ entries: [{ id: "x", startMinute: 600 }] }),
      BERLIN_0945,
    );
    expect(due).toEqual([]);
  });

  describe("reckons in the park's timezone", () => {
    it("uses the park's clock, not the server's", () => {
      // Same instant, a park in New York. 07:45 UTC is 03:45 there, so a block
      // at 10:00 park-local is six hours out and must not notify.
      const due = dueNotifications(
        plan({ timezone: "America/New_York" }),
        BERLIN_0945,
      );
      expect(due).toEqual([]);
    });

    it("uses the park's DATE, not UTC's", () => {
      // 23:50 in Tokyo on the 17th is 14:50 UTC on the 17th — same date by luck.
      // 08:10 in Tokyo on the 18th is 23:10 UTC on the 17th, and THAT is the
      // case that breaks a UTC job: it would look for the 17th's plan and find
      // the wrong day.
      const tokyo = {
        version: 2,
        parks: {
          p: {
            slug: "p",
            name: "A park in Tokyo",
            timezone: "Asia/Tokyo",
            days: {
              "2026-10-18": {
                date: "2026-10-18",
                entries: [
                  {
                    id: "e1",
                    attractionName: "Ride",
                    startMinute: 8 * 60 + 25,
                  },
                ],
              },
            },
          },
        },
      };
      // 23:10 UTC on the 17th = 08:10 on the 18th in Tokyo, fifteen minutes
      // before 08:25.
      const due = dueNotifications(
        tokyo,
        Date.parse("2026-10-17T23:10:00.000Z"),
      );
      expect(due).toHaveLength(1);
      expect(due[0].atTime).toBe("08:25");
    });

    it("says nothing for a park whose zone it does not know", () => {
      // The alternative is reckoning in UTC or in the server's zone, and this is
      // the one place in the planner where being a few hours out wakes somebody.
      expect(dueNotifications(plan({ timezone: null }), BERLIN_0945)).toEqual(
        [],
      );
      expect(
        dueNotifications(plan({ timezone: "Mars/Olympus_Mons" }), BERLIN_0945),
      ).toEqual([]);
    });
  });

  describe("survives a payload it did not write", () => {
    // The plan is stored verbatim from a browser and checked only against a
    // skeleton. A job that throws on one malformed trip stops notifying
    // everybody.
    it.each([
      ["null", null],
      ["a string", "hello"],
      ["an array", [1, 2]],
      ["no parks", { version: 2 }],
      ["a park that is a string", { version: 2, parks: { p: "nope" } }],
      [
        "a day that is a number",
        {
          version: 2,
          parks: {
            p: { slug: "p", timezone: "Europe/Berlin", days: { d: 5 } },
          },
        },
      ],
      [
        "entries that are not a list",
        {
          version: 2,
          parks: {
            p: {
              slug: "p",
              timezone: "Europe/Berlin",
              days: { d: { entries: "x" } },
            },
          },
        },
      ],
    ])("%s", (_label, payload) => {
      expect(() => dueNotifications(payload, BERLIN_0945)).not.toThrow();
      expect(dueNotifications(payload, BERLIN_0945)).toEqual([]);
    });

    it("skips a malformed entry and keeps the good one beside it", () => {
      const due = dueNotifications(
        plan({
          entries: [
            { id: 42, startMinute: 600, attractionName: "Broken" },
            { id: "ok-1", startMinute: 600, attractionName: "Taron" },
          ],
        }),
        BERLIN_0945,
      );
      expect(due.map((d) => d.what)).toEqual(["Taron"]);
    });
  });

  it("keys each event to its own block AND its start", () => {
    // Moving a block genuinely is a new event; re-running the job is not. The
    // dedupe key is what stops the overlapping window sending twice.
    const a = dueNotifications(plan(), BERLIN_0945)[0];
    const b = dueNotifications(plan(), BERLIN_0945 + 60_000)[0];
    expect(a.dedupeKey).toBe(b.dedupeKey);

    const moved = dueNotifications(
      plan({
        entries: [{ id: "taron-1", attractionName: "Taron", startMinute: 601 }],
      }),
      BERLIN_0945,
    )[0];
    expect(moved.dedupeKey).not.toBe(a.dedupeKey);
  });

  it("puts the nearest block first", () => {
    const due = dueNotifications(
      plan({
        entries: [
          { id: "later", attractionName: "Later", startMinute: 604 },
          { id: "sooner", attractionName: "Sooner", startMinute: 596 },
        ],
      }),
      BERLIN_0945,
    );
    expect(due.map((d) => d.what)).toEqual(["Sooner", "Later"]);
  });
});
