import {
  TRIP_MAX_BYTES,
  checkTripPayload,
  tripPayloadBytes,
} from "./trip-payload.util";

/**
 * `POST /v1/trips` is the first unauthenticated write endpoint in this API.
 * What stops it being a file host with a good domain in front of it is entirely
 * in this one function, so the interesting cases are the ones a green build
 * cannot see: the payload that is valid JSON and not a plan, and the plan that
 * passes every structural check and is a megabyte.
 */
describe("checkTripPayload", () => {
  const plan = (over: Record<string, unknown> = {}) => ({
    version: 2,
    activeParkSlug: "phantasialand",
    activeDate: "2026-10-17",
    parks: {
      phantasialand: {
        slug: "phantasialand",
        name: "Phantasialand",
        geo: { continent: "europe", country: "germany", city: "bruehl" },
        timezone: "Europe/Berlin",
        days: {
          "2026-10-17": {
            date: "2026-10-17",
            entries: [
              {
                id: "taron-1",
                attractionSlug: "taron",
                attractionName: "Taron",
                startMinute: 600,
              },
            ],
          },
        },
      },
    },
    ...over,
  });

  it("accepts the plan the planner actually writes", () => {
    expect(checkTripPayload(plan()).ok).toBe(true);
  });

  it("accepts a park whose day is still empty", () => {
    // The planner writes a park the moment somebody opens a day, before
    // anything is in it — `openDay` adds no entry on purpose.
    const payload = plan();
    (
      payload.parks.phantasialand.days["2026-10-17"] as { entries: unknown[] }
    ).entries = [];
    expect(checkTripPayload(payload).ok).toBe(true);
  });

  it("accepts a park with no days at all", () => {
    const payload = plan();
    delete (payload.parks.phantasialand as Record<string, unknown>).days;
    expect(checkTripPayload(payload).ok).toBe(true);
  });

  it("accepts a free block, which carries no ride", () => {
    // A custom block has a label and an icon and no `attractionSlug` — an empty
    // slug would be a claim that a ride exists with no name.
    const payload = plan();
    (
      payload.parks.phantasialand.days["2026-10-17"] as { entries: unknown[] }
    ).entries = [
      {
        id: "lunch-1",
        startMinute: 720,
        custom: { label: "Mittagessen", icon: "food", durationMinutes: 60 },
      },
    ];
    expect(checkTripPayload(payload).ok).toBe(true);
  });

  it("passes through fields it does not know", () => {
    // This API does not own the planner's shape. Rejecting an unknown field
    // would break the planner on the day it grows one.
    const payload = plan();
    (
      payload.parks.phantasialand.days["2026-10-17"] as {
        entries: Array<Record<string, unknown>>;
      }
    ).entries[0].somethingAddedNextYear = { deep: [1, 2, 3] };
    expect(checkTripPayload(payload).ok).toBe(true);
  });

  describe("refuses what is not a plan", () => {
    it.each([
      ["null", null],
      ["a string", "hello"],
      ["a number", 42],
      ["an array", [1, 2, 3]],
    ])("%s", (_label, value) => {
      expect(checkTripPayload(value).ok).toBe(false);
    });

    it("an arbitrary JSON object — the file-host case", () => {
      const verdict = checkTripPayload({ hello: "world", nested: { a: 1 } });
      expect(verdict.ok).toBe(false);
      expect(verdict.reason).toBe("no version");
    });

    it("a plan-shaped object with no parks map", () => {
      expect(checkTripPayload({ version: 2 }).reason).toBe("no parks");
    });

    it("a park with no slug", () => {
      const payload = plan();
      delete (payload.parks.phantasialand as Record<string, unknown>).slug;
      expect(checkTripPayload(payload).ok).toBe(false);
    });

    it("a day with no date", () => {
      const payload = plan();
      delete (
        payload.parks.phantasialand.days["2026-10-17"] as Record<
          string,
          unknown
        >
      ).date;
      expect(checkTripPayload(payload).ok).toBe(false);
    });

    it("a day whose date is not a date", () => {
      const payload = plan();
      (
        payload.parks.phantasialand.days["2026-10-17"] as Record<
          string,
          unknown
        >
      ).date = "sometime in October";
      expect(checkTripPayload(payload).ok).toBe(false);
    });

    it("an entry with no id", () => {
      const payload = plan();
      delete (
        payload.parks.phantasialand.days["2026-10-17"] as {
          entries: Array<Record<string, unknown>>;
        }
      ).entries[0].id;
      expect(checkTripPayload(payload).ok).toBe(false);
    });

    it("an entry with no start", () => {
      const payload = plan();
      (
        payload.parks.phantasialand.days["2026-10-17"] as {
          entries: Array<Record<string, unknown>>;
        }
      ).entries[0].startMinute = "morning";
      expect(checkTripPayload(payload).ok).toBe(false);
    });
  });

  describe("refuses what is too big to be a plan", () => {
    it("a payload over the byte cap", () => {
      const payload = plan({ padding: "x".repeat(TRIP_MAX_BYTES) });
      const verdict = checkTripPayload(payload);
      expect(verdict.ok).toBe(false);
      expect(verdict.reason).toMatch(/too large/);
    });

    it("a structurally valid plan with absurdly many parks", () => {
      // Every check below the counts would pass. The counts are the only thing
      // between this and a megabyte of nested nonsense that is also a plan.
      const parks: Record<string, unknown> = {};
      for (let i = 0; i < 500; i++) {
        parks[`p${i}`] = { slug: `p${i}`, days: {} };
      }
      expect(checkTripPayload({ version: 2, parks }).reason).toBe(
        "too many parks",
      );
    });

    it("a day with absurdly many entries", () => {
      const payload = plan();
      (
        payload.parks.phantasialand.days["2026-10-17"] as { entries: unknown[] }
      ).entries = Array.from({ length: 500 }, (_, i) => ({
        id: `e${i}`,
        startMinute: 600,
      }));
      expect(checkTripPayload(payload).reason).toMatch(/too many entries/);
    });
  });

  it("never echoes the payload back in the reason", () => {
    // The reason reaches the caller in a 400. Reflecting the body would make
    // this endpoint useful for something other than storing plans.
    const verdict = checkTripPayload({ version: 2, parks: { p: "<script>" } });
    expect(verdict.ok).toBe(false);
    expect(verdict.reason).not.toMatch(/<script>/);
  });

  it("measures bytes of UTF-8, not characters", () => {
    // "Phantasialand" is ASCII and a park name is not always. A cap counted in
    // characters is a third too generous for a plan written in Japanese.
    expect(tripPayloadBytes({ a: "ä" })).toBe(tripPayloadBytes({ a: "aa" }));
  });
});
