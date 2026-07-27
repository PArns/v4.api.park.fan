import {
  resolveSurvivingSlug,
  isSafeToAutoMerge,
  chooseDuplicateWinner,
  DuplicateCandidate,
} from "./attraction-merge.util";

/**
 * When two rows for the same ride are merged, the surviving row must carry the
 * slug Google already indexed — the base one. `generateUniqueSlug` starts
 * counting at 2, so "alice-in-wonderland-2" is the accidental second row and
 * "alice-in-wonderland" is the URL in the sitemap.
 *
 * There is no alias table for attractions (only `park_slug_aliases`), so a
 * surviving slug that is not the base one simply 404s with no redirect.
 */
describe("resolveSurvivingSlug", () => {
  it("takes the base slug when the winner is the suffixed row", () => {
    expect(
      resolveSurvivingSlug("alice-in-wonderland-2", "alice-in-wonderland"),
    ).toBe("alice-in-wonderland");
  });

  it("keeps the base slug when the winner already has it", () => {
    expect(
      resolveSurvivingSlug("alice-in-wonderland", "alice-in-wonderland-2"),
    ).toBe("alice-in-wonderland");
  });

  it("handles suffixes beyond -2", () => {
    expect(resolveSurvivingSlug("restroom-5", "restroom")).toBe("restroom");
  });

  it("keeps the winner's slug when neither is the other's base", () => {
    // base "x" may still exist as a third row — we cannot claim it blindly.
    expect(resolveSurvivingSlug("mega-wedgie-2", "mega-wedgie-3")).toBe(
      "mega-wedgie-2",
    );
  });

  it("does not strip digits that are part of the real name", () => {
    // "PEANUTS™ 500" and "Route 66" slug to peanuts-500 / route-66 and are not
    // suffixed duplicates of anything.
    expect(resolveSurvivingSlug("peanuts-500", "peanuts-500-2")).toBe(
      "peanuts-500",
    );
    expect(resolveSurvivingSlug("route-66-2", "route-66")).toBe("route-66");
  });

  it("keeps the winner's slug when the two are unrelated", () => {
    expect(resolveSurvivingSlug("valhalla", "infusion")).toBe("valhalla");
  });
});

describe("isSafeToAutoMerge", () => {
  const row = (over: Partial<DuplicateCandidate> = {}): DuplicateCandidate => ({
    id: "a",
    slug: "x",
    name: "Alice in Wonderland",
    queueTimesEntityId: null,
    hasCoordinates: false,
    recentQueueRows: 0,
    totalQueueRows: 0,
    createdAt: new Date("2026-01-01"),
    ...over,
  });

  it("accepts rows with the same name", () => {
    expect(isSafeToAutoMerge(row(), row({ id: "b" }))).toBe(true);
  });

  it("accepts names that differ only in punctuation or case", () => {
    expect(
      isSafeToAutoMerge(
        row({ name: "Dolly's Home-On-Wheels" }),
        row({ id: "b", name: "Dollys Home On Wheels" }),
      ),
    ).toBe(true);
  });

  it("accepts differing names when the queue-times id is the same ride", () => {
    expect(
      isSafeToAutoMerge(
        row({ name: "Riptide Racer", queueTimesEntityId: "77" }),
        row({ id: "b", name: "Riptide", queueTimesEntityId: "77" }),
      ),
    ).toBe(true);
  });

  it("refuses two different rides that merely collided on a slug", () => {
    // Real pair: main-train / main-train-2 hold "Main Train" and
    // "Choco Chip Creek (215)". Merging them would destroy a real ride.
    expect(
      isSafeToAutoMerge(
        row({ name: "Main Train" }),
        row({ id: "b", name: "Choco Chip Creek (215)" }),
      ),
    ).toBe(false);
  });

  it("refuses a seasonal overlay that is not the base ride", () => {
    expect(
      isSafeToAutoMerge(
        row({ name: "Power Builder" }),
        row({
          id: "b",
          name: "Power Builder Halloween Special: Monster Trail",
        }),
      ),
    ).toBe(false);
  });

  it("does not treat two missing queue-times ids as a match", () => {
    expect(
      isSafeToAutoMerge(
        row({ name: "Spindeln - Nyhet" }),
        row({ id: "b", name: "Spindeln - Nyhet 2026" }),
      ),
    ).toBe(false);
  });
});

describe("chooseDuplicateWinner", () => {
  const row = (over: Partial<DuplicateCandidate> = {}): DuplicateCandidate => ({
    id: "a",
    slug: "x",
    name: "Ride",
    queueTimesEntityId: null,
    hasCoordinates: false,
    recentQueueRows: 0,
    totalQueueRows: 0,
    createdAt: new Date("2026-01-01"),
    ...over,
  });

  it("keeps the row live ingestion is still feeding", () => {
    const active = row({ id: "active", recentQueueRows: 400 });
    const stale = row({
      id: "stale",
      recentQueueRows: 0,
      totalQueueRows: 9000,
    });

    expect(chooseDuplicateWinner(stale, active).winnerId).toBe("active");
  });

  it("prefers the longer history when both are equally live", () => {
    const long = row({
      id: "long",
      recentQueueRows: 100,
      totalQueueRows: 9000,
    });
    const short = row({
      id: "short",
      recentQueueRows: 100,
      totalQueueRows: 50,
    });

    expect(chooseDuplicateWinner(short, long).winnerId).toBe("long");
  });

  it("falls back to the older row when nothing else separates them", () => {
    const older = row({ id: "older", createdAt: new Date("2025-12-24") });
    const newer = row({ id: "newer", createdAt: new Date("2026-04-26") });

    expect(chooseDuplicateWinner(newer, older).winnerId).toBe("older");
  });

  it("always reports the other row as the loser", () => {
    const a = row({ id: "a", recentQueueRows: 5 });
    const b = row({ id: "b", recentQueueRows: 1 });

    expect(chooseDuplicateWinner(a, b)).toEqual({
      winnerId: "a",
      loserId: "b",
    });
  });
});
