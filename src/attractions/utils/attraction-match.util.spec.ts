import { findExistingAttraction } from "./attraction-match.util";

/**
 * Every attraction sync path used to look up existing rows by `externalId`
 * alone. `externalId` is source-scoped ("qt-ride-12979" from Queue-Times,
 * a UUID from ThemeParks.wiki), so a ride that two sources both report got
 * two rows — the second one taking a "-2" slug. That produced 147 duplicate
 * pairs in production, still growing, and the same defect at park level
 * produced two duplicate parks.
 *
 * These fixtures are the real Blackpool Pleasure Beach rows.
 */
describe("findExistingAttraction", () => {
  const existingFromQueueTimes = {
    id: "row-qt",
    externalId: "qt-ride-12979",
    slug: "alice-in-wonderland",
    name: "Alice in Wonderland",
    queueTimesEntityId: "12979",
  };

  const incomingFromWiki = {
    externalId: "59f971c2-f3fa-4ca4-9b92-6c3a097d5b61",
    name: "Alice in Wonderland",
    queueTimesEntityId: "12979",
  };

  it("matches on externalId when the same source syncs again", () => {
    const match = findExistingAttraction(
      {
        externalId: "qt-ride-12979",
        name: "Alice in Wonderland",
        queueTimesEntityId: "12979",
      },
      [existingFromQueueTimes],
    );

    expect(match).toBe(existingFromQueueTimes);
  });

  it("matches the wiki row onto the existing queue-times row via the shared queue-times ID", () => {
    const match = findExistingAttraction(incomingFromWiki, [
      existingFromQueueTimes,
    ]);

    expect(match).toBe(existingFromQueueTimes);
  });

  it("matches on name when no external ID lines up", () => {
    const match = findExistingAttraction(
      { externalId: "wiki-uuid", name: "Alice in Wonderland" },
      [{ ...existingFromQueueTimes, queueTimesEntityId: null }],
    );

    expect(match?.id).toBe("row-qt");
  });

  it("ignores case, punctuation and accents when matching names", () => {
    const existing = {
      id: "row-1",
      externalId: "qt-ride-1",
      slug: "spider-man",
      name: "Spider-Man®",
      queueTimesEntityId: null,
    };

    const match = findExistingAttraction(
      { externalId: "wiki-uuid", name: "spider man" },
      [existing],
    );

    expect(match).toBe(existing);
  });

  it("returns null for a genuinely new ride", () => {
    const match = findExistingAttraction(
      { externalId: "wiki-uuid", name: "Valhalla" },
      [existingFromQueueTimes],
    );

    expect(match).toBeNull();
  });

  it("does not collapse distinct rides that share a name", () => {
    // Wet'n'Wild really has five separate restrooms. Name matching must not
    // merge the second incoming one onto the first existing row when the
    // incoming row already has its own externalId match elsewhere.
    const restroomA = {
      id: "row-a",
      externalId: "wiki-a",
      slug: "restroom",
      name: "Restroom",
      queueTimesEntityId: null,
    };
    const restroomB = {
      id: "row-b",
      externalId: "wiki-b",
      slug: "restroom-2",
      name: "Restroom",
      queueTimesEntityId: null,
    };

    const match = findExistingAttraction(
      { externalId: "wiki-b", name: "Restroom" },
      [restroomA, restroomB],
    );

    expect(match).toBe(restroomB);
  });

  it("prefers the queue-times ID over a name collision", () => {
    const wrongNameMatch = {
      id: "row-name",
      externalId: "wiki-x",
      slug: "the-ride",
      name: "The Ride",
      queueTimesEntityId: null,
    };
    const rightIdMatch = {
      id: "row-id",
      externalId: "qt-ride-99",
      slug: "the-ride-renamed",
      name: "The Ride (renamed upstream)",
      queueTimesEntityId: "99",
    };

    const match = findExistingAttraction(
      { externalId: "wiki-y", name: "The Ride", queueTimesEntityId: "99" },
      [wrongNameMatch, rightIdMatch],
    );

    expect(match).toBe(rightIdMatch);
  });

  it("does not match on a null queue-times ID", () => {
    const match = findExistingAttraction(
      { externalId: "wiki-uuid", name: "Brand New Ride" },
      [{ ...existingFromQueueTimes, queueTimesEntityId: null }],
    );

    expect(match).toBeNull();
  });
});
