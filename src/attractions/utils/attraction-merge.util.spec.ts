import {
  resolveSurvivingName,
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
    // A real duplicate is two SOURCES describing one ride, so the default pair
    // is cross-source: row() is the wiki row, row({id:"b"}) the Queue-Times
    // one. Tests that care about same-source pairs set externalId explicitly.
    externalId:
      over.id === "b" ? "qt-ride-1" : "11111111-1111-1111-1111-111111111111",
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

  it("no longer takes a shared queue-times id as proof on its own", () => {
    // Carowinds is why. "Blackbeard's Revenge - Cannonball Drop & Captain's
    // Curse" and "Blackbeard's Revenge - Pirate's Plank" both carry id 14744,
    // and their slugs say tube-slides and drop-slides: two slide complexes the
    // upstream lumped under one id. Auto-merging them destroys one. Kings
    // Island (two stations of a railroad) and Six Flags Great Escape (three
    // Sasquatch rows) have the same shape.
    expect(
      isSafeToAutoMerge(
        row({ name: "Riptide Racer", queueTimesEntityId: "77" }),
        row({ id: "b", name: "Riptide", queueTimesEntityId: "77" }),
      ),
    ).toBe(false);
  });

  it("sees through the map number Queue-Times puts in some ride names", () => {
    // Energylandia's feed says "Draken (155)" where the wiki says "Draken" —
    // the number is that park's own map label, carried through by one source
    // and not the other. Eight duplicate pairs in that park hang on this.
    expect(
      isSafeToAutoMerge(
        row({ name: "Draken" }),
        row({ id: "b", name: "Draken (155)" }),
      ),
    ).toBe(true);
    expect(
      isSafeToAutoMerge(
        row({ name: "Frutti Loop" }),
        row({ id: "b", name: "Frutti Loop (39)" }),
      ),
    ).toBe(true);
  });

  it("strips the map number without swallowing a genuinely numbered name", () => {
    // "Spindeln - Nyhet 2026" is a real ride name with a year in it, not a
    // map label in brackets — it must not collapse onto "Spindeln - Nyhet".
    expect(
      isSafeToAutoMerge(
        row({ name: "Spindeln - Nyhet" }),
        row({ id: "b", name: "Spindeln - Nyhet 2026" }),
      ),
    ).toBe(false);
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
    externalId: "44444444-4444-4444-4444-444444444444",
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

describe("resolveSurvivingSlug — slugs that do not derive from each other", () => {
  it("takes the slug the surviving name produces", () => {
    // Energylandia's duplicates are found by a shared Queue-Times id, and their
    // slugs share no stem. The winner is whichever row ingestion still feeds,
    // and its slug can be an unrelated leftover — without this the merge would
    // have published "Choco Chip Creek" at /main-train-2.
    expect(
      resolveSurvivingSlug(
        "main-train-2",
        "choco-chip-creek",
        "Choco Chip Creek",
      ),
    ).toBe("choco-chip-creek");
    expect(
      resolveSurvivingSlug(
        "lolipop-farm",
        "mini-track-tour-ride",
        "Mini Track Tour Ride",
      ),
    ).toBe("mini-track-tour-ride");
  });

  it("keeps the winner's slug when it already matches the name", () => {
    expect(resolveSurvivingSlug("draken-rc", "draken", "Draken")).toBe(
      "draken",
    );
    expect(resolveSurvivingSlug("draken", "draken-rc", "Draken")).toBe(
      "draken",
    );
  });

  it("changes nothing when no name is supplied", () => {
    // The base/suffix rule is unaffected: existing callers keep their behaviour.
    expect(resolveSurvivingSlug("taron-2", "taron")).toBe("taron");
    expect(resolveSurvivingSlug("main-train-2", "choco-chip-creek")).toBe(
      "main-train-2",
    );
  });
});

describe("resolveSurvivingName", () => {
  it("drops the map number, whichever row it came from", () => {
    // The Queue-Times row usually wins a merge — it is the one ingestion still
    // feeds — so without this every Energylandia ride would settle on its
    // numbered spelling.
    expect(resolveSurvivingName("Abyssus (184)", "Abyssus")).toBe("Abyssus");
    expect(resolveSurvivingName("Abyssus", "Abyssus (184)")).toBe("Abyssus");
    expect(resolveSurvivingName("Draken (155)", "Draken")).toBe("Draken");
  });

  it("leaves genuinely different names to the winner", () => {
    // Not the place to arbitrate between two real names.
    expect(resolveSurvivingName("Kiddy Hawk", "Kiddy Hawk Cove")).toBe(
      "Kiddy Hawk",
    );
    expect(resolveSurvivingName("Main Train", "Choco Chip Creek")).toBe(
      "Main Train",
    );
  });

  it("keeps a year that is part of the name", () => {
    // "Spindeln - Nyhet 2026" has no bracket, so nothing is stripped.
    expect(
      resolveSurvivingName("Spindeln - Nyhet 2026", "Spindeln - Nyhet"),
    ).toBe("Spindeln - Nyhet 2026");
  });
});

describe("map numbers versus years", () => {
  const row = (over: Partial<DuplicateCandidate> = {}): DuplicateCandidate => ({
    id: "a",
    slug: "x",
    name: "Alice in Wonderland",
    // A real duplicate is two SOURCES describing one ride, so the default pair
    // is cross-source: row() is the wiki row, row({id:"b"}) the Queue-Times
    // one. Tests that care about same-source pairs set externalId explicitly.
    externalId:
      over.id === "b" ? "qt-ride-1" : "11111111-1111-1111-1111-111111111111",
    queueTimesEntityId: null,
    hasCoordinates: false,
    recentQueueRows: 0,
    totalQueueRows: 0,
    createdAt: new Date("2026-01-01"),
    ...over,
  });

  it("does not collapse two editions of a seasonal maze", () => {
    // Six Flags Great America runs "HAUNTED HOUSE: Texas Chainsaw Massacre
    // (2022)". A four-digit parenthetical is a year, and a 2022 maze is not
    // the 2023 one — merging them would erase a distinct event attraction.
    expect(
      isSafeToAutoMerge(
        row({ name: "HAUNTED HOUSE: Texas Chainsaw Massacre" }),
        row({ id: "b", name: "HAUNTED HOUSE: Texas Chainsaw Massacre (2022)" }),
      ),
    ).toBe(false);
    expect(resolveSurvivingName("Maze (2022)", "Maze")).toBe("Maze (2022)");
  });

  it("still strips the two- and three-digit map numbers", () => {
    // Every real one in the data sits between 23 and 224.
    expect(resolveSurvivingName("Energuś (23)", "Energuś")).toBe("Energuś");
    expect(resolveSurvivingName("Honey Harbor (224)", "Honey Harbor")).toBe(
      "Honey Harbor",
    );
  });
});

describe("two ids from one source are not a duplicate", () => {
  const row = (over: Partial<DuplicateCandidate> = {}): DuplicateCandidate => ({
    id: "a",
    slug: "x",
    name: "PLAYGROUND",
    externalId: "11111111-1111-1111-1111-111111111111",
    queueTimesEntityId: null,
    hasCoordinates: false,
    recentQueueRows: 0,
    totalQueueRows: 0,
    createdAt: new Date("2026-01-01"),
    ...over,
  });

  it("refuses rows whose ids both come from the wiki", () => {
    // Heide Park: ThemeParks.wiki publishes THREE separate attraction entities
    // all called "PLAYGROUND". Their names agree perfectly, so name matching
    // alone offered playground-2 and playground-3 as safe auto-merges — which
    // would collapse three real play areas into one.
    expect(
      isSafeToAutoMerge(
        row(),
        row({ id: "b", externalId: "22222222-2222-2222-2222-222222222222" }),
      ),
    ).toBe(false);
  });

  it("refuses rows whose ids both come from Queue-Times", () => {
    expect(
      isSafeToAutoMerge(
        row({ externalId: "qt-ride-1" }),
        row({ id: "b", externalId: "qt-ride-2" }),
      ),
    ).toBe(false);
  });

  it("still accepts one row per source, which is what a duplicate is", () => {
    expect(
      isSafeToAutoMerge(
        row({ externalId: "33333333-3333-3333-3333-333333333333" }),
        row({ id: "b", externalId: "qt-ride-9" }),
      ),
    ).toBe(true);
  });
});
