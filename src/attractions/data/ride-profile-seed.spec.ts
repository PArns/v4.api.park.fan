import { RIDE_PROFILE_SEED } from "./ride-profile-seed";
import { GLOSSARY_TERM_IDS } from "./glossary-term-ids";

/**
 * The seed stores glossary term ids that live in a different repository
 * (park.fan → `lib/glossary/data.ts`). Nothing at runtime can tell us a term
 * is gone: the ride page just silently drops it and the layout reads short.
 * So the allowlist is mirrored here and checked, and a typo fails CI.
 */
describe("RIDE_PROFILE_SEED", () => {
  const known = new Set<string>(GLOSSARY_TERM_IDS);

  it("only references glossary terms that exist", () => {
    const unknown = new Set<string>();

    for (const entry of RIDE_PROFILE_SEED) {
      for (const id of entry.elements ?? []) {
        if (!known.has(id)) unknown.add(`${entry.attractionSlug}: ${id}`);
      }
      for (const id of entry.types ?? []) {
        if (!known.has(id)) unknown.add(`${entry.attractionSlug}: ${id}`);
      }
      if (entry.manufacturerTermId && !known.has(entry.manufacturerTermId)) {
        unknown.add(`${entry.attractionSlug}: ${entry.manufacturerTermId}`);
      }
    }

    expect([...unknown]).toEqual([]);
  });

  it("keys every entry by city, park and attraction", () => {
    for (const entry of RIDE_PROFILE_SEED) {
      expect(entry.citySlug).toBeTruthy();
      expect(entry.parkSlug).toBeTruthy();
      expect(entry.attractionSlug).toBeTruthy();
    }
  });

  it("has no duplicate ride keys", () => {
    const seen = new Set<string>();
    const dupes: string[] = [];

    for (const entry of RIDE_PROFILE_SEED) {
      const key = `${entry.citySlug}/${entry.parkSlug}/${entry.attractionSlug}`;
      if (seen.has(key)) dupes.push(key);
      seen.add(key);
    }

    // Repeated *elements* within one ride are intentional (a layout that hits
    // two corkscrews lists it twice). A repeated ride key is not — the second
    // entry would silently overwrite the first.
    expect(dupes).toEqual([]);
  });

  it("gives every coaster with elements a lift, a launch or a drop to start", () => {
    // A layout that starts mid-air means the element list was truncated.
    const starters = new Set([
      "lifthill",
      "launch",
      "vertical-lift",
      "swing-launch",
      "beyond-vertical-drop",
      "first-drop",
    ]);

    const bad = RIDE_PROFILE_SEED.filter(
      (e) => e.elements?.length && !starters.has(e.elements[0]),
    ).map((e) => `${e.attractionSlug}: ${e.elements?.[0]}`);

    expect(bad).toEqual([]);
  });

  it("never claims inversions on a ride whose layout has none", () => {
    // The reverse is fine (parks under-count), but inversions > 0 with a
    // completely inversion-free element list means one of the two is wrong.
    const inverting = new Set([
      "vertical-loop",
      "corkscrew",
      "immelmann",
      "dive-loop",
      "zero-g-roll",
      "zero-g-stall",
      "cobra-roll",
      "batwing",
      "sea-serpent",
      "sidewinder",
      "inline-twist",
      "heartline-roll",
      "pretzel-loop",
      "barrel-roll-drop",
      "twisted-horseshoe-roll",
      "step-up-under-flip",
      "flat-spin",
      "raven-turn",
      "cutback",
      "butterfly",
      "bowtie",
      "interlocking-loops",
      "norwegian-loop",
      "banana-roll",
      "inclined-loop",
      "scorpion-tail",
      "celestial-spin",
      "jojo-roll",
    ]);

    const bad = RIDE_PROFILE_SEED.filter(
      (e) =>
        (e.inversions ?? 0) > 0 &&
        e.elements?.length &&
        !e.elements.some((id) => inverting.has(id)),
    ).map((e) => e.attractionSlug);

    expect(bad).toEqual([]);
  });
});
