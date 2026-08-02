import {
  canInheritSourceIds,
  MAX_INHERIT_DISTANCE_KM,
} from "./source-id-inheritance.util";

/** Islands of Adventure, Orlando — the row that inherited the wrong id. */
const IOA_ORLANDO = { latitude: 28.472, longitude: -81.471 };
/** Adventure Island, Tampa — the row it inherited `qt-park-97` from. */
const ADVENTURE_ISLAND_TAMPA = { latitude: 28.042, longitude: -82.413 };

describe("canInheritSourceIds", () => {
  it("refuses the inheritance that wired Orlando to a Tampa water park", () => {
    // The case this exists for: a merge copied `qt-park-97` — Queue-Times'
    // Adventure Island — onto Islands of Adventure, and two days later the
    // sync created a second Islands of Adventure because it could not find the
    // park under its real id.
    const verdict = canInheritSourceIds(IOA_ORLANDO, ADVENTURE_ISLAND_TAMPA);

    expect(verdict.allowed).toBe(false);
    expect(verdict.distanceKm).toBeGreaterThan(90);
  });

  it("allows inheritance between two rows for the same park", () => {
    // The pair actually merged: same park, two sources, 300 metres apart.
    const verdict = canInheritSourceIds(IOA_ORLANDO, {
      latitude: 28.47224,
      longitude: -81.46785,
    });

    expect(verdict.allowed).toBe(true);
    expect(verdict.distanceKm).toBeLessThan(1);
  });

  it("allows inheritance when a row states no coordinates", () => {
    // Absence of a position is not evidence of a different position. A row with
    // no geocode is the ordinary case for a park we only know from one feed,
    // and blocking there would strip feeds for no reason.
    expect(canInheritSourceIds(IOA_ORLANDO, {}).allowed).toBe(true);
    expect(canInheritSourceIds({}, ADVENTURE_ISLAND_TAMPA).allowed).toBe(true);
    expect(
      canInheritSourceIds(IOA_ORLANDO, { latitude: null, longitude: null })
        .allowed,
    ).toBe(true);
  });

  it("treats 0,0 as a failed geocode rather than a place in the ocean", () => {
    // Null Island is what a failed geocode looks like. Read as a real position
    // it sits ~9000 km from Orlando and would block every inheritance for the
    // rows we know least about.
    const verdict = canInheritSourceIds(IOA_ORLANDO, {
      latitude: 0,
      longitude: 0,
    });

    expect(verdict.allowed).toBe(true);
    expect(verdict.distanceKm).toBeNull();
  });

  it("reports the distance it measured, so a refusal can be explained", () => {
    const verdict = canInheritSourceIds(IOA_ORLANDO, ADVENTURE_ISLAND_TAMPA);

    expect(verdict.distanceKm).not.toBeNull();
    expect(verdict.distanceKm).toBeGreaterThan(MAX_INHERIT_DISTANCE_KM);
  });

  it("takes the threshold as an argument for callers with other tolerances", () => {
    expect(
      canInheritSourceIds(IOA_ORLANDO, ADVENTURE_ISLAND_TAMPA, 200).allowed,
    ).toBe(true);
    expect(
      canInheritSourceIds(
        IOA_ORLANDO,
        { latitude: 28.52, longitude: -81.471 },
        1,
      ).allowed,
    ).toBe(false);
  });
});
