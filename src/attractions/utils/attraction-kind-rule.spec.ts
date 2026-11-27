import {
  type AttractionKindEvidence,
  deriveAttractionKind,
  resolveAttractionKind,
} from "./attraction-kind-rule";

const evidence = (
  over: Partial<AttractionKindEvidence> = {},
): AttractionKindEvidence => ({
  name: "Something",
  types: [],
  minimumHeight: null,
  isHeadliner: false,
  ...over,
});

/** The cases are real rows of the Phantasialand and Magic Kingdom payloads (2026-11-27). */
describe("deriveAttractionKind", () => {
  it("files a coaster as a ride", () => {
    expect(
      deriveAttractionKind(
        evidence({
          name: "Taron",
          types: ["launch-coaster", "terrain-coaster", "steel-coaster"],
          minimumHeight: 130,
          isHeadliner: true,
        }),
      ),
    ).toBe("RIDE");
    // A coaster term without the "-coaster" suffix counts too.
    expect(deriveAttractionKind(evidence({ types: ["mine-train"] }))).toBe(
      "RIDE",
    );
  });

  it("files a flat ride as a ride", () => {
    expect(
      deriveAttractionKind(
        evidence({ name: "Talocan", types: ["top-spin", "flat-ride"] }),
      ),
    ).toBe("RIDE");
  });

  it("files a live show as a show, past tags that say nothing", () => {
    expect(
      deriveAttractionKind(
        evidence({
          name: "Walt Disney's Enchanted Tiki Room",
          types: ["animatronics", "live-show"],
        }),
      ),
    ).toBe("SHOW");
  });

  it("files a walkthrough as a walkthrough", () => {
    expect(
      deriveAttractionKind(
        evidence({
          name: "Das verrückte Hotel Tartüff",
          types: ["funhouse", "walkthrough"],
        }),
      ),
    ).toBe("WALKTHROUGH");
  });

  it("files a game by its name", () => {
    expect(
      deriveAttractionKind(evidence({ name: "Frontierland Shootin' Arcade" })),
    ).toBe("GAME");
    expect(deriveAttractionKind(evidence({ name: "Midway Games" }))).toBe(
      "GAME",
    );
  });

  it("leaves an attraction with no tags, no height and a plain name unknown", () => {
    expect(
      deriveAttractionKind(evidence({ name: "Dumbo the Flying Elephant" })),
    ).toBeNull();
  });

  it("puts transport before ride, and ride before show", () => {
    expect(
      deriveAttractionKind(
        evidence({ types: ["park-railway", "family-coaster"] }),
      ),
    ).toBe("TRANSPORT");
    expect(
      deriveAttractionKind(
        evidence({ types: ["drop-tower", "dark-ride", "pre-show"] }),
      ),
    ).toBe("RIDE");
    // A loop you ride is a ride, not transport.
    expect(
      deriveAttractionKind(evidence({ types: ["people-mover", "dark-ride"] })),
    ).toBe("RIDE");
  });

  it("reads a minimum height as a ride before the name", () => {
    expect(
      deriveAttractionKind(
        evidence({ name: "Tomorrowland Speedway", minimumHeight: 81 }),
      ),
    ).toBe("RIDE");
    // "Railroad" would say transport; the height says coaster.
    expect(
      deriveAttractionKind(
        evidence({ name: "Big Thunder Mountain Railroad", minimumHeight: 102 }),
      ),
    ).toBe("RIDE");
  });

  it("never reads a headliner's name", () => {
    expect(
      deriveAttractionKind(
        evidence({ name: "Big Thunder Mountain Railroad", isHeadliner: true }),
      ),
    ).toBeNull();
    expect(
      deriveAttractionKind(
        evidence({ name: "Walt Disney World Railroad - Fantasyland" }),
      ),
    ).toBe("TRANSPORT");
  });

  it("reads the name only at word boundaries", () => {
    expect(
      deriveAttractionKind(evidence({ name: "Showboat Splash" })),
    ).toBeNull();
    expect(
      deriveAttractionKind(evidence({ name: "Country Bear Musical Jamboree" })),
    ).toBe("SHOW");
  });

  it("never derives MAZE", () => {
    expect(
      deriveAttractionKind(
        evidence({ types: ["scare-maze"], name: "Maze of Fear" }),
      ),
    ).toBeNull();
  });
});

describe("resolveAttractionKind", () => {
  it("serves the editor's value first and says so", () => {
    expect(
      resolveAttractionKind({
        attractionKind: "TRANSPORT",
        derivedAttractionKind: "RIDE",
      }),
    ).toEqual({ attractionKind: "TRANSPORT", attractionKindSource: "curated" });
  });

  it("falls back to the derived value, marked as derived", () => {
    expect(
      resolveAttractionKind({
        attractionKind: null,
        derivedAttractionKind: "GAME",
      }),
    ).toEqual({ attractionKind: "GAME", attractionKindSource: "derived" });
  });

  it("is null with neither", () => {
    expect(resolveAttractionKind({})).toEqual({
      attractionKind: null,
      attractionKindSource: null,
    });
  });
});
