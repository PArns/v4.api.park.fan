import { resolveCuratedFacts } from "./curated-attraction-facts.util";

/**
 * The worked example throughout is Phantasialand's pair of water attractions,
 * where upstream put the height on the wrong one: Wavy Battle forbids entry
 * below 1.00 m and the wiki carries nothing, Winni Splash only wants an adult
 * along and the wiki carries 100.
 */
describe("resolveCuratedFacts", () => {
  describe("minimum height", () => {
    it("passes the synced height through when nothing is curated", () => {
      expect(
        resolveCuratedFacts({ minimumHeight: 140, minimumHeightUnit: "cm" }),
      ).toMatchObject({ minimumHeight: 140, minimumHeightUnit: "cm" });
    });

    it("lets a curated height beat the synced one", () => {
      expect(
        resolveCuratedFacts({
          minimumHeight: 120,
          curatedMinimumHeight: 100,
          minimumHeightUnit: "cm",
        }).minimumHeight,
      ).toBe(100);
    });

    it("reads a curated 0 as no minimum height at all", () => {
      // Winni Splash: the wiki's 100 is a supervision threshold, not a limit.
      // Zero is how a correction says "the truth is none" — null could not,
      // because null already means "nothing curated here".
      expect(
        resolveCuratedFacts({
          minimumHeight: 100,
          curatedMinimumHeight: 0,
          minimumHeightUnit: "cm",
        }).minimumHeight,
      ).toBeNull();
    });

    it("supplies a height for a ride the sync knows nothing about", () => {
      // Wavy Battle: upstream has no number, the park's sign does.
      expect(
        resolveCuratedFacts({ curatedMinimumHeight: 100 }).minimumHeight,
      ).toBe(100);
    });
  });

  describe("height unit", () => {
    it("drops the unit when there is no height to label", () => {
      // A dangling "cm" next to a null height renders as a bare unit.
      expect(
        resolveCuratedFacts({
          minimumHeight: 100,
          curatedMinimumHeight: 0,
          minimumHeightUnit: "cm",
        }).minimumHeightUnit,
      ).toBeNull();
      expect(resolveCuratedFacts({}).minimumHeightUnit).toBeNull();
    });

    it("keeps the published unit so US signage stays in inches", () => {
      expect(
        resolveCuratedFacts({ minimumHeight: 122, minimumHeightUnit: "in" }),
      ).toMatchObject({ minimumHeight: 122, minimumHeightUnit: "in" });
    });

    it("labels a curated-only height in centimetres", () => {
      expect(
        resolveCuratedFacts({ curatedMinimumHeight: 100 }).minimumHeightUnit,
      ).toBe("cm");
    });
  });

  describe("may get wet", () => {
    it("prefers the curated flag over the synced one", () => {
      expect(
        resolveCuratedFacts({ mayGetWet: true, curatedMayGetWet: false })
          .mayGetWet,
      ).toBe(false);
      expect(
        resolveCuratedFacts({ mayGetWet: null, curatedMayGetWet: true })
          .mayGetWet,
      ).toBe(true);
    });

    it("stays null when neither writer knows", () => {
      // Null is "unknown", not "dry" — most rides simply have no flag.
      expect(resolveCuratedFacts({}).mayGetWet).toBeNull();
    });

    it("does not let a curated false read as unknown", () => {
      expect(resolveCuratedFacts({ curatedMayGetWet: false }).mayGetWet).toBe(
        false,
      );
    });
  });
});
