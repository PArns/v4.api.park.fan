import {
  ATTRACTION_CURATED_FIELDS,
  attractionFieldViews,
  parkFieldViews,
} from "./curated-field.spec-list";
import type { Attraction } from "../../attractions/entities/attraction.entity";
import type { Park } from "../../parks/entities/park.entity";

function anAttraction(overrides: Record<string, unknown> = {}): Attraction {
  return {
    id: "ride-1",
    name: "Taron",
    landName: "Klugheim",
    attractionType: "RIDE",
    minimumHeight: 140,
    minimumHeightUnit: "cm",
    maximumHeight: null,
    mayGetWet: null,
    isSeasonal: false,
    seasonMonths: null,
    curatedName: null,
    curatedLandName: null,
    curatedAttractionType: null,
    curatedMinimumHeight: null,
    curatedMaximumHeight: null,
    curatedMayGetWet: null,
    curatedIsSeasonal: null,
    curatedSeasonMonths: null,
    hasSingleRider: null,
    // NOT NULL with a default — every row in the catalogue holds `false`.
    openWithPark: false,
    rcdbId: null,
    ...overrides,
  } as unknown as Attraction;
}

describe("curated field views", () => {
  const byKey = (views: ReturnType<typeof attractionFieldViews>, key: string) =>
    views.find((view) => view.key === key)!;

  describe("what counts as overridden", () => {
    it("does not flag a NOT NULL column sitting at its default", () => {
      // `open_with_park` is `boolean NOT NULL DEFAULT false`, so comparing
      // against null put a "curated" badge on every attraction in the
      // catalogue and made the badge useless.
      const views = attractionFieldViews(anAttraction());
      expect(byKey(views, "openWithPark").overridden).toBe(false);
      expect(views.filter((view) => view.overridden)).toHaveLength(0);
    });

    it("flags it once somebody sets it", () => {
      const views = attractionFieldViews(anAttraction({ openWithPark: true }));
      expect(byKey(views, "openWithPark").overridden).toBe(true);
    });

    it("flags a correction that differs from upstream", () => {
      const views = attractionFieldViews(
        anAttraction({ curatedName: "TARON" }),
      );
      expect(byKey(views, "curatedName").overridden).toBe(true);
    });

    it("does not flag a correction that merely restates upstream", () => {
      // Somebody typing what was already true has not corrected anything, and
      // a badge there points at a row nobody changed.
      const views = attractionFieldViews(
        anAttraction({ curatedName: "Taron" }),
      );
      expect(byKey(views, "curatedName").overridden).toBe(false);
    });

    it("flags a human-only field with a real value", () => {
      const views = attractionFieldViews(
        anAttraction({ hasSingleRider: false }),
      );
      // `false` here IS a statement — "this ride has no single-rider line" —
      // and its column is nullable, so null is what "nothing decided" means.
      expect(byKey(views, "hasSingleRider").overridden).toBe(true);
    });
  });

  describe("the three values", () => {
    it("carries upstream, curated and effective side by side", () => {
      const views = attractionFieldViews(
        anAttraction({ curatedMinimumHeight: 0 }),
      );
      const height = byKey(views, "curatedMinimumHeight");
      expect(height.syncedValue).toBe(140);
      expect(height.curatedValue).toBe(0);
      // 0 means "no minimum at all", so the effective value is null.
      expect(height.resolvedValue).toBeNull();
    });

    it("reports no upstream value for a human-only field", () => {
      const views = attractionFieldViews(anAttraction());
      expect(byKey(views, "rcdbId").humanOnly).toBe(true);
      expect(byKey(views, "rcdbId").syncedValue).toBeNull();
    });

    it("tells the editor what clearing an input should write", () => {
      const views = attractionFieldViews(anAttraction());
      expect(byKey(views, "openWithPark").defaultValue).toBe(false);
      expect(byKey(views, "curatedName").defaultValue).toBeNull();
    });
  });

  it("describes every curated column exactly once", () => {
    const keys = ATTRACTION_CURATED_FIELDS.map((field) => field.key);
    expect(new Set(keys).size).toBe(keys.length);
  });

  it("resolves a park's fields against the park resolver", () => {
    const park = {
      name: "Disney's Hollywood Studios",
      curatedName: "Hollywood Studios",
      parkType: "THEME_PARK",
      curatedParkType: null,
      citySlug: "orlando",
      slug: "disney-hollywood-studios",
      curatedNoWaitTimesReason: null,
      curationNote: null,
    } as unknown as Park;

    const views = parkFieldViews(park);
    const name = views.find((view) => view.key === "curatedName")!;
    expect(name.syncedValue).toBe("Disney's Hollywood Studios");
    expect(name.resolvedValue).toBe("Hollywood Studios");
    expect(name.overridden).toBe(true);
  });
});
