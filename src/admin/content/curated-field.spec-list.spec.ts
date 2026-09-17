import { CURATED_PARK_COLUMNS } from "../../parks/utils/curated-park-facts.util";
import { ATTRACTION_CURATED_DB_COLUMNS } from "../../attractions/utils/curated-attraction-facts.util";
import { AttractionMergeService } from "../../attractions/services/attraction-merge.service";
import {
  ATTRACTION_CURATED_FIELDS,
  PARK_CURATED_KEYS,
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

/**
 * The same reminder the park side has had since its merge lost a curation,
 * written for the attraction side after the works period spent a day off both
 * of its lists (PAR-297).
 *
 * Two lists have to learn about a new curated attraction key, and they answer
 * different questions: `ATTRACTION_CURATED_DB_COLUMNS` decides whether a ride
 * carrying only that value counts as curated in the admin's figure, and
 * `AttractionMergeService`'s two inheritance lists decide whether the value
 * survives a merge. Neither is derived from the descriptors, so nothing but a
 * test notices a key missing from one of them.
 */
describe("the attraction column lists and the editor's descriptors", () => {
  const snake = (key: string) =>
    key.replace(/[A-Z]/g, (letter) => `_${letter.toLowerCase()}`);

  const keys = ATTRACTION_CURATED_FIELDS.map((field) => field.key);

  /**
   * Filled in bulk rather than by an editor. Counting them would report
   * thousands of rides as curated that nobody has ever looked at — the
   * reasoning sits beside the list itself.
   */
  const NOT_A_CURATION = ["hasSingleRider", "rcdbId", "openWithPark"];

  /**
   * `open_with_park` is NOT NULL with a default of `false`, so the winner's
   * value is never absent and the inheritance test can never fire on it.
   * Carrying it would mean reading a stored `false` as "nobody said", which is
   * the opposite of what its descriptor declares. Tracked as PAR-300.
   */
  const NOT_INHERITABLE = ["openWithPark"];

  it("counts every hand-written key in the admin's figure, bar three", () => {
    const expected = keys.filter((key) => !NOT_A_CURATION.includes(key));

    expect([...ATTRACTION_CURATED_DB_COLUMNS].sort()).toEqual(
      expected.map(snake).sort(),
    );
  });

  it("carries every hand-written key across a merge, bar one", () => {
    const inheritable = [
      ...AttractionMergeService.INHERITABLE_COLUMNS,
      ...AttractionMergeService.INHERITABLE_COLUMN_SETS.flat(),
    ];
    const missing = keys.filter(
      (key) =>
        !NOT_INHERITABLE.includes(key) &&
        !(inheritable as readonly string[]).includes(key),
    );

    expect(missing).toEqual([]);
  });

  it("names a column in one list at most once", () => {
    // A key on both the column-by-column list and in a set would be inherited
    // twice, and the set's all-or-nothing rule would be the one that loses.
    const inheritable = [
      ...AttractionMergeService.INHERITABLE_COLUMNS,
      ...AttractionMergeService.INHERITABLE_COLUMN_SETS.flat(),
    ];

    expect(new Set(inheritable).size).toBe(inheritable.length);
    expect(new Set(ATTRACTION_CURATED_DB_COLUMNS).size).toBe(
      ATTRACTION_CURATED_DB_COLUMNS.length,
    );
  });
});

describe("the park column list and the editor's descriptors", () => {
  it("know exactly the same columns", () => {
    // Two places have to learn about a new curated park column: the editor,
    // which generates itself from the descriptors, and the merge, which copies
    // the list onto the winning row before deleting the losing one. They were
    // written months apart and the second one was forgotten, which cost every
    // curated value on a merged park. This test is the reminder.
    expect([...PARK_CURATED_KEYS].sort()).toEqual(
      [...CURATED_PARK_COLUMNS].sort(),
    );
  });
});
