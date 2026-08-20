import {
  resolveCuratedPark,
  resolveNoWaitTimesReason,
} from "./curated-park-facts.util";

describe("resolveCuratedPark", () => {
  it("prefers the curated name", () => {
    expect(
      resolveCuratedPark({
        name: "Disney's Hollywood Studios",
        curatedName: "Hollywood Studios",
      }).name,
    ).toBe("Hollywood Studios");
  });

  it("ignores a blank curated name", () => {
    expect(
      resolveCuratedPark({ name: "Phantasialand", curatedName: "  " }).name,
    ).toBe("Phantasialand");
  });

  it("defaults the park type when neither side has one", () => {
    expect(resolveCuratedPark({}).parkType).toBe("THEME_PARK");
  });

  it("lets a correction reclassify a water park", () => {
    expect(
      resolveCuratedPark({
        parkType: "THEME_PARK",
        curatedParkType: "WATER_PARK",
      }).parkType,
    ).toBe("WATER_PARK");
  });

  describe("unreadable wait times", () => {
    it("still finds Hansa-Park through the code list", () => {
      // The list came first and stays as the seed, so nothing had to be
      // migrated for the column to start winning.
      expect(
        resolveNoWaitTimesReason({
          citySlug: "sierksdorf",
          slug: "hansa-park",
        }),
      ).toBe("in_park_app_only");
    });

    it("reads a park with a source as readable", () => {
      expect(
        resolveNoWaitTimesReason({ citySlug: "bruehl", slug: "phantasialand" }),
      ).toBeNull();
    });

    it("lets the curated column record the next one without a deploy", () => {
      expect(
        resolveNoWaitTimesReason({
          citySlug: "somewhere",
          slug: "a-new-park",
          curatedNoWaitTimesReason: "not_published",
        }),
      ).toBe("not_published");
    });

    it("lets the column override the code list", () => {
      expect(
        resolveNoWaitTimesReason({
          citySlug: "sierksdorf",
          slug: "hansa-park",
          curatedNoWaitTimesReason: "not_published",
        }),
      ).toBe("not_published");
    });

    it("ignores an unrecognised reason rather than serving it", () => {
      // These strings are contract — the frontend translates them — so an
      // unknown one would render as a missing translation on a live park page.
      expect(
        resolveNoWaitTimesReason({
          citySlug: "somewhere",
          slug: "a-new-park",
          curatedNoWaitTimesReason: "they_just_dont",
        }),
      ).toBeNull();
    });

    it("reads a park with no citySlug as readable", () => {
      // The conservative direction: the alternative is telling visitors of a
      // healthy park that we have no data.
      expect(resolveNoWaitTimesReason({ slug: "hansa-park" })).toBeNull();
    });
  });
});
