import {
  resolveCuratedPark,
  resolveNoWaitTimesReason,
  resolveParkInfo,
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

describe("resolveParkInfo", () => {
  it("is null when nobody has curated anything", () => {
    // Not an object of nulls: the response interceptor strips null keys, so
    // that would arrive as an empty object the frontend has to test for
    // separately from absent.
    expect(resolveParkInfo({ name: "Phantasialand" })).toBeNull();
  });

  it("returns the block as soon as one fact exists", () => {
    const info = resolveParkInfo({
      name: "Phantasialand",
      curatedWebsite: "https://www.phantasialand.de/",
    });
    expect(info?.website).toBe("https://www.phantasialand.de/");
    expect(info?.phone).toBeNull();
    expect(info?.openedYear).toBeNull();
  });

  it("treats whitespace as nothing", () => {
    expect(resolveParkInfo({ curatedStreetAddress: "   " })).toBeNull();
  });

  it("carries every curated fact through", () => {
    const info = resolveParkInfo({
      curatedWebsite: "https://www.europapark.de/",
      curatedTicketsUrl: "https://tickets.europapark.de/",
      curatedWikipediaUrl: "https://de.wikipedia.org/wiki/Europa-Park",
      curatedInstagramUrl: "https://www.instagram.com/europapark/",
      curatedFacebookUrl: "https://www.facebook.com/europapark/",
      curatedYoutubeUrl: "https://www.youtube.com/@europapark",
      curatedStreetAddress: "Europa-Park-Straße 2",
      curatedPostalCode: "77977",
      curatedPhone: "+49 7822 776688",
      curatedOpenedYear: 1975,
      curatedAreaHectares: 95,
    });
    expect(info).toEqual({
      website: "https://www.europapark.de/",
      ticketsUrl: "https://tickets.europapark.de/",
      wikipediaUrl: "https://de.wikipedia.org/wiki/Europa-Park",
      instagramUrl: "https://www.instagram.com/europapark/",
      facebookUrl: "https://www.facebook.com/europapark/",
      youtubeUrl: "https://www.youtube.com/@europapark",
      streetAddress: "Europa-Park-Straße 2",
      postalCode: "77977",
      phone: "+49 7822 776688",
      openedYear: 1975,
      areaHectares: 95,
    });
  });

  it("drops a zero year or area rather than serving it", () => {
    // A 0 in either column is a failed import or a slipped keystroke, and
    // "opened in the year 0" on a park page is worse than no year at all.
    const info = resolveParkInfo({
      curatedOpenedYear: 0,
      curatedAreaHectares: 0,
      curatedPhone: "+49 2232 36200",
    });
    expect(info?.openedYear).toBeNull();
    expect(info?.areaHectares).toBeNull();
  });
});
