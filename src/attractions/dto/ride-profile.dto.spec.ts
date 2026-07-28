import { mapTermAttraction } from "./ride-profile.dto";
import type { AttractionWithTerm } from "../services/ride-profile.service";

/**
 * The reverse lookup hands out a link to each ride it names. That link shipped
 * pointing at `parks/<geo>/<attraction>`, which is the park route with an
 * extra segment — every one of them 404s. Nothing caught it: the frontend
 * builds its own href from the slugs and never follows this field, so the only
 * consumer who would notice is somebody using the published API.
 *
 * The route is defined in `parks.controller.ts` as
 * `:continent/:country/:city/:parkSlug/attractions/:attractionSlug`.
 */
describe("mapTermAttraction", () => {
  const row: AttractionWithTerm = {
    attractionId: "a1",
    attractionName: "Kumba",
    attractionSlug: "kumba",
    parkId: "p1",
    parkName: "Busch Gardens Tampa",
    parkSlug: "busch-gardens-tampa",
    citySlug: "tampa",
    countrySlug: "united-states",
    continentSlug: "north-america",
    kind: "element",
    openedYear: 1993,
    typicalPeakWait: null,
    isHeadliner: false,
  };

  it("builds the attraction detail route, not the park route", () => {
    expect(mapTermAttraction(row).url).toBe(
      "/v1/parks/north-america/united-states/tampa/busch-gardens-tampa/attractions/kumba",
    );
  });

  it("keeps the slugs it was given so a caller can rebuild the path", () => {
    const dto = mapTermAttraction(row);

    expect(dto.url).toContain(
      `/${dto.continentSlug}/${dto.countrySlug}/${dto.citySlug}/${dto.parkSlug}/`,
    );
    expect(dto.url.endsWith(`/${dto.slug}`)).toBe(true);
  });

  it("carries the typical peak wait through to the DTO", () => {
    const dto = mapTermAttraction({
      ...row,
      typicalPeakWait: 75,
      isHeadliner: true,
    });

    expect(dto.typicalPeakWait).toBe(75);
    expect(dto.isHeadliner).toBe(true);
  });

  it("reports a missing baseline as null rather than zero", () => {
    // A ride with no baseline has not been measured. Zero would read as "no
    // queue ever", which is a claim the absence of data does not support —
    // and it would sort as the calmest ride on the list.
    const dto = mapTermAttraction({
      ...row,
      typicalPeakWait: null,
      isHeadliner: false,
    });

    expect(dto.typicalPeakWait).toBeNull();
    expect(dto.isHeadliner).toBe(false);
  });
});
