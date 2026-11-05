import {
  dayCurveProfileKey,
  hasDayCurve,
} from "./park-historical-stats.service";

/** `hasDayCurve` on the attraction detail (PAR-1124). */
describe("hasDayCurve", () => {
  const profile = (displayable: boolean, slugs: string[]) =>
    JSON.stringify({
      meta: { displayable },
      attractions: slugs.map((attractionSlug) => ({ attractionSlug })),
    });

  it("reads the entry getRideDayCurve picks its rides from", () => {
    expect(dayCurveProfileKey("p")).toBe("park:hourly-profile:v7:p:1:8:20");
  });

  it("is true only for a candidate of a displayable profile", () => {
    expect(hasDayCurve(profile(true, ["taron", "black-mamba"]), "taron")).toBe(
      true,
    );
    expect(hasDayCurve(profile(true, ["taron"]), "maus-au-chocolat")).toBe(
      false,
    );
    expect(hasDayCurve(profile(false, ["taron"]), "taron")).toBe(false);
  });

  it("knows nothing while the cache is cold", () => {
    expect(hasDayCurve(null, "taron")).toBeUndefined();
    expect(hasDayCurve("not json", "taron")).toBeUndefined();
  });
});
