import {
  TEST_PARIS,
  TEST_TOKYO,
} from "../../trip-regions/__fixtures__/test-regions";
import { ORLANDO } from "../../trip-regions/regions";
import { heatAndStormWatch, hubPathOf, seasonPhrases } from "./assistant-trip";

/** Parky's region-dependent words: each region's storm and rainy seasons, its hub. */
describe("Parky's words per trip region", () => {
  it("says Orlando's hurricane season as it always has", () => {
    expect(seasonPhrases(ORLANDO)).toEqual({
      hurricane_season:
        "hurricane season (June to November), a fact about the season, not a forecast",
      hurricane_peak:
        "peak of the hurricane season (mid-August to mid-October), a fact about the season, not a forecast",
    });
  });

  it("says a region's typhoons and rainy season, and nothing where it has none", () => {
    expect(seasonPhrases(TEST_TOKYO)).toEqual({
      typhoon_season:
        "typhoon season (August to October), a fact about the season, not a forecast",
      typhoon_peak:
        "peak of the typhoon season (early September to late September), a fact about the season, not a forecast",
      rainy_season:
        "rainy season (early June to mid-July): showers likely, a note about the season, not a forecast",
    });
    expect(seasonPhrases(TEST_PARIS)).toEqual({});
    expect(
      heatAndStormWatch(TEST_TOKYO, "2027-09-12", { tempMax: 30 }, true),
    ).toEqual([seasonPhrases(TEST_TOKYO).typhoon_peak]);
    expect(
      heatAndStormWatch(TEST_PARIS, "2027-09-12", { tempMax: 33 }, true),
    ).toEqual(["extreme heat: hotter than a usual summer day"]);
  });

  it("puts each region's hub at its slug", () => {
    expect(hubPathOf(ORLANDO)).toBe("/orlando");
    expect(hubPathOf(TEST_TOKYO)).toBe("/test-tokyo");
  });
});
