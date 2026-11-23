import {
  TEST_PARIS,
  TEST_TOKYO,
} from "../../trip-regions/__fixtures__/test-regions";
import { JAPAN, ORLANDO, PARIS } from "../../trip-regions/regions";
import { gatewayBase } from "./assistant-tools";
import {
  heatAndStormWatch,
  hubPathOf,
  seasonPhrases,
  slotName,
  typicalWeatherPhrase,
} from "./assistant-trip";

/** Parky's region-dependent words: each region's storm and rainy seasons, its hub. */
describe("Parky's words per trip region", () => {
  it("says Orlando's hurricane season, August to October since PAR-1411", () => {
    expect(seasonPhrases(ORLANDO)).toEqual({
      hurricane_season:
        "hurricane season (August to October), a fact about the season, not a forecast",
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
    expect(seasonPhrases(JAPAN).typhoon_season).toBe(
      "typhoon season (August to October), a fact about the season, not a forecast",
    );
    expect(seasonPhrases(TEST_PARIS)).toEqual({});
    expect(
      heatAndStormWatch(TEST_TOKYO, "2027-09-12", { tempMax: 30 }, true),
    ).toEqual([seasonPhrases(TEST_TOKYO).typhoon_peak]);
    expect(
      heatAndStormWatch(TEST_PARIS, "2027-09-12", { tempMax: 33 }, true),
    ).toEqual(["extreme heat: hotter than a usual summer day"]);
  });

  it("links each region's hub through the site's route folder, which redirects to its segment", () => {
    expect(hubPathOf(ORLANDO)).toBe("/trip-regions/orlando");
    expect(hubPathOf(TEST_TOKYO)).toBe("/trip-regions/test-tokyo");
  });
});

describe("the early and evening slots", () => {
  it("names a slot from the region: its perk and the hotels that grant it", () => {
    expect(slotName(ORLANDO, "early_park_admission")).toBe(
      "Early Park Admission (guests of a Universal Orlando hotel or a Universal Premier hotel (Royal Pacific, Hard Rock, Portofino Bay))",
    );
    // Paris' Early Entry is Parc Astérix's hotel perk, Tokyo's slot Happy Entry: never Disney's words.
    expect(slotName(PARIS, "early_entry")).toBe(
      "Early Entry (guests of a Parc Astérix hotel (Les Trois Hiboux, La Cité Suspendue, Les Quais de Lutèce))",
    );
    expect(slotName(JAPAN, "happy_entry")).toMatch(
      /^Happy Entry \(guests of a Disney Hotel/,
    );
    expect(slotName(ORLANDO, "ticketed_event")).toMatch(/separate ticket/);
    // No hotel grants it: the perk's own name; no name either: its code.
    expect(slotName({ ...ORLANDO, perks: {} }, "early_entry")).toBe(
      "Early Entry",
    );
    expect(slotName(ORLANDO, "moonlight_magic")).toBe("moonlight_magic");
  });
});

describe("change_trip's gateways", () => {
  it("starts or ends a trip in the one city an airport or station serves", () => {
    expect(gatewayBase(JAPAN, "hnd")).toBe("tokyo");
    expect(gatewayBase(JAPAN, "KIX")).toBe("osaka");
    // CDG serves all three of Paris's bases: it pins nothing, the plan picks.
    expect(gatewayBase(PARIS, "CDG")).toBe("");
    expect(gatewayBase(PARIS, "ORY")).toBe("paris");
    expect(() => gatewayBase(JAPAN, "LAX")).toThrow(
      /^LAX is not a way into Japan: HND \(Haneda Airport\)/,
    );
  });
});

describe("Parky's typical weather", () => {
  const base = {
    tempMax: 16.7,
    tempMin: 9.2,
    precipitationMm: 3.3,
    basis: "climate_normal" as const,
    period: "2015-2024",
  };

  it("says the range half of such days fell in and the chance of rain", () => {
    expect(
      typicalWeatherPhrase({
        ...base,
        tempMaxP25: 14.6,
        tempMaxP75: 18.9,
        tempMinP25: 7.1,
        tempMinP75: 11.4,
        wetDayPercent: 27,
      }),
    ).toBe("high 15-19 °C, low 7-11 °C, rain on 27% of such days");
  });

  it("falls back to the means where the normals carry no spread yet", () => {
    expect(typicalWeatherPhrase(base)).toBe(
      "high 17 °C, low 9 °C, 3.3 mm rain",
    );
  });
});
