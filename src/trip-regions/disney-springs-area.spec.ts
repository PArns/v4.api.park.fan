import { ORLANDO } from "./regions";
import { buildLegs } from "./travel";
import { normalizePlanQuery } from "./trip-plan.assembly";
import { describeRegion, type RegionParkRecord } from "./trip-region.assembly";

/**
 * The Disney Springs Resort Area hotels on Hotel Plaza Boulevard: Disney
 * lists them among the "other select hotels" with Early Entry and not among
 * those with Extended Evening Hours, so a stay there carries Disney's Early
 * Entry and nothing else. The rest of Lake Buena Vista stays off-site.
 */

const TODAY = "2027-01-04";
const DISNEY_THEME_PARKS = [
  "magic-kingdom-park",
  "epcot",
  "disneys-hollywood-studios",
  "disneys-animal-kingdom-theme-park",
];

const records: RegionParkRecord[] = ORLANDO.parks.map((config, i) => ({
  config,
  id: `id-${i}`,
  slug: config.slug,
  name: config.slug,
  path: `/parks/${config.continent}/${config.country}/${config.city}/${config.slug}`,
  latitude: 0,
  longitude: 0,
  timezone: ORLANDO.timezone,
}));

const query = (area: string) =>
  normalizePlanQuery(
    ORLANDO,
    ORLANDO.parks.map((p) => p.slug),
    { from: "2027-02-01", to: "2027-02-05", area } as never,
    TODAY,
  );

describe("Orlando's Disney Springs Resort Area hotels", () => {
  it("carry Early Entry at the four Disney theme parks and no extended evening", () => {
    expect(ORLANDO.perks.disney_springs).toEqual([
      expect.objectContaining({
        perk: "early_entry",
        needs: "early",
        parks: DISNEY_THEME_PARKS,
      }),
    ]);
    const region = describeRegion(ORLANDO, records);
    expect(region.hotels).toContainEqual({
      key: "disney_springs",
      perks: ["early_entry"],
    });
    const area = (key: string) => region.hotelAreas.find((a) => a.key === key);
    expect(area("disney_springs")?.perks).toEqual(["early_entry"]);
    expect(area("lake_buena_vista")?.perks).toEqual([]);
  });

  it("are the hotel a stay in the area plans with; the rest of Lake Buena Vista is off-site", () => {
    expect(query("disney_springs").hotel).toBe("disney_springs");
    expect(query("lake_buena_vista").hotel).toBe("offsite");
  });

  it("drive from the same point as Lake Buena Vista", () => {
    const drives = (area: string) =>
      new Map(
        buildLegs(ORLANDO)
          .filter((l) => l.from === `hotel:${area}` || l.to === `hotel:${area}`)
          .map((l) => [
            l.from === `hotel:${area}` ? l.to : l.from,
            `${l.km}/${l.minutes}`,
          ]),
      );
    const springs = drives("disney_springs");
    expect(springs.size).toBeGreaterThan(0);
    expect(springs).toEqual(drives("lake_buena_vista"));
  });
});
