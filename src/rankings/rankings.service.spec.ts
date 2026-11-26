import { rankAttractions } from "./rankings.service";
import { Park } from "../parks/entities/park.entity";
import { YearAttractionStat } from "../analytics/park-historical-stats.service";

const park = (slug: string, name: string, curatedName?: string): Park =>
  ({
    slug,
    name,
    curatedName: curatedName ?? null,
    continentSlug: "europe",
    countrySlug: "germany",
    citySlug: "somewhere",
  }) as Park;

const stat = (
  attractionName: string,
  avgP90: number,
  avgP50: number,
  dataTo = "2026-11-25",
): YearAttractionStat => ({
  attractionSlug: attractionName.toLowerCase(),
  attractionName,
  land: null,
  avgP90,
  avgP50,
  sampleDays: 100,
  dataFrom: "2026-01-01",
  dataTo,
});

describe("rankAttractions", () => {
  const scope = { continentSlug: "europe", countrySlug: "germany" };

  it("ranks rides across parks by the unrounded P90, then P50", () => {
    const result = rankAttractions(
      [
        park("phantasialand", "Phantasialand"),
        park("europa-park", "Europa-Park"),
      ],
      [
        [stat("Taron", 46.2, 40.1), stat("Raik", 30, 21)],
        [stat("Voltron", 46.8, 38), stat("Blue Fire", 46.2, 41)],
      ],
      scope,
      2026,
    );

    expect(result.attractions.map((a) => [a.rank, a.attractionName])).toEqual([
      [1, "Voltron"],
      [2, "Blue Fire"],
      [3, "Taron"],
      [4, "Raik"],
    ]);
    // Rounded like every other wait, though the order came from the raw value.
    expect(result.attractions[0]).toMatchObject({
      parkSlug: "europa-park",
      avgWaitP90: 45,
    });
    expect(result.meta).toMatchObject({
      year: 2026,
      countrySlug: "germany",
      parkCount: 2,
      totalRanked: 4,
      dataTo: "2026-11-25",
      yearComplete: false,
      minAttractionDays: 60,
    });
  });

  it("names the park by its curated name and counts only parks that rank a ride", () => {
    const result = rankAttractions(
      [park("a", "Raw Name", "Curated"), park("b", "Empty")],
      [[stat("Ride", 30, 20, "2025-12-31")], []],
      scope,
      2025,
    );
    expect(result.attractions[0].parkName).toBe("Curated");
    expect(result.meta.parkCount).toBe(1);
    expect(result.meta.yearComplete).toBe(true);
  });
});
