import { AttractionResponseDto } from "./attraction-response.dto";
import { Attraction } from "../entities/attraction.entity";

/**
 * The two builders of this DTO and the one field that separates them.
 *
 * `status` is not a column. `fromEntity` sets "CLOSED" as a floor for the
 * integrated callers, which overwrite it from `queue_data`; the park
 * attractions list joins nothing and served that floor as a reading — 6477 of
 * 6477 attractions "CLOSED" across 190 parks on 2026-09-15 (PAR-184).
 */
describe("AttractionResponseDto › status on the two builders", () => {
  const attraction = {
    id: "attraction-1",
    name: "VAMPIRE",
    slug: "vampire-2",
    latitude: "50.6976610",
    longitude: "4.5871630",
    retiredAt: null,
    retiredReason: null,
    hasSingleRider: null,
    rcdbId: null,
  } as unknown as Attraction;

  it("keeps the placeholders on fromEntity — the integrated callers fill them", () => {
    const dto = AttractionResponseDto.fromEntity(attraction);

    // The CLOSED floor is pinned deliberately: a ride with no queue_data row
    // inside the freshness window keeps this value, and `effectiveStatus` is
    // derived from it (`isSourceAbsent([])` is false by design). Dropping it
    // here would make both fields undefined on the attraction detail endpoint.
    expect(dto.status).toBe("CLOSED");
    expect(dto.hourlyForecast).toEqual([]);
    expect(dto.forecasts).toEqual([]);
    expect(dto.statistics).toBeNull();
  });

  it("omits every live placeholder on fromEntityWithoutLiveData", () => {
    const dto = AttractionResponseDto.fromEntityWithoutLiveData(attraction);

    // Absent, not "CLOSED" and not null: each key must be gone, so a consumer
    // reading it gets "this endpoint has no reading" instead of a closure — and
    // an empty forecast array is the same false negative one field over.
    expect(dto).not.toHaveProperty("status");
    expect(dto).not.toHaveProperty("hourlyForecast");
    expect(dto).not.toHaveProperty("forecasts");
    expect(dto).not.toHaveProperty("statistics");
    expect(dto).not.toHaveProperty("effectiveStatus");
    expect(dto).not.toHaveProperty("queues");
  });

  it("differs from fromEntity in the placeholders and nothing else", () => {
    const withLive = AttractionResponseDto.fromEntity(attraction);
    const withoutLive =
      AttractionResponseDto.fromEntityWithoutLiveData(attraction);

    expect(withoutLive).toEqual({
      ...withLive,
      status: undefined,
      hourlyForecast: undefined,
      forecasts: undefined,
      statistics: undefined,
    });
    expect(withoutLive.id).toBe("attraction-1");
    expect(withoutLive.name).toBe("VAMPIRE");
    // The suffix is stripped for the public slug, as before.
    expect(withoutLive.slug).toBe("vampire");
  });

  it("pins which keys the placeholders add", () => {
    const live = Object.keys(AttractionResponseDto.fromEntity(attraction));
    const stored = Object.keys(
      AttractionResponseDto.fromEntityWithoutLiveData(attraction),
    );

    expect(live.filter((k) => !stored.includes(k)).sort()).toEqual([
      "forecasts",
      "hourlyForecast",
      "statistics",
      "status",
    ]);
  });

  it("pins the stored key set itself, so a dropped field is a red test", () => {
    // Comparing the two builders against each other cannot see this: they
    // share `storedHalf`, so a field deleted there leaves both of them and
    // every other assertion here stays green (`toEqual` ignores undefined).
    expect(
      Object.keys(
        AttractionResponseDto.fromEntityWithoutLiveData(attraction),
      ).sort(),
    ).toEqual([
      "fastPass",
      "hasSingleRider",
      "id",
      "isCurrentlyInSeason",
      "isSeasonal",
      "land",
      "latitude",
      "longitude",
      "maximumHeight",
      "mayGetWet",
      "minimumHeight",
      "minimumHeightUnit",
      "name",
      "park",
      "rcdbId",
      "retiredAt",
      "retiredReason",
      "seasonMonths",
      "slug",
    ]);
  });
});
