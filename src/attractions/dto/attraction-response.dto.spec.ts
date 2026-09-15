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

  it("keeps the CLOSED floor on fromEntity — the attraction detail path reads it", () => {
    const dto = AttractionResponseDto.fromEntity(attraction);

    // Pinned deliberately: a ride with no queue_data row inside the freshness
    // window keeps this value, and `effectiveStatus` is derived from it
    // (`isSourceAbsent([])` is false by design). Dropping it here would make
    // both fields undefined on the attraction detail endpoint.
    expect(dto.status).toBe("CLOSED");
  });

  it("omits status entirely on fromEntityWithoutLiveData", () => {
    const dto = AttractionResponseDto.fromEntityWithoutLiveData(attraction);

    // Absent, not "CLOSED" and not null: the key must be gone, so a consumer
    // reading it gets "this endpoint has no reading" instead of a closure.
    expect(dto).not.toHaveProperty("status");
    expect(dto).not.toHaveProperty("effectiveStatus");
    expect(dto).not.toHaveProperty("queues");
  });

  it("changes nothing else about the row", () => {
    const withLive = AttractionResponseDto.fromEntity(attraction);
    const withoutLive =
      AttractionResponseDto.fromEntityWithoutLiveData(attraction);

    expect(withoutLive).toEqual({ ...withLive, status: undefined });
    expect(withoutLive.id).toBe("attraction-1");
    expect(withoutLive.name).toBe("VAMPIRE");
    // The suffix is stripped for the public slug, as before.
    expect(withoutLive.slug).toBe("vampire");
  });
});
