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
      "attractionKind",
      "fastPass",
      "hasSingleRider",
      "hasVirtualLine",
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
      "worksPeriod",
    ]);
  });
});

/**
 * The curated works period on the ride endpoint.
 *
 * It belongs to {@link AttractionResponseDto.storedHalf} rather than to the
 * live placeholders: it comes off the row, it is the same on every request of
 * the day, and it must therefore reach BOTH builders — the park attractions
 * list joins no live data and would otherwise serve a ride under rebuild with
 * nothing saying so.
 */
describe("AttractionResponseDto › worksPeriod", () => {
  const ride = (overrides: Record<string, unknown> = {}) =>
    ({
      id: "attraction-1",
      name: "Chiapas",
      slug: "chiapas",
      retiredAt: null,
      retiredReason: null,
      hasSingleRider: null,
      rcdbId: null,
      ...overrides,
    }) as unknown as Attraction;

  it("carries the window and the estimate flag", () => {
    const dto = AttractionResponseDto.fromEntity(
      ride({
        curatedOutOfServiceFrom: "2026-01-16",
        curatedOutOfServiceTo: "2026-03-03",
        curatedOutOfServiceToUncertain: true,
      }),
    );

    expect(dto.worksPeriod).toEqual({
      from: "2026-01-16",
      to: "2026-03-03",
      toUncertain: true,
    });
  });

  it("is null for a ride nobody has curated", () => {
    expect(AttractionResponseDto.fromEntity(ride()).worksPeriod).toBeNull();
  });

  it("reaches the builder that joins no live data", () => {
    const dto = AttractionResponseDto.fromEntityWithoutLiveData(
      ride({ curatedOutOfServiceFrom: "2026-01-16" }),
    );

    expect(dto.worksPeriod).toEqual({
      from: "2026-01-16",
      to: null,
      toUncertain: false,
    });
  });
});

/**
 * The hand-decided kind on the ride endpoint.
 *
 * It belongs to the stored half: it comes off the row, it is the same on every
 * request, and it must therefore reach both builders — the park attractions
 * list joins no live data, and that is the payload a park page actually reads.
 */
describe("AttractionResponseDto › attractionKind", () => {
  const ride = (overrides: Record<string, unknown> = {}) =>
    ({
      id: "attraction-1",
      name: "Stoomtrein - Oost",
      slug: "stoomtrein-oost",
      retiredAt: null,
      retiredReason: null,
      hasSingleRider: null,
      rcdbId: null,
      ...overrides,
    }) as unknown as Attraction;

  it("reaches both builders", () => {
    const entity = ride({ attractionKind: "TRANSPORT" });
    expect(AttractionResponseDto.fromEntity(entity).attractionKind).toBe(
      "TRANSPORT",
    );
    expect(
      AttractionResponseDto.fromEntityWithoutLiveData(entity).attractionKind,
    ).toBe("TRANSPORT");
  });

  it("is null for a ride nobody has judged, not a default kind", () => {
    // Nearly every attraction is in this state, and it has to read as an
    // absence: a mapper defaulting to "RIDE" would turn our own silence into
    // a claim about the park.
    expect(AttractionResponseDto.fromEntity(ride()).attractionKind).toBeNull();
  });

  it("is not derived from the upstream label", () => {
    // `attractionType` is the feed's free text and `attractionKind` our
    // verdict. The two are independent on purpose: upstream files water rides
    // as ATTRACTION and walkthroughs as RIDE, so a label here may never seed a
    // kind. A ride with a label and no verdict reads null.
    expect(
      AttractionResponseDto.fromEntity(ride({ attractionType: "Family Ride" }))
        .attractionKind,
    ).toBeNull();
    expect(
      AttractionResponseDto.fromEntity(
        ride({ attractionType: "Family Ride", attractionKind: "TRANSPORT" }),
      ).attractionKind,
    ).toBe("TRANSPORT");
  });
});
