import { Test } from "@nestjs/testing";
import { ParkIntegrationService } from "./park-integration.service";
import { ParksService } from "../parks.service";
import { WeatherService } from "../weather.service";
import { WeatherWarningsService } from "../weather-warnings.service";
import { AttractionsService } from "../../attractions/attractions.service";
import { ShowsService } from "../../shows/shows.service";
import { RestaurantsService } from "../../restaurants/restaurants.service";
import { QueueDataService } from "../../queue-data/queue-data.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { MLService } from "../../ml/ml.service";
import { PredictionAccuracyService } from "../../ml/services/prediction-accuracy.service";
import { PredictionDeviationService } from "../../ml/services/prediction-deviation.service";
import { HolidaysService } from "../../holidays/holidays.service";
import { ParkEnrichmentService } from "./park-enrichment.service";
import { ThemeParksClient } from "../../external-apis/themeparks/themeparks.client";
import { QueueTimesClient } from "../../external-apis/queue-times/queue-times.client";
import { WartezeitenClient } from "../../external-apis/wartezeiten/wartezeiten.client";
import { PopularityService } from "../../popularity/popularity.service";
import { RideProfileService } from "../../attractions/services/ride-profile.service";
import { AttractionOutageService } from "../../attractions/services/attraction-outage.service";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import type { Park } from "../entities/park.entity";

/** The park cache's two rebuild paths: stale-while-revalidate and a cold miss. */
describe("ParkIntegrationService › integrated cache rebuilds", () => {
  const cached = { id: "p1", attractions: [], stale: true };
  const fresh = { id: "p1", attractions: [], stale: false };
  const redis = {
    get: jest.fn(),
    ttl: jest.fn(),
    set: jest.fn(),
  };
  const park = {
    id: "p1",
    slug: "p1",
    attractions: [],
    shows: [],
    restaurants: [],
  } as unknown as Park;

  let service: ParkIntegrationService;
  let rebuild: jest.SpyInstance;

  beforeEach(async () => {
    jest.clearAllMocks();
    const empty = [
      ParksService,
      WeatherService,
      WeatherWarningsService,
      AttractionsService,
      ShowsService,
      RestaurantsService,
      QueueDataService,
      AnalyticsService,
      MLService,
      PredictionAccuracyService,
      PredictionDeviationService,
      HolidaysService,
      ParkEnrichmentService,
      ThemeParksClient,
      QueueTimesClient,
      WartezeitenClient,
      PopularityService,
      RideProfileService,
      AttractionOutageService,
    ].map((provide) => ({ provide, useValue: {} }));
    const module = await Test.createTestingModule({
      providers: [
        ParkIntegrationService,
        ...empty,
        { provide: REDIS_CLIENT, useValue: redis },
      ],
    }).compile();
    service = module.get(ParkIntegrationService);
    rebuild = jest
      .spyOn(
        service as unknown as {
          rebuildIntegratedResponse: () => Promise<unknown>;
        },
        "rebuildIntegratedResponse",
      )
      .mockResolvedValue(fresh);
  });

  it("background refresh rebuilds instead of re-reading the still-live key", async () => {
    redis.get.mockResolvedValue(JSON.stringify(cached));

    await (
      service as unknown as {
        refreshCacheInBackground: (p: Park) => Promise<void>;
      }
    ).refreshCacheInBackground(park);

    expect(rebuild).toHaveBeenCalledTimes(1);
    expect(redis.get).not.toHaveBeenCalled();
  });

  it("concurrent cache misses for one park share a single rebuild", async () => {
    redis.get.mockResolvedValue(null);

    const results = await Promise.all([
      service.buildIntegratedResponse(park, false, false),
      service.buildIntegratedResponse(park, false, false),
    ]);

    expect(rebuild).toHaveBeenCalledTimes(1);
    expect(results).toEqual([fresh, fresh]);
  });
});
