import { Test, TestingModule } from "@nestjs/testing";
import { NotFoundException } from "@nestjs/common";
import { ParksController } from "./parks.controller";
import { ParksService } from "./parks.service";
import { WeatherService } from "./weather.service";
import { WeatherWarningsService } from "./weather-warnings.service";
import { AttractionsService } from "../attractions/attractions.service";
import { AttractionIntegrationService } from "../attractions/services/attraction-integration.service";
import { ShowsService } from "../shows/shows.service";
import { RestaurantsService } from "../restaurants/restaurants.service";
import { QueueDataService } from "../queue-data/queue-data.service";
import { AnalyticsService } from "../analytics/analytics.service";
import { ParkHistoricalStatsService } from "../analytics/park-historical-stats.service";
import { MLService } from "../ml/ml.service";
import { PredictionAccuracyService } from "../ml/services/prediction-accuracy.service";
import { ParkIntegrationService } from "./services/park-integration.service";
import { ParkEnrichmentService } from "./services/park-enrichment.service";
import { CalendarService } from "./services/calendar.service";
import { PlanDayService } from "./services/plan-day.service";
import { BestDaysService } from "./services/best-days.service";
import { PopularityService } from "../popularity/popularity.service";
import { ParkRenameService } from "./services/park-rename.service";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { Park } from "./entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";

/**
 * The park attractions list joins no live data, so it may not ship a status.
 *
 * It shipped `fromEntity`'s "CLOSED" placeholder for every row: 6477 of 6477
 * attractions across 190 parks read CLOSED on 2026-09-15, while the park
 * payload read 849 OPERATING, 307 UNKNOWN, 11 DOWN and 5 REFURBISHMENT among
 * the same rows. A search for broken rides over this route therefore found
 * nothing and looked like a valid answer (PAR-184).
 */
describe("ParksController › /attractions list carries no live status", () => {
  let controller: ParksController;
  let attractionsService: { findAllWithFilters: jest.Mock };

  const park = {
    id: "park-1",
    slug: "walibi-belgium",
    timezone: "Europe/Brussels",
  } as unknown as Park;

  const attraction = (id: string, name: string, slug: string) =>
    ({
      id,
      name,
      slug,
      parkId: "park-1",
      park,
      latitude: "50.6976610",
      longitude: "4.5871630",
      retiredAt: null,
      retiredReason: null,
      hasSingleRider: null,
      rcdbId: null,
    }) as unknown as Attraction;

  beforeEach(async () => {
    attractionsService = {
      findAllWithFilters: jest.fn().mockResolvedValue({
        data: [
          attraction("a-1", "VAMPIRE", "vampire-2"),
          attraction("a-2", "KONDAA", "kondaa"),
        ],
        total: 2,
      }),
    };

    const noop = {};
    const module: TestingModule = await Test.createTestingModule({
      controllers: [ParksController],
      providers: [
        {
          provide: ParksService,
          useValue: { findByGeographicPath: jest.fn().mockResolvedValue(park) },
        },
        { provide: AttractionsService, useValue: attractionsService },
        { provide: CalendarService, useValue: noop },
        { provide: PlanDayService, useValue: noop },
        { provide: BestDaysService, useValue: noop },
        { provide: WeatherService, useValue: noop },
        { provide: WeatherWarningsService, useValue: noop },
        { provide: AttractionIntegrationService, useValue: noop },
        { provide: ShowsService, useValue: noop },
        { provide: RestaurantsService, useValue: noop },
        { provide: QueueDataService, useValue: noop },
        { provide: AnalyticsService, useValue: noop },
        { provide: ParkHistoricalStatsService, useValue: noop },
        { provide: MLService, useValue: noop },
        { provide: PredictionAccuracyService, useValue: noop },
        { provide: ParkIntegrationService, useValue: noop },
        { provide: ParkEnrichmentService, useValue: noop },
        { provide: PopularityService, useValue: noop },
        { provide: ParkRenameService, useValue: noop },
        { provide: REDIS_CLIENT, useValue: noop },
      ],
    }).compile();

    controller = module.get<ParksController>(ParksController);
  });

  const call = () =>
    controller.getAttractionsInParkByGeographicPath(
      "europe",
      "belgium",
      "wavre",
      "walibi-belgium",
      1,
      10,
    );

  it("ships no status, effectiveStatus or queues on any row", async () => {
    const res = await call();

    expect(res.data).toHaveLength(2);
    for (const row of res.data) {
      expect(row).not.toHaveProperty("status");
      expect(row).not.toHaveProperty("effectiveStatus");
      expect(row).not.toHaveProperty("queues");
    }
  });

  it("still ships the stored half of each row", async () => {
    const res = await call();

    expect(res.data[0].id).toBe("a-1");
    expect(res.data[0].name).toBe("VAMPIRE");
    expect(res.data[0].slug).toBe("vampire");
    expect(res.data[0].park?.slug).toBe("walibi-belgium");
    expect(res.pagination).toEqual({
      page: 1,
      limit: 10,
      total: 2,
      totalPages: 1,
      hasNext: false,
      hasPrevious: false,
    });
  });

  it("asks the service with the geo context of the route", async () => {
    await call();

    expect(attractionsService.findAllWithFilters).toHaveBeenCalledWith({
      park: "walibi-belgium",
      continentSlug: "europe",
      countrySlug: "belgium",
      citySlug: "wavre",
      page: 1,
      limit: 10,
    });
  });

  it("404s when the park does not exist", async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [ParksController],
      providers: [
        {
          provide: ParksService,
          useValue: { findByGeographicPath: jest.fn().mockResolvedValue(null) },
        },
        { provide: AttractionsService, useValue: attractionsService },
        { provide: CalendarService, useValue: {} },
        { provide: PlanDayService, useValue: {} },
        { provide: BestDaysService, useValue: {} },
        { provide: WeatherService, useValue: {} },
        { provide: WeatherWarningsService, useValue: {} },
        { provide: AttractionIntegrationService, useValue: {} },
        { provide: ShowsService, useValue: {} },
        { provide: RestaurantsService, useValue: {} },
        { provide: QueueDataService, useValue: {} },
        { provide: AnalyticsService, useValue: {} },
        { provide: ParkHistoricalStatsService, useValue: {} },
        { provide: MLService, useValue: {} },
        { provide: PredictionAccuracyService, useValue: {} },
        { provide: ParkIntegrationService, useValue: {} },
        { provide: ParkEnrichmentService, useValue: {} },
        { provide: PopularityService, useValue: {} },
        { provide: ParkRenameService, useValue: {} },
        { provide: REDIS_CLIENT, useValue: {} },
      ],
    }).compile();

    await expect(
      module
        .get<ParksController>(ParksController)
        .getAttractionsInParkByGeographicPath(
          "europe",
          "belgium",
          "wavre",
          "nope",
          1,
          10,
        ),
    ).rejects.toThrow(NotFoundException);
  });
});
