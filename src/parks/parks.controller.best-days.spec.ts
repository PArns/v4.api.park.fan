import { Test, TestingModule } from "@nestjs/testing";
import { NotFoundException, BadRequestException } from "@nestjs/common";
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
import { BestDaysService } from "./services/best-days.service";
import { PopularityService } from "../popularity/popularity.service";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { Park } from "./entities/park.entity";

/**
 * Focused HTTP-wiring coverage for the /best-days endpoint: park resolution
 * (404), the optional-window validation (park tz, 90-day cap → 400), delegation
 * to BestDaysService, and the long-lived Cache-Control header. Everything else
 * on the controller is exercised elsewhere; these tests only touch the new path.
 */
describe("ParksController › getBestDaysByGeographicPath", () => {
  let controller: ParksController;
  let parksService: { findByGeographicPath: jest.Mock };
  let bestDaysService: { getBestDays: jest.Mock };

  const park = {
    id: "park-1",
    slug: "phantasialand",
    timezone: "Europe/Berlin",
  } as unknown as Park;

  beforeEach(async () => {
    parksService = { findByGeographicPath: jest.fn() };
    bestDaysService = {
      getBestDays: jest.fn().mockResolvedValue({
        meta: {
          slug: "phantasialand",
          timezone: "Europe/Berlin",
          hasOperatingSchedule: true,
          windowFrom: "2026-07-14",
          windowTo: "2026-10-12",
        },
        days: [],
      }),
    };

    // Provide `{}` for every collaborator the new endpoint doesn't touch.
    const noop = {};
    const module: TestingModule = await Test.createTestingModule({
      controllers: [ParksController],
      providers: [
        { provide: ParksService, useValue: parksService },
        { provide: BestDaysService, useValue: bestDaysService },
        { provide: WeatherService, useValue: noop },
        { provide: WeatherWarningsService, useValue: noop },
        { provide: AttractionsService, useValue: noop },
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
        { provide: CalendarService, useValue: noop },
        { provide: PopularityService, useValue: noop },
        { provide: REDIS_CLIENT, useValue: noop },
      ],
    }).compile();

    controller = module.get<ParksController>(ParksController);
  });

  const makeRes = () => {
    const headers: Record<string, string> = {};
    return {
      headers,
      setHeader: (k: string, v: string) => {
        headers[k] = v;
      },
    };
  };

  it("throws 404 when the park is not found", async () => {
    parksService.findByGeographicPath.mockResolvedValue(null);

    await expect(
      controller.getBestDaysByGeographicPath(
        "europe",
        "germany",
        "bruhl",
        "unknown",
        undefined,
        undefined,
        makeRes(),
      ),
    ).rejects.toBeInstanceOf(NotFoundException);
    expect(bestDaysService.getBestDays).not.toHaveBeenCalled();
  });

  it("delegates with an undefined window (service defaults) and sets a 1h SWR cache header", async () => {
    parksService.findByGeographicPath.mockResolvedValue(park);
    const res = makeRes();

    const out = await controller.getBestDaysByGeographicPath(
      "europe",
      "germany",
      "bruhl",
      "phantasialand",
      undefined,
      undefined,
      res,
    );

    expect(bestDaysService.getBestDays).toHaveBeenCalledWith(
      park,
      undefined,
      undefined,
    );
    expect(res.headers["Cache-Control"]).toBe(
      "public, max-age=3600, s-maxage=3600, stale-while-revalidate=86400",
    );
    expect(out.meta.slug).toBe("phantasialand");
  });

  it("parses a valid from/to window into park-tz date strings", async () => {
    parksService.findByGeographicPath.mockResolvedValue(park);

    await controller.getBestDaysByGeographicPath(
      "europe",
      "germany",
      "bruhl",
      "phantasialand",
      "2026-07-14",
      "2026-07-20",
      makeRes(),
    );

    expect(bestDaysService.getBestDays).toHaveBeenCalledWith(
      park,
      "2026-07-14",
      "2026-07-20",
    );
  });

  it("rejects a window wider than 90 days with 400", async () => {
    parksService.findByGeographicPath.mockResolvedValue(park);

    await expect(
      controller.getBestDaysByGeographicPath(
        "europe",
        "germany",
        "bruhl",
        "phantasialand",
        "2026-07-14",
        "2026-12-31",
        makeRes(),
      ),
    ).rejects.toBeInstanceOf(BadRequestException);
    expect(bestDaysService.getBestDays).not.toHaveBeenCalled();
  });
});
