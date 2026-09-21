import { Test, TestingModule } from "@nestjs/testing";
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
import { PARK_FEED_SILENT_DAYS } from "../../common/utils/no-live-data-status.util";
import type { Park } from "../entities/park.entity";

/**
 * What the park payload says about wait times it cannot know (PAR-298).
 *
 * The aggregates are not measurements at such a park, they are what a division
 * by the empty set returns: Ø 0 min, peak 0 min, 0 % occupancy — and, from
 * `calculateParkOccupancy`'s own no-data exit, the verdict `comparisonStatus:
 * "typical"` beside them. A reader cannot tell that from a quiet Tuesday.
 *
 * Each case here is a PAIR, because the assertion that matters is a difference:
 * the two parks are seeded identically and open right now, and the only thing
 * that differs is whether their wait times are knowable. The readable park is
 * what proves the gate is not simply blanking everything (📚 G-44 — first show
 * the path reaches the park, then state the claim).
 */
describe("ParkIntegrationService › a park whose wait times are unknowable", () => {
  let service: ParkIntegrationService;

  const now = new Date();
  const hourAgo = new Date(now.getTime() - 60 * 60 * 1000);
  const inAnHour = new Date(now.getTime() + 60 * 60 * 1000);

  /**
   * The aggregates the analytics branch hands the gate for a park with no wait
   * times behind them. Zeroes rather than nulls on purpose: this is what
   * `getParkStatistics` really returns over no rows, and mocking it as null
   * would test a branch that never runs in production.
   */
  const emptySetStatistics = {
    avgWaitTime: 0,
    avgWaitToday: 0,
    peakWaitToday: 0,
    peakHour: null,
    peakHourLocal: null,
    peakHourConfidence: 0,
    peakHourSource: null,
    crowdLevel: "very_low" as const,
    totalAttractions: 0,
    operatingAttractions: 0,
    closedAttractions: 0,
    timestamp: now.toISOString(),
  };

  /** `calculateParkOccupancy`'s no-data exit, copied field for field. */
  const noDataOccupancy = {
    current: 0,
    crowdLevel: "unknown" as const,
    trend: "stable" as const,
    comparedToTypical: 0,
    comparisonStatus: "typical" as const,
    baseline90thPercentile: 0,
    confidence: "low" as const,
    updatedAt: now.toISOString(),
    breakdown: {
      currentAvgWait: 0,
      typicalAvgWait: 0,
      activeAttractions: 0,
    },
  };

  const analyticsService = {
    calculateParkOccupancy: jest.fn().mockResolvedValue(noDataOccupancy),
    getParkStatistics: jest.fn().mockResolvedValue(emptySetStatistics),
    getParkPercentilesToday: jest
      .fn()
      .mockResolvedValue({ p50: 10, p75: 20, p90: 30, p95: 40 }),
    getEffectiveStartTime: jest.fn().mockResolvedValue(hourAgo),
    getHeadlinerAttractionIds: jest.fn().mockResolvedValue(new Set<string>()),
    getBatchAttractionStatistics: jest.fn().mockResolvedValue(new Map()),
    getBatchAttractionTrends: jest.fn().mockResolvedValue(new Map()),
    getBatchAttractionWaitTimeHistory: jest.fn().mockResolvedValue(new Map()),
    getBatchAttractionP50P90: jest.fn().mockResolvedValue(new Map()),
    getRopeDropForPark: jest.fn().mockResolvedValue(null),
    getTypicalWaitsForPark: jest.fn().mockResolvedValue(new Map()),
    isParkRatable: jest.fn().mockResolvedValue(false),
    getTypicalDayPeakFromCache: jest.fn().mockResolvedValue(0),
    determineCrowdLevel: jest.fn().mockReturnValue("very_low"),
    getComparisonText: jest.fn().mockReturnValue(null),
    getLoadRating: jest.fn().mockResolvedValue(null),
    computeTrend: jest.fn().mockReturnValue("stable"),
  };

  const queueDataService = {
    findCurrentStatusByPark: jest.fn().mockResolvedValue(new Map()),
    hasObservedReadingWithin: jest.fn().mockResolvedValue(true),
  };

  const parksService = {
    getUpcomingSchedule: jest.fn(),
    getNextSchedule: jest.fn().mockResolvedValue(null),
    hasOperatingSchedule: jest.fn().mockResolvedValue(true),
    getOperatingDateRange: jest
      .fn()
      .mockResolvedValue({ minDate: null, maxDate: null }),
  };

  const redis = {
    get: jest.fn().mockResolvedValue(null),
    set: jest.fn().mockResolvedValue("OK"),
    ttl: jest.fn().mockResolvedValue(3600),
    del: jest.fn(),
  };

  /**
   * Sierksdorf's Hansa-Park is the curated entry in
   * `PARKS_WITHOUT_LIVE_WAIT_TIMES`, so `fromEntity` resolves
   * `liveWaitTimes.available` to false off the slugs alone — no mock decides
   * it. `readable` swaps the slugs for a park that is not on the list.
   */
  const buildPark = (readable: boolean): Park =>
    ({
      id: readable ? "park-readable" : "park-hansa",
      name: readable ? "Readable Park" : "Hansa-Park",
      slug: readable ? "readable-park" : "hansa-park",
      city: readable ? "Rust" : "Sierksdorf",
      citySlug: readable ? "rust" : "sierksdorf",
      country: "Germany",
      continent: "Europe",
      timezone: "UTC",
      latitude: 54.1,
      longitude: 10.8,
      attractions: [],
      shows: [],
      restaurants: [],
    }) as unknown as Park;

  const buildResponse = (park: Park) =>
    service.buildIntegratedResponse(park, true, false);

  beforeEach(async () => {
    jest.clearAllMocks();

    // One OPERATING entry, open right now: the park is unmistakably running,
    // which is the only state in which a wall of zeroes is a lie rather than
    // the truth about a park shut for the night.
    parksService.getUpcomingSchedule.mockResolvedValue([
      {
        scheduleType: "OPERATING",
        openingTime: hourAgo,
        closingTime: inAnHour,
        date: now,
      },
    ]);
    queueDataService.hasObservedReadingWithin.mockResolvedValue(true);

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ParkIntegrationService,
        { provide: ParksService, useValue: parksService },
        {
          provide: WeatherService,
          useValue: {
            getCurrentAndForecast: jest
              .fn()
              .mockResolvedValue({ current: null, forecast: [] }),
          },
        },
        {
          provide: WeatherWarningsService,
          useValue: { getActiveWarnings: jest.fn().mockResolvedValue([]) },
        },
        { provide: AttractionsService, useValue: {} },
        {
          provide: ShowsService,
          useValue: {
            findCurrentStatusByPark: jest.fn().mockResolvedValue(new Map()),
            findTodayOperatingDataByPark: jest
              .fn()
              .mockResolvedValue(new Map()),
          },
        },
        {
          provide: RestaurantsService,
          useValue: {
            findCurrentStatusByPark: jest.fn().mockResolvedValue(new Map()),
            findTodayOperatingDataByPark: jest
              .fn()
              .mockResolvedValue(new Map()),
          },
        },
        { provide: QueueDataService, useValue: queueDataService },
        { provide: AnalyticsService, useValue: analyticsService },
        {
          provide: MLService,
          useValue: {
            getParkPredictions: jest.fn().mockResolvedValue(null),
            getBatchStoredPredictions: jest.fn().mockResolvedValue(new Map()),
          },
        },
        { provide: PredictionAccuracyService, useValue: {} },
        {
          provide: PredictionDeviationService,
          useValue: {
            getBatchDeviationFlags: jest.fn().mockResolvedValue(new Map()),
          },
        },
        { provide: HolidaysService, useValue: {} },
        {
          provide: ParkEnrichmentService,
          useValue: {
            enrichScheduleWithHolidays: jest.fn().mockResolvedValue(undefined),
          },
        },
        {
          provide: ThemeParksClient,
          useValue: { getParkLiveData: jest.fn().mockResolvedValue(null) },
        },
        {
          provide: QueueTimesClient,
          useValue: { getParkQueueTimes: jest.fn().mockResolvedValue(null) },
        },
        {
          provide: WartezeitenClient,
          useValue: { getOpeningTimes: jest.fn().mockResolvedValue(null) },
        },
        {
          provide: PopularityService,
          useValue: { recordParkHit: jest.fn().mockResolvedValue(undefined) },
        },
        {
          provide: RideProfileService,
          useValue: { getForPark: jest.fn().mockResolvedValue(new Map()) },
        },
        {
          provide: AttractionOutageService,
          useValue: {
            getCurrentOutages: jest.fn().mockResolvedValue(new Map()),
          },
        },
        { provide: REDIS_CLIENT, useValue: redis },
      ],
    }).compile();

    service = module.get(ParkIntegrationService);
  });

  describe("no readable source (curated half)", () => {
    it("reaches the park: the payload says its wait times are unreadable", async () => {
      const dto = await buildResponse(buildPark(false));

      // Without this the rest of the file asserts nothing — every claim below
      // is about the branch this flag opens.
      expect(dto.liveWaitTimes).toEqual({
        available: false,
        reason: "in_park_app_only",
      });
      expect(dto.status).toBe("OPERATING");
    });

    it("says nothing about the wait aggregates rather than 0 minutes", async () => {
      const dto = await buildResponse(buildPark(false));

      expect(dto.analytics?.statistics.avgWaitTime).toBeNull();
      expect(dto.analytics?.statistics.avgWaitToday).toBeNull();
      expect(dto.analytics?.statistics.peakWaitToday).toBeNull();
    });

    it("drops the occupancy object instead of rating the park 'typical'", async () => {
      const dto = await buildResponse(buildPark(false));

      // `comparedToTypical: 0` + `comparisonStatus: "typical"` is a verdict
      // about a park we have never had a wait time from. Nulling the numbers
      // would not help: the frontend rounds them, and `Math.round(null)` is 0.
      expect(dto.analytics?.occupancy).toBeUndefined();
    });

    it("keeps the catalog, which is real", async () => {
      const dto = await buildResponse(buildPark(false));

      // The gate is about wait-derived claims only. A count of attractions is
      // not one — blanking it would trade one wrong answer for another.
      expect(dto.analytics?.statistics.totalAttractions).toBe(0);
      expect(dto.analytics?.statistics.crowdLevel).toBe("unknown");
    });
  });

  describe("a feed that has said nothing for a month (measured half)", () => {
    it("reaches the park: the source is readable, so only the silence gates it", async () => {
      queueDataService.hasObservedReadingWithin.mockResolvedValue(false);

      const dto = await buildResponse(buildPark(true));

      // `liveWaitTimes.available` stays true here on purpose — it is the
      // contract for "publishes wait times nowhere", and this park does.
      expect(dto.liveWaitTimes.available).toBe(true);
      expect(queueDataService.hasObservedReadingWithin).toHaveBeenCalledWith(
        "park-readable",
        PARK_FEED_SILENT_DAYS,
      );
      expect(dto.analytics?.statistics.avgWaitTime).toBeNull();
      expect(dto.analytics?.occupancy).toBeUndefined();
    });
  });

  describe("the counter-check: a park we can read", () => {
    it("keeps its aggregates and its occupancy object", async () => {
      // Same park, same schedule, same zeroed analytics — the ONLY difference
      // from the cases above is that the wait times are knowable. If this one
      // ever starts returning nulls, the gate has stopped being a gate.
      const dto = await buildResponse(buildPark(true));

      expect(dto.analytics?.statistics.avgWaitTime).toBe(0);
      expect(dto.analytics?.statistics.avgWaitToday).toBe(0);
      expect(dto.analytics?.statistics.peakWaitToday).toBe(0);
      expect(dto.analytics?.occupancy).toBeDefined();
      expect(dto.analytics?.occupancy?.comparisonStatus).toBe("typical");
    });
  });
});
