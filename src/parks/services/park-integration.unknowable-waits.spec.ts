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
import { HolidaysService } from "../../holidays/holidays.service";
import { ParkEnrichmentService } from "./park-enrichment.service";
import { ThemeParksClient } from "../../external-apis/themeparks/themeparks.client";
import { QueueTimesClient } from "../../external-apis/queue-times/queue-times.client";
import { WartezeitenClient } from "../../external-apis/wartezeiten/wartezeiten.client";
import { PopularityService } from "../../popularity/popularity.service";
import { RideProfileService } from "../../attractions/services/ride-profile.service";
import { AttractionOutageService } from "../../attractions/services/attraction-outage.service";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { RECONCILIATION_SOURCE } from "../../common/utils/source-absent-status.util";
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
    getBatchAttractionP50s: jest.fn().mockResolvedValue(new Map()),
    getBatchAttractionP90Baselines: jest.fn().mockResolvedValue(new Map()),
    getRopeDropForPark: jest.fn().mockResolvedValue(new Map()),
    getTypicalWaitsForPark: jest.fn().mockResolvedValue(new Map()),
    getRideRunWeekdays: jest.fn().mockResolvedValue(new Map()),
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
        {
          provide: AttractionsService,
          useValue: { findClosedForParkPage: jest.fn().mockResolvedValue([]) },
        },
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
          useValue: {
            getForPark: jest.fn().mockResolvedValue(new Map()),
            findByAttractions: jest.fn().mockResolvedValue(new Map()),
          },
        },
        {
          provide: AttractionOutageService,
          useValue: {
            getCurrentOutages: jest.fn().mockResolvedValue(new Map()),
            getNotRunToday: jest.fn().mockResolvedValue(new Map()),
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

  describe("a ride whose only rows are our own reconciliation", () => {
    it("is rated unknown, not very_low, on a park we can read", async () => {
      const park = buildPark(true);
      (park as unknown as { attractions: unknown[] }).attractions = [
        { id: "hell-house", name: "Hell House", slug: "hell-house" },
      ];
      queueDataService.findCurrentStatusByPark.mockResolvedValueOnce(
        new Map([
          [
            "hell-house",
            [
              {
                queueType: "STANDBY",
                status: "CLOSED",
                waitTime: null,
                dataSource: RECONCILIATION_SOURCE,
                lastUpdated: now,
                timestamp: now,
              },
            ],
          ],
        ]),
      );

      const dto = await buildResponse(park);
      const ride = dto.attractions?.find((a) => a.id === "hell-house");

      expect(ride?.effectiveStatus).toBe("UNKNOWN");
      expect(ride?.crowdLevel).toBe("unknown");
    });
  });

  describe("hourly forecasts are filtered by the park-local day", () => {
    afterEach(() => jest.useRealTimers());

    it("keeps an LA ride's 18:00 PDT slot, which is already tomorrow in UTC", async () => {
      // 18:20 PDT on Oct 20 = 01:20 UTC on Oct 21. Only Date is faked.
      jest.useFakeTimers({
        now: new Date("2026-10-21T01:20:00Z"),
        doNotFake: [
          "setTimeout",
          "setInterval",
          "setImmediate",
          "nextTick",
          "queueMicrotask",
        ],
      });
      const park = buildPark(true);
      Object.assign(park, {
        timezone: "America/Los_Angeles",
        attractions: [{ id: "ride-la", name: "Ride LA", slug: "ride-la" }],
      });
      parksService.getUpcomingSchedule.mockResolvedValueOnce([
        {
          scheduleType: "OPERATING",
          openingTime: new Date("2026-10-20T16:00:00Z"),
          closingTime: new Date("2026-10-21T05:00:00Z"),
          date: new Date("2026-10-20T12:00:00Z"),
        },
      ]);
      // An open ride with no wait reading: its best visit times come from the
      // forecast, so they only exist if the evening slots survived the filter.
      queueDataService.findCurrentStatusByPark.mockResolvedValueOnce(
        new Map([
          [
            "ride-la",
            [
              {
                queueType: "STANDBY",
                status: "OPERATING",
                waitTime: null,
                lastUpdated: new Date(),
                timestamp: new Date(),
              },
            ],
          ],
        ]),
      );
      (
        service as unknown as {
          mlService: { getParkPredictions: jest.Mock };
        }
      ).mlService.getParkPredictions.mockResolvedValueOnce({
        predictions: [
          {
            attractionId: "ride-la",
            predictedTime: "2026-10-21T01:30:00+00:00",
            predictedWaitTime: 20,
            crowdLevel: "low",
          },
          {
            attractionId: "ride-la",
            predictedTime: "2026-10-21T02:30:00+00:00",
            predictedWaitTime: 70,
            crowdLevel: "high",
          },
        ],
      });

      const dto = await buildResponse(park);
      const ride = dto.attractions?.find((a) => a.id === "ride-la");

      expect(ride?.effectiveStatus).toBe("OPERATING");
      expect(ride?.bestVisitTimes?.map((b) => b.time)).toContain(
        "2026-10-21T01:30:00+00:00",
      );
      // No live wait: the forecast row is not a live crowd level (PAR-1029).
      expect(ride?.crowdLevel).toBe("unknown");
    });
  });

  describe("a DOWN ride with no live wait (PAR-1029)", () => {
    afterEach(() => jest.useRealTimers());

    const buildDownRide = async (waitTime: number | null = null) => {
      jest.useFakeTimers({
        now: new Date("2026-10-21T12:20:00Z"),
        doNotFake: [
          "setTimeout",
          "setInterval",
          "setImmediate",
          "nextTick",
          "queueMicrotask",
        ],
      });
      const park = buildPark(true);
      Object.assign(park, {
        attractions: [
          { id: "ride-down", name: "Ride Down", slug: "ride-down" },
        ],
      });
      parksService.getUpcomingSchedule.mockResolvedValueOnce([
        {
          scheduleType: "OPERATING",
          openingTime: new Date("2026-10-21T08:00:00Z"),
          closingTime: new Date("2026-10-21T20:00:00Z"),
          date: new Date("2026-10-21T12:00:00Z"),
        },
      ]);
      queueDataService.findCurrentStatusByPark.mockResolvedValueOnce(
        new Map([
          [
            "ride-down",
            [
              {
                queueType: "STANDBY",
                status: "DOWN",
                waitTime,
                lastUpdated: new Date(),
                timestamp: new Date(),
              },
            ],
          ],
        ]),
      );
      // The current slot is the day's quietest, and the current hour carries
      // a level: neither describes a ride that is down.
      (
        service as unknown as {
          mlService: { getParkPredictions: jest.Mock };
        }
      ).mlService.getParkPredictions.mockResolvedValueOnce({
        predictions: [
          ["2026-10-21T12:15:00.000Z", 5, "very_low"],
          ["2026-10-21T13:30:00.000Z", 20, "low"],
          ["2026-10-21T15:00:00.000Z", 22, "low"],
        ].map(([predictedTime, predictedWaitTime, crowdLevel]) => ({
          attractionId: "ride-down",
          predictedTime,
          predictedWaitTime,
          crowdLevel,
        })),
      });

      const dto = await buildResponse(park);
      return dto.attractions?.find((a) => a.id === "ride-down");
    };

    it("rates the live crowd level unknown, not the forecast or very_low", async () => {
      const ride = await buildDownRide();

      expect(ride?.effectiveStatus).toBe("DOWN");
      expect(ride?.crowdLevel).toBe("unknown");
    });

    it.each([0, 45])(
      "rates a DOWN ride carrying a %d-minute wait unknown",
      async (waitTime) => {
        analyticsService.getBatchAttractionP50s.mockResolvedValueOnce(
          new Map([["ride-down", 20]]),
        );
        analyticsService.isParkRatable.mockResolvedValueOnce(true);
        analyticsService.getLoadRating.mockReturnValueOnce({
          rating: "very_low",
          baseline: 20,
        });

        const ride = await buildDownRide(waitTime);

        expect(ride?.effectiveStatus).toBe("DOWN");
        expect(ride?.crowdLevel).toBe("unknown");
      },
    );

    it("never offers the current slot as a best visit time", async () => {
      const ride = await buildDownRide();

      expect(ride?.bestVisitTimes?.length).toBeGreaterThan(0);
      expect(ride?.bestVisitTimes?.map((b) => b.time)).not.toContain(
        "2026-10-21T12:15:00.000Z",
      );
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
