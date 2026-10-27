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
import { SOURCE_SILENT_DAYS } from "../../common/utils/source-absent-status.util";
import type { Park } from "../entities/park.entity";

/**
 * A source that still answers and has stopped measuring (PAR-1023).
 *
 * LEGOLAND Billund on 2026-10-27: a row for all 37 attractions at 09:15 UTC,
 * `lastUpdated = 2026-04-06 13:54:00+00` on every one of them, identical to
 * the second and 203 days old, park open, all 37 published as CLOSED. The rows
 * carry a real `dataSource`, so the reverse-reconciliation rule cannot see
 * them, and a gate on the current row's feed age cannot separate them from a
 * water park in October — at six hours it would have taken 80.6 % of every
 * CLOSED reading in an open park.
 *
 * Three rides, one park, one difference each, because every claim here is a
 * difference (📚 G-44): the ride the feed still measures is what proves the
 * rule is not simply blanking the list.
 */
describe("ParkIntegrationService › a source that stopped measuring", () => {
  let service: ParkIntegrationService;

  const now = new Date();
  const hourAgo = new Date(now.getTime() - 60 * 60 * 1000);
  const inAnHour = new Date(now.getTime() + 60 * 60 * 1000);
  const daysAgo = (days: number) =>
    new Date(now.getTime() - days * 24 * 60 * 60 * 1000);

  const SILENT = "ride-silent";
  const MEASURED = "ride-measured";
  const RUNNING = "ride-running";

  const statistics = {
    avgWaitTime: 15,
    avgWaitToday: 12,
    peakWaitToday: 40,
    peakHour: null,
    peakHourLocal: null,
    peakHourConfidence: 0,
    peakHourSource: null,
    crowdLevel: "low" as const,
    // Overwritten by the ride loop's own counters — the three numbers under
    // test are computed there, not here.
    totalAttractions: 0,
    operatingAttractions: 0,
    closedAttractions: 0,
    timestamp: now.toISOString(),
  };

  const analyticsService = {
    calculateParkOccupancy: jest.fn().mockResolvedValue({
      current: 30,
      crowdLevel: "low" as const,
      trend: "stable" as const,
      comparedToTypical: 0,
      comparisonStatus: "typical" as const,
      baseline90thPercentile: 20,
      confidence: "medium" as const,
      updatedAt: now.toISOString(),
      breakdown: {
        currentAvgWait: 15,
        typicalAvgWait: 15,
        activeAttractions: 1,
      },
    }),
    getParkStatistics: jest.fn().mockResolvedValue(statistics),
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
    determineCrowdLevel: jest.fn().mockReturnValue("low"),
    getComparisonText: jest.fn().mockReturnValue(null),
    getLoadRating: jest.fn().mockResolvedValue(null),
    computeTrend: jest.fn().mockReturnValue("stable"),
  };

  /**
   * All three rides publish a current row with a real `dataSource`, which is
   * the point: nothing here is source-ABSENT, so the only thing that can tell
   * them apart is `source_last_fresh_at` on the entity.
   */
  const queueDataService = {
    findCurrentStatusByPark: jest.fn().mockResolvedValue(
      new Map([
        [
          SILENT,
          [
            {
              queueType: "STANDBY",
              status: "CLOSED",
              waitTime: null,
              dataSource: "themeparks-wiki",
              timestamp: now,
              lastUpdated: daysAgo(203),
            },
          ],
        ],
        [
          MEASURED,
          [
            {
              queueType: "STANDBY",
              status: "CLOSED",
              waitTime: null,
              dataSource: "themeparks-wiki",
              timestamp: now,
              lastUpdated: now,
            },
          ],
        ],
        [
          RUNNING,
          [
            {
              queueType: "STANDBY",
              status: "OPERATING",
              waitTime: 20,
              dataSource: "themeparks-wiki",
              timestamp: now,
              lastUpdated: now,
            },
          ],
        ],
      ]),
    ),
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
   * `sourceLastFreshAt` is the only field that differs between the silent ride
   * and the measured one. Not on the curated-season list and not retired:
   * those are the hard markers the rule steps aside for, and a fixture that
   * carried one would be testing the exclusion instead of the rule.
   */
  const buildAttraction = (
    id: string,
    sourceLastFreshAt: Date | null,
    over: Record<string, unknown> = {},
  ) => ({
    id,
    name: id,
    slug: id,
    parkId: "park-billund",
    isSeasonal: false,
    seasonMonths: null,
    seasonOutSince: null,
    curatedIsSeasonal: null,
    curatedSeasonMonths: null,
    retiredAt: null,
    openWithPark: false,
    sourceLastFreshAt,
    ...over,
  });

  const buildPark = (attractions: ReturnType<typeof buildAttraction>[]): Park =>
    ({
      id: "park-billund",
      name: "LEGOLAND Billund",
      slug: "legoland-billund",
      city: "Billund",
      citySlug: "billund",
      country: "Denmark",
      continent: "Europe",
      timezone: "UTC",
      latitude: 55.73,
      longitude: 9.12,
      attractions,
      shows: [],
      restaurants: [],
    }) as unknown as Park;

  const threeRides = () => [
    buildAttraction(SILENT, daysAgo(203)),
    buildAttraction(MEASURED, now),
    buildAttraction(RUNNING, now),
  ];

  const buildResponse = (park: Park) =>
    service.buildIntegratedResponse(park, true, false);

  const rideIn = (dto: Awaited<ReturnType<typeof buildResponse>>, id: string) =>
    dto.attractions.find((a) => a.id === id);

  beforeEach(async () => {
    jest.clearAllMocks();

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

  it("reaches the park: it is open and its wait times are readable", async () => {
    const dto = await buildResponse(buildPark(threeRides()));

    // Without this every claim below would hold for a park the payload never
    // got to, or for one the unknowable-waits gate had already blanked.
    expect(dto.status).toBe("OPERATING");
    expect(dto.liveWaitTimes.available).toBe(true);
    expect(dto.attractions).toHaveLength(3);
  });

  it("says UNKNOWN for the ride the source stopped measuring", async () => {
    const dto = await buildResponse(buildPark(threeRides()));

    expect(rideIn(dto, SILENT)?.status).toBe("UNKNOWN");
    // The rows carry our last frozen answer and nothing else, so they go with
    // it — same as the absent-source branch beside it.
    expect(rideIn(dto, SILENT)?.queues).toEqual([]);
  });

  it("leaves the ride the source still measures as CLOSED", async () => {
    const dto = await buildResponse(buildPark(threeRides()));

    // Same status, same `dataSource`, same park, fresh stamp. This is the
    // difference the rule is made of.
    expect(rideIn(dto, MEASURED)?.status).toBe("CLOSED");
    expect(rideIn(dto, MEASURED)?.queues).toHaveLength(1);
    expect(rideIn(dto, RUNNING)?.status).toBe("OPERATING");
  });

  it("counts the unknown ride in neither half of the headline", async () => {
    const dto = await buildResponse(buildPark(threeRides()));

    // `closedAttractions` is the remainder of total and operating. Left
    // uncorrected it would read 2 of 3 closed while the list beside it serves
    // one of those two as unknown — the page contradicting itself.
    expect(dto.analytics?.statistics.totalAttractions).toBe(3);
    expect(dto.analytics?.statistics.operatingAttractions).toBe(1);
    expect(dto.analytics?.statistics.closedAttractions).toBe(1);
  });

  it("stays quiet while the silence is still inside the threshold", async () => {
    // A day short of the threshold, not on it: this path reads the wall clock
    // at request time, so a fixture pinned to the exact boundary would flip on
    // the milliseconds between the two. The boundary itself is asserted in
    // `source-absent-status.util.spec.ts`, where the clock is injected.
    const dto = await buildResponse(
      buildPark([
        buildAttraction(SILENT, daysAgo(SOURCE_SILENT_DAYS - 1)),
        buildAttraction(MEASURED, now),
        buildAttraction(RUNNING, now),
      ]),
    );

    expect(rideIn(dto, SILENT)?.status).toBe("CLOSED");
    expect(dto.analytics?.statistics.closedAttractions).toBe(2);
  });

  it("stands aside for a ride the season has closed", async () => {
    const dto = await buildResponse(
      buildPark([
        buildAttraction(SILENT, daysAgo(203), {
          seasonOutSince: "2026-09-07",
          isSeasonal: true,
        }),
        buildAttraction(MEASURED, now),
        buildAttraction(RUNNING, now),
      ]),
    );

    // A water park in October reports a forty-five-day-old stamp because that
    // is when it shut. CLOSED is right, and a shrug would be worse.
    expect(rideIn(dto, SILENT)?.status).toBe("CLOSED");
  });

  it("leaves a ride with no current row on the optimistic path", async () => {
    queueDataService.findCurrentStatusByPark.mockResolvedValueOnce(
      new Map([
        [
          MEASURED,
          [
            {
              queueType: "STANDBY",
              status: "CLOSED",
              waitTime: null,
              dataSource: "themeparks-wiki",
              timestamp: now,
              lastUpdated: now,
            },
          ],
        ],
      ]),
    );

    const dto = await buildResponse(buildPark(threeRides()));

    // The silent ride publishes nothing at this instant, so there is no
    // closure to retract — that case belongs to the optimism one branch
    // further down and is not this rule's decision to take.
    expect(rideIn(dto, SILENT)?.status).not.toBe("UNKNOWN");
  });

  it("leaves every ride alone before the job has ever run", async () => {
    const dto = await buildResponse(
      buildPark([
        buildAttraction(SILENT, null),
        buildAttraction(MEASURED, null),
        buildAttraction(RUNNING, null),
      ]),
    );

    // The state the deploy leaves behind: the column is null for all 7,631
    // rows until `detect-silent-sources` first runs. Reading null as silence
    // would put the whole database on UNKNOWN in that window.
    expect(rideIn(dto, SILENT)?.status).toBe("CLOSED");
    expect(rideIn(dto, MEASURED)?.status).toBe("CLOSED");
    expect(dto.analytics?.statistics.closedAttractions).toBe(2);
  });
});
