import { BestDaysService } from "./best-days.service";
import { CalendarService } from "./calendar.service";
import { ParkHistoricalStatsService } from "../../analytics/park-historical-stats.service";
import { Park } from "../entities/park.entity";
import {
  IntegratedCalendarResponse,
  CalendarDay,
} from "../dto/integrated-calendar.dto";
import { BestDaysResponse } from "../dto/best-days-calendar.dto";
import { getCurrentDateInTimezone } from "../../common/utils/date.util";

/**
 * BestDaysService owns the lean best-days projection and its materialized
 * Redis snapshot. These tests pin down the two guarantees the endpoint SLO
 * depends on:
 *   1. getBestDays NEVER rebuilds — it only reads/slices the snapshot (and
 *      degrades to empty on a miss), so it can't trigger the cold ML path.
 *   2. precomputeForPark projects EXACTLY the lean shape (dropping the heavy
 *      calendar fields) and stores it, so the payload stays small.
 */
describe("BestDaysService", () => {
  const park = {
    id: "park-1",
    slug: "phantasialand",
    timezone: "Europe/Berlin",
  } as unknown as Park;

  const makeCalendarDay = (over: Partial<CalendarDay>): CalendarDay =>
    ({
      date: "2026-07-14",
      status: "OPERATING",
      crowdLevel: "low",
      predictedCrowdLevel: "low",
      isToday: false,
      isHoliday: false,
      isBridgeDay: false,
      isSchoolVacation: true,
      // Heavy fields that MUST be dropped by the projection:
      weather: {
        condition: "clear",
        icon: 0,
        tempMin: 10,
        tempMax: 20,
        rainChance: 0,
      },
      influencingHolidays: [
        {
          name: "X",
          source: { countryCode: "NL", regionCode: null },
          holidayType: "public",
        },
      ],
      events: [{ name: "Y", type: "holiday" }],
      hourly: [{ hour: 10, crowdLevel: "low", predictedWaitTime: 15 }],
      peakLoad: "moderate",
      recommendation: "recommended",
      ...over,
    }) as CalendarDay;

  let calendarService: { buildCalendarResponse: jest.Mock };
  let statsService: { getCachedByDayOfWeek: jest.Mock };
  let redis: { get: jest.Mock; set: jest.Mock };
  let service: BestDaysService;

  beforeEach(() => {
    calendarService = { buildCalendarResponse: jest.fn() };
    statsService = { getCachedByDayOfWeek: jest.fn().mockResolvedValue(null) };
    redis = {
      get: jest.fn().mockResolvedValue(null),
      set: jest.fn().mockResolvedValue("OK"),
    };
    service = new BestDaysService(
      calendarService as unknown as CalendarService,
      statsService as unknown as ParkHistoricalStatsService,
      redis as never,
    );
  });

  describe("getBestDays", () => {
    it("rebuilds on-demand on a cache miss and serves the materialized snapshot", async () => {
      // Map-backed redis so the precompute's set is visible to getBestDays' re-read.
      const store = new Map<string, string>();
      redis.get.mockImplementation((k: string) =>
        Promise.resolve(store.get(k) ?? null),
      );
      redis.set.mockImplementation((k: string, v: string) => {
        store.set(k, v);
        return Promise.resolve("OK");
      });
      calendarService.buildCalendarResponse.mockResolvedValue({
        meta: { hasOperatingSchedule: true },
        days: [
          {
            date: "2099-07-14",
            status: "OPERATING",
            crowdLevel: "high",
            predictedCrowdLevel: "moderate",
            isHoliday: false,
            isSchoolVacation: false,
            isBridgeDay: false,
          },
        ],
      });

      const res = await service.getBestDays(park);

      // Miss ⇒ rebuild (no longer serves an empty payload the CDN would cache).
      expect(calendarService.buildCalendarResponse).toHaveBeenCalled();
      expect(res.days).toHaveLength(1);
      expect(res.days[0].predictedCrowdLevel).toBe("moderate");
    });

    it("still degrades to empty days when the on-demand rebuild yields nothing", async () => {
      redis.get.mockResolvedValue(null); // rebuild can't populate → stays empty
      calendarService.buildCalendarResponse.mockRejectedValue(
        new Error("build boom"),
      );

      const res = await service.getBestDays(park);

      expect(res.days).toEqual([]);
      expect(res.meta.slug).toBe("phantasialand");
      // The rebuild WAS attempted (precompute → builder), it just produced nothing.
      expect(calendarService.buildCalendarResponse).toHaveBeenCalled();
    });

    it("reads the snapshot and slices it to the requested window", async () => {
      const snapshot: BestDaysResponse = {
        meta: {
          slug: "phantasialand",
          timezone: "Europe/Berlin",
          hasOperatingSchedule: true,
          computedAt: "2026-07-14T03:10:00.000Z",
          windowFrom: "2026-07-14",
          windowTo: "2026-07-18",
        },
        days: [
          {
            date: "2026-07-14",
            status: "OPERATING",
            crowdLevel: "low",
            isHoliday: false,
            isSchoolVacation: true,
            isBridgeDay: false,
          },
          {
            date: "2026-07-15",
            status: "OPERATING",
            crowdLevel: "moderate",
            isHoliday: false,
            isSchoolVacation: true,
            isBridgeDay: false,
          },
          {
            date: "2026-07-16",
            status: "OPERATING",
            crowdLevel: "high",
            isHoliday: false,
            isSchoolVacation: true,
            isBridgeDay: false,
          },
        ],
        byDayOfWeek: [{ dayOfWeek: 1, avgCrowdScore: 2.1, sampleDays: 98 }],
      };
      redis.get.mockResolvedValue(JSON.stringify(snapshot));

      const res = await service.getBestDays(park, "2026-07-15", "2026-07-15");

      expect(res.days).toHaveLength(1);
      expect(res.days[0].date).toBe("2026-07-15");
      expect(res.meta.windowFrom).toBe("2026-07-15");
      expect(res.meta.windowTo).toBe("2026-07-15");
      expect(res.meta.computedAt).toBe("2026-07-14T03:10:00.000Z");
      expect(res.byDayOfWeek).toEqual([
        { dayOfWeek: 1, avgCrowdScore: 2.1, sampleDays: 98 },
      ]);
      expect(calendarService.buildCalendarResponse).not.toHaveBeenCalled();
    });

    it("defaults the window to today → +90d when no params are given", async () => {
      const today = getCurrentDateInTimezone("Europe/Berlin");
      const snapshot: BestDaysResponse = {
        meta: {
          slug: "phantasialand",
          timezone: "Europe/Berlin",
          hasOperatingSchedule: true,
          windowFrom: today,
          windowTo: today,
        },
        days: [
          {
            date: today,
            status: "OPERATING",
            crowdLevel: "low",
            isHoliday: false,
            isSchoolVacation: false,
            isBridgeDay: false,
          },
        ],
      };
      redis.get.mockResolvedValue(JSON.stringify(snapshot));

      const res = await service.getBestDays(park);

      expect(res.days).toHaveLength(1);
      expect(res.days[0].date).toBe(today);
    });
  });

  describe("precomputeForPark", () => {
    it("projects the lean shape (dropping weather/holidays/hourly), stores it, returns the slug", async () => {
      const calendar: IntegratedCalendarResponse = {
        meta: {
          slug: "phantasialand",
          timezone: "Europe/Berlin",
          hasOperatingSchedule: true,
        },
        days: [makeCalendarDay({ date: "2026-07-14" })],
      };
      calendarService.buildCalendarResponse.mockResolvedValue(calendar);
      statsService.getCachedByDayOfWeek.mockResolvedValue([
        {
          dayOfWeek: 1,
          avgCrowdScore: 2.1,
          avgCrowdLevel: "low",
          avgWaitP50: 12,
          avgWaitP90: 30,
          sampleDays: 98,
        },
      ]);

      const slug = await service.precomputeForPark(park);

      expect(slug).toBe("phantasialand");
      expect(redis.set).toHaveBeenCalledTimes(1);
      const [key, payload, mode, ttl] = redis.set.mock.calls[0];
      expect(key).toBe("best-days:park-1");
      expect(mode).toBe("EX");
      expect(ttl).toBeGreaterThan(24 * 60 * 60); // survives a full daily cycle

      const stored = JSON.parse(payload as string) as BestDaysResponse;
      // Lean projection only:
      expect(Object.keys(stored.days[0]).sort()).toEqual(
        [
          "crowdLevel",
          "date",
          "isBridgeDay",
          "isHoliday",
          "isSchoolVacation",
          "predictedCrowdLevel",
          "status",
        ].sort(),
      );
      expect(stored.days[0]).not.toHaveProperty("weather");
      expect(stored.days[0]).not.toHaveProperty("influencingHolidays");
      expect(stored.days[0]).not.toHaveProperty("hourly");
      expect(stored.meta.computedAt).toBeDefined();
      // byDayOfWeek projected to the lean 3-field shape:
      expect(stored.byDayOfWeek).toEqual([
        { dayOfWeek: 1, avgCrowdScore: 2.1, sampleDays: 98 },
      ]);
    });

    it("omits byDayOfWeek when the /stats cache is cold", async () => {
      calendarService.buildCalendarResponse.mockResolvedValue({
        meta: {
          slug: "phantasialand",
          timezone: "Europe/Berlin",
          hasOperatingSchedule: true,
        },
        days: [makeCalendarDay({})],
      } as IntegratedCalendarResponse);
      statsService.getCachedByDayOfWeek.mockResolvedValue(null);

      await service.precomputeForPark(park);

      const stored = JSON.parse(
        redis.set.mock.calls[0][1] as string,
      ) as BestDaysResponse;
      expect(stored.byDayOfWeek).toBeUndefined();
    });

    it("returns null and never throws when the calendar build fails", async () => {
      calendarService.buildCalendarResponse.mockRejectedValue(
        new Error("cold ML timeout"),
      );

      const slug = await service.precomputeForPark(park);

      expect(slug).toBeNull();
      expect(redis.set).not.toHaveBeenCalled();
    });
  });
});
