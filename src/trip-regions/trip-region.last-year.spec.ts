import type { Redis } from "ioredis";
import { TripRegionService } from "./trip-region.service";
import { ORLANDO } from "./regions";
import { addIsoDays } from "../common/utils/date.util";
import { buildSchoolCoverage } from "../common/utils/holiday.utils";
import type { HolidayRow, HolidaysService } from "../holidays/holidays.service";
import type { ParksService } from "../parks/parks.service";
import type { CalendarService } from "../parks/services/calendar.service";
import type { ParkSeasonService } from "../parks/services/park-season.service";

/**
 * The region calendar reads each day's last year through the park's holidays
 * (PAR-1377): Memorial Day 2027 reads Memorial Day 2026, and the Monday whose
 * day 52 weeks back was Memorial Day 2026 reads the Monday before it.
 */
describe("TripRegionService › the day a year back", () => {
  const TODAY = "2026-11-17";
  const FROM = "2027-05-24";
  const TO = "2027-06-01";
  /** Last year's measured headliner peaks; every other past day measured 50. */
  const MEASURED: Record<string, number> = {
    "2026-05-18": 45,
    "2026-05-25": 90,
    "2026-06-01": 55,
  };
  const memorialDay = (date: string): HolidayRow => ({
    date,
    name: "Memorial Day",
    localName: "Memorial Day",
    country: "US",
    region: null,
    holidayType: "public",
    isNationwide: true,
  });

  function setup(getHolidays: HolidaysService["getHolidays"]) {
    const config = ORLANDO.parks.find((p) => p.slug === "magic-kingdom-park");
    const park = {
      id: "00000000-0000-4000-8000-000000000001",
      slug: "magic-kingdom-park",
      name: "Magic Kingdom Park",
      continentSlug: "north-america",
      countrySlug: "united-states",
      citySlug: "orlando",
      latitude: "28.4",
      longitude: "-81.6",
      timezone: "America/New_York",
      countryCode: "US",
      regionCode: "FL",
      destinationId: null,
      retiredAt: null,
    };
    const buildCalendarResponse = jest.fn(
      async (_park: unknown, fromDate: Date, toDate: Date) => {
        const days = [];
        const last = toDate.toISOString().slice(0, 10);
        for (
          let d = fromDate.toISOString().slice(0, 10);
          d <= last;
          d = addIsoDays(d, 1)
        ) {
          const past = d < TODAY;
          days.push({
            date: d,
            status: "OPERATING",
            crowdLevel: past ? "high" : "moderate",
            isHoliday: false,
            isSchoolVacation: false,
            isBridgeDay: false,
            headlinerForecast: {
              avgWait: past ? (MEASURED[d] ?? 50) : 30,
              rides: [],
              actual: past,
            },
          });
        }
        return { meta: { scheduleCoverage: null }, days };
      },
    );
    const forParksInRange = jest.fn(async () => []);
    const holidays = {
      getHolidays: jest.fn(getHolidays),
      getSchoolHolidayCoverage: jest.fn(async () =>
        buildSchoolCoverage([
          { country: "US", region: "US-FL", through: "2027-06-04" },
        ]),
      ),
    };
    const store = new Map<string, string>();
    const redis = {
      get: jest.fn(async (k: string) => store.get(k) ?? null),
      set: jest.fn(async (k: string, v: string) => {
        store.set(k, v);
        return "OK";
      }),
      mget: jest.fn(async (...keys: string[]) =>
        keys.map((k) => store.get(k) ?? null),
      ),
    };
    const service = new TripRegionService(
      { getSchedule: jest.fn(async () => []) } as unknown as ParksService,
      { buildCalendarResponse } as unknown as CalendarService,
      { forParksInRange } as unknown as ParkSeasonService,
      holidays as unknown as HolidaysService,
      redis as unknown as Redis,
    );
    jest
      .spyOn(service, "resolveParks")
      .mockResolvedValue([{ config, park }] as never);
    jest.spyOn(service, "today").mockReturnValue(TODAY);
    return { service, holidays, forParksInRange };
  }

  const lastYearWaits = async (service: TripRegionService) => {
    const calendar = await service.internalCalendar(ORLANDO, FROM, TO);
    return Object.fromEntries(
      calendar.days.map((d) => {
        const cell = d.parks["magic-kingdom-park"];
        expect(cell.basis).toBe("last_year");
        return [d.date, cell.lastYearWait];
      }),
    );
  };

  it("reads a holiday as last year's and moves a plain day off it", async () => {
    const { service, holidays, forParksInRange } = setup(async () => [
      memorialDay("2026-05-25"),
      memorialDay("2027-05-31"),
    ]);
    const waits = await lastYearWaits(service);
    expect(waits["2027-05-31"]).toBe(90);
    expect(waits["2027-05-24"]).toBe(45);
    expect(waits["2027-05-25"]).toBe(50);
    expect(holidays.getHolidays).toHaveBeenCalledWith(
      "US",
      addIsoDays(FROM, -364 - 36 - 3),
      addIsoDays(TO, 3),
    );
    expect(forParksInRange).toHaveBeenCalledWith(
      ["00000000-0000-4000-8000-000000000001"],
      addIsoDays(FROM, -364 - 21),
      TO,
    );
  });

  it("reads 52 weeks back when the holidays cannot be read", async () => {
    const { service } = setup(async () => {
      throw new Error("holidays down");
    });
    const waits = await lastYearWaits(service);
    expect(waits["2027-05-31"]).toBe(55);
    expect(waits["2027-05-24"]).toBe(90);
  });
});
