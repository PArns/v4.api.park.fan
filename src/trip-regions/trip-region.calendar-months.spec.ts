import type { Redis } from "ioredis";
import { TripRegionService } from "./trip-region.service";
import { ORLANDO, type RegionConfig } from "./regions";
import type {
  CalendarMonthPart,
  InternalRegionCalendar,
} from "./trip-region.assembly";
import { addIsoDays } from "../common/utils/date.util";
import { buildSchoolCoverage } from "../common/utils/holiday.utils";
import type { HolidaysService } from "../holidays/holidays.service";
import type { ParksService } from "../parks/parks.service";
import type { CalendarService } from "../parks/services/calendar.service";
import type { ParkSeasonService } from "../parks/services/park-season.service";
import {
  withTripTrace,
  type TripRequestTrace,
} from "./metrics/trip-request-trace";

/**
 * The region calendar is kept by month from the region's today, so the hub's
 * 30 days, a month page, `/best-days` and `/when`'s year read the months the
 * others built (PAR-1402). Before, each window was its own entry and
 * `/best-days` had no answer cache, so its rows counted the calendar's misses
 * as their builds.
 */
describe("TripRegionService › the calendar by month", () => {
  const TODAY = "2026-11-17";
  const PARK_ID = "00000000-0000-4000-8000-000000000001";
  const SEASONS = [
    {
      id: "5b0c0000-0000-4000-8000-000000000001",
      parkId: PARK_ID,
      kind: "christmas",
      name: "Christmas",
      startDate: "2026-12-20",
      endDate: "2027-01-12",
      dates: null,
      status: "confirmed",
      separateTicket: true,
      confirmedAt: null,
    },
    {
      id: "5b0c0000-0000-4000-8000-000000000002",
      parkId: PARK_ID,
      kind: "special_event",
      name: "Festival",
      startDate: "2027-02-20",
      endDate: "2027-03-10",
      dates: null,
      status: "confirmed",
      separateTicket: false,
      confirmedAt: null,
    },
  ];

  function setup() {
    const config = ORLANDO.parks.find((p) => p.slug === "magic-kingdom-park");
    const park = {
      id: PARK_ID,
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
          // A wait and a close that move with the date, so a day priced
          // against the wrong span would show.
          const n = Number(d.slice(8, 10));
          days.push({
            date: d,
            status: "OPERATING",
            crowdLevel: "moderate",
            isHoliday: false,
            isSchoolVacation: false,
            isBridgeDay: false,
            hours: {
              openingTime: `${d}T09:00:00-05:00`,
              closingTime: `${d}T${n % 7 === 0 ? 18 : 22}:00:00-05:00`,
            },
            headlinerForecast: {
              avgWait: 20 + (n % 9) * 5,
              rides: [],
              actual: past,
            },
          });
        }
        return { meta: { scheduleCoverage: null }, days };
      },
    );
    const forParksInRange = jest.fn(
      async (_ids: string[], from: string, to: string) =>
        SEASONS.filter((s) => s.startDate <= to && s.endDate >= from),
    );
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
      {
        getHolidays: jest.fn(async () => []),
        getSchoolHolidayCoverage: jest.fn(async () =>
          buildSchoolCoverage([
            { country: "US", region: "US-FL", through: "2027-12-31" },
          ]),
        ),
      } as unknown as HolidaysService,
      redis as unknown as Redis,
    );
    jest
      .spyOn(service, "resolveParks")
      .mockResolvedValue([{ config, park }] as never);
    jest.spyOn(service, "today").mockReturnValue(TODAY);
    // The window's reads, with their reference span: longer than the single
    // months a last-year snapshot reads.
    const windowReads = () =>
      buildCalendarResponse.mock.calls.filter(
        ([, from, to]) =>
          (to as Date).getTime() - (from as Date).getTime() > 32 * 86400000,
      ).length;
    return { service, buildCalendarResponse, windowReads };
  }

  /** Runs `fn` as a measured request and answers what its trace noted. */
  async function traced<T>(fn: () => Promise<T>) {
    const trace: TripRequestTrace = {};
    const result = await withTripTrace(trace, fn);
    return { result, trace };
  }

  /** The service's month build, which a window assembled alone also goes through. */
  interface BuildsMonths {
    buildMonths(
      region: RegionConfig,
      today: string,
      parts: CalendarMonthPart[],
    ): Promise<InternalRegionCalendar[]>;
  }

  const withoutComputedAt = <T extends { meta: { computedAt?: string } }>(
    c: T,
  ) => ({ ...c, meta: { ...c.meta, computedAt: undefined } });

  it("cuts a window from months another read built, and it reads as a cold build", async () => {
    const warm = setup();
    await warm.service.internalCalendar(ORLANDO, "2026-11-20", "2027-03-31");
    const reads = warm.windowReads();
    expect(reads).toBe(1);

    const { result: cut, trace } = await traced(() =>
      warm.service.internalCalendar(ORLANDO, "2026-12-28", "2027-01-26"),
    );
    expect(trace.calendarCache).toBe("hit");
    expect(warm.windowReads()).toBe(reads);

    // What the calendar was before it was kept by month: the window read with
    // its own reference span and assembled alone.
    const cold = setup();
    const [direct] = await (
      cold.service as unknown as BuildsMonths
    ).buildMonths(ORLANDO, TODAY, [
      { month: "2026-12", from: "2026-12-28", to: "2027-01-26" },
    ]);
    expect(withoutComputedAt(cut)).toEqual(withoutComputedAt(direct));
    expect(cut.days.map((d) => d.date)).toEqual(
      Array.from({ length: 30 }, (_, i) => addIsoDays("2026-12-28", i)),
    );
    expect(cut.meta).toMatchObject({ from: "2026-12-28", to: "2027-01-26" });
    expect(cut.seasons.map((s) => s.name)).toEqual(["Christmas"]);
  });

  it("keeps the current month from the region's today on", async () => {
    const { service } = setup();
    const calendar = await service.internalCalendar(
      ORLANDO,
      TODAY,
      "2026-11-30",
    );
    expect(calendar.days[0].date).toBe(TODAY);
    expect(calendar.days).toHaveLength(14);
  });

  it("waits for a month another request is building instead of building it twice", async () => {
    const { service, windowReads } = setup();
    const [a, b] = await Promise.all([
      traced(() =>
        service.internalCalendar(ORLANDO, "2027-01-05", "2027-01-20"),
      ),
      traced(() =>
        service.internalCalendar(ORLANDO, "2027-01-10", "2027-01-31"),
      ),
    ]);
    expect(windowReads()).toBe(1);
    expect(a.trace.calendarCache).toBe("miss");
    expect(b.trace.calendarCache).toBe("joined");
    expect(b.result.days).toHaveLength(22);
  });

  it("reads the hub's best days off the calendar the hub's calendar read built, then keeps its own answer", async () => {
    const { service, windowReads } = setup();
    // The hub asks for its 30 days with no dates on the best days and with
    // them on the calendar; both are today … +30.
    await service.calendar("orlando", TODAY, addIsoDays(TODAY, 30));
    const reads = windowReads();

    const first = await traced(() =>
      service.bestDays("orlando", undefined, undefined, 5),
    );
    expect(first.trace).toMatchObject({ cache: "miss", calendarCache: "hit" });
    const second = await traced(() =>
      service.bestDays("orlando", TODAY, addIsoDays(TODAY, 30), 5),
    );
    expect(second.trace.cache).toBe("hit");
    expect(second.result).toEqual(first.result);

    // A month page: the month the 30 days already read.
    const month = await traced(() =>
      service.bestDays("orlando", "2026-12-01", "2026-12-31", 5),
    );
    expect(month.trace).toMatchObject({ cache: "miss", calendarCache: "hit" });
    expect(windowReads()).toBe(reads);
  });
});
