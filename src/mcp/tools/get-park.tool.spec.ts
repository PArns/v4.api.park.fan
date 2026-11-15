import type { McpToolContext } from "../mcp-tool";
import { DataSource } from "typeorm";
import { ParksService } from "../../parks/parks.service";
import { ParkIntegrationService } from "../../parks/services/park-integration.service";
import {
  addIsoDays,
  getCurrentDateInTimezone,
} from "../../common/utils/date.util";
import { getParkTool } from "./get-park.tool";
import { ParkSeasonService } from "../../parks/services/park-season.service";

const PARK_ID = "11111111-1111-4111-8111-111111111111";
const park = {
  id: PARK_ID,
  name: "Efteling",
  slug: "efteling",
  timezone: "Europe/Amsterdam",
  continentSlug: "europe",
  countrySlug: "netherlands",
  citySlug: "kaatsheuvel",
};

describe("get_park maintenance", () => {
  // Relative to the park's today, which the tool reads off the clock.
  const today = getCurrentDateInTimezone(park.timezone);
  const window = (from: number | null, to: number | null) => ({
    from: from === null ? null : addIsoDays(today, from),
    to: to === null ? null : addIsoDays(today, to),
    toUncertain: false,
  });
  const ride = (
    name: string,
    status: string,
    worksPeriods = [] as object[],
  ) => ({
    id: `${name}-id`,
    name,
    status,
    queues: [],
    worksPeriods,
  });
  const dto = {
    id: PARK_ID,
    name: "Efteling",
    timezone: park.timezone,
    status: "OPERATING",
    schedule: [],
    attractions: [
      ride("Droomvlucht", "CLOSED", [window(-3, 20)]),
      ride("Baron 1898", "OPERATING", [window(10, 10)]),
      ride("Piraña", "OPERATING", [window(60, 90)]),
      ride("Python", "REFURBISHMENT"),
      ride("Symbolica", "OPERATING"),
      {
        ...ride("Fata Morgana", "CLOSED"),
        notRunToday: { lastRunAt: "2026-11-14T16:00:00.000Z" },
      },
      {
        ...ride("Kino", "CLOSED"),
        notRunToday: { lastRunAt: "2026-11-14T16:00:00.000Z" },
        performanceSchedules: [
          {
            from: addIsoDays(today, -1),
            to: addIsoDays(today, 1),
            times: ["23:59"],
          },
        ],
      },
      {
        ...ride("Zomerkino", "CLOSED"),
        performanceSchedules: [
          { from: addIsoDays(today, 30), to: null, times: ["12:00"] },
        ],
      },
    ],
  };
  const seasons = {
    forParkInRange: jest.fn(async () => [
      {
        id: "s1",
        parkId: PARK_ID,
        kind: "halloween",
        name: "Halloween Fright Nights",
        startDate: today,
        endDate: today,
        dates: null,
        status: "confirmed",
        priceFrom: null,
      },
    ]),
  };
  const ctxWith = (seasonService?: object) =>
    ({
      admin: {},
      get: (token: unknown) =>
        new Map<unknown, unknown>([
          ...(seasonService
            ? [[ParkSeasonService, seasonService] as const]
            : []),
          [
            ParksService,
            {
              findById: jest.fn(async () => park),
              loadParkRelations: jest.fn(async () => undefined),
            },
          ],
          [
            DataSource,
            { getRepository: () => ({ find: jest.fn(async () => [park]) }) },
          ],
          [
            ParkIntegrationService,
            { buildIntegratedResponse: jest.fn(async () => dto) },
          ],
        ]).get(token),
    }) as unknown as McpToolContext;
  const ctx = ctxWith();

  it("lists a ride a curated window shuts today with the window, beside the feed's REFURBISHMENT", async () => {
    const out = (await getParkTool.handler(
      { park: PARK_ID, longestWaits: 5 } as never,
      ctx,
    )) as any;
    expect(out.rides.maintenance.map((r: { name: string }) => r.name)).toEqual([
      "Droomvlucht",
      "Python",
    ]);
    expect(out.rides.maintenance[0].window).toEqual(window(-3, 20));
    expect(out.rides.maintenance[1].window).toBeNull();
  });

  it("lists the seasons of the year ahead, null when they cannot be read", async () => {
    const out = (await getParkTool.handler(
      { park: PARK_ID, longestWaits: 5 } as never,
      ctxWith(seasons),
    )) as any;
    expect(seasons.forParkInRange).toHaveBeenCalledWith(
      PARK_ID,
      today,
      addIsoDays(today, 365),
    );
    expect(out.seasons).toEqual([
      expect.objectContaining({
        id: "s1",
        kind: "halloween",
        name: "Halloween Fright Nights",
      }),
    ]);
    const failed = (await getParkTool.handler(
      { park: PARK_ID, longestWaits: 5 } as never,
      ctx,
    )) as any;
    expect(failed.seasons).toBeNull();
  });

  it("lists the rides that start at set times today, and leaves them out of notRunToday (PAR-1355)", async () => {
    const out = (await getParkTool.handler(
      { park: PARK_ID, longestWaits: 5 } as never,
      ctx,
    )) as any;
    expect(out.rides.performancesToday).toEqual([
      {
        id: "Kino-id",
        name: "Kino",
        status: "CLOSED",
        times: ["23:59"],
        next: "23:59",
      },
    ]);
    expect(out.rides.notRunToday).toEqual([
      { id: "Fata Morgana-id", name: "Fata Morgana" },
    ]);
  });

  it("names the windows starting in the next 30 days, soonest first", async () => {
    const out = (await getParkTool.handler(
      { park: PARK_ID, longestWaits: 5 } as never,
      ctx,
    )) as any;
    expect(out.rides.maintenanceAhead).toEqual([
      { id: "Baron 1898-id", name: "Baron 1898", window: window(10, 10) },
    ]);
  });
});
