import type { McpToolContext } from "../mcp-tool";
import { DataSource } from "typeorm";
import { ParksService } from "../../parks/parks.service";
import { ParkIntegrationService } from "../../parks/services/park-integration.service";
import {
  addIsoDays,
  getCurrentDateInTimezone,
} from "../../common/utils/date.util";
import { getParkTool } from "./get-park.tool";

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
    ],
  };
  const ctx = {
    admin: {},
    get: (token: unknown) =>
      new Map<unknown, unknown>([
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
  } as unknown as McpToolContext;

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
