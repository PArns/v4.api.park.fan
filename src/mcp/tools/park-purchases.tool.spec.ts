import { DataSource } from "typeorm";
import { z } from "zod";
import type { McpToolContext } from "../mcp-tool";
import { ParksService } from "../../parks/parks.service";
import { parkPurchasesTool, rideOfProduct } from "./park-purchases.tool";

const PARK = "1e1bfe68-87ef-44d4-ac65-70ce45542ea2";
const park = {
  id: PARK,
  name: "Magic Kingdom Park",
  timezone: "America/New_York",
};

const multiPass = {
  id: "lightninglanemultipass_80007944",
  name: "Lightning Lane Multi Pass",
  type: "PACKAGE",
  price: { amount: 3700, currency: "USD", formatted: "$37.00" },
  available: true,
};
const tron = {
  id: "lightninglane_411504498",
  name: "Lightning Lane for TRON Lightcycle / Run",
  type: "ATTRACTION",
  price: { amount: 2500, currency: "USD", formatted: "$25.00" },
  available: false,
};

const rides = [
  { id: "r-tron", name: "TRON Lightcycle / Run", curatedName: null },
  { id: "r-sdmt", name: "Seven Dwarfs Mine Train", curatedName: null },
  { id: "r-run", name: "Run", curatedName: null },
];

function ctxWith(schedule: object[]) {
  const getSchedule = jest.fn(async () => schedule);
  const find = jest.fn(async () => rides);
  const providers = new Map<unknown, unknown>([
    [ParksService, { findById: async () => park, getSchedule }],
    [DataSource, { getRepository: () => ({ find }) }],
  ]);
  const ctx = {
    admin: {},
    get: (token: unknown) => providers.get(token),
  } as unknown as McpToolContext;
  return { ctx, getSchedule, find };
}

const row = (date: string, scheduleType: string, purchases: unknown) => ({
  date,
  scheduleType,
  openingTime: null,
  closingTime: null,
  purchases,
});

describe("park_purchases", () => {
  it("lists each day's products from the first row that carries any, and leaves bare days out", async () => {
    const { ctx, getSchedule } = ctxWith([
      row("2026-11-28", "OPERATING", [tron, multiPass]),
      row("2026-11-28", "TICKETED_EVENT", [multiPass]),
      row("2026-11-29", "OPERATING", null),
      row("2026-11-30", "EXTRA_HOURS", []),
      row("2026-11-30", "OPERATING", [
        {
          ...multiPass,
          price: { amount: 0, currency: "USD", formatted: "Unknown" },
        },
        { type: "PACKAGE", price: { amount: 100 } },
      ]),
    ]);

    const answer = (await parkPurchasesTool.handler(
      { park: PARK, from: "2026-11-28", days: 3 },
      ctx,
    )) as Record<string, any>;

    // Park-local midnight to the end of the last day, as `/schedule` asks.
    expect(getSchedule).toHaveBeenCalledWith(
      PARK,
      new Date("2026-11-28T05:00:00.000Z"),
      new Date("2026-12-01T04:59:59.000Z"),
    );
    expect(answer).toMatchObject({
      park: { id: PARK, timezone: "America/New_York" },
      from: "2026-11-28",
      to: "2026-11-30",
    });
    expect(answer.days).toEqual([
      {
        date: "2026-11-28",
        products: [
          {
            name: "Lightning Lane Multi Pass",
            type: "PACKAGE",
            price: { amount: 3700, currency: "USD", formatted: "$37.00" },
            available: true,
          },
          {
            name: "Lightning Lane for TRON Lightcycle / Run",
            type: "ATTRACTION",
            price: { amount: 2500, currency: "USD", formatted: "$25.00" },
            available: false,
            attraction: { id: "r-tron", name: "TRON Lightcycle / Run" },
          },
        ],
      },
      {
        // A placeholder price is no price; an item without a name is left out.
        date: "2026-11-30",
        products: [
          {
            name: "Lightning Lane Multi Pass",
            type: "PACKAGE",
            price: null,
            available: true,
          },
        ],
      },
    ]);
  });

  it("keeps to the park's days, which getSchedule overshoots by a day", async () => {
    const { ctx } = ctxWith([
      row("2026-11-27", "OPERATING", [multiPass]),
      row("2026-11-28", "OPERATING", [
        multiPass,
        {
          ...multiPass,
          name: "Premier Pass",
          price: { amount: 400, currency: "USD" },
        },
      ]),
      row("2026-11-29", "OPERATING", [multiPass]),
    ]);
    const answer = (await parkPurchasesTool.handler(
      { park: PARK, from: "2026-11-28", days: 1 },
      ctx,
    )) as Record<string, any>;
    expect(answer.days.map((day: { date: string }) => day.date)).toEqual([
      "2026-11-28",
    ]);
    // Without a formatted price the website shows none, so neither does this.
    expect(answer.days[0].products[1]).toMatchObject({
      name: "Premier Pass",
      price: null,
    });
  });

  it("reads the rides only when a product is for one", async () => {
    const { ctx, find } = ctxWith([
      row("2026-11-28", "OPERATING", [multiPass]),
    ]);
    await parkPurchasesTool.handler(
      { park: PARK, from: "2026-11-28", days: 1 },
      ctx,
    );
    expect(find).not.toHaveBeenCalled();
  });

  it("takes only a day the calendar has", () => {
    const input = z.object(parkPurchasesTool.input);
    expect(input.safeParse({ park: PARK, from: "2026-02-31" }).success).toBe(
      false,
    );
    expect(input.safeParse({ park: PARK, from: "2026-02-28" }).data).toEqual({
      park: PARK,
      from: "2026-02-28",
      days: 31,
    });
  });
});

describe("rideOfProduct", () => {
  it("takes the longest ride name inside the product's, by either name", () => {
    expect(
      rideOfProduct("Lightning Lane for Seven Dwarfs Mine Train", rides),
    ).toEqual({ id: "r-sdmt", name: "Seven Dwarfs Mine Train" });
    expect(
      rideOfProduct("Lightning Lane for Space Mountain", [
        { id: "r-sm", name: "Space Mtn", curatedName: "Space Mountain" },
      ]),
    ).toEqual({ id: "r-sm", name: "Space Mountain" });
  });

  it("says null rather than guess from a short name", () => {
    expect(rideOfProduct("Lightning Lane for Big Run", rides)).toBeNull();
  });
});
