import { DataSource } from "typeorm";
import type { McpToolContext } from "../mcp-tool";
import { AttractionIntegrationService } from "../../attractions/services/attraction-integration.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { getAttractionTool } from "./get-attraction.tool";

const ID = "22222222-2222-4222-8222-222222222222";
const attraction = {
  id: ID,
  parkId: "p1",
  slug: "taron",
  park: {
    slug: "phantasialand",
    continentSlug: "europe",
    countrySlug: "germany",
    citySlug: "bruehl",
  },
};

function ctxWith(dto: object, headliners: Promise<Set<string>>) {
  const providers = new Map<unknown, unknown>([
    [
      DataSource,
      { getRepository: () => ({ findOne: async () => attraction }) },
    ],
    [
      AttractionIntegrationService,
      { buildIntegratedResponse: async () => dto },
    ],
    [AnalyticsService, { getHeadlinerAttractionIds: () => headliners }],
  ]);
  return {
    admin: {},
    get: (token: unknown) => providers.get(token),
  } as unknown as McpToolContext;
}

describe("get_attraction", () => {
  it("passes on land, fast pass, headliner, rope drop and the best visit times", async () => {
    const result = (await getAttractionTool.handler(
      { attractionId: ID, historyDays: 0 },
      ctxWith(
        {
          id: ID,
          name: "Taron",
          land: "Klugheim",
          fastPass: { name: "QuickPass", price: 12, currency: "EUR" },
          bestVisitTimes: [
            {
              time: "2026-11-05T16:00:00Z",
              predictedWaitTime: 15,
              rating: "optimal",
            },
          ],
          ropeDrop: {
            worth: true,
            strength: "high",
            confidence: "high",
            openWait: 10,
            busyPeak: 65,
            savings: 55,
            rideByUtc: "2026-11-06T08:45:00Z",
            bestSlotUtc: "2026-11-06T16:30:00Z",
            bestSlotWait: 20,
            endOfDayWorth: false,
            byDaytype: {},
          },
        },
        Promise.resolve(new Set([ID])),
      ),
    )) as Record<string, any>;
    expect(result.facts).toMatchObject({
      land: "Klugheim",
      fastPass: { name: "QuickPass" },
      headliner: true,
    });
    expect(result.ropeDrop).toEqual({
      worth: true,
      strength: "high",
      confidence: "high",
      openWaitMinutes: 10,
      busyPeakMinutes: 65,
      savingsMinutes: 55,
      rideBy: "2026-11-06T08:45:00Z",
      bestSlot: "2026-11-06T16:30:00Z",
      bestSlotWaitMinutes: 20,
      betterLateInTheDay: false,
    });
    expect(result.bestVisitTimesToday).toHaveLength(1);
  });

  it("says nothing it does not know: no rope drop, headliners unreadable", async () => {
    const result = (await getAttractionTool.handler(
      { attractionId: ID, historyDays: 0 },
      ctxWith({ id: ID, name: "Taron" }, Promise.reject(new Error("redis"))),
    )) as Record<string, any>;
    expect(result.ropeDrop).toBeNull();
    expect(result.bestVisitTimesToday).toBeNull();
    expect(result.facts).toMatchObject({
      land: null,
      fastPass: null,
      headliner: null,
    });
  });
});
