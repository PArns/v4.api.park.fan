import { z } from "zod";
import type { McpToolContext } from "../mcp-tool";
import { MCP_TOOLS } from "./index";
import { displayConsistencyTool } from "./display-consistency.tool";

jest.mock(
  "../../monitoring/display-consistency/display-consistency.service",
  () => ({
    DISPLAY_CONSISTENCY_DEFAULT_DAYS: 9,
    DISPLAY_CONSISTENCY_MAX_DAYS: 14,
    DisplayConsistencyService: class {},
  }),
);

const PARK = "11111111-1111-4111-8111-111111111111";

/** One fake for every service the tool reaches: the park lookup and the check. */
function services() {
  const mismatch = {
    date: "2026-11-17",
    measure: "crowd_level",
    a: { surface: "calendar", value: "very_low" },
    b: { surface: "best_days", value: "high" },
    statistic: "the day's forecast level",
  };
  return {
    findById: jest.fn(async (id: string) => ({
      id,
      name: "Phantasialand",
      slug: "phantasialand",
      timezone: "Europe/Berlin",
    })),
    check: jest.fn(async () => ({
      park: { id: PARK, name: "Phantasialand" },
      checkedAt: "2026-11-17T09:40:00.000Z",
      days: [],
      compared: 30,
      mismatches: [mismatch, mismatch],
      differences: [],
      failures: [],
    })),
    latestSummary: jest.fn(async () => ({
      checkedAt: "2026-11-17T09:40:00.000Z",
      days: 9,
      mismatches: 2,
      parks: [
        { id: PARK, name: "Phantasialand", mismatches: 2, failures: 0 },
        { id: "other", name: "Efteling", mismatches: 0, failures: 0 },
      ],
    })),
    latestForPark: jest.fn(async (id: string) => ({
      park: { id, name: "Phantasialand" },
      checkedAt: "2026-11-17T09:40:00.000Z",
      mismatches: [mismatch, mismatch],
      failures: [],
    })),
  };
}

async function run(raw: Record<string, unknown>) {
  const fake = services();
  const ctx = { admin: {}, get: () => fake } as unknown as McpToolContext;
  const input = z.object(displayConsistencyTool.input).parse(raw);
  const answer = (await displayConsistencyTool.handler(
    input as never,
    ctx,
  )) as Record<string, unknown>;
  return { answer, fake };
}

describe("display_consistency", () => {
  it("is registered once, as a read any viewer may call", () => {
    expect(
      MCP_TOOLS.filter((tool) => tool.name === "display_consistency"),
    ).toEqual([displayConsistencyTool]);
    expect(displayConsistencyTool.writes).toBeUndefined();
    expect(displayConsistencyTool.minRole ?? "viewer").toBe("viewer");
  });

  it("checks a park now over the days asked for, lists cut to the limit", async () => {
    const { answer, fake } = await run({ park: PARK, days: 3, limit: 1 });
    expect(fake.check).toHaveBeenCalledWith(
      expect.objectContaining({ id: PARK }),
      3,
    );
    expect(answer.mismatches).toEqual({
      total: 2,
      items: [expect.objectContaining({ date: "2026-11-17" })],
    });
  });

  it("checks today and the next eight days by default", async () => {
    const { fake } = await run({ park: PARK });
    expect(fake.check).toHaveBeenCalledWith(expect.anything(), 9);
  });

  it("without a park, answers with the last daily run and the flagged parks only", async () => {
    const { answer, fake } = await run({});
    expect(fake.check).not.toHaveBeenCalled();
    expect(fake.latestForPark).toHaveBeenCalledTimes(1);
    expect(answer).toEqual({
      lastRun: {
        checkedAt: "2026-11-17T09:40:00.000Z",
        days: 9,
        parksChecked: 2,
        mismatches: 2,
      },
      parks: {
        total: 1,
        items: [
          expect.objectContaining({
            park: { id: PARK, name: "Phantasialand" },
          }),
        ],
      },
    });
  });

  it("says so when no run is kept", async () => {
    const fake = services();
    fake.latestSummary.mockResolvedValue(null as never);
    const ctx = { admin: {}, get: () => fake } as unknown as McpToolContext;
    const input = z.object(displayConsistencyTool.input).parse({});
    expect(await displayConsistencyTool.handler(input as never, ctx)).toEqual({
      lastRun: null,
      note: "no daily run in the last two days",
    });
  });

  it("refuses more days than the check reads", () => {
    expect(() =>
      z.object(displayConsistencyTool.input).parse({ park: PARK, days: 15 }),
    ).toThrow();
  });
});
