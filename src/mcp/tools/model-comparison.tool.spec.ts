import type { McpToolContext } from "../mcp-tool";
import { ModelComparisonService } from "../../parks/services/model-comparison.service";
import {
  modelComparisonDayTool,
  modelComparisonTool,
} from "./model-comparison.tool";

const live = { diff: -0.8, leads: [{ lead: "h0-1", n: 10 }] };
const matrix = {
  generatedAt: "2026-11-07T09:00:00.000Z",
  summary: { better: 1, pending: 0, unclear: 0, worse: 0 },
  questions: [
    {
      id: "live_h0_1",
      group: "live",
      cells: { h5: { state: "better", live } },
    },
    {
      id: "plan_d0",
      group: "planner",
      cells: { pcn: { state: "today", live: null } },
    },
  ],
};

const service = {
  getMatrix: jest.fn(async () => matrix),
  getDayOptions: jest.fn(async () => ({ dates: ["2026-11-06"] })),
  getDay: jest.fn(async () => ({ scores: [] })),
};
const ctx = {
  admin: {},
  get: (token: unknown) => (token === ModelComparisonService ? service : null),
} as unknown as McpToolContext;

type Answer = {
  questions: Array<{
    id: string;
    cells: Record<string, { live: { leads?: unknown } | null }>;
  }>;
};

describe("model_comparison", () => {
  it("drops the per-lead rows from the whole matrix", async () => {
    const answer = (await modelComparisonTool.handler(
      { days: 28, region: "ALL" },
      ctx,
    )) as Answer;
    expect(answer.questions).toHaveLength(2);
    expect(answer.questions[0].cells.h5.live).toMatchObject({ diff: -0.8 });
    expect(answer.questions[0].cells.h5.live?.leads).toBeUndefined();
    expect(answer.questions[1].cells.pcn.live).toBeNull();
  });

  it("keeps them for one question, and filters by group", async () => {
    const one = (await modelComparisonTool.handler(
      { days: 28, region: "ALL", question: "live_h0_1" },
      ctx,
    )) as Answer;
    expect(one.questions[0].cells.h5.live?.leads).toHaveLength(1);
    const planner = (await modelComparisonTool.handler(
      { days: 28, region: "ALL", group: "planner" },
      ctx,
    )) as Answer;
    expect(planner.questions.map((q) => q.id)).toEqual(["plan_d0"]);
  });

  it("names the known questions for an unknown one", async () => {
    await expect(
      modelComparisonTool.handler(
        { days: 28, region: "ALL", question: "nope" },
        ctx,
      ),
    ).rejects.toThrow("live_h0_1, plan_d0");
  });
});

describe("model_comparison_day", () => {
  it("lists the options until park, ride and date are all given", async () => {
    await modelComparisonDayTool.handler(
      { origin: "morning", date: "2026-11-06" },
      ctx,
    );
    expect(service.getDayOptions).toHaveBeenCalledWith("2026-11-06");
    expect(service.getDay).not.toHaveBeenCalled();
  });

  it("reads the day with lowercased ids", async () => {
    const park = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA";
    const ride = "BBBBBBBB-BBBB-4BBB-8BBB-BBBBBBBBBBBB";
    await modelComparisonDayTool.handler(
      { date: "2026-11-06", park, ride, origin: "d1" },
      ctx,
    );
    expect(service.getDay).toHaveBeenCalledWith(
      park.toLowerCase(),
      "2026-11-06",
      ride.toLowerCase(),
      "d1",
    );
  });
});
