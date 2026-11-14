import { z } from "zod";
import type { McpToolContext } from "../mcp-tool";
import {
  AssistantRecordsService,
  type ToolInputStats,
  type ToolStatRow,
} from "../../assistant/assistant-records.service";
import { assistantToolsReportTool } from "./assistant-tools-report.tool";
import { CHAT_DATA_DESCRIPTION, CHAT_DATA_NOTICE } from "./visitor-data";

const row = (
  tool: string,
  outcome: string,
  calls: number,
  region = "",
  promptVersion = 53,
): ToolStatRow => ({ tool, outcome, region, promptVersion, calls });

const ROWS: ToolStatRow[] = [
  row("plan_vacation", "ok", 30, "orlando"),
  row("plan_vacation", "refused", 6, "orlando"),
  row("plan_vacation", "invalid_input", 2, "japan"),
  row("change_trip", "ok", 12, "orlando"),
  row("change_trip", "invalid_input", 5, "orlando"),
  // A failure stored before the kinds were recorded.
  row("change_trip", "error", 1, "", 52),
  row("get_park_live", "ok", 3),
];

const INPUTS: ToolInputStats = {
  keptFrom: "2026-11-08",
  answers: 40,
  answersWithTools: 25,
  calls: 50,
  issues: [{ tool: "change_trip", path: "dates.from", count: 5 }],
  ignoredKeys: [
    { tool: "change_trip", key: "bases", count: 7 },
    { tool: "change_trip", key: "startBase", count: 2 },
  ],
  changeTripKeys: [
    { key: "region", count: 17 },
    { key: "bases", count: 7 },
  ],
  interviewOps: [
    { op: "start", outcome: "", count: 10 },
    { op: "answered", outcome: "", count: 24 },
    { op: "skip", outcome: "", count: 3 },
    { op: "end", outcome: "done", count: 6 },
    { op: "end", outcome: "left", count: 2 },
  ],
};

async function run(raw: Record<string, unknown>) {
  const records = {
    toolStats: jest.fn(async () => ROWS),
    toolInputStats: jest.fn(async () => INPUTS),
  };
  const ctx = {
    admin: {} as McpToolContext["admin"],
    get: (token: unknown) =>
      token === AssistantRecordsService ? records : undefined,
  } as unknown as McpToolContext;
  const input = z.object(assistantToolsReportTool.input).parse(raw);
  const answer = (await assistantToolsReportTool.handler(
    input as never,
    ctx,
  )) as Record<string, any>;
  return { answer, records };
}

describe("assistant_tools_report", () => {
  it("says what each outcome means and labels the chats' keys", () => {
    const text = assistantToolsReportTool.description;
    expect(text).toContain(CHAT_DATA_DESCRIPTION);
    for (const kind of ["invalid_input", "refused", "failed", "unknown_tool"]) {
      expect(text).toContain(kind);
    }
    const schema = z.object(assistantToolsReportTool.input);
    expect(schema.safeParse({ tool: "analytics_overview" }).success).toBe(
      false,
    );
    expect(schema.safeParse({ days: 91 }).success).toBe(false);
  });

  it("sums the calls per tool and outcome, folds the rare ones, and passes the window on", async () => {
    const { answer, records } = await run({
      days: 7,
      promptVersion: 53,
      tool: "change_trip",
    });
    const query = (records.toolStats.mock.calls[0] as unknown[])[0] as Record<
      string,
      unknown
    >;
    expect(query).toMatchObject({ promptVersion: 53, tool: "change_trip" });
    expect(query.tools).toEqual(expect.arrayContaining(["plan_vacation"]));
    expect(query.regions).toEqual(expect.arrayContaining(["orlando"]));

    expect(answer.totals).toMatchObject({
      calls: 59,
      errors: 14,
      errorRate: 0.237,
      ok: 45,
      refused: 6,
      invalid_input: 7,
      failed: 0,
      error: 1,
    });
    expect(answer.tools).toEqual([
      expect.objectContaining({
        tool: "plan_vacation",
        calls: 38,
        errors: 8,
        errorRate: 0.211,
        refused: 6,
        invalid_input: 2,
      }),
      expect.objectContaining({ tool: "change_trip", calls: 18, error: 1 }),
      // Three calls are too few to name.
      expect.objectContaining({ tool: "(other)", calls: 3, errorRate: null }),
    ]);
    expect(answer.regions).toEqual([
      expect.objectContaining({
        region: "orlando",
        tool: "plan_vacation",
        calls: 36,
      }),
      expect.objectContaining({
        region: "orlando",
        tool: "change_trip",
        calls: 17,
      }),
      expect.objectContaining({ region: "(other)", tool: "(other)", calls: 2 }),
    ]);
    expect(answer.promptVersions).toEqual([
      expect.objectContaining({ promptVersion: 53, calls: 58 }),
      expect.objectContaining({ promptVersion: "(other)", calls: 1 }),
    ]);
  });

  it("reads the kept chats' answers, the interview's completion and the dropped keys", async () => {
    const { answer } = await run({});
    expect(answer.kept).toEqual({
      from: "2026-11-08",
      answers: 40,
      answersWithTools: 25,
      calls: 50,
      callsPerAnswer: 1.25,
    });
    expect(answer.interview).toEqual({
      started: 10,
      answered: 24,
      skipped: 3,
      asked: 0,
      endedDone: 6,
      endedLeft: 2,
      completion: 0.6,
    });
    expect(answer.visitorDataNotice).toBe(CHAT_DATA_NOTICE);
    expect(answer.visitorData.ignoredKeys).toEqual([
      { tool: "change_trip", key: "bases", count: 7 },
      { tool: "(other)", key: "(other)", count: 2 },
    ]);
    expect(answer.visitorData.issues).toEqual([
      { tool: "change_trip", path: "dates.from", count: 5 },
    ]);
  });
});
