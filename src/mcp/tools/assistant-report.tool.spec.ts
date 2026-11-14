import { z } from "zod";
import type { McpToolContext } from "../mcp-tool";
import { SiteAnalyticsAssistantService } from "../../site-analytics/site-analytics-assistant.service";
import {
  ASSISTANT_EVENTS as A,
  assistantFigures,
} from "../../site-analytics/site-analytics-assistant";
import type { EventPropRow } from "../../site-analytics/site-analytics-event-props";
import { assistantReportTool } from "./assistant-report.tool";
import { SMALL_CELLS_DESCRIPTION } from "./small-cells";
import { VISITOR_DATA_DESCRIPTION, VISITOR_DATA_NOTICE } from "./visitor-data";

const ev = (
  name: string,
  props: Record<string, string>,
  count: number,
): EventPropRow => ({ name, props, count });

const ROWS: EventPropRow[] = [
  ev(A.opened, { source: "button" }, 30),
  ev(A.opened, { source: "interview" }, 2),
  ev(A.question, { kind: "typed" }, 40),
  ev(A.question, { kind: "chip", position: "1" }, 9),
  ev(A.question, { kind: "chip" }, 3),
  ev(A.question, { kind: "interview" }, 12),
  ev(A.repliesShown, { count: "3" }, 20),
  ev(A.repliesShown, { count: "4", context: "interview" }, 5),
  ev(A.interviewStarted, { kind: "trip", source: "planner" }, 10),
  ev(A.interviewStep, { kind: "trip", step: "dates", outcome: "answered" }, 9),
  ev(A.interviewStep, { kind: "trip", step: "party", outcome: "skipped" }, 4),
  ev(
    A.interviewEnded,
    { kind: "trip", outcome: "done", steps: "4", plan: "true" },
    6,
  ),
  ev(A.interviewEnded, { kind: "trip", outcome: "left", steps: "1" }, 3),
  ev(A.planChanged, { planner: "trip", change: "dates" }, 7),
  ev(A.changeUndone, { planner: "trip", outcome: "undone" }, 1),
  ev(A.rated, { rating: "up" }, 8),
];

const WINDOW = {
  from: "2026-11-08",
  to: "2026-11-14",
  days: 7,
  interval: "day",
  locale: null,
  filters: [],
  datacenter: "exclude",
  hourlyFrom: null,
};

async function run(raw: Record<string, unknown>) {
  const service = {
    report: jest.fn(async () => ({ ...WINDOW, ...assistantFigures(ROWS) })),
  };
  const ctx = {
    admin: {} as McpToolContext["admin"],
    get: (token: unknown) =>
      token === SiteAnalyticsAssistantService ? service : undefined,
  } as unknown as McpToolContext;
  const input = z.object(assistantReportTool.input).parse(raw);
  return (await assistantReportTool.handler(input as never, ctx)) as Record<
    string,
    any
  >;
}

describe("assistant_report", () => {
  it("labels the visitor strings and says how small values fold", () => {
    expect(assistantReportTool.description).toContain(VISITOR_DATA_DESCRIPTION);
    expect(assistantReportTool.description).toContain(SMALL_CELLS_DESCRIPTION);
    expect(assistantReportTool.description).toContain("assistant_tools_report");
  });

  it("counts the chips drawn, shown and pressed, and the interview's completion", async () => {
    const answer = await run({ days: 7 });
    expect(answer).toMatchObject({
      opened: 32,
      questions: 64,
      chips: { shown: 25, drawn: 80, pressed: 12, pressRate: 0.48 },
      interview: {
        questions: 12,
        stepsAnswered: 9,
        stepsSkipped: 4,
        trip: {
          started: 10,
          endedDone: 6,
          endedLeft: 3,
          withPlan: 6,
          completion: 0.6,
          leftRate: 0.3,
        },
        day: { started: 0, completion: null },
      },
      planChanged: 7,
      ratings: { up: 8, down: 0 },
    });
    expect(answer).not.toHaveProperty("smallValuesHidden");
    expect(answer.visitorDataNotice).toBe(VISITOR_DATA_NOTICE);
    const data = answer.visitorData;
    expect(data.openedBySource).toEqual([
      { source: "button", count: 30 },
      { source: "(other)", count: 2 },
    ]);
    // A chip's position and the chips' context, where the frontend sent them.
    expect(data.chips.byPosition).toEqual([{ position: "1", count: 9 }]);
    expect(data.chips.shownByContext).toEqual([
      { context: "interview", count: 5 },
    ]);
    expect(data.interview.ended).toEqual([
      { kind: "trip", outcome: "done", plan: "true", count: 6 },
      { kind: "(other)", outcome: "(other)", plan: "(other)", count: 3 },
    ]);
  });

  it("hides small totals and the rates worked out from them under a filter", async () => {
    const answer = await run({ filters: [{ dim: "country", value: "lu" }] });
    expect(answer).toMatchObject({
      chips: { pressed: 12 },
      changeUndone: null,
      interview: { trip: { endedLeft: null, leftRate: null } },
      smallValuesHidden: true,
    });
  });
});
