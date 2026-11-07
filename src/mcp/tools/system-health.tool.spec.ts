import type { McpToolContext } from "../mcp-tool";
import { SystemHealthService } from "../../admin/system-health.service";
import { systemHealthTool } from "./system-health.tool";

const board = () => ({
  rows: Array.from({ length: 400 }, (_, i) => ({ n: i })),
  verdict: [{ segment: "all", pcnWins: true }],
  count: 400,
});

function ctx(): McpToolContext {
  const health = {
    timestamp: "2026-11-07T09:00:00.000Z",
    host: { cpu: 1 },
    ml: { catboost: {}, pcnComparison: board(), pcnLeadCurve: board() },
  };
  return {
    admin: {},
    get: (token: unknown) =>
      token === SystemHealthService ? { getHealth: async () => health } : null,
  } as unknown as McpToolContext;
}

describe("system_health", () => {
  it("keeps the PCN boards' verdict but none of their day rows unless asked", async () => {
    const { ml } = (await systemHealthTool.handler(
      { section: "ml", boardRows: 0 },
      ctx(),
    )) as { ml: Record<string, ReturnType<typeof board>> };
    expect(ml.pcnComparison).toMatchObject({ rows: [], count: 400 });
    expect(ml.pcnLeadCurve.verdict).toHaveLength(1);
  });

  it("keeps the newest rows asked for", async () => {
    const { ml } = (await systemHealthTool.handler(
      { section: "all", boardRows: 2 },
      ctx(),
    )) as { ml: Record<string, ReturnType<typeof board>> };
    expect(ml.pcnComparison.rows).toEqual([{ n: 0 }, { n: 1 }]);
  });

  it("leaves the ML half out of the infrastructure answer", async () => {
    const answer = await systemHealthTool.handler(
      { section: "infrastructure", boardRows: 0 },
      ctx(),
    );
    expect(answer).toEqual({ host: { cpu: 1 } });
  });
});
