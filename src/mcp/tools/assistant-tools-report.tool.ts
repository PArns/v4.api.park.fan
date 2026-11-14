import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import {
  AssistantRecordsService,
  CONVERSATION_RETENTION_DAYS,
  type ToolStatRow,
} from "../../assistant/assistant-records.service";
import { ASSISTANT_TOOL_ERROR_KINDS } from "../../assistant/entities/assistant-message.entity";
import { ASSISTANT_TOOLS } from "../../assistant/tools/assistant-tools";
import { TRIP_REGIONS } from "../../trip-regions/regions";
import { round3 } from "./site-analytics.inputs";
import { foldSmallRows } from "./small-cells";
import { utcDaysInput, utcDaysWindow } from "./utc-window";
import {
  CHAT_DATA_DESCRIPTION,
  CHAT_DATA_NOTICE,
  withVisitorData,
} from "./visitor-data";

const TOOL_NAMES = ASSISTANT_TOOLS.map((tool) => tool.name) as [
  string,
  ...string[],
];

/** The tool sums are kept for good; a window is still a window. */
const LIMITS = { maxDays: 90, defaultDays: 7 };

/** Every outcome a call can have, in the order the report lists them. */
const OUTCOMES = ["ok", ...ASSISTANT_TOOL_ERROR_KINDS, "error"] as const;
type Outcome = (typeof OUTCOMES)[number];
type OutcomeCounts = Record<Outcome, number>;

const empty = (): OutcomeCounts =>
  Object.fromEntries(OUTCOMES.map((o) => [o, 0])) as OutcomeCounts;

/** Calls and errors of a group of rows, with the error rate and the count per outcome. */
function summary(rows: readonly ToolStatRow[]) {
  const outcomes = empty();
  for (const row of rows) {
    const outcome = (OUTCOMES as readonly string[]).includes(row.outcome)
      ? (row.outcome as Outcome)
      : "error";
    outcomes[outcome] += row.calls;
  }
  const calls = rows.reduce((sum, row) => sum + row.calls, 0);
  const errors = calls - outcomes.ok;
  return {
    calls,
    errors,
    errorRate: calls > 0 ? round3(errors / calls) : null,
    ...outcomes,
  };
}

/** Rows grouped by a key, each group summarised, most calls first. */
function groupedBy<K extends string>(
  rows: readonly ToolStatRow[],
  key: (row: ToolStatRow) => Record<K, string | number>,
) {
  const groups = new Map<
    string,
    { head: Record<K, string | number>; rows: ToolStatRow[] }
  >();
  for (const row of rows) {
    const head = key(row);
    const k = JSON.stringify(head);
    const group = groups.get(k) ?? { head, rows: [] };
    group.rows.push(row);
    groups.set(k, group);
  }
  return [...groups.values()]
    .map((group) => ({ ...group.head, ...summary(group.rows) }))
    .sort(
      (a, b) =>
        b.calls - a.calls || JSON.stringify(a).localeCompare(JSON.stringify(b)),
    );
}

const SUMMED = ["calls", "errors", ...OUTCOMES];

export const assistantToolsReportTool = defineMcpTool({
  name: "assistant_tools_report",
  title: "Parky's tool calls",
  description:
    "Parky's tool calls across all chats, over a window of UTC days: per tool the calls, errors " +
    "and error rate, and the calls per outcome: ok, invalid_input (the schema refused the " +
    "model's input), refused (the tool said no: no such park, the plan route's 400), failed (an " +
    "error of ours), unknown_tool, and error for a failure stored before the kinds were. Then " +
    "per trip region the input named, per prompt version, and, from the chats still kept " +
    `(${CONVERSATION_RETENTION_DAYS} days; keptFrom says from when): answers and those with ` +
    "tool calls, calls per answer, the paths a schema refused (issues), the keys it dropped " +
    "without a word (ignoredKeys: a parameter the tool does not have), which keys change_trip " +
    "was given, and the interview's ops (start, answered, skip, ask, end with done or left) with " +
    "its completion. The counts beyond the kept chats come from daily sums the nightly prune " +
    "keeps. Lists fold calls counted fewer than 5 times into (other). Keys and paths never carry " +
    "a value. " +
    CHAT_DATA_DESCRIPTION,
  input: {
    ...utcDaysInput(LIMITS),
    tool: z.enum(TOOL_NAMES).optional().describe("Only this tool's calls"),
    promptVersion: z
      .number()
      .int()
      .min(0)
      .max(32767)
      .optional()
      .describe(
        "Only answers written with this version of Parky's prompt; 0 for answers without one",
      ),
    limit: z
      .number()
      .int()
      .min(1)
      .max(100)
      .default(30)
      .describe("Entries per list"),
  },
  async handler({ days, from, to, tool, promptVersion, limit }, ctx) {
    const window = utcDaysWindow({ days, from, to }, LIMITS);
    const records = ctx.get(AssistantRecordsService);
    const query = {
      ...window,
      tools: TOOL_NAMES,
      regions: Object.keys(TRIP_REGIONS),
      ...(tool !== undefined && { tool }),
      ...(promptVersion !== undefined && { promptVersion }),
    };
    const [rows, inputs] = await Promise.all([
      records.toolStats(query),
      records.toolInputStats(query),
    ]);
    const fold = <R extends object>(
      list: readonly R[],
      dims: string[],
      count: string,
      sum: string[],
    ) => foldSmallRows(list.slice(0, limit), { dims, count, sum });

    const ops = new Map<string, number>();
    for (const row of inputs.interviewOps) {
      const k = row.op === "end" ? `end:${row.outcome || "unknown"}` : row.op;
      ops.set(k, (ops.get(k) ?? 0) + row.count);
    }
    const started = ops.get("start") ?? 0;
    const done = ops.get("end:done") ?? 0;

    return withVisitorData(
      {
        ...window,
        tool: tool ?? null,
        promptVersion: promptVersion ?? null,
        totals: summary(rows),
        tools: fold(
          groupedBy(rows, (row) => ({ tool: row.tool })),
          ["tool"],
          "calls",
          SUMMED,
        ),
        regions: fold(
          groupedBy(
            rows.filter((row) => row.region !== ""),
            (row) => ({ region: row.region, tool: row.tool }),
          ),
          ["region", "tool"],
          "calls",
          SUMMED,
        ),
        promptVersions: fold(
          groupedBy(rows, (row) => ({ promptVersion: row.promptVersion })),
          ["promptVersion"],
          "calls",
          SUMMED,
        ),
        kept: {
          from: inputs.keptFrom,
          answers: inputs.answers,
          answersWithTools: inputs.answersWithTools,
          calls: inputs.calls,
          callsPerAnswer:
            inputs.answers > 0 ? round3(inputs.calls / inputs.answers) : null,
        },
        interview: {
          started,
          answered: ops.get("answered") ?? 0,
          skipped: ops.get("skip") ?? 0,
          asked: ops.get("ask") ?? 0,
          endedDone: done,
          endedLeft: ops.get("end:left") ?? 0,
          completion: started > 0 ? round3(Math.min(1, done / started)) : null,
        },
      },
      {
        issues: fold(inputs.issues, ["tool", "path"], "count", ["count"]),
        ignoredKeys: fold(inputs.ignoredKeys, ["tool", "key"], "count", [
          "count",
        ]),
        changeTripKeys: fold(inputs.changeTripKeys, ["key"], "count", [
          "count",
        ]),
      },
      CHAT_DATA_NOTICE,
    );
  },
});
