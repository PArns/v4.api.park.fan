/**
 * Parky's prompt eval: each case is a short chat sent to the real model with the real tools,
 * which are never run; canned results stand in where a case needs a second step. It checks
 * which tool Parky reaches for first, not how he words the answer.
 *
 *   ANTHROPIC_API_KEY=… pnpm assistant:eval [--prompt-ref <git ref>] [--only <case id>]
 *
 * `--prompt-ref` takes the system prompt from another commit (the tools stay this checkout's),
 * so two prompt versions can be scored side by side.
 */

import { execFileSync } from "node:child_process";
import Anthropic from "@anthropic-ai/sdk";
import { ASSISTANT_MODEL } from "./assistant-chat.service";
import { chatContext, SYSTEM_PROMPT } from "./system-prompt";
import { visitorTrips } from "./tools/assistant-plans";
import { ASSISTANT_TOOL_PARAMS } from "./tools/assistant-tools";
import { EVAL_CASES, type EvalCase } from "./assistant-eval.fixtures";

/**
 * Tools that change the page, the plan, the alerts or the chat: what "actions" forbids.
 * `plan_vacation` only reads, whatever its prefix says.
 */
export const ACTION_TOOLS = ASSISTANT_TOOL_PARAMS.map((t) => t.name).filter(
  (name) =>
    (name.startsWith("plan_") && name !== "plan_vacation") ||
    [
      "open_page",
      "set_ride_alert",
      "set_show_reminder",
      "remove_alert",
      "set_favorite",
      "switch_language",
      "submit_feedback",
      "end_chat",
    ].includes(name),
);

export interface ToolCall {
  name: string;
  input: Record<string, unknown>;
}

/** `"*"` in an expected input means the key only has to be there. */
function matches(expected: unknown, actual: unknown): boolean {
  if (expected === "*") return actual !== undefined && actual !== null;
  if (Array.isArray(expected)) {
    return (
      Array.isArray(actual) &&
      expected.length === actual.length &&
      expected.every((value, i) => matches(value, actual[i]))
    );
  }
  if (expected && typeof expected === "object") {
    if (!actual || typeof actual !== "object") return false;
    return Object.entries(expected).every(([key, value]) =>
      matches(value, (actual as Record<string, unknown>)[key]),
    );
  }
  return expected === actual;
}

/** Why a case failed, or null when it passed. `answer` is the final text, where a case checks it. */
export function judge(
  testCase: EvalCase,
  calls: ToolCall[],
  answer = "",
): string | null {
  const forbidden =
    testCase.forbid === "any"
      ? calls
      : calls.filter((call) =>
          (testCase.forbid === "actions"
            ? ACTION_TOOLS
            : (testCase.forbid ?? [])
          ).includes(call.name),
        );
  if (forbidden.length) return `called ${forbidden[0].name}`;
  const counted = calls.filter(
    (call) => !testCase.allowBefore?.includes(call.name),
  );
  for (const [i, want] of (testCase.want ?? []).entries()) {
    const call = counted[i];
    const names = [want.tool].flat();
    if (!call) return `no ${names.join("/")} call`;
    if (!names.includes(call.name)) {
      return `called ${call.name}, wanted ${names.join("/")}`;
    }
    if (want.input && !matches(want.input, call.input)) {
      return `${call.name} with ${JSON.stringify(call.input)}`;
    }
  }
  for (const rule of testCase.answer ?? []) {
    if (rule.when && !new RegExp(rule.when, "i").test(answer)) continue;
    if (!new RegExp(rule.must, "i").test(answer)) {
      return `answer without /${rule.must}/${rule.when ? ` though it says /${rule.when}/` : ""}`;
    }
  }
  return null;
}

/** The system prompt as it stood at a git ref. */
export function promptAt(ref: string): string {
  const source = execFileSync(
    "git",
    ["show", `${ref}:src/assistant/system-prompt.ts`],
    { encoding: "utf8" },
  );
  const start = source.indexOf("export const SYSTEM_PROMPT = `");
  const end = source.indexOf("`;", start);
  if (start < 0 || end < 0) throw new Error(`No SYSTEM_PROMPT at ${ref}`);
  return source.slice(start + "export const SYSTEM_PROMPT = `".length, end);
}

/** Sends one case, feeding canned results until it has as many calls as it checks. */
async function run(
  client: Anthropic,
  prompt: string,
  testCase: EvalCase,
): Promise<{ calls: ToolCall[]; answer: string }> {
  const system: Anthropic.TextBlockParam[] = [
    { type: "text", text: prompt, cache_control: { type: "ephemeral" } },
    {
      type: "text",
      text: chatContext({
        locale: testCase.locale,
        now: testCase.now ?? "2026-11-07 10:15",
        timeZone: "Europe/Berlin",
        pagePath: testCase.pagePath,
        pageSubject: testCase.pageSubject,
        planner: testCase.planner,
        trips: visitorTrips(testCase.trips),
        tripDigest: testCase.tripDigest,
        interview: testCase.interview,
        alerts: testCase.alerts,
        favorites: testCase.favorites,
      }),
    },
  ];
  const messages: Anthropic.MessageParam[] = [
    ...(testCase.history ?? []).map(
      ([role, content]): Anthropic.MessageParam => ({ role, content }),
    ),
    { role: "user", content: testCase.message },
  ];
  const calls: ToolCall[] = [];
  let answer = "";
  // A case that checks the answer gets one more round to write it.
  const rounds =
    Math.min(
      3,
      (testCase.want?.length ?? 0) + (testCase.allowBefore?.length ?? 0) || 1,
    ) + (testCase.answer ? 1 : 0);
  for (let round = 0; round < rounds; round++) {
    const response = await client.messages.create({
      model: ASSISTANT_MODEL,
      max_tokens: 2048,
      output_config: { effort: "low" },
      system,
      tools: ASSISTANT_TOOL_PARAMS,
      tool_choice: { type: "auto" },
      messages,
    });
    const uses = response.content.filter(
      (block): block is Anthropic.ToolUseBlock => block.type === "tool_use",
    );
    answer = response.content
      .filter((block): block is Anthropic.TextBlock => block.type === "text")
      .map((block) => block.text)
      .join("\n");
    if (!uses.length) break;
    calls.push(
      ...uses.map((use) => ({
        name: use.name,
        input: use.input as Record<string, unknown>,
      })),
    );
    messages.push(
      { role: "assistant", content: response.content },
      {
        role: "user",
        content: uses.map((use) => ({
          type: "tool_result" as const,
          tool_use_id: use.id,
          content: JSON.stringify(
            testCase.results?.[use.name] ?? { note: "No data in this test." },
          ),
        })),
      },
    );
  }
  return { calls, answer };
}

async function main(): Promise<void> {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) {
    console.error("Set ANTHROPIC_API_KEY to run the eval.");
    process.exit(2);
  }
  const arg = (flag: string) => {
    const i = process.argv.indexOf(flag);
    return i > 0 ? process.argv[i + 1] : undefined;
  };
  const ref = arg("--prompt-ref");
  const only = arg("--only");
  const prompt = ref ? promptAt(ref) : SYSTEM_PROMPT;
  const client = new Anthropic({ apiKey, maxRetries: 2 });

  const probe: Anthropic.MessageParam[] = [{ role: "user", content: "hi" }];
  const [alone, withTools] = await Promise.all([
    client.messages.countTokens({
      model: ASSISTANT_MODEL,
      system: prompt,
      messages: probe,
    }),
    client.messages.countTokens({
      model: ASSISTANT_MODEL,
      system: prompt,
      tools: ASSISTANT_TOOL_PARAMS,
      messages: probe,
    }),
  ]);
  console.log(
    `Prompt ${ref ?? "this checkout"}: ${alone.input_tokens} tokens, ${withTools.input_tokens} with the tools`,
  );

  const cases = EVAL_CASES.filter((c) => !only || c.id === only);
  let passed = 0;
  const rows: string[] = [];
  for (const testCase of cases) {
    let verdict: string | null;
    let calls: ToolCall[] = [];
    try {
      const result = await run(client, prompt, testCase);
      calls = result.calls;
      verdict = judge(testCase, calls, result.answer);
    } catch (error) {
      verdict = `error: ${(error as Error).message}`;
    }
    if (!verdict) passed++;
    rows.push(
      [
        verdict ? "FAIL" : "pass",
        testCase.id.padEnd(28),
        (calls.map((c) => c.name).join(" > ") || "(no tool)").padEnd(40),
        verdict ?? "",
      ].join("  "),
    );
  }
  console.log(rows.join("\n"));
  console.log(`\n${passed}/${cases.length} passed`);
  process.exit(passed === cases.length ? 0 : 1);
}

if (require.main === module) void main();
