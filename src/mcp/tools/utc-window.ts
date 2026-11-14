import { z } from "zod";
import { McpToolError } from "../mcp-tool";
import { addIsoDays } from "../../common/utils/date.util";
import { utcDay } from "../../common/utils/reader-hash.util";
import { DAY_PATTERN } from "../../site-analytics/site-analytics.query";

/**
 * A window of UTC days for the tools over the server's own records (the trip
 * routes' metric rows, Parky's tool calls): the last `days`, today included,
 * or `from`–`to`, never before the oldest day the records reach.
 */
export function utcDaysWindow(
  input: { days?: number; from?: string; to?: string },
  limits: { maxDays: number; defaultDays: number },
  now = new Date(),
): { from: string; to: string } {
  const today = utcDay(now);
  const oldest = addIsoDays(today, -(limits.maxDays - 1));
  if (input.from || input.to) {
    if (!input.from || !input.to || input.days !== undefined) {
      throw new McpToolError("Give from and to together, or days alone");
    }
    if (input.from > input.to) throw new McpToolError("from is after to");
    if (input.to > today) {
      throw new McpToolError(`to is after today (${today}, UTC)`);
    }
    if (input.from < oldest) {
      throw new McpToolError(
        `The window reaches back ${limits.maxDays} days: from is ${oldest} at the earliest`,
      );
    }
    return { from: input.from, to: input.to };
  }
  return {
    from: addIsoDays(today, -((input.days ?? limits.defaultDays) - 1)),
    to: today,
  };
}

/** The `days`, `from` and `to` inputs of such a tool. */
export function utcDaysInput(limits: { maxDays: number; defaultDays: number }) {
  return {
    days: z
      .number()
      .int()
      .min(1)
      .max(limits.maxDays)
      .optional()
      .describe(
        `UTC days back from today, today included; ${limits.defaultDays} when neither days nor from/to is given`,
      ),
    from: z
      .string()
      .regex(DAY_PATTERN)
      .optional()
      .describe("First UTC day (YYYY-MM-DD) of a fixed window, with to"),
    to: z
      .string()
      .regex(DAY_PATTERN)
      .optional()
      .describe("Last UTC day (YYYY-MM-DD), with from; today at the latest"),
  };
}
