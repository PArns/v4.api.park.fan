import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsAssistantService } from "../../site-analytics/site-analytics-assistant.service";
import {
  ASSISTANT_LISTS,
  type AssistantListKey,
} from "../../site-analytics/site-analytics-assistant";
import {
  HOURLY_FROM_DESCRIPTION,
  eventWindowInput,
  ratio,
  siteQuery,
  windowOf,
} from "./site-analytics.inputs";
import {
  SMALL_CELLS_DESCRIPTION,
  foldSmallRows,
  hideSmallInSeries,
  isNarrowed,
} from "./small-cells";
import { VISITOR_DATA_DESCRIPTION, withVisitorData } from "./visitor-data";

export const assistantReportTool = defineMcpTool({
  name: "assistant_report",
  title: "Parky's chats, from the site statistic",
  description:
    "How visitors use Parky, park.fan's chat assistant, from his events in the site statistic: " +
    "his window opened (by source), questions by kind (suggestion, typed, chip, interview), the " +
    "reply chips (shown: times chips were drawn under an answer; drawn: how many chips; pressed: " +
    "questions of kind chip; pressRate = pressed / shown; by position and by context once the " +
    "frontend sends them), the interview per kind (trip or day): started by source, steps by " +
    "step and outcome (answered or skipped, once sent), ended by outcome (done or left) and " +
    "whether a plan stood (plan, once sent), with completion = ended done / started and " +
    "leftRate; his plan changes by planner and kind, undos, answers by result and ratings. " +
    "Counts are events, not visitors. The server's own record of his tool calls, with their " +
    "errors, is assistant_tools_report; a chat in full, assistant_conversation. UTC days; up to " +
    "six minutes behind the site; Do Not Track, GPC and opted-out readers are missing. " +
    VISITOR_DATA_DESCRIPTION +
    " " +
    SMALL_CELLS_DESCRIPTION +
    " " +
    HOURLY_FROM_DESCRIPTION,
  input: {
    ...eventWindowInput,
    limit: z
      .number()
      .int()
      .min(1)
      .max(100)
      .default(20)
      .describe("Entries per breakdown list"),
  },
  async handler({ limit, ...input }, ctx) {
    const report = await ctx
      .get(SiteAnalyticsAssistantService)
      .report(siteQuery(input));
    const narrowed = isNarrowed(input);
    const keys = Object.keys(report.totals);
    const masked = narrowed
      ? hideSmallInSeries([report.totals], keys)
      : { series: [report.totals], hidden: false };
    const t = masked.series[0] as Record<string, number | null>;
    const interview = (kind: "trip" | "day") => {
      const started = t[`${kind}Interviews`];
      return {
        started,
        endedDone: t[`${kind}InterviewsDone`],
        endedLeft: t[`${kind}InterviewsLeft`],
        withPlan: t[`${kind}InterviewsWithPlan`],
        completion: ratio(t[`${kind}InterviewsDone`], started),
        leftRate: ratio(t[`${kind}InterviewsLeft`], started),
      };
    };
    const list = (key: AssistantListKey) =>
      foldSmallRows(report.lists[key].slice(0, limit), {
        dims: [...ASSISTANT_LISTS[key].props],
        count: "count",
        sum: ["count"],
      });
    return withVisitorData(
      {
        ...windowOf(report),
        opened: t.opened,
        questions: t.questions,
        chips: {
          shown: t.chipsShown,
          drawn: t.chipsDrawn,
          pressed: t.chipsPressed,
          pressRate: ratio(t.chipsPressed, t.chipsShown),
        },
        interview: {
          questions: t.interviewQuestions,
          stepsAnswered: t.stepsAnswered,
          stepsSkipped: t.stepsSkipped,
          trip: interview("trip"),
          day: interview("day"),
        },
        planChanged: t.planChanged,
        changeUndone: t.changeUndone,
        ratings: { up: t.ratedUp, down: t.ratedDown },
        ...(narrowed && { smallValuesHidden: masked.hidden }),
      },
      {
        openedBySource: list("openedBySource"),
        questionsByKind: list("questionsByKind"),
        chips: {
          byPosition: list("chipsByPosition"),
          shownByContext: list("chipsShownByContext"),
          shownByCount: list("chipsShownByCount"),
        },
        interview: {
          started: list("interviewStarted"),
          regions: list("interviewRegions"),
          steps: list("interviewSteps"),
          ended: list("interviewEnded"),
          stepsAnswered: list("interviewStepsAnswered"),
        },
        planChanges: list("planChanges"),
        undos: list("undos"),
        answers: list("answers"),
        ratings: list("ratings"),
      },
    );
  },
});
