import {
  tallyLists,
  tallyTotals,
  type EventListSpec,
  type EventPropRow,
} from "./site-analytics-event-props";

/**
 * Parky's events as the frontend sends them (`SiteEventMap` in its
 * `lib/analytics/site-analytics.ts`, the list in its
 * docs/development/analytics.md): his window, the questions and their kind,
 * the reply chips, the interview and the changes he made to a planner. The
 * tracking audit's T22 and T23 add a chip's `position` and the chips'
 * `context`, the interview's `region`, a step's `outcome` and the end's
 * `plan`; a property the frontend does not send yet lists nothing.
 */
export const ASSISTANT_EVENTS = {
  opened: "assistant_opened",
  question: "assistant_question",
  answer: "assistant_answer",
  rated: "assistant_rated",
  repliesShown: "assistant_replies_shown",
  interviewStarted: "assistant_interview_started",
  interviewStep: "assistant_interview_step",
  interviewEnded: "assistant_interview_ended",
  planChanged: "assistant_plan_changed",
  changeUndone: "assistant_change_undone",
} as const;

const A = ASSISTANT_EVENTS;

/** The breakdowns of the report, by event and properties. */
export const ASSISTANT_LISTS = {
  openedBySource: { events: [A.opened], props: ["source"] },
  questionsByKind: { events: [A.question], props: ["kind"] },
  chipsByPosition: {
    events: [A.question],
    props: ["position"],
    where: { kind: "chip", position: "*" },
  },
  chipsShownByContext: {
    events: [A.repliesShown],
    props: ["context"],
    where: { context: "*" },
  },
  chipsShownByCount: { events: [A.repliesShown], props: ["count"] },
  interviewStarted: { events: [A.interviewStarted], props: ["kind", "source"] },
  interviewRegions: {
    events: [A.interviewStarted],
    props: ["kind", "region"],
    where: { region: "*" },
  },
  interviewSteps: {
    events: [A.interviewStep],
    props: ["kind", "step", "outcome"],
  },
  interviewEnded: {
    events: [A.interviewEnded],
    props: ["kind", "outcome", "plan"],
  },
  interviewStepsAnswered: {
    events: [A.interviewEnded],
    props: ["kind", "steps"],
  },
  planChanges: { events: [A.planChanged], props: ["planner", "change"] },
  undos: { events: [A.changeUndone], props: ["planner", "outcome"] },
  answers: { events: [A.answer], props: ["result"] },
  ratings: { events: [A.rated], props: ["rating"] },
} as const satisfies Record<string, EventListSpec>;
export type AssistantListKey = keyof typeof ASSISTANT_LISTS;

/** The counts outside the lists. */
export const ASSISTANT_TOTALS = {
  opened: { events: [A.opened], props: [] },
  questions: { events: [A.question], props: [] },
  chipsPressed: { events: [A.question], props: [], where: { kind: "chip" } },
  chipsShown: { events: [A.repliesShown], props: [] },
  interviewQuestions: {
    events: [A.question],
    props: [],
    where: { kind: "interview" },
  },
  tripInterviews: {
    events: [A.interviewStarted],
    props: [],
    where: { kind: "trip" },
  },
  tripInterviewsDone: {
    events: [A.interviewEnded],
    props: [],
    where: { kind: "trip", outcome: "done" },
  },
  tripInterviewsLeft: {
    events: [A.interviewEnded],
    props: [],
    where: { kind: "trip", outcome: "left" },
  },
  tripInterviewsWithPlan: {
    events: [A.interviewEnded],
    props: [],
    where: { kind: "trip", plan: "true" },
  },
  dayInterviews: {
    events: [A.interviewStarted],
    props: [],
    where: { kind: "day" },
  },
  dayInterviewsDone: {
    events: [A.interviewEnded],
    props: [],
    where: { kind: "day", outcome: "done" },
  },
  dayInterviewsLeft: {
    events: [A.interviewEnded],
    props: [],
    where: { kind: "day", outcome: "left" },
  },
  dayInterviewsWithPlan: {
    events: [A.interviewEnded],
    props: [],
    where: { kind: "day", plan: "true" },
  },
  stepsSkipped: {
    events: [A.interviewStep],
    props: [],
    where: { outcome: "skipped" },
  },
  stepsAnswered: {
    events: [A.interviewStep],
    props: [],
    where: { outcome: "answered" },
  },
  planChanged: { events: [A.planChanged], props: [] },
  changeUndone: {
    events: [A.changeUndone],
    props: [],
    where: { outcome: "undone" },
  },
  ratedUp: { events: [A.rated], props: [], where: { rating: "up" } },
  ratedDown: { events: [A.rated], props: [], where: { rating: "down" } },
} as const satisfies Record<string, EventListSpec>;
export type AssistantTotalKey = keyof typeof ASSISTANT_TOTALS;

export interface AssistantFigures {
  totals: Record<AssistantTotalKey | "chipsDrawn", number>;
  lists: Record<
    AssistantListKey,
    Array<Record<string, string> & { count: number }>
  >;
}

/**
 * The figures of Parky's report from his events' property counts: every
 * total and breakdown above, and the chips drawn (the sum of
 * `assistant_replies_shown`'s `count`).
 */
export function assistantFigures(
  rows: readonly EventPropRow[],
): AssistantFigures {
  const totals = tallyTotals(rows, ASSISTANT_TOTALS);
  const chipsDrawn = rows
    .filter((row) => row.name === A.repliesShown)
    .reduce((sum, row) => {
      const n = Number(row.props.count);
      return sum + (Number.isInteger(n) && n > 0 && n < 10 ? n * row.count : 0);
    }, 0);
  return {
    totals: { ...totals, chipsDrawn },
    lists: tallyLists(rows, ASSISTANT_LISTS),
  };
}
