import {
  ModelComparisonCellDto,
  ModelComparisonResponseDto,
} from "../dto/model-comparison.dto";
import {
  DecisionFile,
  LATE_AFTER_MS,
  buildDecisions,
  componentStatus,
  decisionProgress,
  deriveDecisionState,
  earliestFromProgress,
} from "./ml-overview.util";
import decisionsJson from "../data/ml-decisions.json";
import {
  COMPARISON_MODELS,
  COMPARISON_QUESTIONS,
} from "./model-comparison.util";

const TODAY = "2026-10-11";

function cell(
  over: Partial<ModelComparisonCellDto> & {
    days?: number;
    parks?: number;
  } = {},
): ModelComparisonCellDto {
  const { days, parks, ...rest } = over;
  return {
    state: "pending",
    note: "",
    checks: { offline: "yes", live: "running", visible: false },
    live:
      days === undefined
        ? null
        : {
            diff: -1,
            ciLow: -2,
            ciHigh: 0.5,
            days,
            parks: parks ?? 0,
            n: 100,
            modelMae: 10,
            referenceMae: 11,
            since: "2026-10-10",
          },
    offline: null,
    expectedBy: null,
    ...rest,
  };
}

function matrix(
  cells: Record<string, Record<string, ModelComparisonCellDto>>,
): ModelComparisonResponseDto {
  return {
    generatedAt: "2026-10-11T12:00:00Z",
    window: { days: 28, firstDate: null, lastDate: null },
    rule: { minDays: 14, minParks: 10 },
    models: [],
    groups: [],
    questions: Object.entries(cells).map(([id, c]) => ({
      id,
      group: "planner",
      label: id,
      sub: "",
      leadLabel: "",
      servedModel: "served",
      metric: { unit: "min", lowerIsBetter: true },
      cells: c,
    })),
    summary: { better: 1, pending: 2, unclear: 3, worse: 4 },
  };
}

describe("deriveDecisionState", () => {
  it("is ready only when every cell is better with offline and live green", () => {
    const green = cell({
      state: "better",
      checks: { offline: "yes", live: "yes", visible: true },
      days: 20,
      parks: 30,
    });
    expect(deriveDecisionState([green, green])).toBe("ready");
    // One cell still measuring → waiting, not ready.
    expect(deriveDecisionState([green, cell({ days: 3, parks: 12 })])).toBe(
      "waiting_live",
    );
    // Better live but the offline check is not green → not ready.
    expect(
      deriveDecisionState([
        cell({
          state: "better",
          checks: { offline: "no", live: "yes", visible: true },
        }),
      ]),
    ).toBe("open");
  });

  it("waits on live when offline is better (or still running) and the gate is not met", () => {
    expect(deriveDecisionState([cell()])).toBe("waiting_live");
    expect(
      deriveDecisionState([
        cell({
          checks: { offline: "running", live: "running", visible: false },
        }),
      ]),
    ).toBe("waiting_live");
  });

  it("is open when offline shows no sure win, or live has already said no", () => {
    expect(
      deriveDecisionState([
        cell({
          state: "unclear",
          checks: { offline: "no", live: "running", visible: false },
        }),
      ]),
    ).toBe("open");
    expect(
      deriveDecisionState([
        cell({
          state: "unclear",
          checks: { offline: "yes", live: "no", visible: false },
        }),
      ]),
    ).toBe("open");
    expect(deriveDecisionState([])).toBe("open");
  });
});

describe("decisionProgress", () => {
  it("takes the least-measured cell, 0 where a cell has no live figure", () => {
    expect(
      decisionProgress([
        cell({ days: 5, parks: 30 }),
        cell({ days: 9, parks: 4 }),
      ]),
    ).toEqual({ liveDays: 5, liveParks: 4, needDays: 14, needParks: 10 });
    expect(decisionProgress([cell({ days: 5, parks: 30 }), cell()])).toEqual({
      liveDays: 0,
      liveParks: 0,
      needDays: 14,
      needParks: 10,
    });
    expect(decisionProgress([])).toBeNull();
  });
});

describe("earliestFromProgress", () => {
  const p = (liveDays: number, liveParks: number) => ({
    liveDays,
    liveParks,
    needDays: 14,
    needParks: 10,
  });

  it("is today plus the missing days", () => {
    expect(earliestFromProgress(p(1, 40), TODAY)).toEqual({
      date: "2026-10-24",
      label: "frühestens 24.10.",
    });
    expect(earliestFromProgress(p(0, 0), TODAY)?.date).toBe("2026-10-25");
    // Across a month boundary, on the calendar, not by milliseconds.
    expect(earliestFromProgress(p(0, 0), "2026-10-25")?.date).toBe(
      "2026-11-08",
    );
  });

  it("is the next scoring when only parks are missing, null when the gate is met", () => {
    expect(earliestFromProgress(p(20, 4), TODAY)).toEqual({
      date: "2026-10-12",
      label: "frühestens 12.10. (es fehlen noch Parks)",
    });
    expect(earliestFromProgress(p(20, 12), TODAY)).toBeNull();
    expect(earliestFromProgress(null, TODAY)).toBeNull();
  });
});

describe("buildDecisions", () => {
  const file: DecisionFile = {
    decisions: [
      {
        id: "a",
        title: "A",
        detail: "a",
        cells: [
          { questionId: "plan_d0_1", modelId: "h5" },
          { questionId: "plan_d0_1", modelId: "served" },
        ],
      },
      {
        id: "b",
        title: "B",
        detail: "b",
        cells: [{ questionId: "plan_d2_5", modelId: "chronos" }],
      },
      {
        id: "later",
        title: "L",
        detail: "l",
        state: "later",
        dependsOn: ["a", "b"],
        cells: [{ questionId: "plan_d0_1", modelId: "h5" }],
      },
      { id: "owner", title: "O", detail: "o", state: "owner", cells: [] },
    ],
  };
  const m = matrix({
    plan_d0_1: {
      served: cell({ state: "today" }),
      h5: cell({ days: 1, parks: 40 }),
    },
    plan_d2_5: {
      chronos: cell({
        state: "unclear",
        checks: { offline: "no", live: "running", visible: false },
      }),
    },
  });

  it("derives state, progress and earliest per decision, served cells left out", () => {
    const [a, b] = buildDecisions(file, m, TODAY);
    expect(a).toMatchObject({
      state: "waiting_live",
      progress: { liveDays: 1, liveParks: 40 },
      earliest: { date: "2026-10-24", label: "frühestens 24.10." },
    });
    // The served cell is still listed for the link.
    expect(a.cells).toHaveLength(2);
    expect(b).toMatchObject({
      state: "open",
      progress: { liveDays: 0, liveParks: 0 },
      earliest: { date: "2026-10-25" },
    });
  });

  it("lets a static state win and dates `later` after its dependencies", () => {
    const [, , later, owner] = buildDecisions(file, m, TODAY);
    expect(later).toMatchObject({
      state: "later",
      progress: null,
      earliest: { date: "2026-10-25" },
    });
    expect(later.earliest?.label).toContain("nicht vor 25.10.");
    expect(owner).toMatchObject({
      state: "owner",
      progress: null,
      earliest: null,
    });
  });

  it("degrades to open without a matrix", () => {
    const [a] = buildDecisions(file, null, TODAY);
    expect(a).toMatchObject({ state: "open", progress: null, earliest: null });
  });

  it("the shipped decisions file names only real questions and models", () => {
    const f = decisionsJson as unknown as DecisionFile;
    const questions = new Set(COMPARISON_QUESTIONS.map((q) => q.id));
    const models = new Set(COMPARISON_MODELS.map((x) => x.id));
    const ids = new Set<string>();
    expect(f.decisions).toHaveLength(6);
    for (const d of f.decisions) {
      expect(ids.has(d.id)).toBe(false);
      ids.add(d.id);
      for (const c of d.cells) {
        expect(questions.has(c.questionId)).toBe(true);
        expect(models.has(c.modelId)).toBe(true);
      }
      for (const dep of d.dependsOn ?? []) expect(ids.has(dep)).toBe(true);
      if (d.state) expect(["later", "owner"]).toContain(d.state);
    }
  });
});

describe("componentStatus", () => {
  const now = new Date("2026-10-11T20:20:00Z");
  const ago = (ms: number) => new Date(now.getTime() - ms);

  it("is ok within the threshold, late beyond it, unknown without a run", () => {
    expect(componentStatus(ago(4 * 60_000), LATE_AFTER_MS.pcn, now)).toBe("ok");
    expect(componentStatus(ago(45 * 60_000), LATE_AFTER_MS.pcn, now)).toBe(
      "ok",
    );
    expect(componentStatus(ago(46 * 60_000), LATE_AFTER_MS.pcn, now)).toBe(
      "late",
    );
    expect(
      componentStatus(ago(27 * 3_600_000), LATE_AFTER_MS.scoring, now),
    ).toBe("late");
    expect(componentStatus(null, LATE_AFTER_MS.pcn, now)).toBe("unknown");
  });

  it("lets a service's own failed / warming win over the age", () => {
    expect(componentStatus(ago(60_000), LATE_AFTER_MS.tft, now, "failed")).toBe(
      "failed",
    );
    expect(componentStatus(null, LATE_AFTER_MS.chronos, now, "warming")).toBe(
      "warming",
    );
  });
});
