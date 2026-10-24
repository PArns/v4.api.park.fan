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
  decisionFloor,
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

describe("deriveDecisionState with expect: not_better (PAR-1042 review)", () => {
  // catboost_daily_off: CatBoost measured against served TFT. Switching it off
  // needs CatBoost NOT to win, so a CatBoost win must never make it ready.
  const notBetter = (over: Partial<ModelComparisonCellDto>) => ({
    ...cell({ days: 20, parks: 30, ...over }),
    expect: "not_better" as const,
  });
  const wkdGreen = cell({
    state: "better",
    checks: { offline: "yes", live: "yes", visible: true },
    days: 20,
    parks: 30,
  });

  it("is ready when CatBoost is measurably worse or not better than TFT", () => {
    const worse = notBetter({
      state: "worse",
      checks: { offline: "no", live: "no", visible: false },
    });
    const unclear = notBetter({
      state: "unclear",
      checks: { offline: "open", live: "no", visible: false },
    });
    expect(deriveDecisionState([worse, unclear, wkdGreen])).toBe("ready");
  });

  it("is not ready when CatBoost beats TFT, and waits while live is still measuring", () => {
    const wins = notBetter({
      state: "better",
      checks: { offline: "yes", live: "yes", visible: true },
    });
    expect(deriveDecisionState([wins, wkdGreen])).toBe("open");
    const measuring = notBetter({
      state: "unclear",
      checks: { offline: "no", live: "running", visible: false },
    });
    expect(deriveDecisionState([measuring, wkdGreen])).toBe("waiting_live");
  });

  it("the shipped file expects CatBoost not to beat TFT where TFT is served", () => {
    const f = decisionsJson as unknown as DecisionFile;
    const off = f.decisions.find((d) => d.id === "catboost_daily_off")!;
    for (const q of ["cal_rank", "cal_busier", "cal_far"]) {
      expect(
        off.cells.find((c) => c.questionId === q && c.modelId === "catboost")
          ?.expect,
      ).toBe("not_better");
    }
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
    // today + 14 would be 25.10., but the cell's longest lead is d5: its
    // first d5 target date was 14.10., so 14 of them cannot exist before 28.10.
    expect(b).toMatchObject({
      state: "open",
      progress: { liveDays: 0, liveParks: 0 },
      earliest: { date: "2026-10-28" },
    });
  });

  it("lets a static state win and dates `later` after its dependencies", () => {
    const [, , later, owner] = buildDecisions(file, m, TODAY);
    expect(later).toMatchObject({
      state: "later",
      progress: null,
      earliest: { date: "2026-10-28" },
    });
    expect(later.earliest?.label).toContain("nicht vor 28.10.");
    expect(owner).toMatchObject({
      state: "owner",
      progress: null,
      earliest: null,
    });
  });

  it("never dates a long-lead decision before its longest lead can have 14 dates (review of PAR-1036)", () => {
    const def = (questionId: string, modelId: string): DecisionFile => ({
      decisions: [
        { id: "x", title: "X", detail: "x", cells: [{ questionId, modelId }] },
      ],
    });
    const thin = (q: string, mId: string) =>
      matrix({ [q]: { [mId]: cell({ days: 0, parks: 0 }) } });
    // d60: 09.10. + 60 + 14 — not "today + 14", which slides every day.
    for (const today of ["2026-10-11", "2026-11-20"]) {
      expect(
        buildDecisions(
          def("plan_d6_60", "h5"),
          thin("plan_d6_60", "h5"),
          today,
        )[0].earliest?.date,
      ).toBe("2026-12-22");
    }
    expect(
      buildDecisions(
        def("cal_far_90", "wkd"),
        thin("cal_far_90", "wkd"),
        TODAY,
      )[0].earliest?.date,
    ).toBe("2027-01-21");
    expect(decisionFloor(def("plan_besttime", "h5").decisions[0])).toBe(
      "2026-11-22",
    );
  });

  it("is never ready before its evaluation date, and judged on the frozen window after it", () => {
    const ready = matrix({
      plan_d0_1: {
        h5: cell({
          state: "better",
          days: 20,
          parks: 20,
          checks: { offline: "yes", live: "yes", visible: true },
        }),
      },
    });
    const f: DecisionFile = {
      decisions: [
        {
          id: "e",
          title: "E",
          detail: "e",
          evaluateOn: "2026-11-09",
          cells: [{ questionId: "plan_d0_1", modelId: "h5" }],
        },
      ],
    };
    const before = buildDecisions(f, ready, "2026-11-01")[0];
    expect(before).toMatchObject({
      state: "waiting_live",
      evaluateOn: "2026-11-09",
      evaluated: false,
      earliest: { date: "2026-11-09", label: "ausgewertet am 09.11." },
    });
    // From the date on, the frozen window (ending 08.11.) decides — not the
    // latest matrix, which may have drifted either way.
    const worse = matrix({
      plan_d0_1: { h5: cell({ state: "worse", days: 30, parks: 30 }) },
    });
    const after = buildDecisions(
      f,
      worse,
      "2026-12-01",
      new Map([["2026-11-09", ready]]),
    )[0];
    expect(after).toMatchObject({ state: "ready", evaluated: true });
    expect(after.earliest).toBeNull();
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
    expect(f.decisions).toHaveLength(5);
    for (const d of f.decisions) {
      expect(ids.has(d.id)).toBe(false);
      ids.add(d.id);
      for (const c of d.cells) {
        expect(questions.has(c.questionId)).toBe(true);
        expect(models.has(c.modelId)).toBe(true);
      }
      for (const dep of d.dependsOn ?? []) expect(ids.has(dep)).toBe(true);
      if (d.state) expect(["later", "owner"]).toContain(d.state);
      // A fixed evaluation date before the gate can be met would freeze a
      // decision on a window that cannot pass.
      expect({
        id: d.id,
        ok: !!d.evaluateOn && d.evaluateOn >= decisionFloor(d)!,
      }).toEqual({
        id: d.id,
        ok: true,
      });
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
