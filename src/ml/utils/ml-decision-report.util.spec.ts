import { addIsoDays } from "../../common/utils/date.util";
import {
  ModelComparisonCellDto,
  ModelComparisonResponseDto,
} from "../dto/model-comparison.dto";
import {
  PairedParkDate,
  WEEKDAY_MAX_SHARE,
  buildDecisionReport,
  decisionPairedKeys,
  reportDue,
  seasonLabel,
} from "./ml-decision-report.util";
import { DecisionDef } from "./ml-overview.util";

const DEF: DecisionDef & { evaluateOn: string } = {
  id: "planner_h5_slots",
  title: "T",
  detail: "D",
  evaluateOn: "2026-12-22",
  cells: [{ questionId: "plan_d0_1", modelId: "h5" }],
};
const NOW = new Date("2026-12-22T12:30:00Z");

function greenMatrix(lastDate = "2026-12-21"): ModelComparisonResponseDto {
  const cell: ModelComparisonCellDto = {
    state: "better",
    note: "",
    checks: { offline: "yes", live: "yes", visible: true },
    live: {
      diff: -1.2,
      ciLow: -1.8,
      ciHigh: -0.4,
      parkCiLow: -1.6,
      parkCiHigh: -0.6,
      ci: "park_date",
      days: 28,
      parks: 40,
      n: 9000,
      modelMae: 7,
      referenceMae: 8.2,
      since: "2026-10-10",
    },
    offline: null,
    expectedBy: null,
  };
  return {
    generatedAt: NOW.toISOString(),
    window: { days: 28, firstDate: "2026-11-24", lastDate },
    rule: { minDays: 14, minParks: 10 },
    models: [],
    groups: [],
    questions: [
      {
        id: "plan_d0_1",
        group: "planner",
        label: "",
        sub: "",
        leadLabel: "",
        servedModel: "served",
        metric: { unit: "min", lowerIsBetter: true },
        cells: { h5: cell },
        coverage: { jointRows: 9000, cells: { h5: 9000 } },
      },
    ],
    summary: { better: 1, pending: 0, unclear: 0, worse: 0 },
  };
}

/** Every date of the 28-day window, for `parks` parks: each weekday exactly 1/7. */
function evenDates(parks = 3, holiday = false): PairedParkDate[] {
  const out: PairedParkDate[] = [];
  for (let p = 0; p < parks; p++) {
    for (let i = 0; i < 28; i++) {
      out.push({ parkId: `p${p}`, date: addIsoDays("2026-11-24", i), holiday });
    }
  }
  return out;
}

describe("buildDecisionReport", () => {
  it("freezes the matrix figures, both CIs and the joint rows", () => {
    const r = buildDecisionReport(DEF, greenMatrix(), evenDates(), NOW);
    expect(r).toMatchObject({
      decisionId: "planner_h5_slots",
      evaluateOn: "2026-12-22",
      createdAt: NOW.toISOString(),
      window: { firstDate: "2026-11-24", lastDate: "2026-12-21" },
      verdict: "ready",
      parks: 3,
      days: 28,
      holidayShare: 0,
      season: "Herbst und Winter",
      warning: false,
      warnings: [],
    });
    expect(r.cells).toEqual([
      {
        questionId: "plan_d0_1",
        modelId: "h5",
        state: "better",
        diff: -1.2,
        ciLow: -1.8,
        ciHigh: -0.4,
        parkCiLow: -1.6,
        parkCiHigh: -0.6,
        parks: 40,
        days: 28,
        n: 9000,
        jointRows: 9000,
        cellRows: 9000,
      },
    ]);
    expect(r.weekdays.map((w) => w.count)).toEqual([
      12, 12, 12, 12, 12, 12, 12,
    ]);
  });

  it("warns when one weekday carries more than 25 % of the paired dates", () => {
    // Weekend-only parks in autumn: add Saturdays until Sa > 25 %.
    const paired = evenDates(1);
    const saturdays = paired.filter(
      (p) => new Date(`${p.date}T12:00:00Z`).getUTCDay() === 6,
    );
    for (let k = 0; k < 6; k++) {
      for (const s of saturdays) paired.push({ ...s, parkId: `w${k}` });
    }
    const r = buildDecisionReport(DEF, greenMatrix(), paired, NOW);
    const sa = r.weekdays.find((w) => w.weekday === "Sa")!;
    expect(sa.share).toBeGreaterThan(WEEKDAY_MAX_SHARE);
    expect(r.checks.find((c) => c.id === "weekday_mix")!.ok).toBe(false);
    // Still the matrix's verdict — but never a silent green.
    expect(r.verdict).toBe("ready");
    expect(r.warning).toBe(true);
    expect(r.warnings[0]).toContain("Ein Wochentag überwiegt");
  });

  it("warns on a holiday-heavy window, an autumn-only one and an unfinished one", () => {
    const autumn = evenDates(2, true).map((p) => ({
      ...p,
      date: addIsoDays(p.date, -30),
    }));
    const r = buildDecisionReport(
      { ...DEF, evaluateOn: "2026-11-09" },
      greenMatrix("2026-11-07"),
      autumn,
      NOW,
    );
    const failed = r.checks.filter((c) => !c.ok).map((c) => c.id);
    expect(failed).toEqual(["holiday_share", "autumn_only", "window_end"]);
    expect(r.holidayShare).toBe(1);
    expect(r.season).toBe("Herbst");
    expect(r.warning).toBe(true);
  });

  it("does not call unknown holidays a typical window", () => {
    const r = buildDecisionReport(
      DEF,
      greenMatrix(),
      evenDates(1).map((p) => ({ ...p, holiday: null })),
      NOW,
    );
    expect(r.holidayShare).toBeNull();
    expect(r.checks.find((c) => c.id === "holiday_share")!.ok).toBe(false);
  });

  it("reads open without a matrix, with every check failed", () => {
    const r = buildDecisionReport(DEF, null, [], NOW);
    expect(r.verdict).toBe("open");
    expect(r.cells).toEqual([]);
    expect(r.checks.every((c) => !c.ok)).toBe(true);
  });
});

describe("reportDue", () => {
  it("waits for evaluateOn − 1 to be scored, up to the grace period", () => {
    expect(reportDue(DEF, "2026-12-21", "2026-12-20")).toBe(false);
    expect(reportDue(DEF, "2026-12-22", "2026-12-20")).toBe(false);
    expect(reportDue(DEF, "2026-12-22", "2026-12-21")).toBe(true);
    expect(reportDue(DEF, "2026-12-25", "2026-12-20")).toBe(true);
    expect(
      reportDue({ ...DEF, state: "later" }, "2027-01-01", "2026-12-21"),
    ).toBe(false);
  });
});

describe("decisionPairedKeys / seasonLabel", () => {
  it("names the paired rows behind the cells, never the served one", () => {
    expect(decisionPairedKeys(DEF)).toEqual([
      "UC3|d0|shadow_h5|served_joint|all",
      "UC3|d1|shadow_h5|served_joint|all",
    ]);
  });

  it("lists the seasons in calendar order", () => {
    expect(seasonLabel(["2026-12-01", "2026-06-01", "2026-10-01"])).toBe(
      "Sommer, Herbst und Winter",
    );
  });
});
