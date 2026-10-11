import {
  COMPARISON_MODELS,
  COMPARISON_QUESTIONS,
  LiveComparison,
  OfflineVerdict,
  OfflineVerdictFile,
  PairedAggregate,
  buildModelComparison,
  cellChecks,
  cellExpectedBy,
  cellNote,
  cellState,
  fmtDe,
  poolCell,
} from "./model-comparison.util";
import verdictsJson from "../data/offline-model-verdicts.json";

const live = (o: Partial<LiveComparison> = {}): LiveComparison => ({
  n: 5000,
  days: 18,
  parks: 34,
  diff: -1.3,
  modelMae: 8.2,
  referenceMae: 9.5,
  ciLow: -2.1,
  ciHigh: -0.6,
  enoughData: true,
  ...o,
});
const offline = (state: OfflineVerdict["state"], note?: string) =>
  ({ state, source: "ml-bench PR #445", note }) as OfflineVerdict;
const q = (id: string) => COMPARISON_QUESTIONS.find((x) => x.id === id)!;

/** `parks` parks × `days` days, each park with the same per-day sums. */
function rows(
  parks: number,
  days: number,
  perDay: { n: number; a: number; b: number },
  key: Partial<PairedAggregate> = {},
): PairedAggregate[] {
  const out: PairedAggregate[] = [];
  for (let p = 0; p < parks; p++) {
    out.push({
      useCase: "UC3",
      lead: "d2",
      model: "shadow_h5",
      segment: "all",
      parkId: `p${p}`,
      dates: Array.from(
        { length: days },
        (_, d) => `2026-10-${String(d + 1).padStart(2, "0")}`,
      ),
      // a little park-to-park spread so the bootstrap has a width
      n: perDay.n * days,
      saeModel: (perDay.a + (p % 3) * 0.1 * perDay.n) * days,
      saeRef: perDay.b * days,
      ...key,
    });
  }
  return out;
}

describe("poolCell", () => {
  it("sums the minutes per park, then divides once; gate = 14 days and 10 parks", () => {
    const c = poolCell(rows(12, 14, { n: 10, a: 80, b: 100 }), 1)!;
    expect(c.n).toBe(12 * 14 * 10);
    expect(c.days).toBe(14);
    expect(c.parks).toBe(12);
    expect(c.referenceMae).toBe(10);
    expect(c.diff).toBeLessThan(0);
    expect(c.ciHigh!).toBeLessThan(0);
    expect(c.enoughData).toBe(true);
    expect(poolCell(rows(9, 20, { n: 10, a: 80, b: 100 }), 1)!.enoughData).toBe(
      false,
    );
    expect(
      poolCell(rows(12, 13, { n: 10, a: 80, b: 100 }), 1)!.enoughData,
    ).toBe(false);
  });

  it("has no CI with a single park and nothing with no slots", () => {
    expect(
      poolCell(rows(1, 20, { n: 10, a: 80, b: 100 }), 1)!.ciLow,
    ).toBeNull();
    expect(poolCell([], 1)).toBeNull();
  });
});

describe("cellState — the rule", () => {
  it("is today for the served model, whatever the numbers", () => {
    expect(cellState(true, live({ ciHigh: 5, ciLow: 1 }), null)).toBe("today");
  });
  it("lets a live comparison past the gate decide", () => {
    expect(cellState(false, live(), offline("worse"))).toBe("better");
    expect(cellState(false, live({ ciLow: 0.2, ciHigh: 0.9 }), null)).toBe(
      "worse",
    );
    expect(
      cellState(false, live({ ciLow: -0.5, ciHigh: 0.3 }), offline("better")),
    ).toBe("unclear");
  });
  it("falls back to the offline verdict below the gate", () => {
    const thin = live({ days: 9, enoughData: false });
    expect(cellState(false, thin, offline("better"))).toBe("pending");
    expect(cellState(false, null, offline("pending"))).toBe("pending");
    expect(cellState(false, thin, offline("worse"))).toBe("worse");
    expect(cellState(false, thin, offline("unclear"))).toBe("unclear");
    expect(cellState(false, null, null)).toBe("unclear");
  });
});

describe("cellChecks", () => {
  it("maps offline, live and visibility", () => {
    expect(cellChecks("better", true, live(), offline("better"))).toEqual({
      offline: "yes",
      live: "yes",
      visible: true,
    });
    // A confirmed win under one minute is not visible in the app.
    expect(cellChecks("better", true, live({ diff: -0.4 }), null).visible).toBe(
      false,
    );
    expect(
      cellChecks(
        "pending",
        true,
        live({ enoughData: false }),
        offline("pending"),
      ),
    ).toEqual({ offline: "running", live: "running", visible: false });
    expect(cellChecks("worse", false, null, offline("worse"))).toEqual({
      offline: "no",
      live: "open",
      visible: false,
    });
  });
});

describe("cellNote — German, from the numbers", () => {
  it("states a confirmed win the way the owner reads it", () => {
    expect(
      cellNote(q("plan_d2_5"), "better", true, live(), null, undefined),
    ).toBe("1,3 Minuten besser als heute, sicher. 18 Tage, 34 Parks.");
  });
  it("states a loss, an unclear result and a thin one", () => {
    expect(
      cellNote(
        q("plan_d2_5"),
        "worse",
        true,
        live({ diff: 0.42, ciLow: 0.1, ciHigh: 0.7 }),
        null,
        undefined,
      ),
    ).toBe("0,4 Minuten schlechter als heute, sicher. 18 Tage, 34 Parks.");
    expect(
      cellNote(
        q("plan_d2_5"),
        "unclear",
        true,
        live({ diff: -0.2, ciLow: -0.6, ciHigh: 0.3 }),
        null,
        undefined,
      ),
    ).toBe(
      "Kein sicherer Unterschied zu heute (−0,2 Minuten, Spanne −0,6 bis +0,3). 18 Tage, 34 Parks.",
    );
    expect(
      cellNote(
        q("plan_d2_5"),
        "pending",
        true,
        live({ diff: -0.8, days: 9, parks: 12, enoughData: false }),
        offline("better", "Offline besser."),
        undefined,
      ),
    ).toBe(
      "Offline besser. Live bisher −0,8 Minuten gegenüber heute, 9 von 14 Tagen, 12 von 10 Parks.",
    );
  });
  it("names the unit of best time", () => {
    expect(
      cellNote(q("plan_besttime"), "better", true, live(), null, undefined),
    ).toContain("Minuten verschenkte Wartezeit besser als heute");
  });
  it("formats German numbers", () => {
    expect(fmtDe(1.25)).toBe("1,3");
    expect(fmtDe(0.06)).toBe("0,06");
    expect(fmtDe(-0.26, true)).toBe("−0,3");
    expect(fmtDe(0, true)).toBe("0,0");
  });
});

describe("cellExpectedBy", () => {
  const checks = (
    live: "running" | "open",
    off: "running" | "open" = "open",
  ) => ({
    offline: off,
    live,
    visible: false,
  });
  it("counts the days still missing from the last scored date (+1 for the scoring run)", () => {
    expect(
      cellExpectedBy(
        "pending",
        checks("running"),
        live({ days: 5, enoughData: false }),
        null,
        "2026-10-10",
        "2026-10-11",
      ),
    ).toEqual({
      date: "2026-10-20",
      label: "frühestens 20.10. (noch 9 von 14 Tagen)",
    });
  });
  it("waits for parks once the days are there", () => {
    expect(
      cellExpectedBy(
        "unclear",
        checks("running"),
        live({ days: 20, parks: 7, enoughData: false }),
        null,
        "2026-10-10",
        "2026-10-11",
      )!.label,
    ).toBe("sobald 10 Parks dabei sind (bisher 7), frühestens 11.10.");
  });
  it("takes the earlier of an offline run and the live gate", () => {
    const off = {
      ...offline("pending"),
      expectedBy: { date: "2026-10-12", label: "Offline-Lauf" },
    };
    expect(
      cellExpectedBy(
        "pending",
        checks("running", "running"),
        null,
        off,
        "2026-10-10",
        "2026-10-11",
      ),
    ).toEqual({ date: "2026-10-12", label: "Offline-Lauf" });
    expect(
      cellExpectedBy("today", checks("running"), null, off, null, "2026-10-11"),
    ).toBeNull();
    expect(
      cellExpectedBy("worse", checks("open"), null, null, null, "2026-10-11"),
    ).toBeNull();
  });
});

describe("buildModelComparison", () => {
  const verdicts = verdictsJson as unknown as OfflineVerdictFile;
  const build = (aggregates: PairedAggregate[]) =>
    buildModelComparison({
      aggregates,
      since: new Map([["UC3|d2|shadow_h5|all", "2026-10-01"]]),
      verdicts,
      window: { days: 28, firstDate: "2026-10-01", lastDate: "2026-10-14" },
      today: "2026-10-15",
      generatedAt: "2026-10-15T08:00:00.000Z",
    });

  it("has a today cell for the served model of every question and only applicable cells", () => {
    const res = build([]);
    expect(res.rule).toEqual({ minDays: 14, minParks: 10 });
    expect(res.models.map((m) => m.id)).toEqual(
      COMPARISON_MODELS.map((m) => m.id),
    );
    expect(res.questions.map((x) => x.group)).toEqual([
      "live",
      "live",
      "today",
      "today",
      "planner",
      "planner",
      "planner",
      "planner",
      "calendar",
      "calendar",
    ]);
    for (const question of res.questions) {
      expect(question.cells[question.servedModel].state).toBe("today");
      expect(question.leadLabel.length).toBeGreaterThan(0);
      expect(question.label).not.toMatch(/\bd\d/);
    }
    // TFT is not a planner column; PCN is not a calendar one.
    expect(
      res.questions.find((x) => x.id === "plan_d2_5")!.cells.tft,
    ).toBeUndefined();
    expect(
      res.questions.find((x) => x.id === "cal_far")!.cells.pcn,
    ).toBeUndefined();
    const total = Object.values(res.summary).reduce((a, b) => a + b, 0);
    const cells = res.questions.reduce(
      (s, x) => s + Object.keys(x.cells).length - 1,
      0,
    );
    expect(total).toBe(cells);
  });

  it("never invents an offline CI, carries the Chronos planner verdicts and keeps Toto/TiRex pending", () => {
    const res = build([]);
    for (const question of res.questions) {
      for (const cell of Object.values(question.cells)) {
        if (cell.offline && cell.offline.ciLow !== undefined) {
          throw new Error(`unexpected offline CI in ${question.id}`);
        }
      }
    }
    // The mixed-weekday re-run of ml-bench #450 (`--shard 0/5`) landed on
    // 2026-10-11 and decided the three planner cells, which carried
    // "offline noch nicht ausgewertet" while only the weekday-aliased
    // `--shard 0/7` run existed. The three offline verdicts differ on purpose
    // (Chronos-2's grid curve wins the near leads, wins only d2 inside d2-d5,
    // and never gets under climatology on the best-time regret), and with NO
    // live rows the gate rule maps them to three different CELL states: offline
    // `better` becomes `pending` because live confirmation is still owed,
    // offline `unclear` stays `unclear`, offline `worse` is `worse` straight
    // away. A single state across all three would mean the file was seeded
    // rather than measured.
    const planner: Record<string, string> = {
      plan_d0_1: "pending",
      plan_d2_5: "unclear",
      plan_besttime: "worse",
    };
    for (const [id, state] of Object.entries(planner)) {
      const cell = res.questions.find((x) => x.id === id)!.cells.chronos;
      expect(cell.state).toBe(state);
      expect(cell.offline!.source).toContain("Wochentagen");
      expect(cell.offline!.diff).toBeUndefined();
    }
    // the two `today` questions stay pending: Chronos beats the H5 profile there,
    // but PCN is what is served and PCN is not in the offline benchmark
    for (const id of ["today_h2_6", "today_h6_12"]) {
      expect(res.questions.find((x) => x.id === id)!.cells.chronos.state).toBe(
        "pending",
      );
    }
    // the archived chronos_grid surface stops at d7, so the long planner band has
    // no Chronos column at all rather than an empty-looking one
    expect(
      res.questions.find((x) => x.id === "plan_d6_60")!.cells.chronos,
    ).toBeUndefined();
    const fm = res.questions.find((x) => x.id === "live_h0_1")!.cells.fm;
    expect(fm.state).toBe("pending");
    expect(fm.live).toBeNull();
    expect(fm.checks.live).toBe("open");
    expect(fm.expectedBy?.date).toBe("2026-10-12");
  });

  it("turns a confirmed live win into `better` with its numbers and since", () => {
    const res = build(rows(12, 14, { n: 10, a: 80, b: 100 }));
    const cell = res.questions.find((x) => x.id === "plan_d2_5")!.cells.h5;
    expect(cell.state).toBe("better");
    expect(cell.checks.live).toBe("yes");
    expect(cell.live).toMatchObject({
      days: 14,
      parks: 12,
      since: "2026-10-01",
    });
    expect(cell.live!.diff).toBeLessThan(0);
    expect(cell.note).toMatch(
      /Minuten besser als heute, sicher\. 14 Tage, 12 Parks\./,
    );
    expect(cell.expectedBy).toBeNull();
  });
});
