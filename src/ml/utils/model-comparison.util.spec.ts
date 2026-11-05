import {
  COMPARISON_MODELS,
  COMPARISON_QUESTIONS,
  LiveComparison,
  OfflineVerdict,
  OfflineVerdictFile,
  PairedAggregate,
  buildModelComparison,
  cellChecks,
  cellLeads,
  d7Aggregates,
  d7AggregatesByModel,
  cellExpectedBy,
  cellFloorDate,
  cellNote,
  cellState,
  fmtDe,
  poolCell,
} from "./model-comparison.util";
import verdictsJson from "../data/offline-model-verdicts.json";
import {
  ArchivedCurve,
  PAIRED_OPTIONAL_MODELS,
  PairedAccumulator,
  scorePairedModels,
} from "./forecast-archive-scoring.util";

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
const offline = (
  state: OfflineVerdict["state"],
  note?: string,
  reference: string | null = "served",
) => ({ state, source: "ml-bench PR #445", note, reference }) as OfflineVerdict;
const q = (id: string) => COMPARISON_QUESTIONS.find((x) => x.id === id)!;

/** `parks` parks × `days` days, each park with the same per-day sums. */
function rows(
  parks: number,
  days: number,
  perDay: { n: number; a: number; b: number },
  key: Partial<PairedAggregate> = {},
): PairedAggregate[] {
  // One row per park × date — the grain the service reads (PAR-1036).
  const out: PairedAggregate[] = [];
  for (let p = 0; p < parks; p++) {
    for (let d = 0; d < days; d++) {
      out.push({
        useCase: "UC3",
        lead: "d2",
        model: "shadow_h5",
        segment: "all",
        parkId: `p${p}`,
        dates: [`2026-10-${String(d + 1).padStart(2, "0")}`],
        // a little park-to-park and day-to-day spread so both bootstraps
        // have a width
        n: perDay.n,
        saeModel: perDay.a + ((p % 3) * 0.1 + (d % 2) * 0.05) * perDay.n,
        saeRef: perDay.b,
        ...key,
      });
    }
  }
  return out;
}

describe("poolCell", () => {
  it("sums the minutes per park, then divides once; gate = 14 days and 10 parks", () => {
    const c = poolCell(rows(12, 14, { n: 10, a: 80, b: 100 }), 1)!;
    expect(c.n).toBe(12 * 14 * 10);
    // Both intervals: the gate's two-way one and the park-only one beside it.
    expect(c.parkCiHigh!).toBeLessThan(0);
    expect(c.parkCiLow!).toBeLessThan(c.parkCiHigh!);
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

  it("counts the days each park reached, not the union over parks", () => {
    // One park on 14 days and nine on one day each: 14 distinct dates in
    // all, but only one park measured for 14 days — the gate must not pass.
    const sparse = [
      ...rows(1, 14, { n: 10, a: 80, b: 100 }),
      ...rows(9, 1, { n: 10, a: 80, b: 100 }).map((r, i) => ({
        ...r,
        parkId: `q${i}`,
      })),
    ];
    const c = poolCell(sparse, 1)!;
    expect(c.parks).toBe(10);
    expect(c.days).toBe(1);
    expect(c.enoughData).toBe(false);
    // The 10th-best park sets the count: 11 parks, the weakest on 3 days.
    const mixed = [
      ...rows(10, 14, { n: 10, a: 80, b: 100 }),
      ...rows(1, 3, { n: 10, a: 80, b: 100 }).map((r) => ({
        ...r,
        parkId: "z",
      })),
    ];
    expect(poolCell(mixed, 1)!.days).toBe(14);
    expect(poolCell(mixed, 1)!.enoughData).toBe(true);
  });

  it("weighs every lead the same, whatever its number of rows (PAR-1036)", () => {
    // d2: many slots, 2 minutes better; d3: few slots, no difference.
    const d2 = rows(12, 14, { n: 100, a: 800, b: 1000 });
    const d3 = rows(12, 14, { n: 1, a: 10, b: 10 }, { lead: "d3" });
    const c = poolCell([...d2, ...d3], 1, ["d2", "d3"])!;
    // Pooled by n this would read about −2.0; equal weight is the mean.
    const d2Diff = c.leads!.find((l) => l.lead === "d2")!.diff;
    const d3Diff = c.leads!.find((l) => l.lead === "d3")!.diff;
    expect(c.diff).toBeCloseTo((d2Diff + d3Diff) / 2, 1);
    expect(c.diff).toBeGreaterThan(-1.5);
    expect(c.enoughData).toBe(true);
  });

  it("passes the gate only when every lead of the cell does", () => {
    const d6 = rows(12, 14, { n: 10, a: 80, b: 100 }, { lead: "d6" });
    // d60 has no rows yet: the cell is d6 alone and must not be decided.
    const c = poolCell(d6, 1, ["d6", "d60"])!;
    expect(c.enoughData).toBe(false);
    expect(c.days).toBe(0);
    expect(c.leads!.map((l) => l.lead)).toEqual(["d6"]);
    const thin = rows(12, 5, { n: 10, a: 80, b: 100 }, { lead: "d60" });
    expect(poolCell([...d6, ...thin], 1, ["d6", "d60"])!.days).toBe(5);
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
    expect(cellState(false, live(), offline("better"))).toBe("better");
    // A live win counts as better only when offline agrees (owner rule O ∧ L).
    expect(cellState(false, live(), offline("worse"))).toBe("unclear");
    expect(cellState(false, live(), null)).toBe("unclear");
    expect(cellState(false, live(), offline("pending"))).toBe("pending");
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

describe("cellState — an offline win over another reference (PAR-1038)", () => {
  it("never makes a cell better, live win or not", () => {
    for (const ref of ["persistence", "wt_med", "hourly_profile", null]) {
      expect(cellState(false, live(), offline("better", undefined, ref))).toBe(
        "pending",
      );
      expect(
        cellChecks("pending", true, live(), offline("better", undefined, ref))
          .offline,
      ).toBe("open");
    }
    // A verdict without the field is not known to be against served either.
    expect(
      cellState(false, live(), {
        state: "better",
        source: "x",
      } as OfflineVerdict),
    ).toBe("pending");
    expect(
      cellNote(
        q("live_h1_2"),
        "pending",
        true,
        live(),
        offline("better", undefined, "persistence"),
        undefined,
      ),
    ).toMatch(/nur gegen einen anderen Vergleichswert gemessen/);
  });
});

describe("offline-model-verdicts.json — the PAR-1038 schema", () => {
  const file = verdictsJson as unknown as OfflineVerdictFile;
  it("states reference, metric, run id and the figures on every entry, inventing no number", () => {
    for (const [qid, models] of Object.entries(file.questions)) {
      for (const [mid, v] of Object.entries(models)) {
        for (const k of [
          "reference",
          "metric",
          "runId",
          "diff",
          "ciLow",
          "ciHigh",
        ]) {
          expect({ cell: `${qid}/${mid}`, has: k in v }).toEqual({
            cell: `${qid}/${mid}`,
            has: true,
          });
        }
        // A figure only comes from `python -m mlbench.served_verdicts` (PAR-1043): a
        // whole triple, against served, from a named run. Hand-typed entries carry none.
        const fig = [v.diff, v.ciLow, v.ciHigh];
        if (fig.every((x) => x === null)) continue;
        expect({
          cell: `${qid}/${mid}`,
          reference: v.reference,
          run: !!v.runId,
        }).toEqual({
          cell: `${qid}/${mid}`,
          reference: "served",
          run: true,
        });
        expect(v.ciLow! <= v.diff! && v.diff! <= v.ciHigh!).toBe(true);
      }
    }
  });
  it("files a hit rate under the day-ranking question only, and no orphaned cell", () => {
    const hitRates = Object.entries(file.questions).flatMap(([qid, models]) =>
      Object.entries(models)
        .filter(([, v]) => v.metric === "hit_rate")
        .map(([mid]) => `${qid}/${mid}`),
    );
    expect(hitRates.sort()).toEqual(["cal_rank/driver", "cal_rank/wkd"]);
    expect(file.questions.today_h6_12.persist).toBeUndefined();
  });
  it("keeps the PCN questions from turning better on a win over persistence", () => {
    const v = file.questions.live_h1_2.chronos;
    expect(v.state).toBe("better");
    expect(v.reference).toBe("persistence");
    expect(cellState(false, live(), v)).toBe("pending");
  });
});

describe("cellNote — a live win offline does not back", () => {
  it("names whether offline disagrees or is still running", () => {
    expect(
      cellNote(
        q("plan_d0_1"),
        "unclear",
        true,
        live(),
        offline("worse"),
        undefined,
      ),
    ).toMatch(/besser als heute, sicher, offline aber nicht bestätigt\./);
    expect(
      cellNote(
        q("plan_d0_1"),
        "pending",
        true,
        live(),
        offline("pending"),
        undefined,
      ),
    ).toMatch(/offline läuft die Messung noch\./);
    expect(
      cellNote(q("plan_d0_1"), "unclear", true, live(), null, undefined),
    ).toMatch(/offline nicht gemessen\./);
  });
});

describe("cellChecks", () => {
  it("measures visibility in the question's unit (review of PAR-1037)", () => {
    // 1.3 percentage points of wrongly ordered day pairs is not visible;
    // the minute threshold would have called it so.
    const won = live({ diff: -1.3 });
    expect(cellChecks("better", true, won, offline("better")).visible).toBe(
      true,
    );
    expect(
      cellChecks("better", true, won, offline("better"), "rate").visible,
    ).toBe(false);
    expect(
      cellChecks("better", true, live({ diff: -6 }), offline("better"), "rate")
        .visible,
    ).toBe(true);
  });
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
    ).toBe("1,3 Minuten besser als heute, sicher. 18 Tage je Park, 34 Parks.");
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
    ).toBe(
      "0,4 Minuten schlechter als heute, sicher. 18 Tage je Park, 34 Parks.",
    );
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
      "Kein sicherer Unterschied zu heute (−0,2 Minuten, Spanne −0,6 bis +0,3). 18 Tage je Park, 34 Parks.",
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
      "Offline besser. Live bisher −0,8 Minuten gegenüber heute, 9 von 14 Tagen je Park, 12 von 10 Parks.",
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
      since: new Map([["UC3|d2|shadow_h5|served_joint|all", "2026-10-01"]]),
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

  it("never invents an offline CI and carries the Chronos and Toto/TiRex planner verdicts", () => {
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
    // but PCN is what is served and PCN is not in the offline benchmark — and a
    // pending cell carries the date its live confirmation is expected
    for (const id of ["today_h2_6", "today_h6_12"]) {
      const cell = res.questions.find((x) => x.id === id)!.cells.chronos;
      expect(cell.state).toBe("pending");
      expect(cell.expectedBy?.date).toBe("2026-10-26");
    }
    // the archived chronos_grid surface stops at d7, so the long planner band has
    // no Chronos column at all rather than an empty-looking one
    expect(
      res.questions.find((x) => x.id === "plan_d6_60")!.cells.chronos,
    ).toBeUndefined();
    // Toto 2.0 / TiRex-2 (PAR-853) are measured but never archived: no live
    // source, so `live` stays null and the live check stays open for good. Now
    // that the run has reported, the cell must carry its offline verdict and no
    // longer dangle an `expectedBy` — a lingering one would promise an arrival
    // that is not coming.
    const fm = res.questions.find((x) => x.id === "live_h0_1")!.cells.fm;
    expect(fm.state).toBe("unclear");
    expect(fm.live).toBeNull();
    expect(fm.checks.live).toBe("open");
    expect(fm.expectedBy).toBeNull();
    expect(fm.offline!.source).toContain("#457");
    // and the planner band where they lose outright is `worse`, not softened
    expect(
      res.questions.find((x) => x.id === "plan_d2_5")!.cells.fm.state,
    ).toBe("worse");
  });

  it("turns a confirmed live win into `better` with its numbers and since", () => {
    const res = build(
      ["d2", "d3", "d4", "d5"].flatMap((lead) =>
        rows(12, 14, { n: 10, a: 80, b: 100 }, { lead }),
      ),
    );
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
      /Minuten besser als heute, sicher\. 14 Tage je Park, 12 Parks\./,
    );
    expect(cell.expectedBy).toBeNull();
  });
});

describe("one question, one set of rows (PAR-1035)", () => {
  const T0 = Date.parse("2026-10-08T08:00:00Z");
  const MIN = 60_000;
  const SLOT = 15 * MIN;
  const curve = (
    surface: ArchivedCurve["surface"],
    waits: (number | null)[],
    extra: Partial<ArchivedCurve> = {},
  ): ArchivedCurve => ({
    attractionId: "a1",
    surface,
    originKind: "daily",
    originAt: T0 - 125 * MIN,
    leadDays: 0,
    slotStart: T0,
    slotMinutes: 15,
    waits,
    sources: "c".repeat(waits.length),
    bands: null,
    dayPeak: 45,
    expectedError: null,
    rideQ90: 50,
    isHeadliner: true,
    levelSource: "tft",
    ...extra,
  });
  const clock = (ms: number) => new Date(ms).toISOString().slice(11, 16);

  /** Two parks, two days, every surface — scored by the real scorer. */
  function aggregates(): PairedAggregate[] {
    const out: PairedAggregate[] = [];
    for (const [p, shift] of [
      ["p1", 0],
      ["p2", 10],
    ] as const) {
      for (const date of ["2026-10-08", "2026-10-09"]) {
        const tv = [30, 30, 40, 40, 50, 50, 20, 20].map((v) => v + shift);
        const truth = new Map([
          ["a1", new Map(tv.map((v, i) => [T0 + i * SLOT, v]))],
        ]);
        const intraday = { originKind: "intraday" as const, originAt: T0 };
        // Chronos misses one slot and H5 another: each candidate's own
        // coverage differs, the paired rows must not.
        const chronos: (number | null)[] = [35, null, 35, 35, 35, 35, 35, 35];
        const h5: (number | null)[] = [30, 30, null, 40, 50, 50, 20, 20];
        const curves = [
          curve("park_hourly", Array(8).fill(30)),
          curve("plan_day", [30, 45], { slotMinutes: 60 }),
          curve("plan_day_shadow_h5", h5),
          curve("plan_day_shadow_peak", [30, 30, 30, 30, 45, 45, 45, 45]),
          curve("chronos_grid", Array(8).fill(30)),
          curve("park_hourly", Array(8).fill(30), {
            ...intraday,
            liveWait: 25,
          }),
          curve("chronos_live", chronos, intraday),
        ];
        const acc = new PairedAccumulator();
        scorePairedModels(curves, truth, acc, {
          targetDate: "2026-10-08",
          clock,
          history: new Map([
            [
              "a1",
              new Map([
                [
                  "2026-10-01",
                  new Map(
                    Array.from({ length: 8 }, (_, i) => [
                      clock(T0 + i * SLOT),
                      30,
                    ]),
                  ),
                ],
                [
                  "2026-09-24",
                  new Map(
                    Array.from({ length: 8 }, (_, i) => [
                      clock(T0 + i * SLOT),
                      40,
                    ]),
                  ),
                ],
              ]),
            ],
          ]),
        });
        for (const e of acc.entries()) {
          out.push({ ...e, parkId: p, dates: [date] });
        }
      }
    }
    return out;
  }

  it("gives every core cell of a question the same reference MAE, lead by lead", () => {
    const res = buildModelComparison({
      aggregates: aggregates(),
      since: new Map(),
      verdicts: verdictsJson as unknown as OfflineVerdictFile,
      window: { days: 28, firstDate: "2026-10-08", lastDate: "2026-10-09" },
      today: "2026-10-10",
      generatedAt: "2026-10-10T08:00:00.000Z",
    });
    let checked = 0;
    for (const question of res.questions) {
      const q = COMPARISON_QUESTIONS.find((x) => x.id === question.id)!;
      // Optional shadows (Chronos-2, the old composer) ride on the core rows
      // where they have a value; their outage shows in `coverage` instead.
      const core = Object.entries(question.cells)
        .filter(([id]) =>
          (q.models[id] ?? []).some((m) => !PAIRED_OPTIONAL_MODELS.has(m)),
        )
        .map(([, c]) => c.live);
      const lives = core.filter((l) => l !== null);
      if (lives.length < 2) continue;
      checked++;
      const byLead = new Map<string, Set<number>>();
      for (const l of lives) {
        for (const x of l.leads ?? []) {
          const set = byLead.get(x.lead) ?? new Set<number>();
          set.add(x.referenceMae);
          byLead.set(x.lead, set);
        }
      }
      for (const [lead, set] of byLead) {
        expect({ q: question.id, lead, refs: [...set] }).toEqual({
          q: question.id,
          lead,
          refs: [[...set][0]],
        });
      }
      // Cells covering the same leads share the question's reference MAE.
      expect(new Set(lives.map((l) => l.referenceMae)).size).toBe(1);
    }
    // live_h0_1, live_h1_2, plan_d0_1, plan_besttime at least.
    expect(checked).toBeGreaterThanOrEqual(4);
    // Chronos missed one slot per park-day: its cell is below the joint rows,
    // and the other cells keep all of them.
    const live = res.questions.find((x) => x.id === "live_h0_1")!;
    expect(live.coverage!.cells.chronos).toBeLessThan(live.coverage!.jointRows);
    expect(live.coverage!.cells.persist).toBe(live.coverage!.jointRows);
    // …and the direct comparison sits on Chronos' rows.
    const pair = live.comparisons!.find((c) => c.model === "chronos")!;
    expect(pair.live!.modelMae).toBe(live.cells.chronos.live!.modelMae);
  });
});

describe("calendar questions (PAR-1037)", () => {
  const q = (id: string) => COMPARISON_QUESTIONS.find((x) => x.id === id)!;
  it("compares TFT's days with TFT and CatBoost's with CatBoost", () => {
    expect(q("cal_busier").reference).toBe("served_tft");
    expect(q("cal_far").leads).toEqual(["d30", "d45"]);
    expect(q("cal_far_90")).toMatchObject({
      servedModel: "catboost",
      leads: ["d90"],
      reference: "served_catboost",
    });
    expect(q("cal_rank")).toMatchObject({
      label: "Welcher Tag ist voller?",
      useCase: "D7",
      unit: "rate",
    });
  });

  it("has a grid-TFT shadow column on the TFT calendar questions, live only (PAR-1041)", () => {
    for (const id of ["cal_rank", "cal_busier", "cal_far"]) {
      expect(q(id).models.tft_grid).toEqual(["tft_grid"]);
      // PAR-1057: the drivers variant is its own column, not a replacement.
      expect(q(id).models.tft_grid_drivers).toEqual(["tft_grid_drivers"]);
    }
    expect(q("cal_far_90").models.tft_grid).toBeUndefined();
    expect(cellFloorDate("cal_far", "tft_grid_drivers")).toBe("2026-12-24");
    // Its rows begin with the shadow, not with the archive.
    expect(cellFloorDate("cal_far", "tft_grid")).toBe("2026-12-24");
    expect(cellFloorDate("cal_far", "wkd")).toBe("2026-12-07");
    const agg = Array.from({ length: 24 }, (_, i) => ({
      useCase: "PEAK",
      lead: "d3",
      model: "tft_grid",
      reference: "served_tft",
      segment: "headliner",
      parkId: `p${i % 12}`,
      dates: [i < 12 ? "2026-10-08" : "2026-10-09"],
      n: 5,
      saeModel: 20,
      saeRef: 25,
    }));
    const res = buildModelComparison({
      aggregates: agg,
      since: new Map(),
      verdicts: verdictsJson as unknown as OfflineVerdictFile,
      window: { days: 28, firstDate: "2026-10-08", lastDate: "2026-10-09" },
      today: "2026-10-10",
      generatedAt: "2026-10-10T08:00:00.000Z",
    });
    const busier = res.questions.find((x) => x.id === "cal_busier")!;
    const cell = busier.cells.tft_grid;
    expect(cell.offline).toBeNull();
    expect(cell.live!.modelMae).toBe(4);
    expect(cell.live!.referenceMae).toBe(5);
    expect(Object.keys(busier.cells).slice(0, 2)).toEqual(["tft", "tft_grid"]);
  });

  it("forms D7 day pairs within one model's rows (PAR-1041)", () => {
    const row = (model: string, date: string, pred: number, truth: number) => ({
      model,
      parkId: "p1",
      lead: "d3",
      date,
      n: 1,
      sumModel: pred,
      sumRef: 40,
      sumTruth: truth,
    });
    const out = d7AggregatesByModel([
      row("weekday_avg", "2026-10-01", 30, 30),
      row("weekday_avg", "2026-10-02", 40, 50),
      // The grid shadow had no value on 10-01: no pair for it.
      row("tft_grid", "2026-10-02", 10, 50),
      row("tft_grid", "2026-10-03", 20, 70),
    ]);
    const of = (m: string) => out.filter((a) => a.model === m);
    expect(of("weekday_avg").map((a) => a.dates)).toEqual([
      ["2026-10-02", "2026-10-01"],
    ]);
    expect(of("tft_grid").map((a) => a.dates)).toEqual([
      ["2026-10-03", "2026-10-02"],
    ]);
    expect(of("tft_grid")[0]).toMatchObject({ n: 1, saeModel: 0 });
  });

  it("gives CatBoost a live figure against the weekday average on its own days", () => {
    const agg = (reference: string, a: number, b: number) =>
      Array.from({ length: 24 }, (_, i) => ({
        useCase: "PEAK",
        lead: "d3",
        model: "weekday_avg",
        reference,
        segment: "headliner",
        parkId: `p${i % 12}`,
        dates: [i < 12 ? "2026-10-08" : "2026-10-09"],
        n: 5,
        saeModel: (a + (i % 12)) / 2,
        saeRef: b / 2,
      }));
    const res = buildModelComparison({
      aggregates: [
        ...agg("served_catboost", 100, 60),
        ...agg("served_tft", 90, 50),
      ],
      since: new Map(),
      verdicts: verdictsJson as unknown as OfflineVerdictFile,
      window: { days: 28, firstDate: "2026-10-08", lastDate: "2026-10-08" },
      today: "2026-10-09",
      generatedAt: "2026-10-09T08:00:00.000Z",
    });
    const busier = res.questions.find((x) => x.id === "cal_busier")!;
    const cb = busier.comparisons!.find((c) => c.model === "catboost")!;
    // Stored as wkd vs CatBoost; shown as CatBoost vs wkd: 6.0 vs ~15.6.
    expect(cb.reference).toBe("wkd");
    expect(cb.live!.modelMae).toBe(6);
    expect(cb.live!.diff).toBeLessThan(0);
    // The served TFT cell's wkd column reads TFT's days only.
    expect(busier.cells.wkd.live!.referenceMae).toBe(5);
    expect(busier.cells.catboost.live).toBeNull();
  });

  // PAR-1042: CatBoost's shadow level is a cell against served TFT.
  it("gives CatBoost a live cell against TFT on the days TFT levelled", () => {
    const res = buildModelComparison({
      aggregates: Array.from({ length: 24 }, (_, i) => ({
        useCase: "PEAK",
        lead: "d30",
        model: "catboost_shadow",
        reference: "served_tft",
        segment: "headliner",
        parkId: `p${i % 12}`,
        dates: [i < 12 ? "2026-10-08" : "2026-10-09"],
        n: 5,
        saeModel: 40,
        saeRef: 25,
      })),
      since: new Map(),
      verdicts: verdictsJson as unknown as OfflineVerdictFile,
      window: { days: 28, firstDate: "2026-10-08", lastDate: "2026-10-09" },
      today: "2026-10-10",
      generatedAt: "2026-10-10T08:00:00.000Z",
    });
    const far = res.questions.find((x) => x.id === "cal_far")!;
    // 8 vs 5 min per ride-day: CatBoost 3 minutes behind TFT.
    expect(far.cells.catboost.live).toMatchObject({
      modelMae: 8,
      referenceMae: 5,
      diff: 3,
    });
  });

  // PAR-1058: the driver level is a column on all four calendar questions,
  // read under the reference of the model that serves each one.
  it("gives the driver level a live cell on every calendar question", () => {
    const rows = (lead: string, reference: string) =>
      Array.from({ length: 24 }, (_, i) => ({
        useCase: "PEAK",
        lead,
        model: "driver_level",
        reference,
        segment: "headliner",
        parkId: `p${i % 12}`,
        dates: [i < 12 ? "2026-10-08" : "2026-10-09"],
        n: 5,
        saeModel: 20,
        saeRef: 25,
      }));
    const res = buildModelComparison({
      aggregates: [
        ...rows("d30", "served_tft"),
        ...rows("d90", "served_catboost"),
      ],
      since: new Map(),
      verdicts: verdictsJson as unknown as OfflineVerdictFile,
      window: { days: 28, firstDate: "2026-10-08", lastDate: "2026-10-09" },
      today: "2026-10-10",
      generatedAt: "2026-10-10T08:00:00.000Z",
    });
    const cell = (id: string) =>
      res.questions.find((x) => x.id === id)!.cells.driver;
    // 4 vs 5 min per ride-day: the driver level one minute ahead.
    for (const id of ["cal_far", "cal_far_90"]) {
      expect(cell(id).live).toMatchObject({
        modelMae: 4,
        referenceMae: 5,
        diff: -1,
      });
    }
    // The D7 / near-lead questions have the column, with no rows yet.
    expect(cell("cal_rank")).toBeDefined();
    expect(cell("cal_busier")).toBeDefined();
    expect(res.models.find((m) => m.id === "driver")?.label).toBe(
      "Treiber-Niveau",
    );
  });

  it("orders a park's days: D7 counts wrongly ordered day pairs, ties half", () => {
    const row = (date: string, wkd: number, served: number, truth: number) => ({
      parkId: "p1",
      lead: "d3",
      date,
      n: 2,
      sumModel: 2 * wkd,
      sumRef: 2 * served,
      sumTruth: 2 * truth,
    });
    const out = d7Aggregates([
      row("2026-10-01", 30, 40, 40),
      row("2026-10-02", 30, 50, 60), // truth up
      row("2026-10-03", 40, 30, 20), // truth down
      row("2026-10-04", 30, 40, 42), // within 5 min of 10-01 → that pair skipped
    ]);
    const sum = (k: "n" | "saeModel" | "saeRef") =>
      out.reduce((s, a) => s + a[k], 0);
    // Pairs: (01,02) (01,03) (02,03) (02,04) (03,04) — (01,04) is under 5 min.
    expect(sum("n")).toBe(5);
    // Served orders all five the way they turned out.
    expect(sum("saeRef")).toBe(0);
    // wkd: (01,02) tie ½, (01,03) 40>30 but truth down → 1, (02,03) 1,
    // (02,04) tie ½, (03,04) 30<40 but truth up → 1 → 4 of 5, in percent.
    expect(sum("saeModel")).toBe(400);
    // Every date that took part is a date of the gate.
    expect(new Set(out.flatMap((a) => a.dates)).size).toBe(4);
  });

  it("asks the best-time Chronos cell only for the leads Chronos is archived at", () => {
    expect(cellLeads(q("plan_besttime"), ["chronos2_grid"])).toEqual([
      "d0",
      "d1",
      "d2",
      "d3",
      "d4",
      "d5",
      "d6",
      "d7",
    ]);
    expect(cellLeads(q("plan_besttime"), ["shadow_h5"])).toEqual(
      q("plan_besttime").leads,
    );
  });

  it("does not ask the CatBoost shadow for today, which CatBoost never forecasts", () => {
    expect(cellLeads(q("cal_rank"), ["catboost_shadow"])).toEqual(
      q("cal_rank").leads.filter((l) => l !== "d0"),
    );
    expect(cellLeads(q("cal_rank"), ["weekday_avg"])).toEqual(
      q("cal_rank").leads,
    );
  });
});
