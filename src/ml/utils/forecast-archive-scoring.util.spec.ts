import {
  ArchivedCurve,
  PAIRED_QUESTION_MODELS,
  PairedAccumulator,
  ScoreAccumulator,
  bestTimeOutcome,
  buildTruthSlots,
  calendarDayValue,
  dayLead,
  nextRideLater,
  crowdOrdinal,
  percentileCont,
  regionOf,
  scoreCrossParkCrowd,
  scoreParkDay,
  scorePairedModels,
  scorePairedShadows,
  shadowLevelAt,
  slotLeadBucket,
  spearman,
} from "./forecast-archive-scoring.util";
import { COMPARISON_QUESTIONS } from "./model-comparison.util";
import { SCORE_KEY_WIDTHS } from "../entities/forecast-archive-score.entity";
import { ARCHIVE_SOURCE_CODES } from "../entities/forecast-archive-curve.entity";

const T0 = Date.parse("2026-10-08T08:00:00Z");
const MIN = 60_000;
const SLOT = 15 * MIN;

function sumsOf(acc: ScoreAccumulator, key: string): Record<string, number> {
  const e = acc
    .entries()
    .find(
      (x) =>
        [x.region, x.useCase, x.lead, x.source, x.segment].join("|") === key,
    );
  return e?.sums ?? {};
}

describe("buildTruthSlots (BENCH-SPEC truth)", () => {
  it("forward-fills the change log to each slot midpoint inside the window", () => {
    const slots = buildTruthSlots(
      [
        { timestamp: T0 - 30 * MIN, status: "OPERATING", waitTime: 10 },
        { timestamp: T0 + 20 * MIN, status: "OPERATING", waitTime: 30 },
      ],
      [{ open: T0, close: T0 + 60 * MIN }],
    );
    // 08:00 (mid 08:07:30) → 10; 08:15 (mid 08:22:30) → 30; 08:30, 08:45 → 30
    expect([...slots.entries()]).toEqual([
      [T0, 10],
      [T0 + SLOT, 30],
      [T0 + 2 * SLOT, 30],
      [T0 + 3 * SLOT, 30],
    ]);
  });

  it("drops stale, non-operating, sub-5 and out-of-window slots", () => {
    const slots = buildTruthSlots(
      [
        { timestamp: T0 - 4 * 60 * MIN, status: "OPERATING", waitTime: 50 },
        { timestamp: T0 + 20 * MIN, status: "DOWN", waitTime: null },
        { timestamp: T0 + 35 * MIN, status: "OPERATING", waitTime: 0 },
        { timestamp: T0 + 50 * MIN, status: "OPERATING", waitTime: 25 },
      ],
      [{ open: T0, close: T0 + 60 * MIN }],
    );
    // 08:00 stale (> 3 h old), 08:15 DOWN, 08:30 wait 0 → only 08:45
    expect([...slots.entries()]).toEqual([[T0 + 3 * SLOT, 25]]);
  });

  it("returns nothing when there are no readings", () => {
    expect(buildTruthSlots([], [{ open: T0, close: T0 + SLOT }]).size).toBe(0);
  });
});

describe("helpers", () => {
  it("percentileCont matches Postgres percentile_cont", () => {
    expect(percentileCont([10, 20, 30, 40], 0.9)).toBeCloseTo(37);
    expect(percentileCont([], 0.9)).toBeNull();
  });

  it("spearman handles ties and constant input", () => {
    expect(spearman([1, 2, 3, 4], [10, 20, 30, 40])).toBeCloseTo(1);
    expect(spearman([1, 2, 3, 4], [40, 30, 20, 10])).toBeCloseTo(-1);
    expect(spearman([5, 5, 5], [1, 2, 3])).toBeNull();
    expect(spearman([1, 2], [1, 2])).toBeNull();
  });

  it("slot leads: UC1 up to two hours, never pooled beyond 48 h", () => {
    expect(slotLeadBucket(15)).toEqual({ useCase: "UC1", lead: "h0-1" });
    expect(slotLeadBucket(120)).toEqual({ useCase: "UC1", lead: "h1-2" });
    expect(slotLeadBucket(121)).toEqual({ useCase: "UC2", lead: "h2-6" });
    expect(slotLeadBucket(2000)).toEqual({ useCase: "UC2", lead: "h24-48" });
    expect(slotLeadBucket(3000)).toBeNull();
    expect(slotLeadBucket(-1)).toBeNull();
  });

  it("crowd ordinals skip unknown", () => {
    expect(crowdOrdinal("very_low")).toBe(0);
    expect(crowdOrdinal("extreme")).toBe(5);
    expect(crowdOrdinal("unknown")).toBeNull();
    expect(crowdOrdinal(null)).toBeNull();
  });

  it("regions", () => {
    expect(regionOf("europe")).toBe("EU");
    expect(regionOf("north-america")).toBe("NA");
    expect(regionOf("asia")).toBe("ASIA");
    expect(regionOf("oceania")).toBe("OTHER");
  });
});

describe("bestTimeOutcome (D3)", () => {
  const c = (i: number, pred: number, truth: number) => ({
    slot: T0 + i * SLOT,
    pred,
    truth,
    src: "catboost",
    band: null,
  });

  it("hits when the true minimum is in the predicted top-2", () => {
    const r = bestTimeOutcome([
      c(0, 10, 30),
      c(1, 15, 5),
      c(2, 40, 40),
      c(3, 50, 45),
    ]);
    expect(r).toEqual({ hitTop2: true, hit30: true, regret: 25 });
  });

  it("misses and charges the regret when the forecast picks the wrong end", () => {
    const r = bestTimeOutcome([
      c(0, 10, 50),
      c(1, 12, 45),
      c(2, 40, 40),
      c(8, 50, 5),
    ]);
    expect(r).toEqual({ hitTop2: false, hit30: false, regret: 45 });
  });

  it("needs four slots", () => {
    expect(bestTimeOutcome([c(0, 1, 1), c(1, 2, 2), c(2, 3, 3)])).toBeNull();
  });
});

describe("scoreParkDay", () => {
  const ride = "a1";
  const ride2 = "a2";
  const ride3 = "a3";
  const origin = T0 - 135 * MIN; // 05:45, so every slot is > 2 h ahead

  const truth = new Map<string, Map<number, number>>([
    [
      ride,
      new Map([0, 1, 2, 3].map((i) => [T0 + i * SLOT, [20, 40, 60, 30][i]])),
    ],
    [ride2, new Map([0, 1, 2, 3].map((i) => [T0 + i * SLOT, 10]))],
    [ride3, new Map([0, 1, 2, 3].map((i) => [T0 + i * SLOT, 80]))],
  ]);

  const hourly: ArchivedCurve = {
    attractionId: ride,
    surface: "park_hourly",
    originKind: "daily",
    originAt: origin,
    leadDays: 0,
    slotStart: T0,
    slotMinutes: 15,
    waits: [25, 35, 55, 35],
    sources: "ccpp",
    bands: [10, 10, null, null],
    dayPeak: null,
    expectedError: null,
    rideQ90: 50,
    isHeadliner: true,
  };
  const plan = (id: string, peak: number, wait: number): ArchivedCurve => ({
    attractionId: id,
    surface: "plan_day",
    originKind: "daily",
    originAt: origin,
    leadDays: 0,
    slotStart: T0,
    slotMinutes: 60,
    waits: [wait],
    sources: "m",
    bands: null,
    dayPeak: peak,
    expectedError: 8,
    rideQ90: 20,
    isHeadliner: id === ride,
  });

  it("scores slots per lead and source, ride-days, dayPeak, coverage and crowd", () => {
    const acc = new ScoreAccumulator();
    const obs = scoreParkDay(
      {
        region: "EU",
        curves: [
          hourly,
          plan(ride, 50, 40),
          plan(ride2, 15, 10),
          plan(ride3, 70, 80),
        ],
        parkDays: [
          {
            originAt: origin,
            leadDays: 0,
            tier: "measured",
            crowdLevel: "high",
            predictedCrowdLevel: "moderate",
            typicalDayPeak: 50,
            crowdLevelFallback: false,
          },
        ],
        truth,
        hadWindows: true,
        calendarDayValue: 54,
      },
      acc,
    );

    // Every slot is 2.25-3 h after the origin → h2-6 (UC2).
    const cat = sumsOf(acc, "EU|UC2|h2-6|catboost|all");
    expect(cat.n).toBe(2);
    expect(cat.sae).toBe(5 + 5);
    expect(cat.se).toBe(5 - 5);
    expect(cat.nBand).toBe(2);
    expect(cat.nBandCov).toBe(2);
    const pcn = sumsOf(acc, "EU|UC2|h2-6|pcn_blend|busy");
    expect(pcn.n).toBe(2);
    expect(sumsOf(acc, "ALL|UC2|h2-6|all|all").n).toBe(4);

    // Ride-day best time on the 15-min curve: predicted best 08:00 (25), true
    // best 08:00 (20) → hit, regret 0.
    const rd = sumsOf(acc, "EU|UC2|d0|mixed|all");
    expect(rd.rd).toBe(1);
    expect(rd.hitTop2).toBe(1);
    expect(rd.regret).toBe(0);

    // plan_day: hourly value 40 against 20/40/60/30.
    const uc3 = sumsOf(acc, "EU|UC3|d0|measured|all");
    expect(uc3.n).toBe(4 + 4 + 4);
    // dayPeak vs truth P90 (ride: P90 of 20,40,60,30 = 54), stated error 8.
    const peak = sumsOf(acc, "EU|UC3|d0|measured|headliner");
    expect(peak.nPeak).toBe(1);
    expect(peak.sPeakAe).toBeCloseTo(4);
    expect(peak.nStated).toBe(1);
    expect(peak.sStated).toBe(8);

    // Park-day ordering: 15 < 50 < 70 against 10 < 54 < 80 → perfect.
    const rank = sumsOf(acc, "EU|UC3|d0|all|all");
    expect(rank.nRank).toBe(1);
    expect(rank.sRank).toBeCloseTo(1);
    expect(rank.pairs).toBe(3);
    expect(rank.pairsOk).toBe(3);

    // D9: three rides ran, all three offered.
    const cov = sumsOf(acc, "EU|D9|d0|measured|all");
    expect(cov).toMatchObject({
      truthRides: 3,
      truthRidesOffered: 3,
      offered: 3,
      offeredNotOperating: 0,
      parkDays: 1,
      parkDaysEmpty: 0,
    });

    // D6: calendar day value 54 ÷ the origin's baseline 50 = 108 % → moderate.
    const d6 = sumsOf(acc, "EU|D6|d0|predicted|all");
    expect(d6).toMatchObject({ n: 1, exact: 1, within1: 1, busyTrue: 0 });
    // d0's planner crowdLevel is the live-overridden one: scored apart.
    const d6plan = sumsOf(acc, "EU|D6|d0|plan_day_live|all");
    expect(d6plan).toMatchObject({ n: 1, exact: 0, within1: 1, busyPred: 1 });
    expect(obs).toHaveLength(2);
  });

  it("counts an empty plan against a park whose rides ran (D9)", () => {
    const acc = new ScoreAccumulator();
    scoreParkDay(
      {
        region: "NA",
        curves: [],
        parkDays: [
          {
            originAt: origin,
            leadDays: 3,
            tier: "composed",
            crowdLevel: null,
            predictedCrowdLevel: "unknown",
          },
        ],
        truth,
        hadWindows: true,
        calendarDayValue: null,
      },
      acc,
    );
    expect(sumsOf(acc, "NA|D9|d3|composed|all")).toMatchObject({
      truthRides: 3,
      truthRidesOffered: 0,
      parkDaysEmpty: 1,
    });
    // Not ratable → no D6 row at all.
    expect(acc.entries().some((e) => e.useCase === "D6")).toBe(false);
  });

  it("intraday origins score slots but never ride-days", () => {
    const acc = new ScoreAccumulator();
    scoreParkDay(
      {
        region: "EU",
        curves: [
          { ...hourly, originKind: "intraday", originAt: T0 - 30 * MIN },
        ],
        parkDays: [],
        truth,
        hadWindows: true,
        calendarDayValue: null,
      },
      acc,
    );
    expect(sumsOf(acc, "EU|UC1|h0-1|all|all").n).toBe(3);
    expect(acc.entries().some((e) => e.lead.startsWith("d"))).toBe(false);
  });
});

describe("nextRideLater (frontend next-best-ride rule)", () => {
  const hours = [10, 11, 12, 13, 14, 15].map((hour) => ({
    hour,
    wait: hour * 2,
  }));

  it("takes the max over hour STARTS in [now, now + 120] before close", () => {
    // 11:10 → hours starting 12:00 and 13:00 qualify; 11:00 has begun.
    expect(nextRideLater(hours, 10, 18, 11 * 60 + 10)).toEqual({
      hour: 13,
      wait: 26,
    });
    // The close hour itself is not an open hour.
    expect(nextRideLater(hours, 10, 13, 11 * 60 + 10)).toEqual({
      hour: 12,
      wait: 24,
    });
    expect(nextRideLater(hours, 10, 18, 16 * 60)).toBeNull();
  });

  it("unfolds a day that runs past midnight", () => {
    const late = [22, 23, 24].map((hour) => ({ hour, wait: hour }));
    // 22:30 at a 16 → 1 park: 23:00 and 00:00 (axis 24) qualify.
    expect(nextRideLater(late, 16, 1, 22 * 60 + 30)).toEqual({
      hour: 24,
      wait: 24,
    });
  });
});

describe("D1 next-best-ride", () => {
  const origin = T0;
  // An intraday plan row: hours 08:00 … 10:00, later = 40 at 09:00.
  const base: ArchivedCurve = {
    attractionId: "r",
    surface: "plan_day",
    originKind: "intraday",
    originAt: origin,
    leadDays: 0,
    slotStart: origin,
    slotMinutes: 60,
    waits: [20, 40, 30],
    sources: "mkm",
    bands: null,
    dayPeak: 40,
    expectedError: null,
    rideQ90: null,
    isHeadliner: false,
    liveWait: 20,
    laterWait: 40,
    laterAt: origin + 60 * MIN,
  };
  const truthOf = (values: number[]) =>
    new Map<string, Map<number, number>>([
      ["r", new Map(values.map((v, i) => [origin + (i + 1) * SLOT, v]))],
    ]);
  const run = (curve: ArchivedCurve, values: number[]) => {
    const acc = new ScoreAccumulator();
    scoreParkDay(
      {
        region: "EU",
        curves: [curve],
        parkDays: [],
        truth: truthOf(values),
        hadWindows: true,
        calendarDayValue: null,
      },
      acc,
    );
    return acc;
  };

  it("counts a suggestion that held, under the later hour's lead and source", () => {
    const acc = run(base, [20, 25, 35, 40]);
    expect(sumsOf(acc, "EU|D1|h0-2|all|all")).toEqual({
      d1Sugg: 1,
      d1SuggOk: 1,
    });
    expect(sumsOf(acc, "EU|D1|h0-1|composed|all")).toEqual({
      d1Sugg: 1,
      d1SuggOk: 1,
    });
    // An intraday plan row feeds D1 only.
    expect(acc.entries().some((e) => e.useCase === "UC3")).toBe(false);
  });

  it("counts a suggestion that did not hold", () => {
    const acc = run(base, [20, 22, 25, 25]);
    expect(sumsOf(acc, "EU|D1|h0-2|all|all")).toEqual({
      d1Sugg: 1,
      d1SuggOk: 0,
    });
  });

  it("records the base rate when the gap is under 10 minutes", () => {
    const acc = run({ ...base, liveWait: 35 }, [35, 50]);
    expect(sumsOf(acc, "EU|D1|h0-2|all|all")).toEqual({
      d1None: 1,
      d1NoneWorse: 1,
    });
  });

  it("no qualifying hour is counted apart from a gap under 10", () => {
    const acc = run({ ...base, laterWait: null, laterAt: null }, [20, 20]);
    expect(sumsOf(acc, "EU|D1|h0-2|all|all")).toEqual({
      d1NoHour: 1,
      d1NoHourWorse: 0,
    });
  });

  it("needs a live anchor", () => {
    const acc = run({ ...base, liveWait: null }, [20, 40]);
    expect(acc.entries().some((e) => e.useCase === "D1")).toBe(false);
  });
});

describe("level-source split and long leads", () => {
  it("writes UC3 and D9 under the level source too, and scores long-lead ride-days", () => {
    const origin = T0 - 135 * MIN;
    const acc = new ScoreAccumulator();
    scoreParkDay(
      {
        region: "EU",
        curves: [
          {
            attractionId: "a1",
            surface: "plan_day",
            originKind: "long",
            originAt: origin,
            leadDays: 30,
            slotStart: T0,
            slotMinutes: 60,
            waits: [30],
            sources: "k",
            bands: null,
            dayPeak: 40,
            expectedError: 15,
            rideQ90: null,
            isHeadliner: false,
            levelSource: "catboost",
          },
        ],
        parkDays: [
          {
            originAt: origin,
            leadDays: 30,
            tier: "composed",
            crowdLevel: null,
            predictedCrowdLevel: null,
            levelSource: "catboost",
          },
        ],
        truth: new Map([
          ["a1", new Map([0, 1, 2, 3].map((i) => [T0 + i * SLOT, 30 + i * 5]))],
        ]),
        hadWindows: true,
        calendarDayValue: null,
      },
      acc,
    );
    expect(sumsOf(acc, "EU|UC3|d30|level_catboost|all").n).toBe(4);
    expect(sumsOf(acc, "EU|UC3|d30|level_catboost|all").nStated).toBe(1);
    expect(sumsOf(acc, "EU|UC3|d30|composed|all").rd).toBe(1);
    expect(sumsOf(acc, "EU|D9|d30|level_catboost|all").truthRidesOffered).toBe(
      1,
    );
  });
});

describe("composer A/B shadow surfaces (PAR-834)", () => {
  it("scores each composer under UC3S, apart from each other and from UC3", () => {
    const origin = T0 - 135 * MIN;
    const curve = (
      surface: ArchivedCurve["surface"],
      waits: number[],
      sources: string,
    ): ArchivedCurve => ({
      attractionId: "a1",
      surface,
      originKind: "daily",
      originAt: origin,
      leadDays: 3,
      slotStart: T0,
      slotMinutes: 15,
      waits,
      sources,
      bands: null,
      dayPeak: null,
      expectedError: null,
      rideQ90: 60,
      isHeadliner: false,
      levelSource: null,
    });
    const acc = new ScoreAccumulator();
    scoreParkDay(
      {
        region: "EU",
        curves: [
          curve("plan_day_shadow_peak", [40, 40, 40, 40], "kkkk"),
          curve("plan_day_shadow_h5", [30, 35, 40, 45], "hhhh"),
          curve("plan_day_shadow_routed", [45, 50, 60, 65], "tttt"),
        ],
        parkDays: [],
        truth: new Map([
          ["a1", new Map([0, 1, 2, 3].map((i) => [T0 + i * SLOT, 30 + i * 5]))],
        ]),
        hadWindows: false,
        calendarDayValue: null,
      },
      acc,
    );
    expect(sumsOf(acc, "EU|UC3S|d3|shadow_peak_scaled|all")).toMatchObject({
      n: 4,
      sae: 10 + 5 + 0 + 5,
    });
    expect(sumsOf(acc, "EU|UC3S|d3|shadow_h5|all")).toMatchObject({
      n: 4,
      sae: 0,
      rd: 1,
    });
    expect(sumsOf(acc, "EU|UC3S|d3|shadow_h5_routed|all").n).toBe(4);
    // Never pooled: the three are the same slots three times.
    expect(acc.entries().some((e) => e.source === "all")).toBe(false);
    expect(acc.entries().some((e) => e.useCase === "UC3")).toBe(false);

    // Without a routed row (no level applied) the plain row stands in for it.
    const acc2 = new ScoreAccumulator();
    scoreParkDay(
      {
        region: "EU",
        curves: [curve("plan_day_shadow_h5", [30, 35, 40, 45], "hhhh")],
        parkDays: [],
        truth: new Map([
          ["a1", new Map([0, 1, 2, 3].map((i) => [T0 + i * SLOT, 30 + i * 5]))],
        ]),
        hadWindows: false,
        calendarDayValue: null,
      },
      acc2,
    );
    expect(sumsOf(acc2, "EU|UC3S|d3|shadow_h5_routed|all").n).toBe(4);
    expect(sumsOf(acc2, "EU|UC3S|d3|shadow_h5|all").n).toBe(4);
  });
});

describe("Chronos-2 shadow surfaces (PAR-852)", () => {
  // Intraday origin 08:05; truth 08:15 … 09:00 = 30, 35, 40, 45.
  const origin = T0 + 5 * MIN;
  const truth = new Map([
    ["a1", new Map([1, 2, 3, 4].map((i) => [T0 + i * SLOT, 25 + i * 5]))],
  ]);
  const curve = (
    surface: ArchivedCurve["surface"],
    waits: (number | null)[],
    sources: string,
    extra: Partial<ArchivedCurve> = {},
  ): ArchivedCurve => ({
    attractionId: "a1",
    surface,
    originKind: "intraday",
    originAt: origin,
    leadDays: 0,
    slotStart: T0,
    slotMinutes: 15,
    waits,
    sources,
    bands: null,
    dayPeak: null,
    expectedError: null,
    rideQ90: 20,
    isHeadliner: false,
    levelSource: null,
    ...extra,
  });

  it("scores the live shadow on the served slot leads under its own source, never into all", () => {
    const acc = new ScoreAccumulator();
    scoreParkDay(
      {
        region: "EU",
        curves: [
          curve("park_hourly", [20, 20, 20, 20, 20], "ccccc"),
          curve("chronos_live", [null, 30, 35, 40, 45], "-xxxx", {
            bands: [null, 5, 5, 5, 5],
            liveWait: 25,
          }),
        ],
        parkDays: [],
        truth,
        hadWindows: false,
        calendarDayValue: null,
      },
      acc,
    );
    expect(sumsOf(acc, "EU|UC1|h0-1|chronos2|all")).toMatchObject({
      n: 4,
      sae: 0,
      nBand: 4,
      nBandCov: 4,
    });
    // `all` is the served curve alone: 20 vs 30/35/40/45.
    expect(sumsOf(acc, "EU|UC1|h0-1|all|all")).toMatchObject({
      n: 4,
      sae: 10 + 15 + 20 + 25,
    });
  });

  it("pairs live with the served curve and persistence on identical slots", () => {
    const paired = new PairedAccumulator();
    scorePairedShadows(
      [
        // The served curve lacks 08:30: that slot is paired with persistence only.
        curve("park_hourly", [20, 20, null, 20, 20], "cc-cc"),
        curve("chronos_live", [null, 30, 35, 40, 45], "-xxxx", {
          liveWait: 25,
        }),
        // Another origin's served row must not be paired.
        curve("park_hourly", [0, 0, 0, 0, 0], "ccccc", {
          originAt: origin + 2 * 60 * MIN,
        }),
      ],
      truth,
      paired,
    );
    const row = (ref: string) =>
      paired
        .entries()
        .find(
          (e) =>
            e.reference === ref &&
            e.segment === "all" &&
            e.lead === "h0-1" &&
            e.model === "chronos2",
        );
    expect(row("served")).toMatchObject({
      useCase: "UC1",
      n: 3,
      saeModel: 0,
      saeRef: 10 + 20 + 25,
    });
    expect(row("persistence")).toMatchObject({
      n: 4,
      saeRef: 5 + 10 + 15 + 20,
      seRef: -(5 + 10 + 15 + 20),
    });
    // rideQ90 20 < 45: no busy segment, not a headliner.
    expect(paired.entries().map((e) => e.segment)).not.toContain("busy");
  });
  // PAR-911: once Chronos is served, park_hourly carries it on 45–120 min and
  // the incumbent (surface 8) is what the shadow must be paired with.
  it("pairs live with the incumbent when it was archived, not with the served copy of itself", () => {
    const paired = new PairedAccumulator();
    scorePairedShadows(
      [
        curve("park_hourly", [null, 30, 35, 40, 45], "-ssss"),
        curve("park_hourly_incumbent", [20, 20, 20, 20, 20], "ppppp"),
        curve("chronos_live", [null, 30, 35, 40, 45], "-xxxx"),
      ],
      truth,
      paired,
    );
    const served = paired
      .entries()
      .find(
        (e) =>
          e.reference === "served" &&
          e.segment === "all" &&
          e.lead === "h0-1" &&
          e.model === "chronos2",
      );
    expect(served).toMatchObject({
      n: 4,
      saeModel: 0,
      saeRef: 10 + 15 + 20 + 25,
    });
  });

  it("scores the incumbent under its own source and never into all", () => {
    const acc = new ScoreAccumulator();
    scoreParkDay(
      {
        region: "EU",
        curves: [
          curve("park_hourly", [null, 30, 35, 40, 45], "-ssss"),
          curve("park_hourly_incumbent", [20, 20, 20, 20, 20], "ppppp"),
        ],
        parkDays: [],
        truth,
        hadWindows: false,
        calendarDayValue: null,
      },
      acc,
    );
    expect(sumsOf(acc, "EU|UC1|h0-1|incumbent|all")).toMatchObject({
      n: 4,
      sae: 10 + 15 + 20 + 25,
    });
    // `all` is the served curve alone, its Chronos slots under their own code.
    expect(sumsOf(acc, "EU|UC1|h0-1|all|all")).toMatchObject({ n: 4, sae: 0 });
    expect(sumsOf(acc, "EU|UC1|h0-1|chronos2_served|all")).toMatchObject({
      n: 4,
    });
  });
});

describe("scoreCrossParkCrowd (D6 pairs)", () => {
  it("counts pairs within a region and over all parks, without double counting", () => {
    const acc = new ScoreAccumulator();
    scoreCrossParkCrowd(
      [
        {
          region: "EU",
          lead: "d1",
          source: "predicted",
          predicted: 1,
          truth: 1,
          truthRatio: 80,
        },
        {
          region: "EU",
          lead: "d1",
          source: "predicted",
          predicted: 3,
          truth: 3,
          truthRatio: 130,
        },
        {
          region: "NA",
          lead: "d1",
          source: "predicted",
          predicted: 2,
          truth: 0,
          truthRatio: 50,
        },
      ],
      acc,
    );
    expect(sumsOf(acc, "EU|D6|d1|predicted|all")).toEqual({
      crossPairs: 1,
      crossPairsOk: 1,
    });
    // ALL: EU1-EU3 ok, EU1-NA wrong (truth 1>0, pred 1<2), EU3-NA ok.
    expect(sumsOf(acc, "ALL|D6|d1|predicted|all")).toEqual({
      crossPairs: 3,
      crossPairsOk: 2,
    });
  });
});

describe("review fixes", () => {
  const origin = T0 - 135 * MIN;
  const planCurve = (over: Partial<ArchivedCurve>): ArchivedCurve => ({
    attractionId: "a1",
    surface: "plan_day",
    originKind: "daily",
    originAt: origin,
    leadDays: 2,
    slotStart: T0,
    slotMinutes: 60,
    waits: [30],
    sources: "m",
    bands: null,
    dayPeak: 40,
    expectedError: null,
    rideQ90: null,
    isHeadliner: false,
    ...over,
  });
  const truth = new Map([
    ["a1", new Map([0, 1, 2, 3].map((i) => [T0 + i * SLOT, 30 + i * 5]))],
  ]);
  const score = (curves: ArchivedCurve[]) => {
    const acc = new ScoreAccumulator();
    scoreParkDay(
      {
        region: "EU",
        curves,
        parkDays: [],
        truth,
        hadWindows: true,
        calendarDayValue: null,
      },
      acc,
    );
    return acc;
  };

  it("scores the plan band one-sided: an over-forecast is always covered", () => {
    // dayPeak 60 against truth P90 43.5: 16.5 over, band 3 — inside, because
    // the band only bounds how much LONGER the day can run.
    const over = sumsOf(
      score([planCurve({ dayPeak: 60, peakBand: 3 })]),
      "EU|UC3|d2|all|all",
    );
    expect(over).toMatchObject({ nPeakBand: 1, nPeakBandCov: 1 });
    // Under-forecast by 13.5 with a 10-min band: outside.
    const under = sumsOf(
      score([planCurve({ dayPeak: 30, peakBand: 10 })]),
      "EU|UC3|d2|all|all",
    );
    expect(under).toMatchObject({ nPeakBand: 1, nPeakBandCov: 0 });
  });

  it("scores the plan band around dayPeak per ride-day, never per hour", () => {
    // truth P90 of 30,35,40,45 = 43.5; 43.5 − 40 = 3.5 ≤ 5 → covered.
    const acc = score([planCurve({ peakBand: 5 })]);
    const s = sumsOf(acc, "EU|UC3|d2|measured|all");
    expect(s.nPeakBand).toBe(1);
    expect(s.nPeakBandCov).toBe(1);
    expect(s.nBand).toBeUndefined();
    expect(
      sumsOf(score([planCurve({ peakBand: 3 })]), "EU|UC3|d2|all|all")
        .nPeakBandCov,
    ).toBe(0);
  });

  it("files a measured hour's numbers under the level source only for dayPeak", () => {
    const acc = score([planCurve({ levelSource: "tft" })]);
    const lvl = sumsOf(acc, "EU|UC3|d2|level_tft|all");
    expect(lvl.nPeak).toBe(1);
    expect(lvl.n).toBeUndefined();
    expect(lvl.rd).toBeUndefined();
    const composed = sumsOf(
      score([planCurve({ levelSource: "tft", sources: "k" })]),
      "EU|UC3|d2|level_tft|all",
    );
    expect(composed.n).toBe(4);
    expect(composed.rd).toBe(1);
  });

  it("a flat ride-day counts for regret, not for the hit rates", () => {
    const c = (i: number, pred: number, truthValue: number) => ({
      slot: T0 + i * SLOT,
      pred,
      truth: truthValue,
      src: "catboost",
      band: null,
    });
    // 5 min in three of four slots: any forecast would "hit".
    expect(
      bestTimeOutcome([c(0, 30, 5), c(1, 10, 5), c(2, 20, 5), c(3, 5, 20)]),
    ).toEqual({ hitTop2: null, hit30: null, regret: 15 });
  });

  it("calendarDayValue mirrors the typical-day-peak statistic", () => {
    const inDay = (t: number) => t >= T0 && t < T0 + 24 * 60 * MIN;
    const readings = new Map([
      [
        "h1",
        [
          { timestamp: T0, status: "OPERATING", waitTime: 10 },
          { timestamp: T0 + MIN, status: "OPERATING", waitTime: 20 },
          { timestamp: T0 + 2 * MIN, status: "OPERATING", waitTime: 5 },
          { timestamp: T0 + 3 * MIN, status: "DOWN", waitTime: 90 },
          { timestamp: T0 - MIN, status: "OPERATING", waitTime: 99 },
        ],
      ],
      ["h2", [{ timestamp: T0, status: "OPERATING", waitTime: 40 }]],
      ["other", [{ timestamp: T0, status: "OPERATING", waitTime: 100 }]],
    ]);
    // h1: P90 of [10, 20] = 19; h2: 40 → mean 29.5. Below 10, DOWN, the
    // previous day and non-headliners do not count.
    expect(
      calendarDayValue(readings, new Set(["h1", "h2"]), inDay),
    ).toBeCloseTo(29.5);
    expect(calendarDayValue(readings, new Set(["none"]), inDay)).toBeNull();
  });

  it("D6 scores the planner's fallback crowdLevel apart", () => {
    const acc = new ScoreAccumulator();
    scoreParkDay(
      {
        region: "EU",
        curves: [],
        parkDays: [
          {
            originAt: origin,
            leadDays: 3,
            tier: "composed",
            crowdLevel: "moderate",
            predictedCrowdLevel: null,
            typicalDayPeak: 40,
            crowdLevelFallback: true,
          },
        ],
        truth,
        hadWindows: true,
        calendarDayValue: 40,
      },
      acc,
    );
    expect(sumsOf(acc, "EU|D6|d3|fallback|all").n).toBe(1);
    expect(sumsOf(acc, "EU|D6|d3|predicted|all")).toEqual({ unknownPred: 1 });
    expect(acc.entries().some((e) => e.source === "plan_day")).toBe(false);
  });

  it("every key the scorer can emit fits its column (B1)", () => {
    const sources = [
      ...Object.values(ARCHIVE_SOURCE_CODES),
      "all",
      "mixed",
      "unknown",
      ...["tft", "catboost", "climatology", "mixed", "none"].map(
        (l) => `level_${l}`,
      ),
      "observed",
      "measured",
      "composed",
      "climatology",
      "long_range",
      "none",
      "predicted",
      "plan_day",
      "plan_day_live",
      "fallback",
      "shadow_peak_scaled",
      "shadow_h5",
      "shadow_h5_routed",
    ];
    const leads = [
      "h0-1",
      "h1-2",
      "h0-2",
      "h2-6",
      "h6-12",
      "h12-24",
      "h24-48",
      ...Array.from({ length: 366 }, (_, i) => dayLead(i)),
    ];
    const tooLong = (values: string[], width: number) =>
      values.filter((v) => v.length > width);
    expect(tooLong(sources, SCORE_KEY_WIDTHS.source)).toEqual([]);
    expect(tooLong(leads, SCORE_KEY_WIDTHS.lead)).toEqual([]);
    expect(
      tooLong(["EU", "NA", "ASIA", "OTHER", "ALL"], SCORE_KEY_WIDTHS.region),
    ).toEqual([]);
    expect(
      tooLong(
        ["UC1", "UC2", "UC3", "D1", "D6", "D9"],
        SCORE_KEY_WIDTHS.useCase,
      ),
    ).toEqual([]);
    expect(
      tooLong(["all", "busy", "headliner"], SCORE_KEY_WIDTHS.segment),
    ).toEqual([]);

    // And what a mixed scenario actually emits.
    const acc = score([
      planCurve({ levelSource: "climatology", sources: "l", leadDays: 90 }),
      planCurve({
        surface: "park_hourly",
        slotMinutes: 15,
        waits: [30, 35, 40, 45],
        sources: "cpcp",
        bands: [5, 5, 5, 5],
        rideQ90: 60,
        isHeadliner: true,
      }),
    ]);
    for (const e of acc.entries()) {
      expect(e.source.length).toBeLessThanOrEqual(SCORE_KEY_WIDTHS.source);
      expect(e.lead.length).toBeLessThanOrEqual(SCORE_KEY_WIDTHS.lead);
      expect(e.useCase.length).toBeLessThanOrEqual(SCORE_KEY_WIDTHS.useCase);
      expect(e.segment.length).toBeLessThanOrEqual(SCORE_KEY_WIDTHS.segment);
      expect(e.region.length).toBeLessThanOrEqual(SCORE_KEY_WIDTHS.region);
    }
    expect(acc.entries().some((e) => e.source === "level_climatology")).toBe(
      true,
    );
  });
});

describe("scorePairedModels — joint rows per question (PAR-855, PAR-1035)", () => {
  // Truth 08:00–09:45 UTC on 2026-10-08.
  const truthValues = [30, 30, 40, 40, 50, 50, 20, 20];
  const truth = new Map([
    ["a1", new Map(truthValues.map((v, i) => [T0 + i * SLOT, v]))],
  ]);
  const daily = T0 - 125 * MIN; // 05:55
  const curve = (
    surface: ArchivedCurve["surface"],
    waits: (number | null)[],
    extra: Partial<ArchivedCurve> = {},
  ): ArchivedCurve => ({
    attractionId: "a1",
    surface,
    originKind: "daily",
    originAt: daily,
    leadDays: 0,
    slotStart: T0,
    slotMinutes: 15,
    waits,
    sources: "c".repeat(waits.length),
    bands: null,
    dayPeak: null,
    expectedError: null,
    rideQ90: 50,
    isHeadliner: true,
    levelSource: null,
    ...extra,
  });
  const find = (
    paired: PairedAccumulator,
    useCase: string,
    lead: string,
    model: string,
    reference = "served_joint",
    segment = "all",
  ) =>
    paired
      .entries()
      .find(
        (e) =>
          e.useCase === useCase &&
          e.lead === lead &&
          e.model === model &&
          e.reference === reference &&
          e.segment === segment,
      );
  const flat = (v: number) => Array.from({ length: 8 }, () => v);
  const intraday = { originKind: "intraday" as const, originAt: T0 };

  it("pairs every live candidate on the same slots, from the 10–18 h origins only", () => {
    const paired = new PairedAccumulator();
    scorePairedModels(
      [
        // 06:00: served hourly and the daily H5 curve, no live wait, no Chronos.
        curve("park_hourly", flat(30)),
        curve("plan_day_shadow_h5", truthValues),
        // 08:00 intraday origin: served, the live wait, Chronos live.
        curve("park_hourly", flat(30), { ...intraday, liveWait: 25 }),
        curve("chronos_live", flat(35), intraday),
      ],
      truth,
      paired,
    );
    // Slots 08:00–09:00 are 0–60 min after the 08:00 origin, 09:15–09:45 are
    // 75–105 min. H5 is the 06:00 curve, the newest issued before 08:00.
    for (const m of ["persistence", "chronos2", "shadow_h5"]) {
      expect(find(paired, "UC1", "h0-1", m)?.n).toBe(5);
    }
    for (const m of [
      "persistence",
      "chronos2",
      "shadow_h5",
      "shadow_h5_routed",
    ]) {
      expect(find(paired, "UC1", "h1-2", m)?.n).toBe(3);
    }
    // One reference MAE per question: every candidate saw the same slots.
    const refs = paired
      .entries()
      .filter(
        (e) =>
          e.useCase === "UC1" &&
          e.lead === "h0-1" &&
          e.reference === "served_joint",
      )
      .map((e) => e.saeRef);
    expect(new Set(refs).size).toBe(1);
    // The 06:00 origin has no Chronos live curve: its 08:00–09:45 slots
    // (2–4 h ahead, rope drop where a park opens early) feed no live cell.
    expect(find(paired, "UC2", "h2-6", "shadow_h5")).toBeUndefined();
    // Direct comparison on the same slots.
    expect(find(paired, "UC1", "h0-1", "chronos2", "shadow_h5")).toMatchObject({
      n: 5,
      saeRef: find(paired, "UC1", "h0-1", "shadow_h5")!.saeModel,
    });
  });

  it("drops a slot for every candidate when a core one has no value there", () => {
    const paired = new PairedAccumulator();
    const h5 = [...truthValues] as (number | null)[];
    h5[1] = null;
    scorePairedModels(
      [
        curve("plan_day_shadow_h5", h5),
        curve("park_hourly", flat(30), { ...intraday, liveWait: 25 }),
        curve("chronos_live", flat(35), intraday),
      ],
      truth,
      paired,
    );
    for (const m of ["persistence", "chronos2", "shadow_h5"]) {
      expect(find(paired, "UC1", "h0-1", m)?.n).toBe(4);
    }
  });

  it("lets an optional shadow's outage cost only its own cell", () => {
    // Chronos-2 (nf-service) misses a slot, then is down altogether: the core
    // candidates keep every slot.
    const chronos = flat(35) as (number | null)[];
    chronos[1] = null;
    const run = (extra: ArchivedCurve[]) => {
      const paired = new PairedAccumulator();
      scorePairedModels(
        [
          curve("plan_day_shadow_h5", truthValues),
          curve("park_hourly", flat(30), { ...intraday, liveWait: 25 }),
          ...extra,
        ],
        truth,
        paired,
      );
      return paired;
    };
    const partial = run([curve("chronos_live", chronos, intraday)]);
    expect(find(partial, "UC1", "h0-1", "chronos2")?.n).toBe(4);
    expect(find(partial, "UC1", "h0-1", "shadow_h5")?.n).toBe(5);
    expect(find(partial, "UC1", "h0-1", "chronos2", "shadow_h5")?.n).toBe(4);
    const down = run([]);
    expect(find(down, "UC1", "h0-1", "chronos2")).toBeUndefined();
    expect(find(down, "UC1", "h0-1", "persistence")?.n).toBe(5);
  });

  it("keeps the 06:00 origin out of the live questions even where a park is open", () => {
    const paired = new PairedAccumulator();
    scorePairedModels(
      [
        // A park already open at 06:00: live wait, H5 — still rope drop.
        curve("park_hourly", flat(30), { liveWait: 25 }),
        curve("plan_day_shadow_h5", truthValues),
      ],
      truth,
      paired,
    );
    expect(
      paired.entries().filter((e) => e.lead !== "h6-12" && e.useCase !== "UC3"),
    ).toEqual([]);
  });

  it("never stands in yesterday's daily curve for today's", () => {
    const paired = new PairedAccumulator();
    scorePairedModels(
      [
        // only the d1 H5 curve from the day before exists
        curve("plan_day_shadow_h5", truthValues, {
          leadDays: 1,
          originAt: daily - 86_400_000,
        }),
        curve("park_hourly", flat(30), { ...intraday, liveWait: 25 }),
        curve("chronos_live", flat(35), intraday),
      ],
      truth,
      paired,
    );
    expect(paired.entries().filter((e) => e.useCase === "UC1")).toEqual([]);
  });

  const clock = (ms: number) => new Date(ms).toISOString().slice(11, 16);
  const flatDay = (v: number) =>
    new Map(Array.from({ length: 8 }, (_, i) => [clock(T0 + i * SLOT), v]));
  // Same weekday as 2026-10-08 (Thursday): 01.10. and 24.09.; and the target
  // day itself, which must never count.
  const weekday = {
    targetDate: "2026-10-08",
    clock,
    history: new Map([
      [
        "a1",
        new Map([
          ["2026-10-08", flatDay(999)],
          ["2026-10-01", flatDay(30)],
          ["2026-09-24", flatDay(40)],
        ]),
      ],
    ]),
  };
  const planner = (extra: Partial<ArchivedCurve> = {}) => [
    curve("plan_day", [30, 45], { slotMinutes: 60, dayPeak: 45, ...extra }),
    curve("plan_day_shadow_h5", truthValues),
    curve("plan_day_shadow_peak", [30, 30, 30, 30, 45, 45, 45, 45]),
    curve("chronos_grid", flat(30)),
  ];

  it("pairs the planner candidates on the same slots and ride-days", () => {
    const paired = new PairedAccumulator();
    scorePairedModels(planner(), truth, paired, weekday);
    const models = [
      "shadow_h5",
      "shadow_h5_routed",
      "shadow_peak_scaled",
      "weekday_avg",
      "chronos2_grid",
    ];
    for (const m of models) {
      expect(find(paired, "UC3", "d0", m)).toMatchObject({
        n: 8,
        saeRef: 0 + 0 + 10 + 10 + 5 + 5 + 25 + 25,
      });
      expect(find(paired, "D3", "d0", m)).toMatchObject({ n: 1, saeRef: 10 });
    }
    // Weekday average = 35 on every slot (the target day's 999 never counts).
    expect(find(paired, "UC3", "d0", "weekday_avg")!.saeModel).toBe(
      5 + 5 + 5 + 5 + 15 + 15 + 15 + 15,
    );
    // H5 picks 09:30 (truth 20, regret 0); the plan picks 08:00 (regret 10).
    expect(find(paired, "D3", "d0", "shadow_h5")!.saeModel).toBe(0);
    expect(find(paired, "UC3", "d0", "chronos2_grid", "shadow_h5")?.n).toBe(8);
    expect(find(paired, "UC3", "d0", "shadow_h5_routed", "shadow_h5")?.n).toBe(
      8,
    );

    // Without the old-composer curve (optional) the core cells keep the
    // ride-day; without the weekday average (core) no planner row is written:
    // a core cell is never judged on rides another core cell lacks.
    const withoutPeak = new PairedAccumulator();
    scorePairedModels(
      planner().filter((c) => c.surface !== "plan_day_shadow_peak"),
      truth,
      withoutPeak,
      weekday,
    );
    expect(find(withoutPeak, "UC3", "d0", "shadow_h5")?.n).toBe(8);
    expect(
      find(withoutPeak, "UC3", "d0", "shadow_peak_scaled"),
    ).toBeUndefined();
    expect(find(withoutPeak, "D3", "d0", "weekday_avg")?.n).toBe(1);
    const withoutWkd = new PairedAccumulator();
    scorePairedModels(planner(), truth, withoutWkd, null);
    expect(
      withoutWkd.entries().filter((e) => ["UC3", "D3"].includes(e.useCase)),
    ).toEqual([]);
  });

  it("splits the day peak by the model behind the served level and keeps D7's level sums", () => {
    const run = (levelSource: string | null) => {
      const paired = new PairedAccumulator();
      scorePairedModels(planner({ levelSource }), truth, paired, weekday);
      return paired;
    };
    // wkd peak 35, served 45, truth P90 50.
    expect(
      find(run("tft"), "PEAK", "d0", "weekday_avg", "served_tft", "headliner"),
    ).toMatchObject({ n: 1, saeModel: 15, saeRef: 5 });
    expect(
      find(run("catboost"), "PEAK", "d0", "weekday_avg", "served_catboost"),
    ).toMatchObject({ n: 1 });
    expect(
      run("climatology")
        .entries()
        .filter((e) => e.useCase === "PEAK"),
    ).toEqual([]);
    // D7: raw level sums, headliners with a TFT level only.
    expect(
      find(run("tft"), "D7", "d0", "weekday_avg", "served_tft", "headliner"),
    ).toMatchObject({ n: 1, saeModel: 35, saeRef: 45, seModel: 50 });
    expect(
      run("catboost")
        .entries()
        .filter((e) => e.useCase === "D7"),
    ).toEqual([]);
  });

  // PAR-1058: the driver-level shadow against whichever model levelled the day.
  it("pairs the driver-level shadow with the served level of either model", () => {
    const run = (extra: Partial<ArchivedCurve>) => {
      const paired = new PairedAccumulator();
      scorePairedModels(planner(extra), truth, paired, weekday);
      return paired;
    };
    // driver 53, served 45, truth P90 50.
    const tft = run({ levelSource: "tft", driverDayPeak: 53 });
    for (const seg of ["all", "headliner"]) {
      expect(
        find(tft, "PEAK", "d0", "driver_level", "served_tft", seg),
      ).toMatchObject({ n: 1, saeModel: 3, saeRef: 5, seModel: 3 });
    }
    expect(
      find(tft, "D7", "d0", "driver_level", "served_tft", "headliner"),
    ).toMatchObject({ n: 1, saeModel: 53, saeRef: 45, seModel: 50 });
    // A CatBoost-levelled day (d90's case): PEAK under served_catboost, no D7.
    const cb = run({ levelSource: "catboost", driverDayPeak: 53 });
    expect(
      find(cb, "PEAK", "d0", "driver_level", "served_catboost", "headliner"),
    ).toMatchObject({ n: 1, saeModel: 3, saeRef: 5 });
    expect(
      cb
        .entries()
        .filter((e) => e.model === "driver_level" && e.useCase === "D7"),
    ).toEqual([]);
    // No shadow value: no row, and the weekday average's rows are unchanged.
    const none = run({ levelSource: "tft" });
    expect(none.entries().filter((e) => e.model === "driver_level")).toEqual(
      [],
    );
    expect(
      find(tft, "PEAK", "d0", "weekday_avg", "served_tft", "headliner"),
    ).toEqual(
      find(none, "PEAK", "d0", "weekday_avg", "served_tft", "headliner"),
    );
  });

  // PAR-1042: CatBoost's shadow level against the served TFT dayPeak, on the
  // weekday average's PEAK / D7 rows.
  it("pairs the CatBoost shadow level with served TFT on the same ride-days", () => {
    const run = (
      extra: Partial<ArchivedCurve>,
      wkd: typeof weekday | null = weekday,
    ) => {
      const paired = new PairedAccumulator();
      scorePairedModels(planner(extra), truth, paired, wkd);
      return paired;
    };
    const tft = run({ levelSource: "tft", catboostDayPeak: 62 });
    // CatBoost 62, served 45, truth P90 50 — same ride-day as the wkd row.
    for (const seg of ["all", "headliner"]) {
      expect(
        find(tft, "PEAK", "d0", "catboost_shadow", "served_tft", seg),
      ).toMatchObject({ n: 1, saeModel: 12, saeRef: 5, seModel: 12 });
    }
    expect(
      find(tft, "PEAK", "d0", "weekday_avg", "served_tft", "headliner"),
    ).toMatchObject({ n: 1, saeModel: 15, saeRef: 5 });
    expect(
      find(tft, "D7", "d0", "catboost_shadow", "served_tft", "headliner"),
    ).toMatchObject({ n: 1, saeModel: 62, saeRef: 45, seModel: 50 });
    // No shadow, a CatBoost-served day, or no weekday average: no row.
    for (const paired of [
      run({ levelSource: "tft" }),
      run({ levelSource: "catboost", catboostDayPeak: 62 }),
      run({ levelSource: "tft", catboostDayPeak: 62 }, null),
    ]) {
      expect(
        paired.entries().filter((e) => e.model === "catboost_shadow"),
      ).toEqual([]);
    }
  });

  it("pairs the grid TFT shadow with the served TFT level on PEAK and D7 (PAR-1041)", () => {
    const run = (extra: Partial<ArchivedCurve>) => {
      const paired = new PairedAccumulator();
      scorePairedModels(planner(extra), truth, paired, weekday);
      return paired;
    };
    // grid 48, served 45, truth P90 50.
    const tft = run({ levelSource: "tft", tftGridPeak: 48 });
    expect(
      find(tft, "PEAK", "d0", "tft_grid", "served_tft", "headliner"),
    ).toMatchObject({ n: 1, saeModel: 2, saeRef: 5 });
    expect(find(tft, "PEAK", "d0", "tft_grid", "served_tft")).toMatchObject({
      n: 1,
    });
    expect(
      find(tft, "D7", "d0", "tft_grid", "served_tft", "headliner"),
    ).toMatchObject({ n: 1, saeModel: 48, saeRef: 45, seModel: 50 });
    // The weekday average's rows are unchanged by it.
    expect(
      find(tft, "PEAK", "d0", "weekday_avg", "served_tft", "headliner"),
    ).toMatchObject({ n: 1, saeModel: 15, saeRef: 5 });
    // Never on a CatBoost-levelled day, never without a shadow value.
    for (const p of [
      run({ levelSource: "catboost", tftGridPeak: 48 }),
      run({ levelSource: "tft", tftGridPeak: null }),
    ]) {
      expect(p.entries().filter((e) => e.model === "tft_grid")).toEqual([]);
    }
  });

  it("reads the shadow level as it stood at the origin", () => {
    const at = Date.parse("2026-10-08T04:05:00Z");
    const rows = [
      {
        attractionId: "a1",
        forecastDate: "2026-10-04",
        createdAt: 0,
        peak: 10,
      },
      {
        attractionId: "a1",
        forecastDate: "2026-10-07",
        createdAt: 0,
        peak: 20.4,
      },
      // Written after the origin: never counts, although it is the freshest.
      {
        attractionId: "a1",
        forecastDate: "2026-10-08",
        createdAt: at + 1,
        peak: 99,
      },
      {
        attractionId: "a2",
        forecastDate: "2026-10-08",
        createdAt: 0,
        peak: 77,
      },
    ];
    expect(shadowLevelAt(rows, "a1", at, "2026-10-08")).toBe(20);
    // Older than 3 days before the origin date: stale, like the served read.
    expect(shadowLevelAt(rows.slice(0, 1), "a1", at, "2026-10-08")).toBeNull();
    expect(shadowLevelAt(rows, "a3", at, "2026-10-08")).toBeNull();
  });

  it("uses no history at or after the origin's date", () => {
    // At lead 7 the origin is 2026-10-01: only 24.09. is before it, fewer
    // than two dates → no weekday average, so no row needing one.
    const later = new PairedAccumulator();
    scorePairedModels(
      planner({ leadDays: 7, levelSource: "tft" }).map((c) => ({
        ...c,
        leadDays: 7,
      })),
      truth,
      later,
      weekday,
    );
    expect(later.entries()).toEqual([]);
  });

  it("never writes a key scorePairedShadows writes", () => {
    const legacy = new PairedAccumulator();
    const general = new PairedAccumulator();
    const curves = [
      ...planner({ levelSource: "tft" }),
      curve("park_hourly", flat(30), { ...intraday, liveWait: 25 }),
      curve("chronos_live", flat(35), intraday),
    ];
    scorePairedShadows(curves, truth, legacy);
    scorePairedModels(curves, truth, general, weekday);
    const key = (e: {
      useCase: string;
      lead: string;
      model: string;
      reference: string;
      segment: string;
    }) => [e.useCase, e.lead, e.model, e.reference, e.segment].join("|");
    const legacyKeys = new Set(legacy.entries().map(key));
    expect(legacyKeys.size).toBeGreaterThan(0);
    expect(general.entries().length).toBeGreaterThan(0);
    expect(general.entries().filter((e) => legacyKeys.has(key(e)))).toEqual([]);
  });

  it("names exactly the candidates the matrix questions show", () => {
    // Every (use case, lead) a question reads has candidates, and every
    // candidate is a column's model there.
    for (const q of COMPARISON_QUESTIONS) {
      if (q.reference !== "served_joint") continue;
      const names = Object.values(q.models).flatMap((v) => v ?? []);
      for (const lead of q.leads) {
        const candidates = PAIRED_QUESTION_MODELS[`${q.useCase}|${lead}`];
        expect({ q: q.id, lead, ok: !!candidates }).toEqual({
          q: q.id,
          lead,
          ok: true,
        });
        for (const c of candidates) {
          expect({ q: q.id, lead, c, shown: names.includes(c) }).toEqual({
            q: q.id,
            lead,
            c,
            shown: true,
          });
        }
      }
    }
  });
});

describe("PairedAccumulator — served rounding (PAR-1024)", () => {
  it("rounds both arms of a slot use case like the served value", () => {
    const acc = new PairedAccumulator();
    // truth 5: a raw 3 would be 2 off, served shows 5 (floor, PAR-1027) — both
    // arms are judged on what a visitor would see; a 10 stays 10.
    acc.add("UC1", "h0-1", "persistence", "served", "all", 3, 10, 5);
    acc.add("UC3", "d1", "shadow_h5", "served", "all", 23, 25, 25);
    const [a, b] = acc.entries();
    expect(a.saeModel).toBe(0);
    expect(a.saeRef).toBe(5);
    expect(b.saeModel).toBe(0); // 23 → 25
  });

  it("leaves best-time regret and day peaks unrounded", () => {
    const acc = new PairedAccumulator();
    acc.add("D3", "d1", "shadow_h5", "served", "all", 3, 7, 0);
    acc.add("PEAK", "d1", "weekday_avg", "served", "all", 41, 44, 42);
    const [d3, peak] = acc.entries();
    expect(d3.saeModel).toBe(3);
    expect(peak.saeRef).toBe(2);
  });
});
