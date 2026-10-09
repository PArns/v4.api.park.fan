import {
  ArchivedCurve,
  ScoreAccumulator,
  bestTimeOutcome,
  buildTruthSlots,
  crowdOrdinal,
  percentileCont,
  regionOf,
  scoreCrossParkCrowd,
  scoreParkDay,
  slotLeadBucket,
  spearman,
} from "./forecast-archive-scoring.util";

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
          },
        ],
        truth,
        hadWindows: true,
        headlinerIds: new Set([ride]),
        typicalDayPeak: 50,
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

    // D6: headliner P90 54 ÷ 50 = 108 % → moderate.
    const d6 = sumsOf(acc, "EU|D6|d0|predicted|all");
    expect(d6).toMatchObject({ n: 1, exact: 1, within1: 1, busyTrue: 0 });
    const d6plan = sumsOf(acc, "EU|D6|d0|plan_day|all");
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
        headlinerIds: new Set(),
        typicalDayPeak: 0,
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
        headlinerIds: new Set(),
        typicalDayPeak: 0,
      },
      acc,
    );
    expect(sumsOf(acc, "EU|UC1|h0-1|all|all").n).toBe(3);
    expect(acc.entries().some((e) => e.lead.startsWith("d"))).toBe(false);
  });
});

describe("D1 next-best-ride", () => {
  const origin = T0;
  const base: ArchivedCurve = {
    attractionId: "r",
    surface: "park_hourly",
    originKind: "intraday",
    originAt: origin,
    leadDays: 0,
    slotStart: origin,
    slotMinutes: 15,
    // forecast climbs to 40 at +45 min
    waits: [20, 25, 28, 40, 40, 40, 40, 40, 40],
    sources: "cccpcpccc",
    bands: null,
    dayPeak: null,
    expectedError: null,
    rideQ90: null,
    isHeadliner: false,
    liveWait: 20,
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
        headlinerIds: new Set(),
        typicalDayPeak: 0,
      },
      acc,
    );
    return acc;
  };

  it("counts a suggestion that held", () => {
    const acc = run(base, [20, 25, 35, 40]);
    expect(sumsOf(acc, "EU|D1|h0-2|all|all")).toEqual({
      d1Sugg: 1,
      d1SuggOk: 1,
    });
    // Triggered by the +45 min slot (40 ≥ 20 + 10), a PCN slot.
    expect(sumsOf(acc, "EU|D1|h0-1|pcn_blend|all")).toEqual({
      d1Sugg: 1,
      d1SuggOk: 1,
    });
  });

  it("counts a suggestion that did not hold", () => {
    const acc = run(base, [20, 22, 25, 25]);
    expect(sumsOf(acc, "EU|D1|h0-2|all|all")).toEqual({
      d1Sugg: 1,
      d1SuggOk: 0,
    });
  });

  it("records the base rate when there is no suggestion", () => {
    const acc = run({ ...base, liveWait: 35 }, [35, 50]);
    expect(sumsOf(acc, "EU|D1|h0-2|all|all")).toEqual({
      d1None: 1,
      d1NoneWorse: 1,
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
        headlinerIds: new Set(),
        typicalDayPeak: 0,
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
