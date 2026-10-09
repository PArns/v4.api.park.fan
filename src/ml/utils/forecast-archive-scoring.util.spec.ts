import {
  ArchivedCurve,
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
  slotLeadBucket,
  spearman,
} from "./forecast-archive-scoring.util";
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

  it("no qualifying hour is a non-suggestion", () => {
    const acc = run({ ...base, laterWait: null, laterAt: null }, [20, 20]);
    expect(sumsOf(acc, "EU|D1|h0-2|all|all")).toEqual({
      d1None: 1,
      d1NoneWorse: 0,
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

  it("scores the plan band around dayPeak per ride-day, never per hour", () => {
    // truth P90 of 30,35,40,45 = 43.5; |40 − 43.5| = 3.5 ≤ 5 → covered.
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
