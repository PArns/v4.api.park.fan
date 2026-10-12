import { Test } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import {
  PredictionLeadSnapshotService,
  SCORING_UPDATE_CHUNK,
  buildLeadSnapshotScoreUpdate,
} from "./prediction-lead-snapshot.service";
import { PredictionLeadSnapshot } from "../entities/prediction-lead-snapshot.entity";
import { AnalyticsService } from "../../analytics/analytics.service";
import { PredictionDto } from "../dto/prediction-response.dto";
import { Park } from "../../parks/entities/park.entity";
import { REDIS_CLIENT } from "../../common/redis/redis.module";

/**
 * The snapshot exists because the information is destroyed a day later: the
 * nightly run's `deduplicatePredictions` rewrites every daily row for a target
 * day, so what we said 30 days out is gone by the time the day arrives. These
 * pin the parts of that recording which are easy to get subtly wrong and
 * impossible to notice afterwards, because a wrong lead label looks exactly like
 * a correct one.
 */
describe("PredictionLeadSnapshotService", () => {
  const park = {
    id: "park-1",
    slug: "phantasialand",
    timezone: "Europe/Berlin",
  } as Park;

  let service: PredictionLeadSnapshotService;
  let inserted: PredictionLeadSnapshot[];
  let headliners: { attractionId: string }[];
  /** The aggregate the read path runs, so a test can hand back a row. */
  let aggregate: { mae: string | null; scored: string };
  let redis: { store: Map<string, string> };
  /** Every condition the read path added, so a test can pin the filter. */
  let conditions: string[];
  /** The repository fake, so the scoring tests can feed `find` and read `query`. */
  let repo: { find: jest.Mock; save: jest.Mock; query: jest.Mock };
  let peaks: Map<string, { attractionId: string; peak: number }[]>;

  const makeService = async () => {
    inserted = [];
    const insertBuilder = {
      insert: () => insertBuilder,
      into: () => insertBuilder,
      values: (rows: PredictionLeadSnapshot[]) => {
        inserted.push(...rows);
        return insertBuilder;
      },
      orIgnore: () => insertBuilder,
      execute: async () => ({ raw: [] }),
    };

    aggregate = { mae: null, scored: "0" };
    redis = { store: new Map<string, string>() };
    conditions = [];
    peaks = new Map();
    repo = {
      find: jest.fn().mockResolvedValue([]),
      save: jest.fn().mockResolvedValue([]),
      query: jest.fn().mockResolvedValue([]),
    };

    // The read path chains select/addSelect/where/andWhere and ends in
    // getRawOne; the write path chains insert/into/values/orIgnore/execute.
    // One object answering both keeps the fake to a dozen lines.
    const readBuilder = {
      select: () => readBuilder,
      addSelect: () => readBuilder,
      where: (c: string) => {
        conditions.push(c);
        return readBuilder;
      },
      andWhere: (c: string) => {
        conditions.push(c);
        return readBuilder;
      },
      getRawOne: async () => aggregate,
    };
    Object.assign(insertBuilder, readBuilder);

    const moduleRef = await Test.createTestingModule({
      providers: [
        PredictionLeadSnapshotService,
        {
          provide: getRepositoryToken(PredictionLeadSnapshot),
          useValue: {
            createQueryBuilder: () => insertBuilder,
            find: (...a: unknown[]) => repo.find(...a),
            save: (...a: unknown[]) => repo.save(...a),
            query: (...a: unknown[]) => repo.query(...a),
          },
        },
        {
          provide: REDIS_CLIENT,
          useValue: {
            get: async (k: string) => redis.store.get(k) ?? null,
            set: async (k: string, v: string) => {
              redis.store.set(k, v);
              return "OK";
            },
          },
        },
        {
          provide: AnalyticsService,
          useValue: {
            getHeadlinerAttractions: jest
              .fn()
              .mockImplementation(async () => headliners),
            getHeadlinerDailyPeaks: jest
              .fn()
              .mockImplementation(async () => peaks),
          },
        },
      ],
    }).compile();

    return moduleRef.get(PredictionLeadSnapshotService);
  };

  const daily = (
    attractionId: string,
    predictedTime: string,
    wait = 40,
    uncertainty: number | null = 12,
  ): PredictionDto =>
    ({
      attractionId,
      predictedTime,
      predictedWaitTime: wait,
      predictionType: "daily",
      confidence: 70,
      uncertaintyMinutes: uncertainty,
      crowdLevel: "moderate",
      baseline: 30,
      modelVersion: "v1.1.0",
    }) as PredictionDto;

  beforeEach(async () => {
    headliners = [{ attractionId: "taron" }, { attractionId: "fly" }];
    service = await makeService();
  });

  it("keeps only the predictions landing exactly on a bucket distance", async () => {
    // Run on 2026-03-10 Berlin time. Buckets are 1, 3, 7, 14, 30, 60 days out.
    const now = new Date("2026-03-10T01:00:00Z");
    const predictions = [
      daily("taron", "2026-03-11T12:00:00Z"), // +1  → kept
      daily("taron", "2026-03-12T12:00:00Z"), // +2  → dropped
      daily("taron", "2026-03-13T12:00:00Z"), // +3  → kept
      daily("taron", "2026-03-17T12:00:00Z"), // +7  → kept
      daily("taron", "2026-03-18T12:00:00Z"), // +8  → dropped
      daily("taron", "2026-04-09T12:00:00Z"), // +30 → kept
      daily("taron", "2026-05-09T12:00:00Z"), // +60 → kept
      daily("taron", "2026-05-10T12:00:00Z"), // +61 → dropped, past the horizon
    ];

    await service.snapshotPark(park, predictions, now);

    expect(inserted.map((r) => r.leadDays).sort((a, b) => a - b)).toEqual([
      1, 3, 7, 30, 60,
    ]);
    // +14 was not in the input at all, so it is absent rather than invented.
    expect(inserted.some((r) => r.leadDays === 14)).toBe(false);
  });

  it("labels the lead distance in the park's calendar days, not UTC's", async () => {
    // 23:30 UTC on 2026-03-10 is already 00:30 on the 11th in Berlin. A target of
    // 2026-03-12 is therefore ONE day out locally, not two — get this wrong and
    // every row for half the world's parks carries a lead that is off by one,
    // which no later check can detect.
    const now = new Date("2026-03-10T23:30:00Z");
    await service.snapshotPark(
      park,
      [daily("taron", "2026-03-12T12:00:00Z")],
      now,
    );

    expect(inserted).toHaveLength(1);
    expect(inserted[0].leadDays).toBe(1);
    expect(inserted[0].targetDate).toBe("2026-03-12");
  });

  it("samples headliners only, and carries the band along", async () => {
    const now = new Date("2026-03-10T01:00:00Z");
    await service.snapshotPark(
      park,
      [
        daily("taron", "2026-03-11T12:00:00Z", 45, 9),
        daily("some-flat-ride", "2026-03-11T12:00:00Z", 5, 2),
      ],
      now,
    );

    expect(inserted).toHaveLength(1);
    expect(inserted[0].attractionId).toBe("taron");
    expect(inserted[0].predictedWaitTime).toBe(45);
    expect(inserted[0].uncertaintyMinutes).toBe(9);
    // Unscored on the way in — the actual is a separate job's business.
    expect(inserted[0].actualWaitTime).toBeNull();
    expect(inserted[0].scoredAt).toBeNull();
  });

  it("ignores hourly predictions", async () => {
    const now = new Date("2026-03-10T01:00:00Z");
    const hourly = {
      ...daily("taron", "2026-03-11T12:00:00Z"),
      predictionType: "hourly" as const,
    };

    await service.snapshotPark(park, [hourly], now);

    expect(inserted).toHaveLength(0);
  });

  it("records a missing band as null rather than zero", async () => {
    // A model with no real spread reports nothing. Writing 0 would later average
    // into the curve as a confident prediction, which is the opposite of true.
    const now = new Date("2026-03-10T01:00:00Z");
    await service.snapshotPark(
      park,
      [daily("taron", "2026-03-11T12:00:00Z", 40, null)],
      now,
    );

    expect(inserted).toHaveLength(1);
    expect(inserted[0].uncertaintyMinutes).toBeNull();
  });

  it("writes nothing for a park with no headliners", async () => {
    headliners = [];
    service = await makeService();

    await service.snapshotPark(
      park,
      [daily("taron", "2026-03-11T12:00:00Z")],
      new Date("2026-03-10T01:00:00Z"),
    );

    expect(inserted).toHaveLength(0);
  });

  // ── PAR-854: the snapshot records the SERVED number ────────────────────────
  // The calendar and /plan/day show TFT for headliners over days 1-60; the
  // archive used to record CatBoost's raw answer there, so `leadTimeMae`
  // published the error of a model the reader never saw.
  describe("served source", () => {
    const tft = (attractionId: string, day: string, wait = 55): PredictionDto =>
      ({
        ...daily(attractionId, `${day}T12:00:00`, wait, 14),
        modelVersion: "tft",
      }) as PredictionDto;

    it("labels TFT rows tft and everything else catboost", async () => {
      const now = new Date("2026-03-10T01:00:00Z");
      await service.snapshotPark(
        park,
        [
          tft("taron", "2026-03-11", 55), // +1, served by TFT
          daily("fly", "2026-03-11T12:00:00Z", 30), // +1, TFT had nothing → CatBoost
        ],
        now,
      );

      const bySource = Object.fromEntries(
        inserted.map((r) => [r.attractionId, [r.source, r.predictedWaitTime]]),
      );
      expect(bySource).toEqual({
        taron: ["tft", 55],
        fly: ["catboost", 30],
      });
    });

    it("reads TFT's offset-less day as the park's own day, far from UTC", async () => {
      // TFT names the park-local day as `YYYY-MM-DDT12:00:00`. Parsed with
      // `new Date()` that is noon on the SERVER's clock, which in Auckland
      // (UTC+13 in March) is already the next day — a one-day-off lead label.
      const auckland = { ...park, timezone: "Pacific/Auckland" } as Park;
      // 2026-03-10 01:00 UTC is 14:00 on the 10th in Auckland.
      const now = new Date("2026-03-10T01:00:00Z");
      await service.snapshotPark(auckland, [tft("taron", "2026-03-11")], now);

      expect(inserted).toHaveLength(1);
      expect(inserted[0].targetDate).toBe("2026-03-11");
      expect(inserted[0].leadDays).toBe(1);
      expect(inserted[0].source).toBe("tft");
    });
  });

  it("samples out to sixty days", () => {
    // A sampling choice, not a limit: the daily run answers as far ahead as the
    // park has published a schedule (181-362 days across the live parks), so a
    // longer bucket would be written fine — it would simply say nothing for its
    // first 120 days.
    expect(Math.max(...PredictionLeadSnapshotService.LEAD_BUCKETS)).toBe(60);
  });

  // ── Reading the curve back ─────────────────────────────────────────────────
  // The archive was written from the first day and read by nobody: `leadTimeMae`
  // on /plan/day was hard-coded null, so 6,000 rows a night accumulated to
  // answer a question nothing asked.
  describe("getLeadTimeMae", () => {
    const enough = String(200);

    it("answers with the nearest sampled distance AT OR BELOW the one asked", async () => {
      aggregate = { mae: "7.25", scored: enough };

      // 20 days is answered by the 14-day bucket, never by the 30-day one:
      // overstating the distance understates the error.
      await expect(service.getLeadTimeMae(20)).resolves.toBe(7.3);
      expect([...redis.store.keys()]).toEqual(["ml:lead-mae:v2:14"]);
    });

    it("averages served snapshots only, never the pre-PAR-854 CatBoost rows", async () => {
      // A row without a source recorded CatBoost while readers saw TFT. Mixing
      // it in would publish a blend of two models as the served model's error.
      aggregate = { mae: "6.0", scored: enough };
      await service.getLeadTimeMae(7);

      expect(conditions).toContain("s.source IS NOT NULL");
      expect(conditions).toContain("s.absoluteError IS NOT NULL");
    });

    it("answers past the last bucket with the last bucket", async () => {
      aggregate = { mae: "11.0", scored: enough };

      // The furthest thing anybody has measured. "At least this wrong" is
      // honest where interpolating into unmeasured distance is not.
      await expect(service.getLeadTimeMae(300)).resolves.toBe(11);
      expect([...redis.store.keys()]).toEqual(["ml:lead-mae:v2:60"]);
    });

    it("says nothing about today, which no bucket covers", async () => {
      await expect(service.getLeadTimeMae(0)).resolves.toBeNull();
      expect(redis.store.size).toBe(0);
    });

    it("withholds a figure until the bucket has enough scored rows", async () => {
      // The normal state of the far buckets for the first weeks after this
      // ships. A number that moves with one park's bad week is worse than none.
      aggregate = { mae: "9.9", scored: String(3) };

      await expect(service.getLeadTimeMae(30)).resolves.toBeNull();
    });

    it("remembers that a bucket had too little data, rather than re-asking", async () => {
      aggregate = { mae: null, scored: "0" };
      await service.getLeadTimeMae(7);

      // "none" and not an absent key: /plan/day is a per-request read path, and
      // an unrecorded miss is an aggregate query on every call.
      expect(redis.store.get("ml:lead-mae:v2:7")).toBe("none");

      aggregate = { mae: "5.0", scored: enough };
      await expect(service.getLeadTimeMae(7)).resolves.toBeNull();
    });
  });

  // ── Scoring ────────────────────────────────────────────────────────────────
  // `save()` on loaded entities was one UPDATE per row: ~340 of them at 03:00,
  // all queueing for the pool at once (PAR-872). The write is now one
  // `UPDATE … FROM (VALUES …)` per chunk, with exactly the values save() wrote.
  describe("scoreDueSnapshots", () => {
    const row = (
      attractionId: string,
      targetDate: string,
      leadDays: number,
      predictedWaitTime = 40,
    ) =>
      ({
        attractionId,
        targetDate,
        leadDays,
        predictedWaitTime,
        actualWaitTime: null,
        absoluteError: null,
        scoredAt: null,
      }) as unknown as PredictionLeadSnapshot;

    const now = new Date("2026-10-12T03:00:00Z");

    it("writes every scored row in one set-based UPDATE, never through save()", async () => {
      repo.find.mockResolvedValue([
        row("taron", "2026-10-09", 1, 40),
        row("fly", "2026-10-09", 7, 30),
        row("gone", "2026-10-09", 1),
      ]);
      peaks.set("2026-10-09", [{ attractionId: "taron", peak: 52.4 }]);

      await expect(service.scoreDueSnapshots([park], now)).resolves.toEqual({
        scored: 2,
        withActual: 1,
      });

      expect(repo.save).not.toHaveBeenCalled();
      expect(repo.query).toHaveBeenCalledTimes(1);
      const [sql, params] = repo.query.mock.calls[0];
      expect(sql).toMatch(/^UPDATE prediction_lead_snapshots AS s/);
      expect(sql).toContain("FROM (VALUES");
      // scoredAt once, then five per row: taron scored, fly without an actual,
      // and the orphan (no longer a headliner) marked seen with nothing else.
      expect(params).toEqual([
        now,
        "taron",
        "2026-10-09",
        1,
        52,
        12,
        "fly",
        "2026-10-09",
        7,
        null,
        null,
        "gone",
        "2026-10-09",
        1,
        null,
        null,
      ]);
    });

    it("leaves a park unscored when its peaks query failed", async () => {
      repo.find.mockResolvedValue([row("taron", "2026-10-09", 1)]);
      const analytics = (
        service as unknown as {
          analyticsService: { getHeadlinerDailyPeaks: jest.Mock };
        }
      ).analyticsService;
      analytics.getHeadlinerDailyPeaks.mockRejectedValueOnce(new Error("boom"));

      await service.scoreDueSnapshots([park], now);
      expect(repo.query).not.toHaveBeenCalled();
    });

    it("chunks the update so no statement carries more than one chunk", async () => {
      const many = Array.from({ length: SCORING_UPDATE_CHUNK + 1 }, (_, i) =>
        row("taron", `2026-09-${String((i % 28) + 1).padStart(2, "0")}`, i),
      );
      repo.find.mockResolvedValue(many);

      await service.scoreDueSnapshots([park], now);
      expect(repo.query).toHaveBeenCalledTimes(2);
      expect(repo.query.mock.calls[0][1]).toHaveLength(
        1 + 5 * SCORING_UPDATE_CHUNK,
      );
      expect(repo.query.mock.calls[1][1]).toHaveLength(1 + 5);
    });
  });

  describe("buildLeadSnapshotScoreUpdate", () => {
    it("casts every value so a NULL first row still types the column", () => {
      const { sql, params } = buildLeadSnapshotScoreUpdate(
        [
          {
            attractionId: "a",
            targetDate: "2026-10-01",
            leadDays: 3,
            actualWaitTime: null,
            absoluteError: null,
          },
          {
            attractionId: "b",
            targetDate: "2026-10-02",
            leadDays: 7,
            actualWaitTime: 45,
            absoluteError: 5,
          },
        ],
        new Date(0),
      );
      expect(sql).toContain(
        "($2::uuid, $3::date, $4::smallint, $5::int, $6::int), ($7::uuid, $8::date, $9::smallint, $10::int, $11::int)",
      );
      expect(sql).toContain("scored_at = $1");
      expect(sql).toContain("s.lead_days = v.lead_days");
      expect(params).toHaveLength(11);
    });
  });
});
