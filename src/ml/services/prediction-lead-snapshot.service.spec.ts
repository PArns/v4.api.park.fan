import { Test } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { PredictionLeadSnapshotService } from "./prediction-lead-snapshot.service";
import { PredictionLeadSnapshot } from "../entities/prediction-lead-snapshot.entity";
import { AnalyticsService } from "../../analytics/analytics.service";
import { PredictionDto } from "../dto/prediction-response.dto";
import { Park } from "../../parks/entities/park.entity";

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

    const moduleRef = await Test.createTestingModule({
      providers: [
        PredictionLeadSnapshotService,
        {
          provide: getRepositoryToken(PredictionLeadSnapshot),
          useValue: {
            createQueryBuilder: () => insertBuilder,
            find: jest.fn().mockResolvedValue([]),
            save: jest.fn().mockResolvedValue([]),
          },
        },
        {
          provide: AnalyticsService,
          useValue: {
            getHeadlinerAttractions: jest
              .fn()
              .mockImplementation(async () => headliners),
            getHeadlinerDailyPeaks: jest.fn().mockResolvedValue(new Map()),
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

  it("stops at the stored daily horizon", () => {
    // A bucket past 60 days would never be written, because
    // deduplicatePredictions only keeps daily rows out to 60 — and an empty
    // bucket reads as a gap in the curve rather than as out of range.
    expect(Math.max(...PredictionLeadSnapshotService.LEAD_BUCKETS)).toBe(60);
  });
});
