import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import { Park } from "../../src/parks/entities/park.entity";
import { Attraction } from "../../src/attractions/entities/attraction.entity";
import { MLService } from "../../src/ml/ml.service";
import { PredictionDto } from "../../src/ml/dto";
import { currentSlotStartMs } from "../../src/common/utils/best-visit-times.util";

/**
 * The hourly diff-write (PAR-874) against a real TimescaleDB hypertable,
 * because the two statements that do it are raw SQL — a join read and a
 * `DELETE … USING unnest(uuid[], timestamptz[], timestamptz[])` — that the unit
 * spec can only read as text.
 *
 * Every "left alone" is asserted beside a "was replaced" out of the same run,
 * so a DELETE that matches nothing cannot pass for a diff that works.
 */
describe("hourly prediction diff-write (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let ml: MLService;
  let parkId: string;
  let rideId: string;

  const slot = (h: number) =>
    new Date(currentSlotStartMs() + h * 3_600_000).toISOString();

  const pred = (h: number, wait: number): PredictionDto => ({
    attractionId: rideId,
    predictedTime: slot(h),
    predictedWaitTime: wait,
    predictionType: "hourly",
    confidence: 68.2,
    uncertaintyMinutes: 12,
    crowdLevel: "moderate",
    baseline: 32.5,
    modelVersion: "v-e2e",
    status: "OPERATING",
  });

  const rows = async (): Promise<
    Array<{ predictedTime: Date; createdAt: Date; wait: number }>
  > =>
    dataSource.query(
      `SELECT "predictedTime", "createdAt", "predictedWaitTime" AS wait
         FROM wait_time_predictions
        WHERE "attractionId" = $1 AND "predictionType" = 'hourly'
        ORDER BY "predictedTime", "createdAt"`,
      [rideId],
    );

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    app = moduleFixture.createNestApplication();
    await app.init();
    dataSource = app.get(DataSource);
    ml = app.get(MLService);
  });

  afterAll(async () => {
    await app.close();
  });

  beforeEach(async () => {
    const park = await dataSource.getRepository(Park).save(
      dataSource.getRepository(Park).create({
        externalId: "e2e-diff-write",
        name: "Diff Write Park",
        slug: "diff-write-park",
        latitude: 48.27,
        longitude: 7.72,
        timezone: "UTC",
        continent: "Europe",
        continentSlug: "europe",
        country: "Germany",
        countrySlug: "germany",
        countryCode: "DE",
        city: "Rust",
        citySlug: "rust-diff-write",
      }),
    );
    const ride = await dataSource.getRepository(Attraction).save(
      dataSource.getRepository(Attraction).create({
        externalId: "e2e-diff-write-ride",
        name: "Diff Write Ride",
        slug: "diff-write-ride",
        parkId: park.id,
        latitude: 48.27,
        longitude: 7.72,
      }),
    );
    parkId = park.id;
    rideId = ride.id;
  });

  it("keeps an unchanged slot's row, replaces a changed one and drops a vanished one", async () => {
    // The table is a hypertable here as in production — the DELETE has to
    // work through chunk exclusion, not against a plain table.
    const [{ ht }] = await dataSource.query(
      `SELECT count(*)::int AS ht FROM timescaledb_information.hypertables
        WHERE hypertable_name = 'wait_time_predictions'`,
    );
    expect(ht).toBe(1);

    await ml.storePredictions([pred(1, 30), pred(2, 40), pred(3, 50)], {
      replaceHourlyForPark: parkId,
    });
    const first = await rows();
    expect(first.map((r) => r.wait)).toEqual([30, 40, 50]);
    const firstCreated = new Date(first[0].createdAt).getTime();

    // Postgres timestamps are taken in milliseconds here; make the second
    // run's createdAt distinguishable.
    await new Promise((r) => setTimeout(r, 20));

    // Slot 1 unchanged, slot 2 changed, slot 3 no longer predicted, slot 4 new.
    await ml.storePredictions([pred(1, 30), pred(2, 45), pred(4, 60)], {
      replaceHourlyForPark: parkId,
    });
    const second = await rows();

    expect(
      second.map((r) => [new Date(r.predictedTime).toISOString(), r.wait]),
    ).toEqual([
      [slot(1), 30],
      [slot(2), 45],
      [slot(4), 60],
    ]);
    // The unchanged row was not rewritten: it still carries the first run's createdAt…
    expect(new Date(second[0].createdAt).getTime()).toBe(firstCreated);
    // …while the changed one is the second run's.
    expect(new Date(second[1].createdAt).getTime()).toBeGreaterThan(
      firstCreated,
    );
  });

  it("serves the unchanged hours-old row because the run vouched for the ride", async () => {
    // Rows written three hours ago — past the old 30-minute createdAt test,
    // which is what an unchanged row looks like after a few confirming runs.
    for (const [h, wait] of [
      [1, 30],
      [2, 40],
    ] as const) {
      await dataSource.query(
        `INSERT INTO wait_time_predictions
           ("attractionId", "createdAt", "predictedTime", "predictionType",
            "predictedWaitTime", confidence, "uncertaintyMinutes", "crowdLevel",
            status, baseline, "modelVersion")
         VALUES ($1, now() - interval '3 hours', $2, 'hourly', $3, 68.2, 12,
                 'moderate', 'OPERATING', 32.5, 'v-e2e')`,
        [rideId, slot(h), wait],
      );
    }

    // Without a run vouching for the ride, the aged rows are not served…
    const before = await ml.getBatchStoredPredictions(
      [rideId],
      "hourly",
      new Date(currentSlotStartMs()),
    );
    expect(before.get(rideId)).toBeUndefined();

    // …a run with the same answer writes nothing and vouches for them.
    await ml.storePredictions([pred(1, 30), pred(2, 40)], {
      replaceHourlyForPark: parkId,
    });
    const [{ fresh }] = await dataSource.query(
      `SELECT count(*)::int AS fresh FROM wait_time_predictions
        WHERE "attractionId" = $1 AND "createdAt" > now() - interval '1 hour'`,
      [rideId],
    );
    expect(fresh).toBe(0);

    const served = await ml.getBatchStoredPredictions(
      [rideId],
      "hourly",
      new Date(currentSlotStartMs()),
    );
    expect(served.get(rideId)?.map((p) => p.predictedWaitTime)).toEqual([
      30, 40,
    ]);
  });
});
