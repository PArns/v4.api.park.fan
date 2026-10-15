import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import { Park } from "../../src/parks/entities/park.entity";
import { Attraction } from "../../src/attractions/entities/attraction.entity";
import { MLService } from "../../src/ml/ml.service";
import { HYPERTABLES } from "../../src/database/hypertables";

/**
 * The nightly daily-prediction delete (PAR-900) against a real TimescaleDB
 * hypertable with a compressed chunk.
 *
 * In production the DELETE filtered on `predictedTime` alone, which is not the
 * partition key, so it planned every chunk and decompressed the matching
 * batches of the compressed ones — and died on the duplicate keys two of them
 * held. What this pins is that the bounded DELETE never opens a compressed
 * chunk: the chunk stays fully compressed (its own heap stays empty, which a
 * DML decompression would have filled with the rest of the batch) while a row
 * with the same old `predictedTime` in an uncompressed chunk IS deleted, so a
 * DELETE that matches nothing cannot pass for one that skips correctly.
 */
describe("daily prediction cleanup skips compressed chunks (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let ml: MLService;
  let rideId: string;
  // Whether this suite switched compression on, so it can leave the table as
  // the next suite expects it (global-setup.ts: the first suite to boot decides
  // the compression state the later ones inherit).
  let enabledCompression = false;

  const DAY = 86_400_000;
  const now = Date.now();
  const at = (daysAgo: number) => new Date(now - daysAgo * DAY);

  const insert = (createdAt: Date, predictedTime: Date) =>
    dataSource.query(
      `INSERT INTO wait_time_predictions
         ("attractionId", "createdAt", "predictedTime", "predictionType",
          "predictedWaitTime", "modelVersion")
       VALUES ($1, $2, $3, 'daily', 30, 'v-e2e')`,
      [rideId, createdAt, predictedTime],
    );

  const chunkOf = async (createdAt: Date): Promise<string> => {
    const [{ chunk }] = await dataSource.query(
      `SELECT tableoid::regclass::text AS chunk
         FROM wait_time_predictions
        WHERE "attractionId" = $1 AND "createdAt" = $2
        LIMIT 1`,
      [rideId, createdAt],
    );
    return chunk;
  };

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
    if (enabledCompression) {
      await dataSource.query(
        `SELECT decompress_chunk(c, if_compressed => true)
           FROM show_chunks('wait_time_predictions') AS c`,
      );
      await dataSource.query(
        `ALTER TABLE wait_time_predictions SET (timescaledb.compress = false)`,
      );
    }
    await app.close();
  });

  beforeEach(async () => {
    const park = await dataSource.getRepository(Park).save(
      dataSource.getRepository(Park).create({
        externalId: "e2e-daily-cleanup",
        name: "Daily Cleanup Park",
        slug: "daily-cleanup-park",
        latitude: 48.27,
        longitude: 7.72,
        timezone: "UTC",
        continent: "Europe",
        continentSlug: "europe",
        country: "Germany",
        countrySlug: "germany",
        countryCode: "DE",
        city: "Rust",
        citySlug: "rust-daily-cleanup",
      }),
    );
    const ride = await dataSource.getRepository(Attraction).save(
      dataSource.getRepository(Attraction).create({
        externalId: "e2e-daily-cleanup-ride",
        name: "Daily Cleanup Ride",
        slug: "daily-cleanup-ride",
        parkId: park.id,
        latitude: 48.27,
        longitude: 7.72,
      }),
    );
    rideId = ride.id;
  });

  it("deletes old targets from uncompressed chunks and leaves compressed ones unopened", async () => {
    // Production's compression settings, from the same constant
    // TimescaleInitService applies them from. Whether the boot here already
    // applied them depends on which suite booted first.
    const [{ compressed }] = await dataSource.query(
      `SELECT count(*)::int AS compressed
         FROM timescaledb_information.compression_settings
        WHERE hypertable_name = 'wait_time_predictions'`,
    );
    if (compressed === 0) {
      const spec = HYPERTABLES.find(
        (h) => h.table === "wait_time_predictions",
      )!;
      const [orderCol, orderDir] = spec.orderBy!.split(" ");
      await dataSource.query(
        `ALTER TABLE wait_time_predictions SET (
           timescaledb.compress,
           timescaledb.compress_segmentby = '"${spec.segmentBy}"',
           timescaledb.compress_orderby = '"${orderCol}" ${orderDir}'
         )`,
      );
      enabledCompression = true;
    }

    // Old chunk (30 days ago): one row whose target is past the cutoff — the
    // shape the unbounded DELETE decompressed — and one beside it in the same
    // segment, which a decompression would have pulled into the chunk's heap.
    const oldCreated = at(30);
    await insert(oldCreated, at(120));
    await insert(new Date(oldCreated.getTime() + 1000), at(20));
    const oldChunk = await chunkOf(oldCreated);
    await dataSource.query(`SELECT compress_chunk($1::regclass)`, [oldChunk]);

    // Recent, uncompressed chunk: one past the cutoff, one future target.
    const recentCreated = at(1);
    await insert(recentCreated, at(120));
    await insert(recentCreated, new Date(now + 30 * DAY));

    const result = await ml.deleteOldPredictions("daily", at(90));

    const [{ chunkEnd }] = await dataSource.query(
      `SELECT range_end AS "chunkEnd" FROM timescaledb_information.chunks
        WHERE hypertable_name = 'wait_time_predictions'
          AND format('%I.%I', chunk_schema, chunk_name)::regclass = $1::regclass`,
      [oldChunk],
    );
    expect(result.createdFrom).not.toBeNull();
    expect(result.createdFrom!.getTime()).toBeGreaterThanOrEqual(
      new Date(chunkEnd).getTime(),
    );
    expect(result.deleted).toBe(1);

    const left: Array<{ createdAt: Date; predictedTime: Date }> =
      await dataSource.query(
        `SELECT "createdAt", "predictedTime" FROM wait_time_predictions
          WHERE "attractionId" = $1 ORDER BY "createdAt", "predictedTime"`,
        [rideId],
      );
    // Both compressed rows survive; of the recent pair only the future target.
    expect(left).toHaveLength(3);
    expect(
      left
        .filter((r) => new Date(r.createdAt) < at(2))
        .map((r) => r.predictedTime),
    ).toHaveLength(2);
    expect(
      left
        .filter((r) => new Date(r.createdAt) >= at(2))
        .map((r) => new Date(r.predictedTime).getTime()),
    ).toEqual([now + 30 * DAY]);

    // Never opened: still compressed, and nothing was decompressed into its heap.
    const [{ isCompressed }] = await dataSource.query(
      `SELECT is_compressed AS "isCompressed" FROM timescaledb_information.chunks
        WHERE format('%I.%I', chunk_schema, chunk_name)::regclass = $1::regclass`,
      [oldChunk],
    );
    expect(isCompressed).toBe(true);
    const [{ heap }] = await dataSource.query(
      `SELECT count(*)::int AS heap FROM ONLY ${oldChunk}`,
    );
    expect(heap).toBe(0);
  });
});
