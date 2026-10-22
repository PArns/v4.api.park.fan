import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import { Park } from "../../src/parks/entities/park.entity";
import { Attraction } from "../../src/attractions/entities/attraction.entity";
import { HOURLY_AGGREGATE_SELECT } from "../../src/queues/processors/hourly-aggregate.sql";
import { HEARTBEAT_SOURCE } from "../../src/common/utils/outage-rows.sql";
import { RECONCILIATION_SOURCE } from "../../src/common/utils/source-absent-status.util";

/**
 * The hourly rollup against rows our own bookkeeping wrote (PAR-966).
 *
 * Executed rather than matched as text, because the two placements of the
 * filter move the result in opposite directions: in `observed` it removes
 * samples, in the anchor LATERAL it can add a bucket. Each case below goes red
 * when its own placement is removed and stays green without the other.
 */
describe("hourly aggregate ignores carried rows (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let rideId: string;

  const HOUR = new Date("2026-06-15T10:00:00Z");
  const at = (minutes: number) => new Date(HOUR.getTime() + minutes * 60_000);

  const reading = async (
    minutes: number,
    status: string,
    waitTime: number | null,
    source: string,
    isHeartbeat: boolean,
  ): Promise<void> => {
    const ts = at(minutes);
    await dataSource.query(
      `INSERT INTO queue_data (id, "attractionId", "queueType", status, "waitTime",
                               timestamp, is_heartbeat, data_source, "lastUpdated")
       VALUES (gen_random_uuid(), $1, 'STANDBY', $2, $3, $4, $5, $6, $7)`,
      [
        rideId,
        status,
        waitTime,
        ts,
        isHeartbeat,
        source,
        new Date(ts.getTime() - 60_000),
      ],
    );
  };

  const run = (): Promise<
    Array<{ hour: Date; sampleCount: string; p50: number }>
  > => dataSource.query(HOURLY_AGGREGATE_SELECT, [at(-24 * 60), at(24 * 60)]);

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    app = moduleFixture.createNestApplication();
    await app.init();
    dataSource = app.get(DataSource);
  });

  afterAll(async () => {
    await app.close();
  });

  beforeEach(async () => {
    const park = await dataSource.getRepository(Park).save(
      dataSource.getRepository(Park).create({
        externalId: "e2e-agg-park",
        name: "Aggregate Park",
        slug: "aggregate-park",
        latitude: 50.8,
        longitude: 6.9,
        timezone: "Europe/Berlin",
        continent: "Europe",
        continentSlug: "europe",
        country: "Germany",
        countrySlug: "germany",
        countryCode: "DE",
        city: "Brühl",
        citySlug: "bruehl",
      }),
    );
    const ride = await dataSource.getRepository(Attraction).save(
      dataSource.getRepository(Attraction).create({
        externalId: "e2e-agg-ride",
        name: "Carry Coaster",
        slug: "carry-coaster",
        parkId: park.id,
        latitude: 50.8,
        longitude: 6.9,
      }),
    );
    rideId = ride.id;
  });

  it("a carried OPERATING heartbeat creates no bucket", async () => {
    // Two heartbeat rows in the hour would make COUNT(*) = 2 on their own.
    await reading(10, "OPERATING", 90, HEARTBEAT_SOURCE, true);
    await reading(40, "OPERATING", 90, HEARTBEAT_SOURCE, true);

    expect(await run()).toHaveLength(0);
  });

  it("a carried OPERATING heartbeat does not enter the percentiles", async () => {
    await reading(10, "OPERATING", 20, "themeparks-wiki", false);
    await reading(40, "OPERATING", 30, "themeparks-wiki", false);
    await reading(50, "OPERATING", 90, HEARTBEAT_SOURCE, true);

    const rows = await run();
    expect(rows).toHaveLength(1);
    expect(Number(rows[0].sampleCount)).toBe(2);
    expect(Number(rows[0].p50)).toBe(25);
  });

  it("a reconciliation CLOSED row does not hide the real anchor before it", async () => {
    // The real OPERATING reading at 09:30 is what a guest saw at 10:00; the
    // CLOSED row at 09:55 is our own write for a ride no source mentioned.
    await reading(-30, "OPERATING", 40, "themeparks-wiki", false);
    await reading(-5, "CLOSED", null, RECONCILIATION_SOURCE, false);
    await reading(20, "OPERATING", 20, "themeparks-wiki", false);

    const rows = await run();
    // Without the LATERAL filter the anchor is CLOSED, the hour holds one
    // sample, and HAVING COUNT(*) >= 2 drops it.
    expect(rows).toHaveLength(1);
    expect(new Date(rows[0].hour).toISOString()).toBe(HOUR.toISOString());
    expect(Number(rows[0].sampleCount)).toBe(2);
    expect(Number(rows[0].p50)).toBe(30);
  });
});
