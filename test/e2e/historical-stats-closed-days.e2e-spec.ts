import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { ConfigModule } from "@nestjs/config";
import { DataSource } from "typeorm";
import type { Redis } from "ioredis";
import { randomUUID } from "node:crypto";
import { formatInTimeZone, fromZonedTime } from "date-fns-tz";
import { getDatabaseConfig } from "../../src/config/database.config";
import { RedisModule, REDIS_CLIENT } from "../../src/common/redis/redis.module";
import { QueueDataModule } from "../../src/queue-data/queue-data.module";
import { ParksModule } from "../../src/parks/parks.module";
import { AnalyticsModule } from "../../src/analytics/analytics.module";
import { ParkHistoricalStatsService } from "../../src/analytics/park-historical-stats.service";
import { QueueDataAggregate } from "../../src/analytics/entities/queue-data-aggregate.entity";
import {
  ScheduleEntry,
  ScheduleType,
} from "../../src/parks/entities/schedule-entry.entity";
import { Park } from "../../src/parks/entities/park.entity";
import { ParkDayOperation } from "../../src/parks/entities/park-day-operation.entity";
import { seedMinimalTestData, clearTestData } from "../helpers/seed-test-data";

/**
 * PAR-692. `byMonth` and `byDayOfWeek` read one list of day values, and that
 * list took every day `queue_data_aggregates` had rows for. Some feeds report
 * rides as OPERATING with a wait of 0 while the park is shut, so Europa-Park's
 * February 2026 (every day CLOSED in the schedule) came out as 28 sample days
 * with a median of 0.
 *
 * The filter is raw SQL, so it runs here against a real database. Each case
 * first shows the day IS counted without the CLOSED entry, so the assertion
 * afterwards is about the filter and not about a day that never got in.
 *
 * PAR-698 adds the two readers the first round missed. `topAttractions` and
 * `/stats/hourly` read `queue_data_aggregates` directly, so they kept counting
 * shut days: 78,425 of Europa-Park's 252,893 aggregate rows over one year sit
 * on them. All three now read one rule, `closed-park-days.sql.ts`.
 */
describe("Park historical stats — days the schedule calls CLOSED (E2E)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let redis: Redis;
  let service: ParkHistoricalStatsService;

  /**
   * The park-local date `n` days back, by calendar arithmetic rather than by
   * shifting an instant.
   *
   * `subDays(new Date(), n)` then formatted in the park's zone is not the same
   * thing: the host runs in UTC and the test parks in America/New_York, so in
   * the UTC hour where the two zones disagree about the date, a window that
   * spans a US DST transition maps two different `n` onto the same park-local
   * date. The counts below are exact equalities, so one duplicate turns a green
   * suite red in March and November — and the runner's 04:00 UTC slot sits
   * inside exactly that hour (G-56). Anchoring on the park-local date and
   * stepping a pure date takes the instant out of the arithmetic.
   */
  function daysAgo(park: Park, n: number): string {
    const today = formatInTimeZone(new Date(), park.timezone, "yyyy-MM-dd");
    const anchor = new Date(`${today}T00:00:00Z`);
    anchor.setUTCDate(anchor.getUTCDate() - n);
    return anchor.toISOString().slice(0, 10);
  }

  /** Two measured hours (12:00 and 13:00 park time) for one ride on one day. */
  async function addDay(
    park: Park,
    attractionId: string,
    day: string,
    wait: number,
  ) {
    const repo = dataSource.getRepository(QueueDataAggregate);
    for (const h of ["12:00:00", "13:00:00"]) {
      await repo.insert({
        id: randomUUID(),
        hour: fromZonedTime(`${day}T${h}`, park.timezone),
        attractionId,
        parkId: park.id,
        p25: wait,
        p50: wait,
        p75: wait,
        p90: wait,
        p95: wait,
        p99: wait,
        iqr: 0,
        stdDev: 0,
        mean: wait,
        sampleCount: 6,
      });
    }
  }

  /** The nightly job's verdict for one park-local day (PAR-697). */
  async function addVerdict(park: Park, day: string, measured: boolean) {
    await dataSource.getRepository(ParkDayOperation).insert({
      parkId: park.id,
      day,
      measuredOperation: measured,
      derivedOpen: null,
      derivedClose: null,
      computedAt: new Date(),
    });
  }

  async function addSchedule(park: Park, day: string, type: ScheduleType) {
    await dataSource.getRepository(ScheduleEntry).insert({
      parkId: park.id,
      date: day as unknown as Date,
      scheduleType: type,
    });
  }

  /** The service caches its DTO for 24 h; every read here must be fresh. */
  async function stats(park: Park) {
    await redis.flushdb();
    return service.getParkHistoricalStats(park, 1);
  }

  /**
   * `topAttractions` with the sample floor lowered to two days: the default 20
   * would need 20 seeded days before a single assertion could run, and the
   * floor is not what these cases are about.
   */
  async function topAttractions(park: Park) {
    await redis.flushdb();
    const result = await service.getParkHistoricalStats(park, 1, 10, 30, 2);
    return result.topAttractions;
  }

  /** `/stats/hourly`, fresh, with the ride floor lowered the same way. */
  async function hourly(park: Park) {
    await redis.flushdb();
    return service.getParkHourlyProfile(park, 1, 8, 2);
  }

  const sum = (rows: Array<{ sampleDays: number }>) =>
    rows.reduce((n, r) => n + r.sampleDays, 0);

  beforeAll(async () => {
    const dbConfig = getDatabaseConfig();
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [
        ConfigModule.forRoot({ isGlobal: true, envFilePath: ".env.test" }),
        TypeOrmModule.forRoot({
          type: "postgres",
          host: dbConfig.host,
          port: dbConfig.port,
          username: dbConfig.username,
          password: dbConfig.password,
          database: dbConfig.database,
          entities: [__dirname + "/../../src/**/*.entity{.ts,.js}"],
          synchronize: true,
          logging: false,
        }),
        RedisModule,
        QueueDataModule,
        ParksModule,
        AnalyticsModule,
      ],
    }).compile();

    app = moduleFixture.createNestApplication();
    await app.init();
    dataSource = app.get(DataSource);
    redis = app.get(REDIS_CLIENT);
    service = app.get(ParkHistoricalStatsService);
  });

  afterAll(async () => {
    await app.close();
  });

  afterEach(async () => {
    await clearTestData(app);
    await redis.flushdb();
  });

  it("drops a CLOSED day from byMonth, byDayOfWeek and the sample-day total", async () => {
    const seeded = await seedMinimalTestData(app);
    const park = seeded.parks[0];
    const ride = seeded.attractions.find((a) => a.parkId === park.id)!;

    const open = daysAgo(park, 13);
    const closed = daysAgo(park, 12);
    const unscheduled = daysAgo(park, 11);
    await addSchedule(park, open, ScheduleType.OPERATING);
    await addDay(park, ride.id, open, 30);
    // The feed's closed day: rides OPERATING, every wait 0.
    await addDay(park, ride.id, closed, 0);
    await addDay(park, ride.id, unscheduled, 20);

    const closedDow = new Date(`${closed}T12:00:00Z`).getUTCDay();

    // Reachable: without a schedule entry the zero day counts.
    const before = await stats(park);
    expect(before.meta.totalSampleDays).toBe(3);
    expect(sum(before.byMonth)).toBe(3);
    expect(sum(before.byDayOfWeek)).toBe(3);
    expect(before.byDayOfWeek.map((d) => d.dayOfWeek)).toContain(closedDow);

    await addSchedule(park, closed, ScheduleType.CLOSED);

    const after = await stats(park);
    expect(after.meta.totalSampleDays).toBe(2);
    expect(sum(after.byMonth)).toBe(2);
    expect(sum(after.byDayOfWeek)).toBe(2);
    expect(after.byDayOfWeek.map((d) => d.dayOfWeek)).not.toContain(closedDow);
    // The zero no longer drags the average: (30 + 20) / 2.
    expect(after.byDayOfWeek.every((d) => d.avgWaitP50 >= 20)).toBe(true);
  });

  it("keeps a day that has an OPERATING entry beside the CLOSED one", async () => {
    const seeded = await seedMinimalTestData(app);
    const park = seeded.parks[0];
    const ride = seeded.attractions.find((a) => a.parkId === park.id)!;

    const day = daysAgo(park, 10);
    await addDay(park, ride.id, day, 25);
    await addSchedule(park, day, ScheduleType.CLOSED);
    await addSchedule(park, day, ScheduleType.OPERATING);

    const result = await stats(park);
    expect(result.meta.totalSampleDays).toBe(1);
  });

  it("counts a CLOSED day again once a verdict says the park operated", async () => {
    // PAR-697. The calendar publishes such a day as an estimated operating day,
    // and this is the other half of that decision: the statistics have to count
    // it as a measuring day, or the split PAR-692 closed reopens on the other
    // side.
    const seeded = await seedMinimalTestData(app);
    const park = seeded.parks[0];
    const ride = seeded.attractions.find((a) => a.parkId === park.id)!;

    const day = daysAgo(park, 10);
    await addDay(park, ride.id, day, 25);
    await addSchedule(park, day, ScheduleType.CLOSED);

    // Reachable: the CLOSED entry alone drops it.
    expect((await stats(park)).meta.totalSampleDays).toBe(0);

    await addVerdict(park, day, true);

    expect((await stats(park)).meta.totalSampleDays).toBe(1);
  });

  it("keeps a CLOSED day dropped when the verdict refused it", async () => {
    // A row with `measured_operation = false` is a verdict, not a missing one:
    // the day was judged and the activity did not clear the gate.
    const seeded = await seedMinimalTestData(app);
    const park = seeded.parks[0];
    const ride = seeded.attractions.find((a) => a.parkId === park.id)!;

    const day = daysAgo(park, 10);
    await addDay(park, ride.id, day, 25);
    await addSchedule(park, day, ScheduleType.CLOSED);
    await addVerdict(park, day, false);

    expect((await stats(park)).meta.totalSampleDays).toBe(0);
  });

  it("does not read another park's verdict for the same day", async () => {
    const seeded = await seedMinimalTestData(app);
    const [park, other] = seeded.parks;
    const ride = seeded.attractions.find((a) => a.parkId === park.id)!;

    const day = daysAgo(park, 10);
    await addDay(park, ride.id, day, 25);
    await addSchedule(park, day, ScheduleType.CLOSED);
    await addVerdict(other, day, true);

    expect((await stats(park)).meta.totalSampleDays).toBe(0);
  });

  it("ignores a single ride's CLOSED entry: only the park's own day counts", async () => {
    const seeded = await seedMinimalTestData(app);
    const park = seeded.parks[0];
    const ride = seeded.attractions.find((a) => a.parkId === park.id)!;

    const day = daysAgo(park, 10);
    await addDay(park, ride.id, day, 25);
    await dataSource.getRepository(ScheduleEntry).insert({
      parkId: park.id,
      attractionId: ride.id,
      date: day as unknown as Date,
      scheduleType: ScheduleType.CLOSED,
    });

    const result = await stats(park);
    expect(result.meta.totalSampleDays).toBe(1);
  });

  it("does not read another park's CLOSED entry", async () => {
    const seeded = await seedMinimalTestData(app);
    const [park, other] = seeded.parks;
    const ride = seeded.attractions.find((a) => a.parkId === park.id)!;

    const day = daysAgo(park, 10);
    await addDay(park, ride.id, day, 25);
    await addSchedule(other, day, ScheduleType.CLOSED);

    const result = await stats(park);
    expect(result.meta.totalSampleDays).toBe(1);
  });

  it("drops a CLOSED day from topAttractions: the ride's average and its sample days", async () => {
    // PAR-698. The ranking and the sample floor both read these rows. A feed
    // posting a wait of 0 through a shut day therefore halved the ride's
    // average AND bought it a day towards the floor that lets it be ranked.
    const seeded = await seedMinimalTestData(app);
    const park = seeded.parks[0];
    const ride = seeded.attractions.find((a) => a.parkId === park.id)!;

    const open = [daysAgo(park, 13), daysAgo(park, 12)];
    const shut = [daysAgo(park, 11), daysAgo(park, 10)];
    for (const day of open) {
      await addSchedule(park, day, ScheduleType.OPERATING);
      await addDay(park, ride.id, day, 40);
    }
    // The feed's shut days: rides OPERATING, every wait 0.
    for (const day of shut) await addDay(park, ride.id, day, 0);

    // Reachable: without the CLOSED entries the zero days count.
    const before = await topAttractions(park);
    const beforeRow = before.find((r) => r.attractionSlug === ride.slug)!;
    expect(beforeRow.sampleDays).toBe(4);
    expect(beforeRow.avgWaitP90).toBe(20);

    for (const day of shut) await addSchedule(park, day, ScheduleType.CLOSED);

    const after = await topAttractions(park);
    const afterRow = after.find((r) => r.attractionSlug === ride.slug)!;
    expect(afterRow.sampleDays).toBe(2);
    expect(afterRow.avgWaitP90).toBe(40);
  });

  it("keeps a ride out of topAttractions when only CLOSED days carried it over the floor", async () => {
    // The floor is a HAVING over the same rows, so the filter has to reach it:
    // one measured day plus three shut ones must not be four days' evidence.
    const seeded = await seedMinimalTestData(app);
    const park = seeded.parks[0];
    const ride = seeded.attractions.find((a) => a.parkId === park.id)!;

    const open = daysAgo(park, 13);
    const shut = [daysAgo(park, 12), daysAgo(park, 11), daysAgo(park, 10)];
    await addSchedule(park, open, ScheduleType.OPERATING);
    await addDay(park, ride.id, open, 40);
    for (const day of shut) await addDay(park, ride.id, day, 0);

    // Reachable: four days clears the two-day floor.
    const before = await topAttractions(park);
    expect(before.map((r) => r.attractionSlug)).toContain(ride.slug);

    for (const day of shut) await addSchedule(park, day, ScheduleType.CLOSED);

    const after = await topAttractions(park);
    expect(after.map((r) => r.attractionSlug)).not.toContain(ride.slug);
  });

  it("drops CLOSED days from /stats/hourly, in the ranking CTE and in the printed hours", async () => {
    // Two halves of one statement read these rows: the CTE picks the rides,
    // the outer SELECT averages their hours. The rule has to sit in both, or
    // the shut days leave the ranking and stay in every number on the table.
    const seeded = await seedMinimalTestData(app);
    const park = seeded.parks[0];
    const ride = seeded.attractions.find((a) => a.parkId === park.id)!;

    // Twelve of each: an hour needs MIN_DAYS_PER_HOUR = 10 measured days to
    // become a column at all, so both the before and the after state have to
    // clear ten on their own.
    const open: string[] = [];
    const shut: string[] = [];
    for (let n = 10; n < 22; n++) open.push(daysAgo(park, n));
    for (let n = 22; n < 34; n++) shut.push(daysAgo(park, n));
    for (const day of open) {
      await addSchedule(park, day, ScheduleType.OPERATING);
      await addDay(park, ride.id, day, 40);
    }
    for (const day of shut) await addDay(park, ride.id, day, 0);

    // Reachable: without the CLOSED entries all 24 days count, and the zeros
    // pull the printed median to 20.
    const before = await hourly(park);
    const beforeRide = before.attractions.find(
      (a) => a.attractionSlug === ride.slug,
    )!;
    expect(beforeRide.sampleDays).toBe(24);
    expect(before.hours).toEqual([12, 13]);
    expect(beforeRide.p50).toEqual([20, 20]);

    for (const day of shut) await addSchedule(park, day, ScheduleType.CLOSED);

    const after = await hourly(park);
    const afterRide = after.attractions.find(
      (a) => a.attractionSlug === ride.slug,
    )!;
    expect(afterRide.sampleDays).toBe(12);
    expect(after.hours).toEqual([12, 13]);
    expect(afterRide.p50).toEqual([40, 40]);
  });

  it("keeps a /stats/hourly day that has an OPERATING entry beside the CLOSED one", async () => {
    // Same exception as the day values: an OPERATING entry means the park
    // opened after all, whatever else the calendar carries for that date.
    const seeded = await seedMinimalTestData(app);
    const park = seeded.parks[0];
    const ride = seeded.attractions.find((a) => a.parkId === park.id)!;

    const days: string[] = [];
    for (let n = 10; n < 22; n++) days.push(daysAgo(park, n));
    for (const day of days) {
      await addDay(park, ride.id, day, 40);
      await addSchedule(park, day, ScheduleType.CLOSED);
      await addSchedule(park, day, ScheduleType.OPERATING);
    }

    const result = await hourly(park);
    const row = result.attractions.find((a) => a.attractionSlug === ride.slug)!;
    expect(row.sampleDays).toBe(12);
    expect(row.p50).toEqual([40, 40]);
  });

  it("does not read another park's CLOSED entry in topAttractions", async () => {
    const seeded = await seedMinimalTestData(app);
    const [park, other] = seeded.parks;
    const ride = seeded.attractions.find((a) => a.parkId === park.id)!;

    const days = [daysAgo(park, 12), daysAgo(park, 11)];
    for (const day of days) {
      await addDay(park, ride.id, day, 40);
      await addSchedule(other, day, ScheduleType.CLOSED);
    }

    const result = await topAttractions(park);
    const row = result.find((r) => r.attractionSlug === ride.slug)!;
    expect(row.sampleDays).toBe(2);
    expect(row.avgWaitP90).toBe(40);
  });
});
