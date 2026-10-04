import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { ConfigModule } from "@nestjs/config";
import { DataSource } from "typeorm";
import type { Redis } from "ioredis";
import { randomUUID } from "node:crypto";
import { formatInTimeZone, fromZonedTime } from "date-fns-tz";
import { subDays } from "date-fns";
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
 */
describe("Park historical stats — days the schedule calls CLOSED (E2E)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let redis: Redis;
  let service: ParkHistoricalStatsService;

  /** Four consecutive park-local days, 10 to 13 days ago: four distinct weekdays. */
  function daysAgo(park: Park, n: number): string {
    return formatInTimeZone(
      subDays(new Date(), n),
      park.timezone,
      "yyyy-MM-dd",
    );
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
});
