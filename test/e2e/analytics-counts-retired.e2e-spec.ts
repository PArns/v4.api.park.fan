import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { ConfigModule } from "@nestjs/config";
import { DataSource } from "typeorm";
import type { Redis } from "ioredis";
import { getDatabaseConfig } from "../../src/config/database.config";
import { RedisModule, REDIS_CLIENT } from "../../src/common/redis/redis.module";
import { QueueDataModule } from "../../src/queue-data/queue-data.module";
import { ParksModule } from "../../src/parks/parks.module";
import { AnalyticsModule } from "../../src/analytics/analytics.module";
import {
  AnalyticsService,
  GLOBAL_PARK_STATS_SQL,
  OPEN_ATTRACTIONS_COUNT_SQL,
} from "../../src/analytics/analytics.service";
import { Attraction } from "../../src/attractions/entities/attraction.entity";
import { QueueData } from "../../src/queue-data/entities/queue-data.entity";
import { ScheduleEntry } from "../../src/parks/entities/schedule-entry.entity";
import { ParkDailyStats } from "../../src/stats/entities/park-daily-stats.entity";
import {
  LiveStatus,
  QueueType,
  ScheduleType,
} from "../../src/external-apis/themeparks/themeparks.types";
import { getCurrentDateInTimezone } from "../../src/common/utils/date.util";
import { seedMinimalTestData, clearTestData } from "../helpers/seed-test-data";
import { randomUUID } from "node:crypto";

/**
 * PAR-286. The three analytics counters read `FROM attractions` without
 * `retired_at IS NULL`, so a demolished ride stayed in them. Measured against
 * production on 2026-09-17: 49 retired rows in 13 parks, every one of them
 * through the season predicate and therefore fully counted — Universal Studios
 * Singapore served `operatingAttractions: 17` on the nearby list while all 17
 * were retired rows.
 *
 * All of this is raw SQL, so it is executed here against a real database
 * rather than asserted as a string (G-84). A string assertion cannot tell a
 * predicate in the right clause from one in a syntactically legal wrong one —
 * and one of these queries has exactly that trap: `attraction_counts` in
 * `GLOBAL_PARK_STATS_SQL` ends in `... OR lu.status = 'OPERATING'`, where an
 * unparenthesised `AND a.retired_at IS NULL` binds tighter than the `OR` and
 * lets a retired ride with a live reading straight back in.
 *
 * Every case establishes reachability first (G-44/G-72): the ride is counted
 * BEFORE it is retired, so the assertion afterwards is about the predicate and
 * not about an empty table. Each also keeps a neighbour in the same park, so a
 * count that fell because the whole park dropped out is told apart from one
 * that fell because of the retirement.
 */
describe("Analytics counters — retired attractions (E2E)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let redis: Redis;
  let service: AnalyticsService;

  /** Puts the park on `schedule_open_parks`, the cheap way into `park_status`. */
  async function openParkNow(parkId: string) {
    const now = new Date();
    const entry = dataSource.getRepository(ScheduleEntry).create({
      parkId,
      date: now,
      scheduleType: ScheduleType.OPERATING,
      openingTime: new Date(now.getTime() - 60 * 60 * 1000),
      closingTime: new Date(now.getTime() + 60 * 60 * 1000),
    });
    await dataSource.getRepository(ScheduleEntry).save(entry);
  }

  /**
   * A STANDBY reading stamped now. The 2 h window of `getParkStatistics` and
   * the 24 h window of the global statement both contain it, so one helper
   * serves every case here.
   */
  async function addReading(attractionId: string, status: LiveStatus) {
    const row = new QueueData();
    Object.assign(row, {
      id: randomUUID(),
      attractionId,
      queueType: QueueType.STANDBY,
      status,
      waitTime: status === LiveStatus.OPERATING ? 30 : 0,
      timestamp: new Date(),
      lastUpdated: new Date(),
    });
    await dataSource.getRepository(QueueData).save(row);
  }

  async function retire(attractionId: string) {
    const result = await dataSource
      .getRepository(Attraction)
      .update({ id: attractionId }, { retiredAt: new Date() });
    expect(result.affected).toBe(1);
  }

  type ParkStatsRow = {
    id: string;
    total_attractions: string;
    explicitly_closed_attractions: string;
  };

  async function globalStatsFor(parkId: string): Promise<ParkStatsRow> {
    const rows: ParkStatsRow[] = await dataSource.query(GLOBAL_PARK_STATS_SQL);
    const row = rows.find((r) => r.id === parkId);
    // The statement only emits parks it considers open, so a missing row means
    // the park fell out of `park_status` — a different failure from a count of
    // zero, and one the caller must not read as success.
    expect(row).toBeDefined();
    return row!;
  }

  /**
   * `getParkStatistics` caches its whole DTO for 5 minutes under
   * `park:statistics:<id>`. Without this every "after" assertion would read
   * the "before" answer back out of Redis and pass no matter what the SQL does.
   */
  async function parkStatistics(parkId: string) {
    await redis.del(`park:statistics:${parkId}`);
    return service.getParkStatistics(parkId, "America/New_York", new Date());
  }

  /**
   * `getAttractionCounts` is private and — measured on 2026-09-17 —
   * has no caller at all: `grep -rn "getAttractionCounts" src/ test/` finds the
   * definition and two comments. The defect in it is real, so it is fixed and
   * guarded (G-44), and reaching it from a test needs this cast. Should the
   * method ever get a caller, this spec keeps holding; should it be deleted
   * instead, this spec is what says out loud that it was covered.
   */
  function attractionCounts(parkId: string) {
    return (
      service as unknown as {
        getAttractionCounts: (
          id: string,
        ) => Promise<{ total: number; operating: number; closed: number }>;
      }
    ).getAttractionCounts(parkId);
  }

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
    service = app.get(AnalyticsService);
  });

  afterAll(async () => {
    await app.close();
  });

  afterEach(async () => {
    await clearTestData(app);
    await redis.flushdb();
  });

  describe("GLOBAL_PARK_STATS_SQL", () => {
    it("runs against a real database", async () => {
      const rows = await dataSource.query(GLOBAL_PARK_STATS_SQL);
      expect(Array.isArray(rows)).toBe(true);
    });

    it("drops a retired ride out of both total and closed", async () => {
      const seeded = await seedMinimalTestData(app);
      const park = seeded.parks[0];
      const [open, closed, alsoClosed] = seeded.attractions.filter(
        (a) => a.parkId === park.id,
      );
      await openParkNow(park.id);
      await addReading(open.id, LiveStatus.OPERATING);
      await addReading(closed.id, LiveStatus.CLOSED);
      await addReading(alsoClosed.id, LiveStatus.CLOSED);

      // Reachable (G-44): all five seeded rides are in `total`, and the three
      // without an OPERATING reading are in `closed` — the two with no reading
      // at all count as closed here, which is the reason a retired row moves
      // BOTH numbers.
      const before = await globalStatsFor(park.id);
      expect(Number(before.total_attractions)).toBe(5);
      expect(Number(before.explicitly_closed_attractions)).toBe(4);

      await retire(closed.id);

      const after = await globalStatsFor(park.id);
      expect(Number(after.total_attractions)).toBe(4);
      expect(Number(after.explicitly_closed_attractions)).toBe(3);
    });

    it("drops a retired ride that still reports OPERATING", async () => {
      // The parenthesisation guard. `attraction_counts` reads
      // `WHERE a.retired_at IS NULL AND (NOT <season> OR lu.status = ...)`.
      // Drop those brackets and `AND` binds tighter than `OR`, so this ride —
      // retired, but with a live OPERATING row — walks back into the count and
      // every other case in this file still passes.
      const seeded = await seedMinimalTestData(app);
      const park = seeded.parks[0];
      const [open, neighbour] = seeded.attractions.filter(
        (a) => a.parkId === park.id,
      );
      await openParkNow(park.id);
      await addReading(open.id, LiveStatus.OPERATING);
      await addReading(neighbour.id, LiveStatus.OPERATING);

      expect(Number((await globalStatsFor(park.id)).total_attractions)).toBe(5);

      await retire(open.id);

      // The neighbour's OPERATING row is what keeps the park in `park_status`,
      // so this 4 is the predicate at work and not the park falling out.
      expect(Number((await globalStatsFor(park.id)).total_attractions)).toBe(4);
    });

    it("leaves the other park's counts alone", async () => {
      const seeded = await seedMinimalTestData(app);
      const [park, otherPark] = seeded.parks;
      await openParkNow(park.id);
      await openParkNow(otherPark.id);
      for (const a of seeded.attractions) {
        await addReading(a.id, LiveStatus.OPERATING);
      }
      const otherBefore = Number(
        (await globalStatsFor(otherPark.id)).total_attractions,
      );

      await retire(seeded.attractions.find((a) => a.parkId === park.id)!.id);

      expect(
        Number((await globalStatsFor(otherPark.id)).total_attractions),
      ).toBe(otherBefore);
    });
  });

  describe("OPEN_ATTRACTIONS_COUNT_SQL", () => {
    it("stops counting a retired ride as open", async () => {
      const seeded = await seedMinimalTestData(app);
      const park = seeded.parks[0];
      const [open, neighbour] = seeded.attractions.filter(
        (a) => a.parkId === park.id,
      );
      await addReading(open.id, LiveStatus.OPERATING);
      await addReading(neighbour.id, LiveStatus.OPERATING);

      // Reachable (G-44). This counter is global and has no park filter, so
      // the number is over the whole seeded database, not over one park.
      const count = async () =>
        Number((await dataSource.query(OPEN_ATTRACTIONS_COUNT_SQL))[0].count);
      expect(await count()).toBe(2);

      await retire(open.id);

      // The neighbour's reading is untouched, so the drop is the predicate and
      // not the 24 h window closing under both rides.
      expect(await count()).toBe(1);
    });
  });

  describe("getParkStatistics", () => {
    /**
     * Two branches, and they carry two copies of the same SQL: the fast path
     * fires when a `ParkDailyStats` row exists for the park's today and reads
     * `avg_wait_today`/`max_wait_today` from it, the slow path aggregates them
     * from `queue_data`. Both count attractions the same way, so a fix applied
     * to one and not the other is invisible from outside — hence one case per
     * branch rather than one case for the method.
     */
    async function seedParkWithReadings() {
      const seeded = await seedMinimalTestData(app);
      const park = seeded.parks[0];
      const attractions = seeded.attractions.filter(
        (a) => a.parkId === park.id,
      );
      await openParkNow(park.id);
      await addReading(attractions[0].id, LiveStatus.OPERATING);
      await addReading(attractions[1].id, LiveStatus.CLOSED);
      return { park, attractions };
    }

    /**
     * Each branch carries the predicate TWICE, in two reads of `attractions`
     * that feed two different columns: the `attraction_counts` CTE behind
     * `total_count`, and the outer query behind `explicitly_closed_count`.
     * Asserting the total alone leaves the second free to lose its predicate
     * without turning anything red, so the retired ride here is the one
     * carrying the CLOSED reading and both columns are read back.
     *
     * `closedAttractions` is `total - operating`, and `operatingAttractions`
     * is `total - explicitlyClosed`, so the pair below pins the outer read:
     * with the predicate the retired ride leaves `explicitly_closed_count`
     * (4 operating, 0 closed), without it it stays (3 operating, 1 closed).
     */
    async function expectCountsAfterRetiringTheClosedRide(
      parkId: string,
      closedRideId: string,
    ) {
      const before = await parkStatistics(parkId);
      expect(before.totalAttractions).toBe(5);
      expect(before.operatingAttractions).toBe(4);
      expect(before.closedAttractions).toBe(1);

      await retire(closedRideId);

      const after = await parkStatistics(parkId);
      expect(after.totalAttractions).toBe(4);
      expect(after.operatingAttractions).toBe(4);
      expect(after.closedAttractions).toBe(0);
    }

    it("counts a ride until it is retired (slow path)", async () => {
      const { park, attractions } = await seedParkWithReadings();
      await expectCountsAfterRetiringTheClosedRide(park.id, attractions[1].id);
    });

    it("counts a ride until it is retired (fast path)", async () => {
      const { park, attractions } = await seedParkWithReadings();
      // The fast path's only entry condition: a daily-stats row for the park's
      // today, with both figures present. "Today" is the PARK's, read with the
      // same helper the service uses — a UTC date would miss the row between
      // 00:00 and ~05:00 UTC and drop this case silently into the slow path,
      // where it would be a second copy of the one above and still pass.
      await dataSource.getRepository(ParkDailyStats).save(
        dataSource.getRepository(ParkDailyStats).create({
          parkId: park.id,
          date: getCurrentDateInTimezone("America/New_York"),
          p90WaitTime: 25,
          maxWaitTime: 60,
        }),
      );

      // Proof that the branch was taken: both figures come straight from the
      // row above. The slow path aggregates them from `queue_data` inside
      // [startOfDay, now] and answers 0 here, because the readings were
      // written before `startOfDay`.
      const before = await parkStatistics(park.id);
      expect(before.avgWaitToday).toBe(25);
      expect(before.peakWaitToday).toBe(60);
      await redis.del(`park:statistics:${park.id}`);

      await expectCountsAfterRetiringTheClosedRide(park.id, attractions[1].id);
    });

    it("does not count a retired ride as one you can queue for", async () => {
      // `operatingAttractions` is `total - explicitlyClosed`, and the
      // subtrahend does not move: a retired ride has no fresh reading, so it
      // was never in `explicitly_closed_count`. The whole error therefore
      // landed in this difference — this is the number `getNearbyParks`
      // publishes, and the one that told a shut Universal Studios Singapore to
      // report 17 open rides.
      const { park, attractions } = await seedParkWithReadings();
      const before = await parkStatistics(park.id);
      expect(before.operatingAttractions).toBe(4);

      await retire(attractions[2].id);

      expect((await parkStatistics(park.id)).operatingAttractions).toBe(3);
    });
  });

  describe("getAttractionCounts", () => {
    it("stops counting a retired ride in total and closed", async () => {
      const seeded = await seedMinimalTestData(app);
      const park = seeded.parks[0];
      const attractions = seeded.attractions.filter(
        (a) => a.parkId === park.id,
      );
      await addReading(attractions[0].id, LiveStatus.OPERATING);

      // Reachable (G-44).
      const before = await attractionCounts(park.id);
      expect(before.total).toBe(5);
      expect(before.operating).toBe(1);
      expect(before.closed).toBe(4);

      await retire(attractions[1].id);

      const after = await attractionCounts(park.id);
      expect(after.total).toBe(4);
      // The point of the whole method: `closed` is derived, so it only moves
      // because `total` did.
      expect(after.closed).toBe(3);
      expect(after.operating).toBe(1);
    });

    it("stops counting a retired ride as operating", async () => {
      // The other half. Filtering `operating_count` alone changes nothing in
      // production (no retired row carries an OPERATING reading there), but it
      // is the half that keeps `closed` from going negative the day one does.
      const seeded = await seedMinimalTestData(app);
      const park = seeded.parks[0];
      const attractions = seeded.attractions.filter(
        (a) => a.parkId === park.id,
      );
      await addReading(attractions[0].id, LiveStatus.OPERATING);
      await addReading(attractions[1].id, LiveStatus.OPERATING);

      expect((await attractionCounts(park.id)).operating).toBe(2);

      await retire(attractions[0].id);

      const after = await attractionCounts(park.id);
      expect(after.operating).toBe(1);
      expect(after.total).toBe(4);
      expect(after.closed).toBe(3);
    });
  });
});
