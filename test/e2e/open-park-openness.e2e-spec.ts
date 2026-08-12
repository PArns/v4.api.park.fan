import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { ConfigModule } from "@nestjs/config";
import { DataSource } from "typeorm";
import type { Redis } from "ioredis";
import { AnalyticsModule } from "../../src/analytics/analytics.module";
import { AnalyticsService } from "../../src/analytics/analytics.service";
import { getDatabaseConfig } from "../../src/config/database.config";
import { REDIS_CLIENT, RedisModule } from "../../src/common/redis/redis.module";
import { QueueDataModule } from "../../src/queue-data/queue-data.module";
import { ParksModule } from "../../src/parks/parks.module";
import { Park } from "../../src/parks/entities/park.entity";
import { Attraction } from "../../src/attractions/entities/attraction.entity";
import { createTestPark } from "../fixtures/park.fixtures";
import { createTestAttractions } from "../fixtures/attraction.fixtures";

/**
 * A park whose schedule publishes nothing for today is judged open from its live ride
 * feed alone. Some upstream feeds (queue-times) keep serving the last snapshot after
 * the park closes — same waits, same OPERATING flags, a fresh `last_updated` — so a
 * rule that only asks "is a ride OPERATING with a wait >= 10" reads those parks as
 * open 24/7.
 *
 * These tests pin the distinction the analytics queries have to make: a feed that is
 * MOVING is live evidence, a frozen snapshot is not. They also pin that all three
 * call sites answer "which parks are open" with the same set.
 *
 * The last two parks pin who may take the fallback at all. The gate used to be "this
 * park has never published hours", read across the park's whole schedule history, so
 * one dead sync locked a park into a schedule it could no longer satisfy —
 * Energylandia ran at Ø 45 min for weeks while every listing called it closed. The
 * gate now asks whether hours were published *for today*, which is a question a stale
 * feed answers with "no" and a park that is genuinely shut answers with its CLOSED row.
 */
describe("Ride-fallback openness (E2E)", () => {
  let app: INestApplication;
  let analytics: AnalyticsService;
  let dataSource: DataSource;
  let redis: Redis;

  const FROZEN_SLUG = "test-frozen-feed-park";
  const LIVE_SLUG = "test-live-feed-park";
  const TAIL_SLUG = "test-tail-feed-park";
  /** Published hours until three weeks ago, nothing but UNKNOWN rows since. */
  const STALE_SCHEDULE_SLUG = "test-stale-schedule-park";
  /** Same dead history, but today's row says CLOSED — and that settles it. */
  const CLOSED_TODAY_SLUG = "test-closed-today-park";

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

    analytics = app.get(AnalyticsService);
    dataSource = app.get(DataSource);
    redis = app.get(REDIS_CLIENT);
  });

  afterAll(async () => {
    await app?.close();
  });

  // The shared E2E setup truncates every table after each test, so the feeds have
  // to be rebuilt per test rather than once for the suite.
  beforeEach(async () => {
    // These endpoints cache for five minutes; without a flush every test after the
    // first would assert against the first test's snapshot.
    await redis.flushdb();
    await seedFeeds();
  });

  /**
   * Five parks, none of which publishes hours covering now — so all five are decided
   * by the ride fallback, or by the gate in front of it. Each gets six samples per
   * attraction spread over the last 3.5 hours.
   */
  async function seedFeeds(): Promise<void> {
    const parkRepo = dataSource.getRepository(Park);
    const attractionRepo = dataSource.getRepository(Attraction);

    // Minutes back from now for the six samples of every attraction.
    const offsets = [210, 150, 90, 60, 30, 5];

    const feeds: {
      slug: string;
      country: string;
      waits: number[];
      /** Schedule history to seed, or none at all. */
      schedule?: "stale" | "closed-today";
    }[] = [
      // Frozen: the closed-park-still-reporting case. Identical every sample.
      {
        slug: FROZEN_SLUG,
        country: "frozenland",
        waits: [15, 15, 15, 15, 15, 15],
      },
      // Live: waits move right up to the newest sample.
      { slug: LIVE_SLUG, country: "liveland", waits: [15, 20, 25, 30, 35, 40] },
      // Tail: moved 2.5 h ago, flat since — a real park whose queues settled near
      // closing. Must stay open; this is what makes the change window wider than
      // the 2 h freshness window.
      { slug: TAIL_SLUG, country: "tailland", waits: [15, 25, 25, 25, 25, 25] },
      // Stale schedule: hours in the table, none of them about today. The moving
      // feed is the only thing that knows, so it decides.
      {
        slug: STALE_SCHEDULE_SLUG,
        country: "staleland",
        waits: [15, 20, 25, 30, 35, 40],
        schedule: "stale",
      },
      // Closed today: same dead history plus an explicit CLOSED row for today. The
      // feed moves just like staleland's — the schedule still wins.
      {
        slug: CLOSED_TODAY_SLUG,
        country: "closedtodayland",
        waits: [15, 20, 25, 30, 35, 40],
        schedule: "closed-today",
      },
    ];

    for (const [index, feed] of feeds.entries()) {
      const park = await parkRepo.save(
        createTestPark({
          externalId: `test-park-${feed.slug}`,
          name: feed.slug,
          slug: feed.slug,
          timezone: "Europe/Berlin",
          continent: "Europe",
          continentSlug: "europe",
          country: feed.country,
          countrySlug: feed.country,
          city: feed.country,
          citySlug: feed.country,
        }),
      );

      const attractions = await attractionRepo.save(
        createTestAttractions(park.id, index + 1),
      );

      const values: string[] = [];
      const params: unknown[] = [];
      for (const attraction of attractions) {
        for (const [sample, minutesAgo] of offsets.entries()) {
          params.push(attraction.id, feed.waits[sample], minutesAgo);
          const p = params.length;
          values.push(
            `(gen_random_uuid(), $${p - 2}, 'STANDBY', 'OPERATING', $${p - 1}, NOW() - ($${p} || ' minutes')::interval, 'queue-times')`,
          );
        }
      }
      await dataSource.query(
        `INSERT INTO queue_data (id, "attractionId", "queueType", status, "waitTime", timestamp, data_source)
         VALUES ${values.join(", ")}`,
        params,
      );

      if (feed.schedule) await seedDeadSchedule(park.id, feed.schedule);
    }
  }

  /**
   * A schedule feed that stopped: real hours from 60 down to 21 days ago, then the
   * `UNKNOWN` rows the sync writes when it has a date but no times. `closed-today`
   * adds the one row that still speaks for today.
   */
  async function seedDeadSchedule(
    parkId: string,
    kind: "stale" | "closed-today",
  ): Promise<void> {
    await dataSource.query(
      `INSERT INTO schedule_entries (id, "parkId", date, "scheduleType", "openingTime", "closingTime")
       SELECT gen_random_uuid(), $1, d::date, 'OPERATING',
              d::date + time '10:00', d::date + time '18:00'
       FROM generate_series(CURRENT_DATE - 60, CURRENT_DATE - 21, '1 day') d`,
      [parkId],
    );
    await dataSource.query(
      `INSERT INTO schedule_entries (id, "parkId", date, "scheduleType")
       SELECT gen_random_uuid(), $1, d::date, 'UNKNOWN'
       FROM generate_series(CURRENT_DATE - 20, CURRENT_DATE + 14, '1 day') d
       ${kind === "closed-today" ? "WHERE d::date <> CURRENT_DATE" : ""}`,
      [parkId],
    );

    if (kind === "closed-today") {
      await dataSource.query(
        `INSERT INTO schedule_entries (id, "parkId", date, "scheduleType")
         VALUES (gen_random_uuid(), $1, CURRENT_DATE, 'CLOSED')`,
        [parkId],
      );
    }
  }

  async function openParkSlugsFromGeoLive(): Promise<string[]> {
    const geo = await analytics.getGeoLiveStats();
    const europe = geo.continents.find(
      (c: { slug: string }) => c.slug === "europe",
    );
    return (europe?.countries ?? [])
      .filter((c: { openParkCount: number }) => c.openParkCount > 0)
      .map((c: { slug: string }) => c.slug)
      .sort();
  }

  it("does not count a park whose feed has been frozen for the whole window", async () => {
    const openCountries = await openParkSlugsFromGeoLive();

    expect(openCountries).not.toContain("frozenland");
  });

  it("still counts a park whose feed is moving", async () => {
    const openCountries = await openParkSlugsFromGeoLive();

    expect(openCountries).toContain("liveland");
  });

  it("still counts a park whose feed moved earlier in the window but has since settled", async () => {
    const openCountries = await openParkSlugsFromGeoLive();

    expect(openCountries).toContain("tailland");
  });

  it("counts a park whose schedule feed died weeks ago but whose rides are moving", async () => {
    const openCountries = await openParkSlugsFromGeoLive();

    expect(openCountries).toContain("staleland");
  });

  it("does not count a park whose schedule still says CLOSED for today", async () => {
    const openCountries = await openParkSlugsFromGeoLive();

    expect(openCountries).not.toContain("closedtodayland");
  });

  it("keeps the frozen park out of the global realtime open-park count", async () => {
    const stats = await analytics.getGlobalRealtimeStats();

    expect(stats.counts.openParks).toBe(3);
  });

  it("keeps the frozen park out of the ticker", async () => {
    const ticker = await analytics.getTickerData();
    const parkSlugs = ticker.items.map(
      (item: unknown) => (item as { parkSlug: string }).parkSlug,
    );

    expect(parkSlugs).not.toContain(FROZEN_SLUG);
    expect(parkSlugs).not.toContain(CLOSED_TODAY_SLUG);
    expect(parkSlugs).toEqual(
      expect.arrayContaining([LIVE_SLUG, TAIL_SLUG, STALE_SCHEDULE_SLUG]),
    );
  });

  it("answers the open-park question identically across all three call sites", async () => {
    const [geoCountries, stats, ticker] = await Promise.all([
      openParkSlugsFromGeoLive(),
      analytics.getGlobalRealtimeStats(),
      analytics.getTickerData(),
    ]);

    const tickerParks = new Set(
      ticker.items.map(
        (item: unknown) => (item as { parkSlug: string }).parkSlug,
      ),
    );

    expect(geoCountries).toEqual(["liveland", "staleland", "tailland"]);
    expect(stats.counts.openParks).toBe(geoCountries.length);
    expect(tickerParks.size).toBe(geoCountries.length);
  });
});
