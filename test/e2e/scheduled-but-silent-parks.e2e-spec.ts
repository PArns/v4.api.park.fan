import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import request from "supertest";
import type { Redis } from "ioredis";
import { DataSource } from "typeorm";
import { REDIS_CLIENT } from "../../src/common/redis/redis.module";
import { AppModule } from "../../src/app.module";
import { Park } from "../../src/parks/entities/park.entity";
import { Attraction } from "../../src/attractions/entities/attraction.entity";
import {
  ScheduleEntry,
  ScheduleType,
} from "../../src/parks/entities/schedule-entry.entity";
import {
  DataQualityMonitorService,
  SILENT_PARK_LOOKAHEAD_DAYS,
} from "../../src/monitoring/data-quality-monitor.service";
import { QueueDataService } from "../../src/queue-data/queue-data.service";
import { PARK_FEED_SILENT_DAYS } from "../../src/common/utils/no-live-data-status.util";

/**
 * `findScheduledButSilentParks` and `hasObservedReadingWithin` against a real
 * PostgreSQL, because the unit spec next to them can only read the SQL text.
 *
 * The detector is three CTEs, a correlated subquery and a `LEFT JOIN … IS
 * NULL` — the shapes that pass a regular expression and fail a query. The first
 * SQL error of PAR-134 was found by an e2e run and not by any review (📚 G-84),
 * and the same applies here: an anti-join that matches nothing returns an empty
 * list, which is also what "no park is in this state" looks like.
 *
 * So a case is built as a PAIR wherever the assertion is an absence. The silent
 * park and the healthy park differ in exactly one row — a single `queue_data`
 * reading — and the healthy one has to be ABSENT from the same result the
 * silent one is present in; an absence asserted on its own is green against a
 * detector that returns nothing at all (📚 G-44).
 *
 * Two cases are deliberately unpaired and mutation-checked instead: "ignores a
 * per-ride schedule row" and "does not let a heartbeat stand in for an
 * observation". Each one's gate was removed and each one turned red, which is
 * the same proof by a shorter route — their fixtures are one park and one row,
 * so a partner would only restate the case above.
 *
 * Dates are relative to `now()`. The detector compares against
 * `(now() AT TIME ZONE p.timezone)::date`, so a fixture pinned to a written-down
 * date stops being "the future" on its own (📚 G-56).
 */
describe("scheduled but silent parks (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let monitor: DataQualityMonitorService;
  let queueData: QueueDataService;
  let redis: Redis;

  const TZ = "America/Toronto";
  const daysFromNow = (days: number): string =>
    new Date(Date.now() + days * 86_400_000).toISOString().slice(0, 10);
  const daysAgo = (days: number): Date =>
    new Date(Date.now() - days * 86_400_000);
  /**
   * The same instant as the park reads it. Written out rather than as a fixed
   * offset: Toronto is UTC-4 in summer and UTC-5 in winter, so a hard-coded
   * offset would make this file pass or fail by the month it runs in (📚 G-56).
   */
  const parkLocalDate = (at: Date): string =>
    new Intl.DateTimeFormat("en-CA", { timeZone: TZ }).format(at);

  /**
   * Two days rather than one, so `operatingDaysAhead` is a count and not a
   * constant a `count(*) = 1` bug would reproduce by accident, and both inside
   * `SILENT_PARK_LOOKAHEAD_DAYS` so the window is not what excludes them.
   */
  const FUTURE_DAYS = [2, 5];
  /**
   * Two days inside the window, one outside, and day 2 twice.
   *
   * `operatingDaysAhead` has to come back as 2 against this, which is the only
   * shape that pins both halves of its expression: drop the `FILTER` and day 8
   * makes it 3, drop the `DISTINCT` and the duplicate makes it 3 as well.
   */
  const MIXED_DAYS = [2, 2, 5, SILENT_PARK_LOOKAHEAD_DAYS + 1];
  /**
   * The same shape in the past. A park with NO schedule row at all would drop
   * out of the join for a second reason, and the case below would stay green
   * with the date comparison removed — measured: it did, until this fixture
   * grew its past days.
   */
  const PAST_DAYS = [-60, -30];

  const seedPark = async (
    slug: string,
    rideCount: number,
    opts: {
      scheduleDayOffsets: number[];
      openNow?: boolean;
      timezone?: string;
    },
  ): Promise<{ parkId: string; rideIds: string[] }> => {
    const park = await dataSource.getRepository(Park).save(
      dataSource.getRepository(Park).create({
        externalId: `e2e-silent-${slug}`,
        name: `Silent ${slug}`,
        slug,
        latitude: 45.52,
        longitude: -73.53,
        timezone: opts.timezone ?? TZ,
        continent: "North America",
        continentSlug: "north-america",
        country: "Canada",
        countrySlug: "canada",
        countryCode: "CA",
        city: "Montréal",
        citySlug: `montreal-${slug}`,
      }),
    );

    const rideIds: string[] = [];
    for (let i = 0; i < rideCount; i++) {
      const ride = await dataSource.getRepository(Attraction).save(
        dataSource.getRepository(Attraction).create({
          externalId: `e2e-silent-${slug}-${i}`,
          name: `Ride ${i} at ${slug}`,
          slug: `${slug}-ride-${i}`,
          parkId: park.id,
          latitude: 45.52,
          longitude: -73.53,
        }),
      );
      rideIds.push(ride.id);
    }

    for (const offset of opts.scheduleDayOffsets) {
      await dataSource.getRepository(ScheduleEntry).save(
        dataSource.getRepository(ScheduleEntry).create({
          parkId: park.id,
          attractionId: null,
          date: daysFromNow(offset) as unknown as Date,
          scheduleType: ScheduleType.OPERATING,
          openingTime: new Date(`${daysFromNow(offset)}T14:00:00Z`),
          closingTime: new Date(`${daysFromNow(offset)}T23:00:00Z`),
        }),
      );
    }

    if (opts.openNow) {
      // Gates opened ten minutes ago and close in eight hours, so the park reads
      // OPERATING at whatever hour the suite runs — which is the only state in
      // which the optimistic fallback is reached at all.
      await dataSource.query(
        `INSERT INTO schedule_entries (id, "parkId", date, "scheduleType", "openingTime", "closingTime")
         VALUES (gen_random_uuid(), $1, CURRENT_DATE, 'OPERATING',
                 NOW() - INTERVAL '10 minutes', NOW() + INTERVAL '8 hours')`,
        [park.id],
      );
    }

    return { parkId: park.id, rideIds };
  };

  /**
   * One reading. `is_heartbeat` and `data_source` are written explicitly:
   * `observedReadingsSql()` reads both, and a fixture that left them to their
   * defaults would be asserting against the defaults rather than against the
   * predicate.
   */
  const reading = async (
    rideId: string,
    at: Date,
    source = "queue-times",
    isHeartbeat = false,
  ): Promise<void> => {
    await dataSource.query(
      `INSERT INTO queue_data (id, "attractionId", "queueType", status, timestamp,
                               is_heartbeat, data_source, "lastUpdated")
       VALUES (gen_random_uuid(), $1, 'STANDBY', 'OPERATING', $2, $3, $4, $5)`,
      [rideId, at, isHeartbeat, source, new Date(at.getTime() - 60_000)],
    );
  };

  const reportFor = async (parkId: string) =>
    (await monitor.findScheduledButSilentParks()).find(
      (p) => p.parkId === parkId,
    );

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    app = moduleFixture.createNestApplication();
    // Production serves everything under /v1; without this the park path 404s
    // regardless of the data.
    app.setGlobalPrefix("v1");
    await app.init();
    dataSource = app.get(DataSource);
    monitor = app.get(DataQualityMonitorService);
    queueData = app.get(QueueDataService);
    redis = app.get(REDIS_CLIENT);
  });

  afterAll(async () => {
    await app.close();
  });

  it("names the park whose schedule runs on while its feed stopped", async () => {
    const silent = await seedPark("la-ronde-e2e", 3, {
      scheduleDayOffsets: FUTURE_DAYS,
    });
    // Old enough to be outside the window and recent enough to still be in the
    // 400-day lookback the report reads `lastReading` from — both halves of the
    // detector's date handling are exercised by this one row.
    await reading(silent.rideIds[0], daysAgo(PARK_FEED_SILENT_DAYS + 54));

    const hit = await reportFor(silent.parkId);

    expect(hit).toBeDefined();
    expect(hit!.parkName).toBe("Silent la-ronde-e2e");
    expect(hit!.attractionCount).toBe(3);
    expect(hit!.operatingDaysAhead).toBe(2);
    expect(hit!.lastScheduledDay).toBe(daysFromNow(5));
    // The date is park-local, which is why the report can print it next to a
    // park name without a timezone beside it.
    expect(hit!.lastReading).toBe(
      parkLocalDate(daysAgo(PARK_FEED_SILENT_DAYS + 54)),
    );
  });

  it("leaves an identically shaped park alone as soon as one reading arrives", async () => {
    // The presence half. This park differs from the one above in a single
    // queue_data row; if the detector returned nothing at all, the assertion
    // below would pass for the wrong reason, so it is read off the SAME call
    // that finds the silent park.
    const silent = await seedPark("still-silent-e2e", 2, {
      scheduleDayOffsets: FUTURE_DAYS,
    });
    const healthy = await seedPark("still-talking-e2e", 2, {
      scheduleDayOffsets: FUTURE_DAYS,
    });
    await reading(healthy.rideIds[0], daysAgo(1));

    const report = await monitor.findScheduledButSilentParks();

    expect(report.map((p) => p.parkId)).toContain(silent.parkId);
    expect(report.map((p) => p.parkId)).not.toContain(healthy.parkId);
  });

  it("counts our own bookkeeping as silence, not as a reading", async () => {
    // A reconciliation row and a carried heartbeat are both fresh and both say
    // nothing about whether anyone is watching this park. Without
    // observedReadingsSql they would clear the anti-join and hide the park.
    const reconciled = await seedPark("reconciled-e2e", 2, {
      scheduleDayOffsets: FUTURE_DAYS,
    });
    await reading(reconciled.rideIds[0], daysAgo(1), "system-reconciliation");
    await reading(reconciled.rideIds[1], daysAgo(1), "queue-times", true);

    expect(await reportFor(reconciled.parkId)).toBeDefined();
  });

  it("says nothing about a park whose schedule has run out", async () => {
    // The gate that separates "a contradiction between two of our sources" from
    // "a park that is simply over for the season". Same silence, no claim.
    //
    // Read as a pair with a silent park seeded beside it, for the reason every
    // case here is: `toBeUndefined()` alone is also what a detector returning
    // nothing at all looks like. Both parks come out of the same call.
    const overForTheYear = await seedPark("season-over-e2e", 2, {
      scheduleDayOffsets: PAST_DAYS,
    });
    const stillClaimed = await seedPark("season-open-e2e", 2, {
      scheduleDayOffsets: FUTURE_DAYS,
    });

    const report = await monitor.findScheduledButSilentParks();

    expect(report.map((p) => p.parkId)).toContain(stillClaimed.parkId);
    expect(report.map((p) => p.parkId)).not.toContain(overForTheYear.parkId);
  });

  it("counts the days inside the window, once each", async () => {
    const mixed = await seedPark("mixed-days-e2e", 2, {
      scheduleDayOffsets: MIXED_DAYS,
    });

    const hit = await reportFor(mixed.parkId);

    expect(hit).toBeDefined();
    expect(hit!.operatingDaysAhead).toBe(2);
    // `lastScheduledDay` is deliberately NOT windowed — it is the number that
    // says how far the unconfirmed calendar runs, which at La Ronde is 2027.
    expect(hit!.lastScheduledDay).toBe(
      daysFromNow(SILENT_PARK_LOOKAHEAD_DAYS + 1),
    );
  });

  it("holds its tongue about next summer until the week it starts", async () => {
    // A seasonal park shut for the winter has future operating days and an
    // empty feed, and is neither a fault nor news. Without the lookahead window
    // this detector is a January alarm for every one of them.
    const nextSeason = await seedPark("next-season-e2e", 2, {
      scheduleDayOffsets: [SILENT_PARK_LOOKAHEAD_DAYS + 1, 120],
    });
    const openThisWeek = await seedPark("open-this-week-e2e", 2, {
      scheduleDayOffsets: FUTURE_DAYS,
    });

    const report = await monitor.findScheduledButSilentParks();

    expect(report.map((p) => p.parkId)).toContain(openThisWeek.parkId);
    expect(report.map((p) => p.parkId)).not.toContain(nextSeason.parkId);
  });

  it("ignores a per-ride schedule row when it reads the park's own calendar", async () => {
    // `schedule_entries` holds both; every other reader of a park's hours
    // filters `attractionId IS NULL`, and PAR-246 is what the same blind key
    // cost on the cleanup path. The park's own calendar has run out here and
    // only a ride still claims days, so the park must stay off the list.
    const rideOnly = await seedPark("ride-schedule-only-e2e", 2, {
      scheduleDayOffsets: PAST_DAYS,
    });
    await dataSource.getRepository(ScheduleEntry).save(
      dataSource.getRepository(ScheduleEntry).create({
        parkId: rideOnly.parkId,
        attractionId: rideOnly.rideIds[0],
        date: daysFromNow(3) as unknown as Date,
        scheduleType: ScheduleType.OPERATING,
        openingTime: new Date(`${daysFromNow(3)}T14:00:00Z`),
        closingTime: new Date(`${daysFromNow(3)}T23:00:00Z`),
      }),
    );

    expect(await reportFor(rideOnly.parkId)).toBeUndefined();
  });

  describe("hasObservedReadingWithin", () => {
    it("separates the two parks the park payload has to tell apart", async () => {
      const silent = await seedPark("probe-silent-e2e", 2, {
        scheduleDayOffsets: FUTURE_DAYS,
      });
      const talking = await seedPark("probe-talking-e2e", 2, {
        scheduleDayOffsets: FUTURE_DAYS,
      });
      await reading(silent.rideIds[0], daysAgo(PARK_FEED_SILENT_DAYS + 1));
      await reading(talking.rideIds[0], daysAgo(PARK_FEED_SILENT_DAYS - 1));

      // Both parks hold a row. Only the side of the window they fall on differs,
      // so a probe that ignored its `days` argument would fail here rather than
      // pass on an empty table.
      expect(
        await queueData.hasObservedReadingWithin(
          talking.parkId,
          PARK_FEED_SILENT_DAYS,
        ),
      ).toBe(true);
      expect(
        await queueData.hasObservedReadingWithin(
          silent.parkId,
          PARK_FEED_SILENT_DAYS,
        ),
      ).toBe(false);
    });

    it("does not let a heartbeat stand in for an observation", async () => {
      const carried = await seedPark("probe-heartbeat-e2e", 1, {
        scheduleDayOffsets: FUTURE_DAYS,
      });
      await reading(carried.rideIds[0], daysAgo(1), "queue-times", true);

      expect(
        await queueData.hasObservedReadingWithin(
          carried.parkId,
          PARK_FEED_SILENT_DAYS,
        ),
      ).toBe(false);
    });
  });

  /**
   * What a visitor gets, which is the half of PAR-192 that is not a log line.
   *
   * Read as a pair for the reason every case here is, and this one especially:
   * the two parks are seeded identically and open right now, and the ONLY
   * difference between them is how old their single reading is. Delete
   * `&& parkObservedRecently` from `ParkIntegrationService` and the two
   * responses become the same, which is what these assertions catch.
   *
   * `UTC` rather than Toronto: `CURRENT_DATE` in the schedule insert is the
   * server's, and only UTC makes park-local "today" agree with it at every hour
   * the suite might run.
   */
  describe("GET /v1/parks/:continent/:country/:city/:slug", () => {
    const geoPath = (slug: string): string =>
      `/v1/parks/north-america/canada/montreal-${slug}/${slug}`;

    const openSilentPark = (slug: string, lastReadingDaysAgo: number) =>
      seedPark(slug, 2, {
        scheduleDayOffsets: FUTURE_DAYS,
        openNow: true,
        timezone: "UTC",
      }).then(async (park) => {
        await reading(park.rideIds[0], daysAgo(lastReadingDaysAgo));
        await reading(park.rideIds[1], daysAgo(lastReadingDaysAgo));
        return park;
      });

    it("says UNKNOWN about the rides of a park nobody has read in a month", async () => {
      // The park response caches under the request URL and under the park id.
      await redis.flushdb();
      const slug = "silent-payload-e2e";
      await openSilentPark(slug, PARK_FEED_SILENT_DAYS + 1);

      const { body } = await request(app.getHttpServer())
        .get(geoPath(slug))
        .expect(200);

      expect(body.status).toBe("OPERATING");
      expect(body.attractions).toHaveLength(2);
      for (const ride of body.attractions) {
        expect(ride.status).toBe("UNKNOWN");
        expect(ride.effectiveStatus).toBe("UNKNOWN");
        // Not `very_low`, which is the last-resort default of the crowd chain
        // and reads as "walk on, no queues" off no data at all.
        expect(ride.crowdLevel).toBe("unknown");
      }
      // "38 of 38 closed" under an OPERATING badge is the same false page from
      // the other direction: none of them is known to be closed either.
      expect(body.analytics.statistics.operatingAttractions).toBe(0);
      expect(body.analytics.statistics.closedAttractions).toBe(0);
      expect(body.analytics.statistics.crowdLevel).toBe("unknown");
      // PAR-298: the aggregates go with the tier. Over no wait times they are
      // a division by the empty set — Ø 0 min under an OPERATING badge — and
      // the occupancy object rated that 0 % "typical".
      expect(body.analytics.statistics.avgWaitTime).toBeNull();
      expect(body.analytics.statistics.avgWaitToday).toBeNull();
      expect(body.analytics.statistics.peakWaitToday).toBeNull();
      expect(body.analytics.occupancy).toBeUndefined();
    });

    it("still says 'all of them closed' when the park itself is shut", async () => {
      // The counter override is gated on the park being OPERATING, and this is
      // the case that gate exists for. A shut park's rides ARE closed for a
      // reason we can state, and `closedAttractions: 0` there would replace a
      // true answer with a shrug — which is what the first draft of the fix
      // did. Without `openNow` there is no schedule row for today at all.
      await redis.flushdb();
      const slug = "silent-and-shut-e2e";
      const park = await seedPark(slug, 2, {
        scheduleDayOffsets: FUTURE_DAYS,
        timezone: "UTC",
      });
      await reading(park.rideIds[0], daysAgo(PARK_FEED_SILENT_DAYS + 1));

      const { body } = await request(app.getHttpServer())
        .get(geoPath(slug))
        .expect(200);

      expect(body.status).toBe("CLOSED");
      for (const ride of body.attractions) {
        expect(ride.effectiveStatus).toBe("CLOSED");
      }
      expect(body.analytics.statistics.operatingAttractions).toBe(0);
      expect(body.analytics.statistics.closedAttractions).toBe(2);
    });

    it("keeps the optimistic fallback for a park whose feed is alive", async () => {
      // One day inside the window instead of one day outside it, and nothing
      // else differs. The reading is older than the freshness cutoff, so these
      // rides reach the very branch the case above no longer reaches.
      await redis.flushdb();
      const slug = "talking-payload-e2e";
      await openSilentPark(slug, PARK_FEED_SILENT_DAYS - 1);

      const { body } = await request(app.getHttpServer())
        .get(geoPath(slug))
        .expect(200);

      expect(body.status).toBe("OPERATING");
      expect(body.attractions).toHaveLength(2);
      for (const ride of body.attractions) {
        expect(ride.status).toBe("OPERATING");
        expect(ride.effectiveStatus).toBe("OPERATING");
      }
      // The other half of the PAR-298 pair: one day inside the window, and the
      // payload keeps every figure. A gate that fires here is not a gate.
      expect(body.analytics.occupancy).toBeDefined();
      expect(body.analytics.statistics.avgWaitTime).not.toBeNull();
    });
  });
});
