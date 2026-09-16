import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import { Park } from "../../src/parks/entities/park.entity";
import { Attraction } from "../../src/attractions/entities/attraction.entity";
import {
  ScheduleEntry,
  ScheduleType,
} from "../../src/parks/entities/schedule-entry.entity";
import { DataQualityMonitorService } from "../../src/monitoring/data-quality-monitor.service";
import { QueueDataService } from "../../src/queue-data/queue-data.service";
import { PARK_FEED_SILENT_DAYS } from "../../src/common/utils/no-live-data-status.util";

/**
 * `findScheduledButSilentParks` and `hasObservedReadingWithin` against a real
 * PostgreSQL, because the unit spec next to them can only read the SQL text.
 *
 * The detector is four CTEs, two correlated subqueries and a `LEFT JOIN … IS
 * NULL` — the shapes that pass a regular expression and fail a query. The first
 * SQL error of PAR-134 was found by an e2e run and not by any review (📚 G-84),
 * and the same applies here: an anti-join that matches nothing returns an empty
 * list, which is also what "no park is in this state" looks like.
 *
 * So every case is built as a PAIR. The silent park and the healthy park differ
 * in exactly one row — a single `queue_data` reading — and the healthy one has
 * to be ABSENT from the same result the silent one is present in. A test that
 * only asserted the absence would be green against a detector that returns
 * nothing at all (📚 G-44).
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

  const seedPark = async (
    slug: string,
    rideCount: number,
    opts: { futureSchedule: boolean },
  ): Promise<{ parkId: string; rideIds: string[] }> => {
    const park = await dataSource.getRepository(Park).save(
      dataSource.getRepository(Park).create({
        externalId: `e2e-silent-${slug}`,
        name: `Silent ${slug}`,
        slug,
        latitude: 45.52,
        longitude: -73.53,
        timezone: TZ,
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

    if (opts.futureSchedule) {
      // Two days, so `futureOperatingDays` is a count and not a constant that a
      // `count(*) = 1` bug would reproduce by accident.
      for (const offset of [30, 60]) {
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
    await app.init();
    dataSource = app.get(DataSource);
    monitor = app.get(DataQualityMonitorService);
    queueData = app.get(QueueDataService);
  });

  afterAll(async () => {
    await app.close();
  });

  it("names the park whose schedule runs on while its feed stopped", async () => {
    const silent = await seedPark("la-ronde-e2e", 3, { futureSchedule: true });
    // Old enough to be outside the window and recent enough to still be in the
    // 400-day lookback the report reads `lastReading` from — both halves of the
    // detector's date handling are exercised by this one row.
    await reading(silent.rideIds[0], daysAgo(PARK_FEED_SILENT_DAYS + 54));

    const hit = await reportFor(silent.parkId);

    expect(hit).toBeDefined();
    expect(hit!.parkName).toBe("Silent la-ronde-e2e");
    expect(hit!.attractionCount).toBe(3);
    expect(hit!.futureOperatingDays).toBe(2);
    expect(hit!.lastScheduledDay).toBe(daysFromNow(60));
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
      futureSchedule: true,
    });
    const healthy = await seedPark("still-talking-e2e", 2, {
      futureSchedule: true,
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
      futureSchedule: true,
    });
    await reading(reconciled.rideIds[0], daysAgo(1), "system-reconciliation");
    await reading(reconciled.rideIds[1], daysAgo(1), "queue-times", true);

    expect(await reportFor(reconciled.parkId)).toBeDefined();
  });

  it("says nothing about a park whose schedule has run out", async () => {
    // The gate that separates "a contradiction between two of our sources" from
    // "a park that is simply over for the season". Same silence, no claim.
    const overForTheYear = await seedPark("season-over-e2e", 2, {
      futureSchedule: false,
    });

    expect(await reportFor(overForTheYear.parkId)).toBeUndefined();
  });

  describe("hasObservedReadingWithin", () => {
    it("separates the two parks the park payload has to tell apart", async () => {
      const silent = await seedPark("probe-silent-e2e", 2, {
        futureSchedule: true,
      });
      const talking = await seedPark("probe-talking-e2e", 2, {
        futureSchedule: true,
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
        futureSchedule: true,
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
});
