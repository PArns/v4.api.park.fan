import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import { Park } from "../../src/parks/entities/park.entity";
import { Attraction } from "../../src/attractions/entities/attraction.entity";
import { ScheduleEntry } from "../../src/parks/entities/schedule-entry.entity";
import { ScheduleType } from "../../src/parks/entities/schedule-entry.entity";
import { ParkDowntimeCoverage } from "../../src/analytics/entities/park-downtime-coverage.entity";
import { CURRENT_CLOSURE_GAP_SQL } from "../../src/common/utils/closure-gap.sql";

/**
 * The live closure-gap statement against a park that closes after midnight.
 *
 * The unit spec next to the SQL asserts its SHAPE — that the day comes from
 * `win` and not from a `::date` cast. Only a real PostgreSQL says whether the
 * statement then returns the row, and this file exists because the shape
 * assertions cannot: a CTE that selects a column nobody reads, a join that
 * silently matches nothing, a comparison whose two sides are typed differently
 * all pass a regular expression and fail a query.
 *
 * The fixture is the case the whole change is about, built to the smallest size
 * that still exercises every gate:
 *
 * - a blind park (`regime = 'never_reports'`), because the statement opens with
 *   an EXISTS on that and returns nothing otherwise,
 * - one published `OPERATING` window running 10:00 → 02:00 the next park-local
 *   day, which is a genuine wrap rather than the 00:00 close the production
 *   data happens to hold today (measured 2026-09-15: every wrap day in the last
 *   30 days closes at exactly 00:00, and `[opens, 00:00)` contains no instant of
 *   the following date, so the two day notions cannot differ there),
 * - one ride, OPERATING before midnight and CLOSED after it.
 *
 * `attraction_exposure_days` is deliberately left empty: `active_days` is then 0,
 * which is below `MIN_DAYS_FOR_CYCLE_TEST`, and the duty-cycle arm passes
 * unconditionally. That is the same reasoning the statement's own comment gives
 * for keeping the left arm of that OR, and it keeps this fixture about the day
 * keying rather than about the filters stacked behind it.
 */
describe("closure gaps across park-local midnight (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;

  const TZ = "Europe/Berlin";
  // June, far from either DST boundary: a fixture that also crosses a shift
  // would be testing two things and telling you which one failed by coin toss.
  const DAY = "2026-06-15";
  const NEXT = "2026-06-16";
  // 10:00 → 02:00 the following park-local day. +02:00 is Berlin's summer offset.
  const OPENS = new Date(`${DAY}T10:00:00+02:00`);
  const CLOSES = new Date(`${NEXT}T02:00:00+02:00`);

  let parkId: string;
  let rideId: string;

  const seedPark = async (closesAt: Date): Promise<void> => {
    const park = await dataSource.getRepository(Park).save(
      dataSource.getRepository(Park).create({
        externalId: "e2e-wrap-park",
        name: "Wrap Park",
        slug: "wrap-park",
        latitude: 50.8,
        longitude: 6.9,
        timezone: TZ,
        continent: "Europe",
        continentSlug: "europe",
        country: "Germany",
        countrySlug: "germany",
        countryCode: "DE",
        city: "Brühl",
        citySlug: "bruehl",
      }),
    );
    parkId = park.id;

    const ride = await dataSource.getRepository(Attraction).save(
      dataSource.getRepository(Attraction).create({
        externalId: "e2e-wrap-ride",
        name: "Night Coaster",
        slug: "night-coaster",
        parkId: park.id,
        latitude: 50.8,
        longitude: 6.9,
      }),
    );
    rideId = ride.id;

    await dataSource.getRepository(ScheduleEntry).save(
      dataSource.getRepository(ScheduleEntry).create({
        parkId: park.id,
        attractionId: null,
        date: DAY as unknown as Date,
        scheduleType: ScheduleType.OPERATING,
        openingTime: OPENS,
        closingTime: closesAt,
      }),
    );

    // The regime table is read, never re-derived — the statement says so, and
    // without this row the EXISTS is false and every assertion below would pass
    // for the wrong reason.
    await dataSource.getRepository(ParkDowntimeCoverage).save(
      dataSource.getRepository(ParkDowntimeCoverage).create({
        parkId: park.id,
        regime: "never_reports",
        generatedAt: OPENS,
      }),
    );
  };

  /**
   * One STANDBY reading. `is_heartbeat` and `data_source` are set explicitly
   * because `observedReadingsSql()` reads both: a row our own bookkeeping wrote,
   * or carried, is not an observation, and a fixture that left them to defaults
   * would be asserting against whatever those defaults happen to be.
   */
  const reading = async (at: Date, status: string): Promise<void> => {
    await dataSource.query(
      `INSERT INTO queue_data (id, "attractionId", "queueType", status, timestamp,
                               is_heartbeat, data_source, "lastUpdated")
       VALUES (gen_random_uuid(), $1, 'STANDBY', $2, $3, false, 'themeparks-wiki', $4)`,
      [rideId, status, at, new Date(at.getTime() - 60_000)],
    );
  };

  // The two columns the statement emits, and the two `addClosureGaps` reads.
  const run = (
    asOf: Date,
  ): Promise<Array<{ attractionId: string; startedAt: Date }>> =>
    dataSource.query(CURRENT_CLOSURE_GAP_SQL, [[rideId], TZ, asOf, parkId]);

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

  it("keeps a ride that broke after midnight on the line", async () => {
    await seedPark(CLOSES);
    // Open at 23:30, stopped at 00:10, still standing at 00:45. Those three
    // instants span two calendar dates and one operating day, which is the
    // whole case.
    await reading(new Date(`${DAY}T23:30:00+02:00`), "OPERATING");
    await reading(new Date(`${NEXT}T00:10:00+02:00`), "CLOSED");

    const rows = await run(new Date(`${NEXT}T00:45:00+02:00`));

    expect(rows).toHaveLength(1);
    expect(rows[0].attractionId).toBe(rideId);
    // The line starts when the ride stopped, not when the calendar rolled over.
    expect(new Date(rows[0].startedAt).toISOString()).toBe(
      new Date(`${NEXT}T00:10:00+02:00`).toISOString(),
    );
  });

  it("the same fixture straddles two calendar dates, which is what used to drop it", async () => {
    // The counter-check, and it is not decoration: without it the test above
    // passes just as well against a fixture whose two readings share a calendar
    // date, where the old comparison would have kept the row too. This asserts
    // the fixture really is the case that used to be excluded.
    await seedPark(CLOSES);
    const [check] = await dataSource.query(
      `SELECT (($1::timestamptz AT TIME ZONE $3)::date
             = ($2::timestamptz AT TIME ZONE $3)::date) AS same_calendar_day`,
      [
        new Date(`${DAY}T23:30:00+02:00`),
        new Date(`${NEXT}T00:10:00+02:00`),
        TZ,
      ],
    );
    expect(check.same_calendar_day).toBe(false);
  });

  it("still refuses a ride whose last OPERATING was a different operating day", async () => {
    // The gate has to keep biting, or the fix has simply switched it off. The
    // ride was open late on the 14th and has read CLOSED ever since: a seasonal
    // or all-day closure, which is exactly what this filter exists to exclude.
    //
    // 23:30 rather than 20:00, and the hour is the whole test. `recent` looks
    // back LIVE_LOOKBACK_HOURS = 26 h from the as-of, so a reading at 20:00 on
    // the 14th is 28¾ h old and never enters the statement — open_today would
    // then be empty because there is no OPERATING row at all, and the case
    // would pass with the day comparison replaced by TRUE. Verified by that
    // mutation: at 20:00 all five cases stay green, at 23:30 this one fails.
    await seedPark(CLOSES);
    await reading(new Date("2026-06-14T23:30:00+02:00"), "OPERATING");
    await reading(new Date(`${DAY}T23:00:00+02:00`), "CLOSED");

    const rows = await run(new Date(`${NEXT}T00:45:00+02:00`));

    expect(rows).toHaveLength(0);
  });

  it("leaves an ordinary park that closes before midnight unchanged", async () => {
    // The population this change must not move, and the reason the join to win
    // is LEFT: a park closing at 20:00 has its evening readings inside the
    // window and its after-hours ones outside it, and both keep the day they
    // had. queue_data is a change log, so a ride reads OPERATING for hours
    // after the park shuts — an INNER join would drop exactly those.
    await seedPark(new Date(`${DAY}T20:00:00+02:00`));
    await reading(new Date(`${DAY}T14:00:00+02:00`), "OPERATING");
    await reading(new Date(`${DAY}T15:00:00+02:00`), "CLOSED");

    const rows = await run(new Date(`${DAY}T15:30:00+02:00`));

    expect(rows).toHaveLength(1);
    expect(rows[0].attractionId).toBe(rideId);
  });

  it("says nothing once the park has shut, however recent the closure", async () => {
    // A CLOSED ride in a shut park is a shut park. park_open is read out of win
    // now, so this is also the assertion that the window's END survived the
    // move — a win row whose closes_at were wrong would keep the park open all
    // night and this case would return a row.
    await seedPark(CLOSES);
    await reading(new Date(`${DAY}T23:30:00+02:00`), "OPERATING");
    await reading(new Date(`${NEXT}T00:10:00+02:00`), "CLOSED");

    const rows = await run(new Date(`${NEXT}T02:30:00+02:00`));

    expect(rows).toHaveLength(0);
  });
});
