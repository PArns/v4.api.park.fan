import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import { Park } from "../../src/parks/entities/park.entity";
import { Attraction } from "../../src/attractions/entities/attraction.entity";
import { ScheduleEntry } from "../../src/parks/entities/schedule-entry.entity";
import { ScheduleType } from "../../src/parks/entities/schedule-entry.entity";
import { ParkDowntimeCoverage } from "../../src/analytics/entities/park-downtime-coverage.entity";
import { AttractionExposureDay } from "../../src/analytics/entities/attraction-exposure-day.entity";
import {
  CURRENT_CLOSURE_GAP_SQL,
  MAX_GAP_DAY_SHARE,
  MIN_DAYS_FOR_CYCLE_TEST,
} from "../../src/common/utils/closure-gap.sql";

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

  // ---------------------------------------------------------------------------
  // The denominator fixture (PAR-252).
  //
  // The five cases above leave `attraction_exposure_days` empty on purpose, so
  // `active_days` is 0, the `MIN_DAYS_FOR_CYCLE_TEST` arm passes unconditionally
  // and nothing below the day keying is exercised. That is what let the
  // denominator's own bounds go unguarded: `LEAST` -> `GREATEST` in
  // `active_floor`, a dropped `FILTER` there, or `<` -> `<=` on the upper bound
  // all leave every case in this file green, because each of them can only
  // SHRINK the denominator to 0 or grow it, and both directions end in a row
  // being emitted rather than an error.
  //
  // The three cases at the end seed a real denominator instead. They share one
  // shape: `gap_days` is fixed at GAP_DAYS and `active_days` lands on either
  // side of `MAX_GAP_DAY_SHARE` depending on a SINGLE operating day at one of
  // the two bounds. Both counts stay at or above `MIN_DAYS_FOR_CYCLE_TEST`, so
  // the duty-cycle ratio is what decides the row and not the floor in front of
  // it.
  // ---------------------------------------------------------------------------

  /** The as-of every denominator case is judged at: 45 minutes past midnight. */
  const AS_OF = new Date(`${NEXT}T00:45:00+02:00`);

  /**
   * The lowest operating day `active_floor` reaches as written.
   *
   * `$3 - CYCLE_WINDOW_DAYS` is 2026-05-17 00:45 park-local, so the calendar
   * candidate is 2026-05-17 and the filtered `MIN(op_day)` is 2026-05-16 — the
   * window that opened on the 16th at 10:00 is still running at that instant.
   * `LEAST` takes the 16th. `GREATEST` would take the 17th and lose the day.
   */
  const FLOOR_DAY = "2026-05-16";

  /**
   * One operating day below it, and it is what the `FILTER` excludes.
   *
   * Its window (10:00 on the 15th to 02:00 on the 16th) shut 22 3/4 hours
   * before the reading window starts, so `MIN(op_day) FILTER (closes_at > ...)`
   * does not see it while a bare `MIN(op_day)` does.
   */
  const BELOW_FLOOR_DAY = "2026-05-15";

  /** Five ordinary operating days inside the window, well clear of both bounds. */
  const MIDDLE_DAYS = [
    "2026-06-09",
    "2026-06-10",
    "2026-06-11",
    "2026-06-12",
    "2026-06-13",
  ];

  /**
   * The last operating day before today, and the day the upper bound sits on.
   *
   * It carries no readings and no published window, so it moves `active_days`
   * and nothing else.
   */
  const YESTERDAY = "2026-06-14";

  /** The numerator: three of those days carry a real gap triple. */
  const GAP_DAYS = 3;

  /**
   * The arithmetic the three cases below rest on, as assertions rather than as
   * a comment: re-calibrating either constant has to break this line, not turn
   * the cases into three green statements about nothing.
   */
  const expectTheRatioToBeWhatDecides = (): void => {
    expect(MIDDLE_DAYS.length).toBeGreaterThanOrEqual(MIN_DAYS_FOR_CYCLE_TEST);
    expect(GAP_DAYS / (MIDDLE_DAYS.length + 1)).toBeLessThanOrEqual(
      MAX_GAP_DAY_SHARE,
    );
    expect(GAP_DAYS / MIDDLE_DAYS.length).toBeGreaterThan(MAX_GAP_DAY_SHARE);
  };

  const plusDays = (day: string, n: number): string => {
    const d = new Date(`${day}T12:00:00Z`);
    d.setUTCDate(d.getUTCDate() + n);
    return d.toISOString().slice(0, 10);
  };

  /**
   * An instant on a park-local date. Every date in this fixture is in May or
   * June 2026, so Berlin is on +02:00 throughout and no case straddles a DST
   * shift — the same reason the cases above sit in June.
   */
  const at = (day: string, clock: string): Date =>
    new Date(`${day}T${clock}+02:00`);

  /** One more published window, 10:00 to 02:00 the next park-local day. */
  const seedWindow = async (opDay: string): Promise<void> => {
    await dataSource.getRepository(ScheduleEntry).save(
      dataSource.getRepository(ScheduleEntry).create({
        parkId,
        attractionId: null,
        date: opDay as unknown as Date,
        scheduleType: ScheduleType.OPERATING,
        openingTime: at(opDay, "10:00:00"),
        closingTime: at(plusDays(opDay, 1), "02:00:00"),
      }),
    );
  };

  /**
   * One day in the denominator: the ride was at risk, so `active` counts it.
   *
   * `operating_minutes` has to be positive — `active` counts under a FILTER on
   * exactly that column, and a row of zeroes is a day the ride was never open.
   */
  const seedExposureDay = async (
    opDay: string,
    operatingMinutes = 900,
  ): Promise<void> => {
    await dataSource.getRepository(AttractionExposureDay).save(
      dataSource.getRepository(AttractionExposureDay).create({
        attractionId: rideId,
        parkId,
        opDay,
        parkOpenMinutes: 960,
        operatingMinutes,
        computedAt: at(plusDays(opDay, 1), "03:00:00"),
      }),
    );
  };

  /**
   * One day in the numerator: OPERATING, CLOSED, OPERATING again, all inside
   * the same window. That is the triple `cycle` recognises, and it is the same
   * one the nightly statement counts.
   */
  const seedGapDay = async (opDay: string): Promise<void> => {
    await reading(at(opDay, "14:00:00"), "OPERATING");
    await reading(at(opDay, "14:30:00"), "CLOSED");
    await reading(at(opDay, "15:00:00"), "OPERATING");
  };

  /**
   * Today's standing closure: the candidate row every denominator case judges.
   *
   * Identical to the first case in this file — open at 23:30, stopped at 00:10,
   * still standing at the 00:45 as-of — so the only thing that varies between
   * the cases below is the denominator.
   */
  const seedStandingClosure = async (): Promise<void> => {
    await reading(at(DAY, "23:30:00"), "OPERATING");
    await reading(at(NEXT, "00:10:00"), "CLOSED");
  };

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

  it("counts the operating day at the floor, so LEAST rather than GREATEST decides the denominator", async () => {
    // `active_floor` takes the LOWER of two candidates: the calendar date of
    // `$3 - CYCLE_WINDOW_DAYS` (2026-05-17) and the earliest operating day
    // whose window was still running at that instant (2026-05-16). In a park
    // that closes after midnight those differ by exactly one day, and that day
    // is a day the numerator can reach — a gap read just after the window's
    // start carries the PREVIOUS operating day. Take the higher candidate and
    // the denominator loses it while the numerator keeps it, which pushes
    // gap_days/active_days up and suppresses a genuine fault as a duty cycle.
    //
    // 3/6 = 0.5 is at MAX_GAP_DAY_SHARE and the line survives; 3/5 = 0.6 is
    // over it and the line is gone. The mutation therefore shows up as a
    // MISSING row and never as an error, which is why no existing case sees it.
    await seedPark(CLOSES);
    for (const day of [FLOOR_DAY, ...MIDDLE_DAYS]) {
      await seedWindow(day);
      await seedExposureDay(day);
    }
    for (const day of MIDDLE_DAYS.slice(-GAP_DAYS)) {
      await seedGapDay(day);
    }
    await seedStandingClosure();

    expectTheRatioToBeWhatDecides();

    const rows = await run(AS_OF);

    expect(rows).toHaveLength(1);
    expect(rows[0].attractionId).toBe(rideId);

    // And the counter-check that names the day: drop the exposure row at the
    // floor — the one and only row `GREATEST` would exclude — and the same
    // fixture loses its line. Without this the case above would also pass
    // against a fixture whose denominator never reached the floor at all.
    await dataSource
      .getRepository(AttractionExposureDay)
      .delete({ attractionId: rideId, opDay: FLOOR_DAY });

    expect(await run(AS_OF)).toHaveLength(0);
  });

  it("keeps a window that was already over out of the floor, which is what the FILTER does", async () => {
    // The second candidate is `MIN(op_day) FILTER (closes_at > $3 - 30 days)`,
    // and the FILTER is the whole of it. The park published hours on
    // 2026-05-15 as well, but that window shut at 02:00 on the 16th — more
    // than 22 hours before the reading window starts — so no gap inside it can
    // ever enter the numerator. A bare `MIN(op_day)` would drag the floor down
    // to it anyway and hand the denominator a day the numerator cannot reach,
    // which dilutes the share downward: a timetable then reads as a fault.
    //
    // Five denominator days against three gap days is 0.6, over
    // MAX_GAP_DAY_SHARE, so the ride is a duty cycle and stays off the line.
    await seedPark(CLOSES);
    for (const day of [BELOW_FLOOR_DAY, FLOOR_DAY, ...MIDDLE_DAYS]) {
      await seedWindow(day);
    }
    for (const day of [BELOW_FLOOR_DAY, ...MIDDLE_DAYS]) {
      await seedExposureDay(day);
    }
    for (const day of MIDDLE_DAYS.slice(-GAP_DAYS)) {
      await seedGapDay(day);
    }
    await seedStandingClosure();

    expectTheRatioToBeWhatDecides();

    expect(await run(AS_OF)).toHaveLength(0);

    // The counter-check, and it is what makes the empty result mean something:
    // the fixture IS a candidate, and one denominator day at the floor is all
    // that stands between 0.6 and 0.5. Seeded at FLOOR_DAY rather than at
    // BELOW_FLOOR_DAY, so this also fails if the floor moves UP a day.
    await seedExposureDay(FLOOR_DAY);

    const rows = await run(AS_OF);

    expect(rows).toHaveLength(1);
    expect(rows[0].attractionId).toBe(rideId);
  });

  it("leaves the operating day in progress out of the denominator", async () => {
    // The upper bound is `e.op_day < park_open.op_day`, and it has the same
    // silent direction as the floor. `cycle` stops one day below today, so a
    // denominator that counted today would be summing over a wider span than
    // its own numerator — the share falls, and a ride on a timetable is
    // published as broken.
    //
    // Today's exposure row carries positive operating minutes here on purpose.
    // Production writes it overnight with `operating_minutes = 0`, so the
    // FILTER skips it and the bound looks unnecessary; measured on 2026-09-15
    // it is not, because 1083 rows of the operating day in progress do carry
    // minutes by the time this statement runs. `<=` would count every one of
    // them.
    await seedPark(CLOSES);
    for (const day of MIDDLE_DAYS) {
      await seedWindow(day);
      await seedExposureDay(day);
    }
    await seedExposureDay(DAY);
    for (const day of MIDDLE_DAYS.slice(-GAP_DAYS)) {
      await seedGapDay(day);
    }
    await seedStandingClosure();

    expectTheRatioToBeWhatDecides();

    expect(await run(AS_OF)).toHaveLength(0);

    // The counter-check, and it is the day IMMEDIATELY below today rather than
    // an arbitrary one, which pins the bound from both sides: an exposure row
    // on 2026-06-14 counts, so a bound that lost a further day would fail here
    // just as an inclusive one fails above. A control day further down leaves
    // that direction — the one that shrinks the denominator and suppresses a
    // real fault — bounded only by the regular expression in
    // `closure-gap.sql.spec.ts`. No window is published for it: `active` reads
    // `attraction_exposure_days`, not the schedule, and the floor here is the
    // calendar candidate 2026-05-17 in any case.
    await seedExposureDay(YESTERDAY);

    const rows = await run(AS_OF);

    expect(rows).toHaveLength(1);
    expect(rows[0].attractionId).toBe(rideId);
  });
});
