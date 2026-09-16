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
import { OUTAGE_INTERVALS_SQL } from "../../src/analytics/utils/outage-reconstruction.sql";

/**
 * The curated works-period filter against a park that closes after midnight.
 *
 * The unit spec next to the SQL asserts its SHAPE — that the filter reads
 * `start_op_day` and not a `::date` cast of `started_at`. Only a real PostgreSQL
 * says whether the statement then drops the row, and this file exists because
 * the shape assertions cannot: a CTE column nobody selects, a `date` compared
 * against a `timestamptz`, a correlated subquery that resolves to NULL all pass
 * a regular expression and fail a query.
 *
 * The fixture is the case the change is about, at the smallest size that still
 * reaches the filter:
 *
 * - one park with a `wiki_entity_id`, because `parkOpenWindowCtes({wikiOnly})`
 *   drops every park without one and the statement would return nothing,
 * - one published `OPERATING` window running 10:00 → 02:00 the next park-local
 *   day — La Ronde's shape, and a genuine wrap rather than the 00:00 close the
 *   production rows happen to hold,
 * - one ride, DOWN from 00:10 and running again at 01:20, so its interval sits
 *   entirely on the far side of midnight: calendar date the 16th, operating day
 *   the 15th.
 *
 * Production cannot supply this case at all. Measured 2026-09-16: **zero**
 * attractions carry `curated_out_of_service_from`/`_to`, so every row of the
 * predicate's opening `IS NOT NULL` disjunction is false and the two day notions
 * cannot be told apart there. That is why the proof is a container and not a
 * query against the live database.
 */
describe("curated works window across park-local midnight (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;

  const TZ = "Europe/Berlin";
  // June, far from either DST boundary: a fixture that also crossed a shift
  // would be testing two things and telling you which one failed by coin toss.
  const DAY = "2026-06-15";
  const NEXT = "2026-06-16";
  // 10:00 → 02:00 the following park-local day. +02:00 is Berlin's summer offset.
  const OPENS = new Date(`${DAY}T10:00:00+02:00`);
  const CLOSES = new Date(`${NEXT}T02:00:00+02:00`);

  // The statement's four bound parameters: park filter, scan start, window end,
  // as-of. The as-of sits after the ride recovered, so nothing is `ongoing`.
  const SCAN_START = new Date(`${DAY}T00:00:00+02:00`);
  const WIN_END = new Date("2026-06-17T00:00:00+02:00");
  const AS_OF = new Date(`${NEXT}T03:00:00+02:00`);

  let parkId: string;
  let rideId: string;

  interface IntervalRow {
    attractionId: string;
    startedAt: Date;
    startOpDay: Date | string;
    endReason: string;
    operatingMinutes: number;
  }

  /**
   * `startOpDay` as `YYYY-MM-DD`.
   *
   * `pg` parses a `date` column into a JS Date at the RUNNING PROCESS's local
   * midnight, so the raw value stringifies to a locale sentence. Read back
   * through the calendar fields rather than `toISOString()`: east of UTC local
   * midnight is the previous day in UTC, so the ISO form would report the 14th
   * for a column holding the 15th and this fixture would assert the bug.
   */
  const asDay = (value: Date | string): string => {
    if (!(value instanceof Date)) return String(value);
    const pad = (n: number) => String(n).padStart(2, "0");
    return `${value.getFullYear()}-${pad(value.getMonth() + 1)}-${pad(value.getDate())}`;
  };

  const seedPark = async (closesAt: Date): Promise<void> => {
    const park = await dataSource.getRepository(Park).save(
      dataSource.getRepository(Park).create({
        externalId: "e2e-works-park",
        name: "Works Park",
        slug: "works-park",
        latitude: 45.5,
        longitude: -73.5,
        timezone: TZ,
        continent: "Europe",
        continentSlug: "europe",
        country: "Germany",
        countrySlug: "germany",
        countryCode: "DE",
        city: "Brühl",
        citySlug: "bruehl",
        // Without this the shared CTEs never see the park and every assertion
        // below would be about an empty result set.
        wikiEntityId: "e2e-works-park-wiki",
      }),
    );
    parkId = park.id;

    const ride = await dataSource.getRepository(Attraction).save(
      dataSource.getRepository(Attraction).create({
        externalId: "e2e-works-ride",
        name: "Night Coaster",
        slug: "night-coaster",
        parkId: park.id,
        latitude: 45.5,
        longitude: -73.5,
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
  };

  /** A second published day for the same park, opening hours as given. */
  const addDay = async (
    day: string,
    opensAt: Date,
    closesAt: Date,
  ): Promise<void> => {
    await dataSource.getRepository(ScheduleEntry).save(
      dataSource.getRepository(ScheduleEntry).create({
        parkId,
        attractionId: null,
        date: day as unknown as Date,
        scheduleType: ScheduleType.OPERATING,
        openingTime: opensAt,
        closingTime: closesAt,
      }),
    );
  };

  /** The hand-written works period, as an editor would leave it. */
  const curate = async (from: string | null, to: string | null) => {
    await dataSource.query(
      `UPDATE attractions
          SET curated_out_of_service_from = $2::date,
              curated_out_of_service_to   = $3::date
        WHERE id = $1`,
      [rideId, from, to],
    );
  };

  /**
   * One STANDBY reading. `is_heartbeat` and `data_source` are set explicitly
   * because `outageRunBreaks()` reads both: a row our own bookkeeping wrote, or
   * carried, ends the run rather than extending it, and a fixture that left them
   * to defaults would be asserting against whatever those defaults happen to be.
   */
  const reading = async (at: Date, status: string): Promise<void> => {
    await dataSource.query(
      `INSERT INTO queue_data (id, "attractionId", "queueType", status, timestamp,
                               is_heartbeat, data_source, "lastUpdated")
       VALUES (gen_random_uuid(), $1, 'STANDBY', $2, $3, false, 'themeparks-wiki', $4)`,
      [rideId, status, at, new Date(at.getTime() - 60_000)],
    );
  };

  /** The outage that starts after midnight and belongs to the evening before. */
  const seedPastMidnightOutage = async (): Promise<void> => {
    await reading(new Date(`${NEXT}T00:10:00+02:00`), "DOWN");
    await reading(new Date(`${NEXT}T00:40:00+02:00`), "DOWN");
    await reading(new Date(`${NEXT}T01:20:00+02:00`), "OPERATING");
  };

  const run = (): Promise<IntervalRow[]> =>
    dataSource.query(OUTAGE_INTERVALS_SQL, [
      [parkId],
      SCAN_START,
      WIN_END,
      AS_OF,
    ]);

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

  it("files a past-midnight outage under the operating day it belongs to", async () => {
    // The anchor case, and the one that proves the fixture is the diverging
    // one: with no works window at all the interval comes back, its start is on
    // the 16th and its operating day is the 15th. Every assertion below rests
    // on those two being different, and without this they could quietly agree.
    await seedPark(CLOSES);
    await seedPastMidnightOutage();

    const rows = await run();

    expect(rows).toHaveLength(1);
    expect(rows[0].attractionId).toBe(rideId);
    expect(new Date(rows[0].startedAt).toISOString()).toBe(
      new Date(`${NEXT}T00:10:00+02:00`).toISOString(),
    );
    expect(asDay(rows[0].startOpDay)).toBe(DAY);
    // 00:10 → 01:20 is 70 minutes, all of it inside the window. Above
    // MIN_OUTAGE_OPERATING_MINUTES, so the row is not dropped for being noise —
    // which would make the exclusion cases below pass for the wrong reason.
    expect(rows[0].operatingMinutes).toBe(70);
    expect(rows[0].endReason).toBe("recovered");
  });

  it("excludes it when the works window covers that operating day", async () => {
    // The case the whole change is about. The editor declared the 15th; the
    // outage started at 00:10 on the 16th and belongs to the 15th's evening.
    // Against the calendar date it fell one day past the window's end and was
    // stored as a real breakdown.
    await seedPark(CLOSES);
    await seedPastMidnightOutage();
    await curate(DAY, DAY);

    await expect(run()).resolves.toHaveLength(0);
  });

  it("still returns it when the window covers only the calendar date", async () => {
    // The counter-check, and it is not decoration: without it the case above
    // passes just as well against a filter that was switched off rather than
    // moved. A window on the 16th alone must NOT exclude an outage whose
    // operating day is the 15th — that is the same statement read from the
    // other side.
    await seedPark(CLOSES);
    await seedPastMidnightOutage();
    await curate(NEXT, NEXT);

    await expect(run()).resolves.toHaveLength(1);
  });

  it("keeps biting in an ordinary park that closes before midnight", async () => {
    // The population this change must not move. A park shutting at 20:00 whose
    // outage starts and ends inside one day has one operating day per calendar
    // date, so both readings of the predicate agree — and the filter has to
    // keep excluding, or the fix has simply deleted it.
    await seedPark(new Date(`${DAY}T20:00:00+02:00`));
    await reading(new Date(`${DAY}T14:00:00+02:00`), "DOWN");
    await reading(new Date(`${DAY}T15:00:00+02:00`), "OPERATING");
    await curate(DAY, DAY);

    await expect(run()).resolves.toHaveLength(0);
  });

  it("leaves an ordinary park's outage alone when no window is declared", async () => {
    // And the same park without the declaration, so the case above is known to
    // be excluded BY the window rather than by the park's shape.
    await seedPark(new Date(`${DAY}T20:00:00+02:00`));
    await reading(new Date(`${DAY}T14:00:00+02:00`), "DOWN");
    await reading(new Date(`${DAY}T15:00:00+02:00`), "OPERATING");

    const rows = await run();

    expect(rows).toHaveLength(1);
    expect(asDay(rows[0].startOpDay)).toBe(DAY);
  });

  /**
   * A ride that breaks after closing time and is still down the next morning,
   * in a park that shuts at 20:00. No midnight wrap anywhere — and the two day
   * notions still disagree, which is the class the ticket's wording misses.
   *
   * `startOpDay` takes the LOWEST window that overlaps the interval, and the
   * evening one does not: its `closes_at` is 20:00 and the interval starts at
   * 22:00. So the row is filed under the 16th while the calendar date of
   * `started_at` is the 15th. Both readings reach the predicate, because the
   * next morning's segments put the interval well over
   * MIN_OUTAGE_OPERATING_MINUTES.
   */
  const seedAfterHoursOutage = async (): Promise<void> => {
    await seedPark(new Date(`${DAY}T20:00:00+02:00`));
    await addDay(
      NEXT,
      new Date(`${NEXT}T10:00:00+02:00`),
      new Date(`${NEXT}T20:00:00+02:00`),
    );
    await reading(new Date(`${DAY}T22:00:00+02:00`), "DOWN");
    await reading(new Date(`${NEXT}T10:30:00+02:00`), "DOWN");
    await reading(new Date(`${NEXT}T11:30:00+02:00`), "OPERATING");
  };

  it("files an after-hours outage under the morning it was next seen open for", async () => {
    // The anchor for that class, and the proof it really is one: no wrap, and
    // the operating day is still a day later than the calendar date.
    await seedAfterHoursOutage();

    const rows = await run();

    expect(rows).toHaveLength(1);
    expect(new Date(rows[0].startedAt).toISOString()).toBe(
      new Date(`${DAY}T22:00:00+02:00`).toISOString(),
    );
    expect(asDay(rows[0].startOpDay)).toBe(NEXT);
  });

  it("excludes an after-hours outage under a window on its operating day", async () => {
    await seedAfterHoursOutage();
    await curate(NEXT, NEXT);

    await expect(run()).resolves.toHaveLength(0);
  });

  it("still returns it when the window covers only the evening it broke on", async () => {
    // The mirror, and the one that used to exclude. Under the calendar reading
    // a window on the 15th covered this interval; under the operating day it
    // does not, because the ride was next open for on the 16th.
    await seedAfterHoursOutage();
    await curate(DAY, DAY);

    await expect(run()).resolves.toHaveLength(1);
  });

  it("reads a half-open window on the operating day too", async () => {
    // `to` with no `from` is a works period that was already running when
    // somebody wrote it down. Two things at once, and both are needed:
    //
    // - the NULL arm. A comparison against a NULL bound yields NULL, and a NULL
    //   passes no filter, so a slip there switches the exclusion off silently.
    // - the day again, from the other side. The window ends on the 15th and the
    //   outage's calendar date is the 16th, so the old predicate let it escape
    //   past the closing bound — the ticket's failure mode, mirrored.
    //
    // The `from`-only shape would NOT discriminate: the 16th is `>= the 15th`
    // under either reading, so that fixture stays excluded even unfixed.
    await seedPark(CLOSES);
    await seedPastMidnightOutage();
    await curate(null, DAY);

    await expect(run()).resolves.toHaveLength(0);
  });
});
