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
import { ParksService } from "../../src/parks/parks.service";

/**
 * `saveScheduleData`'s retraction of days the source stopped naming, against a
 * real PostgreSQL — because the statement that does it is raw SQL over
 * `to_char(date, …)` comparisons, and the unit spec beside it can only read that
 * text (📚 G-84).
 *
 * The case is Rulantica's, in shape: a block of consecutive days in the middle
 * of an operating stretch that the upstream stopped naming. In production those
 * 14 rows (2026-11-16 to 27, plus 24/25 December) stood as OPERATING from
 * 2026-02-11 to 2026-09-28 while the upstream named none of them, and the
 * calendar, the day planner and best-days offered all 14 as visit days
 * (PAR-538).
 *
 * **Every absence here is asserted as a pair.** A DELETE whose WHERE clause
 * matches nothing leaves the same rows standing as one that was never issued, so
 * "the row survived" on its own is green against a fix that does nothing at all
 * (📚 G-44). Each such test seeds two parks that differ in exactly one thing and
 * reads both out of the same pair of calls.
 *
 * Dates are offsets from today, never written down: the whole rule is "future vs
 * past" and a fixed date walks across that boundary on its own (📚 G-56). The
 * parks sit in UTC so no offset can move a day across it either.
 */
describe("schedule retraction (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let parks: ParksService;

  const dayOffset = (days: number): string =>
    new Date(Date.now() + days * 86_400_000).toISOString().slice(0, 10);

  /** The park's operating stretch, and the block in its middle the source drops. */
  const BEFORE_GAP = [40, 41, 42, 43, 44].map(dayOffset);
  const WITHDRAWN = [45, 46, 47].map(dayOffset);
  const AFTER_GAP = [48, 49, 50].map(dayOffset);
  const PAST_DAY = dayOffset(-40);

  const STORED = [...BEFORE_GAP, ...WITHDRAWN, ...AFTER_GAP];
  /** What the source still names: the same stretch minus the withdrawn block. */
  const STILL_NAMED = [...BEFORE_GAP, ...AFTER_GAP];

  /**
   * The months the fetch answered for, derived from the fixture's own dates
   * rather than listed, so a run that straddles a month boundary covers both
   * halves of the stretch.
   */
  const monthsOf = (dates: string[]): string[] => [
    ...new Set(dates.map((d) => d.slice(0, 7))),
  ];

  const payloadFor = (dates: string[]) =>
    dates.map((date) => ({
      date,
      type: "OPERATING",
      openingTime: `${date}T08:30:00Z`,
      closingTime: `${date}T21:00:00Z`,
    }));

  /**
   * A park with one ride, one fresh reading and the given park-level OPERATING
   * days.
   *
   * The reading is not decoration: without it the park's feed reads as silent and
   * the guard in `saveScheduleData` downgrades every future operating day in the
   * payload to UNKNOWN (PAR-299), which would make this file assert the wrong
   * mechanism. `data_source` and `is_heartbeat` are written out because
   * `observedReadingsSql` reads both.
   */
  const seedPark = async (
    slug: string,
    operatingDays: string[],
  ): Promise<{ parkId: string; rideId: string }> => {
    const park = await dataSource.getRepository(Park).save(
      dataSource.getRepository(Park).create({
        externalId: `e2e-retract-${slug}`,
        name: `Retract ${slug}`,
        slug,
        latitude: 48.27,
        longitude: 7.72,
        timezone: "UTC",
        continent: "Europe",
        continentSlug: "europe",
        country: "Germany",
        countrySlug: "germany",
        countryCode: "DE",
        city: "Rust",
        citySlug: `rust-${slug}`,
      }),
    );

    const ride = await dataSource.getRepository(Attraction).save(
      dataSource.getRepository(Attraction).create({
        externalId: `e2e-retract-${slug}-ride`,
        name: `Ride at ${slug}`,
        slug: `${slug}-ride`,
        parkId: park.id,
        latitude: 48.27,
        longitude: 7.72,
      }),
    );

    await dataSource.query(
      `INSERT INTO queue_data (id, "attractionId", "queueType", status, timestamp,
                               is_heartbeat, data_source, "lastUpdated")
       VALUES (gen_random_uuid(), $1, 'STANDBY', 'OPERATING', NOW() - INTERVAL '1 hour',
               false, 'queue-times', NOW() - INTERVAL '2 hours')`,
      [ride.id],
    );

    for (const date of operatingDays) {
      await insertScheduleRow(park.id, date, ScheduleType.OPERATING, {
        openingTime: `${date}T08:30:00Z`,
        closingTime: `${date}T21:00:00Z`,
      });
    }

    return { parkId: park.id, rideId: ride.id };
  };

  const insertScheduleRow = async (
    parkId: string,
    date: string,
    scheduleType: ScheduleType,
    opts: {
      openingTime?: string;
      closingTime?: string;
      description?: string;
      attractionId?: string;
    } = {},
  ): Promise<void> => {
    await dataSource.getRepository(ScheduleEntry).save(
      dataSource.getRepository(ScheduleEntry).create({
        parkId,
        attractionId: opts.attractionId ?? null,
        date: date as unknown as Date,
        scheduleType,
        openingTime: opts.openingTime ? new Date(opts.openingTime) : null,
        closingTime: opts.closingTime ? new Date(opts.closingTime) : null,
        description: opts.description ?? null,
      }),
    );
  };

  /** Park-level rows for `parkId`, keyed `YYYY-MM-DD` → type. */
  const parkLevelRows = async (
    parkId: string,
    dates: string[],
  ): Promise<
    Map<string, { type: ScheduleType; description: string | null }>
  > => {
    const rows: Array<{
      date: string;
      scheduleType: ScheduleType;
      description: string | null;
    }> = await dataSource.query(
      `SELECT to_char(date, 'YYYY-MM-DD') AS date, "scheduleType", description
         FROM schedule_entries
        WHERE "parkId" = $1 AND "attractionId" IS NULL
          AND to_char(date, 'YYYY-MM-DD') = ANY($2::text[])`,
      [parkId, dates],
    );
    return new Map(
      rows.map((r) => [
        r.date,
        { type: r.scheduleType, description: r.description },
      ]),
    );
  };

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    app = moduleFixture.createNestApplication();
    app.setGlobalPrefix("v1");
    await app.init();
    dataSource = app.get(DataSource);
    parks = app.get(ParksService);
  });

  afterAll(async () => {
    await app.close();
  });

  it("drops the withdrawn days inside an answered month and keeps them in an unanswered one", async () => {
    // The pair the whole change hangs on. Both parks hold the same eleven
    // operating days and are handed the same payload of eight; the only
    // difference is whether the fetch reported the month as answered.
    const answered = await seedPark("answered", STORED);
    const unanswered = await seedPark("unanswered", STORED);

    await parks.saveScheduleData(
      answered.parkId,
      payloadFor(STILL_NAMED),
      monthsOf(STORED),
    );
    await parks.saveScheduleData(
      unanswered.parkId,
      payloadFor(STILL_NAMED),
      [],
    );

    const answeredRows = await parkLevelRows(answered.parkId, STORED);
    const unansweredRows = await parkLevelRows(unanswered.parkId, STORED);

    for (const date of WITHDRAWN) {
      expect(answeredRows.has(date)).toBe(false);
      expect(unansweredRows.get(date)?.type).toBe(ScheduleType.OPERATING);
    }
    // The days the source still names are untouched on both sides, so the DELETE
    // is about the withdrawal and not about the month.
    for (const date of STILL_NAMED) {
      expect(answeredRows.get(date)?.type).toBe(ScheduleType.OPERATING);
      expect(unansweredRows.get(date)?.type).toBe(ScheduleType.OPERATING);
    }
  });

  it("leaves the end state to gap-fill, which closes a day between two operating ones", async () => {
    // The retraction deletes and stops. What a visitor sees is written by the
    // `fillScheduleGaps` pass both callers run immediately afterwards: a day
    // inside the operating stretch becomes CLOSED, which is what Rulantica's
    // maintenance closure is.
    const park = await seedPark("gap-filled", STORED);

    await parks.saveScheduleData(
      park.parkId,
      payloadFor(STILL_NAMED),
      monthsOf(STORED),
    );
    await parks.fillScheduleGaps(park.parkId);

    const rows = await parkLevelRows(park.parkId, STORED);
    for (const date of WITHDRAWN) {
      expect(rows.get(date)?.type).toBe(ScheduleType.CLOSED);
      expect(rows.get(date)?.description).toBe("Gap-filled");
    }
    for (const date of STILL_NAMED) {
      expect(rows.get(date)?.type).toBe(ScheduleType.OPERATING);
    }
  });

  it("does not retract a hand-written CLOSED row for a day the source stopped naming", async () => {
    // Phantasialand's 2027-01-12 and 13 in production: one CLOSED row per date,
    // written by hand, on dates the upstream no longer names inside a month it
    // answers for — exactly the shape the DELETE looks for, minus the type.
    //
    // The partner is a second park where the same date carries the feed's
    // OPERATING row instead. Both are handed the same payload and the same
    // months; if the type filter were missing, the curated row would go with it.
    const CURATED = "Closed per park announcement (PAR-532)";
    const curated = await seedPark("curated", STILL_NAMED);
    await insertScheduleRow(curated.parkId, WITHDRAWN[0], ScheduleType.CLOSED, {
      description: CURATED,
    });
    const fromFeed = await seedPark("from-feed", [
      ...STILL_NAMED,
      WITHDRAWN[0],
    ]);

    await parks.saveScheduleData(
      curated.parkId,
      payloadFor(STILL_NAMED),
      monthsOf(STORED),
    );
    await parks.saveScheduleData(
      fromFeed.parkId,
      payloadFor(STILL_NAMED),
      monthsOf(STORED),
    );

    const curatedRow = (await parkLevelRows(curated.parkId, WITHDRAWN)).get(
      WITHDRAWN[0],
    );
    expect(curatedRow?.type).toBe(ScheduleType.CLOSED);
    expect(curatedRow?.description).toBe(CURATED);
    // Same date, same payload, same months — the feed's row goes.
    expect(
      (await parkLevelRows(fromFeed.parkId, WITHDRAWN)).has(WITHDRAWN[0]),
    ).toBe(false);
  });

  it("does not retract a ride's own row for a withdrawn day", async () => {
    // A per-ride row is a different statement by a different writer, as
    // everywhere else in this method. The park-level row on the same date is the
    // partner: it goes in the same call the ride's row survives.
    const park = await seedPark("per-ride", STORED);
    await insertScheduleRow(park.parkId, WITHDRAWN[0], ScheduleType.OPERATING, {
      attractionId: park.rideId,
      openingTime: `${WITHDRAWN[0]}T09:00:00Z`,
      closingTime: `${WITHDRAWN[0]}T20:00:00Z`,
    });

    await parks.saveScheduleData(
      park.parkId,
      payloadFor(STILL_NAMED),
      monthsOf(STORED),
    );

    const rideRows = await dataSource.query(
      `SELECT to_char(date, 'YYYY-MM-DD') AS date FROM schedule_entries
        WHERE "parkId" = $1 AND "attractionId" = $2`,
      [park.parkId, park.rideId],
    );
    expect(rideRows).toHaveLength(1);
    expect(rideRows[0].date).toBe(WITHDRAWN[0]);
    expect(
      (await parkLevelRows(park.parkId, WITHDRAWN)).has(WITHDRAWN[0]),
    ).toBe(false);
  });

  it("does not retract a past operating day the source stopped naming", async () => {
    // The historical reconstruction in CalendarService reads past OPERATING
    // rows. The partner is a future day in the same call and the same answered
    // month, so a missing date filter shows up as the past day going too.
    const park = await seedPark("past-day", [PAST_DAY, ...STORED]);

    await parks.saveScheduleData(
      park.parkId,
      payloadFor(STILL_NAMED),
      monthsOf([PAST_DAY, ...STORED]),
    );

    const rows = await parkLevelRows(park.parkId, [PAST_DAY, ...WITHDRAWN]);
    expect(rows.get(PAST_DAY)?.type).toBe(ScheduleType.OPERATING);
    expect(rows.has(WITHDRAWN[0])).toBe(false);
  });

  it("retracts nothing when the source still names every stored day", async () => {
    // The everyday case: 154 to 190 parks are written on a normal day and none
    // of them may lose a row to this. A DELETE that fired here would take the
    // whole stretch.
    const park = await seedPark("unchanged", STORED);

    await parks.saveScheduleData(
      park.parkId,
      payloadFor(STORED),
      monthsOf(STORED),
    );

    const rows = await parkLevelRows(park.parkId, STORED);
    expect(rows.size).toBe(STORED.length);
    for (const date of STORED) {
      expect(rows.get(date)?.type).toBe(ScheduleType.OPERATING);
    }
  });
});
