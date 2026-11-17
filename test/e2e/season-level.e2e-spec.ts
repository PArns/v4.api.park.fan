import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import { Park } from "../../src/parks/entities/park.entity";
import { Attraction } from "../../src/attractions/entities/attraction.entity";
import { loadSeasonLevelInputs } from "../../src/ml/utils/season-level.query";
import { applySeasonLevel } from "../../src/ml/utils/season-level.util";
import type { HolidayRow } from "../../src/holidays/holidays.service";
import { isWeekendDate } from "../../src/common/utils/date.util";

/**
 * The season level's two reads against the real schema (PAR-1343): which
 * seasons run, and what each ride measured on their days so far.
 */
describe("season level reads the running season's measured days (e2e)", () => {
  const TZ = "Europe/Berlin";
  let app: INestApplication;
  let dataSource: DataSource;
  let parkId: string;
  let rideId: string;

  /**
   * The park as the season level reads it: no holidays unless given, and
   * school breaks known far past the season unless `schoolThrough` says when
   * they stop.
   */
  const load = (
    holidays: HolidayRow[] = [],
    schoolThrough: string | null = "2099-12-31",
  ) =>
    loadSeasonLevelInputs(
      dataSource,
      { id: parkId, timezone: TZ, countryCode: "DE", regionCode: "NRW" },
      {
        holidays: async (from, to) =>
          holidays.filter((h) => h.date >= from && h.date <= to),
        schoolThrough: async () => schoolThrough,
      },
    );

  /** A North Rhine-Westphalian school-break day. */
  const breakDay = (date: string): HolidayRow => ({
    date,
    name: "Autumn Holidays",
    localName: "Herbstferien",
    country: "DE",
    region: "DE-NW",
    holidayType: "school",
    isNationwide: false,
  });

  /** Park-local `YYYY-MM-DD`, `offset` days from today. */
  const localDay = async (offset: number): Promise<string> =>
    (
      await dataSource.query(
        `SELECT ((now() AT TIME ZONE $1)::date + $2::int)::text AS day`,
        [TZ, offset],
      )
    )[0].day;

  const season = (
    kind: string,
    status: string,
    start: string,
    end: string,
    dates: string[] | null = null,
  ) =>
    dataSource.query(
      `INSERT INTO park_seasons (park_id, kind, name, start_date, end_date, dates, status)
       VALUES ($1, $2, $2, $3, $4, $5, $6) RETURNING id`,
      [parkId, kind, start, end, dates ? JSON.stringify(dates) : null, status],
    );

  /** Four OPERATING readings at midday park time; the day's P90 is `wait`. */
  const measuredDay = async (day: string, wait: number, heartbeat = false) => {
    await dataSource.query(
      `INSERT INTO queue_data
         (id, "attractionId", "queueType", status, "waitTime", timestamp, "lastUpdated", is_heartbeat)
       SELECT gen_random_uuid(), $1, 'STANDBY', 'OPERATING', $2, t, t, $5
         FROM generate_series(0, 3) AS i,
              LATERAL (SELECT (($3::date + time '12:00') AT TIME ZONE $4)
                              + i * interval '15 minutes' AS t) x`,
      [rideId, wait, day, TZ, heartbeat],
    );
  };

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    app = moduleFixture.createNestApplication();
    await app.init();
    dataSource = app.get(DataSource);
  });

  beforeEach(async () => {
    const parks = dataSource.getRepository(Park);
    parkId = (
      await parks.save(
        parks.create({
          externalId: "e2e-season-level",
          name: "Season Level Park",
          slug: "season-level-park",
          latitude: 50.8,
          longitude: 6.9,
          timezone: TZ,
          continent: "Europe",
          continentSlug: "europe",
          country: "Germany",
          countrySlug: "germany",
          countryCode: "DE",
          city: "Brühl",
          citySlug: "bruehl-season-level",
        }),
      )
    ).id;
    const rides = dataSource.getRepository(Attraction);
    rideId = (
      await rides.save(
        rides.create({
          externalId: "e2e-season-level-ride",
          name: "Season Level Ride",
          slug: "season-level-ride",
          parkId,
          latitude: 50.8,
          longitude: 6.9,
        }),
      )
    ).id;
  });

  afterAll(async () => {
    await app.close();
  });

  it("reads only seasons that run today, are published, and bind a level", async () => {
    const [from, to] = [await localDay(-10), await localDay(30)];
    const [{ id }] = await season("winter", "confirmed", from, to);
    await season("winter", "expected", from, to); // a projection, not a fact
    await season("winter", "cancelled", from, to);
    await season("closure", "confirmed", from, to); // not a level-binding kind
    await season(
      "halloween",
      "confirmed",
      await localDay(-60),
      await localDay(-20),
    );
    await season(
      "christmas",
      "confirmed",
      await localDay(5),
      await localDay(40),
    );

    const { seasons } = await load();

    expect(seasons).toEqual([
      { id, startDate: from, endDate: to, dates: null },
    ]);
  });

  it("takes the median of the season's past days by day type", async () => {
    const [{ id }] = await season(
      "winter",
      "confirmed",
      await localDay(-9),
      await localDay(30),
    );
    // Nine past days of the season: days off read 60, working days 30. The
    // first weekday among them is a school-break day, a day off.
    const days: string[] = [];
    for (let back = 9; back >= 1; back--) days.push(await localDay(-back));
    const onBreak = days.find((day) => !isWeekendDate(day))!;
    const dayOff = (day: string) => isWeekendDate(day) || day === onBreak;
    for (const day of days) await measuredDay(day, dayOff(day) ? 60 : 30);
    // Not the season's: the day before it began, and today (a running day).
    await measuredDay(await localDay(-10), 200);
    await measuredDay(await localDay(0), 200);
    // A heartbeat copy is not a reading.
    await measuredDay(days[0], 200, true);

    const { seasons, dayTypes, references } = await load([breakDay(onBreak)]);

    const offDays = days.filter(dayOff).length;
    expect(dayTypes.daysOff).toEqual(
      expect.arrayContaining(days.filter(dayOff)),
    );
    expect(dayTypes.unknownDays).toEqual([]);
    const level = (off: boolean) => references.find((r) => r.dayOff === off);
    expect(level(false)).toMatchObject({
      seasonId: id,
      attractionId: rideId,
      days: 9 - offDays,
      level: 30,
    });
    expect(level(true)).toMatchObject({ days: offDays, level: 60 });
    expect(references).toHaveLength(2);

    // The whole chain: next Saturday is served the season's days-off level.
    // Nine days hold at least two weekend days, and the break day makes three.
    expect(offDays).toBeGreaterThanOrEqual(3);
    let saturday = await localDay(1);
    for (let i = 2; !isWeekendDate(saturday); i++) saturday = await localDay(i);
    const [served] = applySeasonLevel(
      [
        {
          attractionId: rideId,
          predictedTime: `${saturday}T12:00:00`,
          predictedWaitTime: 15,
          modelVersion: "tft",
        },
      ],
      seasons,
      references,
      dayTypes,
      (p) => p.predictedTime.slice(0, 10),
    );
    expect(served).toMatchObject({
      predictedWaitTime: 60,
      modelVersion: "tft+season",
    });
  });

  it("measures no weekday past the park's school data", async () => {
    await season("winter", "confirmed", await localDay(-9), await localDay(30));
    const days: string[] = [];
    for (let back = 9; back >= 1; back--) days.push(await localDay(-back));
    for (const day of days) await measuredDay(day, 40);

    // The school data ends before the season began.
    const { dayTypes, references } = await load([], await localDay(-20));

    const weekdays = days.filter((day) => !isWeekendDate(day));
    expect(dayTypes.unknownDays).toEqual(expect.arrayContaining(weekdays));
    expect(references.map((r) => r.dayOff)).toEqual([true]);
    expect(references[0].days).toBe(days.length - weekdays.length);
  });

  it("counts a listed season's own days only", async () => {
    const listed = [await localDay(-3), await localDay(-2), await localDay(4)];
    await season(
      "halloween",
      "confirmed",
      await localDay(-6),
      await localDay(10),
      listed,
    );
    for (const back of [6, 5, 4, 3, 2, 1]) {
      const day = await localDay(-back);
      await measuredDay(day, listed.includes(day) ? 80 : 20);
    }

    const { references } = await load();

    expect(references.reduce((sum, r) => sum + r.days, 0)).toBe(2);
    expect(references.every((r) => r.level === 80)).toBe(true);
  });

  it("answers with nothing for a park without a running season", async () => {
    await expect(load()).resolves.toEqual({
      seasons: [],
      daysOff: [],
      references: [],
    });
  });
});
