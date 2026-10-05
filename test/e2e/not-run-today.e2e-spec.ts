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
import {
  NOT_RUN_TODAY_GRACE_MINUTES,
  NOT_RUN_TODAY_LOOKBACK_DAYS,
  NOT_RUN_TODAY_SQL,
} from "../../src/common/utils/not-run-today.sql";

/**
 * „Heute noch nicht in Betrieb", and when the ride last ran.
 *
 * One park in Europe/Berlin open 10:00–18:00, a Monday and the Tuesday after
 * it, and the as-of on Tuesday late morning unless a case says otherwise.
 *
 * | ride | readings | must come out |
 * | --- | --- | --- |
 * | FLIPS | runs Monday, CLOSED at Monday's close | last run Monday 18:00 |
 * | CARRIES | runs Monday, the feed only flips it on Tuesday 09:30 | last run Monday 18:00, not Tuesday 09:30 |
 * | TAIL | runs Monday, one more OPERATING reading 45 s after the close | last run Monday 18:00:45 — that reading is still Monday |
 * | MORNING | ran on Tuesday morning and stopped | nothing — that is a closure, not "not yet" |
 * | STALE | last ran eight days ago | nothing — longer than "today" can mean |
 * | RUNNING | running now | nothing |
 */
describe("rides that have not run yet today (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;

  const TZ = "Europe/Berlin";
  const at = (day: string, clock: string): Date =>
    new Date(`${day}T${clock}+02:00`);
  const MON = "2026-06-08";
  const TUE = "2026-06-09";
  const AS_OF = at(TUE, "11:00:00");

  let parkId: string;
  const rides = new Map<string, string>();

  /** Tuesday's published windows. One by default; a case can split the day. */
  const ONE_WINDOW: Array<[string, string]> = [["10:00:00", "18:00:00"]];

  const seed = async (
    tuesday: Array<[string, string]> = ONE_WINDOW,
  ): Promise<void> => {
    rides.clear();
    const park = await dataSource.getRepository(Park).save(
      dataSource.getRepository(Park).create({
        externalId: "e2e-not-run-park",
        name: "Not Run Park",
        slug: "not-run-park",
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

    for (const name of [
      "FLIPS",
      "CARRIES",
      "TAIL",
      "MORNING",
      "STALE",
      "RUNNING",
    ]) {
      const ride = await dataSource.getRepository(Attraction).save(
        dataSource.getRepository(Attraction).create({
          externalId: `e2e-not-run-${name}`,
          name: `Ride ${name}`,
          slug: `not-run-${name.toLowerCase()}`,
          parkId,
          latitude: 50.8,
          longitude: 6.9,
        }),
      );
      rides.set(name, ride.id);
    }

    const windows: Array<[string, string, string]> = [
      ["2026-05-31", "10:00:00", "18:00:00"],
      [MON, "10:00:00", "18:00:00"],
      ...tuesday.map(([o, c]): [string, string, string] => [TUE, o, c]),
    ];
    for (const [day, opens, closes] of windows) {
      await dataSource.getRepository(ScheduleEntry).save(
        dataSource.getRepository(ScheduleEntry).create({
          parkId,
          attractionId: null,
          date: day as unknown as Date,
          scheduleType: ScheduleType.OPERATING,
          openingTime: at(day, opens),
          closingTime: at(day, closes),
        }),
      );
    }

    await reading("FLIPS", at(MON, "10:00:00"), "OPERATING");
    await reading("FLIPS", at(MON, "18:00:00"), "CLOSED");

    await reading("CARRIES", at(MON, "10:00:00"), "OPERATING");
    await reading("CARRIES", at(TUE, "09:30:00"), "CLOSED");

    // The poll that lands just after the close still sees the ride running.
    await reading("TAIL", at(MON, "10:00:00"), "OPERATING");
    await reading("TAIL", at(MON, "18:00:45"), "OPERATING");
    await reading("TAIL", at(MON, "18:10:00"), "CLOSED");

    await reading("MORNING", at(MON, "10:00:00"), "OPERATING");
    await reading("MORNING", at(MON, "18:00:00"), "CLOSED");
    await reading("MORNING", at(TUE, "10:05:00"), "OPERATING");
    await reading("MORNING", at(TUE, "10:30:00"), "CLOSED");

    await reading("STALE", at("2026-05-31", "10:00:00"), "OPERATING");
    await reading("STALE", at("2026-05-31", "18:00:00"), "CLOSED");

    await reading("RUNNING", at(TUE, "10:00:00"), "OPERATING");
  };

  const reading = async (
    ride: string,
    when: Date,
    status: string,
  ): Promise<void> => {
    await dataSource.query(
      `INSERT INTO queue_data (id, "attractionId", "queueType", status, timestamp,
                               is_heartbeat, data_source, "lastUpdated")
       VALUES (gen_random_uuid(), $1, 'STANDBY', $2, $3, false, 'queue-times', $4)`,
      [rides.get(ride), status, when, new Date(when.getTime() - 60_000)],
    );
  };

  const run = async (asOf: Date): Promise<Map<string, string>> => {
    const rows: Array<{ attractionId: string; lastRunAt: Date }> =
      await dataSource.query(NOT_RUN_TODAY_SQL, [
        [...rides.values()],
        TZ,
        asOf,
        parkId,
      ]);
    const byName = new Map<string, string>();
    for (const [name, id] of rides) {
      const row = rows.find((r) => r.attractionId === id);
      if (row) byName.set(name, new Date(row.lastRunAt).toISOString());
    }
    return byName;
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

  it("names the close of the day the ride last ran", async () => {
    await seed();

    const out = await run(AS_OF);

    expect(out.get("FLIPS")).toBe(at(MON, "18:00:00").toISOString());
  });

  it("clips a run the feed carried overnight to the park's close", async () => {
    // The feed only flipped it on Tuesday morning. „Last ran Tuesday 09:30"
    // in a line that also says it has not run today would be a contradiction,
    // and nobody rode it between 18:00 and 09:30.
    await seed();

    const out = await run(AS_OF);

    expect(out.get("CARRIES")).toBe(at(MON, "18:00:00").toISOString());
  });

  it("counts a reading just after yesterday's close as yesterday", async () => {
    // "Today" used to begin at the previous close itself, so this reading
    // made the ride count as having run today, and it got no line at all.
    await seed();

    const out = await run(AS_OF);

    expect(out.get("TAIL")).toBe(at(MON, "18:00:45").toISOString());
  });

  it("does not start a new day at a midday break", async () => {
    // Two windows on Tuesday. MORNING ran in the first one, so it has run
    // today, whatever the second window says.
    await seed([
      ["10:00:00", "12:00:00"],
      ["13:00:00", "18:00:00"],
    ]);

    const out = await run(at(TUE, "15:00:00"));

    expect(out.has("MORNING")).toBe(false);
    expect(out.get("FLIPS")).toBe(at(MON, "18:00:00").toISOString());
  });

  it("says nothing about a ride that ran this morning, one that is running, or one gone for over a week", async () => {
    await seed();
    expect(NOT_RUN_TODAY_LOOKBACK_DAYS).toBe(7);

    const out = await run(AS_OF);

    expect(out.has("MORNING")).toBe(false);
    expect(out.has("RUNNING")).toBe(false);
    expect(out.has("STALE")).toBe(false);
    expect([...out.keys()].sort()).toEqual(["CARRIES", "FLIPS", "TAIL"]);
  });

  it("waits out the first minutes of the day, and says nothing once the park has shut", async () => {
    await seed();
    expect(NOT_RUN_TODAY_GRACE_MINUTES).toBe(15);

    expect((await run(at(TUE, "10:10:00"))).size).toBe(0);
    expect((await run(at(TUE, "10:20:00"))).has("FLIPS")).toBe(true);
    expect((await run(at(TUE, "18:30:00"))).size).toBe(0);
  });
});
