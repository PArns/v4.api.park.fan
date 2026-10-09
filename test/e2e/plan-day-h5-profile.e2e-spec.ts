import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { DataSource } from "typeorm";
import { Redis } from "ioredis";
import { AppModule } from "../../src/app.module";
import { Park } from "../../src/parks/entities/park.entity";
import { Attraction } from "../../src/attractions/entities/attraction.entity";
import {
  ScheduleEntry,
  ScheduleType,
} from "../../src/parks/entities/schedule-entry.entity";
import { AttractionHourlyHistory } from "../../src/analytics/entities/attraction-hourly-history.entity";
import { H5ProfileService } from "../../src/parks/services/h5-profile.service";
import { REDIS_CLIENT } from "../../src/common/redis/redis.module";
import { clearTestData } from "../helpers/seed-test-data";

/**
 * The H5 profile read (PAR-834) against a real PostgreSQL: the two statements
 * are strings to the compiler, and the window arithmetic (minutes from the
 * service date's own midnight, a closing past midnight above 1440) only exists
 * in SQL.
 *
 * One Europe/Berlin park, open 10:00–12:00 on four June weekdays and 18:00–
 * 01:00 on a fifth, with one ride whose rollup rows hold a 20-minute queue at
 * the opening, 40 or 60 from 11:00, and 70 after midnight on the late day.
 */
describe("H5 profiles from the rollup (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let service: H5ProfileService;
  let redis: Redis;

  const TZ = "Europe/Berlin";
  const TODAY = "2026-06-20";
  const DAYS = ["2026-06-15", "2026-06-16", "2026-06-17", "2026-06-19"];
  /** Open 18:00 → 01:00; its small hours sit in 2026-06-19's rollup row. */
  const LATE = "2026-06-18";
  const at = (day: string, clock: string) => new Date(`${day}T${clock}+02:00`);

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    app = moduleFixture.createNestApplication();
    await app.init();
    dataSource = app.get(DataSource);
    service = app.get(H5ProfileService);
    redis = app.get(REDIS_CLIENT);
  });

  afterAll(async () => {
    await app.close();
  });

  beforeEach(async () => {
    await clearTestData(app);
    await redis.flushdb();
  });

  it("builds the opening ramp, the hourly shape and the night past midnight", async () => {
    const park = await dataSource.getRepository(Park).save(
      dataSource.getRepository(Park).create({
        externalId: "e2e-h5-park",
        name: "H5 Park",
        slug: "h5-park",
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
    const ride = await dataSource.getRepository(Attraction).save(
      dataSource.getRepository(Attraction).create({
        externalId: "e2e-h5-ride",
        name: "Ride",
        slug: "h5-ride",
        parkId: park.id,
      }),
    );

    const schedule = async (day: string, open: Date, close: Date) =>
      dataSource.getRepository(ScheduleEntry).save(
        dataSource.getRepository(ScheduleEntry).create({
          parkId: park.id,
          attractionId: null,
          date: day as unknown as Date,
          scheduleType: ScheduleType.OPERATING,
          openingTime: open,
          closingTime: close,
        }),
      );
    const history = async (day: string, slots: Array<[string, number]>) =>
      dataSource.getRepository(AttractionHourlyHistory).save(
        dataSource.getRepository(AttractionHourlyHistory).create({
          attractionId: ride.id,
          parkId: park.id,
          date: day,
          slots: slots.map(([time_slot, avgWait]) => ({
            time_slot,
            avgWait,
            p90: avgWait,
            sampleCount: 3,
          })),
          downCount: 0,
          calculatedAt: new Date(),
        }),
      );

    // 11:00 onward: 40 on two days, 60 on the other two. The 19th's row also
    // holds 00:15 — the late day's small hours, stored under the next date,
    // and over three hours before 10:00, so it does not carry into the 19th's
    // own opening. (Today's row never exists: the rollup writes finished days.)
    for (const [i, day] of DAYS.entries()) {
      await schedule(day, at(day, "10:00:00"), at(day, "12:00:00"));
      const late: Array<[string, number]> =
        day === "2026-06-19" ? [["00:15", 70]] : [];
      await history(day, [...late, ["10:00", 20], ["11:00", i < 2 ? 40 : 60]]);
    }
    await schedule(LATE, at(LATE, "18:00:00"), at("2026-06-19", "01:00:00"));
    await history(LATE, [["18:00", 30]]);
    // A row outside the 56-day window is not read.
    await schedule(
      "2026-04-01",
      at("2026-04-01", "10:00:00"),
      at("2026-04-01", "12:00:00"),
    );
    await history("2026-04-01", [["10:00", 90]]);

    const profiles = await service.getProfiles(park, TODAY);
    const p = profiles.get(ride.id)!;

    expect(p.days).toBe(5);
    // Four 10:00–12:00 days: the opening slot and the 10 o'clock hour are 20,
    // forward-filled across the hour. The April 90 is outside the window.
    expect(p.open.all[0]).toBe(20);
    expect(p.hourly.all[10]).toBe(20);
    expect(p.hourly.all[11]).toBe(50);
    // The closing slot: 40, 40, 60, 60 on the short days, and the late day's
    // 00:45 — the 70 stored under the NEXT date, which only a window running
    // past 1440 reaches. With it the median is 60; without it, 50.
    expect(p.close.all[0]).toBe(60);
    // Daily P90s 40, 40, 60, 60 and the late day's 70.
    // All five are weekdays, so the weekday reference is the same.
    expect(p.refLevel).toEqual({ wd: 60, we: null, all: 60 });

    // Cached: a second read does not need the database.
    const cached = await redis.get(`plan-day:h5:v2:${park.id}:${TODAY}`);
    expect(cached).not.toBeNull();
  });
});
