import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { ConfigModule } from "@nestjs/config";
import { DataSource } from "typeorm";
import { ShowsModule } from "../../src/shows/shows.module";
import { ShowsService } from "../../src/shows/shows.service";
import { getDatabaseConfig } from "../../src/config/database.config";
import { RedisModule } from "../../src/common/redis/redis.module";
import { Park } from "../../src/parks/entities/park.entity";
import { Show } from "../../src/shows/entities/show.entity";
import { ShowLiveData } from "../../src/shows/entities/show-live-data.entity";
import { ShowSchedulePattern } from "../../src/shows/entities/show-schedule-pattern.entity";
import {
  ScheduleEntry,
  ScheduleType,
} from "../../src/parks/entities/schedule-entry.entity";
import { LiveStatus } from "../../src/external-apis/themeparks/themeparks.types";
import { createTestPark } from "../fixtures/park.fixtures";

/**
 * A showtime belongs to the OPERATING day, not to the calendar date it falls on.
 *
 * §5 of `docs/frontend/plan-day-endpoint.md` unfolds a day that crosses
 * midnight — La Ronde's `10 → 0` is one ascending run of hours, and the hour
 * after 23 is 24 rather than 0 of the next date. Showtimes were not unfolded
 * that way: the last performance of such a day landed on the morning after,
 * where `PlanDayService.buildShows` served it as that morning's `scheduled`
 * programme and suppressed the projection behind it.
 *
 * These cases pin the rule against a real Postgres, which is the only place it
 * can be pinned: both readers are raw SQL, and a mocked manager only records
 * the statement rather than answering it. The measurements that motivated them
 * are in PAR-51 — against production, 20 showtimes move, all of them at exactly
 * 00:00. How many parks publish a wrap day is a figure that moves with the
 * schedule sync and is quoted with its date there, not here.
 *
 * The suite deliberately covers both directions. A wrap day claims the night
 * that follows it; a NORMAL day claims nothing, which is what keeps the rule
 * from becoming a blanket "early times belong to yesterday" — Universal
 * Studios Japan serves a daytime walkthrough at 01:00 park-local on 148 days,
 * and no schedule of theirs says the park was open.
 */
describe("Showtimes follow the operating day (E2E)", () => {
  let app: INestApplication;
  let shows: ShowsService;
  let dataSource: DataSource;

  const TZ = "America/Toronto";
  /** The wrap day: open 10:00, closes 01:00 the next morning. */
  const WRAP_DAY = "2026-07-04";
  /** The morning after the wrap day — a normal 10:00 → 22:00 day of its own. */
  const NEXT_DAY = "2026-07-05";

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
        ShowsModule,
      ],
    }).compile();

    app = moduleFixture.createNestApplication();
    await app.init();

    shows = app.get(ShowsService);
    dataSource = app.get(DataSource);
  });

  afterAll(async () => {
    await app?.close();
  });

  /**
   * One park, one show, and the two schedule rows that make `WRAP_DAY` a day
   * that runs past midnight while `NEXT_DAY` is an ordinary one.
   *
   * Times are written as absolute instants built from the park's own wall
   * clock, because that is what the feed stores and what both readers convert
   * back. `-04:00` is Toronto's summer offset; July is nowhere near a DST
   * boundary, so a fixed offset is honest here rather than a shortcut (see
   * 📚 G-39 for why a date plus `T00:00` is not).
   */
  async function seed(opts: {
    /** Park-local wall-clock times to publish, as `YYYY-MM-DDTHH:mm`. */
    showtimes: string[];
    /** Whether the wrap day's schedule row is written at all. */
    wrapDay?: boolean;
  }): Promise<{ showId: string; parkId: string }> {
    const parkRepo = dataSource.getRepository(Park);
    const showRepo = dataSource.getRepository(Show);
    const liveRepo = dataSource.getRepository(ShowLiveData);
    const scheduleRepo = dataSource.getRepository(ScheduleEntry);

    const park = await parkRepo.save(
      createTestPark({
        externalId: "test-park-wrap",
        name: "Test Wrap Park",
        slug: "test-wrap-park",
        timezone: TZ,
      }),
    );

    const show = await showRepo.save(
      showRepo.create({
        externalId: "test-show-latenight",
        name: "Test Late Night Show",
        slug: "test-late-night-show",
        parkId: park.id,
      }),
    );

    if (opts.wrapDay !== false) {
      // 10:00 → 01:00 the following morning. This is the row that makes the
      // night belong to WRAP_DAY.
      await scheduleRepo.save(
        scheduleRepo.create({
          parkId: park.id,
          date: WRAP_DAY as unknown as Date,
          scheduleType: ScheduleType.OPERATING,
          openingTime: new Date(`${WRAP_DAY}T10:00:00-04:00`),
          closingTime: new Date(`${NEXT_DAY}T01:00:00-04:00`),
        }),
      );
    }

    // The morning after is an ordinary day and claims nothing of the night.
    await scheduleRepo.save(
      scheduleRepo.create({
        parkId: park.id,
        date: NEXT_DAY as unknown as Date,
        scheduleType: ScheduleType.OPERATING,
        openingTime: new Date(`${NEXT_DAY}T10:00:00-04:00`),
        closingTime: new Date(`${NEXT_DAY}T22:00:00-04:00`),
      }),
    );

    // One snapshot per showtime, taken on the wrap day itself — the shape the
    // feed actually produces, where the evening's snapshot already carries the
    // performance that runs after midnight.
    await liveRepo.save(
      liveRepo.create({
        showId: show.id,
        status: LiveStatus.OPERATING,
        timestamp: new Date(`${WRAP_DAY}T20:00:00-04:00`),
        showtimes: opts.showtimes.map((t) => ({
          startTime: new Date(`${t}:00-04:00`).toISOString(),
          type: "Operating",
        })),
      }),
    );

    return { showId: show.id, parkId: park.id };
  }

  describe("getShowtimesOnDate", () => {
    it("counts a performance after midnight on the day that is still open", async () => {
      const { showId, parkId } = await seed({
        showtimes: [`${WRAP_DAY}T22:00`, `${NEXT_DAY}T00:30`],
      });

      const onWrapDay = await shows.getShowtimesOnDate(parkId, TZ, WRAP_DAY);

      // Both performances belong to the wrap day, and the one past midnight
      // sorts LAST rather than first — the day is read downwards.
      expect(onWrapDay.get(showId)).toEqual(["22:00", "00:30"]);
    });

    it("does not let that performance claim the following morning", async () => {
      const { showId, parkId } = await seed({
        showtimes: [`${WRAP_DAY}T22:00`, `${NEXT_DAY}T00:30`],
      });

      const onNextDay = await shows.getShowtimesOnDate(parkId, TZ, NEXT_DAY);

      // This is the defect PAR-51 was filed for: a single orphaned 00:30 entry
      // used to answer here, and `buildShows` prefers `scheduled` over the
      // projection, so the whole day's programme disappeared behind it.
      expect(onNextDay.has(showId)).toBe(false);
    });

    it("leaves an after-midnight time alone when no published window covers it", async () => {
      const { showId, parkId } = await seed({
        showtimes: [`${NEXT_DAY}T00:30`],
        wrapDay: false,
      });

      // Without the wrap row the night belongs to nobody but its own date. The
      // rule reads a schedule; it never guesses from the clock, which is what
      // keeps a feed that mislabels its offsets from being silently reshuffled.
      expect(
        (await shows.getShowtimesOnDate(parkId, TZ, WRAP_DAY)).has(showId),
      ).toBe(false);
      expect(
        (await shows.getShowtimesOnDate(parkId, TZ, NEXT_DAY)).get(showId),
      ).toEqual(["00:30"]);
    });

    it("leaves a performance that starts after the window closes", async () => {
      // 02:00 under a 01:00 close. This is the narrowing the rule claims to be:
      // the previous day reaches into the night only as far as it published,
      // not to some hour of the morning we picked.
      const { showId, parkId } = await seed({
        showtimes: [`${NEXT_DAY}T02:00`],
      });

      expect(
        (await shows.getShowtimesOnDate(parkId, TZ, WRAP_DAY)).has(showId),
      ).toBe(false);
      expect(
        (await shows.getShowtimesOnDate(parkId, TZ, NEXT_DAY)).get(showId),
      ).toEqual(["02:00"]);
    });

    it("counts a performance that starts exactly on the closing instant", async () => {
      // 01:00 under a 01:00 close, and the bound is inclusive on purpose: every
      // one of the 20 showtimes this rule moves in production sits exactly on
      // its day's closing time (00:00 under a midnight close), so a half-open
      // `st < closes` would move none of them and the rule would be dead code.
      // The case above — 02:00 under the same close — is what keeps the
      // inclusive bound from becoming a blanket claim on the morning.
      const { showId, parkId } = await seed({
        showtimes: [`${NEXT_DAY}T01:00`],
      });

      expect(
        (await shows.getShowtimesOnDate(parkId, TZ, WRAP_DAY)).get(showId),
      ).toEqual(["01:00"]);
      expect(
        (await shows.getShowtimesOnDate(parkId, TZ, NEXT_DAY)).has(showId),
      ).toBe(false);
    });

    it("ignores a ticketed event that runs past midnight", async () => {
      // Universal's Halloween Horror Nights is a TICKETED_EVENT, and its 00:30
      // performances are the entries this ticket was filed for. They stay put:
      // `OPERATING` is what every other reader in this codebase treats as
      // opening hours, and widening it here would change what "the park is
      // open" means for shows alone. Production holds two such rows.
      const { showId, parkId } = await seed({
        showtimes: [`${NEXT_DAY}T00:30`],
        wrapDay: false,
      });

      await dataSource.getRepository(ScheduleEntry).save(
        dataSource.getRepository(ScheduleEntry).create({
          parkId,
          date: WRAP_DAY as unknown as Date,
          scheduleType: ScheduleType.TICKETED_EVENT,
          openingTime: new Date(`${WRAP_DAY}T19:00:00-04:00`),
          closingTime: new Date(`${NEXT_DAY}T02:00:00-04:00`),
        }),
      );

      expect(
        (await shows.getShowtimesOnDate(parkId, TZ, WRAP_DAY)).has(showId),
      ).toBe(false);
      expect(
        (await shows.getShowtimesOnDate(parkId, TZ, NEXT_DAY)).get(showId),
      ).toEqual(["00:30"]);
    });

    it("does not let an overshot window swallow the following day", async () => {
      // `operating-window.util.ts` names these: a 34-hour row at SeaWorld San
      // Diego, a three-year one at Busch Gardens Williamsburg. Read raw, such a
      // row passes the wrap test and drags the NEXT day's ordinary afternoon
      // onto the date before, which would empty that day for the show.
      // `normalizedClosingSql` re-anchors it first.
      const { showId, parkId } = await seed({
        showtimes: [`${NEXT_DAY}T14:00`],
        wrapDay: false,
      });

      await dataSource.getRepository(ScheduleEntry).save(
        dataSource.getRepository(ScheduleEntry).create({
          parkId,
          date: WRAP_DAY as unknown as Date,
          scheduleType: ScheduleType.OPERATING,
          openingTime: new Date(`${WRAP_DAY}T09:00:00-04:00`),
          // 34 hours: ends the evening AFTER the next day began.
          closingTime: new Date(`${NEXT_DAY}T19:00:00-04:00`),
        }),
      );

      expect(
        (await shows.getShowtimesOnDate(parkId, TZ, NEXT_DAY)).get(showId),
      ).toEqual(["14:00"]);
    });

    it("keeps an ordinary day untouched", async () => {
      const { showId, parkId } = await seed({
        showtimes: [`${NEXT_DAY}T12:00`, `${NEXT_DAY}T18:00`],
      });

      expect(
        (await shows.getShowtimesOnDate(parkId, TZ, NEXT_DAY)).get(showId),
      ).toEqual(["12:00", "18:00"]);
    });
  });

  describe("getShowtimeInstantsOnDate", () => {
    it("returns the real instant of a performance after midnight", async () => {
      const { showId, parkId } = await seed({
        showtimes: [`${WRAP_DAY}T22:00`, `${NEXT_DAY}T00:30`],
      });

      const instants = await shows.getShowtimeInstantsOnDate(
        parkId,
        TZ,
        WRAP_DAY,
      );

      // The point of the method: the 00:30 performance belongs to WRAP_DAY but
      // happens on NEXT_DAY, so a caller rebuilding it from WRAP_DAY plus
      // "00:30" would be 24 hours early. The push job did exactly that.
      expect(instants.get(showId)).toEqual([
        new Date(`${WRAP_DAY}T22:00:00-04:00`).toISOString(),
        new Date(`${NEXT_DAY}T00:30:00-04:00`).toISOString(),
      ]);
    });

    it("agrees with getShowtimesOnDate about which day a showtime is on", async () => {
      const { showId, parkId } = await seed({
        showtimes: [`${WRAP_DAY}T22:00`, `${NEXT_DAY}T00:30`],
      });

      // Same rule, same answer: neither may report the performance on the
      // morning after, or the two callers would disagree about the same day.
      expect(
        (await shows.getShowtimesOnDate(parkId, TZ, NEXT_DAY)).has(showId),
      ).toBe(false);
      expect(
        (await shows.getShowtimeInstantsOnDate(parkId, TZ, NEXT_DAY)).has(
          showId,
        ),
      ).toBe(false);
    });
  });

  describe("rebuildSchedulePatterns", () => {
    /**
     * The pattern job reads a rolling window ending at `now()`, so a fixture
     * dated in a fixed month would fall out of it as the calendar moves. These
     * cases therefore place the wrap day relative to today and assert on the
     * WEEKDAY, which is the field the defect actually corrupts: a performance
     * counted on the morning after is counted on the wrong weekday, and the
     * projection then promises Saturday's late show on a Sunday.
     */
    async function seedRelative(
      daysAgo: number,
      opts: {
        /** Unique per case: nothing clears the tables between tests. */
        tag?: string;
        /** A plain 10:00 → 22:00 day instead of one that runs to 01:00. */
        wrap?: boolean;
        /** Write the same showtimes a second time, from a later snapshot. */
        twice?: boolean;
      } = {},
    ): Promise<{
      showId: string;
      openDay: Date;
    }> {
      const { tag = "", wrap = true, twice = false } = opts;
      const parkRepo = dataSource.getRepository(Park);
      const showRepo = dataSource.getRepository(Show);
      const liveRepo = dataSource.getRepository(ShowLiveData);
      const scheduleRepo = dataSource.getRepository(ScheduleEntry);

      const park = await parkRepo.save(
        createTestPark({
          externalId: `test-park-wrap-rel${tag}`,
          name: `Test Wrap Park Rel${tag}`,
          slug: `test-wrap-park-rel${tag}`,
          timezone: "UTC",
        }),
      );
      const show = await showRepo.save(
        showRepo.create({
          externalId: `test-show-rel${tag}`,
          name: `Test Rel Show${tag}`,
          slug: `test-rel-show${tag}`,
          parkId: park.id,
        }),
      );

      // UTC throughout: the park-local date and the stored instant agree, so
      // the assertion is about the operating-day rule and not about an offset.
      const open = new Date();
      open.setUTCDate(open.getUTCDate() - daysAgo);
      open.setUTCHours(10, 0, 0, 0);
      const close = new Date(open);
      if (wrap) {
        close.setUTCDate(close.getUTCDate() + 1);
        close.setUTCHours(1, 0, 0, 0);
      } else {
        close.setUTCHours(22, 0, 0, 0);
      }
      const midnightShow = new Date(open);
      midnightShow.setUTCDate(midnightShow.getUTCDate() + 1);
      midnightShow.setUTCHours(0, 30, 0, 0);

      const openDate = open.toISOString().slice(0, 10);

      await scheduleRepo.save(
        scheduleRepo.create({
          parkId: park.id,
          date: openDate as unknown as Date,
          scheduleType: ScheduleType.OPERATING,
          openingTime: open,
          closingTime: close,
        }),
      );

      const showtimes = [
        {
          startTime: new Date(
            open.getTime() + 12 * 60 * 60 * 1000,
          ).toISOString(),
          type: "Operating",
        },
        // Only reachable on a wrap day; on a plain one it falls outside the
        // published window and stays on its own date, which is the point of
        // the negative case below.
        { startTime: midnightShow.toISOString(), type: "Operating" },
      ];

      await liveRepo.save(
        liveRepo.create({
          showId: show.id,
          status: LiveStatus.OPERATING,
          timestamp: new Date(open.getTime() + 10 * 60 * 60 * 1000),
          showtimes,
        }),
      );

      if (twice) {
        // The feed republishes the same programme every poll, so the same
        // start time arrives once per snapshot. Deduplication used to be
        // `array_agg(DISTINCT hhmm)`; it now runs over a wider key, and this
        // is the fixture that tells the two apart.
        await liveRepo.save(
          liveRepo.create({
            showId: show.id,
            status: LiveStatus.OPERATING,
            timestamp: new Date(open.getTime() + 11 * 60 * 60 * 1000),
            showtimes,
          }),
        );
      }

      return { showId: show.id, openDay: open };
    }

    it("keys the after-midnight performance on the weekday that opened the day", async () => {
      const { showId, openDay } = await seedRelative(3);

      await shows.rebuildSchedulePatterns();

      const patterns = await dataSource
        .getRepository(ShowSchedulePattern)
        .find({ where: { showId } });

      // One pattern, not two: both performances belong to the same operating
      // day, so the night does not seed a second weekday of its own.
      expect(patterns).toHaveLength(1);
      expect(patterns[0].weekday).toBe(openDay.getUTCDay());
      expect(patterns[0].times).toEqual(["22:00", "00:30"]);
    });

    it("keys an ordinary day on its own weekday, and leaves the night out", async () => {
      // The negative half. A park closing at 22:00 publishes no claim on the
      // night, so the 00:30 entry is NOT folded back — it belongs to the next
      // calendar day, which seeds a weekday of its own. Without this case the
      // rule could key everything on the opening day and still look right.
      const { showId, openDay } = await seedRelative(4, {
        tag: "-plain",
        wrap: false,
      });

      await shows.rebuildSchedulePatterns();

      const patterns = await dataSource
        .getRepository(ShowSchedulePattern)
        .find({ where: { showId }, order: { weekday: "ASC" } });

      const openWeekday = patterns.find(
        (p) => p.weekday === openDay.getUTCDay(),
      );
      expect(openWeekday?.times).toEqual(["22:00"]);

      const nextWeekday = patterns.find(
        (p) => p.weekday === (openDay.getUTCDay() + 1) % 7,
      );
      expect(nextWeekday?.times).toEqual(["00:30"]);
    });

    it("does not repeat a time two snapshots both carry", async () => {
      // Every poll republishes the day's whole programme, so the raw rows hold
      // each start time once per snapshot. The times array is a set.
      const { showId } = await seedRelative(5, {
        tag: "-twice",
        twice: true,
      });

      await shows.rebuildSchedulePatterns();

      const patterns = await dataSource
        .getRepository(ShowSchedulePattern)
        .find({ where: { showId } });

      expect(patterns).toHaveLength(1);
      expect(patterns[0].times).toEqual(["22:00", "00:30"]);
    });
  });
});
