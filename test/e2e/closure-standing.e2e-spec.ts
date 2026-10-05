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
import { ParkDowntimeCoverage } from "../../src/analytics/entities/park-downtime-coverage.entity";
import { AttractionExposureDay } from "../../src/analytics/entities/attraction-exposure-day.entity";
import { MIN_BLIND_EVIDENCE_HOURS } from "../../src/analytics/entities/park-downtime-coverage.entity";
import { DowntimeRecoveryService } from "../../src/analytics/downtime-recovery.service";
import {
  CLOSURE_GAP_INTERVALS_SQL,
  CURRENT_CLOSURE_GAP_SQL,
  LIVE_LOOKBACK_HOURS,
  MAX_EARLY_END_SHARE,
  MAX_SIMULTANEOUS_CLOSERS,
  MIN_DAYS_FOR_CYCLE_TEST,
} from "../../src/common/utils/closure-gap.sql";

/**
 * Closures that do not come back the same day, from the readings to the curve.
 *
 * The nightly statement used to keep a closure only once the ride was running
 * again the same operating day, and the population it left behind was defined by
 * having recovered: zero censoring, and the ride that broke at 15:00 and stayed
 * shut gone before anything was counted. That is why the closure signal had no
 * "how much longer". It now keeps those closures — followed across the night to
 * the ride's next status, cut off at the live line's own lookback and stored
 * censored there — and this file shows the rows that come out, against a real
 * PostgreSQL, because every clause involved is a string to the compiler.
 *
 * One blind park in Europe/Berlin, open 10:00–18:00 every day of one June week,
 * so no case crosses a DST shift or a midnight close (closure-gap-operating-day
 * covers the wrap). Each ride is one case:
 *
 * | ride | what it does | what must come out |
 * | --- | --- | --- |
 * | A | stops Mon 15:00, runs again Tue 10:30 | one `recovered` row, 180 + 30 operating minutes |
 * | B | stops Mon 14:00, nothing for 46 hours | one `window_edge` row, cut at the 26-hour horizon |
 * | C | a 30-minute gap, then the park's own closing | the gap, and no row for the closing |
 * | D | stops in the last hour, twice | nothing |
 * | E, F, G | three rides stop in one minute | nothing, the gap that came back included |
 * | H | stops Fri 16:00, the run is at Sat 03:00 | one `ongoing` row |
 * | I | ends its day early on 6 of 7 days | nothing — a timetable |
 * | J | ends its day early on 3 of 7 days | all three |
 */
describe("standing closures, nightly and live (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;

  const TZ = "Europe/Berlin";
  /** The week the park is open. Berlin is on +02:00 throughout. */
  const DAYS = [
    "2026-06-06",
    "2026-06-07",
    "2026-06-08",
    "2026-06-09",
    "2026-06-10",
    "2026-06-11",
    "2026-06-12",
  ];
  const at = (day: string, clock: string): Date =>
    new Date(`${day}T${clock}+02:00`);

  /** The nightly run: the scan opens before the week and ends at Sat 03:00. */
  const SCAN_START = at("2026-06-05", "00:00:00");
  const AS_OF = at("2026-06-13", "03:00:00");

  let parkId: string;
  const rides = new Map<string, string>();

  const seedPark = async (): Promise<void> => {
    const park = await dataSource.getRepository(Park).save(
      dataSource.getRepository(Park).create({
        externalId: "e2e-standing-park",
        name: "Standing Park",
        slug: "standing-park",
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
        // The window builder serves parks that can report through the wiki,
        // the same capability the DOWN reconstruction is scoped to.
        wikiEntityId: "e2e-standing-wiki",
      }),
    );
    parkId = park.id;

    for (const name of [
      "A",
      "B",
      "C",
      "D",
      "E",
      "F",
      "G",
      "H",
      "I",
      "J",
      "EVIDENCE",
    ]) {
      const ride = await dataSource.getRepository(Attraction).save(
        dataSource.getRepository(Attraction).create({
          externalId: `e2e-standing-${name}`,
          name: `Ride ${name}`,
          slug: `ride-${name.toLowerCase()}`,
          parkId,
          latitude: 50.8,
          longitude: 6.9,
        }),
      );
      rides.set(name, ride.id);
    }

    for (const day of DAYS) {
      await dataSource.getRepository(ScheduleEntry).save(
        dataSource.getRepository(ScheduleEntry).create({
          parkId,
          attractionId: null,
          date: day as unknown as Date,
          scheduleType: ScheduleType.OPERATING,
          openingTime: at(day, "10:00:00"),
          closingTime: at(day, "18:00:00"),
        }),
      );
    }

    // The nightly statement derives "blind" itself: never a DOWN reading, and
    // MIN_BLIND_EVIDENCE_HOURS of observed operation. One old exposure row on a
    // ride that takes no part in any case supplies the second half without
    // touching any case's denominator.
    await exposure("EVIDENCE", "2025-01-01", MIN_BLIND_EVIDENCE_HOURS * 60);

    // The live statement reads the regime instead of deriving it.
    await dataSource.getRepository(ParkDowntimeCoverage).save(
      dataSource.getRepository(ParkDowntimeCoverage).create({
        parkId,
        regime: "never_reports",
        generatedAt: SCAN_START,
      }),
    );
  };

  /** One observed STANDBY reading: neither a heartbeat nor our own writer's. */
  const reading = async (
    ride: string,
    when: Date,
    status: string,
  ): Promise<void> => {
    await dataSource.query(
      `INSERT INTO queue_data (id, "attractionId", "queueType", status, timestamp,
                               is_heartbeat, data_source, "lastUpdated")
       VALUES (gen_random_uuid(), $1, 'STANDBY', $2, $3, false, 'themeparks-wiki', $4)`,
      [rides.get(ride), status, when, new Date(when.getTime() - 60_000)],
    );
  };

  async function exposure(
    ride: string,
    opDay: string,
    operatingMinutes = 400,
  ): Promise<void> {
    await dataSource.getRepository(AttractionExposureDay).save(
      dataSource.getRepository(AttractionExposureDay).create({
        attractionId: rides.get(ride),
        parkId,
        opDay,
        parkOpenMinutes: 480,
        operatingMinutes,
        computedAt: SCAN_START,
      }),
    );
  }

  interface NightlyRow {
    attractionId: string;
    startedAt: Date;
    endedAt: Date | null;
    startOpDay: string;
    wallMinutes: number;
    operatingMinutes: number;
    operatingDays: number;
    endReason: string;
  }

  const nightly = async (): Promise<Map<string, NightlyRow[]>> => {
    const rows: NightlyRow[] = await dataSource.query(
      CLOSURE_GAP_INTERVALS_SQL,
      [[parkId], SCAN_START, AS_OF],
    );
    const byRide = new Map<string, NightlyRow[]>();
    for (const [name, id] of rides) {
      byRide.set(
        name,
        rows.filter((row) => row.attractionId === id),
      );
    }
    return byRide;
  };

  const iso = (d: Date | string | null): string | null =>
    d === null ? null : new Date(d).toISOString();

  const seedCases = async (): Promise<void> => {
    // A: stops on Monday afternoon, stands through the night, runs again
    // half an hour after Tuesday's opening.
    await reading("A", at("2026-06-08", "10:00:00"), "OPERATING");
    await reading("A", at("2026-06-08", "15:00:00"), "CLOSED");
    await reading("A", at("2026-06-09", "10:30:00"), "OPERATING");

    // B: stops on Monday at 14:00 and is next seen running on Thursday.
    await reading("B", at("2026-06-08", "10:00:00"), "OPERATING");
    await reading("B", at("2026-06-08", "14:00:00"), "CLOSED");
    await reading("B", at("2026-06-11", "12:00:00"), "OPERATING");

    // C: an ordinary gap, then the park's own closing time.
    await reading("C", at("2026-06-08", "10:00:00"), "OPERATING");
    await reading("C", at("2026-06-08", "12:00:00"), "CLOSED");
    await reading("C", at("2026-06-08", "12:30:00"), "OPERATING");
    await reading("C", at("2026-06-08", "18:00:00"), "CLOSED");

    // D: a gap inside the last hour, and a standing closure inside it the
    // next day. The live line shows neither.
    await reading("D", at("2026-06-08", "10:00:00"), "OPERATING");
    await reading("D", at("2026-06-08", "17:15:00"), "CLOSED");
    await reading("D", at("2026-06-08", "17:40:00"), "OPERATING");
    await reading("D", at("2026-06-08", "18:00:00"), "CLOSED");
    await reading("D", at("2026-06-09", "10:00:00"), "OPERATING");
    await reading("D", at("2026-06-09", "17:30:00"), "CLOSED");

    // E, F, G: a storm. Three rides stop in one minute; E comes back.
    for (const ride of ["E", "F", "G"]) {
      await reading(ride, at("2026-06-10", "10:00:00"), "OPERATING");
      await reading(ride, at("2026-06-10", "13:00:00"), "CLOSED");
    }
    await reading("E", at("2026-06-10", "13:40:00"), "OPERATING");

    // H: stops on Friday at 16:00 and is still standing when the job runs.
    await reading("H", at("2026-06-12", "10:00:00"), "OPERATING");
    await reading("H", at("2026-06-12", "16:00:00"), "CLOSED");

    // I: ends its day early on six of its seven days, at a different hour
    // each time so the same-hour filter cannot catch it — a cinema's shape.
    const iEnds = ["12:00", "13:00", "14:00", "15:00", "16:00", "12:30"];
    for (let i = 0; i < iEnds.length; i++) {
      await reading("I", at(DAYS[i], "10:00:00"), "OPERATING");
      await reading("I", at(DAYS[i], `${iEnds[i]}:00`), "CLOSED");
    }
    await reading("I", at(DAYS[6], "10:00:00"), "OPERATING");

    // J: ends early on three of its seven days and runs again each morning.
    await reading("J", at("2026-06-06", "10:00:00"), "OPERATING");
    await reading("J", at("2026-06-07", "14:00:00"), "CLOSED");
    await reading("J", at("2026-06-08", "10:00:00"), "OPERATING");
    await reading("J", at("2026-06-08", "15:00:00"), "CLOSED");
    await reading("J", at("2026-06-09", "10:00:00"), "OPERATING");
    await reading("J", at("2026-06-09", "13:00:00"), "CLOSED");
    await reading("J", at("2026-06-10", "10:00:00"), "OPERATING");

    // The early-end denominator: seven operating days for each of I and J.
    for (const day of DAYS) {
      await exposure("I", day);
      await exposure("J", day);
    }
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

  beforeEach(() => rides.clear());

  it("keeps a closure that came back the next morning, measured on the operating clock", async () => {
    await seedPark();
    await seedCases();

    const [row, ...rest] = (await nightly()).get("A")!;

    expect(rest).toHaveLength(0);
    expect(row.endReason).toBe("recovered");
    expect(iso(row.startedAt)).toBe(iso(at("2026-06-08", "15:00:00")));
    expect(iso(row.endedAt)).toBe(iso(at("2026-06-09", "10:30:00")));
    // Monday 15:00-18:00 and Tuesday 10:00-10:30. The night is not counted.
    expect(Number(row.operatingMinutes)).toBe(210);
    expect(Number(row.operatingDays)).toBe(2);
    expect(Number(row.wallMinutes)).toBe(19 * 60 + 30);
    // node-postgres hands a DATE back as local midnight, so read it locally.
    const opDay = new Date(row.startOpDay);
    expect(
      `${opDay.getFullYear()}-${String(opDay.getMonth() + 1).padStart(2, "0")}-${String(opDay.getDate()).padStart(2, "0")}`,
    ).toBe("2026-06-08");
  });

  it("cuts a closure off at the live line's horizon and stores it censored", async () => {
    await seedPark();
    await seedCases();

    const [row, ...rest] = (await nightly()).get("B")!;

    expect(rest).toHaveLength(0);
    // Not `recovered` on Thursday: that is 46 hours later, past the horizon.
    expect(LIVE_LOOKBACK_HOURS).toBe(26);
    expect(row.endReason).toBe("window_edge");
    expect(row.endedAt).toBeNull();
    // Monday 14:00-18:00 and Tuesday 10:00-16:00, where the 26 hours end.
    expect(Number(row.operatingMinutes)).toBe(240 + 360);
    expect(Number(row.wallMinutes)).toBe(LIVE_LOOKBACK_HOURS * 60);
  });

  it("keeps the same-day gap and leaves the park's closing time alone", async () => {
    await seedPark();
    await seedCases();

    const rows = (await nightly()).get("C")!;

    expect(rows).toHaveLength(1);
    expect(rows[0].endReason).toBe("recovered");
    expect(Number(rows[0].operatingMinutes)).toBe(30);
    expect(Number(rows[0].operatingDays)).toBe(1);
  });

  it("drops a closure that starts in the last hour, gap or not", async () => {
    await seedPark();
    await seedCases();

    expect((await nightly()).get("D")).toHaveLength(0);
  });

  it("drops a storm, the ride that came back included", async () => {
    // The gap E took used to be judged among gaps alone, where it was the only
    // closer of its minute; among every closure of the park it is one of three.
    await seedPark();
    await seedCases();
    expect(3).toBeGreaterThan(MAX_SIMULTANEOUS_CLOSERS);

    const byRide = await nightly();

    for (const ride of ["E", "F", "G"]) {
      expect(byRide.get(ride)).toHaveLength(0);
    }

    // The counter-check that the storm, and nothing else, is what drops them:
    // with G's closure moved a minute later, E and F are two closers and both
    // come out — E's gap and F's standing closure.
    await dataSource.query(
      `UPDATE queue_data SET timestamp = $2
        WHERE "attractionId" = $1 AND status = 'CLOSED'`,
      [rides.get("G"), at("2026-06-10", "13:01:00")],
    );
    const after = await nightly();
    expect(after.get("E")).toHaveLength(1);
    expect(after.get("E")![0].endReason).toBe("recovered");
    expect(after.get("F")).toHaveLength(1);
  });

  it("stores a closure still standing at the run as ongoing", async () => {
    await seedPark();
    await seedCases();

    const [row, ...rest] = (await nightly()).get("H")!;

    expect(rest).toHaveLength(0);
    expect(row.endReason).toBe("ongoing");
    expect(row.endedAt).toBeNull();
    expect(Number(row.operatingMinutes)).toBe(120);
  });

  it("drops a ride whose day ends early more often than not, and keeps one that does not", async () => {
    await seedPark();
    await seedCases();
    expect(DAYS.length).toBeGreaterThanOrEqual(MIN_DAYS_FOR_CYCLE_TEST);
    expect(6 / DAYS.length).toBeGreaterThan(MAX_EARLY_END_SHARE);
    expect(3 / DAYS.length).toBeLessThanOrEqual(MAX_EARLY_END_SHARE);

    const byRide = await nightly();

    expect(byRide.get("I")).toHaveLength(0);
    const j = byRide.get("J")!;
    expect(j).toHaveLength(3);
    expect(j.every((row) => row.endReason === "recovered")).toBe(true);
  });

  it("the live line reads the elapsed minutes on the same clock", async () => {
    // Ride A at 10:15 on Tuesday: still standing, with 180 minutes of Monday
    // and 15 of Tuesday behind it. This is the figure the curve is read at.
    await seedPark();
    await reading("A", at("2026-06-08", "10:00:00"), "OPERATING");
    await reading("A", at("2026-06-08", "15:00:00"), "CLOSED");

    const rows: Array<{
      attractionId: string;
      startedAt: Date;
      elapsedOperatingMinutes: number;
    }> = await dataSource.query(CURRENT_CLOSURE_GAP_SQL, [
      [rides.get("A")],
      TZ,
      at("2026-06-09", "10:15:00"),
      parkId,
    ]);

    expect(rows).toHaveLength(1);
    expect(Number(rows[0].elapsedOperatingMinutes)).toBe(195);
  });

  it("builds a curve for each signal, and a closure that turned into REFURBISHMENT is censored", async () => {
    // Twenty-two intervals per signal, identical except for the signal:
    // one recovery at 5 minutes, one REFURBISHMENT at 20, ten recoveries at 30
    // and ten closures cut off at 60.
    //
    // At the 5-minute bucket S(5) = 21/22 for both. For closed_gap the
    // REFURBISHMENT is censored, so S(35) = 21/22 * 10/20 and the share back
    // within 30 more minutes is exactly 0.500. For down it is an event, S(35)
    // = 21/22 * 20/21 * 10/20, and the share is 0.524. Same rows, two answers,
    // which is the whole of the signal split.
    await seedPark();
    const service = app.get(DowntimeRecoveryService);

    const base = Date.now() - 10 * 24 * 60 * 60 * 1000;
    let n = 0;
    const outage = async (
      signal: "down" | "closed_gap",
      minutes: number,
      endReason: string,
    ) => {
      n++;
      await dataSource.query(
        `INSERT INTO attraction_outages
           ("attractionId", started_at, "parkId", ended_at, operating_minutes,
            observed_operating_minutes, wall_minutes, operating_days, signal,
            end_reason, duration_usable, likely_works_period, rows_in_spell,
            heartbeat_rows, start_censored, fingerprint_version, computed_at)
         VALUES ($1, $2, $3, NULL, $4, $4, $4, 1, $5, $6, false, false, 1, 0,
                 false, 1, now())`,
        [
          rides.get(signal === "down" ? "A" : "B"),
          new Date(base + n * 60_000),
          parkId,
          minutes,
          signal,
          endReason,
        ],
      );
    };
    for (const signal of ["down", "closed_gap"] as const) {
      await outage(signal, 5, "recovered");
      await outage(signal, 20, "reclassified");
      for (let i = 0; i < 10; i++) await outage(signal, 30, "recovered");
      for (let i = 0; i < 10; i++) await outage(signal, 60, "window_edge");
    }

    await service.rebuild();

    const pooled: Array<{
      signal: string;
      recovery_within_30: string;
      at_risk: number;
    }> = await dataSource.query(
      `SELECT signal, recovery_within_30, at_risk
         FROM downtime_recovery_curves
        WHERE park_id IS NULL AND elapsed_minutes = 5`,
    );
    const bySignal = new Map(pooled.map((row) => [row.signal, row]));

    expect(Number(bySignal.get("closed_gap")?.recovery_within_30)).toBeCloseTo(
      0.5,
      3,
    );
    expect(Number(bySignal.get("down")?.recovery_within_30)).toBeCloseTo(
      0.524,
      3,
    );
  });
});
