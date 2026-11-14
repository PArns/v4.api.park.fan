import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import { Park } from "../../src/parks/entities/park.entity";
import { Attraction } from "../../src/attractions/entities/attraction.entity";
import { PredictionLeadSnapshotService } from "../../src/ml/services/prediction-lead-snapshot.service";

/**
 * The per-park, per-day read a trip region's forecast accuracy is built
 * from (PAR-1316), against the real `prediction_lead_snapshots`: the rows
 * `getLeadTimeMae` counts (scored, served) at one lead distance, grouped by
 * park and target day, inside the dates asked for, for the parks asked for.
 * A row without a source (written before the served snapshots), without an
 * actual, at another lead, outside the dates or of another park is not one.
 */
describe("trip region accuracy reads the lead-time archive (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let snapshots: PredictionLeadSnapshotService;
  let parkA: string;
  let parkB: string;

  const park = (suffix: string) =>
    dataSource.getRepository(Park).save(
      dataSource.getRepository(Park).create({
        externalId: `e2e-accuracy-${suffix}`,
        name: `Accuracy Park ${suffix}`,
        slug: `accuracy-park-${suffix}`,
        latitude: 28.4,
        longitude: -81.5,
        timezone: "America/New_York",
        continent: "North America",
        continentSlug: "north-america",
        country: "United States",
        countrySlug: "united-states",
        countryCode: "US",
        city: "Orlando",
        citySlug: `orlando-accuracy-${suffix}`,
      }),
    );

  const ride = async (parkId: string, suffix: string) =>
    (
      await dataSource.getRepository(Attraction).save(
        dataSource.getRepository(Attraction).create({
          externalId: `e2e-accuracy-ride-${suffix}`,
          name: `Accuracy Ride ${suffix}`,
          slug: `accuracy-ride-${suffix}`,
          parkId,
          latitude: 28.4,
          longitude: -81.5,
        }),
      )
    ).id;

  const snapshot = (
    attractionId: string,
    targetDate: string,
    leadDays: number,
    absoluteError: number | null,
    source: string | null = "tft",
  ) =>
    dataSource.query(
      `INSERT INTO prediction_lead_snapshots
         (attraction_id, target_date, lead_days, predicted_wait_time,
          actual_wait_time, absolute_error, model_version, source)
       VALUES ($1::uuid, $2::date, $3::smallint, 40, 40, $4::int, 'e2e', $5)`,
      [attractionId, targetDate, leadDays, absoluteError, source],
    );

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    app = moduleFixture.createNestApplication();
    await app.init();
    dataSource = app.get(DataSource);
    snapshots = app.get(PredictionLeadSnapshotService);
  });

  // The e2e setup truncates every table after each test, so each test seeds.
  beforeEach(async () => {
    parkA = (await park("a")).id;
    parkB = (await park("b")).id;
    const [a1, a2, b1] = [
      await ride(parkA, "a1"),
      await ride(parkA, "a2"),
      await ride(parkB, "b1"),
    ];
    // Counted: lead 7, scored, served, inside the dates.
    await snapshot(a1, "2026-11-01", 7, 10);
    await snapshot(a2, "2026-11-01", 7, 20);
    await snapshot(a1, "2026-11-02", 7, 6);
    await snapshot(b1, "2026-11-01", 7, 15);
    // Not counted: another lead, no actual yet, no source, outside the dates.
    await snapshot(a1, "2026-11-03", 1, 50);
    await snapshot(a2, "2026-11-03", 7, null);
    await snapshot(a2, "2026-11-02", 7, 90, null);
    await snapshot(a1, "2026-10-01", 7, 70);
  });

  afterAll(async () => {
    await app.close();
  });

  it("groups the served, scored rows of one lead by park and day", async () => {
    const rows = await snapshots.parkDayErrors(
      [parkA, parkB],
      7,
      "2026-10-15",
      "2026-11-05",
    );
    const sorted = [...rows].sort(
      (x, y) =>
        x.parkId.localeCompare(y.parkId) ||
        x.targetDate.localeCompare(y.targetDate),
    );
    const expected = [
      { parkId: parkA, targetDate: "2026-11-01", scored: 2, errorSum: 30 },
      { parkId: parkA, targetDate: "2026-11-02", scored: 1, errorSum: 6 },
      { parkId: parkB, targetDate: "2026-11-01", scored: 1, errorSum: 15 },
    ].sort(
      (x, y) =>
        x.parkId.localeCompare(y.parkId) ||
        x.targetDate.localeCompare(y.targetDate),
    );
    expect(sorted).toEqual(expected);
  });

  it("reads only the parks asked for, and binds no id that is no uuid", async () => {
    const rows = await snapshots.parkDayErrors(
      [parkB, "not-a-uuid"],
      7,
      "2026-10-15",
      "2026-11-05",
    );
    expect(rows).toEqual([
      { parkId: parkB, targetDate: "2026-11-01", scored: 1, errorSum: 15 },
    ]);
    expect(
      await snapshots.parkDayErrors(["x"], 7, "2026-10-15", "2026-11-05"),
    ).toEqual([]);
  });
});
