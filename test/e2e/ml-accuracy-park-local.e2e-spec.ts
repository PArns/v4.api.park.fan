import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import { Park } from "../../src/parks/entities/park.entity";
import { Attraction } from "../../src/attractions/entities/attraction.entity";
import { ParkP50Baseline } from "../../src/analytics/entities/park-p50-baseline.entity";
import { PredictionAccuracy } from "../../src/ml/entities/prediction-accuracy.entity";
import { PredictionAccuracyService } from "../../src/ml/services/prediction-accuracy.service";
import { ForecastAccuracyService } from "../../src/ml/services/forecast-accuracy.service";

/**
 * Two accuracy reads that grouped or cut by the UTC day (PAR-1333), against a
 * Los Angeles park, where UTC and the park disagree for a third of every day.
 */
describe("accuracy reads use the park's day and hour (e2e)", () => {
  const TZ = "America/Los_Angeles";
  let app: INestApplication;
  let dataSource: DataSource;
  let parkId: string;
  let rideId: string;

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    app = moduleFixture.createNestApplication();
    await app.init();
    dataSource = app.get(DataSource);
  });

  // The e2e setup truncates every table after each test, so each test seeds.
  beforeEach(async () => {
    const parks = dataSource.getRepository(Park);
    parkId = (
      await parks.save(
        parks.create({
          externalId: "e2e-park-local",
          name: "Park Local",
          slug: "park-local",
          latitude: 34.1,
          longitude: -118.3,
          timezone: TZ,
          continent: "North America",
          continentSlug: "north-america",
          country: "United States",
          countrySlug: "united-states",
          countryCode: "US",
          city: "Los Angeles",
          citySlug: "los-angeles-park-local",
        }),
      )
    ).id;
    const rides = dataSource.getRepository(Attraction);
    rideId = (
      await rides.save(
        rides.create({
          externalId: "e2e-park-local-ride",
          name: "Park Local Ride",
          slug: "park-local-ride",
          parkId,
          latitude: 34.1,
          longitude: -118.3,
        }),
      )
    ).id;
  });

  afterAll(async () => {
    await app.close();
  });

  it("files a forecast error under the park's hour and weekday", async () => {
    await dataSource.getRepository(ParkP50Baseline).save({
      parkId,
      p50Baseline: 20,
      typicalDayPeak: 40,
      headlinerCount: 1,
      tier: "tier1",
      sampleCount: 100,
      distinctDays: 30,
      confidence: "high",
      calculatedAt: new Date(),
    });
    // The newest Saturday 20:00 in Los Angeles that is over: Sunday 03:00 or
    // 04:00 in UTC, which the UTC grouping filed under Sunday.
    const [{ target }] = await dataSource.query(
      `SELECT ((d::date + time '20:00') AT TIME ZONE $1) AS target
         FROM generate_series(current_date - 9, current_date - 2, interval '1 day') AS d
        WHERE extract(dow FROM d) = 6
        ORDER BY d DESC LIMIT 1`,
      [TZ],
    );
    const accuracy = dataSource.getRepository(PredictionAccuracy);
    await accuracy.save(
      accuracy.create({
        attractionId: rideId,
        predictionTime: new Date(new Date(target).getTime() - 3_600_000),
        targetTime: new Date(target),
        predictedWaitTime: 40,
        actualWaitTime: 30,
        absoluteError: 10,
        modelVersion: "e2e",
        predictionType: "hourly",
        wasUnplannedClosure: false,
        comparisonStatus: "COMPLETED",
      }),
    );

    const service = app.get(PredictionAccuracyService);
    expect(await service.getHourlyAccuracyPatterns(30)).toEqual([
      { hour: 20, mae: 10, predictionsCount: 1 },
    ]);
    expect(await service.getDayOfWeekAccuracyPatterns(30)).toEqual([
      expect.objectContaining({ dayOfWeek: 6, dayName: "Saturday" }),
    ]);
  });

  it("scores a park's day once it is over there, evening included", async () => {
    // Yesterday by the UTC date. Its Los Angeles evening lies after UTC
    // midnight, and until 08:00 UTC the day is still running in the park.
    const [{ day, over }] = await dataSource.query(
      `SELECT (current_date - 1)::text AS day,
              (current_date - 1) < (now() AT TIME ZONE $1)::date AS over`,
      [TZ],
    );
    // 600 forecasts of 60 min at leads 31–60 (one cell, above MIN_SAMPLE), for
    // rides that read 20 at 10:00 and 60 at 20:00 park time.
    await dataSource.query(
      `WITH rides AS (
         SELECT gen_random_uuid() AS aid FROM generate_series(1, 20)
       ), hours AS (
         INSERT INTO queue_data_aggregates
           (id, hour, "attractionId", "parkId", p25, p50, p75, p90, p95, p99,
            iqr, "stdDev", mean, "sampleCount")
         SELECT gen_random_uuid(),
                (($1 || ' ' || h.clock)::timestamp AT TIME ZONE $3),
                r.aid::text, $2, h.wait, h.wait, h.wait, h.wait, h.wait, h.wait,
                0, 0, h.wait, 5
           FROM rides r, (VALUES ('10:00', 20), ('20:00', 60)) AS h(clock, wait)
       )
       INSERT INTO tft_forecasts
         (attraction_id, target_date, forecast_date, predicted_peak, model_version)
       SELECT r.aid, $1::date, $1::date - lead, 60, 'e2e'
         FROM rides r, generate_series(31, 60) AS lead`,
      [day, parkId, TZ],
    );

    await app.get(ForecastAccuracyService).rebuild();

    const cells = await dataSource.query(
      `SELECT predicted_band, lead_bucket, mean_actual::float AS mean_actual
         FROM forecast_accuracy_profile`,
    );
    expect(cells).toEqual(
      over
        ? [{ predicted_band: "busy", lead_bucket: "d60", mean_actual: 60 }]
        : [],
    );
  });
});
