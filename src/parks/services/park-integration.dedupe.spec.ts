import { Test, TestingModule } from "@nestjs/testing";
import { ParkIntegrationService } from "./park-integration.service";
import { ParksService } from "../parks.service";
import { WeatherService } from "../weather.service";
import { WeatherWarningsService } from "../weather-warnings.service";
import { AttractionsService } from "../../attractions/attractions.service";
import { ShowsService } from "../../shows/shows.service";
import { RestaurantsService } from "../../restaurants/restaurants.service";
import { QueueDataService } from "../../queue-data/queue-data.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { MLService } from "../../ml/ml.service";
import { PredictionAccuracyService } from "../../ml/services/prediction-accuracy.service";
import { PredictionDeviationService } from "../../ml/services/prediction-deviation.service";
import { HolidaysService } from "../../holidays/holidays.service";
import { ParkEnrichmentService } from "./park-enrichment.service";
import { ThemeParksClient } from "../../external-apis/themeparks/themeparks.client";
import { QueueTimesClient } from "../../external-apis/queue-times/queue-times.client";
import { WartezeitenClient } from "../../external-apis/wartezeiten/wartezeiten.client";
import { PopularityService } from "../../popularity/popularity.service";
import { RideProfileService } from "../../attractions/services/ride-profile.service";
import { AttractionOutageService } from "../../attractions/services/attraction-outage.service";
import { REDIS_CLIENT } from "../../common/redis/redis.module";

/**
 * The park payload serves one row per attraction name. What the key may be is
 * the whole subject here, because the obvious improvement is wrong: keying on
 * the slug as well looks like it would stop a name collision from hiding a
 * ride, and measured against production it only invents rides.
 *
 * A slug is a frozen name — a rename updates `name` and deliberately leaves
 * `slug` alone so the public URL survives, and 281 rows carry a slug that no
 * longer matches. Identity is the `externalId`, resolved against the upstream
 * entity (`docs/architecture/attraction-status-and-seasonality.md` §4a), and
 * that column never reaches this DTO.
 *
 * Measured on 2026-09-20 over all 210 parks and 7300 rows: grouping by name
 * yields 7252 attractions, grouping by name plus slug base yields 7255. Exactly
 * three groups split, and in each one the ride the odd slug names is already
 * served as its own row — `wahoo-racer-twisted-whizzard` carries Wahoo Racer,
 * `castaway-bay-sky-fortress` the Castaway Bay climb, `discovery-bay-treehouse`
 * "Discovery Bay - Mini Waves". So the split recovers nothing and publishes
 * "Typhoon Twister", "Wally the Walrus" and "Discovery Bay" twice each
 * (PAR-259).
 */
describe("ParkIntegrationService › deduplicateEntities", () => {
  let deduplicate: <T>(entities: T[]) => T[];

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ParkIntegrationService,
        // The constructor takes 20 deps; none is reached on this path.
        { provide: ParksService, useValue: {} },
        { provide: WeatherService, useValue: {} },
        { provide: WeatherWarningsService, useValue: {} },
        { provide: AttractionsService, useValue: {} },
        { provide: ShowsService, useValue: {} },
        { provide: RestaurantsService, useValue: {} },
        { provide: QueueDataService, useValue: {} },
        { provide: AnalyticsService, useValue: {} },
        { provide: MLService, useValue: {} },
        { provide: PredictionAccuracyService, useValue: {} },
        { provide: PredictionDeviationService, useValue: {} },
        { provide: HolidaysService, useValue: {} },
        { provide: ParkEnrichmentService, useValue: {} },
        { provide: ThemeParksClient, useValue: {} },
        { provide: QueueTimesClient, useValue: {} },
        { provide: WartezeitenClient, useValue: {} },
        { provide: PopularityService, useValue: {} },
        { provide: RideProfileService, useValue: {} },
        { provide: AttractionOutageService, useValue: {} },
        { provide: REDIS_CLIENT, useValue: {} },
      ],
    }).compile();

    const service = module.get(ParkIntegrationService);
    deduplicate = (entities) =>
      (
        service as unknown as Record<string, (e: unknown[]) => unknown[]>
      ).deduplicateEntities.call(service, entities) as typeof entities;
  });

  /**
   * The three production pairs, with the fields the API actually returns on
   * 2026-09-20. In each one the slugs differ and the ride is the same.
   */
  const productionPairs = [
    {
      park: "Hurricane Harbor Arlington!",
      name: "Typhoon Twister",
      rows: [
        { slug: "wahoo-racer", latitude: 32.761227, longitude: -97.080663 },
        { slug: "typhoon-twister", latitude: null, longitude: null },
      ],
    },
    {
      park: "Sea World",
      name: "Wally the Walrus",
      rows: [
        {
          slug: "wally-the-walrus",
          latitude: -27.9589941,
          longitude: 153.424758,
        },
        {
          slug: "castaway-bay-sky-climb",
          latitude: -27.9590947,
          longitude: 153.4246361,
        },
      ],
    },
    {
      park: "Hurricane Harbor New Jersey",
      name: "Discovery Bay",
      rows: [
        {
          slug: "discovery-bay-mini-waves",
          latitude: 40.147331,
          longitude: -74.438763,
        },
        { slug: "discovery-bay", latitude: 40.147666, longitude: -74.43879 },
      ],
    },
  ];

  it.each(productionPairs)(
    "serves one row for the $park pair whose slugs disagree ($name)",
    ({ name, rows }) => {
      const result = deduplicate(rows.map((row) => ({ ...row, name })));

      expect(result).toHaveLength(1);
    },
  );

  it("keeps two rows apart when their names differ", () => {
    const result = deduplicate([
      { name: "Wahoo Racer", slug: "wahoo-racer-twisted-whizzard" },
      { name: "Typhoon Twister", slug: "typhoon-twister" },
    ]);

    expect(result).toHaveLength(2);
  });

  it("prefers the OPERATING row over one that is not", () => {
    const [survivor] = deduplicate([
      { name: "Taron", slug: "taron", status: "CLOSED", id: "closed" },
      { name: "Taron", slug: "taron-2", status: "OPERATING", id: "operating" },
    ]);

    expect(survivor).toMatchObject({ id: "operating" });
  });

  it("prefers the row with coordinates when the status is the same", () => {
    const [survivor] = deduplicate([
      {
        name: "Taron",
        slug: "taron",
        status: "CLOSED",
        latitude: null,
        id: "no-coords",
      },
      {
        name: "Taron",
        slug: "taron-2",
        status: "CLOSED",
        latitude: 50.798,
        id: "coords",
      },
    ]);

    expect(survivor).toMatchObject({ id: "coords" });
  });

  it("reads the nested wait-times shape, where name and status sit one level down", () => {
    const result = deduplicate([
      {
        attraction: { id: "a", name: "Taron", slug: "taron" },
        queues: [{ status: "CLOSED" }],
      },
      {
        attraction: { id: "b", name: "Taron", slug: "taron-2" },
        queues: [{ status: "OPERATING" }],
      },
    ]);

    expect(result).toHaveLength(1);
    expect(result[0]).toMatchObject({ attraction: { id: "b" } });
  });

  it("drops a row with no name rather than grouping every one of them together", () => {
    const result = deduplicate([
      { name: "", slug: "a" },
      { name: "   ", slug: "b" },
      { name: "Taron", slug: "taron" },
    ]);

    expect(result).toHaveLength(1);
    expect(result[0]).toMatchObject({ slug: "taron" });
  });

  it("returns a single-element list untouched", () => {
    const rows = [{ name: "Taron", slug: "taron" }];

    expect(deduplicate(rows)).toBe(rows);
  });
});
