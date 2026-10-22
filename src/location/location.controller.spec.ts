import { Test } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
import { LocationController } from "./location.controller";
import { LocationService } from "./location.service";
import { GeoipService } from "../geoip/geoip.service";
import { HttpExceptionFilter } from "../common/filters/http-exception.filter";

/**
 * `parseFloat`/`parseInt` cut trailing garbage (`radius=5km` → 5) and a sent
 * but invalid coordinate fell back to the IP, answering for another place as
 * if it were a hit (PAR-963). Runs the real HTTP pipeline, because the pipes
 * sit on the route and a direct method call would skip them.
 */
describe("LocationController › nearby query parameters", () => {
  let app: INestApplication;
  let findNearby: jest.Mock;
  let lookupCoordinates: jest.Mock;

  const url = "/v1/discovery/nearby";

  beforeAll(async () => {
    findNearby = jest.fn().mockResolvedValue({ type: "nearby_parks" });
    lookupCoordinates = jest
      .fn()
      .mockReturnValue({ latitude: 50.8, longitude: 6.9 });

    const moduleRef = await Test.createTestingModule({
      controllers: [LocationController],
      providers: [
        { provide: LocationService, useValue: { findNearby } },
        {
          provide: GeoipService,
          useValue: { lookupCoordinates, isAvailable: () => true },
        },
      ],
    }).compile();

    app = moduleRef.createNestApplication({ logger: false });
    app.setGlobalPrefix("v1");
    app.useGlobalPipes(
      new ValidationPipe({
        whitelist: true,
        forbidNonWhitelisted: true,
        transform: true,
        transformOptions: { enableImplicitConversion: true },
      }),
    );
    app.useGlobalFilters(new HttpExceptionFilter());
    await app.init();
  });

  afterAll(async () => {
    await app.close();
  });

  beforeEach(() => {
    findNearby.mockClear();
    lookupCoordinates.mockClear();
  });

  it.each([
    ["radius", "5km"],
    ["radius", "1.5"],
    ["limit", "6abc"],
  ])("answers %s=%s with 400", async (key, value) => {
    await request(app.getHttpServer())
      .get(url)
      .query({ lat: "48.1", lng: "7.7", [key]: value })
      .expect(400);
    expect(findNearby).not.toHaveBeenCalled();
  });

  it.each([
    [{ lat: "91", lng: "7.7" }],
    [{ lat: "48.1abc", lng: "7.7" }],
    [{ lat: "abc", lng: "7.7" }],
    [{ lat: "48.1", lng: "181" }],
    [{ lat: "48.1" }],
    [{ lat: "", lng: "" }],
  ])("answers %o with 400 without asking the IP", async (query) => {
    await request(app.getHttpServer()).get(url).query(query).expect(400);
    expect(lookupCoordinates).not.toHaveBeenCalled();
    expect(findNearby).not.toHaveBeenCalled();
  });

  it("passes valid coordinates and integers through", async () => {
    await request(app.getHttpServer())
      .get(url)
      .query({ lat: "-33.5", lng: "151.2", radius: "500", limit: "10" })
      .expect(200);
    expect(lookupCoordinates).not.toHaveBeenCalled();
    expect(findNearby).toHaveBeenCalledWith(-33.5, 151.2, 500, 10);
  });

  it("resolves the IP when neither coordinate is sent, with the defaults", async () => {
    await request(app.getHttpServer())
      .get(url)
      .set("X-Forwarded-For", "203.0.113.7")
      .expect(200);
    expect(lookupCoordinates).toHaveBeenCalledTimes(1);
    expect(findNearby).toHaveBeenCalledWith(50.8, 6.9, 1000, 6);
  });
});
