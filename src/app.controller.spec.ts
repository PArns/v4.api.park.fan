// `marked` ships ESM only and AppService imports it at module load; the unit jest
// config (unlike test/jest-e2e.json) has no moduleNameMapper for it.
jest.mock("marked", () => ({ marked: { parse: (text: string) => text } }));

import { Test } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import request from "supertest";
import { AppController } from "./app.controller";
import { AppService } from "./app.service";
import { CacheControlInterceptor } from "./common/interceptors/cache-control.interceptor";
import { ExcludeNullInterceptor } from "./common/interceptors/exclude-null.interceptor";

/**
 * Mirror main.ts: the global prefix exclusion AND the global interceptors that sit on
 * every response. Without them these suites would pass against a pipeline the real app
 * never uses — ExcludeNullInterceptor maps every response body, and CacheControlInterceptor
 * would clobber a handler's Cache-Control if it did not defer to an already-set header.
 *
 * The exclude list is the point of both suites below, so it is written out once here: two
 * copies drifting apart is the same failure as forgetting main.ts.
 */
async function createRootApp(): Promise<INestApplication> {
  const moduleRef = await Test.createTestingModule({
    controllers: [AppController],
    providers: [AppService],
  }).compile();

  const app = moduleRef.createNestApplication();
  app.setGlobalPrefix("v1", {
    exclude: ["/", "/robots.txt", "/.well-known/api-catalog"],
  });
  app.useGlobalInterceptors(
    new CacheControlInterceptor(),
    new ExcludeNullInterceptor(),
  );
  await app.init();
  return app;
}

/**
 * `/robots.txt` must resolve at the HOST ROOT. It is served by the root controller,
 * so it only works while it stays in `setGlobalPrefix`'s exclude list — drop it there
 * and the route silently moves to `/v1/robots.txt`, which no crawler ever requests,
 * putting the 404 warning straight back in the log. That is the regression this guards.
 */
describe("AppController — robots.txt", () => {
  let app: INestApplication;

  beforeAll(async () => {
    app = await createRootApp();
  });

  afterAll(async () => {
    await app.close();
  });

  it("serves plain-text robots directives at the root", async () => {
    const response = await request(app.getHttpServer())
      .get("/robots.txt")
      .expect(200);

    expect(response.headers["content-type"]).toContain("text/plain");
    expect(response.text).toContain("User-agent: *");
    expect(response.text).toContain("Disallow: /v1/");
    expect(response.text).toContain("Disallow: /api");
  });

  it("keeps the handler's own Cache-Control through the global interceptor", async () => {
    const response = await request(app.getHttpServer())
      .get("/robots.txt")
      .expect(200);

    expect(response.headers["cache-control"]).toBe("public, max-age=86400");
  });

  it("does not move robots.txt under the /v1 prefix", async () => {
    await request(app.getHttpServer()).get("/v1/robots.txt").expect(404);
  });
});

/**
 * The API catalog (RFC 9727). Agents do not follow a link to find it — they ask the host for
 * /.well-known/api-catalog — so the path, the media type and the Link header on a HEAD are all
 * part of the contract, and none of them is visible on any page a human looks at.
 */
describe("AppController — API catalog", () => {
  let app: INestApplication;

  beforeAll(async () => {
    app = await createRootApp();
  });

  afterAll(async () => {
    await app.close();
  });

  it("serves a linkset naming the OpenAPI description, the docs and the health endpoint", async () => {
    const response = await request(app.getHttpServer())
      .get("/.well-known/api-catalog")
      .expect(200);

    // Express re-formats the type it was handed and sorts the parameters, so `charset`
    // lands between the media type and the profile RFC 9727 §4.2 asks for. Pinned in full
    // because the media type is what a client content-negotiates on.
    expect(response.headers["content-type"]).toBe(
      'application/linkset+json; charset=utf-8; profile="https://www.rfc-editor.org/info/rfc9727"',
    );

    const [entry] = response.body.linkset;
    expect(entry.anchor).toBe("https://api.park.fan/v1");
    expect(entry["service-desc"][0].href).toBe("https://api.park.fan/api-json");
    expect(entry["service-doc"][0].href).toBe("https://api.park.fan/api");
    expect(entry.status[0].href).toBe("https://api.park.fan/v1/health");
  });

  it("answers a HEAD with the api-catalog link relation", async () => {
    const response = await request(app.getHttpServer())
      .head("/.well-known/api-catalog")
      .expect(200);

    expect(response.headers["link"]).toContain('rel="api-catalog"');
  });

  it("points at the catalog from the docs page at the root", async () => {
    const response = await request(app.getHttpServer()).get("/").expect(200);

    expect(response.headers["link"]).toBe(
      '</.well-known/api-catalog>; rel="api-catalog"; type="application/linkset+json"',
    );
  });

  it("does not move the catalog under the /v1 prefix", async () => {
    await request(app.getHttpServer())
      .get("/v1/.well-known/api-catalog")
      .expect(404);
  });
});
