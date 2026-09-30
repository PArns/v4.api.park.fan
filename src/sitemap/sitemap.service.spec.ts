import { SitemapService } from "./sitemap.service";

/**
 * The attraction sitemap has to list exactly the ride pages the frontend can
 * render.
 *
 * That is every ride in the park payload, plus every ride retired as closed:
 * the frontend renders those from the detail endpoint as closed permanently.
 * A row the children sync retired as a show or a restaurant has no ride page
 * and stays out.
 *
 * The payload keeps one row per attraction NAME, which cost 48 entries
 * measured on 2026-09-26 — see `oneRowPerName` (PAR-498).
 */
describe("SitemapService.getAttractionsSitemap", () => {
  const phantasialand = {
    id: "park-1",
    slug: "phantasialand",
    continentSlug: "europe",
    countrySlug: "germany",
    citySlug: "bruehl",
  };

  function setup(rows?: unknown[]) {
    const conditions: string[] = [];
    const record = function (this: unknown, condition: string): unknown {
      conditions.push(condition);
      return this;
    };
    const qb = {
      select: jest.fn().mockReturnThis(),
      innerJoin: jest.fn().mockReturnThis(),
      where: jest.fn(record),
      andWhere: jest.fn(record),
      getMany: jest
        .fn()
        .mockResolvedValue(
          rows ?? [{ slug: "taron", name: "Taron", park: phantasialand }],
        ),
    };
    const repository = { createQueryBuilder: jest.fn(() => qb) };
    const redis = {
      get: jest.fn().mockResolvedValue(null),
      setex: jest.fn().mockResolvedValue("OK"),
    };
    const service = new SitemapService(repository as never, redis as never);
    return { service, conditions, redis };
  }

  it("keeps closed rides in the query and leaves reclassified rows out", async () => {
    const { service, conditions } = setup();

    await service.getAttractionsSitemap();

    expect(conditions).toContain(
      "(a.retiredAt IS NULL OR a.retiredReason IS NULL OR a.retiredReason NOT IN (:...reclassified))",
    );
  });

  it("lists a ride retired as closed", async () => {
    const { service } = setup([
      {
        slug: "x2",
        name: "X2",
        retiredAt: new Date("2026-07-13T00:00:00Z"),
        retiredReason:
          "https://park.fan/news/x2-six-flags-magic-mountain-closed",
        park: phantasialand,
      },
      { slug: "taron", name: "Taron", park: phantasialand },
    ]);

    const items = await service.getAttractionsSitemap();

    expect(items.map((item) => item.slug).sort()).toEqual(["taron", "x2"]);
  });

  it("never lets a closed row win a name group over a live ride", async () => {
    // `chooseNameDuplicateWinner` would pick the unsuffixed slug, which here is
    // the closed row. The payload serves the live one, so that is the URL.
    const { service } = setup([
      {
        slug: "raven",
        name: "Raven",
        retiredAt: new Date("2025-11-01T00:00:00Z"),
        retiredReason: "Replaced by a new coaster of the same name.",
        park: phantasialand,
      },
      { slug: "raven-2", name: "Raven", park: phantasialand },
    ]);

    const items = await service.getAttractionsSitemap();

    expect(items.map((item) => item.slug)).toEqual(["raven-2"]);
  });

  it("collapses closed rows that share a name", async () => {
    const { service } = setup([
      {
        slug: "dino-sue",
        name: "Dino-Sue",
        retiredAt: new Date("2026-02-01T00:00:00Z"),
        retiredReason: null,
        park: phantasialand,
      },
      {
        slug: "dino-sue-2",
        name: "Dino-Sue",
        retiredAt: new Date("2026-02-01T00:00:00Z"),
        retiredReason: null,
        park: phantasialand,
      },
    ]);

    const items = await service.getAttractionsSitemap();

    expect(items.map((item) => item.slug)).toEqual(["dino-sue"]);
  });

  it("still requires the park's full geo path", async () => {
    const { service, conditions } = setup();

    await service.getAttractionsSitemap();

    expect(conditions).toEqual(
      expect.arrayContaining([
        "p.continentSlug IS NOT NULL",
        "p.countrySlug IS NOT NULL",
        "p.citySlug IS NOT NULL",
      ]),
    );
  });

  it("lists one slug per attraction name, as the park payload serves it", async () => {
    const { service } = setup([
      { slug: "raven-2", name: "Raven", park: phantasialand },
      { slug: "raven", name: "Raven", park: phantasialand },
      { slug: "taron", name: "Taron", park: phantasialand },
    ]);

    const items = await service.getAttractionsSitemap();

    expect(items.map((item) => item.slug).sort()).toEqual(["raven", "taron"]);
  });

  it("groups per park, so two parks may both list the same slug", async () => {
    const walibi = { ...phantasialand, id: "park-2", slug: "walibi-belgium" };
    const { service } = setup([
      { slug: "vampire", name: "VAMPIRE", park: phantasialand },
      { slug: "vampire", name: "VAMPIRE", park: walibi },
    ]);

    const items = await service.getAttractionsSitemap();

    expect(items).toHaveLength(2);
  });

  it("groups on the curated name, which is the name the payload keys on", async () => {
    const { service } = setup([
      { slug: "nexus-ai", name: "NEXUS AI", park: phantasialand },
      {
        slug: "nexus-ai-2",
        name: "Something Else",
        curatedName: "NEXUS AI",
        park: phantasialand,
      },
    ]);

    const items = await service.getAttractionsSitemap();

    expect(items.map((item) => item.slug)).toEqual(["nexus-ai"]);
  });

  it("trims the name before grouping, as the park payload does", async () => {
    const { service } = setup([
      { slug: "excalibur", name: "Excalibur ", park: phantasialand },
      { slug: "excalibur-2", name: "Excalibur", park: phantasialand },
    ]);

    const items = await service.getAttractionsSitemap();

    expect(items.map((item) => item.slug)).toEqual(["excalibur"]);
  });

  it("leaves out a row with a blank name, which the payload never serves", async () => {
    const { service } = setup([
      { slug: "nameless", name: "   ", park: phantasialand },
      { slug: "taron", name: "Taron", park: phantasialand },
    ]);

    const items = await service.getAttractionsSitemap();

    expect(items.map((item) => item.slug)).toEqual(["taron"]);
  });

  it("serves the cached list without querying", async () => {
    const { service, redis } = setup();
    const cached = [{ url: "/v1/parks/x/y/z/p/attractions/a", slug: "a" }];
    redis.get.mockResolvedValueOnce(JSON.stringify(cached));

    await expect(service.getAttractionsSitemap()).resolves.toEqual(cached);
  });
});
