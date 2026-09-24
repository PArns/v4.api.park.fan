import { SitemapService } from "./sitemap.service";

/**
 * The attraction sitemap has to list exactly the rides a park page can render.
 *
 * The park payload drops retired rides (`ParksService.loadParkRelations`), and
 * the frontend resolves a ride page from that payload, so a retired ride's URL
 * is a 404. This query did not carry the filter: on 2026-09-24 it listed 70
 * retired rides, 420 sitemap URLs across the six locales that all 404ed.
 */
describe("SitemapService.getAttractionsSitemap", () => {
  function setup() {
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
      getMany: jest.fn().mockResolvedValue([
        {
          slug: "taron",
          park: {
            slug: "phantasialand",
            continentSlug: "europe",
            countrySlug: "germany",
            citySlug: "bruehl",
          },
        },
      ]),
    };
    const repository = { createQueryBuilder: jest.fn(() => qb) };
    const redis = {
      get: jest.fn().mockResolvedValue(null),
      setex: jest.fn().mockResolvedValue("OK"),
    };
    const service = new SitemapService(repository as never, redis as never);
    return { service, conditions, redis };
  }

  it("leaves retired rides out, as the park payload does", async () => {
    const { service, conditions } = setup();

    await service.getAttractionsSitemap();

    expect(conditions).toContain("a.retiredAt IS NULL");
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

  it("serves the cached list without querying", async () => {
    const { service, redis } = setup();
    const cached = [{ url: "/v1/parks/x/y/z/p/attractions/a", slug: "a" }];
    redis.get.mockResolvedValueOnce(JSON.stringify(cached));

    await expect(service.getAttractionsSitemap()).resolves.toEqual(cached);
  });
});
