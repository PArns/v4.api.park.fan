import { Repository } from "typeorm";
import { Redis } from "ioredis";
import { ParkRenameService, captureParkPath } from "./park-rename.service";
import { ParkSlugAlias } from "../entities/park-slug-alias.entity";
import { Park } from "../entities/park.entity";
import { CacheKeys } from "../../common/cache/cache-keys";

/**
 * ParkRenameService is what keeps an upstream park rename from silently 404ing the park.
 *
 * The bug it fixes: the metadata processor regenerates `park.slug` whenever a source reports a
 * new name (ThemeParks Wiki wins), but nothing invalidated the 24 h discovery geo cache and
 * nothing remembered the old path. The API's own structure endpoint therefore advertised URLs
 * its park endpoint answered with 404 — that is how "Attractiepark Toverland" → "Toverland"
 * and "Magic Kingdom Park" → "Disney Magic Kingdom" took those parks off the site.
 *
 * These tests pin the three guarantees that prevent a repeat:
 *   1. A changed path evicts the discovery geo skeleton (the key the frontend builds every
 *      park link, redirect and sitemap entry from).
 *   2. A changed path is recorded so the old URL can 301 instead of 404.
 *   3. An unchanged path does neither — the processor calls this on every save.
 */
describe("ParkRenameService", () => {
  const makePark = (over: Partial<Park> = {}): Park =>
    ({
      id: "park-1",
      name: "Toverland",
      continentSlug: "europe",
      countrySlug: "netherlands",
      citySlug: "sevenum",
      slug: "toverland",
      ...over,
    }) as unknown as Park;

  const previousPath = {
    continentSlug: "europe",
    countrySlug: "netherlands",
    citySlug: "sevenum",
    slug: "attractiepark-toverland",
  };

  let insertValues: jest.Mock;
  let aliasRepository: jest.Mocked<Repository<ParkSlugAlias>>;
  let redis: jest.Mocked<Redis>;
  let service: ParkRenameService;

  beforeEach(() => {
    insertValues = jest.fn().mockReturnValue({
      orIgnore: () => ({ execute: jest.fn().mockResolvedValue(undefined) }),
    });

    aliasRepository = {
      delete: jest.fn().mockResolvedValue(undefined),
      findOne: jest.fn().mockResolvedValue(null),
      createQueryBuilder: jest.fn().mockReturnValue({
        insert: () => ({ into: () => ({ values: insertValues }) }),
      }),
    } as unknown as jest.Mocked<Repository<ParkSlugAlias>>;

    redis = {
      del: jest.fn().mockResolvedValue(1),
      keys: jest.fn().mockResolvedValue([]),
      scan: jest.fn().mockResolvedValue(["0", []]),
    } as unknown as jest.Mocked<Redis>;

    // The park repository is only used by the startup backfill, which these tests don't run.
    const parkRepository = {
      findOne: jest.fn().mockResolvedValue(null),
    } as unknown as jest.Mocked<Repository<Park>>;

    service = new ParkRenameService(aliasRepository, redis, parkRepository);
  });

  describe("handlePathChange", () => {
    it("evicts the discovery geo skeleton when the slug changed", async () => {
      await service.handlePathChange(makePark(), previousPath);

      const evicted = redis.del.mock.calls.flat();
      expect(evicted).toContain(CacheKeys.discoveryGeoStructure());
    });

    it("records the previous path so the old URL can redirect", async () => {
      await service.handlePathChange(makePark(), previousPath);

      expect(insertValues).toHaveBeenCalledWith({
        parkId: "park-1",
        ...previousPath,
      });
    });

    it("clears a stale alias occupying the park's new path", async () => {
      // Upstream names flip back; without this the park would alias to itself.
      await service.handlePathChange(makePark(), previousPath);

      expect(aliasRepository.delete).toHaveBeenCalledWith({
        continentSlug: "europe",
        countrySlug: "netherlands",
        citySlug: "sevenum",
        slug: "toverland",
      });
    });

    it("does nothing when the path is unchanged", async () => {
      const park = makePark();

      await service.handlePathChange(park, captureParkPath(park));

      expect(insertValues).not.toHaveBeenCalled();
      expect(redis.del).not.toHaveBeenCalled();
    });

    it("also fires when only a geo slug changed (city re-slug)", async () => {
      await service.handlePathChange(
        makePark({ citySlug: "sevenum-limburg" }),
        {
          ...previousPath,
          slug: "toverland",
        },
      );

      expect(insertValues).toHaveBeenCalled();
      expect(redis.del.mock.calls.flat()).toContain(
        CacheKeys.discoveryGeoStructure(),
      );
    });

    it("never throws when the bookkeeping fails", async () => {
      aliasRepository.delete.mockRejectedValueOnce(new Error("db down"));
      redis.del.mockRejectedValueOnce(new Error("redis down"));

      await expect(
        service.handlePathChange(makePark(), previousPath),
      ).resolves.toBeUndefined();
    });
  });

  describe("resolveAlias", () => {
    it("returns the park's current path for a recorded old one", async () => {
      aliasRepository.findOne.mockResolvedValueOnce({
        park: makePark(),
      } as unknown as ParkSlugAlias);

      await expect(service.resolveAlias(previousPath)).resolves.toEqual({
        continentSlug: "europe",
        countrySlug: "netherlands",
        citySlug: "sevenum",
        slug: "toverland",
      });
    });

    it("returns null for a path that was never a park's", async () => {
      await expect(service.resolveAlias(previousPath)).resolves.toBeNull();
    });

    it("returns null when the alias would redirect to itself", async () => {
      // Guards against an infinite redirect if a stale row survives.
      aliasRepository.findOne.mockResolvedValueOnce({
        park: makePark({ slug: "attractiepark-toverland" }),
      } as unknown as ParkSlugAlias);

      await expect(service.resolveAlias(previousPath)).resolves.toBeNull();
    });
  });
});
