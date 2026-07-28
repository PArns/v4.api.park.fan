import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { ParkRenameService } from "./park-rename.service";
import { Park } from "../entities/park.entity";
import { ParkSlugAlias } from "../entities/park-slug-alias.entity";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { RevalidationService } from "../../common/revalidation/revalidation.service";

/**
 * Universal Studios Hollywood survived its merge on the row whose geocode had
 * failed: city "Bull Creek" at 28.0/-81.0, smooth-rounded coordinates in
 * central Florida for a park in California. The winner is the right row on
 * every other measure — it holds the wiki entity id, the destination link and
 * the deeper history — so the fix is to correct the location rather than to
 * have merged the other way.
 *
 * Changing citySlug changes the park's public path, so it has to run through
 * the same alias + revalidation machinery as an upstream rename; otherwise the
 * already-indexed /bull-creek/ URLs 404 with nothing pointing anywhere.
 */
describe("ParkRenameService.correctLocation", () => {
  let service: ParkRenameService;

  const parkRepository = { findOne: jest.fn(), save: jest.fn() };
  const insertedAliases: Record<string, unknown>[] = [];
  const aliasRepository = {
    findOne: jest.fn().mockResolvedValue(null),
    find: jest.fn().mockResolvedValue([]),
    delete: jest.fn().mockResolvedValue({}),
    createQueryBuilder: jest.fn(() => ({
      insert: jest.fn().mockReturnThis(),
      into: jest.fn().mockReturnThis(),
      values: jest.fn((v: Record<string, unknown>) => {
        insertedAliases.push(v);
        return { orIgnore: () => ({ execute: async () => ({}) }) };
      }),
    })),
  };
  const revalidation = { revalidateTags: jest.fn() };
  const redis = {
    keys: jest.fn().mockResolvedValue([]),
    del: jest.fn(),
    pipeline: jest.fn(() => ({ del: jest.fn(), exec: jest.fn() })),
  };

  const ush = (): Park =>
    ({
      id: "ush",
      name: "Universal Studios Hollywood",
      slug: "universal-studios-hollywood",
      continentSlug: "north-america",
      countrySlug: "united-states",
      city: "Bull Creek",
      citySlug: "bull-creek",
      latitude: 28.0,
      longitude: -81.0,
    }) as Park;

  beforeEach(async () => {
    jest.clearAllMocks();
    insertedAliases.length = 0;
    parkRepository.save.mockImplementation(async (p: unknown) => p);

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ParkRenameService,
        { provide: getRepositoryToken(Park), useValue: parkRepository },
        {
          provide: getRepositoryToken(ParkSlugAlias),
          useValue: aliasRepository,
        },
        { provide: REDIS_CLIENT, useValue: redis },
        { provide: RevalidationService, useValue: revalidation },
      ],
    }).compile();

    service = module.get(ParkRenameService);
  });

  it("writes the corrected city and coordinates", async () => {
    const park = ush();
    parkRepository.findOne.mockResolvedValue(park);

    await service.correctLocation("ush", {
      city: "Los Angeles",
      latitude: 34.137261,
      longitude: -118.355516,
    });

    expect(parkRepository.save).toHaveBeenCalledWith(
      expect.objectContaining({
        city: "Los Angeles",
        citySlug: "los-angeles",
        latitude: 34.137261,
        longitude: -118.355516,
      }),
    );
  });

  it("keeps the old path alive as an alias", async () => {
    parkRepository.findOne.mockResolvedValue(ush());

    await service.correctLocation("ush", {
      city: "Los Angeles",
      latitude: 34.137261,
      longitude: -118.355516,
    });

    expect(insertedAliases).toContainEqual(
      expect.objectContaining({ citySlug: "bull-creek", parkId: "ush" }),
    );
  });

  it("tells the frontend the path moved", async () => {
    parkRepository.findOne.mockResolvedValue(ush());

    await service.correctLocation("ush", {
      city: "Los Angeles",
      latitude: 34.137261,
      longitude: -118.355516,
    });

    expect(revalidation.revalidateTags).toHaveBeenCalled();
  });

  it("does not record an alias when only the coordinates change", async () => {
    parkRepository.findOne.mockResolvedValue(ush());

    await service.correctLocation("ush", {
      latitude: 34.137261,
      longitude: -118.355516,
    });

    expect(parkRepository.save).toHaveBeenCalled();
    expect(insertedAliases).toHaveLength(0);
  });

  it("refuses an unknown park", async () => {
    parkRepository.findOne.mockResolvedValue(null);

    await expect(
      service.correctLocation("nope", { city: "X" }),
    ).rejects.toThrow(/not found/i);
  });
});
