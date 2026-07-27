import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { DataSource } from "typeorm";
import { ParkMergeService } from "./park-merge.service";
import { Park } from "../entities/park.entity";
import { ScheduleEntry } from "../entities/schedule-entry.entity";
import { ExternalEntityMapping } from "../../database/entities/external-entity-mapping.entity";
import { ParkSlugAlias } from "../entities/park-slug-alias.entity";
import { REDIS_CLIENT } from "../../common/redis/redis.module";

/**
 * A merge deletes the losing park, and that park's path was live and indexed:
 * the Tampa row for Universal Islands of Adventure alone accounted for an
 * empty page in six locales. Deleting it without recording where it went
 * turns every one of those URLs into a 404 instead of a redirect.
 */
describe("ParkMergeService — the loser's path", () => {
  let service: ParkMergeService;

  const inserted: Record<string, unknown>[] = [];
  const manager = {
    findOne: jest.fn(),
    query: jest.fn().mockResolvedValue([]),
    delete: jest.fn().mockResolvedValue({}),
    createQueryBuilder: jest.fn(() => ({
      update: jest.fn().mockReturnThis(),
      set: jest.fn().mockReturnThis(),
      where: jest.fn().mockReturnThis(),
      execute: jest.fn().mockResolvedValue({ affected: 0 }),
      insert: jest.fn().mockReturnThis(),
      into: jest.fn().mockReturnThis(),
      values: jest.fn((v: Record<string, unknown>) => {
        inserted.push(v);
        return { orIgnore: () => ({ execute: async () => ({}) }) };
      }),
    })),
  };
  const dataSource = {
    transaction: jest.fn(async (fn: any) => fn(manager)),
    query: jest.fn().mockResolvedValue([]),
  };
  const redis = {
    keys: jest.fn().mockResolvedValue([]),
    del: jest.fn(),
    pipeline: jest.fn(() => ({ del: jest.fn(), exec: jest.fn() })),
  };

  const park = (over: Partial<Park>): Park =>
    ({
      continentSlug: "north-america",
      countrySlug: "united-states",
      slug: "universal-islands-of-adventure",
      ...over,
    }) as Park;

  beforeEach(async () => {
    jest.clearAllMocks();
    inserted.length = 0;
    manager.query.mockResolvedValue([]);

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ParkMergeService,
        { provide: getRepositoryToken(Park), useValue: {} },
        { provide: getRepositoryToken(ScheduleEntry), useValue: {} },
        { provide: getRepositoryToken(ExternalEntityMapping), useValue: {} },
        { provide: DataSource, useValue: dataSource },
        { provide: REDIS_CLIENT, useValue: redis },
      ],
    }).compile();

    service = module.get(ParkMergeService);
  });

  it("redirects the losing park's path to the winner", async () => {
    manager.findOne.mockImplementation(async (_e: unknown, opts: any) =>
      opts.where.id === "winner"
        ? park({ id: "winner", name: "IOA", citySlug: "orlando" })
        : park({ id: "loser", name: "IOA", citySlug: "tampa" }),
    );

    await service.mergeParks("winner", "loser");

    expect(inserted).toContainEqual(
      expect.objectContaining({
        parkId: "winner",
        citySlug: "tampa",
        slug: "universal-islands-of-adventure",
      }),
    );
  });

  it("records nothing when both parks shared a path", async () => {
    manager.findOne.mockImplementation(async () =>
      park({ id: "x", name: "IOA", citySlug: "orlando" }),
    );

    await service.mergeParks("winner", "loser");

    expect(
      inserted.filter((i) => "citySlug" in i),
    ).toHaveLength(0);
  });
});
