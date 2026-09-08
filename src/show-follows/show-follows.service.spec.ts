import { Test } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { ShowFollowsService } from "./show-follows.service";
import { ShowFollow } from "./entities/show-follow.entity";
import { Show } from "../shows/entities/show.entity";

/**
 * Simple CRUD, but two properties are worth pinning: `upsert` must be a true
 * no-op on an already-followed show (no second row, no write at all — unlike
 * `RideAlertsService.upsert`, which has a threshold to update), and
 * `findShowForFollow` must refuse a show with no resolvable park, the same
 * "never accept what can never notify" shape `RideAlertsService
 * .findAttractionForAlert` uses.
 */
describe("ShowFollowsService", () => {
  let followRows: Map<string, ShowFollow>;
  let showRows: Map<string, Show>;
  let followRepo: {
    find: jest.Mock;
    findOne: jest.Mock;
    count: jest.Mock;
    create: jest.Mock;
    save: jest.Mock;
    delete: jest.Mock;
  };
  let service: ShowFollowsService;

  beforeEach(async () => {
    followRows = new Map();
    showRows = new Map([
      [
        "show-1",
        { id: "show-1", name: "Feuerwerk", park: { id: "park-1" } } as Show,
      ],
      [
        "show-2",
        { id: "show-2", name: "Orphaned", park: null } as unknown as Show,
      ],
    ]);

    followRepo = {
      find: jest.fn(async (opts?: { where?: { subscriptionId?: string } }) =>
        [...followRows.values()].filter(
          (row) =>
            !opts?.where?.subscriptionId ||
            row.subscriptionId === opts.where.subscriptionId,
        ),
      ),
      findOne: jest.fn(
        async (opts: { where: { subscriptionId: string; showId: string } }) => {
          for (const row of followRows.values()) {
            if (
              row.subscriptionId === opts.where.subscriptionId &&
              row.showId === opts.where.showId
            )
              return row;
          }
          return null;
        },
      ),
      count: jest.fn(
        async (opts: { where: { subscriptionId: string } }) =>
          [...followRows.values()].filter(
            (row) => row.subscriptionId === opts.where.subscriptionId,
          ).length,
      ),
      create: jest.fn(
        (data: Partial<ShowFollow>) => ({ ...data }) as ShowFollow,
      ),
      save: jest.fn(async (row: ShowFollow) => {
        const stored = {
          ...row,
          id: row.id ?? `follow-${followRows.size + 1}`,
          createdAt: row.createdAt ?? new Date(),
        };
        followRows.set(stored.id, stored as ShowFollow);
        return stored;
      }),
      delete: jest.fn(
        async (criteria: { subscriptionId: string; showId: string }) => {
          for (const [id, row] of followRows) {
            if (
              row.subscriptionId === criteria.subscriptionId &&
              row.showId === criteria.showId
            ) {
              followRows.delete(id);
            }
          }
          return { affected: 1 };
        },
      ),
    };

    const showRepo = {
      findOne: jest.fn(
        async (opts: { where: { id: string } }) =>
          showRows.get(opts.where.id) ?? null,
      ),
    };

    const moduleRef = await Test.createTestingModule({
      providers: [
        ShowFollowsService,
        { provide: getRepositoryToken(ShowFollow), useValue: followRepo },
        { provide: getRepositoryToken(Show), useValue: showRepo },
      ],
    }).compile();

    service = moduleRef.get(ShowFollowsService);
  });

  it("resolves a show that has a park", async () => {
    const found = await service.findShowForFollow("show-1");
    expect(found?.park.id).toBe("park-1");
  });

  it("refuses a show with no park to build a link from", async () => {
    expect(await service.findShowForFollow("show-2")).toBeNull();
  });

  it("refuses a show that does not exist", async () => {
    expect(await service.findShowForFollow("gone")).toBeNull();
  });

  it("creates a follow on first upsert", async () => {
    const follow = await service.upsert("sub-1", "show-1");
    expect(follow.subscriptionId).toBe("sub-1");
    expect(follow.showId).toBe("show-1");
    expect(followRepo.save).toHaveBeenCalledTimes(1);
  });

  it("is a true no-op writing nothing when already followed", async () => {
    const first = await service.upsert("sub-1", "show-1");
    const second = await service.upsert("sub-1", "show-1");
    expect(second).toBe(first);
    expect(followRepo.save).toHaveBeenCalledTimes(1);
  });

  it("removes a follow idempotently", async () => {
    await service.upsert("sub-1", "show-1");
    await service.remove("sub-1", "show-1");
    await expect(service.remove("sub-1", "show-1")).resolves.toBeUndefined();
    expect(await service.find("sub-1", "show-1")).toBeNull();
  });

  it("lists only the requested subscription's follows", async () => {
    await service.upsert("sub-1", "show-1");
    await service.upsert("sub-2", "show-1");
    const list = await service.listForSubscription("sub-1");
    expect(list).toHaveLength(1);
    expect(list[0].subscriptionId).toBe("sub-1");
  });

  it("counts per-subscription independently of other subscriptions' follows", async () => {
    await service.upsert("sub-1", "show-1");
    await service.upsert("sub-2", "show-1");
    expect(await service.countForSubscription("sub-1")).toBe(1);
  });

  it("allFollows returns every follow for the notification job to group", async () => {
    await service.upsert("sub-1", "show-1");
    await service.upsert("sub-2", "show-1");
    expect(await service.allFollows()).toHaveLength(2);
  });
});
