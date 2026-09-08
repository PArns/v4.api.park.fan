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
    createQueryBuilder: jest.Mock;
  };
  let service: ShowFollowsService;
  let deleteCutoff: Date | undefined;

  beforeEach(async () => {
    followRows = new Map();
    deleteCutoff = undefined;
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
      // Stands in for `INSERT ... ON CONFLICT DO UPDATE SET "startTime"`:
      // inserts when no row matches `(subscriptionId, showId)`, and
      // otherwise overwrites that row's `startTime` — the same thing
      // Postgres does, and the reason a re-follow can move the reminder to a
      // different performance.
      createQueryBuilder: jest.fn(() => {
        let pending: Partial<ShowFollow> = {};
        // The sweep's chain: .delete().from().where().andWhere().execute() —
        // standing in for `DELETE ... WHERE "startTime" IS NOT NULL AND
        // "startTime" < $cutoff`, which is what the real query issues.
        const deleteBuilder: {
          from: () => typeof deleteBuilder;
          where: (sql: string) => typeof deleteBuilder;
          andWhere: (
            sql: string,
            params: { cutoff: Date },
          ) => typeof deleteBuilder;
          execute: () => Promise<{ affected: number }>;
        } = {
          from: jest.fn(() => deleteBuilder),
          where: jest.fn(() => deleteBuilder),
          andWhere: jest.fn((_sql: string, params: { cutoff: Date }) => {
            deleteCutoff = params.cutoff;
            return deleteBuilder;
          }),
          execute: jest.fn(async () => {
            let affected = 0;
            for (const [id, row] of [...followRows]) {
              if (
                row.startTime &&
                deleteCutoff &&
                row.startTime < deleteCutoff
              ) {
                followRows.delete(id);
                affected += 1;
              }
            }
            return { affected };
          }),
        };
        const builder: {
          insert: () => typeof builder;
          delete: () => typeof deleteBuilder;
          into: () => typeof builder;
          values: (vals: Partial<ShowFollow>) => typeof builder;
          orUpdate: (cols: string[], conflict: string[]) => typeof builder;
          execute: () => Promise<{
            raw: unknown[];
            identifiers: unknown[];
            generatedMaps: unknown[];
          }>;
        } = {
          insert: jest.fn(() => builder),
          delete: jest.fn(() => deleteBuilder),
          into: jest.fn(() => builder),
          values: jest.fn((vals: Partial<ShowFollow>) => {
            pending = vals;
            return builder;
          }),
          orUpdate: jest.fn(() => builder),
          execute: jest.fn(async () => {
            const existing = [...followRows.values()].find(
              (row) =>
                row.subscriptionId === pending.subscriptionId &&
                row.showId === pending.showId,
            );
            if (existing) {
              existing.startTime = pending.startTime ?? null;
            } else {
              const stored = {
                id: `follow-${followRows.size + 1}`,
                createdAt: new Date(),
                updatedAt: new Date(),
                startTime: null,
                ...pending,
              };
              followRows.set(stored.id, stored as ShowFollow);
            }
            return { raw: [], identifiers: [], generatedMaps: [] };
          }),
        };
        return builder;
      }),
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
    const follow = await service.upsert("sub-1", "show-1", null);
    expect(follow.subscriptionId).toBe("sub-1");
    expect(follow.showId).toBe("show-1");
  });

  it("is a true no-op leaving the existing row untouched when already followed", async () => {
    // Both calls issue the same `INSERT ... ON CONFLICT DO UPDATE` — the
    // no-op happens at the database's conflict resolution, not by skipping
    // the attempt, which is what makes concurrent double-follows safe.
    const first = await service.upsert("sub-1", "show-1", null);
    const second = await service.upsert("sub-1", "show-1", null);
    expect(second).toBe(first);
    expect(followRepo.createQueryBuilder).toHaveBeenCalledTimes(2);
    expect(followRows.size).toBe(1);
  });

  it("never 500s on two concurrent upserts of the same follow", async () => {
    // The property the old read-then-write shape could not guarantee: both
    // calls racing (unresolved together, not sequentially awaited) settle
    // without either one throwing.
    const [first, second] = await Promise.all([
      service.upsert("sub-1", "show-1", null),
      service.upsert("sub-1", "show-1", null),
    ]);
    expect(first.showId).toBe("show-1");
    expect(second.showId).toBe("show-1");
    expect(followRows.size).toBe(1);
  });

  it("removes a follow idempotently", async () => {
    await service.upsert("sub-1", "show-1", null);
    await service.remove("sub-1", "show-1");
    await expect(service.remove("sub-1", "show-1")).resolves.toBeUndefined();
    expect(await service.find("sub-1", "show-1")).toBeNull();
  });

  it("lists only the requested subscription's follows", async () => {
    await service.upsert("sub-1", "show-1", null);
    await service.upsert("sub-2", "show-1", null);
    const list = await service.listForSubscription("sub-1");
    expect(list).toHaveLength(1);
    expect(list[0].subscriptionId).toBe("sub-1");
  });

  it("counts per-subscription independently of other subscriptions' follows", async () => {
    await service.upsert("sub-1", "show-1", null);
    await service.upsert("sub-2", "show-1", null);
    expect(await service.countForSubscription("sub-1")).toBe(1);
  });

  it("allFollows returns every follow for the notification job to group", async () => {
    await service.upsert("sub-1", "show-1", null);
    await service.upsert("sub-2", "show-1", null);
    expect(await service.allFollows()).toHaveLength(2);
  });

  describe("sweepExpired", () => {
    const HOUR = 60 * 60 * 1000;
    const NOW = new Date("2026-09-10T12:00:00.000Z");

    const seed = (id: string, startTime: Date | null) => {
      followRows.set(id, {
        id,
        subscriptionId: "sub-1",
        showId: "show-1",
        startTime,
        createdAt: new Date(),
        updatedAt: new Date(),
      } as ShowFollow);
    };

    it("removes a follow whose performance is well past", () => {
      seed("old", new Date(NOW.getTime() - 48 * HOUR));
      return service.sweepExpired(NOW).then((removed) => {
        expect(removed).toBe(1);
        expect(followRows.size).toBe(0);
      });
    });

    it("leaves an open-ended follow alone — it has no performance to expire", async () => {
      seed("open", null);
      const removed = await service.sweepExpired(NOW);
      expect(removed).toBe(0);
      expect(followRows.has("open")).toBe(true);
    });

    it("leaves a future performance alone", async () => {
      seed("later", new Date(NOW.getTime() + 3 * HOUR));
      const removed = await service.sweepExpired(NOW);
      expect(removed).toBe(0);
      expect(followRows.has("later")).toBe(true);
    });

    it("keeps a performance inside the grace period, so a tick still working on it is not cut off", async () => {
      seed("justOver", new Date(NOW.getTime() - 2 * HOUR));
      const removed = await service.sweepExpired(NOW);
      expect(removed).toBe(0);
      expect(followRows.has("justOver")).toBe(true);
    });

    it("sweeps only what is due, leaving the rest", async () => {
      seed("old", new Date(NOW.getTime() - 72 * HOUR));
      seed("open", null);
      seed("later", new Date(NOW.getTime() + HOUR));
      const removed = await service.sweepExpired(NOW);
      expect(removed).toBe(1);
      expect([...followRows.keys()].sort()).toEqual(["later", "open"]);
    });
  });
});
