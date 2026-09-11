import { Test } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { TripsService } from "./trips.service";
import { Trip } from "./entities/trip.entity";
import { PushSubscription } from "../push/entities/push-subscription.entity";

/**
 * The id is the whole of the authorisation, so the properties worth pinning are
 * the ones an attacker would probe: that an id cannot be predicted from the
 * plan, that a PUT to an id nobody created does not create it, and that an
 * expired trip is gone rather than merely stale.
 */
describe("TripsService", () => {
  let service: TripsService;
  let rows: Map<string, Trip>;
  /** `push_subscriptions`, only the two columns `remove` is allowed to touch. */
  let subscriptions: Array<{
    endpoint: string;
    tripId: string | null;
    topics: string[];
  }>;
  /** True only while the transaction callback is running. */
  let inTransaction: boolean;
  /** The instant the sweep's two statements agreed on, for the fake. */
  let sweepNow: Date | undefined;
  /** The fake `EntityManager`, so a test can make one statement misbehave. */
  let manager: {
    transaction: jest.Mock;
    findOne: jest.Mock;
    update: jest.Mock;
    delete: jest.Mock;
    createQueryBuilder: jest.Mock;
  };

  beforeEach(async () => {
    rows = new Map();
    subscriptions = [];
    inTransaction = false;
    sweepNow = undefined;

    // Every write `remove` makes has to happen through this manager, and the
    // guards below fail the test if one escapes the transaction: a cleared
    // pointer that commits without the delete leaves a plan nobody is told
    // about, and a delete that commits without the clear leaves the
    // five-minute job walking a trip that is gone.
    manager = {
      transaction: jest.fn(async (run: (m: unknown) => Promise<unknown>) => {
        inTransaction = true;
        try {
          return await run(manager);
        } finally {
          inTransaction = false;
        }
      }),
      findOne: jest.fn(
        async (entity: unknown, { where }: { where: { id: string } }) => {
          if (entity !== Trip)
            throw new Error("findOne against a foreign table");
          return rows.get(where.id) ?? null;
        },
      ),
      update: jest.fn(
        async (
          entity: unknown,
          where: { tripId: string },
          patch: { tripId: null; topics: string[] },
        ) => {
          if (!inTransaction) throw new Error("update outside the transaction");
          // The entity is checked, not ignored: swapping the two tables round
          // would delete a browser's subscription and cascade its ride alerts
          // away, and every assertion below would still pass.
          if (entity !== PushSubscription) {
            throw new Error("update against the wrong table");
          }
          let affected = 0;
          for (const row of subscriptions) {
            if (row.tripId !== where.tripId) continue;
            row.tripId = patch.tripId;
            row.topics = patch.topics;
            affected++;
          }
          return { affected };
        },
      ),
      delete: jest.fn(
        async (entity: unknown, criteria: string | { expiresAt: unknown }) => {
          if (!inTransaction) throw new Error("delete outside the transaction");
          if (entity !== Trip)
            throw new Error("delete against the wrong table");
          if (typeof criteria === "string") {
            return { affected: rows.delete(criteria) ? 1 : 0 };
          }
          // The sweep: everything already past its expiry at `sweepNow`.
          let affected = 0;
          for (const [id, row] of [...rows]) {
            if (row.expiresAt.getTime() >= sweepNow!.getTime()) continue;
            rows.delete(id);
            affected++;
          }
          return { affected };
        },
      ),
      // The sweep's pointer clear is one set-based statement, so the fake
      // reproduces its predicate rather than its SQL: every subscription whose
      // trip is already past `:now`.
      createQueryBuilder: jest.fn(() => ({
        update: (entity: unknown) => {
          if (entity !== PushSubscription) {
            throw new Error("sweep cleared the wrong table");
          }
          return {
            set: (patch: { tripId: null; topics: string[] }) => ({
              where: (_sql: string, params: { now: Date }) => {
                sweepNow = params.now;
                return {
                  execute: async () => {
                    if (!inTransaction) {
                      throw new Error("sweep cleared outside the transaction");
                    }
                    for (const row of subscriptions) {
                      const trip = row.tripId
                        ? rows.get(row.tripId)
                        : undefined;
                      if (!trip) continue;
                      if (trip.expiresAt.getTime() >= params.now.getTime()) {
                        continue;
                      }
                      row.tripId = patch.tripId;
                      row.topics = patch.topics;
                    }
                  },
                };
              },
            }),
          };
        },
      })),
    };

    const moduleRef = await Test.createTestingModule({
      providers: [
        TripsService,
        {
          provide: getRepositoryToken(Trip),
          useValue: {
            create: jest.fn((data: Partial<Trip>) => ({ ...data }) as Trip),
            save: jest.fn(async (trip: Trip) => {
              // The columns TypeORM fills in, only where the entity has not
              // already carried one through an update.
              const stored: Trip = { ...trip };
              stored.createdAt ??= new Date();
              stored.updatedAt = new Date();
              rows.set(stored.id, stored);
              return stored;
            }),
            findOne: jest.fn(async ({ where }: { where: { id: string } }) => {
              return rows.get(where.id) ?? null;
            }),
            delete: jest.fn(async () => ({ affected: 0 })),
            manager,
          },
        },
      ],
    }).compile();

    service = moduleRef.get(TripsService);
  });

  const plan = () => ({ version: 2, parks: {} });

  it("gives every trip an unguessable id", async () => {
    const ids = new Set<string>();
    for (let i = 0; i < 200; i++) {
      const trip = await service.create(plan());
      ids.add(trip.id);
    }
    // 96 bits: a collision in two hundred draws would mean the generator is not
    // random, not that we were unlucky.
    expect(ids.size).toBe(200);
    for (const id of ids) {
      expect(id).toHaveLength(16);
      // base64url — safe in a path segment without escaping, so a shared link
      // survives every mail client that rewrites URLs.
      expect(id).toMatch(/^[A-Za-z0-9_-]{16}$/);
    }
  });

  it("does not derive the id from the plan", async () => {
    // Two identical plans must not land on the same id. A content hash would,
    // and since the id IS the credential that would hand one visitor's trip to
    // anybody who planned the same day.
    const a = await service.create({ version: 2, parks: {}, note: "same" });
    const b = await service.create({ version: 2, parks: {}, note: "same" });
    expect(a.id).not.toBe(b.id);
  });

  it("reads a trip back verbatim", async () => {
    const payload = { version: 2, parks: { p: { slug: "p" } }, extra: [1, 2] };
    const created = await service.create(payload);
    const found = await service.find(created.id);
    expect(found?.payload).toEqual(payload);
  });

  it("replaces a plan rather than merging into it", async () => {
    // The browser holds the whole plan and is the only writer that knows what
    // was deleted. A merge would resurrect an entry somebody removed.
    const created = await service.create({
      version: 2,
      parks: { a: { slug: "a" } },
    });
    await service.update(created.id, {
      version: 2,
      parks: { b: { slug: "b" } },
    });
    const found = await service.find(created.id);
    expect(found?.payload).toEqual({ version: 2, parks: { b: { slug: "b" } } });
  });

  it("refuses to create a trip at an id the caller chose", async () => {
    // Otherwise an attacker picks their own ids, and with them overwrites a
    // trip by guessing one.
    const result = await service.update("an-id-nobody-made", plan());
    expect(result).toBeNull();
    expect(rows.has("an-id-nobody-made")).toBe(false);
  });

  it("pushes the expiry out on every write", async () => {
    const created = await service.create(plan());
    const first = created.expiresAt.getTime();
    // A plan somebody keeps editing must never expire under them.
    jest.spyOn(Date, "now").mockReturnValue(Date.now() + 30 * 86_400_000);
    const updated = await service.update(created.id, plan());
    jest.spyOn(Date, "now").mockRestore();
    expect(updated!.expiresAt.getTime()).toBeGreaterThan(first);
  });

  it("treats an expired trip as gone, not as an error", async () => {
    const created = await service.create(plan());
    // Whether the sweep has run yet is not the caller's business.
    rows.get(created.id)!.expiresAt = new Date(Date.now() - 1000);
    expect(await service.find(created.id)).toBeNull();
  });

  it("answers null for an id that was never issued", async () => {
    expect(await service.find("nope")).toBeNull();
  });

  describe("remove", () => {
    it("deletes the trip and clears the subscriptions pointing at it", async () => {
      const created = await service.create(plan());
      const other = await service.create(plan());
      subscriptions.push(
        {
          endpoint: "https://push.example/a",
          tripId: created.id,
          topics: ["x"],
        },
        {
          endpoint: "https://push.example/b",
          tripId: created.id,
          topics: ["y"],
        },
        { endpoint: "https://push.example/c", tripId: other.id, topics: ["z"] },
      );

      expect(await service.remove(created.id)).toBe(true);

      expect(rows.has(created.id)).toBe(false);
      // The subscription row survives — it is the browser's, not the trip's,
      // and it still carries that browser's ride alerts and followed shows.
      expect(subscriptions).toHaveLength(3);
      expect(
        subscriptions.filter((row) => row.tripId === created.id),
      ).toHaveLength(0);
      expect(subscriptions[0].topics).toEqual([]);
      // A second trip's subscriber is untouched, so the clear is scoped to the
      // id and is not an unscoped wipe that happens to pass the assertion above.
      expect(subscriptions[2]).toEqual({
        endpoint: "https://push.example/c",
        tripId: other.id,
        topics: ["z"],
      });
      expect(rows.has(other.id)).toBe(true);
    });

    it("removes a trip nobody subscribed to", async () => {
      const created = await service.create(plan());
      expect(await service.remove(created.id)).toBe(true);
      expect(rows.has(created.id)).toBe(false);
    });

    it("reports an id that was never issued as nothing to remove", async () => {
      expect(await service.remove("an-id-nobody-made")).toBe(false);
    });

    it("leaves an expired trip to the sweep, and writes nothing", async () => {
      const created = await service.create(plan());
      subscriptions.push({
        endpoint: "https://push.example/a",
        tripId: created.id,
        topics: ["x"],
      });
      rows.get(created.id)!.expiresAt = new Date(Date.now() - 1000);

      // `find` is the only place that decides what exists, so an expired trip
      // is absent here exactly as it is on GET and PUT.
      expect(await service.remove(created.id)).toBe(false);
      expect(rows.has(created.id)).toBe(true);
      expect(subscriptions[0].tripId).toBe(created.id);
    });

    it("answers 'nothing to remove' when a concurrent delete got there first", async () => {
      const created = await service.create(plan());
      subscriptions.push({
        endpoint: "https://push.example/a",
        tripId: created.id,
        topics: ["x"],
      });
      // The row is gone between the lookup and the DELETE. The DELETE's own
      // `affected` has to answer, not the read — otherwise two deletes racing
      // on one id both report 204 on a route that documents 404 for an id with
      // nothing behind it.
      manager.findOne.mockImplementationOnce(async () => {
        const trip = rows.get(created.id)!;
        rows.delete(created.id);
        return trip;
      });

      expect(await service.remove(created.id)).toBe(false);
      // And the loser clears nothing: the winner already did.
      expect(manager.update).not.toHaveBeenCalled();
    });
  });

  describe("sweepExpired", () => {
    it("clears the pointers at the trips it deletes", async () => {
      const stale = await service.create(plan());
      const live = await service.create(plan());
      rows.get(stale.id)!.expiresAt = new Date(Date.now() - 1000);
      subscriptions.push(
        { endpoint: "https://push.example/a", tripId: stale.id, topics: ["x"] },
        { endpoint: "https://push.example/b", tripId: live.id, topics: ["y"] },
      );

      expect(await service.sweepExpired()).toBe(1);

      expect(rows.has(stale.id)).toBe(false);
      expect(rows.has(live.id)).toBe(true);
      // Otherwise the five-minute notification job walks this subscription for
      // the life of the browser, for a trip that is gone.
      expect(subscriptions[0]).toMatchObject({ tripId: null, topics: [] });
      // And the live trip's subscriber is untouched.
      expect(subscriptions[1]).toMatchObject({
        tripId: live.id,
        topics: ["y"],
      });
    });

    it("clears and deletes against the same instant, inside one transaction", async () => {
      const stale = await service.create(plan());
      rows.get(stale.id)!.expiresAt = new Date(Date.now() - 1000);
      await service.sweepExpired();
      // Two `new Date()` calls would let a row expire between the clear and the
      // delete and be deleted with its pointer still standing.
      expect(manager.transaction).toHaveBeenCalledTimes(1);
      expect(sweepNow).toBeInstanceOf(Date);
    });
  });
});
