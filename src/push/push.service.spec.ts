import { Test } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { In } from "typeorm";
import { PushService } from "./push.service";
import { PushSubscription } from "./entities/push-subscription.entity";

/**
 * The property worth pinning here is the one a naive nullable-ification would
 * have broken silently: this table now serves three unrelated reasons to
 * subscribe (a trip, a followed show, a ride's wait time), and each call only
 * knows about its own reason. `subscribe` has to leave the other two alone —
 * see the docstring on `SubscribeInput` for why.
 */
describe("PushService", () => {
  let service: PushService;
  let rows: Map<string, PushSubscription>;
  let findBy: jest.Mock;

  // `await`s the callback INSIDE the try — not `return run()` — because a
  // callback with more than one `await` in it would otherwise see `finally`
  // restore the env vars after only its first `await` suspends, not after it
  // actually finishes. `subscribe()` only reads `isPushConfigured()` once at
  // its top, so a single-call test never notices; a test that calls it twice
  // does, since the second call runs once the env vars are already gone.
  const withVapid = async <T>(run: () => Promise<T> | T): Promise<T> => {
    const before = {
      pub: process.env.VAPID_PUBLIC_KEY,
      priv: process.env.VAPID_PRIVATE_KEY,
      sub: process.env.VAPID_SUBJECT,
    };
    process.env.VAPID_PUBLIC_KEY = "test-public";
    process.env.VAPID_PRIVATE_KEY = "test-private";
    process.env.VAPID_SUBJECT = "mailto:hello@park.fan";
    try {
      return await run();
    } finally {
      restore("VAPID_PUBLIC_KEY", before.pub);
      restore("VAPID_PRIVATE_KEY", before.priv);
      restore("VAPID_SUBJECT", before.sub);
    }
  };

  const restore = (key: string, value: string | undefined) => {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  };

  beforeEach(async () => {
    rows = new Map();
    let nextId = 1;
    // Real filtering is TypeORM's job, not this fake's — it just hands back
    // everything currently stored, and a separate test checks the criteria
    // `findByIds` builds (`{ id: In(ids) }`) rather than re-simulating `In`.
    findBy = jest.fn(async () => [...rows.values()]);

    const moduleRef = await Test.createTestingModule({
      providers: [
        PushService,
        {
          provide: getRepositoryToken(PushSubscription),
          useValue: {
            create: jest.fn(
              (data: Partial<PushSubscription>) =>
                ({ ...data }) as PushSubscription,
            ),
            findOne: jest.fn(
              async ({ where }: { where: { endpoint: string } }) => {
                for (const row of rows.values()) {
                  if (row.endpoint === where.endpoint) return row;
                }
                return null;
              },
            ),
            findBy,
            save: jest.fn(async (row: PushSubscription) => {
              const stored: PushSubscription = {
                ...row,
                id: row.id ?? `sub-${nextId++}`,
                createdAt: row.createdAt ?? new Date(),
                updatedAt: new Date(),
              };
              rows.set(stored.id, stored);
              return stored;
            }),
            delete: jest.fn(async () => ({ affected: 0 })),
          },
        },
      ],
    }).compile();

    service = moduleRef.get(PushService);
    restore("VAPID_PUBLIC_KEY", undefined);
    restore("VAPID_PRIVATE_KEY", undefined);
    restore("VAPID_SUBJECT", undefined);
  });

  const base = {
    endpoint: "https://fcm.googleapis.com/fcm/send/e1",
    p256dh: "key",
    auth: "secret",
    locale: "de",
    timezone: "Europe/Berlin",
  };

  it("stores nothing when push is not configured", async () => {
    expect(await service.subscribe({ ...base, tripId: "t1" })).toBeNull();
    expect(rows.size).toBe(0);
  });

  it("creates a trip-free row when tripId and topics are both omitted", async () => {
    await withVapid(async () => {
      const stored = await service.subscribe({ ...base });
      expect(stored?.tripId).toBeNull();
      expect(stored?.topics).toEqual([]);
    });
  });

  it("stores tripId and topics when the trip planner sends them", async () => {
    await withVapid(async () => {
      const stored = await service.subscribe({
        ...base,
        tripId: "t1",
        topics: ["next-up"],
      });
      expect(stored?.tripId).toBe("t1");
      expect(stored?.topics).toEqual(["next-up"]);
    });
  });

  it("never erases an existing tripId when a later call omits it", async () => {
    await withVapid(async () => {
      await service.subscribe({ ...base, tripId: "t1", topics: ["next-up"] });
      // A ride-alert or show-follow subscribe against the same browser: same
      // endpoint, no tripId, no topics.
      const stored = await service.subscribe({ ...base });
      expect(stored?.tripId).toBe("t1");
      expect(stored?.topics).toEqual(["next-up"]);
    });
  });

  it("never erases existing topics when a later call omits them", async () => {
    await withVapid(async () => {
      await service.subscribe({ ...base, tripId: "t1", topics: ["next-up"] });
      const stored = await service.subscribe({ ...base, tripId: "t1" });
      expect(stored?.topics).toEqual(["next-up"]);
    });
  });

  it("does still update tripId/topics when a later call sends new ones", async () => {
    await withVapid(async () => {
      await service.subscribe({ ...base });
      const stored = await service.subscribe({
        ...base,
        tripId: "t2",
        topics: ["next-up"],
      });
      expect(stored?.tripId).toBe("t2");
      expect(stored?.topics).toEqual(["next-up"]);
    });
  });

  it("resets failureCount on every (re-)subscribe", async () => {
    await withVapid(async () => {
      const first = await service.subscribe({ ...base });
      first!.failureCount = 5;
      rows.set(first!.id, first!);
      const again = await service.subscribe({ ...base });
      expect(again?.failureCount).toBe(0);
    });
  });

  it("asks the repository for exactly the requested ids", async () => {
    await service.findByIds(["a", "b"]);
    expect(findBy).toHaveBeenCalledWith({ id: In(["a", "b"]) });
  });

  it("builds a map keyed by id from what the repository returns", async () => {
    await withVapid(async () => {
      const a = await service.subscribe({ ...base });
      const found = await service.findByIds([a!.id]);
      expect(found.get(a!.id)?.endpoint).toBe(base.endpoint);
    });
  });

  it("answers an empty map for an empty id list without touching the repository", async () => {
    const found = await service.findByIds([]);
    expect(found.size).toBe(0);
    expect(findBy).not.toHaveBeenCalled();
  });
});
