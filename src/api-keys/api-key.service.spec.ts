import { hashToken } from "../admin/auth/token-hash.util";
import { ApiKeyService, looksLikeApiKey } from "./api-key.service";

const ID = "33333333-3333-4333-8333-333333333333";

function build(found: Record<string, unknown> | null = null) {
  const store = new Map<string, string>();
  const redis: Record<string, jest.Mock> = {
    get: jest.fn(async (key: string) => store.get(key) ?? null),
    set: jest.fn(async (key: string, value: string, ...args: unknown[]) => {
      if (args.includes("NX") && store.has(key)) return null;
      store.set(key, value);
      return "OK";
    }),
    del: jest.fn(async (key: string) => (store.delete(key) ? 1 : 0)),
    incr: jest.fn(async (key: string) => {
      const next = Number(store.get(key) ?? 0) + 1;
      store.set(key, String(next));
      return next;
    }),
    expire: jest.fn(async () => 1),
    multi: jest.fn(() => {
      const ops: Array<() => Promise<unknown>> = [];
      const chain = {
        incr: (key: string) => {
          ops.push(() => redis.incr(key));
          return chain;
        },
        expire: (key: string, ...args: unknown[]) => {
          ops.push(() => redis.expire(key, ...(args as [])));
          return chain;
        },
        exec: async () => {
          const out: Array<[null, unknown]> = [];
          for (const op of ops) out.push([null, await op()]);
          return out;
        },
      };
      return chain;
    }),
    mget: jest.fn(async (...keys: string[]) =>
      keys.map((key) => store.get(key) ?? null),
    ),
  };
  const keys = {
    create: jest.fn((value: Record<string, unknown>) => value),
    save: jest.fn(async (value: Record<string, unknown>) => ({
      id: ID,
      createdAt: new Date(),
      lastUsedAt: null,
      revokedAt: null,
      ...value,
    })),
    findOne: jest.fn(async () => found),
    update: jest.fn(async () => undefined),
    query: jest.fn(async (_sql: string, params: unknown[]) =>
      ((params[0] as string[]) ?? [])
        .filter((id) => id.startsWith("park-"))
        .map((id) => ({ id, name: `Name of ${id}` })),
    ),
  };
  const service = new ApiKeyService(keys as never, redis as never);
  return { service, keys, redis, store };
}

describe("looksLikeApiKey", () => {
  it("takes the prefix and 43 base64url characters, nothing else", () => {
    expect(looksLikeApiKey(`pfk_${"a".repeat(43)}`)).toBe(true);
    expect(looksLikeApiKey(`pfa_${"a".repeat(43)}`)).toBe(false);
    expect(looksLikeApiKey(`pfk_${"a".repeat(42)}`)).toBe(false);
    expect(looksLikeApiKey(`pfk_${"a".repeat(42)}!`)).toBe(false);
  });
});

describe("ApiKeyService end dates", () => {
  it("refuses an end date that has passed, on create and on edit", async () => {
    const { service } = build({ id: ID, keyHash: "h", revokedAt: null });
    const past = new Date(Date.now() - 60_000);
    await expect(
      service.create({ name: "x", expiresAt: past }, null),
    ).rejects.toThrow("expiresAt must be in the future");
    await expect(service.update(ID, { expiresAt: past })).rejects.toThrow(
      "expiresAt must be in the future",
    );
  });

  it("brings an expired key back with a new date, and null removes it", async () => {
    const row = {
      id: ID,
      keyHash: "h",
      revokedAt: null,
      expiresAt: new Date(Date.now() - 60_000),
    };
    const { service, store } = build(row);
    const later = new Date(Date.now() + 86_400_000);
    const changed = await service.update(ID, { expiresAt: later });
    expect(changed?.after.expiresAt).toEqual(later);
    expect(JSON.parse(store.get("api-key:h") ?? "{}")).toMatchObject({
      expiresAt: later.toISOString(),
    });
    expect(
      (await service.update(ID, { expiresAt: null }))?.item.expiresAt,
    ).toBe(null);
  });
});

describe("ApiKeyService.create", () => {
  it("returns the key once and stores only its hash and prefix", async () => {
    const { service, keys } = build();
    const { key, item } = await service.create({ name: "Club site" }, null);

    expect(looksLikeApiKey(key)).toBe(true);
    const saved = keys.save.mock.calls[0][0] as Record<string, unknown>;
    expect(saved.keyHash).toBe(hashToken(key));
    expect(JSON.stringify(saved)).not.toContain(key);
    expect(item.prefix).toBe(key.slice(0, 10));
    expect(item.usage).toBe("non-commercial");
    expect(item.requestsPerHour).toBe(1000);
    expect(item).not.toHaveProperty("keyHash");
  });
});

describe("ApiKeyService.resolve", () => {
  const key = `pfk_${"b".repeat(43)}`;

  it("refuses a value that is not shaped like a key without a lookup", async () => {
    const { service, keys, redis } = build();
    expect(await service.resolve("pfa_whatever")).toBeNull();
    expect(redis.get).not.toHaveBeenCalled();
    expect(keys.findOne).not.toHaveBeenCalled();
  });

  it("answers a known key from Postgres once, then from Redis", async () => {
    const row = {
      id: ID,
      usage: "commercial",
      requestsPerHour: 50,
      allowedParkIds: ["p1"],
    };
    const resolved = {
      id: ID,
      usage: "commercial",
      requestsPerHour: 50,
      allowedParkIds: ["p1"],
      expiresAt: null,
    };
    const { service, keys } = build(row);
    expect(await service.resolve(key)).toEqual(resolved);
    expect(await service.resolve(key)).toEqual(resolved);
    expect(keys.findOne).toHaveBeenCalledTimes(1);
  });

  it("stops a key at its end date, though the lookup is cached", async () => {
    const until = new Date(Date.UTC(2026, 10, 30, 22, 59, 59));
    const row = {
      id: ID,
      usage: "non-commercial",
      requestsPerHour: 50,
      allowedParkIds: null,
      expiresAt: until,
    };
    const { service, keys } = build(row);
    const before = until.getTime() - 1000;
    expect(await service.lookup(key, before)).toMatchObject({
      status: "active",
      key: { expiresAt: until.toISOString() },
    });
    expect(await service.lookup(key, until.getTime())).toEqual({
      status: "expired",
      expiresAt: until.toISOString(),
    });
    expect(await service.resolve(key, until.getTime())).toBeNull();
    expect(keys.findOne).toHaveBeenCalledTimes(1);
  });

  it("caches an unknown key as refused", async () => {
    const { service, keys } = build(null);
    expect(await service.resolve(key)).toBeNull();
    expect(await service.resolve(key)).toBeNull();
    expect(keys.findOne).toHaveBeenCalledTimes(1);
  });

  it("refuses a key once it is revoked, even if a lookup was in flight", async () => {
    const row = {
      id: ID,
      usage: "non-commercial",
      requestsPerHour: null,
      allowedParkIds: null,
      keyHash: hashToken(key),
    };
    const { service, keys, store } = build(null);
    let release: (value: unknown) => void = () => undefined;
    keys.findOne
      // The lookup of a request, held until the revocation has gone through.
      .mockImplementationOnce(
        () => new Promise((resolve) => (release = resolve)) as never,
      )
      // The revocation's own lookup.
      .mockImplementationOnce(async () => ({ ...row }) as never);

    const inFlight = service.resolve(key);
    // Let the request reach its Postgres lookup before the admin acts.
    while (keys.findOne.mock.calls.length === 0) {
      await new Promise((resolve) => setImmediate(resolve));
    }
    await service.revoke(ID);
    release({ ...row });
    await inFlight;

    expect(store.get(`api-key:${hashToken(key)}`)).toBe("-");
    expect(await service.resolve(key)).toBeNull();
  });
});

describe("ApiKeyService.countRequest", () => {
  const key = {
    id: ID,
    usage: "non-commercial" as const,
    requestsPerHour: 2,
    allowedParkIds: null,
    expiresAt: null,
  };
  // 10:15 UTC: 45 minutes to the next full hour.
  const at = Date.UTC(2026, 10, 11, 10, 15, 0);

  it("counts per clock hour and says when the count starts over", async () => {
    const { service } = build();
    expect(await service.countRequest(key, at)).toEqual({
      limit: 2,
      remaining: 1,
      resetSeconds: 2700,
      exceeded: false,
    });
    await service.countRequest(key, at);
    expect((await service.countRequest(key, at))?.exceeded).toBe(true);
    // A new hour, a new count.
    expect((await service.countRequest(key, at + 3_600_000))?.exceeded).toBe(
      false,
    );
  });

  it("sets no allowance for a key without one, but counts its use", async () => {
    const { service, store } = build();
    expect(
      await service.countRequest({ ...key, requestsPerHour: null }, at),
    ).toBeNull();
    expect(store.get(`api-key:day:${ID}:2026-11-11`)).toBe("1");
    expect(store.get(`api-key:month:${ID}:2026-11`)).toBe("1");
  });

  it("lists the last days and months, oldest first, and the refusals", async () => {
    const { service, keys, store } = build();
    (keys as unknown as { createQueryBuilder: jest.Mock }).createQueryBuilder =
      jest.fn(() => ({
        orderBy: () => ({
          addOrderBy: () => ({
            getMany: async () => [
              { id: ID, allowedParkIds: null, createdAt: new Date(at) },
            ],
          }),
        }),
      }));
    // Three requests against an allowance of two: the third is refused.
    await service.countRequest(key, at);
    await service.countRequest(key, at);
    await service.countRequest(key, at);
    await new Promise((resolve) => setImmediate(resolve));
    store.set(`api-key:day:${ID}:2026-11-10`, "7");
    store.set(`api-key:month:${ID}:2026-10`, "40");

    const [item] = await service.list(at);
    expect(item.requestsThisHour).toBe(3);
    expect(item.stats?.days).toHaveLength(30);
    expect(item.stats?.days.slice(-2)).toEqual([
      { date: "2026-11-10", requests: 7, limited: 0 },
      { date: "2026-11-11", requests: 3, limited: 1 },
    ]);
    expect(item.stats?.days[0].date).toBe("2026-10-13");
    expect(item.stats?.months.slice(-2)).toEqual([
      { month: "2026-10", requests: 40 },
      { month: "2026-11", requests: 3 },
    ]);
    expect(item.stats?.months[0].month).toBe("2025-12");
  });
});

describe("ApiKeyService parks", () => {
  it("stores known parks once each, lowercased, and only null means every park", async () => {
    const { service, keys } = build();
    await service.create(
      { name: "a", allowedParkIds: ["park-1", "PARK-1"] },
      null,
    );
    expect(keys.save.mock.calls[0][0]).toMatchObject({
      allowedParkIds: ["park-1"],
    });
    await service.create({ name: "b", allowedParkIds: null }, null);
    expect(keys.save.mock.calls[1][0]).toMatchObject({ allowedParkIds: null });
    // The DTO refuses an empty list; should one reach here, it opens nothing.
    await service.create({ name: "c", allowedParkIds: [] }, null);
    expect(keys.save.mock.calls[2][0]).toMatchObject({ allowedParkIds: [] });
  });

  it("holds park.fan's own internal key to no allowance, but counts its use", async () => {
    const { service, keys, store } = build();
    await service.create({ name: "park.fan", usage: "internal" }, null);
    expect(keys.save.mock.calls[0][0]).toMatchObject({ requestsPerHour: null });
    expect(
      await service.countRequest({
        id: ID,
        usage: "internal",
        requestsPerHour: 1000,
        allowedParkIds: null,
        expiresAt: null,
      }),
    ).toBeNull();
    expect(
      [...store.keys()].some((name) => name.startsWith(`api-key:day:${ID}:`)),
    ).toBe(true);
  });
  it("refuses a park that does not exist", async () => {
    const { service, keys } = build();
    await expect(
      service.create({ name: "a", allowedParkIds: ["park-1", "nope"] }, null),
    ).rejects.toThrow("Unknown park ids: nope");
    expect(keys.save).not.toHaveBeenCalled();
  });
});
