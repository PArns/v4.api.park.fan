import { hashToken } from "../admin/auth/token-hash.util";
import { ApiKeyService, looksLikeApiKey } from "./api-key.service";

const ID = "33333333-3333-4333-8333-333333333333";

function build(found: Record<string, unknown> | null = null) {
  const store = new Map<string, string>();
  const redis = {
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
    };
    const { service, keys } = build(row);
    expect(await service.resolve(key)).toEqual(resolved);
    expect(await service.resolve(key)).toEqual(resolved);
    expect(keys.findOne).toHaveBeenCalledTimes(1);
  });

  it("caches an unknown key as refused", async () => {
    const { service, keys } = build(null);
    expect(await service.resolve(key)).toBeNull();
    expect(await service.resolve(key)).toBeNull();
    expect(keys.findOne).toHaveBeenCalledTimes(1);
  });

  it("forgets a cached key when it is revoked", async () => {
    const row = { id: ID, usage: "non-commercial", keyHash: hashToken(key) };
    const { service, store } = build(row);
    await service.resolve(key);
    expect(store.has(`api-key:${hashToken(key)}`)).toBe(true);

    await service.revoke(ID);
    expect(store.has(`api-key:${hashToken(key)}`)).toBe(false);
  });
});

describe("ApiKeyService.countRequest", () => {
  const key = {
    id: ID,
    usage: "non-commercial" as const,
    requestsPerHour: 2,
    allowedParkIds: null,
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

  it("counts nothing for a key without an hourly allowance", async () => {
    const { service, redis } = build();
    expect(
      await service.countRequest({ ...key, requestsPerHour: null }, at),
    ).toBeNull();
    expect(redis.incr).not.toHaveBeenCalled();
  });
});

describe("ApiKeyService parks", () => {
  it("stores known parks once each and reads an empty list as every park", async () => {
    const { service, keys } = build();
    await service.create(
      { name: "a", allowedParkIds: ["park-1", "park-1"] },
      null,
    );
    expect(keys.save.mock.calls[0][0]).toMatchObject({
      allowedParkIds: ["park-1"],
    });
    await service.create({ name: "b", allowedParkIds: [] }, null);
    expect(keys.save.mock.calls[1][0]).toMatchObject({ allowedParkIds: null });
  });

  it("refuses a park that does not exist", async () => {
    const { service, keys } = build();
    await expect(
      service.create({ name: "a", allowedParkIds: ["park-1", "nope"] }, null),
    ).rejects.toThrow("Unknown park ids: nope");
    expect(keys.save).not.toHaveBeenCalled();
  });
});
