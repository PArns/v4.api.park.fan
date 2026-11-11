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
    const { service, keys } = build({ id: ID, usage: "commercial" });
    expect(await service.resolve(key)).toEqual({ id: ID, usage: "commercial" });
    expect(await service.resolve(key)).toEqual({ id: ID, usage: "commercial" });
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
