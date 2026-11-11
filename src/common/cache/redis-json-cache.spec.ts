import { cachedJson } from "./redis-json-cache";

function redis() {
  const store = new Map<string, string>();
  return {
    store,
    get: jest.fn(async (key: string) => store.get(key) ?? null),
    set: jest.fn(async (key: string, value: string) => {
      store.set(key, value);
      return "OK";
    }),
  };
}

describe("cachedJson", () => {
  it("computes once and answers the repeat from Redis", async () => {
    const r = redis();
    const compute = jest.fn(async () => ({ n: 1 }));
    expect(await cachedJson(r as never, "k", 60, compute)).toEqual({ n: 1 });
    expect(await cachedJson(r as never, "k", 60, compute)).toEqual({ n: 1 });
    expect(compute).toHaveBeenCalledTimes(1);
  });

  it("does not keep what `keep` refuses, or nothing", async () => {
    const r = redis();
    await cachedJson(
      r as never,
      "a",
      60,
      async () => ({ degraded: true }),
      (v) => !v.degraded,
    );
    await cachedJson(r as never, "b", 60, async () => null);
    expect(r.store.size).toBe(0);
  });

  it("computes when Redis fails", async () => {
    const r = redis();
    r.get.mockRejectedValueOnce(new Error("down"));
    r.set.mockRejectedValueOnce(new Error("down"));
    expect(await cachedJson(r as never, "k", 60, async () => 7)).toBe(7);
  });
});
