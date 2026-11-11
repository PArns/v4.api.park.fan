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

describe("cachedJson under load", () => {
  it("builds once for concurrent misses on the same key", async () => {
    const r = redis();
    let builds = 0;
    const compute = async () => {
      builds += 1;
      await new Promise((resolve) => setTimeout(resolve, 5));
      return { n: builds };
    };
    const answers = await Promise.all(
      Array.from({ length: 5 }, () => cachedJson(r as never, "k", 60, compute)),
    );
    expect(builds).toBe(1);
    expect(answers.every((a) => a.n === 1)).toBe(true);
  });

  it("rebuilds when the entry does not parse", async () => {
    const r = redis();
    r.store.set("k", "{not json");
    expect(await cachedJson(r as never, "k", 60, async () => 3)).toBe(3);
  });
});
