import { HttpException } from "@nestjs/common";
import { AdminController } from "./admin.controller";
import { deleteKeysByPatterns } from "./delete-keys-by-patterns";

function globToRegExp(glob: string): RegExp {
  const escaped = glob.replace(/[.+^${}()|\\]/g, "\\$&").replace(/\*/g, ".*");
  return new RegExp(`^${escaped}$`);
}

/**
 * In-memory Redis with SCAN (two keys per step, so the cursor loop runs more
 * than once) and pipelined DEL. FLUSHALL and KEYS throw: neither may be used.
 */
function fakeRedis(initial: string[]) {
  const store = new Set(initial);
  // Real SCAN never misses a key that exists for the whole walk, even when
  // others are deleted in between; a snapshot per walk models that.
  let snapshot: string[] = [];
  return {
    store,
    flushall: jest.fn(() => {
      throw new Error("FLUSHALL must not be called");
    }),
    keys: jest.fn(() => {
      throw new Error("KEYS must not be called");
    }),
    scan: jest.fn(
      (
        cursor: string,
        _m: "MATCH",
        pattern: string,
        _c: "COUNT",
        _n: number,
      ) => {
        if (cursor === "0") snapshot = [...store].sort();
        const all = snapshot;
        const start = Number(cursor);
        const page = all.slice(start, start + 2);
        const next = start + 2 >= all.length ? "0" : String(start + 2);
        const re = globToRegExp(pattern);
        return Promise.resolve([next, page.filter((k) => re.test(k))]);
      },
    ),
    pipeline: jest.fn(() => {
      const queued: string[] = [];
      const p = {
        del: (key: string) => {
          queued.push(key);
          return p;
        },
        exec: () => {
          for (const key of queued) store.delete(key);
          return Promise.resolve(queued.map(() => [null, 1]));
        },
      };
      return p;
    }),
  };
}

const BULL_KEYS = [
  "parkfan:wait-times:1",
  "parkfan:wait-times:wait",
  "parkfan:wait-times:delayed",
  "parkfan:wait-times:repeat",
  "parkfan:wait-times:repeat:3f9a1c:1759392000000",
  "parkfan:analytics:repeat",
  "parkfan:holidays:id",
];
const KEPT_STATE = [
  "admin:session:abc",
  "admin:sessions:user-1",
  "popularity:ranking",
  "ratelimit:queue-times",
];
const CACHE_KEYS = [
  "park:integrated:p1",
  "attraction:integrated:a1",
  "schedule:p1:2026-10-02",
  "parkfan:queue:latest:a1:STANDBY",
  "parkfan:attraction:tz:a1",
  "ml:park:p1:daily",
];

describe("deleteKeysByPatterns", () => {
  it("deletes only matching keys across several SCAN steps", async () => {
    const redis = fakeRedis([...BULL_KEYS, ...CACHE_KEYS, ...KEPT_STATE]);
    const n = await deleteKeysByPatterns(redis as never, [
      "park:*",
      "parkfan:queue:latest:*",
    ]);
    expect(n).toBe(2);
    expect(redis.store.has("park:integrated:p1")).toBe(false);
    expect(redis.store.has("parkfan:queue:latest:a1:STANDBY")).toBe(false);
    expect(redis.store.has("parkfan:wait-times:repeat")).toBe(true);
    expect(redis.scan.mock.calls.length).toBeGreaterThan(2);
  });

  it("returns 0 and sends no DEL on an empty keyspace", async () => {
    const redis = fakeRedis([]);
    expect(await deleteKeysByPatterns(redis as never, ["park:*"])).toBe(0);
    expect(redis.pipeline).not.toHaveBeenCalled();
  });
});

describe("AdminController.resetCache", () => {
  function controllerWith(redis: ReturnType<typeof fakeRedis>) {
    const queue = () => ({ add: jest.fn().mockResolvedValue({ id: 1 }) });
    const controller = Object.create(
      AdminController.prototype,
    ) as AdminController;
    Object.assign(controller, {
      redis,
      logger: { warn: jest.fn(), log: jest.fn() },
      holidaysQueue: queue(),
      parkMetadataQueue: queue(),
      childrenQueue: queue(),
      waitTimesQueue: queue(),
    });
    return controller;
  }

  it("refuses without confirm=true and deletes nothing", async () => {
    const redis = fakeRedis([...BULL_KEYS, ...CACHE_KEYS]);
    await expect(controllerWith(redis).resetCache()).rejects.toBeInstanceOf(
      HttpException,
    );
    expect(redis.store.size).toBe(BULL_KEYS.length + CACHE_KEYS.length);
  });

  it("deletes the cache, keeps Bull keys and admin sessions, never FLUSHALL", async () => {
    const redis = fakeRedis([...BULL_KEYS, ...CACHE_KEYS, ...KEPT_STATE]);
    const res = await controllerWith(redis).resetCache("true");

    expect(redis.flushall).not.toHaveBeenCalled();
    expect(redis.keys).not.toHaveBeenCalled();
    expect([...redis.store].sort()).toEqual(
      [...BULL_KEYS, ...KEPT_STATE].sort(),
    );
    expect(res.keysDeleted).toBe(CACHE_KEYS.length);
    expect(res.jobsTriggered).toEqual([
      "fetch-holidays",
      "sync-all-parks",
      "fetch-all-children",
      "fetch-wait-times",
    ]);
  });
});
