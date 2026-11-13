import { ConflictException } from "@nestjs/common";
import { FindOperator } from "typeorm";
import { hashToken } from "../admin/auth/token-hash.util";
import { ApiKeyService, looksLikeApiKey } from "./api-key.service";
import type { ApiKey } from "./entities/api-key.entity";

const ID = "44444444-4444-4444-8444-444444444444";
const HOUR = 3_600_000;
const T0 = Date.UTC(2026, 10, 13, 9, 0, 0);
const OLD_KEY = `pfk_${"o".repeat(43)}`;

type Row = Record<string, unknown>;

/** Whether a row matches one TypeORM `where` object: equality, `IsNull()`, `MoreThan()`. */
function matches(row: Row, where: Row): boolean {
  return Object.entries(where).every(([field, wanted]) => {
    const value = row[field];
    if (wanted instanceof FindOperator) {
      if (wanted.type === "isNull")
        return value === null || value === undefined;
      if (wanted.type === "moreThan") {
        return value instanceof Date && value > (wanted.value as Date);
      }
      throw new Error(`Unexpected operator ${wanted.type}`);
    }
    return value === wanted;
  });
}

/**
 * One key in a repository that honours the service's `where` clauses, and a
 * Redis that honours EX, PX and NX against a clock the test moves.
 */
function build(overrides: Partial<ApiKey> = {}) {
  const clock = { now: T0 };
  const store = new Map<string, string>();
  const expiry = new Map<string, number>();
  const live = (key: string): boolean => {
    const at = expiry.get(key);
    if (at !== undefined && at <= clock.now) {
      store.delete(key);
      expiry.delete(key);
    }
    return store.has(key);
  };
  const redis = {
    get: jest.fn(async (key: string) => (live(key) ? store.get(key)! : null)),
    set: jest.fn(async (key: string, value: string, ...args: unknown[]) => {
      if (args.includes("NX") && live(key)) return null;
      store.set(key, value);
      const ex = args.indexOf("EX");
      const px = args.indexOf("PX");
      if (ex >= 0) expiry.set(key, clock.now + Number(args[ex + 1]) * 1000);
      else if (px >= 0) expiry.set(key, clock.now + Number(args[px + 1]));
      else expiry.delete(key);
      return "OK";
    }),
    del: jest.fn(async (...keys: string[]) => {
      let removed = 0;
      for (const key of keys) {
        if (store.delete(key)) removed++;
        expiry.delete(key);
      }
      return removed;
    }),
  };
  const row: Row = {
    id: ID,
    name: "Coaster club",
    contact: null,
    usage: "non-commercial",
    note: null,
    requestsPerHour: 1000,
    allowedParkIds: null,
    expiresAt: null,
    prefix: OLD_KEY.slice(0, 10),
    keyHash: hashToken(OLD_KEY),
    previousKeyHash: null,
    previousKeyExpiresAt: null,
    previousLastUsedAt: null,
    rotatedAt: null,
    createdBy: null,
    createdAt: new Date(T0 - 30 * 24 * HOUR),
    lastUsedAt: new Date(T0 - HOUR),
    revokedAt: null,
    ...overrides,
  };
  const anyMatch = (where: Row | Row[]) =>
    (Array.isArray(where) ? where : [where]).some((w) => matches(row, w));
  const keys = {
    create: jest.fn((value: Row) => ({ ...value })),
    findOne: jest.fn(async ({ where }: { where: Row | Row[] }) =>
      anyMatch(where) ? { ...row } : null,
    ),
    save: jest.fn(async (value: Row) => Object.assign(row, value)),
    update: jest.fn(async (where: Row, changes: Row) => {
      if (!anyMatch(where)) return { affected: 0 };
      Object.assign(row, changes);
      return { affected: 1 };
    }),
  };
  const service = new ApiKeyService(keys as never, redis as never);
  return { service, keys, redis, store, row, clock };
}

/** Lets the use a lookup records (never awaited) reach the repository. */
const settle = () => new Promise((resolve) => setImmediate(resolve));

describe("ApiKeyService.rotate", () => {
  it("gives the key a new secret and keeps the old one for the grace period", async () => {
    const { service, row } = build();
    const rotated = await service.rotate(ID, 24, T0);

    expect(rotated).not.toBeNull();
    const { key, item } = rotated!;
    expect(looksLikeApiKey(key)).toBe(true);
    expect(key).not.toBe(OLD_KEY);
    expect(row.keyHash).toBe(hashToken(key));
    expect(row.previousKeyHash).toBe(hashToken(OLD_KEY));
    expect(item.prefix).toBe(key.slice(0, 10));
    expect(item.previousExpiresAt).toEqual(new Date(T0 + 24 * HOUR));
    expect(item.rotatedAt).toEqual(new Date(T0));
    // Each secret keeps its own last use: the old one's moves over.
    expect(item.previousLastUsedAt).toEqual(new Date(T0 - HOUR));
    expect(item.lastUsedAt).toBeNull();
    // Name, usage, allowance and parks stay.
    expect(item).toMatchObject({
      id: ID,
      name: "Coaster club",
      usage: "non-commercial",
      requestsPerHour: 1000,
      allowedParkIds: null,
    });
  });

  it("accepts both secrets during the grace period, as the same key", async () => {
    const { service, clock } = build();
    const { key } = (await service.rotate(ID, 24, T0))!;
    clock.now = T0 + HOUR;

    const current = await service.lookup(key, clock.now);
    const previous = await service.lookup(OLD_KEY, clock.now);
    expect(current).toMatchObject({ status: "active", key: { id: ID } });
    expect(previous).toMatchObject({ status: "active", key: { id: ID } });
    // The old secret tells its holder when it stops.
    expect(previous).toMatchObject({
      key: { expiresAt: new Date(T0 + 24 * HOUR).toISOString() },
    });
    expect(current).toMatchObject({ key: { expiresAt: null } });
  });

  it("refuses the old secret once the grace period is over, cached or not", async () => {
    const { service, clock } = build();
    await service.rotate(ID, 1, T0);
    const end = T0 + HOUR;

    clock.now = end - 1000;
    expect((await service.lookup(OLD_KEY, clock.now)).status).toBe("active");
    // Even an entry Redis still held would be refused at the end of the grace.
    expect((await service.lookup(OLD_KEY, end)).status).toBe("unknown");
    clock.now = end;
    expect((await service.lookup(OLD_KEY, clock.now)).status).toBe("unknown");
    clock.now = end + 10 * 60_000;
    expect((await service.lookup(OLD_KEY, clock.now)).status).toBe("unknown");
  });

  it("never caches the old secret past the end of its grace period", async () => {
    const { service, redis, store, clock } = build();
    await service.rotate(ID, 1, T0);
    const end = T0 + HOUR;
    const oldEntry = `api-key:${hashToken(OLD_KEY)}`;

    for (const at of [T0, end - 4 * 60_000, end - 90_000, end - 1000]) {
      clock.now = at;
      // Each lookup asks Postgres and writes the entry afresh.
      store.clear();
      redis.set.mockClear();
      expect((await service.lookup(OLD_KEY, at)).status).toBe("active");
      const written = redis.set.mock.calls.find((call) => call[0] === oldEntry);
      expect(written?.[2]).toBe("PX");
      expect(at + Number(written?.[3])).toBeLessThanOrEqual(end);
    }
    // The entry the rotation itself wrote is capped the same way.
    const fresh = build();
    await fresh.service.rotate(ID, 1, T0);
    const written = fresh.redis.set.mock.calls.find(
      (call) => call[0] === oldEntry,
    );
    expect(written?.slice(2)).toEqual(["PX", 5 * 60_000]);
  });

  it("refuses the old secret at once without a grace period", async () => {
    const { service, row, store, clock } = build();
    // A lookup before the rotation cached the old secret as valid.
    expect((await service.lookup(OLD_KEY, T0)).status).toBe("active");

    const { key, item } = (await service.rotate(ID, 0, T0))!;
    expect(row.previousKeyHash).toBeNull();
    expect(item.previousExpiresAt).toBeNull();
    expect(item.previousLastUsedAt).toBeNull();
    expect(store.get(`api-key:${hashToken(OLD_KEY)}`)).toBe("-");
    clock.now = T0 + 1000;
    expect((await service.lookup(OLD_KEY, clock.now)).status).toBe("unknown");
    expect((await service.lookup(key, clock.now)).status).toBe("active");
  });

  it("forgets a new secret that was cached as unknown before it existed", async () => {
    const { service, redis } = build();
    const { key } = (await service.rotate(ID, 0, T0))!;
    expect(redis.del).toHaveBeenCalledWith(
      `api-key:${hashToken(key)}`,
      expect.any(String),
      expect.any(String),
    );
  });

  it("replaces the kept secret when it rotates again during a grace period", async () => {
    const { service, row, clock } = build();
    const { key: second } = (await service.rotate(ID, 24, T0))!;
    clock.now = T0 + HOUR;
    expect((await service.lookup(OLD_KEY, clock.now)).status).toBe("active");

    const { key: third, item } = (await service.rotate(ID, 1, clock.now))!;
    expect(row.previousKeyHash).toBe(hashToken(second));
    expect(item.previousExpiresAt).toEqual(new Date(clock.now + HOUR));
    clock.now += 1000;
    expect((await service.lookup(OLD_KEY, clock.now)).status).toBe("unknown");
    expect((await service.lookup(second, clock.now)).status).toBe("active");
    expect((await service.lookup(third, clock.now)).status).toBe("active");
  });

  it("refuses to rotate a revoked or expired key, and finds no unknown one", async () => {
    const revoked = build({ revokedAt: new Date(T0 - HOUR) });
    await expect(revoked.service.rotate(ID, 24, T0)).rejects.toBeInstanceOf(
      ConflictException,
    );
    expect(revoked.keys.update).not.toHaveBeenCalled();

    const expired = build({ expiresAt: new Date(T0 - 1000) });
    await expect(expired.service.rotate(ID, 24, T0)).rejects.toBeInstanceOf(
      ConflictException,
    );
    expect(expired.keys.update).not.toHaveBeenCalled();

    const { service } = build();
    expect(
      await service.rotate("55555555-5555-4555-8555-555555555555", 24, T0),
    ).toBeNull();
  });

  it("refuses a grace period outside 0 to 720 whole hours", async () => {
    const { service, keys } = build();
    for (const hours of [-1, 721, 1.5]) {
      await expect(service.rotate(ID, hours, T0)).rejects.toThrow(
        "graceHours must be a whole number from 0 to 720",
      );
    }
    expect(keys.update).not.toHaveBeenCalled();
  });

  it("answers 409 when another rotation got there first", async () => {
    const { service, keys } = build();
    keys.update.mockResolvedValueOnce({ affected: 0 });
    await expect(service.rotate(ID, 24, T0)).rejects.toBeInstanceOf(
      ConflictException,
    );
  });

  it("ends the old secret's grace at the key's own end date if that comes first", async () => {
    const until = new Date(T0 + 2 * HOUR);
    const { service, clock } = build({ expiresAt: until });
    await service.rotate(ID, 24, T0);
    clock.now = T0 + HOUR;
    expect(await service.lookup(OLD_KEY, clock.now)).toMatchObject({
      status: "active",
      key: { expiresAt: until.toISOString() },
    });
    clock.now = until.getTime();
    expect((await service.lookup(OLD_KEY, clock.now)).status).toBe("expired");
  });

  it("records the old secret's use apart from the new one's", async () => {
    const { service, row, clock } = build({ lastUsedAt: null });
    const { key } = (await service.rotate(ID, 24, T0))!;
    clock.now = T0 + HOUR;

    await service.lookup(OLD_KEY, clock.now);
    await settle();
    expect(row.previousLastUsedAt).toBeInstanceOf(Date);
    expect(row.lastUsedAt).toBeNull();

    await service.lookup(key, clock.now);
    await settle();
    expect(row.lastUsedAt).toBeInstanceOf(Date);
  });

  it("refuses both secrets when a key in its grace period is revoked", async () => {
    const { service, clock } = build();
    const { key } = (await service.rotate(ID, 24, T0))!;
    await service.lookup(OLD_KEY, T0);
    await service.lookup(key, T0);

    await service.revoke(ID);
    clock.now = T0 + 1000;
    expect((await service.lookup(OLD_KEY, clock.now)).status).toBe("unknown");
    expect((await service.lookup(key, clock.now)).status).toBe("unknown");
  });

  it("carries an edit to the old secret too, still capped at its grace", async () => {
    const { service, store, redis } = build();
    await service.rotate(ID, 1, T0);
    // `update` reads the clock itself: ten minutes before the grace ends.
    const clock = jest.spyOn(Date, "now").mockReturnValue(T0 + 50 * 60_000);
    try {
      await service.update(ID, { requestsPerHour: 50 });
    } finally {
      clock.mockRestore();
    }
    expect(
      redis.set.mock.calls.find(
        (call) =>
          call[0] === `api-key:${hashToken(OLD_KEY)}` && call[2] === "PX",
      )?.[3],
    ).toBe(5 * 60_000);
    const cached = JSON.parse(
      store.get(`api-key:${hashToken(OLD_KEY)}`) ?? "{}",
    ) as Record<string, unknown>;
    expect(cached.requestsPerHour).toBe(50);
  });
});

describe("ApiKeyService.revokePrevious", () => {
  it("ends the grace period now and leaves the new secret alone", async () => {
    const { service, row, store, clock } = build();
    const { key } = (await service.rotate(ID, 24, T0))!;
    clock.now = T0 + HOUR;
    expect((await service.lookup(OLD_KEY, clock.now)).status).toBe("active");

    const ended = await service.revokePrevious(ID, clock.now);
    expect(ended?.previousExpiresAt).toEqual(new Date(T0 + 24 * HOUR));
    expect(ended?.item.previousExpiresAt).toBeNull();
    expect(row.previousKeyHash).toBeNull();
    expect(store.get(`api-key:${hashToken(OLD_KEY)}`)).toBe("-");
    clock.now += 1000;
    expect((await service.lookup(OLD_KEY, clock.now)).status).toBe("unknown");
    expect((await service.lookup(key, clock.now)).status).toBe("active");
  });

  it("finds nothing to end without a running grace period", async () => {
    const { service } = build();
    expect(await service.revokePrevious(ID, T0)).toBeNull();
    await service.rotate(ID, 1, T0);
    expect(await service.revokePrevious(ID, T0 + HOUR)).toBeNull();
    const revoked = build({ revokedAt: new Date(T0) });
    expect(await revoked.service.revokePrevious(ID, T0)).toBeNull();
  });
});

describe("rotation answers", () => {
  it("carry no hash, of either secret", async () => {
    const { service, row } = build();
    const rotated = (await service.rotate(ID, 24, T0))!;
    const ended = (await service.revokePrevious(ID, T0 + HOUR))!;
    const hashes = [
      hashToken(OLD_KEY),
      hashToken(rotated.key),
      row.keyHash as string,
    ];
    for (const answer of [rotated.item, ended.item]) {
      const text = JSON.stringify(answer);
      for (const hash of hashes) expect(text).not.toContain(hash);
      expect(answer).not.toHaveProperty("keyHash");
      expect(answer).not.toHaveProperty("previousKeyHash");
    }
    expect(JSON.stringify(rotated)).not.toContain(OLD_KEY);
  });
});
