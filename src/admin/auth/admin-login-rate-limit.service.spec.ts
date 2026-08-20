import { AdminLoginRateLimitService } from "./admin-login-rate-limit.service";
import { RedisMock } from "../../../test/mocks/redis.mock";

describe("AdminLoginRateLimitService", () => {
  let redis: RedisMock;
  let limiter: AdminLoginRateLimitService;
  let now: number;

  beforeEach(() => {
    now = Date.UTC(2026, 7, 20, 9, 0, 0);
    redis = new RedisMock();
    redis.now = () => now;
    limiter = new AdminLoginRateLimitService(redis as never);
  });

  it("allows a first attempt", async () => {
    const verdict = await limiter.check("198.51.100.7", "you@park.fan");
    expect(verdict).toEqual({
      allowed: true,
      retryAfterSeconds: 0,
      reason: null,
    });
  });

  it("shuts one account down after ten failures, whatever the address", async () => {
    for (let i = 0; i < 10; i++) {
      await limiter.recordFailure(`198.51.100.${i}`, "you@park.fan");
    }
    const verdict = await limiter.check("198.51.100.200", "you@park.fan");
    expect(verdict.allowed).toBe(false);
    expect(verdict.reason).toBe("account");
    expect(verdict.retryAfterSeconds).toBeGreaterThan(0);
  });

  it("shuts one address down after a spray across many accounts", async () => {
    // The case the per-account lockout cannot see: each account fails once or
    // twice, so none of them ever locks.
    for (let i = 0; i < 25; i++) {
      await limiter.recordFailure("198.51.100.7", `victim-${i}@park.fan`);
    }
    const verdict = await limiter.check(
      "198.51.100.7",
      "someone-else@park.fan",
    );
    expect(verdict.allowed).toBe(false);
    expect(verdict.reason).toBe("ip");
  });

  it("keeps a fixed window rather than a sliding one", async () => {
    // A sliding window would let a slow attacker hold the door shut forever by
    // failing once every minute.
    for (let i = 0; i < 10; i++) {
      await limiter.recordFailure(null, "you@park.fan");
      now += 60_000;
    }
    // Fifteen minutes after the FIRST failure the window is over, even though
    // the last one was four minutes ago.
    now += 6 * 60_000;
    expect((await limiter.check(null, "you@park.fan")).allowed).toBe(true);
  });

  it("forgets an account's failures once it succeeds", async () => {
    for (let i = 0; i < 9; i++)
      await limiter.recordFailure(null, "you@park.fan");
    await limiter.recordSuccess("you@park.fan");
    for (let i = 0; i < 9; i++)
      await limiter.recordFailure(null, "you@park.fan");
    expect((await limiter.check(null, "you@park.fan")).allowed).toBe(true);
  });

  it("does not let one correct password vouch for the address it came from", async () => {
    for (let i = 0; i < 25; i++) {
      await limiter.recordFailure("198.51.100.7", `victim-${i}@park.fan`);
    }
    await limiter.recordSuccess("victim-0@park.fan");
    expect(
      (await limiter.check("198.51.100.7", "victim-1@park.fan")).allowed,
    ).toBe(false);
  });

  it("is case-insensitive about the account, so casing is not a free retry", async () => {
    for (let i = 0; i < 10; i++)
      await limiter.recordFailure(null, "You@Park.Fan");
    expect((await limiter.check(null, "you@park.fan")).allowed).toBe(false);
  });

  it("stores no readable address or email", async () => {
    await limiter.recordFailure("198.51.100.7", "you@park.fan");
    // Both are personal data in a cache somebody will one day dump.
    expect(await redis.get("admin:login:ip:198.51.100.7")).toBeNull();
    expect(await redis.get("admin:login:acct:you@park.fan")).toBeNull();
  });

  it("fails open when Redis is unavailable", async () => {
    // The durable defence is the per-account lockout in Postgres. A Redis
    // outage must not lock every administrator out of their own admin.
    const broken = new AdminLoginRateLimitService({
      get: () => Promise.reject(new Error("connection refused")),
      incr: () => Promise.reject(new Error("connection refused")),
      expire: () => Promise.reject(new Error("connection refused")),
      del: () => Promise.reject(new Error("connection refused")),
      ttl: () => Promise.reject(new Error("connection refused")),
    } as never);

    expect((await broken.check("198.51.100.7", "you@park.fan")).allowed).toBe(
      true,
    );
    await expect(
      broken.recordFailure("198.51.100.7", "you@park.fan"),
    ).resolves.toBeUndefined();
    await expect(broken.recordSuccess("you@park.fan")).resolves.toBeUndefined();
  });
});
