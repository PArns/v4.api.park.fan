import { ServiceUnavailableException } from "@nestjs/common";
import { HEALTH_COUNT_KEYS, HealthController } from "./health.controller";

describe("HealthController", () => {
  const connection = { query: jest.fn(), isInitialized: true };
  const redis = { ping: jest.fn(), get: jest.fn(), set: jest.fn() };
  const queueData = { count: jest.fn() };

  const make = () =>
    new HealthController(
      connection as never,
      {} as never,
      {} as never,
      queueData as never,
      {} as never,
      redis as never,
    );

  type Counts = {
    getPredictionsCount24h: () => Promise<number>;
    getWaitTimesCount24h: () => Promise<number>;
  };

  beforeEach(() => {
    jest.resetAllMocks();
    jest.useRealTimers();
  });

  describe("ready", () => {
    it("answers ok when Postgres and Redis respond", async () => {
      connection.query.mockResolvedValue([{ "?column?": 1 }]);
      redis.ping.mockResolvedValue("PONG");

      await expect(make().ready()).resolves.toEqual({ status: "ok" });
      expect(connection.query).toHaveBeenCalledWith("SELECT 1");
    });

    it("answers 503 when the database is unreachable", async () => {
      connection.query.mockRejectedValue(new Error("ECONNREFUSED"));
      redis.ping.mockResolvedValue("PONG");

      await expect(make().ready()).rejects.toBeInstanceOf(
        ServiceUnavailableException,
      );
    });

    it("answers 503 when Redis is unreachable", async () => {
      connection.query.mockResolvedValue([]);
      redis.ping.mockRejectedValue(new Error("Connection is closed"));

      await expect(make().ready()).rejects.toBeInstanceOf(
        ServiceUnavailableException,
      );
    });
  });

  describe("getHealth memo", () => {
    const payload = { status: "ok" } as never;

    it("builds once per minute and shares the in-flight build", async () => {
      jest.useFakeTimers({ now: 0 });
      const c = make();
      const build = jest
        .spyOn(
          c as never as { buildHealth: () => Promise<unknown> },
          "buildHealth",
        )
        .mockResolvedValue(payload);

      const [a, b] = await Promise.all([c.getHealth(), c.getHealth()]);
      jest.setSystemTime(59_999);
      await c.getHealth();
      expect(a).toBe(payload);
      expect(b).toBe(payload);
      expect(build).toHaveBeenCalledTimes(1);

      jest.setSystemTime(60_001);
      await c.getHealth();
      expect(build).toHaveBeenCalledTimes(2);
    });

    it("does not keep a failed build", async () => {
      const c = make();
      const build = jest
        .spyOn(
          c as never as { buildHealth: () => Promise<unknown> },
          "buildHealth",
        )
        .mockRejectedValueOnce(new Error("db down"))
        .mockResolvedValueOnce(payload);

      await expect(c.getHealth()).rejects.toThrow("db down");
      await expect(c.getHealth()).resolves.toBe(payload);
      expect(build).toHaveBeenCalledTimes(2);
    });
  });

  // PAR-872: the two 24 h counts ran on every /health build — 2 620 times a
  // day for wait_time_predictions at 106 ms. They are shared through Redis.
  describe("24 h counts", () => {
    it("answers from Redis without touching the database", async () => {
      redis.get.mockResolvedValue("1638556");
      const c = make() as never as Counts;

      await expect(c.getPredictionsCount24h()).resolves.toBe(1638556);
      expect(connection.query).not.toHaveBeenCalled();
      expect(redis.get).toHaveBeenCalledWith(HEALTH_COUNT_KEYS.predictions24h);
    });

    it("counts once on a miss and shares the result for ten minutes", async () => {
      redis.get.mockResolvedValue(null);
      connection.query.mockResolvedValue([{ count: "42" }]);
      queueData.count.mockResolvedValue(7);
      const c = make() as never as Counts;

      await expect(c.getPredictionsCount24h()).resolves.toBe(42);
      await expect(c.getWaitTimesCount24h()).resolves.toBe(7);
      expect(redis.set).toHaveBeenCalledWith(
        HEALTH_COUNT_KEYS.predictions24h,
        "42",
        "EX",
        600,
      );
      expect(redis.set).toHaveBeenCalledWith(
        HEALTH_COUNT_KEYS.waitTimes24h,
        "7",
        "EX",
        600,
      );
    });

    it("never caches a failed count, and still answers 0", async () => {
      redis.get.mockResolvedValue(null);
      connection.query.mockRejectedValue(new Error("timeout"));
      const c = make() as never as Counts;

      await expect(c.getPredictionsCount24h()).resolves.toBe(0);
      expect(redis.set).not.toHaveBeenCalled();
    });

    it("falls through to the database when Redis is down", async () => {
      redis.get.mockRejectedValue(new Error("Connection is closed"));
      redis.set.mockRejectedValue(new Error("Connection is closed"));
      queueData.count.mockResolvedValue(197047);
      const c = make() as never as Counts;

      await expect(c.getWaitTimesCount24h()).resolves.toBe(197047);
    });
  });
});
