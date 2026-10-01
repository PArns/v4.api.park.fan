import { ServiceUnavailableException } from "@nestjs/common";
import { HealthController } from "./health.controller";

describe("HealthController", () => {
  const connection = { query: jest.fn(), isInitialized: true };
  const redis = { ping: jest.fn() };

  const make = () =>
    new HealthController(
      connection as never,
      {} as never,
      {} as never,
      {} as never,
      {} as never,
      redis as never,
    );

  beforeEach(() => {
    jest.clearAllMocks();
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
});
