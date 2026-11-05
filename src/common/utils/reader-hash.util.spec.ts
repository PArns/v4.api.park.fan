import {
  readerHash,
  readerHashSecret,
  secondsUntilUtcDayEnds,
  utcDay,
} from "./reader-hash.util";

/**
 * The reader counters' shared helpers. What is pinned: a reader's day is UTC;
 * a key lives to the end of its UTC day plus the grace; the secret is the
 * first variable set, else a random one with a warning; the hash is 32 hex
 * characters that change with the secret.
 */
describe("reader-hash.util", () => {
  afterEach(() => {
    delete process.env.FIRST_SECRET;
    delete process.env.SECOND_SECRET;
  });

  it("dates a reader by UTC, not by any park's zone", () => {
    expect(utcDay(new Date("2026-10-27T23:59:59Z"))).toBe("2026-10-27");
    expect(utcDay(new Date("2026-10-28T00:00:00Z"))).toBe("2026-10-28");
  });

  it("counts the seconds to the end of the UTC day, plus the grace", () => {
    expect(secondsUntilUtcDayEnds(new Date("2026-10-27T21:15:00Z"), 60)).toBe(
      2 * 3600 + 45 * 60 + 60,
    );
    expect(secondsUntilUtcDayEnds(new Date("2026-10-27T23:59:59.500Z"))).toBe(
      1,
    );
  });

  it("takes the first secret that is set, and warns before making one up", () => {
    const logger = { warn: jest.fn() };
    process.env.SECOND_SECRET = "second";
    expect(
      readerHashSecret(["FIRST_SECRET", "SECOND_SECRET"], logger, "x"),
    ).toBe("second");
    process.env.FIRST_SECRET = " first ";
    expect(
      readerHashSecret(["FIRST_SECRET", "SECOND_SECRET"], logger, "x"),
    ).toBe("first");
    expect(logger.warn).not.toHaveBeenCalled();

    delete process.env.FIRST_SECRET;
    delete process.env.SECOND_SECRET;
    const random = readerHashSecret(["FIRST_SECRET"], logger, "the cap");
    expect(random).toMatch(/^[0-9a-f]{64}$/);
    expect(logger.warn).toHaveBeenCalledWith(
      "FIRST_SECRET not set; the cap uses a per-process secret.",
    );
  });

  it("hashes to 32 hex characters that depend on the secret and every part", () => {
    const key = readerHash("s", "2026-10-27", "203.0.113.7");
    expect(key).toMatch(/^[0-9a-f]{32}$/);
    expect(readerHash("s", "2026-10-27", "203.0.113.7")).toBe(key);
    expect(readerHash("t", "2026-10-27", "203.0.113.7")).not.toBe(key);
    expect(readerHash("s", "2026-10-28", "203.0.113.7")).not.toBe(key);
  });
});
