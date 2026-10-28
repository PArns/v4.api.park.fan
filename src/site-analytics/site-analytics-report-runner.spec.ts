import { ServiceUnavailableException } from "@nestjs/common";
import {
  ReportGate,
  SiteReportRunner,
  envInt,
} from "./site-analytics-report-runner";
import { ROLLED_DAYS_CACHE_MS } from "./site-analytics.constants";

/** A promise a test settles itself. */
function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
}

const tick = () => new Promise((resolve) => setImmediate(resolve));

/**
 * What bounds an admin report. What is pinned: only so many reports run at
 * once and the rest wait their turn in order; a full queue and a wait that
 * lasts too long are refused as 503 "busy" without touching the database; a
 * place is given back when a report fails; a statement the timeout cancelled
 * is a 503 that says what to try, any other error passes through; the timeout
 * is set for the statement's own transaction; the days a cuboid has rolled
 * are read once a minute and a failed read fails the report.
 */
describe("ReportGate", () => {
  it("runs the limit at once, queues the rest in order and hands each place on", async () => {
    const gate = new ReportGate(2, 5, 1000);
    const order: string[] = [];
    const gates = ["a", "b", "c", "d"].map(() => deferred());
    const runs = ["a", "b", "c", "d"].map((name, i) =>
      gate.run(async () => {
        order.push(`start ${name}`);
        await gates[i].promise;
        order.push(`end ${name}`);
        return name;
      }),
    );
    await tick();
    expect(order).toEqual(["start a", "start b"]);
    expect(gate.load).toEqual({ running: 2, waiting: 2 });
    gates[0].resolve();
    await tick();
    expect(order).toEqual(["start a", "start b", "end a", "start c"]);
    gates[1].resolve();
    gates[2].resolve();
    await tick();
    gates[3].resolve();
    await expect(Promise.all(runs)).resolves.toEqual(["a", "b", "c", "d"]);
    expect(gate.load).toEqual({ running: 0, waiting: 0 });
  });

  it("refuses a report at once when the queue is full", async () => {
    const gate = new ReportGate(1, 1, 1000);
    const hold = deferred();
    const first = gate.run(() => hold.promise);
    const second = gate.run(async () => "waits");
    await expect(gate.run(async () => "never")).rejects.toMatchObject({
      response: { statusCode: 503, reason: "busy" },
    });
    hold.resolve();
    await expect(second).resolves.toBe("waits");
    await first;
  });

  it("refuses a report that waited too long, and leaves the queue", async () => {
    jest.useFakeTimers();
    try {
      const gate = new ReportGate(1, 3, 500);
      const hold = deferred();
      const first = gate.run(() => hold.promise);
      const waiting = gate.run(async () => "late");
      const refused = expect(waiting).rejects.toBeInstanceOf(
        ServiceUnavailableException,
      );
      await jest.advanceTimersByTimeAsync(500);
      await refused;
      expect(gate.load).toEqual({ running: 1, waiting: 0 });
      hold.resolve();
      await first;
      expect(gate.load).toEqual({ running: 0, waiting: 0 });
    } finally {
      jest.useRealTimers();
    }
  });

  it("gives its place back when a report fails", async () => {
    const gate = new ReportGate(1, 1, 1000);
    await expect(
      gate.run(async () => {
        throw new Error("boom");
      }),
    ).rejects.toThrow("boom");
    await expect(gate.run(async () => "next")).resolves.toBe("next");
  });
});

describe("envInt", () => {
  afterEach(() => delete process.env.SITE_ANALYTICS_TEST_VALUE);

  it("takes a positive whole number from the environment, else the default", () => {
    expect(envInt("SITE_ANALYTICS_TEST_VALUE", 7)).toBe(7);
    process.env.SITE_ANALYTICS_TEST_VALUE = "12";
    expect(envInt("SITE_ANALYTICS_TEST_VALUE", 7)).toBe(12);
    for (const bad of ["0", "-3", "1.5", "soon", ""]) {
      process.env.SITE_ANALYTICS_TEST_VALUE = bad;
      expect(envInt("SITE_ANALYTICS_TEST_VALUE", 7)).toBe(7);
    }
  });
});

describe("SiteReportRunner", () => {
  let statements: Array<[string, unknown[] | undefined]>;
  let fail: unknown;
  let rolledRows: Array<{ day: string }>;
  let rolledError: Error | null;
  const manager = {
    query: jest.fn(async (sql: string, params?: unknown[]) => {
      statements.push([sql, params]);
      if (fail && !sql.includes("set_config")) throw fail;
      return [{ n: 1 }];
    }),
  };
  const db = {
    transaction: jest.fn(async (work: (m: typeof manager) => unknown) =>
      work(manager),
    ),
    query: jest.fn(async () => {
      if (rolledError) throw rolledError;
      return rolledRows;
    }),
  };
  const build = () => new SiteReportRunner(db as never);

  beforeEach(() => {
    statements = [];
    fail = null;
    rolledRows = [{ day: "2026-10-26" }, { day: "2026-10-27" }];
    rolledError = null;
    manager.query.mockClear();
    db.transaction.mockClear();
    db.query.mockClear();
    delete process.env.SITE_ANALYTICS_STATEMENT_TIMEOUT_MS;
  });

  it("sets the timeout for the statement's own transaction, 15 s unless configured", async () => {
    await expect(build().query("SELECT 1", ["x"])).resolves.toEqual([{ n: 1 }]);
    expect(db.transaction).toHaveBeenCalledTimes(1);
    expect(statements[0][0]).toContain("set_config('statement_timeout', $1");
    expect(statements[0][0]).toContain("set_config('work_mem', $2");
    expect(statements[0][0]).toContain(
      "set_config('timescaledb.enable_chunkwise_aggregation', 'off', true)",
    );
    expect(statements[0][1]).toEqual(["15000", "32MB"]);
    expect(statements[1]).toEqual(["SELECT 1", ["x"]]);

    process.env.SITE_ANALYTICS_STATEMENT_TIMEOUT_MS = "4000";
    process.env.SITE_ANALYTICS_REPORT_WORK_MEM_MB = "128";
    statements = [];
    await build().query("SELECT 2");
    expect(statements[0][1]).toEqual(["4000", "128MB"]);
    delete process.env.SITE_ANALYTICS_REPORT_WORK_MEM_MB;
  });

  it("answers a cancelled statement as 503 with what to try", async () => {
    process.env.SITE_ANALYTICS_STATEMENT_TIMEOUT_MS = "5000";
    fail = Object.assign(
      new Error("canceling statement due to statement timeout"),
      { driverError: { code: "57014" } },
    );
    const error = await build()
      .query("SELECT slow")
      .catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ServiceUnavailableException);
    expect((error as ServiceUnavailableException).getResponse()).toMatchObject({
      statusCode: 503,
      reason: "timeout",
      message:
        "The report took longer than 5 s and was stopped. Try a shorter window, a coarser interval or a filter.",
    });
  });

  it("lets every other error through, a user's cancel included", async () => {
    fail = new Error("relation does not exist");
    await expect(build().query("SELECT 1")).rejects.toThrow(
      "relation does not exist",
    );
    fail = Object.assign(new Error("canceling statement due to user request"), {
      code: "57014",
    });
    await expect(build().query("SELECT 1")).rejects.toThrow("user request");
  });

  it("reads the days a cuboid has rolled once, and again after a minute", async () => {
    jest.useFakeTimers({ now: new Date("2026-10-28T07:00:00Z") });
    try {
      const runner = build();
      const days = await runner.rolledDays("pageviews", 7);
      expect([...days]).toEqual(["2026-10-26", "2026-10-27"]);
      await runner.rolledDays("pageviews", 7);
      expect(db.query).toHaveBeenCalledTimes(1);
      expect(db.query.mock.calls[0]).toEqual([
        expect.stringContaining("FROM site_rollup_days"),
        ["pageviews", 7],
      ]);
      await runner.rolledDays("events", 7);
      expect(db.query).toHaveBeenCalledTimes(2);
      jest.setSystemTime(Date.now() + ROLLED_DAYS_CACHE_MS + 1);
      await runner.rolledDays("pageviews", 7);
      expect(db.query).toHaveBeenCalledTimes(3);
    } finally {
      jest.useRealTimers();
    }
  });

  it("fails when the markers cannot be read, and tries again next time", async () => {
    rolledError = new Error("down");
    const runner = build();
    await expect(runner.rolledDays("pageviews", 1)).rejects.toThrow("down");
    rolledError = null;
    expect((await runner.rolledDays("pageviews", 1)).size).toBe(2);
  });

  it("runs reports through the gate", async () => {
    process.env.SITE_ANALYTICS_REPORT_CONCURRENCY = "1";
    process.env.SITE_ANALYTICS_REPORT_QUEUE = "1";
    try {
      const runner = build();
      const hold = deferred();
      const first = runner.run(() => hold.promise);
      const queued = runner.run(async () => "queued");
      await expect(runner.run(async () => "x")).rejects.toMatchObject({
        response: { reason: "busy" },
      });
      hold.resolve();
      await expect(queued).resolves.toBe("queued");
      await first;
    } finally {
      delete process.env.SITE_ANALYTICS_REPORT_CONCURRENCY;
      delete process.env.SITE_ANALYTICS_REPORT_QUEUE;
    }
  });
});
