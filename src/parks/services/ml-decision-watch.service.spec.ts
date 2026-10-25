import {
  DRIFT_WATCH_KEY,
  MlDecisionWatchService,
} from "./ml-decision-watch.service";

/** After the 09.11. scoring: live_chronos and planner_chronos are due. */
const NOW = new Date("2026-11-09T12:30:00Z");

function matrix(lastDate: string) {
  return {
    generatedAt: NOW.toISOString(),
    window: { days: 28, firstDate: "2026-10-12", lastDate },
    rule: { minDays: 14, minParks: 10 },
    models: [],
    groups: [],
    questions: [],
    summary: { better: 0, pending: 0, unclear: 0, worse: 0 },
  };
}

function make(
  existing: Array<{ id: string; on: string }>,
  lastDate = "2026-11-08",
) {
  const inserted: unknown[][] = [];
  const query = jest.fn(async (sql: string, params: unknown[] = []) => {
    if (sql.startsWith("SET LOCAL")) return [];
    if (sql.includes("FROM ml_decision_reports")) return existing;
    if (sql.includes("INSERT INTO ml_decision_reports")) {
      inserted.push(params);
      // What the table does on a repeat: nothing.
      existing.push({ id: params[0] as string, on: params[1] as string });
      return [];
    }
    if (sql.includes("FROM forecast_archive_paired")) {
      return [{ parkId: "p1", date: "2026-11-01" }];
    }
    if (sql.includes("FROM parks")) {
      return [
        {
          id: "p1",
          countryCode: "DE",
          regionCode: "BW",
          timezone: "Europe/Berlin",
        },
      ];
    }
    return [];
  });
  const ds = {
    query,
    transaction: jest.fn(async (fn: (m: unknown) => unknown) => fn({ query })),
  };
  const redis = {
    set: jest.fn().mockResolvedValue("OK"),
    del: jest.fn().mockResolvedValue(1),
  };
  const comparison = { getMatrix: jest.fn(async () => matrix(lastDate)) };
  const holidays = { getHolidays: jest.fn(async () => []) };
  const service = new MlDecisionWatchService(
    ds as never,
    redis as never,
    comparison as never,
    holidays as never,
  );
  return { service, inserted, comparison, redis, query };
}

describe("MlDecisionWatchService.freezeDueReports", () => {
  it("freezes each due decision once, on the window ending evaluateOn − 1", async () => {
    const { service, inserted, comparison } = make([]);
    const written = await service.freezeDueReports(NOW);
    expect(written.sort()).toEqual(["live_chronos", "planner_chronos"]);
    expect(comparison.getMatrix).toHaveBeenCalledWith(
      28,
      "ALL",
      NOW,
      "2026-11-08",
    );
    expect(inserted).toHaveLength(2);
    const report = JSON.parse(inserted[0][4] as string);
    expect(report).toMatchObject({
      evaluateOn: "2026-11-09",
      window: { lastDate: "2026-11-08" },
      parks: 1,
      days: 1,
      holidayShare: 0,
    });
  });

  it("is idempotent: a second run neither recomputes nor writes", async () => {
    const { service, inserted, comparison } = make([]);
    await service.freezeDueReports(NOW);
    comparison.getMatrix.mockClear();
    const again = await service.freezeDueReports(
      new Date("2026-11-20T12:30:00Z"),
    );
    expect(again).toEqual([]);
    expect(inserted).toHaveLength(2);
    expect(comparison.getMatrix).not.toHaveBeenCalled();
  });

  it("never overwrites: the INSERT does nothing on conflict", async () => {
    const { service, query } = make([]);
    await service.freezeDueReports(NOW);
    const sql = query.mock.calls
      .map((c) => c[0] as string)
      .find((s) => s.includes("INSERT INTO ml_decision_reports"))!;
    expect(sql).toContain("ON CONFLICT (decision_id, evaluate_on) DO NOTHING");
  });

  it("waits while the window's last date is not scored yet", async () => {
    const { service, inserted } = make([], "2026-11-07");
    expect(await service.freezeDueReports(NOW)).toEqual([]);
    expect(inserted).toHaveLength(0);
  });
});

describe("MlDecisionWatchService.computeDrift", () => {
  it("joins the weekday median on (ride, hour) keys, never an OR filter", async () => {
    const { service, query } = make([]);
    await service.computeDrift(NOW);
    const sql = query.mock.calls
      .map((c) => c[0] as string)
      .find((s) => s.includes("FROM prediction_accuracy"))!;
    expect(sql).toContain("CROSS JOIN generate_series(1, 4) w");
    expect(sql).toContain(
      "JOIN h ON h.a = s.a AND h.lt = s.lt - w * interval '7 days'",
    );
    // The self-join runs on one row per ride-hour, never on raw r.
    expect(sql).toContain("FROM h s");
    expect(sql).toMatch(
      /h AS \(\s*SELECT a, lt, avg\(actual\)[\s\S]*GROUP BY a, lt/,
    );
    expect(sql).not.toMatch(/h\.lt IN \(/);
  });

  it("stores the watch for the overview", async () => {
    const { service, redis } = make([]);
    const w = await service.computeDrift(NOW);
    expect(w.status).toBe("no_data");
    expect(redis.set).toHaveBeenCalledWith(
      DRIFT_WATCH_KEY,
      expect.any(String),
      "EX",
      expect.any(Number),
    );
  });
});
