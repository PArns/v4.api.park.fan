import axios from "axios";
import {
  ML_OVERVIEW_CACHE_KEY,
  MlOverviewService,
  chronosState,
  trainingFailedAfter,
  trainingNote,
} from "./ml-overview.service";
import { CHRONOS_LAST_RUN_KEY } from "../parks/services/chronos-shadow.service";
import { DRIFT_WATCH_KEY } from "../parks/services/ml-decision-watch.service";

jest.mock("axios");
const mockedGet = axios.get as jest.Mock;

const NOW = new Date("2026-10-11T20:20:00Z");

/** A DataSource whose queries answer by table name; SET LOCAL is a no-op. */
function dataSource(rows: Record<string, unknown[]>) {
  const query = jest.fn(async (sql: string) => {
    if (sql.startsWith("SET LOCAL")) return [];
    for (const [table, r] of Object.entries(rows)) {
      if (sql.includes(`FROM ${table}`)) return r;
    }
    return [];
  });
  return {
    query,
    transaction: jest.fn(async (fn: (m: unknown) => unknown) => fn({ query })),
  };
}

function make(
  rows: Record<string, unknown[]>,
  redisValues: Record<string, string> = {},
  matrix: unknown = null,
) {
  const redis = {
    get: jest.fn(async (k: string) => redisValues[k] ?? null),
    set: jest.fn().mockResolvedValue("OK"),
  };
  const comparison = {
    getMatrix: jest.fn(async () => {
      if (!matrix) throw new Error("no matrix");
      return matrix;
    }),
  };
  const ds = dataSource(rows);
  const service = new MlOverviewService(
    ds as never,
    redis as never,
    comparison as never,
  );
  return { service, redis, comparison, ds };
}

describe("MlOverviewService", () => {
  const env = { ...process.env };
  beforeEach(() => {
    mockedGet.mockReset();
    mockedGet.mockRejectedValue(new Error("unreachable"));
    delete process.env.SERVE_PCN_INTRADAY;
    delete process.env.CHRONOS_SHADOW_ENABLED;
  });
  afterAll(() => {
    process.env = env;
  });

  it("reads empty tables as unknown, never as a failure", async () => {
    const { service } = make({});
    const out = await service.build(NOW);
    expect(out.components.map((c) => c.id)).toEqual([
      "pcn",
      "tft",
      "catboost",
      "chronos",
      "h5",
      "archive",
      "scoring",
    ]);
    for (const c of out.components) {
      expect(c.status).toBe("unknown");
      expect(c.lastRunAt).toBeNull();
    }
    // Without a matrix the summary is zero and nothing is ready.
    expect(out.matrixSummary).toEqual({
      better: 0,
      pending: 0,
      unclear: 0,
      worse: 0,
    });
    expect(out.decisions).toHaveLength(5);
    expect(out.decisions.some((d) => d.state === "ready")).toBe(false);
  });

  it("carries the drift watch and stored reports without re-reading their window (PAR-1059)", async () => {
    const drift = {
      computedAt: NOW.toISOString(),
      status: "drift",
      models: [],
    };
    const report = {
      decisionId: "live_chronos",
      evaluateOn: "2026-11-09",
      verdict: "open",
      warning: true,
    };
    const { service } = make(
      { ml_decision_reports: [{ report }] },
      { [DRIFT_WATCH_KEY]: JSON.stringify(drift) },
    );
    const out = await service.build(new Date("2026-11-10T08:00:00Z"));
    expect(out.drift).toEqual(drift);
    const live = out.decisions.find((d) => d.id === "live_chronos")!;
    expect(live).toMatchObject({ state: "open", evaluated: true, report });
    const planner = out.decisions.find((d) => d.id === "planner_chronos")!;
    expect(planner.report).toBeUndefined();
  });

  it("reads no frozen window for a date every decision of which has a report", async () => {
    const reports = ["live_chronos", "planner_chronos"].map((id) => ({
      report: { decisionId: id, evaluateOn: "2026-11-09", verdict: "open" },
    }));
    const { service, comparison } = make({ ml_decision_reports: reports });
    await service.build(new Date("2026-11-10T08:00:00Z"));
    expect(comparison.getMatrix).toHaveBeenCalledTimes(1);
  });

  it("derives ok / late from the newest run of each component", async () => {
    process.env.SERVE_PCN_INTRADAY = "true";
    const { service } = make({
      pcn_forecasts: [
        { at: "2026-10-11T20:16:48Z", version: "pcn20261011_201501" },
      ],
      tft_forecasts: [
        { at: "2026-10-11T03:11:32Z", version: "nf20261011_030000" },
      ],
      ml_models: [{ at: "2026-10-11T07:11:02Z", version: "v20261011_0600" }],
      forecast_archive_curves: [
        { surface: 1, at: "2026-10-11T20:05:46Z" },
        { surface: 4, at: "2026-10-11T20:05:14Z" },
        { surface: 6, at: "2026-10-11T20:05:46Z" },
      ],
      forecast_archive_scores: [
        { at: "2026-10-11T12:00:00Z", date: "2026-10-10" },
      ],
    });
    const out = await service.build(NOW);
    const by = Object.fromEntries(out.components.map((c) => [c.id, c]));
    expect(by.pcn).toMatchObject({
      status: "ok",
      role: "served",
      note: "Version pcn20261011_201501",
    });
    expect(by.tft).toMatchObject({
      status: "ok",
      note: "Version nf20261011_030000",
    });
    expect(by.catboost).toMatchObject({
      status: "ok",
      lastRunAt: "2026-10-11T07:11:02.000Z",
      note: "Version v20261011_0600",
    });
    expect(by.chronos).toMatchObject({
      status: "ok",
      lastRunAt: "2026-10-11T20:05:46.000Z",
    });
    expect(by.h5.status).toBe("ok");
    expect(by.archive.lastRunAt).toBe("2026-10-11T20:05:46.000Z");
    expect(by.scoring).toMatchObject({
      status: "ok",
      note: "zuletzt bewertet: Tag 10.10.",
    });
  });

  it("marks a component late once its newest run is older than its bound", async () => {
    const { service } = make({
      // Two days old, past TFT's 30-hour bound.
      tft_forecasts: [
        { at: "2026-10-09T03:11:32Z", version: "nf20261009_030000" },
      ],
    });
    const out = await service.build(NOW);
    const tft = out.components.find((c) => c.id === "tft");
    expect(tft?.status).toBe("late");
  });

  it("marks PCN as shadow when it is not served", async () => {
    const { service } = make({});
    const out = await service.build(NOW);
    expect(out.components[0]).toMatchObject({
      id: "pcn",
      role: "shadow",
    });
    expect(out.components[0].note).toContain("SERVE_PCN_INTRADAY");
  });

  // PAR-1053: fresh forecasts from the previous model hid a failed training.
  it("marks PCN failed from pcn-service's training status", async () => {
    process.env.SERVE_PCN_INTRADAY = "true";
    mockedGet.mockImplementation(async (url: string) => {
      if (url.endsWith("/train/status") && url.includes("pcn")) {
        return {
          data: {
            is_training: false,
            status: "failed",
            version: "pcn20261011_200000",
            error: "CUDA out of memory",
          },
        };
      }
      throw new Error("unreachable");
    });
    const { service } = make({
      pcn_forecasts: [
        { at: "2026-10-11T20:16:48Z", version: "pcn20261010_201501" },
      ],
    });
    const out = await service.build(NOW);
    expect(out.components[0]).toMatchObject({
      id: "pcn",
      status: "failed",
      note: "Letztes Training fehlgeschlagen: CUDA out of memory · Version pcn20261010_201501",
    });
  });

  it("reports Chronos warming from the archive's last run summary", async () => {
    const { service } = make(
      {
        forecast_archive_curves: [{ surface: 6, at: "2026-10-11T20:05:46Z" }],
      },
      {
        [CHRONOS_LAST_RUN_KEY]: JSON.stringify({
          at: "2026-10-11T20:06:00Z",
          calls: 70,
          ok: 12,
          warming: 58,
          failures: 0,
          skipped: 0,
        }),
      },
    );
    const out = await service.build(NOW);
    const chronos = out.components.find((c) => c.id === "chronos");
    expect(chronos).toMatchObject({
      status: "warming",
      note: "lädt Historie (12 von 70 Parks liefern schon)",
    });
  });

  it("serves the cached page without touching the database", async () => {
    const cached = { generatedAt: "x", decisions: [], components: [] };
    const { service, ds } = make(
      {},
      { [ML_OVERVIEW_CACHE_KEY]: JSON.stringify(cached) },
    );
    expect(await service.getOverview(NOW)).toEqual(cached);
    expect(ds.transaction).not.toHaveBeenCalled();
  });

  it("caches a fresh build for five minutes", async () => {
    const { service, redis } = make({});
    await service.getOverview(NOW);
    expect(redis.set).toHaveBeenCalledWith(
      ML_OVERVIEW_CACHE_KEY,
      expect.any(String),
      "EX",
      300,
    );
  });
});

describe("trainingFailedAfter / trainingNote", () => {
  const last = new Date("2026-10-11T03:11:00Z");

  it("flags a failed run only when it ended after the newest output", () => {
    expect(
      trainingFailedAfter(
        { status: "failed", finished_at: "2026-10-11T04:00:00Z" },
        last,
      ),
    ).toBe(true);
    expect(
      trainingFailedAfter(
        { status: "failed", finished_at: "2026-10-10T04:00:00Z" },
        last,
      ),
    ).toBe(false);
    expect(trainingFailedAfter({ status: "completed" }, last)).toBe(false);
    expect(trainingFailedAfter(null, last)).toBe(false);
  });

  it("writes the note in plain German", () => {
    expect(trainingNote({ status: "failed", error: "OOM" }, true, "v1")).toBe(
      "Letztes Training fehlgeschlagen: OOM · Version v1",
    );
    expect(trainingNote({ is_training: true }, false, "v1")).toBe(
      "trainiert gerade · Version v1",
    );
    expect(trainingNote(null, false, null)).toBeNull();
  });
});

describe("chronosState", () => {
  const now = new Date("2026-10-11T20:20:00Z");
  const run = (
    o: Partial<Record<string, number>>,
    at = "2026-10-11T20:06:00Z",
  ) => ({
    at,
    calls: 10,
    ok: 10,
    warming: 0,
    failures: 0,
    skipped: 0,
    ...o,
  });

  it("is failed when nothing in the last run answered", () => {
    expect(
      chronosState(
        run({ ok: 0, failures: 3, skipped: 7 }),
        null,
        null,
        now,
        true,
      ),
    ).toMatchObject({ override: "failed" });
  });

  it("ignores a stale summary", () => {
    expect(
      chronosState(
        run({ warming: 5 }, "2026-10-11T10:00:00Z"),
        null,
        null,
        now,
        true,
      ).override,
    ).toBeNull();
  });

  it("says when it is switched off", () => {
    expect(chronosState(null, null, null, now, false).note).toContain(
      "CHRONOS_SHADOW_ENABLED",
    );
    expect(chronosState(null, { enabled: false }, null, now, true).note).toBe(
      "in nf-service abgeschaltet",
    );
  });
});
