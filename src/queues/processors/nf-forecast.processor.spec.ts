import { Logger } from "@nestjs/common";
import { Job } from "bull";
import axios from "axios";
import { Repository } from "typeorm";
import { NfForecastProcessor } from "./nf-forecast.processor";
import { ModelComparison } from "../../ml/entities/model-comparison.entity";

jest.mock("axios");
const mockedAxios = axios as jest.Mocked<typeof axios>;

/**
 * PAR-814: the nightly `train-nf` job must never persist a forecast its own
 * run did not produce. On 2026-10-09 the poll loop ran out after 90 minutes,
 * fell through to nf `/forecast`, and that wrote the previous night's cached
 * parquet under today's forecast_date — 231,480 rows identical to 2026-10-08.
 */
describe("NfForecastProcessor.handleTrainNf", () => {
  const VERSION = "nf20261009_030000";
  let processor: NfForecastProcessor;
  /** Status bodies returned by successive GET /train/status calls. */
  let statuses: Array<Record<string, unknown>>;
  let warn: jest.SpyInstance;

  beforeEach(() => {
    jest.useFakeTimers();
    jest.clearAllMocks();
    statuses = [];
    jest.spyOn(Logger.prototype, "log").mockImplementation(() => undefined);
    jest.spyOn(Logger.prototype, "error").mockImplementation(() => undefined);
    warn = jest
      .spyOn(Logger.prototype, "warn")
      .mockImplementation(() => undefined);
    processor = new NfForecastProcessor(
      {} as unknown as Repository<ModelComparison>,
    );
    mockedAxios.post.mockImplementation(async (url: string) => {
      if (url.endsWith("/train")) {
        return { data: { status: "training_started", version: VERSION } };
      }
      return { data: { status: "ok", rows: 1, persisted: 1 } };
    });
    mockedAxios.get.mockImplementation(async () => ({
      // The last status repeats forever (a hung run keeps saying "training").
      data: statuses.length > 1 ? statuses.shift() : statuses[0],
    }));
  });

  afterEach(() => {
    jest.useRealTimers();
    jest.restoreAllMocks();
  });

  /** Runs the job while advancing the 30 s poll timer until it settles. */
  async function run(): Promise<{ result?: unknown; error?: Error }> {
    let settled = false;
    const p = processor
      .handleTrainNf({} as Job)
      .then((result) => ({ result }))
      .catch((error: Error) => ({ error }))
      .finally(() => {
        settled = true;
      });
    for (let i = 0; i < 400 && !settled; i++) {
      await jest.advanceTimersByTimeAsync(30_000);
    }
    return p;
  }

  const forecastCalls = () =>
    mockedAxios.post.mock.calls.filter(([url]) =>
      String(url).endsWith("/forecast"),
    );

  it("throws on a poll timeout and does not call /forecast", async () => {
    statuses = [
      { is_training: false, status: "completed", version: "nf20261008" }, // pre-check
      { is_training: true, status: "training", version: VERSION },
    ];

    const { error } = await run();

    expect(error?.message).toMatch(/did not complete within 90 min/);
    expect(forecastCalls()).toHaveLength(0);
  });

  it("throws when nf-service restarted mid-run (status reset to idle)", async () => {
    statuses = [
      { is_training: false, status: "completed", version: "nf20261008" },
      { is_training: true, status: "training", version: VERSION },
      {
        is_training: false,
        status: "idle",
        version: VERSION,
        error: "reset on startup (previous run interrupted)",
      },
    ];

    const { error } = await run();

    expect(error?.message).toMatch(/ended without completing: status=idle/);
    expect(forecastCalls()).toHaveLength(0);
  });

  it("throws when the completed status belongs to another run", async () => {
    statuses = [
      { is_training: false, status: "completed", version: "nf20261008" },
      { is_training: false, status: "completed", version: "nf20261008" },
    ];

    const { error } = await run();

    expect(error?.message).toMatch(/version=nf20261008/);
    expect(forecastCalls()).toHaveLength(0);
  });

  it("throws on a failed run", async () => {
    statuses = [
      { is_training: false, status: "completed", version: "nf20261008" },
      { is_training: false, status: "failed", version: VERSION, error: "OOM" },
    ];

    const { error } = await run();

    expect(error?.message).toBe("TFT training failed: OOM");
    expect(forecastCalls()).toHaveLength(0);
  });

  it("succeeds on this run's completion without re-persisting via /forecast", async () => {
    statuses = [
      { is_training: false, status: "completed", version: "nf20261008" },
      { is_training: true, status: "training", version: VERSION },
      {
        is_training: false,
        status: "completed",
        version: VERSION,
        forecast_date: "2026-10-09",
        info: {
          rows: 231480,
          persisted: 231480,
          chunks: { total: 10, ok: 9, skipped: 1 },
        },
      },
    ];
    const { result, error } = await run();

    expect(error).toBeUndefined();
    expect(result).toEqual({ status: "ok", version: VERSION });
    expect(forecastCalls()).toHaveLength(0);
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining("1 of 10 park chunk(s) failed"),
    );
  });

  it("skips when a training is already in flight", async () => {
    statuses = [{ is_training: true, status: "training", version: "other" }];

    const { result } = await run();

    expect(result).toEqual({ status: "skipped" });
    expect(mockedAxios.post).not.toHaveBeenCalled();
  });
});
