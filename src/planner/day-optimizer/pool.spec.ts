import { mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import {
  DayOptimizerBusyError,
  DayOptimizerPool,
  DayOptimizerTimeoutError,
  compiledWorkerFile,
} from "./pool";

/**
 * The pool's mechanics, against stand-in workers: the real one is compiled JavaScript that only
 * exists after `nest build`, and what it computes is the engine, which has its own specs.
 */
describe("DayOptimizerPool", () => {
  let dir: string;
  const worker = (name: string, body: string) => {
    const file = join(dir, name);
    writeFileSync(
      file,
      `const { parentPort } = require("worker_threads");\nparentPort.on("message", (job) => {${body}});\n`,
    );
    return file;
  };

  beforeAll(() => {
    dir = mkdtempSync(join(tmpdir(), "day-optimizer-pool-"));
  });
  afterAll(() => rmSync(dir, { recursive: true, force: true }));

  it("runs inline where there is no compiled worker", async () => {
    expect(compiledWorkerFile()).toBeNull();
    const pool = new DayOptimizerPool({
      size: 1,
      maxQueue: 1,
      timeoutMs: 1000,
      workerFile: null,
    });
    const answer = await pool.run({
      day: null,
      request: { mode: "day", entries: [] },
      clock: { phase: "past" },
    });
    expect(answer.refusal).toBe("past-day");
    expect(pool.threaded).toBe(false);
  });

  it("answers every job from a thread, in parallel up to its size", async () => {
    const pool = new DayOptimizerPool({
      size: 2,
      maxQueue: 10,
      timeoutMs: 5000,
      workerFile: worker(
        "echo.js",
        `setTimeout(() => parentPort.postMessage({ id: job.id, result: { mode: job.request.mode, clock: job.clock } }), 20);`,
      ),
    });
    const answers = await Promise.all(
      [1, 2, 3, 4].map((n) =>
        pool.run({
          day: null,
          request: { mode: "day", entries: [] },
          clock: { phase: "today", nowMinute: n },
        }),
      ),
    );
    expect(
      answers.map((a) => (a.clock as { nowMinute: number }).nowMinute),
    ).toEqual([1, 2, 3, 4]);
    await pool.close();
  });

  it("passes a worker's error on to the caller", async () => {
    const pool = new DayOptimizerPool({
      size: 1,
      maxQueue: 1,
      timeoutMs: 5000,
      workerFile: worker(
        "fail.js",
        `parentPort.postMessage({ id: job.id, error: "bad day" });`,
      ),
    });
    await expect(
      pool.run({
        day: null,
        request: { mode: "day", entries: [] },
        clock: { phase: "future" },
      }),
    ).rejects.toThrow("bad day");
    await pool.close();
  });

  it("stops a job that runs too long and keeps serving", async () => {
    const pool = new DayOptimizerPool({
      size: 1,
      maxQueue: 2,
      timeoutMs: 100,
      workerFile: worker(
        "slow.js",
        `if (job.clock.phase === "future") return; parentPort.postMessage({ id: job.id, result: { status: "fit" } });`,
      ),
    });
    const job = { day: null, request: { mode: "day" as const, entries: [] } };
    await expect(
      pool.run({ ...job, clock: { phase: "future" } }),
    ).rejects.toBeInstanceOf(DayOptimizerTimeoutError);
    await expect(
      pool.run({ ...job, clock: { phase: "past" } }),
    ).resolves.toEqual({ status: "fit" });
    await pool.close();
  });

  it("refuses once the queue is full", async () => {
    const pool = new DayOptimizerPool({
      size: 1,
      maxQueue: 1,
      timeoutMs: 5000,
      workerFile: worker(
        "hold.js",
        `setTimeout(() => parentPort.postMessage({ id: job.id, result: {} }), 200);`,
      ),
    });
    const job = {
      day: null,
      request: { mode: "day" as const, entries: [] },
      clock: { phase: "future" as const },
    };
    const first = pool.run(job);
    const second = pool.run(job);
    await expect(pool.run(job)).rejects.toBeInstanceOf(DayOptimizerBusyError);
    await expect(Promise.all([first, second])).resolves.toHaveLength(2);
    await pool.close();
  });
});
