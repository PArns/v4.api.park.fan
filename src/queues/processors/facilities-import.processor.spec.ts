import type { Job } from "bull";
import type {
  FacilitiesService,
  FacilityRunResult,
} from "../../facilities/facilities.service";
import {
  FacilitiesImportProcessor,
  PAUSE_BETWEEN_PARKS_MS,
  type FacilityBulkProgress,
  type FacilityImportJobData,
} from "./facilities-import.processor";

const ok = (parkId: string, points = 5): FacilityRunResult => ({
  parkId,
  outcome: "ok",
  points,
  message: null,
  overpassDown: false,
});
const down = (parkId: string): FacilityRunResult => ({
  parkId,
  outcome: "failed",
  points: 0,
  message: "Overpass did not answer: 504",
  overpassDown: true,
});

function setup(
  ids: string[],
  outcome: (parkId: string) => FacilityRunResult = ok,
) {
  /** What happened, in order: imports and pauses. */
  const log: string[] = [];
  let running = 0;
  let overlapped = false;
  const facilities = {
    parkIdsForBulkRun: jest.fn(async () => ids),
    runImport: jest.fn(async (parkId: string) => {
      if (++running > 1) overlapped = true;
      await Promise.resolve();
      running -= 1;
      log.push(parkId);
      return outcome(parkId);
    }),
  };
  class Processor extends FacilitiesImportProcessor {
    protected override pause(ms: number): Promise<void> {
      log.push(`pause ${ms}`);
      return Promise.resolve();
    }
  }
  const job = (data: FacilityImportJobData, progress: unknown = 0) => {
    let stored = progress;
    return {
      data,
      progress: jest.fn((value?: unknown) => {
        if (value === undefined) return stored;
        stored = structuredClone(value);
        return Promise.resolve();
      }),
    } as unknown as Job<FacilityImportJobData>;
  };
  return {
    processor: new Processor(facilities as unknown as FacilitiesService),
    facilities,
    log,
    job,
    overlapped: () => overlapped,
  };
}

describe("FacilitiesImportProcessor", () => {
  it("imports one park, with the outline it was given, and does not pause", async () => {
    const { processor, facilities, log, job } = setup([]);
    const boundary = { type: "way" as const, id: "1" };
    expect(await processor.handle(job({ parkId: "a", boundary }))).toEqual(
      ok("a"),
    );
    expect(facilities.runImport).toHaveBeenCalledWith("a", boundary);
    expect(facilities.parkIdsForBulkRun).not.toHaveBeenCalled();
    expect(log).toEqual(["a"]);
  });

  it("walks the parks one after the other, with the pause after each", async () => {
    const { processor, log, job, overlapped } = setup(["a", "b", "c"]);
    const pause = `pause ${PAUSE_BETWEEN_PARKS_MS}`;
    await processor.handle(job({}));
    expect(log).toEqual(["a", pause, "b", pause, "c", pause]);
    expect(overlapped()).toBe(false);
    expect(PAUSE_BETWEEN_PARKS_MS).toBeGreaterThanOrEqual(10_000);
  });

  it("asks for every park only when told to", async () => {
    const { processor, facilities, job } = setup([]);
    await processor.handle(job({}));
    await processor.handle(job({ all: true }));
    expect(facilities.parkIdsForBulkRun.mock.calls).toEqual([[false], [true]]);
  });

  it("tallies each park's outcome and leaves it as the job's progress", async () => {
    const { processor, job } = setup(["a", "b", "c", "d"], (id) =>
      id === "b"
        ? { ...ok(id, 0), outcome: "no_outline", message: "none" }
        : id === "c"
          ? { ...ok(id, 0), outcome: "failed", message: "not closed" }
          : ok(id, 7),
    );
    const j = job({});
    const tally: FacilityBulkProgress = {
      after: "d",
      total: 4,
      done: 4,
      ok: 2,
      noOutline: 1,
      failed: 1,
      points: 14,
    };
    expect(await processor.handle(j)).toEqual(tally);
    expect(j.progress()).toEqual(tally);
    expect(
      (j.progress as jest.Mock).mock.calls.filter((c) => c.length),
    ).toHaveLength(4);
  });

  it("resumes behind the last park a stalled run finished, keeping its tally", async () => {
    const { processor, log, job } = setup(["a", "b", "c", "d"]);
    const before: FacilityBulkProgress = {
      after: "b",
      total: 4,
      done: 2,
      ok: 1,
      noOutline: 1,
      failed: 0,
      points: 5,
    };
    const result = await processor.handle(job({ all: true }, before));
    expect(log.filter((l) => !l.startsWith("pause"))).toEqual(["c", "d"]);
    expect(result).toMatchObject({
      after: "d",
      total: 4,
      done: 4,
      ok: 3,
      points: 15,
    });
  });

  it("goes on past one park Overpass did not answer for", async () => {
    const { processor, log, job } = setup(["a", "b", "c"], (id) =>
      id === "b" ? down(id) : ok(id),
    );
    expect(await processor.handle(job({}))).toMatchObject({ ok: 2, failed: 1 });
    expect(log.filter((l) => !l.startsWith("pause"))).toEqual(["a", "b", "c"]);
  });

  it("stops when Overpass stays down, with its place kept for the next start", async () => {
    const { processor, log, job } = setup(["a", "b", "c", "d", "e"], (id) =>
      id === "a" ? ok(id) : down(id),
    );
    const j = job({});
    await expect(processor.handle(j)).rejects.toThrow(
      /3 parks in a row; stopped after 4 of 5/,
    );
    expect(log.filter((l) => !l.startsWith("pause"))).toEqual([
      "a",
      "b",
      "c",
      "d",
    ]);
    expect(j.progress()).toMatchObject({ after: "d", ok: 1, failed: 3 });
  });
});
