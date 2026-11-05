import type { Queue } from "bull";
import { AdminQueueStatusService } from "./admin-queue-status.service";

const DAY = 24 * 60 * 60 * 1000;
const NOW = Date.parse("2026-10-30T12:00:00Z");

interface Run {
  id: string;
  name: string;
  /** When it finished, ms. */
  at: number;
}

/**
 * A queue over Bull's Redis layout: the `failed` and `completed` sets scored
 * by finish time, a hash per job holding its name, and `getFailed` newest
 * first, as Bull answers it.
 */
function fakeQueue(failed: Run[] = [], completed: Run[] = []): Queue {
  const prefix = "parkfan:q";
  const byScore = (runs: Run[]) => [...runs].sort((a, b) => a.at - b.at);
  const jobs = new Map(
    [...failed, ...completed].map((run) => [`${prefix}:${run.id}`, run]),
  );
  return {
    toKey: (typeOrId: string) => `${prefix}:${typeOrId}`,
    client: {
      zrange: jest.fn(async (key: string, start: number, stop: number) => {
        if (key !== `${prefix}:failed`) return [];
        const sorted = byScore(failed);
        const from = start < 0 ? Math.max(sorted.length + start, 0) : start;
        const to = stop < 0 ? sorted.length + stop : stop;
        return sorted
          .slice(from, to + 1)
          .flatMap((run) => [run.id, String(run.at)]);
      }),
      zrangebyscore: jest.fn(async (key: string, min: string) => {
        if (key !== `${prefix}:completed`) return [];
        const floor = Number(min.replace("(", ""));
        return byScore(completed)
          .filter((run) => run.at > floor)
          .flatMap((run) => [run.id, String(run.at)]);
      }),
      hmget: jest.fn(async (key: string) => [jobs.get(key)?.name ?? null]),
    },
    getActiveCount: jest.fn().mockResolvedValue(0),
    getWaitingCount: jest.fn().mockResolvedValue(0),
    getFailedCount: jest.fn().mockResolvedValue(failed.length),
    getDelayedCount: jest.fn().mockResolvedValue(0),
    getCompletedCount: jest.fn().mockResolvedValue(completed.length),
    getFailed: jest.fn(async (start: number, end: number) =>
      byScore(failed)
        .reverse()
        .slice(start, end + 1)
        .map((run) => ({
          id: run.id,
          name: run.name,
          data: {},
          attemptsMade: 3,
          failedReason: `${run.name} broke`,
          stacktrace: [`Error: ${run.name} broke\n    at x`],
          finishedOn: run.at,
        })),
    ),
  } as unknown as Queue;
}

/** The fifteen queues in constructor order; `queues` replaces the first ones. */
function build(...queues: Queue[]): AdminQueueStatusService {
  const all = Array.from(
    { length: 15 },
    (_, i) => queues[i] ?? fakeQueue([{ id: "1", name: "job", at: NOW }]),
  );
  return new (
    AdminQueueStatusService as unknown as new (
      ...args: Queue[]
    ) => AdminQueueStatusService
  )(...all);
}

describe("AdminQueueStatusService", () => {
  it("counts the Search Console queue, whose import the admin starts by hand", async () => {
    const names = (await build().counts()).map((queue) => queue.name);
    expect(names).toEqual(
      expect.arrayContaining([
        "search-console",
        "downtime",
        "pcn-shadow",
        "park-day-operation",
      ]),
    );
  });

  it("reads the failures of every queue it counts", async () => {
    const service = build();
    const counted = (await service.counts()).map((queue) => queue.name);
    expect(service.inspectableQueues()).toEqual(counted);
    for (const name of counted) {
      const failures = await service.failures(name, 3);
      expect(failures?.[0]?.failedReason).toBe("job broke");
    }
  });

  it("knows no queue it does not count", async () => {
    expect(await build().failures("toString", 3)).toBeNull();
    expect(await build().failures("weather", 3)).toBeNull();
  });

  // The admin's card on 2026-10-30: three red queues, two of them settled.
  // ml-training's train-model failed on 24.10 and trained every morning
  // since; children-metadata's weekly sync failed on 25.10 and has not run.
  const holidays = fakeQueue();
  const parkMetadata = fakeQueue();
  const parkEnrichment = fakeQueue();
  const mlTraining = fakeQueue(
    [{ id: "f1", name: "train-model", at: NOW - 6 * DAY }],
    [
      { id: "c1", name: "train-model", at: NOW - 6 * DAY - 1000 },
      { id: "c2", name: "train-model", at: NOW - 5 * 60 * 60 * 1000 },
    ],
  );
  const waitTimes = fakeQueue();
  const children = fakeQueue(
    [{ id: "f2", name: "sync-attraction-details", at: NOW - 5 * DAY }],
    [{ id: "c3", name: "fetch-all-children", at: NOW - DAY }],
  );
  const service = () =>
    build(
      holidays,
      parkMetadata,
      parkEnrichment,
      mlTraining,
      waitTimes,
      children,
    );

  it("counts a failure as open only until a later run of the same job succeeds", async () => {
    const counts = await service().counts();
    const of = (name: string) => counts.find((q) => q.name === name);
    expect(of("ml-training")).toMatchObject({ failed: 1, failedOpen: 0 });
    // Another job of the queue succeeding settles nothing.
    expect(of("children-metadata")).toMatchObject({
      failed: 1,
      failedOpen: 1,
    });
    expect(of("wait-times")).toMatchObject({ failed: 0, failedOpen: 0 });
  });

  it("says when a job failed and since when it has run again", async () => {
    const [settled] = (await service().failures("ml-training", 3)) ?? [];
    expect(settled).toMatchObject({
      name: "train-model",
      failedAt: new Date(NOW - 6 * DAY).toISOString(),
      succeededSince: new Date(NOW - 5 * 60 * 60 * 1000).toISOString(),
    });
    const [open] = (await service().failures("children-metadata", 3)) ?? [];
    expect(open).toMatchObject({
      name: "sync-attraction-details",
      failedAt: new Date(NOW - 5 * DAY).toISOString(),
      succeededSince: null,
    });
  });
});
