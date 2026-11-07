import type { Queue } from "bull";
import type { ModuleRef } from "@nestjs/core";
import { getQueueToken } from "@nestjs/bull";
import { BULL_QUEUE_REGISTRATIONS } from "../queues/queue-registrations";
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
function fakeQueue(
  failed: Run[] = [],
  completed: Run[] = [],
  lastSuccess: Record<string, string> = {},
): Queue {
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
      hgetall: jest.fn(async (key: string) =>
        key === `${prefix}:last-success` ? lastSuccess : {},
      ),
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

const REGISTERED = BULL_QUEUE_REGISTRATIONS.map((q) => q.name as string);

/** Every registered queue, failed once; `queues` replaces some by name. */
function build(queues: Record<string, Queue> = {}): AdminQueueStatusService {
  const tokens = new Map(
    REGISTERED.map((name) => [
      getQueueToken(name),
      queues[name] ?? fakeQueue([{ id: "1", name: "job", at: NOW }]),
    ]),
  );
  const service = new AdminQueueStatusService({
    get: (token: string) => tokens.get(token),
  } as unknown as ModuleRef);
  service.onModuleInit();
  return service;
}

describe("AdminQueueStatusService", () => {
  it("counts every registered queue, the cache warmup too", async () => {
    const names = (await build().counts()).map((queue) => queue.name);
    expect(names).toEqual(REGISTERED);
    expect(names).toContain("live-cache-warmup");
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
    expect(await build().failures("no-such-queue", 3)).toBeNull();
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
    build({
      holidays,
      "park-metadata": parkMetadata,
      "park-enrichment": parkEnrichment,
      "ml-training": mlTraining,
      "wait-times": waitTimes,
      "children-metadata": children,
    });

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

  it("settles a failure by the recorded last success when Bull trimmed that run from its completed set", async () => {
    // pcn-shadow: the daily training's success is pushed out of the last 100
    // completed runs by the 15-minute forecast within a day.
    const trainedAt = NOW - 20 * 60 * 60 * 1000;
    const pcn = fakeQueue(
      [{ id: "f9", name: "train-pcn", at: NOW - 6 * DAY }],
      [{ id: "c9", name: "forecast-pcn", at: NOW - 60 * 1000 }],
      { "train-pcn": String(trainedAt) },
    );
    const counts = await build({ "pcn-shadow": pcn }).counts();
    expect(counts.find((q) => q.name === "pcn-shadow")).toMatchObject({
      failed: 1,
      failedOpen: 0,
    });
    const [settled] =
      (await build({ "pcn-shadow": pcn }).failures("pcn-shadow", 1)) ?? [];
    expect(settled.succeededSince).toBe(new Date(trainedAt).toISOString());
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
