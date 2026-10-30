import type { Queue } from "bull";
import { AdminQueueStatusService } from "./admin-queue-status.service";

/** A queue that answers its counts and one failed job. */
function fakeQueue(): Queue {
  return {
    getActiveCount: jest.fn().mockResolvedValue(0),
    getWaitingCount: jest.fn().mockResolvedValue(0),
    getFailedCount: jest.fn().mockResolvedValue(1),
    getDelayedCount: jest.fn().mockResolvedValue(0),
    getCompletedCount: jest.fn().mockResolvedValue(3),
    getFailed: jest.fn().mockResolvedValue([
      {
        id: 7,
        name: "job",
        data: {},
        attemptsMade: 1,
        failedReason: "boom",
        stacktrace: ["Error: boom\n    at x"],
      },
    ]),
  } as unknown as Queue;
}

function build(): AdminQueueStatusService {
  const queues = Array.from({ length: 15 }, fakeQueue);
  return new (
    AdminQueueStatusService as unknown as new (
      ...args: Queue[]
    ) => AdminQueueStatusService
  )(...queues);
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
      expect(failures?.[0]?.failedReason).toBe("boom");
    }
  });

  it("knows no queue it does not count", async () => {
    expect(await build().failures("toString", 3)).toBeNull();
    expect(await build().failures("weather", 3)).toBeNull();
  });
});
