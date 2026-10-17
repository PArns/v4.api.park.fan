import { Logger } from "@nestjs/common";
import { QueueBootstrapService } from "./queue-bootstrap.service";

/**
 * PAR-821: the bootstrap used to run `queue.clean(0, "completed"|"failed")` on
 * six queues at every start, so each deploy erased the failures the nightly
 * failing-job sweep exists to report. Retention is bounded by QueuesModule's
 * defaultJobOptions; a boot must never delete job history.
 */
describe("QueueBootstrapService", () => {
  const queue = () => ({
    add: jest.fn().mockResolvedValue({}),
    getActive: jest.fn().mockResolvedValue([]),
    getWaiting: jest.fn().mockResolvedValue([]),
    clean: jest.fn().mockResolvedValue([]),
  });

  const build = (parkCount: number) => {
    const queues = Array.from({ length: 7 }, queue);
    const repo = {
      count: jest.fn().mockResolvedValue(parkCount),
      findOne: jest.fn().mockResolvedValue(null),
      query: jest.fn().mockResolvedValue([]),
    };
    const service = new (
      QueueBootstrapService as unknown as new (
        ...args: unknown[]
      ) => QueueBootstrapService
    )(...queues, repo, repo, repo);
    return { service, queues };
  };

  it.each([
    ["an empty database", 0],
    ["a populated database", 120],
  ])("never cleans a queue on boot (%s)", async (_label, parkCount) => {
    const { service, queues } = build(parkCount);

    await (
      service as unknown as { bootstrapQueues(): Promise<void> }
    ).bootstrapQueues();

    for (const q of queues) {
      expect(q.clean).not.toHaveBeenCalled();
    }
    // The bootstrap still did its actual job: something was enqueued.
    expect(queues.some((q) => q.add.mock.calls.length > 0)).toBe(true);
  });
});

describe("QueueBootstrapService — shutdown during the boot chain", () => {
  it("stops at the next step without an error line and awaits the step in flight", async () => {
    let releaseCount!: (n: number) => void;
    const countGate = new Promise<number>(
      (resolve) => (releaseCount = resolve),
    );
    const queues = Array.from({ length: 7 }, () => ({
      add: jest.fn().mockResolvedValue({}),
      getActive: jest.fn().mockResolvedValue([]),
      getWaiting: jest.fn().mockResolvedValue([]),
    }));
    const repo = {
      count: jest.fn().mockReturnValue(countGate),
      findOne: jest.fn().mockResolvedValue(null),
      query: jest.fn().mockResolvedValue([]),
    };
    const service = new (
      QueueBootstrapService as unknown as new (
        ...args: unknown[]
      ) => QueueBootstrapService
    )(...queues, repo, repo, repo);
    const error = jest
      .spyOn(Logger.prototype, "error")
      .mockImplementation(() => undefined);
    jest.spyOn(Logger.prototype, "log").mockImplementation(() => undefined);
    const warn = jest
      .spyOn(Logger.prototype, "warn")
      .mockImplementation(() => undefined);
    const prev = process.env.SKIP_QUEUE_BOOTSTRAP;
    delete process.env.SKIP_QUEUE_BOOTSTRAP;

    try {
      await service.onModuleInit();
      expect(repo.count).toHaveBeenCalledTimes(1);

      const destroyed = service.onModuleDestroy();
      releaseCount(120);
      await destroyed;

      expect(queues.every((q) => q.add.mock.calls.length === 0)).toBe(true);
      expect(repo.findOne).not.toHaveBeenCalled();
      expect(error).not.toHaveBeenCalled();
      expect(warn).not.toHaveBeenCalled();
    } finally {
      if (prev !== undefined) process.env.SKIP_QUEUE_BOOTSTRAP = prev;
      jest.restoreAllMocks();
    }
  });
});
