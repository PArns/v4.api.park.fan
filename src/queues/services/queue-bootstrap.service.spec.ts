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
