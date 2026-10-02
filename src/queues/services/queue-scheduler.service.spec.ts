import { Queue } from "bull";
import {
  materialisedRepeatJobId,
  QueueSchedulerService,
} from "./queue-scheduler.service";

/**
 * PAR-626: a repeatable entry whose next run lost its job hash fails as
 * `__default__` and never schedules the run after it. hasRepeatableJob must
 * see that and hand the entry back for re-registration.
 */

// The entry and the failed job id read from production Redis on 2026-10-02
// (`parkfan:analytics:repeat`, `parkfan:analytics:failed`).
const PERCENTILES_ENTRY = {
  key: "calculate-percentiles:percentiles-cron:::0 2 * * *",
  name: "calculate-percentiles",
  id: "percentiles-cron",
  endDate: null,
  tz: null,
  cron: "0 2 * * *",
  every: null,
  next: 1790906400000,
};
const PERCENTILES_JOB_ID =
  "repeat:53c45fdc9bcc1d462c1cb9d06b461a80:1790906400000";

function fakeQueue(jobHashName: string | null) {
  const hget = jest.fn().mockResolvedValue(jobHashName);
  const queue = {
    getRepeatableJobs: jest.fn().mockResolvedValue([PERCENTILES_ENTRY]),
    removeRepeatableByKey: jest.fn().mockResolvedValue(undefined),
    toKey: (type: string) => `parkfan:analytics:${type}`,
    client: { hget },
  };
  return { queue: queue as unknown as Queue, raw: queue, hget };
}

function hasRepeatableJob(
  service: QueueSchedulerService,
  queue: Queue,
  jobId: string,
): Promise<boolean> {
  return (
    service as unknown as {
      hasRepeatableJob(q: Queue, id: string): Promise<boolean>;
    }
  ).hasRepeatableJob(queue, jobId);
}

describe("QueueSchedulerService — repeatable entry whose next run lost its name (PAR-626)", () => {
  // Every constructor argument is a queue the tested method never touches.
  const service = new (
    QueueSchedulerService as unknown as new (
      ...args: unknown[]
    ) => QueueSchedulerService
  )(...new Array(30).fill({}));

  beforeEach(() => {
    // Inside the window, so the overdue rule does not fire first.
    jest.spyOn(Date, "now").mockReturnValue(PERCENTILES_ENTRY.next - 60_000);
  });

  afterEach(() => jest.restoreAllMocks());

  it("derives the same job id Bull materialised in production", () => {
    expect(materialisedRepeatJobId(PERCENTILES_ENTRY)).toBe(PERCENTILES_JOB_ID);
  });

  it("keeps an entry whose next run still carries its name", async () => {
    const { queue, raw, hget } = fakeQueue("calculate-percentiles");

    await expect(
      hasRepeatableJob(service, queue, "percentiles-cron"),
    ).resolves.toBe(true);
    expect(hget).toHaveBeenCalledWith(
      `parkfan:analytics:${PERCENTILES_JOB_ID}`,
      "name",
    );
    expect(raw.removeRepeatableByKey).not.toHaveBeenCalled();
  });

  it("re-registers an entry whose next run has no name", async () => {
    const { queue, raw } = fakeQueue(null);

    await expect(
      hasRepeatableJob(service, queue, "percentiles-cron"),
    ).resolves.toBe(false);
    expect(raw.removeRepeatableByKey).toHaveBeenCalledWith(
      PERCENTILES_ENTRY.key,
    );
  });

  it("still re-registers an overdue entry without looking at the job", async () => {
    jest
      .spyOn(Date, "now")
      .mockReturnValue(PERCENTILES_ENTRY.next + 5 * 60_000);
    const { queue, raw, hget } = fakeQueue("calculate-percentiles");

    await expect(
      hasRepeatableJob(service, queue, "percentiles-cron"),
    ).resolves.toBe(false);
    expect(raw.removeRepeatableByKey).toHaveBeenCalledWith(
      PERCENTILES_ENTRY.key,
    );
    expect(hget).not.toHaveBeenCalled();
  });

  it("re-checks the schedule every hour, not only at startup", () => {
    jest.useFakeTimers();
    const prev = process.env.SKIP_QUEUE_BOOTSTRAP;
    delete process.env.SKIP_QUEUE_BOOTSTRAP;
    const register = jest
      .spyOn(
        service as unknown as {
          registerScheduledJobs(repair?: boolean): Promise<void>;
        },
        "registerScheduledJobs",
      )
      .mockResolvedValue(undefined);
    try {
      void service.onModuleInit();
      jest.advanceTimersByTime(5000);
      expect(register).toHaveBeenLastCalledWith();
      jest.advanceTimersByTime(60 * 60 * 1000);
      expect(register).toHaveBeenLastCalledWith(true);
      expect(register).toHaveBeenCalledTimes(2);

      service.onModuleDestroy();
      jest.advanceTimersByTime(2 * 60 * 60 * 1000);
      expect(register).toHaveBeenCalledTimes(2);
    } finally {
      if (prev !== undefined) process.env.SKIP_QUEUE_BOOTSTRAP = prev;
      jest.useRealTimers();
    }
  });
});
