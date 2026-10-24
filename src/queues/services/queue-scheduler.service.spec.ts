import { Logger } from "@nestjs/common";
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

      void service.onModuleDestroy();
      jest.advanceTimersByTime(2 * 60 * 60 * 1000);
      expect(register).toHaveBeenCalledTimes(2);
    } finally {
      if (prev !== undefined) process.env.SKIP_QUEUE_BOOTSTRAP = prev;
      jest.useRealTimers();
    }
  });
});

describe("QueueSchedulerService — shutdown during a registration", () => {
  it("stops before the next entry, logs no error and awaits the entry in flight", async () => {
    let releaseFirst!: () => void;
    const firstGate = new Promise<void>((resolve) => (releaseFirst = resolve));
    const queue = {
      getRepeatableJobs: jest
        .fn()
        .mockReturnValueOnce(firstGate.then(() => []))
        .mockResolvedValue([]),
      add: jest.fn().mockResolvedValue({}),
    };
    const service = new (
      QueueSchedulerService as unknown as new (
        ...args: unknown[]
      ) => QueueSchedulerService
    )(...new Array(30).fill(queue));
    const error = jest
      .spyOn(Logger.prototype, "error")
      .mockImplementation(() => undefined);
    jest.spyOn(Logger.prototype, "log").mockImplementation(() => undefined);
    jest.useFakeTimers();
    const prev = process.env.SKIP_QUEUE_BOOTSTRAP;
    delete process.env.SKIP_QUEUE_BOOTSTRAP;

    try {
      await service.onModuleInit();
      jest.advanceTimersByTime(5000);
      expect(queue.getRepeatableJobs).toHaveBeenCalledTimes(1);

      const destroyed = service.onModuleDestroy();
      releaseFirst();
      await destroyed;

      // The entry in flight finished its add; no further entry was looked at.
      expect(queue.add).toHaveBeenCalledTimes(1);
      expect(queue.getRepeatableJobs).toHaveBeenCalledTimes(1);
      expect(error).not.toHaveBeenCalled();
    } finally {
      if (prev !== undefined) process.env.SKIP_QUEUE_BOOTSTRAP = prev;
      jest.useRealTimers();
      jest.restoreAllMocks();
    }
  });
});

describe("QueueSchedulerService — nightly VACUUM (PAR-868)", () => {
  function fakeQueue() {
    return {
      getRepeatableJobs: jest.fn().mockResolvedValue([]),
      removeRepeatableByKey: jest.fn().mockResolvedValue(undefined),
      add: jest.fn().mockResolvedValue(undefined),
    };
  }

  afterEach(() => jest.restoreAllMocks());

  it("registers vacuum-churn-tables at 04:50 UTC on db-maintenance, before the 06:00 training", async () => {
    // One fake per injected queue; db-maintenance is the last argument.
    const queues = Array.from({ length: QueueSchedulerService.length }, () =>
      fakeQueue(),
    );
    const service = new (
      QueueSchedulerService as unknown as new (
        ...args: unknown[]
      ) => QueueSchedulerService
    )(...queues);
    jest
      .spyOn(
        (service as unknown as { logger: { log: () => void } }).logger,
        "log",
      )
      .mockImplementation(() => undefined);

    await (
      service as unknown as { registerScheduledJobs(): Promise<void> }
    ).registerScheduledJobs();

    const dbMaintenance = queues[queues.length - 1];
    expect(dbMaintenance.add).toHaveBeenCalledTimes(3);
    expect(dbMaintenance.add).toHaveBeenCalledWith(
      "vacuum-churn-tables",
      {},
      expect.objectContaining({
        repeat: { cron: "50 4 * * *", tz: "UTC" },
        jobId: "db-vacuum-cron",
        attempts: 1,
      }),
    );
    // PAR-1054: TFT retention before the VACUUM, weekly REINDEX outside the night.
    expect(dbMaintenance.add).toHaveBeenCalledWith(
      "prune-tft-forecasts",
      {},
      expect.objectContaining({
        repeat: { cron: "35 4 * * *", tz: "UTC" },
        jobId: "db-tft-prune-cron",
        attempts: 1,
      }),
    );
    expect(dbMaintenance.add).toHaveBeenCalledWith(
      "reindex-pcn-forecasts-pkey",
      {},
      expect.objectContaining({
        repeat: { cron: "10 19 * * 0", tz: "UTC" },
        jobId: "db-pcn-reindex-cron",
        attempts: 1,
      }),
    );

    // The training it has to finish before is still at 06:00.
    const training = queues.find((q) =>
      q.add.mock.calls.some((c: unknown[]) => c[0] === "train-model"),
    );
    expect(training?.add).toHaveBeenCalledWith(
      "train-model",
      expect.anything(),
      expect.objectContaining({ repeat: { cron: "0 6 * * *" } }),
    );
  });
});

describe("QueueSchedulerService — children sync runs once a day (PAR-875)", () => {
  function fakeQueue() {
    return {
      getRepeatableJobs: jest.fn().mockResolvedValue([]),
      removeRepeatableByKey: jest.fn().mockResolvedValue(undefined),
      add: jest.fn().mockResolvedValue(undefined),
    };
  }

  const RETIRED_ENTRY = {
    key: "fetch-all-children:children-metadata-cron:::0 4 * * *",
    name: "fetch-all-children",
    id: "children-metadata-cron",
    endDate: null,
    tz: null,
    cron: "0 4 * * *",
    every: null,
    next: 1791000000000,
  };

  afterEach(() => jest.restoreAllMocks());

  async function register(childrenQueue: ReturnType<typeof fakeQueue>) {
    const queues = Array.from({ length: QueueSchedulerService.length }, () =>
      fakeQueue(),
    );
    const service = new (
      QueueSchedulerService as unknown as new (
        ...args: unknown[]
      ) => QueueSchedulerService
    )(...queues);
    // The children queue is a distinct fake so its calls can be read back;
    // swap it into the one slot the service reads it from.
    (service as unknown as { childrenQueue: unknown }).childrenQueue =
      childrenQueue;
    jest
      .spyOn(
        (service as unknown as { logger: { log: () => void } }).logger,
        "log",
      )
      .mockImplementation(() => undefined);
    await (
      service as unknown as { registerScheduledJobs(): Promise<void> }
    ).registerScheduledJobs();
  }

  it("schedules no fetch-all-children cron and removes the retired 04:00 entry", async () => {
    const children = fakeQueue();
    children.getRepeatableJobs.mockResolvedValue([RETIRED_ENTRY]);

    await register(children);

    expect(children.add.mock.calls.map((c: unknown[]) => c[0])).not.toContain(
      "fetch-all-children",
    );
    expect(children.removeRepeatableByKey).toHaveBeenCalledWith(
      RETIRED_ENTRY.key,
    );
  });

  it("leaves the queue alone once the entry is gone", async () => {
    const children = fakeQueue();

    await register(children);

    expect(children.add.mock.calls.map((c: unknown[]) => c[0])).not.toContain(
      "fetch-all-children",
    );
    expect(children.removeRepeatableByKey).not.toHaveBeenCalled();
  });
});
