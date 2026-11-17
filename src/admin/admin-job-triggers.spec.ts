import { AdminController } from "./admin.controller";

/** The controller with only the queues these triggers touch. */
function controllerWith(queues: Record<string, unknown>): AdminController {
  return Object.assign(Object.create(AdminController.prototype), queues);
}

describe("calendar warmup trigger", () => {
  const job = (id: number, name: string) => ({ id, name });

  it("queues the warmup the cron queues", async () => {
    const parkMetadataQueue = {
      getJobs: jest.fn(async () => [job(3, "sync-schedules-only")]),
      add: jest.fn(async () => job(9, "warmup-calendar-daily")),
    };
    expect(
      await controllerWith({ parkMetadataQueue }).triggerCalendarWarmup(),
    ).toEqual({ message: "Calendar warmup queued", jobId: "9" });
    expect(parkMetadataQueue.getJobs).toHaveBeenCalledWith([
      "active",
      "waiting",
    ]);
    expect(parkMetadataQueue.add).toHaveBeenCalledWith(
      "warmup-calendar-daily",
      {},
      { priority: 10 },
    );
  });

  it("answers with the warmup already running instead of starting a second", async () => {
    const parkMetadataQueue = {
      getJobs: jest.fn(async () => [job(4, "warmup-calendar-daily")]),
      add: jest.fn(),
    };
    expect(
      await controllerWith({ parkMetadataQueue }).triggerCalendarWarmup(),
    ).toEqual({
      message: "Calendar warmup already running or waiting",
      jobId: "4",
    });
    expect(parkMetadataQueue.add).not.toHaveBeenCalled();
  });
});

describe("TFT training trigger", () => {
  it("queues one attempt of train-nf, as the cron", async () => {
    const nfTrainingQueue = { add: jest.fn(async () => ({ id: 5 })) };
    expect(
      await controllerWith({ nfTrainingQueue }).triggerTftTraining(),
    ).toEqual({ message: "TFT training job queued", jobId: "5" });
    expect(nfTrainingQueue.add).toHaveBeenCalledWith(
      "train-nf",
      {},
      { priority: 10, attempts: 1 },
    );
  });
});
