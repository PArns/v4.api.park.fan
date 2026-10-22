import { EventEmitter } from "events";
import { watchShutdownWork } from "./shutdown-work";

/** A stand-in for a Bull queue: local events plus a recorded pause. */
class FakeQueue extends EventEmitter {
  readonly pauses: unknown[][] = [];
  pauseImpl: () => Promise<void> = () => Promise.resolve();

  constructor(readonly name: string) {
    super();
  }

  pause(...args: unknown[]): Promise<void> {
    this.pauses.push(args);
    return this.pauseImpl();
  }

  whenCurrentJobsFinished(): Promise<void> {
    return Promise.resolve();
  }

  // What @nestjs/bull sets on every queue it provides.
  onApplicationShutdown(): Promise<void> {
    return new Promise(() => undefined);
  }
}

class SlowService {
  release: () => void = () => undefined;
  onModuleDestroy(): Promise<void> {
    return new Promise((resolve) => (this.release = resolve));
  }
}

class QuickService {
  calls = 0;
  onApplicationShutdown(): void {
    this.calls += 1;
  }
}

class BrokenService {
  onModuleDestroy(): void {
    throw new Error("boom");
  }
}

/** The slice of a ModulesContainer the walk reads. */
function modulesOf(...instances: unknown[]): never {
  const wrappers = new Map(instances.map((instance, i) => [i, { instance }]));
  return new Map([
    [
      "AppModule",
      { providers: wrappers, controllers: new Map(), injectables: new Map() },
    ],
  ]) as never;
}

describe("watchShutdownWork", () => {
  it("pauses every queue locally without waiting for the running job", () => {
    const warmup = new FakeQueue("live-cache-warmup");
    const waitTimes = new FakeQueue("wait-times");
    const work = watchShutdownWork(modulesOf(warmup, waitTimes));

    work.stopTakingJobs();

    expect(warmup.pauses).toEqual([[true, true]]);
    expect(waitTimes.pauses).toEqual([[true, true]]);
  });

  it("pauses the remaining queues when one pause throws or rejects", async () => {
    const throwing = new FakeQueue("analytics");
    throwing.pauseImpl = () => {
      throw new Error("Connection is closed.");
    };
    const rejecting = new FakeQueue("stats");
    rejecting.pauseImpl = () =>
      Promise.reject(new Error("Connection is closed."));
    const healthy = new FakeQueue("wait-times");
    const work = watchShutdownWork(modulesOf(throwing, rejecting, healthy));

    expect(() => work.stopTakingJobs()).not.toThrow();
    await Promise.resolve();

    expect(healthy.pauses).toEqual([[true, true]]);
  });

  it("lists the jobs this process is running, per queue", () => {
    const warmup = new FakeQueue("live-cache-warmup");
    const stats = new FakeQueue("stats");
    const work = watchShutdownWork(modulesOf(warmup, stats));

    warmup.emit("active", { id: 3698, name: "warmup-after-sync" });
    stats.emit("active", { id: 7, name: "aggregate" });
    stats.emit("completed", { id: 7, name: "aggregate" });
    // Bull's stalled checker can emit a local `failed` with no job.
    expect(() => stats.emit("failed", null)).not.toThrow();

    expect(work.describeOpen().activeJobs).toEqual({
      "live-cache-warmup": [{ id: "3698", name: "warmup-after-sync" }],
    });
  });

  it("names the hooks still running once the shutdown has begun", async () => {
    const slow = new SlowService();
    const quick = new QuickService();
    const work = watchShutdownWork(modulesOf(slow, quick));

    work.stopTakingJobs();
    const destroying = slow.onModuleDestroy();
    quick.onApplicationShutdown();

    expect(work.describeOpen().openHooks).toEqual([
      "SlowService.onModuleDestroy",
    ]);

    slow.release();
    await destroying;
    expect(work.describeOpen().openHooks).toEqual([]);
  });

  it("keeps `this`, and clears a hook that throws", () => {
    const quick = new QuickService();
    const broken = new BrokenService();
    const work = watchShutdownWork(modulesOf(quick, broken));

    work.stopTakingJobs();
    quick.onApplicationShutdown();

    expect(quick.calls).toBe(1);
    expect(() => broken.onModuleDestroy()).toThrow("boom");
    expect(work.describeOpen().openHooks).toEqual([]);
  });

  it("names a queue's own close hook by the queue", () => {
    const warmup = new FakeQueue("live-cache-warmup");
    const work = watchShutdownWork(modulesOf(warmup));

    work.stopTakingJobs();
    void warmup.onApplicationShutdown();

    expect(work.describeOpen().openHooks).toEqual([
      "Queue(live-cache-warmup).onApplicationShutdown",
    ]);
  });
});
