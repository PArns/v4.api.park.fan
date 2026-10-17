import { Logger } from "@nestjs/common";
import { DbSeedService } from "./db-seed.service";

describe("DbSeedService — shutdown", () => {
  const repo = () => ({
    count: jest.fn().mockResolvedValue(1),
    findOne: jest.fn().mockResolvedValue(null),
  });
  const queue = () => ({ add: jest.fn().mockResolvedValue({}) });

  const build = (parkRepo: ReturnType<typeof repo>) => {
    const repos = [parkRepo, repo(), repo(), repo(), repo(), repo()];
    const queues = [queue(), queue(), queue(), queue()];
    const service = new (
      DbSeedService as unknown as new (...args: unknown[]) => DbSeedService
    )(...repos, ...queues);
    return { service, queues };
  };

  beforeEach(() => {
    jest.useFakeTimers();
    jest.spyOn(Logger.prototype, "log").mockImplementation(() => undefined);
  });

  afterEach(() => {
    jest.useRealTimers();
    jest.restoreAllMocks();
  });

  it("cancels a seed that has not fired yet", async () => {
    const parkRepo = repo();
    const { service } = build(parkRepo);

    await service.onModuleInit();
    await service.onModuleDestroy();
    jest.advanceTimersByTime(11 * 60 * 1000);

    expect(parkRepo.count).not.toHaveBeenCalled();
  });

  it("stops a seed that has fired, without an error line, and awaits its step", async () => {
    let releaseCount!: (n: number) => void;
    const countGate = new Promise<number>(
      (resolve) => (releaseCount = resolve),
    );
    const parkRepo = { ...repo(), count: jest.fn().mockReturnValue(countGate) };
    const { service, queues } = build(parkRepo);
    const error = jest
      .spyOn(Logger.prototype, "error")
      .mockImplementation(() => undefined);

    await service.onModuleInit();
    jest.advanceTimersByTime(10 * 60 * 1000);
    expect(parkRepo.count).toHaveBeenCalledTimes(1);

    const destroyed = service.onModuleDestroy();
    releaseCount(0);
    await destroyed;

    expect(queues.every((q) => q.add.mock.calls.length === 0)).toBe(true);
    expect(error).not.toHaveBeenCalled();
  });
});
