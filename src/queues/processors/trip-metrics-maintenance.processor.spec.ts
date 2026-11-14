import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { TripMetricsMaintenanceProcessor } from "./trip-metrics-maintenance.processor";
import type { TripRequestMetricsService } from "../../trip-regions/metrics/trip-request-metrics.service";

describe("TripMetricsMaintenanceProcessor", () => {
  const processor = (prune: () => Promise<number>) =>
    new TripMetricsMaintenanceProcessor({
      prune: jest.fn(prune),
    } as unknown as TripRequestMetricsService);

  afterEach(() => jest.restoreAllMocks());

  it("logs the rows a run deleted, and nothing on a night with none", async () => {
    const log = jest
      .spyOn(Logger.prototype, "log")
      .mockImplementation(() => undefined);
    expect(await processor(async () => 12).handlePrune({} as Job)).toBe(12);
    expect(log.mock.calls[0][0]).toBe(
      "🧹 Trip request metrics: deleted 12 row(s) past 90 days",
    );
    log.mockClear();
    expect(await processor(async () => 0).handlePrune({} as Job)).toBe(0);
    expect(log).not.toHaveBeenCalled();
  });

  it("rethrows a failure so Bull retries it and the failing-jobs sweep sees it", async () => {
    jest.spyOn(Logger.prototype, "error").mockImplementation(() => undefined);
    await expect(
      processor(async () => {
        throw new Error("lock timeout");
      }).handlePrune({} as Job),
    ).rejects.toThrow("lock timeout");
  });
});
