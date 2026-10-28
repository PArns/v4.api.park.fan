import { Logger } from "@nestjs/common";
import type { Job } from "bull";
import { SiteAnalyticsProcessor } from "./site-analytics.processor";
import type { DropOldChunksResult } from "../../site-analytics/site-analytics-storage.service";

const NONE: DropOldChunksResult = {
  dropped: { site_pageviews: 0, site_events: 0 },
  skipped: [],
  pruned: { visitorDays: 0, visitorHours: 0, rollupMarkers: 0 },
};

/**
 * The nightly retention job fails when it had to keep an hourly chunk, as the
 * rollup job fails when a day does not add up, so the failing-jobs report
 * shows it. What is pinned: a clean run is quiet and passes; chunks dropped
 * are logged; a kept chunk fails the job with the count and the first days,
 * after the work is done.
 */
describe("SiteAnalyticsProcessor drop-old-chunks", () => {
  const job = {} as Job;
  let result: DropOldChunksResult;
  let log: jest.SpyInstance;
  let processor: SiteAnalyticsProcessor;

  beforeEach(() => {
    result = NONE;
    log = jest.spyOn(Logger.prototype, "log").mockImplementation();
    processor = new SiteAnalyticsProcessor({
      dropOldChunks: jest.fn(async () => result),
    } as never);
  });
  afterEach(() => jest.restoreAllMocks());

  it("passes without a word when nothing was due", async () => {
    await expect(processor.handleDropOldChunks(job)).resolves.toBeUndefined();
    expect(log).not.toHaveBeenCalled();
  });

  it("logs what it dropped and deleted", async () => {
    result = {
      ...NONE,
      dropped: { site_pageviews: 2, site_events: 1 },
      pruned: { visitorDays: 7, visitorHours: 168, rollupMarkers: 44 },
    };
    await processor.handleDropOldChunks(job);
    expect(log).toHaveBeenCalledWith(
      expect.stringContaining('"site_pageviews":2'),
    );
    expect(log).toHaveBeenCalledWith(
      expect.stringContaining('"rollupMarkers":44'),
    );
  });

  it("fails the job when an hourly chunk was kept, naming the count and the first days", async () => {
    result = {
      ...NONE,
      dropped: { site_pageviews: 1, site_events: 0 },
      skipped: [
        {
          table: "site_events",
          from: "2026-07-16",
          days: [
            "2026-07-17",
            "2026-07-16",
            "2026-07-18",
            "2026-07-19",
            "2026-07-20",
            "2026-07-21",
          ],
        },
      ],
    };
    await expect(processor.handleDropOldChunks(job)).rejects.toThrow(
      /1 hourly chunk\(s\) kept, 6 day\(s\) without a complete roll-up \(2026-07-16, 2026-07-17, 2026-07-18, 2026-07-19, 2026-07-20, …\)/,
    );
    // What could go was logged before the failure.
    expect(log).toHaveBeenCalled();
  });
});
