import { Logger } from "@nestjs/common";
import { DataQualityProcessor } from "./data-quality.processor";

/**
 * The processor's own failure mode, which is the one the module exists for.
 *
 * Three detectors behind one `Promise.all`: a rejection takes the other two
 * down with it, and three caught rejections leave three empty lists — which is
 * indistinguishable from "nothing is wrong" unless something counts how many
 * checks actually ran. `detect-seasonal` threw on every run for 73 days and
 * nothing said so; a clean ✅ printed under three ERROR lines is the same
 * silence with more output.
 */
describe("DataQualityProcessor", () => {
  const build = (monitor: Partial<Record<string, jest.Mock>>) =>
    new DataQualityProcessor({
      findSilencedClusters:
        monitor.findSilencedClusters ?? jest.fn(async () => []),
      findScheduledButSilentParks:
        monitor.findScheduledButSilentParks ?? jest.fn(async () => []),
      findFailingJobs: monitor.findFailingJobs ?? jest.fn(async () => []),
    } as never);

  let warn: jest.SpyInstance;
  let log: jest.SpyInstance;
  let error: jest.SpyInstance;

  beforeEach(() => {
    warn = jest.spyOn(Logger.prototype, "warn").mockImplementation();
    log = jest.spyOn(Logger.prototype, "log").mockImplementation();
    error = jest.spyOn(Logger.prototype, "error").mockImplementation();
  });

  afterEach(() => jest.restoreAllMocks());

  it("reports clean only when all three checks ran", async () => {
    await build({}).handleMonitorDataQuality({} as never);

    expect(error).not.toHaveBeenCalled();
    expect(log).toHaveBeenCalledWith(expect.stringContaining("clean"));
  });

  it("keeps the other two findings when one detector throws", async () => {
    // The presence half: the cluster finding has to survive the rejection
    // beside it, or the assertion about the missing ✅ would pass for a
    // processor that simply reported nothing.
    await build({
      findSilencedClusters: jest.fn(async () => [
        {
          parkId: "p1",
          parkName: "Europa-Park",
          attractionCount: 44,
          lastOperating: "2026-06-07",
          sampleNames: ["Ball Pool"],
        },
      ]),
      findScheduledButSilentParks: jest.fn(async () => {
        throw new Error('column "tz" does not exist');
      }),
    }).handleMonitorDataQuality({} as never);

    expect(warn).toHaveBeenCalledWith(expect.stringContaining("Europa-Park"));
    expect(error).toHaveBeenCalledWith(
      expect.stringContaining('column "tz" does not exist'),
    );
    expect(log).not.toHaveBeenCalled();
  });

  it("does not call a park with no findings clean when a check never ran", async () => {
    await build({
      findFailingJobs: jest.fn(async () => {
        throw new Error("redis unavailable");
      }),
    }).handleMonitorDataQuality({} as never);

    expect(log).not.toHaveBeenCalled();
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining("2 of 3 checks ran"),
    );
  });

  it("names a park that has never been read without claiming a date", async () => {
    await build({
      findScheduledButSilentParks: jest.fn(async () => [
        {
          parkId: "p2",
          parkName: "Paradise Country",
          attractionCount: 12,
          lastReading: null,
          operatingDaysAhead: 7,
          lastScheduledDay: "2027-01-13",
        },
      ]),
    }).handleMonitorDataQuality({} as never);

    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining("no reading in 400 days"),
    );
  });
});
