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
      findAbsenceRetiredUnreviewed:
        monitor.findAbsenceRetiredUnreviewed ?? jest.fn(async () => []),
      findReissueCandidates:
        monitor.findReissueCandidates ?? jest.fn(async () => []),
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

  it("reports clean only when all four checks ran", async () => {
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
      expect.stringContaining("3 of 4 checks ran"),
    );
  });

  describe("re-issue candidates (PAR-686)", () => {
    const pair = (namesMatch: boolean) => ({
      parkId: "p",
      parkName: "Six Flags Great America",
      previous: {} as never,
      current: {} as never,
      meters: 16.4,
      namesMatch,
    });

    it("warns with the count of likely pairs and the total", async () => {
      await build({
        findReissueCandidates: jest.fn(async () => [pair(true), pair(false)]),
      }).handleMonitorDataQuality({} as never);

      expect(warn).toHaveBeenCalledWith(
        expect.stringMatching(/^🔁 1 retired ride\(s\).*2 nearby pairs in all/),
      );
    });

    it("stays quiet about nearby pairs whose names say nothing, and still reports clean", async () => {
      // Most of the radius is two attractions in one building — Walibi
      // Belgium's three 4D films at 0 m. Those are not a nightly warning.
      await build({
        findReissueCandidates: jest.fn(async () => [pair(false)]),
      }).handleMonitorDataQuality({} as never);

      expect(warn).not.toHaveBeenCalled();
      expect(log).toHaveBeenCalledWith(expect.stringContaining("clean"));
    });

    it("does not count as a check that ran or failed", async () => {
      await build({
        findReissueCandidates: jest.fn(async () => {
          throw new Error("statement timeout");
        }),
      }).handleMonitorDataQuality({} as never);

      expect(error).toHaveBeenCalledWith(
        expect.stringContaining("statement timeout"),
      );
      expect(log).toHaveBeenCalledWith(expect.stringContaining("clean"));
    });
  });

  it("names unreviewed absence retirements once per park, not once per ride (PAR-684)", async () => {
    const row = (name: string, parkName: string) => ({
      attractionId: name,
      name,
      slug: name,
      parkId: parkName,
      parkName,
      retiredAt: "2026-10-03T03:29:45.000Z",
      lastReading: "2026-04-24",
    });
    await build({
      findAbsenceRetiredUnreviewed: jest.fn(async () => [
        row("Restroom", "Wet'n'Wild"),
        row("Lockers", "Wet'n'Wild"),
        row("Asylum", "Parque de Atracciones de Madrid"),
      ]),
    }).handleMonitorDataQuality({} as never);

    const lines = warn.mock.calls.map(([m]) => String(m));
    expect(lines.filter((l) => l.startsWith("🪦"))).toEqual([
      expect.stringContaining("Wet'n'Wild: 2 ride(s)"),
      expect.stringContaining("Parque de Atracciones de Madrid: 1 ride(s)"),
    ]);
    expect(log).not.toHaveBeenCalled();
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
